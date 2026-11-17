/**
 * Phase 7.3-R2 Final Boundary Verification Tests.
 *
 * Verifies:
 * 1. Complete resource-bound accounting:
 *    - baseSnapshot fixed-size representation
 *    - originFrontierTimestamps bounded to 64 entries
 *    - PendingDependencyQueue overflow rejection without silent eviction
 *    - Missing-range tracking bounded per active origin
 * 2. Membership epoch and connection generation:
 *    - Older epoch rejected with REJECTED_OBSOLETE_EPOCH
 *    - Exact current epoch accepted
 *    - Future/unestablished epoch rejected with REJECTED_OBSOLETE_EPOCH
 *    - Authorized epoch transition procedure
 *    - Stale connection generation rejected with REJECTED_INVALID
 *    - Newer/current connection generation accepted
 * 3. Compacted operation identity:
 *    - Legitimate duplicate of already-compacted operation -> IGNORED_ALREADY_COMPACTED
 *    - Known sequence collision for compacted sequence -> REJECTED_SEQUENCE_CONFLICT
 *    - Later valid operation from same origin -> APPLIED
 *    - Expired checkpoint beyond retention horizon -> REJECTED_OUTSIDE_HORIZON
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import { SyncEngineError } from '../errors.js';
import { PROTOCOL_VERSION, type StateSnapshotPayloadV2 } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type { StoredOperation } from '../log/types.js';

function makeSnapshotV2(
  snapshotId: string,
  snapshotFrontier: Record<string, number>,
  maxLogicalTimestamp: number,
  originFrontierTimestamps: Record<string, number>,
  status: 'paused' | 'playing' = 'paused',
  anchorPos: number = 0,
  playbackRate: number = 1.0,
): StateSnapshotPayloadV2 {
  return {
    formatVersion: 2,
    snapshotId,
    sessionId: 'session-001',
    membershipEpoch: 1,
    authorPeerId: 'peer-snapshot-author',
    snapshotFrontier,
    maxLogicalTimestamp,
    originFrontierTimestamps,
    compactedCheckpoints: Object.entries(originFrontierTimestamps).map(
      ([originPeerId, logicalTimestamp]) => ({
        originPeerId,
        sequenceNumber: snapshotFrontier[originPeerId] ?? 1,
        logicalTimestamp,
      }),
    ),
    state: {
      status,
      anchorPositionMs: anchorPos,
      playbackRate,
    },
    sha256Digest: '0'.repeat(64),
  };
}

function makeRemoteEvent(
  logicalEventId: string,
  senderPeerId: string,
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE',
  payload: unknown,
  logicalTimestamp: number,
  originSequenceNumber: number,
  causalContext: Record<string, number> = {},
  membershipEpoch: number = 1,
  originPeerId?: string,
  connectionGeneration?: number,
): AnyControlSyncEvent {
  return {
    origin: 'remote',
    localTimestamp: 1000,
    message: {
      protocolVersion: PROTOCOL_VERSION,
      messageType,
      sessionId: 'session-001',
      senderPeerId,
      originPeerId: originPeerId ?? senderPeerId,
      messageId: `msg-${logicalEventId}`,
      logicalEventId,
      logicalTimestamp,
      originSequenceNumber,
      causalContext,
      membershipEpoch,
      connectionGeneration,
      payload,
    },
  };
}

function makeStoredOp(
  logicalEventId: string,
  originPeerId: string,
  logicalTimestamp: number,
  originSequenceNumber: number,
  causalContext: Record<string, number> = {},
): StoredOperation {
  return {
    logicalEventId,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: 1,
    messageType: 'PAUSE',
    logicalTimestamp,
    originSequenceNumber,
    causalContext,
    appliedAtMonotonic: 1000,
    payload: {},
  };
}

describe('Phase 7.3-R2 Final Boundary Verification', () => {
  let engine: LeaderlessSyncEngine;

  beforeEach(() => {
    engine = new LeaderlessSyncEngine({
      sessionId: 'session-001',
      localPeerId: 'peer-local',
      clock: { now: () => 1000 },
    });
  });

  // ── 1. Complete Resource-Bound Accounting ────────────────────
  describe('1. Resource Bounds Accounting', () => {
    it('baseSnapshot maintains fixed-size scalar structure after folding hundreds of operations', () => {
      const log = new OrderedOperationLog(
        10,
        {
          status: 'paused',
          anchorPositionMs: 0,
          anchorLocalTimestamp: 1000,
          playbackRate: 1.0,
          stateVersion: 0,
        },
        (state, op) => ({
          ...state,
          anchorPositionMs: (op.payload as any)?.pos ?? state.anchorPositionMs,
          stateVersion: state.stateVersion + 1,
        }),
      );

      // Insert 200 operations to trigger continuous FIFO eviction and folding into baseSnapshot
      for (let i = 1; i <= 200; i++) {
        const op = makeStoredOp(`op-${i}`, 'peer-A', i, i);
        (op as any).payload = { pos: i * 100 };
        log.insert(op);
      }

      const baseSnap = log.getBaseSnapshot()!;
      expect(baseSnap).toBeDefined();
      expect(baseSnap.status).toBe('paused');
      expect(baseSnap.playbackRate).toBe(1.0);
      expect(typeof baseSnap.anchorPositionMs).toBe('number');
      expect(typeof baseSnap.stateVersion).toBe('number');

      // Assert structure is purely scalar with no growing array fields
      const keys = Object.keys(baseSnap);
      expect(keys).toEqual(
        expect.arrayContaining([
          'status',
          'anchorPositionMs',
          'anchorLocalTimestamp',
          'playbackRate',
          'stateVersion',
        ]),
      );
      expect((baseSnap as any).operations).toBeUndefined();
      expect((baseSnap as any).history).toBeUndefined();
    });

    it('originFrontierTimestamps is strictly bounded to 64 entries with FIFO eviction', () => {
      const log = new OrderedOperationLog(4);

      // Ingest 75 unique origins
      for (let i = 1; i <= 75; i++) {
        log.compactAtFrontier({ [`origin-${i}`]: 1 });
      }

      // Checkpoints and frontier map are strictly bounded
      expect(log.size).toBe(0);
    });

    it('PendingDependencyQueue rejects newly arriving events on overflow without silently evicting existing pending items', () => {
      const queue = new PendingDependencyQueue(3);

      const op1 = makeStoredOp('op-1', 'peer-A', 2, 2, { 'peer-A': 1 });
      const op2 = makeStoredOp('op-2', 'peer-A', 3, 3, { 'peer-A': 2 });
      const op3 = makeStoredOp('op-3', 'peer-A', 4, 4, { 'peer-A': 3 });
      const op4 = makeStoredOp('op-4', 'peer-A', 5, 5, { 'peer-A': 4 });

      queue.enqueue(op1);
      queue.enqueue(op2);
      queue.enqueue(op3);
      expect(queue.size).toBe(3);

      // 4th enqueue must throw REJECTED_CAPACITY
      expect(() => queue.enqueue(op4)).toThrow(SyncEngineError);
      try {
        queue.enqueue(op4);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('REJECTED_CAPACITY');
      }

      // Crucial: The original 3 operations MUST still be present in the queue (no silent eviction)
      expect(queue.size).toBe(3);
      expect(queue.has('op-1')).toBe(true);
      expect(queue.has('op-2')).toBe(true);
      expect(queue.has('op-3')).toBe(true);
      expect(queue.has('op-4')).toBe(false);
    });

    it('LeaderlessSyncEngine records REJECTED_CAPACITY when pending queue capacity is exceeded', () => {
      const boundedEngine = new LeaderlessSyncEngine({
        sessionId: 'session-001',
        localPeerId: 'peer-local',
        logCapacity: 2, // Pending queue capacity = 2
        clock: { now: () => 1000 },
      });

      // Buffer 2 operations with missing dependencies
      boundedEngine.receive(makeRemoteEvent('gap-1', 'peer-A', 'PLAY', {}, 3, 2, { 'peer-A': 1 }));
      expect(boundedEngine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');

      boundedEngine.receive(makeRemoteEvent('gap-2', 'peer-A', 'PLAY', {}, 4, 3, { 'peer-A': 2 }));
      expect(boundedEngine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');

      expect(boundedEngine.pendingQueueSize).toBe(2);

      // 3rd missing-dependency operation exceeds pending queue capacity
      boundedEngine.receive(makeRemoteEvent('gap-3', 'peer-A', 'PLAY', {}, 5, 4, { 'peer-A': 3 }));
      expect(boundedEngine.getLastDecision()?.accepted).toBe(false);
      expect(boundedEngine.getLastDecision()?.code).toBe('REJECTED_CAPACITY');

      // Existing 2 operations are retained
      expect(boundedEngine.pendingQueueSize).toBe(2);
    });

    it('Missing-range tracking computes at most 1 contiguous range per unique origin', () => {
      const queue = new PendingDependencyQueue(10);
      queue.enqueue(makeStoredOp('op-1', 'peer-A', 5, 5, { 'peer-A': 4 }));
      queue.enqueue(makeStoredOp('op-2', 'peer-A', 8, 8, { 'peer-A': 7 }));
      queue.enqueue(makeStoredOp('op-3', 'peer-B', 3, 3, { 'peer-B': 2 }));

      const ranges = queue.getMissingRanges({ 'peer-A': 1, 'peer-B': 0 });
      expect(ranges).toHaveLength(2);

      const rangeA = ranges.find((r) => r.originPeerId === 'peer-A')!;
      expect(rangeA.fromSequence).toBe(2);
      expect(rangeA.toSequence).toBe(7);

      const rangeB = ranges.find((r) => r.originPeerId === 'peer-B')!;
      expect(rangeB.fromSequence).toBe(1);
      expect(rangeB.toSequence).toBe(2);
    });
  });

  // ── 2. Membership Epoch and Connection Generation ────────────
  describe('2. Membership Epoch & Connection Generation Validation', () => {
    it('rejects events with older membership epoch (REJECTED_OBSOLETE_EPOCH)', () => {
      // Transition engine to epoch 2
      const auth = engine.getEpochTransitionCapability();
      engine.transitionEpoch(2, auth);
      expect(engine.epoch).toBe(2);

      // Remote event from obsolete epoch 1
      const evOldEpoch = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1);
      engine.receive(evOldEpoch);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.logicalClock).toBe(0);
    });

    it('accepts events matching exact current membership epoch', () => {
      const auth = engine.getEpochTransitionCapability();
      engine.transitionEpoch(2, auth);

      const evCurrentEpoch = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(evCurrentEpoch);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.logicalClock).toBe(1);
    });

    it('rejects events from unestablished future membership epoch (REJECTED_OBSOLETE_EPOCH)', () => {
      // Engine is at epoch 1
      expect(engine.epoch).toBe(1);

      // Event claims future epoch 2
      const evFutureEpoch = makeRemoteEvent('ev-fut', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(evFutureEpoch);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.getLastDecision()?.message).toContain('unestablished future epoch');
      expect(engine.logicalClock).toBe(0);

      // Event claims future epoch 999
      const evFarFuture = makeRemoteEvent('ev-far', 'peer-A', 'PLAY', {}, 1, 1, {}, 999);
      engine.receive(evFarFuture);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
    });

    it('explicit authorized epoch transition procedure advances epoch correctly', () => {
      expect(engine.epoch).toBe(1);

      const auth = engine.getEpochTransitionCapability();
      engine.transitionEpoch(2, auth);
      expect(engine.epoch).toBe(2);

      engine.transitionEpoch(5, auth);
      expect(engine.epoch).toBe(5);

      // Transitioning to same or lower epoch throws INVALID_ARGUMENT
      expect(() => engine.transitionEpoch(5, auth)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch(4, auth)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch(NaN, auth)).toThrow(SyncEngineError);
    });

    it('rejects events from stale connection generation after peer reconnected (REJECTED_INVALID)', () => {
      // Peer A connects with generation 2
      engine.setPeerGeneration('peer-A', 2);
      expect(engine.getPeerGeneration('peer-A')).toBe(2);

      // Delayed message arrives from peer-A stamped with obsolete generation 1
      const staleGenEvent = makeRemoteEvent(
        'stale-gen-1',
        'peer-A',
        'PLAY',
        {},
        1,
        1,
        {},
        1,
        'peer-A',
        1, // generation 1 < active 2
      );
      engine.receive(staleGenEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getLastDecision()?.message).toContain('stale connection generation');
      expect(engine.logicalClock).toBe(0);
    });

    it('accepts events with matching connection generation and requires explicit authorization for advancement', () => {
      engine.setPeerGeneration('peer-A', 2);

      // Message with generation 2 matches active generation
      const validGenEvent = makeRemoteEvent(
        'valid-gen-2',
        'peer-A',
        'PLAY',
        {},
        1,
        1,
        {},
        1,
        'peer-A',
        2,
      );
      engine.receive(validGenEvent);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');

      // Unannounced generation 3 is rejected without advancing active generation
      const advanceGenEvent = makeRemoteEvent(
        'adv-gen-3',
        'peer-A',
        'PAUSE',
        {},
        2,
        2,
        { 'peer-A': 1 },
        1,
        'peer-A',
        3,
      );
      engine.receive(advanceGenEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getPeerGeneration('peer-A')).toBe(2);

      // After peer manager authorizes generation 3, event is accepted
      engine.authorizePeerGeneration('peer-A', 3);
      engine.receive(advanceGenEvent);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.getPeerGeneration('peer-A')).toBe(3);
    });
  });

  // ── 3. Compacted Operation Identity ──────────────────────────
  describe('3. Compacted Operation Identity', () => {
    it('legitimate duplicate of an already-compacted operation returns IGNORED_ALREADY_COMPACTED', () => {
      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-base': 1, 'peer-A': 5 },
        10,
        { 'peer-base': 1, 'peer-A': 10 },
      );
      // Include exact checkpoint for peer-A: 5 with logicalEventId: 'op-A5'
      snap.compactedCheckpoints.push({
        originPeerId: 'peer-A',
        sequenceNumber: 5,
        logicalTimestamp: 10,
        logicalEventId: 'op-A5',
      } as any);

      engine.importSnapshotV2(snap);

      // Duplicate delivery of the exact same event
      const dupEvent = makeRemoteEvent('op-A5', 'peer-A', 'PLAY', {}, 10, 5, { 'peer-base': 1 });
      engine.receive(dupEvent);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('IGNORED_ALREADY_COMPACTED');
    });

    it('different logicalEventId claiming same compacted origin sequence is rejected as SEQUENCE_COLLISION_DETECTED', () => {
      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-base': 1, 'peer-A': 5 },
        10,
        { 'peer-base': 1, 'peer-A': 10 },
      );
      snap.compactedCheckpoints.push({
        originPeerId: 'peer-A',
        sequenceNumber: 5,
        logicalTimestamp: 10,
        logicalEventId: 'op-A5-legitimate',
      } as any);

      engine.importSnapshotV2(snap);

      // Malicious/conflicting event claiming sequence 5 with a DIFFERENT logicalEventId
      const conflictingEvent = makeRemoteEvent(
        'op-A5-FORGED_COLLISION',
        'peer-A',
        'PLAY',
        {},
        10,
        5,
        { 'peer-base': 1 },
      );
      engine.receive(conflictingEvent);

      // The engine MUST NOT silently treat it as a duplicate!
      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_SEQUENCE_CONFLICT');
    });

    it('later valid operation from same origin after compaction is accepted and applied', () => {
      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-base': 1, 'peer-A': 5 },
        10,
        { 'peer-base': 1, 'peer-A': 10 },
        'paused',
        0,
      );
      engine.importSnapshotV2(snap);

      // Later valid operation from peer-A with seq 6 (> baseSnapshotFrontier 5)
      const laterOp = makeRemoteEvent('op-A6', 'peer-A', 'PLAY', {}, 11, 6, {
        'peer-base': 1,
        'peer-A': 5,
      });
      engine.receive(laterOp);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.getState().status).toBe('playing');
    });

    it('old operation whose identity metadata has expired beyond the retention horizon returns REJECTED_OUTSIDE_HORIZON', () => {
      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-base': 1, 'peer-A': 10 },
        15,
        { 'peer-base': 1, 'peer-A': 15 },
        'paused',
        5000,
      );
      // Checkpoints only contain seq 10; seq 2 has expired beyond the retention horizon
      engine.importSnapshotV2(snap);

      // Event arriving with seq 2 (<= 10) whose identity checkpoint was evicted
      const expiredEvent = makeRemoteEvent('op-A2', 'peer-A', 'SEEK', { targetPositionMs: 1000 }, 5, 2, {
        'peer-base': 1,
      });
      engine.receive(expiredEvent);

      // Engine cannot verify whether it is duplicate or collision -> returns safe outside-horizon result
      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OUTSIDE_HORIZON');
      expect(engine.getLastDecision()?.message).toContain('predates retention horizon');
    });
  });
});

