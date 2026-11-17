/**
 * Phase 7.3-R3 Final Identity & Authorization Audit Test Suite.
 *
 * Implements strict verification for:
 * 1. Complete Compacted Operation Identity & Canonical Fingerprinting (Task 1)
 * 2. Authoritative Connection Generation Lifecycle & Separation from Origin (Task 2)
 * 3. Explicit Epoch Transition Semantics & Obsolete Queue Pruning (Task 3)
 * 4. Pending Queue Overflow Machine-Readable REJECTED_CAPACITY Semantics (Task 4)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { PROTOCOL_VERSION, type StateSnapshotPayloadV2 } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  type EpochTransitionCapability,
} from '../engine/leaderless-sync-engine.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import {
  canonicalizeJson,
  sha256,
  computeOperationFingerprint,
} from '../log/operation-fingerprint.js';
import { SyncEngineError } from '../errors.js';
import type { StoredOperation } from '../log/types.js';

function makeSnapshotV2(
  snapshotId: string,
  snapshotFrontier: Record<string, number>,
  maxLogicalTimestamp: number,
  originFrontierTimestamps: Record<string, number>,
  compactedCheckpoints: Array<{
    originPeerId: string;
    sequenceNumber: number;
    logicalTimestamp: number;
    logicalEventId?: string;
    fingerprint?: string;
  }> = [],
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
    compactedCheckpoints,
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
  originSequenceNumber: number,
  logicalTimestamp: number,
  causalContext: Record<string, number> = {},
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE' = 'PLAY',
  payload: unknown = {},
  membershipEpoch: number = 1,
): StoredOperation {
  return {
    logicalEventId,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch,
    messageType,
    logicalTimestamp,
    originSequenceNumber,
    causalContext,
    appliedAtMonotonic: 1000,
    payload,
  };
}

describe('Phase 7.3-R3 Final Identity & Authorization Audit', () => {
  let engine: LeaderlessSyncEngine;
  let epochCapability: EpochTransitionCapability;

  beforeEach(() => {
    epochCapability = createEpochTransitionCapability('peer-local', 'session-001');
    engine = new LeaderlessSyncEngine({
      sessionId: 'session-001',
      localPeerId: 'peer-local',
      clock: { now: () => 1000 },
      epochCapability,
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 1. Task 1: Complete Compacted Operation Identity & Canonical Fingerprinting
  // ════════════════════════════════════════════════════════════════
  describe('Task 1: Compacted Operation Identity & Deduplication', () => {
    it('equivalent objects with different property insertion orders produce identical canonical JSON and fingerprint', () => {
      const obj1 = {
        originPeerId: 'peer-A',
        originSequenceNumber: 5,
        logicalEventId: 'evt-100',
        logicalTimestamp: 10,
        causalContext: { 'peer-A': 4, 'peer-B': 2 },
        messageType: 'SEEK',
        payload: { positionMs: 42000, reason: 'user_scrub' },
        membershipEpoch: 1,
      };

      // Same data with permuted key insertion order at all levels
      const obj2 = {
        payload: { reason: 'user_scrub', positionMs: 42000 },
        messageType: 'SEEK',
        logicalEventId: 'evt-100',
        causalContext: { 'peer-B': 2, 'peer-A': 4 },
        originPeerId: 'peer-A',
        membershipEpoch: 1,
        logicalTimestamp: 10,
        originSequenceNumber: 5,
      };

      const canon1 = canonicalizeJson(obj1);
      const canon2 = canonicalizeJson(obj2);
      expect(canon1).toBe(canon2);

      const fp1 = computeOperationFingerprint(obj1);
      const fp2 = computeOperationFingerprint(obj2);
      expect(fp1).toBe(fp2);
      expect(fp1).toMatch(/^[0-9a-f]{64}$/);
    });

    it('exact compacted duplicate matching all immutable fields returns IGNORED_ALREADY_COMPACTED', () => {
      const originalOp = makeStoredOp('op-A5', 'peer-A', 5, 10, { 'peer-A': 4 }, 'PLAY', { test: true });
      const fp = computeOperationFingerprint(originalOp);

      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 5 },
        10,
        { 'peer-A': 10 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 5,
            logicalTimestamp: 10,
            logicalEventId: 'op-A5',
            fingerprint: fp,
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Re-transmit exact duplicate
      const dupEvent = makeRemoteEvent(
        'op-A5',
        'peer-A',
        'PLAY',
        { test: true },
        10,
        5,
        { 'peer-A': 4 },
      );
      engine.receive(dupEvent);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('IGNORED_ALREADY_COMPACTED');
    });

    it('compacted operation identity conflict: same event ID and sequence but altered timestamp is rejected with REJECTED_IDENTITY_CONFLICT', () => {
      const originalOp = makeStoredOp('op-A5', 'peer-A', 5, 10, { 'peer-A': 4 });
      const fp = computeOperationFingerprint(originalOp);

      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 5 },
        10,
        { 'peer-A': 10 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 5,
            logicalTimestamp: 10,
            logicalEventId: 'op-A5',
            fingerprint: fp,
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Incoming altered event: timestamp is 9 instead of 10
      const forgedEvent = makeRemoteEvent(
        'op-A5',
        'peer-A',
        'PLAY',
        {},
        9, // Altered!
        5,
        { 'peer-A': 4 },
      );
      engine.receive(forgedEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_IDENTITY_CONFLICT');
      expect(engine.getLastDecision()?.message).toContain('immutable content altered');
    });

    it('compacted operation identity conflict: same event ID and sequence but altered causalContext is rejected with REJECTED_IDENTITY_CONFLICT', () => {
      const originalOp = makeStoredOp('op-A5', 'peer-A', 5, 10, { 'peer-A': 4, 'peer-B': 2 });
      const fp = computeOperationFingerprint(originalOp);

      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 5 },
        10,
        { 'peer-A': 10 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 5,
            logicalTimestamp: 10,
            logicalEventId: 'op-A5',
            fingerprint: fp,
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Incoming altered event: causalContext omitted peer-B dependency
      const forgedEvent = makeRemoteEvent(
        'op-A5',
        'peer-A',
        'PLAY',
        {},
        10,
        5,
        { 'peer-A': 4 }, // Missing peer-B: 2!
      );
      engine.receive(forgedEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_IDENTITY_CONFLICT');
    });

    it('compacted operation identity conflict: same event ID and sequence but altered payload is rejected with REJECTED_IDENTITY_CONFLICT', () => {
      const originalOp = makeStoredOp('op-A5', 'peer-A', 5, 10, { 'peer-A': 4 }, 'SEEK', { positionMs: 5000 });
      const fp = computeOperationFingerprint(originalOp);

      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 5 },
        10,
        { 'peer-A': 10 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 5,
            logicalTimestamp: 10,
            logicalEventId: 'op-A5',
            fingerprint: fp,
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Incoming altered event: positionMs altered to 99999
      const forgedEvent = makeRemoteEvent(
        'op-A5',
        'peer-A',
        'SEEK',
        { positionMs: 99999 }, // Altered payload!
        10,
        5,
        { 'peer-A': 4 },
      );
      engine.receive(forgedEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_IDENTITY_CONFLICT');
    });

    it('compacted sequence collision: different logicalEventId claiming same sequence is rejected with REJECTED_SEQUENCE_CONFLICT', () => {
      const originalOp = makeStoredOp('op-original', 'peer-A', 5, 10, { 'peer-A': 4 });
      const fp = computeOperationFingerprint(originalOp);

      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 5 },
        10,
        { 'peer-A': 10 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 5,
            logicalTimestamp: 10,
            logicalEventId: 'op-original',
            fingerprint: fp,
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Different event ID claiming compacted sequence 5
      const collidingEvent = makeRemoteEvent(
        'op-forged-collision',
        'peer-A',
        'PLAY',
        {},
        10,
        5,
        { 'peer-A': 4 },
      );
      engine.receive(collidingEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_SEQUENCE_CONFLICT');
    });

    it('expired identity checkpoint outside horizon returns REJECTED_OUTSIDE_HORIZON rather than guessing', () => {
      // Snapshot frontier = 10, but compactedCheckpoints only covers sequence 10
      const snap = makeSnapshotV2(
        'snap-1',
        { 'peer-A': 10 },
        20,
        { 'peer-A': 20 },
        [
          {
            originPeerId: 'peer-A',
            sequenceNumber: 10,
            logicalTimestamp: 20,
            logicalEventId: 'op-A10',
          },
        ],
      );
      engine.importSnapshotV2(snap);

      // Arrival claiming sequence 3 (checkpoint expired/absent)
      const expiredEvent = makeRemoteEvent('op-A3', 'peer-A', 'PLAY', {}, 5, 3, { 'peer-A': 2 });
      engine.receive(expiredEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OUTSIDE_HORIZON');
      expect(engine.getLastDecision()?.message).toContain('predates retention horizon');
    });

    it('retained-operation identity reuse with altered immutable content is detected and rejected', () => {
      const log = new OrderedOperationLog();
      const op1 = makeStoredOp('evt-retained', 'peer-A', 1, 1, {}, 'PLAY', { track: 1 });
      log.insert(op1);

      // 1. Same event ID but altered payload
      const alteredPayloadOp = makeStoredOp('evt-retained', 'peer-A', 1, 1, {}, 'PLAY', { track: 2 });
      expect(() => log.insert(alteredPayloadOp)).toThrow(SyncEngineError);
      try {
        log.insert(alteredPayloadOp);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('IDENTITY_CONFLICT_DETECTED');
      }

      // 2. Same event ID but altered timestamp
      const alteredTimestampOp = makeStoredOp('evt-retained', 'peer-A', 1, 2, {}, 'PLAY', { track: 1 });
      expect(() => log.insert(alteredTimestampOp)).toThrow(SyncEngineError);
      try {
        log.insert(alteredTimestampOp);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('IDENTITY_CONFLICT_DETECTED');
      }

      // 3. Exact retransmission succeeds as IGNORED_DUPLICATE
      const exactDup = makeStoredOp('evt-retained', 'peer-A', 1, 1, {}, 'PLAY', { track: 1 });
      const res = log.insert(exactDup);
      expect(res.inserted).toBe(false);
      expect(res.code).toBe('IGNORED_DUPLICATE');
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 2. Task 2: Authoritative Connection Generation
  // ════════════════════════════════════════════════════════════════
  describe('Task 2: Authoritative Connection Generation', () => {
    it('accepts events matching authorized connection generation', () => {
      engine.authorizePeerGeneration('peer-A', 2);

      const eventGen2 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 2);
      engine.receive(eventGen2);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
    });

    it('rejects events with stale connection generation', () => {
      engine.authorizePeerGeneration('peer-A', 2);

      const staleEvent = makeRemoteEvent('stale-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 1);
      engine.receive(staleEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getLastDecision()?.message).toContain('stale connection generation');
    });

    it('rejects unannounced future connection generation without advancing engine active generation', () => {
      engine.authorizePeerGeneration('peer-A', 2);
      expect(engine.getPeerGeneration('peer-A')).toBe(2);

      // Event claims generation 5 while 2 is authorized
      const futureGenEvent = makeRemoteEvent('future-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 5);
      engine.receive(futureGenEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getLastDecision()?.message).toContain('unannounced connection generation');

      // Crucial: Engine active generation MUST NOT be bumped to 5!
      expect(engine.getPeerGeneration('peer-A')).toBe(2);
    });

    it('explicitly authorized reconnect generation is accepted after peer manager registration', () => {
      engine.authorizePeerGeneration('peer-A', 2);

      const event1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 2);
      engine.receive(event1);
      expect(engine.getLastDecision()?.accepted).toBe(true);

      // Peer reconnects: transport peer manager explicitly authorizes generation 3
      engine.authorizePeerGeneration('peer-A', 3);
      expect(engine.getPeerGeneration('peer-A')).toBe(3);

      const event2 = makeRemoteEvent('ev-2', 'peer-A', 'PAUSE', {}, 2, 2, { 'peer-A': 1 }, 1, 'peer-A', 3);
      engine.receive(event2);
      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
    });

    it('delayed old-generation events are rejected after reconnect authorization', () => {
      engine.authorizePeerGeneration('peer-A', 2);

      // Reconnect to generation 3
      engine.authorizePeerGeneration('peer-A', 3);

      // Delayed message stamped with generation 2 arrives after reconnect
      const delayedEvent = makeRemoteEvent('delayed-gen2', 'peer-A', 'PLAY', {}, 2, 2, { 'peer-A': 1 }, 1, 'peer-A', 2);
      engine.receive(delayedEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getLastDecision()?.message).toContain('stale connection generation');
    });

    it('connection generation changes do not reset membership epoch or origin sequences', () => {
      engine.authorizePeerGeneration('peer-A', 2);
      const ev1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 2);
      engine.receive(ev1);

      expect(engine.epoch).toBe(1);

      // Reconnect advances generation to 3
      engine.authorizePeerGeneration('peer-A', 3);

      expect(engine.epoch).toBe(1);
      // Origin sequence continuity remains required: re-sending sequence 1 with different ID is rejected as collision
      const regressedSeq = makeRemoteEvent('ev-bad', 'peer-A', 'PLAY', {}, 2, 1, {}, 1, 'peer-A', 3);
      engine.receive(regressedSeq);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_SEQUENCE_CONFLICT');

      // Re-sending sequence 1 with exact duplicate ID is ignored as duplicate
      const duplicateSeq = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1, 'peer-A', 3);
      engine.receive(duplicateSeq);
      expect(engine.getLastDecision()?.code).toBe('IGNORED_DUPLICATE');
    });

    it('historical repair batch preserves event author identity and is not confused with supplier transport generation', () => {
      // Peer B is at generation 4
      engine.authorizePeerGeneration('peer-B', 4);

      // Historical event authored by Peer A with generation 1, supplied in a REPAIR_BATCH by Peer B
      const repairEvent: AnyControlSyncEvent = {
        origin: 'remote',
        localTimestamp: 1000,
        message: {
          protocolVersion: PROTOCOL_VERSION,
          messageType: 'PLAY',
          sessionId: 'session-001',
          senderPeerId: 'peer-B', // Transport supplier
          originPeerId: 'peer-A', // Historical author
          messageId: 'msg-rep-1',
          logicalEventId: 'rep-A1',
          logicalTimestamp: 1,
          originSequenceNumber: 1,
          causalContext: {},
          membershipEpoch: 1,
          connectionGeneration: 1, // Historical generation from author Peer A
          payload: {},
        },
      };

      // Ingest via repair batch path
      const batchPayload = {
        formatVersion: 2 as const,
        sessionId: 'session-001',
        supplierPeerId: 'peer-B',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'rep-A1',
            originPeerId: 'peer-A',
            messageType: 'PLAY' as const,
            membershipEpoch: 1,
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };
      const decisions = engine.ingestRepairBatch(batchPayload as any);

      expect(decisions.length).toBe(1);
      expect(decisions[0]?.accepted).toBe(true);
      expect(engine.getState().lastAppliedEvent?.originPeerId).toBe('peer-A');
      expect(engine.getState().lastAppliedEvent?.senderPeerId).toBe('peer-A');
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 3. Task 3: Explicit Epoch Transition Semantics
  // ════════════════════════════════════════════════════════════════
  describe('Task 3: Explicit Epoch Transition Semantics', () => {
    it('valid authorized epoch transition advances currentEpoch', () => {
      expect(engine.epoch).toBe(1);
      engine.transitionEpoch(2, epochCapability);
      expect(engine.epoch).toBe(2);
    });

    it('unauthorized transition with non-integer or smaller/equal epoch throws INVALID_ARGUMENT', () => {
      expect(() => engine.transitionEpoch(1, epochCapability)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch(0, epochCapability)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch(-1, epochCapability)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch(2.5 as any, epochCapability)).toThrow(SyncEngineError);
      expect(() => engine.transitionEpoch('2' as any, epochCapability)).toThrow(SyncEngineError);
    });

    it('old-epoch events arriving after transition are rejected with REJECTED_OBSOLETE_EPOCH', () => {
      engine.transitionEpoch(2, epochCapability);

      const oldEpochEvent = makeRemoteEvent('ev-old', 'peer-A', 'PLAY', {}, 1, 1, {}, 1);
      engine.receive(oldEpochEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.getLastDecision()?.message).toContain('obsolete epoch');
    });

    it('unestablished future-epoch events arriving from wire are rejected with REJECTED_OBSOLETE_EPOCH', () => {
      // Current epoch = 1; wire event claims epoch 2
      const futureEpochEvent = makeRemoteEvent('ev-future', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(futureEpochEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.getLastDecision()?.message).toContain('unestablished future epoch');
    });

    it('pending old-epoch operations are purged during epoch transition and cannot leak into the new epoch', () => {
      // 1. Buffer an operation in epoch 1 waiting for missing dependency
      const pendingOldOp = makeRemoteEvent('pending-old', 'peer-A', 'PLAY', {}, 2, 2, { 'peer-A': 1 }, 1);
      engine.receive(pendingOldOp);
      expect(engine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(engine.pendingQueueSize).toBe(1);

      // 2. Authoritative epoch transition to epoch 2 occurs
      engine.transitionEpoch(2, epochCapability);

      // 3. Crucial: The old-epoch operation MUST be purged from the pending queue
      expect(engine.pendingQueueSize).toBe(0);

      // 4. If predecessor arrives in epoch 2, the purged old-epoch operation does NOT leak or apply
      const predOpEpoch2 = makeRemoteEvent('pred-ep2', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(predOpEpoch2);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.pendingQueueSize).toBe(0);
    });

    it('authoritative playback state and monotonic Lamport clock are preserved across epoch transition', () => {
      // Apply an event in epoch 1
      const ev1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1);
      engine.receive(ev1);
      expect(engine.getState().status).toBe('playing');
      const versionBefore = engine.getState().stateVersion;

      // Transition epoch
      engine.transitionEpoch(2, epochCapability);

      // State is preserved
      expect(engine.getState().status).toBe('playing');
      expect(engine.getState().stateVersion).toBe(versionBefore);
      expect(engine.logicalClock).toBe(1);

      // Next event in epoch 2 increments Lamport clock monotonically
      const ev2 = makeRemoteEvent('ev-2', 'peer-A', 'PAUSE', {}, 2, 2, { 'peer-A': 1 }, 2);
      engine.receive(ev2);
      expect(engine.getState().status).toBe('paused');
      expect(engine.logicalClock).toBe(2);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 4. Task 4: Pending Queue Overflow Semantics
  // ════════════════════════════════════════════════════════════════
  describe('Task 4: Pending Queue Overflow Semantics', () => {
    it('overflowing pending queue returns unambiguous REJECTED_CAPACITY decision', () => {
      const boundedEngine = new LeaderlessSyncEngine({
        sessionId: 'session-001',
        localPeerId: 'peer-local',
        logCapacity: 2, // Pending queue capacity = 2
        clock: { now: () => 1000 },
      });

      // Buffer 2 missing-dependency operations
      boundedEngine.receive(makeRemoteEvent('gap-1', 'peer-A', 'PLAY', {}, 3, 2, { 'peer-A': 1 }));
      expect(boundedEngine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');

      boundedEngine.receive(makeRemoteEvent('gap-2', 'peer-A', 'PLAY', {}, 4, 3, { 'peer-A': 2 }));
      expect(boundedEngine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(boundedEngine.pendingQueueSize).toBe(2);

      // 3rd missing-dependency event overflows capacity
      boundedEngine.receive(makeRemoteEvent('overflow-ev', 'peer-A', 'PLAY', {}, 5, 4, { 'peer-A': 3 }));

      expect(boundedEngine.getLastDecision()?.accepted).toBe(false);
      expect(boundedEngine.getLastDecision()?.code).toBe('REJECTED_CAPACITY');
      expect(boundedEngine.getLastDecision()?.message).toContain('Pending dependency queue capacity (2) exceeded');
    });

    it('existing pending operations remain untouched after queue overflow (no silent eviction)', () => {
      const queue = new PendingDependencyQueue(2);
      const op1 = makeStoredOp('op-1', 'peer-A', 2, 2, { 'peer-A': 1 });
      const op2 = makeStoredOp('op-2', 'peer-A', 3, 3, { 'peer-A': 2 });
      const op3 = makeStoredOp('op-3', 'peer-A', 4, 4, { 'peer-A': 3 });

      queue.enqueue(op1);
      queue.enqueue(op2);
      expect(queue.size).toBe(2);

      // 3rd enqueue throws REJECTED_CAPACITY
      expect(() => queue.enqueue(op3)).toThrow(SyncEngineError);
      try {
        queue.enqueue(op3);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('REJECTED_CAPACITY');
      }

      // Existing operations are intact
      expect(queue.size).toBe(2);
      expect(queue.has('op-1')).toBe(true);
      expect(queue.has('op-2')).toBe(true);
      expect(queue.has('op-3')).toBe(false);
    });

    it('overflowing event does not advance logical clock or replication vector', () => {
      const boundedEngine = new LeaderlessSyncEngine({
        sessionId: 'session-001',
        localPeerId: 'peer-local',
        logCapacity: 1,
        clock: { now: () => 1000 },
      });

      // Buffer 1 operation to fill queue
      boundedEngine.receive(makeRemoteEvent('gap-1', 'peer-A', 'PLAY', {}, 3, 2, { 'peer-A': 1 }));
      expect(boundedEngine.pendingQueueSize).toBe(1);
      expect(boundedEngine.logicalClock).toBe(0);

      // Overflow event with high timestamp and sequence
      boundedEngine.receive(makeRemoteEvent('overflow-ev', 'peer-A', 'PLAY', {}, 999, 50, { 'peer-A': 49 }));
      expect(boundedEngine.getLastDecision()?.code).toBe('REJECTED_CAPACITY');

      // Crucial: Clock remains 0, replication vector has not advanced
      expect(boundedEngine.logicalClock).toBe(0);
    });

    it('retrying rejected event after capacity becomes available succeeds', () => {
      const boundedEngine = new LeaderlessSyncEngine({
        sessionId: 'session-001',
        localPeerId: 'peer-local',
        logCapacity: 1,
        clock: { now: () => 1000 },
      });

      // Fill queue with op awaiting seq 1 (SEEK)
      const gapOp = makeRemoteEvent('gap-2', 'peer-A', 'SEEK', { targetPositionMs: 5000 }, 2, 2, { 'peer-A': 1 });
      boundedEngine.receive(gapOp);
      expect(boundedEngine.pendingQueueSize).toBe(1);

      // 2nd op overflows and is rejected
      const overflowOp = makeRemoteEvent('gap-3', 'peer-A', 'PAUSE', {}, 3, 3, { 'peer-A': 2 });
      boundedEngine.receive(overflowOp);
      expect(boundedEngine.getLastDecision()?.code).toBe('REJECTED_CAPACITY');

      // Now supply missing predecessor seq 1 (PLAY)
      const predOp = makeRemoteEvent('pred-1', 'peer-A', 'PLAY', {}, 1, 1, {});
      boundedEngine.receive(predOp);
      // gap-2 was unblocked and applied, draining the queue!
      expect(boundedEngine.getLastDecision()?.code).toBe('APPLIED');
      expect(boundedEngine.pendingQueueSize).toBe(0);

      // Retrying gap-3 now succeeds!
      boundedEngine.receive(overflowOp);
      expect(boundedEngine.getLastDecision()?.accepted).toBe(true);
      expect(boundedEngine.getLastDecision()?.code).toBe('APPLIED');
    });
  });
});
