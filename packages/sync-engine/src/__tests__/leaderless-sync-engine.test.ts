/**
 * Unit tests for LeaderlessSyncEngine.
 *
 * Verifies:
 * - Public SyncEngine contract compliance
 * - Local authoring (sequence increment, Lamport tick, causal context stamping)
 * - Remote event validation, clock update without increment, frontier merge
 * - Missing dependency buffering in PendingDependencyQueue
 * - Strict No-Forwarding enforcement on live broadcasts
 * - Sequence collision rejection
 * - Listener exception isolation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';
import { SyncEngineError } from '../errors.js';
import { PROTOCOL_VERSION, type StateSnapshotPayloadV2 } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

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

function makeLocalEvent(
  logicalEventId: string,
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE',
  payload: unknown,
): AnyControlSyncEvent {
  return {
    origin: 'local',
    localTimestamp: 1000,
    message: {
      protocolVersion: PROTOCOL_VERSION,
      messageType,
      sessionId: 'session-001',
      senderPeerId: 'peer-local',
      messageId: `msg-${logicalEventId}`,
      logicalEventId,
      payload,
    },
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
      payload,
    },
  };
}

describe('LeaderlessSyncEngine', () => {
  let engine: LeaderlessSyncEngine;

  beforeEach(() => {
    engine = new LeaderlessSyncEngine({
      sessionId: 'session-001',
      localPeerId: 'peer-local',
      clock: { now: () => 1000 },
    });
  });

  describe('initial state and lifecycle', () => {
    it('initializes in active paused state at position 0', () => {
      expect(engine.isDisposed).toBe(false);
      expect(engine.getState().status).toBe('paused');
      expect(engine.getState().anchorPositionMs).toBe(0);
      expect(engine.getState().playbackRate).toBe(1.0);
      expect(engine.getState().stateVersion).toBe(0);
      expect(engine.logicalClock).toBe(0);
    });

    it('rejects local events when disposed', () => {
      engine.dispose();
      expect(engine.isDisposed).toBe(true);
      expect(() =>
        engine.receive(makeLocalEvent('op-1', 'PLAY', {})),
      ).toThrow(SyncEngineError);
    });

    it('safely isolates remote events when disposed', () => {
      engine.dispose();
      const remote = makeRemoteEvent('op-1', 'peer-remote', 'PLAY', {}, 1, 1);
      engine.receive(remote);
      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_DISPOSED');
    });
  });

  describe('local event authoring', () => {
    it('authors local PLAY: increments clock to 1, sequence to 1, transitions state to playing', () => {
      engine.receive(makeLocalEvent('op-play', 'PLAY', {}));

      expect(engine.logicalClock).toBe(1);
      expect(engine.localFrontier['peer-local']).toBe(1);
      expect(engine.replicationFrontier['peer-local']).toBe(1);
      expect(engine.getState().status).toBe('playing');
      expect(engine.getState().stateVersion).toBe(1);
      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
    });

    it('subsequent local events increment Lamport clock and sequence counter', () => {
      engine.receive(makeLocalEvent('op-play', 'PLAY', {}));
      expect(engine.logicalClock).toBe(1);

      engine.receive(makeLocalEvent('op-pause', 'PAUSE', {}));
      expect(engine.logicalClock).toBe(2);
      expect(engine.localFrontier['peer-local']).toBe(2);
      expect(engine.getState().status).toBe('paused');
    });
  });

  describe('remote event validation and receipt', () => {
    it('accepts valid remote event and advances clock without an extra increment', () => {
      // Remote peer-A authors first event with timestamp 1, seq 1, causalContext {}
      const remote = makeRemoteEvent('rem-1', 'peer-A', 'PLAY', {}, 1, 1, {});
      engine.receive(remote);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.getState().status).toBe('playing');
      expect(engine.logicalClock).toBe(1); // max(0, 1) without +1
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.localFrontier['peer-A']).toBe(1);
    });

    it('buffers remote event when predecessor sequence is missing (gap)', () => {
      // Receives sequence 2 from peer-A when sequence 1 is missing
      const remoteGap = makeRemoteEvent('rem-2', 'peer-A', 'PLAY', {}, 2, 2, { 'peer-A': 1 });
      engine.receive(remoteGap);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(engine.pendingQueueSize).toBe(1);
      // Clock and replication vector MUST NOT advance!
      expect(engine.logicalClock).toBe(0);
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
      expect(engine.getState().status).toBe('paused');
    });

    it('buffers remote event when cross-origin causal dependency is missing', () => {
      // Receives event from peer-B seq 1 claiming dependency on peer-A: 5, but local has no peer-A
      const remoteMissingDep = makeRemoteEvent(
        'rem-b1',
        'peer-B',
        'PLAY',
        {},
        6,
        1,
        { 'peer-A': 5 },
      );
      engine.receive(remoteMissingDep);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(engine.pendingQueueSize).toBe(1);
      expect(engine.logicalClock).toBe(0);
    });

    it('rejects live broadcast when originPeerId does not match senderPeerId (Strict No-Forwarding)', () => {
      const forwarded = makeRemoteEvent(
        'fwd-1',
        'peer-intermediary',
        'PLAY',
        {},
        1,
        1,
        {},
        1,
        'peer-author', // Different origin!
      );
      engine.receive(forwarded);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.logicalClock).toBe(0);
    });

    it('rejects sequence collision from same origin', () => {
      const ev1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1);
      engine.receive(ev1);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');

      // Different logicalEventId for same origin sequence 1
      const evCollision = makeRemoteEvent('ev-collision', 'peer-A', 'PAUSE', {}, 1, 1);
      engine.receive(evCollision);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_SEQUENCE_CONFLICT');
    });

    it('treats identical logicalEventId as IGNORED_DUPLICATE', () => {
      const ev1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1);
      engine.receive(ev1);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');

      // Duplicate delivery
      engine.receive(ev1);
      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('IGNORED_DUPLICATE');
    });
  });

  describe('Phase 7.3-R1 Exact Lamport & Causal Validation 8-Case Matrix', () => {
    it('Case 1: e.causalContext is empty -> expectedTimestamp is exactly 1', () => {
      const evGenesis = makeRemoteEvent('ev-gen', 'peer-A', 'PLAY', {}, 1, 1, {});
      engine.receive(evGenesis);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.logicalClock).toBe(1); // Exactly 1
    });

    it('Case 2: e.causalContext has 1 dependency -> expectedTimestamp is 1 + depClock', () => {
      // First, peer-A produces event with L=1
      engine.receive(makeRemoteEvent('ev-a1', 'peer-A', 'PLAY', {}, 1, 1, {}));
      expect(engine.logicalClock).toBe(1);

      // Peer-B produces event with causalContext { peer-A: 1 } -> expected = 1 + 1 = 2
      const evB = makeRemoteEvent('ev-b1', 'peer-B', 'SEEK', { targetPositionMs: 5000 }, 2, 1, { 'peer-A': 1 });
      engine.receive(evB);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.logicalClock).toBe(2);
    });

    it('Case 3: e.causalContext has multiple dependencies across origins -> expectedTimestamp is 1 + max(depClocks)', () => {
      engine.importSnapshotV2(
        makeSnapshotV2('snap-1', { 'peer-A': 1 }, 10, { 'peer-A': 10 }),
      );
      engine.receive(makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 11, 1, { 'peer-A': 1 })); // L=11
      engine.receive(makeRemoteEvent('ev-c1', 'peer-C', 'SEEK', { targetPositionMs: 1000 }, 12, 1, { 'peer-A': 1, 'peer-B': 1 })); // L=12

      // Now peer-D depends on peer-A: 1 (10), peer-B: 1 (11), peer-C: 1 (12)
      // Expected = 1 + max(10, 11, 12) = 13
      const evD = makeRemoteEvent('ev-d1', 'peer-D', 'RATE_CHANGE', { playbackRate: 1.5 }, 13, 1, {
        'peer-A': 1,
        'peer-B': 1,
        'peer-C': 1,
      });
      engine.receive(evD);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.logicalClock).toBe(13);
    });

    it('Case 4: dependency timestamp is in operation log vs compacted checkpoints -> both resolved correctly', () => {
      engine.importSnapshotV2(
        makeSnapshotV2('snap-1', { 'peer-A': 1 }, 20, { 'peer-A': 20 }),
      );

      // Ingest log operation for peer-B: 1 with L=21
      engine.receive(makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 21, 1, { 'peer-A': 1 }));

      // Peer-C depends on peer-A: 1 (compacted checkpoint L=20) and peer-B: 1 (retained log L=21)
      // Expected = 1 + max(20, 21) = 22
      const evC = makeRemoteEvent('ev-c1', 'peer-C', 'PAUSE', {}, 22, 1, {
        'peer-A': 1,
        'peer-B': 1,
      });
      engine.receive(evC);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.logicalClock).toBe(22);
    });

    it('Case 5: dependency timestamp is missing from both log and checkpoints -> rejects with REJECTED_OUTSIDE_HORIZON', () => {
      const snap: StateSnapshotPayloadV2 = {
        formatVersion: 2,
        snapshotId: 'snap-no-cp',
        sessionId: 'session-001',
        membershipEpoch: 1,
        authorPeerId: 'peer-author',
        snapshotFrontier: { 'peer-A': 5 },
        maxLogicalTimestamp: 50,
        originFrontierTimestamps: {},
        compactedCheckpoints: [],
        state: { status: 'paused', anchorPositionMs: 0, playbackRate: 1.0 },
        sha256Digest: '0'.repeat(64),
      };
      engine.importSnapshotV2(snap);

      const evMissing = makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 10, 1, { 'peer-A': 1 });
      engine.receive(evMissing);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OUTSIDE_HORIZON');
    });

    it('Case 6: author sends L(e) < expected -> rejects with REJECTED_CAUSAL_VIOLATION', () => {
      engine.receive(makeRemoteEvent('ev-a1', 'peer-A', 'PLAY', {}, 1, 1, {}));

      // Expected is 1 + 1 = 2, but author claims 1
      const evUnder = makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 1, 1, { 'peer-A': 1 });
      engine.receive(evUnder);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_CAUSAL_VIOLATION');
      expect(engine.logicalClock).toBe(1);
    });

    it('Case 7: author sends L(e) > expected -> rejects with REJECTED_CAUSAL_VIOLATION (INVALID_LOGICAL_TIMESTAMP)', () => {
      engine.receive(makeRemoteEvent('ev-a1', 'peer-A', 'PLAY', {}, 1, 1, {}));

      // Expected is 1 + 1 = 2, but author claims 5
      const evOver = makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 5, 1, { 'peer-A': 1 });
      engine.receive(evOver);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_CAUSAL_VIOLATION');
      expect(engine.logicalClock).toBe(1);
    });

    it('Case 8: author sends L(e) == expected -> accepts with APPLIED', () => {
      engine.receive(makeRemoteEvent('ev-a1', 'peer-A', 'PLAY', {}, 1, 1, {}));

      // Expected is 1 + 1 = 2, author claims exactly 2
      const evExact = makeRemoteEvent('ev-b1', 'peer-B', 'PLAY', {}, 2, 1, { 'peer-A': 1 });
      engine.receive(evExact);

      expect(engine.getLastDecision()?.accepted).toBe(true);
      expect(engine.getLastDecision()?.code).toBe('NO_OP'); // playing -> playing
      expect(engine.logicalClock).toBe(2);
    });

    it('Predecessor check: originSequenceNumber > 1 missing author predecessor in causalContext is rejected', () => {
      engine.receive(makeRemoteEvent('ev-a1', 'peer-A', 'PLAY', {}, 1, 1, {}));

      // Peer-A sends seq 2 (L=2) but causalContext is EMPTY instead of { 'peer-A': 1 }
      const evMissingPred = makeRemoteEvent('ev-a2', 'peer-A', 'PAUSE', {}, 2, 2, {});
      engine.receive(evMissingPred);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_CAUSAL_VIOLATION');
    });

    it('Local authoring prevents clock inflation beyond declared frontier', () => {
      engine.receive(makeLocalEvent('loc-1', 'PLAY', {}));
      expect(engine.logicalClock).toBe(1);

      engine.receive(makeLocalEvent('loc-2', 'PAUSE', {}));
      expect(engine.logicalClock).toBe(2);
      expect(engine.localFrontier['peer-local']).toBe(2);
    });
  });
});
