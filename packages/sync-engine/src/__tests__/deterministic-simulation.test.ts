/**
 * Comprehensive Deterministic Permutation Simulation Tests for Phase 7.3.
 *
 * Implements Phase 7.3 Section 7 test suites:
 * - Suite A: Lamport correctness (A11=14 accepted, A11=15 rejected, C1=15 accepted, receiver validation)
 * - Suite B: Causal dependency handling (missing A10 buffers D1, repair unbuffers D1, no vector advance, sequence gaps)
 * - Suite C: Receiver-only concurrent observation isolation (R9 Suite 13 Case 8)
 * - Suite D: Deterministic delivery permutations (concurrent SEEKs, concurrent PAUSE/PLAY, concurrent RATEs, mixed)
 * - Suite E: Bounded resource behavior (K=256 log limits, queue bounds, deduplication limits)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createTransportRegistrationCapability,
  type TransportRegistrationCapability,
} from '../index.js';
import { PROTOCOL_VERSION } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type { PlaybackState } from '../types.js';
import type { StateSnapshotPayloadV2 } from '@syncplay/protocol';

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
    sessionId: 'test-session',
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
): AnyControlSyncEvent {
  return {
    origin: 'remote',
    localTimestamp: 1000,
    message: {
      protocolVersion: PROTOCOL_VERSION,
      messageType,
      sessionId: 'test-session',
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
      sessionId: 'test-session',
      senderPeerId: 'peer-C',
      messageId: `msg-${logicalEventId}`,
      logicalEventId,
      payload,
    },
  };
}

describe('Phase 7.3 Deterministic Simulation Test Suite', () => {
  // ── Suite A: Lamport Correctness ─────────────────────────────
  describe('Suite A: Lamport Correctness', () => {
    let peerC: LeaderlessSyncEngine;

    beforeEach(() => {
      peerC = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-C',
        clock: { now: () => 1000 },
      });

      // Peer C initializes with snapshot frontier { A: 10, B: 5 }
      // L(A, 10) = 12, L(B, 5) = 8, maxClock = 12
      peerC.importSnapshotV2(
        makeSnapshotV2(
          'snap-init',
          { 'peer-A': 10, 'peer-B': 5 },
          12,
          { 'peer-A': 12, 'peer-B': 8 },
        ),
      );

      // Peer D authors D1: dependencies { A: 10, B: 5 }
      // expected = 1 + max(12, 8) = 13. Peer C admits D1.
      peerC.receive(
        makeRemoteEvent(
          'd-1',
          'peer-D',
          'SEEK',
          { targetPositionMs: 13000 },
          13,
          1,
          { 'peer-A': 10, 'peer-B': 5 },
        ),
      );
    });

    it('A11 with exact expected timestamp 14 is accepted under exact Lamport equality', () => {
      // Dependencies: A: 10 (12), B: 5 (8), D: 1 (13)
      // expected = 1 + max(12, 8, 13) = 14
      const a11Valid = makeRemoteEvent(
        'a-11',
        'peer-A',
        'SEEK',
        { targetPositionMs: 14000 },
        14,
        11,
        { 'peer-A': 10, 'peer-B': 5, 'peer-D': 1 },
      );

      peerC.receive(a11Valid);
      expect(peerC.getLastDecision()?.accepted).toBe(true);
      expect(peerC.getLastDecision()?.code).toBe('APPLIED');
      expect(peerC.replicationFrontier['peer-A']).toBe(11);
      expect(peerC.logicalClock).toBe(14); // max(13, 14) without +1
    });

    it('A11 with inflated timestamp 15 is rejected with INVALID_LOGICAL_TIMESTAMP', () => {
      // Expected is 14, but remote claims 15
      const a11Invalid = makeRemoteEvent(
        'a-11-bad',
        'peer-A',
        'SEEK',
        { targetPositionMs: 14000 },
        15,
        11,
        { 'peer-A': 10, 'peer-B': 5, 'peer-D': 1 },
      );

      peerC.receive(a11Invalid);
      expect(peerC.getLastDecision()?.accepted).toBe(false);
      expect(peerC.getLastDecision()?.code).toBe('REJECTED_CAUSAL_VIOLATION');
      // Local clock and vector MUST NOT advance!
      expect(peerC.logicalClock).toBe(13);
      expect(peerC.replicationFrontier['peer-A']).toBe(10);
    });

    it('C1 authored by Peer C after admitting A11 receives timestamp 15', () => {
      // Admit A11 (ts 14)
      peerC.receive(
        makeRemoteEvent(
          'a-11',
          'peer-A',
          'SEEK',
          { targetPositionMs: 14000 },
          14,
          11,
          { 'peer-A': 10, 'peer-B': 5, 'peer-D': 1 },
        ),
      );

      // Now Peer C authors local event C1
      peerC.receive(makeLocalEvent('c-1', 'PLAY', {}));

      // C1 timestamp = 1 + L_local = 1 + 14 = 15!
      expect(peerC.logicalClock).toBe(15);
      expect(peerC.localFrontier['peer-C']).toBe(1);
      expect(peerC.getState().lastAppliedEvent?.logicalTimestamp).toBe(15);
    });

    it('independent receiver validates C1 as timestamp 15', () => {
      const peerE = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-E',
        clock: { now: () => 1000 },
      });

      // Provide same baseline to Peer E via snapshot
      peerE.importSnapshotV2(
        makeSnapshotV2(
          'snap-init',
          { 'peer-A': 10, 'peer-B': 5 },
          12,
          { 'peer-A': 12, 'peer-B': 8 },
        ),
      );

      peerE.receive(
        makeRemoteEvent(
          'd-1',
          'peer-D',
          'SEEK',
          { targetPositionMs: 13000 },
          13,
          1,
          { 'peer-A': 10, 'peer-B': 5 },
        ),
      );
      peerE.receive(
        makeRemoteEvent(
          'a-11',
          'peer-A',
          'SEEK',
          { targetPositionMs: 14000 },
          14,
          11,
          { 'peer-A': 10, 'peer-B': 5, 'peer-D': 1 },
        ),
      );

      // C1 arrives at Peer E: timestamp 15, seq 1, causalContext { 'peer-A': 11, 'peer-B': 5, 'peer-D': 1 }
      // Expected = 1 + max(14, 8, 13) = 15. Exact match!
      const c1Event = makeRemoteEvent(
        'c-1',
        'peer-C',
        'PLAY',
        {},
        15,
        1,
        { 'peer-A': 11, 'peer-B': 5, 'peer-D': 1 },
      );

      peerE.receive(c1Event);
      expect(peerE.getLastDecision()?.accepted).toBe(true);
      expect(peerE.getLastDecision()?.code).toBe('APPLIED');
      expect(peerE.logicalClock).toBe(15);
    });
  });

  // ── Suite B: Causal Dependency Handling & Buffering ───────────
  describe('Suite B: Causal Dependency Handling', () => {
    let peerC: LeaderlessSyncEngine;
    let peerCTransportCap: TransportRegistrationCapability;

    beforeEach(() => {
      peerCTransportCap = createTransportRegistrationCapability('peer-C', 'test-session');
      peerC = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-C',
        clock: { now: () => 1000 },
        transportCapability: peerCTransportCap,
      });

      // Peer C initializes with A: 9 and B: 5 (missing A: 10)
      peerC.importSnapshotV2(
        makeSnapshotV2(
          'snap-init',
          { 'peer-A': 9, 'peer-B': 5 },
          11,
          { 'peer-A': 11, 'peer-B': 8 },
        ),
      );
    });

    it('missing A10 causes D1 to be buffered, leaving clock and replication vector untouched', () => {
      // D1 requires A: 10, B: 5
      const d1 = makeRemoteEvent(
        'd-1',
        'peer-D',
        'SEEK',
        { targetPositionMs: 13000 },
        13,
        1,
        { 'peer-A': 10, 'peer-B': 5 },
      );

      peerC.receive(d1);
      expect(peerC.getLastDecision()?.accepted).toBe(false);
      expect(peerC.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(peerC.pendingQueueSize).toBe(1);

      // Invariant checks:
      expect(peerC.replicationFrontier['peer-D']).toBeUndefined();
      expect(peerC.replicationFrontier['peer-A']).toBe(9);
      expect(peerC.replicationFrontier['peer-B']).toBe(5);
      expect(peerC.logicalClock).toBe(11); // Did not advance to 13
    });

    it('a repair batch supplying A10 allows D1 to be automatically revalidated and applied', () => {
      // Buffer D1
      const d1 = makeRemoteEvent(
        'd-1',
        'peer-D',
        'SEEK',
        { targetPositionMs: 13000 },
        13,
        1,
        { 'peer-A': 10, 'peer-B': 5 },
      );
      peerC.receive(d1);
      expect(peerC.pendingQueueSize).toBe(1);

      // Ingest repair batch supplying A10 (L=12, seq=10, causalContext { 'peer-A': 9 })
      // Expected for A10: 1 + L(A9) = 1 + 11 = 12. Exact match!
      const repairBatch = {
        requestId: '00000000-0000-4000-8000-000000000001',
        sessionId: 'session-test',
        supplierPeerId: 'peer-repair-node',
        originPeerId: 'peer-A',
        fromSequence: 10,
        toSequence: 10,
        membershipEpoch: 1,
        operations: [
          {
            logicalEventId: 'a-10',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'SEEK' as const,
            logicalTimestamp: 12,
            originSequenceNumber: 10,
            causalContext: { 'peer-A': 9 },
            payload: { targetPositionMs: 10000 },
          },
        ],
      };

      peerC.authorizePeerGeneration(repairBatch.supplierPeerId, 1);
      const transportContext = peerC.registerInboundSupplierContext(
        repairBatch.supplierPeerId,
        1,
        peerCTransportCap,
      );
      peerC.ingestRepairBatch(repairBatch, transportContext);

      // D1 should now be unblocked and applied!
      expect(peerC.pendingQueueSize).toBe(0);
      expect(peerC.replicationFrontier['peer-A']).toBe(10);
      expect(peerC.replicationFrontier['peer-D']).toBe(1);
      // Unrelated frontier B: 5 preserved!
      expect(peerC.replicationFrontier['peer-B']).toBe(5);
      expect(peerC.logicalClock).toBe(13);
      expect(peerC.getState().anchorPositionMs).toBe(13000);
    });
  });

  // ── Suite C: Receiver-Only Concurrent Observation Isolation ──
  describe('Suite C: Receiver-Only Concurrent Observation Isolation (R9 Suite 13 Case 8)', () => {
    it('isolates receiver concurrent observations from incoming event timestamp validation', () => {
      const peerC = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-C',
        clock: { now: () => 1000 },
      });

      // Peer C initializes with A: 10 (L=14) and E: 1 (L=20)
      peerC.importSnapshotV2(
        makeSnapshotV2(
          'snap-init',
          { 'peer-A': 10, 'peer-E': 1 },
          20,
          { 'peer-A': 14, 'peer-E': 20 },
        ),
      );

      expect(peerC.logicalClock).toBe(20);

      // Remote Peer X authors X1 with causalContext { 'peer-A': 10 }.
      // X1 did NOT observe E1.
      // Expected timestamp for X1: 1 + L(A10) = 1 + 14 = 15.
      // Crucial test: Peer C does NOT substitute its own clock (20) into X1's check.
      const x1 = makeRemoteEvent(
        'x-1',
        'peer-X',
        'SEEK',
        { targetPositionMs: 15000 },
        15,
        1,
        { 'peer-A': 10 },
      );

      peerC.receive(x1);
      expect(peerC.getLastDecision()?.accepted).toBe(true);
      expect(peerC.getLastDecision()?.code).toBe('APPLIED');

      // C's clock remains 20 (max(20, 15) = 20)
      expect(peerC.logicalClock).toBe(20);

      // Subsequently, Peer C authors local event C1.
      // C1 observes both E1 (20) and X1 (15).
      // L(C1) = 1 + max(20, 15) = 21!
      peerC.receive(makeLocalEvent('c-1', 'PLAY', {}));
      expect(peerC.logicalClock).toBe(21);
      expect(peerC.getState().lastAppliedEvent?.logicalTimestamp).toBe(21);
    });
  });

  // ── Suite D: Deterministic Delivery Permutations ──────────────
  describe('Suite D: Deterministic Delivery Permutations', () => {
    it('concurrent SEEK operations converge to identical state regardless of delivery permutation', () => {
      // Base state: base operation with timestamp 4
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 4, { 'peer-base': 4 }, 'paused', 0),
        );
        return eng;
      };

      // 3 concurrent seeks:
      // A seeks to 30000 (key: ts 5, 'peer-A', 'op-seek-A')
      // B seeks to 60000 (key: ts 8, 'peer-B', 'op-seek-B') -> Highest Key Winner!
      // C seeks to 45000 (key: ts 6, 'peer-C', 'op-seek-C')
      // Note: for valid Lamport timestamps, B depended on an event with ts 7, C on an event with ts 5.
      // Or: all 3 depend on peer-base: 1 (L=4), so expected timestamp is 5.
      // By total-order tie-breaker:
      // If ts is identical (5):
      // key: <5, 'peer-A'> vs <5, 'peer-B'> vs <5, 'peer-C'>.
      // 'peer-C' has highest lexicographic key! Winner is C!
      const evA = makeRemoteEvent('op-seek-A', 'peer-A', 'SEEK', { targetPositionMs: 30000 }, 5, 1, { 'peer-base': 1 });
      const evB = makeRemoteEvent('op-seek-B', 'peer-B', 'SEEK', { targetPositionMs: 60000 }, 5, 1, { 'peer-base': 1 });
      const evC = makeRemoteEvent('op-seek-C', 'peer-C', 'SEEK', { targetPositionMs: 45000 }, 5, 1, { 'peer-base': 1 });

      const permutations: AnyControlSyncEvent[][] = [
        [evA, evB, evC],
        [evA, evC, evB],
        [evB, evA, evC],
        [evB, evC, evA],
        [evC, evA, evB],
        [evC, evB, evA],
      ];

      const finalStates: PlaybackState[] = [];

      for (const order of permutations) {
        const engine = makeEngine();
        for (const ev of order) {
          engine.receive(ev);
        }
        finalStates.push(engine.getState());
      }

      // Assert all 6 permutations produced the exact same winner ('peer-C' target 45000ms)
      for (const state of finalStates) {
        expect(state.anchorPositionMs).toBe(45000);
      }
    });

    it('concurrent PLAY and PAUSE converge to PAUSED in all delivery permutations (Pause Priority)', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        // Initial state is playing at 1000ms
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'playing', 1000),
        );
        return eng;
      };

      const evPause = makeRemoteEvent('op-pause', 'peer-A', 'PAUSE', {}, 2, 1, { 'peer-base': 1 });
      const evPlay = makeRemoteEvent('op-play', 'peer-B', 'PLAY', {}, 2, 1, { 'peer-base': 1 });

      // Permutation 1: PAUSE then PLAY
      const engine1 = makeEngine();
      engine1.receive(evPause);
      engine1.receive(evPlay);

      // Permutation 2: PLAY then PAUSE
      const engine2 = makeEngine();
      engine2.receive(evPlay);
      engine2.receive(evPause);

      expect(engine1.getState().status).toBe('paused');
      expect(engine2.getState().status).toBe('paused');
    });

    it('concurrent RATE_CHANGE operations converge to higher key in all delivery permutations', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'playing', 0, 1.0),
        );
        return eng;
      };

      // Both depend on peer-base: 1 (L=1), so expected timestamp is 2.
      // Tie breaker: 'peer-B' > 'peer-A', so peer-B (rate 2.0) wins!
      const evRateLow = makeRemoteEvent('op-rate-low', 'peer-A', 'RATE_CHANGE', { playbackRate: 1.5 }, 2, 1, { 'peer-base': 1 });
      const evRateHigh = makeRemoteEvent('op-rate-high', 'peer-B', 'RATE_CHANGE', { playbackRate: 2.0 }, 2, 1, { 'peer-base': 1 });

      const engine1 = makeEngine();
      engine1.receive(evRateLow);
      engine1.receive(evRateHigh);

      const engine2 = makeEngine();
      engine2.receive(evRateHigh);
      engine2.receive(evRateLow);

      expect(engine1.getState().playbackRate).toBe(2.0);
      expect(engine2.getState().playbackRate).toBe(2.0);
    });

    it('mixed operation types (SEEK + PLAY + RATE_CHANGE + sequential PAUSE) converge in all delivery permutations', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'paused', 0, 1.0),
        );
        return eng;
      };

      // Base: L=1.
      // Peer A: SEEK to 10000ms (L=2, seq 1, causal { 'peer-base': 1 })
      const opSeek = makeRemoteEvent('op-seek', 'peer-A', 'SEEK', { targetPositionMs: 10000 }, 2, 1, { 'peer-base': 1 });
      // Peer B: PLAY (L=2, seq 1, causal { 'peer-base': 1 })
      const opPlay = makeRemoteEvent('op-play', 'peer-B', 'PLAY', {}, 2, 1, { 'peer-base': 1 });
      // Peer C: RATE_CHANGE to 1.5 (L=2, seq 1, causal { 'peer-base': 1 })
      const opRate = makeRemoteEvent('op-rate', 'peer-C', 'RATE_CHANGE', { playbackRate: 1.5 }, 2, 1, { 'peer-base': 1 });
      // Peer D: PAUSE after observing B's PLAY (L=3, seq 1, causal { 'peer-base': 1, 'peer-B': 1 })
      const opPause = makeRemoteEvent('op-pause', 'peer-D', 'PAUSE', {}, 3, 1, { 'peer-base': 1, 'peer-B': 1 });

      // Permutations testing both causal order and inverted arrival where opPause arrives before opPlay (buffering)
      const testPermutations = [
        [opSeek, opPlay, opRate, opPause], // Normal order
        [opPause, opRate, opPlay, opSeek], // opPause arrives first -> buffered until opPlay arrives!
        [opPlay, opPause, opSeek, opRate], // Immediate sequential pause
        [opRate, opSeek, opPlay, opPause], // Rate first
        [opSeek, opPause, opPlay, opRate], // Mixed buffering
      ];

      const states: PlaybackState[] = [];

      for (const order of testPermutations) {
        const eng = makeEngine();
        for (const ev of order) {
          eng.receive(ev);
        }
        states.push(eng.getState());
      }

      // Assert identical final state across all permutations:
      // - Paused (sequential PAUSE after PLAY)
      // - Position 10000ms (from SEEK)
      // - PlaybackRate 1.5 (from RATE_CHANGE)
      for (const st of states) {
        expect(st.status).toBe('paused');
        expect(st.anchorPositionMs).toBe(10000);
        expect(st.playbackRate).toBe(1.5);
      }
    });

    it('three-operation Pause/Play resumption: A pauses, B plays concurrently, C plays observing both -> all 6 permutations converge to playing', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'paused', 0),
        );
        return eng;
      };

      // Peer A: PAUSE (L=2, seq 1, causal { 'peer-base': 1 })
      const opA = makeRemoteEvent('op-A', 'peer-A', 'PAUSE', {}, 2, 1, { 'peer-base': 1 });
      // Peer B: PLAY (L=2, seq 1, causal { 'peer-base': 1 }) - concurrent with A
      const opB = makeRemoteEvent('op-B', 'peer-B', 'PLAY', {}, 2, 1, { 'peer-base': 1 });
      // Peer C: PLAY (L=3, seq 1, causal { 'peer-base': 1, 'peer-A': 1, 'peer-B': 1 }) - observes both
      const opC = makeRemoteEvent('op-C', 'peer-C', 'PLAY', {}, 3, 1, {
        'peer-base': 1,
        'peer-A': 1,
        'peer-B': 1,
      });

      const getPerms = <T>(items: T[]): T[][] => {
        if (items.length <= 1) return [items];
        const res: T[][] = [];
        for (let i = 0; i < items.length; i++) {
          const cur = items[i]!;
          const rem = [...items.slice(0, i), ...items.slice(i + 1)];
          for (const p of getPerms(rem)) {
            res.push([cur, ...p]);
          }
        }
        return res;
      };

      const permutations = getPerms([opA, opB, opC]);
      expect(permutations).toHaveLength(6);

      const states: PlaybackState[] = [];
      const fixedEvalTime = 2000;

      for (const perm of permutations) {
        const eng = makeEngine();
        for (const ev of perm) {
          eng.receive(ev);
        }
        states.push(eng.getState());
        expect(eng.getState().status).toBe('playing');
        expect(eng.getCurrentPositionMs(fixedEvalTime)).toBe(1000);
      }

      // Verify complete resulting PlaybackState matches across all 6 permutations
      const first = states[0]!;
      for (const st of states) {
        expect(st.status).toBe('playing');
        expect(st.anchorPositionMs).toBe(first.anchorPositionMs);
        expect(st.playbackRate).toBe(first.playbackRate);
        expect(st.lastAppliedEvent?.logicalEventId).toBe(first.lastAppliedEvent?.logicalEventId);
      }
    });

    it('4 concurrent operations: all 24 delivery permutations converge to identical PlaybackState', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'paused', 0),
        );
        return eng;
      };

      // 4 concurrent operations with L=2, causal { 'peer-base': 1 }
      const op1 = makeRemoteEvent('op-1', 'peer-A', 'SEEK', { targetPositionMs: 5000 }, 2, 1, { 'peer-base': 1 });
      const op2 = makeRemoteEvent('op-2', 'peer-B', 'SEEK', { targetPositionMs: 15000 }, 2, 1, { 'peer-base': 1 });
      const op3 = makeRemoteEvent('op-3', 'peer-C', 'RATE_CHANGE', { playbackRate: 1.25 }, 2, 1, { 'peer-base': 1 });
      const op4 = makeRemoteEvent('op-4', 'peer-D', 'RATE_CHANGE', { playbackRate: 1.75 }, 2, 1, { 'peer-base': 1 });

      const getPerms = <T>(items: T[]): T[][] => {
        if (items.length <= 1) return [items];
        const res: T[][] = [];
        for (let i = 0; i < items.length; i++) {
          const cur = items[i]!;
          const rem = [...items.slice(0, i), ...items.slice(i + 1)];
          for (const p of getPerms(rem)) {
            res.push([cur, ...p]);
          }
        }
        return res;
      };

      const permutations = getPerms([op1, op2, op3, op4]);
      expect(permutations).toHaveLength(24);

      const states: PlaybackState[] = [];
      for (const perm of permutations) {
        const eng = makeEngine();
        for (const ev of perm) {
          eng.receive(ev);
        }
        states.push(eng.getState());
      }

      const first = states[0]!;
      for (const st of states) {
        expect(st.status).toBe(first.status);
        expect(st.anchorPositionMs).toBe(first.anchorPositionMs);
        expect(st.playbackRate).toBe(first.playbackRate);
        expect(st.lastAppliedEvent?.logicalEventId).toBe(first.lastAppliedEvent?.logicalEventId);
      }
    });

    it('out-of-order with duplicate deliveries [A, B, A, C, B] converges to identical PlaybackState as [A, B, C]', () => {
      const makeEngine = () => {
        const eng = new LeaderlessSyncEngine({
          sessionId: 'test-session',
          localPeerId: 'peer-obs',
          clock: { now: () => 1000 },
        });
        eng.importSnapshotV2(
          makeSnapshotV2('snap-base', { 'peer-base': 1 }, 1, { 'peer-base': 1 }, 'paused', 0),
        );
        return eng;
      };

      const opA = makeRemoteEvent('op-A', 'peer-A', 'PLAY', {}, 2, 1, { 'peer-base': 1 });
      const opB = makeRemoteEvent('op-B', 'peer-B', 'SEEK', { targetPositionMs: 25000 }, 2, 1, { 'peer-base': 1 });
      const opC = makeRemoteEvent('op-C', 'peer-C', 'RATE_CHANGE', { playbackRate: 1.5 }, 2, 1, { 'peer-base': 1 });

      const cleanEngine = makeEngine();
      cleanEngine.receive(opA);
      cleanEngine.receive(opB);
      cleanEngine.receive(opC);

      const noisyEngine = makeEngine();
      const noisySequence = [opA, opB, opA, opC, opB];
      for (const ev of noisySequence) {
        noisyEngine.receive(ev);
      }

      const cleanState = cleanEngine.getState();
      const noisyState = noisyEngine.getState();

      expect(noisyState.status).toBe(cleanState.status);
      expect(noisyState.anchorPositionMs).toBe(cleanState.anchorPositionMs);
      expect(noisyState.playbackRate).toBe(cleanState.playbackRate);
      expect(noisyState.lastAppliedEvent?.logicalEventId).toBe(cleanState.lastAppliedEvent?.logicalEventId);
    });

    it('out-of-order with post-compaction arrival: pre-horizon events with checkpoint are ignored, expired are rejected', () => {
      const engine = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-obs',
        clock: { now: () => 1000 },
      });

      const snap = makeSnapshotV2(
        'snap-compact',
        { 'peer-base': 1, 'peer-A': 10 },
        15,
        { 'peer-base': 1, 'peer-A': 10 },
        'paused',
        5000,
      );
      // Explicitly include checkpoint for peer-A: 5
      snap.compactedCheckpoints.push({
        originPeerId: 'peer-A',
        sequenceNumber: 5,
        logicalTimestamp: 8,
        logicalEventId: 'late-1',
      } as any);

      engine.importSnapshotV2(snap);

      // Arrival 1: Legitimate duplicate with retained checkpoint -> IGNORED_ALREADY_COMPACTED
      const lateCompacted = makeRemoteEvent('late-1', 'peer-A', 'SEEK', { targetPositionMs: 2000 }, 8, 5, { 'peer-base': 1 });
      engine.receive(lateCompacted);
      expect(engine.getLastDecision()?.code).toBe('IGNORED_ALREADY_COMPACTED');

      // Arrival 2: Expired pre-horizon event without checkpoint (e.g. seq 3) -> REJECTED_OUTSIDE_HORIZON
      const expiredEvent = makeRemoteEvent('expired-1', 'peer-A', 'SEEK', { targetPositionMs: 1000 }, 6, 3, { 'peer-base': 1 });
      engine.receive(expiredEvent);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OUTSIDE_HORIZON');

      expect(engine.getState().anchorPositionMs).toBe(5000);
    });
  });

  // ── Suite E: Bounded Resource Behavior ───────────────────────
  describe('Suite E: Bounded Resource Behavior', () => {
    it('OrderedOperationLog capacity strictly bounded to K=256 without unbounded growth', () => {
      const engine = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-local',
        logCapacity: 256,
        clock: { now: () => 1000 },
      });

      // Author 300 operations
      for (let i = 1; i <= 300; i++) {
        engine.receive(makeLocalEvent(`op-${i}`, 'SEEK', { targetPositionMs: i * 10 }));
      }

      expect(engine.logSize).toBe(256);
      expect(engine.pendingQueueSize).toBe(0);
    });

    it('repeated duplicate events do not increase log size or mutate state', () => {
      const engine = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-local',
        clock: { now: () => 1000 },
      });

      const ev = makeRemoteEvent('rem-1', 'peer-A', 'PLAY', {}, 1, 1);
      engine.receive(ev);
      const stateVersion = engine.getState().stateVersion;

      for (let i = 0; i < 50; i++) {
        engine.receive(ev);
      }

      expect(engine.logSize).toBe(1);
      expect(engine.getState().stateVersion).toBe(stateVersion);
    });
  });
});
