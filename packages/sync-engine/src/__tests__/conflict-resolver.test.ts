/**
 * Unit tests for ConflictResolver.
 *
 * Verifies:
 * - Deterministic total-order LWW for SEEK and RATE_CHANGE
 * - Concurrency-aware PAUSE priority over concurrent PLAY
 * - Sequential PLAY resumption after PAUSE
 * - Deterministic replay across operation sequences
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ConflictResolver, areConcurrent, happensBefore } from '../conflict/conflict-resolver.js';
import type { PlaybackState } from '../types.js';
import type { StoredOperation } from '../log/types.js';
import type { ReducerContext } from '../conflict/types.js';

function createInitialState(status: 'paused' | 'playing' = 'paused', anchorPos: number = 0): PlaybackState {
  return Object.freeze<PlaybackState>({
    status,
    anchorPositionMs: anchorPos,
    anchorLocalTimestamp: 1000,
    playbackRate: 1.0,
    stateVersion: 0,
  });
}

function makeOp(
  logicalEventId: string,
  originPeerId: string,
  logicalTimestamp: number,
  originSequenceNumber: number,
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE',
  payload: unknown,
  causalContext: Record<string, number> = {},
): StoredOperation {
  return {
    logicalEventId,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: 1,
    messageType,
    logicalTimestamp,
    originSequenceNumber,
    causalContext,
    appliedAtMonotonic: 1000,
    payload,
  };
}

const testContext: ReducerContext = {
  sessionId: 'test-session',
  localPeerId: 'peer-local',
  minPlaybackRate: 0.25,
  maxPlaybackRate: 4.0,
  positionEpsilonMs: 5.0,
};

describe('ConflictResolver', () => {
  let resolver: ConflictResolver;

  beforeEach(() => {
    resolver = new ConflictResolver();
  });

  describe('causality and concurrency detection', () => {
    it('detects happens-before correctly', () => {
      const e1 = { originPeerId: 'peer-A', originSequenceNumber: 2 };
      const e2 = { causalContext: { 'peer-A': 2, 'peer-B': 1 } };
      expect(happensBefore(e1, e2)).toBe(true);

      const e3 = { causalContext: { 'peer-A': 1 } };
      expect(happensBefore(e1, e3)).toBe(false);
    });

    it('detects concurrent operations when neither observed the other', () => {
      const opA = {
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        causalContext: {},
      };
      const opB = {
        originPeerId: 'peer-B',
        originSequenceNumber: 1,
        causalContext: {},
      };
      expect(areConcurrent(opA, opB)).toBe(true);
    });

    it('same origin operations are never concurrent', () => {
      const op1 = {
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        causalContext: {},
      };
      const op2 = {
        originPeerId: 'peer-A',
        originSequenceNumber: 2,
        causalContext: { 'peer-A': 1 },
      };
      expect(areConcurrent(op1, op2)).toBe(false);
    });
  });

  describe('SEEK vs SEEK conflict (Total-Order LWW)', () => {
    it('lower key seek is superseded by higher key seek', () => {
      const state = createInitialState('paused', 0);
      const seekHigh = makeOp('seek-high', 'peer-B', 10, 1, 'SEEK', { targetPositionMs: 60000 });
      const seekLow = makeOp('seek-low', 'peer-A', 5, 1, 'SEEK', { targetPositionMs: 30000 });

      // Apply high seek first
      const res1 = resolver.reduce(state, seekHigh, 1000, testContext);
      expect(res1.nextState.anchorPositionMs).toBe(60000);
      expect(res1.decision.code).toBe('APPLIED');

      // Then evaluate lower seek with high seek as prior
      const res2 = resolver.reduce(res1.nextState, seekLow, 1000, testContext, seekHigh);
      expect(res2.decision.code).toBe('SUPERSEDED');
      expect(res2.nextState.anchorPositionMs).toBe(60000); // Unchanged
    });
  });

  describe('PLAY vs PAUSE conflict (Concurrency-Aware Pause Priority)', () => {
    it('concurrent PAUSE overrides PLAY, maintaining paused resting state', () => {
      // Peer A pauses, Peer B plays, concurrently (neither observed the other)
      const pauseOp = makeOp('pause-1', 'peer-A', 1, 1, 'PAUSE', {}, {});
      const playOp = makeOp('play-1', 'peer-B', 1, 1, 'PLAY', {}, {});

      // Assume playing state initially
      const state = createInitialState('playing', 10000);

      // Replay order: [pauseOp, playOp]
      const replayRes = resolver.replay(state, [pauseOp, playOp], 1000, testContext);
      expect(replayRes.finalState.status).toBe('paused');
      expect(replayRes.decisions[1]!.code).toBe('SUPERSEDED');
    });

    it('causally sequential PLAY after PAUSE resumes playback', () => {
      const pauseOp = makeOp('pause-1', 'peer-A', 1, 1, 'PAUSE', {}, {});
      // Peer B observed Peer A's pause sequence 1
      const playOp = makeOp('play-2', 'peer-B', 2, 1, 'PLAY', {}, { 'peer-A': 1 });

      const state = createInitialState('playing', 10000);

      const replayRes = resolver.replay(state, [pauseOp, playOp], 1000, testContext);
      expect(replayRes.finalState.status).toBe('playing');
      expect(replayRes.decisions[1]!.code).toBe('APPLIED');
    });

    it('pause priority is preserved across interleaved SEEK and RATE_CHANGE operations', () => {
      const pauseOp = makeOp('pause-1', 'peer-A', 1, 1, 'PAUSE', {}, {});
      const seekOp = makeOp('seek-1', 'peer-C', 1, 1, 'SEEK', { targetPositionMs: 20000 }, {});
      const rateOp = makeOp('rate-1', 'peer-D', 1, 1, 'RATE_CHANGE', { playbackRate: 1.5 }, {});
      // Concurrent PLAY
      const playOp = makeOp('play-1', 'peer-B', 1, 1, 'PLAY', {}, {});

      const state = createInitialState('playing', 10000);

      // Replay order: [PAUSE, SEEK, RATE_CHANGE, PLAY]
      const replayRes = resolver.replay(state, [pauseOp, seekOp, rateOp, playOp], 1000, testContext);
      expect(replayRes.finalState.status).toBe('paused');
      expect(replayRes.finalState.anchorPositionMs).toBe(20000); // SEEK applied
      expect(replayRes.finalState.playbackRate).toBe(1.5); // RATE applied
      expect(replayRes.decisions[3]!.code).toBe('SUPERSEDED'); // Concurrent PLAY superseded
    });

    it('three-operation Pause/Play resumption in pure reducer: A pauses, B plays concurrently, C plays observing both', () => {
      const opA = makeOp('op-A', 'peer-A', 1, 1, 'PAUSE', {}, {});
      const opB = makeOp('op-B', 'peer-B', 1, 1, 'PLAY', {}, {});
      const opC = makeOp('op-C', 'peer-C', 2, 1, 'PLAY', {}, { 'peer-A': 1, 'peer-B': 1 });

      const state = createInitialState('playing', 10000);

      // Order [opA, opB, opC]
      const res1 = resolver.replay(state, [opA, opB, opC], 1000, testContext);
      expect(res1.finalState.status).toBe('playing');

      // Order [opB, opA, opC]
      const res2 = resolver.replay(state, [opB, opA, opC], 1000, testContext);
      expect(res2.finalState.status).toBe('playing');
    });
  });

  describe('RATE_CHANGE conflict (Scalar LWW)', () => {
    it('higher key rate change supersedes lower key rate change', () => {
      const state = createInitialState('playing', 10000);
      const rateHigh = makeOp('rate-high', 'peer-B', 10, 1, 'RATE_CHANGE', { playbackRate: 2.0 });
      const rateLow = makeOp('rate-low', 'peer-A', 5, 1, 'RATE_CHANGE', { playbackRate: 1.5 });

      const res1 = resolver.reduce(state, rateHigh, 1000, testContext);
      expect(res1.nextState.playbackRate).toBe(2.0);

      const res2 = resolver.reduce(res1.nextState, rateLow, 1000, testContext, rateHigh);
      expect(res2.decision.code).toBe('SUPERSEDED');
      expect(res2.nextState.playbackRate).toBe(2.0);
    });
  });
});
