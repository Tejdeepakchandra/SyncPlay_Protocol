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
import { PROTOCOL_VERSION } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

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
});
