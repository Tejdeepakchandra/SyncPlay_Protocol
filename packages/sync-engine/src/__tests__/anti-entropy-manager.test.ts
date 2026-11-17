import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { StateDigestPayload } from '@syncplay/protocol';
import { StateDigestPayloadSchema } from '@syncplay/protocol';
import { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';
import {
  AntiEntropyManager,
  type SchedulerClock,
  type AntiEntropyCallbacks,
} from '../replication/index.js';
import { computeOperationFingerprint } from '../log/operation-fingerprint.js';

class FakeSchedulerClock implements SchedulerClock {
  private currentTime = 1000;
  private nextTimerId = 1;
  private timers = new Map<number, { callback: () => void; triggerTime: number }>();

  public now(): number {
    return this.currentTime;
  }

  public setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextTimerId++;
    this.timers.set(id, { callback, triggerTime: this.currentTime + delayMs });
    return id;
  }

  public clearTimeout(id: unknown): void {
    if (typeof id === 'number') {
      this.timers.delete(id);
    }
  }

  public advanceTime(ms: number): void {
    this.currentTime += ms;
    // Collect and sort ready timers by trigger time
    const ready = Array.from(this.timers.entries())
      .filter(([_, t]) => t.triggerTime <= this.currentTime)
      .sort((a, b) => a[1].triggerTime - b[1].triggerTime);

    for (const [id, t] of ready) {
      if (this.timers.has(id)) {
        this.timers.delete(id);
        t.callback();
      }
    }
  }

  public get pendingTimersCount(): number {
    return this.timers.size;
  }
}

describe('AntiEntropyManager & Digest Engine Integration', () => {
  let fakeClock: FakeSchedulerClock;
  let engine: LeaderlessSyncEngine;

  beforeEach(() => {
    fakeClock = new FakeSchedulerClock();
    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session-1',
      localPeerId: 'peer-local',
      clock: fakeClock,
    });
  });

  describe('Local Digest Generation (createLocalDigest)', () => {
    it('generates a valid schema-compliant digest in an empty session', () => {
      const digest = engine.createLocalDigest();

      expect(digest.formatVersion).toBe(2);
      expect(digest.sessionId).toBe('test-session-1');
      expect(digest.senderPeerId).toBe('peer-local');
      expect(digest.connectionGeneration).toBe(1);
      expect(digest.membershipEpoch).toBe(1);
      expect(digest.stateVersion).toBe(0);
      expect(digest.replicationVector).toEqual({});
      expect(digest.snapshotFrontier).toEqual({});
      expect(digest.historyFingerprints).toEqual({});
      expect(digest.sentAt).toBe(1000);

      // Verify schema round-trip
      expect(() => StateDigestPayloadSchema.parse(digest)).not.toThrow();
    });

    it('generates accurate replicationVector, snapshotFrontier, and tail fingerprints after applying operations', () => {
      // Receive and admit an event for peer-A
      engine.receive({
        origin: 'remote',
        localTimestamp: 1000,
        message: {
          protocolVersion: 1,
          sessionId: 'test-session-1',
          senderPeerId: 'peer-A',
          originPeerId: 'peer-A',
          messageId: 'msg-evt-a-1',
          logicalEventId: 'evt-a-1',
          membershipEpoch: 1,
          messageType: 'PLAY',
          logicalTimestamp: 1,
          originSequenceNumber: 1,
          causalContext: {},
          payload: { positionMs: 1000 },
        },
      });

      const digest = engine.createLocalDigest();
      expect(digest.replicationVector).toEqual({ 'peer-A': 1 });
      expect(digest.snapshotFrontier).toEqual({});
      expect(digest.historyFingerprints['peer-A']).toBeDefined();
      expect(digest.historyFingerprints['peer-A']).toHaveLength(64);

      // Verify fingerprint matches exact canonical SHA-256
      const expectedFp = computeOperationFingerprint({
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        logicalEventId: 'evt-a-1',
        logicalTimestamp: 1,
        causalContext: {},
        messageType: 'PLAY',
        payload: { positionMs: 1000 },
        membershipEpoch: 1,
      });
      expect(digest.historyFingerprints['peer-A']).toBe(expectedFp);
    });

    it('preserves distinction between compacted base snapshot frontier and active log', () => {
      // Import a snapshot with a compacted frontier
      engine.importSnapshotV2({
        formatVersion: 2,
        snapshotId: 'snap-1',
        sessionId: 'test-session-1',
        membershipEpoch: 1,
        authorPeerId: 'peer-remote',
        maxLogicalTimestamp: 50,
        snapshotFrontier: { 'peer-historical': 50 },
        originFrontierTimestamps: { 'peer-historical': 50 },
        compactedCheckpoints: [
          {
            originPeerId: 'peer-historical',
            sequenceNumber: 50,
            logicalTimestamp: 50,
          },
        ],
        state: {
          playbackStatus: 'PLAYING',
          anchorPositionMs: 50000,
          anchorLocalTimestamp: 1000,
          playbackRate: 1.0,
          mediaDurationMs: 120000,
          stateVersion: 50,
          lastAppliedOperation: {
            logicalEventId: 'evt-hist-50',
            originPeerId: 'peer-historical',
            messageType: 'PLAY',
            appliedAtLocalTimestamp: 1000,
            isImportedProvenance: true,
          },
        },
      });

      const digest = engine.createLocalDigest();
      expect(digest.snapshotFrontier['peer-historical']).toBe(50);
      expect(digest.stateVersion).toBe(1); // import advances local version by 1
    });

    it('generates digests correctly across multiple concurrent origins', () => {
      // Admit operations for peer-A, peer-B, and local peer
      engine.receive({
        origin: 'remote',
        localTimestamp: 1000,
        message: {
          protocolVersion: 1,
          sessionId: 'test-session-1',
          senderPeerId: 'peer-A',
          originPeerId: 'peer-A',
          messageId: 'msg-a-1',
          logicalEventId: 'evt-a-1',
          membershipEpoch: 1,
          messageType: 'PLAY',
          logicalTimestamp: 1,
          originSequenceNumber: 1,
          causalContext: {},
          payload: {},
        },
      });

      engine.receive({
        origin: 'remote',
        localTimestamp: 1001,
        message: {
          protocolVersion: 1,
          sessionId: 'test-session-1',
          senderPeerId: 'peer-B',
          originPeerId: 'peer-B',
          messageId: 'msg-b-1',
          logicalEventId: 'evt-b-1',
          membershipEpoch: 1,
          messageType: 'PAUSE',
          logicalTimestamp: 2,
          originSequenceNumber: 1,
          causalContext: { 'peer-A': 1 },
          payload: {},
        },
      });

      const digest = engine.createLocalDigest();
      expect(digest.replicationVector['peer-A']).toBe(1);
      expect(digest.replicationVector['peer-B']).toBe(1);
      expect(digest.historyFingerprints['peer-A']).toBeDefined();
      expect(digest.historyFingerprints['peer-B']).toBeDefined();
      expect(digest.historyFingerprints['peer-A']).not.toBe(digest.historyFingerprints['peer-B']);
    });

    it('validates 64 origin bound on digest generation', () => {
      // Construct a digest payload with exactly 64 origins -> passes
      const vector64: Record<string, number> = {};
      for (let i = 1; i <= 64; i++) {
        vector64[`peer-${i}`] = 1;
      }
      const digest64: StateDigestPayload = {
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-local',
        connectionGeneration: 1,
        membershipEpoch: 1,
        stateVersion: 1,
        replicationVector: vector64,
        snapshotFrontier: {},
        historyFingerprints: {},
        sentAt: 1000,
      };
      expect(() => StateDigestPayloadSchema.parse(digest64)).not.toThrow();

      // 65 origins -> rejected by schema
      const vector65 = { ...vector64, 'peer-65': 1 };
      const digest65 = { ...digest64, replicationVector: vector65 };
      expect(() => StateDigestPayloadSchema.parse(digest65)).toThrowError(
        /cannot exceed 64 origin entries/,
      );
    });

    it('performs consistent point-in-time read under state mutations', () => {
      const digest1 = engine.createLocalDigest();
      expect(digest1.replicationVector).toEqual({});

      // Apply operation
      engine.receive({
        origin: 'remote',
        localTimestamp: 1000,
        message: {
          protocolVersion: 1,
          sessionId: 'test-session-1',
          senderPeerId: 'peer-A',
          originPeerId: 'peer-A',
          messageId: 'msg-a-1',
          logicalEventId: 'evt-a-1',
          membershipEpoch: 1,
          messageType: 'PLAY',
          logicalTimestamp: 1,
          originSequenceNumber: 1,
          causalContext: {},
          payload: {},
        },
      });

      // Digest1 was an immutable point-in-time snapshot and remains unchanged
      expect(digest1.replicationVector).toEqual({});

      // Digest2 reflects the new state
      const digest2 = engine.createLocalDigest();
      expect(digest2.replicationVector).toEqual({ 'peer-A': 1 });
    });

    it('throws DISPOSED when creating digest after engine disposal', () => {
      engine.dispose();
      expect(() => engine.createLocalDigest()).toThrowError(/disposed/);
    });
  });

  describe('Remote Digest Handling (compareRemoteDigest)', () => {
    it('safely drops legacy Phase 2 digests lacking formatVersion or with digestId', () => {
      const legacyPhase2 = {
        digestId: 'legacy-1',
        stateVersion: 5,
        digest: 'sha256-hash',
      };

      const result = engine.compareRemoteDigest(legacyPhase2);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.diagnosticReasons?.[0]).toContain('Ignoring legacy Phase 2');
      expect(result.forkDetected).toBe(false);
    });

    it('safely drops future protocol formats (formatVersion > 2)', () => {
      const futureDigest = {
        formatVersion: 3,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-future',
      };

      const result = engine.compareRemoteDigest(futureDigest);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.diagnosticReasons?.[0]).toContain('Unsupported future protocol format version: 3');
    });

    it('returns diagnostic reasons on schema validation errors', () => {
      const malformed = {
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-bad',
        connectionGeneration: -1, // invalid negative number
        membershipEpoch: 1,
        stateVersion: 0,
        replicationVector: {},
        snapshotFrontier: {},
        historyFingerprints: {},
        sentAt: 1000,
      };

      const result = engine.compareRemoteDigest(malformed);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.diagnosticReasons?.some((r) => r.includes('connectionGeneration'))).toBe(true);
    });
  });

  describe('AntiEntropyManager Scheduling & Lifecycle', () => {
    it('schedules periodic digest within 5000ms ± 1000ms jitter bounds', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: broadcastSpy,
      };

      // Mock RNG returning 0.5 -> jitter = 0ms -> delay = 5000ms
      const rngZeroJitter = () => 0.5;

      const manager = new AntiEntropyManager(
        engine,
        callbacks,
        {
          periodicIntervalMs: 5000,
          jitterMs: 1000,
          rng: rngZeroJitter,
        },
        fakeClock,
      );

      expect(broadcastSpy).not.toHaveBeenCalled();
      expect(fakeClock.pendingTimersCount).toBe(1);

      // Advance by 4999ms: should not have fired yet
      fakeClock.advanceTime(4999);
      expect(broadcastSpy).not.toHaveBeenCalled();

      // Advance 1ms: fires at exactly 5000ms
      fakeClock.advanceTime(1);
      expect(broadcastSpy).toHaveBeenCalledTimes(1);

      // Exactly 1 new periodic timer scheduled
      expect(fakeClock.pendingTimersCount).toBe(1);

      manager.dispose();
    });

    it('respects maximum jitter bounds (4000ms to 6000ms)', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: broadcastSpy };

      // RNG = 0.0 -> jitter = -1000ms -> delay = 4000ms
      const rngMinJitter = () => 0.0;
      const managerMin = new AntiEntropyManager(
        engine,
        callbacks,
        { rng: rngMinJitter },
        fakeClock,
      );

      fakeClock.advanceTime(3999);
      expect(broadcastSpy).not.toHaveBeenCalled();
      fakeClock.advanceTime(1);
      expect(broadcastSpy).toHaveBeenCalledTimes(1);
      managerMin.dispose();

      broadcastSpy.mockClear();

      // RNG = 1.0 -> jitter = +1000ms -> delay = 6000ms
      const rngMaxJitter = () => 1.0;
      const managerMax = new AntiEntropyManager(
        engine,
        callbacks,
        { rng: rngMaxJitter },
        fakeClock,
      );

      fakeClock.advanceTime(5999);
      expect(broadcastSpy).not.toHaveBeenCalled();
      fakeClock.advanceTime(1);
      expect(broadcastSpy).toHaveBeenCalledTimes(1);
      managerMax.dispose();
    });

    it('coalesces multiple reactive triggers within the same tick', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: broadcastSpy };

      const manager = new AntiEntropyManager(
        engine,
        callbacks,
        {
          rateLimitBurst: 5,
        },
        fakeClock,
      );

      // Call triggerReactiveDigest multiple times in the same tick
      manager.triggerReactiveDigest();
      manager.triggerReactiveDigest();
      manager.triggerReactiveDigest();

      expect(broadcastSpy).not.toHaveBeenCalled();

      // Advance microtask/tick (0ms)
      fakeClock.advanceTime(0);

      // Only ONE broadcast occurs due to coalescing
      expect(broadcastSpy).toHaveBeenCalledTimes(1);

      manager.dispose();
    });

    it('enforces token-bucket rate limiting (burst 2, 1 digest/sec)', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: broadcastSpy };

      const manager = new AntiEntropyManager(
        engine,
        callbacks,
        {
          rateLimitBurst: 2,
          rateLimitMinIntervalMs: 1000,
        },
        fakeClock,
      );

      // First broadcast succeeds (consumes 1 of 2 tokens)
      expect(manager.broadcastDigestIfPermitted()).toBe(true);
      expect(broadcastSpy).toHaveBeenCalledTimes(1);

      // Second broadcast succeeds immediately (burst = 2, consumes 2nd token)
      expect(manager.broadcastDigestIfPermitted()).toBe(true);
      expect(broadcastSpy).toHaveBeenCalledTimes(2);

      // Third broadcast in same ms fails (tokens exhausted)
      expect(manager.broadcastDigestIfPermitted()).toBe(false);
      expect(broadcastSpy).toHaveBeenCalledTimes(2);

      // Advance by 500ms (half a token refilled, still < 1)
      fakeClock.advanceTime(500);
      expect(manager.broadcastDigestIfPermitted()).toBe(false);

      // Advance another 500ms (total 1000ms elapsed, 1 token refilled)
      fakeClock.advanceTime(500);
      expect(manager.broadcastDigestIfPermitted()).toBe(true);
      expect(broadcastSpy).toHaveBeenCalledTimes(3);

      manager.dispose();
    });

    it('disposal cancels all pending timers and prevents subsequent callbacks', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: broadcastSpy };

      const manager = new AntiEntropyManager(engine, callbacks, {}, fakeClock);

      expect(fakeClock.pendingTimersCount).toBe(1);

      manager.dispose();
      expect(manager.isDisposed).toBe(true);
      expect(fakeClock.pendingTimersCount).toBe(0);

      // Advance time by 10 seconds: no callbacks should fire
      fakeClock.advanceTime(10000);
      expect(broadcastSpy).not.toHaveBeenCalled();

      // Additional calls are safe no-ops
      manager.triggerReactiveDigest();
      expect(fakeClock.pendingTimersCount).toBe(0);
      expect(manager.broadcastDigestIfPermitted()).toBe(false);
    });

    it('resets scheduling and triggers reactive broadcast on epoch transition', () => {
      const broadcastSpy = vi.fn();
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: broadcastSpy };

      const manager = new AntiEntropyManager(engine, callbacks, {}, fakeClock);

      // Epoch transition fires
      manager.onEpochTransition(2);

      // Should have triggered reactive broadcast scheduled for next tick
      fakeClock.advanceTime(0);
      expect(broadcastSpy).toHaveBeenCalledTimes(1);

      manager.dispose();
    });

    it('strictly preserves M3/M4 separation by throwing on unimplemented repair methods', () => {
      const callbacks: AntiEntropyCallbacks = { onBroadcastDigest: vi.fn() };
      const manager = new AntiEntropyManager(engine, callbacks, {}, fakeClock);

      expect(() => manager.reconcileGaps()).toThrowError(/Phase 7\.4 M3/);
      expect(() =>
        manager.serveRepairRequest({
          requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
          sessionId: 'test-session-1',
          requesterPeerId: 'peer-requester',
          targetOriginPeerId: 'peer-origin',
          fromSequence: 1,
          toSequence: 5,
          membershipEpoch: 1,
        }),
      ).toThrowError(/Phase 7\.4 M4/);

      manager.dispose();
    });
  });
});
