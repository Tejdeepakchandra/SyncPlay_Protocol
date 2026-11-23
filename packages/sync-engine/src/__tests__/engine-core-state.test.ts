import { describe, it, expect, vi } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEngineCoreState,
  createInitialEngineCoreState,
  LogicalClockManager,
  OrderedOperationLog,
  PendingDependencyQueue,
  type EngineCoreState,
  type PlaybackState,
  type StoredOperation,
} from '../index.js';

describe('Phase 7.5 M4.3b — EngineCoreState & Root Ownership', () => {
  const baseConfig = {
    sessionId: 'session-m43b-root',
    localPeerId: 'peer-local',
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 128,
  };

  it('A1: creates a sealed EngineCoreState container with all 12 authoritative fields', () => {
    const initialState: PlaybackState = {
      status: 'paused',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 100,
      playbackRate: 1.0,
      stateVersion: 1,
    };

    const clockManager = new LogicalClockManager(5);
    const operationLog = new OrderedOperationLog(64, initialState, (s) => s);
    const pendingQueue = new PendingDependencyQueue(64);

    const core = createEngineCoreState({
      playbackState: initialState,
      baseSnapshot: initialState,
      baseSnapshotFrontier: { 'peer-1': 10 },
      operationLog,
      clockManager,
      pendingQueue,
      replicationVector: { 'peer-1': 10 },
      localCausalContext: { 'peer-1': 10 },
      localOriginSequence: 3,
      currentEpoch: 2,
      lastAppliedEventId: 'evt-test-1',
      stateVersion: 1,
    });

    // Verify container sealing
    expect(Object.isSealed(core)).toBe(true);

    // Verify all 12 authoritative fields
    expect(core.playbackState.anchorPositionMs).toBe(1000);
    expect(core.state.anchorPositionMs).toBe(1000);
    expect(core.baseSnapshot.anchorPositionMs).toBe(1000);
    expect(core.baseSnapshotFrontier['peer-1']).toBe(10);
    expect(core.operationLog).toBe(operationLog);
    expect(core.clockManager).toBe(clockManager);
    expect(core.pendingQueue).toBe(pendingQueue);
    expect(core.replicationVector['peer-1']).toBe(10);
    expect(core.localCausalContext['peer-1']).toBe(10);
    expect(core.localOriginSequence).toBe(3);
    expect(core.currentEpoch).toBe(2);
    expect(core.lastAppliedEventId).toBe('evt-test-1');
    expect(core.stateVersion).toBe(1);

    // Prohibit adding properties (sealed container)
    expect(() => {
      (core as any).newProperty = 'invalid';
    }).toThrow();

    // Prohibit deleting properties (sealed container)
    expect(() => {
      delete (core as any).playbackState;
    }).toThrow();
  });

  it('A2: LeaderlessSyncEngine initializes with exactly one authoritative core root and delegates to it', () => {
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      initialClock: 42,
    });

    const core = engine.coreState;
    expect(core).toBeDefined();
    expect(Object.isSealed(core)).toBe(true);

    // Verify all public accessors read strictly from this.core
    expect(engine.logicalClock).toBe(42);
    expect(core.clockManager.clock).toBe(42);
    expect(engine.epoch).toBe(1);
    expect(core.currentEpoch).toBe(1);
    expect(engine.stateVersion).toBe(0);
    expect(core.stateVersion).toBe(0);
    expect(engine.logSize).toBe(0);
    expect(core.operationLog.size).toBe(0);
    expect(engine.pendingQueueSize).toBe(0);
    expect(core.pendingQueue.size).toBe(0);

    // Playback state matches
    expect(engine.playbackState.status).toBe('paused');
    expect(core.playbackState.status).toBe('paused');
    expect(engine.getState()).toBe(core.playbackState);
  });

  it('A3: public projections return defensive/frozen copies and cannot mutate the core state', () => {
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      initialClock: 10,
    });

    const frontier = engine.replicationFrontier as Record<string, number>;
    expect(frontier).toBeDefined();

    // Attempting to mutate returned frontier does not affect engine core
    frontier['peer-malicious'] = 999;
    expect(engine.replicationFrontier['peer-malicious']).toBeUndefined();
    expect(engine.coreState.replicationVector['peer-malicious']).toBeUndefined();

    // Local frontier defensive copy
    const localFrontier = engine.localFrontier as Record<string, number>;
    localFrontier['peer-malicious'] = 888;
    expect(engine.localFrontier['peer-malicious']).toBeUndefined();
    expect(engine.coreState.localCausalContext['peer-malicious']).toBeUndefined();

    // PlaybackState is frozen
    const state = engine.getState();
    expect(Object.isFrozen(state)).toBe(true);
    expect(() => {
      (state as any).status = 'playing';
    }).toThrow();
  });

  it('A4: replaceCoreStateAtomic performs single synchronous root swap without throwing', () => {
    const engine = new LeaderlessSyncEngine(baseConfig);
    const oldCore = engine.coreState;

    const newPlaybackState = Object.freeze<PlaybackState>({
      status: 'playing',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.5,
      stateVersion: 10,
    });

    const newClockManager = new LogicalClockManager(100);
    const newOpLog = new OrderedOperationLog(64, newPlaybackState, (s) => s);
    const newPendingQueue = new PendingDependencyQueue(64);

    const candidateCore = createEngineCoreState({
      playbackState: newPlaybackState,
      baseSnapshot: newPlaybackState,
      baseSnapshotFrontier: { 'peer-remote': 50 },
      operationLog: newOpLog,
      clockManager: newClockManager,
      pendingQueue: newPendingQueue,
      replicationVector: { 'peer-remote': 50 },
      localCausalContext: { 'peer-remote': 50 },
      localOriginSequence: 0,
      currentEpoch: 3,
      lastAppliedEventId: 'snap-remote-99',
      stateVersion: 10,
    });

    // Execute atomic pointer replacement
    engine.replaceCoreStateAtomic(candidateCore);

    // Live engine immediately resolves all accessors from the new root
    expect(engine.coreState).toBe(candidateCore);
    expect(engine.coreState).not.toBe(oldCore);
    expect(engine.logicalClock).toBe(100);
    expect(engine.epoch).toBe(3);
    expect(engine.stateVersion).toBe(10);
    expect(engine.playbackState.status).toBe('playing');
    expect(engine.playbackState.anchorPositionMs).toBe(5000);
    expect(engine.replicationFrontier['peer-remote']).toBe(50);
    expect(engine.localFrontier['peer-remote']).toBe(50);

    // Old core remains detached and unmodified
    expect(oldCore.currentEpoch).toBe(1);
    expect(oldCore.clockManager.clock).toBe(0);
    expect(oldCore.replicationVector['peer-remote']).toBeUndefined();
  });

  it('A5: throws INVALID_ARGUMENT when candidate core is null or not an object', () => {
    const engine = new LeaderlessSyncEngine(baseConfig);
    expect(() => {
      engine.replaceCoreStateAtomic(null as any);
    }).toThrow(/Candidate core state must be a non-null object/);

    expect(() => {
      engine.replaceCoreStateAtomic(undefined as any);
    }).toThrow(/Candidate core state must be a non-null object/);

    expect(() => {
      engine.replaceCoreStateAtomic('not-an-object' as any);
    }).toThrow(/Candidate core state must be a non-null object/);

    try {
      engine.replaceCoreStateAtomic(null as any);
    } catch (err: any) {
      expect(err.code).toBe('INVALID_ARGUMENT');
    }
  });

  it('A6: throws ENGINE_DISPOSED when replaceCoreStateAtomic is called on disposed engine', () => {
    const engine = new LeaderlessSyncEngine(baseConfig);
    engine.dispose();

    const candidateCore = createEngineCoreState({
      playbackState: Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 0,
        playbackRate: 1,
        stateVersion: 1,
      }),
      baseSnapshot: Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 0,
        playbackRate: 1,
        stateVersion: 1,
      }),
      baseSnapshotFrontier: {},
      operationLog: new OrderedOperationLog(64, Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 0,
        playbackRate: 1,
        stateVersion: 1,
      }), (s) => s),
      clockManager: new LogicalClockManager(0),
      pendingQueue: new PendingDependencyQueue(64),
      replicationVector: {},
      localCausalContext: {},
      localOriginSequence: 0,
      currentEpoch: 1,
    });

    expect(() => {
      engine.replaceCoreStateAtomic(candidateCore);
    }).toThrow(/Cannot replace core state on disposed engine/);

    try {
      engine.replaceCoreStateAtomic(candidateCore);
    } catch (err: any) {
      expect(err.code).toBe('ENGINE_DISPOSED');
    }
  });

  it('A7: supports recovery mode and defers state change and decision notifications until flush', () => {
    const engine = new LeaderlessSyncEngine(baseConfig);

    const stateChanges: PlaybackState[] = [];
    engine.onStateChange((state) => {
      stateChanges.push(state);
    });

    // Activate recovery mode
    expect(engine.isRecoveryActive()).toBe(false);
    engine.setRecoveryMode(true);
    expect(engine.isRecoveryActive()).toBe(true);

    // Apply an exported and modified snapshot in recovery mode
    const exported = engine.exportSnapshot();
    const modifiedSnapshot = {
      ...exported,
      state: {
        ...exported.state,
        status: 'playing' as const,
        anchorPositionMs: 2000,
      },
    };

    const decision = engine.importSnapshot(modifiedSnapshot);
    expect(decision.accepted).toBe(true);

    // Notification is deferred: listener was not called immediately
    expect(stateChanges.length).toBe(0);

    // Flush deferred notifications
    engine.flushDeferredNotifications();

    // Exactly one coalesced notification was received
    expect(stateChanges.length).toBe(1);
    expect(stateChanges[0].status).toBe('playing');
    expect(stateChanges[0].anchorPositionMs).toBe(2000);

    // Re-flushing when empty does nothing
    engine.flushDeferredNotifications();
    expect(stateChanges.length).toBe(1);

    // Deactivate recovery mode
    engine.setRecoveryMode(false);
    expect(engine.isRecoveryActive()).toBe(false);
  });
});
