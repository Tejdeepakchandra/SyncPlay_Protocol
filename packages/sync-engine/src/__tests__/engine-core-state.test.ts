import { describe, it, expect } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEngineCoreState,
  createInitialEngineCoreState,
  createRecoveryInstallationCapability,
  LogicalClockManager,
  OrderedOperationLog,
  PendingDependencyQueue,
  type EngineCoreState,
  type PlaybackState,
  type StoredOperation,
  type RecoveryInstallationCapability,
} from '../index.js';

describe('Phase 7.5 M4.3b — EngineCoreState, Root Ownership & Capability-Gated Installation', () => {
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

  it('A2: LeaderlessSyncEngine initializes with authoritative core root and delegates to it', () => {
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      initialClock: 42,
    });

    // Verify all public accessors read strictly from internal root
    expect(engine.logicalClock).toBe(42);
    expect(engine.epoch).toBe(1);
    expect(engine.stateVersion).toBe(0);
    expect(engine.logSize).toBe(0);
    expect(engine.pendingQueueSize).toBe(0);

    // Playback state matches
    expect(engine.playbackState.status).toBe('paused');
    expect(engine.getState().status).toBe('paused');
  });

  it('A3: public root exposure is eliminated; getDiagnosticCoreSnapshot returns detached frozen copies', () => {
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      initialClock: 10,
    });

    // Public coreState getter is completely removed
    expect((engine as any).coreState).toBeUndefined();

    // Diagnostic snapshot returns frozen detached projection
    const diag = engine.getDiagnosticCoreSnapshot();
    expect(diag).toBeDefined();
    expect(Object.isFrozen(diag)).toBe(true);
    expect(Object.isFrozen(diag.replicationFrontier)).toBe(true);
    expect(Object.isFrozen(diag.localFrontier)).toBe(true);
    expect(Object.isFrozen(diag.baseSnapshotFrontier)).toBe(true);
    expect(diag.logicalClock).toBe(10);
    expect(diag.currentEpoch).toBe(1);
    expect(diag.stateVersion).toBe(0);

    // Operational classes are NOT exposed on diagnostic snapshot
    expect((diag as any).clockManager).toBeUndefined();
    expect((diag as any).operationLog).toBeUndefined();
    expect((diag as any).pendingQueue).toBeUndefined();

    // Mutating returned frontiers throws due to Object.freeze
    expect(() => {
      (diag.replicationFrontier as any)['peer-malicious'] = 999;
    }).toThrow();

    expect(() => {
      (diag.localFrontier as any)['peer-malicious'] = 888;
    }).toThrow();

    // Public getters return detached defensive copies
    const frontier = engine.replicationFrontier as Record<string, number>;
    frontier['peer-malicious'] = 999;
    expect(engine.replicationFrontier['peer-malicious']).toBeUndefined();
    expect(engine.getDiagnosticCoreSnapshot().replicationFrontier['peer-malicious']).toBeUndefined();
  });

  it('A4: replaceCoreStateAtomic performs single synchronous root swap when authorized with valid capability', () => {
    const capability = createRecoveryInstallationCapability('test-coord', baseConfig.sessionId);
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: capability,
    });

    const newPlaybackState = Object.freeze<PlaybackState>({
      status: 'playing',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.5,
      stateVersion: 10,
      lastAppliedEvent: Object.freeze({
        logicalEventId: 'snap-remote-99',
        senderPeerId: 'peer-remote',
        messageType: 'SNAPSHOT_IMPORT',
        appliedAtLocalTimestamp: 1000,
        isImportedProvenance: true,
      }),
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
      localOriginSequence: 5,
      currentEpoch: 3,
      lastAppliedEventId: 'snap-remote-99',
      stateVersion: 10,
    });

    // Execute atomic pointer replacement with authorized capability
    engine.replaceCoreStateAtomic(candidateCore, capability);

    // Live engine immediately resolves all accessors from the new root
    expect(engine.logicalClock).toBe(100);
    expect(engine.epoch).toBe(3);
    expect(engine.stateVersion).toBe(10);
    expect(engine.playbackState.status).toBe('playing');
    expect(engine.playbackState.anchorPositionMs).toBe(5000);
    expect(engine.replicationFrontier['peer-remote']).toBe(50);
    expect(engine.localFrontier['peer-remote']).toBe(50);

    const diag = engine.getDiagnosticCoreSnapshot();
    expect(diag.logicalClock).toBe(100);
    expect(diag.currentEpoch).toBe(3);
    expect(diag.localOriginSequence).toBe(5);
    expect(diag.lastAppliedEventId).toBe('snap-remote-99');
  });

  it('A5: replaceCoreStateAtomic strictly rejects unauthorized callers without or with wrong capability', () => {
    const authorizedCap = createRecoveryInstallationCapability('auth-coord', baseConfig.sessionId);
    const forgedCap = createRecoveryInstallationCapability('attacker', baseConfig.sessionId);

    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: authorizedCap,
    });

    const dummyCore = createEngineCoreState({
      playbackState: Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 0,
        playbackRate: 1,
        stateVersion: 0,
      }),
      baseSnapshot: Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 0,
        playbackRate: 1,
        stateVersion: 0,
      }),
      baseSnapshotFrontier: {},
      operationLog: new OrderedOperationLog(64, Object.freeze({ status: 'paused', anchorPositionMs: 0, anchorLocalTimestamp: 0, playbackRate: 1, stateVersion: 0 }), s => s),
      clockManager: new LogicalClockManager(0),
      pendingQueue: new PendingDependencyQueue(64),
      replicationVector: {},
      localCausalContext: {},
      localOriginSequence: 0,
      currentEpoch: 1,
    });

    // 1. Missing capability throws UNAUTHORIZED
    expect(() => {
      engine.replaceCoreStateAtomic(dummyCore, undefined as any);
    }).toThrow(/requires authorized RecoveryInstallationCapability/);

    // 2. Forged capability throws UNAUTHORIZED
    expect(() => {
      engine.replaceCoreStateAtomic(dummyCore, forgedCap);
    }).toThrow(/requires authorized RecoveryInstallationCapability/);

    // 3. Engine initialized without capability rejects all replacement attempts
    const unconfiguredEngine = new LeaderlessSyncEngine(baseConfig);
    expect(() => {
      unconfiguredEngine.replaceCoreStateAtomic(dummyCore, authorizedCap);
    }).toThrow(/requires authorized RecoveryInstallationCapability/);
  });

  it('A6: replaceCoreStateAtomic validates candidate shape and throws INVALID_ARGUMENT / INVALID_CANDIDATE_ROOT', () => {
    const capability = createRecoveryInstallationCapability('test-coord', baseConfig.sessionId);
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: capability,
    });

    expect(() => {
      engine.replaceCoreStateAtomic(null as any, capability);
    }).toThrow(/Candidate core state must be a non-null object/);

    expect(() => {
      engine.replaceCoreStateAtomic({} as any, capability);
    }).toThrow(/Candidate state root is malformed/);
  });

  it('A7: throws ENGINE_DISPOSED when replaceCoreStateAtomic is called on disposed engine', () => {
    const capability = createRecoveryInstallationCapability('test-coord', baseConfig.sessionId);
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: capability,
    });
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
      engine.replaceCoreStateAtomic(candidateCore, capability);
    }).toThrow(/Cannot replace core state on disposed engine/);
  });

  it('A8: supports recovery mode and defers state change and decision notifications until flush', () => {
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

  it('A9: lastAppliedEventId remains synchronized with playbackState.lastAppliedEvent', () => {
    const engine = new LeaderlessSyncEngine(baseConfig);
    expect(engine.getDiagnosticCoreSnapshot().lastAppliedEventId).toBeUndefined();

    // Import snapshot V2: sets lastAppliedEvent
    const payload = {
      formatVersion: 2 as const,
      snapshotId: 'snap-sync-1',
      sessionId: baseConfig.sessionId,
      membershipEpoch: 1,
      authorPeerId: 'peer-author',
      state: {
        formatVersion: 1 as const,
        sessionId: baseConfig.sessionId,
        status: 'paused' as const,
        anchorPositionMs: 1500,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-author': 5 },
      originFrontierTimestamps: { 'peer-author': 50 },
      maxLogicalTimestamp: 50,
      compactedCheckpoints: [],
      sha256Digest: 'a'.repeat(64),
    };

    engine.importSnapshotV2(payload);
    expect(engine.getDiagnosticCoreSnapshot().lastAppliedEventId).toBe('snap-snap-sync-1');
    expect(engine.getState().lastAppliedEvent?.logicalEventId).toBe('snap-snap-sync-1');
  });
});
