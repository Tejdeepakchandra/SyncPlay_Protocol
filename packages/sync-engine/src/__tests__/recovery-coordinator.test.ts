import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  RecoveryCoordinator,
  type RecoveryCoordinatorConfig,
  RecoveryStateMachine,
  QuarantineBuffer,
  FutureEpochBuffer,
  RecoveryAdmissionController,
  SnapshotInstaller,
  QuarantineDrainer,
  LeaderlessSyncEngine,
  createRecoveryInstallationCapability,
  computeSnapshotDigestV2,
  type StateSnapshotPayloadV2,
  type SnapshotOfferPayload,
  type QuarantinedOperation,
} from '../index.js';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

describe('Phase 7.5 M4.3c — RecoveryCoordinator & Quarantine Drainer Master Orchestration', () => {
  const baseConfig = {
    sessionId: 'session-m43c-coord',
    localPeerId: 'peer-local',
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 128,
  };

  const createValidSnapshotPayload = (overrides?: Partial<StateSnapshotPayloadV2>): StateSnapshotPayloadV2 => {
    const preimage: Omit<StateSnapshotPayloadV2, 'sha256Digest'> = {
      formatVersion: 2,
      snapshotId: 'snap-coord-001',
      sessionId: 'session-m43c-coord',
      membershipEpoch: 1,
      authorPeerId: 'peer-remote',
      state: {
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        status: 'playing',
        anchorPositionMs: 5000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-remote': 10 },
      originFrontierTimestamps: { 'peer-remote': 100 },
      maxLogicalTimestamp: 100,
      compactedCheckpoints: [
        {
          originPeerId: 'peer-remote',
          sequenceNumber: 10,
          logicalTimestamp: 100,
        },
      ],
      ...overrides,
    };

    const digest = computeSnapshotDigestV2(preimage);
    return {
      ...preimage,
      sha256Digest: overrides?.sha256Digest ?? digest,
    };
  };

  const createMockRemoteOp = (
    seq: number,
    logicalTs: number,
    causalContext: Record<string, number> = {},
    peerId = 'peer-remote',
  ): QuarantinedOperation => {
    const event: AnyControlSyncEvent = {
      version: 1,
      type: 'CONTROL_MESSAGE',
      senderPeerId: peerId,
      recipientPeerId: 'broadcast',
      membershipEpoch: 1,
      transportTimestamp: Date.now(),
      message: {
        protocolVersion: 1,
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        logicalEventId: `evt-${peerId}-${seq}`,
        originPeerId: peerId,
        senderPeerId: peerId,
        originSequenceNumber: seq,
        logicalTimestamp: logicalTs,
        causalContext,
        action: 'STATE_TRANSITION',
        payload: {
          formatVersion: 1,
          type: 'PAUSE',
          positionMs: 5000 + seq * 10,
        },
      },
    };

    return {
      event,
      originPeerId: peerId,
      sequenceNumber: seq,
      membershipEpoch: 1,
      serializedByteLength: 128,
      receivedAtMs: Date.now(),
    };
  };

  const sendLocalEvent = (seq: number) => {
    engine.receive({
      origin: 'local',
      localTimestamp: 1000 + seq,
      version: 1,
      type: 'CONTROL_MESSAGE',
      senderPeerId: 'peer-local',
      recipientPeerId: 'broadcast',
      membershipEpoch: 1,
      transportTimestamp: 1000 + seq,
      message: {
        protocolVersion: 1,
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        logicalEventId: `evt-local-${seq}`,
        originPeerId: 'peer-local',
        senderPeerId: 'peer-local',
        originSequenceNumber: seq,
        logicalTimestamp: seq,
        causalContext: {},
        action: 'STATE_TRANSITION',
        payload: {
          formatVersion: 1,
          type: 'PLAY',
          positionMs: 1000,
        },
      },
    } as AnyControlSyncEvent);
  };

  let engine: LeaderlessSyncEngine;
  let capability: ReturnType<typeof createRecoveryInstallationCapability>;
  let coordinator: RecoveryCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    capability = createRecoveryInstallationCapability('test-coord', baseConfig.sessionId);
    engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: capability,
    });
  });

  afterEach(() => {
    if (coordinator && !coordinator.isDisposed) {
      coordinator.dispose();
    }
    if (engine && !engine.isDisposed) {
      engine.dispose();
    }
    vi.useRealTimers();
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 1. Lifecycle Transitions & State Machine Integration
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Lifecycle Transitions & State Machine', () => {
    it('L1: triggerRecovery enters RECOVERING, sets substate to AWAITING_OFFER, and activates engine recovery mode', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      expect(coordinator.state).toBe('ACTIVE');
      expect(coordinator.substate).toBe('IDLE');
      expect(engine.isRecoveryActive()).toBe(false);

      coordinator.triggerRecovery('peer-remote', 'Causal gap detected');

      expect(coordinator.state).toBe('RECOVERING');
      expect(coordinator.substate).toBe('AWAITING_OFFER');
      expect(engine.isRecoveryActive()).toBe(true);
    });

    it('L2: re-entrant triggerRecovery calls while already recovering are ignored', () => {
      const stateChanges: string[] = [];
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        callbacks: {
          onStateChange: (newSt, oldSt) => stateChanges.push(`${oldSt}->${newSt}`),
        },
      });

      coordinator.triggerRecovery('peer-remote', 'First trigger');
      expect(coordinator.state).toBe('RECOVERING');
      expect(stateChanges).toEqual(['ACTIVE->RECOVERING']);

      // Attempt second trigger while still recovering
      coordinator.triggerRecovery('peer-other', 'Second trigger');
      expect(coordinator.state).toBe('RECOVERING');
      expect(stateChanges).toEqual(['ACTIVE->RECOVERING']);
    });

    it('L3: handleSnapshotOffer advances substate to TRANSFERRING for admissible offer', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');

      const admissibleOffer: SnapshotOfferPayload = {
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        snapshotId: 'snap-offer-01',
        membershipEpoch: 1,
        authorPeerId: 'peer-remote',
        snapshotFrontier: { 'peer-remote': 10 },
        originFrontierTimestamps: { 'peer-remote': 100 },
        maxLogicalTimestamp: 100,
        totalBytes: 1024,
        totalChunks: 1,
        chunkDigestAlgorithm: 'sha256',
        snapshotSha256Digest: 'a'.repeat(64),
      };

      const accepted = coordinator.handleSnapshotOffer(admissibleOffer);
      expect(accepted).toBe(true);
      expect(coordinator.substate).toBe('TRANSFERRING');
    });

    it('L4: handleSnapshotOffer rejects offer with incompatible epoch or author', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');

      const obsoleteOffer: SnapshotOfferPayload = {
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        snapshotId: 'snap-offer-old',
        membershipEpoch: 0, // Incompatible epoch
        authorPeerId: 'peer-remote',
        snapshotFrontier: { 'peer-remote': 10 },
        originFrontierTimestamps: { 'peer-remote': 100 },
        maxLogicalTimestamp: 100,
        totalBytes: 1024,
        totalChunks: 1,
        chunkDigestAlgorithm: 'sha256',
        snapshotSha256Digest: 'a'.repeat(64),
      };

      const accepted = coordinator.handleSnapshotOffer(obsoleteOffer);
      expect(accepted).toBe(false);
      expect(coordinator.substate).toBe('AWAITING_OFFER');
    });

    it('L5: executeRecoveryWithSnapshot completes full 4-stage flow and synchronous handoff to ACTIVE', async () => {
      const stateTransitions: string[] = [];
      const substateTransitions: string[] = [];
      let completionReportCalled = false;

      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        callbacks: {
          onStateChange: (n, o) => stateTransitions.push(`${o}->${n}`),
          onSubstateChange: (n, o) => substateTransitions.push(`${o}->${n}`),
          onRecoveryCompleted: () => { completionReportCalled = true; },
        },
      });

      const payload = createValidSnapshotPayload();
      const report = await coordinator.executeRecoveryWithSnapshot(payload);

      expect(report.success).toBe(true);
      expect(report.installedEpoch).toBe(1);
      expect(report.commitDurationMs).toBeGreaterThanOrEqual(0);

      // Verify engine state was replaced
      expect(engine.playbackState.status).toBe('playing');
      expect(engine.playbackState.anchorPositionMs).toBe(5000);
      expect(engine.replicationFrontier).toEqual({ 'peer-remote': 10 });

      // Synchronous handoff verification: engine recovery mode must be false, state ACTIVE
      expect(engine.isRecoveryActive()).toBe(false);
      expect(coordinator.state).toBe('ACTIVE');
      expect(coordinator.substate).toBe('IDLE');
      expect(completionReportCalled).toBe(true);

      // Verify state sequence: ACTIVE -> RECOVERING -> RECONCILING -> ACTIVE
      expect(stateTransitions).toEqual([
        'ACTIVE->RECOVERING',
        'RECOVERING->RECONCILING',
        'RECONCILING->ACTIVE',
      ]);

      // Substate sequence
      expect(substateTransitions).toContain('AWAITING_OFFER->VALIDATING');
      expect(substateTransitions).toContain('VALIDATING->STAGING');
      expect(substateTransitions).toContain('STAGING->COMMITTING');
      expect(substateTransitions).toContain('COMMITTING->DRAINING_QUARANTINE');
      expect(substateTransitions).toContain('DRAINING_QUARANTINE->RESOLVING_GAPS');
      expect(substateTransitions).toContain('RESOLVING_GAPS->IDLE');
    });

    it('L6: abortRecovery deterministically transitions to DEGRADED and fires onRecoveryDegraded', () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        callbacks: {
          onRecoveryDegraded: (reason) => { degradedReason = reason; },
        },
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');
      expect(coordinator.state).toBe('RECOVERING');

      coordinator.abortRecovery('APPLICATION_ABORT', 'User initiated manual abort');
      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('APPLICATION_ABORT');
    });

    it('L7: resetRecovery resets active recovery to ACTIVE and clears recovery mode', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');
      expect(coordinator.state).toBe('RECOVERING');
      expect(engine.isRecoveryActive()).toBe(true);

      coordinator.resetRecovery('test-harness');
      expect(coordinator.state).toBe('ACTIVE');
      expect(engine.isRecoveryActive()).toBe(false);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 2. Watchdog Timers & Timeouts
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Watchdog Timers & Timeouts', () => {
    it('T1: offerAwaitTimeout fires after offerAwaitTimeoutMs and degrades coordinator', () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        offerAwaitTimeoutMs: 5000,
        callbacks: {
          onRecoveryDegraded: (reason) => { degradedReason = reason; },
        },
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');
      expect(coordinator.substate).toBe('AWAITING_OFFER');

      // Advance clock past offer timeout
      vi.advanceTimersByTime(5001);

      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('NO_OFFERS_AVAILABLE');
    });

    it('T2: overallRecoveryTimeout fires after overallRecoveryTimeoutMs during transfer', () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        overallRecoveryTimeoutMs: 15000,
        offerAwaitTimeoutMs: 10000,
        callbacks: {
          onRecoveryDegraded: (reason) => { degradedReason = reason; },
        },
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');

      // Accept offer to advance from AWAITING_OFFER to TRANSFERRING (clears offerAwaitTimer)
      coordinator.handleSnapshotOffer({
        formatVersion: 1,
        sessionId: 'session-m43c-coord',
        snapshotId: 'snap-offer-01',
        membershipEpoch: 1,
        authorPeerId: 'peer-remote',
        snapshotFrontier: { 'peer-remote': 10 },
        originFrontierTimestamps: { 'peer-remote': 100 },
        maxLogicalTimestamp: 100,
        totalBytes: 1024,
        totalChunks: 1,
        chunkDigestAlgorithm: 'sha256',
        snapshotSha256Digest: 'a'.repeat(64),
      });

      expect(coordinator.substate).toBe('TRANSFERRING');

      // Advance clock past overall recovery timeout
      vi.advanceTimersByTime(15001);

      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('OVERALL_RECOVERY_TIMEOUT');
    });

    it('T3: reconciliationTimeout fires if quarantine drain hangs in RECONCILING', async () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        reconciliationTimeoutMs: 8000,
        callbacks: {
          onRecoveryDegraded: (reason) => { degradedReason = reason; },
        },
      });

      // Hook drainBatch to return a hanging promise so reconcileQuarantine does not finish
      vi.spyOn(coordinator.quarantineDrainer, 'drainBatch').mockImplementation(
        () => new Promise(() => {}),
      );

      // Enqueue an op so reconcileQuarantine enters the drain loop
      const op = createMockRemoteOp(11, 101, { 'peer-remote': 10 });
      coordinator.quarantineBuffer.enqueue(op);

      const payload = createValidSnapshotPayload();
      const executionPromise = coordinator.executeRecoveryWithSnapshot(payload);
      executionPromise.catch(() => {}); // Prevent unhandled rejection warning during timer tick

      // Advance clock past reconciliation timeout
      await vi.advanceTimersByTimeAsync(8001);

      // executeRecoveryWithSnapshot should reject with abort
      await expect(executionPromise).rejects.toThrow();
      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('RECONCILIATION_TIMEOUT');
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 3. Counter Preservation Integration (M4.3b Integration)
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Counter Preservation Integration', () => {
    it('CP1: preserves local logical clock when local clock > snapshot clock (L_local > L_snap)', async () => {
      vi.spyOn(engine, 'logicalClock', 'get').mockReturnValue(75);

      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // Snapshot with maxLogicalTimestamp = 20 (< 75)
      const payload = createValidSnapshotPayload({
        maxLogicalTimestamp: 20,
        originFrontierTimestamps: { 'peer-remote': 20 },
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Target clock must preserve 75, never defaulting to zero!
      expect(engine.logicalClock).toBe(75);
    });

    it('CP2: advances local logical clock when snapshot clock > local clock (L_snap > L_local)', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      expect(engine.logicalClock).toBe(0);

      // Snapshot with maxLogicalTimestamp = 250
      const payload = createValidSnapshotPayload({
        maxLogicalTimestamp: 250,
        originFrontierTimestamps: { 'peer-remote': 250 },
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Target clock must advance to at least 250
      expect(engine.logicalClock).toBeGreaterThanOrEqual(250);
    });

    it('CP3: preserves local sequence when local sequence > snapshot sequence (S_local > S_snap)', async () => {
      const origDiag = engine.getDiagnosticCoreSnapshot();
      vi.spyOn(engine, 'getDiagnosticCoreSnapshot').mockReturnValue({
        ...origDiag,
        localOriginSequence: 8,
      });

      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // Snapshot where snapshotFrontier for peer-local is 0 (or absent)
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { 'peer-remote': 10 },
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Engine localOriginSequence must remain 8
      expect(engine.getDiagnosticCoreSnapshot().localOriginSequence).toBe(8);
    });

    it('CP4: advances local sequence when snapshot sequence > local sequence (S_snap > S_local)', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      expect(engine.getDiagnosticCoreSnapshot().localOriginSequence).toBe(0);

      // Snapshot containing higher sequence for peer-local ('peer-local' < 'peer-remote')
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { 'peer-local': 15, 'peer-remote': 10 },
        originFrontierTimestamps: { 'peer-local': 100, 'peer-remote': 100 },
        compactedCheckpoints: [
          { originPeerId: 'peer-local', sequenceNumber: 15, logicalTimestamp: 100 },
          { originPeerId: 'peer-remote', sequenceNumber: 10, logicalTimestamp: 100 },
        ],
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Engine localOriginSequence must advance to 15
      expect(engine.getDiagnosticCoreSnapshot().localOriginSequence).toBe(15);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 4. Capability Custody & Authorization Security
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Capability Custody & Authorization Security', () => {
    it('CAP1: invalid or forged capability is rejected during atomic commit', async () => {
      // Create a forged capability not belonging to this engine
      const forgedEngine = new LeaderlessSyncEngine({
        ...baseConfig,
        localPeerId: 'peer-forged',
      });
      const forgedCapability = createRecoveryInstallationCapability('forged', baseConfig.sessionId);

      coordinator = new RecoveryCoordinator({
        engine,
        capability: forgedCapability, // Misconfigured forged capability
      });

      const payload = createValidSnapshotPayload();

      // Must throw capability rejection error and abort to DEGRADED
      await expect(coordinator.executeRecoveryWithSnapshot(payload)).rejects.toThrow();
      expect(coordinator.state).toBe('DEGRADED');

      forgedEngine.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 5. Quarantine Draining, Replay & In-Flight Tracking
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Quarantine Draining & Replay Orchestration', () => {
    it('QD1: drains pre-buffered operations and applies valid causal successors', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // Pre-buffer 2 operations into quarantine buffer before snapshot installation
      // Snapshot frontier will be peer-remote: 10
      // Op 1: seq 11, depends on remote: 10, valid causal successor
      // Op 2: seq 12, depends on remote: 11, valid causal successor
      const op1 = createMockRemoteOp(11, 101, { 'peer-remote': 10 });
      const op2 = createMockRemoteOp(12, 102, { 'peer-remote': 11 });

      coordinator.quarantineBuffer.enqueue(op1);
      coordinator.quarantineBuffer.enqueue(op2);
      expect(coordinator.quarantineBuffer.size).toBe(2);

      const snapshotPayload = createValidSnapshotPayload({
        snapshotFrontier: { 'peer-remote': 10 },
        originFrontierTimestamps: { 'peer-remote': 100 },
        maxLogicalTimestamp: 100,
      });

      await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

      // Quarantine buffer must be fully drained
      expect(coordinator.quarantineBuffer.size).toBe(0);
      expect(coordinator.quarantineBuffer.drainSize).toBe(0);

      // The two valid operations must be admitted into engine operationLog
      expect(engine.hasOperation('peer-remote', 11)).toBe(true);
      expect(engine.hasOperation('peer-remote', 12)).toBe(true);
      expect(engine.replicationFrontier['peer-remote']).toBe(12);

      // Engine must have cleanly returned to ACTIVE
      expect(coordinator.state).toBe('ACTIVE');
      expect(engine.isRecoveryActive()).toBe(false);
    });

    it('QD2: drops drained operations that are already represented in snapshot frontier', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // Snapshot will have remote: 10
      // Buffer op with seq 5 (<= 10), so it is already represented in base snapshot
      const staleOp = createMockRemoteOp(5, 50, {});
      coordinator.quarantineBuffer.enqueue(staleOp);

      const snapshotPayload = createValidSnapshotPayload({
        snapshotFrontier: { 'peer-remote': 10 },
      });

      await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

      expect(coordinator.quarantineBuffer.size).toBe(0);
      // Operation seq 5 must NOT be admitted as a new log entry
      expect(engine.operationLog.getAllOperations().some(op => op.originSequenceNumber === 5)).toBe(false);
      expect(coordinator.getMetrics().quarantinedDiscarded).toBe(1);
    });

    it('QD3: routes drained operations with missing causal dependencies to pending queue', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // Snapshot has remote: 10
      // Buffer op with seq 15, depending on missing seq 14
      const gapOp = createMockRemoteOp(15, 115, { 'peer-remote': 14 });
      coordinator.quarantineBuffer.enqueue(gapOp);

      const snapshotPayload = createValidSnapshotPayload({
        snapshotFrontier: { 'peer-remote': 10 },
      });

      await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

      expect(coordinator.quarantineBuffer.size).toBe(0);
      // Gap op must be in engine's pending queue, not in log
      expect(engine.pendingQueue.size).toBe(1);
      expect(engine.hasOperation('peer-remote', 15)).toBe(true);
    });

    it('QD4: isInFlight prevents duplicate remote arrival while operation is held in DRAINER_IN_FLIGHT', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      const op = createMockRemoteOp(11, 101, { 'peer-remote': 10 });

      // Simulate drainer holding the operation in DRAINER_IN_FLIGHT
      (coordinator.quarantineDrainer as any)._inFlightRecord = {
        operation: op,
        dequeuedAtMs: Date.now(),
        attemptCount: 1,
        destinationStatus: 'PENDING',
      };

      expect(coordinator.inFlightRecord).not.toBeNull();

      // Intercept remote event with exact same (peer-remote, 11)
      const interceptResult = coordinator.interceptRemoteEvent(op.event, 128);

      // Must be dropped as duplicate
      expect(interceptResult.shouldQuarantine).toBe(false);
      expect(interceptResult.dropped).toBe(true);
      expect(interceptResult.duplicate).toBe(true);

      // Clean up mock
      (coordinator.quarantineDrainer as any)._inFlightRecord = null;
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 6. Admission Interception & Critical Section
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Admission Interception & Critical Section', () => {
    it('ADM1: interceptLocalSubmission fail-closes during RECOVERING and RECONCILING', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      // When ACTIVE, local submission is allowed
      expect(coordinator.interceptLocalSubmission().allowed).toBe(true);

      // In RECOVERING
      coordinator.triggerRecovery('peer-remote', 'Causal gap');
      const recResult = coordinator.interceptLocalSubmission();
      expect(recResult.allowed).toBe(false);
      expect(recResult.code).toBe('REJECTED_RECOVERING');

      // In RECONCILING
      coordinator.stateMachine.transition('RECONCILING', 'Reconciliation', 'DRAINING_QUARANTINE');
      const reconResult = coordinator.interceptLocalSubmission();
      expect(reconResult.allowed).toBe(false);
      expect(reconResult.code).toBe('REJECTED_RECOVERING');
    });

    it('ADM2: interceptRemoteEvent quarantines current epoch events and drops obsolete epoch events', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      coordinator.triggerRecovery('peer-remote', 'Causal gap');

      // Current epoch (epoch 1) event
      const currentOp = createMockRemoteOp(1, 1);
      const resCurrent = coordinator.interceptRemoteEvent(currentOp.event, 128);
      expect(resCurrent.shouldQuarantine).toBe(true);
      expect(coordinator.quarantineBuffer.size).toBe(1);

      // Obsolete epoch (epoch 0) event
      const obsoleteOp = createMockRemoteOp(2, 2);
      (obsoleteOp.event as any).membershipEpoch = 0;
      const resObsolete = coordinator.interceptRemoteEvent(obsoleteOp.event, 128);
      expect(resObsolete.shouldQuarantine).toBe(false);
      expect(resObsolete.dropped).toBe(true);
    });

    it('ADM3: fires onQuarantineWarning when quarantine buffer reaches threshold', () => {
      let warningFired = false;

      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        quarantineWarningThresholdBytes: 100,
        callbacks: {
          onQuarantineWarning: () => { warningFired = true; },
        },
      });

      coordinator.triggerRecovery('peer-remote', 'Trigger');

      // Event with 128 bytes exceeds warning threshold (100 bytes)
      const op = createMockRemoteOp(1, 1);
      coordinator.interceptRemoteEvent(op.event, 128);

      expect(warningFired).toBe(true);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 7. Disposal & Metrics
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Disposal & Metrics', () => {
    it('DISP1: dispose releases all resources and forbids further operations', () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      coordinator.dispose();
      expect(coordinator.isDisposed).toBe(true);
      expect(coordinator.quarantineDrainer.isDisposed).toBe(true);
      expect(coordinator.quarantineBuffer.isDisposed).toBe(true);
      expect(coordinator.futureEpochBuffer.isDisposed).toBe(true);

      expect(() => coordinator.triggerRecovery('peer-remote', 'Trigger')).toThrow();
      expect(() => coordinator.start()).toThrow();
    });

    it('MET1: getMetrics returns complete diagnostic metrics', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      const metricsBefore = coordinator.getMetrics();
      expect(metricsBefore.totalDurationMs).toBe(0);
      expect(metricsBefore.quarantinedApplied).toBe(0);

      const op = createMockRemoteOp(11, 101, { 'peer-remote': 10 });
      coordinator.quarantineBuffer.enqueue(op);

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      const metricsAfter = coordinator.getMetrics();
      expect(metricsAfter.quarantinedApplied).toBe(1);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // 8. M4.3c-R1 Adversarial Fencing, Drain Ownership & Handoff Ordering
  // ─────────────────────────────────────────────────────────────────────────────
  describe('M4.3c-R1 Adversarial Fencing, Drain Ownership & Handoff Ordering', () => {
    it('R1_F1: blocks staging and commit if overall timeout fires during asynchronous validation', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        overallRecoveryTimeoutMs: 1000,
      });

      let resolveValidation!: (val: any) => void;
      const validationPromise = new Promise((resolve) => {
        resolveValidation = resolve;
      });

      vi.spyOn(coordinator.snapshotInstaller, 'validate').mockImplementation(
        () => validationPromise as any,
      );
      const stageSpy = vi.spyOn(coordinator.snapshotInstaller, 'stage');
      const commitSpy = vi.spyOn(coordinator.snapshotInstaller, 'commitAtomic');

      const payload = createValidSnapshotPayload();
      const execPromise = coordinator.executeRecoveryWithSnapshot(payload);
      execPromise.catch(() => {});

      expect(coordinator.substate).toBe('VALIDATING');

      // Trigger overall timeout
      await vi.advanceTimersByTimeAsync(1001);

      expect(coordinator.state).toBe('DEGRADED');

      // Now validation resolves belatedly
      resolveValidation({
        valid: true,
        parsedPayload: payload,
      });

      await expect(execPromise).rejects.toThrow();
      expect(stageSpy).not.toHaveBeenCalled();
      expect(commitSpy).not.toHaveBeenCalled();
    });

    it('R1_F2: blocks staging and commit if abortRecovery is called during validation', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      let resolveValidation!: (val: any) => void;
      const validationPromise = new Promise((resolve) => {
        resolveValidation = resolve;
      });

      vi.spyOn(coordinator.snapshotInstaller, 'validate').mockImplementation(
        () => validationPromise as any,
      );
      const stageSpy = vi.spyOn(coordinator.snapshotInstaller, 'stage');
      const commitSpy = vi.spyOn(coordinator.snapshotInstaller, 'commitAtomic');

      const payload = createValidSnapshotPayload();
      const execPromise = coordinator.executeRecoveryWithSnapshot(payload);
      execPromise.catch(() => {});

      expect(coordinator.substate).toBe('VALIDATING');

      // Explicit abort
      coordinator.abortRecovery('APPLICATION_ABORT', 'Manual abort');
      expect(coordinator.state).toBe('DEGRADED');

      // Validation resolves
      resolveValidation({
        valid: true,
        parsedPayload: payload,
      });

      await expect(execPromise).rejects.toThrow();
      expect(stageSpy).not.toHaveBeenCalled();
      expect(commitSpy).not.toHaveBeenCalled();
    });

    it('R1_F3: newer recovery attempt invalidates earlier attempt preventing stale commit', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
      });

      let resolveValidation1!: (val: any) => void;
      const validationPromise1 = new Promise((resolve) => {
        resolveValidation1 = resolve;
      });

      const validateSpy = vi.spyOn(coordinator.snapshotInstaller, 'validate');
      validateSpy.mockImplementationOnce(() => validationPromise1 as any);

      const commitSpy = vi.spyOn(coordinator.snapshotInstaller, 'commitAtomic');

      const payload1 = createValidSnapshotPayload({ snapshotId: 'snap-1' });
      const execPromise1 = coordinator.executeRecoveryWithSnapshot(payload1);
      execPromise1.catch(() => {});

      const attempt1 = coordinator.currentAttemptId;
      expect(attempt1).toBe(1);

      // Abort attempt 1 and start attempt 2
      coordinator.abortRecovery('APPLICATION_ABORT', 'Resetting');
      expect(coordinator.state).toBe('DEGRADED');

      coordinator.triggerRecovery('peer-remote-2', 'Attempt 2');
      const attempt2 = coordinator.currentAttemptId;
      expect(attempt2).toBe(3); // 1 (trigger) -> 2 (abort) -> 3 (trigger)

      // Late resolution of attempt 1's validation
      resolveValidation1({
        valid: true,
        parsedPayload: payload1,
      });

      await expect(execPromise1).rejects.toThrow();
      expect(commitSpy).not.toHaveBeenCalled();
    });

    it('R1_F4: reconciliation loop halts immediately when timeout occurs during reconcileQuarantine', async () => {
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        reconciliationTimeoutMs: 5000,
      });

      let drainCount = 0;
      let resolveDrain1!: (res: any) => void;
      const drainPromise1 = new Promise((resolve) => {
        resolveDrain1 = resolve;
      });

      vi.spyOn(coordinator.quarantineDrainer, 'drainBatch').mockImplementation(() => {
        drainCount++;
        if (drainCount === 1) {
          return drainPromise1 as any;
        }
        return Promise.resolve({
          batchSize: 1,
          appliedCount: 1,
          pendingCount: 0,
          discardedCount: 0,
          haltedOnCapacity: false,
        });
      });

      const op1 = createMockRemoteOp(11, 101);
      const op2 = createMockRemoteOp(12, 102);
      coordinator.quarantineBuffer.enqueue(op1);
      coordinator.quarantineBuffer.enqueue(op2);

      const payload = createValidSnapshotPayload();
      const execPromise = coordinator.executeRecoveryWithSnapshot(payload);
      execPromise.catch(() => {});

      // Advance clock past reconciliation timeout while drain 1 is in-flight
      await vi.advanceTimersByTimeAsync(5001);
      expect(coordinator.state).toBe('DEGRADED');

      // Resolve drain 1
      resolveDrain1({
        batchSize: 1,
        appliedCount: 1,
        pendingCount: 0,
        discardedCount: 0,
        haltedOnCapacity: false,
      });

      await expect(execPromise).rejects.toThrow();
      // Drain loop should NOT have executed batch 2
      expect(drainCount).toBe(1);
      // State machine should NOT have reached RESOLVING_GAPS
      expect(coordinator.substate).not.toBe('RESOLVING_GAPS');
    });

    it('R1_F5: synchronous handoff guarantees observers of ACTIVE see engine.isRecoveryActive() === false', async () => {
      let recoveryActiveInListener: boolean | null = null;
      let stateTransitionObserved: string | null = null;

      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        callbacks: {
          onStateChange: (newState) => {
            if (newState === 'ACTIVE') {
              stateTransitionObserved = newState;
              recoveryActiveInListener = engine.isRecoveryActive();
            }
          },
        },
      });

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(stateTransitionObserved).toBe('ACTIVE');
      expect(recoveryActiveInListener).toBe(false);
      expect(coordinator.state).toBe('ACTIVE');
      expect(engine.isRecoveryActive()).toBe(false);
    });
  });
});
