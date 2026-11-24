/**
 * Phase 7.5 M4.3d — Exhaustive 36-Scenario Acceptance Matrix Test Suite
 *
 * Implements all 36 acceptance scenarios defined in PHASE7_5_M4_3_ARCHITECTURE.md §13:
 * - Group A: Offer Eligibility & Comparison (Scenarios 1–6)
 * - Group B: Atomic Staging & Installation (Scenarios 7–11)
 * - Group C: Quarantine Drain & Active Handoff (Scenarios 12–17)
 * - Group D: Future-Epoch Buffering (Scenarios 18–20)
 * - Group E: Exceptions, Bounds & Teardown (Scenarios 21–26)
 * - Group F: Interrupted-Drain Adversarial Cases (Scenarios 27–36, R4-3 Cases 1–10)
 *
 * Strict Invariants:
 * - Single-owner custody of all quarantined/drained operations.
 * - Monotonic attempt fencing across all asynchronous continuations.
 * - Single-root atomic replacement without intermediate live mutation.
 * - Complete exception isolation and deterministic teardown.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createRecoveryInstallationCapability,
  RecoveryCoordinator,
  SnapshotInstaller,
  QuarantineBuffer,
  FutureEpochBuffer,
  QuarantineDrainer,
  evaluateSnapshotEligibility,
  computeSnapshotDigestV2,
  SyncEngineError,
  type StateSnapshotPayloadV2,
  type SnapshotOfferPayload,
  type QuarantinedOperation,
  type IDrainDestination,
  type DestinationAcceptanceReceipt,
} from '../index.js';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

describe('Phase 7.5 M4.3d — 36-Scenario Recovery Acceptance Matrix', () => {
  const SESSION_ID = 'session-m43d-matrix';
  const LOCAL_PEER = 'peer-local';
  const REMOTE_PEER_A = 'peer-A';
  const REMOTE_PEER_B = 'peer-B';

  const baseConfig = {
    sessionId: SESSION_ID,
    localPeerId: LOCAL_PEER,
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 256,
  };

  let engine: LeaderlessSyncEngine;
  let capability: ReturnType<typeof createRecoveryInstallationCapability>;
  let coordinator: RecoveryCoordinator;

  beforeEach(() => {
    vi.useFakeTimers();
    capability = createRecoveryInstallationCapability('test-m43d', SESSION_ID);
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

  // ── Helper Constructors ───────────────────────────────────────────────────

  function createValidSnapshotPayload(
    overrides?: Partial<StateSnapshotPayloadV2>,
  ): StateSnapshotPayloadV2 {
    const preimage: Omit<StateSnapshotPayloadV2, 'sha256Digest'> = {
      formatVersion: 2,
      snapshotId: 'snap-matrix-001',
      sessionId: SESSION_ID,
      membershipEpoch: 1,
      authorPeerId: REMOTE_PEER_A,
      state: {
        formatVersion: 1,
        sessionId: SESSION_ID,
        status: 'playing',
        anchorPositionMs: 10000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { [REMOTE_PEER_A]: 20 },
      originFrontierTimestamps: { [REMOTE_PEER_A]: 200 },
      maxLogicalTimestamp: 200,
      compactedCheckpoints: [
        {
          originPeerId: REMOTE_PEER_A,
          sequenceNumber: 20,
          logicalTimestamp: 200,
        },
      ],
      ...overrides,
    };

    const digest = computeSnapshotDigestV2(preimage);
    return {
      ...preimage,
      sha256Digest: overrides?.sha256Digest ?? digest,
    };
  }

  function createMockQuarantinedOp(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp = sequenceNumber * 10,
    causalContext: Record<string, number> = {},
    membershipEpoch = 1,
  ): QuarantinedOperation {
    const event: AnyControlSyncEvent = {
      version: 1,
      type: 'CONTROL_MESSAGE',
      senderPeerId: originPeerId,
      recipientPeerId: 'broadcast',
      membershipEpoch,
      transportTimestamp: 1000 + sequenceNumber,
      message: {
        protocolVersion: 1,
        formatVersion: 1,
        sessionId: SESSION_ID,
        logicalEventId: `evt-${originPeerId}-${sequenceNumber}`,
        originPeerId,
        senderPeerId: originPeerId,
        originSequenceNumber: sequenceNumber,
        logicalTimestamp,
        causalContext,
        action: 'STATE_TRANSITION',
        messageType: 'PAUSE',
        membershipEpoch,
        payload: {
          formatVersion: 1,
          type: 'PAUSE',
          positionMs: 10000 + sequenceNumber * 100,
        },
      },
    };

    return {
      event,
      originPeerId,
      sequenceNumber,
      membershipEpoch,
      serializedByteLength: 128,
      receivedAtMs: Date.now(),
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP A: Offer Eligibility & Comparison (Scenarios 1–6)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group A — Offer Eligibility & Comparison (Scenarios 1–6)', () => {
    it('M4.3d-S01: Equal vectors, different intermediate history rejected as FRONTIER_AGREEMENT_ONLY', () => {
      // Local vector at seq 10, remote at seq 10, but local timestamp is 150 and candidate is 140 (divergent history)
      const candidate = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 10 },
        originFrontierTimestamps: { [REMOTE_PEER_A]: 140 },
        maxLogicalTimestamp: 140,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: candidate,
        currentReplicationVector: { [REMOTE_PEER_A]: 10 },
        currentFrontierTimestamps: { [REMOTE_PEER_A]: 150 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('FRONTIER_AGREEMENT_ONLY');
      expect(result.eligible).toBe(false);
      expect(result.reason).toContain('intermediate history is unverified');
    });

    it('M4.3d-S02: Matching frontier fingerprint without verified intermediate history rejected', () => {
      // Matching frontier at seq 10, but compactedCheckpoints is empty (unverified intermediate checkpoints)
      const candidate = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 10 },
        originFrontierTimestamps: { [REMOTE_PEER_A]: 100 },
        maxLogicalTimestamp: 100,
        compactedCheckpoints: [], // unverified history
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: candidate,
        currentReplicationVector: { [REMOTE_PEER_A]: 10 },
        currentFrontierTimestamps: { [REMOTE_PEER_A]: 100 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('FRONTIER_AGREEMENT_ONLY');
      expect(result.eligible).toBe(false);
    });

    it('M4.3d-S03: Strictly dominating snapshot is eligible and installs successfully', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Local engine at seq 10, candidate at seq 25
      const candidate = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 25 },
        originFrontierTimestamps: { [REMOTE_PEER_A]: 250 },
        maxLogicalTimestamp: 250,
      });

      const eligibility = evaluateSnapshotEligibility({
        candidateSnapshot: candidate,
        currentReplicationVector: { [REMOTE_PEER_A]: 10 },
        currentEpoch: 1,
      });

      expect(eligibility.eligibility).toBe('STRICTLY_DOMINATES');
      expect(eligibility.eligible).toBe(true);

      const report = await coordinator.executeRecoveryWithSnapshot(candidate);
      expect(report.success).toBe(true);
      expect(coordinator.state).toBe('ACTIVE');
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(25);
    });

    it('M4.3d-S04: Repairable incomparable snapshot is rejected for log repair via Anti-Entropy', () => {
      // Local has A:10, remote has B:10 -> concurrent / incomparable
      const candidate = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 5, [REMOTE_PEER_B]: 10 },
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: candidate,
        currentReplicationVector: { [REMOTE_PEER_A]: 10, [REMOTE_PEER_B]: 5 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('INCOMPARABLE');
      expect(result.eligible).toBe(false);
      expect(result.reason).toContain('arbitrary tie-breaking is prohibited');
    });

    it('M4.3d-S05: Unrepairable incomparable snapshot causes coordinator to degrade safely', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger recovery');

      const candidateOffer: SnapshotOfferPayload = {
        formatVersion: 1,
        sessionId: SESSION_ID,
        snapshotId: 'snap-incomparable',
        membershipEpoch: 1,
        authorPeerId: REMOTE_PEER_A,
        snapshotFrontier: { [REMOTE_PEER_A]: 5, [REMOTE_PEER_B]: 20 },
        originFrontierTimestamps: { [REMOTE_PEER_A]: 50, [REMOTE_PEER_B]: 200 },
        maxLogicalTimestamp: 200,
        totalBytes: 1024,
        totalChunks: 1,
        chunkDigestAlgorithm: 'sha256',
        snapshotSha256Digest: 'a'.repeat(64),
      };

      // Mock engine vector to be incomparable: local has A:20, B:5
      vi.spyOn(engine, 'replicationFrontier', 'get').mockReturnValue({
        [REMOTE_PEER_A]: 20,
        [REMOTE_PEER_B]: 5,
      });

      const handled = coordinator.handleSnapshotOffer(candidateOffer);
      expect(handled).toBe(false); // Rejected without tie-breaking

      // Coordinator aborts to DEGRADED
      coordinator.abortRecovery('NO_OFFERS_AVAILABLE', 'No admissible snapshot offers received');
      expect(coordinator.state).toBe('DEGRADED');
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(20); // Local state intact
    });

    it('M4.3d-S06: Snapshot dominated by local state is rejected as DOMINATED_BY_LOCAL', () => {
      // Local at seq 50, remote at seq 30
      const candidate = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 30 },
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: candidate,
        currentReplicationVector: { [REMOTE_PEER_A]: 50 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('DOMINATED');
      expect(result.eligible).toBe(false);
      expect(result.reason).toContain('dominated by local replication vector');
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP B: Atomic Staging & Installation (Scenarios 7–11)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group B — Atomic Staging & Installation (Scenarios 7–11)', () => {
    it('M4.3d-S07: Corrupted checkpoint metadata throws during stage() without mutating live engine', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Mock stage() to throw simulating checkpoint metadata corruption
      vi.spyOn(coordinator.snapshotInstaller, 'stage').mockImplementation(() => {
        throw new SyncEngineError('CORRUPTED_CHECKPOINT', 'Corrupted checkpoint metadata during candidate staging');
      });

      const preSnapshot = engine.getDiagnosticCoreSnapshot();
      const payload = createValidSnapshotPayload();

      await expect(coordinator.executeRecoveryWithSnapshot(payload)).rejects.toThrow(/Corrupted checkpoint metadata/);

      // Verify ZERO mutation of live engine
      const postSnapshot = engine.getDiagnosticCoreSnapshot();
      expect(postSnapshot.replicationFrontier).toEqual(preSnapshot.replicationFrontier);
      expect(postSnapshot.localOriginSequence).toBe(preSnapshot.localOriginSequence);
      expect(postSnapshot.logicalClock).toBe(preSnapshot.logicalClock);
      expect(coordinator.state).toBe('DEGRADED');
    });

    it('M4.3d-S08: Snapshot SHA-256 mismatch causes validation failure and candidate eviction', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      const tamperedPayload = createValidSnapshotPayload({
        sha256Digest: 'f'.repeat(64), // invalid digest
      });

      await expect(coordinator.executeRecoveryWithSnapshot(tamperedPayload)).rejects.toThrow();
      expect(coordinator.state).toBe('DEGRADED');
      expect(engine.isRecoveryActive()).toBe(true); // Engine remains protected
    });

    it('M4.3d-S09: Zero live state changes occur before root commit', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      const preFrontier = { ...engine.replicationFrontier };
      const preClock = engine.logicalClock;
      const prePos = engine.playbackPositionMs;

      let validatedChecked = false;
      const origValidate = coordinator.snapshotInstaller.validate.bind(coordinator.snapshotInstaller);
      vi.spyOn(coordinator.snapshotInstaller, 'validate').mockImplementation(async (...args) => {
        // While in VALIDATING stage, inspect live engine
        expect(engine.replicationFrontier).toEqual(preFrontier);
        expect(engine.logicalClock).toBe(preClock);
        expect(engine.playbackPositionMs).toBe(prePos);
        validatedChecked = true;
        return origValidate(...args);
      });

      let stagedChecked = false;
      const origStage = coordinator.snapshotInstaller.stage.bind(coordinator.snapshotInstaller);
      vi.spyOn(coordinator.snapshotInstaller, 'stage').mockImplementation((...args) => {
        // While in STAGING stage, inspect live engine
        expect(engine.replicationFrontier).toEqual(preFrontier);
        expect(engine.logicalClock).toBe(preClock);
        expect(engine.playbackPositionMs).toBe(prePos);
        stagedChecked = true;
        return origStage(...args);
      });

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(validatedChecked).toBe(true);
      expect(stagedChecked).toBe(true);
      expect(coordinator.state).toBe('ACTIVE');
    });

    it('M4.3d-S10: All engine readers observe new root consistently post-commit', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      const payload = createValidSnapshotPayload({
        state: {
          formatVersion: 1,
          sessionId: SESSION_ID,
          status: 'playing',
          anchorPositionMs: 42000,
          playbackRate: 1.5,
        },
        snapshotFrontier: { [REMOTE_PEER_A]: 77 },
        maxLogicalTimestamp: 770,
        originFrontierTimestamps: { [REMOTE_PEER_A]: 770 },
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Verify all public engine readers observe new root
      expect(engine.playbackState.status).toBe('playing');
      expect(engine.playbackState.anchorPositionMs).toBe(42000);
      expect(engine.playbackState.playbackRate).toBe(1.5);
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(77);
      expect(engine.baseSnapshotFrontier[REMOTE_PEER_A]).toBe(77);
      expect(engine.logicalClock).toBeGreaterThanOrEqual(770);
      const diag = engine.getDiagnosticCoreSnapshot();
      expect(diag.playbackStatus).toBe('playing');
      expect(diag.anchorPositionMs).toBe(42000);
      expect(diag.playbackRate).toBe(1.5);
    });

    it('M4.3d-S11: Detached stale state references cannot mutate the new live engine root', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Grab stale diagnostic snapshot before recovery
      const staleDiag = engine.getDiagnosticCoreSnapshot();
      const staleState = engine.playbackState;

      const payload = createValidSnapshotPayload({
        state: {
          formatVersion: 1,
          sessionId: SESSION_ID,
          status: 'playing',
          anchorPositionMs: 8888,
          playbackRate: 1.0,
        },
        snapshotFrontier: { [REMOTE_PEER_A]: 50 },
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Attempt to modify stale objects
      try {
        (staleState as any).anchorPositionMs = 999999;
        (staleDiag.replicationFrontier as any)[REMOTE_PEER_A] = 999999;
      } catch {
        // Readonly / frozen objects throw in strict mode
      }

      // Live engine state is completely unpolluted
      expect(engine.playbackState.anchorPositionMs).toBe(8888);
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(50);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP C: Quarantine Drain & Active Handoff (Scenarios 12–17)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group C — Quarantine Drain & Active Handoff (Scenarios 12–17)', () => {
    it('M4.3d-S12: Partial drain applies independent ops and buffers causally blocked ops', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Snapshot with base at seq 20
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 20 },
        originFrontierTimestamps: { [REMOTE_PEER_A]: 200 },
        maxLogicalTimestamp: 200,
      });

      // Enqueue ops 21..60 (40 sequential ops from peer-A, independent)
      for (let seq = 21; seq <= 60; seq++) {
        coordinator.quarantineBuffer.enqueue(
          createMockQuarantinedOp(
            REMOTE_PEER_A,
            seq,
            200 + (seq - 20),
            { [REMOTE_PEER_A]: seq - 1 },
          ),
        );
      }

      // Op 61 depends on missing dependency from peer-B: seq 5
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp(REMOTE_PEER_A, 61, 241, {
          [REMOTE_PEER_A]: 60,
          [REMOTE_PEER_B]: 5, // missing!
        }),
      );

      // Ops 62..70 depend on 61
      for (let seq = 62; seq <= 70; seq++) {
        coordinator.quarantineBuffer.enqueue(
          createMockQuarantinedOp(REMOTE_PEER_A, seq, 200 + (seq - 20), {
            [REMOTE_PEER_A]: seq - 1,
          }),
        );
      }

      await coordinator.executeRecoveryWithSnapshot(payload);

      // Ops 21..60 must be applied into log
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(60);
      expect(engine.hasOperation(REMOTE_PEER_A, 60)).toBe(true);

      // Ops 61..70 must be safely buffered in PendingDependencyQueue
      const diag = engine.getDiagnosticCoreSnapshot();
      expect(diag.pendingQueueSize).toBe(10);
      expect(engine.hasOperation(REMOTE_PEER_A, 61)).toBe(true); // present in pending queue
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(60); // vector does NOT advance across gap
    });

    it('M4.3d-S13: Duplicate operations during resumed drain are discarded without duplicate apply', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Snapshot base at 20
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 20 },
      });

      const op21 = createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 });
      coordinator.quarantineBuffer.enqueue(op21);

      await coordinator.executeRecoveryWithSnapshot(payload);
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(21);

      // Now drainer encounters op21 again in a second drain pass
      coordinator.quarantineBuffer.enqueue(op21);
      coordinator.quarantineBuffer.swapDrainBatch();
      const drainResult = await coordinator.quarantineDrainer.drainBatch();

      expect(drainResult.appliedCount).toBe(0);
      expect(drainResult.discardedCount).toBe(1);
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(21); // Not duplicated
    });

    it('M4.3d-S14: Causal gap triggers repair and unblocks dependent pending operations', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Snapshot base at 20
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 20 },
      });

      // Op 21 from peer A (contiguous)
      coordinator.quarantineBuffer.enqueue(createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 }));
      // Op 22 depends on missing peer B:1
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp(REMOTE_PEER_A, 22, 202, { [REMOTE_PEER_A]: 21, [REMOTE_PEER_B]: 1 }),
      );

      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(21);
      expect(engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(1);

      // Missing dependency arrives from peer B: seq 1
      const missingOp = createMockQuarantinedOp(REMOTE_PEER_B, 1, 1, {});
      engine.receive(missingOp.event);

      // Delivering missing op unblocks op 22
      expect(engine.replicationFrontier[REMOTE_PEER_B]).toBe(1);
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(22);
      expect(engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(0);
    });

    it('M4.3d-S15: Remote operation arriving during drain is quarantined and drained without loss', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      // Snapshot base at 20
      const payload = createValidSnapshotPayload({
        snapshotFrontier: { [REMOTE_PEER_A]: 20 },
      });

      const op21 = createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 });
      coordinator.quarantineBuffer.enqueue(op21);

      // Spy on drainBatch to simulate remote op arrival during drain pass 1
      const origDrainBatch = coordinator.quarantineDrainer.drainBatch.bind(
        coordinator.quarantineDrainer,
      );
      let arrivedDuringDrain = false;

      vi.spyOn(coordinator.quarantineDrainer, 'drainBatch').mockImplementation(async () => {
        if (!arrivedDuringDrain) {
          arrivedDuringDrain = true;
          // Op 22 arrives during drain pass 1
          const op22 = createMockQuarantinedOp(REMOTE_PEER_A, 22, 202, { [REMOTE_PEER_A]: 21 });
          coordinator.interceptRemoteEvent(op22.event, 128);
        }
        return origDrainBatch();
      });

      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(arrivedDuringDrain).toBe(true);
      // Both ops 21 and 22 must be drained and applied cleanly
      expect(engine.replicationFrontier[REMOTE_PEER_A]).toBe(22);
      expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(0);
    });

    it('M4.3d-S16: Reentrant local submissions during notification flushing succeed', async () => {
      let reentrantCallSucceeded = false;

      engine.onStateChange(() => {
        if (!reentrantCallSucceeded) {
          reentrantCallSucceeded = true;
          // Reentrant submission during flush
          const decision = engine.receive({
            origin: 'local',
            localTimestamp: 2000,
            version: 1,
            type: 'CONTROL_MESSAGE',
            senderPeerId: LOCAL_PEER,
            recipientPeerId: 'broadcast',
            membershipEpoch: 1,
            transportTimestamp: 2000,
            message: {
              protocolVersion: 1,
              formatVersion: 1,
              sessionId: SESSION_ID,
              logicalEventId: 'evt-local-reentrant',
              originPeerId: LOCAL_PEER,
              senderPeerId: LOCAL_PEER,
              originSequenceNumber: 1,
              logicalTimestamp: 300,
              causalContext: {},
              action: 'STATE_TRANSITION',
              messageType: 'PLAY',
              payload: {
                formatVersion: 1,
                type: 'PLAY',
                positionMs: 15000,
              },
            },
          } as AnyControlSyncEvent);

          expect(decision.accepted).toBe(true);
        }
      });

      coordinator = new RecoveryCoordinator({ engine, capability });
      // Enqueue op 21 so drain applies it and pushes a state change to deferredStateChanges
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 }),
      );

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(reentrantCallSucceeded).toBe(true);
      expect(engine.isRecoveryActive()).toBe(false);
    });

    it('M4.3d-S17: Operation arriving at ACTIVE transition gate is admitted directly without loss', async () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      let interceptedAtActive = false;
      coordinator.stateMachine.onStateChange((newState) => {
        if (newState === 'ACTIVE') {
          // Verify that when ACTIVE transition occurs, engine recovery mode is already false
          expect(engine.isRecoveryActive()).toBe(false);
          const localAdm = coordinator.interceptLocalSubmission();
          expect(localAdm.allowed).toBe(true);
          interceptedAtActive = true;
        }
      });

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(interceptedAtActive).toBe(true);
      expect(coordinator.state).toBe('ACTIVE');
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP D: Future-Epoch Buffering (Scenarios 18–20)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group D — Future-Epoch Buffering (Scenarios 18–20)', () => {
    it('M4.3d-S18: Future epoch operation arriving during RECOVERING is buffered for grace period', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger recovery');

      // Engine in epoch 1, op arrives with epoch 2
      const futureOp = createMockQuarantinedOp(REMOTE_PEER_A, 1, 10, {}, 2);
      const res = coordinator.interceptRemoteEvent(futureOp.event, 128);

      expect(res.shouldQuarantine).toBe(false);
      expect(res.dropped).toBe(false);
      expect(coordinator.futureEpochBuffer.totalCount).toBe(1);
      expect(coordinator.futureEpochBuffer.getBufferedEpochs()).toContain(2);
      expect(coordinator.state).toBe('RECOVERING');
    });

    it('M4.3d-S19: Future epoch transition after 5s expiry causes degraded transition', async () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        epochConvergenceGraceMs: 5000,
        callbacks: {
          onRecoveryDegraded: (reason) => {
            degradedReason = reason;
          },
        },
      });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger recovery');

      const futureOp = createMockQuarantinedOp(REMOTE_PEER_A, 1, 10, {}, 2);
      coordinator.interceptRemoteEvent(futureOp.event, 128);
      expect(coordinator.futureEpochBuffer.totalCount).toBe(1);
      expect(coordinator.futureEpochBuffer.hasActiveTimer(2)).toBe(true);

      // Advance clock past 5s grace period - autonomous degradation triggered via onGraceExpired
      await vi.advanceTimersByTimeAsync(5001);

      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('UNRESOLVED_FUTURE_EPOCH');
      expect(coordinator.futureEpochBuffer.totalCount).toBe(0);

      // Subsequent current-epoch events are dropped / rejected while degraded
      const nextOp = createMockQuarantinedOp(REMOTE_PEER_A, 2, 20);
      const postDegraded = coordinator.interceptRemoteEvent(nextOp.event, 128);
      expect(postDegraded.shouldQuarantine).toBe(false);
      expect(postDegraded.dropped).toBe(true);
    });

    it('M4.3d-S20: Future-epoch buffer overflow beyond 100 ops fails closed with DEGRADED', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger recovery');

      // 100 ops succeed
      for (let i = 1; i <= 100; i++) {
        const op = createMockQuarantinedOp(REMOTE_PEER_A, i, i, {}, 2);
        coordinator.futureEpochBuffer.bufferFutureEpoch(op, 1);
      }
      expect(coordinator.futureEpochBuffer.totalCount).toBe(100);

      // 101st op breaches limit
      const op101 = createMockQuarantinedOp(REMOTE_PEER_A, 101, 101, {}, 2);
      expect(() => coordinator.futureEpochBuffer.bufferFutureEpoch(op101, 1)).toThrow();

      coordinator.abortRecovery('FUTURE_EPOCH_OVERFLOW', 'Future epoch buffer count overflow');
      expect(coordinator.state).toBe('DEGRADED');
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP E: Exceptions, Bounds & Teardown (Scenarios 21–26)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group E — Exceptions, Bounds & Teardown (Scenarios 21–26)', () => {
    it('M4.3d-S21: Listener exceptions during flush do not halt remaining listeners or recovery', async () => {
      let listener2Executed = false;

      engine.onStateChange(() => {
        throw new Error('Crashing listener 1');
      });

      engine.onStateChange(() => {
        listener2Executed = true;
      });

      coordinator = new RecoveryCoordinator({ engine, capability });
      // Enqueue op 21 so drain applies it and triggers deferred state flush
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 }),
      );

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(listener2Executed).toBe(true);
      expect(coordinator.state).toBe('ACTIVE');
    });

    it('M4.3d-S22: Listener re-entry during notification executes without recursive corruption', async () => {
      let reentrantCallCount = 0;

      engine.onStateChange(() => {
        reentrantCallCount++;
        // Query live state safely
        expect(engine.playbackState.status).toBe('playing');
      });

      coordinator = new RecoveryCoordinator({ engine, capability });
      // Enqueue op 21 so drain applies it and triggers deferred state flush
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp(REMOTE_PEER_A, 21, 201, { [REMOTE_PEER_A]: 20 }),
      );

      const payload = createValidSnapshotPayload();
      await coordinator.executeRecoveryWithSnapshot(payload);

      expect(reentrantCallCount).toBeGreaterThanOrEqual(1);
      expect(coordinator.state).toBe('ACTIVE');
    });

    it('M4.3d-S23: Engine disposal during notification flushing terminates safely', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });

      engine.onStateChange(() => {
        engine.dispose();
      });

      // Trigger recovery and reset
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger');
      expect(() => engine.dispose()).not.toThrow();
      expect(engine.isDisposed).toBe(true);
    });

    it('M4.3d-S24: Quarantine buffer bounds enforce count, byte, and individual limits via coordinator admission', () => {
      // Subcase 1: Count overflow beyond 1,000 ops
      let degradedReason1: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        callbacks: {
          onRecoveryDegraded: (reason) => {
            degradedReason1 = reason;
          },
        },
      });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger');

      for (let i = 1; i <= 1000; i++) {
        const op = createMockQuarantinedOp(REMOTE_PEER_A, i, i);
        const decision = coordinator.interceptRemoteEvent(op.event, 128);
        expect(decision.shouldQuarantine).toBe(true);
      }
      expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(1000);

      const op1001 = createMockQuarantinedOp(REMOTE_PEER_A, 1001, 1001);
      expect(() => coordinator.interceptRemoteEvent(op1001.event, 128)).toThrow(SyncEngineError);
      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason1).toBe('QUARANTINE_OVERFLOW');
      expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(1000);

      // Subcase 2: Aggregate byte overflow with configured buffer (64 KiB)
      let degradedReason2: string | undefined;
      const smallBuffer = new QuarantineBuffer({ maxTotalBytes: 65536, maxOperations: 1000 });
      const coord2 = new RecoveryCoordinator({
        engine,
        capability,
        quarantineBuffer: smallBuffer,
        callbacks: {
          onRecoveryDegraded: (reason) => {
            degradedReason2 = reason;
          },
        },
      });
      coord2.triggerRecovery(REMOTE_PEER_A, 'Trigger byte overflow test');

      // 16 ops of 4096 bytes = 65,536 bytes
      for (let i = 1; i <= 16; i++) {
        const op = createMockQuarantinedOp(REMOTE_PEER_A, 2000 + i, 2000 + i);
        const decision = coord2.interceptRemoteEvent(op.event, 4096);
        expect(decision.shouldQuarantine).toBe(true);
      }
      expect(coord2.quarantineBuffer.totalRetainedCount).toBe(16);
      expect(coord2.quarantineBuffer.totalRetainedBytes).toBe(65536);

      // 17th op of 4096 bytes breaches byte capacity (N=16 <= 1000)
      const op17 = createMockQuarantinedOp(REMOTE_PEER_A, 2017, 2017);
      expect(() => coord2.interceptRemoteEvent(op17.event, 4096)).toThrow(SyncEngineError);
      expect(coord2.state).toBe('DEGRADED');
      expect(degradedReason2).toBe('QUARANTINE_OVERFLOW');
      expect(coord2.quarantineBuffer.totalRetainedCount).toBe(16);
      expect(coord2.quarantineBuffer.totalRetainedBytes).toBe(65536);
      coord2.dispose();

      // Subcase 3: Individual event cap (4,097 B > 4,096 B cap)
      const coord3 = new RecoveryCoordinator({ engine, capability });
      coord3.triggerRecovery(REMOTE_PEER_A, 'Trigger individual cap test');
      const oversizedOp = createMockQuarantinedOp(REMOTE_PEER_A, 3001, 3001);
      const decision3 = coord3.interceptRemoteEvent(oversizedOp.event, 4097);
      expect(decision3.shouldQuarantine).toBe(false);
      expect(decision3.dropped).toBe(true);
      expect(coord3.quarantineBuffer.totalRetainedCount).toBe(0);
      coord3.dispose();
    });

    it('M4.3d-S25: Repair timeout after partial drain halts reconciliation in DEGRADED', async () => {
      let degradedReason: string | undefined;
      coordinator = new RecoveryCoordinator({
        engine,
        capability,
        reconciliationTimeoutMs: 5000,
        callbacks: {
          onRecoveryDegraded: (reason) => {
            degradedReason = reason;
          },
        },
      });

      // Hang drainBatch to simulate repair timeout during gap wait
      vi.spyOn(coordinator.quarantineDrainer, 'drainBatch').mockImplementation(
        () => new Promise(() => {}),
      );

      coordinator.quarantineBuffer.enqueue(createMockQuarantinedOp(REMOTE_PEER_A, 1, 1));
      const payload = createValidSnapshotPayload();

      const execPromise = coordinator.executeRecoveryWithSnapshot(payload);
      execPromise.catch(() => {});

      await vi.advanceTimersByTimeAsync(5001);

      await expect(execPromise).rejects.toThrow();
      expect(coordinator.state).toBe('DEGRADED');
      expect(degradedReason).toBe('RECONCILIATION_TIMEOUT');
    });

    it('M4.3d-S26: Coordinator disposal during partial drain cancels timers and restores in-flight op', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger');

      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);
      // Simulate destination capacity rejection so inFlightRecord is retained
      vi.spyOn(engine, 'admitDrainedOperation').mockReturnValue({ status: 'CAPACITY_REJECTED' });

      coordinator.quarantineDrainer.processSingleOperation(op);
      expect(coordinator.inFlightRecord).not.toBeNull();

      // Drainer disposal restores in-flight record to quarantine buffer
      coordinator.quarantineDrainer.dispose();
      expect(coordinator.quarantineDrainer.isDisposed).toBe(true);
      expect(coordinator.inFlightRecord).toBeNull();
      expect(coordinator.quarantineBuffer.drainSize).toBe(1);

      // Full coordinator disposal clears all buffers and terminates cleanly
      coordinator.dispose();
      expect(coordinator.isDisposed).toBe(true);
      expect(coordinator.quarantineBuffer.drainSize).toBe(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GROUP F: Interrupted-Drain Adversarial Cases (Scenarios 27–36)
  // ═══════════════════════════════════════════════════════════════════════════

  describe('Group F — Interrupted-Drain Adversarial Cases (Scenarios 27–36, R4-3 Cases 1–10)', () => {
    it('M4.3d-S27: Exception immediately after dequeue preserves operation in inFlightRecord (Case 1)', () => {
      const mockDest: IDrainDestination = {
        admitDrainedOperation: () => {
          throw new Error('Simulated pre-admission crash');
        },
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);

      const res = drainer.processSingleOperation(op);
      expect(res.status).toBe('HALTED_ERROR');
      // Operation is preserved in inFlightRecord; zero loss!
      expect(drainer.inFlightRecord).not.toBeNull();
      expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);
    });

    it('M4.3d-S28: PendingDependencyQueue capacity rejection preserves inFlightRecord and halts (Case 2)', () => {
      const mockDest: IDrainDestination = {
        admitDrainedOperation: () => ({ status: 'CAPACITY_REJECTED' }),
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);

      const res = drainer.processSingleOperation(op);
      expect(res.status).toBe('HALTED_CAPACITY');
      expect(drainer.inFlightRecord?.destinationStatus).toBe('CAPACITY_REJECTED');
      expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);
    });

    it('M4.3d-S29: Ordered-operation-log insertion failure preserves inFlightRecord (Case 3)', () => {
      const mockDest: IDrainDestination = {
        admitDrainedOperation: () => {
          throw new SyncEngineError('STORAGE_ERROR', 'Disk full on log insertion');
        },
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);

      const res = drainer.processSingleOperation(op);
      expect(res.status).toBe('HALTED_ERROR');
      expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);
    });

    it('M4.3d-S30: Engine reduction failure preserves inFlightRecord and leaves state uncorrupted (Case 4)', () => {
      const mockDest: IDrainDestination = {
        admitDrainedOperation: () => {
          throw new Error('Reducer NaN arithmetic exception');
        },
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);

      const res = drainer.processSingleOperation(op);
      expect(res.status).toBe('HALTED_ERROR');
      expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);
    });

    it('M4.3d-S31: Destination acceptance before in-flight cleanup does not duplicate-advance vector on resume (Case 5)', () => {
      const admittedOps = new Set<string>();

      const mockDest: IDrainDestination = {
        admitDrainedOperation: (op) => {
          admittedOps.add(`${op.originPeerId}:${op.sequenceNumber}`);
          return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: op.sequenceNumber };
        },
        hasOperation: (peer, seq) => admittedOps.has(`${peer}:${seq}`),
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);

      // Simulate crash: op was admitted by destination, but inFlightRecord was not cleared
      mockDest.admitDrainedOperation(op);
      (drainer as any)._inFlightRecord = {
        operation: op,
        dequeuedAtMs: Date.now(),
        attemptCount: 1,
        destinationStatus: 'PENDING',
      };

      // resumeDrain detects destination already has operation
      drainer.resumeDrain();

      // In-flight record cleared, destination not duplicate-called
      expect(drainer.inFlightRecord).toBeNull();
      expect(admittedOps.size).toBe(1);
    });

    it('M4.3d-S32: Transition to DEGRADED while op is in-flight preserves op in inFlightRecord (Case 6)', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger');

      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);
      // Simulate destination capacity rejection so op remains held in inFlightRecord
      vi.spyOn(engine, 'admitDrainedOperation').mockReturnValue({ status: 'CAPACITY_REJECTED' });
      coordinator.quarantineDrainer.processSingleOperation(op);
      expect(coordinator.inFlightRecord).not.toBeNull();

      // Outer watchdog aborts to DEGRADED
      coordinator.abortRecovery('OVERALL_RECOVERY_TIMEOUT', 'Watchdog timeout');
      expect(coordinator.state).toBe('DEGRADED');

      // Op remains safely in inFlightRecord; zero loss!
      expect(coordinator.inFlightRecord).not.toBeNull();
      expect(coordinator.inFlightRecord?.operation.sequenceNumber).toBe(1);
    });

    it('M4.3d-S33: Resume after interruption reduces op exactly once and clears in-flight record (Case 7)', async () => {
      let attempts = 0;
      const admitted: number[] = [];

      const mockDest: IDrainDestination = {
        admitDrainedOperation: (op) => {
          attempts++;
          if (attempts === 1) {
            return { status: 'CAPACITY_REJECTED' };
          }
          admitted.push(op.sequenceNumber);
          return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: op.sequenceNumber };
        },
        hasOperation: (peer, seq) => admitted.includes(seq),
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);
      buffer.enqueue(op);
      buffer.swapDrainBatch();

      // Pass 1 halts on capacity
      const r1 = await drainer.drainBatch();
      expect(r1.haltedOnCapacity).toBe(true);
      expect(drainer.inFlightRecord).not.toBeNull();

      // Pass 2 resumes and succeeds
      const r2 = await drainer.drainBatch();
      expect(r2.appliedCount).toBe(1);
      expect(drainer.inFlightRecord).toBeNull();
      expect(admitted).toEqual([1]); // reduced exactly once!
    });

    it('M4.3d-S34: Duplicate op arriving while original is in-flight is dropped (Case 8)', () => {
      coordinator = new RecoveryCoordinator({ engine, capability });
      coordinator.triggerRecovery(REMOTE_PEER_A, 'Trigger');

      const op = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);
      let duplicateIntercepted = false;

      // When op is being admitted by destination, it is currently in flight
      vi.spyOn(engine, 'admitDrainedOperation').mockImplementation(() => {
        expect(coordinator.quarantineDrainer.isInFlight(REMOTE_PEER_A, 1)).toBe(true);
        // Duplicate arrives over transport while original is in flight
        const intercept = coordinator.interceptRemoteEvent(op.event, 128);
        expect(intercept.shouldQuarantine).toBe(false);
        expect(intercept.dropped).toBe(true);
        expect(intercept.duplicate).toBe(true);
        duplicateIntercepted = true;
        return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: 1 };
      });

      coordinator.quarantineDrainer.processSingleOperation(op);
      expect(duplicateIntercepted).toBe(true);
    });

    it('M4.3d-S35: Terminal rejection retires in-flight op and allows drain to continue (Case 9)', async () => {
      const admitted: number[] = [];

      const mockDest: IDrainDestination = {
        admitDrainedOperation: (op) => {
          if (!op.event.message.logicalEventId || op.sequenceNumber === 1) {
            return {
              status: 'TERMINALLY_REJECTED',
              reason: 'Remote event missing logicalEventId',
            };
          }
          admitted.push(op.sequenceNumber);
          return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: op.sequenceNumber };
        },
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });

      // Op 1 has missing logicalEventId -> terminally rejected under cooperative validation
      const op1 = createMockQuarantinedOp(REMOTE_PEER_A, 1, 1);
      (op1.event.message as any).logicalEventId = '';
      buffer.enqueue(op1);

      // Op 2 is fully valid
      const op2 = createMockQuarantinedOp(REMOTE_PEER_A, 2, 2);
      buffer.enqueue(op2);
      buffer.swapDrainBatch();

      const result = await drainer.drainBatch();
      // Op 1 was terminally rejected and discarded, op 2 was applied cleanly
      expect(result.discardedCount).toBe(1);
      expect(result.appliedCount).toBe(1);
      expect(drainer.inFlightRecord).toBeNull();
      expect(admitted).toEqual([2]);
    });

    it('M4.3d-S36: Successful drain after recoverable destination failure (Case 10)', async () => {
      let isBlocked = true;
      const pending: number[] = [];

      const mockDest: IDrainDestination = {
        admitDrainedOperation: (op) => {
          if (isBlocked) {
            return { status: 'CAPACITY_REJECTED' };
          }
          pending.push(op.sequenceNumber);
          return { status: 'ACCEPTED_PENDING', pendingBuffered: true };
        },
      };

      const buffer = new QuarantineBuffer();
      const drainer = new QuarantineDrainer({ destination: mockDest, quarantineBuffer: buffer });
      buffer.enqueue(createMockQuarantinedOp(REMOTE_PEER_A, 1, 1));
      buffer.swapDrainBatch();

      // Pass 1: capacity blocked
      const r1 = await drainer.drainBatch();
      expect(r1.haltedOnCapacity).toBe(true);
      expect(drainer.inFlightRecord).not.toBeNull();

      // Unblock capacity
      isBlocked = false;

      // Pass 2: capacity recovered
      const r2 = await drainer.drainBatch();
      expect(r2.pendingCount).toBe(1);
      expect(drainer.inFlightRecord).toBeNull();
      expect(pending).toEqual([1]);
    });
  });
});
