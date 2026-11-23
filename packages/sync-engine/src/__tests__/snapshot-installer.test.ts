import { describe, it, expect } from 'vitest';
import {
  SnapshotInstaller,
  evaluateSnapshotEligibility,
  LeaderlessSyncEngine,
  computeSnapshotDigestV2,
  createRecoveryInstallationCapability,
  type StateSnapshotPayloadV2,
  type PlaybackState,
} from '../index.js';

describe('Phase 7.5 M4.3b — SnapshotInstaller & Eligibility', () => {
  const baseConfig = {
    sessionId: 'session-m43b-install',
    localPeerId: 'peer-local',
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 128,
  };

  const createValidPayload = (overrides?: Partial<StateSnapshotPayloadV2>): StateSnapshotPayloadV2 => {
    const preimage: Omit<StateSnapshotPayloadV2, 'sha256Digest'> = {
      formatVersion: 2,
      snapshotId: 'snap-test-100',
      sessionId: 'session-m43b-install',
      membershipEpoch: 1,
      authorPeerId: 'peer-author',
      state: {
        formatVersion: 1,
        sessionId: 'session-m43b-install',
        status: 'paused',
        anchorPositionMs: 3000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-author': 10 },
      originFrontierTimestamps: { 'peer-author': 100 },
      maxLogicalTimestamp: 100,
      compactedCheckpoints: [
        {
          originPeerId: 'peer-author',
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

  describe('Eligibility Evaluation (M4.3-R2 / M4.3-R4)', () => {
    it('C1: classifies identical base frontiers (k <= 1) as IDENTICAL', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 1 },
        originFrontierTimestamps: { 'peer-author': 10 },
        maxLogicalTimestamp: 10,
        compactedCheckpoints: [],
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 1 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('IDENTICAL');
      expect(result.eligible).toBe(false);
    });

    it('C2: classifies equal frontiers at seq > 1 without full checkpoint proof as FRONTIER_AGREEMENT_ONLY', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 10 },
        originFrontierTimestamps: { 'peer-author': 100 },
        compactedCheckpoints: [], // No intermediate checkpoints
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 10 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('FRONTIER_AGREEMENT_ONLY');
      expect(result.eligible).toBe(false);
    });

    it('C3: classifies candidate with remote >= local on all origins and > on at least one as STRICTLY_DOMINATES', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 15, 'peer-local': 5 },
        originFrontierTimestamps: { 'peer-author': 150, 'peer-local': 50 },
        maxLogicalTimestamp: 150,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 10, 'peer-local': 5 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('STRICTLY_DOMINATES');
      expect(result.eligible).toBe(true);
    });

    it('C4: classifies concurrent divergent frontiers as INCOMPARABLE (prohibits arbitrary tie-breaking)', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 20, 'peer-local': 2 },
        originFrontierTimestamps: { 'peer-author': 200, 'peer-local': 20 },
        maxLogicalTimestamp: 200,
      });

      // Remote has peer-author:20 > 10, but local has peer-local:5 > 2
      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 10, 'peer-local': 5 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('INCOMPARABLE');
      expect(result.eligible).toBe(false);
    });

    it('C5: classifies candidate behind local as DOMINATED', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 5 },
        originFrontierTimestamps: { 'peer-author': 50 },
        maxLogicalTimestamp: 50,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 10 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('DOMINATED');
      expect(result.eligible).toBe(false);
    });

    it('C6: classifies older epoch snapshot as DOMINATED', () => {
      const payload = createValidPayload({
        membershipEpoch: 1,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 5 },
        currentEpoch: 2,
      });

      expect(result.eligibility).toBe('DOMINATED');
      expect(result.eligible).toBe(false);
    });

    it('C7: classifies future epoch snapshot as FUTURE', () => {
      const payload = createValidPayload({
        membershipEpoch: 3,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 5 },
        currentEpoch: 1,
      });

      expect(result.eligibility).toBe('FUTURE');
      expect(result.eligible).toBe(false);
    });

    it('C8: classifies small missing gap within active log capacity as REPAIRABLE_WITHIN_HORIZON when requested', () => {
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 15 },
        originFrontierTimestamps: { 'peer-author': 150 },
        maxLogicalTimestamp: 150,
      });

      const result = evaluateSnapshotEligibility({
        candidateSnapshot: payload,
        currentReplicationVector: { 'peer-author': 10 }, // gap is 5 <= 128
        currentEpoch: 1,
        logCapacity: 128,
        checkRepairableHorizon: true,
      });

      expect(result.eligibility).toBe('REPAIRABLE_WITHIN_HORIZON');
      expect(result.eligible).toBe(false);
    });
  });

  describe('Stage 1 — Out-Of-Band Validation', () => {
    it('B1: validates a well-formed dominating snapshot payload successfully', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 20 },
        originFrontierTimestamps: { 'peer-author': 200 },
        maxLogicalTimestamp: 200,
        compactedCheckpoints: [
          { originPeerId: 'peer-author', sequenceNumber: 20, logicalTimestamp: 200 },
        ],
      });

      const validation = await installer.validate(
        payload,
        1,
        { 'peer-author': 5 },
      );

      expect(validation.valid).toBe(true);
      expect(validation.computedDigest).toBe(payload.sha256Digest);
      expect(validation.eligibility?.eligibility).toBe('STRICTLY_DOMINATES');
    });

    it('B2: rejects malformed JSON string payload', async () => {
      const installer = new SnapshotInstaller();
      const validation = await installer.validate(
        '{ not valid json ...',
        1,
        {},
      );

      expect(validation.valid).toBe(false);
      expect(validation.errorReason).toMatch(/JSON parsing failed/);
    });

    it('B3: rejects payload with session ID mismatch', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        sessionId: 'session-other',
      });

      const validation = await installer.validate(
        payload,
        1,
        {},
        undefined,
        { expectedSessionId: 'session-m43b-install' },
      );

      expect(validation.valid).toBe(false);
      expect(validation.errorReason).toMatch(/sessionId/);
    });

    it('B4: rejects payload with digest mismatch', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        sha256Digest: 'a'.repeat(64),
      });

      const validation = await installer.validate(
        payload,
        1,
        {},
      );

      expect(validation.valid).toBe(false);
      expect(validation.errorReason).toMatch(/Digest mismatch/);
    });

    it('B5: rejects payload with missing origin frontier timestamp', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-missing-ts': 10 },
        originFrontierTimestamps: {},
        maxLogicalTimestamp: 0,
      });

      const validation = await installer.validate(
        payload,
        1,
        {},
      );

      expect(validation.valid).toBe(false);
      expect(validation.errorReason).toMatch(/origin frontier timestamp/);
    });

    it('B6: rejects payload with unsorted compacted checkpoints', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-a': 10, 'peer-b': 10 },
        originFrontierTimestamps: { 'peer-a': 100, 'peer-b': 100 },
        maxLogicalTimestamp: 100,
        compactedCheckpoints: [
          { originPeerId: 'peer-b', sequenceNumber: 10, logicalTimestamp: 100 },
          { originPeerId: 'peer-a', sequenceNumber: 10, logicalTimestamp: 100 }, // Out of order: 'peer-a' < 'peer-b'
        ],
      });

      const validation = await installer.validate(
        payload,
        1,
        {},
      );

      expect(validation.valid).toBe(false);
      expect(validation.errorReason).toMatch(/not sorted deterministically/);
    });
  });

  describe('Stage 2 — Staging Candidate Core State & Monotonicity Preservation', () => {
    it('B7: pre-constructs candidate in private storage and returns opaque, frozen handle', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 25 },
        originFrontierTimestamps: { 'peer-author': 250 },
        maxLogicalTimestamp: 250,
        compactedCheckpoints: [
          { originPeerId: 'peer-author', sequenceNumber: 25, logicalTimestamp: 250 },
        ],
      });

      const validation = await installer.validate(payload, 1, { 'peer-author': 5 });
      expect(validation.valid).toBe(true);

      const staged = installer.stage(
        validation,
        (engine as any).config,
        {
          currentLogicalClock: 0,
          currentLocalOriginSequence: 0,
          now: 12345,
          reducerFn: engine.getReducerFunction(),
        },
      );

      // Verify staged handle is opaque and frozen
      expect(staged.handleId).toBeDefined();
      expect(staged.targetEpoch).toBe(1);
      expect(staged.targetFrontier['peer-author']).toBe(25);
      expect(staged.maxLogicalTimestamp).toBe(250);
      expect(Object.isFrozen(staged)).toBe(true);
      expect(Object.isFrozen(staged.targetFrontier)).toBe(true);

      // Candidate core is NOT exposed on handle
      expect((staged as any).candidateCore).toBeUndefined();

      // Zero engine mutation occurred during staging
      expect(engine.logicalClock).toBe(0);
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    });

    it('B8: throws INVALID_ARGUMENT when staging an invalid validation result', () => {
      const installer = new SnapshotInstaller();
      const engine = new LeaderlessSyncEngine(baseConfig);

      expect(() => {
        installer.stage(
          { valid: false, errorReason: 'Corrupted payload' },
          (engine as any).config,
        );
      }).toThrow(/Cannot stage invalid snapshot validation result/);
    });

    it('B9: mutations on source snapshot payload or handle targetFrontier do not affect candidate', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();

      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 30 },
        originFrontierTimestamps: { 'peer-author': 300 },
        maxLogicalTimestamp: 300,
      });

      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config, {
        currentLogicalClock: 0,
        currentLocalOriginSequence: 0,
        now: 1000,
        reducerFn: engine.getReducerFunction(),
      });

      // Attempt mutating the source payload
      payload.snapshotFrontier['peer-author'] = 9999;
      payload.state.anchorPositionMs = 8888;

      // Handle target frontier remains unmutated
      expect(staged.targetFrontier['peer-author']).toBe(30);

      // Attempt mutating staged.targetFrontier throws because it is Object.freeze'd
      expect(() => {
        (staged.targetFrontier as any)['peer-author'] = 7777;
      }).toThrow();
    });

    it('C10: preserves Lamport clock monotonicity: max(L_local, maxLogicalTimestamp)', async () => {
      const installer = new SnapshotInstaller();
      const capability = createRecoveryInstallationCapability('test', baseConfig.sessionId);
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 20 },
        originFrontierTimestamps: { 'peer-author': 100 },
        maxLogicalTimestamp: 100,
      });

      const validation = await installer.validate(payload, 1, {});

      // Case 1: Local clock 500 > Snapshot clock 100 -> candidate must be 500 (NOT regressed to 100!)
      const engineA = new LeaderlessSyncEngine({
        ...baseConfig,
        initialClock: 500,
        recoveryInstallationCapability: capability,
      });
      const stagedA = installer.stage(validation, (engineA as any).config, {
        currentLogicalClock: engineA.logicalClock,
        currentLocalOriginSequence: 0,
      });
      installer.commitAtomic(engineA, stagedA, capability);
      expect(engineA.logicalClock).toBe(500);

      // Case 2: Local clock 50 < Snapshot clock 100 -> candidate advances to 100
      const engineB = new LeaderlessSyncEngine({
        ...baseConfig,
        initialClock: 50,
        recoveryInstallationCapability: capability,
      });
      const stagedB = installer.stage(validation, (engineB as any).config, {
        currentLogicalClock: engineB.logicalClock,
        currentLocalOriginSequence: 0,
      });
      installer.commitAtomic(engineB, stagedB, capability);
      expect(engineB.logicalClock).toBe(100);

      // Case 3: Equal clocks 100 == 100 -> unchanged 100 (no +1 increment on snapshot install)
      const engineC = new LeaderlessSyncEngine({
        ...baseConfig,
        initialClock: 100,
        recoveryInstallationCapability: capability,
      });
      const stagedC = installer.stage(validation, (engineC as any).config, {
        currentLogicalClock: engineC.logicalClock,
        currentLocalOriginSequence: 0,
      });
      installer.commitAtomic(engineC, stagedC, capability);
      expect(engineC.logicalClock).toBe(100);
    });

    it('C11: validates currentLogicalClock and rejects invalid clock values', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload();
      const validation = await installer.validate(payload, 1, {});

      expect(() => {
        installer.stage(validation, baseConfig as any, {
          currentLogicalClock: -5,
          currentLocalOriginSequence: 0,
        });
      }).toThrow(/currentLogicalClock must be a non-negative integer/);

      expect(() => {
        installer.stage(validation, baseConfig as any, {
          currentLogicalClock: 3.14,
          currentLocalOriginSequence: 0,
        });
      }).toThrow(/currentLogicalClock must be a non-negative integer/);
    });

    it('S1: preserves local origin sequence counter: max(S_local, V_snapshot[localPeerId])', async () => {
      const installer = new SnapshotInstaller();
      const capability = createRecoveryInstallationCapability('test', baseConfig.sessionId);

      // Case 1: Local sequence 15 > snapshot local frontier 10 -> preserves 15
      const payloadA = createValidPayload({
        snapshotFrontier: { 'peer-author': 20, [baseConfig.localPeerId]: 10 },
        originFrontierTimestamps: { 'peer-author': 200, [baseConfig.localPeerId]: 100 },
        maxLogicalTimestamp: 200,
      });
      const validationA = await installer.validate(payloadA, 1, {});
      const engineA = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const stagedA = installer.stage(validationA, (engineA as any).config, {
        currentLogicalClock: 0,
        currentLocalOriginSequence: 15,
      });
      installer.commitAtomic(engineA, stagedA, capability);
      expect(engineA.getDiagnosticCoreSnapshot().localOriginSequence).toBe(15);

      // Case 2: Snapshot contains higher local sequence 20 > current local 10 -> advances to 20
      const payloadB = createValidPayload({
        snapshotFrontier: { 'peer-author': 20, [baseConfig.localPeerId]: 20 },
        originFrontierTimestamps: { 'peer-author': 200, [baseConfig.localPeerId]: 200 },
        maxLogicalTimestamp: 200,
      });
      const validationB = await installer.validate(payloadB, 1, {});
      const engineB = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const stagedB = installer.stage(validationB, (engineB as any).config, {
        currentLogicalClock: 0,
        currentLocalOriginSequence: 10,
      });
      installer.commitAtomic(engineB, stagedB, capability);
      expect(engineB.getDiagnosticCoreSnapshot().localOriginSequence).toBe(20);

      // Case 3: Snapshot does NOT contain local peer -> preserves local sequence 12
      const payloadC = createValidPayload({
        snapshotFrontier: { 'peer-author': 20 },
        originFrontierTimestamps: { 'peer-author': 200 },
        maxLogicalTimestamp: 200,
      });
      const validationC = await installer.validate(payloadC, 1, {});
      const engineC = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const stagedC = installer.stage(validationC, (engineC as any).config, {
        currentLogicalClock: 0,
        currentLocalOriginSequence: 12,
      });
      installer.commitAtomic(engineC, stagedC, capability);
      expect(engineC.getDiagnosticCoreSnapshot().localOriginSequence).toBe(12);
    });

    it('S2: validates currentLocalOriginSequence and rejects negative or non-integer values', async () => {
      const installer = new SnapshotInstaller();
      const payload = createValidPayload();
      const validation = await installer.validate(payload, 1, {});

      expect(() => {
        installer.stage(validation, baseConfig as any, {
          currentLogicalClock: 0,
          currentLocalOriginSequence: -1,
        });
      }).toThrow(/currentLocalOriginSequence must be a non-negative integer/);

      expect(() => {
        installer.stage(validation, baseConfig as any, {
          currentLogicalClock: 0,
          currentLocalOriginSequence: 4.5,
        });
      }).toThrow(/currentLocalOriginSequence must be a non-negative integer/);
    });
  });

  describe('Stage 3 — Capability-Gated Atomic Commit & Handle Safety', () => {
    it('D1: commits installation via single synchronous root replacement with authorized capability', async () => {
      const capability = createRecoveryInstallationCapability('coord', baseConfig.sessionId);
      const engine = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const installer = new SnapshotInstaller();

      const payload = createValidPayload({
        snapshotFrontier: { 'peer-remote': 50 },
        originFrontierTimestamps: { 'peer-remote': 500 },
        maxLogicalTimestamp: 500,
        compactedCheckpoints: [
          { originPeerId: 'peer-remote', sequenceNumber: 50, logicalTimestamp: 500 },
        ],
      });

      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config, {
        currentLogicalClock: 0,
        currentLocalOriginSequence: 0,
        now: 2000,
        reducerFn: engine.getReducerFunction(),
      });

      // Commit with authorized capability
      const report = installer.commitAtomic(engine, staged, capability);

      expect(report.success).toBe(true);
      expect(report.installedEpoch).toBe(1);
      expect(report.installedFrontier['peer-remote']).toBe(50);
      expect(report.installedMaxLogicalTimestamp).toBe(500);
      expect(typeof report.commitDurationMs).toBe('number');

      // Live engine state updated
      expect(engine.logicalClock).toBe(500);
      expect(engine.replicationFrontier['peer-remote']).toBe(50);
      expect(engine.playbackState.anchorPositionMs).toBe(3000);
    });

    it('D2: rejects commit when capability is missing or unauthorized', async () => {
      const authorizedCap = createRecoveryInstallationCapability('coord', baseConfig.sessionId);
      const forgedCap = createRecoveryInstallationCapability('attacker', baseConfig.sessionId);
      const engine = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: authorizedCap,
      });
      const installer = new SnapshotInstaller();

      const payload = createValidPayload();
      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config);

      // Missing capability
      expect(() => {
        installer.commitAtomic(engine, staged, undefined as any);
      }).toThrow(/requires an authorized RecoveryInstallationCapability/);

      // Forged capability
      expect(() => {
        installer.commitAtomic(engine, staged, forgedCap);
      }).toThrow(/requires authorized RecoveryInstallationCapability/);

      // Active engine remains completely untouched
      expect(engine.logicalClock).toBe(0);
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    });

    it('D3: rejects forged staged handle with INVALID_STAGED_HANDLE', () => {
      const capability = createRecoveryInstallationCapability('coord', baseConfig.sessionId);
      const engine = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const installer = new SnapshotInstaller();

      const forgedHandle = Object.freeze({
        handleId: 'forged-handle-999',
        sessionId: baseConfig.sessionId,
        targetEpoch: 1,
        targetFrontier: {},
        canonicalDigest: 'fake',
        maxLogicalTimestamp: 100,
      });

      expect(() => {
        installer.commitAtomic(engine, forgedHandle, capability);
      }).toThrow(/Staged snapshot handle is invalid, forged, or has already been consumed/);
    });

    it('D4: rejects double-commit / reusing an already consumed staged handle', async () => {
      const capability = createRecoveryInstallationCapability('coord', baseConfig.sessionId);
      const engine = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      const installer = new SnapshotInstaller();

      const payload = createValidPayload();
      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config);

      // First commit succeeds
      installer.commitAtomic(engine, staged, capability);

      // Second commit with same handle throws INVALID_STAGED_HANDLE
      expect(() => {
        installer.commitAtomic(engine, staged, capability);
      }).toThrow(/Staged snapshot handle is invalid, forged, or has already been consumed/);
    });

    it('D5: validation failure leaves existing engine root 100% unchanged', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();

      // Incomparable payload
      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 20, 'peer-local': 1 },
        originFrontierTimestamps: { 'peer-author': 200, 'peer-local': 10 },
        maxLogicalTimestamp: 200,
      });

      const validation = await installer.validate(
        payload,
        1,
        { 'peer-author': 10, 'peer-local': 5 }, // Concurrent!
      );

      expect(validation.valid).toBe(false);
      expect(validation.eligibility?.eligibility).toBe('INCOMPARABLE');

      // Engine is completely untouched
      expect(engine.logicalClock).toBe(0);
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    });

    it('D6: post-commit listener exceptions do not roll back or corrupt committed state root', () => {
      const engine = new LeaderlessSyncEngine(baseConfig);

      // Register a failing listener
      engine.onStateChange(() => {
        throw new Error('Explosive observer failure');
      });

      const payload = createValidPayload({
        snapshotFrontier: { 'peer-remote': 60 },
        originFrontierTimestamps: { 'peer-remote': 600 },
        maxLogicalTimestamp: 600,
      });

      // Use importSnapshotV2 which performs atomic swap followed by listener notification
      const decision = engine.importSnapshotV2(payload);

      expect(decision.accepted).toBe(true);
      expect(engine.replicationFrontier['peer-remote']).toBe(60);
      expect(engine.logicalClock).toBe(600);
      expect(engine.playbackState.anchorPositionMs).toBe(3000);
    });

    it('D7: throws ENGINE_DISPOSED when committing to disposed engine', async () => {
      const capability = createRecoveryInstallationCapability('coord', baseConfig.sessionId);
      const engine = new LeaderlessSyncEngine({
        ...baseConfig,
        recoveryInstallationCapability: capability,
      });
      engine.dispose();

      const installer = new SnapshotInstaller();
      const payload = createValidPayload();
      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config);

      expect(() => {
        installer.commitAtomic(engine, staged, capability);
      }).toThrow(/Cannot commit snapshot to a disposed engine/);
    });
  });
});
