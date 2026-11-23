import { describe, it, expect } from 'vitest';
import {
  SnapshotInstaller,
  evaluateSnapshotEligibility,
  LeaderlessSyncEngine,
  computeSnapshotDigestV2,
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

  describe('Stage 2 — Staging Candidate Core State', () => {
    it('B7: pre-constructs sealed candidate EngineCoreState without mutating engine', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const initialRoot = engine.coreState;

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
        12345,
        engine.getReducerFunction(),
      );

      // Verify staged candidate core
      expect(staged.candidateCore).toBeDefined();
      expect(Object.isSealed(staged.candidateCore)).toBe(true);
      expect(staged.candidateCore.clockManager.clock).toBe(250);
      expect(staged.candidateCore.replicationVector['peer-author']).toBe(25);
      expect(staged.candidateCore.playbackState.anchorPositionMs).toBe(3000);
      expect(staged.candidateCore.playbackState.anchorLocalTimestamp).toBe(12345);

      // Verify ZERO engine mutation occurred during staging
      expect(engine.coreState).toBe(initialRoot);
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

    it('B9: caller mutations on source snapshot payload after staging do not affect candidate core', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();

      const payload = createValidPayload({
        snapshotFrontier: { 'peer-author': 30 },
        originFrontierTimestamps: { 'peer-author': 300 },
        maxLogicalTimestamp: 300,
      });

      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config, 1000, engine.getReducerFunction());

      // Attempt mutating the payload
      payload.snapshotFrontier['peer-author'] = 9999;
      payload.state.anchorPositionMs = 8888;

      // Candidate core vectors and state remain encapsulated and unchanged
      expect(staged.candidateCore.replicationVector['peer-author']).toBe(30);
      expect(staged.candidateCore.playbackState.anchorPositionMs).toBe(3000);
    });
  });

  describe('Stage 3 — Atomic Commit Point & Stale Reference Safety', () => {
    it('D1: commits installation via single synchronous root replacement and dereferences creator alias', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();
      const oldRoot = engine.coreState;

      const payload = createValidPayload({
        snapshotFrontier: { 'peer-remote': 50 },
        originFrontierTimestamps: { 'peer-remote': 500 },
        maxLogicalTimestamp: 500,
        compactedCheckpoints: [
          { originPeerId: 'peer-remote', sequenceNumber: 50, logicalTimestamp: 500 },
        ],
      });

      const validation = await installer.validate(payload, 1, {});
      const staged = installer.stage(validation, (engine as any).config, 2000, engine.getReducerFunction());
      const candidateCore = staged.candidateCore;

      // Commit
      const report = installer.commitAtomic(engine, staged);

      expect(report.success).toBe(true);
      expect(report.installedEpoch).toBe(1);
      expect(report.installedFrontier['peer-remote']).toBe(50);
      expect(report.installedMaxLogicalTimestamp).toBe(500);
      expect(typeof report.commitDurationMs).toBe('number');

      // Live engine root is now candidateCore
      expect(engine.coreState).toBe(candidateCore);
      expect(engine.logicalClock).toBe(500);
      expect(engine.replicationFrontier['peer-remote']).toBe(50);

      // Creator alias is explicitly dropped to prevent memory leaks / retained external mutations
      expect(staged.candidateCore).toBeNull();

      // Old root remains completely detached
      expect(oldRoot.clockManager.clock).toBe(0);
      expect(oldRoot.replicationVector['peer-remote']).toBeUndefined();
    });

    it('D2: validation failure leaves existing engine root 100% unchanged', async () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const initialRoot = engine.coreState;
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

      // Engine root is completely untouched
      expect(engine.coreState).toBe(initialRoot);
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    });

    it('D3: post-commit listener exceptions do not roll back or corrupt committed state root', () => {
      const engine = new LeaderlessSyncEngine(baseConfig);
      const installer = new SnapshotInstaller();

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
  });
});
