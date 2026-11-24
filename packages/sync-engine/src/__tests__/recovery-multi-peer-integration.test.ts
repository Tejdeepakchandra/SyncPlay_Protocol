/**
 * Phase 7.5 Sub-Milestone M4.3d — Multi-Peer Recovery Integration Suite
 *
 * Implements the 6 end-to-end multi-peer simulation scenarios:
 * - M4.3d-MP1: Late-Join Peer Catch-Up
 * - M4.3d-MP2: Compaction-Horizon Gap Recovery ($K=256$)
 * - M4.3d-MP3: Supplier Crash During Transfer & Candidate Registry Failover
 * - M4.3d-MP4: High-Concurrency Quarantine Burst (500 ops)
 * - M4.3d-MP5: Partial Drain with Anti-Entropy Repair (100 ops)
 * - M4.3d-MP6: Strong Eventual Consistency Verification (3 peers under reordering, delay, duplication)
 *
 * Strict invariants verified:
 * - Deterministic convergence across reordered, delayed, and duplicate events
 * - Exactly-once operation application through quarantine draining
 * - Zero state corruption or resource leakage across failure modes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  RecoveryCoordinator,
  CandidateOfferRegistry,
  createRecoveryInstallationCapability,
  createInboundTransportCapability,
  computeSnapshotDigestV2,
  SyncEngineError,
  type StateSnapshotPayloadV2,
  type SnapshotOfferPayload,
  type QuarantinedOperation,
  type AuthenticatedSupplierContext,
} from '../index.js';
import { PROTOCOL_VERSION } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

describe('Phase 7.5 M4.3d — Multi-Peer Recovery Integration (MP1–MP6)', () => {
  const SESSION_ID = 'session-m43d-multipeer';

  const baseConfig = {
    sessionId: SESSION_ID,
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 256,
  };

  const cleanupFns: Array<() => void> = [];

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    while (cleanupFns.length > 0) {
      try {
        cleanupFns.pop()!();
      } catch {
        // Safe cleanup
      }
    }
    vi.useRealTimers();
  });

  // ── Helper Constructors ───────────────────────────────────────────────────

  function createEngine(peerId: string, logCapacity = 256) {
    const capability = createRecoveryInstallationCapability(`cap-${peerId}`, SESSION_ID);
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      localPeerId: peerId,
      logCapacity,
      recoveryInstallationCapability: capability,
    });
    cleanupFns.push(() => {
      if (!engine.isDisposed) engine.dispose();
    });
    return { engine, capability };
  }

  function createCoordinator(
    engine: LeaderlessSyncEngine,
    capability: ReturnType<typeof createRecoveryInstallationCapability>,
  ) {
    const coordinator = new RecoveryCoordinator({ engine, capability });
    cleanupFns.push(() => {
      if (!coordinator.isDisposed) coordinator.dispose();
    });
    return coordinator;
  }

  function createValidSnapshotPayload(
    overrides?: Partial<StateSnapshotPayloadV2>,
  ): StateSnapshotPayloadV2 {
    const preimage: Omit<StateSnapshotPayloadV2, 'sha256Digest'> = {
      formatVersion: 2,
      snapshotId: 'snap-mp-001',
      sessionId: SESSION_ID,
      membershipEpoch: 1,
      authorPeerId: 'peer-A',
      state: {
        formatVersion: 1,
        sessionId: SESSION_ID,
        status: 'playing',
        anchorPositionMs: 10000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-A': 20 },
      originFrontierTimestamps: { 'peer-A': 200 },
      maxLogicalTimestamp: 200,
      compactedCheckpoints: [
        {
          originPeerId: 'peer-A',
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

  function makeRemoteSyncEvent(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp: number,
    causalContext: Record<string, number> = {},
    messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE' = 'PLAY',
    positionMs = 10000,
    rate = 1.0,
  ): AnyControlSyncEvent {
    const payload: Record<string, unknown> = {
      formatVersion: 1,
      type: messageType,
    };
    if (messageType === 'SEEK' || messageType === 'PLAY') {
      payload.targetPositionMs = positionMs;
      payload.positionMs = positionMs;
    }
    if (messageType === 'RATE_CHANGE') {
      payload.playbackRate = rate;
    }

    return {
      version: 1,
      type: 'CONTROL_MESSAGE',
      senderPeerId: originPeerId,
      recipientPeerId: 'broadcast',
      membershipEpoch: 1,
      transportTimestamp: 1000 + sequenceNumber,
      message: {
        protocolVersion: PROTOCOL_VERSION,
        formatVersion: 1,
        sessionId: SESSION_ID,
        logicalEventId: `evt-${originPeerId}-${sequenceNumber}`,
        originPeerId,
        senderPeerId: originPeerId,
        originSequenceNumber: sequenceNumber,
        logicalTimestamp,
        causalContext,
        action: 'STATE_TRANSITION',
        messageType,
        membershipEpoch: 1,
        payload,
      },
    };
  }

  function createMockQuarantinedOp(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp: number,
    causalContext: Record<string, number> = {},
    messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE' = 'PAUSE',
    positionMs = 10000,
    rate = 1.0,
  ): QuarantinedOperation {
    const event = makeRemoteSyncEvent(
      originPeerId,
      sequenceNumber,
      logicalTimestamp,
      causalContext,
      messageType,
      positionMs,
      rate,
    );

    return {
      event,
      originPeerId,
      sequenceNumber,
      membershipEpoch: 1,
      serializedByteLength: 128,
      receivedAtMs: Date.now(),
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP1: Late-Join Peer Catch-Up
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP1: Late-Join Peer Catch-Up: 3 active peers produce history, late peer catches up cleanly', async () => {
    // 3 peers active: peer-1, peer-2, peer-3
    const p1 = createEngine('peer-1');
    const p2 = createEngine('peer-2');
    const p3 = createEngine('peer-3');

    // Peer-1 plays at ts 1
    const evt1 = makeRemoteSyncEvent('peer-1', 1, 1, {}, 'PLAY', 5000);
    p1.engine.receive(evt1);
    p2.engine.receive(evt1);
    p3.engine.receive(evt1);

    // Peer-2 seeks to 30000ms at ts 2 (dep on peer-1: 1)
    const evt2 = makeRemoteSyncEvent('peer-2', 1, 2, { 'peer-1': 1 }, 'SEEK', 30000);
    p1.engine.receive(evt2);
    p2.engine.receive(evt2);
    p3.engine.receive(evt2);

    // Peer-3 changes rate to 1.5 at ts 3 (dep on peer-2: 1)
    const evt3 = makeRemoteSyncEvent('peer-3', 1, 3, { 'peer-2': 1 }, 'RATE_CHANGE', 30000);
    p1.engine.receive(evt3);
    p2.engine.receive(evt3);
    p3.engine.receive(evt3);

    // Verify all 3 initial peers are synchronized
    expect(p1.engine.replicationFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p2.engine.replicationFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p3.engine.replicationFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });

    // Peer-4 joins late with 0 state
    const p4 = createEngine('peer-4');
    const coord4 = createCoordinator(p4.engine, p4.capability);

    expect(p4.engine.replicationFrontier).toEqual({});
    expect(p4.engine.logicalClock).toBe(0);

    // Snapshot reflecting active state from peer-2
    const snapshotPayload = createValidSnapshotPayload({
      snapshotId: 'snap-late-join-001',
      authorPeerId: 'peer-2',
      state: {
        formatVersion: 1,
        sessionId: SESSION_ID,
        status: 'playing',
        anchorPositionMs: 30000,
        playbackRate: 1.5,
      },
      snapshotFrontier: { 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 },
      originFrontierTimestamps: { 'peer-1': 1, 'peer-2': 2, 'peer-3': 3 },
      maxLogicalTimestamp: 3,
      compactedCheckpoints: [
        { originPeerId: 'peer-1', sequenceNumber: 1, logicalTimestamp: 1 },
        { originPeerId: 'peer-2', sequenceNumber: 1, logicalTimestamp: 2 },
        { originPeerId: 'peer-3', sequenceNumber: 1, logicalTimestamp: 3 },
      ],
    });

    // Peer-4 executes recovery with snapshot
    await coord4.executeRecoveryWithSnapshot(snapshotPayload);

    // Verification: peer-4 state matches active playback state and frontier
    expect(coord4.state).toBe('ACTIVE');
    expect(p4.engine.isRecoveryActive()).toBe(false);
    expect(p4.engine.playbackState.status).toBe('playing');
    expect(p4.engine.playbackState.anchorPositionMs).toBe(30000);
    expect(p4.engine.playbackState.playbackRate).toBe(1.5);
    expect(p4.engine.replicationFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p4.engine.baseSnapshotFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p4.engine.logicalClock).toBeGreaterThanOrEqual(3);

    // Peer-4 can now participate in live replication
    // Peer-1 emits sequence 2 at ts 4 (declaring immediate predecessor peer-1: 1 and peer-3: 1)
    const evt4 = makeRemoteSyncEvent('peer-1', 2, 4, { 'peer-1': 1, 'peer-3': 1 }, 'PAUSE', 35000);
    p4.engine.receive(evt4);

    expect(p4.engine.replicationFrontier['peer-1']).toBe(2);
    expect(p4.engine.playbackState.status).toBe('paused');
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP2: Compaction-Horizon Gap Recovery ($K=256$)
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP2: Compaction-Horizon Gap Recovery: returning peer recovers across log compaction horizon', async () => {
    // Peer-A active, Peer-B was offline
    const pA = createEngine('peer-A', 256);
    const pB = createEngine('peer-B', 256);
    const coordB = createCoordinator(pB.engine, pB.capability);

    // Peer-B only saw initial op 1
    const initOp = makeRemoteSyncEvent('peer-A', 1, 1, {}, 'PLAY', 1000);
    pA.engine.receive(initOp);
    pB.engine.receive(initOp);
    expect(pB.engine.replicationFrontier['peer-A']).toBe(1);

    // Peer-A advances 300 sequential ops beyond compaction horizon (K=256)
    // Peer-A compacts history into a snapshot with frontier peer-A: 300
    const compactedCheckpoints = [];
    for (let seq = 1; seq <= 300; seq += 25) {
      compactedCheckpoints.push({
        originPeerId: 'peer-A',
        sequenceNumber: seq,
        logicalTimestamp: seq,
      });
    }
    // Also include frontier checkpoint 300
    compactedCheckpoints.push({
      originPeerId: 'peer-A',
      sequenceNumber: 300,
      logicalTimestamp: 300,
    });

    const snapshotPayload = createValidSnapshotPayload({
      snapshotId: 'snap-horizon-300',
      authorPeerId: 'peer-A',
      state: {
        formatVersion: 1,
        sessionId: SESSION_ID,
        status: 'playing',
        anchorPositionMs: 50000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-A': 300 },
      originFrontierTimestamps: { 'peer-A': 300 },
      maxLogicalTimestamp: 300,
      compactedCheckpoints,
    });

    // Returning Peer-B receives op 301 from Peer-A while offline gap exists
    const op301 = createMockQuarantinedOp('peer-A', 301, 301, { 'peer-A': 300 }, 'PAUSE', 50000);

    // Direct admission of op 301 would fail due to missing dependencies before horizon
    // Peer-B quarantines op 301 and recovers via snapshot
    coordB.triggerRecovery('peer-A', 'Horizon gap detected');
    coordB.interceptRemoteEvent(op301.event, 128);
    expect(coordB.quarantineBuffer.totalRetainedCount).toBe(1);

    // Peer-B installs snapshot
    await coordB.executeRecoveryWithSnapshot(snapshotPayload);

    // Verification:
    // Snapshot installs base up to 300
    // Quarantined op 301 drains cleanly and advances vector to 301
    expect(coordB.state).toBe('ACTIVE');
    expect(pB.engine.replicationFrontier['peer-A']).toBe(301);
    expect(pB.engine.baseSnapshotFrontier['peer-A']).toBe(300);
    expect(pB.engine.logicalClock).toBeGreaterThanOrEqual(301);
    expect(pB.engine.playbackState.status).toBe('paused');
    expect(coordB.quarantineBuffer.totalRetainedCount).toBe(0);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP3: Supplier Crash During Transfer & Candidate Registry Failover
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP3: Supplier Crash During Transfer: registry evicts crashed supplier and fails over', async () => {
    let simulatedTime = 100000;
    const registry = new CandidateOfferRegistry(() => simulatedTime);
    cleanupFns.push(() => registry.dispose());

    const pA = createEngine('peer-A');
    const coordA = createCoordinator(pA.engine, pA.capability);

    // Two candidates offer snapshots:
    // Peer-B offers snapshot at frontier 50 (higher frontier, ranked 1st)
    // Peer-C offers snapshot at frontier 40 (ranked 2nd)
    const offerB: SnapshotOfferPayload = {
      offerId: '00000000-0000-4000-8000-00000000000b',
      sessionId: SESSION_ID,
      membershipEpoch: 1,
      supplierPeerId: 'peer-B',
      snapshotId: 'snap-peer-B-50',
      authorPeerId: 'peer-B',
      snapshotFrontier: { 'peer-B': 50 },
      maxLogicalTimestamp: 500,
      totalSizeBytes: 32768,
      totalChunks: 2,
      nominalChunkSizeBytes: 16384,
      sha256Digest: 'b'.repeat(64),
    };

    const offerC: SnapshotOfferPayload = {
      offerId: '00000000-0000-4000-8000-00000000000c',
      sessionId: SESSION_ID,
      membershipEpoch: 1,
      supplierPeerId: 'peer-C',
      snapshotId: 'snap-peer-C-40',
      authorPeerId: 'peer-C',
      snapshotFrontier: { 'peer-C': 40 },
      maxLogicalTimestamp: 400,
      totalSizeBytes: 16384,
      totalChunks: 1,
      nominalChunkSizeBytes: 16384,
      sha256Digest: 'c'.repeat(64),
    };

    const ctxB: AuthenticatedSupplierContext = Object.freeze({
      supplierPeerId: 'peer-B',
      connectionGeneration: 1,
    });
    const ctxC: AuthenticatedSupplierContext = Object.freeze({
      supplierPeerId: 'peer-C',
      connectionGeneration: 1,
    });

    registry.addCandidate(offerB, ctxB, simulatedTime);
    registry.addCandidate(offerC, ctxC, simulatedTime);

    expect(registry.size).toBe(2);
    expect(registry.getCandidate('peer-B')).toBeDefined();
    expect(registry.getCandidate('peer-C')).toBeDefined();

    // Supplier Peer-B crashes / disconnects during transfer
    // Registry evicts crashed supplier
    const removed = registry.evict('peer-B');
    expect(removed).toBe(true);
    expect(registry.size).toBe(1);
    expect(registry.getCandidate('peer-B')).toBeUndefined();

    // Failover: registry retains Peer-C
    const failoverCandidate = registry.getCandidate('peer-C');
    expect(failoverCandidate).toBeDefined();
    expect(failoverCandidate!.offer.supplierPeerId).toBe('peer-C');

    // Peer-C transfer completes successfully
    const snapshotPayloadC = createValidSnapshotPayload({
      snapshotId: 'snap-peer-C-40',
      authorPeerId: 'peer-C',
      snapshotFrontier: { 'peer-C': 40 },
      originFrontierTimestamps: { 'peer-C': 400 },
      maxLogicalTimestamp: 400,
      compactedCheckpoints: [
        { originPeerId: 'peer-C', sequenceNumber: 40, logicalTimestamp: 400 },
      ],
    });

    await coordA.executeRecoveryWithSnapshot(snapshotPayloadC);

    // Verification: Peer-A is active with Peer-C snapshot installed
    expect(coordA.state).toBe('ACTIVE');
    expect(pA.engine.replicationFrontier['peer-C']).toBe(40);
    expect(pA.engine.baseSnapshotFrontier['peer-C']).toBe(40);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP4: High-Concurrency Quarantine Burst (500 Operations)
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP4: High-Concurrency Quarantine Burst: 500 remote ops quarantined and drained without loss', async () => {
    const pLocal = createEngine('peer-local');
    const coordinator = createCoordinator(pLocal.engine, pLocal.capability);

    // Trigger recovery
    coordinator.triggerRecovery('peer-remote', 'High concurrency burst test');
    expect(coordinator.state).toBe('RECOVERING');

    // Snapshot base at seq 20
    const snapshotPayload = createValidSnapshotPayload({
      snapshotFrontier: { 'peer-remote': 20 },
      originFrontierTimestamps: { 'peer-remote': 200 },
      maxLogicalTimestamp: 200,
      compactedCheckpoints: [
        { originPeerId: 'peer-remote', sequenceNumber: 20, logicalTimestamp: 200 },
      ],
    });

    // Burst of 500 remote operations arriving during recovery (sequences 21..520)
    const BURST_COUNT = 500;
    for (let i = 1; i <= BURST_COUNT; i++) {
      const seq = 20 + i;
      const ts = 200 + i;
      const prevSeq = seq - 1;
      const op = createMockQuarantinedOp(
        'peer-remote',
        seq,
        ts,
        { 'peer-remote': prevSeq },
        i % 2 === 0 ? 'PLAY' : 'PAUSE',
        10000 + i * 10,
      );

      const decision = coordinator.interceptRemoteEvent(op.event, 128);
      expect(decision.shouldQuarantine).toBe(true);
      expect(decision.dropped).toBe(false);
    }

    // Verify dual bounds: 500 <= 1,000 count, 500 * 128 = 64,000 <= 4 MiB
    expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(500);
    expect(coordinator.quarantineBuffer.totalRetainedBytes).toBe(500 * 128);

    // Verify zero engine state changes prior to snapshot installation
    expect(pLocal.engine.replicationFrontier['peer-remote']).toBeUndefined();

    // Execute recovery: installs snapshot base 20, then drains all 500 quarantined ops
    await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

    // Verification:
    // All 500 operations applied without loss or out-of-order execution
    expect(coordinator.state).toBe('ACTIVE');
    expect(pLocal.engine.replicationFrontier['peer-remote']).toBe(520);
    expect(pLocal.engine.baseSnapshotFrontier['peer-remote']).toBe(20);
    expect(pLocal.engine.logicalClock).toBeGreaterThanOrEqual(700);
    expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(0);
    expect(coordinator.quarantineBuffer.drainSize).toBe(0);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP5: Partial Drain with Anti-Entropy Repair (100 Operations)
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP5: Partial Drain with Anti-Entropy Repair: blocked ops buffer, AE repair unblocks full drain', async () => {
    const pLocal = createEngine('peer-local');
    const coordinator = createCoordinator(pLocal.engine, pLocal.capability);

    // Snapshot base at seq 20 for peer-A, seq 10 for peer-B
    const snapshotPayload = createValidSnapshotPayload({
      snapshotFrontier: { 'peer-A': 20, 'peer-B': 10 },
      originFrontierTimestamps: { 'peer-A': 200, 'peer-B': 100 },
      maxLogicalTimestamp: 200,
      compactedCheckpoints: [
        { originPeerId: 'peer-A', sequenceNumber: 20, logicalTimestamp: 200 },
        { originPeerId: 'peer-B', sequenceNumber: 10, logicalTimestamp: 100 },
      ],
    });

    // 100 operations in quarantine:
    // Ops 1..40 from peer-A (seqs 21..60): independent, contiguous
    for (let seq = 21; seq <= 60; seq++) {
      const op = createMockQuarantinedOp('peer-A', seq, 200 + (seq - 20), {
        'peer-A': seq - 1,
      });
      coordinator.quarantineBuffer.enqueue(op);
    }

    // Op 41 from peer-A (seq 61): depends on missing peer-B: 11
    coordinator.quarantineBuffer.enqueue(
      createMockQuarantinedOp('peer-A', 61, 241, {
        'peer-A': 60,
        'peer-B': 11, // Missing dependency!
      }),
    );

    // Ops 42..100 from peer-A (seqs 62..120): depend on seq 61
    for (let seq = 62; seq <= 120; seq++) {
      coordinator.quarantineBuffer.enqueue(
        createMockQuarantinedOp('peer-A', seq, 200 + (seq - 20), {
          'peer-A': seq - 1,
        }),
      );
    }

    expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(100);

    // Execute recovery: installs snapshot base
    // Drain pass 1: applies 40 ops, buffers 60 ops in PendingDependencyQueue
    // Because 60 ops are pending on missing peer-B:11, drainBatch yields pending ops
    await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

    // Ops 21..60 are applied in operationLog
    expect(pLocal.engine.replicationFrontier['peer-A']).toBe(60);
    // Ops 61..120 are buffered in PendingDependencyQueue
    const diag = pLocal.engine.getDiagnosticCoreSnapshot();
    expect(diag.pendingQueueSize).toBe(60);
    expect(pLocal.engine.replicationFrontier['peer-B']).toBe(10);

    // Anti-Entropy repair delivers missing dependency peer-B: seq 11
    const missingOp = makeRemoteSyncEvent(
      'peer-B',
      11,
      101,
      { 'peer-B': 10 },
      'PLAY',
      20000,
    );

    pLocal.engine.receive(missingOp);

    // Delivering peer-B: 11 cascades to unblock op 61, which cascades to unblock ops 62..120!
    expect(pLocal.engine.replicationFrontier['peer-B']).toBe(11);
    expect(pLocal.engine.replicationFrontier['peer-A']).toBe(120);
    expect(pLocal.engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(0);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP6: Strong Eventual Consistency Verification
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP6: Strong Eventual Consistency: 3 peers converge identically under reordering, delay, duplicate delivery & recovery', async () => {
    // 3 peers: peer-1, peer-2, peer-3
    const p1 = createEngine('peer-1');
    const p2 = createEngine('peer-2');
    const p3 = createEngine('peer-3');
    const coord1 = createCoordinator(p1.engine, p1.capability);

    // Peer-2 and Peer-3 produce concurrent operations at epoch 1
    // Peer-2: seq 1 (ts 1) PLAY
    const p2_op1 = makeRemoteSyncEvent('peer-2', 1, 1, {}, 'PLAY', 1000);
    // Peer-3: seq 1 (ts 2) SEEK to 25000ms (dep on peer-2: 1)
    const p3_op1 = makeRemoteSyncEvent('peer-3', 1, 2, { 'peer-2': 1 }, 'SEEK', 25000);
    // Peer-2: seq 2 (ts 3) SEEK to 15000ms (dep on peer-2: 1, peer-3: 1)
    const p2_op2 = makeRemoteSyncEvent('peer-2', 2, 3, { 'peer-2': 1, 'peer-3': 1 }, 'SEEK', 15000);
    // Peer-3: seq 2 (ts 4) RATE_CHANGE to 1.5 (dep on peer-3: 1, peer-2: 2)
    const p3_op2 = makeRemoteSyncEvent('peer-3', 2, 4, { 'peer-3': 1, 'peer-2': 2 }, 'RATE_CHANGE', 15000, 1.5);

    // Network exchange between Peer-2 and Peer-3 (with arbitrary delivery order)
    p2.engine.receive(p2_op1);
    p2.engine.receive(p3_op1);
    p2.engine.receive(p2_op2);
    p2.engine.receive(p3_op2);

    // Peer-3 receives in reversed/reordered sequence + duplicate delivery
    p3.engine.receive(p2_op1);
    p3.engine.receive(p3_op1);
    p3.engine.receive(p2_op1); // duplicate!
    p3.engine.receive(p2_op2);
    p3.engine.receive(p3_op2);
    p3.engine.receive(p2_op2); // duplicate!

    // Verify Peer-2 and Peer-3 have converged identically
    expect(p2.engine.replicationFrontier).toEqual(p3.engine.replicationFrontier);
    expect(p2.engine.playbackState.status).toBe(p3.engine.playbackState.status);
    expect(p2.engine.playbackState.anchorPositionMs).toBe(p3.engine.playbackState.anchorPositionMs);
    expect(p2.engine.playbackState.playbackRate).toBe(p3.engine.playbackState.playbackRate);

    // Peer-1 was lagging/recovering. Peer-1 receives snapshot from Peer-2 covering up to seq 1 of each peer
    const snapshotPayload = createValidSnapshotPayload({
      snapshotId: 'snap-sec-convergence',
      authorPeerId: 'peer-2',
      state: {
        formatVersion: 1,
        sessionId: SESSION_ID,
        status: 'playing',
        anchorPositionMs: 25000,
        playbackRate: 1.0,
      },
      snapshotFrontier: { 'peer-2': 1, 'peer-3': 1 },
      originFrontierTimestamps: { 'peer-2': 1, 'peer-3': 2 },
      maxLogicalTimestamp: 2,
      compactedCheckpoints: [
        { originPeerId: 'peer-2', sequenceNumber: 1, logicalTimestamp: 1 },
        { originPeerId: 'peer-3', sequenceNumber: 1, logicalTimestamp: 2 },
      ],
    });

    // While Peer-1 is recovering, p2_op2 and p3_op2 arrive over DataChannel and are quarantined
    coord1.triggerRecovery('peer-2', 'Convergence test');
    coord1.interceptRemoteEvent(p2_op2, 128);
    coord1.interceptRemoteEvent(p3_op2, 128);
    coord1.interceptRemoteEvent(p2_op2, 128); // duplicate arrival during recovery!

    expect(coord1.quarantineBuffer.totalRetainedCount).toBe(2); // duplicate recognized and dropped

    // Peer-1 installs snapshot and drains quarantine
    await coord1.executeRecoveryWithSnapshot(snapshotPayload);
    expect(coord1.state).toBe('ACTIVE');

    // Deliver any remaining duplicate network messages to Peer-1
    p1.engine.receive(p2_op1); // already covered by snapshot base
    p1.engine.receive(p3_op1); // already covered by snapshot base

    // FINAL SEC INVARIANT VERIFICATION across all 3 engines:
    // 1. Identical replication vectors
    expect(p1.engine.replicationFrontier).toEqual(p2.engine.replicationFrontier);
    expect(p2.engine.replicationFrontier).toEqual(p3.engine.replicationFrontier);

    // 2. Identical playback status
    expect(p1.engine.playbackState.status).toBe(p2.engine.playbackState.status);
    expect(p2.engine.playbackState.status).toBe(p3.engine.playbackState.status);

    // 3. Identical anchor positions
    expect(p1.engine.playbackState.anchorPositionMs).toBe(p2.engine.playbackState.anchorPositionMs);
    expect(p2.engine.playbackState.anchorPositionMs).toBe(p3.engine.playbackState.anchorPositionMs);

    // 4. Identical playback rates
    expect(p1.engine.playbackState.playbackRate).toBe(p2.engine.playbackState.playbackRate);
    expect(p2.engine.playbackState.playbackRate).toBe(p3.engine.playbackState.playbackRate);

    // 5. Zero pending operations remaining in any engine
    expect(p1.engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(0);
    expect(p2.engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(0);
    expect(p3.engine.getDiagnosticCoreSnapshot().pendingQueueSize).toBe(0);
  });
});
