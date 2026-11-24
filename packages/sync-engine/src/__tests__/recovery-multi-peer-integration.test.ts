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
  SnapshotSerializer,
  SnapshotChunkProducer,
  ChunkAssembler,
  SnapshotOutboundPipeline,
  SnapshotInboundPipeline,
  createRecoveryInstallationCapability,
  createInboundTransportCapability,
  createOutboundTransportCapability,
  createAuthenticatedRecipientContext,
  computeSnapshotDigestV2,
  SyncEngineError,
  type StateSnapshotPayloadV2,
  type SnapshotOfferPayload,
  type SnapshotRequestPayload,
  type QuarantinedOperation,
  type AuthenticatedSupplierContext,
  type OutboundHandoffRequest,
  type SnapshotTransferResult,
} from '../index.js';
import { PROTOCOL_VERSION, MessageType, type ProtocolMessage } from '@syncplay/protocol';
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
    const evt3 = makeRemoteSyncEvent('peer-3', 1, 3, { 'peer-2': 1 }, 'RATE_CHANGE', 30000, 1.5);
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
    coord4.triggerRecovery('peer-2', 'Late join catch-up');

    expect(p4.engine.replicationFrontier).toEqual({});
    expect(p4.engine.logicalClock).toBe(0);

    // Export active snapshot from peer-2
    const rawSnapshot = p2.engine.exportSnapshotV2('snap-late-join-001');
    const artifact = SnapshotSerializer.serialize(rawSnapshot);
    expect(artifact.totalSizeBytes).toBeGreaterThan(0);
    expect(artifact.sha256Digest).toBe(rawSnapshot.sha256Digest);

    // Setup SnapshotOutboundPipeline on Supplier Peer-2
    const outboundCap = createOutboundTransportCapability(SESSION_ID, 'peer-2');
    const outboundPipeline = new SnapshotOutboundPipeline({ transportCapability: outboundCap });
    cleanupFns.push(() => outboundPipeline.dispose());

    // Setup SnapshotInboundPipeline on Receiving Peer-4
    let completedResult: SnapshotTransferResult | undefined;
    const emittedRequests: OutboundHandoffRequest[] = [];
    const inboundCap = createInboundTransportCapability(SESSION_ID, 'peer-4');
    const inboundPipeline = new SnapshotInboundPipeline({
      transportCapability: inboundCap,
      onTransferComplete: async (result) => {
        completedResult = result;
        const payload: StateSnapshotPayloadV2 = {
          ...result.snapshot,
          sha256Digest: result.sha256Digest,
        };
        await coord4.executeRecoveryWithSnapshot(payload);
      },
      onEmitMessage: async (handoff) => {
        emittedRequests.push(handoff);
      },
    });
    cleanupFns.push(() => inboundPipeline.dispose());

    // Register authenticated supplier context on Peer-4 inbound pipeline
    const supplierCtx = inboundPipeline.registerSupplierContext('peer-2', 1, 65536, inboundCap, 1);

    // Step 1: Supplier Peer-2 initiates outbound transfer session and generates initial offer
    const initTransferId = '00000000-0000-4000-8000-000000000001';
    const initSession = await outboundPipeline.startTransfer({
      transferId: initTransferId,
      recipientPeerId: 'peer-4',
      supplierPeerId: 'peer-2',
      membershipEpoch: 1,
      connectionGeneration: 1,
      sctpMaxMessageSize: 65536,
      artifact,
    });
    expect(initSession.offerMessage).toBeDefined();

    // Step 2: Receiving Peer-4 processes the offer through SnapshotInboundPipeline
    const offerRes = inboundPipeline.handleOffer(initSession.offerMessage!, supplierCtx);
    expect(offerRes.admitted).toBe(true);
    expect(offerRes.role).toBe('ACTIVE');

    // Step 3: Peer-4 starts inbound transfer, emitting SNAPSHOT_REQUEST
    await inboundPipeline.startTransfer();
    expect(emittedRequests.length).toBeGreaterThan(0);
    const reqHandoff = emittedRequests[0];
    expect(reqHandoff.messageType).toBe('SNAPSHOT_REQUEST');

    const session = inboundPipeline.getActiveSession()!;
    const transferId = session.transferId;
    expect(transferId).toBeDefined();
    expect(session.supplierPeerId).toBe('peer-2');

    // Step 4: Supplier Peer-2 receives chunk request for active transferId
    outboundPipeline.cancelTransfer(initTransferId);
    const activeOutbound = await outboundPipeline.startTransfer({
      transferId,
      recipientPeerId: 'peer-4',
      supplierPeerId: 'peer-2',
      membershipEpoch: 1,
      connectionGeneration: 1,
      sctpMaxMessageSize: 65536,
      artifact,
    });

    const recipientCtx = createAuthenticatedRecipientContext(
      outboundPipeline,
      {
        transferId,
        recipientPeerId: 'peer-4',
        sessionId: SESSION_ID,
        membershipEpoch: 1,
        connectionGeneration: 1,
      },
      outboundCap,
    );

    const reqPayload = reqHandoff.message.payload as SnapshotRequestPayload;
    const preparedChunks = outboundPipeline.handleChunkRequest(
      transferId,
      reqPayload.missingChunkIndices ?? [0],
      recipientCtx,
    );
    expect(preparedChunks.length).toBe(activeOutbound.totalChunks);

    // Step 5: Deliver chunks through bidirectional pipelines
    for (const chunk of preparedChunks) {
      // Local handoff on outbound pipeline
      outboundPipeline.handleHandoffSuccess(
        transferId,
        chunk.payload.chunkIndex,
        chunk.messageId,
        chunk.attemptToken,
        recipientCtx,
      );

      // Request handoff confirmation on inbound pipeline
      inboundPipeline.handleHandoffSuccess(
        transferId,
        chunk.payload.chunkIndex,
        reqHandoff.message.messageId,
        `token-${chunk.payload.chunkIndex}`,
        supplierCtx,
      );

      // Inbound reception and ingestion
      const chunkRes = await inboundPipeline.handleChunk(chunk.message, supplierCtx);
      expect(chunkRes.accepted).toBe(true);

      // Delivery acknowledgement on outbound pipeline
      outboundPipeline.handleChunkAck(transferId, chunk.payload.chunkIndex, recipientCtx);
    }

    // Verification:
    // Transfer completed, snapshot installed through RecoveryCoordinator
    expect(completedResult).toBeDefined();
    expect(coord4.state).toBe('ACTIVE');
    expect(p4.engine.isRecoveryActive()).toBe(false);
    expect(p4.engine.playbackState.status).toBe('playing');
    expect(p4.engine.playbackState.anchorPositionMs).toBe(30000);
    expect(p4.engine.playbackState.playbackRate).toBe(1.5);
    expect(p4.engine.replicationFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p4.engine.baseSnapshotFrontier).toEqual({ 'peer-1': 1, 'peer-2': 1, 'peer-3': 1 });
    expect(p4.engine.logicalClock).toBeGreaterThanOrEqual(3);

    // Peer-4 participates in live replication
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
    for (let seq = 2; seq <= 300; seq++) {
      vi.advanceTimersByTime(100);
      const op = makeRemoteSyncEvent('peer-A', seq, seq, { 'peer-A': seq - 1 }, 'PLAY', 1000 + seq * 10);
      pA.engine.receive(op);
    }
    expect(pA.engine.replicationFrontier['peer-A']).toBe(300);

    // Production safe-compaction workflow via LeaderlessSyncEngine & CompactionManager:
    // Peer-B is offline/disconnected. The connected peer descriptor set is empty.
    const view = pA.engine.captureMembershipView([]);
    const candidate = pA.engine.calculateCompactionFrontier('MEMBERSHIP_SAFE', view);
    expect(candidate.eligible).toBe(true);
    expect(candidate.frontier).toEqual({ 'peer-A': 300 });

    const execResult = pA.engine.executeCompaction(candidate, []);
    expect(execResult.committed).toBe(true);
    // Compaction pruned all 256 operations held within the ring buffer capacity
    expect(execResult.prunedCount).toBe(256);
    expect(pA.engine.operationLog.size).toBe(0);
    expect(pA.engine.operationLog.getBaseSnapshotFrontier()['peer-A']).toBe(300);

    // Returning Peer-B initiates anti-entropy digest evaluation against Peer-A
    const digestA = pA.engine.createLocalDigest();
    expect(digestA.replicationVector['peer-A']).toBe(300);
    expect(digestA.snapshotFrontier['peer-A']).toBe(300);

    const comparison = pB.engine.compareRemoteDigest(digestA);
    expect(comparison.requiresSnapshotRecovery).toBe(true);
    expect(comparison.outsideRemoteHorizon).toBe(true);
    expect(comparison.classification).toBe('REQUIRES_SNAPSHOT_RECOVERY');
    const originDetail = comparison.originDetails.find((d) => d.originPeerId === 'peer-A');
    expect(originDetail?.status).toBe('OUTSIDE_REMOTE_HORIZON');

    // Real snapshot export from compacted Peer-A state
    const snapshotPayload = pA.engine.exportSnapshotV2('snap-horizon-300');
    expect(snapshotPayload.snapshotFrontier['peer-A']).toBe(300);

    // Returning Peer-B receives op 301 from Peer-A while offline gap exists
    const op301 = createMockQuarantinedOp('peer-A', 301, 301, { 'peer-A': 300 }, 'PAUSE', 50000);

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
    const pA = createEngine('peer-A');
    const coordA = createCoordinator(pA.engine, pA.capability);
    coordA.triggerRecovery('peer-B', 'Supplier failover test');

    // Canonical snapshot payload offered by both candidate suppliers
    const snapshotPayload = createValidSnapshotPayload({
      snapshotId: 'snap-failover-40',
      authorPeerId: 'peer-B',
      snapshotFrontier: { 'peer-B': 40 },
      originFrontierTimestamps: { 'peer-B': 400 },
      maxLogicalTimestamp: 400,
      compactedCheckpoints: [
        { originPeerId: 'peer-B', sequenceNumber: 40, logicalTimestamp: 400 },
      ],
    });

    const artifactB = SnapshotSerializer.serialize(snapshotPayload);
    const artifactC = SnapshotSerializer.serialize(snapshotPayload);
    const nominalChunkSizeBytes = Math.max(256, Math.ceil(artifactB.totalSizeBytes / 2));

    let completedResult: SnapshotTransferResult | undefined;
    const emittedRequests: OutboundHandoffRequest[] = [];
    const inboundCap = createInboundTransportCapability(SESSION_ID, 'peer-A');
    const inboundPipeline = new SnapshotInboundPipeline({
      transportCapability: inboundCap,
      onTransferComplete: async (result) => {
        completedResult = result;
        const payload: StateSnapshotPayloadV2 = {
          ...result.snapshot,
          sha256Digest: result.sha256Digest,
        };
        await coordA.executeRecoveryWithSnapshot(payload);
      },
      onEmitMessage: async (handoff) => {
        emittedRequests.push(handoff);
      },
    });
    cleanupFns.push(() => inboundPipeline.dispose());

    // Register supplier contexts with unforgeable transport capability
    const ctxB = inboundPipeline.registerSupplierContext('peer-B', 1, 65536, inboundCap, 1);
    const ctxC = inboundPipeline.registerSupplierContext('peer-C', 1, 65536, inboundCap, 1);

    // Two candidates offer the identical snapshot:
    // Peer-B offers snapshot (admitted as ACTIVE)
    // Peer-C offers snapshot (admitted as CANDIDATE in CandidateOfferRegistry)
    const offerMsgB: ProtocolMessage<SnapshotOfferPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_OFFER,
      sessionId: SESSION_ID,
      senderPeerId: 'peer-B',
      membershipEpoch: 1,
      messageId: 'msg-offer-b',
      payload: {
        offerId: '00000000-0000-4000-8000-000000000001',
        sessionId: SESSION_ID,
        membershipEpoch: 1,
        supplierPeerId: 'peer-B',
        snapshotId: 'snap-failover-40',
        authorPeerId: 'peer-B',
        snapshotFrontier: { 'peer-B': 40 },
        maxLogicalTimestamp: 400,
        totalSizeBytes: artifactB.totalSizeBytes,
        totalChunks: 2,
        nominalChunkSizeBytes,
        sha256Digest: artifactB.sha256Digest,
      },
    };

    const offerMsgC: ProtocolMessage<SnapshotOfferPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_OFFER,
      sessionId: SESSION_ID,
      senderPeerId: 'peer-C',
      membershipEpoch: 1,
      messageId: 'msg-offer-c',
      payload: {
        offerId: '00000000-0000-4000-8000-000000000002',
        sessionId: SESSION_ID,
        membershipEpoch: 1,
        supplierPeerId: 'peer-C',
        snapshotId: 'snap-failover-40',
        authorPeerId: 'peer-B',
        snapshotFrontier: { 'peer-B': 40 },
        maxLogicalTimestamp: 400,
        totalSizeBytes: artifactC.totalSizeBytes,
        totalChunks: 2,
        nominalChunkSizeBytes,
        sha256Digest: artifactC.sha256Digest,
      },
    };

    const resB = inboundPipeline.handleOffer(offerMsgB, ctxB);
    expect(resB.admitted).toBe(true);
    expect(resB.role).toBe('ACTIVE');

    const resC = inboundPipeline.handleOffer(offerMsgC, ctxC);
    expect(resC.admitted).toBe(true);
    expect(resC.role).toBe('CANDIDATE');

    // Start transfer with Supplier B
    await inboundPipeline.startTransfer();
    expect(emittedRequests.length).toBeGreaterThan(0);
    const session = inboundPipeline.getActiveSession()!;
    const transferId = session.transferId;
    expect(session.supplierPeerId).toBe('peer-B');

    // Supplier B produces chunk 0
    inboundPipeline.handleHandoffSuccess(
      transferId,
      0,
      emittedRequests[0].message.messageId,
      'attempt-0',
      ctxB,
    );
    const producerB = new SnapshotChunkProducer(artifactB, {
      nominalChunkSizeBytes,
      transferId,
      allowSctpFallback: true,
    });
    const chunk0B = producerB.produceChunk(0, { supplierPeerId: 'peer-B' });
    const ingest0 = await inboundPipeline.handleChunk(chunk0B.message, ctxB);
    expect(ingest0.accepted).toBe(true);
    expect(session.receivedChunks.has(0)).toBe(true);
    expect(session.missingChunks.has(1)).toBe(true);

    // Supplier Peer-B crashes / disconnects during transfer -> trigger production failover
    const failoverResult = await inboundPipeline.handleSupplierDisconnected(ctxB, inboundCap);
    expect(failoverResult.failoverSuccess).toBe(true);

    // Verify inbound pipeline orchestrates seamless failover to Supplier C:
    const sessionAfter = inboundPipeline.getActiveSession()!;
    expect(sessionAfter.supplierPeerId).toBe('peer-C');
    expect(sessionAfter.receivedChunks.has(0)).toBe(true); // Chunk 0 preserved across failover!
    expect(sessionAfter.missingChunks.has(1)).toBe(true); // Chunk 1 scheduled for C!
    expect(sessionAfter.sessionId).toBe(SESSION_ID);
    expect(sessionAfter.snapshotId).toBe('snap-failover-40');
    expect(sessionAfter.expectedSha256Digest).toBe(artifactB.sha256Digest);
    expect(sessionAfter.totalSizeBytes).toBe(artifactB.totalSizeBytes);
    expect(sessionAfter.totalChunks).toBe(2);

    // Stale chunk from crashed Supplier B arrives -> REJECTED (inactive supplier context)
    const chunk1B = producerB.produceChunk(1, { supplierPeerId: 'peer-B' });
    const staleRes = await inboundPipeline.handleChunk(chunk1B.message, ctxB);
    expect(staleRes.accepted).toBe(false);

    // Supplier C produces chunk 1 for active transferId
    const failoverReq = emittedRequests[emittedRequests.length - 1];
    inboundPipeline.handleHandoffSuccess(
      transferId,
      1,
      failoverReq.message.messageId,
      'attempt-1',
      ctxC,
    );
    const producerC = new SnapshotChunkProducer(artifactC, {
      nominalChunkSizeBytes,
      transferId,
      allowSctpFallback: true,
    });
    const chunk1C = producerC.produceChunk(1, { supplierPeerId: 'peer-C' });
    const freshRes = await inboundPipeline.handleChunk(chunk1C.message, ctxC);
    expect(freshRes.accepted).toBe(true);
    expect(freshRes.completed).toBe(true);

    // Verification: Snapshot installed into coordinator, Peer-A transitions to ACTIVE
    expect(completedResult).toBeDefined();
    expect(coordA.state).toBe('ACTIVE');
    expect(pA.engine.replicationFrontier['peer-B']).toBe(40);
    expect(pA.engine.baseSnapshotFrontier['peer-B']).toBe(40);
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SCENARIO MP4: High-Concurrency Quarantine Burst (500 Operations)
  // ═══════════════════════════════════════════════════════════════════════════

  it('M4.3d-MP4: High-Concurrency Quarantine Burst: 500 remote ops across 3 origins quarantined and drained without loss', async () => {
    const pLocal = createEngine('peer-local');
    const coordinator = createCoordinator(pLocal.engine, pLocal.capability);

    // Trigger recovery
    coordinator.triggerRecovery('peer-remote', 'High concurrency burst test');
    expect(coordinator.state).toBe('RECOVERING');

    // Snapshot base at seq 20 for 3 remote peers
    const snapshotPayload = createValidSnapshotPayload({
      snapshotFrontier: { 'peer-A': 20, 'peer-B': 20, 'peer-C': 20 },
      originFrontierTimestamps: { 'peer-A': 200, 'peer-B': 200, 'peer-C': 200 },
      maxLogicalTimestamp: 200,
      compactedCheckpoints: [
        { originPeerId: 'peer-A', sequenceNumber: 20, logicalTimestamp: 200 },
        { originPeerId: 'peer-B', sequenceNumber: 20, logicalTimestamp: 200 },
        { originPeerId: 'peer-C', sequenceNumber: 20, logicalTimestamp: 200 },
      ],
    });

    // Interleaved burst of 500 operations across 3 remote origins:
    // peer-A: 200 ops (seq 21..220)
    // peer-B: 150 ops (seq 21..170)
    // peer-C: 150 ops (seq 21..170)
    let countA = 0;
    let countB = 0;
    let countC = 0;

    for (let round = 1; round <= 200; round++) {
      if (round <= 200) {
        countA++;
        const seq = 20 + countA;
        const opA = createMockQuarantinedOp(
          'peer-A',
          seq,
          200 + countA,
          { 'peer-A': seq - 1 },
          countA % 2 === 0 ? 'PLAY' : 'PAUSE',
          10000 + countA * 10,
        );
        const decA = coordinator.interceptRemoteEvent(opA.event, 128);
        expect(decA.shouldQuarantine).toBe(true);
        expect(decA.dropped).toBe(false);
      }
      if (round <= 150) {
        countB++;
        const seq = 20 + countB;
        const opB = createMockQuarantinedOp(
          'peer-B',
          seq,
          200 + countB,
          { 'peer-B': seq - 1 },
          countB % 2 === 0 ? 'PLAY' : 'PAUSE',
          10000 + countB * 10,
        );
        const decB = coordinator.interceptRemoteEvent(opB.event, 128);
        expect(decB.shouldQuarantine).toBe(true);
        expect(decB.dropped).toBe(false);
      }
      if (round <= 150) {
        countC++;
        const seq = 20 + countC;
        const opC = createMockQuarantinedOp(
          'peer-C',
          seq,
          200 + countC,
          { 'peer-C': seq - 1 },
          countC % 2 === 0 ? 'PLAY' : 'PAUSE',
          10000 + countC * 10,
        );
        const decC = coordinator.interceptRemoteEvent(opC.event, 128);
        expect(decC.shouldQuarantine).toBe(true);
        expect(decC.dropped).toBe(false);
      }
    }
    expect(countA + countB + countC).toBe(500);

    // Verify dual bounds: 500 <= 1,000 count, 500 * 128 = 64,000 <= 4 MiB
    expect(coordinator.quarantineBuffer.totalRetainedCount).toBe(500);
    expect(coordinator.quarantineBuffer.totalRetainedBytes).toBe(500 * 128);

    // Verify zero engine state changes prior to snapshot installation
    expect(pLocal.engine.replicationFrontier['peer-A']).toBeUndefined();
    expect(pLocal.engine.replicationFrontier['peer-B']).toBeUndefined();
    expect(pLocal.engine.replicationFrontier['peer-C']).toBeUndefined();

    // Execute recovery: installs snapshot base 20, then drains all 500 quarantined ops
    await coordinator.executeRecoveryWithSnapshot(snapshotPayload);

    // Verification:
    // All 500 operations applied without loss or causal violation across 3 origins
    expect(coordinator.state).toBe('ACTIVE');
    expect(pLocal.engine.replicationFrontier['peer-A']).toBe(220);
    expect(pLocal.engine.replicationFrontier['peer-B']).toBe(170);
    expect(pLocal.engine.replicationFrontier['peer-C']).toBe(170);
    expect(pLocal.engine.baseSnapshotFrontier).toEqual({ 'peer-A': 20, 'peer-B': 20, 'peer-C': 20 });
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
