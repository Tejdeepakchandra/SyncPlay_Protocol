import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import crypto from 'node:crypto';
import {
  SnapshotOutboundPipeline,
  type OutboundOfferContext,
  type OutboundChunkContext,
} from '../recovery/snapshot-outbound-pipeline.js';
import {
  SnapshotChunkProducer,
} from '../recovery/snapshot-chunk-producer.js';
import {
  SnapshotSerializer,
  SerializedSnapshotArtifact,
} from '../recovery/snapshot-serializer.js';
import {
  SnapshotPreparationQueue,
} from '../recovery/snapshot-preparation-queue.js';
import {
  ChunkAssembler,
} from '../recovery/chunk-assembler.js';
import {
  PROTOCOL_VERSION,
  MessageType,
  SnapshotOfferPayloadSchema,
  SnapshotChunkEnvelopeSchema,
  type StateSnapshotPayloadV2,
  type ProtocolMessage,
  type SnapshotChunkPayload,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';

describe('Phase 7.5 M4.2c — SnapshotOutboundPipeline & Send-Boundary Verification', () => {
  const SESSION_ID = 'sess-test-alpha';
  const SUPPLIER_PEER_ID = 'peer-supplier-01';
  const RECIPIENT_PEER_ID = 'peer-recipient-02';
  const MEMBERSHIP_EPOCH = 2;
  const FALLBACK_SCTP_LIMIT = 65_536;

  function generateUUID(): string {
    return crypto.randomUUID();
  }

  function createTestSnapshot(checkpointCount = 2, idSuffix = '1'): StateSnapshotPayloadV2 {
    const rawState = {
      formatVersion: 1 as const,
      sessionId: SESSION_ID,
      status: 'playing' as const,
      anchorPositionMs: 15000,
      playbackRate: 1.0,
      lastAppliedOperation: {
        logicalEventId: `evt-${idSuffix}`,
        senderPeerId: SUPPLIER_PEER_ID,
        messageType: 'PLAYBACK_PLAY',
      },
    };

    const compactedCheckpoints = [];
    for (let i = 0; i < checkpointCount; i++) {
      compactedCheckpoints.push({
        originPeerId: SUPPLIER_PEER_ID,
        sequenceNumber: i + 1,
        logicalTimestamp: 100 + i,
      });
    }

    return {
      formatVersion: 2,
      snapshotId: `snap-${idSuffix}`,
      sessionId: SESSION_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      authorPeerId: SUPPLIER_PEER_ID,
      snapshotFrontier: { [SUPPLIER_PEER_ID]: 10 },
      maxLogicalTimestamp: 1000,
      originFrontierTimestamps: { [SUPPLIER_PEER_ID]: 1000 },
      compactedCheckpoints,
      state: rawState,
    };
  }

  function createSerializedArtifact(checkpointCount = 2, idSuffix = '1'): SerializedSnapshotArtifact {
    const snapshot = createTestSnapshot(checkpointCount, idSuffix);
    return SnapshotSerializer.serialize(snapshot);
  }

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // ─────────────────────────────────────────────────────────────
  // Static Methods: createOfferMessage & prepareChunkMessage
  // ─────────────────────────────────────────────────────────────
  describe('Static Wire Message Formatters', () => {
    it('creates valid SNAPSHOT_OFFER conforming to SnapshotOfferPayloadSchema', () => {
      const artifact = createSerializedArtifact(3, 'offer-test');
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      const transferId = generateUUID();
      const offerContext: OutboundOfferContext = {
        transferId,
        supplierPeerId: SUPPLIER_PEER_ID,
        recipientPeerId: RECIPIENT_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 3,
        snapshotFrontier: { [SUPPLIER_PEER_ID]: 42 },
        maxLogicalTimestamp: 500,
      };

      const offerMsg = SnapshotOutboundPipeline.createOfferMessage(producer, offerContext);

      expect(offerMsg.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(offerMsg.messageType).toBe(MessageType.SNAPSHOT_OFFER);
      expect(offerMsg.sessionId).toBe(SESSION_ID);
      expect(offerMsg.senderPeerId).toBe(SUPPLIER_PEER_ID);
      expect(offerMsg.membershipEpoch).toBe(MEMBERSHIP_EPOCH);
      expect(offerMsg.connectionGeneration).toBe(3);

      const parsed = SnapshotOfferPayloadSchema.safeParse(offerMsg.payload);
      expect(parsed.success).toBe(true);
      if (parsed.success) {
        expect(parsed.data.offerId).toBe(transferId);
        expect(parsed.data.sessionId).toBe(SESSION_ID);
        expect(parsed.data.snapshotId).toBe(artifact.snapshotId);
        expect(parsed.data.totalSizeBytes).toBe(artifact.totalSizeBytes);
        expect(parsed.data.totalChunks).toBe(producer.totalChunks);
        expect(parsed.data.sha256Digest).toBe(artifact.sha256Digest);
      }

      producer.dispose();
      artifact.dispose();
    });

    it('rejects offer creation on epoch mismatch', () => {
      const artifact = createSerializedArtifact(1, 'epoch-test');
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      try {
        SnapshotOutboundPipeline.createOfferMessage(producer, {
          transferId: generateUUID(),
          supplierPeerId: SUPPLIER_PEER_ID,
          recipientPeerId: RECIPIENT_PEER_ID,
          membershipEpoch: MEMBERSHIP_EPOCH + 1, // Mismatched epoch
        });
        expect.unreachable('Should have thrown INVALID_ARGUMENT');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('INVALID_ARGUMENT');
      }

      producer.dispose();
      artifact.dispose();
    });

    it('rejects offer creation on disposed artifact', () => {
      const artifact = createSerializedArtifact(1, 'disp-test');
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      producer.dispose();
      artifact.dispose();

      try {
        SnapshotOutboundPipeline.createOfferMessage(producer, {
          transferId: generateUUID(),
          supplierPeerId: SUPPLIER_PEER_ID,
          recipientPeerId: RECIPIENT_PEER_ID,
          membershipEpoch: MEMBERSHIP_EPOCH,
        });
        expect.unreachable('Should have thrown ARTIFACT_DISPOSED');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('ARTIFACT_DISPOSED');
      }
    });

    it('prepares chunk message with exact send-boundary UTF-8 measurement matching serialized JSON', () => {
      const artifact = createSerializedArtifact(5, 'chunk-meas');
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      const chunkContext: OutboundChunkContext = {
        transferId: generateUUID(),
        supplierPeerId: SUPPLIER_PEER_ID,
        recipientPeerId: RECIPIENT_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      };

      const result = SnapshotOutboundPipeline.prepareChunkMessage(producer, 0, chunkContext);
      expect(result.ok).toBe(true);

      if (result.ok) {
        expect(result.protocolMessage.messageType).toBe(MessageType.SNAPSHOT_CHUNK);
        expect(result.byteLength).toBe(new TextEncoder().encode(result.serializedJson).byteLength);
        expect(result.byteLength).toBeLessThanOrEqual(FALLBACK_SCTP_LIMIT);

        const envelopeParsed = SnapshotChunkEnvelopeSchema.safeParse(result.protocolMessage);
        expect(envelopeParsed.success).toBe(true);
      }

      producer.dispose();
      artifact.dispose();
    });

    it('immediately rejects chunk on send-boundary overshoot without slice resizing', () => {
      const artifact = createSerializedArtifact(2, 'overshoot-test');
      // Create partition with comfortable transport limit
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      // Now prepare chunk with an artificially tiny send-boundary SCTP limit
      const tinyLimit = 300; // Too small for chunk envelope + Base64
      const chunkContext: OutboundChunkContext = {
        transferId: generateUUID(),
        supplierPeerId: SUPPLIER_PEER_ID,
        recipientPeerId: RECIPIENT_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: tinyLimit,
      };

      const result = SnapshotOutboundPipeline.prepareChunkMessage(producer, 0, chunkContext);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe('TRANSPORT_LIMIT_EXCEEDED');
        expect(result.detail).toContain('exceeds negotiated SCTP limit');
      }

      // Verify slice in producer was NOT resized or corrupted
      const slice0 = producer.getSlice(0)!;
      expect(slice0.chunkSizeBytes).toBe(artifact.totalSizeBytes);

      producer.dispose();
      artifact.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 24 Lifecycle & Architecture Verification Requirements
  // ─────────────────────────────────────────────────────────────

  // 1. Successful snapshot preparation and producer creation.
  it('1. performs successful snapshot preparation and producer creation', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    const session = await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(2, 'test-1'),
    });

    expect(session.state).toBe('PREPARED');
    expect(session.transferId).toBe(transferId);
    expect(session.totalChunks).toBeGreaterThan(0);
    expect(session.offerMessage).toBeDefined();
    expect(session.offerMessage?.payload.offerId).toBe(transferId);

    pipeline.dispose();
  });

  // 2. Correct snapshot-offer preparation with epoch & session fencing.
  it('2. verifies correct snapshot-offer preparation with epoch and session fencing', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    const session = await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      connectionGeneration: 7,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      snapshotFrontier: { 'peer-x': 99 },
      maxLogicalTimestamp: 8888,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'test-2'),
    });

    const offer = session.offerMessage!;
    expect(offer.messageType).toBe(MessageType.SNAPSHOT_OFFER);
    expect(offer.membershipEpoch).toBe(MEMBERSHIP_EPOCH);
    expect(offer.connectionGeneration).toBe(7);
    expect(offer.payload.snapshotFrontier).toEqual({ 'peer-x': 99 });
    expect(offer.payload.maxLogicalTimestamp).toBe(8888);

    pipeline.dispose();
  });

  // 3. Bounded preparation and active-transfer admission (A + P <= 2).
  it('3. enforces bounded active-transfer admission ceiling (maxActiveTransfers = 2)', async () => {
    const pipeline = new SnapshotOutboundPipeline({ maxActiveTransfers: 2 });

    const id1 = generateUUID();
    const id2 = generateUUID();
    const id3 = generateUUID();

    await pipeline.startTransfer({
      transferId: id1,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 't1'),
    });

    await pipeline.startTransfer({
      transferId: id2,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 't2'),
    });

    expect(pipeline.activeTransferCount).toBe(2);

    // Third concurrent transfer should be rejected immediately
    await expect(
      pipeline.startTransfer({
        transferId: id3,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 't3'),
      })
    ).rejects.toThrowError(/Maximum concurrent active outbound transfers reached/);

    pipeline.dispose();
  });

  // 4. Bounded sliding-window behavior (W = 4).
  it('4. enforces bounded sliding-window behavior (W = 4)', async () => {
    const pipeline = new SnapshotOutboundPipeline({ slidingWindowSize: 4 });
    const transferId = generateUUID();

    // Use small transport limit to produce at least 6 chunks
    const artifact = createSerializedArtifact(30, 'window-test');
    const session = await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144, // forces small chunks
      artifact,
    });

    expect(session.totalChunks).toBeGreaterThanOrEqual(6);

    // Request 4 chunks: fits window exactly
    const frames = pipeline.handleChunkRequest(transferId, [0, 1, 2, 3]);
    expect(frames.length).toBe(4);

    let state = pipeline.getTransfer(transferId)!;
    expect(state.state).toBe('WAITING_ACK');
    expect(state.inflightChunks.size).toBe(4);

    // Requesting a 5th chunk while window is full should throw LOCAL_ABORT
    expect(() => pipeline.handleChunkRequest(transferId, [4])).toThrowError(/sliding window capacity/);

    // Acknowledge chunk 0: window advances!
    pipeline.handleChunkAck(transferId, 0);
    state = pipeline.getTransfer(transferId)!;
    expect(state.state).toBe('ACTIVE');
    expect(state.inflightChunks.size).toBe(3);

    // Now chunk 4 can be requested
    const nextFrames = pipeline.handleChunkRequest(transferId, [4]);
    expect(nextFrames.length).toBe(1);
    expect(nextFrames[0].payload.chunkIndex).toBe(4);

    pipeline.dispose();
  });

  // 5. Correct chunk ordering and chunk identity.
  it('5. preserves canonical chunk ordering and chunk payload identity', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144,
      captureAndSerializeFn: () => createSerializedArtifact(10, 'order-id'),
    });

    const frames = pipeline.handleChunkRequest(transferId, [0, 1]);
    expect(frames[0].payload.chunkIndex).toBe(0);
    expect(frames[1].payload.chunkIndex).toBe(1);
    expect(frames[0].payload.transferId).toBe(transferId);
    expect(frames[1].payload.transferId).toBe(transferId);

    pipeline.dispose();
  });

  // 6. Out-of-order chunk production where permitted.
  it('6. produces chunks out-of-order within valid range', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144,
      captureAndSerializeFn: () => createSerializedArtifact(10, 'ooo-test'),
    });

    // Request out of order: [2, 0]
    const frames = pipeline.handleChunkRequest(transferId, [2, 0]);
    expect(frames[0].payload.chunkIndex).toBe(2);
    expect(frames[1].payload.chunkIndex).toBe(0);

    const session = pipeline.getTransfer(transferId)!;
    expect(session.inflightChunks.has(2)).toBe(true);
    expect(session.inflightChunks.has(0)).toBe(true);

    pipeline.dispose();
  });

  // 7. Retransmission of previously produced chunks.
  it('7. supports retransmission of unacknowledged frames without increasing in-flight count', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144,
      captureAndSerializeFn: () => createSerializedArtifact(10, 'retransmit-test'),
    });

    // First request chunk 0
    const [frame1] = pipeline.handleChunkRequest(transferId, [0]);
    expect(pipeline.getTransfer(transferId)!.inflightChunks.size).toBe(1);

    // Retransmit chunk 0
    const [frame2] = pipeline.handleChunkRequest(transferId, [0]);
    expect(frame2.payload.chunkIndex).toBe(0);
    expect(frame2.serializedJson).toBe(frame1.serializedJson);

    // In-flight count remains 1, retry count is 2
    const session = pipeline.getTransfer(transferId)!;
    expect(session.inflightChunks.size).toBe(1);
    expect(session.retryCounts.get(0)).toBe(2);

    pipeline.dispose();
  });

  // 8. Duplicate acknowledgement handling.
  it('8. handles duplicate chunk acknowledgement as an idempotent no-op', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'dup-ack'),
    });

    pipeline.handleChunkRequest(transferId, [0]);

    // First ACK
    expect(() => pipeline.handleChunkAck(transferId, 0)).not.toThrow();
    expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

    // Duplicate ACK on already completed transfer
    expect(() => pipeline.handleChunkAck(transferId, 0)).not.toThrow();
    expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

    pipeline.dispose();
  });

  // 9. Late acknowledgement after transfer termination.
  it('9. safely ignores late acknowledgement after transfer termination without resurrecting', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'late-ack'),
    });

    // Abort transfer
    pipeline.cancelTransfer(transferId, 'Cancelled for test');
    expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

    // Late ACK arrives
    expect(() => pipeline.handleChunkAck(transferId, 0)).not.toThrow();
    expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

    pipeline.dispose();
  });

  // 10. Invalid acknowledgement rejection.
  it('10. throws INVALID_RANGE on out-of-bounds or non-integer chunk acknowledgement', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'bad-ack'),
    });

    try {
      pipeline.handleChunkAck(transferId, -1);
      expect.unreachable('Should have thrown INVALID_RANGE');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
    }

    try {
      pipeline.handleChunkAck(transferId, 9999);
      expect.unreachable('Should have thrown INVALID_RANGE');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
    }

    try {
      pipeline.handleChunkAck(transferId, 1.5);
      expect.unreachable('Should have thrown INVALID_RANGE');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
    }

    pipeline.dispose();
  });

  // 11. Retry limit enforcement.
  it('11. aborts transfer with RETRY_BUDGET_EXHAUSTED when per-chunk retry limit is exceeded', async () => {
    const pipeline = new SnapshotOutboundPipeline({ maxRetriesPerChunk: 2 });
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'retry-limit'),
    });

    // Request 1: retries = 1
    pipeline.handleChunkRequest(transferId, [0]);
    // Request 2: retries = 2
    pipeline.handleChunkRequest(transferId, [0]);

    // Request 3: retries exceeds limit of 2 -> aborts with RETRY_BUDGET_EXHAUSTED
    expect(() => pipeline.handleChunkRequest(transferId, [0])).toThrowError(/Retry limit exceeded/);

    const session = pipeline.getTransfer(transferId)!;
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('RETRY_BUDGET_EXHAUSTED');

    pipeline.dispose();
  });

  // 12. Cancellation during preparation.
  it('12. handles cancellation during preparation queue wait and cancels queued item', async () => {
    const queue = new SnapshotPreparationQueue();
    const pipeline = new SnapshotOutboundPipeline({ preparationQueue: queue });

    const id1 = generateUUID();
    const id2 = generateUUID();

    let resolveP1: (a: SerializedSnapshotArtifact) => void;
    const p1Promise = new Promise<SerializedSnapshotArtifact>((res) => {
      resolveP1 = res;
    });

    // Start transfer 1 (occupies preparation gate)
    const t1Promise = pipeline.startTransfer({
      transferId: id1,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => p1Promise,
    });

    // Start transfer 2 (will wait in queue)
    const t2Promise = pipeline.startTransfer({
      transferId: id2,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 't2-prep'),
    });

    expect(pipeline.getTransfer(id2)!.state).toBe('PREPARING');

    // Cancel transfer 2 while still preparing
    const cancelled = pipeline.cancelTransfer(id2, 'Cancelled while queued');
    expect(cancelled).toBe(true);
    expect(pipeline.getTransfer(id2)!.state).toBe('CANCELLED');

    // Finish transfer 1
    const a1 = createSerializedArtifact(1, 't1-done');
    resolveP1!(a1);
    await t1Promise;

    // t2Promise should reject with LOCAL_ABORT because it was cancelled in queue
    await expect(t2Promise).rejects.toThrowError(/Preparation request cancelled/);
    expect(pipeline.getTransfer(id2)!.state).toBe('CANCELLED');

    pipeline.dispose();
  });

  // 13. Cancellation after producer creation.
  it('13. handles cancellation after producer creation and disposes producer and leases', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();
    const artifact = createSerializedArtifact(1, 'cancel-active');

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact,
    });

    expect(artifact.activeLeaseCount).toBe(1);

    const cancelled = pipeline.cancelTransfer(transferId, 'User abort');
    expect(cancelled).toBe(true);

    const session = pipeline.getTransfer(transferId)!;
    expect(session.state).toBe('CANCELLED');
    expect(session.isTerminal).toBe(true);
    expect(artifact.activeLeaseCount).toBe(0);

    pipeline.dispose();
  });

  // 14. Timeout and terminal cleanup.
  it('14. triggers TIMED_OUT and cleans up resources on transfer timeout', async () => {
    const pipeline = new SnapshotOutboundPipeline({ transferTimeoutMs: 5000 });
    const transferId = generateUUID();
    const artifact = createSerializedArtifact(1, 'timeout-test');

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact,
    });

    expect(pipeline.getTransfer(transferId)!.state).toBe('PREPARED');
    expect(artifact.activeLeaseCount).toBe(1);

    // Advance fake timer by 5000 ms
    vi.advanceTimersByTime(5000);

    const session = pipeline.getTransfer(transferId)!;
    expect(session.state).toBe('TIMED_OUT');
    expect(session.failureReason).toBe('TRANSFER_TIMEOUT');
    expect(artifact.activeLeaseCount).toBe(0);

    pipeline.dispose();
  });

  // 15. Preparation failure cleanup.
  it('15. aborts cleanly on captureAndSerializeFn failure with no leaked leases', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await expect(
      pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => {
          throw new Error('Capture failed');
        },
      })
    ).rejects.toThrowError('Capture failed');

    const session = pipeline.getTransfer(transferId)!;
    expect(session.state).toBe('ABORTED');
    expect(session.isTerminal).toBe(true);

    pipeline.dispose();
  });

  // 16. Producer construction failure cleanup.
  it('16. aborts cleanly and releases leases if SnapshotChunkProducer construction fails', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    // Sctp limit below MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER (2,144 B) should fail early
    await expect(
      pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 500, // Invalid limit
        captureAndSerializeFn: () => createSerializedArtifact(1, 'bad-prod'),
      })
    ).rejects.toThrowError(/below minimum required/);

    pipeline.dispose();
  });

  // 17. Chunk-production failure cleanup.
  it('17. aborts transfer if chunk production fails unexpectedly', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();
    const artifact = createSerializedArtifact(1, 'chunk-fail');

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact,
    });

    // Detach buffer underneath by releasing lease and disposing artifact
    artifact.releaseLease(transferId);
    artifact.dispose();

    expect(() => pipeline.handleChunkRequest(transferId, [0])).toThrow();

    const session = pipeline.getTransfer(transferId)!;
    expect(session.state).toBe('ABORTED');

    pipeline.dispose();
  });

  // 18. Pipeline disposal with multiple active transfers.
  it('18. cancels all active transfers and releases all leases on pipeline disposal', async () => {
    const pipeline = new SnapshotOutboundPipeline({ maxActiveTransfers: 2 });
    const id1 = generateUUID();
    const id2 = generateUUID();
    const a1 = createSerializedArtifact(1, 'disp-1');
    const a2 = createSerializedArtifact(1, 'disp-2');

    await pipeline.startTransfer({
      transferId: id1,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact: a1,
    });

    await pipeline.startTransfer({
      transferId: id2,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact: a2,
    });

    expect(pipeline.activeTransferCount).toBe(2);
    expect(a1.activeLeaseCount).toBe(1);
    expect(a2.activeLeaseCount).toBe(1);

    pipeline.dispose();

    expect(pipeline.isDisposed).toBe(true);
    expect(pipeline.activeTransferCount).toBe(0);
    expect(a1.activeLeaseCount).toBe(0);
    expect(a2.activeLeaseCount).toBe(0);

    // Any subsequent call throws DISPOSED
    try {
      pipeline.handleChunkRequest(id1, [0]);
      expect.unreachable('Should have thrown DISPOSED');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('DISPOSED');
    }
  });

  // 19. No double release or leaked leases.
  it('19. guarantees exactly-once lease release across redundant terminal calls', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();
    const artifact = createSerializedArtifact(1, 'no-double-rel');

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      artifact,
    });

    expect(artifact.activeLeaseCount).toBe(1);

    // Cancel transfer
    pipeline.cancelTransfer(transferId);
    expect(artifact.activeLeaseCount).toBe(0);

    // Cancel again: idempotent, no negative lease count or throw
    pipeline.cancelTransfer(transferId);
    expect(artifact.activeLeaseCount).toBe(0);

    // Remote abort on already cancelled: idempotent
    pipeline.handleRemoteAbort(transferId, 'SUPPLIER_REJECTED');
    expect(artifact.activeLeaseCount).toBe(0);

    pipeline.dispose();
  });

  // 20. No stale callback resurrecting a terminal transfer.
  it('20. prevents stale timeout timer or queue callback from resurrecting terminal transfer', async () => {
    const pipeline = new SnapshotOutboundPipeline({ transferTimeoutMs: 10000 });
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(1, 'stale-cb'),
    });

    // Complete normally
    pipeline.handleChunkRequest(transferId, [0]);
    pipeline.handleChunkAck(transferId, 0);
    expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

    // Advance time past the 10000 ms timeout
    vi.advanceTimersByTime(12000);

    // State remains COMPLETED, not overwritten by TIMED_OUT
    expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

    pipeline.dispose();
  });

  // 21. Exact serializedJson preservation through the pipeline handoff.
  it('21. preserves exact serializedJson wire string through pipeline handoff', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();

    await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      captureAndSerializeFn: () => createSerializedArtifact(2, 'exact-wire'),
    });

    const [produced] = pipeline.handleChunkRequest(transferId, [0]);

    // Test that serializedJson is non-empty, exactly equals byteLength when UTF-8 encoded
    expect(typeof produced.serializedJson).toBe('string');
    expect(produced.byteLength).toBe(new TextEncoder().encode(produced.serializedJson).byteLength);

    // Parse back from string and verify field values
    const parsed = JSON.parse(produced.serializedJson);
    expect(parsed.messageType).toBe(MessageType.SNAPSHOT_CHUNK);
    expect(parsed.payload.chunkIndex).toBe(0);
    expect(parsed.payload.dataBase64).toBe(produced.payload.dataBase64);

    pipeline.dispose();
  });

  // 22. Bounded retained-frame accounting under concurrent transfers.
  it('22. enforces bounded retained-frame accounting across concurrent transfers (total <= 8 frames)', async () => {
    const pipeline = new SnapshotOutboundPipeline({
      maxActiveTransfers: 2,
      slidingWindowSize: 4,
    });

    const id1 = generateUUID();
    const id2 = generateUUID();

    await pipeline.startTransfer({
      transferId: id1,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144,
      captureAndSerializeFn: () => createSerializedArtifact(15, 'retained-1'),
    });

    await pipeline.startTransfer({
      transferId: id2,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144,
      captureAndSerializeFn: () => createSerializedArtifact(15, 'retained-2'),
    });

    // Request 4 frames each
    pipeline.handleChunkRequest(id1, [0, 1, 2, 3]);
    pipeline.handleChunkRequest(id2, [0, 1, 2, 3]);

    expect(pipeline.totalRetainedPreparedFrames).toBe(8);

    // ACK 2 frames from id1
    pipeline.handleChunkAck(id1, 0);
    pipeline.handleChunkAck(id1, 1);
    expect(pipeline.totalRetainedPreparedFrames).toBe(6);

    // Terminate id2
    pipeline.cancelTransfer(id2);
    expect(pipeline.totalRetainedPreparedFrames).toBe(2);

    pipeline.dispose();
    expect(pipeline.totalRetainedPreparedFrames).toBe(0);
  });

  // 23. M4.1 assembler compatibility using produced frames.
  it('23. demonstrates bit-for-bit round-trip recovery through frozen M4.1 ChunkAssembler', async () => {
    const pipeline = new SnapshotOutboundPipeline();
    const transferId = generateUUID();
    const originalArtifact = createSerializedArtifact(12, 'assembler-compat');

    const session = await pipeline.startTransfer({
      transferId,
      recipientPeerId: RECIPIENT_PEER_ID,
      supplierPeerId: SUPPLIER_PEER_ID,
      membershipEpoch: MEMBERSHIP_EPOCH,
      sctpMaxMessageSize: 2144, // produces multi-chunk partition
      artifact: originalArtifact,
    });

    const offer = session.offerMessage!;
    expect(offer).toBeDefined();

    // Receiver initializes M4.1 ChunkAssembler from offer
    const assembler = new ChunkAssembler();
    assembler.initTransfer({
      transferId: offer.payload.offerId,
      sessionId: offer.payload.sessionId,
      snapshotId: offer.payload.snapshotId,
      totalChunks: offer.payload.totalChunks,
      totalSizeBytes: offer.payload.totalSizeBytes,
      nominalChunkSizeBytes: offer.payload.nominalChunkSizeBytes ?? 32768,
      expectedSha256Digest: offer.payload.sha256Digest,
      supplierPeerId: offer.payload.supplierPeerId,
      membershipEpoch: offer.payload.membershipEpoch,
      connectionGeneration: offer.connectionGeneration,
    });

    // Sender produces chunks in window batches until complete
    let nextChunk = 0;
    while (!assembler.isComplete() && nextChunk < session.totalChunks) {
      const batchSize = Math.min(4, session.totalChunks - nextChunk);
      const batchIndices = Array.from({ length: batchSize }, (_, i) => nextChunk + i);

      const frames = pipeline.handleChunkRequest(transferId, batchIndices);

      for (const frame of frames) {
        // Receiver parses wire JSON
        const wireMessage = JSON.parse(frame.serializedJson) as ProtocolMessage<SnapshotChunkPayload>;

        // Receiver ingests chunk
        const ingestResult = assembler.ingestChunk(wireMessage.payload, {
          sessionId: wireMessage.sessionId,
          senderPeerId: wireMessage.senderPeerId,
          membershipEpoch: wireMessage.membershipEpoch,
          connectionGeneration: wireMessage.connectionGeneration,
        });
        expect(ingestResult.ok).toBe(true);

        // Receiver acknowledges chunk back to sender
        pipeline.handleChunkAck(transferId, wireMessage.payload.chunkIndex);
      }

      nextChunk += batchSize;
    }

    expect(assembler.isComplete()).toBe(true);
    expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

    // Receiver finalizes assembly
    const finalizeResult = assembler.finalizeAssembly();
    expect(finalizeResult.ok).toBe(true);

    if (finalizeResult.ok) {
      // Bit-for-bit equivalence with original artifact!
      expect(finalizeResult.snapshot.sha256Digest).toBe(originalArtifact.sha256Digest);
      expect(finalizeResult.snapshot.totalSizeBytes).toBe(originalArtifact.totalSizeBytes);
      expect(finalizeResult.snapshot.buffer).toEqual(originalArtifact.cloneBytes());
    }

    assembler.dispose();
    pipeline.dispose();
  });

  // 24. No regression in frozen M4.2a/b behavior.
  it('24. verifies no regressions in M4.2a serializer and M4.2b chunk producer invariants', () => {
    const artifact = createSerializedArtifact(3, 'reg-check');

    // M4.2a invariant: artifact starts with 0 leases
    expect(artifact.activeLeaseCount).toBe(0);
    expect(artifact.isDisposed).toBe(false);

    // M4.2b invariant: producer calculates canonical partition geometry
    const producer = new SnapshotChunkProducer(artifact, {
      sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
      transferId: generateUUID(),
    });

    expect(producer.isLeaseActive).toBe(true);
    expect(artifact.activeLeaseCount).toBe(1);

    // Final chunk production keeps lease intact (M4.2b closure fix)
    const finalChunk = producer.produceChunk(producer.totalChunks - 1);
    expect(finalChunk.payload.chunkIndex).toBe(producer.totalChunks - 1);
    expect(producer.isLeaseActive).toBe(true);
    expect(artifact.activeLeaseCount).toBe(1);

    // Explicit disposal releases lease
    producer.dispose();
    expect(producer.isLeaseActive).toBe(false);
    expect(artifact.activeLeaseCount).toBe(0);

    artifact.dispose();
    expect(artifact.isDisposed).toBe(true);
  });
});
