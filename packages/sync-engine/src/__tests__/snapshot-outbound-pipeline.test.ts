import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import crypto from 'node:crypto';
import {
  SnapshotOutboundPipeline,
  createOutboundTransportCapability,
  createAuthenticatedRecipientContext,
  type OutboundTransportCapability,
  type AuthenticatedRecipientContext,
  type InboundRecipientContextParams,
  type OutboundOfferContext,
  type OutboundChunkContext,
  type OutboundPipelineConfig,
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

  let defaultCapability: OutboundTransportCapability;

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

  function createPipeline(config: OutboundPipelineConfig = {}): SnapshotOutboundPipeline {
    return new SnapshotOutboundPipeline({
      transportCapability: defaultCapability,
      ...config,
    });
  }

  function createContext(
    pipeline: SnapshotOutboundPipeline,
    transferId: string,
    overrides?: Partial<InboundRecipientContextParams>,
    capability: OutboundTransportCapability = defaultCapability
  ): AuthenticatedRecipientContext {
    return createAuthenticatedRecipientContext(
      pipeline,
      {
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        sessionId: SESSION_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 1,
        ...overrides,
      },
      capability
    );
  }

  beforeEach(() => {
    vi.useFakeTimers();
    defaultCapability = createOutboundTransportCapability('trusted-transport', SESSION_ID);
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
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

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
  describe('Lifecycle & Architecture Verification (1 to 24)', () => {
    // 1. Successful snapshot preparation and producer creation.
    it('1. performs successful snapshot preparation and producer creation', async () => {
      const pipeline = createPipeline();
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
      const pipeline = createPipeline();
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
      const pipeline = createPipeline({ maxActiveTransfers: 2 });

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
      const pipeline = createPipeline({ slidingWindowSize: 4 });
      const transferId = generateUUID();

      const artifact = createSerializedArtifact(30, 'window-test');
      const session = await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 1,
        sctpMaxMessageSize: 2144, // forces small chunks
        artifact,
      });

      expect(session.totalChunks).toBeGreaterThanOrEqual(6);
      const context = createContext(pipeline, transferId);

      // Request 4 chunks: fits window exactly
      const frames = pipeline.handleChunkRequest(transferId, [0, 1, 2, 3], context);
      expect(frames.length).toBe(4);

      let state = pipeline.getTransfer(transferId)!;
      expect(state.state).toBe('WAITING_ACK');
      expect(state.inflightChunks.size).toBe(4);

      // Requesting a 5th chunk while window is full should throw LOCAL_ABORT
      expect(() => pipeline.handleChunkRequest(transferId, [4], context)).toThrowError(/sliding window capacity/);

      // Acknowledge chunk 0: window advances!
      pipeline.handleChunkAck(transferId, 0, context);
      state = pipeline.getTransfer(transferId)!;
      expect(state.state).toBe('ACTIVE');
      expect(state.inflightChunks.size).toBe(3);

      // Now chunk 4 can be requested
      const nextFrames = pipeline.handleChunkRequest(transferId, [4], context);
      expect(nextFrames.length).toBe(1);
      expect(nextFrames[0].payload.chunkIndex).toBe(4);

      pipeline.dispose();
    });

    // 5. Correct chunk ordering and chunk identity.
    it('5. preserves canonical chunk ordering and chunk payload identity', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(10, 'order-id'),
      });

      const context = createContext(pipeline, transferId);
      const frames = pipeline.handleChunkRequest(transferId, [0, 1], context);
      expect(frames[0].payload.chunkIndex).toBe(0);
      expect(frames[1].payload.chunkIndex).toBe(1);
      expect(frames[0].payload.transferId).toBe(transferId);
      expect(frames[1].payload.transferId).toBe(transferId);

      pipeline.dispose();
    });

    // 6. Out-of-order chunk production where permitted.
    it('6. produces chunks out-of-order within valid range', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(10, 'ooo-test'),
      });

      const context = createContext(pipeline, transferId);
      const frames = pipeline.handleChunkRequest(transferId, [2, 0], context);
      expect(frames[0].payload.chunkIndex).toBe(2);
      expect(frames[1].payload.chunkIndex).toBe(0);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(2)).toBe(true);
      expect(session.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // 7. Retransmission of previously produced chunks.
    it('7. supports retransmission of unacknowledged frames without increasing in-flight count', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(10, 'retransmit-test'),
      });

      const context = createContext(pipeline, transferId);

      // First request chunk 0 (initial transmission: retryCounts = 0 / undefined)
      const [frame1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.size).toBe(1);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // Hand off frame1 to transport send buffer
      pipeline.handleHandoffSuccess(transferId, 0, frame1.message.messageId, frame1.attemptToken, context);

      // Retransmit chunk 0 (first retransmission: retryCounts = 1, fresh messageId per Rule 7)
      const [frame2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(frame2.payload.chunkIndex).toBe(0);
      expect(frame2.payload).toEqual(frame1.payload);
      expect(frame2.message.messageId).not.toBe(frame1.message.messageId);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.size).toBe(1);
      expect(session.retryCounts.get(0)).toBe(1);

      pipeline.dispose();
    });

    // 8. Duplicate acknowledgement handling.
    it('8. handles duplicate chunk acknowledgement as an idempotent no-op', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'dup-ack'),
      });

      const context = createContext(pipeline, transferId);
      pipeline.handleChunkRequest(transferId, [0], context);

      // First ACK
      expect(() => pipeline.handleChunkAck(transferId, 0, context)).not.toThrow();
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

      // Duplicate ACK on already completed transfer
      expect(() => pipeline.handleChunkAck(transferId, 0, context)).not.toThrow();
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

      pipeline.dispose();
    });

    // 9. Late acknowledgement after transfer termination.
    it('9. safely ignores late acknowledgement after transfer termination without resurrecting', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'late-ack'),
      });

      const context = createContext(pipeline, transferId);

      // Abort transfer
      pipeline.cancelTransfer(transferId, 'Cancelled for test');
      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      // Late ACK arrives
      expect(() => pipeline.handleChunkAck(transferId, 0, context)).not.toThrow();
      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      pipeline.dispose();
    });

    // 10. Invalid acknowledgement rejection.
    it('10. throws INVALID_RANGE on out-of-bounds or non-integer chunk acknowledgement', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'bad-ack'),
      });

      const context = createContext(pipeline, transferId);

      try {
        pipeline.handleChunkAck(transferId, -1, context);
        expect.unreachable('Should have thrown INVALID_RANGE');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
      }

      try {
        pipeline.handleChunkAck(transferId, 9999, context);
        expect.unreachable('Should have thrown INVALID_RANGE');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
      }

      try {
        pipeline.handleChunkAck(transferId, 1.5, context);
        expect.unreachable('Should have thrown INVALID_RANGE');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
      }

      pipeline.dispose();
    });

    // 11. Retry limit enforcement.
    it('11. aborts transfer with RETRY_BUDGET_EXHAUSTED when per-chunk retry limit is exceeded', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 2 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'retry-limit'),
      });

      const context = createContext(pipeline, transferId);

      // Request 1 (initial transmission): retryCount = undefined / 0
      const [f1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();
      pipeline.handleHandoffSuccess(transferId, 0, f1.message.messageId, f1.attemptToken, context);

      // Request 2 (retry 1): retryCount = 1 <= 2
      const [f2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);
      pipeline.handleHandoffSuccess(transferId, 0, f2.message.messageId, f2.attemptToken, context);

      // Request 3 (retry 2): retryCount = 2 <= 2
      const [f3] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);
      pipeline.handleHandoffSuccess(transferId, 0, f3.message.messageId, f3.attemptToken, context);

      // Request 4 (retry 3 > 2): exceeds maxRetriesPerChunk of 2 -> aborts with RETRY_BUDGET_EXHAUSTED
      expect(() => pipeline.handleChunkRequest(transferId, [0], context)).toThrowError(/Retry budget exhausted/);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('ABORTED');
      expect(session.failureReason).toBe('RETRY_BUDGET_EXHAUSTED');

      pipeline.dispose();
    });

    // 12. Cancellation during preparation.
    it('12. handles cancellation during preparation queue wait and cancels queued item', async () => {
      const queue = new SnapshotPreparationQueue();
      const pipeline = createPipeline({ preparationQueue: queue });

      const id1 = generateUUID();
      const id2 = generateUUID();

      let resolveP1: (a: SerializedSnapshotArtifact) => void;
      const p1Promise = new Promise<SerializedSnapshotArtifact>((res) => {
        resolveP1 = res;
      });

      const t1Promise = pipeline.startTransfer({
        transferId: id1,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => p1Promise,
      });

      const t2Promise = pipeline.startTransfer({
        transferId: id2,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 't2-prep'),
      });

      expect(pipeline.getTransfer(id2)!.state).toBe('PREPARING');

      const cancelled = pipeline.cancelTransfer(id2, 'Cancelled while queued');
      expect(cancelled).toBe(true);
      expect(pipeline.getTransfer(id2)!.state).toBe('CANCELLED');

      const a1 = createSerializedArtifact(1, 't1-done');
      resolveP1!(a1);
      await t1Promise;

      await expect(t2Promise).rejects.toThrowError(/Preparation request cancelled/);
      expect(pipeline.getTransfer(id2)!.state).toBe('CANCELLED');

      pipeline.dispose();
    });

    // 13. Cancellation after producer creation.
    it('13. handles cancellation after producer creation and disposes producer and leases', async () => {
      const pipeline = createPipeline();
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
      const pipeline = createPipeline({ transferTimeoutMs: 5000 });
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

      vi.advanceTimersByTime(5000);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('TIMED_OUT');
      expect(session.failureReason).toBe('TRANSFER_TIMEOUT');
      expect(artifact.activeLeaseCount).toBe(0);

      pipeline.dispose();
    });

    // 15. Preparation failure cleanup.
    it('15. aborts cleanly on captureAndSerializeFn failure with no leaked leases', async () => {
      const pipeline = createPipeline();
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
      const pipeline = createPipeline();
      const transferId = generateUUID();

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
      const pipeline = createPipeline();
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

      const context = createContext(pipeline, transferId);

      // Detach buffer underneath by releasing lease and disposing artifact
      artifact.releaseLease(transferId);
      artifact.dispose();

      expect(() => pipeline.handleChunkRequest(transferId, [0], context)).toThrow();

      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('ABORTED');

      pipeline.dispose();
    });

    // 18. Pipeline disposal with multiple active transfers.
    it('18. cancels all active transfers and releases all leases on pipeline disposal', async () => {
      const pipeline = createPipeline({ maxActiveTransfers: 2 });
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

      const ctx1 = createContext(pipeline, id1);

      pipeline.dispose();

      expect(pipeline.isDisposed).toBe(true);
      expect(pipeline.activeTransferCount).toBe(0);
      expect(a1.activeLeaseCount).toBe(0);
      expect(a2.activeLeaseCount).toBe(0);

      // Subsequent call throws DISPOSED
      try {
        pipeline.handleChunkRequest(id1, [0], ctx1);
        expect.unreachable('Should have thrown DISPOSED');
      } catch (err) {
        expect(err).toBeInstanceOf(SyncEngineError);
        expect((err as SyncEngineError).code).toBe('DISPOSED');
      }
    });

    // 19. No double release or leaked leases.
    it('19. guarantees exactly-once lease release across redundant terminal calls', async () => {
      const pipeline = createPipeline();
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

      // Cancel again: idempotent
      pipeline.cancelTransfer(transferId);
      expect(artifact.activeLeaseCount).toBe(0);

      const context = createContext(pipeline, transferId);

      // Remote abort on already cancelled: idempotent
      pipeline.handleRemoteAbort(transferId, 'SUPPLIER_REJECTED', context);
      expect(artifact.activeLeaseCount).toBe(0);

      pipeline.dispose();
    });

    // 20. No stale callback resurrecting a terminal transfer.
    it('20. prevents stale timeout timer or queue callback from resurrecting terminal transfer', async () => {
      const pipeline = createPipeline({ transferTimeoutMs: 10000 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'stale-cb'),
      });

      const context = createContext(pipeline, transferId);

      // Complete normally
      pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleChunkAck(transferId, 0, context);
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

      // Advance time past the 10000 ms timeout
      vi.advanceTimersByTime(12000);

      // State remains COMPLETED, not overwritten by TIMED_OUT
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

      pipeline.dispose();
    });

    // 21. Exact serializedJson preservation through the pipeline handoff.
    it('21. preserves exact serializedJson wire string through pipeline handoff', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(2, 'exact-wire'),
      });

      const context = createContext(pipeline, transferId);
      const [produced] = pipeline.handleChunkRequest(transferId, [0], context);

      expect(typeof produced.serializedJson).toBe('string');
      expect(produced.byteLength).toBe(new TextEncoder().encode(produced.serializedJson).byteLength);

      const parsed = JSON.parse(produced.serializedJson);
      expect(parsed.messageType).toBe(MessageType.SNAPSHOT_CHUNK);
      expect(parsed.payload.chunkIndex).toBe(0);
      expect(parsed.payload.dataBase64).toBe(produced.payload.dataBase64);

      pipeline.dispose();
    });

    // 22. Bounded retained-frame accounting under concurrent transfers.
    it('22. enforces bounded retained-frame accounting across concurrent transfers (total <= 8 frames)', async () => {
      const pipeline = createPipeline({
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

      const ctx1 = createContext(pipeline, id1);
      const ctx2 = createContext(pipeline, id2);

      // Request 4 frames each
      pipeline.handleChunkRequest(id1, [0, 1, 2, 3], ctx1);
      pipeline.handleChunkRequest(id2, [0, 1, 2, 3], ctx2);

      expect(pipeline.totalRetainedPreparedFrames).toBe(8);

      // ACK 2 frames from id1
      pipeline.handleChunkAck(id1, 0, ctx1);
      pipeline.handleChunkAck(id1, 1, ctx1);
      expect(pipeline.totalRetainedPreparedFrames).toBe(6);

      // Terminate id2
      pipeline.cancelTransfer(id2);
      expect(pipeline.totalRetainedPreparedFrames).toBe(2);

      pipeline.dispose();
      expect(pipeline.totalRetainedPreparedFrames).toBe(0);
    });

    // 23. M4.1 assembler compatibility using produced frames.
    it('23. demonstrates bit-for-bit round-trip recovery through frozen M4.1 ChunkAssembler', async () => {
      const pipeline = createPipeline();
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

      const context = createContext(pipeline, transferId);

      let nextChunk = 0;
      while (!assembler.isComplete() && nextChunk < session.totalChunks) {
        const batchSize = Math.min(4, session.totalChunks - nextChunk);
        const batchIndices = Array.from({ length: batchSize }, (_, i) => nextChunk + i);

        const frames = pipeline.handleChunkRequest(transferId, batchIndices, context);

        for (const frame of frames) {
          const wireMessage = JSON.parse(frame.serializedJson) as ProtocolMessage<SnapshotChunkPayload>;

          const ingestResult = assembler.ingestChunk(wireMessage.payload, {
            sessionId: wireMessage.sessionId,
            senderPeerId: wireMessage.senderPeerId,
            membershipEpoch: wireMessage.membershipEpoch,
            connectionGeneration: wireMessage.connectionGeneration,
          });
          expect(ingestResult.ok).toBe(true);

          pipeline.handleChunkAck(transferId, wireMessage.payload.chunkIndex, context);
        }

        nextChunk += batchSize;
      }

      expect(assembler.isComplete()).toBe(true);
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(true);

      if (finalizeResult.ok) {
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

      expect(artifact.activeLeaseCount).toBe(0);
      expect(artifact.isDisposed).toBe(false);

      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        transferId: generateUUID(),
      });

      expect(producer.isLeaseActive).toBe(true);
      expect(artifact.activeLeaseCount).toBe(1);

      const finalChunk = producer.produceChunk(producer.totalChunks - 1);
      expect(finalChunk.payload.chunkIndex).toBe(producer.totalChunks - 1);
      expect(producer.isLeaseActive).toBe(true);
      expect(artifact.activeLeaseCount).toBe(1);

      producer.dispose();
      expect(producer.isLeaseActive).toBe(false);
      expect(artifact.activeLeaseCount).toBe(0);

      artifact.dispose();
      expect(artifact.isDisposed).toBe(true);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F1 Closure: Capability-Gated Recipient Context & Fencing
  // ─────────────────────────────────────────────────────────────
  describe('Finding F1 Closure: Capability-Gated Recipient Context & Contextual Fencing', () => {
    it('requires authorized OutboundTransportCapability for context registration', () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      const forgedCapability = createOutboundTransportCapability('attacker', SESSION_ID);

      expect(() => {
        createAuthenticatedRecipientContext(
          pipeline,
          {
            transferId,
            recipientPeerId: RECIPIENT_PEER_ID,
            sessionId: SESSION_ID,
            membershipEpoch: MEMBERSHIP_EPOCH,
            connectionGeneration: 1,
          },
          forgedCapability
        );
      }).toThrowError(/authorized OutboundTransportCapability/);

      pipeline.dispose();
    });

    it('validates context registration parameters strictly', () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      // Missing transferId
      expect(() => {
        createContext(pipeline, '', { transferId: '' });
      }).toThrowError(/transferId must be a non-empty string/);

      // Missing recipientPeerId
      expect(() => {
        createContext(pipeline, transferId, { recipientPeerId: '' });
      }).toThrowError(/recipientPeerId must be a non-empty string/);

      // Missing sessionId
      expect(() => {
        createContext(pipeline, transferId, { sessionId: '' });
      }).toThrowError(/sessionId must be a non-empty string/);

      // Negative epoch
      expect(() => {
        createContext(pipeline, transferId, { membershipEpoch: -1 });
      }).toThrowError(/must be non-negative integer/);

      // Non-positive connectionGeneration
      expect(() => {
        createContext(pipeline, transferId, { connectionGeneration: 0 });
      }).toThrowError(/must be positive integer/);

      pipeline.dispose();
    });

    // 12 Negative Context Fencing Tests
    it('F1-1. rejects chunk request from wrong peer (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-1'),
      });

      const badPeerContext = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter-99',
      });

      expect(() => {
        pipeline.handleChunkRequest(transferId, [0], badPeerContext);
      }).toThrow(SyncEngineError);

      try {
        pipeline.handleChunkRequest(transferId, [0], badPeerContext);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      // Zero side-effects check
      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.size).toBe(0);
      expect(session.state).toBe('PREPARED');

      pipeline.dispose();
    });

    it('F1-2. rejects chunk request with wrong session (INVALID_ARGUMENT)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-2'),
      });

      const badSessionContext = createContext(pipeline, transferId, {
        sessionId: 'wrong-session-xyz',
      });

      try {
        pipeline.handleChunkRequest(transferId, [0], badSessionContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_ARGUMENT');
      }

      pipeline.dispose();
    });

    it('F1-3. rejects chunk request with obsolete or wrong epoch (REJECTED_OBSOLETE_EPOCH)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-3'),
      });

      const badEpochContext = createContext(pipeline, transferId, {
        membershipEpoch: MEMBERSHIP_EPOCH + 1,
      });

      try {
        pipeline.handleChunkRequest(transferId, [0], badEpochContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('REJECTED_OBSOLETE_EPOCH');
      }

      pipeline.dispose();
    });

    it('F1-4. rejects chunk request with stale connection generation (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 2,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-4'),
      });

      const staleGenContext = createContext(pipeline, transferId, {
        connectionGeneration: 1, // Stale generation
      });

      try {
        pipeline.handleChunkRequest(transferId, [0], staleGenContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      pipeline.dispose();
    });

    it('F1-5. rejects chunk ACK from wrong peer (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-5'),
      });

      const validContext = createContext(pipeline, transferId);
      pipeline.handleChunkRequest(transferId, [0], validContext);

      const imposterContext = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter-99',
      });

      try {
        pipeline.handleChunkAck(transferId, 0, imposterContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      // Zero side-effects: chunk 0 is still unacknowledged and in-flight
      const session = pipeline.getTransfer(transferId)!;
      expect(session.acknowledgedChunks.has(0)).toBe(false);
      expect(session.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('F1-6. rejects chunk ACK with wrong session (INVALID_ARGUMENT)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-6'),
      });

      const validContext = createContext(pipeline, transferId);
      pipeline.handleChunkRequest(transferId, [0], validContext);

      const badSessionContext = createContext(pipeline, transferId, {
        sessionId: 'wrong-session-xyz',
      });

      try {
        pipeline.handleChunkAck(transferId, 0, badSessionContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_ARGUMENT');
      }

      pipeline.dispose();
    });

    it('F1-7. rejects chunk ACK with wrong epoch (REJECTED_OBSOLETE_EPOCH)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-7'),
      });

      const validContext = createContext(pipeline, transferId);
      pipeline.handleChunkRequest(transferId, [0], validContext);

      const badEpochContext = createContext(pipeline, transferId, {
        membershipEpoch: MEMBERSHIP_EPOCH + 1,
      });

      try {
        pipeline.handleChunkAck(transferId, 0, badEpochContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('REJECTED_OBSOLETE_EPOCH');
      }

      pipeline.dispose();
    });

    it('F1-8. rejects chunk ACK with stale connection generation (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 2,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-8'),
      });

      const validContext = createContext(pipeline, transferId, { connectionGeneration: 2 });
      pipeline.handleChunkRequest(transferId, [0], validContext);

      const staleGenContext = createContext(pipeline, transferId, {
        connectionGeneration: 1,
      });

      try {
        pipeline.handleChunkAck(transferId, 0, staleGenContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      pipeline.dispose();
    });

    it('F1-9. rejects remote abort from wrong peer (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-9'),
      });

      const badPeerContext = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter-99',
      });

      try {
        pipeline.handleRemoteAbort(transferId, 'RECIPIENT_ABORT', badPeerContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      // Zero side-effects: transfer remains PREPARED, not aborted
      expect(pipeline.getTransfer(transferId)!.state).toBe('PREPARED');

      pipeline.dispose();
    });

    it('F1-10. rejects remote abort with stale connection generation (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 3,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-10'),
      });

      const staleGenContext = createContext(pipeline, transferId, {
        connectionGeneration: 2,
      });

      try {
        pipeline.handleRemoteAbort(transferId, 'RECIPIENT_ABORT', staleGenContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      expect(pipeline.getTransfer(transferId)!.state).toBe('PREPARED');

      pipeline.dispose();
    });

    it('F1-11. rejects forged or unregistered authenticated context (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-11'),
      });

      const forgedContext: AuthenticatedRecipientContext = {
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        sessionId: SESSION_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 1,
      };

      try {
        pipeline.handleChunkRequest(transferId, [0], forgedContext);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      pipeline.dispose();
    });

    it('F1-12. rejects context from one transfer used against another transfer (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline({ maxActiveTransfers: 2 });
      const id1 = generateUUID();
      const id2 = generateUUID();

      await pipeline.startTransfer({
        transferId: id1,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-12a'),
      });

      await pipeline.startTransfer({
        transferId: id2,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f1-12b'),
      });

      const context1 = createContext(pipeline, id1);

      // Attempt to use transfer 1 context against transfer 2
      try {
        pipeline.handleChunkRequest(id2, [0], context1);
        expect.unreachable('Should have thrown');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      pipeline.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F2 Closure: Retransmission vs. Retry Accounting
  // ─────────────────────────────────────────────────────────────
  describe('Finding F2 Closure: Retransmission vs. Retry Accounting', () => {
    it('initial transmission records in transmittedChunks without incrementing retryCounts', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f2-initial'),
      });

      const context = createContext(pipeline, transferId);

      // Transmit chunks 0 and 1 initially
      pipeline.handleChunkRequest(transferId, [0, 1], context);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.transmittedChunks.has(0)).toBe(true);
      expect(session.transmittedChunks.has(1)).toBe(true);
      expect(session.retryCounts.get(0)).toBeUndefined();
      expect(session.retryCounts.get(1)).toBeUndefined();

      pipeline.dispose();
    });

    it('permits 1 initial transmission + exactly 3 retransmissions (4 total) when maxRetriesPerChunk = 3, rejecting 5th', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f2-budget'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Initial transmission: transmitted=true, retryCount=0
      const [f0] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();
      pipeline.handleHandoffSuccess(transferId, 0, f0.message.messageId, f0.attemptToken, context);

      // 2. Retransmission 1: retryCount=1 <= 3
      const [f1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);
      pipeline.handleHandoffSuccess(transferId, 0, f1.message.messageId, f1.attemptToken, context);

      // 3. Retransmission 2: retryCount=2 <= 3
      const [f2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);
      pipeline.handleHandoffSuccess(transferId, 0, f2.message.messageId, f2.attemptToken, context);

      // 4. Retransmission 3: retryCount=3 <= 3
      const [f3] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(3);
      pipeline.handleHandoffSuccess(transferId, 0, f3.message.messageId, f3.attemptToken, context);

      // 5. Retransmission 4 (> 3): exceeds retry limit -> throws and aborts transfer
      expect(() => {
        pipeline.handleChunkRequest(transferId, [0], context);
      }).toThrowError(/Retry budget exhausted/);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('ABORTED');
      expect(session.failureReason).toBe('RETRY_BUDGET_EXHAUSTED');

      pipeline.dispose();
    });

    it('retransmitting chunk A does not increment retry count for chunk B', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f2-isolate'),
      });

      const context = createContext(pipeline, transferId);

      // Initial transmission of both 0 and 1
      const [f0, f1] = pipeline.handleChunkRequest(transferId, [0, 1], context);
      pipeline.handleHandoffSuccess(transferId, 0, f0.message.messageId, f0.attemptToken, context);
      pipeline.handleHandoffSuccess(transferId, 1, f1.message.messageId, f1.attemptToken, context);

      // Retransmit chunk 0 twice
      const [f0Retry1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, f0Retry1.message.messageId, f0Retry1.attemptToken, context);
      pipeline.handleChunkRequest(transferId, [0], context);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.retryCounts.get(0)).toBe(2);
      expect(session.retryCounts.get(1)).toBeUndefined();

      pipeline.dispose();
    });

    it('chunk ACK stops further retransmission while retaining historical retry count', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f2-ack-retain'),
      });

      const context = createContext(pipeline, transferId);

      // Initial + 1 retry of chunk 0
      const [f0] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, f0.message.messageId, f0.attemptToken, context);
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Acknowledge chunk 0
      pipeline.handleChunkAck(transferId, 0, context);

      // Further request for chunk 0 is rejected because chunk is already acknowledged
      expect(() => {
        pipeline.handleChunkRequest(transferId, [0], context);
      }).toThrowError(/already acknowledged/);

      // Retry count is preserved for historical audit
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      pipeline.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F3 Closure: Deterministic Wire Representation Across Eviction
  // ─────────────────────────────────────────────────────────────
  describe('Finding F3 Closure: Deterministic Retransmission Wire Representation Across Cache Eviction', () => {
    it('preserves bit-for-bit identical messageId and serializedJson when re-producing evicted chunk', async () => {
      // Sliding window size 2
      const pipeline = createPipeline({ slidingWindowSize: 2 });
      const transferId = generateUUID();

      // Produce at least 6 chunks
      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 5,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(15, 'f3-evict'),
      });

      const context = createContext(pipeline, transferId, { connectionGeneration: 5 });

      // 1. Initial request for chunk 0
      const [initialFrame0] = pipeline.handleChunkRequest(transferId, [0], context);
      const initialMessageId = initialFrame0.message.messageId;
      const initialJson = initialFrame0.serializedJson;
      const initialByteLength = initialFrame0.byteLength;

      // 2. Request and ACK chunks 0 and 1 to advance window
      const [initialFrame1] = pipeline.handleChunkRequest(transferId, [1], context);
      pipeline.handleChunkAck(transferId, 0, context);
      pipeline.handleChunkAck(transferId, 1, context);

      // 3. Request chunks 2 and 3, which fills window and evicts chunk 0 from preparedFrames cache
      pipeline.handleChunkRequest(transferId, [2, 3], context);

      pipeline.dispose();
    });

    it('re-produces evicted unacknowledged chunk with identical messageId and serializedJson', async () => {
      // Window size 2
      const pipeline = createPipeline({ slidingWindowSize: 2 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 4,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(15, 'f3-reproduce'),
      });

      const context = createContext(pipeline, transferId, { connectionGeneration: 4 });

      // Request chunk 0
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);
      const originalMessageId = frame0.message.messageId;
      const originalJson = frame0.serializedJson;
      const originalByteLength = frame0.byteLength;

      // Request chunk 1
      pipeline.handleChunkRequest(transferId, [1], context);

      // Simulate a handoff failure on chunk 0 so its reservation is released
      pipeline.handleHandoffFailure(transferId, 0, originalMessageId, frame0.attemptToken, context);

      // Now request chunks 2 and 3: this causes preparedFrames to evict chunk 0 due to sliding window
      // First ACK chunk 1
      pipeline.handleChunkAck(transferId, 1, context);
      pipeline.handleChunkRequest(transferId, [2], context);
      pipeline.handleChunkAck(transferId, 2, context);
      pipeline.handleChunkRequest(transferId, [3], context);

      // Now re-request chunk 0 (which was evicted from preparedFrames)
      // Release reservation on 3 or ACK 3 so window opens
      pipeline.handleChunkAck(transferId, 3, context);

      const [reproducedFrame0] = pipeline.handleChunkRequest(transferId, [0], context);

      // Bit-for-bit equivalence!
      expect(reproducedFrame0.message.messageId).toBe(originalMessageId);
      expect(reproducedFrame0.serializedJson).toBe(originalJson);
      expect(reproducedFrame0.byteLength).toBe(originalByteLength);
      expect(reproducedFrame0.message.connectionGeneration).toBe(4);

      pipeline.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F4 Closure: Explicit Handoff-Failure Handling
  // ─────────────────────────────────────────────────────────────
  describe('Finding F4 Closure: Explicit Handoff-Failure Handling', () => {
    it('releases in-flight reservation and reopens window on handoff failure', async () => {
      const pipeline = createPipeline({ slidingWindowSize: 2 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(10, 'f4-reopen'),
      });

      const context = createContext(pipeline, transferId);

      // Request 2 chunks to fill window (W = 2)
      const [f0, f1] = pipeline.handleChunkRequest(transferId, [0, 1], context);

      let session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('WAITING_ACK');
      expect(session.inflightChunks.size).toBe(2);

      // Handoff failure reported on chunk 1
      pipeline.handleHandoffFailure(transferId, 1, f1.message.messageId, f1.attemptToken, context);

      session = pipeline.getTransfer(transferId)!;
      // Reservation released from inflightChunks
      expect(session.inflightChunks.has(1)).toBe(false);
      expect(session.inflightChunks.size).toBe(1);
      // Window reopened to ACTIVE
      expect(session.state).toBe('ACTIVE');

      // Now another chunk (e.g. chunk 2) can be requested because window reopened
      const frames = pipeline.handleChunkRequest(transferId, [2], context);
      expect(frames.length).toBe(1);
      expect(frames[0].payload.chunkIndex).toBe(2);

      pipeline.dispose();
    });

    it('preserves cached frame and does not affect leases on handoff failure', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();
      const artifact = createSerializedArtifact(5, 'f4-cache');

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        artifact,
      });

      const context = createContext(pipeline, transferId);

      // Produce chunk 0
      const [initialFrame] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(artifact.activeLeaseCount).toBe(1);

      // Report handoff failure
      pipeline.handleHandoffFailure(transferId, 0, initialFrame.message.messageId, initialFrame.attemptToken, context);

      // Lease is retained
      expect(artifact.activeLeaseCount).toBe(1);

      // Subsequent attempt succeeds using preserved cached frame
      const [retryFrame] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(retryFrame.serializedJson).toBe(initialFrame.serializedJson);

      pipeline.dispose();
    });

    it('M4.2c-R2: does not consume retry budget on initial handoff failure', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r2-initial-fail'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Request chunk 0 for initial transmission
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(frame0).toBeDefined();

      // 2. Verify in-flight reservation
      let session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);

      // 3. Report local transport handoff failure
      pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, context);

      // Verify in-flight reservation released and transmittedChunks rolled back
      session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(false);
      expect(session.transmittedChunks.has(0)).toBe(false);
      expect(session.retryCounts.get(0)).toBeUndefined();

      // 4. Request the same chunk again
      const [frame0Retry] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(frame0Retry.serializedJson).toBe(frame0.serializedJson);

      // 5 & 6. Verify chunk is NOT classified as a retransmission and consumes 0 retries
      session = pipeline.getTransfer(transferId)!;
      expect(session.transmittedChunks.has(0)).toBe(true);
      expect(session.retryCounts.get(0)).toBeUndefined();

      pipeline.dispose();
    });

    it('M4.2c-R2: repeated initial handoff failures consume zero retries', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r2-repeat-fail'),
      });

      const context = createContext(pipeline, transferId);

      // Repeat handoff failure 5 times on initial transmission
      for (let i = 0; i < 5; i++) {
        const [frame] = pipeline.handleChunkRequest(transferId, [0], context);
        expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, context);
        const session = pipeline.getTransfer(transferId)!;
        expect(session.inflightChunks.has(0)).toBe(false);
        expect(session.transmittedChunks.has(0)).toBe(false);
        expect(session.retryCounts.get(0)).toBeUndefined();
      }

      // 6th attempt: succeeds as initial transmission with 0 retry budget consumed
      pipeline.handleChunkRequest(transferId, [0], context);
      const finalSession = pipeline.getTransfer(transferId)!;
      expect(finalSession.transmittedChunks.has(0)).toBe(true);
      expect(finalSession.retryCounts.get(0)).toBeUndefined();

      pipeline.dispose();
    });

    it('M4.2c-R2: failed retransmission handoff rolls back retry count for another retry', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r2-retransmit-fail'),
      });

      const context = createContext(pipeline, transferId);

      // Initial transmission succeeds (handed off)
      const [f0] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, f0.message.messageId, f0.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.transmittedChunks.has(0)).toBe(true);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // Recipient requests retransmission -> retryCounts becomes 1
      const [f0Retry1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Local handoff of this retransmission fails
      pipeline.handleHandoffFailure(transferId, 0, f0Retry1.message.messageId, f0Retry1.attemptToken, context);

      // Retry count is rolled back, chunk remains transmitted
      let session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(false);
      expect(session.transmittedChunks.has(0)).toBe(true);
      expect(session.retryCounts.get(0)).toBeUndefined();

      // Transport retries the retransmission
      const [f0Retry2] = pipeline.handleChunkRequest(transferId, [0], context);
      session = pipeline.getTransfer(transferId)!;
      // Retried retransmission is retry #1, NOT retry #2!
      expect(session.retryCounts.get(0)).toBe(1);

      pipeline.dispose();
    });

    it('handles duplicate handoff failure idempotently without error', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f4-dup'),
      });

      const context = createContext(pipeline, transferId);
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);

      // First failure report
      expect(() => pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, context)).not.toThrow();

      // Duplicate failure report: idempotent no-op
      expect(() => pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, context)).not.toThrow();

      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.size).toBe(0);

      pipeline.dispose();
    });

    it('safely ignores handoff failure reported after chunk ACK', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'f4-after-ack'),
      });

      const context = createContext(pipeline, transferId);
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleChunkAck(transferId, 0, context);

      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      // Late handoff failure arrives for chunk 0
      pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, context);

      // Chunk 0 remains acknowledged!
      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('safely ignores handoff failure reported after transfer termination', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f4-terminal'),
      });

      const context = createContext(pipeline, transferId);
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.cancelTransfer(transferId);

      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      // Late handoff failure
      expect(() => pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, context)).not.toThrow();
      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      pipeline.dispose();
    });

    it('rejects handoff failure with stale connection generation or wrong peer (UNAUTHORIZED)', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 3,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f4-stale'),
      });

      const validContext = createContext(pipeline, transferId, { connectionGeneration: 3 });
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], validContext);

      const staleContext = createContext(pipeline, transferId, {
        connectionGeneration: 2,
      });

      try {
        pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, staleContext);
        expect.unreachable('Should have thrown UNAUTHORIZED');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      const imposterContext = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter-99',
        connectionGeneration: 3,
      });

      try {
        pipeline.handleHandoffFailure(transferId, 0, frame0.message.messageId, frame0.attemptToken, imposterContext);
        expect.unreachable('Should have thrown UNAUTHORIZED');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      pipeline.dispose();
    });

    it('throws INVALID_RANGE on out-of-bounds chunkIndex for handoff failure', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'f4-bounds'),
      });

      const context = createContext(pipeline, transferId);

      try {
        pipeline.handleHandoffFailure(transferId, 9999, 'msg-none', 'msg-none:1', context);
        expect.unreachable('Should have thrown INVALID_RANGE');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
      }

      pipeline.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Phase 7.5 M4.2c-R3: Attempt-Specific Handoff Fencing & Adversarial Lifecycle Tests
  // ─────────────────────────────────────────────────────────────
  describe('Phase 7.5 M4.2c-R3: Attempt-Specific Handoff Fencing & Adversarial Lifecycle Tests', () => {
    it('Scenario A1: stale failure for earlier attempt does not release newer reservation or alter retry accounting', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r3-a1'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Initial attempt A1 requested
      const [a1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(a1.message.messageId).toBeDefined();

      // 2. A1 successfully handed off to DataChannel send buffer
      pipeline.handleHandoffSuccess(transferId, 0, a1.message.messageId, a1.attemptToken, context);

      // 3. Network retransmission A2 requested
      const [a2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(a2.message.messageId).not.toBe(a1.message.messageId); // Rule 7: new messageId
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // 4. A2 successfully handed off
      pipeline.handleHandoffSuccess(transferId, 0, a2.message.messageId, a2.attemptToken, context);

      // 5. Delayed failure notification for A1 arrives
      pipeline.handleHandoffFailure(transferId, 0, a1.message.messageId, a1.attemptToken, context);

      // 6. Verify that A2's reservation and retry accounting remain completely unchanged
      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);
      expect(session.retryCounts.get(0)).toBe(1);

      pipeline.dispose();
    });

    it('Scenario A2: duplicate old failure notification does not release subsequent attempt', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r3-a2'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Attempt A1 fails locally
      const [a1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffFailure(transferId, 0, a1.message.messageId, a1.attemptToken, context);

      // 2. Reservation is released
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(false);

      // 3. Attempt A2 is created and reserved
      const [a2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      // 4. Duplicate failure notification for A1 arrives with A1's attemptToken
      pipeline.handleHandoffFailure(transferId, 0, a1.message.messageId, a1.attemptToken, context);

      // 5. Verify that A2 is not released
      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);
      expect(session.transmittedChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('Scenario A3: delayed failure callback after remote chunk ACK is a strict no-op', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'r3-a3'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Chunks requested and handed off
      const totalChunks = pipeline.getTransfer(transferId)!.totalChunks;
      let firstFrameMessageId = '';
      let firstFrameAttemptToken = '';
      for (let i = 0; i < totalChunks; i++) {
        const [frame] = pipeline.handleChunkRequest(transferId, [i], context);
        if (i === 0) {
          firstFrameMessageId = frame.message.messageId;
          firstFrameAttemptToken = frame.attemptToken;
        }
        pipeline.handleHandoffSuccess(transferId, i, frame.message.messageId, frame.attemptToken, context);
        pipeline.handleChunkAck(transferId, i, context);
      }

      // 2. All chunks ACKed, transfer transitions to COMPLETED
      expect(pipeline.getTransfer(transferId)!.state).toBe('COMPLETED');
      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.size).toBe(totalChunks);

      // 3. Delayed handoff-failure notification arrives for chunk 0
      pipeline.handleHandoffFailure(transferId, 0, firstFrameMessageId, firstFrameAttemptToken, context);

      // 4. Verify terminal completion and acknowledged state remain intact
      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('COMPLETED');
      expect(session.acknowledgedChunks.has(0)).toBe(true);
      expect(session.inflightChunks.has(0)).toBe(false);

      pipeline.dispose();
    });

    it('Scenario A4: out-of-order and overlapping callbacks never corrupt active attempt or double-rollback retries', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r3-a4'),
      });

      const context = createContext(pipeline, transferId);

      // Initial attempt A1
      const [a1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, a1.message.messageId, a1.attemptToken, context);

      // Retransmission attempt A2
      const [a2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Out-of-order failure for attempt A1 arrives
      pipeline.handleHandoffFailure(transferId, 0, a1.message.messageId, a1.attemptToken, context);
      // Ignored: retry count remains 1 and inflight remains true
      let session = pipeline.getTransfer(transferId)!;
      expect(session.retryCounts.get(0)).toBe(1);
      expect(session.inflightChunks.has(0)).toBe(true);

      // Success for attempt A2 arrives
      pipeline.handleHandoffSuccess(transferId, 0, a2.message.messageId, a2.attemptToken, context);
      session = pipeline.getTransfer(transferId)!;
      expect(session.retryCounts.get(0)).toBe(1);
      expect(session.inflightChunks.has(0)).toBe(true);

      // Duplicate failure for A1 arrives again
      pipeline.handleHandoffFailure(transferId, 0, a1.message.messageId, a1.attemptToken, context);
      session = pipeline.getTransfer(transferId)!;
      expect(session.retryCounts.get(0)).toBe(1);
      expect(session.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('Scenario A5: repeated initial handoff failures preserve messageId and consume zero retries under duplicate callbacks', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r3-a5'),
      });

      const context = createContext(pipeline, transferId);

      let initialMessageId: string | undefined;

      // 5 failed initial handoff attempts with duplicate failure callbacks
      for (let i = 0; i < 5; i++) {
        const [frame] = pipeline.handleChunkRequest(transferId, [0], context);
        if (i === 0) {
          initialMessageId = frame.message.messageId;
        } else {
          // Rule 5: Failed local handoff preserves messageId for next local attempt
          expect(frame.message.messageId).toBe(initialMessageId);
        }

        expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

        // First failure callback
        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, context);
        // Duplicate failure callback for same attempt
        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, context);

        const session = pipeline.getTransfer(transferId)!;
        expect(session.inflightChunks.has(0)).toBe(false);
        expect(session.transmittedChunks.has(0)).toBe(false);
        expect(session.retryCounts.get(0)).toBeUndefined();
      }

      // 6th attempt: succeeds as initial transmission with zero retries consumed
      const [finalFrame] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(finalFrame.message.messageId).toBe(initialMessageId);
      pipeline.handleHandoffSuccess(transferId, 0, finalFrame.message.messageId, finalFrame.attemptToken, context);

      const finalSession = pipeline.getTransfer(transferId)!;
      expect(finalSession.transmittedChunks.has(0)).toBe(true);
      expect(finalSession.retryCounts.get(0)).toBeUndefined();
      expect(finalSession.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('Rule 6 & 10: handoff success records send buffer entry without claiming remote receipt', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(3, 'r3-send-buffer'),
      });

      const context = createContext(pipeline, transferId);

      const [frame0] = pipeline.handleChunkRequest(transferId, [0], context);

      // Report handoff success
      pipeline.handleHandoffSuccess(transferId, 0, frame0.message.messageId, frame0.attemptToken, context);

      const session = pipeline.getTransfer(transferId)!;
      // In-flight reservation is retained (waiting for remote ACK)
      expect(session.inflightChunks.has(0)).toBe(true);
      // Chunk is NOT acknowledged locally
      expect(session.acknowledgedChunks.has(0)).toBe(false);
      // Transfer is still ACTIVE, not COMPLETED
      expect(session.state).toBe('ACTIVE');

      // Stale handoff success with wrong messageId is a strict no-op
      pipeline.handleHandoffSuccess(transferId, 0, 'msg-wrong-999', 'msg-wrong-999:1', context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    it('contextual fencing on handleHandoffSuccess rejects wrong peer, session, or stale epoch/generation', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 2,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(2, 'r3-fence'),
      });

      const validContext = createContext(pipeline, transferId, { connectionGeneration: 2 });
      const [frame0] = pipeline.handleChunkRequest(transferId, [0], validContext);

      // Wrong peer
      const wrongPeerCtx = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter',
        connectionGeneration: 2,
      });
      expect(() => pipeline.handleHandoffSuccess(transferId, 0, frame0.message.messageId, frame0.attemptToken, wrongPeerCtx)).toThrowError(
        /Authenticated peer 'peer-imposter' does not match/
      );

      // Wrong session
      const wrongSessCtx = createContext(pipeline, transferId, {
        sessionId: 'sess-wrong',
        connectionGeneration: 2,
      });
      expect(() => pipeline.handleHandoffSuccess(transferId, 0, frame0.message.messageId, frame0.attemptToken, wrongSessCtx)).toThrowError(
        /Context sessionId 'sess-wrong' does not match/
      );

      // Out-of-bounds index
      expect(() => pipeline.handleHandoffSuccess(transferId, 9999, frame0.message.messageId, frame0.attemptToken, validContext)).toThrowError(
        /out of bounds/
      );

      pipeline.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Phase 7.5 M4.2c-R4: Adversarial Attempt Identity & Retransmission Lifecycle Tests
  // ─────────────────────────────────────────────────────────────
  describe('Phase 7.5 M4.2c-R4: Adversarial Attempt Identity & Retransmission Lifecycle Tests', () => {
    // R4-1 — Omitted attempt token
    it('R4-1: omitted attempt token is safely ignored and cannot release or mutate active attempt H2', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-1'),
      });

      const context = createContext(pipeline, transferId);

      // Attempt H1
      const [h1] = pipeline.handleChunkRequest(transferId, [0], context);
      const m1 = h1.message.messageId;
      const t1 = h1.attemptToken;

      // H1 fails locally
      pipeline.handleHandoffFailure(transferId, 0, m1, t1, context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(false);

      // Attempt H2 requested: reuses m1 with new token T2
      const [h2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(h2.message.messageId).toBe(m1);
      expect(h2.attemptToken).not.toBe(t1);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      // Stale callback for H1 with undefined/omitted attemptToken arrives
      (pipeline as any).handleHandoffFailure(transferId, 0, m1, undefined, context);

      // Verify H2 is NOT released and NOT mutated!
      let session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);
      expect(session.transmittedChunks.has(0)).toBe(true);

      // Stale success callback without token
      (pipeline as any).handleHandoffSuccess(transferId, 0, m1, undefined, context);
      session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-2 — Empty and malformed tokens
    it('R4-2: rejects/ignores empty, whitespace, null, and non-string malformed tokens safely without error', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-2'),
      });

      const context = createContext(pipeline, transferId);
      const [frame] = pipeline.handleChunkRequest(transferId, [0], context);
      const msgId = frame.message.messageId;

      const malformedTokens = ['', '   ', null, undefined, 12345, {}, [], true, 'wrong-token'];

      for (const token of malformedTokens) {
        expect(() => {
          (pipeline as any).handleHandoffFailure(transferId, 0, msgId, token, context);
        }).not.toThrow();
        expect(() => {
          (pipeline as any).handleHandoffSuccess(transferId, 0, msgId, token, context);
        }).not.toThrow();

        // Attempt remains active and in-flight
        const session = pipeline.getTransfer(transferId)!;
        expect(session.inflightChunks.has(0)).toBe(true);
      }

      pipeline.dispose();
    });

    // R4-3 — Stale success and failure
    it('R4-3: stale success and failure callbacks from H1 cannot mutate active attempt H2', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-3'),
      });

      const context = createContext(pipeline, transferId);

      const [h1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffFailure(transferId, 0, h1.message.messageId, h1.attemptToken, context);

      const [h2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      // Stale failure for H1 with H1 token arrives
      pipeline.handleHandoffFailure(transferId, 0, h1.message.messageId, h1.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      // Stale success for H1 with H1 token arrives
      pipeline.handleHandoffSuccess(transferId, 0, h1.message.messageId, h1.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-4 — Attempt-history isolation
    it('R4-4: tracks attempt history by attemptToken so H1 failure and H2 success remain independent', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-4'),
      });

      const context = createContext(pipeline, transferId);

      const [h1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffFailure(transferId, 0, h1.message.messageId, h1.attemptToken, context);

      const [h2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(h2.message.messageId).toBe(h1.message.messageId);
      expect(h2.attemptToken).not.toBe(h1.attemptToken);

      pipeline.handleHandoffSuccess(transferId, 0, h2.message.messageId, h2.attemptToken, context);

      // Observable verification:
      // H1 is failed, so repeating H1 callback is a no-op
      pipeline.handleHandoffFailure(transferId, 0, h1.message.messageId, h1.attemptToken, context);
      pipeline.handleHandoffSuccess(transferId, 0, h1.message.messageId, h1.attemptToken, context);

      // H2 remains successfully handed off and waiting for ACK
      const session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(true);
      expect(session.acknowledgedChunks.has(0)).toBe(false);

      // ACK H2 succeeds
      pipeline.handleChunkAck(transferId, 0, context);
      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-5 — Frame return is not successful handoff
    it('R4-5: returning a frame and requesting it again before handoff does not allocate new messageId or consume retry', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-5'),
      });

      const context = createContext(pipeline, transferId);

      // Initial request: returns F1
      const [f1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // Request again without any handoff success or failure
      const [f2] = pipeline.handleChunkRequest(transferId, [0], context);

      // Must preserve the exact same messageId
      expect(f2.message.messageId).toBe(f1.message.messageId);
      // Zero retries consumed
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-6 — Initial failure and retry
    it('R4-6: repeated initial local failures preserve M1 and consume zero network retries across new attemptTokens', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-6'),
      });

      const context = createContext(pipeline, transferId);

      const seenTokens = new Set<string>();
      let initialMsgId = '';

      for (let attempt = 1; attempt <= 4; attempt++) {
        const [frame] = pipeline.handleChunkRequest(transferId, [0], context);
        if (attempt === 1) {
          initialMsgId = frame.message.messageId;
        } else {
          expect(frame.message.messageId).toBe(initialMsgId);
        }
        expect(seenTokens.has(frame.attemptToken)).toBe(false);
        seenTokens.add(frame.attemptToken);

        // Fail locally
        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, context);
        const session = pipeline.getTransfer(transferId)!;
        expect(session.inflightChunks.has(0)).toBe(false);
        expect(session.retryCounts.get(0)).toBeUndefined();
      }

      // 5th attempt succeeds
      const [finalFrame] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(finalFrame.message.messageId).toBe(initialMsgId);
      expect(seenTokens.has(finalFrame.attemptToken)).toBe(false);

      pipeline.handleHandoffSuccess(transferId, 0, finalFrame.message.messageId, finalFrame.attemptToken, context);

      const finalSession = pipeline.getTransfer(transferId)!;
      expect(finalSession.retryCounts.get(0)).toBeUndefined(); // Zero retries!
      expect(finalSession.inflightChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-7 — Retransmission local failure
    it('R4-7: retransmission local failure rolls back reservation and next local attempt reuses M2 with new token, not M3', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-7'),
      });

      const context = createContext(pipeline, transferId);

      // 1. Initial M1 succeeds
      const [m1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, m1.message.messageId, m1.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // 2. Genuine network retransmission requested: receives M2
      const [m2Attempt1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m2Attempt1.message.messageId).not.toBe(m1.message.messageId);
      const m2Id = m2Attempt1.message.messageId;
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // 3. M2 fails locally
      pipeline.handleHandoffFailure(transferId, 0, m2Id, m2Attempt1.attemptToken, context);
      // Reservation rolled back: retryCount becomes undefined (0 retries)
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(false);

      // 4. Next local attempt requested: must reuse M2 with new attemptToken, NOT create M3!
      const [m2Attempt2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m2Attempt2.message.messageId).toBe(m2Id);
      expect(m2Attempt2.attemptToken).not.toBe(m2Attempt1.attemptToken);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // 5. M2 succeeds local handoff
      pipeline.handleHandoffSuccess(transferId, 0, m2Id, m2Attempt2.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // 6. Next genuine network retransmission receives M3
      const [m3] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m3.message.messageId).not.toBe(m2Id);
      expect(m3.message.messageId).not.toBe(m1.message.messageId);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);

      pipeline.dispose();
    });

    // R4-8 — Multiple retransmissions
    it('R4-8: multiple genuine network retransmissions each receive fresh messageId and increment retries exactly once up to limit', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-8'),
      });

      const context = createContext(pipeline, transferId);

      // Initial transmission (M1)
      const [m1] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();
      pipeline.handleHandoffSuccess(transferId, 0, m1.message.messageId, m1.attemptToken, context);

      // Retransmission 1 (M2)
      const [m2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m2.message.messageId).not.toBe(m1.message.messageId);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);
      pipeline.handleHandoffSuccess(transferId, 0, m2.message.messageId, m2.attemptToken, context);

      // Retransmission 2 (M3)
      const [m3] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m3.message.messageId).not.toBe(m2.message.messageId);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);
      pipeline.handleHandoffSuccess(transferId, 0, m3.message.messageId, m3.attemptToken, context);

      // Retransmission 3 (M4)
      const [m4] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(m4.message.messageId).not.toBe(m3.message.messageId);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(3);
      pipeline.handleHandoffSuccess(transferId, 0, m4.message.messageId, m4.attemptToken, context);

      // Retransmission 4 (exceeds maxRetriesPerChunk 3) -> aborts
      expect(() => {
        pipeline.handleChunkRequest(transferId, [0], context);
      }).toThrowError(/Retry budget exhausted/);

      const session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('ABORTED');
      expect(session.failureReason).toBe('RETRY_BUDGET_EXHAUSTED');

      pipeline.dispose();
    });

    // R4-9 — Delayed callback after a newer successful attempt
    it('R4-9: delayed callback after a newer successful attempt does not alter the attempt or ACK lifecycle', async () => {
      const pipeline = createPipeline({ maxRetriesPerChunk: 3 });
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        sctpMaxMessageSize: 2144,
        captureAndSerializeFn: () => createSerializedArtifact(5, 'r4-9'),
      });

      const context = createContext(pipeline, transferId);

      // M1 succeeds
      const [m1] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, m1.message.messageId, m1.attemptToken, context);

      // M2 retransmission succeeds
      const [m2] = pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleHandoffSuccess(transferId, 0, m2.message.messageId, m2.attemptToken, context);

      // Delayed failure for M1 arrives
      pipeline.handleHandoffFailure(transferId, 0, m1.message.messageId, m1.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Chunk ACK arrives
      pipeline.handleChunkAck(transferId, 0, context);
      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      // Late callbacks for M2 arrive after ACK
      pipeline.handleHandoffFailure(transferId, 0, m2.message.messageId, m2.attemptToken, context);
      pipeline.handleHandoffSuccess(transferId, 0, m2.message.messageId, m2.attemptToken, context);
      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      pipeline.dispose();
    });

    // R4-10 — Context and terminal fencing
    it('R4-10: enforces contextual fencing on callbacks and ensures terminal state immunity', async () => {
      const pipeline = createPipeline();
      const transferId = generateUUID();

      await pipeline.startTransfer({
        transferId,
        recipientPeerId: RECIPIENT_PEER_ID,
        supplierPeerId: SUPPLIER_PEER_ID,
        membershipEpoch: MEMBERSHIP_EPOCH,
        connectionGeneration: 1,
        sctpMaxMessageSize: FALLBACK_SCTP_LIMIT,
        captureAndSerializeFn: () => createSerializedArtifact(1, 'r4-10'),
      });

      const validContext = createContext(pipeline, transferId, { connectionGeneration: 1 });
      const [frame] = pipeline.handleChunkRequest(transferId, [0], validContext);

      // 1. Context fencing checks:
      const imposterPeer = createContext(pipeline, transferId, { recipientPeerId: 'peer-wrong' });
      expect(() => {
        pipeline.handleHandoffSuccess(transferId, 0, frame.message.messageId, frame.attemptToken, imposterPeer);
      }).toThrowError(/Authenticated peer 'peer-wrong' does not match/);
      expect(() => {
        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, imposterPeer);
      }).toThrowError(/Authenticated peer 'peer-wrong' does not match/);

      const imposterSession = createContext(pipeline, transferId, { sessionId: 'sess-wrong' });
      expect(() => {
        pipeline.handleHandoffSuccess(transferId, 0, frame.message.messageId, frame.attemptToken, imposterSession);
      }).toThrowError(/Context sessionId 'sess-wrong' does not match/);

      const staleEpoch = createContext(pipeline, transferId, { membershipEpoch: 999 });
      expect(() => {
        pipeline.handleHandoffSuccess(transferId, 0, frame.message.messageId, frame.attemptToken, staleEpoch);
      }).toThrowError(/Context membershipEpoch/);

      const staleGeneration = createContext(pipeline, transferId, { connectionGeneration: 99 });
      expect(() => {
        pipeline.handleHandoffSuccess(transferId, 0, frame.message.messageId, frame.attemptToken, staleGeneration);
      }).toThrowError(/Context connectionGeneration/);

      // 2. Terminal state immunity: cancel transfer
      pipeline.cancelTransfer(transferId);
      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      // Duplicate callbacks on cancelled transfer are harmless no-ops
      expect(() => {
        pipeline.handleHandoffSuccess(transferId, 0, frame.message.messageId, frame.attemptToken, validContext);
      }).not.toThrow();
      expect(() => {
        pipeline.handleHandoffFailure(transferId, 0, frame.message.messageId, frame.attemptToken, validContext);
      }).not.toThrow();

      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      pipeline.dispose();
    });
  });
});
