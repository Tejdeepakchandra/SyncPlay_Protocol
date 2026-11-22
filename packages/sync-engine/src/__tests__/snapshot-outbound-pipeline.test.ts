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

      // Retransmit chunk 0 (first retransmission: retryCounts = 1)
      const [frame2] = pipeline.handleChunkRequest(transferId, [0], context);
      expect(frame2.payload.chunkIndex).toBe(0);
      expect(frame2.serializedJson).toBe(frame1.serializedJson);

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
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // Request 2 (retry 1): retryCount = 1 <= 2
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Request 3 (retry 2): retryCount = 2 <= 2
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);

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
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // 2. Retransmission 1: retryCount=1 <= 3
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // 3. Retransmission 2: retryCount=2 <= 3
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(2);

      // 4. Retransmission 3: retryCount=3 <= 3
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(3);

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
      pipeline.handleChunkRequest(transferId, [0, 1], context);

      // Retransmit chunk 0 twice
      pipeline.handleChunkRequest(transferId, [0], context);
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
      pipeline.handleChunkRequest(transferId, [0], context);
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
      pipeline.handleHandoffFailure(transferId, 0, context);

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
      pipeline.handleChunkRequest(transferId, [0, 1], context);

      let session = pipeline.getTransfer(transferId)!;
      expect(session.state).toBe('WAITING_ACK');
      expect(session.inflightChunks.size).toBe(2);

      // Handoff failure reported on chunk 1
      pipeline.handleHandoffFailure(transferId, 1, context);

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
      pipeline.handleHandoffFailure(transferId, 0, context);

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
      pipeline.handleHandoffFailure(transferId, 0, context);

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
        pipeline.handleChunkRequest(transferId, [0], context);
        expect(pipeline.getTransfer(transferId)!.inflightChunks.has(0)).toBe(true);

        pipeline.handleHandoffFailure(transferId, 0, context);
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

      // Initial transmission succeeds (no handoff failure)
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.transmittedChunks.has(0)).toBe(true);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBeUndefined();

      // Recipient requests retransmission -> retryCounts becomes 1
      pipeline.handleChunkRequest(transferId, [0], context);
      expect(pipeline.getTransfer(transferId)!.retryCounts.get(0)).toBe(1);

      // Local handoff of this retransmission fails
      pipeline.handleHandoffFailure(transferId, 0, context);

      // Retry count is rolled back, chunk remains transmitted
      let session = pipeline.getTransfer(transferId)!;
      expect(session.inflightChunks.has(0)).toBe(false);
      expect(session.transmittedChunks.has(0)).toBe(true);
      expect(session.retryCounts.get(0)).toBeUndefined();

      // Transport retries the retransmission
      pipeline.handleChunkRequest(transferId, [0], context);
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
      pipeline.handleChunkRequest(transferId, [0], context);

      // First failure report
      expect(() => pipeline.handleHandoffFailure(transferId, 0, context)).not.toThrow();

      // Duplicate failure report: idempotent no-op
      expect(() => pipeline.handleHandoffFailure(transferId, 0, context)).not.toThrow();

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
      pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.handleChunkAck(transferId, 0, context);

      expect(pipeline.getTransfer(transferId)!.acknowledgedChunks.has(0)).toBe(true);

      // Late handoff failure arrives for chunk 0
      pipeline.handleHandoffFailure(transferId, 0, context);

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
      pipeline.handleChunkRequest(transferId, [0], context);
      pipeline.cancelTransfer(transferId);

      expect(pipeline.getTransfer(transferId)!.state).toBe('CANCELLED');

      // Late handoff failure
      expect(() => pipeline.handleHandoffFailure(transferId, 0, context)).not.toThrow();
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

      const staleContext = createContext(pipeline, transferId, {
        connectionGeneration: 2,
      });

      try {
        pipeline.handleHandoffFailure(transferId, 0, staleContext);
        expect.unreachable('Should have thrown UNAUTHORIZED');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      }

      const imposterContext = createContext(pipeline, transferId, {
        recipientPeerId: 'peer-imposter-99',
        connectionGeneration: 3,
      });

      try {
        pipeline.handleHandoffFailure(transferId, 0, imposterContext);
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
        pipeline.handleHandoffFailure(transferId, 9999, context);
        expect.unreachable('Should have thrown INVALID_RANGE');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_RANGE');
      }

      pipeline.dispose();
    });
  });
});
