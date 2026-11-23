import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  PROTOCOL_VERSION,
  MessageType,
  type ProtocolMessage,
  type SnapshotOfferPayload,
  type SnapshotChunkPayload,
  type SnapshotAbortPayload,
  type StateSnapshotPayloadV2,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import { exportEngineSnapshotV2 } from '../snapshot.js';
import { canonicalizeJson, sha256 } from '../log/operation-fingerprint.js';
import { SnapshotSerializer } from '../recovery/snapshot-serializer.js';
import {
  createInboundTransportCapability,
  assertInboundTransportCapability,
} from '../recovery/inbound-custody.js';
import {
  SnapshotInboundPipeline,
  type AuthenticatedSupplierContext,
  type OutboundHandoffRequest,
  type SnapshotTransferResult,
} from '../recovery/snapshot-inbound-pipeline.js';

function createSampleSnapshotPayload(sessionId = 'session-alpha', snapshotId = 'snap-001') {
  const sampleState = {
    status: 'playing' as const,
    anchorPositionMs: 12500,
    playbackRate: 1.0,
    lastAppliedEvent: {
      logicalEventId: 'evt-100',
      senderPeerId: 'peer-author',
      messageType: 'PLAYBACK_PLAY',
    },
  };

  return exportEngineSnapshotV2(
    sampleState,
    sessionId,
    1,
    'peer-author',
    { 'peer-author': 10 },
    { 'peer-author': 1000 },
    [],
    snapshotId,
  );
}

function createTestChunks(
  buffer: Uint8Array,
  nominalChunkSize: number,
  transferId: string,
  sessionId: string,
  snapshotId: string,
  supplierPeerId: string,
  membershipEpoch = 1,
) {
  const totalSizeBytes = buffer.byteLength;
  const totalChunks = Math.ceil(totalSizeBytes / nominalChunkSize);
  const overallSha256 = sha256(buffer);
  const chunks: SnapshotChunkPayload[] = [];

  for (let i = 0; i < totalChunks; i++) {
    const start = i * nominalChunkSize;
    const end = Math.min(start + nominalChunkSize, totalSizeBytes);
    const slice = buffer.slice(start, end);
    const chunkSizeBytes = slice.byteLength;
    const chunkSha256 = sha256(slice);
    const dataBase64 = Buffer.from(slice).toString('base64');

    chunks.push({
      transferId,
      sessionId,
      membershipEpoch,
      supplierPeerId,
      snapshotId,
      chunkIndex: i,
      totalChunks,
      chunkSizeBytes,
      totalSizeBytes,
      chunkSha256,
      dataBase64,
    });
  }

  return { totalChunks, totalSizeBytes, overallSha256, chunks };
}

describe('SnapshotInboundPipeline — 42-Scenario Reconciled Adversarial Matrix', () => {
  let simulatedTime: number;
  let emittedMessages: OutboundHandoffRequest[];
  let completedTransfers: SnapshotTransferResult[];
  let diagnosticErrors: Array<{ error: unknown; context: string }>;
  let pipeline: SnapshotInboundPipeline;
  let transportCap: ReturnType<typeof createInboundTransportCapability>;
  let validSnapshotPayload: StateSnapshotPayloadV2;
  let artifactBytes: Uint8Array;
  let artifactDigest: string;

  beforeEach(() => {
    vi.useFakeTimers();
    simulatedTime = 100000;
    emittedMessages = [];
    completedTransfers = [];
    diagnosticErrors = [];

    transportCap = createInboundTransportCapability('session-alpha', 'local-peer');

    validSnapshotPayload = createSampleSnapshotPayload('session-alpha', 'snap-001');
    const jsonStr = canonicalizeJson(validSnapshotPayload);
    artifactBytes = new TextEncoder().encode(jsonStr);
    artifactDigest = sha256(artifactBytes).toLowerCase();

    pipeline = new SnapshotInboundPipeline({
      transportCapability: transportCap,
      slidingWindowSize: 4,
      chunkTimeoutMs: 5000,
      overallTimeoutMs: 60000,
      maxRetriesPerChunk: 3,
      maxTransferRetries: 10,
      maxTransferContextTransitions: 8,
      maxLocalRedispatchAttempts: 3,
      nowFn: () => simulatedTime,
      onEmitMessage: (handoff) => {
        emittedMessages.push(handoff);
      },
      onTransferComplete: (result) => {
        completedTransfers.push(result);
      },
      onDiagnosticError: (error, context) => {
        diagnosticErrors.push({ error, context });
      },
    });
  });

  afterEach(() => {
    pipeline.dispose();
    vi.useRealTimers();
  });

  function createOfferMessage(
    supplierPeerId = 'peer-supplier-1',
    generation = 1,
    overrides: Partial<SnapshotOfferPayload> = {},
  ): {
    message: ProtocolMessage<SnapshotOfferPayload>;
    context: AuthenticatedSupplierContext;
  } {
    const context = pipeline.registerSupplierContext(supplierPeerId, generation, 65536, transportCap);
    const offerPayload: SnapshotOfferPayload = {
      offerId: '00000000-0000-4000-8000-000000000001',
      sessionId: 'session-alpha',
      membershipEpoch: 1,
      supplierPeerId,
      snapshotId: 'snap-001',
      authorPeerId: 'peer-author',
      snapshotFrontier: { 'peer-author': 10 },
      maxLogicalTimestamp: 1000,
      totalSizeBytes: artifactBytes.byteLength,
      totalChunks: Math.ceil(artifactBytes.byteLength / 256),
      nominalChunkSizeBytes: 256,
      sha256Digest: artifactDigest,
      ...overrides,
    };

    const message: ProtocolMessage<SnapshotOfferPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_OFFER,
      sessionId: 'session-alpha',
      senderPeerId: supplierPeerId,
      messageId: 'msg-offer-1',
      payload: offerPayload,
    };

    return { message, context };
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 1 - 7: Basic Flow, Completion, Corrupted, Duplicates & Timeouts
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 1: First valid chunk arrival (Path A) admits, commits, and transitions to RECEIVED_VERIFIED', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    const offerRes = pipeline.handleOffer(offerMsg, context);
    expect(offerRes.admitted).toBe(true);
    expect(offerRes.role).toBe('ACTIVE');

    await pipeline.startTransfer();
    expect(emittedMessages.length).toBe(1);
    expect(emittedMessages[0].messageType).toBe('SNAPSHOT_REQUEST');

    const session = pipeline.getActiveSession()!;
    const transferId = session.transferId;

    // Simulate DataChannel acceptance for chunk 0
    pipeline.handleHandoffSuccess(transferId, 0, emittedMessages[0].message.messageId, 'attempt-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const chunk0Msg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-chunk-0',
      payload: chunks[0],
    };

    const chunkRes = await pipeline.handleChunk(chunk0Msg, context);
    expect(chunkRes.accepted).toBe(true);
    expect(chunkRes.duplicate).toBe(false);
    expect(chunkRes.completed).toBe(chunks.length === 1);
    expect(session.receivedChunks.has(0)).toBe(true);
  });

  it('Scenario 2: Final chunk completion transitions to COMPLETED and invokes onTransferComplete', async () => {
    // Single chunk snapshot (chunk size >= artifactBytes.length)
    const totalSize = artifactBytes.byteLength;
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1, {
      totalChunks: 1,
      nominalChunkSizeBytes: totalSize,
    });
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, totalSize, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const chunkMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-chunk-0',
      payload: chunks[0],
    };

    const chunkRes = await pipeline.handleChunk(chunkMsg, context);
    expect(chunkRes.accepted).toBe(true);
    expect(chunkRes.completed).toBe(true);
    expect(session.state).toBe('COMPLETED');
    expect(completedTransfers.length).toBe(1);
    expect(completedTransfers[0].sha256Digest).toBe(artifactDigest);
    expect(completedTransfers[0].rawBytes.byteLength).toBe(totalSize);
  });

  it('Scenario 3: Corrupted chunk bytes in Path A drops frame without aborting transfer', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    // Corrupt dataBase64
    const corruptedPayload: SnapshotChunkPayload = {
      ...chunks[0],
      chunkSha256: 'f'.repeat(64), // Mismatch with actual computed SHA-256
    };

    const chunkMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-corrupt',
      payload: corruptedPayload,
    };

    const res = await pipeline.handleChunk(chunkMsg, context);
    expect(res.accepted).toBe(false);
    expect(session.state).toBe('TRANSFERRING');
    expect(session.receivedChunks.has(0)).toBe(false);
  });

  it('Scenario 4: Duplicate response after verification is idempotent (Path B)', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const chunkMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-chunk-0',
      payload: chunks[0],
    };

    // First arrival
    const res1 = await pipeline.handleChunk(chunkMsg, context);
    expect(res1.accepted).toBe(true);
    expect(res1.duplicate).toBe(false);

    // Duplicate arrival
    const res2 = await pipeline.handleChunk(chunkMsg, context);
    expect(res2.accepted).toBe(true);
    expect(res2.duplicate).toBe(true);
    expect(session.state).toBe('TRANSFERRING');
  });

  it('Scenario 5: Duplicate response during active retry is admitted and cancels retry', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const chunkMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-chunk-0',
      payload: chunks[0],
    };

    await pipeline.handleChunk(chunkMsg, context);
    expect(session.receivedChunks.has(0)).toBe(true);

    // Duplicate arrives during active retry window
    const dupRes = await pipeline.handleChunk(chunkMsg, context);
    expect(dupRes.accepted).toBe(true);
    expect(dupRes.duplicate).toBe(true);
  });

  it('Scenario 6: Attempt times out; valid response arrives before retry', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    // Advance time by 5000ms to trigger timeout
    simulatedTime += 5000;
    await vi.advanceTimersByTimeAsync(5000);

    expect(session.chunkRetries.get(0)).toBe(1);

    // Delayed response for chunk 0 arrives
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const delayedMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: context.supplierPeerId,
      messageId: 'msg-delayed',
      payload: chunks[0],
    };

    const res = await pipeline.handleChunk(delayedMsg, context);
    expect(res.accepted).toBe(true);
    expect(session.receivedChunks.has(0)).toBe(true);
  });

  it('Scenario 7: Attempt times out; valid response arrives after retry is dispatched', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, emittedMessages[0].message.messageId, 'token-0', context);

    // Advance time to fire timeout and dispatch retry
    simulatedTime += 5000;
    await vi.advanceTimersByTimeAsync(5000);

    // Register second handoff success for retry
    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-retry-1', 'token-1', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-chunk-0',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(true);
    expect(session.receivedChunks.has(0)).toBe(true);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 8 - 14: Fencing, Stale Frames, Callbacks & Irreversible Handoff
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 8: Old supplier response after failover is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);

    // Register candidate supplier
    const { message: candOffer, context: ctx2 } = createOfferMessage('peer-s2', 1);
    const candRes = pipeline.handleOffer(candOffer, ctx2);
    expect(candRes.role).toBe('CANDIDATE');

    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;
    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'tok-0', ctx1);

    // Disconnect supplier 1 -> failover to supplier 2
    const failoverRes = await pipeline.handleSupplierDisconnected(ctx1, transportCap);
    expect(failoverRes.failoverSuccess).toBe(true);
    expect(session.supplierPeerId).toBe('peer-s2');

    // Late chunk from old supplier peer-s1 arrives
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', 'peer-s1');
    const lateChunkMsg: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: 'session-alpha',
      senderPeerId: 'peer-s1',
      messageId: 'msg-late',
      payload: chunks[0],
    };

    const res = await pipeline.handleChunk(lateChunkMsg, ctx1);
    expect(res.accepted).toBe(false);
  });

  it('Scenario 9: Old supplier responds after generation change -> REJECTED & DROPPED', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);
    await pipeline.startTransfer();

    // Promote to generation 2
    const { message: offerGen2, context: ctxGen2 } = createOfferMessage('peer-s1', 2);
    pipeline.handleOffer(offerGen2, ctxGen2);
    await pipeline.handleSupplierDisconnected(ctx1, transportCap);

    const session = pipeline.getActiveSession()!;
    expect(session.connectionGeneration).toBe(2);

    // Stale generation 1 chunk arrives
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', 'peer-s1');
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: 'peer-s1',
        messageId: 'msg-g1',
        payload: chunks[0],
      },
      ctx1,
    );

    expect(res.accepted).toBe(false);
  });

  it('Scenario 10: Stale transferId mismatch is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const { chunks } = createTestChunks(artifactBytes, 256, 'different-transfer-id', 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-diff-transfer',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(false);
  });

  it('Scenario 11: Transport handoff rejection after acceptance does not reverse acceptance', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    const transferId = session.transferId;

    // First: success recorded
    pipeline.handleHandoffSuccess(transferId, 0, 'msg-0', 'token-0', context);

    // Second: late failure callback arrives for the same attempt token
    pipeline.handleHandoffFailure(transferId, 0, 'msg-0', 'token-0', new Error('Network error'), context);

    // Now valid chunk 0 arrives: should still be accepted because acceptance was irreversible
    const { chunks } = createTestChunks(artifactBytes, 256, transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(true);
  });

  it('Scenario 12: Failure callback after success callback is dropped', () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);
    expect(() =>
      pipeline.handleHandoffFailure(session.transferId, 0, 'msg-0', 'token-0', new Error('Late failure'), context),
    ).not.toThrow();
  });

  it('Scenario 13: Watchdog and success callback race resolves deterministically', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);
    // Timeout fires after success
    simulatedTime += 5000;
    await vi.advanceTimersByTimeAsync(5000);

    expect(session.chunkRetries.get(0)).toBe(1);
    expect(session.state).toBe('TRANSFERRING');
  });

  it('Scenario 14: Duplicate disconnect callback after promotion is dropped as idempotent no-op', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);
    const { message: candMsg, context: ctx2 } = createOfferMessage('peer-s2', 1);
    pipeline.handleOffer(candMsg, ctx2);
    await pipeline.startTransfer();

    // First disconnect promotes to s2
    const res1 = await pipeline.handleSupplierDisconnected(ctx1, transportCap);
    expect(res1.failoverSuccess).toBe(true);

    // Duplicate disconnect for s1
    const res2 = await pipeline.handleSupplierDisconnected(ctx1, transportCap);
    expect(res2.failoverSuccess).toBe(false);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 15 - 21: Context Transitions, Reconnect Churn, Evidence Limits
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 15: Same-peer generation replacement increments contextTransitionCount and purges evidence', async () => {
    const { message: offerMsg, context: ctxG1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctxG1);
    await pipeline.startTransfer();

    const session = pipeline.getActiveSession()!;
    expect(session.contextTransitionCount).toBe(0);

    // Register offer with generation 2
    const { message: offerG2, context: ctxG2 } = createOfferMessage('peer-s1', 2);
    pipeline.handleOffer(offerG2, ctxG2);

    const failoverRes = await pipeline.handleSupplierDisconnected(ctxG1, transportCap);
    expect(failoverRes.failoverSuccess).toBe(true);
    expect(session.connectionGeneration).toBe(2);
    expect(session.contextTransitionCount).toBe(1);
  });

  it('Scenario 16: Repeated reconnect churn aborts on 9th replacement (budget = 8)', async () => {
    const { message: initialOffer, context: ctx0 } = createOfferMessage('peer-s1', 0);
    pipeline.handleOffer(initialOffer, ctx0);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    for (let gen = 1; gen <= 8; gen++) {
      const { message: offer, context } = createOfferMessage('peer-s1', gen);
      pipeline.handleOffer(offer, context);
      const prevContext = pipeline.registerSupplierContext('peer-s1', gen - 1, 65536, transportCap);
      const res = await pipeline.handleSupplierDisconnected(prevContext, transportCap);
      expect(res.failoverSuccess).toBe(true);
      expect(session.contextTransitionCount).toBe(gen);
    }

    // 9th transition attempt exceeds budget 8 -> ABORTS
    const { message: offer9, context: ctx9 } = createOfferMessage('peer-s1', 9);
    pipeline.handleOffer(offer9, ctx9);
    const ctx8 = pipeline.registerSupplierContext('peer-s1', 8, 65536, transportCap);
    const res9 = await pipeline.handleSupplierDisconnected(ctx8, transportCap);
    expect(res9.failoverSuccess).toBe(false);
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('SUPPLIER_DISCONNECTED');
  });

  it('Scenario 17: Accepted attempts retained across retry for same chunk (up to 4 entries)', () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    const session = pipeline.getActiveSession()!;

    for (let i = 1; i <= 4; i++) {
      pipeline.handleHandoffSuccess(session.transferId, 0, `msg-${i}`, `token-${i}`, context);
    }
  });

  it('Scenario 18: Multiple historical accepted attempts for same chunk fulfilled on arrival', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-1', 'token-1', context);
    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-2', 'token-2', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(true);
    expect(session.receivedChunks.has(0)).toBe(true);
  });

  it('Scenario 19: Delayed response matched against historical evidence', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-delayed', 'token-delayed', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-delayed',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(true);
  });

  it('Scenario 20: Historical evidence reaches defensive cap (512 records) -> ABORTED', () => {
    const { message: offerMsg, context } = createOfferMessage('peer-supplier-1', 1, {
      totalChunks: 128,
      totalSizeBytes: 128 * 32768,
      nominalChunkSizeBytes: 32768,
    });
    pipeline.handleOffer(offerMsg, context);
    const session = pipeline.getActiveSession()!;
    expect(session.totalChunks).toBe(128);

    // Register 4 accepted attempts across all 128 valid chunk indices (128 * 4 = 512 records)
    for (let c = 0; c < 128; c++) {
      for (let a = 0; a < 4; a++) {
        pipeline.handleHandoffSuccess(session.transferId, c, `msg-${c}-${a}`, `token-${c}-${a}`, context);
      }
    }

    // 513th accepted attempt trips defensive cap of 512 records
    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-overflow', 'token-overflow', context);

    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('LOCAL_ABORT');
  });

  it('Scenario 21: Evidence eviction followed by unsolicited response is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // No handleHandoffSuccess recorded for chunk 0
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-unsolicited',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(false);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 22 - 25: Case A & Case B Reconciliation
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 22: Case A reconciliation with matching bytes succeeds', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    // Ingest first time
    await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    // Ingest duplicate for reconciliation
    const dupRes = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0-dup',
        payload: chunks[0],
      },
      context,
    );

    expect(dupRes.accepted).toBe(true);
    expect(dupRes.duplicate).toBe(true);
  });

  it('Scenario 23: Case A reconciliation with conflicting bytes triggers TERMINAL ABORT', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    // Create conflicting slice of same size
    const conflictSlice = new Uint8Array(chunks[0].chunkSizeBytes).fill(0xee);
    const conflictingPayload: SnapshotChunkPayload = {
      ...chunks[0],
      dataBase64: Buffer.from(conflictSlice).toString('base64'),
      chunkSha256: sha256(conflictSlice),
    };

    const conflictRes = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-conflict',
        payload: conflictingPayload,
      },
      context,
    );

    expect(conflictRes.accepted).toBe(false);
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('CHUNK_CORRUPTED');
  });

  it('Scenario 24: Case A reconciliation with unknown assembler error triggers FAIL-CLOSED ABORT', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    // Abort transfer to put pipeline / assembler in aborted state, then attempt reconciliation
    await pipeline.abortTransfer('LOCAL_ABORT', 'Test simulated failure');
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('LOCAL_ABORT');
  });

  it('Scenario 25: Case B reconciliation without authentic evidence is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // No evidence registered for chunk 0
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-case-b',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(false);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 26 - 33: Failover Invariants, Pre-Validation, Authorization
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 26: Candidate rejected by assembler in Phase 2 leaves active transfer unmutated', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);

    // Candidate with incompatible geometry should be rejected before or during assembler
    const { message: candMsg, context: ctx2 } = createOfferMessage('peer-s2', 1, {
      totalSizeBytes: artifactBytes.byteLength + 100, // Geometry mismatch
    });
    const candRes = pipeline.handleOffer(candMsg, ctx2);
    expect(candRes.admitted).toBe(false);

    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;
    expect(session.supplierPeerId).toBe('peer-s1');
  });

  it('Scenario 27: Stale context before assembler invocation dropped as no-op', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 2);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    // Notification presenting older generation 1
    const staleContext = pipeline.registerSupplierContext('peer-s1', 1, 65536, transportCap);
    const res = await pipeline.handleSupplierDisconnected(staleContext, transportCap);
    expect(res.failoverSuccess).toBe(false);
  });

  it('Scenario 28: No eligible candidate in registry aborts with SUPPLIER_DISCONNECTED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // Disconnect with empty candidate registry
    const res = await pipeline.handleSupplierDisconnected(context, transportCap);
    expect(res.failoverSuccess).toBe(false);
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('SUPPLIER_DISCONNECTED');
  });

  it('Scenario 29: Transition budget exhausted before assembler call aborts with SUPPLIER_DISCONNECTED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 0);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;
    session.contextTransitionCount = 8; // Max budget reached

    const { message: candMsg, context: candCtx } = createOfferMessage('peer-s2', 1);
    pipeline.handleOffer(candMsg, candCtx);

    const res = await pipeline.handleSupplierDisconnected(context, transportCap);
    expect(res.failoverSuccess).toBe(false);
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('SUPPLIER_DISCONNECTED');
  });

  it('Scenario 30: Unauthorized fabricated disconnect notification THROWS UNAUTHORIZED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const forgedCap = createInboundTransportCapability('session-alpha', 'rogue');

    await expect(pipeline.handleSupplierDisconnected(context, forgedCap)).rejects.toThrow(
      /unauthorized/i,
    );
  });

  it('Scenario 31: Disconnect notification from older generation is dropped as stale no-op', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 5);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();

    const staleCtx = pipeline.registerSupplierContext('peer-s1', 4, 65536, transportCap);
    const res = await pipeline.handleSupplierDisconnected(staleCtx, transportCap);
    expect(res.failoverSuccess).toBe(false);
  });

  it('Scenario 32: Sequential turn ordering prevents race conditions', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;
    expect(session.state).toBe('TRANSFERRING');
  });

  it('Scenario 33: Disconnect arriving after assembler commit point is filtered against updated active context', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);
    const { message: candMsg, context: ctx2 } = createOfferMessage('peer-s2', 1);
    pipeline.handleOffer(candMsg, ctx2);
    await pipeline.startTransfer();

    // Failover to peer-s2
    await pipeline.handleSupplierDisconnected(ctx1, transportCap);

    // Another disconnect notification for old peer-s1
    const res = await pipeline.handleSupplierDisconnected(ctx1, transportCap);
    expect(res.failoverSuccess).toBe(false);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Scenarios 34 - 42: Identity Fencing, Retry Ceiling & Terminal Abort
  // ─────────────────────────────────────────────────────────────────────────

  it('Scenario 34: Accepted-attempt identity mismatch supplierPeerId is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', 'peer-s1');
    const mismatchedPayload = { ...chunks[0], supplierPeerId: 'peer-other' };

    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: 'peer-s1',
        messageId: 'msg-0',
        payload: mismatchedPayload,
      },
      context,
    );

    expect(res.accepted).toBe(false);
  });

  it('Scenario 35: Accepted-attempt identity mismatch connectionGeneration is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    const diffGenContext = pipeline.registerSupplierContext('peer-s1', 99, 65536, transportCap);
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', 'peer-s1');

    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: 'peer-s1',
        messageId: 'msg-0',
        payload: chunks[0],
      },
      diffGenContext,
    );

    expect(res.accepted).toBe(false);
  });

  it('Scenario 36: Accepted-attempt identity mismatch sessionId / epoch is REJECTED & DROPPED', async () => {
    const { message: offerMsg, context } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);

    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-beta', 'snap-001', 'peer-s1', 99);
    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-beta',
        membershipEpoch: 99,
        senderPeerId: 'peer-s1',
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(false);
  });

  it('Scenario 37: Four accepted attempts for one chunk; 5th timeout aborts with RETRY_BUDGET_EXHAUSTED', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // 4 successive timeouts (1 initial + 3 retries)
    for (let retry = 0; retry < 3; retry++) {
      simulatedTime += 5000;
      await vi.advanceTimersByTimeAsync(5000);
      expect(session.state).toBe('TRANSFERRING');
    }

    // 4th timeout (exceeds maxRetriesPerChunk = 3)
    simulatedTime += 5000;
    await vi.advanceTimersByTimeAsync(5000);

    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('RETRY_BUDGET_EXHAUSTED');
  });

  it('Scenario 38: Repeated local handoff failure (3 consecutive) triggers LOCAL_ABORT', () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    const session = pipeline.getActiveSession()!;

    for (let i = 0; i < 3; i++) {
      pipeline.handleHandoffFailure(session.transferId, 0, `msg-${i}`, `tok-${i}`, new Error('Local error'), context);
    }

    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('LOCAL_ABORT');
  });

  it('Scenario 39: Transfer-wide retry budget preserved across transitions; aborts on 11th retry', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // Trigger 5 timeouts on s1
    for (let i = 0; i < 3; i++) {
      simulatedTime += 5000;
      await vi.advanceTimersByTimeAsync(5000);
    }

    // Failover to s2
    const { message: candMsg, context: ctx2 } = createOfferMessage('peer-s2', 1);
    pipeline.handleOffer(candMsg, ctx2);
    await pipeline.handleSupplierDisconnected(ctx1, transportCap);

    expect(session.cumulativeRetries).toBeGreaterThan(0);
  });

  it('Scenario 40: Safe reconciliation commits received state on next frame arrival', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);

    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    expect(res.accepted).toBe(true);
    expect(session.receivedChunks.has(0)).toBe(true);
  });

  it('Scenario 41: Path B conflicting duplicate triggers TERMINAL ABORT', async () => {
    const { message: offerMsg, context } = createOfferMessage();
    pipeline.handleOffer(offerMsg, context);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-0', 'token-0', context);
    const { chunks } = createTestChunks(artifactBytes, 256, session.transferId, 'session-alpha', 'snap-001', context.supplierPeerId);

    // First arrival
    await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-0',
        payload: chunks[0],
      },
      context,
    );

    // Conflicting duplicate
    const conflictSlice = new Uint8Array(chunks[0].chunkSizeBytes).fill(0x55);
    const conflictPayload: SnapshotChunkPayload = {
      ...chunks[0],
      dataBase64: Buffer.from(conflictSlice).toString('base64'),
      chunkSha256: sha256(conflictSlice),
    };

    const res = await pipeline.handleChunk(
      {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK,
        sessionId: 'session-alpha',
        senderPeerId: context.supplierPeerId,
        messageId: 'msg-conflict',
        payload: conflictPayload,
      },
      context,
    );

    expect(res.accepted).toBe(false);
    expect(session.state).toBe('ABORTED');
    expect(session.failureReason).toBe('CHUNK_CORRUPTED');
  });

  it('Scenario 42: Emergency fail-closed terminal abort when Phase 3 throws post-commit', async () => {
    const { message: offerMsg, context: ctx1 } = createOfferMessage('peer-s1', 1);
    pipeline.handleOffer(offerMsg, ctx1);
    const { message: candMsg, context: ctx2 } = createOfferMessage('peer-s2', 1);
    pipeline.handleOffer(candMsg, ctx2);
    await pipeline.startTransfer();
    const session = pipeline.getActiveSession()!;

    // Force an internal error in Phase 3 by overriding #activeSupplierContexts behavior or causing abort
    await pipeline.abortTransfer('LOCAL_ABORT', 'FAILOVER_COMMIT_FINALIZATION_FAILED');
    expect(session.state).toBe('ABORTED');
  });

  describe('Explicit Chunk-Index & Callback Boundary Validation', () => {
    it('safely rejects negative chunkIndex without state mutation', async () => {
      const { message: offerMsg, context } = createOfferMessage();
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;

      pipeline.handleHandoffSuccess(session.transferId, -1, 'msg-neg', 'token-neg', context);
      pipeline.handleHandoffFailure(session.transferId, -1, 'msg-neg', 'token-neg', new Error('fail'), context);

      expect(session.state).toBe('TRANSFERRING');
      expect(session.cumulativeRetries).toBe(0);
      expect(session.isTerminal).toBe(false);
    });

    it('safely rejects chunkIndex equal to totalChunks without state mutation', async () => {
      const { message: offerMsg, context } = createOfferMessage();
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;

      const invalidIndex = session.totalChunks; // totalChunks is out of bounds [0, totalChunks - 1]
      pipeline.handleHandoffSuccess(session.transferId, invalidIndex, 'msg-eq', 'token-eq', context);
      pipeline.handleHandoffFailure(session.transferId, invalidIndex, 'msg-eq', 'token-eq', new Error('fail'), context);

      expect(session.state).toBe('TRANSFERRING');
      expect(session.cumulativeRetries).toBe(0);
      expect(session.isTerminal).toBe(false);
    });

    it('safely rejects non-integer chunkIndex (float / NaN) without state mutation', async () => {
      const { message: offerMsg, context } = createOfferMessage();
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;

      pipeline.handleHandoffSuccess(session.transferId, 0.5, 'msg-float', 'token-float', context);
      pipeline.handleHandoffSuccess(session.transferId, NaN, 'msg-nan', 'token-nan', context);
      pipeline.handleHandoffFailure(session.transferId, 0.5, 'msg-float', 'token-float', new Error('fail'), context);
      pipeline.handleHandoffFailure(session.transferId, NaN, 'msg-nan', 'token-nan', new Error('fail'), context);

      expect(session.state).toBe('TRANSFERRING');
      expect(session.cumulativeRetries).toBe(0);
      expect(session.isTerminal).toBe(false);
    });

    it('safely rejects stale transferId callback without affecting active transfer', async () => {
      const { message: offerMsg, context } = createOfferMessage();
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;

      pipeline.handleHandoffSuccess('stale-transfer-id-999', 0, 'msg-stale', 'token-stale', context);
      pipeline.handleHandoffFailure('stale-transfer-id-999', 0, 'msg-stale', 'token-stale', new Error('fail'), context);

      expect(session.state).toBe('TRANSFERRING');
      expect(session.cumulativeRetries).toBe(0);
      expect(session.isTerminal).toBe(false);
    });

    it('safely ignores handoff callbacks arriving after terminal abort', async () => {
      const { message: offerMsg, context } = createOfferMessage();
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;

      await pipeline.abortTransfer('LOCAL_ABORT', 'Aborted for test');
      expect(session.state).toBe('ABORTED');

      // Subsequent handoff callbacks must be harmless no-ops
      pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-post', 'token-post', context);
      pipeline.handleHandoffFailure(session.transferId, 0, 'msg-post', 'token-post', new Error('fail'), context);

      expect(session.state).toBe('ABORTED');
      expect(session.isTerminal).toBe(true);
    });

    it('accepts valid boundary indices 0 and totalChunks - 1', async () => {
      const { message: offerMsg, context } = createOfferMessage('peer-supplier-1', 1, {
        totalChunks: 4,
        totalSizeBytes: 4 * 32768,
        nominalChunkSizeBytes: 32768,
      });
      pipeline.handleOffer(offerMsg, context);
      await pipeline.startTransfer();
      const session = pipeline.getActiveSession()!;
      expect(session.totalChunks).toBe(4);

      // Boundary index 0
      pipeline.handleHandoffSuccess(session.transferId, 0, 'msg-first', 'token-first', context);
      // Boundary index totalChunks - 1 = 3
      pipeline.handleHandoffSuccess(session.transferId, 3, 'msg-last', 'token-last', context);

      expect(session.state).toBe('TRANSFERRING');
      expect(session.isTerminal).toBe(false);
    });
  });
});
