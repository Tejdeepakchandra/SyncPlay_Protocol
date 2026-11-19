/**
 * Public export smoke test.
 *
 * Verifies that all expected symbols are accessible through
 * the package's public entry point (index.ts).
 *
 * This test catches accidental export omissions or breakage.
 */
import { describe, it, expect } from 'vitest';
import {
  // Constants
  PROTOCOL_VERSION,
  MAX_MESSAGE_BYTES,
  MAX_PAYLOAD_BYTES,
  MAX_BATCH_PAYLOAD_BYTES,
  MAX_OPERATIONS_PER_BATCH,
  MAX_VECTOR_ENTRIES,
  MAX_SNAPSHOT_BYTES,
  ASCII_IDENTIFIER_REGEX,
  MAX_ENVELOPE_OVERHEAD_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MIN_BASE64_CHUNK_BYTES,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  MIN_TRANSPORT_LIMIT_FULL_CHUNK,
  MAX_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_CONCURRENT_CHUNK_REQUESTS,
  MIN_SUPPORTED_SCTP_LIMIT,
  CHUNK_REQUEST_TIMEOUT_MS,
  MAX_CHUNK_RETRIES,
  MAX_TRANSFER_RETRIES,
  OVERALL_TRANSFER_TIMEOUT_MS,
  MAX_SUPPLIER_CONCURRENT_TRANSFERS,

  // Message type registry
  MessageType,
  SYNC_MESSAGE_TYPES,
  KNOWN_MESSAGE_TYPES,
  REQUIRES_LOGICAL_EVENT_ID,

  // Envelope
  ProtocolMessageSchema,
  SnapshotChunkEnvelopeSchema,

  // Payload schemas
  PingPayloadSchema,
  PongPayloadSchema,
  PlayPayloadSchema,
  PausePayloadSchema,
  SeekPayloadSchema,
  RateChangePayloadSchema,
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadSchema,
  StateDigestPayloadV1Schema,
  StateDigestPayloadV2Schema,
  StateSnapshotPayloadV2Schema,
  OperationBatchPayloadSchema,
  CompatibilityOperationBatchPayloadSchema,
  RepairRequestPayloadSchema,
  RepairRejectedPayloadSchema,
  ReplicationVectorSchema,
  SnapshotFrontierSchema,
  HistoryFingerprintsSchema,
  CompactedCheckpointItemSchema,
  STATE_REQUEST_REASONS,
  REPAIR_REJECTION_REASONS,
  PAYLOAD_SCHEMAS,
  StateDigestWireSchema,
  SnapshotAbortReasonSchema,
  SnapshotOfferPayloadSchema,
  SnapshotRequestPayloadSchema,
  SnapshotChunkPayloadSchema,
  SnapshotChunkRequestPayloadSchema,
  SnapshotAbortPayloadSchema,

  // Admission
  calculateChunkCapacity,
  calculateSnapshotAdmission,

  // Base64 helpers
  decodeBase64,
  encodeBase64,

  // Helpers
  UUID_V4_REGEX,
  UUIDv4Schema,
  isLegacyStateDigest,
  isStateDigestV2,
  validateContiguousSequenceBatch,

  // Validation
  validateMessage,
  validatePayload,
  checkMessageSize,
  checkPayloadSize,
  parseAndValidateMessage,
  computeUtf8ByteLength,
  serializeAndCheckSize,
} from '../index.js';

// Type-only imports verified at compile time
import type {
  PeerId,
  SessionId,
  SocketId,
  MessageId,
  LogicalEventId,
  ConnectionGeneration,
  ProtocolMessage,
  PingPayload,
  PongPayload,
  PlayPayload,
  PausePayload,
  SeekPayload,
  RateChangePayload,
  StateRequestPayload,
  StateSnapshotPayload,
  StateDigestPayload,
  StateRequestReason,
  Result,
  ValidationError,
  ValidationErrorCode,
} from '../index.js';

describe('Public exports', () => {
  it('should export PROTOCOL_VERSION as a number', () => {
    expect(typeof PROTOCOL_VERSION).toBe('number');
    expect(PROTOCOL_VERSION).toBe(1);
  });

  it('should export size limit constants', () => {
    expect(typeof MAX_MESSAGE_BYTES).toBe('number');
    expect(typeof MAX_PAYLOAD_BYTES).toBe('number');
    expect(typeof MAX_BATCH_PAYLOAD_BYTES).toBe('number');
    expect(typeof MAX_OPERATIONS_PER_BATCH).toBe('number');
    expect(typeof MAX_VECTOR_ENTRIES).toBe('number');
    expect(MAX_MESSAGE_BYTES).toBeGreaterThan(0);
    expect(MAX_PAYLOAD_BYTES).toBeGreaterThan(0);
    expect(MAX_MESSAGE_BYTES).toBeGreaterThanOrEqual(MAX_PAYLOAD_BYTES);
    expect(MAX_OPERATIONS_PER_BATCH).toBe(32);
    expect(MAX_VECTOR_ENTRIES).toBe(64);
    expect(MAX_SNAPSHOT_BYTES).toBe(4 * 1024 * 1024);
    expect(ASCII_IDENTIFIER_REGEX instanceof RegExp).toBe(true);
    expect(MAX_ENVELOPE_OVERHEAD_BYTES).toBe(1800);
    expect(MIN_RAW_CHUNK_BYTES).toBe(256);
    expect(MIN_BASE64_CHUNK_BYTES).toBe(344);
    expect(MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER).toBe(2144);
    expect(MIN_TRANSPORT_LIMIT_FULL_CHUNK).toBe(45492);
    expect(MAX_CHUNK_BYTES).toBe(32768);
    expect(MAX_CHUNKS_PER_SNAPSHOT).toBe(128);
    expect(MAX_CONCURRENT_CHUNK_REQUESTS).toBe(4);
    expect(MIN_SUPPORTED_SCTP_LIMIT).toBe(2144);
    expect(CHUNK_REQUEST_TIMEOUT_MS).toBe(5000);
    expect(MAX_CHUNK_RETRIES).toBe(3);
    expect(MAX_TRANSFER_RETRIES).toBe(10);
    expect(OVERALL_TRANSFER_TIMEOUT_MS).toBe(60000);
    expect(MAX_SUPPLIER_CONCURRENT_TRANSFERS).toBe(2);
  });

  it('should export ProtocolMessageSchema and SnapshotChunkEnvelopeSchema', () => {
    expect(ProtocolMessageSchema).toBeDefined();
    expect(typeof ProtocolMessageSchema.safeParse).toBe('function');
    expect(SnapshotChunkEnvelopeSchema).toBeDefined();
    expect(typeof SnapshotChunkEnvelopeSchema.safeParse).toBe('function');
  });

  it('should export all known message types including Phase 7.4 anti-entropy and Phase 7.5 snapshot recovery', () => {
    expect(KNOWN_MESSAGE_TYPES.size).toBe(27);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.PING)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.PONG)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.PLAY)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.PAUSE)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SEEK)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.RATE_CHANGE)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.STATE_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.STATE_SNAPSHOT)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.STATE_DIGEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.OPERATION_BATCH)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.REPAIR_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.REPAIR_REJECTED)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SNAPSHOT_OFFER)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SNAPSHOT_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SNAPSHOT_CHUNK)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SNAPSHOT_CHUNK_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(MessageType.SNAPSHOT_ABORT)).toBe(true);

    // Wire subprotocol constants
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.CONTROL_EVENT)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.OPERATION_BATCH)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.STATE_DIGEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.REPAIR_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.REPAIR_REJECTED)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.SNAPSHOT_OFFER)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.SNAPSHOT_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.SNAPSHOT_ABORT)).toBe(true);
  });

  it('should export REQUIRES_LOGICAL_EVENT_ID with 4 control types', () => {
    expect(REQUIRES_LOGICAL_EVENT_ID.size).toBe(4);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.PLAY)).toBe(true);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.PAUSE)).toBe(true);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.SEEK)).toBe(true);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.RATE_CHANGE)).toBe(true);
    // Transport, state, and anti-entropy repair messages do NOT require it
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.PING)).toBe(false);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.PONG)).toBe(false);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.STATE_REQUEST)).toBe(false);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.OPERATION_BATCH)).toBe(false);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.REPAIR_REQUEST)).toBe(false);
    expect(REQUIRES_LOGICAL_EVENT_ID.has(MessageType.REPAIR_REJECTED)).toBe(false);
  });

  it('should export ProtocolMessageSchema as a Zod schema', () => {
    expect(typeof ProtocolMessageSchema.safeParse).toBe('function');
  });

  it('should export all payload schemas', () => {
    expect(typeof PingPayloadSchema.safeParse).toBe('function');
    expect(typeof PongPayloadSchema.safeParse).toBe('function');
    expect(typeof PlayPayloadSchema.safeParse).toBe('function');
    expect(typeof PausePayloadSchema.safeParse).toBe('function');
    expect(typeof SeekPayloadSchema.safeParse).toBe('function');
    expect(typeof RateChangePayloadSchema.safeParse).toBe('function');
    expect(typeof StateRequestPayloadSchema.safeParse).toBe('function');
    expect(typeof StateSnapshotPayloadSchema.safeParse).toBe('function');
    expect(typeof StateDigestPayloadSchema.safeParse).toBe('function');
    expect(typeof StateDigestPayloadV1Schema.safeParse).toBe('function');
    expect(typeof StateDigestPayloadV2Schema.safeParse).toBe('function');
    expect(typeof OperationBatchPayloadSchema.safeParse).toBe('function');
    expect(typeof CompatibilityOperationBatchPayloadSchema.safeParse).toBe('function');
    expect(typeof RepairRequestPayloadSchema.safeParse).toBe('function');
    expect(typeof RepairRejectedPayloadSchema.safeParse).toBe('function');
    expect(typeof ReplicationVectorSchema.safeParse).toBe('function');
    expect(typeof SnapshotFrontierSchema.safeParse).toBe('function');
    expect(typeof HistoryFingerprintsSchema.safeParse).toBe('function');
    expect(typeof CompactedCheckpointItemSchema.safeParse).toBe('function');
    expect(typeof StateSnapshotPayloadV2Schema.safeParse).toBe('function');
    expect(typeof StateDigestWireSchema.safeParse).toBe('function');
    expect(typeof SnapshotAbortReasonSchema.safeParse).toBe('function');
    expect(typeof SnapshotOfferPayloadSchema.safeParse).toBe('function');
    expect(typeof SnapshotRequestPayloadSchema.safeParse).toBe('function');
    expect(typeof SnapshotChunkPayloadSchema.safeParse).toBe('function');
    expect(typeof SnapshotChunkRequestPayloadSchema.safeParse).toBe('function');
    expect(typeof SnapshotAbortPayloadSchema.safeParse).toBe('function');
    expect(typeof UUIDv4Schema.safeParse).toBe('function');
    expect(UUID_V4_REGEX instanceof RegExp).toBe(true);
  });

  it('should export validation, size helper, and admission functions', () => {
    expect(typeof validateMessage).toBe('function');
    expect(typeof validatePayload).toBe('function');
    expect(typeof checkMessageSize).toBe('function');
    expect(typeof checkPayloadSize).toBe('function');
    expect(typeof parseAndValidateMessage).toBe('function');
    expect(typeof computeUtf8ByteLength).toBe('function');
    expect(typeof serializeAndCheckSize).toBe('function');
    expect(typeof isLegacyStateDigest).toBe('function');
    expect(typeof isStateDigestV2).toBe('function');
    expect(typeof validateContiguousSequenceBatch).toBe('function');
    expect(typeof calculateChunkCapacity).toBe('function');
    expect(typeof calculateSnapshotAdmission).toBe('function');
    expect(typeof decodeBase64).toBe('function');
    expect(typeof encodeBase64).toBe('function');
  });

  it('should export PAYLOAD_SCHEMAS map with entry for each message type', () => {
    expect(PAYLOAD_SCHEMAS.size).toBe(KNOWN_MESSAGE_TYPES.size);
    for (const type of KNOWN_MESSAGE_TYPES) {
      expect(PAYLOAD_SCHEMAS.has(type)).toBe(true);
    }
  });

  it('should export STATE_REQUEST_REASONS', () => {
    expect(STATE_REQUEST_REASONS).toContain('INITIAL_SYNC');
    expect(STATE_REQUEST_REASONS).toContain('REJOIN');
    expect(STATE_REQUEST_REASONS).toContain('DIVERGENCE_SUSPECTED');
  });

  it('should export validation functions', () => {
    expect(typeof validateMessage).toBe('function');
    expect(typeof validatePayload).toBe('function');
    expect(typeof checkMessageSize).toBe('function');
    expect(typeof checkPayloadSize).toBe('function');
    expect(typeof parseAndValidateMessage).toBe('function');
  });

  it('should perform a round-trip validation through public API', () => {
    const msg = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SEEK,
      sessionId: 'room-1',
      senderPeerId: 'peer-1',
      messageId: 'msg-1',
      logicalEventId: 'op-1',
      payload: { targetPositionMs: 45000 },
    };

    const json = JSON.stringify(msg);
    const result = parseAndValidateMessage(json);

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.SEEK);
      const payload = result.value.payload as SeekPayload;
      expect(payload.targetPositionMs).toBe(45000);
    }
  });
});
