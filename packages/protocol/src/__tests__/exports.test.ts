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

  // Message type registry
  MessageType,
  SYNC_MESSAGE_TYPES,
  KNOWN_MESSAGE_TYPES,
  REQUIRES_LOGICAL_EVENT_ID,

  // Envelope
  ProtocolMessageSchema,

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
  PushOperationBatchPayloadSchema,
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

  // Helpers
  UUID_V4_REGEX,
  UUIDv4Schema,
  isLegacyStateDigest,
  isStateDigestV2,
  isRequestDrivenOperationBatch,
  isPushOperationBatch,

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
  });

  it('should export all known message types including Phase 7.4 anti-entropy', () => {
    expect(KNOWN_MESSAGE_TYPES.size).toBe(17);
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

    // Wire subprotocol constants
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.CONTROL_EVENT)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.OPERATION_BATCH)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.STATE_DIGEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.REPAIR_REQUEST)).toBe(true);
    expect(KNOWN_MESSAGE_TYPES.has(SYNC_MESSAGE_TYPES.REPAIR_REJECTED)).toBe(true);
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
    expect(typeof PushOperationBatchPayloadSchema.safeParse).toBe('function');
    expect(typeof CompatibilityOperationBatchPayloadSchema.safeParse).toBe('function');
    expect(typeof RepairRequestPayloadSchema.safeParse).toBe('function');
    expect(typeof RepairRejectedPayloadSchema.safeParse).toBe('function');
    expect(typeof ReplicationVectorSchema.safeParse).toBe('function');
    expect(typeof SnapshotFrontierSchema.safeParse).toBe('function');
    expect(typeof HistoryFingerprintsSchema.safeParse).toBe('function');
    expect(typeof CompactedCheckpointItemSchema.safeParse).toBe('function');
    expect(typeof StateSnapshotPayloadV2Schema.safeParse).toBe('function');
    expect(typeof StateDigestWireSchema.safeParse).toBe('function');
    expect(typeof UUIDv4Schema.safeParse).toBe('function');
    expect(UUID_V4_REGEX instanceof RegExp).toBe(true);
  });

  it('should export validation and size helper functions', () => {
    expect(typeof validateMessage).toBe('function');
    expect(typeof validatePayload).toBe('function');
    expect(typeof checkMessageSize).toBe('function');
    expect(typeof checkPayloadSize).toBe('function');
    expect(typeof parseAndValidateMessage).toBe('function');
    expect(typeof computeUtf8ByteLength).toBe('function');
    expect(typeof serializeAndCheckSize).toBe('function');
    expect(typeof isLegacyStateDigest).toBe('function');
    expect(typeof isStateDigestV2).toBe('function');
    expect(typeof isRequestDrivenOperationBatch).toBe('function');
    expect(typeof isPushOperationBatch).toBe('function');
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
