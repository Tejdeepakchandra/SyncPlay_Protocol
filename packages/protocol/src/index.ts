/**
 * @module @syncplay/protocol
 *
 * Shared protocol package for SyncPlay.
 * Single source of truth for message schemas, envelope types,
 * identity types, and runtime validation.
 *
 * No module should copy these types locally.
 */

// ── Constants ────────────────────────────────────────────────
export {
  PROTOCOL_VERSION,
  MAX_MESSAGE_BYTES,
  MAX_PAYLOAD_BYTES,
  MAX_BATCH_PAYLOAD_BYTES,
  MAX_OPERATIONS_PER_BATCH,
  MAX_VECTOR_ENTRIES,
} from './constants.js';

// ── Identity types ───────────────────────────────────────────
export type {
  PeerId,
  SessionId,
  SocketId,
  MessageId,
  LogicalEventId,
  ConnectionGeneration,
} from './identity.js';

// ── Message type registry ────────────────────────────────────
export {
  MessageType,
  SYNC_MESSAGE_TYPES,
  KNOWN_MESSAGE_TYPES,
  REQUIRES_LOGICAL_EVENT_ID,
} from './message-types.js';
export type { SyncMessageType } from './message-types.js';

// ── Protocol envelope ────────────────────────────────────────
export type { ProtocolMessage } from './envelope.js';
export { ProtocolMessageSchema } from './envelope.js';

// ── Payload types and schemas ────────────────────────────────
export type {
  PingPayload,
  PongPayload,
} from './payloads/ping-pong.js';
export {
  PingPayloadSchema,
  PongPayloadSchema,
} from './payloads/ping-pong.js';

export type {
  PlayPayload,
  PausePayload,
  SeekPayload,
  RateChangePayload,
} from './payloads/control.js';
export {
  PlayPayloadSchema,
  PausePayloadSchema,
  SeekPayloadSchema,
  RateChangePayloadSchema,
} from './payloads/control.js';

export type {
  StateRequestPayload,
  StateSnapshotPayload,
  StateDigestPayloadV1,
  LegacyStateDigestPayload,
  StateRequestReason,
} from './payloads/state.js';
export {
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadV1Schema,
  LegacyStateDigestPayloadSchema,
  isLegacyStateDigest,
  STATE_REQUEST_REASONS,
} from './payloads/state.js';

export type {
  StateDigestPayload,
  StateDigestPayloadV2,
  SerializedControlOperation,
  OperationBatchPayload,
  PushOperationBatchPayload,
  CompatibilityOperationBatchPayload,
  RepairRequestPayload,
  RepairRejectedPayload,
  RepairRejectionReason,
  CompactedCheckpointItem,
  StateSnapshotPayloadV2,
  ContiguousSequenceBatchData,
} from './payloads/repair.js';
export {
  UUID_V4_REGEX,
  UUIDv4Schema,
  StateDigestPayloadSchema,
  StateDigestPayloadV2Schema,
  isStateDigestV2,
  isRequestDrivenOperationBatch,
  isPushOperationBatch,
  SerializedControlOperationSchema,
  OperationBatchPayloadSchema,
  PushOperationBatchPayloadSchema,
  CompatibilityOperationBatchPayloadSchema,
  RepairRequestPayloadSchema,
  RepairRejectedPayloadSchema,
  ReplicationVectorSchema,
  SnapshotFrontierSchema,
  HistoryFingerprintsSchema,
  CompactedCheckpointItemSchema,
  StateSnapshotPayloadV2Schema,
  REPAIR_REJECTION_REASONS,
  validateContiguousSequenceBatch,
} from './payloads/repair.js';

// ── Payload schema registry ─────────────────────────────────
export { PAYLOAD_SCHEMAS, StateDigestWireSchema } from './payloads/index.js';

// ── Result types ─────────────────────────────────────────────
export type {
  Result,
  ValidationError,
  ValidationErrorCode,
} from './result.js';

// ── Validation ───────────────────────────────────────────────
export type { SerializedPayload } from './validation.js';
export {
  validateMessage,
  validatePayload,
  checkMessageSize,
  checkPayloadSize,
  parseAndValidateMessage,
  computeUtf8ByteLength,
  serializeAndCheckSize,
} from './validation.js';
