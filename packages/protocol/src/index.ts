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
export { MessageType, KNOWN_MESSAGE_TYPES, REQUIRES_LOGICAL_EVENT_ID } from './message-types.js';

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
  StateDigestPayload,
  StateRequestReason,
} from './payloads/state.js';
export {
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadSchema,
  STATE_REQUEST_REASONS,
} from './payloads/state.js';

// ── Payload schema registry ─────────────────────────────────
export { PAYLOAD_SCHEMAS } from './payloads/index.js';

// ── Result types ─────────────────────────────────────────────
export type {
  Result,
  ValidationError,
  ValidationErrorCode,
} from './result.js';

// ── Validation ───────────────────────────────────────────────
export {
  validateMessage,
  validatePayload,
  checkMessageSize,
  checkPayloadSize,
  parseAndValidateMessage,
} from './validation.js';
