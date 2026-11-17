/**
 * Message type registry.
 *
 * This is the authoritative list of known message types for the protocol.
 * Every valid ProtocolMessage must have a messageType from this registry.
 *
 * Categories (from PEER_MESH_RTT_ENGINE_LLD §6):
 * - Transport/diagnostics: PING, PONG
 * - Media control: PLAY, PAUSE, SEEK, RATE_CHANGE
 * - State exchange: STATE_REQUEST, STATE_SNAPSHOT, STATE_DIGEST
 *
 * Additional types (PEER_STATUS, REJOIN, RECOVERY_HINT) will be added
 * when their respective modules are implemented.
 */
export const MessageType = {
  // Transport / diagnostics
  PING: 'PING',
  PONG: 'PONG',

  // Media control
  PLAY: 'PLAY',
  PAUSE: 'PAUSE',
  SEEK: 'SEEK',
  RATE_CHANGE: 'RATE_CHANGE',

  // State exchange
  STATE_REQUEST: 'STATE_REQUEST',
  STATE_SNAPSHOT: 'STATE_SNAPSHOT',
  STATE_DIGEST: 'STATE_DIGEST',

  // Anti-entropy repair (Phase 7)
  OPERATION_BATCH: 'OPERATION_BATCH',
  REPAIR_REQUEST: 'REPAIR_REQUEST',
  REPAIR_REJECTED: 'REPAIR_REJECTED',
} as const;

/** Union of all known message type string values. */
export type MessageType = (typeof MessageType)[keyof typeof MessageType];

/**
 * Wire-level subprotocol message type constants for anti-entropy and sync control.
 * Implements Phase 7.4 Interface Specification §1.1.
 */
export const SYNC_MESSAGE_TYPES = {
  // Live control event wire envelope type
  CONTROL_EVENT: 'control:event',
  // Historical operation batch fulfilling a repair request
  OPERATION_BATCH: 'sync:operation-batch',
  // Anti-entropy replication vector & tail fingerprint digest
  STATE_DIGEST: 'sync:state-digest',
  // Targeted missing sequence range repair request
  REPAIR_REQUEST: 'sync:repair-request',
  // Explicit negative acknowledgement for unfulfillable repair request
  REPAIR_REJECTED: 'sync:repair-rejected',
} as const;

export type SyncMessageType = (typeof SYNC_MESSAGE_TYPES)[keyof typeof SYNC_MESSAGE_TYPES];

/** Set of all known message type strings, for runtime validation. */
export const KNOWN_MESSAGE_TYPES: ReadonlySet<string> = new Set([
  ...Object.values(MessageType),
  ...Object.values(SYNC_MESSAGE_TYPES),
]);

/**
 * Message types that require logicalEventId.
 *
 * Per CONTROL_EVENTS_LAYER_LLD §5: control events represent user
 * operations that may be retried, and each operation must have a
 * stable logicalEventId for deduplication.
 *
 * Transport messages (PING/PONG) and state exchange messages
 * do not represent user operations and do not require it.
 */
export const REQUIRES_LOGICAL_EVENT_ID: ReadonlySet<string> = new Set([
  MessageType.PLAY,
  MessageType.PAUSE,
  MessageType.SEEK,
  MessageType.RATE_CHANGE,
]);
