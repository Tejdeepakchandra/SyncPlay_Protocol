/**
 * Protocol constants.
 *
 * Size limits are provisional starting values for the MVP.
 * They must be configurable in production and measured against
 * actual SDP/ICE and control-message sizes.
 */

/** Current protocol version. Reject messages with a different version. */
export const PROTOCOL_VERSION = 1;

/**
 * Maximum total serialized message size in bytes (64 KB = 65,536 bytes).
 * Defined by WebRTC DataChannel maximum interoperable message size (RFC 8831 / RFC 8841 / W3C).
 * Applied before JSON parsing at the network boundary.
 */
export const MAX_MESSAGE_BYTES = 65_536;

/**
 * Maximum payload size in bytes for standard control events (32 KB).
 * Leaves headroom for envelope fields and serialization overhead.
 */
export const MAX_PAYLOAD_BYTES = 32_768;

/**
 * Maximum application payload size for anti-entropy operation batches (64 KB = 65,536 bytes).
 * WebRTC DataChannel interoperability limit.
 */
export const MAX_BATCH_PAYLOAD_BYTES = 65_536;

/**
 * Maximum number of operations permitted in a single OPERATION_BATCH or REPAIR_REQUEST.
 * Hard limit enforced by Zod schema and engine pagination.
 */
export const MAX_OPERATIONS_PER_BATCH = 32;
