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
 *
 * NOTE: This is an application-level batch payload cap derived from RFC 8831 / RFC 8841
 * SCTP interoperability recommendations, NOT a universal browser WebRTC guarantee.
 *
 * Standard WebRTC 1.0 (W3C) defines negotiated message size limits on the underlying
 * SCTP transport: `RTCPeerConnection.sctp?.maxMessageSize` (or the default 65,536 bytes if not signaled),
 * rather than on `RTCDataChannel`. The SCTP layer handles lower-level packet fragmentation
 * and reassembly, but application-level frames exceeding the remote peer's negotiated
 * `sctp?.maxMessageSize` will truncate or fail at the transport boundary.
 *
 * In Milestone M5 (Transport Integration), the transport adapter layer must inspect
 * `RTCPeerConnection.sctp?.maxMessageSize` (handling undefined gracefully with 65,536 fallback)
 * and paginate, slice, or sub-batch frames accordingly so that the serialized envelope
 * stays strictly within min(65,536, sctp.maxMessageSize).
 */
export const MAX_BATCH_PAYLOAD_BYTES = 65_536;

/**
 * Maximum number of operations permitted in a single OPERATION_BATCH or REPAIR_REQUEST.
 * Hard limit enforced by Zod schema and engine pagination.
 */
export const MAX_OPERATIONS_PER_BATCH = 32;

/**
 * Maximum number of origin entries permitted in a single StateDigest replicationVector,
 * snapshotFrontier, or historyFingerprints collection.
 *
 * Architectural basis:
 * - SyncPlay rooms are configured for 4-10 concurrent peers (default: 4, absolute ceiling: 50
 *   in BootstrapServer).
 * - Full-mesh WebRTC topologies are practically bounded to 10-16 active peers due to O(N^2) links.
 * - An upper bound of 64 accommodates all active participants plus historical departed/churned peers
 *   over the session lifetime prior to snapshot compaction.
 * - Caps serialized StateDigest size to ~7 KB, preserving ample headroom within MAX_PAYLOAD_BYTES (32 KB).
 */
export const MAX_VECTOR_ENTRIES = 64;
