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
 * SCTP transport: `RTCPeerConnection.sctp?.maxMessageSize`, NOT on `RTCDataChannel`.
 *
 * Send-Size Contract & Transport Semantics:
 * 1. Negotiated Limit Discovery: The remote peer's maximum incoming message size limit
 *    is read from `RTCPeerConnection.sctp?.maxMessageSize`.
 * 2. Conservative Fallback: If SCTP information is unavailable (e.g. `sctp` is undefined or
 *    `maxMessageSize` is null/unannounced), the implementation must handle it conservatively
 *    by falling back to the standard baseline limit of 65,536 bytes.
 * 3. Pre-Send Byte-Length Verification: Before transmitting an operation batch or repair message
 *    over the DataChannel, the sender must measure the actual UTF-8 byte length of the serialized
 *    message (`serializeAndCheckSize`).
 * 4. Prevention of Oversized Sends: Any send where the serialized UTF-8 byte length exceeds
 *    `min(MAX_BATCH_PAYLOAD_BYTES, sctp.maxMessageSize)` must be prevented upfront by the
 *    application layer or treated as an unrecoverable send failure.
 * 5. No Truncation or Partial Delivery: The application MUST NEVER rely on truncation or partial
 *    delivery. WebRTC DataChannels do not support frame truncation; transmitting an oversized
 *    frame triggers an immediate RTCError / transport failure.
 * 6. SCTP Fragmentation Boundaries: Lower-layer SCTP packet fragmentation and reassembly handles
 *    wire-level packetization, but is NOT a substitute for application-level message-size compliance.
 * 7. Application-Level Slicing: Application-level pagination, slicing, and sub-batching into
 *    contiguous sequence ranges that strictly conform to the negotiated message-size envelope
 *    is an M5 (Transport Integration) responsibility.
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
