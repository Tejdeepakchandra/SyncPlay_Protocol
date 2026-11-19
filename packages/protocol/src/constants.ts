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

/**
 * Maximum snapshot payload size: 4 MB (4,194,304 bytes).
 * Phase 7.5 state recovery snapshot limit (128 chunks of up to 32 KB each).
 */
export const MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024;

/**
 * Regular expression restricting protocol identifiers to an explicit ASCII-safe alphabet (Option A).
 * Matches characters: a-z, A-Z, 0-9, _, ., :, -
 */
export const ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/;

/**
 * Guaranteed maximum envelope overhead in bytes for snapshot chunk wire messages under
 * Option A ASCII identifier restrictions.
 *
 * Covers standard outer envelope, JSON syntax, UUIDs, chunk metadata framing, and SHA-256 hex strings.
 * Empirical maximum under valid permitted schema fields is 1,231 bytes (or 1,194 bytes with standard
 * chunk payload sizes), providing an airtight 569-byte (46%) safety margin to the 1,800-byte bound.
 */
export const MAX_ENVELOPE_OVERHEAD_BYTES = 1800;

/**
 * Minimum allowable chunk binary payload size before aborting transfer: 256 bytes.
 */
export const MIN_RAW_CHUNK_BYTES = 256;

/**
 * Exact Base64 encoded byte length for minimum 256-byte binary chunk:
 * Math.ceil(256 / 3) * 4 = 86 * 4 = 344 bytes.
 */
export const MIN_BASE64_CHUNK_BYTES = 344;

/**
 * Absolute minimum transport message limit required to support chunked snapshot transfer:
 * 1,800 B envelope + 344 B Base64 (256 B raw) = 2,144 bytes (2.09 KiB).
 * Any transport negotiating an SCTP limit below this value cannot carry even the smallest
 * allowable chunk and must be rejected upfront before allocating reassembly buffer memory.
 */
export const MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER = 2144;

/**
 * Minimum transport message limit required to support a full 32 KB binary chunk:
 * 43,692 B Base64 + 1,800 B envelope = 45,492 B (44.43 KiB).
 */
export const MIN_TRANSPORT_LIMIT_FULL_CHUNK = 45492;

/**
 * Absolute maximum chunk binary payload size: 32 KB (32,768 bytes).
 */
export const MAX_CHUNK_BYTES = 32 * 1024;

/**
 * Maximum number of chunks allowed per snapshot transfer.
 */
export const MAX_CHUNKS_PER_SNAPSHOT = 128;

/**
 * Maximum concurrent in-flight chunk requests allowed per transfer.
 */
export const MAX_CONCURRENT_CHUNK_REQUESTS = 4;

/**
 * Minimum supported SCTP message limit for chunked snapshot transfer:
 * 1,800 B envelope + 344 B Base64 (256 B raw) = 2,144 bytes.
 */
export const MIN_SUPPORTED_SCTP_LIMIT = 2144;

/**
 * Timeout in milliseconds before missing chunks in the active window are re-requested.
 */
export const CHUNK_REQUEST_TIMEOUT_MS = 5000;

/**
 * Maximum retransmission attempts allowed for an individual chunk index.
 */
export const MAX_CHUNK_RETRIES = 3;

/**
 * Maximum cumulative chunk retransmission attempts allowed across an entire transfer attempt.
 */
export const MAX_TRANSFER_RETRIES = 10;

/**
 * Hard wall-clock transfer deadline in milliseconds (60 seconds).
 */
export const OVERALL_TRANSFER_TIMEOUT_MS = 60000;

/**
 * Maximum concurrent outbound transfers served by a single supplier.
 */
export const MAX_SUPPLIER_CONCURRENT_TRANSFERS = 2;


