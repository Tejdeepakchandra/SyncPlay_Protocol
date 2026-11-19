/**
 * Transport-dependent snapshot admission and chunk sizing logic.
 *
 * Implements Phase 7.5 M4 Dynamic Chunk Sizing and Admission Gate.
 */
import {
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_ENVELOPE_OVERHEAD_BYTES,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
} from './constants.js';
import type { SnapshotAbortReason } from './payloads/snapshot-recovery.js';

/**
 * Result of upfront transfer admission evaluation.
 */
export interface SnapshotAdmissionResult {
  readonly eligible: boolean;
  readonly requiredChunks: number;
  readonly chunkBinaryCapacity: number;
  readonly nominalChunkSizeBytes: number;
  readonly effectiveLimit: number;
  readonly reason?: SnapshotAbortReason;
  readonly detail?: string;
}

/**
 * Calculates the exact binary capacity per chunk for a given effective message limit.
 * Accounts for complete ProtocolMessage envelope overhead, Base64 expansion,
 * chunk indices, UUIDs, type strings, timestamps, signatures, and hash hex strings.
 *
 * Guaranteed to never underestimate the required chunk count or overshoot wire limits.
 *
 * Note on 2,144 B transport floor (F-5):
 * At MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER (2,144 B), available Base64 characters = 2,144 - 1,800 = 344 B.
 * Math.floor(344 / 4) * 3 = 86 * 3 = 258 B.
 * 258 B is the exact calculated capacity at the 2,144 B transport floor, satisfying
 * MIN_RAW_CHUNK_BYTES (256 B) since 258 >= 256, with ceil(258 / 3) * 4 = 344 B Base64 framing.
 */
export function calculateChunkCapacity(effectiveLimit: number): number {
  if (effectiveLimit < MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER) {
    throw new Error(
      `Effective limit (${effectiveLimit} B) is below minimum supported transport limit (${MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER} B)`,
    );
  }

  const availableBase64 = Math.max(0, effectiveLimit - MAX_ENVELOPE_OVERHEAD_BYTES);
  // Base64 expands 3 binary bytes to 4 characters; floor to complete 3-byte triples
  const maxBinaryBytes = Math.floor(availableBase64 / 4) * 3;
  return Math.min(MAX_CHUNK_BYTES, maxBinaryBytes);
}

/**
 * Exact, conservative admission evaluation for snapshot transfers.
 * Validates:
 * 1. effectiveLimit >= MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER (2144 B) -> TRANSPORT_LIMIT_EXCEEDED
 * 2. totalSizeBytes <= MAX_SNAPSHOT_BYTES (4,194,304 B) -> LOCAL_ABORT
 * 3. chunkBinaryCapacity >= MIN_RAW_CHUNK_BYTES (256 B) -> TRANSPORT_LIMIT_EXCEEDED
 * 4. calculatedRequiredChunks <= MAX_CHUNKS_PER_SNAPSHOT (128) -> CHUNK_COUNT_EXCEEDED
 * 5. If advertisedTotalChunks provided: advertisedTotalChunks === calculatedRequiredChunks -> OFFER_INCONSISTENT
 *
 * Guarantees rejection before allocating full reassembly buffers.
 */
export function calculateSnapshotAdmission(
  totalSizeBytes: number,
  sctpMaxMessageSize?: number,
  advertisedTotalChunks?: number,
  options?: { allowSctpFallback?: boolean; advertisedNominalChunkSizeBytes?: number },
): SnapshotAdmissionResult {
  // If unnegotiated or unavailable in production, defer or reject
  if (
    sctpMaxMessageSize === undefined ||
    sctpMaxMessageSize <= 0 ||
    !Number.isFinite(sctpMaxMessageSize)
  ) {
    if (!options?.allowSctpFallback) {
      return {
        eligible: false,
        requiredChunks: 0,
        chunkBinaryCapacity: 0,
        nominalChunkSizeBytes: 0,
        effectiveLimit: 0,
        reason: 'TRANSPORT_LIMIT_EXCEEDED',
        detail: 'SCTP transport is unnegotiated or maxMessageSize is unavailable',
      };
    }
  }

  // Resolve effective limit: clamp valid SCTP limit against local framing policy cap of 65,536 bytes
  const fallbackLimit = 65536;
  const effectiveLimit =
    sctpMaxMessageSize !== undefined && sctpMaxMessageSize > 0 && Number.isFinite(sctpMaxMessageSize)
      ? Math.min(fallbackLimit, sctpMaxMessageSize)
      : fallbackLimit;

  if (effectiveLimit < MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER) {
    return {
      eligible: false,
      requiredChunks: 0,
      chunkBinaryCapacity: 0,
      nominalChunkSizeBytes: 0,
      effectiveLimit,
      reason: 'TRANSPORT_LIMIT_EXCEEDED',
      detail: `Effective limit (${effectiveLimit} B) is below minimum supported limit (${MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER} B)`,
    };
  }

  if (totalSizeBytes <= 0 || totalSizeBytes > MAX_SNAPSHOT_BYTES) {
    return {
      eligible: false,
      requiredChunks: 0,
      chunkBinaryCapacity: 0,
      nominalChunkSizeBytes: 0,
      effectiveLimit,
      reason: 'LOCAL_ABORT',
      detail: `Total size (${totalSizeBytes} B) is non-positive or exceeds maximum permitted (${MAX_SNAPSHOT_BYTES} B)`,
    };
  }

  const chunkCapacity = calculateChunkCapacity(effectiveLimit);
  if (chunkCapacity < MIN_RAW_CHUNK_BYTES) {
    return {
      eligible: false,
      requiredChunks: 0,
      chunkBinaryCapacity: chunkCapacity,
      nominalChunkSizeBytes: chunkCapacity,
      effectiveLimit,
      reason: 'TRANSPORT_LIMIT_EXCEEDED',
      detail: `Chunk binary capacity (${chunkCapacity} B) is below minimum raw chunk size (${MIN_RAW_CHUNK_BYTES} B)`,
    };
  }

  const calculatedRequiredChunks = Math.ceil(totalSizeBytes / chunkCapacity);

  if (calculatedRequiredChunks > MAX_CHUNKS_PER_SNAPSHOT) {
    return {
      eligible: false,
      requiredChunks: calculatedRequiredChunks,
      chunkBinaryCapacity: chunkCapacity,
      nominalChunkSizeBytes: chunkCapacity,
      effectiveLimit,
      reason: 'CHUNK_COUNT_EXCEEDED',
      detail: `Required chunks (${calculatedRequiredChunks}) exceeds maximum allowed (${MAX_CHUNKS_PER_SNAPSHOT}) on ${effectiveLimit} B transport`,
    };
  }

  if (advertisedTotalChunks !== undefined && advertisedTotalChunks !== calculatedRequiredChunks) {
    return {
      eligible: false,
      requiredChunks: calculatedRequiredChunks,
      chunkBinaryCapacity: chunkCapacity,
      nominalChunkSizeBytes: chunkCapacity,
      effectiveLimit,
      reason: 'OFFER_INCONSISTENT',
      detail: `Advertised totalChunks (${advertisedTotalChunks}) differs from calculatedRequiredChunks (${calculatedRequiredChunks})`,
    };
  }

  if (
    options?.advertisedNominalChunkSizeBytes !== undefined &&
    options.advertisedNominalChunkSizeBytes !== chunkCapacity
  ) {
    return {
      eligible: false,
      requiredChunks: calculatedRequiredChunks,
      chunkBinaryCapacity: chunkCapacity,
      nominalChunkSizeBytes: chunkCapacity,
      effectiveLimit,
      reason: 'OFFER_INCONSISTENT',
      detail: `Advertised nominalChunkSizeBytes (${options.advertisedNominalChunkSizeBytes}) differs from calculated chunkBinaryCapacity (${chunkCapacity})`,
    };
  }

  return {
    eligible: true,
    requiredChunks: calculatedRequiredChunks,
    chunkBinaryCapacity: chunkCapacity,
    nominalChunkSizeBytes: chunkCapacity,
    effectiveLimit,
  };
}
