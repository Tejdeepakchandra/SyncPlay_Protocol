# Phase 7.5 Milestone M4 — Snapshot Transfer Interface Specification

**Status:** APPROVED FOR PLANNING (DOCUMENTATION-ONLY)  
**Date:** 2026-11-19  
**Refinement Date:** 2026-11-19  
**Branch Baseline:** `main` at `3b90116`  
**Parent Implementation Baseline:** `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`  
**Specification Baseline:** `119da194c3feb883cfd61b6928cb81d4b200ac5d`  

---

## 1. Constants and Numerical Limits

```typescript
/** Maximum snapshot payload size: 4 MB (128 chunks of up to 32 KB each). */
export const MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024; // 4,194,304 bytes

/** Absolute maximum chunk binary payload size: 32 KB. */
export const MAX_CHUNK_BYTES = 32 * 1024; // 32,768 bytes

/** Minimum allowable chunk binary payload size before aborting transfer: 256 bytes. */
export const MIN_CHUNK_BYTES = 256;

/** Regular expression restricting protocol identifiers to an explicit ASCII-safe alphabet (Option A). */
export const ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/;

/** Minimum transport message limit required to support a full 32 KB binary chunk: 43,692 B Base64 + 1,800 B envelope = 45,492 B. */
export const MIN_TRANSPORT_LIMIT_FULL_CHUNK = 45492; // 45,492 bytes (44.43 KiB)

/** Maximum number of chunks allowed per snapshot transfer. */
export const MAX_CHUNKS_PER_SNAPSHOT = 128;

/** Maximum concurrent in-flight chunk requests allowed per transfer. */
export const MAX_CONCURRENT_CHUNK_REQUESTS = 4;

/** Minimum supported SCTP message limit for chunked snapshot transfer: 1,800 B envelope + 344 B Base64 (256 B raw) = 2,144 bytes. */
export const MIN_SUPPORTED_SCTP_LIMIT = 2144;

/** Envelope budget in bytes for SNAPSHOT_CHUNK metadata, UUIDs, and Base64 framing overhead. */
export const CHUNK_ENVELOPE_BUDGET_BYTES = 768;

/** Guaranteed maximum envelope overhead in bytes for snapshot chunk wire messages (proven upper bound across maximum permitted schema field lengths under Option A). */
export const MAX_ENVELOPE_OVERHEAD_BYTES = 1800;

/** Hard operational bound on sizing loop attempts per chunk. */
export const MAX_SIZING_ITERATIONS = 5;

/** Conservative theoretical upper bound on peak transient memory during 4 MiB snapshot recovery. */
export const THEORETICAL_PEAK_MEMORY_BYTES = 32 * 1024 * 1024; // 33,554,432 bytes (~32 MiB)

/** Timeout in milliseconds before missing chunks in the active window are re-requested. */
export const CHUNK_REQUEST_TIMEOUT_MS = 5000;

/** Maximum retransmission attempts allowed for an individual chunk index. */
export const MAX_CHUNK_RETRIES = 3;

/** Maximum cumulative chunk retransmission attempts allowed across an entire transfer attempt. */
export const MAX_TRANSFER_RETRIES = 10;

/** Hard wall-clock transfer deadline in milliseconds (60 seconds). */
export const OVERALL_TRANSFER_TIMEOUT_MS = 60000;

/** Maximum concurrent outbound transfers served by a single supplier. */
export const MAX_SUPPLIER_CONCURRENT_TRANSFERS = 2;
```

---

## 2. Wire Protocol Schemas & Payloads (`@syncplay/protocol`)

### 2.1 Abort Reason Taxonomy

```typescript
import { z } from 'zod';
import { UUIDv4Schema, StateSnapshotPayloadV2Schema } from './repair.js';

export const SnapshotAbortReasonSchema = z.enum([
  'TRANSFER_TIMEOUT',
  'SUPPLIER_DISCONNECTED',
  'EPOCH_MISMATCH',
  'CHUNK_CORRUPTED',
  'HASH_MISMATCH',
  'QUARANTINE_OVERFLOW',
  'SUPPLIER_REJECTED',
  'LOCAL_ABORT',
  'TRANSPORT_LIMIT_EXCEEDED',
  'CHUNK_COUNT_EXCEEDED',
  'OFFER_INCONSISTENT',
  'STALE_SNAPSHOT',
  'CHECKPOINT_DIVERGENCE',
  'IDENTITY_MISMATCH',
  'RETRY_BUDGET_EXHAUSTED',
]);

export type SnapshotAbortReason = z.infer<typeof SnapshotAbortReasonSchema>;
```

---

### 2.2 `SNAPSHOT_OFFER` Wire Payload

Advertised by an authoritative peer when a remote peer's repair request falls outside the retention horizon.

```typescript
export const SnapshotOfferPayloadSchema = z.object({
  offerId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  supplierPeerId: z.string().min(1).max(64),
  snapshotId: z.string().min(1).max(128),
  authorPeerId: z.string().min(1).max(64),
  snapshotFrontier: z.record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER)
  ),
  maxLogicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  totalSizeBytes: z.number().int().positive().max(MAX_SNAPSHOT_BYTES),
  totalChunks: z.number().int().positive().max(MAX_CHUNKS_PER_SNAPSHOT),
  sha256Digest: z.string().regex(/^[0-9a-f]{64}$/i, {
    message: 'Must be canonical 64-character hex SHA-256 digest',
  }),
});

export type SnapshotOfferPayload = z.infer<typeof SnapshotOfferPayloadSchema>;
```

---

### 2.3 `SNAPSHOT_REQUEST` Wire Payload

Issued by a recovering peer to select an offer and initiate or resume chunked transfer.

```typescript
export const SnapshotRequestPayloadSchema = z.object({
  transferId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  requesterPeerId: z.string().min(1).max(64),
  supplierPeerId: z.string().min(1).max(64),
  snapshotId: z.string().min(1).max(128),
  requestedChunkWindow: z.number().int().positive().max(MAX_CONCURRENT_CHUNK_REQUESTS).default(4),
  clientMaxChunkBytes: z.number().int().positive().max(MAX_CHUNK_BYTES).optional(),
  missingChunkIndices: z.array(
    z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1)
  ).max(MAX_CHUNKS_PER_SNAPSHOT).optional(),
});

export type SnapshotRequestPayload = z.infer<typeof SnapshotRequestPayloadSchema>;
```

---

### 2.4 `SNAPSHOT_CHUNK` Wire Payload

Transfers a single chunk from supplier to requester.

```typescript
export const SnapshotChunkPayloadSchema = z.object({
  transferId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  membershipEpoch: z.number().int().nonnegative(),
  supplierPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
  snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  chunkIndex: z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1),
  totalChunks: z.number().int().positive().max(MAX_CHUNKS_PER_SNAPSHOT),
  chunkSizeBytes: z.number().int().positive().max(MAX_CHUNK_BYTES),
  chunkSha256: z.string().regex(/^[0-9a-f]{64}$/i, {
    message: 'Must be 64-character hex SHA-256 of raw chunk binary payload',
  }),
  dataBase64: z.string().min(1).max(Math.ceil((MAX_CHUNK_BYTES * 4) / 3) + 4),
}).superRefine((data, ctx) => {
  if (data.chunkIndex >= data.totalChunks) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `chunkIndex (${data.chunkIndex}) must be strictly less than totalChunks (${data.totalChunks})`,
      path: ['chunkIndex'],
    });
  }
});

export type SnapshotChunkPayload = z.infer<typeof SnapshotChunkPayloadSchema>;
```

---

### 2.5 `SNAPSHOT_CHUNK_REQUEST` Wire Payload

Issues window credit or requests selective retransmission of missing chunk indices.

```typescript
export const SnapshotChunkRequestPayloadSchema = z.object({
  transferId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  requesterPeerId: z.string().min(1).max(64),
  supplierPeerId: z.string().min(1).max(64),
  snapshotId: z.string().min(1).max(128),
  missingChunkIndices: z.array(
    z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1)
  ).min(1).max(MAX_CONCURRENT_CHUNK_REQUESTS),
});

export type SnapshotChunkRequestPayload = z.infer<typeof SnapshotChunkRequestPayloadSchema>;
```

---

### 2.6 `SNAPSHOT_ABORT` Wire Payload

Signals unilateral transfer cancellation.

```typescript
export const SnapshotAbortPayloadSchema = z.object({
  transferId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  senderPeerId: z.string().min(1).max(64),
  snapshotId: z.string().min(1).max(128),
  reason: SnapshotAbortReasonSchema,
  detail: z.string().max(256).optional(),
});

export type SnapshotAbortPayload = z.infer<typeof SnapshotAbortPayloadSchema>;
```

---

## 3. Dynamic Chunk Sizing and Admission Gate Functions

```typescript
/**
 * Result of upfront transfer admission evaluation.
 */
export interface SnapshotAdmissionResult {
  readonly eligible: boolean;
  readonly requiredChunks: number;
  readonly chunkBinaryCapacity: number;
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
 */
export function calculateChunkCapacity(effectiveLimit: number): number {
  if (effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT) {
    throw new Error(
      `SCTP limit (${effectiveLimit} bytes) is below minimum supported limit (${MIN_SUPPORTED_SCTP_LIMIT} bytes)`
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
 * 1. effectiveLimit >= MIN_SUPPORTED_SCTP_LIMIT (2144) -> TRANSPORT_LIMIT_EXCEEDED
 * 2. totalSizeBytes <= MAX_SNAPSHOT_BYTES (4,194,304) -> LOCAL_ABORT
 * 3. calculatedRequiredChunks <= MAX_CHUNKS_PER_SNAPSHOT (128) -> CHUNK_COUNT_EXCEEDED
 * 4. If advertisedTotalChunks provided: advertisedTotalChunks === calculatedRequiredChunks -> OFFER_INCONSISTENT
 *
 * Guarantees rejection before allocating full reassembly buffers.
 */
export function calculateSnapshotAdmission(
  totalSizeBytes: number,
  sctpMaxMessageSize?: number,
  advertisedTotalChunks?: number,
  options?: { allowSctpFallback?: boolean },
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
        effectiveLimit: 0,
        reason: 'TRANSPORT_LIMIT_EXCEEDED',
        detail: 'SCTP transport is unnegotiated or maxMessageSize is unavailable',
      };
    }
  }

  // Resolve effective limit: clamp valid SCTP limit against local framing policy cap of 65,536 bytes
  const fallbackLimit = 65536; // Conservative local implementation policy cap (mock/test environments only)
  const effectiveLimit =
    sctpMaxMessageSize !== undefined && sctpMaxMessageSize > 0 && Number.isFinite(sctpMaxMessageSize)
      ? Math.min(fallbackLimit, sctpMaxMessageSize)
      : fallbackLimit;

  if (effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT) {
    return {
      eligible: false,
      requiredChunks: 0,
      chunkBinaryCapacity: 0,
      effectiveLimit,
      reason: 'TRANSPORT_LIMIT_EXCEEDED',
      detail: `SCTP limit (${effectiveLimit} B) is below minimum supported limit (${MIN_SUPPORTED_SCTP_LIMIT} B)`,
    };
  }

  if (totalSizeBytes <= 0 || totalSizeBytes > MAX_SNAPSHOT_BYTES) {
    return {
      eligible: false,
      requiredChunks: 0,
      chunkBinaryCapacity: 0,
      effectiveLimit,
      reason: 'LOCAL_ABORT',
      detail: `Total size (${totalSizeBytes} B) exceeds maximum permitted (${MAX_SNAPSHOT_BYTES} B)`,
    };
  }

  const chunkCapacity = calculateChunkCapacity(effectiveLimit);
  const calculatedRequiredChunks = Math.ceil(totalSizeBytes / chunkCapacity);

  if (calculatedRequiredChunks > MAX_CHUNKS_PER_SNAPSHOT) {
    return {
      eligible: false,
      requiredChunks: calculatedRequiredChunks,
      chunkBinaryCapacity: chunkCapacity,
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
      effectiveLimit,
      reason: 'OFFER_INCONSISTENT',
      detail: `Advertised totalChunks (${advertisedTotalChunks}) differs from calculatedRequiredChunks (${calculatedRequiredChunks})`,
    };
  }

  return {
    eligible: true,
    requiredChunks: calculatedRequiredChunks,
    chunkBinaryCapacity: chunkCapacity,
    effectiveLimit,
  };
}

/**
 * Compatible initial estimate for chunk payload size based on negotiated SCTP limit.
 * Delegates to calculateChunkCapacity(effectiveLimit).
 *
 * NOTE: The 65,536-byte fallback represents SyncPlay's conservative local framing cap,
 * used ONLY when allowSctpFallback is explicitly enabled for mock test harnesses.
 */
export function calculateDynamicChunkSize(
  sctpMaxMessageSize?: number,
  options?: { allowSctpFallback?: boolean },
): number {
  if (
    sctpMaxMessageSize === undefined ||
    sctpMaxMessageSize <= 0 ||
    !Number.isFinite(sctpMaxMessageSize)
  ) {
    if (!options?.allowSctpFallback) {
      throw new Error('SCTP transport is unnegotiated or maxMessageSize is unavailable');
    }
  }

  const fallbackLimit = 65536;
  const effectiveLimit =
    sctpMaxMessageSize !== undefined && sctpMaxMessageSize > 0 && Number.isFinite(sctpMaxMessageSize)
      ? Math.min(fallbackLimit, sctpMaxMessageSize)
      : fallbackLimit;

  return calculateChunkCapacity(effectiveLimit);
}
```

---

## 4. Component Interfaces

### 4.1 Chunk Assembler Interface (`IChunkAssembler`)

```typescript
export interface ChunkIngestResult {
  readonly accepted: boolean;
  readonly isComplete: boolean;
  readonly duplicate: boolean;
  readonly receivedChunksCount: number;
  readonly totalChunksCount: number;
  readonly error?: string;
}

export interface IChunkAssembler {
  readonly transferId: string;
  readonly snapshotId: string;
  readonly totalChunks: number;
  readonly totalSizeBytes: number;
  readonly expectedSha256: string;
  readonly isComplete: boolean;

  /** Checks if a specific chunk index has already been received and verified. */
  hasChunk(chunkIndex: number): boolean;

  /** Ingests a single chunk, validates Base64 and chunkSha256. */
  ingestChunk(chunk: SnapshotChunkPayload): ChunkIngestResult;

  /** Returns bounded list of chunk indices not yet received. */
  getMissingChunkIndices(maxCount?: number): number[];

  /**
   * Evaluates whether retained verified chunks can be preserved for a failover supplier.
   * If snapshotId and expectedSha256 match bit-exact, retained chunks are preserved.
   * If mismatch, all chunks are purged.
   */
  prepareForFailover(newTransferId: string, newSnapshotId: string, newSha256: string): boolean;

  /**
   * Reassembles contiguous Uint8Array, immediately dereferences sparse slice arrays
   * (`this.chunks = null`) to minimize memory duplication before JSON parsing,
   * decodes UTF-8 JSON, validates StateSnapshotPayloadV2Schema, and verifies
   * bit-exact match against canonical RFC 8785 JSON SHA-256 digest.
   */
  finalizeAssembly(): { rawBytes: Uint8Array; payload: StateSnapshotPayloadV2 };

  /**
   * Clears all memory references (`this.chunks = null`, `this.assembledBuffer = null`).
   * Explicitly notes that JavaScript garbage collection is asynchronous and non-deterministic;
   * nullifying references ensures immediate eligibility for V8 GC without leaks.
   */
  dispose(): void;
}
```

---

### 4.2 Snapshot Transfer Manager Interface (`ISnapshotTransferManager`)

```typescript
export interface TransferProgress {
  readonly transferId: string;
  readonly snapshotId: string;
  readonly role: 'SUPPLIER' | 'REQUESTER';
  readonly remotePeerId: string;
  readonly totalChunks: number;
  readonly receivedChunks: number;
  readonly totalBytes: number;
  readonly receivedBytes: number;
  readonly retryCount: number;
  readonly percentComplete: number;
}

export interface SnapshotTransferResult {
  readonly transferId: string;
  readonly snapshotId: string;
  readonly supplierPeerId: string;
  readonly membershipEpoch: number;
  readonly payload: StateSnapshotPayloadV2;
  readonly rawBytes: Uint8Array;
  readonly sha256Digest: string;
}

export interface ISnapshotTransferManager {
  /**
   * Initiates outbound transfer to fulfill an incoming SNAPSHOT_REQUEST.
   * Enforces admission check: rejects if calculated requiredChunks > MAX_CHUNKS_PER_SNAPSHOT (128).
   * Executes normative 7-step iterative sizing loop with MAX_SIZING_ITERATIONS = 5.
   *
   * Synchronous send handling: If sendSerialized() returns { ok: false }, chunk is NOT marked as sent,
   * retry counter is incremented, transfer counters are preserved, and failure policy is enforced.
   */
  startOutboundTransfer(
    request: SnapshotRequestPayload,
    snapshotBytes: Uint8Array,
    fromPeerId: string,
    generation: number
  ): void;

  /**
   * Initiates inbound transfer to accept an incoming SNAPSHOT_OFFER.
   * Enforces upfront admission check via calculateSnapshotAdmission():
   * Rejects immediately before allocating reassembly buffer if requiredChunks > 128
   * or offer.totalChunks !== calculatedRequiredChunks.
   */
  startInboundTransfer(
    offer: SnapshotOfferPayload,
    fromPeerId: string,
    generation: number
  ): Promise<SnapshotTransferResult>;

  /** Fails over an interrupted transfer to a replacement supplier offering the same snapshot. */
  failoverInboundTransfer(
    offer: SnapshotOfferPayload,
    fromPeerId: string,
    generation: number
  ): Promise<SnapshotTransferResult>;

  /** Handles incoming SNAPSHOT_CHUNK. */
  handleIncomingChunk(chunk: SnapshotChunkPayload, fromPeerId: string): void;

  /** Handles incoming SNAPSHOT_CHUNK_REQUEST (credit/retransmission). */
  handleChunkRequest(request: SnapshotChunkRequestPayload, fromPeerId: string): void;

  /** Handles incoming SNAPSHOT_ABORT. */
  handleAbort(abort: SnapshotAbortPayload, fromPeerId: string): void;

  /** Retrieves active transfer progress. */
  getProgress(transferId: string): TransferProgress | null;

  /** Unilaterally aborts an active transfer. */
  abortActiveTransfer(transferId: string, reason: SnapshotAbortReason, detail?: string): void;

  /**
   * Clears all transfer contexts on engine disposal, timeout, or epoch change.
   * Cancels active timeouts (clearTimeout), deletes retry counters, and dereferences all buffers.
   */
  dispose(): void;
}
```

---

## 5. Pre-M4 Security & Compaction Boundary Corrections

### 5.1 Inbound Identity Binding in `TrustedTransportAdapter` (Limitation A)

```typescript
export interface InboundTransportSecurityCheck {
  verifySenderIdentity(
    inboundFromPeerId: string,
    declaredSenderPeerId: string,
    activeGeneration: number,
    declaredGeneration: number
  ): boolean;
}
```
At transport receipt:
1. `inboundFromPeerId === declaredSenderPeerId` is asserted.
2. `activeGeneration === declaredGeneration` is asserted.
3. Violations result in immediate dropped message with security log warning; no registration in `CompactionManager` or `AntiEntropyManager`.

### 5.2 Exact Frontier Timestamp Resolution Contract (Limitation B)

```typescript
export interface FrontierTimestampResolutionResult {
  readonly timestamps: Record<string, number>;
  readonly exact: boolean;
  readonly unresolvableOrigins: ReadonlyArray<string>;
}
```
In `OrderedOperationLog.getOriginFrontierTimestamps(frontier)`:
- If requested `seq <= 0`: resolve timestamp to 0.
- If exact sequence is found in active log (`originSeqIndex`): resolve to `activeOp.logicalTimestamp`.
- If exact sequence is found in compacted checkpoints (`compactedCheckpoints`): resolve to `checkpoint.logicalTimestamp`.
- If exact sequence is evicted from checkpoints or outside retention horizon: origin is unresolvable (timestamp omitted or marked `null`, origin listed in `unresolvableOrigins`, `exact = false`).
- Fallback to `this.originFrontierTimestamps.get(origin)` is **STRICTLY ELIMINATED**.
- **Invariant:** Never substitute a later sequence's Lamport timestamp for the exact timestamp of an earlier requested sequence.

### 5.3 Transactional Staging for Compaction (Limitation C / Milestone M4.0d)

```typescript
import type { StoredOperation, CompactedCheckpointRecord } from '../log/types.js';

/**
 * Complete staged mutation inventory for transactional log compaction in OrderedOperationLog.
 * Reconciled against all 8 mutable properties of OrderedOperationLog.
 * No active state structure is mutated before all candidate components succeed.
 */
export interface StagedCompactionCandidate {
  /** Deep clone of baseSnapshot folded forward with pruned operations (structuredClone). */
  readonly candidateBaseSnapshot?: PlaybackState;
  /** Shallow array slice containing only retained operations strictly newer than stableFrontier. */
  readonly candidateOperations: StoredOperation[];
  /** Reconstructed index mapping logicalEventId to candidate StoredOperation. */
  readonly candidateIdIndex: Map<string, StoredOperation>;
  /** Reconstructed index mapping originPeerId:sequenceNumber to candidate StoredOperation. */
  readonly candidateOriginSeqIndex: Map<string, StoredOperation>;
  /** Shallow Map clone retaining checkpoints at K=256 intervals up to compaction horizon. */
  readonly candidateCompactedCheckpoints: Map<string, CompactedCheckpointRecord>;
  /** Shallow Map clone updated with latest pruned timestamp per origin (strictly an origin-level tracker, not sequence-indexed). */
  readonly candidateOriginFrontierTimestamps: Map<string, number>;
  /** Shallow Record clone matching candidate base snapshot frontier. */
  readonly candidateBaseSnapshotFrontier: Record<string, number>;
  /** Scalar boolean indicating whether operations have been pruned. */
  readonly candidateHasPrunedOperations: boolean;
}

export interface StagedCompactionResult {
  readonly success: boolean;
  readonly prunedCount: number;
  readonly newBaseSnapshot?: PlaybackState;
  readonly error?: string;
}
```
In `OrderedOperationLog.compactAtFrontier(stableFrontier)`:
- **Phase 1 (Staging):** Construct `StagedCompactionCandidate` out-of-band:
  1. Deep clone `baseSnapshot` via `structuredClone(this.baseSnapshot)`.
  2. Sequentially reduce pruned operations into `candidateBaseSnapshot`: `candidateBaseSnapshot = this.reducer(candidateBaseSnapshot, op)`.
  3. Filter retained operations into `candidateOperations` (shallow array, immutable operation references).
  4. Reconstruct `candidateIdIndex` and `candidateOriginSeqIndex` solely from `candidateOperations`.
  5. Clone `this.compactedCheckpoints` into a new Map, adding new immutable checkpoint records for pruned operations with 256-entry FIFO eviction.
  6. Clone `this.originFrontierTimestamps` into a new Map, updating frontier timestamps with 64-entry FIFO eviction.
  7. Copy `this.baseSnapshotFrontier` into a new record, updating sequence maxima.
- **Phase 2 (Atomic Swap):**
  - **Only if candidate reduction and validation complete without error**, perform atomic reference swap across all 8 mutable fields in a single synchronous block via direct reference assignment:
    ```typescript
    this.baseSnapshot = candidate.candidateBaseSnapshot;
    this.operations = candidate.candidateOperations; // complete reference replacement
    this.idIndex = candidate.candidateIdIndex;
    this.originSeqIndex = candidate.candidateOriginSeqIndex;
    this.compactedCheckpoints = candidate.candidateCompactedCheckpoints;
    this.originFrontierTimestamps = candidate.candidateOriginFrontierTimestamps;
    this.baseSnapshotFrontier = candidate.candidateBaseSnapshotFrontier;
    this.hasPrunedOperations = candidate.candidateHasPrunedOperations;
    ```
- **Formal Failure Invariant:**
  $$\mathbf{\text{CandidateConstructionFailure}} \implies \mathbf{\text{ActiveStateAfter} \equiv \text{ActiveStateBefore}}$$
  If `this.reducer` throws during candidate folding, or candidate construction fails, candidate objects are discarded, the exception is re-thrown, and active engine state remains 100% untouched. (Note: Reducer is contracted to be pure; external reducer side effects cannot be rolled back by the runtime.)

### 5.4 Facade Capability Boundary (Milestone M4.0c)

```typescript
/** In-process capability token for internal digest registration (API custody boundary). */
export const INTERNAL_DIGEST_ACCESS_TOKEN: unique symbol = Symbol('INTERNAL_DIGEST_ACCESS_TOKEN');

export interface IInternalDigestRegistrar {
  recordPeerDigest(token: typeof INTERNAL_DIGEST_ACCESS_TOKEN, digest: StateDigestPayload): void;
}
```
- Restricts `recordPeerDigest()` to internal callers (`TrustedTransportAdapter`, `AntiEntropyManager`).
- **Custody Boundary Scope:** Classified explicitly as an in-process API custody boundary, not a security sandbox.
