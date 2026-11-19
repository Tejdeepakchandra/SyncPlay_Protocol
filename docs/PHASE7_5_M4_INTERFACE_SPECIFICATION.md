# Phase 7.5 Milestone M4 — Snapshot Transfer Interface Specification

**Status:** APPROVED FOR PLANNING (DOCUMENTATION-ONLY)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`  
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

/** Maximum number of chunks allowed per snapshot transfer. */
export const MAX_CHUNKS_PER_SNAPSHOT = 128;

/** Maximum concurrent in-flight chunk requests allowed per transfer. */
export const MAX_CONCURRENT_CHUNK_REQUESTS = 4;

/** Minimum supported SCTP message limit for chunked snapshot transfer. */
export const MIN_SUPPORTED_SCTP_LIMIT = 2048;

/** Envelope budget in bytes for SNAPSHOT_CHUNK metadata, UUIDs, and Base64 framing overhead. */
export const CHUNK_ENVELOPE_BUDGET_BYTES = 768;

/** Timeout in milliseconds before a single missing chunk is re-requested. */
export const CHUNK_REQUEST_TIMEOUT_MS = 5000;

/** Maximum number of retransmission attempts per chunk before transfer abort. */
export const MAX_CHUNK_RETRIES = 3;

/** Overall transfer timeout in milliseconds. */
export const OVERALL_TRANSFER_TIMEOUT_MS = 60000; // 60 seconds
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
  'STALE_SNAPSHOT',
  'CHECKPOINT_DIVERGENCE',
  'IDENTITY_MISMATCH',
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

Issued by a recovering peer to select an offer and initiate chunked transfer.

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
});

export type SnapshotRequestPayload = z.infer<typeof SnapshotRequestPayloadSchema>;
```

---

### 2.4 `SNAPSHOT_CHUNK` Wire Payload

Transfers a single chunk from supplier to requester.

```typescript
export const SnapshotChunkPayloadSchema = z.object({
  transferId: UUIDv4Schema,
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  supplierPeerId: z.string().min(1).max(64),
  snapshotId: z.string().min(1).max(128),
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

## 3. Dynamic Chunk Sizing Function

```typescript
/**
 * Heuristic initial estimate for chunk payload size based on negotiated SCTP limit.
 * The normative send boundary iteratively verifies exact serialized UTF-8 bytes.
 */
export function calculateDynamicChunkSize(sctpMaxMessageSize?: number): number {
  const effectiveLimit =
    sctpMaxMessageSize !== undefined && sctpMaxMessageSize > 0
      ? Math.min(65536, sctpMaxMessageSize)
      : 65536;

  if (effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT) {
    throw new Error(
      `SCTP limit (${effectiveLimit} bytes) is below minimum supported limit (${MIN_SUPPORTED_SCTP_LIMIT} bytes)`
    );
  }

  const availableBase64 = Math.max(0, effectiveLimit - CHUNK_ENVELOPE_BUDGET_BYTES);
  const maxBinaryBytes = Math.floor((availableBase64 * 3) / 4);
  return Math.min(MAX_CHUNK_BYTES, maxBinaryBytes);
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

  /**
   * Ingests a single chunk, validates Base64 and chunkSha256.
   */
  ingestChunk(chunk: SnapshotChunkPayload): ChunkIngestResult;

  /**
   * Returns list of chunk indices not yet received.
   */
  getMissingChunkIndices(maxCount?: number): number[];

  /**
   * Reassembles contiguous buffer and verifies complete payload SHA-256 digest.
   * Throws Error if incomplete or hash mismatch.
   */
  finalizeAssembly(): { rawBytes: Uint8Array; payload: StateSnapshotPayloadV2 };

  /** Clears memory and aborts assembly. */
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
   * Initiates an outbound transfer to fulfill an incoming SNAPSHOT_REQUEST.
   * Executes normative 7-step iterative send-boundary loop.
   */
  startOutboundTransfer(
    request: SnapshotRequestPayload,
    snapshotBytes: Uint8Array,
    fromPeerId: string,
    generation: number
  ): void;

  /**
   * Initiates an inbound transfer to accept an incoming SNAPSHOT_OFFER.
   * Enforces strict single active transfer invariant.
   */
  startInboundTransfer(
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

  /** Clears all transfer contexts on engine disposal or epoch change. */
  dispose(): void;
}
```

---

## 5. Pre-M4 Security Boundary Corrections

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
- If exact sequence is found in active log or compacted checkpoints: resolve timestamp.
- If not found: mark origin as unresolvable.
- **Never substitute a later or earlier sequence timestamp.**
