# Phase 7.5 — Fault Tolerance & State Recovery: Interface Specification

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `30b8e5f`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Architectural Context and Design Principles

Phase 7.5 specifies the recovery mechanisms required when a peer experiences conditions outside the bounded retention horizon of the Phase 7.4 anti-entropy log repair protocol. This interface specification defines all wire-level message contracts, Zod schemas, runtime component interfaces, configuration structures, error taxonomies, and integration hooks.

### 1.1 Non-Negotiable Invariants
1. **Preservation of Frozen Boundaries:** Existing interfaces established in Phase 7.3 (core engine, ordered operation log, logical clock) and Phase 7.4 (anti-entropy manager, repair pipeline, supplier service, transport adapter) are frozen and MUST NOT be broken or altered.
2. **Capability-Gated Transport Provenance:** Outbound and inbound snapshot operations must route through `TrustedTransportAdapter` using unforgeable token capabilities (`EpochTransitionCapability`, `TransportRegistrationCapability`, `AuthenticatedSupplierContext`, `AuthenticatedRequesterContext`).
3. **Iterative SCTP Wire Measurement:** Initial chunk size estimation uses `calculateDynamicChunkSize()`, but the normative send boundary requires exact UTF-8 serialization measurement and iterative binary reduction before calling `RTCDataChannel.send()`.
4. **Historical Prefix Compatibility:** Single-operation fingerprints at sequence $k$ do not commit to historical chains. Compatibility requires verifying all common retained checkpoints across the prefix $[1, k]$; any unverified gaps trigger `REQUIRES_HISTORY_REPAIR` and fail to `DEGRADED`.
5. **Exact Phase 7.2 Lamport Invariant Reinstated:**
   $$S.\text{maxLogicalTimestamp} \equiv \max_{p \in \text{origins}(V_S)} S.\text{originFrontierTimestamps}[p]$$
6. **Staged Immutable State Replacement:** Snapshot installation constructs new engine instances out-of-band and performs a single synchronous reference swap. External listeners are notified only post-swap and post-reconciliation.

---

## 2. Wire Protocol Schemas and Payloads

All recovery wire messages extend or conform to `@syncplay/protocol`.

### 2.1 Identifiers, Constants, and Dynamic Sizing Heuristic

```typescript
import { z } from 'zod';
import { UUIDv4Schema, StateSnapshotPayloadV2Schema, CompactedCheckpointItemSchema } from './repair.js';

/** Maximum snapshot payload size: 4 MB (128 chunks of up to 32 KB each). */
export const MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024; // 4,194,304 bytes

/** Absolute maximum chunk payload size: 32 KB. */
export const MAX_CHUNK_BYTES = 32 * 1024; // 32,768 bytes

/** Minimum allowable chunk binary payload size before aborting transfer: 256 bytes. */
export const MIN_CHUNK_BYTES = 256;

/** Maximum number of chunks per snapshot transfer: 128. */
export const MAX_CHUNKS_PER_SNAPSHOT = 128;

/** Maximum concurrent chunk requests permitted per transfer window. */
export const MAX_CONCURRENT_CHUNK_REQUESTS = 4;

/** Maximum capacity of live operation quarantine buffer. */
export const MAX_QUARANTINED_OPERATIONS = 1000;

/** Minimum supported SCTP message limit for chunked transfer. */
export const MIN_SUPPORTED_SCTP_LIMIT = 2048;

/** Envelope budget in bytes for SNAPSHOT_CHUNK metadata, UUIDs, and Base64 padding. */
export const CHUNK_ENVELOPE_BUDGET_BYTES = 768;

/**
 * Reason codes for aborting a snapshot recovery transfer.
 */
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
]);

export type SnapshotAbortReason = z.infer<typeof SnapshotAbortReasonSchema>;

/**
 * Heuristic initial estimate for chunk payload size based on negotiated SCTP limit.
 * Normative send boundary MUST iteratively verify exact serialized UTF-8 bytes.
 */
export function calculateDynamicChunkSize(sctpMaxMessageSize?: number): number {
  const effectiveLimit =
    sctpMaxMessageSize !== undefined && sctpMaxMessageSize > 0
      ? Math.min(65536, sctpMaxMessageSize)
      : 65536;

  if (effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT) {
    throw new Error(`SCTP limit (${effectiveLimit} bytes) is below minimum required (${MIN_SUPPORTED_SCTP_LIMIT} bytes)`);
  }

  const availableBase64 = Math.max(0, effectiveLimit - CHUNK_ENVELOPE_BUDGET_BYTES);
  const maxBinaryBytes = Math.floor((availableBase64 * 3) / 4);
  return Math.min(MAX_CHUNK_BYTES, maxBinaryBytes);
}
```

---

### 2.2 `SNAPSHOT_OFFER` Payload

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

### 2.3 `SNAPSHOT_REQUEST` Payload

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

### 2.4 `SNAPSHOT_CHUNK` Payload

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
  chunkSha256: z.string().regex(/^[0-9a-f]{64}$/i),
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

### 2.5 `SNAPSHOT_CHUNK_REQUEST` Payload

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

### 2.6 `SNAPSHOT_ABORT` Payload

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

## 3. Core Component Interfaces

### 3.1 Membership View & Compaction Types

```typescript
export interface PeerConnectionDescriptor {
  readonly peerId: string;
  readonly generation: number;
}

export interface CapturedMembershipView {
  readonly epoch: number;
  readonly connectedPeers: ReadonlyArray<PeerConnectionDescriptor>;
}
```

---

### 3.2 Recovery State Machine (`IRecoveryStateMachine`)

```typescript
export type RecoveryState = 'ACTIVE' | 'RECOVERING' | 'RECONCILING' | 'DEGRADED';

export type RecoveryEvent =
  | { type: 'HISTORY_EXHAUSTED'; reason: string }
  | { type: 'SNAPSHOT_OFFER_RECEIVED'; offer: SnapshotOfferPayload }
  | { type: 'TRANSFER_STARTED'; transferId: string; supplierPeerId: string }
  | { type: 'TRANSFER_COMPLETED'; snapshotBytes: Uint8Array }
  | { type: 'INSTALLATION_SUCCEEDED'; frontier: Record<string, number> }
  | { type: 'RECONCILIATION_COMPLETED' }
  | { type: 'ABORT_RECOVERY'; reason: SnapshotAbortReason }
  | { type: 'EPOCH_CHANGED'; newEpoch: number };

export interface IRecoveryStateMachine {
  readonly currentState: RecoveryState;
  readonly activeTransferId: string | null;
  readonly activeSupplierPeerId: string | null;

  transition(event: RecoveryEvent): RecoveryState;
  canInitiateLocalOperation(): boolean;
  canProcessRemoteOperation(): boolean;
  reset(): void;
}
```

---

### 3.3 Snapshot Transfer Manager (`ISnapshotTransferManager`)

```typescript
export interface TransferProgress {
  readonly transferId: string;
  readonly snapshotId: string;
  readonly supplierPeerId: string;
  readonly totalChunks: number;
  readonly receivedChunks: number;
  readonly totalBytes: number;
  readonly receivedBytes: number;
  readonly percentComplete: number;
}

export interface ISnapshotTransferManager {
  /**
   * Outbound transfer with iterative send boundary measurement.
   */
  startOutboundTransfer(
    request: SnapshotRequestPayload,
    snapshotBytes: Uint8Array,
    context: AuthenticatedSupplierContext
  ): void;

  /**
   * Inbound transfer under strict single-transfer invariant.
   */
  startInboundTransfer(
    offer: SnapshotOfferPayload,
    context: AuthenticatedRequesterContext
  ): Promise<Uint8Array>;

  handleIncomingChunk(
    chunk: SnapshotChunkPayload,
    context: AuthenticatedSupplierContext | AuthenticatedRequesterContext
  ): void;

  handleChunkRequest(
    request: SnapshotChunkRequestPayload,
    context: AuthenticatedSupplierContext
  ): void;

  handleAbort(abort: SnapshotAbortPayload): void;
  getProgress(transferId: string): TransferProgress | null;
  abortActiveTransfer(transferId: string, reason: SnapshotAbortReason): void;
}
```

---

### 3.4 Atomic Snapshot Installer (`ISnapshotInstaller`)

```typescript
export type SnapshotEligibilityResult =
  | { eligible: true; snapshot: StateSnapshotPayloadV2 }
  | { eligible: false; code: 'REJECTED'; reason: string }
  | { eligible: false; code: 'DEGRADED'; reason: string }
  | { eligible: false; code: 'REQUIRES_HISTORY_REPAIR'; reason: string };

export interface SnapshotInstallResult {
  readonly success: boolean;
  readonly snapshotId: string;
  readonly committedFrontier: Readonly<Record<string, number>>;
  readonly compactedCheckpointsLoaded: number;
  readonly reseededLogicalClock: number;
  readonly error?: string;
}

export interface ISnapshotInstaller {
  /**
   * STAGE 1: Pure dry-run pre-commit validation out-of-engine.
   * Verifies schema, canonical SHA-256, epoch currency, vector domination,
   * and complete common checkpoint prefix.
   * ZERO engine state is modified.
   */
  validateCandidate(
    snapshotBytes: Uint8Array,
    expectedSha256: string,
    currentEpoch: number
  ): Promise<SnapshotEligibilityResult>;

  /**
   * STAGE 2 & 3: Constructs staged instances and executes atomic reference swap.
   * Synchronously swaps engine references in a single run-to-completion step.
   * External listeners are deferred until reconciliation completes.
   */
  installSnapshotAtomic(
    snapshot: StateSnapshotPayloadV2,
    epochCapability: EpochTransitionCapability
  ): SnapshotInstallResult;
}
```

---

### 3.5 Recovery Coordinator (`IRecoveryCoordinator`)

```typescript
export interface IRecoveryCoordinator {
  readonly state: RecoveryState;

  handleHistoryExhausted(originPeerId: string, fromSequence: number): void;
  handleSnapshotOffer(offer: SnapshotOfferPayload, context: AuthenticatedRequesterContext): void;
  quarantineLiveOperation(operation: SerializedControlOperation): boolean;
  drainQuarantineQueue(): Promise<void>;
  abortRecovery(reason: SnapshotAbortReason): void;
  dispose(): void;
}
```

---

## 4. Open Decisions Register

| ID | 1. Question | 2. Why It Matters | 3. Available Alternatives | 4. Recommended Choice | 5. Can Proceed to M3? |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **OD-7.5-1** | How should the 1,000-operation quarantine buffer handle overflow during transfer? | High-traffic churn during long transfers could cause unbounded memory bloat if unconstrained. | **Alt A:** Drop older idempotent events.<br/>**Alt B:** Synchronously abort transfer to `DEGRADED`. | **Alt B (Abort to DEGRADED):** Preserves strict causal correctness; prevents partial state corruption. | **YES** (Decided: Alt B) |
| **OD-7.5-2** | How should `SNAPSHOT_OFFER` messages be advertised? | Flooding offers on every timer tick wastes WebRTC bandwidth on mobile/constrained clients. | **Alt A:** Periodic broadcast.<br/>**Alt B:** Request-driven / targeted responses to `onHistoryExhausted`. | **Alt B (Request-driven):** Eliminates unneeded signaling overhead; offers are published only when missing history is declared. | **YES** (Decided: Alt B) |
| **OD-7.5-3** | How should Explicit Manual Reset be exposed to applications when in `DEGRADED` state? | When two partitions have incomparable histories, users need a way to force convergence without causing silent causal overwrites. | **Alt A:** Automatic tie-breaking (e.g. lowest peerId).<br/>**Alt B:** Provide explicit `resetSession({ newSessionId })` API with UI prompt.<br/>**Alt C:** Defer Manual Reset to Phase 8. | **Alt C (Defer to Phase 8):** Phase 7.5 provides the mathematical safety, fencing, and `DEGRADED` state. Application-level session recreation is a Phase 8 SDK concern. | **YES** (Decided: Alt C) |
| **OD-7.5-4** | What is the minimum supported SCTP max message size? | If a WebRTC link negotiates an extremely small MTU ($< 2\text{ KB}$), chunk framing overhead exceeds data payload. | **Alt A:** Reject connections with SCTP limit $< 64\text{ KB}$.<br/>**Alt B:** Support down to 2,048 bytes; abort if $< 2048$ bytes. | **Alt B:** Supports standard constrained networks (e.g. 16 KB) while protecting against pathologically tiny MTUs. | **YES** (Decided: Alt B) |
| **OD-7.5-5** | How should unverified intermediate sequence gaps below compaction horizon be resolved? | Since checkpoints do not chain previous operations, missing checkpoints mean history cannot be verified. | **Alt A:** Assume equality if vector dominates.<br/>**Alt B:** Declare `REQUIRES_HISTORY_REPAIR` and fail to `DEGRADED` if unrepairable. | **Alt B (Require repair / fail to DEGRADED):** Prevents silent branch overwriting when intermediate divergence exists. | **YES** (Decided: Alt B) |
