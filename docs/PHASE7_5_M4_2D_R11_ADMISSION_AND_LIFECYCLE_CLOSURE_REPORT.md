# Phase 7.5 M4.2d-R11 — Admission Semantics, Evidence Retirement & Lifecycle Consistency Closure Report

**Document Version:** 1.0.0  
**Status:** COMPLETE — READY FOR FINAL INDEPENDENT REVIEW — IMPLEMENTATION STILL UNAUTHORIZED  
**Date:** November 23, 2026  
**Author:** Senior TypeScript Distributed-Systems Architect  
**Scope:** Strictly Documentational & Architectural Correction (Zero Runtime/Test Code Modifications)  
**Authoritative Baseline:** M4.2c-R6 Closure (`79919a0f8cb0bb81cb80cc010fe02918bef79787`)  

---

> [!CRITICAL]
> **AUTHORIZATION BOUNDARY NOTICE**  
> **M4.2d implementation remains strictly unauthorized pending independent review and explicit user approval.**  
> This task is strictly documentational, read-only, and analytical. It introduces NO modifications to runtime source code, NO modifications to test files, NO package export or configuration changes, NO dependency additions, and NO wire schema modifications. Frozen baselines M4.1, M4.2a, M4.2b, and M4.2c remain 100% byte-exact and unmodified.

---

## 1. Baseline Provenance & Working-Tree Cleanliness Verification

### 1.1 Git Working-Tree Verification
- **Current HEAD Commit:** `79919a0f8cb0bb81cb80cc010fe02918bef79787` (M4.2c-R6 Closure)
- **Direct Parent Commit:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5)
- **Active Branch:** `main`
- **Working-Tree Status:** Clean on all tracked source and test files. Zero uncommitted changes in `packages/`.
- **Frozen Baselines Verified (0 diffs):**
  - Frozen M4.1 Baseline: `fd23edae5bdb52cff32674860229277a79cb1aca` (Wire Schemas, Constants, `ChunkAssembler`)
  - Frozen M4.2a Baseline: `e3245912626758c094d83edbd4d9264a922070f5` (`SnapshotSerializer`, `SerializedSnapshotArtifact`)
  - Frozen M4.2b Baseline: `8f509f94a39be9ece4060003ee498dc747efb813` (`SnapshotPreparationQueue`, `SnapshotChunkProducer`)
  - Frozen M4.2c Baseline: `79919a0f8cb0bb81cb80cc010fe02918bef79787` (`SnapshotOutboundPipeline`)

---

## 2. Exact Frozen Source Implementation Inspection & Verification

The following exact frozen source implementations and line ranges were inspected read-only to establish ground truth:

1. `packages/sync-engine/src/recovery/chunk-assembler.ts`:
   - **Lines 45–50:** `ChunkEnvelopeContext` interface (`sessionId`, `senderPeerId`, `membershipEpoch`, `connectionGeneration?`).
   - **Lines 184–230:** Step 1 & 2 validation: Disposed check, active transfer parameters existence check, and envelope context verification (`sessionId`, `membershipEpoch`, `senderPeerId`, `connectionGeneration`).
   - **Lines 231–290:** Step 3 validation: Payload metadata validation against active transfer parameters (`transferId`, `sessionId`, `snapshotId`, `supplierPeerId`, `membershipEpoch`, `totalChunks`, `totalSizeBytes`, `chunkIndex` bounds $[0, \text{totalChunks})$, and `chunkSizeBytes > this.maxChunkBytes`).
   - **Lines 292–306:** Step 4 validation: Canonical partition expected size calculation and equality verification (`expectedChunkSize`).
   - **Lines 308–336:** Step 5 validation: Base64 decoding of `payload.dataBase64` into `rawBytes`, followed by exact byte length assertion (`rawBytes.byteLength === payload.chunkSizeBytes`).
   - **Lines 338–346:** Step 6 validation: SHA-256 calculation `computedHash = sha256(rawBytes).toLowerCase()` and comparison with claimed `payload.chunkSha256.toLowerCase()`. Returns `{ ok: false, reason: 'CHUNK_CORRUPTED' }` on mismatch.
   - **Lines 348–361:** Step 7 & 8 validation: Existing hash lookup `this.chunkHashes[payload.chunkIndex]`. If `existingHash !== null`:
     - If `existingHash === computedHash`: returns `{ ok: true, duplicate: true, completed: this.isComplete() }` (identical duplicate).
     - Else: returns `{ ok: false, reason: 'CHUNK_CORRUPTED' }` (conflicting duplicate).
   - **Lines 363–374:** Step 9 mutation: Executed **strictly after** all validation:
     `this.chunks[payload.chunkIndex] = rawBytes;`
     `this.chunkHashes[payload.chunkIndex] = computedHash;`
     `this.receivedChunksCount++;`
     `this.receivedBytesCount += rawBytes.byteLength;`
   - **Lines 389–601:** `prepareForFailover()`: Preserves `this.chunks` and `this.chunkHashes` while updating expected `supplierPeerId`, `membershipEpoch`, and `connectionGeneration`.
   - **Lines 689–716:** `finalizeAssembly()`: Allocates contiguous `new Uint8Array(totalSizeBytes)`, copies slices via `.set()`, and immediately executes `this.chunks = null; this.chunkHashes = null;` at line 714 before returning.

2. `packages/sync-engine/src/recovery/snapshot-serializer.ts`:
   - **Lines 364–377:** `copyByteRange(offset, length)` calls `this.#masterBytes!.slice(offset, offset + length)`. `cloneBytes()` calls `new Uint8Array(this.#masterBuffer!.slice(0))`, producing completely unaliased deep copies.
   - **Lines 417–464:** `dispose()` provides idempotent resource cleanup; `#finalizeDisposal()` nullifies `#masterBuffer` and `#masterBytes`.

3. `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`:
   - **Lines 61–86:** `OutboundTransportCapability` and `createOutboundTransportCapability`.
   - **Lines 92–98:** `AuthenticatedRecipientContext` interface.
   - **Lines 505–527:** `#authorizedCapability` validation and private storage in `#transfers` and `#validRecipientContexts`.

4. `packages/protocol/src/constants.ts`:
   - **Lines 78–168:** `MAX_SNAPSHOT_BYTES` ($4,194,304\text{ B}$), `MAX_CHUNK_BYTES` ($32,768\text{ B}$), `MIN_RAW_CHUNK_BYTES` ($256\text{ B}$), `MAX_CHUNKS_PER_SNAPSHOT` (128), `MAX_CONCURRENT_CHUNK_REQUESTS` (4), `CHUNK_REQUEST_TIMEOUT_MS` ($5,000\text{ ms}$), `MAX_CHUNK_RETRIES` (3), `MAX_TRANSFER_RETRIES` (10), `OVERALL_TRANSFER_TIMEOUT_MS` ($60,000\text{ ms}$).

---

## 3. R11-1 — Separation of First-Time Ingestion and Duplicate Verification

### 3.1 Resolving the Contradiction in R10
In R10 Section 3.2, the admission invariant stated that a chunk is admissible only if `AND the chunk has not already been terminally verified`, while Section 3.4 Table row 6 allowed `RECEIVED_VERIFIED` chunks to be admitted to the duplicate handler. This created an internal contradiction regarding whether verified chunks are admissible.

We resolve this by defining two distinct, explicitly governed admission paths:

### 3.2 Path A: First-Time Chunk Ingestion (Unverified Chunk Index)
**Precondition:** `chunkStates[chunkIndex] !== 'RECEIVED_VERIFIED'`.

Before submitting the payload to `ChunkAssembler.ingestChunk()`, the pipeline executes the following mandatory pre-ingestion authorization checks:
1. **Active Transfer Session:** `session.state === 'TRANSFERRING' && !session.isTerminal`.
2. **Session and Snapshot Identity:**
   - `payload.transferId === session.transferId`
   - `payload.sessionId === session.sessionId`
   - `payload.snapshotId === session.snapshotId`
   - `payload.totalChunks === session.totalChunks`
   - `payload.totalSizeBytes === session.totalSizeBytes`
3. **Membership Epoch Fencing:** `payload.membershipEpoch === session.membershipEpoch` and `envelopeContext.membershipEpoch === session.membershipEpoch`.
4. **Active Supplier Identity Fencing:** `payload.supplierPeerId === session.supplierPeerId` and `envelopeContext.senderPeerId === session.supplierPeerId`.
5. **Active Supplier Generation Fencing:** `envelopeContext.connectionGeneration === session.supplierGeneration`.
6. **Historical Acceptance Authority:** `#chunkEvidence.get(chunkIndex)` contains at least one record where `status !== 'UNACCEPTED'` and `hasActiveDataChannelAcceptance === true`, matching `session.supplierPeerId` and `session.supplierGeneration`.
7. **Chunk Index & Canonical Partition Metadata:** `chunkIndex >= 0 && chunkIndex < session.totalChunks`, `payload.chunkSizeBytes <= MAX_CHUNK_BYTES`, and `payload.chunkSizeBytes === expectedChunkSize(chunkIndex)`.

**Execution:** The frame is classified as an **Admitted First-Time Inbound Frame**. The pipeline submits the payload and envelope to `ChunkAssembler.ingestChunk(payload, envelopeContext)`.

### 3.3 Path B: Duplicate Verification (Already Verified Chunk Index)
**Precondition:** `chunkStates[chunkIndex] === 'RECEIVED_VERIFIED'`.

When an inbound chunk arrives for an index that is already verified:
1. **Fencing Against Active Supplier and Generation:**
   - The frame MUST match active `session.transferId`, `session.sessionId`, `session.membershipEpoch`, `session.supplierPeerId`, and `session.supplierGeneration`.
   - **Stale Response Protection:** If a packet arrives from an old supplier after failover ($S_{\text{old}} \ne S_{\text{active}}$) or from an old connection generation ($G < G_{\text{active}}$), it is **strictly rejected at the transport boundary and dropped**. It is NEVER submitted to the assembler.
2. **Eligibility of Previous Attempt Responses:**
   - If the duplicate frame originated from the current active supplier under the active generation (e.g. response to Attempt A arrived after Attempt B was fulfilled), it is admitted to Path B.
3. **Assembler Duplicate Path Invocation:**
   - The frame is submitted to `ChunkAssembler.ingestChunk(payload, envelopeContext)`.
   - `ingestChunk()` decodes the Base64 bytes, computes the actual SHA-256 digest, verifies it against claimed `payload.chunkSha256`, looks up `this.chunkHashes[chunkIndex]`, and performs the duplicate comparison:
     - **Identical Duplicate:** `existingHash === computedHash` $\implies$ returns `{ ok: true, duplicate: true }`. The pipeline logs a diagnostic trace and drops the frame idempotently with zero state or retry mutation.
     - **Conflicting Duplicate:** `existingHash !== computedHash` $\implies$ returns `{ ok: false, reason: 'CHUNK_CORRUPTED' }`. The pipeline immediately aborts the transfer session with `CHUNK_CORRUPTED`, cancels all pending operations, and emits `SNAPSHOT_ABORT`!

### 3.4 Summary of Standardized Terminology
- **Admitted First-Time Inbound Frame:** A wire frame for an unverified index that satisfies all Path A pre-ingestion checks.
- **Admitted Assembler Invocation:** A call to `ChunkAssembler.ingestChunk()` authorized by either Path A or Path B.
- **Identical Duplicate Response:** An admitted Path B frame whose computed hash matches the existing stored hash in `ChunkAssembler`.
- **Conflicting Duplicate Response:** An admitted Path B frame whose computed hash conflicts with the existing stored hash, triggering an immediate terminal abort.
- **Stale / Unconfirmed Inbound Frame:** A frame arriving from an obsolete supplier, stale generation, or for a request that never reached DataChannel acceptance. Dropped at the pipeline boundary without assembler invocation.

---

## 4. R11-2 — Definitive Result-to-State Transition Table for `ChunkAssembler.ingestChunk()`

The following table defines the exact state mutations, evidence transitions, retry impacts, and lifecycle outcomes for every possible return from `ChunkAssembler.ingestChunk()`:

| # | Assembler Return / Outcome | Frame Admitted? | Assembler Invoked? | Chunk State | Evidence Records | Historical Evidence Retained? | Active Retry Cancelled? | Retry Budget Mutated? | Transfer Lifecycle |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | `{ ok: true, duplicate: false }` (First-Time Storage) | **Yes** (Path A) | **Yes** | $\to$ `RECEIVED_VERIFIED` | Records for $i \to$ `'FULFILLED'` | **Yes** (Retained until transfer ends) | **Yes** (Watchdog cleared, batch index removed) | **No** (Consumed retries stay consumed) | Continues $\to$ `TRANSFERRING` (or `REASSEMBLING` if last chunk) |
| **2** | `{ ok: true, duplicate: true }` (Identical Duplicate) | **Yes** (Path B) | **Yes** | Remains `RECEIVED_VERIFIED` | Remain `'FULFILLED'` | **Yes** (Retained) | N/A (No retry active) | **No** | Continues $\to$ `TRANSFERRING` (Dropped idempotently) |
| **3** | `{ ok: false, reason: 'CHUNK_CORRUPTED' }` (Conflicting Duplicate) | **Yes** (Path B) | **Yes** | Remains `RECEIVED_VERIFIED` before abort | Cleared on abort | **No** (Purged on abort) | **Yes** (All cancelled) | **No** | **ABORTS IMMEDIATELY** with `CHUNK_CORRUPTED` |
| **4** | `{ ok: false, reason: 'CHUNK_CORRUPTED' }` (Claimed Digest Mismatch) | **Yes** (Path A/B) | **Yes** (Fails Step 6) | Unverified: remains `REQUESTED`; Verified: `RECEIVED_VERIFIED` | **NOT FULFILLED** (remains `PENDING` or `TIMED_OUT`) | **Yes** (Valid acceptance proof retained) | **NO** (Retry must continue) | **No** (Corrupted frame does not consume budget) | Continues $\to$ `TRANSFERRING` (Corrupted frame dropped) |
| **5** | `{ ok: false, reason: 'CHUNK_CORRUPTED' }` (Base64 / Length Error) | **Yes** (Path A/B) | **Yes** (Fails Step 5) | Unchanged | **NOT FULFILLED** | **Yes** (Retained) | **NO** | **No** | Continues $\to$ `TRANSFERRING` (Corrupted frame dropped) |
| **6** | `{ ok: false, reason: 'CHUNK_CORRUPTED' }` (Partition Size Error) | **No** (Caught at pipeline pre-check) | **No** | Unchanged | **NOT FULFILLED** | **Yes** (Retained) | **NO** | **No** | Continues $\to$ `TRANSFERRING` (Malformed frame dropped) |
| **7** | `{ ok: false, reason: 'IDENTITY_MISMATCH' \| 'EPOCH_MISMATCH' }` | **No** (Caught at pipeline fencing) | **No** | Unchanged | Unchanged | **Yes** (Retained) | **NO** | **No** | Continues $\to$ `TRANSFERRING` (Stale frame dropped) |
| **8** | `{ ok: false, reason: 'LOCAL_ABORT' }` (Disposed Assembler) | **No** (Caught at pipeline disposal) | **No** | Terminal | Purged | **No** | **Yes** (All cancelled) | N/A | Terminal (Pipeline disposed) |
| **9** | `{ ok: false, reason: 'TRANSPORT_LIMIT_EXCEEDED' }` | **No** (Caught at pipeline pre-check) | **No** | Unchanged | Unchanged | **Yes** (Retained) | **NO** | **No** | Continues $\to$ `TRANSFERRING` (Oversized frame dropped) |

> [!CRITICAL]
> **Definitive Verification Invariant:**  
> ONLY outcome 1 (`{ ok: true, duplicate: false }`) or outcome 2 (`{ ok: true, duplicate: true }`) establishes verified chunk state. Any corruption, decoding error, or validation failure strictly preserves unfulfilled historical evidence, does NOT cancel pending retries, does NOT consume retry budget, and leaves the transfer awaiting valid frames.

---

## 5. R11-3 — Formal Proof of Historical Evidence Bounds & Safe Retirement

### 5.1 Protocol Budget Constants
- `MAX_CHUNKS_PER_SNAPSHOT = 128` (logical chunk count $N \le 128$).
- `MAX_CHUNK_RETRIES = 3` (maximum retries per individual chunk).
- `MAX_TRANSFER_RETRIES = 10` (maximum cumulative remote retries across entire transfer).
- `MAX_CONCURRENT_CHUNK_REQUESTS = 4` (sliding window size $W = 4$).

### 5.2 Event Taxonomy & Retry Budget Consumption
We explicitly define which events consume retry budgets:
1. **Local Handoff Failures / Watchdog Timeouts:**
   - Occur at the local adapter interface (send threw or watchdog expired before DataChannel acceptance).
   - **Budget Impact:** Consumes **0 remote chunk retries** (`chunkRetries[i]` unmutated) and **0 transfer retries** (`cumulativeTransferRetries` unmutated).
   - **Evidence Impact:** Creates **0 accepted attempt records** because `RTCDataChannel.send()` never succeeded.
2. **Remote Response Timeouts (5,000 ms timer expiry):**
   - Occurs after `DATACHANNEL_ACCEPTED`.
   - **Budget Impact:** Consumes **1 per-chunk retry** (`chunkRetries[i]++`) AND **1 cumulative transfer retry** (`cumulativeTransferRetries++`).
   - If `chunkRetries[i] > 3` OR `cumulativeTransferRetries > 10`, transfer enters terminal `ABORTED` with `failureReason = 'RETRY_BUDGET_EXHAUSTED'`.
3. **Supplier Failover:**
   - Active supplier disconnects. In-flight attempts to that supplier are aborted.
   - **Budget Impact:** `cumulativeTransferRetries` is a transfer-scoped global counter that **NEVER resets on failover**. Resetting it would allow unbounded retransmission across oscillating peers.

### 5.3 Mathematical Proof of the 138-Record Bound
An `AcceptedAttemptRecord` is created strictly when `handleHandoffSuccess()` is invoked.

**Theorem:** *The maximum number of active accepted attempt records in `#chunkEvidence` at any point during a transfer session cannot exceed 138.*

**Proof:**
1. Let $N$ be the total chunks in the snapshot ($1 \le N \le 128$).
2. An initial request batch can result in at most $N$ initial DataChannel acceptances across all chunks:
   $$\text{Initial Accepted Records} \le N \le 128$$
3. A subsequent DataChannel acceptance for chunk $i$ can occur only after a remote response timeout (`REMOTE_RESPONSE_TIMED_OUT`) has triggered a Tier 2 redispatch.
4. Each remote response timeout increments `cumulativeTransferRetries`.
5. The transfer aborts if `cumulativeTransferRetries > MAX_TRANSFER_RETRIES = 10`.
6. Therefore, the total number of retry requests that can ever reach `DATACHANNEL_ACCEPTED` across all chunks combined is bounded by:
   $$\text{Retry Accepted Records} \le 10$$
7. Summing initial and retry acceptances:
   $$\text{Total Accepted Records Ever Created} \le N + 10 \le 128 + 10 = 138$$
8. Since $138 \ll 512$, the 512-record bound is an absolute, mathematically proven upper bound for `#chunkEvidence`. Total memory is $< 50\text{ KiB}$. $\blacksquare$

### 5.4 Safe Evidence-Retirement Rules
1. **Retirement on Supplier Failover:** When active supplier $S_1$ disconnects and failover promotes candidate $S_2$:
   - All accepted records associated with $S_1$ are **immediately purged from `#chunkEvidence`**.
   - *Proof of Safe Eviction:* Because $S_1$ is disconnected, any subsequent packet from $S_1$ is rejected by transport supplier fencing (`envelope.senderPeerId !== session.supplierPeerId`). Therefore, evidence for $S_1$ cannot be required for any future legitimate arrival.
   - *Proof of Stale Traffic Block:* Purging $S_1$ evidence ensures that unverified chunks must be requested afresh from $S_2$ before $S_2$ chunks can be admitted.
2. **Retirement on Transfer Terminal State:** When transfer enters `COMPLETED` or `ABORTED`, the entire `#chunkEvidence` map is nullified and dereferenced.

---

## 6. R11-4 — Explicit Authorization Contracts for Supplier-Context Operations

The SyncPlay Protocol operates under an in-process cooperative trust model. Capabilities and contexts enforce in-process API custody to prevent accidental bypass by application code; they do not represent hardware security boundaries.

### 6.1 `registerSupplierContext()`
```typescript
public registerSupplierContext(
  supplierPeerId: string,
  connectionGeneration: number,
  sctpMaxMessageSize: number,
  capability: InboundTransportCapability
): AuthenticatedSupplierContext;
```
1. **Authorized Invoker:** Trusted transport adapter during DataChannel opening.
2. **Capability Required:** `InboundTransportCapability` (holds `INBOUND_CAPABILITY_BRAND: unique symbol` and `sessionId`).
3. **Capability Validation:** Asserts `capability === this.#authorizedCapability` via exact reference equality. Throws `SyncEngineError('UNAUTHORIZED')` on failure.
4. **Context Minting:** Creates an immutable, frozen `AuthenticatedSupplierContext` object:
   ```typescript
   const context: AuthenticatedSupplierContext = Object.freeze({
     supplierPeerId,
     sessionId: this.#session.sessionId,
     membershipEpoch: this.#session.membershipEpoch,
     connectionGeneration,
     sctpMaxMessageSize,
   });
   ```
5. **Registry Tracking:** Recorded in private `#validSupplierContexts: WeakSet<AuthenticatedSupplierContext>` (unforgeable membership) and `#activeSupplierContexts: Map<string, ActiveSupplierRecord>`.
6. **Idempotency:** Calling with identical `supplierPeerId` and `connectionGeneration` returns the existing context object. Calling with newer generation updates the active record. Calling with older generation throws `SyncEngineError('INVALID_STATE')`.

### 6.2 `revokeSupplierContext()`
```typescript
public revokeSupplierContext(
  supplierPeerId: string,
  connectionGeneration: number,
  capability: InboundTransportCapability
): void;
```
1. **Authorized Invoker:** Trusted transport adapter on channel close.
2. **Capability Validation:** Asserts `capability === this.#authorizedCapability`.
3. **Execution:** Deletes entry from `#activeSupplierContexts: Map`.
4. **Context Retention Invariant:** The revoked context **remains in `#validSupplierContexts: WeakSet`** so that subsequent `handleSupplierDisconnected()` calls presenting this context can still be verified as genuine.
5. **Idempotency:** Repeated calls are safe no-ops.

### 6.3 `handleChunk()`
```typescript
public handleChunk(
  message: ProtocolMessage<SnapshotChunkPayload>,
  context: AuthenticatedSupplierContext
): Promise<{ accepted: boolean; duplicate: boolean; completed: boolean }>;
```
1. **Authorized Invoker:** Trusted transport adapter on message receipt.
2. **Context Verification:** Asserts `this.#validSupplierContexts.has(context) === true`.
3. **Active Fencing:** Asserts:
   - `context.sessionId === this.#session.sessionId`
   - `context.membershipEpoch === this.#session.membershipEpoch`
   - `context.supplierPeerId === this.#session.supplierPeerId`
   - `context.connectionGeneration === this.#session.supplierGeneration`
   - Context is present in `#activeSupplierContexts` (not revoked).
4. **Stale Context Protection:** Any frame presenting a revoked or stale context is dropped at the transport boundary with zero assembler invocation.

### 6.4 `handleSupplierDisconnected()`
```typescript
public handleSupplierDisconnected(
  context: AuthenticatedSupplierContext,
  capability: InboundTransportCapability
): Promise<{ failoverSuccess: boolean }>;
```
1. **Authorized Invoker:** Trusted transport adapter upon transport disconnect.
2. **Capability Validation:** Asserts `capability === this.#authorizedCapability`.
3. **Context Verification:** Asserts `this.#validSupplierContexts.has(context) === true`.
4. **Session Fencing:** Asserts:
   - `context.supplierPeerId === this.#session.supplierPeerId`
   - `context.connectionGeneration === this.#session.supplierGeneration`
5. **Execution:** Cancels attempts, purges old supplier evidence, executes failover to best candidate, and promotes candidate to active supplier.
6. **Idempotency:** Duplicate disconnect calls find `context.supplierPeerId !== session.supplierPeerId` (due to promotion) and drop as no-ops.

---

## 7. R11-5 — Reconciled 4-Lifecycle State Machine Transition Table

The following table provides the complete, authoritative transition matrix reconciling all four lifecycles across 18 distinct triggers:

| Trigger | Attempt Transition | Chunk Transition | Evidence Transition | Batch Transition | Transfer Transition |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Request Created** | $\to$ `LOCAL_HANDOFF_PENDING` | `UNREQUESTED` $\to$ `REQUESTED` | Entry initialized (`hasActive=false`) | Index added to `unsettledChunkIndices` | `TRANSFERRING` |
| **2. Local Handoff Begins** | In `LOCAL_HANDOFF_PENDING`, watchdog armed (2s) | `REQUESTED` | Unchanged | In `unsettledChunkIndices` | `TRANSFERRING` |
| **3. DataChannel Acceptance** | $\to$ `AWAITING_REMOTE_RESPONSE`, watchdog cleared, 5s timer armed | `REQUESTED` | `AcceptedAttemptRecord` added (`PENDING_RESPONSE`), `hasActive=true` | Retained in `unsettledChunkIndices` | `TRANSFERRING` |
| **4. Sync Send Exception** | $\to$ `LOCAL_HANDOFF_FAILED`, watchdog cleared | `REQUESTED` $\to$ `UNREQUESTED` | Zero accepted records added | Removed from `unsettledChunkIndices` | `TRANSFERRING` |
| **5. Promise Rejection Pre-Accept** | $\to$ `LOCAL_HANDOFF_FAILED`, watchdog cleared | `REQUESTED` $\to$ `UNREQUESTED` | Zero accepted records added | Removed from `unsettledChunkIndices` | `TRANSFERRING` |
| **6. Promise Rejection Post-Accept** | **UNMUTATED** (`AWAITING_REMOTE_RESPONSE`) | `REQUESTED` | Unchanged (`PENDING_RESPONSE`) | Retained in `unsettledChunkIndices` | `TRANSFERRING` |
| **7. Local Watchdog Timeout** | $\to$ `LOCAL_HANDOFF_TIMED_OUT` | `REQUESTED` $\to$ `UNREQUESTED` | Zero accepted records added | Removed from `unsettledChunkIndices` | `TRANSFERRING` |
| **8. Remote Response Timeout** | $\to$ `REMOTE_RESPONSE_TIMED_OUT` | `REQUESTED` $\to$ `UNREQUESTED` | Record status $\to$ `'TIMED_OUT'`, `hasActive` remains `true` | Removed from `unsettledChunkIndices` | `TRANSFERRING` |
| **9. Retry Creation** | New attempt $\to$ `LOCAL_HANDOFF_PENDING` | `UNREQUESTED` $\to$ `REQUESTED` | Prior record `'TIMED_OUT'` retained; `hasActive` remains `true` | Added to new batch `unsettledChunkIndices` | `TRANSFERRING` |
| **10. Delayed Arrival During Retry** | Prior attempt fulfilled; pending retry cancelled | `REQUESTED` $\to$ `RECEIVED_VERIFIED` | Prior record $\to$ `'FULFILLED'`, pending retry cancelled | Removed from pending retry batch | `TRANSFERRING` |
| **11. Successful Ingestion** | Active attempt $\to$ `TERMINAL: RECEIVED` | `REQUESTED` $\to$ `RECEIVED_VERIFIED` | All accepted records $\to$ `'FULFILLED'` | Removed from `unsettledChunkIndices` | `TRANSFERRING` (or `REASSEMBLING`) |
| **12. Identical Duplicate** | Late attempt dropped as no-op | Remains `RECEIVED_VERIFIED` | Retained as `'FULFILLED'` | Unchanged | `TRANSFERRING` |
| **13. Conflicting Duplicate** | Active attempt cancelled | Aborted | Cleared on abort | All batches retired | `TRANSFERRING` $\to$ `ABORTED` |
| **14. Supplier Failover** | All in-flight attempts cancelled | Unverified $\to$ `UNREQUESTED`; verified retained | Old supplier records purged; `hasActive=false` | Old batches retired | Promotes candidate; continues |
| **15. Supplier Revocation** | In-flight attempts to revoked context cleared | Unverified $\to$ `UNREQUESTED` | Old supplier records purged | Old batches retired | Handled via failover |
| **16. Disconnect Notification** | Attempt timers cleared; attempts cancelled | Unverified $\to$ `UNREQUESTED` | Old supplier records purged | Old batches retired | Promotes candidate $\to$ `TRANSFERRING` (or aborts) |
| **17. Transfer Completion** | All attempts settled | All chunks `RECEIVED_VERIFIED` | Evidence map disposed | All batches retired | `TRANSFERRING` $\to$ `REASSEMBLING` $\to$ `VERIFYING` $\to$ `COMPLETED` |
| **18. Transfer Abort** | All attempts cancelled & timers cleared | Aborted | Evidence map disposed | All batches retired | `TRANSFERRING` $\to$ `ABORTED` |

---

## 8. R11-6 — Complete Promise and Partial-Batch Settlement Rules

### 8.1 Contract of `onEmitMessage()`
```typescript
onEmitMessage: (handoff: OutboundHandoffRequest) => Promise<void> | void;
```

### 8.2 Precedence Ordering of Settlement Events
1. **Synchronous DataChannel Acceptance:** If adapter executes `dataChannel.send()` and invokes `handleHandoffSuccess()` synchronously, the attempt transitions to `AWAITING_REMOTE_RESPONSE` immediately within the same microtask.
2. **Synchronous Exceptions:** If `onEmitMessage()` throws synchronously, the catch block immediately transitions chunk attempts to `LOCAL_HANDOFF_FAILURE` and clears watchdogs.
3. **Success Callbacks (`handleHandoffSuccess`):** Settle specific attempts identifying `attemptToken`.
4. **Failure Callbacks (`handleHandoffFailure`):** Settle specific attempts identifying `attemptToken`.
5. **Promise Resolution:** Informational only. Zero state mutation on accepted attempts.
6. **Promise Rejection:**
   - Any attempt still in `LOCAL_HANDOFF_PENDING` transitions to `LOCAL_HANDOFF_FAILURE`.
   - Any attempt already in `AWAITING_REMOTE_RESPONSE` is **STRICTLY UNMUTATED**. DataChannel acceptance cannot be reversed. Error forwarded to `onDiagnosticError`.
7. **Local Watchdog Timeout (2,000 ms):** If watchdog expires while attempt is `LOCAL_HANDOFF_PENDING`, attempt transitions to `LOCAL_WATCHDOG_TIMEOUT`. Any subsequent callback presenting that `attemptToken` is dropped as stale.

### 8.3 Multi-Chunk Batch Independent Settlement
A batch message may request 1 to 4 chunks (e.g. $[0, 1]$).
- Each chunk possesses an independent `attemptToken`: `${messageId}:${chunkIndex}:${attemptSeq}`.
- If chunk 0 succeeds and chunk 1 fails send:
  - Chunk 0: enters `AWAITING_REMOTE_RESPONSE`.
  - Chunk 1: enters `LOCAL_HANDOFF_FAILURE`.
  - Batch: removes chunk 1 from `unsettledChunkIndices`, but retains chunk 0.
  - The batch retires only when `unsettledChunkIndices.size === 0`. One chunk's settlement never settles or mutates another chunk in the batch.

---

## 9. R11-7 — Corrected Memory Accounting & Simultaneous Coexistence Timeline

### 9.1 De-duplicating Decoded Base64 and Assembler Slice
Inspection of `packages/sync-engine/src/recovery/chunk-assembler.ts` lines 310–364 confirms:
```typescript
const buf = maybeBuffer.from(payload.dataBase64, 'base64');
rawBytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
// ...
this.chunks[payload.chunkIndex] = rawBytes;
```
`rawBytes` is allocated once during Base64 decoding and its reference is directly assigned into `this.chunks[chunkIndex]`. **It is the exact same allocation.** R10 erroneously listed them as two separate $4.00\text{ MiB}$ allocations. In R11, we properly account for them as a single retained allocation accumulating up to $4.00\text{ MiB}$.

### 9.2 Inbound Transport Queue Accounting
The pipeline cannot constrain operating-system socket buffers or WebRTC internal SCTP receive queues. We explicitly classify the inbound WebRTC DataChannel message queue as **Unmeasured External Transport Overhead**.

### 9.3 Simultaneous Allocation Timeline Across Lifecycle

```
Phase 1: Chunk Transfer (Steady State)
├── Accumulating Chunk Slices (ChunkAssembler.chunks): 0 -> 4.00 MiB (Retained)
├── Historical Evidence & Active Attempts: < 60 KiB (Retained)
└── Inbound Base64 JSON Message: ~43.7 KiB (Transient per chunk)
TOTAL STEADY STATE: <= 4.06 MiB

Phase 2: Finalize Assembly (finalizeAssembly())
├── Accumulating Chunk Slices (before line 714): 4.00 MiB
├── Contiguous Assembled Buffer (allocated line 690): 4.00 MiB
└── Line 714 executes: this.chunks = null (Slices freed immediately!)
TRANSIENT PEAK DURING COPY: ~8.00 MiB

Phase 3: 10-Step Canonical Verification (Steps 2-8)
├── Contiguous Assembled Buffer (assembledBuffer): 4.00 MiB (Retained)
├── Decoded UTF-8 JSON String: ~4.00 - 8.00 MiB (V8 Heap, transient)
├── Parsed JSON Object Graph: ~150 - 300 KiB (V8 Heap, retained to M5)
├── Re-serialized Canonical JSON String: ~4.00 - 8.00 MiB (Transient, freed line 580)
├── Canonical Artifact Master Buffer: 4.00 MiB (Freed in finally block)
└── cloneBytes() Comparison Copy: 4.00 MiB (Dereferenced after loop)
ANALYTICAL LIVE HEAP ESTIMATE: ~20.25 - 28.25 MiB (Not a hard process ceiling)

Phase 4: Handoff to Milestone M5
├── SnapshotTransferResult.rawBytes: 4.00 MiB (Unaliased deep copy)
└── SnapshotTransferResult.snapshot: ~300 KiB (Deeply frozen object graph)
TOTAL QUIESCENT APPLICATION RETENTION: <= 4.30 MiB
```

---

## 10. R11-8 — Expanded Adversarial Verification Matrix (31 Core Cases)

| # | Test Scenario | Admission Decision | Assembler Invocation | Attempt State | Chunk State | Evidence Mutation | Batch State | Transfer State | Retry Budget Impact |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | First request never reaches DataChannel acceptance; unsolicited chunk arrives | **REJECTED & DROPPED** | **NO** | Remains `LOCAL_HANDOFF_PENDING` | `REQUESTED` | Zero mutation | Retained in `unsettled` | `TRANSFERRING` | Zero change |
| **2** | Delayed Attempt A response arrives during pending Attempt B retry | **ADMITTED** (Path A) | **YES** | Attempt B cancelled & cleared | `RECEIVED_VERIFIED` | Attempt A $\to$ `'FULFILLED'` | Attempt B batch removes index | `TRANSFERRING` | Zero change |
| **3** | Delayed response arrives after local retry failure | **ADMITTED** (Path A) | **YES** | Attempt B was `LOCAL_HANDOFF_FAILURE` | `RECEIVED_VERIFIED` | Attempt A $\to$ `'FULFILLED'` | Attempt B batch already removed | `TRANSFERRING` | Zero change |
| **4** | Duplicate response after successful first-time ingestion | **ADMITTED** (Path B) | **YES** (Dup Check) | Settled | Remains `RECEIVED_VERIFIED` | Retained as `'FULFILLED'` | Already removed | `TRANSFERRING` | Zero change (Idempotent) |
| **5** | Duplicate response during a pending retry | **ADMITTED** (Path B) | **YES** (Dup Check) | Retry attempt cancelled | Remains `RECEIVED_VERIFIED` | Retained as `'FULFILLED'` | Removed from retry batch | `TRANSFERRING` | Zero change |
| **6** | Attempt A times out; response arrives before retry | **ADMITTED** (Path A) | **YES** | Was `REMOTE_RESPONSE_TIMED_OUT` | `RECEIVED_VERIFIED` | Attempt A $\to$ `'FULFILLED'` | Already settled | `TRANSFERRING` | Zero change |
| **7** | Attempt A times out; response arrives after retry | **ADMITTED** (Path A) | **YES** | Retry attempt cancelled | `RECEIVED_VERIFIED` | Attempt A $\to$ `'FULFILLED'` | Removed from retry batch | `TRANSFERRING` | Zero change |
| **8** | Old supplier responds after failover | **REJECTED & DROPPED** | **NO** (Supplier mismatch) | Old attempts cleared | Unchanged | Zero mutation | Old batches retired | `TRANSFERRING` ($S_{\text{new}}$) | Zero change |
| **9** | Old supplier responds after generation revocation | **REJECTED & DROPPED** | **NO** (Generation mismatch) | Unchanged | Unchanged | Zero mutation | Unchanged | `TRANSFERRING` | Zero change |
| **10** | Stale transfer chunk with reused chunkIndex | **REJECTED & DROPPED** | **NO** (transferId mismatch) | Unchanged | Unchanged | Zero mutation | Unchanged | `TRANSFERRING` (active) | Zero change |
| **11** | Promise rejects after DataChannel acceptance | N/A (Outbound) | **NO** | **UNMUTATED** (`AWAITING_REMOTE_RESPONSE`) | `REQUESTED` | Retained (`PENDING_RESPONSE`) | Retained | `TRANSFERRING` | Zero change |
| **12** | Failure callback after success callback | N/A (Outbound) | **NO** | **UNMUTATED** (`AWAITING_REMOTE_RESPONSE`) | `REQUESTED` | Zero mutation | Retained | `TRANSFERRING` | Zero change |
| **13** | Watchdog and success callback race | N/A (Outbound) | **NO** | Success wins $\to$ `AWAITING`; Watchdog wins $\to$ `LOCAL_WATCHDOG_TIMEOUT` | Success: `REQUESTED`; Watchdog: `UNREQUESTED` | Success: recorded; Watchdog: unrecorded | Success: retained; Watchdog: removed | `TRANSFERRING` | 0 remote retries consumed |
| **14** | Duplicate disconnect callback after supplier promotion | N/A | **NO** | Unchanged | Unchanged | Zero mutation | Unchanged | `TRANSFERRING` (promoted) | Zero change (Idempotent) |
| **15** | Revocation before disconnect notification | N/A | **NO** | Cleared for old supplier | Unverified $\to$ `UNREQUESTED` | Old evidence purged | Old batches retired | Promotes candidate or aborts | Zero change |
| **16** | Disconnect notification before revocation | N/A | **NO** | Cleared during disconnect | As in Case 15 | Old evidence purged | Old batches retired | Promotes candidate or aborts | Zero change |
| **17** | Incorrect claimed digest for valid existing bytes | **ADMITTED** (Path B) | **YES** (Fails Step 6) | Dropped as corrupted | Remains `RECEIVED_VERIFIED` | Zero mutation | Unchanged | `TRANSFERRING` | Zero change |
| **18** | Identical valid duplicate | **ADMITTED** (Path B) | **YES** (Step 8 matches) | Handled idempotently | Remains `RECEIVED_VERIFIED` | Retains `'FULFILLED'` | Unchanged | `TRANSFERRING` | Zero change |
| **19** | Conflicting valid chunk for stored index | **ADMITTED** (Path B) | **YES** (Step 8 differs) | Terminal abort | `ABORTED` | Cleared on abort | All retired | `TRANSFERRING` $\to$ `ABORTED` | Transfer terminated |
| **20** | Corrupted response after valid historical acceptance | **ADMITTED** (Path A) | **YES** (Fails Step 6) | Attempt remains `AWAITING` | `REQUESTED` | Proof retained; not fulfilled | Retained | `TRANSFERRING` | Zero change (Awaits timeout) |
| **21** | Invalid digest on otherwise identical duplicate | **ADMITTED** (Path B) | **YES** (Fails Step 6) | Dropped as corrupted | Remains `RECEIVED_VERIFIED` | Retained `'FULFILLED'` | Unchanged | `TRANSFERRING` | Zero change |
| **22** | Historical evidence eviction at max capacity | **N/A** (Math Proven) | **N/A** | Capacity bounded $\le 138$ | Fully accommodates | Total records $\le 138 \ll 512$ | Bounded | `TRANSFERRING` | Eviction unnecessary |
| **23** | Retry budget exhausted across supplier failover | N/A | **NO** | Transfer terminates | `ABORTED` | Cleared on abort | All retired | `TRANSFERRING` $\to$ `ABORTED` | Terminated on 11th timeout |
| **24** | Local redispatch without DataChannel acceptance | N/A | **NO** | Tier 1 redispatch $\to$ `LOCAL_HANDOFF_PENDING` | `REQUESTED` | Zero accepted records added | Added to new batch | `TRANSFERRING` | 0 remote retries consumed |
| **25** | Promise rejection for mixed batch after partial acceptance | N/A | **NO** | Accepted chunk: `AWAITING`; Failed chunk: `LOCAL_HANDOFF_FAILURE` | Accepted: `REQUESTED`; Failed: `UNREQUESTED` | Accepted: recorded; Failed: unrecorded | Failed removed; Accepted retained | `TRANSFERRING` | Zero change |
| **26** | Attempt-token mismatch in callback | N/A | **NO** | Dropped as stale/unrecognized | Unchanged | Zero mutation | Unchanged | `TRANSFERRING` | Zero change |
| **27** | Stale callback after transfer cancellation | N/A | **NO** | Dropped as terminal no-op | `ABORTED` | Zero mutation | All retired | `ABORTED` | Zero change |
| **28** | Context registration without capability | **THROWS UNAUTHORIZED** | **NO** | Registration rejected | N/A | Zero mutation | N/A | Unchanged | Zero change |
| **29** | Revoked context attempting chunk admission | **REJECTED & DROPPED** | **NO** (Active check fails) | Dropped at transport boundary | Unchanged | Zero mutation | Unchanged | `TRANSFERRING` | Zero change |
| **30** | Inbound message queue exceeding 4 messages | Evaluated sequentially | Evaluated per chunk | Handled per chunk | Handled per chunk | Strictly bounded $\le 138$ | Window paced | `TRANSFERRING` | Unmeasured transport depth |
| **31** | Chunk verification failure while sibling in batch active | **ADMITTED** (Path A) | **YES** (Fails) | Fails independently | Failed: `REQUESTED`; Sibling: `AWAITING` | Sibling unaffected | Sibling retained | `TRANSFERRING` | Sibling continues independently |

---

## 11. R11-9 — Master Specification Synchronization & Obsolete Rule Pruning

The master specification `docs/PHASE7_5_M4_2D_ARCHITECTURE_AND_INTERFACE_SPECIFICATION.md` has been updated to **Version 1.11.0**:
1. **Explicit Ingestion vs Duplicate Paths (§3):** Separated Path A (First-time ingestion) and Path B (Duplicate verification).
2. **Definitive Assembler Transition Matrix (§4):** Fully codified the result-to-state table for `ChunkAssembler.ingestChunk()`.
3. **Evidence Bound Proof (§5):** Established the mathematical proof bounding active accepted records to $\le 138$ (cap 512).
4. **Authorized Context Contracts (§6):** Codified exact signatures and capability validation for registration, revocation, chunk handling, and disconnects.
5. **Reconciled Lifecycle Transitions (§7):** Corrected chunk state from `UNREQUESTED` to `REQUESTED` during `LOCAL_HANDOFF_PENDING`.
6. **Corrected Memory Accounting (§9):** De-duplicated decoded Base64 and assembler slice allocations; classified transport queue depth as unmeasured overhead.
7. **Expanded Adversarial Matrix (§10):** Codified all 31 adversarial race conditions.
8. **Preserved Frozen Baselines:** Maintained all protocol constants and schemas byte-exact.

---

## 12. Frozen Baseline Diff Verification (0 Diffs)

```bash
git diff --exit-code packages/
# Returns 0 (0 bytes diff)

git diff --exit-code 79919a0f8cb0bb81cb80cc010fe02918bef79787 packages/
# Returns 0 (0 bytes diff against M4.2c-R6 closure)
```

Zero modifications to runtime source code, test files, package configurations, or wire protocol schemas.

---

## 13. Final Decision Gate

**Verdict:** **`READY FOR FINAL INDEPENDENT REVIEW — IMPLEMENTATION STILL UNAUTHORIZED`**

> [!CRITICAL]
> **M4.2d implementation remains strictly unauthorized pending independent review and explicit user approval.**
