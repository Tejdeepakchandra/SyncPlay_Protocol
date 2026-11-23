# Phase 7.5 M4.2d-R9 — Transport Acceptance, Inbound Admission and Callback Identity Correction Report

**Document Version:** 1.0.0  
**Status:** COMPLETE — READY FOR INDEPENDENT REVIEW — IMPLEMENTATION STILL UNAUTHORIZED  
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

### 1.2 Exact Frozen Source Files and Line Ranges Inspected (Read-Only)
1. `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`:
   - Lines 88–98: `AuthenticatedRecipientContext` definition (`transferId`, `recipientPeerId`, `sessionId`, `membershipEpoch`, `connectionGeneration`).
   - Lines 103–109: `InboundRecipientContextParams`.
   - Lines 114–134: `OutboundOfferContext` and `OutboundChunkContext`.
   - Lines 506, 537–595: `#validRecipientContexts` WeakSet registration and capability assertion.
   - Lines 787–850: `handleChunkRequest()` active attempt coalescing and messageId allocation.
   - Lines 1024–1080: `handleHandoffSuccess(transferId, chunkIndex, messageId, attemptToken, context)`.
   - Lines 1102–1165: `handleHandoffFailure(transferId, chunkIndex, messageId, attemptToken, error, context)`.
2. `packages/sync-engine/src/recovery/chunk-assembler.ts`:
   - Lines 45–50: `ChunkEnvelopeContext` (`sessionId`, `senderPeerId`, `membershipEpoch`, `connectionGeneration?`).
   - Lines 184–374: `ChunkAssembler.ingestChunk()` fail-fast validation, Base64 decoding, SHA-256 verification, duplicate hash comparison against private `this.chunkHashes[chunkIndex]`, and atomic storage mutation at line 364.
   - Lines 389–601: `prepareForFailover()` retaining `this.chunks` and `this.chunkHashes` across supplier change.
   - Lines 667–742: `finalizeAssembly()` immediate slice nullification (line 714: `this.chunks = null; this.chunkHashes = null;`).
3. `packages/sync-engine/src/recovery/snapshot-serializer.ts`:
   - Lines 315–322: `#assertReadable()` asserting `!this.#isDisposed && this.#masterBytes !== null && this.#masterBuffer !== null`.
   - Lines 364–377: `copyByteRange()` and `cloneBytes()` returning unaliased `new Uint8Array(this.#masterBuffer!.slice(0))`.
   - Lines 417–464: `dispose()` idempotency, `#finalizeDisposal()`, and public getters `isDisposed`, `isDisposalRequested`, `activeLeaseCount`.
4. `packages/protocol/src/constants.ts`:
   - Lines 78–168: `MAX_SNAPSHOT_BYTES` (4,194,304 B), `MAX_CHUNK_BYTES` (32,768 B), `MIN_RAW_CHUNK_BYTES` (256 B), `MAX_CHUNKS_PER_SNAPSHOT` (128), `MAX_CONCURRENT_CHUNK_REQUESTS` (4), `CHUNK_REQUEST_TIMEOUT_MS` (5,000 ms), `MAX_CHUNK_RETRIES` (3), `MAX_TRANSFER_RETRIES` (10), `OVERALL_TRANSFER_TIMEOUT_MS` (60,000 ms).
5. `packages/protocol/src/payloads/snapshot-recovery.ts`:
   - Lines 107–127: `SnapshotRequestPayloadSchema` (`requestedChunkWindow` max 4, default 4; `missingChunkIndices` max 128).
   - Lines 280–296: `SnapshotChunkRequestPayloadSchema` (`missingChunkIndices` min 1, max 4; rejects `requestedChunkWindow` and `clientMaxChunkBytes`).
6. `docs/PHASE7_5_INTERFACE_SPECIFICATION.md`:
   - Lines 324–365: `ISnapshotInstaller.validateCandidate` and `installSnapshotAtomic`.

---

## 2. R9-1 — Precise Distinction: DataChannel Acceptance vs Backpressure vs Delivery

### 2.1 Separation of Confused Concepts
In earlier drafts, Stage 4 was described as `DATACHANNEL_ACCEPTED`, but the text conflated three distinct WebRTC transport mechanisms. We establish the following definitive separation:

1. **Local Adapter Submission:**
   - **Mechanism:** The inbound pipeline invokes `onEmitMessage(handoffRequest)`.
   - **Meaning:** Custody of the outbound request frame is handed to the in-process adapter shim. It indicates internal invocation only. Zero network or transport progress is established.
2. **DataChannel Acceptance:**
   - **Mechanism:** The adapter invokes `RTCDataChannel.send(serializedMessage)` and that call **returns successfully without throwing**.
   - **Meaning:** The WebRTC implementation has accepted the serialized bytes into the underlying SCTP transmission queue. The message has entered the local transport stack.
   - **Crucial Boundary:** This is **NOT** remote delivery. It provides zero proof that the remote peer received, read, or validated the request.
3. **Backpressure Availability:**
   - **Mechanism:** `RTCDataChannel.bufferedAmount` inspection and `bufferedAmountLow` event listener.
   - **Meaning:** Signals whether the underlying queue has drained below the configured low-water threshold, indicating whether the adapter can safely attempt *another* send.
   - **Crucial Boundary:** `bufferedAmountLow` does **NOT** identify any specific message, does **NOT** prove that a previous message was delivered, and **MUST NEVER** be used as evidence that a particular request was accepted.

### 2.2 Point of Notification & Synchronous Sequencing Requirement
To prevent an inbound response from racing ahead of local acceptance tracking:
- The adapter **MUST notify the pipeline of DataChannel acceptance synchronously within the same execution turn in which `RTCDataChannel.send()` returns successfully**.
- **Architectural Rationale:** JavaScript runtimes (browsers and Node.js) operate on a single-threaded event loop. If the adapter executes:
  ```typescript
  try {
    dataChannel.send(serializedMessage);
    pipeline.handleHandoffSuccess(transferId, chunkIndex, messageId, attemptToken, context);
  } catch (err) {
    pipeline.handleHandoffFailure(transferId, chunkIndex, messageId, attemptToken, err, context);
  }
  ```
  The pipeline transitions the attempt to `AWAITING_REMOTE_RESPONSE` and records `DATACHANNEL_ACCEPTED` in `#historicalDispatches` **synchronously before yielding to the event loop**.
- **Impossibility of Inbound Preemption:** Because I/O callbacks (including incoming WebRTC DataChannel packet arrival events) can only execute when the event loop is free, an incoming remote response **CANNOT preempt** the synchronous execution between `dataChannel.send()` and `pipeline.handleHandoffSuccess()`.

> [!NOTE]
> **Open External Dependency (M4.3 Transport Contract):** Enforcing that `handleHandoffSuccess` is called synchronously immediately upon `send()` return without asynchronous tick deferral is an explicit transport adapter invariant tracked under **DEP-M4.3-1**.

---

## 3. R9-2 — Closing the Receipt-Before-Callback Admission Gap

### 3.1 Two Discrete Scenarios Defined
R8 permitted an inbound chunk to be accepted while an attempt was in `LOCAL_HANDOFF_PENDING`. This was an architectural flaw: an active attempt in `LOCAL_HANDOFF_PENDING` represents only that a request was allocated or submitted locally; it does not prove that `RTCDataChannel.send()` succeeded. We distinguish two cases:

#### Case A: Legitimate Early Response
- **Precondition:** `RTCDataChannel.send()` returned successfully, and the request traveled across the network. The remote supplier processed it and transmitted chunk $i$.
- **Temporal Analysis:** Under the synchronous notification contract established in §2.2, `handleHandoffSuccess()` executes synchronously within the same microtask as `send()`. The pipeline transitions the attempt to `AWAITING_REMOTE_RESPONSE` **before** the event loop can dispatch the incoming chunk packet.
- **Deduction:** Under the strict adapter contract, **Case A cannot observe `LOCAL_HANDOFF_PENDING` at the moment of chunk arrival**. By the time the chunk arrives, the attempt has already transitioned to `AWAITING_REMOTE_RESPONSE`.

#### Case B: Unconfirmed / Failed Request
- **Precondition:** An attempt is in `LOCAL_HANDOFF_PENDING` (e.g. queued in the adapter, or `send()` has not yet returned, or `send()` threw an exception).
- **Arrival of Chunk $i$:** A remote chunk arrives claiming to correspond to attempt $i$.
- **Problem:** The pipeline possesses **zero trusted local evidence** that a request was accepted by the DataChannel. The chunk could be an unsolicited frame from a buggy supplier or an attack intended to bypass sliding-window bounds.
- **Rule:** The physical arrival of a chunk **MUST NEVER be treated as proof that the request was authorized**. A supplier's behavior is untrusted.

### 3.2 Authoritative Admission Invariant
**Invariant:** *An inbound chunk is admissible for assembler ingestion if and only if its index has already reached Stage 4 (`DATACHANNEL_ACCEPTED`) through a verified local `handleHandoffSuccess()` notification.*

If an inbound chunk arrives while the attempt is still in `LOCAL_HANDOFF_PENDING`:
1. It is classified as an **Unconfirmed Inbound Frame**.
2. The pipeline **STRICTLY REJECTS AND DROPS** the frame.
3. `ChunkAssembler.ingestChunk()` is **NEVER** called.
4. Attempt state, batch ownership, and window credit remain completely unmutated.

---

## 4. R9-3 — Callback Identity and Context Direction

### 4.1 Directional Symmetry with Frozen M4.2c Outbound Pipeline
In the SyncPlay Protocol architecture, snapshot transfer is asymmetric between Supplier and Receiver:

```
Sender / Supplier Node (M4.2c)                 Receiver / Requester Node (M4.2d)
------------------------------                 ---------------------------------
Local Node: Supplier                           Local Node: Requester (Receiver)
Remote Peer: Recipient                         Remote Peer: Supplier
Gating Context: AuthenticatedRecipientContext   Gating Context: AuthenticatedSupplierContext
```

- In M4.2c (`SnapshotOutboundPipeline`), the local node is the Supplier. It interacts with a remote Recipient. All gating contexts authenticate the remote recipient (`AuthenticatedRecipientContext`, lines 92–98).
- In M4.2d (`SnapshotInboundPipeline`), the local node is the Requester. It interacts with a remote Supplier.
- Calling the context `AuthenticatedSenderContext` (as in R8) created directional confusion: for inbound chunks, the remote peer is the sender; but for outbound requests, the local receiver is the sender.

### 4.2 Authoritative Definition: `AuthenticatedSupplierContext`
We establish a dedicated, genuinely typed interface that explicitly designates the remote peer as the **Supplier**:

```typescript
export interface AuthenticatedSupplierContext {
  readonly supplierPeerId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration: number;
  readonly sctpMaxMessageSize?: number;
}
```

### 4.3 Identity and Generation Fencing in Callbacks
When the transport adapter invokes `handleHandoffSuccess` or `handleHandoffFailure`:
```typescript
public handleHandoffSuccess(
  transferId: string,
  chunkIndex: number,
  messageId: string,
  attemptToken: string,
  context: AuthenticatedSupplierContext
): void;
```

The pipeline executes strict fencing:
1. **Capability Fencing:** The adapter must prove possession of the in-process `InboundTransportCapability`.
2. **Transfer Match:** `transferId === session.transferId`.
3. **Peer Match:** `context.supplierPeerId === session.supplierPeerId`.
4. **Generation Match:** `context.connectionGeneration === session.supplierGeneration`.
5. **Session & Epoch Match:** `context.sessionId === session.sessionId && context.membershipEpoch === session.membershipEpoch`.
6. **Context Registry Check:** `this.#validSupplierContexts.has(context)` and `this.#activeSupplierContexts.get(supplierPeerId).context === context`.

If any check fails, the callback is rejected and dropped as stale or unauthenticated.

---

## 5. R9-4 — Complete Disconnect-Generation Decision Table

The decision matrix covers all permutations of peer identity, connection generation, and lifecycle ordering against the active transfer session and candidate registry:

| Scenario | Peer Category | Generation Check | Registry State | Authoritative Action |
| :--- | :--- | :--- | :--- | :--- |
| **D-1** | Active Supplier | `gen === session.supplierGeneration` | Context active or revoked | **Valid Active Disconnect.** Clear attempt timers. Initiate failover to highest-priority candidate in `#candidateSuppliers`. If no candidate, abort transfer with `SUPPLIER_DISCONNECTED`. |
| **D-2** | Active Supplier | `gen < session.supplierGeneration` | Context active, revoked, or retired | **Stale Active Disconnect.** Generation is obsolete. Drop as strict no-op. Active transfer continues completely undisturbed. |
| **D-3** | Active Supplier | `gen > session.supplierGeneration` | Future generation | **Unregistered Generation.** Disconnect specifies a generation ahead of active session. Drop as unfenced no-op. |
| **D-4** | Candidate Peer | `gen === candidate.generation` | In `#candidateSuppliers` | **Valid Candidate Disconnect.** Remove candidate from `#candidateSuppliers`. Active supplier transfer continues with zero interruption. |
| **D-5** | Candidate Peer | `gen < candidate.generation` | In `#candidateSuppliers` | **Stale Candidate Disconnect.** Generation is obsolete. Drop as strict no-op. Candidate remains registered. |
| **D-6** | Candidate Peer | `gen > candidate.generation` | In `#candidateSuppliers` | **Unregistered Candidate Generation.** Drop as safe no-op. Candidate remains registered. |
| **D-7** | Removed / Unknown Peer | Any generation | Not in active session or candidate registry | **Extraneous Notification.** Peer is neither active supplier nor candidate. Drop as strict no-op. |
| **D-8** | Duplicate Notification | `gen === session.supplierGeneration` | Already processed | **Duplicate Disconnect.** First call already promoted a candidate or aborted the session. Second call finds `peerId !== session.supplierPeerId`. Drop as idempotent no-op. |
| **D-9** | Promoted Candidate Disconnect | `gen === session.supplierGeneration` | Promoted to active supplier | **Promoted Supplier Disconnect.** When candidate was promoted, `session.supplierPeerId` and `generation` were updated. Matches D-1: initiates failover to remaining candidates. |
| **D-10** | Revocation Before Disconnect | `gen === session.supplierGeneration` | Context already revoked from `#activeSupplierContexts` | **Immune to Ordering.** Evaluates immutable `session.supplierPeerId` and `session.supplierGeneration` (NOT `#activeSupplierContexts`). Matches D-1: failover executes identically. |
| **D-11** | Disconnect Before Revocation | `gen === session.supplierGeneration` | Context active in `#activeSupplierContexts` | **Standard Order.** Matches D-1: failover executes. Subsequent `revokeSenderContext` removes context from registry. |

---

## 6. R9-5 — Correct Memory-Bound Terminology and Lifecycle Classification

We strictly classify all memory paths into 5 distinct categories, eliminating unsubstantiated "proven maximum" claims:

```
+-----------------------------------------------------------------------------------+
| 1. PROVEN RETAINED APPLICATION BUFFERS (M4.2d Code-Guaranteed Bound)             |
|    - Contiguous Assembled Buffer in ChunkAssembler: Exactly <= 4,194,304 B (4.00 MiB) |
|    - Chunk slices freed on line 714 of chunk-assembler.ts before return.          |
|    - Proven Quiescent Bound: <= 4.00 MiB (4,194,304 Bytes)                        |
+-----------------------------------------------------------------------------------+
| 2. KNOWN TRANSIENT ALLOCATIONS (Deterministic Lifetime, Explicitly Released)      |
|    - ChunkAssembler chunk slices (accumulating during transfer): <= 4.00 MiB      |
|    - Per-chunk Base64 decoded Uint8Array: <= 32.00 KiB (released per chunk)       |
|    - Canonical SerializedSnapshotArtifact masterBuffer: <= 4.00 MiB               |
|      (Released immediately in finally block via canonicalArtifact.dispose())      |
|    - cloneBytes() temporary comparison buffer: <= 4.00 MiB                        |
|      (Dereferenced immediately after byte equality loop)                          |
+-----------------------------------------------------------------------------------+
| 3. ANALYTICAL TRANSIENT PEAK ESTIMATE (V8 Heap Model, NOT a Proven Ceiling)      |
|    - Assembled buffer (4.00 MiB) + UTF-8 string (~4.00-8.00 MiB) +                |
|      Parsed object graph (~0.25 MiB) + Re-serialized JSON string (~4.00-8.00 MiB) |
|      + Canonical masterBuffer (4.00 MiB) + cloneBytes copy (4.00 MiB)             |
|    - Estimated Live Heap Coexistence: ~20.25 - 28.25 MiB                          |
+-----------------------------------------------------------------------------------+
| 4. UNMEASURED RUNTIME & GC OVERHEAD (Engine-Dependent, Not Bounded by Code)       |
|    - V8 Scavenger/Mark-Sweep GC cycles, internal string representation shifts,   |
|      process-level RSS, native memory fragmentation.                              |
+-----------------------------------------------------------------------------------+
| 5. M5-OWNED ALLOCATIONS (Strictly Outside M4.2d Boundary)                        |
|    - Post-handoff retention or copying of rawBytes and snapshot by               |
|      ISnapshotInstaller.validateCandidate() / installSnapshotAtomic().           |
+-----------------------------------------------------------------------------------+
```

---

## 7. R9-6 — Reconciled Adversarial Test Matrix (10 Core Cases)

| # | Test Scenario | Impairment / Race Sequence | Exact State & Ownership Assertions |
| :--- | :--- | :--- | :--- |
| **1** | `send()` throws before acceptance | `RTCDataChannel.send()` throws immediately (e.g. channel closing or buffer full). | - Attempt: Transitioned to `LOCAL_HANDOFF_FAILURE`.<br/>- Watchdog: Cancelled.<br/>- Historical Record: Deleted / not confirmed.<br/>- Assembler: Zero mutation.<br/>- Terminal: Yes (attempt level). Retries permitted.<br/>- Batch: Unsettled index removed; retired if empty. |
| **2** | `send()` succeeds & acceptance processed | `send()` returns successfully; `handleHandoffSuccess()` fires synchronously. | - Attempt: Transitioned to `AWAITING_REMOTE_RESPONSE`.<br/>- Watchdog: Cancelled.<br/>- Remote Timer: Armed (5,000 ms).<br/>- Historical Record: Marked `DATACHANNEL_ACCEPTED`.<br/>- Assembler: Zero mutation.<br/>- Terminal: No (non-terminal transition).<br/>- Batch: Retains index in `unsettledChunkIndices`. |
| **3** | Inbound chunk arrives after `send()` success | Synchronous adapter notification established; chunk arrives over wire. | - Attempt: Transitioned to `RECEIVED`.<br/>- Remote Timer: Cancelled.<br/>- Historical Record: Confirmed `DATACHANNEL_ACCEPTED`.<br/>- Assembler: Chunk ingested and stored.<br/>- Terminal: Yes. Chunk state `RECEIVED_VERIFIED`.<br/>- Batch: Index removed from `unsettledChunkIndices`. |
| **4** | Inbound chunk arrives before `send()` attempted | Remote chunk arrives while attempt in `LOCAL_HANDOFF_PENDING` before `send()`. | - Attempt: Remains in `LOCAL_HANDOFF_PENDING`.<br/>- Historical Record: Not confirmed.<br/>- Assembler: **Ingestion strictly blocked**. Zero mutation.<br/>- Chunk Frame: **Rejected and dropped as unconfirmed**.<br/>- Terminal: No. |
| **5** | Promise rejects after 1 chunk accepted | Batch [0, 1]. Chunk 0 accepted (`AWAITING_REMOTE_RESPONSE`). Promise rejects. | - Chunk 0: Untouched in `AWAITING_REMOTE_RESPONSE`; remote timer runs.<br/>- Chunk 1: Settled as `LOCAL_HANDOFF_FAILURE`.<br/>- Assembler: Zero mutation.<br/>- Batch: Retains Chunk 0 in `unsettledChunkIndices`; remains active. |
| **6** | Watchdog expires before acceptance | 2,000 ms watchdog expires without `send()` confirmation. | - Attempt: Settled as `LOCAL_WATCHDOG_TIMEOUT`.<br/>- Historical Record: Marked unconfirmed.<br/>- Assembler: Zero mutation.<br/>- Terminal: Yes.<br/>- Retry: Permitted locally (zero remote budget consumed). |
| **7** | Delayed response after confirmed remote timeout | Attempt reached `AWAITING_REMOTE_RESPONSE` $\to$ timed out (5s) $\to$ delayed chunk arrives. | - Attempt: Already settled as `REMOTE_TIMEOUT`.<br/>- Historical Record: Confirmed `DATACHANNEL_ACCEPTED`.<br/>- Assembler: **Ingestion allowed**; stored.<br/>- Chunk State: Set to `RECEIVED_VERIFIED`.<br/>- Batch: Not resurrected. |
| **8** | Delayed response arrives after failover | Old supplier sends chunk after failover to new supplier. | - Supplier Fencing: Fails (`session.supplierPeerId !== oldSupplier`).<br/>- Assembler: **Ingestion strictly blocked**.<br/>- Chunk Frame: Dropped as stale.<br/>- Transfer: Undisturbed. |
| **9** | Stale-generation response after revocation | Disconnected supplier sends chunk with generation $G-1$. | - Generation Fencing: Fails ($G-1 < G_{\text{session}}$).<br/>- Assembler: **Ingestion strictly blocked**.<br/>- Chunk Frame: Dropped as stale.<br/>- Transfer: Undisturbed. |
| **10** | Duplicate chunk arrives after assembly admission | Authenticated supplier retransmits chunk for already verified index. | - Assembler: `ingestChunk()` detects identical hash $\to$ returns `{ ok: true, duplicate: true }`.<br/>- Storage: Zero mutation.<br/>- Pipeline: Dropped idempotently without error. Zero state change. |

---

## 8. R9-7 — Reconciled Master Specification Summary

The master specification `docs/PHASE7_5_M4_2D_ARCHITECTURE_AND_INTERFACE_SPECIFICATION.md` is updated to **Version 1.9.0**:
- Aligns all lifecycle stages with the precise DataChannel acceptance definition (§2).
- Enforces strict unconfirmed chunk rejection (§3).
- Uses `AuthenticatedSupplierContext` consistently (§4).
- Incorporates the complete disconnect decision table (§5).
- Updates memory accounting to the 5-tier classification (§6).
- Codifies the 10 core adversarial test cases (§7).

---

## 9. Explicit Remaining Open Dependencies

| Dependency | Target Milestone | Description | Architectural Impact |
| :--- | :--- | :--- | :--- |
| **DEP-M4.3-1** | M4.3 Transport Integration | Synchronous adapter invocation of `handleHandoffSuccess` immediately following `RTCDataChannel.send()` return. | Required to prevent inbound preemption races. |
| **DEP-M4.3-2** | M4.3 Transport Integration | Transport-level DataChannel completion vs protocol wire ACK. | Outbound pipeline chunk acknowledgement. |
| **DEP-M5-1** | M5 State Recovery | Ownership and retention of `rawBytes` and `StateSnapshotPayloadV2` inside `SnapshotInstaller`. | Post-handoff memory boundary. |

---

## 10. Modified Files Register & Frozen Baseline Integrity Confirmation

### 10.1 Modified Files
1. `docs/PHASE7_5_M4_2D_R9_TRANSPORT_AND_ADMISSION_CORRECTION_REPORT.md`: (Created) Authoritative architectural correction report.
2. `docs/PHASE7_5_M4_2D_ARCHITECTURE_AND_INTERFACE_SPECIFICATION.md`: (Updated to v1.9.0) Master specification document.
3. `docs/design/README.md`: (Updated) Design documentation index.

### 10.2 Frozen Baseline Integrity Confirmation (0 Diffs)
- `packages/protocol/`: 0 diffs against M4.1 baseline (`fd23edae5bdb52cff32674860229277a79cb1aca`).
- `packages/sync-engine/src/recovery/snapshot-serializer.ts`: 0 diffs against M4.2a baseline (`e3245912626758c094d83edbd4d9264a922070f5`).
- `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`: 0 diffs against M4.2b baseline (`8f509f94a39be9ece4060003ee498dc747efb813`).
- `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`: 0 diffs against M4.2b baseline (`8f509f94a39be9ece4060003ee498dc747efb813`).
- `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`: 0 diffs against M4.2c baseline (`79919a0f8cb0bb81cb80cc010fe02918bef79787`).
- `packages/sync-engine/src/engine/custody.ts`: 0 diffs.

---

## 11. Final Decision Gate

**Verdict:** **`READY FOR INDEPENDENT REVIEW — IMPLEMENTATION STILL UNAUTHORIZED`**

> [!CRITICAL]
> **M4.2d implementation remains strictly unauthorized pending independent review and explicit user approval.**
