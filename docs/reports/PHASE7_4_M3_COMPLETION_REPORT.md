# Phase 7.4 Milestone M3 Completion Report: Repair Request Pipeline (Requester-Side Lifecycle)

**Milestone:** Phase 7.4 — Milestone M3 (Repair Request Pipeline)  
**Status:** COMPLETED, AUDITED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Milestone M1 Baseline:** `309b808` (amending `5cadc03`)  
**Milestone M2 Baseline:** `2d5e9f3` (docs: `4926c52`)  
**Milestone M3 Implementation Commit:** `9ae0f0b`  
**Date:** 2026-11-18  

---

## 1. Executive Summary

Milestone M3 implements the **Requester-Side Repair Request Pipeline** for the SyncPlay Protocol anti-entropy subsystem.

Following the mandatory transport authentication and generation-binding audit, M3 strictly enforces:

1. **Mandatory Authenticated Transport Context (Zero Unauthenticated Path):**
   - For every network-originated `OPERATION_BATCH`:
     - A genuine, unforgeable `AuthenticatedSupplierContext` is strictly required. Missing context is rejected with `REJECTED_INVALID` before calling `engine.ingestRepairBatch()`.
     - Fabricated plain objects, unregistered contexts, and cross-session contexts are rejected using the engine's capability check (`engine.isValidSupplierContext(context)`).
     - Context supplier identity and connection generation must match the active `InFlightRepairRequest` and the engine's active generation for that supplier.
     - There is zero optional-context fallback in production.
   - For every network-originated `REPAIR_REJECTED`:
     - A genuine `AuthenticatedSupplierContext` is strictly required.
     - Unauthenticated, fabricated, mismatched, or obsolete rejection messages are dropped immediately without triggering retries, failovers, backoff timers, or mutating supplier knowledge.
     - Spoofed rejections from unauthorized or third-party peers cannot poison replication knowledge or consume retry budgets.

2. **Connection Generation Binding on In-Flight Requests:**
   - Every `InFlightRepairRequest` records the supplier's active `supplierConnectionGeneration` at the exact moment of dispatch.
   - For both `OPERATION_BATCH` and `REPAIR_REJECTED`, the response must satisfy four strict equality invariants:
     $$\text{context.connectionGeneration} = \text{inFlight.supplierConnectionGeneration} = \text{engine.getPeerGeneration(inFlight.supplierPeerId)}$$
     $$\text{context.supplierPeerId} = \text{inFlight.supplierPeerId} = \text{payload.supplierPeerId}$$
   - When a peer reconnects, its connection generation increments on the engine (`authorizePeerGeneration`). Active in-flight requests bound to the previous generation are immediately invalidated, cancelled, and re-enqueued for failover via `onPeerGenerationChanged`.
   - A reconnected peer cannot fulfill requests that were dispatched under an older connection generation.

3. **Partial-Response Range Safety (Prefix Preservation):**
   - When a supplier returns a partial range (e.g., `[10, 20]` for request `[1, 32]`), operations $10..20$ cannot execute immediately because $1..9$ are missing.
   - Operations $10..20$ are buffered in `PendingDependencyQueue`. The engine replication frontier does not advance to 20.
   - The unfulfilled prefix `[1, 9]` is not lost: remainder ranges are calculated from the actual replication frontier ($\max(\text{inFlight.fromSequence}, V_{\text{rep}}[\text{origin}] + 1)$) rather than dropping unreceived prefixes.
   - A new request is dispatched for $[1, 32]$. When $[1, 9]$ arrives, operations $1..9$ are admitted, and `drainPendingOperations` immediately unblocks and drains the buffered operations $10..20$, advancing the frontier directly to 20 and triggering repair for $[21, 32]$.

4. **Stale Response Rejection Across Membership Epochs:**
   - On `onEpochTransition(newEpoch)`, all active in-flight request timers are cleared, in-flight tracking is purged (`inFlightRequests.clear()`), and queued gaps from the old epoch are wiped.
   - Delayed responses arriving from older epochs are rejected before engine ingestion.

5. **Globally Bounded Logical Retry Budget & Concurrency Invariants:**
   - A single logical repair range has a finite, globally bounded retry budget ($\le 3$ retries).
   - Switching suppliers during failover consumes from the same logical retry budget.
   - Timeout duration ($1500\text{ms}$) and exponential backoff durations ($1\text{s}, 2\text{s}, 4\text{s}$) are strictly sequential and non-overlapping.
   - `RATE_LIMITED` and `BUSY` consume retry budget.
   - Concurrency limits ($\le 2$ requests per supplier peer; $\le 16$ room-wide) are strictly enforced at all times.

6. **Preservation of Milestone M2 Historical Completeness Invariant:**
   - Repairing known missing ranges advances $V_{\text{rep}}$, but **never** falsely reports unconditional complete historical identity (`IDENTICAL`) when intermediate history remains unverified ($k > 1$ with single tail fingerprints).
   - Preserves `hasUnverifiedHistory === true` and `classification === 'FRONTIER_AGREEMENT_ONLY'` in full accordance with Milestone M2 invariants.

7. **Strict Scope Boundaries:**
   - M4 (Supplier Range Servicing): Untouched.
   - M5 (WebRTC DataChannel Integration): Untouched.
   - Phase 7.5 (Snapshot Recovery): Untouched.

---

## 2. Baselines and Lineage

- **Approved Phase 7.4 Architecture Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`
- **Approved Phase 7.2-R9 Causal Baseline:** `1825de1`
- **Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`
- **Milestone M1 Protocol Schemas Baseline:** `309b808` (amending `5cadc03`)
- **Milestone M2 Digest & Comparison Baseline:** `2d5e9f3` (docs: `4926c52`)

---

## 3. Implementation Details

### 3.1. `RepairRequestPipeline` (`packages/sync-engine/src/replication/repair-request-pipeline.ts`)
- `handleRepairBatch(batch: OperationBatchPayload, context: AuthenticatedSupplierContext): EngineDecision[]`:
  - Enforces mandatory `context` check via `engine.isValidSupplierContext(context)`.
  - Validates correlation: active request ID, session ID, epoch, supplier ID, context supplier ID, connection generation (must equal dispatch generation AND active engine generation), target origin, and sequence range bounds.
  - Evaluates progress against local replication frontier; recalculates remainder from actual frontier ($\max(\text{inFlight.fromSequence}, V_{\text{rep}}[\text{origin}] + 1)$) to ensure missing prefixes are preserved.
- `handleRepairRejected(rejection: RepairRejectedPayload, context: AuthenticatedSupplierContext): void`:
  - Enforces mandatory `context` check via `engine.isValidSupplierContext(context)`.
  - Validates correlation: active request ID, session ID, supplier ID, context supplier ID, connection generation, target origin, and sequence range bounds.
  - Drops unauthenticated or mismatched rejections without mutating peer knowledge or triggering retry/failover.
- `onPeerGenerationChanged(peerId: string, newGeneration: number): void`:
  - Automatically called via `engine.onPeerGenerationChange` when a peer reconnects.
  - Invalidates active in-flight requests to `peerId` dispatched under older generations, cancels timers, records attempted supplier, and re-enqueues for failover.
- `drainQueue()`:
  - Records `supplierConnectionGeneration` in `InFlightRepairRequest` at dispatch.

### 3.2. `LeaderlessSyncEngine` (`packages/sync-engine/src/engine/leaderless-sync-engine.ts`)
- `isValidSupplierContext(context?: AuthenticatedSupplierContext): boolean`:
  - Verifies presence in internal `validSupplierContexts` WeakSet.
- `ingestRepairBatch(batch: OperationBatchPayload, transportContext: AuthenticatedSupplierContext): EngineDecision[]`:
  - Mandatory transport context parameter.
  - Buffers out-of-order operations in `PendingDependencyQueue` when missing forward dependencies occur in repair batches, preventing spurious rejections.
- `onPeerGenerationChange(listener: (peerId: string, newGen: number, oldGen?: number) => void): () => void`:
  - Publishes peer generation change events to registered listeners on `authorizePeerGeneration`.

### 3.3. `AntiEntropyManager` (`packages/sync-engine/src/replication/anti-entropy-manager.ts`)
- Updates `handleRepairBatch` and `handleRepairRejected` signatures to require `context: AuthenticatedSupplierContext`.
- Provides `onPeerGenerationChanged` delegating to pipeline.

---

## 4. Verification and Adversarial Tests

### 4.1. Adversarial Test Matrix (`packages/sync-engine/src/__tests__/repair-request-pipeline.test.ts`)
The test suite contains 45 tests, including 10 targeted adversarial tests verifying Section 5 of the mandate:

| # | Test Description | Result | Safety Invariant Verified |
|---|---|---|---|
| 1 | Inbound `OPERATION_BATCH` without context | PASSED | Rejected with `REJECTED_INVALID` before engine ingestion; engine state unmutated. |
| 2 | Inbound `OPERATION_BATCH` with fabricated context | PASSED | Rejected with `REJECTED_INVALID` before engine ingestion; engine state unmutated. |
| 3 | Inbound `REPAIR_REJECTED` without context | PASSED | Dropped; request remains active in-flight; no failover or degradation. |
| 4 | Inbound `REPAIR_REJECTED` with fabricated context | PASSED | Dropped; request remains active in-flight; no state mutation. |
| 5 | `REPAIR_REJECTED` from peer X for request sent to peer Y | PASSED | Dropped due to supplier mismatch; targeted request remains active. |
| 6 | Request on gen 3; peer reconnects to gen 4; response arrives for gen-3 request | PASSED | Gen-3 request invalidated on reconnect; response rejected with `REJECTED_INVALID`; engine state unmutated. |
| 7 | Response arrives with context gen differing from dispatch gen | PASSED | Rejected with `REJECTED_INVALID` before engine ingestion. |
| 8 | Response arrives with context gen differing from engine active gen | PASSED | Rejected with `REJECTED_INVALID` before engine ingestion. |
| 9 | `REPAIR_REJECTED` arriving after peer reconnect with old gen | PASSED | Dropped without mutating retry count or triggering failover. |
| 10 | Partial response `[10, 20]` for request `[1, 32]` | PASSED | Operations $10..20$ buffered in `PendingDependencyQueue`; prefix $[1, 9]$ preserved in repair queue; unblocks and converges to 32 once $[1, 9]$ and $[21, 32]$ arrive. |

### 4.2. Monorepo Test Results
- **`@syncplay/sync-engine`:** **356 passed** across 23 files (0 failures).
- **`@syncplay/protocol`:** **273 passed** across 4 files (0 failures).
- **`@syncplay/peer-mesh`:** **101 passed** across 6 files (0 failures).
- **`@syncplay/control-events`:** **49 passed** across 6 files (0 failures).
- **`@syncplay/bootstrap-server`:** **91 passed** across 3 files (0 failures).
- **Total Monorepo Tests:** **870 passed across 42 files with 0 failures**.
- **Build & Typecheck:** `tsc` and Vite production build passed cleanly across all packages.

---

## 5. Explicit Stop Condition Responses

As required by Section 6 of the mandate:

1. **Can an unauthenticated or fabricated transport context ever reach `LeaderlessSyncEngine.ingestRepairBatch()` in production?**  
   **NO.** `RepairRequestPipeline.handleRepairBatch()` validates `engine.isValidSupplierContext(context)` as its first pre-ingestion check and rejects missing, fabricated, or unregistered contexts immediately with `REJECTED_INVALID`.

2. **Can an unauthenticated `REPAIR_REJECTED` trigger failover, retry backoff, or supplier blacklist?**  
   **NO.** `RepairRequestPipeline.handleRepairRejected()` requires a genuine, unforgeable `AuthenticatedSupplierContext` registered with the engine; any unauthenticated or fabricated rejection is silently dropped before mutating in-flight requests, peer knowledge, retry counts, or backoff timers.

3. **Can a reconnected peer fulfill a repair request dispatched prior to reconnection?**  
   **NO.** Every in-flight request is capability-authenticated and connection-generation-bound to the active `supplierConnectionGeneration` at dispatch. When a peer reconnects, `onPeerGenerationChanged` immediately invalidates and fails over the previous request. Furthermore, `handleRepairBatch` rejects any response where `context.connectionGeneration` does not strictly equal the dispatch generation recorded in the request.

4. **Can a partial response cause missing prefix ranges to be permanently dropped from repair?**  
   **NO.** When a partial batch arrives (e.g. $[10, 20]$ for request $[1, 32]$), out-of-order operations are buffered in `PendingDependencyQueue` without advancing the replication frontier. The unfulfilled range is recalculated from the actual local replication frontier ($\max(\text{inFlight.fromSequence}, V_{\text{rep}}[\text{origin}] + 1)$), strictly preserving missing prefix ranges and re-dispatching them for targeted repair.

---

## 6. Trusted Transport Provenance Audit & Production Inbound Path

### 6.1 Real Production Inbound Call Chain
The production inbound routing for both `OPERATION_BATCH` and `REPAIR_REJECTED` originates at the WebRTC transport boundary:
1. **Physical Transport Delivery:** WebRTC DataChannel message event triggers `WebRtcTransport.events.onDataChannelMessage(data)`.
2. **Channel Identity & Generation Binding:** `PeerManager.handleDataChannelMessage(fromPeerId, data)` validates the envelope via `parseAndValidateMessage(data)`. The `fromPeerId` is verified during signaling and channel setup; the active `connectionGeneration` is retrieved from `peerTable.get(fromPeerId).generation`.
3. **Trusted Transport Adapter Routing:** `PeerManager` routes repair payloads to `TrustedTransportAdapter.handleInboundRepairBatch(batch, fromPeerId, generation)` or `TrustedTransportAdapter.handleInboundRepairRejected(rejection, fromPeerId, generation)`.
4. **Capability-Gated Context Issuance:** `TrustedTransportAdapter` holds the private `TransportRegistrationCapability` granted at the composition root and registers the context via `engine.registerInboundSupplierContext(fromPeerId, generation, capability)`.
5. **Pipeline Dispatch & Ingestion:**
   - For `OPERATION_BATCH`: routed through `AntiEntropyManager.handleRepairBatch(batch, context)` $\to$ `RepairRequestPipeline.handleRepairBatch(batch, context)`. Correlation, supplier identity, epoch, origin, range, and generation are verified against the active in-flight request, and valid operations are ingested into `LeaderlessSyncEngine.ingestRepairBatch()`.
   - For `REPAIR_REJECTED`: routed through `AntiEntropyManager.handleRepairRejected(rejection, context)` $\to$ `RepairRequestPipeline.handleRepairRejected(rejection, context)`. Verification occurs before triggering failover, retry backoff, or peer blacklisting.

### 6.2 Transport State Derivation vs. Payload Fields
Neither the pipeline nor the engine trusts the sender ID, supplier ID, or connection generation claimed within inbound message payloads. Identity and generation are derived solely from trusted transport state (`fromPeerId` and `transportConnectionGeneration` maintained by the local WebRTC manager) and stamped into an unforgeable in-memory `AuthenticatedSupplierContext`. Any mismatch between the packet payload's `supplierPeerId` and the transport context's `supplierPeerId` triggers an immediate drop (`REJECTED_INVALID: payload supplier does not match transport context supplier`).

### 6.3 Capability Custody & Export Boundary
1. **No Public Capability Accessors:** The `authorizedTransportCapability` is held in a private instance field (`#authorizedTransportCapability`) within `LeaderlessSyncEngine`. No getter, method, or public property exposes it.
2. **Strict Object Reference Equality:** Calling the exported factory `createTransportRegistrationCapability(...)` constructs an independent instance with a distinct memory reference. The engine validates registration via `if (capability !== this.authorizedTransportCapability)` and throws `SyncEngineError('UNAUTHORIZED')` on any reference mismatch. Cloned or forged capability objects `{ ...cap }` are rejected.
3. **Composition Root Confinement:** Only the application composition root has access to initialize the engine with its capability and inject it into the `TrustedTransportAdapter`. Ordinary SDK consumers cannot register or manufacture contexts.

### 6.4 Context Reuse & Packet-Binding Analysis
- **Context Model:** `AuthenticatedSupplierContext` represents the local transport driver's trusted attestation of a specific remote peer connection (`supplierPeerId`, `connectionGeneration`). It is an immutable, frozen in-memory object (`Object.freeze`) that is never sent over the network.
- **Cross-Supplier Reuse:** Impossible. `context.supplierPeerId` is verified against both `inFlight.supplierPeerId` and `batch.supplierPeerId`.
- **Cross-Generation Reuse:** Impossible. `context.connectionGeneration` is verified against `inFlight.supplierConnectionGeneration` and the current generation returned by `engine.getPeerGeneration()`.
- **Cross-Session Reuse:** Impossible. The context's engine session ID is verified against the engine's active `sessionId`.
- **Packet-Binding and Replay Safety:** Safety against packet replay or context recycling is guaranteed by the request-correlation and in-flight tracking mechanism:
  - Every repair request generates a unique UUID v4 `requestId`.
  - Exactly one in-flight request exists per `(originPeerId, supplierPeerId)`.
  - When an inbound packet is processed, the request is either retired on completion or updated for the remaining prefix range.
  - Any replayed, duplicate, or unsolicited packet arrives with an unrecognized `requestId` and is immediately dropped with `REJECTED_INVALID: not an active in-flight request`.

---

## 7. Final Safety Audit Questions & Explicit Determinations

| # | Mandatory Question | Determination | Verification Detail |
|---|---|:---:|---|
| 1 | **Can an ordinary production SDK consumer manufacture or register a valid supplier context?** | **NO** | The engine requires reference equality against its internal private `authorizedTransportCapability`. `createTransportRegistrationCapability()` produces non-matching instances. No public API or getter exposes the capability. |
| 2 | **Does the production inbound path derive supplier identity and generation from trusted transport state rather than the payload?** | **YES** | `TrustedTransportAdapter.handleInboundRepairBatch` and `handleInboundRepairRejected` take `transportSupplierPeerId` and `transportConnectionGeneration` directly from the WebRTC connection state, stamping them into the context. Payload fields are checked against this context; payload claims cannot forge context fields. |
| 3 | **Can an unauthenticated or mismatched `REPAIR_REJECTED` mutate repair lifecycle state?** | **NO** | `RepairRequestPipeline.handleRepairRejected()` requires a valid, registered `AuthenticatedSupplierContext`. Unauthenticated rejections, generation mismatches, or supplier mismatches are dropped prior to touching in-flight requests, peer knowledge, or backoff timers. |
| 4 | **Is context reuse prevented, or is its safety guaranteed by a documented packet-binding mechanism?** | **GUARANTEED BY BINDING** | An authenticated context attests to an active channel `(peerId, generation)`. Replay or reuse across requests is prevented by UUID v4 in-flight request correlation: requests are removed upon completion, rendering any replayed packet invalid (`not an active in-flight request`). |
| 5 | **Is the Phase 7.3-R5-H capability boundary preserved?** | **YES** | The Phase 7.3-R5-H object-capability pattern is strictly maintained. The transport adapter is granted registration capability exclusively at composition root; all anti-entropy repair operations require capability-authenticated contexts before admission. |

---

## 8. Final Duplicate Repair Response and Replay-Safety Audit

### 8.1 Mechanism: Atomic Retirement Before Performing Side Effects
To guarantee that duplicate, replayed, or re-entrant repair responses can never cause duplicate state mutations or corrupt request lifecycle state, the pipeline enforces strict **atomic retirement before side effects**:
1. **`RepairRequestPipeline.handleRepairBatch()`:**
   - Synchronously clears the in-flight request timer: `this.clock.clearTimeout(inFlight.timerId)`.
   - **Immediately and atomically deletes the request from tracking**: `this.inFlightRequests.delete(batch.requestId)` **prior** to invoking `this.engine.ingestRepairBatch(batch, context)`.
   - Any duplicate packet delivered sequentially or re-entrantly (e.g., via synchronous `stateChangeListener` callbacks fired during operation admission) immediately fails the correlation check `!this.inFlightRequests.has(batch.requestId)` and is rejected with `REJECTED_INVALID`.
   - It cannot re-ingest operations, advance replication vectors, enqueue duplicate pending-queue entries, increment `totalRepairsCompleted` multiple times, or create duplicate remainder ranges.
2. **`RepairRequestPipeline.handleRepairRejected()`:**
   - Synchronously clears the in-flight request timer: `this.clock.clearTimeout(inFlight.timerId)`.
   - **Immediately and atomically deletes the request from tracking**: `this.inFlightRequests.delete(rejection.requestId)` **prior** to mutating `inFlight.attemptedSuppliers`, updating peer knowledge, or scheduling backoff timers.
   - Any duplicate rejection packet delivered with the same `requestId` is silently dropped without consuming another retry, scheduling another backoff timer, penalizing a supplier twice, or triggering duplicate failover.

### 8.2 Explicit Context Terminology
The architectural boundaries are strictly differentiated:
- **`AuthenticatedSupplierContext`:** Authenticates the remote peer connection and its active connection generation within the local in-process object-capability boundary.
- **`requestId` (UUID v4):** Correlates a response with an active, outstanding logical request. It does not provide cryptographic packet authentication.
- **Duplicate-Response Handling:** Protects the internal request lifecycle, retry budgets, and state transitions against network replays, retransmissions, and re-entrant deliveries.

### 8.3 Adversarial Test Matrix (`packages/sync-engine/src/__tests__/duplicate-repair-safety.test.ts`)
The 7 dedicated tests verify all edge cases:

| # | Test Scenario | Result | State & Lifecycle Invariants Verified |
|---|---|:---:|---|
| 1 | Identical `OPERATION_BATCH` delivered twice while request is active | PASSED | Re-entrant duplicate during admission rejected with `REJECTED_INVALID`; sequential duplicate rejected; $V_{\text{rep}}$ advances once; `totalRepairsCompleted` is 1. |
| 2 | Identical `OPERATION_BATCH` delivered after request completion | PASSED | Rejected with `REJECTED_INVALID`; engine stats and state version untouched; no re-admission. |
| 3 | Identical partial `OPERATION_BATCH` delivered twice | PASSED | Operations $1..5$ applied; remainder $[6..10]$ dispatched under new $R_2$; duplicate of $R_1$ rejected; no duplicate remainder gap. |
| 4 | Identical `REPAIR_REJECTED` delivered twice while request is active | PASSED | Duplicate rejection dropped; retry count increments once; exactly one backoff timer scheduled; one retry dispatched. |
| 5 | Duplicate response after supplier failover | PASSED | Delayed response from old supplier rejected; active failover request to secondary supplier unaffected and converges cleanly. |
| 6 | Duplicate response after epoch transition | PASSED | Request tracking wiped on epoch transition; old epoch duplicate rejected with `REJECTED_INVALID`; zero operations admitted in new epoch. |
| 7 | Legitimate subsequent partial response completes remaining range | PASSED | Partial batch 1 admitted; duplicate rejected; subsequent partial batch 2 completes range; convergence verified. |

### 8.4 Answers to Duplicate-Response Audit Questions

1. **Can a duplicate `OPERATION_BATCH` mutate repair lifecycle state twice?**  
   **NO.** The request is atomically removed from `inFlightRequests` before `ingestRepairBatch` is called. Any duplicate (re-entrant or sequential) fails `inFlightRequests.has(batch.requestId)` and is rejected with `REJECTED_INVALID` without touching engine state or pipeline stats.
2. **Can a duplicate `REPAIR_REJECTED` consume retry budget twice?**  
   **NO.** The request is atomically removed from `inFlightRequests` before `reEnqueueForFailover` or backoff scheduling occurs. Any duplicate rejection is immediately dropped on `!inFlightRequests.has(rejection.requestId)`, preventing duplicate retry consumption or redundant timer creation.
3. **Can a duplicate partial response lose or corrupt a missing range?**  
   **NO.** A partial batch updates the local replication frontier and dispatches the remainder range with a fresh `requestId`. The duplicate of the original partial batch is rejected, leaving the active remainder request intact and preserving all missing ranges.
4. **Does legitimate partial repair still work after the correction?**  
   **YES.** When the supplier legitimately returns the subsequent chunk matching the newly dispatched request ID, it is correlated, admitted, and converges the remaining sequence range.

---

## 9. Final Retirement Failure-Safety, Ingestion Recovery & Reconciled Audit

### 9.1 Synchronous In-Process Request Retirement Architecture
- **Pre-Retirement Validation:** In both `handleRepairBatch` and `handleRepairRejected`, all transport authenticity checks (`engine.isValidSupplierContext`), correlation checks (`inFlightRequests.has`), session ID checks, epoch checks, supplier identity checks, connection generation checks, and range bound checks execute *prior* to retirement.
- **Synchronous In-Process Timing:** Retirement occurs via synchronous in-process deletion (`this.inFlightRequests.delete(requestId)`) accompanied by timer clearance (`this.clock.clearTimeout(inFlight.timerId)`). This is strictly an in-memory, synchronous state transition protecting local pipeline state machine invariants, not a distributed atomic transaction across nodes.
- **Re-entrancy Protection:** Because the deletion occurs before invoking `engine.ingestRepairBatch()`, any synchronous listener callback attempting re-entrant delivery fails correlation and is rejected immediately with `REJECTED_INVALID`.
- **No Delayed Resurrection:** Once retired, an old `requestId` can never be revived or restored by a delayed network packet.

### 9.2 Ingestion Failure Safety & State-Driven Recovery
To prevent ingestion failures from causing silent loss of repair responsibility, `handleRepairBatch()` encapsulates engine admission in a structured recovery block:
```typescript
try {
  decisions = this.engine.ingestRepairBatch(batch, context);
} catch (err) {
  ingestionError = err;
  decisions = [{ accepted: false, code: 'REJECTED_INVALID', ... }];
}
```
**Invariant Guaranteed:** Retiring a request never permanently loses the responsibility to repair its outstanding sequence range:
1. If `ingestRepairBatch()` throws an exception, the exception is caught and recorded as an invalid decision.
2. The pipeline evaluates actual progress by inspecting the true engine replication frontier: `currentSeq = engine.replicationFrontier[origin] ?? 0`.
3. The unfulfilled range is calculated as `[max(inFlight.fromSequence, currentSeq + 1), inFlight.toSequence]`.
4. If `ingestionError !== undefined`, the supplier attempt is recorded (`inFlight.attemptedSuppliers.add(supplier)`), retry count is incremented, and `reEnqueueGapForFailover` re-queues the range with exponential backoff or triggers `onHistoryExhausted` if `maxRetries` (3) is exceeded.

### 9.3 Partial-Response Shapes Analysis

| Partial-Response Shape | Pipeline & Engine Behavior | State Verification Invariant |
|---|---|---|
| **Complete range** $[1..10]$ for $[1..10]$ | All operations admitted; frontier advances to 10; `totalRepairsCompleted++`. | Range marked complete; active requests 0. |
| **Prefix only** $[1..5]$ for $[1..10]$ | Operations $1..5$ admitted; frontier advances to 5; suffix $[6..10]$ enqueued and dispatched immediately under fresh $R_2$. | Unfulfilled suffix guaranteed repaired; no missing range lost. |
| **Suffix only** $[6..10]$ for $[1..10]$ | Missing prefix $1..5$ causes ops $6..10$ to be buffered in `PendingDependencyQueue`; frontier stays 0; range $[1..10]$ is re-enqueued. When prefix $[1..5]$ arrives, buffered ops $6..10$ are drained and admitted. | Out-of-order operations buffered safely; frontier reflects only contiguous operations. |
| **Middle range** $[6..15]$ for $[1..20]$ | Operations $6..15$ buffered in `PendingDependencyQueue`; frontier stays 0; range $[1..20]$ re-enqueued. Prefix $[1..5]$ drains middle range to frontier 15; suffix $[16..20]$ dispatched and completed. | Prefix and suffix both repaired; middle buffered and unblocked without duplicate requests. |
| **Partially admitted** (errors midway) | Contiguous operations up to error point admitted; frontier reflects contiguous progress; unfulfilled remainder derived from frontier and re-enqueued. | Partial admission advances frontier accurately; remainder strictly preserved. |
| **Duplicate of earlier partial batch** | Arrives with old $R_1$ while remainder $R_2$ is active; rejected with `REJECTED_INVALID`; $R_2$ proceeds unaffected. | Zero double-ingestion; no corrupted remainder ranges. |

### 9.4 Full Test Count Reconciliation

The test suite progression across Milestone M3 is reconciled below:

| Milestone Stage | `@syncplay/sync-engine` | Total Monorepo Tests | Test Files | New Test Suite Added |
|---|:---:|:---:|:---:|---|
| **M3 Initial Pipeline Implementation** | 336 | 850 | 39 | `repair-request-pipeline.test.ts` (45 tests) |
| **M3 Trusted Transport Provenance Audit** | 344 (+8) | 858 (+8) | 40 (+1) | `trusted-transport-provenance.test.ts` (8 tests) |
| **M3 Duplicate Response & Replay Audit** | 351 (+7) | 865 (+7) | 41 (+1) | `duplicate-repair-safety.test.ts` (7 tests) |
| **M3 Retirement Failure-Safety Final Audit** | **356 (+5)** | **870 (+5)** | **42 (+1)** | `retirement-failure-safety.test.ts` (5 tests) |

### 9.5 Fresh Monorepo Test Results (`npm test`)
- **Command:** `npm test` from monorepo root.
- **`@syncplay/control-events`:** **49 passed** (6 files)
- **`@syncplay/peer-mesh`:** **101 passed** (6 files)
- **`@syncplay/protocol`:** **273 passed** (4 files)
- **`@syncplay/sync-engine`:** **356 passed** (23 files)
- **`@syncplay/bootstrap-server`:** **91 passed** (3 files)
- **Total Monorepo:** **870 passed across 42 files with 0 failures, 0 skipped, 0 excluded**.
- **Monorepo Build (`npm run build`):** Clean TypeScript typecheck (`tsc`) and Vite bundle build across all packages.

### 9.6 Answers to Final Stop Conditions

1. **Can an ingestion failure after request retirement permanently lose a missing repair range?**  
   **NO.** `handleRepairBatch()` catches ingestion exceptions and uses the true engine replication frontier to recalculate and recover the unfulfilled sequence range into `pendingGaps` with failover and bounded retries.
2. **Can partial admission cause an unfulfilled prefix or suffix to disappear?**  
   **NO.** Unfulfilled ranges are recalculated as `[max(inFlight.fromSequence, currentSeq + 1), inFlight.toSequence]` directly from the contiguous replication frontier. Out-of-order suffixes are buffered in `PendingDependencyQueue` and drained upon prefix admission.
3. **Can a re-entrant or sequential duplicate process the same request twice?**  
   **NO.** Synchronous in-process retirement deletes the request from `inFlightRequests` before ingestion begins. Any re-entrant or sequential duplicate fails `inFlightRequests.has()` and is rejected with `REJECTED_INVALID`.
4. **Can a duplicate rejection consume retry budget or schedule another backoff?**  
   **NO.** Synchronous deletion occurs before retry or backoff scheduling; duplicate rejections are dropped on `!inFlightRequests.has()`.
5. **Are all remaining repair ranges calculated from actual engine state?**  
   **YES.** All ranges are calculated from `engine.replicationFrontier[origin]`, guaranteeing that only contiguously admitted operations advance repair progress.
6. **Are the final test counts freshly verified and reconciled?**  
   **YES.** Monorepo: 870 tests across 42 test files with 0 failures; `@syncplay/sync-engine`: 356 tests across 23 files.
7. **Is the final implementation committed with a clean working tree?**  
   **YES.** Clean working tree committed to `main`.



