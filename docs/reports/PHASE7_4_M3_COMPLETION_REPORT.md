# Phase 7.4 Milestone M3 Completion Report: Repair Request Pipeline (Requester-Side Lifecycle)

**Milestone:** Phase 7.4 — Milestone M3 (Repair Request Pipeline)  
**Status:** COMPLETED, AUDITED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Milestone M1 Baseline:** `309b808` (amending `5cadc03`)  
**Milestone M2 Baseline:** `2d5e9f3` (docs: `4926c52`)  
**Milestone M3 Initial Implementation Commit:** `b5dc7e8` (docs: `5d151e7`)  
**Milestone M3 Final Safety Audit & Correlation Commit:** `0e1b678`  
**Date:** 2026-11-18  

---

## 1. Executive Summary

Milestone M3 implements the **Requester-Side Repair Request Pipeline** for the SyncPlay Protocol anti-entropy subsystem.

Following the final safety audit and architectural closure, M3 rigorously enforces:

1. **Pre-Ingestion Correlation and Structural Validation (Zero-Bypass):**
   - Every inbound `OPERATION_BATCH` and `REPAIR_REJECTED` is validated against active in-flight request tracking **before** engine ingestion is permitted.
   - Any response carrying an unknown, expired, cancelled, duplicate, or obsolete `requestId` is rejected immediately with `REJECTED_INVALID`, without mutating engine state.
   - Verifies 8 invariant criteria before admission:
     1. `batch.requestId` correlates to an active record in `inFlightRequests`.
     2. `batch.sessionId === this.engine.sessionId === inFlight.sessionId`.
     3. `batch.membershipEpoch === this.engine.epoch === inFlight.membershipEpoch`.
     4. `batch.supplierPeerId === inFlight.supplierPeerId`.
     5. If transport context is present: `context.supplierPeerId === inFlight.supplierPeerId`.
     6. If transport context specifies generation: `context.connectionGeneration === engine.getPeerGeneration(inFlight.supplierPeerId)`.
     7. `batch.originPeerId === inFlight.originPeerId`.
     8. `batch.fromSequence >= inFlight.fromSequence && batch.toSequence <= inFlight.toSequence`.

2. **Stale Response Rejection Across Membership Epochs:**
   - On `onEpochTransition(newEpoch)`:
     - All active in-flight request timers are cleared.
     - All in-flight request correlation entries are invalidated (`inFlightRequests.clear()`).
     - All pending gaps and backoff timers queued from the old epoch are cancelled (`pendingGaps.length = 0`).
   - A delayed `OPERATION_BATCH` or `REPAIR_REJECTED` from an older epoch arrives with an obsolete `requestId` that is no longer in `inFlightRequests`. It is rejected before `engine.ingestRepairBatch()` is ever invoked.
   - **Can a delayed response from an obsolete membership epoch mutate current engine state? The answer is strictly NO.**

3. **Advisory Supplier Knowledge Trust Limitations:**
   - Advertised replication vectors and snapshot frontiers received in `StateDigestPayload` are **untrusted remote claims**.
   - They serve exclusively as best-effort hints for candidate supplier selection.
   - They do **not** prove that a supplier actually retains requested operations in its log.
   - They do **not** authorize a supplier or authenticate its connection generation.
   - Authorization and transport identity remain strictly gated by the Phase 7.3-R5-H capability boundary (`AuthenticatedSupplierContext`).
   - Milestone M4 (Supplier Range Servicing) must independently verify actual retained operation logs before fulfilling any request.

4. **Globally Bounded Logical Retry Budget & Concurrency Invariants:**
   - A single logical repair range has a finite, globally bounded retry budget ($\le 3$ retries).
   - Switching suppliers during failover **consumes the same logical retry budget** (`retryCount` is carried forward across failovers, never multiplying without bound).
   - Timeout duration ($1500\text{ms}$) and exponential backoff durations ($1\text{s}, 2\text{s}, 4\text{s}$) are **strictly sequential and non-overlapping**.
   - `RATE_LIMITED` and `BUSY` responses consume from the logical retry budget to prevent infinite spinning.
   - Late responses cannot reset, revive, or extend a completed or exhausted retry budget.
   - Simultaneous timeout, disconnect, and rejection callbacks cannot schedule duplicate retries (idempotent lookup guard).
   - Concurrency limits ($\le 2$ requests per supplier peer; $\le 16$ room-wide) remain strictly enforced at all times.

5. **Partial Batch and Correlation Safety:**
   - Every partial response is correlated to exactly one outstanding request.
   - Only the validated contiguous sub-range $[fromSeq, fromSeq + M - 1]$ is ingested.
   - Local replication frontier advances to $fromSeq + M - 1$, unblocking dependent operations in `PendingDependencyQueue`.
   - Remaining unfulfilled sub-range $[V_{\text{rep}}[origin] + 1, toSeq]$ is recalculated and dispatched under a fresh correlation ID.
   - Duplicate or delayed responses for the already-fulfilled partial range are rejected as inactive request IDs.
   - Responses attempting to serve sequences outside requested bounds are rejected with `REJECTED_INVALID`.

6. **Preservation of Milestone M2 Historical Completeness Invariant:**
   - Completing repairs of known missing ranges advances $V_{\text{rep}}$ to the frontier, but **never** falsely reports unconditional complete historical identity (`IDENTICAL`) when intermediate history remains unverified ($k > 1$ with single tail fingerprints).
   - Preserves `hasUnverifiedHistory === true` and `classification === 'FRONTIER_AGREEMENT_ONLY'` in full accordance with Milestone M2 invariants.

7. **Strict Architectural Scope Separation:**
   - Supplier range servicing (`serveRepairRequest`) remains strictly deferred to Milestone M4.
   - WebRTC DataChannel transport integration remains strictly deferred to Milestone M5.
   - Snapshot recovery / state catch-up remains strictly deferred to Phase 7.5.

---

## 2. Baselines and Lineage

- **Approved Phase 7.4 Architecture Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`
- **Approved Phase 7.2-R9 Causal Baseline:** `1825de1`
- **Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`
- **Milestone M1 Protocol Schemas Baseline:** `309b808` (amending `5cadc03`)
- **Milestone M2 Digest & Comparison Baseline:** `2d5e9f3` (docs: `4926c52`)
- **Milestone M3 Initial Implementation Commit:** `b5dc7e8` (docs: `5d151e7`)
- **Milestone M3 Final Safety Audit & Correlation Commit:** `0e1b678`

---

## 3. Detailed Component Architecture

### 3.1. `RepairRequestPipeline` (`packages/sync-engine/src/replication/repair-request-pipeline.ts`)
- Manages the entire requester-side repair lifecycle.
- Holds internal `pendingGaps: PendingGapTask[]`, `inFlightRequests: Map<string, InFlightRepairRequest>`, and `peerKnowledge: Map<string, PeerReplicationKnowledge>`.
- Generates RFC 4122 UUID v4 `requestId` via `generateUUIDv4()`.
- Implements 8-point pre-ingestion correlation validation in `handleRepairBatch()`.
- Implements correlation validation in `handleRepairRejected()`.
- Enforces globally bounded retry budget across supplier failovers.
- Purges in-flight tracking and queued gaps upon `onEpochTransition()`.

### 3.2. `LeaderlessSyncEngine` Additions (`packages/sync-engine/src/engine/leaderless-sync-engine.ts`)
- `getPendingGaps(): MissingSequenceRange[]`: Computes sequence ranges needed to unblock operations in `PendingDependencyQueue`.
- `localPeerId`: Accesses `this.config.localPeerId`.
- `sessionId`: Accesses `this.config.sessionId`.
- `getActivePeerGenerations(): ReadonlyMap<string, number>`: Exposes snapshot copy of active peer generations map.

### 3.3. `AntiEntropyManager` Integration (`packages/sync-engine/src/replication/anti-entropy-manager.ts`)
- Delegates requester-side repair handling to `this.pipeline`.
- `processRemoteDigest`: records remote peer knowledge and schedules missing ranges from `result.missingFromLocal`.
- `reconcileGaps`: collects gaps from `engine.getPendingGaps()` and adds to pipeline.
- `handleRepairBatch`: validates correlation and passes through `engine.ingestRepairBatch()`.
- `handleRepairRejected`: handles rejections and manages failover/backoff.
- `serveRepairRequest`: explicitly throws `Error('AntiEntropyManager.serveRepairRequest() is scheduled for Phase 7.4 M4')`.

---

## 4. Verification and Test Results

### 4.1. Unit Test Suite (`packages/sync-engine/src/__tests__/repair-request-pipeline.test.ts`)
A dedicated 35-test suite verifies all core and adversarial requirements:

| Test Group | Tests | Result | Verification Scope |
|---|---|---|---|
| **1. Missing Sequence Range Detection & Slicing** | 3 | PASSED | Slices $> 32$ ops into chunks of $\le 32$; trims satisfied prefixes against $V_{\text{rep}}$; drops fully satisfied ranges. |
| **2. Request ID Generation & RFC 4122** | 2 | PASSED | Conforms to RFC 4122 v4 regex; generates unique pairwise request IDs across concurrent requests. |
| **3. Request Tracking & Timeouts** | 1 | PASSED | Tracks in-flight request; fires timeout at $1500\text{ms}$; blacklists failed supplier; fails over. |
| **4. Bounded Retries & Exponential Backoff** | 1 | PASSED | Executes retries with $1\text{s}, 2\text{s}, 4\text{s}$ backoff; aborts after 3 retries; triggers `onHistoryExhausted`. |
| **5. Duplicate Request Suppression** | 1 | PASSED | Suppresses identical and sub-range requests while one is already in flight. |
| **6. Cancellation on Epoch Changes** | 1 | PASSED | Cancels all active in-flight timers and requests upon `onEpochTransition`. |
| **7. REPAIR_REJECTED Handling** | 3 | PASSED | Handles `OUTSIDE_HORIZON` (updates frontier, fails over, or triggers exhausted); handles `ORIGIN_UNKNOWN`, `RATE_LIMITED`, `EPOCH_MISMATCH`, and `BUSY`. |
| **8. Supplier Authorization Boundary** | 2 | PASSED | Accepts batches with genuine `AuthenticatedSupplierContext`; rejects fabricated or unregistered contexts with `REJECTED_INVALID`. |
| **9. Ingestion Pipeline & Partial Batches** | 2 | PASSED | Ingests partial batches, advances $V_{\text{rep}}$, and dispatches remainder; cascades and unblocks `PendingDependencyQueue`. |
| **10. Historical Completeness Invariant** | 1 | PASSED | Verifies that repairing all missing ranges preserves `hasUnverifiedHistory === true` and `FRONTIER_AGREEMENT_ONLY`, NEVER falsely reporting `IDENTICAL`. |
| **11. Supplier Selection Algorithm** | 4 | PASSED | Prefers author when eligible; falls back to minimum SRTT; enforces concurrency limit of 2 per peer; fails over on peer disconnect. |
| **12. Adversarial Stale-Response & Correlation Safety** | 8 | PASSED | 1. Epoch transition followed by delayed old-epoch OPERATION_BATCH rejected before ingestion.<br/>2. Epoch transition followed by delayed old-epoch REPAIR_REJECTED cannot trigger retries or mutate state.<br/>3. Response for an expired request rejected before ingestion.<br/>4. Duplicate response after successful completion rejected before ingestion.<br/>5. Response with mismatched session ID rejected before ingestion.<br/>6. Response with mismatched epoch rejected before ingestion.<br/>7. Response with mismatched supplier or generation rejected before ingestion.<br/>8. Response with unrelated origin or sequence range rejected before ingestion. |
| **13. Advisory Supplier Knowledge Trust Boundary** | 1 | PASSED | Proves optimistic advertised vectors cannot bypass inbound response validation or supplier authorization. |
| **14. Global Retry Budget & Concurrency Invariants** | 3 | PASSED | Proves switching suppliers consumes the single finite retry budget; `RATE_LIMITED` and `BUSY` consume retry budget; simultaneous timeout/disconnect/rejection cannot schedule duplicate retries. |
| **15. Partial Batch & Overlapping Correlation Safety** | 2 | PASSED | Partial response followed by duplicate response, delayed response, and concurrent gap update; response cannot advance sequence progress beyond requested range. |

### 4.2. Monorepo Test Results
- **`@syncplay/sync-engine`:** **326 passed** across 20 files (0 failures).
- **`@syncplay/protocol`:** **273 passed** across 4 files (0 failures).
- **`@syncplay/peer-mesh`:** **101 passed** across 6 files (0 failures).
- **`@syncplay/control-events`:** **49 passed** across 6 files (0 failures).
- **`@syncplay/bootstrap-server`:** **91 passed** across 3 files (0 failures).
- **Total Monorepo Tests:** **840 passed across 39 files with 0 failures**.
- **Build & Typecheck:** `tsc` and Vite production build passed cleanly across all packages.

---

## 5. Scope Boundary Compliance

| Milestone | Scope Definition | Status in M3 |
|---|---|---|
| **Phase 7.4 M1** | Protocol Schemas & Serialization | FROZEN & VERIFIED (`309b808`) |
| **Phase 7.4 M2** | Digest Generation & Comparison | FROZEN & VERIFIED (`2d5e9f3`) |
| **Phase 7.4 M3** | Repair Request Pipeline (Requester Lifecycle) | **COMPLETED & AUDITED** (`0e1b678`) |
| **Phase 7.4 M4** | Supplier Range Servicing (`serveRepairRequest`) | **UNTOUCHED / SCHEDULED FOR M4** |
| **Phase 7.4 M5** | WebRTC DataChannel Transport Integration | **UNTOUCHED / SCHEDULED FOR M5** |
| **Phase 7.5** | Snapshot Recovery & Catch-Up | **UNTOUCHED / SCHEDULED FOR PHASE 7.5** |
