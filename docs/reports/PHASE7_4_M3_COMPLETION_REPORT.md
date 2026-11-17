# Phase 7.4 Milestone M3 Completion Report: Repair Request Pipeline (Requester-Side Lifecycle)

**Milestone:** Phase 7.4 — Milestone M3 (Repair Request Pipeline)  
**Status:** COMPLETED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Milestone M1 Baseline:** `309b808` (amending `5cadc03`)  
**Milestone M2 Baseline:** `2d5e9f3` (docs: `4926c52`)  
**Milestone M3 Implementation Commit:** `b5dc7e8`  
**Date:** 2026-11-18  

---

## 1. Executive Summary

Milestone M3 implements the **Requester-Side Repair Request Pipeline** for the SyncPlay Protocol anti-entropy subsystem.

In strict conformance with the authorized scope and architectural design, M3 implements:

1. **Exact Missing Sequence Range Translation & Slicing:**
   - Translates state digest comparison results (`result.missingFromLocal`) and pending dependency queue deficits (`engine.getPendingGaps()`) into exact contiguous missing sequence ranges.
   - Trims ranges against the local replication frontier ($V_{\text{rep}}[origin]$) to drop already-satisfied sequence prefixes.
   - Slices ranges into contiguous chunks of at most 32 operations (`MAX_OPERATIONS_PER_BATCH = 32`), strictly conforming to `RepairRequestPayloadSchema`.

2. **Unique Request ID Generation (RFC 4122 UUID v4):**
   - Dispatches requests with canonical RFC 4122 UUID v4 `requestId`s (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`), validated by `UUIDv4Schema`.
   - Ensures end-to-end correlation for response matching and timeout cancellation.

3. **Outstanding Request Tracking & Timeouts ($1500\text{ms}$):**
   - Tracks in-flight requests with monotonic timers scheduled at $1500\text{ms}$ (`DEFAULT_REPAIR_REQUEST_TIMEOUT_MS = 1500`).
   - Limits per-peer concurrency to at most 2 active requests (`MAX_CONCURRENT_REQUESTS_PER_PEER = 2`) and room-wide concurrency to at most 16 active requests (`MAX_CONCURRENT_REQUESTS_TOTAL = 16`).

4. **Bounded Retries ($\le 3$) and Exponential Backoff ($1\text{s}, 2\text{s}, 4\text{s}$):**
   - Implements bounded retries ($\le 3$) with exponential backoff delays ($1000\text{ms} \times 2^{\text{retryCount}-1}$).
   - On timeout or failure, blacklists the supplier for the requested range and executes automatic supplier failover.
   - Upon exceeding 3 retries, marks `degraded = true`, `degradedReason = 'MAX_RETRIES_EXCEEDED'`, and triggers `callbacks.onHistoryExhausted`.

5. **Duplicate and Overlapping Request Suppression:**
   - Drops gaps already satisfied locally ($toSeq \le V_{\text{rep}}[origin]$).
   - Suppresses duplicate requests when an identical or enclosing range is already actively in-flight or queued.

6. **Cancellation on Membership Epoch Transitions (`onEpochTransition`):**
   - Cancels all active in-flight request timers and clears pending requests initiated under older epochs.
   - Ensures no obsolete epoch repair requests or timers leak across the epoch boundary.

7. **Comprehensive `REPAIR_REJECTED` Reason Handling:**
   - `OUTSIDE_HORIZON`: Updates supplier snapshot frontier knowledge; fails over to alternative eligible suppliers; triggers `onHistoryExhausted` if no eligible supplier remains.
   - `ORIGIN_UNKNOWN`: Blacklists supplier for this origin; fails over immediately.
   - `RATE_LIMITED` & `BUSY`: Applies backoff delay; fails over to alternative suppliers.
   - `EPOCH_MISMATCH`: Blacklists supplier and fails over.

8. **Preservation of the Phase 7.3-R5-H Trust Boundary:**
   - Inbound `OPERATION_BATCH` delivery strictly passes genuine `AuthenticatedSupplierContext` to `LeaderlessSyncEngine.ingestRepairBatch()`.
   - Fabricated, unregistered, or plain object contexts are rejected with `REJECTED_INVALID`.

9. **Phase 7.3 Admission Pipeline Verification & Partial Batch Slicing:**
   - All repaired operations pass through the existing Phase 7.3 admission gauntlet, enforcing total ordering, Lamport clock monotonicity, and state reducer execution.
   - Automatically unblocks operations buffered in `PendingDependencyQueue` via `drainSatisfied()`.
   - Detects partial batch fulfillment ($V_{\text{rep}}[origin] < toSeq$), advancing progress and immediately re-enqueuing and dispatching the remaining sub-range $[V_{\text{rep}}[origin] + 1, toSeq]$.

10. **Strict Historical Completeness Invariant Preservation:**
    - Completing repairs of known missing ranges advances $V_{\text{rep}}$ to the frontier, but NEVER falsely claims complete historical identity (`IDENTICAL`) when intermediate history remains unverified ($k > 1$ with single tail fingerprints).
    - Preserves `hasUnverifiedHistory === true` and `classification === 'FRONTIER_AGREEMENT_ONLY'` in full accordance with Milestone M2 invariants.

11. **Strict Architectural Scope Boundaries:**
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
- **Milestone M3 Implementation Baseline:** `b5dc7e8`

---

## 3. Detailed Component Architecture

### 3.1. `RepairRequestPipeline` (`packages/sync-engine/src/replication/repair-request-pipeline.ts`)
- Manages the entire requester-side repair lifecycle.
- Holds internal `pendingGaps: PendingGapTask[]`, `inFlightRequests: Map<string, InFlightRepairRequest>`, and `peerKnowledge: Map<string, PeerReplicationKnowledge>`.
- Generates RFC 4122 UUID v4 `requestId` via `generateUUIDv4()`.
- Implements supplier eligibility filtering and ranking:
  1. Connection generation active in `engine.getActivePeerGenerations()` ($\ge 1$).
  2. Peer replication vector holds target sequence ($V_S[origin] \ge toSeq$).
  3. Range is uncompacted ($fromSeq > \text{snapshotFrontier}[origin]$).
  4. Concurrency limit ($\le 2$ active requests per peer; $\le 16$ room total).
  5. Ranking: author preference first, then minimum SRTT (via `rttProvider`).

### 3.2. `LeaderlessSyncEngine` Additions (`packages/sync-engine/src/engine/leaderless-sync-engine.ts`)
- `getPendingGaps(): MissingSequenceRange[]`: Delegates to `this.pendingQueue.getMissingRanges(this.replicationVector)` to compute sequence ranges required to unblock buffered operations.
- `localPeerId`: Accesses `this.config.localPeerId`.
- `sessionId`: Accesses `this.config.sessionId`.
- `getActivePeerGenerations(): ReadonlyMap<string, number>`: Exposes copy of active peer generations map.

### 3.3. `AntiEntropyManager` Integration (`packages/sync-engine/src/replication/anti-entropy-manager.ts`)
- Delegates requester-side repair handling to `this.pipeline`:
  - `processRemoteDigest`: records remote peer knowledge and schedules missing ranges from `result.missingFromLocal`.
  - `reconcileGaps`: collects gaps from `engine.getPendingGaps()` and adds to pipeline.
  - `handleRepairBatch`: correlates request, passes through `engine.ingestRepairBatch()`, and manages partial batch slicing.
  - `handleRepairRejected`: handles all 5 rejection reasons, manages failover and backoff.
  - `onPeerDisconnected`: cleans up tracking and triggers failover for active requests.
  - `onEpochTransition`: cancels older epoch requests and timers.
  - `getStatus`: aggregates pipeline metrics and pending queue size.
  - `serveRepairRequest`: remains throwing `Error('AntiEntropyManager.serveRepairRequest() is scheduled for Phase 7.4 M4')`.

---

## 4. Verification and Test Results

### 4.1. Unit Test Suite (`packages/sync-engine/src/__tests__/repair-request-pipeline.test.ts`)
A dedicated 21-test suite verifies all 10 core lifecycle requirements:

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

### 4.2. Monorepo Test Results
- **`@syncplay/sync-engine`:** 312 tests passed across 20 files (0 failures).
- **`@syncplay/protocol`:** 273 tests passed across 4 files (0 failures).
- **`@syncplay/peer-mesh`:** 101 tests passed across 6 files (0 failures).
- **`@syncplay/control-events`:** 49 tests passed across 6 files (0 failures).
- **`@syncplay/bootstrap-server`:** 91 tests passed across 3 files (0 failures).
- **Total Monorepo Tests:** **826 passed across 39 files with 0 failures**.
- **Build & Typecheck:** `tsc` and Vite production build passed cleanly across all packages.

---

## 5. Scope Boundary Compliance

| Milestone | Scope Definition | Status in M3 |
|---|---|---|
| **Phase 7.4 M1** | Protocol Schemas & Serialization | FROZEN & VERIFIED (`309b808`) |
| **Phase 7.4 M2** | Digest Generation & Comparison | FROZEN & VERIFIED (`2d5e9f3`) |
| **Phase 7.4 M3** | Repair Request Pipeline (Requester Lifecycle) | **COMPLETED** (`b5dc7e8`) |
| **Phase 7.4 M4** | Supplier Range Servicing (`serveRepairRequest`) | **UNTOUCHED / SCHEDULED FOR M4** |
| **Phase 7.4 M5** | WebRTC DataChannel Transport Integration | **UNTOUCHED / SCHEDULED FOR M5** |
| **Phase 7.5** | Snapshot Recovery & Catch-Up | **UNTOUCHED / SCHEDULED FOR PHASE 7.5** |
