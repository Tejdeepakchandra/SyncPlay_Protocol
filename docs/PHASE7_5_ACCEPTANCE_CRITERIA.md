# Phase 7.5 — Fault Tolerance & State Recovery: Acceptance Criteria

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL SAFETY CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `6fddcf9` (Phase 7.5 M3 Implemented & Frozen)  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`), Phase 7.5 M3 (`6fddcf9`)  
**Detailed M4 Acceptance Criteria:** See [PHASE7_5_M4_ACCEPTANCE_CRITERIA.md](./PHASE7_5_M4_ACCEPTANCE_CRITERIA.md)  

---

## 1. Overview and Verification Framework

This document establishes the binding acceptance criteria for Phase 7.5: Fault Tolerance & State Recovery. Every criterion is specified with concrete, measurable pass/fail thresholds and references the corresponding architectural invariant.

---

## 2. Compaction Horizon and Checkpoint Retention (AC-COMPACT)

### AC-COMPACT-1: Mandatory Local Peer Inclusion in $V_{\text{epoch\_safe}}$
- **Condition:** Compaction at $V_{\text{epoch\_safe}}$ is computed over a captured membership view $\mathcal{V} = (E_{\text{local}}, V_{\text{local\_snapshot}}, \mathcal{P})$.
- **Pass Threshold:**
  1. Local peer's replication vector $V_{\text{local}}$ is an explicit member of the GLB formula:
     $$V_{\text{epoch\_safe}}[p] = \min \Big( V_{\text{local}}[p], \min_{(r, gen) \in \mathcal{P}} D_r.\text{replicationVector}[p] \Big)$$
  2. If remote membership $\mathcal{P} = \emptyset$, $V_{\text{epoch\_safe}} = V_{\text{local}}$.
  3. Any origin missing from a peer's vector is treated as sequence $0$ ($V[p] ?? 0$).
  4. The engine MUST NEVER compact beyond its own applied sequence frontier.

### AC-COMPACT-2: Pre-Commit Revalidation and Abort
- **Condition:** Pre-commit revalidation immediately before `compactAtFrontier()` commits.
- **Pass Threshold:**
  1. $V_{\text{epoch\_safe}}[p] \le \text{engine.replicationVector}[p]$ is verified for all origins.
  2. Active epoch matches $\mathcal{V}.\text{epoch}$ and connected peer set matches $\mathcal{P}$ bit-exact.
  3. If any peer disconnected, generation changed, epoch incremented, or local vector rolled back, the compaction attempt is aborted synchronously with zero operations pruned.

### AC-COMPACT-3: Checkpoint Preservation
- **Condition:** Pruning operations MUST NOT delete compacted checkpoint entries for pruned origins.
- **Pass Threshold:** `log.getCompactedCheckpoints()` returns a complete, contiguous record of the highest sequence and operation digest for every origin pruned.

### AC-COMPACT-4: Horizon Rejection via Anti-Entropy
- **Condition:** An incoming Phase 7.4 `REPAIR_REQUEST` requesting a sequence range below the compaction frontier is evaluated by `RepairSupplierService`.
- **Pass Threshold:** Supplier immediately returns `REPAIR_REJECTED` with reason `OUTSIDE_HORIZON`. Requester dispatches `onHistoryExhausted`.

---

## 3. Snapshot Eligibility and Validation (AC-ELIGIBLE)

### AC-ELIGIBLE-1: Complete Prefix Checkpoint Verification (Four Outcomes)
- **Condition:** Candidate snapshot $S$ is evaluated against local state across common prefix $[1, \min(V_L[p], V_S[p])]$.
- **Pass Threshold:** Exactly one of the four outcomes is returned:
  1. **`HISTORY_VERIFIED`:** All retained common sequence checkpoints match bit-exact, and zero intermediate gaps exist. Evaluates to **ELIGIBLE**.
  2. **`HISTORY_CONFLICT`:** Any common sequence fingerprint differs. Evaluates to **REJECTED** (history fork).
  3. **`HISTORY_INCOMPLETE`:** Gaps exist, but suppliers retain missing ranges in active logs. Evaluates to **REQUIRES_HISTORY_REPAIR**.
  4. **`HISTORY_UNAVAILABLE`:** Gaps exist, and proof is permanently outside every retention horizon across the mesh. Evaluates to **DEGRADED**.
- Matching only the final operation at sequence $k$ is strictly prohibited.

### AC-ELIGIBLE-2: Exact Lamport Invariant Reinstated
- **Condition:** Validation of `maxLogicalTimestamp`.
- **Pass Threshold:** Must hold bit-exact:
  $$S.\text{maxLogicalTimestamp} \equiv \max_{p \in \text{origins}(V_S)} S.\text{originFrontierTimestamps}[p]$$

### AC-ELIGIBLE-3: Mechanical Older-Epoch Non-Supersession
- **Condition:** Snapshot carries `membershipEpoch < currentEpoch`.
- **Pass Threshold:** Accepted ONLY IF:
  1. $S.\text{sessionId} === \text{engine.sessionId}$.
  2. Transport envelope carries current epoch $E_{\text{local}}$ and active generation.
  3. Supplier certifies in current epoch that $S$ is its active base snapshot.
  4. $\forall op \in \text{localOperationLog}: op.\text{membershipEpoch} > S.\text{membershipEpoch} \implies op.\text{originSequenceNumber} \le S.V_S[op.\text{originPeerId}]$.
  5. If unprovable before M5, classified as a historical candidate that halts in `RECOVERING` or transitions to `DEGRADED`.

---

## 4. Chunked Transfer and Transport Boundary (AC-XFER)

### AC-XFER-1: Normative Iterative Send Boundary Measurement
- **Condition:** `SnapshotTransferManager` sends chunks over WebRTC DataChannels.
- **Pass Threshold:**
  1. Complete message is formatted and serialized to `wireJson`.
  2. Exact UTF-8 byte length is measured via `TextEncoder`.
  3. If `byteLength > effectiveLimit`, binary payload is iteratively reduced.
  4. Exact measured string is passed unchanged to `PeerManager.sendSerialized()`.
  5. If binary chunk drops below 256 bytes without fitting, transfer safely aborts with `TRANSPORT_LIMIT_EXCEEDED` $\to$ `DEGRADED`.

### AC-XFER-2: Single Active Transfer Invariant
- **Condition:** Multiple snapshot offers arrive while a transfer is in progress.
- **Pass Threshold:** The engine maintains strictly **1 active transfer** at any time.

### AC-XFER-3: Sliding Window Retransmission
- **Condition:** Transfer operates across simulated links with 10% packet drop.
- **Pass Threshold:** Pipelined window of up to 4 concurrent chunk requests is maintained. Missing chunks are re-requested via `SNAPSHOT_CHUNK_REQUEST` and the complete snapshot is assembled without failure.

### AC-XFER-4: Immediate Abort on Transport Currency Breach
- **Condition:** During transfer, either supplier disconnects, DataChannel closes, or membership epoch increments.
- **Pass Threshold:** Transfer aborts synchronously within 1 tick. All allocated chunk buffers and timers are freed.

---

## 5. Atomic Installation and Quarantine Buffer (AC-INSTALL)

### AC-INSTALL-1: Staged Immutable Instance Replacement across 14 Components
- **Condition:** Snapshot installation executes via `SnapshotInstaller`.
- **Pass Threshold:**
  1. Stage 1 (`validateCandidate`) runs dry-run checks with zero engine mutation.
  2. Stage 2 constructs new `OrderedOperationLog`, state, and replication vectors out-of-band.
  3. Stage 3 performs a single synchronous run-to-completion reference swap across all 14 mutable components.
  4. External listeners are NOT notified during staging or swap.

### AC-INSTALL-2: Deferred Listener Dispatch
- **Condition:** Snapshot commit completes and engine transitions to `RECONCILING`.
- **Pass Threshold:** External `stateChangeListeners` are notified ONLY AFTER `QuarantineBuffer` drains and engine reaches `ACTIVE` state. Listeners never observe intermediate recovery states.

### AC-INSTALL-3: Live Event Quarantine Buffer and Capacity Limit
- **Condition:** Live broadcast events arrive during snapshot transfer.
- **Pass Threshold:** Events are stored in `QuarantineBuffer` (capacity 1,000 operations). If incoming events exceed 1,000, transfer is aborted synchronously with reason `QUARANTINE_OVERFLOW` and engine transitions to `DEGRADED`.

### AC-INSTALL-4: Quarantine Buffer Drain
- **Condition:** Engine is in `RECONCILING`.
- **Pass Threshold:**
  1. Operations with origin sequence $\le V_S[\text{origin}]$ are discarded as already subsumed.
  2. Operations with origin sequence $> V_S[\text{origin}]$ are causally validated and applied in strict total order.
  3. Engine transitions to `ACTIVE`.

---

## 6. Monotonicity and Clock Correctness (AC-CLOCK)

### AC-CLOCK-1: Exact Lamport Reseed without $+1$ Drift
- **Condition:** Snapshot with `maxLogicalTimestamp = T_snap` is installed when local clock is `T_local`.
- **Pass Threshold:** New local Lamport clock is set to $\max(T_{\text{local}}, T_{\text{snap}})$ strictly without adding $+1$.

### AC-CLOCK-2: Monotonic State Version Advancement
- **Condition:** Post-installation state version is evaluated.
- **Pass Threshold:** Local state version is incremented to $V_{\text{local}} + 1$. Remote version is stored in metadata but does not replace local sequence.

---

## 7. System Regression Baseline (AC-REGRESSION)

### AC-REGRESSION-1: Zero Monorepo Test Regressions
- **Condition:** Full vitest execution across all monorepo workspaces.
- **Pass Threshold:** All 967 existing tests across 44 files pass cleanly (100% pass rate).

### AC-REGRESSION-2: Zero Build Warnings and Type Errors
- **Condition:** Full monorepo TypeScript compilation (`tsc --noEmit`).
- **Pass Threshold:** Exit code 0, 0 compiler errors, 0 type errors across all packages.
