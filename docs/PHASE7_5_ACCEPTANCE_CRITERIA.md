# Phase 7.5 — Fault Tolerance & State Recovery: Acceptance Criteria

**Status:** APPROVED ARCHITECTURAL DESIGN (RE-AUDITED)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `3a3d9e5`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Overview and Verification Framework

This document establishes the binding acceptance criteria for Phase 7.5: Fault Tolerance & State Recovery. Every criterion is specified with concrete, measurable pass/fail thresholds and references the corresponding architectural invariant.

---

## 2. Compaction Horizon and Checkpoint Retention (AC-COMPACT)

### AC-COMPACT-1: Autonomous Frontier Compaction
- **Condition:** Operation logs are compacted up to the membership-scoped safe frontier ($V_{\text{epoch\_safe}} = \min_{m \in \mathcal{M}} V_m$) or up to local ring buffer capacity ($K=256$).
- **Pass Threshold:** Memory consumption of `OrderedOperationLog` decreases proportionally; the architecture does NOT assume a globally agreed consensus frontier exists.

### AC-COMPACT-2: Checkpoint Preservation
- **Condition:** Pruning operations MUST NOT delete the compacted checkpoint entries for the pruned origins.
- **Pass Threshold:** `log.getCompactedCheckpoints()` returns a complete, contiguous record of the highest sequence and operation digest for every origin pruned.

### AC-COMPACT-3: Horizon Rejection via Anti-Entropy
- **Condition:** An incoming Phase 7.4 `REPAIR_REQUEST` requesting a sequence range below the compaction frontier is evaluated by `RepairSupplierService`.
- **Pass Threshold:** Supplier immediately returns `REPAIR_REJECTED` with reason `OUTSIDE_HORIZON`. No operations are retrieved or returned. Requester invokes `onHistoryExhausted`.

---

## 3. Snapshot Eligibility and Validation (AC-ELIGIBLE)

### AC-ELIGIBLE-1: Strict Vector Domination and Checkpoint Compatibility
- **Condition:** Candidate snapshot $S$ is evaluated against local state.
- **Pass Threshold:** $S$ is ELIGIBLE if and only if:
  1. $\forall p \in \text{origins}(V_L): V_S[p] \ge V_L[p]$.
  2. For every common sequence $k = \min(V_L[p], V_S[p])$, $CP_S(p, k).\text{fingerprint} == CP_L(p, k).\text{fingerprint}$.
  3. $S.\text{membershipEpoch} \le \text{currentEpoch}$.
  4. Bit-exact SHA-256 match over canonical RFC 8785 JSON representation.

### AC-ELIGIBLE-2: Formal Compatibility Cases (A through E)
- **Case A (Valid Dominating Snapshot):** $V_S \ge V_L$, matching common checkpoints.
  - *Pass Threshold:* Evaluates to **ELIGIBLE**.
- **Case B (Incomparable Snapshots):** $V_S[A] < V_L[A]$ while $V_S[B] > V_L[B]$.
  - *Pass Threshold:* Evaluates to **DEGRADED**. Arbitrary tie-breaking is strictly prevented.
- **Case C (Conflicting Checkpoint):** $V_S \ge V_L$, but fingerprints at common sequence differ.
  - *Pass Threshold:* Evaluates to **REJECTED** (history fork detected).
- **Case D (Unavailable Checkpoint):** $V_S \ge V_L$, but common checkpoints between local frontier and snapshot base are missing.
  - *Pass Threshold:* Evaluates to **REQUIRES_HISTORY_REPAIR**. If unrepairable, transitions to **DEGRADED**.
- **Case E (Missing Frontier Timestamp):** Snapshot has origin sequence $>0$ but lacks corresponding `originFrontierTimestamps` entry.
  - *Pass Threshold:* Evaluates to **REJECTED** (malformed metadata).

---

## 4. Chunked Transfer and Transport Boundary (AC-XFER)

### AC-XFER-1: Dynamic SCTP Wire Sizing
- **Condition:** `SnapshotTransferManager` prepares chunks for transmission across links with varying negotiated SCTP max message sizes.
- **Pass Threshold:** Chunk size is dynamically derived via `calculateDynamicChunkSize(sctpMaxMessageSize)`. The final complete JSON wire message at `RTCDataChannel.send()` MUST NEVER exceed $\min(\text{sctpLimit}, 65536)$ bytes. Chunks are capped at 32 KB payload under standard limits, and scale down under constrained MTUs.

### AC-XFER-2: Single Active Transfer Invariant
- **Condition:** Multiple snapshot offers arrive while a transfer is pending or active.
- **Pass Threshold:** The engine maintains strictly **1 active transfer** at any time. Secondary offers are held or dropped; concurrent snapshot downloads are prohibited.

### AC-XFER-3: Sliding Window Retransmission
- **Condition:** Transfer operates across simulated links with 10% packet drop.
- **Pass Threshold:** Pipelined window of up to 4 concurrent chunk requests is maintained. Missing chunks are re-requested via `SNAPSHOT_CHUNK_REQUEST` and the complete snapshot is assembled without failure.

### AC-XFER-4: Immediate Abort on Transport Currency Breach
- **Condition:** During transfer, either supplier disconnects, DataChannel closes, or membership epoch increments.
- **Pass Threshold:** Transfer aborts synchronously within 1 tick. All allocated chunk buffers and timers are freed.

---

## 5. Atomic Installation and Quarantine Buffer (AC-INSTALL)

### AC-INSTALL-1: Two-Stage Commit and Rollback
- **Condition:** Snapshot installation executes via `SnapshotInstaller`.
- **Pass Threshold:** Stage 1 (`validateCandidate`) runs pure dry-run validation with zero engine mutation. Stage 2 (`installSnapshotAtomic`) executes synchronously with state backup; any failure triggers complete rollback to pre-install state.

### AC-INSTALL-2: Local Input Fencing
- **Condition:** Local user controls (e.g. `play()`, `seek()`) are invoked while engine is in `RECOVERING` or `RECONCILING` state.
- **Pass Threshold:** Local operations are rejected or locally queued. No network broadcast events are emitted until `ACTIVE` state is reached.

### AC-INSTALL-3: Live Event Quarantine Buffer and Capacity Limit
- **Condition:** Live broadcast events arrive during snapshot transfer.
- **Pass Threshold:** Events are stored in `QuarantineBuffer` (capacity 1,000 operations). If incoming events exceed 1,000, transfer is aborted synchronously with reason `QUARANTINE_OVERFLOW` and engine transitions to `DEGRADED`.

### AC-INSTALL-4: Quarantine Buffer Drain
- **Condition:** Snapshot commits, transitioning engine to `RECONCILING`.
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
