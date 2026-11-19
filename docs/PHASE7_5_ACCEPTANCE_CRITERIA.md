# Phase 7.5 — Fault Tolerance & State Recovery: Acceptance Criteria

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `30b8e5f`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Overview and Verification Framework

This document establishes the binding acceptance criteria for Phase 7.5: Fault Tolerance & State Recovery. Every criterion is specified with concrete, measurable pass/fail thresholds and references the corresponding architectural invariant.

---

## 2. Compaction Horizon and Checkpoint Retention (AC-COMPACT)

### AC-COMPACT-1: Captured View Compaction & Pre-Commit Revalidation
- **Condition:** Compaction at $V_{\text{epoch\_safe}}$ is computed over a captured membership view $\mathcal{V} = (E_{\text{local}}, \mathcal{P})$.
- **Pass Threshold:**
  1. A current, authenticated `STATE_DIGEST` is required from EVERY peer in $\mathcal{P}$. If any peer lacks a digest, compaction at $V_{\text{epoch\_safe}}$ is held.
  2. Immediately before commit, $\mathcal{V}$ is revalidated. If epoch, membership set, or generation changes, the compaction attempt is aborted synchronously.
  3. The architecture explicitly does NOT claim that $V_{\text{epoch\_safe}}$ protects disconnected or partitioned peers.

### AC-COMPACT-2: Checkpoint Preservation
- **Condition:** Pruning operations MUST NOT delete compacted checkpoint entries for pruned origins.
- **Pass Threshold:** `log.getCompactedCheckpoints()` returns a complete, contiguous record of the highest sequence and operation digest for every origin pruned.

### AC-COMPACT-3: Horizon Rejection via Anti-Entropy
- **Condition:** An incoming Phase 7.4 `REPAIR_REQUEST` requesting a sequence range below the compaction frontier is evaluated by `RepairSupplierService`.
- **Pass Threshold:** Supplier immediately returns `REPAIR_REJECTED` with reason `OUTSIDE_HORIZON`. Requester dispatches `onHistoryExhausted`.

---

## 3. Snapshot Eligibility and Validation (AC-ELIGIBLE)

### AC-ELIGIBLE-1: Complete Prefix Checkpoint Verification
- **Condition:** Candidate snapshot $S$ is evaluated against local state.
- **Pass Threshold:** $S$ is ELIGIBLE if and only if:
  1. $\forall p \in \text{origins}(V_L): V_S[p] \ge V_L[p]$.
  2. For every origin $p$ and every common sequence $i \in [1, \min(V_L[p], V_S[p])]$ retained by either party:
     $$\text{fingerprint}_S(p, i) == \text{fingerprint}_L(p, i)$$
     Matching only the final operation at sequence $k$ is strictly insufficient.
  3. If unverified gaps exist in the common prefix: evaluates to `REQUIRES_HISTORY_REPAIR`. If unrepairable, transitions to `DEGRADED`.
  4. Exact Phase 7.2 Lamport invariant holds: $S.\text{maxLogicalTimestamp} \equiv \max_{p} S.\text{originFrontierTimestamps}[p]$.
  5. Bit-exact SHA-256 match over canonical RFC 8785 JSON representation.

### AC-ELIGIBLE-2: Formal Compatibility Cases (A through E)
- **Case A (Valid Dominating Snapshot):** $V_S \ge V_L$, complete common prefix checkpoints match.
  - *Pass Threshold:* Evaluates to **ELIGIBLE**.
- **Case B (Incomparable Snapshots):** $V_S[A] < V_L[A]$ while $V_S[B] > V_L[B]$.
  - *Pass Threshold:* Evaluates to **DEGRADED**. Arbitrary tie-breaking is strictly prevented.
- **Case C (Conflicting Checkpoint):** Fingerprints differ at any sequence in common prefix.
  - *Pass Threshold:* Evaluates to **REJECTED** (history fork detected).
- **Case D (Unavailable Checkpoint):** Checkpoints missing across uncompacted common prefix.
  - *Pass Threshold:* Evaluates to **REQUIRES_HISTORY_REPAIR** $\to$ **DEGRADED**.
- **Case E (Missing Frontier Timestamp):** Origin has sequence $>0$ but lacks frontier timestamp.
  - *Pass Threshold:* Evaluates to **REJECTED** (malformed metadata).

### AC-ELIGIBLE-3: Older-Epoch Snapshot Revalidation
- **Condition:** Snapshot carries `membershipEpoch < currentEpoch`.
- **Pass Threshold:** Accepted ONLY IF: (1) `sessionId` matches, (2) served in a current-epoch transport envelope, (3) certified by supplier in current epoch, and (4) local engine has committed zero newer operations conflicting with or superseding $S$.

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

### AC-INSTALL-1: Staged Immutable Instance Replacement
- **Condition:** Snapshot installation executes via `SnapshotInstaller`.
- **Pass Threshold:**
  1. Stage 1 (`validateCandidate`) runs dry-run checks with zero engine mutation.
  2. Stage 2 constructs new `OrderedOperationLog`, state, and replication vectors out-of-band.
  3. Stage 3 performs a single synchronous run-to-completion reference swap.
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
