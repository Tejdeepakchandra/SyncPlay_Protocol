# Phase 7.4 Milestone M2 Completion Report: Digest Generation and Comparison

**Milestone:** Phase 7.4 — Milestone M2 (Digest Generation & Comparison)  
**Status:** COMPLETED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Parent / M1 Implementation Baseline:** `309b808` (amending `5cadc03`)  
**M2 Initial Commit:** `7049ca6`  
**M2 Targeted Correction Commit:** `022d430` (docs: `5589be8`)  
**M2 Historical Completeness Commit:** `bded62d` (docs: `e71706a`)  
**M2 Aggregate Classification & History Invariant Commit:** `2d5e9f3`  
**Date:** 2026-11-18  

---

## 1. Executive Summary

Milestone M2 implements decentralized state digest generation, pure deterministic replication comparison, and anti-entropy scheduling for the SyncPlay Protocol.

Following the final aggregate classification and history verification review, M2 strictly standardizes:

1. **Rigorous Aggregate Classification Semantics:**
   - **`IDENTICAL` Invariant:** The aggregate result **MUST NOT** be `'IDENTICAL'` whenever `hasUnverifiedHistory === true`, or when any origin has missing ranges, outside-horizon uncertainty, or unresolved conditions.
   - **Explicit `FRONTIER_AGREEMENT_ONLY`:** When replication vectors match and tail fingerprints match across all origins but intermediate historical equality cannot be proven ($k > 1$ with single tail fingerprints), the aggregate classification is explicitly `'FRONTIER_AGREEMENT_ONLY'`, **NEVER** `'IDENTICAL'`.
   - **Can the aggregate classification ever report `IDENTICAL` while `hasUnverifiedHistory === true`? The answer is strictly NO.**

2. **Consistent History Verification & Invariant:**
   - **1-to-1 Mapping Invariant:** An origin appears in `unverifiedHistoryOrigins` **if and only if** `historyVerified === false`.
   - **Confirmed Forks:** An origin with divergent tail fingerprints (`CONFLICT_FORK`) has `historyVerified = false` (appearing in `unverifiedHistoryOrigins`) and `forkDetected = true` (appearing in `conflictingOrigins`). Confirmed forks retain the highest classification precedence (`DIVERGENT_FORK`).

3. **Property A vs. Property B Separation:**
   - **Property A (`frontierAgreement: boolean`):** The operation at the replication frontier ($k = V_{\text{rep}}[p]$) has matching fingerprints (or both at sequence 0).
   - **Property B (`historyVerified: boolean`):** Preceding operation history has been independently verified to match. Matching frontier fingerprints establish Property A, **NOT** Property B.
   - For single tail fingerprints:
     - At $k = 0$: History is empty; `historyVerified = true`.
     - At $k = 1$: Operation 1 is frontier with no predecessors; `historyVerified = true`.
     - At $k > 1$: Intermediate operations $1..k-1$ are unverified; `historyVerified = false`.

4. **Exact Snapshot Frontier Semantics:** `snapshotFrontier[origin]` is the highest contiguous sequence for that origin incorporated into the compacted base snapshot (sequences $\le \text{snapshotFrontier}$ are compacted out of the active log).
5. **Reconciled Rate-Limiting Policy:** Token-bucket capacity 2, refill rate 1 token per 1000ms ($1\text{ digest/sec}$), strictly shared across periodic and reactive triggers.
6. **Preserved Trust Boundaries:** Digest payloads are untrusted remote claims. Digest comparison does not authorize repair suppliers or authenticate connection generations.

Milestones M3 (Repair Request Pipeline), M4 (Supplier Servicing), M5 (Transport Integration), and Phase 7.5 (Snapshot Recovery) remain strictly unimplemented.

---

## 2. Baselines and Commit Lineage

- **Approved Phase 7.4 Architecture:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`
- **Approved Phase 7.2-R9 Causal Baseline:** `1825de1`
- **Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`
- **Milestone M1 Baseline:** Commit `309b808` (amending `5cadc03`)
- **Milestone M2 Initial Implementation:** Commit `7049ca6`
- **Milestone M2 Targeted Corrections:** Commit `022d430` (docs: `5589be8`)
- **Milestone M2 Historical Completeness Correction:** Commit `bded62d` (docs: `e71706a`)
- **Milestone M2 Aggregate Classification & History Invariant Correction:** Commit `2d5e9f3`

---

## 3. Detailed Comparison & History Verification Invariants

### 3.1 Status & Classification Mapping Table

| Sequence & Fingerprint Condition | Origin `status` | `frontierAgreement` | `historyVerified` | In `unverifiedHistoryOrigins` | In `conflictingOrigins` | Aggregate `classification` (if sole origin) |
|---|---|---|---|---|---|---|
| $localSeq = remoteSeq = 0$ | `'IDENTICAL'` | `true` | `true` | No | No | `'IDENTICAL'` |
| $localSeq = remoteSeq = 1$, $localFp = remoteFp$ | `'IDENTICAL'` | `true` | `true` | No | No | `'IDENTICAL'` |
| $localSeq = remoteSeq > 1$, $localFp = remoteFp$ | `'FRONTIER_IDENTICAL'` | `true` | `false` | **Yes** | No | **`'FRONTIER_AGREEMENT_ONLY'`** |
| $localSeq = remoteSeq \ge 1$, $localFp \ne remoteFp$ | `'CONFLICT_FORK'` | `false` | `false` | **Yes** | **Yes** | **`'DIVERGENT_FORK'`** |
| $localSeq = remoteSeq \ge 1$, missing fingerprint | `'HISTORICAL_EQUALITY_UNPROVABLE'` | `false` | `false` | **Yes** | No | **`'INCONCLUSIVE'`** |
| $localSeq < remoteSeq$, within horizon | `'REMOTE_AHEAD'` / `'REMOTE_ONLY'` | `false` | `false` | **Yes** | No | **`'REMOTE_DOMINATES'`** |
| $localSeq > remoteSeq$, within horizon | `'LOCAL_AHEAD'` / `'LOCAL_ONLY'` | `false` | `false` | **Yes** | No | **`'LOCAL_DOMINATES'`** |
| Missing sequence $\le snapshotFrontier$ | `'OUTSIDE_REMOTE_HORIZON'` / `'OUTSIDE_LOCAL_HORIZON'` | `false` | `false` | **Yes** | No | **`'REQUIRES_SNAPSHOT_RECOVERY'`** |

### 3.2 Precedence Order for Aggregate Classification

1. **`DIVERGENT_FORK`:** Assigned if `forkDetected === true` (`conflictingOrigins.length > 0`). Confirmed frontier forks take absolute highest precedence.
2. **`REQUIRES_SNAPSHOT_RECOVERY`:** Assigned if `requiresSnapshotRecovery === true` (needed sequences fall at or below compacted `snapshotFrontier`).
3. **`INCONCLUSIVE`:** Assigned if `unprovableOrigins.length > 0` with no sequence gaps.
4. **Matching Vector Sequences (`missingFromLocal.length === 0 && missingFromRemote.length === 0`):**
   - If `hasUnverifiedHistory === true`: **`'FRONTIER_AGREEMENT_ONLY'`**.
   - If `hasUnverifiedHistory === false`: **`'IDENTICAL'`** (all origins have verified history: $k = 0$ or $k = 1$ with matching fp).
5. **`LOCAL_DOMINATES`:** Assigned if local holds a strict superset of sequences (`missingFromLocal.length === 0 && missingFromRemote.length > 0`).
6. **`REMOTE_DOMINATES`:** Assigned if remote holds a strict superset of sequences (`missingFromLocal.length > 0 && missingFromRemote.length === 0`).
7. **`CONCURRENT_INCOMPARABLE`:** Assigned if progress diverged concurrently across multiple origins.

### 3.3 Clarification of Phase 7.3 Collision Detection Capabilities

1. **Detection Upon Admission:** Phase 7.3 detects conflicting origin/sequence operations when conflicting operations are *actually received and admitted* for comparison via the `originSeqIndex` and exact Lamport timestamp checks in `OrderedOperationLog`.
2. **Unseen Historical Conflicts:** Phase 7.3 *cannot* discover an unseen conflicting operation merely because two peers advertise matching tail fingerprints in their state digests.
3. **No Intermediate Fork Elimination:** Matching tail fingerprints do **not** eliminate the possibility of an intermediate historical fork.
4. **Mandatory M3 Design Constraint:** Milestone M3 (Repair Request Pipeline) must **not** treat matching tail fingerprints or `FRONTIER_AGREEMENT_ONLY` as proof that all preceding operations are identical. Anti-entropy repair pipelines must account for unverified intermediate history and rely on Phase 7.3 admission gates when ingesting repaired batches.

---

## 4. Verification and Test Results

### 4.1 Regression & Completeness Test Suites

The test suite in `packages/sync-engine/src/__tests__/replication-comparator.test.ts` (32 tests passed) explicitly proves all required behaviors:

1. **Equal vectors + matching tail fingerprints at sequence 12:**
   - Evaluates to `classification: 'FRONTIER_AGREEMENT_ONLY'`, `classification !== 'IDENTICAL'`.
   - `detail.frontierAgreement = true`, `detail.historyVerified = false`, `detail.status = 'FRONTIER_IDENTICAL'`.
   - `hasUnverifiedHistory = true`, `unverifiedHistoryOrigins = ['peer-A']`.
2. **Equal vectors + matching tail fingerprints at sequence 1:**
   - Evaluates to `classification: 'IDENTICAL'`.
   - `detail.frontierAgreement = true`, `detail.historyVerified = true`, `detail.status = 'IDENTICAL'`.
   - `hasUnverifiedHistory = false`, `unverifiedHistoryOrigins = []`.
3. **Equal vectors + matching tail fingerprints at sequence 0 (empty):**
   - Evaluates to `classification: 'IDENTICAL'`.
   - `hasUnverifiedHistory = false`, `unverifiedHistoryOrigins = []`.
4. **Equal vectors + different tail fingerprints:**
   - Evaluates to `classification: 'DIVERGENT_FORK'`, `forkDetected = true`.
   - `conflictingOrigins = ['peer-A']`, `unverifiedHistoryOrigins = ['peer-A']`, `hasUnverifiedHistory = true`.
5. **Multi-origin comparison with verified frontier + unverified history:**
   - Origin A ($k = 1$, verified) + Origin B ($k = 5$, unverified history).
   - Evaluates to `classification: 'FRONTIER_AGREEMENT_ONLY'`, `classification !== 'IDENTICAL'`.
   - `hasUnverifiedHistory = true`, `unverifiedHistoryOrigins = ['peer-B']`.
6. **Mixed fork + unverified history + ordinary missing range:**
   - Origin A (fork, $k=5$) + Origin B (frontier agreement, $k=10$) + Origin C (ordinary missing range, 2 vs 8).
   - Evaluates to `classification: 'DIVERGENT_FORK'`, `forkDetected = true`.
   - `conflictingOrigins = ['peer-A']`.
   - `unverifiedHistoryOrigins = ['peer-A', 'peer-B', 'peer-C']`, `hasUnverifiedHistory = true`.
   - `missingFromLocal` accurately retains `[{ originPeerId: 'peer-C', fromSequence: 3, toSequence: 8 }]`.
7. **Missing fingerprints and outside-horizon conditions:**
   - Missing fingerprint: `classification: 'INCONCLUSIVE'`, `classification !== 'IDENTICAL'`, `hasUnverifiedHistory = true`.
   - Outside horizon: `classification: 'REQUIRES_SNAPSHOT_RECOVERY'`, `classification !== 'IDENTICAL'`, `hasUnverifiedHistory = true`.
8. **Public caller invariant regression:**
   - Confirms callers (`engine.compareRemoteDigest()`, `manager.processRemoteDigest()`) receive `FRONTIER_AGREEMENT_ONLY` for $k > 1$ matching tail fingerprints.
   - Proves aggregate classification can **NEVER** be `'IDENTICAL'` when `hasUnverifiedHistory === true`.

### 4.2 Full Monorepo Test Totals

```
> npm test

Packages:
- @syncplay/control-events:   6 test files,  49 passed (49 total)
- @syncplay/peer-mesh:        6 test files, 101 passed (101 total)
- @syncplay/protocol:         4 test files, 273 passed (273 total)
- @syncplay/sync-engine:     19 test files, 291 passed (291 total)  [+50 tests from M1]
- @syncplay/bootstrap-server: 3 test files,  91 passed (91 total)

Total Monorepo Tests: 805 tests passed across 38 test files (0 failures).
```

### 4.3 Monorepo Build Results

- `@syncplay/control-events`: `tsc` clean (0 errors)
- `@syncplay/peer-mesh`: `tsc` clean (0 errors)
- `@syncplay/protocol`: `tsc` clean (0 errors)
- `@syncplay/sync-engine`: `tsc` clean (0 errors)
- `@syncplay/bootstrap-server`: `tsc` clean (0 errors)
- `@syncplay/demo`: Vite production build passed (175 modules transformed, bundle size 202.72 kB)

---

## 5. Explicit Confirmation of Unimplemented Subsystems

The following subsystems remain **STRICTLY UNIMPLEMENTED**:
1. **M3 — Repair Request Pipeline:** No `REPAIR_REQUEST` dispatching, no repair timeout tracking, no retry backoff, and no repair response ingestion pipeline.
2. **M4 — Supplier Servicing:** No supplier log range query servicing, no `OPERATION_BATCH` slicing or transmission, and no supplier failover.
3. **M5 — Transport Integration:** No WebRTC DataChannel transport adapter or message size negotiation.
4. **Phase 7.5 — Snapshot Recovery:** No snapshot reset or full state catch-up automation.

---

**PHASE 7.4 M2 — HISTORICAL COMPLETENESS CORRECTION COMPLETE. AWAITING FINAL ARCHITECTURAL ACCEPTANCE AND EXPLICIT AUTHORIZATION FOR M3.**
