# Phase 7.4 Milestone M2 Completion Report: Digest Generation and Comparison

**Milestone:** Phase 7.4 — Milestone M2 (Digest Generation & Comparison)  
**Status:** COMPLETED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Parent / M1 Implementation Baseline:** `309b808` (amending `5cadc03`)  
**M2 Initial Commit:** `7049ca6`  
**M2 Targeted Correction Commit:** `022d430` (docs: `5589be8`)  
**M2 Final Historical Completeness Correction Commit:** `bded62d`  
**Date:** 2026-11-17 / 2026-11-18  

---

## 1. Executive Summary

Milestone M2 implements the decentralized state digest generation, pure deterministic replication comparison, and anti-entropy scheduling subsystem for the SyncPlay Protocol.

Following the final historical completeness correction, M2 strictly standardizes:
1. **Property A vs. Property B Separation:**
   - **Property A (`frontierAgreement: boolean`):** The operation at the current replication frontier has matching fingerprints (or both at sequence 0).
   - **Property B (`historyVerified: boolean`):** Preceding operation history has been independently verified to match. Matching frontier fingerprints establish Property A, NOT Property B.
2. **Exact Status & Completeness Semantics:**
   - For $k = 0$: `status: 'IDENTICAL'`, `frontierAgreement: true`, `historyVerified: true`.
   - For $k = 1$ with matching fingerprint: `status: 'IDENTICAL'`, `frontierAgreement: true`, `historyVerified: true` (operation 1 has no preceding intermediate operations).
   - For $k > 1$ with matching tail fingerprint: `status: 'FRONTIER_IDENTICAL'`, `frontierAgreement: true`, `historyVerified: false` (intermediate history $1..k-1$ is unverified under a single tail fingerprint).
   - `unverifiedHistoryOrigins: ReadonlyArray<string>` records all origins where historical equality is unverified.
   - `hasUnverifiedHistory: boolean` (`unverifiedHistoryOrigins.length > 0`) explicitly prevents callers from falsely assuming complete historical equality when only frontier fingerprints match.
3. **Exact Snapshot Frontier Semantics:** `snapshotFrontier[origin]` is the highest contiguous sequence for that origin already incorporated into the compacted base snapshot (sequences $\le \text{snapshotFrontier}$ are compacted out of the active log).
4. **Reconciled Rate-Limiting Policy:** Token-bucket capacity 2, refill rate 1 token per 1000ms ($1\text{ digest/sec}$), strictly shared across periodic and reactive triggers.
5. **Preserved Trust Boundaries:** Digest payloads are untrusted remote claims. Digest comparison does not authorize repair suppliers or authenticate connection generations.

Milestones M3 (Repair Request Pipeline), M4 (Supplier Servicing), M5 (Transport Integration), and Phase 7.5 (Snapshot Recovery) remain strictly unimplemented.

---

## 2. Baselines and Commit Lineage

- **Approved Phase 7.4 Architecture:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`
- **Approved Phase 7.2-R9 Causal Baseline:** `1825de1`
- **Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`
- **Milestone M1 Baseline:** Commit `309b808` (amending `5cadc03`)
- **Milestone M2 Initial Implementation:** Commit `7049ca6`
- **Milestone M2 Targeted Corrections:** Commit `022d430` (docs: `5589be8`)
- **Milestone M2 Final Historical Completeness Correction:** Commit `bded62d`

---

## 3. Historical Completeness & Phase 7.3 Collision Detection Clarifications

### 3.1 The Historical Completeness Problem

Under Phase 7.4 M1 contracts, the state digest contains at most one operation fingerprint per origin, at sequence $k = V_{\text{rep}}[\text{origin}]$.

Consider the following scenario:

| Sequence | Peer A | Peer B |
|---|---|---|
| 10 | Operation A | Operation A |
| 11 | Operation B | Different operation X |
| 12 | Operation C | Operation C |

Both peers report replication sequence 12 and advertise identical tail fingerprints for Operation C.

If the comparator classified this origin as `IDENTICAL` without qualification, callers would assume historical equality. That assumption is mathematically unsound because intermediate operations ($1..k-1$) are neither chained in a cumulative hash nor Merkle-verified.

### 3.2 Formal Semantic Separation: Property A vs Property B

To resolve this defect without violating frozen M1 wire contracts, M2 introduces two explicit properties:

1. **Property A (`frontierAgreement: boolean`):**
   - The operation at the current replication frontier ($k = V_{\text{rep}}[p]$) has matching SHA-256 fingerprints between local and remote peers (or both frontiers are at sequence 0).
2. **Property B (`historyVerified: boolean`):**
   - The relevant preceding operation history has been independently verified to match.
   - A matching frontier fingerprint establishes Property A, **NOT** Property B.
   - For single tail fingerprints without cumulative hashing:
     - At $k = 0$: History is empty; `historyVerified = true`.
     - At $k = 1$: Operation 1 is the frontier and has no predecessors ($1..0$ is empty); `historyVerified = true`.
     - At $k > 1$: Intermediate operations $1..k-1$ have not been verified; `historyVerified = false`.

### 3.3 Status Classification Mapping

| Sequence & Fingerprint Relationship | `status` | `frontierAgreement` | `historyVerified` | In `unverifiedHistoryOrigins` | In `conflictingOrigins` |
|---|---|---|---|---|---|
| $localSeq = remoteSeq = 0$ | `'IDENTICAL'` | `true` | `true` | No | No |
| $localSeq = remoteSeq = 1$, $localFp = remoteFp$ | `'IDENTICAL'` | `true` | `true` | No | No |
| $localSeq = remoteSeq > 1$, $localFp = remoteFp$ | `'FRONTIER_IDENTICAL'` | `true` | `false` | **Yes** | No |
| $localSeq = remoteSeq \ge 1$, $localFp \ne remoteFp$ | `'CONFLICT_FORK'` | `false` | `false` | No | **Yes** |
| $localSeq = remoteSeq \ge 1$, missing fingerprint | `'HISTORICAL_EQUALITY_UNPROVABLE'` | `false` | `false` | **Yes** | No |
| $localSeq < remoteSeq$, within horizon | `'REMOTE_AHEAD'` / `'REMOTE_ONLY'` | `false` | `false` | No (tracked as gap) | No |
| $localSeq > remoteSeq$, within horizon | `'LOCAL_AHEAD'` / `'LOCAL_ONLY'` | `false` | `false` | No (tracked as gap) | No |
| Missing sequence $\le snapshotFrontier$ | `'OUTSIDE_REMOTE_HORIZON'` / `'OUTSIDE_LOCAL_HORIZON'` | `false` | `false` | No (tracked as snapshot req) | No |

### 3.4 Clarification of Phase 7.3 Collision Detection Capabilities

1. **Detection Upon Admission:** Phase 7.3 detects conflicting origin/sequence operations when conflicting operations are *actually received and admitted* for comparison via the `originSeqIndex` and exact Lamport timestamp checks in `OrderedOperationLog`.
2. **Unseen Historical Conflicts:** Phase 7.3 *cannot* discover an unseen conflicting operation merely because two peers advertise matching tail fingerprints in their state digests.
3. **No Intermediate Fork Elimination:** Matching tail fingerprints do **not** eliminate the possibility of an intermediate historical fork.
4. **Mandatory M3 Design Constraint:** Milestone M3 (Repair Request Pipeline) must **not** treat matching tail fingerprints as proof that all preceding operations are identical. Anti-entropy repair pipelines must account for unverified intermediate history and rely on Phase 7.3 admission gates when ingesting repaired batches.

---

## 4. Verification and Test Results

### 4.1 Regression & Completeness Test Suites

The test suite in `packages/sync-engine/src/__tests__/replication-comparator.test.ts` (31 tests passed) explicitly proves all 7 required completeness behaviors:

1. **Equal vectors + equal tail fingerprints do not claim complete historical equality:**
   - At sequence 12 with matching tail fingerprints, `classification = 'IDENTICAL'`, `detail.frontierAgreement = true`, `detail.historyVerified = false`, `detail.status = 'FRONTIER_IDENTICAL'`, `hasUnverifiedHistory = true`, and `unverifiedHistoryOrigins = ['peer-A']`.
2. **Equal vectors + different tail fingerprints produce a confirmed frontier conflict:**
   - At sequence 5 with mismatched fingerprints, `classification = 'DIVERGENT_FORK'`, `forkDetected = true`, `conflictingOrigins = ['peer-A']`, `detail.status = 'CONFLICT_FORK'`, and `frontierAgreement = false`.
3. **Equal vectors + missing fingerprints remain historically unverified:**
   - At sequence 5 with missing fingerprint on one/both sides, `classification = 'INCONCLUSIVE'`, `unprovableOrigins = ['peer-A']`, `unverifiedHistoryOrigins = ['peer-A']`, and `hasUnverifiedHistory = true`.
4. **A peer missing an operation range remains correctly classified as behind, while separate historical uncertainty is preserved:**
   - Origin A has lag ($2$ vs $7$, `REMOTE_DOMINATES`), while Origin B has $k = 5$ with matching tail fingerprint (`FRONTIER_IDENTICAL`). Result: `classification = 'REMOTE_DOMINATES'` while `hasUnverifiedHistory = true` (`unverifiedHistoryOrigins = ['peer-B']`).
5. **A multi-origin comparison with one verified frontier and another unverified history preserves that uncertainty:**
   - Origin A has $k = 1$ with matching fingerprint (`IDENTICAL`, `historyVerified = true`). Origin B has $k = 5$ with matching tail fingerprint (`FRONTIER_IDENTICAL`, `historyVerified = false`). Result: `hasUnverifiedHistory = true` (`unverifiedHistoryOrigins = ['peer-B']`).
6. **A confirmed fork still takes precedence over ordinary dominance:**
   - Origin A has a confirmed fork (`CONFLICT_FORK`), while Origin B has an ordinary missing range (`REMOTE_AHEAD`). Result: `classification = 'DIVERGENT_FORK'`, `forkDetected = true`.
7. **Snapshot recovery requirements remain correctly represented:**
   - Origin A has a missing sequence range starting at or below the snapshot frontier. Result: `classification = 'REQUIRES_SNAPSHOT_RECOVERY'`, `outsideRemoteHorizon = true`, `requiresSnapshotRecovery = true`.

### 4.2 Full Monorepo Test Totals

```
> npm test

Packages:
- @syncplay/control-events:   6 test files,  49 passed (49 total)
- @syncplay/peer-mesh:        6 test files, 101 passed (101 total)
- @syncplay/protocol:         4 test files, 273 passed (273 total)
- @syncplay/sync-engine:     19 test files, 290 passed (290 total)  [+49 tests from M1]
- @syncplay/bootstrap-server: 3 test files,  91 passed (91 total)

Total Monorepo Tests: 804 tests passed across 38 test files (0 failures).
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
