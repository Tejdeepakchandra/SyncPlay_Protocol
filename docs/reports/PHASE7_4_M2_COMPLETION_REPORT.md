# Phase 7.4 Milestone M2 Completion Report: Digest Generation and Comparison

**Milestone:** Phase 7.4 — Milestone M2 (Digest Generation & Comparison)  
**Status:** COMPLETED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Parent / M1 Implementation Baseline:** `309b808` (amending `5cadc03`)  
**M2 Initial Commit:** `7049ca6`  
**M2 Final Correction Commit:** Current (to be committed)  
**Date:** 2026-11-17  

---

## 1. Executive Summary

Milestone M2 implements the decentralized state digest generation, pure deterministic replication comparison, and anti-entropy scheduling subsystem for the SyncPlay Protocol.

Following the targeted architectural review pass, M2 strictly standardizes:
1. **Exact Snapshot Frontier Semantics:** `snapshotFrontier[origin]` is the highest contiguous sequence for that origin already incorporated into the compacted base snapshot (sequences $\le \text{snapshotFrontier}$ are compacted out of the active log).
2. **Bounded Fingerprint Guarantees (Option 1):** Tail fingerprints represent the operation at sequence $k = V_{\text{rep}}[\text{origin}]$. They detect conflicting operations at the frontier, but do NOT prove equality across arbitrary unchained predecessor operations in earlier history.
3. **Structured Completeness Indicator:** Added `hasUnverifiedHistory: boolean` (`unprovableOrigins.length > 0`) to `DigestComparisonResult` so callers never mistake vector dominance for complete historical verification when origins lack fingerprint evidence.
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
- **Milestone M2 Targeted Corrections:** Current commit

---

## 3. Targeted Review Corrections Summary

### 3.1 Correction A: Standardized `snapshotFrontier` Semantics

- **Exact Definition:**
  $$\text{snapshotFrontier}[p] = \text{highest contiguous sequence for origin } p \text{ already incorporated into the compacted base snapshot}$$
- **Operational Horizon:**
  - Sequences $1 \le s \le \text{snapshotFrontier}[p]$ have been compacted into the base snapshot and removed from the active log.
  - The oldest retained operation in the active operation log is $\text{snapshotFrontier}[p] + 1$.
  - Missing ranges $[fromSeq, toSeq]$ where $fromSeq \le \text{snapshotFrontier}[p]$ cannot be served from the log and trigger `OUTSIDE_REMOTE_HORIZON` (`requiresSnapshotRecovery = true`).
  - Missing ranges where $fromSeq > \text{snapshotFrontier}[p]$ are retained in the log and served via `REMOTE_AHEAD`.
  - An invariant check detects and reports any structural inconsistency where $\text{snapshotFrontier}[p] > \text{replicationVector}[p]$.

### 3.2 Correction B: Fingerprint Completeness Audit (Option 1 Selected)

- **Audit Findings:** A single tail fingerprint $H[p]$ at $k = V_{\text{rep}}[p]$ hashes only the operation at sequence $k$. If two peers share op C at sequence 12 but hold conflicting operations at sequence 11 (e.g. op B vs op X), their tail fingerprints at sequence 12 match if op C's causal context does not chain the hash of sequence 11.
- **Option 1 Conformance:** The existing M1 schema contract (one tail fingerprint per origin) is strictly preserved without inventing wire extensions.
- **Documented Guarantees:**
  - The state digest tail fingerprint detects a conflicting operation at the compared frontier ($V_{\text{rep}}[p]$).
  - It does **not** prove universal equality across intermediate operations in preceding history.
  - Intermediate divergence is detected by Phase 7.3 operation admission (`originSeqIndex` collision detection and exact Lamport checks) during event ingestion or replay, not by the state digest.
  - Compacted sequences lacking retained fingerprints are explicitly classified as `HISTORICAL_EQUALITY_UNPROVABLE`.

### 3.3 Correction C: Completeness Indicator & Multi-Origin Combination

- **Completeness Indicator:** Added `hasUnverifiedHistory: boolean` to `DigestComparisonResult` (`unprovableOrigins.length > 0`).
- **Global Classification Semantics:**
  - A global dominance classification (`LOCAL_DOMINATES` / `REMOTE_DOMINATES`) indicates that across verified origins, one peer holds a superset of operations.
  - If `hasUnverifiedHistory === true`, the caller is explicitly warned that historical equality could not be proven for unverified origins.
  - If an active fork is detected on any origin ($H_L \ne H_R$), `DIVERGENT_FORK` is prioritized regardless of other origins.
  - If any origin requires snapshot recovery ($fromSeq \le S_R$), `REQUIRES_SNAPSHOT_RECOVERY` is prioritized for the overall classification.

### 3.4 Correction D: Reconciled Anti-Entropy Scheduling Limits

- **Unified Policy:**
  - Token-bucket capacity: 2 tokens.
  - Token refill rate: 1 token per 1000ms ($1\text{ digest/sec}$).
  - Shared bucket: Both periodic timer ticks and reactive triggers pass through `broadcastDigestIfPermitted()`, preventing accidental rate-limit bypasses.
  - Reactive trigger coalescing: Multiple reactive triggers within the same event loop tick are coalesced into a single 0ms task.

### 3.5 Correction E: Trusted Transport Identity & Capability Boundaries

- **Trust Boundary Invariant:** In `LeaderlessSyncEngine.compareRemoteDigest()`, `remoteDigest.senderPeerId` and `remoteDigest.connectionGeneration` are untrusted remote payload claims.
- **Non-Authorization:** Digest comparison does **not** authorize a peer as a repair supplier and does **not** authenticate connection generations.
- **Phase 7.3 Preservation:** Authoritative supplier authorization strictly requires Phase 7.3-R5-H capability-gated registration via `AuthenticatedSupplierContext` injected by the trusted transport adapter.

---

## 4. Verification and Test Results

### 4.1 Focused Test Suites

1. **`packages/sync-engine/src/__tests__/replication-comparator.test.ts` (23 tests passed):**
   - Session and epoch mismatch detection.
   - Identical empty and non-empty vector comparison.
   - `LOCAL_DOMINATES` and `REMOTE_DOMINATES` with exact missing sequence ranges.
   - `LOCAL_ONLY` and `REMOTE_ONLY` origin handling.
   - `CONCURRENT_INCOMPARABLE` multi-origin gaps.
   - `CONFLICT_FORK` detection on tail fingerprint divergence.
   - `HISTORICAL_EQUALITY_UNPROVABLE` detection on missing tail fingerprints.
   - **Boundary Tests (Correction A):**
     - Requested missing range entirely below compacted frontier ($toSeq \le S_R$).
     - Missing range starting exactly at compacted frontier ($fromSeq = S_R$).
     - Missing range starting immediately after compacted frontier ($fromSeq = S_R + 1$).
     - Origin with no compacted operations ($S_R = 0$).
     - Structural inconsistency detection ($S_R > V_R$).
   - **Mandatory Completeness Case (Correction B):**
     - Verified that matching tail fingerprints at sequence 12 with divergence at sequence 11 evaluates to `IDENTICAL` at the frontier, proving that a single tail fingerprint does not detect intermediate divergence.
   - **Multi-Origin Combinations (Correction C):**
     - Clear missing range on origin 1 + unprovable origin on origin 2 (`hasUnverifiedHistory === true`).
     - Confirmed fork on origin 1 + normal missing range on origin 2 (`DIVERGENT_FORK`).
     - Outside-horizon on origin 1 + normal missing range on origin 2 (`REQUIRES_SNAPSHOT_RECOVERY`).
   - Lexicographical determinism under reversed key construction order.
   - Deep object immutability and frozen return structures.

2. **`packages/sync-engine/src/__tests__/anti-entropy-manager.test.ts` (18 tests passed):**
   - Schema-compliant digest generation in empty sessions.
   - Accurate replication vector, snapshot frontier, and tail SHA-256 fingerprint generation.
   - Multi-origin concurrent digest generation.
   - Distinction between compacted base snapshot frontier and active log.
   - Schema enforcement of 64-origin bound (64 origins pass, 65 origins fail).
   - Consistent point-in-time reads under rapid state updates.
   - Safe dropping of legacy Phase 2 digests and future format versions.
   - Fake-clock periodic scheduling within $[4000\text{ms}, 6000\text{ms}]$ jitter bounds.
   - Reactive trigger coalescing within single tick.
   - **Token-bucket rate limiting (Correction D):**
     - Verified capacity 2, refill 1 token/sec.
     - Verified periodic and reactive triggers share the exact same token bucket.
   - Clean disposal and cancellation of all timers.
   - Epoch transition reset and reactive trigger.
   - Strict separation: throwing on unimplemented M3 (`reconcileGaps`) and M4 (`serveRepairRequest`) methods.

### 4.2 Full Monorepo Test Totals

```
> npm test

Packages:
- @syncplay/control-events:   6 test files,  49 passed (49 total)
- @syncplay/peer-mesh:        6 test files, 101 passed (101 total)
- @syncplay/protocol:         4 test files, 273 passed (273 total)
- @syncplay/sync-engine:     19 test files, 282 passed (282 total)  [+41 tests from M1]
- @syncplay/bootstrap-server: 3 test files,  91 passed (91 total)

Total Monorepo Tests: 796 tests passed across 38 test files (0 failures).
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

**PHASE 7.4 M2 — TARGETED CORRECTIONS COMPLETE. AWAITING ARCHITECTURAL REVIEW AND EXPLICIT AUTHORIZATION FOR M3.**
