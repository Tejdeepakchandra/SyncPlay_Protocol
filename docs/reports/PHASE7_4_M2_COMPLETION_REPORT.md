# Phase 7.4 Milestone M2 Completion Report: Digest Generation and Comparison

**Milestone:** Phase 7.4 — Milestone M2 (Digest Generation & Comparison)  
**Status:** COMPLETED & VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Previous Implementation Baseline:** Phase 7.3-R5-H (`e49b81a`)  
**Parent / M1 Implementation Baseline:** `309b808` (amending `5cadc03`)  
**Date:** 2026-11-17  

---

## 1. Executive Summary

Milestone M2 implements the decentralized state digest generation, pure deterministic replication comparison, and anti-entropy scheduling subsystem for the SyncPlay Protocol.

Building on the M1 wire schema contracts and Phase 7.3 causal ordering and capability-gated boundaries, M2 enables peers to atomically capture their contiguous replication frontier, compare it against remote peer digests, identify exact missing sequence ranges, detect sequence collisions/forks, distinguish uncompacted log history from compacted base snapshots, and coordinate bounded periodic and coalesced reactive digest exchanges without centralized coordination.

Milestones M3 (Repair Request Pipeline), M4 (Supplier Servicing), M5 (Transport Integration), and Phase 7.5 (Snapshot Recovery) remain strictly unimplemented.

---

## 2. Baselines and Commit Lineage

- **Approved Phase 7.4 Architecture:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`
- **Approved Phase 7.2-R9 Causal Baseline:** `1825de1`
- **Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`
- **Milestone M1 Baseline:** Commit `309b808` (amending `5cadc03`)
- **Milestone M2 Implementation:** Current commit (to be committed)

---

## 3. New Modules and Interface Contracts

### 3.1 New Modules Added

1. **`packages/sync-engine/src/replication/types.ts`:**
   - `DigestComparisonClassification`: `'IDENTICAL' | 'LOCAL_DOMINATES' | 'REMOTE_DOMINATES' | 'CONCURRENT_INCOMPARABLE' | 'DIVERGENT_FORK' | 'REQUIRES_SNAPSHOT_RECOVERY' | 'INCONCLUSIVE'`.
   - `OriginComparisonStatus`: `'IDENTICAL' | 'LOCAL_AHEAD' | 'REMOTE_AHEAD' | 'LOCAL_ONLY' | 'REMOTE_ONLY' | 'CONFLICT_FORK' | 'OUTSIDE_LOCAL_HORIZON' | 'OUTSIDE_REMOTE_HORIZON' | 'HISTORICAL_EQUALITY_UNPROVABLE'`.
   - `OriginComparisonDetail`: Granular per-origin structure containing sequence numbers, frontiers, fingerprints, missing ranges, and diagnostic reasons.
   - `DigestComparisonResult`: Structured comparison contract conforming strictly to `PHASE7_4_INTERFACE_SPECIFICATION.md §2.1` plus enriched diagnostic fields.
   - `IAntiEntropyManager`: Anti-entropy subsystem lifecycle interface.
   - `AntiEntropyStatus`: Operational telemetry metrics.
   - `AntiEntropySchedulerOptions` & `SchedulerClock`: Injectable monotonic clock and timer interface for 100% deterministic testing.
2. **`packages/sync-engine/src/replication/replication-comparator.ts`:**
   - Pure deterministic comparison function: `compareDigests(localDigest: StateDigestPayload, remoteDigest: StateDigestPayload): DigestComparisonResult`.
   - Independent of local clock, caller peer ID, or arrival order.
   - Origins sorted strictly lexicographically (`allOrigins.sort()`) guaranteeing identical output regardless of object key order.
3. **`packages/sync-engine/src/replication/anti-entropy-manager.ts`:**
   - `AntiEntropyManager` implementing `IAntiEntropyManager`.
   - Manages bounded periodic digest broadcasting with uniform random jitter ($5000\text{ms} \pm 1000\text{ms}$, interval $4000\text{ms}..6000\text{ms}$).
   - Coalesces reactive triggers within the same event loop tick.
   - Enforces token-bucket rate limiting ($1\text{ digest/sec}$, burst 2) to protect SCTP DataChannels.
   - Clean lifecycle management: cancels all timers and suppresses callbacks upon `dispose()`; cleans state on `onPeerDisconnected()` and `onEpochTransition()`.
4. **`packages/sync-engine/src/replication/index.ts`:**
   - Facade re-exporting all replication types, comparator, and manager.

### 3.2 Modified Existing Files

1. **`packages/sync-engine/src/log/ordered-operation-log.ts` & `types.ts`:**
   - Added `getTailFingerprint(originPeerId: string, atSequence?: number): string | undefined`: queries active log first, then compacted checkpoints, returning the canonical 64-char lowercase hexadecimal SHA-256 fingerprint.
   - Added `getReplicationFrontier(): Record<string, number>`: returns highest sequence numbers observed per origin across retained operations and compacted snapshots.
2. **`packages/sync-engine/src/engine/leaderless-sync-engine.ts`:**
   - Added `createLocalDigest(sentAtOverride?: number): StateDigestPayload`: atomic point-in-time snapshot read of contiguous replication vector, snapshot frontier, and tail fingerprints, validated against `StateDigestPayloadSchema`.
   - Added `compareRemoteDigest(remoteDigest: unknown): DigestComparisonResult`: discriminates legacy Phase 2 digests and unsupported future format versions, validates schema, and invokes pure comparator.
3. **`packages/sync-engine/src/index.ts`:**
   - Re-exported replication subsystem.

---

## 4. Digest Data Model and Consistency

- **`formatVersion` (Literal `2`):** Distinguishes Phase 7.4 multi-vector digests from legacy Phase 2 scalar digests (`digestId`, `stateVersion`, `digest`).
- **`replicationVector` ($V_{\text{rep}}$):** Contiguous highest applied sequences per origin. $V_{\text{rep}}[p] = k$ guarantees that sequences $1..k$ have passed schema validation, transport capability authorization, Lamport timestamp check, total-order insertion, and deterministic reduction.
- **`snapshotFrontier` ($V_{\text{baseSnap}}$):** Oldest retained sequence per origin. Operations below this threshold have been compacted into the base snapshot.
- **`historyFingerprints` ($H_{\text{origin}}$):** Canonical SHA-256 tail fingerprint for each origin with sequence $k = V_{\text{rep}}[p] > 0$, computed using the approved RFC 8785 canonical JSON serializer.
- **Consistent Snapshot Read:** In the JavaScript runtime, `LeaderlessSyncEngine` updates are synchronous. `createLocalDigest()` performs an atomic point-in-time defensive clone of the replication vector and snapshot frontier, queries tail fingerprints, and returns an immutable structure.

---

## 5. Comparison Semantics & Decision Matrix

Given `localDigest` and `remoteDigest`:

1. **Pre-Checks:**
   - Mismatched `sessionId` $\to$ `INCONCLUSIVE` (`"Session mismatch"`).
   - Mismatched `membershipEpoch` $\to$ `INCONCLUSIVE` (`"Epoch mismatch"`).
2. **Deterministic Origin Evaluation (Lexicographically Sorted):**
   - **$L == R$ ($L > 0$):**
     - $H_L === H_R \implies$ `IDENTICAL`.
     - $H_L !== H_R \implies$ `CONFLICT_FORK` (`forkDetected = true`, `conflictingOrigins.push(p)`).
     - Missing fingerprints on either side $\implies$ `HISTORICAL_EQUALITY_UNPROVABLE` (`unprovableOrigins.push(p)`).
   - **$L < R$ (Remote Ahead):**
     - Missing range: $[L + 1, R]$.
     - If $L + 1 \le S_R \implies$ `OUTSIDE_REMOTE_HORIZON` (`outsideRemoteHorizon = true`, `requiresSnapshotRecovery = true`).
     - If $L + 1 > S_R \implies$ `REMOTE_AHEAD`, `missingFromLocal.push([L + 1, R])`.
   - **$L > R$ (Local Ahead):**
     - Missing range: $[R + 1, L]$.
     - If $R + 1 \le S_L \implies$ `OUTSIDE_LOCAL_HORIZON` (`outsideLocalHorizon = true`).
     - If $R + 1 > S_L \implies$ `LOCAL_AHEAD`, `missingFromRemote.push([R + 1, L])`.
   - **Present on Only One Side:**
     - Remote only ($L = 0, R > 0$): if $1 \le S_R \implies$ `OUTSIDE_REMOTE_HORIZON`; else `missingFromLocal.push([1, R])`.
     - Local only ($L > 0, R = 0$): if $1 \le S_L \implies$ `OUTSIDE_LOCAL_HORIZON`; else `missingFromRemote.push([1, L])`.
3. **Overall Classification:**
   - `DIVERGENT_FORK` if `forkDetected`.
   - `REQUIRES_SNAPSHOT_RECOVERY` if `requiresSnapshotRecovery`.
   - `INCONCLUSIVE` if unprovable historical differences exist without clear missing ranges.
   - `IDENTICAL`, `LOCAL_DOMINATES`, `REMOTE_DOMINATES`, or `CONCURRENT_INCOMPARABLE` based on missing sequence range presence.

---

## 6. Horizon Limitations & Completeness Guarantees

As established in the Phase 7.4 architectural review:
- **Detectable Differences:** The hybrid digest reliably detects missing retained sequence ranges $[V_A[p] + 1, V_B[p]]$, conflicting tail operations at the frontier ($H_A[p] \ne H_B[p]$), and divergence within retained checkpoint windows ($K=256$).
- **Undetectable Compacted History:** Matching replication vectors and tail fingerprints establish latest-retained operation agreement, **not** unconditional Byzantine historical equality across ancient history compacted before the retention horizon ($s \le \text{snapshotFrontier}$). When historical fingerprints are missing for compacted sequences, the comparator explicitly marks the origin as `HISTORICAL_EQUALITY_UNPROVABLE` rather than falsely claiming convergence.
- **Snapshot Recovery Handoff:** When a peer requires operations that have been compacted out of the remote peer's log ($L + 1 \le S_R$), log-based repair is bypassed and flagged as `REQUIRES_SNAPSHOT_RECOVERY` for Phase 7.5 catch-up.

---

## 7. Scheduling and Flow Control

1. **Periodic Scheduling:**
   - Default interval: $5000\text{ms} \pm 1000\text{ms}$ uniform random variance ($4000\text{ms}..6000\text{ms}$).
   - Exactly one periodic timer active at any given time.
2. **Reactive Scheduling:**
   - Coalesced on microtask/0ms timer to collapse multiple triggers occurring in the same tick into a single broadcast.
3. **Token-Bucket Rate Limiter:**
   - Bucket capacity (burst): 2 tokens.
   - Refill rate: 1 token per 1000ms.
   - Excess broadcast requests are safely dropped.
4. **Lifecycle & Disposal:**
   - `dispose()` cancels all active timers, marks `isDisposed = true`, and prevents callbacks.
   - `onEpochTransition(newEpoch)` cancels in-flight reactive timers, reschedules periodic ticks, and broadcasts under the new epoch.
   - `onPeerDisconnected(peerId)` clears peer rate limits and records.

---

## 8. Resource Bounds and Error Handling

- **Origin Bounds:** Bounded to `MAX_VECTOR_ENTRIES = 64` origins per digest (strictly validated by Zod schema).
- **Memory Safety:** Comparator returns deeply frozen objects and arrays without mutating or retaining references to input payloads.
- **Legacy & Version Discrimination:** Legacy Phase 2 digests and unsupported future format versions are safely dropped with diagnostic reasons without mutating engine state.
- **Error Codes:** Throws `SyncEngineError('DISPOSED')` on disposed operations.

---

## 9. Verification & Monorepo Test Results

### 9.1 Verification Suite

1. **`packages/sync-engine/src/__tests__/replication-comparator.test.ts` (15 tests):**
   - Session and epoch mismatch detection.
   - Identical empty and non-empty vector comparison.
   - `LOCAL_DOMINATES` and `REMOTE_DOMINATES` with exact missing sequence ranges.
   - `LOCAL_ONLY` and `REMOTE_ONLY` origin handling.
   - `CONCURRENT_INCOMPARABLE` multi-origin gaps.
   - `CONFLICT_FORK` detection on tail fingerprint divergence.
   - `HISTORICAL_EQUALITY_UNPROVABLE` detection on missing tail fingerprints.
   - `OUTSIDE_REMOTE_HORIZON` and `OUTSIDE_LOCAL_HORIZON` compacted snapshot detection.
   - Lexicographical determinism under reversed key construction order.
   - Deep object immutability and frozen return structures.
2. **`packages/sync-engine/src/__tests__/anti-entropy-manager.test.ts` (17 tests):**
   - Schema-compliant digest generation in empty sessions.
   - Exact replication vector, snapshot frontier, and tail SHA-256 fingerprint generation.
   - Multi-origin concurrent digest generation.
   - Distinction between compacted base snapshot frontier and active log.
   - Schema enforcement of 64-origin bound (64 origins pass, 65 origins fail).
   - Consistent point-in-time reads under rapid state updates.
   - Safe dropping of legacy Phase 2 digests and future format versions.
   - Fake-clock periodic scheduling within $[4000\text{ms}, 6000\text{ms}]$ jitter bounds.
   - Reactive trigger coalescing within single tick.
   - Token-bucket rate limiting (burst 2, refill 1/s).
   - Clean disposal and cancellation of all timers.
   - Epoch transition reset and reactive trigger.
   - Strict separation: throwing on unimplemented M3 (`reconcileGaps`) and M4 (`serveRepairRequest`) methods.

### 9.2 Monorepo Test Totals

```
> npm test

Packages:
- @syncplay/control-events:  6 test files,  49 passed (49 total)
- @syncplay/peer-mesh:       6 test files, 101 passed (101 total)
- @syncplay/protocol:        4 test files, 273 passed (273 total)
- @syncplay/sync-engine:    19 test files, 273 passed (273 total)  [+32 tests from M1]
- @syncplay/bootstrap-server:3 test files,  91 passed (91 total)

Total Monorepo Tests: 787 tests passed across 38 test files (0 failures).
```

### 9.3 Monorepo Build Results

- `@syncplay/control-events`: `tsc` clean (0 errors)
- `@syncplay/peer-mesh`: `tsc` clean (0 errors)
- `@syncplay/protocol`: `tsc` clean (0 errors)
- `@syncplay/sync-engine`: `tsc` clean (0 errors)
- `@syncplay/bootstrap-server`: `tsc` clean (0 errors)
- `@syncplay/demo`: Vite production build passed (175 modules transformed, bundle size 202.72 kB)

---

## 10. Explicit Confirmation of Unimplemented Subsystems

The following subsystems are explicitly confirmed to remain **UNIMPLEMENTED** in Milestone M2:

1. **M3 — Repair Request Pipeline:** No `REPAIR_REQUEST` dispatching, no repair timeout tracking, no retry backoff, and no repair response ingestion pipeline.
2. **M4 — Supplier Servicing:** No supplier log range query servicing, no `OPERATION_BATCH` slicing or transmission, and no supplier failover.
3. **M5 — Transport Integration:** No WebRTC DataChannel transport adapter or message size negotiation.
4. **Phase 7.5 — Snapshot Recovery:** No snapshot reset or full state catch-up automation.

---

**PHASE 7.4 M2 — DIGEST GENERATION AND COMPARISON COMPLETE. AWAITING ARCHITECTURAL REVIEW AND EXPLICIT AUTHORIZATION FOR M3.**
