# Phase 7.5 M4.1 — Final Targeted Correction Report: Epoch Fencing and Non-Destructive Failover

**Target Baseline Commit:** `55bdf80d7fb4bc573d8b5abe672d06dec969678d`  
**Direct Parent Commit:** `55bdf80d7fb4bc573d8b5abe672d06dec969678d`  
**Scope:** Phase 7.5 M4.1 Only  
**Date:** 2026-11-20  

---

## 1. Executive Summary

This targeted correction milestone resolves the two defects identified during the independent final correctness audit of Phase 7.5 Milestone M4.1:
1. **Defect A (Cross-Epoch Chunk Retention):** Enforces strict membership-epoch fencing in `ChunkAssembler.prepareForFailover()`, preventing chunks received under an earlier membership epoch from being retained when failover is attempted.
2. **Defect B (Destructive Handling of Incompatible Candidates):** Refactors `prepareForFailover()` into a non-destructive discriminated API that rejects incompatible or malformed candidates without altering or destroying the in-progress transfer, while providing explicit transfer restart APIs.

All monorepo tests, TypeScript builds, and Vite production bundles pass with zero errors. No work from Milestone M4.2 or later was initiated.

---

## 2. Root Cause Analysis

### Defect A: Cross-Epoch Chunk Retention
- **Root Cause:** In the previous implementation of `ChunkAssembler.prepareForFailover()`, the compatibility predicate `isExactMatch` compared only `snapshotId`, `expectedSha256Digest`, `totalChunks`, `totalSizeBytes`, and `nominalChunkSizeBytes`. It did not compare `membershipEpoch`.
- **Impact:** When a candidate with an incremented membership epoch (`sampleEpoch + 1`) was passed with matching snapshot parameters, `isExactMatch` evaluated to `true`. The method updated `this.transferParams.membershipEpoch = params.membershipEpoch` and retained previously received chunks across the epoch boundary, violating the protocol's strict membership-epoch fencing invariant.

### Defect B: Destructive Handling of Incompatible Candidates
- **Root Cause:** The `else` branch of `prepareForFailover()` unconditionally called `this.abortTransfer()` followed by `this.initTransfer()`.
- **Impact:** Any stale, incompatible, or invalid failover candidate (such as an offer for an older snapshot or mismatched geometry) resulted in the immediate abortion of the healthy active transfer and destruction of all accumulated chunks. This conflated candidate compatibility testing with intentional transfer abortion.

---

## 3. Architecture & Implementation Corrections

### 3.1 Exact Failover Compatibility Predicate
Verified chunks in `ChunkAssembler` are retained **if and only if** all immutable transfer identity and partition geometry fields match bit-exact:

$$\begin{aligned}
\text{candidate}.\text{sessionId} &=== \text{active}.\text{sessionId} \quad (\text{if provided}) \\
\land \quad \text{candidate}.\text{membershipEpoch} &=== \text{active}.\text{membershipEpoch} \\
\land \quad \text{candidate}.\text{snapshotId} &=== \text{active}.\text{snapshotId} \\
\land \quad \text{candidate}.\text{expectedSha256Digest} &=== \text{active}.\text{expectedSha256Digest} \quad (\text{case-insensitive hex}) \\
\land \quad \text{candidate}.\text{totalSizeBytes} &=== \text{active}.\text{totalSizeBytes} \\
\land \quad \text{candidate}.\text{totalChunks} &=== \text{active}.\text{totalChunks} \\
\land \quad \text{candidate}.\text{nominalChunkSizeBytes} &=== \text{active}.\text{nominalChunkSizeBytes}
\end{aligned}$$

The following fields may legitimately change during a valid same-epoch supplier failover:
- `transferId`: Fresh attempt identifier to fence out late or in-flight packets from the previous supplier.
- `supplierPeerId`: Identity of the replacement supplier.
- `connectionGeneration`: Transport connection generation of the replacement supplier.

### 3.2 Non-Destructive Discriminated Failover API
In `packages/sync-engine/src/recovery/chunk-assembler.ts`:
```typescript
export interface FailoverCandidateParams {
  transferId: string;
  supplierPeerId: string;
  membershipEpoch: number;
  connectionGeneration?: number;
  snapshotId: string;
  expectedSha256Digest: string;
  totalChunks: number;
  totalSizeBytes: number;
  nominalChunkSizeBytes: number;
  sessionId?: string;
}

export type FailoverRejectReason =
  | 'NO_ACTIVE_TRANSFER'
  | 'DISPOSED'
  | 'INVALID_PARAMETERS'
  | 'SESSION_MISMATCH'
  | 'EPOCH_MISMATCH'
  | 'SNAPSHOT_ID_MISMATCH'
  | 'DIGEST_MISMATCH'
  | 'GEOMETRY_MISMATCH';

export type PrepareForFailoverResult =
  | {
      ok: true;
      retainedChunks: number;
    }
  | {
      ok: false;
      reason: FailoverRejectReason;
      detail: string;
      retainedChunks: 0;
    };
```

When a candidate fails validation or compatibility:
- An explicit rejection object is returned (`{ ok: false, reason, detail, retainedChunks: 0 }`).
- **Zero side effects occur on the active transfer:** `this.transferParams`, sparse chunk array `this.chunks`, chunk hash tracking `this.chunkHashes`, received counters, and memory reservations remain completely untouched.

### 3.3 Explicit Transfer Restart
Transfer restart is now decoupled from failover candidate evaluation:
1. **Existing Lifecycle:** Callers can explicitly invoke `assembler.abortTransfer()` to purge state, followed by `assembler.initTransfer(params)` with newly admitted parameters.
2. **Explicit Restart Method:**
   ```typescript
   public restartTransfer(params: InitTransferParams): void {
     this.abortTransfer();
     this.initTransfer(params);
   }
   ```
   This guarantees that abandoning an active transfer is an explicit coordinator decision, rather than an accidental side effect of an incompatible candidate probe.

### 3.4 Inbound Epoch Validation & Transition Behavior
- `ChunkAssembler.ingestChunk()` strictly validates `membershipEpoch` on both the optional envelope context and the payload itself before processing buffers:
  ```typescript
  if (payload.membershipEpoch !== this.transferParams.membershipEpoch) {
    return {
      ok: false,
      reason: 'EPOCH_MISMATCH',
      detail: `Payload membershipEpoch ${payload.membershipEpoch} does not match active ${this.transferParams.membershipEpoch}`,
    };
  }
  ```
- Chunks with mismatched membership epochs are rejected immediately before storage, allocation, or progress counter increments.

---

## 4. Admission Authority Boundary Clarification

The architecture documentation ([docs/PHASE7_5_M4_ARCHITECTURE.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_ARCHITECTURE.md#L215-L250)) has been updated to clarify the boundary between M4.1 and M4.3:
1. `calculateSnapshotAdmission()` is a pure calculation function in `@syncplay/protocol` that evaluates whether an advertised snapshot transfer fits within negotiated SCTP message size limits without exceeding envelope caps or chunk count ceilings.
2. `ChunkAssembler` is an isolated data structure responsible for slicing assembly, per-chunk SHA-256 checks, duplicate handling, and full snapshot verification. It has no WebRTC or SCTP handles and cannot query transport limits directly.
3. The future `SnapshotRecoveryCoordinator` (Milestone M4.3) is solely responsible for evaluating `calculateSnapshotAdmission()` against the actual negotiated transport limit and passing the admitted parameters into `ChunkAssembler.initTransfer()`.
4. M4.1 provides the foundational schemas, pure admission math, and isolated chunk assembler; production enforcement of admission authority belongs to M4.3 coordinator integration.

---

## 5. Verification & Test Results

### 5.1 Focused ChunkAssembler Regression Suite
The test suite in `packages/sync-engine/src/__tests__/chunk-assembler.test.ts` was expanded to 37 comprehensive unit tests covering all required scenarios:
- **3.A Valid Same-Epoch Failover:** Verified retention of chunks `[0, 1]`, updated supplier peer and connection generation, rejection of old supplier/transfer chunks with `IDENTITY_MISMATCH`, and successful completion with new supplier chunks.
- **3.B Cross-Epoch Candidate Rejection:** Verified rejection with `EPOCH_MISMATCH`, 0 retained chunks for candidate, active transfer state completely preserved, cross-epoch chunk ingestion rejected, and original supplier transfer continuation.
- **3.C Stale Snapshot Candidate Rejection:** Verified rejection with `SNAPSHOT_ID_MISMATCH`, active transfer state and chunks untouched.
- **3.D Digest Mismatch Rejection:** Verified rejection with `DIGEST_MISMATCH` without mutating active transfer.
- **3.E Geometry Mismatch Rejection:** Independently verified rejection for mismatched `totalSizeBytes`, `totalChunks`, and `nominalChunkSizeBytes` with `GEOMETRY_MISMATCH`.
- **3.F Invalid Candidate Parameter Rejection:** Verified rejection for sub-minimum nominal chunk size, inconsistent canonical partition chunk count, malformed digest, and mismatched session ID without mutating active transfer.
- **3.G Explicit Restart:** Verified both explicit `abortTransfer() + initTransfer()` lifecycle and `restartTransfer()` method, ensuring zero chunk leakage across epochs.
- **3.H Existing Lifecycle Invariants:** Re-verified duplicate handling, corrupt chunk rejection, incomplete assembly safety, and AC-M4-MEM-3 memory nullification.

```
✓ src/__tests__/chunk-assembler.test.ts (37 tests) 33ms
```

### 5.2 Full Monorepo Test Results
Executed `npm test` across all 5 workspace packages:
- `@syncplay/control-events`: 6 test files, 49 tests passed.
- `@syncplay/peer-mesh`: 6 test files, 108 tests passed.
- `@syncplay/protocol`: 6 test files, 337 tests passed.
- `@syncplay/sync-engine`: 30 test files, 540 tests passed.
- `@syncplay/bootstrap-server`: 3 test files, 91 tests passed.
- **Total:** 51 test suites, **1,125 tests passed, 0 failures**.

### 5.3 TypeScript & Production Build Verification
- `npm run build` executed `tsc` across all workspace packages and `vite build` on `@syncplay/demo`.
- **Result:** 182 modules transformed, built production bundle in 1.29s with **0 errors**.

### 5.4 Phase 7.4 Regression Status
All 40+ Phase 7.4 test suites covering state replication, anti-entropy digests, repair fulfillment pipelines, compaction frontiers, and transport adapters passed without any modifications or regressions.

---

## 6. Exact Files Modified

1. `packages/sync-engine/src/recovery/chunk-assembler.ts`
   - Added `FailoverCandidateParams`, `FailoverRejectReason`, `PrepareForFailoverResult` types.
   - Refactored `prepareForFailover()` to validate parameters and enforce strict bit-exact compatibility including `membershipEpoch` and `sessionId`.
   - Made candidate rejection non-destructive.
   - Added `restartTransfer()` method.
2. `packages/sync-engine/src/index.ts`
   - Exported `FailoverCandidateParams`, `FailoverRejectReason`, `PrepareForFailoverResult`.
3. `packages/sync-engine/src/__tests__/chunk-assembler.test.ts`
   - Replaced old destructive/epoch-polluting failover tests with 14 comprehensive non-destructive failover, epoch fencing, invalid candidate, and explicit restart tests.
   - Updated F-1 nominal chunk size mismatch test to assert non-destructive preservation of active chunks.
4. `docs/PHASE7_5_M4_ARCHITECTURE.md`
   - Updated §3.2 with the complete failover compatibility predicate, non-destructive rejection semantics, explicit restart lifecycle, and M4.1 vs M4.3 admission authority boundaries.
5. `docs/PHASE7_5_M4_1_FINAL_CORRECTION_REPORT.md`
   - Created this comprehensive completion report.

---

## 7. Scope Compliance & Milestone Freeze

- **M4.2 / Outbound Serialization:** No code, schemas, or tests for outbound snapshot transfer or serialization were authored.
- **M4.3 / Recovery Coordination:** No coordinator state machine or transport binding code was authored.
- **Milestone Status:** Phase 7.5 Milestone M4.1 is now **complete, fully verified, and ready to be frozen**.
