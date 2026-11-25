# Phase 7.5 Sub-Milestone M5.0-R9 Correction Report: Concurrent Reset Serialization, Admission Linearization & Settlement Failure Semantics

**Target:** Phase 7.5 Sub-Milestone M5.0-R9
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Baseline Commit (M5.0-R8):** `d2a50c8b80befcaa0f2b388f4f700a7f779e47ea`
**Direct Parent of Baseline:** `1cef0fddc0dc7d7b2c0197c91fb3dee7c496e584` (M5.0-R7)
**Authoritative Accepted Implementation Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## Executive Summary & Scope Verification

This document records the architectural resolutions for Sub-Milestone M5.0-R9 in response to the Engineering Authority's review of M5.0-R8 ("Conditional acceptance — not yet frozen / Milestone M5.1: NOT AUTHORIZED").

In accordance with strict architectural governance:
- **Zero Production Code Modified:** No files in `packages/*/src/**/*.ts` or `packages/*/src/**/*.tsx` were modified.
- **Zero Test Files Modified:** No files in `packages/**/__tests__/**` were modified.
- **Zero Package Manifests / Lockfiles Modified:** `package.json` and `package-lock.json` remain untouched.
- **Zero Configuration Modified:** `tsconfig*.json` remains untouched.
- **Zero Frozen Phase 1–7.4 or M4 Baselines Modified:** All prior baselines remain immutable.
- **Documentation Only:** Modifications are strictly restricted to the 5 M5 specifications:
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`
  - `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`
  and this correction report `docs/PHASE7_5_M5_0_R9_CORRECTION_REPORT.md`.

---

## Detailed Resolutions of Review Findings

### 1. Concurrent Reset Requests & Shared Settlement-Barrier Ownership (R8-A / R9-01)

#### Problem Statement
In M5.0-R8, while single in-flight custody was preserved across a reset boundary ($N_{\text{in-flight}} \le 1$), the specification did not fully define behavior when multiple reset requests arrive in rapid succession while an asynchronous drain operation is pending (e.g. Attempt 1 is in-flight, Reset A starts Attempt 2 and begins awaiting the settlement barrier, and Reset B arrives starting Attempt 3 before Attempt 1 settles). Uncoordinated settlement continuations could create competing recovery transfers or corrupted in-flight custody records.

#### Architectural Specification & Source Mapping
1. **Monotonic Attempt Sequence Invariant:**
   Every invocation of `resetRecovery(strategy, capability)` increments the coordinator's monotonic sequence counter:
   ```typescript
   const myAttemptId = ++this.currentAttemptId;
   ```
   Reset A assigns `myAttemptId = 2`; Reset B assigns `myAttemptId = 3`.

2. **Shared Settlement Barrier:**
   The settlement barrier is an idempotent promise tracking whether an in-flight drain operation is currently executing. Both Reset A and Reset B await the single shared settlement barrier:
   ```typescript
   await this.quarantineDrainer.drainSettledBarrier();
   ```

3. **Post-Settlement Attempt Re-Validation:**
   When the barrier resolves (i.e. Attempt 1 settles), every waiting reset continuation executes in microtask turns. Each continuation immediately validates its attempt token:
   ```typescript
   if (myAttemptId !== this.currentAttemptId || this.stateMachine.state !== 'RECOVERING') {
     // Obsolete reset attempt: abort continuation immediately without mutating state
     return;
   }
   ```
   - Reset A observes `myAttemptId (2) !== this.currentAttemptId (3)` and aborts immediately with zero side-effects.
   - Reset B observes `myAttemptId (3) === this.currentAttemptId (3)` and proceeds to initiate recovery discovery and transfer.

4. **Exact In-Flight Record Instance Ownership:**
   When the stale in-flight operation from Attempt 1 settles, its completion continuation releases strictly the exact record instance it owns:
   ```typescript
   if (this._inFlightRecord === staleRecord) {
     this._inFlightRecord = null;
   }
   ```
   It never clears or mutates a newer record belonging to a subsequent attempt. $N_{\text{in-flight}} \le 1$ is invariant at all times.

---

### 2. Mandatory Attempt Fencing at Admission Boundary (R8-B / R9-02)

#### Problem Statement
M5.0-R8 documented the method signature as `admitDrainedOperation(op, capability, attemptId?: number)`. Making `attemptId` optional created an architectural ambiguity: callers could omit `attemptId` and bypass attempt fencing during recovery admission.

#### Architectural Specification
1. **Mandatory Attempt ID for Recovery Admission:**
   The parameter signature is updated to make `attemptId` strictly required:
   ```typescript
   admitDrainedOperation(
     op: QuarantinedOperation,
     capability: DrainAdmissionContext,
     attemptId: number,
   ): DestinationAcceptanceReceipt;
   ```
2. **Exclusivity of Recovery Admission:**
   Non-recovery operational pathways (such as live client submission or live peer gossip) use `submit()` or `receive()`. They do not possess a `DrainAdmissionContext` capability and cannot invoke `admitDrainedOperation()`. Therefore, `admitDrainedOperation()` is exclusively invoked during recovery draining, and the `attemptId` parameter is strictly mandatory.
3. **Attempt Fencing Verification:**
   On entry, `admitDrainedOperation` validates:
   ```typescript
   if (attemptId !== this.coordinator.currentAttemptId || this.coordinator.recoveryState !== 'RECONCILING') {
     return {
       status: 'TERMINALLY_REJECTED',
       reason: 'STALE_ATTEMPT_FENCED',
       operationId: op.operation.id,
     };
   }
   ```
   If the attempt ID does not match the active coordinator attempt, the operation is terminally rejected with zero state mutation.

---

### 3. Synchronous Indivisibility of Admission & Run-to-Completion Linearization (R8-B / R9-03)

#### Problem Statement
The relationship between asynchronous quarantine draining and synchronous engine admission required explicit grounding in the single-threaded JavaScript execution model.

#### Source Audit & Verification
- **Source Inspection:** `packages/sync-engine/src/engine/leaderless-sync-engine.ts:975-1020`.
- **Finding:** `admitDrainedOperation` is a 100% synchronous method. It contains zero `await`, zero Promise returns, and zero microtask yields.
- **Run-to-Completion Guarantee:** Under JavaScript run-to-completion semantics, once `admitDrainedOperation()` is called, the attempt fencing check, capability brand verification, duplicate check, total-order SMR evaluation, and atomic state/log/vector mutations execute in a **single, indivisible call frame**.
- **Absence of Race Condition:** It is physically impossible for an asynchronous reset or attempt counter increment to interleave between the attempt check and the state mutation. Either the reset occurs before the synchronous frame (and the attempt check fails), or the reset occurs after the synchronous frame (and the admission has already linearized).

---

### 4. Settlement Rejection, Error & Watchdog Timeout Terminal Policy (R8-C / R9-04)

#### Problem Statement
The contract needed explicit definitions of settlement when an asynchronous drain operation is terminally rejected by the engine, throws an unhandled error, or hangs indefinitely.

#### Architectural Specification
1. **Definition of Settlement:**
   Settlement occurs when the drain promise settles (either resolves with a `DestinationAcceptanceReceipt` or rejects with an error).
2. **Terminal Rejection & Error Handling:**
   - When `admitDrainedOperation()` returns `TERMINALLY_REJECTED` (e.g. `STALE_ATTEMPT_FENCED`, `VALIDATION_FAILED`) or throws, the drainer marks the record `STATUS = TERMINALLY_REJECTED`, safely retains the operation in `QuarantineBuffer` custody (or archives it with error diagnostics), and releases the in-flight slot:
     ```typescript
     this._inFlightRecord = null;
     ```
   - In-flight concurrency returns to 0 ($N_{\text{in-flight}} = 0$).
3. **Settlement Barrier Watchdog Timeout:**
   To guarantee bounded recovery and prevent deadlock from an indefinitely hung drain operation, the settlement barrier is guarded by a watchdog timer (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`):
   - If the drain step does not settle within 5 seconds, the coordinator transitions fail-closed to `DEGRADED` (`DRAIN_HANG_TIMEOUT`).
   - The coordinator does **not** forcefully overwrite the in-flight slot while an unknown async operation is pending.
   - The engine locks notification deferral and halts operational progress.
   - Mandatory attempt-ID fencing guarantees that even if the hung operation completes later, it will be rejected as stale with zero state mutation.

---

### 5. Acceptance Linearization Point & Treatment of Prior Accepted Operations (R8-F / R9-05)

#### Problem Statement
The specification needed to define the exact linearization point of admission and clarify how operations accepted prior to an invalidation are handled under both reset strategies.

#### Architectural Specification
1. **Exact Linearization Point:**
   The exact linearization point of admission is the synchronous mutation of `this.core` (operation log append and replication vector advance) within `admitDrainedOperation()`.
2. **Handling Under `PURGE_AND_RETRY`:**
   - Operations that committed to `OrderedOperationLog` before a reset occurred have already linearized and are part of the engine's authoritative history.
   - They are **not** retroactively undone or rolled back.
   - When the new attempt re-drains or transfers a snapshot, previously admitted operations are evaluated against the current log and frontier:
     - If the operation is covered by the current replication vector: categorized as Category 1 (`ALREADY_REPRESENTED`) and discarded without re-execution.
     - If the operation is an exact duplicate in the log: categorized as Category 5 (`ALREADY_OWNED_DUPLICATE`) and discarded.
3. **Handling Under `REINITIALIZE_CLEAN`:**
   - Step 2 of `REINITIALIZE_CLEAN` invokes `engine.replaceCoreStateAtomic(cleanBaseline, capability)`, replacing the entire core state root.
   - All prior log entries, vector components, and state branches are cleanly replaced with the clean baseline anchor.
4. **Stale Continuation Zero-Mutation Invariant:**
   The invariant that an invalidated attempt performs zero state mutation applies strictly to **future operations**: after an attempt is invalidated, its continuation cannot perform any *new* destination acceptance or state mutation.

---

### 6. Exact REINITIALIZE_CLEAN Notification Ordering (R8-D / R9-06)

#### Problem Statement
The specification required reconciliation with the accepted M4.3c 5-step sequence, ensuring that no notification is ever delivered before the coordinator reaches `ACTIVE`, and defining the lifecycle of stale deferred notifications.

#### Source Audit & Architectural Sequence
Inspecting `packages/sync-engine/src/engine/leaderless-sync-engine.ts:929-960` and the accepted M4.3c sequence confirms the exact 5-step order:

```
Step 1: Stale Notification Purge
  Coordinator/Engine purges all deferred notifications accumulated during failed attempts:
  this.deferredStateChanges.length = 0;
  this.deferredDecisions.length = 0;

Step 2: Clean Baseline Installation
  Atomically replace core state root with verified clean baseline anchor:
  engine.replaceCoreStateAtomic(cleanBaseline, capability);

Step 3: Disengage Recovery Mode & Synthesize Clean Baseline Notification
  Invoke engine.setRecoveryMode(false).
  Operational notification deferral is disengaged.
  Clean baseline installation synthesizes exactly ONE coalesced PlaybackState
  change notification representing the clean genesis state into deferredStateChanges.
  (Stale notifications purged in Step 1 cannot leak into this queue).

Step 4: Transition Coordinator to ACTIVE
  Invoke stateMachine.transition('ACTIVE', ...).
  Lifecycle state is now ACTIVE. External observer boundary is crossed.

Step 5: Flush Clean Baseline Notification
  Invoke engine.flushDeferredNotifications() via non-throwing dispatch.
  Delivers clean baseline notification to registered listeners with isolated try/catch.
```

- **Irreversible Observer Boundary:** No notification is EVER delivered to external listeners before Step 4 reaches `ACTIVE`.
- **Pre-ACTIVE Failure Containment:** If any failure occurs in Steps 1–3, `setRecoveryMode(true)` is restored, deferred notifications are never flushed, and the state machine halts in `DEGRADED`.

---

### 7. Precise Package Export Boundary Qualification (R8-E / R9-07)

#### Problem Statement
The specification's description of capability confinement via `package.json` required qualification to avoid overclaiming OS-level file permission boundaries.

#### Source Audit & Architectural Qualification
- **Inspection of `packages/sync-engine/package.json`:**
  ```json
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js",
      "default": "./dist/index.js"
    }
  }
  ```
- **Boundary Qualification:**
  The `package.json` export map strictly exposes root `.`. There are zero subpath exports.
  - Any supported package-specifier import targeting internal custody modules (e.g. `import ... from '@syncplay/sync-engine/recovery/drain-admission-custody'`) throws `ERR_PACKAGE_PATH_NOT_EXPORTED` at runtime and fails TypeScript compilation.
  - This is an **API package boundary** enforcing architectural encapsulation against external consumers using package imports, not an OS-level security sandbox against an attacker with raw direct filesystem access to `node_modules`.
  - Type-only exports (`export type { ... }`) in `packages/sync-engine/src/index.ts` pose zero runtime security risk because the engine validates exact object reference identity (`capability === this.authorizedRecoveryCapability`) against runtime instances retained privately at the composition root.

---

## Verification & Baseline Audit

### Document Edits Summary
| Document | Sections Modified | Purpose |
|---|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Status line, 2.1.2, 2.1.3, 2.1.4, 2.2.1, 2.2.2, 2.4.1 | Formalized shared settlement barrier, mandatory attempt fencing, synchronous indivisibility, timeout policy, linearization point, clean notification sequence, and package boundary. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Status line, Section 1 (`admitDrainedOperation`), Section 2 (`QuarantineDrainer`, `RecoveryCoordinator`), Section 3 | Enforced mandatory `attemptId: number`, documented shared barrier, attempt re-validation, watchdog timeout, and 5-step clean notification sequence. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Status line, M5.1, M5.2, M5.3, M5.5 | Added implementation tasks and verification deliverables for R9 findings. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Status line, AC-M5-01, AC-M5-27 | Updated criteria to enforce concurrent reset serialization, mandatory attempt fencing, and exact 5-step sequence. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Status line, R-M5-06, R-M5-08, R-M5-10 | Added mitigations and controls for concurrent reset races, timeout fail-closed policy, and acceptance linearization. |

---

## Conclusion & Readiness Declaration

All findings from the M5.0-R8 review have been resolved with mathematical precision, grounded in the accepted TypeScript engine implementation and Node.js/JavaScript run-to-completion concurrency semantics.

- Milestone M5.0-R9 is **COMPLETE**.
- Milestone M5.1 implementation remains **UNAUTHORIZED AND UNSTARTED**.
- The M5.0-R9 documentation baseline is ready for independent review and formal baseline freeze.
