# Phase 7.5 M4.3c-R1 — Targeted Recovery Fencing, Drain Ownership & Handoff Corrections Report

**Role:** Senior TypeScript Distributed-Systems Engineer  
**Mode:** STRICTLY LIMITED IMPLEMENTATION CORRECTION  
**Milestone:** Phase 7.5 Sub-Milestone M4.3c-R1  
**Audited Starting Baseline:** `227062be23e9dbc952f98896d7675d35673d5740`  
**Approved Architecture Baseline:** `2bb117c5db474a13307be65c6f68845f2bd74720`  
**Frozen Implementation Baseline:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  

---

## 1. Executive Summary

Sub-milestone **M4.3c-R1** resolves all four findings identified by the independent source audit of M4.3c (`227062be23e9dbc952f98896d7675d35673d5740`). The corrections introduce authoritative monotonic recovery attempt fencing, prevent in-flight drain record overwriting, guarantee immediate termination of invalidated quarantine reconciliation loops, and rectify the active handoff sequence to eliminate observer race windows.

All corrections strictly adhere to the approved M4.3 architecture (`2bb117c5db474a13307be65c6f68845f2bd74720`) and preserve the frozen M4.1–M4.2d baseline (`aafa366cc48fdb2b83940f00e80d92d813e7c52e`) with zero diff.

---

## 2. Detailed Audit Findings & Implemented Corrections

### 2.1 FINDING-M4.3c-01 (BLOCKER): Stale Snapshot Staging/Commit Post-Timeout or Abort

- **Root Cause:** In M4.3c, `executeRecoveryWithSnapshot()` initiated asynchronous validation via `await this.snapshotInstaller.validate(...)`. If an overall recovery watchdog timeout fired or an explicit `abortRecovery()` was executed during this validation window, the state machine transitioned to `DEGRADED`. However, upon promise resolution, `executeRecoveryWithSnapshot()` resumed unconditionally, advancing through `STAGING` and `COMMITTING` to invoke `snapshotInstaller.commitAtomic()`, overwriting the engine state root with a stale candidate.
- **Architectural Correction:**
  1. Added a private monotonic attempt counter `activeAttemptId: number` and public accessor `readonly currentAttemptId: number` to `RecoveryCoordinator`.
  2. In `triggerRecovery()`, incremented `activeAttemptId += 1` on each newly accepted recovery attempt.
  3. In `abortRecovery()`, `resetRecovery()`, and `dispose()`, incremented `activeAttemptId += 1` to immediately and permanently invalidate any in-flight attempts.
  4. Introduced `isAttemptValid(attemptId, expectedState)` and `assertAttemptValid(attemptId, expectedState, stage)`.
  5. In `executeRecoveryWithSnapshot()`:
     - Captured `const attemptId = this.activeAttemptId` at start.
     - Enforced `assertAttemptValid(attemptId, 'RECOVERING', ...)` before validation, immediately after validation resolution, in the validation catch handler, before staging, before atomic commit, before the transition to `RECONCILING`, and before active handoff.
     - Any post-invalidation resumption throws a `DISPOSED` / fencing exception and immediately halts execution prior to staging or commit.
- **Adversarial Test Verification:**
  - `R1_F1`: Overall recovery timeout fires during asynchronous validation; validation belatedly resolves: staging and atomic commit are strictly blocked (`stageSpy` and `commitSpy` not called).
  - `R1_F2`: Explicit `abortRecovery()` called during validation; validation resolves: staging and atomic commit are strictly blocked.
  - `R1_F3`: Attempt 1 interrupted, degraded, and Attempt 2 triggered (`activeAttemptId` increments from 1 to 3); Attempt 1's validation belatedly resolves: commit is strictly blocked.

---

### 2.2 FINDING-M4.3c-02 (HIGH): Quarantine Reconciliation Loop Continues Post-Timeout or Invalidation

- **Root Cause:** In M4.3c, `reconcileQuarantine()` was raced against `reconciliationTimeout` using `Promise.race()`. Because `Promise.race` does not cancel underlying promises or loop iterations, when the timeout rejected, the background loop in `reconcileQuarantine()` continued executing `this.quarantineBuffer.swapDrainBatch()` and `this.quarantineDrainer.drainBatch()`, and ultimately executed `this.stateMachine.setSubstate('RESOLVING_GAPS')` on an already degraded coordinator.
- **Architectural Correction:**
  1. Updated `reconcileQuarantine(attemptId: number = this.activeAttemptId)` to bind to a specific attempt ID.
  2. Enforced attempt validity assertions:
     - At entry of `reconcileQuarantine()`.
     - At the start of each iteration in `while (this.quarantineBuffer.size > 0 || this.quarantineBuffer.drainSize > 0)`.
     - Immediately following `await this.quarantineDrainer.drainBatch()`.
     - Before advancing substate to `RESOLVING_GAPS`.
  3. When `handleReconciliationTimeout()` fires, it calls `abortRecovery('RECONCILIATION_TIMEOUT')`, which increments `activeAttemptId` and transitions state to `DEGRADED`.
  4. The in-flight `drainBatch()` promise completes, and the subsequent check immediately detects that the attempt was invalidated, throwing without initiating further swaps or transitioning substate to `RESOLVING_GAPS`.
- **Adversarial Test Verification:**
  - `R1_F4`: Reconciliation timeout fires while a drain batch is in flight; the promise rejects with `SNAPSHOT_INSTALLATION_FAILED`, the coordinator transitions to `DEGRADED`, no subsequent batches are dequeued, and the substate never advances to `RESOLVING_GAPS`.

---

### 2.3 FINDING-M4.3c-03 (HIGH): In-Flight Record Overwrite on Subsequent `drainBatch()` Calls

- **Root Cause:** In `QuarantineDrainer`, if an operation halted on `CAPACITY_REJECTED` or an unexpected destination error, it remained in `_inFlightRecord`. If `drainBatch()` was invoked again on the drainer, it dequeued the next operation and unconditionally overwrote `_inFlightRecord`, violating DRAINER_IN_FLIGHT singular custody and dropping the unconfirmed operation.
- **Architectural Correction:**
  1. In `drainBatch()`, added an entry guard: if `this._inFlightRecord !== null`, inspects `this.destination.hasOperation(peer, seq)`. If the destination already committed it, releases in-flight custody; otherwise calls `this.restoreInFlightToQueue()` (which calls `quarantineBuffer.unshiftDrain(op)`) so the operation is restored to the head of the drain queue and processed first before any newer operation.
  2. In `processSingleOperation()`, added an assertion: throws `SyncEngineError('INVALID_STATE')` if `this._inFlightRecord !== null`.
  3. In `dispose()`, if `this._inFlightRecord !== null`, restores the unresolved in-flight operation to `quarantineBuffer.unshiftDrain(op)` before nulling, ensuring zero data loss upon drainer disposal.
- **Adversarial Test Verification:**
  - `D11`: `drainBatch()` halts on `CAPACITY_REJECTED`, preserving in-flight record. A subsequent `drainBatch()` call restores the in-flight operation to the queue and admits it first before subsequent operations, without overwrite.
  - `D12`: Invoking `processSingleOperation()` while in-flight custody is held throws `INVALID_STATE`.
  - `D13`: Disposing `QuarantineDrainer` while in-flight custody is held unshifts the operation back to the quarantine buffer.

---

### 2.4 FINDING-M4.3c-04 (MEDIUM): Inconsistent Observer Window Due to Inverted `ACTIVE` Handoff Ordering

- **Root Cause:** In M4.3c, `executeRecoveryWithSnapshot()` executed `this.stateMachine.transition('ACTIVE', ...)` *before* calling `this.engine.setRecoveryMode(false)`. Synchronous listeners subscribed to `onStateChange` who observed `newState === 'ACTIVE'` would query `engine.isRecoveryActive()` and receive `true`, exposing an inconsistent recovery boundary.
- **Architectural Correction:**
  1. Rectified the critical section ordering in `executeRecoveryWithSnapshot()` and `resetRecovery()`:
     ```typescript
     this.assertAttemptValid(attemptId, 'RECONCILING', 'pre-active-handoff');
     this.engine.setRecoveryMode(false);
     try {
       this.stateMachine.transition('ACTIVE', 'Recovery and reconciliation successfully completed', 'IDLE');
     } catch (err) {
       this.engine.setRecoveryMode(true);
       throw err;
     }
     this.clearTimers();
     this.engine.flushDeferredNotifications();
     ```
  2. Guarantees that at the exact instant `onStateChange` fires for `ACTIVE`, `engine.isRecoveryActive()` is already `false`.
  3. Guarantees that if state machine transition fails, recovery mode is restored.
  4. Guarantees deferred notifications are flushed only after state machine and recovery mode are mutually consistent.
- **Adversarial Test Verification:**
  - `R1_F5`: Subscribes `onStateChange` callback and records `engine.isRecoveryActive()` when `newState === 'ACTIVE'`. Confirms `engine.isRecoveryActive() === false` inside the callback.

---

## 3. Verification & Compliance Matrix

| Requirement | Target | Result | Evidence |
| :--- | :--- | :--- | :--- |
| **Attempt Fencing** | Block stale commit post-timeout/abort | **PASS** | `R1_F1`, `R1_F2`, `R1_F3` pass |
| **Reconciliation Fencing** | Halt drain loop post-timeout/abort | **PASS** | `R1_F4` passes |
| **Drain Ownership** | In-flight record overwrite prevention | **PASS** | `D11`, `D12`, `D13` pass |
| **Active Handoff Order** | Recovery mode `false` before `ACTIVE` event | **PASS** | `R1_F5` passes |
| **Targeted Test Suite** | `quarantine-drainer.test.ts` & `recovery-coordinator.test.ts` | **PASS** | 42/42 tests passing |
| **Sync Engine Suite** | `@syncplay/sync-engine` vitest run | **PASS** | 44/44 files, 916/916 tests passing |
| **Monorepo Suite** | All 5 workspaces (`syncplay-protocol`) | **PASS** | 60/60 files, 1,401/1,401 tests passing |
| **Monorepo Build** | `tsc` (all packages) + `vite build` (demo) | **PASS** | Clean build, 0 errors |
| **Frozen Files Baseline** | Diff against `aafa366cc48fdb2b83940f00e80d92d813e7c52e` | **PASS** | 0 diff in frozen baseline files |

---

## 4. Git Closure

- **Modified Files:**
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - `packages/sync-engine/src/recovery/quarantine-drainer.ts`
  - `packages/sync-engine/src/__tests__/recovery-coordinator.test.ts`
  - `packages/sync-engine/src/__tests__/quarantine-drainer.test.ts`
  - `docs/PHASE7_5_M4_3C_R1_CORRECTION_REPORT.md`
- **Commit Target:**
  - `fix(phase-7.5-m4): resolve m4.3c recovery attempt fencing, drain ownership, and handoff ordering`
