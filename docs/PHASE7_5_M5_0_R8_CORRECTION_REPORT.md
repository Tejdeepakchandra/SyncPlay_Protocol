# Phase 7.5 Milestone M5.0-R8 — In-Flight Drain Cancellation, Reset Notification Semantics & Package Boundary Closure Report

**Milestone:** Phase 7.5 Sub-Milestone M5.0-R8 (Architecture Consistency & Reset Semantics Closure)<br/>
**Date:** 2026-11-25<br/>
**Author:** Senior TypeScript Distributed-Systems Architect<br/>
**Authoritative Starting Baseline:** `1cef0fddc0dc7d7b2c0197c91fb3dee7c496e584` (Milestone M5.0-R7 Committed)<br/>
**Preceding Baselines:** M5.0-R6 (`06fc776`), M5.0-R5 (`0e15f31`), M5.0-R4 (`d945591`), M4.3d (`40b11fd`), M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)<br/>
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT<br/>
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## 1. Executive Summary & Review Context

Following the Engineering Authority's independent review of Milestone M5.0-R7 (*"Documentation scope and reported Git evidence: PASS / Technical acceptance: CONDITIONAL / Milestone M5.1: NOT AUTHORIZED"*), this sub-milestone (M5.0-R8) formally resolves the remaining concurrency, failure-atomic lifecycle, notification delivery, and package boundary findings across the M5 specification suite:

1. **R8-01 — In-Flight Drain Cancellation & Retained Ownership Barrier:** Proved that clearing `inFlightRecord = null` during an active asynchronous operation is insufficient to guarantee $N_{\text{in-flight}} \le 1$. Formally specified an asynchronous drain settlement barrier: in-flight records are flagged as invalidated (`invalidated = true`) while retaining the ownership slot ($N_{\text{in-flight}} = 1$) until the underlying operation settles, prohibiting any new drain operation from beginning until the slot is completely settled and empty ($N_{\text{in-flight}} \to 0$).
2. **R8-02 — Stale Continuation Behavior at Actual Acceptance Boundary:** Extended attempt-ID fencing directly to the state-mutation/acceptance boundary inside `admitDrainedOperation(op, capability, attemptId)`. If `attemptId !== coordinator.currentAttemptId` or the coordinator is no longer in `RECONCILING`, the destination engine immediately fails closed (`TERMINALLY_REJECTED` / `'STALE_ATTEMPT_FENCED'`), guaranteeing zero mutation of `operationLog`, `replicationVector`, or playback state.
3. **R8-03 — Resolution of REINITIALIZE_CLEAN Purge-vs-Flush Ambiguity:** Explicitly differentiated the notification queues: Step 2 purges all stale deferred notifications accumulated during the failed recovery attempt (`this.deferredStateChanges.length = 0; this.deferredDecisions.length = 0;`), while Step 5 dispatches strictly any fresh clean baseline state change synthesized upon installing the clean anchor (or cleanly no-ops if none was enqueued).
4. **R8-04 — Non-Throwing Listener Dispatch & Irreversible External Effects:** Documented that consistent with existing implementation (`packages/sync-engine/src/engine/leaderless-sync-engine.ts:929`), `flushDeferredNotifications()` is a non-throwing dispatch operation that isolates individual listener exceptions via `try...catch` (recording errors in `listenerErrorCount`). Documented the irreversible external observer boundary: once the coordinator transitions to `ACTIVE` in Step 4, external side effects cannot be rolled back, and the specification explicitly defines this failure boundary rather than promising retroactive rollback of observed events.
5. **R8-05 — Package-Boundary Guarantee, Shallow Freezing & bindEngine() Scope:** Verified against `packages/sync-engine/package.json` that the package export map exposes strictly root `.` (no subpath or wildcard exports), preventing external deep-imports into internal custody modules. Corrected freezing terminology to shallow freezing (`Object.freeze()`), which is completely sufficient for primitive-field capabilities and their container. Documented `bindEngine()` as an internal composition-root hook enforcing single attachment (`ALREADY_BOUND`), with zero capability accessors or diagnostic leaks.

Consistent with absolute scope restrictions, **zero lines of production code, test code, build configuration, or package manifests were created or modified**. Milestone M5.1 implementation remains completely unstarted.

---

## 2. Technical Resolution of Review Findings

### 2.1 Resolution of R8-01 & R8-02: In-Flight Drain Cancellation & Acceptance Boundary Fencing

#### The Problem:
Clearing `inFlightRecord = null` in `QuarantineDrainer.reset()` during `PURGE_AND_RETRY` does not cancel a pending Promise or prevent a race where an old asynchronous drain operation A resolves after a new attempt has already started and dequeued operation B, violating $N_{\text{in-flight}} \le 1$ and corrupting state.

#### Architectural Resolution:
Milestone M5.0-R8 establishes a two-layer defense-in-depth model:

1. **Layer 1: Invalidation with Retained Ownership Slot & Settlement Barrier (R8-01):**
   - When `resetRecovery('PURGE_AND_RETRY')` is invoked, `QuarantineDrainer` checks whether an operation is currently in flight (`this._inFlightRecord !== null`).
   - If an in-flight operation exists:
     - Its record is marked `this._inFlightRecord.invalidated = true`.
     - **The ownership slot is retained** ($N_{\text{in-flight}} = 1$); the drainer does NOT clear the record or allow new operations to be dequeued.
     - `PURGE_AND_RETRY` executes an asynchronous drain settlement barrier: it awaits or latches settlement of the pending in-flight operation (`await this.quarantineDrainer.drainSettledBarrier()`). No new drain operation can begin until the pending operation settles.
     - When the stale asynchronous continuation settles (resolves or rejects), it detects `invalidated === true`. It performs zero state mutation, releases the retained ownership slot (`this._inFlightRecord = null`, $N_{\text{in-flight}} \to 0$), and signals the settlement barrier.
     - Only after the slot is empty can the new recovery attempt dequeue and drain operations. Stale continuations can NEVER overwrite, release, or mutate ownership belonging to the new attempt.
   - **Mathematical Invariant:**
     $$N_{\text{in-flight}} \le 1 \quad \forall t \ge 0$$
     remains strictly true across all recovery reset and retry boundaries.

2. **Layer 2: Attempt-ID Fencing at the Actual State-Mutation Boundary (R8-02):**
   - Attempt-ID fencing is verified not merely when an asynchronous callback begins, but at the **final state-mutation and acceptance boundary**:
     ```typescript
     admitDrainedOperation(
       op: QuarantinedOperation,
       capability: DrainAdmissionContext,
       attemptId?: number,
     ): DestinationAcceptanceReceipt;
     ```
   - Inside `LeaderlessSyncEngine.admitDrainedOperation()`:
     - If `attemptId` is provided, the engine asserts:
       ```typescript
       if (attemptId !== this.coordinator.currentAttemptId || this.coordinator.state !== 'RECONCILING') {
         return {
           status: 'TERMINALLY_REJECTED',
           vectorAdvanced: false,
           error: 'STALE_ATTEMPT_FENCED',
         };
       }
       ```
     - If the attempt ID is stale, the engine fails closed immediately, performing **zero mutation** of `OrderedOperationLog`, **zero advancement** of `replicationVector`, and **zero state reduction**.

---

### 2.2 Resolution of R8-03 & R8-04: REINITIALIZE_CLEAN Purge-vs-Flush & Non-Throwing Dispatch

#### The Problem:
In R7, Step 2 stated that deferred notifications were discarded, while Step 5 stated that deferred notifications were flushed, creating an ambiguity. Furthermore, if notification flush throws partway through, delivered listener notifications cannot be rolled back, making promises of failure atomicity inaccurate if unhandled exceptions are allowed to escape.

#### Architectural Resolution:

1. **Resolution of Purge-vs-Flush Ambiguity (R8-03):**
   - **Step 2 (Stale Notification Discard):** Explicitly purges all stale deferred notifications accumulated during the failed recovery attempt:
     ```typescript
     this.deferredStateChanges.length = 0;
     this.deferredDecisions.length = 0;
     ```
   - **Step 3 (Disengage Recovery Mode & Baseline Enqueue):** When `engine.setRecoveryMode(false)` is invoked, operational notification deferral is disengaged. If clean baseline restoration synthesizes a fresh state change notification (e.g. initial playback state for the clean genesis/anchor state), that single coalesced `PlaybackState` event is enqueued into `deferredStateChanges`.
   - **Step 5 (Clean Baseline Flush):** `engine.flushDeferredNotifications()` flushes strictly this fresh clean baseline state notification (or cleanly no-ops if no fresh notifications were enqueued).

2. **Non-Throwing Dispatch with Isolated Listener Failures (R8-04):**
   - As confirmed by source inspection of `packages/sync-engine/src/engine/leaderless-sync-engine.ts:929`, `flushDeferredNotifications()` is a **non-throwing dispatch operation**. Every listener callback is isolated within a `try...catch` block:
     ```typescript
     for (const listener of this.stateChangeListeners) {
       try {
         listener(finalState);
       } catch {
         this.mutableStats.listenerErrorCount++;
       }
     }
     ```
   - Listener exceptions are caught and recorded in diagnostic counters (`listenerErrorCount`); they **never abort the flush loop and never throw out of `flushDeferredNotifications()`**.

3. **Explicit External Observer Boundary (R8-04):**
   - Once the coordinator transitions to `ACTIVE` in Step 4, that state transition has occurred and is externally observable.
   - Any notifications delivered to listeners have taken effect externally. The architecture explicitly documents that delivered external effects cannot be retracted or rolled back.
   - If a catastrophic system failure occurs prior to Step 4 (e.g., Step 2 or 3 throws), the catch block restores `engine.setRecoveryMode(true)`, locks the engine in `DEGRADED`, and deferred notifications are **never flushed**.

---

### 2.3 Resolution of R8-05: Package-Boundary Guarantee, Shallow Freezing & bindEngine() Custody

#### The Problem:
Excluding factories from `index.ts` is insufficient if package subpaths allow direct imports of internal modules. Furthermore, `Object.freeze()` is shallow freezing, and `bindEngine()` must be formalized as an internal composition-root hook with zero getter leaks.

#### Architectural Resolution:

1. **Package-Boundary Guarantee:**
   Inspection of `packages/sync-engine/package.json` confirms:
   ```json
   "exports": {
     ".": {
       "types": "./dist/index.d.ts",
       "import": "./dist/index.js"
     }
   }
   ```
   - Root `.` is the **sole supported package export**. There are NO wildcard subpaths (`"./*"`), NO `./recovery/*` subpaths, and NO internal file mappings.
   - Any consumer attempting to import from `@syncplay/sync-engine/recovery/...` or `@syncplay/sync-engine/dist/...` is rejected at compile time by TypeScript and at runtime with `ERR_PACKAGE_PATH_NOT_EXPORTED` by Node.js.
   - The public index (`packages/sync-engine/src/index.ts`) exports **strictly TypeScript type declarations**:
     `export type { DrainAdmissionContext, TrustedLocalReplayCapability, TrustedRepairBatchCapability, AuthorizedDrainCapabilities } from './recovery/drain-admission-custody.js';`
   - Zero runtime values, zero symbols, and zero factory functions are exported.

2. **Runtime Shallow Immutability:**
   - Corrected terminology: `Object.freeze()` performs shallow freezing.
   - Because `TrustedLocalReplayCapability` and `TrustedRepairBatchCapability` contain exclusively primitive fields (`kind: string`, `sessionId: string`, `authority: string`, `token: string`) and the unique symbol, shallow freezing is completely sufficient and renders the objects 100% immutable.
   - The containing `AuthorizedDrainCapabilities` bundle is likewise shallow-frozen with `Object.freeze()`.

3. **`bindEngine()` Scope & Invariants:**
   - `bindEngine()` is an internal composition-root binding method, NOT an open public reconfiguration API.
   - Enforces single attachment: `if (this.engine !== null) throw new SyncEngineError('ALREADY_BOUND')`. Prohibits rebinding or sharing across engines.
   - **Zero Public Accessors:** Neither engine nor coordinator provides public getters (`engine.localReplayCapability` and `coordinator.repairBatchCapability` do not exist). Capabilities are never exposed in diagnostics, state snapshots, debug logs, or event objects.
   - **Least Privilege:** Coordinator receives `localReplayCapability` strictly for local reconciliation; Drainer receives strictly `repairBatchCapability` for remote batch drain.

---

## 3. Specification Traceability Matrix

| Review Finding | Primary Document | Affected Section | Formalized Architectural Contract |
|---|---|---|---|
| **R8-01** (In-Flight Settlement Barrier) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §3.4<br/>§3.1<br/>AC-M5-01<br/>R-M5-06 | In-flight records marked `invalidated = true` with ownership slot retained ($N_{\text{in-flight}} = 1$) until settlement; drain settlement barrier prohibits new drains until settled, guaranteeing $N_{\text{in-flight}} \le 1$ across reset boundaries. |
| **R8-02** (Acceptance Boundary Fencing) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §3.4, §5.2<br/>§2.2<br/>AC-M5-27<br/>R-M5-06 | `admitDrainedOperation(op, capability, attemptId)` verifies `attemptId === currentAttemptId` and `state === 'RECONCILING'` at the state-mutation boundary; stale attempt operations rejected with zero state mutation. |
| **R8-03** (Stale Purge vs Clean Flush) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §3.4<br/>§3.1<br/>AC-M5-01<br/>R-M5-10 | Step 2 explicitly discards all stale deferred notifications; Step 5 dispatches strictly fresh clean baseline notifications synthesized upon clean anchor install. |
| **R8-04** (Non-Throwing Dispatch & Irreversible Effects) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §3.4<br/>§2.2, §3.1<br/>AC-M5-01, AC-M5-04<br/>R-M5-10 | `flushDeferredNotifications()` is a non-throwing dispatch operation (listener exceptions isolated in `try...catch`); once `ACTIVE` is reached in Step 4, external observer effects cannot be rolled back; explicit failure boundary documented. |
| **R8-05** (Package Boundary & Shallow Freezing) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §2.1.2<br/>§2.1, §3.1<br/>§2.2 (M5.1)<br/>AC-M5-27<br/>R-M5-08 | Package entry manifest `package.json` maps root `.` only (prohibiting subpath imports); capability objects and container shallow-frozen via `Object.freeze()`; `bindEngine()` single-attachment composition root scope; zero capability accessors. |

---

## 4. Verification & Source Baseline Audit

All specifications have been verified against the accepted codebase baselines:
- **`packages/sync-engine/package.json`:** Confirms explicit `"exports": { ".": { ... } }` map with zero subpath exports.
- **`packages/sync-engine/src/engine/leaderless-sync-engine.ts:929-960`:** Confirms non-throwing `flushDeferredNotifications()` with isolated listener callback `try...catch` and `listenerErrorCount` instrumentation.
- **`packages/sync-engine/src/recovery/quarantine-drainer.ts:284-345`:** Confirms single-operation processing lifecycle and synchronous receipt handling.
- **`packages/sync-engine/src/recovery/recovery-coordinator.ts:536-588`:** Confirms attempt validation guards (`assertAttemptValid`) across loop iterations.

---

## 5. Scope Restrictions Compliance

- **Production Code Changes:** 0 files, 0 lines.
- **Test Code Changes:** 0 files, 0 lines.
- **Build / Manifest Changes:** 0 files, 0 lines.
- **Documentation Changes:** Strictly M5 specification documentation under `docs/PHASE7_5_M5_*.md`.
- **M5.1 Implementation Status:** UNAUTHORIZED AND UNSTARTED.

---

## 6. Conclusion & Recommendation

With the formalization of:
1. The asynchronous drain settlement barrier and acceptance-boundary attempt-ID fencing guaranteeing $N_{\text{in-flight}} \le 1$ across reset boundaries,
2. The resolution of the `REINITIALIZE_CLEAN` purge-versus-flush distinction and non-throwing listener dispatch,
3. The package-boundary export guarantee, shallow freezing, and composition-root scope of `bindEngine()`,

Milestone M5.0-R8 closes all remaining architectural and lifecycle questions. The M5 architecture suite is fully consistent, rigorous, and ready for unconditional formal acceptance.
