# Phase 7.5 Sub-Milestone M5.0-R10 Correction Report: Final Operation Custody, Watchdog Settlement Independence, Admission Commit Atomicity & Baseline Notification Reconciliation

**Target:** Phase 7.5 Sub-Milestone M5.0-R10
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Baseline Commit (M5.0-R9):** `c59a2b192b4dc5b471bed2a26d59cfae3d7fa939`
**Direct Parent of Baseline:** `d2a50c8b80befcaa0f2b388f4f700a7f779e47ea` (M5.0-R8)
**Authoritative Accepted Implementation Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## Executive Summary & Scope Verification

This document records the architectural resolutions for Sub-Milestone M5.0-R10 in response to the Engineering Authority's review of M5.0-R9 ("Documentation scope: PASS, as reported / Technical acceptance: CONDITIONAL / Milestone M5.1: NOT AUTHORIZED").

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
  and this correction report `docs/PHASE7_5_M5_0_R10_CORRECTION_REPORT.md`.

---

## Detailed Resolutions of Review Findings

### 1. Operation Custody After Rejection & Acknowledged Transfer (R9-A / R10-01)

#### Problem Statement
In M5.0-R9, the specification stated that an operation that fails or is rejected is "safely retains the operation in QuarantineBuffer custody (or archives it with error diagnostics)". The conjunction "or" left two competing ownership destinations. Furthermore, if the drainer releases its in-flight slot (`this._inFlightRecord = null`) upon non-acceptance, an operation could disappear into an un-tracked ownership void between the in-flight record and quarantine buffer.

#### Architectural Specification & Source Mapping
1. **Elimination of Implementation Ambiguity:**
   Operation custody following non-acceptance is governed by a strict, deterministic ownership-transfer contract. Operation retention is never left as an implementation choice.

2. **Deterministic Custody Destinations:**
   - **Retryable Destination Backpressure (`CAPACITY_REJECTED`):**
     - Ownership destination: Transferred back to `QuarantineBuffer` custody via `quarantineBuffer.unshiftDrain(op)`.
     - In-flight release: `this._inFlightRecord = null` occurs strictly **after** `unshiftDrain()` succeeds and is acknowledged.
     - Drain loop behavior: The drain pass halts with `HALTED_CAPACITY`, and the coordinator transitions safely to `DEGRADED`.
   - **Stale-Attempt Invalidation (`TERMINALLY_REJECTED` with error `'STALE_ATTEMPT_FENCED'`):**
     - Ownership destination: The operation was rejected solely because the active recovery attempt was superseded or invalid; the operation's payload was not evaluated and remains uncommitted.
     - Custody transfer: Restored to `QuarantineBuffer` custody via `quarantineBuffer.unshiftDrain(op)`.
     - In-flight release: `this._inFlightRecord = null` occurs strictly **after** `unshiftDrain()` succeeds. The operation will be re-evaluated by the drainer during the subsequent authorized recovery attempt.
   - **Permanent Validation Failure (`TERMINALLY_REJECTED` with errors such as `'MISSING_LOGICAL_EVENT_ID'`, `'OBSOLETE_EPOCH'`, `'MALFORMED_PAYLOAD'`):**
     - Ownership destination: The operation is permanently invalid and can never be admitted into the authoritative log.
     - Custody transfer: Transferred to an explicitly defined, bounded `TerminalQuarantineArchive` (diagnostic dead-letter archive owned by the coordinator, capped at `MAX_TERMINAL_ARCHIVE_SIZE = 500` entries) with recorded failure reason, attempt ID, originating peer, and timestamp.
     - In-flight release: `this._inFlightRecord = null` occurs strictly **after** `archive()` succeeds.
     - Auditability: Preserves forensic diagnosability and byte-accounting integrity without leaking memory.
   - **Post-Dequeue Destination Exception (Destination throws an unhandled Error):**
     - In accordance with the accepted M4.3c baseline (`quarantine-drainer.ts:310-315`), `this._inFlightRecord` is **retained** ($N_{\text{in-flight}} = 1$), drain halts immediately with `HALTED_ERROR`, and coordinator enters `DEGRADED`.

3. **Acknowledged Transfer Invariant:**
   Before releasing the in-flight slot (`this._inFlightRecord = null`), the target custody transfer (to `QuarantineBuffer` or `TerminalQuarantineArchive`) must complete and be acknowledged. If the custody transfer throws or fails, the record remains owned, and the coordinator immediately halts in `DEGRADED`. Zero operations can ever be lost or left in ambiguous ownership.

---

### 2. Watchdog Timeout vs. Settlement vs. Ownership Release (R9-B / R10-02)

#### Problem Statement
A 5-second watchdog timeout does not cancel a JavaScript Promise and does not prove that the underlying asynchronous work has stopped. The specification needed to explicitly decouple watchdog timeout from actual operation settlement and in-flight ownership release, and guarantee that all side effects of an invalidated operation are suppressed.

#### Architectural Specification
The architecture explicitly defines three strictly independent events:

1. **Event 1: Watchdog Timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expired):**
   - Trigger: The settlement barrier timer fires because an asynchronous drain step has not settled within 5,000 ms.
   - Action: The coordinator stops waiting for the settlement barrier and transitions fail-closed to `DEGRADED` (`DRAIN_HANG_TIMEOUT`).
   - Engine state: Notification deferral remains locked (`engine.setRecoveryMode(true)`); live inputs remain rejected.
   - **Critical Invariant:** The watchdog timeout does NOT cancel the underlying Promise (which is impossible under JavaScript semantics) and does NOT forcefully clear or overwrite the in-flight slot.

2. **Event 2: Underlying Operation Settlement:**
   - The asynchronous operation actually finishes execution (either resolves or rejects) on the event loop.
   - Attempt Fencing: When `admitDrainedOperation` executes, mandatory attempt-ID fencing validates `attemptId === coordinator.currentAttemptId`. Because the attempt timed out and degraded, the attempt ID is stale, and the operation is terminally rejected with `STALE_ATTEMPT_FENCED` with **zero engine state mutation**.
   - Side-Effect Suppression: Any other side effects in the asynchronous drain task (network broadcasts, peer responses, timers) are guarded by `isAttemptValid(attemptId)` and suppressed, rendering late completion completely inert.

3. **Event 3: Ownership Release & Custody Resolution:**
   - Occurs strictly **after** Event 2 completes settlement and custody is transferred and acknowledged (as defined in R10-01).
   - Only then is `this._inFlightRecord = null` set ($N_{\text{in-flight}} \to 0$), and the drain settlement barrier resolved.

- **Drain Settlement Barrier Invariant:**
  After a watchdog timeout, the in-flight slot remains owned until actual settlement. No new drain pass can EVER begin while an operation from an earlier attempt remains unsettled.

---

### 3. Admission Linearization Point, Candidate Root Replacement & Property Alignment (R9-C / R10-03)

#### Problem Statement
If admission updates log append, replication vector, clock manager, and playback state sequentially in place, JavaScript run-to-completion prevents interleaving from other tasks but does not provide exception atomicity. An exception thrown halfway through would corrupt the engine core state. Additionally, references to `coordinator.recoveryState` needed to be reconciled with the accepted code.

#### Source Audit & Verification
- **Source Inspection of `packages/sync-engine/src/recovery/recovery-coordinator.ts:111,237-239`:**
  ```typescript
  public get state(): RecoveryState {
    return this.stateMachine.state;
  }
  ```
  The authoritative property on `IRecoveryCoordinator` and `RecoveryCoordinator` is strictly `coordinator.state` (of type `RecoveryState`). The ambiguous reference `coordinator.recoveryState` is eliminated across all documentation.
- **Source Inspection of `packages/sync-engine/src/recovery/engine-core-state.ts`:**
  `EngineCoreState` encapsulates all 12 authoritative fields into a single sealed container.

#### Architectural Specification: Candidate Staging & Single Root Swap
Admission avoids separate mutable updates across log, replication vector, clock, and playback state. Instead, it enforces absolute exception atomicity via a two-phase commit:

1. **Phase 1: Validation & Candidate Staging (Side-Effect Free):**
   - Validates epoch fencing, base snapshot frontier, replication vector, duplicate checks, causal dependencies, and Lamport clock validity.
   - Performs SMR conflict reduction dry-run.
   - Assembles a complete candidate `EngineCoreState` container encapsulating the appended log, advanced replication vector, merged causal context, next playback state, `lastAppliedEventId`, and `stateVersion`.
   - All operations in Phase 1 are side-effect free relative to authoritative engine state. If any validation fails or an exception is thrown, `this.core` is completely untouched.

2. **Phase 2: Atomic Linearization Point:**
   - The exact linearization point of admission is the **single synchronous root pointer replacement**:
     ```typescript
     this.core = candidateCore;
     ```
   - Under JavaScript run-to-completion semantics, this single assignment commits all 12 core state fields simultaneously in 1 call frame with zero intermediate partial mutation.

3. **Receipt Shape Alignment:**
   Matches `DestinationAcceptanceReceipt` in `quarantine-drainer.ts:65-72`:
   ```typescript
   export type DestinationAcceptanceReceipt =
     | { readonly status: 'ACCEPTED_REDUCED'; readonly vectorAdvanced: true; readonly newSequence: number }
     | { readonly status: 'ACCEPTED_PENDING'; readonly vectorAdvanced: false; readonly reason: 'MISSING_DEPENDENCY' }
     | { readonly status: 'ALREADY_REPRESENTED'; readonly vectorAdvanced: false }
     | { readonly status: 'ALREADY_OWNED_DUPLICATE'; readonly vectorAdvanced: false }
     | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string; readonly reason?: string; readonly operationId?: string }
     | { readonly status: 'CAPACITY_REJECTED'; readonly vectorAdvanced: false; readonly currentQueueSize: number; readonly maxCapacity: number };
   ```

4. **Treatment of Prior Accepted Operations:**
   - Under `PURGE_AND_RETRY`: Operations accepted into the log before a reset have already linearized and remain in the log (safely categorized as `ALREADY_REPRESENTED` or `ALREADY_OWNED_DUPLICATE` upon retry).
   - Under `REINITIALIZE_CLEAN`: Step 2 atomically replaces the entire root pointer with the clean baseline anchor. Stale continuations perform zero new state mutations.

---

### 4. Reconciliation of REINITIALIZE_CLEAN Notification Contract (R9-D / R10-04)

#### Problem Statement
The claim in M5.0-R9 that clean baseline installation automatically synthesizes "exactly ONE notification" was an unsupported assertion not demonstrated by the accepted engine implementation.

#### Source Audit & Verification
- **Source Inspection of `packages/sync-engine/src/engine/leaderless-sync-engine.ts:883-960`:**
  - `replaceCoreStateAtomic` assigns `this.core = candidate;`. It does NOT touch `this.deferredStateChanges`.
  - `setRecoveryMode(false)` sets `this.recoveryActive = false;`. It does NOT touch `this.deferredStateChanges`.
  - `flushDeferredNotifications()` coalesces whatever notifications are enqueued in `this.deferredStateChanges` and dispatches them to registered listeners.

#### Precise, Grounded Architectural Contract
We remove the unsupported claim and establish the strict, verifiable contract recommended by the Engineering Authority:

1. **Step 1: Stale Deferred Notification Purge:**
   Before installing the clean baseline, all stale deferred notifications accumulated during prior attempts are purged:
   ```typescript
   this.deferredStateChanges.length = 0;
   this.deferredDecisions.length = 0;
   ```
2. **Step 2: Clean Baseline Installation:**
   Atomically replace core state root with verified clean baseline anchor via `engine.replaceCoreStateAtomic(cleanBaseline, capability)`.
3. **Step 3: Disengage Recovery Mode:**
   Invoke `engine.setRecoveryMode(false)`. Operational notification deferral is disengaged. Any notification generated by the clean baseline installation may be retained; no stale recovery-attempt notification may be delivered.
4. **Step 4: Transition Coordinator to ACTIVE:**
   Invoke `stateMachine.transition('ACTIVE', ...)`. Lifecycle state is now `ACTIVE`.
5. **Step 5: Flush Notifications:**
   Invoke `engine.flushDeferredNotifications()` via non-throwing dispatch with isolated listener try/catch. Any resulting clean-baseline notification is delivered only after `coordinator.state === 'ACTIVE'` and recovery mode is `false`.

- **Irreversible Observer Boundary:** No notification is EVER delivered to external listeners before Step 4 reaches `ACTIVE`. If pre-ACTIVE steps fail, recovery mode is restored, deferred notifications are never flushed, and the engine halts in `DEGRADED`.

---

## Verification & Baseline Audit

### Document Edits Summary
| Document | Sections Modified | Purpose |
|---|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Status line, 2.1.2, 3.4 (`PURGE_AND_RETRY`, `REINITIALIZE_CLEAN`), 6.2 | Formalized unambiguous custody transfer, `TerminalQuarantineArchive`, 3-event watchdog settlement independence, candidate staging with single root swap commit point, and grounded notification contract. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Status line, Section 2.2 (`DestinationAcceptanceReceipt`, `admitDrainedOperation`), Section 3.1 (`IRuntimeRecoveryCoordinator`, `resetRecovery`), Section 3.2.1 | Added `ITerminalQuarantineArchive` / `TerminalQuarantineEntry`, aligned receipt shape with source baseline, reconciled `coordinator.state`, and documented two-phase candidate staging. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Status line, M5.0, M5.1, M5.5 | Added implementation tasks and verification deliverables for R10 custody resolution, watchdog independence, and candidate staging. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Status line, AC-M5-01, AC-M5-27 | Updated criteria to enforce acknowledged custody transfer, 3-event watchdog semantics, candidate staging with atomic root swap, and grounded notification contract. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Status line, R-M5-06, R-M5-08, R-M5-10 | Added mitigations and controls for unambiguous custody transfer, watchdog settlement independence, and candidate staging exception atomicity. |

---

## Conclusion & Readiness Declaration

All four findings from the M5.0-R9 review have been resolved with mathematical precision, grounded directly in the accepted TypeScript engine implementation, `EngineCoreState` container architecture, and Node.js/JavaScript run-to-completion concurrency semantics.

- Milestone M5.0-R10 is **COMPLETE**.
- Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED**.
- The M5.0-R10 documentation baseline is fully consistent and ready for final architecture freeze review.
