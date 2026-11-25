# Phase 7.5 Sub-Milestone M5.0-R11 Correction Report: Final Semantic Closure, Candidate Isolation & Reset Custody Reconciliation

**Target:** Phase 7.5 Sub-Milestone M5.0-R11
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Starting Baseline Commit (M5.0-R10):** `2cbcb4064f20fe562db3777ec7440b405158d3bc`
**Direct Parent of Baseline Commit:** `c59a2b192b4dc5b471bed2a26d59cfae3d7fa939` (M5.0-R9)
**Authoritative Accepted Implementation Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## 1. Executive Summary & Mandatory Scope Statement

This document records the final semantic closure, candidate isolation, and reset custody reconciliation for Sub-Milestone M5.0-R11 in response to the Engineering Authority's review of M5.0-R10.

In accordance with strict architectural governance:
- **Zero Production Code Modified:** No files in `packages/*/src/**/*.ts` or `packages/*/src/**/*.tsx` were modified.
- **Zero Test Files Modified:** No files in `packages/**/__tests__/**` were modified.
- **Zero Package Manifests / Lockfiles Modified:** `package.json` and `package-lock.json` remain untouched.
- **Zero Configuration Modified:** `tsconfig*.json`, build scripts, and linter configs remain untouched.
- **Zero Frozen Phase 1–7.4 or M4 Baselines Modified:** All prior baselines remain immutable.
- **Documentation Only:** Modifications are strictly restricted to the 5 M5 specifications:
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`
  - `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`
  and this correction report `docs/PHASE7_5_M5_0_R11_CORRECTION_REPORT.md`.

---

## 2. Source Code Baseline Audit & Verification Citations

In accordance with the R11 mandate, all assertions regarding existing implementation behavior are cited with exact file paths and line ranges:

1. **`DestinationAcceptanceReceipt` & Drainer Lifecycle:**
   - *Source File:* `packages/sync-engine/src/recovery/quarantine-drainer.ts:65-72`
   - *Current Implementation:* Defines 6 variants (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, `CAPACITY_REJECTED`).
   - *Audit Finding:* `ATTEMPT_INVALIDATED` does NOT exist in the M4.3c baseline. It is formally specified as a **Proposed M5 Interface Extension** to decouple attempt fencing from permanent payload validation failure.
2. **`replaceCoreStateAtomic` & Core Replacement:**
   - *Source File:* `packages/sync-engine/src/engine/leaderless-sync-engine.ts:883-909`
   - *Current Implementation:* Gated by `RecoveryInstallationCapability` reference identity; validates object structure; replaces `this.core = candidate;`.
   - *Audit Finding:* `replaceCoreStateAtomic` does not push to `this.deferredStateChanges` and does not mutate nested structures.
3. **`EngineCoreState` Container & `Object.seal()` Limitation:**
   - *Source File:* `packages/sync-engine/src/recovery/engine-core-state.ts:89-130`
   - *Current Implementation:* Top-level container is sealed with `Object.seal(core)`.
   - *Audit Finding:* `Object.seal()` prevents adding or deleting properties on `core`, but does NOT freeze or prevent mutation of nested objects (`operationLog`, `clockManager`, `pendingQueue`, `replicationVector`, `localCausalContext`). Deep immutability requires candidate reference disjointness (Candidate Isolation).
4. **`admitDrainedOperation` & Causal Validation:**
   - *Source File:* `packages/sync-engine/src/engine/leaderless-sync-engine.ts:977-1050`
   - *Current Implementation:* Completely synchronous; executes epoch fencing, base frontier check, replication vector check, and pending queue insertion or log reduction.
5. **`QuarantineBuffer` Bounds & Envelopes:**
   - *Source File:* `packages/sync-engine/src/recovery/quarantine-buffer.ts:26-50`
   - *Current Implementation:* `MAX_QUARANTINE_OPERATIONS = 1000`, `MAX_QUARANTINE_TOTAL_BYTES = 4194304`, `MAX_QUARANTINE_EVENT_BYTES = 4096`. Generational queue with `shiftDrain()` and `unshiftDrain()`.
6. **`TerminalQuarantineArchive` & `drainSettledBarrier`:**
   - *Source Audit:* Inspected `packages/sync-engine/src/recovery/*`.
   - *Audit Finding:* Neither component exists in the current accepted M4.3d implementation. Both are explicitly specified as **Proposed M5 Architectural Components/Extensions**.
7. **`resetRecovery` & Lifecycle State:**
   - *Source File:* `packages/sync-engine/src/recovery/recovery-coordinator.ts:111,237-239, 668-686`
   - *Current Implementation:* Coordinator exposes `public get state(): RecoveryState`. The method `resetRecovery(initiatedBy: string)` in M4.3 increments `activeAttemptId` and clears timers. The capability-authorized, 2-strategy signature `resetRecovery(capability, resetAction)` is a **Proposed M5.1 Interface**.

---

## 3. R11-A — Separate Attempt Invalidation from Permanent Operation Rejection

### Problem Statement
In M5.0-R10, attempt-fenced operations (`STALE_ATTEMPT_FENCED`) were returned as `TERMINALLY_REJECTED` and then unshifted back into quarantine for retry. This created a semantic contradiction (attempt invalidation is not permanent operation rejection) and introduced fragile string-parsing control flow (`if (receipt.error === 'STALE_ATTEMPT_FENCED')`).

### Architectural Specification & Proposed M5 Interface Extension
M5 introduces `ATTEMPT_INVALIDATED` into `DestinationAcceptanceReceipt` as a distinct typed variant:

```typescript
export type DestinationAcceptanceReceipt =
  | { readonly status: 'ACCEPTED_REDUCED'; readonly vectorAdvanced: true; readonly newSequence: number }
  | { readonly status: 'ACCEPTED_PENDING'; readonly vectorAdvanced: false; readonly reason: 'MISSING_DEPENDENCY' }
  | { readonly status: 'ALREADY_REPRESENTED'; readonly vectorAdvanced: false }
  | { readonly status: 'ALREADY_OWNED_DUPLICATE'; readonly vectorAdvanced: false }
  | {
      readonly status: 'ATTEMPT_INVALIDATED';
      readonly vectorAdvanced: false;
      readonly attemptId: number;
      readonly reason: 'STALE_ATTEMPT_FENCED' | 'COORDINATOR_NOT_RECONCILING';
    } // [PROPOSED M5 INTERFACE EXTENSION]
  | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string; readonly reason?: string; readonly operationId?: string }
  | { readonly status: 'CAPACITY_REJECTED'; readonly vectorAdvanced: false; readonly currentQueueSize: number; readonly maxCapacity: number };
```

### Comprehensive Receipt-Outcome Ownership Table

| Receipt Outcome | Status Classification | Vector Advanced? | Destination Core Mutated? | Next Custody Holder | Drainer Next Action |
|---|---|---|---|---|---|
| `ACCEPTED_REDUCED` | Successful Reduction | Yes | Yes (Log + Vector + State) | `EngineCoreState.operationLog` | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ACCEPTED_PENDING` | Missing Dependency | No | Yes (Pending Queue) | `EngineCoreState.pendingQueue` | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ALREADY_REPRESENTED` | Deduplicated (Frontier) | No | No (Covered by base) | None (Safely retired) | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ALREADY_OWNED_DUPLICATE`| Deduplicated (Log) | No | No (Present in log) | None (Safely retired) | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ATTEMPT_INVALIDATED` | Attempt Fenced (M5 New) | No | No (Zero mutation) | `QuarantineBuffer` (head via `unshiftDrain`) | Releases slot after acknowledged transfer; preserves op for next attempt |
| `CAPACITY_REJECTED` | Transient Backpressure | No | No (Zero mutation) | `QuarantineBuffer` (head via `unshiftDrain`) | Releases slot after acknowledged transfer; halts drain (`HALTED_CAPACITY`) |
| `TERMINALLY_REJECTED` | Permanent Validation Failure | No | No (Zero mutation) | `TerminalQuarantineArchive` | Releases slot after acknowledged archive; continues drain |

---

## 4. R11-B — Candidate Isolation & Root Commit Invariants

### Problem Statement
A single root-pointer swap `this.core = candidateCore` does not guarantee exception atomicity if the candidate shares mutable nested objects with the active engine root. Furthermore, `Object.seal()` prevents property additions/deletions on the container object itself, but does NOT prevent mutation of nested collections (`operationLog`, `clockManager`, `pendingQueue`, `replicationVector`, `localCausalContext`).

### Architectural Specification: Candidate Isolation Contract (Required Future M5.1 Invariant)
To guarantee absolute exception atomicity, candidate staging must enforce strict reference disjointness:

1. **Strict Nested Reference Disjointness:** Every mutable nested structure modified or written during candidate staging (`operationLog`, `clockManager`, `pendingQueue`, `replicationVector`, `localCausalContext`) must be a newly allocated instance or a safe copy-on-write clone. No staging function may mutate any object reachable from the current authoritative `this.core`.
2. **Side-Effect Free Phase 1 (Staging):** Causal checks, Lamport validation, and SMR reduction dry-run execute purely against candidate instances. If staging fails or throws, the active `this.core` and every object reachable from it remain 100% pristine.
3. **Pre-Commit Verification & CAS Fencing:** Immediately before replacement, the commit asserts:
   - Attempt identity: `attemptId === this.coordinator.currentAttemptId` and `this.coordinator.state === 'RECONCILING'`.
   - CAS-style root identity check: `this.core === expectedPriorRoot` (verifying no concurrent task replaced the root during staging).
   - Capability authorization: `capability === this.authorizedRecoveryCapability`.
4. **Atomic Linearization Point (Phase 2):** Synchronous root pointer replacement:
   ```typescript
   this.core = candidateCore;
   ```
   Under JavaScript run-to-completion semantics, this single assignment commits all 12 core state fields simultaneously in 1 call frame.
5. **No Fallible Post-Commit Mutations:** No fallible state mutations execute after pointer replacement as part of the logical commit.
6. **Post-Commit Encapsulation:** Candidate structures are encapsulated inside the engine boundary; external SDK callers obtain no writable references.
7. **Strict Boundary Enforcement:** `replaceCoreStateAtomic` is gated by `RecoveryInstallationCapability` checked via exact object reference identity (`===`), preventing unauthorized consumers from replacing `this.core`.

---

## 5. R11-C — Terminal Archive Overflow & Failure Semantics

### Problem Statement
The diagnostic `TerminalQuarantineArchive` has a hard capacity bound of `MAX_TERMINAL_ARCHIVE_SIZE = 500`. The specification needed to define exact behavior when full or upon insertion failure, strictly prohibiting silent eviction.

### Architectural Specification
1. **No Silent Eviction Rule:** The archive NEVER evicts or silently drops entries upon reaching capacity. Silent circular ring overwriting is strictly prohibited to preserve complete forensic evidence.
2. **Custody Transfer Failure Invariant:** If `terminalArchive.size >= MAX_TERMINAL_ARCHIVE_SIZE`, or if `terminalArchive.archive(entry)` throws an exception:
   - This is formally classified as an unrecoverable **Custody-Transfer Failure**.
   - The in-flight slot `this._inFlightRecord` is **NOT** released ($N_{\text{in-flight}} = 1$ is retained).
   - The drain pass halts immediately with `HALTED_ERROR` (`HALTED_ARCHIVE_OVERFLOW`).
   - The coordinator transitions fail-closed to `DEGRADED` (`TERMINAL_ARCHIVE_OVERFLOW`).
   - No unbounded secondary queue or silent dropping is permitted as a workaround.
3. **Explicit Capacity Reclamation:** Capacity can be reclaimed strictly through an explicit, capability-authorized administrative export operation:
   ```typescript
   coordinator.exportAndClearTerminalArchive(
     capability: RecoveryInstallationCapability,
   ): readonly TerminalQuarantineEntry[];
   ```
   This exports the diagnostic records for out-of-band audit before clearing the collection.

---

## 6. R11-D — Explicit Reset-Policy Quarantine Fate Contracts

### Problem Statement
The specification required unambiguous separation of quarantine buffer fate under `PURGE_AND_RETRY` versus `REINITIALIZE_CLEAN`.

### Architectural Specification
- **`PURGE_AND_RETRY`:**
  - Operations currently in `QuarantineBuffer` (both active and detached generations) remain in buffer custody to be re-evaluated against the new attempt.
  - An un-linearized in-flight operation from a prior attempt receives `ATTEMPT_INVALIDATED` and is restored to `QuarantineBuffer` head via `unshiftDrain(op)`.
  - Already-linearized operations remain in `operationLog`; upon retry, they are deduplicated as `ALREADY_REPRESENTED` or `ALREADY_OWNED_DUPLICATE`.
  - Draining can resume only after: (1) settlement barrier resolves, (2) in-flight slot is empty ($N_{\text{in-flight}} = 0$), and (3) new attempt enters `RECONCILING`.
- **`REINITIALIZE_CLEAN`:**
  - When clean reinitialization is executed, the entire engine state is reset to a clean baseline anchor. Quarantined operations represent gossip from the previous unanchored session.
  - **Explicit Generational Retirement:** All pending operations in `QuarantineBuffer` are permanently retired to `TerminalQuarantineArchive` (reason: `REINITIALIZE_CLEAN_PURGED`). Pre-reset quarantined operations are **strictly prohibited from replaying into the clean baseline**.
  - **Late In-Flight Arrival:** If an asynchronous drain step from a prior attempt settles post-commit, it is rejected with `ATTEMPT_INVALIDATED` (`STALE_ATTEMPT_FENCED`) with zero state mutation, and is permanently retired.
  - **Settlement Barrier:** `REINITIALIZE_CLEAN` awaits `drainSettledBarrier()` before executing the clean baseline swap.

### Reset-Policy Quarantine Fate Table

| Reset Strategy | In-Flight Un-linearized Operation Fate | QuarantineBuffer Pending Operations Fate | Already-Linearized Log Operations Fate | When Can Draining Resume? |
|---|---|---|---|---|
| **`PURGE_AND_RETRY`** | Receives `ATTEMPT_INVALIDATED`; restored to head of `QuarantineBuffer` via `unshiftDrain(op)` | Retained in buffer custody; re-evaluated against new recovery attempt | Retained in `operationLog`; deduplicated during retry (`ALREADY_REPRESENTED` / `ALREADY_OWNED_DUPLICATE`) | Only after settlement barrier resolves, slot is empty ($N_{\text{in-flight}} = 0$), and new attempt enters `RECONCILING` |
| **`REINITIALIZE_CLEAN`** | If settles post-commit: rejected with `ATTEMPT_INVALIDATED`; permanently retired without state mutation | All pending operations permanently retired to `TerminalQuarantineArchive` (`REINITIALIZE_CLEAN_PURGED`); never replayed into clean baseline | Entire engine root replaced with clean baseline anchor via `replaceCoreStateAtomic`; prior log discarded | Pre-reset quarantine is permanently retired; live operation ingestion begins fresh in `ACTIVE` state |

---

## 7. Notification Contract & Zero-Notification Clarification

### 5-Step M4.3c Completion Order
1. **Step 1: Stale Deferred Notification Purge:** Before installing clean baseline, all stale deferred notifications accumulated during prior attempts are purged:
   ```typescript
   this.deferredStateChanges.length = 0;
   this.deferredDecisions.length = 0;
   ```
2. **Step 2: Clean Baseline Installation:** Atomically replace core state root with verified clean baseline anchor via `engine.replaceCoreStateAtomic(cleanBaseline, capability)`.
3. **Step 3: Disengage Recovery Mode:** Invoke `engine.setRecoveryMode(false)`. Operational notification deferral is disengaged.
4. **Step 4: Transition Coordinator to ACTIVE:** Invoke `stateMachine.transition('ACTIVE', ...)`. Lifecycle state is now `ACTIVE`.
5. **Step 5: Flush Notifications:** Invoke `engine.flushDeferredNotifications()` via non-throwing dispatch with isolated listener try/catch.

### Zero-Notification Clarification (R10-04, R11-D)
In existing accepted source (`leaderless-sync-engine.ts:883-960`), neither `replaceCoreStateAtomic` nor `setRecoveryMode(false)` pushes to `deferredStateChanges`. Therefore, Step 5 may deliver **zero notifications** unless an explicit clean baseline state change notification is specifically enqueued by caller or implementation. No clean baseline notification is guaranteed to exist automatically.

### Observer Boundary Invariant
No notification is EVER delivered to external listeners before Step 4 reaches `ACTIVE` and recovery mode is `false`. If pre-ACTIVE steps fail, recovery mode is restored, deferred notifications are never flushed, and the engine halts in `DEGRADED`.

---

## 8. Verification & Baseline Audit

### Document Edits Summary
| Document | Sections Modified | Purpose |
|---|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Status line, 2.1.2, 3.4 (`PURGE_AND_RETRY`, `REINITIALIZE_CLEAN`), 6.2 | Formalized `ATTEMPT_INVALIDATED`, Receipt-Outcome Ownership Table, Candidate Isolation Contract, Terminal Archive Overflow Fail-Closed, Reset Fate Table, and Zero-Notification Clarification. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Status line, Section 2.2 (`DestinationAcceptanceReceipt`, `admitDrainedOperation`), Section 3.1 (`IRuntimeRecoveryCoordinator`, `resetRecovery`), Section 3.2.1 (`ITerminalQuarantineArchive`) | Added `ATTEMPT_INVALIDATED`, `exportAndClearTerminalArchive`, hard cap 500 without silent eviction, and Candidate Isolation pre-commit CAS check. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Status line, M5.0, M5.1, M5.5 | Added implementation tasks and verification deliverables for R11-A, R11-B, R11-C, R11-D. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Status line, AC-M5-01, AC-M5-27 | Updated criteria to enforce `ATTEMPT_INVALIDATED`, Candidate Isolation with CAS root check, archive overflow fail-closed, and quarantine fate separation. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Status line, R-M5-06, R-M5-08, R-M5-10 | Added mitigations and controls for typed invalidation, Candidate Isolation under `Object.seal()` limits, archive overflow, and clean reinitialization quarantine retirement. |

---

## 9. Conclusion & Final Readiness Declaration

All four findings from the M5.0-R10 review have been resolved with mathematical precision, grounded directly in the accepted TypeScript engine implementation, `EngineCoreState` container architecture, and Node.js/JavaScript run-to-completion concurrency semantics.

- Milestone M5.0-R11 is **COMPLETE**.
- Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED**.
- The M5.0-R11 documentation baseline is fully consistent and ready for final architecture freeze review.
