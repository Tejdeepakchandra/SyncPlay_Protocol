# Phase 7.5 M5.1 — Stage 0-R1: Implementation Readiness, Settlement Barrier Semantics & Source Integration Plan

**Sub-Milestone Title:** Engine Runtime Recovery Integration — Stage 0-R1 Implementation Readiness
**Role:** Principal TypeScript Distributed-Systems Architect, Senior Recovery-Systems Engineer, and Independent Implementation Auditor
**Mode:** STRICTLY READ-ONLY SOURCE AUDIT, IMPLEMENTATION READINESS ANALYSIS, AND EXECUTION PLANNING
**Target Milestone:** Phase 7.5 Sub-Milestone M5.1 (Stage 0-R1 Correction)
**Revision:** Stage 0-R1 (Settlement Barrier Semantics & Watchdog Separation Correction)
**Starting Authoritative Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Direct Baselines:**
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`

**M5.0 Final Freeze Commit:** `b654cd2e5c38440f6c26ff0d827a6fdc9652c378`
**Direct Parent of M5.0 Freeze (M5.0-R17 Commit):** `e16b38ea08b340d9945f069f4a41b4fc80958d0c`
**Initial Stage 0 Commit:** `260647707edbdc0b0faa78b05cf870424330583e`

**Implementation Authorization Status: Milestone M5.1 remains STRICTLY UNAUTHORIZED AND UNSTARTED.**

---

## 1. Executive Summary & R1 Corrections

Milestone M5.0 is formally accepted and frozen. This Stage 0-R1 report refines the implementation readiness analysis by addressing the core review finding regarding **settlement barrier semantics and watchdog separation**:

1. **Mandatory Correction A — Separation of Settlement from Watchdog Timeout:**
   - The settlement barrier (`drainSettledBarrier(): Promise<void>`) represents the **actual completion** of all owned in-flight drain work.
   - It resolves strictly and exclusively when the underlying drain operation has settled and the drainer asserts `this._inFlightRecord === null && this._isDraining === false`.
   - The settlement barrier must **NEVER reject, resolve, or otherwise become settled merely because a watchdog deadline expires**.
   - The watchdog timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) is a coordinator-level attempt-invalidation mechanism: it latches the current attempt status to `TIMED_OUT` and aborts clean reset to `DEGRADED`. It does NOT cancel the underlying Promise, clear `_inFlightRecord`, force `_isDraining` to false, release operation custody, or allow a subsequent attempt to bypass the barrier.
2. **Mandatory Correction B — Independent State Transitions for Timeout and Settlement:**
   - Watchdog Timeout and Actual Promise Settlement are modeled as two distinct, independent state transitions.
   - Even after a timeout occurs, the underlying Promise continues running until actual settlement; its continuation is fenced against stale attempts (`ATTEMPT_INVALIDATED`), and ownership is released only upon actual receipt evaluation.
   - Concurrent or subsequent reset attempts share and await the same outstanding barrier; they cannot create a second independent owner for the in-flight operation.
3. **Mandatory Correction C — Deterministic 17-Step Test Plan:**
   - Codifies an explicit 17-step deterministic test verifying that in-flight custody is retained past the 5,000 ms watchdog timeout, concurrent resets are serialized on the barrier, and custody is cleared only upon actual settlement.
4. **Mandatory Correction D — Monotonic Elapsed Duration Wording:**
   - Formally clarifies that `performance.now()` measures monotonic elapsed time, not wall-clock time, and documents browser timer throttling characteristics.
5. **Mandatory Correction E — Preserving Testability Without Exposing Private State:**
   - Asserts that unit and failure-injection tests verify observable all-or-nothing behavior and defensive cloning without leaking backing Maps, core roots, or private byte arrays via public test APIs.

---

## 2. Mandatory First Action: Repository and Baseline Verification

An inspection of the local Git repository reveals:

1. **Current Branch:** `main`
2. **Starting Baseline Commit for R1:** `260647707edbdc0b0faa78b05cf870424330583e` (Initial Stage 0 Report)
3. **Working-Tree Status:** Clean (`nothing to commit, working tree clean`).
4. **M5.0 Final Acceptance Commit in History:** Confirmed (`b654cd2e5c38440f6c26ff0d827a6fdc9652c378`).
5. **M5.0-R17 Commit in History:** Confirmed (`e16b38ea08b340d9945f069f4a41b4fc80958d0c`).
6. **M4.3d Implementation Baseline in History:** Confirmed (`40b11fdafcd247c7a289723f90ec0e2029c94f64`).
7. **Lineage Relationship:**
   ```
   2606477 (Initial Stage 0) docs(phase-7.5-m5.1): stage 0 implementation readiness and source integration plan
   │
   b654cd2 (M5.0 Final Freeze) docs(phase-7.5-m5): formal architectural acceptance and baseline freeze (m5.0-final)
   │
   e16b38e (M5.0-R17) docs(phase-7.5-m5): formalize published map encapsulation and runtime ownership (m5.0-r17)
   │
   ... [Intervening M5.0 Documentation Commits R16 -> R1]
   │
   40b11fd (tag: m4.3d-accepted) feat(phase-7.5-m4.3d): complete recovery acceptance matrix and test coverage
   ```
8. **Source Tree Integrity:** Executing `git diff --stat 40b11fdafcd247c7a289723f90ec0e2029c94f64 HEAD -- packages/` yields **zero lines changed**. The current codebase corresponds 100% byte-for-byte to the accepted M4.3d implementation baseline.

---

## 3. Frozen M5.0 Document Verification

All six authoritative M5.0 specification documents remain formally accepted and frozen:

1. `docs/PHASE7_5_M5_ARCHITECTURE.md`: Status: `FORMALLY ACCEPTED & FROZEN ARCHITECTURAL SPECIFICATION (MILESTONE M5.0 FINAL)`. Defines the 10-step reset sequence, 11-stage retirement transaction lifecycle, Option A candidate-root publication, Eight Rules of Map Ownership, settlement barrier, and 12-scenario failure matrix.
2. `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Status: `FORMALLY ACCEPTED & FROZEN ARCHITECTURAL SPECIFICATION (MILESTONE M5.0 FINAL)`. Defines formal TypeScript contracts, `ITerminalQuarantineArchive`, `IQuarantineBufferRetirementExtension`, `CompositeOperationIdentity`, `canonicalOperationIdentityKey`, and capability definitions.
3. `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`: Status: `FORMALLY ACCEPTED & FROZEN ARCHITECTURAL SPECIFICATION (MILESTONE M5.0 FINAL)`. Defines roadmap phasing (M5.0 through M5.5) and gating prerequisites.
4. `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: Status: `FORMALLY ACCEPTED & FROZEN ARCHITECTURAL SPECIFICATION (MILESTONE M5.0 FINAL)`. Defines invariants INV-M5-01 through INV-M5-09, criteria AC-M5-01 through AC-M5-33, and regression test floors.
5. `docs/PHASE7_5_M5_RISK_REGISTER.md`: Status: `FORMALLY ACCEPTED & FROZEN ARCHITECTURAL SPECIFICATION (MILESTONE M5.0 FINAL)`. Defines risks R-M5-01 through R-M5-10 and mitigations.
6. `docs/PHASE7_5_M5_0_FINAL_ACCEPTANCE_REPORT.md`: Status: `FORMALLY ACCEPTED AND FROZEN`. Records formal acceptance, review lineage, and codification of Qualifications A through D.

---

## 4. Existing Recovery Implementation Inventory

The accepted source files in `packages/sync-engine/src/recovery/` and `packages/sync-engine/src/engine/` were audited:

| Source File | Line Count | Existing Role in M4.3d | Existing Classes / Types | Audit Status for M5.1 |
|:---|:---|:---|:---|:---|
| `quarantine-buffer.ts` | 311 lines | Generational quarantine queue | `QuarantineBuffer`, `QuarantinedOperation`, `QuarantineDrainResult` | Partially sufficient. Missing candidate-root state (`QuarantineBufferCoreState`), Option A source retirement, and private collection encapsulation. |
| `quarantine-drainer.ts` | 421 lines | $N_{\text{in-flight}} \le 1$ drain loop | `QuarantineDrainer`, `DrainedInFlightRecord`, `DestinationAcceptanceReceipt` | Partially sufficient. Missing `ATTEMPT_INVALIDATED` receipt handling, settlement barrier Promise, and monotonic attempt-ID fencing. |
| `recovery-coordinator.ts` | 810 lines | Recovery orchestration & snapshot staging | `RecoveryCoordinator`, `RecoveryCoordinatorConfig`, `IRecoveryCoordinator` | Partially sufficient. Missing `bindEngine()` capability ceremony, failure-atomic `resetRecovery()`, 10-step reset sequence, and transactional retirement. |
| `recovery-state-machine.ts` | 212 lines | Legal 4-state lifecycle transitions | `RecoveryStateMachine`, `RecoveryState`, `RecoverySubstate` | Fully sufficient. Valid transition graph already allows `DEGRADED -> RECOVERING` and `RECOVERING -> ACTIVE`. |
| `snapshot-installer.ts` | 647 lines | 4-stage atomic snapshot installation | `SnapshotInstaller`, `StagedSnapshotInstallation`, `SnapshotInstallationReport` | Fully sufficient. `commitAtomic()` with `replaceCoreStateAtomic` verified and stable. |
| `engine-core-state.ts` | 199 lines | 12-field single-root engine state | `EngineCoreState`, `createEngineCoreState`, `createInitialEngineCoreState` | Fully sufficient. Sealed core state container verified. |
| `leaderless-sync-engine.ts` | 2,750 lines | Core playback and causal sync authority | `LeaderlessSyncEngine`, `RecoveryInstallationCapability` | Partially sufficient. `replaceCoreStateAtomic` exists; missing public recovery lifecycle accessors and coordinator constructor wiring. |
| `recovery-admission-controller.ts` | 327 lines | Remote event and local intent routing | `RecoveryAdmissionController`, `RemoteAdmissionResult`, `LocalAdmissionResult` | Fully sufficient. Validates epochs, event byte sizes, and enforces fail-close local intent rejection. |

---

## 5. Architecture-to-Source Traceability Matrix & Settlement Semantics

| M5.0 Architectural Requirement | Reference Spec | Target Source File | Existing Line Range | Status in Baseline | Concrete Required Implementation Change |
|:---|:---|:---|:---|:---|:---|
| **Authoritative Lifecycle State vs. Operational Mode** | Architecture §2.1.1, AC-M5-01 | `leaderless-sync-engine.ts` | 693, 914–922 | Partially Exists | Expose `isRecoveryInProgress()`, `isDegraded()`, and `recoveryState` delegating to coordinator; preserve `recoveryActive` strictly as slave notification gate. |
| **Engine-Coordinator Capability Binding Ceremony** | Architecture §2.1.1, AC-M5-07 | `recovery-coordinator.ts`, `leaderless-sync-engine.ts` | 146–175, 754–757 | Partially Exists | Add `bindEngine(engine, capability)` method; enforce single-attachment check (`SyncEngineError('ALREADY_BOUND')`); check object identity. |
| **Drain Admission Context Capability & Replay Gating** | Architecture §2.1.2, AC-M5-01 | `leaderless-sync-engine.ts` | 977–1010 | Partially Exists | Add capability-gated provenance parameter to `admitDrainedOperation(op, capability)`; gate local replay provenance checks. |
| **Receipt Variant `ATTEMPT_INVALIDATED`** | Interface §3.2, AC-M5-01 | `quarantine-drainer.ts` | 65–71, 318–344 | Missing | Add `ATTEMPT_INVALIDATED` variant to `DestinationAcceptanceReceipt`; handle in `processSingleOperation` by restoring operation to buffer via `unshiftDrain`. |
| **Shared Settlement Barrier (`drainSettledBarrier`) (Correction A)** | Architecture §3.4.1, AC-M5-01 | `quarantine-drainer.ts`, `recovery-coordinator.ts` | 119, 174–279 | Missing | Implement `drainSettledBarrier(): Promise<void>`. Resolves **strictly and only** upon actual settlement (`_inFlightRecord === null && !_isDraining`). **Never resolves or rejects on watchdog timeout.** |
| **Monotonic Attempt-ID Fencing & Watchdog Separation (Correction B)** | Architecture §3.4.1, AC-M5-01 | `recovery-coordinator.ts`, `quarantine-drainer.ts` | 670, 696 | Partially Exists | Coordinator advances `currentAttemptId` monotonically on reset/abort. Watchdog latches attempt to `TIMED_OUT` without touching drainer custody. Actual continuation fences on attempt ID. |
| **Terminal Quarantine Archive Implementation** | Architecture §3.4.4, Interface §3.3 | `terminal-quarantine-archive.ts` (New) | N/A (New File) | **Missing** | Implement `TerminalQuarantineArchive`, `TerminalArchiveCoreState`, `reserveCapacity`, `commitRetirementBatch`, `getEntries`, and export lifecycle. |
| **Composite Operation Identity & UTF-8 Canonical Key** | Architecture §3.4.3, Interface §3.3 | `terminal-quarantine-archive.ts` (New) | N/A (New File) | **Missing** | Implement `CompositeOperationIdentity`, pre-validate ASCII schema (`/^[a-zA-Z0-9_.:-]+$/`), compute exact UTF-8 byte lengths via `TextEncoder`, construct `opid:...` key. |
| **Option A Atomic Source Retirement in QuarantineBuffer** | Architecture §3.4.4, Interface §3.3 | `quarantine-buffer.ts` | 76–86, 243–271 | Missing | Add `QuarantineBufferCoreState`, `#identityIndex` private encapsulation, and `retireCommittedBatch(identities, capability)` with candidate-root pointer swap. |
| **10-Step Reset Sequence (`PURGE_AND_RETRY`)** | Architecture §3.4.2, AC-M5-01 | `recovery-coordinator.ts` | 668–686 | Partially Exists | Overhaul `resetRecovery` to require `RecoveryInstallationCapability`, execute 10-step sequence, await settlement barrier, and suppress stale notifications. |
| **11-Stage Transactional Retirement (`REINITIALIZE_CLEAN`)** | Architecture §3.4.3, AC-M5-01 | `recovery-coordinator.ts` | 668–686 | Missing | Implement 11-stage pipeline: generation freeze, settlement verify, complete entry staging, UTF-8 measurement, pre-reservation, commit, source retirement, zero-custody assert. |
| **Dual Zero-Custody Verification Assertion** | Architecture §3.4.3, AC-M5-01 | `recovery-coordinator.ts` | N/A | Missing | Assert `quarantineBuffer.totalRetainedCount === 0 && quarantineDrainer.inFlightRecord === null`; throw `SyncEngineError('CUSTODY_LEAK_DETECTED')` on failure. |
| **3-Way Uncertain Commit Reconciliation** | Architecture §3.4.3, AC-M5-01 | `recovery-coordinator.ts` | N/A | Missing | On commit timeout/error, query `archive.getTransaction(txId)`; if committed $\to$ complete source retirement; if absent $\to$ cancel reservation; if fails $\to$ halt `DEGRADED`. |
| **Non-Throwing Notification Flush** | Architecture §3.4.3, AC-M5-04 | `leaderless-sync-engine.ts` | 929–960 | Partially Exists | Ensure listener exceptions in `flushDeferredNotifications()` are caught, isolated, increment `listenerErrorCount`, and never throw out to caller. |

---

## 6. Detailed Settlement Barrier Semantics & State Separation (Corrections A & B)

### 6.1 The Conceptual Settlement Barrier Contract (Correction A)
The settlement barrier is defined by the formal contract:
```typescript
export interface IDrainSettlementBarrier {
  /**
   * Returns a promise that resolves strictly and only when all owned in-flight
   * drain operations have reached actual settlement, confirming that:
   *   this._inFlightRecord === null && this._isDraining === false
   *
   * This promise NEVER rejects, resolves, or settles upon watchdog timeout.
   */
  drainSettledBarrier(): Promise<void>;
}
```

#### Inviolable Settlement Invariants:
1. **Actual Settlement Prerequisite:** The settlement barrier promise resolves **only** after the underlying destination operation has settled and the drainer confirms that custody has been safely released or restored:
   $$\text{this.\_inFlightRecord} === \text{null} \quad \land \quad \text{this.\_isDraining} === \text{false}$$
2. **Watchdog Independence:** The watchdog timeout is a coordinator concern. When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expires, the watchdog latches the recovery attempt to `TIMED_OUT` and aborts the clean reset. **The watchdog callback must NEVER reject, resolve, or force-settle the settlement barrier.**
3. **No Phantom Cancellation:** ECMAScript Promises cannot be cancelled. The in-flight destination Promise continues running until completion.
4. **No Custody Eviction on Timeout:** Expiring a watchdog deadline does NOT clear `_inFlightRecord`, does NOT set `_isDraining = false`, and does NOT permit a subsequent reset attempt to dequeue or overwrite the occupied in-flight slot.

### 6.2 Independent State Transitions (Correction B)

```mermaid
stateDiagram-v2
    [*] --> InFlightPending: op dequeued to _inFlightRecord (N_in-flight = 1)

    state InFlightPending {
        [*] --> AwaitingDestination
        AwaitingDestination --> DestinationResolved: Async admission finishes
    }

    state WatchdogProcess {
        [*] --> WatchdogRunning
        WatchdogRunning --> WatchdogExpired: Elapsed monotonic time >= 5000ms
        WatchdogExpired --> AttemptTimedOut: attempt.status = 'TIMED_OUT'\n(No custody mutation!)
    }

    DestinationResolved --> AttemptFenced: Attempt was invalidated or timed out\n(receipt = ATTEMPT_INVALIDATED)
    DestinationResolved --> NormalReceipt: Attempt remains valid\n(ACCEPTED / REJECTED)

    AttemptFenced --> RestoredToBuffer: unshiftDrain(op)
    NormalReceipt --> CustodyFinalized: Destination confirmed or rejected

    RestoredToBuffer --> SlotReleased: _inFlightRecord = null\n_isDraining = false
    CustodyFinalized --> SlotReleased: _inFlightRecord = null\n_isDraining = false

    SlotReleased --> BarrierResolved: drainSettledBarrier Promise RESOLVES
    BarrierResolved --> [*]
```

#### Distinction Between Watchdog Timeout and Actual Settlement:
- **Transition 1: Watchdog Timeout (Coordinator-Level)**
  - Monotonic elapsed duration reaches `SETTLEMENT_BARRIER_TIMEOUT_MS` (5,000 ms).
  - Attempt status transitions to `attempt.status = 'TIMED_OUT'`.
  - Coordinator aborts clean reset to `DEGRADED` (`RESET_PREREQUISITE_FAILED`).
  - **In-flight record remains occupied** (`_inFlightRecord !== null`).
  - **Drainer remains draining** (`_isDraining === true`).
  - **Settlement barrier remains unresolved (pending).**
  - Any subsequent reset attempt sharing or observing the barrier is blocked from proceeding until actual settlement occurs.
- **Transition 2: Actual Promise Settlement (Drainer-Level)**
  - The underlying destination Promise completes.
  - Continuation executes: checks `attemptId === currentAttemptId`.
  - Because attempt was timed out or invalidated, destination returns `{ status: 'ATTEMPT_INVALIDATED', reason: 'STALE_ATTEMPT_FENCED' }`.
  - Drainer restores operation to buffer via `quarantineBuffer.unshiftDrain(op)`.
  - `_inFlightRecord` is set to `null`.
  - `_isDraining` is set to `false`.
  - **Settlement barrier Promise resolves.**
  - A subsequent reset attempt awaiting the barrier is now unblocked and can safely proceed.

---

## 7. Runtime Ownership & Collection Encapsulation Audit

### 7.1 Real Runtime Private Encapsulation (Qualification A)
`TerminalQuarantineArchive` and `QuarantineBuffer` must enforce genuine runtime privacy via **ECMAScript `#private` class fields** (`#committedTransactions`, `#activeReservations`, `#identityIndex`, `#core`) or closure-held state:
- TypeScript's compile-time `private` compiles to ordinary enumerable/accessible JavaScript properties.
- `ReadonlyMap<K, V>` is completely erased at compile time.
- `#private` class fields guarantee that external callers and reflection cannot retrieve or call `.set()`, `.delete()`, or `.clear()` on backing collections.

### 7.2 Protected Elements & Defensive Cloning (Qualification B)
- `Object.freeze([...this._core.entries])` protects only the outer array structure.
- Every `TerminalQuarantineEntry` must be individually shallow-frozen with `Object.freeze(entry)`.
- `entry.canonicalWireBytes: Uint8Array` is held as private backing storage.
- All public accessors and `archive.getEntries()` return defensively cloned `Uint8Array` instances (`new Uint8Array(entry.canonicalWireBytes)`).

---

## 8. Archive Transaction & Atomic Retirement Integration Analysis

`TerminalQuarantineArchive` commits staged retirement batches via candidate-root reference replacement:
```typescript
const candidateCore: TerminalArchiveCoreState = Object.freeze({
  entries: Object.freeze([...this.#core.entries, ...stagedBatch]),
  committedTransactions: nextTransactions, // Private encapsulated Map
  activeReservations: nextReservations,       // Private encapsulated Map
  totalBytes: this.#core.totalBytes + batchBytes,
  size: this.#core.size + stagedBatch.length,
  archiveGeneration: this.#core.archiveGeneration + 1,
});
// Single atomic linearization instruction:
this.#core = candidateCore;
```
Concurrent readers observe either the pre-commit root or the complete post-commit root with entries added and reservation consumed together.

---

## 9. Mandatory Audit of the 12 Failure Scenarios

| Scenario # | Scenario Title | Participating Components | Existing Baseline Support | Required M5.1 Implementation | State & Custody Invariants | Automated Test Specification |
|:---|:---|:---|:---|:---|:---|:---|
| **1** | Reservation capacity insufficient | `TQA`, `RC` | Missing | `TQA.reserveCapacity` checks $N \le 500$, bytes $\le 2\,\text{MiB}$; throws `CAPACITY_INSUFFICIENT`. | Buffer retains operations; clean reset aborts to `DEGRADED`; zero loss. | Unit test attempting reservation with 501 entries or 2,097,153 bytes; assert fail-closed rejection. |
| **2** | Oversized canonical entry | `TQA`, `RC` | Missing | UTF-8 byte measurement asserts $\le 4096$; throws `ENTRY_SIZE_EXCEEDED`. | Entry never truncated; retained in buffer; clean reset aborts to `DEGRADED`. | Unit test with 4097-byte entry; assert rejection and buffer custody preservation. |
| **3** | Generation mismatch at reservation commit | `TQA`, `RC` | Missing | `TQA.commitRetirementBatch` asserts `expectedArchiveGeneration === core.archiveGeneration`. | Reservation cancelled; batch re-staged or reset aborted cleanly. | Concurrent commit advances generation before pending commit; assert `GENERATION_MISMATCH`. |
| **4** | Exception during candidate-root construction | `TQA`, `RC` | Missing | Candidate allocation wrapped in try/catch prior to reference swap. | Old root remains authoritative; zero entries added; reservation unconsumed. | Inject simulated fault during Map copying; assert `#core` remains identical reference. |
| **5** | **Archive commit succeeds but source retirement throws (CRITICAL PATH)** | `TQA`, `QB`, `RC` | Missing | Coordinator catches failure; queries `getTransaction()`; skips duplicate archive commit; retries source retirement. | **No duplicate archive entries**; buffer retained until successful retry; transaction record is authoritative. | Deterministic partial-failure test: mock `QB.retireCommittedBatch` to throw on first call; verify coordinator retries, queries receipt, and completes retirement cleanly. |
| **6** | Duplicate transaction submission | `TQA` | Missing | Lookup-first commit ordering: checks `committedTransactions.has(txId)` before validation. | Exactly zero duplicate entries added; returns existing receipt idempotently. | Submit identical transaction ID and digest twice; assert identical receipt and unchanged archive size. |
| **7** | Transaction ID reused with different digest | `TQA` | Missing | If transaction ID exists but digest differs, throws `TRANSACTION_ID_DIGEST_MISMATCH`. | Archive and buffer preserved; prevents corrupted partial batch replay. | Submit same transaction ID with different batch digest; assert conflict error thrown. |
| **8** | Uncertain commit outcome | `TQA`, `RC` | Missing | 3-way reconciliation: query `getTransaction(txId)`; if committed $\to$ retire source; if absent $\to$ cancel reservation. | Zero dual mutation; zero lost custody; halts safely in `DEGRADED` if query fails. | Simulate timeout awaiting commit receipt; execute 3-way query; assert reconciliation to committed or aborted. |
| **9** | Attempt invalidation during preparation | `RC`, `QD` | Missing | Reset/abort advances `currentAttemptId`; stale continuation detects mismatch and aborts. | Stale continuation cannot install state or mutate archive; operations preserved. | Trigger concurrent reset while preparation awaits barrier; assert earlier attempt terminates with zero effect. |
| **10** | Conflicting composite identity in batch | `RC`, `QB` | Missing | Full 4-field comparison (`originPeerId`, `sequenceNumber`, `membershipEpoch`, `logicalEventId`). | Batch rejected fail-closed; quarantine buffer preserved; reset halts in `DEGRADED`. | Batch contains operation with same origin/seq but different epoch; assert `RETIREMENT_BATCH_CONFLICT`. |
| **11** | **Settlement watchdog timeout (Corrections A & B)** | `RC`, `QD` | Missing | Watchdog timer (`5000ms`); latches attempt to `TIMED_OUT`; clean reset aborts to `DEGRADED`. **Barrier remains pending.** | In-flight slot remains occupied; barrier does NOT settle on timeout; continuation restores custody upon actual settlement. | 17-step deterministic test sequence (§12.1); verifies custody retention past timeout and barrier resolution only upon actual settlement. |
| **12** | Dual zero-custody verification failure | `RC`, `QB`, `QD` | Missing | Coordinator asserts `buffer.count === 0 && drainer.inFlight === null`; throws `CUSTODY_LEAK_DETECTED`. | Clean-root install strictly blocked; enters `DEGRADED`; prevents uncommitted operation loss. | Inject mock leftover operation in drainer after retirement; assert clean baseline install is blocked. |

---

## 10. Proposed M5.1 Implementation Phasing

To guarantee risk isolation, incremental testability, and zero regression, M5.1 is partitioned into five sequential sub-milestones:

### Sub-Milestone M5.1a: Terminal Quarantine Archive & Composite Identity Domain
- **Scope:** New file `packages/sync-engine/src/recovery/terminal-quarantine-archive.ts`. Implements `TerminalQuarantineArchive`, `CompositeOperationIdentity`, `canonicalOperationIdentityKey()`, `TerminalArchiveCoreState`, Eight Rules of Map Ownership, ECMAScript `#private` fields, defensive `Uint8Array` cloning, single-pointer candidate-root publication (`this.#core = candidateCore`), reservation allocation/consumption/cancellation, lookup-first idempotency, and single-lease export governance.
- **Why Safe First:** Completely self-contained leaf component. Zero changes to existing engine, buffer, or drainer code. 100% testable via isolated unit tests and failure-injection tests (covering Scenarios 1, 2, 3, 4, 6, 7).

### Sub-Milestone M5.1b: QuarantineBuffer Option A Atomic Source Retirement & Core State Root
- **Scope:** Enhance `quarantine-buffer.ts` with candidate-root encapsulation (`QuarantineBufferCoreState`), ECMAScript `#private` identity index, and Option A atomic source retirement method `retireCommittedBatch(identities, capability)`.

### Sub-Milestone M5.1c: QuarantineDrainer Settlement Barrier & Attempt-Fenced Admission
- **Scope:** Enhance `quarantine-drainer.ts` with `ATTEMPT_INVALIDATED` confirmation handling, the shared settlement barrier (`drainSettledBarrier(): Promise<void>`), and attempt-ID fencing.
- **Settlement Invariant:** The barrier resolves **only** when `_inFlightRecord === null && !_isDraining`. It does **NOT** resolve or reject on watchdog timeout.

### Sub-Milestone M5.1d: RecoveryCoordinator Lifecycle Binding, Settlement & Administrative Reset Orchestration
- **Scope:** Enhance `recovery-coordinator.ts` with `bindEngine()` capability ceremony, 10-step `PURGE_AND_RETRY` sequence, 11-stage `REINITIALIZE_CLEAN` transactional retirement, independent watchdog timeout latching (`attempt.status = 'TIMED_OUT'`), 3-way uncertain commit reconciliation, and dual zero-custody verification.

### Sub-Milestone M5.1e: LeaderlessSyncEngine Public Lifecycle Queries, Capability Wiring & Regression Floor Verification
- **Scope:** Enhance `leaderless-sync-engine.ts` with constructor coordinator binding, public queries (`isRecoveryInProgress()`, `isDegraded()`, `recoveryState`), wire intake interception, and run full monorepo regression suite ($\ge 1,550$ tests).

---

## 11. Implementation Dependency Graph

```mermaid
flowchart TD
    M5_0["Milestone M5.0: Architecture Audit & Final Freeze<br/>(COMPLETED & FROZEN)"] --> M5_1_Stage0["M5.1 Stage 0-R1: Readiness & Settlement Correction<br/>(THIS AUDIT REPORT)"]
    M5_1_Stage0 --> M5_1a["M5.1a: Terminal Quarantine Archive & Composite Identity<br/>(Self-Contained; Zero Engine Touch)"]
    M5_1a --> M5_1b["M5.1b: QuarantineBuffer Atomic Source Retirement<br/>(Option A CoreState & Private Encapsulation)"]
    M5_1b --> M5_1c["M5.1c: QuarantineDrainer Settlement Barrier<br/>(drainSettledBarrier & Attempt-Fenced Receipts)"]
    M5_1a --> M5_1d["M5.1d: RecoveryCoordinator Administrative Reset<br/>(10-Step Reset & 11-Stage Transactional Retirement)"]
    M5_1b --> M5_1d
    M5_1c --> M5_1d
    M5_1d --> M5_1e["M5.1e: LeaderlessSyncEngine Lifecycle Wiring<br/>(Public Queries & Full Regression Verification)"]

    style M5_0 fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style M5_1_Stage0 fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    style M5_1a fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_1b fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_1c fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_1d fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_1e fill:#fff3e0,stroke:#e65100,stroke-width:2px
```

---

## 12. Mandatory Test Strategy & Deterministic Sequences (Corrections C & E)

### 12.1 Deterministic Settlement Barrier & Watchdog Timeout Test Sequence (Correction C)
To verify that watchdog timeout and actual settlement are completely independent and that in-flight custody cannot be leaked or bypassed, Sub-Milestone M5.1c/M5.1d must include an explicit deterministic test executing the following 17-step sequence:

```
1. Start a drain operation whose destination Promise remains unresolved (pending).
2. Begin a recovery reset and await its settlement barrier (drainSettledBarrier()).
3. Advance the watchdog timer beyond 5,000 ms using deterministic injected fake timers.
4. Assert that the reset attempt transitions to and latches as TIMED_OUT.
5. Assert that the underlying destination Promise remains pending.
6. Assert that _inFlightRecord remains occupied (N_in-flight == 1).
7. Assert that _isDraining remains true (consistent with actual active drain ownership).
8. Assert that the settlement barrier Promise remains pending (unresolved and unrejected).
9. Attempt to start a second recovery reset attempt.
10. Assert that the second reset cannot acquire a new drain owner or bypass the pending barrier.
11. Resolve the original destination Promise.
12. Allow the asynchronous drain continuation to execute in the microtask queue.
13. Assert that stale-attempt fencing intercepts the continuation (receipt = ATTEMPT_INVALIDATED).
14. Assert that operation custody is restored to QuarantineBuffer via unshiftDrain().
15. Assert that _inFlightRecord is cleared to null ONLY AFTER actual settlement and ownership restoration.
16. Assert that the settlement barrier Promise NOW resolves.
17. Assert that a subsequent authorized reset attempt can now proceed cleanly.
```

*Testing Infrastructure Requirement:* This test must be implemented using Vitest deterministic fake timers (`vi.useFakeTimers()`) and microtask queue flushing (`await Promise.resolve()`). Zero arbitrary real-time sleeps (`setTimeout`) are permitted.

### 12.2 Preserving Testability Without Exposing Private State (Correction E)
To ensure 100% testability while strictly enforcing the Eight Rules of Map Ownership:
1. **Public Observable Behavior:** Unit and integration tests verify state transitions, returned receipts, thrown error codes, and exported snapshots via public API methods only.
2. **Structural & Encapsulation Verification:** ECMAScript `#private` fields (`#committedTransactions`, `#activeReservations`, `#identityIndex`, `#core`) are verified via compile-time syntax enforcement and static AST checks, asserting that reflection (`Object.getOwnPropertyNames`, `Object.getOwnPropertySymbols`) reveals zero internal collection handles.
3. **Defensive Copy Verification:** Tests verify defensive cloning by explicitly mutating returned values (e.g. calling `bytes[0] = 0xFF` on the `Uint8Array` returned by `entry.canonicalWireBytes` or pushing to the array returned by `archive.getEntries()`) and asserting that subsequent public queries return unaltered data.
4. **Zero Test-Only Leaks:** No test-only public getters, backdoor inspection handles, or debug accessors may be added to expose backing Maps, core roots, or internal buffer references.

### 12.3 Regression Test Floors
- Total Monorepo Tests: $\ge 1,550$ passing tests.
- Sync-Engine Package Tests: $\ge 965$ passing tests.
- Recovery Subsystem Tests: $\ge 42$ passing tests.
- Zero skipped tests, zero failed tests.

---

## 13. Risks and Unresolved Questions (Correction D)

1. **V8 Heap vs. Wire Bounds:** As identified in R17, JavaScript object wrappers and closures consume $\sim 3.5\times$ to $5\times$ more heap than serialized wire bytes. The 2 MiB wire limit for `TerminalQuarantineArchive` translates to $\sim 7$ to $10$ MiB of V8 heap. Telemetry must track heap expansion during long-running sessions.
2. **Monotonic Elapsed Duration & Browser Throttling (Correction D):**
   - `performance.now()` measures **monotonic elapsed time**, NOT wall-clock time. It is immune to system clock adjustments, NTP stepping, or leap seconds.
   - The coordinator watchdog must record its start time using `performance.now()` and compare elapsed monotonic duration when the timer callback executes.
   - Browser environments frequently throttle timers in inactive background tabs (delaying `setTimeout` callbacks by seconds or minutes).
   - **Critical Rule:** Executing a watchdog callback must NEVER be treated as evidence that an underlying destination Promise settled. The settlement barrier must continue awaiting real Promise settlement regardless of elapsed duration. The frozen 5,000 ms timeout policy (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) is preserved.
3. **Promise Cancellation Impossibility:** ECMAScript Promises cannot be aborted at the engine runtime level. The shared settlement barrier guarantees that even after an attempt times out, the occupied slot is retained until the Promise completes, preventing concurrent re-entry or custody leaks.

---

## 14. First Implementation Sub-Milestone Recommendation

The auditor explicitly recommends:
```
================================================================================
RECOMMENDED FIRST IMPLEMENTATION SUB-MILESTONE:
SUB-MILESTONE M5.1a: TERMINAL QUARANTINE ARCHIVE & COMPOSITE IDENTITY DOMAIN
================================================================================
```

### Justification for M5.1a:
1. **Zero Coupling to Live Engine:** `TerminalQuarantineArchive` is an independent, leaf component. Implementing it creates a new file (`packages/sync-engine/src/recovery/terminal-quarantine-archive.ts`) with **zero modifications** to existing `LeaderlessSyncEngine`, `QuarantineDrainer`, or `RecoveryCoordinator` logic.
2. **Immediate Testability:** All core collection encapsulation invariants (Eight Rules of Map Ownership, `#private` fields, defensive `Uint8Array` cloning, single-pointer swap, lookup-first idempotency, and Scenarios 1, 2, 3, 4, 6, 7) can be verified via dedicated unit tests.
3. **Firm Architectural Anchor:** Establishing M5.1a provides the exact, audited contracts and composite identity structures required by subsequent stages (M5.1b source retirement and M5.1d coordinator orchestration).

---

## 15. Final Stage 0-R1 Verdict & Mandatory Authorization Boundary

```
================================================================================
STAGE 0-R1 IMPLEMENTATION READINESS VERDICT:
PASS — STAGE 0 READY FOR FINAL REVIEW
================================================================================
```

All 20 Scope and Architecture Preservation Rules have been adhered to. The baseline source code is fully audited, verified, and grounded in exact file paths and line ranges. The settlement barrier semantics have been corrected to remain completely independent of watchdog timeout, the 17-step deterministic test sequence has been codified, monotonic clock semantics have been refined, and private state encapsulation is fully preserved.

### Mandatory Implementation Authorization Statement
**"M5.1 Stage 0-R1 documentation correction is complete. No production implementation has been started. M5.1a remains unauthorized pending explicit Engineering Authority approval."**
