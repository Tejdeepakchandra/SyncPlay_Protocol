# Phase 7.5 M5.0-R18-R1 — Concurrent Recovery Arbitration, Ownership Separation & Custody Governance

**Sub-Milestone Title:** Recovery Ownership Separation & Failed-Custody Retry Governance (R18-R1 Revision)
**Role:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Mode:** STRICTLY DOCUMENTATION-ONLY ARCHITECTURAL CORRECTION
**Status:** PROPOSED ARCHITECTURAL DECISION AMENDMENT (AWAITING ENGINEERING AUTHORITY APPROVAL)
**Baseline Document:** R18 Baseline Commit `74d9440811dc7de1a3b43975887776eae00bec45`, Formally Accepted M5.0 Freeze (`b654cd2e5c38440f6c26ff0d827a6fdc9652c378`), and Stage 0-R3 Report (`4175e8930df01bc914be4e0f902f59c8e4fb20a5`)
**Implementation Authorization Status: Milestone M5.1 remains STRICTLY BLOCKED AND UNAUTHORIZED.**

---

## 1. Problem Statement & Executive Summary

Following the M5.0-R18 proposal, this R18-R1 revision corrects three critical architectural precision gaps identified during independent audit:

1. **Conflation of Administrative Attempt Ownership and Drainer Operation Custody:**
   Administrative recovery-attempt ownership (minted attempt IDs, recovery state machine transitions, and root-replacement authority) is fundamentally distinct from drainer operation custody (exclusive possession of an in-flight operation by `QuarantineDrainer`). The expiration of a watchdog timer terminates administrative attempt authority, but does **not** settle the underlying Promise or release operation custody.
2. **Underspecified Retry Governance During Failed or Inconclusive Reconciliation:**
   Being in `DEGRADED` state does not automatically confer permission to retry an administrative reset. If an operation's custody reconciliation is failing, inconclusive, or blocked (e.g. `unshiftDrain()` threw or an archive commit query timed out), admitting a fresh reset request could trigger uncommitted operation overwrites, dual zero-custody assertion panics, or duplicate archive commits.
3. **Overstated Policy Safety Claims:**
   Previous policy comparisons utilized unsupported absolute terms ("Absolute Zero Leak Guarantee", "100% Compliant"). These are replaced with bounded, failure-model-specific descriptions acknowledging that single-attempt ownership eliminates concurrency hazards but relies on the Custody-Reconciliation Gate for end-to-end custody safety.

---

## 2. Separation of the Two Independent Ownership Lifecycles

To eliminate architectural ambiguity, the recovery protocol formally decouples two distinct runtime lifecycles:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│              LIFECYCLE 1: ADMINISTRATIVE RECOVERY-ATTEMPT OWNERSHIP             │
│                                                                                 │
│   [DEGRADED] ──resetRecovery()──► [RECOVERING] ──Step 4 await──► [ACTIVE]       │
│        ▲                                │ (Watchdog Expired)                    │
│        └────────────────────────────────┴───────────────────────────────────────┘
│   Authority: Attempt ID, State Machine Transition, Candidate Root Swap          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                         ▲
                         COUPLING POINT: STEP 4 BARRIER
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                 LIFECYCLE 2: DRAINER OPERATION CUSTODY                          │
│                                                                                 │
│   [BUFFER] ──drainNext()──► [IN_FLIGHT (N=1)] ──Reconciliation──► [SETTLED]    │
│                                  │ (Pending Promise)                  │         │
│                                  └────────────────────────────────────┴─────────┤
│   Custody: Physical Operation Record, Drain Promise, Buffer/Archive Transfer    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Administrative Recovery-Attempt Ownership
- **Owner Component:** `RecoveryCoordinator`.
- **Acquisition:** Acquired when `resetRecovery(capability, action)` is invoked strictly from `DEGRADED` state and the Custody-Reconciliation Gate is clear. Mints a monotonic `attemptId = ++this.currentAttemptId`.
- **Exclusive Authority:** The active attempt holds exclusive authority to transition the coordinator state machine, configure recovery admission gating, pre-reserve archive capacity, and commit clean baseline roots via `replaceCoreStateAtomic`.
- **Concurrency Control:** While an administrative attempt is active (`RECOVERING` or `RECONCILING`), all concurrent `resetRecovery` invocations are rejected fail-closed with `SyncEngineError('RECOVERY_ALREADY_IN_PROGRESS')`.
- **Watchdog Invalidation:** When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expires in Step 4, the coordinator latches `attempt.status = 'TIMED_OUT'` and transitions to `DEGRADED`. This strips the attempt of all authority.
- **Continuation Fencing:** Every asynchronous suspension point (`await`) re-validates `attempt.status !== 'TIMED_OUT'` and `attempt.attemptId === this.currentAttemptId`. If invalidated, the continuation terminates immediately with zero state mutation, zero candidate root replacement, and zero notification flush.

### 2.2 Drainer Operation Custody
- **Owner Component:** `QuarantineDrainer`.
- **Acquisition:** Acquired when `drainer.drainNext()` dequeues an operation from `QuarantineBuffer` and enters in-flight state (`this._inFlightRecord !== null`, $N_{\text{in-flight}} = 1$, `this._isDraining = true`).
- **Persistence Across Attempt Invalidation:** Drainer custody persists through the underlying destination Promise regardless of whether the administrative attempt times out, degrades, or transitions. Administrative attempt termination does **NOT** cancel the Promise, clear `_inFlightRecord`, or release custody.
- **Custody Reconciliation:** Upon actual Promise settlement (resolution or rejection), the drainer executes safe custody disposition:
  - `ATTEMPT_INVALIDATED` or Promise Rejection $\to$ restores to `QuarantineBuffer` head via `unshiftDrain(op)`.
  - `ACCEPTED_REDUCED` / `ACCEPTED_PENDING` $\to$ transfers ownership to destination engine log.
  - `TERMINALLY_REJECTED` $\to$ commits to `TerminalQuarantineArchive`.
  - `CAPACITY_REJECTED` $\to$ restores to `QuarantineBuffer` head via `unshiftDrain(op)`.
- **Release and Settlement Barrier Resolution:** `this._inFlightRecord = null` and `this._isDraining = false` occur strictly **after** custody disposition has been confirmed. The settlement barrier (`drainSettledBarrier(): Promise<void>`) resolves strictly and exclusively upon this condition.

---

## 3. The Custody-Reconciliation Gate

To prevent unsafe resets when operation custody is unresolved or failing, the coordinator formalizes an explicit **Custody-Reconciliation Gate**:

```typescript
export type CustodyGateStatus =
  | 'CUSTODY_SETTLED'                 // 1. In-flight slot empty (N=0); barrier settled. Reset PERMITTED.
  | 'SETTLEMENT_PENDING'               // 2. In-flight slot occupied (N=1); Promise pending. Reset PERMITTED via Step 4 barrier wait.
  | 'CUSTODY_RECONCILIATION_REQUIRED'  // 3. Destination settled; reconciliation query or transfer active. Reset QUEUED/REJECTED.
  | 'CUSTODY_RECONCILIATION_BLOCKED';  // 4. Reconciliation failed (unshift threw or query inconclusive). Reset STRICTLY BLOCKED.
```

### Invariants Enforced by the Gate:
1. **Root-Commit Prevention:** No candidate root replacement can ever occur while gate status is `SETTLEMENT_PENDING`, `CUSTODY_RECONCILIATION_REQUIRED`, or `CUSTODY_RECONCILIATION_BLOCKED`.
2. **In-Flight Overwrite Prevention:** No new drain can dequeue an operation while `_inFlightRecord !== null`.
3. **Archive Idempotency:** Uncertain archive transactions must be queried via 3-way reconciliation before any subsequent retirement attempt is admitted.
4. **Permanent Fault Containment:** If custody restoration throws (`unshiftDrain` fails), the coordinator locks in `CUSTODY_RECONCILIATION_BLOCKED`, prohibiting further automated resets until explicit administrative intervention.

---

## 4. Comprehensive Analysis of Retry Admission Across Unresolved Custody Cases (Cases A through H)

The table below governs coordinator state, attempt ownership, drainer custody, and retry admission across all eight failure modes:

| Failure Case | Coordinator State | Administrative Attempt Ownership | Drainer In-Flight Slot ($N_{\text{in-flight}}$) | Settlement Barrier Status | Custody Gate Status | New Reset Accepted? | Root Replacement Permitted? | Caller-Visible Result / Action |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| **Case A: Watchdog timeout; destination Promise still pending** | `DEGRADED` | Attempt 1 `TIMED_OUT` (Invalidated) | 1 (Occupied by `op1`) | Pending (Unresolved) | `SETTLEMENT_PENDING` | **YES, with serialized wait.** Enters Step 4 under Attempt 2; halts until barrier resolves. | **STRICTLY PROHIBITED.** | Attempt 2 awaits settlement. If barrier times out again, Attempt 2 latches `TIMED_OUT`. |
| **Case B: Destination Promise settled; normal reconciliation** | `RECOVERING` or `DEGRADED` | Attempt 1 active (or settling) | 1 (Transferring) | Pending (Resolving) | `CUSTODY_RECONCILIATION_REQUIRED` | **REJECTED.** If `RECOVERING`, throws `RECOVERY_ALREADY_IN_PROGRESS`. | Permitted strictly after Step 7 zero-custody verification. | Microtask turns finalize custody transfer cleanly. |
| **Case C: Destination settled; `unshiftDrain()` threw error** | `DEGRADED` | Attempt 1 `TIMED_OUT` or `FAILED` | 1 (Stranded in drainer) | Pending (Blocked) | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** | **STRICTLY PROHIBITED.** | Throws `SyncEngineError('CUSTODY_RECONCILIATION_BLOCKED', 'Quarantine restoration failed. Administrative repair required.')`. |
| **Case D: Archive commit outcome inconclusive** | `DEGRADED` | Attempt 1 `FAILED` | 1 (Occupied) | Pending (Blocked) | `CUSTODY_RECONCILIATION_REQUIRED` | **STRICTLY REJECTED.** | **STRICTLY PROHIBITED.** | Throws `SyncEngineError('CUSTODY_RECONCILIATION_REQUIRED', 'Archive transaction outcome uncertain. Awaiting 3-way query.')`. |
| **Case E: Archive query confirms committed** | `DEGRADED` | Latched terminal | 0 (Released post-query) | Resolved | `CUSTODY_SETTLED` | **YES.** Fresh attempt admitted from `DEGRADED`. | Permitted under new attempt post-settlement. | Prior transaction recognized; slot released; fresh reset proceeds. |
| **Case F: Archive query confirms absent** | `DEGRADED` | Latched terminal | 1 $\to$ 0 (Unshifted to buffer) | Resolves after unshift | `CUSTODY_SETTLED` (post-unshift) | **YES.** Admitted once unshift completes. | Permitted under new attempt. | Reservation cancelled; op unshifted to buffer; fresh reset proceeds. |
| **Case G: Archive query remains inconclusive** | `DEGRADED` | Permanently failed | 1 (Stranded in drainer) | Pending (Blocked) | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** | **STRICTLY PROHIBITED.** | Throws `SyncEngineError('CUSTODY_RECONCILIATION_BLOCKED', 'Archive commit inconclusive. Automated reset blocked.')`. |
| **Case H: Permanent custody fault / unrecoverable failure** | `DEGRADED` | Permanently failed | 1 (Stranded) | Permanently Pending | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** | **STRICTLY PROHIBITED.** | Requires explicit emergency override (`forceEmergencyReset(cap)`) acknowledging data abandonment. |

---

## 5. Resolution of Attempt Overlap After Watchdog Timeout

The audit definitively resolves whether Attempt 2 may be accepted from `DEGRADED` while Attempt 1's destination Promise remains unresolved:

### Permitted Overlap Workflow:
1. Attempt 1 times out after 5,000 ms. Coordinator marks `Attempt 1 = TIMED_OUT` and transitions to `DEGRADED`.
2. The destination Promise remains pending on the JavaScript event loop. Drainer in-flight slot remains occupied ($N_{\text{in-flight}} = 1$). Barrier remains pending. Custody gate is `SETTLEMENT_PENDING`.
3. Caller invokes `resetRecovery(capability, action)` (Attempt 2).
4. Because the coordinator is in `DEGRADED` and gate is `SETTLEMENT_PENDING`, Attempt 2 is **accepted**:
   - Coordinator mints `currentAttemptId = 2`.
   - Coordinator transitions state machine to `RECOVERING`.
   - Attempt 2 executes Steps 1–3, and in Step 4 invokes `await drainer.drainSettledBarrier()`.
5. **Execution Fencing during Overlap:**
   - Attempt 2 halts in Step 4 awaiting the barrier. It is **strictly prohibited** from performing candidate root staging, archive pre-reservation, or state replacement while in Step 4.
   - Attempt 1 continuation wakes up when the destination Promise finishes. It checks its attempt ID: `1 !== currentAttemptId (2)` and `attempt.status === 'TIMED_OUT'`.
   - Attempt 1 continuation is **fenced** and terminates immediately with zero state mutations.
6. **Custody Handoff:**
   - Drainer evaluates the receipt (e.g. `ATTEMPT_INVALIDATED`), restores operation to `QuarantineBuffer` head via `unshiftDrain(op1)`, clears `_inFlightRecord = null`, sets `_isDraining = false`, and resolves the settlement barrier.
   - The barrier resolution unblocks Attempt 2's Step 4 await.
   - Attempt 2 confirms `quarantineDrainer.inFlightRecord === null` ($N_{\text{in-flight}} == 0$) and proceeds to Step 7 with exclusive, fresh authority.

---

## 6. Corrected Policy Comparison Language

Previous assertions of "Absolute Zero Leak Guarantee" and "100% Compliant" are formally replaced with bounded, model-specific evaluations:

| Evaluation Dimension | Previous Wording (Overstated) | Corrected Architectural Evaluation (R18-R1) |
|:---|:---|:---|
| **Custody Safety** | "Absolute (Zero Leak Guarantee)" | **Bounded Single-Owner Custody under Tested Failure Model:** Single-attempt ownership eliminates concurrent root replacement and race conditions. Custody safety relies on fail-close unshift handling and the Custody-Reconciliation Gate. |
| **Archive Isolation** | "Completely Isolated" | **Isolated from Cross-Mode Concurrency Races:** Pre-reservations and candidate roots are protected from concurrent cancellation, but remain bounded by archive capacity ($N \le 500$, bytes $\le 2\,\text{MiB}$). |
| **State-Machine Alignment** | "100% Compliant" | **Fully Aligned with Explicit `DEGRADED` State Guard:** Reconciles the state guard by treating `RECOVERING` as an active lock and serializing post-timeout re-entries on the drainer barrier. |

---

## 7. Deterministic Failure Scenarios & Test Requirements

The test suite must verify the following five deterministic test sequences:

1. **Test R18-R1-T1 (Retry Blocked During Unshift Failure):**
   - Drainer receives `ATTEMPT_INVALIDATED`. Mock `quarantineBuffer.unshiftDrain` to throw.
   - Assert coordinator enters `DEGRADED` with gate `CUSTODY_RECONCILIATION_BLOCKED`.
   - Caller invokes `resetRecovery()`. Assert request is rejected with `CUSTODY_RECONCILIATION_BLOCKED`.
2. **Test R18-R1-T2 (Retry Awaits Pending Barrier Post-Timeout):**
   - Attempt 1 times out after 5,000 ms. Destination Promise remains unsettled.
   - Caller invokes `resetRecovery()` (Attempt 2). Assert Attempt 2 is admitted into `WAITING_FOR_SETTLEMENT` (Step 4).
   - Assert Attempt 2 does NOT execute candidate root staging while barrier is pending.
   - Resolve destination Promise $\to$ assert Attempt 1 fenced $\to$ barrier resolves $\to$ Attempt 2 completes recovery.
3. **Test R18-R1-T3 (Retry Blocked During Inconclusive Archive Query):**
   - Archive commit outcome is uncertain; mock `archive.getTransaction` to throw.
   - Assert coordinator locks in `DEGRADED` with gate `CUSTODY_RECONCILIATION_BLOCKED`.
   - Assert subsequent reset requests are rejected fail-closed without modifying archive or buffer.
4. **Test R18-R1-T4 (Same-Mode Concurrency Rejection):**
   - Attempt 1 active in `RECOVERING`. Caller 2 invokes `resetRecovery()` with identical mode.
   - Assert Caller 2 receives `RECOVERY_ALREADY_IN_PROGRESS`.
5. **Test R18-R1-T5 (Conflicting-Mode Concurrency Rejection):**
   - Attempt 1 active with `PURGE_AND_RETRY`. Caller 2 invokes `resetRecovery()` with `REINITIALIZE_CLEAN`.
   - Assert Caller 2 receives `RECOVERY_ALREADY_IN_PROGRESS` with metadata. Assert zero archive purging.

---

## 8. Updated Formal Approval Questions for Engineering Authority

The Engineering Authority is requested to review and approve five specific architectural questions:

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R1):
================================================================================
1. [LIFECYCLE SEPARATION]: Does the Engineering Authority approve the formal
   separation of Administrative Recovery-Attempt Ownership (coordinator-scoped)
   from Drainer Operation Custody (drainer-scoped), establishing that attempt
   timeout does not release drainer custody or cancel the destination Promise?

2. [CUSTODY-RECONCILIATION GATE]: Does the Engineering Authority approve the
   four-state Custody-Reconciliation Gate, strictly blocking reset admission
   when custody reconciliation is failed or inconclusive (CUSTODY_RECONCILIATION_BLOCKED)?

3. [POST-TIMEOUT OVERLAP RE-ENTRY]: Does the Engineering Authority approve that
   a fresh reset from DEGRADED following a watchdog timeout is admitted under a
   new attempt ID and serialized on the pending settlement barrier in Step 4,
   while the timed-out attempt is permanently fenced?

4. [CONCURRENCY REJECTION]: Does the Engineering Authority approve rejecting all
   concurrent reset requests arriving during RECOVERING or RECONCILING fail-closed
   with RECOVERY_ALREADY_IN_PROGRESS, rather than ambient Promise joining?

5. [ERROR CLASSIFICATION]: Does the Engineering Authority approve introducing
   'RECOVERY_ALREADY_IN_PROGRESS' and 'CUSTODY_RECONCILIATION_BLOCKED' into the
   SyncEngineError classification?
================================================================================
```

---

## 9. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R1 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — AWAITING ENGINEERING AUTHORITY
================================================================================
```

This R18-R1 proposal establishes complete architectural precision: it decouples administrative attempt authority from drainer custody, introduces the Custody-Reconciliation Gate, governs retry admission across all eight failure modes, and resolves attempt overlap without compromising custody safety.

### Mandatory Implementation Authorization Statement
**"M5.0-R18-R1 is an architectural decision proposal only. M5.1 implementation remains unauthorized. No M5.1a implementation has started."**
