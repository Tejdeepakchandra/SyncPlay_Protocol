# Phase 7.5 Milestone M5 — Risk Register

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R17 AMENDED)
**Date:** 2026-11-25
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Baselines:** M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)
**Scope:** Risk identification, mitigation strategies, and failure mode analysis for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Executive Summary

Milestone M5 integrates the recovery subsystem directly into `LeaderlessSyncEngine`. Because this touches core operation admission, replication vectors, Lamport clocks, and playback state reducers, rigorous risk identification and automated verification controls are essential to prevent silent state corruption, lifecycle desynchronization, or regressions.

---

## 2. Risk Matrix Overview

| Risk ID | Risk Category | Severity | Likelihood | Impact | Status |
|---|---|:---:|:---:|---|:---:|
| **R-M5-01** | Lamport Clock Integrity | Critical | Medium | Clock regression or artificial inflation violates causal ordering | MITIGATED |
| **R-M5-02** | Staged State Leaks | Critical | Low | Partial state mutation during failed snapshot install | MITIGATED |
| **R-M5-03** | Local Intent Inversion | High | Medium | Queued local actions apply at invalid future media timestamps | MITIGATED |
| **R-M5-04** | Quarantine Memory Exhaustion | High | Medium | Unbounded memory growth under high-frequency event bursts | MITIGATED |
| **R-M5-05** | Causal Dependency Deadlock | High | Low | Quarantined events with missing dependencies stall drain | MITIGATED |
| **R-M5-06** | Drain-Arrival Concurrency Race | Medium | Medium | Race between post-install drain and incoming live events | MITIGATED |
| **R-M5-07** | Stale Timer Callback Corruption | Medium | Low | Timer fires after abort/disposal, corrupting new attempt | MITIGATED |
| **R-M5-08** | Ambiguous Lifecycle & Teardown Leak | Medium | Low | Unclear ownership, active timers, or listener leaks prevent teardown | MITIGATED |
| **R-M5-09** | Incomparable Frontier Lattice Fork | High | Low | Arbitrary ordering of incomparable vectors causes split-brain | MITIGATED |
| **R-M5-10** | Lifecycle State Desynchronization | High | Low | Engine boolean and coordinator state machine disagree on state | MITIGATED |
| **R-M5-11** | Async Pipeline Callback Race | Medium | Low | Late transfer completion fires after attempt timeout or abort | MITIGATED |
| **R-M5-12** | Drain Capacity & Operation Loss | High | Low | Pending queue overflow during drain causes dropped operations | MITIGATED |

---

## 3. Detailed Risk Analysis & Mitigation Specifications

### R-M5-01: Lamport Clock Regression or Artificial Inflation During Reseeding
- **Root Cause:** If snapshot reseeding incorrectly computes $L = L_{\text{snapshot}} + 1$, the local clock is artificially inflated above the causal frontier, causing local event generation to throw `CLOCK_INFLATION`. Conversely, if it assigns $L = L_{\text{snapshot}}$ when $L_{\text{local}} > L_{\text{snapshot}}$, the local clock regresses, violating Lamport monotonicity.
- **Architectural Impact:** Corrupts logical timestamps across the entire cluster, breaking deterministic tie-breaking in `ConflictResolver`.
- **Mitigation Strategy:**
  - Enforce exact mathematical formula in `SnapshotInstaller.stage()`:
    $$L_{\text{candidate}} = \max(L_{\text{local}}, L_{\text{snapshot}})$$
  - Never add $+1$ during snapshot installation.
  - In M5.4, test both branches: $L_{\text{local}} < L_{\text{snapshot}}$ and $L_{\text{local}} > L_{\text{snapshot}}$.
- **Automated Verification Control:** Unit tests in `recovery-anti-entropy-trigger.test.ts` assert exact clock equality post-installation.

---

### R-M5-02: Staged State Leaks During Snapshot Installation Failure (Closing R2-C1, R2-M2)
- **Root Cause:** If any step of candidate validation, log reset, or checkpoint ingestion throws an exception after mutating active engine state, the engine is left in a corrupted split-brain state.
- **Architectural Impact:** Irreversible desynchronization requiring process restart.
- **Mitigation Strategy:**
  - **Opaque Staged-Handle Architecture (Stage 2):** All candidate structures (`candidateCore: EngineCoreState`) are constructed completely in memory and stored in a private `WeakMap` inside `SnapshotInstaller`. The coordinator receives only an immutable `StagedSnapshotInstallation` handle.
  - **Single Synchronous Pointer Swap (Stage 3):** Committing executes as one synchronous reference replacement (`this.core = candidateCore;`) under JavaScript run-to-completion semantics inside `replaceCoreStateAtomic()`.
  - If staging throws, candidate objects are discarded and active state remains 100% untouched.
- **Automated Verification Control:** Simulated install failure tests in `recovery-anti-entropy-trigger.test.ts` verify that active engine state before failure equals active engine state after failure bit-exact.

---

### R-M5-03: Local Intent Inversion & Causal Skew from Queued Actions
- **Root Cause:** If local user actions are queued during recovery and executed after snapshot installation, they carry obsolete causal dependencies ($V_{\text{local}}$ from before recovery) and obsolete media timestamps.
- **Architectural Impact:** User pauses or seeks at ancient positions (e.g. 5s) after the room has advanced to 120s, causing state corruption and user intent inversion.
- **Mitigation Strategy:**
  - **Strict Synchronous Fail-Close:** Local actions are NEVER queued during recovery.
  - `receiveLocal()` and `submit()` synchronously throw `SyncEngineError('REJECTED_RECOVERING')` when `isRecoveryInProgress() === true`.
  - Applications receive an immediate error and can disable UI controls during recovery.
- **Automated Verification Control:** Unit tests in `recovery-local-gating.test.ts` verify synchronous exception throwing and clock/sequence invariance.

---

### R-M5-04: Quarantine Memory Exhaustion Under High-Concurrency Bursts (Closing R2-H2, R2-H3, R3-01, R3-04, R4-01, R13-D, R14-A, R15-G, R16-A, R16-D, R17-B)
- **Root Cause:** While catching up across a 4 MB snapshot over a high-latency link, active peers continue broadcasting live operations. A malicious or high-throughput peer could flood the recovering node with events, causing OOM.
- **Architectural Impact:** Browser tab crash or engine heap exhaustion.
- **Mitigation Strategy:**
  - Explicit bounds verified from source code in `QuarantineBuffer` (`quarantine-buffer.ts:26-30`):
    - Hard count limit: 1,000 operations (`MAX_QUARANTINE_OPERATIONS = 1000`).
    - Hard aggregate byte limit: 4 MiB (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`).
    - Individual event limit: 4 KiB (`MAX_QUARANTINE_EVENT_BYTES = 4096`).
    - Warning threshold: ~3.2 MiB (`QUARANTINE_WARNING_THRESHOLD_BYTES = 3355443`).
    - Note on Discrepancy Reconciliation (R4-01): Corrected inadvertent typo in M5.0-R3 report mentioning 64 MB; 4 MiB is the authoritative source baseline.
  - Explicit bounds in `FutureEpochBuffer`:
    - Hard count limit: 100 operations.
    - Hard aggregate byte limit: 512 KiB.
    - Individual event limit: 4 KiB.
  - **Browser-Compatible Wire Accounting (R3-01):** Wire byte lengths are measured directly at transport ingestion (`TextEncoder` / `rawSerializedLength`) without re-serializing with `JSON.stringify()`.
  - **Complete Overflow Contract (R3-04):** Buffer throws $\to$ `RecoveryCoordinator` catches, transitions to `DEGRADED`, and rethrows $\to$ `LeaderlessSyncEngine.receive()` isolates remote error to `REJECTED_CAPACITY` without throwing to transport callers. Rejected operations retain zero buffer memory and zero ownership.
- **Automated Verification Control:** Stress tests in `recovery-remote-interception.test.ts` injecting count- and byte-exceeding operations verify deterministic transition to `DEGRADED`, remote error isolation, and zero buffer retention.

---

### R-M5-05: Causal Dependency Deadlock During Post-Installation Drain
- **Root Cause:** A quarantined operation might depend on an intermediate operation that was neither included in the snapshot base nor received in quarantine (e.g. dropped by network).
- **Architectural Impact:** Drain loop stalls or throws if dependencies are unsatisfied.
- **Mitigation Strategy:**
  - `LeaderlessSyncEngine.admitDrainedOperation()` implements the complete causal dependency check.
  - If an operation has missing dependencies, it is enqueued into `engine.pendingQueue` with status `ACCEPTED_PENDING`.
  - It does NOT stall the drain pass! `QuarantineDrainer` proceeds to the next quarantined operation.
  - When the missing operation arrives or is repaired, `pendingQueue` naturally drains into the log.
- **Automated Verification Control:** Tests in `recovery-engine-integration.test.ts` verify out-of-order quarantined operations correctly enter `pendingQueue`.

---

### R-M5-06: Drain-Arrival Concurrency Race (Closing R16-C, R16-D)
- **Root Cause:** While `QuarantineDrainer` is draining pass 1, new remote events arrive over the network.
- **Architectural Impact:** Race conditions between dequeuing and enqueuing, or operations drained out of order.
- **Mitigation Strategy:**
  - **Generational Detachment:** `quarantineBuffer.detachActiveGeneration()` detaches the accumulated operations into an isolated drain batch. Newly arriving events enter a fresh active generation.
  - **In-Flight Fencing:** `QuarantineDrainer.isInFlight(origin, seq)` causes newly arriving duplicates of in-flight operations to be dropped immediately.
  - **Iterative Drain:** Coordinator executes subsequent drain passes until all generations are cleared.
- **Automated Verification Control:** Multi-peer concurrency tests in `recovery-engine-integration.test.ts` verify zero dropped or duplicated events during concurrent drain.

  - **Physical Custody Separation & Full Identity Matching (R16-C, R16-D):** Operations in flight are held strictly in QuarantineDrainer._inFlightRecord ({\text{in-flight}} \le 1$) and physically removed from QuarantineBuffer.drainBatch. Step 6 settlement barrier verifies empty in-flight slot ({\text{in-flight}} == 0$) before retirement begins. Index hits in QuarantineBuffer verify all 4 composite identity fields, preventing identity collisions.
---

### R-M5-07: Stale Timer Callbacks Post-Abort or Post-Disposal (Closing R2-H4)
- **Root Cause:** Asynchronous timeouts fire after the recovery attempt has already completed, aborted, or been disposed.
- **Architectural Impact:** Late callback aborts a new valid recovery attempt or throws uncaught exception.
- **Mitigation Strategy:**
  - Separate 4 timer categories: attempt timers (`overallTimer`), reconciliation timers (`reconciliationTimer`), offer timers (`offerAwaitTimer`), independent future-epoch grace timer (`futureEpochGraceTimer`).
  - Attempt-scoped callbacks check: `if (this.activeAttemptId !== attemptId || this._isDisposed) return;`.
  - Future-epoch convergence timer is NOT cancelled when a snapshot attempt completes or retries; cancelled only on epoch match, grace expiration, or disposal.
  - `clearTimers()` explicitly clears attempt and reconciliation timers on transition to `ACTIVE` or `DEGRADED`.
  - `dispose()` permanently cancels all timers.
- **Automated Verification Control:** Regression tests in `recovery-coordinator.test.ts` verifying timer cancellations and independent future-epoch timer persistence under rapid attempt resets.

---

### R-M5-08: Ambiguous Lifecycle Ownership, Package Boundary, Reset Failures & Teardown Leaks (Closing F5, R2-H5, R3-07, R6-02, R7-02, R8-01, R8-05, R9-07, R10-01, R11-B, R11-C, R11-D, R12-A–R12-D, R13-A–R13-E, R14-A–R14-E, R15-A–R15-G, R16-A–R16-D, R17-A–R17-C)
- **Root Cause:** Unclear lifecycle ownership where active timers retain the coordinator, improper capability checks allow unauthorized replacement, or administrative reset operations fail partway through without failure-atomic cleanup.
- **Architectural Impact:** Retained background processes, hanging test suites, unauthorized core state replacement, or inconsistent partial state exposed after failed reset.
- **Mitigation Strategy:**
  - **Single Construction Authority:** `LeaderlessSyncEngine` owns `RecoveryCoordinator`.
  - **Single-Attachment Invariant:** Calling `bindEngine()` when already bound throws `SyncEngineError('ALREADY_BOUND')`. Sharing coordinators across engines is prohibited.
  - **Capability Custody & Package Boundary (R3-07, R7-01, R8-05, R9-07):** Checked strictly via object identity (`capability === this.authorizedRecoveryCapability`). `package.json` exports only root `.`, prohibiting supported public package subpath imports of internal custody modules. Capability instances and containers shallow-frozen with `Object.freeze()`. `bindEngine()` is strictly an internal composition-root hook. Zero public capability getters or diagnostic leaks. Least privilege enforced: Coordinator holds local replay authority; Drainer holds repair batch authority.
  - **Failure-Atomic Reset Semantics (R6-02, R7-02):**
    - `PURGE_AND_RETRY`: monotonic attempt counter increment (`currentAttemptId++`) fences all async callbacks and timers from prior attempts; synchronous drainer reset cancels in-flight record ($N_{\text{in-flight}} \le 1$); notifications are permanently suppressed.
    - `REINITIALIZE_CLEAN`: strict 5-step M4.3c sequence; if any post-install step fails, catch block immediately restores `engine.setRecoveryMode(true)`, engine remains in `DEGRADED`, notifications are never flushed, and clean baseline swap remains irrevocable without rolling back to corrupt pre-reset root.
  - **Multi-Stage Retirement Transaction with Pre-Reservation (R12-A):** Resolves capacity mismatch between quarantine (1,000 ops / 4 MiB) and archive (500 entries / 2 MiB). Pre-verifies required entries/bytes against available archive capacity; insufficient capacity halts in `DEGRADED` without mutating engine core or buffer.
  - **Dedicated Archive Administration Capability (R12-C):** `TerminalArchiveAdministrationCapability` separates diagnostic archive export/clear from engine root installation authority. 2-stage export (prepareExport produces detached snapshot without clearing; acknowledgeArchiveExport clears only confirmed entries).
  - **Unambiguous Custody Resolution & Archive Limits (R10-01, R11-C, R12-D):** No operation disappears between buffer and in-flight ownership; rejected operations transferred to `TerminalQuarantineArchive` (500 entries / 2 MiB / 4 KiB max entry without silent eviction; overflow degrades fail-closed) or restored to `QuarantineBuffer`.
  - **Reset-Policy Quarantine Fate (R11-D):** `PURGE_AND_RETRY` preserves un-drained buffer ops for retry; `REINITIALIZE_CLEAN` atomically commits pre-reset buffer ops to `TerminalQuarantineArchive` (`REINITIALIZE_CLEAN_PURGED`), prohibiting silent replay into clean baseline.
  - **Deterministic Disposal Propagation:** Calling `engine.dispose()` synchronously calls `this.recoveryCoordinator?.dispose()`.
  - Coordinator disposal is synchronous and idempotent: cancels all timers, clears all buffer queues, unregisters all callbacks, nullifies references, and marks `_isDisposed = true`.
- **Automated Verification Control:** Teardown and reset failure tests in `recovery-lifecycle-binding.test.ts` verifying zero active timers, listener leaks, attempt fencing, and notification suppression post-failure.

---

### R-M5-09: Incomparable Frontier Lattice Fork, Compacted Snapshot Reordering Hazard & Capability-Gated Provenance (Closing F4, R2-C3, R2-C4, R2-H1, R3-02, R3-05, R4-02, R5-01, R5-02, R6-01, R6-03, R7-03)
- **Root Cause:** Vector frontiers form a partially ordered product lattice ($\mathbb{N}^k$). Incomparable frontiers ($V_A \parallel V_B$) are not necessarily forks; treating them as automatic forks causes unnecessary degradation. Conversely, blindly overwriting active state with an incomparable snapshot destroys unrepresented local operations. Furthermore, if a candidate snapshot has already compacted an operation $B$ ($L(B)=11$) into its base state, replaying an uncompacted concurrent local operation $A$ ($L(A)=10$) on top of the snapshot evaluates $B \to A$ out of canonical SMR order ($A \prec B$). Because `ConflictResolver` operations (e.g. `PAUSE`, `PLAY`, `SEEK`) are not universally commutative, this reordering hazard yields divergent playback states. Additionally, if replay trust context were accepted via a caller-supplied string (`'LOCAL_REPLAY'`), or if capability possession alone were treated as proof of payload provenance without queue isolation, an untrusted caller could inject remote operations into local replay or bypass arrival-rate and clock-inflation checks.
- **Architectural Impact:** Canonical SMR total order inversion, accidental trust escalation, state divergence across nodes, unnecessary recovery degradation, or loss of local progress.
- **Mitigation Strategy:**
  - **Canonical Ordering Feasibility Check (R5-02):** Stashed local operations must satisfy $L(A) > \text{snapshot.maxLogicalTimestamp}$ (with `maxLogicalTimestamp` representing maximum Lamport timestamp across entire compacted history) or $\forall p \in \text{compacted origins},\ L(A) > \text{originFrontierTimestamps}[p]$ (with every compacted origin represented and mapped to the exact timestamp of its highest compacted sequence). If $L(A) \le \text{snapshot.maxLogicalTimestamp}$ and $A$ is concurrent with operations folded into the snapshot base, an explicit ordering hazard is detected.
  - **Hazard Resolution:**
    - Priority 1: Reject snapshot replacement and initiate anti-entropy log repair (`RepairRequestPipeline`) from a common ancestor so all operations are ordered canonically in the log before compaction.
    - Priority 2 (Explicit Incompatibility Gate): If log repair is impossible (history truncated across all peers), abort recovery deterministically to `DEGRADED` with `SyncEngineError('INCOMPATIBLE_COMPACTED_HISTORY')`. Out-of-order replay is strictly prohibited. Causal dependencies must still be validated by the engine upon replay.
  - **Safe Incomparable Recovery & Deterministic Replay (R3-02, R4-02):** A snapshot must NEVER replace active state if doing so would discard compatible local operations! Prioritize anti-entropy delta log repair. If snapshot replacement is necessary, stash local operations whose sequence numbers are ahead of the snapshot, install snapshot, and sequentially re-admit stashed operations strictly via `admitDrainedOperation(op, capability)`.
  - **Capability Authority vs. Operation Provenance (R6-01, R6-03, R7-01, R7-03):** Capability possession establishes caller authority to request admission mode, but does NOT validate payload provenance. Operations admitted under `TrustedLocalReplayCapability` must originate strictly and exclusively from `stashedLocalQueue` (populated strictly from immutable local log entries; remote operations prohibited from entering or being relabeled as local); identity hash $H(op) == H(log)$ and causal dependencies verified against snapshot base, immediately aborting to `DEGRADED` on conflict. Replay trust is derived strictly from unforgeable capability custody (`DrainAdmissionContext = TrustedLocalReplayCapability | TrustedRepairBatchCapability`). `QuarantineDrainer` is initialized strictly with `TrustedRepairBatchCapability` and cannot obtain `TrustedLocalReplayCapability`. Local reconciliation presents `TrustedLocalReplayCapability`. The engine checks strict object-reference identity (`===`). Zero wire fields or metadata can influence trust. Missing, invalid, or forged capabilities throw `SyncEngineError('UNAUTHORIZED')`.
  - **Deterministic 6-Category Replay Contract (R4-02, R5-01, R5-02, R6-01):** Cat 1 (already represented: discarded), Cat 2 (unrepresented satisfied: applied/reduced), Cat 3 (concurrent with frontier: deterministic total-order SMR reduction via `compareOperations`), Cat 4 (missing dependencies: buffered in `pendingQueue` / delta repair), Cat 5 (duplicates: discarded), Cat 6 (validation failures: remote quarantined discarded, locally committed stashed aborts to `DEGRADED`).
  - **Preservation of 8 Replay Invariants (R4-02, R6-01):** Never silently discard committed local progress; never invert canonical SMR order on compacted snapshots; never apply twice; never fabricate dependencies; preserve event ID/sequence; enforce capability-gated trust; single-owner $N_{\text{in-flight}} \le 1$ drain; single admission pipeline; abort when safe replay is unprovable.
  - **Post-Installation Failure Handling (R4-02):** If replay fails after snapshot installation, coordinator transitions to `DEGRADED` (`DRAIN_FAILED`), `engine.setRecoveryMode(true)` is maintained, deferred notifications are NEVER flushed, and `inFlightRecord` is preserved.
  - **Evidence Requirement (R3-02):** Matching frontier fingerprints alone are insufficient; full checkpoint hash chains or frontier timestamps are required to verify history compatibility.
  - **Deterministic Peer Comparison (R2-H1, R3-05):** Ordinal ASCII code point comparison (`comparePeerIds`) over the validated domain `/^[a-zA-Z0-9_.:-]+$/`. `localeCompare` is strictly prohibited.
  - **Cooperative Trust Model (R2-C4):** SHA-256 verifies byte-level slice and payload integrity against transport corruption, but does not prove cryptographic author authentication.
  - **Fork Detection:** Abort to `DEGRADED` only upon proven causal divergence ($H(op_A) \ne H(op_B)$) or unresolvable history.
- **Automated Verification Control:** Unit tests in `recovery-anti-entropy-trigger.test.ts` verifying canonical ordering feasibility, capability-gated trust admission, operation provenance isolation, delta repair prioritization, and local operation preservation.

---

### R-M5-10: Lifecycle State Desynchronization, Notification Dispatch & Irreversible Effects (Closing F1, F7, R2-C2, R3-03, R5-03, R6-02, R7-01, R7-02, R8-03, R8-04, R9-03, R9-05, R9-06, R10-03, R10-04, R11-B, R11-D)
- **Root Cause:** If the engine maintains an independent mutable boolean alongside the coordinator's state machine, or if completion handoff ordering is inverted, listeners can observe inconsistent states. If callers confuse `!isRecoveryInProgress()` with healthy active operation during `DEGRADED` states, operations could be incorrectly submitted. If `resetRecovery()` acts as an unvalidated escape hatch, incomplete state could be exposed as healthy.
- **Architectural Impact:** Engine admits remote events while coordinator is recovering, callers submit intents while degraded, incomplete states exposed as healthy, or listeners observe `ACTIVE` state while engine still rejects local inputs.
- **Mitigation Strategy:**
  - **Three-Tier Architecture (R3-03, R5-03):** Coordinator state machine is Tier 1 sole lifecycle authority (`recoveryState`). Public engine queries are Tier 2 delegators: `isRecoveryInProgress()` returns `true` strictly in `RECOVERING` and `RECONCILING`; `isDegraded()` returns `true` strictly in `DEGRADED`. Callers must never infer health from `!isRecoveryInProgress()`. `this.recoveryActive` is strictly a Tier 3 slave operational gate for notification deferral, remaining held in locked mode when `DEGRADED`.
  - **Administrative Reset Guardrails (R6-02, R7-02):** `resetRecovery(capability, resetAction)` requires authorized `RecoveryInstallationCapability` checked via object identity, is permitted strictly from `DEGRADED`, and enforces failure-atomic reset semantics (`PURGE_AND_RETRY` with monotonic attempt counter fencing vs `REINITIALIZE_CLEAN` with 5-step M4.3c sequence and irrevocable clean root swap). It never exposes an incomplete state as healthy.
  - **M4.3c Completion Sequence & Non-Throwing Dispatch (R2-C2, R8-04):** Drain completes $\to$ bookkeeping completes $\to$ `engine.setRecoveryMode(false)` $\to$ `coordinator.transition('ACTIVE')` $\to$ non-throwing `flushDeferredNotifications()` with isolated listener errors.
  - If transition throws before ACTIVE, catch block restores `engine.setRecoveryMode(true)`, aborts recovery, and does NOT flush notifications.
  - **Explicit Observer Boundary & Zero Notification (R8-04, R9-06, R10-04, R11-D):** Once ACTIVE is reached, state transition is externally observable; delivered listener effects cannot be retracted. In `REINITIALIZE_CLEAN`, Step 1 purges stale notifications, Step 2 atomically installs clean baseline, Step 3 disengages recovery mode, Step 4 transitions coordinator to ACTIVE, and Step 5 dispatches notifications strictly after ACTIVE (acknowledging zero notifications may be delivered).
  - **Candidate Isolation & CAS Root Verification (R10-03, R11-B):** `admitDrainedOperation` enforces fresh/CoW mutable nested instances without mutating active root references; pre-commit asserts CAS `this.core === expectedPriorRoot` and attempt ID; Phase 2 commits via atomic reference assignment (`this.core = candidateCore`), ensuring absolute exception atomicity.
  - **Synchronous Indivisibility & Acceptance Linearization (R9-03, R9-05):** `admitDrainedOperation` executes synchronously in a single call frame under run-to-completion semantics. Operations admitted before reset remain in the log; stale continuations perform zero new state mutations.
  - **Capability Branding & Custody (R7-01):** `DRAIN_ADMISSION_BRAND: unique symbol` is module-private to `drain-admission-custody.ts` and non-exported in public package entry point. Capability instances are deeply frozen with `Object.freeze()` and transferred privately via `AuthorizedDrainCapabilities` during `bindEngine()`. In-realm defensive trust model prevents API misuse and duck-typing.
- **Automated Verification Control:** State consistency tests in `recovery-lifecycle-binding.test.ts` verifying listener observation guarantees, `isDegraded()` query behavior, capability-authorized `resetRecovery()` safety, and handoff rollback.

---

### R-M5-11: Asynchronous Pipeline Callback Race (Closing F10)
- **Root Cause:** Inbound snapshot transfer is asynchronous. If `SnapshotInboundPipeline.onTransferComplete()` fires after the coordinator has aborted recovery due to timeout or supplier disconnect, late installation could corrupt the engine.
- **Architectural Impact:** Stale snapshot committed to engine while in `DEGRADED` or new recovery attempt.
- **Mitigation Strategy:**
  - **Attempt Fencing:** `onTransferComplete(result, attemptId)` verifies `attemptId === coordinator.currentAttemptId`.
  - **State Guard:** Verifies `coordinator.stateMachine.state === 'RECOVERING'`. If not, callback is dropped.
  - **One-Shot Latch:** Inbound pipeline enforces a one-shot completion latch (`hasCompleted = true`) per transfer session.
- **Automated Verification Control:** Callback race tests in `recovery-anti-entropy-trigger.test.ts` simulating late transfer resolution.

---

### R-M5-12: Drain Capacity Exhaustion & Pending Queue Limit (Closing R2-H6, R3-06)
- **Root Cause:** During quarantine drain, admitted operations that are causal dependencies are forwarded to `engine.pendingQueue`. If `pendingQueue` capacity is reached, operations could be dropped if dequeued eagerly.
- **Architectural Impact:** Silent operation loss during post-recovery reconciliation.
- **Mitigation Strategy:**
  - **$N_{\text{in-flight}} \le 1$ Ownership:** `QuarantineDrainer` retains dequeued operation in `inFlightRecord` until confirmed.
  - **Capacity Rejection & Post-Installation Failure Handling (R4-02):** Destination returns `CAPACITY_REJECTED`. The operation is preserved in `inFlightRecord`, drain halts immediately (`haltedOnCapacity = true`), and recovery transitions safely to `DEGRADED` (`substate: 'DRAIN_FAILED'`). Operational notification deferral (`engine.setRecoveryMode(true)`) remains locked; deferred notifications are NEVER flushed.
  - **Identified Open Design Issue (R3-06):** `PendingDependencyQueue` enforces count capacity ($K=256$), but has no aggregate byte limit. Documented as an open design issue for post-M5 hardening.
- **Automated Verification Control:** Capacity tests in `recovery-engine-integration.test.ts` verifying zero operation loss when `pendingQueue` capacity is exhausted during drain.
