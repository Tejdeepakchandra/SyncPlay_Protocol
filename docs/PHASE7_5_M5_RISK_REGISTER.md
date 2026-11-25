# Phase 7.5 Milestone M5 — Risk Register

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R1 AMENDED)  
**Date:** 2026-11-25  
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)  
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

### R-M5-02: Staged State Leaks During Snapshot Installation Failure
- **Root Cause:** If any step of candidate validation, log reset, or checkpoint ingestion throws an exception after mutating active engine state, the engine is left in a corrupted split-brain state.
- **Architectural Impact:** Irreversible desynchronization requiring process restart.
- **Mitigation Strategy:**
  - Strictly enforce **Stage 2 Out-of-Band Staging**: All candidate structures (`candidateCore: EngineCoreState`) are constructed completely in memory without referencing or mutating active engine properties.
  - **Single Synchronous Pointer Swap (Stage 3):** Committing is a single JavaScript statement: `this.core = candidateCore;` inside `replaceCoreStateAtomic()`.
  - If staging throws, candidate objects are garbage collected and active state remains 100% untouched.
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

### R-M5-04: Quarantine Memory Exhaustion Under High-Concurrency Bursts
- **Root Cause:** While catching up across a 4 MB snapshot over a high-latency link, active peers continue broadcasting live operations. A malicious or high-throughput peer could flood the recovering node with events, causing OOM.
- **Architectural Impact:** Browser tab crash or engine heap exhaustion.
- **Mitigation Strategy:**
  - Dual bounded constraints in `QuarantineBuffer`:
    - Hard count limit: 1,000 operations (`MAX_QUARANTINE_OPERATIONS = 1000`).
    - Hard aggregate byte limit: 4 MiB (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`).
    - Individual event limit: 4 KiB (`MAX_QUARANTINE_EVENT_BYTES = 4096`).
  - If any limit is exceeded, `QuarantineBuffer` throws `SyncEngineError('QUARANTINE_OVERFLOW')`.
  - `RecoveryCoordinator` catches the error and autonomously aborts recovery to `DEGRADED`.
- **Automated Verification Control:** Stress tests in `recovery-remote-interception.test.ts` injecting 1,001 operations verify deterministic transition to `DEGRADED`.

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

### R-M5-06: Drain-Arrival Concurrency Race
- **Root Cause:** While `QuarantineDrainer` is draining pass 1, new remote events arrive over the network.
- **Architectural Impact:** Race conditions between dequeuing and enqueuing, or operations drained out of order.
- **Mitigation Strategy:**
  - **Generational Detachment:** `quarantineBuffer.detachActiveGeneration()` detaches the accumulated operations into an isolated drain batch. Newly arriving events enter a fresh active generation.
  - **In-Flight Fencing:** `QuarantineDrainer.isInFlight(origin, seq)` causes newly arriving duplicates of in-flight operations to be dropped immediately.
  - **Iterative Drain:** Coordinator executes subsequent drain passes until all generations are cleared.
- **Automated Verification Control:** Multi-peer concurrency tests in `recovery-engine-integration.test.ts` verify zero dropped or duplicated events during concurrent drain.

---

### R-M5-07: Stale Timer Callbacks Post-Abort or Post-Disposal
- **Root Cause:** Asynchronous timeouts (`overallRecoveryTimeoutMs`, `reconciliationTimeoutMs`, `epochConvergenceGraceMs`) fire after the recovery attempt has already completed, aborted, or been disposed.
- **Architectural Impact:** Late callback aborts a new valid recovery attempt or throws uncaught exception.
- **Mitigation Strategy:**
  - Monotonic `activeAttemptId` incremented on every attempt, abort, or reset.
  - Timers capture `attemptId` at scheduling time and check:
    `if (this.activeAttemptId !== attemptId || this._isDisposed) return;`
  - `clearTimers()` explicitly clears all timers on transition to `ACTIVE` or `DEGRADED`.
  - `dispose()` permanently cancels all timers.
- **Automated Verification Control:** Regression test in `recovery-coordinator.test.ts` verifying timer cancellations under rapid abort/reset.

---

### R-M5-08: Ambiguous Lifecycle Ownership and Teardown Leaks (Closing F5)
- **Root Cause:** In modern V8 engines, circular references between unreachable objects are garbage-collected. The actual architectural hazard is **unclear lifecycle ownership**, where active timers (`setInterval`/`setTimeout`) or detached event listeners retain the coordinator or engine in memory indefinitely after application shutdown.
- **Architectural Impact:** Retained background processes, hanging test suites, and memory growth across repeated session lifecycles.
- **Mitigation Strategy:**
  - **Single Construction Authority:** `LeaderlessSyncEngine` owns `RecoveryCoordinator`.
  - **Deterministic Disposal Propagation:** Calling `engine.dispose()` synchronously calls `this.recoveryCoordinator?.dispose()`.
  - Coordinator disposal:
    - Cancels and clears all active timers (`overallTimer`, `offerAwaitTimer`, `reconciliationTimer`).
    - Clears all internal buffer queues (`quarantineBuffer`, `futureEpochBuffer`).
    - Unregisters all internal callbacks and nullifies references.
    - Permanently marks `_isDisposed = true`.
- **Automated Verification Control:** Teardown test in `recovery-lifecycle-binding.test.ts` verifying zero active timers or listener leaks post-dispose.

---

### R-M5-09: Incomparable Frontier Lattice Fork (Closing F4)
- **Root Cause:** Vector frontiers form a partially ordered product lattice ($\mathbb{N}^k$). If two peers advertise dominating frontiers that are mutually incomparable ($V_A \parallel V_B$), attempting to order them using arbitrary metrics (such as peer ID or timestamp) creates a silent split-brain fork.
- **Architectural Impact:** Partition divergence where different peers install mutually contradictory snapshots.
- **Mitigation Strategy:**
  - **Strict Domination Ranking:** Only strictly dominating frontiers ($V_A > V_B$) are ranked higher.
  - **Fork Detection:** If two dominating candidate frontiers are mutually incomparable, the coordinator checks historical prefix fingerprints across common origins:
    - If a prefix conflict is detected (`HISTORY_CONFLICT`), or neither candidate subsumes the other, the coordinator aborts recovery to `DEGRADED` (`FORK_DETECTED_INCOMPARABLE`).
    - The engine **NEVER** fabricates a total order between incomparable frontiers.
- **Automated Verification Control:** Unit tests in `recovery-anti-entropy-trigger.test.ts` verifying deterministic transition to `DEGRADED` upon detecting incomparable candidate frontiers.

---

### R-M5-10: Lifecycle State Desynchronization (Closing F1)
- **Root Cause:** If the engine maintains an independent mutable boolean (`recoveryActive`) alongside the coordinator's state machine (`stateMachine.state`), asynchronous updates or partial errors can cause them to disagree.
- **Architectural Impact:** Engine admits remote events while coordinator is recovering, or engine rejects local inputs while coordinator is active.
- **Mitigation Strategy:**
  - `RecoveryCoordinator.stateMachine.state` is the **single source of truth** (`recoveryState`).
  - `engine.isRecoveryInProgress()` queries the coordinator directly.
  - The mutable engine boolean is eliminated from serving as an independent authority.
- **Automated Verification Control:** State consistency tests in `recovery-lifecycle-binding.test.ts` verifying that `engine.isRecoveryInProgress()` and `coordinator.state` never diverge.

---

### R-M5-11: Asynchronous Pipeline Callback Race (Closing F10)
- **Root Cause:** Inbound snapshot transfer is asynchronous. If `SnapshotInboundPipeline.onTransferComplete()` fires after the coordinator has aborted recovery due to timeout or supplier disconnect, late installation could corrupt the engine.
- **Architectural Impact:** Stale snapshot committed to engine while in `DEGRADED` or new recovery attempt.
- **Mitigation Strategy:**
  - **Attempt Fencing:** `onTransferComplete(result, attemptId)` verifies `attemptId === coordinator.currentAttemptId`.
  - **State Guard:** Verifies `coordinator.stateMachine.state === 'RECOVERING'`. If not, callback is dropped.
  - **One-Shot Latch:** Inbound pipeline enforces a one-shot completion latch (`hasCompleted = true`) per transfer session.
- **Automated Verification Control:** Callback race tests in `recovery-anti-entropy-trigger.test.ts` simulating late transfer resolution.
