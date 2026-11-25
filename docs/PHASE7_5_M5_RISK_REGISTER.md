# Phase 7.5 Milestone M5 — Risk Register

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R2 AMENDED)
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

### R-M5-04: Quarantine Memory Exhaustion Under High-Concurrency Bursts (Closing R2-H2, R2-H3)
- **Root Cause:** While catching up across a 4 MB snapshot over a high-latency link, active peers continue broadcasting live operations. A malicious or high-throughput peer could flood the recovering node with events, causing OOM.
- **Architectural Impact:** Browser tab crash or engine heap exhaustion.
- **Mitigation Strategy:**
  - Explicit bounds in `QuarantineBuffer`:
    - Hard count limit: 1,000 operations (`MAX_QUARANTINE_OPERATIONS = 1000`).
    - Hard aggregate byte limit: 4 MiB (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`).
    - Individual event limit: 4 KiB (`MAX_QUARANTINE_EVENT_BYTES = 4096`).
  - Explicit bounds in `FutureEpochBuffer`:
    - Hard count limit: 100 operations.
    - Hard aggregate byte limit: 512 KiB.
    - Individual event limit: 4 KiB.
  - Unified overflow contract (R2-H2): buffer throws `QUARANTINE_OVERFLOW` / `FUTURE_EPOCH_OVERFLOW`. `RecoveryCoordinator` catches, autonomously aborts recovery to `DEGRADED`, and rethrows. `LeaderlessSyncEngine.receive()` catches, isolates remote error to `REJECTED_CAPACITY`, and records decision without throwing to transport callers.
- **Automated Verification Control:** Stress tests in `recovery-remote-interception.test.ts` injecting count- and byte-exceeding operations verify deterministic transition to `DEGRADED` and remote error isolation.

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

### R-M5-08: Ambiguous Lifecycle Ownership and Teardown Leaks (Closing F5, R2-H5)
- **Root Cause:** Unclear lifecycle ownership where active timers or detached event listeners retain the coordinator or engine in memory indefinitely after application shutdown.
- **Architectural Impact:** Retained background processes, hanging test suites, and memory growth across repeated session lifecycles.
- **Mitigation Strategy:**
  - **Single Construction Authority:** `LeaderlessSyncEngine` owns `RecoveryCoordinator`.
  - **Single-Attachment Invariant:** Calling `bindEngine()` when already bound throws `SyncEngineError('ALREADY_BOUND')`. Sharing coordinators across engines is prohibited.
  - **Deterministic Disposal Propagation:** Calling `engine.dispose()` synchronously calls `this.recoveryCoordinator?.dispose()`.
  - Coordinator disposal is synchronous and idempotent: cancels all timers, clears all buffer queues, unregisters all callbacks, nullifies references, and marks `_isDisposed = true`.
- **Automated Verification Control:** Teardown test in `recovery-lifecycle-binding.test.ts` verifying zero active timers or listener leaks post-dispose.

---

### R-M5-09: Incomparable Frontier Lattice Fork vs. Compatible Concurrent Branches (Closing F4, R2-C3, R2-C4, R2-H1)
- **Root Cause:** Vector frontiers form a partially ordered product lattice ($\mathbb{N}^k$). Incomparable frontiers ($V_A \parallel V_B$) are not necessarily forks; treating them as automatic forks causes unnecessary degradation. Conversely, ordering them via arbitrary metrics causes split-brain state divergence.
- **Architectural Impact:** Unnecessary recovery degradation or partition divergence where different peers install mutually contradictory snapshots.
- **Mitigation Strategy:**
  - **Differentiated Classification (R2-C3):** Distinguish strict domination, compatible concurrent branches, repairable gaps, and proven conflicting forks ($H(op_A) \ne H(op_B)$).
  - **Deterministic Peer Comparison (R2-H1):** When tie-breaking equal frontiers, use lowest RTT followed by protocol-stable code-point peer ID comparison (`comparePeerIds`). `localeCompare` is strictly prohibited.
  - **Cooperative Trust Model (R2-C4):** SHA-256 verifies byte-level slice and payload integrity against transport corruption, but does not prove cryptographic author authentication.
  - **Fork Detection:** Abort to `DEGRADED` only when true causal divergence is proven or unresolved history exceeds the repair horizon under recovery policy.
- **Automated Verification Control:** Unit tests in `recovery-anti-entropy-trigger.test.ts` verifying correct classification of compatible branches vs. proven forks.

---

### R-M5-10: Lifecycle State Desynchronization & Completion Handoff (Closing F1, F7, R2-C2)
- **Root Cause:** If the engine maintains an independent mutable boolean (`recoveryActive`) alongside the coordinator's state machine (`stateMachine.state`), or if completion handoff ordering is inverted, listeners can observe inconsistent states.
- **Architectural Impact:** Engine admits remote events while coordinator is recovering, or listeners observe `ACTIVE` state while engine still rejects local inputs.
- **Mitigation Strategy:**
  - `RecoveryCoordinator.stateMachine.state` is the **single source of truth** (`recoveryState`).
  - `engine.isRecoveryInProgress()` queries the coordinator directly.
  - **M4.3c Completion Sequence (R2-C2):** Drain completes $\to$ bookkeeping completes $\to$ `engine.setRecoveryMode(false)` $\to$ `coordinator.transition('ACTIVE')` $\to$ `flushDeferredNotifications()`.
  - If transition throws, catch block immediately restores `engine.setRecoveryMode(true)`, aborts recovery, and does NOT flush notifications.
- **Automated Verification Control:** State consistency tests in `recovery-lifecycle-binding.test.ts` verifying listener observation guarantees and handoff rollback.

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

### R-M5-12: Drain Capacity Exhaustion & Operation Loss Prevention (Closing R2-H6)
- **Root Cause:** During quarantine drain, admitted operations that are causal dependencies are forwarded to `engine.pendingQueue`. If `pendingQueue` capacity is reached, operations could be dropped if dequeued eagerly.
- **Architectural Impact:** Silent operation loss during post-recovery reconciliation.
- **Mitigation Strategy:**
  - **$N_{\text{in-flight}} \le 1$ Ownership:** `QuarantineDrainer` retains dequeued operation in `inFlightRecord` until confirmed.
  - **Capacity Rejection Handling:** Destination returns `CAPACITY_REJECTED`. The operation is restored to queue, drain halts immediately (`haltedOnCapacity = true`), and recovery transitions safely to `DEGRADED`.
  - An operation is never marked applied merely because it was dequeued.
- **Automated Verification Control:** Capacity tests in `recovery-engine-integration.test.ts` verifying zero operation loss when `pendingQueue` capacity is exhausted during drain.
