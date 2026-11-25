# Phase 7.5 Milestone M5 — Acceptance Criteria & Invariants

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0)  
**Date:** 2026-11-25  
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)  
**Scope:** Acceptance criteria, non-negotiable invariants, and verification matrix for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Non-Negotiable Architectural Invariants

Every sub-milestone implementation within Milestone M5 must adhere strictly to the following nine non-negotiable invariants:

| Invariant ID | Title | Invariant Definition | Enforcement Mechanism |
|---|---|---|---|
| **INV-M5-01** | Inactive Transparency | When recovery is inactive (`ACTIVE`), the engine behaves 100% identically to Phase 7.4. Zero performance overhead or regression on the normal operation path. | `admissionController.interceptRemoteEvent()` returns `ADMIT_ACTIVE` immediately; normal `receiveRemote()` executes unhindered. |
| **INV-M5-02** | Zero Remote State Mutation During Recovery | Incoming remote operations must NEVER mutate active playback state, insert into `OrderedOperationLog`, or advance `replicationVector` while recovery is in progress (`RECOVERING` or `RECONCILING`). | `LeaderlessSyncEngine.receive()` intercepts remote events before `receiveRemote()`; current epoch events are held in `QuarantineBuffer`. |
| **INV-M5-03** | Local Input Fail-Close | Local user operations must NOT bypass recovery admission controls or create concurrent divergence while catching up. Local intents must fail-close synchronously. | `receiveLocal()` and `submit()` query `coordinator.interceptLocalSubmission()`; throw `SyncEngineError('REJECTED_RECOVERING')` if not `ACTIVE`. |
| **INV-M5-04** | Atomic State Replacement | Snapshot installation must be single-tick atomic from the engine's observable state perspective across all 12 core state properties. No intermediate partially-applied states are visible. | `replaceCoreStateAtomic(candidateCore, capability)` executes single synchronous pointer replacement (`this.core = candidateCore`) in 0 event loop ticks. |
| **INV-M5-05** | Exact Lamport Reseed Monotonicity | Lamport clock reseeding upon snapshot installation must strictly evaluate: $L_{\text{local}} = \max(L_{\text{local}}, L_{\text{snapshot}})$. Under no circumstances may an artificial $+1$ increment be added during snapshot installation. | `SnapshotInstaller.stage()` sets `candidateClockManager = new LogicalClockManager(Math.max(currentClock, maxLogicalTimestamp))`. |
| **INV-M5-06** | Causal Vector Advance | Replication vectors must advance ONLY after valid, causally admissible operations are accepted and reduced into the operation log. | `replicationVector` is updated strictly within `admitDrainedOperation()` / `admitOperation()` upon `ACCEPTED_REDUCED`. |
| **INV-M5-07** | Authoritative Drain Admission | Quarantined operations must be drained strictly through the engine's existing causal-validation path (`admitDrainedOperation()`). An operation is never marked applied merely because it was dequeued. | `QuarantineDrainer` enforces $N_{\text{in-flight}} \le 1$ with synchronous `DestinationAcceptanceReceipt` confirmation. |
| **INV-M5-08** | Rollback & Failure Isolation | Failed or aborted recovery must NEVER leave the engine partially switched into a new state. Candidate objects must be discarded and the active state must remain 100% untouched. | Out-of-band staging: candidate core state is held in `stagedRoots`; failure prior to `commitAtomic` drops candidate without mutating active core. |
| **INV-M5-09** | Backward Compatibility | All existing Phase 1–7.4 behaviors, wire schemas, error codes, and frozen M4.3d behaviors must remain completely intact. | Automated regression test runs on all 67 test files and 1,550 monorepo tests. |

---

## 2. Milestone M5 Acceptance Criteria

### Category 1: Lifecycle & State Consistency (Sub-Milestone M5.1)
- **AC-M5-01 (Lifecycle Authority):** When a `RecoveryCoordinator` is attached, `engine.isRecoveryActive()` returns `true` if and only if `coordinator.state` is `'RECOVERING'` or `'RECONCILING'`.
- **AC-M5-02 (Zero Duplicate State):** Mutating recovery state occurs exclusively through `RecoveryStateMachine.transition()`; the engine contains no independently mutable boolean flags that can desynchronize.
- **AC-M5-03 (Notification Deferral):** State change notifications (`stateChangeListeners`) and decision notifications (`decisionListeners`) are buffered while `isRecoveryActive() === true`.
- **AC-M5-04 (Coalesced Notification Flush):** When recovery returns to `'ACTIVE'`, `engine.flushDeferredNotifications()` dispatches exactly one coalesced `PlaybackState` representing the final reconciled state.
- **AC-M5-05 (Disposal Propagation):** Invoking `engine.dispose()` automatically cancels all coordinator timers and disposes the attached coordinator.

### Category 2: Inbound Interception & Quarantine Buffering (Sub-Milestone M5.2)
- **AC-M5-06 (Remote Operation Quarantine):** In `RECOVERING` or `RECONCILING` state, valid remote events matching the current epoch are enqueued into `QuarantineBuffer` and recorded as `BUFFERED_QUARANTINE`.
- **AC-M5-07 (Zero Log Mutation):** While in `RECOVERING`, receiving 100 remote events results in `engine.operationLog.size` remaining 0 and `engine.playbackState` remaining completely unchanged.
- **AC-M5-08 (Future Epoch Staging):** Remote events claiming a future epoch ($E_{\text{event}} > E_{\text{engine}}$) are placed into `FutureEpochBuffer` with a 5-second grace period timer.
- **AC-M5-09 (Obsolete Epoch Discard):** Remote events claiming an obsolete epoch ($E_{\text{event}} < E_{\text{engine}}$) are dropped with zero state, anti-entropy, or log side effects.
- **AC-M5-10 (Quarantine Overflow Degradation):** If cumulative quarantined operations exceed 1,000 operations or 4 MiB aggregate bytes, recovery aborts deterministically to `DEGRADED` with reason `QUARANTINE_OVERFLOW`.
- **AC-M5-11 (Active State Pass-Through):** When coordinator is in `ACTIVE` state, remote events bypass quarantine and are processed by standard `receiveRemote()` without added latency.

### Category 3: Local Input Gating (Sub-Milestone M5.3)
- **AC-M5-12 (Local Synchronous Rejection):** Invoking `engine.receiveLocal()` or `engine.submit()` while `isRecoveryActive() === true` throws a synchronous `SyncEngineError('REJECTED_RECOVERING', ...)`.
- **AC-M5-13 (Degraded Input Rejection):** When in `DEGRADED` state, local submissions throw `SyncEngineError('REJECTED_DEGRADED', ...)`.
- **AC-M5-14 (Clock & Sequence Invariance):** A rejected local submission does not advance `engine.localOriginSequence` or mutate `engine.clockManager.clock`.
- **AC-M5-15 (Zero Causal Skew):** Local operations cannot be generated during recovery with obsolete causal dependencies.

### Category 4: Anti-Entropy Autonomous Trigger & Installation (Sub-Milestone M5.4)
- **AC-M5-16 (Proactive Horizon Trigger):** When `antiEntropyManager.evaluateRemoteDigest()` receives a digest where `compareRemoteDigest()` detects `OUTSIDE_REMOTE_HORIZON`, it autonomously triggers `coordinator.triggerRecovery()`.
- **AC-M5-17 (Reactive Exhaustion Trigger):** When `repairRequestPipeline` exhausts all suppliers with `OUTSIDE_HORIZON`, `onHistoryExhausted` autonomously triggers `coordinator.triggerRecovery()`.
- **AC-M5-18 (Trigger Idempotency):** Calling `triggerRecovery()` when already in `RECOVERING` or `RECONCILING` state is an idempotent no-op that does not reset timers or disrupt the active attempt.
- **AC-M5-19 (Atomic Core State Replacement):** Successful snapshot installation invokes `replaceCoreStateAtomic()`, atomically updating all 12 core engine fields in 0 ticks.
- **AC-M5-20 (Exact Lamport Reseed):** If local clock was 100 and snapshot `maxLogicalTimestamp` was 500, the reseeded clock is exactly 500 (not 501). If local clock was 600, reseeded clock remains 600.
- **AC-M5-21 (Causal Frontier Alignment):** Immediately post-installation, `engine.replicationFrontier` and `engine.baseSnapshotFrontier` match the snapshot's `snapshotFrontier` bit-exact.

### Category 5: Quarantine Drain & End-to-End Convergence (Sub-Milestone M5.5)
- **AC-M5-22 (Sequential Drain & Receipts):** Post-installation, quarantined operations are drained with $N_{\text{in-flight}} \le 1$. Reducible operations advance `replicationVector`; operations missing dependencies enter `pendingQueue` with `ACCEPTED_PENDING`.
- **AC-M5-23 (Duplicate Discard):** Quarantined operations whose sequence numbers are $\le$ the newly installed base snapshot frontier are classified as `ALREADY_REPRESENTED` and discarded without duplicate execution.
- **AC-M5-24 (Iterative Generational Drain):** Operations arriving while drain pass 1 is executing are captured in subsequent drain passes, completing when `quarantineBuffer.totalRetainedCount === 0`.
- **AC-M5-25 (Return to Active & Live Convergence):** Upon drain completion, the coordinator transitions to `ACTIVE`, local gating is lifted, notifications are flushed, and subsequent live operations replicate seamlessly across all peers.

---

## 3. Verification Traceability Matrix

| Acceptance Criterion | Primary Component Under Test | Test Suite File (Planned) |
|---|---|---|
| AC-M5-01 .. AC-M5-05 | `LeaderlessSyncEngine` + `RecoveryCoordinator` | `packages/sync-engine/src/__tests__/recovery-lifecycle-binding.test.ts` |
| AC-M5-06 .. AC-M5-11 | `LeaderlessSyncEngine` + `QuarantineBuffer` | `packages/sync-engine/src/__tests__/recovery-remote-interception.test.ts` |
| AC-M5-12 .. AC-M5-15 | `LeaderlessSyncEngine` + `RecoveryAdmissionController` | `packages/sync-engine/src/__tests__/recovery-local-gating.test.ts` |
| AC-M5-16 .. AC-M5-21 | `AntiEntropyManager` + `SnapshotInstaller` | `packages/sync-engine/src/__tests__/recovery-anti-entropy-trigger.test.ts` |
| AC-M5-22 .. AC-M5-25 | Full Multi-Peer SyncEngine Mesh | `packages/sync-engine/src/__tests__/recovery-engine-integration.test.ts` |
