# Phase 7.5 Milestone M5 — Acceptance Criteria & Invariants

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R2 AMENDED)
**Date:** 2026-11-25
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Baselines:** M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)
**Scope:** Acceptance criteria, non-negotiable invariants, and verification matrix for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Non-Negotiable Architectural Invariants

Every sub-milestone implementation within Milestone M5 must adhere strictly to the following nine non-negotiable invariants:

| Invariant ID | Title | Invariant Definition | Enforcement Mechanism |
|---|---|---|---|
| **INV-M5-01** | Measurable Inactive Transparency (Closing F6, R2-M1) | When recovery is inactive (`ACTIVE`), normal active-state semantics remain 100% unchanged from Phase 7.4. No event enters quarantine. The admission check is strictly synchronous, $O(1)$, non-blocking, and adds $\le 5\,\mu\text{s}$ p99 overhead per remote event on reference hardware (Node 20 LTS, 4 vCPU, 512-byte payload, 100k runs). Zero queue heap allocations. | `interceptRemoteEvent()` immediately returns `ADMIT_ACTIVE` in $O(1)$ time; event proceeds directly to normal `receiveRemote()`. |
| **INV-M5-02** | Zero Remote State Mutation During Recovery | Incoming remote operations must NEVER mutate active playback state, insert into `OrderedOperationLog`, or advance `replicationVector` while recovery is in progress (`RECOVERING` or `RECONCILING`). | `LeaderlessSyncEngine.receive()` intercepts remote events before `receiveRemote()`; current epoch events are held in `QuarantineBuffer`. |
| **INV-M5-03** | Local Input Fail-Close | Local user operations must NOT bypass recovery admission controls or create concurrent divergence while catching up. Local intents must fail-close synchronously. | `receiveLocal()` and `submit()` query `coordinator.interceptLocalSubmission()`; throw `SyncEngineError('REJECTED_RECOVERING')` if not `ACTIVE`. |
| **INV-M5-04** | Atomic State Replacement (Closing F2, R2-C1, R2-M2) | Snapshot installation must be executed as one synchronous root-reference replacement (`this.core = candidateCore;`) under JavaScript run-to-completion semantics across all 12 authoritative core state fields plus referential alias (`playbackState === state`). No intermediate root is visible to other JavaScript tasks. External callers receive only opaque `StagedSnapshotInstallation` handles; candidate core is held in private `WeakMap`. | `replaceCoreStateAtomic(candidateCore, capability)` executes single synchronous reference replacement in 1 call frame. |
| **INV-M5-05** | Exact Lamport Reseed Monotonicity | Lamport clock reseeding upon snapshot installation must strictly evaluate: $L_{\text{local}} = \max(L_{\text{local}}, L_{\text{snapshot}})$. Under no circumstances may an artificial $+1$ increment be added during snapshot installation. | `SnapshotInstaller.stage()` sets `candidateClockManager = new LogicalClockManager(Math.max(currentClock, maxLogicalTimestamp))`. |
| **INV-M5-06** | Causal Vector Advance | Replication vectors must advance ONLY after valid, causally admissible operations are accepted and reduced into the operation log. | `replicationVector` is updated strictly within `admitDrainedOperation()` / `admitOperation()` upon `ACCEPTED_REDUCED`. |
| **INV-M5-07** | Authoritative Drain Admission (Closing R2-H6) | Quarantined operations must be drained strictly through the engine's existing causal-validation path (`admitDrainedOperation()`). An operation is never marked applied merely because it was dequeued. | `QuarantineDrainer` enforces $N_{\text{in-flight}} \le 1$ with synchronous `DestinationAcceptanceReceipt` confirmation. |
| **INV-M5-08** | Rollback & Failure Isolation (Closing R2-C2) | Failed or aborted recovery must NEVER leave the engine partially switched into a new state. Candidate objects must be discarded and the active state must remain 100% untouched. If clearing recovery mode or the state transition throws during handoff, recovery mode is reverted and completion notifications are not flushed. | Out-of-band staging: candidate core state is held in `stagedRoots`; failure prior to `commitAtomic` drops candidate without mutating active core. Rollback catch block restores `engine.setRecoveryMode(true)`. |
| **INV-M5-09** | Backward Compatibility & Regression Floors (Closing R2-M3) | All existing Phase 1–7.4 behaviors, wire schemas, error codes, and frozen M4.3d behaviors must remain completely intact. Test counts represent minimum regression floors ($\ge 1,550$ monorepo tests, $\ge 965$ sync-engine tests, $\ge 42$ recovery acceptance tests). | Automated regression test runs with zero failures and zero skipped tests. |

---

## 2. Milestone M5 Acceptance Criteria

### Category 1: Lifecycle & State Consistency (Sub-Milestone M5.1 — Closing F1, F5, F7, R2-C2, R2-H5)
- **AC-M5-01 (Authoritative Lifecycle State):** The coordinator's state machine is the sole authority for recovery lifecycle state (`recoveryState`). `engine.isRecoveryInProgress()` returns `true` if and only if `recoveryState === 'RECOVERING' || recoveryState === 'RECONCILING'`. It returns `false` in `ACTIVE`, `DEGRADED`, and `DISPOSED`.
- **AC-M5-02 (Zero Duplicate Boolean State):** The engine maintains no independently mutable boolean flags for recovery state; all recovery queries delegate directly to the attached `RecoveryCoordinator`.
- **AC-M5-03 (Notification Deferral):** State change notifications (`stateChangeListeners`) and decision notifications (`decisionListeners`) are buffered while `isRecoveryInProgress() === true`.
- **AC-M5-04 (Handoff Ordering & Coalesced Notifications — Closing F7, R2-C2):** Recovery completion executes strictly in order: (1) Quarantine drain completes (`totalRetainedCount === 0`), (2) Bookkeeping finalizes, (3) `engine.setRecoveryMode(false)` succeeds, (4) Coordinator transitions to `ACTIVE`, (5) `engine.flushDeferredNotifications()` is invoked. If step 4 throws, `engine.setRecoveryMode(true)` is restored and deferred notifications are NOT flushed. Listeners observing `ACTIVE` are guaranteed to see `engine.isRecoveryActive() === false`. Coalesces to exactly ONE `PlaybackState` event; emits decisions sequentially FIFO.
- **AC-M5-05 (Synchronous Idempotent Disposal):** Invoking `engine.dispose()` synchronously disposes the attached coordinator. Multiple invocations are safe no-ops. All active timers and listeners are cleared.
- **AC-M5-06 (Configuration Mutual Exclusion):** Supplying both `recoveryOptions` and `recoveryCoordinator` to `LeaderlessSyncEngineConfig` throws `SyncEngineError('INVALID_CONFIG')`.
- **AC-M5-07 (Single Attachment Invariant):** Attempting to bind a `RecoveryCoordinator` to more than one engine throws `SyncEngineError('ALREADY_BOUND')`. Sharing coordinators across engines is prohibited.

### Category 2: Inbound Interception & Memory Bounds (Sub-Milestone M5.2 — Closing F3, F9, R2-H2, R2-H3)
- **AC-M5-08 (Remote Operation Quarantine):** In `RECOVERING` or `RECONCILING` state, valid remote events matching the current epoch are enqueued into `QuarantineBuffer` with outcome `QUARANTINED`.
- **AC-M5-09 (Zero State Mutation Relative to Baseline):** While in `RECOVERING`, receiving 100 remote events results in `engine.operationLog.size === preRecoveryLogSize` and `engine.playbackState` remaining bit-exact identical to the pre-recovery state baseline.
- **AC-M5-10 (Future Epoch Staging & Independent Timer — Closing F9, R2-H4):** Remote events claiming a future epoch ($E_{\text{event}} > E_{\text{engine}}$) are staged in `FutureEpochBuffer` with outcome `BUFFERED_FUTURE_EPOCH` and an independent 5-second grace period timer (not dropped).
- **AC-M5-11 (Obsolete Epoch Discard):** Remote events claiming an obsolete epoch ($E_{\text{event}} < E_{\text{engine}}$) are dropped with outcome `DISCARDED_OBSOLETE_EPOCH` and zero state, anti-entropy, or log side effects.
- **AC-M5-12a (Quarantine Count Bound):** Enqueuing operation 1,001 into `QuarantineBuffer` breaches `maxOperations = 1000` and throws `SyncEngineError('QUARANTINE_OVERFLOW')`.
- **AC-M5-12b (Quarantine Aggregate Byte Bound):** Enqueuing operations exceeding 4 MiB (4,194,304 bytes) aggregate retained bytes breaches `maxTotalBytes` and throws `SyncEngineError('QUARANTINE_OVERFLOW')`.
- **AC-M5-12c (Quarantine Individual Event Limit):** Enqueuing an individual operation exceeding 4 KiB (4,096 bytes) serialized length throws `SyncEngineError('REJECTED_INVALID')`.
- **AC-M5-12d (Future Epoch Count Bound):** Enqueuing operation 101 into `FutureEpochBuffer` breaches `maxOperations = 100` and throws `SyncEngineError('FUTURE_EPOCH_OVERFLOW')`.
- **AC-M5-12e (Future Epoch Aggregate Byte Bound):** Enqueuing future epoch operations exceeding 512 KiB (524,288 bytes) aggregate retained bytes throws `SyncEngineError('FUTURE_EPOCH_OVERFLOW')`.
- **AC-M5-12f (Future Epoch Individual Event Limit):** Enqueuing an individual future epoch event exceeding 4 KiB throws `SyncEngineError('REJECTED_INVALID')`.
- **AC-M5-12g (Memory Accounting & Release):** Byte accounting tracks UTF-8 serialized byte lengths. Bytes are subtracted upon dequeue/drain and generational clearing. Duplicates add 0 bytes. Rejections retain 0 bytes.
- **AC-M5-12h (Unified Overflow Contract — Closing R2-H2):** On buffer overflow, `QuarantineBuffer` throws $\to$ `RecoveryCoordinator` catches, aborts recovery to `DEGRADED`, and rethrows $\to$ `LeaderlessSyncEngine.receive()` catches, isolates remote error, maps to `REJECTED_CAPACITY`, and records decision without throwing to transport callers.
- **AC-M5-13 (Active State Pass-Through — Closing F6, R2-M1):** When coordinator is in `ACTIVE` state, remote events bypass quarantine with outcome `ADMIT_ACTIVE` in $\le 5\,\mu\text{s}$ p99 and are processed by standard `receiveRemote()`.

### Category 3: Local Input Gating (Sub-Milestone M5.3 — Closing F3)
- **AC-M5-14 (Local Synchronous Rejection during Recovery):** Invoking `engine.receiveLocal()` or `engine.submit()` while `isRecoveryInProgress() === true` throws a synchronous `SyncEngineError('REJECTED_RECOVERING', ...)`.
- **AC-M5-15 (Degraded Input Rejection):** When in `DEGRADED` state, local submissions throw `SyncEngineError('REJECTED_DEGRADED', ...)`.
- **AC-M5-16 (Clock & Sequence Invariance):** A rejected local submission does not advance `engine.localOriginSequence` or mutate `engine.clockManager.clock`.
- **AC-M5-17 (Zero Causal Skew):** Local operations cannot be generated during recovery with obsolete causal dependencies.

### Category 4: Anti-Entropy Autonomous Trigger & Installation (Sub-Milestone M5.4 — Closing F2, F4, F10, R2-C1, R2-C3, R2-C4, R2-H1, R2-H4, R2-M2)
- **AC-M5-18 (Proactive Horizon Trigger):** When `antiEntropyManager.evaluateRemoteDigest()` receives a digest where `compareRemoteDigest()` detects `OUTSIDE_REMOTE_HORIZON`, it autonomously triggers `coordinator.triggerRecovery()`.
- **AC-M5-19 (Reactive Exhaustion Trigger):** When `repairRequestPipeline` exhausts all suppliers with `OUTSIDE_HORIZON`, `onHistoryExhausted` autonomously triggers `coordinator.triggerRecovery()`.
- **AC-M5-20 (Supplier Lattice Ordering & Deterministic Comparator — Closing F4, R2-C3, R2-H1):** Candidates are evaluated across partial ordering: strict domination ($V_A > V_B$), lowest RTT, and protocol-stable code-point peer ID comparison (`comparePeerIds`). Incomparable vectors do NOT automatically equate to forks; compatible concurrent branches or repairable gaps are processed without degradation; proven conflicts ($H(op_A) \ne H(op_B)$) abort to `DEGRADED`.
- **AC-M5-21 (Trigger Idempotency):** Calling `triggerRecovery()` when already in `RECOVERING` or `RECONCILING` state is an idempotent no-op that does not reset timers or disrupt the active attempt.
- **AC-M5-22 (Atomic Core State Replacement via Opaque Staged Handle — Closing F2, R2-C1, R2-M2):** `SnapshotInstaller.stage()` produces opaque `StagedSnapshotInstallation` handle, holding candidate core in private `WeakMap`. Calling `commitAtomic()` executes `replaceCoreStateAtomic()`, atomically updating all 12 core engine fields in 1 synchronous reference assignment under run-to-completion semantics, preserving `candidate.playbackState === candidate.state`.
- **AC-M5-23 (Exact Lamport Reseed):** If local clock was 100 and snapshot `maxLogicalTimestamp` was 500, the reseeded clock is exactly 500 (not 501). If local clock was 600, reseeded clock remains 600.
- **AC-M5-24 (Causal Frontier Alignment):** Immediately post-installation, `engine.replicationFrontier` and `engine.baseSnapshotFrontier` match the snapshot's `snapshotFrontier` bit-exact.
- **AC-M5-25 (Pipeline Attempt Fencing & Independent Epoch Timers — Closing F10, R2-H4):** Stale completion callbacks matching obsolete `attemptId` are dropped as no-ops. `onTransferComplete` executes at most once per attempt. Independent future-epoch grace timer is NOT cancelled on snapshot completion or retry.

### Category 5: Quarantine Drain & End-to-End Convergence (Sub-Milestone M5.5 — Closing R2-H6)
- **AC-M5-26 (Sequential Drain & Receipts — Closing R2-H6):** Post-installation, quarantined operations are drained with $N_{\text{in-flight}} \le 1$. Reducible operations advance `replicationVector`; operations missing dependencies enter `pendingQueue` with `ACCEPTED_PENDING`. If pending queue capacity is exhausted, destination returns `CAPACITY_REJECTED`, operation is retained, drain halts, and recovery aborts to `DEGRADED` without operation loss.
- **AC-M5-27 (Duplicate Discard):** Quarantined operations whose sequence numbers are $\le$ the newly installed base snapshot frontier are classified as `ALREADY_REPRESENTED` and discarded without duplicate execution.
- **AC-M5-28 (Iterative Generational Drain):** Operations arriving while drain pass 1 is executing are captured in subsequent drain passes, completing when `quarantineBuffer.totalRetainedCount === 0`.
- **AC-M5-29 (Return to Active & Live Convergence):** Upon drain completion, the coordinator transitions to `ACTIVE`, local gating is lifted, notifications are flushed, and subsequent live operations replicate seamlessly across all peers.

---

## 3. Verification Traceability Matrix

| Acceptance Criteria | Primary Component Under Test | Planned Test Suite File |
|---|---|---|
| AC-M5-01 .. AC-M5-07 | `LeaderlessSyncEngine` + `RecoveryCoordinator` | `packages/sync-engine/src/__tests__/recovery-lifecycle-binding.test.ts` |
| AC-M5-08 .. AC-M5-13 | `LeaderlessSyncEngine` + `QuarantineBuffer` | `packages/sync-engine/src/__tests__/recovery-remote-interception.test.ts` |
| AC-M5-14 .. AC-M5-17 | `LeaderlessSyncEngine` + `RecoveryAdmissionController` | `packages/sync-engine/src/__tests__/recovery-local-gating.test.ts` |
| AC-M5-18 .. AC-M5-25 | `AntiEntropyManager` + `SnapshotInstaller` | `packages/sync-engine/src/__tests__/recovery-anti-entropy-trigger.test.ts` |
| AC-M5-26 .. AC-M5-29 | Full Multi-Peer SyncEngine Mesh | `packages/sync-engine/src/__tests__/recovery-engine-integration.test.ts` |
