# Phase 7.5 M4.3 — Recovery Orchestration & Lifecycle Integration Implementation Roadmap

**Author:** Senior TypeScript Distributed-Systems Architect  
**Status:** M4.3 ARCHITECTURE READY FOR FINAL ACCEPTANCE — IMPLEMENTATION UNAUTHORIZED  
**Mode:** STRICTLY READ-ONLY SPECIFICATION (IMPLEMENTATION UNAUTHORIZED)  
**Milestone:** Phase 7.5 Sub-Milestone M4.3  
**Date:** November 23, 2026  
**Revision:** R3 Final Contract Closure  
**Authoritative Baseline Commit:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Direct Parent:** `79919a0f8cb0bb81cb80cc010fe02918bef79787` (M4.2c-R6 closure)  
**Parent of Direct Parent:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5 closure)  

---

## 1. Executive Strategy & Staging Overview

Sub-Milestone M4.3 connects the frozen snapshot primitives (M4.1–M4.2d) into an end-to-end recovery pipeline. Following the R3 final contract review, M4.3 incorporates the **sealed single-root atomic state replacement pattern**, **complete reentrancy exclusion rule**, **7-state operation lifecycle taxonomy**, **honest memory accounting**, and the **26-scenario acceptance verification suite**.

The implementation is structured into **four staged, independently reviewable sub-milestones**:

```
M4.3a: State Machine & Generational Quarantine Buffer
  │   - RecoveryStateMachine (4 states, 8 substates)
  │   - QuarantineBuffer (Double-buffered, 1000 count / 4MB byte bound, FutureEpochBuffer: 100 / 512KB)
  ▼
M4.3b: Single-Root Snapshot Installer & Engine Core State Contract
  │   - SnapshotInstaller (Stage 1 validation, Stage 2 sealed staging, Stage 3 single reference commit)
  │   - LeaderlessSyncEngine.replaceCoreStateAtomic() & deferred observer queues
  ▼
M4.3c: Recovery Coordinator Subsystem & Reentrancy Isolation
  │   - RecoveryCoordinator (orchestrator, decoupled timers, 7-state drain loop, complete reentrancy exclusion)
  │   - AntiEntropyManager & SnapshotInboundPipeline integration
  ▼
M4.3d: End-to-End Multi-Peer Recovery Integration & 26-Scenario Acceptance Matrix Verification
      - Multi-peer simulation suite (late join, failover, partial drain, quarantine overflow)
      - Exhaustive 26-scenario acceptance matrix test suite
```

---

## 2. Staged Sub-Milestones

### 2.1 Sub-Milestone M4.3a — State Machine & Generational Quarantine Buffer

- **Goal:** Establish deterministic state transitions and dual-constraint bounded buffering for live operations arriving during recovery.
- **Scope of Work:**
  1. Implement `RecoveryStateMachine` managing `RecoveryState` (`ACTIVE`, `RECOVERING`, `RECONCILING`, `DEGRADED`) and `RecoverySubstate`.
  2. Implement `QuarantineBuffer` as a double-buffered FIFO queue enforcing:
     - Count limit: `MAX_QUARANTINE_OPERATIONS = 1000`.
     - Byte limit: `MAX_QUARANTINE_TOTAL_BYTES = 4_194_304` (4 MiB serialized UTF-8).
     - Per-event limit: `MAX_QUARANTINE_EVENT_BYTES = 4096` (4 KiB).
  3. Implement `FutureEpochBuffer`:
     - Count limit: `MAX_FUTURE_EPOCH_OPERATIONS = 100`.
     - Byte limit: `MAX_FUTURE_EPOCH_BYTES = 524_288` (512 KiB).
     - 5-second convergence timer with cancellation upon `EpochTransitionCapability`.
  4. Enforce fail-closed overflow guard: exceeding count or byte limit immediately triggers transition to `DEGRADED`.
  5. Implement generational swap API (`swapDrainBatch()`) for lossless atomic handoff.
- **Target Files:**
  - `packages/sync-engine/src/recovery/recovery-state-machine.ts` (NEW)
  - `packages/sync-engine/src/recovery/quarantine-buffer.ts` (NEW)
  - `packages/sync-engine/src/__tests__/recovery-state-machine.test.ts` (NEW)
  - `packages/sync-engine/src/__tests__/quarantine-buffer.test.ts` (NEW)
- **Dependencies:** `@syncplay/protocol` only.
- **Invariants:**
  - Zero modifications to core engine state.
  - Pure state transition logic; invalid state transitions are rejected with descriptive errors.
  - Strict dual count/byte memory bounds.
- **Exit Criteria:**
  - 100% test pass rate for state machine transitions, illegal transitions, quarantine buffer count/byte bounds, generational swap, and overflow triggers.

---

### 2.2 Sub-Milestone M4.3b — Single-Root Snapshot Installer & Engine Core State Contract

- **Goal:** Implement the sealed single-root atomic snapshot installation pattern guaranteeing zero side-effects before commit and zero intermediate UI notifications.
- **Scope of Work:**
  1. Refactor `LeaderlessSyncEngine` internal state storage into `EngineCoreState` root container:
     - Wrap `PlaybackState`, `OrderedOperationLog`, `LogicalClockManager`, `PendingDependencyQueue`, `replicationVector`, `localCausalContext`, `baseSnapshot`, `baseSnapshotFrontier`, `localOriginSequence`, and `currentEpoch` into a structurally sealed root (`Object.seal`).
     - Implement `replaceCoreStateAtomic(candidate: EngineCoreState): void` via single synchronous reference assignment.
     - Implement `setRecoveryMode(active: boolean): void` to gate local submissions and buffer notifications.
     - Implement `flushDeferredNotifications(): void` with individual listener try/catch isolation.
  2. Implement `SnapshotInstaller`:
     - **Stage 1 (Dry-Run Validation):** Pure out-of-band JSON schema, RFC 8785 digest, epoch fencing, and strict frontier dominance checks.
     - **Stage 2 (Staging Construction):** Pre-construct candidate `LogicalClockManager`, candidate `OrderedOperationLog`, ingest all checkpoints out-of-band, instantiate candidate `PendingDependencyQueue`, assemble and seal `EngineCoreState`, and release local creator references upon completion.
     - **Stage 3 (Single Reference Commit Point):** Synchronously swap `this.core = candidateCore` in a single reference assignment under run-to-completion.
- **Target Files:**
  - `packages/sync-engine/src/recovery/snapshot-installer.ts` (NEW)
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (MODIFIED — single-root refactor and method additions)
  - `packages/sync-engine/src/__tests__/snapshot-installer.test.ts` (NEW)
- **Dependencies:** M4.3a, `LeaderlessSyncEngine`.
- **Invariants:**
  - Zero engine mutation during Stage 1 and Stage 2.
  - Stage 3 executes in 1 reference assignment (`this.core = candidateCore`) with ZERO `await` or async yields. Cannot throw.
  - `stateChangeListeners` and `decisionListeners` are deferred until Stage 4 completion.
- **Exit Criteria:**
  - Snapshot validation detects corrupted payloads, outdated epochs, and non-dominating vectors with zero engine corruption.
  - Single-root atomic swap passes all regression and unit tests.

---

### 2.3 Sub-Milestone M4.3c — Recovery Coordinator Subsystem & Reentrancy Isolation

- **Goal:** Integrate all recovery components into `RecoveryCoordinator`, connecting `AntiEntropyManager`, `SnapshotInboundPipeline`, `QuarantineBuffer`, and `SnapshotInstaller`.
- **Scope of Work:**
  1. Implement `RecoveryCoordinator`:
     - Evaluates incoming `SNAPSHOT_OFFER` messages against P7.4-aligned candidate eligibility rules (rejecting older epochs, dominated vectors, identical frontiers, and incomparable concurrent histories without arbitrary tie-breaking).
     - Intercepts remote events and routes to `QuarantineBuffer` while in `RECOVERING` or `RECONCILING`.
     - Intercepts local user intents and returns `REJECTED_RECOVERING` (not queued).
     - Owns outer workflow timeout (120,000ms), reconciliation timeout (15,000ms), and offer await timeout (10,000ms) without replenishing inner pipeline retry budgets.
     - Implements 7-state drain loop with partial drain failure resumption (ops 1..40 applied, op 41 buffered in pending queue, ops 42..100 buffered, AE requests missing range).
     - Enforces Complete Reentrancy Exclusion Rule: defers observer notifications and outbound repair messages outside the critical transition gate.
     - Flushes deferred notifications upon reconciliation completion.
     - Implements absorbing `isDisposed` flag and idempotent `dispose()`.
  2. Connect `AntiEntropyManager` to invoke `onSnapshotRecoveryRequired`.
- **Target Files:**
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts` (NEW)
  - `packages/sync-engine/src/replication/anti-entropy-manager.ts` (MODIFIED — add recovery trigger callback)
  - `packages/sync-engine/src/__tests__/recovery-coordinator.test.ts` (NEW)
- **Dependencies:** M4.3a, M4.3b, M4.2d (`SnapshotInboundPipeline`).
- **Invariants:**
  - Single active recovery workflow per engine.
  - Zero external callbacks inside critical transition gate.
  - Exactly-once resumption on partial drain failure.
  - Outer timeouts never reset inner pipeline retry counters.
  - Fail-closed transition to `DEGRADED` on timeout or overflow.
- **Exit Criteria:**
  - End-to-end unit and integration tests passing for normal recovery, candidate failover, quarantine overflow, and partial drain catch-up.

---

### 2.4 Sub-Milestone M4.3d — Multi-Peer Recovery Integration & 26-Scenario Acceptance Matrix

- **Goal:** Validate the integrated recovery architecture across simulated multi-peer networks under adverse conditions.
- **Scope of Work:**
  1. Multi-peer late-join test: new peer joins mesh and catches up via snapshot recovery.
  2. Compaction gap recovery test: peer falls behind log horizon ($K=256$) and successfully recovers via snapshot.
  3. Mid-transfer supplier crash & failover test: supplier peer disconnects during chunk transfer; inbound pipeline fails over to candidate; recovery completes seamlessly.
  4. High-concurrency quarantine test: 500 remote operations arrive during snapshot transfer; all are correctly quarantined and drained post-commit without loss or double-apply.
  5. Partial drain gap test: 100-op batch with op 41 having causal gap; verify ops 1..40 stay applied, ops 41..100 buffered, repair fulfills gap, all 100 apply in order.
  6. Future epoch buffering test: peer receives future epoch operations, transitions epoch via capability within 5s, and completes recovery.
  7. Dual-constraint overflow test: verify count overflow (> 1,000) and byte overflow (> 4 MiB) both trigger clean transition to `DEGRADED`.
  8. Verification of all 26 acceptance test scenarios from `PHASE7_5_M4_3_ARCHITECTURE.md` §13.
  9. Zero regressions against existing 1,357 monorepo tests.
- **Target Files:**
  - `packages/sync-engine/src/__tests__/recovery-multi-peer-integration.test.ts` (NEW)
  - `packages/sync-engine/src/__tests__/recovery-acceptance-matrix.test.ts` (NEW)
- **Dependencies:** M4.3a, M4.3b, M4.3c.
- **Invariants:**
  - Zero state divergence across peers following quiescence.
  - Strong Eventual Consistency (SEC) satisfied upon reconciliation completion.
- **Exit Criteria:**
  - All 26 acceptance scenarios and multi-peer recovery simulation tests pass with 0 failures and 0 memory leaks.

---

## 3. Strict Non-Regression & Frozen Code Boundaries

During M4.3 implementation, the following files and modules are **STRICTLY FROZEN** and must NOT be modified:
1. `packages/protocol/*` (Frozen M4.0)
2. `packages/sync-engine/src/recovery/chunk-assembler.ts` (Frozen M4.1)
3. `packages/sync-engine/src/recovery/snapshot-serializer.ts` (Frozen M4.2a)
4. `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts` (Frozen M4.2a)
5. `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts` (Frozen M4.2b)
6. `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts` (Frozen M4.2c)
7. `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts` (Frozen M4.2d)
8. `packages/sync-engine/src/recovery/candidate-offer-registry.ts` (Frozen M4.2d)
9. `packages/sync-engine/src/recovery/inbound-custody.ts` (Frozen M4.2d)

The only existing engine files subject to targeted integration additions are:
- `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (Refactoring internal state storage to `EngineCoreState` and adding `replaceCoreStateAtomic()`, `isRecoveryActive()`, `setRecoveryMode()`, `flushDeferredNotifications()`)
- `packages/sync-engine/src/replication/anti-entropy-manager.ts` (Adding `onSnapshotRecoveryRequired` callback triggering)
- `packages/sync-engine/src/index.ts` (Exporting M4.3 coordinator and installer interfaces)

---

## 4. Verification & Gate Checkpoints

| Gate | Target Milestone | Verification Command | Required Result |
| :---: | :--- | :--- | :--- |
| **G1** | M4.3a (State & Quarantine) | `pnpm --filter @syncplay/sync-engine test recovery-state-machine quarantine-buffer` | All unit tests pass, 100% branch coverage on state machine, dual memory bounds, and FutureEpochBuffer. |
| **G2** | M4.3b (Single-Root Installer) | `pnpm --filter @syncplay/sync-engine test snapshot-installer leaderless-sync-engine` | All installer tests pass, single-root atomic swap verified without intermediate yields. |
| **G3** | M4.3c (Recovery Coordinator) | `pnpm --filter @syncplay/sync-engine test recovery-coordinator anti-entropy` | Full coordinator workflow passes, partial drain resumption verified. |
| **G4** | M4.3d (26-Scenario Matrix) | `pnpm --filter @syncplay/sync-engine test recovery-acceptance-matrix recovery-multi-peer` | All 26 acceptance scenarios and multi-peer recovery tests pass. |
| **G5** | Full Monorepo Regression | `pnpm test` | 100% pass across all 1,357+ tests. |
