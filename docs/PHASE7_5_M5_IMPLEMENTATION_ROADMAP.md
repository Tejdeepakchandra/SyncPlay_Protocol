# Phase 7.5 Milestone M5 — Implementation Roadmap

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R2 AMENDED)
**Date:** 2026-11-25
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Baselines:** M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)
**Scope:** Staged execution plan and gating procedures for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Roadmap Phasing Strategy & Dependency Order

Milestone M5 integrates the accepted snapshot recovery subsystem (`RecoveryCoordinator`, `SnapshotInstaller`, `QuarantineBuffer`) directly into `LeaderlessSyncEngine`.

Because `LeaderlessSyncEngine` is the core playback, causal ordering, and replication authority, M5 is divided into six sequential, risk-isolated sub-milestones (M5.0 through M5.5). Each sub-milestone has explicit entry prerequisites, strict scope constraints, and automated verification gates:

```mermaid
flowchart TD
    M5_0["Milestone M5.0: Architecture Audit & Consistency Corrections<br/>(Strictly Read-Only / Closed via M5.0-R2)"] --> M5_1["Milestone M5.1: Lifecycle Ownership & Coordinator Binding"]
    M5_1 --> M5_2["Milestone M5.2: Inbound Remote Interception & Quarantine"]
    M5_2 --> M5_3["Milestone M5.3: Local Input Gating & Synchronous Fail-Close"]
    M5_3 --> M5_4["Milestone M5.4: Anti-Entropy Autonomous Trigger & Installation"]
    M5_4 --> M5_5["Milestone M5.5: End-to-End Runtime Recovery & Stress Suite"]

    style M5_0 fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style M5_1 fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_2 fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_3 fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_4 fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style M5_5 fill:#fff3e0,stroke:#e65100,stroke-width:2px
```

---

## 2. Sub-Milestone Detailed Specifications

### Milestone M5.0: Runtime Integration Architecture Audit & Consistency Correction (Amended M5.0-R2)
- **Role:** Independent Senior TypeScript Distributed-Systems Architect
- **Status:** CURRENT MILESTONE (Planning Only)
- **Objective:** Complete source audit, close review findings F1–F10 and R2-C1–R2-C4, R2-H1–R2-H6, R2-M1–R2-M3, and author amended architecture specifications for M5.
- **Scope:**
  - Source audit across all recovery and engine components against accepted M4.3b, M4.3c, and M4.3d baselines.
  - Reconcile `EngineCoreState` against accepted source (`packages/sync-engine/src/recovery/engine-core-state.ts`) confirming exactly 12 stored fields + 1 referential alias (`playbackState === state`), single authoritative root, and preservation of M4.3b's opaque staged-handle design.
  - Correct recovery completion ordering: drain completes $\to$ bookkeeping completes $\to$ `engine.setRecoveryMode(false)` $\to$ `coordinator.transition('ACTIVE')` $\to$ `flushDeferredNotifications()`. Specify failure rollback if transition throws.
  - Correct incomparable vector semantics: do not equate incomparable vectors with proven forks. Distinguish compatible branches, repairable histories, and proven conflicts.
  - Replace ambiguous digest statements; define cooperative non-Byzantine trust model and distinguish SHA-256 integrity from author authentication.
  - Define protocol-stable, locale-independent peer ID comparison (`comparePeerIds`).
  - Unify overflow semantics: buffers throw `QUARANTINE_OVERFLOW`, coordinator aborts to `DEGRADED` and rethrows, `LeaderlessSyncEngine.receive()` isolates remote error to `REJECTED_CAPACITY` without throwing.
  - Complete memory-bound specifications: Quarantine (1,000 ops / 4 MiB / 4 KiB), Future Epoch (100 ops / 512 KiB / 4 KiB).
  - Separate timer ownership across 4 distinct categories (preserving independent future-epoch grace timer).
  - Clarify coordinator construction vs. lifecycle ownership and single-attachment invariant.
  - Preserve bounded quarantine drain ($N_{\text{in-flight}} \le 1$) and destination receipts.
  - Define performance measurement contract (reference hardware, Node LTS, 512-byte payload, 10k warm-up, 100k runs, p99 $\le 5\,\mu\text{s}$, zero queue allocations).
  - Replace atomicity wording with precise run-to-completion reference replacement.
  - Treat test counts as minimum regression floors ($\ge 1,550$ monorepo, $\ge 965$ sync-engine, $\ge 42$ recovery acceptance).
- **Affected Files:** Documentation files only (`docs/PHASE7_5_M5_*.md`). Zero production or test code modified.
- **Exit Condition:** Clean audit report; zero production diff; documentation committed; engineering authority approval.

---

### Milestone M5.1: Lifecycle Ownership, Coordinator Construction, Attachment & Disposal
- **Objective:** Establish the authoritative recovery lifecycle coupling between `RecoveryCoordinator` and `LeaderlessSyncEngine`.
- **Scope:**
  - Update `LeaderlessSyncEngineConfig` with mutually exclusive options: `recoveryOptions?: EngineRecoveryOptions` vs. `recoveryCoordinator?: IRuntimeRecoveryCoordinator`. Throw `SyncEngineError('INVALID_CONFIG')` if both are supplied.
  - Refactor `LeaderlessSyncEngine`:
    - Add `readonly recoveryState: RecoveryState` delegating to coordinator (or returning `'ACTIVE'` if unattached, `'DISPOSED'` if disposed).
    - Implement `isRecoveryInProgress()` returning `true` strictly for `'RECOVERING'` and `'RECONCILING'`.
    - Retain `isRecoveryActive()` as a backward-compatible alias.
    - Remove mutable `recoveryActive` boolean from serving as an independent authority.
  - Implement single-attachment invariant in `coordinator.bindEngine(engine, capability)`: throw `SyncEngineError('ALREADY_BOUND')` if already attached. Prohibit sharing coordinators across engines.
  - Wire coordinator lifecycle completion sequence:
    1. Quarantine drain completes (`totalRetainedCount === 0`).
    2. Recovery bookkeeping completes.
    3. `engine.setRecoveryMode(false)`.
    4. Coordinator transitions to `ACTIVE`. If transition throws, catch and rollback `engine.setRecoveryMode(true)`, aborting recovery without flushing notifications.
    5. Coordinator clears timers and invokes `engine.flushDeferredNotifications()` strictly **after** state is `'ACTIVE'`.
  - Ensure `engine.dispose()` synchronously disposes the attached `RecoveryCoordinator`, clearing all timers and listeners.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-lifecycle-binding.test.ts`
- **Invariants Enforced:**
  - Single authoritative recovery state source of truth (`RecoveryCoordinator.stateMachine.state`).
  - Strict mutual exclusion in engine configuration.
  - Clean disposal with zero lingering timers or detached listeners.
- **Prohibited Work:** Do not implement remote interception (M5.2) or local input gating (M5.3) in this milestone.
- **Exit Condition:** Unit tests verify lifecycle coupling, configuration mutual exclusion, single attachment, and disposal.

---

### Milestone M5.2: Inbound Remote-Event Interception & Quarantine
- **Objective:** Route incoming remote operations through `RecoveryCoordinator.interceptRemoteEvent()` returning discriminated `RemoteAdmissionDecision`.
- **Scope:**
  - In `LeaderlessSyncEngine.receive()`, route remote events (`isLocal === false`) to `coordinator.interceptRemoteEvent(event, size)` when coordinator is attached.
  - Handle discriminated outcomes:
    - `ADMIT_ACTIVE`: proceed to normal `receiveRemote()` without added latency ($\le 5\,\mu\text{s}$).
    - `QUARANTINED`: record decision `BUFFERED_QUARANTINE`, bypass `receiveRemote()`, zero log/state mutation.
    - `BUFFERED_FUTURE_EPOCH`: record decision `BUFFERED_FUTURE_EPOCH`, staged in `FutureEpochBuffer` with independent 5s grace timer.
    - `DISCARDED_OBSOLETE_EPOCH`: drop event with zero state or log side effects.
    - `DROPPED_DRAINER_DUPLICATE`: drop event matching currently in-flight draining operation.
    - `REJECTED_DEGRADED`: drop event with decision `REJECTED_DEGRADED`.
    - `REJECTED_DISPOSED`: reject event on disposed engine.
  - Unified overflow handling: `QuarantineBuffer.enqueue` and `FutureEpochBuffer.enqueue` throw `QUARANTINE_OVERFLOW` / `FUTURE_EPOCH_OVERFLOW`. Coordinator catches, aborts to `DEGRADED`, and rethrows. `LeaderlessSyncEngine.receive()` catches, isolates remote error, maps to rejection decision `code: 'REJECTED_CAPACITY'`, and records it without throwing to transport callers.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - `packages/sync-engine/src/recovery/recovery-admission-controller.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-remote-interception.test.ts`
- **Invariants Enforced:**
  - Zero state mutation or causal advancement while recovery is in progress.
  - In-flight draining operations are deduplicated against newly arriving remote events.
  - Memory bounds (1,000 ops / 4 MiB quarantine; 100 ops / 512 KiB future epoch; 4 KiB event limit) are strictly enforced.
- **Exit Condition:** Unit tests prove remote events are quarantined during recovery, overflow triggers degradation and remote isolation, and normal events are admitted when active.

---

### Milestone M5.3: Local Input Gating & Synchronous Fail-Close
- **Objective:** Gate local user actions during recovery and degraded states, synchronously rejecting inputs.
- **Scope:**
  - In `LeaderlessSyncEngine.receiveLocal()`, call `coordinator.interceptLocalSubmission()`.
  - If `allowed === false`: throw synchronous `SyncEngineError(admission.code, admission.reason)`.
    - Throws `REJECTED_RECOVERING` during `RECOVERING` or `RECONCILING`.
    - Throws `REJECTED_DEGRADED` during `DEGRADED`.
  - Implement `LeaderlessSyncEngine.submit(intent)` helper that validates intent and calls `receiveLocal()`, fail-closing during recovery.
  - Ensure local sequence numbers (`localOriginSequence`) and local clocks are NOT advanced when local inputs are rejected.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-local-gating.test.ts`
- **Invariants Enforced:**
  - Local inputs strictly prohibited during non-ACTIVE states.
  - No silent queuing of local actions (preventing causal skew and intent inversion).
  - Local sequence counter and Lamport clock remain unmutated on rejection.
- **Exit Condition:** Unit tests verify synchronous rejection of all local control events during recovery and degraded states.

---

### Milestone M5.4: Anti-Entropy Autonomous Trigger, Supplier Selection & Snapshot Installation Integration
- **Objective:** Autonomously trigger snapshot recovery from anti-entropy gap detection and execute atomic snapshot installation.
- **Scope:**
  - Implement deterministic supplier selection under lattice partial ordering: strict domination, lowest RTT, and protocol-stable code-point peer ID comparison (`comparePeerIds`).
  - Differentiate incomparable vector cases: compatible branches, repairable histories within log horizon, and proven causal forks.
  - In `AntiEntropyManager.evaluateRemoteDigest()`, inspect `compareRemoteDigest(digest)`. If `requiresSnapshotRecovery === true` and `outsideRemoteHorizon === true`, invoke `onSnapshotRecoveryRequired` to trigger `coordinator.triggerRecovery(supplierPeerId)`.
  - Wire `RepairRequestPipeline.onHistoryExhausted` into `coordinator.triggerRecovery()`.
  - Wire `SnapshotInboundPipeline.onTransferComplete` to invoke `coordinator.executeRecoveryWithSnapshot(payload)` with one-shot completion latch and `activeAttemptId` fencing.
  - Execute `SnapshotInstaller.stage()` producing opaque `StagedSnapshotInstallation` handle, holding candidate `EngineCoreState` in private `WeakMap`.
  - Execute capability-gated commit: `installer.commitAtomic(engine, stagedHandle, capability)` invoking `engine.replaceCoreStateAtomic(candidateCore, capability)` in one synchronous reference replacement under JavaScript run-to-completion semantics.
  - Enforce exact Lamport clock reseeding: $L_{\text{local}} = \max(L_{\text{local}}, L_{\text{snapshot}})$ strictly without $+1$.
  - Execute post-installation quarantine drain via `reconcileQuarantine()`, iteratively draining quarantined operations until empty with $N_{\text{in-flight}} \le 1$.
- **Affected Files:**
  - `packages/sync-engine/src/replication/anti-entropy-manager.ts`
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-anti-entropy-trigger.test.ts`
- **Invariants Enforced:**
  - Both reactive repair exhaustion and proactive digest comparison autonomously initiate recovery.
  - Triggering is strictly idempotent.
  - Snapshot installation is single-reference atomic across all 12 authoritative fields via opaque staged handles.
  - Lamport clock is monotonic without artificial $+1$ increment.
  - Drained events advance replication vectors through causal validation.
- **Exit Condition:** Integration tests prove autonomous triggering, atomic install, and post-install drain.

---

### Milestone M5.5: End-to-End Runtime Recovery, Clock, Vector, and Fault Suite
- **Objective:** Comprehensive multi-peer simulation and stress hardening of the fully integrated engine runtime.
- **Scope:**
  - Multi-peer simulation suite covering:
    1. Lagged peer rejoining after 500-op compaction horizon gap.
    2. Concurrent remote event burst during active recovery transfer.
    3. Missing causal dependencies in quarantined events correctly enqueued in `pendingQueue`.
    4. Supplier crash and failover during active recovery with seamless assembly completion.
    5. Future-epoch operation buffering, grace period countdown, and epoch advancement.
    6. Quarantine buffer overflow triggering graceful degradation to `DEGRADED`.
    7. Lamport clock monotonicity across multiple sequential recovery and drain cycles.
  - Reference benchmark suite verifying active-state performance ($\le 5\,\mu\text{s}$ p99 overhead, zero queue allocations).
  - Monorepo regression test run meeting or exceeding minimum floors ($\ge 1,550$ monorepo tests, $\ge 965$ sync-engine tests, $\ge 42$ recovery acceptance tests).
- **Affected Files:**
  - `packages/sync-engine/src/__tests__/recovery-engine-integration.test.ts`
  - Milestone M5 Completion Report: `docs/PHASE7_5_M5_COMPLETION_REPORT.md`
- **Invariants Enforced:**
  - 100% test pass rate across sync-engine and full monorepo with 0 failures and 0 skips.
  - Zero memory leaks, dangling timers, or unhandled promise rejections.
  - Zero regressions against Phase 7.3, 7.4, and M4 baselines.
- **Exit Condition:** All integration tests pass; independent audit sign-off; milestone frozen.

---

## 3. Sub-Milestone Gating Rules

To ensure strict engineering governance, the following gating rules are non-negotiable:

1. **Sequential Authorization:** Implementation of sub-milestone $M5.(n+1)$ may ONLY begin after $M5.n$ is completed, verified, and authorized by the Engineering Authority. M5.1 must not quietly implement M5.2 or M5.3.
2. **Zero Regression Gate:** Every sub-milestone must pass all existing unit tests (minimum regression floor of 965 in `@syncplay/sync-engine`, 1,550 monorepo total) with zero skipped tests and zero warnings. New tests must increase these totals.
3. **TypeScript Build Gate:** `npm run typecheck` and `npm run build` must exit with code 0 across all 6 workspaces on every commit.
4. **Frozen Baseline Preservation:** No commit may modify any file in `@syncplay/protocol`, `@syncplay/peer-mesh`, or `@syncplay/control-events` without explicit architectural re-authorization.
5. **Single-Purpose Commits:** Each sub-milestone must be committed as a clean, focused Git commit with a detailed conventional commit message.
