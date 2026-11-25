# Phase 7.5 Milestone M5 — Implementation Roadmap

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0)  
**Date:** 2026-11-25  
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)  
**Scope:** Staged execution plan and gating procedures for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Roadmap Phasing Strategy & Dependency Order

Milestone M5 connects the accepted snapshot recovery subsystem (`RecoveryCoordinator`, `SnapshotInstaller`, `QuarantineBuffer`) directly into `LeaderlessSyncEngine`.

Because `LeaderlessSyncEngine` is the core playback, causal ordering, and replication authority, M5 is divided into six sequential, risk-isolated sub-milestones (M5.0 through M5.5). Each sub-milestone has explicit entry prerequisites, strict scope constraints, and automated verification gates:

```mermaid
flowchart TD
    M5_0["Milestone M5.0: Architecture Audit & Interface Design<br/>(Strictly Read-Only / Current Milestone)"] --> M5_1["Milestone M5.1: Recovery Lifecycle & Coordinator Binding"]
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

### Milestone M5.0: Runtime Integration Architecture Audit & Interface Design
- **Role:** Independent Senior TypeScript Distributed-Systems Architect
- **Status:** CURRENT MILESTONE (Planning Only)
- **Objective:** Complete source audit and author architecture specifications for M5.
- **Scope:**
  - Source audit across `LeaderlessSyncEngine`, `RecoveryCoordinator`, `SnapshotInstaller`, `QuarantineBuffer`, `AntiEntropyManager`, and `RepairRequestPipeline`.
  - Resolve design questions A through G.
  - Author the 5 M5 architectural documents.
- **Affected Files:** Documentation files only (`docs/PHASE7_5_M5_*.md`). Zero production or test code modified.
- **Exit Condition:** Clean audit report; zero production diff; documentation committed; engineering authority approval.

---

### Milestone M5.1: Recovery Lifecycle Coupling & Coordinator Binding
- **Objective:** Establish the authoritative recovery lifecycle coupling between `RecoveryCoordinator` and `LeaderlessSyncEngine`.
- **Scope:**
  - Update `LeaderlessSyncEngineConfig` to optionally accept `recoveryCoordinator?: IRuntimeRecoveryCoordinator` or `recoveryOptions?: EngineRecoveryOptions`.
  - Refactor `LeaderlessSyncEngine.isRecoveryActive()` to query `this.recoveryCoordinator.state` dynamically, eliminating duplicate boolean state.
  - Implement `LeaderlessSyncEngine.attachRecoveryCoordinator(coordinator)`.
  - Wire coordinator lifecycle transitions to invoke `engine.setRecoveryMode(true/false)` and `engine.flushDeferredNotifications()`.
  - Ensure engine disposal (`engine.dispose()`) cleanly disposes the attached `RecoveryCoordinator`.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-lifecycle-binding.test.ts`
- **Invariants Enforced:**
  - `isRecoveryActive()` is 100% consistent with `recoveryCoordinator.state`.
  - Listener notifications are buffered while in recovery and flushed coalesced upon return to `ACTIVE`.
  - Disposal is clean with zero dangling timers or listener leaks.
- **Exit Condition:** Unit tests verify lifecycle coupling, state consistency, notification deferral/flushing, and disposal.

---

### Milestone M5.2: Inbound Remote-Event Interception & Quarantine
- **Objective:** Route incoming remote operations through `RecoveryCoordinator.interceptRemoteEvent()` during active recovery.
- **Scope:**
  - In `LeaderlessSyncEngine.receive()`, route remote events (`isLocal === false`) to `coordinator.interceptRemoteEvent(event, size)` when coordinator is attached.
  - If `shouldQuarantine === true`: record decision `BUFFERED_QUARANTINE`, bypass `receiveRemote()`, and guarantee zero mutation of `operationLog` or `PlaybackState`.
  - If `dropped === true`: record appropriate rejection decision (`REJECTED_RECOVERY_FENCE`, `DISCARD_OBSOLETE_EPOCH`, `ALREADY_OWNED_DUPLICATE`) without state mutation.
  - If `ADMIT_ACTIVE` (coordinator in `ACTIVE`): proceed to normal `receiveRemote()` execution.
  - Handle quarantine and future-epoch overflow exceptions by aborting recovery to `DEGRADED`.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-remote-interception.test.ts`
- **Invariants Enforced:**
  - Zero state mutation or causal advancement while recovery is in progress.
  - In-flight draining operations are deduplicated against newly arriving remote events.
  - Memory bounds (1,000 ops / 4 MiB quarantine; 100 ops / 512 KiB future epoch) are strictly enforced.
- **Exit Condition:** Unit tests prove remote events are quarantined during recovery and normal events admitted when active.

---

### Milestone M5.3: Local Input Gating & Synchronous Fail-Close
- **Objective:** Gate local user actions during recovery states, synchronously rejecting inputs.
- **Scope:**
  - In `LeaderlessSyncEngine.receiveLocal()`, call `coordinator.interceptLocalSubmission()`.
  - If `allowed === false`: throw synchronous `SyncEngineError(admission.code ?? 'REJECTED_RECOVERING', admission.reason)`.
  - Implement `LeaderlessSyncEngine.submit(intent)` helper that validates intent and calls `receiveLocal()`, fail-closing during recovery.
  - Ensure local sequence numbers (`localOriginSequence`) and local clocks are NOT advanced when local inputs are rejected.
- **Affected Files:**
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-local-gating.test.ts`
- **Invariants Enforced:**
  - Local inputs strictly prohibited during `RECOVERING`, `RECONCILING`, and `DEGRADED` states.
  - No silent queuing of local actions (preventing causal skew and intent inversion).
  - Local sequence counter and Lamport clock remain unmutated on rejection.
- **Exit Condition:** Unit tests verify synchronous rejection of all local control events during recovery.

---

### Milestone M5.4: Anti-Entropy Autonomous Trigger & Snapshot Installation Integration
- **Objective:** Autonomously trigger snapshot recovery from anti-entropy gap detection and execute atomic snapshot installation.
- **Scope:**
  - In `AntiEntropyManager.evaluateRemoteDigest()`, inspect `compareRemoteDigest(digest)`. If `requiresSnapshotRecovery === true` and `outsideRemoteHorizon === true`, invoke `onSnapshotRecoveryRequired` to trigger `coordinator.triggerRecovery(digest.senderPeerId)`.
  - Wire `RepairRequestPipeline.onHistoryExhausted` into `coordinator.triggerRecovery()`.
  - Wire `SnapshotInboundPipeline.onTransferComplete` (or direct transfer callback) to invoke `coordinator.executeRecoveryWithSnapshot(payload)`.
  - Verify that `executeRecoveryWithSnapshot` stages candidate core state, executes single-root swap `replaceCoreStateAtomic()`, and reclaims snapshot resources.
  - Enforce Lamport clock reseeding: $L_{\text{local}} = \max(L_{\text{local}}, L_{\text{snapshot}})$ without $+1$.
  - Execute post-installation quarantine drain via `reconcileQuarantine()`, iteratively draining quarantined operations until empty.
- **Affected Files:**
  - `packages/sync-engine/src/replication/anti-entropy-manager.ts`
  - `packages/sync-engine/src/engine/leaderless-sync-engine.ts`
  - `packages/sync-engine/src/recovery/recovery-coordinator.ts`
  - New test file: `packages/sync-engine/src/__tests__/recovery-anti-entropy-trigger.test.ts`
- **Invariants Enforced:**
  - Both reactive repair exhaustion and proactive digest comparison autonomously initiate recovery.
  - Triggering is strictly idempotent.
  - Snapshot installation is single-tick atomic.
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
  - Monorepo regression test run (all 67 test files, 1,550+ tests passing).
- **Affected Files:**
  - `packages/sync-engine/src/__tests__/recovery-engine-integration.test.ts`
  - Milestone M5 Completion Report: `docs/PHASE7_5_M5_COMPLETION_REPORT.md`
- **Invariants Enforced:**
  - 100% test pass rate across sync-engine and full monorepo.
  - Zero memory leaks, dangling timers, or unhandled promise rejections.
  - Zero regressions against Phase 7.3, 7.4, and M4 baselines.
- **Exit Condition:** All integration tests pass; independent audit sign-off; milestone frozen.

---

## 3. Sub-Milestone Gating Rules

To ensure strict engineering governance, the following gating rules are non-negotiable:

1. **Sequential Authorization:** Implementation of sub-milestone $M5.(n+1)$ may ONLY begin after $M5.n$ is completed, verified, and authorized by the Engineering Authority.
2. **Zero Regression Gate:** Every sub-milestone must pass all existing unit tests (minimum 965 in `@syncplay/sync-engine`, 1,550 monorepo total) with zero skipped tests and zero warnings.
3. **TypeScript Build Gate:** `npm run typecheck` and `npm run build` must exit with code 0 across all 6 workspaces on every commit.
4. **Frozen Baseline Preservation:** No commit may modify any file in `@syncplay/protocol`, `@syncplay/peer-mesh`, or `@syncplay/control-events` without explicit architectural re-authorization.
5. **Single-Purpose Commits:** Each sub-milestone must be committed as a clean, focused Git commit with a detailed conventional commit message.
