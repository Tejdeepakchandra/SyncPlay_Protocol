# Phase 7.5 M4.3b — Single-Root Atomic Installer Implementation Report

**Author:** Senior TypeScript Distributed-Systems Engineer  
**Mode:** STRICT IMPLEMENTATION — APPROVED ARCHITECTURE  
**Milestone:** Phase 7.5 Sub-Milestone M4.3b  
**Starting Parent Baseline:** `2589e33b5adb3056cfad1f1eeb3176dc69272530` (M4.3a final build closure)  
**Approved Architecture Parent:** `2bb117c5db474a13307be65c6f68845f2bd74720`  
**Date:** November 24, 2026  

---

## 1. Executive Summary

Phase 7.5 Sub-Milestone M4.3b implements the **Single-Root Atomic Installer** for leaderless snapshot recovery according to the approved M4.3-R4 architecture and specifications.

Previously, snapshot import and engine state initialization sequentially mutated multiple independent fields on `LeaderlessSyncEngine`. If an interruption, error, or race occurred, the engine was susceptible to inconsistent intermediate states.

Under M4.3b:
1. **Single Authoritative State Root (`EngineCoreState`):** All 12 authoritative engine state fields have been migrated into a unified container (`EngineCoreState`), sealed via `Object.seal()`.
2. **Encapsulated Internal Mutation:** The root container is solely owned by the engine. Public accessors expose defensive frozen copies (`Object.freeze`), completely prohibiting direct external mutation of internal maps, logs, or vectors.
3. **Four-Stage Installation Workflow:**
   - **Stage 1 (Dry-Run Validation):** Pure out-of-band JSON, schema, RFC 8785 canonical digest, epoch, Lamport, and frontier dominance validation. Zero engine mutation.
   - **Stage 2 (Staging Construction):** Pre-constructs candidate `LogicalClockManager`, candidate `OrderedOperationLog` (with pre-ingested checkpoints and frontier timestamps), candidate `PendingDependencyQueue`, and candidate vectors. Assembles and seals candidate `EngineCoreState` out-of-band. Zero engine mutation.
   - **Stage 3 (Irreversible Synchronous Atomic Commit Point):** Executes single-word pointer swap `this.core = candidateCore` in a single synchronous JavaScript event-loop turn under run-to-completion semantics. Cannot throw. Drops creator aliases post-commit.
   - **Stage 4:** Handed off to M4.3c (Reconciliation Drain & Post-Install Notification).
4. **Eligibility Rules:** Fully preserves the approved Phase 7.4 comparator semantics, enforcing strict prohibition of arbitrary tie-breaking across concurrent incomparable frontiers.

All **86 / 86** focused recovery tests and all **1,449 / 1,449** monorepo tests pass cleanly with **0 errors** on full production build (`npm run build`).

---

## 2. Pre-Implementation Audit & Approved Architecture Documents Inspected

Before making any source modifications, the authoritative M4.3 specifications were inspected:
- `docs/PHASE7_5_M4_3_ARCHITECTURE.md`
- `docs/PHASE7_5_M4_3_INTERFACE_SPECIFICATION.md`
- `docs/PHASE7_5_M4_3_IMPLEMENTATION_ROADMAP.md`
- `docs/PHASE7_5_M4_3_ACCEPTANCE_CRITERIA.md`
- `docs/PHASE7_5_M4_3_RISK_REGISTER.md`
- `docs/design/README.md`
- `docs/PHASE7_5_M4_3_R1_ARCHITECTURE_CONSISTENCY_CLOSURE_REPORT.md`
- `docs/PHASE7_5_M4_3_R2_FINAL_CONSISTENCY_CLOSURE_REPORT.md`
- `docs/PHASE7_5_M4_3_R3_FINAL_CONTRACT_CLOSURE_REPORT.md`
- `docs/PHASE7_5_M4_3_R4_FINAL_OWNERSHIP_CLOSURE_REPORT.md`

Starting Git baseline verified:
```bash
$ git status --short
(clean working tree)
$ git branch --show-current
main
$ git rev-parse HEAD
2589e33b5adb3056cfad1f1eeb3176dc69272530
$ git rev-parse HEAD^
9bb37491b2359e46cf7449a87152bf30600b0e7c
```

---

## 3. Exhaustive 26-Field Engine Inventory & 12 Authoritative Root Fields

All 26 fields of `LeaderlessSyncEngine` were inspected and categorized according to the approved architecture:

| # | Field Name | Type | Categorization | Architectural Placement | Rationale |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **1** | `state` | `PlaybackState` | **Authoritative State** | **Inside `EngineCoreState`** | Primary active playback state (position, rate, status, version). Deeply frozen. |
| **2** | `baseSnapshot` | `PlaybackState` | **Authoritative State** | **Inside `EngineCoreState`** | Compaction base anchor for speculative replay. Deeply frozen. |
| **3** | `baseSnapshotFrontier` | `Record<string, number>` | **Authoritative State** | **Inside `EngineCoreState`** | Highest compacted sequence numbers per origin. |
| **4** | `operationLog` | `OrderedOperationLog` | **Authoritative State** | **Inside `EngineCoreState`** | Authoritative totally-ordered operation log. |
| **5** | `clockManager` | `LogicalClockManager` | **Authoritative State** | **Inside `EngineCoreState`** | Authoritative Lamport logical clock state and validator. |
| **6** | `pendingQueue` | `PendingDependencyQueue` | **Authoritative State** | **Inside `EngineCoreState`** | Authoritative missing-dependency queue for buffered causal operations. |
| **7** | `replicationVector` | `Record<string, number>` | **Authoritative State** | **Inside `EngineCoreState`** | Highest contiguous applied sequences per origin peer. |
| **8** | `localCausalContext` | `Record<string, number>` | **Authoritative State** | **Inside `EngineCoreState`** | Observed causal horizon for local event generation. |
| **9** | `localOriginSequence` | `number` | **Authoritative State** | **Inside `EngineCoreState`** | Monotonic counter for locally originated events. |
| **10** | `currentEpoch` | `number` | **Authoritative State** | **Inside `EngineCoreState`** | Current membership configuration epoch. |
| **11** | `lastAppliedEventId` | `string \| undefined` | **Authoritative State** | **Inside `EngineCoreState`** | Identity of the most recently applied operation. |
| **12** | `stateVersion` | `number` | **Authoritative State** | **Inside `EngineCoreState`** | Monotonic state version counter. |
| **13** | `lastDecision` | `EngineDecision \| undefined` | **Non-Authoritative Cache** | Engine instance property | Cached return value from most recent engine decision. |
| **14** | `mutableStats` | `MutableStats` | **Non-Authoritative Cache** | Engine instance property | Operational telemetry counters. |
| **15** | `disposed` | `boolean` | **External Lifecycle** | Engine instance property | Lifecycle teardown flag. |
| **16** | `stateChangeListeners` | `Set<StateChangeListener>` | **External Observer Registry** | Engine instance property | External subscriber callbacks. |
| **17** | `decisionListeners` | `Set<DecisionListener>` | **External Observer Registry** | Engine instance property | External decision subscriber callbacks. |
| **18** | `peerGenerationListeners` | `Set<...>` | **External Observer Registry** | Engine instance property | External transport generation subscriber callbacks. |
| **19** | `config` | `ResolvedSyncEngineConfig` | **Immutable Configuration** | Injected in constructor | Fixed engine configuration parameters. |
| **20** | `clock` | `Clock` | **Immutable Configuration** | Injected in constructor | Wall-clock / monotonic time provider. |
| **21** | `authorizedEpochCapability` | `EpochTransitionCapability` | **Immutable Capability** | Injected in constructor | Capability for epoch increments. |
| **22** | `authorizedTransportCapability` | `TransportRegistrationCapability`| **Immutable Capability** | Injected in constructor | Capability for transport adapter registration. |
| **23** | `validSupplierContexts` | `WeakSet<...>` | **External Transport Custody**| Engine instance property | Set of valid transport supplier contexts. |
| **24** | `validRequesterContexts` | `WeakSet<...>` | **External Transport Custody**| Engine instance property | Set of valid transport requester contexts. |
| **25** | `conflictResolver` | `ConflictResolver` | **Stateless Utility** | Helper instance | Pure reducer and deterministic conflict resolution. |
| **26** | `compactionManager` | `CompactionManager` | **Stateless Utility** | Helper instance | Computes safe compaction frontiers from peer digests. |

---

## 4. Single-Root State Architecture & Encapsulated Mutation

### 4.1 `EngineCoreState` Container
Defined in `packages/sync-engine/src/recovery/engine-core-state.ts`:
- All 12 authoritative fields are declared on `EngineCoreState`.
- `createEngineCoreState()` constructs the root and immediately seals it with `Object.seal()`.
  - Properties cannot be dynamically added or removed.
  - Existing properties are marked non-configurable.
  - `playbackState` and `baseSnapshot` are deeply frozen with `Object.freeze()`.
- `createInitialEngineCoreState()` initializes the starting root for a new engine.

### 4.2 Single Authoritative Root Ownership in `LeaderlessSyncEngine`
In `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
- Replaced the 10 separate fields (`state`, `baseSnapshot`, `baseSnapshotFrontier`, `operationLog`, `clockManager`, `pendingQueue`, `localCausalContext`, `replicationVector`, `localOriginSequence`, `currentEpoch`) with a single reference:
  ```typescript
  private core: EngineCoreState;
  ```
- All internal readers and writers delegate dynamically to `this.core.*`.
- **Zero Duplicate State:** There is no duplicate field on `this`. Every accessor (`this.replicationVector`, `this.state`, etc.) reads or mutates the properties of the currently authoritative `this.core`.
- Public projections (`engine.replicationFrontier`, `engine.localFrontier`, `engine.playbackState`) return defensive copies or frozen objects:
  ```typescript
  public get replicationFrontier(): Readonly<Record<string, number>> {
    return { ...this.core.replicationVector };
  }
  ```
- Any mutation on the returned object has zero effect on the internal root.

---

## 5. Candidate Construction, Validation & Snapshot Eligibility

Implemented in `packages/sync-engine/src/recovery/snapshot-installer.ts`:

### 5.1 Eligibility Evaluation (`evaluateSnapshotEligibility`)
Strictly adheres to approved classifications and prohibits arbitrary tie-breaking:
- **`IDENTICAL`:** All origin sequences match and full history is verified ($k \le 1$ or matching checkpoints). Reject candidate (no recovery needed).
- **`FRONTIER_AGREEMENT_ONLY`:** Frontier matches ($seq > 1$), but intermediate history is unverified. Reject candidate (cannot overwrite local state without proof).
- **`STRICTLY_DOMINATES`:** Remote is $\ge$ on all origins and strictly $>$ on at least one origin. Eligible for snapshot installation.
- **`INCOMPARABLE`:** Remote is ahead on origin A, but local is ahead on origin B. Strictly rejected; arbitrary tie-breaking is prohibited.
- **`DOMINATED`:** Remote candidate is behind local replication vector, or belongs to an older membership epoch. Strictly rejected.
- **`FUTURE`:** Snapshot belongs to a future membership epoch. Preserved for M4.3a future-epoch buffer grace period.
- **`REPAIRABLE_WITHIN_HORIZON`:** Missing gap is within active log capacity ($gap \le \text{logCapacity}$). Handled via anti-entropy batch rather than snapshot recovery.

### 5.2 Four-Stage Installer (`SnapshotInstaller`)
- **Stage 1 — Out-Of-Band Validation (`validate`):**
  - Validates JSON parsing and outer `StateSnapshotPayloadV2Schema`.
  - Validates matching `sessionId`.
  - Re-computes RFC 8785 canonical JSON SHA-256 digest (`computeSnapshotDigestV2`) and checks against `sha256Digest`.
  - Validates Lamport clock invariant (`maxLogicalTimestamp === max(originFrontierTimestamps)`).
  - Validates deterministic sorting of compacted checkpoints `(originPeerId, sequenceNumber)`.
  - Validates eligibility via `evaluateSnapshotEligibility`.
  - Returns `SnapshotValidationResult`.
- **Stage 2 — Out-Of-Band Staging (`stage`):**
  - Instantiates candidate `LogicalClockManager`, candidate `OrderedOperationLog`, candidate `PendingDependencyQueue`, and candidate vectors.
  - Ingests compacted checkpoints and origin frontier timestamps.
  - Assembles and seals candidate `EngineCoreState` via `createEngineCoreState`.
  - Zero engine mutation during staging.
  - Returns `StagedSnapshotInstallation`.
- **Stage 3 — Atomic Commit (`commitAtomic`):**
  - Executes single synchronous pointer swap:
    ```typescript
    engine.replaceCoreStateAtomic(staged.candidateCore);
    ```
  - Cannot throw under single-threaded run-to-completion event loop semantics.
  - Explicitly nullifies creator alias:
    ```typescript
    (staged as { candidateCore: unknown }).candidateCore = null;
    ```
  - Returns `SnapshotInstallationReport`.

### 5.3 Stale Reference & Listener Safety
- **Stale Root Detachment:** When `this.core = candidateCore` executes, the old `EngineCoreState` is unreferenced by the engine. Subsequent operations on the engine operate strictly on the new root. Old projections held by external callers remain detached and immutable.
- **Recovery Mode & Deferred Notifications:**
  - `setRecoveryMode(true)` suppresses immediate listener callbacks during recovery.
  - `flushDeferredNotifications()` coalesces state change notifications into exactly one final state change and flushes decisions sequentially in causal order.
  - Listener exceptions during notification are isolated in `try/catch` and cannot roll back the committed root.

---

## 6. Files Created and Modified

1. **`packages/sync-engine/src/recovery/engine-core-state.ts`** *(NEW)*
   - `EngineCoreState` interface definition (all 12 authoritative fields).
   - `createEngineCoreState()` container sealing and deep freezing.
   - `createInitialEngineCoreState()` initial root construction.

2. **`packages/sync-engine/src/recovery/snapshot-installer.ts`** *(NEW)*
   - `SnapshotEligibility` enum and `evaluateSnapshotEligibility()` function.
   - `SnapshotValidationResult`, `StagedSnapshotInstallation`, `SnapshotInstallationReport`.
   - `ISnapshotInstaller` and `SnapshotInstaller` class (Stage 1 validate, Stage 2 stage, Stage 3 commitAtomic).

3. **`packages/sync-engine/src/errors.ts`** *(MODIFIED)*
   - Added typed error codes: `SNAPSHOT_INSTALLATION_FAILED`, `SNAPSHOT_INELIGIBLE`, `ENGINE_DISPOSED`, `INVALID_CANDIDATE_ROOT`.

4. **`packages/sync-engine/src/engine/leaderless-sync-engine.ts`** *(MODIFIED)*
   - Migrated 10 separate member fields into single authoritative `this.core: EngineCoreState`.
   - Added private delegating accessors for zero duplicate authoritative state.
   - Implemented `replaceCoreStateAtomic(candidate: EngineCoreState)`.
   - Implemented `isRecoveryActive()`, `setRecoveryMode(active: boolean)`, `flushDeferredNotifications()`.
   - Implemented `getReducerFunction()`, `coreState`, `playbackState`, `stateVersion`.
   - Refactored `importSnapshotV2` to construct candidate `EngineCoreState` and commit atomically via `replaceCoreStateAtomic`.

5. **`packages/sync-engine/src/index.ts`** *(MODIFIED)*
   - Exported `EngineCoreState`, `createEngineCoreState`, `createInitialEngineCoreState`.
   - Exported `SnapshotInstaller`, `evaluateSnapshotEligibility`, and associated installer types.

6. **`packages/sync-engine/src/__tests__/engine-core-state.test.ts`** *(NEW)*
   - 7 unit tests verifying root ownership, container sealing, delegation, reference isolation, and recovery notification deferral.

7. **`packages/sync-engine/src/__tests__/snapshot-installer.test.ts`** *(NEW)*
   - 20 unit tests verifying candidate eligibility classifications, Stage 1 out-of-band validation, Stage 2 staging, and Stage 3 atomic commit.

---

## 7. Test and Build Verification Evidence

### 7.1 Focused Recovery Suites (86 / 86 Passed)
```bash
$ npx vitest run packages/sync-engine/src/__tests__/recovery-state-machine.test.ts packages/sync-engine/src/__tests__/quarantine-buffer.test.ts packages/sync-engine/src/__tests__/future-epoch-buffer.test.ts packages/sync-engine/src/__tests__/recovery-admission.test.ts packages/sync-engine/src/__tests__/engine-core-state.test.ts packages/sync-engine/src/__tests__/snapshot-installer.test.ts
```
```text
 ✓ packages/sync-engine/src/__tests__/recovery-state-machine.test.ts (19 tests) 17ms
 ✓ packages/sync-engine/src/__tests__/quarantine-buffer.test.ts (14 tests) 33ms
 ✓ packages/sync-engine/src/__tests__/recovery-admission.test.ts (14 tests) 16ms
 ✓ packages/sync-engine/src/__tests__/future-epoch-buffer.test.ts (12 tests) 29ms
 ✓ packages/sync-engine/src/__tests__/engine-core-state.test.ts (7 tests) 17ms
 ✓ packages/sync-engine/src/__tests__/snapshot-installer.test.ts (20 tests) 32ms

 Test Files  6 passed (6)
      Tests  86 passed (86)
   Duration  1.12s
```

### 7.2 Full Monorepo Test Suite (1,449 / 1,449 Passed)
```bash
$ npm test
```
```text
> @syncplay/control-events@0.1.0 test: 49 passed (6 test files)
> @syncplay/peer-mesh@0.1.0 test: 108 passed (6 test files)
> @syncplay/protocol@0.1.0 test: 337 passed (6 test files)
> @syncplay/sync-engine@0.1.0 test: 864 passed (42 test files)  <-- +27 new tests (837 -> 864)
> @syncplay/bootstrap-server@0.1.0 test: 91 passed (3 test files)

Total Tests: 1,449 passed / 1,449 total (0 failed, 0 skipped)
```

### 7.3 Full Production Build (`npm run build`)
```bash
$ npm run build
```
```text
> syncplay-protocol@0.0.0 build
> npm run build --workspaces --if-present

> @syncplay/control-events@0.1.0 build: tsc (0 errors)
> @syncplay/peer-mesh@0.1.0 build: tsc (0 errors)
> @syncplay/protocol@0.1.0 build: tsc (0 errors)
> @syncplay/sync-engine@0.1.0 build: tsc (0 errors)
> @syncplay/bootstrap-server@0.1.0 build: tsc (0 errors)
> @syncplay/demo@0.1.0 build: vite build (195 modules, 212.71 kB, 0 errors)
```

---

## 8. Frozen Baseline Diff Verification

Executed `git diff aafa366cc48fdb2b83940f00e80d92d813e7c52e` across all frozen paths:
- `packages/protocol/*`: **0 diffs**
- `packages/sync-engine/src/recovery/chunk-assembler.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-serializer.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/candidate-offer-registry.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/inbound-custody.ts`: **0 diffs**

**Frozen Baseline Integrity:** **100% UNTOUCHED (0 lines modified)**

---

## 9. Explicitly Deferred Work (M4.3c & M4.3d)

As instructed by the approved roadmap, the following areas were strictly deferred:
- **M4.3c Scope:**
  - `RecoveryCoordinator` orchestration (`IRecoveryCoordinator`).
  - Quarantine drainer coordinator (`IQuarantineDrainer`).
  - `DRAINER_IN_FLIGHT` single-operation custody ($N_{\text{in-flight}} \le 1$).
  - Partial-drain resumption and destination acceptance receipts (`DestinationAcceptanceReceipt`).
  - Concurrent duplicate arrival reconciliation during drain.
  - End-to-end recovery completion and transition to `ACTIVE`.
- **M4.3d Scope:**
  - Full multi-peer recovery scenarios and 36-scenario architecture verification matrix.

---

## 10. Final Status Declaration

`M4.3b IMPLEMENTATION COMPLETE — READY FOR INDEPENDENT AUDIT`
