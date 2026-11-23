# Phase 7.5 M4.3b — Independent Source Audit Report

**Audit Mode:** STRICTLY READ-ONLY CODE AUDIT  
**Role:** Senior TypeScript Distributed-Systems Architect and Independent Code Auditor  
**Milestone:** Phase 7.5 Sub-Milestone M4.3b (Single-Root Atomic Installer)  
**Date:** November 24, 2026  
**Final Audit Verdict:** `M4.3b INDEPENDENT AUDIT FAIL — CORRECTION REQUIRED`

---

## 1. Audit Scope & Exact Git Baseline

| Metric | Required Specification | Audited Source / Value | Verification Result |
|---|---|---|---|
| **Audit Milestone** | Phase 7.5 M4.3b | M4.3b Single-Root Atomic Installer | Verified |
| **Commit Under Audit** | `a15ae9b183cb650a3443a5f3c40238cef45ca351` | `a15ae9b183cb650a3443a5f3c40238cef45ca351` | Match |
| **Expected Direct Parent** | `2589e33b5adb3056cfad1f1eeb3176dc69272530` | `2589e33b5adb3056cfad1f1eeb3176dc69272530` | Match (`git rev-parse HEAD^`) |
| **Approved Architecture Parent** | `2bb117c5db474a13307be65c6f68845f2bd74720` | `2bb117c5db474a13307be65c6f68845f2bd74720` | Verified |
| **Frozen Baseline Reference** | `aafa366cc48fdb2b83940f00e80d92d813e7c52e` | `aafa366cc48fdb2b83940f00e80d92d813e7c52e` | 0 diffs across frozen files |
| **Working Tree Status** | Clean | Clean (`git status --short` is empty) | Verified |

---

## 2. Source Files Inspected

The following implementation, configuration, and test files were inspected in full:

1. `packages/sync-engine/src/recovery/engine-core-state.ts` (199 lines) — `EngineCoreState` definition, factory, and bootstrap.
2. `packages/sync-engine/src/recovery/snapshot-installer.ts` (551 lines) — `SnapshotInstaller`, eligibility evaluation, and staged commit pipeline.
3. `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (2,594 lines) — Single-root `core` migration, delegating accessors, `replaceCoreStateAtomic`, recovery hooks.
4. `packages/sync-engine/src/errors.ts` — Engine error codes and types.
5. `packages/sync-engine/src/index.ts` — Package public export surface.
6. `packages/sync-engine/src/replication/replication-comparator.ts` — Phase 7.4 multi-vector comparator semantics.
7. `packages/sync-engine/src/__tests__/engine-core-state.test.ts` (301 lines) — Unit tests for single-root container.
8. `packages/sync-engine/src/__tests__/snapshot-installer.test.ts` (468 lines) — Unit tests for snapshot installer and eligibility.
9. Frozen pipeline baseline files:
   - `packages/sync-engine/src/recovery/chunk-assembler.ts`
   - `packages/sync-engine/src/recovery/snapshot-serializer.ts`
   - `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`
   - `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`
   - `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`
   - `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts`
   - `packages/sync-engine/src/recovery/candidate-offer-registry.ts`
   - `packages/sync-engine/src/recovery/inbound-custody.ts`
   - Entire `packages/protocol` package

---

## 3. Authoritative Root Inventory (12 Fields)

The approved M4.3-R4 architecture defines exactly 12 authoritative fields from the 26-field engine inventory. The audit verified their ownership and dereferencing:

| # | Field Name | Authoritative Container | Unique Authoritative Owner | Readers | Writers | Verification Assessment |
|---|---|---|---|---|---|---|
| **1** | `state` / `playbackState` | `this.core.playbackState` | `LeaderlessSyncEngine.core` | `this.state`, `getState()`, `playbackState` | `this.state = val`, `replaceCoreStateAtomic` | **Pass** — Deeply frozen, no duplicate field. |
| **2** | `baseSnapshot` | `this.core.baseSnapshot` | `LeaderlessSyncEngine.core` | `this.baseSnapshot`, `createLocalDigest` | `this.baseSnapshot = val`, `replaceCoreStateAtomic` | **Pass** — Deeply frozen, no duplicate field. |
| **3** | `baseSnapshotFrontier` | `this.core.baseSnapshotFrontier` | `LeaderlessSyncEngine.core` | `this.baseSnapshotFrontier` | `this.baseSnapshotFrontier = val`, `replaceCoreStateAtomic` | **Pass** — Delegated to `this.core`. |
| **4** | `operationLog` | `this.core.operationLog` | `LeaderlessSyncEngine.core` | `this.operationLog`, `logSize`, `getOperationLog` | `replaceCoreStateAtomic` (internal mutations via log methods) | **Pass** — Single instance inside root. |
| **5** | `clockManager` | `this.core.clockManager` | `LeaderlessSyncEngine.core` | `this.clockManager`, `logicalClock` | `replaceCoreStateAtomic` (internal mutations via clock methods) | **Pass** — Single instance inside root. |
| **6** | `pendingQueue` | `this.core.pendingQueue` | `LeaderlessSyncEngine.core` | `this.pendingQueue`, `pendingQueueSize`, `getPendingGaps` | `replaceCoreStateAtomic` (internal mutations via queue methods) | **Pass** — Single instance inside root. |
| **7** | `replicationVector` | `this.core.replicationVector` | `LeaderlessSyncEngine.core` | `this.replicationVector`, `replicationFrontier` | Direct mutation via private methods, `replaceCoreStateAtomic` | **Pass** — Single dictionary inside root. |
| **8** | `localCausalContext` | `this.core.localCausalContext` | `LeaderlessSyncEngine.core` | `this.localCausalContext`, `localFrontier` | Direct mutation via private methods, `replaceCoreStateAtomic` | **Pass** — Single dictionary inside root. |
| **9** | `localOriginSequence` | `this.core.localOriginSequence` | `LeaderlessSyncEngine.core` | `this.localOriginSequence` | `this.localOriginSequence = val`, `replaceCoreStateAtomic` | **Pass** — Delegated to `this.core`. |
| **10** | `currentEpoch` | `this.core.currentEpoch` | `LeaderlessSyncEngine.core` | `this.currentEpoch`, `epoch` | `this.currentEpoch = val`, `replaceCoreStateAtomic` | **Pass** — Delegated to `this.core`. |
| **11** | `lastAppliedEventId` | `this.core.lastAppliedEventId` | `LeaderlessSyncEngine.core` | `engine.coreState.lastAppliedEventId` | Set at initialization and snapshot staging/import | **Observation** — Not updated during regular event processing (see Finding 6). |
| **12** | `stateVersion` | `this.core.stateVersion` | `LeaderlessSyncEngine.core` | `this.stateVersion`, `stateVersion` | `this.state = val`, `replaceCoreStateAtomic` | **Pass** — Synchronized with `playbackState.stateVersion`. |

**Zero Duplicate State:** The 10 previously separate member variables on `LeaderlessSyncEngine` were removed. All reads and writes in `LeaderlessSyncEngine` delegate to `this.core`.

---

## 4. Public API Exposure & Encapsulation Findings

### Audit Finding FINDING-M4.3b-01: Direct External State Mutation via `coreState` Accessor
- **Severity:** `BLOCKING / CRITICAL`
- **Location:** `packages/sync-engine/src/engine/leaderless-sync-engine.ts:790-792`
- **Source Code:**
  ```typescript
  public get coreState(): Readonly<EngineCoreState> {
    return this.core;
  }
  ```
- **Execution Proof / Exploit:**
  Executing against production package exports:
  ```javascript
  const { LeaderlessSyncEngine } = require('./dist/index.js');
  const e = new LeaderlessSyncEngine({ localPeerId: 'p1', sessionId: 's1' });

  // 1. Mutate replication vector directly:
  e.coreState.replicationVector['peer-malicious'] = 999;
  console.log(e.replicationFrontier); // Output: { 'peer-malicious': 999 }

  // 2. Advance logical clock directly:
  e.coreState.clockManager.tickLocal();
  console.log(e.logicalClock); // Output: 1

  // 3. Mutate local origin sequence directly:
  e.coreState.localOriginSequence = 999;
  // Engine will now generate next event with sequence 1000!

  // 4. Mutate epoch directly (bypassing EpochTransitionCapability!):
  e.coreState.currentEpoch = 55;
  console.log(e.epoch); // Output: 55
  ```
- **Violation Analysis:**
  - TypeScript's `Readonly<EngineCoreState>` is completely erased at runtime and provides zero JavaScript security boundary.
  - `Object.seal(core)` only prevents adding and deleting properties (`configurable: false`). All existing properties remain `writable: true`.
  - Nested objects (`replicationVector`, `localCausalContext`, `clockManager`, `operationLog`, `pendingQueue`) are live mutable instances and plain objects.
  - An ordinary consumer can mutate authoritative engine state without invoking any engine validation or transition methods.
- **Mandatory Correction:**
  Remove `public get coreState()` or replace it with a deeply frozen, defensively cloned projection where every nested collection is disconnected from `this.core` and `clockManager`/`operationLog`/`pendingQueue` are completely inaccessible or exposed only through strictly read-only view interfaces.

---

### Audit Finding FINDING-M4.3b-02: Unprotected Public `replaceCoreStateAtomic` Permitting Arbitrary State Hijacking
- **Severity:** `BLOCKING / CRITICAL`
- **Location:** `packages/sync-engine/src/engine/leaderless-sync-engine.ts:813-823`
- **Source Code:**
  ```typescript
  public replaceCoreStateAtomic(candidate: EngineCoreState): void {
    if (!candidate || typeof candidate !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'Candidate core state must be a non-null object');
    }
    if (this.disposed) {
      throw new SyncEngineError('ENGINE_DISPOSED', 'Cannot replace core state on disposed engine');
    }

    // Irreversible synchronous reference replacement under JavaScript run-to-completion semantics
    this.core = candidate;
  }
  ```
- **Execution Proof / Exploit:**
  ```javascript
  const { LeaderlessSyncEngine, createEngineCoreState, LogicalClockManager, OrderedOperationLog, PendingDependencyQueue } = require('./dist/index.js');
  const e = new LeaderlessSyncEngine({ localPeerId: 'p1', sessionId: 's1' });
  const fake = createEngineCoreState({
    playbackState: { status: 'playing', anchorPositionMs: 12345, anchorLocalTimestamp: 0, playbackRate: 2, stateVersion: 99 },
    baseSnapshot: { status: 'playing', anchorPositionMs: 12345, anchorLocalTimestamp: 0, playbackRate: 2, stateVersion: 99 },
    baseSnapshotFrontier: {},
    operationLog: new OrderedOperationLog(256, { status: 'playing', anchorPositionMs: 0, anchorLocalTimestamp: 0, playbackRate: 1, stateVersion: 0 }, s => s),
    clockManager: new LogicalClockManager(999),
    pendingQueue: new PendingDependencyQueue(256),
    replicationVector: { hijacked: 1 },
    localCausalContext: {},
    localOriginSequence: 50,
    currentEpoch: 100,
  });
  e.replaceCoreStateAtomic(fake);
  console.log(e.getState());      // State hijacked: { status: 'playing', anchorPositionMs: 12345, ... }
  console.log(e.epoch);          // Epoch hijacked: 100
  console.log(e.logicalClock);   // Clock hijacked: 999
  console.log(e.replicationFrontier); // Frontier hijacked: { hijacked: 1 }
  ```
- **Violation Analysis:**
  - `replaceCoreStateAtomic` is marked `public` on `LeaderlessSyncEngine` without any capability, token, or coordinator authentication.
  - In Phase 7, all sensitive mutations (`transitionEpoch`, `registerInboundSupplierContext`) require authorized capabilities (`EpochTransitionCapability`, `TransportRegistrationCapability`).
  - Exposing `replaceCoreStateAtomic` publicly allows any external script to bypass all recovery verification, snapshot validation, eligibility checks, and epoch fencing to inject arbitrary unvalidated roots.
- **Mandatory Correction:**
  Gate `replaceCoreStateAtomic` with an authorized `RecoveryInstallationCapability` (injected at trusted engine initialization) or make it package-internal / accessible only to an authenticated `ISnapshotInstaller` / `RecoveryCoordinator`.

---

## 5. Playback & Replication Consistency Findings

### Audit Finding FINDING-M4.3b-03: Lamport Logical Clock Regression in `SnapshotInstaller.stage`
- **Severity:** `BLOCKING / HIGH`
- **Location:** `packages/sync-engine/src/recovery/snapshot-installer.ts:442`
- **Source Code:**
  ```typescript
  // In SnapshotInstaller.stage():
  const candidateClockManager = new LogicalClockManager(payload.maxLogicalTimestamp);
  ```
- **Execution Proof / Exploit:**
  ```javascript
  const e = new LeaderlessSyncEngine({ localPeerId: 'peer-local', sessionId: 'session-m43b-install', initialClock: 500 });
  console.log('initial clock:', e.logicalClock); // 500
  // Snapshot has payload.maxLogicalTimestamp = 100
  const val = await installer.validate(payload, 1, { 'peer-author': 0 });
  const staged = installer.stage(val, config);
  installer.commitAtomic(e, staged);
  console.log('clock after snapshot install:', e.logicalClock); // 100! (REGRESSION!)
  ```
- **Violation Analysis:**
  - In distributed systems, Lamport logical clocks must be strictly monotonic: an engine's logical clock must never decrease.
  - The approved Phase 7 Lamport clock reseeding invariant is:
    $$L_{\text{candidate}} = \max(L_{\text{local}}, \text{maxLogicalTimestamp})$$
    (as correctly implemented in `LeaderlessSyncEngine.importSnapshotV2:2330`).
  - `SnapshotInstaller.stage()` completely ignores $L_{\text{local}}$ and sets the candidate clock directly to `payload.maxLogicalTimestamp`. If $L_{\text{local}} > \text{maxLogicalTimestamp}$, installation causes the clock to regress backwards (from 500 to 100 in reproduction).
- **Mandatory Correction:**
  Pass the engine's current Lamport clock $L_{\text{local}}$ into `SnapshotInstaller.stage()` (or stage parameters) and reseed candidate clock manager with $\max(L_{\text{local}}, \text{payload.maxLogicalTimestamp})$.

---

### Audit Finding FINDING-M4.3b-04: Local Origin Sequence Reset to Zero in `SnapshotInstaller.stage`
- **Severity:** `BLOCKING / HIGH`
- **Location:** `packages/sync-engine/src/recovery/snapshot-installer.ts:497`
- **Source Code:**
  ```typescript
  // In SnapshotInstaller.stage():
  localOriginSequence: 0,
  ```
- **Execution Proof / Exploit:**
  ```javascript
  const e = new LeaderlessSyncEngine({ localPeerId: 'peer-local', sessionId: 'session-m43b-install' });
  e.coreState.localOriginSequence = 15; // Local peer has originated 15 operations
  const val = await installer.validate(payload, 1, { 'peer-author': 0 });
  const staged = installer.stage(val, config);
  installer.commitAtomic(e, staged);
  console.log('localOriginSequence after install:', e.coreState.localOriginSequence); // 0!
  ```
- **Violation Analysis:**
  - `localOriginSequence` tracks the highest sequence number originated by the local peer.
  - If a peer that has originated 15 operations recovers a snapshot and its `localOriginSequence` is reset to 0, its next originated event will be assigned sequence number 1.
  - This causes sequence number collision and creates duplicate `(localPeerId, sequenceNumber)` events, breaking causal ordering and anti-entropy deduplication across the mesh.
  - In contrast, `LeaderlessSyncEngine.importSnapshotV2:2395` correctly preserved `localOriginSequence: this.localOriginSequence`.
- **Mandatory Correction:**
  Preserve the local peer's existing origin sequence: pass `currentLocalOriginSequence` into `stage()` and set `localOriginSequence: currentLocalOriginSequence`.

---

### Audit Finding FINDING-M4.3b-05: Retention of External Caller References in `createEngineCoreState`
- **Severity:** `MEDIUM`
- **Location:** `packages/sync-engine/src/recovery/engine-core-state.ts:145-147`
- **Source Code:**
  ```typescript
  operationLog: params.operationLog,
  clockManager: params.clockManager,
  pendingQueue: params.pendingQueue,
  ```
- **Violation Analysis:**
  - While `replicationVector` and `localCausalContext` are copied (`{ ...params.replicationVector }`), the operational collections (`operationLog`, `clockManager`, `pendingQueue`) are assigned by reference directly from `params`.
  - Because `createEngineCoreState` is an exported public factory, any external caller creating a core state retains live references to the operational collections. If that candidate is passed to `replaceCoreStateAtomic`, the caller continues to hold live aliases capable of mutating engine internals.
- **Mandatory Correction:**
  Ensure that candidate assembly is strictly coupled to internal factories or that creator aliases are explicitly dropped/frozen.

---

### Audit Finding FINDING-M4.3b-06: Divergence of `lastAppliedEventId` During Regular Engine Operation
- **Severity:** `LOW`
- **Location:** `packages/sync-engine/src/engine/leaderless-sync-engine.ts:732-736`
- **Source Code:**
  ```typescript
  private set state(val: PlaybackState) {
    this.core.playbackState = val;
    this.core.state = val;
    this.core.stateVersion = val.stateVersion;
  }
  ```
- **Violation Analysis:**
  - `this.set state(val)` updates `this.core.playbackState`, `this.core.state`, and `this.core.stateVersion`, but does NOT update `this.core.lastAppliedEventId`.
  - During regular event reduction, `this.core.playbackState.lastAppliedEvent.logicalEventId` contains the actual event ID, but `this.core.lastAppliedEventId` remains `undefined` (or retains a snapshot ID).
- **Mandatory Correction:**
  In `this.set state(val)`, add `this.core.lastAppliedEventId = val.lastAppliedEvent?.logicalEventId;`.

---

## 6. Deep Mutability & Alias Ownership Analysis

- **`EngineCoreState` Container:** Shallow-sealed via `Object.seal(core)`. This prevents adding or deleting root properties, but all existing properties remain mutable (`writable: true`).
- **`playbackState` & `baseSnapshot`:** Deeply frozen via `Object.freeze()`. Direct property mutation throws `TypeError`. (Compliant)
- **Vectors (`replicationVector`, `localCausalContext`):** Plain mutable JavaScript dictionaries. Returned via defensive copies in public getters (`{ ...this.replicationVector }`), but exposed directly via `engine.coreState`. (Violated via `coreState`)
- **Operational Classes (`LogicalClockManager`, `OrderedOperationLog`, `PendingDependencyQueue`):** Mutable classes with internal state. Exposed directly via `engine.coreState`. (Violated via `coreState`)
- **Candidate Staging:** In `SnapshotInstaller.stage()`, candidate instances are constructed out-of-band and encapsulated in `staged.candidateCore`.
- **Candidate Alias Dropping:** In `SnapshotInstaller.commitAtomic()`, `(staged as { candidateCore: unknown }).candidateCore = null` drops the reference to the candidate root. However, because `staged.candidateCore` was accessible to the caller between `stage()` and `commitAtomic()`, any caller retaining a reference to `staged.candidateCore` before calling `commitAtomic()` retains an alias to the live engine root.

---

## 7. Atomic Commit & Failure Semantics Analysis

- **Dry-Run Validation (Stage 1):** `SnapshotInstaller.validate()` parses JSON, validates schema via `validateSnapshotPayloadV2`, computes RFC 8785 canonical hash via `computeSnapshotDigestV2`, verifies Lamport timestamps and checkpoint sorting, and evaluates snapshot eligibility. Modifies zero engine state. (Compliant)
- **Staging Construction (Stage 2):** `SnapshotInstaller.stage()` constructs candidate `LogicalClockManager`, candidate `OrderedOperationLog`, candidate `PendingDependencyQueue`, and candidate vectors completely out-of-band. Modifies zero engine state. (Compliant)
- **Commit Point (Stage 3):** `SnapshotInstaller.commitAtomic()` calls `engine.replaceCoreStateAtomic(staged.candidateCore)`. Under JavaScript run-to-completion single-threaded event-loop execution, the reference replacement `this.core = candidate` executes in a single synchronous turn without intermediate microtask yields. (Compliant)
- **Failure Rollback:** If validation fails or staging throws, the engine's active `core` remains 100% untouched. (Compliant)
- **Disposal Safety:** `replaceCoreStateAtomic` throws `ENGINE_DISPOSED` if called on a disposed engine. (Compliant)

---

## 8. Candidate Eligibility & Digest Analysis

`evaluateSnapshotEligibility()` in `snapshot-installer.ts` was audited against Phase 7.4 comparator semantics:

1. **`FUTURE` Epoch:** Returns `{ eligibility: 'FUTURE', eligible: false }` if snapshot epoch > current engine epoch. (Compliant)
2. **`DOMINATED` Epoch:** Returns `{ eligibility: 'DOMINATED', eligible: false }` if snapshot epoch < current engine epoch. (Compliant)
3. **`INCOMPARABLE` Frontiers:** When local and remote are each ahead on different origins (`remoteAheadCount > 0 && localAheadCount > 0`), returns `{ eligibility: 'INCOMPARABLE', eligible: false }`. Arbitrary tie-breaking is strictly prohibited. (Compliant)
4. **`DOMINATED` Frontier:** Returns `{ eligibility: 'DOMINATED', eligible: false }` when local is ahead and remote has no lead. (Compliant)
5. **`IDENTICAL` Frontier ($k \le 1$):** At base anchor ($k \le 1$), returns `{ eligibility: 'IDENTICAL', eligible: false }`. (Compliant)
6. **`FRONTIER_AGREEMENT_ONLY` ($k > 1$):** When frontiers match at $k > 1$ but intermediate history/timestamps are unverified, returns `{ eligibility: 'FRONTIER_AGREEMENT_ONLY', eligible: false }`. Prohibits overwriting state without historical proof. (Compliant)
7. **`REPAIRABLE_WITHIN_HORIZON`:** When maximum gap is within log capacity, returns `{ eligibility: 'REPAIRABLE_WITHIN_HORIZON', eligible: false }`, directing peer to anti-entropy rather than snapshot recovery. (Compliant)
8. **`STRICTLY_DOMINATES`:** When remote is ahead on at least one origin and $\ge$ on all origins, returns `{ eligibility: 'STRICTLY_DOMINATES', eligible: true }`. (Compliant)

---

## 9. Listener & Deferred Notification Semantics

- **Recovery Mode:** `isRecoveryActive()` and `setRecoveryMode(active: boolean)` allow buffering state change and decision notifications during recovery.
- **Notification Deferral:** When `recoveryActive === true`, state change notifications are pushed to `deferredStateChanges`.
- **Coalesced Flush:** `flushDeferredNotifications()` coalesces intermediate state transitions and emits only the final `PlaybackState`.
- **Decision Preservation:** `deferredDecisions` are dequeued and emitted sequentially in causal order.
- **Exception Isolation:** All listener invocations in `flushDeferredNotifications()` are wrapped in `try/catch` and increment `mutableStats.listenerErrorCount`, preventing listener crashes from corrupting engine state.

---

## 10. Frozen Baseline Integrity

Diff against frozen baseline commit `aafa366cc48fdb2b83940f00e80d92d813e7c52e`:

```bash
git diff aafa366cc48fdb2b83940f00e80d92d813e7c52e..HEAD -- \
  packages/protocol \
  packages/sync-engine/src/recovery/chunk-assembler.ts \
  packages/sync-engine/src/recovery/snapshot-serializer.ts \
  packages/sync-engine/src/recovery/snapshot-preparation-queue.ts \
  packages/sync-engine/src/recovery/snapshot-chunk-producer.ts \
  packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts \
  packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts \
  packages/sync-engine/src/recovery/candidate-offer-registry.ts \
  packages/sync-engine/src/recovery/inbound-custody.ts
```

**Result:** Clean / Empty diff (0 lines changed across all frozen paths). Frozen baseline integrity is 100% preserved.

---

## 11. Test & Build Execution Results

### 11.1 Test Suite Results
- **M4.3b & Focused Recovery Suites:** **86 / 86 tests passed** (1.20s)
  - `engine-core-state.test.ts`: 7/7 passed
  - `snapshot-installer.test.ts`: 20/20 passed
  - `recovery-state-machine.test.ts`: 19/19 passed
  - `quarantine-buffer.test.ts`: 14/14 passed
  - `future-epoch-buffer.test.ts`: 12/12 passed
  - `recovery-admission.test.ts`: 14/14 passed
- **Recovery & Snapshot Test Suites:** **286 / 286 tests passed** across 11 test files (1.51s)
- **Monorepo Complete Test Suite:** **1,449 / 1,449 tests passed** across 63 test files (0 failed, 0 skipped, duration: ~13s)
  - `@syncplay/control-events`: 49 passed
  - `@syncplay/peer-mesh`: 108 passed
  - `@syncplay/protocol`: 337 passed
  - `@syncplay/sync-engine`: 864 passed
  - `@syncplay/bootstrap-server`: 91 passed
- **Test Exclusions:** Verified 0 occurrences of `.only()` or `.skip()` in test files.

### 11.2 Production Build Results
- **Monorepo Build (`npm run build`):** Clean exit code 0 across all workspaces.
- **TypeScript Compilation (`tsc`):** Clean 0 errors across `@syncplay/protocol`, `@syncplay/control-events`, `@syncplay/peer-mesh`, `@syncplay/sync-engine`, `@syncplay/bootstrap-server`.
- **Demo Vite Build (`vite build`):** Clean production bundle output (195 modules transformed, 212 kB JS bundle).

---

## 12. Adversarial Scenario Matrix (24 Scenarios)

| # | Scenario Description | Actual Audit Finding / Execution Path | Verdict |
|---|---|---|---|
| **1** | Caller mutates a candidate vector after staging | `staged.candidateCore.replicationVector` is mutable before commit | **FAIL** (Finding 1/5) |
| **2** | Caller mutates a candidate log after staging | `staged.candidateCore.operationLog` is mutable before commit | **FAIL** (Finding 1/5) |
| **3** | Caller mutates a candidate pending queue after staging | `staged.candidateCore.pendingQueue` is mutable before commit | **FAIL** (Finding 1/5) |
| **4** | Consumer attempts to mutate `coreState` | Direct mutation of vectors, clock, sequence, epoch succeeds via `engine.coreState` | **FAIL** (Finding 1) |
| **5** | Consumer mutates a returned replication frontier | `engine.replicationFrontier` returns defensive copy `{ ...replicationVector }` | **PASS** |
| **6** | Old root references are retained across installation | `this.core = candidate` cleanly abandons old root pointer | **PASS** |
| **7** | A closure attempts to write through a stale root | Operations in engine dynamically dereference `this.core` | **PASS** |
| **8** | Candidate validation fails | Validation returns `{ valid: false }`, zero engine mutation | **PASS** |
| **9** | Candidate construction throws | Out-of-band staging failure leaves active engine root untouched | **PASS** |
| **10** | Listener throws after commit | Listener calls wrapped in `try/catch`, stats incremented, no rollback | **PASS** |
| **11** | Listener re-enters engine APIs | Reads observe committed state; notifications isolated | **PASS** |
| **12** | Two installation attempts occur in sequence | Staged object nullified post-commit; second commit throws `INVALID_ARGUMENT` | **PASS** |
| **13** | Installation is attempted after disposal | `replaceCoreStateAtomic` and `commitAtomic` reject with `ENGINE_DISPOSED` | **PASS** |
| **14** | Snapshot frontier matches but history differs | Returns `FRONTIER_AGREEMENT_ONLY`, installation rejected | **PASS** |
| **15** | Snapshot is strictly dominated | Local vector strictly ahead; returns `DOMINATED`, rejected | **PASS** |
| **16** | Snapshot histories are incomparable | Concurrent incomparable vectors; returns `INCOMPARABLE`, rejected | **PASS** |
| **17** | Checkpoint timestamp is missing | Validation fails with malformed/missing timestamp error | **PASS** |
| **18** | Lamport timestamp is inconsistent | Validation catches declared vs calculated mismatch, BUT `stage()` regresses local clock if $L_{\text{local}} > \text{maxLogicalTimestamp}$ | **FAIL** (Finding 3) |
| **19** | Snapshot belongs to an obsolete epoch | Returns `DOMINATED`, installation rejected | **PASS** |
| **20** | Snapshot belongs to a future epoch | Returns `FUTURE`, installation rejected | **PASS** |
| **21** | Pending queue contains operation already in candidate log | Fresh `candidatePendingQueue` instantiated in `stage()`, old queue dropped | **PASS** |
| **22** | Base snapshot frontier conflicts with replication vector | Both initialized identically from `payload.snapshotFrontier` | **PASS** |
| **23** | Old public projections are retained after installation | Projections are detached copies; unaffected by new installation | **PASS** |
| **24** | A public helper retains a candidate collection alias | `createEngineCoreState` retains collection references from caller | **FAIL** (Finding 5) |

---

## 13. Summary of Findings by Severity

| Finding ID | Severity | Title | Blocks Acceptance? |
|---|---|---|---|
| **FINDING-M4.3b-01** | `BLOCKING / CRITICAL` | Unrestricted Public Root Exposure via `coreState` Accessor Allowing External State Corruption | **YES** |
| **FINDING-M4.3b-02** | `BLOCKING / CRITICAL` | Unprotected Public Exposure of `replaceCoreStateAtomic` Permitting Arbitrary State Hijacking | **YES** |
| **FINDING-M4.3b-03** | `BLOCKING / HIGH` | Lamport Logical Clock Regression in `SnapshotInstaller.stage` Violating Monotonicity | **YES** |
| **FINDING-M4.3b-04** | `BLOCKING / HIGH` | Local Origin Sequence Reset to Zero in `SnapshotInstaller.stage` Causing Sequence Collisions | **YES** |
| **FINDING-M4.3b-05** | `MEDIUM` | Retention of External Caller References in `createEngineCoreState` | No (requires cleanup) |
| **FINDING-M4.3b-06** | `LOW` | Divergence of `lastAppliedEventId` During Regular Engine Operation | No (minor sync fix) |

---

## 14. Explicit Deferrals (M4.3c & M4.3d)

The following areas are explicitly confirmed as NOT implemented in M4.3b and correctly deferred:
1. **Master Recovery Coordinator (`IRecoveryCoordinator`):** Deferred to Sub-Milestone M4.3c.
2. **Quarantine Drain Loop & `DRAINER_IN_FLIGHT` 1-Op Custody:** Deferred to Sub-Milestone M4.3c.
3. **End-to-End Multi-Peer Recovery Matrix:** Deferred to Sub-Milestone M4.3d.

---

## 15. Final Audit Verdict

```text
M4.3b INDEPENDENT AUDIT FAIL — CORRECTION REQUIRED
```

### Reason for Verdict
While unit tests pass (1,449/1,449) and builds succeed, the independent source audit identified four **BLOCKING** architectural defects that violate the approved M4.3-R4 contract:
1. `LeaderlessSyncEngine.coreState` exposes the live, unproxied internal engine root, allowing external callers to directly mutate authoritative replication vectors, advance the logical clock, alter local sequence counters, and transition epochs without capabilities.
2. `LeaderlessSyncEngine.replaceCoreStateAtomic` is exposed as an unprotected public method without capability or coordinator gating, allowing arbitrary state hijacking.
3. `SnapshotInstaller.stage` initializes the candidate clock manager solely from `payload.maxLogicalTimestamp`, causing the engine's Lamport clock to regress backwards if the local clock was ahead of the snapshot.
4. `SnapshotInstaller.stage` hardcodes `localOriginSequence: 0`, destroying the local peer's origin sequence counter and causing subsequent locally originated events to duplicate sequence numbers.

A targeted correction cycle must resolve these four issues before M4.3b can be accepted and before M4.3c can be authorized.
