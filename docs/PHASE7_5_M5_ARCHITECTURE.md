# Phase 7.5 Milestone M5 — Engine Runtime Recovery Integration Architecture

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R17 AMENDED)
**Date:** 2026-11-25
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Baselines:** M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)
**Scope:** Architecture planning and runtime integration specification for Milestone M5. Strictly read-only audit and design.

---

## 1. Executive Summary & Objective

Milestone M4 established complete implementation and verification evidence for chunked snapshot transfers, sliding window pipelining, candidate offer registries, active supplier failover, single-root atomic installation, and quarantine buffering across 42 acceptance and multi-peer scenarios.

**Milestone M5** integrates this accepted recovery subsystem into the active runtime lifecycle of `LeaderlessSyncEngine`.

Prior to M5, the engine and recovery components operated with an explicit integration boundary: `RecoveryCoordinator` and `LeaderlessSyncEngine` were instantiated independently in test harnesses, with recovery triggered manually and drained operations submitted explicitly via test fixtures. Milestone M5 bridges this boundary so that:
1. Anti-entropy gap evaluation (`compareRemoteDigest`) and repair exhaustion (`onHistoryExhausted`) autonomously trigger snapshot recovery when operations fall outside the retained operation-log compaction horizon.
2. The engine's live ingestion path (`receive()`) routes incoming remote operations through a discriminated admission controller, ensuring current-epoch events enter quarantine, future-epoch events enter an independent 5-second staged buffer, and obsolete events are dropped, guaranteeing zero active state mutation during snapshot staging and installation. Original transport wire byte sizes are preserved throughout admission without reparsing or re-serialization.
3. Local user inputs (`receiveLocal()`, `submit()`) fail-close synchronously during recovery and degraded states, preventing concurrent local divergence and causal clock skew.
4. Snapshot installation synchronously and atomically replaces the engine's core state root (`EngineCoreState`) across all 12 authoritative components through M4.3b's opaque staged handle design, preserving exact Lamport monotonicity ($L_{\text{local}} = \max(L_{\text{local}}, L_{\text{snapshot}})$ without $+1$) and causal replication frontiers in one synchronous reference replacement under JavaScript run-to-completion semantics.
5. Incomparable but compatible histories ($V_A \parallel V_B$) are reconciled safely without discarding local operations that are ahead of the candidate snapshot, prioritizing anti-entropy delta repair or pre-install local operation stashing.
6. The post-installation quarantine drain protocol sequentially submits quarantined operations through the engine's causal admission pipeline (`admitDrainedOperation()`), enforcing bounded single-in-flight ownership ($N_{\text{in-flight}} \le 1$), advancing replication vectors, and emitting coalesced state notifications only after reconciliation successfully clears engine recovery mode and reaches `ACTIVE`.

---

## 2. Component Architecture & Ownership Model

The runtime recovery integration connects nine distinct architectural subsystems, organized in a clear, non-circular ownership hierarchy:

```mermaid
flowchart TD
    subgraph EngineRuntime ["LeaderlessSyncEngine Runtime Boundary"]
        LSE["LeaderlessSyncEngine<br/>(Core Playback, Causal Validation, Reducer)"]
        ECS["EngineCoreState Root<br/>(12 Authoritative Fields + 1 Alias)"]
        LSE -->|owns single sealed root| ECS
    end

    subgraph RecoverySubsystem ["Recovery Subsystem (M4 Accepted)"]
        RC["RecoveryCoordinator<br/>(Authoritative Recovery Orchestrator)"]
        RSM["RecoveryStateMachine<br/>(ACTIVE / RECOVERING / RECONCILING / DEGRADED)"]
        RAC["RecoveryAdmissionController<br/>(Local & Remote Admission Fences)"]
        QB["QuarantineBuffer<br/>(1,000 ops / 4 MiB aggregate / 4 KiB event)"]
        FEB["FutureEpochBuffer<br/>(100 ops / 512 KiB aggregate / 4 KiB event / 5s grace)"]
        QD["QuarantineDrainer<br/>(N_in-flight <= 1 Authoritative Drainer)"]
        SI["SnapshotInstaller<br/>(Stage 1 Validate, Stage 2 Stage, Stage 3 Commit)"]

        RC -->|owns| RSM
        RC -->|owns| RAC
        RC -->|owns| QB
        RC -->|owns| FEB
        RC -->|owns| QD
        RC -->|owns| SI
    end

    subgraph ReplicationSubsystem ["Replication & Anti-Entropy (Phase 7.4)"]
        AEM["AntiEntropyManager<br/>(Digest Generation & Comparison)"]
        RRP["RepairRequestPipeline<br/>(Log-Based Operation Catch-Up)"]
        CM["CompactionManager<br/>(Safe Frontier & View Planning)"]

        AEM -->|coordinates| RRP
        LSE -->|owns| CM
    end

    %% Bindings and Invocations
    LSE -->|owns & delegates lifecycle| RC
    RC -.->|commitAtomic with StagedHandle| SI
    SI -.->|replaceCoreStateAtomic| LSE
    QD -.->|admitDrainedOperation| LSE
    AEM -->|triggerRecovery on OUTSIDE_REMOTE_HORIZON| RC
    RRP -->|triggerRecovery on onHistoryExhausted| RC
```

### 2.1 Capability Custody, Attachment Semantics & Failure Behavior (Closing F5, R2-H5, R3-07, R6-01)

Milestone M5 follows the established capability-custody architecture of Phase 7.5 (e.g. `RecoveryInstallationCapability`, `InboundTransportCapability`), relying on branded symbols, frozen objects, and strict reference identity verification.

#### 2.1.1 Recovery Installation Capability
The accepted installation capability model relies on frozen object identity:
`export interface RecoveryInstallationCapability { readonly authority: string; readonly sessionId: string; readonly token: string; }`.
To preserve exact capability custody and single-attachment guarantees without claiming un-modeled capability "revocation":

1. **Custody & Object Identity:**
   - `LeaderlessSyncEngine` creates and stores `private readonly authorizedRecoveryCapability?: RecoveryInstallationCapability;`.
   - `replaceCoreStateAtomic(candidate, capability)` enforces strict object-reference identity: `if (capability !== this.authorizedRecoveryCapability) throw new SyncEngineError('UNAUTHORIZED')`.
   - No dynamic revocation token or membrane exists in the accepted baseline; security rests strictly on in-process reference confinement.
2. **Construction Options:**
   - **Option A (Autonomous Internal Construction — Standard Production):** The engine constructs its own `RecoveryCoordinator`, mints an authorized capability via `createRecoveryInstallationCapability()`, binds the coordinator via `coordinator.bindEngine(engine, capability)`, and retains private custody.
   - **Option B (External Coordinator Injection — Advanced Testing):** The caller provides an already constructed `recoveryCoordinator` and authorized capability. The engine binds to it during initialization. Supplying both `recoveryOptions` and `recoveryCoordinator` throws `SyncEngineError('INVALID_CONFIG')`.
3. **Attachment Lifecycle Matrix:**
   - **Invalid Capability:** If `capability !== this.authorizedRecoveryCapability`, `replaceCoreStateAtomic` rejects immediately with `SyncEngineError('UNAUTHORIZED')`.
   - **Already-Bound Coordinator:** Calling `coordinator.bindEngine()` when already attached throws `SyncEngineError('ALREADY_BOUND')`. A coordinator cannot be re-bound or shared across multiple engines.
   - **Successful Binding:** Coordinator stores the engine reference and authorized capability in private fields.
   - **Failure During Engine Construction:** If engine initialization throws after `bindEngine()` has run (e.g. invalid configuration or constructor failure), engine construction unwinds; the engine reference is never published, and the partially constructed engine is reclaimed by garbage collection. An externally injected coordinator bound during a failed constructor call cannot be safely reused and must be discarded.
   - **Engine Disposal:** Invoking `engine.dispose()` invokes `coordinator.dispose()`. Calling `replaceCoreStateAtomic` on a disposed engine throws `SyncEngineError('ENGINE_DISPOSED')`, effectively dead-ending the capability even if a reference is retained externally.

#### 2.1.2 Drain Admission Context Capabilities: Unforgeable Provenance & Package-Boundary Contract (Closing R6-01, R7-01, R8-05, R9-07, R10-01, R10-03, R11-A, R11-B, R12-C, R13-A–R13-E, R14-A–R14-E, R15-A–R15-G, R16-A–R16-D, R17-A–R17-C)
A typed string (`'LOCAL_REPLAY' | 'REPAIR_BATCH'`) is a declaration of context, not proof of provenance: any caller can supply a string. To prevent accidental trust escalation where untrusted remote operations bypass arrival-rate limits and clock-inflation checks, post-snapshot drain admission is capability-gated:

```typescript
// Declared in module packages/sync-engine/src/recovery/drain-admission-custody.ts
// Module-private: NEVER re-exported from packages/sync-engine/src/index.ts
const DRAIN_ADMISSION_BRAND: unique symbol = Symbol('DRAIN_ADMISSION_BRAND');

export interface TrustedLocalReplayCapability {
  readonly [DRAIN_ADMISSION_BRAND]: true;
  readonly kind: 'LOCAL_REPLAY';
  readonly sessionId: string;
  readonly authority: string;
  readonly token: string;
}

export interface TrustedRepairBatchCapability {
  readonly [DRAIN_ADMISSION_BRAND]: true;
  readonly kind: 'REPAIR_BATCH';
  readonly sessionId: string;
  readonly authority: string;
  readonly token: string;
}

export type DrainAdmissionContext =
  | TrustedLocalReplayCapability
  | TrustedRepairBatchCapability;

export interface AuthorizedDrainCapabilities {
  readonly localReplayCapability: TrustedLocalReplayCapability;
  readonly repairBatchCapability: TrustedRepairBatchCapability;
}
```

#### Seven Non-Negotiable Capability & Custody Properties:
1. **Module-Private Branding Symbol:** `DRAIN_ADMISSION_BRAND` is declared in internal module `drain-admission-custody.ts` and is **strictly prohibited from being exported** in the public package entry point (`packages/sync-engine/src/index.ts`). Ordinary SDK consumers cannot import or fabricate a branded capability literal with `[DRAIN_ADMISSION_BRAND]: true`.
2. **Internal Factories & Runtime Shallow Immutability (R8-05):** Capability instances are minted strictly by internal module factories (`createLocalReplayCapability`, `createRepairBatchCapability`) and shallow-frozen with `Object.freeze()` so they are immutable at runtime. Shallow freezing is completely sufficient because all capability properties (`kind`, `sessionId`, `authority`, `token`) are primitive values. The containing `AuthorizedDrainCapabilities` container object is likewise shallow-frozen with `Object.freeze()`.
3. **Private Transfer via Composition Root & Scope of `bindEngine()` (R8-05):**
   - `LeaderlessSyncEngine` mints its own authorized `localReplayCapability` and `repairBatchCapability` during initialization, bundled into an immutable `AuthorizedDrainCapabilities` container (`Object.freeze(...)`).
   - The capabilities are transferred privately to `RecoveryCoordinator` via `coordinator.bindEngine(this, this.authorizedRecoveryCapability, drainCapabilities)`.
   - `bindEngine()` is strictly an internal composition-root binding method, NOT an open public reconfiguration API. It enforces the single-attachment invariant (`if (this.engine !== null) throw new SyncEngineError('ALREADY_BOUND')`) and rejects unauthorized rebinding.
   - **Zero Public Accessors:** Neither `LeaderlessSyncEngine` nor `RecoveryCoordinator` nor `QuarantineDrainer` provides ANY public getter, method, or property exposing these capability references (`engine.localReplayCapability` and `coordinator.repairBatchCapability` do not exist). Capabilities cannot be retrieved through getters, diagnostics, state snapshots, debug introspection, or event objects.
4. **Physical Custody Segregation & Least Privilege (R8-05):**
   - `QuarantineDrainer` is initialized strictly with `TrustedRepairBatchCapability`. It has **zero reference** to `TrustedLocalReplayCapability`. Remote quarantine draining can NEVER obtain or present local replay capability.
   - The local stashed reconciliation workflow is held privately by `RecoveryCoordinator` and provided with `TrustedLocalReplayCapability`.
   - Neither component receives or retains unnecessary capabilities.
5. **Exact Reference Identity Check:** `admitDrainedOperation(op, capability, attemptId)` evaluates the presented capability using strict object-reference identity (`capability === this.authorizedLocalReplayCapability` or `capability === this.authorizedRepairBatchCapability`).
6. **Package-Boundary Export Guarantee (R8-05, R9-07):**
   - The package entry manifest (`packages/sync-engine/package.json`) defines strictly:
     ```json
     "exports": {
       ".": {
         "types": "./dist/index.d.ts",
         "import": "./dist/index.js"
       }
     }
     ```
     Under standard Node.js and TypeScript module resolution, root `.` is the ONLY supported package entry point. There are NO wildcard subpaths (`"./*"`), NO `./recovery/*` exports, and NO internal deep-import paths.
   - **Wording Qualification:** This guarantee is an API package boundary against supported package-specifier imports (e.g. `import ... from '@syncplay/sync-engine/recovery/...'` throws `ERR_PACKAGE_PATH_NOT_EXPORTED` at runtime and fails compilation). It is not claimed to be an OS-level file permission sandbox against an attacker with direct local filesystem access to `node_modules` files.
   - The public package entry point (`packages/sync-engine/src/index.ts`) exports **strictly TypeScript type declarations** (`export type { DrainAdmissionContext, TrustedLocalReplayCapability, TrustedRepairBatchCapability, AuthorizedDrainCapabilities }`), never runtime values, symbols, or factory functions. Type-only exports pose zero runtime risk because the engine validates exact object identity against privately retained capability instances.
7. **In-Realm Defensive Trust Model Clarification:** In a cooperative JavaScript runtime, unique symbols and opaque objects provide structural encapsulation and defensiveness against API misuse, configuration errors, and duck-typing, but do not construct a cryptographic sandbox against hostile code running in the identical JavaScript realm. Actual protection is achieved through private capability custody, exact object-reference checks, package export gating, and fail-closed validation.

---

## 3. Authoritative Lifecycle vs. Operational Recovery Mode (Closing F1, F3, R2-C2, R3-03)

To prevent contradictory or duplicate recovery authorities, Milestone M5 defines three strictly distinguished tiers:

```mermaid
flowchart TD
    subgraph Tier1 ["Tier 1: Authoritative Recovery Lifecycle"]
        RC_State["RecoveryCoordinator.stateMachine.state<br/>('ACTIVE' | 'RECOVERING' | 'RECONCILING' | 'DEGRADED' | 'DISPOSED')<br/><b>SOLE AUTHORITATIVE LIFECYCLE STATE</b>"]
    end

    subgraph Tier2 ["Tier 2: Public Engine Recovery Queries"]
        LSE_State["engine.recoveryState -> delegates to coordinator.state<br/>engine.isRecoveryInProgress() -> true iff RECOVERING or RECONCILING<br/>engine.isDegraded() -> true iff DEGRADED<br/>engine.isRecoveryActive() -> alias of isRecoveryInProgress()"]
    end

    subgraph Tier3 ["Tier 3: Internal Engine Operational Mode"]
        LSE_Flag["engine.recoveryActive (boolean)<br/><b>RESTRICTED SLAVE OPERATIONAL SWITCH</b><br/>Gates notifyStateChange() -> pushes to deferredStateChanges<br/>Gates notifyDecision() -> pushes to deferredDecisions<br/>Toggled strictly via engine.setRecoveryMode(boolean)"]
    end

    RC_State -->|authoritative query| LSE_State
    RC_State -.->|drives operational switch| LSE_Flag
```

### 3.1 Strict Separation of Lifecycle State and Operational Mode (R3-03, R5-03)
1. **Tier 1 — Authoritative Coordinator Lifecycle State (`coordinator.stateMachine.state`):** The sole system authority for recovery state (`ACTIVE`, `RECOVERING`, `RECONCILING`, `DEGRADED`, `DISPOSED`). Owns state transitions, recovery substates, attempt sequence IDs, and timeout controllers.
2. **Tier 2 — Public Engine State Queries:**
   - `engine.recoveryState`: Readonly getter delegating directly to `coordinator.stateMachine.state` (or `'ACTIVE'` if unattached, `'DISPOSED'` if engine is disposed).
   - `engine.isRecoveryInProgress()`: Derived boolean helper returning `true` **if and only if** `recoveryState === 'RECOVERING' || recoveryState === 'RECONCILING'`. It returns `false` in `ACTIVE`, `DEGRADED`, and `DISPOSED`.
   - `engine.isDegraded()`: Derived boolean helper returning `true` **if and only if** `recoveryState === 'DEGRADED'`.
   - `engine.isRecoveryActive()`: Retained strictly as a backward-compatible alias of `isRecoveryInProgress()`.
   - **Crucial Caller Contract:** Callers and external clients must NEVER treat `!isRecoveryInProgress()` as an indicator of healthy, operational active state. In the `DEGRADED` state, `isRecoveryInProgress()` returns `false` because recovery is no longer actively executing; however, the engine is halted and rejecting operations. To determine whether the engine is ready for normal operation, callers must check `engine.recoveryState === 'ACTIVE'` (or `!engine.isRecoveryInProgress() && !engine.isDegraded()`).
3. **Tier 3 — Internal Operational Mode (`this.recoveryActive` in `LeaderlessSyncEngine`):**
   - An engine-internal operational switch that gates event notification dispatch.
   - When `this.recoveryActive === true`, `notifyStateChange` buffers into `this.deferredStateChanges` and `notifyDecision` buffers into `this.deferredDecisions`.
   - **Locked Deferral in `DEGRADED` State:** If recovery terminates unrecoverably or is aborted into `DEGRADED`, `engine.setRecoveryMode(true)` is **strictly maintained**. Operational notification deferral remains locked; `deferredStateChanges` and `deferredDecisions` are NEVER flushed. The engine remains fail-closed to local inputs (`submit` throws `REJECTED_DEGRADED`) and drops remote events (`REJECTED_DEGRADED`).
   - **No Lifecycle Authority:** `this.recoveryActive` has zero awareness of `RECOVERING` vs `RECONCILING` vs `DEGRADED`. It is strictly a slave operational gate toggled by the coordinator via `engine.setRecoveryMode(boolean)`. No independently mutable engine boolean may serve as a second recovery lifecycle authority.

### 3.2 Comprehensive Lifecycle Admission Matrix

| Lifecycle State (`recoveryState`) | Local Intent Policy (`submit` / `receiveLocal`) | Remote Event Ingestion Policy (`receive`) | Public Ingestion Outcome |
|---|---|---|---|
| **`ACTIVE`** | **Admitted.** Sequenced contiguously ($S_{\text{local}} += 1$), advances Lamport clock, captures causal context, reduces into log. | **Admitted.** Synchronous $O(1)$ check. Passes directly to `receiveRemote()`, admitted to log, reduces state. | `ADMIT_ACTIVE` |
| **`RECOVERING`** | **Synchronously Rejected.** Throws `SyncEngineError('REJECTED_RECOVERING')`. Zero sequence advance, zero clock advance. Local intent fails closed. | **Intercepted & Segregated.**<br/>• Current Epoch $\to$ `QuarantineBuffer`<br/>• Future Epoch ($E > E_{\text{local}}$) $\to$ `FutureEpochBuffer` (5s grace)<br/>• Obsolete Epoch ($E < E_{\text{local}}$) $\to$ Discarded<br/>• In-flight duplicate $\to$ Dropped<br/>Zero mutation of operation log or active playback state. | `QUARANTINED`<br/>`BUFFERED_FUTURE_EPOCH`<br/>`DISCARDED_OBSOLETE_EPOCH`<br/>`DROPPED_DRAINER_DUPLICATE` |
| **`RECONCILING`** | **Synchronously Rejected.** Throws `SyncEngineError('REJECTED_RECOVERING')`. Zero sequence advance, zero clock advance. | **Intercepted & Segregated.** Identical to `RECOVERING`. Newly arriving events enter active generation of `QuarantineBuffer` while detached generation is drained. | `QUARANTINED`<br/>`BUFFERED_FUTURE_EPOCH`<br/>`DISCARDED_OBSOLETE_EPOCH`<br/>`DROPPED_DRAINER_DUPLICATE` |
| **`DEGRADED`** | **Synchronously Rejected.** Throws `SyncEngineError('REJECTED_DEGRADED')`. Engine is halted; requires explicit application session reset. | **Intercepted & Dropped.** Dropped immediately with decision `REJECTED_DEGRADED`. Zero buffer allocation, zero log insertion. | `REJECTED_DEGRADED` |
| **`DISPOSED`** | **Synchronously Rejected.** Throws `SyncEngineError('DISPOSED')`. | **Rejected.** Engine is shut down. Zero activity. | `REJECTED_DISPOSED` |

### 3.3 Complete Overflow Decision Contract Across Boundaries (R2-H2, R3-04)

```typescript
export type RemoteAdmissionDecision =
  | { readonly outcome: 'ADMIT_ACTIVE' }
  | { readonly outcome: 'QUARANTINED'; readonly sequenceNumber: number; readonly originPeerId: string; readonly rawSerializedLength: number }
  | { readonly outcome: 'BUFFERED_FUTURE_EPOCH'; readonly targetEpoch: number; readonly expiresAtMs: number }
  | { readonly outcome: 'DISCARDED_OBSOLETE_EPOCH'; readonly eventEpoch: number; readonly currentEpoch: number }
  | { readonly outcome: 'DROPPED_DRAINER_DUPLICATE'; readonly originPeerId: string; readonly sequenceNumber: number }
  | { readonly outcome: 'REJECTED_DEGRADED'; readonly failureReason?: string }
  | { readonly outcome: 'REJECTED_DISPOSED' };
```

#### Precise Boundary Exception & Return Behavior (R3-04)
1. **Buffer Boundary (`QuarantineBuffer` / `FutureEpochBuffer`):**
   - On limit breach (count > 1000 or aggregate bytes > 4 MiB for quarantine; count > 100 or bytes > 512 KiB for future epoch), `enqueue()` throws `SyncEngineError('QUARANTINE_OVERFLOW')` or `SyncEngineError('FUTURE_EPOCH_OVERFLOW')`.
   - **Zero Retention Proof:** The rejected operation is rejected *before* array push and identity indexing. Retained byte counters (`_activeBytes`) and count counters (`totalRetainedCount`) are not incremented. The operation is not retained or partially owned by the buffer.
2. **Coordinator Boundary (`RecoveryCoordinator.interceptRemoteEvent()`):**
   - Catches `QUARANTINE_OVERFLOW` / `FUTURE_EPOCH_OVERFLOW`.
   - Autonomously transitions to `DEGRADED`: calls `this.abortRecovery(failureReason, err.message)`.
   - Re-throws the `SyncEngineError`, preserving complete diagnostic context (error code, message, active byte totals, and breached threshold).
   - For non-overflow paths, returns one of the 7 declared `RemoteAdmissionDecision` outcomes above.
3. **Engine Boundary (`LeaderlessSyncEngine.receive()`):**
   - Preserves remote error isolation: remote operations **never throw** unhandled exceptions out of `receive()`.
   - Catches `SyncEngineError('QUARANTINE_OVERFLOW')` / `FUTURE_EPOCH_OVERFLOW`.
   - Maps the error to an `EngineDecision`: `accepted: false`, `code: 'REJECTED_CAPACITY'`, with message and diagnostic details preserved from the caught error.
   - Records the decision via `recordDecision(event, decision)` and drops the remote event.
   - **Contract Clarity:** `REJECTED_CAPACITY` is a declared `EngineDecision.code` emitted by the engine, NOT a variant of `RemoteAdmissionDecision`.
   - For local user actions (`receiveLocal` / `submit`), local inputs **do throw** `SyncEngineError('REJECTED_DEGRADED')` synchronously.

### 3.4 Administrative Recovery Reset Semantics, Canonical Entry Accounting, Composite Identity & Publication Atomicity (Closing R6-02, R7-02, R8-01–R8-05, R9-01–R9-06, R10-01–R10-04, R11-A–R11-D, R12-A–R12-D, R13-A–R13-E, R14-A–R14-E, R15-A–R15-G, R16-A–R16-D, R17-A–R17-C)

`resetRecovery()` exists strictly as an administrative recovery reset, NOT as a general-purpose escape hatch to clear degradation while leaving incomplete or corrupted state in place.

#### 1. Strict State Guard, Capability Authorization & State-Machine Transition Paths (R13-E)
- `resetRecovery(capability, resetAction)` is permitted **strictly from the `DEGRADED` state** (`this.stateMachine.state === 'DEGRADED'`). Calling it from `ACTIVE`, `RECOVERING`, `RECONCILING`, or `DISPOSED` throws `SyncEngineError('INVALID_STATE')`.
- Requires an authorized `RecoveryInstallationCapability` checked via strict object-reference identity (`capability === this.authorizedRecoveryCapability`). Unauthorized invocations throw `SyncEngineError('UNAUTHORIZED')`.
- **Authoritative State-Machine Transition Path (R13-E):**
  Inspection of the accepted M4.3 `RecoveryStateMachine` (`recovery-state-machine.ts:49-54`) establishes the authoritative transition graph:
  ```typescript
  const VALID_STATE_TRANSITIONS: Readonly<Record<RecoveryState, ReadonlySet<RecoveryState>>> = {
    ACTIVE: new Set<RecoveryState>(['RECOVERING', 'DEGRADED']),
    RECOVERING: new Set<RecoveryState>(['RECONCILING', 'DEGRADED', 'ACTIVE']),
    RECONCILING: new Set<RecoveryState>(['ACTIVE', 'DEGRADED']),
    DEGRADED: new Set<RecoveryState>(['RECOVERING']),
  };
  ```
  `DEGRADED` permits transition **only to `RECOVERING`**. A direct transition from `DEGRADED` to `ACTIVE` is prohibited and throws a state machine error. Therefore, the administrative reset lifecycle strictly executes:
  - **For `REINITIALIZE_CLEAN`:** `DEGRADED` $\to$ `RECOVERING` (with substate `RESET_STAGING` or `COMMITTING`) $\to$ `ACTIVE` (strictly after clean root replacement and disengaging recovery mode).
  - **For `PURGE_AND_RETRY`:** `DEGRADED` $\to$ `RECOVERING` (with substate `IDLE` / `AWAITING_OFFER`) $\to$ `RECONCILING` (once new transfer/staging completes and drain resumes).

#### 2. Safe Reset Actions, Monotonic Attempt Sequence & Latched Attempt States (R13-B, R14-E)
- **Monotonic Attempt Advance:** Every invocation of `resetRecovery` advances the attempt counter: `const newAttemptId = ++this.currentAttemptId;`. All pending asynchronous recovery workflows check `attemptId === this.currentAttemptId`.
- **Latched Reset Attempt State Machine:**
  Each reset attempt is tracked by an explicit record with latched terminal outcomes:
  ```typescript
  export type ResetAttemptState =
    | 'INITIATED'              // 1. Reset invoked, attempt ID minted
    | 'WAITING_FOR_SETTLEMENT' // 2. Drainer fenced, settlement barrier awaited
    | 'SETTLED'                // 3. In-flight work settled, barrier resolved (N_in-flight == 0)
    | 'TIMED_OUT'              // 4. Watchdog expired. Latched terminal failure; cannot proceed
    | 'INVALIDATED'            // 5. Superseded by newer reset or abort. Latched terminal failure
    | 'COMPLETED';             // 6. Root replaced, lifecycle active, notifications released
  ```
- **Latched Terminal Outcome (R13-B):** Once an attempt transitions to `TIMED_OUT` or `INVALIDATED`, it is **permanently latched**: it can never transition to `SETTLED` or `COMPLETED`. A late-settling barrier or late-arriving callback is strictly prevented from reviving or continuing that attempt.
- **Independent Verification for New Reset Attempts (R14-E):**
  When a new reset attempt is initiated, it must independently verify:
  1. Old asynchronous work actually settled on the event loop.
  2. Old operation custody was resolved (restored to buffer head or retired).
  3. Old in-flight slot is verified empty ($N_{\text{in-flight}} == 0$).
  4. Old continuations cannot perform state mutation or external side-effects (attempt-ID fencing).
  5. The new attempt remains current, authorized, and non-terminal.
  A barrier resolution alone NEVER authorizes a reset continuation; continuation requires current attempt authorization.

#### 3. Complete 10-Step Lifecycle Sequence for Administrative Reset (R12-B, R13-B, R13-E, R14-A, R14-C, R15-A–R15-E, R16-A–R16-D, R17-A–R17-C)

To reconcile asynchronous settlement with the settlement barrier, canonical entry accounting, transactional retirement, and clean-reset commit, the protocol specifies an authoritative 10-step sequence:

1. **Step 1: Administrative Reset Requested:** Invocation of `resetRecovery(capability, resetAction)` strictly from `DEGRADED`.
2. **Step 2: Attempt Allocation & Monotonic Advance:** Coordinator increments `this.currentAttemptId` (`newAttemptId = ++this.currentAttemptId`) and mints `attempt = { attemptId: newAttemptId, status: 'INITIATED', action: resetAction }`.
3. **Step 3: State-Machine Transition & Drainer Fencing:**
   - Coordinator transitions state machine: `this.stateMachine.transition('RECOVERING', 'Administrative reset initiated', 'RESET_STAGING')`.
   - Coordinator flags drainer: `this.quarantineDrainer.fenceActiveDrain(newAttemptId)`. Drainer sets `_inFlightRecord.invalidated = true` (if an operation is in flight) and prohibits any new dequeue ($N_{\text{in-flight}} \le 1$).
   - `attempt.status = 'WAITING_FOR_SETTLEMENT'`.
4. **Step 4: Mandatory Settlement Barrier Execution:** Coordinator awaits `await this.quarantineDrainer.drainSettledBarrier()`.
   - **Watchdog Timeout Invariant (R13-B):** The barrier is guarded by `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`. If the watchdog expires before settlement:
     - Coordinator marks `attempt.status = 'TIMED_OUT'` with `terminalReason = 'SETTLEMENT_WATCHDOG_EXPIRED'`.
     - The attempt is **permanently latched**; it cannot enter archive retirement or replace the engine root.
     - The in-flight slot remains owned ($N_{\text{in-flight}} = 1$); the timeout does NOT cancel the underlying Promise and does NOT clear the in-flight slot.
     - Coordinator aborts to `DEGRADED` (`DRAIN_HANG_TIMEOUT`). Engine root remains untouched.
5. **Step 5: In-Flight Operation Settlement & Custody Resolution:** The underlying asynchronous operation finishes on the JavaScript event loop.
   - Destination admission boundary checks `attemptId === coordinator.currentAttemptId`. Because the attempt was fenced/invalidated, the engine returns typed receipt `ATTEMPT_INVALIDATED` (`STALE_ATTEMPT_FENCED`) with **zero engine state mutation**!
   - In-flight custody is resolved: operation is restored to `QuarantineBuffer` head via `unshiftDrain(op)`.
   - In-flight slot is released: `this._inFlightRecord = null` ($N_{\text{in-flight}} \to 0$).
6. **Step 6: Settlement Barrier Resolution:** The barrier promise resolves. The in-flight slot is verified empty ($N_{\text{in-flight}} == 0$).
   - If the attempt was previously latched as `TIMED_OUT` or `INVALIDATED`, continuation execution terminates immediately with zero side-effects.
   - If valid, coordinator transitions `attempt.status = 'SETTLED'`.
7. **Step 7: Transactional Quarantine Ownership Transfer & Zero-Custody Verification (R13-A, R14-A, R14-B, R14-C, R15-A–R15-E, R16-A–R16-D, R17-A–R17-C):**
   - Under `REINITIALIZE_CLEAN`:
     - **Pre-Condition Check:** Step 6 has resolved and confirmed `quarantineDrainer.inFlightRecord === null` ($N_{\text{in-flight}} == 0$) and `quarantineDrainer.isDraining === false`. All uncommitted operations reside strictly within `QuarantineBuffer`.
     - **Atomic 11-Stage Retirement Execution:** Executes the 11-stage canonical retirement transaction (Section 3.4.4): discovers composite operation identities, validates length-prefixed keys with UTF-8 byte lengths, constructs detached `TerminalQuarantineEntry` records, measures canonical UTF-8 bytes using `TextEncoder`, pre-reserves capacity in `TerminalArchiveCoreState` (Option A), atomically commits the batch via candidate-root swap, and coordinates with `QuarantineBuffer` to retire the source operations under Option A.
     - **Dual Zero-Custody Verification:** Coordinator asserts BOTH `quarantineBuffer.totalRetainedCount === 0` AND `quarantineDrainer.inFlightRecord === null`. If any operation remains in either buffer or drainer, clean reset fails closed to `DEGRADED`.
   - Under `PURGE_AND_RETRY`, operations remain safely in `QuarantineBuffer` for the new recovery attempt.
8. **Step 8: State Replacement (Irrevocable Single Root Pointer Replacement):**
   - Under `REINITIALIZE_CLEAN`, `engine.replaceCoreStateAtomic(cleanBaseline, capability)` executes single synchronous root-pointer replacement under JavaScript run-to-completion semantics across all 12 core state fields.
   - Under `PURGE_AND_RETRY`, the existing log is preserved.
9. **Step 9: Lifecycle Transition:**
   - Under `REINITIALIZE_CLEAN`, `engine.setRecoveryMode(false)`. Coordinator transitions `this.stateMachine.transition('ACTIVE', 'Clean re-initialization complete', 'IDLE')`. `attempt.status = 'COMPLETED'`.
   - Under `PURGE_AND_RETRY`, coordinator resets to `RECOVERING` (substate `IDLE` / `AWAITING_OFFER`) under `newAttemptId`, maintaining `setRecoveryMode(true)`. Once new transfer completes, moves to `RECONCILING` to resume drain.
10. **Step 10: Notification Release:**
    - Under `REINITIALIZE_CLEAN`, `engine.flushDeferredNotifications()` dispatches deferred notifications via non-throwing dispatch (delivering 0 notifications if none were enqueued, preserving the zero-notification contract).

#### 4. R16-D — Portable Composite Identity Encoding & Full-Identity Quarantine Matching, and R17-B — Nested Records & Canonical Bytes

##### Source Grounding & Schema Reconciliation (R15-E, R16-D)
In accepted source code (`packages/sync-engine/src/recovery/quarantine-buffer.ts:43-50, 167-172` and `packages/sync-engine/src/engine/leaderless-sync-engine.ts:982-995`), operation identity operates at two distinct layers:
1. **Existing Quarantine Custody Index:** `QuarantineBuffer` indexes retained operations by generating peer and monotonic sequence counter: `const key = `${op.originPeerId}:${op.sequenceNumber}`` (`this.identityIndex = new Map<string, number>()`).
2. **Existing Epoch Fencing:** Operations carry `op.membershipEpoch` and are evaluated against `this.currentEpoch` (`epoch < this.currentEpoch` triggers obsolete rejection; `epoch > this.currentEpoch` triggers unestablished future rejection).
3. **Payload Logical Identity:** The contained sync event carries `op.event.message.logicalEventId`, which uniquely identifies the event domain instance.

To eliminate field-naming divergence, the immutable retirement identity explicitly preserves the exact accepted source schema property `sequenceNumber` (directly mapped from `op.sequenceNumber`, eliminating any ambiguous renaming to `originSequence`):

```typescript
export interface CompositeOperationIdentity {
  readonly originPeerId: string;    // Generating peer domain identifier (op.originPeerId)
  readonly sequenceNumber: number;  // Strictly monotonic sequence counter for originPeerId (op.sequenceNumber)
  readonly membershipEpoch: number; // Membership configuration epoch (op.membershipEpoch)
  readonly logicalEventId: string;  // Unique logical event identifier (op.event.message.logicalEventId)
}
```

##### Portable Length-Prefixed Canonical Key Encoding (R16-D)
Delimiter-based key serialization (such as colon-separated strings) is vulnerable to delimiter collision if `originPeerId` or `logicalEventId` contain colons or slashes. Furthermore, string length measurements based on JavaScript UTF-16 code units diverge from wire byte lengths if non-ASCII characters appear.

The canonical operation key uses an unambiguous, length-prefixed encoding where string lengths are measured strictly in **UTF-8 bytes** using `TextEncoder`:

```typescript
export function canonicalOperationIdentityKey(id: CompositeOperationIdentity): string {
  // 1. Strict numeric integer validation
  if (!Number.isSafeInteger(id.sequenceNumber) || id.sequenceNumber <= 0) {
    throw new SyncEngineError('INVALID_IDENTITY', `Invalid sequenceNumber: ${id.sequenceNumber}`);
  }
  if (!Number.isSafeInteger(id.membershipEpoch) || id.membershipEpoch < 0) {
    throw new SyncEngineError('INVALID_IDENTITY', `Invalid membershipEpoch: ${id.membershipEpoch}`);
  }

  // 2. Strict non-empty string validation
  if (!id.originPeerId || typeof id.originPeerId !== 'string' || !id.originPeerId.trim()) {
    throw new SyncEngineError('INVALID_IDENTITY', 'originPeerId must be a non-empty string');
  }
  if (!id.logicalEventId || typeof id.logicalEventId !== 'string' || !id.logicalEventId.trim()) {
    throw new SyncEngineError('INVALID_IDENTITY', 'logicalEventId must be a non-empty string');
  }

  // 3. UTF-8 byte length computation
  const peerIdBytes = new TextEncoder().encode(id.originPeerId).length;
  const eventIdBytes = new TextEncoder().encode(id.logicalEventId).length;

  return `opid:${peerIdBytes}:${id.originPeerId}/${id.sequenceNumber}/${id.membershipEpoch}/${eventIdBytes}:${id.logicalEventId}`;
}
```

*Cross-Platform Determinism:*
- Peer IDs conform strictly to `ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/` (`packages/protocol/src/constants.ts:87`), bounding characters to printable ASCII ($1 \le L \le 128$).
- Measuring UTF-8 bytes guarantees absolute consistency across Node.js LTS, browsers, and WebWorkers, without dependency on host string encoding or UTF-16 code unit indexing.
- *Example:* For `originPeerId = "peer-A"` (6 UTF-8 bytes), `sequenceNumber = 42`, `membershipEpoch = 1`, `logicalEventId = "evt_9a8b7c"` (10 UTF-8 bytes), the key evaluates to:
  `opid:6:peer-A/42/1/10:evt_9a8b7c`.

##### Full Identity Comparison After Quarantine Index Hit (R16-D)
In `QuarantineBuffer`, operations are indexed by the 2-field key `${op.originPeerId}:${op.sequenceNumber}` in `this.identityIndex`. When matching an incoming or retiring `CompositeOperationIdentity`:
1. **Index Query:** Query `this.identityIndex.has(`${id.originPeerId}:${id.sequenceNumber}`)`.
2. **Index Miss:** If the index does not hold the key, the operation is absent $\to$ rejected fail-closed with `SyncEngineError('RETIREMENT_SOURCE_MISMATCH')`.
3. **Index Hit — 4-Field Verification:** If the index hits, the buffer retrieves the quarantined operation envelope and performs an exhaustive equality check across all four composite fields:
   - `op.originPeerId === id.originPeerId`
   - `op.sequenceNumber === id.sequenceNumber`
   - `op.membershipEpoch === id.membershipEpoch`
   - `op.event.message.logicalEventId === id.logicalEventId`
4. **Conflict Resolution:**
   - If all 4 fields match: exact identity hit $\to$ processed idempotently.
   - If `originPeerId` and `sequenceNumber` match, but `membershipEpoch` or `logicalEventId` differ: **REJECTS FAIL-CLOSED** with `SyncEngineError('RETIREMENT_BATCH_CONFLICT')`. This collision signifies a divergent or split-brain payload attempting to retire under an existing sequence slot. Clean reset is immediately aborted, quarantine custody is retained, and zero operations are deleted.

##### Nested Records & Canonical Wire Byte Protection (R15-G, R16-A, R17-B)
1. **Shallow vs. Deep Object Immutability:**
   In JavaScript, `Object.freeze()` is strictly shallow: it prevents mutation of own properties of the target object, but does NOT freeze referenced nested objects. All nested plain records (such as `TerminalQuarantineEntry` and `QuarantineRetirementTransactionRecord`) are independently frozen before being placed into parent collections.
2. **TypedArray Non-Freezability & Detached Copying (R17-B):**
   Non-empty TypedArrays (`Uint8Array`) **cannot be frozen** via `Object.freeze()` (calling `Object.freeze()` on a non-empty `Uint8Array` throws a `TypeError: Cannot freeze array buffer views with elements` in strict mode in V8). TypedArrays are therefore managed as **private, encapsulated backing storage**:
   - `TerminalQuarantineEntry` privately holds `canonicalWireBytes: Uint8Array`.
   - Accessors, receipts, and export batches never expose this internal array directly; they return a fresh detached defensive clone:
     ```typescript
     const exposedBytes = new Uint8Array(entry.canonicalWireBytes);
     ```
   - Mutating `exposedBytes` modifies only the caller's copy; the archive's internal byte representation remains 100% unaltered.
3. **Single Immutable Staged Representation:**
   The exact byte length measurement (`new TextEncoder().encode(canonicalJson).length`), the SHA-256 digest computation (`crypto.subtle.digest('SHA-256', stagedBytes)`), and the bytes committed to `TerminalQuarantineArchive` are derived from the **exact same immutable staged `Uint8Array`**.
4. **Strict 4,096-Byte Entry Boundary & Oversized Quarantining:**
   - Individual complete serialized entries cannot exceed 4,096 canonical wire bytes (`MAX_TERMINAL_ENTRY_SERIALIZED_BYTES = 4096`).
   - If an operation produces a complete terminal entry exceeding 4,096 bytes, it **CANNOT** be admitted to `TerminalQuarantineArchive`.
   - Any oversized entry remains safely quarantined in `QuarantineBuffer` and causes administrative clean reset (`REINITIALIZE_CLEAN`) to fail closed with `SyncEngineError('RESET_PREREQUISITE_FAILED')`.
   - Oversized operations are **NEVER truncated, corrupted, or silently discarded**.
   - Dual archive bounds remain strictly enforced: max 500 entries (`MAX_TERMINAL_ARCHIVE_ENTRIES = 500`), max 2 MiB aggregate wire bytes (`MAX_TERMINAL_ARCHIVE_TOTAL_BYTES = 2097152`), max 4,096 bytes per entry (`MAX_TERMINAL_ENTRY_SERIALIZED_BYTES = 4096`).

##### Authoritative 11-Stage Transaction Lifecycle (R14-A, R15-A–R15-G, R16-A–R16-D, R17-A–R17-C)
1. **Stage 1 (Quarantine Generation Freeze):** Coordinator freezes the current quarantine generation (`quarantineBuffer.freezeGeneration()`).
2. **Stage 2 (Settle All In-Flight Work):** Confirms Step 6 settlement has completed: `quarantineDrainer.inFlightRecord === null` ($N_{\text{in-flight}} == 0$) and `quarantineDrainer.isDraining === false`. Restored in-flight operations are locked inside the frozen generation.
3. **Stage 3 (Retirement Set Discovery):** Constructs an immutable snapshot of all operations in the frozen generation without mutating buffer ownership.
4. **Stage 4 (Composite Identity & Metadata Validation):** Validates each operation's `CompositeOperationIdentity` (`sequenceNumber`, `originPeerId`, `membershipEpoch`, `logicalEventId`). Computes length-prefixed `canonicalOperationIdentityKey`. Conflicting records fail closed with `RETIREMENT_BATCH_CONFLICT`.
5. **Stage 5 (Complete Detached Entry Construction):** Constructs complete detached `TerminalQuarantineEntry` records (`rejectionStatus: 'REINITIALIZE_CLEAN_PURGED'`), shallow-freezing each record independently (`Object.freeze(entry)`).
6. **Stage 6 (Canonical Serialization):** Canonically serializes each complete `TerminalQuarantineEntry` using RFC 8785 JCS (`canonicalEntryJson = canonicalize(entry)`).
7. **Stage 7 (Portable UTF-8 Byte Measurement):** Measures exact wire bytes using `new TextEncoder().encode(canonicalEntryJson).length`. Asserts each entry $\le 4096$ bytes. Computes aggregate complete entry bytes $B_{\text{complete}} = \sum b_i$.
8. **Stage 8 (Immutable Wire Bytes Packaging):** Encodes canonical JSON into private `Uint8Array` stored in `entry.canonicalWireBytes`.
9. **Stage 9 (Archive Pre-Reservation - Option A):** Invokes `archive.reserveCapacity(N, B_{\text{complete}}, currentArchiveGen)` to obtain `reservationId`. Publishes candidate root incorporating the new reservation atomically.
10. **Stage 10 (Candidate-Root Archive Commit - Option A):** Invokes `archive.commitRetirementBatch(retirementTxId, batchDigest, reservationId, stagedBatch)`. Atomically linearizes entry publication, transaction recording, and reservation consumption via single pointer replacement.
11. **Stage 11 (Atomic Source Retirement & Dual Custody Verification):** Invokes `quarantineBuffer.retireCommittedBatch(operationIdentities, capability)` under Option A (atomic source retirement). Coordinator asserts BOTH `quarantineBuffer.totalRetainedCount === 0` AND `quarantineDrainer.inFlightRecord === null`.

#### 5. R17-A — Published Map Encapsulation & Runtime Ownership, R16-B — Archive Reservation Linearization (Option A) & R16-C — Drainer Custody Reconciliation

##### Published Map Encapsulation & Runtime Ownership Correction (R17-A)
In ECMAScript, `Object.freeze(map)` on a JavaScript `Map` object only prevents adding, deleting, or reconfiguring properties on the Map instance wrapper itself. **It does NOT prevent mutating internal Map entries**: `Map.prototype.set()`, `delete()`, and `clear()` operate on the internal `[[MapData]]` slot and remain fully functional even when called on an `Object.freeze()`-ed Map!

Therefore, claiming that `Object.freeze(map)` guarantees runtime immutability is technically false in JavaScript.

To establish defensible runtime immutability and complete encapsulation, the recovery architecture enforces the **Eight Rules of Map Ownership and Encapsulation**:

1. **Private Implementation Details:** Backing `Map` instances are strictly private fields of the owning component (`_core.committedTransactions`, `_core.activeReservations`, `_core.identityIndex`).
2. **Exclusive Root Ownership:** A published core state object exclusively owns its internal Map instances. No external component holds a reference to these maps.
3. **Zero Backing Map Exposure:** No public getter, accessor, receipt, callback, or exported structure ever returns or leaks a backing Map reference.
4. **Lexical Builder Isolation:** In every state transition, candidate maps are allocated fresh in local lexical scope (`const nextMap = new Map(...)`). Once the atomic pointer swap occurs, builder references go out of scope and cannot be retained, exported, or mutated.
5. **No Post-Publication Mutation:** No method or continuation ever calls `.set()`, `.delete()`, or `.clear()` on a Map belonging to an already-published root.
6. **Copy-on-Write (CoW) State Transitions:** Every state modification (reservation, cancellation, commit, export) constructs a fresh `Map` and a fresh candidate root object, publishing via a single reference assignment.
7. **Failure Isolation:** If candidate construction, allocation, or validation throws, the authoritative published root remains 100% unaltered.
8. **Safe Detached Public Read Operations:** Public read queries return safe representations:
   - `archive.getTransaction(id)` returns a shallow-frozen record copy: `existingRecord ? Object.freeze({ ...existingRecord }) : undefined`.
   - `archive.hasTransaction(id)` returns a primitive boolean.
   - `archive.getEntries()` returns a shallow-frozen detached Array copy: `Object.freeze([...this._core.entries])`.

```typescript
export interface TerminalArchiveCoreState {
  readonly entries: ReadonlyArray<TerminalQuarantineEntry>;
  readonly committedTransactions: ReadonlyMap<string, QuarantineRetirementTransactionRecord>;
  readonly activeReservations: ReadonlyMap<string, ArchiveReservationRecord>;
  readonly totalBytes: number;
  readonly size: number;
  readonly archiveGeneration: number;
}

export interface QuarantineBufferCoreState {
  readonly activeQueue: ReadonlyArray<QuarantinedOperation>;
  readonly drainBatch: ReadonlyArray<QuarantinedOperation>;
  readonly identityIndex: ReadonlyMap<string, number>;
  readonly activeBytes: number;
  readonly drainBytes: number;
  readonly generationId: number;
  readonly drainGenerationId: number | null;
}
```

##### Archive Reservation Linearization (Option A Selected - R16-B)
M5.0-R16 formally selects **Option A — Candidate-Root Reservation Integration**: `activeReservations` is placed directly inside `TerminalArchiveCoreState`.

This design guarantees that reservation consumption and archive entry publication are **linearized at the exact same atomic instruction** (`this._core = candidateCore;`):

```typescript
commitRetirementBatch(
  retirementTransactionId: string,
  batchDigest: string,
  reservationId: string,
  stagedBatch: readonly TerminalQuarantineEntry[],
): TerminalBatchCommitReceipt {
  // 1. Authoritative lookup in committed transactions
  const existingRecord = this._core.committedTransactions.get(retirementTransactionId);
  if (existingRecord) {
    if (existingRecord.batchDigest === batchDigest) {
      return {
        committed: true,
        alreadyCommitted: true,
        transactionId: retirementTransactionId,
        entryCount: existingRecord.entryCount,
        totalSerializedBytes: existingRecord.totalSerializedBytes,
        archiveGeneration: this._core.archiveGeneration,
        batchDigest: existingRecord.batchDigest,
      };
    }
    throw new SyncEngineError(
      'TRANSACTION_ID_DIGEST_MISMATCH',
      `Transaction ${retirementTransactionId} already committed with different digest`,
    );
  }

  // 2. Validate reservation against activeReservations inside the authoritative root
  const reservation = this._core.activeReservations.get(reservationId);
  if (!reservation) {
    throw new SyncEngineError('RESERVATION_NOT_FOUND', `Reservation ${reservationId} not found`);
  }
  if (Date.now() > reservation.expiresAtMs) {
    throw new SyncEngineError('RESERVATION_EXPIRED', `Reservation ${reservationId} has expired`);
  }
  if (reservation.archiveGeneration !== this._core.archiveGeneration) {
    throw new SyncEngineError('GENERATION_MISMATCH', 'Archive generation advanced since reservation');
  }

  // 3. Validate staged batch integrity and exact counts
  if (stagedBatch.length !== reservation.reservedCount) {
    throw new SyncEngineError('BATCH_COUNT_MISMATCH', 'Staged batch count does not match reservation');
  }
  let batchBytes = 0;
  for (const entry of stagedBatch) {
    if (entry.byteLength > MAX_TERMINAL_ENTRY_SERIALIZED_BYTES) {
      throw new SyncEngineError('ENTRY_SIZE_EXCEEDED', `Entry ${entry.id} exceeds 4096 bytes`);
    }
    batchBytes += entry.byteLength;
  }
  if (batchBytes !== reservation.reservedBytes) {
    throw new SyncEngineError('BATCH_BYTE_MISMATCH', 'Staged batch byte total does not match reservation');
  }

  // 4. Assemble candidate maps with builder isolation (CoW)
  const nextReservations = new Map(this._core.activeReservations);
  nextReservations.delete(reservationId); // Linearized reservation consumption!

  const nextTransactions = new Map(this._core.committedTransactions);
  const txRecord: QuarantineRetirementTransactionRecord = Object.freeze({
    transactionId: retirementTransactionId,
    outcome: 'COMMITTED',
    entryCount: stagedBatch.length,
    totalSerializedBytes: batchBytes,
    batchDigest,
    committedAtMs: Date.now(),
  });
  nextTransactions.set(retirementTransactionId, txRecord);

  // 5. Construct candidate root and publish atomically
  // The outer root object and entries array are shallow-frozen via Object.freeze().
  // Maps are private encapsulated structures owned exclusively by the root (R17-A).
  const candidateCore: TerminalArchiveCoreState = Object.freeze({
    entries: Object.freeze([...this._core.entries, ...stagedBatch]),
    committedTransactions: nextTransactions, // Private encapsulated Map
    activeReservations: nextReservations,       // Private encapsulated Map
    totalBytes: this._core.totalBytes + batchBytes,
    size: this._core.size + stagedBatch.length,
    archiveGeneration: this._core.archiveGeneration + 1,
  });

  // LINEARIZATION POINT: Single synchronous atomic reference swap
  this._core = candidateCore;

  return {
    committed: true,
    alreadyCommitted: false,
    transactionId: retirementTransactionId,
    entryCount: stagedBatch.length,
    totalSerializedBytes: batchBytes,
    archiveGeneration: candidateCore.archiveGeneration,
    batchDigest,
  };
}
```

*Option A Correctness Guarantees:*
- **Zero Inconsistent Observation:** Because `entries`, `committedTransactions`, and `activeReservations` are updated in the single root pointer replacement, no caller can ever witness a committed transaction whose reservation is still active, or an uncommitted transaction whose reservation appears consumed.
- **Atomic Failure Unwind:** If candidate construction or validation throws, `this._core` is completely untouched. The reservation remains active and unconsumed, entries are uncommitted, and transaction count is un-incremented.
- **Uncertain Commit Reconciliation:** An uncertain transaction outcome is queried via `this._core.committedTransactions.get(txId)`. If absent, the reservation remains intact in `this._core.activeReservations` and can be cancelled via `cancelReservation(reservationId, capability)` or retried cleanly.

##### QuarantineBuffer and QuarantineDrainer Ownership Reconciliation (R16-C)
An audit of accepted source code (`packages/sync-engine/src/recovery/quarantine-buffer.ts:243-271` and `packages/sync-engine/src/recovery/quarantine-drainer.ts:119-215, 381-392`) establishes the exact physical custody transitions across the recovery lifecycle:

1. **Physical Custody Separation:**
   - In-flight ownership is stored in `QuarantineDrainer._inFlightRecord` (`quarantine-drainer.ts:119`).
   - When `quarantineDrainer.drainBatch()` calls `op = this.quarantineBuffer.shiftDrain()` (`quarantine-buffer.ts:248-251`), the operation is **physically dequeued from `QuarantineBuffer.drainBatch`**, `_drainBytes` is decremented, and the identity is deleted from `identityIndex`.
   - The operation is held **strictly and exclusively in `QuarantineDrainer._inFlightRecord`** ($N_{\text{in-flight}} = 1$). It is **NOT** present in `QuarantineBuffer` while in flight.
2. **Restoration Mechanics:**
   - When an in-flight operation is rejected with `CAPACITY_REJECTED` or fenced by attempt invalidation, `quarantineDrainer.restoreInFlightToQueue()` calls `quarantineBuffer.unshiftDrain(op)` (`quarantine-buffer.ts:268-271`).
   - `unshiftDrain()` prepends the operation back to `QuarantineBuffer.drainBatch`, restores byte accounting, re-indexes the identity, and sets `_inFlightRecord = null`.
3. **Mandatory Invariants for Atomic Retirement:**
   - **No Dual Mutable Custody:** An operation is owned either by `QuarantineBuffer` (queued) or by `QuarantineDrainer` (in flight), never simultaneously by both.
   - **Settlement Barrier Pre-Condition:** Retirement (Stage 3 discovery and Stage 11 execution) begins strictly **AFTER** Step 6 confirms `quarantineDrainer.inFlightRecord === null` ($N_{\text{in-flight}} == 0$) and `quarantineDrainer.isDraining === false`.
   - **No Stale Drainer Resurrection:** Stale drainer callbacks attempting destination admission are rejected with `ATTEMPT_INVALIDATED` (`STALE_ATTEMPT_FENCED`) via attempt-ID fencing. Stale callbacks cannot restore retired operations to `QuarantineBuffer`.
   - **Dual Zero-Custody Verification:** Upon completion of Stage 11 source retirement, the coordinator asserts BOTH:
     ```typescript
     if (this.quarantineBuffer.totalRetainedCount !== 0 || this.quarantineDrainer.inFlightRecord !== null) {
       throw new SyncEngineError('CUSTODY_LEAK_DETECTED', 'Zero-custody verification failed after retirement');
     }
     ```
   - **Watchdog Timeout Fencing:** If Step 4 settlement barrier does not resolve within `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`, attempt is permanently latched to `TIMED_OUT`, clean reset aborts to `DEGRADED`, and clean-root installation is blocked.

##### Custody Lifecycle State Machine (R16-C)

```mermaid
stateDiagram-v2
    [*] --> ActiveBuffer: Arriving remote event
    ActiveBuffer --> DrainBatch: detachActiveGeneration()
    DrainBatch --> DrainerInFlight: shiftDrain() (N_in-flight = 1)

    state DrainerInFlight {
        [*] --> DestinationPending
        DestinationPending --> DestinationAccepted: Destination confirms acceptance
        DestinationPending --> CapacityRejected: Destination queue full
        DestinationPending --> AttemptFenced: Reset/Abort increments attemptId
    }

    DestinationAccepted --> [*]: inFlightRecord = null (Custody to Destination)

    CapacityRejected --> DrainBatch: restoreInFlightToQueue() (unshiftDrain)
    AttemptFenced --> DrainBatch: restoreInFlightToQueue() (unshiftDrain)

    DrainBatch --> RetirementStaged: Step 6 Settlement Barrier resolved (N_in-flight = 0)
    RetirementStaged --> TerminalArchive: commitRetirementBatch() (Option A candidate-root swap)
    TerminalArchive --> RetiredSource: retireCommittedBatch() (Option A buffer swap)
    RetiredSource --> ZeroCustody: Verify buffer.count == 0 && drainer.inFlight == null
    ZeroCustody --> CleanActive: installCleanBaseline() -> ACTIVE
```

##### Complete Transaction Failure Recovery Matrix (R14-C, R15-A–R15-G, R16-A–R16-D, R17-A–R17-C)

| Failure Scenario | Engine State | QuarantineBuffer Custody | TerminalQuarantineArchive Custody | Coordinator Transaction State | Recovery Action |
|:---|:---|:---|:---|:---|:---|
| **1. Reservation capacity insufficient ($> 500$ entries or $> 2$ MiB)** | Untouched | Retains operations ($N$) | Rejects reservation | `FAILED` (`CAPACITY_INSUFFICIENT`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); zero loss |
| **2. Oversized entry ($> 4096$ canonical bytes)** | Untouched | Retains operations ($N$) | Rejects batch staging | `FAILED` (`ENTRY_SIZE_EXCEEDED`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); entry never truncated |
| **3. Generation mismatch at reservation commit** | Untouched | Retains operations ($N$) | Rejects commit | `FAILED` (`GENERATION_MISMATCH`) | Cancels reservation; re-reads generation; re-stages batch |
| **4. Exception during candidate root construction** | Untouched | Retains operations ($N$) | Old root remains authoritative; zero entries added; reservation unconsumed | `PREPARING` / `FAILED` | Old archive root intact; reservation preserved for retry or cancellation |
| **5. Archive commit succeeds but source retirement throws (Option A validation failure)** | Untouched | Retains operations ($N$) | Batch committed under `retirementTxId`; reservation consumed | `COMMITTED_SOURCE_RETIREMENT_PENDING` | Source buffer untouched; coordinator queries `getTransaction()`; skips archive commit; retries Stage 11 source retirement |
| **6. Duplicate transaction submission** | Untouched | Intact | Returns existing receipt via Step 1 lookup | `COMPLETED` | Idempotent; zero duplicate entries; returns success receipt |
| **7. Transaction ID reused with different digest** | Untouched | Retains operations ($N$) | Rejects with conflict | `FAILED` (`TRANSACTION_ID_DIGEST_MISMATCH`) | Throws error; preserves archive and buffer; prevents corruption |
| **8. Uncertain commit outcome (timeout/exception)** | Untouched | Retains operations ($N$) | Query via `getTransaction()` | `COMMIT_OUTCOME_UNKNOWN` | 3-way query: if committed $\to$ retires source; if absent $\to$ cancels reservation; if fails $\to$ remains unknown |
| **9. Attempt invalidation during preparation** | Untouched | Retains operations ($N$) | Reservation cancelled | `FAILED` (`ATTEMPT_FENCED`) | Stale continuation aborts immediately; zero mutations |
| **10. Conflicting composite identity in batch** | Untouched | Retains operations ($N$) | Untouched | `FAILED` (`RETIREMENT_BATCH_CONFLICT`) | Rejects batch fail-closed; preserves buffer; halts reset |
| **11. Settlement watchdog timeout during Step 4** | Untouched | Retains operations ($N$) | Untouched | `TIMED_OUT` | Attempt latched to `TIMED_OUT`; clean reset aborts to `DEGRADED`; zero corruption |
| **12. Dual zero-custody verification failure** | Untouched | Detected leak | Committed | `FAILED` (`CUSTODY_LEAK_DETECTED`) | Aborts clean-root install; enters `DEGRADED`; prevents uncommitted operation loss |

**Hard Invariant:** Clean-root installation is strictly forbidden until every operation in the retirement set has unambiguous terminal custody and dual zero-custody verification confirms zero remaining operations in buffer and drainer.

#### 6. R15-F — Export Lease, Generation-Independent Acknowledgement & Tombstone Governance (R16-A, R17-A Aligned)

##### Dedicated Capability Contract
Archive administration requires `TerminalArchiveAdministrationCapability` (`TERMINAL_ARCHIVE_BRAND`). Possession of `RecoveryInstallationCapability` does NOT grant archive export authority.

##### Single-Lease Concurrency Governance (`MAX_OUTSTANDING_EXPORTS = 1`)
- `TerminalQuarantineArchive` enforces at most **one** active export lease at any time.
- Calling `prepareArchiveExport()` while an unacknowledged lease exists is rejected with `SyncEngineError('ARCHIVE_EXPORT_IN_PROGRESS')`.

##### Generation-Independent Export Acknowledgement (R15-F, R16-A, R17-A)
When an export batch is prepared at archive generation $G$, it captures an immutable snapshot of entries and assigns an `ArchiveExportLease`.
If subsequent retirement transactions commit to the archive before export acknowledgement, the archive's internal generation advances to $G' > G$.

**Inviolable Invariant:** Export acknowledgement is validated against the **immutable export lease record** (`lease.exportId`, `lease.batchDigest`, `lease.leaseExpiryMs`), **NOT** blindly against the archive's current generation `this._core.archiveGeneration`!
- Acknowledgement succeeds if:
  1. The export lease exists and has not expired: `currentTime <= lease.leaseExpiryMs`.
  2. The acknowledgement matches `lease.exportId` and `lease.batchDigest`.
  3. The exact entries captured in the export lease are still present in `this._core.entries`.
- **Surgical Entry Eviction:** On valid acknowledgement, the archive constructs a candidate root that removes **strictly and exclusively the captured entries**. Any entries appended at $G' > G$ remain completely untouched.
- Archive generation advances to $G'' = G' + 1$ upon acknowledgement.

##### Deterministic Race Between Acknowledgement and Lease Expiry (R15-F)
Under single-threaded JavaScript execution, either the lease expiration callback executes first, or the acknowledgement call executes first:
- **Case 1 (Expiration Executes First):** The active lease is marked expired. A subsequent `acknowledgeArchiveExport()` call detects `now > lease.leaseExpiryMs`, clears zero entries, and throws `SyncEngineError('ARCHIVE_EXPORT_EXPIRED')`. **An expired lease NEVER clears archive entries.**
- **Case 2 (Acknowledgement Executes First):** The lease is verified active, entries are purged via candidate root swap, `activeExportLease` is set to `null`, and a tombstone is recorded. When the later expiration timer callback runs, it observes `activeExportLease === null` and safely no-ops.

##### Acknowledgement Tombstones & Deterministic Eviction
- When an export is acknowledged, a tombstone record is recorded:
  ```typescript
  export interface ArchiveExportTombstone {
    readonly exportId: string;
    readonly archiveGeneration: number;
    readonly entryIdsDigest: string;
    readonly acknowledgedAtMs: number;
    readonly clearedCount: number;
  }
  ```
- **Bounded Tombstone Capacity:** `MAX_ACKNOWLEDGEMENT_TOMBSTONES = 50`.
- **Tombstone Lifetime:** `TOMBSTONE_TTL_MS = 600000` (10 minutes).
- **Deterministic Eviction Policy:** Maintained as a bounded FIFO ring buffer. When tombstone count reaches 50 or records exceed 10 minutes TTL, the oldest tombstone is evicted.
- **Inviolable Invariant:** Tombstone eviction deletes **ONLY tombstone metadata**. It **NEVER** deletes archive entries!
- **Repeated Acknowledgement Semantics:**
  - Repeated acknowledgement while tombstone exists returns idempotent receipt: `{ acknowledged: true, alreadyAcknowledged: true, clearedCount: 0 }`.
  - Repeated acknowledgement after tombstone eviction returns `SyncEngineError('ARCHIVE_EXPORT_UNKNOWN')` or `SyncEngineError('ARCHIVE_EXPORT_EXPIRED')`.
- **Preservation Boundary:** Protocol guarantees in-memory custody until explicit acknowledgement. External durability remains the responsibility of the administrative caller.


#### 7. Reset Continuation, Timeout Fencing & Ownership Consistency (R14-E, R15-B)
- **Latched Timeout Preservation:** When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expires, `attempt.status` is permanently latched to `TIMED_OUT`. The in-flight slot remains owned ($N_{\text{in-flight}} = 1$); the timeout does not cancel the underlying Promise and does not clear the slot.
- **Late Settlement Halts:** When the slow operation settles later, continuation observes `TIMED_OUT`, resolves custody to buffer head, releases the slot ($N_{\text{in-flight}} \to 0$), and strictly terminates with zero lifecycle continuation.
- **New Attempt Independent Verification:** A new attempt must independently verify: (1) old async work settled, (2) old custody resolved, (3) old slot empty, (4) old continuations fenced, (5) new attempt current, authorized, and non-terminal.
- **Clean Root Gating:** Under `REINITIALIZE_CLEAN`, clean-root installation remains strictly forbidden until the retirement transaction reaches `COMPLETED` and zero-custody verification succeeds.
- **Preservation of M4.3c Notification Sequence:**
  Quarantine draining/retirement $\to$ Bookkeeping $\to$ `setRecoveryMode(false)` $\to$ Coordinator to `ACTIVE` $\to$ `flushDeferredNotifications()`.

#### 8. Enforceable Memory Bounds & Heap Limitations (R13-D, R14-A, R15-G)

##### Explicit Hard Bound Constants
| Metric | Hard Bound Constant | Value | Description |
|---|---|---|---|
| **Maximum Archive Entries** | `MAX_TERMINAL_ARCHIVE_ENTRIES` | 500 entries | Hard ceiling on retained terminal entries |
| **Maximum Aggregate Serialized Bytes** | `MAX_TERMINAL_ARCHIVE_TOTAL_BYTES` | 2,097,152 bytes (2.0 MiB) | Enforceable canonical wire payload ceiling |
| **Maximum Serialized Entry Bytes** | `MAX_TERMINAL_ENTRY_BYTES` | 4,096 bytes (4.0 KiB) | Maximum serialized byte length per individual entry |
| **Maximum Error String Length** | `MAX_TERMINAL_ERROR_LENGTH` | 512 characters | Diagnostic error text length ceiling |
| **Maximum Reason String Length**| `MAX_TERMINAL_REASON_LENGTH` | 256 characters | Rejection reason description ceiling |
| **Maximum Peer Identifier Length**| `MAX_TERMINAL_PEER_ID_LENGTH` | 128 characters | Printable ASCII peer domain identifier limit |
| **Maximum Event Identifier Length**| `MAX_TERMINAL_LOGICAL_EVENT_ID_LENGTH`| 128 characters | Logical event identifier limit |
| **Maximum Outstanding Exports** | `MAX_OUTSTANDING_EXPORTS` | 1 export lease | Single active export lease ceiling |
| **Maximum Export Batch Wire Bytes** | `MAX_EXPORT_BATCH_TOTAL_BYTES` | 2,097,152 bytes (2.0 MiB) | Separate explicit memory budget for export snapshot |
| **Maximum Acknowledgement Tombstones** | `MAX_ACKNOWLEDGEMENT_TOMBSTONES` | 50 records | Bounded FIFO ring buffer ceiling for tombstones |
| **Tombstone Lifetime TTL** | `TOMBSTONE_TTL_MS` | 600,000 ms (10 min) | Time-to-live for export acknowledgement tombstones |
| **Reservation Lease Timeout** | `RESERVATION_LEASE_TIMEOUT_MS` | 10,000 ms (10 s) | Active capacity reservation lease duration |
| **Export Lease Timeout** | `EXPORT_LEASE_TIMEOUT_MS` | 30,000 ms (30 s) | Active export lease duration before expiry |

##### Enforceable Serialized-Data Calculation vs. V8 Heap Reality
1. **Enforceable Serialized Payload Bound:**
   $$\text{Max Serialized Wire Bytes} = \min(500 \times 4096, 2097152) = 2,048,000 \text{ bytes} \le 2.0 \text{ MiB}$$
2. **V8 JavaScript Object Heap Reality Disclaimer:**
   In garbage-collected runtimes like Node.js and V8, JavaScript object heap overhead cannot be deterministically enforced by application code due to hidden classes, string internalization, 32-bit pointer compression, generational GC cycles, and heap fragmentation.
   - The protocol **strictly enforces hard bounds on serialized wire payloads and string lengths**.
   - The previously estimated $\approx 2.56\text{ MiB}$ heap footprint is an **empirical approximation**, NOT an enforceable theoretical guarantee. The protocol explicitly disclaims any theoretical guarantee of portable V8 heap memory ceilings.
3. **Canonical Serialization & UTF-8 Byte Counting:**
   - **Serialization Standard:** RFC 8785 JSON Canonicalization Scheme (JCS). Deterministic key sorting, standard number formatting, UTF-8 output.
   - **Byte Measurement:** Wire bytes are measured using `Buffer.byteLength(canonicalJson, 'utf8')` (Node.js) or `new TextEncoder().encode(canonicalJson).length` (browser/universal).
   - **Unicode & Multi-Byte Handling:** String fields must satisfy character length limits AND serialized entry must satisfy `byteLength <= 4096`. Multi-byte characters (2–4 bytes) cannot bypass the 4096 byte ceiling.
4. **Detached Representations & Peak Memory Budget:**
   - The archive retains detached, deeply frozen canonical records.
   - Detached export batches hold independent immutable snapshots.
   - With `MAX_OUTSTANDING_EXPORTS = 1` and `MAX_EXPORT_BATCH_TOTAL_BYTES = 2.0 MiB`, peak serialized payload is strictly bounded to:
     $$\text{Peak Wire Bytes} = 2.0 \text{ MiB (Archive)} + 2.0 \text{ MiB (Export Snapshot)} = 4.0 \text{ MiB}$$
5. **Fail-Closed Overflow Behavior:**
   - Any entry exceeding 4096 serialized bytes or string length bounds is rejected fail-closed (`TERMINAL_ENTRY_OVERSIZED`). Identity fields are **NEVER** truncated to make an entry fit.
   - Capacity overflow breaches trigger fail-closed transition to `DEGRADED` (`TERMINAL_ARCHIVE_OVERFLOW`). Silent eviction is strictly prohibited.

#### 9. Candidate Isolation, Deep Immutability & Capability-Gated Commit (R11-B)
`Object.seal()` prevents property addition/deletion on the container, but does NOT prevent mutation of nested objects (`operationLog`, `clockManager`, `pendingQueue`, `replicationVector`, `localCausalContext`). To guarantee absolute exception atomicity and prevent partial commits:
1. **Required Future M5.1 Invariant:** Candidate staging must enforce strict reference disjointness. Every mutable nested structure modified during staging must be newly allocated or copy-on-write cloned. Staging functions must NEVER mutate any object reachable from the current authoritative `this.core`.
2. **Side-Effect Free Staging (Phase 1):** Causal checks, Lamport validation, and SMR reduction dry-run execute purely against candidate instances. If staging fails or throws, active `this.core` remains completely pristine.
3. **Pre-Commit Fencing & CAS Verification:** Immediately before replacement, the commit asserts:
   - Attempt identity: `attemptId === this.coordinator.currentAttemptId` and `this.coordinator.state === 'RECONCILING'`.
   - Expected root verification: `this.core === expectedPriorRoot` (CAS-style reference check ensuring no concurrent task replaced the root during staging).
   - Capability authorization: `capability === this.authorizedRecoveryCapability`.
4. **Atomic Linearization Point (Phase 2):** Synchronous root pointer swap:
   ```typescript
   this.core = candidateCore;
   ```
   Commits all 12 core state fields simultaneously in 1 call frame. Zero fallible mutations may execute after this assignment as part of the logical commit.
5. **Post-Commit Encapsulation:** Candidate structures are encapsulated inside the engine boundary; no external SDK consumer retains writable references.

#### 10. Receipt-Outcome Ownership & Vector Matrix (R11-A)

| Receipt Outcome | Status Classification | Vector Advanced? | Destination Core Mutated? | Next Custody Holder | Drainer Next Action |
|---|---|---|---|---|---|
| `ACCEPTED_REDUCED` | Successful Admission | Yes | Yes (Log + Vector + State) | `EngineCoreState.operationLog` | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ACCEPTED_PENDING` | Missing Dependency | No | Yes (Pending Queue) | `EngineCoreState.pendingQueue` | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ALREADY_REPRESENTED` | Deduplicated (Frontier) | No | No (Covered by base) | None (Retired) | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ALREADY_OWNED_DUPLICATE`| Deduplicated (Log) | No | No (Present in log) | None (Retired) | Releases in-flight slot ($N_{\text{in-flight}} \to 0$); continues drain |
| `ATTEMPT_INVALIDATED` | Attempt Fenced (M5 New) | No | No (Zero mutation) | `QuarantineBuffer` (head via `unshiftDrain`) | Releases slot after acknowledged transfer; preserves op for next attempt |
| `CAPACITY_REJECTED` | Transient Backpressure | No | No (Zero mutation) | `QuarantineBuffer` (head via `unshiftDrain`) | Releases slot after acknowledged transfer; halts drain (`HALTED_CAPACITY`) |
| `TERMINALLY_REJECTED` | Permanent Validation Failure | No | No (Zero mutation) | `TerminalQuarantineArchive` | Releases slot after acknowledged archive; continues drain |

#### 11. Reset-Policy Quarantine Fate Matrix (R11-D, R12-A, R13-A, R14-A, R14-B, R15-C)

| Reset Strategy | In-Flight Un-linearized Operation Fate | QuarantineBuffer Pending Operations Fate | Already-Linearized Log Operations Fate | When Can Draining Resume? |
|---|---|---|---|---|
| **`PURGE_AND_RETRY`** | Receives `ATTEMPT_INVALIDATED`; restored to head of `QuarantineBuffer` via `unshiftDrain(op)` | Retained in buffer custody; re-evaluated against new recovery attempt | Retained in `operationLog`; deduplicated during retry (`ALREADY_REPRESENTED` / `ALREADY_OWNED_DUPLICATE`) | Only after settlement barrier resolves, slot is empty ($N_{\text{in-flight}} = 0$), and new attempt enters `RECONCILING` |
| **`REINITIALIZE_CLEAN`** | Settles at Step 5 prior to commit; restored to buffer; included in Step 7 atomic retirement transaction | All pending operations atomically committed to `TerminalQuarantineArchive` via Stage 11 transaction (`REINITIALIZE_CLEAN_PURGED`) | Entire engine root replaced with clean baseline anchor via `replaceCoreStateAtomic`; prior log discarded | Pre-reset quarantine is permanently retired; live operation ingestion begins fresh in `ACTIVE` state |

#### 12. Clean Notification Contract & Zero-Notification Clarification (R10-04, R11-D, R13-E)
In existing accepted source (`leaderless-sync-engine.ts:883-960`), neither `replaceCoreStateAtomic` nor `setRecoveryMode(false)` pushes to `deferredStateChanges`. Therefore, Step 10 may deliver **zero notifications** unless an explicit clean baseline state change notification is specifically enqueued by caller or implementation. No clean baseline notification is guaranteed to exist automatically.
- **Observer Boundary Invariant:** No notification is EVER delivered to external listeners before Step 9 reaches `ACTIVE` and recovery mode is `false`. If pre-ACTIVE steps fail, recovery mode is restored, deferred notifications are never flushed, and the engine halts in `DEGRADED`.

---

## 4. Single-Root State Inventory & Opaque Staged-Handle Contract (Closing F2, R2-C1, R2-M2)

Inspection of `packages/sync-engine/src/recovery/engine-core-state.ts` confirms that the engine's core state root encompasses **exactly 12 distinct authoritative fields**, exposed across 13 property keys on the sealed interface:

```typescript
export interface EngineCoreState {
  // ── 12 Authoritative Stored Fields ──────────────────────────────────────────
  state: PlaybackState;                        // 1. Primary active playback state (deeply frozen)
  baseSnapshot: PlaybackState;                 // 2. Compaction base anchor for speculative replay
  baseSnapshotFrontier: Record<string, number>;// 3. Highest compacted sequence numbers per origin
  operationLog: OrderedOperationLog;           // 4. Authoritative totally-ordered operation log
  clockManager: LogicalClockManager;           // 5. Authoritative Lamport logical clock manager
  pendingQueue: PendingDependencyQueue;        // 6. Missing-dependency queue for causal operations
  replicationVector: Record<string, number>;   // 7. Highest contiguous applied sequences per peer
  localCausalContext: Record<string, number>;  // 8. Observed causal horizon for local event generation
  localOriginSequence: number;                 // 9. Monotonic sequence counter for local events
  currentEpoch: number;                        // 10. Current membership configuration epoch
  lastAppliedEventId: string | undefined;      // 11. Identity of most recently applied operation
  stateVersion: number;                        // 12. Monotonic state version counter

  // ── Referential Alias (Property 13) ────────────────────────────────────────
  playbackState: PlaybackState;                // 13. Invariant alias: playbackState === state
}
```

### 4.1 Authoritative Mapping Against Accepted Baseline (R2-C1)
- **12 Stored Properties:** `state`, `baseSnapshot`, `baseSnapshotFrontier`, `operationLog`, `clockManager`, `pendingQueue`, `replicationVector`, `localCausalContext`, `localOriginSequence`, `currentEpoch`, `lastAppliedEventId`, `stateVersion`.
- **1 Referential Alias:** `playbackState === state`. `createEngineCoreState(params)` enforces `core.playbackState = core.state` and seals the root container via `Object.seal()`.
- **Encapsulation & Readers/Writers:** All engine property getters and setters delegate exclusively to `this.core.*`. Zero duplicate state exists outside `this.core`.
- **Omitted Non-Core Fields:** Infrastructure fields (`sessionId`, `localPeerId`, `clock`, `conflictResolver`, `logCapacity`, `eventListeners`, `disposed`) and ephemeral recovery state (`recoveryActive`, `quarantineDrainer`) are excluded from `EngineCoreState` as they are immutable configuration or connection lifecycle variables.

### 4.2 Preservation of M4.3b Opaque Staged-Handle Architecture (R2-C1)
To ensure memory safety and prevent live reference leaking:
1. `SnapshotInstaller.stage()` constructs candidate collections and candidate `EngineCoreState` completely out-of-band.
2. The candidate `EngineCoreState` is stored in a module-private `WeakMap<StagedSnapshotInstallation, EngineCoreState>()` owned by `SnapshotInstaller`.
3. The method returns ONLY the immutable, opaque `StagedSnapshotInstallation` handle (`handleId`, `sessionId`, `targetEpoch`, `targetFrontier`, `canonicalDigest`, `maxLogicalTimestamp`).
4. External callers, including `RecoveryCoordinator`, never receive, hold, or inspect the mutable candidate `EngineCoreState`.
5. In Stage 3, `commitAtomic(engine, stagedHandle, capability)` retrieves the candidate from private storage and executes capability-gated replacement. Ordinary consumers cannot bypass this contract.

### 4.3 Precise Atomicity Semantics (R2-M2)
Snapshot state replacement executes via `replaceCoreStateAtomic(candidateCore, capability)` as **one synchronous root-reference replacement (`this.core = candidateCore;`) under JavaScript run-to-completion semantics, with no intermediate root visible to other JavaScript tasks**.
Under JavaScript's single-threaded event loop, run-to-completion guarantees that no microtask, macrotask, or callback can interleave during this reference assignment.

---

## 5. Supplier Selection, Lattice Ordering & Safe Incomparable Recovery (Closing F4, R2-C3, R2-C4, R2-H1, R3-02, R3-05)

### 5.1 Protocol-Stable Peer-ID Domain & Ordinal ASCII Ordering (R2-H1, R3-05)
1. **Character Domain:** All peer IDs across SyncPlay are strictly validated against `ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/` (`packages/protocol/src/constants.ts:87`), with lengths bounded between 1 and 64 or 128 characters.
2. **Ordinal ASCII Comparison:** Because peer IDs are strictly constrained to this printable ASCII subset, peer ID comparison is defined as **ordinal ASCII code point comparison**:
```typescript
export function comparePeerIds(a: string, b: string): number {
  return a < b ? -1 : (a > b ? 1 : 0);
}
```
`String.prototype.localeCompare()` is strictly prohibited across all protocol and selection logic.

### 5.2 Safe Incomparable-Compatible Recovery, Canonical Ordering Feasibility & Deterministic Replay Contract (Closing R2-C3, R3-02, R4-02, R5-01, R5-02)
Vector frontiers form a product lattice ($\mathbb{N}^k$). In a leaderless system, incomparable frontiers ($V_A \parallel V_B$) naturally arise from concurrent progress across independent peers.

**Fundamental Safety Invariant:** A snapshot must NEVER replace active state if doing so would discard compatible local operations! If the local node has operations that the candidate snapshot lacks ($S_{\text{local}} > S_{\text{snapshot}}$), blindly overwriting state with the snapshot would irreversibly wipe out local progress.

#### Normative Procedure for Incomparable Frontiers (R3-02, R4-02)
1. **Priority 1 — Anti-Entropy Log Repair (Delta Catch-Up):**
   If the missing operations from the remote candidate fall within the active log horizon ($\text{gap} \le \text{logCapacity}$), the engine **must not** perform snapshot replacement. Instead, it delegates to `RepairRequestPipeline` to fetch only the missing delta operations. When applied, the local vector advances without overwriting local progress.
2. **Priority 2 — Compatible History Reconciliation (Local Operation Stashing & Deterministic Replay):**
   If missing remote operations exceed the log horizon and snapshot recovery is strictly necessary:
   - The engine extracts all local operations whose sequence numbers are ahead of the candidate snapshot's frontier ($S_{\text{local}} > S_{\text{snapshot}}$) from `OrderedOperationLog`.
   - These local operations are stashed into a temporary reconciliation queue (`stashedLocalQueue`) prior to snapshot replacement.
   - The candidate snapshot is installed atomically via `replaceCoreStateAtomic()`.
   - Following installation, stashed local operations and quarantined remote operations are reconciled through the deterministic replay contract.
3. **Rigorous Evidence Requirement for History Compatibility (R3-02):**
   A matching frontier fingerprint alone is NOT proof of historical identity! To establish history compatibility across shared prefix ranges:
   - Full checkpoint hash chain verification (`compactedCheckpoints` SHA-256 chain), or
   - Bit-exact matching frontier timestamps (`originFrontierTimestamps`) across all shared origins, or
   - Verified operation digest chains across the shared prefix range are required.
   If common prefix history cannot be verified, the candidate is classified as `INSUFFICIENT_EVIDENCE` and cannot overwrite local state.
4. **Proven Conflict Forks:** If divergent operations are detected for the same origin and sequence ($H(op_A) \ne H(op_B)$), the histories are conflicting forks. Recovery aborts deterministically to `DEGRADED` (`FORK_DETECTED_INCOMPARABLE`).

#### The $A \prec B$ Compacted Snapshot Reordering Hazard & Canonical Ordering Feasibility (R5-02)
A central challenge in state machine replication over compacted snapshots is the potential for **ordering inversion**:

1. **The Ordering Hazard:**
   Suppose the local node committed an operation $A$ with ordering key $K(A) = \langle 10, \text{peerA}, \text{eventA} \rangle$.
   A remote peer committed a concurrent operation $B$ with ordering key $K(B) = \langle 11, \text{peerB}, \text{eventB} \rangle$.
   Under canonical total ordering (`compareOperations`), $K(A) \prec K(B)$, meaning canonical SMR requires evaluating $A \to B$.
   If the remote candidate snapshot has already compacted operation $B$ into its base playback state ($S_{\text{snap}}$), replaying $A$ on top of $S_{\text{snap}}$ evaluates the state transition sequence:
   $$S_{\text{snap}} \to A \implies B \to A$$
   Because SyncPlay's conflict resolution policies in `ConflictResolver` (`packages/sync-engine/src/conflict/conflict-resolver.ts`) are **not universally commutative** (e.g. `PAUSE` takes priority over concurrent `PLAY`; later timestamp `PLAY` resumes playback over earlier `PAUSE`; LWW applies on `SEEK` and `RATE_CHANGE`), applying $B \to A$ instead of $A \to B$ yields an incorrect, divergent playback state.
   The total-order comparator `compareOperations` cannot retroactively interleave $A$ before operations that were already irreversibly reduced into the compacted snapshot base!

2. **Canonical Ordering Feasibility Check:**
   To guarantee deterministic convergence, the engine must verify that replaying stashed local operations preserves canonical SMR total ordering:
   - In `StateSnapshotPayloadV2`, the compacted base state is bounded by `originFrontierTimestamps: Record<string, number>` and `maxLogicalTimestamp: number`.
   - Prior to snapshot installation, the engine evaluates each stashed local operation $A$:
     - If $L(A) \le \text{snapshot.maxLogicalTimestamp}$ and $A$ is concurrent with operations folded into the snapshot base ($\exists p \in \text{compacted origins},\ L(A) \le \text{snapshot.originFrontierTimestamps}[p]$), an **Explicit Ordering Hazard** exists: at least one operation compacted into the snapshot base canonically succeeds $A$.
   - **Hazard Resolution:**
     - **Priority 1 (Delta Log Repair):** The engine rejects snapshot replacement and initiates anti-entropy log repair (`RepairRequestPipeline`) from a common ancestor so that all uncompacted operations from all peers can be placed into `OrderedOperationLog` and ordered canonically by `compareOperations` before compaction.
     - **Priority 2 (Explicit Incompatibility Gate):** If anti-entropy log repair is not possible (history has been truncated across all peers and cannot be fetched), the engine **must not** apply the snapshot and replay $A$ out of order. Recovery detects that canonical SMR order cannot be guaranteed and **aborts safely to `DEGRADED`** with error code `INCOMPATIBLE_COMPACTED_HISTORY` (`SyncEngineError('INCOMPATIBLE_COMPACTED_HISTORY')`).
   - **Replay-Safe Conditions & Precise Semantics:**
     - **Conservative Sufficient Condition:**
       $$L(A) > \text{snapshot.maxLogicalTimestamp}$$
       provided `snapshot.maxLogicalTimestamp` genuinely represents the maximum Lamport timestamp across the entire compacted history.
     - **Per-Origin Frontier Condition:**
       $$\forall p \in \text{compacted origins},\ L(A) > \text{snapshot.originFrontierTimestamps}[p]$$
       with the non-negotiable guarantees that:
       1. Every compacted origin is explicitly represented in `originFrontierTimestamps`.
       2. Each origin's frontier timestamp is the exact Lamport timestamp of its highest compacted sequence (not an approximation or an unrelated later checkpoint).
     - **Critical Qualification on Causal Dependencies:** The canonical ordering feasibility check proves that $A$ sorts strictly after all compacted operations in the canonical total order. It does NOT independently prove that all causal dependencies of $A$ are satisfied. Causal dependencies must still be validated by the engine's causal admission pipeline upon replay.

#### Capability-Gated Replay Trust Contexts: Authority vs. Provenance (Closing R5-01, R6-01, R6-03, R7-03)
A typed string parameter (e.g. `'LOCAL_REPLAY'`) is merely a declaration of context, not proof of provenance: any caller can supply a string. To eliminate accidental trust escalation where untrusted remote operations bypass arrival-rate limits and clock-inflation checks, post-snapshot reconciliation enforces capability-gated admission:

```typescript
admitDrainedOperation(
  op: QuarantinedOperation,
  capability: DrainAdmissionContext,
  attemptId: number,
): DestinationAcceptanceReceipt;
```

#### 1. Inviolable Distinction: Capability Authority vs. Operation Provenance (R6-03, R7-03)
A fundamental distributed-systems principle governs post-snapshot admission: **Capability possession establishes caller authority to request an admission mode, but does NOT independently validate operation data provenance.**

To enforce this distinction and prevent payload forgery:
- **`stashedLocalQueue` Provenance Exclusivity:** Operations admitted under `TrustedLocalReplayCapability` must originate **strictly and exclusively** from the private `stashedLocalQueue`.
- **Immutable Log Extraction Only:** `stashedLocalQueue` is populated strictly by extracting previously committed, immutable local operations from `OrderedOperationLog` where `originPeerId === this.localPeerId` and `originSequenceNumber > baseSnapshotFrontier[this.localPeerId]`.
- **Prohibition of Remote Injection or Relabeling:**
  - Remote operations can **NEVER** be inserted into `stashedLocalQueue`.
  - Remote operations can **NEVER** be relabeled or masqueraded as local operations through metadata, headers, or property mutations.
  - Capability possession alone is never treated as proof that arbitrary operation data has valid provenance.
- **Identity Hash & Causal Context Verification:** Even when presented with `TrustedLocalReplayCapability`, each replayed operation is verified against committed history:
  - Identity hash verification: $H(op) == H(log)$. The operation's `logicalEventId`, `originPeerId`, `originSequenceNumber`, `logicalTimestamp`, `causalContext`, and `payload` must match the committed log record.
  - Causal context check: Causal dependencies must be satisfied by the snapshot base checkpoints or preceding replayed operations.
  - Any identity conflict or unsatisfied dependency proves history divergence; recovery **immediately aborts to `DEGRADED`**. Local operations are never dropped.

#### 2. Subsystem Custody & Trust Boundaries
1. **Unforgeable In-Process Custody:**
   - **Subsystem Custody:** Trust context is determined exclusively by the authorized capability held by the calling subsystem:
     - Draining `stashedLocalQueue` presents `this.authorizedLocalReplayCapability` (`TrustedLocalReplayCapability`).
     - Draining `quarantineBuffer` presents `this.repairBatchCapability` (`TrustedRepairBatchCapability`).
   - `QuarantineDrainer` is constructed with and holds strictly `TrustedRepairBatchCapability`. It has **zero access** to `TrustedLocalReplayCapability`. Remote quarantine draining can never obtain or present local replay trust.
   - **No Metadata Inference:** Trust context is **NEVER read, inferred, or accepted from:**
     - Network wire frames, payload properties, or message headers.
     - Mutable operation object fields.
     - User input or peer-supplied metadata.
   - Any invocation with an invalid, forged, or mismatched capability throws `SyncEngineError('UNAUTHORIZED')` immediately.

2. **Locally Committed Stashed Operations (`TrustedLocalReplayCapability`):**
   - **Nature:** Operations that were already validated, accepted, and committed in the local `OrderedOperationLog` prior to snapshot installation.
   - **Trust Boundary:** Trusted internal replay. Cannot introduce new origin peer IDs, new sequence numbers, or altered payloads.
   - **Validation Semantics:** Evaluated upon presenting `TrustedLocalReplayCapability` (verified via `capability === this.authorizedLocalReplayCapability`). Exempt from token-bucket arrival rate limits and remote clock inflation checks (`LogicalClockManager.validateAndAdvanceRemote` with `context === 'LOCAL_REPLAY'` returns `this.currentClock` without mutating Lamport clocks).
   - **Admission Evaluation:** Evaluated on top of the newly installed snapshot base. Their causal dependencies must be satisfied by the snapshot base checkpoints or earlier replayed operations.
   - **Fork / Failure Action:** If a locally committed stashed operation encounters a sequence collision, identity conflict ($H(op) \ne H(log)$), or missing dependency that cannot be resolved, this proves an irreconcilable history divergence between local committed state and the snapshot. Recovery **must immediately abort to `DEGRADED`**. Locally committed progress is NEVER silently dropped.

3. **Remotely Quarantined Operations (`TrustedRepairBatchCapability`):**
   - **Nature:** Remote operations that arrived over the network while recovery was in progress (`RECOVERING` or `RECONCILING`). They were buffered in `QuarantineBuffer` and have never been admitted to the local log.
   - **Trust Boundary:** Untrusted remote historical/repair operations.
   - **Validation Semantics:** Evaluated upon presenting `TrustedRepairBatchCapability` (verified via `capability === this.authorizedRepairBatchCapability`). They bypass wall-clock token-bucket arrival rate limits, but must pass complete structural validation: epoch fencing, per-origin Lamport monotonicity, dependency resolution across snapshot checkpoints and log, and collision checks.
   - **Admission Evaluation:** Admitted into `OrderedOperationLog` only upon satisfying all causal dependencies (`ACCEPTED_REDUCED`) or buffered in `PendingDependencyQueue` (`ACCEPTED_PENDING`).

#### Deterministic Replay Decision Contract & Total Ordering (R4-02, R5-02)
Post-snapshot operation evaluation operates strictly under **Deterministic Total-Order State Machine Replication (SMR)** with operation-specific conflict resolution (not generic CRDT or OT).

The deterministic total ordering key is:
$$\text{OperationOrderingKey} = \langle \text{logicalTimestamp}, \text{originPeerId}, \text{logicalEventId} \rangle$$
enforced by `compareOperations(e1, e2)` (`packages/sync-engine/src/ordering/comparator.ts`):
1. Primary order: Lamport `logicalTimestamp` (ascending numerical).
2. Secondary order: Deterministic ordinal ASCII `originPeerId` tie-breaker.
3. Tertiary order: Stable `logicalEventId` tie-breaker.

Reducer entry points:
- **Incremental Admission:** `ConflictResolver.reduce(currentState, operation, monotonicNow, context, priorOperations)` (`packages/sync-engine/src/conflict/conflict-resolver.ts:40`).
- **Base Snapshot Replay:** `ConflictResolver.replay(baseState, operations, monotonicNow, context)` (`packages/sync-engine/src/conflict/conflict-resolver.ts:403`).

Operations are deterministically classified into six exhaustive categories:

| Category | Description & Condition | Engine Admission Check | Typed Receipt | Drain Action | Invariants Preserved |
|---|---|---|---|---|---|
| **Cat 1: Already Represented** | Covered by newly installed snapshot base (`originSeq <= baseSnapshotFrontier[originPeerId]`). | `baseFrontier` check | `ALREADY_REPRESENTED` | **Discarded.** Safe retirement; in-flight custody released. | Never applied twice; idempotent retirement. |
| **Cat 2: Not Represented (Satisfied)** | Ahead of snapshot base (`originSeq > baseFrontier`), causal dependencies satisfied (`originSeq === haveSeq + 1` and all $reqSeq \le repVec$). | Causal context check | `ACCEPTED_REDUCED` | **Retained & Reduced.** Admitted into `operationLog`, reduced into state, vector advanced. | Zero operation loss; deterministic reduction on snapshot base. |
| **Cat 3: Concurrent with Frontier** | Concurrent with snapshot frontier ($V_{\text{op}} \parallel V_{\text{snapshot}}$), causal dependencies satisfied by snapshot, passes canonical ordering feasibility check ($L(op) > \text{frontierTimestamp}$). | Total-order SMR evaluation (`compareOperations`) | `ACCEPTED_REDUCED` | **Retained & Reduced.** Deterministic reduction via `ConflictResolver.reduce()` / `replay()`. | Total order tie-break on $\langle L, \text{peerId}, \text{eventId} \rangle$ strictly preserved. |
| **Cat 4: Missing Dependencies** | Causal dependencies not yet satisfied (`originSeq !== haveSeq + 1` or missing dependency in $repVec$). | Missing dependency check | `ACCEPTED_PENDING` (or `CAPACITY_REJECTED`) | **Retained in Queue.** Buffered in `PendingDependencyQueue`. Anti-entropy log repair requested. If queue full ($K=256$), drain halts immediately on `CAPACITY_REJECTED` $\to$ **Abort to `DEGRADED`**. | Never fabricate dependencies; in-flight custody preserved on queue overflow. |
| **Cat 5: Duplicate / Existing Ownership** | Already present in `replicationVector`, `operationLog`, or `pendingQueue`. | Duplicate index check | `ALREADY_OWNED_DUPLICATE` | **Discarded.** In-flight custody released without duplicate execution. | Exactly-once reduction guarantee; duplicate-safe. |
| **Cat 6: Validation / Reduction Failure** | Fails schema, epoch mismatch, identity conflict ($H(op) \ne H(log)$), or pre-horizon dependency breach. | Validation / clock checks | `TERMINALLY_REJECTED` or throws | **Action depends on trust context:**<br/>• Quarantined remote (`REPAIR_BATCH`): Discarded (`DISCARDED`).<br/>• **Locally committed stashed (`LOCAL_REPLAY`):** **Abort to `DEGRADED`**. | **Never silently discard committed local progress!** Abort recovery if local history cannot safely reduce. |

#### Eight Non-Negotiable Replay Invariants (R4-02, R5-01, R5-02)
1. **Never Silently Discard Committed Local Operations:** A locally committed operation that cannot be safely reduced onto the snapshot base must cause recovery to abort into `DEGRADED`. Silent dropping is strictly prohibited.
2. **Never Invert Canonical SMR Order on Compacted Snapshots:** Stashed local operations must satisfy the canonical ordering feasibility condition ($L(A) > \text{originFrontierTimestamps}[p]$). If an ordering hazard exists, prioritize delta log repair; if impossible, abort to `DEGRADED` with `INCOMPATIBLE_COMPACTED_HISTORY`.
3. **Never Apply an Operation Twice:** Re-admission strictly fences against `baseSnapshotFrontier`, `replicationVector`, and `operationLog`, preventing duplicate state mutations.
4. **Never Fabricate Causal Dependencies:** If causal dependencies are missing, the operation enters `PendingDependencyQueue` and triggers delta repair; dependencies are never synthesized or assumed.
5. **Preserve Original Identity & Sequence:** Stashed operations retain their immutable `logicalEventId`, `originPeerId`, `originSequenceNumber`, `logicalTimestamp`, `causalContext`, and `payload`. Re-numbering or rewriting event identity is strictly forbidden.
6. **Preserve Capability-Gated Trust Distinction:** Replay trust is derived strictly from unforgeable capability custody (`DrainAdmissionContext`). `TrustedLocalReplayCapability` applies strictly to already-accepted local operations re-anchoring on the new base state. `TrustedRepairBatchCapability` applies to unadmitted network arrivals. Trust context is never inferred from wire messages, headers, or mutable operation fields. Any invalid, forged, or mismatched capability fails closed with `SyncEngineError('UNAUTHORIZED')`.
7. **Reuse Accepted M4.3c Single-Owner Drain:** Quarantined and stashed operations are processed with single-in-flight custody ($N_{\text{in-flight}} \le 1$) via `QuarantineDrainer`.
8. **Abort When Safe Replay Is Unprovable:** If historical compatibility cannot be proven, or if stashed operations cannot reduce cleanly, snapshot recovery must abort to `DEGRADED` rather than risking divergence or data loss.

#### Snapshot Installation Atomicity vs. Replay Completion & Degraded State (R4-02, R5-03)
- **Installation Atomicity:** Snapshot installation (`SnapshotInstaller.commitAtomic()`) is a **synchronous, indivisible, single-pointer swap** (`this.core = candidateCore;`) under JavaScript run-to-completion semantics. Intermediate states are impossible.
- **Irrevocable Base Replacement:** Once `replaceCoreStateAtomic(candidateCore, capability)` executes, the previous `EngineCoreState` is permanently superseded by the candidate `EngineCoreState`. The engine core now holds the newly installed base snapshot state.
- **Replay Completion:** Replay and quarantine drain are **asynchronous, multi-step batch operations** executed post-commit.
- **Post-Installation Failure Behavior:** If replay subsequently fails or halts (e.g. `CAPACITY_REJECTED`, validation failure on stashed local operation, canonical ordering hazard with history exhaustion, unhandled exception, or timeout):
  1. The engine does NOT roll back to the pre-installation state (the previous root is permanently unanchored). The engine remains anchored on the newly installed base snapshot with an incomplete replay log.
  2. `RecoveryCoordinator` transitions immediately to `state: 'DEGRADED'`, `substate: 'DRAIN_FAILED'`.
  3. `engine.setRecoveryMode(true)` is **maintained** (operational deferral remains locked; recovery mode is NOT cleared).
  4. `engine.flushDeferredNotifications()` is **NEVER called**. External listeners receive zero intermediate or corrupted notifications.
  5. The failing operation remains preserved in `QuarantineDrainer.inFlightRecord`.
  6. Un-drained operations remain in `QuarantineBuffer` custody.
  7. Public engine queries expose `isRecoveryInProgress() === false`, `isDegraded() === true`, and `recoveryState === 'DEGRADED'`. Under no circumstances can recovery report or transition to `'ACTIVE'`.
  8. Coordinator exposes diagnostics via `getMetrics()` and error events for operator inspection or automated session reset.

### 5.3 Candidate Eligibility Taxonomy

| Case | Relationship | Condition | Architectural Action |
|---|---|---|---|
| **Case 1** | `IDENTICAL` | $V_P = V_{\text{local}}$ and intermediate checkpoint history verified (or $k \le 1$). | Installation unnecessary. Candidate discarded. |
| **Case 2** | `FRONTIER_AGREEMENT_ONLY` | $V_P = V_{\text{local}}$ at $k > 1$, but intermediate history unverified. | Cannot overwrite local state without proof. Candidate discarded. |
| **Case 3** | `STRICTLY_DOMINATES` | $V_P \ge V_{\text{local}}$ on all origins, and $V_P > V_{\text{local}}$ on at least one origin. | **Eligible candidate.** Ranked by strict domination; tie-break by RTT, then `comparePeerIds`. |
| **Case 4** | `INCOMPARABLE_COMPATIBLE` | $V_P \parallel V_{\text{local}}$, common prefixes verified compatible ($H(op_A) == H(op_B)$). | Priority 1: log repair. Priority 2: local operation stashing before snapshot install. No local loss. |
| **Case 5** | `REPAIRABLE_WITHIN_HORIZON` | Missing operation gap $\le \text{logCapacity}$ (e.g. $\le 256$ ops). | Bypass snapshot transfer; delegate to `RepairRequestPipeline` anti-entropy log repair. |
| **Case 6** | `PROVEN_CONFLICT_FORK` | Common prefix history diverges for same origin and sequence ($H(op_A) \ne H(op_B)$). | **True causal fork.** Abort recovery deterministically to `DEGRADED` (`FORK_DETECTED_INCOMPARABLE`). |
| **Case 7** | `DOMINATED` / `INSUFFICIENT_EVIDENCE` | $V_P < V_{\text{local}}$, epoch is older, or evidence is outside repair horizon without verifiable ancestry. | Candidate rejected; retry discovery or degrade if no candidates remain. |

### 5.4 Cryptographic Digest & Trust Model Clarification (R2-C4)
1. **SHA-256 Byte Integrity:** Slice digests and snapshot canonical digests (RFC 8785) verify **byte-level transfer integrity** against network corruption and reassembly errors.
2. **Session & Epoch Binding:** Verified by inspecting header metadata (`sessionId`, `membershipEpoch`, `connectionGeneration`).
3. **Historical Consistency:** Verified by validating compacted checkpoint chains and frontier timestamps.
4. **Cooperative Non-Byzantine Trust Model:** SyncPlay operates under a **cooperative non-Byzantine trust model**. Cluster participants are trusted to be authorized and non-malicious. **SHA-256 digests do NOT provide digital signatures, non-repudiation, or cryptographic author authentication against an active Byzantine adversary.**

---

## 6. End-to-End Recovery Boundary & Drain Ownership (Closing F10, R2-C2, R2-H4, R2-H6, R3-06)

```mermaid
sequenceDiagram
    autonumber
    participant AEM as AntiEntropyManager
    participant RC as RecoveryCoordinator
    participant SIP as SnapshotInboundPipeline
    participant SI as SnapshotInstaller
    participant LSE as LeaderlessSyncEngine
    participant QD as QuarantineDrainer

    AEM->>RC: triggerRecovery(supplierPeerId, reason)
    RC->>RC: activeAttemptId++
    RC->>LSE: setRecoveryMode(true)
    RC->>SIP: startTransfer(supplierPeerId, activeAttemptId)

    loop Chunk Transfer & Retries
        SIP->>SIP: Receive chunks, verify SHA-256 slice digests
    end

    SIP->>SIP: finalizeAssembly() -> canonical RFC 8785 SHA-256 match
    Note over SIP,RC: Inbound Pipeline One-Shot Latch Guard
    SIP->>RC: onTransferComplete(result, attemptId)

    alt attemptId !== RC.currentAttemptId
        RC-->>SIP: Drop stale completion callback (Fenced!)
    else attemptId === RC.currentAttemptId
        RC->>SI: validate(result.snapshot)
        SI-->>RC: valid (HISTORY_VERIFIED)
        RC->>SI: stage(result.snapshot, engineConfig)
        SI-->>RC: stagedHandle (Opaque Handle)
        RC->>SI: commitAtomic(engine, stagedHandle, capability)
        SI->>LSE: replaceCoreStateAtomic(candidateCore, capability)
        Note over LSE: Single reference assignment under run-to-completion!

        RC->>QD: reconcileQuarantine(attemptId)
        loop Sequential Drain (N_in-flight <= 1)
            QD->>LSE: admitDrainedOperation(op)
            LSE-->>QD: DestinationAcceptanceReceipt
        end

        Note over RC,LSE: Accepted M4.3c Completion Ordering (R2-C2)
        RC->>RC: drainEndMs = nowFn() (Bookkeeping)
        RC->>LSE: setRecoveryMode(false)
        alt transition('ACTIVE') succeeds
            RC->>RC: stateMachine.transition('ACTIVE', ...)
            RC->>RC: clearTimers()
            RC->>LSE: flushDeferredNotifications()
            Note over LSE: Listeners observe engine.isRecoveryActive() === false and state === ACTIVE!
        else transition throws
            RC->>LSE: setRecoveryMode(true) (Rollback)
            Note over RC: Notifications NOT flushed on failed handoff!
        end
    end
```

### 6.1 Separate Timer Ownership & Lifecycle Fencing (R2-H4)
1. **Snapshot-Attempt Timers:** `overallTimer` (`overallRecoveryTimeoutMs`). Scoped to `activeAttemptId`. Cancelled on attempt success, retry, abort to `DEGRADED`, transition to `ACTIVE`, or disposal.
2. **Reconciliation Timers:** `reconciliationTimer` (`reconciliationTimeoutMs`). Scoped to post-install drain. Cancelled on drain completion, abort to `DEGRADED`, transition to `ACTIVE`, or disposal.
3. **Offer/Transfer Timers:** `offerAwaitTimer` (`offerAwaitTimeoutMs`). Scoped to offer discovery. Cancelled on offer selection, transfer initiation, abort to `DEGRADED`, or disposal.
4. **Future-Epoch Convergence Timers:** `futureEpochGraceTimer` (`futureEpochGracePeriodMs`, 5s). Owned by `FutureEpochBuffer`. **Crucial Invariant:** Scoped to epoch identity; **independent of snapshot attempts**. NOT cancelled when a snapshot attempt completes, resets, or retries. Cancelled ONLY when local epoch advances $\ge$ target epoch, on grace expiration, or upon disposal.

### 6.2 Bounded In-Flight Drain Ownership & Pending-Queue Capacity (R2-H6, R3-06)
- **Single In-Flight Rule:** Exactly $N_{\text{in-flight}} \le 1$ operation in flight between `QuarantineDrainer` and `LeaderlessSyncEngine`. No dequeue alone implies acceptance.
- **Destination Acceptance Receipts (R11-A, R11-C):**
  - `ACCEPTED_REDUCED`: Admitted to `operationLog`. Vector advanced. `inFlightRecord` cleared.
  - `ACCEPTED_PENDING`: Admitted to `PendingDependencyQueue`. Vector not advanced. `inFlightRecord` cleared.
  - `ALREADY_REPRESENTED`: Covered by base snapshot frontier. `inFlightRecord` cleared.
  - `ALREADY_OWNED_DUPLICATE`: Already present in log or pending queue. `inFlightRecord` cleared.
  - `ATTEMPT_INVALIDATED` [Proposed M5 Extension]: Rejected due to stale recovery attempt fencing. Vector not advanced. Transferred back to `QuarantineBuffer` head via `unshiftDrain(op)`; `inFlightRecord` cleared strictly after acknowledged custody transfer.
  - `TERMINALLY_REJECTED`: Fails validation, obsolete epoch, or malformed payload. Vector not advanced. Transferred to bounded `TerminalQuarantineArchive` (capped at 500 entries); `inFlightRecord` cleared strictly after acknowledged archive insertion. If archive full, fails closed to `DEGRADED`.
  - `CAPACITY_REJECTED`: Destination queue full. Vector not advanced. Transferred to `QuarantineBuffer` head via `unshiftDrain(op)`; `inFlightRecord` cleared strictly after acknowledged transfer. Drain halts immediately (`haltedOnCapacity = true`), and recovery transitions safely to `DEGRADED`.
- **Pending-Queue Bound Inspection & Identified Open Design Issue (R3-06):**
  - Accepted source inspection of `PendingDependencyQueue` (`packages/sync-engine/src/log/pending-dependency-queue.ts`) reveals a strict **count bound** (`capacity: number = 256`, set from `engineConfig.logCapacity`).
  - **Identified Open Design Issue:** `PendingDependencyQueue` enforces **NO aggregate byte limit** (unlike `QuarantineBuffer` which enforces both count and 4 MiB aggregate bytes). This missing byte bound on `PendingDependencyQueue` is formally documented as an open design issue for post-M5 hardening.

---

## 7. Authoritative Transition & Notification Sequence (Closing F7, R2-C2)

To preserve the accepted M4.3c listener-observation guarantee:
1. **Quarantine Drain Completes:** `quarantineBuffer.totalRetainedCount === 0`.
2. **Recovery Bookkeeping Completes:** `drainEndMs = nowFn()`, metrics compiled.
3. **Clear Engine Recovery Mode:** `this.engine.setRecoveryMode(false)`.
4. **Transition Coordinator to `ACTIVE`:** `this.stateMachine.transition('ACTIVE', 'Recovery completed', 'IDLE')`.
   - **Failure Rollback:** If clearing recovery mode or the state transition throws, the catch block immediately restores `this.engine.setRecoveryMode(true)`, aborts recovery, and re-throws. Staged completion notifications are **never flushed on a failed handoff**.
5. **Clear Timers & Dispatch Notifications:** Coordinator invokes `this.clearTimers()` and `this.engine.flushDeferredNotifications()` strictly **after** state is `'ACTIVE'` and engine recovery mode is cleared.
6. **Notification Guarantees:**
   - Any `onStateChange` listener observing `'ACTIVE'` is guaranteed to observe `engine.isRecoveryActive() === false`.
   - `stateChangeListeners`: Coalesced to exactly ONE notification with final reconciled `PlaybackState`.
   - `decisionListeners`: All decisions accumulated during drain are dispatched sequentially in FIFO order.

---

## 8. Measurable Active-State Performance Requirement (Closing F6, R2-M1, R3-08)

Invariant `INV-M5-01` is governed by a rigorous performance measurement contract:
- **Reference Target Environment (R3-08):** The $\le 5\,\mu\text{s}$ p99 execution overhead target applies strictly to **Node.js LTS on a dedicated reference CI runner / developer workstation (4 vCPU / 8 GiB RAM, V8 TurboFan tier-up)**.
- **Browser Runtime Characterization (R3-08):** In browser execution contexts, execution latency varies with hardware capabilities, mobile throttling, and UI thread contention; browser targets are characterized by non-blocking synchronous execution rather than fixed microsecond guarantees.
- **Event Shape & Size:** Canonical 512-byte `SyncOperationPayload` / `ControlSyncEvent`.
- **Warm-Up:** 10,000 warm-up iterations to allow JIT tier-up.
- **Benchmark Iterations:** 100,000 consecutive remote events evaluated in `ACTIVE` state.
- **Measurement Method:** High-resolution timing via `process.hrtime.bigint()`.
- **Queue Allocation Constraint (R3-08):** In the `ACTIVE` fast path, `interceptRemoteEvent()` performs synchronous state checks without allocating entries in `QuarantineBuffer` or `FutureEpochBuffer`, without pushing to internal arrays, and without retaining buffer wrapper heap objects. Short-lived result descriptors are subject to V8 escape analysis or young-generation reclamation.

---

## 9. Browser-Compatible Exact Byte Accounting Contract (Closing R2-H3, R3-01)

All recovery buffers enforce strict, verifiable memory bounds using browser-compatible byte accounting:

### 9.1 Explicit Buffer Boundaries & Verified Source Limits (Closing R2-H3, R3-01, R4-01)
The exact memory bounds of `QuarantineBuffer` and `FutureEpochBuffer` are verified directly from accepted source code (`packages/sync-engine/src/recovery/quarantine-buffer.ts:26-30` and `future-epoch-buffer.ts:20-24`):

| Buffer Component | Maximum Operation Count | Maximum Aggregate Bytes | Maximum Individual Event Size | Expiration Grace Period |
|---|:---:|:---:|:---:|:---:|
| **QuarantineBuffer** | **1,000 operations** (`MAX_QUARANTINE_OPERATIONS = 1000`) | **4 MiB** (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`) | **4 KiB** (`MAX_QUARANTINE_EVENT_BYTES = 4096`) | None (drained post-commit) |
| **FutureEpochBuffer** | **100 operations** (`DEFAULT_MAX_OPERATIONS = 100`) | **512 KiB** (`DEFAULT_MAX_TOTAL_BYTES = 524288`) | **4 KiB** (`DEFAULT_MAX_EVENT_BYTES = 4096`) | **5,000 ms** (autonomous timer) |

#### Exact Implemented QuarantineBuffer Contracts (R4-01):
1. **Maximum Retained Operation Count:** Strictly bounded to **1,000 operations** across both active generation queue and detached drain batch (`this.totalRetainedCount + 1 > this.maxOperations` throws `SyncEngineError('QUARANTINE_OVERFLOW')`).
2. **Maximum Aggregate Retained Wire Bytes:** Strictly bounded to **4 MiB** (4,194,304 bytes) across both active and drain storage (`this.totalRetainedBytes + op.serializedByteLength > this.maxTotalBytes` throws `SyncEngineError('QUARANTINE_OVERFLOW')`). Warning threshold triggers at 80% (3,355,443 bytes / ~3.2 MiB).
3. **Maximum Individual Operation Wire Bytes:** Strictly bounded to **4 KiB** (4,096 bytes) (`op.serializedByteLength > this.maxEventBytes` throws `SyncEngineError('REJECTED_INVALID')`).
4. **Configurability:** Configurable via constructor interface `QuarantineBufferConfig { maxOperations?: number; maxTotalBytes?: number; maxEventBytes?: number; }`. When omitted, default constants (`MAX_QUARANTINE_OPERATIONS = 1000`, `MAX_QUARANTINE_TOTAL_BYTES = 4 * 1024 * 1024`, `MAX_QUARANTINE_EVENT_BYTES = 4096`) are enforced. `RecoveryCoordinator` instantiates `this.quarantineBuffer = config.quarantineBuffer ?? new QuarantineBuffer()`, inheriting the standard 1,000 ops / 4 MiB / 4 KiB defaults.
5. **Zero Retention on Rejection:** When `enqueue()` throws (`REJECTED_INVALID` or `QUARANTINE_OVERFLOW`), the operation is NOT added to `activeQueue` or `identityIndex`, and `_activeBytes` is NOT incremented. Rejected operations retain **0 bytes** and **0 ownership** in the buffer.
6. **Discrepancy Reconciliation Note (R4-01):** The errant text reference to "64 MB" appearing in M5.0-R3 report sections 4.6 and 6 was an accidental drafting typo. The actual accepted implementation has always enforced 4 MiB (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`), matching the original M4.3a implementation and M5.0-R2 specifications. All documents are now 100% reconciled to the 4 MiB source baseline.
7. **Serialized Wire Weight vs. JavaScript Heap Footprint (R4-01):** The 4 MiB bound strictly constrains the retained serialized UTF-8 wire byte weight as received from transport frames, ensuring a deterministic, platform-independent boundary. In-memory JavaScript object graph allocations (V8 object headers, hidden class descriptors, internal array slots) naturally incur additional runtime heap overhead beyond raw wire bytes; this heap footprint is strictly bounded by the concurrent count limit ($K=1,000$ operations) and is completely reclaimed upon post-commit generational drain.

### 9.2 Browser-Compatible Wire Byte Accounting (R3-01)
1. **Preserving Original Transport Wire Size:**
   - The transport layer delivers raw wire bytes or UTF-8 strings. The original byte size is captured at network ingestion (`rawSerializedLength` from WebRTC `ArrayBuffer`, `Uint8Array`, or `new TextEncoder().encode(wireString).length`).
   - **No Parse & Reserialize:** The engine **does NOT re-serialize** parsed JSON messages with `JSON.stringify()` to compute byte lengths. Re-serialization alters key ordering, whitespace, and Unicode formatting, falsifying byte accounting. Original transport wire length is passed directly through admission and buffering.
2. **Admission Accounting:** Retained byte counters (`_activeBytes`) increment immediately upon valid admission using `rawSerializedLength`.
3. **Release Accounting:** Bytes are decremented upon dequeue/drain (`_drainBytes -= size`), upon generational clearing, or upon buffer disposal.
4. **Duplicate Handling:** Duplicates detected via identity index are rejected without incrementing retained bytes or operation counts.
5. **Rejection & Overflow:** Operations exceeding individual event size (4 KiB) or causing aggregate byte/count breaches are rejected before queue insertion, retaining 0 bytes and throwing `SyncEngineError`.
