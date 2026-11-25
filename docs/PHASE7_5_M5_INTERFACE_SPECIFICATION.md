# Phase 7.5 Milestone M5 — Interface Specification

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0)  
**Date:** 2026-11-25  
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)  
**Scope:** Interface contracts, type definitions, and API specifications for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Overview & Notation

This specification formalizes all runtime interfaces, data contracts, and error taxonomies required to integrate the snapshot recovery subsystem into the `LeaderlessSyncEngine`.

To maintain strict auditability, every interface and method in this document is labeled as either:
- **`[EXISTING — M4 ACCEPTED]`**: Already implemented and verified in the authoritative baseline `40b11fdafcd247c7a289723f90ec0e2029c94f64`.
- **`[PROPOSED — M5 RUNTIME]`**: To be implemented during Milestone M5 sub-milestones (M5.1–M5.4).

---

## 2. LeaderlessSyncEngine Runtime Integration Interfaces

### 2.1 Engine Configuration Updates

```typescript
/**
 * Configuration options for LeaderlessSyncEngine.
 */
export interface LeaderlessSyncEngineConfig {
  // Existing fields (Phase 1–7.4)
  readonly sessionId: string;
  readonly localPeerId: string;
  readonly clock?: { now: () => number };
  readonly logCapacity?: number;
  readonly reducer?: (state: PlaybackState, op: StoredOperation) => PlaybackState;
  readonly initialState?: Partial<PlaybackState>;

  // Capability bindings [EXISTING — M4 ACCEPTED]
  readonly authorizedEpochCapability?: EpochTransitionCapability;
  readonly authorizedTransportCapability?: TransportRegistrationCapability;
  readonly authorizedRecoveryCapability?: RecoveryInstallationCapability;

  /**
   * Optional runtime recovery coordinator binding.
   * If provided, the engine automatically routes inbound remote events to quarantine,
   * fail-closes local inputs, and delegates isRecoveryActive() queries.
   *
   * [PROPOSED — M5.1]
   */
  readonly recoveryCoordinator?: IRuntimeRecoveryCoordinator;

  /**
   * Optional runtime recovery coordinator options for autonomous creation.
   *
   * [PROPOSED — M5.1]
   */
  readonly recoveryOptions?: EngineRecoveryOptions;
}

/**
 * Options for autonomous recovery coordinator initialization inside the engine.
 * [PROPOSED — M5.1]
 */
export interface EngineRecoveryOptions {
  readonly overallRecoveryTimeoutMs?: number;
  readonly reconciliationTimeoutMs?: number;
  readonly offerAwaitTimeoutMs?: number;
  readonly epochConvergenceGraceMs?: number;
  readonly quarantineWarningThresholdBytes?: number;
}
```

### 2.2 LeaderlessSyncEngine Public & Protected Recovery Methods

```typescript
export interface ILeaderlessSyncEngineRecovery {
  // ── Existing Recovery Methods [EXISTING — M4 ACCEPTED] ──────────────────────

  /**
   * Queries whether recovery mode is currently active on the engine.
   * [EXISTING — M4 ACCEPTED, to be updated in M5.1 to query recoveryCoordinator dynamically]
   */
  isRecoveryActive(): boolean;

  /**
   * Synchronous capability-gated single-root core state replacement.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:883]
   */
  replaceCoreStateAtomic(
    candidate: EngineCoreState,
    capability: RecoveryInstallationCapability,
  ): void;

  /**
   * Submits an in-flight quarantined operation to destination engine for admission/reduction.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:977]
   */
  admitDrainedOperation(op: QuarantinedOperation): DestinationAcceptanceReceipt;

  /**
   * Queries whether the engine already holds the specified operation.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:1114]
   */
  hasOperation(originPeerId: string, sequenceNumber: number): boolean;

  /**
   * Flushes deferred state change and decision notifications after recovery completes.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:929]
   */
  flushDeferredNotifications(): void;

  /**
   * Imports snapshot using single-root atomic candidate construction.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:2567]
   */
  importSnapshotV2(snapshot: StateSnapshotPayloadV2): EngineDecision;

  /**
   * Exports canonical StateSnapshotPayloadV2.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:2377]
   */
  exportSnapshotV2(snapshotId?: string): StateSnapshotPayloadV2;

  // ── Proposed Runtime Recovery Methods [PROPOSED — M5.1 / M5.2] ────────────

  /**
   * Attaches a RecoveryCoordinator to the engine at runtime.
   * [PROPOSED — M5.1]
   */
  attachRecoveryCoordinator(coordinator: IRuntimeRecoveryCoordinator): void;

  /**
   * Retrieves the currently attached RecoveryCoordinator, if any.
   * [PROPOSED — M5.1]
   */
  getRecoveryCoordinator(): IRuntimeRecoveryCoordinator | undefined;

  /**
   * Submits a local user intent for execution, fail-closing if recovery is active.
   * [PROPOSED — M5.3]
   */
  submit(intent: LocalControlIntent): EngineDecision;
}
```

---

## 3. Recovery Coordinator Runtime Binding Interfaces

### 3.1 IRuntimeRecoveryCoordinator Interface

```typescript
/**
 * Public interface exposed by RecoveryCoordinator to the engine runtime.
 * Extends the M4 accepted IRecoveryCoordinator contract.
 */
export interface IRuntimeRecoveryCoordinator {
  // ── Existing Coordinator Methods [EXISTING — M4 ACCEPTED] ──────────────────

  /** Current authoritative recovery state */
  readonly state: RecoveryState;

  /** Current recovery substate */
  readonly substate: RecoverySubstate;

  /** Whether the coordinator is disposed */
  readonly isDisposed: boolean;

  /** Current attempt sequence number */
  readonly currentAttemptId: number;

  /**
   * Triggers an autonomous recovery attempt targeting the specified supplier.
   */
  triggerRecovery(peerId: string, reason: string): void;

  /**
   * Intercepts a remote event candidate, routing to quarantine, future-epoch, or active admission.
   */
  interceptRemoteEvent(
    event: AnyControlSyncEvent,
    rawSerializedLength: number,
  ): RemoteInterceptionResult;

  /**
   * Intercepts a local user intent, fail-closing during non-ACTIVE recovery states.
   */
  interceptLocalSubmission(): LocalAdmissionResult;

  /**
   * Executes atomic installation and quarantine drain for an assembled snapshot.
   */
  executeRecoveryWithSnapshot(
    snapshotPayload: StateSnapshotPayloadV2,
    expectedSha256Digest?: string,
  ): Promise<SnapshotRecoveryExecutionResult>;

  /**
   * Reconciles quarantined operations after snapshot installation.
   */
  reconcileQuarantine(attemptId?: number): Promise<QuarantineDrainResult>;

  /**
   * Aborts active recovery attempt and transitions deterministically to DEGRADED.
   */
  abortRecovery(reason: RecoveryFailureReason, errorOrMessage?: Error | string): void;

  /**
   * Resets recovery state back to ACTIVE or IDLE.
   */
  resetRecovery(initiatedBy: string): void;

  /** Disposes coordinator resources, timers, and listeners */
  dispose(): void;

  // ── Proposed Runtime Hooks [PROPOSED — M5.1] ───────────────────────────────

  /**
   * Binds the coordinator to the engine and registers lifecycle callbacks.
   * [PROPOSED — M5.1]
   */
  bindEngine(engine: LeaderlessSyncEngine): void;
}
```

### 3.2 Interception and Admission Result Types

```typescript
/**
 * Result of intercepting an inbound remote event.
 * [EXISTING — M4 ACCEPTED]
 */
export interface RemoteInterceptionResult {
  readonly shouldQuarantine: boolean;
  readonly dropped?: boolean;
  readonly duplicate?: boolean;
}

/**
 * Result of intercepting a local user action submission.
 * [EXISTING — M4 ACCEPTED in recovery-admission-controller.ts]
 */
export type LocalAdmissionResult =
  | { readonly allowed: true }
  | {
      readonly allowed: false;
      readonly code: 'DISPOSED' | 'REJECTED_RECOVERING' | 'REJECTED_DEGRADED';
      readonly reason: string;
    };
```

---

## 4. Single-Root Engine Core State Contract

```typescript
/**
 * Authoritative single-root container holding all 12 core engine state fields.
 * Swapped atomically via LeaderlessSyncEngine.replaceCoreStateAtomic().
 * [EXISTING — M4 ACCEPTED in engine-core-state.ts]
 */
export interface EngineCoreState {
  /** Primary active playback state (deeply frozen) */
  state: PlaybackState;

  /** Alias for state */
  playbackState: PlaybackState;

  /** Compaction base anchor for speculative replay */
  baseSnapshot: PlaybackState;

  /** Highest compacted sequence numbers per origin */
  baseSnapshotFrontier: Record<string, number>;

  /** Authoritative totally-ordered operation log */
  operationLog: OrderedOperationLog;

  /** Authoritative Lamport logical clock manager and validator */
  clockManager: LogicalClockManager;

  /** Authoritative missing-dependency queue for buffered causal operations */
  pendingQueue: PendingDependencyQueue;

  /** Highest contiguous applied sequences per origin peer */
  replicationVector: Record<string, number>;

  /** Observed causal horizon for local event generation */
  localCausalContext: Record<string, number>;

  /** Monotonic sequence counter for locally originated events */
  localOriginSequence: number;

  /** Current membership configuration epoch */
  currentEpoch: number;

  /** Identity of the most recently applied operation */
  lastAppliedEventId: string | undefined;

  /** Monotonic state version counter */
  stateVersion: number;
}
```

---

## 5. Quarantine Drainer and Destination Contracts

```typescript
/**
 * Synchronous confirmation receipt returned by the destination engine container
 * upon receiving an in-flight operation from QuarantineDrainer.
 * [EXISTING — M4 ACCEPTED in quarantine-drainer.ts]
 */
export type DestinationAcceptanceReceipt =
  | {
      readonly status: 'ACCEPTED_REDUCED';
      readonly vectorAdvanced: true;
      readonly newSequence: number;
    }
  | {
      readonly status: 'ACCEPTED_PENDING';
      readonly vectorAdvanced: false;
      readonly reason: 'MISSING_DEPENDENCY';
    }
  | {
      readonly status: 'ALREADY_REPRESENTED';
      readonly vectorAdvanced: false;
    }
  | {
      readonly status: 'ALREADY_OWNED_DUPLICATE';
      readonly vectorAdvanced: false;
    }
  | {
      readonly status: 'TERMINALLY_REJECTED';
      readonly vectorAdvanced: false;
      readonly error: string;
    }
  | {
      readonly status: 'CAPACITY_REJECTED';
      readonly vectorAdvanced: false;
      readonly currentQueueSize: number;
      readonly maxCapacity: number;
    };

/**
 * In-flight tracking record owned exclusively by QuarantineDrainer.
 * [EXISTING — M4 ACCEPTED in quarantine-drainer.ts]
 */
export interface DrainedInFlightRecord {
  readonly operation: QuarantinedOperation;
  readonly dequeuedAtMs: number;
  attemptCount: number;
  destinationStatus: DestinationAcceptanceStatus;
}

/**
 * Summary metrics of a quarantine drain execution.
 * [EXISTING — M4 ACCEPTED in quarantine-buffer.ts]
 */
export interface QuarantineDrainResult {
  readonly batchSize: number;
  readonly appliedCount: number;
  readonly pendingCount: number;
  readonly discardedCount: number;
  readonly haltedOnCapacity: boolean;
}
```

---

## 6. Anti-Entropy & Recovery Trigger Bindings

```typescript
/**
 * Callback additions to AntiEntropyCallbacks for autonomous recovery handoff.
 * [PROPOSED — M5.4]
 */
export interface AntiEntropyCallbacksWithRecovery extends AntiEntropyCallbacks {
  /**
   * Invoked when compareRemoteDigest() detects that the remote peer's snapshot frontier
   * exceeds local history (OUTSIDE_REMOTE_HORIZON).
   * Triggers autonomous snapshot recovery.
   * [PROPOSED — M5.4]
   */
  readonly onSnapshotRecoveryRequired?: (details: {
    readonly supplierPeerId: string;
    readonly remoteFrontier: Record<string, number>;
    readonly localVector: Record<string, number>;
    readonly reason: string;
  }) => void;
}
```

---

## 7. Error Code Taxonomy

| Error Code | Layer | Invariant Enforced | Behavior |
|---|---|---|---|
| `REJECTED_RECOVERING` | Engine / Admission | Local inputs strictly prohibited during recovery | Synchronous exception throw (`SyncEngineError`) in `receiveLocal()` / `submit()`. |
| `REJECTED_DEGRADED` | Engine / Admission | Engine state corrupted or recovery unresolvable | Synchronous exception throw on local; remote events dropped. |
| `BUFFERED_QUARANTINE` | Engine Decision | Remote events during recovery must not mutate active state | Engine logs decision with `accepted: false`; event queued in `QuarantineBuffer`. |
| `BUFFERED_FUTURE_EPOCH`| Coordinator | Epoch transition must be capability authorized | Event staged in `FutureEpochBuffer` with 5s expiration timer. |
| `DISCARD_OBSOLETE_EPOCH`| Coordinator | Obsolete membership operations ignored | Remote event dropped with zero anti-entropy or state recording. |
| `QUARANTINE_OVERFLOW` | QuarantineBuffer | Dual bound: 1,000 ops / 4 MiB total | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`. |
| `FUTURE_EPOCH_OVERFLOW`| FutureEpochBuffer | Bound: 100 ops / 512 KiB total | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`. |
| `UNRESOLVED_FUTURE_EPOCH`| Coordinator | 5s grace period expired without capability | Coordinator aborts recovery to `DEGRADED`. |
| `DRAIN_CAPACITY_EXCEEDED`| QuarantineDrainer | Destination queue capacity preserved | Drain halts; in-flight op unshifted; recovery aborts to `DEGRADED`. |
