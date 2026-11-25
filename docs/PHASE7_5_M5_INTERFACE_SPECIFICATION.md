# Phase 7.5 Milestone M5 — Interface Specification

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R1 AMENDED)  
**Date:** 2026-11-25  
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)  
**Scope:** Interface contracts, type definitions, and API specifications for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Overview & Notation

This specification formalizes all runtime interfaces, data contracts, and error taxonomies required to integrate the snapshot recovery subsystem into `LeaderlessSyncEngine`.

Every interface and method in this document is labeled as either:
- **`[EXISTING — M4 ACCEPTED]`**: Already implemented and verified in the authoritative baseline `40b11fdafcd247c7a289723f90ec0e2029c94f64`.
- **`[PROPOSED — M5 RUNTIME]`**: To be implemented during Milestone M5 sub-milestones (M5.1–M5.4).

---

## 2. LeaderlessSyncEngine Runtime Integration Interfaces

### 2.1 Engine Configuration (Closing F5 — Mutually Exclusive Options)

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
   * Option A (Autonomous Construction): Options to construct and own RecoveryCoordinator internally.
   * Mutually exclusive with recoveryCoordinator.
   * [PROPOSED — M5.1]
   */
  readonly recoveryOptions?: EngineRecoveryOptions;

  /**
   * Option B (External Coordinator Injection): Injected coordinator instance.
   * Mutually exclusive with recoveryOptions.
   * [PROPOSED — M5.1]
   */
  readonly recoveryCoordinator?: IRuntimeRecoveryCoordinator;
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

### 2.2 LeaderlessSyncEngine Public & Protected Recovery Methods (Closing F1 & F7)

```typescript
export interface ILeaderlessSyncEngineRecovery {
  // ── Authoritative Lifecycle Queries (Closing F1) ──────────────────────────

  /**
   * Queries the authoritative recovery state from the attached coordinator.
   * Returns 'ACTIVE' if no coordinator is attached, or 'DISPOSED' if engine is disposed.
   * [PROPOSED — M5.1]
   */
  readonly recoveryState: RecoveryState;

  /**
   * Queries whether recovery is currently in progress (RECOVERING or RECONCILING).
   * Strictly false in ACTIVE, DEGRADED, and DISPOSED states.
   * [PROPOSED — M5.1]
   */
  isRecoveryInProgress(): boolean;

  /**
   * Backward-compatible alias for isRecoveryInProgress().
   * [EXISTING — M4 ACCEPTED, refactored in M5.1 to delegate to isRecoveryInProgress()]
   */
  isRecoveryActive(): boolean;

  // ── Single-Root Core State Replacement [EXISTING — M4 ACCEPTED] ───────────

  /**
   * Synchronous capability-gated single-root core state replacement.
   * Replaces this.core across all 12 authoritative fields in 0 ticks.
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
   * Flushes deferred notifications strictly AFTER state transitions to ACTIVE.
   * - stateChangeListeners: exactly ONE coalesced notification with final PlaybackState.
   * - decisionListeners: all decisions dispatched sequentially in FIFO order.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:929, updated in M5.1]
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

  // ── Local Input Gating & Intent Submission [PROPOSED — M5.3] ─────────────

  /**
   * Submits a local user intent for execution, fail-closing if recovery is not ACTIVE.
   * Throws SyncEngineError('REJECTED_RECOVERING') during RECOVERING/RECONCILING.
   * Throws SyncEngineError('REJECTED_DEGRADED') during DEGRADED.
   * [PROPOSED — M5.3]
   */
  submit(intent: LocalControlIntent): EngineDecision;
}
```

---

## 3. Recovery Coordinator Runtime Binding & Discriminated Decisions

### 3.1 IRuntimeRecoveryCoordinator Interface (Closing F1 & F5)

```typescript
/**
 * Public interface exposed by RecoveryCoordinator to the engine runtime.
 */
export interface IRuntimeRecoveryCoordinator {
  /** Current authoritative recovery state ('ACTIVE' | 'RECOVERING' | 'RECONCILING' | 'DEGRADED' | 'DISPOSED') */
  readonly state: RecoveryState;

  /** Current recovery substate */
  readonly substate: RecoverySubstate;

  /** Whether the coordinator is disposed */
  readonly isDisposed: boolean;

  /** Current attempt sequence number (fences all async callbacks) */
  readonly currentAttemptId: number;

  /**
   * Binds the coordinator to the engine. Throws if already bound.
   * [PROPOSED — M5.1]
   */
  bindEngine(engine: LeaderlessSyncEngine, capability: RecoveryInstallationCapability): void;

  /**
   * Triggers an autonomous recovery attempt targeting the specified supplier.
   * Strictly idempotent if recovery is already in progress.
   */
  triggerRecovery(peerId: string, reason: string): void;

  /**
   * Intercepts an inbound remote event using discriminated union results.
   * Synchronous, O(1), <= 5μs overhead in ACTIVE state.
   * [PROPOSED — M5.2 replacing optional boolean result]
   */
  interceptRemoteEvent(
    event: AnyControlSyncEvent,
    rawSerializedLength: number,
  ): RemoteAdmissionDecision;

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
   * Resets recovery state back to ACTIVE. Allowed only from DEGRADED state.
   */
  resetRecovery(initiatedBy: string): void;

  /** Disposes coordinator resources, timers, and listeners */
  dispose(): void;
}
```

### 3.2 Discriminated Inbound Interception Decision (Closing F3 & F9)

```typescript
/**
 * Unambiguous discriminated union returned by interceptRemoteEvent().
 * [PROPOSED — M5.2]
 */
export type RemoteAdmissionDecision =
  | {
      /** In ACTIVE state: event admitted to normal receiveRemote() execution */
      readonly outcome: 'ADMIT_ACTIVE';
    }
  | {
      /** In RECOVERING/RECONCILING state: current epoch event placed in QuarantineBuffer */
      readonly outcome: 'QUARANTINED';
      readonly sequenceNumber: number;
      readonly originPeerId: string;
      readonly rawSerializedLength: number;
    }
  | {
      /** Event claims future epoch: staged in FutureEpochBuffer with 5s grace timer */
      readonly outcome: 'BUFFERED_FUTURE_EPOCH';
      readonly targetEpoch: number;
      readonly expiresAtMs: number;
    }
  | {
      /** Event claims obsolete epoch (E < currentEpoch): discarded deterministically */
      readonly outcome: 'DISCARDED_OBSOLETE_EPOCH';
      readonly eventEpoch: number;
      readonly currentEpoch: number;
    }
  | {
      /** Duplicate of operation currently owned by QuarantineDrainer (DRAINER_IN_FLIGHT) */
      readonly outcome: 'DROPPED_DRAINER_DUPLICATE';
      readonly originPeerId: string;
      readonly sequenceNumber: number;
    }
  | {
      /** Engine or coordinator is in DEGRADED state: event dropped */
      readonly outcome: 'REJECTED_DEGRADED';
      readonly failureReason?: string;
    }
  | {
      /** Engine or coordinator is disposed: event rejected */
      readonly outcome: 'REJECTED_DISPOSED';
    }
  | {
      /** Quarantine or future-epoch buffer overflow: recovery aborted to DEGRADED */
      readonly outcome: 'ABORTED_OVERFLOW';
      readonly overflowType: 'QUARANTINE_OVERFLOW' | 'FUTURE_EPOCH_OVERFLOW';
      readonly detail: string;
    };
```

---

## 4. Single-Root Engine Core State Contract (Closing F2)

```typescript
/**
 * Authoritative single-root container holding all 12 core engine state fields,
 * plus 1 invariant referential alias (playbackState === state).
 * Sealed via Object.seal() upon construction.
 * [EXISTING — M4 ACCEPTED in engine-core-state.ts]
 */
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

  // ── Referential Alias Property (Invariant: playbackState === state) ────────
  playbackState: PlaybackState;                // 13. Deeply frozen alias pointing to state
}
```

---

## 5. Supplier Selection & Lattice Ordering Types (Closing F4)

```typescript
/**
 * Detailed outcome of evaluating candidates for snapshot recovery.
 * [PROPOSED — M5.4]
 */
export type CandidateSupplierSelectionResult =
  | {
      readonly status: 'SELECTED';
      readonly supplierPeerId: string;
      readonly snapshotFrontier: Record<string, number>;
      readonly selectionReason: 'STRICT_DOMINATION' | 'EQUAL_LOWER_RTT' | 'EQUAL_LEXICOGRAPHICAL_TIEBREAK' | 'PROACTIVE_HORIZON_SENDER';
    }
  | {
      readonly status: 'INCOMPARABLE_FORK_DETECTED';
      readonly candidateA: string;
      readonly frontierA: Record<string, number>;
      readonly candidateB: string;
      readonly frontierB: Record<string, number>;
      readonly reason: string;
    }
  | {
      readonly status: 'NO_ELIGIBLE_SUPPLIERS';
      readonly reason: string;
    };
```

---

## 6. Pipeline Handoff & Latch Interfaces (Closing F10)

```typescript
/**
 * Completion callback registered with SnapshotInboundPipeline.
 * [PROPOSED — M5.4 with explicit attemptId fencing and one-shot latch]
 */
export interface InboundTransferCompletionHandler {
  (
    result: SnapshotTransferResult,
    attemptId: number,
  ): Promise<void>;
}
```

---

## 7. Error Code Taxonomy

| Error Code | Layer | Invariant Enforced | Behavior |
|---|---|---|---|
| `REJECTED_RECOVERING` | Engine / Local Gating | Local inputs strictly prohibited during recovery | Synchronous exception throw (`SyncEngineError`) in `receiveLocal()` / `submit()`. |
| `REJECTED_DEGRADED` | Engine / Local Gating | Engine state corrupted or recovery unresolvable | Synchronous exception throw on local intents; remote events dropped with `outcome: 'REJECTED_DEGRADED'`. |
| `BUFFERED_QUARANTINE` | Engine Decision | Remote events during recovery must not mutate active state | Engine records decision; event queued in `QuarantineBuffer`. |
| `BUFFERED_FUTURE_EPOCH`| Coordinator | Epoch transition must be capability authorized | Event staged in `FutureEpochBuffer` with 5s expiration timer. |
| `DISCARD_OBSOLETE_EPOCH`| Coordinator | Obsolete membership operations ignored | Remote event dropped with zero anti-entropy or state recording. |
| `QUARANTINE_OVERFLOW` | QuarantineBuffer | Dual bound: 1,000 ops / 4 MiB total | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`. |
| `FUTURE_EPOCH_OVERFLOW`| FutureEpochBuffer | Bound: 100 ops / 512 KiB total | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`. |
| `UNRESOLVED_FUTURE_EPOCH`| Coordinator | 5s grace period expired without capability | Coordinator aborts recovery to `DEGRADED`. |
| `DRAIN_CAPACITY_EXCEEDED`| QuarantineDrainer | Destination queue capacity preserved | Drain halts; in-flight op unshifted; recovery aborts to `DEGRADED`. |
| `FORK_DETECTED_INCOMPARABLE`| Coordinator | No arbitrary ordering between incomparable dominating frontiers | Coordinator aborts recovery to `DEGRADED`. |
