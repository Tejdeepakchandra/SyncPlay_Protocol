# Phase 7.5 Milestone M5 — Interface Specification

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R5 AMENDED)
**Date:** 2026-11-25
**Authoritative Starting Baseline:** `40b11fdafcd247c7a289723f90ec0e2029c94f64` (Phase 7.5 M4.3d Formally Accepted)
**Accepted Baselines:** M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)
**Scope:** Interface contracts, type definitions, and API specifications for Milestone M5 Engine Runtime Recovery Integration.

---

## 1. Overview & Notation

This specification formalizes all runtime interfaces, data contracts, and error taxonomies required to integrate the snapshot recovery subsystem into `LeaderlessSyncEngine`.

Every interface and method in this document is labeled as either:
- **`[EXISTING — M4 ACCEPTED]`**: Already implemented and verified in the authoritative baseline `40b11fdafcd247c7a289723f90ec0e2029c94f64`.
- **`[PROPOSED — M5 RUNTIME]`**: To be implemented during Milestone M5 sub-milestones (M5.1–M5.4).

---

## 2. LeaderlessSyncEngine Runtime Integration Interfaces

### 2.1 Engine Configuration & Capability Custody (Closing F5, R2-H5, R3-07)

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
   * Mutually exclusive with recoveryCoordinator. Supplying both throws SyncEngineError('INVALID_CONFIG').
   * [PROPOSED — M5.1]
   */
  readonly recoveryOptions?: EngineRecoveryOptions;

  /**
   * Option B (External Coordinator Injection): Injected coordinator instance.
   * Mutually exclusive with recoveryOptions. Supplying both throws SyncEngineError('INVALID_CONFIG').
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

/**
 * Unforgeable in-process installation capability checked via object identity.
 * [EXISTING — M4 ACCEPTED in leaderless-sync-engine.ts:132]
 */
export interface RecoveryInstallationCapability {
  readonly authority: string;
  readonly sessionId: string;
  readonly token: string;
}
```

### 2.2 LeaderlessSyncEngine Public & Protected Recovery Methods (Closing F1, F7, R2-C1, R2-C2, R2-M2, R3-03, R3-06)

```typescript
export interface ILeaderlessSyncEngineRecovery {
  // ── Authoritative Lifecycle Queries vs Operational Mode (Closing F1, R3-03) ──

  /**
   * Queries authoritative recovery state from attached coordinator (Tier 1).
   * Returns 'ACTIVE' if unattached, or 'DISPOSED' if engine is disposed.
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
   * Queries whether recovery has terminated in a failed DEGRADED state.
   * Strictly true iff recoveryState === 'DEGRADED'.
   * [PROPOSED — M5.1]
   */
  isDegraded(): boolean;

  /**
   * Backward-compatible alias for isRecoveryInProgress().
   * [EXISTING — M4 ACCEPTED, refactored in M5.1 to delegate to isRecoveryInProgress()]
   */
  isRecoveryActive(): boolean;

  /**
   * Toggles internal operational notification-deferral flag (Tier 3).
   * Restricted slave switch driven exclusively by coordinator.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:921]
   */
  setRecoveryMode(active: boolean): void;

  // ── Single-Root Core State Replacement [EXISTING — M4 ACCEPTED] ───────────

  /**
   * Capability-gated single-root core state replacement.
   * Replaces this.core across all 12 authoritative fields in one synchronous reference
   * replacement under JavaScript run-to-completion semantics.
   * Called exclusively by SnapshotInstaller.commitAtomic() via opaque handle.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:883]
   */
  replaceCoreStateAtomic(
    candidate: EngineCoreState,
    capability: RecoveryInstallationCapability,
  ): void;

  /**
   * Submits an in-flight quarantined or stashed operation to destination engine for admission/reduction.
   * Enforces N_in-flight <= 1. Evaluates operations across the 6 deterministic replay categories.
   * Trust context is bound to caller's physical in-process custody:
   * - 'LOCAL_REPLAY' for stashed local operations from OrderedOperationLog (trusted, immutable, rate/clock checks bypassed).
   * - 'REPAIR_BATCH' for quarantined remote operations from QuarantineBuffer (untrusted, full structural validation).
   * Trust context is NEVER read or inferred from wire payloads or mutable operation fields.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:977, amended with trustContext in M5.1/M5.4]
   */
  admitDrainedOperation(
    op: QuarantinedOperation,
    trustContext?: 'LOCAL_REPLAY' | 'REPAIR_BATCH',
  ): DestinationAcceptanceReceipt;

  /**
   * Queries whether the engine already holds the specified operation.
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:1114]
   */
  hasOperation(originPeerId: string, sequenceNumber: number): boolean;

  /**
   * Flushes deferred notifications strictly AFTER engine recovery mode is cleared
   * and coordinator state transitions to ACTIVE.
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
   * Submits local user intent, fail-closing if recovery is not ACTIVE.
   * Throws SyncEngineError('REJECTED_RECOVERING') during RECOVERING/RECONCILING.
   * Throws SyncEngineError('REJECTED_DEGRADED') during DEGRADED.
   * [PROPOSED — M5.3]
   */
  submit(intent: LocalControlIntent): EngineDecision;
}

/**
 * Synchronous confirmation receipt returned by the destination engine container
 * upon evaluating an in-flight operation from QuarantineDrainer.
 * [EXISTING — M4 ACCEPTED in quarantine-drainer.ts:65]
 */
export type DestinationAcceptanceReceipt =
  | { readonly status: 'ACCEPTED_REDUCED'; readonly vectorAdvanced: true; readonly newSequence: number }
  | { readonly status: 'ACCEPTED_PENDING'; readonly vectorAdvanced: false; readonly reason: 'MISSING_DEPENDENCY' }
  | { readonly status: 'ALREADY_REPRESENTED'; readonly vectorAdvanced: false }
  | { readonly status: 'ALREADY_OWNED_DUPLICATE'; readonly vectorAdvanced: false }
  | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string }
  | { readonly status: 'CAPACITY_REJECTED'; readonly vectorAdvanced: false; readonly currentQueueSize: number; readonly maxCapacity: number };

/**
 * Authoritative in-flight tracking record owned exclusively by QuarantineDrainer.
 * Cardinality is bounded to at most 1 operation in flight at any instant.
 * [EXISTING — M4 ACCEPTED in quarantine-drainer.ts:54]
 */
export interface DrainedInFlightRecord {
  readonly operation: QuarantinedOperation;
  readonly dequeuedAtMs: number;
  attemptCount: number;
  destinationStatus:
    | 'PENDING'
    | 'ACCEPTED_REDUCED'
    | 'ACCEPTED_PENDING'
    | 'ALREADY_REPRESENTED'
    | 'ALREADY_OWNED_DUPLICATE'
    | 'TERMINALLY_REJECTED'
    | 'CAPACITY_REJECTED';
}
```

---

## 3. Recovery Coordinator Runtime Binding & Interception Decisions

### 3.1 IRuntimeRecoveryCoordinator Interface (Closing F1, F5, R2-H5, R3-07)

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

  /** Current attempt sequence number (fences all async attempt-scoped callbacks) */
  readonly currentAttemptId: number;

  /**
   * Binds the coordinator to the engine. Enforces single-attachment invariant.
   * Throws SyncEngineError('ALREADY_BOUND') if coordinator is already bound.
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
   * Original transport wire length passed directly as rawSerializedLength.
   * Throws SyncEngineError('QUARANTINE_OVERFLOW') on buffer limit breach.
   * [PROPOSED — M5.2]
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
   * Executes atomic installation and quarantine drain for an assembled snapshot
   * using the accepted opaque staged handle contract.
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

  /** Disposes coordinator resources, timers, and listeners. Idempotent. */
  dispose(): void;
}
```

### 3.2 Discriminated Inbound Interception Decision (Closing F3, F9, R2-H2, R3-04)

```typescript
/**
 * Unambiguous discriminated union returned by interceptRemoteEvent().
 * Note: Overflow is an exceptional boundary condition that throws SyncEngineError,
 * which is caught and mapped by engine to EngineDecision.code = 'REJECTED_CAPACITY'.
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
    };
```

---

## 4. Single-Root Engine Core State Contract (Closing F2, R2-C1)

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

/**
 * Immutable opaque handle representing a staged snapshot installation.
 * Candidate EngineCoreState is held in private WeakMap storage inside
 * SnapshotInstaller and is never exposed to external callers or coordinator.
 * [EXISTING — M4 ACCEPTED in snapshot-installer.ts]
 */
export interface StagedSnapshotInstallation {
  readonly handleId: string;
  readonly sessionId: string;
  readonly targetEpoch: number;
  readonly targetFrontier: Readonly<Record<string, number>>;
  readonly canonicalDigest: string;
  readonly maxLogicalTimestamp: number;
}
```

---

## 5. Supplier Selection, Lattice Ordering & Deterministic Comparator (Closing F4, R2-C3, R2-H1, R3-02, R3-05)

```typescript
/**
 * Protocol-stable, locale-independent peer ID comparator.
 * Based on ordinal ASCII code point comparison over ASCII_IDENTIFIER_REGEX domain.
 * [PROPOSED — M5.4]
 */
export function comparePeerIds(a: string, b: string): number {
  return a < b ? -1 : (a > b ? 1 : 0);
}

/**
 * Detailed outcome of evaluating candidates for snapshot recovery.
 * Fully distinguishes strict domination, identical states, compatible branches,
 * and true causal forks without conflating incomparable vectors with forks.
 * [PROPOSED — M5.4]
 */
export type CandidateSupplierSelectionResult =
  | {
      readonly status: 'SELECTED';
      readonly supplierPeerId: string;
      readonly snapshotFrontier: Record<string, number>;
      readonly selectionReason:
        | 'STRICT_DOMINATION'
        | 'EQUAL_LOWER_RTT'
        | 'EQUAL_ORDINAL_ASCII_ID'
        | 'HISTORICAL_PREFIX_SUBSUMES';
    }
  | {
      readonly status: 'IDENTICAL_STATE';
      readonly reason: string;
    }
  | {
      /** Incomparable but verified compatible histories: triggers operation repair or stashing */
      readonly status: 'INCOMPARABLE_COMPATIBLE';
      readonly candidateA: string;
      readonly candidateB: string;
      readonly reconciliationAction: 'OPERATION_REPAIR_PRIORITY' | 'LOCAL_STASHING_REQUIRED';
      readonly reason: string;
    }
  | {
      readonly status: 'REPAIRABLE_WITHIN_HORIZON';
      readonly maxGap: number;
      readonly logCapacity: number;
    }
  | {
      /** Proven conflict fork: divergent operations detected for same origin and sequence */
      readonly status: 'PROVEN_CONFLICT_FORK_DETECTED';
      readonly candidateA: string;
      readonly candidateB: string;
      readonly conflictingOrigin: string;
      readonly sequenceNumber: number;
      readonly reason: string;
    }
  | {
      /** Insufficient evidence: matching frontier alone without verified checkpoint chain */
      readonly status: 'INSUFFICIENT_EVIDENCE';
      readonly reason: string;
    }
  | {
      readonly status: 'NO_ELIGIBLE_SUPPLIERS';
      readonly reason: string;
    };
```

---

## 6. Pipeline Handoff, Latch & Timer Ownership (Closing F10, R2-H4)

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

/**
 * Four-category timer classifications and their cancellation lifecycles.
 */
export interface RecoveryTimerInventory {
  /** Snapshot-attempt timer: cancelled on attempt success, retry, abort, or disposal */
  overallTimer: NodeJS.Timeout | null;

  /** Reconciliation timer: cancelled on drain success, abort, or disposal */
  reconciliationTimer: NodeJS.Timeout | null;

  /** Offer/transfer discovery timer: cancelled on offer selection, transfer start, abort, or disposal */
  offerAwaitTimer: NodeJS.Timeout | null;

  /**
   * Future-epoch convergence timer: independent grace timer (5s).
   * Scoped to epoch identity. NOT cancelled on snapshot attempt complete/retry/reset.
   * Cancelled ONLY when engine epoch advances >= target epoch, on grace expiration, or on disposal.
   */
  futureEpochGraceTimer: NodeJS.Timeout | null;
}
```

---

## 7. Error Code Taxonomy

| Error Code | Layer | Invariant Enforced | Behavior |
|---|---|---|---|
| `REJECTED_RECOVERING` | Engine / Local Gating | Local inputs strictly prohibited during recovery | Synchronous exception throw (`SyncEngineError`) in `receiveLocal()` / `submit()`. |
| `REJECTED_DEGRADED` | Engine / Local Gating | Engine state corrupted or recovery unresolvable | Synchronous exception throw on local intents; remote events dropped with `outcome: 'REJECTED_DEGRADED'`. |
| `BUFFERED_QUARANTINE` | Engine Decision | Remote events during recovery must not mutate active state | Engine records decision; event queued in `QuarantineBuffer`. |
| `BUFFERED_FUTURE_EPOCH`| Coordinator | Epoch transition must be capability authorized | Event staged in `FutureEpochBuffer` with independent 5s expiration timer. |
| `DISCARD_OBSOLETE_EPOCH`| Coordinator | Obsolete membership operations ignored | Remote event dropped with zero anti-entropy or state recording. |
| `QUARANTINE_OVERFLOW` | QuarantineBuffer | Dual bound: 1,000 ops / 4 MiB aggregate | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`; engine isolates to `REJECTED_CAPACITY`. |
| `FUTURE_EPOCH_OVERFLOW`| FutureEpochBuffer | Bound: 100 ops / 512 KiB aggregate | Throws `SyncEngineError`; coordinator aborts recovery to `DEGRADED`; engine isolates to `REJECTED_CAPACITY`. |
| `UNRESOLVED_FUTURE_EPOCH`| Coordinator | 5s grace period expired without capability | Coordinator aborts recovery to `DEGRADED`. |
| `REJECTED_CAPACITY` | LeaderlessSyncEngine | Memory and queue capacity bounds preserved | Remote event rejected; engine records decision; remote error isolated without throwing. |
| `FORK_DETECTED_INCOMPARABLE`| Coordinator | Divergent histories with conflicting operations | Coordinator aborts recovery to `DEGRADED`. |
| `INCOMPATIBLE_COMPACTED_HISTORY` | Coordinator / Replay | Canonical SMR total-order preservation against compacted snapshots | Recovery aborts to `DEGRADED` when stashed local operations have Lamport timestamp <= snapshot compacted frontier and cannot be canonically ordered after snapshot base. |
| `ALREADY_BOUND` | Coordinator | Single-attachment invariant | Throws `SyncEngineError` if coordinator is bound to more than one engine. |
| `UNAUTHORIZED` | LeaderlessSyncEngine | Capability custody enforced by object identity | Throws `SyncEngineError` if capability does not match `authorizedRecoveryCapability`. |
