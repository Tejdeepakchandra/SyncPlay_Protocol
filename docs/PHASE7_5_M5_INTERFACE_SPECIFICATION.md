# Phase 7.5 Milestone M5 — Interface Specification

**Status:** APPROVED ARCHITECTURAL SPECIFICATION (MILESTONE M5.0-R14 AMENDED)
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

// ── Drain Admission Context Capabilities & Private Custody (Closing R6-01, R7-01) ──

/**
 * Internal unique symbol branding unforgeable drain admission capabilities.
 * Declared strictly within module packages/sync-engine/src/recovery/drain-admission-custody.ts.
 * Module-private: NEVER exported from packages/sync-engine/src/index.ts.
 * [PROPOSED — M5.1]
 */
const DRAIN_ADMISSION_BRAND: unique symbol = Symbol('DRAIN_ADMISSION_BRAND');

/**
 * Unforgeable capability authorizing local operation replay during post-snapshot reconciliation.
 * Bypasses arrival token-bucket rate limits and remote clock inflation checks.
 * Held strictly by trusted internal local reconciliation workflow.
 * [PROPOSED — M5.1]
 */
export interface TrustedLocalReplayCapability {
  readonly [DRAIN_ADMISSION_BRAND]: true;
  readonly kind: 'LOCAL_REPLAY';
  readonly sessionId: string;
  readonly authority: string;
  readonly token: string;
}

/**
 * Unforgeable capability authorizing quarantined remote batch admission during post-snapshot drain.
 * Undergoes full structural and causal dependency validation.
 * Held strictly by QuarantineDrainer.
 * [PROPOSED — M5.1]
 */
export interface TrustedRepairBatchCapability {
  readonly [DRAIN_ADMISSION_BRAND]: true;
  readonly kind: 'REPAIR_BATCH';
  readonly sessionId: string;
  readonly authority: string;
  readonly token: string;
}

/**
 * Discriminated union of unforgeable drain admission capabilities.
 * [PROPOSED — M5.1]
 */
export type DrainAdmissionContext =
  | TrustedLocalReplayCapability
  | TrustedRepairBatchCapability;

/**
 * Immutable container bundle transferring drain admission capabilities privately
 * from LeaderlessSyncEngine to RecoveryCoordinator during engine binding.
 * [PROPOSED — M5.1]
 */
export interface AuthorizedDrainCapabilities {
  readonly localReplayCapability: TrustedLocalReplayCapability;
  readonly repairBatchCapability: TrustedRepairBatchCapability;
}

/**
 * Internal factory for minting authorized TrustedLocalReplayCapability.
 * Sealed at runtime via Object.freeze() (shallow freezing is sufficient for primitive fields).
 * Module-private to drain-admission-custody.ts; not exported in package entry points.
 * [PROPOSED — M5.1]
 */
export function createLocalReplayCapability(
  sessionId: string,
  authority = 'local-reconciliation',
  token?: string,
): TrustedLocalReplayCapability {
  return Object.freeze({
    [DRAIN_ADMISSION_BRAND]: true as const,
    kind: 'LOCAL_REPLAY' as const,
    sessionId,
    authority,
    token:
      token ??
      `local-replay-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
}

/**
 * Internal factory for minting authorized TrustedRepairBatchCapability.
 * Sealed at runtime via Object.freeze() (shallow freezing is sufficient for primitive fields).
 * Module-private to drain-admission-custody.ts; not exported in package entry points.
 * [PROPOSED — M5.1]
 */
export function createRepairBatchCapability(
  sessionId: string,
  authority = 'quarantine-drainer',
  token?: string,
): TrustedRepairBatchCapability {
  return Object.freeze({
    [DRAIN_ADMISSION_BRAND]: true as const,
    kind: 'REPAIR_BATCH' as const,
    sessionId,
    authority,
    token:
      token ??
      `repair-batch-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
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
   * Capability-gated: requires unforgeable DrainAdmissionContext checked via strict object-reference identity:
   * - TrustedLocalReplayCapability (strictly for stashed local operations from OrderedOperationLog).
   * - TrustedRepairBatchCapability (strictly for quarantined remote operations from QuarantineDrainer).
   *
   * Authority vs. Provenance Invariant:
   * Capability possession establishes caller authority to request admission mode, but does NOT validate
   * operation payload provenance. Operations admitted under TrustedLocalReplayCapability must originate
   * strictly and exclusively from stashedLocalQueue (immutable committed local log entries; remote operations
   * cannot be inserted or relabeled as local). Identity hash H(op) == H(log) and causal dependencies are verified;
   * any identity conflict or unsatisfied dependency aborts recovery immediately to DEGRADED.
   *
   * Mandatory Attempt-ID Fencing, Candidate Staging & Root Commit Invariants (R8-02, R9-02, R9-03, R10-03, R11-A, R11-B):
   * attemptId is strictly mandatory for recovery admission. If attemptId !== coordinator.currentAttemptId,
   * or coordinator is not in RECONCILING, admitDrainedOperation fails closed: returns status ATTEMPT_INVALIDATED
   * with reason 'STALE_ATTEMPT_FENCED' and performs zero state mutation, zero log insertion, and zero vector advancement.
   *
   * Candidate Isolation & Commit Sequence (R11-B - Required Future M5.1 Invariant):
   * 1. Mutable Nested Object Disjointness: candidate staging allocates fresh/CoW instances of operationLog,
   *    clockManager, pendingQueue, and replicationVector; never mutates objects reachable from active this.core.
   * 2. Side-Effect Free Phase 1: causal validation and SMR dry-run execute purely against candidate instances.
   * 3. Pre-Commit Fencing & CAS: asserts attemptId, coordinator in RECONCILING, and this.core === expectedPriorRoot.
   * 4. Atomic Linearization Phase 2: single synchronous root pointer swap (this.core = candidateCore).
   * 5. Post-Commit Encapsulation: candidate structures not exposed as writable to external callers.
   *
   * Trust context is NEVER read or inferred from wire payloads or mutable operation fields.
   * Calling with an invalid, forged, or mismatched capability throws SyncEngineError('UNAUTHORIZED').
   * [EXISTING — M4 ACCEPTED in LeaderlessSyncEngine.ts:977, capability-gated in M5.1/M5.4]
   */
  admitDrainedOperation(
    op: QuarantinedOperation,
    capability: DrainAdmissionContext,
    attemptId: number,
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
  | { readonly status: 'ATTEMPT_INVALIDATED'; readonly vectorAdvanced: false; readonly attemptId: number; readonly reason: 'STALE_ATTEMPT_FENCED' | 'COORDINATOR_NOT_RECONCILING' }
  | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string; readonly reason?: string; readonly operationId?: string }
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
   * Transfers authorized RecoveryInstallationCapability and AuthorizedDrainCapabilities privately.
   * Throws SyncEngineError('ALREADY_BOUND') if coordinator is already bound.
   * [PROPOSED — M5.1]
   */
  bindEngine(
    engine: LeaderlessSyncEngine,
    capability: RecoveryInstallationCapability,
    drainCapabilities: AuthorizedDrainCapabilities,
  ): void;

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
   * Administratively resets recovery state. Allowed strictly from DEGRADED state.
   * Requires authorized RecoveryInstallationCapability checked via strict reference equality.
   * Safely purges un-reconciled state or re-initializes engine to a clean verified baseline;
   * never declares incomplete or corrupted state as healthy.
   *
   * Failure-Atomic Lifecycle Semantics (Closing R8-01–R8-05, R9-01–R9-06, R10-01–R10-04, R11-A–R11-D):
   * 1. 'PURGE_AND_RETRY' (R11-A, R11-C, R11-D):
   *    - Monotonically increments attempt counter (currentAttemptId++), fencing all async callbacks/timers.
   *    - Concurrent Reset Serialization (R9-01): multiple reset requests share settlement barrier; each re-validates
   *      attemptId === currentAttemptId post-settlement so only newest attempt starts recovery work.
   *    - In-flight invalidation with retained ownership slot (R8-01): marks invalidated = true, retains slot
   *      (N_in-flight = 1), blocks new drains via drainSettledBarrier() until settled.
   *    - Separate Invalidation from Permanent Rejection (R11-A): returns ATTEMPT_INVALIDATED; unshifted to QuarantineBuffer.
   *    - Quarantine Fate (R11-D): un-drained buffer ops retained for new attempt; already-linearized ops remain in log.
   *    - Terminal Archive Overflow (R11-C): archive capacity capped at 500 without silent eviction; overflow degrades fail-closed.
   *    - Transitions coordinator to RECOVERING under new attempt ID, keeping recoveryActive locked.
   * 2. 'REINITIALIZE_CLEAN' (R8-03, R8-04, R9-06, R10-04, R11-D):
   *    - Quarantine Fate (R11-D): pre-reset quarantined operations permanently retired to TerminalQuarantineArchive
   *      (reason: REINITIALIZE_CLEAN_PURGED); strictly prohibited from replaying into clean baseline.
   *    - Preserves strict 5-step M4.3c sequence: (1) stale notification purge, (2) atomic clean baseline install,
   *      (3) engine.setRecoveryMode(false), (4) coordinator to ACTIVE, (5) non-throwing flushDeferredNotifications().
   *    - Zero-Notification Clarification (R10-04, R11-D): step 5 delivers zero notifications unless an explicit baseline
   *      state change is enqueued; clean baseline notification is not synthesized automatically.
   *    - Explicit failure boundary (R8-04): once ACTIVE is reached in step 4, state transition is externally
   *      observable; delivered listener effects cannot be rolled back. If pre-ACTIVE steps fail, catch block
   *      restores engine.setRecoveryMode(true), engine remains in DEGRADED, and notifications are NEVER flushed.
   * [PROPOSED — M5.1]
   */
  resetRecovery(
    capability: RecoveryInstallationCapability,
    resetAction: 'PURGE_AND_RETRY' | 'REINITIALIZE_CLEAN',
  ): void;

  /**
   * Prepares a detached immutable export batch of the terminal quarantine archive.
   * Enforces single-lease constraint (MAX_OUTSTANDING_EXPORTS = 1).
   * Does NOT clear or mutate the archive.
   * [PROPOSED — M5.1]
   */
  prepareArchiveExport(
    capability: TerminalArchiveAdministrationCapability,
  ): TerminalArchiveExportBatch;

  /**
   * Acknowledges successful external receipt and clears the exported archive entries.
   * Clears only the exact entry IDs included in the prepared export batch.
   * [PROPOSED — M5.1]
   */
  acknowledgeArchiveExport(
    exportId: string,
    capability: TerminalArchiveAdministrationCapability,
  ): TerminalArchiveExportReceipt;

  /**
   * Cancels an active outstanding export lease without clearing any archive entries.
   * [PROPOSED — M5.1]
   */
  cancelArchiveExport(
    exportId: string,
    capability: TerminalArchiveAdministrationCapability,
  ): void;

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

### 3.3 Terminal Quarantine Archive Governance, Atomic Retirement & Export Contracts (R12-A–R12-D, R13-A–R13-D, R14-A–R14-E)

```typescript
// ── Module Branding & Custody Symbols [PROPOSED — M5.1] ─────────────────────
const TERMINAL_ARCHIVE_BRAND: unique symbol = Symbol('TERMINAL_ARCHIVE_BRAND');

export interface TerminalArchiveAdministrationCapability {
  readonly [TERMINAL_ARCHIVE_BRAND]: true;
  readonly sessionId: string;
  readonly authority: 'terminal-archive-administration';
  readonly token: string;
}

// ── Composite Operation Identity [PROPOSED — M5.1] ──────────────────────────
export interface CompositeOperationIdentity {
  readonly originPeerId: string;    // Generating peer domain identifier
  readonly originSequence: number;  // Strictly monotonic sequence counter for originPeerId
  readonly membershipEpoch: number; // Membership configuration epoch
  readonly logicalEventId: string;  // Unique logical event identifier
}

/**
 * Returns deterministic canonical string representation:
 * op-id:${originPeerId}:${originSequence}:${membershipEpoch}:${logicalEventId}
 */
export function canonicalOperationIdentityKey(id: CompositeOperationIdentity): string {
  return `op-id:${id.originPeerId}:${id.originSequence}:${id.membershipEpoch}:${id.logicalEventId}`;
}

// ── Reset Attempt Lifecycle Tracking Types [PROPOSED — M5.1] ────────────────
export type ResetAttemptState =
  | 'INITIATED'              // Reset requested, monotonic attempt ID minted
  | 'WAITING_FOR_SETTLEMENT' // Drainer fenced, drainSettledBarrier awaited
  | 'SETTLED'                // In-flight settled, barrier resolved (N_in-flight == 0)
  | 'TIMED_OUT'              // Watchdog expired. Latched terminal failure state
  | 'INVALIDATED'            // Superseded by newer reset or abort. Latched failure
  | 'COMPLETED';             // Retirement and root replacement committed

export interface ResetAttemptRecord {
  readonly attemptId: number;
  readonly action: 'PURGE_AND_RETRY' | 'REINITIALIZE_CLEAN';
  status: ResetAttemptState;
  terminalReason?: string;
  readonly initiatedAtMs: number;
  settledAtMs?: number;
  completedAtMs?: number;
}

// ── Quarantine Retirement Transaction Types [PROPOSED — M5.1] ───────────────
export type QuarantineRetirementTransactionOutcome =
  | 'NOT_COMMITTED'                       // Initial state before commit invocation
  | 'COMMITTED'                           // Batch and transaction record published atomically
  | 'COMMIT_OUTCOME_UNKNOWN'              // Exception/timeout during commit; requires query
  | 'COMMITTED_SOURCE_RETIREMENT_PENDING' // Archive committed; source retirement executing
  | 'COMPLETED'                           // Source retired, zero-custody verified
  | 'FAILED';                             // Terminal failure; transaction aborted

export interface QuarantineRetirementTransaction {
  readonly retirementTransactionId: string;
  readonly quarantineGeneration: number;
  readonly operationIdentities: readonly CompositeOperationIdentity[];
  readonly entryCount: number;
  readonly completeArchiveEntryBytes: number; // Exact sum of RFC 8785 UTF-8 bytes of complete entries
  readonly archiveReservationId: string;
  readonly batchDigest: string;               // SHA-256 digest of canonically serialized staged entries
  status:
    | 'PREPARING'
    | 'RESERVED'
    | 'STAGED'
    | 'COMMITTED_ARCHIVE'
    | 'RETIRED_SOURCE'
    | 'COMPLETED'
    | 'FAILED'
    | 'ROLLED_BACK';
  terminalOutcome?:
    | 'SUCCESS'
    | 'CAPACITY_INSUFFICIENT'
    | 'GENERATION_MISMATCH'
    | 'INSERTION_FAILED'
    | 'ATTEMPT_FENCED'
    | 'TIMEOUT';
}

export interface TerminalBatchCommitReceipt {
  readonly committed: boolean;
  readonly alreadyCommitted: boolean;
  readonly transactionId: string;
  readonly entryCount: number;
  readonly totalSerializedBytes: number;
  readonly archiveGeneration: number;
  readonly batchDigest: string;
}

export interface QuarantineRetirementTransactionRecord {
  readonly transactionId: string;
  readonly outcome: QuarantineRetirementTransactionOutcome;
  readonly entryCount: number;
  readonly totalSerializedBytes: number;
  readonly batchDigest: string;
  readonly committedAtMs: number;
}

// ── Archive Export Types [PROPOSED — M5.1] ──────────────────────────────────
export interface TerminalArchiveExportBatch {
  readonly exportId: string;
  readonly entryIds: readonly string[];
  readonly archiveGeneration: number;
  readonly entryCount: number;
  readonly totalSerializedBytes: number;
  readonly canonicalDigest: string; // SHA-256 hex digest of RFC 8785 canonical JSON
  readonly entries: readonly TerminalQuarantineEntry[];
  readonly preparedAtMs: number;
}

export interface TerminalArchiveExportReceipt {
  readonly exportId: string;
  readonly acknowledged: boolean;
  readonly alreadyAcknowledged: boolean;
  readonly clearedCount: number;
  readonly remainingArchiveEntries: number;
  readonly remainingArchiveBytes: number;
}

export interface ArchiveExportTombstone {
  readonly exportId: string;
  readonly archiveGeneration: number;
  readonly entryIdsDigest: string;
  readonly acknowledgedAtMs: number;
  readonly clearedCount: number;
}

// ── Terminal Quarantine Archive Interface [PROPOSED — M5.1] ─────────────────
export interface ITerminalQuarantineArchive {
  readonly size: number;
  readonly totalBytes: number;
  readonly archiveGeneration: number;

  /**
   * Pre-reserves archive entry count and byte capacity against current archive generation.
   * Throws SyncEngineError('CAPACITY_INSUFFICIENT') if limits would be breached.
   * Throws SyncEngineError('GENERATION_MISMATCH') if expectedGeneration !== archiveGeneration.
   */
  reserveCapacity(
    entryCount: number,
    byteCount: number,
    expectedArchiveGeneration: number,
  ): string; // Returns reservationId

  /**
   * Cancels active reservation and releases reserved capacity.
   */
  cancelReservation(
    reservationId: string,
    capability: RecoveryInstallationCapability,
  ): void;

  /**
   * Idempotently commits a staged retirement batch under retirementTransactionId.
   */
  commitRetirementBatch(
    reservationId: string,
    stagedBatch: readonly TerminalQuarantineEntry[],
    retirementTransactionId: string,
  ): TerminalBatchCommitReceipt;

  /**
   * Queries committed transaction record for uncertain-outcome reconciliation.
   */
  getTransaction(
    retirementTransactionId: string,
  ): QuarantineRetirementTransactionRecord | undefined;

  /**
   * Checks whether the archive has already committed a retirement transaction.
   */
  hasTransaction(retirementTransactionId: string): boolean;

  /**
   * Prepares a detached immutable export batch. Enforces single-lease constraint.
   * Throws SyncEngineError('ARCHIVE_EXPORT_IN_PROGRESS') if an unacknowledged lease exists.
   */
  prepareArchiveExport(
    capability: TerminalArchiveAdministrationCapability,
  ): TerminalArchiveExportBatch;

  /**
   * Acknowledges export and clears ONLY the exact entry IDs included in the batch.
   */
  acknowledgeArchiveExport(
    exportId: string,
    capability: TerminalArchiveAdministrationCapability,
  ): TerminalArchiveExportReceipt;

  /**
   * Cancels active export lease without clearing any entries.
   */
  cancelArchiveExport(
    exportId: string,
    capability: TerminalArchiveAdministrationCapability,
  ): void;
}

// ── Enforceable Numeric Constants [PROPOSED — M5.1] ─────────────────────────
export const MAX_TERMINAL_ARCHIVE_ENTRIES = 500;
export const MAX_TERMINAL_ARCHIVE_TOTAL_BYTES = 2097152; // 2.0 MiB
export const MAX_TERMINAL_ENTRY_BYTES = 4096;           // 4.0 KiB
export const MAX_TERMINAL_ERROR_LENGTH = 512;
export const MAX_TERMINAL_REASON_LENGTH = 256;
export const MAX_TERMINAL_PEER_ID_LENGTH = 128;
export const MAX_TERMINAL_LOGICAL_EVENT_ID_LENGTH = 128;
export const MAX_OUTSTANDING_EXPORTS = 1;
export const MAX_EXPORT_BATCH_TOTAL_BYTES = 2097152;     // 2.0 MiB
export const MAX_ACKNOWLEDGEMENT_TOMBSTONES = 50;
export const TOMBSTONE_TTL_MS = 600000;                 // 10 minutes
export const RESERVATION_LEASE_TIMEOUT_MS = 10000;       // 10 seconds
export const EXPORT_LEASE_TIMEOUT_MS = 30000;            // 30 seconds
export const SETTLEMENT_BARRIER_TIMEOUT_MS = 5000;       // 5.0 seconds
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
| `UNAUTHORIZED` | LeaderlessSyncEngine | Capability custody enforced by object identity | Throws `SyncEngineError` if capability does not match `authorizedRecoveryCapability` or `DrainAdmissionContext`. |
| `INVALID_STATE` | Coordinator | State guard for administrative recovery resets | Throws `SyncEngineError` if `resetRecovery()` is invoked from non-`DEGRADED` state. |
