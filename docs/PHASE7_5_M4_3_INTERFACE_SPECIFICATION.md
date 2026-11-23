# Phase 7.5 M4.3 — Recovery Orchestration & Lifecycle Integration Interface Specification

**Author:** Senior TypeScript Distributed-Systems Architect  
**Status:** M4.3 ARCHITECTURE READY FOR FINAL ACCEPTANCE — IMPLEMENTATION UNAUTHORIZED  
**Mode:** STRICTLY READ-ONLY SPECIFICATION (IMPLEMENTATION UNAUTHORIZED)  
**Milestone:** Phase 7.5 Sub-Milestone M4.3  
**Date:** November 24, 2026  
**Revision:** R4 Final Ownership Closure  
**Authoritative Baseline Commit:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Direct Parent:** `79919a0f8cb0bb81cb80cc010fe02918bef79787` (M4.2c-R6 closure)  
**Parent of Direct Parent:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5 closure)  

---

## 1. Overview & Architectural Purpose

This specification defines the exact TypeScript types, interfaces, error codes, and configuration parameters for Sub-Milestone M4.3: **Recovery Orchestration & Lifecycle Integration**.

This R4 revision establishes:
1. Structurally controlled single-root container (`EngineCoreState`) with shallow container sealing (`Object.seal`), deep recursive freeze on state projections, engine-confined mutation, and candidate creator dereferencing.
2. Explicit in-flight drain ownership model (`DRAINER_IN_FLIGHT`) with $N_{\text{in-flight}} \le 1$ cardinality and typed destination confirmation receipts (`DestinationAcceptanceReceipt`).
3. 8-state operation lifecycle taxonomy for partial drain resumption (`DrainedOperationState`), guaranteeing zero lost operations on capacity rejections or exceptions.
4. Complete reentrancy exclusion contracts, deferring both observer listeners and outbound repair dispatches outside the critical transition gate.
5. Dual-constraint bounded quarantine buffer (`IQuarantineBuffer`) with generational handoff.
6. Dedicated quarantine drainer contract (`IQuarantineDrainer`) with in-flight record lifecycle and unshift recovery.
7. Bounded `FutureEpochBuffer` contract (100 ops, 512 KiB) with 5s capability grace period.
8. Four-stage atomic snapshot installer contracts (`ISnapshotInstaller`).
9. Master recovery coordinator contracts (`IRecoveryCoordinator`, `RecoveryCoordinatorConfig`).
10. Honest memory boundaries separating enforced programmatic limits from analytical estimates.

---

## 2. Recovery State Machine Types & Events

### 2.1 States and Substates

```typescript
/**
 * Top-level recovery states governing the local node's synchronization lifecycle.
 */
export type RecoveryState = 
  | 'ACTIVE'       // Normal operation: local and remote operations processed in real-time
  | 'RECOVERING'   // Catching up: transferring & validating snapshot, local inputs blocked, remote quarantined
  | 'RECONCILING'  // Single-root snapshot committed: draining quarantine buffer, local inputs blocked
  | 'DEGRADED';    // Recovery failed or causal divergence detected: frozen safely, awaiting manual reset

/**
 * Granular internal execution substates for fine-grained observability.
 */
export type RecoverySubstate =
  | 'IDLE'                 // State is ACTIVE or DEGRADED, no transfer in flight
  | 'AWAITING_OFFER'       // Recovery triggered, awaiting admissible snapshot offer
  | 'TRANSFERRING'         // SnapshotInboundPipeline actively requesting/receiving chunks
  | 'VALIDATING'           // Stage 1 dry-run structural and semantic validation
  | 'STAGING'              // Stage 2 pre-constructing candidate EngineCoreState out-of-band
  | 'COMMITTING'           // Stage 3 single-root pointer replacement turn
  | 'DRAINING_QUARANTINE'  // Stage 4 filtering and replaying quarantined remote operations
  | 'RESOLVING_GAPS';      // Stage 4 post-drain anti-entropy gap repair
```

### 2.2 Operation Lifecycle State Taxonomy & In-Flight Contracts (R4-1 & R4-2)

```typescript
/**
 * Formal lifecycle states distinguishing ownership and progress of individual operations
 * during recovery and quarantine drain.
 */
export type DrainedOperationState =
  | 'NOT_YET_DRAINED'                   // Buffered in QuarantineBuffer (activeQueue or drainBatch)
  | 'DRAINER_IN_FLIGHT'                 // Owned by QuarantineDrainer; undergoing destination admission
  | 'SUCCESSFULLY_REDUCED'              // Destination (OrderedOperationLog) confirmed; vector advanced
  | 'SUCCESSFULLY_INSERTED_PENDING'     // Destination (PendingDependencyQueue) confirmed; awaiting gap
  | 'ALREADY_REPRESENTED_BY_FRONTIER'   // seq <= installedFrontier; discarded safely
  | 'ALREADY_OWNED_DUPLICATE'           // Already present in log or pendingQueue; discarded safely
  | 'TERMINALLY_REJECTED'               // Malformed, invalid schema, or obsolete epoch; discarded
  | 'RECOVERABLE_DESTINATION_REJECTION';// Capacity rejection by destination; preserved in inFlightRecord

/**
 * Detailed status of an in-flight operation during drain processing.
 */
export type DestinationAcceptanceStatus =
  | 'PENDING'
  | 'ACCEPTED_REDUCED'
  | 'ACCEPTED_PENDING'
  | 'ALREADY_REPRESENTED'
  | 'ALREADY_OWNED_DUPLICATE'
  | 'TERMINALLY_REJECTED'
  | 'CAPACITY_REJECTED';

/**
 * Authoritative in-flight tracking record owned exclusively by QuarantineDrainer.
 * Cardinality is bounded to exactly ONE operation in flight at any instant.
 */
export interface DrainedInFlightRecord {
  readonly operation: QuarantinedOperation;
  readonly dequeuedAtMs: number;
  attemptCount: number;
  destinationStatus: DestinationAcceptanceStatus;
}

/**
 * Synchronous confirmation receipt returned by the destination engine container
 * upon receiving an in-flight operation from QuarantineDrainer.
 */
export type DestinationAcceptanceReceipt =
  | { readonly status: 'ACCEPTED_REDUCED'; readonly vectorAdvanced: true; readonly newSequence: number }
  | { readonly status: 'ACCEPTED_PENDING'; readonly vectorAdvanced: false; readonly reason: 'MISSING_DEPENDENCY' }
  | { readonly status: 'ALREADY_REPRESENTED'; readonly vectorAdvanced: false }
  | { readonly status: 'ALREADY_OWNED_DUPLICATE'; readonly vectorAdvanced: false }
  | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string }
  | { readonly status: 'CAPACITY_REJECTED'; readonly vectorAdvanced: false; readonly currentQueueSize: number; readonly maxCapacity: number };
```

### 2.3 Candidate Eligibility Taxonomy

```typescript
export type CandidateEligibilityClassification =
  | 'IDENTICAL'                 // Vectors identical and history verified; no recovery needed
  | 'FRONTIER_AGREEMENT_ONLY'   // Frontiers match but history unverified; cannot overwrite state
  | 'STRICTLY_DOMINATES'        // Candidate strictly dominates local state; ADMISSIBLE
  | 'INCOMPARABLE_CONCURRENT'   // Vectors concurrent; arbitrary tie-breaking strictly prohibited
  | 'DOMINATED_BY_LOCAL'        // Candidate is behind local node; rejected
  | 'OLDER_EPOCH'               // Candidate offer membershipEpoch < local engine epoch; rejected
  | 'FUTURE_EPOCH'              // Candidate offer membershipEpoch > local engine epoch; buffered
  | 'REPAIRABLE_WITHIN_HORIZON' // History gap <= 256; handled via AE log repair, not snapshot
  | 'OUTSIDE_RETAINED_HORIZON'; // History gap compacted out; requires snapshot recovery
```

### 2.4 Failure Reasons & Error Taxonomy

```typescript
export type RecoveryFailureReason =
  | 'NO_OFFERS_AVAILABLE'        // No peers responded with an offer after timeout (10s)
  | 'ALL_CANDIDATES_EXHAUSTED'   // All eligible candidates failed or disconnected (max 8)
  | 'OVERALL_RECOVERY_TIMEOUT'   // Recovery exceeded max allowed duration (120s)
  | 'RECONCILIATION_TIMEOUT'     // Quarantine drain exceeded max allowed duration (15s)
  | 'QUARANTINE_OVERFLOW'        // QuarantineBuffer exceeded count (1,000) or byte limit (4 MiB)
  | 'FUTURE_EPOCH_OVERFLOW'      // FutureEpochBuffer exceeded count (100) or byte limit (512 KiB)
  | 'PAYLOAD_CORRUPTED'          // Reassembled snapshot payload failed JSON parse or RFC 8785 check
  | 'CAUSAL_DIVERGENCE'          // Snapshot frontier is strictly concurrent or contradictory to local state
  | 'EPOCH_MISMATCH'             // Snapshot membership epoch < current engine epoch
  | 'UNRESOLVED_FUTURE_EPOCH'    // Future epoch operations timed out without EpochTransitionCapability (5s)
  | 'ENGINE_DISPOSED'            // Subsystem disposed while recovery in flight
  | 'APPLICATION_ABORT';         // External application requested recovery abort
```

---

## 3. Single-Root Engine Core State Container (R4-4)

To eliminate partial multi-field assignment errors during synchronous execution, all mutable synchronization state of `LeaderlessSyncEngine` is unified under a single structurally sealed root container:

```typescript
import type { PlaybackState } from '@syncplay/protocol';
import type { OrderedOperationLog } from '../log/ordered-operation-log.js';
import type { LogicalClockManager } from '../clock/logical-clock-manager.js';
import type { PendingDependencyQueue } from '../log/pending-dependency-queue.js';

/**
 * Coherent root container representing the complete mutable execution state
 * of LeaderlessSyncEngine.
 * 
 * OWNERSHIP & IMMUTABILITY CONTRACT (R4-4):
 * 1. Shallow Sealing: Sealed via Object.seal(core). This prevents adding or deleting
 *    top-level properties, but does NOT recursively seal nested objects.
 * 2. Deeply Frozen Projections: playbackState and baseSnapshot are deeply frozen
 *    via Object.freeze().
 * 3. Encapsulated Mutation: Nested mutable structures (operationLog, pendingQueue,
 *    clockManager, replicationVector) are mutated STRICTLY by private engine methods.
 * 4. Zero Mutable Aliases: External accessors return frozen copies or readonly projections.
 * 5. Candidate Creator Dereferencing: Upon handoff, creator drops all local references.
 * 6. GC Eligibility: Old root becomes eligible for GC only after all references drop;
 *    deterministic GC is NOT guaranteed.
 */
export interface EngineCoreState {
  readonly state: PlaybackState;
  readonly baseSnapshot: PlaybackState;
  readonly baseSnapshotFrontier: Readonly<Record<string, number>>;
  readonly operationLog: OrderedOperationLog;
  readonly clockManager: LogicalClockManager;
  readonly pendingQueue: PendingDependencyQueue;
  readonly replicationVector: Readonly<Record<string, number>>;
  readonly localCausalContext: Readonly<Record<string, number>>;
  readonly localOriginSequence: number;
  readonly currentEpoch: number;
  readonly lastAppliedEventId?: string;
  readonly stateVersion: number;
}
```

---

## 4. Quarantine Buffer Contracts, In-Flight Drainer & Generational Handoff (R4-1 & R4-2)

The quarantine buffer provides a dual-constraint bounded container ($N \le 1000$, $B \le 4 \text{ MiB}$) with double-buffered generational handoff, an auxiliary `FutureEpochBuffer` ($N \le 100$, $B \le 512\text{ KiB}$), and an authoritative in-flight drainer component (`IQuarantineDrainer`).

### 4.1 Data Structures

```typescript
import type { AnyControlSyncEvent } from '@syncplay/protocol';

/**
 * Envelope for remote operations captured while recovery is in progress.
 */
export interface QuarantinedOperation {
  readonly event: AnyControlSyncEvent;
  readonly originPeerId: string;
  readonly sequenceNumber: number;
  readonly membershipEpoch: number;
  readonly serializedByteLength: number;
  readonly receivedAtMs: number;
}

/**
 * Summary metrics of a quarantine drain execution.
 */
export interface QuarantineDrainResult {
  readonly totalOperations: number;
  readonly appliedCount: number;
  readonly acceptedPendingCount: number;
  readonly discardedMalformedEpoch: number;
  readonly discardedObsoleteEpoch: number;
  readonly discardedAlreadyApplied: number;
  readonly bufferedFutureEpoch: number;
  readonly drainDurationMs: number;
}
```

### 4.2 Quarantine Buffer Interface

```typescript
export interface IQuarantineBuffer {
  /**
   * Current number of operations buffered in active queue.
   */
  readonly size: number;

  /**
   * Total serialized UTF-8 bytes retained in active queue.
   */
  readonly totalRetainedBytes: number;

  /**
   * Number of future-epoch operations buffered.
   */
  readonly futureEpochSize: number;

  /**
   * Total serialized UTF-8 bytes in future-epoch buffer.
   */
  readonly futureEpochBytes: number;

  /**
   * Maximum allowed count before overflow (1,000).
   */
  readonly countCapacity: number;

  /**
   * Maximum allowed byte total before overflow (4 MiB).
   */
  readonly byteCapacity: number;

  /**
   * Enqueues an incoming remote operation into quarantine.
   * Rejects if event serialized byte length > 4,096 bytes.
   * Throws Error with code 'QUARANTINE_OVERFLOW' if size >= countCapacity OR bytes >= byteCapacity.
   */
  enqueue(op: QuarantinedOperation): void;

  /**
   * Atomically swaps active queue into drain batch in a single synchronous JS turn.
   * New operations entering during drain are placed in the fresh active queue.
   */
  swapDrainBatch(): readonly QuarantinedOperation[];

  /**
   * Retains an operation from a future membership epoch in FutureEpochBuffer.
   * Throws Error with code 'FUTURE_EPOCH_OVERFLOW' if size >= 100 or bytes >= 512 KiB.
   */
  bufferFutureEpoch(op: QuarantinedOperation): void;

  /**
   * Clears all quarantined operations across active, drain, and future buffers.
   */
  clear(): void;
}
```

### 4.3 Quarantine Drainer Interface (R4-1 In-Flight Ownership)

```typescript
/**
 * Authoritative drain execution component responsible for sequential operation admission,
 * in-flight ownership tracking, and destination confirmation verification.
 */
export interface IQuarantineDrainer {
  /**
   * Authoritative in-flight tracking record.
   * Cardinality is bounded to at most 1 operation in flight at any instant.
   */
  readonly inFlightRecord: DrainedInFlightRecord | null;

  /**
   * Executes a sequential drain pass over the supplied batch.
   * Acquires in-flight ownership before destination submission, verifies receipt,
   * and clears in-flight record only upon confirmation.
   */
  drainBatch(batch: readonly QuarantinedOperation[]): Promise<QuarantineDrainResult>;

  /**
   * Resumes an interrupted drain pass, inspecting inFlightRecord against destination.
   */
  resumeDrain(): void;

  /**
   * Restores an unconfirmed in-flight operation to the head of the active queue (via unshift).
   */
  restoreInFlightToQueue(): void;

  /**
   * Explicitly clears the in-flight record upon confirmed destination acceptance.
   */
  clearInFlight(): void;
}
```

---

## 5. Snapshot Installer Contracts

The `SnapshotInstaller` performs the four-stage validation, staging, and atomic commit sequence.

### 5.1 Data Structures

```typescript
import type { StateSnapshotPayloadV2, PlaybackState } from '@syncplay/protocol';

/**
 * Result of Stage 1 out-of-band dry-run validation.
 */
export interface SnapshotValidationResult {
  readonly valid: boolean;
  readonly errorReason?: string;
  readonly parsedPayload?: StateSnapshotPayloadV2;
  readonly computedDigest?: string;
}

/**
 * Fully constructed, ready-to-swap EngineCoreState container from Stage 2.
 */
export interface StagedSnapshotInstallation {
  readonly payload: StateSnapshotPayloadV2;
  readonly canonicalDigest: string;
  readonly targetEpoch: number;
  readonly targetFrontier: Record<string, number>;
  readonly candidateCore: EngineCoreState;
}

/**
 * Final execution report returned after Stage 3 commit.
 */
export interface SnapshotInstallationReport {
  readonly success: boolean;
  readonly installedEpoch: number;
  readonly installedFrontier: Record<string, number>;
  readonly installedMaxLogicalTimestamp: number;
  readonly commitDurationMs: number;
}
```

### 5.2 Snapshot Installer Interface

```typescript
import type { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';

export interface ISnapshotInstaller {
  /**
   * STAGE 1: Pure out-of-band validation.
   * Performs JSON parsing, schema checks, RFC 8785 digest verification,
   * epoch fencing, and strict frontier dominance checks.
   * Modifies ZERO engine state.
   */
  validate(
    rawPayload: string,
    currentEpoch: number,
    currentReplicationVector: Record<string, number>
  ): Promise<SnapshotValidationResult>;

  /**
   * STAGE 2: Pre-construct candidate EngineCoreState and all sub-objects out-of-band.
   * All potentially throwing operations (checkpoint ingestion, allocations) execute here.
   * Seals the candidate core (Object.seal) and releases creator references upon completion.
   * Modifies ZERO engine state.
   */
  stage(
    validationResult: SnapshotValidationResult,
    engineConfig: unknown
  ): StagedSnapshotInstallation;

  /**
   * STAGE 3: Irreversible Synchronous Atomic Commit Point.
   * Replaces engine state root in a single synchronous reference assignment:
   *   engine.replaceCoreStateAtomic(staged.candidateCore);
   * CANNOT throw. Suppresses immediate listener emissions.
   */
  commitAtomic(
    engine: LeaderlessSyncEngine,
    staged: StagedSnapshotInstallation
  ): SnapshotInstallationReport;
}
```

---

## 6. Recovery Coordinator Contracts & Reentrancy Isolation (R3-2)

The `RecoveryCoordinator` is the central orchestration component.

### 6.1 Configuration & Callbacks

```typescript
export interface RecoveryCoordinatorConfig {
  readonly overallRecoveryTimeoutMs?: number;
  readonly reconciliationTimeoutMs?: number;
  readonly offerAwaitTimeoutMs?: number;
  readonly epochConvergenceGraceMs?: number;
  readonly quarantineMaxOperations?: number;
  readonly quarantineMaxBytes?: number;
  readonly maxIndividualEventBytes?: number;
}

export interface RecoveryCoordinatorCallbacks {
  readonly onStateChange?: (newState: RecoveryState, oldState: RecoveryState, reason: string) => void;
  readonly onSubstateChange?: (newSubstate: RecoverySubstate, oldSubstate: RecoverySubstate) => void;
  readonly onQuarantineWarning?: (currentBytes: number, currentCount: number) => void;
  readonly onRecoveryDegraded?: (reason: RecoveryFailureReason, error?: Error) => void;
  readonly onRecoveryCompleted?: (metrics: RecoveryMetrics) => void;
}

export interface RecoveryMetrics {
  readonly totalDurationMs: number;
  readonly transferDurationMs: number;
  readonly drainDurationMs: number;
  readonly chunksReceived: number;
  readonly failoversCount: number;
  readonly quarantinedApplied: number;
  readonly quarantinedDiscarded: number;
}
```

### 6.2 Recovery Coordinator Interface

```typescript
import type { AnyControlSyncEvent, SnapshotOfferMessage } from '@syncplay/protocol';
import type { AuthenticatedSupplierContext } from './snapshot-inbound-pipeline.js';

export interface IRecoveryCoordinator {
  readonly state: RecoveryState;
  readonly substate: RecoverySubstate;
  readonly isDisposed: boolean;

  start(): void;
  dispose(): void;

  handleSnapshotOffer(
    offer: SnapshotOfferMessage,
    context: AuthenticatedSupplierContext
  ): void;

  interceptRemoteEvent(
    event: AnyControlSyncEvent,
    rawSerializedLength: number
  ): { shouldQuarantine: boolean; dropped?: boolean };

  /**
   * Intercepts local user intents. During RECOVERING, RECONCILING, or during handoff,
   * returns { allowed: false, code: 'REJECTED_RECOVERING' } (or throws for intents).
   * Local submissions are NOT queued.
   */
  interceptLocalSubmission(): { allowed: boolean; code?: string };

  triggerRecovery(peerId: string, reason: string): void;
  resetRecovery(initiatedBy: string): void;
}
```

---

## 7. Engine Integration Hooks (`LeaderlessSyncEngine`)

The `LeaderlessSyncEngine` requires dedicated methods for single-root state replacement, local event gating, and deferred observer flushing:

```typescript
export interface IRecoveryCompatibleEngine {
  /**
   * Single-root atomic state replacement in 1 synchronous reference assignment:
   *   this.core = candidateCore;
   * CANNOT throw under run-to-completion. Suppresses immediate listener notifications.
   */
  replaceCoreStateAtomic(candidate: EngineCoreState): void;

  /**
   * Evaluates an in-flight quarantined operation during Stage 4 drain.
   * Confirms destination acceptance or returns explicit typed receipt.
   * Advancing replication vector occurs strictly upon first successful reduction into log.
   */
  admitDrainedOperation(op: QuarantinedOperation): DestinationAcceptanceReceipt;

  /**
   * Flushes deferred state change and decision notifications after Stage 4 drain completes.
   * Executes individual listener calls in try/catch to ensure reentrancy safety.
   */
  flushDeferredNotifications(): void;

  /**
   * Queries whether recovery is currently active on the engine.
   */
  isRecoveryActive(): boolean;

  /**
   * Sets recovery mode on the engine to enforce local input rejection and listener buffering.
   */
  setRecoveryMode(active: boolean): void;
}
```

---

## 8. Protocol Constants & Honest Memory Boundaries (R3-3)

```typescript
export const RECOVERY_CONSTANTS = {
  // Hard Enforced Programmatic Limits
  MAX_QUARANTINE_OPERATIONS: 1000,
  MAX_QUARANTINE_TOTAL_BYTES: 4 * 1024 * 1024, // 4 MiB (4,194,304 bytes)
  MAX_QUARANTINE_EVENT_BYTES: 4096,            // 4 KiB (4,096 bytes)
  MAX_FUTURE_EPOCH_OPERATIONS: 100,
  MAX_FUTURE_EPOCH_BYTES: 512 * 1024,          // 512 KiB (524,288 bytes)
  MAX_PENDING_QUEUE_OPERATIONS: 256,           // From PendingDependencyQueue

  // Telemetry Thresholds & Timeouts
  QUARANTINE_WARNING_THRESHOLD_BYTES: Math.floor(4 * 1024 * 1024 * 0.8), // ~3.2 MiB
  DEFAULT_OVERALL_RECOVERY_TIMEOUT_MS: 120_000,
  DEFAULT_RECONCILIATION_TIMEOUT_MS: 15_000,
  DEFAULT_OFFER_AWAIT_TIMEOUT_MS: 10_000,
  DEFAULT_EPOCH_CONVERGENCE_GRACE_MS: 5_000,

  // Standard Error Codes
  REJECTED_RECOVERING_CODE: 'REJECTED_RECOVERING' as const,
  QUARANTINE_OVERFLOW_CODE: 'QUARANTINE_OVERFLOW' as const,
  FUTURE_EPOCH_OVERFLOW_CODE: 'FUTURE_EPOCH_OVERFLOW' as const,

  // Analytical Estimates (Excludes transient decode buffers & GC latency)
  ESTIMATED_RETAINED_HEAP_BYTES_STEADY_STATE: 12_600_000, // ~12.6 MiB analytical estimate
} as const;
```

---

## 9. Summary of Contract Boundaries (R4 Final Ownership Closure)

1. **Candidate Eligibility:** Reconciled with P7.4 digest classifications. Strict vector dominance is required; incomparable concurrent histories are rejected without arbitrary tie-breaking.
2. **Single-Root Swap & Ownership (R4-4):** `replaceCoreStateAtomic()` executes via single synchronous reference assignment (`this.core = candidateCore`). Shallow container sealing (`Object.seal`); deep recursive freeze on state projections; engine-confined mutation; creator explicitly drops mutable aliases post-handoff; old root becomes eligible for GC without false claims of deterministic garbage collection.
3. **In-Flight Drain Ownership & 8-State Taxonomy (R4-1 & R4-2):** Dedicated `DRAINER_IN_FLIGHT` ownership state with $N_{\text{in-flight}} \le 1$ cardinality. Full 6-step lifecycle with explicit `DestinationAcceptanceReceipt` confirmation before release, guaranteeing zero operation loss on crash, capacity rejection, or transition to `DEGRADED`.
4. **Complete Reentrancy Exclusion (R3-2):** Observers and outbound repair messages are deferred outside the critical transition gate. Local submissions are rejected with `REJECTED_RECOVERING`.
5. **Honest Memory Accounting (R3-3):** Transient decode buffer coexistence acknowledged. Clear separation between enforced programmatic limits ($N \le 1000, B \le 4\text{ MiB}$) and analytical heap estimates ($\approx 12.6\text{ MiB}$).
