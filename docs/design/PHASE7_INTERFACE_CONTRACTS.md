# Phase 7.2 — Leaderless SyncEngine Interface Contracts

> **Document Version:** 1.0.0  
> **Status:** SPECIFICATION ONLY — NO IMPLEMENTATION AUTHORIZED  
> **Parent Milestone:** Phase 7.2 (Architecture & Interface Design)  
> **Baseline Commit:** `926c575`  
> **Target Package:** `@syncplay/sync-engine` (Contract definitions only; do not modify source code)  

---

## 1. Public SyncEngine Contract Compatibility

The `LeaderlessSyncEngine` strictly implements the existing public `SyncEngine` contract and extends the Phase 5 `SyncEventReceiver` contract. Existing callers (`ControlEventService` and `apps/demo`) interact with the leaderless engine through the identical TypeScript interface.

```ts
import type { AnyControlSyncEvent, SyncEventReceiver } from '@syncplay/control-events';
import type { StateSnapshotPayload } from '@syncplay/protocol';
import type {
  PlaybackState,
  EngineDecision,
  EngineStatus,
  EngineStats,
  StateChangeListener,
  DecisionListener,
  Unsubscribe,
} from '../types.js';

/**
 * Public facade interface implemented by LeaderlessSyncEngine.
 * 100% backward-compatible with BaselineSyncEngine.
 */
export interface SyncEngine extends SyncEventReceiver {
  readonly isDisposed: boolean;

  /**
   * Consumes an incoming control event (Phase 5 boundary).
   * Throws SyncEngineError on local validation failure; isolates on remote.
   */
  receive(event: AnyControlSyncEvent): void;

  /** Returns current locally authoritative state. */
  getState(): Readonly<PlaybackState>;

  /** Computes continuous position from anchor and clock. */
  getCurrentPositionMs(atTimestamp?: number): number;

  /** Returns current engine status and lifecycle state. */
  getStatus(): EngineStatus;

  /** Returns immutable copy of diagnostic stats. */
  getStats(): Readonly<EngineStats>;

  /** Returns latest event decision. */
  getLastDecision(): Readonly<EngineDecision> | undefined;

  /** State mutation subscriber. */
  onStateChange(listener: StateChangeListener): Unsubscribe;

  /** Decision subscriber. */
  onDecision(listener: DecisionListener): Unsubscribe;

  /** Export state snapshot conforming to StateSnapshotPayload. */
  exportSnapshot(snapshotId?: string): StateSnapshotPayload;

  /** Import snapshot atomically. */
  importSnapshot(payload: StateSnapshotPayload): EngineDecision;

  /** Terminal lifecycle disposal. */
  dispose(): void;
}
```

---

## 2. Internal Subsystem Interface Specifications

---

### 2.1. `LogicalClockManager` Contract

- **Ownership:** Owns local integer Lamport logical clock counter.
- **Dependency Direction:** Core utility; depends only on basic math and config.

```ts
export interface ILogicalClockManager {
  /** Current local Lamport logical counter. */
  readonly clock: number;

  /**
   * Advances the local clock for an outbound operation:
   * L = 1 + max(L_local, max_deps L).
   * Throws SyncEngineError('CLOCK_OVERFLOW') if L >= Number.MAX_SAFE_INTEGER - 1.
   */
  tickLocal(dependencies?: ReadonlyArray<{ logicalTimestamp: number }>): number;

  /**
   * Validates and advances local clock upon receiving a remote operation.
   * Advances: L = max(L_local, remoteClock) (without +1; receiving an event is not a local event creation).
   * Validates non-negative integer, per-origin monotonicity, and safe integer bounds.
   * The caller evaluates dependencies in causalContext across the 3 tiers (Log, Compacted Checkpoints, or PendingBuffer).
   * If any causal dependency is missing (s > replicationVector[p]), caller MUST buffer in PendingDependencyQueue
   * and MUST NOT call validateAndAdvanceRemote (clock does not advance).
   * Exact validation invariant: remoteClock MUST EQUAL 1 + max(lastOriginClock, maxDepClock).
   * Receiver isolation: Validation evaluates dependencies strictly from causalContext; receiver concurrent observations are never substituted.
   * Frontier merge: Upon admission, caller updates replicationVector and localCausalContext component-wise for originPeerId, preserving all other origins.
   * Throws SyncEngineError('INVALID_CAUSAL_TIMESTAMP') if remoteClock < expected.
   * Throws SyncEngineError('INVALID_LOGICAL_TIMESTAMP') if remoteClock > expected (unwarranted clock inflation).
   * When context === 'LIVE_BROADCAST': strictly evaluates token-bucket arrival throttling ($10\text{ ops/sec}$, burst $50$).
   * When context === 'REPAIR_BATCH': bypasses token bucket rate limiter, but strictly evaluates all causal, monotonic, and integer bounds.
   * When context === 'LOCAL_REPLAY': trusted internal replay of already accepted immutable events; returns current clock without mutation.
   * Critical safety invariant: If validation fails, the local clock is NEVER advanced.
   */
  validateAndAdvanceRemote(
    originPeerId: string,
    remoteClock: number,
    dependencyClocks: ReadonlyArray<number>,
    monotonicNow: number,
    membershipEpoch: number,
    context?: 'LIVE_BROADCAST' | 'REPAIR_BATCH' | 'LOCAL_REPLAY'
  ): number;

  /**
   * Reseeds clock upon reconnect or snapshot recovery.
   * Sets: L = max(L_local, maxObservedClock) (without an extra +1, so subsequent tickLocal evaluates exactly to 1 + maxObservedClock).
   */
  reseed(maxObservedClock: number): void;
}
```

---

### 2.2. `OrderedOperationLog` Contract

- **Ownership:** Owns in-memory storage, total-order sorting, deduplication, and compaction of historical operations.
- **Capacity Horizon:** $K = 256$ operations maximum.

```ts
export interface StoredOperation {
  readonly logicalEventId: string;
  readonly originPeerId: string;
  /** In live broadcasts, senderPeerId MUST equal originPeerId (Strict No-Forwarding Policy). In repair batches, senderPeerId reflects the supplierPeerId. */
  readonly senderPeerId: string;
  readonly membershipEpoch: number;
  readonly messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE';
  readonly logicalTimestamp: number;
  readonly originSequenceNumber: number;
  readonly causalContext: Record<string, number>;
  readonly appliedAtMonotonic: number;
  readonly payload: unknown;
}

export interface InsertOperationResult {
  /** True if inserted into log; false if duplicate or outside horizon. */
  readonly inserted: boolean;
  /** True if appended to head; false if inserted earlier requiring rollback. */
  readonly isHead: boolean;
  /** Index where inserted in sorted log array. */
  readonly index: number;
  /** Specific outcome code conforming to the 5-way horizon classification. */
  readonly code:
    | 'INSERTED_HEAD'
    | 'INSERTED_REORDER'
    | 'IGNORED_DUPLICATE'
    | 'IGNORED_ALREADY_COMPACTED'
    | 'BUFFERED_PENDING_DEPENDENCY'
    | 'REJECTED_OUTSIDE_HORIZON'
    | 'REJECTED_INVALID_ORIGIN'
    | 'REJECTED_SEQUENCE_CONFLICT'
    | 'REJECTED_OBSOLETE_EPOCH';
}

export interface IOrderedOperationLog {
  /** Current count of operations retained in log (<= 256). */
  readonly size: number;

  /** Returns true if an operation with logicalEventId is already in log or index. */
  has(logicalEventId: string): boolean;

  /** Returns operation by logicalEventId or undefined. */
  get(logicalEventId: string): StoredOperation | undefined;

  /**
   * Inserts an operation in total-order sorted position.
   * Throws SyncEngineError if capacity exceeded without compaction.
   */
  insert(operation: StoredOperation): InsertOperationResult;

  /** Returns all retained operations in strict total order. */
  getAllOperations(): ReadonlyArray<StoredOperation>;

  /** Returns slice of operations originating from originPeerId in range [fromSeq, toSeq]. */
  getRange(originPeerId: string, fromSeq: number, toSeq: number): ReadonlyArray<StoredOperation>;

  /**
   * Retrieves the exact Lamport timestamp for a sequence compacted into the base state.
   * Returns undefined if sequence predates the retained checkpoint horizon (K = 256).
   */
  getCompactedCheckpoint(originPeerId: string, sequenceNumber: number): number | undefined;

  /**
   * Prunes operations older than stable frontier sequence numbers.
   * Folds pruned operations into base snapshot.
   */
  compactAtFrontier(stableFrontier: Record<string, number>): number;

  /** Resets log completely and initializes new base snapshot. */
  reset(baseSnapshot: PlaybackState): void;

  /** Clears all memory upon disposal. */
  dispose(): void;
}
```

---

### 2.3. `ConflictResolver` Contract

- **Ownership:** Pure functional state reducer implementing operation-specific conflict semantics.
- **Purity Guarantee:** Zero side-effects; never mutates input parameters; deterministic output for same inputs.

```ts
export interface ReducerContext {
  readonly sessionId: string;
  readonly localPeerId: string;
  readonly minPlaybackRate: number;
  readonly maxPlaybackRate: number;
  readonly positionEpsilonMs: number;
}

export interface IConflictResolver {
  /**
   * Pure functional reduction of a single operation against current playback state.
   */
  reduce(
    currentState: PlaybackState,
    operation: StoredOperation,
    monotonicNow: number,
    context: ReducerContext
  ): { nextState: PlaybackState; decision: EngineDecision };

  /**
   * Full replay from base snapshot across an ordered slice of operations.
   * Used when late operations trigger speculative rollback.
   */
  replay(
    baseState: PlaybackState,
    operations: ReadonlyArray<StoredOperation>,
    monotonicNow: number,
    context: ReducerContext
  ): { finalState: PlaybackState; decisions: ReadonlyArray<EngineDecision> };
}
```

---

### 2.4. `ReplicationController` Contract

- **Ownership:** Drives anti-entropy state digest exchange and targeted operation repair.

```ts
export interface IReplicationController {
  /** Current local Replication Vector (originPeerId -> highestConsecutiveSeq received and reduced). */
  readonly replicationVector: Readonly<Record<string, number>>;

  /** Generates a StateDigestPayload to announce local contiguous frontier. */
  createDigest(): StateDigestPayload;

  /**
   * Evaluates an incoming remote digest against local replication vector.
   * Returns array of RepairRequests needed to fill local gaps.
   */
  processRemoteDigest(digest: StateDigestPayload): ReadonlyArray<RepairRequestPayload>;

  /**
   * Fulfills an incoming RepairRequest by querying OrderedOperationLog.
   * Returns OperationBatchPayload (bounded to max 32 operations).
   */
  fulfillRepairRequest(request: RepairRequestPayload): OperationBatchPayload;

  /** Increments local sequence counter for a new locally originated operation. */
  nextLocalSequence(): number;

  /**
   * Updates replication vector upon consecutively receiving and reducing an operation.
   * Out-of-order operations do NOT advance this vector.
   */
  recordOperationReduced(originPeerId: string, sequenceNumber: number): void;

  /** Disposes background anti-entropy timers. */
  dispose(): void;
}
```

---

### 2.5. `RecoveryManager` Contract

- **Ownership:** Manages peer reconnect catch-up, snapshot handshakes, and lifecycle state gating.

```ts
export interface IRecoveryManager {
  /** True if engine is currently in RECOVERING state awaiting catch-up. */
  readonly isRecovering: boolean;

  /** Enters recovery mode upon reconnect (connection generation >= 2). */
  enterRecovery(generation: number): void;

  /**
   * Atomically validates and installs an imported snapshot.
   * Verifies frontier dominates local history (V_snap >= V_local).
   * If V_snap < V_local, rejects with 'SNAPSHOT_STALE'.
   * If frontiers are incomparable:
   *   1. If history log retains missing causal gaps, requests log repair.
   *   2. If history is compacted beyond horizon (K = 256), NO tie-breaker is used.
   *      Transitions engine to 'DEGRADED_OUT_OF_SYNC' lifecycle state.
   * Verifies RFC 8785 canonical SHA-256 digest (preimage binds formatVersion, sessionId, epoch, author, frontier, maxClock, state).
   * Note: Byte integrity and vector dominance do NOT prove semantic correctness; relies on Cooperative Non-Byzantine Trust Model.
   * Sets isImportedProvenance = true, and replays newer local operations.
   *
   * R7 Causal & Lamport Invariants:
   *   - Atomically initializes `localCausalContext` to `snapshot.snapshotFrontier` (cloned).
   *   - Updates local replication vector to `snapshot.snapshotFrontier`.
   *   - Ingests `originFrontierTimestamps` into `compactedCheckpoints` (mapping `(p, snapshotFrontier[p]) -> originFrontierTimestamps[p]`).
   *   - Reseeds local Lamport clock: `clockManager.reseed(snapshot.maxLogicalTimestamp)` (without extra +1).
   *   - Guarantees subsequent post-import event D_1 ticks clock to `1 + maxLogicalTimestamp`, strictly matching
   *     receiver exact validation: `1 + max_{(p, s) in D_1.causalContext} L(p, s)`.
   */
  installSnapshot(
    snapshot: StateSnapshotPayloadV2,
    expectedSessionId: string,
    minRate: number,
    maxRate: number
  ): EngineDecision;

  /** Clears recovery status after successful catch-up. */
  completeRecovery(): void;

  /** Disposes resources. */
  dispose(): void;
}
```

---

## 3. Error Model Specifications

The existing `SyncEngineError` is extended with standardized Phase 7 error codes:

| Error Code | Description | Origin Context |
| :--- | :--- | :--- |
| `'CLOCK_OVERFLOW'` | Lamport clock reached `Number.MAX_SAFE_INTEGER`. | Local clock manager |
| `'CLOCK_REGRESSION'` | Remote timestamp does not strictly exceed previous timestamp from same origin. | Clock validation |
| `'INVALID_CAUSAL_TIMESTAMP'` | Remote timestamp is strictly less than expected causal dependency timestamp. | Causal verification |
| `'INVALID_LOGICAL_TIMESTAMP'` | Remote timestamp violates exact creation invariant (L(e) > expectedTimestamp). | Lamport validation |
| `'REJECTED_CLOCK_INFLATION'` | Remote clock advances faster than per-origin/per-epoch token bucket rate limit. | Rate inflation check |
| `'REJECTED_UNAUTHORIZED_ORIGIN'` | Live broadcast received where originPeerId !== senderPeerId (Strict No-Forwarding violation). | Origin validation |
| `'SEQUENCE_REGRESSION'` | Operation sequence number is lower than known sequence for origin. | Sequence validation |
| `'SEQUENCE_COLLISION_DETECTED'`| Different logicalEventId received for an already processed origin sequence. | Duplicate detection |
| `'INVALID_ORIGIN_SEQUENCE'` | Sequence number is non-positive or non-safe integer. | Boundary check |
| `'REJECTED_OBSOLETE_EPOCH'` | Operation originated under an obsolete membership epoch. | Strict epoch fencing |
| `'REJECTED_OUTSIDE_HORIZON'` | Late operation sequence predates compacted horizon ($K=256$). | Log insertion |
| `'REPAIR_GAP_EXCEEDED'` | Requested sequence range cannot be fulfilled by log. | Anti-entropy repair |
| `'SNAPSHOT_STALE'` | Imported snapshot frontier is older than local known history. | Recovery manager |
| `'DIGEST_MISMATCH'` | Snapshot payload SHA-256 digest failed cryptographic validation. | Snapshot verification |
| `'DEGRADED_OUT_OF_SYNC'` | Snapshot frontiers are incomparable and cannot be reconciled. | Recovery manager |
| `'INVALID_VERSION_VECTOR'` | Version vector contains negative or non-integer sequences. | Anti-entropy parser |
| `'BATCH_OVERFLOW'` | `OPERATION_BATCH` exceeds maximum allowed size ($> 32$). | Wire validation |

