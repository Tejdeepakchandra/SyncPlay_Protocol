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
   * Advances the local clock for an outbound operation.
   * Increments: L = L + 1.
   * Throws SyncEngineError('CLOCK_OVERFLOW') if L >= Number.MAX_SAFE_INTEGER - 1.
   */
  tickLocal(): number;

  /**
   * Updates local clock upon receiving a remote operation.
   * Advances: L = max(L_local, remoteClock) + 1.
   * Validates non-negative integer and checks MAX_CLOCK_SKEW bound.
   * Throws SyncEngineError('INVALID_CLOCK') or SyncEngineError('CLOCK_SKEW_EXCEEDED').
   */
  updateRemote(remoteClock: number): number;

  /**
   * Reseeds clock upon reconnect or snapshot recovery.
   * Sets: L = max(L_local, maxObservedClock) + 1.
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
  readonly senderPeerId: string;
  readonly messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE';
  readonly logicalTimestamp: number;
  readonly originSequenceNumber: number;
  readonly predecessorEventId?: string;
  readonly appliedAtMonotonic: number;
  readonly payload: unknown;
}

export interface InsertOperationResult {
  /** True if inserted into log; false if duplicate or predates horizon. */
  readonly inserted: boolean;
  /** True if appended to head; false if inserted earlier requiring rollback. */
  readonly isHead: boolean;
  /** Index where inserted in sorted log array. */
  readonly index: number;
  /** Specific outcome code. */
  readonly code: 'INSERTED_HEAD' | 'INSERTED_REORDER' | 'IGNORED_DUPLICATE' | 'REJECTED_OUTSIDE_HORIZON';
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
  /** Current local Version Vector (originPeerId -> highestConsecutiveSeq). */
  readonly versionVector: Readonly<Record<string, number>>;

  /** Generates a StateDigestPayload to announce local frontier. */
  createDigest(): StateDigestPayload;

  /**
   * Evaluates an incoming remote digest against local version vector.
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

  /** Updates version vector upon receiving an operation. */
  recordOperationReceived(originPeerId: string, sequenceNumber: number): void;

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
   * Sets isImportedProvenance = true, clamps clocks, resets log.
   */
  installSnapshot(
    snapshot: StateSnapshotPayload,
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
| `'CLOCK_SKEW_EXCEEDED'` | Remote timestamp exceeds local clock by $> 10000$. | Remote receive check |
| `'REJECTED_OUTSIDE_HORIZON'` | Late operation timestamp predates retained log ($K=256$). | Log insertion |
| `'REPAIR_GAP_EXCEEDED'` | Requested sequence range cannot be fulfilled by log. | Anti-entropy repair |
| `'SNAPSHOT_STALE'` | Imported snapshot frontier is older than local frontier. | Recovery manager |
| `'INVALID_VERSION_VECTOR'` | Version vector contains negative or non-integer sequences. | Anti-entropy parser |
| `'BATCH_OVERFLOW'` | `OPERATION_BATCH` exceeds maximum allowed size ($> 32$). | Wire validation |
