/**
 * OrderedOperationLog implementation.
 *
 * Implements Phase 7 specifications:
 * - Bounded circular ring buffer (capacity K = 256)
 * - Binary search insertion maintaining total order <L, originPeerId, logicalEventId>
 * - Compacted checkpoints lookup preserving exact Lamport timestamps
 * - Collision and duplicate detection
 */

import { SyncEngineError } from '../errors.js';
import { compareOperations } from '../ordering/comparator.js';
import type { PlaybackState } from '../types.js';
import type {
  StoredOperation,
  InsertOperationResult,
  IOrderedOperationLog,
} from './types.js';

export class OrderedOperationLog implements IOrderedOperationLog {
  public static readonly DEFAULT_CAPACITY = 256;

  private readonly _capacity: number;
  private readonly operations: StoredOperation[] = [];
  private readonly idIndex = new Map<string, StoredOperation>();
  private readonly originSeqIndex = new Map<string, StoredOperation>();
  private readonly compactedCheckpoints = new Map<string, number>();
  private readonly originFrontierTimestamps = new Map<string, number>();
  private baseSnapshot?: PlaybackState;
  private baseSnapshotFrontier: Record<string, number> = {};
  private hasPrunedOperations = false;
  private reducer?: (state: PlaybackState, op: StoredOperation) => PlaybackState;

  constructor(
    capacity: number = OrderedOperationLog.DEFAULT_CAPACITY,
    initialBaseSnapshot?: PlaybackState,
    reducer?: (state: PlaybackState, op: StoredOperation) => PlaybackState,
  ) {
    this._capacity = capacity;
    this.baseSnapshot = initialBaseSnapshot;
    this.reducer = reducer;
  }

  public get size(): number {
    return this.operations.length;
  }

  public get capacity(): number {
    return this._capacity;
  }

  public getBaseSnapshot(): PlaybackState | undefined {
    return this.baseSnapshot;
  }

  public getBaseSnapshotFrontier(): Readonly<Record<string, number>> {
    return { ...this.baseSnapshotFrontier };
  }

  public setReducer(reducer: (state: PlaybackState, op: StoredOperation) => PlaybackState): void {
    this.reducer = reducer;
  }

  public setBaseSnapshot(snapshot: PlaybackState, frontier?: Record<string, number>): void {
    this.baseSnapshot = snapshot;
    if (frontier) {
      this.baseSnapshotFrontier = { ...frontier };
    }
  }

  public has(logicalEventId: string): boolean {
    return this.idIndex.has(logicalEventId);
  }

  public get(logicalEventId: string): StoredOperation | undefined {
    return this.idIndex.get(logicalEventId);
  }

  public getOperationAt(
    originPeerId: string,
    sequenceNumber: number,
  ): StoredOperation | undefined {
    return this.originSeqIndex.get(`${originPeerId}:${sequenceNumber}`);
  }

  public getAllOperations(): ReadonlyArray<StoredOperation> {
    return [...this.operations];
  }

  public getCompactedCheckpoint(
    originPeerId: string,
    sequenceNumber: number,
  ): number | undefined {
    return this.compactedCheckpoints.get(`${originPeerId}:${sequenceNumber}`);
  }

  public setCompactedCheckpoint(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp: number,
  ): void {
    this.compactedCheckpoints.set(`${originPeerId}:${sequenceNumber}`, logicalTimestamp);
    // Strict global bound K = 256 entries across all origins
    if (this.compactedCheckpoints.size > this._capacity) {
      // FIFO eviction: prune oldest checkpoint entry globally
      const firstKey = this.compactedCheckpoints.keys().next().value;
      if (firstKey) {
        this.compactedCheckpoints.delete(firstKey);
      }
    }
  }

  public getRange(
    originPeerId: string,
    fromSeq: number,
    toSeq: number,
  ): ReadonlyArray<StoredOperation> {
    return this.operations
      .filter(
        (op) =>
          op.originPeerId === originPeerId &&
          op.originSequenceNumber >= fromSeq &&
          op.originSequenceNumber <= toSeq,
      )
      .sort((a, b) => a.originSequenceNumber - b.originSequenceNumber);
  }

  /**
   * Inserts an operation into the log maintaining strict total ordering.
   * Performs deduplication and collision detection.
   */
  public insert(operation: StoredOperation): InsertOperationResult {
    // 1. Deduplication check by logicalEventId
    const existingById = this.idIndex.get(operation.logicalEventId);
    if (existingById) {
      return {
        inserted: false,
        isHead: false,
        index: -1,
        code: 'IGNORED_DUPLICATE',
      };
    }

    // 2. Collision check by origin sequence number
    const seqKey = `${operation.originPeerId}:${operation.originSequenceNumber}`;
    const existingBySeq = this.originSeqIndex.get(seqKey);
    if (existingBySeq) {
      if (existingBySeq.logicalEventId !== operation.logicalEventId) {
        throw new SyncEngineError(
          'SEQUENCE_COLLISION_DETECTED',
          `Sequence collision detected for origin ${operation.originPeerId} seq ${operation.originSequenceNumber}: existing ${existingBySeq.logicalEventId} vs incoming ${operation.logicalEventId}`,
        );
      }
      return {
        inserted: false,
        isHead: false,
        index: -1,
        code: 'IGNORED_DUPLICATE',
      };
    }

    // 3. Compacted check: if already subsumed by base snapshot frontier
    const baseSeq = this.baseSnapshotFrontier[operation.originPeerId] ?? 0;
    if (operation.originSequenceNumber <= baseSeq) {
      return {
        inserted: false,
        isHead: false,
        index: -1,
        code: 'IGNORED_ALREADY_COMPACTED',
      };
    }

    // Also check if explicitly marked in compactedCheckpoints
    if (this.compactedCheckpoints.has(seqKey)) {
      return {
        inserted: false,
        isHead: false,
        index: -1,
        code: 'IGNORED_ALREADY_COMPACTED',
      };
    }

    // 4. Binary search insertion location
    const insertIndex = this.findInsertionIndex(operation);

    // Check if operation falls before earliest retained operation when compacted
    if (this.hasPrunedOperations && this.operations.length > 0 && insertIndex === 0) {
      if (compareOperations(operation, this.operations[0]!) < 0) {
        // Late event older than earliest retained operation, but sequence > baseSeq!
        // Cannot be inserted before compacted base state -> reject outside horizon!
        return {
          inserted: false,
          isHead: false,
          index: -1,
          code: 'REJECTED_OUTSIDE_HORIZON',
        };
      }
    }

    const isHead = insertIndex === this.operations.length;

    this.operations.splice(insertIndex, 0, operation);
    this.idIndex.set(operation.logicalEventId, operation);
    this.originSeqIndex.set(seqKey, operation);

    // 5. Enforce bounded ring buffer capacity K = 256
    if (this.operations.length > this._capacity) {
      const evicted = this.operations.shift()!;
      this.idIndex.delete(evicted.logicalEventId);
      this.originSeqIndex.delete(
        `${evicted.originPeerId}:${evicted.originSequenceNumber}`,
      );
      this.setCompactedCheckpoint(
        evicted.originPeerId,
        evicted.originSequenceNumber,
        evicted.logicalTimestamp,
      );
      // Fold evicted operation into base snapshot
      if (this.baseSnapshot && this.reducer) {
        this.baseSnapshot = this.reducer(this.baseSnapshot, evicted);
      }
      this.baseSnapshotFrontier[evicted.originPeerId] = Math.max(
        this.baseSnapshotFrontier[evicted.originPeerId] ?? 0,
        evicted.originSequenceNumber,
      );
      this.originFrontierTimestamps.set(evicted.originPeerId, evicted.logicalTimestamp);
      this.hasPrunedOperations = true;
    }

    return {
      inserted: true,
      isHead,
      index: insertIndex,
      code: isHead ? 'INSERTED_HEAD' : 'INSERTED_REORDER',
    };
  }

  /**
   * Prunes operations older than stable frontier, updates base snapshot, and preserves checkpoints.
   */
  public compactAtFrontier(stableFrontier: Record<string, number>): number {
    const toPrune: StoredOperation[] = [];
    for (let i = this.operations.length - 1; i >= 0; i--) {
      const op = this.operations[i]!;
      const stableSeq = stableFrontier[op.originPeerId] ?? 0;
      if (op.originSequenceNumber <= stableSeq) {
        this.operations.splice(i, 1);
        this.idIndex.delete(op.logicalEventId);
        this.originSeqIndex.delete(`${op.originPeerId}:${op.originSequenceNumber}`);
        this.setCompactedCheckpoint(
          op.originPeerId,
          op.originSequenceNumber,
          op.logicalTimestamp,
        );
        toPrune.push(op);
      }
    }

    if (toPrune.length > 0) {
      // Sort ascending to fold into base snapshot in deterministic total order
      toPrune.sort((a, b) => compareOperations(a, b));
      for (const op of toPrune) {
        if (this.baseSnapshot && this.reducer) {
          this.baseSnapshot = this.reducer(this.baseSnapshot, op);
        }
        this.baseSnapshotFrontier[op.originPeerId] = Math.max(
          this.baseSnapshotFrontier[op.originPeerId] ?? 0,
          op.originSequenceNumber,
        );
        this.originFrontierTimestamps.set(op.originPeerId, op.logicalTimestamp);
      }
      this.hasPrunedOperations = true;
    }

    return toPrune.length;
  }

  public reset(baseSnapshot: PlaybackState, frontier?: Record<string, number>): void {
    this.operations.length = 0;
    this.idIndex.clear();
    this.originSeqIndex.clear();
    this.compactedCheckpoints.clear();
    this.originFrontierTimestamps.clear();
    this.baseSnapshot = baseSnapshot;
    this.baseSnapshotFrontier = frontier ? { ...frontier } : {};
    this.hasPrunedOperations = false;
  }

  public dispose(): void {
    this.operations.length = 0;
    this.idIndex.clear();
    this.originSeqIndex.clear();
    this.compactedCheckpoints.clear();
    this.originFrontierTimestamps.clear();
    this.baseSnapshot = undefined;
    this.baseSnapshotFrontier = {};
    this.hasPrunedOperations = false;
    this.reducer = undefined;
  }

  /**
   * Binary search to find sorted insertion position.
   */
  private findInsertionIndex(op: StoredOperation): number {
    let low = 0;
    let high = this.operations.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      const cmp = compareOperations(op, this.operations[mid]!);
      if (cmp < 0) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }

    return low;
  }
}
