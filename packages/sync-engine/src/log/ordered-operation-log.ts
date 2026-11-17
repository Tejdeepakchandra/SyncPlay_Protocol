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
  private baseSnapshot?: PlaybackState;

  constructor(
    capacity: number = OrderedOperationLog.DEFAULT_CAPACITY,
    initialBaseSnapshot?: PlaybackState,
  ) {
    this._capacity = capacity;
    this.baseSnapshot = initialBaseSnapshot;
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

  public setBaseSnapshot(snapshot: PlaybackState): void {
    this.baseSnapshot = snapshot;
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
    if (this.compactedCheckpoints.size > this._capacity * 2) {
      // Prune oldest checkpoint entries if excessive
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

    // 3. Compacted check: if already compacted
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
    }

    return {
      inserted: true,
      isHead,
      index: insertIndex,
      code: isHead ? 'INSERTED_HEAD' : 'INSERTED_REORDER',
    };
  }

  /**
   * Prunes operations older than stable frontier and preserves checkpoints.
   */
  public compactAtFrontier(stableFrontier: Record<string, number>): number {
    let prunedCount = 0;
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
        prunedCount++;
      }
    }
    return prunedCount;
  }

  public reset(baseSnapshot: PlaybackState): void {
    this.operations.length = 0;
    this.idIndex.clear();
    this.originSeqIndex.clear();
    this.compactedCheckpoints.clear();
    this.baseSnapshot = baseSnapshot;
  }

  public dispose(): void {
    this.operations.length = 0;
    this.idIndex.clear();
    this.originSeqIndex.clear();
    this.compactedCheckpoints.clear();
    this.baseSnapshot = undefined;
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
