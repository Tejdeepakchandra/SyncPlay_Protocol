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
import { computeOperationFingerprint } from './operation-fingerprint.js';
import type { CompactedCheckpointItem } from '@syncplay/protocol';
import type { PlaybackState } from '../types.js';
import type {
  StoredOperation,
  InsertOperationResult,
  IOrderedOperationLog,
  CompactedCheckpointRecord,
} from './types.js';

function deepClone<T>(val: T): T {
  if (typeof structuredClone === 'function') {
    return structuredClone(val);
  }
  return JSON.parse(JSON.stringify(val));
}

export class OrderedOperationLog implements IOrderedOperationLog {
  public static readonly DEFAULT_CAPACITY = 256;

  private readonly _capacity: number;
  private operations: StoredOperation[] = [];
  private idIndex = new Map<string, StoredOperation>();
  private originSeqIndex = new Map<string, StoredOperation>();
  private compactedCheckpoints = new Map<string, CompactedCheckpointRecord>();
  private originFrontierTimestamps = new Map<string, number>();
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
    return this.compactedCheckpoints.get(`${originPeerId}:${sequenceNumber}`)?.logicalTimestamp;
  }

  public getCompactedCheckpointRecord(
    originPeerId: string,
    sequenceNumber: number,
  ): CompactedCheckpointRecord | undefined {
    return this.compactedCheckpoints.get(`${originPeerId}:${sequenceNumber}`);
  }

  /**
   * Returns all retained compacted checkpoints across origins, sorted deterministically
   * by originPeerId ascending, then sequenceNumber ascending.
   */
  public getAllCompactedCheckpoints(): ReadonlyArray<CompactedCheckpointItem> {
    const items: CompactedCheckpointItem[] = [];
    for (const [key, record] of this.compactedCheckpoints.entries()) {
      const idx = key.indexOf(':');
      if (idx !== -1) {
        const originPeerId = key.substring(0, idx);
        const sequenceNumber = parseInt(key.substring(idx + 1), 10);
        if (!isNaN(sequenceNumber)) {
          items.push({
            originPeerId,
            sequenceNumber,
            logicalTimestamp: record.logicalTimestamp,
          });
        }
      }
    }
    return items.sort((a, b) => {
      const cmp = a.originPeerId.localeCompare(b.originPeerId);
      if (cmp !== 0) return cmp;
      return a.sequenceNumber - b.sequenceNumber;
    });
  }

  /**
   * Resolves the logical timestamp for each origin at the sequence specified by the frontier.
   * Checks:
   * 1. If seq <= 0: returns 0
   * 2. Active log retained operation at (origin, seq): returns exact Lamport timestamp
   * 3. Compacted checkpoint at (origin, seq): returns exact Lamport timestamp
   * 4. Evicted or unavailable historical sequence: returns null
   * (Strictly DOES NOT fall back to latest origin-level timestamp)
   */
  public getOriginFrontierTimestamps(frontier: Record<string, number>): Record<string, number | null> {
    const result: Record<string, number | null> = {};
    for (const [origin, seq] of Object.entries(frontier)) {
      if (seq <= 0) {
        result[origin] = 0;
        continue;
      }
      const activeOp = this.getOperationAt(origin, seq);
      if (activeOp) {
        result[origin] = activeOp.logicalTimestamp;
        continue;
      }
      const cp = this.getCompactedCheckpoint(origin, seq);
      if (cp !== undefined) {
        result[origin] = cp;
        continue;
      }
      result[origin] = null;
    }
    return result;
  }

  public setCompactedCheckpoint(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp: number,
    logicalEventId: string = '',
    fingerprint: string = '',
  ): void {
    this.compactedCheckpoints.set(`${originPeerId}:${sequenceNumber}`, {
      logicalTimestamp,
      logicalEventId,
      fingerprint,
    });
    // Strict global bound K = 256 entries across all origins
    if (this.compactedCheckpoints.size > this._capacity) {
      // FIFO eviction: prune oldest checkpoint entry globally
      const firstKey = this.compactedCheckpoints.keys().next().value;
      if (firstKey) {
        this.compactedCheckpoints.delete(firstKey);
      }
    }
  }

  /**
   * Retrieves the canonical SHA-256 fingerprint for an origin at a target sequence,
   * or the highest sequence retained in the log / compacted checkpoints.
   *
   * Reuses the canonical computeOperationFingerprint() implementation.
   */
  public getTailFingerprint(originPeerId: string, atSequence?: number): string | undefined {
    let targetSeq = atSequence;
    if (targetSeq === undefined) {
      for (let i = this.operations.length - 1; i >= 0; i--) {
        const op = this.operations[i];
        if (op && op.originPeerId === originPeerId) {
          targetSeq = op.originSequenceNumber;
          break;
        }
      }
      if (targetSeq === undefined) {
        const snapSeq = this.baseSnapshotFrontier[originPeerId];
        if (snapSeq !== undefined && snapSeq > 0) {
          targetSeq = snapSeq;
        }
      }
    }

    if (targetSeq === undefined || targetSeq <= 0) {
      return undefined;
    }

    const op = this.getOperationAt(originPeerId, targetSeq);
    if (op) {
      return computeOperationFingerprint(op);
    }

    const checkpoint = this.getCompactedCheckpointRecord(originPeerId, targetSeq);
    if (checkpoint?.fingerprint && checkpoint.fingerprint.length === 64) {
      return checkpoint.fingerprint;
    }

    return undefined;
  }

  /**
   * Returns a copy of the highest sequence numbers observed per origin across
   * retained operations and compacted base snapshot frontiers.
   */
  public getReplicationFrontier(): Record<string, number> {
    const frontier: Record<string, number> = { ...this.baseSnapshotFrontier };
    for (const op of this.operations) {
      const current = frontier[op.originPeerId] ?? 0;
      if (op.originSequenceNumber > current) {
        frontier[op.originPeerId] = op.originSequenceNumber;
      }
    }
    return frontier;
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
   * Retrieves the lowest origin sequence number currently retained in the active log
   * for a given origin peer, or undefined if no operations are retained for that origin.
   */
  public getOldestRetainedSequence(originPeerId: string): number | undefined {
    let oldest: number | undefined = undefined;
    for (const op of this.operations) {
      if (op.originPeerId === originPeerId) {
        if (oldest === undefined || op.originSequenceNumber < oldest) {
          oldest = op.originSequenceNumber;
        }
      }
    }
    return oldest;
  }

  /**
   * Inserts an operation into the log maintaining strict total ordering.
   * Performs deduplication and collision detection.
   */
  public insert(operation: StoredOperation): InsertOperationResult {
    // Freeze operation and nested causalContext to guarantee runtime immutability
    Object.freeze(operation);
    if (operation.causalContext) {
      Object.freeze(operation.causalContext);
    }

    // 1. Deduplication check by logicalEventId with identity reuse verification
    const existingById = this.idIndex.get(operation.logicalEventId);
    if (existingById) {
      const incomingFp = computeOperationFingerprint(operation);
      const existingFp = computeOperationFingerprint(existingById);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${operation.logicalEventId}: immutable content altered`,
        );
      }
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
      const incomingFp = computeOperationFingerprint(operation);
      const existingFp = computeOperationFingerprint(existingBySeq);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${operation.logicalEventId}: immutable content altered`,
        );
      }
      return {
        inserted: false,
        isHead: false,
        index: -1,
        code: 'IGNORED_DUPLICATE',
      };
    }

    // 3. Compacted check: if already subsumed by base snapshot frontier or in checkpoints
    const baseSeq = this.baseSnapshotFrontier[operation.originPeerId] ?? 0;
    if (operation.originSequenceNumber <= baseSeq) {
      const checkpoint = this.compactedCheckpoints.get(seqKey);
      if (checkpoint) {
        // Identity metadata is retained in compactedCheckpoints horizon!
        if (checkpoint.logicalEventId && checkpoint.logicalEventId !== operation.logicalEventId) {
          // Known sequence collision with compacted operation! Must NOT treat as duplicate!
          throw new SyncEngineError(
            'SEQUENCE_COLLISION_DETECTED',
            `Sequence collision detected for compacted origin ${operation.originPeerId} seq ${operation.originSequenceNumber}: compacted ${checkpoint.logicalEventId} vs incoming ${operation.logicalEventId}`,
          );
        }
        if (checkpoint.fingerprint) {
          const incomingFp = computeOperationFingerprint(operation);
          if (incomingFp !== checkpoint.fingerprint) {
            throw new SyncEngineError(
              'IDENTITY_CONFLICT_DETECTED',
              `Identity conflict detected for compacted event ${operation.logicalEventId}: immutable content altered`,
            );
          }
        }
        return {
          inserted: false,
          isHead: false,
          index: -1,
          code: 'IGNORED_ALREADY_COMPACTED',
        };
      } else {
        // Identity metadata has expired beyond the retention horizon!
        // Cannot distinguish legitimate duplicate from conflicting event -> return safe outside-horizon
        return {
          inserted: false,
          isHead: false,
          index: -1,
          code: 'REJECTED_OUTSIDE_HORIZON',
        };
      }
    }

    // Also check if explicitly marked in compactedCheckpoints (even if baseSeq wasn't updated)
    if (this.compactedCheckpoints.has(seqKey)) {
      const checkpoint = this.compactedCheckpoints.get(seqKey)!;
      if (checkpoint.logicalEventId && checkpoint.logicalEventId !== operation.logicalEventId) {
        throw new SyncEngineError(
          'SEQUENCE_COLLISION_DETECTED',
          `Sequence collision detected for compacted origin ${operation.originPeerId} seq ${operation.originSequenceNumber}: compacted ${checkpoint.logicalEventId} vs incoming ${operation.logicalEventId}`,
        );
      }
      if (checkpoint.fingerprint) {
        const incomingFp = computeOperationFingerprint(operation);
        if (incomingFp !== checkpoint.fingerprint) {
          throw new SyncEngineError(
            'IDENTITY_CONFLICT_DETECTED',
            `Identity conflict detected for compacted event ${operation.logicalEventId}: immutable content altered`,
          );
        }
      }
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
      const evictedFp = computeOperationFingerprint(evicted);
      this.setCompactedCheckpoint(
        evicted.originPeerId,
        evicted.originSequenceNumber,
        evicted.logicalTimestamp,
        evicted.logicalEventId,
        evictedFp,
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
      if (this.originFrontierTimestamps.size > 64) {
        const firstKey = this.originFrontierTimestamps.keys().next().value;
        if (firstKey) this.originFrontierTimestamps.delete(firstKey);
      }
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
   * Uses transactional candidate staging across all 8 mutable components:
   * 1. baseSnapshot (deep cloned before reduction)
   * 2. operations (candidate retained array)
   * 3. idIndex (rebuilt from candidate retained operations)
   * 4. originSeqIndex (rebuilt from candidate retained operations)
   * 5. compactedCheckpoints (cloned with FIFO bound)
   * 6. originFrontierTimestamps (cloned with FIFO bound)
   * 7. baseSnapshotFrontier (cloned and updated)
   * 8. hasPrunedOperations (candidate boolean)
   *
   * Reducer contract: The reducer is expected to be a pure function:
   * (state: PlaybackState, op: StoredOperation) => PlaybackState
   * It must not mutate the incoming state or operation objects.
   *
   * Invariant: Candidate construction failure (e.g. reducer exception) leaves
   * all 8 active components unchanged: ActiveStateAfter === ActiveStateBefore.
   */
  public compactAtFrontier(stableFrontier: Record<string, number>): number {
    const candidateRetainedOperations: StoredOperation[] = [];
    const candidateToPrune: StoredOperation[] = [];

    for (const op of this.operations) {
      const stableSeq = stableFrontier[op.originPeerId] ?? 0;
      if (op.originSequenceNumber <= stableSeq) {
        candidateToPrune.push(op);
      } else {
        candidateRetainedOperations.push(op);
      }
    }

    if (candidateToPrune.length === 0) {
      return 0;
    }

    // Sort operations to prune ascending to fold into base snapshot in deterministic total order
    candidateToPrune.sort((a, b) => compareOperations(a, b));

    // 1. Stage candidate base snapshot (deep cloned before reduction)
    let candidateBaseSnapshot: PlaybackState | undefined = undefined;
    if (this.baseSnapshot) {
      candidateBaseSnapshot = deepClone(this.baseSnapshot);
    }

    // 2. Stage candidate indexes from candidateRetainedOperations
    const candidateIdIndex = new Map<string, StoredOperation>();
    const candidateOriginSeqIndex = new Map<string, StoredOperation>();
    for (const op of candidateRetainedOperations) {
      candidateIdIndex.set(op.logicalEventId, op);
      candidateOriginSeqIndex.set(`${op.originPeerId}:${op.originSequenceNumber}`, op);
    }

    // 3. Stage candidate compactedCheckpoints (clone existing map)
    const candidateCompactedCheckpoints = new Map<string, CompactedCheckpointRecord>(
      this.compactedCheckpoints,
    );

    // 4. Stage candidate originFrontierTimestamps (clone existing map)
    const candidateOriginFrontierTimestamps = new Map<string, number>(
      this.originFrontierTimestamps,
    );

    // 5. Stage candidate baseSnapshotFrontier (clone existing object)
    const candidateBaseSnapshotFrontier: Record<string, number> = {
      ...this.baseSnapshotFrontier,
    };

    // 6. Update candidate checkpoints, base snapshot, baseSnapshotFrontier, and originFrontierTimestamps
    for (const op of candidateToPrune) {
      const opFp = computeOperationFingerprint(op);
      const seqKey = `${op.originPeerId}:${op.originSequenceNumber}`;

      candidateCompactedCheckpoints.set(seqKey, {
        logicalTimestamp: op.logicalTimestamp,
        logicalEventId: op.logicalEventId,
        fingerprint: opFp,
      });
      if (candidateCompactedCheckpoints.size > this._capacity) {
        const firstKey = candidateCompactedCheckpoints.keys().next().value;
        if (firstKey) {
          candidateCompactedCheckpoints.delete(firstKey);
        }
      }

      // Fold op into candidate base snapshot using reducer
      if (candidateBaseSnapshot && this.reducer) {
        candidateBaseSnapshot = this.reducer(candidateBaseSnapshot, op);
      }

      candidateBaseSnapshotFrontier[op.originPeerId] = Math.max(
        candidateBaseSnapshotFrontier[op.originPeerId] ?? 0,
        op.originSequenceNumber,
      );

      candidateOriginFrontierTimestamps.set(op.originPeerId, op.logicalTimestamp);
      if (candidateOriginFrontierTimestamps.size > 64) {
        const firstKey = candidateOriginFrontierTimestamps.keys().next().value;
        if (firstKey) candidateOriginFrontierTimestamps.delete(firstKey);
      }
    }

    // 7. Stage candidate hasPrunedOperations
    const candidateHasPrunedOperations = true;

    // --- ALL POTENTIALLY THROWING OPERATIONS ARE COMPLETE ABOVE ---
    // Perform synchronous reference replacement of all 8 mutable components.
    // No await, callback, or external observer may execute between these assignments.
    this.baseSnapshot = candidateBaseSnapshot;
    this.operations = candidateRetainedOperations;
    this.idIndex = candidateIdIndex;
    this.originSeqIndex = candidateOriginSeqIndex;
    this.compactedCheckpoints = candidateCompactedCheckpoints;
    this.originFrontierTimestamps = candidateOriginFrontierTimestamps;
    this.baseSnapshotFrontier = candidateBaseSnapshotFrontier;
    this.hasPrunedOperations = candidateHasPrunedOperations;

    return candidateToPrune.length;
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
