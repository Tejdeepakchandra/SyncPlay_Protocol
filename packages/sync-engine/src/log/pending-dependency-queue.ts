/**
 * PendingDependencyQueue implementation.
 *
 * Implements Phase 7 bounded dependency buffering:
 * - Bounded buffer (capacity K = 256).
 * - Buffers operations waiting for missing dependencies or origin sequence gaps.
 * - Discovers satisfied operations when replication vector advances.
 * - Computes missing sequence ranges for targeted repair requests.
 */

import { SyncEngineError } from '../errors.js';
import type { StoredOperation } from './types.js';

export interface MissingSequenceRange {
  readonly originPeerId: string;
  readonly fromSequence: number;
  readonly toSequence: number;
}

export class PendingDependencyQueue {
  public static readonly DEFAULT_CAPACITY = 256;

  private readonly capacity: number;
  private readonly queue: StoredOperation[] = [];
  private readonly idIndex = new Set<string>();

  constructor(capacity: number = PendingDependencyQueue.DEFAULT_CAPACITY) {
    this.capacity = capacity;
  }

  public get size(): number {
    return this.queue.length;
  }

  public has(logicalEventId: string): boolean {
    return this.idIndex.has(logicalEventId);
  }

  /**
   * Buffers an operation awaiting dependencies.
   * Throws SyncEngineError('REJECTED_CAPACITY') if queue capacity is exceeded.
   */
  public enqueue(operation: StoredOperation): void {
    if (this.idIndex.has(operation.logicalEventId)) {
      return; // Already buffered
    }

    if (this.queue.length >= this.capacity) {
      throw new SyncEngineError(
        'REJECTED_CAPACITY',
        `Pending dependency queue capacity (${this.capacity}) exceeded`,
      );
    }

    this.queue.push(operation);
    this.idIndex.add(operation.logicalEventId);
  }

  /**
   * Removes and returns all operations whose dependencies are fully satisfied
   * by the provided replication vector.
   * Cascades repeatedly until no more operations can be unblocked.
   */
  public drainSatisfied(
    replicationVector: Record<string, number>,
  ): StoredOperation[] {
    const unblocked: StoredOperation[] = [];
    const localVector: Record<string, number> = { ...replicationVector };

    let progressed = true;
    while (progressed) {
      progressed = false;

      for (let i = 0; i < this.queue.length; i++) {
        const op = this.queue[i]!;

        // 1. Same-origin sequence continuity check
        const currentOriginSeq = localVector[op.originPeerId] ?? 0;
        if (op.originSequenceNumber !== currentOriginSeq + 1) {
          continue; // Predecessor not yet satisfied
        }

        // 2. Cross-origin causal context dependencies check
        let depsSatisfied = true;
        for (const [origin, reqSeq] of Object.entries(op.causalContext)) {
          const haveSeq = localVector[origin] ?? 0;
          if (reqSeq > haveSeq) {
            depsSatisfied = false;
            break;
          }
        }

        if (depsSatisfied) {
          // Unblock this operation
          this.queue.splice(i, 1);
          this.idIndex.delete(op.logicalEventId);
          unblocked.push(op);

          // Speculatively advance localVector to unblock subsequent buffered operations in cascade
          localVector[op.originPeerId] = op.originSequenceNumber;
          progressed = true;
          break; // Restart loop with updated vector
        }
      }
    }

    return unblocked;
  }

  /**
   * Computes missing sequence ranges needed to unblock all currently buffered operations.
   */
  public getMissingRanges(
    replicationVector: Record<string, number>,
  ): MissingSequenceRange[] {
    const missingByOrigin = new Map<string, { minNeeded: number; maxNeeded: number }>();

    for (const op of this.queue) {
      // 1. Same-origin sequence gap
      const currentOriginSeq = replicationVector[op.originPeerId] ?? 0;
      if (op.originSequenceNumber > currentOriginSeq + 1) {
        const existing = missingByOrigin.get(op.originPeerId);
        const fromSeq = currentOriginSeq + 1;
        const toSeq = op.originSequenceNumber - 1;
        if (!existing) {
          missingByOrigin.set(op.originPeerId, { minNeeded: fromSeq, maxNeeded: toSeq });
        } else {
          existing.minNeeded = Math.min(existing.minNeeded, fromSeq);
          existing.maxNeeded = Math.max(existing.maxNeeded, toSeq);
        }
      }

      // 2. Cross-origin causal dependencies
      for (const [depOrigin, reqSeq] of Object.entries(op.causalContext)) {
        const currentDepSeq = replicationVector[depOrigin] ?? 0;
        if (reqSeq > currentDepSeq) {
          const fromSeq = currentDepSeq + 1;
          const toSeq = reqSeq;
          const existing = missingByOrigin.get(depOrigin);
          if (!existing) {
            missingByOrigin.set(depOrigin, { minNeeded: fromSeq, maxNeeded: toSeq });
          } else {
            existing.minNeeded = Math.min(existing.minNeeded, fromSeq);
            existing.maxNeeded = Math.max(existing.maxNeeded, toSeq);
          }
        }
      }
    }

    const ranges: MissingSequenceRange[] = [];
    for (const [originPeerId, range] of missingByOrigin.entries()) {
      ranges.push({
        originPeerId,
        fromSequence: range.minNeeded,
        toSequence: range.maxNeeded,
      });
    }

    return ranges;
  }

  public getAll(): ReadonlyArray<StoredOperation> {
    return [...this.queue];
  }

  /**
   * Prunes all buffered operations belonging to obsolete membership epochs (< currentEpoch).
   * Prevents old-epoch operations from leaking into the new epoch.
   */
  public pruneObsoleteEpoch(currentEpoch: number): number {
    let prunedCount = 0;
    for (let i = this.queue.length - 1; i >= 0; i--) {
      if (this.queue[i]!.membershipEpoch < currentEpoch) {
        const removed = this.queue.splice(i, 1)[0]!;
        this.idIndex.delete(removed.logicalEventId);
        prunedCount++;
      }
    }
    return prunedCount;
  }

  public clear(): void {
    this.queue.length = 0;
    this.idIndex.clear();
  }
}
