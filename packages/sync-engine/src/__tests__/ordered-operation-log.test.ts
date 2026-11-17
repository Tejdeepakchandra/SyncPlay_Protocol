/**
 * Unit tests for OrderedOperationLog and PendingDependencyQueue.
 *
 * Verifies:
 * - Total-order binary insertion <L, originPeerId, logicalEventId>
 * - Duplicate handling and sequence collision detection
 * - Bounded ring buffer eviction and compacted checkpoint preservation
 * - Pending dependency buffering, unblocking cascades, and missing range tracking
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import { SyncEngineError } from '../errors.js';
import type { StoredOperation } from '../log/types.js';

function makeOp(
  logicalEventId: string,
  originPeerId: string,
  logicalTimestamp: number,
  originSequenceNumber: number,
  causalContext: Record<string, number> = {},
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE' = 'PAUSE',
): StoredOperation {
  return {
    logicalEventId,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: 1,
    messageType,
    logicalTimestamp,
    originSequenceNumber,
    causalContext,
    appliedAtMonotonic: 1000,
    payload: {},
  };
}

describe('OrderedOperationLog', () => {
  let log: OrderedOperationLog;

  beforeEach(() => {
    log = new OrderedOperationLog(4); // Small capacity for easy testing
  });

  it('inserts operations at head when timestamps are monotonically increasing', () => {
    const op1 = makeOp('op-1', 'peer-A', 1, 1);
    const op2 = makeOp('op-2', 'peer-A', 2, 2, { 'peer-A': 1 });

    const res1 = log.insert(op1);
    expect(res1.inserted).toBe(true);
    expect(res1.isHead).toBe(true);
    expect(res1.code).toBe('INSERTED_HEAD');

    const res2 = log.insert(op2);
    expect(res2.inserted).toBe(true);
    expect(res2.isHead).toBe(true);
    expect(res2.code).toBe('INSERTED_HEAD');

    expect(log.size).toBe(2);
    expect(log.getAllOperations().map((o) => o.logicalEventId)).toEqual(['op-1', 'op-2']);
  });

  it('inserts out-of-order operations at correct sorted index via binary search', () => {
    const op1 = makeOp('op-1', 'peer-A', 10, 1);
    const op3 = makeOp('op-3', 'peer-A', 30, 3);
    const op2 = makeOp('op-2', 'peer-B', 20, 1);

    log.insert(op1);
    log.insert(op3);
    const res2 = log.insert(op2);

    expect(res2.inserted).toBe(true);
    expect(res2.isHead).toBe(false);
    expect(res2.code).toBe('INSERTED_REORDER');
    expect(res2.index).toBe(1);

    expect(log.getAllOperations().map((o) => o.logicalEventId)).toEqual([
      'op-1',
      'op-2',
      'op-3',
    ]);
  });

  it('resolves identical logicalEventId as IGNORED_DUPLICATE', () => {
    const op1 = makeOp('op-1', 'peer-A', 10, 1);
    log.insert(op1);

    const dup = makeOp('op-1', 'peer-A', 10, 1);
    const res = log.insert(dup);

    expect(res.inserted).toBe(false);
    expect(res.code).toBe('IGNORED_DUPLICATE');
    expect(log.size).toBe(1);
  });

  it('throws SEQUENCE_COLLISION_DETECTED on different logicalEventId for same origin sequence', () => {
    const op1 = makeOp('op-1', 'peer-A', 10, 1);
    log.insert(op1);

    const conflicting = makeOp('op-conflict', 'peer-A', 10, 1);
    expect(() => log.insert(conflicting)).toThrow(SyncEngineError);

    try {
      log.insert(conflicting);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('SEQUENCE_COLLISION_DETECTED');
    }
  });

  it('bounds capacity and preserves compacted checkpoints when evicting', () => {
    // Capacity is 4
    const ops = [
      makeOp('op-1', 'peer-A', 10, 1),
      makeOp('op-2', 'peer-A', 20, 2),
      makeOp('op-3', 'peer-A', 30, 3),
      makeOp('op-4', 'peer-A', 40, 4),
      makeOp('op-5', 'peer-A', 50, 5),
    ];

    for (const op of ops) {
      log.insert(op);
    }

    expect(log.size).toBe(4);
    // op-1 should have been evicted
    expect(log.has('op-1')).toBe(false);
    expect(log.has('op-2')).toBe(true);
    expect(log.has('op-5')).toBe(true);

    // Evicted op-1 timestamp preserved in compacted checkpoints
    expect(log.getCompactedCheckpoint('peer-A', 1)).toBe(10);
  });

  it('compactAtFrontier prunes operations up to stable sequence', () => {
    log.insert(makeOp('op-1', 'peer-A', 1, 1));
    log.insert(makeOp('op-2', 'peer-A', 2, 2));
    log.insert(makeOp('op-3', 'peer-B', 3, 1));

    const pruned = log.compactAtFrontier({ 'peer-A': 1 });
    expect(pruned).toBe(1);
    expect(log.size).toBe(2);
    expect(log.has('op-1')).toBe(false);
    expect(log.getCompactedCheckpoint('peer-A', 1)).toBe(1);
  });
});

describe('PendingDependencyQueue', () => {
  let queue: PendingDependencyQueue;

  beforeEach(() => {
    queue = new PendingDependencyQueue(10);
  });

  it('buffers and unblocks operations as replication vector advances', () => {
    // D1 requires A: 10, B: 5, and origin sequence D: 1
    const d1 = makeOp('d-1', 'peer-D', 13, 1, { 'peer-A': 10, 'peer-B': 5 });
    queue.enqueue(d1);
    expect(queue.size).toBe(1);

    // Vector has only A: 9, B: 5 -> cannot unblock
    let unblocked = queue.drainSatisfied({ 'peer-A': 9, 'peer-B': 5 });
    expect(unblocked).toHaveLength(0);
    expect(queue.size).toBe(1);

    // Vector advances to A: 10, B: 5 -> unblocks D1
    unblocked = queue.drainSatisfied({ 'peer-A': 10, 'peer-B': 5 });
    expect(unblocked).toHaveLength(1);
    expect(unblocked[0]!.logicalEventId).toBe('d-1');
    expect(queue.size).toBe(0);
  });

  it('cascades unblocking when multiple interdependent operations are buffered', () => {
    // op2 is origin A seq 2 (requires seq 1)
    const op2 = makeOp('op-2', 'peer-A', 2, 2, { 'peer-A': 1 });
    // op3 is origin A seq 3 (requires seq 2)
    const op3 = makeOp('op-3', 'peer-A', 3, 3, { 'peer-A': 2 });

    queue.enqueue(op3);
    queue.enqueue(op2);

    // Replication vector has seq 0 -> nothing unblocked
    expect(queue.drainSatisfied({ 'peer-A': 0 })).toHaveLength(0);

    // When seq 1 arrives, draining with vector { 'peer-A': 1 } should cascade to unblock both op2 and op3!
    const unblocked = queue.drainSatisfied({ 'peer-A': 1 });
    expect(unblocked.map((o) => o.logicalEventId)).toEqual(['op-2', 'op-3']);
    expect(queue.size).toBe(0);
  });

  it('calculates missing sequence ranges accurately', () => {
    const d1 = makeOp('d-1', 'peer-D', 13, 1, { 'peer-A': 10, 'peer-B': 5 });
    queue.enqueue(d1);

    const ranges = queue.getMissingRanges({ 'peer-A': 8, 'peer-B': 5 });
    expect(ranges).toHaveLength(1);
    expect(ranges[0]).toEqual({
      originPeerId: 'peer-A',
      fromSequence: 9,
      toSequence: 10,
    });
  });

  it('rejects enqueuing beyond capacity', () => {
    const smallQueue = new PendingDependencyQueue(2);
    smallQueue.enqueue(makeOp('op-1', 'peer-A', 1, 1));
    smallQueue.enqueue(makeOp('op-2', 'peer-A', 2, 2));

    expect(() => smallQueue.enqueue(makeOp('op-3', 'peer-A', 3, 3))).toThrow(
      SyncEngineError,
    );
  });
});
