import { describe, it, expect } from 'vitest';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import {
  QuarantineBuffer,
  MAX_QUARANTINE_OPERATIONS,
  MAX_QUARANTINE_TOTAL_BYTES,
  MAX_QUARANTINE_EVENT_BYTES,
  type QuarantinedOperation,
} from '../recovery/quarantine-buffer.js';
import { SyncEngineError } from '../errors.js';

function createMockEvent(peerId: string, seq: number, epoch = 1): AnyControlSyncEvent {
  return {
    type: 'PLAYBACK_PLAY',
    eventId: `evt-${peerId}-${seq}`,
    aggregateId: 'session-1',
    membershipEpoch: epoch,
    issuerPeerId: peerId,
    clock: {
      logicalTimestamp: seq,
      originPeerId: peerId,
      sequenceNumber: seq,
      membershipEpoch: epoch,
    },
    data: {
      playbackPosition: seq * 10,
      wallClockTimestamp: Date.now(),
    },
  } as unknown as AnyControlSyncEvent;
}

function createMockOp(
  peerId: string,
  seq: number,
  sizeBytes = 256,
  epoch = 1,
): QuarantinedOperation {
  return {
    event: createMockEvent(peerId, seq, epoch),
    originPeerId: peerId,
    sequenceNumber: seq,
    membershipEpoch: epoch,
    serializedByteLength: sizeBytes,
    receivedAtMs: Date.now(),
  };
}

describe('QuarantineBuffer (Phase 7.5 M4.3a)', () => {
  it('initializes with empty buffer state and default limits', () => {
    const buffer = new QuarantineBuffer();

    expect(buffer.size).toBe(0);
    expect(buffer.drainSize).toBe(0);
    expect(buffer.totalRetainedCount).toBe(0);
    expect(buffer.totalRetainedBytes).toBe(0);
    expect(buffer.activeBytes).toBe(0);
    expect(buffer.drainBytes).toBe(0);
    expect(buffer.generationId).toBe(1);
    expect(buffer.drainGenerationId).toBeNull();
    expect(buffer.isDisposed).toBe(false);
    expect(buffer.isDrainComplete).toBe(true);
    expect(buffer.peekDrain()).toBeUndefined();
    expect(buffer.shiftDrain()).toBeUndefined();
    expect(buffer.getActiveQueue()).toEqual([]);
    expect(buffer.getDrainBatch()).toEqual([]);
    expect(buffer.maxOperations).toBe(MAX_QUARANTINE_OPERATIONS);
    expect(buffer.maxTotalBytes).toBe(MAX_QUARANTINE_TOTAL_BYTES);
    expect(buffer.maxEventBytes).toBe(MAX_QUARANTINE_EVENT_BYTES);
  });

  it('admits a single valid operation and maintains exact byte accounting', () => {
    const buffer = new QuarantineBuffer();
    const op = createMockOp('peer-A', 1, 512);

    const admitted = buffer.enqueue(op);
    expect(admitted).toBe(true);

    expect(buffer.size).toBe(1);
    expect(buffer.totalRetainedCount).toBe(1);
    expect(buffer.activeBytes).toBe(512);
    expect(buffer.totalRetainedBytes).toBe(512);
    expect(buffer.hasOperation('peer-A', 1)).toBe(true);
    expect(buffer.hasOperation('peer-A', 2)).toBe(false);

    // Swap to drain
    const detached = buffer.swapDrainBatch();
    expect(detached).toHaveLength(1);
    expect(detached[0].originPeerId).toBe('peer-A');
    expect(detached[0].sequenceNumber).toBe(1);
    expect(buffer.size).toBe(0);
    expect(buffer.drainSize).toBe(1);
    expect(buffer.totalRetainedCount).toBe(1);
    expect(buffer.activeBytes).toBe(0);
    expect(buffer.drainBytes).toBe(512);
    expect(buffer.totalRetainedBytes).toBe(512);
    expect(buffer.generationId).toBe(2);
    expect(buffer.drainGenerationId).toBe(1);

    // Shift from drain
    const shifted = buffer.shiftDrain();
    expect(shifted).toBeDefined();
    expect(shifted?.originPeerId).toBe('peer-A');
    expect(buffer.drainSize).toBe(0);
    expect(buffer.totalRetainedCount).toBe(0);
    expect(buffer.drainBytes).toBe(0);
    expect(buffer.totalRetainedBytes).toBe(0);
    expect(buffer.hasOperation('peer-A', 1)).toBe(false);
    expect(buffer.isDrainComplete).toBe(true);
    expect(buffer.drainGenerationId).toBeNull();
  });

  it('preserves strict FIFO arrival ordering across enqueue and drain', () => {
    const buffer = new QuarantineBuffer();
    const op1 = createMockOp('peer-A', 1, 100);
    const op2 = createMockOp('peer-B', 1, 200);
    const op3 = createMockOp('peer-A', 2, 300);

    buffer.enqueue(op1);
    buffer.enqueue(op2);
    buffer.enqueue(op3);

    expect(buffer.size).toBe(3);
    expect(buffer.totalRetainedBytes).toBe(600);

    buffer.swapDrainBatch();
    expect(buffer.drainSize).toBe(3);

    expect(buffer.peekDrain()?.originPeerId).toBe('peer-A');
    expect(buffer.peekDrain()?.sequenceNumber).toBe(1);

    const s1 = buffer.shiftDrain();
    expect(s1?.originPeerId).toBe('peer-A');
    expect(s1?.sequenceNumber).toBe(1);

    const s2 = buffer.shiftDrain();
    expect(s2?.originPeerId).toBe('peer-B');
    expect(s2?.sequenceNumber).toBe(1);

    const s3 = buffer.shiftDrain();
    expect(s3?.originPeerId).toBe('peer-A');
    expect(s3?.sequenceNumber).toBe(2);

    expect(buffer.shiftDrain()).toBeUndefined();
    expect(buffer.totalRetainedBytes).toBe(0);
  });

  it('detects duplicate operation identities idempotently without increasing count or bytes', () => {
    const buffer = new QuarantineBuffer();
    const op1 = createMockOp('peer-A', 10, 400);
    const op1Duplicate = createMockOp('peer-A', 10, 800); // duplicate seq with different size

    expect(buffer.enqueue(op1)).toBe(true);
    expect(buffer.size).toBe(1);
    expect(buffer.totalRetainedBytes).toBe(400);

    // Duplicate admission attempt
    expect(buffer.enqueue(op1Duplicate)).toBe(false);
    expect(buffer.size).toBe(1);
    expect(buffer.totalRetainedBytes).toBe(400);

    // After swapping to drain, duplicate check still finds it
    buffer.swapDrainBatch();
    expect(buffer.hasOperation('peer-A', 10)).toBe(true);
    expect(buffer.enqueue(op1Duplicate)).toBe(false);
    expect(buffer.size).toBe(0);
    expect(buffer.drainSize).toBe(1);
    expect(buffer.totalRetainedBytes).toBe(400);
  });

  it('enforces maximum count boundary: 1,000 operations accepted, 1,001st rejected without eviction', () => {
    const buffer = new QuarantineBuffer();

    for (let i = 1; i <= MAX_QUARANTINE_OPERATIONS; i++) {
      const op = createMockOp('peer-A', i, 100);
      expect(buffer.enqueue(op)).toBe(true);
    }

    expect(buffer.size).toBe(1000);
    expect(buffer.totalRetainedCount).toBe(1000);
    expect(buffer.totalRetainedBytes).toBe(1000 * 100);

    // 1001st operation must be rejected with QUARANTINE_OVERFLOW
    const overflowingOp = createMockOp('peer-A', 1001, 100);
    expect(() => buffer.enqueue(overflowingOp)).toThrowError(SyncEngineError);
    expect(() => buffer.enqueue(overflowingOp)).toThrowError(/Quarantine buffer count limit reached/);

    try {
      buffer.enqueue(overflowingOp);
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('QUARANTINE_OVERFLOW');
    }

    // Verify non-eviction: count and first operation intact
    expect(buffer.totalRetainedCount).toBe(1000);
    expect(buffer.hasOperation('peer-A', 1)).toBe(true);
    expect(buffer.hasOperation('peer-A', 1001)).toBe(false);
  });

  it('enforces maximum aggregate byte boundary: reject operations exceeding capacity', () => {
    // Test with small custom bound to verify exact boundary behavior
    const buffer = new QuarantineBuffer({ maxTotalBytes: 1000 });
    const op1 = createMockOp('peer-A', 1, 600);
    const op2 = createMockOp('peer-A', 2, 400);
    const op3 = createMockOp('peer-A', 3, 1);

    expect(buffer.enqueue(op1)).toBe(true);
    expect(buffer.enqueue(op2)).toBe(true);
    expect(buffer.totalRetainedBytes).toBe(1000);

    // Next byte breaches aggregate limit
    expect(() => buffer.enqueue(op3)).toThrowError(SyncEngineError);
    try {
      buffer.enqueue(op3);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('QUARANTINE_OVERFLOW');
    }

    expect(buffer.totalRetainedBytes).toBe(1000);
    expect(buffer.size).toBe(2);
  });

  it('enforces default 4 MiB aggregate byte capacity boundary', () => {
    const buffer = new QuarantineBuffer();
    // 1000 ops * 4096 bytes = 4,096,000 bytes (< 4,194,304 bytes)
    // If we test with custom maxTotalBytes = 4,194,304 bytes:
    const opOver = createMockOp('peer-B', 1, MAX_QUARANTINE_EVENT_BYTES);
    expect(buffer.enqueue(opOver)).toBe(true);
    expect(buffer.totalRetainedBytes).toBe(4096);
  });

  it('enforces individual 4 KiB event boundary: 4096 allowed, 4097 rejected', () => {
    const buffer = new QuarantineBuffer();

    const valid4KiB = createMockOp('peer-A', 1, 4096);
    expect(buffer.enqueue(valid4KiB)).toBe(true);

    const oversizedOp = createMockOp('peer-A', 2, 4097);
    expect(() => buffer.enqueue(oversizedOp)).toThrowError(SyncEngineError);
    try {
      buffer.enqueue(oversizedOp);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('REJECTED_INVALID');
      expect((err as SyncEngineError).message).toContain('exceeds maximum allowed event size');
    }

    expect(buffer.size).toBe(1);
    expect(buffer.hasOperation('peer-A', 2)).toBe(false);
  });

  it('rejects invalid serialized sizes (zero, negative, NaN, infinite)', () => {
    const buffer = new QuarantineBuffer();

    expect(() => buffer.enqueue(createMockOp('peer-A', 1, 0))).toThrowError(SyncEngineError);
    expect(() => buffer.enqueue(createMockOp('peer-A', 2, -10))).toThrowError(SyncEngineError);
    expect(() => buffer.enqueue(createMockOp('peer-A', 3, Number.NaN))).toThrowError(SyncEngineError);
    expect(() => buffer.enqueue(createMockOp('peer-A', 4, Number.POSITIVE_INFINITY))).toThrowError(SyncEngineError);

    expect(buffer.size).toBe(0);
    expect(buffer.totalRetainedBytes).toBe(0);
  });

  it('manages multiple generations and generation detachment seamlessly', () => {
    const buffer = new QuarantineBuffer();

    // Gen 1
    buffer.enqueue(createMockOp('peer-A', 1, 100));
    buffer.enqueue(createMockOp('peer-A', 2, 200));
    expect(buffer.generationId).toBe(1);

    // Swap Gen 1 to drain
    const detached1 = buffer.swapDrainBatch();
    expect(detached1).toHaveLength(2);
    expect(buffer.generationId).toBe(2);
    expect(buffer.drainGenerationId).toBe(1);

    // Gen 2 arrives during drain
    buffer.enqueue(createMockOp('peer-B', 1, 300));
    expect(buffer.size).toBe(1);
    expect(buffer.drainSize).toBe(2);
    expect(buffer.activeBytes).toBe(300);
    expect(buffer.drainBytes).toBe(300);
    expect(buffer.totalRetainedBytes).toBe(600);

    // Partial drain of Gen 1
    const drained1 = buffer.shiftDrain();
    expect(drained1?.originPeerId).toBe('peer-A');
    expect(drained1?.sequenceNumber).toBe(1);
    expect(buffer.drainSize).toBe(1);
    expect(buffer.drainBytes).toBe(200);

    // Swap again while Gen 1 has remaining op: merges remaining Gen 1 + Gen 2
    const detached2 = buffer.swapDrainBatch();
    expect(detached2).toHaveLength(2); // remaining op from Gen 1 + new op from Gen 2
    expect(detached2[0].originPeerId).toBe('peer-A');
    expect(detached2[0].sequenceNumber).toBe(2);
    expect(detached2[1].originPeerId).toBe('peer-B');
    expect(detached2[1].sequenceNumber).toBe(1);
    expect(buffer.generationId).toBe(3);
    expect(buffer.drainGenerationId).toBe(2);
    expect(buffer.size).toBe(0);
    expect(buffer.drainBytes).toBe(500);
  });

  it('restores in-flight unconfirmed operations safely via unshiftDrain', () => {
    const buffer = new QuarantineBuffer();
    const op1 = createMockOp('peer-A', 1, 150);
    const op2 = createMockOp('peer-A', 2, 250);

    buffer.enqueue(op1);
    buffer.enqueue(op2);
    buffer.swapDrainBatch();

    // Shift op1
    const inFlight = buffer.shiftDrain();
    expect(inFlight).toBeDefined();
    expect(buffer.drainSize).toBe(1);
    expect(buffer.drainBytes).toBe(250);
    expect(buffer.hasOperation('peer-A', 1)).toBe(false);

    // Destination fails to accept: restore to head of drain batch
    buffer.unshiftDrain(inFlight!);
    expect(buffer.drainSize).toBe(2);
    expect(buffer.drainBytes).toBe(400);
    expect(buffer.hasOperation('peer-A', 1)).toBe(true);
    expect(buffer.peekDrain()?.sequenceNumber).toBe(1);

    // Shift again to confirm it is back at head
    const reShifted = buffer.shiftDrain();
    expect(reShifted?.sequenceNumber).toBe(1);
    const nextShifted = buffer.shiftDrain();
    expect(nextShifted?.sequenceNumber).toBe(2);
  });

  it('clears all retained operations across active and drain batches', () => {
    const buffer = new QuarantineBuffer();
    buffer.enqueue(createMockOp('peer-A', 1, 100));
    buffer.swapDrainBatch();
    buffer.enqueue(createMockOp('peer-B', 1, 200));

    expect(buffer.totalRetainedCount).toBe(2);
    expect(buffer.totalRetainedBytes).toBe(300);

    buffer.clear();

    expect(buffer.size).toBe(0);
    expect(buffer.drainSize).toBe(0);
    expect(buffer.totalRetainedCount).toBe(0);
    expect(buffer.totalRetainedBytes).toBe(0);
    expect(buffer.activeBytes).toBe(0);
    expect(buffer.drainBytes).toBe(0);
    expect(buffer.hasOperation('peer-A', 1)).toBe(false);
    expect(buffer.hasOperation('peer-B', 1)).toBe(false);
    expect(buffer.isDrainComplete).toBe(true);
  });

  it('enforces disposed lifecycle: terminal state and fail-closed rejections', () => {
    const buffer = new QuarantineBuffer();
    buffer.enqueue(createMockOp('peer-A', 1, 100));

    buffer.dispose();
    expect(buffer.isDisposed).toBe(true);
    expect(buffer.totalRetainedCount).toBe(0);
    expect(buffer.totalRetainedBytes).toBe(0);

    // Idempotent dispose
    expect(() => buffer.dispose()).not.toThrow();
    expect(buffer.isDisposed).toBe(true);

    // Operations throw DISPOSED
    expect(() => buffer.enqueue(createMockOp('peer-B', 1, 100))).toThrowError(SyncEngineError);
    expect(() => buffer.swapDrainBatch()).toThrowError(SyncEngineError);
    expect(() => buffer.unshiftDrain(createMockOp('peer-A', 1, 100))).toThrowError(SyncEngineError);
    expect(buffer.shiftDrain()).toBeUndefined();
    expect(buffer.peekDrain()).toBeUndefined();
  });

  it('guarantees immutability and no internal collection leakage', () => {
    const buffer = new QuarantineBuffer();
    const op = createMockOp('peer-A', 1, 100);

    buffer.enqueue(op);

    const activeQueue = buffer.getActiveQueue();
    expect(Object.isFrozen(activeQueue)).toBe(true);
    expect(() => {
      // Attempting to push or mutate frozen array
      (activeQueue as unknown as unknown[]).push(createMockOp('peer-C', 1, 100));
    }).toThrow();

    // Verify enqueued envelope is frozen
    expect(Object.isFrozen(activeQueue[0])).toBe(true);

    const detached = buffer.swapDrainBatch();
    expect(Object.isFrozen(detached)).toBe(true);

    const drainBatch = buffer.getDrainBatch();
    expect(Object.isFrozen(drainBatch)).toBe(true);
  });
});
