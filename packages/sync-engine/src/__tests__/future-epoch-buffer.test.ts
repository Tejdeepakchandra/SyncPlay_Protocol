import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import {
  FutureEpochBuffer,
  MAX_FUTURE_EPOCH_OPERATIONS,
  MAX_FUTURE_EPOCH_BYTES,
  MAX_FUTURE_EPOCH_EVENT_BYTES,
  DEFAULT_EPOCH_CONVERGENCE_GRACE_MS,
} from '../recovery/future-epoch-buffer.js';
import type { QuarantinedOperation } from '../recovery/quarantine-buffer.js';
import { createEpochTransitionCapability, type EpochTransitionCapability } from '../engine/leaderless-sync-engine.js';
import { SyncEngineError } from '../errors.js';

function createMockEvent(peerId: string, seq: number, epoch: number): AnyControlSyncEvent {
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
  epoch: number,
  sizeBytes = 256,
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

describe('FutureEpochBuffer (Phase 7.5 M4.3a)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('initializes with empty buffer state and default limits', () => {
    const buffer = new FutureEpochBuffer();

    expect(buffer.size).toBe(0);
    expect(buffer.totalCount).toBe(0);
    expect(buffer.totalBytes).toBe(0);
    expect(buffer.isDisposed).toBe(false);
    expect(buffer.getBufferedEpochs()).toEqual([]);
    expect(buffer.maxOperations).toBe(MAX_FUTURE_EPOCH_OPERATIONS);
    expect(buffer.maxTotalBytes).toBe(MAX_FUTURE_EPOCH_BYTES);
    expect(buffer.maxEventBytes).toBe(MAX_FUTURE_EPOCH_EVENT_BYTES);
    expect(buffer.gracePeriodMs).toBe(DEFAULT_EPOCH_CONVERGENCE_GRACE_MS);
  });

  it('admits a valid future-epoch operation and sets up grace timer', () => {
    const buffer = new FutureEpochBuffer();
    const op = createMockOp('peer-A', 1, 2, 512);

    const admitted = buffer.bufferFutureEpoch(op, 1);
    expect(admitted).toBe(true);

    expect(buffer.size).toBe(1);
    expect(buffer.totalBytes).toBe(512);
    expect(buffer.hasOperation('peer-A', 1)).toBe(true);
    expect(buffer.hasActiveTimer(2)).toBe(true);
    expect(buffer.getBufferedEpochs()).toEqual([2]);
    expect(buffer.getOperationsForEpoch(2)).toHaveLength(1);
  });

  it('rejects operations where membershipEpoch <= currentEpoch', () => {
    const buffer = new FutureEpochBuffer();

    // Equal to current epoch (current-epoch operation)
    const currentOp = createMockOp('peer-A', 1, 1, 100);
    expect(() => buffer.bufferFutureEpoch(currentOp, 1)).toThrowError(SyncEngineError);
    try {
      buffer.bufferFutureEpoch(currentOp, 1);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('REJECTED_INVALID');
      expect((err as SyncEngineError).message).toContain('must be strictly greater than current epoch');
    }

    // Older than current epoch (obsolete-epoch operation)
    const oldOp = createMockOp('peer-A', 2, 1, 100);
    expect(() => buffer.bufferFutureEpoch(oldOp, 2)).toThrowError(SyncEngineError);
    try {
      buffer.bufferFutureEpoch(oldOp, 2);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('REJECTED_INVALID');
    }

    expect(buffer.size).toBe(0);
  });

  it('rejects invalid membership epochs (negative, zero, float, non-number)', () => {
    const buffer = new FutureEpochBuffer();

    expect(() => buffer.bufferFutureEpoch(createMockOp('peer-A', 1, 0), 0)).toThrowError(SyncEngineError);
    expect(() => buffer.bufferFutureEpoch(createMockOp('peer-A', 2, -1), -2)).toThrowError(SyncEngineError);
    expect(() => buffer.bufferFutureEpoch(createMockOp('peer-A', 3, 2.5), 1)).toThrowError(SyncEngineError);

    expect(buffer.size).toBe(0);
  });

  it('enforces maximum 100 operations count boundary', () => {
    const buffer = new FutureEpochBuffer();

    for (let i = 1; i <= MAX_FUTURE_EPOCH_OPERATIONS; i++) {
      const op = createMockOp('peer-A', i, 2, 50);
      expect(buffer.bufferFutureEpoch(op, 1)).toBe(true);
    }

    expect(buffer.totalCount).toBe(100);
    expect(buffer.totalBytes).toBe(5000);

    // 101st operation throws FUTURE_EPOCH_OVERFLOW
    const overflowingOp = createMockOp('peer-A', 101, 2, 50);
    expect(() => buffer.bufferFutureEpoch(overflowingOp, 1)).toThrowError(SyncEngineError);
    try {
      buffer.bufferFutureEpoch(overflowingOp, 1);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('FUTURE_EPOCH_OVERFLOW');
      expect((err as SyncEngineError).message).toContain('FutureEpochBuffer count limit reached');
    }

    expect(buffer.totalCount).toBe(100);
  });

  it('enforces maximum 512 KiB aggregate byte boundary', () => {
    const buffer = new FutureEpochBuffer({ maxTotalBytes: 1000 });

    const op1 = createMockOp('peer-A', 1, 2, 600);
    const op2 = createMockOp('peer-A', 2, 2, 400);
    const op3 = createMockOp('peer-A', 3, 2, 1);

    expect(buffer.bufferFutureEpoch(op1, 1)).toBe(true);
    expect(buffer.bufferFutureEpoch(op2, 1)).toBe(true);
    expect(buffer.totalBytes).toBe(1000);

    // Exceeds aggregate limit
    expect(() => buffer.bufferFutureEpoch(op3, 1)).toThrowError(SyncEngineError);
    try {
      buffer.bufferFutureEpoch(op3, 1);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('FUTURE_EPOCH_OVERFLOW');
      expect((err as SyncEngineError).message).toContain('byte capacity reached');
    }

    expect(buffer.totalBytes).toBe(1000);
    expect(buffer.size).toBe(2);
  });

  it('enforces individual 4 KiB event boundary', () => {
    const buffer = new FutureEpochBuffer();

    const valid4KiB = createMockOp('peer-A', 1, 2, 4096);
    expect(buffer.bufferFutureEpoch(valid4KiB, 1)).toBe(true);

    const oversized = createMockOp('peer-A', 2, 2, 4097);
    expect(() => buffer.bufferFutureEpoch(oversized, 1)).toThrowError(SyncEngineError);
    try {
      buffer.bufferFutureEpoch(oversized, 1);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('REJECTED_INVALID');
    }

    expect(buffer.size).toBe(1);
  });

  it('detects duplicate operations idempotently without increasing count or resetting timer', () => {
    let timerCount = 0;
    const buffer = new FutureEpochBuffer({
      setTimeoutFn: (h, ms) => {
        timerCount++;
        return setTimeout(h, ms);
      },
    });

    const op1 = createMockOp('peer-A', 1, 2, 200);
    const op1Duplicate = createMockOp('peer-A', 1, 2, 400);

    expect(buffer.bufferFutureEpoch(op1, 1)).toBe(true);
    expect(buffer.size).toBe(1);
    expect(buffer.totalBytes).toBe(200);
    expect(timerCount).toBe(1);

    // Duplicate admission
    expect(buffer.bufferFutureEpoch(op1Duplicate, 1)).toBe(false);
    expect(buffer.size).toBe(1);
    expect(buffer.totalBytes).toBe(200);
    expect(timerCount).toBe(1); // Timer was NOT reset or rescheduled
  });

  it('repeated admissions to the same epoch do NOT extend the original grace period', () => {
    const expiredCallback = vi.fn();
    const buffer = new FutureEpochBuffer({
      onGraceExpired: expiredCallback,
      gracePeriodMs: 5000,
    });

    // Op 1 arrives at t = 0
    buffer.bufferFutureEpoch(createMockOp('peer-A', 1, 2, 100), 1);

    // Advance 3000 ms
    vi.advanceTimersByTime(3000);

    // Op 2 arrives at t = 3000 ms for the SAME epoch
    buffer.bufferFutureEpoch(createMockOp('peer-A', 2, 2, 100), 1);
    expect(buffer.size).toBe(2);

    // Advance 1999 ms (t = 4999 ms). Timer should not have fired yet.
    vi.advanceTimersByTime(1999);
    expect(expiredCallback).not.toHaveBeenCalled();
    expect(buffer.size).toBe(2);

    // Advance 1 ms (t = 5000 ms). Timer expires exactly at 5000 ms from first admission!
    vi.advanceTimersByTime(1);
    expect(expiredCallback).toHaveBeenCalledTimes(1);
    expect(expiredCallback).toHaveBeenCalledWith(2, expect.any(Array));
    expect(buffer.size).toBe(0);
    expect(buffer.totalBytes).toBe(0);
  });

  it('releases buffered operations in FIFO arrival order with valid capability', () => {
    const validCap = createEpochTransitionCapability('coord', 'session-1');
    const buffer = new FutureEpochBuffer({
      authorizedCapability: validCap,
    });

    buffer.bufferFutureEpoch(createMockOp('peer-A', 1, 2, 100), 1);
    buffer.bufferFutureEpoch(createMockOp('peer-B', 1, 2, 200), 1);
    buffer.bufferFutureEpoch(createMockOp('peer-A', 2, 2, 150), 1);

    expect(buffer.size).toBe(3);
    expect(buffer.totalBytes).toBe(450);
    expect(buffer.hasActiveTimer(2)).toBe(true);

    // Attempt release with unauthorized capability
    const unauthorizedCap = createEpochTransitionCapability('malicious', 'session-1');
    expect(() => buffer.releaseWithCapability(unauthorizedCap, 2)).toThrowError(SyncEngineError);
    try {
      buffer.releaseWithCapability(unauthorizedCap, 2);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }

    // Attempt release with malformed capability
    expect(() => buffer.releaseWithCapability({} as unknown as EpochTransitionCapability, 2)).toThrowError(SyncEngineError);

    // Release with authorized capability
    const released = buffer.releaseWithCapability(validCap, 2);
    expect(released).toHaveLength(3);
    expect(released[0].originPeerId).toBe('peer-A');
    expect(released[0].sequenceNumber).toBe(1);
    expect(released[1].originPeerId).toBe('peer-B');
    expect(released[1].sequenceNumber).toBe(1);
    expect(released[2].originPeerId).toBe('peer-A');
    expect(released[2].sequenceNumber).toBe(2);

    expect(buffer.size).toBe(0);
    expect(buffer.totalBytes).toBe(0);
    expect(buffer.hasActiveTimer(2)).toBe(false);

    // Verify timer does not fire after release
    vi.advanceTimersByTime(10000);
    expect(buffer.size).toBe(0);
  });

  it('clears all buffered operations and active timers cleanly', () => {
    const buffer = new FutureEpochBuffer();
    buffer.bufferFutureEpoch(createMockOp('peer-A', 1, 2, 100), 1);
    buffer.bufferFutureEpoch(createMockOp('peer-A', 2, 3, 200), 1);

    expect(buffer.getBufferedEpochs()).toEqual([2, 3]);
    expect(buffer.size).toBe(2);
    expect(buffer.totalBytes).toBe(300);

    buffer.clear();

    expect(buffer.size).toBe(0);
    expect(buffer.totalBytes).toBe(0);
    expect(buffer.hasActiveTimer(2)).toBe(false);
    expect(buffer.hasActiveTimer(3)).toBe(false);
    expect(buffer.getBufferedEpochs()).toEqual([]);
  });

  it('enforces terminal disposed state and fences delayed callbacks', () => {
    const expiredCallback = vi.fn();
    const buffer = new FutureEpochBuffer({
      onGraceExpired: expiredCallback,
      gracePeriodMs: 5000,
    });

    buffer.bufferFutureEpoch(createMockOp('peer-A', 1, 2, 100), 1);
    buffer.dispose();

    expect(buffer.isDisposed).toBe(true);
    expect(buffer.size).toBe(0);

    // Idempotent dispose
    expect(() => buffer.dispose()).not.toThrow();

    // Operations throw DISPOSED
    expect(() => buffer.bufferFutureEpoch(createMockOp('peer-B', 1, 2, 100), 1)).toThrowError(SyncEngineError);
    const cap = createEpochTransitionCapability('coord', 'session-1');
    expect(() => buffer.releaseWithCapability(cap, 2)).toThrowError(SyncEngineError);

    // Timer firing after dispose must not invoke callback
    vi.advanceTimersByTime(10000);
    expect(expiredCallback).not.toHaveBeenCalled();
  });
});
