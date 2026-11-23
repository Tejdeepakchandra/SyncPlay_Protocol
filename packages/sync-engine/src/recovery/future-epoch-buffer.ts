/**
 * @module @syncplay/sync-engine/recovery
 *
 * Future-Epoch Buffer for Phase 7.5 M4.3a.
 *
 * Implements a bounded staging container for operations arriving from future membership epochs
 * (op.epoch > currentEpoch) while the local engine awaits an authorized EpochTransitionCapability.
 *
 * Hard Bounds & Timing:
 * - Max Operation Count: 100 operations
 * - Max Aggregate Bytes: 512 KiB (524,288 serialized UTF-8 bytes)
 * - Individual Event Limit: 4 KiB (4,096 bytes)
 * - Grace Period: 5,000 ms (5 seconds) per future epoch
 *
 * Invariants:
 * 1. Count never exceeds 100 across all buffered future epochs.
 * 2. Aggregate serialized bytes never exceed 512 KiB.
 * 3. Individual operation size never exceeds 4 KiB.
 * 4. Current-epoch replication vector and playback state are NEVER advanced by future-epoch buffering.
 * 5. Releasing operations strictly requires an authorized EpochTransitionCapability.
 * 6. Repeated admissions do not extend the original 5-second grace period for a future epoch.
 * 7. Operations expire deterministically if transition capability does not arrive within 5,000 ms.
 * 8. Stale timer callbacks are fenced and cannot resurrect cleared or disposed operations.
 */

import { SyncEngineError } from '../errors.js';
import type { EpochTransitionCapability } from '../engine/leaderless-sync-engine.js';
import type { QuarantinedOperation } from './quarantine-buffer.js';

export const MAX_FUTURE_EPOCH_OPERATIONS = 100;
export const MAX_FUTURE_EPOCH_BYTES = 512 * 1024; // 512 KiB (524,288 bytes)
export const MAX_FUTURE_EPOCH_EVENT_BYTES = 4096; // 4 KiB (4,096 bytes)
export const DEFAULT_EPOCH_CONVERGENCE_GRACE_MS = 5000; // 5,000 ms

export interface FutureEpochBufferConfig {
  readonly maxOperations?: number;
  readonly maxTotalBytes?: number;
  readonly maxEventBytes?: number;
  readonly gracePeriodMs?: number;
  readonly nowFn?: () => number;
  readonly setTimeoutFn?: (handler: () => void, timeoutMs: number) => unknown;
  readonly clearTimeoutFn?: (timerId: unknown) => void;
  readonly onGraceExpired?: (targetEpoch: number, expiredOps: readonly QuarantinedOperation[]) => void;
  readonly authorizedCapability?: EpochTransitionCapability;
}

export class FutureEpochBuffer {
  public readonly maxOperations: number;
  public readonly maxTotalBytes: number;
  public readonly maxEventBytes: number;
  public readonly gracePeriodMs: number;

  private readonly nowFn: () => number;
  private readonly setTimeoutFn: (handler: () => void, timeoutMs: number) => unknown;
  private readonly clearTimeoutFn: (timerId: unknown) => void;
  private readonly onGraceExpired?: (targetEpoch: number, expiredOps: readonly QuarantinedOperation[]) => void;
  private readonly authorizedCapability?: EpochTransitionCapability;

  // Stored operations grouped by target epoch in FIFO arrival order
  private readonly epochBuckets = new Map<number, QuarantinedOperation[]>();

  // Active grace timers per target epoch
  private readonly epochTimers = new Map<number, unknown>();

  // Identity index: maps `${originPeerId}:${sequenceNumber}` to target epoch
  private readonly identityIndex = new Map<string, number>();

  private _totalBytes = 0;
  private _totalCount = 0;
  private _isDisposed = false;

  public constructor(config?: FutureEpochBufferConfig) {
    this.maxOperations = config?.maxOperations ?? MAX_FUTURE_EPOCH_OPERATIONS;
    this.maxTotalBytes = config?.maxTotalBytes ?? MAX_FUTURE_EPOCH_BYTES;
    this.maxEventBytes = config?.maxEventBytes ?? MAX_FUTURE_EPOCH_EVENT_BYTES;
    this.gracePeriodMs = config?.gracePeriodMs ?? DEFAULT_EPOCH_CONVERGENCE_GRACE_MS;

    this.nowFn = config?.nowFn ?? Date.now;
    this.setTimeoutFn = config?.setTimeoutFn ?? ((handler, ms) => setTimeout(handler, ms));
    this.clearTimeoutFn = config?.clearTimeoutFn ?? ((id) => clearTimeout(id as ReturnType<typeof setTimeout>));
    this.onGraceExpired = config?.onGraceExpired;
    this.authorizedCapability = config?.authorizedCapability
      ? Object.freeze({ ...config.authorizedCapability })
      : undefined;
  }

  public get size(): number {
    return this._totalCount;
  }

  public get totalCount(): number {
    return this._totalCount;
  }

  public get totalBytes(): number {
    return this._totalBytes;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  /**
   * Checks whether an operation identity is already retained.
   */
  public hasOperation(originPeerId: string, sequenceNumber: number): boolean {
    return this.identityIndex.has(`${originPeerId}:${sequenceNumber}`);
  }

  /**
   * Checks whether a grace timer is actively running for a given target epoch.
   */
  public hasActiveTimer(epoch: number): boolean {
    return this.epochTimers.has(epoch);
  }

  /**
   * Returns a list of all distinct future epochs currently buffered.
   */
  public getBufferedEpochs(): readonly number[] {
    return Object.freeze(Array.from(this.epochBuckets.keys()).sort((a, b) => a - b));
  }

  /**
   * Returns an immutable copy of operations buffered for a given epoch.
   */
  public getOperationsForEpoch(epoch: number): readonly QuarantinedOperation[] {
    const bucket = this.epochBuckets.get(epoch);
    return bucket ? Object.freeze([...bucket]) : Object.freeze([]);
  }

  /**
   * Buffers an incoming future-epoch operation awaiting authorized epoch transition.
   *
   * @param op QuarantinedOperation envelope.
   * @param currentEpoch Current engine membership epoch.
   * @throws SyncEngineError('DISPOSED') if buffer is disposed.
   * @throws SyncEngineError('REJECTED_INVALID') if op.epoch <= currentEpoch or op exceeds individual limit.
   * @throws SyncEngineError('FUTURE_EPOCH_OVERFLOW') if count or aggregate byte limits are breached.
   * @returns true if newly admitted, false if idempotent duplicate.
   */
  public bufferFutureEpoch(op: QuarantinedOperation, currentEpoch: number): boolean {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'FutureEpochBuffer is disposed');
    }

    // 1. Validate epoch strictly greater than current and strictly positive
    if (
      typeof op.membershipEpoch !== 'number' ||
      !Number.isInteger(op.membershipEpoch) ||
      op.membershipEpoch <= 0
    ) {
      throw new SyncEngineError(
        'REJECTED_INVALID',
        `Operation membership epoch (${op.membershipEpoch}) must be a positive integer`,
      );
    }

    if (op.membershipEpoch <= currentEpoch) {
      throw new SyncEngineError(
        'REJECTED_INVALID',
        `Operation membership epoch (${op.membershipEpoch}) must be strictly greater than current epoch (${currentEpoch})`,
      );
    }

    // 2. Validate individual event size boundary
    if (
      typeof op.serializedByteLength !== 'number' ||
      !Number.isFinite(op.serializedByteLength) ||
      op.serializedByteLength <= 0 ||
      op.serializedByteLength > this.maxEventBytes
    ) {
      throw new SyncEngineError(
        'REJECTED_INVALID',
        `Operation serialized size (${op.serializedByteLength} bytes) exceeds maximum allowed event size (${this.maxEventBytes} bytes)`,
      );
    }

    const key = `${op.originPeerId}:${op.sequenceNumber}`;

    // 3. Duplicate detection: idempotent acceptance without duplicate accounting or timer reset
    if (this.identityIndex.has(key)) {
      return false;
    }

    // 4. Enforce count boundary (100 operations)
    if (this._totalCount + 1 > this.maxOperations) {
      throw new SyncEngineError(
        'FUTURE_EPOCH_OVERFLOW',
        `FutureEpochBuffer count limit reached (${this.maxOperations} operations)`,
      );
    }

    // 5. Enforce aggregate byte boundary (512 KiB)
    if (this._totalBytes + op.serializedByteLength > this.maxTotalBytes) {
      throw new SyncEngineError(
        'FUTURE_EPOCH_OVERFLOW',
        `FutureEpochBuffer byte capacity reached (${this.maxTotalBytes} bytes)`,
      );
    }

    // Freeze envelope to prevent external mutation post-admission
    const frozenOp: QuarantinedOperation = Object.freeze({ ...op });

    let bucket = this.epochBuckets.get(frozenOp.membershipEpoch);
    if (!bucket) {
      bucket = [];
      this.epochBuckets.set(frozenOp.membershipEpoch, bucket);
    }

    bucket.push(frozenOp);
    this._totalCount++;
    this._totalBytes += frozenOp.serializedByteLength;
    this.identityIndex.set(key, frozenOp.membershipEpoch);

    // 6. Grace period timer: start timer for this target epoch if not already running.
    // Repeated admissions do NOT extend or reset the timer.
    if (!this.epochTimers.has(frozenOp.membershipEpoch)) {
      const targetEpoch = frozenOp.membershipEpoch;
      const timerId = this.setTimeoutFn(() => {
        this.handleGraceTimeout(targetEpoch);
      }, this.gracePeriodMs);
      this.epochTimers.set(targetEpoch, timerId);
    }

    return true;
  }

  /**
   * Releases buffered future-epoch operations for targetEpoch upon presentation of
   * an authorized EpochTransitionCapability.
   *
   * @param capability Authorized EpochTransitionCapability.
   * @param targetEpoch Target membership epoch being transitioned to.
   * @throws SyncEngineError('DISPOSED') if buffer is disposed.
   * @throws SyncEngineError('UNAUTHORIZED') if capability is missing or unauthorized.
   * @returns Readonly array of released operations in FIFO arrival order.
   */
  public releaseWithCapability(
    capability: EpochTransitionCapability,
    targetEpoch: number,
  ): readonly QuarantinedOperation[] {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'FutureEpochBuffer is disposed');
    }

    // 1. Verify capability authenticity
    if (
      !capability ||
      typeof capability !== 'object' ||
      typeof capability.authority !== 'string' ||
      capability.authority.trim().length === 0 ||
      typeof capability.sessionId !== 'string' ||
      capability.sessionId.trim().length === 0 ||
      typeof capability.token !== 'string' ||
      capability.token.trim().length === 0
    ) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        'Invalid or missing EpochTransitionCapability',
      );
    }

    // If an authorized capability reference was configured, verify referential match or token match
    if (this.authorizedCapability) {
      if (
        capability !== this.authorizedCapability &&
        capability.token !== this.authorizedCapability.token
      ) {
        throw new SyncEngineError(
          'UNAUTHORIZED',
          'Epoch transition requires authorized EpochTransitionCapability',
        );
      }
    }

    // 2. Cancel and clean up grace timer for target epoch
    this.cancelEpochTimer(targetEpoch);

    // 3. Extract and release operations
    const bucket = this.epochBuckets.get(targetEpoch);
    if (!bucket || bucket.length === 0) {
      this.epochBuckets.delete(targetEpoch);
      return Object.freeze([]);
    }

    this.epochBuckets.delete(targetEpoch);

    // Deduct accounting
    for (const op of bucket) {
      this._totalCount--;
      this._totalBytes = Math.max(0, this._totalBytes - op.serializedByteLength);
      this.identityIndex.delete(`${op.originPeerId}:${op.sequenceNumber}`);
    }

    return Object.freeze([...bucket]);
  }

  /**
   * Handles grace period expiration for an epoch bucket.
   */
  private handleGraceTimeout(targetEpoch: number): void {
    if (this._isDisposed) {
      return;
    }

    // Clear timer record
    this.epochTimers.delete(targetEpoch);

    const bucket = this.epochBuckets.get(targetEpoch);
    if (!bucket || bucket.length === 0) {
      this.epochBuckets.delete(targetEpoch);
      return;
    }

    this.epochBuckets.delete(targetEpoch);

    // Deduct accounting for expired operations
    for (const op of bucket) {
      this._totalCount--;
      this._totalBytes = Math.max(0, this._totalBytes - op.serializedByteLength);
      this.identityIndex.delete(`${op.originPeerId}:${op.sequenceNumber}`);
    }

    const expiredOps = Object.freeze([...bucket]);
    this.onGraceExpired?.(targetEpoch, expiredOps);
  }

  private cancelEpochTimer(epoch: number): void {
    const timerId = this.epochTimers.get(epoch);
    if (timerId !== undefined) {
      this.clearTimeoutFn(timerId);
      this.epochTimers.delete(epoch);
    }
  }

  /**
   * Clears all buffered operations across all epochs and cancels all active timers.
   */
  public clear(): void {
    for (const timerId of this.epochTimers.values()) {
      this.clearTimeoutFn(timerId);
    }
    this.epochTimers.clear();
    this.epochBuckets.clear();
    this.identityIndex.clear();
    this._totalCount = 0;
    this._totalBytes = 0;
  }

  /**
   * Disposes the buffer idempotently. Terminal state.
   */
  public dispose(): void {
    if (this._isDisposed) {
      return;
    }
    this._isDisposed = true;
    this.clear();
  }
}

