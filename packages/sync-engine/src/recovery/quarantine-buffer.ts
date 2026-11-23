/**
 * @module @syncplay/sync-engine/recovery
 *
 * Generational Quarantine Buffer for Phase 7.5 M4.3a.
 *
 * Implements a dual-constraint bounded FIFO queue with double-buffered generational handoff:
 * - Hard Count Limit: 1,000 operations
 * - Hard Aggregate Byte Limit: 4 MiB (4,194,304 serialized UTF-8 bytes)
 * - Individual Event Limit: 4 KiB (4,096 bytes)
 * - Generational Handoff: detaching the active generation into an independently drainable
 *   batch while new arrivals enter a fresh active generation.
 *
 * Invariants:
 * 1. Count never exceeds 1,000 across retained storage.
 * 2. Total retained bytes never exceeds 4 MiB across retained storage.
 * 3. Individual event size never exceeds 4 KiB.
 * 4. Byte accounting strictly equals the sum of retained operation serializedByteLength.
 * 5. Stable operation identity (${originPeerId}:${sequenceNumber}) prevents duplicate retention.
 * 6. Generational detachment is explicit and prevents stale generation interference.
 * 7. Overflow triggers deterministic fail-closed rejection without silent eviction.
 */

import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { SyncEngineError } from '../errors.js';

export const MAX_QUARANTINE_OPERATIONS = 1000;
export const MAX_QUARANTINE_TOTAL_BYTES = 4 * 1024 * 1024; // 4 MiB (4,194,304 bytes)
export const MAX_QUARANTINE_EVENT_BYTES = 4096; // 4 KiB (4,096 bytes)
export const QUARANTINE_WARNING_THRESHOLD_BYTES = 3_355_443; // ~3.2 MiB (80%)

/**
 * Optional configuration for QuarantineBuffer limits.
 */
export interface QuarantineBufferConfig {
  readonly maxOperations?: number;
  readonly maxTotalBytes?: number;
  readonly maxEventBytes?: number;
}

/**
 * Envelope for a remote operation retained in the quarantine buffer.
 */
export interface QuarantinedOperation {
  readonly event: AnyControlSyncEvent;
  readonly originPeerId: string;
  readonly sequenceNumber: number;
  readonly membershipEpoch: number;
  readonly serializedByteLength: number;
  readonly receivedAtMs: number;
}

/**
 * Summary metrics of a quarantine drain execution.
 */
export interface QuarantineDrainResult {
  readonly totalOperations: number;
  readonly appliedCount: number;
  readonly acceptedPendingCount: number;
  readonly discardedMalformedEpoch: number;
  readonly discardedObsoleteEpoch: number;
  readonly discardedAlreadyApplied: number;
  readonly bufferedFutureEpoch: number;
  readonly drainDurationMs: number;
}

export class QuarantineBuffer {
  public readonly maxOperations: number;
  public readonly maxTotalBytes: number;
  public readonly maxEventBytes: number;

  private activeQueue: QuarantinedOperation[] = [];
  private drainBatch: QuarantinedOperation[] = [];

  private _activeBytes = 0;
  private _drainBytes = 0;
  private _generationId = 1;
  private _drainGenerationId: number | null = null;
  private _isDisposed = false;

  // Identity index: maps `${originPeerId}:${sequenceNumber}` to generation number
  private readonly identityIndex = new Map<string, number>();

  public constructor(config?: QuarantineBufferConfig) {
    this.maxOperations = config?.maxOperations ?? MAX_QUARANTINE_OPERATIONS;
    this.maxTotalBytes = config?.maxTotalBytes ?? MAX_QUARANTINE_TOTAL_BYTES;
    this.maxEventBytes = config?.maxEventBytes ?? MAX_QUARANTINE_EVENT_BYTES;
  }

  public get size(): number {
    return this.activeQueue.length;
  }

  public get drainSize(): number {
    return this.drainBatch.length;
  }

  public get totalRetainedCount(): number {
    return this.activeQueue.length + this.drainBatch.length;
  }

  public get totalRetainedBytes(): number {
    return this._activeBytes + this._drainBytes;
  }

  public get activeBytes(): number {
    return this._activeBytes;
  }

  public get drainBytes(): number {
    return this._drainBytes;
  }

  public get generationId(): number {
    return this._generationId;
  }

  public get drainGenerationId(): number | null {
    return this._drainGenerationId;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  public get isDrainComplete(): boolean {
    return this.drainBatch.length === 0;
  }

  /**
   * Checks whether an operation identity is already retained in either active or drain batch.
   */
  public hasOperation(originPeerId: string, sequenceNumber: number): boolean {
    return this.identityIndex.has(`${originPeerId}:${sequenceNumber}`);
  }

  /**
   * Enqueues an incoming remote operation into the active generation queue.
   *
   * @throws SyncEngineError('DISPOSED') if buffer is disposed.
   * @throws SyncEngineError('REJECTED_INVALID') if operation exceeds individual 4 KiB limit.
   * @throws SyncEngineError('QUARANTINE_OVERFLOW') if count or aggregate byte limits are breached.
   * @returns true if operation was newly admitted, false if it was an idempotent duplicate.
   */
  public enqueue(op: QuarantinedOperation): boolean {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineBuffer is disposed');
    }

    // 1. Validate individual event size boundary
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

    // 2. Duplicate detection: idempotent acceptance without duplicate allocation or counting
    if (this.identityIndex.has(key)) {
      return false;
    }

    // 3. Enforce count boundary
    if (this.totalRetainedCount + 1 > this.maxOperations) {
      throw new SyncEngineError(
        'QUARANTINE_OVERFLOW',
        `Quarantine buffer count limit reached (${this.maxOperations} operations)`,
      );
    }

    // 4. Enforce aggregate byte boundary
    if (this.totalRetainedBytes + op.serializedByteLength > this.maxTotalBytes) {
      throw new SyncEngineError(
        'QUARANTINE_OVERFLOW',
        `Quarantine buffer byte capacity reached (${this.maxTotalBytes} bytes)`,
      );
    }

    // Freeze operation envelope to prevent external mutation post-admission
    const frozenOp: QuarantinedOperation = Object.freeze({ ...op });

    this.activeQueue.push(frozenOp);
    this._activeBytes += frozenOp.serializedByteLength;
    this.identityIndex.set(key, this._generationId);

    return true;
  }

  /**
   * Generational Handoff:
   * Detaches the current active queue as a stable drain batch and installs a fresh active queue.
   * New operations entering during drain are assigned to the new active generation.
   *
   * @returns Readonly array of the detached drain batch.
   */
  public swapDrainBatch(): readonly QuarantinedOperation[] {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineBuffer is disposed');
    }

    // If a previous drain batch still has operations, append remaining items or merge generations
    if (this.drainBatch.length > 0) {
      // Concatenate remaining drainBatch with activeQueue into a new combined drain generation
      this.drainBatch = this.drainBatch.concat(this.activeQueue);
      this._drainBytes = this._drainBytes + this._activeBytes;
    } else {
      this.drainBatch = this.activeQueue;
      this._drainBytes = this._activeBytes;
    }

    this._drainGenerationId = this._generationId;

    // Reset active queue for the next generation
    this.activeQueue = [];
    this._activeBytes = 0;
    this._generationId++;

    return Object.freeze([...this.drainBatch]);
  }

  /**
   * Peeks at the next operation in the detached drain batch without removing it.
   */
  public peekDrain(): QuarantinedOperation | undefined {
    return this.drainBatch[0];
  }

  /**
   * Removes and returns the next operation from the detached drain batch (FIFO).
   * Atomically releases byte and identity accounting for the removed operation.
   */
  public shiftDrain(): QuarantinedOperation | undefined {
    if (this._isDisposed || this.drainBatch.length === 0) {
      return undefined;
    }

    const op = this.drainBatch.shift()!;
    this._drainBytes = Math.max(0, this._drainBytes - op.serializedByteLength);
    this.identityIndex.delete(`${op.originPeerId}:${op.sequenceNumber}`);

    if (this.drainBatch.length === 0) {
      this._drainGenerationId = null;
    }

    return op;
  }

  /**
   * Restores an unconfirmed operation to the head of the drain batch (for M4.3c recoverable failures).
   */
  public unshiftDrain(op: QuarantinedOperation): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineBuffer is disposed');
    }

    const key = `${op.originPeerId}:${op.sequenceNumber}`;
    this.drainBatch.unshift(op);
    this._drainBytes += op.serializedByteLength;
    this.identityIndex.set(key, this._drainGenerationId ?? this._generationId);
  }

  /**
   * Returns an immutable snapshot copy of all operations currently in the active queue.
   */
  public getActiveQueue(): readonly QuarantinedOperation[] {
    return Object.freeze([...this.activeQueue]);
  }

  /**
   * Returns an immutable snapshot copy of all operations currently in the drain batch.
   */
  public getDrainBatch(): readonly QuarantinedOperation[] {
    return Object.freeze([...this.drainBatch]);
  }

  /**
   * Clears all quarantined operations across active queue and drain batch.
   * Resets all byte counters, generation pointers, and identity indexes.
   */
  public clear(): void {
    this.activeQueue = [];
    this.drainBatch = [];
    this._activeBytes = 0;
    this._drainBytes = 0;
    this._drainGenerationId = null;
    this.identityIndex.clear();
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

