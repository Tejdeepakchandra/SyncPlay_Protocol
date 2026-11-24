/**
 * @module @syncplay/sync-engine/recovery
 *
 * Quarantine Drainer for Phase 7.5 M4.3c.
 *
 * Implements the authoritative DRAINER_IN_FLIGHT protocol per approved M4.3-R4 specifications:
 * - Explicit In-Flight Ownership: An operation dequeued from QuarantineBuffer is held in
 *   `inFlightRecord` (state DRAINER_IN_FLIGHT) until the destination confirms acceptance.
 * - Single-Operation Cardinality: Bounded to exactly N_in-flight <= 1 concurrent operation.
 * - Destination Confirmation Receipts: Synchronous, typed DestinationAcceptanceReceipt
 *   confirming ACCEPTED_REDUCED, ACCEPTED_PENDING, ALREADY_REPRESENTED, ALREADY_OWNED_DUPLICATE,
 *   TERMINALLY_REJECTED, or CAPACITY_REJECTED.
 * - Zero Operation Loss: If destination fails with capacity rejection or an exception,
 *   the in-flight operation is preserved in inFlightRecord (or unshifted back to queue)
 *   and drain halts immediately, transitioning to DEGRADED.
 * - Reentrancy Exclusion: Drain pass is guarded against concurrent or reentrant drain invocations.
 */

import { SyncEngineError } from '../errors.js';
import type { QuarantineBuffer, QuarantinedOperation, QuarantineDrainResult } from './quarantine-buffer.js';

export type { QuarantineDrainResult };

/**
 * Formal lifecycle states distinguishing ownership and progress of individual operations
 * during recovery and quarantine drain.
 */
export type DrainedOperationState =
  | 'NOT_YET_DRAINED'
  | 'DRAINER_IN_FLIGHT'
  | 'SUCCESSFULLY_REDUCED'
  | 'SUCCESSFULLY_INSERTED_PENDING'
  | 'ALREADY_REPRESENTED_BY_FRONTIER'
  | 'ALREADY_OWNED_DUPLICATE'
  | 'TERMINALLY_REJECTED'
  | 'RECOVERABLE_DESTINATION_REJECTION';

/**
 * Detailed status of an in-flight operation during drain processing.
 */
export type DestinationAcceptanceStatus =
  | 'PENDING'
  | 'ACCEPTED_REDUCED'
  | 'ACCEPTED_PENDING'
  | 'ALREADY_REPRESENTED'
  | 'ALREADY_OWNED_DUPLICATE'
  | 'TERMINALLY_REJECTED'
  | 'CAPACITY_REJECTED';

/**
 * Authoritative in-flight tracking record owned exclusively by QuarantineDrainer.
 * Cardinality is bounded to at most 1 operation in flight at any instant.
 */
export interface DrainedInFlightRecord {
  readonly operation: QuarantinedOperation;
  readonly dequeuedAtMs: number;
  attemptCount: number;
  destinationStatus: DestinationAcceptanceStatus;
}

/**
 * Synchronous confirmation receipt returned by the destination engine container
 * upon receiving an in-flight operation from QuarantineDrainer.
 */
export type DestinationAcceptanceReceipt =
  | { readonly status: 'ACCEPTED_REDUCED'; readonly vectorAdvanced: true; readonly newSequence: number }
  | { readonly status: 'ACCEPTED_PENDING'; readonly vectorAdvanced: false; readonly reason: 'MISSING_DEPENDENCY' }
  | { readonly status: 'ALREADY_REPRESENTED'; readonly vectorAdvanced: false }
  | { readonly status: 'ALREADY_OWNED_DUPLICATE'; readonly vectorAdvanced: false }
  | { readonly status: 'TERMINALLY_REJECTED'; readonly vectorAdvanced: false; readonly error: string }
  | { readonly status: 'CAPACITY_REJECTED'; readonly vectorAdvanced: false; readonly currentQueueSize: number; readonly maxCapacity: number };

/**
 * Destination engine contract required by QuarantineDrainer.
 */
export interface IDrainDestination {
  /**
   * Submits an in-flight quarantined operation to destination for admission/reduction.
   */
  admitDrainedOperation(op: QuarantinedOperation): DestinationAcceptanceReceipt;

  /**
   * Queries whether destination container already holds the operation.
   */
  hasOperation?(originPeerId: string, sequenceNumber: number): boolean;
}

/**
 * Configuration options for QuarantineDrainer.
 */
export interface QuarantineDrainerConfig {
  readonly destination: IDrainDestination;
  readonly quarantineBuffer: QuarantineBuffer;
  readonly nowFn?: () => number;
}

/**
 * Authoritative drain execution component responsible for sequential operation admission,
 * in-flight ownership tracking, and destination confirmation verification.
 */
export interface IQuarantineDrainer {
  readonly inFlightRecord: DrainedInFlightRecord | null;
  readonly isDraining: boolean;
  readonly isDisposed: boolean;

  drainBatch(batch?: readonly QuarantinedOperation[]): Promise<QuarantineDrainResult>;
  resumeDrain(): void;
  restoreInFlightToQueue(): void;
  clearInFlight(): void;
  isInFlight(originPeerId: string, sequenceNumber: number): boolean;
  dispose(): void;
}

export class QuarantineDrainer implements IQuarantineDrainer {
  private readonly destination: IDrainDestination;
  private readonly quarantineBuffer: QuarantineBuffer;
  private readonly nowFn: () => number;

  private _inFlightRecord: DrainedInFlightRecord | null = null;
  private _isDraining = false;
  private _isDisposed = false;

  constructor(config: QuarantineDrainerConfig) {
    if (!config || typeof config !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'QuarantineDrainerConfig must be a non-null object');
    }
    if (!config.destination) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'destination is required in QuarantineDrainerConfig');
    }
    if (!config.quarantineBuffer) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'quarantineBuffer is required in QuarantineDrainerConfig');
    }

    this.destination = config.destination;
    this.quarantineBuffer = config.quarantineBuffer;
    this.nowFn = config.nowFn ?? Date.now;
  }

  public get inFlightRecord(): DrainedInFlightRecord | null {
    return this._inFlightRecord;
  }

  public get isDraining(): boolean {
    return this._isDraining;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  /**
   * Checks whether the specified operation is currently in DRAINER_IN_FLIGHT ownership.
   * Used to filter duplicate arrivals during in-flight processing.
   */
  public isInFlight(originPeerId: string, sequenceNumber: number): boolean {
    if (!this._inFlightRecord) {
      return false;
    }
    return (
      this._inFlightRecord.operation.originPeerId === originPeerId &&
      this._inFlightRecord.operation.sequenceNumber === sequenceNumber
    );
  }

  /**
   * Executes a sequential drain pass over the current drain batch.
   * Acquires in-flight ownership before destination submission, verifies receipt,
   * and clears in-flight record only upon confirmation.
   *
   * Cardinality is bounded: N_in-flight <= 1.
   *
   * @param batch Optional array of operations to drain. If omitted, drains until quarantineBuffer.drainBatch is empty.
   */
  public async drainBatch(batch?: readonly QuarantinedOperation[]): Promise<QuarantineDrainResult> {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineDrainer is disposed');
    }
    if (this._isDraining) {
      throw new SyncEngineError('INVALID_STATE', 'QuarantineDrainer is already draining a batch');
    }

    // Guard against in-flight record overwrite: if an unresolved in-flight operation exists
    // from a previous halted drain (e.g. CAPACITY_REJECTED or destination exception),
    // resolve or restore it before dequeuing any new operation.
    if (this._inFlightRecord !== null) {
      const inFlightOp = this._inFlightRecord.operation;
      if (
        this.destination.hasOperation &&
        this.destination.hasOperation(inFlightOp.originPeerId, inFlightOp.sequenceNumber)
      ) {
        // Destination already committed prior to interruption; release in-flight custody
        this._inFlightRecord = null;
      } else {
        // Atomically restore the in-flight operation to the front of the drain queue
        // so it is processed first before any newer operation.
        this.restoreInFlightToQueue();
      }
    }

    this._isDraining = true;

    let appliedCount = 0;
    let pendingCount = 0;
    let discardedCount = 0;
    let batchSize = 0;

    try {
      // If an explicit batch array was passed and drainBatch in buffer is empty, iterate over the batch array.
      // Otherwise, drain sequentially using quarantineBuffer.shiftDrain() to keep buffer accounting exact.
      const useBufferShift = this.quarantineBuffer.drainSize > 0 || !batch;

      if (useBufferShift) {
        batchSize = this.quarantineBuffer.drainSize;
        while (this.quarantineBuffer.drainSize > 0) {
          const op = this.quarantineBuffer.shiftDrain();
          if (!op) break;

          const stepResult = this.processSingleOperation(op);
          if (stepResult.status === 'ACCEPTED_REDUCED') appliedCount++;
          else if (stepResult.status === 'ACCEPTED_PENDING') pendingCount++;
          else if (stepResult.status === 'DISCARDED') discardedCount++;
          else if (stepResult.status === 'HALTED_CAPACITY') {
            return {
              batchSize,
              appliedCount,
              pendingCount,
              discardedCount,
              haltedOnCapacity: true,
            };
          } else if (stepResult.status === 'HALTED_ERROR') {
            return {
              batchSize,
              appliedCount,
              pendingCount,
              discardedCount,
              haltedOnCapacity: false,
              haltedOnError: stepResult.error,
            };
          }
        }
      } else {
        batchSize = batch.length;
        for (const op of batch) {
          const stepResult = this.processSingleOperation(op);
          if (stepResult.status === 'ACCEPTED_REDUCED') appliedCount++;
          else if (stepResult.status === 'ACCEPTED_PENDING') pendingCount++;
          else if (stepResult.status === 'DISCARDED') discardedCount++;
          else if (stepResult.status === 'HALTED_CAPACITY') {
            return {
              batchSize,
              appliedCount,
              pendingCount,
              discardedCount,
              haltedOnCapacity: true,
            };
          } else if (stepResult.status === 'HALTED_ERROR') {
            return {
              batchSize,
              appliedCount,
              pendingCount,
              discardedCount,
              haltedOnCapacity: false,
              haltedOnError: stepResult.error,
            };
          }
        }
      }

      return {
        batchSize,
        appliedCount,
        pendingCount,
        discardedCount,
        haltedOnCapacity: false,
      };
    } finally {
      this._isDraining = false;
    }
  }

  /**
   * Internal 6-step lifecycle execution for a single operation.
   */
  private processSingleOperation(
    op: QuarantinedOperation,
  ):
    | { status: 'ACCEPTED_REDUCED' }
    | { status: 'ACCEPTED_PENDING' }
    | { status: 'DISCARDED' }
    | { status: 'HALTED_CAPACITY' }
    | { status: 'HALTED_ERROR'; error: string } {
    // Step 2: In-Flight Acquisition
    if (this._inFlightRecord !== null) {
      throw new SyncEngineError(
        'INVALID_STATE',
        'Cannot acquire in-flight custody: an in-flight operation already exists',
      );
    }
    this._inFlightRecord = {
      operation: op,
      dequeuedAtMs: this.nowFn(),
      attemptCount: 1,
      destinationStatus: 'PENDING',
    };

    // Step 3 & Step 4: Destination Submission & Confirmation Receipt
    let receipt: DestinationAcceptanceReceipt;
    try {
      receipt = this.destination.admitDrainedOperation(op);
    } catch (err) {
      // Step 6: Post-dequeue exception handling.
      // inFlightRecord is preserved, drain halts immediately.
      const errorMsg = err instanceof Error ? err.message : String(err);
      return { status: 'HALTED_ERROR', error: errorMsg };
    }

    // Step 5: Evaluate confirmation receipt
    switch (receipt.status) {
      case 'ACCEPTED_REDUCED':
        // Destination confirmed reduction into operationLog. Vector advanced.
        this._inFlightRecord = null;
        return { status: 'ACCEPTED_REDUCED' };

      case 'ACCEPTED_PENDING':
        // Destination confirmed buffering into PendingDependencyQueue.
        this._inFlightRecord = null;
        return { status: 'ACCEPTED_PENDING' };

      case 'ALREADY_REPRESENTED':
      case 'ALREADY_OWNED_DUPLICATE':
        // Already incorporated into snapshot base or destination log. Safely retired.
        this._inFlightRecord = null;
        return { status: 'DISCARDED' };

      case 'TERMINALLY_REJECTED':
        // Fails validation, obsolete epoch, or malformed payload. Permanently discarded.
        this._inFlightRecord = null;
        return { status: 'DISCARDED' };

      case 'CAPACITY_REJECTED':
        // Destination queue full. Operation remains preserved in inFlightRecord.
        this._inFlightRecord.destinationStatus = 'CAPACITY_REJECTED';
        return { status: 'HALTED_CAPACITY' };
    }
  }

  /**
   * Resumes an interrupted drain pass, inspecting inFlightRecord against destination.
   * If destination already committed the operation before the interruption, cleans in-flight ownership.
   * Otherwise re-attempts admission.
   */
  public resumeDrain(): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineDrainer is disposed');
    }
    if (!this._inFlightRecord) {
      return;
    }

    const op = this._inFlightRecord.operation;
    if (this.destination.hasOperation && this.destination.hasOperation(op.originPeerId, op.sequenceNumber)) {
      // Destination accepted before interruption; release in-flight ownership
      this._inFlightRecord = null;
      return;
    }

    // Destination did not accept; re-attempt admission
    this._inFlightRecord.attemptCount++;
    try {
      const receipt = this.destination.admitDrainedOperation(op);
      if (receipt.status !== 'CAPACITY_REJECTED') {
        this._inFlightRecord = null;
      } else {
        this._inFlightRecord.destinationStatus = 'CAPACITY_REJECTED';
      }
    } catch {
      // Error during retry preserves inFlightRecord
    }
  }

  /**
   * Restores an unconfirmed in-flight operation to the head of the active/drain queue (via unshift).
   */
  public restoreInFlightToQueue(): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'QuarantineDrainer is disposed');
    }
    if (this._inFlightRecord !== null) {
      this.quarantineBuffer.unshiftDrain(this._inFlightRecord.operation);
      this._inFlightRecord = null;
    }
  }

  /**
   * Explicitly clears the in-flight record upon confirmed destination acceptance or terminal abort.
   */
  public clearInFlight(): void {
    this._inFlightRecord = null;
  }

  /**
   * Disposes the drainer idempotently. Terminal state.
   */
  public dispose(): void {
    if (this._isDisposed) {
      return;
    }
    // If an in-flight operation is still unresolved upon disposal,
    // restore it to the quarantine queue so it is not silently dropped.
    if (this._inFlightRecord !== null) {
      try {
        this.quarantineBuffer.unshiftDrain(this._inFlightRecord.operation);
      } catch {
        // Buffer may already be disposed
      }
      this._inFlightRecord = null;
    }
    this._isDisposed = true;
    this._isDraining = false;
  }
}
