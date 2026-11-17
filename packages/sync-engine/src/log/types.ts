/**
 * OrderedOperationLog types and contracts.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_INTERFACE_CONTRACTS.md §2.2
 * - PHASE7_2_LLD.md §3
 */

import type { PlaybackState } from '../types.js';

export interface StoredOperation {
  readonly logicalEventId: string;
  readonly originPeerId: string;
  readonly senderPeerId: string;
  readonly membershipEpoch: number;
  readonly messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE';
  readonly logicalTimestamp: number;
  readonly originSequenceNumber: number;
  readonly causalContext: Record<string, number>;
  readonly appliedAtMonotonic: number;
  readonly payload: unknown;
}

export type InsertOperationCode =
  | 'INSERTED_HEAD'
  | 'INSERTED_REORDER'
  | 'IGNORED_DUPLICATE'
  | 'IGNORED_ALREADY_COMPACTED'
  | 'BUFFERED_PENDING_DEPENDENCY'
  | 'REJECTED_OUTSIDE_HORIZON'
  | 'REJECTED_INVALID_ORIGIN'
  | 'REJECTED_SEQUENCE_CONFLICT'
  | 'REJECTED_OBSOLETE_EPOCH';

export interface InsertOperationResult {
  readonly inserted: boolean;
  readonly isHead: boolean;
  readonly index: number;
  readonly code: InsertOperationCode;
}

export interface IOrderedOperationLog {
  /** Count of retained operations in the log (<= 256). */
  readonly size: number;

  /** Maximum capacity horizon (K = 256). */
  readonly capacity: number;

  /** Checks if operation exists by logicalEventId in log. */
  has(logicalEventId: string): boolean;

  /** Retrieves operation by logicalEventId. */
  get(logicalEventId: string): StoredOperation | undefined;

  /** Retrieves operation by origin and sequence number. */
  getOperationAt(originPeerId: string, sequenceNumber: number): StoredOperation | undefined;

  /** Inserts operation in total-order position. */
  insert(operation: StoredOperation): InsertOperationResult;

  /** Returns all retained operations in strict total order. */
  getAllOperations(): ReadonlyArray<StoredOperation>;

  /** Returns slice of operations originating from originPeerId in range [fromSeq, toSeq]. */
  getRange(originPeerId: string, fromSeq: number, toSeq: number): ReadonlyArray<StoredOperation>;

  /** Retrieves exact Lamport timestamp for a compacted sequence. */
  getCompactedCheckpoint(originPeerId: string, sequenceNumber: number): number | undefined;

  /** Prunes operations older than stable frontier and preserves checkpoints. */
  compactAtFrontier(stableFrontier: Record<string, number>): number;

  /** Resets log and initializes new base snapshot. */
  reset(baseSnapshot: PlaybackState): void;

  /** Clears memory on disposal. */
  dispose(): void;
}
