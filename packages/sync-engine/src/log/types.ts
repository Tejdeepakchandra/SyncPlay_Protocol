/**
 * OrderedOperationLog types and contracts.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_INTERFACE_CONTRACTS.md §2.2
 * - PHASE7_2_LLD.md §3
 */

import type { CompactedCheckpointItem } from '@syncplay/protocol';
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

export interface CompactedCheckpointRecord {
  readonly logicalTimestamp: number;
  readonly logicalEventId: string;
  readonly fingerprint?: string;
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
  | 'REJECTED_IDENTITY_CONFLICT'
  | 'REJECTED_CAPACITY'
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

  /** Retrieves the lowest origin sequence number currently retained in the active log for an origin. */
  getOldestRetainedSequence?(originPeerId: string): number | undefined;

  /** Retrieves exact Lamport timestamp for a compacted sequence. */
  getCompactedCheckpoint(originPeerId: string, sequenceNumber: number): number | undefined;

  /** Retrieves compacted checkpoint record including identity metadata. */
  getCompactedCheckpointRecord?(
    originPeerId: string,
    sequenceNumber: number,
  ): CompactedCheckpointRecord | undefined;

  /** Retrieves all retained compacted checkpoints across origins. */
  getAllCompactedCheckpoints?(): ReadonlyArray<CompactedCheckpointItem>;

  /**
   * Resolves the logical timestamp for each origin at the sequence specified by the frontier.
   * Returns exact Lamport timestamp, or null if historical sequence is evicted or unavailable.
   */
  getOriginFrontierTimestamps?(frontier: Record<string, number>): Record<string, number | null>;

  /** Retrieves tail fingerprint for an origin at an optional target sequence. */
  getTailFingerprint?(originPeerId: string, atSequence?: number): string | undefined;

  /** Gets the replication frontier of highest sequences per origin. */
  getReplicationFrontier?(): Record<string, number>;

  /** Sets compacted checkpoint with timestamp, optional logical event id, and optional fingerprint. */
  setCompactedCheckpoint?(
    originPeerId: string,
    sequenceNumber: number,
    logicalTimestamp: number,
    logicalEventId?: string,
    fingerprint?: string,
  ): void;

  /** Gets base snapshot frontier representation. */
  getBaseSnapshotFrontier?(): Readonly<Record<string, number>>;

  /** Sets reducer callback for folding compacted operations forward into base snapshot. */
  setReducer?(reducer: (state: PlaybackState, op: StoredOperation) => PlaybackState): void;

  /** Prunes operations older than stable frontier and preserves checkpoints. */
  compactAtFrontier(stableFrontier: Record<string, number>): number;

  /** Resets log and initializes new base snapshot. */
  reset(baseSnapshot: PlaybackState, frontier?: Record<string, number>): void;

  /** Clears memory on disposal. */
  dispose(): void;
}
