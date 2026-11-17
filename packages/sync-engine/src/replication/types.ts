/**
 * Phase 7.4 State Replication and Anti-Entropy Subsystem Types.
 *
 * Implements Phase 7.4 specifications:
 * - PHASE7_4_ARCHITECTURE.md §3 & §6
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §2
 */

import type {
  StateDigestPayload,
  RepairRequestPayload,
  OperationBatchPayload,
  RepairRejectedPayload,
} from '@syncplay/protocol';
import type { Clock } from '../types.js';
import type { MissingSequenceRange } from '../log/pending-dependency-queue.js';

export type { MissingSequenceRange };

/**
 * High-level classification of a state digest comparison.
 */
export type DigestComparisonClassification =
  | 'IDENTICAL'
  | 'LOCAL_DOMINATES'
  | 'REMOTE_DOMINATES'
  | 'CONCURRENT_INCOMPARABLE'
  | 'DIVERGENT_FORK'
  | 'REQUIRES_SNAPSHOT_RECOVERY'
  | 'INCONCLUSIVE';

/**
 * Detailed per-origin replication status.
 */
export type OriginComparisonStatus =
  | 'IDENTICAL'
  | 'LOCAL_AHEAD'
  | 'REMOTE_AHEAD'
  | 'LOCAL_ONLY'
  | 'REMOTE_ONLY'
  | 'CONFLICT_FORK'
  | 'OUTSIDE_LOCAL_HORIZON'
  | 'OUTSIDE_REMOTE_HORIZON'
  | 'HISTORICAL_EQUALITY_UNPROVABLE';

/**
 * Per-origin granular comparison details.
 */
export interface OriginComparisonDetail {
  readonly originPeerId: string;
  readonly status: OriginComparisonStatus;
  readonly localSequence: number;
  readonly remoteSequence: number;
  readonly localSnapshotFrontier: number;
  readonly remoteSnapshotFrontier: number;
  readonly localFingerprint?: string;
  readonly remoteFingerprint?: string;
  readonly missingRange?: MissingSequenceRange;
  readonly reason?: string;
}

/**
 * Result of comparing a local StateDigestPayload against a remote StateDigestPayload.
 * Conforms strictly to PHASE7_4_INTERFACE_SPECIFICATION.md §2.1.
 */
export interface DigestComparisonResult {
  /** The remote peer transmitting this digest. */
  readonly remotePeerId: string;

  /** Missing sequence ranges that remote peer holds but local peer needs. */
  readonly missingFromLocal: ReadonlyArray<MissingSequenceRange>;

  /** Missing sequence ranges that local peer holds but remote peer needs. */
  readonly missingFromRemote: ReadonlyArray<MissingSequenceRange>;

  /** True if a sequence collision or divergent history fork is detected. */
  readonly forkDetected: boolean;

  /** Origins where fingerprints diverged for the same sequence number. */
  readonly conflictingOrigins: ReadonlyArray<string>;

  /** True if remote needs sequences that local peer has already compacted into base snapshot. */
  readonly outsideLocalHorizon: boolean;

  /** High-level classification of the vector relationship. */
  readonly classification: DigestComparisonClassification;

  /** True if local needs sequences that remote has already compacted into its base snapshot. */
  readonly outsideRemoteHorizon: boolean;

  /** True if either side requires snapshot catch-up because operations are compacted. */
  readonly requiresSnapshotRecovery: boolean;

  /** Origins where historical equality cannot be proven because checkpoints/ops were compacted. */
  readonly unprovableOrigins: ReadonlyArray<string>;

  /** Granular per-origin comparison details sorted lexicographically by originPeerId. */
  readonly originDetails: ReadonlyArray<OriginComparisonDetail>;

  /** Diagnostic notes or failure reasons, if any. */
  readonly diagnosticReasons?: ReadonlyArray<string>;
}

/**
 * Diagnostic and telemetry status of the anti-entropy subsystem.
 */
export interface AntiEntropyStatus {
  readonly activeRequestsCount: number;
  readonly pendingGapsCount: number;
  readonly lastDigestSentAt: number;
  readonly totalRepairsCompleted: number;
  readonly totalRepairsFailed: number;
  readonly degraded: boolean;
}

/**
 * Timer and monotonic scheduler interface supporting deterministic testing.
 */
export interface SchedulerClock extends Clock {
  /** Schedules a timer callback after delayMs. */
  setTimeout(callback: () => void, delayMs: number): unknown;

  /** Cancels a previously scheduled timer. */
  clearTimeout(id: unknown): void;
}

/**
 * Configuration options for the anti-entropy digest scheduler.
 */
export interface AntiEntropySchedulerOptions {
  /** Base interval for periodic digest broadcast in milliseconds (default: 5000ms). */
  readonly periodicIntervalMs?: number;

  /** Maximum jitter in milliseconds added or subtracted from interval (default: 1000ms). */
  readonly jitterMs?: number;

  /** Minimum interval in milliseconds between digest transmissions to a peer (default: 1000ms). */
  readonly rateLimitMinIntervalMs?: number;

  /** Maximum burst of digests allowed before throttling (default: 2). */
  readonly rateLimitBurst?: number;

  /** Custom pseudo-random number generator returning [0, 1) for jitter calculation. */
  readonly rng?: () => number;
}

/**
 * Outbound transport callback hooks for the anti-entropy manager.
 */
export interface AntiEntropyCallbacks {
  /** Emitted when a local digest should be broadcast to peers. */
  readonly onBroadcastDigest: (digest: StateDigestPayload) => void;

  /** Emitted when a unicast repair request should be sent to a specific peer. */
  readonly onSendRepairRequest?: (targetPeerId: string, request: RepairRequestPayload) => void;

  /** Emitted when a repair response should be transmitted back to the requester. */
  readonly onSendRepairResponse?: (
    targetPeerId: string,
    response: OperationBatchPayload | RepairRejectedPayload,
  ) => void;

  /** Emitted when a required sequence is unavailable in all peer logs (Phase 7.5 Handoff). */
  readonly onHistoryExhausted?: (details: {
    originPeerId: string;
    missingSequence: number;
    highestRetainedSequence: number;
  }) => void;
}

/**
 * Public interface for the anti-entropy manager coordinating state replication.
 * Conforms to PHASE7_4_INTERFACE_SPECIFICATION.md §2.1.
 */
export interface IAntiEntropyManager {
  /** Generates the local StateDigestPayload for broadcasting. */
  createLocalDigest(): StateDigestPayload;

  /** Evaluates an incoming remote digest against local replication knowledge. */
  processRemoteDigest(digest: StateDigestPayload): DigestComparisonResult;

  /** Serves an inbound REPAIR_REQUEST from local log history (M4). */
  serveRepairRequest(request: RepairRequestPayload): OperationBatchPayload | RepairRejectedPayload;

  /** Schedules targeted repair requests for all currently detected sequence gaps (M3). */
  reconcileGaps(): void;

  /** Cancels active repair requests to a departing peer. */
  onPeerDisconnected(peerId: string): void;

  /** Resets state upon membership epoch transition. */
  onEpochTransition(newEpoch: number): void;

  /** Current diagnostic status. */
  getStatus(): AntiEntropyStatus;

  /** Disposes all active timers and background routines. */
  dispose(): void;
}
