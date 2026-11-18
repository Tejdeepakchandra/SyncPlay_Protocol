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
import type { Clock, EngineDecision } from '../types.js';
import type { MissingSequenceRange } from '../log/pending-dependency-queue.js';
import type { AuthenticatedSupplierContext, AuthenticatedRequesterContext } from '../engine/leaderless-sync-engine.js';

export type { MissingSequenceRange };

/**
 * High-level classification of a state digest comparison.
 *
 * INVARIANT:
 * The aggregate classification MUST NOT be 'IDENTICAL' whenever hasUnverifiedHistory is true,
 * or when any origin has missing ranges, outside-horizon uncertainty, or unresolved conditions.
 * When replication vectors and tail fingerprints match but intermediate historical equality
 * cannot be proven (e.g. k > 1 with single tail fingerprints), 'FRONTIER_AGREEMENT_ONLY' is returned.
 */
export type DigestComparisonClassification =
  | 'IDENTICAL'
  | 'FRONTIER_AGREEMENT_ONLY'
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
  | 'FRONTIER_IDENTICAL'
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
  /**
   * Local snapshot frontier: highest contiguous sequence for this origin already incorporated
   * into the local compacted base snapshot. Sequences <= localSnapshotFrontier have been compacted
   * out of the active operation log.
   */
  readonly localSnapshotFrontier: number;
  /**
   * Remote snapshot frontier: highest contiguous sequence for this origin already incorporated
   * into the remote compacted base snapshot. Sequences <= remoteSnapshotFrontier have been compacted
   * out of the remote active operation log.
   */
  readonly remoteSnapshotFrontier: number;
  readonly localFingerprint?: string;
  readonly remoteFingerprint?: string;
  readonly missingRange?: MissingSequenceRange;
  readonly reason?: string;
  /**
   * Property A: Frontier fingerprint agreement.
   * True if the operation at the current replication frontier has matching fingerprints (or both at sequence 0).
   */
  readonly frontierAgreement: boolean;
  /**
   * Property B: Historical equality verification.
   * True ONLY if all preceding operation history has been independently verified to match.
   * For single tail fingerprints without cumulative hashes, intermediate history (1..k-1) is unverified.
   * Therefore, historyVerified is true ONLY when k = 0, or k = 1 with matching fingerprint.
   */
  readonly historyVerified: boolean;
}

/**
 * Result of comparing a local StateDigestPayload against a remote StateDigestPayload.
 * Conforms strictly to PHASE7_4_INTERFACE_SPECIFICATION.md §2.1.
 *
 * SECURITY & TRUST BOUNDARY:
 * A remote StateDigestPayload's fields (senderPeerId, connectionGeneration, vectors) are untrusted
 * remote claims. Digest comparison does NOT authorize the remote peer as a repair supplier, does NOT
 * authenticate its connection generation, and does NOT permit operation ingestion. Authoritative
 * supplier authorization and repair batch ingestion strictly require Phase 7.3-R5-H capability-gated
 * registration via AuthenticatedSupplierContext.
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

  /** Origins where historical equality cannot be proven because checkpoints/ops were compacted or missing. */
  readonly unprovableOrigins: ReadonlyArray<string>;

  /**
   * Origins whose complete historical equality has not been established.
   * INVARIANT: Whenever originDetail.historyVerified === false, the origin MUST appear in unverifiedHistoryOrigins.
   * Origins with confirmed forks (CONFLICT_FORK) additionally appear in conflictingOrigins.
   */
  readonly unverifiedHistoryOrigins: ReadonlyArray<string>;

  /**
   * Completeness indicator: true if one or more origins had unverified historical equality
   * (i.e. unverifiedHistoryOrigins.length > 0).
   * INVARIANT: classification CAN NEVER be 'IDENTICAL' when hasUnverifiedHistory is true.
   * When true, global dominance classifications (LOCAL_DOMINATES / REMOTE_DOMINATES)
   * do NOT imply complete historical equality across origins.
   */
  readonly hasUnverifiedHistory: boolean;

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
 * In-flight unicast repair request record tracked by the requester-side pipeline.
 */
export interface InFlightRepairRequest {
  readonly requestId: string;
  readonly sessionId: string;
  readonly supplierPeerId: string;
  readonly supplierConnectionGeneration: number;
  readonly originPeerId: string;
  readonly fromSequence: number;
  readonly toSequence: number;
  readonly membershipEpoch: number;
  readonly sentAtMonotonic: number;
  readonly timerId: unknown;
  retryCount: number;
  readonly attemptedSuppliers: Set<string>;
}

/**
 * Peer replication knowledge tracked from incoming StateDigestPayloads.
 */
export interface PeerReplicationKnowledge {
  readonly peerId: string;
  readonly generation: number;
  readonly epoch: number;
  readonly replicationVector: Record<string, number>;
  readonly snapshotFrontier: Record<string, number>;
  readonly lastObservedAt: number;
}

/**
 * Diagnostic and telemetry status of the repair request pipeline.
 */
export interface RepairPipelineStatus {
  readonly activeRequestsCount: number;
  readonly pendingGapsCount: number;
  readonly totalRepairsCompleted: number;
  readonly totalRepairsFailed: number;
  readonly degraded: boolean;
  readonly degradedReason?: string;
}

/**
 * Configuration options for the repair request pipeline.
 */
export interface RepairPipelineOptions {
  /** Unicast repair request timeout in milliseconds before retry/failover (default: 1500ms). */
  readonly repairRequestTimeoutMs?: number;

  /** Maximum retries per missing range before marking history exhausted (default: 3). */
  readonly maxRetries?: number;

  /** Base interval for exponential backoff in milliseconds (default: 1000ms). */
  readonly backoffBaseMs?: number;

  /** Maximum concurrent in-flight requests per supplier peer (default: 2). */
  readonly maxConcurrentRequestsPerPeer?: number;

  /** Maximum concurrent in-flight requests room-wide (default: 16). */
  readonly maxConcurrentRequestsTotal?: number;

  /** Optional RTT provider returning smoothed RTT in milliseconds for supplier ranking. */
  readonly rttProvider?: (peerId: string) => number | undefined;
}

/**
 * Configuration options for supplier range servicing (M4).
 */
export interface RepairSupplierOptions {
  /** Maximum concurrent repair requests serviced room-wide (default: 16). */
  readonly maxConcurrentRequests?: number;

  /** Maximum concurrent repair requests serviced per requesting peer (default: 4). */
  readonly maxConcurrentRequestsPerPeer?: number;

  /** Rate limiter: maximum repair requests accepted per second per peer (default: 10). */
  readonly rateLimitMaxRequestsPerSecond?: number;

  /** Rate limiter: maximum burst of repair requests allowed per peer (default: 20). */
  readonly rateLimitBurst?: number;

  /** Bounded capacity for recent response cache to handle duplicate request replays (default: 64). */
  readonly responseCacheCapacity?: number;

  /** Maximum operations per batch (default: 32). */
  readonly maxOperationsPerBatch?: number;

  /** Maximum application payload bytes per batch frame (default: 65536). */
  readonly maxPayloadBytes?: number;

  /** Optional provider for negotiated DataChannel / SCTP max message size per peer. */
  readonly negotiatedSctpMaxMessageSizeProvider?: (peerId: string) => number | undefined;
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

  /** Serves an inbound REPAIR_REQUEST from local log history (M4). Requires genuine AuthenticatedRequesterContext. */
  serveRepairRequest(
    request: RepairRequestPayload,
    context?: AuthenticatedRequesterContext,
  ): OperationBatchPayload | RepairRejectedPayload;

  /** Processes an inbound OPERATION_BATCH fulfilling an active repair request (M3). Requires genuine AuthenticatedSupplierContext. */
  handleRepairBatch(
    batch: OperationBatchPayload,
    context: AuthenticatedSupplierContext,
  ): EngineDecision[];

  /** Processes an inbound REPAIR_REJECTED response handling failover/backoff (M3). Requires genuine AuthenticatedSupplierContext. */
  handleRepairRejected(
    rejection: RepairRejectedPayload,
    context: AuthenticatedSupplierContext,
  ): void;

  /** Schedules targeted repair requests for all currently detected sequence gaps (M3). */
  reconcileGaps(): void;

  /** Cancels active repair requests to a departing peer. */
  onPeerDisconnected(peerId: string): void;

  /** Invalidates and fails over in-flight requests bound to an obsolete supplier connection generation. */
  onPeerGenerationChanged?(peerId: string, newGeneration: number): void;

  /** Resets state upon membership epoch transition. */
  onEpochTransition(newEpoch: number): void;

  /** Current diagnostic status. */
  getStatus(): AntiEntropyStatus;

  /** Disposes all active timers and background routines. */
  dispose(): void;
}
