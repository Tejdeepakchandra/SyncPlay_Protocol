/**
 * Compaction Horizon and Membership View Manager.
 *
 * Implements Phase 7.5 Milestone M3 specifications:
 * - Mandatory local peer inclusion in compaction frontier (GLB formula):
 *   V_epoch_safe[p] = min(V_local[p], min_{(r, gen) in P} D_r.replicationVector[p])
 * - Empty remote membership handling: V_epoch_safe = V_local
 * - Missing origin handling: origin absent from any peer vector treated as 0 (vector[p] ?? 0)
 * - Compaction never prunes operations beyond V_epoch_safe[p] or V_local[p]
 * - Membership view capture: V = (epoch, localVectorSnapshot, P = {(peerId, generation)})
 * - Current digest requirement: all connected peers must hold current authenticated STATE_DIGEST
 * - Synchronous pre-commit revalidation in a single run-to-completion step:
 *   (1) epoch matches, (2) connected peer set matches bit-exact, (3) local vector >= frontier,
 *   (4) peer digests current, (5) log not in recovery
 * - Zero partial compaction if any pre-commit check fails.
 */

import type { StateDigestPayload } from '@syncplay/protocol';
import type { IOrderedOperationLog } from '../log/types.js';

export interface PeerConnectionDescriptor {
  readonly peerId: string;
  readonly generation: number;
}

export interface CapturedMembershipView {
  readonly epoch: number;
  readonly localVectorSnapshot: Readonly<Record<string, number>>;
  readonly connectedPeers: ReadonlyArray<PeerConnectionDescriptor>;
}

export type CompactionHoldReason =
  | 'MISSING_PEER_DIGEST'
  | 'STALE_PEER_GENERATION'
  | 'STALE_PEER_EPOCH'
  | 'RECOVERY_ACTIVE';

export type CompactionAbortReason =
  | 'EPOCH_CHANGED'
  | 'MEMBERSHIP_CHANGED'
  | 'LOCAL_VECTOR_REGRESSED'
  | 'PEER_DIGEST_INVALID'
  | 'RECOVERY_ACTIVE';

export interface CompactionFrontierCandidate {
  readonly eligible: boolean;
  readonly frontier?: Readonly<Record<string, number>>;
  readonly reason?: CompactionHoldReason;
  readonly missingPeerId?: string;
  readonly capturedView?: CapturedMembershipView;
}

export interface CompactionExecutionResult {
  readonly committed: boolean;
  readonly prunedCount: number;
  readonly frontier?: Readonly<Record<string, number>>;
  readonly abortReason?: CompactionAbortReason;
  readonly details?: string;
}

export interface CompactionExecutionContext {
  getCurrentEpoch(): number;
  getConnectedPeers(): ReadonlyArray<PeerConnectionDescriptor>;
  getLocalReplicationVector(): Readonly<Record<string, number>>;
  getPeerDigests(): ReadonlyMap<string, StateDigestPayload>;
  isRecoveryActive(): boolean;
  compactLog(frontier: Record<string, number>): number;
}

/**
 * Captures a point-in-time membership view for compaction planning.
 * Deterministically sorts connected peers by peerId for bit-exact comparison.
 */
export function captureMembershipView(
  epoch: number,
  localVector: Readonly<Record<string, number>>,
  connectedPeers: ReadonlyArray<PeerConnectionDescriptor>,
): CapturedMembershipView {
  const sortedPeers = [...connectedPeers].sort((a, b) => a.peerId.localeCompare(b.peerId));
  return {
    epoch,
    localVectorSnapshot: Object.freeze({ ...localVector }),
    connectedPeers: Object.freeze(sortedPeers),
  };
}

/**
 * Calculates the membership-safe compaction frontier (V_epoch_safe) from a captured view.
 *
 * Implements non-negotiable invariants:
 * 1. The local engine's applied replication vector V_local is an explicit member of every calculation.
 * 2. If connectedPeers is empty (no connected remote peers), V_epoch_safe = V_local.
 * 3. Any origin missing from a peer's replication vector is treated as sequence 0.
 * 4. All connected peers must have a valid authenticated digest matching view epoch and generation.
 */
export function calculateMembershipSafeFrontier(
  view: CapturedMembershipView,
  peerDigests: ReadonlyMap<string, StateDigestPayload>,
): CompactionFrontierCandidate {
  // 1. Verify every connected peer has a valid, epoch-current, generation-current digest
  for (const peer of view.connectedPeers) {
    const digest = peerDigests.get(peer.peerId);
    if (!digest) {
      return {
        eligible: false,
        reason: 'MISSING_PEER_DIGEST',
        missingPeerId: peer.peerId,
        capturedView: view,
      };
    }
    if (digest.membershipEpoch !== view.epoch) {
      return {
        eligible: false,
        reason: 'STALE_PEER_EPOCH',
        missingPeerId: peer.peerId,
        capturedView: view,
      };
    }
    if (digest.connectionGeneration !== peer.generation) {
      return {
        eligible: false,
        reason: 'STALE_PEER_GENERATION',
        missingPeerId: peer.peerId,
        capturedView: view,
      };
    }
  }

  // 2. If remote membership is empty, V_epoch_safe = V_local
  if (view.connectedPeers.length === 0) {
    const safeFrontier: Record<string, number> = {};
    for (const [origin, seq] of Object.entries(view.localVectorSnapshot)) {
      safeFrontier[origin] = seq;
    }
    return {
      eligible: true,
      frontier: Object.freeze(safeFrontier),
      capturedView: view,
    };
  }

  // 3. Collect all distinct origins across local vector and all remote digests
  const origins = new Set<string>(Object.keys(view.localVectorSnapshot));
  for (const peer of view.connectedPeers) {
    const digest = peerDigests.get(peer.peerId)!;
    for (const origin of Object.keys(digest.replicationVector)) {
      origins.add(origin);
    }
  }

  // 4. Calculate Greatest Lower Bound (GLB) including local peer explicitly:
  // V_epoch_safe[p] = min(V_local[p], min_{(r, gen) in P} D_r.replicationVector[p])
  const safeFrontier: Record<string, number> = {};
  for (const origin of origins) {
    const localSeq = view.localVectorSnapshot[origin] ?? 0;
    let minSeq = localSeq;

    for (const peer of view.connectedPeers) {
      const digest = peerDigests.get(peer.peerId)!;
      const remoteSeq = digest.replicationVector[origin] ?? 0;
      minSeq = Math.min(minSeq, remoteSeq);
    }

    safeFrontier[origin] = minSeq;
  }

  return {
    eligible: true,
    frontier: Object.freeze(safeFrontier),
    capturedView: view,
  };
}

/**
 * Calculates local-pressure compaction candidate (V_local_cand).
 * Strictly bounded by the local applied replication vector: V_local_cand[p] <= V_local[p].
 */
export function calculateLocalPressureFrontier(
  localVector: Readonly<Record<string, number>>,
  operationLog: IOrderedOperationLog,
  targetPruneCount: number = 64,
): Record<string, number> {
  const operations = operationLog.getAllOperations();
  const candidateFrontier: Record<string, number> = {};

  const pruneLimit = Math.min(operations.length, Math.max(1, targetPruneCount));
  for (let i = 0; i < pruneLimit; i++) {
    const op = operations[i];
    if (op) {
      const origin = op.originPeerId;
      const currentCand = candidateFrontier[origin] ?? 0;
      if (op.originSequenceNumber > currentCand) {
        candidateFrontier[origin] = op.originSequenceNumber;
      }
    }
  }

  // Mandatory invariant: local candidate must NEVER exceed local applied replication vector
  for (const [origin, candSeq] of Object.entries(candidateFrontier)) {
    const localApplied = localVector[origin] ?? 0;
    candidateFrontier[origin] = Math.min(candSeq, localApplied);
  }

  return candidateFrontier;
}

/**
 * Executes compaction synchronously with atomic pre-commit revalidation.
 * If any revalidation check fails, aborts immediately with zero operations pruned.
 */
export function executeCompaction(
  candidate: CompactionFrontierCandidate,
  context: CompactionExecutionContext,
): CompactionExecutionResult {
  if (!candidate.eligible || !candidate.frontier || !candidate.capturedView) {
    return {
      committed: false,
      prunedCount: 0,
      abortReason: 'PEER_DIGEST_INVALID',
      details: 'Candidate not eligible for compaction',
    };
  }

  const view = candidate.capturedView;

  // Single synchronous run-to-completion critical section:
  // 1. Check recovery state
  if (context.isRecoveryActive()) {
    return {
      committed: false,
      prunedCount: 0,
      abortReason: 'RECOVERY_ACTIVE',
      details: 'Compaction aborted: recovery is active',
    };
  }

  // 2. Check epoch currency
  const currentEpoch = context.getCurrentEpoch();
  if (currentEpoch !== view.epoch) {
    return {
      committed: false,
      prunedCount: 0,
      abortReason: 'EPOCH_CHANGED',
      details: `Epoch changed from ${view.epoch} to ${currentEpoch}`,
    };
  }

  // 3. Check connected peer set matches bit-exact
  const currentPeers = [...context.getConnectedPeers()].sort((a, b) =>
    a.peerId.localeCompare(b.peerId),
  );
  const capturedPeers = [...view.connectedPeers].sort((a, b) =>
    a.peerId.localeCompare(b.peerId),
  );

  if (currentPeers.length !== capturedPeers.length) {
    return {
      committed: false,
      prunedCount: 0,
      abortReason: 'MEMBERSHIP_CHANGED',
      details: `Connected peer count changed from ${capturedPeers.length} to ${currentPeers.length}`,
    };
  }

  for (let i = 0; i < currentPeers.length; i++) {
    const curr = currentPeers[i]!;
    const cap = capturedPeers[i]!;
    if (curr.peerId !== cap.peerId || curr.generation !== cap.generation) {
      return {
        committed: false,
        prunedCount: 0,
        abortReason: 'MEMBERSHIP_CHANGED',
        details: `Connected peer mismatch at index ${i}: current (${curr.peerId}, gen ${curr.generation}) !== captured (${cap.peerId}, gen ${cap.generation})`,
      };
    }
  }

  // 4. Check local vector has not regressed below proposed frontier
  const currentLocalVector = context.getLocalReplicationVector();
  for (const [origin, frontierSeq] of Object.entries(candidate.frontier)) {
    const currentSeq = currentLocalVector[origin] ?? 0;
    if (frontierSeq > currentSeq) {
      return {
        committed: false,
        prunedCount: 0,
        abortReason: 'LOCAL_VECTOR_REGRESSED',
        details: `Local vector regressed for origin ${origin}: proposed frontier requires ${frontierSeq} but current local sequence is ${currentSeq}`,
      };
    }
  }

  // 5. Check peer digests remain valid for active generations
  const peerDigests = context.getPeerDigests();
  for (const peer of capturedPeers) {
    const digest = peerDigests.get(peer.peerId);
    if (
      !digest ||
      digest.membershipEpoch !== currentEpoch ||
      digest.connectionGeneration !== peer.generation
    ) {
      return {
        committed: false,
        prunedCount: 0,
        abortReason: 'PEER_DIGEST_INVALID',
        details: `Digest for peer ${peer.peerId} is invalid or stale at commit boundary`,
      };
    }
  }

  // All pre-commit checks PASSED. Synchronously commit log compaction.
  const prunedCount = context.compactLog(candidate.frontier);

  return {
    committed: true,
    prunedCount,
    frontier: candidate.frontier,
  };
}

/**
 * Manages compaction state, peer digest registry, and safe horizon calculations.
 */
export class CompactionManager {
  private readonly peerDigests = new Map<string, StateDigestPayload>();

  /**
   * Records or updates an authenticated peer state digest.
   */
  public recordPeerDigest(digest: StateDigestPayload): void {
    this.peerDigests.set(digest.senderPeerId, digest);
  }

  /**
   * Removes peer digest when peer disconnects.
   */
  public removePeerDigest(peerId: string): void {
    this.peerDigests.delete(peerId);
  }

  /**
   * Clears all stored peer digests (e.g. on epoch transition or reset).
   */
  public clearDigests(): void {
    this.peerDigests.clear();
  }

  /**
   * Returns a read-only copy of all currently held peer digests.
   */
  public getPeerDigests(): ReadonlyMap<string, StateDigestPayload> {
    return new Map(this.peerDigests);
  }

  /**
   * Captures a membership view.
   */
  public captureView(
    epoch: number,
    localVector: Readonly<Record<string, number>>,
    connectedPeers: ReadonlyArray<PeerConnectionDescriptor>,
  ): CapturedMembershipView {
    return captureMembershipView(epoch, localVector, connectedPeers);
  }

  /**
   * Calculates membership safe compaction frontier from a captured view.
   */
  public calculateSafeFrontier(view: CapturedMembershipView): CompactionFrontierCandidate {
    return calculateMembershipSafeFrontier(view, this.peerDigests);
  }

  /**
   * Calculates local-pressure compaction candidate frontier.
   */
  public calculatePressureFrontier(
    localVector: Readonly<Record<string, number>>,
    operationLog: IOrderedOperationLog,
    targetPruneCount?: number,
  ): Record<string, number> {
    return calculateLocalPressureFrontier(localVector, operationLog, targetPruneCount);
  }

  /**
   * Executes atomic pre-commit revalidation and compaction.
   */
  public execute(
    candidate: CompactionFrontierCandidate,
    context: CompactionExecutionContext,
  ): CompactionExecutionResult {
    return executeCompaction(candidate, context);
  }
}
