/**
 * @module @syncplay/sync-engine/recovery
 *
 * Snapshot Installer for Phase 7.5 M4.3b.
 *
 * Implements the four-stage validation, staging, and atomic commit architecture:
 * - STAGE 1 (Dry-Run Validation): Pure out-of-band JSON, schema, RFC 8785 digest, epoch, and vector dominance checks.
 *   Modifies ZERO engine state.
 * - STAGE 2 (Staging Construction): Pre-constructs candidate LogicalClockManager, OrderedOperationLog,
 *   ingests compacted checkpoints out-of-band, instantiates candidate PendingDependencyQueue,
 *   and assembles sealed candidate EngineCoreState.
 *   All potentially throwing work executes here; modifies ZERO engine state.
 * - STAGE 3 (Irreversible Synchronous Atomic Commit Point):
 *   Executes single synchronous pointer replacement: engine.replaceCoreStateAtomic(candidateCore).
 *   Cannot throw. Drops creator aliases post-handoff.
 * - STAGE 4: Handed off to M4.3c (Reconciliation Drain & Post-Install Notification).
 */

import type { StateSnapshotPayloadV2 } from '@syncplay/protocol';
import type { PlaybackState } from '../types.js';
import { SyncEngineError } from '../errors.js';
import { validateSnapshotPayloadV2, computeSnapshotDigestV2 } from '../snapshot.js';
import { LogicalClockManager } from '../clock/logical-clock-manager.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import type { StoredOperation } from '../log/types.js';
import type { ResolvedSyncEngineConfig } from '../config.js';
import {
  type EngineCoreState,
  createEngineCoreState,
} from './engine-core-state.js';

/**
 * Snapshot candidate eligibility classifications per approved M4.3-R2 / M4.3-R4 specifications.
 */
export type SnapshotEligibility =
  | 'IDENTICAL'
  | 'FRONTIER_AGREEMENT_ONLY'
  | 'STRICTLY_DOMINATES'
  | 'INCOMPARABLE'
  | 'DOMINATED'
  | 'FUTURE'
  | 'REPAIRABLE_WITHIN_HORIZON';

export interface SnapshotEligibilityResult {
  readonly eligibility: SnapshotEligibility;
  readonly eligible: boolean;
  readonly reason: string;
}

export interface EvaluateSnapshotEligibilityParams {
  readonly candidateSnapshot: StateSnapshotPayloadV2;
  readonly currentReplicationVector: Record<string, number>;
  readonly currentEpoch: number;
  readonly currentFrontierTimestamps?: Record<string, number>;
  readonly logCapacity?: number;
  readonly checkRepairableHorizon?: boolean;
}

/**
 * Evaluates snapshot candidate eligibility against current engine replication frontier and epoch.
 * Strictly adheres to Phase 7.4 comparator semantics and prohibits arbitrary tie-breaking.
 */
export function evaluateSnapshotEligibility(
  params: EvaluateSnapshotEligibilityParams,
): SnapshotEligibilityResult {
  const { candidateSnapshot, currentReplicationVector, currentEpoch } = params;

  // 1. Membership Epoch Evaluation
  if (candidateSnapshot.membershipEpoch > currentEpoch) {
    return {
      eligibility: 'FUTURE',
      eligible: false,
      reason: `Snapshot membership epoch (${candidateSnapshot.membershipEpoch}) is in the future relative to engine epoch (${currentEpoch})`,
    };
  }

  if (candidateSnapshot.membershipEpoch < currentEpoch) {
    return {
      eligibility: 'DOMINATED',
      eligible: false,
      reason: `Snapshot membership epoch (${candidateSnapshot.membershipEpoch}) is older than current engine epoch (${currentEpoch})`,
    };
  }

  // 2. Vector Dominance Comparison (Current Epoch)
  const remoteFrontier = candidateSnapshot.snapshotFrontier;
  const localVector = currentReplicationVector;
  const allOrigins = new Set([...Object.keys(remoteFrontier), ...Object.keys(localVector)]);

  let remoteAheadCount = 0;
  let localAheadCount = 0;
  let equalCount = 0;
  let maxGap = 0;

  for (const origin of allOrigins) {
    const rSeq = remoteFrontier[origin] ?? 0;
    const lSeq = localVector[origin] ?? 0;

    if (rSeq > lSeq) {
      remoteAheadCount++;
      const gap = rSeq - lSeq;
      if (gap > maxGap) {
        maxGap = gap;
      }
    } else if (lSeq > rSeq) {
      localAheadCount++;
    } else {
      equalCount++;
    }
  }

  // Case 4: Concurrent / Incomparable histories (Arbitrary tie-breaking strictly prohibited)
  if (remoteAheadCount > 0 && localAheadCount > 0) {
    return {
      eligibility: 'INCOMPARABLE',
      eligible: false,
      reason: 'Candidate snapshot frontier is concurrent and incomparable with local vector; arbitrary tie-breaking is prohibited',
    };
  }

  // Case 5: Local strictly dominates or remote is completely behind
  if (remoteAheadCount === 0 && localAheadCount > 0) {
    return {
      eligibility: 'DOMINATED',
      eligible: false,
      reason: 'Candidate snapshot frontier is dominated by local replication vector',
    };
  }

  // Case 1 & Case 2: Frontiers are numerically equal on all origins
  if (remoteAheadCount === 0 && localAheadCount === 0) {
    const maxSeq = Math.max(0, ...Object.values(remoteFrontier));
    // For seq <= 1 (base anchor), agreement proves identical state
    if (maxSeq <= 1) {
      return {
        eligibility: 'IDENTICAL',
        eligible: false,
        reason: 'Candidate snapshot has identical frontier to local state (k <= 1); installation is unnecessary',
      };
    }

    // For seq > 1: check if full checkpoint history is verified against local timestamps or checkpoints
    const timestampsMatch =
      params.currentFrontierTimestamps &&
      Object.entries(params.currentFrontierTimestamps).every(
        ([orig, ts]) => candidateSnapshot.originFrontierTimestamps[orig] === ts,
      );

    if (timestampsMatch && candidateSnapshot.compactedCheckpoints.length > 0) {
      return {
        eligibility: 'IDENTICAL',
        eligible: false,
        reason: 'Candidate snapshot has identical frontier and verified history; installation is unnecessary',
      };
    }

    return {
      eligibility: 'FRONTIER_AGREEMENT_ONLY',
      eligible: false,
      reason: 'Candidate snapshot matches frontier at seq > 1 but intermediate history is unverified; cannot overwrite local state without proof',
    };
  }

  // Case 7: Missing range is repairable within retained active log horizon via Anti-Entropy
  if (
    params.checkRepairableHorizon &&
    maxGap > 0 &&
    maxGap <= (params.logCapacity ?? 256)
  ) {
    return {
      eligibility: 'REPAIRABLE_WITHIN_HORIZON',
      eligible: false,
      reason: `Missing gap (${maxGap}) is within retained log horizon (${params.logCapacity ?? 256}); repairable via anti-entropy batch rather than snapshot recovery`,
    };
  }

  // Case 3: Remote strictly dominates local on at least one origin, and is >= on all origins
  return {
    eligibility: 'STRICTLY_DOMINATES',
    eligible: true,
    reason: 'Candidate snapshot strictly dominates local replication vector across all origins',
  };
}

/**
 * Result of Stage 1 out-of-band dry-run validation.
 */
export interface SnapshotValidationResult {
  readonly valid: boolean;
  readonly errorReason?: string;
  readonly parsedPayload?: StateSnapshotPayloadV2;
  readonly computedDigest?: string;
  readonly eligibility?: SnapshotEligibilityResult;
}

/**
 * Fully constructed, ready-to-swap EngineCoreState container from Stage 2.
 */
export interface StagedSnapshotInstallation {
  readonly payload: StateSnapshotPayloadV2;
  readonly canonicalDigest: string;
  readonly targetEpoch: number;
  readonly targetFrontier: Record<string, number>;
  readonly candidateCore: EngineCoreState;
}

/**
 * Final execution report returned after Stage 3 commit.
 */
export interface SnapshotInstallationReport {
  readonly success: boolean;
  readonly installedEpoch: number;
  readonly installedFrontier: Record<string, number>;
  readonly installedMaxLogicalTimestamp: number;
  readonly commitDurationMs: number;
}

export interface SnapshotInstallerValidateOptions {
  readonly expectedSessionId?: string;
  readonly checkRepairableHorizon?: boolean;
  readonly logCapacity?: number;
}

/**
 * Interface for SnapshotInstaller.
 */
export interface ISnapshotInstaller {
  validate(
    rawPayload: string | StateSnapshotPayloadV2,
    currentEpoch: number,
    currentReplicationVector: Record<string, number>,
    currentFrontierTimestamps?: Record<string, number>,
    options?: SnapshotInstallerValidateOptions,
  ): Promise<SnapshotValidationResult>;

  stage(
    validationResult: SnapshotValidationResult,
    engineConfig: ResolvedSyncEngineConfig,
    now?: number,
    reducerFn?: (state: PlaybackState, op: StoredOperation) => PlaybackState,
  ): StagedSnapshotInstallation;

  commitAtomic(
    engine: { replaceCoreStateAtomic: (candidate: EngineCoreState) => void; isDisposed?: boolean },
    staged: StagedSnapshotInstallation,
  ): SnapshotInstallationReport;
}

/**
 * Four-stage atomic snapshot installer for Phase 7.5 M4.3b.
 */
export class SnapshotInstaller implements ISnapshotInstaller {
  /**
   * STAGE 1: Pure out-of-band validation.
   * Performs JSON parsing, schema checks, RFC 8785 digest verification,
   * epoch fencing, and strict frontier dominance checks.
   * Modifies ZERO engine state.
   */
  public async validate(
    rawPayload: string | StateSnapshotPayloadV2,
    currentEpoch: number,
    currentReplicationVector: Record<string, number>,
    currentFrontierTimestamps?: Record<string, number>,
    options?: SnapshotInstallerValidateOptions,
  ): Promise<SnapshotValidationResult> {
    let payload: StateSnapshotPayloadV2;

    // 1. JSON parsing if string
    if (typeof rawPayload === 'string') {
      try {
        payload = JSON.parse(rawPayload) as StateSnapshotPayloadV2;
      } catch (err) {
        return {
          valid: false,
          errorReason: `JSON parsing failed: ${err instanceof Error ? err.message : String(err)}`,
        };
      }
    } else {
      payload = rawPayload;
    }

    if (!payload || typeof payload !== 'object') {
      return {
        valid: false,
        errorReason: 'Snapshot payload must be a non-null object',
      };
    }

    // 2. Structural schema validation
    const expectedSessionId = options?.expectedSessionId ?? payload.sessionId;
    try {
      validateSnapshotPayloadV2(payload, expectedSessionId);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const isDigest = msg.toLowerCase().includes('digest mismatch');
      return {
        valid: false,
        errorReason: isDigest ? `Digest mismatch: ${msg}` : `Snapshot schema validation failed: ${msg}`,
        parsedPayload: payload,
      };
    }

    // 3. RFC 8785 JCS Canonical SHA-256 digest verification
    let computedDigest: string;
    try {
      computedDigest = computeSnapshotDigestV2(payload);
    } catch (err) {
      return {
        valid: false,
        errorReason: `Digest computation failed: ${err instanceof Error ? err.message : String(err)}`,
        parsedPayload: payload,
      };
    }

    if (computedDigest !== payload.sha256Digest) {
      return {
        valid: false,
        errorReason: `Digest mismatch: expected "${payload.sha256Digest}", computed "${computedDigest}"`,
        parsedPayload: payload,
        computedDigest,
      };
    }

    // 4. Lamport clock & origin timestamp plausibility checks
    const frontierOrigins = Object.keys(payload.snapshotFrontier);
    for (const origin of frontierOrigins) {
      const ts = payload.originFrontierTimestamps[origin];
      if (typeof ts !== 'number' || !Number.isInteger(ts) || ts < 0) {
        return {
          valid: false,
          errorReason: `Missing or invalid origin frontier timestamp for origin "${origin}"`,
          parsedPayload: payload,
          computedDigest,
        };
      }
    }

    const calculatedMaxTs = Math.max(0, ...Object.values(payload.originFrontierTimestamps));
    if (payload.maxLogicalTimestamp !== calculatedMaxTs) {
      return {
        valid: false,
        errorReason: `Lamport maxLogicalTimestamp invariant violated: declared ${payload.maxLogicalTimestamp}, calculated ${calculatedMaxTs}`,
        parsedPayload: payload,
        computedDigest,
      };
    }

    // 5. Compacted checkpoints sorting & validity
    let lastOrigin = '';
    let lastSeq = -1;
    for (const cp of payload.compactedCheckpoints) {
      if (!cp.originPeerId || typeof cp.sequenceNumber !== 'number' || cp.sequenceNumber <= 0) {
        return {
          valid: false,
          errorReason: 'Malformed compacted checkpoint item in snapshot payload',
          parsedPayload: payload,
          computedDigest,
        };
      }
      // Check deterministic sort order: (originPeerId, sequenceNumber)
      if (cp.originPeerId < lastOrigin || (cp.originPeerId === lastOrigin && cp.sequenceNumber < lastSeq)) {
        return {
          valid: false,
          errorReason: 'Compacted checkpoints are not sorted deterministically by (originPeerId, sequenceNumber)',
          parsedPayload: payload,
          computedDigest,
        };
      }
      lastOrigin = cp.originPeerId;
      lastSeq = cp.sequenceNumber;
    }

    // 6. Snapshot candidate eligibility evaluation
    const eligibility = evaluateSnapshotEligibility({
      candidateSnapshot: payload,
      currentReplicationVector,
      currentEpoch,
      currentFrontierTimestamps,
      logCapacity: options?.logCapacity,
      checkRepairableHorizon: options?.checkRepairableHorizon,
    });

    if (!eligibility.eligible) {
      return {
        valid: false,
        errorReason: `Candidate ineligible: ${eligibility.eligibility} - ${eligibility.reason}`,
        parsedPayload: payload,
        computedDigest,
        eligibility,
      };
    }

    return {
      valid: true,
      parsedPayload: payload,
      computedDigest,
      eligibility,
    };
  }

  /**
   * STAGE 2: Pre-construct candidate EngineCoreState and all sub-objects out-of-band.
   * All potentially throwing operations (checkpoint ingestion, allocations) execute here.
   * Seals the candidate core (Object.seal) and releases creator references upon completion.
   * Modifies ZERO engine state.
   */
  public stage(
    validationResult: SnapshotValidationResult,
    engineConfig: ResolvedSyncEngineConfig,
    now?: number,
    reducerFn?: (state: PlaybackState, op: StoredOperation) => PlaybackState,
  ): StagedSnapshotInstallation {
    if (!validationResult.valid || !validationResult.parsedPayload) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Cannot stage invalid snapshot validation result: ${validationResult.errorReason ?? 'Unknown validation error'}`,
      );
    }

    const payload = validationResult.parsedPayload;
    const anchorNow = now ?? Date.now();

    // 1. Construct candidate PlaybackState
    const rawState = payload.state as Partial<PlaybackState>;
    const importedState = Object.freeze<PlaybackState>({
      status: rawState.status ?? 'paused',
      anchorPositionMs: rawState.anchorPositionMs ?? 0,
      anchorLocalTimestamp: anchorNow,
      playbackRate: rawState.playbackRate ?? 1.0,
      stateVersion: (rawState.stateVersion ?? 0) + 1,
      lastAppliedEvent: Object.freeze({
        logicalEventId: `snap-${payload.snapshotId}`,
        senderPeerId: payload.authorPeerId,
        messageType: 'SNAPSHOT_IMPORT',
        appliedAtLocalTimestamp: anchorNow,
        isImportedProvenance: true,
      }),
    });

    // 2. Construct candidate LogicalClockManager
    const candidateClockManager = new LogicalClockManager(payload.maxLogicalTimestamp);

    // 3. Construct candidate OrderedOperationLog
    const logCapacity = (engineConfig as unknown as { logCapacity?: number }).logCapacity ?? 256;
    const candidateLog = new OrderedOperationLog(
      logCapacity,
      importedState,
      reducerFn ?? ((s) => s),
    );
    candidateLog.reset(importedState, payload.snapshotFrontier);

    // Ingest all compacted checkpoints
    for (const cp of payload.compactedCheckpoints) {
      const cpRecord = cp as {
        readonly originPeerId: string;
        readonly sequenceNumber: number;
        readonly logicalTimestamp: number;
        readonly logicalEventId?: string;
        readonly fingerprint?: string;
      };
      candidateLog.setCompactedCheckpoint(
        cpRecord.originPeerId,
        cpRecord.sequenceNumber,
        cpRecord.logicalTimestamp,
        cpRecord.logicalEventId ?? '',
        cpRecord.fingerprint ?? '',
      );
    }

    // Ingest frontier timestamps if not already present
    for (const [peer, seq] of Object.entries(payload.snapshotFrontier)) {
      const ts = payload.originFrontierTimestamps[peer];
      if (ts !== undefined && candidateLog.getCompactedCheckpoint(peer, seq) === undefined) {
        candidateLog.setCompactedCheckpoint(peer, seq, ts, '');
      }
    }

    // 4. Construct candidate PendingDependencyQueue
    const candidatePendingQueue = new PendingDependencyQueue(logCapacity);

    // 5. Construct candidate causal vectors (defensive copies)
    const candidateReplicationVector: Record<string, number> = { ...payload.snapshotFrontier };
    const candidateLocalCausalContext: Record<string, number> = { ...payload.snapshotFrontier };
    const candidateBaseFrontier: Record<string, number> = { ...payload.snapshotFrontier };

    // 6. Assemble and seal candidate EngineCoreState
    const candidateCore = createEngineCoreState({
      playbackState: importedState,
      baseSnapshot: importedState,
      baseSnapshotFrontier: candidateBaseFrontier,
      operationLog: candidateLog,
      clockManager: candidateClockManager,
      pendingQueue: candidatePendingQueue,
      replicationVector: candidateReplicationVector,
      localCausalContext: candidateLocalCausalContext,
      localOriginSequence: 0,
      currentEpoch: payload.membershipEpoch,
      lastAppliedEventId: `snap-${payload.snapshotId}`,
      stateVersion: importedState.stateVersion,
    });

    return {
      payload,
      canonicalDigest: validationResult.computedDigest ?? payload.sha256Digest,
      targetEpoch: payload.membershipEpoch,
      targetFrontier: { ...payload.snapshotFrontier },
      candidateCore,
    };
  }

  /**
   * STAGE 3: Irreversible Synchronous Atomic Commit Point.
   * Replaces engine state root in a single synchronous reference assignment:
   *   engine.replaceCoreStateAtomic(staged.candidateCore);
   * CANNOT throw under run-to-completion. Drops creator aliases post-handoff.
   */
  public commitAtomic(
    engine: { replaceCoreStateAtomic: (candidate: EngineCoreState) => void; isDisposed?: boolean },
    staged: StagedSnapshotInstallation,
  ): SnapshotInstallationReport {
    if (!staged || !staged.candidateCore) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'StagedSnapshotInstallation contains no candidate core state');
    }
    if (engine.isDisposed) {
      throw new SyncEngineError('ENGINE_DISPOSED', 'Cannot commit snapshot to a disposed engine');
    }

    const startTime = performance.now();

    // The single synchronous atomic root replacement:
    engine.replaceCoreStateAtomic(staged.candidateCore);

    const installedEpoch = staged.targetEpoch;
    const installedFrontier = { ...staged.targetFrontier };
    const installedMaxLogicalTimestamp = staged.payload.maxLogicalTimestamp;

    // Explicit creator dereferencing: drop reference so installer holds no alias to the live root
    (staged as { candidateCore: unknown }).candidateCore = null;

    const commitDurationMs = performance.now() - startTime;

    return {
      success: true,
      installedEpoch,
      installedFrontier,
      installedMaxLogicalTimestamp,
      commitDurationMs,
    };
  }
}
