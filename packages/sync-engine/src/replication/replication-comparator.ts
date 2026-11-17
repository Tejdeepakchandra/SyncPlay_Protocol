/**
 * Deterministic State Digest Replication Comparator.
 *
 * Implements Phase 7.4 specifications:
 * - PHASE7_4_ARCHITECTURE.md §3.3
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §2.1
 * - PHASE7_4_ACCEPTANCE_CRITERIA.md AC-2
 *
 * Pure, deterministic comparison function independent of peer identity,
 * clock readings, or input object insertion order.
 *
 * SEMANTIC DEFINITIONS:
 * - replicationVector[p] = k: highest contiguous sequence for origin p that has passed
 *   validation, causal ordering, and deterministic reduction. Sequences 1..k are applied.
 * - snapshotFrontier[p] = k: highest contiguous sequence for origin p already incorporated
 *   into the compacted base snapshot. Sequences 1..k have been compacted out of the active log.
 *   The oldest retained sequence in the active operation log is snapshotFrontier[p] + 1.
 * - historyFingerprints[p]: canonical SHA-256 fingerprint of the operation at sequence k = replicationVector[p].
 *   Detects conflicting operations at the compared replication frontier (V_rep[p]), but does NOT
 *   prove equality of arbitrary unchained predecessor operations in earlier history.
 */

import type { StateDigestPayload } from '@syncplay/protocol';
import type {
  DigestComparisonResult,
  DigestComparisonClassification,
  OriginComparisonDetail,
  OriginComparisonStatus,
  MissingSequenceRange,
} from './types.js';

/**
 * Pure deterministic comparison between a local state digest and a remote state digest.
 *
 * Guaranteed properties:
 * 1. Determinism: Given identical local and remote digests, produces identical results.
 * 2. Key Order Invariant: Independent of JavaScript object key insertion order (lexicographically sorted origins).
 * 3. Immutable: Returns defensively constructed read-only results without leaking internal references.
 * 4. Horizon Aware: Detects when missing sequences fall into compacted base snapshots, requiring snapshot catch-up.
 * 5. Conflict Detecting: Identifies tail fingerprint mismatches for equal sequences as sequence collisions / forks.
 * 6. Historical Limits: Flags compacted sequences lacking retained fingerprints as unprovable historical equality.
 *
 * @param localDigest The local peer's StateDigestPayload
 * @param remoteDigest The remote peer's StateDigestPayload
 * @returns Structured, deterministic DigestComparisonResult
 */
export function compareDigests(
  localDigest: StateDigestPayload,
  remoteDigest: StateDigestPayload,
): DigestComparisonResult {
  const remotePeerId = remoteDigest.senderPeerId;

  // 1. Session verification: Anti-entropy must be room-scoped
  if (localDigest.sessionId !== remoteDigest.sessionId) {
    return Object.freeze({
      remotePeerId,
      missingFromLocal: Object.freeze([]),
      missingFromRemote: Object.freeze([]),
      forkDetected: false,
      conflictingOrigins: Object.freeze([]),
      outsideLocalHorizon: false,
      classification: 'INCONCLUSIVE',
      outsideRemoteHorizon: false,
      requiresSnapshotRecovery: false,
      unprovableOrigins: Object.freeze([]),
      hasUnverifiedHistory: false,
      originDetails: Object.freeze([]),
      diagnosticReasons: Object.freeze([
        `Session mismatch: local session '${localDigest.sessionId}' vs remote '${remoteDigest.sessionId}'`,
      ]),
    });
  }

  // 2. Epoch verification: Anti-entropy operations are strictly epoch-scoped
  if (localDigest.membershipEpoch !== remoteDigest.membershipEpoch) {
    return Object.freeze({
      remotePeerId,
      missingFromLocal: Object.freeze([]),
      missingFromRemote: Object.freeze([]),
      forkDetected: false,
      conflictingOrigins: Object.freeze([]),
      outsideLocalHorizon: false,
      classification: 'INCONCLUSIVE',
      outsideRemoteHorizon: false,
      requiresSnapshotRecovery: false,
      unprovableOrigins: Object.freeze([]),
      hasUnverifiedHistory: false,
      originDetails: Object.freeze([]),
      diagnosticReasons: Object.freeze([
        `Epoch mismatch: local epoch ${localDigest.membershipEpoch} vs remote ${remoteDigest.membershipEpoch}`,
      ]),
    });
  }

  // 3. Extract and sort all origins lexicographically for 100% determinism
  const originSet = new Set<string>();
  for (const origin of Object.keys(localDigest.replicationVector)) originSet.add(origin);
  for (const origin of Object.keys(remoteDigest.replicationVector)) originSet.add(origin);
  for (const origin of Object.keys(localDigest.snapshotFrontier)) originSet.add(origin);
  for (const origin of Object.keys(remoteDigest.snapshotFrontier)) originSet.add(origin);

  const allOrigins = Array.from(originSet).sort();

  const missingFromLocal: MissingSequenceRange[] = [];
  const missingFromRemote: MissingSequenceRange[] = [];
  const conflictingOrigins: string[] = [];
  const unprovableOrigins: string[] = [];
  const originDetails: OriginComparisonDetail[] = [];
  const diagnosticReasons: string[] = [];

  let forkDetected = false;
  let outsideLocalHorizon = false;
  let outsideRemoteHorizon = false;
  let requiresSnapshotRecovery = false;

  for (const origin of allOrigins) {
    const localSeq = localDigest.replicationVector[origin] ?? 0;
    const remoteSeq = remoteDigest.replicationVector[origin] ?? 0;
    const localSnap = localDigest.snapshotFrontier[origin] ?? 0;
    const remoteSnap = remoteDigest.snapshotFrontier[origin] ?? 0;
    const localFp = localDigest.historyFingerprints[origin];
    const remoteFp = remoteDigest.historyFingerprints[origin];

    // Invariant check: snapshotFrontier must not exceed replicationVector
    if (localSnap > localSeq) {
      diagnosticReasons.push(
        `Local snapshotFrontier (${localSnap}) exceeds replicationVector (${localSeq}) for origin '${origin}'`,
      );
    }
    if (remoteSnap > remoteSeq) {
      diagnosticReasons.push(
        `Remote snapshotFrontier (${remoteSnap}) exceeds replicationVector (${remoteSeq}) for origin '${origin}'`,
      );
    }

    let status: OriginComparisonStatus;
    let missingRange: MissingSequenceRange | undefined;
    let reason: string | undefined;

    if (localSeq === remoteSeq) {
      if (localSeq === 0) {
        status = 'IDENTICAL';
      } else {
        // Both peers have observed the exact same sequence count
        if (localFp !== undefined && remoteFp !== undefined) {
          if (localFp === remoteFp) {
            status = 'IDENTICAL';
          } else {
            // Sequence collision or fork detected! Identical sequence number, divergent fingerprint
            status = 'CONFLICT_FORK';
            forkDetected = true;
            conflictingOrigins.push(origin);
            reason = `Fingerprint mismatch at sequence ${localSeq}: local '${localFp}' vs remote '${remoteFp}'`;
          }
        } else {
          // One or both tail fingerprints missing.
          // In Phase 7.4, matching replication vectors cannot prove equality across compacted history
          // if fingerprints are absent.
          status = 'HISTORICAL_EQUALITY_UNPROVABLE';
          unprovableOrigins.push(origin);
          reason = `Historical equality at sequence ${localSeq} cannot be verified: missing tail fingerprint`;
        }
      }
    } else if (localSeq < remoteSeq) {
      // Remote is ahead on this origin: local is missing (localSeq + 1)..remoteSeq
      const fromSeq = localSeq + 1;
      const toSeq = remoteSeq;
      missingRange = { originPeerId: origin, fromSequence: fromSeq, toSequence: toSeq };

      if (fromSeq <= remoteSnap) {
        // Remote has compacted part or all of the needed range into its base snapshot
        // (sequences 1..remoteSnap are compacted into base snapshot, not retained in log)
        status = 'OUTSIDE_REMOTE_HORIZON';
        outsideRemoteHorizon = true;
        requiresSnapshotRecovery = true;
        reason = `Remote compacted needed sequence range [${fromSeq}..${toSeq}] at snapshot frontier ${remoteSnap} (sequences <= ${remoteSnap} are compacted)`;
      } else {
        status = localSeq === 0 ? 'REMOTE_ONLY' : 'REMOTE_AHEAD';
        missingFromLocal.push(missingRange);
      }
    } else {
      // Local is ahead on this origin: remote is missing (remoteSeq + 1)..localSeq
      const fromSeq = remoteSeq + 1;
      const toSeq = localSeq;
      missingRange = { originPeerId: origin, fromSequence: fromSeq, toSequence: toSeq };

      if (fromSeq <= localSnap) {
        // Local has compacted part or all of the requested range into its base snapshot
        // (sequences 1..localSnap are compacted into base snapshot, not retained in log)
        status = 'OUTSIDE_LOCAL_HORIZON';
        outsideLocalHorizon = true;
        reason = `Local compacted needed sequence range [${fromSeq}..${toSeq}] at snapshot frontier ${localSnap} (sequences <= ${localSnap} are compacted)`;
      } else {
        status = remoteSeq === 0 ? 'LOCAL_ONLY' : 'LOCAL_AHEAD';
        missingFromRemote.push(missingRange);
      }
    }

    originDetails.push(
      Object.freeze({
        originPeerId: origin,
        status,
        localSequence: localSeq,
        remoteSequence: remoteSeq,
        localSnapshotFrontier: localSnap,
        remoteSnapshotFrontier: remoteSnap,
        localFingerprint: localFp,
        remoteFingerprint: remoteFp,
        missingRange,
        reason,
      }),
    );
  }

  // 4. Derive overall vector classification
  let classification: DigestComparisonClassification;
  if (forkDetected) {
    classification = 'DIVERGENT_FORK';
  } else if (requiresSnapshotRecovery) {
    classification = 'REQUIRES_SNAPSHOT_RECOVERY';
  } else if (
    unprovableOrigins.length > 0 &&
    missingFromLocal.length === 0 &&
    missingFromRemote.length === 0
  ) {
    classification = 'INCONCLUSIVE';
  } else if (missingFromLocal.length === 0 && missingFromRemote.length === 0) {
    classification = 'IDENTICAL';
  } else if (missingFromLocal.length === 0 && missingFromRemote.length > 0) {
    classification = 'LOCAL_DOMINATES';
  } else if (missingFromLocal.length > 0 && missingFromRemote.length === 0) {
    classification = 'REMOTE_DOMINATES';
  } else {
    classification = 'CONCURRENT_INCOMPARABLE';
  }

  const hasUnverifiedHistory = unprovableOrigins.length > 0;

  return Object.freeze({
    remotePeerId,
    missingFromLocal: Object.freeze(missingFromLocal),
    missingFromRemote: Object.freeze(missingFromRemote),
    forkDetected,
    conflictingOrigins: Object.freeze(conflictingOrigins),
    outsideLocalHorizon,
    classification,
    outsideRemoteHorizon,
    requiresSnapshotRecovery,
    unprovableOrigins: Object.freeze(unprovableOrigins),
    hasUnverifiedHistory,
    originDetails: Object.freeze(originDetails),
    ...(diagnosticReasons.length > 0 ? { diagnosticReasons: Object.freeze(diagnosticReasons) } : {}),
  });
}
