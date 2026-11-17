import { describe, it, expect } from 'vitest';
import type { StateDigestPayload } from '@syncplay/protocol';
import { compareDigests } from '../replication/replication-comparator.js';
import { computeOperationFingerprint } from '../log/operation-fingerprint.js';

function makeDigest(overrides: Partial<StateDigestPayload> = {}): StateDigestPayload {
  return {
    formatVersion: 2,
    sessionId: 'session-test-room',
    senderPeerId: 'peer-local',
    connectionGeneration: 1,
    membershipEpoch: 1,
    stateVersion: 10,
    replicationVector: {},
    snapshotFrontier: {},
    historyFingerprints: {},
    sentAt: 1000,
    ...overrides,
  };
}

const sampleFp1 = computeOperationFingerprint({
  originPeerId: 'peer-A',
  originSequenceNumber: 5,
  logicalEventId: 'evt-1',
  logicalTimestamp: 10,
  causalContext: {},
  messageType: 'PLAY',
  payload: { positionMs: 5000 },
  membershipEpoch: 1,
});

const sampleFp2 = computeOperationFingerprint({
  originPeerId: 'peer-A',
  originSequenceNumber: 5,
  logicalEventId: 'evt-2-divergent',
  logicalTimestamp: 10,
  causalContext: {},
  messageType: 'PLAY',
  payload: { positionMs: 6000 },
  membershipEpoch: 1,
});

const sampleFpB = computeOperationFingerprint({
  originPeerId: 'peer-B',
  originSequenceNumber: 3,
  logicalEventId: 'evt-b',
  logicalTimestamp: 12,
  causalContext: { 'peer-A': 5 },
  messageType: 'PAUSE',
  payload: {},
  membershipEpoch: 1,
});

describe('ReplicationComparator', () => {
  describe('Preconditions and Session/Epoch validation', () => {
    it('returns INCONCLUSIVE when session IDs do not match', () => {
      const local = makeDigest({ sessionId: 'session-alpha' });
      const remote = makeDigest({ sessionId: 'session-beta', senderPeerId: 'peer-remote' });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.remotePeerId).toBe('peer-remote');
      expect(result.missingFromLocal).toEqual([]);
      expect(result.missingFromRemote).toEqual([]);
      expect(result.diagnosticReasons?.[0]).toContain('Session mismatch');
    });

    it('returns INCONCLUSIVE when membership epochs do not match', () => {
      const local = makeDigest({ membershipEpoch: 1 });
      const remote = makeDigest({ membershipEpoch: 2, senderPeerId: 'peer-remote' });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.diagnosticReasons?.[0]).toContain('Epoch mismatch');
    });
  });

  describe('Identical Digests', () => {
    it('classifies empty replication vectors as IDENTICAL', () => {
      const local = makeDigest();
      const remote = makeDigest({ senderPeerId: 'peer-remote' });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('IDENTICAL');
      expect(result.forkDetected).toBe(false);
      expect(result.missingFromLocal).toEqual([]);
      expect(result.missingFromRemote).toEqual([]);
      expect(result.conflictingOrigins).toEqual([]);
      expect(result.outsideLocalHorizon).toBe(false);
      expect(result.outsideRemoteHorizon).toBe(false);
      expect(result.hasUnverifiedHistory).toBe(false);
      expect(result.unverifiedHistoryOrigins).toEqual([]);
    });

    it('classifies matching sequence 1 vectors with matching fingerprints as fully IDENTICAL', () => {
      const fp1 = computeOperationFingerprint({
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        logicalEventId: 'evt-1',
        logicalTimestamp: 10,
        causalContext: {},
        messageType: 'PLAY',
        payload: { positionMs: 1000 },
        membershipEpoch: 1,
      });

      const local = makeDigest({
        replicationVector: { 'peer-A': 1 },
        historyFingerprints: { 'peer-A': fp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 1 },
        historyFingerprints: { 'peer-A': fp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('IDENTICAL');
      expect(result.forkDetected).toBe(false);
      expect(result.originDetails[0]?.status).toBe('IDENTICAL');
      expect(result.originDetails[0]?.frontierAgreement).toBe(true);
      expect(result.originDetails[0]?.historyVerified).toBe(true);
      expect(result.hasUnverifiedHistory).toBe(false);
      expect(result.unverifiedHistoryOrigins).toHaveLength(0);
    });

    it('classifies matching non-empty vectors (k > 1) with matching fingerprints as IDENTICAL at frontier, but FRONTIER_IDENTICAL with unverified intermediate history', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5, 'peer-B': 3 },
        historyFingerprints: { 'peer-A': sampleFp1, 'peer-B': sampleFpB },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5, 'peer-B': 3 },
        historyFingerprints: { 'peer-A': sampleFp1, 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('IDENTICAL');
      expect(result.forkDetected).toBe(false);
      expect(result.missingFromLocal).toHaveLength(0);
      expect(result.missingFromRemote).toHaveLength(0);
      // For k > 1, frontier matches (frontierAgreement = true), but intermediate history is unverified (status = FRONTIER_IDENTICAL)
      expect(result.originDetails.every((d) => d.status === 'FRONTIER_IDENTICAL')).toBe(true);
      expect(result.originDetails.every((d) => d.frontierAgreement)).toBe(true);
      expect(result.originDetails.every((d) => !d.historyVerified)).toBe(true);
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-A', 'peer-B']);
    });
  });

  describe('Domination and Lag (Missing Sequences)', () => {
    it('detects local ahead (LOCAL_DOMINATES) when local has higher sequence', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 2 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('LOCAL_DOMINATES');
      expect(result.missingFromLocal).toHaveLength(0);
      expect(result.missingFromRemote).toHaveLength(1);
      expect(result.missingFromRemote[0]).toEqual({
        originPeerId: 'peer-A',
        fromSequence: 3,
        toSequence: 5,
      });
    });

    it('detects remote ahead (REMOTE_DOMINATES) when remote has higher sequence within horizon', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 2 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 7 },
        snapshotFrontier: { 'peer-A': 0 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REMOTE_DOMINATES');
      expect(result.missingFromLocal).toHaveLength(1);
      expect(result.missingFromLocal[0]).toEqual({
        originPeerId: 'peer-A',
        fromSequence: 3,
        toSequence: 7,
      });
      expect(result.missingFromRemote).toHaveLength(0);
      expect(result.outsideRemoteHorizon).toBe(false);
    });

    it('detects origin present only on local side (LOCAL_ONLY)', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 4 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: {},
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('LOCAL_DOMINATES');
      expect(result.missingFromRemote).toEqual([
        { originPeerId: 'peer-A', fromSequence: 1, toSequence: 4 },
      ]);
    });

    it('detects origin present only on remote side (REMOTE_ONLY)', () => {
      const local = makeDigest();
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-B': 3 },
        snapshotFrontier: { 'peer-B': 0 },
        historyFingerprints: { 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REMOTE_DOMINATES');
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-B', fromSequence: 1, toSequence: 3 },
      ]);
    });

    it('detects concurrent incomparable progress across multiple origins', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5, 'peer-B': 2 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 3, 'peer-B': 6 },
        historyFingerprints: { 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('CONCURRENT_INCOMPARABLE');
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-B', fromSequence: 3, toSequence: 6 },
      ]);
      expect(result.missingFromRemote).toEqual([
        { originPeerId: 'peer-A', fromSequence: 4, toSequence: 5 },
      ]);
      expect(result.forkDetected).toBe(false);
    });
  });

  describe('Fingerprint Conflicts and Fork Detection', () => {
    it('detects sequence collision/fork when vectors match but tail fingerprints differ', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp2 }, // divergent SHA-256 for same origin:seq
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('DIVERGENT_FORK');
      expect(result.forkDetected).toBe(true);
      expect(result.conflictingOrigins).toContain('peer-A');
      expect(result.missingFromLocal).toHaveLength(0);
      expect(result.missingFromRemote).toHaveLength(0);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('CONFLICT_FORK');
      expect(detail?.reason).toContain('Fingerprint mismatch');
    });

    it('flags unprovable equality when equal sequence has no tail fingerprint', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
        // No fingerprint
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.forkDetected).toBe(false);
      expect(result.unprovableOrigins).toContain('peer-A');
      expect(result.hasUnverifiedHistory).toBe(true);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('HISTORICAL_EQUALITY_UNPROVABLE');
    });

    it('documents mandatory fingerprint completeness limitation: tail fingerprint does NOT detect intermediate divergence', () => {
      // Mandatory test case:
      // Seq 10: op A (both)
      // Seq 11: op B on Peer A vs op X on Peer B (divergent!)
      // Seq 12: op C (both have identical op C at sequence 12)
      // Both advertise replicationVector = { 'peer-A': 12 }
      // Both advertise tail fingerprint = fp(op C)
      const opC_Fp = computeOperationFingerprint({
        originPeerId: 'peer-A',
        originSequenceNumber: 12,
        logicalEventId: 'evt-c-12',
        logicalTimestamp: 15,
        causalContext: { 'peer-A': 10 }, // Note: does not chain op 11's hash
        messageType: 'PLAY',
        payload: { positionMs: 12000 },
        membershipEpoch: 1,
      });

      const localDigest = makeDigest({
        replicationVector: { 'peer-A': 12 },
        historyFingerprints: { 'peer-A': opC_Fp },
      });

      const remoteDigest = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 12 },
        historyFingerprints: { 'peer-A': opC_Fp }, // exact same tail fingerprint
      });

      const result = compareDigests(localDigest, remoteDigest);

      // Demonstrates Option 1 property:
      // The state digest compares the tail operation at the replication frontier (sequence 12).
      // Since both peers have identical op C at sequence 12, the comparison classifies as IDENTICAL at the frontier.
      // It does NOT detect that Peer A had op B at seq 11 while Peer B had op X at seq 11!
      expect(result.classification).toBe('IDENTICAL');
      expect(result.forkDetected).toBe(false);

      // Under Phase 7.4 M2 correction, the comparator explicitly marks intermediate history unverified:
      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detail.status).toBe('FRONTIER_IDENTICAL');
      expect(detail.frontierAgreement).toBe(true); // Property A holds
      expect(detail.historyVerified).toBe(false); // Property B does NOT hold
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-A']);

      // Intermediate divergence at sequence 11 is detected during event admission / repair replay
      // via Phase 7.3 originSeqIndex check, not by the single tail digest fingerprint.
    });
  });

  describe('Historical Completeness & Frontier Agreement Verification (Phase 7.4 M2 Correction)', () => {
    it('1. equal vectors + equal tail fingerprints do not claim complete historical equality', () => {
      // k = 12 on both sides with matching tail fingerprint
      const local = makeDigest({
        replicationVector: { 'peer-A': 12 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 12 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('IDENTICAL');
      expect(result.forkDetected).toBe(false);

      // Crucial: Property A (frontier agreement) is true, but Property B (history verified) is false
      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detail.frontierAgreement).toBe(true);
      expect(detail.historyVerified).toBe(false);
      expect(detail.status).toBe('FRONTIER_IDENTICAL');
      expect(detail.reason).toContain('intermediate history (1..11) is unverified');

      // Global indicator flags unverified history
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-A']);
    });

    it('2. equal vectors + different tail fingerprints produce a confirmed frontier conflict', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp2 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('DIVERGENT_FORK');
      expect(result.forkDetected).toBe(true);
      expect(result.conflictingOrigins).toEqual(['peer-A']);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detail.status).toBe('CONFLICT_FORK');
      expect(detail.frontierAgreement).toBe(false);
      expect(detail.historyVerified).toBe(false);
    });

    it('3. equal vectors + missing fingerprints remain historically unverified', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('INCONCLUSIVE');
      expect(result.forkDetected).toBe(false);
      expect(result.unprovableOrigins).toEqual(['peer-A']);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-A']);
      expect(result.hasUnverifiedHistory).toBe(true);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detail.status).toBe('HISTORICAL_EQUALITY_UNPROVABLE');
      expect(detail.frontierAgreement).toBe(false);
      expect(detail.historyVerified).toBe(false);
    });

    it('4. a peer missing an operation range remains correctly classified as behind, while any separate historical uncertainty is preserved', () => {
      // Origin A: local is behind (2 vs 7) -> REMOTE_AHEAD, missingFromLocal [3..7]
      // Origin B: local and remote both have 5 with matching tail fingerprint -> FRONTIER_IDENTICAL (unverified intermediate history)
      const local = makeDigest({
        replicationVector: { 'peer-A': 2, 'peer-B': 5 },
        historyFingerprints: { 'peer-B': sampleFpB },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 7, 'peer-B': 5 },
        snapshotFrontier: { 'peer-A': 0, 'peer-B': 0 },
        historyFingerprints: { 'peer-A': sampleFp1, 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      // Correctly classified as behind (REMOTE_DOMINATES)
      expect(result.classification).toBe('REMOTE_DOMINATES');
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-A', fromSequence: 3, toSequence: 7 },
      ]);
      expect(result.missingFromRemote).toHaveLength(0);

      // Separate historical uncertainty on origin B is strictly preserved!
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-B']);
    });

    it('5. a multi-origin comparison with one verified frontier and another unverified history preserves that uncertainty', () => {
      // Origin A: sequence 1 with matching fingerprint -> verified frontier and verified history! (op 1 has no predecessor)
      // Origin B: sequence 5 with matching tail fingerprint -> frontier agreement, but unverified intermediate history!
      const fpA1 = computeOperationFingerprint({
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        logicalEventId: 'evt-a-1',
        logicalTimestamp: 10,
        causalContext: {},
        messageType: 'PLAY',
        payload: { positionMs: 1000 },
        membershipEpoch: 1,
      });

      const local = makeDigest({
        replicationVector: { 'peer-A': 1, 'peer-B': 5 },
        historyFingerprints: { 'peer-A': fpA1, 'peer-B': sampleFpB },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 1, 'peer-B': 5 },
        historyFingerprints: { 'peer-A': fpA1, 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('IDENTICAL');

      // Origin A has verified frontier and verified history
      const detailA = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detailA.status).toBe('IDENTICAL');
      expect(detailA.frontierAgreement).toBe(true);
      expect(detailA.historyVerified).toBe(true);

      // Origin B has frontier agreement but unverified history
      const detailB = result.originDetails.find((d) => d.originPeerId === 'peer-B')!;
      expect(detailB.status).toBe('FRONTIER_IDENTICAL');
      expect(detailB.frontierAgreement).toBe(true);
      expect(detailB.historyVerified).toBe(false);

      // The comparison preserves the uncertainty from Origin B
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unverifiedHistoryOrigins).toEqual(['peer-B']);
    });

    it('6. a confirmed fork still takes precedence over ordinary dominance', () => {
      // Origin A: conflicting fingerprints at sequence 5 -> CONFLICT_FORK
      // Origin B: local has 2, remote has 8 -> REMOTE_AHEAD (ordinary dominance)
      const local = makeDigest({
        replicationVector: { 'peer-A': 5, 'peer-B': 2 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5, 'peer-B': 8 },
        historyFingerprints: { 'peer-A': sampleFp2, 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);
      // Fork takes absolute precedence over ordinary dominance!
      expect(result.classification).toBe('DIVERGENT_FORK');
      expect(result.forkDetected).toBe(true);
      expect(result.conflictingOrigins).toEqual(['peer-A']);
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-B', fromSequence: 3, toSequence: 8 },
      ]);
    });

    it('7. snapshot recovery requirements remain correctly represented', () => {
      // Origin A: needed range [3..8], remote snapshot frontier is 5 (1..5 compacted)
      const local = makeDigest({
        replicationVector: { 'peer-A': 2 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 8 },
        snapshotFrontier: { 'peer-A': 5 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REQUIRES_SNAPSHOT_RECOVERY');
      expect(result.outsideRemoteHorizon).toBe(true);
      expect(result.requiresSnapshotRecovery).toBe(true);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A')!;
      expect(detail.status).toBe('OUTSIDE_REMOTE_HORIZON');
      expect(detail.frontierAgreement).toBe(false);
      expect(detail.historyVerified).toBe(false);
    });
  });

  describe('Compacted History and Snapshot Horizon Boundary Tests', () => {
    it('flags OUTSIDE_REMOTE_HORIZON when missing range is entirely below compacted frontier', () => {
      // Missing range is 3..5. Remote snapshot frontier is 5 (sequences 1..5 compacted)
      const local = makeDigest({
        replicationVector: { 'peer-A': 2 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
        snapshotFrontier: { 'peer-A': 5 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REQUIRES_SNAPSHOT_RECOVERY');
      expect(result.outsideRemoteHorizon).toBe(true);
      expect(result.requiresSnapshotRecovery).toBe(true);
      expect(result.missingFromLocal).toHaveLength(0);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('OUTSIDE_REMOTE_HORIZON');
      expect(detail?.reason).toContain('Remote compacted needed sequence range [3..5]');
    });

    it('flags OUTSIDE_REMOTE_HORIZON when missing range starts exactly at the compacted frontier', () => {
      // Local has 4. Remote has 8. Remote snapshot frontier is 5 (sequences 1..5 compacted).
      // Needed range is 5..8. First missing sequence (5) is compacted in snapshot!
      const local = makeDigest({
        replicationVector: { 'peer-A': 4 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 8 },
        snapshotFrontier: { 'peer-A': 5 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REQUIRES_SNAPSHOT_RECOVERY');
      expect(result.outsideRemoteHorizon).toBe(true);
      expect(result.requiresSnapshotRecovery).toBe(true);
      expect(result.missingFromLocal).toHaveLength(0);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('OUTSIDE_REMOTE_HORIZON');
      expect(detail?.reason).toContain('Remote compacted needed sequence range [5..8]');
    });

    it('accepts log repair when missing range starts immediately after compacted frontier', () => {
      // Local has 5. Remote has 8. Remote snapshot frontier is 5 (sequences 1..5 compacted).
      // Needed range is 6..8. First missing sequence (6) is > remoteSnap (5), so 6..8 are retained in log!
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 8 },
        snapshotFrontier: { 'peer-A': 5 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REMOTE_DOMINATES');
      expect(result.outsideRemoteHorizon).toBe(false);
      expect(result.requiresSnapshotRecovery).toBe(false);
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-A', fromSequence: 6, toSequence: 8 },
      ]);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('REMOTE_AHEAD');
    });

    it('accepts log repair when origin has no compacted operations (snapshotFrontier = 0)', () => {
      const local = makeDigest();
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 4 },
        snapshotFrontier: { 'peer-A': 0 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });

      const result = compareDigests(local, remote);
      expect(result.classification).toBe('REMOTE_DOMINATES');
      expect(result.outsideRemoteHorizon).toBe(false);
      expect(result.requiresSnapshotRecovery).toBe(false);
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-A', fromSequence: 1, toSequence: 4 },
      ]);
    });

    it('detects structural inconsistency when snapshotFrontier exceeds replicationVector', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 5 },
        snapshotFrontier: { 'peer-A': 8 }, // Invariant violation: snap (8) > rep (5)
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5 },
      });

      const result = compareDigests(local, remote);
      expect(result.diagnosticReasons?.some((r) => r.includes('exceeds replicationVector'))).toBe(true);
    });

    it('flags outsideLocalHorizon when remote needs sequence that local has already compacted', () => {
      const local = makeDigest({
        replicationVector: { 'peer-A': 10 },
        snapshotFrontier: { 'peer-A': 6 }, // local compacted 1..6
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 3 }, // remote needs 4..10
      });

      const result = compareDigests(local, remote);
      expect(result.outsideLocalHorizon).toBe(true);
      expect(result.missingFromRemote).toHaveLength(0);

      const detail = result.originDetails.find((d) => d.originPeerId === 'peer-A');
      expect(detail?.status).toBe('OUTSIDE_LOCAL_HORIZON');
      expect(detail?.reason).toContain('Local compacted needed sequence range');
    });
  });

  describe('Multi-Origin Combinations & Global Dominance Completeness (Correction C)', () => {
    it('sets hasUnverifiedHistory = true when clear missing range exists alongside unverified origin', () => {
      // Origin A: local is ahead (5 vs 2) -> missingFromRemote: [3..5]
      // Origin B: local and remote both have 10, but NO fingerprint provided -> unverified!
      const local = makeDigest({
        replicationVector: { 'peer-A': 5, 'peer-B': 10 },
        historyFingerprints: { 'peer-A': sampleFp1 },
        // peer-B omitted from fingerprints
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 2, 'peer-B': 10 },
        // peer-B omitted from fingerprints
      });

      const result = compareDigests(local, remote);

      // Dominance classification is LOCAL_DOMINATES based on replication vectors...
      expect(result.classification).toBe('LOCAL_DOMINATES');
      expect(result.missingFromRemote).toEqual([
        { originPeerId: 'peer-A', fromSequence: 3, toSequence: 5 },
      ]);

      // ...BUT completeness indicator explicitly warns caller that historical equality is unverified on peer-B!
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.unprovableOrigins).toEqual(['peer-B']);
    });

    it('prioritizes DIVERGENT_FORK when fork on one origin is combined with normal missing range on another', () => {
      // Origin A: both at 5, but conflicting fingerprints!
      // Origin B: local has 2, remote has 6 -> normal missing range [3..6]
      const local = makeDigest({
        replicationVector: { 'peer-A': 5, 'peer-B': 2 },
        historyFingerprints: { 'peer-A': sampleFp1 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 5, 'peer-B': 6 },
        historyFingerprints: { 'peer-A': sampleFp2, 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);

      // Critical fork takes priority for overall classification
      expect(result.classification).toBe('DIVERGENT_FORK');
      expect(result.forkDetected).toBe(true);
      expect(result.conflictingOrigins).toEqual(['peer-A']);

      // Missing range on peer-B is still preserved for diagnostics/repair awareness
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-B', fromSequence: 3, toSequence: 6 },
      ]);
    });

    it('prioritizes REQUIRES_SNAPSHOT_RECOVERY when outside-horizon origin is combined with normal missing range', () => {
      // Origin A: remote has 10, snapshotFrontier 8; local has 2 -> needed 3..10, but 3 <= snap(8) -> outside horizon!
      // Origin B: remote has 5, snapshotFrontier 0; local has 2 -> needed 3..5, in log
      const local = makeDigest({
        replicationVector: { 'peer-A': 2, 'peer-B': 2 },
      });
      const remote = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 10, 'peer-B': 5 },
        snapshotFrontier: { 'peer-A': 8, 'peer-B': 0 },
        historyFingerprints: { 'peer-B': sampleFpB },
      });

      const result = compareDigests(local, remote);

      expect(result.classification).toBe('REQUIRES_SNAPSHOT_RECOVERY');
      expect(result.outsideRemoteHorizon).toBe(true);
      expect(result.requiresSnapshotRecovery).toBe(true);

      // Normal missing range on peer-B is still retained
      expect(result.missingFromLocal).toEqual([
        { originPeerId: 'peer-B', fromSequence: 3, toSequence: 5 },
      ]);
    });
  });

  describe('Determinism & Immutability', () => {
    it('guarantees identical output regardless of object key insertion order', () => {
      const digest1 = makeDigest({
        replicationVector: { 'peer-Z': 1, 'peer-A': 2, 'peer-M': 3 },
      });
      const digest2 = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-A': 2, 'peer-M': 4, 'peer-Z': 1 },
      });

      // Forward comparison
      const resForward = compareDigests(digest1, digest2);

      // Same data constructed with reversed key insertion order
      const digest1Reversed = makeDigest({
        replicationVector: { 'peer-M': 3, 'peer-Z': 1, 'peer-A': 2 },
      });
      const digest2Reversed = makeDigest({
        senderPeerId: 'peer-remote',
        replicationVector: { 'peer-Z': 1, 'peer-M': 4, 'peer-A': 2 },
      });

      const resReversed = compareDigests(digest1Reversed, digest2Reversed);

      expect(resForward.classification).toBe(resReversed.classification);
      expect(resForward.missingFromLocal).toEqual(resReversed.missingFromLocal);
      expect(resForward.missingFromRemote).toEqual(resReversed.missingFromRemote);
      expect(resForward.originDetails).toEqual(resReversed.originDetails);
      // Check alphabetical ordering
      expect(resForward.originDetails.map((d) => d.originPeerId)).toEqual(['peer-A', 'peer-M', 'peer-Z']);
    });

    it('returns deeply frozen immutable comparison structures', () => {
      const local = makeDigest({ replicationVector: { 'peer-A': 5 } });
      const remote = makeDigest({ senderPeerId: 'peer-remote', replicationVector: { 'peer-A': 8 } });

      const result = compareDigests(local, remote);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.missingFromLocal)).toBe(true);
      expect(Object.isFrozen(result.originDetails)).toBe(true);
      if (result.originDetails[0]) {
        expect(Object.isFrozen(result.originDetails[0])).toBe(true);
      }
    });
  });
});
