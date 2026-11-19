import { describe, it, expect, beforeEach } from 'vitest';
import {
  captureMembershipView,
  calculateMembershipSafeFrontier,
  calculateLocalPressureFrontier,
  executeCompaction,
  CompactionManager,
  type CapturedMembershipView,
  type PeerConnectionDescriptor,
  type CompactionExecutionContext,
} from '../compaction/compaction-manager.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';
import { validateSnapshotPayloadV2 } from '../snapshot.js';
import type { StateDigestPayload } from '@syncplay/protocol';
import type { StoredOperation } from '../log/types.js';

function makeDigest(overrides: Partial<StateDigestPayload> = {}): StateDigestPayload {
  return {
    formatVersion: 2,
    sessionId: 'test-session',
    senderPeerId: 'peer-remote-1',
    connectionGeneration: 1,
    membershipEpoch: 1,
    stateVersion: 1,
    replicationVector: {},
    snapshotFrontier: {},
    historyFingerprints: {},
    sentAt: 1000,
    ...overrides,
  };
}

function makeOperation(
  originPeerId: string,
  seq: number,
  logicalTs: number,
  epoch: number = 1,
): StoredOperation {
  return {
    logicalEventId: `evt-${originPeerId}-${seq}`,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: epoch,
    messageType: 'PLAY',
    logicalTimestamp: logicalTs,
    originSequenceNumber: seq,
    causalContext: {},
    appliedAtMonotonic: 1000 + seq,
    payload: { status: 'playing', positionMs: seq * 1000, playbackRate: 1.0 },
  };
}

describe('Phase 7.5 M3 — Compaction Horizon & Membership View', () => {
  let log: OrderedOperationLog;

  beforeEach(() => {
    log = new OrderedOperationLog(256);
  });

  // Scenario 1: Local vector lower than every remote vector
  it('Scenario 1: local vector bounds the compaction frontier when lower than remote vectors', () => {
    const localVector = { 'origin-a': 5, 'origin-b': 8 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
      { peerId: 'peer-r2', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          replicationVector: { 'origin-a': 10, 'origin-b': 12 },
        }),
      ],
      [
        'peer-r2',
        makeDigest({
          senderPeerId: 'peer-r2',
          replicationVector: { 'origin-a': 8, 'origin-b': 9 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(true);
    expect(result.frontier).toEqual({
      'origin-a': 5, // min(5, 10, 8) = 5 (local limits)
      'origin-b': 8, // min(8, 12, 9) = 8 (local limits)
    });
  });

  // Scenario 2: A remote vector lower than local vector
  it('Scenario 2: remote vector bounds the compaction frontier when lower than local vector', () => {
    const localVector = { 'origin-a': 10, 'origin-b': 15 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          replicationVector: { 'origin-a': 4, 'origin-b': 12 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(true);
    expect(result.frontier).toEqual({
      'origin-a': 4, // min(10, 4) = 4 (remote limits)
      'origin-b': 12, // min(15, 12) = 12 (remote limits)
    });
  });

  // Scenario 3: Different origins having different limiting peers
  it('Scenario 3: different origins have different limiting peers in GLB', () => {
    const localVector = { 'origin-a': 10, 'origin-b': 10, 'origin-c': 10 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
      { peerId: 'peer-r2', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          replicationVector: { 'origin-a': 4, 'origin-b': 8, 'origin-c': 15 },
        }),
      ],
      [
        'peer-r2',
        makeDigest({
          senderPeerId: 'peer-r2',
          replicationVector: { 'origin-a': 9, 'origin-b': 3, 'origin-c': 12 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(true);
    expect(result.frontier).toEqual({
      'origin-a': 4, // Limited by peer-r1 (4)
      'origin-b': 3, // Limited by peer-r2 (3)
      'origin-c': 10, // Limited by local (10)
    });
  });

  // Scenario 4: Origin absent from remote vector treated as 0
  it('Scenario 4: origin absent from remote vector is treated strictly as sequence 0', () => {
    const localVector = { 'origin-a': 10, 'origin-b': 5 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          replicationVector: { 'origin-a': 8 }, // origin-b is completely absent
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(true);
    expect(result.frontier).toEqual({
      'origin-a': 8,
      'origin-b': 0, // Treated as 0, preventing compaction past 0 for origin-b
    });
  });

  // Scenario 5: Empty remote membership producing local vector
  it('Scenario 5: empty remote membership produces local vector as V_epoch_safe', () => {
    const localVector = { 'origin-a': 7, 'origin-b': 14 };
    const view = captureMembershipView(1, localVector, []);

    const result = calculateMembershipSafeFrontier(view, new Map());
    expect(result.eligible).toBe(true);
    expect(result.frontier).toEqual(localVector);
  });

  // Scenario 6: Connected peer having no current digest -> compaction held
  it('Scenario 6: connected peer with missing digest causes compaction to be held', () => {
    const localVector = { 'origin-a': 5 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
      { peerId: 'peer-r2', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    // Only peer-r1 has a digest; peer-r2 is missing
    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          replicationVector: { 'origin-a': 5 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(false);
    expect(result.reason).toBe('MISSING_PEER_DIGEST');
    expect(result.missingPeerId).toBe('peer-r2');
  });

  // Scenario 7: Digest from old connection generation rejected
  it('Scenario 7: digest with stale connection generation is rejected', () => {
    const localVector = { 'origin-a': 5 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 3 }, // Active generation is 3
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          connectionGeneration: 2, // Stale generation 2
          replicationVector: { 'origin-a': 5 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(false);
    expect(result.reason).toBe('STALE_PEER_GENERATION');
    expect(result.missingPeerId).toBe('peer-r1');
  });

  // Scenario 8: Digest from old membership epoch rejected
  it('Scenario 8: digest with stale membership epoch is rejected', () => {
    const localVector = { 'origin-a': 5 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
    ];
    const view = captureMembershipView(3, localVector, connectedPeers); // Active epoch is 3

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({
          senderPeerId: 'peer-r1',
          membershipEpoch: 2, // Stale epoch 2
          replicationVector: { 'origin-a': 5 },
        }),
      ],
    ]);

    const result = calculateMembershipSafeFrontier(view, digests);
    expect(result.eligible).toBe(false);
    expect(result.reason).toBe('STALE_PEER_EPOCH');
    expect(result.missingPeerId).toBe('peer-r1');
  });

  // Scenario 9: Peer disconnecting before compaction -> revalidation aborts
  it('Scenario 9: peer disconnecting before compaction commits causes pre-commit abort', () => {
    const localVector = { 'origin-a': 10 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
      { peerId: 'peer-r2', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({ senderPeerId: 'peer-r1', replicationVector: { 'origin-a': 5 } }),
      ],
      [
        'peer-r2',
        makeDigest({ senderPeerId: 'peer-r2', replicationVector: { 'origin-a': 6 } }),
      ],
    ]);

    const candidate = calculateMembershipSafeFrontier(view, digests);
    expect(candidate.eligible).toBe(true);

    // Populate log
    log.insert(makeOperation('origin-a', 1, 10));
    log.insert(makeOperation('origin-a', 2, 20));

    // Execution context where peer-r2 has disconnected
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [{ peerId: 'peer-r1', generation: 1 }], // peer-r2 disconnected!
      getLocalReplicationVector: () => ({ 'origin-a': 10 }),
      getPeerDigests: () => digests,
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('MEMBERSHIP_CHANGED');
    expect(execResult.prunedCount).toBe(0);
    expect(log.size).toBe(2); // Zero operations pruned
  });

  // Scenario 10: Peer reconnecting with new generation before compaction -> revalidation aborts
  it('Scenario 10: peer reconnecting with new generation before compaction causes pre-commit abort', () => {
    const localVector = { 'origin-a': 10 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
    ];
    const view = captureMembershipView(1, localVector, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({ senderPeerId: 'peer-r1', connectionGeneration: 1, replicationVector: { 'origin-a': 5 } }),
      ],
    ]);

    const candidate = calculateMembershipSafeFrontier(view, digests);
    expect(candidate.eligible).toBe(true);

    // At commit boundary, peer-r1 reconnected with generation 2
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [{ peerId: 'peer-r1', generation: 2 }], // Gen changed!
      getLocalReplicationVector: () => ({ 'origin-a': 10 }),
      getPeerDigests: () => digests,
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('MEMBERSHIP_CHANGED');
    expect(execResult.prunedCount).toBe(0);
  });

  // Scenario 11: Local vector advancing after capture -> compaction remains bounded and safe
  it('Scenario 11: local vector advancing after capture permits safe compaction up to captured bound', () => {
    const localVectorAtCapture = { 'origin-a': 5 };
    const connectedPeers: PeerConnectionDescriptor[] = [
      { peerId: 'peer-r1', generation: 1 },
    ];
    const view = captureMembershipView(1, localVectorAtCapture, connectedPeers);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({ senderPeerId: 'peer-r1', replicationVector: { 'origin-a': 5 } }),
      ],
    ]);

    const candidate = calculateMembershipSafeFrontier(view, digests);
    expect(candidate.frontier).toEqual({ 'origin-a': 5 });

    // Populate log with 8 operations
    for (let i = 1; i <= 8; i++) {
      log.insert(makeOperation('origin-a', i, i * 10));
    }

    // Local vector advanced from 5 to 8!
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [{ peerId: 'peer-r1', generation: 1 }],
      getLocalReplicationVector: () => ({ 'origin-a': 8 }), // Advanced
      getPeerDigests: () => digests,
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(true);
    expect(execResult.prunedCount).toBe(5); // Only 1..5 pruned, 6..8 retained!
    expect(log.size).toBe(3);
  });

  // Scenario 12: Local vector rollback/regression detected -> pre-commit aborts
  it('Scenario 12: local vector regression below proposed frontier causes pre-commit abort', () => {
    const localVector = { 'origin-a': 5 };
    const view = captureMembershipView(1, localVector, []);

    const candidate = calculateMembershipSafeFrontier(view, new Map());
    expect(candidate.frontier).toEqual({ 'origin-a': 5 });

    log.insert(makeOperation('origin-a', 1, 10));

    // Local vector rolled back to 3!
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [],
      getLocalReplicationVector: () => ({ 'origin-a': 3 }), // Regressed!
      getPeerDigests: () => new Map(),
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('LOCAL_VECTOR_REGRESSED');
    expect(execResult.prunedCount).toBe(0);
    expect(log.size).toBe(1);
  });

  // Scenario 13: Membership changing between capture and commit (new peer joined) -> aborts
  it('Scenario 13: new peer joining between capture and commit causes pre-commit abort', () => {
    const localVector = { 'origin-a': 5 };
    const view = captureMembershipView(1, localVector, [{ peerId: 'peer-r1', generation: 1 }]);

    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({ senderPeerId: 'peer-r1', replicationVector: { 'origin-a': 5 } }),
      ],
    ]);

    const candidate = calculateMembershipSafeFrontier(view, digests);

    // At commit boundary, peer-r2 has joined
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [
        { peerId: 'peer-r1', generation: 1 },
        { peerId: 'peer-r2', generation: 1 }, // New peer!
      ],
      getLocalReplicationVector: () => ({ 'origin-a': 5 }),
      getPeerDigests: () => digests,
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('MEMBERSHIP_CHANGED');
    expect(execResult.prunedCount).toBe(0);
  });

  // Scenario 14: Epoch changing between capture and commit -> aborts
  it('Scenario 14: epoch advancing between capture and commit causes pre-commit abort', () => {
    const localVector = { 'origin-a': 5 };
    const view = captureMembershipView(1, localVector, []);
    const candidate = calculateMembershipSafeFrontier(view, new Map());

    // At commit boundary, epoch transitioned to 2
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 2, // New epoch!
      getConnectedPeers: () => [],
      getLocalReplicationVector: () => ({ 'origin-a': 5 }),
      getPeerDigests: () => new Map(),
      isRecoveryActive: () => false,
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('EPOCH_CHANGED');
    expect(execResult.prunedCount).toBe(0);
  });

  // Scenario 15: Local-pressure compaction never exceeding local applied vector
  it('Scenario 15: local-pressure compaction never exceeds local applied replication vector', () => {
    // Log has operations 1..10
    for (let i = 1; i <= 10; i++) {
      log.insert(makeOperation('origin-a', i, i * 10));
    }

    // But local applied vector only acknowledges up to 4
    const localVector = { 'origin-a': 4 };

    const pressureFrontier = calculateLocalPressureFrontier(localVector, log, 8);
    // Even though targetPruneCount is 8, the frontier must be clamped to 4!
    expect(pressureFrontier['origin-a']).toBe(4);
    expect(pressureFrontier['origin-a']).toBeLessThanOrEqual(localVector['origin-a']);
  });

  // Scenario 16: Zero partial compaction when pre-commit fails
  it('Scenario 16: zero operations pruned and state remains bit-exact intact when pre-commit aborts', () => {
    for (let i = 1; i <= 5; i++) {
      log.insert(makeOperation('origin-a', i, i * 10));
    }
    const initialSize = log.size;
    const initialOps = log.getAllOperations();

    const view = captureMembershipView(1, { 'origin-a': 5 }, [{ peerId: 'peer-r1', generation: 1 }]);
    const digests = new Map<string, StateDigestPayload>([
      [
        'peer-r1',
        makeDigest({ senderPeerId: 'peer-r1', replicationVector: { 'origin-a': 5 } }),
      ],
    ]);
    const candidate = calculateMembershipSafeFrontier(view, digests);

    // Fail via recovery active
    const context: CompactionExecutionContext = {
      getCurrentEpoch: () => 1,
      getConnectedPeers: () => [{ peerId: 'peer-r1', generation: 1 }],
      getLocalReplicationVector: () => ({ 'origin-a': 5 }),
      getPeerDigests: () => digests,
      isRecoveryActive: () => true, // Recovery active!
      compactLog: (f) => log.compactAtFrontier(f),
    };

    const execResult = executeCompaction(candidate, context);
    expect(execResult.committed).toBe(false);
    expect(execResult.abortReason).toBe('RECOVERY_ACTIVE');
    expect(execResult.prunedCount).toBe(0);

    // Verify complete state integrity
    expect(log.size).toBe(initialSize);
    expect(log.getAllOperations()).toEqual(initialOps);
    expect(log.getAllCompactedCheckpoints()).toHaveLength(0);
  });

  // Scenario 17: Snapshot metadata and canonical SHA-256 digest consistency via LeaderlessSyncEngine
  it('Scenario 17: LeaderlessSyncEngine exports snapshot V2 with canonical SHA-256 and validates cleanly', () => {
    const engine = new LeaderlessSyncEngine({
      sessionId: 'engine-session-1',
      localPeerId: 'peer-local',
    });

    // Authorize peer and record digest
    engine.authorizePeerGeneration('peer-remote', 1);
    const remoteDigest = makeDigest({
      sessionId: 'engine-session-1',
      senderPeerId: 'peer-remote',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-local': 2, 'peer-remote': 5 },
    });
    engine.recordPeerDigest(remoteDigest);

    // Export snapshot V2
    const snapshot = engine.exportSnapshotV2('snap-test-engine');
    expect(snapshot.snapshotId).toBe('snap-test-engine');
    expect(snapshot.sessionId).toBe('engine-session-1');
    expect(snapshot.membershipEpoch).toBe(1);
    expect(snapshot.authorPeerId).toBe('peer-local');
    expect(snapshot.formatVersion).toBe(2);

    // Validate with validateSnapshotPayloadV2
    const validated = validateSnapshotPayloadV2(snapshot, 'engine-session-1', 0.5, 2.0);
    expect(validated.snapshotId).toBe('snap-test-engine');
    expect(validated.sha256Digest).toBe(snapshot.sha256Digest);
  });

  // Scenario 18: Checkpoint preservation during compaction
  it('Scenario 18: compacted checkpoints are preserved up to capacity during compactAtFrontier', () => {
    // Insert 4 operations for origin-a and 3 for origin-b
    for (let i = 1; i <= 4; i++) {
      log.insert(makeOperation('origin-a', i, 100 + i));
    }
    for (let i = 1; i <= 3; i++) {
      log.insert(makeOperation('origin-b', i, 200 + i));
    }

    expect(log.size).toBe(7);

    // Compact at frontier: origin-a up to 3, origin-b up to 2
    const pruned = log.compactAtFrontier({ 'origin-a': 3, 'origin-b': 2 });
    expect(pruned).toBe(5);
    expect(log.size).toBe(2); // Remaining: origin-a seq 4, origin-b seq 3

    // Check compacted checkpoints
    const checkpoints = log.getAllCompactedCheckpoints();
    expect(checkpoints).toHaveLength(5);

    // Verify deterministic ordering: origin-a 1..3, then origin-b 1..2
    expect(checkpoints).toEqual([
      { originPeerId: 'origin-a', sequenceNumber: 1, logicalTimestamp: 101 },
      { originPeerId: 'origin-a', sequenceNumber: 2, logicalTimestamp: 102 },
      { originPeerId: 'origin-a', sequenceNumber: 3, logicalTimestamp: 103 },
      { originPeerId: 'origin-b', sequenceNumber: 1, logicalTimestamp: 201 },
      { originPeerId: 'origin-b', sequenceNumber: 2, logicalTimestamp: 202 },
    ]);

    // Check individual checkpoint accessors
    expect(log.getCompactedCheckpoint('origin-a', 2)).toBe(102);
    expect(log.getCompactedCheckpoint('origin-b', 1)).toBe(201);
    expect(log.getCompactedCheckpoint('origin-a', 4)).toBeUndefined(); // Still in active log!

    // Origin frontier timestamps resolution
    const frontierTimestamps = log.getOriginFrontierTimestamps({
      'origin-a': 3, // In checkpoint
      'origin-b': 3, // In active log
    });
    expect(frontierTimestamps).toEqual({
      'origin-a': 103,
      'origin-b': 203,
    });
  });
});
