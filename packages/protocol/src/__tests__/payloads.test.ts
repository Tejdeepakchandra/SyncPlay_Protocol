/**
 * Tests for individual payload schemas.
 *
 * Each message type's payload is tested for valid input,
 * missing required fields, and invalid values.
 */
import { describe, it, expect } from 'vitest';
import {
  PingPayloadSchema,
  PongPayloadSchema,
} from '../payloads/ping-pong.js';
import {
  PlayPayloadSchema,
  PausePayloadSchema,
  SeekPayloadSchema,
  RateChangePayloadSchema,
} from '../payloads/control.js';
import {
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadV1Schema,
  LegacyStateDigestPayloadSchema,
  isLegacyStateDigest,
} from '../payloads/state.js';
import {
  StateDigestPayloadSchema,
  StateDigestPayloadV2Schema,
  isStateDigestV2,
  RepairRequestPayloadSchema,
  OperationBatchPayloadSchema,
  RepairRejectedPayloadSchema,
  REPAIR_REJECTION_REASONS,
  SerializedControlOperationSchema,
} from '../payloads/repair.js';

// ── PING ─────────────────────────────────────────────────────

describe('PingPayloadSchema', () => {
  it('should accept a valid PING payload', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: 'probe-1',
      senderTimestamp: 1234.5,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.probeId).toBe('probe-1');
      expect(result.data.senderTimestamp).toBe(1234.5);
    }
  });

  it('should accept zero timestamp', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: 'probe-1',
      senderTimestamp: 0,
    });
    expect(result.success).toBe(true);
  });

  it('should reject missing probeId', () => {
    const result = PingPayloadSchema.safeParse({ senderTimestamp: 100 });
    expect(result.success).toBe(false);
  });

  it('should reject missing senderTimestamp', () => {
    const result = PingPayloadSchema.safeParse({ probeId: 'probe-1' });
    expect(result.success).toBe(false);
  });

  it('should reject empty probeId', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: '',
      senderTimestamp: 100,
    });
    expect(result.success).toBe(false);
  });

  it('should reject negative senderTimestamp', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: 'probe-1',
      senderTimestamp: -1,
    });
    expect(result.success).toBe(false);
  });

  it('should reject NaN senderTimestamp', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: 'probe-1',
      senderTimestamp: NaN,
    });
    expect(result.success).toBe(false);
  });

  it('should reject Infinity senderTimestamp', () => {
    const result = PingPayloadSchema.safeParse({
      probeId: 'probe-1',
      senderTimestamp: Infinity,
    });
    expect(result.success).toBe(false);
  });
});

// ── PONG ─────────────────────────────────────────────────────

describe('PongPayloadSchema', () => {
  it('should accept a valid PONG payload', () => {
    const result = PongPayloadSchema.safeParse({ probeId: 'probe-1' });
    expect(result.success).toBe(true);
  });

  it('should reject empty probeId', () => {
    const result = PongPayloadSchema.safeParse({ probeId: '' });
    expect(result.success).toBe(false);
  });

  it('should reject missing probeId', () => {
    const result = PongPayloadSchema.safeParse({});
    expect(result.success).toBe(false);
  });
});

// ── PLAY ─────────────────────────────────────────────────────

describe('PlayPayloadSchema', () => {
  it('should accept PLAY with targetPositionMs', () => {
    const result = PlayPayloadSchema.safeParse({ targetPositionMs: 5000 });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.targetPositionMs).toBe(5000);
    }
  });

  it('should accept PLAY without targetPositionMs', () => {
    const result = PlayPayloadSchema.safeParse({});
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.targetPositionMs).toBeUndefined();
    }
  });

  it('should accept PLAY with zero position', () => {
    const result = PlayPayloadSchema.safeParse({ targetPositionMs: 0 });
    expect(result.success).toBe(true);
  });

  it('should reject negative targetPositionMs', () => {
    const result = PlayPayloadSchema.safeParse({ targetPositionMs: -100 });
    expect(result.success).toBe(false);
  });

  it('should reject NaN targetPositionMs', () => {
    const result = PlayPayloadSchema.safeParse({ targetPositionMs: NaN });
    expect(result.success).toBe(false);
  });

  it('should reject Infinity targetPositionMs', () => {
    const result = PlayPayloadSchema.safeParse({ targetPositionMs: Infinity });
    expect(result.success).toBe(false);
  });
});

// ── PAUSE ────────────────────────────────────────────────────

describe('PausePayloadSchema', () => {
  it('should accept PAUSE with observedPositionMs', () => {
    const result = PausePayloadSchema.safeParse({
      observedPositionMs: 12000,
    });
    expect(result.success).toBe(true);
  });

  it('should accept PAUSE without observedPositionMs', () => {
    const result = PausePayloadSchema.safeParse({});
    expect(result.success).toBe(true);
  });

  it('should reject negative observedPositionMs', () => {
    const result = PausePayloadSchema.safeParse({
      observedPositionMs: -1,
    });
    expect(result.success).toBe(false);
  });
});

// ── SEEK ─────────────────────────────────────────────────────

describe('SeekPayloadSchema', () => {
  it('should accept valid SEEK payload', () => {
    const result = SeekPayloadSchema.safeParse({ targetPositionMs: 30000 });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.targetPositionMs).toBe(30000);
    }
  });

  it('should accept SEEK to position zero', () => {
    const result = SeekPayloadSchema.safeParse({ targetPositionMs: 0 });
    expect(result.success).toBe(true);
  });

  it('should reject SEEK without targetPositionMs', () => {
    const result = SeekPayloadSchema.safeParse({});
    expect(result.success).toBe(false);
  });

  it('should reject negative targetPositionMs', () => {
    const result = SeekPayloadSchema.safeParse({ targetPositionMs: -500 });
    expect(result.success).toBe(false);
  });

  it('should reject NaN targetPositionMs', () => {
    const result = SeekPayloadSchema.safeParse({ targetPositionMs: NaN });
    expect(result.success).toBe(false);
  });
});

// ── RATE_CHANGE ──────────────────────────────────────────────

describe('RateChangePayloadSchema', () => {
  it('should accept valid playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: 1.5 });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.playbackRate).toBe(1.5);
    }
  });

  it('should accept normal speed (1.0)', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: 1.0 });
    expect(result.success).toBe(true);
  });

  it('should accept slow playback (0.25)', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: 0.25 });
    expect(result.success).toBe(true);
  });

  it('should reject zero playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: 0 });
    expect(result.success).toBe(false);
  });

  it('should reject negative playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: -1 });
    expect(result.success).toBe(false);
  });

  it('should reject NaN playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({ playbackRate: NaN });
    expect(result.success).toBe(false);
  });

  it('should reject Infinity playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({
      playbackRate: Infinity,
    });
    expect(result.success).toBe(false);
  });

  it('should reject missing playbackRate', () => {
    const result = RateChangePayloadSchema.safeParse({});
    expect(result.success).toBe(false);
  });
});

// ── STATE_REQUEST ────────────────────────────────────────────

describe('StateRequestPayloadSchema', () => {
  it('should accept valid STATE_REQUEST with INITIAL_SYNC', () => {
    const result = StateRequestPayloadSchema.safeParse({
      requestId: 'req-001',
      reason: 'INITIAL_SYNC',
    });
    expect(result.success).toBe(true);
  });

  it('should accept valid STATE_REQUEST with REJOIN', () => {
    const result = StateRequestPayloadSchema.safeParse({
      requestId: 'req-002',
      reason: 'REJOIN',
    });
    expect(result.success).toBe(true);
  });

  it('should accept valid STATE_REQUEST with DIVERGENCE_SUSPECTED', () => {
    const result = StateRequestPayloadSchema.safeParse({
      requestId: 'req-003',
      reason: 'DIVERGENCE_SUSPECTED',
    });
    expect(result.success).toBe(true);
  });

  it('should reject unknown reason', () => {
    const result = StateRequestPayloadSchema.safeParse({
      requestId: 'req-001',
      reason: 'UNKNOWN_REASON',
    });
    expect(result.success).toBe(false);
  });

  it('should reject missing requestId', () => {
    const result = StateRequestPayloadSchema.safeParse({
      reason: 'REJOIN',
    });
    expect(result.success).toBe(false);
  });

  it('should reject missing reason', () => {
    const result = StateRequestPayloadSchema.safeParse({
      requestId: 'req-001',
    });
    expect(result.success).toBe(false);
  });
});

// ── STATE_SNAPSHOT ────────────────────────────────────────────

describe('StateSnapshotPayloadSchema', () => {
  it('should accept valid STATE_SNAPSHOT', () => {
    const result = StateSnapshotPayloadSchema.safeParse({
      snapshotId: 'snap-001',
      stateVersion: 5,
      state: { playbackStatus: 'paused', positionMs: 12000 },
    });
    expect(result.success).toBe(true);
  });

  it('should accept STATE_SNAPSHOT with null state', () => {
    const result = StateSnapshotPayloadSchema.safeParse({
      snapshotId: 'snap-001',
      stateVersion: 0,
      state: null,
    });
    expect(result.success).toBe(true);
  });

  it('should reject negative stateVersion', () => {
    const result = StateSnapshotPayloadSchema.safeParse({
      snapshotId: 'snap-001',
      stateVersion: -1,
      state: {},
    });
    expect(result.success).toBe(false);
  });

  it('should reject non-integer stateVersion', () => {
    const result = StateSnapshotPayloadSchema.safeParse({
      snapshotId: 'snap-001',
      stateVersion: 1.5,
      state: {},
    });
    expect(result.success).toBe(false);
  });
});

// ── STATE_DIGEST (Phase 2 Legacy) ──────────────────────────

describe('StateDigestPayloadV1Schema (Phase 2 Legacy Digest)', () => {
  it('should accept valid Phase 2 legacy STATE_DIGEST', () => {
    const result = StateDigestPayloadV1Schema.safeParse({
      digestId: 'digest-001',
      stateVersion: 3,
      digest: 'sha256:abc123',
    });
    expect(result.success).toBe(true);
  });

  it('should reject empty digest string', () => {
    const result = StateDigestPayloadV1Schema.safeParse({
      digestId: 'digest-001',
      stateVersion: 3,
      digest: '',
    });
    expect(result.success).toBe(false);
  });

  it('should reject digest exceeding max length', () => {
    const result = StateDigestPayloadV1Schema.safeParse({
      digestId: 'digest-001',
      stateVersion: 3,
      digest: 'X'.repeat(4097),
    });
    expect(result.success).toBe(false);
  });

  it('should accept digest at max length', () => {
    const result = StateDigestPayloadV1Schema.safeParse({
      digestId: 'digest-001',
      stateVersion: 3,
      digest: 'X'.repeat(4096),
    });
    expect(result.success).toBe(true);
  });

  it('should identify legacy digest via isLegacyStateDigest', () => {
    const legacy = {
      digestId: 'digest-001',
      stateVersion: 3,
      digest: 'sha256:abc123',
    };
    expect(isLegacyStateDigest(legacy)).toBe(true);
    expect(isStateDigestV2(legacy)).toBe(false);
    expect(isLegacyStateDigest(null)).toBe(false);
    expect(isLegacyStateDigest(undefined)).toBe(false);
    expect(isLegacyStateDigest({ other: 'field' })).toBe(false);
  });
});

// ── STATE_DIGEST (Phase 7.4 V2 Multi-Vector) ─────────────────

describe('StateDigestPayloadSchema (Phase 7.4 V2 Multi-Vector Digest)', () => {
  const validV2Digest = {
    formatVersion: 2 as const,
    sessionId: 'session-room-1',
    senderPeerId: 'peer-sender-1',
    connectionGeneration: 1,
    membershipEpoch: 2,
    stateVersion: 42,
    replicationVector: {
      'peer-sender-1': 10,
      'peer-remote-2': 5,
    },
    snapshotFrontier: {
      'peer-sender-1': 0,
      'peer-remote-2': 0,
    },
    historyFingerprints: {
      'peer-sender-1': 'a1b2c3d4e5f60718',
      'peer-remote-2': '0123456789abcdef0123456789abcdef',
    },
    sentAt: 1700000000000,
  };

  it('should accept a valid Phase 7.4 V2 state digest', () => {
    const result = StateDigestPayloadSchema.safeParse(validV2Digest);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.formatVersion).toBe(2);
      expect(result.data.sessionId).toBe('session-room-1');
      expect(result.data.replicationVector['peer-sender-1']).toBe(10);
      expect(result.data.historyFingerprints['peer-remote-2']).toBe(
        '0123456789abcdef0123456789abcdef',
      );
    }
  });

  it('should verify isStateDigestV2 helper returns true for valid v2 digest', () => {
    expect(isStateDigestV2(validV2Digest)).toBe(true);
    expect(isLegacyStateDigest(validV2Digest)).toBe(false);
  });

  it('should reject digest missing formatVersion', () => {
    const { formatVersion, ...withoutVersion } = validV2Digest;
    const result = StateDigestPayloadSchema.safeParse(withoutVersion);
    expect(result.success).toBe(false);
  });

  it('should reject digest with formatVersion: 1', () => {
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      formatVersion: 1,
    });
    expect(result.success).toBe(false);
  });

  it('should reject digest with unsupported future formatVersion: 3', () => {
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      formatVersion: 3,
    });
    expect(result.success).toBe(false);
  });

  it('should reject non-number formatVersion', () => {
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      formatVersion: '2',
    });
    expect(result.success).toBe(false);
  });

  it('should reject legacy Phase 2 digest passed to V2 schema', () => {
    const legacy = {
      digestId: 'digest-001',
      stateVersion: 3,
      digest: 'sha256:abc123',
    };
    const result = StateDigestPayloadSchema.safeParse(legacy);
    expect(result.success).toBe(false);
  });

  it('should reject missing or empty sessionId', () => {
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, sessionId: '' }).success,
    ).toBe(false);
  });

  it('should reject zero or negative connectionGeneration', () => {
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, connectionGeneration: 0 }).success,
    ).toBe(false);
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, connectionGeneration: -1 }).success,
    ).toBe(false);
  });

  it('should reject zero or negative membershipEpoch', () => {
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, membershipEpoch: 0 }).success,
    ).toBe(false);
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, membershipEpoch: -2 }).success,
    ).toBe(false);
  });

  it('should reject negative stateVersion', () => {
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, stateVersion: -1 }).success,
    ).toBe(false);
  });

  it('should reject negative sequence numbers in replicationVector', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        replicationVector: { 'peer-1': -1 },
      }).success,
    ).toBe(false);
  });

  it('should reject negative sequence numbers in snapshotFrontier', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        snapshotFrontier: { 'peer-1': -5 },
      }).success,
    ).toBe(false);
  });

  it('should reject invalid hex characters in historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'not-a-hex-string!!' },
      }).success,
    ).toBe(false);
  });

  it('should reject fingerprints shorter than 16 characters or longer than 64', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'abc' },
      }).success,
    ).toBe(false);
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'a'.repeat(65) },
      }).success,
    ).toBe(false);
  });

  it('should reject negative sentAt timestamp', () => {
    expect(
      StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: -100 }).success,
    ).toBe(false);
  });
});

// ── REPAIR_REQUEST (Phase 7.4) ────────────────────────────────

describe('RepairRequestPayloadSchema (Phase 7.4)', () => {
  const validRequest = {
    requestId: 'req-uuid-12345',
    sessionId: 'session-room-1',
    requesterPeerId: 'peer-requester-1',
    targetOriginPeerId: 'peer-author-1',
    fromSequence: 1,
    toSequence: 10,
    membershipEpoch: 1,
  };

  it('should accept a valid repair request within 32 operations range', () => {
    const result = RepairRequestPayloadSchema.safeParse(validRequest);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.requestId).toBe('req-uuid-12345');
      expect(result.data.fromSequence).toBe(1);
      expect(result.data.toSequence).toBe(10);
      expect(result.data.toSequence - result.data.fromSequence + 1).toBe(10);
    }
  });

  it('should accept single-operation repair request (from == to)', () => {
    const result = RepairRequestPayloadSchema.safeParse({
      ...validRequest,
      fromSequence: 5,
      toSequence: 5,
    });
    expect(result.success).toBe(true);
  });

  it('should accept exactly 32 operations range (1 to 32)', () => {
    const result = RepairRequestPayloadSchema.safeParse({
      ...validRequest,
      fromSequence: 1,
      toSequence: 32,
    });
    expect(result.success).toBe(true);
  });

  it('should reject range exceeding 32 operations (1 to 33 = 33 ops)', () => {
    const result = RepairRequestPayloadSchema.safeParse({
      ...validRequest,
      fromSequence: 1,
      toSequence: 33,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain(
        'Requested range cannot exceed 32 operations per request',
      );
    }
  });

  it('should reject inverted sequence range (toSequence < fromSequence)', () => {
    const result = RepairRequestPayloadSchema.safeParse({
      ...validRequest,
      fromSequence: 10,
      toSequence: 5,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain(
        'toSequence must be greater than or equal to fromSequence',
      );
    }
  });

  it('should reject missing or empty requestId', () => {
    const { requestId, ...withoutId } = validRequest;
    expect(RepairRequestPayloadSchema.safeParse(withoutId).success).toBe(false);
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, requestId: '' }).success,
    ).toBe(false);
  });

  it('should reject zero or negative fromSequence', () => {
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: 0 }).success,
    ).toBe(false);
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: -1 }).success,
    ).toBe(false);
  });

  it('should reject non-integer sequence numbers', () => {
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: 1.5 }).success,
    ).toBe(false);
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, toSequence: 2.7 }).success,
    ).toBe(false);
  });

  it('should reject zero or negative membershipEpoch', () => {
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, membershipEpoch: 0 }).success,
    ).toBe(false);
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, membershipEpoch: -1 }).success,
    ).toBe(false);
  });
});

// ── OPERATION_BATCH (Phase 7.4) ───────────────────────────────

describe('OperationBatchPayloadSchema (Phase 7.4)', () => {
  const createTestOp = (seq: number) => ({
    logicalEventId: `op-evt-${seq}`,
    originPeerId: 'peer-author-1',
    membershipEpoch: 1,
    messageType: 'PLAY' as const,
    logicalTimestamp: seq,
    originSequenceNumber: seq,
    causalContext: { 'peer-author-1': seq - 1 },
    payload: { targetPositionMs: seq * 1000 },
  });

  const validBatch = {
    requestId: 'req-uuid-12345',
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    originPeerId: 'peer-author-1',
    fromSequence: 1,
    toSequence: 2,
    operations: [createTestOp(1), createTestOp(2)],
  };

  it('should accept a valid operation batch with echoed requestId', () => {
    const result = OperationBatchPayloadSchema.safeParse(validBatch);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.requestId).toBe('req-uuid-12345');
      expect(result.data.operations.length).toBe(2);
    }
  });

  it('should accept operation batch without optional requestId', () => {
    const { requestId, ...withoutId } = validBatch;
    const result = OperationBatchPayloadSchema.safeParse(withoutId);
    expect(result.success).toBe(true);
  });

  it('should accept operation batch without optional sessionId (Phase 7.3 backward compatibility)', () => {
    const { sessionId, ...withoutSession } = validBatch;
    const result = OperationBatchPayloadSchema.safeParse(withoutSession);
    expect(result.success).toBe(true);
  });

  it('should accept batch with exactly 32 operations', () => {
    const ops = Array.from({ length: 32 }, (_, i) => createTestOp(i + 1));
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 32,
      operations: ops,
    });
    expect(result.success).toBe(true);
  });

  it('should reject batch exceeding 32 operations', () => {
    const ops = Array.from({ length: 33 }, (_, i) => createTestOp(i + 1));
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 33,
      operations: ops,
    });
    expect(result.success).toBe(false);
  });

  it('should reject inverted sequence range in batch', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 5,
      toSequence: 2,
    });
    expect(result.success).toBe(false);
  });

  it('should reject operation with invalid messageType in batch', () => {
    const badOp = {
      ...createTestOp(1),
      messageType: 'UNSUPPORTED_TYPE',
    };
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      operations: [badOp],
    });
    expect(result.success).toBe(false);
  });
});

// ── REPAIR_REJECTED (Phase 7.4) ───────────────────────────────

describe('RepairRejectedPayloadSchema (Phase 7.4)', () => {
  const baseRejection = {
    requestId: 'req-uuid-12345',
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    targetOriginPeerId: 'peer-author-1',
    fromSequence: 5,
    toSequence: 10,
  };

  it.each(REPAIR_REJECTION_REASONS)('should accept valid rejection reason: %s', (reason) => {
    const result = RepairRejectedPayloadSchema.safeParse({
      ...baseRejection,
      reason,
      retainedHorizonSequence: reason === 'OUTSIDE_HORIZON' ? 20 : undefined,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.reason).toBe(reason);
    }
  });

  it('should accept OUTSIDE_HORIZON with retainedHorizonSequence', () => {
    const result = RepairRejectedPayloadSchema.safeParse({
      ...baseRejection,
      reason: 'OUTSIDE_HORIZON',
      retainedHorizonSequence: 50,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.retainedHorizonSequence).toBe(50);
    }
  });

  it('should reject unknown rejection reason', () => {
    const result = RepairRejectedPayloadSchema.safeParse({
      ...baseRejection,
      reason: 'INVALID_REASON',
    });
    expect(result.success).toBe(false);
  });

  it('should reject missing requestId', () => {
    const { requestId, ...withoutId } = baseRejection;
    const result = RepairRejectedPayloadSchema.safeParse({
      ...withoutId,
      reason: 'RATE_LIMITED',
    });
    expect(result.success).toBe(false);
  });

  it('should reject empty requestId', () => {
    const result = RepairRejectedPayloadSchema.safeParse({
      ...baseRejection,
      requestId: '',
      reason: 'BUSY',
    });
    expect(result.success).toBe(false);
  });
});
