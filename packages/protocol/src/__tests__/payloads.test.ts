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
  UUID_V4_REGEX,
  UUIDv4Schema,
  StateDigestPayloadSchema,
  StateDigestPayloadV2Schema,
  isStateDigestV2,
  RepairRequestPayloadSchema,
  OperationBatchPayloadSchema,
  CompatibilityOperationBatchPayloadSchema,
  RepairRejectedPayloadSchema,
  REPAIR_REJECTION_REASONS,
  SerializedControlOperationSchema,
  ReplicationVectorSchema,
  SnapshotFrontierSchema,
  HistoryFingerprintsSchema,
} from '../payloads/repair.js';
import { MAX_VECTOR_ENTRIES, MAX_OPERATIONS_PER_BATCH } from '../constants.js';

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

// ── UUID v4 Schema Tests ──────────────────────────────────────

describe('UUIDv4Schema', () => {
  const validUuidV4 = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

  it('should accept a canonical lowercase UUID v4', () => {
    expect(UUIDv4Schema.safeParse(validUuidV4).success).toBe(true);
  });

  it('should accept an uppercase UUID v4 (RFC 4122 case-insensitivity)', () => {
    expect(UUIDv4Schema.safeParse(validUuidV4.toUpperCase()).success).toBe(true);
  });

  it.each(['8', '9', 'a', 'b', 'A', 'B'])(
    'should accept valid variant character: %s in position 17',
    (variant) => {
      const uuid = `f47ac10b-58cc-4372-${variant}567-0e02b2c3d479`;
      expect(UUIDv4Schema.safeParse(uuid).success).toBe(true);
    },
  );

  it.each(['1', '2', '3', '5'])(
    'should reject non-v4 version digit: %s in position 13',
    (version) => {
      const uuid = `f47ac10b-58cc-${version}372-a567-0e02b2c3d479`;
      const result = UUIDv4Schema.safeParse(uuid);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.issues[0]?.message).toContain('Invalid UUID v4 format');
      }
    },
  );

  it.each(['0', '1', '7', 'c', 'd', 'e', 'f', 'C', 'D', 'E', 'F'])(
    'should reject invalid variant character: %s in position 17',
    (variant) => {
      const uuid = `f47ac10b-58cc-4372-${variant}567-0e02b2c3d479`;
      expect(UUIDv4Schema.safeParse(uuid).success).toBe(false);
    },
  );

  it('should reject UUID v4 without hyphens', () => {
    expect(UUIDv4Schema.safeParse('f47ac10b58cc4372a5670e02b2c3d479').success).toBe(false);
  });

  it('should reject UUID v4 with incorrect length (35 chars)', () => {
    expect(UUIDv4Schema.safeParse('f47ac10b-58cc-4372-a567-0e02b2c3d47').success).toBe(false);
  });

  it('should reject UUID v4 with incorrect length (37 chars)', () => {
    expect(UUIDv4Schema.safeParse('f47ac10b-58cc-4372-a567-0e02b2c3d4799').success).toBe(false);
  });

  it('should reject non-hex characters', () => {
    expect(UUIDv4Schema.safeParse('g47ac10b-58cc-4372-a567-0e02b2c3d479').success).toBe(false);
  });

  it('should reject empty or whitespace strings', () => {
    expect(UUIDv4Schema.safeParse('').success).toBe(false);
    expect(UUIDv4Schema.safeParse('   ').success).toBe(false);
  });
});

// ── STATE_DIGEST (Phase 7.4 V2 Multi-Vector) ─────────────────

describe('StateDigestPayloadSchema (Phase 7.4 V2 Multi-Vector Digest)', () => {
  const sha256A = 'a'.repeat(64);
  const sha256B = 'b'.repeat(64);

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
      'peer-sender-1': sha256A,
      'peer-remote-2': sha256B,
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
      expect(result.data.historyFingerprints['peer-remote-2']).toBe(sha256B);
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

  // ── History Fingerprint Full SHA-256 Tests ─────────────────

  it('should reject truncated 16-character historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'a1b2c3d4e5f60718' },
      }).success,
    ).toBe(false);
  });

  it('should reject truncated 32-character historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': '0123456789abcdef0123456789abcdef' },
      }).success,
    ).toBe(false);
  });

  it('should reject 63-character historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'a'.repeat(63) },
      }).success,
    ).toBe(false);
  });

  it('should reject 65-character historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'a'.repeat(65) },
      }).success,
    ).toBe(false);
  });

  it('should reject uppercase hexadecimal historyFingerprints (must be canonical lowercase)', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'A'.repeat(64) },
      }).success,
    ).toBe(false);
  });

  it('should reject invalid hex characters in historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': 'g'.repeat(64) },
      }).success,
    ).toBe(false);
  });

  it('should accept valid exact 64-character lowercase hex historyFingerprints', () => {
    expect(
      StateDigestPayloadSchema.safeParse({
        ...validV2Digest,
        historyFingerprints: { 'peer-1': '0123456789abcdef'.repeat(4) },
      }).success,
    ).toBe(true);
  });

  // ── Collection Bounds (MAX_VECTOR_ENTRIES = 64) ────────────

  it('should accept empty collections in StateDigest', () => {
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      replicationVector: {},
      snapshotFrontier: {},
      historyFingerprints: {},
    });
    expect(result.success).toBe(true);
  });

  it('should accept collections with exactly 64 origin entries (MAX_VECTOR_ENTRIES boundary)', () => {
    const atBoundReplication: Record<string, number> = {};
    const atBoundSnapshot: Record<string, number> = {};
    const atBoundFingerprints: Record<string, string> = {};

    for (let i = 1; i <= MAX_VECTOR_ENTRIES; i++) {
      const peerId = `p${i}`;
      atBoundReplication[peerId] = i;
      atBoundSnapshot[peerId] = 0;
      atBoundFingerprints[peerId] = sha256A;
    }

    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      replicationVector: atBoundReplication,
      snapshotFrontier: atBoundSnapshot,
      historyFingerprints: atBoundFingerprints,
    });
    expect(result.success).toBe(true);
  });

  it('should reject replicationVector exceeding 64 entries (65 entries)', () => {
    const overReplication: Record<string, number> = {};
    for (let i = 1; i <= MAX_VECTOR_ENTRIES + 1; i++) {
      overReplication[`p${i}`] = i;
    }
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      replicationVector: overReplication,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain(
        `Replication vector cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
      );
    }
  });

  it('should reject snapshotFrontier exceeding 64 entries (65 entries)', () => {
    const overSnapshot: Record<string, number> = {};
    for (let i = 1; i <= MAX_VECTOR_ENTRIES + 1; i++) {
      overSnapshot[`p${i}`] = 0;
    }
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      snapshotFrontier: overSnapshot,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain(
        `Snapshot frontier cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
      );
    }
  });

  it('should reject historyFingerprints exceeding 64 entries (65 entries)', () => {
    const overFingerprints: Record<string, string> = {};
    for (let i = 1; i <= MAX_VECTOR_ENTRIES + 1; i++) {
      overFingerprints[`p${i}`] = sha256A;
    }
    const result = StateDigestPayloadSchema.safeParse({
      ...validV2Digest,
      historyFingerprints: overFingerprints,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain(
        `History fingerprints cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
      );
    }
  });

  describe('Standalone Vector Schemas Boundary Tests (MAX_VECTOR_ENTRIES = 64)', () => {
    it('accepts ReplicationVectorSchema with exactly 64 entries and rejects 65 entries', () => {
      const atBound: Record<string, number> = {};
      const overBound: Record<string, number> = {};
      for (let i = 1; i <= MAX_VECTOR_ENTRIES; i++) {
        atBound[`p${i}`] = i;
        overBound[`p${i}`] = i;
      }
      overBound[`p65`] = 65;

      const okResult = ReplicationVectorSchema.safeParse(atBound);
      expect(okResult.success).toBe(true);

      const errResult = ReplicationVectorSchema.safeParse(overBound);
      expect(errResult.success).toBe(false);
      if (!errResult.success) {
        expect(errResult.error.issues[0]?.message).toContain(
          `Replication vector cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
        );
      }
    });

    it('accepts SnapshotFrontierSchema with exactly 64 entries and rejects 65 entries', () => {
      const atBound: Record<string, number> = {};
      const overBound: Record<string, number> = {};
      for (let i = 1; i <= MAX_VECTOR_ENTRIES; i++) {
        atBound[`p${i}`] = i;
        overBound[`p${i}`] = i;
      }
      overBound[`p65`] = 65;

      const okResult = SnapshotFrontierSchema.safeParse(atBound);
      expect(okResult.success).toBe(true);

      const errResult = SnapshotFrontierSchema.safeParse(overBound);
      expect(errResult.success).toBe(false);
      if (!errResult.success) {
        expect(errResult.error.issues[0]?.message).toContain(
          `Snapshot frontier cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
        );
      }
    });

    it('accepts HistoryFingerprintsSchema with exactly 64 entries and rejects 65 entries', () => {
      const atBound: Record<string, string> = {};
      const overBound: Record<string, string> = {};
      for (let i = 1; i <= MAX_VECTOR_ENTRIES; i++) {
        atBound[`p${i}`] = sha256A;
        overBound[`p${i}`] = sha256A;
      }
      overBound[`p65`] = sha256A;

      const okResult = HistoryFingerprintsSchema.safeParse(atBound);
      expect(okResult.success).toBe(true);

      const errResult = HistoryFingerprintsSchema.safeParse(overBound);
      expect(errResult.success).toBe(false);
      if (!errResult.success) {
        expect(errResult.error.issues[0]?.message).toContain(
          `History fingerprints cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
        );
      }
    });
  });

  // ── Timestamp and RTT Semantics Tests ──────────────────────

  it('should accept valid non-negative finite sentAt timestamp', () => {
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: 0 }).success).toBe(true);
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: 1700000000000 }).success).toBe(true);
  });

  it('should reject negative sentAt timestamp', () => {
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: -100 }).success).toBe(false);
  });

  it('should reject non-finite sentAt timestamp', () => {
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: Infinity }).success).toBe(false);
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: -Infinity }).success).toBe(false);
    expect(StateDigestPayloadSchema.safeParse({ ...validV2Digest, sentAt: NaN }).success).toBe(false);
  });

  it('documents that sentAt is sender telemetry and receiver must not compute RTT from it', () => {
    // Contract verification: sentAt provides sender transmission metadata.
    // Receivers must measure RTT via dedicated RTTEngine two-way probes.
    const digest = StateDigestPayloadSchema.parse(validV2Digest);
    expect(digest.sentAt).toBeGreaterThan(0);
  });
});

// ── REPAIR_REQUEST (Phase 7.4) ────────────────────────────────

describe('RepairRequestPayloadSchema (Phase 7.4)', () => {
  const validRequestUuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

  const validRequest = {
    requestId: validRequestUuid,
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
      expect(result.data.requestId).toBe(validRequestUuid);
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

  it('should reject non-UUID v4 requestId in repair request', () => {
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, requestId: 'req-opaque-123' }).success,
    ).toBe(false);
    expect(
      RepairRequestPayloadSchema.safeParse({ ...validRequest, requestId: 'f47ac10b-58cc-1372-a567-0e02b2c3d479' }).success,
    ).toBe(false);
  });

  it('should reject missing or empty requestId', () => {
    const { requestId, ...withoutId } = validRequest;
    expect(RepairRequestPayloadSchema.safeParse(withoutId).success).toBe(false);
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, requestId: '' }).success).toBe(false);
  });

  it('should reject zero or negative fromSequence', () => {
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: 0 }).success).toBe(false);
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: -1 }).success).toBe(false);
  });

  it('should reject non-integer sequence numbers', () => {
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, fromSequence: 1.5 }).success).toBe(false);
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, toSequence: 2.7 }).success).toBe(false);
  });

  it('should reject zero or negative membershipEpoch', () => {
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, membershipEpoch: 0 }).success).toBe(false);
    expect(RepairRequestPayloadSchema.safeParse({ ...validRequest, membershipEpoch: -1 }).success).toBe(false);
  });
});

// ── OPERATION_BATCH (Phase 7.4) ───────────────────────────────

describe('OperationBatchPayloadSchema (Phase 7.4 Request-Driven)', () => {
  const validRequestUuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

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
    requestId: validRequestUuid,
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    originPeerId: 'peer-author-1',
    fromSequence: 1,
    toSequence: 2,
    membershipEpoch: 1,
    operations: [createTestOp(1), createTestOp(2)],
  };

  it('should accept a valid request-driven batch with correlated UUID v4 requestId', () => {
    const result = OperationBatchPayloadSchema.safeParse(validBatch);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.requestId).toBe(validRequestUuid);
      expect(result.data.sessionId).toBe('session-room-1');
      expect(result.data.operations.length).toBe(2);
    }
  });

  it('should reject request-driven batch missing mandatory requestId', () => {
    const { requestId, ...withoutId } = validBatch;
    const result = OperationBatchPayloadSchema.safeParse(withoutId);
    expect(result.success).toBe(false);
  });

  it('should reject request-driven batch with non-UUID v4 requestId', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      requestId: 'non-uuid-string',
    });
    expect(result.success).toBe(false);
  });

  it('should reject request-driven batch missing mandatory sessionId', () => {
    const { sessionId, ...withoutSession } = validBatch;
    const result = OperationBatchPayloadSchema.safeParse(withoutSession);
    expect(result.success).toBe(false);
  });

  it('should reject request-driven batch with empty sessionId', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      sessionId: '',
    });
    expect(result.success).toBe(false);
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

  it('should reject batch with missing middle sequence', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 3,
      operations: [createTestOp(1), createTestOp(3)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain('Operation count (2) does not match declared sequence range 1..3 (expected 3)');
    }
  });

  it('should reject batch with duplicate sequence numbers', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 2,
      operations: [createTestOp(1), createTestOp(1)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toMatch(/Last operation originSequenceNumber \(1\) does not match toSequence \(2\)|strictly contiguous/);
    }
  });

  it('should reject batch with out-of-order sequence numbers', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 2,
      operations: [createTestOp(2), createTestOp(1)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toMatch(/First operation originSequenceNumber \(2\) does not match fromSequence \(1\)|strictly contiguous/);
    }
  });

  it('should reject batch with an operation belonging to a different originPeerId', () => {
    const mixedOps = [
      createTestOp(1),
      {
        ...createTestOp(2),
        originPeerId: 'peer-impostor-99',
      },
    ];
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      operations: mixedOps,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain("does not match batch originPeerId 'peer-author-1'");
    }
  });

  it('should reject batch with an operation belonging to a different membershipEpoch', () => {
    const mixedEpochOps = [
      createTestOp(1),
      {
        ...createTestOp(2),
        membershipEpoch: 2,
      },
    ];
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      membershipEpoch: 1,
      operations: mixedEpochOps,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain('does not match batch membershipEpoch 1');
    }
  });

  it('should reject batch missing declared membershipEpoch', () => {
    const { membershipEpoch, ...withoutEpoch } = validBatch;
    const result = OperationBatchPayloadSchema.safeParse(withoutEpoch);
    expect(result.success).toBe(false);
  });

  it('should reject batch where fromSequence does not match the first operation', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 2,
      toSequence: 2,
      operations: [createTestOp(1)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain('First operation originSequenceNumber (1) does not match fromSequence (2)');
    }
  });

  it('should reject batch where toSequence does not match the last operation', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 2,
      operations: [createTestOp(1), createTestOp(3)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toMatch(/Last operation originSequenceNumber \(3\) does not match toSequence \(2\)|strictly contiguous/);
    }
  });

  it('should reject batch where operation count does not match toSequence - fromSequence + 1', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 5,
      operations: [createTestOp(1), createTestOp(2)],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain('Operation count (2) does not match declared sequence range 1..5 (expected 5)');
    }
  });

  it('should reject empty batch with explicit REPAIR_REJECTED guidance', () => {
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 1,
      operations: [],
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const messages = result.error.issues.map((i) => i.message).join('; ');
      expect(messages).toContain(
        'Empty operation batches are prohibited. Suppliers unable to satisfy requests must send REPAIR_REJECTED',
      );
    }
  });

  it('should accept valid partial contiguous batch (e.g. 5 of 10 requested ops)', () => {
    const partialOps = Array.from({ length: 5 }, (_, i) => createTestOp(i + 1));
    const result = OperationBatchPayloadSchema.safeParse({
      ...validBatch,
      fromSequence: 1,
      toSequence: 5,
      membershipEpoch: 1,
      operations: partialOps,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.fromSequence).toBe(1);
      expect(result.data.toSequence).toBe(5);
      expect(result.data.operations.length).toBe(5);
    }
  });
});

// ── COMPATIBILITY OPERATION BATCH SCHEMA ──────────────────────

describe('CompatibilityOperationBatchPayloadSchema (Phase 7.3 Fixture Isolation)', () => {
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

  it('should validate Phase 7.3 compatibility batch without requestId or sessionId', () => {
    const legacyBatch = {
      supplierPeerId: 'peer-supplier-1',
      originPeerId: 'peer-author-1',
      fromSequence: 1,
      toSequence: 1,
      operations: [createTestOp(1)],
    };
    const result = CompatibilityOperationBatchPayloadSchema.safeParse(legacyBatch);
    expect(result.success).toBe(true);
  });

  it('should validate Phase 7.3 compatibility batch with optional requestId and sessionId', () => {
    const legacyBatchWithIds = {
      requestId: 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
      sessionId: 'session-room-1',
      supplierPeerId: 'peer-supplier-1',
      originPeerId: 'peer-author-1',
      fromSequence: 1,
      toSequence: 1,
      operations: [createTestOp(1)],
    };
    const result = CompatibilityOperationBatchPayloadSchema.safeParse(legacyBatchWithIds);
    expect(result.success).toBe(true);
  });
});

// ── REPAIR_REJECTED (Phase 7.4) ───────────────────────────────

describe('RepairRejectedPayloadSchema (Phase 7.4)', () => {
  const validRequestUuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

  const baseRejection = {
    requestId: validRequestUuid,
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
      expect(result.data.requestId).toBe(validRequestUuid);
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

  it('should reject non-UUID v4 requestId in rejection', () => {
    const result = RepairRejectedPayloadSchema.safeParse({
      ...baseRejection,
      requestId: 'non-uuid-string',
      reason: 'RATE_LIMITED',
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

  it('should preserve identical requestId when echoing rejection', () => {
    const originalRequestId = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';
    const rejection = RepairRejectedPayloadSchema.parse({
      ...baseRejection,
      requestId: originalRequestId,
      reason: 'OUTSIDE_HORIZON',
      retainedHorizonSequence: 100,
    });
    expect(rejection.requestId).toBe(originalRequestId);
  });
});
