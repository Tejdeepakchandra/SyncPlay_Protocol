import { describe, it, expect, beforeEach } from 'vitest';
import crypto from 'node:crypto';
import {
  SnapshotSerializer,
  SerializedSnapshotArtifact,
  compareCheckpointsDeterministic,
  StrictStateSnapshotPayloadV2Schema,
} from '../recovery/snapshot-serializer.js';
import {
  computeSnapshotDigestV2,
  exportEngineSnapshotV2,
} from '../snapshot.js';
import { SyncEngineError } from '../errors.js';
import { MAX_SNAPSHOT_BYTES, type StateSnapshotPayloadV2 } from '@syncplay/protocol';
import { sha256 } from '../log/operation-fingerprint.js';

describe('Phase 7.5 M4.2a — SnapshotSerializer & SerializedSnapshotArtifact', () => {
  const sampleState = {
    status: 'playing' as const,
    anchorPositionMs: 12500,
    playbackRate: 1.0,
    lastAppliedEvent: {
      logicalEventId: 'evt-100',
      senderPeerId: 'peer-author',
      messageType: 'PLAYBACK_PLAY',
    },
  };

  const sampleContext = {
    sessionId: 'session-alpha',
    membershipEpoch: 3,
    authorPeerId: 'peer-author',
    snapshotFrontier: { 'peer-a': 10, 'peer-b': 20 },
    originFrontierTimestamps: { 'peer-a': 1000, 'peer-b': 2000 },
    compactedCheckpoints: [
      { originPeerId: 'peer-b', sequenceNumber: 15, logicalTimestamp: 1500 },
      { originPeerId: 'peer-a', sequenceNumber: 5, logicalTimestamp: 500 },
    ],
    snapshotId: 'snap-fixed-test-1',
  };

  // ─────────────────────────────────────────────────────────────
  // AC-M4-SER-1: Deterministic RFC 8785 JSON Canonicalization
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-SER-1: produces byte-for-byte identical output regardless of key order or checkpoint order', () => {
    const exported = exportEngineSnapshotV2(
      sampleState,
      sampleContext.sessionId,
      sampleContext.membershipEpoch,
      sampleContext.authorPeerId,
      sampleContext.snapshotFrontier,
      sampleContext.originFrontierTimestamps,
      sampleContext.compactedCheckpoints,
      sampleContext.snapshotId
    );

    // Permute top-level keys
    const permuted1: StateSnapshotPayloadV2 = {
      state: exported.state,
      authorPeerId: exported.authorPeerId,
      snapshotId: exported.snapshotId,
      sessionId: exported.sessionId,
      formatVersion: exported.formatVersion,
      originFrontierTimestamps: { 'peer-b': 2000, 'peer-a': 1000 },
      membershipEpoch: exported.membershipEpoch,
      compactedCheckpoints: [
        { originPeerId: 'peer-b', sequenceNumber: 15, logicalTimestamp: 1500 },
        { originPeerId: 'peer-a', sequenceNumber: 5, logicalTimestamp: 500 },
      ],
      snapshotFrontier: { 'peer-b': 20, 'peer-a': 10 },
      maxLogicalTimestamp: exported.maxLogicalTimestamp,
      sha256Digest: exported.sha256Digest,
    };

    // Permute checkpoint order and state object key order
    const permuted2: StateSnapshotPayloadV2 = {
      snapshotId: exported.snapshotId,
      formatVersion: exported.formatVersion,
      membershipEpoch: exported.membershipEpoch,
      sessionId: exported.sessionId,
      authorPeerId: exported.authorPeerId,
      maxLogicalTimestamp: exported.maxLogicalTimestamp,
      snapshotFrontier: { 'peer-a': 10, 'peer-b': 20 },
      originFrontierTimestamps: { 'peer-a': 1000, 'peer-b': 2000 },
      compactedCheckpoints: [
        { originPeerId: 'peer-a', sequenceNumber: 5, logicalTimestamp: 500 },
        { originPeerId: 'peer-b', sequenceNumber: 15, logicalTimestamp: 1500 },
      ],
      state: {
        anchorPositionMs: 12500,
        sessionId: 'session-alpha',
        status: 'playing',
        playbackRate: 1.0,
        formatVersion: 1,
        lastAppliedOperation: {
          messageType: 'PLAYBACK_PLAY',
          senderPeerId: 'peer-author',
          logicalEventId: 'evt-100',
        },
      },
      sha256Digest: exported.sha256Digest,
    };

    const artifact1 = SnapshotSerializer.serialize(permuted1);
    const artifact2 = SnapshotSerializer.serialize(permuted2);

    const bytes1 = artifact1.cloneBytes();
    const bytes2 = artifact2.cloneBytes();

    expect(bytes1.byteLength).toBe(bytes2.byteLength);
    expect(bytes1).toEqual(bytes2);
    expect(artifact1.sha256Digest).toBe(artifact2.sha256Digest);
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-SER-2: Bit-Exact Overall SHA-256 Digest
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-SER-2: sha256Digest equals sha256(artifact.cloneBytes()) and matches computeSnapshotDigestV2()', () => {
    const exported = exportEngineSnapshotV2(
      sampleState,
      sampleContext.sessionId,
      sampleContext.membershipEpoch,
      sampleContext.authorPeerId,
      sampleContext.snapshotFrontier,
      sampleContext.originFrontierTimestamps,
      sampleContext.compactedCheckpoints,
      sampleContext.snapshotId
    );

    const artifact = SnapshotSerializer.serialize(exported);
    const expectedDigest = computeSnapshotDigestV2(exported);

    expect(artifact.sha256Digest).toBe(expectedDigest);
    expect(artifact.sha256Digest).toBe(sha256(artifact.cloneBytes()).toLowerCase());
    expect(artifact.sha256Digest).toMatch(/^[0-9a-f]{64}$/);

    // Independent Node crypto oracle check
    const nodeHash = crypto.createHash('sha256').update(artifact.cloneBytes()).digest('hex');
    expect(artifact.sha256Digest).toBe(nodeHash);
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F1: Strict Structural Validation on Every Entry Point
  // ─────────────────────────────────────────────────────────────
  describe('Finding F1 — Pre-Serialization Strict Structural Validation', () => {
    let validSnapshot: StateSnapshotPayloadV2;

    beforeEach(() => {
      validSnapshot = exportEngineSnapshotV2(
        sampleState,
        sampleContext.sessionId,
        sampleContext.membershipEpoch,
        sampleContext.authorPeerId,
        sampleContext.snapshotFrontier,
        sampleContext.originFrontierTimestamps,
        sampleContext.compactedCheckpoints,
        sampleContext.snapshotId
      );
    });

    it('rejects null or non-object snapshots', () => {
      expect(() => SnapshotSerializer.serialize(null as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(undefined as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize('string' as any)).toThrow(SyncEngineError);
    });

    it('rejects formatVersion !== 2', () => {
      const invalid = { ...validSnapshot, formatVersion: 1 };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/Pre-serialization validation failed/);
    });

    it('rejects oversized snapshotId (> 128 characters)', () => {
      const invalid = { ...validSnapshot, snapshotId: 'a'.repeat(129) };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotId/);
    });

    it('rejects non-ASCII identifier in snapshotId', () => {
      const invalid = { ...validSnapshot, snapshotId: 'snap-id-with-spaces and symbols!' };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotId/);
    });

    it('rejects oversized sessionId (> 128 characters)', () => {
      const invalid = { ...validSnapshot, sessionId: 's'.repeat(129) };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/sessionId/);
    });

    it('rejects oversized authorPeerId (> 64 characters)', () => {
      const invalid = { ...validSnapshot, authorPeerId: 'p'.repeat(65) };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/authorPeerId/);
    });

    it('rejects snapshotFrontier with > 64 entries', () => {
      const bloatedFrontier: Record<string, number> = {};
      for (let i = 0; i < 65; i++) {
        bloatedFrontier[`peer-${i}`] = i;
      }
      const invalid = { ...validSnapshot, snapshotFrontier: bloatedFrontier };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotFrontier key count exceeds 64/);
    });

    it('rejects snapshotFrontier with oversized key (> 64 characters)', () => {
      const invalid = {
        ...validSnapshot,
        snapshotFrontier: { ['k'.repeat(65)]: 1 },
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotFrontier/);
    });

    it('rejects snapshotFrontier with non-safe-integer value', () => {
      const invalid = {
        ...validSnapshot,
        snapshotFrontier: { 'peer-a': Number.MAX_SAFE_INTEGER + 10 },
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotFrontier/);
    });

    it('rejects originFrontierTimestamps with > 64 entries', () => {
      const bloatedTimestamps: Record<string, number> = {};
      for (let i = 0; i < 65; i++) {
        bloatedTimestamps[`peer-${i}`] = 1000 + i;
      }
      const invalid = { ...validSnapshot, originFrontierTimestamps: bloatedTimestamps };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/originFrontierTimestamps key count exceeds 64/);
    });

    it('rejects compactedCheckpoints with > 256 entries', () => {
      const bloatedCheckpoints = Array.from({ length: 257 }, (_, i) => ({
        originPeerId: `peer-${i % 10}`,
        sequenceNumber: i + 1,
        logicalTimestamp: i * 10,
      }));
      const invalid = { ...validSnapshot, compactedCheckpoints: bloatedCheckpoints };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/compactedCheckpoints/);
    });

    it('rejects compactedCheckpoints with malformed item (sequenceNumber <= 0 or non-safe)', () => {
      const invalid1 = {
        ...validSnapshot,
        compactedCheckpoints: [{ originPeerId: 'peer-a', sequenceNumber: 0, logicalTimestamp: 100 }],
      };
      expect(() => SnapshotSerializer.serialize(invalid1 as any)).toThrow(SyncEngineError);

      const invalid2 = {
        ...validSnapshot,
        compactedCheckpoints: [{ originPeerId: 'peer-a', sequenceNumber: Number.MAX_SAFE_INTEGER + 5, logicalTimestamp: 100 }],
      };
      expect(() => SnapshotSerializer.serialize(invalid2 as any)).toThrow(SyncEngineError);
    });

    it('rejects compactedCheckpoints item with unexpected extra properties', () => {
      const invalid = {
        ...validSnapshot,
        compactedCheckpoints: [
          { originPeerId: 'peer-a', sequenceNumber: 1, logicalTimestamp: 100, unexpectedField: 'bad' },
        ],
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/unexpectedField|Pre-serialization validation failed/);
    });

    it('rejects state with unexpected additional properties (strictness)', () => {
      const invalid = {
        ...validSnapshot,
        state: {
          ...validSnapshot.state,
          extraMaliciousPayload: 'x'.repeat(1000),
        },
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/extraMaliciousPayload|Pre-serialization validation failed/);
    });

    it('rejects state with non-finite numeric values (NaN, Infinity)', () => {
      const invalid1 = {
        ...validSnapshot,
        state: { ...validSnapshot.state, anchorPositionMs: NaN },
      };
      expect(() => SnapshotSerializer.serialize(invalid1 as any)).toThrow(SyncEngineError);

      const invalid2 = {
        ...validSnapshot,
        state: { ...validSnapshot.state, playbackRate: Infinity },
      };
      expect(() => SnapshotSerializer.serialize(invalid2 as any)).toThrow(SyncEngineError);
    });

    it('rejects state with negative anchorPositionMs or non-positive playbackRate', () => {
      const invalid1 = {
        ...validSnapshot,
        state: { ...validSnapshot.state, anchorPositionMs: -1 },
      };
      expect(() => SnapshotSerializer.serialize(invalid1 as any)).toThrow(SyncEngineError);

      const invalid2 = {
        ...validSnapshot,
        state: { ...validSnapshot.state, playbackRate: 0 },
      };
      expect(() => SnapshotSerializer.serialize(invalid2 as any)).toThrow(SyncEngineError);
    });

    it('rejects snapshot with unexpected top-level additional properties', () => {
      const invalid = {
        ...validSnapshot,
        unexpectedTopLevelKey: 'malicious',
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/unexpectedTopLevelKey|Pre-serialization validation failed/);
    });

    it('rejects sessionId mismatch between snapshot header and state', () => {
      const invalid = {
        ...validSnapshot,
        sessionId: 'session-header-differs',
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/sessionId mismatch/);
    });

    it('rejects maxLogicalTimestamp mismatch with originFrontierTimestamps', () => {
      const invalid = {
        ...validSnapshot,
        maxLogicalTimestamp: 99999, // origin max is 2000
      };
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
      expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/maxLogicalTimestamp/);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F4: Locale-Independent Checkpoint Ordering & JCS Vectors
  // ─────────────────────────────────────────────────────────────
  describe('Finding F4 — Locale-Independent Deterministic Ordering & JCS', () => {
    it('compareCheckpointsDeterministic sorts by UTF-16 code units independently of locale', () => {
      const c1 = { originPeerId: 'peer-a', sequenceNumber: 10, logicalTimestamp: 100 };
      const c2 = { originPeerId: 'peer-b', sequenceNumber: 5, logicalTimestamp: 100 };
      const c3 = { originPeerId: 'peer-a', sequenceNumber: 20, logicalTimestamp: 200 };

      // peer-a seq 10 should precede peer-a seq 20
      expect(compareCheckpointsDeterministic(c1, c3)).toBeLessThan(0);
      expect(compareCheckpointsDeterministic(c3, c1)).toBeGreaterThan(0);

      // peer-a should precede peer-b regardless of sequence number
      expect(compareCheckpointsDeterministic(c1, c2)).toBeLessThan(0);
      expect(compareCheckpointsDeterministic(c2, c1)).toBeGreaterThan(0);

      // Same peer and sequence number returns 0
      expect(compareCheckpointsDeterministic(c1, { ...c1 })).toBe(0);
    });

    it('RFC 8785 test vectors: canonicalizes numbers, escaping, and negative zero deterministically', () => {
      const snapshot = exportEngineSnapshotV2(
        sampleState,
        sampleContext.sessionId,
        sampleContext.membershipEpoch,
        sampleContext.authorPeerId,
        sampleContext.snapshotFrontier,
        sampleContext.originFrontierTimestamps,
        sampleContext.compactedCheckpoints,
        sampleContext.snapshotId
      );

      // Test zero canonicalization: anchorPositionMs = 0 (and -0)
      const zeroStateSnapshot: StateSnapshotPayloadV2 = {
        ...snapshot,
        state: {
          ...snapshot.state,
          anchorPositionMs: -0, // RFC 8785 canonicalizes -0 to 0
        },
      };

      const artifact = SnapshotSerializer.serialize(zeroStateSnapshot);
      const jsonString = new TextDecoder().decode(artifact.cloneBytes());
      expect(jsonString).toContain('"anchorPositionMs":0');
      expect(jsonString).not.toContain('"anchorPositionMs":-0');

      // Verify node crypto independent digest
      const nodeHash = crypto.createHash('sha256').update(artifact.cloneBytes()).digest('hex');
      expect(artifact.sha256Digest).toBe(nodeHash);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-OWN-1 & AC-M4-OWN-2: Private Buffer Encapsulation & Range Encoding
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-OWN-1: private backing storage exposes zero mutable typed-array properties or views', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);

    const keys = Object.keys(artifact);
    expect(keys).not.toContain('masterBuffer');
    expect(keys).not.toContain('#masterBuffer');
    expect(keys).not.toContain('masterBytes');
    expect(keys).not.toContain('#masterBytes');

    for (const key of keys) {
      const val = (artifact as any)[key];
      expect(typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean').toBe(true);
    }
  });

  it('AC-M4-OWN-2: encodeSliceToBase64 and computeSliceSha256 return immutable primitive strings', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
    const total = artifact.totalSizeBytes;
    expect(total).toBeGreaterThan(50);

    const sliceOffset = 10;
    const sliceLen = 30;

    const base64Str = artifact.encodeSliceToBase64(sliceOffset, sliceLen);
    expect(typeof base64Str).toBe('string');

    const fullBytes = artifact.cloneBytes();
    const expectedSlice = fullBytes.subarray(sliceOffset, sliceOffset + sliceLen);
    const decodedBytes = Buffer.from(base64Str, 'base64');
    expect(new Uint8Array(decodedBytes)).toEqual(expectedSlice);

    const sliceHash = artifact.computeSliceSha256(sliceOffset, sliceLen);
    expect(typeof sliceHash).toBe('string');
    expect(sliceHash).toMatch(/^[0-9a-f]{64}$/);
    expect(sliceHash).toBe(sha256(expectedSlice).toLowerCase());
  });

  it('AC-M4-OWN-2: range methods reject invalid ranges with INVALID_RANGE', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
    const total = artifact.totalSizeBytes;

    expect(() => artifact.encodeSliceToBase64(-1, 10)).toThrow(SyncEngineError);
    expect(() => artifact.encodeSliceToBase64(0, -5)).toThrow(SyncEngineError);
    expect(() => artifact.encodeSliceToBase64(total - 5, 10)).toThrow(SyncEngineError);
    expect(() => artifact.encodeSliceToBase64(0.5, 10)).toThrow(SyncEngineError);

    expect(() => artifact.computeSliceSha256(-1, 10)).toThrow(SyncEngineError);
    expect(() => artifact.copyByteRange(total, 1)).toThrow(SyncEngineError);
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-OWN-3: Unaliased Public Copies
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-OWN-3: cloneBytes and copyByteRange return unaliased deep copies', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
    const clone1 = artifact.cloneBytes();
    const originalByte = clone1[0];

    // Mutate clone1
    clone1[0] = (originalByte! ^ 0xff);

    // clone2 should still have the original byte
    const clone2 = artifact.cloneBytes();
    expect(clone2[0]).toBe(originalByte);

    // copyByteRange should also be unaliased
    const range1 = artifact.copyByteRange(0, 10);
    range1[0] = (originalByte! ^ 0xff);
    const range2 = artifact.copyByteRange(0, 10);
    expect(range2[0]).toBe(originalByte);
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-LSE-1, LSE-2, LSE-3: Lease Lifecycle & Deferred Disposal
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-LSE-1: transfer lease acquisition, duplicate prevention, and idempotent release', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
    expect(artifact.activeLeaseCount).toBe(0);

    artifact.acquireLease('transfer-1');
    expect(artifact.activeLeaseCount).toBe(1);

    // Duplicate lease acquisition throws INVALID_STATE
    expect(() => artifact.acquireLease('transfer-1')).toThrow(SyncEngineError);
    expect(() => artifact.acquireLease('transfer-1')).toThrow(/Transfer lease already acquired/);

    // Another lease succeeds
    artifact.acquireLease('transfer-2');
    expect(artifact.activeLeaseCount).toBe(2);

    // Release transfer-1
    expect(artifact.releaseLease('transfer-1')).toBe(true);
    expect(artifact.activeLeaseCount).toBe(1);

    // Duplicate release is idempotent and returns false
    expect(artifact.releaseLease('transfer-1')).toBe(false);
    expect(artifact.activeLeaseCount).toBe(1);

    expect(artifact.releaseLease('transfer-2')).toBe(true);
    expect(artifact.activeLeaseCount).toBe(0);
  });

  it('AC-M4-LSE-2: deferred disposal retains buffer until all active leases are released', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);

    artifact.acquireLease('transfer-A');
    artifact.acquireLease('transfer-B');
    expect(artifact.activeLeaseCount).toBe(2);

    // Request disposal
    artifact.dispose();
    expect(artifact.isDisposalRequested).toBe(true);
    expect(artifact.isDisposed).toBe(false);

    // New leases cannot be acquired once disposal is requested
    expect(() => artifact.acquireLease('transfer-C')).toThrow(SyncEngineError);
    expect(() => artifact.acquireLease('transfer-C')).toThrow(/Cannot acquire lease/);

    // Reading data still works while leased
    expect(artifact.encodeSliceToBase64(0, 10)).toBeTruthy();
    expect(artifact.cloneBytes().byteLength).toBe(artifact.totalSizeBytes);

    // Release first lease: still deferred
    artifact.releaseLease('transfer-A');
    expect(artifact.isDisposed).toBe(false);
    expect(artifact.activeLeaseCount).toBe(1);

    // Release second (last) lease: finalizes disposal immediately
    artifact.releaseLease('transfer-B');
    expect(artifact.isDisposed).toBe(true);
    expect(artifact.activeLeaseCount).toBe(0);
  });

  it('AC-M4-LSE-3: read and clone operations throw ARTIFACT_DISPOSED after disposal', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);

    // Immediate disposal when 0 leases
    artifact.dispose();
    expect(artifact.isDisposed).toBe(true);

    expect(() => artifact.encodeSliceToBase64(0, 10)).toThrow(SyncEngineError);
    expect(() => artifact.encodeSliceToBase64(0, 10)).toThrow(/ARTIFACT_DISPOSED/);

    expect(() => artifact.computeSliceSha256(0, 10)).toThrow(SyncEngineError);
    expect(() => artifact.copyByteRange(0, 10)).toThrow(SyncEngineError);
    expect(() => artifact.cloneBytes()).toThrow(SyncEngineError);
    expect(() => artifact.acquireLease('new-transfer')).toThrow(SyncEngineError);
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F1: Direct SerializedSnapshotArtifact Instantiation Validation
  // ─────────────────────────────────────────────────────────────
  describe('Finding F1 — Direct SerializedSnapshotArtifact Public Creation Path', () => {
    let validBuffer: ArrayBuffer;
    let validDigest: string;
    let validParams: {
      snapshotId: string;
      sessionId: string;
      membershipEpoch: number;
      authorPeerId: string;
      totalSizeBytes: number;
      sha256Digest: string;
      masterBuffer: ArrayBuffer;
    };

    beforeEach(() => {
      const art = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
      validBuffer = art.cloneBytes().buffer as ArrayBuffer;
      validDigest = art.sha256Digest;
      validParams = {
        snapshotId: art.snapshotId,
        sessionId: art.sessionId,
        membershipEpoch: art.membershipEpoch,
        authorPeerId: art.authorPeerId,
        totalSizeBytes: art.totalSizeBytes,
        sha256Digest: validDigest,
        masterBuffer: validBuffer,
      };
    });

    it('succeeds when directly instantiating with valid buffer, valid metadata, and valid snapshot payload', () => {
      const direct = new SerializedSnapshotArtifact(validParams);
      expect(direct).toBeInstanceOf(SerializedSnapshotArtifact);
      expect(direct.snapshotId).toBe(validParams.snapshotId);
      expect(direct.sessionId).toBe(validParams.sessionId);
      expect(direct.authorPeerId).toBe(validParams.authorPeerId);
      expect(direct.membershipEpoch).toBe(validParams.membershipEpoch);
      expect(direct.totalSizeBytes).toBe(validParams.totalSizeBytes);
      expect(direct.sha256Digest).toBe(validDigest);
      expect(direct.isDisposed).toBe(false);
      expect(direct.cloneBytes().byteLength).toBe(validParams.totalSizeBytes);
    });

    it('rejects direct instantiation with non-object params or non-ArrayBuffer', () => {
      expect(() => new SerializedSnapshotArtifact(null as any)).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, masterBuffer: 'not-buffer' as any })).toThrow(
        SyncEngineError
      );
    });

    it('rejects direct instantiation with invalid snapshotId (empty, non-ASCII, or > 128 chars)', () => {
      expect(() => new SerializedSnapshotArtifact({ ...validParams, snapshotId: '' })).toThrow(SyncEngineError);
      expect(
        () => new SerializedSnapshotArtifact({ ...validParams, snapshotId: 'invalid id with spaces' })
      ).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, snapshotId: 'a'.repeat(129) })).toThrow(
        SyncEngineError
      );
    });

    it('rejects direct instantiation with invalid sessionId (empty, non-ASCII, or > 128 chars)', () => {
      expect(() => new SerializedSnapshotArtifact({ ...validParams, sessionId: '' })).toThrow(SyncEngineError);
      expect(
        () => new SerializedSnapshotArtifact({ ...validParams, sessionId: 'invalid session id!' })
      ).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, sessionId: 's'.repeat(129) })).toThrow(
        SyncEngineError
      );
    });

    it('rejects direct instantiation with invalid authorPeerId (empty, non-ASCII, or > 64 chars)', () => {
      expect(() => new SerializedSnapshotArtifact({ ...validParams, authorPeerId: '' })).toThrow(SyncEngineError);
      expect(
        () => new SerializedSnapshotArtifact({ ...validParams, authorPeerId: 'peer with spaces!' })
      ).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, authorPeerId: 'p'.repeat(65) })).toThrow(
        SyncEngineError
      );
    });

    it('rejects direct instantiation with invalid membershipEpoch (negative or non-integer)', () => {
      expect(() => new SerializedSnapshotArtifact({ ...validParams, membershipEpoch: -1 })).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, membershipEpoch: 1.5 })).toThrow(SyncEngineError);
    });

    it('rejects direct instantiation with invalid sha256Digest (non-hex or wrong length)', () => {
      expect(() => new SerializedSnapshotArtifact({ ...validParams, sha256Digest: 'invalid-hex' })).toThrow(
        SyncEngineError
      );
      expect(() => new SerializedSnapshotArtifact({ ...validParams, sha256Digest: 'a'.repeat(63) })).toThrow(
        SyncEngineError
      );
      expect(() => new SerializedSnapshotArtifact({ ...validParams, sha256Digest: 'a'.repeat(65) })).toThrow(
        SyncEngineError
      );
    });

    it('rejects direct instantiation with totalSizeBytes mismatch with masterBuffer.byteLength', () => {
      expect(
        () => new SerializedSnapshotArtifact({ ...validParams, totalSizeBytes: validParams.totalSizeBytes + 1 })
      ).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact({ ...validParams, totalSizeBytes: 0 })).toThrow(SyncEngineError);
      expect(
        () => new SerializedSnapshotArtifact({ ...validParams, totalSizeBytes: MAX_SNAPSHOT_BYTES + 1 })
      ).toThrow(SyncEngineError);
    });

    it('rejects direct instantiation with masterBuffer containing non-JSON raw bytes', () => {
      const corruptBytes = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe]);
      const corruptDigest = sha256(corruptBytes);
      expect(
        () =>
          new SerializedSnapshotArtifact({
            ...validParams,
            masterBuffer: corruptBytes.buffer as ArrayBuffer,
            totalSizeBytes: corruptBytes.byteLength,
            sha256Digest: corruptDigest,
          })
      ).toThrow(SyncEngineError);
    });

    it('rejects direct instantiation with masterBuffer containing invalid snapshot payload (bypassing validateSnapshot)', () => {
      const badSnapshot = {
        formatVersion: 1, // Invalid formatVersion
        snapshotId: validParams.snapshotId,
        sessionId: validParams.sessionId,
        authorPeerId: validParams.authorPeerId,
        membershipEpoch: validParams.membershipEpoch,
      };
      const badBytes = new TextEncoder().encode(JSON.stringify(badSnapshot));
      const badDigest = sha256(badBytes);
      expect(
        () =>
          new SerializedSnapshotArtifact({
            ...validParams,
            masterBuffer: badBytes.buffer as ArrayBuffer,
            totalSizeBytes: badBytes.byteLength,
            sha256Digest: badDigest,
          })
      ).toThrow(SyncEngineError);
    });

    it('rejects direct instantiation with masterBuffer metadata mismatch (e.g. authorPeerId mismatch)', () => {
      const mismatchedParams = {
        ...validParams,
        authorPeerId: 'peer-different-author',
      };
      expect(() => new SerializedSnapshotArtifact(mismatchedParams)).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact(mismatchedParams)).toThrow(/authorPeerId/);
    });

    it('rejects direct instantiation when declared sha256Digest does not match masterBuffer', () => {
      const mismatchedDigestParams = {
        ...validParams,
        sha256Digest: '0'.repeat(64),
      };
      expect(() => new SerializedSnapshotArtifact(mismatchedDigestParams)).toThrow(SyncEngineError);
      expect(() => new SerializedSnapshotArtifact(mismatchedDigestParams)).toThrow(/sha256Digest mismatch/);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F2: Disposal Listener Exception Isolation & Order
  // ─────────────────────────────────────────────────────────────
  describe('Finding F2 — Disposal Listener Exception Isolation', () => {
    it('executes all registered listeners in order even if one throws', () => {
      const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
      const executionOrder: number[] = [];

      artifact.registerDisposalListener(() => {
        executionOrder.push(1);
      });
      artifact.registerDisposalListener(() => {
        executionOrder.push(2);
        throw new Error('Listener 2 catastrophic error');
      });
      artifact.registerDisposalListener(() => {
        executionOrder.push(3);
      });

      // Dispose
      expect(() => artifact.dispose()).not.toThrow();
      expect(executionOrder).toEqual([1, 2, 3]);
      expect(artifact.isDisposed).toBe(true);

      // Registering on already disposed artifact runs immediately with exception isolation
      expect(() => {
        artifact.registerDisposalListener(() => {
          executionOrder.push(4);
          throw new Error('Post-disposal listener failure');
        });
      }).not.toThrow();
      expect(executionOrder).toEqual([1, 2, 3, 4]);
    });
  });
});
