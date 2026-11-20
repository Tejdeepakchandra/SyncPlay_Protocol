import { describe, it, expect } from 'vitest';
import {
  SnapshotSerializer,
  SerializedSnapshotArtifact,
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
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-SER-3: Pre-Serialization Bounds & 4 MiB Size Ceiling
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-SER-3: rejects snapshotFrontier with > 64 entries', () => {
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

    const bloatedFrontier: Record<string, number> = {};
    for (let i = 0; i < 65; i++) {
      bloatedFrontier[`peer-${i}`] = i;
    }

    const invalid = { ...exported, snapshotFrontier: bloatedFrontier };
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/snapshotFrontier key count exceeds 64/);
  });

  it('AC-M4-SER-3: rejects originFrontierTimestamps with > 64 entries', () => {
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

    const bloatedTimestamps: Record<string, number> = {};
    for (let i = 0; i < 65; i++) {
      bloatedTimestamps[`peer-${i}`] = 1000 + i;
    }

    const invalid = { ...exported, originFrontierTimestamps: bloatedTimestamps };
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/originFrontierTimestamps key count exceeds 64/);
  });

  it('AC-M4-SER-3: rejects compactedCheckpoints with > 256 entries', () => {
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

    const bloatedCheckpoints = Array.from({ length: 257 }, (_, i) => ({
      originPeerId: `peer-${i % 10}`,
      sequenceNumber: i,
      logicalTimestamp: i * 10,
    }));

    const invalid = { ...exported, compactedCheckpoints: bloatedCheckpoints };
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(SyncEngineError);
    expect(() => SnapshotSerializer.serialize(invalid as any)).toThrow(/compactedCheckpoints count exceeds 256/);
  });

  it('AC-M4-SER-3: rejects invalid/non-plain state objects', () => {
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

    const invalid1 = { ...exported, state: null as any };
    expect(() => SnapshotSerializer.serialize(invalid1)).toThrow(SyncEngineError);

    const invalid2 = {
      ...exported,
      state: { ...exported.state, anchorPositionMs: NaN },
    };
    expect(() => SnapshotSerializer.serialize(invalid2 as any)).toThrow(SyncEngineError);
  });

  it('AC-M4-SER-3: rejects payload exceeding MAX_SNAPSHOT_BYTES', () => {
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

    // Create a large snapshot by creating an oversized sessionId or strings
    const hugePayload = {
      ...exported,
      sessionId: 's'.repeat(MAX_SNAPSHOT_BYTES + 1024),
    };

    expect(() => SnapshotSerializer.serialize(hugePayload as any)).toThrow(SyncEngineError);
    expect(() => SnapshotSerializer.serialize(hugePayload as any)).toThrow(/exceeds MAX_SNAPSHOT_BYTES/);
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-OWN-1 & AC-M4-OWN-2: Private Buffer Encapsulation & Range Encoding
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-OWN-1: private backing storage exposes zero mutable typed-array properties or views', () => {
    const artifact = SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);

    // Verify property keys on the instance
    const keys = Object.keys(artifact);
    expect(keys).not.toContain('masterBuffer');
    expect(keys).not.toContain('#masterBuffer');
    expect(keys).not.toContain('masterBytes');
    expect(keys).not.toContain('#masterBytes');

    // Values of all public keys should be primitive strings or numbers
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

    // Verify decoded base64 matches cloned slice bytes
    const fullBytes = artifact.cloneBytes();
    const expectedSlice = fullBytes.subarray(sliceOffset, sliceOffset + sliceLen);
    const decodedBytes = Buffer.from(base64Str, 'base64');
    expect(new Uint8Array(decodedBytes)).toEqual(expectedSlice);

    // Verify computeSliceSha256 matches sha256 of slice
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
});
