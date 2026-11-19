import { describe, it, expect } from 'vitest';
import {
  exportEngineSnapshotV2,
  validateSnapshotPayloadV2,
  computeSnapshotDigestV2,
} from '../snapshot.js';
import type { PlaybackState } from '../types.js';
import { SyncEngineError } from '../errors.js';
import { MAX_SNAPSHOT_BYTES } from '@syncplay/protocol';
import { canonicalizeJson } from '../log/operation-fingerprint.js';

describe('Phase 7.5 M3 — StateSnapshotPayloadV2 & Canonical Serialization', () => {
  const basePlaybackState: PlaybackState = {
    status: 'playing',
    anchorPositionMs: 42000,
    anchorLocalTimestamp: 999999, // Should NOT leak into exported snapshot
    playbackRate: 1.25,
    stateVersion: 10,
    lastAppliedEvent: {
      logicalEventId: 'evt-42',
      senderPeerId: 'peer-alpha',
      messageType: 'PLAY',
      appliedAtLocalTimestamp: 999999,
    },
  };

  it('exports valid StateSnapshotPayloadV2 with formatVersion 2 and stripped monotonic timestamps', () => {
    const frontier = { 'peer-alpha': 5, 'peer-beta': 8 };
    const originTimestamps = { 'peer-alpha': 105, 'peer-beta': 208 };
    const checkpoints = [
      { originPeerId: 'peer-alpha', sequenceNumber: 1, logicalTimestamp: 101 },
      { originPeerId: 'peer-alpha', sequenceNumber: 2, logicalTimestamp: 102 },
    ];

    const snapshot = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      3,
      'peer-alpha',
      frontier,
      originTimestamps,
      checkpoints,
      'snap-001',
    );

    expect(snapshot.formatVersion).toBe(2);
    expect(snapshot.snapshotId).toBe('snap-001');
    expect(snapshot.sessionId).toBe('session-xyz');
    expect(snapshot.membershipEpoch).toBe(3);
    expect(snapshot.authorPeerId).toBe('peer-alpha');
    expect(snapshot.snapshotFrontier).toEqual(frontier);
    expect(snapshot.originFrontierTimestamps).toEqual(originTimestamps);
    expect(snapshot.maxLogicalTimestamp).toBe(208); // max(105, 208)
    expect(snapshot.compactedCheckpoints).toHaveLength(2);
    expect(snapshot.sha256Digest).toMatch(/^[0-9a-f]{64}$/);

    // Verify monotonic timestamp is NOT leaked
    const stateData = snapshot.state as any;
    expect(stateData.formatVersion).toBe(1);
    expect(stateData.status).toBe('playing');
    expect(stateData.anchorPositionMs).toBe(42000);
    expect(stateData.playbackRate).toBe(1.25);
    expect(stateData.anchorLocalTimestamp).toBeUndefined();
    expect(stateData.lastAppliedOperation).toEqual({
      logicalEventId: 'evt-42',
      senderPeerId: 'peer-alpha',
      messageType: 'PLAY',
    });
  });

  it('produces bit-exact canonical SHA-256 digest regardless of compacted checkpoints insertion order', () => {
    const frontier = { 'peer-alpha': 5, 'peer-beta': 8 };
    const originTimestamps = { 'peer-alpha': 105, 'peer-beta': 208 };

    const cp1 = [
      { originPeerId: 'peer-beta', sequenceNumber: 3, logicalTimestamp: 203 },
      { originPeerId: 'peer-alpha', sequenceNumber: 1, logicalTimestamp: 101 },
      { originPeerId: 'peer-alpha', sequenceNumber: 2, logicalTimestamp: 102 },
    ];

    const cp2 = [
      { originPeerId: 'peer-alpha', sequenceNumber: 2, logicalTimestamp: 102 },
      { originPeerId: 'peer-alpha', sequenceNumber: 1, logicalTimestamp: 101 },
      { originPeerId: 'peer-beta', sequenceNumber: 3, logicalTimestamp: 203 },
    ];

    const snap1 = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      3,
      'peer-alpha',
      frontier,
      originTimestamps,
      cp1,
      'snap-fixed-id',
    );

    const snap2 = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      3,
      'peer-alpha',
      frontier,
      originTimestamps,
      cp2,
      'snap-fixed-id',
    );

    expect(snap1.sha256Digest).toBe(snap2.sha256Digest);
    expect(snap1.compactedCheckpoints).toEqual(snap2.compactedCheckpoints);
  });

  it('RFC 8785 canonicalization ensures object key insertion order independence', () => {
    const objA = { z: 1, a: 2, m: { b: 3, a: 4 } };
    const objB = { a: 2, m: { a: 4, b: 3 }, z: 1 };

    expect(canonicalizeJson(objA)).toBe(canonicalizeJson(objB));
    expect(canonicalizeJson(objA)).toBe('{"a":2,"m":{"a":4,"b":3},"z":1}');
  });

  it('enforces exact Phase 7.2 Lamport invariant: maxLogicalTimestamp === max(originFrontierTimestamps)', () => {
    // Non-empty timestamps
    const snap = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 1, 'peer-b': 2 },
      { 'peer-a': 50, 'peer-b': 120 },
      [],
    );
    expect(snap.maxLogicalTimestamp).toBe(120);

    // Empty timestamps
    const snapEmpty = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      {},
      {},
      [],
    );
    expect(snapEmpty.maxLogicalTimestamp).toBe(0);
  });

  it('validates snapshot successfully with validateSnapshotPayloadV2', () => {
    const snapshot = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 3 },
      { 'peer-a': 45 },
      [],
    );

    const validated = validateSnapshotPayloadV2(snapshot, 'session-xyz', 0.5, 2.0);
    expect(validated.snapshotId).toBe(snapshot.snapshotId);
    expect(validated.sessionId).toBe('session-xyz');
    expect(validated.membershipEpoch).toBe(1);
    expect(validated.authorPeerId).toBe('peer-a');
    expect(validated.maxLogicalTimestamp).toBe(45);
    expect(validated.state.playbackRate).toBe(1.25);
  });

  it('rejects snapshot when sessionId does not match expected session', () => {
    const snapshot = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 3 },
      { 'peer-a': 45 },
      [],
    );

    expect(() => validateSnapshotPayloadV2(snapshot, 'different-session')).toThrow(
      SyncEngineError,
    );
    expect(() => validateSnapshotPayloadV2(snapshot, 'different-session')).toThrow(
      /sessionId .* does not match active session/,
    );
  });

  it('rejects snapshot when maxLogicalTimestamp does not match originFrontierTimestamps maximum', () => {
    const snapshot = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 3 },
      { 'peer-a': 45 },
      [],
    );

    const tampered = {
      ...snapshot,
      maxLogicalTimestamp: 999, // Tampered value
    };
    // Recompute digest for tampered to test maxLogicalTimestamp validator specifically
    const tamperedWithDigest = {
      ...tampered,
      sha256Digest: computeSnapshotDigestV2(tampered),
    };

    expect(() => validateSnapshotPayloadV2(tamperedWithDigest, 'session-xyz')).toThrow(
      SyncEngineError,
    );
    expect(() => validateSnapshotPayloadV2(tamperedWithDigest, 'session-xyz')).toThrow(
      /does not match originFrontierTimestamps maximum/,
    );
  });

  it('rejects snapshot when sha256Digest is tampered or does not match', () => {
    const snapshot = exportEngineSnapshotV2(
      basePlaybackState,
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 3 },
      { 'peer-a': 45 },
      [],
    );

    const tampered = {
      ...snapshot,
      sha256Digest: '0'.repeat(64), // Invalid digest
    };

    expect(() => validateSnapshotPayloadV2(tampered, 'session-xyz')).toThrow(
      SyncEngineError,
    );
    expect(() => validateSnapshotPayloadV2(tampered, 'session-xyz')).toThrow(
      /Snapshot SHA-256 digest mismatch/,
    );
  });

  it('rejects snapshot when playbackRate is outside permitted range', () => {
    const snapshot = exportEngineSnapshotV2(
      basePlaybackState, // playbackRate is 1.25
      'session-xyz',
      1,
      'peer-a',
      { 'peer-a': 3 },
      { 'peer-a': 45 },
      [],
    );

    // permitted: [0.5, 1.0] -> 1.25 violates upper bound
    expect(() => validateSnapshotPayloadV2(snapshot, 'session-xyz', 0.5, 1.0)).toThrow(
      SyncEngineError,
    );
    expect(() => validateSnapshotPayloadV2(snapshot, 'session-xyz', 0.5, 1.0)).toThrow(
      /playbackRate 1.25 outside permitted range/,
    );
  });

  it('rejects snapshot exceeding MAX_SNAPSHOT_BYTES', () => {
    const giantState: PlaybackState = {
      ...basePlaybackState,
    };
    // Create large dummy string
    const hugeString = 'x'.repeat(MAX_SNAPSHOT_BYTES + 100);

    expect(() =>
      validateSnapshotPayloadV2(hugeString as any, 'session-xyz'),
    ).toThrow(SyncEngineError);
  });
});
