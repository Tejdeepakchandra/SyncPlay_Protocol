import { describe, it, expect } from 'vitest';
import { MessageType } from '@syncplay/protocol';
import type { StateSnapshotPayload } from '@syncplay/protocol';
import { BaselineSyncEngine } from '../baseline-sync-engine.js';
import type { EngineSnapshotData } from '../snapshot.js';

describe('BaselineSyncEngine — Snapshot Export & Import', () => {
  it('exportSnapshot never includes local monotonic timestamp and respects protocol schema', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    // Advance state
    engine.receive({
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload: { targetPositionMs: 5000 },
      },
      localTimestamp: 1000,
      origin: 'local',
    } as any);

    const snapshot = engine.exportSnapshot('custom-snap-1');
    expect(snapshot.snapshotId).toBe('custom-snap-1');
    expect(snapshot.stateVersion).toBe(1);

    const data = snapshot.state as EngineSnapshotData;
    expect(data.formatVersion).toBe(1);
    expect(data.sessionId).toBe('room-1');
    expect(data.status).toBe('playing');
    expect(data.anchorPositionMs).toBe(5000);
    expect(data.playbackRate).toBe(1.0);
    expect(data.lastAppliedOperation?.logicalEventId).toBe('evt-1');

    // Invariant: anchorLocalTimestamp is NEVER exported!
    expect((data as any).anchorLocalTimestamp).toBeUndefined();
  });

  it('importSnapshot sets stateVersion = localVersion + 1 and ignores remote version', () => {
    let mockTimeA = 1000;
    const engineA = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-a',
      clock: { now: () => mockTimeA },
    });

    // engineA is at local version 0
    expect(engineA.getState().stateVersion).toBe(0);

    // Foreign snapshot has remote stateVersion 999
    const foreignSnapshot: StateSnapshotPayload = {
      snapshotId: 'snap-remote-1',
      stateVersion: 999, // Remote peer is at version 999
      state: {
        formatVersion: 1,
        sessionId: 'room-1',
        status: 'playing',
        anchorPositionMs: 45000,
        playbackRate: 1.5,
        lastAppliedOperation: {
          logicalEventId: 'evt-remote-42',
          senderPeerId: 'peer-b',
          messageType: 'PLAY',
        },
      },
    };

    mockTimeA = 2500;
    const decision = engineA.importSnapshot(foreignSnapshot);

    expect(decision.accepted).toBe(true);
    expect(decision.code).toBe('APPLIED');

    const stateA = engineA.getState();
    // Invariant Amendment 1: local stateVersion = currentLocalVersion + 1 = 1 (NOT 1000!)
    expect(stateA.stateVersion).toBe(1);
    expect(decision.stateVersion).toBe(1);

    // Invariant: anchorLocalTimestamp is reset to receiving engine's clock (2500)
    expect(stateA.anchorLocalTimestamp).toBe(2500);
    expect(stateA.anchorPositionMs).toBe(45000);
    expect(stateA.status).toBe('playing');
    expect(stateA.playbackRate).toBe(1.5);

    // Invariant: lastAppliedEvent is marked as untrusted imported provenance
    expect(stateA.lastAppliedEvent?.logicalEventId).toBe('evt-remote-42');
    expect(stateA.lastAppliedEvent?.senderPeerId).toBe('peer-b');
    expect(stateA.lastAppliedEvent?.isImportedProvenance).toBe(true);
  });

  it('rejects snapshots with mismatched sessionId without mutating state', () => {
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-a',
    });

    const badSnapshot: StateSnapshotPayload = {
      snapshotId: 'snap-bad',
      stateVersion: 5,
      state: {
        formatVersion: 1,
        sessionId: 'DIFFERENT-ROOM',
        status: 'paused',
        anchorPositionMs: 10000,
        playbackRate: 1.0,
      },
    };

    const decision = engine.importSnapshot(badSnapshot);
    expect(decision.accepted).toBe(false);
    expect(decision.code).toBe('REJECTED_INVALID');
    expect(engine.getState().stateVersion).toBe(0); // State unchanged
    expect(engine.getState().anchorPositionMs).toBe(0);
  });

  it('rejects snapshots with out-of-bounds playback rate without mutating state', () => {
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-a',
      minPlaybackRate: 0.5,
      maxPlaybackRate: 2.0,
    });

    const badRateSnapshot: StateSnapshotPayload = {
      snapshotId: 'snap-bad-rate',
      stateVersion: 5,
      state: {
        formatVersion: 1,
        sessionId: 'room-1',
        status: 'paused',
        anchorPositionMs: 10000,
        playbackRate: 10.0, // Exceeds max 2.0!
      },
    };

    const decision = engine.importSnapshot(badRateSnapshot);
    expect(decision.accepted).toBe(false);
    expect(decision.code).toBe('REJECTED_INVALID');
    expect(engine.getState().stateVersion).toBe(0);
  });

  it('rejects snapshot import if engine is disposed', () => {
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-a',
    });
    engine.dispose();

    const snapshot: StateSnapshotPayload = {
      snapshotId: 'snap-1',
      stateVersion: 1,
      state: {
        formatVersion: 1,
        sessionId: 'room-1',
        status: 'paused',
        anchorPositionMs: 5000,
        playbackRate: 1.0,
      },
    };

    const decision = engine.importSnapshot(snapshot);
    expect(decision.accepted).toBe(false);
    expect(decision.code).toBe('REJECTED_DISPOSED');
  });
});
