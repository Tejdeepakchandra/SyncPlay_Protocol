/**
 * In-memory loopback integration test connecting:
 * Peer Mesh + ControlEventService + BaselineSyncEngine across two peers.
 */

import { describe, it, expect, vi } from 'vitest';
import { ControlEventServiceImpl } from '@syncplay/control-events';
import type { PeerMesh, InboundMessage, PeerStatus, SendResult } from '@syncplay/peer-mesh';
import { BaselineSyncEngine } from '../baseline-sync-engine.js';

function createLoopbackMeshPair(peerAId: string, peerBId: string) {
  let aMessageHandlers: ((msg: InboundMessage) => void)[] = [];
  let bMessageHandlers: ((msg: InboundMessage) => void)[] = [];

  const aPeerStatus: PeerStatus = {
    peerId: peerBId,
    state: 'CONNECTED',
    dataChannelState: 'open',
    generation: 1,
    lastUpdatedAt: Date.now(),
  };

  const bPeerStatus: PeerStatus = {
    peerId: peerAId,
    state: 'CONNECTED',
    dataChannelState: 'open',
    generation: 1,
    lastUpdatedAt: Date.now(),
  };

  const meshA: PeerMesh = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn(),
    send: vi.fn((targetPeerId, message) => {
      if (targetPeerId === peerBId) {
        for (const handler of bMessageHandlers) {
          handler({ fromPeerId: peerAId, message });
        }
        return { ok: true } as SendResult;
      }
      return { ok: false, error: 'Peer not found' } as SendResult;
    }),
    getPeerStatus: vi.fn((id) => (id === peerBId ? aPeerStatus : undefined)),
    listPeers: vi.fn(() => [aPeerStatus]),
    onMessage: vi.fn((h) => {
      aMessageHandlers.push(h);
      return () => { aMessageHandlers = aMessageHandlers.filter((fn) => fn !== h); };
    }),
    onPeerStatus: vi.fn(() => () => {}),
    dispose: vi.fn().mockResolvedValue(undefined),
  };

  const meshB: PeerMesh = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn(),
    send: vi.fn((targetPeerId, message) => {
      if (targetPeerId === peerAId) {
        for (const handler of aMessageHandlers) {
          handler({ fromPeerId: peerBId, message });
        }
        return { ok: true } as SendResult;
      }
      return { ok: false, error: 'Peer not found' } as SendResult;
    }),
    getPeerStatus: vi.fn((id) => (id === peerAId ? bPeerStatus : undefined)),
    listPeers: vi.fn(() => [bPeerStatus]),
    onMessage: vi.fn((h) => {
      bMessageHandlers.push(h);
      return () => { bMessageHandlers = bMessageHandlers.filter((fn) => fn !== h); };
    }),
    onPeerStatus: vi.fn(() => () => {}),
    dispose: vi.fn().mockResolvedValue(undefined),
  };

  return { meshA, meshB };
}

describe('Control Events + SyncEngine Full Integration', () => {
  it('synchronizes control operations and updates both engines in full loopback', async () => {
    let mockTime = 1000;
    const clock = { now: () => mockTime };

    const { meshA, meshB } = createLoopbackMeshPair('peer-alice', 'peer-bob');

    const controlServiceA = new ControlEventServiceImpl(meshA, {
      sessionId: 'room-101',
      localPeerId: 'peer-alice',
      clock,
    });

    const controlServiceB = new ControlEventServiceImpl(meshB, {
      sessionId: 'room-101',
      localPeerId: 'peer-bob',
      clock,
    });

    const engineA = new BaselineSyncEngine({
      sessionId: 'room-101',
      localPeerId: 'peer-alice',
      clock,
    });

    const engineB = new BaselineSyncEngine({
      sessionId: 'room-101',
      localPeerId: 'peer-bob',
      clock,
    });

    // Connect ControlEventService to BaselineSyncEngine via SyncEventReceiver
    controlServiceA.setReceiver(engineA);
    controlServiceB.setReceiver(engineB);

    const controlsA = controlServiceA.getControls();
    const controlsB = controlServiceB.getControls();

    // 1. Peer A plays from 5000ms
    const playResult = await controlsA.play(5000);
    expect(playResult.accepted).toBe(true);
    expect(playResult.deliveredCount).toBe(1);

    expect(engineA.getState().status).toBe('playing');
    expect(engineA.getState().anchorPositionMs).toBe(5000);

    expect(engineB.getState().status).toBe('playing');
    expect(engineB.getState().anchorPositionMs).toBe(5000);
    expect(engineB.getState().lastAppliedEvent?.senderPeerId).toBe('peer-alice');

    // 2. Peer B seeks to 25000ms
    mockTime = 2000;
    const seekResult = await controlsB.seek(25000);
    expect(seekResult.accepted).toBe(true);
    expect(seekResult.deliveredCount).toBe(1);

    expect(engineA.getState().anchorPositionMs).toBe(25000);
    expect(engineA.getState().status).toBe('playing');
    expect(engineB.getState().anchorPositionMs).toBe(25000);
    expect(engineB.getState().status).toBe('playing');

    // 3. Peer A sets rate to 2.0
    mockTime = 3000;
    const rateResult = await controlsA.setPlaybackRate(2.0);
    expect(rateResult.accepted).toBe(true);

    expect(engineA.getState().playbackRate).toBe(2.0);
    expect(engineB.getState().playbackRate).toBe(2.0);

    // 4. Peer B pauses
    mockTime = 4000;
    const pauseResult = await controlsB.pause();
    expect(pauseResult.accepted).toBe(true);

    expect(engineA.getState().status).toBe('paused');
    expect(engineB.getState().status).toBe('paused');

    // 5. Semantic rejection prevents broadcast
    // Engine A configured with stricter minPlaybackRate (0.5).
    // controlsA.setPlaybackRate(0.3) passes ControlEventService check (>= 0.25),
    // but fails Engine A semantic validation (< 0.5), causing Engine A to throw SyncEngineError.
    // ControlEventService catches it, aborts broadcast, and returns ENGINE_ERROR.
    const strictEngineA = new BaselineSyncEngine({
      sessionId: 'room-101',
      localPeerId: 'peer-alice',
      clock,
      minPlaybackRate: 0.5,
    });
    controlServiceA.setReceiver(strictEngineA);

    const badRateResult = await controlsA.setPlaybackRate(0.3);
    expect(badRateResult.accepted).toBe(false);
    expect(badRateResult.error?.code).toBe('ENGINE_ERROR');
    expect(badRateResult.deliveredCount).toBe(0); // Broadcast aborted!

    // State on both engines preserved
    expect(engineA.getState().playbackRate).toBe(2.0);
    expect(engineB.getState().playbackRate).toBe(2.0);

    // 6. Listener exception does not prevent broadcast
    strictEngineA.onStateChange(() => {
      throw new Error('Crashing application listener on Peer A');
    });

    const safePlayResult = await controlsA.play();
    expect(safePlayResult.accepted).toBe(true);
    expect(safePlayResult.deliveredCount).toBe(1);
    expect(strictEngineA.getState().status).toBe('playing');
    expect(engineB.getState().status).toBe('playing');
  });
});
