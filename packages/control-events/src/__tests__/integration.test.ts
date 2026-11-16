import { describe, it, expect, vi } from 'vitest';
import { ControlEventServiceImpl } from '../control-event-service.js';
import type {
  SyncEventReceiver,
  AnyControlSyncEvent,
  Clock,
  ControlEventServiceConfig,
} from '../types.js';
import type { PeerMesh, InboundMessage, PeerStatus, SendResult } from '@syncplay/peer-mesh';

/**
 * Creates a two-peer in-memory mesh loopback connecting Peer A and Peer B directly.
 */
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
        // Direct loopback delivery to Peer B's message handlers
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
        // Direct loopback delivery to Peer A's message handlers
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

describe('Control Events Loopback Integration', () => {
  const clock: Clock = { now: () => performance.now() };

  it('exchanges PLAY, SEEK, and RATE_CHANGE between Peer A and Peer B with deduplication', async () => {
    const { meshA, meshB } = createLoopbackMeshPair('peer-alice', 'peer-bob');

    const configA: ControlEventServiceConfig = {
      sessionId: 'test-session-1',
      localPeerId: 'peer-alice',
      clock,
    };
    const configB: ControlEventServiceConfig = {
      sessionId: 'test-session-1',
      localPeerId: 'peer-bob',
      clock,
    };

    const serviceA = new ControlEventServiceImpl(meshA, configA);
    const serviceB = new ControlEventServiceImpl(meshB, configB);

    const receivedAtA: AnyControlSyncEvent[] = [];
    const receivedAtB: AnyControlSyncEvent[] = [];

    serviceA.setReceiver({ receive: (evt) => receivedAtA.push(evt) });
    serviceB.setReceiver({ receive: (evt) => receivedAtB.push(evt) });

    // 1. Peer A calls play(1000)
    const playRes = await serviceA.getControls().play(1000);
    expect(playRes.accepted).toBe(true);
    expect(playRes.peerCount).toBe(1);
    expect(playRes.deliveredCount).toBe(1);

    // Peer A receives local event
    expect(receivedAtA.length).toBe(1);
    expect(receivedAtA[0].origin).toBe('local');
    expect(receivedAtA[0].message.messageType).toBe('PLAY');
    expect(receivedAtA[0].message.payload).toEqual({ targetPositionMs: 1000 });

    // Peer B receives remote event over mesh
    expect(receivedAtB.length).toBe(1);
    expect(receivedAtB[0].origin).toBe('remote');
    expect(receivedAtB[0].message.senderPeerId).toBe('peer-alice');
    expect(receivedAtB[0].message.messageType).toBe('PLAY');
    expect(receivedAtB[0].message.payload).toEqual({ targetPositionMs: 1000 });

    // 2. Peer B calls seek(45000)
    const seekRes = await serviceB.getControls().seek(45000);
    expect(seekRes.accepted).toBe(true);

    // Peer B receives local event
    expect(receivedAtB.length).toBe(2);
    expect(receivedAtB[1].origin).toBe('local');
    expect(receivedAtB[1].message.messageType).toBe('SEEK');
    expect(receivedAtB[1].message.payload).toEqual({ targetPositionMs: 45000 });

    // Peer A receives remote seek from Peer B
    expect(receivedAtA.length).toBe(2);
    expect(receivedAtA[1].origin).toBe('remote');
    expect(receivedAtA[1].message.senderPeerId).toBe('peer-bob');
    expect(receivedAtA[1].message.messageType).toBe('SEEK');
    expect(receivedAtA[1].message.payload).toEqual({ targetPositionMs: 45000 });

    // 3. Peer A retries the play command
    const retryRes = await serviceA.getControls().retry(receivedAtA[0]);
    expect(retryRes.accepted).toBe(true);

    // Peer A did NOT redispatch locally
    expect(receivedAtA.length).toBe(2);

    // Peer B received retry over wire, but deduplication suppressed it!
    expect(receivedAtB.length).toBe(2);
    expect(serviceB.getStats().deduplication.duplicateCount).toBe(1);

    // 4. Peer B changes rate to 1.5
    const rateRes = await serviceB.getControls().setPlaybackRate(1.5);
    expect(rateRes.accepted).toBe(true);

    expect(receivedAtB.length).toBe(3);
    expect(receivedAtA.length).toBe(3);
    expect(receivedAtA[2].message.messageType).toBe('RATE_CHANGE');
    expect(receivedAtA[2].message.payload).toEqual({ playbackRate: 1.5 });

    // Teardown
    serviceA.dispose();
    serviceB.dispose();

    expect(serviceA.isDisposed).toBe(true);
    expect(serviceB.isDisposed).toBe(true);
  });
});

