/**
 * PeerManager unit tests.
 *
 * Tests connection lifecycle, signaling order, cleanup, and message exchange.
 * Uses mocked WebRTC (no real browser connections).
 *
 * Real browser verification is done separately via apps/demo/.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { PeerManager } from '../peer-manager.js';
import { SignalingClient } from '../signaling-client.js';
import { installWebRTCMocks, removeWebRTCMocks, MockRTCPeerConnection, MockRTCDataChannel } from './webrtc-mocks.js';
import { PROTOCOL_VERSION, MessageType } from '@syncplay/protocol';
import type { PeerStatus, InboundMessage } from '../types.js';

// ── Helpers ──────────────────────────────────────────────────

function createMockSignaling(): SignalingClient {
  const client = new SignalingClient();
  // Mock all methods that interact with socket.io
  client.sendOffer = vi.fn();
  client.sendAnswer = vi.fn();
  client.sendIceCandidate = vi.fn();
  // Mock event registration to return unsubscribe functions
  client.onSignalOffer = vi.fn(() => () => {});
  client.onSignalAnswer = vi.fn(() => () => {});
  client.onSignalIceCandidate = vi.fn(() => () => {});
  client.onPeerJoined = vi.fn(() => () => {});
  client.onPeerLeft = vi.fn(() => () => {});
  client.onPeerDisconnected = vi.fn(() => () => {});
  client.dispose = vi.fn();
  return client;
}

describe('PeerManager', () => {
  beforeEach(() => {
    installWebRTCMocks();
  });

  afterEach(() => {
    removeWebRTCMocks();
  });

  // ── Construction ─────────────────────────────────────────────

  describe('construction', () => {
    it('should create with no peers', () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      expect(pm.listPeers()).toHaveLength(0);
    });

    it('should return undefined for unknown peer status', () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      expect(pm.getPeerStatus('unknown')).toBeUndefined();
    });
  });

  // ── Connect ──────────────────────────────────────────────────

  describe('connect', () => {
    it('should create a peer entry on connect', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      const status = pm.getPeerStatus('zzz');
      expect(status).toBeDefined();
      expect(status!.peerId).toBe('zzz');
    });

    it('should reject self-connection', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);

      await expect(
        pm.connect({ peerId: 'local', sessionId: 'session-1', generation: 1 }),
      ).rejects.toThrow('Cannot connect to self');
    });

    it('should throw when disposed', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      await pm.dispose();

      await expect(
        pm.connect({ peerId: 'remote', sessionId: 'session-1', generation: 1 }),
      ).rejects.toThrow('PeerManager disposed');
    });

    it('designated offerer should send offer via signaling', async () => {
      const signaling = createMockSignaling();
      // 'aaa' < 'zzz', so 'aaa' is the initial offerer
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      expect(signaling.sendOffer).toHaveBeenCalledOnce();
      expect(signaling.sendOffer).toHaveBeenCalledWith(
        'room-1',
        'zzz',
        expect.any(String), // SDP
        expect.any(String), // negotiationId
        1, // local generation
      );
    });

    it('non-offerer should NOT send offer on connect', async () => {
      const signaling = createMockSignaling();
      // 'zzz' > 'aaa', so 'zzz' is NOT the initial offerer
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });

      expect(signaling.sendOffer).not.toHaveBeenCalled();
    });
  });

  // ── Disconnect ───────────────────────────────────────────────

  describe('disconnect', () => {
    it('should remove peer on explicit disconnect', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      pm.disconnect('zzz');

      expect(pm.getPeerStatus('zzz')).toBeUndefined();
      expect(pm.listPeers()).toHaveLength(0);
    });

    it('should emit CLOSED status on disconnect', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      const statuses: PeerStatus[] = [];
      pm.onPeerStatus((s) => statuses.push(s));

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      pm.disconnect('zzz');

      const closed = statuses.find((s) => s.state === 'CLOSED');
      expect(closed).toBeDefined();
      expect(closed!.peerId).toBe('zzz');
    });

    it('should be safe to disconnect unknown peer', () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      // Should not throw
      pm.disconnect('nonexistent');
    });
  });

  // ── Send ─────────────────────────────────────────────────────

  describe('send', () => {
    it('should fail for unknown peer', () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);

      const result = pm.send('unknown', {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PING,
        sessionId: 'session-1',
        senderPeerId: 'local',
        messageId: 'msg-1',
        payload: { probeId: 'p1' },
      });

      expect(result.ok).toBe(false);
      expect(result.error).toBe('Peer not found');
    });
  });

  // ── Dispose ──────────────────────────────────────────────────

  describe('dispose', () => {
    it('should close all connections', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'bbb', sessionId: 'session-1', generation: 1 });
      await pm.connect({ peerId: 'ccc', sessionId: 'session-1', generation: 1 });

      expect(pm.listPeers()).toHaveLength(2);

      await pm.dispose();

      expect(pm.listPeers()).toHaveLength(0);
    });

    it('should be idempotent', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      await pm.dispose();
      await pm.dispose(); // should not throw
    });
  });

  // ── Peer list ────────────────────────────────────────────────

  describe('listPeers', () => {
    it('should return all connected peers', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'bbb', sessionId: 'session-1', generation: 1 });
      await pm.connect({ peerId: 'ccc', sessionId: 'session-1', generation: 2 });

      const peers = pm.listPeers();
      expect(peers).toHaveLength(2);
      expect(peers.map((p) => p.peerId).sort()).toEqual(['bbb', 'ccc']);
    });
  });

  // ── Status callbacks ─────────────────────────────────────────

  describe('onPeerStatus', () => {
    it('should emit status changes', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      const statuses: PeerStatus[] = [];
      pm.onPeerStatus((s) => statuses.push(s));

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      // Should have at least SIGNALING status
      expect(statuses.length).toBeGreaterThan(0);
      expect(statuses.some((s) => s.state === 'SIGNALING')).toBe(true);
    });

    it('should support unsubscribe', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      const statuses: PeerStatus[] = [];
      const unsub = pm.onPeerStatus((s) => statuses.push(s));
      unsub();

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      expect(statuses).toHaveLength(0);
    });
  });

  // ── Reconnect ────────────────────────────────────────────────

  describe('reconnect', () => {
    it('should replace existing connection on reconnect', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      // Reconnect with new generation
      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 2 });

      const peers = pm.listPeers();
      expect(peers).toHaveLength(1);
      expect(peers[0]!.peerId).toBe('zzz');
      expect(peers[0]!.generation).toBe(2);
    });
  });
});

