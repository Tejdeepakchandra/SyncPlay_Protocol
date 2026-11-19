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

  // ── RTT Service Integration ──────────────────────────────────

  describe('RTT service integration', () => {
    it('should expose RTT service via getRttService()', () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      const rtt = pm.getRttService();
      expect(rtt).toBeDefined();
      expect(typeof rtt.start).toBe('function');
      expect(typeof rtt.stop).toBe('function');
      expect(typeof rtt.getEstimate).toBe('function');
      expect(typeof rtt.onSample).toBe('function');
    });

    it('should handle incoming PING by sending PONG without exposing to onMessage', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      const messages: InboundMessage[] = [];
      pm.onMessage((m) => messages.push(m));

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      // Spy on pm.send to check the PONG response
      const sendSpy = vi.spyOn(pm, 'send');

      // Simulate incoming PING message on DataChannel
      const pingMsg = JSON.stringify({
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PING,
        sessionId: 'session-1',
        senderPeerId: 'zzz',
        messageId: 'ping-1',
        payload: {
          probeId: 'probe-from-zzz-1',
          senderTimestamp: 1000,
        },
      });

      // Retrieve DataChannel from WebRTC mock
      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('zzz');
      expect(entry).toBeDefined();

      // Trigger message via transport
      // @ts-expect-error accessing private transport adapter events
      entry.transport.events.onDataChannelMessage(pingMsg);

      // PING must NOT be forwarded to application onMessage
      expect(messages).toHaveLength(0);

      // PONG must have been sent
      expect(sendSpy).toHaveBeenCalledWith(
        'zzz',
        expect.objectContaining({
          messageType: MessageType.PONG,
          payload: { probeId: 'probe-from-zzz-1' },
        }),
      );
    });

    it('should route incoming PONG to RttEngine without exposing to onMessage', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      const messages: InboundMessage[] = [];
      pm.onMessage((m) => messages.push(m));

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      const rtt = pm.getRttService();
      // @ts-expect-error accessing private rttEngine method for spy
      const pongSpy = vi.spyOn(rtt, 'handlePong');

      const pongMsg = JSON.stringify({
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PONG,
        sessionId: 'session-1',
        senderPeerId: 'zzz',
        messageId: 'pong-1',
        payload: {
          probeId: 'probe-test-1',
        },
      });

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('zzz');
      // @ts-expect-error accessing private transport adapter events
      entry.transport.events.onDataChannelMessage(pongMsg);

      // PONG must NOT be delivered to application onMessage
      expect(messages).toHaveLength(0);

      // PONG must be routed to RttEngine
      expect(pongSpy).toHaveBeenCalledWith('zzz', { probeId: 'probe-test-1' });
    });

    it('should start RTT probing on DataChannel open and stop on close', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 2 });

      const rtt = pm.getRttService();
      const startSpy = vi.spyOn(rtt, 'start');
      const stopSpy = vi.spyOn(rtt, 'stop');

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('zzz');

      // Simulate DataChannel open
      // @ts-expect-error accessing private transport adapter events
      entry.transport.events.onDataChannelOpen();
      expect(startSpy).toHaveBeenCalledWith('zzz', 2);

      // Simulate DataChannel close
      // @ts-expect-error accessing private transport adapter events
      entry.transport.events.onDataChannelClose();
      expect(stopSpy).toHaveBeenCalledWith('zzz');
    });
  });

  // ── M5 Transport Size & Currency Enforcement ─────────────────

  describe('M5 Transport Size & Currency Enforcement', () => {
    it('should return undefined sctpMaxMessageSize before negotiation', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 1 });

      expect(pm.getSctpMaxMessageSize('unknown')).toBeUndefined();
      expect(pm.getSctpMaxMessageSize('peer-b')).toBeUndefined();
    });

    it('should return correct connection generation via getPeerGeneration', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 3 });

      expect(pm.getPeerGeneration('unknown')).toBeUndefined();
      expect(pm.getPeerGeneration('peer-b')).toBe(3);
    });

    it('should reject sendSerialized if generation mismatches', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 2 });

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('peer-b');
      // @ts-expect-error accessing private transport dc
      const dc = entry.transport.dc;
      dc.simulateOpen();

      const result = pm.sendSerialized('peer-b', '{"test":true}', 1); // Stale generation 1 vs 2
      expect(result.ok).toBe(false);
      expect(result.error).toBe('Generation mismatch');
    });

    it('should reject sendSerialized if DataChannel is not open', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 2 });

      const result = pm.sendSerialized('peer-b', '{"test":true}', 2);
      expect(result.ok).toBe(false);
      expect(result.error).toBe('DataChannel not open');
    });

    it('should enforce negotiated SCTP limit in sendSerialized', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 1 });

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('peer-b');
      // @ts-expect-error accessing private transport pc and dc
      entry.transport.pc.sctp = { maxMessageSize: 50 }; // Tiny limit: 50 bytes
      entry.transport.dc.simulateOpen();

      const smallPayload = JSON.stringify({ a: 1 }); // < 50 bytes
      const bigPayload = JSON.stringify({ a: 'x'.repeat(100) }); // > 50 bytes

      const okResult = pm.sendSerialized('peer-b', smallPayload, 1);
      expect(okResult.ok).toBe(true);

      const failResult = pm.sendSerialized('peer-b', bigPayload, 1);
      expect(failResult.ok).toBe(false);
      expect(failResult.error).toContain('Message exceeds maximum size');
    });

    it('should enforce 65,536-byte cap when SCTP limit is 0 (unconstrained remote)', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 1 });

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('peer-b');
      entry.transport.pc.sctp = { maxMessageSize: 0 }; // RFC 8841: 0 = unconstrained remote
      entry.transport.dc.simulateOpen();

      const oversizedString = 'x'.repeat(65537);
      const failResult = pm.sendSerialized('peer-b', oversizedString, 1);
      expect(failResult.ok).toBe(false);
      expect(failResult.error).toContain('Message exceeds maximum size');
    });

    it('should pass exact serialized string directly to DataChannel.send', async () => {
      const signaling = createMockSignaling();
      const pm = new PeerManager('local', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');
      await pm.connect({ peerId: 'peer-b', sessionId: 'session-1', generation: 1 });

      // @ts-expect-error accessing private peers map for test simulation
      const entry = pm.peers.get('peer-b');
      const dc = entry.transport.dc;
      dc.simulateOpen();

      const exactJson = '{"exact":"bytes","unicode":"🎉"}';
      const result = pm.sendSerialized('peer-b', exactJson, 1);
      expect(result.ok).toBe(true);

      const sent = dc.getSentMessages();
      expect(sent).toContain(exactJson);
      expect(sent[sent.length - 1]).toBe(exactJson);
    });
  });
});

