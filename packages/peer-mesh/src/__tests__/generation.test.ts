/**
 * Connection generation validation tests.
 *
 * Per user requirement §6 and DECISIONS.md D10:
 * "Every signaling message must include the current connection generation.
 *  Validate received senderGeneration against the expected peer generation
 *  and discard stale offers, answers, and ICE candidates."
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { PeerManager } from '../peer-manager.js';
import { SignalingClient, type SignalEvent } from '../signaling-client.js';
import { installWebRTCMocks, removeWebRTCMocks } from './webrtc-mocks.js';

// ── Helpers ──────────────────────────────────────────────────

type SignalHandler = (event: SignalEvent) => void;

interface MockSignalingWithHandlers extends SignalingClient {
  _offerHandlers: SignalHandler[];
  _answerHandlers: SignalHandler[];
  _iceHandlers: SignalHandler[];
  _peerLeftHandlers: Array<(e: { roomId: string; peerId: string }) => void>;
  _peerDisconnectedHandlers: Array<(e: { roomId: string; peerId: string }) => void>;
}

function createMockSignalingWithCapture(): MockSignalingWithHandlers {
  const client = new SignalingClient() as MockSignalingWithHandlers;
  client._offerHandlers = [];
  client._answerHandlers = [];
  client._iceHandlers = [];
  client._peerLeftHandlers = [];
  client._peerDisconnectedHandlers = [];

  client.sendOffer = vi.fn();
  client.sendAnswer = vi.fn();
  client.sendIceCandidate = vi.fn();

  // Capture registered handlers so tests can simulate incoming signals
  client.onSignalOffer = vi.fn((handler: SignalHandler) => {
    client._offerHandlers.push(handler);
    return () => {
      const idx = client._offerHandlers.indexOf(handler);
      if (idx >= 0) client._offerHandlers.splice(idx, 1);
    };
  });
  client.onSignalAnswer = vi.fn((handler: SignalHandler) => {
    client._answerHandlers.push(handler);
    return () => {
      const idx = client._answerHandlers.indexOf(handler);
      if (idx >= 0) client._answerHandlers.splice(idx, 1);
    };
  });
  client.onSignalIceCandidate = vi.fn((handler: SignalHandler) => {
    client._iceHandlers.push(handler);
    return () => {
      const idx = client._iceHandlers.indexOf(handler);
      if (idx >= 0) client._iceHandlers.splice(idx, 1);
    };
  });
  client.onPeerJoined = vi.fn(() => () => {});
  client.onPeerLeft = vi.fn((handler) => {
    client._peerLeftHandlers.push(handler as (e: { roomId: string; peerId: string }) => void);
    return () => {};
  });
  client.onPeerDisconnected = vi.fn((handler) => {
    client._peerDisconnectedHandlers.push(handler as (e: { roomId: string; peerId: string }) => void);
    return () => {};
  });
  client.dispose = vi.fn();

  return client;
}

function makeOfferEvent(fromPeerId: string, generation: number): SignalEvent {
  return {
    roomId: 'room-1',
    fromPeerId,
    senderGeneration: generation,
    negotiationId: `neg-${generation}`,
    payload: {
      type: 'offer',
      sdp: `v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`,
    },
  };
}

function makeAnswerEvent(fromPeerId: string, generation: number): SignalEvent {
  return {
    roomId: 'room-1',
    fromPeerId,
    senderGeneration: generation,
    negotiationId: `neg-${generation}`,
    payload: {
      type: 'answer',
      sdp: `v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`,
    },
  };
}

function makeIceCandidateEvent(fromPeerId: string, generation: number): SignalEvent {
  return {
    roomId: 'room-1',
    fromPeerId,
    senderGeneration: generation,
    negotiationId: `neg-${generation}`,
    payload: {
      candidate: 'candidate:1 1 udp 2122260223 192.168.1.1 12345 typ host',
      sdpMid: '0',
      sdpMLineIndex: 0,
    },
  };
}

describe('generation validation', () => {
  beforeEach(() => {
    installWebRTCMocks();
  });

  afterEach(() => {
    removeWebRTCMocks();
  });

  describe('offer generation validation', () => {
    it('should accept offer with matching generation', async () => {
      const signaling = createMockSignalingWithCapture();
      // 'zzz' > 'aaa', so 'zzz' is NOT the initial offerer — will receive offers
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Simulate incoming offer from 'aaa' with generation 1
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 1));
      }

      // Should process the offer and send an answer
      // Give async operations time to settle
      await new Promise((r) => setTimeout(r, 50));
      expect(signaling.sendAnswer).toHaveBeenCalled();
    });

    it('should discard offer with stale generation', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // First, establish connection with generation 2
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 2 });

      vi.mocked(signaling.sendAnswer).mockClear();

      // Now receive an offer with stale generation 1
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 1));
      }

      await new Promise((r) => setTimeout(r, 50));
      // Should NOT process the stale offer
      expect(signaling.sendAnswer).not.toHaveBeenCalled();
    });

    it('should accept offer with newer generation (peer reconnected)', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Establish connection with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });
      vi.mocked(signaling.sendAnswer).mockClear();

      // Now receive an offer with newer generation 2
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 2));
      }

      await new Promise((r) => setTimeout(r, 50));
      // Should process the offer (peer reconnected with new generation)
      expect(signaling.sendAnswer).toHaveBeenCalled();
    });
  });

  describe('answer generation validation', () => {
    it('should discard answer with stale generation', async () => {
      const signaling = createMockSignalingWithCapture();
      // 'aaa' < 'zzz', so 'aaa' is the initial offerer
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Connect with generation 2
      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 2 });

      // Simulate stale answer with generation 1
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('zzz', 1));
      }

      // Should silently discard — peer status should not change to CONNECTED
      await new Promise((r) => setTimeout(r, 50));
      const status = pm.getPeerStatus('zzz');
      expect(status).toBeDefined();
      // Should not be in a corrupted state
      expect(status!.generation).toBe(2);
    });

    it('should discard answer with newer generation (remote peer reconnected)', async () => {
      const signaling = createMockSignalingWithCapture();
      // 'aaa' < 'zzz', so 'aaa' is the initial offerer — will send offers
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Connect with generation 1 — sends offer to 'zzz'
      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      expect(signaling.sendOffer).toHaveBeenCalled();

      // Remote peer reconnected with generation 2 — but we haven't received
      // the new offer yet. An answer with gen 2 arrives for our gen-1 offer.
      // This answer does not belong to any negotiation with the new incarnation.
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('zzz', 2));
      }

      await new Promise((r) => setTimeout(r, 50));
      const status = pm.getPeerStatus('zzz');
      expect(status).toBeDefined();
      // Entry should still be at generation 1 — not bumped by the newer answer
      expect(status!.generation).toBe(1);
    });

    it('should accept answer with matching generation', async () => {
      const signaling = createMockSignalingWithCapture();
      // 'aaa' < 'zzz', so 'aaa' is the initial offerer
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Connect with generation 1 — sends offer to 'zzz'
      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      // Send matching-generation answer — should be applied without error
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('zzz', 1));
      }

      await new Promise((r) => setTimeout(r, 50));
      const status = pm.getPeerStatus('zzz');
      expect(status).toBeDefined();
      expect(status!.generation).toBe(1);
      // The peer entry should still exist (not removed by an error)
      expect(pm.listPeers()).toHaveLength(1);
    });
  });

  describe('ICE candidate generation validation', () => {
    it('should discard ICE candidate with stale generation', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Establish with generation 2
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 2 });

      // Send stale ICE candidate with generation 1
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 1));
      }

      // Should silently discard — no error, no crash
      await new Promise((r) => setTimeout(r, 50));
      const status = pm.getPeerStatus('aaa');
      expect(status).toBeDefined();
    });

    it('should accept ICE candidate with matching generation', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Establish with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });

      // Send ICE candidate with matching generation
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 1));
      }

      // Should accept — no error
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')).toBeDefined();
    });

    it('should discard ICE candidate with newer generation (remote peer reconnected)', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Establish with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });

      // ICE candidate arrives with newer generation 2 — belongs to a
      // negotiation we haven't started yet. Must be discarded.
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 2));
      }

      await new Promise((r) => setTimeout(r, 50));
      const status = pm.getPeerStatus('aaa');
      expect(status).toBeDefined();
      // Entry should remain at generation 1 — not bumped
      expect(status!.generation).toBe(1);
    });
  });

  describe('self-signal rejection', () => {
    it('should ignore signals from self', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Simulate offer from self (should never happen, but must be safe)
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 1));
      }

      await new Promise((r) => setTimeout(r, 50));
      expect(pm.listPeers()).toHaveLength(0);
    });
  });

  describe('wrong room rejection', () => {
    it('should ignore signals for different room', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Simulate offer for wrong room
      for (const handler of signaling._offerHandlers) {
        handler({
          ...makeOfferEvent('aaa', 1),
          roomId: 'wrong-room',
        });
      }

      await new Promise((r) => setTimeout(r, 50));
      expect(signaling.sendAnswer).not.toHaveBeenCalled();
    });
  });

  describe('peer left cleanup', () => {
    it('should clean up transport when peer leaves', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });
      expect(pm.listPeers()).toHaveLength(1);

      // Simulate peer left
      for (const handler of signaling._peerLeftHandlers) {
        handler({ roomId: 'room-1', peerId: 'zzz' });
      }

      expect(pm.listPeers()).toHaveLength(0);
    });
  });

  describe('newer-generation signals for unknown peer', () => {
    it('should not create entry for answer from unknown peer', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // No connect() call — 'zzz' is unknown to PeerManager.
      // A newer-gen answer arrives. handleSignalAnswer does peers.get()
      // which returns undefined, so it returns early without creating an entry.
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('zzz', 5));
      }

      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('zzz')).toBeUndefined();
      expect(pm.listPeers()).toHaveLength(0);
    });

    it('should not create entry for ICE candidate from unknown peer', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('aaa', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // No connect() call — 'zzz' is unknown.
      // A newer-gen ICE candidate arrives. handleSignalIceCandidate does
      // peers.get() which returns undefined, so it returns early.
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('zzz', 5));
      }

      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('zzz')).toBeUndefined();
      expect(pm.listPeers()).toHaveLength(0);
    });
  });

  describe('transport lifecycle during reconnection', () => {
    it('should recreate transport on newer-gen offer, then reject stale answer', async () => {
      const signaling = createMockSignalingWithCapture();
      // 'zzz' > 'aaa', so 'zzz' is NOT the initial offerer — receives offers
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Step 1: establish connection with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });
      expect(pm.getPeerStatus('aaa')!.generation).toBe(1);

      // Step 2: receive newer-gen offer (gen 2) — transport should be recreated
      vi.mocked(signaling.sendAnswer).mockClear();
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));

      // Should have answered the gen-2 offer (transport was recreated)
      expect(signaling.sendAnswer).toHaveBeenCalled();
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Step 3: stale gen-1 answer arrives — must be discarded
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('aaa', 1));
      }
      await new Promise((r) => setTimeout(r, 50));
      // Entry should remain at generation 2
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);
    });

    it('should recreate transport on newer-gen offer, then reject stale ICE', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Step 1: establish connection with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });

      // Step 2: receive newer-gen offer (gen 2)
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Step 3: stale gen-1 ICE candidate — must be discarded
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 1));
      }
      await new Promise((r) => setTimeout(r, 50));
      // Entry should remain at generation 2, no error
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);
      expect(pm.listPeers()).toHaveLength(1);
    });

    it('should accept gen-2 ICE after gen-2 offer recreates transport', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Step 1: establish connection with generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });

      // Step 2: receive newer-gen offer (gen 2) — transport recreated
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Step 3: matching gen-2 ICE candidate — should be accepted
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));
      // Peer should still exist (no error from applying candidate)
      expect(pm.getPeerStatus('aaa')).toBeDefined();
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);
    });

    it('full reconnection lifecycle: gen1 → gen2 offer → discard gen1 signals → accept gen2 signals', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 1, signaling);
      pm.bindToRoom('room-1');

      // Phase 1: initial connection at generation 1
      await pm.connect({ peerId: 'aaa', sessionId: 'session-1', generation: 1 });
      expect(pm.getPeerStatus('aaa')!.generation).toBe(1);

      // Phase 2: peer reconnects — gen-2 offer arrives, transport is recreated
      vi.mocked(signaling.sendAnswer).mockClear();
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(signaling.sendAnswer).toHaveBeenCalledTimes(1);
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Phase 3: stale gen-1 answer arrives — discarded
      for (const handler of signaling._answerHandlers) {
        handler(makeAnswerEvent('aaa', 1));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Phase 4: stale gen-1 ICE arrives — discarded
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 1));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);

      // Phase 5: matching gen-2 ICE arrives — accepted
      for (const handler of signaling._iceHandlers) {
        handler(makeIceCandidateEvent('aaa', 2));
      }
      await new Promise((r) => setTimeout(r, 50));
      expect(pm.getPeerStatus('aaa')).toBeDefined();
      expect(pm.getPeerStatus('aaa')!.generation).toBe(2);
      expect(pm.listPeers()).toHaveLength(1);
    });
  });

  describe('local generation in outgoing signals', () => {
    it('should include local generation in sent offers', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('aaa', 'session-1', 3, signaling);
      pm.bindToRoom('room-1');

      await pm.connect({ peerId: 'zzz', sessionId: 'session-1', generation: 1 });

      expect(signaling.sendOffer).toHaveBeenCalledWith(
        'room-1',
        'zzz',
        expect.any(String),
        expect.any(String),
        3, // local generation
      );
    });

    it('should include local generation in sent answers', async () => {
      const signaling = createMockSignalingWithCapture();
      const pm = new PeerManager('zzz', 'session-1', 5, signaling);
      pm.bindToRoom('room-1');

      // Simulate incoming offer
      for (const handler of signaling._offerHandlers) {
        handler(makeOfferEvent('aaa', 1));
      }

      await new Promise((r) => setTimeout(r, 50));

      expect(signaling.sendAnswer).toHaveBeenCalledWith(
        'room-1',
        'aaa',
        expect.any(String),
        expect.any(String),
        5, // local generation
      );
    });
  });
});

