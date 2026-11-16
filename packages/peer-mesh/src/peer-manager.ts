/**
 * PeerManager — main orchestrator for the peer mesh.
 *
 * Implements the PeerMesh interface (INTERFACE_CONTRACTS.md §2).
 * Manages one WebRTCTransport per remote peer.
 *
 * Per PEER_MESH_RTT_ENGINE_LLD §2:
 * "Peer Manager owns connection lifecycle and peer state, not room membership truth."
 *
 * Sender identity is always server-derived. Connection generation is validated
 * on every received signaling message. Stale signals are discarded.
 */
import {
  type PeerId,
  type SessionId,
  type ConnectionGeneration,
  type ProtocolMessage,
  MessageType,
  PROTOCOL_VERSION,
  parseAndValidateMessage,
} from '@syncplay/protocol';

import type {
  PeerMesh,
  PeerDescriptor,
  PeerStatus,
  PeerConnectionState,
  SendResult,
  InboundMessage,
  Unsubscribe,
  PeerMeshConfig,
  RttService,
} from './types.js';
import { DEFAULT_PEER_MESH_CONFIG } from './types.js';
import { WebRTCTransport, type TransportEvents } from './webrtc-transport.js';
import { SignalingClient, type SignalEvent } from './signaling-client.js';
import { isInitialOfferer } from './offerer-policy.js';
import { RttEngine } from './rtt/index.js';

// ── Internal peer entry ──────────────────────────────────────

interface PeerEntry {
  peerId: PeerId;
  generation: ConnectionGeneration;
  transport: WebRTCTransport;
  negotiationId: string;
  disconnectTimer: ReturnType<typeof setTimeout> | null;
}

// ── PeerManager ──────────────────────────────────────────────

export class PeerManager implements PeerMesh {
  private readonly peers = new Map<PeerId, PeerEntry>();
  private readonly messageHandlers = new Set<(m: InboundMessage) => void>();
  private readonly statusHandlers = new Set<(s: PeerStatus) => void>();
  private readonly signaling: SignalingClient;
  private readonly config: PeerMeshConfig;
  private readonly localPeerId: PeerId;
  private readonly sessionId: SessionId;
  private readonly localGeneration: ConnectionGeneration;
  private readonly rttEngine: RttEngine;
  private roomId: SessionId | null = null;
  private disposed = false;
  private signalingCleanup: Unsubscribe[] = [];

  constructor(
    localPeerId: PeerId,
    sessionId: SessionId,
    localGeneration: ConnectionGeneration,
    signaling: SignalingClient,
    config: Partial<PeerMeshConfig> = {},
    rttEngine?: RttEngine,
  ) {
    this.localPeerId = localPeerId;
    this.sessionId = sessionId;
    this.localGeneration = localGeneration;
    this.signaling = signaling;
    this.config = { ...DEFAULT_PEER_MESH_CONFIG, ...config };
    this.rttEngine = rttEngine ?? new RttEngine(
      this.localPeerId,
      this.sessionId,
      (peerId, message) => this.send(peerId, message),
      this.config.rtt,
    );
  }

  /**
   * Access the RTT measurement service.
   */
  getRttService(): RttService {
    return this.rttEngine;
  }

  /**
   * Bind to the Bootstrap Server signaling events for a room.
   * Must be called after joining the room.
   */
  bindToRoom(roomId: SessionId): void {
    this.roomId = roomId;

    this.signalingCleanup.push(
      this.signaling.onSignalOffer((event) => this.handleSignalOffer(event)),
      this.signaling.onSignalAnswer((event) => this.handleSignalAnswer(event)),
      this.signaling.onSignalIceCandidate((event) => this.handleSignalIceCandidate(event)),
      this.signaling.onPeerLeft((event) => {
        this.removePeer(event.peerId, 'peer-left');
      }),
      this.signaling.onPeerDisconnected((event) => {
        this.handlePeerDisconnected(event.peerId);
      }),
    );
  }

  // ── PeerMesh interface ───────────────────────────────────────

  async connect(peer: PeerDescriptor): Promise<void> {
    if (this.disposed) throw new Error('PeerManager disposed');
    if (peer.peerId === this.localPeerId) {
      throw new Error('Cannot connect to self');
    }

    // Remove existing connection if any
    this.removePeer(peer.peerId, 'reconnect');

    const entry = this.createPeerEntry(peer.peerId, peer.generation);
    entry.transport.initialize();

    // If we're the initial offerer, start negotiation
    if (isInitialOfferer(this.localPeerId, peer.peerId)) {
      await this.startNegotiation(entry);
    }
  }

  disconnect(peerId: PeerId, reason?: string): void {
    this.removePeer(peerId, reason ?? 'explicit-disconnect');
  }

  send(peerId: PeerId, message: ProtocolMessage): SendResult {
    const entry = this.peers.get(peerId);
    if (!entry) {
      return { ok: false, error: 'Peer not found' };
    }

    const json = JSON.stringify(message);
    const sent = entry.transport.send(json);
    return sent
      ? { ok: true }
      : { ok: false, error: 'DataChannel not open' };
  }

  getPeerStatus(peerId: PeerId): PeerStatus | undefined {
    const entry = this.peers.get(peerId);
    if (!entry) return undefined;
    return this.toPeerStatus(entry);
  }

  listPeers(): PeerStatus[] {
    return [...this.peers.values()].map((e) => this.toPeerStatus(e));
  }

  onMessage(handler: (message: InboundMessage) => void): Unsubscribe {
    this.messageHandlers.add(handler);
    return () => { this.messageHandlers.delete(handler); };
  }

  onPeerStatus(handler: (status: PeerStatus) => void): Unsubscribe {
    this.statusHandlers.add(handler);
    return () => { this.statusHandlers.delete(handler); };
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;

    for (const unsub of this.signalingCleanup) unsub();
    this.signalingCleanup = [];

    this.rttEngine.dispose();

    for (const [peerId] of this.peers) {
      this.removePeer(peerId, 'dispose');
    }

    this.messageHandlers.clear();
    this.statusHandlers.clear();
  }

  // ── Signaling handlers ───────────────────────────────────────

  private handleSignalOffer(event: SignalEvent): void {
    if (!this.isValidSignal(event)) return;

    const entry = this.getOrCreatePeerEntry(event.fromPeerId, event.senderGeneration);

    // Validate generation: reject stale signals
    if (event.senderGeneration < entry.generation) {
      return; // Stale generation, discard
    }

    // If generation increased, the peer reconnected — recreate transport
    if (event.senderGeneration > entry.generation) {
      this.removePeer(event.fromPeerId, 'generation-change');
      const newEntry = this.createPeerEntry(event.fromPeerId, event.senderGeneration);
      newEntry.transport.initialize();
      this.processOffer(newEntry, event);
      return;
    }

    if (!entry.transport.currentState || entry.transport.currentState === 'NEW') {
      entry.transport.initialize();
    }

    this.processOffer(entry, event);
  }

  private async processOffer(entry: PeerEntry, event: SignalEvent): Promise<void> {
    try {
      const payload = event.payload as { type: string; sdp: string };
      const answerSdp = await entry.transport.handleOffer(payload.sdp);

      if (answerSdp && this.roomId) {
        entry.negotiationId = event.negotiationId;
        this.signaling.sendAnswer(
          this.roomId,
          event.fromPeerId,
          answerSdp,
          event.negotiationId,
          this.localGeneration,
        );
      }
    } catch (err) {
      // Non-fatal: SDP processing can fail
      this.removePeer(entry.peerId, 'offer-error');
    }
  }

  private handleSignalAnswer(event: SignalEvent): void {
    if (!this.isValidSignal(event)) return;

    const entry = this.peers.get(event.fromPeerId);
    if (!entry) return;

    // Reject stale generation — answer is for an obsolete connection
    if (event.senderGeneration < entry.generation) return;

    // Reject newer generation — an answer is a response to an offer WE sent.
    // If the remote peer reconnected (generation advanced), our outstanding
    // offer was addressed to the old incarnation. This answer does not belong
    // to any negotiation with the new incarnation. Discard it; the correct
    // new offer will arrive via handleSignalOffer and start a fresh transport.
    if (event.senderGeneration > entry.generation) return;

    // Generation matches — apply the answer to the current transport
    const payload = event.payload as { type: string; sdp: string };
    entry.transport.handleAnswer(payload.sdp).catch(() => {
      // Non-fatal: answer processing can fail
    });
  }

  private handleSignalIceCandidate(event: SignalEvent): void {
    if (!this.isValidSignal(event)) return;

    const entry = this.peers.get(event.fromPeerId);
    if (!entry) return;

    // Reject stale generation — candidate is for an obsolete connection
    if (event.senderGeneration < entry.generation) return;

    // Reject newer generation — ICE candidates belong to a specific
    // negotiation. If the remote peer reconnected (generation advanced),
    // these candidates are for a negotiation we haven't started yet or
    // for the old transport that will be replaced. Discard them; the new
    // offer will create the correct transport and matching-generation
    // candidates will follow.
    if (event.senderGeneration > entry.generation) return;

    // Generation matches — apply the candidate to the current transport
    const payload = event.payload as Record<string, unknown>;
    if (payload['done']) return; // ICE gathering complete, no candidate

    entry.transport.addIceCandidate({
      candidate: payload['candidate'] as string | undefined,
      sdpMid: payload['sdpMid'] as string | null | undefined,
      sdpMLineIndex: payload['sdpMLineIndex'] as number | null | undefined,
      usernameFragment: payload['usernameFragment'] as string | null | undefined,
    }).catch(() => {
      // Non-fatal
    });
  }

  private handlePeerDisconnected(peerId: PeerId): void {
    const entry = this.peers.get(peerId);
    if (!entry) return;

    // Start grace timer — peer may reconnect
    if (entry.disconnectTimer) clearTimeout(entry.disconnectTimer);
    entry.disconnectTimer = setTimeout(() => {
      this.removePeer(peerId, 'disconnect-timeout');
    }, this.config.disconnectGraceMs);
  }

  // ── Negotiation ──────────────────────────────────────────────

  private async startNegotiation(entry: PeerEntry): Promise<void> {
    if (entry.transport.isMakingOffer) {
      return;
    }
    if (entry.transport.signalingState && entry.transport.signalingState !== 'stable') {
      return;
    }
    try {
      const offerSdp = await entry.transport.createOffer();
      if (this.roomId) {
        this.signaling.sendOffer(
          this.roomId,
          entry.peerId,
          offerSdp,
          entry.negotiationId,
          this.localGeneration,
        );
      }
    } catch {
      // Non-fatal: transient negotiation collisions should not destroy the connection
    }
  }

  // ── Peer entry management ────────────────────────────────────

  private createPeerEntry(peerId: PeerId, generation: ConnectionGeneration): PeerEntry {
    const negotiationId = this.generateNegotiationId();
    const events = this.createTransportEvents(peerId);
    const transport = new WebRTCTransport(
      this.localPeerId,
      peerId,
      generation,
      {
        iceServers: this.config.iceServers,
        dataChannelLabel: this.config.dataChannelLabel,
        maxIceQueueSize: this.config.maxIceQueueSize,
      },
      events,
    );

    const entry: PeerEntry = {
      peerId,
      generation,
      transport,
      negotiationId,
      disconnectTimer: null,
    };

    this.peers.set(peerId, entry);
    return entry;
  }

  private getOrCreatePeerEntry(peerId: PeerId, generation: ConnectionGeneration): PeerEntry {
    const existing = this.peers.get(peerId);
    if (existing) return existing;
    return this.createPeerEntry(peerId, generation);
  }

  private removePeer(peerId: PeerId, reason: string): void {
    const entry = this.peers.get(peerId);
    if (!entry) return;

    if (entry.disconnectTimer) {
      clearTimeout(entry.disconnectTimer);
      entry.disconnectTimer = null;
    }

    this.rttEngine.removePeer(peerId);
    entry.transport.close();
    this.peers.delete(peerId);

    this.emitPeerStatus({
      peerId,
      state: 'CLOSED',
      dataChannelState: null,
      generation: entry.generation,
      lastUpdatedAt: Date.now(),
    });
  }

  private createTransportEvents(remotePeerId: PeerId): TransportEvents {
    return {
      onStateChange: (state: PeerConnectionState) => {
        const entry = this.peers.get(remotePeerId);
        if (entry) {
          this.emitPeerStatus(this.toPeerStatus(entry));
        }
      },
      onDataChannelOpen: () => {
        const entry = this.peers.get(remotePeerId);
        if (entry) {
          // Cancel disconnect timer if peer reconnected
          if (entry.disconnectTimer) {
            clearTimeout(entry.disconnectTimer);
            entry.disconnectTimer = null;
          }
          this.rttEngine.start(remotePeerId, entry.generation);
          this.emitPeerStatus(this.toPeerStatus(entry));
        }
      },
      onDataChannelClose: () => {
        const entry = this.peers.get(remotePeerId);
        if (entry) {
          this.rttEngine.stop(remotePeerId);
          this.emitPeerStatus(this.toPeerStatus(entry));
        }
      },
      onDataChannelMessage: (data: string) => {
        this.handleDataChannelMessage(remotePeerId, data);
      },
      onIceCandidate: (candidate: RTCIceCandidate | null) => {
        if (!this.roomId) return;
        const entry = this.peers.get(remotePeerId);
        if (!entry) return;
        this.signaling.sendIceCandidate(
          this.roomId,
          remotePeerId,
          candidate ? candidate.toJSON() : null,
          entry.negotiationId,
          this.localGeneration,
        );
      },
      onNegotiationNeeded: () => {
        const entry = this.peers.get(remotePeerId);
        if (entry && !entry.transport.isMakingOffer && entry.transport.signalingState === 'stable') {
          this.startNegotiation(entry).catch(() => {
            // Non-fatal
          });
        }
      },
    };
  }

  // ── Message handling ─────────────────────────────────────────

  private handleDataChannelMessage(fromPeerId: PeerId, data: string): void {
    // Validate and parse through the protocol pipeline
    const result = parseAndValidateMessage(data);

    if (!result.ok) {
      // Reject malformed messages (LLD §11: "Validate every inbound DataChannel message")
      return;
    }

    const message = result.value;

    // Handle transport-level messages internally
    if (message.messageType === MessageType.PING) {
      this.handlePing(fromPeerId, message);
      return;
    }

    if (message.messageType === MessageType.PONG) {
      const pongPayload = message.payload as { probeId: string };
      this.rttEngine.handlePong(fromPeerId, pongPayload);
      return;
    }

    // Deliver to application handlers
    for (const handler of this.messageHandlers) {
      handler({ fromPeerId, message });
    }
  }

  private handlePing(fromPeerId: PeerId, pingMessage: ProtocolMessage): void {
    const pongPayload = {
      probeId: (pingMessage.payload as { probeId: string }).probeId,
    };

    const pongMessage: ProtocolMessage = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.PONG,
      sessionId: this.sessionId,
      senderPeerId: this.localPeerId,
      messageId: this.generateMessageId(),
      payload: pongPayload,
    };

    this.send(fromPeerId, pongMessage);
  }

  // ── Helpers ──────────────────────────────────────────────────

  private isValidSignal(event: SignalEvent): boolean {
    if (!this.roomId) return false;
    if (event.roomId !== this.roomId) return false;
    if (event.fromPeerId === this.localPeerId) return false; // Self-signal
    return true;
  }

  private toPeerStatus(entry: PeerEntry): PeerStatus {
    return {
      peerId: entry.peerId,
      state: entry.transport.currentState,
      dataChannelState: entry.transport.dataChannelState,
      generation: entry.generation,
      lastUpdatedAt: Date.now(),
    };
  }

  private emitPeerStatus(status: PeerStatus): void {
    for (const handler of this.statusHandlers) {
      handler(status);
    }
  }

  private generateNegotiationId(): string {
    return `neg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  private generateMessageId(): string {
    return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }
}

