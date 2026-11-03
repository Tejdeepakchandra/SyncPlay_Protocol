/**
 * WebRTC transport adapter.
 *
 * Wraps RTCPeerConnection and RTCDataChannel.
 * Per PEER_MESH_RTT_ENGINE_LLD §5:
 * "Transport adapter owns RTCPeerConnection, DataChannel, SDP/ICE."
 * "Do not expose raw browser objects outside the transport adapter."
 *
 * This module is never exported from the package — PeerManager is the
 * public interface.
 */
import type { PeerId, ConnectionGeneration } from '@syncplay/protocol';
import type { PeerConnectionState, Unsubscribe } from './types.js';
import { IceQueue } from './ice-queue.js';
import { isInitialOfferer, shouldLocalOfferWinGlare } from './offerer-policy.js';

// ── Types ────────────────────────────────────────────────────

export interface TransportConfig {
  readonly iceServers: readonly RTCIceServer[];
  readonly dataChannelLabel: string;
  readonly maxIceQueueSize: number;
}

export interface TransportEvents {
  onStateChange(state: PeerConnectionState): void;
  onDataChannelOpen(): void;
  onDataChannelClose(): void;
  onDataChannelMessage(data: string): void;
  onIceCandidate(candidate: RTCIceCandidate | null): void;
  onNegotiationNeeded(): void;
}

// ── Transport ────────────────────────────────────────────────

export class WebRTCTransport {
  private pc: RTCPeerConnection | null = null;
  private dc: RTCDataChannel | null = null;
  private readonly iceQueue: IceQueue;
  private makingOffer = false;
  private ignoreOffer = false;
  private state: PeerConnectionState = 'NEW';
  private disposed = false;

  readonly localPeerId: PeerId;
  readonly remotePeerId: PeerId;
  readonly generation: ConnectionGeneration;
  private readonly config: TransportConfig;
  private readonly events: TransportEvents;

  constructor(
    localPeerId: PeerId,
    remotePeerId: PeerId,
    generation: ConnectionGeneration,
    config: TransportConfig,
    events: TransportEvents,
  ) {
    this.localPeerId = localPeerId;
    this.remotePeerId = remotePeerId;
    this.generation = generation;
    this.config = config;
    this.events = events;
    this.iceQueue = new IceQueue(config.maxIceQueueSize);
  }

  get currentState(): PeerConnectionState {
    return this.state;
  }

  get dataChannelState(): RTCDataChannelState | null {
    return this.dc?.readyState ?? null;
  }

  /**
   * Initialize the RTCPeerConnection and set up event handlers.
   * If this peer is the initial offerer, also creates the DataChannel.
   */
  initialize(): void {
    if (this.disposed) throw new Error('Transport disposed');
    if (this.pc) throw new Error('Transport already initialized');

    this.pc = new RTCPeerConnection({
      iceServers: [...this.config.iceServers],
    });

    this.setupPeerConnectionHandlers(this.pc);

    // The designated offerer creates the DataChannel
    if (isInitialOfferer(this.localPeerId, this.remotePeerId)) {
      this.dc = this.pc.createDataChannel(this.config.dataChannelLabel, {
        ordered: true,
      });
      this.setupDataChannelHandlers(this.dc);
    }

    this.setState('SIGNALING');
  }

  /**
   * Create an SDP offer and set it as local description.
   * Returns the offer SDP string.
   */
  async createOffer(): Promise<string> {
    if (!this.pc) throw new Error('Transport not initialized');
    this.makingOffer = true;
    try {
      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      const sdp = this.pc.localDescription?.sdp;
      if (!sdp) throw new Error('No local description SDP after createOffer');
      return sdp;
    } finally {
      this.makingOffer = false;
    }
  }

  /**
   * Handle an incoming SDP offer.
   * Implements "perfect negotiation" glare resolution.
   *
   * Returns the answer SDP if the offer was accepted,
   * or null if it was ignored (our offer wins glare).
   */
  async handleOffer(sdp: string): Promise<string | null> {
    if (!this.pc) throw new Error('Transport not initialized');

    const offerCollision = this.makingOffer || this.pc.signalingState !== 'stable';

    // Perfect negotiation: the designated offerer's offer wins
    this.ignoreOffer = offerCollision && shouldLocalOfferWinGlare(this.localPeerId, this.remotePeerId);

    if (this.ignoreOffer) {
      return null; // Our offer wins; ignore the incoming offer
    }

    // Accept the incoming offer
    await this.pc.setRemoteDescription({ type: 'offer', sdp });
    this.flushIceQueue();

    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);

    const answerSdp = this.pc.localDescription?.sdp;
    if (!answerSdp) throw new Error('No local description SDP after createAnswer');
    return answerSdp;
  }

  /**
   * Handle an incoming SDP answer.
   */
  async handleAnswer(sdp: string): Promise<void> {
    if (!this.pc) throw new Error('Transport not initialized');
    if (this.pc.signalingState === 'stable') {
      // Already stable, ignore late answer
      return;
    }
    await this.pc.setRemoteDescription({ type: 'answer', sdp });
    this.flushIceQueue();
  }

  /**
   * Add an ICE candidate, queuing if remote description not yet set.
   */
  async addIceCandidate(candidate: RTCIceCandidateInit): Promise<void> {
    if (!this.pc) throw new Error('Transport not initialized');

    const result = this.iceQueue.enqueue(candidate, this.generation);
    switch (result) {
      case 'ready':
        await this.pc.addIceCandidate(candidate);
        break;
      case 'queued':
        // Will be flushed when remote description is set
        break;
      case 'dropped':
        // Queue full; candidate lost
        break;
    }
  }

  /**
   * Send a string message over the DataChannel.
   */
  send(data: string): boolean {
    if (!this.dc || this.dc.readyState !== 'open') {
      return false;
    }
    try {
      this.dc.send(data);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Close the transport and release all resources.
   */
  close(): void {
    if (this.disposed) return;
    this.disposed = true;

    this.iceQueue.reset();

    if (this.dc) {
      try { this.dc.close(); } catch { /* ignore */ }
      this.dc = null;
    }

    if (this.pc) {
      try { this.pc.close(); } catch { /* ignore */ }
      this.pc = null;
    }

    this.setState('CLOSED');
  }

  // ── Private helpers ──────────────────────────────────────────

  private setState(newState: PeerConnectionState): void {
    if (this.state === newState) return;
    this.state = newState;
    this.events.onStateChange(newState);
  }

  private flushIceQueue(): void {
    if (!this.pc) return;
    const candidates = this.iceQueue.flush();
    for (const { candidate } of candidates) {
      this.pc.addIceCandidate(candidate).catch(() => {
        // Non-fatal: candidate may be invalid after state change
      });
    }
  }

  private setupPeerConnectionHandlers(pc: RTCPeerConnection): void {
    pc.onicecandidate = (event) => {
      this.events.onIceCandidate(event.candidate);
    };

    pc.onconnectionstatechange = () => {
      this.mapConnectionState(pc.connectionState);
    };

    pc.oniceconnectionstatechange = () => {
      // Use connectionState as primary; iceConnectionState as fallback
      if (pc.iceConnectionState === 'failed' && this.state !== 'FAILED') {
        this.setState('FAILED');
      }
    };

    pc.onnegotiationneeded = () => {
      this.events.onNegotiationNeeded();
    };

    // The answerer receives the DataChannel via ondatachannel
    pc.ondatachannel = (event) => {
      this.dc = event.channel;
      this.setupDataChannelHandlers(this.dc);
    };
  }

  private setupDataChannelHandlers(dc: RTCDataChannel): void {
    dc.onopen = () => {
      this.events.onDataChannelOpen();
    };

    dc.onclose = () => {
      this.events.onDataChannelClose();
    };

    dc.onmessage = (event: MessageEvent) => {
      if (typeof event.data === 'string') {
        this.events.onDataChannelMessage(event.data);
      }
    };
  }

  private mapConnectionState(rtcState: RTCPeerConnectionState): void {
    switch (rtcState) {
      case 'new':
        // Don't override our higher-level state
        break;
      case 'connecting':
        this.setState('CONNECTING');
        break;
      case 'connected':
        this.setState('CONNECTED');
        break;
      case 'disconnected':
        this.setState('DISCONNECTED');
        break;
      case 'failed':
        this.setState('FAILED');
        break;
      case 'closed':
        this.setState('CLOSED');
        break;
    }
  }
}

