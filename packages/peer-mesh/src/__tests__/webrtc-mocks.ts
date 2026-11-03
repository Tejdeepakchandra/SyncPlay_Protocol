/**
 * Minimal WebRTC mock for Node.js test environment.
 *
 * These mocks simulate the essential RTCPeerConnection/RTCDataChannel
 * behavior needed by unit tests. They are NOT a full WebRTC implementation.
 *
 * Real browser verification is done via the demo app (apps/demo/).
 */
import { vi } from 'vitest';

// ── Mock DataChannel ─────────────────────────────────────────

export class MockRTCDataChannel extends EventTarget {
  label: string;
  ordered: boolean;
  readyState: RTCDataChannelState = 'connecting';
  private sentMessages: string[] = [];

  onopen: ((this: RTCDataChannel, ev: Event) => void) | null = null;
  onclose: ((this: RTCDataChannel, ev: Event) => void) | null = null;
  onmessage: ((this: RTCDataChannel, ev: MessageEvent) => void) | null = null;
  onerror: ((this: RTCDataChannel, ev: Event) => void) | null = null;

  constructor(label: string, options?: { ordered?: boolean }) {
    super();
    this.label = label;
    this.ordered = options?.ordered ?? true;
  }

  send(data: string): void {
    if (this.readyState !== 'open') {
      throw new DOMException('DataChannel not open', 'InvalidStateError');
    }
    this.sentMessages.push(data);
  }

  close(): void {
    this.readyState = 'closed';
  }

  // ── Test helpers ─────────────────────────────────────────────

  simulateOpen(): void {
    this.readyState = 'open';
    const event = new Event('open');
    this.onopen?.call(this as unknown as RTCDataChannel, event);
  }

  simulateClose(): void {
    this.readyState = 'closed';
    const event = new Event('close');
    this.onclose?.call(this as unknown as RTCDataChannel, event);
  }

  simulateMessage(data: string): void {
    const event = new MessageEvent('message', { data });
    this.onmessage?.call(this as unknown as RTCDataChannel, event);
  }

  getSentMessages(): string[] {
    return [...this.sentMessages];
  }
}

// ── Mock PeerConnection ──────────────────────────────────────

export class MockRTCPeerConnection extends EventTarget {
  connectionState: RTCPeerConnectionState = 'new';
  iceConnectionState: RTCIceConnectionState = 'new';
  signalingState: RTCSignalingState = 'stable';
  localDescription: RTCSessionDescription | null = null;
  remoteDescription: RTCSessionDescription | null = null;

  onicecandidate: ((this: RTCPeerConnection, ev: RTCPeerConnectionIceEvent) => void) | null = null;
  onconnectionstatechange: ((this: RTCPeerConnection, ev: Event) => void) | null = null;
  oniceconnectionstatechange: ((this: RTCPeerConnection, ev: Event) => void) | null = null;
  onnegotiationneeded: ((this: RTCPeerConnection, ev: Event) => void) | null = null;
  ondatachannel: ((this: RTCPeerConnection, ev: RTCDataChannelEvent) => void) | null = null;

  private dataChannels: MockRTCDataChannel[] = [];
  private closed = false;
  private offerCount = 0;

  constructor(_config?: RTCConfiguration) {
    super();
  }

  createDataChannel(label: string, options?: RTCDataChannelInit): MockRTCDataChannel {
    const dc = new MockRTCDataChannel(label, { ordered: options?.ordered });
    this.dataChannels.push(dc);
    return dc;
  }

  async createOffer(_options?: RTCOfferOptions): Promise<RTCSessionDescriptionInit> {
    this.offerCount++;
    return {
      type: 'offer',
      sdp: `v=0\r\no=- ${Date.now()} ${this.offerCount} IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`,
    };
  }

  async createAnswer(_options?: RTCAnswerOptions): Promise<RTCSessionDescriptionInit> {
    return {
      type: 'answer',
      sdp: `v=0\r\no=- ${Date.now()} 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`,
    };
  }

  async setLocalDescription(desc: RTCSessionDescriptionInit): Promise<void> {
    this.localDescription = desc as RTCSessionDescription;
    if (desc.type === 'offer') {
      this.signalingState = 'have-local-offer';
    } else if (desc.type === 'answer') {
      this.signalingState = 'stable';
    }
  }

  async setRemoteDescription(desc: RTCSessionDescriptionInit): Promise<void> {
    this.remoteDescription = desc as RTCSessionDescription;
    if (desc.type === 'offer') {
      this.signalingState = 'have-remote-offer';
    } else if (desc.type === 'answer') {
      this.signalingState = 'stable';
    }
  }

  async addIceCandidate(_candidate: RTCIceCandidateInit): Promise<void> {
    // Mock: no-op
  }

  close(): void {
    this.closed = true;
    this.connectionState = 'closed';
    this.signalingState = 'closed';
  }

  // ── Test helpers ─────────────────────────────────────────────

  simulateConnectionState(state: RTCPeerConnectionState): void {
    this.connectionState = state;
    this.onconnectionstatechange?.call(
      this as unknown as RTCPeerConnection,
      new Event('connectionstatechange'),
    );
  }

  simulateIceCandidate(candidate: RTCIceCandidate | null): void {
    const event = { candidate } as RTCPeerConnectionIceEvent;
    this.onicecandidate?.call(this as unknown as RTCPeerConnection, event);
  }

  simulateDataChannel(dc: MockRTCDataChannel): void {
    const event = { channel: dc } as unknown as RTCDataChannelEvent;
    this.ondatachannel?.call(this as unknown as RTCPeerConnection, event);
  }

  simulateNegotiationNeeded(): void {
    this.onnegotiationneeded?.call(
      this as unknown as RTCPeerConnection,
      new Event('negotiationneeded'),
    );
  }

  getDataChannels(): MockRTCDataChannel[] {
    return [...this.dataChannels];
  }

  isClosed(): boolean {
    return this.closed;
  }
}

// ── Install mocks globally ───────────────────────────────────

export function installWebRTCMocks(): void {
  // @ts-expect-error -- Mocking browser globals in Node.js test environment
  globalThis.RTCPeerConnection = MockRTCPeerConnection;

  // Mock RTCSessionDescription if not present
  if (!globalThis.RTCSessionDescription) {
    // @ts-expect-error -- Mock
    globalThis.RTCSessionDescription = class {
      type: string;
      sdp: string;
      constructor(init: { type: string; sdp: string }) {
        this.type = init.type;
        this.sdp = init.sdp;
      }
    };
  }
}

export function removeWebRTCMocks(): void {
  // @ts-expect-error -- Cleaning up mocks
  delete globalThis.RTCPeerConnection;
  // @ts-expect-error -- Cleaning up mocks
  delete globalThis.RTCSessionDescription;
}

