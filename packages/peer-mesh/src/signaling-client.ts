/**
 * Signaling client — thin wrapper around socket.io-client
 * connecting to the Bootstrap Server.
 *
 * Sender identity is always server-derived (never client-supplied).
 * All relayed signals include senderGeneration from the server.
 */
import { io, type Socket } from 'socket.io-client';
import type { PeerId, SessionId, ConnectionGeneration } from '@syncplay/protocol';
import type { Unsubscribe } from './types.js';

// ── Event payloads ───────────────────────────────────────────

export interface PeerJoinedEvent {
  readonly roomId: SessionId;
  readonly peerId: PeerId;
  readonly generation: ConnectionGeneration;
  readonly peers: ReadonlyArray<{
    readonly peerId: PeerId;
    readonly status: string;
    readonly generation?: ConnectionGeneration;
  }>;
}

export interface PeerLeftEvent {
  readonly roomId: SessionId;
  readonly peerId: PeerId;
}

export interface PeerDisconnectedEvent {
  readonly roomId: SessionId;
  readonly peerId: PeerId;
}

export interface SignalEvent {
  readonly roomId: SessionId;
  readonly fromPeerId: PeerId;
  readonly senderGeneration: ConnectionGeneration;
  readonly negotiationId: string;
  readonly payload: Record<string, unknown>;
}

// ── Ack response ─────────────────────────────────────────────

interface AckResponse {
  ok: boolean;
  error?: { code: string; message: string };
  data?: Record<string, unknown>;
}

// ── Signaling Client ─────────────────────────────────────────

export class SignalingClient {
  private socket: Socket | null = null;
  private cleanupFns: Array<() => void> = [];

  /**
   * Connect to the Bootstrap Server.
   */
  connect(serverUrl: string): void {
    if (this.socket) return;

    this.socket = io(serverUrl, {
      transports: ['websocket'],
      autoConnect: true,
    });
  }

  /**
   * Join a room. Returns the ack from the server.
   */
  async joinRoom(
    roomId: SessionId,
    peerId: PeerId,
    membershipToken: string,
  ): Promise<AckResponse> {
    const socket = this.requireSocket();
    return new Promise((resolve) => {
      socket.emit('session:join', { roomId, peerId, membershipToken }, (ack: AckResponse) => {
        resolve(ack);
      });
    });
  }

  /**
   * Leave the current room.
   */
  async leaveRoom(roomId: SessionId): Promise<AckResponse> {
    const socket = this.requireSocket();
    return new Promise((resolve) => {
      socket.emit('session:leave', { roomId }, (ack: AckResponse) => {
        resolve(ack);
      });
    });
  }

  /**
   * Send an SDP offer to a target peer.
   */
  sendOffer(
    roomId: SessionId,
    targetPeerId: PeerId,
    sdp: string,
    negotiationId: string,
    declaredGeneration?: ConnectionGeneration,
  ): void {
    const socket = this.requireSocket();
    socket.emit('signal:offer', {
      roomId,
      targetPeerId,
      negotiationId,
      ...(declaredGeneration !== undefined ? { declaredGeneration } : {}),
      payload: { type: 'offer', sdp },
    });
  }

  /**
   * Send an SDP answer to a target peer.
   */
  sendAnswer(
    roomId: SessionId,
    targetPeerId: PeerId,
    sdp: string,
    negotiationId: string,
    declaredGeneration?: ConnectionGeneration,
  ): void {
    const socket = this.requireSocket();
    socket.emit('signal:answer', {
      roomId,
      targetPeerId,
      negotiationId,
      ...(declaredGeneration !== undefined ? { declaredGeneration } : {}),
      payload: { type: 'answer', sdp },
    });
  }

  /**
   * Send an ICE candidate to a target peer.
   */
  sendIceCandidate(
    roomId: SessionId,
    targetPeerId: PeerId,
    candidate: RTCIceCandidateInit | null,
    negotiationId: string,
    declaredGeneration?: ConnectionGeneration,
  ): void {
    const socket = this.requireSocket();
    socket.emit('signal:ice-candidate', {
      roomId,
      targetPeerId,
      negotiationId,
      ...(declaredGeneration !== undefined ? { declaredGeneration } : {}),
      payload: candidate
        ? {
            candidate: candidate.candidate,
            sdpMid: candidate.sdpMid,
            sdpMLineIndex: candidate.sdpMLineIndex,
            usernameFragment: candidate.usernameFragment,
          }
        : { done: true },
    });
  }

  // ── Event listeners ──────────────────────────────────────────

  onPeerJoined(handler: (event: PeerJoinedEvent) => void): Unsubscribe {
    return this.on('session:peer-joined', handler);
  }

  onPeerLeft(handler: (event: PeerLeftEvent) => void): Unsubscribe {
    return this.on('session:peer-left', handler);
  }

  onPeerDisconnected(handler: (event: PeerDisconnectedEvent) => void): Unsubscribe {
    return this.on('session:peer-disconnected', handler);
  }

  onSignalOffer(handler: (event: SignalEvent) => void): Unsubscribe {
    return this.on('signal:offer', handler);
  }

  onSignalAnswer(handler: (event: SignalEvent) => void): Unsubscribe {
    return this.on('signal:answer', handler);
  }

  onSignalIceCandidate(handler: (event: SignalEvent) => void): Unsubscribe {
    return this.on('signal:ice-candidate', handler);
  }

  onConnect(handler: () => void): Unsubscribe {
    return this.on('connect', handler);
  }

  onDisconnect(handler: (reason: string) => void): Unsubscribe {
    return this.on('disconnect', handler);
  }

  /**
   * Disconnect and clean up.
   */
  dispose(): void {
    for (const fn of this.cleanupFns) fn();
    this.cleanupFns = [];

    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
    }
  }

  // ── Private helpers ──────────────────────────────────────────

  private requireSocket(): Socket {
    if (!this.socket) throw new Error('SignalingClient not connected');
    return this.socket;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private on(event: string, handler: (...args: any[]) => void): Unsubscribe {
    const socket = this.requireSocket();
    socket.on(event, handler as (...args: unknown[]) => void);
    const unsub = () => { socket.off(event, handler as (...args: unknown[]) => void); };
    this.cleanupFns.push(unsub);
    return unsub;
  }
}
