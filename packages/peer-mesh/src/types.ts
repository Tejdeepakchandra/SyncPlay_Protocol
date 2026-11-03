/**
 * Peer Mesh types.
 *
 * Per INTERFACE_CONTRACTS.md §2 and PEER_MESH_RTT_ENGINE_LLD §4–5.
 * Transport-agnostic interfaces; no browser objects exposed.
 */
import type {
  PeerId,
  SessionId,
  ConnectionGeneration,
  ProtocolMessage,
} from '@syncplay/protocol';

// ── Peer connection state machine (LLD §4) ───────────────────

/**
 * Connection state for a single peer.
 *
 * NEW → SIGNALING → CONNECTING → CONNECTED → DISCONNECTED → CLOSED
 *                                          → FAILED → CLOSED
 */
export type PeerConnectionState =
  | 'NEW'
  | 'SIGNALING'
  | 'CONNECTING'
  | 'CONNECTED'
  | 'DISCONNECTED'
  | 'FAILED'
  | 'CLOSED';

// ── Peer descriptor ──────────────────────────────────────────

/** Information needed to initiate a connection to a peer. */
export interface PeerDescriptor {
  readonly peerId: PeerId;
  readonly sessionId: SessionId;
  readonly generation: ConnectionGeneration;
}

// ── Peer status ──────────────────────────────────────────────

/** Observable status of a peer connection. */
export interface PeerStatus {
  readonly peerId: PeerId;
  readonly state: PeerConnectionState;
  readonly dataChannelState: 'connecting' | 'open' | 'closing' | 'closed' | null;
  readonly generation: ConnectionGeneration;
  readonly lastUpdatedAt: number;
}

// ── Send result ──────────────────────────────────────────────

export interface SendResult {
  readonly ok: boolean;
  readonly error?: string;
}

// ── Inbound message ──────────────────────────────────────────

export interface InboundMessage {
  readonly fromPeerId: PeerId;
  readonly message: ProtocolMessage;
}

// ── Unsubscribe ──────────────────────────────────────────────

export type Unsubscribe = () => void;

// ── PeerMesh interface (INTERFACE_CONTRACTS.md §2) ───────────

export interface PeerMesh {
  connect(peer: PeerDescriptor): Promise<void>;
  disconnect(peerId: PeerId, reason?: string): void;
  send(peerId: PeerId, message: ProtocolMessage): SendResult;
  getPeerStatus(peerId: PeerId): PeerStatus | undefined;
  listPeers(): PeerStatus[];
  onMessage(handler: (message: InboundMessage) => void): Unsubscribe;
  onPeerStatus(handler: (status: PeerStatus) => void): Unsubscribe;
  dispose(): Promise<void>;
}

// ── Signaling events from Bootstrap Server ───────────────────

export interface SignalingOffer {
  readonly roomId: SessionId;
  readonly fromPeerId: PeerId;
  readonly senderGeneration: ConnectionGeneration;
  readonly negotiationId: string;
  readonly payload: {
    readonly type: 'offer' | 'answer';
    readonly sdp: string;
  };
}

export interface SignalingIceCandidate {
  readonly roomId: SessionId;
  readonly fromPeerId: PeerId;
  readonly senderGeneration: ConnectionGeneration;
  readonly negotiationId: string;
  readonly payload: {
    readonly candidate?: string;
    readonly sdpMid?: string | null;
    readonly sdpMLineIndex?: number | null;
    readonly usernameFragment?: string | null;
    readonly done?: boolean;
  };
}

// ── Configuration ────────────────────────────────────────────

export interface PeerMeshConfig {
  /** Maximum ICE candidates to queue before remote description is set. Default: 64 */
  readonly maxIceQueueSize: number;
  /** ICE servers for RTCPeerConnection configuration. */
  readonly iceServers: readonly RTCIceServer[];
  /** DataChannel label. Default: 'syncplay-data' */
  readonly dataChannelLabel: string;
  /** Grace period (ms) before marking disconnected peer as failed. Default: 5000 */
  readonly disconnectGraceMs: number;
}

export const DEFAULT_PEER_MESH_CONFIG: PeerMeshConfig = {
  maxIceQueueSize: 64,
  iceServers: [],
  dataChannelLabel: 'syncplay-data',
  disconnectGraceMs: 5000,
};
