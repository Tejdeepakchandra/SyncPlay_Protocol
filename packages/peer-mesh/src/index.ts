/**
 * @module @syncplay/peer-mesh
 *
 * WebRTC Peer Mesh SDK for SyncPlay.
 *
 * Manages RTCPeerConnection lifecycle, DataChannel communication,
 * and signaling integration with the Bootstrap Server.
 *
 * The transport adapter is internal — PeerManager is the public interface.
 * No raw browser objects (RTCPeerConnection, RTCDataChannel) are exposed.
 */

// ── Types ────────────────────────────────────────────────────
export type {
  PeerConnectionState,
  PeerDescriptor,
  PeerStatus,
  SendResult,
  InboundMessage,
  Unsubscribe,
  PeerMesh,
  PeerMeshConfig,
  SignalingOffer,
  SignalingIceCandidate,
} from './types.js';

export { DEFAULT_PEER_MESH_CONFIG } from './types.js';

// ── PeerManager ──────────────────────────────────────────────
export { PeerManager } from './peer-manager.js';

// ── Signaling Client ─────────────────────────────────────────
export { SignalingClient } from './signaling-client.js';

// ── Offerer Policy ───────────────────────────────────────────
export { isInitialOfferer, shouldLocalOfferWinGlare } from './offerer-policy.js';

// ── ICE Queue (exposed for testing, not typical application use) ─
export { IceQueue } from './ice-queue.js';

