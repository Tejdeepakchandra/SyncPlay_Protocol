/**
 * Room data model.
 *
 * Per BOOTSTRAP_SERVER_LLD §3 — Data Model and Identity Semantics:
 * - Room: roomId, createdAt, expiresAt, maxPeers, status, peers
 * - Membership: peerId, role, joinedAt, presence, generation, tokenHash
 * - Live binding: socketId, peerId, roomId, lastSeenAt
 *
 * INVARIANT: No playback fields (currentTime, isPlaying, playbackLeader)
 * are stored in the room model. The server never owns playback state.
 */
import type { PeerId, SessionId, SocketId, ConnectionGeneration } from '@syncplay/protocol';

// ── Room status ──────────────────────────────────────────────

/** Room lifecycle states per BOOTSTRAP_SERVER_LLD §4. */
export type RoomStatus = 'ACTIVE' | 'CLOSING';

// ── Peer presence ────────────────────────────────────────────

/** Peer presence states. */
export type PeerPresence = 'CONNECTED' | 'DISCONNECTED';

/** Role of a peer in the room. */
export type PeerRole = 'OWNER' | 'MEMBER';

// ── Membership ───────────────────────────────────────────────

/**
 * A peer's membership record within a room.
 *
 * peerId is stable across reconnections.
 * socketId changes on every new socket connection.
 * generation increments on rebind.
 */
export interface PeerMembership {
  readonly peerId: PeerId;
  readonly role: PeerRole;
  readonly joinedAt: number;
  presence: PeerPresence;
  socketId: SocketId | null;
  generation: ConnectionGeneration;
  /** SHA-256 hash of the membership token. Never store the raw token. */
  tokenHash: string;
  lastSeenAt: number;
}

// ── Room ─────────────────────────────────────────────────────

/**
 * A synchronization room.
 *
 * Contains membership data and lifecycle metadata.
 * Does NOT contain any playback state.
 */
export interface Room {
  readonly roomId: SessionId;
  readonly createdAt: number;
  expiresAt: number;
  readonly maxPeers: number;
  status: RoomStatus;
  /** Map of peerId → PeerMembership. */
  peers: Map<PeerId, PeerMembership>;
}

// ── Public room info (safe to expose via HTTP) ───────────────

/**
 * Safe room metadata for HTTP responses.
 * Per BOOTSTRAP_SERVER_LLD §5: never expose token or socketId.
 */
export interface SafeRoomInfo {
  readonly roomId: SessionId;
  readonly status: RoomStatus;
  readonly peerCount: number;
  readonly maxPeers: number;
}

// ── Room creation result ─────────────────────────────────────

/**
 * Returned to the room creator.
 * Contains the raw membership token (sent only once, never stored).
 */
export interface RoomCreationResult {
  readonly roomId: SessionId;
  readonly peerId: PeerId;
  readonly membershipToken: string;
  readonly expiresAt: number;
  readonly maxPeers: number;
}

// ── Peer snapshot (sent on join/reconnect) ────────────────────

/**
 * Snapshot of current room members, sent on session:joined.
 */
export interface PeerSnapshot {
  readonly roomId: SessionId;
  readonly peers: ReadonlyArray<{
    readonly peerId: PeerId;
    readonly role: PeerRole;
    readonly presence: PeerPresence;
  }>;
}
