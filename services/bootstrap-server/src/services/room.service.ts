/**
 * Room lifecycle service.
 *
 * Implements BOOTSTRAP_SERVER_LLD §4 — Room and Membership Lifecycle:
 * - Create: validate limits, generate room ID, owner peer ID and scoped credential
 * - Join: validate room, credential, status and capacity; reserve membership atomically
 * - Leave: remove membership and notify peers
 * - Reconnect: validate credential, bind new socket, increment generation
 * - Empty room: grace timer → expiry → deletion
 *
 * INVARIANT: This service never stores or decides playback state.
 */
import { nanoid } from 'nanoid';
import type { PeerId, SessionId, SocketId } from '@syncplay/protocol';
import type { RoomStore } from '../infrastructure/room-store.interface.js';
import type {
  Room,
  PeerMembership,
  RoomCreationResult,
  SafeRoomInfo,
  PeerSnapshot,
} from '../models/room.model.js';
import type { ServerConfig } from '../config/env.js';

// ── Error codes (from BOOTSTRAP_SERVER_LLD §5) ──────────────

export type RoomErrorCode =
  | 'INVALID_REQUEST'
  | 'ROOM_NOT_FOUND'
  | 'ROOM_FULL'
  | 'ROOM_CLOSING'
  | 'INVALID_CREDENTIALS'
  | 'PEER_NOT_FOUND'
  | 'DUPLICATE_JOIN'
  | 'STALE_GENERATION'
  | 'INTERNAL_ERROR';

export class RoomError extends Error {
  constructor(
    public readonly code: RoomErrorCode,
    message: string,
  ) {
    super(message);
    this.name = 'RoomError';
  }
}

// ── Token helpers ────────────────────────────────────────────

/** Generate a high-entropy membership token (never logged). */
function generateToken(): string {
  return nanoid(32);
}

/** Hash a token for storage (simple but sufficient for v1 in-memory). */
async function hashToken(token: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(token);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** Verify a token against a stored hash. */
async function verifyToken(token: string, storedHash: string): Promise<boolean> {
  const hash = await hashToken(token);
  return hash === storedHash;
}

// ── Service ──────────────────────────────────────────────────

export class RoomService {
  /** Active timers for empty-room cleanup. */
  private readonly emptyRoomTimers = new Map<SessionId, ReturnType<typeof setTimeout>>();

  /** Active timers for disconnected-peer grace. */
  private readonly disconnectTimers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(
    private readonly store: RoomStore,
    private readonly config: ServerConfig,
  ) {}

  // ── Create ───────────────────────────────────────────────

  /**
   * Create a new room.
   *
   * @param requestedMaxPeers - Optional requested max peers (bounded by config).
   * @returns Room creation result with credentials (token sent only once).
   */
  async createRoom(requestedMaxPeers?: number): Promise<RoomCreationResult> {
    const maxPeers = Math.min(
      requestedMaxPeers ?? this.config.DEFAULT_MAX_PEERS,
      this.config.ABSOLUTE_MAX_PEERS,
    );

    if (maxPeers < 2) {
      throw new RoomError('INVALID_REQUEST', 'maxPeers must be at least 2');
    }

    const roomId = nanoid(12);
    const peerId = nanoid(10);
    const token = generateToken();
    const tokenHash = await hashToken(token);
    const now = Date.now();

    const owner: PeerMembership = {
      peerId,
      role: 'OWNER',
      joinedAt: now,
      presence: 'CONNECTED',
      socketId: null, // Will be set on Socket.IO join
      generation: 1,
      tokenHash,
      lastSeenAt: now,
    };

    const room: Room = {
      roomId,
      createdAt: now,
      expiresAt: now + this.config.ROOM_TTL_MS,
      maxPeers,
      status: 'ACTIVE',
      peers: new Map([[peerId, owner]]),
    };

    this.store.createRoom(room);

    return {
      roomId,
      peerId,
      membershipToken: token,
      expiresAt: room.expiresAt,
      maxPeers,
    };
  }

  // ── Join ─────────────────────────────────────────────────

  /**
   * Join an existing room.
   *
   * @returns New peer's credentials and room snapshot.
   */
  async joinRoom(roomId: SessionId): Promise<RoomCreationResult> {
    const room = this.store.getRoom(roomId);
    if (!room) {
      throw new RoomError('ROOM_NOT_FOUND', `Room ${roomId} not found`);
    }

    if (room.status === 'CLOSING') {
      throw new RoomError('ROOM_CLOSING', `Room ${roomId} is closing`);
    }

    // Count connected peers for capacity check
    if (room.peers.size >= room.maxPeers) {
      throw new RoomError('ROOM_FULL', `Room ${roomId} is full (${room.maxPeers} peers)`);
    }

    const peerId = nanoid(10);
    const token = generateToken();
    const tokenHash = await hashToken(token);
    const now = Date.now();

    const member: PeerMembership = {
      peerId,
      role: 'MEMBER',
      joinedAt: now,
      presence: 'CONNECTED',
      socketId: null,
      generation: 1,
      tokenHash,
      lastSeenAt: now,
    };

    this.store.addPeer(roomId, member);

    // Cancel empty-room timer if this room was about to expire
    this.cancelEmptyRoomTimer(roomId);

    return {
      roomId,
      peerId,
      membershipToken: token,
      expiresAt: room.expiresAt,
      maxPeers: room.maxPeers,
    };
  }

  // ── Bind socket (called after Socket.IO session:join) ────

  /**
   * Bind a socket to a peer membership after token validation.
   *
   * @returns Peer snapshot for the joining client.
   */
  async bindSocket(
    roomId: SessionId,
    peerId: PeerId,
    token: string,
    socketId: SocketId,
  ): Promise<PeerSnapshot> {
    const room = this.store.getRoom(roomId);
    if (!room) {
      throw new RoomError('ROOM_NOT_FOUND', `Room ${roomId} not found`);
    }

    if (room.status === 'CLOSING') {
      throw new RoomError('ROOM_CLOSING', `Room ${roomId} is closing`);
    }

    const peer = room.peers.get(peerId);
    if (!peer) {
      throw new RoomError('PEER_NOT_FOUND', `Peer ${peerId} not in room ${roomId}`);
    }

    // Validate credential
    const valid = await verifyToken(token, peer.tokenHash);
    if (!valid) {
      throw new RoomError('INVALID_CREDENTIALS', 'Invalid membership token');
    }

    // Bind socket
    const oldSocketId = peer.socketId;
    peer.socketId = socketId;
    peer.presence = 'CONNECTED';
    peer.lastSeenAt = Date.now();

    // If rebinding (reconnect), increment generation
    if (oldSocketId !== null) {
      peer.generation += 1;
    }

    // Update socket index in store
    if ('updateSocketBinding' in this.store) {
      (this.store as any).updateSocketBinding(roomId, peerId, oldSocketId, socketId);
    }

    // Cancel disconnect grace timer if reconnecting
    this.cancelDisconnectTimer(peerId);

    return this.buildPeerSnapshot(room);
  }

  // ── Leave ────────────────────────────────────────────────

  /**
   * Explicit peer departure.
   * Removes membership permanently and cleans up.
   */
  leavePeer(roomId: SessionId, peerId: PeerId): void {
    const removed = this.store.removePeer(roomId, peerId);
    if (!removed) return;

    this.cancelDisconnectTimer(peerId);

    // Check if room is now empty
    const room = this.store.getRoom(roomId);
    if (room && room.peers.size === 0) {
      this.startEmptyRoomTimer(roomId);
    }
  }

  // ── Disconnect (unexpected) ──────────────────────────────

  /**
   * Handle unexpected socket disconnect.
   *
   * Per BOOTSTRAP_SERVER_LLD §4: "mark DISCONNECTED; start grace timer
   * rather than immediate permanent removal."
   */
  handleDisconnect(socketId: SocketId): { roomId: SessionId; peerId: PeerId } | undefined {
    const binding = this.store.findBySocketId(socketId);
    if (!binding) return undefined;

    const { roomId, peerId } = binding;
    const peer = this.store.getPeer(roomId, peerId);
    if (!peer) return undefined;

    peer.presence = 'DISCONNECTED';
    peer.socketId = null;

    // Start grace timer for reconnection
    this.startDisconnectTimer(roomId, peerId);

    return binding;
  }

  // ── Room info (safe) ─────────────────────────────────────

  /**
   * Get safe room info for HTTP responses.
   * Never exposes tokens or socketIds.
   */
  getRoomInfo(roomId: SessionId): SafeRoomInfo | undefined {
    const room = this.store.getRoom(roomId);
    if (!room) return undefined;

    return {
      roomId: room.roomId,
      status: room.status,
      peerCount: room.peers.size,
      maxPeers: room.maxPeers,
    };
  }

  // ── Peer snapshot ────────────────────────────────────────

  buildPeerSnapshot(room: Room): PeerSnapshot {
    return {
      roomId: room.roomId,
      peers: [...room.peers.values()].map((p) => ({
        peerId: p.peerId,
        role: p.role,
        presence: p.presence,
      })),
    };
  }

  // ── Signaling authorization ──────────────────────────────

  /** Result of signaling authorization. */
  static readonly SignalingAuth = class {
    constructor(
      public readonly roomId: SessionId,
      public readonly fromPeerId: PeerId,
      public readonly targetSocketId: SocketId,
      public readonly senderGeneration: number,
    ) {}
  };

  /**
   * Validate that a signaling message can be relayed.
   *
   * Per BOOTSTRAP_SERVER_LLD §7:
   * - Never trust client-supplied fromPeerId; derive from socket binding
   * - Sender must be bound to socket
   * - Target must be in the same room
   * - Room must be ACTIVE
   * - Reject stale signaling if client declares an obsolete generation
   *
   * @param socketId - The sending socket's ID (server-derived, never client-supplied)
   * @param targetPeerId - The intended recipient peer
   * @param declaredGeneration - Optional: the generation the sender believes is current.
   *   If provided and does not match the server's record, the signal is rejected as stale.
   */
  authorizeSignaling(
    socketId: SocketId,
    targetPeerId: PeerId,
    declaredGeneration?: number,
  ): { roomId: SessionId; fromPeerId: PeerId; targetSocketId: SocketId; senderGeneration: number } {
    const binding = this.store.findBySocketId(socketId);
    if (!binding) {
      throw new RoomError('INVALID_CREDENTIALS', 'Socket not bound to any peer');
    }

    const { roomId, peerId: fromPeerId } = binding;
    const room = this.store.getRoom(roomId);
    if (!room) {
      throw new RoomError('ROOM_NOT_FOUND', 'Room no longer exists');
    }

    if (room.status === 'CLOSING') {
      throw new RoomError('ROOM_CLOSING', 'Room is closing');
    }

    if (fromPeerId === targetPeerId) {
      throw new RoomError('INVALID_REQUEST', 'Cannot signal yourself');
    }

    // Look up sender's current generation
    const senderPeer = room.peers.get(fromPeerId);
    if (!senderPeer) {
      throw new RoomError('PEER_NOT_FOUND', 'Sender peer no longer in room');
    }

    // Reject stale generation if the client declared one
    if (declaredGeneration !== undefined && declaredGeneration !== senderPeer.generation) {
      throw new RoomError(
        'STALE_GENERATION',
        `Stale generation: client declared ${declaredGeneration}, server has ${senderPeer.generation}`,
      );
    }

    const targetPeer = room.peers.get(targetPeerId);
    if (!targetPeer) {
      throw new RoomError('PEER_NOT_FOUND', `Target peer ${targetPeerId} not in room`);
    }

    if (!targetPeer.socketId) {
      throw new RoomError('PEER_NOT_FOUND', `Target peer ${targetPeerId} is not connected`);
    }

    return {
      roomId,
      fromPeerId,
      targetSocketId: targetPeer.socketId,
      senderGeneration: senderPeer.generation,
    };
  }

  /**
   * Get the generation of a peer for stale-connection checks.
   */
  getPeerGeneration(roomId: SessionId, peerId: PeerId): number | undefined {
    const peer = this.store.getPeer(roomId, peerId);
    return peer?.generation;
  }

  // ── Cleanup ──────────────────────────────────────────────

  /**
   * Run periodic room expiry check.
   * Closes rooms that have exceeded their TTL.
   */
  cleanupExpiredRooms(): number {
    const now = Date.now();
    let cleaned = 0;

    for (const room of this.store.getAllRooms()) {
      if (now >= room.expiresAt) {
        room.status = 'CLOSING';
        this.store.deleteRoom(room.roomId);
        this.cancelEmptyRoomTimer(room.roomId);
        cleaned++;
      }
    }

    return cleaned;
  }

  /**
   * Dispose all timers. Call on server shutdown.
   */
  dispose(): void {
    for (const timer of this.emptyRoomTimers.values()) {
      clearTimeout(timer);
    }
    this.emptyRoomTimers.clear();

    for (const timer of this.disconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectTimers.clear();
  }

  // ── Private timer helpers ────────────────────────────────

  private startEmptyRoomTimer(roomId: SessionId): void {
    this.cancelEmptyRoomTimer(roomId);

    const timer = setTimeout(() => {
      this.emptyRoomTimers.delete(roomId);
      const room = this.store.getRoom(roomId);
      if (room && room.peers.size === 0) {
        room.status = 'CLOSING';
        this.store.deleteRoom(roomId);
      }
    }, this.config.EMPTY_ROOM_GRACE_MS);

    // Don't keep the process alive for room cleanup timers
    timer.unref();
    this.emptyRoomTimers.set(roomId, timer);
  }

  private cancelEmptyRoomTimer(roomId: SessionId): void {
    const timer = this.emptyRoomTimers.get(roomId);
    if (timer) {
      clearTimeout(timer);
      this.emptyRoomTimers.delete(roomId);
    }
  }

  private startDisconnectTimer(roomId: SessionId, peerId: PeerId): void {
    const key = `${roomId}:${peerId}`;
    this.cancelDisconnectTimer(peerId);

    const timer = setTimeout(() => {
      this.disconnectTimers.delete(key);
      // Permanently remove peer after grace period
      this.leavePeer(roomId, peerId);
    }, this.config.DISCONNECTED_GRACE_MS);

    timer.unref();
    this.disconnectTimers.set(key, timer);
  }

  private cancelDisconnectTimer(peerId: PeerId): void {
    // Find and cancel any disconnect timer for this peer
    for (const [key, timer] of this.disconnectTimers.entries()) {
      if (key.endsWith(`:${peerId}`)) {
        clearTimeout(timer);
        this.disconnectTimers.delete(key);
        break;
      }
    }
  }
}
