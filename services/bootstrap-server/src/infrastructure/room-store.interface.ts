/**
 * RoomStore interface.
 *
 * Defines the persistence boundary for room data.
 * Business rules live in the service layer, not here.
 *
 * Per BOOTSTRAP_SERVER_LLD §2:
 * "InMemoryRoomStore implements a RoomStore interface;
 *  replace storage without rewriting business rules."
 */
import type { PeerId, SessionId } from '@syncplay/protocol';
import type { Room, PeerMembership } from '../models/room.model.js';

/**
 * Abstract storage interface for rooms.
 *
 * Implementations must be synchronous for in-memory and may
 * return Promises for persistent backends (future work).
 * The v1 in-memory implementation is synchronous.
 */
export interface RoomStore {
  /** Create a new room. Throws if roomId already exists. */
  createRoom(room: Room): void;

  /** Get a room by ID, or undefined if not found. */
  getRoom(roomId: SessionId): Room | undefined;

  /** Delete a room. Returns true if deleted, false if not found. */
  deleteRoom(roomId: SessionId): boolean;

  /** Check if a room exists. */
  hasRoom(roomId: SessionId): boolean;

  /** Get all rooms (for admin/cleanup). */
  getAllRooms(): IterableIterator<Room>;

  /** Get the count of active rooms. */
  getRoomCount(): number;

  // ── Peer operations (convenience for atomic updates) ───────

  /**
   * Add a peer to a room.
   * Returns false if room not found.
   * Does NOT check capacity — caller must enforce.
   */
  addPeer(roomId: SessionId, peer: PeerMembership): boolean;

  /**
   * Remove a peer from a room.
   * Returns the removed membership or undefined.
   */
  removePeer(roomId: SessionId, peerId: PeerId): PeerMembership | undefined;

  /**
   * Get a peer's membership from a room.
   */
  getPeer(roomId: SessionId, peerId: PeerId): PeerMembership | undefined;

  /**
   * Find the room and peerId associated with a given socketId.
   * Used on disconnect to locate the peer's room.
   */
  findBySocketId(socketId: string): { roomId: SessionId; peerId: PeerId } | undefined;
}
