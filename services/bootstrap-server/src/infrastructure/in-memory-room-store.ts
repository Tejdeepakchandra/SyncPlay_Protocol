/**
 * In-memory RoomStore implementation.
 *
 * Per BOOTSTRAP_SERVER_LLD §2:
 * "In-memory Map for v1; process restart loses rooms,
 *  and separate server instances do not share membership."
 *
 * This is intentionally ephemeral — no persistence or high availability.
 */
import type { PeerId, SessionId } from '@syncplay/protocol';
import type { Room, PeerMembership } from '../models/room.model.js';
import type { RoomStore } from './room-store.interface.js';

export class InMemoryRoomStore implements RoomStore {
  private readonly rooms = new Map<SessionId, Room>();

  /**
   * Secondary index: socketId → { roomId, peerId }.
   * Maintained on addPeer/removePeer for O(1) disconnect lookup.
   */
  private readonly socketIndex = new Map<string, { roomId: SessionId; peerId: PeerId }>();

  createRoom(room: Room): void {
    if (this.rooms.has(room.roomId)) {
      throw new Error(`Room ${room.roomId} already exists`);
    }
    this.rooms.set(room.roomId, room);
  }

  getRoom(roomId: SessionId): Room | undefined {
    return this.rooms.get(roomId);
  }

  deleteRoom(roomId: SessionId): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    // Clean up socket index for all peers in the room
    for (const peer of room.peers.values()) {
      if (peer.socketId) {
        this.socketIndex.delete(peer.socketId);
      }
    }

    return this.rooms.delete(roomId);
  }

  hasRoom(roomId: SessionId): boolean {
    return this.rooms.has(roomId);
  }

  *getAllRooms(): IterableIterator<Room> {
    yield* this.rooms.values();
  }

  getRoomCount(): number {
    return this.rooms.size;
  }

  addPeer(roomId: SessionId, peer: PeerMembership): boolean {
    const room = this.rooms.get(roomId);
    if (!room) return false;

    room.peers.set(peer.peerId, peer);

    // Update socket index
    if (peer.socketId) {
      this.socketIndex.set(peer.socketId, { roomId, peerId: peer.peerId });
    }

    return true;
  }

  removePeer(roomId: SessionId, peerId: PeerId): PeerMembership | undefined {
    const room = this.rooms.get(roomId);
    if (!room) return undefined;

    const peer = room.peers.get(peerId);
    if (!peer) return undefined;

    // Clean up socket index
    if (peer.socketId) {
      this.socketIndex.delete(peer.socketId);
    }

    room.peers.delete(peerId);
    return peer;
  }

  getPeer(roomId: SessionId, peerId: PeerId): PeerMembership | undefined {
    const room = this.rooms.get(roomId);
    if (!room) return undefined;
    return room.peers.get(peerId);
  }

  findBySocketId(socketId: string): { roomId: SessionId; peerId: PeerId } | undefined {
    return this.socketIndex.get(socketId);
  }

  /**
   * Update the socket index when a peer rebinds (reconnect).
   * Removes old socketId mapping and adds new one.
   */
  updateSocketBinding(
    roomId: SessionId,
    peerId: PeerId,
    oldSocketId: string | null,
    newSocketId: string,
  ): void {
    if (oldSocketId) {
      this.socketIndex.delete(oldSocketId);
    }
    this.socketIndex.set(newSocketId, { roomId, peerId });
  }
}
