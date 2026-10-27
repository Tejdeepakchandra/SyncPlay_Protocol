/**
 * Unit tests for InMemoryRoomStore.
 *
 * Tests the storage layer in isolation from business logic.
 * Covers CRUD operations, socket index, and edge cases.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryRoomStore } from '../infrastructure/in-memory-room-store.js';
import type { Room, PeerMembership } from '../models/room.model.js';

function makeRoom(overrides: Partial<Room> = {}): Room {
  return {
    roomId: 'room-001',
    createdAt: Date.now(),
    expiresAt: Date.now() + 7_200_000,
    maxPeers: 4,
    status: 'ACTIVE',
    peers: new Map(),
    ...overrides,
  };
}

function makePeer(overrides: Partial<PeerMembership> = {}): PeerMembership {
  return {
    peerId: 'peer-001',
    role: 'MEMBER',
    joinedAt: Date.now(),
    presence: 'CONNECTED',
    socketId: 'socket-001',
    generation: 1,
    tokenHash: 'hash-abc',
    lastSeenAt: Date.now(),
    ...overrides,
  };
}

describe('InMemoryRoomStore', () => {
  let store: InMemoryRoomStore;

  beforeEach(() => {
    store = new InMemoryRoomStore();
  });

  // ── Room CRUD ────────────────────────────────────────────

  describe('createRoom', () => {
    it('should create a room', () => {
      const room = makeRoom();
      store.createRoom(room);
      expect(store.hasRoom('room-001')).toBe(true);
    });

    it('should throw on duplicate roomId', () => {
      store.createRoom(makeRoom());
      expect(() => store.createRoom(makeRoom())).toThrow('already exists');
    });
  });

  describe('getRoom', () => {
    it('should return the room', () => {
      const room = makeRoom();
      store.createRoom(room);
      expect(store.getRoom('room-001')).toBe(room);
    });

    it('should return undefined for missing room', () => {
      expect(store.getRoom('nonexistent')).toBeUndefined();
    });
  });

  describe('deleteRoom', () => {
    it('should delete a room', () => {
      store.createRoom(makeRoom());
      expect(store.deleteRoom('room-001')).toBe(true);
      expect(store.hasRoom('room-001')).toBe(false);
    });

    it('should return false for missing room', () => {
      expect(store.deleteRoom('nonexistent')).toBe(false);
    });

    it('should clean up socket index when deleting a room with peers', () => {
      const room = makeRoom();
      store.createRoom(room);
      store.addPeer('room-001', makePeer({ socketId: 'sock-1' }));
      store.addPeer('room-001', makePeer({ peerId: 'peer-002', socketId: 'sock-2' }));

      // Socket index should be populated
      expect(store.findBySocketId('sock-1')).toBeDefined();
      expect(store.findBySocketId('sock-2')).toBeDefined();

      store.deleteRoom('room-001');

      // Socket index should be cleaned up
      expect(store.findBySocketId('sock-1')).toBeUndefined();
      expect(store.findBySocketId('sock-2')).toBeUndefined();
    });
  });

  describe('hasRoom', () => {
    it('should return false for empty store', () => {
      expect(store.hasRoom('room-001')).toBe(false);
    });

    it('should return true for existing room', () => {
      store.createRoom(makeRoom());
      expect(store.hasRoom('room-001')).toBe(true);
    });
  });

  describe('getAllRooms', () => {
    it('should yield all rooms', () => {
      store.createRoom(makeRoom({ roomId: 'r1' }));
      store.createRoom(makeRoom({ roomId: 'r2' }));
      store.createRoom(makeRoom({ roomId: 'r3' }));

      const all = [...store.getAllRooms()];
      expect(all).toHaveLength(3);
      expect(all.map((r) => r.roomId).sort()).toEqual(['r1', 'r2', 'r3']);
    });

    it('should yield nothing for empty store', () => {
      expect([...store.getAllRooms()]).toHaveLength(0);
    });
  });

  describe('getRoomCount', () => {
    it('should return 0 for empty store', () => {
      expect(store.getRoomCount()).toBe(0);
    });

    it('should return the count of rooms', () => {
      store.createRoom(makeRoom({ roomId: 'r1' }));
      store.createRoom(makeRoom({ roomId: 'r2' }));
      expect(store.getRoomCount()).toBe(2);
    });

    it('should decrement after delete', () => {
      store.createRoom(makeRoom({ roomId: 'r1' }));
      store.deleteRoom('r1');
      expect(store.getRoomCount()).toBe(0);
    });
  });

  // ── Peer operations ──────────────────────────────────────

  describe('addPeer', () => {
    it('should add a peer to a room', () => {
      store.createRoom(makeRoom());
      expect(store.addPeer('room-001', makePeer())).toBe(true);

      const room = store.getRoom('room-001')!;
      expect(room.peers.size).toBe(1);
      expect(room.peers.get('peer-001')!.peerId).toBe('peer-001');
    });

    it('should return false for missing room', () => {
      expect(store.addPeer('nonexistent', makePeer())).toBe(false);
    });

    it('should update socket index on add', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: 'sock-abc' }));

      const binding = store.findBySocketId('sock-abc');
      expect(binding).toEqual({ roomId: 'room-001', peerId: 'peer-001' });
    });

    it('should not index null socketId', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: null }));
      expect(store.findBySocketId('')).toBeUndefined();
    });
  });

  describe('removePeer', () => {
    it('should remove a peer from a room', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer());

      const removed = store.removePeer('room-001', 'peer-001');
      expect(removed).toBeDefined();
      expect(removed!.peerId).toBe('peer-001');
      expect(store.getRoom('room-001')!.peers.size).toBe(0);
    });

    it('should return undefined for missing room', () => {
      expect(store.removePeer('nonexistent', 'peer-001')).toBeUndefined();
    });

    it('should return undefined for missing peer', () => {
      store.createRoom(makeRoom());
      expect(store.removePeer('room-001', 'nonexistent')).toBeUndefined();
    });

    it('should clean up socket index on remove', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: 'sock-xyz' }));
      expect(store.findBySocketId('sock-xyz')).toBeDefined();

      store.removePeer('room-001', 'peer-001');
      expect(store.findBySocketId('sock-xyz')).toBeUndefined();
    });
  });

  describe('getPeer', () => {
    it('should return the peer', () => {
      store.createRoom(makeRoom());
      const peer = makePeer();
      store.addPeer('room-001', peer);

      expect(store.getPeer('room-001', 'peer-001')).toBe(peer);
    });

    it('should return undefined for missing room', () => {
      expect(store.getPeer('nonexistent', 'peer-001')).toBeUndefined();
    });

    it('should return undefined for missing peer', () => {
      store.createRoom(makeRoom());
      expect(store.getPeer('room-001', 'nonexistent')).toBeUndefined();
    });
  });

  // ── Socket index ─────────────────────────────────────────

  describe('findBySocketId', () => {
    it('should find room and peer by socketId', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: 'sock-123' }));

      expect(store.findBySocketId('sock-123')).toEqual({
        roomId: 'room-001',
        peerId: 'peer-001',
      });
    });

    it('should return undefined for unknown socketId', () => {
      expect(store.findBySocketId('unknown')).toBeUndefined();
    });

    it('should handle multiple peers in different rooms', () => {
      store.createRoom(makeRoom({ roomId: 'r1' }));
      store.createRoom(makeRoom({ roomId: 'r2' }));
      store.addPeer('r1', makePeer({ peerId: 'p1', socketId: 's1' }));
      store.addPeer('r2', makePeer({ peerId: 'p2', socketId: 's2' }));

      expect(store.findBySocketId('s1')).toEqual({ roomId: 'r1', peerId: 'p1' });
      expect(store.findBySocketId('s2')).toEqual({ roomId: 'r2', peerId: 'p2' });
    });
  });

  describe('updateSocketBinding', () => {
    it('should update socket index on rebind', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: 'old-sock' }));

      store.updateSocketBinding('room-001', 'peer-001', 'old-sock', 'new-sock');

      expect(store.findBySocketId('old-sock')).toBeUndefined();
      expect(store.findBySocketId('new-sock')).toEqual({
        roomId: 'room-001',
        peerId: 'peer-001',
      });
    });

    it('should handle null old socketId', () => {
      store.createRoom(makeRoom());
      store.addPeer('room-001', makePeer({ socketId: null }));

      store.updateSocketBinding('room-001', 'peer-001', null, 'new-sock');

      expect(store.findBySocketId('new-sock')).toEqual({
        roomId: 'room-001',
        peerId: 'peer-001',
      });
    });
  });
});
