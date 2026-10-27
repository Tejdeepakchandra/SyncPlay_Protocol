/**
 * Unit tests for RoomService.
 *
 * Tests room lifecycle: create, join, bind, leave, disconnect,
 * capacity enforcement, token validation, and signaling authorization.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { InMemoryRoomStore } from '../infrastructure/in-memory-room-store.js';
import { RoomService, RoomError } from '../services/room.service.js';
import type { ServerConfig } from '../config/env.js';

/** Minimal test config with fast timers. */
function testConfig(overrides: Partial<ServerConfig> = {}): ServerConfig {
  return {
    PORT: 3001,
    HOST: '0.0.0.0',
    DEFAULT_MAX_PEERS: 4,
    ABSOLUTE_MAX_PEERS: 10,
    ROOM_TTL_MS: 3_600_000, // 1 hour
    EMPTY_ROOM_GRACE_MS: 100, // 100ms for fast tests
    DISCONNECTED_GRACE_MS: 100,
    MAX_SIGNALING_BYTES: 65_536,
    ALLOWED_ORIGINS: 'http://localhost:3000',
    NODE_ENV: 'test',
    ...overrides,
  };
}

describe('RoomService', () => {
  let store: InMemoryRoomStore;
  let service: RoomService;

  beforeEach(() => {
    store = new InMemoryRoomStore();
    service = new RoomService(store, testConfig());
  });

  afterEach(() => {
    service.dispose();
  });

  // ── Create ───────────────────────────────────────────────

  describe('createRoom', () => {
    it('should create a room and return credentials', async () => {
      const result = await service.createRoom();

      expect(result.roomId).toBeTruthy();
      expect(result.peerId).toBeTruthy();
      expect(result.membershipToken).toBeTruthy();
      expect(result.membershipToken.length).toBe(32);
      expect(result.maxPeers).toBe(4);
      expect(result.expiresAt).toBeGreaterThan(Date.now());
    });

    it('should create room with custom maxPeers', async () => {
      const result = await service.createRoom(6);
      expect(result.maxPeers).toBe(6);
    });

    it('should cap maxPeers at ABSOLUTE_MAX_PEERS', async () => {
      const result = await service.createRoom(50);
      expect(result.maxPeers).toBe(10);
    });

    it('should use DEFAULT_MAX_PEERS when not specified', async () => {
      const result = await service.createRoom();
      expect(result.maxPeers).toBe(4);
    });

    it('should store the room', async () => {
      const result = await service.createRoom();
      const room = store.getRoom(result.roomId);
      expect(room).toBeDefined();
      expect(room!.status).toBe('ACTIVE');
      expect(room!.peers.size).toBe(1);
    });

    it('should create owner as first peer', async () => {
      const result = await service.createRoom();
      const room = store.getRoom(result.roomId)!;
      const owner = room.peers.get(result.peerId)!;
      expect(owner.role).toBe('OWNER');
      expect(owner.presence).toBe('CONNECTED');
      expect(owner.generation).toBe(1);
    });

    it('should hash the token (not store raw)', async () => {
      const result = await service.createRoom();
      const room = store.getRoom(result.roomId)!;
      const owner = room.peers.get(result.peerId)!;
      // Token hash should NOT equal the raw token
      expect(owner.tokenHash).not.toBe(result.membershipToken);
      expect(owner.tokenHash.length).toBe(64); // SHA-256 hex
    });
  });

  // ── Join ─────────────────────────────────────────────────

  describe('joinRoom', () => {
    it('should add a new peer to an existing room', async () => {
      const room = await service.createRoom();
      const joined = await service.joinRoom(room.roomId);

      expect(joined.roomId).toBe(room.roomId);
      expect(joined.peerId).not.toBe(room.peerId);
      expect(joined.membershipToken).toBeTruthy();
      expect(joined.membershipToken.length).toBe(32);
    });

    it('should throw ROOM_NOT_FOUND for missing room', async () => {
      await expect(service.joinRoom('nonexistent')).rejects.toThrow(RoomError);
      try {
        await service.joinRoom('nonexistent');
      } catch (err) {
        expect((err as RoomError).code).toBe('ROOM_NOT_FOUND');
      }
    });

    it('should throw ROOM_FULL at capacity', async () => {
      const config = testConfig({ DEFAULT_MAX_PEERS: 2, ABSOLUTE_MAX_PEERS: 2 });
      service.dispose();
      service = new RoomService(store, config);

      const room = await service.createRoom(2);
      // Room already has owner (1 peer), join adds second (2 peers = full)
      await service.joinRoom(room.roomId);

      // Third join should fail
      await expect(service.joinRoom(room.roomId)).rejects.toThrow(RoomError);
      try {
        await service.joinRoom(room.roomId);
      } catch (err) {
        expect((err as RoomError).code).toBe('ROOM_FULL');
      }
    });

    it('should throw ROOM_CLOSING for closing room', async () => {
      const room = await service.createRoom();
      const storedRoom = store.getRoom(room.roomId)!;
      storedRoom.status = 'CLOSING';

      await expect(service.joinRoom(room.roomId)).rejects.toThrow(RoomError);
      try {
        await service.joinRoom(room.roomId);
      } catch (err) {
        expect((err as RoomError).code).toBe('ROOM_CLOSING');
      }
    });
  });

  // ── Bind socket ──────────────────────────────────────────

  describe('bindSocket', () => {
    it('should bind socket and return peer snapshot', async () => {
      const room = await service.createRoom();
      const snapshot = await service.bindSocket(
        room.roomId,
        room.peerId,
        room.membershipToken,
        'socket-001',
      );

      expect(snapshot.roomId).toBe(room.roomId);
      expect(snapshot.peers.length).toBe(1);
      expect(snapshot.peers[0]!.peerId).toBe(room.peerId);
    });

    it('should reject invalid token', async () => {
      const room = await service.createRoom();

      await expect(
        service.bindSocket(room.roomId, room.peerId, 'wrong-token', 'socket-001'),
      ).rejects.toThrow(RoomError);

      try {
        await service.bindSocket(room.roomId, room.peerId, 'wrong-token', 'socket-001');
      } catch (err) {
        expect((err as RoomError).code).toBe('INVALID_CREDENTIALS');
      }
    });

    it('should increment generation on rebind', async () => {
      const room = await service.createRoom();

      // First bind
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-001');
      expect(store.getPeer(room.roomId, room.peerId)!.generation).toBe(1);

      // Rebind (reconnect)
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-002');
      expect(store.getPeer(room.roomId, room.peerId)!.generation).toBe(2);
    });

    it('should update socket index on rebind', async () => {
      const room = await service.createRoom();

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-001');
      expect(store.findBySocketId('socket-001')).toBeDefined();

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-002');
      expect(store.findBySocketId('socket-001')).toBeUndefined();
      expect(store.findBySocketId('socket-002')).toBeDefined();
    });

    it('should throw ROOM_NOT_FOUND for missing room', async () => {
      await expect(
        service.bindSocket('nonexistent', 'peer-1', 'token', 'socket-1'),
      ).rejects.toThrow(RoomError);
    });

    it('should throw PEER_NOT_FOUND for missing peer', async () => {
      const room = await service.createRoom();

      await expect(
        service.bindSocket(room.roomId, 'nonexistent', 'token', 'socket-1'),
      ).rejects.toThrow(RoomError);
    });
  });

  // ── Leave ────────────────────────────────────────────────

  describe('leavePeer', () => {
    it('should remove peer from room', async () => {
      const room = await service.createRoom();
      service.leavePeer(room.roomId, room.peerId);

      expect(store.getPeer(room.roomId, room.peerId)).toBeUndefined();
    });

    it('should not throw for missing peer', () => {
      expect(() => service.leavePeer('nonexistent', 'peer-1')).not.toThrow();
    });
  });

  // ── Disconnect ───────────────────────────────────────────

  describe('handleDisconnect', () => {
    it('should mark peer as DISCONNECTED', async () => {
      const room = await service.createRoom();
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-001');

      const result = service.handleDisconnect('socket-001');
      expect(result).toEqual({ roomId: room.roomId, peerId: room.peerId });

      const peer = store.getPeer(room.roomId, room.peerId)!;
      expect(peer.presence).toBe('DISCONNECTED');
      expect(peer.socketId).toBeNull();
    });

    it('should return undefined for unknown socket', () => {
      expect(service.handleDisconnect('unknown')).toBeUndefined();
    });

    it('should remove peer after grace period', async () => {
      const room = await service.createRoom();
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-001');

      service.handleDisconnect('socket-001');

      // Wait for disconnect grace period (100ms in test config)
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(store.getPeer(room.roomId, room.peerId)).toBeUndefined();
    });

    it('should cancel grace timer on reconnect', async () => {
      const room = await service.createRoom();
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-001');

      service.handleDisconnect('socket-001');

      // Reconnect before grace period expires
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-002');

      // Wait past the grace period
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Peer should still exist (reconnect cancelled the timer)
      expect(store.getPeer(room.roomId, room.peerId)).toBeDefined();
      expect(store.getPeer(room.roomId, room.peerId)!.presence).toBe('CONNECTED');
    });
  });

  // ── Room info ────────────────────────────────────────────

  describe('getRoomInfo', () => {
    it('should return safe room info', async () => {
      const room = await service.createRoom();
      const info = service.getRoomInfo(room.roomId);

      expect(info).toBeDefined();
      expect(info!.roomId).toBe(room.roomId);
      expect(info!.status).toBe('ACTIVE');
      expect(info!.peerCount).toBe(1);
      expect(info!.maxPeers).toBe(4);
    });

    it('should return undefined for missing room', () => {
      expect(service.getRoomInfo('nonexistent')).toBeUndefined();
    });

    it('should not expose token or socketId', async () => {
      const room = await service.createRoom();
      const info = service.getRoomInfo(room.roomId)!;

      // SafeRoomInfo should only have roomId, status, peerCount, maxPeers
      const keys = Object.keys(info);
      expect(keys).not.toContain('membershipToken');
      expect(keys).not.toContain('socketId');
      expect(keys).not.toContain('tokenHash');
    });
  });

  // ── Signaling authorization ──────────────────────────────

  describe('authorizeSignaling', () => {
    it('should authorize valid signaling between peers', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      const auth = service.authorizeSignaling('socket-1', peer2.peerId);
      expect(auth.roomId).toBe(room.roomId);
      expect(auth.fromPeerId).toBe(room.peerId);
      expect(auth.targetSocketId).toBe('socket-2');
    });

    it('should throw for unbound socket', () => {
      expect(() => service.authorizeSignaling('unknown', 'peer-1')).toThrow(RoomError);
    });

    it('should throw for self-signaling', async () => {
      const room = await service.createRoom();
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');

      expect(() => service.authorizeSignaling('socket-1', room.peerId)).toThrow(RoomError);
      try {
        service.authorizeSignaling('socket-1', room.peerId);
      } catch (err) {
        expect((err as RoomError).code).toBe('INVALID_REQUEST');
      }
    });

    it('should throw for target not in room', async () => {
      const room = await service.createRoom();
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');

      expect(() => service.authorizeSignaling('socket-1', 'nonexistent')).toThrow(RoomError);
    });

    it('should throw for disconnected target', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      // peer2 never binds a socket — socketId remains null

      expect(() => service.authorizeSignaling('socket-1', peer2.peerId)).toThrow(RoomError);
    });

    it('should return senderGeneration in authorization result', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      const auth = service.authorizeSignaling('socket-1', peer2.peerId);
      expect(auth.senderGeneration).toBe(1);
    });

    it('should return incremented generation after reconnect', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      // Reconnect peer1
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1b');

      const auth = service.authorizeSignaling('socket-1b', peer2.peerId);
      expect(auth.senderGeneration).toBe(2);
    });

    it('should reject stale declared generation', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      // Reconnect peer1 → generation goes to 2
      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1b');

      // Try signaling with stale generation 1
      expect(() => service.authorizeSignaling('socket-1b', peer2.peerId, 1)).toThrow(RoomError);
      try {
        service.authorizeSignaling('socket-1b', peer2.peerId, 1);
      } catch (err) {
        expect((err as RoomError).code).toBe('STALE_GENERATION');
      }
    });

    it('should accept matching declared generation', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      // Declare correct generation
      const auth = service.authorizeSignaling('socket-1', peer2.peerId, 1);
      expect(auth.senderGeneration).toBe(1);
    });

    it('should accept when no generation is declared (backwards compatible)', async () => {
      const room = await service.createRoom();
      const peer2 = await service.joinRoom(room.roomId);

      await service.bindSocket(room.roomId, room.peerId, room.membershipToken, 'socket-1');
      await service.bindSocket(room.roomId, peer2.peerId, peer2.membershipToken, 'socket-2');

      // No generation declared — should still work
      const auth = service.authorizeSignaling('socket-1', peer2.peerId);
      expect(auth.senderGeneration).toBe(1);
    });
  });

  // ── Room expiry cleanup ──────────────────────────────────

  describe('cleanupExpiredRooms', () => {
    it('should delete expired rooms', async () => {
      const config = testConfig({ ROOM_TTL_MS: 1 }); // 1ms TTL
      service.dispose();
      service = new RoomService(store, config);

      const room = await service.createRoom();

      // Wait for room to expire
      await new Promise((resolve) => setTimeout(resolve, 10));

      const cleaned = service.cleanupExpiredRooms();
      expect(cleaned).toBe(1);
      expect(store.hasRoom(room.roomId)).toBe(false);
    });

    it('should not delete active rooms', async () => {
      await service.createRoom();
      const cleaned = service.cleanupExpiredRooms();
      expect(cleaned).toBe(0);
    });
  });

  // ── Empty room grace ─────────────────────────────────────

  describe('empty room grace timer', () => {
    it('should delete room after all peers leave and grace expires', async () => {
      const room = await service.createRoom();
      service.leavePeer(room.roomId, room.peerId);

      // Room should still exist during grace period
      expect(store.hasRoom(room.roomId)).toBe(true);

      // Wait for grace period (100ms)
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(store.hasRoom(room.roomId)).toBe(false);
    });

    it('should cancel grace timer when new peer joins', async () => {
      const room = await service.createRoom();
      service.leavePeer(room.roomId, room.peerId);

      // Join before grace expires
      await service.joinRoom(room.roomId);

      // Wait past grace period
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Room should still exist
      expect(store.hasRoom(room.roomId)).toBe(true);
    });
  });
});
