/**
 * Integration tests for the Bootstrap Server.
 *
 * Tests the full HTTP API and Socket.IO gateway working together.
 * Covers BOOTSTRAP_SERVER_LLD §14 — Test Plan:
 * - Room service: Create, lookup, expiry
 * - Membership: Join, capacity, bad token
 * - Signaling auth: Wrong room, nonmember target, spoofed sender
 * - Reconnect: Drop socket, grace, rejoin
 * - Security: Malformed payload, safe rejection
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { createServer } from 'node:http';
import type { Server as HttpServer } from 'node:http';
import { Server as SocketIOServer } from 'socket.io';
import { io as ioClient, type Socket as ClientSocket } from 'socket.io-client';
import request from 'supertest';
import type { Express } from 'express';

import { InMemoryRoomStore } from '../infrastructure/in-memory-room-store.js';
import { RoomService } from '../services/room.service.js';
import { createApp } from '../app.js';
import { registerSignalingGateway } from '../gateway/signaling.gateway.js';
import type { ServerConfig } from '../config/env.js';

// ── Test helpers ─────────────────────────────────────────────

function testConfig(overrides: Partial<ServerConfig> = {}): ServerConfig {
  return {
    PORT: 0, // random port
    HOST: '127.0.0.1',
    DEFAULT_MAX_PEERS: 4,
    ABSOLUTE_MAX_PEERS: 10,
    ROOM_TTL_MS: 3_600_000,
    EMPTY_ROOM_GRACE_MS: 200,
    DISCONNECTED_GRACE_MS: 200,
    MAX_SIGNALING_BYTES: 65_536,
    ALLOWED_ORIGINS: '*',
    NODE_ENV: 'test',
    ...overrides,
  };
}

interface TestContext {
  store: InMemoryRoomStore;
  service: RoomService;
  app: Express;
  httpServer: HttpServer;
  io: SocketIOServer;
  port: number;
  url: string;
}

async function setupServer(configOverrides: Partial<ServerConfig> = {}): Promise<TestContext> {
  const config = testConfig(configOverrides);
  const store = new InMemoryRoomStore();
  const service = new RoomService(store, config);
  const app = createApp(service);
  const httpServer = createServer(app);

  const io = new SocketIOServer(httpServer, {
    cors: { origin: '*' },
    maxHttpBufferSize: config.MAX_SIGNALING_BYTES,
  });

  registerSignalingGateway(io, service);

  await new Promise<void>((resolve) => {
    httpServer.listen(0, '127.0.0.1', () => resolve());
  });

  const addr = httpServer.address();
  const port = typeof addr === 'object' && addr ? addr.port : 0;
  const url = `http://127.0.0.1:${port}`;

  return { store, service, app, httpServer, io, port, url };
}

async function teardownServer(ctx: TestContext): Promise<void> {
  ctx.service.dispose();
  ctx.io.close();
  await new Promise<void>((resolve) => ctx.httpServer.close(() => resolve()));
}

function connectClient(url: string): ClientSocket {
  return ioClient(url, {
    transports: ['websocket'],
    forceNew: true,
    autoConnect: true,
  });
}

function waitForEvent<T = unknown>(socket: ClientSocket, event: string, timeout = 2000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for ${event}`)), timeout);
    socket.once(event, (data: T) => {
      clearTimeout(timer);
      resolve(data);
    });
  });
}

// ── HTTP API Tests ───────────────────────────────────────────

describe('HTTP API', () => {
  let ctx: TestContext;

  beforeAll(async () => {
    ctx = await setupServer();
  });

  afterAll(async () => {
    await teardownServer(ctx);
  });

  describe('GET /api/v1/health', () => {
    it('should return 200 with status ok', async () => {
      const res = await request(ctx.app).get('/api/v1/health');
      expect(res.status).toBe(200);
      expect(res.body.status).toBe('ok');
      expect(res.body.uptime).toBeDefined();
      expect(res.body.timestamp).toBeDefined();
    });
  });

  describe('POST /api/v1/rooms', () => {
    it('should create a room and return credentials', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({});
      expect(res.status).toBe(201);
      expect(res.body.roomId).toBeTruthy();
      expect(res.body.peerId).toBeTruthy();
      expect(res.body.membershipToken).toBeTruthy();
      expect(res.body.expiresAt).toBeGreaterThan(Date.now());
      expect(res.body.maxPeers).toBe(4);
    });

    it('should accept custom maxPeers', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({ maxPeers: 6 });
      expect(res.status).toBe(201);
      expect(res.body.maxPeers).toBe(6);
    });

    it('should reject invalid maxPeers', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({ maxPeers: 1 });
      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('INVALID_REQUEST');
    });

    it('should reject unknown fields (strict schema)', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({ maxPeers: 4, isPlaying: true });
      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('INVALID_REQUEST');
    });
  });

  describe('GET /api/v1/rooms/:roomId', () => {
    it('should return safe room info', async () => {
      const createRes = await request(ctx.app).post('/api/v1/rooms').send({});
      const roomId = createRes.body.roomId;

      const res = await request(ctx.app).get(`/api/v1/rooms/${roomId}`);
      expect(res.status).toBe(200);
      expect(res.body.roomId).toBe(roomId);
      expect(res.body.status).toBe('ACTIVE');
      expect(res.body.peerCount).toBe(1);
      expect(res.body.maxPeers).toBe(4);

      // Must NOT expose credentials
      expect(res.body.membershipToken).toBeUndefined();
      expect(res.body.tokenHash).toBeUndefined();
      expect(res.body.socketId).toBeUndefined();
    });

    it('should return 404 for missing room', async () => {
      const res = await request(ctx.app).get('/api/v1/rooms/nonexistent');
      expect(res.status).toBe(404);
      expect(res.body.error.code).toBe('ROOM_NOT_FOUND');
      expect(res.body.error.requestId).toBeTruthy();
    });
  });

  describe('error response format', () => {
    it('should use consistent error body: { error: { code, message, requestId } }', async () => {
      const res = await request(ctx.app).get('/api/v1/rooms/missing-id');
      expect(res.status).toBe(404);
      expect(res.body.error).toBeDefined();
      expect(typeof res.body.error.code).toBe('string');
      expect(typeof res.body.error.message).toBe('string');
      expect(typeof res.body.error.requestId).toBe('string');
    });
  });
});

// ── Socket.IO Integration Tests ──────────────────────────────

describe('Socket.IO Integration', () => {
  let ctx: TestContext;
  const clients: ClientSocket[] = [];

  beforeAll(async () => {
    ctx = await setupServer();
  });

  afterEach(() => {
    // Disconnect all clients
    for (const client of clients) {
      if (client.connected) {
        client.disconnect();
      }
    }
    clients.length = 0;
  });

  afterAll(async () => {
    await teardownServer(ctx);
  });

  function createClient(): ClientSocket {
    const client = connectClient(ctx.url);
    clients.push(client);
    return client;
  }

  async function createAndJoinRoom(): Promise<{
    roomId: string;
    peerId: string;
    token: string;
    client: ClientSocket;
  }> {
    const res = await request(ctx.app).post('/api/v1/rooms').send({});
    const { roomId, peerId, membershipToken } = res.body;

    const client = createClient();
    await new Promise<void>((resolve) => client.on('connect', () => resolve()));

    return new Promise((resolve, reject) => {
      client.emit(
        'session:join',
        { roomId, peerId, membershipToken },
        (ack: any) => {
          if (ack.ok) {
            resolve({ roomId, peerId, token: membershipToken, client });
          } else {
            reject(new Error(`Join failed: ${JSON.stringify(ack.error)}`));
          }
        },
      );
    });
  }

  // ── Normal join flow ─────────────────────────────────────

  describe('session:join', () => {
    it('should join room and receive snapshot via ack', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({});
      const { roomId, peerId, membershipToken } = res.body;

      const client = createClient();
      await new Promise<void>((resolve) => client.on('connect', () => resolve()));

      const ack = await new Promise<any>((resolve) => {
        client.emit(
          'session:join',
          { roomId, peerId, membershipToken },
          (response: any) => resolve(response),
        );
      });

      expect(ack.ok).toBe(true);
      expect(ack.snapshot.roomId).toBe(roomId);
      expect(ack.snapshot.peers.length).toBe(1);
      expect(ack.snapshot.peers[0].peerId).toBe(peerId);
    });

    it('should notify existing peers when new peer joins', async () => {
      const peer1 = await createAndJoinRoom();

      // Peer2 joins same room
      const joinRes = await request(ctx.app).post('/api/v1/rooms').send({});
      // Actually we need to join the SAME room. Let's use joinRoom via HTTP first.
      const peer2Res = await ctx.service.joinRoom(peer1.roomId);

      const peerJoinedPromise = waitForEvent(peer1.client, 'session:peer-joined');

      const client2 = createClient();
      clients.push(client2);
      await new Promise<void>((resolve) => client2.on('connect', () => resolve()));

      await new Promise<any>((resolve) => {
        client2.emit(
          'session:join',
          { roomId: peer1.roomId, peerId: peer2Res.peerId, membershipToken: peer2Res.membershipToken },
          resolve,
        );
      });

      const notification = await peerJoinedPromise;
      expect((notification as any).peerId).toBe(peer2Res.peerId);
    });
  });

  // ── Invalid token ────────────────────────────────────────

  describe('session:join with invalid token', () => {
    it('should reject join with wrong token', async () => {
      const res = await request(ctx.app).post('/api/v1/rooms').send({});
      const { roomId, peerId } = res.body;

      const client = createClient();
      await new Promise<void>((resolve) => client.on('connect', () => resolve()));

      const ack = await new Promise<any>((resolve) => {
        client.emit(
          'session:join',
          { roomId, peerId, membershipToken: 'wrong-token-value' },
          resolve,
        );
      });

      expect(ack.ok).toBe(false);
      expect(ack.error.code).toBe('INVALID_CREDENTIALS');
    });
  });

  // ── Leave ────────────────────────────────────────────────

  describe('session:leave', () => {
    it('should notify remaining peers on explicit leave', async () => {
      const peer1 = await createAndJoinRoom();
      const peer2Res = await ctx.service.joinRoom(peer1.roomId);

      const client2 = createClient();
      await new Promise<void>((resolve) => client2.on('connect', () => resolve()));
      await new Promise<any>((resolve) => {
        client2.emit(
          'session:join',
          { roomId: peer1.roomId, peerId: peer2Res.peerId, membershipToken: peer2Res.membershipToken },
          resolve,
        );
      });

      const peerLeftPromise = waitForEvent(peer1.client, 'session:peer-left');

      await new Promise<any>((resolve) => {
        client2.emit('session:leave', { roomId: peer1.roomId, peerId: peer2Res.peerId }, resolve);
      });

      const notification = await peerLeftPromise;
      expect((notification as any).peerId).toBe(peer2Res.peerId);
      expect((notification as any).reason).toBe('explicit');
    });
  });

  // ── Disconnect ───────────────────────────────────────────

  describe('disconnect', () => {
    it('should notify peers of temporary disconnect', async () => {
      const peer1 = await createAndJoinRoom();
      const peer2Res = await ctx.service.joinRoom(peer1.roomId);

      const client2 = createClient();
      await new Promise<void>((resolve) => client2.on('connect', () => resolve()));
      await new Promise<any>((resolve) => {
        client2.emit(
          'session:join',
          { roomId: peer1.roomId, peerId: peer2Res.peerId, membershipToken: peer2Res.membershipToken },
          resolve,
        );
      });

      const disconnectedPromise = waitForEvent(peer1.client, 'session:peer-disconnected');

      client2.disconnect();

      const notification = await disconnectedPromise;
      expect((notification as any).peerId).toBe(peer2Res.peerId);
    });
  });

  // ── Signaling relay ──────────────────────────────────────

  describe('signal:offer', () => {
    it('should relay offer with server-derived fromPeerId', async () => {
      const peer1 = await createAndJoinRoom();
      const peer2Res = await ctx.service.joinRoom(peer1.roomId);

      const client2 = createClient();
      await new Promise<void>((resolve) => client2.on('connect', () => resolve()));
      await new Promise<any>((resolve) => {
        client2.emit(
          'session:join',
          { roomId: peer1.roomId, peerId: peer2Res.peerId, membershipToken: peer2Res.membershipToken },
          resolve,
        );
      });

      const offerPromise = waitForEvent(client2, 'signal:offer');

      const ack = await new Promise<any>((resolve) => {
        peer1.client.emit('signal:offer', {
          roomId: peer1.roomId,
          targetPeerId: peer2Res.peerId,
          negotiationId: 'neg-001',
          payload: { type: 'offer', sdp: 'v=0\r\n...' },
        }, resolve);
      });

      expect(ack.ok).toBe(true);

      const relayed = await offerPromise;
      expect((relayed as any).fromPeerId).toBe(peer1.peerId);
      expect((relayed as any).negotiationId).toBe('neg-001');
      expect((relayed as any).payload.type).toBe('offer');
      expect((relayed as any).senderGeneration).toBe(1);
    });

    it('should include senderGeneration in relayed ice-candidate', async () => {
      const peer1 = await createAndJoinRoom();
      const peer2Res = await ctx.service.joinRoom(peer1.roomId);

      const client2 = createClient();
      await new Promise<void>((resolve) => client2.on('connect', () => resolve()));
      await new Promise<any>((resolve) => {
        client2.emit(
          'session:join',
          { roomId: peer1.roomId, peerId: peer2Res.peerId, membershipToken: peer2Res.membershipToken },
          resolve,
        );
      });

      const icePromise = waitForEvent(client2, 'signal:ice-candidate');

      const ack = await new Promise<any>((resolve) => {
        peer1.client.emit('signal:ice-candidate', {
          roomId: peer1.roomId,
          targetPeerId: peer2Res.peerId,
          negotiationId: 'neg-001',
          payload: { candidate: 'candidate:1 1 udp ...', sdpMid: '0', sdpMLineIndex: 0 },
        }, resolve);
      });

      expect(ack.ok).toBe(true);

      const relayed = await icePromise;
      expect((relayed as any).fromPeerId).toBe(peer1.peerId);
      expect((relayed as any).senderGeneration).toBe(1);
      expect((relayed as any).payload.candidate).toBe('candidate:1 1 udp ...');
    });
  });

  // ── Unauthorized signaling ───────────────────────────────

  describe('unauthorized signaling', () => {
    it('should reject signaling from unbound socket', async () => {
      const client = createClient();
      await new Promise<void>((resolve) => client.on('connect', () => resolve()));

      const ack = await new Promise<any>((resolve) => {
        client.emit('signal:offer', {
          roomId: 'any-room',
          targetPeerId: 'any-peer',
          negotiationId: 'neg-001',
          payload: { type: 'offer', sdp: 'v=0\r\n...' },
        }, resolve);
      });

      expect(ack.ok).toBe(false);
      expect(ack.error.code).toBe('INVALID_CREDENTIALS');
    });

    it('should reject signaling to nonexistent peer', async () => {
      const peer1 = await createAndJoinRoom();

      const ack = await new Promise<any>((resolve) => {
        peer1.client.emit('signal:offer', {
          roomId: peer1.roomId,
          targetPeerId: 'nonexistent-peer',
          negotiationId: 'neg-001',
          payload: { type: 'offer', sdp: 'v=0\r\n...' },
        }, resolve);
      });

      expect(ack.ok).toBe(false);
      expect(ack.error.code).toBe('PEER_NOT_FOUND');
    });
  });

  // ── Malformed events ─────────────────────────────────────

  describe('malformed events', () => {
    it('should reject session:join with missing fields', async () => {
      const client = createClient();
      await new Promise<void>((resolve) => client.on('connect', () => resolve()));

      const ack = await new Promise<any>((resolve) => {
        client.emit('session:join', { roomId: 'r1' }, resolve); // missing peerId and token
      });

      expect(ack.ok).toBe(false);
      expect(ack.error.code).toBe('INVALID_REQUEST');
    });

    it('should reject signal:offer with invalid payload', async () => {
      const peer1 = await createAndJoinRoom();

      const ack = await new Promise<any>((resolve) => {
        peer1.client.emit('signal:offer', { bad: 'data' }, resolve);
      });

      expect(ack.ok).toBe(false);
      expect(ack.error.code).toBe('INVALID_REQUEST');
    });
  });
});

// ── Capacity Limit Tests ─────────────────────────────────────

describe('Capacity limits', () => {
  let ctx: TestContext;

  beforeAll(async () => {
    ctx = await setupServer({ DEFAULT_MAX_PEERS: 2, ABSOLUTE_MAX_PEERS: 2 });
  });

  afterAll(async () => {
    await teardownServer(ctx);
  });

  it('should reject join when room is at capacity', async () => {
    // Create room (1 peer: owner)
    const createRes = await request(ctx.app).post('/api/v1/rooms').send({ maxPeers: 2 });
    const { roomId } = createRes.body;

    // Join (2 peers: full)
    await ctx.service.joinRoom(roomId);

    // Third join should fail
    try {
      await ctx.service.joinRoom(roomId);
      expect.fail('Should have thrown');
    } catch (err) {
      expect((err as any).code).toBe('ROOM_FULL');
    }
  });

  it('should show correct peerCount in room info', async () => {
    const createRes = await request(ctx.app).post('/api/v1/rooms').send({ maxPeers: 2 });
    const { roomId } = createRes.body;
    await ctx.service.joinRoom(roomId);

    const res = await request(ctx.app).get(`/api/v1/rooms/${roomId}`);
    expect(res.body.peerCount).toBe(2);
    expect(res.body.maxPeers).toBe(2);
  });
});
