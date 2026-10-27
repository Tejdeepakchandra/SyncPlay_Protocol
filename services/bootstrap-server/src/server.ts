/**
 * HTTP + Socket.IO server startup.
 *
 * Wires together config, store, service, app, and gateway.
 * Per BOOTSTRAP_SERVER_LLD §2: single-process coordination service.
 */
import { createServer } from 'node:http';
import { Server as SocketIOServer } from 'socket.io';
import { loadConfig } from './config/env.js';
import { InMemoryRoomStore } from './infrastructure/in-memory-room-store.js';
import { RoomService } from './services/room.service.js';
import { createApp } from './app.js';
import { registerSignalingGateway } from './gateway/signaling.gateway.js';

const config = loadConfig();

// ── Infrastructure ───────────────────────────────────────────
const store = new InMemoryRoomStore();
const roomService = new RoomService(store, config);

// ── Express app ──────────────────────────────────────────────
const app = createApp(roomService);
const httpServer = createServer(app);

// ── Socket.IO ────────────────────────────────────────────────
const io = new SocketIOServer(httpServer, {
  cors: {
    origin: config.ALLOWED_ORIGINS.split(',').map((o) => o.trim()),
    methods: ['GET', 'POST'],
  },
  maxHttpBufferSize: config.MAX_SIGNALING_BYTES,
});

registerSignalingGateway(io, roomService);

// ── Room expiry cleanup (runs every 60s) ─────────────────────
const cleanupInterval = setInterval(() => {
  roomService.cleanupExpiredRooms();
}, 60_000);
cleanupInterval.unref();

// ── Start ────────────────────────────────────────────────────
httpServer.listen(config.PORT, config.HOST, () => {
  console.log(`[bootstrap-server] listening on ${config.HOST}:${config.PORT} (${config.NODE_ENV})`);
  console.log(`[bootstrap-server] max peers per room: ${config.DEFAULT_MAX_PEERS} (absolute max: ${config.ABSOLUTE_MAX_PEERS})`);
});

// ── Graceful shutdown ────────────────────────────────────────
function shutdown() {
  console.log('[bootstrap-server] shutting down...');
  clearInterval(cleanupInterval);
  roomService.dispose();
  io.close();
  httpServer.close(() => {
    console.log('[bootstrap-server] closed');
    process.exit(0);
  });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
