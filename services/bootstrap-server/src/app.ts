/**
 * Express application factory.
 *
 * Creates the Express app with middleware and routes.
 * Separated from the server for testability.
 */
import express from 'express';
import type { Express } from 'express';
import { createRoomRoutes } from './routes/room.routes.js';
import type { RoomService } from './services/room.service.js';

/**
 * Create and configure the Express application.
 */
export function createApp(roomService: RoomService): Express {
  const app = express();

  // ── Middleware ──────────────────────────────────────────

  app.use(express.json({ limit: '64kb' }));

  // ── Routes ─────────────────────────────────────────────

  app.use('/api/v1', createRoomRoutes(roomService));

  return app;
}
