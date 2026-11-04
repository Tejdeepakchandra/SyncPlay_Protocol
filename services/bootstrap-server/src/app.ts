/**
 * Express application factory.
 *
 * Creates the Express app with middleware and routes.
 * Separated from the server for testability.
 */
import express from 'express';
import type { Express, Request, Response, NextFunction } from 'express';
import { createRoomRoutes } from './routes/room.routes.js';
import type { RoomService } from './services/room.service.js';

export interface AppOptions {
  /** Comma-separated allowed CORS origins. Default: '*' */
  readonly allowedOrigins?: string;
}

/**
 * Create and configure the Express application.
 */
export function createApp(roomService: RoomService, options: AppOptions = {}): Express {
  const app = express();

  // ── CORS middleware ────────────────────────────────────
  const origins = options.allowedOrigins
    ? options.allowedOrigins.split(',').map((o) => o.trim())
    : ['*'];

  app.use((_req: Request, res: Response, next: NextFunction) => {
    const requestOrigin = _req.headers.origin;
    if (origins.includes('*') || (requestOrigin && origins.includes(requestOrigin))) {
      res.setHeader('Access-Control-Allow-Origin', requestOrigin ?? '*');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (_req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }
    next();
  });

  // ── Body parsing ───────────────────────────────────────

  app.use(express.json({ limit: '64kb' }));

  // ── Routes ─────────────────────────────────────────────

  app.use('/api/v1', createRoomRoutes(roomService));

  return app;
}
