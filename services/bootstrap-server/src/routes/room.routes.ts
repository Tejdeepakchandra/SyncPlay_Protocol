/**
 * HTTP API routes for room management.
 *
 * Per BOOTSTRAP_SERVER_LLD §5:
 * - POST /api/v1/rooms — create room
 * - GET /api/v1/rooms/:roomId — safe room metadata
 * - GET /api/v1/health — liveness/readiness
 *
 * Error body: { error: { code, message, requestId } }
 */
import { Router } from 'express';
import type { Request, Response } from 'express';
import { nanoid } from 'nanoid';
import { ZodError } from 'zod';
import { CreateRoomSchema, RoomParamsSchema } from './room.schemas.js';
import { RoomService, RoomError } from '../services/room.service.js';

/**
 * Map RoomError codes to HTTP status codes.
 */
function errorToStatus(code: string): number {
  switch (code) {
    case 'INVALID_REQUEST': return 400;
    case 'INVALID_CREDENTIALS': return 401;
    case 'ROOM_NOT_FOUND': return 404;
    case 'ROOM_FULL': return 409;
    case 'ROOM_CLOSING': return 409;
    case 'DUPLICATE_JOIN': return 409;
    case 'STALE_GENERATION': return 409;
    case 'PEER_NOT_FOUND': return 404;
    default: return 500;
  }
}

/**
 * Build a consistent error response body.
 */
function errorBody(code: string, message: string, requestId: string) {
  return { error: { code, message, requestId } };
}

/**
 * Create room routes.
 */
export function createRoomRoutes(roomService: RoomService): Router {
  const router = Router();

  // ── POST /api/v1/rooms ─────────────────────────────────

  router.post('/rooms', async (req: Request, res: Response) => {
    const requestId = nanoid(8);

    try {
      const body = CreateRoomSchema.parse(req.body ?? {});
      const result = await roomService.createRoom(body.maxPeers);

      res.status(201).json({
        roomId: result.roomId,
        peerId: result.peerId,
        membershipToken: result.membershipToken,
        expiresAt: result.expiresAt,
        maxPeers: result.maxPeers,
      });
    } catch (err) {
      if (err instanceof ZodError) {
        res.status(400).json(errorBody('INVALID_REQUEST', err.message, requestId));
        return;
      }
      if (err instanceof RoomError) {
        res.status(errorToStatus(err.code)).json(errorBody(err.code, err.message, requestId));
        return;
      }
      res.status(500).json(errorBody('INTERNAL_ERROR', 'Internal server error', requestId));
    }
  });

  // ── GET /api/v1/rooms/:roomId ──────────────────────────

  router.get('/rooms/:roomId', (req: Request, res: Response) => {
    const requestId = nanoid(8);

    try {
      const params = RoomParamsSchema.parse(req.params);
      const info = roomService.getRoomInfo(params.roomId);

      if (!info) {
        res.status(404).json(errorBody('ROOM_NOT_FOUND', `Room ${params.roomId} not found`, requestId));
        return;
      }

      res.status(200).json(info);
    } catch (err) {
      if (err instanceof ZodError) {
        res.status(400).json(errorBody('INVALID_REQUEST', err.message, requestId));
        return;
      }
      res.status(500).json(errorBody('INTERNAL_ERROR', 'Internal server error', requestId));
    }
  });

  // ── GET /api/v1/health ─────────────────────────────────

  router.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  });

  return router;
}
