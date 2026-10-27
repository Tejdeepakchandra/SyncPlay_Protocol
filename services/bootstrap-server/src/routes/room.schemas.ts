/**
 * Zod schemas for HTTP request/response validation.
 *
 * Per BOOTSTRAP_SERVER_LLD §5, error responses use:
 * { error: { code, message, requestId } }
 */
import { z } from 'zod';

/** POST /api/v1/rooms request body. */
export const CreateRoomSchema = z.strictObject({
  maxPeers: z.number().int().min(2).max(50).optional(),
});

/** GET /api/v1/rooms/:roomId params. */
export const RoomParamsSchema = z.object({
  roomId: z.string().min(1).max(128),
});
