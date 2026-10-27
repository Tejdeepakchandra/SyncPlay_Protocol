/**
 * Zod schemas for Socket.IO event validation.
 *
 * Per BOOTSTRAP_SERVER_LLD §6 and §7:
 * Every client command is validated and acknowledged.
 */
import { z } from 'zod';

/** session:join event payload (C → S). */
export const SessionJoinSchema = z.strictObject({
  roomId: z.string().min(1).max(128),
  peerId: z.string().min(1).max(128),
  membershipToken: z.string().min(1).max(256),
});

/** session:leave event payload (C → S). */
export const SessionLeaveSchema = z.strictObject({
  roomId: z.string().min(1).max(128),
  peerId: z.string().min(1).max(128),
});

/** signal:offer / signal:answer event payload (C → S). */
export const SignalOfferAnswerSchema = z.strictObject({
  roomId: z.string().min(1).max(128),
  targetPeerId: z.string().min(1).max(128),
  negotiationId: z.string().min(1).max(128),
  payload: z.object({
    type: z.enum(['offer', 'answer']),
    sdp: z.string().min(1).max(65536),
  }),
});

/** signal:ice-candidate event payload (C → S). */
export const SignalIceCandidateSchema = z.strictObject({
  roomId: z.string().min(1).max(128),
  targetPeerId: z.string().min(1).max(128),
  negotiationId: z.string().min(1).max(128),
  payload: z.union([
    z.object({
      candidate: z.string(),
      sdpMid: z.string().nullable().optional(),
      sdpMLineIndex: z.number().int().nonnegative().nullable().optional(),
      usernameFragment: z.string().nullable().optional(),
    }),
    z.object({ done: z.literal(true) }), // end-of-candidates marker
  ]),
});

export type SessionJoinPayload = z.infer<typeof SessionJoinSchema>;
export type SessionLeavePayload = z.infer<typeof SessionLeaveSchema>;
export type SignalOfferAnswerPayload = z.infer<typeof SignalOfferAnswerSchema>;
export type SignalIceCandidatePayload = z.infer<typeof SignalIceCandidateSchema>;
