/**
 * Socket.IO signaling gateway.
 *
 * Per BOOTSTRAP_SERVER_LLD §6 — Socket.IO Event Contract:
 * - session:join — validate token, bind membership, return snapshot
 * - session:leave — explicitly leave, clean resources
 * - signal:offer / signal:answer — targeted SDP relay
 * - signal:ice-candidate — candidate / end-of-candidates relay
 * - disconnect — mark DISCONNECTED, start grace timer
 *
 * Per BOOTSTRAP_SERVER_LLD §7:
 * - Never trust client-supplied fromPeerId; derive from socket binding
 * - Never broadcast SDP/ICE to all members
 * - Validate room membership and generation
 */
import type { Server as SocketIOServer, Socket } from 'socket.io';
import { ZodError } from 'zod';
import { RoomService, RoomError } from '../services/room.service.js';
import {
  SessionJoinSchema,
  SessionLeaveSchema,
  SignalOfferAnswerSchema,
  SignalIceCandidateSchema,
} from './signaling.schemas.js';

/** Structured error for session:error event. */
interface SessionError {
  code: string;
  message: string;
}

/**
 * Register Socket.IO event handlers on a server instance.
 */
export function registerSignalingGateway(
  io: SocketIOServer,
  roomService: RoomService,
): void {
  io.on('connection', (socket: Socket) => {

    // ── session:join ───────────────────────────────────────

    socket.on('session:join', async (data: unknown, ack?: (response: unknown) => void) => {
      try {
        const { roomId, peerId, membershipToken } = SessionJoinSchema.parse(data);

        const snapshot = await roomService.bindSocket(roomId, peerId, membershipToken, socket.id);

        // Join the Socket.IO room for broadcast
        await socket.join(roomId);

        // Acknowledge with snapshot
        if (typeof ack === 'function') {
          ack({ ok: true, snapshot });
        }
        socket.emit('session:joined', snapshot);

        // Notify other peers in the room
        socket.to(roomId).emit('session:peer-joined', {
          peerId,
          role: snapshot.peers.find((p) => p.peerId === peerId)?.role ?? 'MEMBER',
        });
      } catch (err) {
        const error = toSessionError(err);
        if (typeof ack === 'function') {
          ack({ ok: false, error });
        }
        socket.emit('session:error', error);
      }
    });

    // ── session:leave ──────────────────────────────────────

    socket.on('session:leave', (data: unknown, ack?: (response: unknown) => void) => {
      try {
        const { roomId, peerId } = SessionLeaveSchema.parse(data);

        roomService.leavePeer(roomId, peerId);

        socket.leave(roomId);

        if (typeof ack === 'function') {
          ack({ ok: true });
        }

        // Notify remaining peers
        socket.to(roomId).emit('session:peer-left', { peerId, reason: 'explicit' });
      } catch (err) {
        const error = toSessionError(err);
        if (typeof ack === 'function') {
          ack({ ok: false, error });
        }
        socket.emit('session:error', error);
      }
    });

    // ── signal:offer ───────────────────────────────────────

    socket.on('signal:offer', (data: unknown, ack?: (response: unknown) => void) => {
      try {
        const parsed = SignalOfferAnswerSchema.parse(data);
        relaySignal(socket, roomService, 'signal:offer', parsed, ack);
      } catch (err) {
        const error = toSessionError(err);
        if (typeof ack === 'function') {
          ack({ ok: false, error });
        }
        socket.emit('session:error', error);
      }
    });

    // ── signal:answer ──────────────────────────────────────

    socket.on('signal:answer', (data: unknown, ack?: (response: unknown) => void) => {
      try {
        const parsed = SignalOfferAnswerSchema.parse(data);
        relaySignal(socket, roomService, 'signal:answer', parsed, ack);
      } catch (err) {
        const error = toSessionError(err);
        if (typeof ack === 'function') {
          ack({ ok: false, error });
        }
        socket.emit('session:error', error);
      }
    });

    // ── signal:ice-candidate ───────────────────────────────

    socket.on('signal:ice-candidate', (data: unknown, ack?: (response: unknown) => void) => {
      try {
        const parsed = SignalIceCandidateSchema.parse(data);
        relayIceCandidate(socket, roomService, parsed, ack);
      } catch (err) {
        const error = toSessionError(err);
        if (typeof ack === 'function') {
          ack({ ok: false, error });
        }
        socket.emit('session:error', error);
      }
    });

    // ── disconnect ─────────────────────────────────────────

    socket.on('disconnect', () => {
      const result = roomService.handleDisconnect(socket.id);
      if (result) {
        // Notify remaining peers of temporary disconnect
        socket.to(result.roomId).emit('session:peer-disconnected', {
          peerId: result.peerId,
        });
      }
    });
  });
}

// ── Relay helpers ────────────────────────────────────────────

function relaySignal(
  socket: Socket,
  roomService: RoomService,
  event: string,
  data: { roomId: string; targetPeerId: string; negotiationId: string; payload: unknown },
  ack?: (response: unknown) => void,
): void {
  try {
    const auth = roomService.authorizeSignaling(socket.id, data.targetPeerId);

    // Forward with server-derived fromPeerId and generation (never trust client)
    socket.to(auth.targetSocketId).emit(event, {
      roomId: auth.roomId,
      fromPeerId: auth.fromPeerId,
      senderGeneration: auth.senderGeneration,
      negotiationId: data.negotiationId,
      payload: data.payload,
    });

    if (typeof ack === 'function') {
      ack({ ok: true });
    }
  } catch (err) {
    const error = toSessionError(err);
    if (typeof ack === 'function') {
      ack({ ok: false, error });
    }
    socket.emit('session:error', error);
  }
}

function relayIceCandidate(
  socket: Socket,
  roomService: RoomService,
  data: { roomId: string; targetPeerId: string; negotiationId: string; payload: unknown },
  ack?: (response: unknown) => void,
): void {
  try {
    const auth = roomService.authorizeSignaling(socket.id, data.targetPeerId);

    socket.to(auth.targetSocketId).emit('signal:ice-candidate', {
      roomId: auth.roomId,
      fromPeerId: auth.fromPeerId,
      senderGeneration: auth.senderGeneration,
      negotiationId: data.negotiationId,
      payload: data.payload,
    });

    if (typeof ack === 'function') {
      ack({ ok: true });
    }
  } catch (err) {
    const error = toSessionError(err);
    if (typeof ack === 'function') {
      ack({ ok: false, error });
    }
    socket.emit('session:error', error);
  }
}

// ── Error mapping ────────────────────────────────────────────

function toSessionError(err: unknown): SessionError {
  if (err instanceof RoomError) {
    return { code: err.code, message: err.message };
  }
  if (err instanceof ZodError) {
    return { code: 'INVALID_REQUEST', message: err.message };
  }
  return { code: 'INTERNAL_ERROR', message: 'Internal server error' };
}
