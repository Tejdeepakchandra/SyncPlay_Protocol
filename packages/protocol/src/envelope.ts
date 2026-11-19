/**
 * Protocol message envelope.
 *
 * This is the canonical shared envelope for all SyncPlay protocol messages.
 * Based on INTERFACE_CONTRACTS.md §1.
 *
 * v0.1 envelope is deliberately minimal:
 * - No `metadata` field (deferred — D19)
 * - No `createdAt` field (deferred — D20)
 * - `logicalEventId` in the envelope replaces `operationId` in payloads (D21)
 *
 * Zod validation strips unknown fields (forward-compatible).
 * Unknown protocolVersion and unknown messageType are rejected
 * at the validation layer, not here.
 */
import { z } from 'zod';
import { ASCII_IDENTIFIER_REGEX } from './constants.js';
import type { PeerId, SessionId, MessageId, LogicalEventId } from './identity.js';

/**
 * The shared protocol message envelope.
 *
 * @typeParam TPayload - The payload type, validated per messageType.
 */
export interface ProtocolMessage<TPayload = unknown> {
  /** Protocol version. Must match PROTOCOL_VERSION for acceptance. */
  readonly protocolVersion: number;
  /** Message type from the MessageType registry. */
  readonly messageType: string;
  /** Room/session identifier. */
  readonly sessionId: SessionId;
  /** Logical participant identity of the sender. */
  readonly senderPeerId: PeerId;
  /** Unique identifier for this specific message transmission. */
  readonly messageId: MessageId;
  /**
   * Stable identity for the logical user operation.
   * Preserved across retries of the same operation.
   * Required for control events; optional for transport messages.
   */
  readonly logicalEventId?: LogicalEventId;
  /**
   * Logical author identity of the operation.
   * Stamped by originating peer; immutable across multi-hop relay.
   */
  readonly originPeerId?: PeerId;
  /**
   * Room membership epoch from Bootstrap at the moment this message was created.
   * Used for strict epoch fencing: events from obsolete epochs are discarded.
   */
  readonly membershipEpoch?: number;
  /**
   * WebRTC connection generation counter.
   * Used to filter stale messages across peer reconnections.
   */
  readonly connectionGeneration?: number;
  /**
   * Monotonic Lamport logical timestamp.
   * Required for all distributed control operations.
   */
  readonly logicalTimestamp?: number;
  /**
   * Origin-specific contiguous sequence counter for anti-entropy replication tracking.
   * Stamped by originating peer; immutable across retransmissions.
   */
  readonly originSequenceNumber?: number;
  /**
   * Causal Context Version Vector.
   * Highest consecutive origin sequences observed by author at event creation.
   */
  readonly causalContext?: Record<string, number>;
  /** Message payload, validated per messageType. */
  readonly payload: TPayload;
}

/**
 * Zod schema for the protocol message envelope.
 *
 * Validates structural correctness only. Version and messageType
 * semantic checks are performed by the validation layer.
 *
 * Uses default Zod behavior (strip unknown fields) for forward
 * compatibility with potential future envelope additions.
 */
export const ProtocolMessageSchema = z.object({
  protocolVersion: z.number().int().positive(),
  messageType: z.string().min(1).max(64),
  sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  senderPeerId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  messageId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  logicalEventId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX).optional(),
  originPeerId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX).optional(),
  membershipEpoch: z.number().int().nonnegative().optional(),
  connectionGeneration: z.number().int().positive().max(Number.MAX_SAFE_INTEGER).optional(),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER).optional(),
  originSequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER).optional(),
  causalContext: z
    .record(
      z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
      z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    )
    .optional(),
  payload: z.unknown(),
});

/**
 * Strict wire envelope schema for snapshot chunk messages.
 *
 * Implements Phase 7.5 M4 wire contract:
 * - Requires standard envelope fields: protocolVersion, messageType, sessionId, senderPeerId, messageId, payload.
 * - Permits legitimate transport metadata: membershipEpoch, connectionGeneration.
 * - STRICTLY PROHIBITS operation-specific and replication metadata:
 *   causalContext, logicalEventId, originPeerId, logicalTimestamp, originSequenceNumber.
 * - Strictly rejects unknown keys on the envelope layer (.strict()).
 *
 * This restriction guarantees that serialized chunk message envelope overhead
 * is strictly bounded to <= 1,800 bytes (worst-case measured at 1,231 bytes with max identifiers and chunk framing).
 */
export const SnapshotChunkEnvelopeSchema = z
  .object({
    protocolVersion: z.number().int().positive(),
    messageType: z.string().min(1).max(64),
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    senderPeerId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    messageId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER).optional(),
    connectionGeneration: z.number().int().positive().max(Number.MAX_SAFE_INTEGER).optional(),
    payload: z.unknown(),
    // Strictly prohibited fields on snapshot chunk wire envelopes
    causalContext: z
      .never({ message: 'Snapshot chunk envelope must not contain causalContext' })
      .optional(),
    logicalEventId: z
      .never({ message: 'Snapshot chunk envelope must not contain logicalEventId' })
      .optional(),
    originPeerId: z
      .never({ message: 'Snapshot chunk envelope must not contain originPeerId' })
      .optional(),
    logicalTimestamp: z
      .never({ message: 'Snapshot chunk envelope must not contain logicalTimestamp' })
      .optional(),
    originSequenceNumber: z
      .never({ message: 'Snapshot chunk envelope must not contain originSequenceNumber' })
      .optional(),
  })
  .strict();

export interface SnapshotChunkEnvelope<TPayload = unknown> {
  readonly protocolVersion: number;
  readonly messageType: string;
  readonly sessionId: SessionId;
  readonly senderPeerId: PeerId;
  readonly messageId: MessageId;
  readonly membershipEpoch?: number;
  readonly connectionGeneration?: number;
  readonly payload: TPayload;
}
