/**
 * Repair and anti-entropy operation batch payloads.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_2_LLD.md §4
 * - PHASE7_2_PROTOCOL_EVOLUTION_RFC.md §3
 * - PHASE7_4_ARCHITECTURE.md §3 & §4
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §1
 */
import { z } from 'zod';

export const SerializedControlOperationSchema = z.object({
  logicalEventId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  messageType: z.enum(['PLAY', 'PAUSE', 'SEEK', 'RATE_CHANGE']),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  originSequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  causalContext: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  payload: z.unknown(),
});

export type SerializedControlOperation = z.infer<typeof SerializedControlOperationSchema>;

export const OperationBatchPayloadSchema = z
  .object({
    /** Correlation identifier echoing the RepairRequestPayload.requestId when fulfilling a repair. */
    requestId: z.string().min(1).max(128).optional(),
    sessionId: z.string().min(1).max(128).optional(),
    supplierPeerId: z.string().min(1).max(128),
    originPeerId: z.string().min(1).max(128),
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    operations: z.array(SerializedControlOperationSchema).max(32),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
  })
  .refine((data) => data.operations.length <= 32, {
    message: 'Operation batch cannot exceed 32 operations',
  });

export type OperationBatchPayload = z.infer<typeof OperationBatchPayloadSchema>;

export const RepairRequestPayloadSchema = z
  .object({
    /** Globally unique request identifier for correlation and timeout tracking. */
    requestId: z.string().min(1).max(128),

    /** Session identifier. */
    sessionId: z.string().min(1).max(128),

    /** The peer requesting the missing operations. */
    requesterPeerId: z.string().min(1).max(128),

    /** The origin peer whose historical operations are being requested. */
    targetOriginPeerId: z.string().min(1).max(128),

    /** Alias for targetOriginPeerId for backward compatibility. */
    originPeerId: z.string().min(1).max(128).optional(),

    /** First missing sequence number (inclusive). */
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Last missing sequence number (inclusive). */
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Membership epoch under which this request is authorized. */
    membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
  })
  .refine((data) => data.toSequence - data.fromSequence + 1 <= 32, {
    message: 'Requested range cannot exceed 32 operations per request',
  });

export type RepairRequestPayload = z.infer<typeof RepairRequestPayloadSchema>;

export const REPAIR_REJECTION_REASONS = [
  'OUTSIDE_HORIZON', // Requested operations have been compacted to base snapshot
  'ORIGIN_UNKNOWN', // Supplier has never observed this origin peer
  'RATE_LIMITED', // Supplier is receiving too many repair requests
  'EPOCH_MISMATCH', // Supplier is operating on a different membership epoch
  'BUSY', // Supplier queue full or undergoing re-sync
] as const;

export type RepairRejectionReason = (typeof REPAIR_REJECTION_REASONS)[number];

export const RepairRejectedPayloadSchema = z.object({
  /** Correlation ID matching the original RepairRequestPayload.requestId. */
  requestId: z.string().min(1).max(128),

  /** Session identifier. */
  sessionId: z.string().min(1).max(128),

  /** The supplier rejecting the request. */
  supplierPeerId: z.string().min(1).max(128),

  /** The requested origin peer. */
  targetOriginPeerId: z.string().min(1).max(128),

  /** The requested sequence boundaries. */
  fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

  /** Machine-readable reason for the rejection. */
  reason: z.enum(REPAIR_REJECTION_REASONS),

  /**
   * If reason is 'OUTSIDE_HORIZON', provides the oldest sequence currently
   * retained in the supplier's log for this origin.
   */
  retainedHorizonSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER).optional(),
});

export type RepairRejectedPayload = z.infer<typeof RepairRejectedPayloadSchema>;

/**
 * Phase 7.4 Versioned Multi-Vector State Digest Payload Schema.
 * Carries required formatVersion: 2 discriminator to distinguish from Phase 2 legacy digests.
 */
export const StateDigestPayloadSchema = z.object({
  /**
   * Protocol format version discriminator.
   * Literal 2 strictly differentiates Phase 7.4 multi-vector digests from
   * Phase 2 legacy digests ({ digestId, stateVersion, digest }).
   */
  formatVersion: z.literal(2),

  /** Session identifier to ensure messages belong to active room. */
  sessionId: z.string().min(1).max(128),

  /** The peer transmitting this digest. */
  senderPeerId: z.string().min(1).max(128),

  /** Active connection generation of the sender at dispatch time. */
  connectionGeneration: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

  /** Current membership epoch acknowledged by the sender. */
  membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

  /** Monotonically increasing engine state version. */
  stateVersion: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),

  /**
   * Contiguous highest applied sequence numbers per origin.
   * V_rep[p] = k guarantees sequences 1..k have been reduced.
   */
  replicationVector: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),

  /**
   * Base snapshot frontier: oldest retained sequence per origin.
   * Sequences below this have been compacted out of the log.
   */
  snapshotFrontier: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),

  /**
   * Tail operation fingerprints for each origin (SHA-256 hash prefix).
   * Used to detect sequence collisions or silent historical divergence.
   */
  historyFingerprints: z.record(
    z.string().min(1).max(128),
    z.string().regex(/^[0-9a-f]{16,64}$/),
  ),

  /** Monotonic local timestamp (ms) when digest was generated. */
  sentAt: z.number().nonnegative(),
});

export type StateDigestPayload = z.infer<typeof StateDigestPayloadSchema>;
export const StateDigestPayloadV2Schema = StateDigestPayloadSchema;
export type StateDigestPayloadV2 = StateDigestPayload;

/**
 * Type guard verifying whether an incoming payload is a valid Phase 7.4 V2 state digest.
 */
export function isStateDigestV2(payload: unknown): payload is StateDigestPayload {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    (payload as { formatVersion?: unknown }).formatVersion === 2
  );
}

export const CompactedCheckpointItemSchema = z.object({
  originPeerId: z.string().min(1).max(128),
  sequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
});

export type CompactedCheckpointItem = z.infer<typeof CompactedCheckpointItemSchema>;

export const StateSnapshotPayloadV2Schema = z.object({
  formatVersion: z.literal(2),
  snapshotId: z.string().min(1).max(128),
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  authorPeerId: z.string().min(1).max(128),
  snapshotFrontier: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  maxLogicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  originFrontierTimestamps: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  compactedCheckpoints: z.array(CompactedCheckpointItemSchema).max(256),
  state: z.unknown(),
  sha256Digest: z.string().regex(/^[0-9a-f]{64}$/),
});

export type StateSnapshotPayloadV2 = z.infer<typeof StateSnapshotPayloadV2Schema>;
