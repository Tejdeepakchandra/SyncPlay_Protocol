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
import { MAX_OPERATIONS_PER_BATCH, MAX_VECTOR_ENTRIES } from '../constants.js';

/**
 * Canonical UUID v4 regular expression (RFC 4122 §4.4).
 * Strictly requires 8-4-4-4-12 hexadecimal structure with:
 * - Version digit equal to 4 in position 13
 * - Variant bits 8, 9, a, or b (case-insensitive) in position 17
 */
export const UUID_V4_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const UUIDv4Schema = z.string().regex(UUID_V4_REGEX, {
  message:
    'Invalid UUID v4 format: must be canonical 8-4-4-4-12 hex string with version 4 and variant 1 (8, 9, a, or b)',
});

export const SerializedControlOperationSchema = z.object({
  logicalEventId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(64),
  membershipEpoch: z.number().int().nonnegative(),
  messageType: z.enum(['PLAY', 'PAUSE', 'SEEK', 'RATE_CHANGE']),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  originSequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  causalContext: z.record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  payload: z.unknown(),
});

export type SerializedControlOperation = z.infer<typeof SerializedControlOperationSchema>;

/**
 * Request-driven Operation Batch Payload Schema (Phase 7.4).
 * Enforces mandatory UUID v4 requestId for request-response correlation
 * and sessionId to prevent cross-room repair pollution.
 */
export const OperationBatchPayloadSchema = z
  .object({
    /**
     * Request correlation identifier matching the originating RepairRequestPayload.requestId.
     * Mandatory for request-driven repair to correlate batches with active in-flight requests.
     */
    requestId: UUIDv4Schema,

    /** Session identifier to ensure batch belongs to the active room session. */
    sessionId: z.string().min(1).max(128),

    /** The supplier peer serving this batch. */
    supplierPeerId: z.string().min(1).max(64),

    /** The author / origin peer of the contained operations. */
    originPeerId: z.string().min(1).max(64),

    /** First sequence number in this batch (inclusive). */
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Last sequence number in this batch (inclusive). */
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Contained operations ordered by sequence number (at most 32). */
    operations: z.array(SerializedControlOperationSchema).max(MAX_OPERATIONS_PER_BATCH),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
    path: ['toSequence'],
  });

export type OperationBatchPayload = z.infer<typeof OperationBatchPayloadSchema>;

/**
 * Push-based Operation Batch Payload Schema (unsolicited delivery).
 * Requires sessionId for room scoping, but has no correlating requestId.
 */
export const PushOperationBatchPayloadSchema = z
  .object({
    /** Session identifier to ensure batch belongs to the active room session. */
    sessionId: z.string().min(1).max(128),

    /** The supplier peer pushing this batch. */
    supplierPeerId: z.string().min(1).max(64),

    /** The author / origin peer of the contained operations. */
    originPeerId: z.string().min(1).max(64),

    /** First sequence number in this batch (inclusive). */
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Last sequence number in this batch (inclusive). */
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Contained operations ordered by sequence number (at most 32). */
    operations: z.array(SerializedControlOperationSchema).max(MAX_OPERATIONS_PER_BATCH),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
    path: ['toSequence'],
  });

export type PushOperationBatchPayload = z.infer<typeof PushOperationBatchPayloadSchema>;

/**
 * Phase 7.3 backward-compatibility operation batch schema.
 * Defines the contract for legacy/internal engine repair batches where
 * requestId and sessionId were optional.
 */
export const CompatibilityOperationBatchPayloadSchema = z
  .object({
    requestId: UUIDv4Schema.optional(),
    sessionId: z.string().min(1).max(128).optional(),
    supplierPeerId: z.string().min(1).max(128),
    originPeerId: z.string().min(1).max(128),
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
    operations: z.array(SerializedControlOperationSchema).max(MAX_OPERATIONS_PER_BATCH),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
    path: ['toSequence'],
  });

export type CompatibilityOperationBatchPayload = z.infer<
  typeof CompatibilityOperationBatchPayloadSchema
>;

export const RepairRequestPayloadSchema = z
  .object({
    /** Globally unique request identifier for correlation and timeout tracking (UUID v4 mandatory). */
    requestId: UUIDv4Schema,

    /** Session identifier to scope request to active room. */
    sessionId: z.string().min(1).max(128),

    /** The peer requesting the missing operations. */
    requesterPeerId: z.string().min(1).max(64),

    /** The origin peer whose historical operations are being requested. */
    targetOriginPeerId: z.string().min(1).max(64),

    /** Alias for targetOriginPeerId for backward compatibility. */
    originPeerId: z.string().min(1).max(64).optional(),

    /** First missing sequence number (inclusive). */
    fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Last missing sequence number (inclusive). */
    toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Membership epoch under which this request is authorized. */
    membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  })
  .refine((data) => data.toSequence >= data.fromSequence, {
    message: 'toSequence must be greater than or equal to fromSequence',
    path: ['toSequence'],
  })
  .refine((data) => data.toSequence - data.fromSequence + 1 <= MAX_OPERATIONS_PER_BATCH, {
    message: `Requested range cannot exceed ${MAX_OPERATIONS_PER_BATCH} operations per request`,
    path: ['toSequence'],
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
  /** Correlation ID matching the original RepairRequestPayload.requestId (UUID v4). */
  requestId: UUIDv4Schema,

  /** Session identifier to ensure rejection belongs to the active room session. */
  sessionId: z.string().min(1).max(128),

  /** The supplier peer rejecting the request. */
  supplierPeerId: z.string().min(1).max(64),

  /** The origin peer whose operations were requested. */
  targetOriginPeerId: z.string().min(1).max(64),

  /** Alias for targetOriginPeerId for backward compatibility. */
  originPeerId: z.string().min(1).max(64).optional(),

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
 * Replication vector schema:
 * Maps origin peer IDs to contiguous highest applied sequence numbers.
 * Bounded to MAX_VECTOR_ENTRIES (64) origins.
 */
export const ReplicationVectorSchema = z
  .record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  )
  .refine((val) => Object.keys(val).length <= MAX_VECTOR_ENTRIES, {
    message: `Replication vector cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
  });

/**
 * Snapshot frontier schema:
 * Maps origin peer IDs to base snapshot compacted horizon sequences.
 * Bounded to MAX_VECTOR_ENTRIES (64) origins.
 */
export const SnapshotFrontierSchema = z
  .record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  )
  .refine((val) => Object.keys(val).length <= MAX_VECTOR_ENTRIES, {
    message: `Snapshot frontier cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
  });

/**
 * History fingerprints schema:
 * Maps origin peer IDs to 64-character lowercase hexadecimal canonical SHA-256 fingerprints.
 * Truncated or malformed hashes are rejected.
 * Bounded to MAX_VECTOR_ENTRIES (64) origins.
 */
export const HistoryFingerprintsSchema = z
  .record(
    z.string().min(1).max(64),
    z.string().regex(/^[0-9a-f]{64}$/, {
      message: 'Fingerprint must be an exact 64-character lowercase hexadecimal SHA-256 hash',
    }),
  )
  .refine((val) => Object.keys(val).length <= MAX_VECTOR_ENTRIES, {
    message: `History fingerprints cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
  });

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
  senderPeerId: z.string().min(1).max(64),

  /** Active connection generation of the sender at dispatch time. */
  connectionGeneration: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

  /** Current membership epoch acknowledged by the sender. */
  membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

  /** Monotonically increasing engine state version. */
  stateVersion: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),

  /**
   * Contiguous highest applied sequence numbers per origin.
   * V_rep[p] = k guarantees sequences 1..k have been reduced.
   * Bounded to MAX_VECTOR_ENTRIES (64) origins.
   */
  replicationVector: ReplicationVectorSchema,

  /**
   * Base snapshot frontier: oldest retained sequence per origin.
   * Sequences below this have been compacted out of the log.
   * Bounded to MAX_VECTOR_ENTRIES (64) origins.
   */
  snapshotFrontier: SnapshotFrontierSchema,

  /**
   * Tail operation fingerprints for each origin (full canonical SHA-256 hash).
   * Used to detect sequence collisions or silent historical divergence.
   * Values must be exact 64-char lowercase hexadecimal SHA-256 strings.
   * Bounded to MAX_VECTOR_ENTRIES (64) origins.
   */
  historyFingerprints: HistoryFingerprintsSchema,

  /**
   * Sender-local transmission timestamp (ms) when digest was generated.
   *
   * SEMANTIC CONSTRAINTS:
   * - Diagnostic/telemetry metadata ONLY.
   * - Receiver MUST NOT use sentAt to calculate RTT (must use RTTEngine two-way probes).
   * - Receiver MUST NOT use sentAt to calculate local staleness (must use local monotonic receive time).
   * - Peer wall clocks are not assumed or required to be synchronized.
   */
  sentAt: z.number().finite().nonnegative(),
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

/**
 * Type guard verifying whether an incoming operation batch is a request-driven repair batch.
 */
export function isRequestDrivenOperationBatch(
  payload: unknown,
): payload is OperationBatchPayload {
  if (typeof payload !== 'object' || payload === null) return false;
  const p = payload as Record<string, unknown>;
  const reqId = p['requestId'];
  return typeof reqId === 'string' && UUID_V4_REGEX.test(reqId);
}

/**
 * Type guard verifying whether an incoming operation batch is an unsolicited push batch.
 */
export function isPushOperationBatch(
  payload: unknown,
): payload is PushOperationBatchPayload {
  if (typeof payload !== 'object' || payload === null) return false;
  const p = payload as Record<string, unknown>;
  return typeof p['sessionId'] === 'string' && p['requestId'] === undefined;
}

export const CompactedCheckpointItemSchema = z.object({
  originPeerId: z.string().min(1).max(64),
  sequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
});

export type CompactedCheckpointItem = z.infer<typeof CompactedCheckpointItemSchema>;

export const StateSnapshotPayloadV2Schema = z.object({
  formatVersion: z.literal(2),
  snapshotId: z.string().min(1).max(128),
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  authorPeerId: z.string().min(1).max(64),
  snapshotFrontier: z.record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  maxLogicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  originFrontierTimestamps: z.record(
    z.string().min(1).max(64),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  compactedCheckpoints: z.array(CompactedCheckpointItemSchema).max(256),
  state: z.unknown(),
  sha256Digest: z.string().regex(/^[0-9a-f]{64}$/),
});

export type StateSnapshotPayloadV2 = z.infer<typeof StateSnapshotPayloadV2Schema>;
