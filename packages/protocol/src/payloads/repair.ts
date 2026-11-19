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
import { MAX_OPERATIONS_PER_BATCH, MAX_VECTOR_ENTRIES, ASCII_IDENTIFIER_REGEX } from '../constants.js';

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
 * Interface representing common fields required for contiguous sequence batch validation.
 */
export interface ContiguousSequenceBatchData {
  originPeerId: string;
  membershipEpoch: number;
  fromSequence: number;
  toSequence: number;
  operations: ReadonlyArray<SerializedControlOperation>;
}

/**
 * Validates contiguous sequence coverage, origin matching, epoch matching,
 * and boundary conditions for operation batches.
 *
 * Invariants enforced:
 * 1. toSequence >= fromSequence.
 * 2. Empty batches are strictly prohibited. Suppliers unable to satisfy requests
 *    must send REPAIR_REJECTED with an appropriate reason code (e.g. OUTSIDE_HORIZON,
 *    ORIGIN_UNKNOWN, RATE_LIMITED, EPOCH_MISMATCH, BUSY).
 * 3. Range count (toSequence - fromSequence + 1) does not exceed MAX_OPERATIONS_PER_BATCH (32).
 * 4. operations.length exactly equals declared range count (toSequence - fromSequence + 1).
 * 5. operations[0].originSequenceNumber === fromSequence.
 * 6. operations[last].originSequenceNumber === toSequence.
 * 7. For each operation i in operations:
 *    - op.originPeerId === originPeerId (no origin rewriting/cross-author mixing).
 *    - op.membershipEpoch === membershipEpoch (no cross-epoch mixing).
 *    - op.originSequenceNumber === fromSequence + i (strictly ascending, contiguous, no gaps, no duplicates).
 */
export function validateContiguousSequenceBatch(
  data: ContiguousSequenceBatchData,
  ctx: z.RefinementCtx,
): void {
  // 1. Range bounds check
  if (data.toSequence < data.fromSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `toSequence (${data.toSequence}) must be greater than or equal to fromSequence (${data.fromSequence})`,
      path: ['toSequence'],
    });
    return;
  }

  // 2. Empty batch check
  if (data.operations.length === 0) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message:
        'Empty operation batches are prohibited. Suppliers unable to satisfy requests must send REPAIR_REJECTED with an appropriate reason code.',
      path: ['operations'],
    });
    return;
  }

  // 3. Expected count based on sequence range
  const expectedCount = data.toSequence - data.fromSequence + 1;

  // 4. Batch size cap
  if (expectedCount > MAX_OPERATIONS_PER_BATCH) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Batch sequence range ${data.fromSequence}..${data.toSequence} (${expectedCount} operations) exceeds maximum allowed ${MAX_OPERATIONS_PER_BATCH}`,
      path: ['toSequence'],
    });
    return;
  }

  // 5. Operation count must exactly match expected count
  if (data.operations.length !== expectedCount) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Operation count (${data.operations.length}) does not match declared sequence range ${data.fromSequence}..${data.toSequence} (expected ${expectedCount})`,
      path: ['operations'],
    });
  }

  // 6. First operation sequence
  const firstOp = data.operations[0];
  if (firstOp && firstOp.originSequenceNumber !== data.fromSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `First operation originSequenceNumber (${firstOp.originSequenceNumber}) does not match fromSequence (${data.fromSequence})`,
      path: ['operations', 0, 'originSequenceNumber'],
    });
  }

  // 7. Last operation sequence
  const lastOp = data.operations[data.operations.length - 1];
  if (lastOp && lastOp.originSequenceNumber !== data.toSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Last operation originSequenceNumber (${lastOp.originSequenceNumber}) does not match toSequence (${data.toSequence})`,
      path: ['operations', data.operations.length - 1, 'originSequenceNumber'],
    });
  }

  // 8. Strict contiguous sequence, origin matching, and epoch matching for every operation
  for (let i = 0; i < data.operations.length; i++) {
    const op = data.operations[i];
    if (!op) continue;
    const expectedSeq = data.fromSequence + i;

    if (op.originSequenceNumber !== expectedSeq) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has originSequenceNumber ${op.originSequenceNumber}, expected strictly contiguous ${expectedSeq}`,
        path: ['operations', i, 'originSequenceNumber'],
      });
    }

    if (op.originPeerId !== data.originPeerId) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has originPeerId '${op.originPeerId}', which does not match batch originPeerId '${data.originPeerId}'`,
        path: ['operations', i, 'originPeerId'],
      });
    }

    if (op.membershipEpoch !== data.membershipEpoch) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has membershipEpoch ${op.membershipEpoch}, which does not match batch membershipEpoch ${data.membershipEpoch}`,
        path: ['operations', i, 'membershipEpoch'],
      });
    }
  }
}

/**
 * Canonical Operation Batch Payload Schema (Phase 7.4).
 *
 * Anti-entropy operation batch delivery in Phase 7.4 is strictly request-driven:
 * a supplier serves an OPERATION_BATCH only in response to a correlated REPAIR_REQUEST.
 *
 * NOTE: Unsolicited or push-based batch delivery is NOT part of the approved Phase 7.4
 * architecture and is not an active network contract. Introducing a separate push-based
 * delivery path would require future architectural authorization.
 *
 * Enforces:
 * - Mandatory canonical UUID v4 requestId matching originating RepairRequestPayload.requestId.
 * - Mandatory sessionId to enforce room scoping.
 * - Mandatory supplierPeerId and originPeerId.
 * - Mandatory membershipEpoch under which operations are authorized.
 * - Contiguous sequence coverage (validateContiguousSequenceBatch):
 *   - Strictly ascending originSequenceNumber with zero gaps and zero duplicates.
 *   - First sequence equals fromSequence, last sequence equals toSequence.
 *   - Operation count exactly equals toSequence - fromSequence + 1 (<= MAX_OPERATIONS_PER_BATCH).
 *   - Every contained operation belongs to originPeerId and membershipEpoch.
 *   - Empty batches are strictly prohibited (suppliers must dispatch REPAIR_REJECTED).
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

    /** Membership epoch under which operations are authorized. */
    membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),

    /** Contained operations ordered by sequence number (at most 32). */
    operations: z.array(SerializedControlOperationSchema).max(MAX_OPERATIONS_PER_BATCH),
  })
  .superRefine(validateContiguousSequenceBatch);

export type OperationBatchPayload = z.infer<typeof OperationBatchPayloadSchema>;

/**
 * Phase 7.3 backward-compatibility operation batch schema.
 * Defines the contract for legacy/internal engine repair batches where
 * requestId and sessionId were optional.
 *
 * Isolated strictly to Phase 7.3 engine test fixtures; NOT registered in PAYLOAD_SCHEMAS.
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
 * Maps origin peer IDs to highest contiguous sequence numbers already incorporated
 * into the compacted base snapshot. Sequences <= snapshotFrontier are compacted out of the log.
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
  sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),

  /** The peer transmitting this digest. */
  senderPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),

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
   * Base snapshot frontier: highest contiguous sequence per origin already incorporated
   * into the compacted base snapshot. Sequences <= snapshotFrontier have been compacted out of the active log.
   * The oldest retained sequence in the active log is snapshotFrontier + 1.
   * Bounded to MAX_VECTOR_ENTRIES (64) origins.
   */
  snapshotFrontier: SnapshotFrontierSchema,

  /**
   * Tail operation fingerprints for each origin (full canonical SHA-256 hash).
   * Fingerprint of the operation at sequence k = replicationVector[origin].
   * Detects sequence collisions or conflicting operations at the replication frontier,
   * but does not prove equality across arbitrary intermediate unchained history.
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

export const CompactedCheckpointItemSchema = z.object({
  originPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
  sequenceNumber: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  logicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
});

export type CompactedCheckpointItem = z.infer<typeof CompactedCheckpointItemSchema>;

export const StateSnapshotPayloadV2Schema = z.object({
  formatVersion: z.literal(2),
  snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
  membershipEpoch: z.number().int().nonnegative(),
  authorPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
  snapshotFrontier: z.record(
    z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  maxLogicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  originFrontierTimestamps: z.record(
    z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  ),
  compactedCheckpoints: z.array(CompactedCheckpointItemSchema).max(256),
  state: z.unknown(),
  sha256Digest: z.string().regex(/^[0-9a-f]{64}$/),
});

export type StateSnapshotPayloadV2 = z.infer<typeof StateSnapshotPayloadV2Schema>;
