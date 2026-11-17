/**
 * Repair and anti-entropy operation batch payloads.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_2_LLD.md §4
 * - PHASE7_2_PROTOCOL_EVOLUTION_RFC.md §3
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

export const OperationBatchPayloadSchema = z.object({
  supplierPeerId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(128),
  fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  operations: z.array(SerializedControlOperationSchema).max(32),
});

export type OperationBatchPayload = z.infer<typeof OperationBatchPayloadSchema>;

export const RepairRequestPayloadSchema = z.object({
  requestId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(128),
  fromSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  toSequence: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
});

export type RepairRequestPayload = z.infer<typeof RepairRequestPayloadSchema>;

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
