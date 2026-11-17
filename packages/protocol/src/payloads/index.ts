/**
 * Payload schema registry.
 *
 * Maps each known MessageType and SyncMessageType to its Zod payload schema.
 * Used by the validation layer to look up the correct schema.
 */
import { z } from 'zod';
import { MessageType, SYNC_MESSAGE_TYPES } from '../message-types.js';
import { PingPayloadSchema, PongPayloadSchema } from './ping-pong.js';
import {
  PlayPayloadSchema,
  PausePayloadSchema,
  SeekPayloadSchema,
  RateChangePayloadSchema,
} from './control.js';
import {
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadV1Schema,
  LegacyStateDigestPayloadSchema,
  isLegacyStateDigest,
} from './state.js';
import {
  StateDigestPayloadSchema,
  StateDigestPayloadV2Schema,
  isStateDigestV2,
  SerializedControlOperationSchema,
  OperationBatchPayloadSchema,
  RepairRequestPayloadSchema,
  RepairRejectedPayloadSchema,
  CompactedCheckpointItemSchema,
  StateSnapshotPayloadV2Schema,
  REPAIR_REJECTION_REASONS,
} from './repair.js';

export { PingPayloadSchema, PongPayloadSchema } from './ping-pong.js';
export {
  PlayPayloadSchema,
  PausePayloadSchema,
  SeekPayloadSchema,
  RateChangePayloadSchema,
} from './control.js';
export {
  StateRequestPayloadSchema,
  StateSnapshotPayloadSchema,
  StateDigestPayloadV1Schema,
  LegacyStateDigestPayloadSchema,
  isLegacyStateDigest,
  STATE_REQUEST_REASONS,
} from './state.js';
export type {
  StateRequestPayload,
  StateSnapshotPayload,
  StateDigestPayloadV1,
  LegacyStateDigestPayload,
  StateRequestReason,
} from './state.js';
export {
  StateDigestPayloadSchema,
  StateDigestPayloadV2Schema,
  isStateDigestV2,
  SerializedControlOperationSchema,
  OperationBatchPayloadSchema,
  RepairRequestPayloadSchema,
  RepairRejectedPayloadSchema,
  CompactedCheckpointItemSchema,
  StateSnapshotPayloadV2Schema,
  REPAIR_REJECTION_REASONS,
} from './repair.js';
export type {
  StateDigestPayload,
  StateDigestPayloadV2,
  SerializedControlOperation,
  OperationBatchPayload,
  RepairRequestPayload,
  RepairRejectedPayload,
  RepairRejectionReason,
  CompactedCheckpointItem,
  StateSnapshotPayloadV2,
} from './repair.js';

/**
 * Union schema for STATE_DIGEST message type, accepting either Phase 7.4 V2 multi-vector
 * digest or Phase 2 V1 legacy digest.
 */
export const StateDigestWireSchema = z.union([StateDigestPayloadSchema, StateDigestPayloadV1Schema]);

/**
 * Registry mapping each MessageType to its payload Zod schema.
 * Every entry in MessageType and SYNC_MESSAGE_TYPES must have a corresponding schema here.
 */
const entries: ReadonlyArray<[string, z.ZodTypeAny]> = [
  [MessageType.PING, PingPayloadSchema],
  [MessageType.PONG, PongPayloadSchema],
  [MessageType.PLAY, PlayPayloadSchema],
  [MessageType.PAUSE, PausePayloadSchema],
  [MessageType.SEEK, SeekPayloadSchema],
  [MessageType.RATE_CHANGE, RateChangePayloadSchema],
  [MessageType.STATE_REQUEST, StateRequestPayloadSchema],
  [MessageType.STATE_SNAPSHOT, StateSnapshotPayloadSchema],
  [MessageType.STATE_DIGEST, StateDigestWireSchema],
  [MessageType.OPERATION_BATCH, OperationBatchPayloadSchema],
  [MessageType.REPAIR_REQUEST, RepairRequestPayloadSchema],
  [MessageType.REPAIR_REJECTED, RepairRejectedPayloadSchema],
  [SYNC_MESSAGE_TYPES.CONTROL_EVENT, z.unknown()],
  [SYNC_MESSAGE_TYPES.OPERATION_BATCH, OperationBatchPayloadSchema],
  [SYNC_MESSAGE_TYPES.STATE_DIGEST, StateDigestPayloadSchema],
  [SYNC_MESSAGE_TYPES.REPAIR_REQUEST, RepairRequestPayloadSchema],
  [SYNC_MESSAGE_TYPES.REPAIR_REJECTED, RepairRejectedPayloadSchema],
];

export const PAYLOAD_SCHEMAS: ReadonlyMap<string, z.ZodTypeAny> = new Map(entries);
