/**
 * Payload schema registry.
 *
 * Maps each known MessageType to its Zod payload schema.
 * Used by the validation layer to look up the correct schema.
 */
import type { z } from 'zod';
import { MessageType } from '../message-types.js';
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
  StateDigestPayloadSchema,
} from './state.js';

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
  StateDigestPayloadSchema,
} from './state.js';
export {
  SerializedControlOperationSchema,
  OperationBatchPayloadSchema,
  RepairRequestPayloadSchema,
  CompactedCheckpointItemSchema,
  StateSnapshotPayloadV2Schema,
} from './repair.js';
export type {
  SerializedControlOperation,
  OperationBatchPayload,
  RepairRequestPayload,
  CompactedCheckpointItem,
  StateSnapshotPayloadV2,
} from './repair.js';

/**
 * Registry mapping each MessageType to its payload Zod schema.
 * Every entry in MessageType must have a corresponding schema here.
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
  [MessageType.STATE_DIGEST, StateDigestPayloadSchema],
];

export const PAYLOAD_SCHEMAS: ReadonlyMap<string, z.ZodTypeAny> = new Map(entries);
