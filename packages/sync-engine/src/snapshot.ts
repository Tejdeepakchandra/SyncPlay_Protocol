/**
 * Snapshot serialization, schema validation, and atomic state import/export.
 *
 * Implements Phase 6 requirements:
 * - Conforms to StateSnapshotPayload in @syncplay/protocol.
 * - Never exports or imports monotonic timestamps.
 * - Enforces protocol size limits (MAX_PAYLOAD_BYTES).
 * - Treats imported provenance as untrusted.
 */

import { z } from 'zod';
import {
  MAX_PAYLOAD_BYTES,
  StateSnapshotPayloadSchema,
} from '@syncplay/protocol';
import type { StateSnapshotPayload } from '@syncplay/protocol';
import type { PlaybackState } from './types.js';
import { SyncEngineError } from './errors.js';

/**
 * Strict schema for the embedded `state` field in StateSnapshotPayload.
 */
export const EngineSnapshotDataSchema = z.object({
  formatVersion: z.literal(1),
  sessionId: z.string().min(1).max(128),
  status: z.enum(['paused', 'playing']),
  anchorPositionMs: z.number().finite().nonnegative(),
  playbackRate: z.number().finite().positive(),
  lastAppliedOperation: z
    .object({
      logicalEventId: z.string().min(1).max(128),
      senderPeerId: z.string().min(1).max(128),
      messageType: z.string().min(1).max(64),
    })
    .optional(),
});

export type EngineSnapshotData = z.infer<typeof EngineSnapshotDataSchema>;

/**
 * Exports current playback state as a StateSnapshotPayload.
 * Never includes peer-local monotonic timestamps.
 */
export function exportEngineSnapshot(
  state: PlaybackState,
  sessionId: string,
  snapshotId?: string
): StateSnapshotPayload {
  const sid = snapshotId ?? `snap-${crypto.randomUUID()}`;

  const snapshotData: EngineSnapshotData = {
    formatVersion: 1,
    sessionId,
    status: state.status,
    anchorPositionMs: state.anchorPositionMs,
    playbackRate: state.playbackRate,
    lastAppliedOperation: state.lastAppliedEvent
      ? {
          logicalEventId: state.lastAppliedEvent.logicalEventId,
          senderPeerId: state.lastAppliedEvent.senderPeerId,
          messageType: state.lastAppliedEvent.messageType,
        }
      : undefined,
  };

  const payload: StateSnapshotPayload = {
    snapshotId: sid,
    stateVersion: state.stateVersion,
    state: snapshotData,
  };

  // Enforce protocol payload size limit
  const serialized = JSON.stringify(payload);
  const byteLength = new TextEncoder().encode(serialized).length;
  if (byteLength > MAX_PAYLOAD_BYTES) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Exported snapshot exceeds MAX_PAYLOAD_BYTES (${byteLength} > ${MAX_PAYLOAD_BYTES})`
    );
  }

  return payload;
}

export interface ValidatedSnapshotResult {
  readonly snapshotId: string;
  readonly remoteVersion: number;
  readonly data: EngineSnapshotData;
}

/**
 * Validates an incoming snapshot payload atomically.
 * Throws SyncEngineError if malformed or violates semantic bounds.
 */
export function validateSnapshotPayload(
  payload: unknown,
  expectedSessionId: string,
  minRate: number,
  maxRate: number
): ValidatedSnapshotResult {
  if (!payload || typeof payload !== 'object') {
    throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot payload must be a non-null object');
  }

  // Size check on raw representation
  const serialized = JSON.stringify(payload);
  const byteLength = new TextEncoder().encode(serialized).length;
  if (byteLength > MAX_PAYLOAD_BYTES) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot payload exceeds MAX_PAYLOAD_BYTES (${byteLength} > ${MAX_PAYLOAD_BYTES})`
    );
  }

  // Validate outer envelope schema
  const outerParsed = StateSnapshotPayloadSchema.safeParse(payload);
  if (!outerParsed.success) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Outer StateSnapshotPayload validation failed: ${outerParsed.error.message}`
    );
  }

  // Validate inner state data schema
  const innerParsed = EngineSnapshotDataSchema.safeParse(outerParsed.data.state);
  if (!innerParsed.success) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Inner EngineSnapshotData validation failed: ${innerParsed.error.message}`
    );
  }

  const data = innerParsed.data;

  // Validate session ID matches current engine session
  if (data.sessionId !== expectedSessionId) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot sessionId "${data.sessionId}" does not match active session "${expectedSessionId}"`
    );
  }

  // Validate rate bounds
  if (data.playbackRate < minRate || data.playbackRate > maxRate) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot playbackRate ${data.playbackRate} outside permitted range [${minRate}, ${maxRate}]`
    );
  }

  return {
    snapshotId: outerParsed.data.snapshotId,
    remoteVersion: outerParsed.data.stateVersion,
    data,
  };
}
