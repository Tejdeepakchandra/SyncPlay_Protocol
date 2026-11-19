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
  MAX_SNAPSHOT_BYTES,
  StateSnapshotPayloadSchema,
  StateSnapshotPayloadV2Schema,
  type StateSnapshotPayload,
  type StateSnapshotPayloadV2,
  type CompactedCheckpointItem,
} from '@syncplay/protocol';
import { canonicalizeJson, sha256 } from './log/operation-fingerprint.js';
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

/**
 * Computes canonical RFC 8785 JSON SHA-256 digest for StateSnapshotPayloadV2.
 * The digest covers all fields of StateSnapshotPayloadV2 EXCEPT `sha256Digest`.
 */
export function computeSnapshotDigestV2(
  snapshot: Omit<StateSnapshotPayloadV2, 'sha256Digest'> | StateSnapshotPayloadV2
): string {
  const { sha256Digest: _, ...rest } = snapshot as Record<string, unknown>;
  const canonical = canonicalizeJson(rest);
  return sha256(canonical);
}

/**
 * Exports current playback state and causal replication state as a StateSnapshotPayloadV2.
 *
 * Implements Phase 7.5 M3 specifications:
 * - Conforms to StateSnapshotPayloadV2 in @syncplay/protocol.
 * - Never exports peer-local monotonic timestamps.
 * - Deterministically sorts compactedCheckpoints by (originPeerId, sequenceNumber).
 * - Enforces maxLogicalTimestamp === max(Object.values(originFrontierTimestamps)) (or 0 if empty).
 * - Computes canonical RFC 8785 JSON SHA-256 digest over preimage.
 * - Enforces MAX_SNAPSHOT_BYTES limit.
 */
export function exportEngineSnapshotV2(
  state: PlaybackState,
  sessionId: string,
  membershipEpoch: number,
  authorPeerId: string,
  snapshotFrontier: Record<string, number>,
  originFrontierTimestamps: Record<string, number>,
  compactedCheckpoints: ReadonlyArray<CompactedCheckpointItem>,
  snapshotId?: string
): StateSnapshotPayloadV2 {
  const sid = snapshotId ?? `snap-${crypto.randomUUID()}`;

  const snapshotData: EngineSnapshotData = {
    formatVersion: 1,
    sessionId,
    status: state.status,
    anchorPositionMs: state.anchorPositionMs,
    playbackRate: state.playbackRate,
    ...(state.lastAppliedEvent
      ? {
          lastAppliedOperation: {
            logicalEventId: state.lastAppliedEvent.logicalEventId,
            senderPeerId: state.lastAppliedEvent.senderPeerId,
            messageType: state.lastAppliedEvent.messageType,
          },
        }
      : {}),
  };

  // Sort compacted checkpoints deterministically for bit-exact reproducibility
  const sortedCheckpoints: CompactedCheckpointItem[] = [...compactedCheckpoints].sort((a, b) => {
    const cmp = a.originPeerId.localeCompare(b.originPeerId);
    if (cmp !== 0) return cmp;
    return a.sequenceNumber - b.sequenceNumber;
  });

  // Enforce Phase 7.2 / Phase 7.5 exact Lamport invariant:
  // maxLogicalTimestamp === max_{p} originFrontierTimestamps[p] (or 0 if empty)
  const tsValues = Object.values(originFrontierTimestamps);
  const maxLogicalTimestamp = tsValues.length > 0 ? Math.max(0, ...tsValues) : 0;

  const preimage = {
    formatVersion: 2 as const,
    snapshotId: sid,
    sessionId,
    membershipEpoch,
    authorPeerId,
    snapshotFrontier: { ...snapshotFrontier },
    maxLogicalTimestamp,
    originFrontierTimestamps: { ...originFrontierTimestamps },
    compactedCheckpoints: sortedCheckpoints,
    state: snapshotData,
  };

  const sha256Digest = computeSnapshotDigestV2(preimage);

  const payload: StateSnapshotPayloadV2 = {
    ...preimage,
    sha256Digest,
  };

  // Enforce protocol payload size limit
  const serialized = JSON.stringify(payload);
  const byteLength = new TextEncoder().encode(serialized).length;
  if (byteLength > MAX_SNAPSHOT_BYTES) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Exported snapshot V2 exceeds MAX_SNAPSHOT_BYTES (${byteLength} > ${MAX_SNAPSHOT_BYTES})`
    );
  }

  // Schema validation to guarantee structural invariants
  return StateSnapshotPayloadV2Schema.parse(payload);
}

export interface ValidatedSnapshotV2Result {
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly authorPeerId: string;
  readonly snapshotFrontier: Readonly<Record<string, number>>;
  readonly maxLogicalTimestamp: number;
  readonly originFrontierTimestamps: Readonly<Record<string, number>>;
  readonly compactedCheckpoints: ReadonlyArray<CompactedCheckpointItem>;
  readonly state: EngineSnapshotData;
  readonly sha256Digest: string;
}

/**
 * Validates an incoming snapshot V2 payload atomically.
 * Throws SyncEngineError if malformed, digest mismatch, or violates semantic invariants.
 */
export function validateSnapshotPayloadV2(
  payload: unknown,
  expectedSessionId: string,
  minRate?: number,
  maxRate?: number
): ValidatedSnapshotV2Result {
  if (!payload || typeof payload !== 'object') {
    throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot payload must be a non-null object');
  }

  // Size check on raw representation
  const serialized = JSON.stringify(payload);
  const byteLength = new TextEncoder().encode(serialized).length;
  if (byteLength > MAX_SNAPSHOT_BYTES) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot payload exceeds MAX_SNAPSHOT_BYTES (${byteLength} > ${MAX_SNAPSHOT_BYTES})`
    );
  }

  // Validate outer envelope schema
  const outerParsed = StateSnapshotPayloadV2Schema.safeParse(payload);
  if (!outerParsed.success) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Outer StateSnapshotPayloadV2 validation failed: ${outerParsed.error.message}`
    );
  }

  const data = outerParsed.data;

  // Validate session ID matches current engine session
  if (data.sessionId !== expectedSessionId) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot sessionId "${data.sessionId}" does not match active session "${expectedSessionId}"`
    );
  }

  // Validate exact Phase 7.2 Lamport invariant
  const tsValues = Object.values(data.originFrontierTimestamps);
  const expectedMaxTs = tsValues.length > 0 ? Math.max(0, ...tsValues) : 0;
  if (data.maxLogicalTimestamp !== expectedMaxTs) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot maxLogicalTimestamp (${data.maxLogicalTimestamp}) does not match originFrontierTimestamps maximum (${expectedMaxTs})`
    );
  }

  // Validate canonical RFC 8785 JSON SHA-256 digest
  const computedDigest = computeSnapshotDigestV2(data);
  if (computedDigest.toLowerCase() !== data.sha256Digest.toLowerCase()) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Snapshot SHA-256 digest mismatch: computed "${computedDigest}" !== declared "${data.sha256Digest}"`
    );
  }

  // Validate inner state data schema
  const innerParsed = EngineSnapshotDataSchema.safeParse(data.state);
  if (!innerParsed.success) {
    throw new SyncEngineError(
      'SNAPSHOT_ERROR',
      `Inner EngineSnapshotData validation failed: ${innerParsed.error.message}`
    );
  }

  const stateData = innerParsed.data;

  // Validate rate bounds if provided
  if (minRate !== undefined && maxRate !== undefined) {
    if (stateData.playbackRate < minRate || stateData.playbackRate > maxRate) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `Snapshot playbackRate ${stateData.playbackRate} outside permitted range [${minRate}, ${maxRate}]`
      );
    }
  }

  return {
    snapshotId: data.snapshotId,
    sessionId: data.sessionId,
    membershipEpoch: data.membershipEpoch,
    authorPeerId: data.authorPeerId,
    snapshotFrontier: data.snapshotFrontier,
    maxLogicalTimestamp: data.maxLogicalTimestamp,
    originFrontierTimestamps: data.originFrontierTimestamps,
    compactedCheckpoints: data.compactedCheckpoints,
    state: stateData,
    sha256Digest: data.sha256Digest,
  };
}

