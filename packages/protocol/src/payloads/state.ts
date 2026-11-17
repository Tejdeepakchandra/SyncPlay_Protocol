/**
 * State exchange payload schemas.
 *
 * Used for recovery and state reconciliation.
 * See FAULT_TOLERANCE_LLD §6 and CONTROL_EVENTS_LAYER_LLD §4.
 *
 * STATE_REQUEST: a peer requests state from another peer.
 * STATE_SNAPSHOT: a full state payload (size-limited, untrusted).
 * STATE_DIGEST: a compact state summary for comparison.
 *
 * Digest and snapshot schemas define structure only.
 * The SyncEngine owns the semantic content and validation.
 */
import { z } from 'zod';

/**
 * Known reasons for requesting state.
 * Derived from recovery scenarios in FAULT_TOLERANCE_LLD.
 */
export const STATE_REQUEST_REASONS = [
  'INITIAL_SYNC',
  'REJOIN',
  'DIVERGENCE_SUSPECTED',
] as const;

export type StateRequestReason = (typeof STATE_REQUEST_REASONS)[number];

export const StateRequestPayloadSchema = z.object({
  /** Unique identifier for this state request. */
  requestId: z.string().min(1).max(128),
  /** Why state is being requested. */
  reason: z.enum(STATE_REQUEST_REASONS),
});

export type StateRequestPayload = z.infer<typeof StateRequestPayloadSchema>;

export const StateSnapshotPayloadSchema = z.object({
  /** Unique identifier for this snapshot. */
  snapshotId: z.string().min(1).max(128),
  /** Engine-defined state version counter. */
  stateVersion: z.number().int().nonnegative(),
  /**
   * The actual state data. Structure is defined by SyncEngine.
   * Must be size-validated before deep processing.
   */
  state: z.unknown(),
});

export type StateSnapshotPayload = z.infer<typeof StateSnapshotPayloadSchema>;

/**
 * Legacy Phase 2 state digest payload schema.
 * Maintained for backward compatibility and protocol version discrimination.
 */
export const StateDigestPayloadV1Schema = z.object({
  /** Unique identifier for this digest. */
  digestId: z.string().min(1).max(128),
  /** Engine-defined state version counter. */
  stateVersion: z.number().int().nonnegative(),
  /**
   * Compact state representation (e.g. hash, event ID list).
   * Format defined by SyncEngine; bounded to prevent abuse.
   */
  digest: z.string().min(1).max(4096),
});

export type StateDigestPayloadV1 = z.infer<typeof StateDigestPayloadV1Schema>;
export const LegacyStateDigestPayloadSchema = StateDigestPayloadV1Schema;
export type LegacyStateDigestPayload = StateDigestPayloadV1;

/** Backwards-compatible alias for Phase 2 state digest payload schema within state.ts */
export const StateDigestPayloadSchema = StateDigestPayloadV1Schema;
export type StateDigestPayload = StateDigestPayloadV1;

/**
 * Type guard checking whether a raw payload conforms to the Phase 2 legacy digest structure.
 */
export function isLegacyStateDigest(payload: unknown): payload is LegacyStateDigestPayload {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    !('formatVersion' in payload) &&
    'digestId' in payload &&
    'digest' in payload
  );
}
