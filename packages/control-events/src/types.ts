/**
 * Control Events Layer types and contracts.
 *
 * Implements Phase 5 requirements aligned with:
 * - INTERFACE_CONTRACTS.md §4
 * - CONTROL_EVENTS_LAYER_LLD.md §1–8
 * - DECISIONS.md (D1, D2, D8, D11, D21)
 */

import type {
  PeerId,
  SessionId,
  ProtocolMessage,
  PlayPayload,
  PausePayload,
  SeekPayload,
  RateChangePayload,
} from '@syncplay/protocol';
import type { CommandError } from './errors.js';

// ── Clock ────────────────────────────────────────────────────

export interface Clock {
  now(): number;
}

// ── Control Intents (User / Application Level) ────────────────

export type ControlIntent =
  | { readonly type: 'PLAY'; readonly targetPositionMs?: number }
  | { readonly type: 'PAUSE'; readonly observedPositionMs?: number }
  | { readonly type: 'SEEK'; readonly targetPositionMs: number }
  | { readonly type: 'RATE_CHANGE'; readonly playbackRate: number };

// ── Local SyncEvent (SDK In-Memory Representation) ────────────

export type EventOrigin = 'local' | 'remote';

export interface SyncEvent<TPayload = unknown> {
  /** The immutable wire-level protocol message. */
  readonly message: ProtocolMessage<TPayload>;

  /**
   * Monotonic timestamp (performance.now()) when the event was
   * created locally or received from transport.
   * This is local SDK metadata and is NEVER serialized over the wire.
   */
  readonly localTimestamp: number;

  /**
   * Origin classification:
   * - 'local': initiated by local user/controls
   * - 'remote': received over WebRTC DataChannel from a connected peer
   * This is local SDK metadata and is NEVER serialized over the wire.
   */
  readonly origin: EventOrigin;
}

export type PlaySyncEvent = SyncEvent<PlayPayload>;
export type PauseSyncEvent = SyncEvent<PausePayload>;
export type SeekSyncEvent = SyncEvent<SeekPayload>;
export type RateChangeSyncEvent = SyncEvent<RateChangePayload>;

export type AnyControlSyncEvent =
  | PlaySyncEvent
  | PauseSyncEvent
  | SeekSyncEvent
  | RateChangeSyncEvent;

// ── SyncEngine Receiver Boundary (Replaceable Phase 6 Interface) ──

/**
 * Minimal interface required from the SyncEngine by the Control Events layer.
 * Phase 6 SyncEngine implements this receiver.
 *
 * Implementations must be idempotent and should not assume sequential or
 * perfectly synchronized arrival.
 */
export interface SyncEventReceiver {
  /**
   * Receive a validated, non-duplicate control event candidate.
   * @throws Error if local processing fails (will prevent outbound broadcast).
   */
  receive(event: AnyControlSyncEvent): void;
}

// ── Command Outcomes ─────────────────────────────────────────

export interface CommandResult {
  /**
   * True if the intent was semantically valid, passed rate checks,
   * and was successfully accepted by the local client / SyncEventReceiver.
   */
  readonly accepted: boolean;

  /** Stable logicalEventId assigned to this operation. */
  readonly logicalEventId?: string;

  /** Number of currently connected remote peers attempted. */
  readonly peerCount: number;

  /** Number of remote peers where transmission succeeded. */
  readonly deliveredCount: number;

  /** True if deliveredCount < peerCount when peerCount > 0. */
  readonly partialDelivery: boolean;

  /** Present only when accepted is false. */
  readonly error?: CommandError;
}

// ── Publishing Outcomes ──────────────────────────────────────

export interface PublishReport {
  readonly peerId: PeerId;
  readonly ok: boolean;
  readonly error?: string;
}

export interface PublishResult {
  readonly logicalEventId: string;
  readonly messageId: string;
  readonly recipients: ReadonlyArray<PublishReport>;
  readonly peerCount: number;
  readonly deliveredCount: number;
  readonly partialDelivery: boolean;
}

// ── Statistics & Diagnostics ─────────────────────────────────

export interface DeduplicationStats {
  size: number;
  duplicateCount: number;
  evictedCount: number;
}

export interface ServiceStats {
  inboundReceivedCount: number;
  inboundDispatchedCount: number;
  inboundDroppedCount: number;
  outboundCreatedCount: number;
  outboundDispatchedCount: number;
  preReceiverQueuedCount: number;
  preReceiverOverflowCount: number;
  preReceiverDrainedCount: number;
  preReceiverDrainErrorCount: number;
  deduplication: DeduplicationStats;
}

// ── Configuration ────────────────────────────────────────────

export interface ControlEventServiceConfig {
  /** Active session/room ID. */
  readonly sessionId: SessionId;

  /** Local peer ID assigned by the Bootstrap Server. */
  readonly localPeerId: PeerId;

  /** Monotonic clock (defaults to performance.now). */
  readonly clock?: Clock;

  /** Maximum entries retained in deduplication cache (default: 500). */
  readonly maxDeduplicationEntries?: number;

  /** Time-to-live for deduplication entries in ms (default: 60,000 ms). */
  readonly deduplicationTtlMs?: number;

  /** Minimum permitted playback rate (default: 0.25). */
  readonly minPlaybackRate?: number;

  /** Maximum permitted playback rate (default: 4.0). */
  readonly maxPlaybackRate?: number;

  /** Maximum outbound commands per second (default: 20). */
  readonly maxOperationsPerSecond?: number;

  /** Maximum capacity of pre-receiver queue before rejecting new arrivals (default: 20). */
  readonly maxPreReceiverQueueSize?: number;

  /** Maximum remote peers tracked concurrently in the rate limiter (default: 64). */
  readonly maxTrackedRateLimitPeers?: number;
}

export const DEFAULT_CONTROL_CONFIG = {
  maxDeduplicationEntries: 500,
  deduplicationTtlMs: 60_000,
  minPlaybackRate: 0.25,
  maxPlaybackRate: 4.0,
  maxOperationsPerSecond: 20,
  maxPreReceiverQueueSize: 20,
  maxTrackedRateLimitPeers: 64,
} as const;

// ── Application Controls Interface ───────────────────────────

export interface SyncPlayControls {
  /** Request playback resumption, optionally from targetPositionMs. */
  play(targetPositionMs?: number): Promise<CommandResult>;

  /** Request playback pause, optionally specifying observed local position. */
  pause(observedPositionMs?: number): Promise<CommandResult>;

  /** Request a seek to targetPositionMs. */
  seek(targetPositionMs: number): Promise<CommandResult>;

  /** Request playback rate change (within [minPlaybackRate, maxPlaybackRate]). */
  setPlaybackRate(playbackRate: number): Promise<CommandResult>;

  /**
   * Retransmit an earlier operation to connected peers.
   * Generates a new messageId while preserving the original logicalEventId.
   * Does NOT redispatch to the local receiver.
   */
  retry(event: AnyControlSyncEvent): Promise<CommandResult>;
}

// ── Control Event Service Interface ───────────────────────────

export interface ControlEventService {
  /** Register or replace the SyncEventReceiver. Drains any pre-receiver queue. */
  setReceiver(receiver: SyncEventReceiver): void;

  /** Unregister current receiver. Inbound events will be buffered up to queue limit. */
  clearReceiver(): void;

  /** Access the developer-facing controls API. */
  getControls(): SyncPlayControls;

  /** Broadcast a validated event to connected peers in the mesh. */
  publish(event: AnyControlSyncEvent): Promise<PublishResult>;

  /** Inspect service statistics and deduplication diagnostics. */
  getStats(): ServiceStats;

  /** Whether the service has been permanently disposed. */
  readonly isDisposed: boolean;

  /**
   * Permanently dispose the service. Unsubscribes from transport,
   * flushes queues, clears deduplication cache. Idempotent.
   */
  dispose(): void;
}
