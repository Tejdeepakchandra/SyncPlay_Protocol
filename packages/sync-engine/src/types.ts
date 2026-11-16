/**
 * Locally authoritative playback state and SyncEngine contracts.
 *
 * Implements Phase 6 specifications aligned with:
 * - DECISIONS.md (D1, D2, D8, D11)
 * - INTERFACE_CONTRACTS.md §6
 * - Phase 5 SyncEventReceiver boundary contract
 */

import type {
  PeerId,
  SessionId,
  StateSnapshotPayload,
} from '@syncplay/protocol';
import type {
  AnyControlSyncEvent,
  SyncEventReceiver,
} from '@syncplay/control-events';

// ── Status & State Model ─────────────────────────────────────

/**
 * Binary playback status.
 * Locally authoritative representation of playing vs paused.
 */
export type PlaybackStatus = 'paused' | 'playing';

/**
 * Metadata describing the operation that produced the current state.
 */
export interface LastAppliedOperation {
  /** Stable logical identity of the operation across retries. */
  readonly logicalEventId: string;

  /**
   * Peer ID that authored the operation.
   * Derived from the transport-verified identity (or untrusted provenance if from a snapshot).
   */
  readonly senderPeerId: string;

  /** Protocol message type ('PLAY', 'PAUSE', 'SEEK', 'RATE_CHANGE'). */
  readonly messageType: string;

  /** Local monotonic timestamp (performance.now) when applied locally. */
  readonly appliedAtLocalTimestamp: number;

  /** True if this record was restored from an imported snapshot (untrusted provenance). */
  readonly isImportedProvenance?: boolean;
}

/**
 * Locally authoritative playback state.
 *
 * Continuous position while playing is extrapolated dynamically from
 * anchorPositionMs, elapsed monotonic time, and playbackRate.
 *
 * All state instances returned by SyncEngine are deeply frozen at runtime.
 */
export interface PlaybackState {
  /** Whether the media is currently playing or paused. */
  readonly status: PlaybackStatus;

  /**
   * Reference media position in milliseconds at anchorLocalTimestamp.
   * Guaranteed to be finite and non-negative.
   */
  readonly anchorPositionMs: number;

  /**
   * Monotonic clock reading (performance.now) when anchorPositionMs was fixed.
   * Strictly local to this peer; never exported over the wire.
   */
  readonly anchorLocalTimestamp: number;

  /**
   * Playback speed multiplier (e.g. 1.0, 1.5).
   * Guaranteed to be finite, positive, and within [minPlaybackRate, maxPlaybackRate].
   */
  readonly playbackRate: number;

  /**
   * Strictly local monotonic engine revision counter. Incremented by exactly 1
   * on every authoritative local state mutation. Initialized to 0.
   * Has NO distributed ordering meaning and cannot be used to compare two peers.
   */
  readonly stateVersion: number;

  /**
   * Details of the operation that caused the most recent state mutation.
   * Undefined in clean initial state. A NO_OP operation does NOT overwrite this field.
   */
  readonly lastAppliedEvent?: LastAppliedOperation;
}

// ── Decision Outcomes ────────────────────────────────────────

export type EngineDecisionCode =
  | 'APPLIED'           // Valid event resulted in local state mutation
  | 'NO_OP'             // Valid event resulted in identical state (e.g. pause when paused)
  | 'REJECTED_DISPOSED' // Operation rejected because engine is disposed
  | 'REJECTED_INVALID'  // Payload failed semantic validation preconditions
  | 'IGNORED_DUPLICATE';// Redundant event already processed

export interface EngineDecision {
  /** True if the event was semantically accepted and processed without error. */
  readonly accepted: boolean;

  /** Specific decision outcome. */
  readonly code: EngineDecisionCode;

  /** State version following the operation. */
  readonly stateVersion: number;

  /** Monotonic timestamp when decision was reached. */
  readonly decidedAt: number;

  /** Optional diagnostic message or explanation. */
  readonly message?: string;
}

// ── Lifecycle, Status & Diagnostics ──────────────────────────

export type EngineLifecycleState = 'active' | 'disposed';

export interface EngineStatus {
  readonly state: EngineLifecycleState;
  readonly sessionId: SessionId;
  readonly localPeerId: PeerId;
  readonly stateVersion: number;
}

export interface EngineStats {
  readonly eventsReceivedCount: number;
  readonly eventsAppliedCount: number;
  readonly eventsNoOpCount: number;
  readonly eventsRejectedCount: number;
  readonly stateChangeNotificationsCount: number;
  readonly decisionNotificationsCount: number;
  readonly listenerErrorCount: number;
}

// ── Clock & Configuration ────────────────────────────────────

export interface Clock {
  /** Returns monotonic milliseconds (e.g. performance.now()). */
  now(): number;
}

export interface SyncEngineConfig {
  /** Active session/room ID. */
  readonly sessionId: SessionId;

  /** Local peer ID assigned by the Bootstrap Server. */
  readonly localPeerId: PeerId;

  /** Monotonic clock implementation (defaults to performance.now). */
  readonly clock?: Clock;

  /** Minimum permitted playback rate (default: 0.25). */
  readonly minPlaybackRate?: number;

  /** Maximum permitted playback rate (default: 4.0). */
  readonly maxPlaybackRate?: number;

  /** Epsilon threshold in ms for comparing floating-point extrapolated positions (default: 5.0 ms). */
  readonly positionEpsilonMs?: number;
}

export const DEFAULT_SYNC_ENGINE_CONFIG = {
  minPlaybackRate: 0.25,
  maxPlaybackRate: 4.0,
  positionEpsilonMs: 5.0,
} as const;

// ── Listener Signatures ──────────────────────────────────────

export type StateChangeListener = (state: Readonly<PlaybackState>) => void;
export type DecisionListener = (event: AnyControlSyncEvent, decision: Readonly<EngineDecision>) => void;
export type Unsubscribe = () => void;

// ── Core SyncEngine Interface ────────────────────────────────

/**
 * Public contract for SyncEngine implementations.
 * Extends Phase 5 SyncEventReceiver with exact `receive(event): void` signature.
 */
export interface SyncEngine extends SyncEventReceiver {
  /** Lifecycle inspection. */
  readonly isDisposed: boolean;

  /**
   * Consume a control event candidate.
   *
   * EXACT signature from Phase 5 SyncEventReceiver:
   *   receive(event: AnyControlSyncEvent): void;
   *
   * Error Handling Contract:
   * - Local outbound event (origin === 'local'):
   *     If semantic validation fails BEFORE state mutation, throws SyncEngineError.
   *     ControlEventService.executeIntent() catches this, aborts broadcast,
   *     and returns { accepted: false, error: ENGINE_ERROR }.
   * - Remote inbound event (origin === 'remote'):
   *     If semantic validation fails, REJECTS safely WITHOUT throwing.
   *     Records rejection in EngineDecision and EngineStats.
   *     Guarantees malformed remote input never crashes the transport event loop.
   * - Listener exceptions:
   *     Isolated in try-catch; NEVER cause receive() to throw after state is committed.
   */
  receive(event: AnyControlSyncEvent): void;

  /** Returns the most recent decision outcome for caller inspection. */
  getLastDecision(): Readonly<EngineDecision> | undefined;

  /** Returns the current locally authoritative playback state (deeply frozen). */
  getState(): Readonly<PlaybackState>;

  /**
   * Calculates continuous media position in milliseconds.
   * If status is 'paused', returns anchorPositionMs.
   * If status is 'playing', calculates:
   *   anchorPositionMs + max(0, (timestamp - anchorLocalTimestamp) * playbackRate)
   *
   * @param atTimestamp Optional monotonic timestamp (defaults to clock.now()).
   * Clamped to anchorPositionMs if atTimestamp is earlier than anchorLocalTimestamp.
   * Throws SyncEngineError if atTimestamp is non-finite or negative.
   */
  getCurrentPositionMs(atTimestamp?: number): number;

  /** Returns current engine status. */
  getStatus(): EngineStatus;

  /** Returns an immutable snapshot of operational diagnostics. */
  getStats(): Readonly<EngineStats>;

  /**
   * Subscribe to authoritative state mutations.
   * Fires ONLY when state actually changes (never on NO_OP).
   * Subscriber exceptions are isolated and do not prevent other subscribers.
   */
  onStateChange(listener: StateChangeListener): Unsubscribe;

  /**
   * Subscribe to all event processing decisions (applied, no-op, rejected).
   * Subscriber exceptions are isolated.
   */
  onDecision(listener: DecisionListener): Unsubscribe;

  /**
   * Export current state as a portable, serializable snapshot conforming to StateSnapshotPayload.
   * Never exports peer-local monotonic timestamps.
   */
  exportSnapshot(snapshotId?: string): StateSnapshotPayload;

  /**
   * Explicit application-level state import.
   * Validates snapshot, sets anchor to receiving engine's clock, and sets stateVersion = localVersion + 1.
   * Does NOT claim synchronized playback across peers.
   */
  importSnapshot(payload: StateSnapshotPayload): EngineDecision;

  /** Terminal, idempotent lifecycle disposal. Clears all listeners. */
  dispose(): void;
}
