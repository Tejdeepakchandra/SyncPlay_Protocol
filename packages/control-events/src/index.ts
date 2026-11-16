/**
 * @module @syncplay/control-events
 *
 * Control Events Layer for SyncPlay.
 *
 * Validates, deduplicates, and dispatches playback control events
 * between application controls, WebRTC PeerMesh, and the SyncEngine.
 *
 * Designed around stable interfaces; consensus and conflict resolution
 * algorithms remain pluggable behind the SyncEventReceiver contract.
 */

// ── Types ────────────────────────────────────────────────────
export type {
  Clock,
  ControlIntent,
  EventOrigin,
  SyncEvent,
  PlaySyncEvent,
  PauseSyncEvent,
  SeekSyncEvent,
  RateChangeSyncEvent,
  AnyControlSyncEvent,
  SyncEventReceiver,
  CommandResult,
  PublishReport,
  PublishResult,
  DeduplicationStats,
  ServiceStats,
  ControlEventServiceConfig,
  SyncPlayControls,
  ControlEventService,
} from './types.js';

export { DEFAULT_CONTROL_CONFIG } from './types.js';

// ── Errors ───────────────────────────────────────────────────
export type { CommandErrorCode, CommandError, ControlErrorCode } from './errors.js';
export { ControlError } from './errors.js';

// ── Event Factory ────────────────────────────────────────────
export { createControlEvent, createRetryEvent, generateId } from './event-factory.js';

// ── Deduplication Cache ──────────────────────────────────────
export { DeduplicationCache } from './dedup-cache.js';

// ── Rate Limiter ─────────────────────────────────────────────
export { BoundedRateLimiter } from './rate-limiter.js';

// ── Implementation ───────────────────────────────────────────
export { ControlsImpl } from './controls.js';
export { ControlEventServiceImpl } from './control-event-service.js';

