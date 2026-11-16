/**
 * @module @syncplay/sync-engine
 *
 * Baseline SyncEngine package for SyncPlay.
 * Maintains locally authoritative playback state, calculates continuous position,
 * enforces listener exception isolation, and provides atomic snapshot exchange.
 */

// ── Types ────────────────────────────────────────────────────
export type {
  PlaybackStatus,
  LastAppliedOperation,
  PlaybackState,
  EngineDecisionCode,
  EngineDecision,
  EngineLifecycleState,
  EngineStatus,
  EngineStats,
  Clock,
  SyncEngineConfig,
  StateChangeListener,
  DecisionListener,
  Unsubscribe,
  SyncEngine,
} from './types.js';

export { DEFAULT_SYNC_ENGINE_CONFIG } from './types.js';

// ── Errors ───────────────────────────────────────────────────
export { SyncEngineError } from './errors.js';
export type { SyncEngineErrorCode } from './errors.js';

// ── Configuration ────────────────────────────────────────────
export { validateSyncEngineConfig } from './config.js';
export type { ResolvedSyncEngineConfig } from './config.js';

// ── Reducer & Calculations ───────────────────────────────────
export { calculateExtrapolatedPosition, reduceState } from './reducer.js';
export type { ReducerResult } from './reducer.js';

// ── Snapshots ────────────────────────────────────────────────
export {
  EngineSnapshotDataSchema,
  exportEngineSnapshot,
  validateSnapshotPayload,
} from './snapshot.js';
export type { EngineSnapshotData, ValidatedSnapshotResult } from './snapshot.js';

// ── Implementation ───────────────────────────────────────────
export { BaselineSyncEngine } from './baseline-sync-engine.js';
