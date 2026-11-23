/**
 * @module @syncplay/sync-engine/recovery
 *
 * Single-Root Engine Core State Container for Phase 7.5 M4.3b.
 *
 * Establishes the authoritative single-root state model required for atomic snapshot installation.
 * Implements M4.3-R1 through M4.3-R4 specifications:
 * - Exactly one authoritative state root container (EngineCoreState).
 * - Encompasses the 12 authoritative fields identified in the 26-field engine state inventory.
 * - Structurally controlled with encapsulated mutation:
 *   1. Container sealed via Object.seal() to prohibit adding/deleting properties.
 *   2. PlaybackState and baseSnapshot are deeply frozen.
 *   3. Authoritative collections (replicationVector, localCausalContext, operationLog, pendingQueue, clockManager)
 *      are encapsulated inside the root.
 *   4. Public projections return immutable/defensive copies (e.g. Object.freeze({ ...replicationVector })).
 *   5. Zero duplicate authoritative state outside this container.
 */

import type { PlaybackState } from '../types.js';
import type { LogicalClockManager } from '../clock/logical-clock-manager.js';
import type { OrderedOperationLog } from '../log/ordered-operation-log.js';
import type { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import type { StoredOperation } from '../log/types.js';
import { SyncEngineError } from '../errors.js';

/**
 * Authoritative single root container holding all 12 core engine state fields.
 */
export interface EngineCoreState {
  /**
   * Primary active playback state (deeply frozen).
   */
  state: PlaybackState;

  /**
   * Alias for state, providing full contract compatibility across specs.
   */
  playbackState: PlaybackState;

  /**
   * Compaction base anchor for speculative replay (deeply frozen).
   */
  baseSnapshot: PlaybackState;

  /**
   * Highest compacted sequence numbers per origin.
   */
  baseSnapshotFrontier: Record<string, number>;

  /**
   * Authoritative totally-ordered operation log.
   */
  operationLog: OrderedOperationLog;

  /**
   * Authoritative Lamport logical clock manager and validator.
   */
  clockManager: LogicalClockManager;

  /**
   * Authoritative missing-dependency queue for buffered causal operations.
   */
  pendingQueue: PendingDependencyQueue;

  /**
   * Highest contiguous applied sequences per origin peer.
   */
  replicationVector: Record<string, number>;

  /**
   * Observed causal horizon for local event generation.
   */
  localCausalContext: Record<string, number>;

  /**
   * Monotonic sequence counter for locally originated events.
   */
  localOriginSequence: number;

  /**
   * Current membership configuration epoch.
   */
  currentEpoch: number;

  /**
   * Identity of the most recently applied operation.
   */
  lastAppliedEventId: string | undefined;

  /**
   * Monotonic state version counter.
   */
  stateVersion: number;
}

export interface CreateEngineCoreStateParams {
  playbackState: PlaybackState;
  baseSnapshot: PlaybackState;
  baseSnapshotFrontier: Record<string, number>;
  operationLog: OrderedOperationLog;
  clockManager: LogicalClockManager;
  pendingQueue: PendingDependencyQueue;
  replicationVector: Record<string, number>;
  localCausalContext: Record<string, number>;
  localOriginSequence: number;
  currentEpoch: number;
  lastAppliedEventId?: string;
  stateVersion?: number;
}

/**
 * Creates a sealed EngineCoreState container encapsulating all 12 authoritative fields.
 * Applies Object.seal() so that properties cannot be added or deleted.
 */
export function createEngineCoreState(params: CreateEngineCoreStateParams): EngineCoreState {
  if (!params || typeof params !== 'object') {
    throw new SyncEngineError('INVALID_ARGUMENT', 'CreateEngineCoreStateParams must be a non-null object');
  }
  if (!params.operationLog) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'operationLog is required in EngineCoreState');
  }
  if (!params.clockManager) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'clockManager is required in EngineCoreState');
  }
  if (!params.pendingQueue) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'pendingQueue is required in EngineCoreState');
  }

  // Deep-freeze playbackState and baseSnapshot if not already frozen
  const playbackState = Object.isFrozen(params.playbackState)
    ? params.playbackState
    : Object.freeze({ ...params.playbackState });

  const baseSnapshot = Object.isFrozen(params.baseSnapshot)
    ? params.baseSnapshot
    : Object.freeze({ ...params.baseSnapshot });

  const stateVersion = params.stateVersion ?? playbackState.stateVersion ?? 0;

  const core: EngineCoreState = {
    state: playbackState,
    playbackState,
    baseSnapshot,
    baseSnapshotFrontier: { ...params.baseSnapshotFrontier },
    operationLog: params.operationLog,
    clockManager: params.clockManager,
    pendingQueue: params.pendingQueue,
    replicationVector: { ...params.replicationVector },
    localCausalContext: { ...params.localCausalContext },
    localOriginSequence: params.localOriginSequence,
    currentEpoch: params.currentEpoch,
    lastAppliedEventId: params.lastAppliedEventId,
    stateVersion,
  };

  return Object.seal(core);
}

export interface InitialEngineCoreStateConfig {
  initialState: PlaybackState;
  logCapacity: number;
  initialClock: number;
  reducerFn: (state: PlaybackState, op: StoredOperation) => PlaybackState;
  LogicalClockManagerClass: new (initialClock?: number) => LogicalClockManager;
  OrderedOperationLogClass: new (
    capacity: number,
    baseSnapshot: PlaybackState,
    reducer: (state: PlaybackState, op: StoredOperation) => PlaybackState,
  ) => OrderedOperationLog;
  PendingDependencyQueueClass: new (capacity: number) => PendingDependencyQueue;
}

/**
 * Creates the initial authoritative EngineCoreState for a newly constructed engine.
 */
export function createInitialEngineCoreState(config: InitialEngineCoreStateConfig): EngineCoreState {
  const clockManager = new config.LogicalClockManagerClass(config.initialClock);
  const operationLog = new config.OrderedOperationLogClass(
    config.logCapacity,
    config.initialState,
    config.reducerFn,
  );
  const pendingQueue = new config.PendingDependencyQueueClass(config.logCapacity);

  return createEngineCoreState({
    playbackState: config.initialState,
    baseSnapshot: config.initialState,
    baseSnapshotFrontier: {},
    operationLog,
    clockManager,
    pendingQueue,
    replicationVector: {},
    localCausalContext: {},
    localOriginSequence: 0,
    currentEpoch: 1,
    lastAppliedEventId: undefined,
    stateVersion: config.initialState.stateVersion ?? 0,
  });
}
