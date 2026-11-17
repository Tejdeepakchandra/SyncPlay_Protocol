/**
 * Conflict resolver contracts and types.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_INTERFACE_CONTRACTS.md §2.3
 * - PHASE7_2_LLD.md §2
 */

import type { PlaybackState, EngineDecision } from '../types.js';
import type { StoredOperation } from '../log/types.js';

export interface ReducerContext {
  readonly sessionId: string;
  readonly localPeerId: string;
  readonly minPlaybackRate: number;
  readonly maxPlaybackRate: number;
  readonly positionEpsilonMs: number;
}

export interface IConflictResolver {
  /**
   * Pure functional reduction of a single operation against current playback state.
   */
  reduce(
    currentState: PlaybackState,
    operation: StoredOperation,
    monotonicNow: number,
    context: ReducerContext,
    priorOperation?: StoredOperation,
  ): { nextState: PlaybackState; decision: EngineDecision };

  /**
   * Full replay from base snapshot across an ordered slice of operations.
   * Used when late operations trigger speculative rollback.
   */
  replay(
    baseState: PlaybackState,
    operations: ReadonlyArray<StoredOperation>,
    monotonicNow: number,
    context: ReducerContext,
  ): { finalState: PlaybackState; decisions: ReadonlyArray<EngineDecision> };
}
