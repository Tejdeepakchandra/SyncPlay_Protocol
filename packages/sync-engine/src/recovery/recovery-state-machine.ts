/**
 * @module @syncplay/sync-engine/recovery
 *
 * Recovery State Machine for Phase 7.5 M4.3a.
 *
 * Implements the authoritative 4-state recovery lifecycle:
 * - ACTIVE: Normal operation; local and remote operations processed in real-time.
 * - RECOVERING: Snapshot transfer, dry-run validation, and staging in flight; local inputs blocked.
 * - RECONCILING: Snapshot base committed; draining quarantine buffer and resolving gaps.
 * - DEGRADED: Terminal failure or unresolvable divergence; safe freeze.
 *
 * Critical Invariants:
 * 1. Exactly one authoritative recovery state exists at any time.
 * 2. All transition requests are strictly validated against the approved transition graph.
 * 3. Invalid transitions throw deterministically and leave state 100% unmutated.
 * 4. Disposed state is terminal; late callbacks cannot alter state post-disposal.
 * 5. Observer listeners execute inside isolated try/catch blocks post-commit.
 */

import { SyncEngineError } from '../errors.js';
import type { Unsubscribe } from '../types.js';

export type RecoveryState = 'ACTIVE' | 'RECOVERING' | 'RECONCILING' | 'DEGRADED';

export type RecoverySubstate =
  | 'IDLE'
  | 'AWAITING_OFFER'
  | 'TRANSFERRING'
  | 'VALIDATING'
  | 'STAGING'
  | 'COMMITTING'
  | 'DRAINING_QUARANTINE'
  | 'RESOLVING_GAPS';

export type RecoveryStateListener = (
  newState: RecoveryState,
  oldState: RecoveryState,
  reason: string,
) => void;

export type RecoverySubstateListener = (
  newSubstate: RecoverySubstate,
  oldSubstate: RecoverySubstate,
) => void;

/**
 * Approved transition graph defining legal (source -> target) transitions.
 */
const VALID_STATE_TRANSITIONS: Readonly<Record<RecoveryState, ReadonlySet<RecoveryState>>> = {
  ACTIVE: new Set<RecoveryState>(['RECOVERING', 'DEGRADED']),
  RECOVERING: new Set<RecoveryState>(['RECONCILING', 'DEGRADED', 'ACTIVE']),
  RECONCILING: new Set<RecoveryState>(['ACTIVE', 'DEGRADED']),
  DEGRADED: new Set<RecoveryState>(['RECOVERING']),
};

export class RecoveryStateMachine {
  private _state: RecoveryState;
  private _substate: RecoverySubstate;
  private _isDisposed = false;

  private readonly stateListeners = new Set<RecoveryStateListener>();
  private readonly substateListeners = new Set<RecoverySubstateListener>();

  constructor(
    initialState: RecoveryState = 'ACTIVE',
    initialSubstate: RecoverySubstate = 'IDLE',
  ) {
    this._state = initialState;
    this._substate = initialSubstate;
  }

  public get state(): RecoveryState {
    return this._state;
  }

  public get substate(): RecoverySubstate {
    return this._substate;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  /**
   * Checks whether a transition from the current state to targetState is valid.
   */
  public canTransition(targetState: RecoveryState): boolean {
    if (this._isDisposed) {
      return false;
    }
    const allowed = VALID_STATE_TRANSITIONS[this._state];
    return allowed ? allowed.has(targetState) : false;
  }

  /**
   * Transitions the state machine to a new state.
   *
   * @param newState Target state to transition into.
   * @param reason Human-readable diagnostic reason for the transition.
   * @param newSubstate Optional substate to enter simultaneously (defaults to IDLE).
   * @throws SyncEngineError('DISPOSED') if state machine is disposed.
   * @throws SyncEngineError('INVALID_STATE_TRANSITION') if transition is not permitted.
   */
  public transition(
    newState: RecoveryState,
    reason: string,
    newSubstate: RecoverySubstate = 'IDLE',
  ): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'RecoveryStateMachine is disposed');
    }

    if (!this.canTransition(newState)) {
      throw new SyncEngineError(
        'INVALID_STATE_TRANSITION',
        `Illegal recovery transition from ${this._state} to ${newState}: ${reason}`,
      );
    }

    const oldState = this._state;
    const oldSubstate = this._substate;

    // Mutate state atomically before notifying listeners
    this._state = newState;
    this._substate = newSubstate;

    // Notify state change listeners with exception isolation
    for (const listener of this.stateListeners) {
      try {
        listener(newState, oldState, reason);
      } catch {
        // Observer exception isolation: external listener errors must not corrupt engine state
      }
    }

    // Notify substate listeners if substate changed
    if (newSubstate !== oldSubstate) {
      for (const listener of this.substateListeners) {
        try {
          listener(newSubstate, oldSubstate);
        } catch {
          // Observer exception isolation
        }
      }
    }
  }

  /**
   * Updates only the internal execution substate within the current top-level state.
   *
   * @throws SyncEngineError('DISPOSED') if state machine is disposed.
   */
  public setSubstate(newSubstate: RecoverySubstate): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'RecoveryStateMachine is disposed');
    }

    if (this._substate === newSubstate) {
      return;
    }

    const oldSubstate = this._substate;
    this._substate = newSubstate;

    for (const listener of this.substateListeners) {
      try {
        listener(newSubstate, oldSubstate);
      } catch {
        // Observer exception isolation
      }
    }
  }

  /**
   * Subscribes a listener to top-level recovery state transitions.
   */
  public onStateChange(listener: RecoveryStateListener): Unsubscribe {
    if (this._isDisposed) {
      return () => {};
    }
    this.stateListeners.add(listener);
    return () => {
      this.stateListeners.delete(listener);
    };
  }

  /**
   * Subscribes a listener to granular substate changes.
   */
  public onSubstateChange(listener: RecoverySubstateListener): Unsubscribe {
    if (this._isDisposed) {
      return () => {};
    }
    this.substateListeners.add(listener);
    return () => {
      this.substateListeners.delete(listener);
    };
  }

  /**
   * Disposes the state machine idempotently.
   * Terminal state; removes all listeners and prevents future transitions.
   */
  public dispose(): void {
    if (this._isDisposed) {
      return;
    }
    this._isDisposed = true;
    this.stateListeners.clear();
    this.substateListeners.clear();
  }
}
