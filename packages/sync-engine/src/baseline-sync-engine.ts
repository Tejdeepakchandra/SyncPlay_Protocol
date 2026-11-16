/**
 * BaselineSyncEngine implementation.
 *
 * Implements the SyncEngine and SyncEventReceiver contracts.
 * Maintains locally authoritative playback state using a pure reducer,
 * provides continuous position extrapolation, listener exception isolation,
 * and atomic snapshot export/import.
 */

import type { StateSnapshotPayload } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type {
  SyncEngine,
  PlaybackState,
  EngineDecision,
  EngineStatus,
  EngineStats,
  Clock,
  SyncEngineConfig,
  StateChangeListener,
  DecisionListener,
  Unsubscribe,
} from './types.js';
import { SyncEngineError } from './errors.js';
import { validateSyncEngineConfig, type ResolvedSyncEngineConfig } from './config.js';
import { reduceState, calculateExtrapolatedPosition } from './reducer.js';
import {
  exportEngineSnapshot,
  validateSnapshotPayload,
} from './snapshot.js';

interface MutableStats {
  eventsReceivedCount: number;
  eventsAppliedCount: number;
  eventsNoOpCount: number;
  eventsRejectedCount: number;
  stateChangeNotificationsCount: number;
  decisionNotificationsCount: number;
  listenerErrorCount: number;
}

export class BaselineSyncEngine implements SyncEngine {
  private readonly config: ResolvedSyncEngineConfig;
  private readonly clock: Clock;
  private lastObservedClockTime: number;

  private state: PlaybackState;
  private lastDecision?: EngineDecision;
  private disposed = false;

  private readonly stateChangeListeners = new Set<StateChangeListener>();
  private readonly decisionListeners = new Set<DecisionListener>();

  private readonly mutableStats: MutableStats = {
    eventsReceivedCount: 0,
    eventsAppliedCount: 0,
    eventsNoOpCount: 0,
    eventsRejectedCount: 0,
    stateChangeNotificationsCount: 0,
    decisionNotificationsCount: 0,
    listenerErrorCount: 0,
  };

  constructor(config: SyncEngineConfig) {
    this.config = validateSyncEngineConfig(config);
    this.clock = config.clock ?? { now: () => performance.now() };

    // Initial clock check
    const initialNow = this.clock.now();
    if (typeof initialNow !== 'number' || !Number.isFinite(initialNow) || initialNow < 0) {
      throw new SyncEngineError(
        'CLOCK_ERROR',
        `Injected clock returned invalid initial timestamp: ${initialNow}`
      );
    }
    this.lastObservedClockTime = initialNow;

    // Initialize initial locally authoritative playback state
    this.state = Object.freeze<PlaybackState>({
      status: 'paused',
      anchorPositionMs: 0,
      anchorLocalTimestamp: initialNow,
      playbackRate: 1.0,
      stateVersion: 0,
    });
  }

  // ── Lifecycle & Status ───────────────────────────────────────

  public get isDisposed(): boolean {
    return this.disposed;
  }

  public getStatus(): EngineStatus {
    return {
      state: this.disposed ? 'disposed' : 'active',
      sessionId: this.config.sessionId,
      localPeerId: this.config.localPeerId,
      stateVersion: this.state.stateVersion,
    };
  }

  public getStats(): Readonly<EngineStats> {
    return Object.freeze({ ...this.mutableStats });
  }

  public getState(): Readonly<PlaybackState> {
    return this.state;
  }

  public getLastDecision(): Readonly<EngineDecision> | undefined {
    return this.lastDecision;
  }

  // ── Monotonic Clock Reading & Regression Guard ───────────────

  private getMonotonicNow(): number {
    const now = this.clock.now();

    if (typeof now !== 'number' || !Number.isFinite(now) || now < 0) {
      throw new SyncEngineError(
        'CLOCK_ERROR',
        `Injected clock returned non-finite or negative timestamp: ${now}`
      );
    }

    if (now < this.lastObservedClockTime) {
      throw new SyncEngineError(
        'CLOCK_ERROR',
        `Clock regression detected: current ${now} < last observed ${this.lastObservedClockTime}`
      );
    }

    this.lastObservedClockTime = now;
    return now;
  }

  // ── Continuous Playback Position ─────────────────────────────

  public getCurrentPositionMs(atTimestamp?: number): number {
    let targetTime: number;

    if (atTimestamp !== undefined) {
      if (typeof atTimestamp !== 'number' || !Number.isFinite(atTimestamp) || atTimestamp < 0) {
        throw new SyncEngineError(
          'INVALID_ARGUMENT',
          `atTimestamp must be a non-negative finite number, got ${atTimestamp}`
        );
      }
      targetTime = atTimestamp;
    } else {
      targetTime = this.getMonotonicNow();
    }

    return calculateExtrapolatedPosition(this.state, targetTime);
  }

  // ── Event Processing (SyncEventReceiver implementation) ──────

  public receive(event: AnyControlSyncEvent): void {
    if (this.disposed) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_DISPOSED',
        stateVersion: this.state.stateVersion,
        decidedAt: this.lastObservedClockTime,
        message: 'SyncEngine is disposed',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsRejectedCount++;

      if (event.origin === 'local') {
        throw new SyncEngineError('DISPOSED', 'SyncEngine is disposed');
      }
      return;
    }

    let now: number;
    try {
      now = this.getMonotonicNow();
    } catch (err) {
      if (event.origin === 'local') {
        throw err;
      }
      // Malformed remote input / clock fault must not crash transport event loop
      this.mutableStats.eventsRejectedCount++;
      return;
    }

    this.mutableStats.eventsReceivedCount++;

    // 1. Pure state reduction
    const { nextState, decision } = reduceState(this.state, event, now, this.config);
    this.lastDecision = decision;

    // 2. Handle rejection
    if (!decision.accepted) {
      this.mutableStats.eventsRejectedCount++;
      this.notifyDecisionListeners(event, decision);

      if (event.origin === 'local') {
        // Throws SyncEngineError so ControlEventService aborts broadcast
        throw new SyncEngineError(
          'INVALID_PAYLOAD',
          decision.message ?? 'Event failed semantic validation'
        );
      }
      // Remote invalid event rejected safely without throwing
      return;
    }

    // 3. Handle NO_OP
    if (decision.code === 'NO_OP') {
      this.mutableStats.eventsNoOpCount++;
      this.notifyDecisionListeners(event, decision);
      return;
    }

    // 4. Handle APPLIED (Atomic State Commit)
    this.mutableStats.eventsAppliedCount++;
    this.state = nextState;

    // 5. Notify subscribers with complete exception isolation
    this.notifyStateChangeListeners(this.state);
    this.notifyDecisionListeners(event, decision);
  }

  // ── Listener Subscriptions with Exception Isolation ──────────

  public onStateChange(listener: StateChangeListener): Unsubscribe {
    if (this.disposed) return () => {};
    this.stateChangeListeners.add(listener);
    return () => {
      this.stateChangeListeners.delete(listener);
    };
  }

  public onDecision(listener: DecisionListener): Unsubscribe {
    if (this.disposed) return () => {};
    this.decisionListeners.add(listener);
    return () => {
      this.decisionListeners.delete(listener);
    };
  }

  private notifyStateChangeListeners(state: PlaybackState): void {
    // Snapshot listener set to prevent mutation issues during iteration
    for (const listener of Array.from(this.stateChangeListeners)) {
      try {
        listener(state);
        this.mutableStats.stateChangeNotificationsCount++;
      } catch {
        // Invariant: Subscriber failure must never roll back state or bubble out
        this.mutableStats.listenerErrorCount++;
      }
    }
  }

  private notifyDecisionListeners(event: AnyControlSyncEvent, decision: EngineDecision): void {
    for (const listener of Array.from(this.decisionListeners)) {
      try {
        listener(event, decision);
        this.mutableStats.decisionNotificationsCount++;
      } catch {
        this.mutableStats.listenerErrorCount++;
      }
    }
  }

  // ── Snapshot Export & Import ─────────────────────────────────

  public exportSnapshot(snapshotId?: string): StateSnapshotPayload {
    return exportEngineSnapshot(this.state, this.config.sessionId, snapshotId);
  }

  public importSnapshot(payload: StateSnapshotPayload): EngineDecision {
    if (this.disposed) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_DISPOSED',
        stateVersion: this.state.stateVersion,
        decidedAt: this.lastObservedClockTime,
        message: 'SyncEngine is disposed',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsRejectedCount++;
      return decision;
    }

    let now: number;
    try {
      now = this.getMonotonicNow();
    } catch (err) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: this.lastObservedClockTime,
        message: err instanceof Error ? err.message : String(err),
      };
      this.lastDecision = decision;
      this.mutableStats.eventsRejectedCount++;
      return decision;
    }

    // 1. Atomic pre-validation
    let validated;
    try {
      validated = validateSnapshotPayload(
        payload,
        this.config.sessionId,
        this.config.minPlaybackRate,
        this.config.maxPlaybackRate
      );
    } catch (err) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: err instanceof Error ? err.message : String(err),
      };
      this.lastDecision = decision;
      this.mutableStats.eventsRejectedCount++;
      return decision;
    }

    const { data } = validated;

    // 2. Invariant: local stateVersion = currentLocalStateVersion + 1
    // Never adopt or use remote stateVersion for local stateVersion calculation.
    const newVersion = this.state.stateVersion + 1;

    // Invariant: senderPeerId from snapshot is untrusted provenance
    const lastOp = data.lastAppliedOperation
      ? Object.freeze({
          logicalEventId: data.lastAppliedOperation.logicalEventId,
          senderPeerId: data.lastAppliedOperation.senderPeerId,
          messageType: data.lastAppliedOperation.messageType,
          appliedAtLocalTimestamp: now,
          isImportedProvenance: true,
        })
      : undefined;

    // 3. Commit state atomically (anchor timestamp reset to local clock)
    const nextState = Object.freeze<PlaybackState>({
      status: data.status,
      anchorPositionMs: data.anchorPositionMs,
      anchorLocalTimestamp: now,
      playbackRate: data.playbackRate,
      stateVersion: newVersion,
      lastAppliedEvent: lastOp,
    });

    this.state = nextState;
    this.mutableStats.eventsAppliedCount++;

    const decision: EngineDecision = {
      accepted: true,
      code: 'APPLIED',
      stateVersion: newVersion,
      decidedAt: now,
      message: `Snapshot ${validated.snapshotId} imported successfully`,
    };
    this.lastDecision = decision;

    // 4. Notify subscribers with isolated dispatch
    this.notifyStateChangeListeners(this.state);

    return decision;
  }

  // ── Disposal ─────────────────────────────────────────────────

  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.stateChangeListeners.clear();
    this.decisionListeners.clear();
  }
}
