/**
 * @module @syncplay/sync-engine/recovery
 *
 * Recovery Coordinator for Phase 7.5 M4.3c.
 *
 * Master recovery orchestration and quarantine-draining component.
 * Integrates:
 * - RecoveryStateMachine (ACTIVE -> RECOVERING -> RECONCILING -> ACTIVE / DEGRADED)
 * - QuarantineBuffer & FutureEpochBuffer
 * - RecoveryAdmissionController
 * - SnapshotInstaller (Four-Stage Atomic Commit Point)
 * - QuarantineDrainer (DRAINER_IN_FLIGHT with N_in-flight <= 1)
 * - LeaderlessSyncEngine
 *
 * Mandatory M4.3b & M4.3c Invariants:
 * 1. Current Recovery Counters: Explicitly passes live engine.logicalClock and
 *    engine.getDiagnosticCoreSnapshot().localOriginSequence during staging.
 *    Never relies on zero defaults for an active engine.
 * 2. Capability Custody: Mediates atomic installation using the exact authorized
 *    RecoveryInstallationCapability registered at engine initialization.
 * 3. In-Flight Drain Ownership: Manages quarantine replay via QuarantineDrainer,
 *    ensuring zero operation loss on capacity exhaustion or post-dequeue exception.
 * 4. Reentrancy Exclusion: Isolates callbacks and defers engine state notifications
 *    until the handoff to ACTIVE completes synchronously.
 */

import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type { StateSnapshotPayloadV2, SnapshotOfferPayload } from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import type { LeaderlessSyncEngine, RecoveryInstallationCapability } from '../engine/leaderless-sync-engine.js';
import {
  RecoveryStateMachine,
  type RecoveryState,
  type RecoverySubstate,
  type RecoveryStateListener,
  type RecoverySubstateListener,
} from './recovery-state-machine.js';
import {
  QuarantineBuffer,
  QUARANTINE_WARNING_THRESHOLD_BYTES,
} from './quarantine-buffer.js';
import { FutureEpochBuffer } from './future-epoch-buffer.js';
import {
  RecoveryAdmissionController,
  type LocalAdmissionResult,
  type RemoteAdmissionResult,
} from './recovery-admission-controller.js';
import {
  SnapshotInstaller,
  evaluateSnapshotEligibility,
  type SnapshotInstallationReport,
  type StagedSnapshotInstallation,
} from './snapshot-installer.js';
import {
  QuarantineDrainer,
  type DrainedInFlightRecord,
  type QuarantineDrainResult,
} from './quarantine-drainer.js';

export type RecoveryFailureReason =
  | 'NO_OFFERS_AVAILABLE'
  | 'ALL_CANDIDATES_EXHAUSTED'
  | 'OVERALL_RECOVERY_TIMEOUT'
  | 'RECONCILIATION_TIMEOUT'
  | 'QUARANTINE_OVERFLOW'
  | 'FUTURE_EPOCH_OVERFLOW'
  | 'PAYLOAD_CORRUPTED'
  | 'CAUSAL_DIVERGENCE'
  | 'EPOCH_MISMATCH'
  | 'UNRESOLVED_FUTURE_EPOCH'
  | 'ENGINE_DISPOSED'
  | 'APPLICATION_ABORT';

export interface RecoveryMetrics {
  readonly totalDurationMs: number;
  readonly transferDurationMs: number;
  readonly drainDurationMs: number;
  readonly chunksReceived: number;
  readonly failoversCount: number;
  readonly quarantinedApplied: number;
  readonly quarantinedDiscarded: number;
}

export interface RecoveryCoordinatorCallbacks {
  readonly onStateChange?: (newState: RecoveryState, oldState: RecoveryState, reason: string) => void;
  readonly onSubstateChange?: (newSubstate: RecoverySubstate, oldSubstate: RecoverySubstate) => void;
  readonly onQuarantineWarning?: (currentBytes: number, currentCount: number) => void;
  readonly onRecoveryDegraded?: (reason: RecoveryFailureReason, error?: Error) => void;
  readonly onRecoveryCompleted?: (metrics: RecoveryMetrics) => void;
}

export interface RecoveryCoordinatorConfig {
  readonly engine: LeaderlessSyncEngine;
  readonly capability: RecoveryInstallationCapability;
  readonly stateMachine?: RecoveryStateMachine;
  readonly quarantineBuffer?: QuarantineBuffer;
  readonly futureEpochBuffer?: FutureEpochBuffer;
  readonly admissionController?: RecoveryAdmissionController;
  readonly snapshotInstaller?: SnapshotInstaller;
  readonly quarantineDrainer?: QuarantineDrainer;
  readonly overallRecoveryTimeoutMs?: number;      // Default: 120_000ms
  readonly reconciliationTimeoutMs?: number;       // Default: 15_000ms
  readonly offerAwaitTimeoutMs?: number;           // Default: 10_000ms
  readonly epochConvergenceGraceMs?: number;       // Default: 5_000ms
  readonly quarantineWarningThresholdBytes?: number; // Configurable warning threshold
  readonly nowFn?: () => number;
  readonly callbacks?: RecoveryCoordinatorCallbacks;
}

export interface IRecoveryCoordinator {
  readonly state: RecoveryState;
  readonly substate: RecoverySubstate;
  readonly isDisposed: boolean;
  readonly inFlightRecord: DrainedInFlightRecord | null;
  readonly currentAttemptId: number;

  start(): void;
  dispose(): void;

  triggerRecovery(peerId: string, reason: string): void;
  resetRecovery(initiatedBy: string): void;

  handleSnapshotOffer(
    offer: SnapshotOfferPayload,
    context?: { supplierPeerId: string; connectionGeneration?: number },
  ): boolean;

  executeRecoveryWithSnapshot(
    rawPayload: string | StateSnapshotPayloadV2,
  ): Promise<SnapshotInstallationReport>;

  reconcileQuarantine(): Promise<QuarantineDrainResult>;

  abortRecovery(reason: RecoveryFailureReason, errorOrMessage?: Error | string): void;

  interceptRemoteEvent(
    event: AnyControlSyncEvent,
    rawSerializedLength: number,
  ): { shouldQuarantine: boolean; dropped?: boolean; duplicate?: boolean };

  interceptLocalSubmission(): { allowed: boolean; code?: string };

  getMetrics(): RecoveryMetrics;
}

export class RecoveryCoordinator implements IRecoveryCoordinator {
  public readonly engine: LeaderlessSyncEngine;
  public readonly capability: RecoveryInstallationCapability;
  public readonly stateMachine: RecoveryStateMachine;
  public readonly quarantineBuffer: QuarantineBuffer;
  public readonly futureEpochBuffer: FutureEpochBuffer;
  public readonly admissionController: RecoveryAdmissionController;
  public readonly snapshotInstaller: SnapshotInstaller;
  public readonly quarantineDrainer: QuarantineDrainer;

  private readonly overallRecoveryTimeoutMs: number;
  private readonly reconciliationTimeoutMs: number;
  private readonly offerAwaitTimeoutMs: number;
  private readonly epochConvergenceGraceMs: number;
  private readonly quarantineWarningThresholdBytes: number;
  private readonly nowFn: () => number;
  private readonly callbacks?: RecoveryCoordinatorCallbacks;

  private _isDisposed = false;
  private activeAttemptId = 0;
  private overallTimer: ReturnType<typeof setTimeout> | null = null;
  private offerAwaitTimer: ReturnType<typeof setTimeout> | null = null;
  private reconciliationTimer: ReturnType<typeof setTimeout> | null = null;

  // Metrics tracking
  private recoveryStartMs = 0;
  private transferStartMs = 0;
  private transferEndMs = 0;
  private drainStartMs = 0;
  private drainEndMs = 0;
  private chunksReceivedCount = 0;
  private failoversCount = 0;
  private totalQuarantinedApplied = 0;
  private totalQuarantinedDiscarded = 0;

  constructor(config: RecoveryCoordinatorConfig) {
    if (!config || typeof config !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'RecoveryCoordinatorConfig must be a non-null object');
    }
    if (!config.engine) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'engine is required in RecoveryCoordinatorConfig');
    }
    if (!config.capability) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'capability is required in RecoveryCoordinatorConfig');
    }

    this.engine = config.engine;
    this.capability = config.capability;
    this.nowFn = config.nowFn ?? Date.now;
    this.callbacks = config.callbacks;

    this.overallRecoveryTimeoutMs = config.overallRecoveryTimeoutMs ?? 120_000;
    this.reconciliationTimeoutMs = config.reconciliationTimeoutMs ?? 15_000;
    this.offerAwaitTimeoutMs = config.offerAwaitTimeoutMs ?? 10_000;
    this.epochConvergenceGraceMs = config.epochConvergenceGraceMs ?? 5_000;
    this.quarantineWarningThresholdBytes =
      config.quarantineWarningThresholdBytes ?? QUARANTINE_WARNING_THRESHOLD_BYTES;

    this.stateMachine = config.stateMachine ?? new RecoveryStateMachine('ACTIVE', 'IDLE');
    this.quarantineBuffer = config.quarantineBuffer ?? new QuarantineBuffer();
    this.futureEpochBuffer = config.futureEpochBuffer ?? new FutureEpochBuffer();

    this.admissionController =
      config.admissionController ??
      new RecoveryAdmissionController({
        stateMachine: this.stateMachine,
        quarantineBuffer: this.quarantineBuffer,
        futureEpochBuffer: this.futureEpochBuffer,
        nowFn: this.nowFn,
      });

    this.snapshotInstaller = config.snapshotInstaller ?? new SnapshotInstaller();

    this.quarantineDrainer =
      config.quarantineDrainer ??
      new QuarantineDrainer({
        destination: this.engine,
        quarantineBuffer: this.quarantineBuffer,
        nowFn: this.nowFn,
      });

    this.wireListeners();
  }

  public get state(): RecoveryState {
    return this.stateMachine.state;
  }

  public get substate(): RecoverySubstate {
    return this.stateMachine.substate;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  public get inFlightRecord(): DrainedInFlightRecord | null {
    return this.quarantineDrainer.inFlightRecord;
  }

  public get currentAttemptId(): number {
    return this.activeAttemptId;
  }

  /**
   * Evaluates whether the specified recovery attempt remains authoritative and uninvalidated.
   */
  private isAttemptValid(attemptId: number, expectedState?: RecoveryState): boolean {
    if (this._isDisposed) return false;
    if (this.activeAttemptId !== attemptId) return false;
    if (expectedState && this.stateMachine.state !== expectedState) return false;
    return true;
  }

  /**
   * Asserts that the specified recovery attempt is active, uninvalidated, and matches expected state.
   */
  private assertAttemptValid(attemptId: number, expectedState: RecoveryState, stage: string): void {
    if (!this.isAttemptValid(attemptId, expectedState)) {
      throw new SyncEngineError(
        'DISPOSED',
        `Recovery attempt ${attemptId} invalidated during ${stage} (current: ${this.activeAttemptId}, state: ${this.stateMachine.state}, disposed: ${this._isDisposed})`,
      );
    }
  }

  private wireListeners(): void {
    this.stateMachine.onStateChange((newState, oldState, reason) => {
      try {
        this.callbacks?.onStateChange?.(newState, oldState, reason);
      } catch {
        // Exception isolation
      }
    });

    this.stateMachine.onSubstateChange((newSubstate, oldSubstate) => {
      try {
        this.callbacks?.onSubstateChange?.(newSubstate, oldSubstate);
      } catch {
        // Exception isolation
      }
    });
  }

  public start(): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'RecoveryCoordinator is disposed');
    }
  }

  /**
   * Initiates the recovery lifecycle, moving engine to RECOVERING and setting up watchdog timers.
   */
  public triggerRecovery(peerId: string, reason: string): void {
    if (this._isDisposed) {
      throw new SyncEngineError('DISPOSED', 'RecoveryCoordinator is disposed');
    }

    // Only one coordinator-owned recovery attempt may control a given engine at a time
    if (this.stateMachine.state === 'RECOVERING' || this.stateMachine.state === 'RECONCILING') {
      return;
    }

    this.activeAttemptId += 1;
    this.clearTimers();
    this.recoveryStartMs = this.nowFn();
    this.transferStartMs = this.recoveryStartMs;
    this.transferEndMs = 0;
    this.drainStartMs = 0;
    this.drainEndMs = 0;

    // Transition state machine
    this.stateMachine.transition('RECOVERING', reason, 'AWAITING_OFFER');

    // Engine enters recovery mode: blocks local intents and defers notifications
    this.engine.setRecoveryMode(true);

    // Watchdog timers
    this.overallTimer = setTimeout(() => {
      this.handleOverallTimeout();
    }, this.overallRecoveryTimeoutMs);

    this.offerAwaitTimer = setTimeout(() => {
      this.handleOfferAwaitTimeout();
    }, this.offerAwaitTimeoutMs);
  }

  /**
   * Evaluates an inbound snapshot offer from a transport supplier.
   */
  public handleSnapshotOffer(
    offer: SnapshotOfferPayload,
    _context?: { supplierPeerId: string; connectionGeneration?: number },
  ): boolean {
    if (this._isDisposed || this.state !== 'RECOVERING' || this.substate !== 'AWAITING_OFFER') {
      return false;
    }

    const eligibility = evaluateSnapshotEligibility({
      candidateSnapshot: offer as any,
      currentReplicationVector: this.engine.replicationFrontier,
      currentEpoch: this.engine.epoch,
    });

    if (!eligibility.eligible) {
      return false;
    }

    // Admissible offer found: transition substate to TRANSFERRING
    if (this.offerAwaitTimer) {
      clearTimeout(this.offerAwaitTimer);
      this.offerAwaitTimer = null;
    }

    this.stateMachine.setSubstate('TRANSFERRING');
    return true;
  }

  /**
   * Executes the complete snapshot installation and reconciliation workflow.
   * Enforces exact counter preservation (M4.3b integration) and single-root atomic commit.
   */
  public async executeRecoveryWithSnapshot(
    rawPayload: string | StateSnapshotPayloadV2,
  ): Promise<SnapshotInstallationReport> {
    if (this._isDisposed) {
      throw new SyncEngineError('ENGINE_DISPOSED', 'RecoveryCoordinator is disposed');
    }

    // If not already in RECOVERING, initiate it
    if (this.stateMachine.state !== 'RECOVERING') {
      this.triggerRecovery('local', 'Snapshot execution initiated');
    }

    const attemptId = this.activeAttemptId;
    this.assertAttemptValid(attemptId, 'RECOVERING', 'pre-validation');

    this.transferEndMs = this.nowFn();

    // ── STAGE 1: VALIDATING ─────────────────────────────────────────
    this.stateMachine.setSubstate('VALIDATING');

    let validation;
    try {
      validation = await this.snapshotInstaller.validate(
        rawPayload,
        this.engine.epoch,
        this.engine.replicationFrontier,
      );
    } catch (err) {
      this.assertAttemptValid(attemptId, 'RECOVERING', 'post-validation-catch');
      const msg = err instanceof Error ? err.message : String(err);
      this.abortRecovery('PAYLOAD_CORRUPTED', msg);
      throw new SyncEngineError('SNAPSHOT_INSTALLATION_FAILED', `Snapshot validation exception: ${msg}`);
    }

    this.assertAttemptValid(attemptId, 'RECOVERING', 'post-validation');

    if (!validation.valid || !validation.parsedPayload) {
      const reason = validation.errorReason ?? 'Snapshot validation failed';
      this.abortRecovery('PAYLOAD_CORRUPTED', reason);
      throw new SyncEngineError('SNAPSHOT_INSTALLATION_FAILED', reason);
    }

    // ── STAGE 2: STAGING (COUNTER PRESERVATION INTEGRATION) ─────────
    this.assertAttemptValid(attemptId, 'RECOVERING', 'pre-staging');
    this.stateMachine.setSubstate('STAGING');

    // Authoritative counter preservation from the target engine root:
    // Never permits defaulting to 0 for an active engine!
    const currentLogicalClock = this.engine.logicalClock;
    const currentLocalOriginSequence = this.engine.getDiagnosticCoreSnapshot().localOriginSequence;
    const reducerFn = this.engine.getReducerFunction();

    let staged: StagedSnapshotInstallation;
    try {
      staged = this.snapshotInstaller.stage(
        validation,
        (this.engine as any).config,
        {
          currentLogicalClock,
          currentLocalOriginSequence,
          reducerFn,
          now: this.nowFn(),
        },
      );
    } catch (err) {
      this.assertAttemptValid(attemptId, 'RECOVERING', 'post-staging-catch');
      const msg = err instanceof Error ? err.message : String(err);
      this.abortRecovery('APPLICATION_ABORT', msg);
      throw err;
    }

    // ── STAGE 3: COMMITTING (ATOMIC STATE-ROOT REPLACEMENT) ──────────
    this.assertAttemptValid(attemptId, 'RECOVERING', 'pre-committing');
    this.stateMachine.setSubstate('COMMITTING');

    let report: SnapshotInstallationReport;
    try {
      // Passes the exact authorized RecoveryInstallationCapability held by this coordinator:
      report = this.snapshotInstaller.commitAtomic(this.engine, staged, this.capability);
    } catch (err) {
      this.assertAttemptValid(attemptId, 'RECOVERING', 'post-committing-catch');
      const msg = err instanceof Error ? err.message : String(err);
      this.abortRecovery('APPLICATION_ABORT', msg);
      throw err;
    }

    // ── STAGE 4: RECONCILING & QUARANTINE DRAIN ──────────────────────
    this.assertAttemptValid(attemptId, 'RECOVERING', 'pre-reconcile-transition');
    this.stateMachine.transition(
      'RECONCILING',
      'Snapshot installed successfully; reconciling quarantine',
      'DRAINING_QUARANTINE',
    );

    // Start reconciliation watchdog timer with timeout race
    if (this.reconciliationTimer) {
      clearTimeout(this.reconciliationTimer);
      this.reconciliationTimer = null;
    }

    let timeoutTimer: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutTimer = setTimeout(() => {
        this.handleReconciliationTimeout();
        reject(
          new SyncEngineError(
            'SNAPSHOT_INSTALLATION_FAILED',
            'Quarantine drain exceeded max allowed duration (RECONCILIATION_TIMEOUT)',
          ),
        );
      }, this.reconciliationTimeoutMs);
      this.reconciliationTimer = timeoutTimer;
    });

    this.drainStartMs = this.nowFn();

    // Execute quarantine drain
    try {
      await Promise.race([this.reconcileQuarantine(attemptId), timeoutPromise]);
    } finally {
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
      }
      if (this.reconciliationTimer === timeoutTimer) {
        this.reconciliationTimer = null;
      }
    }

    this.drainEndMs = this.nowFn();

    // ── SYNCHRONOUS HANDOFF TO ACTIVE ───────────────────────────────
    // Critical Section: executed in a single synchronous call frame without await
    // Inverted ordering correction: setRecoveryMode(false) occurs BEFORE transition('ACTIVE')
    // so any onStateChange listener observing ACTIVE sees engine.isRecoveryActive() === false.
    this.assertAttemptValid(attemptId, 'RECONCILING', 'pre-active-handoff');
    this.engine.setRecoveryMode(false);
    try {
      this.stateMachine.transition('ACTIVE', 'Recovery and reconciliation successfully completed', 'IDLE');
    } catch (err) {
      this.engine.setRecoveryMode(true);
      throw err;
    }

    // Outside critical section:
    this.clearTimers();
    this.engine.flushDeferredNotifications();

    const metrics = this.getMetrics();
    try {
      this.callbacks?.onRecoveryCompleted?.(metrics);
    } catch {
      // Exception isolation
    }

    return report;
  }

  /**
   * Drains the quarantine buffer through the QuarantineDrainer until completely empty.
   * Performs generational swaps if new operations arrived during drain passes.
   */
  public async reconcileQuarantine(attemptId: number = this.activeAttemptId): Promise<QuarantineDrainResult> {
    if (this._isDisposed) {
      throw new SyncEngineError('ENGINE_DISPOSED', 'RecoveryCoordinator is disposed');
    }
    this.assertAttemptValid(attemptId, 'RECONCILING', 'pre-reconcile-quarantine');

    let aggregatedApplied = 0;
    let aggregatedPending = 0;
    let aggregatedDiscarded = 0;
    let aggregatedBatchSize = 0;

    while (this.quarantineBuffer.size > 0 || this.quarantineBuffer.drainSize > 0) {
      this.assertAttemptValid(attemptId, 'RECONCILING', 'loop-start');
      this.quarantineBuffer.swapDrainBatch();

      const result = await this.quarantineDrainer.drainBatch();
      this.assertAttemptValid(attemptId, 'RECONCILING', 'post-drain-batch');

      aggregatedApplied += result.appliedCount;
      aggregatedPending += result.pendingCount;
      aggregatedDiscarded += result.discardedCount;
      aggregatedBatchSize += result.batchSize;

      this.totalQuarantinedApplied += result.appliedCount;
      this.totalQuarantinedDiscarded += result.discardedCount;

      if (result.haltedOnCapacity) {
        this.abortRecovery('QUARANTINE_OVERFLOW', 'Pending dependency queue capacity exhausted during drain');
        throw new SyncEngineError('REJECTED_CAPACITY', 'Pending dependency queue capacity exhausted during drain');
      }

      if (result.haltedOnError) {
        this.abortRecovery('APPLICATION_ABORT', result.haltedOnError);
        throw new SyncEngineError('SNAPSHOT_INSTALLATION_FAILED', `Drain error: ${result.haltedOnError}`);
      }

      // Check if active queue is completely clear
      if (this.quarantineBuffer.size === 0) {
        break;
      }
    }

    this.assertAttemptValid(attemptId, 'RECONCILING', 'pre-resolving-gaps');
    this.stateMachine.setSubstate('RESOLVING_GAPS');

    return {
      batchSize: aggregatedBatchSize,
      appliedCount: aggregatedApplied,
      pendingCount: aggregatedPending,
      discardedCount: aggregatedDiscarded,
      haltedOnCapacity: false,
    };
  }

  /**
   * Intercepts remote events, applying quarantine admission rules and filtering duplicate
   * in-flight operations.
   */
  public interceptRemoteEvent(
    event: AnyControlSyncEvent,
    rawSerializedLength: number,
  ): { shouldQuarantine: boolean; dropped?: boolean; duplicate?: boolean } {
    if (this._isDisposed) {
      return { shouldQuarantine: false, dropped: true };
    }

    const originPeerId = event.message.originPeerId ?? event.message.senderPeerId;
    const originSeq = event.message.originSequenceNumber ?? 1;

    // Check if duplicate of an operation currently in DRAINER_IN_FLIGHT ownership
    if (this.quarantineDrainer.isInFlight(originPeerId, originSeq)) {
      return { shouldQuarantine: false, dropped: true, duplicate: true };
    }

    const result = this.admissionController.interceptRemoteEvent(
      event,
      rawSerializedLength,
      this.engine.epoch,
    );

    // Quarantine warning threshold check
    if (
      result.action === 'QUARANTINED' &&
      this.quarantineBuffer.totalRetainedBytes >= this.quarantineWarningThresholdBytes
    ) {
      try {
        this.callbacks?.onQuarantineWarning?.(
          this.quarantineBuffer.totalRetainedBytes,
          this.quarantineBuffer.totalRetainedCount,
        );
      } catch {
        // Exception isolation
      }
    }

    const shouldQuarantine = result.action === 'QUARANTINED';
    const dropped =
      result.action === 'DISCARD_OBSOLETE_EPOCH' ||
      result.action === 'REJECTED_DEGRADED' ||
      result.action === 'REJECTED_INVALID';

    return {
      shouldQuarantine,
      dropped,
      duplicate: result.duplicate,
    };
  }

  /**
   * Intercepts local user intents, fail-closing during recovery states.
   */
  public interceptLocalSubmission(): LocalAdmissionResult {
    if (this._isDisposed) {
      return { allowed: false, code: 'DISPOSED', reason: 'RecoveryCoordinator is disposed' };
    }
    return this.admissionController.interceptLocalIntent();
  }

  /**
   * Resets active recovery attempt, restoring engine to ACTIVE or idle state.
   */
  public resetRecovery(initiatedBy: string): void {
    if (this._isDisposed) return;
    this.activeAttemptId += 1;
    this.clearTimers();

    if (
      (this.stateMachine.state === 'RECOVERING' || this.stateMachine.state === 'RECONCILING') &&
      this.stateMachine.canTransition('ACTIVE')
    ) {
      this.engine.setRecoveryMode(false);
      try {
        this.stateMachine.transition('ACTIVE', `Recovery reset by ${initiatedBy}`, 'IDLE');
      } catch (err) {
        this.engine.setRecoveryMode(true);
        throw err;
      }
      this.engine.flushDeferredNotifications();
    }
  }

  /**
   * Aborts active recovery attempt and transitions deterministically to DEGRADED.
   */
  public abortRecovery(reason: RecoveryFailureReason, errorOrMessage?: Error | string): void {
    this.activeAttemptId += 1;
    this.clearTimers();

    const msg =
      errorOrMessage instanceof Error
        ? errorOrMessage.message
        : (errorOrMessage ?? reason);

    if (this.stateMachine.canTransition('DEGRADED')) {
      this.stateMachine.transition('DEGRADED', msg, 'IDLE');
    }

    try {
      this.callbacks?.onRecoveryDegraded?.(
        reason,
        errorOrMessage instanceof Error ? errorOrMessage : new Error(msg),
      );
    } catch {
      // Exception isolation
    }
  }

  private handleOverallTimeout(): void {
    if (this.stateMachine.state === 'RECOVERING' || this.stateMachine.state === 'RECONCILING') {
      this.abortRecovery('OVERALL_RECOVERY_TIMEOUT', 'Recovery exceeded max allowed duration');
    }
  }

  private handleOfferAwaitTimeout(): void {
    if (this.stateMachine.state === 'RECOVERING' && this.stateMachine.substate === 'AWAITING_OFFER') {
      this.abortRecovery('NO_OFFERS_AVAILABLE', 'No admissible snapshot offers received within timeout');
    }
  }

  private handleReconciliationTimeout(): void {
    if (this.stateMachine.state === 'RECONCILING') {
      this.abortRecovery('RECONCILIATION_TIMEOUT', 'Quarantine drain exceeded max allowed duration');
    }
  }

  private clearTimers(): void {
    if (this.overallTimer) {
      clearTimeout(this.overallTimer);
      this.overallTimer = null;
    }
    if (this.offerAwaitTimer) {
      clearTimeout(this.offerAwaitTimer);
      this.offerAwaitTimer = null;
    }
    if (this.reconciliationTimer) {
      clearTimeout(this.reconciliationTimer);
      this.reconciliationTimer = null;
    }
  }

  public getMetrics(): RecoveryMetrics {
    const totalDurationMs =
      this.recoveryStartMs > 0 ? this.nowFn() - this.recoveryStartMs : 0;
    const transferDurationMs =
      this.transferEndMs > this.transferStartMs
        ? this.transferEndMs - this.transferStartMs
        : 0;
    const drainDurationMs =
      this.drainEndMs > this.drainStartMs
        ? this.drainEndMs - this.drainStartMs
        : 0;

    return {
      totalDurationMs,
      transferDurationMs,
      drainDurationMs,
      chunksReceived: this.chunksReceivedCount,
      failoversCount: this.failoversCount,
      quarantinedApplied: this.totalQuarantinedApplied,
      quarantinedDiscarded: this.totalQuarantinedDiscarded,
    };
  }

  /**
   * Terminal disposal. Releases all timers, buffers, drainer, and underlying state machines.
   */
  public dispose(): void {
    if (this._isDisposed) {
      return;
    }
    this._isDisposed = true;
    this.activeAttemptId += 1;
    this.clearTimers();

    this.quarantineDrainer.dispose();
    this.admissionController.dispose();
    this.quarantineBuffer.dispose();
    this.futureEpochBuffer.dispose();
    this.stateMachine.dispose();
  }
}
