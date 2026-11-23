/**
 * @module @syncplay/sync-engine/recovery
 *
 * Recovery Admission Controller for Phase 7.5 M4.3a.
 *
 * Coordinates admission policy across RecoveryStateMachine, QuarantineBuffer, and FutureEpochBuffer.
 * Enforces strict isolation boundaries during recovery states (RECOVERING, RECONCILING, DEGRADED)
 * while preserving full compatibility with normal operation (ACTIVE).
 *
 * Admission Policy:
 * 1. Local Intents:
 *    - ACTIVE: Allowed (passed through to normal engine submission).
 *    - RECOVERING: Rejected with code REJECTED_RECOVERING.
 *    - RECONCILING: Rejected with code REJECTED_RECOVERING.
 *    - DEGRADED: Rejected with code REJECTED_DEGRADED.
 *
 * 2. Remote Events:
 *    - Obsolete Epoch (event.epoch < currentEpoch): Discarded as DISCARD_OBSOLETE_EPOCH.
 *    - Future Epoch (event.epoch > currentEpoch): Isolated in FutureEpochBuffer (max 100 ops, 512 KiB).
 *    - Current Epoch (event.epoch === currentEpoch):
 *      - ACTIVE: Passed through to normal engine reduction pipeline.
 *      - RECOVERING / RECONCILING: Retained in QuarantineBuffer active generation.
 *      - DEGRADED: Rejected with REJECTED_DEGRADED.
 *
 * Invariants:
 * 1. Admission is strictly separated from application: admission never mutates playback state.
 * 2. Neither quarantine admission nor future-epoch buffering ever advances the replication vector.
 * 3. Local intents are fail-closed during recovery.
 * 4. DEGRADED cannot silently transition back to ACTIVE.
 */

import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { SyncEngineError } from '../errors.js';
import type { RecoveryStateMachine } from './recovery-state-machine.js';
import type { QuarantineBuffer, QuarantinedOperation } from './quarantine-buffer.js';
import type { FutureEpochBuffer } from './future-epoch-buffer.js';

export interface LocalAdmissionResult {
  readonly allowed: boolean;
  readonly code?: 'REJECTED_RECOVERING' | 'REJECTED_DEGRADED' | 'DISPOSED';
  readonly reason?: string;
}

export type RemoteAdmissionAction =
  | 'PASS_THROUGH'
  | 'QUARANTINED'
  | 'BUFFERED_FUTURE_EPOCH'
  | 'DISCARD_OBSOLETE_EPOCH'
  | 'REJECTED_DEGRADED'
  | 'REJECTED_INVALID';

export interface RemoteAdmissionResult {
  readonly action: RemoteAdmissionAction;
  readonly reason?: string;
  readonly quarantinedOp?: QuarantinedOperation;
  readonly generationId?: number;
  readonly targetEpoch?: number;
  readonly duplicate?: boolean;
}

export interface RecoveryAdmissionControllerConfig {
  readonly stateMachine: RecoveryStateMachine;
  readonly quarantineBuffer: QuarantineBuffer;
  readonly futureEpochBuffer: FutureEpochBuffer;
  readonly nowFn?: () => number;
}

interface InboundCandidateMessage {
  readonly originPeerId?: string;
  readonly senderPeerId?: string;
  readonly originSequenceNumber?: number;
  readonly logicalTimestamp?: number;
  readonly membershipEpoch?: number;
}

interface InboundCandidateClock {
  readonly originPeerId?: string;
  readonly sequenceNumber?: number;
  readonly logicalTimestamp?: number;
  readonly membershipEpoch?: number;
}

interface InboundCandidateEvent {
  readonly message?: InboundCandidateMessage;
  readonly clock?: InboundCandidateClock;
  readonly issuerPeerId?: string;
  readonly membershipEpoch?: number;
}

export class RecoveryAdmissionController {
  public readonly stateMachine: RecoveryStateMachine;
  public readonly quarantineBuffer: QuarantineBuffer;
  public readonly futureEpochBuffer: FutureEpochBuffer;

  private readonly nowFn: () => number;
  private _isDisposed = false;

  public constructor(config: RecoveryAdmissionControllerConfig) {
    if (!config.stateMachine) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'stateMachine is required');
    }
    if (!config.quarantineBuffer) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'quarantineBuffer is required');
    }
    if (!config.futureEpochBuffer) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'futureEpochBuffer is required');
    }

    this.stateMachine = config.stateMachine;
    this.quarantineBuffer = config.quarantineBuffer;
    this.futureEpochBuffer = config.futureEpochBuffer;
    this.nowFn = config.nowFn ?? Date.now;
  }

  public get isDisposed(): boolean {
    return this._isDisposed;
  }

  /**
   * Intercepts a local user intent submission and verifies whether admission is permitted
   * under current recovery lifecycle state.
   */
  public interceptLocalIntent(): LocalAdmissionResult {
    if (this._isDisposed || this.stateMachine.isDisposed) {
      return {
        allowed: false,
        code: 'DISPOSED',
        reason: 'SyncEngine or RecoveryAdmissionController is disposed',
      };
    }

    const state = this.stateMachine.state;

    switch (state) {
      case 'ACTIVE':
        return { allowed: true };

      case 'RECOVERING':
        return {
          allowed: false,
          code: 'REJECTED_RECOVERING',
          reason: 'Engine is catching up via snapshot recovery; local mutations are temporarily blocked',
        };

      case 'RECONCILING':
        return {
          allowed: false,
          code: 'REJECTED_RECOVERING',
          reason: 'Engine is reconciling post-snapshot operations; local mutations are temporarily blocked',
        };

      case 'DEGRADED':
        return {
          allowed: false,
          code: 'REJECTED_DEGRADED',
          reason: 'Engine is in DEGRADED recovery state; local mutations are blocked',
        };

      default:
        return {
          allowed: false,
          code: 'REJECTED_DEGRADED',
          reason: `Unknown recovery state: ${state as string}`,
        };
    }
  }

  /**
   * Intercepts an incoming remote control event and routes it according to current recovery state
   * and membership epoch comparison.
   *
   * @param event Validated or incoming control event.
   * @param serializedByteLength Serialized wire size of the event.
   * @param currentEpoch Current engine membership epoch.
   * @returns Routing decision and retained envelope if quarantined/buffered.
   */
  public interceptRemoteEvent(
    event: AnyControlSyncEvent,
    serializedByteLength: number,
    currentEpoch: number,
  ): RemoteAdmissionResult {
    if (this._isDisposed || this.stateMachine.isDisposed) {
      return {
        action: 'REJECTED_INVALID',
        reason: 'RecoveryAdmissionController is disposed',
      };
    }

    // 1. Structural event validation
    if (!event || typeof event !== 'object') {
      return {
        action: 'REJECTED_INVALID',
        reason: 'Malformed event: event is null or not an object',
      };
    }

    const candidate = event as unknown as InboundCandidateEvent;
    const message = candidate.message;
    const clock = candidate.clock;

    const originPeerId =
      (typeof message?.originPeerId === 'string' && message.originPeerId.trim()) ||
      (typeof message?.senderPeerId === 'string' && message.senderPeerId.trim()) ||
      (typeof clock?.originPeerId === 'string' && clock.originPeerId.trim()) ||
      (typeof candidate.issuerPeerId === 'string' && candidate.issuerPeerId.trim()) ||
      '';

    const rawSeq =
      message?.originSequenceNumber ??
      message?.logicalTimestamp ??
      clock?.sequenceNumber ??
      clock?.logicalTimestamp;
    const sequenceNumber =
      typeof rawSeq === 'number' && Number.isInteger(rawSeq) ? rawSeq : null;

    const rawEpoch =
      message?.membershipEpoch ??
      clock?.membershipEpoch ??
      candidate.membershipEpoch;
    const membershipEpoch =
      typeof rawEpoch === 'number' && Number.isInteger(rawEpoch) ? rawEpoch : null;

    if (!originPeerId || sequenceNumber === null || membershipEpoch === null) {
      return {
        action: 'REJECTED_INVALID',
        reason: 'Malformed event structure: missing origin identity, sequence number, or membership epoch',
      };
    }

    // 2. Serialized byte length validation
    if (
      typeof serializedByteLength !== 'number' ||
      !Number.isFinite(serializedByteLength) ||
      serializedByteLength <= 0 ||
      serializedByteLength > this.quarantineBuffer.maxEventBytes
    ) {
      return {
        action: 'REJECTED_INVALID',
        reason: `Invalid or oversized serialized event size: ${serializedByteLength} bytes`,
      };
    }

    // 3. Obsolete epoch handling (Case A: event.epoch < currentEpoch)
    if (membershipEpoch < currentEpoch) {
      return {
        action: 'DISCARD_OBSOLETE_EPOCH',
        reason: `Event membership epoch (${membershipEpoch}) is older than current epoch (${currentEpoch})`,
      };
    }

    // Construct quarantined operation envelope
    const op: QuarantinedOperation = {
      event,
      originPeerId,
      sequenceNumber,
      membershipEpoch,
      serializedByteLength,
      receivedAtMs: this.nowFn(),
    };

    // 4. Future epoch handling (Case B: event.epoch > currentEpoch)
    if (membershipEpoch > currentEpoch) {
      try {
        const newlyAdmitted = this.futureEpochBuffer.bufferFutureEpoch(op, currentEpoch);
        return {
          action: 'BUFFERED_FUTURE_EPOCH',
          targetEpoch: membershipEpoch,
          quarantinedOp: op,
          duplicate: !newlyAdmitted,
        };
      } catch (err) {
        if (err instanceof SyncEngineError && err.code === 'FUTURE_EPOCH_OVERFLOW') {
          throw err;
        }
        return {
          action: 'REJECTED_INVALID',
          reason: err instanceof Error ? err.message : String(err),
        };
      }
    }

    // 5. Current epoch handling (Case C: event.epoch === currentEpoch)
    const state = this.stateMachine.state;

    switch (state) {
      case 'ACTIVE':
        // Normal engine reduction
        return {
          action: 'PASS_THROUGH',
        };

      case 'RECOVERING':
      case 'RECONCILING': {
        const newlyAdmitted = this.quarantineBuffer.enqueue(op);
        return {
          action: 'QUARANTINED',
          generationId: this.quarantineBuffer.generationId,
          quarantinedOp: op,
          duplicate: !newlyAdmitted,
        };
      }

      case 'DEGRADED':
        return {
          action: 'REJECTED_DEGRADED',
          reason: 'Engine is in DEGRADED recovery state',
        };

      default:
        return {
          action: 'REJECTED_DEGRADED',
          reason: `Unknown recovery state: ${state as string}`,
        };
    }
  }

  /**
   * Disposes the admission controller.
   */
  public dispose(): void {
    if (this._isDisposed) {
      return;
    }
    this._isDisposed = true;
  }
}

