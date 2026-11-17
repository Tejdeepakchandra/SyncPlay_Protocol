/**
 * Pure functional ConflictResolver for SyncPlay distributed operations.
 *
 * Implements Phase 7 conflict resolution policies:
 * - Deterministic total-order LWW for SEEK and RATE_CHANGE
 * - Concurrency-aware intent-preserving PAUSE priority over PLAY
 * - Causally sequential PLAY resumption
 * - Orthogonal composition of SEEK with PLAY/PAUSE
 * - Deterministic replay from base snapshot
 */

import { compareOperations } from '../ordering/comparator.js';
import { calculateExtrapolatedPosition } from '../reducer.js';
import type { PlaybackState, EngineDecision, LastAppliedOperation } from '../types.js';
import type { StoredOperation } from '../log/types.js';
import type { ReducerContext, IConflictResolver } from './types.js';

export function happensBefore(
  e1: { originPeerId: string; originSequenceNumber: number },
  e2: { causalContext: Record<string, number> },
): boolean {
  const reqSeq = e2.causalContext[e1.originPeerId];
  return typeof reqSeq === 'number' && e1.originSequenceNumber <= reqSeq;
}

export function areConcurrent(
  e1: { originPeerId: string; originSequenceNumber: number; causalContext: Record<string, number> },
  e2: { originPeerId: string; originSequenceNumber: number; causalContext: Record<string, number> },
): boolean {
  if (e1.originPeerId === e2.originPeerId) {
    return false; // Same origin operations are strictly sequential
  }
  return !happensBefore(e1, e2) && !happensBefore(e2, e1);
}

export class ConflictResolver implements IConflictResolver {
  /**
   * Pure functional reduction of an operation against currentState.
   */
  public reduce(
    currentState: PlaybackState,
    operation: StoredOperation,
    monotonicNow: number,
    context: ReducerContext,
    priorOperation?: StoredOperation,
  ): { nextState: PlaybackState; decision: EngineDecision } {
    const { messageType } = operation;

    // Helper to produce LastAppliedOperation metadata
    const createOpDetails = (): LastAppliedOperation =>
      Object.freeze({
        logicalEventId: operation.logicalEventId,
        senderPeerId: operation.originPeerId,
        messageType: operation.messageType,
        appliedAtLocalTimestamp: monotonicNow,
        originPeerId: operation.originPeerId,
        logicalTimestamp: operation.logicalTimestamp,
        originSequenceNumber: operation.originSequenceNumber,
      });

    switch (messageType) {
      case 'SEEK': {
        const payload = operation.payload as { targetPositionMs?: number };
        const targetPos = payload?.targetPositionMs;

        if (typeof targetPos !== 'number' || !Number.isFinite(targetPos) || targetPos < 0) {
          return {
            nextState: currentState,
            decision: {
              accepted: false,
              code: 'REJECTED_INVALID',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: `Invalid seek targetPositionMs: ${targetPos}`,
            },
          };
        }

        // Total-order LWW: Check against prior operation if it was also a SEEK
        if (priorOperation && priorOperation.messageType === 'SEEK') {
          if (compareOperations(operation, priorOperation) < 0) {
            return {
              nextState: currentState,
              decision: {
                accepted: true,
                code: 'SUPERSEDED',
                stateVersion: currentState.stateVersion,
                decidedAt: monotonicNow,
                message: `Seek operation ${operation.logicalEventId} superseded by higher key seek ${priorOperation.logicalEventId}`,
              },
            };
          }
        }

        // Epsilon filtering: check if already at target position
        const currentPos = currentState.anchorPositionMs;
        if (Math.abs(targetPos - currentPos) <= context.positionEpsilonMs) {
          return {
            nextState: currentState,
            decision: {
              accepted: true,
              code: 'NO_OP',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: `Seek target ${targetPos}ms matches current position within epsilon`,
            },
          };
        }

        const nextState = Object.freeze<PlaybackState>({
          status: currentState.status,
          anchorPositionMs: targetPos,
          anchorLocalTimestamp: monotonicNow,
          playbackRate: currentState.playbackRate,
          stateVersion: currentState.stateVersion + 1,
          lastAppliedEvent: createOpDetails(),
        });

        return {
          nextState,
          decision: {
            accepted: true,
            code: 'APPLIED',
            stateVersion: nextState.stateVersion,
            decidedAt: monotonicNow,
          },
        };
      }

      case 'PAUSE': {
        if (currentState.status === 'paused') {
          return {
            nextState: currentState,
            decision: {
              accepted: true,
              code: 'NO_OP',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: 'Already paused',
            },
          };
        }

        // Playing -> Paused
        const frozenPos = calculateExtrapolatedPosition(currentState, monotonicNow);
        const nextState = Object.freeze<PlaybackState>({
          status: 'paused',
          anchorPositionMs: frozenPos,
          anchorLocalTimestamp: monotonicNow,
          playbackRate: currentState.playbackRate,
          stateVersion: currentState.stateVersion + 1,
          lastAppliedEvent: createOpDetails(),
        });

        return {
          nextState,
          decision: {
            accepted: true,
            code: 'APPLIED',
            stateVersion: nextState.stateVersion,
            decidedAt: monotonicNow,
          },
        };
      }

      case 'PLAY': {
        const payload = operation.payload as { targetPositionMs?: number };
        const targetPos = payload?.targetPositionMs;

        if (targetPos !== undefined) {
          if (typeof targetPos !== 'number' || !Number.isFinite(targetPos) || targetPos < 0) {
            return {
              nextState: currentState,
              decision: {
                accepted: false,
                code: 'REJECTED_INVALID',
                stateVersion: currentState.stateVersion,
                decidedAt: monotonicNow,
                message: `Invalid play targetPositionMs: ${targetPos}`,
              },
            };
          }
        }

        // Check Concurrency-Aware Pause Priority against prior operation
        if (priorOperation && priorOperation.messageType === 'PAUSE') {
          if (areConcurrent(priorOperation, operation)) {
            // Concurrent PAUSE takes priority over PLAY!
            return {
              nextState: currentState,
              decision: {
                accepted: true,
                code: 'SUPERSEDED',
                stateVersion: currentState.stateVersion,
                decidedAt: monotonicNow,
                message: `Concurrent PLAY ${operation.logicalEventId} superseded by PAUSE priority from ${priorOperation.logicalEventId}`,
              },
            };
          }
        }

        if (currentState.status === 'paused') {
          // Paused -> Playing
          const anchorPos = targetPos !== undefined ? targetPos : currentState.anchorPositionMs;
          const nextState = Object.freeze<PlaybackState>({
            status: 'playing',
            anchorPositionMs: anchorPos,
            anchorLocalTimestamp: monotonicNow,
            playbackRate: currentState.playbackRate,
            stateVersion: currentState.stateVersion + 1,
            lastAppliedEvent: createOpDetails(),
          });

          return {
            nextState,
            decision: {
              accepted: true,
              code: 'APPLIED',
              stateVersion: nextState.stateVersion,
              decidedAt: monotonicNow,
            },
          };
        }

        // Already playing
        if (targetPos === undefined) {
          return {
            nextState: currentState,
            decision: {
              accepted: true,
              code: 'NO_OP',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: 'Already playing with unanchored resume',
            },
          };
        }

        const currentPos = calculateExtrapolatedPosition(currentState, monotonicNow);
        if (Math.abs(targetPos - currentPos) <= context.positionEpsilonMs) {
          return {
            nextState: currentState,
            decision: {
              accepted: true,
              code: 'NO_OP',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: `Target position ${targetPos} is within epsilon of current position`,
            },
          };
        }

        const nextState = Object.freeze<PlaybackState>({
          status: 'playing',
          anchorPositionMs: targetPos,
          anchorLocalTimestamp: monotonicNow,
          playbackRate: currentState.playbackRate,
          stateVersion: currentState.stateVersion + 1,
          lastAppliedEvent: createOpDetails(),
        });

        return {
          nextState,
          decision: {
            accepted: true,
            code: 'APPLIED',
            stateVersion: nextState.stateVersion,
            decidedAt: monotonicNow,
          },
        };
      }

      case 'RATE_CHANGE': {
        const payload = operation.payload as { playbackRate?: number };
        const rawRate = payload?.playbackRate;

        if (
          typeof rawRate !== 'number' ||
          !Number.isFinite(rawRate) ||
          rawRate <= 0 ||
          rawRate < context.minPlaybackRate ||
          rawRate > context.maxPlaybackRate
        ) {
          return {
            nextState: currentState,
            decision: {
              accepted: false,
              code: 'REJECTED_INVALID',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: `Invalid playbackRate: ${rawRate}`,
            },
          };
        }

        // Scalar LWW check if prior was also RATE_CHANGE
        if (priorOperation && priorOperation.messageType === 'RATE_CHANGE') {
          if (compareOperations(operation, priorOperation) < 0) {
            return {
              nextState: currentState,
              decision: {
                accepted: true,
                code: 'SUPERSEDED',
                stateVersion: currentState.stateVersion,
                decidedAt: monotonicNow,
                message: `Rate change ${operation.logicalEventId} superseded by higher key rate change`,
              },
            };
          }
        }

        if (Math.abs(rawRate - currentState.playbackRate) < 1e-4) {
          return {
            nextState: currentState,
            decision: {
              accepted: true,
              code: 'NO_OP',
              stateVersion: currentState.stateVersion,
              decidedAt: monotonicNow,
              message: `Playback rate is already ${rawRate}`,
            },
          };
        }

        const anchorPos =
          currentState.status === 'playing'
            ? calculateExtrapolatedPosition(currentState, monotonicNow)
            : currentState.anchorPositionMs;

        const nextState = Object.freeze<PlaybackState>({
          status: currentState.status,
          anchorPositionMs: anchorPos,
          anchorLocalTimestamp: monotonicNow,
          playbackRate: rawRate,
          stateVersion: currentState.stateVersion + 1,
          lastAppliedEvent: createOpDetails(),
        });

        return {
          nextState,
          decision: {
            accepted: true,
            code: 'APPLIED',
            stateVersion: nextState.stateVersion,
            decidedAt: monotonicNow,
          },
        };
      }

      default: {
        return {
          nextState: currentState,
          decision: {
            accepted: false,
            code: 'REJECTED_INVALID',
            stateVersion: currentState.stateVersion,
            decidedAt: monotonicNow,
            message: `Unsupported message type: ${messageType}`,
          },
        };
      }
    }
  }

  /**
   * Deterministic replay from baseState across all operations.
   */
  public replay(
    baseState: PlaybackState,
    operations: ReadonlyArray<StoredOperation>,
    monotonicNow: number,
    context: ReducerContext,
  ): { finalState: PlaybackState; decisions: ReadonlyArray<EngineDecision> } {
    let state = baseState;
    const decisions: EngineDecision[] = [];

    for (let i = 0; i < operations.length; i++) {
      const op = operations[i]!;
      const priorOp = i > 0 ? operations[i - 1] : undefined;

      const result = this.reduce(state, op, monotonicNow, context, priorOp);
      state = result.nextState;
      decisions.push(result.decision);
    }

    return { finalState: state, decisions };
  }
}
