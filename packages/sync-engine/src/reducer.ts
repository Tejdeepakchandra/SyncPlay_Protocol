/**
 * Pure state reducer and continuous position extrapolation for SyncEngine.
 *
 * Implements deterministic local state transitions for PLAY, PAUSE, SEEK, and RATE_CHANGE.
 */

import { MessageType } from '@syncplay/protocol';
import type {
  PlayPayload,
  PausePayload,
  SeekPayload,
  RateChangePayload,
} from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type {
  PlaybackState,
  EngineDecision,
  LastAppliedOperation,
} from './types.js';
import type { ResolvedSyncEngineConfig } from './config.js';

/**
 * Calculates continuous media position in milliseconds from an anchor state.
 */
export function calculateExtrapolatedPosition(
  state: PlaybackState,
  atTimestamp: number
): number {
  if (state.status === 'paused') {
    return state.anchorPositionMs;
  }

  // Clamped so position never travels backward in time during forward playback
  const elapsedMs = Math.max(0, atTimestamp - state.anchorLocalTimestamp);
  const extrapolated = state.anchorPositionMs + elapsedMs * state.playbackRate;

  // Prevent overflow from returning Infinity
  return Number.isFinite(extrapolated)
    ? Math.min(extrapolated, Number.MAX_SAFE_INTEGER)
    : Number.MAX_SAFE_INTEGER;
}

export interface ReducerResult {
  readonly nextState: PlaybackState;
  readonly decision: EngineDecision;
}

/**
 * Pure state reducer applying an event to the current locally authoritative playback state.
 */
export function reduceState(
  currentState: PlaybackState,
  event: AnyControlSyncEvent,
  now: number,
  config: ResolvedSyncEngineConfig
): ReducerResult {
  const messageType = event.message.messageType;

  // Validate that control events carry a mandatory, non-empty logicalEventId
  const logicalEventId = event.message.logicalEventId;
  if (!logicalEventId || typeof logicalEventId !== 'string' || logicalEventId.trim().length === 0) {
    return {
      nextState: currentState,
      decision: {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: currentState.stateVersion,
        decidedAt: now,
        message: `Control event ${messageType} requires a mandatory, non-empty logicalEventId`,
      },
    };
  }

  // Create operation details helper
  const createOpDetails = (): LastAppliedOperation =>
    Object.freeze({
      logicalEventId,
      senderPeerId: event.message.senderPeerId,
      messageType,
      appliedAtLocalTimestamp: now,
    });

  switch (messageType) {
    case MessageType.PLAY: {
      const payload = event.message.payload as PlayPayload;
      const targetPos = payload.targetPositionMs;

      // Validate targetPositionMs if provided
      if (targetPos !== undefined) {
        if (typeof targetPos !== 'number' || !Number.isFinite(targetPos) || targetPos < 0) {
          return {
            nextState: currentState,
            decision: {
              accepted: false,
              code: 'REJECTED_INVALID',
              stateVersion: currentState.stateVersion,
              decidedAt: now,
              message: `Invalid targetPositionMs: ${targetPos}`,
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
          anchorLocalTimestamp: now,
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
            decidedAt: now,
          },
        };
      }

      // Current state is 'playing'
      if (targetPos === undefined) {
        // Playing + PLAY() -> Redundant resume is NO_OP
        return {
          nextState: currentState,
          decision: {
            accepted: true,
            code: 'NO_OP',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: 'Already playing with unanchored resume',
          },
        };
      }

      // Playing + PLAY(targetPositionMs) -> check epsilon comparison
      const currentPos = calculateExtrapolatedPosition(currentState, now);
      const delta = Math.abs(targetPos - currentPos);

      if (delta <= config.positionEpsilonMs) {
        // Target matches current extrapolated position within epsilon -> NO_OP
        return {
          nextState: currentState,
          decision: {
            accepted: true,
            code: 'NO_OP',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: `Target position ${targetPos} is within epsilon (${config.positionEpsilonMs}ms) of current position ${currentPos}`,
          },
        };
      }

      // Target differs by more than epsilon -> Re-anchor at target position
      const nextState = Object.freeze<PlaybackState>({
        status: 'playing',
        anchorPositionMs: targetPos,
        anchorLocalTimestamp: now,
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
          decidedAt: now,
        },
      };
    }

    case MessageType.PAUSE: {
      if (currentState.status === 'paused') {
        // Paused + PAUSE() -> Already paused is NO_OP
        return {
          nextState: currentState,
          decision: {
            accepted: true,
            code: 'NO_OP',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: 'Already paused',
          },
        };
      }

      // Playing + PAUSE() -> Freeze at local extrapolated position
      // Note: observedPositionMs is purely diagnostic and is not adopted.
      const frozenPosition = calculateExtrapolatedPosition(currentState, now);
      const nextState = Object.freeze<PlaybackState>({
        status: 'paused',
        anchorPositionMs: frozenPosition,
        anchorLocalTimestamp: now,
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
          decidedAt: now,
        },
      };
    }

    case MessageType.SEEK: {
      const payload = event.message.payload as SeekPayload;
      const targetPos = payload?.targetPositionMs;

      if (typeof targetPos !== 'number' || !Number.isFinite(targetPos) || targetPos < 0) {
        return {
          nextState: currentState,
          decision: {
            accepted: false,
            code: 'REJECTED_INVALID',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: `Invalid seek targetPositionMs: ${targetPos}`,
          },
        };
      }

      // Seek preserves current status (playing stays playing, paused stays paused)
      const nextState = Object.freeze<PlaybackState>({
        status: currentState.status,
        anchorPositionMs: targetPos,
        anchorLocalTimestamp: now,
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
          decidedAt: now,
        },
      };
    }

    case MessageType.RATE_CHANGE: {
      const payload = event.message.payload as RateChangePayload;
      const rate = payload?.playbackRate;

      if (
        typeof rate !== 'number' ||
        !Number.isFinite(rate) ||
        rate <= 0 ||
        rate < config.minPlaybackRate ||
        rate > config.maxPlaybackRate
      ) {
        return {
          nextState: currentState,
          decision: {
            accepted: false,
            code: 'REJECTED_INVALID',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: `Invalid playbackRate: ${rate}. Permitted range: [${config.minPlaybackRate}, ${config.maxPlaybackRate}]`,
          },
        };
      }

      // Invariant: check for unchanged rate BEFORE modifying any anchor fields.
      // NO_OP must leave the complete state unchanged.
      if (rate === currentState.playbackRate) {
        return {
          nextState: currentState,
          decision: {
            accepted: true,
            code: 'NO_OP',
            stateVersion: currentState.stateVersion,
            decidedAt: now,
            message: `Playback rate is already ${rate}`,
          },
        };
      }

      // Rate changed: freeze continuous position continuity if playing
      const anchorPos =
        currentState.status === 'playing'
          ? calculateExtrapolatedPosition(currentState, now)
          : currentState.anchorPositionMs;

      const nextState = Object.freeze<PlaybackState>({
        status: currentState.status,
        anchorPositionMs: anchorPos,
        anchorLocalTimestamp: now,
        playbackRate: rate,
        stateVersion: currentState.stateVersion + 1,
        lastAppliedEvent: createOpDetails(),
      });
      return {
        nextState,
        decision: {
          accepted: true,
          code: 'APPLIED',
          stateVersion: nextState.stateVersion,
          decidedAt: now,
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
          decidedAt: now,
          message: `Unsupported message type: ${messageType}`,
        },
      };
    }
  }
}
