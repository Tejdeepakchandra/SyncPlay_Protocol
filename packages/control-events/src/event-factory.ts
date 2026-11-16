/**
 * Event Factory.
 *
 * Converts high-level user intents into validated, versioned ProtocolMessages
 * and in-memory SyncEvents. Enforces semantic range constraints and identity rules.
 */

import { PROTOCOL_VERSION } from '@syncplay/protocol';
import type { Result } from '@syncplay/protocol';
import type {
  ControlIntent,
  AnyControlSyncEvent,
  ControlEventServiceConfig,
  Clock,
  PlaySyncEvent,
  PauseSyncEvent,
  SeekSyncEvent,
  RateChangeSyncEvent,
} from './types.js';
import { ControlError } from './errors.js';

let counter = 0;

/**
 * Generate a cryptographically strong or collision-resistant identifier.
 */
export function generateId(prefix: string): string {
  counter++;
  if (typeof globalThis.crypto?.randomUUID === 'function') {
    return `${prefix}-${globalThis.crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${counter}-${Math.random().toString(36).slice(2, 9)}`;
}

/**
 * Create a new local control event from a user intent.
 *
 * Validates semantic constraints, allocates a unique messageId and
 * a stable logicalEventId, and packages the result as a SyncEvent.
 */
export function createControlEvent(
  intent: ControlIntent,
  config: ControlEventServiceConfig,
  clock: Clock,
): Result<AnyControlSyncEvent, ControlError> {
  const minRate = config.minPlaybackRate ?? 0.25;
  const maxRate = config.maxPlaybackRate ?? 4.0;
  const now = clock.now();
  const messageId = generateId('msg');
  const logicalEventId = generateId('evt');

  switch (intent.type) {
    case 'PLAY': {
      if (
        intent.targetPositionMs !== undefined &&
        (!Number.isFinite(intent.targetPositionMs) || intent.targetPositionMs < 0)
      ) {
        return {
          ok: false,
          error: new ControlError(
            'INVALID_ARGUMENT',
            `Play targetPositionMs must be a finite, non-negative number, got ${intent.targetPositionMs}`,
          ),
        };
      }

      const event: PlaySyncEvent = {
        message: {
          protocolVersion: PROTOCOL_VERSION,
          messageType: 'PLAY',
          sessionId: config.sessionId,
          senderPeerId: config.localPeerId,
          messageId,
          logicalEventId,
          payload:
            intent.targetPositionMs !== undefined
              ? { targetPositionMs: intent.targetPositionMs }
              : {},
        },
        localTimestamp: now,
        origin: 'local',
      };
      return { ok: true, value: event };
    }

    case 'PAUSE': {
      if (
        intent.observedPositionMs !== undefined &&
        (!Number.isFinite(intent.observedPositionMs) || intent.observedPositionMs < 0)
      ) {
        return {
          ok: false,
          error: new ControlError(
            'INVALID_ARGUMENT',
            `Pause observedPositionMs must be a finite, non-negative number, got ${intent.observedPositionMs}`,
          ),
        };
      }

      const event: PauseSyncEvent = {
        message: {
          protocolVersion: PROTOCOL_VERSION,
          messageType: 'PAUSE',
          sessionId: config.sessionId,
          senderPeerId: config.localPeerId,
          messageId,
          logicalEventId,
          payload:
            intent.observedPositionMs !== undefined
              ? { observedPositionMs: intent.observedPositionMs }
              : {},
        },
        localTimestamp: now,
        origin: 'local',
      };
      return { ok: true, value: event };
    }

    case 'SEEK': {
      if (
        !Number.isFinite(intent.targetPositionMs) ||
        intent.targetPositionMs < 0
      ) {
        return {
          ok: false,
          error: new ControlError(
            'INVALID_ARGUMENT',
            `Seek targetPositionMs must be a finite, non-negative number, got ${intent.targetPositionMs}`,
          ),
        };
      }

      const event: SeekSyncEvent = {
        message: {
          protocolVersion: PROTOCOL_VERSION,
          messageType: 'SEEK',
          sessionId: config.sessionId,
          senderPeerId: config.localPeerId,
          messageId,
          logicalEventId,
          payload: { targetPositionMs: intent.targetPositionMs },
        },
        localTimestamp: now,
        origin: 'local',
      };
      return { ok: true, value: event };
    }

    case 'RATE_CHANGE': {
      if (
        !Number.isFinite(intent.playbackRate) ||
        intent.playbackRate <= 0
      ) {
        return {
          ok: false,
          error: new ControlError(
            'INVALID_ARGUMENT',
            `Playback rate must be a finite, positive number, got ${intent.playbackRate}`,
          ),
        };
      }

      if (
        intent.playbackRate < minRate ||
        intent.playbackRate > maxRate
      ) {
        return {
          ok: false,
          error: new ControlError(
            'INVALID_ARGUMENT',
            `Playback rate ${intent.playbackRate} is outside permitted range [${minRate}, ${maxRate}]`,
          ),
        };
      }

      const event: RateChangeSyncEvent = {
        message: {
          protocolVersion: PROTOCOL_VERSION,
          messageType: 'RATE_CHANGE',
          sessionId: config.sessionId,
          senderPeerId: config.localPeerId,
          messageId,
          logicalEventId,
          payload: { playbackRate: intent.playbackRate },
        },
        localTimestamp: now,
        origin: 'local',
      };
      return { ok: true, value: event };
    }

    default: {
      const exhaustive: never = intent;
      return {
        ok: false,
        error: new ControlError(
          'UNSUPPORTED_TYPE',
          `Unknown intent type: ${(exhaustive as { type: string }).type}`,
        ),
      };
    }
  }
}

/**
 * Create a retry event from an existing control event.
 *
 * Core Invariant (Decision D8):
 * - Allocates a brand new messageId for the new transmission attempt.
 * - Preserves the original logicalEventId so receivers can deduplicate.
 */
export function createRetryEvent(
  event: AnyControlSyncEvent,
  clock: Clock,
): AnyControlSyncEvent {
  const retryMessageId = generateId('msg');

  return {
    ...event,
    message: {
      ...event.message,
      messageId: retryMessageId,
      // logicalEventId is strictly preserved
    },
    localTimestamp: clock.now(),
    origin: 'local',
  } as AnyControlSyncEvent;
}
