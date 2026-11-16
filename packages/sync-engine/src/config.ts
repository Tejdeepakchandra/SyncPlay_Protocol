/**
 * Configuration and validation for SyncEngine.
 */

import type { SyncEngineConfig } from './types.js';
import { DEFAULT_SYNC_ENGINE_CONFIG } from './types.js';
import { SyncEngineError } from './errors.js';

export interface ResolvedSyncEngineConfig {
  readonly sessionId: string;
  readonly localPeerId: string;
  readonly minPlaybackRate: number;
  readonly maxPlaybackRate: number;
  readonly positionEpsilonMs: number;
}

export function validateSyncEngineConfig(config: SyncEngineConfig): ResolvedSyncEngineConfig {
  if (!config.sessionId || typeof config.sessionId !== 'string' || config.sessionId.trim().length === 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'sessionId must be a non-empty string');
  }
  if (config.sessionId.length > 128) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'sessionId exceeds maximum length of 128 characters');
  }

  if (!config.localPeerId || typeof config.localPeerId !== 'string' || config.localPeerId.trim().length === 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'localPeerId must be a non-empty string');
  }
  if (config.localPeerId.length > 128) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'localPeerId exceeds maximum length of 128 characters');
  }

  const minPlaybackRate = config.minPlaybackRate ?? DEFAULT_SYNC_ENGINE_CONFIG.minPlaybackRate;
  if (typeof minPlaybackRate !== 'number' || !Number.isFinite(minPlaybackRate) || minPlaybackRate <= 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', `minPlaybackRate must be a positive finite number, got ${minPlaybackRate}`);
  }

  const maxPlaybackRate = config.maxPlaybackRate ?? DEFAULT_SYNC_ENGINE_CONFIG.maxPlaybackRate;
  if (typeof maxPlaybackRate !== 'number' || !Number.isFinite(maxPlaybackRate) || maxPlaybackRate < minPlaybackRate) {
    throw new SyncEngineError(
      'INVALID_ARGUMENT',
      `maxPlaybackRate must be >= minPlaybackRate (${minPlaybackRate}), got ${maxPlaybackRate}`
    );
  }

  const positionEpsilonMs = config.positionEpsilonMs ?? DEFAULT_SYNC_ENGINE_CONFIG.positionEpsilonMs;
  if (typeof positionEpsilonMs !== 'number' || !Number.isFinite(positionEpsilonMs) || positionEpsilonMs < 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', `positionEpsilonMs must be a non-negative finite number, got ${positionEpsilonMs}`);
  }

  return Object.freeze({
    sessionId: config.sessionId,
    localPeerId: config.localPeerId,
    minPlaybackRate,
    maxPlaybackRate,
    positionEpsilonMs,
  });
}
