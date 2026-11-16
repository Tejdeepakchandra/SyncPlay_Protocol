import { describe, it, expect } from 'vitest';
import { MessageType } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { calculateExtrapolatedPosition, reduceState } from '../reducer.js';
import type { PlaybackState } from '../types.js';
import { validateSyncEngineConfig } from '../config.js';

describe('calculateExtrapolatedPosition', () => {
  const config = validateSyncEngineConfig({
    sessionId: 'test-session',
    localPeerId: 'peer-1',
  });

  it('returns anchorPositionMs when status is paused', () => {
    const state: PlaybackState = {
      status: 'paused',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    };

    expect(calculateExtrapolatedPosition(state, 1000)).toBe(5000);
    expect(calculateExtrapolatedPosition(state, 5000)).toBe(5000);
  });

  it('extrapolates position continuously when status is playing', () => {
    const state: PlaybackState = {
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.5,
      stateVersion: 1,
    };

    // Elapsed: 2000ms at 1.5x = +3000ms -> 4000ms
    expect(calculateExtrapolatedPosition(state, 3000)).toBe(4000);
  });

  it('clamps elapsed time to 0 if atTimestamp is earlier than anchorLocalTimestamp', () => {
    const state: PlaybackState = {
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 2000,
      playbackRate: 1.0,
      stateVersion: 1,
    };

    // atTimestamp 1000 is earlier than anchor 2000 -> clamped to anchorPositionMs
    expect(calculateExtrapolatedPosition(state, 1000)).toBe(1000);
  });

  it('prevents overflow from returning Infinity', () => {
    const state: PlaybackState = {
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 0,
      playbackRate: 1.0,
      stateVersion: 1,
    };

    const extremeTimestamp = Number.MAX_VALUE;
    const pos = calculateExtrapolatedPosition(state, extremeTimestamp);
    expect(Number.isFinite(pos)).toBe(true);
    expect(pos).toBe(Number.MAX_SAFE_INTEGER);
  });
});

describe('reduceState — PLAY semantics', () => {
  const config = validateSyncEngineConfig({
    sessionId: 'session-1',
    localPeerId: 'peer-1',
    positionEpsilonMs: 5.0,
  });

  function makeEvent(type: string, payload: unknown, origin: 'local' | 'remote' = 'local'): AnyControlSyncEvent {
    return {
      message: {
        protocolVersion: 1,
        messageType: type,
        sessionId: 'session-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload,
      },
      localTimestamp: 1000,
      origin,
    } as AnyControlSyncEvent;
  }

  const pausedState: PlaybackState = Object.freeze({
    status: 'paused',
    anchorPositionMs: 5000,
    anchorLocalTimestamp: 1000,
    playbackRate: 1.0,
    stateVersion: 0,
  });

  it('Paused at 5000ms + PLAY() -> resumes from 5000ms', () => {
    const event = makeEvent(MessageType.PLAY, {});
    const { nextState, decision } = reduceState(pausedState, event, 1500, config);

    expect(decision.code).toBe('APPLIED');
    expect(decision.accepted).toBe(true);
    expect(nextState.status).toBe('playing');
    expect(nextState.anchorPositionMs).toBe(5000);
    expect(nextState.anchorLocalTimestamp).toBe(1500);
    expect(nextState.stateVersion).toBe(1);
    expect(nextState.lastAppliedEvent?.logicalEventId).toBe('evt-1');
  });

  it('Paused at 5000ms + PLAY(0) -> starts from 0ms', () => {
    const event = makeEvent(MessageType.PLAY, { targetPositionMs: 0 });
    const { nextState, decision } = reduceState(pausedState, event, 1500, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('playing');
    expect(nextState.anchorPositionMs).toBe(0);
    expect(nextState.anchorLocalTimestamp).toBe(1500);
    expect(nextState.stateVersion).toBe(1);
  });

  it('Playing + PLAY() -> NO_OP, state unchanged', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
      lastAppliedEvent: {
        logicalEventId: 'evt-orig',
        senderPeerId: 'peer-1',
        messageType: 'PLAY',
        appliedAtLocalTimestamp: 1000,
      },
    });

    const event = makeEvent(MessageType.PLAY, {});
    const { nextState, decision } = reduceState(playingState, event, 2000, config);

    expect(decision.code).toBe('NO_OP');
    expect(nextState).toBe(playingState); // Exact reference unchanged
    expect(nextState.stateVersion).toBe(1);
    expect(nextState.lastAppliedEvent?.logicalEventId).toBe('evt-orig');
  });

  it('Playing + PLAY(20000) when delta > epsilon -> re-anchors to 20000ms', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    // At now=2000, current extrapolated pos is 5000 + 1000 = 6000ms.
    // Target is 20000ms (delta = 14000ms > 5ms)
    const event = makeEvent(MessageType.PLAY, { targetPositionMs: 20000 });
    const { nextState, decision } = reduceState(playingState, event, 2000, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('playing');
    expect(nextState.anchorPositionMs).toBe(20000);
    expect(nextState.anchorLocalTimestamp).toBe(2000);
    expect(nextState.stateVersion).toBe(2);
  });

  it('Playing + PLAY(target) when delta <= epsilon -> NO_OP', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 5000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    // At now=2000, current extrapolated pos is 6000ms.
    // Target is 6002ms (delta = 2ms <= 5ms epsilon)
    const event = makeEvent(MessageType.PLAY, { targetPositionMs: 6002 });
    const { nextState, decision } = reduceState(playingState, event, 2000, config);

    expect(decision.code).toBe('NO_OP');
    expect(nextState).toBe(playingState);
    expect(nextState.stateVersion).toBe(1);
  });

  it('Rejects negative targetPositionMs', () => {
    const event = makeEvent(MessageType.PLAY, { targetPositionMs: -100 });
    const { nextState, decision } = reduceState(pausedState, event, 1500, config);

    expect(decision.code).toBe('REJECTED_INVALID');
    expect(decision.accepted).toBe(false);
    expect(nextState).toBe(pausedState);
  });
});

describe('reduceState — PAUSE semantics', () => {
  const config = validateSyncEngineConfig({
    sessionId: 'session-1',
    localPeerId: 'peer-1',
  });

  function makeEvent(payload: unknown): AnyControlSyncEvent {
    return {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PAUSE,
        sessionId: 'session-1',
        senderPeerId: 'peer-2',
        messageId: 'msg-p',
        logicalEventId: 'evt-p',
        payload,
      },
      localTimestamp: 2000,
      origin: 'remote',
    } as AnyControlSyncEvent;
  }

  it('Playing + PAUSE() -> freezes position at local extrapolated time', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    // At now=3000, elapsed = 2000ms -> frozen pos = 3000ms
    const event = makeEvent({ observedPositionMs: 99999 }); // Remote claimed 99999
    const { nextState, decision } = reduceState(playingState, event, 3000, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('paused');
    expect(nextState.anchorPositionMs).toBe(3000); // Local extrapolated, NOT 99999
    expect(nextState.anchorLocalTimestamp).toBe(3000);
    expect(nextState.stateVersion).toBe(2);
    expect(nextState.lastAppliedEvent?.senderPeerId).toBe('peer-2');
  });

  it('Paused + PAUSE() -> NO_OP, state unchanged', () => {
    const pausedState: PlaybackState = Object.freeze({
      status: 'paused',
      anchorPositionMs: 3000,
      anchorLocalTimestamp: 3000,
      playbackRate: 1.0,
      stateVersion: 2,
    });

    const event = makeEvent({});
    const { nextState, decision } = reduceState(pausedState, event, 4000, config);

    expect(decision.code).toBe('NO_OP');
    expect(nextState).toBe(pausedState);
    expect(nextState.stateVersion).toBe(2);
  });
});

describe('reduceState — SEEK semantics', () => {
  const config = validateSyncEngineConfig({
    sessionId: 'session-1',
    localPeerId: 'peer-1',
  });

  function makeEvent(targetPositionMs: unknown): AnyControlSyncEvent {
    return {
      message: {
        protocolVersion: 1,
        messageType: MessageType.SEEK,
        sessionId: 'session-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-s',
        logicalEventId: 'evt-s',
        payload: { targetPositionMs },
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;
  }

  it('Seek while paused -> updates anchorPositionMs and preserves paused status', () => {
    const pausedState: PlaybackState = Object.freeze({
      status: 'paused',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    const event = makeEvent(45000);
    const { nextState, decision } = reduceState(pausedState, event, 1500, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('paused');
    expect(nextState.anchorPositionMs).toBe(45000);
    expect(nextState.anchorLocalTimestamp).toBe(1500);
    expect(nextState.stateVersion).toBe(2);
  });

  it('Seek while playing -> updates anchorPositionMs and preserves playing status', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    const event = makeEvent(45000);
    const { nextState, decision } = reduceState(playingState, event, 1500, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('playing');
    expect(nextState.anchorPositionMs).toBe(45000);
    expect(nextState.anchorLocalTimestamp).toBe(1500);
    expect(nextState.stateVersion).toBe(2);
  });

  it('Rejects negative or invalid seek position', () => {
    const pausedState: PlaybackState = Object.freeze({
      status: 'paused',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    const event = makeEvent(-5);
    const { nextState, decision } = reduceState(pausedState, event, 1500, config);

    expect(decision.code).toBe('REJECTED_INVALID');
    expect(nextState).toBe(pausedState);
  });
});

describe('reduceState — RATE_CHANGE semantics', () => {
  const config = validateSyncEngineConfig({
    sessionId: 'session-1',
    localPeerId: 'peer-1',
    minPlaybackRate: 0.25,
    maxPlaybackRate: 4.0,
  });

  function makeEvent(playbackRate: unknown): AnyControlSyncEvent {
    return {
      message: {
        protocolVersion: 1,
        messageType: MessageType.RATE_CHANGE,
        sessionId: 'session-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-r',
        logicalEventId: 'evt-r',
        payload: { playbackRate },
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;
  }

  it('Rate change while playing -> freezes position at current elapsed time before applying rate', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    // At now=3000, elapsed = 2000ms at 1.0x -> pos is 3000ms
    const event = makeEvent(2.0);
    const { nextState, decision } = reduceState(playingState, event, 3000, config);

    expect(decision.code).toBe('APPLIED');
    expect(nextState.status).toBe('playing');
    expect(nextState.anchorPositionMs).toBe(3000); // Frozen before rate change
    expect(nextState.playbackRate).toBe(2.0);
    expect(nextState.anchorLocalTimestamp).toBe(3000);
    expect(nextState.stateVersion).toBe(2);
  });

  it('Rate unchanged -> NO_OP and complete state unchanged', () => {
    const playingState: PlaybackState = Object.freeze({
      status: 'playing',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.5,
      stateVersion: 1,
    });

    const event = makeEvent(1.5);
    const { nextState, decision } = reduceState(playingState, event, 3000, config);

    expect(decision.code).toBe('NO_OP');
    expect(nextState).toBe(playingState); // Complete reference unchanged
    expect(nextState.anchorPositionMs).toBe(1000); // Anchor not modified
    expect(nextState.anchorLocalTimestamp).toBe(1000); // Anchor timestamp not modified
  });

  it('Rejects out-of-bounds playback rate', () => {
    const pausedState: PlaybackState = Object.freeze({
      status: 'paused',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    expect(reduceState(pausedState, makeEvent(0.1), 1500, config).decision.code).toBe('REJECTED_INVALID');
    expect(reduceState(pausedState, makeEvent(5.0), 1500, config).decision.code).toBe('REJECTED_INVALID');
    expect(reduceState(pausedState, makeEvent(-1.0), 1500, config).decision.code).toBe('REJECTED_INVALID');
  });

  it('Rejects control events missing logicalEventId without silent fallback to messageId', () => {
    const pausedState: PlaybackState = Object.freeze({
      status: 'paused',
      anchorPositionMs: 1000,
      anchorLocalTimestamp: 1000,
      playbackRate: 1.0,
      stateVersion: 1,
    });

    const eventWithoutLogicalId = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'session-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-fallback-attempt',
        // logicalEventId is missing!
        payload: {},
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;

    const result = reduceState(pausedState, eventWithoutLogicalId, 1500, config);
    expect(result.decision.code).toBe('REJECTED_INVALID');
    expect(result.decision.accepted).toBe(false);
    expect(result.decision.message).toContain('requires a mandatory, non-empty logicalEventId');
    expect(result.nextState).toBe(pausedState);
  });
});
