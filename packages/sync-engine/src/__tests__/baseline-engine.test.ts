import { describe, it, expect } from 'vitest';
import { MessageType } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { BaselineSyncEngine } from '../baseline-sync-engine.js';
import { SyncEngineError } from '../errors.js';

describe('BaselineSyncEngine — Construction & Config', () => {
  it('initializes with default locally authoritative paused state', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    expect(engine.isDisposed).toBe(false);
    const status = engine.getStatus();
    expect(status.state).toBe('active');
    expect(status.sessionId).toBe('room-1');
    expect(status.localPeerId).toBe('peer-1');
    expect(status.stateVersion).toBe(0);

    const state = engine.getState();
    expect(state.status).toBe('paused');
    expect(state.anchorPositionMs).toBe(0);
    expect(state.anchorLocalTimestamp).toBe(1000);
    expect(state.playbackRate).toBe(1.0);
    expect(state.stateVersion).toBe(0);
  });

  it('rejects invalid construction configuration', () => {
    expect(() => new BaselineSyncEngine({ sessionId: '', localPeerId: 'peer-1' }))
      .toThrow(SyncEngineError);
    expect(() => new BaselineSyncEngine({ sessionId: 'room-1', localPeerId: '' }))
      .toThrow(SyncEngineError);
    expect(() => new BaselineSyncEngine({ sessionId: 'room-1', localPeerId: 'peer-1', minPlaybackRate: -1 }))
      .toThrow(SyncEngineError);
    expect(() => new BaselineSyncEngine({ sessionId: 'room-1', localPeerId: 'peer-1', minPlaybackRate: 2, maxPlaybackRate: 1 }))
      .toThrow(SyncEngineError);
    expect(() => new BaselineSyncEngine({ sessionId: 'room-1', localPeerId: 'peer-1', positionEpsilonMs: -5 }))
      .toThrow(SyncEngineError);
  });

  it('rejects invalid initial clock reading', () => {
    expect(() => new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => -50 },
    })).toThrow(SyncEngineError);

    expect(() => new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => NaN },
    })).toThrow(SyncEngineError);
  });
});

describe('BaselineSyncEngine — Clock Safety & Extrapolated Position', () => {
  it('detects and rejects clock regressions', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    mockTime = 900; // Clock goes backwards!

    const event: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload: {},
      },
      localTimestamp: 900,
      origin: 'local',
    } as AnyControlSyncEvent;

    expect(() => engine.receive(event)).toThrow(SyncEngineError);
    try {
      engine.receive(event);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('CLOCK_ERROR');
    }
  });

  it('extrapolates position and validates explicit timestamps', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    // Start playback
    engine.receive({
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload: { targetPositionMs: 5000 },
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent);

    mockTime = 3000; // 2000ms elapsed at 1.0x -> 7000ms
    expect(engine.getCurrentPositionMs()).toBe(7000);

    // Explicit timestamp
    expect(engine.getCurrentPositionMs(2000)).toBe(6000);

    // Non-finite explicit timestamp throws
    expect(() => engine.getCurrentPositionMs(NaN)).toThrow(SyncEngineError);
    expect(() => engine.getCurrentPositionMs(-10)).toThrow(SyncEngineError);
  });
});

describe('BaselineSyncEngine — Local vs Remote Event Error Handling', () => {
  it('local invalid event throws SyncEngineError to abort broadcast', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    const localInvalidEvent: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.SEEK,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload: { targetPositionMs: -500 }, // Negative seek!
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;

    expect(() => engine.receive(localInvalidEvent)).toThrow(SyncEngineError);
    expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
    expect(engine.getStats().eventsRejectedCount).toBe(1);
    expect(engine.getState().stateVersion).toBe(0); // State not mutated
  });

  it('remote invalid event rejects safely without throwing to preserve transport loop', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    const remoteInvalidEvent: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.SEEK,
        sessionId: 'room-1',
        senderPeerId: 'peer-2',
        messageId: 'msg-2',
        logicalEventId: 'evt-2',
        payload: { targetPositionMs: -500 },
      },
      localTimestamp: 1000,
      origin: 'remote',
    } as AnyControlSyncEvent;

    // Must NOT throw
    expect(() => engine.receive(remoteInvalidEvent)).not.toThrow();
    expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
    expect(engine.getStats().eventsRejectedCount).toBe(1);
    expect(engine.getState().stateVersion).toBe(0);
  });
});

describe('BaselineSyncEngine — Lifecycle & Diagnostics Immutability', () => {
  it('dispose is terminal and idempotent', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    engine.dispose();
    expect(engine.isDisposed).toBe(true);
    expect(engine.getStatus().state).toBe('disposed');

    // Second dispose is no-op
    engine.dispose();
    expect(engine.isDisposed).toBe(true);

    // Local event on disposed engine throws
    const event: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-1',
        logicalEventId: 'evt-1',
        payload: {},
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;

    expect(() => engine.receive(event)).toThrow(SyncEngineError);

    // Remote event on disposed engine does not throw
    const remoteEvent = { ...event, origin: 'remote' } as AnyControlSyncEvent;
    expect(() => engine.receive(remoteEvent)).not.toThrow();
  });

  it('getStats() returns an immutable snapshot', () => {
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
    });

    const stats = engine.getStats();
    expect(Object.isFrozen(stats)).toBe(true);
    expect(() => {
      // @ts-expect-error - readonly runtime check
      stats.eventsReceivedCount = 999;
    }).toThrow();
  });
});
