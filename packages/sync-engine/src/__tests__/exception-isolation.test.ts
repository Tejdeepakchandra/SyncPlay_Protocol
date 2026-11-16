import { describe, it, expect, vi } from 'vitest';
import { MessageType } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { BaselineSyncEngine } from '../baseline-sync-engine.js';

describe('BaselineSyncEngine — Listener Exception Isolation', () => {
  it('isolates onStateChange listener exceptions and preserves committed state', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    const goodListener1 = vi.fn();
    const crashingListener = vi.fn(() => {
      throw new Error('Fatal UI render failure');
    });
    const goodListener2 = vi.fn();

    engine.onStateChange(goodListener1);
    engine.onStateChange(crashingListener);
    engine.onStateChange(goodListener2);

    const playEvent: AnyControlSyncEvent = {
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
    } as AnyControlSyncEvent;

    // Invariant: receive() must NOT throw even though crashingListener threw!
    expect(() => engine.receive(playEvent)).not.toThrow();

    // Invariant: Committed state must NOT be rolled back
    const state = engine.getState();
    expect(state.status).toBe('playing');
    expect(state.anchorPositionMs).toBe(5000);
    expect(state.stateVersion).toBe(1);

    // Invariant: All other listeners must be called
    expect(goodListener1).toHaveBeenCalledTimes(1);
    expect(crashingListener).toHaveBeenCalledTimes(1);
    expect(goodListener2).toHaveBeenCalledTimes(1);

    // Invariant: Exception tracked in stats
    const stats = engine.getStats();
    expect(stats.listenerErrorCount).toBe(1);
    expect(stats.stateChangeNotificationsCount).toBe(2);
    expect(stats.eventsAppliedCount).toBe(1);
  });

  it('isolates onDecision listener exceptions', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    const crashingDecisionListener = vi.fn(() => {
      throw new Error('Decision logging crashed');
    });
    const goodDecisionListener = vi.fn();

    engine.onDecision(crashingDecisionListener);
    engine.onDecision(goodDecisionListener);

    const event: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PAUSE,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-2',
        logicalEventId: 'evt-2',
        payload: {},
      },
      localTimestamp: 1000,
      origin: 'local',
    } as AnyControlSyncEvent;

    expect(() => engine.receive(event)).not.toThrow();
    expect(crashingDecisionListener).toHaveBeenCalledTimes(1);
    expect(goodDecisionListener).toHaveBeenCalledTimes(1);
    expect(engine.getStats().listenerErrorCount).toBe(1);
  });

  it('safely handles unsubscription during notification iteration', () => {
    let mockTime = 1000;
    const engine = new BaselineSyncEngine({
      sessionId: 'room-1',
      localPeerId: 'peer-1',
      clock: { now: () => mockTime },
    });

    let unsubSelf: (() => void) | undefined;
    const selfUnsubListener = vi.fn(() => {
      unsubSelf?.();
    });
    const normalListener = vi.fn();

    unsubSelf = engine.onStateChange(selfUnsubListener);
    engine.onStateChange(normalListener);

    // First event: both run, selfUnsubListener unregisters itself
    engine.receive({
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
    } as AnyControlSyncEvent);

    expect(selfUnsubListener).toHaveBeenCalledTimes(1);
    expect(normalListener).toHaveBeenCalledTimes(1);

    // Second event: only normalListener runs
    mockTime = 2000;
    engine.receive({
      message: {
        protocolVersion: 1,
        messageType: MessageType.PAUSE,
        sessionId: 'room-1',
        senderPeerId: 'peer-1',
        messageId: 'msg-2',
        logicalEventId: 'evt-2',
        payload: {},
      },
      localTimestamp: 2000,
      origin: 'local',
    } as AnyControlSyncEvent);

    expect(selfUnsubListener).toHaveBeenCalledTimes(1);
    expect(normalListener).toHaveBeenCalledTimes(2);
  });
});
