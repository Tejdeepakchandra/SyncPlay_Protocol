import { describe, it, expect, vi } from 'vitest';
import { ControlsImpl } from '../controls.js';
import type { IntentExecutor } from '../controls.js';
import type { AnyControlSyncEvent, CommandResult } from '../types.js';

describe('SyncPlayControls (ControlsImpl)', () => {
  it('delegates play() to executor', async () => {
    const executor: IntentExecutor = {
      executeIntent: vi.fn().mockResolvedValue({ accepted: true, peerCount: 1, deliveredCount: 1, partialDelivery: false }),
      executeRetry: vi.fn(),
    };
    const controls = new ControlsImpl(executor);

    const res = await controls.play(1500);
    expect(res.accepted).toBe(true);
    expect(executor.executeIntent).toHaveBeenCalledWith({ type: 'PLAY', targetPositionMs: 1500 });
  });

  it('delegates pause() to executor', async () => {
    const executor: IntentExecutor = {
      executeIntent: vi.fn().mockResolvedValue({ accepted: true, peerCount: 1, deliveredCount: 1, partialDelivery: false }),
      executeRetry: vi.fn(),
    };
    const controls = new ControlsImpl(executor);

    const res = await controls.pause(4000);
    expect(res.accepted).toBe(true);
    expect(executor.executeIntent).toHaveBeenCalledWith({ type: 'PAUSE', observedPositionMs: 4000 });
  });

  it('delegates seek() to executor', async () => {
    const executor: IntentExecutor = {
      executeIntent: vi.fn().mockResolvedValue({ accepted: true, peerCount: 1, deliveredCount: 1, partialDelivery: false }),
      executeRetry: vi.fn(),
    };
    const controls = new ControlsImpl(executor);

    const res = await controls.seek(60000);
    expect(res.accepted).toBe(true);
    expect(executor.executeIntent).toHaveBeenCalledWith({ type: 'SEEK', targetPositionMs: 60000 });
  });

  it('delegates setPlaybackRate() to executor', async () => {
    const executor: IntentExecutor = {
      executeIntent: vi.fn().mockResolvedValue({ accepted: true, peerCount: 1, deliveredCount: 1, partialDelivery: false }),
      executeRetry: vi.fn(),
    };
    const controls = new ControlsImpl(executor);

    const res = await controls.setPlaybackRate(1.25);
    expect(res.accepted).toBe(true);
    expect(executor.executeIntent).toHaveBeenCalledWith({ type: 'RATE_CHANGE', playbackRate: 1.25 });
  });

  it('delegates retry() to executor', async () => {
    const executor: IntentExecutor = {
      executeIntent: vi.fn(),
      executeRetry: vi.fn().mockResolvedValue({ accepted: true, peerCount: 1, deliveredCount: 1, partialDelivery: false }),
    };
    const controls = new ControlsImpl(executor);

    const mockEvent = {
      message: {
        protocolVersion: 1,
        messageType: 'SEEK',
        sessionId: 'sess',
        senderPeerId: 'peer1',
        messageId: 'msg1',
        logicalEventId: 'evt1',
        payload: { targetPositionMs: 100 },
      },
      localTimestamp: 100,
      origin: 'local',
    } as AnyControlSyncEvent;

    const res = await controls.retry(mockEvent);
    expect(res.accepted).toBe(true);
    expect(executor.executeRetry).toHaveBeenCalledWith(mockEvent);
  });
});
