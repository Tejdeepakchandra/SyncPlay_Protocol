import { describe, it, expect } from 'vitest';
import { createControlEvent, createRetryEvent, generateId } from '../event-factory.js';
import type { ControlEventServiceConfig, Clock } from '../types.js';

describe('Event Factory', () => {
  const clock: Clock = { now: () => 1000.5 };
  const config: ControlEventServiceConfig = {
    sessionId: 'session-123',
    localPeerId: 'peer-alice',
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
  };

  it('generates distinct IDs with expected prefixes', () => {
    const id1 = generateId('msg');
    const id2 = generateId('msg');
    expect(id1).not.toBe(id2);
    expect(id1.startsWith('msg-')).toBe(true);
    expect(id2.startsWith('msg-')).toBe(true);
  });

  describe('createControlEvent', () => {
    it('creates a valid PLAY event with targetPositionMs', () => {
      const res = createControlEvent({ type: 'PLAY', targetPositionMs: 5000 }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;

      expect(res.value.origin).toBe('local');
      expect(res.value.localTimestamp).toBe(1000.5);
      expect(res.value.message.protocolVersion).toBe(1);
      expect(res.value.message.messageType).toBe('PLAY');
      expect(res.value.message.sessionId).toBe('session-123');
      expect(res.value.message.senderPeerId).toBe('peer-alice');
      expect(res.value.message.messageId.startsWith('msg-')).toBe(true);
      expect(res.value.message.logicalEventId?.startsWith('evt-')).toBe(true);
      expect(res.value.message.payload).toEqual({ targetPositionMs: 5000 });
    });

    it('creates a valid PLAY event without targetPositionMs', () => {
      const res = createControlEvent({ type: 'PLAY' }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.value.message.payload).toEqual({});
    });

    it('rejects invalid PLAY with negative position', () => {
      const res = createControlEvent({ type: 'PLAY', targetPositionMs: -1 }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('rejects invalid PLAY with NaN position', () => {
      const res = createControlEvent({ type: 'PLAY', targetPositionMs: NaN }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('creates a valid PAUSE event with observedPositionMs', () => {
      const res = createControlEvent({ type: 'PAUSE', observedPositionMs: 12000 }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.value.message.payload).toEqual({ observedPositionMs: 12000 });
    });

    it('creates a valid PAUSE event without observedPositionMs', () => {
      const res = createControlEvent({ type: 'PAUSE' }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.value.message.payload).toEqual({});
    });

    it('rejects invalid PAUSE with negative position', () => {
      const res = createControlEvent({ type: 'PAUSE', observedPositionMs: -50 }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('creates a valid SEEK event', () => {
      const res = createControlEvent({ type: 'SEEK', targetPositionMs: 45000 }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.value.message.messageType).toBe('SEEK');
      expect(res.value.message.payload).toEqual({ targetPositionMs: 45000 });
    });

    it('rejects invalid SEEK with negative position', () => {
      const res = createControlEvent({ type: 'SEEK', targetPositionMs: -100 }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('rejects invalid SEEK with Infinity position', () => {
      const res = createControlEvent({ type: 'SEEK', targetPositionMs: Infinity }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('creates a valid RATE_CHANGE event within configured range', () => {
      const res = createControlEvent({ type: 'RATE_CHANGE', playbackRate: 1.5 }, config, clock);
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.value.message.payload).toEqual({ playbackRate: 1.5 });
    });

    it('rejects RATE_CHANGE below minPlaybackRate', () => {
      const res = createControlEvent({ type: 'RATE_CHANGE', playbackRate: 0.2 }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
      expect(res.error.message).toContain('outside permitted range');
    });

    it('rejects RATE_CHANGE above maxPlaybackRate', () => {
      const res = createControlEvent({ type: 'RATE_CHANGE', playbackRate: 3.0 }, config, clock);
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.error.code).toBe('INVALID_ARGUMENT');
    });

    it('rejects RATE_CHANGE with negative or zero rate', () => {
      const res1 = createControlEvent({ type: 'RATE_CHANGE', playbackRate: 0 }, config, clock);
      expect(res1.ok).toBe(false);

      const res2 = createControlEvent({ type: 'RATE_CHANGE', playbackRate: -1.0 }, config, clock);
      expect(res2.ok).toBe(false);
    });
  });

  describe('createRetryEvent', () => {
    it('creates a new messageId while preserving logicalEventId and payload', () => {
      const initialRes = createControlEvent({ type: 'SEEK', targetPositionMs: 30000 }, config, clock);
      expect(initialRes.ok).toBe(true);
      if (!initialRes.ok) return;

      const initialEvent = initialRes.value;
      const laterClock: Clock = { now: () => 2000.0 };

      const retryEvent = createRetryEvent(initialEvent, laterClock);

      // Unique messageId for the new physical transmission attempt
      expect(retryEvent.message.messageId).not.toBe(initialEvent.message.messageId);
      expect(retryEvent.message.messageId.startsWith('msg-')).toBe(true);

      // Stable logicalEventId preserved for deduplication
      expect(retryEvent.message.logicalEventId).toBe(initialEvent.message.logicalEventId);

      // Metadata and payload preserved
      expect(retryEvent.message.protocolVersion).toBe(initialEvent.message.protocolVersion);
      expect(retryEvent.message.messageType).toBe(initialEvent.message.messageType);
      expect(retryEvent.message.sessionId).toBe(initialEvent.message.sessionId);
      expect(retryEvent.message.senderPeerId).toBe(initialEvent.message.senderPeerId);
      expect(retryEvent.message.payload).toEqual(initialEvent.message.payload);

      // Local timestamp updated
      expect(retryEvent.localTimestamp).toBe(2000.0);
      expect(retryEvent.origin).toBe('local');
    });
  });
});
