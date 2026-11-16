/**
 * Unit and lifecycle tests for RttEngine.
 *
 * Per PEER_MESH_RTT_ENGINE_LLD §7–9 and Phase 4 requirements.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { RttEngine } from '../rtt/rtt-engine.js';
import type { Clock, RttSample } from '../rtt/types.js';
import { MessageType, type ProtocolMessage } from '@syncplay/protocol';

class MockClock implements Clock {
  private currentTime = 1000;

  now(): number {
    return this.currentTime;
  }

  advance(ms: number): void {
    this.currentTime += ms;
  }

  setTime(time: number): void {
    this.currentTime = time;
  }
}

describe('RttEngine', () => {
  let clock: MockClock;
  let sentMessages: Array<{ peerId: string; message: ProtocolMessage }>;
  let engine: RttEngine;

  beforeEach(() => {
    vi.useFakeTimers();
    clock = new MockClock();
    sentMessages = [];

    engine = new RttEngine(
      'local-peer',
      'session-1',
      (peerId, message) => {
        sentMessages.push({ peerId, message });
        return { ok: true };
      },
      {
        probeIntervalMs: 2000,
        probeTimeoutMs: 3000,
        maxHistorySamples: 10,
        staleThresholdMs: 6000,
        maxPendingProbes: 1,
      },
      clock,
    );
  });

  afterEach(() => {
    engine.dispose();
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it('dispatches PING immediately on start() and records monotonic sentAt', () => {
    engine.start('peer-1', 1);

    expect(sentMessages).toHaveLength(1);
    const sent = sentMessages[0]!;
    expect(sent.peerId).toBe('peer-1');
    expect(sent.message.messageType).toBe(MessageType.PING);
    expect(sent.message.sessionId).toBe('session-1');
    expect(sent.message.senderPeerId).toBe('local-peer');

    const payload = sent.message.payload as { probeId: string; senderTimestamp: number };
    expect(payload.probeId).toMatch(/^probe-local-peer-/);
    expect(payload.senderTimestamp).toBe(1000);
  });

  it('measures RTT and emits sample when matching PONG is received', () => {
    const samples: RttSample[] = [];
    engine.onSample((s) => samples.push(s));

    engine.start('peer-1', 1);
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    // Advance clock by 45ms and simulate PONG reception
    clock.advance(45);
    engine.handlePong('peer-1', { probeId });

    expect(samples).toHaveLength(1);
    expect(samples[0]!.peerId).toBe('peer-1');
    expect(samples[0]!.probeId).toBe(probeId);
    expect(samples[0]!.generation).toBe(1);
    expect(samples[0]!.rttMs).toBe(45);
    expect(samples[0]!.timestamp).toBe(1045);

    const est = engine.getEstimate('peer-1');
    expect(est).toBeDefined();
    expect(est?.sampleCount).toBe(1);
    expect(est?.latestRttMs).toBe(45);
    expect(est?.isStale).toBe(false);
  });

  it('silently ignores duplicate PONG for same probeId', () => {
    const samples: RttSample[] = [];
    engine.onSample((s) => samples.push(s));

    engine.start('peer-1', 1);
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    clock.advance(30);
    engine.handlePong('peer-1', { probeId });
    expect(samples).toHaveLength(1);

    // Duplicate PONG arrives
    clock.advance(10);
    engine.handlePong('peer-1', { probeId });
    expect(samples).toHaveLength(1); // Still 1

    const est = engine.getEstimate('peer-1');
    expect(est?.sampleCount).toBe(1);
  });

  it('silently ignores unexpected PONG with unknown probeId or unknown peer', () => {
    const samples: RttSample[] = [];
    engine.onSample((s) => samples.push(s));

    engine.start('peer-1', 1);

    // PONG for unknown probe
    engine.handlePong('peer-1', { probeId: 'probe-unknown' });
    // PONG for unknown peer
    engine.handlePong('peer-99', { probeId: 'probe-unknown' });

    expect(samples).toHaveLength(0);
    expect(engine.getEstimate('peer-1')).toBeUndefined();
  });

  it('handles probe timeout without recording 0ms or corrupted samples', () => {
    engine.start('peer-1', 1);
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    // Advance clock past timeout (3000ms)
    clock.advance(3001);
    vi.advanceTimersByTime(3001);

    // No sample was created (must NOT record zero)
    expect(engine.getEstimate('peer-1')).toBeUndefined();

    // If delayed PONG arrives after timeout, it is safely discarded
    engine.handlePong('peer-1', { probeId });
    expect(engine.getEstimate('peer-1')).toBeUndefined();
  });

  it('rejects negative or invalid RTT measurements without clamping', () => {
    const samples: RttSample[] = [];
    engine.onSample((s) => samples.push(s));

    engine.start('peer-1', 1);
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    // Simulate clock going backwards (anomaly: -50ms elapsed)
    clock.setTime(950);
    engine.handlePong('peer-1', { probeId });

    // Rejected, not clamped to 0
    expect(samples).toHaveLength(0);
    expect(engine.getEstimate('peer-1')).toBeUndefined();
  });

  it('enforces maxPendingProbes backpressure and skips interval when probe in-flight', () => {
    engine.start('peer-1', 1);
    expect(sentMessages).toHaveLength(1);

    // Interval fires (2000ms), but probe-1 is still pending (maxPendingProbes = 1)
    clock.advance(2000);
    vi.advanceTimersByTime(2000);

    // No second probe should be sent yet
    expect(sentMessages).toHaveLength(1);

    // Resolve first probe
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;
    engine.handlePong('peer-1', { probeId });

    // Next interval fires -> now next probe can be sent
    clock.advance(2000);
    vi.advanceTimersByTime(2000);
    expect(sentMessages).toHaveLength(2);
  });

  it('invalidates pending probes and timers when peer reconnects with new generation', () => {
    engine.start('peer-1', 1);
    const gen1ProbeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    // Remote peer reconnected with generation 2
    clock.advance(500);
    engine.start('peer-1', 2);

    expect(sentMessages).toHaveLength(2);
    const gen2ProbeId = (sentMessages[1]!.message.payload as { probeId: string }).probeId;
    expect(gen2ProbeId).not.toBe(gen1ProbeId);

    // Stale generation 1 PONG arrives late
    clock.advance(50);
    engine.handlePong('peer-1', { probeId: gen1ProbeId });

    // Gen-1 PONG must NOT produce a sample
    expect(engine.getEstimate('peer-1')).toBeUndefined();

    // Gen-2 PONG arrives
    clock.advance(20);
    engine.handlePong('peer-1', { probeId: gen2ProbeId });

    const est = engine.getEstimate('peer-1');
    expect(est).toBeDefined();
    expect(est?.generation).toBe(2);
    expect(est?.sampleCount).toBe(1);
    expect(est?.latestRttMs).toBe(70);
  });

  it('cleans up all scheduled and timeout timers on stop()', () => {
    engine.start('peer-1', 1);
    expect(sentMessages).toHaveLength(1);

    engine.stop('peer-1');

    // Advancing timers should not cause any timeouts or dispatches
    clock.advance(10000);
    vi.advanceTimersByTime(10000);

    // Still only the initial message
    expect(sentMessages).toHaveLength(1);
  });

  it('cleans up completely on dispose()', () => {
    engine.start('peer-1', 1);
    engine.start('peer-2', 1);

    engine.dispose();

    clock.advance(10000);
    vi.advanceTimersByTime(10000);

    expect(engine.getEstimate('peer-1')).toBeUndefined();
    expect(engine.getEstimate('peer-2')).toBeUndefined();
  });

  it('unsubscribes handler properly', () => {
    const samples: RttSample[] = [];
    const unsub = engine.onSample((s) => samples.push(s));

    engine.start('peer-1', 1);
    const probeId = (sentMessages[0]!.message.payload as { probeId: string }).probeId;

    unsub();

    clock.advance(20);
    engine.handlePong('peer-1', { probeId });

    expect(samples).toHaveLength(0);
    expect(engine.getEstimate('peer-1')).toBeDefined();
  });
});
