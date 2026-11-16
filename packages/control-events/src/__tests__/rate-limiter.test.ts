import { describe, it, expect } from 'vitest';
import { BoundedRateLimiter } from '../rate-limiter.js';
import type { Clock } from '../types.js';

describe('BoundedRateLimiter', () => {
  it('allows outbound calls within limit and rejects on overflow', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const limiter = new BoundedRateLimiter({
      maxOperationsPerSecond: 3,
      windowMs: 1000,
      clock,
    });

    expect(limiter.checkOutbound()).toBe(true);
    expect(limiter.checkOutbound()).toBe(true);
    expect(limiter.checkOutbound()).toBe(true);

    // 4th call within same window rejected
    expect(limiter.checkOutbound()).toBe(false);

    // Advance clock by 1001ms
    now = 2001;
    expect(limiter.checkOutbound()).toBe(true);
  });

  it('limits inbound calls per remote peer independently', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const limiter = new BoundedRateLimiter({
      maxOperationsPerSecond: 2,
      windowMs: 1000,
      clock,
    });

    expect(limiter.checkInbound('peer-A')).toBe(true);
    expect(limiter.checkInbound('peer-A')).toBe(true);
    expect(limiter.checkInbound('peer-A')).toBe(false); // peer-A blocked

    // peer-B is independent
    expect(limiter.checkInbound('peer-B')).toBe(true);
    expect(limiter.checkInbound('peer-B')).toBe(true);
    expect(limiter.checkInbound('peer-B')).toBe(false); // peer-B blocked
  });

  it('bounds memory by evicting least recently active peer when maxTrackedPeers is exceeded', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const limiter = new BoundedRateLimiter({
      maxOperationsPerSecond: 5,
      maxTrackedPeers: 2,
      clock,
    });

    limiter.checkInbound('peer-1'); // seen at 1000
    now = 1100;
    limiter.checkInbound('peer-2'); // seen at 1100

    expect(limiter.getTrackedPeerCount()).toBe(2);

    // Adding peer-3 evicts peer-1 (oldest seen)
    now = 1200;
    limiter.checkInbound('peer-3');

    expect(limiter.getTrackedPeerCount()).toBe(2);
  });

  it('removes peer immediately on removePeer()', () => {
    const clock: Clock = { now: () => 1000 };
    const limiter = new BoundedRateLimiter({ clock });

    limiter.checkInbound('peer-disconnecting');
    expect(limiter.getTrackedPeerCount()).toBe(1);

    limiter.removePeer('peer-disconnecting');
    expect(limiter.getTrackedPeerCount()).toBe(0);
  });
});

