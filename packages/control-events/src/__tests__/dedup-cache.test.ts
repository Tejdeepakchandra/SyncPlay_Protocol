import { describe, it, expect } from 'vitest';
import { DeduplicationCache } from '../dedup-cache.js';
import type { Clock } from '../types.js';

describe('DeduplicationCache', () => {
  it('identifies fresh events and suppresses duplicates', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const cache = new DeduplicationCache({ clock, ttlMs: 5000, maxEntries: 10 });

    // First time: fresh
    expect(cache.checkAndRecord('peer-1', 'evt-100')).toBe(false);

    // Second time: duplicate
    expect(cache.checkAndRecord('peer-1', 'evt-100')).toBe(true);

    const stats = cache.getStats();
    expect(stats.size).toBe(1);
    expect(stats.duplicateCount).toBe(1);
  });

  it('uses composite key so different peers with same logicalEventId do not collide', () => {
    const clock: Clock = { now: () => 1000 };
    const cache = new DeduplicationCache({ clock, ttlMs: 5000, maxEntries: 10 });

    // peer-1 records evt-A
    expect(cache.checkAndRecord('peer-1', 'evt-A')).toBe(false);

    // peer-2 also records evt-A (e.g. independently generated or simultaneous)
    expect(cache.checkAndRecord('peer-2', 'evt-A')).toBe(false);

    // peer-1 duplicate of evt-A
    expect(cache.checkAndRecord('peer-1', 'evt-A')).toBe(true);

    // peer-2 duplicate of evt-A
    expect(cache.checkAndRecord('peer-2', 'evt-A')).toBe(true);

    expect(cache.getStats().size).toBe(2);
  });

  it('expires entries after TTL using monotonic clock', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const cache = new DeduplicationCache({ clock, ttlMs: 2000, maxEntries: 10 });

    expect(cache.checkAndRecord('peer-1', 'evt-1')).toBe(false);
    expect(cache.checkAndRecord('peer-1', 'evt-1')).toBe(true);

    // Advance clock beyond TTL
    now = 3500;

    // Stale entry is pruned and treated as fresh
    expect(cache.checkAndRecord('peer-1', 'evt-1')).toBe(false);
  });

  it('evicts oldest entries when capacity is reached (LRU)', () => {
    let now = 1000;
    const clock: Clock = { now: () => now };
    const cache = new DeduplicationCache({ clock, ttlMs: 60000, maxEntries: 3 });

    cache.checkAndRecord('peer-1', 'evt-1'); // oldest
    cache.checkAndRecord('peer-1', 'evt-2');
    cache.checkAndRecord('peer-1', 'evt-3');

    expect(cache.getStats().size).toBe(3);

    // Adding 4th entry evicts evt-1
    cache.checkAndRecord('peer-1', 'evt-4');
    expect(cache.getStats().size).toBe(3);
    expect(cache.getStats().evictedCount).toBe(1);

    // evt-1 was evicted, so it is treated as fresh if it arrives again
    expect(cache.checkAndRecord('peer-1', 'evt-1')).toBe(false);
  });

  it('supports explicit remove() for failed local dispatches', () => {
    const clock: Clock = { now: () => 1000 };
    const cache = new DeduplicationCache({ clock, ttlMs: 5000, maxEntries: 10 });

    expect(cache.checkAndRecord('peer-local', 'evt-failed')).toBe(false);
    expect(cache.getStats().size).toBe(1);

    // Remove upon failure
    const removed = cache.remove('peer-local', 'evt-failed');
    expect(removed).toBe(true);
    expect(cache.getStats().size).toBe(0);

    // Can be recorded again
    expect(cache.checkAndRecord('peer-local', 'evt-failed')).toBe(false);
  });
});

