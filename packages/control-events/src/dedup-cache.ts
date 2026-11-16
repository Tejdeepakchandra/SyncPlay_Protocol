/**
 * Deduplication Cache.
 *
 * Implements an in-memory LRU cache with monotonic TTL expiration.
 *
 * Requirements:
 * - Composite deduplication key: `${senderPeerId}:${logicalEventId}` (prevents cross-peer ID collisions).
 * - Monotonic clock for all TTL checks (never Date.now()).
 * - Bounded maximum capacity with LRU eviction.
 * - Supports removal if local dispatch throws.
 */

import type { Clock, DeduplicationStats } from './types.js';

interface CacheEntry {
  receivedAt: number;
}

export class DeduplicationCache {
  private readonly cache = new Map<string, CacheEntry>();
  private readonly maxEntries: number;
  private readonly ttlMs: number;
  private readonly clock: Clock;

  private duplicateCount = 0;
  private evictedCount = 0;

  constructor(options: {
    maxEntries?: number;
    ttlMs?: number;
    clock?: Clock;
  } = {}) {
    this.maxEntries = options.maxEntries ?? 500;
    this.ttlMs = options.ttlMs ?? 60_000;
    this.clock = options.clock ?? { now: () => performance.now() };
  }

  /**
   * Format composite deduplication key.
   */
  public makeKey(senderPeerId: string, logicalEventId: string): string {
    return `${senderPeerId}:${logicalEventId}`;
  }

  /**
   * Check if an event is a duplicate. If fresh, records it into the cache.
   *
   * @returns `true` if duplicate (already active within TTL), `false` if fresh (recorded).
   */
  public checkAndRecord(senderPeerId: string, logicalEventId: string): boolean {
    const key = this.makeKey(senderPeerId, logicalEventId);
    const now = this.clock.now();
    const existing = this.cache.get(key);

    if (existing !== undefined) {
      if (now - existing.receivedAt <= this.ttlMs) {
        // Active duplicate: refresh LRU position
        this.cache.delete(key);
        this.cache.set(key, existing);
        this.duplicateCount++;
        return true;
      }
      // Expired entry: prune stale record
      this.cache.delete(key);
    }

    // Capacity check: evict oldest if at capacity
    if (this.cache.size >= this.maxEntries) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
        this.evictedCount++;
      }
    }

    // Insert new entry at end of Map (most recently used)
    this.cache.set(key, { receivedAt: now });
    return false;
  }

  /**
   * Remove an entry from the cache.
   * Used when local receiver dispatch throws, removing the pre-recorded entry
   * so it does not leave orphaned suppression state.
   */
  public remove(senderPeerId: string, logicalEventId: string): boolean {
    const key = this.makeKey(senderPeerId, logicalEventId);
    return this.cache.delete(key);
  }

  /**
   * Clear all entries and reset counters.
   */
  public clear(): void {
    this.cache.clear();
    this.duplicateCount = 0;
    this.evictedCount = 0;
  }

  /**
   * Inspect current cache statistics.
   */
  public getStats(): DeduplicationStats {
    return {
      size: this.cache.size,
      duplicateCount: this.duplicateCount,
      evictedCount: this.evictedCount,
    };
  }
}

