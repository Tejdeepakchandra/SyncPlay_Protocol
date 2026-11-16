/**
 * Bounded Rate Limiter.
 *
 * Enforces sliding-window rate limits on:
 * 1. Local outbound commands (prevents application UI button mashing from flooding DataChannels).
 * 2. Inbound control events per remote peer (protects against event floods from rogue/malfunctioning peers).
 *
 * Resource bounds:
 * - Bounded peer tracking (maxTrackedPeers, default: 64).
 * - Automatic LRU eviction of inactive peer records if peer limit is reached.
 * - Explicit peer cleanup via removePeer(peerId) when peers disconnect.
 * - Zero background timers or leaked intervals.
 */

import type { PeerId } from '@syncplay/protocol';
import type { Clock } from './types.js';

interface PeerWindow {
  timestamps: number[];
  lastSeenAt: number;
}

export class BoundedRateLimiter {
  private readonly maxOpsPerWindow: number;
  private readonly windowMs: number;
  private readonly maxTrackedPeers: number;
  private readonly clock: Clock;

  // Local outbound timestamps
  private localTimestamps: number[] = [];

  // Inbound timestamps per remote peer
  private readonly peerWindows = new Map<PeerId, PeerWindow>();

  constructor(options: {
    maxOperationsPerSecond?: number;
    windowMs?: number;
    maxTrackedPeers?: number;
    clock?: Clock;
  } = {}) {
    this.maxOpsPerWindow = options.maxOperationsPerSecond ?? 20;
    this.windowMs = options.windowMs ?? 1000;
    this.maxTrackedPeers = options.maxTrackedPeers ?? 64;
    this.clock = options.clock ?? { now: () => performance.now() };
  }

  /**
   * Check whether a local outbound command can proceed.
   *
   * @returns `true` if allowed, `false` if rate limit exceeded.
   */
  public checkOutbound(): boolean {
    const now = this.clock.now();
    const cutoff = now - this.windowMs;

    // Prune expired timestamps
    this.localTimestamps = this.localTimestamps.filter((t) => t > cutoff);

    if (this.localTimestamps.length >= this.maxOpsPerWindow) {
      return false;
    }

    this.localTimestamps.push(now);
    return true;
  }

  /**
   * Check whether an inbound event from a specific remote peer can proceed.
   *
   * @returns `true` if allowed, `false` if rate limit exceeded.
   */
  public checkInbound(peerId: PeerId): boolean {
    const now = this.clock.now();
    const cutoff = now - this.windowMs;

    let window = this.peerWindows.get(peerId);

    if (!window) {
      // If capacity reached, evict the peer with the oldest lastSeenAt
      if (this.peerWindows.size >= this.maxTrackedPeers) {
        let oldestPeer: PeerId | null = null;
        let oldestTime = Infinity;

        for (const [id, entry] of this.peerWindows.entries()) {
          if (entry.lastSeenAt < oldestTime) {
            oldestTime = entry.lastSeenAt;
            oldestPeer = id;
          }
        }

        if (oldestPeer !== null) {
          this.peerWindows.delete(oldestPeer);
        }
      }

      window = { timestamps: [], lastSeenAt: now };
      this.peerWindows.set(peerId, window);
    }

    window.lastSeenAt = now;
    window.timestamps = window.timestamps.filter((t) => t > cutoff);

    if (window.timestamps.length >= this.maxOpsPerWindow) {
      return false;
    }

    window.timestamps.push(now);
    return true;
  }

  /**
   * Explicitly remove tracking for a peer when disconnected or closed.
   */
  public removePeer(peerId: PeerId): void {
    this.peerWindows.delete(peerId);
  }

  /**
   * Clear all rate limiting state.
   */
  public clear(): void {
    this.localTimestamps = [];
    this.peerWindows.clear();
  }

  /**
   * Inspect currently tracked peer count.
   */
  public getTrackedPeerCount(): number {
    return this.peerWindows.size;
  }
}

