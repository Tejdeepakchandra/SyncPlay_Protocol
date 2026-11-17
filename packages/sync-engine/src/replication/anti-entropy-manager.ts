/**
 * Anti-Entropy Manager & Digest Scheduler.
 *
 * Implements Phase 7.4 specifications:
 * - PHASE7_4_ARCHITECTURE.md §3 & §6
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §2.1
 * - PHASE7_4_ACCEPTANCE_CRITERIA.md AC-1 & AC-2
 *
 * Responsible for:
 * 1. Generating consistent local StateDigestPayloads.
 * 2. Evaluating inbound remote digests.
 * 3. Bounded periodic digest scheduling (5000ms ± 1000ms uniform jitter).
 * 4. Coalesced reactive digest scheduling on gap detection / peer connect / epoch transition.
 * 5. Token bucket rate limiting (1 digest/sec, burst 2) to protect transport.
 * 6. Deterministic clock injection and lifecycle management.
 *
 * NOTE: Repair request dispatching (M3), supplier range servicing (M4),
 * and transport framing (M5) remain unimplemented in this milestone.
 */

import type {
  StateDigestPayload,
  RepairRequestPayload,
  OperationBatchPayload,
  RepairRejectedPayload,
} from '@syncplay/protocol';
import type { LeaderlessSyncEngine } from '../engine/leaderless-sync-engine.js';
import { SyncEngineError } from '../errors.js';
import type {
  IAntiEntropyManager,
  AntiEntropyStatus,
  AntiEntropyCallbacks,
  AntiEntropySchedulerOptions,
  SchedulerClock,
  DigestComparisonResult,
} from './types.js';

export const DEFAULT_PERIODIC_INTERVAL_MS = 5000;
export const DEFAULT_JITTER_MS = 1000;
export const DEFAULT_RATE_LIMIT_MIN_INTERVAL_MS = 1000;
export const DEFAULT_RATE_LIMIT_BURST = 2;

/**
 * Standard default system scheduler clock.
 */
export const defaultSchedulerClock: SchedulerClock = {
  now: () => (typeof performance !== 'undefined' ? performance.now() : Date.now()),
  setTimeout: (cb: () => void, ms: number) => setTimeout(cb, ms),
  clearTimeout: (id: unknown) => clearTimeout(id as any),
};

export class AntiEntropyManager implements IAntiEntropyManager {
  private readonly engine: LeaderlessSyncEngine;
  private readonly callbacks: AntiEntropyCallbacks;
  private readonly clock: SchedulerClock;
  private readonly periodicIntervalMs: number;
  private readonly jitterMs: number;
  private readonly rateLimitMinIntervalMs: number;
  private readonly rateLimitBurst: number;
  private readonly rng: () => number;

  private disposed = false;
  private periodicTimerId: unknown = undefined;
  private reactiveTimerId: unknown = undefined;

  // Token bucket rate limiter state
  private tokens: number;
  private lastTokenRefillAt: number;
  private lastDigestSentAt = 0;

  constructor(
    engine: LeaderlessSyncEngine,
    callbacks: AntiEntropyCallbacks,
    options?: AntiEntropySchedulerOptions,
    clock?: SchedulerClock,
  ) {
    this.engine = engine;
    this.callbacks = callbacks;
    this.clock = clock ?? defaultSchedulerClock;
    this.periodicIntervalMs = options?.periodicIntervalMs ?? DEFAULT_PERIODIC_INTERVAL_MS;
    this.jitterMs = options?.jitterMs ?? DEFAULT_JITTER_MS;
    this.rateLimitMinIntervalMs =
      options?.rateLimitMinIntervalMs ?? DEFAULT_RATE_LIMIT_MIN_INTERVAL_MS;
    this.rateLimitBurst = options?.rateLimitBurst ?? DEFAULT_RATE_LIMIT_BURST;
    this.rng = options?.rng ?? Math.random;

    this.tokens = this.rateLimitBurst;
    this.lastTokenRefillAt = this.clock.now();

    // Start background periodic digest exchange timer
    this.scheduleNextPeriodicTick();
  }

  public get isDisposed(): boolean {
    return this.disposed;
  }

  /**
   * Generates local StateDigestPayload for broadcasting.
   */
  public createLocalDigest(): StateDigestPayload {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot create digest from disposed manager');
    }
    return this.engine.createLocalDigest();
  }

  /**
   * Evaluates an incoming remote digest against local replication knowledge.
   */
  public processRemoteDigest(digest: StateDigestPayload): DigestComparisonResult {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot process digest on disposed manager');
    }
    return this.engine.compareRemoteDigest(digest);
  }

  /**
   * Triggers a coalesced reactive digest transmission.
   * Multiple calls within the same event loop tick are coalesced into a single dispatch.
   */
  public triggerReactiveDigest(): void {
    if (this.disposed) return;

    if (this.reactiveTimerId !== undefined) {
      // Already scheduled for next tick: coalesce
      return;
    }

    this.reactiveTimerId = this.clock.setTimeout(() => {
      this.reactiveTimerId = undefined;
      if (this.disposed) return;
      this.broadcastDigestIfPermitted();
    }, 0);
  }

  /**
   * Refills token bucket and broadcasts local digest if a token is available.
   * Returns true if broadcast occurred, false if throttled by rate limit.
   */
  public broadcastDigestIfPermitted(): boolean {
    if (this.disposed) return false;

    const now = this.clock.now();
    const elapsed = Math.max(0, now - this.lastTokenRefillAt);
    const refillRatePerMs = 1 / this.rateLimitMinIntervalMs;
    this.tokens = Math.min(this.rateLimitBurst, this.tokens + elapsed * refillRatePerMs);
    this.lastTokenRefillAt = now;

    if (this.tokens < 1) {
      // Rate limit throttled
      return false;
    }

    this.tokens -= 1;
    this.lastDigestSentAt = now;

    try {
      const digest = this.engine.createLocalDigest(now);
      this.callbacks.onBroadcastDigest(digest);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Cancels any active repair requests to a departing peer.
   * Purges peer tracking and reset peer rate limits.
   */
  public onPeerDisconnected(_peerId: string): void {
    if (this.disposed) return;
    // M3 will cancel active repair requests to departing peers.
    // For M2, we ensure no dangling timer or state exists.
  }

  /**
   * Resets anti-entropy scheduling upon membership epoch transition.
   */
  public onEpochTransition(_newEpoch: number): void {
    if (this.disposed) return;

    // Cancel in-flight reactive timers
    if (this.reactiveTimerId !== undefined) {
      this.clock.clearTimeout(this.reactiveTimerId);
      this.reactiveTimerId = undefined;
    }

    // Reset periodic timer with fresh jitter
    this.scheduleNextPeriodicTick();

    // Trigger reactive digest broadcast under new epoch
    this.triggerReactiveDigest();
  }

  /**
   * Serves an inbound REPAIR_REQUEST from local log history (M4).
   * Unimplemented in M2.
   */
  public serveRepairRequest(
    _request: RepairRequestPayload,
  ): OperationBatchPayload | RepairRejectedPayload {
    throw new Error('AntiEntropyManager.serveRepairRequest() is scheduled for Phase 7.4 M4');
  }

  /**
   * Schedules targeted repair requests for detected sequence gaps (M3).
   * Unimplemented in M2.
   */
  public reconcileGaps(): void {
    throw new Error('AntiEntropyManager.reconcileGaps() is scheduled for Phase 7.4 M3');
  }

  /**
   * Retrieves operational status and telemetry for diagnostics.
   */
  public getStatus(): AntiEntropyStatus {
    return {
      activeRequestsCount: 0,
      pendingGapsCount: this.engine.pendingQueueSize,
      lastDigestSentAt: this.lastDigestSentAt,
      totalRepairsCompleted: 0,
      totalRepairsFailed: 0,
      degraded: false,
    };
  }

  /**
   * Idempotent disposal. Cancels all active timers and clears references.
   * Guarantees no callbacks will fire after disposal.
   */
  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    if (this.periodicTimerId !== undefined) {
      this.clock.clearTimeout(this.periodicTimerId);
      this.periodicTimerId = undefined;
    }

    if (this.reactiveTimerId !== undefined) {
      this.clock.clearTimeout(this.reactiveTimerId);
      this.reactiveTimerId = undefined;
    }
  }

  /**
   * Schedules the next periodic tick with uniform random jitter.
   */
  private scheduleNextPeriodicTick(): void {
    if (this.disposed) return;

    if (this.periodicTimerId !== undefined) {
      this.clock.clearTimeout(this.periodicTimerId);
      this.periodicTimerId = undefined;
    }

    // Interval = periodicIntervalMs ± jitterMs
    const randomFraction = this.rng(); // [0, 1)
    const jitter = (randomFraction * 2 - 1) * this.jitterMs; // [-jitterMs, +jitterMs]
    const delayMs = Math.max(100, Math.round(this.periodicIntervalMs + jitter));

    this.periodicTimerId = this.clock.setTimeout(() => {
      this.periodicTimerId = undefined;
      if (this.disposed) return;
      this.broadcastDigestIfPermitted();
      this.scheduleNextPeriodicTick();
    }, delayMs);
  }
}
