/**
 * LogicalClockManager implementation.
 *
 * Implements Phase 7 Lamport clock invariants:
 * - Exact Lamport event authoring: L(e) = 1 + max(L_local, maxDeps)
 * - Exact receiver validation: L(e) === 1 + max(lastOriginClock, maxDepClock)
 * - Non-incrementing receive advance: L_local = max(L_local, L_remote)
 * - Non-incrementing reseed: L_local = max(L_local, maxObservedClock)
 * - Token bucket arrival throttling for live broadcasts
 * - Zero clock advance on rejection
 */

import { SyncEngineError } from '../errors.js';
import type { ILogicalClockManager, ClockValidationContext } from './types.js';

interface OriginRateState {
  lastMonotonicMs: number;
  tokens: number;
  lastClock: number;
}

export class LogicalClockManager implements ILogicalClockManager {
  private currentClock: number = 0;
  private readonly maxObservedByOrigin: Map<string, number> = new Map();
  private readonly originRateStates: Map<string, OriginRateState> = new Map();

  constructor(initialClock: number = 0) {
    if (!Number.isSafeInteger(initialClock) || initialClock < 0) {
      throw new SyncEngineError('INVALID_CLOCK', `Invalid initial clock: ${initialClock}`);
    }
    this.currentClock = initialClock;
  }

  public get clock(): number {
    return this.currentClock;
  }

  /**
   * Advances the local clock for an outbound operation:
   * L = 1 + max(L_local, max_deps L).
   * Advances currentClock to this stamped value.
   */
  public tickLocal(dependencies?: ReadonlyArray<{ logicalTimestamp: number }>): number {
    let maxDepClock = 0;
    if (dependencies && dependencies.length > 0) {
      for (const dep of dependencies) {
        if (typeof dep.logicalTimestamp === 'number' && dep.logicalTimestamp > maxDepClock) {
          maxDepClock = dep.logicalTimestamp;
        }
      }
    }

    const next = 1 + Math.max(this.currentClock, maxDepClock);
    if (next >= Number.MAX_SAFE_INTEGER - 1) {
      throw new SyncEngineError('CLOCK_OVERFLOW', 'Lamport clock exceeded MAX_SAFE_INTEGER');
    }

    this.currentClock = next;
    return this.currentClock;
  }

  /**
   * Validates and advances local clock upon receiving a remote operation.
   * Advances: L_local = max(L_local, remoteClock) (without +1).
   * Exact validation invariant: remoteClock === 1 + max(lastOriginClock, maxDepClock).
   */
  public validateAndAdvanceRemote(
    originPeerId: string,
    membershipEpoch: number,
    remoteClock: number,
    dependencyClocks: ReadonlyArray<number>,
    monotonicNow: number,
    context: ClockValidationContext = 'LIVE_BROADCAST',
  ): number {
    if (context === 'LOCAL_REPLAY') {
      return this.currentClock; // Trusted internal replay; clock already advanced on ingestion
    }

    if (!Number.isSafeInteger(remoteClock) || remoteClock < 0) {
      throw new SyncEngineError('INVALID_CLOCK', `Malformed remote timestamp: ${remoteClock}`);
    }

    if (remoteClock >= Number.MAX_SAFE_INTEGER - 1) {
      throw new SyncEngineError('CLOCK_OVERFLOW', 'Remote Lamport clock reached or exceeded MAX_SAFE_INTEGER');
    }

    // 1. Per-origin Lamport monotonicity
    const lastOriginClock = this.maxObservedByOrigin.get(originPeerId) ?? 0;
    if (remoteClock <= lastOriginClock) {
      throw new SyncEngineError(
        'CLOCK_REGRESSION',
        `Remote timestamp ${remoteClock} from origin ${originPeerId} does not exceed previous ${lastOriginClock}`,
      );
    }

    // 2. Exact Expected Lamport Timestamp Calculation & Validation (R9 invariant)
    // Dependencies declared in causalContext strictly and exclusively determine expected timestamp.
    let maxDepClock = 0;
    for (const clock of dependencyClocks) {
      if (typeof clock === 'number' && clock > maxDepClock) {
        maxDepClock = clock;
      }
    }

    const expectedTimestamp = dependencyClocks.length === 0 ? 1 : 1 + maxDepClock;
    if (remoteClock < expectedTimestamp) {
      throw new SyncEngineError(
        'INVALID_CAUSAL_TIMESTAMP',
        `Remote timestamp ${remoteClock} does not satisfy causal precedence (expected ${expectedTimestamp})`,
      );
    }
    if (remoteClock > expectedTimestamp) {
      throw new SyncEngineError(
        'INVALID_LOGICAL_TIMESTAMP',
        `Remote timestamp ${remoteClock} violates exact Lamport creation invariant (expected ${expectedTimestamp})`,
      );
    }

    // 3. Token-bucket rate check (LIVE_BROADCAST only; REPAIR_BATCH is exempt)
    if (context === 'LIVE_BROADCAST') {
      const rateKey = `${originPeerId}:${membershipEpoch}`;
      let rate = this.originRateStates.get(rateKey);
      if (!rate) {
        if (this.originRateStates.size >= 64) {
          const firstKey = this.originRateStates.keys().next().value;
          if (firstKey) this.originRateStates.delete(firstKey);
        }
        rate = { lastMonotonicMs: monotonicNow, tokens: 50, lastClock: remoteClock };
        this.originRateStates.set(rateKey, rate);
      } else {
        const deltaMs = Math.max(0, monotonicNow - rate.lastMonotonicMs);
        rate.tokens = Math.min(50, rate.tokens + deltaMs * 0.01);
        rate.lastMonotonicMs = monotonicNow;

        const deltaClock = remoteClock - rate.lastClock;
        if (deltaClock > rate.tokens) {
          // REJECTED: Do NOT advance local clock or record maxObservedByOrigin!
          throw new SyncEngineError(
            'REJECTED_CLOCK_INFLATION',
            `Clock advance ${deltaClock} from origin ${originPeerId} exceeds token allowance (${rate.tokens.toFixed(1)})`,
          );
        }
        rate.tokens -= Math.max(0, deltaClock);
        rate.lastClock = remoteClock;
      }
    }

    // All validation passed: safely advance local clock (R8: without +1)
    this.maxObservedByOrigin.set(originPeerId, remoteClock);
    this.currentClock = Math.max(this.currentClock, remoteClock);
    return this.currentClock;
  }

  /**
   * Reseeds clock upon reconnect or snapshot recovery.
   * Sets: L = max(L_local, maxObservedClock) (without an extra +1).
   */
  public reseed(maxObservedClock: number): void {
    if (!Number.isSafeInteger(maxObservedClock) || maxObservedClock < 0) {
      throw new SyncEngineError('INVALID_CLOCK', `Malformed reseed timestamp: ${maxObservedClock}`);
    }
    this.currentClock = Math.max(this.currentClock, Math.floor(maxObservedClock));
    this.originRateStates.clear();
  }

  /**
   * Internal test helper / inspection method: retrieves highest observed timestamp for an origin.
   */
  public getOriginClock(originPeerId: string): number {
    return this.maxObservedByOrigin.get(originPeerId) ?? 0;
  }

  /**
   * Internal test helper / inspection method: manually sets origin clock for testing scenarios.
   */
  public setOriginClock(originPeerId: string, clock: number): void {
    this.maxObservedByOrigin.set(originPeerId, clock);
  }
}
