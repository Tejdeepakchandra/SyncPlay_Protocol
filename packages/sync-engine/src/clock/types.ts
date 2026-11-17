/**
 * Logical clock manager contracts.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_INTERFACE_CONTRACTS.md §2.1
 * - PHASE7_2_LLD.md §1.4
 */

export type ClockValidationContext = 'LIVE_BROADCAST' | 'REPAIR_BATCH' | 'LOCAL_REPLAY';

export interface ILogicalClockManager {
  /** Current local Lamport logical counter. */
  readonly clock: number;

  /**
   * Advances the local clock for an outbound operation:
   * L = 1 + max(L_local, max_deps L).
   * Throws SyncEngineError('CLOCK_OVERFLOW') if L >= Number.MAX_SAFE_INTEGER - 1.
   */
  tickLocal(dependencies?: ReadonlyArray<{ logicalTimestamp: number }>): number;

  /**
   * Validates and advances local clock upon receiving a remote operation.
   * Advances: L = max(L_local, remoteClock) (without +1; receiving an event is not a local event creation).
   * Exact validation invariant: remoteClock MUST EQUAL 1 + max(lastOriginClock, maxDepClock).
   * Receiver isolation: Validation evaluates dependencies strictly from causalContext; receiver concurrent observations are never substituted.
   * Throws SyncEngineError('INVALID_CAUSAL_TIMESTAMP') if remoteClock < expected.
   * Throws SyncEngineError('INVALID_LOGICAL_TIMESTAMP') if remoteClock > expected.
   * Token bucket arrival throttling is evaluated only when context === 'LIVE_BROADCAST'.
   * Critical safety invariant: If validation fails, the local clock is NEVER advanced.
   */
  validateAndAdvanceRemote(
    originPeerId: string,
    membershipEpoch: number,
    remoteClock: number,
    dependencyClocks: ReadonlyArray<number>,
    monotonicNow: number,
    context?: ClockValidationContext,
  ): number;

  /**
   * Reseeds clock upon reconnect or snapshot recovery.
   * Sets: L = max(L_local, maxObservedClock) (without an extra +1).
   */
  reseed(maxObservedClock: number): void;
}
