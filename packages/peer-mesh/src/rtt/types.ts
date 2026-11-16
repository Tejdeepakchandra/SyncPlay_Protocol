/**
 * RTT Engine type definitions.
 *
 * Per INTERFACE_CONTRACTS.md §3, PEER_MESH_RTT_ENGINE_LLD §7–9,
 * and DECISIONS.md (D1, D2, D10).
 *
 * All time arithmetic uses a monotonic clock (performance.now()).
 * Never use Date.now() for elapsed time calculations.
 */
import type { PeerId, ConnectionGeneration } from '@syncplay/protocol';

/**
 * Clock abstraction for monotonic time measurement and deterministic testing.
 */
export interface Clock {
  /** Returns monotonic time in milliseconds (e.g. performance.now()). */
  now(): number;
}

/**
 * A single validated round-trip time measurement.
 */
export interface RttSample {
  readonly peerId: PeerId;
  readonly probeId: string;
  readonly generation: ConnectionGeneration;
  /** Measured round-trip duration in milliseconds (finite, non-negative). */
  readonly rttMs: number;
  /** Monotonic timestamp (clock.now()) when the sample completed. */
  readonly timestamp: number;
}

/**
 * Comprehensive RTT statistics and freshness metrics for a single peer.
 * Conforms to INTERFACE_CONTRACTS.md §3.
 */
export interface RttEstimate {
  readonly peerId: PeerId;
  readonly generation: ConnectionGeneration;
  /** Number of valid samples currently retained in the bounded window. */
  readonly sampleCount: number;
  /** Most recently measured RTT in milliseconds. */
  readonly latestRttMs: number;
  /** Minimum RTT in milliseconds across the active sample window. */
  readonly minRttMs: number;
  /** Maximum RTT in milliseconds across the active sample window. */
  readonly maxRttMs: number;
  /** Arithmetic mean RTT in milliseconds across the active sample window. */
  readonly avgRttMs: number;
  /** Median RTT in milliseconds across the active sample window. */
  readonly medianRttMs: number;
  /** 90th percentile RTT in milliseconds across the active sample window. */
  readonly p90RttMs: number;
  /** Monotonic timestamp (clock.now()) when the last valid sample was recorded. */
  readonly lastMeasurementTimeMs: number;
  /**
   * True if elapsed time since lastMeasurementTimeMs exceeds staleThresholdMs.
   */
  readonly isStale: boolean;
  /** Total number of probes dispatched to this peer for the current generation. */
  readonly totalProbesSent: number;
  /** Total number of probes that timed out for the current generation. */
  readonly totalProbesTimedOut: number;
}

/**
 * Configuration options for the RTT Engine.
 */
export interface RttEngineConfig {
  /** Interval in milliseconds between scheduled probes. Default: 3000ms */
  readonly probeIntervalMs: number;
  /** Timeout in milliseconds before an unmatched probe is considered lost. Default: 5000ms */
  readonly probeTimeoutMs: number;
  /** Maximum number of samples retained in the bounded history per peer. Default: 20 */
  readonly maxHistorySamples: number;
  /** Duration in milliseconds after which an estimate is marked stale. Default: 10000ms */
  readonly staleThresholdMs: number;
  /** Maximum concurrent pending probes per peer. Default: 1 */
  readonly maxPendingProbes: number;
}

export const DEFAULT_RTT_ENGINE_CONFIG: RttEngineConfig = {
  probeIntervalMs: 3000,
  probeTimeoutMs: 5000,
  maxHistorySamples: 20,
  staleThresholdMs: 10000,
  maxPendingProbes: 1,
};

/**
 * Public RTT Service contract conforming to INTERFACE_CONTRACTS.md §3.
 */
export interface RttService {
  /** Start active RTT probing for a connected peer at the specified connection generation. */
  start(peerId: PeerId, generation: ConnectionGeneration): void;
  /** Stop active probing and clear pending probes for a peer. */
  stop(peerId: PeerId): void;
  /** Retrieve the current bounded RTT estimate and statistics for a peer. Returns undefined if no samples. */
  getEstimate(peerId: PeerId): RttEstimate | undefined;
  /** Subscribe to completed RTT samples. Returns an unsubscribe function. */
  onSample(handler: (sample: RttSample) => void): () => void;
  /** Clear all state, stop all timers, and dispose the engine. */
  dispose(): void;
}
