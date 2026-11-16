/**
 * Bounded RTT measurement history and statistics reducer.
 *
 * Per PEER_MESH_RTT_ENGINE_LLD §7 and Phase 4 requirements.
 *
 * Maintains a bounded rolling window of valid samples.
 * Prevents invalid/negative/infinite measurements from entering history.
 */
import type { PeerId, ConnectionGeneration } from '@syncplay/protocol';
import type { RttSample, RttEstimate } from './types.js';

/**
 * Validates whether an RTT duration is a legitimate measurement.
 *
 * - Must be a finite number (rejects NaN, Infinity, -Infinity).
 * - Must be non-negative (rejects negative numbers without clamping).
 * - Genuine zero measurements (e.g. from fast loopbacks) are accepted.
 */
export function isValidRtt(rttMs: unknown): rttMs is number {
  return typeof rttMs === 'number' && Number.isFinite(rttMs) && rttMs >= 0;
}

/**
 * Computes the 90th percentile (p90) from a sorted array of numbers
 * using standard linear interpolation between adjacent ranks:
 *
 * rank = 0.9 * (N - 1)
 * i = floor(rank)
 * fraction = rank - i
 * p90 = sorted[i] + fraction * (sorted[i + 1] - sorted[i])
 *
 * @param sorted Ascending-sorted array of numbers (length >= 1)
 */
export function calculateP90(sorted: readonly number[]): number {
  const n = sorted.length;
  if (n === 0) return 0;
  if (n === 1) return sorted[0]!;

  const rank = 0.9 * (n - 1);
  const i = Math.floor(rank);
  const fraction = rank - i;

  if (i >= n - 1) {
    return sorted[n - 1]!;
  }

  return sorted[i]! + fraction * (sorted[i + 1]! - sorted[i]!);
}

/**
 * Computes median from a sorted array of numbers:
 * - If odd: value at index floor(N / 2)
 * - If even: arithmetic mean of values at N/2 - 1 and N/2
 */
export function calculateMedian(sorted: readonly number[]): number {
  const n = sorted.length;
  if (n === 0) return 0;
  const mid = Math.floor(n / 2);
  if (n % 2 !== 0) {
    return sorted[mid]!;
  }
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

export class RttHistory {
  private readonly samples: RttSample[] = [];
  private readonly maxSamples: number;
  private readonly staleThresholdMs: number;
  private probesSent = 0;
  private probesTimedOut = 0;

  readonly peerId: PeerId;
  private generation: ConnectionGeneration;

  constructor(
    peerId: PeerId,
    generation: ConnectionGeneration,
    maxSamples: number = 20,
    staleThresholdMs: number = 10000,
  ) {
    this.peerId = peerId;
    this.generation = generation;
    this.maxSamples = Math.max(1, maxSamples);
    this.staleThresholdMs = Math.max(1, staleThresholdMs);
  }

  /**
   * Current connection generation for this history.
   */
  get currentGeneration(): ConnectionGeneration {
    return this.generation;
  }

  /**
   * Resets history and counters for a new connection generation.
   */
  resetForGeneration(newGeneration: ConnectionGeneration): void {
    this.generation = newGeneration;
    this.samples.length = 0;
    this.probesSent = 0;
    this.probesTimedOut = 0;
  }

  /**
   * Record that a probe was dispatched.
   */
  recordProbeSent(): void {
    this.probesSent++;
  }

  /**
   * Record that a probe timed out without a valid response.
   */
  recordProbeTimeout(): void {
    this.probesTimedOut++;
  }

  /**
   * Add a validated RTT sample.
   *
   * Rejects invalid values (NaN, Infinity, negative).
   * Evicts the oldest sample when capacity is reached.
   *
   * @returns true if sample was valid and added, false if rejected.
   */
  addSample(sample: RttSample): boolean {
    if (!isValidRtt(sample.rttMs)) {
      return false;
    }

    // Ignore samples from older generations
    if (sample.generation !== this.generation) {
      return false;
    }

    if (this.samples.length >= this.maxSamples) {
      this.samples.shift(); // Evict oldest
    }

    this.samples.push(sample);
    return true;
  }

  /**
   * Current number of retained valid samples.
   */
  get sampleCount(): number {
    return this.samples.length;
  }

  /**
   * Get all current samples in chronological order.
   */
  getSamples(): readonly RttSample[] {
    return this.samples;
  }

  /**
   * Computes an RttEstimate snapshot based on currently retained samples.
   *
   * Returns undefined if no samples have been collected.
   *
   * @param now Current monotonic timestamp from Clock.now()
   */
  getEstimate(now: number): RttEstimate | undefined {
    const n = this.samples.length;
    if (n === 0) {
      return undefined;
    }

    const latest = this.samples[n - 1]!;
    const durations = this.samples.map((s) => s.rttMs);

    let min = durations[0]!;
    let max = durations[0]!;
    let sum = 0;

    for (const d of durations) {
      if (d < min) min = d;
      if (d > max) max = d;
      sum += d;
    }

    const avg = sum / n;

    // Sort durations ascending for median and p90
    const sorted = [...durations].sort((a, b) => a - b);
    const median = calculateMedian(sorted);
    const p90 = calculateP90(sorted);

    const isStale = (now - latest.timestamp) > this.staleThresholdMs;

    return {
      peerId: this.peerId,
      generation: this.generation,
      sampleCount: n,
      latestRttMs: latest.rttMs,
      minRttMs: min,
      maxRttMs: max,
      avgRttMs: avg,
      medianRttMs: median,
      p90RttMs: p90,
      lastMeasurementTimeMs: latest.timestamp,
      isStale,
      totalProbesSent: this.probesSent,
      totalProbesTimedOut: this.probesTimedOut,
    };
  }

  /**
   * Clear all stored samples and counters.
   */
  clear(): void {
    this.samples.length = 0;
    this.probesSent = 0;
    this.probesTimedOut = 0;
  }
}
