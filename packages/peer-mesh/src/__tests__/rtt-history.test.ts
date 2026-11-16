/**
 * Unit tests for RttHistory, bounded buffer, validation, and statistical metrics.
 */
import { describe, it, expect } from 'vitest';
import { RttHistory, isValidRtt, calculateMedian, calculateP90 } from '../rtt/rtt-history.js';
import type { RttSample } from '../rtt/types.js';

describe('isValidRtt', () => {
  it('accepts legitimate positive numbers', () => {
    expect(isValidRtt(10)).toBe(true);
    expect(isValidRtt(0.5)).toBe(true);
    expect(isValidRtt(150.25)).toBe(true);
  });

  it('accepts genuine zero measurement', () => {
    expect(isValidRtt(0)).toBe(true);
  });

  it('rejects negative numbers without clamping', () => {
    expect(isValidRtt(-1)).toBe(false);
    expect(isValidRtt(-0.001)).toBe(false);
  });

  it('rejects NaN, Infinity, -Infinity', () => {
    expect(isValidRtt(NaN)).toBe(false);
    expect(isValidRtt(Infinity)).toBe(false);
    expect(isValidRtt(-Infinity)).toBe(false);
  });

  it('rejects non-number types', () => {
    expect(isValidRtt('50')).toBe(false);
    expect(isValidRtt(null)).toBe(false);
    expect(isValidRtt(undefined)).toBe(false);
    expect(isValidRtt({})).toBe(false);
  });
});

describe('calculateMedian', () => {
  it('handles empty array', () => {
    expect(calculateMedian([])).toBe(0);
  });

  it('handles single element', () => {
    expect(calculateMedian([42])).toBe(42);
  });

  it('handles odd number of elements', () => {
    expect(calculateMedian([10, 20, 30])).toBe(20);
    expect(calculateMedian([10, 20, 30, 40, 50])).toBe(30);
  });

  it('handles even number of elements', () => {
    expect(calculateMedian([10, 20])).toBe(15);
    expect(calculateMedian([10, 20, 30, 40])).toBe(25);
  });
});

describe('calculateP90', () => {
  it('handles empty array', () => {
    expect(calculateP90([])).toBe(0);
  });

  it('handles single element', () => {
    expect(calculateP90([100])).toBe(100);
  });

  it('calculates p90 with linear interpolation', () => {
    // 10 elements: rank = 0.9 * 9 = 8.1
    // i = 8, fraction = 0.1
    // p90 = sorted[8] + 0.1 * (sorted[9] - sorted[8])
    const samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    // sorted[8] is 90, sorted[9] is 100 -> 90 + 0.1 * 10 = 91
    expect(calculateP90(samples)).toBeCloseTo(91, 5);
  });
});

describe('RttHistory', () => {
  const makeSample = (rttMs: number, timestamp: number = 1000, generation: number = 1): RttSample => ({
    peerId: 'peer-1',
    probeId: 'probe-1',
    generation,
    rttMs,
    timestamp,
  });

  it('returns undefined when no samples exist', () => {
    const history = new RttHistory('peer-1', 1, 5);
    expect(history.getEstimate(1000)).toBeUndefined();
    expect(history.sampleCount).toBe(0);
  });

  it('adds valid samples and reports single-sample metrics', () => {
    const history = new RttHistory('peer-1', 1, 5, 10000);
    expect(history.addSample(makeSample(50, 1000))).toBe(true);

    const est = history.getEstimate(1500);
    expect(est).toBeDefined();
    expect(est?.peerId).toBe('peer-1');
    expect(est?.generation).toBe(1);
    expect(est?.sampleCount).toBe(1);
    expect(est?.latestRttMs).toBe(50);
    expect(est?.minRttMs).toBe(50);
    expect(est?.maxRttMs).toBe(50);
    expect(est?.avgRttMs).toBe(50);
    expect(est?.medianRttMs).toBe(50);
    expect(est?.p90RttMs).toBe(50);
    expect(est?.lastMeasurementTimeMs).toBe(1000);
    expect(est?.isStale).toBe(false);
  });

  it('enforces bounded capacity and evicts oldest sample', () => {
    const history = new RttHistory('peer-1', 1, 3);
    history.addSample(makeSample(10, 100));
    history.addSample(makeSample(20, 200));
    history.addSample(makeSample(30, 300));
    expect(history.sampleCount).toBe(3);

    // Adding 4th sample evicts the 1st sample (10ms)
    history.addSample(makeSample(40, 400));
    expect(history.sampleCount).toBe(3);

    const samples = history.getSamples();
    expect(samples.map((s) => s.rttMs)).toEqual([20, 30, 40]);

    const est = history.getEstimate(500);
    expect(est?.minRttMs).toBe(20);
    expect(est?.maxRttMs).toBe(40);
    expect(est?.avgRttMs).toBe(30);
    expect(est?.latestRttMs).toBe(40);
  });

  it('rejects invalid, negative, NaN, and infinite samples', () => {
    const history = new RttHistory('peer-1', 1, 5);
    expect(history.addSample(makeSample(-10))).toBe(false);
    expect(history.addSample(makeSample(NaN))).toBe(false);
    expect(history.addSample(makeSample(Infinity))).toBe(false);
    expect(history.addSample(makeSample(-Infinity))).toBe(false);
    expect(history.sampleCount).toBe(0);
    expect(history.getEstimate(1000)).toBeUndefined();
  });

  it('rejects samples from a different generation', () => {
    const history = new RttHistory('peer-1', 2, 5);
    // Generation 1 sample arrives for generation 2 history
    expect(history.addSample(makeSample(50, 1000, 1))).toBe(false);
    expect(history.sampleCount).toBe(0);
  });

  it('correctly calculates staleness according to threshold', () => {
    const staleThreshold = 5000;
    const history = new RttHistory('peer-1', 1, 5, staleThreshold);
    history.addSample(makeSample(30, 1000));

    // At t=4000 (elapsed 3000ms <= 5000ms) -> fresh
    expect(history.getEstimate(4000)?.isStale).toBe(false);

    // At t=6000 (elapsed 5000ms <= 5000ms) -> fresh
    expect(history.getEstimate(6000)?.isStale).toBe(false);

    // At t=6001 (elapsed 5001ms > 5000ms) -> stale
    expect(history.getEstimate(6001)?.isStale).toBe(true);
  });

  it('records probes sent and timed out counters', () => {
    const history = new RttHistory('peer-1', 1, 5);
    history.recordProbeSent();
    history.recordProbeSent();
    history.recordProbeTimeout();

    history.addSample(makeSample(25, 1000));
    const est = history.getEstimate(1000);
    expect(est?.totalProbesSent).toBe(2);
    expect(est?.totalProbesTimedOut).toBe(1);
  });

  it('resets completely on generation change', () => {
    const history = new RttHistory('peer-1', 1, 5);
    history.recordProbeSent();
    history.recordProbeTimeout();
    history.addSample(makeSample(25, 1000));

    expect(history.sampleCount).toBe(1);

    history.resetForGeneration(2);
    expect(history.currentGeneration).toBe(2);
    expect(history.sampleCount).toBe(0);
    expect(history.getEstimate(1000)).toBeUndefined();

    // Now accepts gen 2 samples
    expect(history.addSample(makeSample(35, 2000, 2))).toBe(true);
    expect(history.getEstimate(2000)?.generation).toBe(2);
  });
});
