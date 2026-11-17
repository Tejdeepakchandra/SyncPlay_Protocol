/**
 * Unit tests for LogicalClockManager.
 *
 * Verifies Lamport clock invariants:
 * - Exact Lamport authoring and validation
 * - Receive clock advance without increment
 * - Snapshot reseed without increment
 * - Token bucket throttling and repair exemption
 * - Clock regression and overflow defenses
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { LogicalClockManager } from '../clock/logical-clock-manager.js';
import { SyncEngineError } from '../errors.js';

describe('LogicalClockManager', () => {
  let clockManager: LogicalClockManager;

  beforeEach(() => {
    clockManager = new LogicalClockManager(0);
  });

  describe('initialization', () => {
    it('initializes with default clock 0', () => {
      expect(clockManager.clock).toBe(0);
    });

    it('initializes with custom valid clock', () => {
      const mgr = new LogicalClockManager(42);
      expect(mgr.clock).toBe(42);
    });

    it('rejects invalid initial clocks', () => {
      expect(() => new LogicalClockManager(-1)).toThrow(SyncEngineError);
      expect(() => new LogicalClockManager(NaN)).toThrow(SyncEngineError);
      expect(() => new LogicalClockManager(1.5)).toThrow(SyncEngineError);
    });
  });

  describe('tickLocal', () => {
    it('advances clock by 1 when no dependencies are provided', () => {
      expect(clockManager.tickLocal()).toBe(1);
      expect(clockManager.clock).toBe(1);
      expect(clockManager.tickLocal()).toBe(2);
      expect(clockManager.clock).toBe(2);
    });

    it('advances clock to 1 + max(localClock, maxDeps)', () => {
      // Local clock is 0, dependencies include timestamp 10
      const next = clockManager.tickLocal([{ logicalTimestamp: 5 }, { logicalTimestamp: 10 }]);
      expect(next).toBe(11);
      expect(clockManager.clock).toBe(11);

      // Local clock is now 11, dependency with timestamp 8
      const next2 = clockManager.tickLocal([{ logicalTimestamp: 8 }]);
      expect(next2).toBe(12);
      expect(clockManager.clock).toBe(12);
    });

    it('throws CLOCK_OVERFLOW when reaching MAX_SAFE_INTEGER', () => {
      const mgr = new LogicalClockManager(Number.MAX_SAFE_INTEGER - 2);
      expect(() => mgr.tickLocal()).toThrow(SyncEngineError);
      try {
        mgr.tickLocal();
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('CLOCK_OVERFLOW');
      }
    });
  });

  describe('validateAndAdvanceRemote', () => {
    it('accepts exact expected timestamp for first event from origin', () => {
      // No prior origin clock, dependencies = [5, 8] -> expected = 1 + max(0, 8) = 9
      const clock = clockManager.validateAndAdvanceRemote('peer-A', 1, 9, [5, 8], 1000);
      expect(clock).toBe(9);
      expect(clockManager.clock).toBe(9);
      expect(clockManager.getOriginClock('peer-A')).toBe(9);
    });

    it('advances local clock to max(localClock, remoteClock) without adding 1', () => {
      // Local clock is 0
      clockManager.validateAndAdvanceRemote('peer-A', 1, 5, [4], 1000);
      expect(clockManager.clock).toBe(5);

      // Subsequent remote event with clock 4 from peer-B (where peer-B depended on 3)
      // Local clock should remain 5 (not 5 + 1)
      clockManager.validateAndAdvanceRemote('peer-B', 1, 4, [3], 1000);
      expect(clockManager.clock).toBe(5);
    });

    it('rejects remote event when timestamp is less than expected (INVALID_CAUSAL_TIMESTAMP)', () => {
      // Dependencies: max is 8 -> expected is 9
      expect(() =>
        clockManager.validateAndAdvanceRemote('peer-A', 1, 8, [5, 8], 1000),
      ).toThrow(SyncEngineError);

      try {
        clockManager.validateAndAdvanceRemote('peer-A', 1, 8, [5, 8], 1000);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_CAUSAL_TIMESTAMP');
      }

      // Local clock must not advance
      expect(clockManager.clock).toBe(0);
    });

    it('rejects remote event when timestamp is greater than expected (INVALID_LOGICAL_TIMESTAMP)', () => {
      // Dependencies: max is 8 -> expected is 9, but event claims 10
      expect(() =>
        clockManager.validateAndAdvanceRemote('peer-A', 1, 10, [5, 8], 1000),
      ).toThrow(SyncEngineError);

      try {
        clockManager.validateAndAdvanceRemote('peer-A', 1, 10, [5, 8], 1000);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('INVALID_LOGICAL_TIMESTAMP');
      }

      // Local clock must not advance
      expect(clockManager.clock).toBe(0);
    });

    it('rejects clock regression from same origin (CLOCK_REGRESSION)', () => {
      clockManager.validateAndAdvanceRemote('peer-A', 1, 5, [4], 1000);
      expect(clockManager.getOriginClock('peer-A')).toBe(5);

      // Next event from peer-A claims clock 5 or 4 (regression)
      expect(() =>
        clockManager.validateAndAdvanceRemote('peer-A', 1, 5, [4], 1000),
      ).toThrow(SyncEngineError);

      try {
        clockManager.validateAndAdvanceRemote('peer-A', 1, 5, [4], 1000);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('CLOCK_REGRESSION');
      }
    });

    it('enforces token-bucket arrival throttling for LIVE_BROADCAST', () => {
      // Burst capacity: 50. Refill rate: 0.01 tokens/ms.
      // First event establishes rate state at monotonicNow = 1000, lastClock = 1
      clockManager.validateAndAdvanceRemote('peer-A', 1, 1, [], 1000, 'LIVE_BROADCAST');

      // Next event at monotonicNow = 1010 (10ms later = 0.1 tokens refilled, remaining ~49.9)
      // Clock jump = 60 (exceeds tokens)
      // Wait, to jump by 60, expectedTimestamp would need dependencies up to 59.
      // Suppose dependencies include 59:
      expect(() =>
        clockManager.validateAndAdvanceRemote('peer-A', 1, 60, [59], 1010, 'LIVE_BROADCAST'),
      ).toThrow(SyncEngineError);

      try {
        clockManager.validateAndAdvanceRemote('peer-A', 1, 60, [59], 1010, 'LIVE_BROADCAST');
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('REJECTED_CLOCK_INFLATION');
      }
    });

    it('exempts REPAIR_BATCH from token-bucket rate limits', () => {
      clockManager.validateAndAdvanceRemote('peer-A', 1, 1, [], 1000, 'LIVE_BROADCAST');

      // Repair batch arrives 10ms later with jump of 60
      const nextClock = clockManager.validateAndAdvanceRemote(
        'peer-A',
        1,
        60,
        [59],
        1010,
        'REPAIR_BATCH',
      );
      expect(nextClock).toBe(60);
      expect(clockManager.clock).toBe(60);
    });

    it('bypasses validation on LOCAL_REPLAY', () => {
      const clockBefore = clockManager.clock;
      const res = clockManager.validateAndAdvanceRemote(
        'peer-A',
        1,
        999,
        [],
        1000,
        'LOCAL_REPLAY',
      );
      expect(res).toBe(clockBefore);
      expect(clockManager.clock).toBe(clockBefore);
    });
  });

  describe('reseed', () => {
    it('reseeds clock to max(localClock, maxObservedClock) without adding 1', () => {
      clockManager.reseed(25);
      expect(clockManager.clock).toBe(25);

      // Reseeding to lower value preserves current clock
      clockManager.reseed(10);
      expect(clockManager.clock).toBe(25);
    });

    it('subsequent tickLocal after reseed increments exactly once', () => {
      clockManager.reseed(25);
      const next = clockManager.tickLocal();
      expect(next).toBe(26);
      expect(clockManager.clock).toBe(26);
    });

    it('rejects invalid reseed timestamps', () => {
      expect(() => clockManager.reseed(-5)).toThrow(SyncEngineError);
      expect(() => clockManager.reseed(NaN)).toThrow(SyncEngineError);
    });
  });
});
