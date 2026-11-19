/**
 * Comprehensive test suite for Phase 7.5 M4.1 transport-dependent admission and chunk sizing.
 *
 * Covers:
 * - Boundaries: 2,143, 2,144, 4,096, 8,192, 16,384, 32,768, 45,492, and 65,536 bytes
 * - Minimum raw chunk enforcement (>= 256 bytes)
 * - Exact required chunk count calculation
 * - Maximum 128 chunks ceiling
 * - Full 4 MiB snapshot admission on eligible transports
 * - Rejection before buffer allocation when capacity is insufficient
 * - No production admission on unnegotiated fallback
 */
import { describe, it, expect } from 'vitest';
import {
  calculateChunkCapacity,
  calculateSnapshotAdmission,
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  MIN_TRANSPORT_LIMIT_FULL_CHUNK,
} from '../index.js';

describe('Phase 7.5 M4.1 — Transport-Dependent Admission and Sizing', () => {
  describe('calculateChunkCapacity()', () => {
    it('should throw an error if effectiveLimit is below minimum transport floor (2,144 B)', () => {
      expect(() => calculateChunkCapacity(2143)).toThrow(/below minimum supported transport limit/i);
      expect(() => calculateChunkCapacity(2048)).toThrow(/below minimum supported transport limit/i);
      expect(() => calculateChunkCapacity(1024)).toThrow(/below minimum supported transport limit/i);
    });

    it('should calculate exact capacity at minimum transport floor (2,144 B)', () => {
      // 2144 - 1800 = 344 B Base64 => floor(344 / 4) * 3 = 86 * 3 = 258 B
      const capacity = calculateChunkCapacity(2144);
      expect(capacity).toBe(258);
      expect(capacity).toBeGreaterThanOrEqual(MIN_RAW_CHUNK_BYTES);
    });

    it('should calculate exact capacity at 4,096 B (4 KiB)', () => {
      // 4096 - 1800 = 2296 B Base64 => floor(2296 / 4) * 3 = 574 * 3 = 1722 B
      expect(calculateChunkCapacity(4096)).toBe(1722);
    });

    it('should calculate exact capacity at 8,192 B (8 KiB)', () => {
      // 8192 - 1800 = 6392 B Base64 => floor(6392 / 4) * 3 = 1598 * 3 = 4794 B
      expect(calculateChunkCapacity(8192)).toBe(4794);
    });

    it('should calculate exact capacity at 16,384 B (16 KiB)', () => {
      // 16384 - 1800 = 14584 B Base64 => floor(14584 / 4) * 3 = 3646 * 3 = 10938 B
      expect(calculateChunkCapacity(16384)).toBe(10938);
    });

    it('should calculate exact capacity at 32,768 B (32 KiB)', () => {
      // 32768 - 1800 = 30968 B Base64 => floor(30968 / 4) * 3 = 7742 * 3 = 23226 B
      expect(calculateChunkCapacity(32768)).toBe(23226);
    });

    it('should achieve full 32 KB chunk capacity at threshold (45,492 B)', () => {
      // 45492 - 1800 = 43692 B Base64 => floor(43692 / 4) * 3 = 10923 * 3 = 32769 => capped at 32768
      expect(calculateChunkCapacity(45492)).toBe(MAX_CHUNK_BYTES);
    });

    it('should remain capped at 32 KB chunk capacity at 65,536 B (64 KiB)', () => {
      expect(calculateChunkCapacity(65536)).toBe(MAX_CHUNK_BYTES);
    });
  });

  describe('calculateSnapshotAdmission() — Boundaries and Invariants', () => {
    it('should reject transport below 2,144 B (e.g. 2,143 B)', () => {
      const result = calculateSnapshotAdmission(1000, 2143);
      expect(result.eligible).toBe(false);
      expect(result.reason).toBe('TRANSPORT_LIMIT_EXCEEDED');
      expect(result.requiredChunks).toBe(0);
      expect(result.chunkBinaryCapacity).toBe(0);
    });

    it('should reject unnegotiated or unavailable SCTP limit without fallback option', () => {
      const resultUndefined = calculateSnapshotAdmission(1000, undefined);
      expect(resultUndefined.eligible).toBe(false);
      expect(resultUndefined.reason).toBe('TRANSPORT_LIMIT_EXCEEDED');

      const resultZero = calculateSnapshotAdmission(1000, 0);
      expect(resultZero.eligible).toBe(false);
      expect(resultZero.reason).toBe('TRANSPORT_LIMIT_EXCEEDED');

      const resultNegative = calculateSnapshotAdmission(1000, -1);
      expect(resultNegative.eligible).toBe(false);
      expect(resultNegative.reason).toBe('TRANSPORT_LIMIT_EXCEEDED');
    });

    it('should permit fallback ONLY when allowSctpFallback: true is explicitly provided', () => {
      const result = calculateSnapshotAdmission(1000, undefined, undefined, {
        allowSctpFallback: true,
      });
      expect(result.eligible).toBe(true);
      expect(result.effectiveLimit).toBe(65536);
      expect(result.chunkBinaryCapacity).toBe(MAX_CHUNK_BYTES);
    });

    it('should admit small snapshot at minimum transport floor (2,144 B)', () => {
      // 258 B capacity. A 500 B snapshot needs ceil(500 / 258) = 2 chunks
      const result = calculateSnapshotAdmission(500, 2144);
      expect(result.eligible).toBe(true);
      expect(result.chunkBinaryCapacity).toBe(258);
      expect(result.requiredChunks).toBe(2);
      expect(result.effectiveLimit).toBe(2144);
    });

    it('should reject a snapshot exceeding 128 chunks on constrained transport (CHUNK_COUNT_EXCEEDED)', () => {
      // On 2,144 B transport, capacity is 258 B. Max snapshot for 128 chunks is 128 * 258 = 33,024 B.
      // A 40,000 B snapshot requires ceil(40000 / 258) = 156 chunks > 128.
      const result = calculateSnapshotAdmission(40000, 2144);
      expect(result.eligible).toBe(false);
      expect(result.reason).toBe('CHUNK_COUNT_EXCEEDED');
      expect(result.requiredChunks).toBe(156);
    });

    it('should reject 4 MiB snapshot on 2,144 B transport before buffer allocation', () => {
      const result = calculateSnapshotAdmission(MAX_SNAPSHOT_BYTES, 2144);
      expect(result.eligible).toBe(false);
      expect(result.reason).toBe('CHUNK_COUNT_EXCEEDED');
      // 4,194,304 / 258 = 16,257 chunks >> 128
      expect(result.requiredChunks).toBe(16257);
    });

    it('should admit full 4 MiB snapshot at threshold transport (45,492 B)', () => {
      // At 45,492 B, capacity is full 32,768 B.
      // 4,194,304 / 32,768 = exactly 128 chunks.
      const result = calculateSnapshotAdmission(MAX_SNAPSHOT_BYTES, 45492);
      expect(result.eligible).toBe(true);
      expect(result.chunkBinaryCapacity).toBe(MAX_CHUNK_BYTES);
      expect(result.requiredChunks).toBe(MAX_CHUNKS_PER_SNAPSHOT);
    });

    it('should admit full 4 MiB snapshot at baseline 65,536 B transport', () => {
      const result = calculateSnapshotAdmission(MAX_SNAPSHOT_BYTES, 65536);
      expect(result.eligible).toBe(true);
      expect(result.chunkBinaryCapacity).toBe(MAX_CHUNK_BYTES);
      expect(result.requiredChunks).toBe(MAX_CHUNKS_PER_SNAPSHOT);
    });

    it('should reject when advertisedTotalChunks differs from calculatedRequiredChunks (OFFER_INCONSISTENT)', () => {
      // For 10,000 B snapshot on 65,536 B transport, requiredChunks is ceil(10000 / 32768) = 1.
      // If advertised is 2, it must be rejected upfront.
      const result = calculateSnapshotAdmission(10000, 65536, 2);
      expect(result.eligible).toBe(false);
      expect(result.reason).toBe('OFFER_INCONSISTENT');
      expect(result.requiredChunks).toBe(1);
    });

    it('should accept when advertisedTotalChunks matches calculatedRequiredChunks', () => {
      const result = calculateSnapshotAdmission(10000, 65536, 1);
      expect(result.eligible).toBe(true);
      expect(result.requiredChunks).toBe(1);
    });

    it('should reject snapshot size exceeding MAX_SNAPSHOT_BYTES (4 MiB)', () => {
      const result = calculateSnapshotAdmission(MAX_SNAPSHOT_BYTES + 1, 65536);
      expect(result.eligible).toBe(false);
      expect(result.reason).toBe('LOCAL_ABORT');
    });

    it('should reject non-positive snapshot size (0 or negative)', () => {
      const resultZero = calculateSnapshotAdmission(0, 65536);
      expect(resultZero.eligible).toBe(false);
      expect(resultZero.reason).toBe('LOCAL_ABORT');

      const resultNeg = calculateSnapshotAdmission(-10, 65536);
      expect(resultNeg.eligible).toBe(false);
      expect(resultNeg.reason).toBe('LOCAL_ABORT');
    });
  });
});
