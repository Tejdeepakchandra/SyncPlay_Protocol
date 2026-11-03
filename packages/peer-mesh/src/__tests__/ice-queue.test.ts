/**
 * Tests for ICE candidate queue.
 *
 * Verifies queuing, flushing, bounding, and generation-based discard.
 */
import { describe, it, expect } from 'vitest';
import { IceQueue } from '../ice-queue.js';

function fakeCandidate(id = '1'): RTCIceCandidateInit {
  return { candidate: `candidate:${id}`, sdpMid: '0', sdpMLineIndex: 0 };
}

describe('IceQueue', () => {
  describe('constructor', () => {
    it('should create with default max size', () => {
      const q = new IceQueue();
      expect(q.size).toBe(0);
      expect(q.isRemoteDescriptionSet).toBe(false);
    });

    it('should reject maxSize < 1', () => {
      expect(() => new IceQueue(0)).toThrow('maxSize must be >= 1');
      expect(() => new IceQueue(-1)).toThrow('maxSize must be >= 1');
    });
  });

  describe('enqueue', () => {
    it('should queue candidates before remote description is set', () => {
      const q = new IceQueue();
      const result = q.enqueue(fakeCandidate(), 1);
      expect(result).toBe('queued');
      expect(q.size).toBe(1);
    });

    it('should return "ready" after remote description is set', () => {
      const q = new IceQueue();
      q.flush(); // marks remote description as set
      const result = q.enqueue(fakeCandidate(), 1);
      expect(result).toBe('ready');
      expect(q.size).toBe(0); // not queued
    });

    it('should drop candidates when queue is full', () => {
      const q = new IceQueue(2);
      expect(q.enqueue(fakeCandidate('1'), 1)).toBe('queued');
      expect(q.enqueue(fakeCandidate('2'), 1)).toBe('queued');
      expect(q.enqueue(fakeCandidate('3'), 1)).toBe('dropped');
      expect(q.size).toBe(2);
    });

    it('should respect exact boundary of maxSize', () => {
      const q = new IceQueue(1);
      expect(q.enqueue(fakeCandidate('1'), 1)).toBe('queued');
      expect(q.size).toBe(1);
      expect(q.enqueue(fakeCandidate('2'), 1)).toBe('dropped');
      expect(q.size).toBe(1);
    });
  });

  describe('flush', () => {
    it('should return all queued candidates in order', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      q.enqueue(fakeCandidate('2'), 1);
      q.enqueue(fakeCandidate('3'), 1);

      const flushed = q.flush();
      expect(flushed).toHaveLength(3);
      expect(flushed[0]!.candidate.candidate).toBe('candidate:1');
      expect(flushed[1]!.candidate.candidate).toBe('candidate:2');
      expect(flushed[2]!.candidate.candidate).toBe('candidate:3');
    });

    it('should clear the queue after flush', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate(), 1);
      q.flush();
      expect(q.size).toBe(0);
    });

    it('should mark remote description as set', () => {
      const q = new IceQueue();
      expect(q.isRemoteDescriptionSet).toBe(false);
      q.flush();
      expect(q.isRemoteDescriptionSet).toBe(true);
    });

    it('should return empty array if nothing was queued', () => {
      const q = new IceQueue();
      const flushed = q.flush();
      expect(flushed).toHaveLength(0);
    });

    it('should preserve generation info in flushed candidates', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      q.enqueue(fakeCandidate('2'), 2);
      const flushed = q.flush();
      expect(flushed[0]!.generation).toBe(1);
      expect(flushed[1]!.generation).toBe(2);
    });
  });

  describe('discardForGeneration', () => {
    it('should discard all candidates for a specific generation', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      q.enqueue(fakeCandidate('2'), 2);
      q.enqueue(fakeCandidate('3'), 1);

      const discarded = q.discardForGeneration(1);
      expect(discarded).toBe(2);
      expect(q.size).toBe(1);
    });

    it('should discard all candidates when no generation specified', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      q.enqueue(fakeCandidate('2'), 2);

      const discarded = q.discardForGeneration();
      expect(discarded).toBe(2);
      expect(q.size).toBe(0);
    });

    it('should return 0 when nothing matches', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      const discarded = q.discardForGeneration(99);
      expect(discarded).toBe(0);
      expect(q.size).toBe(1);
    });

    it('should not reset remoteDescriptionSet flag', () => {
      const q = new IceQueue();
      q.flush(); // set remote description
      q.discardForGeneration();
      expect(q.isRemoteDescriptionSet).toBe(true);
    });
  });

  describe('reset', () => {
    it('should clear queue and reset remote description flag', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate(), 1);
      q.flush();
      expect(q.isRemoteDescriptionSet).toBe(true);

      q.reset();
      expect(q.size).toBe(0);
      expect(q.isRemoteDescriptionSet).toBe(false);
    });

    it('should allow re-queuing after reset', () => {
      const q = new IceQueue();
      q.flush();
      expect(q.enqueue(fakeCandidate(), 1)).toBe('ready');

      q.reset();
      expect(q.enqueue(fakeCandidate(), 2)).toBe('queued');
      expect(q.size).toBe(1);
    });
  });

  describe('late ICE candidates (after connection established)', () => {
    it('should return "ready" for candidates after remote desc is set', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('1'), 1);
      q.flush();

      // Late candidate
      expect(q.enqueue(fakeCandidate('late'), 1)).toBe('ready');
      expect(q.size).toBe(0);
    });
  });

  describe('duplicate candidates', () => {
    it('should queue duplicate candidates (dedup is caller responsibility)', () => {
      const q = new IceQueue();
      q.enqueue(fakeCandidate('same'), 1);
      q.enqueue(fakeCandidate('same'), 1);
      expect(q.size).toBe(2);
    });
  });
});

