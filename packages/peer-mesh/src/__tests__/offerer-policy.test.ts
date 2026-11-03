/**
 * Tests for the deterministic offerer policy.
 * Per PEER_MESH_RTT_ENGINE_LLD §3:
 * "Only the designated initiator creates the initial offer for each peer pair."
 */
import { describe, it, expect } from 'vitest';
import { isInitialOfferer, shouldLocalOfferWinGlare } from '../offerer-policy.js';

describe('isInitialOfferer', () => {
  it('should return true when localPeerId < remotePeerId', () => {
    expect(isInitialOfferer('alice', 'bob')).toBe(true);
  });

  it('should return false when localPeerId > remotePeerId', () => {
    expect(isInitialOfferer('bob', 'alice')).toBe(false);
  });

  it('should be deterministic — same result regardless of who calls', () => {
    const peerA = 'peer-aaa';
    const peerB = 'peer-zzz';
    // Exactly one of them should be the offerer
    expect(isInitialOfferer(peerA, peerB)).toBe(true);
    expect(isInitialOfferer(peerB, peerA)).toBe(false);
  });

  it('should throw for self-connection', () => {
    expect(() => isInitialOfferer('peer-1', 'peer-1')).toThrow(
      'Cannot determine offerer for self-connection',
    );
  });

  it('should handle UUIDs correctly', () => {
    const id1 = '00000000-0000-0000-0000-000000000001';
    const id2 = 'ffffffff-ffff-ffff-ffff-ffffffffffff';
    expect(isInitialOfferer(id1, id2)).toBe(true);
    expect(isInitialOfferer(id2, id1)).toBe(false);
  });

  it('should handle single-character differences', () => {
    expect(isInitialOfferer('a', 'b')).toBe(true);
    expect(isInitialOfferer('b', 'a')).toBe(false);
  });

  it('should be antisymmetric — exactly one peer is offerer', () => {
    const pairs = [
      ['x', 'y'],
      ['peer-1', 'peer-2'],
      ['abc', 'abd'],
    ] as const;
    for (const [a, b] of pairs) {
      const aIsOfferer = isInitialOfferer(a, b);
      const bIsOfferer = isInitialOfferer(b, a);
      expect(aIsOfferer).not.toBe(bIsOfferer);
    }
  });
});

describe('shouldLocalOfferWinGlare', () => {
  it('should return true if local is the designated offerer', () => {
    expect(shouldLocalOfferWinGlare('alice', 'bob')).toBe(true);
  });

  it('should return false if local is NOT the designated offerer', () => {
    expect(shouldLocalOfferWinGlare('bob', 'alice')).toBe(false);
  });

  it('should match isInitialOfferer for all pairs', () => {
    const pairs = [
      ['a', 'b'],
      ['peer-1', 'peer-2'],
      ['zzz', 'aaa'],
    ] as const;
    for (const [a, b] of pairs) {
      expect(shouldLocalOfferWinGlare(a, b)).toBe(isInitialOfferer(a, b));
    }
  });
});

