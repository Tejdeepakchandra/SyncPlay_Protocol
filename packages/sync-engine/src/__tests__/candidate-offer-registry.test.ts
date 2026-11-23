import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { SnapshotOfferPayload } from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import {
  createInboundTransportCapability,
  assertInboundTransportCapability,
} from '../recovery/inbound-custody.js';
import {
  CandidateOfferRegistry,
  MAX_OFFER_CANDIDATES,
  CANDIDATE_OFFER_TTL_MS,
} from '../recovery/candidate-offer-registry.js';
import type { AuthenticatedSupplierContext } from '../engine/leaderless-sync-engine.js';

function createMockOffer(overrides: Partial<SnapshotOfferPayload> = {}): SnapshotOfferPayload {
  return {
    offerId: '00000000-0000-4000-8000-000000000001',
    sessionId: 'session-1',
    membershipEpoch: 1,
    supplierPeerId: 'peer-s1',
    snapshotId: 'snap-1',
    authorPeerId: 'peer-author',
    snapshotFrontier: { 'peer-author': 10 },
    maxLogicalTimestamp: 10,
    totalSizeBytes: 65536,
    totalChunks: 2,
    nominalChunkSizeBytes: 32768,
    sha256Digest: 'a'.repeat(64),
    ...overrides,
  };
}

function createMockSupplierContext(
  supplierPeerId: string,
  connectionGeneration = 1,
): AuthenticatedSupplierContext {
  return Object.freeze({
    supplierPeerId,
    connectionGeneration,
  });
}

describe('Inbound Custody & Capability Branding', () => {
  it('mints genuine InboundTransportCapability with brand', () => {
    const cap = createInboundTransportCapability('session-123', 'custom-authority');
    expect(cap.sessionId).toBe('session-123');
    expect(cap.authority).toBe('custom-authority');
    expect(Object.isFrozen(cap)).toBe(true);

    expect(() => assertInboundTransportCapability(cap)).not.toThrow();
    expect(() => assertInboundTransportCapability(cap, 'session-123')).not.toThrow();
  });

  it('rejects missing or non-object capability', () => {
    try {
      assertInboundTransportCapability(null);
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }

    try {
      assertInboundTransportCapability(undefined);
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }

    try {
      assertInboundTransportCapability('fake');
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }
  });

  it('rejects fabricated capability without private symbol', () => {
    const fake = { sessionId: 'session-123', authority: 'trusted-transport' };
    try {
      assertInboundTransportCapability(fake);
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }
  });

  it('rejects capability forged with a new Symbol', () => {
    const forged = {
      [Symbol('INBOUND_CAPABILITY_BRAND')]: true,
      sessionId: 'session-123',
    };
    try {
      assertInboundTransportCapability(forged);
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }
  });

  it('rejects capability when expected sessionId does not match', () => {
    const cap = createInboundTransportCapability('session-AAA');
    try {
      assertInboundTransportCapability(cap, 'session-BBB');
      expect.fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(SyncEngineError);
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      expect((err as SyncEngineError).message).toContain('does not match expected sessionId');
    }
  });
});

describe('CandidateOfferRegistry', () => {
  let simulatedTime: number;
  let registry: CandidateOfferRegistry;

  beforeEach(() => {
    vi.useFakeTimers();
    simulatedTime = 100000;
    registry = new CandidateOfferRegistry(() => simulatedTime);
  });

  afterEach(() => {
    registry.dispose();
    vi.useRealTimers();
  });

  it('adds and retrieves a candidate offer', () => {
    const offer = createMockOffer();
    const ctx = createMockSupplierContext('peer-1', 1);

    const result = registry.addCandidate(offer, ctx, simulatedTime);
    expect(result.admitted).toBe(true);
    expect(registry.size).toBe(1);

    const query = {
      sessionId: offer.sessionId,
      membershipEpoch: offer.membershipEpoch,
      snapshotId: offer.snapshotId,
      totalSizeBytes: offer.totalSizeBytes,
      totalChunks: offer.totalChunks,
      nominalChunkSizeBytes: offer.nominalChunkSizeBytes,
      expectedSha256Digest: offer.sha256Digest,
    };

    const found = registry.findEligibleCandidate(query, simulatedTime);
    expect(found).not.toBeNull();
    expect(found?.offer.snapshotId).toBe(offer.snapshotId);
    expect(found?.context.supplierPeerId).toBe('peer-1');
  });

  it('enforces maximum capacity of 8 candidates', () => {
    for (let i = 1; i <= MAX_OFFER_CANDIDATES; i++) {
      const offer = createMockOffer({
        supplierPeerId: `peer-${i}`,
        offerId: `00000000-0000-4000-8000-00000000000${i}`,
      });
      const ctx = createMockSupplierContext(`peer-${i}`, 1);
      const res = registry.addCandidate(offer, ctx, simulatedTime);
      expect(res.admitted).toBe(true);
    }
    expect(registry.size).toBe(8);

    // 9th candidate rejected with LOCAL_ABORT / capacity reached
    const overflowOffer = createMockOffer({
      supplierPeerId: 'peer-9',
      offerId: '00000000-0000-4000-8000-000000000009',
    });
    const overflowCtx = createMockSupplierContext('peer-9', 1);
    const overflowRes = registry.addCandidate(overflowOffer, overflowCtx, simulatedTime);
    expect(overflowRes.admitted).toBe(false);
    expect(overflowRes.reason).toBe('LOCAL_ABORT');
    expect(overflowRes.detail).toContain('capacity reached');
    expect(registry.size).toBe(8);
  });

  it('rejects duplicate offer with stale connection generation', () => {
    const offer = createMockOffer();
    const ctx1 = createMockSupplierContext('peer-1', 5);
    registry.addCandidate(offer, ctx1, simulatedTime);

    const staleCtx = createMockSupplierContext('peer-1', 4);
    const res = registry.addCandidate(offer, staleCtx, simulatedTime);
    expect(res.admitted).toBe(false);
    expect(res.reason).toBe('IDENTITY_MISMATCH');
  });

  it('updates context for identical duplicate offer without altering TTL', () => {
    const offer = createMockOffer();
    const ctx1 = createMockSupplierContext('peer-1', 1);
    registry.addCandidate(offer, ctx1, simulatedTime);

    const entry1 = registry.getCandidate('peer-1');
    const originalExpiresAt = entry1?.expiresAtMs;

    // Advance time by 10s and submit identical offer with newer generation
    simulatedTime += 10000;
    const ctx2 = createMockSupplierContext('peer-1', 2);
    const res = registry.addCandidate(offer, ctx2, simulatedTime);
    expect(res.admitted).toBe(true);

    const entry2 = registry.getCandidate('peer-1');
    expect(entry2?.context.connectionGeneration).toBe(2);
    expect(entry2?.expiresAtMs).toBe(originalExpiresAt);
  });

  it('rejects duplicate offer with conflicting fields with OFFER_INCONSISTENT', () => {
    const offer1 = createMockOffer({ totalSizeBytes: 1000 });
    const ctx1 = createMockSupplierContext('peer-1', 1);
    registry.addCandidate(offer1, ctx1, simulatedTime);

    const offer2 = createMockOffer({ totalSizeBytes: 2000 });
    const ctx2 = createMockSupplierContext('peer-1', 2);
    const res = registry.addCandidate(offer2, ctx2, simulatedTime);
    expect(res.admitted).toBe(false);
    expect(res.reason).toBe('OFFER_INCONSISTENT');
  });

  it('expires candidate offer after 30 seconds TTL', () => {
    const offer = createMockOffer();
    const ctx = createMockSupplierContext('peer-1', 1);
    registry.addCandidate(offer, ctx, simulatedTime);

    expect(registry.size).toBe(1);

    // Fast-forward past 30s
    simulatedTime += CANDIDATE_OFFER_TTL_MS + 100;
    vi.advanceTimersByTime(CANDIDATE_OFFER_TTL_MS + 100);

    expect(registry.size).toBe(0);

    const query = {
      sessionId: offer.sessionId,
      membershipEpoch: offer.membershipEpoch,
      snapshotId: offer.snapshotId,
      totalSizeBytes: offer.totalSizeBytes,
      totalChunks: offer.totalChunks,
      nominalChunkSizeBytes: offer.nominalChunkSizeBytes,
      expectedSha256Digest: offer.sha256Digest,
    };
    expect(registry.findEligibleCandidate(query, simulatedTime)).toBeNull();
  });

  it('clear() synchronously disarms all timers and clears candidate records', () => {
    for (let i = 1; i <= 3; i++) {
      registry.addCandidate(
        createMockOffer({
          supplierPeerId: `peer-${i}`,
          offerId: `00000000-0000-4000-8000-00000000000${i}`,
        }),
        createMockSupplierContext(`peer-${i}`),
        simulatedTime,
      );
    }
    expect(registry.size).toBe(3);

    registry.clear();
    expect(registry.size).toBe(0);

    // Advancing timers should not throw or cause unexpected behavior
    vi.advanceTimersByTime(CANDIDATE_OFFER_TTL_MS * 2);
    expect(registry.size).toBe(0);
  });
});
