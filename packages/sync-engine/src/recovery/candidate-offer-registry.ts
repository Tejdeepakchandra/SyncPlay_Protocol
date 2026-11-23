/**
 * @module @syncplay/sync-engine/recovery
 *
 * Candidate Offer Registry for Phase 7.5 M4.2d Snapshot Inbound Pipeline.
 *
 * Manages concurrent candidate snapshot offers:
 * - Bounded capacity: MAX_OFFER_CANDIDATES = 8.
 * - Single unambiguous TTL: exactly 30 seconds from admission.
 * - Encapsulated timer ownership: allocating and clearing candidate TTL timers strictly internally.
 * - Duplicate offer handling: generation currency check, preserves TTL for identical offers, rejects inconsistent.
 * - Synchronous clear() and dispose() lifecycle methods for leak-free, fail-closed terminal abort.
 */

import type { SnapshotOfferPayload, SnapshotAbortReason } from '@syncplay/protocol';
import type { AuthenticatedSupplierContext } from '../engine/leaderless-sync-engine.js';

export const MAX_OFFER_CANDIDATES = 8;
export const CANDIDATE_OFFER_TTL_MS = 30_000;

export interface CandidateOfferEntry {
  offer: Readonly<SnapshotOfferPayload>;
  context: AuthenticatedSupplierContext;
  readonly admittedAtMs: number;
  readonly expiresAtMs: number;
  ttlTimer: ReturnType<typeof setTimeout> | null;
}

export interface CandidateMatchQuery {
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly snapshotId: string;
  readonly totalSizeBytes: number;
  readonly totalChunks: number;
  readonly nominalChunkSizeBytes?: number;
  readonly expectedSha256Digest: string;
}

export interface AddCandidateResult {
  readonly admitted: boolean;
  readonly reason?: SnapshotAbortReason;
  readonly detail?: string;
}

export class CandidateOfferRegistry {
  private readonly candidates = new Map<string, CandidateOfferEntry>();
  private readonly nowFn: () => number;

  constructor(nowFn: () => number = () => Date.now()) {
    this.nowFn = nowFn;
  }

  public get size(): number {
    return this.candidates.size;
  }

  /**
   * Adds or updates a candidate offer.
   */
  public addCandidate(
    offer: SnapshotOfferPayload,
    context: AuthenticatedSupplierContext,
    now: number = this.nowFn(),
  ): AddCandidateResult {
    const peerId = context.supplierPeerId;
    const existing = this.candidates.get(peerId);

    if (existing) {
      // Generation currency check: incoming generation must be >= existing generation
      if (
        context.connectionGeneration !== undefined &&
        existing.context.connectionGeneration !== undefined &&
        context.connectionGeneration < existing.context.connectionGeneration
      ) {
        return {
          admitted: false,
          reason: 'IDENTITY_MISMATCH',
          detail: `Stale connection generation (${context.connectionGeneration} < ${existing.context.connectionGeneration})`,
        };
      }

      // 7-field predicate comparison for consistency
      const isConsistent =
        existing.offer.sessionId === offer.sessionId &&
        existing.offer.membershipEpoch === offer.membershipEpoch &&
        existing.offer.snapshotId === offer.snapshotId &&
        existing.offer.totalSizeBytes === offer.totalSizeBytes &&
        existing.offer.totalChunks === offer.totalChunks &&
        existing.offer.nominalChunkSizeBytes === offer.nominalChunkSizeBytes &&
        existing.offer.sha256Digest === offer.sha256Digest;

      if (!isConsistent) {
        return {
          admitted: false,
          reason: 'OFFER_INCONSISTENT',
          detail: 'Candidate offer fields conflict with previously registered offer for this peer',
        };
      }

      // Identical offer: update context and offer reference, but leave TTL and timer unchanged
      existing.context = context;
      existing.offer = Object.freeze({ ...offer });
      return { admitted: true };
    }

    // Purge expired candidates before enforcing capacity
    this.purgeExpired(now);

    if (this.candidates.size >= MAX_OFFER_CANDIDATES) {
      return {
        admitted: false,
        reason: 'LOCAL_ABORT',
        detail: `Candidate offer registry capacity reached (max ${MAX_OFFER_CANDIDATES})`,
      };
    }

    const admittedAtMs = now;
    const expiresAtMs = now + CANDIDATE_OFFER_TTL_MS;

    const entry: CandidateOfferEntry = {
      offer: Object.freeze({ ...offer }),
      context,
      admittedAtMs,
      expiresAtMs,
      ttlTimer: null,
    };

    const timer = setTimeout(() => {
      this.evict(peerId);
    }, CANDIDATE_OFFER_TTL_MS);

    if (
      typeof timer === 'object' &&
      timer !== null &&
      'unref' in timer &&
      typeof (timer as any).unref === 'function'
    ) {
      (timer as any).unref();
    }

    entry.ttlTimer = timer;
    this.candidates.set(peerId, entry);
    return { admitted: true };
  }

  /**
   * Finds the first eligible candidate matching the 7-field query and not expired.
   */
  public findEligibleCandidate(
    query: CandidateMatchQuery,
    now: number = this.nowFn(),
  ): CandidateOfferEntry | null {
    for (const [peerId, entry] of this.candidates.entries()) {
      // Check expiration
      if (now > entry.expiresAtMs) {
        this.evict(peerId);
        continue;
      }

      // 7-field bit-exact match
      if (
        entry.offer.sessionId === query.sessionId &&
        entry.offer.membershipEpoch === query.membershipEpoch &&
        entry.offer.snapshotId === query.snapshotId &&
        entry.offer.totalSizeBytes === query.totalSizeBytes &&
        entry.offer.totalChunks === query.totalChunks &&
        entry.offer.nominalChunkSizeBytes === query.nominalChunkSizeBytes &&
        entry.offer.sha256Digest === query.expectedSha256Digest
      ) {
        return entry;
      }
    }

    return null;
  }

  /**
   * Evicts a candidate by supplierPeerId and cleanly disarms its timer.
   */
  public evict(peerId: string): boolean {
    const entry = this.candidates.get(peerId);
    if (!entry) return false;

    if (entry.ttlTimer !== null) {
      clearTimeout(entry.ttlTimer);
      entry.ttlTimer = null;
    }
    return this.candidates.delete(peerId);
  }

  public getCandidate(peerId: string): CandidateOfferEntry | undefined {
    return this.candidates.get(peerId);
  }

  public getAllCandidates(): ReadonlyArray<CandidateOfferEntry> {
    return Array.from(this.candidates.values());
  }

  /**
   * Synchronously disarms all candidate TTL timers and clears all candidate offer records.
   * Invoked during pipeline terminal abort (R18-2).
   */
  public clear(): void {
    for (const entry of this.candidates.values()) {
      if (entry.ttlTimer !== null) {
        clearTimeout(entry.ttlTimer);
        entry.ttlTimer = null;
      }
    }
    this.candidates.clear();
  }

  public dispose(): void {
    this.clear();
  }

  private purgeExpired(now: number): void {
    for (const [peerId, entry] of this.candidates.entries()) {
      if (now > entry.expiresAtMs) {
        this.evict(peerId);
      }
    }
  }
}
