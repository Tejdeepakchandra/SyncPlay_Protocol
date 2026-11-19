/**
 * RepairSupplierService implementation.
 *
 * Implements Phase 7.4 specifications:
 * - PHASE7_4_ARCHITECTURE.md §3 & §4
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §1 & §2
 * - PHASE7_4_ACCEPTANCE_CRITERIA.md AC-6, AC-7 & AC-8
 *
 * Responsibilities:
 * 1. Validates inbound REPAIR_REQUEST messages and enforces capability-gated transport provenance.
 * 2. Authenticates requesting peer identity and active connection generation.
 * 3. Bounded concurrency tracking and per-peer token bucket rate limiting.
 * 4. Bounded recent response cache for deterministic, zero-allocation duplicate request handling.
 * 5. Authoritative verification of actual retained history in OrderedOperationLog:
 *    - Rejects with OUTSIDE_HORIZON (with retainedHorizonSequence) when compacted to snapshot or pre-horizon.
 *    - Rejects with ORIGIN_UNKNOWN when origin has never been observed.
 *    - Rejects with EPOCH_MISMATCH when request or operation belongs to a different epoch.
 *    - Rejects with BUSY when log lacks contiguous range, hole exists, or capacity full.
 * 6. Constructs bounded OPERATION_BATCH frames:
 *    - Enforces 32-operation ceiling.
 *    - Enforces 64 KB (65,536 bytes) WebRTC DataChannel message size limit.
 *    - Slices batches into smaller contiguous sub-ranges [fromSeq, fromSeq + M - 1] if frame exceeds cap.
 *    - Rejects with BUSY if even a single operation exceeds frame cap.
 *    - Never mutates playback state or invents missing operations.
 */

import {
  type RepairRequestPayload,
  type OperationBatchPayload,
  type RepairRejectedPayload,
  type SerializedControlOperation,
  RepairRequestPayloadSchema,
  OperationBatchPayloadSchema,
  RepairRejectedPayloadSchema,
} from '@syncplay/protocol';
import type {
  LeaderlessSyncEngine,
  AuthenticatedRequesterContext,
} from '../engine/leaderless-sync-engine.js';
import type { StoredOperation } from '../log/types.js';
import { SyncEngineError } from '../errors.js';
import type {
  RepairSupplierOptions,
  AntiEntropyCallbacks,
  SchedulerClock,
} from './types.js';
import { defaultSchedulerClock } from './anti-entropy-manager.js';

export const DEFAULT_SUPPLIER_MAX_CONCURRENT_REQUESTS = 16;
export const DEFAULT_SUPPLIER_MAX_CONCURRENT_PER_PEER = 4;
export const DEFAULT_SUPPLIER_RATE_LIMIT_PER_SEC = 10;
export const DEFAULT_SUPPLIER_RATE_LIMIT_BURST = 20;
export const DEFAULT_SUPPLIER_RESPONSE_CACHE_CAPACITY = 64;
export const DEFAULT_SUPPLIER_MAX_OPS_PER_BATCH = 32;
export const DEFAULT_SUPPLIER_MAX_PAYLOAD_BYTES = 65536;
export const DEFAULT_SUPPLIER_ENVELOPE_OVERHEAD_BYTES = 256;

interface CachedResponse {
  readonly response: OperationBatchPayload | RepairRejectedPayload;
  readonly epoch: number;
  readonly targetOriginPeerId: string;
  readonly fromSequence: number;
  readonly toSequence: number;
  readonly sessionId: string;
}

interface PeerRateLimitState {
  tokens: number;
  lastRefillAt: number;
}

export class RepairSupplierService {
  private readonly engine: LeaderlessSyncEngine;
  private readonly callbacks: AntiEntropyCallbacks;
  private readonly options: RepairSupplierOptions;
  private readonly clock: SchedulerClock;

  // Bounded recent response cache: key = `${requesterPeerId}:${requestId}`
  private readonly responseCache = new Map<string, CachedResponse>();

  // Rate limiter per requesting peer
  private readonly peerRateLimits = new Map<string, PeerRateLimitState>();

  // Active concurrent requests tracking
  private activeConcurrentRequests = 0;
  private readonly activeRequestsPerPeer = new Map<string, number>();

  private disposed = false;

  constructor(
    engine: LeaderlessSyncEngine,
    callbacks?: AntiEntropyCallbacks,
    options?: RepairSupplierOptions,
    clock?: SchedulerClock,
  ) {
    this.engine = engine;
    this.callbacks = callbacks ?? { onBroadcastDigest: () => {} };
    this.options = options ?? {};
    this.clock = clock ?? defaultSchedulerClock;
  }

  public get isDisposed(): boolean {
    return this.disposed;
  }

  /**
   * Serves an inbound REPAIR_REQUEST strictly from authoritative local retained log history.
   *
   * @param request The unverified repair request payload.
   * @param context Mandatory AuthenticatedRequesterContext registered via capability reference equality.
   * @returns Fulfilling OperationBatchPayload or appropriate RepairRejectedPayload.
   */
  public serveRepairRequest(
    request: RepairRequestPayload,
    context?: AuthenticatedRequesterContext,
  ): OperationBatchPayload | RepairRejectedPayload {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot serve repair request on disposed service');
    }

    // 1. Mandatory Transport Provenance & Capability Verification
    if (!context || !this.engine.isValidRequesterContext(context)) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        'Inbound REPAIR_REQUEST strictly requires a registered AuthenticatedRequesterContext',
      );
    }

    // 2. Connection Generation Currency Check
    const activeGen = this.engine.getPeerGeneration(context.requesterPeerId);
    if (
      context.connectionGeneration === undefined ||
      activeGen === undefined ||
      context.connectionGeneration !== activeGen
    ) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        `Requester connection generation ${context.connectionGeneration} does not match active engine generation ${activeGen}`,
      );
    }

    // 3. Schema & Boundary Validation
    const parseResult = RepairRequestPayloadSchema.safeParse(request);
    if (!parseResult.success) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Malformed RepairRequestPayload schema: ${parseResult.error.message}`,
      );
    }

    // Integer and Range Safety
    const maxOps = this.options.maxOperationsPerBatch ?? DEFAULT_SUPPLIER_MAX_OPS_PER_BATCH;
    if (
      !Number.isSafeInteger(request.fromSequence) ||
      !Number.isSafeInteger(request.toSequence) ||
      request.fromSequence < 1 ||
      request.toSequence < request.fromSequence ||
      request.toSequence - request.fromSequence + 1 > maxOps
    ) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid sequence range ${request.fromSequence}..${request.toSequence}: out of bounds or unsafe integers`,
      );
    }

    // Session Scoping Verification
    if (request.sessionId !== this.engine.sessionId) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Session mismatch: request session ${request.sessionId} does not match active engine session ${this.engine.sessionId}`,
      );
    }

    // Transport Identity Matching: payload requesterPeerId must match authenticated transport peer
    if (request.requesterPeerId !== context.requesterPeerId) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        `Payload requesterPeerId '${request.requesterPeerId}' does not match authenticated transport peer '${context.requesterPeerId}'`,
      );
    }

    const targetOrigin = request.targetOriginPeerId || request.originPeerId!;

    // 4. Membership Epoch Check
    if (request.membershipEpoch !== this.engine.epoch) {
      const rejection: RepairRejectedPayload = {
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'EPOCH_MISMATCH',
      };
      return RepairRejectedPayloadSchema.parse(rejection);
    }

    // 5. Duplicate Request ID & Idempotent Response Cache Check
    const cacheKey = `${context.requesterPeerId}:${request.requestId}`;
    const cached = this.responseCache.get(cacheKey);
    if (cached) {
      if (cached.epoch !== this.engine.epoch) {
        // Purge stale epoch cache entry
        this.responseCache.delete(cacheKey);
      } else {
        // Conflicting parameter check: A reused requestId with different parameters is invalid
        if (
          cached.targetOriginPeerId !== targetOrigin ||
          cached.fromSequence !== request.fromSequence ||
          cached.toSequence !== request.toSequence ||
          cached.sessionId !== request.sessionId
        ) {
          throw new SyncEngineError(
            'INVALID_ARGUMENT',
            `Reused requestId '${request.requestId}' with conflicting request parameters`,
          );
        }

        // Duplicate replay rate-limiting check:
        // Cache hits MUST consume rate-limit budget to prevent high-rate amplification attacks.
        if (!this.consumeRateLimitToken(context.requesterPeerId)) {
          const rejection: RepairRejectedPayload = {
            requestId: request.requestId,
            sessionId: request.sessionId,
            supplierPeerId: this.engine.localPeerId,
            targetOriginPeerId: targetOrigin,
            fromSequence: request.fromSequence,
            toSequence: request.toSequence,
            reason: 'RATE_LIMITED',
          };
          return RepairRejectedPayloadSchema.parse(rejection);
        }

        // Retransmit cached response via outbound callback and return
        this.callbacks.onSendRepairResponse?.(context.requesterPeerId, cached.response);
        return cached.response;
      }
    }

    // 6. Concurrency Bounds Check
    const maxConcurrent =
      this.options.maxConcurrentRequests ?? DEFAULT_SUPPLIER_MAX_CONCURRENT_REQUESTS;
    const maxConcurrentPerPeer =
      this.options.maxConcurrentRequestsPerPeer ?? DEFAULT_SUPPLIER_MAX_CONCURRENT_PER_PEER;
    const currentPeerActive = this.activeRequestsPerPeer.get(context.requesterPeerId) ?? 0;

    if (
      this.activeConcurrentRequests >= maxConcurrent ||
      currentPeerActive >= maxConcurrentPerPeer
    ) {
      const rejection: RepairRejectedPayload = {
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'BUSY',
      };
      return RepairRejectedPayloadSchema.parse(rejection);
    }

    // 7. Rate Limiting Check
    if (!this.consumeRateLimitToken(context.requesterPeerId)) {
      const rejection: RepairRejectedPayload = {
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'RATE_LIMITED',
      };
      return RepairRejectedPayloadSchema.parse(rejection);
    }

    // Track active concurrency
    this.activeConcurrentRequests++;
    this.activeRequestsPerPeer.set(context.requesterPeerId, currentPeerActive + 1);

    try {
      const response = this.queryRetainedHistoryAndBuildResponse(request, context, targetOrigin);

      // Re-verify epoch and connection generation currency before caching and delivery
      if (request.membershipEpoch !== this.engine.epoch) {
        const rejection: RepairRejectedPayload = {
          requestId: request.requestId,
          sessionId: request.sessionId,
          supplierPeerId: this.engine.localPeerId,
          targetOriginPeerId: targetOrigin,
          fromSequence: request.fromSequence,
          toSequence: request.toSequence,
          reason: 'EPOCH_MISMATCH',
        };
        return RepairRejectedPayloadSchema.parse(rejection);
      }

      const currentGen = this.engine.getPeerGeneration(context.requesterPeerId);
      if (
        context.connectionGeneration === undefined ||
        currentGen === undefined ||
        context.connectionGeneration !== currentGen
      ) {
        throw new SyncEngineError(
          'UNAUTHORIZED',
          `Requester connection generation changed during servicing`,
        );
      }

      this.cacheResponse(context.requesterPeerId, request, response);
      this.callbacks.onSendRepairResponse?.(context.requesterPeerId, response);
      return response;
    } finally {
      this.activeConcurrentRequests = Math.max(0, this.activeConcurrentRequests - 1);
      const updatedPeerActive = (this.activeRequestsPerPeer.get(context.requesterPeerId) ?? 1) - 1;
      if (updatedPeerActive <= 0) {
        this.activeRequestsPerPeer.delete(context.requesterPeerId);
      } else {
        this.activeRequestsPerPeer.set(context.requesterPeerId, updatedPeerActive);
      }
    }
  }

  /**
   * Queries actual OrderedOperationLog and builds verified OPERATION_BATCH or REPAIR_REJECTED.
   */
  private queryRetainedHistoryAndBuildResponse(
    request: RepairRequestPayload,
    context: AuthenticatedRequesterContext,
    targetOrigin: string,
  ): OperationBatchPayload | RepairRejectedPayload {
    const log = this.engine.getOperationLog();
    const baseSnapshotFrontier = log.getBaseSnapshotFrontier ? log.getBaseSnapshotFrontier() : {};
    const replicationFrontier = this.engine.replicationFrontier;
    const oldestRetained = log.getOldestRetainedSequence
      ? log.getOldestRetainedSequence(targetOrigin)
      : undefined;

    const baseSeq = baseSnapshotFrontier[targetOrigin] ?? 0;
    const logFrontier = log.getReplicationFrontier ? log.getReplicationFrontier() : {};
    const replSeq = Math.max(replicationFrontier[targetOrigin] ?? 0, logFrontier[targetOrigin] ?? 0);

    // A. Check if origin is known
    const isOriginKnown =
      replSeq > 0 ||
      baseSeq > 0 ||
      oldestRetained !== undefined ||
      log.getOperationAt(targetOrigin, 1) !== undefined;

    if (!isOriginKnown) {
      return RepairRejectedPayloadSchema.parse({
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'ORIGIN_UNKNOWN',
      });
    }

    // B. Check if requested range is compacted into base snapshot or predates active log horizon
    if (baseSeq > 0 && request.fromSequence <= baseSeq) {
      const retainedHorizon = oldestRetained ?? baseSeq + 1;
      return RepairRejectedPayloadSchema.parse({
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: retainedHorizon,
      });
    }

    if (oldestRetained !== undefined && request.fromSequence < oldestRetained) {
      return RepairRejectedPayloadSchema.parse({
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: oldestRetained,
      });
    }

    // C. Check that the entire contiguous range [fromSequence..toSequence] is genuinely available in the log
    const storedOps: StoredOperation[] = [];
    for (let seq = request.fromSequence; seq <= request.toSequence; seq++) {
      const op = log.getOperationAt(targetOrigin, seq);
      if (!op) {
        // Missing sequence! Supplier does not have complete range.
        // If seq <= baseSeq, it's outside horizon
        if (seq <= baseSeq) {
          const retainedHorizon = oldestRetained ?? baseSeq + 1;
          return RepairRejectedPayloadSchema.parse({
            requestId: request.requestId,
            sessionId: request.sessionId,
            supplierPeerId: this.engine.localPeerId,
            targetOriginPeerId: targetOrigin,
            fromSequence: request.fromSequence,
            toSequence: request.toSequence,
            reason: 'OUTSIDE_HORIZON',
            retainedHorizonSequence: retainedHorizon,
          });
        }
        // Middle sequence missing, last sequence missing, or beyond supplier's frontier:
        // Supplier cannot serve complete range; must reject with BUSY rather than fabricating or partial.
        return RepairRejectedPayloadSchema.parse({
          requestId: request.requestId,
          sessionId: request.sessionId,
          supplierPeerId: this.engine.localPeerId,
          targetOriginPeerId: targetOrigin,
          fromSequence: request.fromSequence,
          toSequence: request.toSequence,
          reason: 'BUSY',
        });
      }

      if (op.originPeerId !== targetOrigin) {
        return RepairRejectedPayloadSchema.parse({
          requestId: request.requestId,
          sessionId: request.sessionId,
          supplierPeerId: this.engine.localPeerId,
          targetOriginPeerId: targetOrigin,
          fromSequence: request.fromSequence,
          toSequence: request.toSequence,
          reason: 'BUSY',
        });
      }

      if (op.membershipEpoch !== request.membershipEpoch) {
        return RepairRejectedPayloadSchema.parse({
          requestId: request.requestId,
          sessionId: request.sessionId,
          supplierPeerId: this.engine.localPeerId,
          targetOriginPeerId: targetOrigin,
          fromSequence: request.fromSequence,
          toSequence: request.toSequence,
          reason: 'EPOCH_MISMATCH',
        });
      }

      storedOps.push(op);
    }

    // D. Extract exact stored operations and map to wire format
    const serializedOps: SerializedControlOperation[] = storedOps.map((op) => ({
      logicalEventId: op.logicalEventId,
      originPeerId: op.originPeerId,
      membershipEpoch: op.membershipEpoch,
      messageType: op.messageType,
      logicalTimestamp: op.logicalTimestamp,
      originSequenceNumber: op.originSequenceNumber,
      causalContext: { ...op.causalContext },
      payload: op.payload,
    }));

    // E. Enforce WebRTC DataChannel / SCTP Frame Size Cap (64 KB) and Envelope Overhead
    const negotiatedSize = this.options.negotiatedSctpMaxMessageSizeProvider?.(
      context.requesterPeerId,
    );
    // RFC 8841 / W3C WebRTC §6.2:
    // - undefined: no negotiated size available -> fallback to application cap (65,536 bytes)
    // - 0: remote peer can handle any message size -> unconstrained remote, clamped to local application cap (65,536 bytes)
    // - > 0: remote peer announced explicit limit -> clamp to min(application cap, negotiatedSize)
    const applicationCap = this.options.maxPayloadBytes ?? DEFAULT_SUPPLIER_MAX_PAYLOAD_BYTES;
    const effectiveTransportLimit =
      negotiatedSize !== undefined && negotiatedSize > 0
        ? Math.min(applicationCap, negotiatedSize)
        : applicationCap;

    const envelopeOverhead =
      this.options.envelopeOverheadBytes !== undefined
        ? this.options.envelopeOverheadBytes
        : (effectiveTransportLimit > DEFAULT_SUPPLIER_ENVELOPE_OVERHEAD_BYTES
            ? DEFAULT_SUPPLIER_ENVELOPE_OVERHEAD_BYTES
            : 0);

    const maxBytes = Math.max(1, effectiveTransportLimit - envelopeOverhead);

    let currentOps = serializedOps;
    let currentToSeq = request.toSequence;

    let candidateBatch: OperationBatchPayload = {
      requestId: request.requestId,
      sessionId: request.sessionId,
      supplierPeerId: this.engine.localPeerId,
      originPeerId: targetOrigin,
      fromSequence: request.fromSequence,
      toSequence: currentToSeq,
      membershipEpoch: request.membershipEpoch,
      operations: currentOps,
    };

    let serializedJson = JSON.stringify(candidateBatch);
    let byteLength = new TextEncoder().encode(serializedJson).byteLength;

    if (byteLength > maxBytes) {
      // Slicing: Slice batch into smaller contiguous sub-range [fromSequence, fromSequence + M - 1]
      while (currentOps.length > 1 && byteLength > maxBytes) {
        currentOps = currentOps.slice(0, currentOps.length - 1);
        currentToSeq = currentOps[currentOps.length - 1]!.originSequenceNumber;
        candidateBatch = {
          ...candidateBatch,
          toSequence: currentToSeq,
          operations: currentOps,
        };
        serializedJson = JSON.stringify(candidateBatch);
        byteLength = new TextEncoder().encode(serializedJson).byteLength;
      }
    }

    if (byteLength > maxBytes) {
      // Even 1 operation exceeds the max frame size! Cannot fit into permitted payload size.
      // Follow approved architecture's bounded rejection behavior.
      return RepairRejectedPayloadSchema.parse({
        requestId: request.requestId,
        sessionId: request.sessionId,
        supplierPeerId: this.engine.localPeerId,
        targetOriginPeerId: targetOrigin,
        fromSequence: request.fromSequence,
        toSequence: request.toSequence,
        reason: 'BUSY',
      });
    }

    // Guarantee that wire contracts and contiguous sequence refinements are 100% satisfied
    return OperationBatchPayloadSchema.parse(candidateBatch);
  }

  /**
   * Consumes a token from the requester peer's token bucket rate limiter.
   */
  private consumeRateLimitToken(peerId: string): boolean {
    const maxPerSec =
      this.options.rateLimitMaxRequestsPerSecond ?? DEFAULT_SUPPLIER_RATE_LIMIT_PER_SEC;
    const burst = this.options.rateLimitBurst ?? DEFAULT_SUPPLIER_RATE_LIMIT_BURST;
    const now = this.clock.now();

    let state = this.peerRateLimits.get(peerId);
    if (!state) {
      state = { tokens: burst, lastRefillAt: now };
      this.peerRateLimits.set(peerId, state);
    } else {
      const elapsedMs = Math.max(0, now - state.lastRefillAt);
      const refillTokens = (elapsedMs / 1000) * maxPerSec;
      state.tokens = Math.min(burst, state.tokens + refillTokens);
      state.lastRefillAt = now;
    }

    if (state.tokens < 1) {
      return false;
    }

    state.tokens -= 1;
    return true;
  }

  /**
   * Caches a response in the bounded response cache (FIFO/LRU eviction).
   */
  private cacheResponse(
    requesterPeerId: string,
    request: RepairRequestPayload,
    response: OperationBatchPayload | RepairRejectedPayload,
  ): void {
    const capacity =
      this.options.responseCacheCapacity ?? DEFAULT_SUPPLIER_RESPONSE_CACHE_CAPACITY;
    const cacheKey = `${requesterPeerId}:${request.requestId}`;

    if (this.responseCache.size >= capacity) {
      const oldestKey = this.responseCache.keys().next().value;
      if (oldestKey) {
        this.responseCache.delete(oldestKey);
      }
    }

    const targetOrigin = request.targetOriginPeerId || request.originPeerId!;
    this.responseCache.set(cacheKey, {
      response,
      epoch: this.engine.epoch,
      targetOriginPeerId: targetOrigin,
      fromSequence: request.fromSequence,
      toSequence: request.toSequence,
      sessionId: request.sessionId,
    });
  }

  /**
   * Handles peer disconnection: cleans up rate limiter and active tracking state for that peer.
   */
  public onPeerDisconnected(peerId: string): void {
    if (this.disposed) return;
    this.peerRateLimits.delete(peerId);
    this.activeRequestsPerPeer.delete(peerId);

    // Evict cached responses for this peer
    for (const key of this.responseCache.keys()) {
      if (key.startsWith(`${peerId}:`)) {
        this.responseCache.delete(key);
      }
    }
  }

  /**
   * Handles membership epoch transition: purges cached responses from older epochs.
   */
  public onEpochTransition(_newEpoch: number): void {
    if (this.disposed) return;
    this.responseCache.clear();
    this.activeConcurrentRequests = 0;
    this.activeRequestsPerPeer.clear();
  }

  /**
   * Disposes the supplier service.
   */
  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.responseCache.clear();
    this.peerRateLimits.clear();
    this.activeRequestsPerPeer.clear();
    this.activeConcurrentRequests = 0;
  }
}
