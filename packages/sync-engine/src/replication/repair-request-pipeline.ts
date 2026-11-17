/**
 * Requester-Side Repair Request Pipeline (Phase 7.4 Milestone M3).
 *
 * Implements:
 * - PHASE7_4_ARCHITECTURE.md §4 (Missing-Range Detection & Repair Protocol)
 * - PHASE7_4_ARCHITECTURE.md §5 (Repair Validation and Ingestion Pipeline)
 * - PHASE7_4_ARCHITECTURE.md §6 (Anti-Entropy Scheduling & Flow Control)
 * - PHASE7_4_INTERFACE_SPECIFICATION.md §2.1 & §2.2
 * - PHASE7_4_ACCEPTANCE_CRITERIA.md AC-2, AC-4, AC-5, AC-6, AC-7, AC-8, AC-10
 *
 * Responsibilities:
 * 1. Translating comparison results and pending dependency gaps into exact missing sequence ranges.
 * 2. Slicing missing ranges into contiguous chunks of at most 32 operations (MAX_OPERATIONS_PER_BATCH).
 * 3. Generating unique RFC 4122 UUID v4 repair request identifiers.
 * 4. Tracking outstanding requests with 1500ms timeouts and bounded concurrency (max 2/peer, max 16 room-wide).
 * 5. Bounded retries (<= 3) with exponential backoff (1s, 2s, 4s) and automatic supplier failover.
 * 6. Deduplication and overlapping request suppression against local replication frontier.
 * 7. Immediate cancellation of in-flight requests and timers upon membership epoch transition.
 * 8. Handling all 5 REPAIR_REJECTED reasons (OUTSIDE_HORIZON, ORIGIN_UNKNOWN, RATE_LIMITED, EPOCH_MISMATCH, BUSY).
 * 9. Preserving the Phase 7.3-R5-H capability-gated supplier authorization boundary.
 * 10. Ingesting repaired batches through the Phase 7.3 admission pipeline, detecting conflicts, advancing vectors,
 *     and handling partial batch remainder re-dispatch.
 * 11. Preserving the historical completeness invariant: repairing missing ranges never falsely claims
 *     unconditional complete historical identity across unverified intermediate history.
 */

import {
  type RepairRequestPayload,
  type OperationBatchPayload,
  type RepairRejectedPayload,
  type StateDigestPayload,
  MAX_OPERATIONS_PER_BATCH,
} from '@syncplay/protocol';
import type {
  InFlightRepairRequest,
  PeerReplicationKnowledge,
  RepairPipelineOptions,
  RepairPipelineStatus,
  AntiEntropyCallbacks,
  SchedulerClock,
  MissingSequenceRange,
} from './types.js';
import type { LeaderlessSyncEngine, AuthenticatedSupplierContext } from '../engine/leaderless-sync-engine.js';
import type { EngineDecision } from '../types.js';
import { SyncEngineError } from '../errors.js';

export const DEFAULT_REPAIR_REQUEST_TIMEOUT_MS = 1500;
export const MAX_REPAIR_RETRIES = 3;
export const DEFAULT_BACKOFF_BASE_MS = 1000;
export const MAX_CONCURRENT_REQUESTS_PER_PEER = 2;
export const MAX_CONCURRENT_REQUESTS_TOTAL = 16;
export const MAX_OPERATIONS_PER_REPAIR_BATCH = MAX_OPERATIONS_PER_BATCH; // 32

/**
 * Canonical RFC 4122 UUID v4 generator with cryptographic security and fallback.
 */
export function generateUUIDv4(): string {
  if (typeof globalThis !== 'undefined' && globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Internal pending gap task representation.
 */
interface PendingGapTask {
  readonly originPeerId: string;
  fromSequence: number;
  toSequence: number;
  preferredSupplier?: string;
  retryCount: number;
  readonly attemptedSuppliers: Set<string>;
  backoffTimerId?: unknown;
}

export class RepairRequestPipeline {
  private readonly engine: LeaderlessSyncEngine;
  private readonly callbacks: AntiEntropyCallbacks;
  private readonly clock: SchedulerClock;

  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly backoffBaseMs: number;
  private readonly maxConcurrentPerPeer: number;
  private readonly maxConcurrentTotal: number;
  private readonly rttProvider?: (peerId: string) => number | undefined;

  private readonly inFlightRequests = new Map<string, InFlightRepairRequest>();
  private readonly pendingGaps: PendingGapTask[] = [];
  private readonly peerKnowledge = new Map<string, PeerReplicationKnowledge>();

  private totalRepairsCompleted = 0;
  private totalRepairsFailed = 0;
  private degraded = false;
  private degradedReason?: string;
  private disposed = false;

  constructor(
    engine: LeaderlessSyncEngine,
    callbacks: AntiEntropyCallbacks,
    options?: RepairPipelineOptions,
    clock?: SchedulerClock,
  ) {
    this.engine = engine;
    this.callbacks = callbacks;
    this.clock = clock ?? {
      now: () => (typeof performance !== 'undefined' ? performance.now() : Date.now()),
      setTimeout: (cb: () => void, ms: number) => setTimeout(cb, ms),
      clearTimeout: (id: unknown) => clearTimeout(id as any),
    };

    this.timeoutMs = options?.repairRequestTimeoutMs ?? DEFAULT_REPAIR_REQUEST_TIMEOUT_MS;
    this.maxRetries = options?.maxRetries ?? MAX_REPAIR_RETRIES;
    this.backoffBaseMs = options?.backoffBaseMs ?? DEFAULT_BACKOFF_BASE_MS;
    this.maxConcurrentPerPeer =
      options?.maxConcurrentRequestsPerPeer ?? MAX_CONCURRENT_REQUESTS_PER_PEER;
    this.maxConcurrentTotal =
      options?.maxConcurrentRequestsTotal ?? MAX_CONCURRENT_REQUESTS_TOTAL;
    this.rttProvider = options?.rttProvider;
  }

  public get isDisposed(): boolean {
    return this.disposed;
  }

  public get isDegraded(): boolean {
    return this.degraded;
  }

  /**
   * Records replication knowledge advertised in an incoming StateDigestPayload.
   */
  public recordPeerDigestKnowledge(digest: StateDigestPayload): void {
    if (this.disposed) return;

    this.peerKnowledge.set(digest.senderPeerId, {
      peerId: digest.senderPeerId,
      generation: digest.connectionGeneration,
      epoch: digest.membershipEpoch,
      replicationVector: { ...digest.replicationVector },
      snapshotFrontier: { ...(digest.snapshotFrontier ?? {}) },
      lastObservedAt: this.clock.now(),
    });

    // Reactive drain: an eligible supplier might have just advertised needed history
    this.drainQueue();
  }

  /**
   * Enqueues missing sequence ranges for targeted repair.
   * - Deduplicates against local replication frontier.
   * - Trims already satisfied prefixes.
   * - Slices large ranges into contiguous sub-ranges of at most 32 operations.
   * - Suppresses duplicate requests already in flight or already pending.
   */
  public addGaps(
    ranges: ReadonlyArray<MissingSequenceRange>,
    preferredSupplier?: string,
  ): void {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot add gaps to disposed repair pipeline');
    }

    const localVector = this.engine.replicationFrontier;

    for (const range of ranges) {
      const currentApplied = localVector[range.originPeerId] ?? 0;

      // 1. Skip if already locally satisfied
      if (range.toSequence <= currentApplied) {
        continue;
      }

      // 2. Trim prefix if partially satisfied
      const effectiveFrom = Math.max(range.fromSequence, currentApplied + 1);
      const effectiveTo = range.toSequence;
      if (effectiveFrom > effectiveTo) {
        continue;
      }

      // 3. Slice into chunks of at most MAX_OPERATIONS_PER_REPAIR_BATCH (32 ops)
      let chunkFrom = effectiveFrom;
      while (chunkFrom <= effectiveTo) {
        const chunkTo = Math.min(
          effectiveTo,
          chunkFrom + MAX_OPERATIONS_PER_REPAIR_BATCH - 1,
        );

        this.enqueueGapSlice(
          range.originPeerId,
          chunkFrom,
          chunkTo,
          preferredSupplier,
        );

        chunkFrom = chunkTo + 1;
      }
    }

    this.drainQueue();
  }

  /**
   * Internal helper to enqueue a single sliced gap if not already in-flight or pending.
   */
  private enqueueGapSlice(
    originPeerId: string,
    fromSequence: number,
    toSequence: number,
    preferredSupplier?: string,
  ): void {
    // Check if an identical range is already in-flight
    for (const inFlight of this.inFlightRequests.values()) {
      if (
        inFlight.originPeerId === originPeerId &&
        inFlight.fromSequence <= fromSequence &&
        inFlight.toSequence >= toSequence
      ) {
        return; // Already actively requested
      }
    }

    // Check if an identical or enclosing range is already pending
    for (const pending of this.pendingGaps) {
      if (
        pending.originPeerId === originPeerId &&
        pending.fromSequence <= fromSequence &&
        pending.toSequence >= toSequence
      ) {
        return; // Already pending
      }
    }

    this.pendingGaps.push({
      originPeerId,
      fromSequence,
      toSequence,
      preferredSupplier,
      retryCount: 0,
      attemptedSuppliers: new Set<string>(),
    });
  }

  /**
   * Processes inbound OPERATION_BATCH fulfilling an active repair request.
   * - Correlates by batch.requestId.
   * - Clears timeout timer.
   * - Passes batch and genuine AuthenticatedSupplierContext to engine.ingestRepairBatch().
   * - Advances progress and triggers partial-batch slicing if necessary.
   */
  public handleRepairBatch(
    batch: OperationBatchPayload,
    context?: AuthenticatedSupplierContext,
  ): EngineDecision[] {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot handle repair batch on disposed pipeline');
    }

    const inFlight = batch.requestId ? this.inFlightRequests.get(batch.requestId) : undefined;
    if (inFlight) {
      this.clock.clearTimeout(inFlight.timerId);
      this.inFlightRequests.delete(batch.requestId);
    }

    // Pass repaired operations directly into the Phase 7.3 capability-gated admission pipeline
    const decisions = this.engine.ingestRepairBatch(batch, context);

    // Check for sequence collisions or identity conflicts
    for (const d of decisions) {
      if (
        d.code === 'REJECTED_IDENTITY_CONFLICT' ||
        d.code === 'REJECTED_SEQUENCE_CONFLICT'
      ) {
        this.degraded = true;
        this.degradedReason = d.code;
        return decisions;
      }
    }

    // Evaluate progress against local replication frontier
    const currentSeq = this.engine.replicationFrontier[batch.originPeerId] ?? 0;
    const targetEnd = inFlight ? inFlight.toSequence : batch.toSequence;

    if (currentSeq >= targetEnd) {
      // Completed this range
      this.totalRepairsCompleted++;
    } else if (inFlight && currentSeq < inFlight.toSequence) {
      // Partial batch fulfillment: re-enqueue the unfulfilled remainder
      const remainderFrom = Math.max(batch.toSequence + 1, currentSeq + 1);
      const remainderTo = inFlight.toSequence;
      if (remainderFrom <= remainderTo) {
        this.enqueueGapSlice(
          inFlight.originPeerId,
          remainderFrom,
          remainderTo,
          inFlight.supplierPeerId,
        );
      }
    }

    this.drainQueue();
    return decisions;
  }

  /**
   * Handles an explicit REPAIR_REJECTED response from a supplier.
   * - Clears timeout timer.
   * - Blacklists the rejecting supplier for this range.
   * - Handles specific reason codes and fails over to other eligible suppliers.
   */
  public handleRepairRejected(rejection: RepairRejectedPayload): void {
    if (this.disposed) return;

    const inFlight = this.inFlightRequests.get(rejection.requestId);
    if (!inFlight) return;

    this.clock.clearTimeout(inFlight.timerId);
    this.inFlightRequests.delete(rejection.requestId);

    inFlight.attemptedSuppliers.add(rejection.supplierPeerId);

    switch (rejection.reason) {
      case 'OUTSIDE_HORIZON': {
        // Supplier has compacted this history into base snapshot.
        // Update snapshot frontier knowledge for this supplier.
        const supplierKnow = this.peerKnowledge.get(rejection.supplierPeerId);
        if (supplierKnow) {
          const retained = rejection.retainedHorizonSequence ?? rejection.fromSequence;
          supplierKnow.snapshotFrontier[rejection.targetOriginPeerId] = Math.max(
            supplierKnow.snapshotFrontier[rejection.targetOriginPeerId] ?? 0,
            retained,
          );
        }

        // Check if other eligible suppliers exist
        const hasAlternative = this.hasEligibleSupplier(inFlight);
        if (!hasAlternative) {
          this.totalRepairsFailed++;
          this.degraded = true;
          this.degradedReason = 'HISTORY_HORIZON_EXCEEDED';
          this.callbacks.onHistoryExhausted?.({
            originPeerId: inFlight.originPeerId,
            missingSequence: inFlight.fromSequence,
            highestRetainedSequence: rejection.retainedHorizonSequence ?? 0,
          });
          return;
        }

        // Failover to next eligible supplier
        this.reEnqueueForFailover(inFlight, false);
        break;
      }

      case 'ORIGIN_UNKNOWN': {
        // Supplier has never observed this origin peer
        this.reEnqueueForFailover(inFlight, false);
        break;
      }

      case 'RATE_LIMITED':
      case 'BUSY': {
        // Supplier is congested; back off before retrying or prefer another supplier
        this.reEnqueueForFailover(inFlight, true);
        break;
      }

      case 'EPOCH_MISMATCH': {
        // Supplier on different epoch; failover
        this.reEnqueueForFailover(inFlight, false);
        break;
      }
    }

    this.drainQueue();
  }

  /**
   * Cleans up tracking when a peer disconnects.
   * Cancels in-flight requests to that peer and immediately fails over.
   */
  public onPeerDisconnected(peerId: string): void {
    if (this.disposed) return;

    this.peerKnowledge.delete(peerId);

    // Failover all in-flight requests that were directed to this departing peer
    for (const [reqId, inFlight] of Array.from(this.inFlightRequests.entries())) {
      if (inFlight.supplierPeerId === peerId) {
        this.clock.clearTimeout(inFlight.timerId);
        this.inFlightRequests.delete(reqId);
        inFlight.attemptedSuppliers.add(peerId);
        this.reEnqueueForFailover(inFlight, false);
      }
    }

    this.drainQueue();
  }

  /**
   * Cancels all active in-flight requests and timers upon membership epoch transition.
   */
  public onEpochTransition(_newEpoch: number): void {
    if (this.disposed) return;

    // 1. Cancel all active in-flight timers and purge requests
    for (const inFlight of this.inFlightRequests.values()) {
      this.clock.clearTimeout(inFlight.timerId);
    }
    this.inFlightRequests.clear();

    // 2. Clear all backoff timers in pending queue
    for (const pending of this.pendingGaps) {
      if (pending.backoffTimerId !== undefined) {
        this.clock.clearTimeout(pending.backoffTimerId);
        pending.backoffTimerId = undefined;
      }
      pending.retryCount = 0;
      pending.attemptedSuppliers.clear();
    }

    // 3. Re-evaluate remaining gaps under new epoch
    this.drainQueue();
  }

  /**
   * Diagnostic telemetry status.
   */
  public getStatus(): RepairPipelineStatus {
    return {
      activeRequestsCount: this.inFlightRequests.size,
      pendingGapsCount: this.pendingGaps.length,
      totalRepairsCompleted: this.totalRepairsCompleted,
      totalRepairsFailed: this.totalRepairsFailed,
      degraded: this.degraded,
      degradedReason: this.degradedReason,
    };
  }

  /**
   * Disposes the pipeline and clears all active timers.
   */
  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    for (const inFlight of this.inFlightRequests.values()) {
      this.clock.clearTimeout(inFlight.timerId);
    }
    this.inFlightRequests.clear();

    for (const pending of this.pendingGaps) {
      if (pending.backoffTimerId !== undefined) {
        this.clock.clearTimeout(pending.backoffTimerId);
        pending.backoffTimerId = undefined;
      }
    }
    this.pendingGaps.length = 0;
    this.peerKnowledge.clear();
  }

  /**
   * Re-enqueues an in-flight request for failover or retry.
   */
  private reEnqueueForFailover(
    inFlight: InFlightRepairRequest,
    withBackoff: boolean,
  ): void {
    inFlight.retryCount++;

    if (inFlight.retryCount > this.maxRetries) {
      this.totalRepairsFailed++;
      this.degraded = true;
      this.degradedReason = 'MAX_RETRIES_EXCEEDED';
      this.callbacks.onHistoryExhausted?.({
        originPeerId: inFlight.originPeerId,
        missingSequence: inFlight.fromSequence,
        highestRetainedSequence: 0,
      });
      return;
    }

    const task: PendingGapTask = {
      originPeerId: inFlight.originPeerId,
      fromSequence: inFlight.fromSequence,
      toSequence: inFlight.toSequence,
      retryCount: inFlight.retryCount,
      attemptedSuppliers: inFlight.attemptedSuppliers,
    };

    if (withBackoff) {
      const delayMs = this.backoffBaseMs * Math.pow(2, inFlight.retryCount - 1);
      task.backoffTimerId = this.clock.setTimeout(() => {
        task.backoffTimerId = undefined;
        this.drainQueue();
      }, delayMs);
    }

    this.pendingGaps.unshift(task); // Re-queue at head for priority failover
  }

  /**
   * Handles timeout of an in-flight repair request (1500ms).
   */
  private handleRequestTimeout(requestId: string): void {
    if (this.disposed) return;

    const inFlight = this.inFlightRequests.get(requestId);
    if (!inFlight) return;

    this.inFlightRequests.delete(requestId);
    inFlight.attemptedSuppliers.add(inFlight.supplierPeerId);

    // Timeout triggers failover to next eligible supplier with backoff
    this.reEnqueueForFailover(inFlight, true);
    this.drainQueue();
  }

  /**
   * Checks whether at least one eligible supplier exists for the given task.
   */
  private hasEligibleSupplier(task: {
    originPeerId: string;
    fromSequence: number;
    toSequence: number;
    attemptedSuppliers: Set<string>;
  }): boolean {
    const activePeers = this.engine.getActivePeerGenerations();

    for (const [peerId, gen] of activePeers.entries()) {
      if (peerId === this.engine.localPeerId) continue;
      if (gen < 1) continue;
      if (task.attemptedSuppliers.has(peerId)) continue;

      const know = this.peerKnowledge.get(peerId);
      if (!know) continue;

      const peerFrontier = know.replicationVector[task.originPeerId] ?? 0;
      if (peerFrontier < task.toSequence) continue;

      const snapFrontier = know.snapshotFrontier[task.originPeerId] ?? 0;
      if (task.fromSequence <= snapFrontier) continue;

      return true;
    }

    return false;
  }

  /**
   * Selects the best eligible supplier conforming strictly to Architecture §4.2:
   * 1. Active authorized connection (generation in peerGenerations).
   * 2. Sufficient replication frontier (V_S[origin] >= toSeq).
   * 3. Uncompacted range (fromSeq > snapshotFrontier[origin]).
   * 4. Concurrency limit (active in-flight requests < 2).
   * 5. Ranking: Author preference first, then minimum SRTT.
   */
  private selectSupplier(task: PendingGapTask): string | undefined {
    const activePeers = this.engine.getActivePeerGenerations();
    const eligible: string[] = [];

    // Count in-flight requests per peer
    const inFlightPerPeer = new Map<string, number>();
    for (const req of this.inFlightRequests.values()) {
      inFlightPerPeer.set(
        req.supplierPeerId,
        (inFlightPerPeer.get(req.supplierPeerId) ?? 0) + 1,
      );
    }

    for (const [peerId, gen] of activePeers.entries()) {
      if (peerId === this.engine.localPeerId) continue;
      if (gen < 1) continue;
      if (task.attemptedSuppliers.has(peerId)) continue;

      const currentActive = inFlightPerPeer.get(peerId) ?? 0;
      if (currentActive >= this.maxConcurrentPerPeer) continue;

      const know = this.peerKnowledge.get(peerId);
      if (!know) continue;

      // 1. Sufficient replication frontier
      const peerFrontier = know.replicationVector[task.originPeerId] ?? 0;
      if (peerFrontier < task.toSequence) continue;

      // 2. Uncompacted range
      const snapFrontier = know.snapshotFrontier[task.originPeerId] ?? 0;
      if (task.fromSequence <= snapFrontier) continue;

      eligible.push(peerId);
    }

    if (eligible.length === 0) {
      return undefined;
    }

    // Author preference: if author is eligible, select author
    if (eligible.includes(task.originPeerId)) {
      return task.originPeerId;
    }

    // Preferred supplier if set and eligible
    if (task.preferredSupplier && eligible.includes(task.preferredSupplier)) {
      return task.preferredSupplier;
    }

    // RTT-weighted ranking: select candidate with minimum SRTT
    if (this.rttProvider && eligible.length > 1) {
      eligible.sort((a, b) => {
        const rttA = this.rttProvider!(a) ?? Number.MAX_SAFE_INTEGER;
        const rttB = this.rttProvider!(b) ?? Number.MAX_SAFE_INTEGER;
        return rttA - rttB;
      });
    }

    return eligible[0];
  }

  /**
   * Drains the pending gap queue, dispatching requests up to concurrency limits.
   */
  private drainQueue(): void {
    if (this.disposed) return;

    const localVector = this.engine.replicationFrontier;

    while (
      this.pendingGaps.length > 0 &&
      this.inFlightRequests.size < this.maxConcurrentTotal
    ) {
      const taskIndex = this.pendingGaps.findIndex(
        (t) => t.backoffTimerId === undefined,
      );
      if (taskIndex === -1) break; // All remaining tasks are awaiting backoff timer

      const task = this.pendingGaps[taskIndex]!;

      // 1. Verify if already satisfied while waiting in queue
      const currentApplied = localVector[task.originPeerId] ?? 0;
      if (task.toSequence <= currentApplied) {
        this.pendingGaps.splice(taskIndex, 1);
        continue;
      }
      if (task.fromSequence <= currentApplied) {
        task.fromSequence = currentApplied + 1;
      }

      // 2. Select eligible supplier
      const supplierPeerId = this.selectSupplier(task);
      if (!supplierPeerId) {
        // Check if there are no eligible peers anywhere in the mesh
        const hasAlternative = this.hasEligibleSupplier(task);
        if (!hasAlternative) {
          // If no peer can ever satisfy this gap, history is exhausted
          this.pendingGaps.splice(taskIndex, 1);
          this.totalRepairsFailed++;
          this.degraded = true;
          this.degradedReason = 'NO_ELIGIBLE_REPAIR_SUPPLIER';
          this.callbacks.onHistoryExhausted?.({
            originPeerId: task.originPeerId,
            missingSequence: task.fromSequence,
            highestRetainedSequence: 0,
          });
          continue;
        }

        // Peers exist but are currently busy (at concurrency limit) or awaiting backoff; break for now
        break;
      }

      // Remove task from pending queue
      this.pendingGaps.splice(taskIndex, 1);

      // Generate UUID v4 request identifier
      const requestId = generateUUIDv4();

      const requestPayload: RepairRequestPayload = {
        requestId,
        sessionId: this.engine.sessionId,
        requesterPeerId: this.engine.localPeerId,
        targetOriginPeerId: task.originPeerId,
        originPeerId: task.originPeerId,
        fromSequence: task.fromSequence,
        toSequence: task.toSequence,
        membershipEpoch: this.engine.epoch,
      };

      // Set timeout timer
      const timerId = this.clock.setTimeout(() => {
        this.handleRequestTimeout(requestId);
      }, this.timeoutMs);

      task.attemptedSuppliers.add(supplierPeerId);

      const inFlight: InFlightRepairRequest = {
        requestId,
        supplierPeerId,
        originPeerId: task.originPeerId,
        fromSequence: task.fromSequence,
        toSequence: task.toSequence,
        membershipEpoch: this.engine.epoch,
        sentAtMonotonic: this.clock.now(),
        timerId,
        retryCount: task.retryCount,
        attemptedSuppliers: task.attemptedSuppliers,
      };

      this.inFlightRequests.set(requestId, inFlight);

      // Transmit unicast request via transport callback
      try {
        this.callbacks.onSendRepairRequest?.(supplierPeerId, requestPayload);
      } catch {
        // Transport failure handled on next tick or timeout
      }
    }
  }
}
