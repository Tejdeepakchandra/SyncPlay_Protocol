/**
 * Phase 7.4 M3 — Final Duplicate Repair Response and Replay-Safety Audit Tests.
 *
 * Verifies:
 * 1. Identical OPERATION_BATCH delivered twice while its request remains active (including re-entrant delivery during ingestion).
 * 2. Identical OPERATION_BATCH delivered after its request has completed.
 * 3. Identical partial OPERATION_BATCH delivered twice.
 * 4. Identical REPAIR_REJECTED delivered twice while its request remains active.
 * 5. Duplicate response after supplier failover.
 * 6. Duplicate response after epoch transition.
 * 7. Legitimate subsequent partial response still completes the remaining range.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createTransportRegistrationCapability,
  createEpochTransitionCapability,
  createAuthenticatedSupplierContext,
  TrustedTransportAdapter,
  type TransportRegistrationCapability,
  type EpochTransitionCapability,
} from '../engine/leaderless-sync-engine.js';
import {
  AntiEntropyManager,
  RepairRequestPipeline,
} from '../replication/index.js';
import type {
  SchedulerClock,
  AntiEntropyCallbacks,
} from '../replication/types.js';
import {
  type OperationBatchPayload,
  type RepairRejectedPayload,
  type SerializedControlOperation,
} from '@syncplay/protocol';

class DeterministicClock implements SchedulerClock {
  private currentTime = 1000;
  private nextTimerId = 1;
  private readonly timers = new Map<number, { callback: () => void; dueTime: number }>();

  public now(): number {
    return this.currentTime;
  }

  public setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextTimerId++;
    this.timers.set(id, {
      callback,
      dueTime: this.currentTime + Math.max(0, delayMs),
    });
    return id;
  }

  public clearTimeout(timerId: unknown): void {
    if (typeof timerId === 'number') {
      this.timers.delete(timerId);
    }
  }

  public advanceTime(ms: number): void {
    this.currentTime += ms;
    const due = Array.from(this.timers.entries())
      .filter(([_, t]) => t.dueTime <= this.currentTime)
      .sort((a, b) => a[1].dueTime - b[1].dueTime);

    for (const [id, timer] of due) {
      if (this.timers.has(id)) {
        this.timers.delete(id);
        timer.callback();
      }
    }
  }
}

describe('Phase 7.4 M3: Final Duplicate Repair Response and Replay-Safety Audit', () => {
  let clock: DeterministicClock;
  let engine: LeaderlessSyncEngine;
  let coordinatorCap: EpochTransitionCapability;
  let transportCap: TransportRegistrationCapability;
  let transportAdapter: TrustedTransportAdapter;
  let antiEntropyManager: AntiEntropyManager;
  let sentRequests: Array<{ targetPeerId: string; request: any }>;
  let callbacks: AntiEntropyCallbacks;

  beforeEach(() => {
    clock = new DeterministicClock();
    sentRequests = [];

    coordinatorCap = createEpochTransitionCapability(
      'peer-coord',
      'test-session-1',
      'secret-coord-key',
    );
    transportCap = createTransportRegistrationCapability(
      'transport-driver',
      'test-session-1',
      'secret-transport-key',
    );

    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session-1',
      localPeerId: 'peer-local',
      clock: { now: () => clock.now() },
      epochCapability: coordinatorCap,
      transportCapability: transportCap,
    });

    callbacks = {
      onSendDigest: vi.fn(),
      onSendRepairRequest: (targetPeerId, request) => {
        sentRequests.push({ targetPeerId, request });
      },
      onHistoryExhausted: vi.fn(),
    };

    antiEntropyManager = new AntiEntropyManager(
      engine,
      callbacks,
      { repairRequestTimeoutMs: 1500, backoffBaseMs: 1000, maxRetries: 3 },
      clock,
    );

    transportAdapter = new TrustedTransportAdapter(engine, transportCap, antiEntropyManager);
  });

  function makeSampleOp(
    origin: string,
    seq: number,
    logicalClock: number,
    causalContext: Record<string, number> = {},
    epoch = 1,
  ): SerializedControlOperation {
    return {
      logicalEventId: `evt-${origin}-${seq}-${epoch}`,
      originPeerId: origin,
      membershipEpoch: epoch,
      messageType: 'PLAY',
      logicalTimestamp: logicalClock,
      originSequenceNumber: seq,
      causalContext,
      payload: { positionMs: seq * 1000 },
    };
  }

  function makeSampleBatch(
    requestId: string,
    supplierPeerId: string,
    originPeerId: string,
    fromSeq: number,
    toSeq: number,
    epoch = 1,
  ): OperationBatchPayload {
    const operations: SerializedControlOperation[] = [];
    for (let s = fromSeq; s <= toSeq; s++) {
      const prevContext: Record<string, number> = s > 1 ? { [originPeerId]: s - 1 } : {};
      operations.push(makeSampleOp(originPeerId, s, s, prevContext, epoch));
    }
    return {
      requestId,
      sessionId: 'test-session-1',
      supplierPeerId,
      originPeerId,
      fromSequence: fromSeq,
      toSequence: toSeq,
      membershipEpoch: epoch,
      operations,
    };
  }

  // ───────────────────────────────────────────────────────────────────────────
  // 1. Identical OPERATION_BATCH delivered twice while its request remains active
  // ───────────────────────────────────────────────────────────────────────────
  it('1. Identical OPERATION_BATCH delivered twice while its request remains active cannot re-ingest or mutate state twice', () => {
    engine.setPeerGeneration('peer-S1', 1);

    // Advertise knowledge that peer-S1 has up to seq 5 for origin peer-author
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    expect(sentRequests.length).toBe(1);
    const reqId = sentRequests[0]!.request.requestId;
    expect(sentRequests[0]!.targetPeerId).toBe('peer-S1');
    expect(sentRequests[0]!.request.fromSequence).toBe(1);
    expect(sentRequests[0]!.request.toSequence).toBe(5);

    const batch = makeSampleBatch(reqId, 'peer-S1', 'peer-author', 1, 5);

    // Verify re-entrant execution during active ingestion
    let reentrantDecisions: any[] | undefined;
    let listenerInvocations = 0;
    const unsub = engine.onStateChange(() => {
      listenerInvocations++;
      if (listenerInvocations === 1) {
        // While the first batch is being processed on the stack, attempt duplicate delivery
        reentrantDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
      }
    });

    // Outer delivery
    const primaryDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
    unsub();

    // 1. Assert re-entrant duplicate was immediately rejected before ingestion because request is retired atomically
    expect(reentrantDecisions).toBeDefined();
    expect(reentrantDecisions!.length).toBe(1);
    expect(reentrantDecisions![0].accepted).toBe(false);
    expect(reentrantDecisions![0].code).toBe('REJECTED_INVALID');
    expect(reentrantDecisions![0].message).toContain('not an active in-flight request');

    // 2. Assert primary delivery succeeded
    expect(primaryDecisions.length).toBe(5);
    expect(primaryDecisions.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);

    // 3. Sequential duplicate delivery immediately after
    const sequentialDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
    expect(sequentialDecisions.length).toBe(1);
    expect(sequentialDecisions[0].accepted).toBe(false);
    expect(sequentialDecisions[0].code).toBe('REJECTED_INVALID');

    // 4. Invariant assertions: replication vector, completed count, active requests, queue
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(0);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(1);
    expect(antiEntropyManager.getStatus().totalRepairsFailed).toBe(0);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 2. Identical OPERATION_BATCH delivered after its request has completed
  // ───────────────────────────────────────────────────────────────────────────
  it('2. Identical OPERATION_BATCH delivered after its request has completed is rejected with no state mutation', () => {
    engine.setPeerGeneration('peer-S1', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    const reqId = sentRequests[0]!.request.requestId;
    const batch = makeSampleBatch(reqId, 'peer-S1', 'peer-author', 1, 5);

    // Complete the request
    const primaryDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
    expect(primaryDecisions.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);

    const initialAppliedCount = engine.getStats().eventsAppliedCount;
    const initialReceivedCount = engine.getStats().eventsReceivedCount;
    const initialStateVersion = engine.getState().stateVersion;

    // Delayed duplicate packet arrives
    const duplicateDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
    expect(duplicateDecisions.length).toBe(1);
    expect(duplicateDecisions[0].accepted).toBe(false);
    expect(duplicateDecisions[0].code).toBe('REJECTED_INVALID');
    expect(duplicateDecisions[0].message).toContain('not an active in-flight request');

    // Verify engine state and stats are not mutated
    expect(engine.getStats().eventsAppliedCount).toBe(initialAppliedCount);
    expect(engine.getStats().eventsReceivedCount).toBe(initialReceivedCount);
    expect(engine.getState().stateVersion).toBe(initialStateVersion);
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(1);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 3. Identical partial OPERATION_BATCH delivered twice
  // ───────────────────────────────────────────────────────────────────────────
  it('3. Identical partial OPERATION_BATCH delivered twice does not create duplicate remainder ranges or drop missing ranges', () => {
    engine.setPeerGeneration('peer-S1', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    expect(sentRequests.length).toBe(1);
    const reqId1 = sentRequests[0]!.request.requestId;
    expect(sentRequests[0]!.request.fromSequence).toBe(1);
    expect(sentRequests[0]!.request.toSequence).toBe(10);

    // Partial response: returns only 1..5 for reqId1
    const partialBatch = makeSampleBatch(reqId1, 'peer-S1', 'peer-author', 1, 5);
    const d1 = transportAdapter.handleInboundRepairBatch(partialBatch, 'peer-S1', 1);
    expect(d1.length).toBe(5);
    expect(d1.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);

    // Requester automatically dispatched subsequent request for remainder [6..10]
    expect(sentRequests.length).toBe(2);
    const reqId2 = sentRequests[1]!.request.requestId;
    expect(reqId2).not.toBe(reqId1);
    expect(sentRequests[1]!.request.fromSequence).toBe(6);
    expect(sentRequests[1]!.request.toSequence).toBe(10);

    // Duplicate of partial batch arrives with original reqId1
    const dDup = transportAdapter.handleInboundRepairBatch(partialBatch, 'peer-S1', 1);
    expect(dDup.length).toBe(1);
    expect(dDup[0].accepted).toBe(false);
    expect(dDup[0].code).toBe('REJECTED_INVALID');
    expect(dDup[0].message).toContain('not an active in-flight request');

    // Verify lifecycle state: no extra request dispatched, no duplicate queue entry, active requests is 1 (reqId2)
    expect(sentRequests.length).toBe(2);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(1);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(0);
    expect(engine.replicationFrontier['peer-author']).toBe(5);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 4. Identical REPAIR_REJECTED delivered twice while its request remains active
  // ───────────────────────────────────────────────────────────────────────────
  it('4. Identical REPAIR_REJECTED delivered twice cannot consume retry budget or schedule backoff twice', () => {
    engine.setPeerGeneration('peer-S1', 1);
    engine.setPeerGeneration('peer-S2', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S2',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    expect(sentRequests.length).toBe(1);
    const reqId = sentRequests[0]!.request.requestId;
    const initialSupplier = sentRequests[0]!.targetPeerId;
    const alternateSupplier = initialSupplier === 'peer-S1' ? 'peer-S2' : 'peer-S1';

    const rejection: RepairRejectedPayload = {
      requestId: reqId,
      sessionId: 'test-session-1',
      supplierPeerId: initialSupplier,
      targetOriginPeerId: 'peer-author',
      fromSequence: 1,
      toSequence: 5,
      reason: 'BUSY',
      membershipEpoch: 1,
    };

    // First delivery of rejection
    transportAdapter.handleInboundRepairRejected(rejection, initialSupplier, 1);

    // In-flight request was atomically retired, and gap was re-queued with 1000ms backoff
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(1);

    // Duplicate delivery of rejection with same reqId before backoff expires
    transportAdapter.handleInboundRepairRejected(rejection, initialSupplier, 1);

    // Assert duplicate rejection was dropped: pending gaps count is still 1, not 2
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(1);

    // Advance time to allow the single backoff timer to expire
    clock.advanceTime(1000);

    // Exactly one retry request is dispatched, not two
    expect(sentRequests.length).toBe(2);
    expect(sentRequests[1]!.targetPeerId).toBe(alternateSupplier);
    expect(sentRequests[1]!.request.requestId).not.toBe(reqId);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(1);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(0);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 5. Duplicate response after supplier failover
  // ───────────────────────────────────────────────────────────────────────────
  it('5. Duplicate response after supplier failover does not interfere with the active failover request', () => {
    engine.setPeerGeneration('peer-S1', 1);
    engine.setPeerGeneration('peer-S2', 1);

    // Advertise knowledge from both suppliers
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S2',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    expect(sentRequests.length).toBe(1);
    const reqId1 = sentRequests[0]!.request.requestId;
    const initialSupplier = sentRequests[0]!.targetPeerId;

    // Primary supplier rejects with ORIGIN_UNKNOWN, triggering immediate failover
    transportAdapter.handleInboundRepairRejected(
      {
        requestId: reqId1,
        sessionId: 'test-session-1',
        supplierPeerId: initialSupplier,
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'ORIGIN_UNKNOWN',
        membershipEpoch: 1,
      },
      initialSupplier,
      1,
    );

    // Failover request dispatched to alternate supplier
    expect(sentRequests.length).toBe(2);
    const alternateSupplier = initialSupplier === 'peer-S1' ? 'peer-S2' : 'peer-S1';
    expect(sentRequests[1]!.targetPeerId).toBe(alternateSupplier);
    const reqId2 = sentRequests[1]!.request.requestId;

    // A delayed duplicate OPERATION_BATCH arrives from the old supplier with reqId1
    const delayedBatch = makeSampleBatch(reqId1, initialSupplier, 'peer-author', 1, 5);
    const delayedDecisions = transportAdapter.handleInboundRepairBatch(delayedBatch, initialSupplier, 1);
    expect(delayedDecisions.length).toBe(1);
    expect(delayedDecisions[0].accepted).toBe(false);
    expect(delayedDecisions[0].code).toBe('REJECTED_INVALID');

    // Assert failover request to alternate supplier remains active and undamaged
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(1);
    expect(engine.replicationFrontier['peer-author'] ?? 0).toBe(0);

    // Now alternate supplier fulfills reqId2
    const validBatch = makeSampleBatch(reqId2, alternateSupplier, 'peer-author', 1, 5);
    const validDecisions = transportAdapter.handleInboundRepairBatch(validBatch, alternateSupplier, 1);
    expect(validDecisions.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(1);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 6. Duplicate response after epoch transition
  // ───────────────────────────────────────────────────────────────────────────
  it('6. Duplicate response after epoch transition is rejected without mutating state in new epoch', () => {
    engine.setPeerGeneration('peer-S1', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    const reqId = sentRequests[0]!.request.requestId;
    const batch = makeSampleBatch(reqId, 'peer-S1', 'peer-author', 1, 5, 1);

    // Transition to epoch 2
    engine.transitionEpoch(2, coordinatorCap);
    antiEntropyManager.onEpochTransition(2);

    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(0);

    // Stale/duplicate packet from epoch 1 arrives in epoch 2
    const staleDecisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S1', 1);
    expect(staleDecisions.length).toBe(1);
    expect(staleDecisions[0].accepted).toBe(false);
    expect(staleDecisions[0].code).toBe('REJECTED_INVALID');

    expect(engine.replicationFrontier['peer-author'] ?? 0).toBe(0);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(0);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 7. Legitimate subsequent partial response still completes the remaining range
  // ───────────────────────────────────────────────────────────────────────────
  it('7. Legitimate subsequent partial response still completes the remaining range despite intermediate duplicates', () => {
    engine.setPeerGeneration('peer-S1', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      stateVersion: 1,
      sentAt: 1000,
    });

    const reqId1 = sentRequests[0]!.request.requestId;

    // 1. First partial batch [1..5] arrives
    const part1 = makeSampleBatch(reqId1, 'peer-S1', 'peer-author', 1, 5);
    const d1 = transportAdapter.handleInboundRepairBatch(part1, 'peer-S1', 1);
    expect(d1.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);

    // Second request [6..10] dispatched
    expect(sentRequests.length).toBe(2);
    const reqId2 = sentRequests[1]!.request.requestId;
    expect(sentRequests[1]!.request.fromSequence).toBe(6);
    expect(sentRequests[1]!.request.toSequence).toBe(10);

    // 2. Duplicate of part1 [1..5] arrives
    const dDup = transportAdapter.handleInboundRepairBatch(part1, 'peer-S1', 1);
    expect(dDup.length).toBe(1);
    expect(dDup[0].accepted).toBe(false);
    expect(dDup[0].code).toBe('REJECTED_INVALID');

    // 3. Legitimate subsequent partial batch [6..10] for reqId2 arrives
    const part2 = makeSampleBatch(reqId2, 'peer-S1', 'peer-author', 6, 10);
    const d2 = transportAdapter.handleInboundRepairBatch(part2, 'peer-S1', 1);
    expect(d2.length).toBe(5);
    expect(d2.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(10);

    // Invariant: both parts converged, repair completed
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().pendingGapsCount).toBe(0);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(1);
    expect(antiEntropyManager.getStatus().totalRepairsFailed).toBe(0);
  });
});
