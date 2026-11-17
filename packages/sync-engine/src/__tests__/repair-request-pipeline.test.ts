import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createTransportRegistrationCapability,
  createAuthenticatedSupplierContext,
  createEpochTransitionCapability,
} from '../engine/leaderless-sync-engine.js';
import {
  AntiEntropyManager,
  RepairRequestPipeline,
  generateUUIDv4,
  DEFAULT_REPAIR_REQUEST_TIMEOUT_MS,
  MAX_REPAIR_RETRIES,
  MAX_CONCURRENT_REQUESTS_PER_PEER,
  MAX_CONCURRENT_REQUESTS_TOTAL,
  MAX_OPERATIONS_PER_REPAIR_BATCH,
} from '../replication/index.js';
import type {
  SchedulerClock,
  AntiEntropyCallbacks,
  DigestComparisonResult,
} from '../replication/types.js';
import {
  type OperationBatchPayload,
  type RepairRejectedPayload,
  type StateDigestPayload,
  type SerializedControlOperation,
  UUID_V4_REGEX,
  UUIDv4Schema,
  MessageType,
} from '@syncplay/protocol';

class DeterministicTestClock implements SchedulerClock {
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

  public clearTimeout(id: unknown): void {
    if (typeof id === 'number') {
      this.timers.delete(id);
    }
  }

  public advanceTime(ms: number): void {
    const targetTime = this.currentTime + ms;
    while (this.timers.size > 0) {
      // Find earliest timer
      let earliestId: number | undefined = undefined;
      let earliestDue = Infinity;

      for (const [id, timer] of this.timers.entries()) {
        if (timer.dueTime < earliestDue) {
          earliestDue = timer.dueTime;
          earliestId = id;
        }
      }

      if (earliestId === undefined || earliestDue > targetTime) {
        break;
      }

      this.currentTime = earliestDue;
      const timer = this.timers.get(earliestId)!;
      this.timers.delete(earliestId);
      timer.callback();
    }
    this.currentTime = targetTime;
  }

  public get pendingTimersCount(): number {
    return this.timers.size;
  }
}

describe('Phase 7.4 Milestone M3 — Repair Request Pipeline', () => {
  let clock: DeterministicTestClock;
  let engine: LeaderlessSyncEngine;
  let transportCap: ReturnType<typeof createTransportRegistrationCapability>;
  let epochCap: ReturnType<typeof createEpochTransitionCapability>;
  let sentRequests: Array<{ targetPeerId: string; request: any }>;
  let callbacks: AntiEntropyCallbacks;

  beforeEach(() => {
    clock = new DeterministicTestClock();
    transportCap = createTransportRegistrationCapability('auth-1', 'test-session-1');
    epochCap = createEpochTransitionCapability('auth-1', 'test-session-1');

    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session-1',
      localPeerId: 'peer-local',
      initialClock: 0,
      clock,
      epochCapability: epochCap,
      transportCapability: transportCap,
    });

    sentRequests = [];
    callbacks = {
      onBroadcastDigest: vi.fn(),
      onSendRepairRequest: (targetPeerId, request) => {
        sentRequests.push({ targetPeerId, request });
      },
      onHistoryExhausted: vi.fn(),
    };
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
  // 1. Translating comparison results & pending queue gaps into missing ranges
  // ───────────────────────────────────────────────────────────────────────────
  describe('1. Missing Sequence Range Detection & Slicing', () => {
    it('slices missing range exceeding 32 operations into contiguous chunks of at most 32', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);

      // Register an eligible supplier who holds up to seq 70
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 70 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      });

      // Add a large gap: 1..70
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 70 }]);

      // Concurrency limit is 2 per peer, so the first 2 chunks should be dispatched
      expect(sentRequests.length).toBe(2);

      const req1 = sentRequests[0]!.request;
      expect(req1.fromSequence).toBe(1);
      expect(req1.toSequence).toBe(32);
      expect(req1.toSequence - req1.fromSequence + 1).toBe(MAX_OPERATIONS_PER_REPAIR_BATCH);

      const req2 = sentRequests[1]!.request;
      expect(req2.fromSequence).toBe(33);
      expect(req2.toSequence).toBe(64);
      expect(req2.toSequence - req2.fromSequence + 1).toBe(MAX_OPERATIONS_PER_REPAIR_BATCH);

      // Status reflects 2 active requests and 1 pending remaining gap slice [65, 70]
      const status = pipeline.getStatus();
      expect(status.activeRequestsCount).toBe(2);
      expect(status.pendingGapsCount).toBe(1);

      pipeline.dispose();
    });

    it('trims already satisfied prefixes against local replication frontier', () => {
      // Simulate that local peer already has author seq 1..5 admitted
      engine.setPeerGeneration('peer-author', 1);
      const supplierContext = createAuthenticatedSupplierContext(engine, 'peer-author', 1, transportCap);
      const initialBatch = makeSampleBatch(generateUUIDv4(), 'peer-author', 'peer-author', 1, 5);
      engine.ingestRepairBatch(initialBatch, supplierContext);

      expect(engine.replicationFrontier['peer-author']).toBe(5);

      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-author',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 15 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      // Request gap 1..15; since 1..5 is already applied, should request 6..15
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 15 }]);

      expect(sentRequests.length).toBe(1);
      expect(sentRequests[0]!.request.fromSequence).toBe(6);
      expect(sentRequests[0]!.request.toSequence).toBe(15);

      pipeline.dispose();
    });

    it('completely drops gaps that are already fully satisfied locally', () => {
      engine.setPeerGeneration('peer-author', 1);
      const supplierContext = createAuthenticatedSupplierContext(engine, 'peer-author', 1, transportCap);
      const initialBatch = makeSampleBatch(generateUUIDv4(), 'peer-author', 'peer-author', 1, 10);
      engine.ingestRepairBatch(initialBatch, supplierContext);

      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-author',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 10 }]);

      expect(sentRequests.length).toBe(0);
      expect(pipeline.getStatus().activeRequestsCount).toBe(0);
      expect(pipeline.getStatus().pendingGapsCount).toBe(0);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 2. Generating unique repair request IDs (UUID v4)
  // ───────────────────────────────────────────────────────────────────────────
  describe('2. Request ID Generation and RFC 4122 Compliance', () => {
    it('generates canonical UUID v4 conforming strictly to RFC 4122 §4.4', () => {
      for (let i = 0; i < 50; i++) {
        const id = generateUUIDv4();
        expect(id).toMatch(UUID_V4_REGEX);
        expect(UUIDv4Schema.safeParse(id).success).toBe(true);
        expect(id[14]).toBe('4'); // version 4
        expect(['8', '9', 'a', 'b', 'A', 'B']).toContain(id[19]); // variant 1
      }
    });

    it('ensures all dispatched RepairRequestPayloads carry unique UUID v4 requestIds', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10, 'origin-2': 10 },
        snapshotFrontier: { 'origin-1': 0, 'origin-2': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64), 'origin-2': '2'.repeat(64) },
      });

      pipeline.addGaps([
        { originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 },
        { originPeerId: 'origin-2', fromSequence: 1, toSequence: 5 },
      ]);

      expect(sentRequests.length).toBe(2);
      const id1 = sentRequests[0]!.request.requestId;
      const id2 = sentRequests[1]!.request.requestId;

      expect(id1).not.toBe(id2);
      expect(id1).toMatch(UUID_V4_REGEX);
      expect(id2).toMatch(UUID_V4_REGEX);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 3. Tracking outstanding requests and timeouts (1500ms)
  // ───────────────────────────────────────────────────────────────────────────
  describe('3. Request Tracking and Timeout Lifecycle', () => {
    it('tracks active request and times out after 1500ms', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const firstTarget = sentRequests[0]!.targetPeerId;

      // Advance time by 1499ms (should not time out yet)
      clock.advanceTime(1499);
      expect(sentRequests.length).toBe(1);

      // Advance 2ms (total 1501ms) -> timeout triggers
      // Timeout should blacklist peer-S1, increment retryCount, and re-dispatch/backoff
      clock.advanceTime(2);

      // Backoff delay for retry 1 is 1000ms
      clock.advanceTime(1000);

      // Failover request dispatched to peer-S2!
      expect(sentRequests.length).toBe(2);
      expect(sentRequests[1]!.targetPeerId).not.toBe(firstTarget);
      expect(sentRequests[1]!.request.fromSequence).toBe(1);
      expect(sentRequests[1]!.request.toSequence).toBe(5);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 4. Bounded retries (<= 3) and exponential backoff (1s, 2s, 4s)
  // ───────────────────────────────────────────────────────────────────────────
  describe('4. Bounded Retries and Exponential Backoff', () => {
    it('aborts after exactly 3 failed retries and triggers onHistoryExhausted', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      engine.setPeerGeneration('peer-S3', 1);
      engine.setPeerGeneration('peer-S4', 1);

      for (const peer of ['peer-S1', 'peer-S2', 'peer-S3', 'peer-S4']) {
        pipeline.recordPeerDigestKnowledge({
          formatVersion: 2,
          sessionId: 'test-session-1',
          senderPeerId: peer,
          connectionGeneration: 1,
          membershipEpoch: 1,
          replicationVector: { 'origin-1': 10 },
          snapshotFrontier: { 'origin-1': 0 },
          historyFingerprints: { 'origin-1': '1'.repeat(64) },
        });
      }

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1); // initial attempt (retryCount 0)

      // Timeout 1 (1500ms) + Backoff 1 (1000ms * 2^0 = 1000ms)
      clock.advanceTime(1500);
      clock.advanceTime(1000);
      expect(sentRequests.length).toBe(2); // retry 1

      // Timeout 2 (1500ms) + Backoff 2 (1000ms * 2^1 = 2000ms)
      clock.advanceTime(1500);
      clock.advanceTime(2000);
      expect(sentRequests.length).toBe(3); // retry 2

      // Timeout 3 (1500ms) + Backoff 3 (1000ms * 2^2 = 4000ms)
      clock.advanceTime(1500);
      clock.advanceTime(4000);
      expect(sentRequests.length).toBe(4); // retry 3

      // Timeout 4 (1500ms) -> exceeds maxRetries (3)
      clock.advanceTime(1500);

      // Verify failure handling
      const status = pipeline.getStatus();
      expect(status.totalRepairsFailed).toBe(1);
      expect(status.degraded).toBe(true);
      expect(status.activeRequestsCount).toBe(0);
      expect(status.pendingGapsCount).toBe(0);

      expect(callbacks.onHistoryExhausted).toHaveBeenCalledWith({
        originPeerId: 'origin-1',
        missingSequence: 1,
        highestRetainedSequence: 0,
      });

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 5. Duplicate and overlapping repair requests
  // ───────────────────────────────────────────────────────────────────────────
  describe('5. Duplicate Request Suppression', () => {
    it('suppresses duplicate requests while one is already in flight', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);

      // Add the exact same gap again
      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1); // No duplicate dispatched

      // Add a sub-range [2..4]
      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 2, toSequence: 4 }]);
      expect(sentRequests.length).toBe(1); // Still suppressed

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 6. Cancellation on membership epoch changes
  // ───────────────────────────────────────────────────────────────────────────
  describe('6. Cancellation on Membership Epoch Changes', () => {
    it('cancels active in-flight requests and timers upon onEpochTransition', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      expect(pipeline.getStatus().activeRequestsCount).toBe(1);
      expect(clock.pendingTimersCount).toBeGreaterThan(0);

      // Trigger epoch transition to epoch 2
      pipeline.onEpochTransition(2);

      expect(pipeline.getStatus().activeRequestsCount).toBe(0);
      expect(clock.pendingTimersCount).toBe(0);

      // Advancing time should not fire any obsolete timeout timers
      clock.advanceTime(5000);
      expect(sentRequests.length).toBe(1);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 7. Handling REPAIR_REJECTED (all 5 reasons)
  // ───────────────────────────────────────────────────────────────────────────
  describe('7. REPAIR_REJECTED Handling', () => {
    it('handles OUTSIDE_HORIZON by updating supplier frontier and failing over', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 20 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 20 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const reqId = sentRequests[0]!.request.requestId;
      const firstSupplier = sentRequests[0]!.targetPeerId;

      // Supplier S1 rejects with OUTSIDE_HORIZON (retained horizon is 10)
      const rejection: RepairRejectedPayload = {
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: firstSupplier,
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: 10,
      };

      const supplierCtx = engine.registerInboundSupplierContext(firstSupplier, 1, transportCap);
      pipeline.handleRepairRejected(rejection, supplierCtx);

      // Immediate failover to S2
      expect(sentRequests.length).toBe(2);
      expect(sentRequests[1]!.targetPeerId).not.toBe(firstSupplier);

      pipeline.dispose();
    });

    it('triggers onHistoryExhausted when all suppliers reject OUTSIDE_HORIZON', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 20 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const rejection: RepairRejectedPayload = {
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: 15,
      };

      const supplierCtx = engine.registerInboundSupplierContext('peer-S1', 1, transportCap);
      pipeline.handleRepairRejected(rejection, supplierCtx);

      expect(pipeline.getStatus().degraded).toBe(true);
      expect(pipeline.getStatus().degradedReason).toBe('HISTORY_HORIZON_EXCEEDED');
      expect(callbacks.onHistoryExhausted).toHaveBeenCalledWith({
        originPeerId: 'origin-1',
        missingSequence: 1,
        highestRetainedSequence: 15,
      });

      pipeline.dispose();
    });

    it('handles ORIGIN_UNKNOWN, RATE_LIMITED, EPOCH_MISMATCH, and BUSY gracefully', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 20 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 20 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      // Reject with RATE_LIMITED
      const supplierCtx = engine.registerInboundSupplierContext(sentRequests[0]!.targetPeerId, 1, transportCap);
      pipeline.handleRepairRejected({
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: sentRequests[0]!.targetPeerId,
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'RATE_LIMITED',
      }, supplierCtx);

      // Awaits backoff delay (1000ms)
      clock.advanceTime(1000);

      // Successfully fails over to S2
      expect(sentRequests.length).toBe(2);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 8. Preserving the Phase 7.3-R5-H supplier authorization boundary
  // ───────────────────────────────────────────────────────────────────────────
  describe('8. Preserving Phase 7.3-R5-H Supplier Authorization Boundary', () => {
    it('accepts repair batch when accompanied by genuine AuthenticatedSupplierContext', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-S': 5 },
        snapshotFrontier: { 'peer-S': 0 },
        historyFingerprints: { 'peer-S': 's'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-S', fromSequence: 1, toSequence: 2 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-S', 1, 2);
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      const decisions = pipeline.handleRepairBatch(batch, genuineContext);
      expect(decisions.length).toBe(2);
      expect(decisions.every((d) => d.accepted)).toBe(true);
      expect(engine.replicationFrontier['peer-S']).toBe(2);

      pipeline.dispose();
    });

    it('rejects repair batch when accompanied by fabricated or unregistered context', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-S': 5 },
        snapshotFrontier: { 'peer-S': 0 },
        historyFingerprints: { 'peer-S': 's'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-S', fromSequence: 1, toSequence: 2 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-S', 1, 2);

      // Fabricated context object without registration
      const fabricatedContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1,
      };

      const decisions = pipeline.handleRepairBatch(batch, fabricatedContext as any);
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(engine.replicationFrontier['peer-S']).toBeUndefined();

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 9. Passing operations through Phase 7.3 admission pipeline & partial batch
  // ───────────────────────────────────────────────────────────────────────────
  describe('9. Ingestion Pipeline and Partial Batch Handling', () => {
    it('handles partial batch fulfillment by admitting partial ops and dispatching remainder', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-S': 10 },
        snapshotFrontier: { 'peer-S': 0 },
        historyFingerprints: { 'peer-S': 's'.repeat(64) },
      });

      // Request 1..10
      pipeline.addGaps([{ originPeerId: 'peer-S', fromSequence: 1, toSequence: 10 }]);
      expect(sentRequests.length).toBe(1);
      const reqId = sentRequests[0]!.request.requestId;

      // Supplier only serves partial batch [1..4]
      const partialBatch = makeSampleBatch(reqId, 'peer-S', 'peer-S', 1, 4);
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      const decisions = pipeline.handleRepairBatch(partialBatch, genuineContext);
      expect(decisions.length).toBe(4);
      expect(engine.replicationFrontier['peer-S']).toBe(4);

      // Pipeline should immediately dispatch remainder [5..10]
      expect(sentRequests.length).toBe(2);
      expect(sentRequests[1]!.request.fromSequence).toBe(5);
      expect(sentRequests[1]!.request.toSequence).toBe(10);

      pipeline.dispose();
    });

    it('cascades and unblocks operations in PendingDependencyQueue upon repair ingestion', () => {
      engine.setPeerGeneration('peer-author', 1);
      const supplierContext = createAuthenticatedSupplierContext(engine, 'peer-author', 1, transportCap);

      // Ingest op 2 directly via engine.receive (it will buffer in PendingDependencyQueue because op 1 is missing)
      const op2 = makeSampleOp('peer-author', 2, 2, { 'peer-author': 1 });
      engine.receive({
        origin: 'remote',
        localTimestamp: 1000,
        message: {
          protocolVersion: 1,
          messageType: op2.messageType,
          sessionId: 'test-session-1',
          senderPeerId: 'peer-author',
          originPeerId: 'peer-author',
          messageId: `msg-${op2.logicalEventId}`,
          logicalEventId: op2.logicalEventId,
          logicalTimestamp: op2.logicalTimestamp,
          originSequenceNumber: op2.originSequenceNumber,
          causalContext: op2.causalContext,
          membershipEpoch: op2.membershipEpoch,
          payload: op2.payload,
        },
      });

      const op2Decision = engine.getLastDecision();
      expect(op2Decision?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(engine.pendingQueueSize).toBe(1);
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      // Now create AntiEntropyManager which reconciles pending gaps
      const manager = new AntiEntropyManager(engine, callbacks, {}, clock);
      manager.repairPipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-author',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 5 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      manager.reconcileGaps();
      expect(sentRequests.length).toBe(1);
      expect(sentRequests[0]!.request.fromSequence).toBe(1);
      expect(sentRequests[0]!.request.toSequence).toBe(1);

      const reqId = sentRequests[0]!.request.requestId;
      const op1Batch = makeSampleBatch(reqId, 'peer-author', 'peer-author', 1, 1);

      manager.handleRepairBatch(op1Batch, supplierContext);

      // Op 1 was admitted and unblocked Op 2 in cascade!
      expect(engine.replicationFrontier['peer-author']).toBe(2);
      expect(engine.pendingQueueSize).toBe(0);

      manager.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 10. Historical completeness invariant preservation
  // ───────────────────────────────────────────────────────────────────────────
  describe('10. Historical Completeness Invariant Preservation', () => {
    it('never reports IDENTICAL after repairing known missing ranges when intermediate history is unverified', () => {
      // Simulate two peers: local peer and remote peer-B
      // Both will reach replication vector { 'peer-author': 5 } with matching tail fingerprints
      engine.setPeerGeneration('peer-author', 1);
      engine.setPeerGeneration('peer-B', 1);
      const supplierContext = createAuthenticatedSupplierContext(engine, 'peer-author', 1, transportCap);

      const manager = new AntiEntropyManager(engine, callbacks, {}, clock);

      // Record peer knowledge and schedule repair for missing range 1..5
      manager.repairPipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-author',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 5 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      manager.repairPipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const reqId = sentRequests[0]!.request.requestId;

      // Ingest ops 1..5 into local engine fulfilling active request
      const batch = makeSampleBatch(reqId, 'peer-author', 'peer-author', 1, 5);
      const decisions = manager.handleRepairBatch(batch, supplierContext);
      expect(decisions.every((d) => d.accepted)).toBe(true);

      expect(engine.replicationFrontier['peer-author']).toBe(5);

      // Remote peer B sends digest with identical vector and identical tail fingerprint
      const localDigest = engine.createLocalDigest();
      const remoteDigest: StateDigestPayload = {
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-B',
        connectionGeneration: 1,
        membershipEpoch: 1,
        stateVersion: 1,
        sentAt: 1000,
        replicationVector: { ...localDigest.replicationVector },
        snapshotFrontier: { ...localDigest.snapshotFrontier },
        historyFingerprints: { ...localDigest.historyFingerprints },
      };

      const result: DigestComparisonResult = manager.processRemoteDigest(remoteDigest);

      // Crucial M2/M3 invariant verification:
      // Even though all missing ranges are 0, and vectors match, and tail fingerprints match,
      // because k = 5 (> 1) and only tail fingerprints exist without cumulative hashes,
      // the aggregate classification MUST BE FRONTIER_AGREEMENT_ONLY and hasUnverifiedHistory MUST BE true!
      // It MUST NEVER be falsely claimed as IDENTICAL.
      expect(result.missingFromLocal.length).toBe(0);
      expect(result.hasUnverifiedHistory).toBe(true);
      expect(result.classification).toBe('FRONTIER_AGREEMENT_ONLY');
      expect(result.classification).not.toBe('IDENTICAL');

      manager.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 11. Supplier Selection, Author Preference, and RTT Ranking
  // ───────────────────────────────────────────────────────────────────────────
  describe('11. Supplier Selection Algorithm', () => {
    it('strictly prefers the original author when the author is connected and eligible', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);

      engine.setPeerGeneration('peer-author', 1);
      engine.setPeerGeneration('peer-relay', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-relay',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-author',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);

      expect(sentRequests.length).toBe(1);
      expect(sentRequests[0]!.targetPeerId).toBe('peer-author');

      pipeline.dispose();
    });

    it('falls back to lowest SRTT peer when author is not eligible', () => {
      const rttMap: Record<string, number> = {
        'peer-fast': 25,
        'peer-slow': 150,
      };

      const pipeline = new RepairRequestPipeline(
        engine,
        callbacks,
        { rttProvider: (p) => rttMap[p] },
        clock,
      );

      engine.setPeerGeneration('peer-slow', 1);
      engine.setPeerGeneration('peer-fast', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-slow',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-fast',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);

      expect(sentRequests.length).toBe(1);
      expect(sentRequests[0]!.targetPeerId).toBe('peer-fast');

      pipeline.dispose();
    });

    it('enforces per-peer concurrency limit of 2 in-flight requests', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10, 'origin-2': 10, 'origin-3': 10 },
        snapshotFrontier: { 'origin-1': 0, 'origin-2': 0, 'origin-3': 0 },
        historyFingerprints: {
          'origin-1': '1'.repeat(64),
          'origin-2': '2'.repeat(64),
          'origin-3': '3'.repeat(64),
        },
      });

      pipeline.addGaps([
        { originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 },
        { originPeerId: 'origin-2', fromSequence: 1, toSequence: 5 },
        { originPeerId: 'origin-3', fromSequence: 1, toSequence: 5 },
      ]);

      // Only 2 requests should be sent to peer-S; the 3rd waits in pendingGaps
      expect(sentRequests.length).toBe(2);
      expect(pipeline.getStatus().activeRequestsCount).toBe(2);
      expect(pipeline.getStatus().pendingGapsCount).toBe(1);

      // Fulfill the first request
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const batch1 = makeSampleBatch(sentRequests[0]!.request.requestId, 'peer-S', 'origin-1', 1, 5);
      pipeline.handleRepairBatch(batch1, genuineContext);

      // Now the 3rd request should be dispatched!
      expect(sentRequests.length).toBe(3);
      expect(sentRequests[2]!.request.targetOriginPeerId).toBe('origin-3');

      pipeline.dispose();
    });

    it('handles peer disconnect by cancelling in-flight requests to that peer and failing over', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const departingPeer = sentRequests[0]!.targetPeerId;

      // Disconnect the peer serving the request
      pipeline.onPeerDisconnected(departingPeer);

      // Should failover to the other supplier
      expect(sentRequests.length).toBe(2);
      expect(sentRequests[1]!.targetPeerId).not.toBe(departingPeer);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 12. Adversarial Stale-Response and Correlation Safety Tests (Section 2)
  // ───────────────────────────────────────────────────────────────────────────
  describe('12. Adversarial Stale-Response and Pre-Ingestion Correlation Safety', () => {
    it('1. epoch transition followed by delayed old-epoch OPERATION_BATCH is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const oldReqId = sentRequests[0]!.request.requestId;

      // Authoritative epoch transition to epoch 2
      engine.transitionEpoch(2, epochCap);
      pipeline.onEpochTransition(2);

      // Now a delayed OPERATION_BATCH from epoch 1 arrives
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const staleBatch = makeSampleBatch(oldReqId, 'peer-S', 'peer-author', 1, 5, 1);

      const decisions = pipeline.handleRepairBatch(staleBatch, genuineContext);

      // Must be rejected with REJECTED_INVALID BEFORE mutating engine state
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('not an active in-flight request');

      // Local replication frontier in epoch 2 remains unmodified (0)
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.logSize).toBe(0);

      pipeline.dispose();
    });

    it('2. epoch transition followed by delayed old-epoch REPAIR_REJECTED cannot trigger retries or mutate state', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(1);
      const oldReqId = sentRequests[0]!.request.requestId;

      // Authoritative transition to epoch 2
      engine.transitionEpoch(2, epochCap);
      pipeline.onEpochTransition(2);

      // Delayed REPAIR_REJECTED arrives for the cancelled request
      const delayedRejection: RepairRejectedPayload = {
        requestId: oldReqId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: 10,
      };

      const delayedContext = engine.registerInboundSupplierContext('peer-S1', 1, transportCap);
      pipeline.handleRepairRejected(delayedRejection, delayedContext);

      // Must be dropped immediately: no retry dispatched, no failover in new epoch
      expect(sentRequests.length).toBe(1); // Still only the initial request from epoch 1
      expect(pipeline.getStatus().totalRepairsFailed).toBe(0);
      expect(pipeline.getStatus().degraded).toBe(false);
      expect(callbacks.onHistoryExhausted).not.toHaveBeenCalled();

      pipeline.dispose();
    });

    it('3. response for an expired request (after timeout) is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      // Request times out at 1500ms and is removed from inFlightRequests
      clock.advanceTime(1500);

      // Now the late response arrives with the expired requestId
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const lateBatch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

      const decisions = pipeline.handleRepairBatch(lateBatch, genuineContext);
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('not an active in-flight request');

      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });

    it('4. duplicate response after successful completion is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

      // First delivery: successfully fulfilled and retired from inFlightRequests
      const d1 = pipeline.handleRepairBatch(batch, genuineContext);
      expect(d1.every((d) => d.accepted)).toBe(true);
      expect(engine.replicationFrontier['peer-author']).toBe(5);

      // Duplicate delivery with the same requestId
      const d2 = pipeline.handleRepairBatch(batch, genuineContext);
      expect(d2.length).toBe(1);
      expect(d2[0]!.accepted).toBe(false);
      expect(d2[0]!.code).toBe('REJECTED_INVALID');
      expect(d2[0]!.message).toContain('not an active in-flight request');

      pipeline.dispose();
    });

    it('5. response with mismatched session ID is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const rogueSessionBatch = {
        ...makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5),
        sessionId: 'rogue-session-different',
      };

      const decisions = pipeline.handleRepairBatch(rogueSessionBatch, genuineContext);
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('does not match expected active session');

      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });

    it('6. response with mismatched membership epoch is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const wrongEpochBatch = {
        ...makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5),
        membershipEpoch: 99,
      };

      const decisions = pipeline.handleRepairBatch(wrongEpochBatch, genuineContext);
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('does not match active engine epoch');

      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });

    it('7. response with mismatched supplier identity or connection generation is rejected', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests[0]!.targetPeerId).toBe('peer-S1');
      const reqId = sentRequests[0]!.request.requestId;

      // S2 attempts to fulfill a request dispatched to S1
      const genuineContextS2 = createAuthenticatedSupplierContext(engine, 'peer-S2', 1, transportCap);
      const batchFromS2 = makeSampleBatch(reqId, 'peer-S2', 'peer-author', 1, 5);

      const d1 = pipeline.handleRepairBatch(batchFromS2, genuineContextS2);
      expect(d1.length).toBe(1);
      expect(d1[0]!.accepted).toBe(false);
      expect(d1[0]!.code).toBe('REJECTED_INVALID');
      expect(d1[0]!.message).toContain('does not match requested supplier');

      // Now test context with mismatched connection generation
      const staleContextS1 = engine.registerInboundSupplierContext('peer-S1', 999, transportCap);
      const batchFromS1 = makeSampleBatch(reqId, 'peer-S1', 'peer-author', 1, 5);

      const d2 = pipeline.handleRepairBatch(batchFromS1, staleContextS1);
      expect(d2.length).toBe(1);
      expect(d2[0]!.accepted).toBe(false);
      expect(d2[0]!.code).toBe('REJECTED_INVALID');
      expect(d2[0]!.message).toContain('does not match dispatch generation 1 or active peer generation 1');

      pipeline.dispose();
    });

    it('8. response with unrelated origin or sequence range outside requested bounds is rejected', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-A': 10, 'origin-B': 10 },
        snapshotFrontier: { 'origin-A': 0, 'origin-B': 0 },
        historyFingerprints: { 'origin-A': 'a'.repeat(64), 'origin-B': 'b'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-A', fromSequence: 5, toSequence: 10 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      // Response returns origin-B instead of origin-A
      const wrongOriginBatch = makeSampleBatch(reqId, 'peer-S', 'origin-B', 5, 10);
      const d1 = pipeline.handleRepairBatch(wrongOriginBatch, genuineContext);
      expect(d1.length).toBe(1);
      expect(d1[0]!.accepted).toBe(false);
      expect(d1[0]!.code).toBe('REJECTED_INVALID');
      expect(d1[0]!.message).toContain('does not match requested origin');

      // Response returns sequences extending outside requested range [5..10]
      const outOfBoundsBatch = makeSampleBatch(reqId, 'peer-S', 'origin-A', 4, 10); // starts at 4 instead of 5
      const d2 = pipeline.handleRepairBatch(outOfBoundsBatch, genuineContext);
      expect(d2.length).toBe(1);
      expect(d2[0]!.accepted).toBe(false);
      expect(d2[0]!.code).toBe('REJECTED_INVALID');
      expect(d2[0]!.message).toContain('extends outside requested range');

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 13. Advisory Supplier Knowledge Trust Boundary (Section 3)
  // ───────────────────────────────────────────────────────────────────────────
  describe('13. Advisory Supplier Knowledge Trust Boundary', () => {
    it('optimistic advertised replication vector cannot bypass inbound response validation or supplier authorization', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);

      // Peer advertises an inflated, optimistic replication vector
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-unauthorized-optimist',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 1000 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'f'.repeat(64) },
      });

      // 1. Peer cannot be selected as supplier because it lacks active connection generation in peerGenerations
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      expect(sentRequests.length).toBe(0); // Not eligible!

      // 2. If the peer sends an unsolicited OPERATION_BATCH directly:
      const unsolicitedBatch = makeSampleBatch(generateUUIDv4(), 'peer-unauthorized-optimist', 'peer-author', 1, 5);
      const decisions = pipeline.handleRepairBatch(unsolicitedBatch);

      // Rejected immediately: uncoordinated request ID
      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');

      // 3. Engine state remains unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.logSize).toBe(0);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 14. Global Retry Budget & Concurrency Invariants (Section 4)
  // ───────────────────────────────────────────────────────────────────────────
  describe('14. Global Retry Budget and Concurrency Invariants', () => {
    it('switching suppliers during failover consumes the single globally bounded retry budget', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      engine.setPeerGeneration('peer-S3', 1);
      engine.setPeerGeneration('peer-S4', 1);

      for (const peer of ['peer-S1', 'peer-S2', 'peer-S3', 'peer-S4']) {
        pipeline.recordPeerDigestKnowledge({
          formatVersion: 2,
          sessionId: 'test-session-1',
          senderPeerId: peer,
          connectionGeneration: 1,
          membershipEpoch: 1,
          replicationVector: { 'origin-1': 10 },
          snapshotFrontier: { 'origin-1': 0 },
          historyFingerprints: { 'origin-1': '1'.repeat(64) },
        });
      }

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);

      // Attempt 1: S1 (retryCount 0)
      expect(sentRequests.length).toBe(1);
      expect(sentRequests[0]!.targetPeerId).toBe('peer-S1');

      // S1 times out (1500ms) -> backoff (1000ms) -> Attempt 2: S2 (retryCount 1)
      clock.advanceTime(1500 + 1000);
      expect(sentRequests.length).toBe(2);
      expect(sentRequests[1]!.targetPeerId).toBe('peer-S2');

      // S2 times out (1500ms) -> backoff (2000ms) -> Attempt 3: S3 (retryCount 2)
      clock.advanceTime(1500 + 2000);
      expect(sentRequests.length).toBe(3);
      expect(sentRequests[2]!.targetPeerId).toBe('peer-S3');

      // S3 times out (1500ms) -> backoff (4000ms) -> Attempt 4: S4 (retryCount 3)
      clock.advanceTime(1500 + 4000);
      expect(sentRequests.length).toBe(4);
      expect(sentRequests[3]!.targetPeerId).toBe('peer-S4');

      // S4 times out (1500ms) -> exceeds maxRetries (3) -> Exhausted!
      clock.advanceTime(1500);

      expect(sentRequests.length).toBe(4);
      expect(pipeline.getStatus().totalRepairsFailed).toBe(1);
      expect(pipeline.getStatus().degraded).toBe(true);
      expect(pipeline.getStatus().degradedReason).toBe('MAX_RETRIES_EXCEEDED');
      expect(callbacks.onHistoryExhausted).toHaveBeenCalledWith({
        originPeerId: 'origin-1',
        missingSequence: 1,
        highestRetainedSequence: 0,
      });

      pipeline.dispose();
    });

    it('RATE_LIMITED and BUSY responses consume the logical retry budget', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      engine.setPeerGeneration('peer-S3', 1);
      engine.setPeerGeneration('peer-S4', 1);

      for (const peer of ['peer-S1', 'peer-S2', 'peer-S3', 'peer-S4']) {
        pipeline.recordPeerDigestKnowledge({
          formatVersion: 2,
          sessionId: 'test-session-1',
          senderPeerId: peer,
          connectionGeneration: 1,
          membershipEpoch: 1,
          replicationVector: { 'origin-1': 10 },
          snapshotFrontier: { 'origin-1': 0 },
          historyFingerprints: { 'origin-1': '1'.repeat(64) },
        });
      }

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);

      // Attempt 1 to S1 -> S1 rejects RATE_LIMITED
      const ctxS1 = engine.registerInboundSupplierContext('peer-S1', 1, transportCap);
      pipeline.handleRepairRejected({
        requestId: sentRequests[0]!.request.requestId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'RATE_LIMITED',
      }, ctxS1);

      // Backoff 1 (1000ms) -> Attempt 2 to S2
      clock.advanceTime(1000);
      expect(sentRequests.length).toBe(2);

      // S2 rejects BUSY
      const ctxS2 = engine.registerInboundSupplierContext('peer-S2', 1, transportCap);
      pipeline.handleRepairRejected({
        requestId: sentRequests[1]!.request.requestId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S2',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'BUSY',
      }, ctxS2);

      // Backoff 2 (2000ms) -> Attempt 3 to S3
      clock.advanceTime(2000);
      expect(sentRequests.length).toBe(3);

      pipeline.dispose();
    });

    it('simultaneous timeout, disconnect, and rejection cannot schedule duplicate retries', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);

      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'origin-1': 10 },
        snapshotFrontier: { 'origin-1': 0 },
        historyFingerprints: { 'origin-1': '1'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'origin-1', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      // Event 1: Peer S1 disconnects -> failover initiated
      pipeline.onPeerDisconnected('peer-S1');
      expect(sentRequests.length).toBe(2); // S2 dispatched

      // Event 2: Timeout timer for reqId fires late
      clock.advanceTime(2000);
      // Event 3: A rejection for reqId arrives late
      const ctxS1 = engine.registerInboundSupplierContext('peer-S1', 1, transportCap);
      pipeline.handleRepairRejected({
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'BUSY',
      }, ctxS1);

      // Prove that no duplicate retries were scheduled for reqId
      expect(sentRequests.length).toBe(2);

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 15. Partial Batch and Overlapping Correlation Safety (Section 5)
  // ───────────────────────────────────────────────────────────────────────────
  describe('15. Partial Batch and Overlapping Correlation Safety', () => {
    it('partial response followed by duplicate response, delayed response, and concurrent gap update', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 20 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      // Request 1..20
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 20 }]);
      expect(sentRequests.length).toBe(1);
      const reqId1 = sentRequests[0]!.request.requestId;

      // 1. Partial fulfillment: returns [1..10]
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);
      const partialBatch = makeSampleBatch(reqId1, 'peer-S', 'peer-author', 1, 10);

      const d1 = pipeline.handleRepairBatch(partialBatch, genuineContext);
      expect(d1.every((d) => d.accepted)).toBe(true);
      expect(engine.replicationFrontier['peer-author']).toBe(10);

      // Remaining range [11..20] is automatically dispatched with fresh reqId2
      expect(sentRequests.length).toBe(2);
      const reqId2 = sentRequests[1]!.request.requestId;
      expect(reqId2).not.toBe(reqId1);
      expect(sentRequests[1]!.request.fromSequence).toBe(11);
      expect(sentRequests[1]!.request.toSequence).toBe(20);

      // 2. Duplicate response arrives for reqId1 (the fulfilled partial batch)
      const dDup = pipeline.handleRepairBatch(partialBatch, genuineContext);
      expect(dDup.length).toBe(1);
      expect(dDup[0]!.accepted).toBe(false);
      expect(dDup[0]!.code).toBe('REJECTED_INVALID');
      expect(dDup[0]!.message).toContain('not an active in-flight request');

      // 3. Delayed response arrives for reqId1 with different operations
      const delayedBatch = makeSampleBatch(reqId1, 'peer-S', 'peer-author', 1, 10);
      const dDelayed = pipeline.handleRepairBatch(delayedBatch, genuineContext);
      expect(dDelayed.length).toBe(1);
      expect(dDelayed[0]!.accepted).toBe(false);
      expect(dDelayed[0]!.code).toBe('REJECTED_INVALID');

      // 4. Concurrent gap update arrives for [1..20]
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 20 }]);
      // [1..10] is dropped because replicationFrontier is already 10;
      // [11..20] is suppressed because reqId2 is already actively in-flight!
      expect(sentRequests.length).toBe(2); // No duplicate request dispatched!

      // 5. Complete fulfillment of reqId2 [11..20]
      const batch2 = makeSampleBatch(reqId2, 'peer-S', 'peer-author', 11, 20);
      const d2 = pipeline.handleRepairBatch(batch2, genuineContext);
      expect(d2.every((d) => d.accepted)).toBe(true);
      expect(engine.replicationFrontier['peer-author']).toBe(20);

      // Status verifies both parts completed
      expect(pipeline.getStatus().activeRequestsCount).toBe(0);
      expect(pipeline.getStatus().pendingGapsCount).toBe(0);
      expect(pipeline.getStatus().totalRepairsCompleted).toBe(1);

      pipeline.dispose();
    });

    it('response cannot advance sequence progress beyond the requested range', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 30 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      // Request 1..5
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      // Supplier tries to serve [1..10] when only [1..5] was requested
      const overreachingBatch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 10);
      const decisions = pipeline.handleRepairBatch(overreachingBatch, genuineContext);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('extends outside requested range');

      // Local engine remains unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 16. Mandatory Transport Authentication & Generation-Binding Adversarial Tests
  // ───────────────────────────────────────────────────────────────────────────
  describe('16. Mandatory Transport Authentication & Generation-Binding Adversarial Tests', () => {
    it('1. Inbound OPERATION_BATCH without AuthenticatedSupplierContext is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

      const decisions = pipeline.handleRepairBatch(batch, undefined as any);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'missing or invalid authenticated supplier transport context',
      );

      // Local engine remains completely unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.logSize).toBe(0);

      pipeline.dispose();
    });

    it('2. Inbound OPERATION_BATCH with fabricated AuthenticatedSupplierContext is rejected before engine ingestion', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

      const fabricatedContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1,
      };

      const decisions = pipeline.handleRepairBatch(batch, fabricatedContext as any);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'missing or invalid authenticated supplier transport context',
      );

      // Local engine remains completely unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.logSize).toBe(0);

      pipeline.dispose();
    });

    it('3. Inbound REPAIR_REJECTED without AuthenticatedSupplierContext is ignored and dropped', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'b'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const rejection: RepairRejectedPayload = {
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: 10,
      };

      // Inbound rejection with missing context
      pipeline.handleRepairRejected(rejection, undefined as any);

      // Rejection dropped: request is still active, no failover dispatched
      expect(sentRequests.length).toBe(1);
      expect(pipeline.getStatus().activeRequestsCount).toBe(1);
      expect(pipeline.getStatus().degraded).toBe(false);

      pipeline.dispose();
    });

    it('4. Inbound REPAIR_REJECTED with fabricated AuthenticatedSupplierContext is ignored and dropped', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'b'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      const rejection: RepairRejectedPayload = {
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: 'peer-S1',
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'OUTSIDE_HORIZON',
        retainedHorizonSequence: 10,
      };

      const fabricatedContext = {
        supplierPeerId: 'peer-S1',
        connectionGeneration: 1,
      };

      pipeline.handleRepairRejected(rejection, fabricatedContext as any);

      // Rejection dropped: request is still active, no failover dispatched
      expect(sentRequests.length).toBe(1);
      expect(pipeline.getStatus().activeRequestsCount).toBe(1);
      expect(pipeline.getStatus().degraded).toBe(false);

      pipeline.dispose();
    });

    it('5. REPAIR_REJECTED from peer X for request sent to peer Y is rejected and dropped', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'b'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;
      const targetSupplier = sentRequests[0]!.targetPeerId;
      const otherSupplier = targetSupplier === 'peer-S1' ? 'peer-S2' : 'peer-S1';

      // Genuine context registered for peer X, but request was sent to peer Y
      const otherCtx = engine.registerInboundSupplierContext(otherSupplier, 1, transportCap);
      const spoofedRejection: RepairRejectedPayload = {
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: otherSupplier,
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'RATE_LIMITED',
      };

      pipeline.handleRepairRejected(spoofedRejection, otherCtx);

      // Dropped: request to targeted supplier remains in flight
      expect(sentRequests.length).toBe(1);
      expect(pipeline.getStatus().activeRequestsCount).toBe(1);

      pipeline.dispose();
    });

    it('6. Request sent to peer on generation 3; peer reconnects to generation 4; response on gen 4 for gen-3 request is rejected', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 3);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 3,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqIdGen3 = sentRequests[0]!.request.requestId;

      // Peer reconnects to generation 4
      engine.setPeerGeneration('peer-S', 4);

      // Generating context for generation 4
      const gen4Ctx = engine.registerInboundSupplierContext('peer-S', 4, transportCap);

      // Inbound response arrives with gen-4 context for the old gen-3 request
      const staleBatch = makeSampleBatch(reqIdGen3, 'peer-S', 'peer-author', 1, 5);
      const decisions = pipeline.handleRepairBatch(staleBatch, gen4Ctx);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('not an active in-flight request');

      // Engine remains unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.logSize).toBe(0);

      pipeline.dispose();
    });

    it('7. Response arrives with context generation differing from dispatch generation is rejected', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      // Register context with generation 2 (dispatch was generation 1)
      const gen2Ctx = engine.registerInboundSupplierContext('peer-S', 2, transportCap);
      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

      const decisions = pipeline.handleRepairBatch(batch, gen2Ctx);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'does not match dispatch generation 1 or active peer generation 1',
      );

      // Local engine remains unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });

    it('8. Response arrives with context generation differing from engine active generation is rejected', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqId = sentRequests[0]!.request.requestId;

      // Register context with generation 1 (matches dispatch generation)
      const gen1Ctx = engine.registerInboundSupplierContext('peer-S', 1, transportCap);

      // Engine active generation transitions to 2 directly on the map (simulating state shift)
      engine.setPeerGeneration('peer-S', 2);

      const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);
      const decisions = pipeline.handleRepairBatch(batch, gen1Ctx);

      expect(decisions.length).toBe(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');

      // Local engine remains unmutated
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();

      pipeline.dispose();
    });

    it('9. REPAIR_REJECTED arriving after peer reconnect with old generation is ignored', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S1', 1);
      engine.setPeerGeneration('peer-S2', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S1',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S2',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 10 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'b'.repeat(64) },
      });

      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 5 }]);
      const reqIdGen1 = sentRequests[0]!.request.requestId;
      const firstSupplier = sentRequests[0]!.targetPeerId;

      // Peer reconnects to generation 2
      engine.setPeerGeneration(firstSupplier, 2);

      // Old rejection arrives for gen-1 request with gen-1 context
      const gen1Ctx = engine.registerInboundSupplierContext(firstSupplier, 1, transportCap);
      const oldRejection: RepairRejectedPayload = {
        requestId: reqIdGen1,
        sessionId: 'test-session-1',
        supplierPeerId: firstSupplier,
        targetOriginPeerId: 'peer-author',
        fromSequence: 1,
        toSequence: 5,
        reason: 'RATE_LIMITED',
      };

      pipeline.handleRepairRejected(oldRejection, gen1Ctx);

      // Stale rejection was dropped without mutating retry count or triggering failover
      expect(pipeline.getStatus().degraded).toBe(false);

      pipeline.dispose();
    });

    it('10. Partial response [10, 20] for request [1, 32] preserves [1, 9] in repair queue and converges once [1, 9] arrives', () => {
      const pipeline = new RepairRequestPipeline(engine, callbacks, {}, clock);
      engine.setPeerGeneration('peer-S', 1);
      pipeline.recordPeerDigestKnowledge({
        formatVersion: 2,
        sessionId: 'test-session-1',
        senderPeerId: 'peer-S',
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { 'peer-author': 50 },
        snapshotFrontier: { 'peer-author': 0 },
        historyFingerprints: { 'peer-author': 'a'.repeat(64) },
      });

      // Request 1..32
      pipeline.addGaps([{ originPeerId: 'peer-author', fromSequence: 1, toSequence: 32 }]);
      expect(sentRequests.length).toBe(1);
      const reqId1 = sentRequests[0]!.request.requestId;
      expect(sentRequests[0]!.request.fromSequence).toBe(1);
      expect(sentRequests[0]!.request.toSequence).toBe(32);

      const supplierCtx = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      // Supplier returns partial response [10..20] (missing prefix 1..9)
      const partialBatch10_20 = makeSampleBatch(reqId1, 'peer-S', 'peer-author', 10, 20);
      const d1 = pipeline.handleRepairBatch(partialBatch10_20, supplierCtx);

      // Operations 10..20 are buffered in PendingDependencyQueue awaiting 1..9
      expect(engine.replicationFrontier['peer-author']).toBeUndefined();
      expect(engine.pendingQueueSize).toBe(11); // 10..20 buffered

      // Crucial Safety Property: Unfulfilled prefix [1, 9] was NOT lost!
      // Remainder was recalculated from actual replication frontier (0 + 1 = 1) up to targetEnd (32).
      // A new request was automatically dispatched for [1..32].
      expect(sentRequests.length).toBe(2);
      const reqId2 = sentRequests[1]!.request.requestId;
      expect(sentRequests[1]!.request.fromSequence).toBe(1);
      expect(sentRequests[1]!.request.toSequence).toBe(32);

      // Supplier now fulfills prefix [1..9]
      const batch1_9 = makeSampleBatch(reqId2, 'peer-S', 'peer-author', 1, 9);
      const d2 = pipeline.handleRepairBatch(batch1_9, supplierCtx);
      expect(d2.every((d) => d.accepted)).toBe(true);

      // Operations 1..9 admitted, which unblocked and drained buffered operations 10..20!
      expect(engine.replicationFrontier['peer-author']).toBe(20);
      expect(engine.pendingQueueSize).toBe(0);

      // Suffix remainder [21..32] was automatically recalculated and dispatched
      expect(sentRequests.length).toBe(3);
      const reqId3 = sentRequests[2]!.request.requestId;
      expect(sentRequests[2]!.request.fromSequence).toBe(21);
      expect(sentRequests[2]!.request.toSequence).toBe(32);

      // Supplier now fulfills suffix [21..32]
      const batch21_32 = makeSampleBatch(reqId3, 'peer-S', 'peer-author', 21, 32);
      const d3 = pipeline.handleRepairBatch(batch21_32, supplierCtx);
      expect(d3.every((d) => d.accepted)).toBe(true);

      // All 32 operations are fully applied and frontier reached 32
      expect(engine.replicationFrontier['peer-author']).toBe(32);
      expect(engine.logSize).toBe(32);
      expect(pipeline.getStatus().activeRequestsCount).toBe(0);
      expect(pipeline.getStatus().pendingGapsCount).toBe(0);
      expect(pipeline.getStatus().totalRepairsCompleted).toBe(1);
      expect(pipeline.getStatus().degraded).toBe(false);

      pipeline.dispose();
    });
  });
});
