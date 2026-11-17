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

      pipeline.handleRepairRejected(rejection);

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

      pipeline.handleRepairRejected(rejection);

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
      pipeline.handleRepairRejected({
        requestId: reqId,
        sessionId: 'test-session-1',
        supplierPeerId: sentRequests[0]!.targetPeerId,
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        reason: 'RATE_LIMITED',
      });

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

      // Ingest ops 1..5 into local engine
      const batch = makeSampleBatch(generateUUIDv4(), 'peer-author', 'peer-author', 1, 5);
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
});
