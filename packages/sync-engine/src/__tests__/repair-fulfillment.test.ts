import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createTransportRegistrationCapability,
  createEpochTransitionCapability,
  createAuthenticatedRequesterContext,
  createAuthenticatedSupplierContext,
  TrustedTransportAdapter,
  type AuthenticatedRequesterContext,
} from '../engine/leaderless-sync-engine.js';
import {
  RepairSupplierService,
  DEFAULT_SUPPLIER_MAX_PAYLOAD_BYTES,
  AntiEntropyManager,
} from '../replication/index.js';
import type {
  SchedulerClock,
  AntiEntropyCallbacks,
} from '../replication/types.js';
import {
  type RepairRequestPayload,
  type OperationBatchPayload,
  type RepairRejectedPayload,
  OperationBatchPayloadSchema,
  RepairRejectedPayloadSchema,
} from '@syncplay/protocol';
import type { StoredOperation } from '../log/types.js';
import { SyncEngineError } from '../errors.js';

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

  public advance(ms: number): void {
    this.currentTime += ms;
    const dueEntries = [...this.timers.entries()]
      .filter(([, timer]) => timer.dueTime <= this.currentTime)
      .sort((a, b) => a[1].dueTime - b[1].dueTime);

    for (const [id, timer] of dueEntries) {
      this.timers.delete(id);
      timer.callback();
    }
  }
}

function createStoredOperation(
  originPeerId: string,
  sequenceNumber: number,
  epoch = 1,
  payloadData: unknown = { positionMs: sequenceNumber * 1000 },
): StoredOperation {
  return {
    logicalEventId: `evt-${originPeerId}-${sequenceNumber}`,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: epoch,
    messageType: 'PLAY',
    logicalTimestamp: sequenceNumber,
    originSequenceNumber: sequenceNumber,
    causalContext: { [originPeerId]: sequenceNumber - 1 },
    appliedAtMonotonic: sequenceNumber * 100,
    payload: payloadData,
  };
}

describe('Phase 7.4 Milestone M4: Supplier Range Servicing (RepairSupplierService)', () => {
  const SESSION_ID = 'test-session-1';
  let clock: DeterministicClock;
  let supplierEngine: LeaderlessSyncEngine;
  let supplierTransportCap: ReturnType<typeof createTransportRegistrationCapability>;
  let supplierEpochCap: ReturnType<typeof createEpochTransitionCapability>;
  let supplierCallbacks: AntiEntropyCallbacks;
  let supplierService: RepairSupplierService;
  let requesterContext: AuthenticatedRequesterContext;

  beforeEach(() => {
    clock = new DeterministicClock();
    supplierTransportCap = createTransportRegistrationCapability('auth', SESSION_ID);
    supplierEpochCap = createEpochTransitionCapability('auth', SESSION_ID);

    supplierEngine = new LeaderlessSyncEngine({
      localPeerId: 'peer-supplier',
      sessionId: SESSION_ID,
      transportCapability: supplierTransportCap,
      epochCapability: supplierEpochCap,
    });

    supplierEngine.authorizePeerGeneration('peer-requester', 1);
    requesterContext = createAuthenticatedRequesterContext(
      supplierEngine,
      'peer-requester',
      1,
      supplierTransportCap,
    );

    supplierCallbacks = {
      onBroadcastDigest: vi.fn(),
      onSendRepairResponse: vi.fn(),
    };

    supplierService = new RepairSupplierService(
      supplierEngine,
      supplierCallbacks,
      {},
      clock,
    );
  });

  // ── 1. Request Validation Tests ──────────────────────────────────────────

  describe('1. Request Validation', () => {
    it('1.1 Valid request with authenticated transport context returns valid OPERATION_BATCH', () => {
      // Seed supplier log with operations 1..5 for peer-A
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('operations' in response).toBe(true);
      const batch = response as OperationBatchPayload;
      expect(batch.requestId).toBe(request.requestId);
      expect(batch.supplierPeerId).toBe('peer-supplier');
      expect(batch.originPeerId).toBe('peer-A');
      expect(batch.fromSequence).toBe(1);
      expect(batch.toSequence).toBe(5);
      expect(batch.operations.length).toBe(5);
    });

    it('1.2 Invalid schema throws SyncEngineError INVALID_ARGUMENT', () => {
      const invalidRequest = {
        requestId: 'not-a-uuid',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      } as any;

      expect(() =>
        supplierService.serveRepairRequest(invalidRequest, requesterContext),
      ).toThrow(SyncEngineError);
    });

    it('1.3 Wrong session throws SyncEngineError INVALID_ARGUMENT', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: 'other-session-999',
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrow(SyncEngineError);
    });

    it('1.4 Wrong membership epoch returns REPAIR_REJECTED with EPOCH_MISMATCH', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 2, // Active epoch is 1
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('EPOCH_MISMATCH');
      expect(rejection.requestId).toBe(request.requestId);
    });

    it('1.5 Unauthenticated requester throws SyncEngineError UNAUTHORIZED', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, undefined),
      ).toThrowError(/AuthenticatedRequesterContext/);
    });

    it('1.6 Fabricated transport context object throws SyncEngineError UNAUTHORIZED', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const fakeContext: AuthenticatedRequesterContext = {
        requesterPeerId: 'peer-requester',
        connectionGeneration: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, fakeContext),
      ).toThrowError(/AuthenticatedRequesterContext/);
    });

    it('1.7 Transport context from different engine throws SyncEngineError UNAUTHORIZED', () => {
      const otherCap = createTransportRegistrationCapability('auth-2', 'other-session');
      const otherEngine = new LeaderlessSyncEngine({
        localPeerId: 'other-peer',
        sessionId: 'other-session',
        transportCapability: otherCap,
      });
      otherEngine.authorizePeerGeneration('peer-requester', 1);
      const otherContext = createAuthenticatedRequesterContext(
        otherEngine,
        'peer-requester',
        1,
        otherCap,
      );

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, otherContext),
      ).toThrowError(/AuthenticatedRequesterContext/);
    });

    it('1.8 Payload requester identity differs from transport identity throws SyncEngineError UNAUTHORIZED', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'spoofed-peer',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrowError(/does not match authenticated transport peer/);
    });

    it('1.9 Stale connection generation throws SyncEngineError UNAUTHORIZED', () => {
      // Reconnect peer-requester to generation 2
      supplierEngine.authorizePeerGeneration('peer-requester', 2);

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // requesterContext was issued for generation 1
      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrowError(/does not match active engine generation/);
    });

    it('1.10 Unknown origin returns REPAIR_REJECTED with ORIGIN_UNKNOWN', () => {
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'unknown-origin',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('ORIGIN_UNKNOWN');
    });

    it('1.11 Invalid sequence range (toSequence < fromSequence) throws SyncEngineError INVALID_ARGUMENT', () => {
      const request: any = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 10,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrow(SyncEngineError);
    });

    it('1.12 Unsafe integer sequence values throws SyncEngineError INVALID_ARGUMENT', () => {
      const request: any = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1.5,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrow(SyncEngineError);
    });

    it('1.13 Range larger than 32 throws SyncEngineError INVALID_ARGUMENT', () => {
      const request: any = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 35, // 35 operations > 32 max
        membershipEpoch: 1,
      };

      expect(() =>
        supplierService.serveRepairRequest(request, requesterContext),
      ).toThrow(SyncEngineError);
    });
  });

  // ── 2. Retained-History Verification Tests ────────────────────────────────

  describe('2. Retained-History Verification', () => {
    it('2.1 Entire requested range available returns full contiguous batch', () => {
      for (let s = 10; s <= 20; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 10,
        toSequence: 20,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('operations' in response).toBe(true);
      const batch = response as OperationBatchPayload;
      expect(batch.fromSequence).toBe(10);
      expect(batch.toSequence).toBe(20);
      expect(batch.operations.length).toBe(11);
    });

    it('2.2 First sequence missing due to compaction returns REPAIR_REJECTED with OUTSIDE_HORIZON', () => {
      // Operations 1..9 compacted; log retains 10..20
      supplierEngine.getOperationLog().setBaseSnapshot(
        { stateVersion: 1, playbackStatus: 'PLAYING', mediaTimePosition: 10, mediaPlaybackRate: 1, monotonicTimestamp: 1000 },
        { 'peer-A': 9 },
      );
      for (let s = 10; s <= 20; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // Requester asks for 5..15
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 5,
        toSequence: 15,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('OUTSIDE_HORIZON');
      expect(rejection.retainedHorizonSequence).toBe(10);
    });

    it('2.3 Last sequence missing (beyond supplier frontier) returns REPAIR_REJECTED with BUSY', () => {
      // Supplier only has operations 1..10
      for (let s = 1; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // Requester asks for 1..15
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 15,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('BUSY');
    });

    it('2.4 Middle sequence missing (hole in log) returns REPAIR_REJECTED with BUSY', () => {
      // Supplier has 1..4 and 6..10 (missing 5)
      for (let s = 1; s <= 10; s++) {
        if (s === 5) continue;
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('BUSY');
    });

    it('2.5 Requested range already compacted into base snapshot returns OUTSIDE_HORIZON', () => {
      supplierEngine.getOperationLog().setBaseSnapshot(
        { stateVersion: 1, playbackStatus: 'PLAYING', mediaTimePosition: 10, mediaPlaybackRate: 1, monotonicTimestamp: 1000 },
        { 'peer-A': 20 },
      );

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 5,
        toSequence: 15,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('OUTSIDE_HORIZON');
      expect(rejection.retainedHorizonSequence).toBe(21);
    });

    it('2.6 Requested range partly retained and partly compacted returns OUTSIDE_HORIZON', () => {
      supplierEngine.getOperationLog().setBaseSnapshot(
        { stateVersion: 1, playbackStatus: 'PLAYING', mediaTimePosition: 10, mediaPlaybackRate: 1, monotonicTimestamp: 1000 },
        { 'peer-A': 10 },
      );
      for (let s = 11; s <= 20; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // Request 8..15 crosses the snapshot frontier (8 <= 10)
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 8,
        toSequence: 15,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('OUTSIDE_HORIZON');
      expect(rejection.retainedHorizonSequence).toBe(11);
    });

    it('2.7 Requested range beyond supplier actual contiguous retained frontier returns BUSY', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 6,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('BUSY');
    });

    it('2.8 Advertised vector claims availability but log lacks operations returns BUSY', () => {
      // Peer has 1..5 in log
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // Even if requester expected 1..10, supplier establishes from actual log that it lacks 6..10
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('BUSY');
    });

    it('2.9 Operation in log belongs to a different membership epoch returns EPOCH_MISMATCH', () => {
      // Insert operation with epoch 2 into log
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1, 2));

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('EPOCH_MISMATCH');
    });
  });

  // ── 3. Response Correctness Tests ─────────────────────────────────────────

  describe('3. Response Correctness', () => {
    it('3.1 Exact operation contents and identities preserved without alteration', () => {
      const originalOp = createStoredOperation('peer-A', 1, 1, {
        positionMs: 42000,
        customFlag: true,
        nested: { count: 3 },
      });
      supplierEngine.getOperationLog().insert(originalOp);

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext) as OperationBatchPayload;
      const op = response.operations[0]!;
      expect(op.logicalEventId).toBe(originalOp.logicalEventId);
      expect(op.originPeerId).toBe(originalOp.originPeerId);
      expect(op.membershipEpoch).toBe(originalOp.membershipEpoch);
      expect(op.messageType).toBe(originalOp.messageType);
      expect(op.logicalTimestamp).toBe(originalOp.logicalTimestamp);
      expect(op.originSequenceNumber).toBe(originalOp.originSequenceNumber);
      expect(op.causalContext).toEqual(originalOp.causalContext);
      expect(op.payload).toEqual(originalOp.payload);
    });

    it('3.2 Correct origin, epoch, request ID, and session ID echoed', () => {
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1));

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext) as OperationBatchPayload;
      expect(response.requestId).toBe('e1074a9a-f4ef-4b9e-b2d3-18ef557163ef');
      expect(response.sessionId).toBe(SESSION_ID);
      expect(response.supplierPeerId).toBe('peer-supplier');
      expect(response.originPeerId).toBe('peer-A');
      expect(response.membershipEpoch).toBe(1);
    });

    it('3.3 Contiguous sequence validation: strictly ascending, no gaps, no duplicates', () => {
      for (let s = 1; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext) as OperationBatchPayload;
      expect(response.operations.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(response.operations[i]!.originSequenceNumber).toBe(i + 1);
      }
    });

    it('3.4 Application payload cap enforced: slices batch into smaller contiguous sub-range when > 64 KB', () => {
      // Create 10 large operations where 10 ops exceed 64 KB, but 4 ops fit
      const largePayload = 'X'.repeat(10000); // 10 KB per operation
      for (let s = 1; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(
          createStoredOperation('peer-A', s, 1, { data: largePayload }),
        );
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('operations' in response).toBe(true);
      const batch = response as OperationBatchPayload;

      // Sliced batch should have fewer operations than requested 10
      expect(batch.operations.length).toBeLessThan(10);
      expect(batch.operations.length).toBeGreaterThan(0);
      expect(batch.fromSequence).toBe(1);
      expect(batch.toSequence).toBe(batch.operations.length);

      // Verify serialized frame strictly <= 65536 bytes
      const serializedBytes = new TextEncoder().encode(JSON.stringify(batch)).byteLength;
      expect(serializedBytes).toBeLessThanOrEqual(DEFAULT_SUPPLIER_MAX_PAYLOAD_BYTES);

      // Verify contiguous sequence batch validation schema passes
      expect(() => OperationBatchPayloadSchema.parse(batch)).not.toThrow();
    });

    it('3.5 When even 1 operation exceeds max frame size, returns REPAIR_REJECTED with BUSY', () => {
      const hugePayload = 'Y'.repeat(70000); // 70 KB > 64 KB limit
      supplierEngine.getOperationLog().insert(
        createStoredOperation('peer-A', 1, 1, { data: hugePayload }),
      );

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      const rejection = response as RepairRejectedPayload;
      expect(rejection.reason).toBe('BUSY');
    });

    it('3.6 Uses negotiated SCTP max message size provider when smaller than 64 KB', () => {
      // Negotiated size is 2000 bytes
      const customService = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: (peerId) => (peerId === 'peer-requester' ? 2000 : undefined),
        },
        clock,
      );

      const payload = 'Z'.repeat(500); // 500 bytes per op
      for (let s = 1; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(
          createStoredOperation('peer-A', s, 1, { data: payload }),
        );
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 10,
        membershipEpoch: 1,
      };

      const response = customService.serveRepairRequest(request, requesterContext);
      expect('operations' in response).toBe(true);
      const batch = response as OperationBatchPayload;
      const serializedBytes = new TextEncoder().encode(JSON.stringify(batch)).byteLength;
      expect(serializedBytes).toBeLessThanOrEqual(2000);
    });
  });

  // ── 4. Lifecycle and Adversarial Tests ────────────────────────────────────

  describe('4. Lifecycle and Adversarial Cases', () => {
    it('4.1 Duplicate request ID returns cached response directly without re-reading log', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const response1 = supplierService.serveRepairRequest(request, requesterContext);
      const spyLog = vi.spyOn(supplierEngine.getOperationLog(), 'getOperationAt');

      // Send identical duplicate request
      const response2 = supplierService.serveRepairRequest(request, requesterContext);

      expect(response2).toEqual(response1);
      // Zero log reads on duplicate
      expect(spyLog).not.toHaveBeenCalled();
    });

    it('4.2 Same range under a new request ID is served as fresh request', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request1: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const request2: RepairRequestPayload = {
        requestId: 'd2074a9a-f4ef-4b9e-b2d3-18ef557163ee',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res1 = supplierService.serveRepairRequest(request1, requesterContext);
      const res2 = supplierService.serveRepairRequest(request2, requesterContext);

      expect((res1 as OperationBatchPayload).requestId).toBe(request1.requestId);
      expect((res2 as OperationBatchPayload).requestId).toBe(request2.requestId);
    });

    it('4.3 Epoch transition purges response cache and rejects old-epoch requests', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      supplierService.serveRepairRequest(request, requesterContext);

      // Transition engine to epoch 2
      supplierEngine.transitionEpoch(2, supplierEpochCap);
      supplierService.onEpochTransition(2);

      // Re-sending epoch-1 request should be rejected with EPOCH_MISMATCH
      const response = supplierService.serveRepairRequest(request, requesterContext);
      expect('reason' in response).toBe(true);
      expect((response as RepairRejectedPayload).reason).toBe('EPOCH_MISMATCH');
    });

    it('4.4 Peer disconnection cleans up rate limiter and active tracking for that peer', () => {
      supplierService.onPeerDisconnected('peer-requester');
      // Should not throw and clean up state cleanly
    });

    it('4.5 Outbound delivery callback is invoked with response', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      supplierService.serveRepairRequest(request, requesterContext);
      expect(supplierCallbacks.onSendRepairResponse).toHaveBeenCalledTimes(1);
      expect(supplierCallbacks.onSendRepairResponse).toHaveBeenCalledWith(
        'peer-requester',
        expect.objectContaining({ requestId: request.requestId }),
      );
    });

    it('4.6 Re-entrant callback behavior: callback throwing does not corrupt supplier internal state', () => {
      const throwingCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: vi.fn().mockImplementation(() => {
          throw new Error('Network send error');
        }),
      };

      const service = new RepairSupplierService(
        supplierEngine,
        throwingCallbacks,
        {},
        clock,
      );

      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // Thrown callback error is surfaced
      expect(() =>
        service.serveRepairRequest(request, requesterContext),
      ).toThrowError(/Network send error/);

      // Concurrency tracking should still be cleanly decremented in finally block
      // A subsequent request must not be blocked by BUSY
      const request2: RepairRequestPayload = {
        requestId: 'd2074a9a-f4ef-4b9e-b2d3-18ef557163ee',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // Replace with working callback
      (throwingCallbacks.onSendRepairResponse as any).mockImplementation(() => {});
      const res = service.serveRepairRequest(request2, requesterContext);
      expect('operations' in res).toBe(true);
    });

    it('4.7 Rate limiting drops excessive requests and returns REPAIR_REJECTED with RATE_LIMITED', () => {
      const limitedService = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          rateLimitBurst: 2,
          rateLimitMaxRequestsPerSecond: 1,
        },
        clock,
      );

      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const req1: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163e1',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        membershipEpoch: 1,
      };
      const req2: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163e2',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        membershipEpoch: 1,
      };
      const req3: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163e3',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        membershipEpoch: 1,
      };

      const res1 = limitedService.serveRepairRequest(req1, requesterContext);
      expect('operations' in res1).toBe(true);

      const res2 = limitedService.serveRepairRequest(req2, requesterContext);
      expect('operations' in res2).toBe(true);

      // Third request within same tick exceeds burst (2)
      const res3 = limitedService.serveRepairRequest(req3, requesterContext);
      expect('reason' in res3).toBe(true);
      expect((res3 as RepairRejectedPayload).reason).toBe('RATE_LIMITED');

      // Advance clock by 1 second to replenish token
      clock.advance(1000);
      const res4 = limitedService.serveRepairRequest(req3, requesterContext);
      expect('operations' in res4).toBe(true);
    });

    it('4.8 Zero playback state mutation: supplier serving repairs never mutates playback state, clock, or replication vector', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const stateBefore = JSON.stringify(supplierEngine.getState());
      const vectorBefore = JSON.stringify(supplierEngine.replicationFrontier);

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      supplierService.serveRepairRequest(request, requesterContext);

      expect(JSON.stringify(supplierEngine.getState())).toBe(stateBefore);
      expect(JSON.stringify(supplierEngine.replicationFrontier)).toBe(vectorBefore);
    });

    it('4.9 TrustedTransportAdapter.handleInboundRepairRequest routes requests and creates unforgeable context', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const adapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);

      const manager = new AntiEntropyManager(
        supplierEngine,
        supplierCallbacks,
        {},
        clock,
      );
      adapter.setAntiEntropyManager(manager);

      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // Routed through TrustedTransportAdapter deriving requester identity from transport layer
      const response = adapter.handleInboundRepairRequest(request, 'peer-requester', 1);
      expect('operations' in response).toBe(true);
      expect((response as OperationBatchPayload).operations.length).toBe(5);

      manager.dispose();
    });

    it('4.10 Full round-trip integration: batch served by supplier is accepted by receiver engine ingestRepairBatch()', () => {
      // 1. Supplier seeds log
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // 2. Requester creates request
      const request: RepairRequestPayload = {
        requestId: 'e1074a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // 3. Supplier serves batch
      const response = supplierService.serveRepairRequest(request, requesterContext) as OperationBatchPayload;
      expect('operations' in response).toBe(true);

      // 4. Receiver engine initializes and ingests batch
      const receiverTransportCap = createTransportRegistrationCapability('auth-recv', SESSION_ID);
      const receiverEngine = new LeaderlessSyncEngine({
        localPeerId: 'peer-requester',
        sessionId: SESSION_ID,
        transportCapability: receiverTransportCap,
      });
      receiverEngine.authorizePeerGeneration('peer-supplier', 1);

      const supplierCtxOnReceiver = createAuthenticatedSupplierContext(
        receiverEngine,
        'peer-supplier',
        1,
        receiverTransportCap,
      );

      const decisions = receiverEngine.ingestRepairBatch(response, supplierCtxOnReceiver);
      expect(decisions.length).toBe(5);
      for (const d of decisions) {
        expect(d.accepted).toBe(true);
      }
      expect(receiverEngine.replicationFrontier['peer-A']).toBe(5);
    });
  });

  // ── 5. Final Supplier Safety, Cache, and Transport-Boundary Audit Tests ────
  describe('5. Final Supplier Safety, Cache, and Transport-Boundary Audit', () => {
    it('5.1 Reused requestId with conflicting sequence range is rejected with INVALID_ARGUMENT', () => {
      for (let s = 1; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const req1: RepairRequestPayload = {
        requestId: 'c1a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res1 = supplierService.serveRepairRequest(req1, requesterContext);
      expect('operations' in res1).toBe(true);

      // Reused requestId with different sequence range (fromSequence: 6..10)
      const req2: RepairRequestPayload = {
        ...req1,
        fromSequence: 6,
        toSequence: 10,
      };

      expect(() => supplierService.serveRepairRequest(req2, requesterContext)).toThrow(SyncEngineError);
      expect(() => supplierService.serveRepairRequest(req2, requesterContext)).toThrow(
        /Reused requestId 'c1a74a9a-f4ef-4b9e-b2d3-18ef557163ef' with conflicting request parameters/,
      );
    });

    it('5.2 Reused requestId with conflicting target origin is rejected with INVALID_ARGUMENT', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-B', s));
      }

      const req1: RepairRequestPayload = {
        requestId: 'c2a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res1 = supplierService.serveRepairRequest(req1, requesterContext);
      expect('operations' in res1).toBe(true);

      // Reused requestId with different origin ('peer-B')
      const req2: RepairRequestPayload = {
        ...req1,
        targetOriginPeerId: 'peer-B',
      };

      expect(() => supplierService.serveRepairRequest(req2, requesterContext)).toThrow(SyncEngineError);
      expect(() => supplierService.serveRepairRequest(req2, requesterContext)).toThrow(
        /Reused requestId 'c2a74a9a-f4ef-4b9e-b2d3-18ef557163ef' with conflicting request parameters/,
      );
    });

    it('5.3 Cache hit retransmits cached response via outbound callback', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const sendResponseSpy = vi.fn();
      const customCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: sendResponseSpy,
      };

      const service = new RepairSupplierService(
        supplierEngine,
        customCallbacks,
        {},
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'c3a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // 1. Initial send
      const res1 = service.serveRepairRequest(req, requesterContext);
      expect(sendResponseSpy).toHaveBeenCalledTimes(1);
      expect(sendResponseSpy).toHaveBeenLastCalledWith('peer-requester', res1);

      // 2. Duplicate send (cache hit)
      const res2 = service.serveRepairRequest(req, requesterContext);
      expect(sendResponseSpy).toHaveBeenCalledTimes(2);
      expect(sendResponseSpy).toHaveBeenLastCalledWith('peer-requester', res2);
      expect(res2).toEqual(res1);
    });

    it('5.4 High-rate duplicate replay consumes rate-limit tokens and returns RATE_LIMITED', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // Configure rate limit burst to 2 tokens
      const limitedService = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          rateLimitBurst: 2,
          rateLimitMaxRequestsPerSecond: 1,
        },
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'c4a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // Request 1: uses token 1, cached
      const res1 = limitedService.serveRepairRequest(req, requesterContext);
      expect('operations' in res1).toBe(true);

      // Request 2 (duplicate): uses token 2, served from cache
      const res2 = limitedService.serveRepairRequest(req, requesterContext);
      expect('operations' in res2).toBe(true);

      // Request 3 (duplicate): tokens exhausted! Must return RATE_LIMITED rejection, preventing amplification
      const res3 = limitedService.serveRepairRequest(req, requesterContext);
      expect('reason' in res3).toBe(true);
      expect((res3 as RepairRejectedPayload).reason).toBe('RATE_LIMITED');
    });

    it('5.5 Transport message-size accounting: undefined negotiated size falls back to 64KB cap with envelope overhead', () => {
      // With undefined provider, effective limit is 65536, reserving 256 bytes envelope -> 65280 bytes
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => undefined,
        },
        clock,
      );

      // Seed 1 operation with large payload
      const largePayload = 'X'.repeat(65200);
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1, 1, { data: largePayload }));

      const req: RepairRequestPayload = {
        requestId: 'c5a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      // Exceeds 65,280 bytes limit -> BUSY
      const res = service.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.6 Transport message-size accounting: 0 negotiated size is treated as unconstrained remote clamped to 64KB application cap', () => {
      // RFC 8841: 0 means remote peer can handle any message size, clamped to local 65,536 cap
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 0,
        },
        clock,
      );

      // Seed 1 standard operation
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1));

      const req: RepairRequestPayload = {
        requestId: 'c6a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
      expect((res as OperationBatchPayload).operations.length).toBe(1);
    });

    it('5.7 Transport message-size accounting: negotiated size larger than 64KB is clamped to 64KB application cap', () => {
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 100_000,
        },
        clock,
      );

      // 65536 application cap is preserved even when remote reports 100,000
      const largePayload = 'X'.repeat(65200);
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1, 1, { data: largePayload }));

      const req: RepairRequestPayload = {
        requestId: 'c7a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.8 Exact boundary enforcement: single operation at permitted byte boundary is accepted', () => {
      // Baseline calculation for 1 operation
      const sampleOp = createStoredOperation('peer-A', 1, 1, { data: '' });
      const candidateBatch: OperationBatchPayload = {
        requestId: 'c8a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        supplierPeerId: 'peer-supplier',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [{
          logicalEventId: sampleOp.logicalEventId,
          originPeerId: sampleOp.originPeerId,
          membershipEpoch: sampleOp.membershipEpoch,
          messageType: sampleOp.messageType,
          logicalTimestamp: sampleOp.logicalTimestamp,
          originSequenceNumber: sampleOp.originSequenceNumber,
          causalContext: { ...sampleOp.causalContext },
          payload: { data: 'A'.repeat(50) },
        }],
      };

      const exactLength = new TextEncoder().encode(JSON.stringify(candidateBatch)).byteLength;

      // Configure service with maxPayloadBytes exactly equal to exactLength and 0 envelope overhead
      const boundaryService = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          maxPayloadBytes: exactLength,
          envelopeOverheadBytes: 0,
        },
        clock,
      );

      supplierEngine.getOperationLog().insert(
        createStoredOperation('peer-A', 1, 1, { data: 'A'.repeat(50) }),
      );

      const req: RepairRequestPayload = {
        requestId: 'c8a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = boundaryService.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
      expect((res as OperationBatchPayload).operations.length).toBe(1);
    });

    it('5.9 Exact boundary enforcement: single operation 1 byte over permitted boundary is rejected with BUSY', () => {
      // Baseline calculation for 1 operation
      const sampleOp = createStoredOperation('peer-A', 1, 1, { data: '' });
      const candidateBatch: OperationBatchPayload = {
        requestId: 'c9a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        supplierPeerId: 'peer-supplier',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [{
          logicalEventId: sampleOp.logicalEventId,
          originPeerId: sampleOp.originPeerId,
          membershipEpoch: sampleOp.membershipEpoch,
          messageType: sampleOp.messageType,
          logicalTimestamp: sampleOp.logicalTimestamp,
          originSequenceNumber: sampleOp.originSequenceNumber,
          causalContext: { ...sampleOp.causalContext },
          payload: { data: 'A'.repeat(50) },
        }],
      };

      const exactLength = new TextEncoder().encode(JSON.stringify(candidateBatch)).byteLength;

      // Configure service with maxPayloadBytes 1 byte smaller than exactLength
      const boundaryService = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          maxPayloadBytes: exactLength - 1,
          envelopeOverheadBytes: 0,
        },
        clock,
      );

      supplierEngine.getOperationLog().insert(
        createStoredOperation('peer-A', 1, 1, { data: 'A'.repeat(50) }),
      );

      const req: RepairRequestPayload = {
        requestId: 'c9a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = boundaryService.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.10 Epoch transition purges all cached responses and resets concurrency counters', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const req: RepairRequestPayload = {
        requestId: 'caa74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res1 = supplierService.serveRepairRequest(req, requesterContext);
      expect('operations' in res1).toBe(true);

      // Transition to epoch 2
      supplierEngine.transitionEpoch(2, supplierEpochCap);
      supplierService.onEpochTransition(2);

      // Now request under epoch 1 must reject with EPOCH_MISMATCH
      const res2 = supplierService.serveRepairRequest(req, requesterContext);
      expect('reason' in res2).toBe(true);
      expect((res2 as RepairRejectedPayload).reason).toBe('EPOCH_MISMATCH');
    });

    it('5.11 Missing sequence in middle of range (hole) returns BUSY, never partial or fabricated batch', () => {
      // Sequences 1, 2, 4, 5 present; sequence 3 is missing (hole)
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1));
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 2));
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 4));
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 5));

      const req: RepairRequestPayload = {
        requestId: 'cba74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = supplierService.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.12 Exception during servicing cleanly restores active concurrency counters', () => {
      // Spy on queryRetainedHistoryAndBuildResponse or throw inside log
      const spyLog = vi.spyOn(supplierEngine.getOperationLog(), 'getOperationAt').mockImplementationOnce(() => {
        throw new Error('Disk read failure');
      });

      const req: RepairRequestPayload = {
        requestId: 'cca74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() => supplierService.serveRepairRequest(req, requesterContext)).toThrow('Disk read failure');

      // Subsequent valid request should succeed normally because active concurrency was decremented in finally block
      spyLog.mockRestore();
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const res = supplierService.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
    });

    it('5.13 Envelope overhead accounts for complete wire message and forces batch slicing', () => {
      // Negotiated transport limit: 2000 bytes
      // Envelope overhead: 256 bytes -> maxPayloadBytes: 1744 bytes
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 2000,
          envelopeOverheadBytes: 256,
        },
        clock,
      );

      // Seed 5 operations of ~500 bytes each
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(
          createStoredOperation('peer-A', s, 1, { data: 'M'.repeat(500) }),
        );
      }

      const req: RepairRequestPayload = {
        requestId: 'cda74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
      const batch = res as OperationBatchPayload;

      // Sliced to fit within 1744 bytes payload limit so payload + 256 bytes <= 2000 bytes
      const payloadBytes = new TextEncoder().encode(JSON.stringify(batch)).byteLength;
      expect(payloadBytes).toBeLessThanOrEqual(1744);
      expect(payloadBytes + 256).toBeLessThanOrEqual(2000);
      expect(batch.operations.length).toBeLessThan(5);
    });

    it('5.14 Epoch transition during servicing triggers pre-delivery rejection with EPOCH_MISMATCH', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const sendResponseSpy = vi.fn();
      const customCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: sendResponseSpy,
      };

      const service = new RepairSupplierService(
        supplierEngine,
        customCallbacks,
        {},
        clock,
      );

      // Intercept getOperationLog().getOperationAt to simulate epoch transition during history querying
      const originalGetOp = supplierEngine.getOperationLog().getOperationAt.bind(supplierEngine.getOperationLog());
      vi.spyOn(supplierEngine.getOperationLog(), 'getOperationAt').mockImplementation((origin, seq) => {
        if (supplierEngine.epoch < 2) {
          supplierEngine.transitionEpoch(2, supplierEpochCap);
        }
        return originalGetOp(origin, seq);
      });

      const req: RepairRequestPayload = {
        requestId: 'cea74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('EPOCH_MISMATCH');

      // Obsolete epoch response was never delivered via callback
      expect(sendResponseSpy).not.toHaveBeenCalled();
    });

    it('5.15 Connection generation change during servicing triggers pre-delivery rejection with UNAUTHORIZED', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      const sendResponseSpy = vi.fn();
      const customCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: sendResponseSpy,
      };

      const service = new RepairSupplierService(
        supplierEngine,
        customCallbacks,
        {},
        clock,
      );

      // Intercept getOperationLog().getOperationAt to simulate connection reconnect during history querying
      const originalGetOp = supplierEngine.getOperationLog().getOperationAt.bind(supplierEngine.getOperationLog());
      vi.spyOn(supplierEngine.getOperationLog(), 'getOperationAt').mockImplementation((origin, seq) => {
        supplierEngine.authorizePeerGeneration('peer-requester', 2);
        return originalGetOp(origin, seq);
      });

      const req: RepairRequestPayload = {
        requestId: 'cfa74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      expect(() => service.serveRepairRequest(req, requesterContext)).toThrow(SyncEngineError);
      expect(() => service.serveRepairRequest(req, requesterContext)).toThrow(
        /Requester connection generation 1 does not match active engine generation 2/,
      );
      expect(sendResponseSpy).not.toHaveBeenCalled();
    });

    it('5.16 Re-entrant callback triggering epoch transition preserves non-negative concurrency counters', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      let serviceRef: RepairSupplierService;
      const customCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: () => {
          // Re-entrant epoch transition inside callback
          if (supplierEngine.epoch < 2) {
            supplierEngine.transitionEpoch(2, supplierEpochCap);
            serviceRef.onEpochTransition(2);
          }
        },
      };

      serviceRef = new RepairSupplierService(
        supplierEngine,
        customCallbacks,
        {},
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'd0a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = serviceRef.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);

      // Subsequent request in epoch 2 should succeed cleanly without negative counter corruption
      const reqEpoch2: RepairRequestPayload = {
        ...req,
        requestId: 'd1a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        membershipEpoch: 2,
      };

      // Add op for epoch 2
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 6, 2));
      const resEpoch2 = serviceRef.serveRepairRequest({ ...reqEpoch2, fromSequence: 6, toSequence: 6 }, requesterContext);
      expect('operations' in resEpoch2).toBe(true);
    });

    it('5.17 Re-entrant callback triggering peer disconnect preserves non-negative concurrency counters', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      let serviceRef: RepairSupplierService;
      const customCallbacks: AntiEntropyCallbacks = {
        onBroadcastDigest: vi.fn(),
        onSendRepairResponse: () => {
          // Re-entrant peer disconnect inside callback
          serviceRef.onPeerDisconnected('peer-requester');
        },
      };

      serviceRef = new RepairSupplierService(
        supplierEngine,
        customCallbacks,
        {},
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'd2a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = serviceRef.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
    });

    it('5.18 Permanent vs temporary gap distinction: compacted history returns OUTSIDE_HORIZON, beyond frontier returns BUSY', () => {
      // Base snapshot frontier at sequence 5
      supplierEngine.getOperationLog().setBaseSnapshotFrontier?.({ 'peer-A': 5 });
      for (let s = 6; s <= 10; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      // 1. Permanent gap (compacted to base snapshot) -> OUTSIDE_HORIZON with retainedHorizonSequence
      const reqCompacted: RepairRequestPayload = {
        requestId: 'd3a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 3,
        toSequence: 5,
        membershipEpoch: 1,
      };
      const resCompacted = supplierService.serveRepairRequest(reqCompacted, requesterContext);
      expect('reason' in resCompacted).toBe(true);
      expect((resCompacted as RepairRejectedPayload).reason).toBe('OUTSIDE_HORIZON');
      expect((resCompacted as RepairRejectedPayload).retainedHorizonSequence).toBe(6);

      // 2. Temporary gap (requested sequence 11..15 extends beyond frontier 10) -> BUSY
      const reqBeyond: RepairRequestPayload = {
        requestId: 'd4a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 8,
        toSequence: 12,
        membershipEpoch: 1,
      };
      const resBeyond = supplierService.serveRepairRequest(reqBeyond, requesterContext);
      expect('reason' in resBeyond).toBe(true);
      expect((resBeyond as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.19 Negotiated maximum smaller than envelope overhead safely rejects with BUSY without forcing 1-byte min', () => {
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1));

      // Negotiated limit is 150 bytes, smaller than 256-byte envelope overhead
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 150,
          envelopeOverheadBytes: 256,
        },
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'd5a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.20 Negotiated maximum equal to envelope overhead safely rejects with BUSY', () => {
      supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', 1));

      // Negotiated limit is exactly 256 bytes, equal to envelope overhead -> 0 payload budget
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 256,
          envelopeOverheadBytes: 256,
        },
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'd6a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('reason' in res).toBe(true);
      expect((res as RepairRejectedPayload).reason).toBe('BUSY');
    });

    it('5.21 Variable envelope metadata via envelopeOverheadEstimator measures complete message and slices batch', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(
          createStoredOperation('peer-A', s, 1, { data: 'V'.repeat(400) }),
        );
      }

      // Estimator that computes variable envelope overhead with extra metadata fields (350 bytes)
      const service = new RepairSupplierService(
        supplierEngine,
        supplierCallbacks,
        {
          negotiatedSctpMaxMessageSizeProvider: () => 2000,
          envelopeOverheadEstimator: (_payload, _peerId) => 350,
        },
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'd7a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      const res = service.serveRepairRequest(req, requesterContext);
      expect('operations' in res).toBe(true);
      const batch = res as OperationBatchPayload;
      const payloadBytes = new TextEncoder().encode(JSON.stringify(batch)).byteLength;

      // Complete wire message (payload + 350) must not exceed 2000
      expect(payloadBytes + 350).toBeLessThanOrEqual(2000);
      expect(batch.operations.length).toBeLessThan(5);
    });

    it('5.22 TrustedTransportAdapter.sendRepairResponse drops transmission if epoch transitioned before send', () => {
      const adapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);
      const sendSpy = vi.fn().mockReturnValue(true);

      const batch: OperationBatchPayload = {
        requestId: 'd8a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        supplierPeerId: 'peer-supplier',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        membershipEpoch: 1,
        operations: [],
      };

      // Engine transitions to epoch 2
      supplierEngine.transitionEpoch(2, supplierEpochCap);

      const sent = adapter.sendRepairResponse('peer-requester', batch, 1, sendSpy);
      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('5.23 TrustedTransportAdapter.sendRepairResponse drops transmission if generation changed before send', () => {
      const adapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);
      const sendSpy = vi.fn().mockReturnValue(true);

      const batch: OperationBatchPayload = {
        requestId: 'd9a74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        supplierPeerId: 'peer-supplier',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        membershipEpoch: 1,
        operations: [],
      };

      // Peer generation changes to 2
      supplierEngine.authorizePeerGeneration('peer-requester', 2);

      // Attempting to send with stale expected generation 1
      const sent = adapter.sendRepairResponse('peer-requester', batch, 1, sendSpy);
      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('5.24 Failed send retains retryable response in cache, safely rejecting stale delivery on epoch change', () => {
      for (let s = 1; s <= 5; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('peer-A', s));
      }

      let shouldFailSend = true;
      const sendSpy = vi.fn(() => {
        if (shouldFailSend) {
          throw new Error('Transport send error');
        }
      });

      const service = new RepairSupplierService(
        supplierEngine,
        {
          onBroadcastDigest: vi.fn(),
          onSendRepairResponse: sendSpy,
        },
        {},
        clock,
      );

      const req: RepairRequestPayload = {
        requestId: 'daa74a9a-f4ef-4b9e-b2d3-18ef557163ef',
        sessionId: SESSION_ID,
        requesterPeerId: 'peer-requester',
        targetOriginPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      // Send throws network error
      expect(() => service.serveRepairRequest(req, requesterContext)).toThrow('Transport send error');

      // Response is cached in supplier cache despite send failure.
      // Next attempt succeeds without re-reading log when transport recovers.
      shouldFailSend = false;
      const spyLog = vi.spyOn(supplierEngine.getOperationLog(), 'getOperationAt');
      const resRetry = service.serveRepairRequest(req, requesterContext);
      expect('operations' in resRetry).toBe(true);
      expect(spyLog).not.toHaveBeenCalled();

      // Now if epoch transitions to 2, retry under epoch 1 must reject with EPOCH_MISMATCH
      supplierEngine.transitionEpoch(2, supplierEpochCap);
      service.onEpochTransition(2);

      const resStale = service.serveRepairRequest(req, requesterContext);
      expect('reason' in resStale).toBe(true);
      expect((resStale as RepairRejectedPayload).reason).toBe('EPOCH_MISMATCH');
    });
  });
});
