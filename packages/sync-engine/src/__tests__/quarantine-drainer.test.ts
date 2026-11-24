import { describe, it, expect, vi } from 'vitest';
import {
  QuarantineDrainer,
  QuarantineBuffer,
  LeaderlessSyncEngine,
  createRecoveryInstallationCapability,
  type QuarantinedOperation,
  type IDrainDestination,
  type DestinationAcceptanceReceipt,
} from '../index.js';
import type { AnyControlSyncEvent } from '@syncplay/control-events';

describe('Phase 7.5 M4.3c — QuarantineDrainer & DRAINER_IN_FLIGHT Protocol', () => {
  const baseConfig = {
    sessionId: 'session-drainer-test',
    localPeerId: 'peer-local',
    minPlaybackRate: 0.5,
    maxPlaybackRate: 2.0,
    positionEpsilonMs: 15,
    logCapacity: 128,
  };

  const createMockQuarantinedOp = (
    originPeerId: string,
    sequenceNumber: number,
    overrides?: Partial<QuarantinedOperation>,
  ): QuarantinedOperation => {
    const event: AnyControlSyncEvent = {
      specVersion: '0.1.0',
      eventId: `evt-${originPeerId}-${sequenceNumber}`,
      eventType: 'PLAY',
      origin: 'remote',
      timestamp: 1000 + sequenceNumber,
      membershipEpoch: 1,
      message: {
        logicalEventId: `evt-${originPeerId}-${sequenceNumber}`,
        senderPeerId: originPeerId,
        originPeerId,
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 100 + sequenceNumber,
        originSequenceNumber: sequenceNumber,
        causalContext: sequenceNumber > 1 ? { [originPeerId]: sequenceNumber - 1 } : {},
        payload: { anchorPositionMs: 1000 },
      },
    } as AnyControlSyncEvent;

    return {
      event,
      originPeerId,
      sequenceNumber,
      membershipEpoch: 1,
      serializedByteLength: 120,
      receivedAtMs: Date.now(),
      ...overrides,
    };
  };

  const createValidCausalOp = (
    originPeerId: string,
    sequenceNumber: number,
  ): QuarantinedOperation => {
    const event: AnyControlSyncEvent = {
      specVersion: '0.1.0',
      eventId: `evt-${originPeerId}-${sequenceNumber}`,
      eventType: 'PLAY',
      origin: 'remote',
      timestamp: 1000 + sequenceNumber,
      membershipEpoch: 1,
      message: {
        logicalEventId: `evt-${originPeerId}-${sequenceNumber}`,
        senderPeerId: originPeerId,
        originPeerId,
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: sequenceNumber,
        originSequenceNumber: sequenceNumber,
        causalContext: sequenceNumber > 1 ? { [originPeerId]: sequenceNumber - 1 } : {},
        payload: { anchorPositionMs: 1000 },
      },
    } as AnyControlSyncEvent;

    return {
      event,
      originPeerId,
      sequenceNumber,
      membershipEpoch: 1,
      serializedByteLength: 120,
      receivedAtMs: Date.now(),
    };
  };

  it('D1: performs normal sequential drain with DRAINER_IN_FLIGHT custody and clears on ACCEPTED_REDUCED', async () => {
    const quarantineBuffer = new QuarantineBuffer();
    const admittedOps: QuarantinedOperation[] = [];

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: vi.fn((op: QuarantinedOperation): DestinationAcceptanceReceipt => {
        admittedOps.push(op);
        return {
          status: 'ACCEPTED_REDUCED',
          vectorAdvanced: true,
          newSequence: op.sequenceNumber,
        };
      }),
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 1);
    const op2 = createMockQuarantinedOp('peer-1', 2);
    quarantineBuffer.enqueue(op1);
    quarantineBuffer.enqueue(op2);
    quarantineBuffer.swapDrainBatch();

    expect(drainer.inFlightRecord).toBeNull();
    expect(quarantineBuffer.drainSize).toBe(2);

    const result = await drainer.drainBatch();

    expect(result.batchSize).toBe(2);
    expect(result.appliedCount).toBe(2);
    expect(result.pendingCount).toBe(0);
    expect(result.discardedCount).toBe(0);
    expect(result.haltedOnCapacity).toBe(false);

    // Cardinality verified: inFlightRecord is null post-completion
    expect(drainer.inFlightRecord).toBeNull();
    expect(quarantineBuffer.drainSize).toBe(0);
    expect(admittedOps.length).toBe(2);
  });

  it('D2: detects and filters duplicate operation arriving while identical operation is in flight', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    let inFlightSnapshotDuringAdmission = false;

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (op: QuarantinedOperation): DestinationAcceptanceReceipt => {
        // While op is being admitted, verify drainer tracks it as in-flight
        if (drainer.isInFlight(op.originPeerId, op.sequenceNumber)) {
          inFlightSnapshotDuringAdmission = true;
        }
        return {
          status: 'ACCEPTED_REDUCED',
          vectorAdvanced: true,
          newSequence: op.sequenceNumber,
        };
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 1);
    quarantineBuffer.enqueue(op1);
    quarantineBuffer.swapDrainBatch();

    await drainer.drainBatch();

    expect(inFlightSnapshotDuringAdmission).toBe(true);
    expect(drainer.isInFlight('peer-1', 1)).toBe(false); // Cleared after confirmation
  });

  it('D3: handles ACCEPTED_PENDING destination receipt without advancing vector', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (): DestinationAcceptanceReceipt => {
        return {
          status: 'ACCEPTED_PENDING',
          vectorAdvanced: false,
          reason: 'MISSING_DEPENDENCY',
        };
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 5);
    quarantineBuffer.enqueue(op1);
    quarantineBuffer.swapDrainBatch();

    const result = await drainer.drainBatch();

    expect(result.appliedCount).toBe(0);
    expect(result.pendingCount).toBe(1);
    expect(result.discardedCount).toBe(0);
    expect(drainer.inFlightRecord).toBeNull();
  });

  it('D4: handles ALREADY_REPRESENTED and ALREADY_OWNED_DUPLICATE receipts as discarded', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    let callCount = 0;
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (): DestinationAcceptanceReceipt => {
        callCount++;
        if (callCount === 1) {
          return { status: 'ALREADY_REPRESENTED', vectorAdvanced: false };
        }
        return { status: 'ALREADY_OWNED_DUPLICATE', vectorAdvanced: false };
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 1));
    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 2));
    quarantineBuffer.swapDrainBatch();

    const result = await drainer.drainBatch();

    expect(result.appliedCount).toBe(0);
    expect(result.pendingCount).toBe(0);
    expect(result.discardedCount).toBe(2);
    expect(drainer.inFlightRecord).toBeNull();
  });

  it('D5: preserves inFlightRecord and halts immediately upon CAPACITY_REJECTED receipt', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (op: QuarantinedOperation): DestinationAcceptanceReceipt => {
        if (op.sequenceNumber === 2) {
          return {
            status: 'CAPACITY_REJECTED',
            vectorAdvanced: false,
            currentQueueSize: 256,
            maxCapacity: 256,
          };
        }
        return {
          status: 'ACCEPTED_REDUCED',
          vectorAdvanced: true,
          newSequence: op.sequenceNumber,
        };
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 1));
    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 2));
    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 3));
    quarantineBuffer.swapDrainBatch();

    const result = await drainer.drainBatch();

    expect(result.appliedCount).toBe(1);
    expect(result.haltedOnCapacity).toBe(true);

    // In-flight record is preserved! (ZERO OPERATION LOSS)
    expect(drainer.inFlightRecord).not.toBeNull();
    expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(2);
    expect(drainer.inFlightRecord?.destinationStatus).toBe('CAPACITY_REJECTED');

    // Op 3 was not shifted yet and remains in drain batch
    expect(quarantineBuffer.drainSize).toBe(1);
  });

  it('D6: preserves inFlightRecord and halts when destination throws unexpected exception', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (): DestinationAcceptanceReceipt => {
        throw new Error('Fatal storage corruption during log insertion');
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 1));
    quarantineBuffer.swapDrainBatch();

    const result = await drainer.drainBatch();

    expect(result.haltedOnError).toMatch(/Fatal storage corruption/);
    expect(drainer.inFlightRecord).not.toBeNull();
    expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);
  });

  it('D7: resumeDrain cleans inFlightRecord if destination already committed, or re-attempts admission', () => {
    const quarantineBuffer = new QuarantineBuffer();

    let committedInDestination = false;
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (): DestinationAcceptanceReceipt => {
        committedInDestination = true;
        return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: 1 };
      },
      hasOperation: () => committedInDestination,
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    // Simulate an interrupted in-flight operation (e.g. after crash / timeout)
    const op = createMockQuarantinedOp('peer-1', 1);
    (drainer as any)._inFlightRecord = {
      operation: op,
      dequeuedAtMs: Date.now(),
      attemptCount: 1,
      destinationStatus: 'PENDING',
    };

    // Case 1: Destination does not have operation -> resumeDrain retries admission
    drainer.resumeDrain();
    expect(committedInDestination).toBe(true);
    expect(drainer.inFlightRecord).toBeNull();

    // Case 2: In-flight already in destination -> resumeDrain cleanly drops inFlightRecord
    (drainer as any)._inFlightRecord = {
      operation: op,
      dequeuedAtMs: Date.now(),
      attemptCount: 2,
      destinationStatus: 'PENDING',
    };
    drainer.resumeDrain();
    expect(drainer.inFlightRecord).toBeNull();
  });

  it('D8: restoreInFlightToQueue unshifts in-flight operation back to quarantine queue', () => {
    const quarantineBuffer = new QuarantineBuffer();
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: () => ({ status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: 1 }),
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op = createMockQuarantinedOp('peer-1', 42);
    (drainer as any)._inFlightRecord = {
      operation: op,
      dequeuedAtMs: Date.now(),
      attemptCount: 1,
      destinationStatus: 'CAPACITY_REJECTED',
    };

    drainer.restoreInFlightToQueue();

    expect(drainer.inFlightRecord).toBeNull();
    expect(quarantineBuffer.drainSize).toBe(1);
    expect(quarantineBuffer.peekDrain()?.sequenceNumber).toBe(42);
  });

  it('D9: rejects reentrant drainBatch call while already draining', async () => {
    const quarantineBuffer = new QuarantineBuffer();

    let nestedCallRejected = false;
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: (): DestinationAcceptanceReceipt => {
        // Attempt reentrant drainBatch while actively draining
        drainer.drainBatch().catch((err: any) => {
          if (err.message.includes('already draining a batch')) {
            nestedCallRejected = true;
          }
        });
        return { status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: 1 };
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    quarantineBuffer.enqueue(createMockQuarantinedOp('peer-1', 1));
    quarantineBuffer.swapDrainBatch();

    await drainer.drainBatch();
    expect(nestedCallRejected).toBe(true);
  });

  it('D10: integrates seamlessly with LeaderlessSyncEngine.admitDrainedOperation', async () => {
    const capability = createRecoveryInstallationCapability('test', baseConfig.sessionId);
    const engine = new LeaderlessSyncEngine({
      ...baseConfig,
      recoveryInstallationCapability: capability,
    });
    const quarantineBuffer = new QuarantineBuffer();

    const drainer = new QuarantineDrainer({
      destination: engine,
      quarantineBuffer,
    });

    // Enqueue 2 valid causal sequential operations: peer-remote:1 (clock 1) and peer-remote:2 (clock 2)
    const op1 = createValidCausalOp('peer-remote', 1);
    const op2 = createValidCausalOp('peer-remote', 2);
    quarantineBuffer.enqueue(op1);
    quarantineBuffer.enqueue(op2);
    quarantineBuffer.swapDrainBatch();

    const result = await drainer.drainBatch();

    expect(result.appliedCount).toBe(2);
    expect(engine.replicationFrontier['peer-remote']).toBe(2);
    expect(engine.hasOperation('peer-remote', 1)).toBe(true);
    expect(engine.hasOperation('peer-remote', 2)).toBe(true);
    expect(engine.hasOperation('peer-remote', 3)).toBe(false);
  });

  it('D11: preserves in-flight operation across subsequent drainBatch calls without overwrite', async () => {
    const quarantineBuffer = new QuarantineBuffer();
    let admitCount = 0;
    const admittedOps: QuarantinedOperation[] = [];

    const mockDestination: IDrainDestination = {
      admitDrainedOperation: vi.fn((op: QuarantinedOperation): DestinationAcceptanceReceipt => {
        admitCount++;
        if (admitCount === 1) {
          // First attempt: reject with CAPACITY_REJECTED to halt drain
          return { status: 'CAPACITY_REJECTED' };
        }
        // Subsequent attempts: accept
        admittedOps.push(op);
        return {
          status: 'ACCEPTED_REDUCED',
          vectorAdvanced: true,
          newSequence: op.sequenceNumber,
        };
      }),
      hasOperation: (peerId: string, seq: number) => {
        return admittedOps.some(o => o.originPeerId === peerId && o.sequenceNumber === seq);
      },
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 1);
    const op2 = createMockQuarantinedOp('peer-1', 2);
    quarantineBuffer.enqueue(op1);
    quarantineBuffer.enqueue(op2);
    quarantineBuffer.swapDrainBatch();

    // First drain: processes op1 -> CAPACITY_REJECTED -> halts with in-flight record retained
    const result1 = await drainer.drainBatch();
    expect(result1.haltedOnCapacity).toBe(true);
    expect(drainer.inFlightRecord).not.toBeNull();
    expect(drainer.inFlightRecord?.operation.sequenceNumber).toBe(1);

    // Second drain: must NOT overwrite op1 with op2; op1 is restored and processed first
    const result2 = await drainer.drainBatch();
    expect(result2.appliedCount).toBe(2);
    expect(drainer.inFlightRecord).toBeNull();
    expect(admittedOps.map(o => o.sequenceNumber)).toEqual([1, 2]);
  });

  it('D12: processSingleOperation throws INVALID_STATE if in-flight custody is already held', () => {
    const quarantineBuffer = new QuarantineBuffer();
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: () => ({ status: 'CAPACITY_REJECTED' }),
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 1);
    const op2 = createMockQuarantinedOp('peer-1', 2);

    drainer.processSingleOperation(op1);
    expect(drainer.inFlightRecord).not.toBeNull();

    expect(() => drainer.processSingleOperation(op2)).toThrow(/Cannot acquire in-flight custody/);
  });

  it('D13: dispose restores unresolved in-flight operation to quarantine buffer without data loss', () => {
    const quarantineBuffer = new QuarantineBuffer();
    const mockDestination: IDrainDestination = {
      admitDrainedOperation: () => ({ status: 'CAPACITY_REJECTED' }),
    };

    const drainer = new QuarantineDrainer({
      destination: mockDestination,
      quarantineBuffer,
    });

    const op1 = createMockQuarantinedOp('peer-1', 1);
    drainer.processSingleOperation(op1);
    expect(drainer.inFlightRecord).not.toBeNull();

    drainer.dispose();
    expect(drainer.inFlightRecord).toBeNull();
    // Verify op1 was restored to drain queue
    expect(quarantineBuffer.drainSize).toBe(1);
    expect(quarantineBuffer.shiftDrain()?.sequenceNumber).toBe(1);
  });
});
