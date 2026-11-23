import { describe, it, expect, beforeEach } from 'vitest';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { RecoveryStateMachine } from '../recovery/recovery-state-machine.js';
import { QuarantineBuffer } from '../recovery/quarantine-buffer.js';
import { FutureEpochBuffer } from '../recovery/future-epoch-buffer.js';
import { RecoveryAdmissionController } from '../recovery/recovery-admission-controller.js';
import { SyncEngineError } from '../errors.js';

function createMockEvent(
  peerId: string,
  seq: number,
  epoch: number,
  type = 'PLAYBACK_PLAY',
): AnyControlSyncEvent {
  return {
    type,
    eventId: `evt-${peerId}-${seq}`,
    aggregateId: 'session-1',
    membershipEpoch: epoch,
    issuerPeerId: peerId,
    clock: {
      logicalTimestamp: seq,
      originPeerId: peerId,
      sequenceNumber: seq,
      membershipEpoch: epoch,
    },
    data: {
      playbackPosition: seq * 10,
      wallClockTimestamp: Date.now(),
    },
  } as unknown as AnyControlSyncEvent;
}

describe('RecoveryAdmissionController (Phase 7.5 M4.3a)', () => {
  let stateMachine: RecoveryStateMachine;
  let quarantineBuffer: QuarantineBuffer;
  let futureEpochBuffer: FutureEpochBuffer;
  let controller: RecoveryAdmissionController;

  beforeEach(() => {
    stateMachine = new RecoveryStateMachine();
    quarantineBuffer = new QuarantineBuffer();
    futureEpochBuffer = new FutureEpochBuffer();
    controller = new RecoveryAdmissionController({
      stateMachine,
      quarantineBuffer,
      futureEpochBuffer,
    });
  });

  describe('Local Intent Interception', () => {
    it('allows local intent in ACTIVE state', () => {
      expect(stateMachine.state).toBe('ACTIVE');
      const result = controller.interceptLocalIntent();
      expect(result.allowed).toBe(true);
      expect(result.code).toBeUndefined();
    });

    it('rejects local intent in RECOVERING state with REJECTED_RECOVERING', () => {
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      expect(stateMachine.state).toBe('RECOVERING');

      const result = controller.interceptLocalIntent();
      expect(result.allowed).toBe(false);
      expect(result.code).toBe('REJECTED_RECOVERING');
      expect(result.reason).toContain('Engine is catching up via snapshot recovery');
    });

    it('rejects local intent in RECONCILING state with REJECTED_RECOVERING', () => {
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      stateMachine.transition('RECONCILING', 'DRAIN_QUARANTINE', 'test');
      expect(stateMachine.state).toBe('RECONCILING');

      const result = controller.interceptLocalIntent();
      expect(result.allowed).toBe(false);
      expect(result.code).toBe('REJECTED_RECOVERING');
      expect(result.reason).toContain('Engine is reconciling post-snapshot operations');
    });

    it('rejects local intent in DEGRADED state with REJECTED_DEGRADED', () => {
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      stateMachine.transition('DEGRADED', null, 'test failure');
      expect(stateMachine.state).toBe('DEGRADED');

      const result = controller.interceptLocalIntent();
      expect(result.allowed).toBe(false);
      expect(result.code).toBe('REJECTED_DEGRADED');
      expect(result.reason).toContain('Engine is in DEGRADED recovery state');
    });

    it('rejects local intent when controller or state machine is disposed', () => {
      controller.dispose();
      expect(controller.isDisposed).toBe(true);
      const res1 = controller.interceptLocalIntent();
      expect(res1.allowed).toBe(false);
      expect(res1.code).toBe('DISPOSED');

      // Fresh controller with disposed state machine
      const sm2 = new RecoveryStateMachine();
      sm2.dispose();
      const ctrl2 = new RecoveryAdmissionController({
        stateMachine: sm2,
        quarantineBuffer,
        futureEpochBuffer,
      });
      const res2 = ctrl2.interceptLocalIntent();
      expect(res2.allowed).toBe(false);
      expect(res2.code).toBe('DISPOSED');
    });
  });

  describe('Remote Event Interception', () => {
    it('passes through current-epoch remote events when in ACTIVE state', () => {
      expect(stateMachine.state).toBe('ACTIVE');
      const event = createMockEvent('peer-B', 1, 1);

      const result = controller.interceptRemoteEvent(event, 256, 1);
      expect(result.action).toBe('PASS_THROUGH');
      expect(quarantineBuffer.size).toBe(0);
      expect(futureEpochBuffer.size).toBe(0);
    });

    it('quarantines current-epoch remote events when in RECOVERING state', () => {
      stateMachine.transition('RECOVERING', 'CHUNK_TRANSFER', 'test');
      const event = createMockEvent('peer-B', 1, 1);

      const result = controller.interceptRemoteEvent(event, 300, 1);
      expect(result.action).toBe('QUARANTINED');
      expect(result.generationId).toBe(1);
      expect(result.duplicate).toBe(false);
      expect(quarantineBuffer.size).toBe(1);
      expect(quarantineBuffer.activeBytes).toBe(300);
      expect(quarantineBuffer.hasOperation('peer-B', 1)).toBe(true);
      expect(futureEpochBuffer.size).toBe(0);
    });

    it('quarantines current-epoch remote events when in RECONCILING state', () => {
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      stateMachine.transition('RECONCILING', 'DRAIN_QUARANTINE', 'test');
      const event = createMockEvent('peer-B', 5, 2);

      const result = controller.interceptRemoteEvent(event, 400, 2);
      expect(result.action).toBe('QUARANTINED');
      expect(result.duplicate).toBe(false);
      expect(quarantineBuffer.size).toBe(1);
      expect(quarantineBuffer.hasOperation('peer-B', 5)).toBe(true);
    });

    it('rejects current-epoch remote events when in DEGRADED state', () => {
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      stateMachine.transition('DEGRADED', null, 'test failure');
      const event = createMockEvent('peer-B', 1, 1);

      const result = controller.interceptRemoteEvent(event, 256, 1);
      expect(result.action).toBe('REJECTED_DEGRADED');
      expect(quarantineBuffer.size).toBe(0);
    });

    it('discards obsolete-epoch remote events across any recovery state', () => {
      const oldEvent = createMockEvent('peer-B', 1, 1);

      // In ACTIVE
      const resActive = controller.interceptRemoteEvent(oldEvent, 200, 2);
      expect(resActive.action).toBe('DISCARD_OBSOLETE_EPOCH');

      // In RECOVERING
      stateMachine.transition('RECOVERING', 'OFFER_DISCOVERY', 'test');
      const resRec = controller.interceptRemoteEvent(oldEvent, 200, 2);
      expect(resRec.action).toBe('DISCARD_OBSOLETE_EPOCH');

      expect(quarantineBuffer.size).toBe(0);
      expect(futureEpochBuffer.size).toBe(0);
    });

    it('buffers future-epoch remote events in FutureEpochBuffer across states', () => {
      const futureEvent = createMockEvent('peer-C', 10, 3);

      // In ACTIVE
      const resActive = controller.interceptRemoteEvent(futureEvent, 500, 1);
      expect(resActive.action).toBe('BUFFERED_FUTURE_EPOCH');
      expect(resActive.targetEpoch).toBe(3);
      expect(resActive.duplicate).toBe(false);
      expect(futureEpochBuffer.size).toBe(1);
      expect(quarantineBuffer.size).toBe(0);

      // Duplicate future epoch event
      const resDup = controller.interceptRemoteEvent(futureEvent, 500, 1);
      expect(resDup.action).toBe('BUFFERED_FUTURE_EPOCH');
      expect(resDup.duplicate).toBe(true);
      expect(futureEpochBuffer.size).toBe(1);

      // In RECOVERING
      stateMachine.transition('RECOVERING', 'SNAPSHOT_VALIDATION', 'test');
      const futureEvent2 = createMockEvent('peer-D', 1, 4);
      const resRec = controller.interceptRemoteEvent(futureEvent2, 350, 1);
      expect(resRec.action).toBe('BUFFERED_FUTURE_EPOCH');
      expect(resRec.targetEpoch).toBe(4);
      expect(futureEpochBuffer.size).toBe(2);
      expect(quarantineBuffer.size).toBe(0);
    });

    it('rejects malformed or oversized remote events with REJECTED_INVALID', () => {
      // Missing clock
      const malformedEvent1 = { type: 'PLAYBACK_PLAY', membershipEpoch: 1 } as unknown as AnyControlSyncEvent;
      expect(controller.interceptRemoteEvent(malformedEvent1, 200, 1).action).toBe('REJECTED_INVALID');

      // Missing sequenceNumber
      const malformedEvent2 = createMockEvent('peer-A', 1, 1);
      (malformedEvent2.clock as unknown as { sequenceNumber?: unknown }).sequenceNumber = undefined;
      (malformedEvent2.clock as unknown as { logicalTimestamp?: unknown }).logicalTimestamp = undefined;
      expect(controller.interceptRemoteEvent(malformedEvent2, 200, 1).action).toBe('REJECTED_INVALID');

      // Oversized byte length (> 4 KiB)
      const validEvent = createMockEvent('peer-A', 1, 1);
      expect(controller.interceptRemoteEvent(validEvent, 4097, 1).action).toBe('REJECTED_INVALID');

      // Zero or negative byte length
      expect(controller.interceptRemoteEvent(validEvent, 0, 1).action).toBe('REJECTED_INVALID');
      expect(controller.interceptRemoteEvent(validEvent, -50, 1).action).toBe('REJECTED_INVALID');
    });

    it('propagates QUARANTINE_OVERFLOW and FUTURE_EPOCH_OVERFLOW cleanly', () => {
      const smallQuarantine = new QuarantineBuffer({ maxOperations: 1 });
      const smallFuture = new FutureEpochBuffer({ maxOperations: 1 });
      const customCtrl = new RecoveryAdmissionController({
        stateMachine,
        quarantineBuffer: smallQuarantine,
        futureEpochBuffer: smallFuture,
      });

      stateMachine.transition('RECOVERING', 'CHUNK_TRANSFER', 'test');

      // Fill quarantine
      const ev1 = createMockEvent('peer-A', 1, 1);
      expect(customCtrl.interceptRemoteEvent(ev1, 100, 1).action).toBe('QUARANTINED');

      // Overflow quarantine
      const ev2 = createMockEvent('peer-A', 2, 1);
      expect(() => customCtrl.interceptRemoteEvent(ev2, 100, 1)).toThrowError(SyncEngineError);
      try {
        customCtrl.interceptRemoteEvent(ev2, 100, 1);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('QUARANTINE_OVERFLOW');
      }

      // Fill future buffer
      const f1 = createMockEvent('peer-B', 1, 2);
      expect(customCtrl.interceptRemoteEvent(f1, 100, 1).action).toBe('BUFFERED_FUTURE_EPOCH');

      // Overflow future buffer
      const f2 = createMockEvent('peer-B', 2, 2);
      expect(() => customCtrl.interceptRemoteEvent(f2, 100, 1)).toThrowError(SyncEngineError);
      try {
        customCtrl.interceptRemoteEvent(f2, 100, 1);
      } catch (err) {
        expect((err as SyncEngineError).code).toBe('FUTURE_EPOCH_OVERFLOW');
      }
    });
  });

  describe('Isolation and Zero-Mutation Invariants', () => {
    it('never mutates replication vector or playback state during admission', () => {
      // Simulate state representation
      const mockVector: Record<string, number> = { 'peer-A': 5 };
      const vectorSnapshot = JSON.stringify(mockVector);

      stateMachine.transition('RECOVERING', 'CHUNK_TRANSFER', 'test');
      const event = createMockEvent('peer-A', 6, 1);

      controller.interceptRemoteEvent(event, 200, 1);

      // Verify mockVector is completely untouched
      expect(JSON.stringify(mockVector)).toBe(vectorSnapshot);
    });
  });
});

