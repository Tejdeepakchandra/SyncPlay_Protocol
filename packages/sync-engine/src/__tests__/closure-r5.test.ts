/**
 * Phase 7.3-R5: Final Authorization & Canonicalization Closure Test Suite.
 *
 * Implements strict verification for:
 * 1. Task 1: Epoch Capability Ownership & Access Control (injected capability, no public getter)
 * 2. Task 2: Bind Repair Supplier to Authenticated Transport Identity
 * 3. Task 3: Official RFC 8785 Sample Conformance (conversion vector, french.json, structures, arrays, numbers)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  canonicalizeJson,
  sha256,
  computeOperationFingerprint,
  type EpochTransitionCapability,
  type AuthenticatedSupplierContext,
} from '../index.js';
import { SyncEngineError } from '../errors.js';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type { OperationBatchPayload } from '@syncplay/protocol';

function makeRemoteEvent(
  logicalEventId: string,
  originPeerId: string,
  messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE',
  payload: unknown,
  originSequenceNumber: number,
  logicalTimestamp: number,
  causalContext: Record<string, number> = {},
  membershipEpoch: number = 1,
  connectionGeneration?: number,
  senderPeerId?: string,
): AnyControlSyncEvent {
  const actualSender = senderPeerId ?? originPeerId;
  return {
    origin: 'remote',
    message: {
      messageId: `msg-${logicalEventId}`,
      sessionId: 'test-session',
      senderPeerId: actualSender,
      originPeerId,
      messageType,
      sentAt: 1000,
      membershipEpoch,
      connectionGeneration,
      logicalEventId,
      originSequenceNumber,
      logicalTimestamp,
      causalContext,
      payload,
    },
  } as unknown as AnyControlSyncEvent;
}

describe('Phase 7.3-R5: Final Authorization & Canonicalization Closure', () => {
  let engine: LeaderlessSyncEngine;
  let coordinatorCapability: EpochTransitionCapability;
  let simulatedTime: number;

  beforeEach(() => {
    simulatedTime = 1000;
    // Trusted coordinator instantiates capability outside the engine
    coordinatorCapability = createEpochTransitionCapability(
      'peer-coordinator',
      'test-session',
      'coordinator-secret-key-999',
    );

    // Injects capability at trusted engine creation time
    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session',
      localPeerId: 'peer-coordinator',
      clock: { now: () => simulatedTime },
      epochCapability: coordinatorCapability,
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 1. Task 1: Correct Epoch Capability Ownership
  // ════════════════════════════════════════════════════════════════
  describe('Task 1: Correct Epoch Capability Ownership', () => {
    it('trusted coordinator possessing the capability can transition the epoch', () => {
      expect(engine.epoch).toBe(1);
      engine.transitionEpoch(2, coordinatorCapability);
      expect(engine.epoch).toBe(2);
    });

    it('arbitrary local caller cannot obtain the capability through the engine API', () => {
      // The public getter was removed to prevent arbitrary callers from acquiring capability
      expect((engine as any).getEpochTransitionCapability).toBeUndefined();
    });

    it('engine constructed without capability rejects any transitionEpoch attempt with UNAUTHORIZED', () => {
      const unmanagedEngine = new LeaderlessSyncEngine({
        sessionId: 'test-session',
        localPeerId: 'peer-worker',
        clock: { now: () => 1000 },
        // No epochCapability provided
      });

      expect(() =>
        unmanagedEngine.transitionEpoch(2, coordinatorCapability),
      ).toThrow(SyncEngineError);

      try {
        unmanagedEngine.transitionEpoch(2, coordinatorCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(unmanagedEngine.epoch).toBe(1);
    });

    it('arbitrary caller without capability receives UNAUTHORIZED', () => {
      expect(() => (engine as any).transitionEpoch(2)).toThrow(SyncEngineError);
      try {
        (engine as any).transitionEpoch(2);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
        expect(err.message).toContain('Epoch transition requires authorized EpochTransitionCapability');
      }
      expect(engine.epoch).toBe(1);
    });

    it('caller passing a plain token-string cannot bypass capability ownership', () => {
      // Plain token string is rejected; must pass the EpochTransitionCapability object
      expect(() =>
        (engine as any).transitionEpoch(2, 'coordinator-secret-key-999'),
      ).toThrow(SyncEngineError);

      try {
        (engine as any).transitionEpoch(2, 'coordinator-secret-key-999');
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('separately created capability with matching field values is rejected with UNAUTHORIZED', () => {
      const duplicateCapability = createEpochTransitionCapability(
        'peer-coordinator',
        'test-session',
        'coordinator-secret-key-999',
      );
      expect(() => engine.transitionEpoch(2, duplicateCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, duplicateCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
        expect(err.message).toContain('Epoch transition requires authorized EpochTransitionCapability');
      }
      expect(engine.epoch).toBe(1);
    });

    it('cloned capability object with identical properties is rejected with UNAUTHORIZED', () => {
      const clonedCapability = { ...coordinatorCapability };
      expect(() => engine.transitionEpoch(2, clonedCapability as any)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, clonedCapability as any);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
        expect(err.message).toContain('Epoch transition requires authorized EpochTransitionCapability');
      }
      expect(engine.epoch).toBe(1);
    });

    it('capability from another session or engine is rejected with UNAUTHORIZED', () => {
      const foreignCapability = createEpochTransitionCapability(
        'peer-coordinator',
        'other-session-777',
        'coordinator-secret-key-999',
      );
      expect(() => engine.transitionEpoch(2, foreignCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, foreignCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
        expect(err.message).toContain('Epoch transition requires authorized EpochTransitionCapability');
      }
      expect(engine.epoch).toBe(1);
    });

    it('capability object is frozen to prevent accidental or malicious mutation', () => {
      expect(Object.isFrozen(coordinatorCapability)).toBe(true);
      expect(Object.isFrozen((engine as any).authorizedEpochCapability)).toBe(true);
      expect(() => {
        (coordinatorCapability as any).token = 'tampered';
      }).toThrow();
    });

    it('unauthorized transition attempts do not mutate epoch, pending operations, causal frontier, replication vector, clock, or playback state', () => {
      const ev1 = makeRemoteEvent('ev-init', 'peer-A', 'PLAY', {}, 1, 1, {}, 1);
      engine.receive(ev1);
      expect(engine.getState().status).toBe('playing');
      expect(engine.logicalClock).toBe(1);

      const pendingOp = makeRemoteEvent('pending-gap', 'peer-A', 'PAUSE', {}, 3, 3, { 'peer-A': 2 }, 1);
      engine.receive(pendingOp);
      expect(engine.pendingQueueSize).toBe(1);

      const initialEpoch = engine.epoch;
      const initialPendingSize = engine.pendingQueueSize;
      const initialClock = engine.logicalClock;
      const initialLocalFrontier = { ...engine.localFrontier };
      const initialReplicationFrontier = { ...engine.replicationFrontier };
      const initialPlaybackState = { ...engine.getState() };

      const cloned = { ...coordinatorCapability };
      const fake = createEpochTransitionCapability('rogue', 'test-session', 'fake');
      const matching = createEpochTransitionCapability('peer-coordinator', 'test-session', 'coordinator-secret-key-999');

      expect(() => engine.transitionEpoch(2, cloned as any)).toThrow();
      expect(() => engine.transitionEpoch(2, fake)).toThrow();
      expect(() => engine.transitionEpoch(2, matching)).toThrow();
      expect(() => (engine as any).transitionEpoch(2, null)).toThrow();
      expect(() => (engine as any).transitionEpoch(2)).toThrow();

      expect(engine.epoch).toBe(initialEpoch);
      expect(engine.pendingQueueSize).toBe(initialPendingSize);
      expect(engine.logicalClock).toBe(initialClock);
      expect(engine.localFrontier).toEqual(initialLocalFrontier);
      expect(engine.replicationFrontier).toEqual(initialReplicationFrontier);
      expect(engine.getState()).toEqual(initialPlaybackState);
    });

    it('forged capability with wrong token is rejected with UNAUTHORIZED', () => {
      const forgedCapability = createEpochTransitionCapability(
        'peer-coordinator',
        'test-session',
        'forged-token-xyz',
      );

      expect(() => engine.transitionEpoch(2, forgedCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, forgedCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('mismatched session capability is rejected with UNAUTHORIZED', () => {
      const otherSessionCapability = createEpochTransitionCapability(
        'peer-coordinator',
        'other-session-888',
        'coordinator-secret-key-999',
      );

      expect(() => engine.transitionEpoch(2, otherSessionCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, otherSessionCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('mismatched authority capability is rejected with UNAUTHORIZED', () => {
      const otherAuthorityCapability = createEpochTransitionCapability(
        'rogue-peer',
        'test-session',
        'coordinator-secret-key-999',
      );

      expect(() => engine.transitionEpoch(2, otherAuthorityCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, otherAuthorityCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('remote future-epoch wire event cannot advance the epoch', () => {
      expect(engine.epoch).toBe(1);

      // Premature remote event claiming future epoch 2
      const futureEpochEvent = makeRemoteEvent('ev-fut', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(futureEpochEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.getLastDecision()?.message).toContain('unestablished future epoch');
      expect(engine.epoch).toBe(1);
      expect(engine.logicalClock).toBe(0);
    });

    it('authorized transition preserves playback state, monotonic clock, and purges obsolete pending queue', () => {
      // 1. Play state in epoch 1
      const ev1 = makeRemoteEvent('ev-1', 'peer-A', 'PLAY', {}, 1, 1, {}, 1);
      engine.receive(ev1);
      expect(engine.getState().status).toBe('playing');
      const versionBefore = engine.getState().stateVersion;

      // 2. Buffer pending op in epoch 1
      const pendingOldOp = makeRemoteEvent(
        'pending-old',
        'peer-A',
        'PLAY',
        {},
        3,
        3,
        { 'peer-A': 2 },
        1,
      );
      engine.receive(pendingOldOp);
      expect(engine.pendingQueueSize).toBe(1);

      // 3. Authorized transition
      engine.transitionEpoch(2, coordinatorCapability);
      expect(engine.epoch).toBe(2);

      // 4. Invariants intact
      expect(engine.getState().status).toBe('playing');
      expect(engine.getState().stateVersion).toBe(versionBefore);
      expect(engine.logicalClock).toBe(1);
      expect(engine.pendingQueueSize).toBe(0); // Old epoch purged

      // 5. Subsequent event in epoch 2 advances clock monotonically
      const ev2 = makeRemoteEvent('ev-2', 'peer-A', 'PAUSE', {}, 2, 2, { 'peer-A': 1 }, 2);
      engine.receive(ev2);
      expect(engine.getState().status).toBe('paused');
      expect(engine.logicalClock).toBe(2);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 2. Task 2: Bind Repair Supplier to Authenticated Transport Identity
  // ════════════════════════════════════════════════════════════════
  describe('Task 2: Bind Repair Supplier to Authenticated Transport Identity', () => {
    it('accepts repair batch with valid AuthenticatedSupplierContext matching authorized generation', () => {
      engine.authorizePeerGeneration('peer-S', 2);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 2,
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.getState().status).toBe('playing');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
    });

    it('rejects forged supplier identity when untrusted payload claims different peer than authenticated transport', () => {
      engine.authorizePeerGeneration('peer-S', 1);
      engine.authorizePeerGeneration('peer-attacker', 1);

      // Malicious batch claims to be from trusted peer-S
      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      // Authenticated transport layer identified sender as peer-attacker
      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-attacker',
        connectionGeneration: 1,
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('Forged supplier identity');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('rejects forged/unannounced supplier connection generation (> active)', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 2, // Claims gen 2, but authorized is 1
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('unannounced supplier connection generation');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('rejects stale supplier connection generation (< active)', () => {
      engine.authorizePeerGeneration('peer-S', 3);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 2, // Claims stale gen 2, active is 3
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('stale supplier connection generation');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('preserves historical author identity and does not compare author generation with supplier generation', () => {
      // Supplier Peer S is at generation 2
      engine.authorizePeerGeneration('peer-S', 2);
      // Historical author Peer A is at generation 4
      engine.authorizePeerGeneration('peer-A', 4);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 2,
      };

      // Supplier gen 2 is validated; author Peer A's generation 4 is NOT compared
      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.replicationFrontier['peer-S']).toBeUndefined();
    });

    it('rejects supplier attempt to rewrite originPeerId in batch operations', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 2,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
          {
            // Supplier rewrites second op origin to peer-B
            logicalEventId: 'op-b1',
            originPeerId: 'peer-B',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 2,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1,
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(2);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');

      expect(decisions[1]!.accepted).toBe(false);
      expect(decisions[1]!.code).toBe('REJECTED_INVALID');
      expect(decisions[1]!.message).toContain('does not match batch originPeerId');
      expect(engine.replicationFrontier['peer-B']).toBeUndefined();
    });

    it('rejects supplier attempt to alter operation content with REJECTED_IDENTITY_CONFLICT', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1,
      };

      // 1. Initial op applied
      const originalBatch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: { value: 'original' },
          },
        ],
      };
      const d1 = engine.ingestRepairBatch(originalBatch, transportContext);
      expect(d1[0]!.accepted).toBe(true);

      // 2. Tampered op supplied
      const tamperedBatch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: { value: 'tampered-payload' },
          },
        ],
      };
      const d2 = engine.ingestRepairBatch(tamperedBatch, transportContext);
      expect(d2[0]!.accepted).toBe(false);
      expect(d2[0]!.code).toBe('REJECTED_IDENTITY_CONFLICT');
    });

    it('rejects repair batch when transport context is missing completely', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const decisions = (engine as any).ingestRepairBatch(batch);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Repair batch requires authenticated supplier transport context',
      );
    });

    it('rejects repair batch when connectionGeneration is missing from transport context', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      // Context with missing connectionGeneration
      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Repair batch requires valid supplier connectionGeneration',
      );
    });

    it('rejects repair batch when connectionGeneration is invalid or non-integer', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const decisions1 = engine.ingestRepairBatch(batch, {
        supplierPeerId: 'peer-S',
        connectionGeneration: 0,
      });
      expect(decisions1[0]!.accepted).toBe(false);
      expect(decisions1[0]!.code).toBe('REJECTED_INVALID');

      const decisions2 = engine.ingestRepairBatch(batch, {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1.5,
      });
      expect(decisions2[0]!.accepted).toBe(false);
      expect(decisions2[0]!.code).toBe('REJECTED_INVALID');
    });

    it('rejects untrusted wire callers attempting to pass raw numbers as transport context', () => {
      engine.authorizePeerGeneration('peer-S', 2);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const decisions = (engine as any).ingestRepairBatch(batch, 2);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Repair batch requires authenticated supplier transport context',
      );
    });

    it('historical author old generation does not conflict with valid current supplier generation', () => {
      // Historical author Peer A had generation 1 when creating op
      engine.authorizePeerGeneration('peer-A', 1);
      // Supplier Peer S is currently at generation 5
      engine.authorizePeerGeneration('peer-S', 5);

      const batch: OperationBatchPayload = {
        supplierPeerId: 'peer-S',
        originPeerId: 'peer-A',
        fromSequence: 1,
        toSequence: 1,
        operations: [
          {
            logicalEventId: 'op-hist-a1',
            originPeerId: 'peer-A',
            membershipEpoch: 1,
            messageType: 'PLAY',
            logicalTimestamp: 1,
            originSequenceNumber: 1,
            causalContext: {},
            payload: {},
          },
        ],
      };

      const transportContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 5,
      };

      const decisions = engine.ingestRepairBatch(batch, transportContext);
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 3. Task 3: Official RFC 8785 Conformance Verification
  // ════════════════════════════════════════════════════════════════
  describe('Task 3: Official RFC 8785 Sample Conformance', () => {
    it('strictly satisfies the official RFC 8785 Basic Conversion Example', () => {
      // Official input from RFC 8785 Section 3.2.2 / Appendix A
      const rawJson =
        '{\n' +
        '   "numbers": [333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001],\n' +
        '   "string": "\\u20ac$\\u000F\\u000aA\'\\u0042\\u0022\\u005c\\\\\\"\\/",\n' +
        '   "literals": [null, true, false]\n' +
        '}';
      const input = JSON.parse(rawJson);

      const canonical = canonicalizeJson(input);

      // Expected output per RFC 8785 Section 3.2.3:
      // {"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}
      expect(canonical).toBe(
        '{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\\u000f\\nA\'B\\"\\\\\\\\\\"/"}',
      );
    });

    it('strictly satisfies the official RFC 8785 french.json non-locale sorting vector', () => {
      // Official french.json test vector from cyberphone/json-canonicalization
      const input = {
        peach: 'This sorting order',
        péché: 'is wrong according to French',
        pêche: 'but canonicalization MUST',
        sin: 'ignore locale',
      };

      const canonical = canonicalizeJson(input);

      // Expected output: UTF-16 code units sorting without linguistic locale rules
      expect(canonical).toBe(
        '{"peach":"This sorting order","péché":"is wrong according to French","pêche":"but canonicalization MUST","sin":"ignore locale"}',
      );
    });

    it('strictly satisfies official structures.json property ordering rules', () => {
      // Mixed-case ASCII keys, numeric strings, empty key, and whitespace keys
      const input = {
        e: 1,
        '': 'empty key',
        '\n': 'newline key',
        ' ': 'space key',
        '10': 'ten',
        '2': 'two',
        '1': 'one',
        a: 2,
        E: 3,
        A: 4,
      };

      const canonical = canonicalizeJson(input);

      // UTF-16 code units order:
      // "" (empty, length 0)
      // "\n" (0x0A)
      // " " (0x20)
      // "1" (0x31)
      // "10" (0x31, 0x30)
      // "2" (0x32)
      // "A" (0x41)
      // "E" (0x45)
      // "a" (0x61)
      // "e" (0x65)
      expect(canonical).toBe(
        '{"":"empty key","\\n":"newline key"," ":"space key","1":"one","10":"ten","2":"two","A":4,"E":3,"a":2,"e":1}',
      );
    });

    it('strictly satisfies official arrays.json element order preservation', () => {
      // Official arrays.json test pattern: array ordering is preserved, nested objects are sorted
      const input = [
        56,
        { d: true, '10': null, '1': [] },
        [3, 2, 1],
      ];

      const canonical = canonicalizeJson(input);
      expect(canonical).toBe('[56,{"1":[],"10":null,"d":true},[3,2,1]]');
    });

    it('strictly satisfies official values.json number boundary formatting', () => {
      // Numbers formatted strictly using ECMAScript Grisu2 / RFC 8785 §3.2.2.3 rules
      expect(canonicalizeJson(0)).toBe('0');
      expect(canonicalizeJson(-0)).toBe('0');
      expect(canonicalizeJson(1e-6)).toBe('0.000001');
      expect(canonicalizeJson(1e-7)).toBe('1e-7');
      expect(canonicalizeJson(1e20)).toBe('100000000000000000000');
      expect(canonicalizeJson(1e21)).toBe('1e+21');
      expect(canonicalizeJson(Number.MAX_SAFE_INTEGER)).toBe('9007199254740991');
      expect(canonicalizeJson(Number.MIN_SAFE_INTEGER)).toBe('-9007199254740991');
    });

    it('strictly satisfies RFC 8785 string escaping rules', () => {
      // Control characters U+0000 through U+001F
      expect(canonicalizeJson('\u0000')).toBe('"\\u0000"');
      expect(canonicalizeJson('\u0001')).toBe('"\\u0001"');
      expect(canonicalizeJson('\b')).toBe('"\\b"');
      expect(canonicalizeJson('\t')).toBe('"\\t"');
      expect(canonicalizeJson('\n')).toBe('"\\n"');
      expect(canonicalizeJson('\f')).toBe('"\\f"');
      expect(canonicalizeJson('\r')).toBe('"\\r"');
      expect(canonicalizeJson('\u001f')).toBe('"\\u001f"');

      // Quotation mark and reverse solidus escaped
      expect(canonicalizeJson('"')).toBe('"\\""');
      expect(canonicalizeJson('\\')).toBe('"\\\\"');

      // Slashes and single quotes NOT escaped per RFC 8785 §3.2.2.2
      expect(canonicalizeJson('/')).toBe('"/"');
      expect(canonicalizeJson("'")).toBe('"\'"');

      // Unicode characters and emojis above U+001F NOT escaped
      expect(canonicalizeJson('€')).toBe('"€"');
      expect(canonicalizeJson('Å')).toBe('"Å"');
      expect(canonicalizeJson('💩')).toBe('"💩"');
    });

    it('strictly rejects invalid non-JSON inputs without silent coercion', () => {
      // Non-finite numbers
      expect(() => canonicalizeJson(NaN)).toThrow(TypeError);
      expect(() => canonicalizeJson(Infinity)).toThrow(TypeError);
      expect(() => canonicalizeJson(-Infinity)).toThrow(TypeError);

      // Undefined
      expect(() => canonicalizeJson(undefined)).toThrow(TypeError);
      expect(() => canonicalizeJson({ a: undefined })).toThrow(TypeError);
      expect(() => canonicalizeJson([1, undefined])).toThrow(TypeError);

      // Unsupported types
      expect(() => canonicalizeJson(() => {})).toThrow(TypeError);
      expect(() => canonicalizeJson(Symbol('sym'))).toThrow(TypeError);
      expect(() => canonicalizeJson(100n)).toThrow(TypeError);

      // Cyclic references
      const cyclicObj: any = {};
      cyclicObj.self = cyclicObj;
      expect(() => canonicalizeJson(cyclicObj)).toThrow(TypeError);

      const cyclicArr: any = [];
      cyclicArr.push(cyclicArr);
      expect(() => canonicalizeJson(cyclicArr)).toThrow(TypeError);

      // Non-plain objects
      expect(() => canonicalizeJson(new Map())).toThrow(TypeError);
      expect(() => canonicalizeJson(new Set())).toThrow(TypeError);
      expect(() => canonicalizeJson(new Date())).toThrow(TypeError);
      expect(() => canonicalizeJson(/regex/)).toThrow(TypeError);
    });
  });
});
