/**
 * Phase 7.3-R5-G: Final Supplier Authorization & Trust Boundary Closure Tests.
 *
 * Implements strict verification for:
 * 1. Task 1: Remove Implicit Supplier Generation Authorization
 *    - Unknown supplier with generation 1 is rejected with REJECTED_INVALID.
 *    - Unknown supplier with any other positive generation is rejected with REJECTED_INVALID.
 *    - Known supplier with exact active generation is accepted.
 *    - Known supplier with stale generation is rejected with REJECTED_INVALID.
 *    - Known supplier with unannounced future generation is rejected with REJECTED_INVALID.
 *    - Rejected unknown-supplier traffic does not mutate engine state (log, pending queue, clock, frontiers, state).
 * 2. Task 2: Clarify & Enforce Authenticated Context Boundary
 *    - Fabricated context from untrusted caller cannot authorize repair ingestion.
 *    - Cloned context from untrusted caller cannot authorize repair ingestion.
 *    - Genuine transport-created context succeeds.
 *    - Wire payload supplier identity cannot override transport identity.
 *    - Historical event authorship remains independent of the current supplier's connection generation.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  createTransportRegistrationCapability,
  createAuthenticatedSupplierContext,
  type EpochTransitionCapability,
  type TransportRegistrationCapability,
  type AuthenticatedSupplierContext,
} from '../index.js';
import type { OperationBatchPayload } from '@syncplay/protocol';

describe('Phase 7.3-R5-G: Final Supplier Authorization Closure', () => {
  let engine: LeaderlessSyncEngine;
  let coordinatorCap: EpochTransitionCapability;
  let transportCap: TransportRegistrationCapability;

  beforeEach(() => {
    coordinatorCap = createEpochTransitionCapability(
      'peer-coord',
      'session-r5g',
      'secret-r5g-coord',
    );
    transportCap = createTransportRegistrationCapability(
      'transport-adapter',
      'session-r5g',
      'secret-r5g-transport',
    );
    engine = new LeaderlessSyncEngine({
      sessionId: 'session-r5g',
      localPeerId: 'peer-local',
      clock: { now: () => 1000 },
      epochCapability: coordinatorCap,
      transportCapability: transportCap,
    });
  });

  function makeSampleBatch(
    supplierPeerId: string,
    originPeerId: string = 'peer-A',
    seq: number = 1,
    clock: number = 1,
  ): OperationBatchPayload {
    return {
      requestId: '00000000-0000-4000-8000-000000000001',
      sessionId: 'session-test',
      supplierPeerId,
      originPeerId,
      fromSequence: seq,
      toSequence: seq,
      operations: [
        {
          logicalEventId: `op-${originPeerId}-${seq}`,
          originPeerId,
          membershipEpoch: 1,
          messageType: 'PLAY',
          logicalTimestamp: clock,
          originSequenceNumber: seq,
          causalContext: {},
          payload: { test: 'r5g-payload' },
        },
      ],
    };
  }

  // ════════════════════════════════════════════════════════════════
  // Task 1: Remove Implicit Supplier Generation Authorization
  // ════════════════════════════════════════════════════════════════
  describe('Task 1: Remove Implicit Supplier Generation Authorization', () => {
    it('unknown supplier with generation 1 is rejected with REJECTED_INVALID', () => {
      // Note: 'peer-unknown' was NEVER registered via authorizePeerGeneration.
      // In R5, this would have fallen back to ?? 1 and been accepted.
      const batch = makeSampleBatch('peer-unknown', 'peer-A', 1, 1);
      const transportCtx = engine.registerInboundSupplierContext('peer-unknown', 1, transportCap);

      const decisions = engine.ingestRepairBatch(batch, transportCtx);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('Unknown or unauthorized repair supplier: peer-unknown');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('unknown supplier with any other positive generation is rejected with REJECTED_INVALID', () => {
      // Test generations 2, 5, 100
      for (const gen of [2, 5, 100]) {
        const batch = makeSampleBatch('peer-ghost', 'peer-A', 1, 1);
        const transportCtx = engine.registerInboundSupplierContext('peer-ghost', gen, transportCap);

        const decisions = engine.ingestRepairBatch(batch, transportCtx);

        expect(decisions).toHaveLength(1);
        expect(decisions[0]!.accepted).toBe(false);
        expect(decisions[0]!.code).toBe('REJECTED_INVALID');
        expect(decisions[0]!.message).toContain('Unknown or unauthorized repair supplier: peer-ghost');
        expect(engine.replicationFrontier['peer-A']).toBeUndefined();
      }
    });

    it('known supplier with exact active generation is accepted', () => {
      engine.authorizePeerGeneration('peer-authorized-supplier', 3);

      const batch = makeSampleBatch('peer-authorized-supplier', 'peer-A', 1, 1);
      const transportCtx = engine.registerInboundSupplierContext('peer-authorized-supplier', 3, transportCap);

      const decisions = engine.ingestRepairBatch(batch, transportCtx);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.getState().status).toBe('playing');
    });

    it('known supplier with a stale generation (< active) is rejected with REJECTED_INVALID', () => {
      // Supplier reconnected and is authorized at generation 3
      engine.authorizePeerGeneration('peer-authorized-supplier', 3);

      const batch = makeSampleBatch('peer-authorized-supplier', 'peer-A', 1, 1);
      // Transport context reports stale generation 2
      const transportCtx = engine.registerInboundSupplierContext('peer-authorized-supplier', 2, transportCap);

      const decisions = engine.ingestRepairBatch(batch, transportCtx);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Repair batch from stale supplier connection generation 2 < active 3',
      );
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('known supplier with an unannounced future generation (> active) is rejected with REJECTED_INVALID', () => {
      // Supplier authorized at generation 2
      engine.authorizePeerGeneration('peer-authorized-supplier', 2);

      const batch = makeSampleBatch('peer-authorized-supplier', 'peer-A', 1, 1);
      // Transport context claims future unannounced generation 4
      const transportCtx = engine.registerInboundSupplierContext('peer-authorized-supplier', 4, transportCap);

      const decisions = engine.ingestRepairBatch(batch, transportCtx);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Repair batch claims unannounced supplier connection generation 4 > authorized 2',
      );
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('rejected unknown-supplier traffic does not mutate engine state', () => {
      // Capture baseline state before rejected batch
      const initialClock = engine.logicalClock;
      const initialEpoch = engine.epoch;
      const initialLogSize = engine.logSize;
      const initialPendingSize = engine.pendingQueueSize;
      const initialLocalFrontier = { ...engine.localFrontier };
      const initialReplicationFrontier = { ...engine.replicationFrontier };
      const initialPlaybackState = { ...engine.getState() };

      const batch = makeSampleBatch('peer-unauthorized-rogue', 'peer-A', 1, 1);
      const transportCtx = engine.registerInboundSupplierContext('peer-unauthorized-rogue', 1, transportCap);

      const decisions = engine.ingestRepairBatch(batch, transportCtx);
      expect(decisions[0]!.accepted).toBe(false);

      // Verify complete state invariance
      expect(engine.logicalClock).toBe(initialClock);
      expect(engine.epoch).toBe(initialEpoch);
      expect(engine.logSize).toBe(initialLogSize);
      expect(engine.pendingQueueSize).toBe(initialPendingSize);
      expect(engine.localFrontier).toEqual(initialLocalFrontier);
      expect(engine.replicationFrontier).toEqual(initialReplicationFrontier);
      expect(engine.getState()).toEqual(initialPlaybackState);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // Task 2: Clarify & Enforce the Authenticated Context Boundary
  // ════════════════════════════════════════════════════════════════
  describe('Task 2: Clarify & Enforce Authenticated Context Boundary', () => {
    it('a fabricated context from an untrusted caller cannot authorize repair ingestion', () => {
      engine.authorizePeerGeneration('peer-S', 1);
      const batch = makeSampleBatch('peer-S', 'peer-A', 1, 1);

      // Untrusted caller manufactures a plain object matching the TypeScript interface
      const fabricatedContext: AuthenticatedSupplierContext = {
        supplierPeerId: 'peer-S',
        connectionGeneration: 1,
      };

      const decisions = engine.ingestRepairBatch(batch, fabricatedContext);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Fabricated or unauthenticated supplier transport context: context must be locally registered by transport adapter',
      );
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('a cloned context from an untrusted caller cannot authorize repair ingestion', () => {
      engine.authorizePeerGeneration('peer-S', 1);
      const batch = makeSampleBatch('peer-S', 'peer-A', 1, 1);

      // Genuine transport context created
      const genuineContext = engine.registerInboundSupplierContext('peer-S', 1, transportCap);

      // Untrusted caller shallow-clones the genuine context
      const clonedContext = { ...genuineContext };

      const decisions = engine.ingestRepairBatch(batch, clonedContext as any);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Fabricated or unauthenticated supplier transport context: context must be locally registered by transport adapter',
      );
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('the genuine transport-created context succeeds', () => {
      engine.authorizePeerGeneration('peer-S', 1);
      const batch = makeSampleBatch('peer-S', 'peer-A', 1, 1);

      // Created via genuine engine / transport adapter registration
      const genuineContext = createAuthenticatedSupplierContext(engine, 'peer-S', 1, transportCap);

      const decisions = engine.ingestRepairBatch(batch, genuineContext);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.getState().status).toBe('playing');
    });

    it('wire payload supplier identity cannot override transport identity', () => {
      engine.authorizePeerGeneration('peer-S', 1);
      engine.authorizePeerGeneration('peer-attacker', 1);

      // Wire payload claims supplier is 'peer-attacker'
      const batch = makeSampleBatch('peer-attacker', 'peer-A', 1, 1);

      // Authenticated transport metadata confirms physical channel is 'peer-S'
      const genuineContext = engine.registerInboundSupplierContext('peer-S', 1, transportCap);

      const decisions = engine.ingestRepairBatch(batch, genuineContext);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain(
        'Forged supplier identity: payload claims peer-attacker but authenticated transport supplier is peer-S',
      );
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('historical event authorship remains independent of the current supplier connection generation', () => {
      // Historical author Peer A was at generation 1 when creating operation
      engine.authorizePeerGeneration('peer-A', 1);
      // Transport supplier Peer S has reconnected multiple times and is currently at generation 7
      engine.authorizePeerGeneration('peer-S', 7);

      const batch = makeSampleBatch('peer-S', 'peer-A', 1, 1);
      const genuineContext = engine.registerInboundSupplierContext('peer-S', 7, transportCap);

      const decisions = engine.ingestRepairBatch(batch, genuineContext);

      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');

      // Author frontier updated under 'peer-A'; supplier 'peer-S' frontier untouched
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.replicationFrontier['peer-S']).toBeUndefined();
    });
  });
});

