/**
 * Phase 7.3-R5-H: Final Transport Context Issuer Authorization Tests.
 *
 * Implements strict verification for Task 2 adversarial tests:
 * 1. A normal application caller cannot register a supplier context.
 * 2. A caller cannot use the public engine API to mint an accepted context.
 * 3. A fabricated plain object is rejected.
 * 4. A cloned genuine context is rejected.
 * 5. A context registered by the trusted transport adapter is accepted.
 * 6. A caller without the registration capability is rejected.
 * 7. A separately created or mismatched capability is rejected.
 * 8. An unknown supplier is rejected even when a registration attempt is made.
 * 9. A stale or unannounced supplier generation is rejected.
 * 10. Rejected attempts do not alter the operation log, pending queue, Lamport clock, causal frontier, replication vector, or playback state.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  createTransportRegistrationCapability,
  createAuthenticatedSupplierContext,
  TrustedTransportAdapter,
  type EpochTransitionCapability,
  type TransportRegistrationCapability,
  type AuthenticatedSupplierContext,
} from '../index.js';
import { SyncEngineError } from '../errors.js';
import type { OperationBatchPayload } from '@syncplay/protocol';

describe('Phase 7.3-R5-H: Final Transport Context Issuer Authorization', () => {
  let engine: LeaderlessSyncEngine;
  let coordinatorCap: EpochTransitionCapability;
  let transportCap: TransportRegistrationCapability;
  let transportAdapter: TrustedTransportAdapter;

  beforeEach(() => {
    coordinatorCap = createEpochTransitionCapability(
      'peer-coord',
      'session-r5h',
      'secret-r5h-coord',
    );
    transportCap = createTransportRegistrationCapability(
      'transport-adapter',
      'session-r5h',
      'secret-r5h-transport',
    );

    // Trusted composition root instantiates engine with both capabilities
    engine = new LeaderlessSyncEngine({
      sessionId: 'session-r5h',
      localPeerId: 'peer-local',
      clock: { now: () => 1000 },
      epochCapability: coordinatorCap,
      transportCapability: transportCap,
    });

    // Trusted transport adapter receives engine and transport capability
    transportAdapter = new TrustedTransportAdapter(engine, transportCap);
  });

  function makeSampleBatch(
    supplierPeerId: string,
    originPeerId: string = 'peer-A',
    seq: number = 1,
    clock: number = 1,
    causalContext: Record<string, number> = {},
    messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE' = 'PLAY',
  ): OperationBatchPayload {
    return {
      supplierPeerId,
      originPeerId,
      fromSequence: seq,
      toSequence: seq,
      operations: [
        {
          logicalEventId: `op-${originPeerId}-${seq}`,
          originPeerId,
          membershipEpoch: 1,
          messageType,
          logicalTimestamp: clock,
          originSequenceNumber: seq,
          causalContext,
          payload: messageType === 'SEEK' ? { targetPositionMs: 5000 } : { test: 'r5h-payload' },
        },
      ],
    };
  }

  // 1. A normal application caller cannot register a supplier context
  it('1. normal application caller without capability cannot register a supplier context', () => {
    // Normal application caller holds only engine reference and has no capability
    expect(() => {
      (engine as any).registerInboundSupplierContext('peer-S', 1);
    }).toThrow(SyncEngineError);

    try {
      (engine as any).registerInboundSupplierContext('peer-S', 1);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      expect((err as SyncEngineError).message).toContain(
        'Supplier context registration requires authorized TransportRegistrationCapability',
      );
    }

    // Public engine API does not expose a getter for the capability
    expect((engine as any).getTransportCapability).toBeUndefined();
    expect((engine as any).getSupplierCapability).toBeUndefined();
  });

  // 2. A caller cannot use the public engine API to mint an accepted context
  it('2. caller cannot use the public helper API to mint an accepted context without capability', () => {
    // Calling exported createAuthenticatedSupplierContext without capability throws UNAUTHORIZED
    expect(() => {
      createAuthenticatedSupplierContext(engine, 'peer-S', 1, undefined as any);
    }).toThrow(SyncEngineError);

    // Calling with a bogus capability throws UNAUTHORIZED
    const bogusCap = {
      authority: 'attacker',
      sessionId: 'session-r5h',
      token: 'attacker-token',
    } as TransportRegistrationCapability;

    expect(() => {
      createAuthenticatedSupplierContext(engine, 'peer-S', 1, bogusCap);
    }).toThrow(SyncEngineError);
  });

  // 3. A fabricated plain object is rejected
  it('3. fabricated plain object is rejected by ingestRepairBatch', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    const batch = makeSampleBatch('peer-S');

    // Attacker crafts plain object literal
    const fabricatedContext = {
      supplierPeerId: 'peer-S',
      connectionGeneration: 1,
    } as AuthenticatedSupplierContext;

    const decisions = engine.ingestRepairBatch(batch, fabricatedContext);
    expect(decisions).toHaveLength(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toContain('Fabricated or unauthenticated supplier transport context');
  });

  // 4. A cloned genuine context is rejected
  it('4. cloned genuine context is rejected by ingestRepairBatch', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    const batch = makeSampleBatch('peer-S');

    // Trusted transport adapter registers genuine context
    const genuineContext = transportAdapter.registerContext('peer-S', 1);

    // Attacker creates a shallow clone of the genuine context
    const clonedContext = { ...genuineContext };

    const decisions = engine.ingestRepairBatch(batch, clonedContext as any);
    expect(decisions).toHaveLength(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toContain('Fabricated or unauthenticated supplier transport context');
  });

  // 5. A context registered by the trusted transport adapter is accepted
  it('5. context registered by trusted transport adapter is accepted', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    const batch = makeSampleBatch('peer-S');

    // Via transport adapter registerContext
    const genuineContext = transportAdapter.registerContext('peer-S', 1);
    const decisions = engine.ingestRepairBatch(batch, genuineContext);

    expect(decisions).toHaveLength(1);
    expect(decisions[0]!.accepted).toBe(true);
    expect(decisions[0]!.code).toBe('APPLIED');
    expect(engine.getState().status).toBe('playing');
    expect(engine.replicationFrontier['peer-A']).toBe(1);

    // Or directly via transportAdapter.ingestRepairBatch
    const batch2 = makeSampleBatch('peer-S', 'peer-A', 2, 2, { 'peer-A': 1 }, 'PAUSE');
    const decisions2 = transportAdapter.ingestRepairBatch(batch2, 'peer-S', 1);
    expect(decisions2).toHaveLength(1);
    expect(decisions2[0]!.accepted).toBe(true);
    expect(decisions2[0]!.code).toBe('APPLIED');
    expect(engine.getState().status).toBe('paused');
    expect(engine.replicationFrontier['peer-A']).toBe(2);
  });

  // 6. A caller without the registration capability is rejected
  it('6. caller without registration capability is rejected with UNAUTHORIZED', () => {
    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, null as any);
    }).toThrow(SyncEngineError);

    // An engine instantiated without capability rejects even if someone passes a capability
    const unmanagedEngine = new LeaderlessSyncEngine({
      sessionId: 'session-r5h',
      localPeerId: 'peer-unmanaged',
      clock: { now: () => 1000 },
      // No transportCapability provided
    });

    expect(() => {
      unmanagedEngine.registerInboundSupplierContext('peer-S', 1, transportCap);
    }).toThrow(SyncEngineError);

    try {
      unmanagedEngine.registerInboundSupplierContext('peer-S', 1, transportCap);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
    }
  });

  // 7. A separately created or mismatched capability is rejected
  it('7. separately created or mismatched capability is rejected', () => {
    // Separate capability created with identical or different parameters
    const forgedCap = createTransportRegistrationCapability(
      'transport-adapter',
      'session-r5h',
      'different-secret-token',
    );

    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, forgedCap);
    }).toThrow(SyncEngineError);

    // Shallow copy of authorized capability also fails reference identity check
    const clonedCap = { ...transportCap };
    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, clonedCap as any);
    }).toThrow(SyncEngineError);
  });

  // 8. An unknown supplier is rejected even when a registration attempt is made
  it('8. unknown supplier is rejected even when registered by transport adapter', () => {
    // Adapter registers context for peer-unknown, but peer-unknown was never authorized in peerGenerations
    const unknownCtx = transportAdapter.registerContext('peer-unknown', 1);
    const batch = makeSampleBatch('peer-unknown');

    const decisions = engine.ingestRepairBatch(batch, unknownCtx);
    expect(decisions).toHaveLength(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toBe('Unknown or unauthorized repair supplier: peer-unknown');
  });

  // 9. A stale or unannounced supplier generation is rejected
  it('9. stale or unannounced supplier generation is rejected', () => {
    engine.authorizePeerGeneration('peer-S', 3);
    const batch = makeSampleBatch('peer-S');

    // Stale generation 2 < 3
    const staleCtx = transportAdapter.registerContext('peer-S', 2);
    const decisionsStale = engine.ingestRepairBatch(batch, staleCtx);
    expect(decisionsStale).toHaveLength(1);
    expect(decisionsStale[0]!.accepted).toBe(false);
    expect(decisionsStale[0]!.code).toBe('REJECTED_INVALID');
    expect(decisionsStale[0]!.message).toContain(
      'Repair batch from stale supplier connection generation 2 < active 3',
    );

    // Unannounced future generation 4 > 3
    const futureCtx = transportAdapter.registerContext('peer-S', 4);
    const decisionsFuture = engine.ingestRepairBatch(batch, futureCtx);
    expect(decisionsFuture).toHaveLength(1);
    expect(decisionsFuture[0]!.accepted).toBe(false);
    expect(decisionsFuture[0]!.code).toBe('REJECTED_INVALID');
    expect(decisionsFuture[0]!.message).toContain(
      'unannounced supplier connection generation',
    );
  });

  // 10. Rejected attempts do not alter the operation log, pending queue, Lamport clock, causal frontier, replication vector, or playback state
  it('10. rejected attempts produce zero state mutations across engine internals', () => {
    // Record initial invariant state
    const initialLogSize = engine.logSize;
    const initialPendingSize = engine.pendingQueueSize;
    const initialClock = engine.logicalClock;
    const initialLocalFrontier = { ...engine.localFrontier };
    const initialReplicationFrontier = { ...engine.replicationFrontier };
    const initialState = { ...engine.getState() };

    expect(initialLogSize).toBe(0);
    expect(initialPendingSize).toBe(0);
    expect(initialClock).toBe(0);
    expect(Object.keys(initialLocalFrontier)).toHaveLength(0);
    expect(Object.keys(initialReplicationFrontier)).toHaveLength(0);
    expect(initialState.status).toBe('paused');
    expect(initialState.stateVersion).toBe(0);

    const batch = makeSampleBatch('peer-adversary');

    // Attempt 1: Unauthorized registration call (throws UNAUTHORIZED)
    expect(() => {
      (engine as any).registerInboundSupplierContext('peer-adversary', 1);
    }).toThrow(SyncEngineError);

    // Attempt 2: Registration with forged capability (throws UNAUTHORIZED)
    const forgedCap = createTransportRegistrationCapability('hacker', 'session-r5h');
    expect(() => {
      engine.registerInboundSupplierContext('peer-adversary', 1, forgedCap);
    }).toThrow(SyncEngineError);

    // Attempt 3: Ingest with fabricated plain context (REJECTED_INVALID)
    const decisionsFab = engine.ingestRepairBatch(batch, {
      supplierPeerId: 'peer-adversary',
      connectionGeneration: 1,
    } as any);
    expect(decisionsFab[0]!.accepted).toBe(false);

    // Attempt 4: Ingest with cloned genuine context (REJECTED_INVALID)
    engine.authorizePeerGeneration('peer-S', 1);
    const genuine = transportAdapter.registerContext('peer-S', 1);
    const decisionsClone = engine.ingestRepairBatch(batch, { ...genuine });
    expect(decisionsClone[0]!.accepted).toBe(false);

    // Attempt 5: Ingest with unknown supplier (REJECTED_INVALID)
    const unknownCtx = transportAdapter.registerContext('peer-unknown', 1);
    const decisionsUnknown = engine.ingestRepairBatch(makeSampleBatch('peer-unknown'), unknownCtx);
    expect(decisionsUnknown[0]!.accepted).toBe(false);

    // Attempt 6: Ingest with stale generation (REJECTED_INVALID)
    engine.authorizePeerGeneration('peer-S', 5);
    const staleCtx = transportAdapter.registerContext('peer-S', 4);
    const decisionsStale = engine.ingestRepairBatch(makeSampleBatch('peer-S'), staleCtx);
    expect(decisionsStale[0]!.accepted).toBe(false);

    // Attempt 7: Ingest with unannounced generation (REJECTED_INVALID)
    const futureCtx = transportAdapter.registerContext('peer-S', 6);
    const decisionsFuture = engine.ingestRepairBatch(makeSampleBatch('peer-S'), futureCtx);
    expect(decisionsFuture[0]!.accepted).toBe(false);

    // Invariant verification: Every internal state metric must remain identical to initial state
    expect(engine.logSize).toBe(initialLogSize);
    expect(engine.pendingQueueSize).toBe(initialPendingSize);
    expect(engine.logicalClock).toBe(initialClock);
    expect(engine.localFrontier).toEqual(initialLocalFrontier);
    expect(engine.replicationFrontier).toEqual(initialReplicationFrontier);
    expect(engine.getState()).toEqual(initialState);
  });
});
