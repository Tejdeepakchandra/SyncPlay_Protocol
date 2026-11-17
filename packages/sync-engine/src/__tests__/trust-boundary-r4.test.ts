/**
 * Phase 7.3-R4: Final Trust-Boundary Verification Test Suite.
 *
 * Verifies the three critical trust boundaries:
 * 1. Task 1: Repair Supplier Connection Authorization
 * 2. Task 2: Enforceable Membership Epoch Transition Authorization
 * 3. Task 3: Strict RFC 8785 Canonicalization & Fingerprinting
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  createTransportRegistrationCapability,
  canonicalizeJson,
  sha256,
  computeOperationFingerprint,
  type EpochTransitionCapability,
  type TransportRegistrationCapability,
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

describe('Phase 7.3-R4: Final Trust-Boundary Verification', () => {
  let engine: LeaderlessSyncEngine;
  let simulatedTime: number;
  let epochCapability: EpochTransitionCapability;
  let transportCapability: TransportRegistrationCapability;

  beforeEach(() => {
    simulatedTime = 1000;
    epochCapability = createEpochTransitionCapability(
      'peer-local',
      'test-session',
      'coordinator-secret-token-12345',
    );
    transportCapability = createTransportRegistrationCapability(
      'peer-local',
      'test-session',
      'transport-secret-token-12345',
    );
    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session',
      localPeerId: 'peer-local',
      clock: { now: () => simulatedTime },
      epochCapability,
      transportCapability,
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 1. Task 1: Repair Supplier Connection Authorization
  // ════════════════════════════════════════════════════════════════
  describe('Task 1: Repair Supplier Connection Authorization', () => {
    it('accepts repair batch when supplier connection generation matches authorized generation', () => {
      // Peer S is authorized at generation 2
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

      const decisions = engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 2, transportCapability),
      );
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.getState().status).toBe('playing');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
    });

    it('rejects repair batch when supplier connection generation is stale (< active)', () => {
      // Peer S has reconnected and is authorized at generation 3
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

      // Supplier claims stale generation 2
      const decisions = engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 2, transportCapability),
      );
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('stale supplier connection generation');
      expect(engine.getState().status).toBe('paused');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('rejects repair batch when supplier connection generation is unannounced (> active)', () => {
      // Peer S is authorized at generation 1
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

      // Supplier claims unannounced generation 2
      const decisions = engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 2, transportCapability),
      );
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(false);
      expect(decisions[0]!.code).toBe('REJECTED_INVALID');
      expect(decisions[0]!.message).toContain('unannounced supplier connection generation');
      expect(engine.replicationFrontier['peer-A']).toBeUndefined();
    });

    it('accepts historical operation authored in earlier generation without comparing author generation with supplier generation', () => {
      // Peer S is supplier at authorized generation 2
      engine.authorizePeerGeneration('peer-S', 2);
      // Historical author Peer A is currently at authorized generation 3 (reconnected multiple times)
      engine.authorizePeerGeneration('peer-A', 3);

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

      // Supplier generation 2 is validated; author Peer A's current generation 3 is NOT compared
      const decisions = engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 2, transportCapability),
      );
      expect(decisions).toHaveLength(1);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');
      expect(engine.replicationFrontier['peer-A']).toBe(1);
    });

    it('strictly separates supplier identity from origin identity in causal tracking', () => {
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

      engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 1, transportCapability),
      );

      // Replication vector tracks origin 'peer-A', NOT supplier 'peer-S'
      expect(engine.replicationFrontier['peer-A']).toBe(1);
      expect(engine.replicationFrontier['peer-S']).toBeUndefined();
    });

    it('rejects supplier attempt to rewrite originPeerId inside batch operations', () => {
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
            // Rogue supplier rewrites second operation origin to peer-B
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

      const decisions = engine.ingestRepairBatch(
        batch,
        engine.registerInboundSupplierContext('peer-S', 1, transportCapability),
      );
      expect(decisions).toHaveLength(2);
      expect(decisions[0]!.accepted).toBe(true);
      expect(decisions[0]!.code).toBe('APPLIED');

      // The rewritten operation must be rejected
      expect(decisions[1]!.accepted).toBe(false);
      expect(decisions[1]!.code).toBe('REJECTED_INVALID');
      expect(decisions[1]!.message).toContain('does not match batch originPeerId');
      expect(engine.replicationFrontier['peer-B']).toBeUndefined();
    });

    it('rejects supplier payload alteration on already accepted operation with REJECTED_IDENTITY_CONFLICT', () => {
      engine.authorizePeerGeneration('peer-S', 1);

      // First, op-1 is applied normally
      const opOriginal: OperationBatchPayload = {
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
            payload: { playState: 'original' },
          },
        ],
      };
      const d1 = engine.ingestRepairBatch(
        opOriginal,
        engine.registerInboundSupplierContext('peer-S', 1, transportCapability),
      );
      expect(d1[0]!.accepted).toBe(true);

      // Malicious supplier sends op-1 with altered payload
      const opTampered: OperationBatchPayload = {
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
            payload: { playState: 'tampered-by-supplier' },
          },
        ],
      };
      const d2 = engine.ingestRepairBatch(
        opTampered,
        engine.registerInboundSupplierContext('peer-S', 1, transportCapability),
      );
      expect(d2[0]!.accepted).toBe(false);
      expect(d2[0]!.code).toBe('REJECTED_IDENTITY_CONFLICT');
    });

    it('rejects delayed wire events arriving from a replaced supplier generation', () => {
      // Peer S was at generation 1, now advanced to generation 2
      engine.authorizePeerGeneration('peer-S', 2);

      // Delayed message arrives claiming senderPeerId: 'peer-S' with generation 1
      const delayedEvent = makeRemoteEvent(
        'delayed-ev',
        'peer-A',
        'PLAY',
        {},
        1,
        1,
        {},
        1,
        1, // stale connection generation 1
        'peer-S', // supplier / transport sender
      );

      engine.receive(delayedEvent);
      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_INVALID');
      expect(engine.getLastDecision()?.message).toContain('stale connection generation');
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 2. Task 2: Enforceable Membership Epoch Transition Authorization
  // ════════════════════════════════════════════════════════════════
  describe('Task 2: Enforceable Membership Epoch Transition Authorization', () => {
    it('authorized coordinator holding capability can successfully transition epoch', () => {
      expect(epochCapability.sessionId).toBe('test-session');
      expect(epochCapability.authority).toBe('peer-local');
      expect(epochCapability.token).toBe('coordinator-secret-token-12345');

      engine.transitionEpoch(2, epochCapability);
      expect(engine.epoch).toBe(2);
    });

    it('arbitrary local caller cannot obtain the capability through the engine API', () => {
      expect((engine as any).getEpochTransitionCapability).toBeUndefined();
    });

    it('caller with plain token string cannot bypass capability ownership', () => {
      expect(() => (engine as any).transitionEpoch(2, 'coordinator-secret-token-12345')).toThrow(
        SyncEngineError,
      );
      try {
        (engine as any).transitionEpoch(2, 'coordinator-secret-token-12345');
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('unauthorized caller without capability/token throws UNAUTHORIZED SyncEngineError', () => {
      expect(() => (engine as any).transitionEpoch(2)).toThrow(SyncEngineError);
      try {
        (engine as any).transitionEpoch(2);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
        expect(err.message).toContain('Epoch transition requires authorized EpochTransitionCapability');
      }
      expect(engine.epoch).toBe(1);
    });

    it('caller with invalid or forged token throws UNAUTHORIZED', () => {
      const forgedCapability = {
        authority: 'rogue-peer',
        sessionId: 'test-session',
        token: 'forged-token-abc',
      };

      expect(() => engine.transitionEpoch(2, forgedCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, forgedCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('caller with mismatched sessionId throws UNAUTHORIZED', () => {
      const mismatchedCapability = {
        authority: 'peer-local',
        sessionId: 'wrong-session-999',
        token: 'coordinator-secret-token-12345',
      };

      expect(() => engine.transitionEpoch(2, mismatchedCapability)).toThrow(SyncEngineError);
      try {
        engine.transitionEpoch(2, mismatchedCapability);
      } catch (err: any) {
        expect(err.code).toBe('UNAUTHORIZED');
      }
      expect(engine.epoch).toBe(1);
    });

    it('wire remote event claiming future epoch cannot establish or advance epoch', () => {
      expect(engine.epoch).toBe(1);

      // Malicious or premature remote event claiming epoch 2
      const futureEpochEvent = makeRemoteEvent('ev-future', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(futureEpochEvent);

      expect(engine.getLastDecision()?.accepted).toBe(false);
      expect(engine.getLastDecision()?.code).toBe('REJECTED_OBSOLETE_EPOCH');
      expect(engine.getLastDecision()?.message).toContain('unestablished future epoch');
      // Epoch remains 1
      expect(engine.epoch).toBe(1);
    });

    it('purges obsolete pending queue operations during authorized transition and preserves state', () => {
      // 1. Buffer an operation in epoch 1 waiting for missing dependency
      const pendingOldOp = makeRemoteEvent(
        'pending-old',
        'peer-A',
        'PLAY',
        {},
        2,
        2,
        { 'peer-A': 1 },
        1,
      );
      engine.receive(pendingOldOp);
      expect(engine.getLastDecision()?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(engine.pendingQueueSize).toBe(1);

      // 2. Authorized transition
      engine.transitionEpoch(2, epochCapability);
      expect(engine.epoch).toBe(2);

      // 3. Pending queue purged
      expect(engine.pendingQueueSize).toBe(0);

      // 4. Predecessor arriving in epoch 2 cannot unblock the purged epoch 1 operation
      const predOp = makeRemoteEvent('pred-ep2', 'peer-A', 'PLAY', {}, 1, 1, {}, 2);
      engine.receive(predOp);
      expect(engine.getLastDecision()?.code).toBe('APPLIED');
      expect(engine.pendingQueueSize).toBe(0);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 3. Task 3: Strict RFC 8785 Canonicalization
  // ════════════════════════════════════════════════════════════════
  describe('Task 3: Strict RFC 8785 Canonicalization & Fingerprinting', () => {
    it('strictly rejects non-finite numbers (NaN, Infinity, -Infinity) with TypeError', () => {
      expect(() => canonicalizeJson(NaN)).toThrow(TypeError);
      expect(() => canonicalizeJson(Infinity)).toThrow(TypeError);
      expect(() => canonicalizeJson(-Infinity)).toThrow(TypeError);

      expect(() => canonicalizeJson({ a: NaN })).toThrow(TypeError);
      expect(() => canonicalizeJson({ val: Infinity })).toThrow(TypeError);
      expect(() => canonicalizeJson([1, 2, NaN])).toThrow(TypeError);
      expect(() => canonicalizeJson([Infinity])).toThrow(TypeError);
    });

    it('strictly rejects undefined values with TypeError', () => {
      expect(() => canonicalizeJson(undefined)).toThrow(TypeError);
      expect(() => canonicalizeJson({ a: undefined })).toThrow(TypeError);
      expect(() => canonicalizeJson([1, undefined, 2])).toThrow(TypeError);
    });

    it('strictly rejects functions, symbols, and bigints with TypeError', () => {
      expect(() => canonicalizeJson(() => {})).toThrow(TypeError);
      expect(() => canonicalizeJson(Symbol('sym'))).toThrow(TypeError);
      expect(() => canonicalizeJson(100n)).toThrow(TypeError);

      expect(() => canonicalizeJson({ fn: () => {} })).toThrow(TypeError);
      expect(() => canonicalizeJson({ s: Symbol('test') })).toThrow(TypeError);
      expect(() => canonicalizeJson([100n])).toThrow(TypeError);
    });

    it('strictly rejects cyclic object and array references with TypeError', () => {
      const cyclicObj: any = { name: 'cyclic' };
      cyclicObj.self = cyclicObj;
      expect(() => canonicalizeJson(cyclicObj)).toThrow(TypeError);

      const cyclicArr: any = [1, 2];
      cyclicArr.push(cyclicArr);
      expect(() => canonicalizeJson(cyclicArr)).toThrow(TypeError);
    });

    it('strictly rejects non-plain object instances (Map, Set, Date, RegExp) with TypeError', () => {
      expect(() => canonicalizeJson(new Map())).toThrow(TypeError);
      expect(() => canonicalizeJson(new Set())).toThrow(TypeError);
      expect(() => canonicalizeJson(new Date())).toThrow(TypeError);
      expect(() => canonicalizeJson(/regex/)).toThrow(TypeError);

      class CustomEntity {
        id = 1;
      }
      expect(() => canonicalizeJson(new CustomEntity())).toThrow(TypeError);
    });

    it('canonicalizes negative zero (-0) to "0"', () => {
      expect(canonicalizeJson(-0)).toBe('0');
      expect(canonicalizeJson(0)).toBe('0');
      expect(canonicalizeJson({ zero: -0 })).toBe('{"zero":0}');
      expect(canonicalizeJson([-0, 0])).toBe('[0,0]');
    });

    it('strictly sorts object properties by UTF-16 code units', () => {
      const obj = {
        z: 1,
        a: 2,
        '10': 3,
        '2': 4,
        '1': 5,
        b: 6,
      };
      // UTF-16 code units sort: "1", "10", "2", "a", "b", "z"
      expect(canonicalizeJson(obj)).toBe('{"1":5,"10":3,"2":4,"a":2,"b":6,"z":1}');
    });

    it('deterministically canonicalizes nested objects and arrays', () => {
      const complex1 = {
        b: [3, { y: true, x: false }, 1],
        a: { beta: null, alpha: 'text' },
      };
      const complex2 = {
        a: { alpha: 'text', beta: null },
        b: [3, { x: false, y: true }, 1],
      };

      const c1 = canonicalizeJson(complex1);
      const c2 = canonicalizeJson(complex2);
      expect(c1).toBe(c2);
      expect(c1).toBe('{"a":{"alpha":"text","beta":null},"b":[3,{"x":false,"y":true},1]}');
    });

    it('produces standard SHA-256 test vectors', () => {
      // Empty string SHA-256
      expect(sha256('')).toBe(
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      );
      // 'abc' SHA-256
      expect(sha256('abc')).toBe(
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      );
    });

    it('computes identical operation fingerprints for identical inputs regardless of key order', () => {
      const op1 = {
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        logicalEventId: 'ev-1',
        logicalTimestamp: 1,
        causalContext: { 'peer-B': 2, 'peer-A': 1 },
        messageType: 'PLAY' as const,
        payload: { targetPositionMs: 500, extra: 'value' },
        membershipEpoch: 1,
      };

      const op2 = {
        membershipEpoch: 1,
        payload: { extra: 'value', targetPositionMs: 500 },
        messageType: 'PLAY' as const,
        causalContext: { 'peer-A': 1, 'peer-B': 2 },
        logicalTimestamp: 1,
        logicalEventId: 'ev-1',
        originSequenceNumber: 1,
        originPeerId: 'peer-A',
      };

      const fp1 = computeOperationFingerprint(op1);
      const fp2 = computeOperationFingerprint(op2);
      expect(fp1).toHaveLength(64);
      expect(fp1).toBe(fp2);
    });

    it('computes different fingerprints if any immutable property differs', () => {
      const baseOp = {
        originPeerId: 'peer-A',
        originSequenceNumber: 1,
        logicalEventId: 'ev-1',
        logicalTimestamp: 1,
        causalContext: {},
        messageType: 'PLAY' as const,
        payload: { position: 100 },
        membershipEpoch: 1,
      };
      const baseFp = computeOperationFingerprint(baseOp);

      expect(computeOperationFingerprint({ ...baseOp, originPeerId: 'peer-B' })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, originSequenceNumber: 2 })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, logicalEventId: 'ev-2' })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, logicalTimestamp: 2 })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, causalContext: { 'peer-A': 1 } })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, messageType: 'PAUSE' })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, payload: { position: 101 } })).not.toBe(baseFp);
      expect(computeOperationFingerprint({ ...baseOp, membershipEpoch: 2 })).not.toBe(baseFp);
    });
  });
});

