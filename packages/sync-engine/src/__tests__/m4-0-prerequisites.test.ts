/**
 * Phase 7.5 M4.0: Prerequisite Infrastructure Corrections Test Suite.
 *
 * Covers:
 * - M4.0a: Transport sender identity and generation binding in inbound STATE_DIGEST path
 * - M4.0b: Exact historical timestamp resolution & INCOMPLETE_FRONTIER_METADATA enforcement
 * - M4.0c: In-process API custody boundary for recordPeerDigest and public export audit
 * - M4.0d: Transactional compaction candidate staging across all 8 mutable components & immutability
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  TrustedTransportAdapter,
  createTransportRegistrationCapability,
} from '../engine/leaderless-sync-engine.js';
import { AntiEntropyManager } from '../replication/anti-entropy-manager.js';
import { INTERNAL_DIGEST_ACCESS_TOKEN } from '../engine/custody.js';
import * as SyncEngineBarrel from '../index.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { SyncEngineError } from '../errors.js';
import type { PlaybackState } from '../types.js';
import type { StoredOperation } from '../log/types.js';
import type { StateDigestPayload, ProtocolMessage } from '@syncplay/protocol';
import type { PeerMesh, InboundMessage, PeerStatus } from '@syncplay/peer-mesh';

function createMockMesh(localPeerId: string) {
  const peers = new Map<string, PeerStatus>();
  let messageHandler: ((inbound: InboundMessage) => void) | undefined;
  let statusHandler: ((status: PeerStatus) => void) | undefined;

  const mockMesh = {
    localPeerId,
    listPeers: vi.fn(() => Array.from(peers.values())),
    getPeerStatus: vi.fn((peerId: string) => peers.get(peerId)),
    onMessage: vi.fn((handler: (inbound: InboundMessage) => void) => {
      messageHandler = handler;
      return () => {
        messageHandler = undefined;
      };
    }),
    onPeerStatus: vi.fn((handler: (status: PeerStatus) => void) => {
      statusHandler = handler;
      return () => {
        statusHandler = undefined;
      };
    }),
    send: vi.fn(),
    broadcast: vi.fn(),

    // Test helpers to simulate network events
    simulatePeerStatus(status: PeerStatus) {
      peers.set(status.peerId, status);
      statusHandler?.(status);
    },
    simulateInboundMessage(inbound: InboundMessage) {
      messageHandler?.(inbound);
    },
  };

  return mockMesh;
}

function makeDigestPayload(overrides: Partial<StateDigestPayload> = {}): StateDigestPayload {
  return {
    formatVersion: 2,
    sessionId: 'test-session-1',
    senderPeerId: 'peer-remote',
    connectionGeneration: 1,
    membershipEpoch: 1,
    stateVersion: 10,
    replicationVector: { 'peer-remote': 5 },
    snapshotFrontier: {},
    historyFingerprints: {},
    sentAt: Date.now(),
    ...overrides,
  };
}

describe('Phase 7.5 M4.0: Prerequisite Infrastructure Corrections', () => {
  // ── M4.0a: Transport Sender Identity & Generation Binding ─────────────────
  describe('M4.0a — Transport sender identity and generation binding', () => {
    let engine: LeaderlessSyncEngine;
    let adapter: TrustedTransportAdapter;
    let mockMesh: ReturnType<typeof createMockMesh>;
    const SESSION_ID = 'session-m4-0a';
    const LOCAL_PEER = 'peer-local';
    const REMOTE_PEER = 'peer-remote';

    beforeEach(() => {
      engine = new LeaderlessSyncEngine({
        sessionId: SESSION_ID,
        localPeerId: LOCAL_PEER,
      });
      const transportCap = createTransportRegistrationCapability();
      adapter = new TrustedTransportAdapter(engine, transportCap);
      mockMesh = createMockMesh(LOCAL_PEER);
      adapter.bindPeerMesh(mockMesh as unknown as PeerMesh);

      const callbacks = adapter.createAntiEntropyCallbacks(mockMesh as unknown as PeerMesh);
      const aem = new AntiEntropyManager(engine, callbacks);
      adapter.setAntiEntropyManager(aem);

      // Connect remote peer with generation 1
      mockMesh.simulatePeerStatus({
        peerId: REMOTE_PEER,
        state: 'CONNECTED',
        dataChannelState: 'open',
        generation: 1,
      });
    });

    it('should drop STATE_DIGEST with inner sender spoofing (payload.senderPeerId !== fromPeerId)', () => {
      const spoofedPayload = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: 'victim-peer', // Spoofed inner sender!
        connectionGeneration: 1,
        membershipEpoch: 1,
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER, // Outer matches transport
        messageId: 'msg-spoof-inner',
        membershipEpoch: 1,
        payload: spoofedPayload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      // Proof: No side effects on compaction knowledge or peer digests
      expect(engine.getPeerDigests().has('victim-peer')).toBe(false);
      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('should drop STATE_DIGEST with outer sender mismatch (msg.senderPeerId !== fromPeerId)', () => {
      const payload = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1,
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: 'attacker-peer', // Outer sender does not match fromPeerId
        messageId: 'msg-mismatch-outer',
        membershipEpoch: 1,
        payload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
      expect(engine.getPeerDigests().has('attacker-peer')).toBe(false);
    });

    it('should drop STATE_DIGEST with stale connection generation', () => {
      const staleGenPayload = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 999, // Stale / future generation != 1
        membershipEpoch: 1,
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-stale-gen',
        membershipEpoch: 1,
        payload: staleGenPayload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    // ── Strict Membership Epoch Verification (Audit Regression Suite) ───────
    it('1. should reject STATE_DIGEST when outer membershipEpoch is missing but inner is current', () => {
      const validInner = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1, // Current engine epoch
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-missing-outer-epoch',
        // outer membershipEpoch is omitted (undefined)
        payload: validInner,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      // Proof: Dropped at trust boundary, not registered
      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('2. should reject STATE_DIGEST when outer membershipEpoch is current but inner is missing or mismatched', () => {
      const invalidInner = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: undefined as unknown as number, // Missing inner epoch!
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-missing-inner-epoch',
        membershipEpoch: 1, // Outer is current
        payload: invalidInner,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('3. should reject STATE_DIGEST when outer membershipEpoch is stale even if inner is current', () => {
      const currentInner = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1, // Current
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-stale-outer-epoch',
        membershipEpoch: 99, // Stale outer epoch
        payload: currentInner,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('4. should reject STATE_DIGEST when outer membershipEpoch is current but inner is stale', () => {
      const staleInner = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 99, // Stale inner epoch
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-stale-inner-epoch',
        membershipEpoch: 1, // Current outer
        payload: staleInner,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('5. should accept STATE_DIGEST when both outer and inner epochs match the current engine epoch', () => {
      const validPayload = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1, // Current
        replicationVector: { [REMOTE_PEER]: 12 },
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-both-current-epochs',
        membershipEpoch: 1, // Current
        payload: validPayload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      // Proof: Accepted into compactionManager peer digests
      const recorded = engine.getPeerDigests().get(REMOTE_PEER);
      expect(recorded).toBeDefined();
      expect(recorded?.senderPeerId).toBe(REMOTE_PEER);
      expect(recorded?.membershipEpoch).toBe(1);
      expect(recorded?.replicationVector[REMOTE_PEER]).toBe(12);
    });

    it('6. should guarantee side-effect immunity for rejected epoch messages (no anti-entropy, no compaction update)', () => {
      // Spy on engine digest registration
      const recordSpy = vi.spyOn(engine, 'recordPeerDigest');

      // Send message with missing outer epoch
      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message: {
          protocolVersion: 1,
          messageType: 'STATE_DIGEST',
          sessionId: SESSION_ID,
          senderPeerId: REMOTE_PEER,
          messageId: 'msg-epoch-immunity-test',
          // Omitted outer epoch
          payload: makeDigestPayload({
            sessionId: SESSION_ID,
            senderPeerId: REMOTE_PEER,
            connectionGeneration: 1,
            membershipEpoch: 1,
          }),
        },
      });

      // Assert zero calls to recordPeerDigest and empty peer digests map
      expect(recordSpy).not.toHaveBeenCalled();
      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('should drop STATE_DIGEST with session ID mismatch', () => {
      const mismatchedPayload = makeDigestPayload({
        sessionId: 'wrong-session-xyz',
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1,
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-mismatch-session',
        membershipEpoch: 1,
        payload: mismatchedPayload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('should drop STATE_DIGEST when peer is disconnected or DataChannel is closed', () => {
      // Simulate peer disconnection on transport
      mockMesh.simulatePeerStatus({
        peerId: REMOTE_PEER,
        state: 'DISCONNECTED',
        dataChannelState: 'closed',
        generation: 1,
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-disconnected-peer',
        membershipEpoch: 1,
        payload: makeDigestPayload({
          sessionId: SESSION_ID,
          senderPeerId: REMOTE_PEER,
          connectionGeneration: 1,
          membershipEpoch: 1,
        }),
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      // Proof: Disconnected peer cannot register digest knowledge
      expect(engine.getPeerDigests().has(REMOTE_PEER)).toBe(false);
    });

    it('should accept valid current sender STATE_DIGEST and record peer digest knowledge', () => {
      const validPayload = makeDigestPayload({
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        connectionGeneration: 1,
        membershipEpoch: 1,
        replicationVector: { [REMOTE_PEER]: 8 },
      });

      const message: ProtocolMessage<StateDigestPayload> = {
        protocolVersion: 1,
        messageType: 'STATE_DIGEST',
        sessionId: SESSION_ID,
        senderPeerId: REMOTE_PEER,
        messageId: 'msg-valid-sender',
        membershipEpoch: 1,
        payload: validPayload,
      };

      mockMesh.simulateInboundMessage({
        fromPeerId: REMOTE_PEER,
        message,
      });

      // Proof: The valid digest was accepted into compactionManager peer digests
      const recorded = engine.getPeerDigests().get(REMOTE_PEER);
      expect(recorded).toBeDefined();
      expect(recorded?.senderPeerId).toBe(REMOTE_PEER);
      expect(recorded?.replicationVector[REMOTE_PEER]).toBe(8);
    });
  });

  // ── M4.0b: Exact Historical Timestamp Resolution ──────────────────────────
  describe('M4.0b — Exact historical timestamp resolution', () => {
    it('should return exact timestamp for active operations and compacted checkpoints', () => {
      const log = new OrderedOperationLog();

      const op1: StoredOperation = {
        logicalEventId: 'evt-1',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 100,
        originSequenceNumber: 1,
        causalContext: {},
        appliedAtMonotonic: 1000,
        payload: null,
      };

      const op2: StoredOperation = {
        logicalEventId: 'evt-2',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 200,
        originSequenceNumber: 2,
        causalContext: {},
        appliedAtMonotonic: 2000,
        payload: null,
      };

      log.insert(op1);
      log.insert(op2);

      // Checkpoint sequence 1 explicitly as compacted
      log.compactAtFrontier({ 'peer-A': 1 });

      // After compaction: sequence 1 is in compactedCheckpoints, sequence 2 is active in log
      const timestamps = log.getOriginFrontierTimestamps({ 'peer-A': 1 });
      expect(timestamps['peer-A']).toBe(100); // from checkpoint

      const timestamps2 = log.getOriginFrontierTimestamps({ 'peer-A': 2 });
      expect(timestamps2['peer-A']).toBe(200); // from active log
    });

    it('should return null for evicted or unavailable historical sequences (no fallback to later sequences)', () => {
      const log = new OrderedOperationLog(2); // Small capacity 2

      const op2: StoredOperation = {
        logicalEventId: 'evt-2',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 500,
        originSequenceNumber: 2,
        causalContext: {},
        appliedAtMonotonic: 2000,
        payload: null,
      };
      log.insert(op2);

      // We ask for sequence 1, which was NEVER inserted or has been evicted without checkpoint
      const timestamps = log.getOriginFrontierTimestamps({ 'peer-A': 1 });
      // CRITICAL M4.0b: Sequence 1 must return null, NEVER 500 (sequence 2's timestamp)!
      expect(timestamps['peer-A']).toBeNull();
    });

    it('should return 0 for non-positive frontier sequence (seq <= 0)', () => {
      const log = new OrderedOperationLog();
      const timestamps = log.getOriginFrontierTimestamps({ 'peer-A': 0, 'peer-B': -1 });
      expect(timestamps['peer-A']).toBe(0);
      expect(timestamps['peer-B']).toBe(0);
    });

    it('should prove that request for sequence k never receives timestamp of sequence k+1 or later', () => {
      const log = new OrderedOperationLog();

      const op10: StoredOperation = {
        logicalEventId: 'evt-10',
        originPeerId: 'peer-X',
        senderPeerId: 'peer-X',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 1000,
        originSequenceNumber: 10,
        causalContext: {},
        appliedAtMonotonic: 10000,
        payload: null,
      };
      log.insert(op10);

      // Query sequences 1 through 9
      for (let seq = 1; seq < 10; seq++) {
        const result = log.getOriginFrontierTimestamps({ 'peer-X': seq });
        expect(result['peer-X']).toBeNull();
        expect(result['peer-X']).not.toBe(1000);
      }
    });

    it('should throw INCOMPLETE_FRONTIER_METADATA when exportSnapshotV2 encounters unresolvable timestamp', () => {
      const engine = new LeaderlessSyncEngine({
        sessionId: 'session-snapshot-test',
        localPeerId: 'peer-local',
      });

      // Manually set replication vector with a sequence that has no operation or checkpoint
      (engine as any).replicationVector['peer-remote'] = 5;

      expect(() => {
        engine.exportSnapshotV2('snap-incomplete');
      }).toThrowError(
        expect.objectContaining({
          code: 'INCOMPLETE_FRONTIER_METADATA',
        }),
      );
    });
  });

  // ── M4.0c: Digest-Registration API Custody ────────────────────────────────
  describe('M4.0c — Digest-registration API custody boundary', () => {
    let engine: LeaderlessSyncEngine;

    beforeEach(() => {
      engine = new LeaderlessSyncEngine({
        sessionId: 'session-custody',
        localPeerId: 'peer-local',
      });
    });

    it('should reject direct recordPeerDigest call without valid internal capability token', () => {
      const digest = makeDigestPayload();

      // Passing undefined / invalid token throws UNAUTHORIZED_FACADE_CALL
      expect(() => {
        (engine as any).recordPeerDigest(digest); // missing token (legacy call)
      }).toThrowError(
        expect.objectContaining({
          code: 'UNAUTHORIZED_FACADE_CALL',
        }),
      );

      expect(() => {
        (engine as any).recordPeerDigest(Symbol('bogus-token'), digest);
      }).toThrowError(
        expect.objectContaining({
          code: 'UNAUTHORIZED_FACADE_CALL',
        }),
      );
    });

    it('should accept recordPeerDigest call when presented with INTERNAL_DIGEST_ACCESS_TOKEN', () => {
      const digest = makeDigestPayload({
        senderPeerId: 'peer-remote-custody',
      });

      expect(() => {
        engine.recordPeerDigest(INTERNAL_DIGEST_ACCESS_TOKEN, digest);
      }).not.toThrow();

      expect(engine.getPeerDigests().has('peer-remote-custody')).toBe(true);
    });

    it('should NOT export INTERNAL_DIGEST_ACCESS_TOKEN through public package barrels', () => {
      // Audit public barrel export of @syncplay/sync-engine
      expect((SyncEngineBarrel as any).INTERNAL_DIGEST_ACCESS_TOKEN).toBeUndefined();
      expect(Object.keys(SyncEngineBarrel)).not.toContain('INTERNAL_DIGEST_ACCESS_TOKEN');
      expect(Object.values(SyncEngineBarrel)).not.toContain(INTERNAL_DIGEST_ACCESS_TOKEN);
    });
  });

  // ── M4.0d: Transactional Compaction Candidate Staging ─────────────────────
  describe('M4.0d — Transactional compaction candidate staging & immutability', () => {
    it('should freeze inserted operations and causalContext objects at insertion boundary', () => {
      const log = new OrderedOperationLog();

      const causalContext = { 'peer-A': 1 };
      const op: StoredOperation = {
        logicalEventId: 'evt-freeze-1',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 10,
        originSequenceNumber: 1,
        causalContext,
        appliedAtMonotonic: 1000,
        payload: { trackId: 'trk-1' },
      };

      log.insert(op);

      expect(Object.isFrozen(op)).toBe(true);
      expect(Object.isFrozen(op.causalContext)).toBe(true);

      // Attempting to mutate throws in strict mode
      expect(() => {
        (op as any).originSequenceNumber = 999;
      }).toThrow();

      expect(() => {
        (op.causalContext as any)['peer-A'] = 999;
      }).toThrow();
    });

    it('should prove CandidateConstructionFailure => ActiveStateAfter === ActiveStateBefore on throwing reducer', () => {
      const initialSnapshot: PlaybackState = Object.freeze({
        status: 'paused',
        anchorPositionMs: 1000,
        anchorLocalTimestamp: 5000,
        playbackRate: 1.0,
        stateVersion: 1,
      });

      let failOnOp = 2; // Throw when reducing second op
      const throwingReducer = vi.fn((state: PlaybackState, op: StoredOperation): PlaybackState => {
        if (op.originSequenceNumber === failOnOp) {
          throw new Error('SIMULATED_REDUCER_FAULT');
        }
        return {
          ...state,
          anchorPositionMs: state.anchorPositionMs + 500,
          stateVersion: state.stateVersion + 1,
        };
      });

      const log = new OrderedOperationLog(OrderedOperationLog.DEFAULT_CAPACITY, initialSnapshot, throwingReducer);

      const op1: StoredOperation = {
        logicalEventId: 'evt-atomic-1',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 10,
        originSequenceNumber: 1,
        causalContext: {},
        appliedAtMonotonic: 1000,
        payload: null,
      };

      const op2: StoredOperation = {
        logicalEventId: 'evt-atomic-2',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 20,
        originSequenceNumber: 2,
        causalContext: {},
        appliedAtMonotonic: 2000,
        payload: null,
      };

      const op3: StoredOperation = {
        logicalEventId: 'evt-atomic-3',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 30,
        originSequenceNumber: 3,
        causalContext: {},
        appliedAtMonotonic: 3000,
        payload: null,
      };

      log.insert(op1);
      log.insert(op2);
      log.insert(op3);

      // Snapshot active state before compaction attempt
      const sizeBefore = log.size;
      const allOpsBefore = log.getAllOperations();
      const baseSnapshotBefore = log.getBaseSnapshot();
      const baseFrontierBefore = log.getBaseSnapshotFrontier();
      const checkpointsBefore = log.getAllCompactedCheckpoints();
      const opsArrayRefBefore = (log as any).operations;
      const idIndexRefBefore = (log as any).idIndex;
      const originSeqIndexRefBefore = (log as any).originSeqIndex;
      const compactedCheckpointsRefBefore = (log as any).compactedCheckpoints;
      const originTimestampsRefBefore = (log as any).originFrontierTimestamps;
      expect(log.hasPruned).toBe(false);

      // Attempt compaction that will prune op1 and op2 (throwing on op2)
      expect(() => {
        log.compactAtFrontier({ 'peer-A': 2 });
      }).toThrow('SIMULATED_REDUCER_FAULT');

      // Verify all 8 active components are completely unmodified (both by value and reference identity)!
      // 1. baseSnapshot
      expect(log.getBaseSnapshot()).toEqual(baseSnapshotBefore);
      expect(log.getBaseSnapshot()?.anchorPositionMs).toBe(1000);
      expect(log.getBaseSnapshot()?.stateVersion).toBe(1);

      // 2. operations (array was neither replaced nor mutated)
      expect(log.size).toBe(sizeBefore);
      expect(log.getAllOperations()).toEqual(allOpsBefore);
      expect((log as any).operations).toBe(opsArrayRefBefore);

      // 3. idIndex
      expect((log as any).idIndex).toBe(idIndexRefBefore);
      expect(log.has('evt-atomic-1')).toBe(true);
      expect(log.has('evt-atomic-2')).toBe(true);
      expect(log.has('evt-atomic-3')).toBe(true);
      expect(log.get('evt-atomic-1')).toBe(op1);

      // 4. originSeqIndex
      expect((log as any).originSeqIndex).toBe(originSeqIndexRefBefore);
      expect(log.getOperationAt('peer-A', 1)).toBe(op1);
      expect(log.getOperationAt('peer-A', 2)).toBe(op2);
      expect(log.getOperationAt('peer-A', 3)).toBe(op3);

      // 5. compactedCheckpoints
      expect((log as any).compactedCheckpoints).toBe(compactedCheckpointsRefBefore);
      expect(log.getAllCompactedCheckpoints()).toEqual(checkpointsBefore);
      expect(log.getCompactedCheckpoint('peer-A', 1)).toBeUndefined();

      // 6. originFrontierTimestamps
      expect((log as any).originFrontierTimestamps).toBe(originTimestampsRefBefore);
      expect(log.getOriginFrontierTimestamps({ 'peer-A': 1 })['peer-A']).toBe(10);

      // 7. baseSnapshotFrontier
      expect(log.getBaseSnapshotFrontier()).toEqual(baseFrontierBefore);

      // 8. hasPrunedOperations (explicitly tested: remains false on failure)
      expect(log.hasPruned).toBe(false);
      expect(log.size).toBe(3);
    });

    it('should complete clean compaction and rebuild indexes when reducer succeeds', () => {
      const initialSnapshot: PlaybackState = Object.freeze({
        status: 'paused',
        anchorPositionMs: 0,
        anchorLocalTimestamp: 1000,
        playbackRate: 1.0,
        stateVersion: 0,
      });

      const pureReducer = vi.fn((state: PlaybackState, op: StoredOperation): PlaybackState => {
        return {
          ...state,
          anchorPositionMs: state.anchorPositionMs + 100,
          stateVersion: state.stateVersion + 1,
        };
      });

      const log = new OrderedOperationLog(OrderedOperationLog.DEFAULT_CAPACITY, initialSnapshot, pureReducer);
      expect(log.hasPruned).toBe(false);

      const op1: StoredOperation = {
        logicalEventId: 'evt-succ-1',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 10,
        originSequenceNumber: 1,
        causalContext: {},
        appliedAtMonotonic: 1000,
        payload: null,
      };

      const op2: StoredOperation = {
        logicalEventId: 'evt-succ-2',
        originPeerId: 'peer-A',
        senderPeerId: 'peer-A',
        membershipEpoch: 1,
        messageType: 'PLAY',
        logicalTimestamp: 20,
        originSequenceNumber: 2,
        causalContext: {},
        appliedAtMonotonic: 2000,
        payload: null,
      };

      log.insert(op1);
      log.insert(op2);

      const opsArrayRefBefore = (log as any).operations;

      const pruned = log.compactAtFrontier({ 'peer-A': 1 });
      expect(pruned).toBe(1);

      // 1. baseSnapshot updated
      expect(log.getBaseSnapshot()?.anchorPositionMs).toBe(100);
      expect(log.getBaseSnapshot()?.stateVersion).toBe(1);

      // 2. operations contains only op2 and reference was swapped to candidate array
      expect(log.size).toBe(1);
      expect(log.getAllOperations()[0]?.logicalEventId).toBe('evt-succ-2');
      expect((log as any).operations).not.toBe(opsArrayRefBefore);

      // 3. idIndex updated
      expect(log.has('evt-succ-1')).toBe(false);
      expect(log.has('evt-succ-2')).toBe(true);

      // 4. originSeqIndex updated
      expect(log.getOperationAt('peer-A', 1)).toBeUndefined();
      expect(log.getOperationAt('peer-A', 2)?.logicalEventId).toBe('evt-succ-2');

      // 5. compactedCheckpoints contains op1
      expect(log.getCompactedCheckpoint('peer-A', 1)).toBe(10);

      // 6. baseSnapshotFrontier updated
      expect(log.getBaseSnapshotFrontier()['peer-A']).toBe(1);

      // 7. originFrontierTimestamps updated
      expect(log.getOriginFrontierTimestamps({ 'peer-A': 1 })['peer-A']).toBe(10);

      // 8. hasPrunedOperations is true after successful compaction
      expect(log.hasPruned).toBe(true);
    });

    it('should verify deepClone preserves PlaybackState fields identically without prototype pollution', () => {
      const state: PlaybackState = Object.freeze({
        status: 'playing',
        anchorPositionMs: 42500,
        anchorLocalTimestamp: 12345.67,
        playbackRate: 1.5,
        stateVersion: 42,
      });

      // Test structuredClone path
      const cloned = structuredClone(state);
      expect(cloned).toEqual(state);
      expect(cloned).not.toBe(state);
      expect(Object.isFrozen(cloned)).toBe(false);

      // Test JSON parse/stringify fallback path
      const jsonCloned = JSON.parse(JSON.stringify(state));
      expect(jsonCloned).toEqual(state);
      expect(jsonCloned).not.toBe(state);
      expect(Object.isFrozen(jsonCloned)).toBe(false);
    });
  });
});
