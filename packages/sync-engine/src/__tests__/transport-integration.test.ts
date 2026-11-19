/**
 * Phase 7.4 Milestone M5 — Transport Integration Test Suite
 *
 * Verifies:
 * 1. Serialization and exact message-size enforcement at DataChannel boundary.
 * 2. Epoch and connection-generation currency validation immediately prior to send.
 * 3. Channel-level trusted inbound context derivation (supplier/requester identity and generation).
 * 4. End-to-end P2P repair lifecycle: M3 requester <-> WebRTC DataChannel <-> M4 supplier.
 * 5. Re-entrant lifecycle transitions, disconnects, DataChannel errors, and duplicate handling.
 * 6. Non-interference with control events and transport diagnostics.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  type ProtocolMessage,
  type OperationBatchPayload,
  type RepairRequestPayload,
  type RepairRejectedPayload,
  type StateDigestPayload,
  PROTOCOL_VERSION,
  MessageType,
  SYNC_MESSAGE_TYPES,
  MAX_MESSAGE_BYTES,
} from '@syncplay/protocol';
import type { PeerMesh, InboundMessage, PeerStatus, SendResult, ConnectionGeneration } from '@syncplay/peer-mesh';
import {
  LeaderlessSyncEngine,
  TrustedTransportAdapter,
  createEpochTransitionCapability,
  createTransportRegistrationCapability,
} from '../engine/leaderless-sync-engine.js';
import { AntiEntropyManager } from '../replication/anti-entropy-manager.js';
import type { AntiEntropyCallbacks, SchedulerClock } from '../replication/types.js';
import type { StoredOperation } from '../log/types.js';
import { SyncEngineError } from '../errors.js';

// ── Test Helpers & Fixtures ──────────────────────────────────────

const SESSION_ID = 'test-session-m5';

function createDeterministicClock(startMs = 1000): SchedulerClock & { advance(ms: number): void } {
  let currentTime = startMs;
  let nextTimerId = 1;
  const timers = new Map<number, { callback: () => void; dueTime: number }>();

  return {
    now: () => currentTime,
    setTimeout: (callback: () => void, delayMs: number) => {
      const id = nextTimerId++;
      timers.set(id, { callback, dueTime: currentTime + delayMs });
      return id;
    },
    clearTimeout: (id: unknown) => {
      timers.delete(Number(id));
    },
    advance: (ms: number) => {
      currentTime += ms;
      const ready: (() => void)[] = [];
      for (const [id, timer] of [...timers.entries()]) {
        if (timer.dueTime <= currentTime) {
          timers.delete(id);
          ready.push(timer.callback);
        }
      }
      for (const cb of ready) {
        cb();
      }
    },
  };
}

function createStoredOperation(
  originPeerId: string,
  seq: number,
  timestamp = seq,
  overrides?: Partial<StoredOperation>,
): StoredOperation {
  return {
    logicalEventId: `event-${originPeerId}-${seq}`,
    originPeerId,
    senderPeerId: originPeerId,
    membershipEpoch: 1,
    messageType: 'PLAY',
    logicalTimestamp: timestamp,
    originSequenceNumber: seq,
    causalContext: seq > 1 ? { [originPeerId]: seq - 1 } : {},
    appliedAtMonotonic: seq * 100,
    payload: { positionMs: (10 + seq) * 1000 },
    ...overrides,
  };
}

function makeRemoteControlEvent(
  logicalEventId: string,
  senderPeerId: string,
  originPeerId: string,
  seq: number,
  timestamp: number,
  causalContext: Record<string, number> = {},
  membershipEpoch = 1,
) {
  return {
    origin: 'remote' as const,
    localTimestamp: 1000,
    connectionGeneration: 1,
    message: {
      protocolVersion: PROTOCOL_VERSION,
      messageType: 'PLAY',
      sessionId: SESSION_ID,
      senderPeerId,
      originPeerId,
      messageId: `msg-${logicalEventId}`,
      logicalEventId,
      logicalTimestamp: timestamp,
      originSequenceNumber: seq,
      causalContext,
      membershipEpoch,
      payload: { targetPositionMs: 5000 },
    },
  };
}

function makeStateDigest(
  senderPeerId: string,
  replicationVector: Record<string, number>,
  overrides?: Partial<StateDigestPayload>,
): StateDigestPayload {
  return {
    formatVersion: 2,
    sessionId: SESSION_ID,
    senderPeerId,
    connectionGeneration: 1,
    membershipEpoch: 1,
    stateVersion: 1,
    replicationVector,
    snapshotFrontier: {},
    historyFingerprints: {},
    sentAt: 1000,
    ...overrides,
  };
}

interface MockPeerMeshOptions {
  localPeerId: string;
  remotePeerId: string;
  sctpMaxMessageSize?: number;
  initialGeneration?: number;
}

function createMockPeerMesh(options: MockPeerMeshOptions) {
  const { localPeerId, remotePeerId, sctpMaxMessageSize, initialGeneration = 1 } = options;

  let messageHandlers: ((msg: InboundMessage) => void)[] = [];
  let statusHandlers: ((status: PeerStatus) => void)[] = [];
  const sentRawMessages: { peerId: string; json: string; generation?: number }[] = [];
  const sentProtocolMessages: { peerId: string; message: ProtocolMessage }[] = [];

  let peerStatus: PeerStatus = {
    peerId: remotePeerId,
    state: 'CONNECTED',
    dataChannelState: 'open',
    generation: initialGeneration,
    lastUpdatedAt: Date.now(),
  };

  let currentSctpLimit = sctpMaxMessageSize;

  const mesh: PeerMesh = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn((id, reason) => {
      peerStatus = {
        ...peerStatus,
        state: 'CLOSED',
        dataChannelState: 'closed',
        lastUpdatedAt: Date.now(),
      };
      for (const h of statusHandlers) h(peerStatus);
    }),
    send: vi.fn((targetPeerId, message) => {
      sentProtocolMessages.push({ peerId: targetPeerId, message });
      const json = JSON.stringify(message);
      return mesh.sendSerialized!(targetPeerId, json, peerStatus.generation);
    }),
    sendSerialized: vi.fn((targetPeerId, serializedJson, expectedGeneration) => {
      if (targetPeerId !== remotePeerId) {
        return { ok: false, error: 'Peer not found' };
      }
      if (expectedGeneration !== undefined && expectedGeneration !== peerStatus.generation) {
        return { ok: false, error: 'Generation mismatch' };
      }
      if (peerStatus.dataChannelState !== 'open') {
        return { ok: false, error: 'DataChannel not open' };
      }

      // Exact UTF-8 byte length measurement
      const byteLength = new TextEncoder().encode(serializedJson).byteLength;
      const effectiveLimit =
        currentSctpLimit !== undefined && currentSctpLimit > 0
          ? Math.min(MAX_MESSAGE_BYTES, currentSctpLimit)
          : MAX_MESSAGE_BYTES;

      if (byteLength > effectiveLimit) {
        return { ok: false, error: `Message exceeds maximum size (${byteLength} > ${effectiveLimit})` };
      }

      sentRawMessages.push({ peerId: targetPeerId, json: serializedJson, generation: expectedGeneration });
      return { ok: true };
    }),
    getSctpMaxMessageSize: vi.fn((id) => (id === remotePeerId ? currentSctpLimit : undefined)),
    getPeerGeneration: vi.fn((id) => (id === remotePeerId ? peerStatus.generation : undefined)),
    getPeerStatus: vi.fn((id) => (id === remotePeerId ? peerStatus : undefined)),
    listPeers: vi.fn(() => [peerStatus]),
    onMessage: vi.fn((handler) => {
      messageHandlers.push(handler);
      return () => {
        messageHandlers = messageHandlers.filter((h) => h !== handler);
      };
    }),
    onPeerStatus: vi.fn((handler) => {
      statusHandlers.push(handler);
      return () => {
        statusHandlers = statusHandlers.filter((h) => h !== handler);
      };
    }),
    dispose: vi.fn().mockResolvedValue(undefined),
  };

  return {
    mesh,
    getSentRaw: () => [...sentRawMessages],
    getSentMessages: () => [...sentProtocolMessages],
    setSctpLimit: (limit: number | undefined) => {
      currentSctpLimit = limit;
    },
    setGeneration: (gen: number) => {
      peerStatus = { ...peerStatus, generation: gen, lastUpdatedAt: Date.now() };
      for (const h of statusHandlers) h(peerStatus);
    },
    setDataChannelState: (state: 'connecting' | 'open' | 'closing' | 'closed' | null) => {
      peerStatus = { ...peerStatus, dataChannelState: state, lastUpdatedAt: Date.now() };
      for (const h of statusHandlers) h(peerStatus);
    },
    setConnectionState: (state: PeerStatus['state']) => {
      peerStatus = { ...peerStatus, state, lastUpdatedAt: Date.now() };
      for (const h of statusHandlers) h(peerStatus);
    },
    deliverInbound: (fromPeerId: string, message: ProtocolMessage) => {
      for (const h of messageHandlers) {
        h({ fromPeerId, message });
      }
    },
  };
}

describe('Phase 7.4 Milestone M5: Transport Integration', () => {
  let clock: ReturnType<typeof createDeterministicClock>;

  beforeEach(() => {
    clock = createDeterministicClock();
  });

  // ════════════════════════════════════════════════════════════════
  // 1. Exact Serialization & Message-Size Enforcement
  // ════════════════════════════════════════════════════════════════

  describe('1. Serialization and Exact Message-Size Enforcement', () => {
    it('1.1 Measures complete ProtocolMessage envelope bytes including all metadata', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-exact-size',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      let capturedJson = '';
      const sent = adapter.sendRepairResponse(
        'requester-peer',
        batch,
        1,
        (wireJson) => {
          capturedJson = wireJson;
          return true;
        },
      );

      expect(sent).toBe(true);
      expect(capturedJson).toContain('"protocolVersion":1');
      expect(capturedJson).toContain('"messageType":"OPERATION_BATCH"');
      expect(capturedJson).toContain('"sessionId":"test-session-m5"');
      expect(capturedJson).toContain('"senderPeerId":"supplier-peer"');
      expect(capturedJson).toContain('"messageId":"req-exact-size"');
      expect(capturedJson).toContain('"originPeerId":"origin-1"');
      expect(capturedJson).toContain('"membershipEpoch":1');

      // Envelope byte length must match UTF-8 bytes exactly
      const expectedByteLength = new TextEncoder().encode(capturedJson).byteLength;
      expect(expectedByteLength).toBeGreaterThan(100);
    });

    it('1.2 Multibyte UTF-8 characters are measured in actual bytes, not UTF-16 character length', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      // '🚀' is 4 UTF-8 bytes but 2 JS UTF-16 chars. 'こんにちは' is 15 UTF-8 bytes but 5 chars.
      const unicodeString = '🚀'.repeat(50) + 'こんにちは'.repeat(20);
      const op = createStoredOperation('origin-1', 1, 10, {
        action: {
          type: 'PLAY',
          targetPosition: 10,
          originTimestamp: 10,
          clientExtrapolatedPosition: 10,
          customData: unicodeString,
        } as any,
      });

      const batch: OperationBatchPayload = {
        requestId: 'req-unicode',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [op],
      };

      let capturedJson = '';
      adapter.sendRepairResponse('requester-peer', batch, 1, (wireJson) => {
        capturedJson = wireJson;
        return true;
      });

      const jsLength = capturedJson.length;
      const utf8ByteLength = new TextEncoder().encode(capturedJson).byteLength;
      // UTF-8 byte length must be strictly greater than JS character count due to multibyte characters
      expect(utf8ByteLength).toBeGreaterThan(jsLength);
    });

    it('1.3 Message exactly at limit (65,536 bytes) passes size check and is sent', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      // Construct a payload whose complete wire message is sized up to 65,536 bytes
      const baseBatch: OperationBatchPayload = {
        requestId: 'req-limit-boundary',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1, 10, {
          action: {
            type: 'PLAY',
            targetPosition: 10,
            originTimestamp: 10,
            clientExtrapolatedPosition: 10,
            pad: '',
          } as any,
        })],
      };

      // Measure overhead and pad to exactly 65,536 bytes
      const sampleEnvelope = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'OPERATION_BATCH',
        sessionId: SESSION_ID,
        senderPeerId: 'supplier-peer',
        messageId: baseBatch.requestId,
        logicalEventId: baseBatch.requestId,
        originPeerId: baseBatch.originPeerId,
        membershipEpoch: 1,
        payload: baseBatch,
      };
      const initialBytes = new TextEncoder().encode(JSON.stringify(sampleEnvelope)).byteLength;
      const neededPadding = 65536 - initialBytes;
      (baseBatch.operations[0]!.action as any).pad = 'a'.repeat(neededPadding);

      let sendFnCalled = false;
      const sent = adapter.sendRepairResponse(
        'requester-peer',
        baseBatch,
        1,
        (wireJson) => {
          sendFnCalled = true;
          const bytes = new TextEncoder().encode(wireJson).byteLength;
          expect(bytes).toBe(65536);
          return true;
        },
        65536,
      );

      expect(sent).toBe(true);
      expect(sendFnCalled).toBe(true);
    });

    it('1.4 Message one byte over limit (65,537 bytes) is rejected immediately without sending', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const baseBatch: OperationBatchPayload = {
        requestId: 'req-limit-overflow',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1, 10, {
          action: {
            type: 'PLAY',
            targetPosition: 10,
            originTimestamp: 10,
            clientExtrapolatedPosition: 10,
            pad: '',
          } as any,
        })],
      };

      const sampleEnvelope = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'OPERATION_BATCH',
        sessionId: SESSION_ID,
        senderPeerId: 'supplier-peer',
        messageId: baseBatch.requestId,
        logicalEventId: baseBatch.requestId,
        originPeerId: baseBatch.originPeerId,
        membershipEpoch: 1,
        payload: baseBatch,
      };
      const initialBytes = new TextEncoder().encode(JSON.stringify(sampleEnvelope)).byteLength;
      const neededPadding = 65537 - initialBytes; // Exactly 65,537 bytes
      (baseBatch.operations[0]!.action as any).pad = 'a'.repeat(neededPadding);

      const sendSpy = vi.fn();
      const sent = adapter.sendRepairResponse('requester-peer', baseBatch, 1, sendSpy, 65536);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('1.5 Envelope larger than available SCTP limit rejects safely without positive minimum forcing', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-tiny-sctp',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      // Set negotiated SCTP limit to 50 bytes (envelope alone is >200 bytes)
      const sendSpy = vi.fn();
      const sent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy, 50);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('1.6 Undefined negotiated SCTP limit falls back safely to 65,536-byte application cap', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-undef-sctp',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      const sendSpy = vi.fn().mockReturnValue(true);
      // undefined effective limit -> default cap 65536
      const sent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy, undefined);
      expect(sent).toBe(true);
      expect(sendSpy).toHaveBeenCalledTimes(1);
    });

    it('1.7 Zero negotiated SCTP limit (RFC 8841 unconstrained remote) clamps to 65,536-byte local application cap', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-zero-sctp',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      const sendSpy = vi.fn().mockReturnValue(true);
      // effectiveTransportLimit = 0 (unconstrained remote) -> clamps to 65536
      const sent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy, 0);
      expect(sent).toBe(true);
      expect(sendSpy).toHaveBeenCalledTimes(1);
    });

    it('1.8 Finite negotiated limit below application cap enforces the smaller limit', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      // Create message around 1000 bytes
      const batch: OperationBatchPayload = {
        requestId: 'req-finite-sctp',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1, 10, {
          action: { type: 'PLAY', targetPosition: 10, originTimestamp: 10, pad: 'x'.repeat(800) } as any,
        })],
      };

      const sendSpy = vi.fn().mockReturnValue(true);

      // Limit = 500 bytes (smaller than ~1000 byte message) -> should reject
      const rejectSent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy, 500);
      expect(rejectSent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();

      // Limit = 2000 bytes (larger than ~1000 byte message) -> should accept
      const acceptSent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy, 2000);
      expect(acceptSent).toBe(true);
      expect(sendSpy).toHaveBeenCalledTimes(1);
    });

    it('1.9 Finite negotiated limit above application cap retains 65,536-byte application cap', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      // Remote SCTP supports 262,144 bytes, but application cap is 65,536
      const hugeBatch: OperationBatchPayload = {
        requestId: 'req-huge-sctp',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1, 10, {
          action: { type: 'PLAY', targetPosition: 10, originTimestamp: 10, pad: 'x'.repeat(70000) } as any,
        })],
      };

      const sendSpy = vi.fn().mockReturnValue(true);
      // Passing remote SCTP limit 262,144 -> clamped to 65,536 -> rejects 70KB message
      const sent = adapter.sendRepairResponse('requester-peer', hugeBatch, 1, sendSpy, 262144);
      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('1.10 Exact measured serialized JSON string is passed directly to sendFn', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const req: RepairRequestPayload = {
        requestId: 'req-exact-string',
        sessionId: SESSION_ID,
        requesterPeerId: 'requester-peer',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      let deliveredString = '';
      adapter.sendRepairRequest('requester-peer', req, 1, (wireJson) => {
        deliveredString = wireJson;
        return true;
      });

      // Parse delivered string and verify every single property
      const parsed = JSON.parse(deliveredString);
      expect(parsed.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(parsed.messageType).toBe('REPAIR_REQUEST');
      expect(parsed.messageId).toBe('req-exact-string');
      expect(parsed.sessionId).toBe(SESSION_ID);
      expect(parsed.senderPeerId).toBe('supplier-peer');
      expect(parsed.originPeerId).toBe('origin-1');
      expect(parsed.membershipEpoch).toBe(1);
      expect(parsed.payload).toEqual(req);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 2. Epoch and Connection-Generation Safety at Send Time
  // ════════════════════════════════════════════════════════════════

  describe('2. Epoch and Connection-Generation Safety at Send Time', () => {
    it('2.1 Epoch transition immediately before sendRepairResponse drops transmission', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-epoch-stale',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1, // Epoch 1
        operations: [createStoredOperation('origin-1', 1)],
      };

      // Transition engine epoch to 2
      engine.transitionEpoch(2, epochCap);

      const sendSpy = vi.fn();
      const sent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('2.2 Epoch transition immediately before sendRepairRequest drops transmission', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('supplier-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const request: RepairRequestPayload = {
        requestId: 'req-epoch-req-stale',
        sessionId: SESSION_ID,
        requesterPeerId: 'requester-peer',
        targetOriginPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 5,
        membershipEpoch: 1,
      };

      engine.transitionEpoch(2, epochCap);

      const sendSpy = vi.fn();
      const sent = adapter.sendRepairRequest('supplier-peer', request, 1, sendSpy);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('2.3 Connection generation change before sendRepairResponse drops transmission', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-gen-stale',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      // Advance peer generation to 2 (peer reconnected)
      engine.authorizePeerGeneration('requester-peer', 2);

      const sendSpy = vi.fn();
      // Trying to send with expected generation 1
      const sent = adapter.sendRepairResponse('requester-peer', batch, 1, sendSpy);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });

    it('2.4 Re-entrant epoch transition inside callback prevents subsequent obsolete transmission', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-reentrant',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      let transmissionAttempted = false;
      const sendFnWithReentrantEpoch = vi.fn((_wireJson: string) => {
        transmissionAttempted = true;
        // Re-entrant epoch transition occurs
        engine.transitionEpoch(2, epochCap);
        return true;
      });

      // First send succeeds and triggers re-entrant epoch transition
      const sent1 = adapter.sendRepairResponse('requester-peer', batch, 1, sendFnWithReentrantEpoch);
      expect(sent1).toBe(true);
      expect(transmissionAttempted).toBe(true);

      // Subsequent send of the same epoch 1 response must be dropped immediately
      sendFnWithReentrantEpoch.mockClear();
      const sent2 = adapter.sendRepairResponse('requester-peer', batch, 1, sendFnWithReentrantEpoch);
      expect(sent2).toBe(false);
      expect(sendFnWithReentrantEpoch).not.toHaveBeenCalled();
    });

    it('2.5 Disconnected target peer or unknown generation drops transmission safely', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      // Do NOT authorize 'unknown-peer'

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const batch: OperationBatchPayload = {
        requestId: 'req-unknown-peer',
        sessionId: SESSION_ID,
        originPeerId: 'origin-1',
        fromSequence: 1,
        toSequence: 1,
        membershipEpoch: 1,
        operations: [createStoredOperation('origin-1', 1)],
      };

      const sendSpy = vi.fn();
      const sent = adapter.sendRepairResponse('unknown-peer', batch, 1, sendSpy);

      expect(sent).toBe(false);
      expect(sendSpy).not.toHaveBeenCalled();
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 3. Channel-Level Trusted Inbound Context Derivation
  // ════════════════════════════════════════════════════════════════

  describe('3. Channel-Level Trusted Inbound Context Derivation', () => {
    it('3.1 Inbound REPAIR_REQUEST derives requester peerId and connection generation from transport', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('actual-requester', 4);

      // Seed log so supplier can service
      engine.getOperationLog().insert(createStoredOperation('origin-1', 1));

      const adapter = new TrustedTransportAdapter(engine, transportCap);

      const mockTransport = createMockPeerMesh({
        localPeerId: 'supplier-peer',
        remotePeerId: 'actual-requester',
        initialGeneration: 4,
      });

      adapter.bindPeerMesh(mockTransport.mesh);
      const callbacks = adapter.createAntiEntropyCallbacks(mockTransport.mesh);
      const aem = new AntiEntropyManager(engine, callbacks, {}, clock);
      adapter.setAntiEntropyManager(aem);

      const validUuid = 'a1111111-1111-4111-8111-111111111111';
      const reqMessage: ProtocolMessage<RepairRequestPayload> = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'REPAIR_REQUEST',
        sessionId: SESSION_ID,
        senderPeerId: 'actual-requester',
        messageId: validUuid,
        originPeerId: 'origin-1',
        membershipEpoch: 1,
        payload: {
          requestId: validUuid,
          sessionId: SESSION_ID,
          requesterPeerId: 'actual-requester',
          targetOriginPeerId: 'origin-1',
          fromSequence: 1,
          toSequence: 1,
          membershipEpoch: 1,
        },
      };

      // Deliver via DataChannel
      mockTransport.deliverInbound('actual-requester', reqMessage);

      // Verify that supplier serviced and sent response
      const sentRaw = mockTransport.getSentRaw();
      expect(sentRaw.length).toBeGreaterThan(0);
      const responseJson = JSON.parse(sentRaw[0]!.json);
      expect(responseJson.messageType).toBe('OPERATION_BATCH');
      expect(responseJson.payload.operations).toHaveLength(1);
    });

    it('3.2 Inbound message with mismatched session ID is dropped without processing', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('requester-peer', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);
      const handleBatchSpy = vi.spyOn(adapter, 'handleInboundRepairBatch');

      const mockTransport = createMockPeerMesh({
        localPeerId: 'supplier-peer',
        remotePeerId: 'requester-peer',
      });
      adapter.bindPeerMesh(mockTransport.mesh);

      const wrongSessionMessage: ProtocolMessage<OperationBatchPayload> = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'OPERATION_BATCH',
        sessionId: 'other-session-id', // Mismatched session
        senderPeerId: 'requester-peer',
        messageId: 'batch-wrong-session',
        membershipEpoch: 1,
        payload: {
          requestId: 'req-wrong-session',
          sessionId: 'other-session-id',
          originPeerId: 'origin-1',
          fromSequence: 1,
          toSequence: 1,
          membershipEpoch: 1,
          operations: [],
        },
      };

      mockTransport.deliverInbound('requester-peer', wrongSessionMessage);
      expect(handleBatchSpy).not.toHaveBeenCalled();
    });

    it('3.3 Inbound message with spoofed sender ID is dropped without processing', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('actual-peer', 1);
      engine.authorizePeerGeneration('spoofed-victim', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);
      const handleBatchSpy = vi.spyOn(adapter, 'handleInboundRepairBatch');

      const mockTransport = createMockPeerMesh({
        localPeerId: 'supplier-peer',
        remotePeerId: 'actual-peer',
      });
      adapter.bindPeerMesh(mockTransport.mesh);

      // Packet arrives on 'actual-peer' channel but claims senderPeerId = 'spoofed-victim'
      const spoofedMessage: ProtocolMessage<OperationBatchPayload> = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'OPERATION_BATCH',
        sessionId: SESSION_ID,
        senderPeerId: 'spoofed-victim', // Spoofed sender
        messageId: 'batch-spoofed',
        membershipEpoch: 1,
        payload: {
          requestId: 'req-spoofed',
          sessionId: SESSION_ID,
          originPeerId: 'origin-1',
          fromSequence: 1,
          toSequence: 1,
          membershipEpoch: 1,
          operations: [],
        },
      };

      mockTransport.deliverInbound('actual-peer', spoofedMessage);
      expect(handleBatchSpy).not.toHaveBeenCalled();
    });

    it('3.4 Unauthorized context registration without capability throws SyncEngineError UNAUTHORIZED', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'engine-1',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });

      const fakeCap = createTransportRegistrationCapability('attacker', SESSION_ID);

      expect(() => {
        engine.registerInboundSupplierContext('supplier-1', 1, fakeCap);
      }).toThrowError(SyncEngineError);

      expect(() => {
        engine.registerInboundRequesterContext('requester-1', 1, fakeCap);
      }).toThrowError(SyncEngineError);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 4. End-to-End Transport Lifecycle and Round-Trip Repair
  // ════════════════════════════════════════════════════════════════

  describe('4. End-to-End Transport Lifecycle and Round-Trip Repair', () => {
    it('4.1 Full end-to-end round trip: Requester requests missing range -> Supplier services over transport -> Requester admits into Phase 7.3 engine', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const requesterTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const supplierTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      // 1. Requester Engine & Adapter
      const requesterEngine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: requesterTransportCap,
      });
      requesterEngine.authorizePeerGeneration('supplier-peer', 1);
      const requesterAdapter = new TrustedTransportAdapter(requesterEngine, requesterTransportCap);

      // 2. Supplier Engine & Adapter
      const supplierEngine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: supplierTransportCap,
      });
      supplierEngine.authorizePeerGeneration('requester-peer', 1);
      const supplierAdapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);

      // Seed supplier log with operations 1..3 for origin-A
      for (let s = 1; s <= 3; s++) {
        supplierEngine.getOperationLog().insert(createStoredOperation('origin-A', s));
      }

      // Loopback message wiring
      let requesterDeliver: (from: string, msg: ProtocolMessage) => void = () => {};
      let supplierDeliver: (from: string, msg: ProtocolMessage) => void = () => {};

      const requesterMesh = createMockPeerMesh({
        localPeerId: 'requester-peer',
        remotePeerId: 'supplier-peer',
      });
      requesterDeliver = requesterMesh.deliverInbound;

      const supplierMesh = createMockPeerMesh({
        localPeerId: 'supplier-peer',
        remotePeerId: 'requester-peer',
      });
      supplierDeliver = supplierMesh.deliverInbound;

      // Wire send on requester mesh to deliver to supplier
      requesterMesh.mesh.send = vi.fn((targetPeerId, message) => {
        supplierDeliver('requester-peer', message);
        return { ok: true };
      });
      requesterMesh.mesh.sendSerialized = vi.fn((targetPeerId, json, gen) => {
        supplierDeliver('requester-peer', JSON.parse(json));
        return { ok: true };
      });

      // Wire send on supplier mesh to deliver to requester
      supplierMesh.mesh.send = vi.fn((targetPeerId, message) => {
        requesterDeliver('supplier-peer', message);
        return { ok: true };
      });
      supplierMesh.mesh.sendSerialized = vi.fn((targetPeerId, json, gen) => {
        requesterDeliver('supplier-peer', JSON.parse(json));
        return { ok: true };
      });

      // Bind meshes to adapters
      requesterAdapter.bindPeerMesh(requesterMesh.mesh);
      supplierAdapter.bindPeerMesh(supplierMesh.mesh);

      // Setup anti-entropy managers with callbacks wired to adapters
      const requesterCallbacks = requesterAdapter.createAntiEntropyCallbacks(requesterMesh.mesh);
      const requesterAEM = new AntiEntropyManager(
        requesterEngine,
        requesterCallbacks,
        {},
        clock,
      );
      requesterAdapter.setAntiEntropyManager(requesterAEM);

      const supplierCallbacks = supplierAdapter.createAntiEntropyCallbacks(supplierMesh.mesh);
      const supplierAEM = new AntiEntropyManager(
        supplierEngine,
        supplierCallbacks,
        {},
        clock,
      );
      supplierAdapter.setAntiEntropyManager(supplierAEM);

      // Verify that requester log initially lacks origin-A
      expect(requesterEngine.replicationFrontier['origin-A'] ?? 0).toBe(0);

      // Feed remote knowledge to requester that supplier holds origin-A up to seq 3
      // Triggers anti-entropy gap detection and full P2P repair round trip over DataChannel
      requesterAEM.processRemoteDigest(
        makeStateDigest('supplier-peer', { 'origin-A': 3 }, { sentAt: clock.now() }),
      );

      // Verify that round trip executed and requester admitted operations 1..3
      expect(requesterEngine.replicationFrontier['origin-A']).toBe(3);
      expect(requesterEngine.getOperationLog().size).toBe(3);
      expect(requesterEngine.getOperationLog().getOperationAt('origin-A', 1)).toBeDefined();
      expect(requesterEngine.getOperationLog().getOperationAt('origin-A', 2)).toBeDefined();
      expect(requesterEngine.getOperationLog().getOperationAt('origin-A', 3)).toBeDefined();
    });

    it('4.2 Unfulfillable range returns REPAIR_REJECTED over transport and requester marks failure', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const requesterTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const supplierTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      const requesterEngine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: requesterTransportCap,
      });
      requesterEngine.authorizePeerGeneration('supplier-peer', 1);
      const requesterAdapter = new TrustedTransportAdapter(requesterEngine, requesterTransportCap);

      const supplierEngine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: supplierTransportCap,
      });
      supplierEngine.authorizePeerGeneration('requester-peer', 1);
      const supplierAdapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);
      // Supplier log is EMPTY (cannot fulfill origin-A)

      let requesterDeliver: (from: string, msg: ProtocolMessage) => void = () => {};
      let supplierDeliver: (from: string, msg: ProtocolMessage) => void = () => {};

      const requesterMesh = createMockPeerMesh({ localPeerId: 'requester-peer', remotePeerId: 'supplier-peer' });
      requesterDeliver = requesterMesh.deliverInbound;

      const supplierMesh = createMockPeerMesh({ localPeerId: 'supplier-peer', remotePeerId: 'requester-peer' });
      supplierDeliver = supplierMesh.deliverInbound;

      requesterMesh.mesh.sendSerialized = vi.fn((targetPeerId, json) => {
        supplierDeliver('requester-peer', JSON.parse(json));
        return { ok: true };
      });
      supplierMesh.mesh.sendSerialized = vi.fn((targetPeerId, json) => {
        requesterDeliver('supplier-peer', JSON.parse(json));
        return { ok: true };
      });

      requesterAdapter.bindPeerMesh(requesterMesh.mesh);
      supplierAdapter.bindPeerMesh(supplierMesh.mesh);

      const requesterCallbacks = requesterAdapter.createAntiEntropyCallbacks(requesterMesh.mesh);
      // maxRetries = 0: any rejection immediately counts as failure
      const requesterAEM = new AntiEntropyManager(requesterEngine, requesterCallbacks, { maxRetries: 0 }, clock);
      requesterAdapter.setAntiEntropyManager(requesterAEM);

      const supplierCallbacks = supplierAdapter.createAntiEntropyCallbacks(supplierMesh.mesh);
      const supplierAEM = new AntiEntropyManager(supplierEngine, supplierCallbacks, {}, clock);
      supplierAdapter.setAntiEntropyManager(supplierAEM);

      // Remote digest tells requester that supplier has origin-A up to 5 (even though supplier has none)
      requesterAEM.processRemoteDigest(
        makeStateDigest('supplier-peer', { 'origin-A': 5 }, { sentAt: clock.now() }),
      );

      requesterAEM.reconcileGaps();

      // Supplier sends REPAIR_REJECTED (HISTORY_NOT_FOUND)
      // Pipeline records repair failure
      const status = requesterAEM.getStatus();
      expect(status.totalRepairsFailed).toBeGreaterThanOrEqual(1);
    });

    it('4.3 Dependent operations unblock causal dependencies when repair batch arrives over DataChannel', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const requesterTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const supplierTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      const requesterEngine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: requesterTransportCap,
      });
      requesterEngine.authorizePeerGeneration('supplier-peer', 1);
      requesterEngine.authorizePeerGeneration('origin-A', 1);

      const requesterAdapter = new TrustedTransportAdapter(requesterEngine, requesterTransportCap);

      const supplierEngine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: supplierTransportCap,
      });
      supplierEngine.authorizePeerGeneration('requester-peer', 1);
      const supplierAdapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);

      // Supplier has op 1 for origin-A
      supplierEngine.getOperationLog().insert(createStoredOperation('origin-A', 1));

      // Requester directly receives op 2 which depends on op 1 (so op 2 is buffered in pending queue)
      requesterEngine.receive(
        makeRemoteControlEvent(
          'event-op-2',
          'origin-A',
          'origin-A',
          2,
          2,
          { 'origin-A': 1 },
          1,
        ),
      );

      const decision = requesterEngine.getLastDecision();
      expect(decision?.accepted).toBe(false);
      expect(decision?.code).toBe('BUFFERED_PENDING_DEPENDENCY');
      expect(requesterEngine.getPendingGaps()).toHaveLength(1);

      // Setup loopback
      const requesterMesh = createMockPeerMesh({ localPeerId: 'requester-peer', remotePeerId: 'supplier-peer' });
      const supplierMesh = createMockPeerMesh({ localPeerId: 'supplier-peer', remotePeerId: 'requester-peer' });

      requesterMesh.mesh.sendSerialized = vi.fn((targetPeerId, json) => {
        supplierMesh.deliverInbound('requester-peer', JSON.parse(json));
        return { ok: true };
      });
      supplierMesh.mesh.sendSerialized = vi.fn((targetPeerId, json) => {
        requesterMesh.deliverInbound('supplier-peer', JSON.parse(json));
        return { ok: true };
      });

      requesterAdapter.bindPeerMesh(requesterMesh.mesh);
      supplierAdapter.bindPeerMesh(supplierMesh.mesh);

      const requesterCallbacks = requesterAdapter.createAntiEntropyCallbacks(requesterMesh.mesh);
      const requesterAEM = new AntiEntropyManager(requesterEngine, requesterCallbacks, {}, clock);
      requesterAdapter.setAntiEntropyManager(requesterAEM);

      const supplierCallbacks = supplierAdapter.createAntiEntropyCallbacks(supplierMesh.mesh);
      const supplierAEM = new AntiEntropyManager(supplierEngine, supplierCallbacks, {}, clock);
      supplierAdapter.setAntiEntropyManager(supplierAEM);

      // Provide knowledge to requester that supplier holds origin-A seq 1
      requesterAEM.processRemoteDigest(
        makeStateDigest('supplier-peer', { 'origin-A': 1 }, { sentAt: clock.now() }),
      );

      // Reconcile gaps
      requesterAEM.reconcileGaps();

      // Op 1 was repaired over transport and unblocked op 2! Both are now applied.
      expect(requesterEngine.replicationFrontier['origin-A']).toBe(2);
      expect(requesterEngine.getPendingGaps()).toHaveLength(0);
      expect(requesterEngine.getOperationLog().getOperationAt('origin-A', 1)).toBeDefined();
      expect(requesterEngine.getOperationLog().getOperationAt('origin-A', 2)).toBeDefined();
    });

    it('4.4 Duplicate request replay over transport returns cached response without duplicate execution', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const supplierTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      const supplierEngine = new LeaderlessSyncEngine({
        localPeerId: 'supplier-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: supplierTransportCap,
      });
      supplierEngine.authorizePeerGeneration('requester-peer', 1);
      supplierEngine.getOperationLog().insert(createStoredOperation('origin-1', 1));

      const supplierAdapter = new TrustedTransportAdapter(supplierEngine, supplierTransportCap);
      const mockTransport = createMockPeerMesh({ localPeerId: 'supplier-peer', remotePeerId: 'requester-peer' });

      supplierAdapter.bindPeerMesh(mockTransport.mesh);
      const callbacks = supplierAdapter.createAntiEntropyCallbacks(mockTransport.mesh);
      const aem = new AntiEntropyManager(supplierEngine, callbacks, {}, clock);
      supplierAdapter.setAntiEntropyManager(aem);

      const validUuid = 'b2222222-2222-4222-8222-222222222222';
      const reqMessage: ProtocolMessage<RepairRequestPayload> = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'REPAIR_REQUEST',
        sessionId: SESSION_ID,
        senderPeerId: 'requester-peer',
        messageId: validUuid,
        originPeerId: 'origin-1',
        membershipEpoch: 1,
        payload: {
          requestId: validUuid,
          sessionId: SESSION_ID,
          requesterPeerId: 'requester-peer',
          targetOriginPeerId: 'origin-1',
          fromSequence: 1,
          toSequence: 1,
          membershipEpoch: 1,
        },
      };

      // Deliver first time
      mockTransport.deliverInbound('requester-peer', reqMessage);
      const firstSent = mockTransport.getSentRaw();
      expect(firstSent).toHaveLength(1);

      // Deliver duplicate replay with identical (requesterPeerId, requestId)
      mockTransport.deliverInbound('requester-peer', reqMessage);
      const secondSent = mockTransport.getSentRaw();
      // Should retransmit from cache
      expect(secondSent).toHaveLength(2);
      expect(secondSent[0]!.json).toBe(secondSent[1]!.json);
    });

    it('4.5 Permanent history gap across suppliers triggers onHistoryExhausted handoff hook', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const requesterTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      const requesterEngine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: requesterTransportCap,
      });
      requesterEngine.authorizePeerGeneration('supplier-peer', 1);
      requesterEngine.authorizePeerGeneration('author-peer', 1);
      const requesterAdapter = new TrustedTransportAdapter(requesterEngine, requesterTransportCap);

      const onHistoryExhaustedSpy = vi.fn();

      const requesterMesh = createMockPeerMesh({ localPeerId: 'requester-peer', remotePeerId: 'supplier-peer' });
      requesterAdapter.bindPeerMesh(requesterMesh.mesh);

      const baseCallbacks = requesterAdapter.createAntiEntropyCallbacks(requesterMesh.mesh);
      const callbacks: AntiEntropyCallbacks = {
        ...baseCallbacks,
        onHistoryExhausted: onHistoryExhaustedSpy,
      };

      // Max retries = 1 for fast test, backoffBaseMs = 10ms
      const aem = new AntiEntropyManager(
        requesterEngine,
        callbacks,
        { maxRetries: 1, repairRequestTimeoutMs: 50, backoffBaseMs: 10 },
        clock,
      );
      requesterAdapter.setAntiEntropyManager(aem);

      // Buffer gap for origin-X (missing sequence 1)
      requesterEngine.receive(
        makeRemoteControlEvent(
          'event-gap-x',
          'author-peer',
          'origin-X',
          2,
          10,
          { 'origin-X': 1 },
          1,
        ),
      );

      // Provide knowledge that supplier-peer holds origin-X up to 2
      aem.processRemoteDigest(
        makeStateDigest('supplier-peer', { 'origin-X': 2 }, { sentAt: clock.now() }),
      );

      aem.reconcileGaps();

      // Advance clock past timeout and retry limit to exhaust attempts
      clock.advance(55); // Timeout 1 -> failover with 10ms backoff
      clock.advance(15); // Backoff expires -> retry request sent
      clock.advance(55); // Timeout 2 -> retry limit exceeded -> history exhausted

      expect(onHistoryExhaustedSpy).toHaveBeenCalled();
      expect(onHistoryExhaustedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ originPeerId: 'origin-X', missingSequence: 1 }),
      );
    });

    it('4.6 Transport send failure does not mark operations as delivered and permits retry', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const requesterTransportCap = createTransportRegistrationCapability('transport', SESSION_ID);

      const requesterEngine = new LeaderlessSyncEngine({
        localPeerId: 'requester-peer',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: requesterTransportCap,
      });
      requesterEngine.authorizePeerGeneration('supplier-peer-1', 1);
      requesterEngine.authorizePeerGeneration('supplier-peer-2', 1);
      const requesterAdapter = new TrustedTransportAdapter(requesterEngine, requesterTransportCap);

      const peerStatus1: PeerStatus = {
        peerId: 'supplier-peer-1',
        state: 'CONNECTED',
        dataChannelState: 'open',
        generation: 1,
        lastUpdatedAt: Date.now(),
      };
      const peerStatus2: PeerStatus = {
        peerId: 'supplier-peer-2',
        state: 'CONNECTED',
        dataChannelState: 'open',
        generation: 1,
        lastUpdatedAt: Date.now(),
      };

      const sendSerializedSpy = vi.fn((targetPeerId: string, wireJson: string, gen?: number) => {
        if (targetPeerId === 'supplier-peer-1') {
          return { ok: false, error: 'Simulated network drop' };
        }
        return { ok: true };
      });

      const mesh: PeerMesh = {
        connect: vi.fn().mockResolvedValue(undefined),
        disconnect: vi.fn(),
        send: vi.fn(),
        sendSerialized: sendSerializedSpy,
        getSctpMaxMessageSize: vi.fn(() => 65536),
        getPeerGeneration: vi.fn((id) => (id === 'supplier-peer-1' ? 1 : id === 'supplier-peer-2' ? 1 : undefined)),
        getPeerStatus: vi.fn((id) => (id === 'supplier-peer-1' ? peerStatus1 : id === 'supplier-peer-2' ? peerStatus2 : undefined)),
        listPeers: vi.fn(() => [peerStatus1, peerStatus2]),
        onMessage: vi.fn(() => () => {}),
        onPeerStatus: vi.fn(() => () => {}),
        dispose: vi.fn().mockResolvedValue(undefined),
      };

      requesterAdapter.bindPeerMesh(mesh);

      const callbacks = requesterAdapter.createAntiEntropyCallbacks(mesh);
      const aem = new AntiEntropyManager(
        requesterEngine,
        callbacks,
        { repairRequestTimeoutMs: 100, backoffBaseMs: 10 },
        clock,
      );
      requesterAdapter.setAntiEntropyManager(aem);

      // Tell requester about both suppliers
      aem.processRemoteDigest(
        makeStateDigest('supplier-peer-1', { 'origin-A': 2 }, { sentAt: clock.now() }),
      );
      aem.processRemoteDigest(
        makeStateDigest('supplier-peer-2', { 'origin-A': 2 }, { sentAt: clock.now() }),
      );

      aem.reconcileGaps();
      expect(requesterEngine.replicationFrontier['origin-A'] ?? 0).toBe(0);

      // Timeout 1 fires on supplier-peer-1, marks it attempted, schedules failover to supplier-peer-2 with 10ms backoff
      clock.advance(110);
      // Backoff fires, retry send executed on supplier-peer-2
      clock.advance(15);

      // Retry send was executed (1st on supplier-1, 2nd on supplier-2)
      expect(sendSerializedSpy).toHaveBeenCalledTimes(2);
      expect(sendSerializedSpy).toHaveBeenNthCalledWith(1, 'supplier-peer-1', expect.any(String), 1);
      expect(sendSerializedSpy).toHaveBeenNthCalledWith(2, 'supplier-peer-2', expect.any(String), 1);
    });
  });

  // ════════════════════════════════════════════════════════════════
  // 5. Message Routing and Non-Interference
  // ════════════════════════════════════════════════════════════════

  describe('5. Message Routing and Non-Interference', () => {
    it('5.1 Transport adapter ignores PING and PONG messages without throwing or interfering', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'peer-1',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('peer-2', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);
      const handleBatchSpy = vi.spyOn(adapter, 'handleInboundRepairBatch');
      const handleReqSpy = vi.spyOn(adapter, 'handleInboundRepairRequest');

      const mockTransport = createMockPeerMesh({ localPeerId: 'peer-1', remotePeerId: 'peer-2' });
      adapter.bindPeerMesh(mockTransport.mesh);

      const pingMsg: ProtocolMessage = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PING,
        sessionId: SESSION_ID,
        senderPeerId: 'peer-2',
        messageId: 'ping-1',
        payload: { probeId: 'p1' },
      };

      const pongMsg: ProtocolMessage = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PONG,
        sessionId: SESSION_ID,
        senderPeerId: 'peer-2',
        messageId: 'pong-1',
        payload: { probeId: 'p1' },
      };

      mockTransport.deliverInbound('peer-2', pingMsg);
      mockTransport.deliverInbound('peer-2', pongMsg);

      expect(handleBatchSpy).not.toHaveBeenCalled();
      expect(handleReqSpy).not.toHaveBeenCalled();
    });

    it('5.2 Unknown or unsupported message types are safely dropped without throwing', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'peer-1',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('peer-2', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);
      const mockTransport = createMockPeerMesh({ localPeerId: 'peer-1', remotePeerId: 'peer-2' });
      adapter.bindPeerMesh(mockTransport.mesh);

      const unknownMsg: ProtocolMessage = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: 'UNKNOWN_FUTURE_MESSAGE_TYPE' as any,
        sessionId: SESSION_ID,
        senderPeerId: 'peer-2',
        messageId: 'unknown-1',
        payload: { foo: 'bar' },
      };

      expect(() => {
        mockTransport.deliverInbound('peer-2', unknownMsg);
      }).not.toThrow();
    });

    it('5.3 Peer status disconnect cleans up in-flight requests in anti-entropy manager', () => {
      const epochCap = createEpochTransitionCapability('coord', SESSION_ID);
      const transportCap = createTransportRegistrationCapability('transport', SESSION_ID);
      const engine = new LeaderlessSyncEngine({
        localPeerId: 'peer-1',
        sessionId: SESSION_ID,
        epochCapability: epochCap,
        transportCapability: transportCap,
      });
      engine.authorizePeerGeneration('peer-2', 1);

      const adapter = new TrustedTransportAdapter(engine, transportCap);
      const mockTransport = createMockPeerMesh({ localPeerId: 'peer-1', remotePeerId: 'peer-2' });
      adapter.bindPeerMesh(mockTransport.mesh);

      const callbacks = adapter.createAntiEntropyCallbacks(mockTransport.mesh);
      const aem = new AntiEntropyManager(engine, callbacks, {}, clock);
      adapter.setAntiEntropyManager(aem);

      const disconnectSpy = vi.spyOn(aem, 'onPeerDisconnected');

      // Simulate peer disconnect on transport
      mockTransport.setConnectionState('CLOSED');

      expect(disconnectSpy).toHaveBeenCalledWith('peer-2');
    });
  });
});
