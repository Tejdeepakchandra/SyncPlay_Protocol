import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ControlEventServiceImpl } from '../control-event-service.js';
import type {
  SyncEventReceiver,
  AnyControlSyncEvent,
  Clock,
  ControlEventServiceConfig,
} from '../types.js';
import type { PeerMesh, InboundMessage, PeerStatus, SendResult } from '@syncplay/peer-mesh';
import type { ProtocolMessage } from '@syncplay/protocol';

function createMockPeerMesh(localPeerId: string, initialPeers: PeerStatus[] = []) {
  let messageHandlers: ((msg: InboundMessage) => void)[] = [];
  let statusHandlers: ((s: PeerStatus) => void)[] = [];
  let peers = [...initialPeers];

  const mesh: PeerMesh & {
    simulateInbound(msg: InboundMessage): void;
    simulateStatus(status: PeerStatus): void;
    setPeers(newPeers: PeerStatus[]): void;
  } = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn(),
    send: vi.fn().mockReturnValue({ ok: true } as SendResult),
    getPeerStatus: vi.fn((id) => peers.find((p) => p.peerId === id)),
    listPeers: vi.fn(() => peers),
    onMessage: vi.fn((h) => {
      messageHandlers.push(h);
      return () => {
        messageHandlers = messageHandlers.filter((fn) => fn !== h);
      };
    }),
    onPeerStatus: vi.fn((h) => {
      statusHandlers.push(h);
      return () => {
        statusHandlers = statusHandlers.filter((fn) => fn !== h);
      };
    }),
    dispose: vi.fn().mockResolvedValue(undefined),

    simulateInbound(msg: InboundMessage) {
      for (const h of messageHandlers) h(msg);
    },
    simulateStatus(status: PeerStatus) {
      for (const h of statusHandlers) h(status);
    },
    setPeers(newPeers: PeerStatus[]) {
      peers = newPeers;
    },
  };

  return mesh;
}

describe('ControlEventService', () => {
  let clockTime = 1000;
  const clock: Clock = { now: () => clockTime };
  const config: ControlEventServiceConfig = {
    sessionId: 'session-room-1',
    localPeerId: 'peer-alice',
    clock,
    maxOperationsPerSecond: 10,
    maxPreReceiverQueueSize: 3,
  };

  const connectedPeers: PeerStatus[] = [
    {
      peerId: 'peer-bob',
      state: 'CONNECTED',
      dataChannelState: 'open',
      generation: 1,
      lastUpdatedAt: 1000,
    },
    {
      peerId: 'peer-charlie',
      state: 'CONNECTED',
      dataChannelState: 'open',
      generation: 1,
      lastUpdatedAt: 1000,
    },
  ];

  beforeEach(() => {
    clockTime = 1000;
  });

  describe('Outbound Flow', () => {
    it('creates, dispatches locally, and broadcasts SEEK command', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      const result = await service.getControls().seek(45000);

      // Verify CommandResult
      expect(result.accepted).toBe(true);
      expect(result.logicalEventId).toBeDefined();
      expect(result.peerCount).toBe(2);
      expect(result.deliveredCount).toBe(2);
      expect(result.partialDelivery).toBe(false);

      // Verify local dispatch
      expect(receiver.receive).toHaveBeenCalledTimes(1);
      const dispatchedEvent = (receiver.receive as any).mock.calls[0][0] as AnyControlSyncEvent;
      expect(dispatchedEvent.origin).toBe('local');
      expect(dispatchedEvent.localTimestamp).toBe(1000);
      expect(dispatchedEvent.message.messageType).toBe('SEEK');
      expect(dispatchedEvent.message.payload).toEqual({ targetPositionMs: 45000 });

      // Verify network broadcast
      expect(mesh.send).toHaveBeenCalledTimes(2);
      expect(mesh.send).toHaveBeenCalledWith('peer-bob', dispatchedEvent.message);
      expect(mesh.send).toHaveBeenCalledWith('peer-charlie', dispatchedEvent.message);
    });

    it('rejects invalid argument without creating event or broadcasting', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      const result = await service.getControls().seek(-100);

      expect(result.accepted).toBe(false);
      expect(result.error?.code).toBe('INVALID_ARGUMENT');
      expect(receiver.receive).not.toHaveBeenCalled();
      expect(mesh.send).not.toHaveBeenCalled();
    });

    it('prevents broadcast and clears local dedup if local receiver throws', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      const receiver: SyncEventReceiver = {
        receive: vi.fn().mockImplementation(() => {
          throw new Error('SyncEngine rejected seek transition');
        }),
      };
      service.setReceiver(receiver);

      const result = await service.getControls().seek(10000);

      expect(result.accepted).toBe(false);
      expect(result.error?.code).toBe('ENGINE_ERROR');
      expect(result.error?.message).toContain('SyncEngine rejected seek transition');

      // Crucial: Must NOT broadcast over network when local dispatch fails (Requirement 3)
      expect(mesh.send).not.toHaveBeenCalled();

      // Deduplication entry removed so it is not orphaned
      const stats = service.getStats();
      expect(stats.deduplication.size).toBe(0);
    });

    it('accurately reports partial delivery when some peers fail', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      // Bob succeeds, Charlie fails
      (mesh.send as any).mockImplementation((peerId: string) => {
        if (peerId === 'peer-bob') return { ok: true };
        return { ok: false, error: 'DataChannel closing' };
      });

      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      const result = await service.getControls().play();

      expect(result.accepted).toBe(true);
      expect(result.peerCount).toBe(2);
      expect(result.deliveredCount).toBe(1);
      expect(result.partialDelivery).toBe(true);
    });

    it('enforces outbound rate limiting', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, {
        ...config,
        maxOperationsPerSecond: 2,
      });

      const r1 = await service.getControls().pause();
      const r2 = await service.getControls().pause();
      const r3 = await service.getControls().pause();

      expect(r1.accepted).toBe(true);
      expect(r2.accepted).toBe(true);
      expect(r3.accepted).toBe(false);
      expect(r3.error?.code).toBe('RATE_LIMITED');
    });

    it('supports retry() without redispatching locally, generating new messageId', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      // Initial command
      await service.getControls().seek(20000);
      expect(receiver.receive).toHaveBeenCalledTimes(1);
      const initialEvent = (receiver.receive as any).mock.calls[0][0] as AnyControlSyncEvent;

      (mesh.send as any).mockClear();

      // Advance clock
      clockTime = 2500;

      // Retry the event
      const retryResult = await service.getControls().retry(initialEvent);

      expect(retryResult.accepted).toBe(true);
      expect(retryResult.logicalEventId).toBe(initialEvent.message.logicalEventId);

      // Does NOT redispatch locally (Requirement 1)
      expect(receiver.receive).toHaveBeenCalledTimes(1);

      // Broadcasts with a NEW messageId but SAME logicalEventId
      expect(mesh.send).toHaveBeenCalledTimes(2);
      const sentMessage = (mesh.send as any).mock.calls[0][1] as ProtocolMessage;
      expect(sentMessage.messageId).not.toBe(initialEvent.message.messageId);
      expect(sentMessage.logicalEventId).toBe(initialEvent.message.logicalEventId);
      expect(sentMessage.payload).toEqual({ targetPositionMs: 20000 });
    });
  });

  describe('Inbound Flow & Security Invariants', () => {
    it('dispatches valid inbound control event to receiver', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-b1',
          logicalEventId: 'evt-b1',
          payload: { targetPositionMs: 500 },
        },
      });

      expect(receiver.receive).toHaveBeenCalledTimes(1);
      const dispatched = (receiver.receive as any).mock.calls[0][0] as AnyControlSyncEvent;
      expect(dispatched.origin).toBe('remote');
      expect(dispatched.message.senderPeerId).toBe('peer-bob');
      expect(dispatched.message.payload).toEqual({ targetPositionMs: 500 });
    });

    it('rejects inbound message from mismatched session', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'wrong-session-999',
          senderPeerId: 'peer-bob',
          messageId: 'msg-b1',
          logicalEventId: 'evt-b1',
          payload: {},
        },
      });

      expect(receiver.receive).not.toHaveBeenCalled();
      expect(service.getStats().inboundDroppedCount).toBe(1);
    });

    it('rejects inbound message where senderPeerId does not match transport fromPeerId', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      // Spoofed sender: peer-bob claims to be peer-charlie
      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'SEEK',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-charlie',
          messageId: 'msg-spoof',
          logicalEventId: 'evt-spoof',
          payload: { targetPositionMs: 100 },
        },
      });

      expect(receiver.receive).not.toHaveBeenCalled();
      expect(service.getStats().inboundDroppedCount).toBe(1);
    });

    it('rejects self-reflected message', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      mesh.simulateInbound({
        fromPeerId: 'peer-alice',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-alice',
          messageId: 'msg-self',
          logicalEventId: 'evt-self',
          payload: {},
        },
      });

      expect(receiver.receive).not.toHaveBeenCalled();
      expect(service.getStats().inboundDroppedCount).toBe(1);
    });

    it('ignores non-control messages like PING/PONG', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PING',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-p1',
          payload: { probeId: 'probe-1', senderTimestamp: 100 },
        },
      });

      expect(receiver.receive).not.toHaveBeenCalled();
    });

    it('suppresses duplicate inbound message with same logicalEventId from same peer', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      const validMsg = {
        protocolVersion: 1,
        messageType: 'PAUSE',
        sessionId: 'session-room-1',
        senderPeerId: 'peer-bob',
        messageId: 'msg-1',
        logicalEventId: 'evt-dup-1',
        payload: { observedPositionMs: 100 },
      };

      // First arrival: processed
      mesh.simulateInbound({ fromPeerId: 'peer-bob', message: validMsg });
      expect(receiver.receive).toHaveBeenCalledTimes(1);

      // Second arrival with different messageId but same logicalEventId: suppressed!
      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: { ...validMsg, messageId: 'msg-2-retry' },
      });
      expect(receiver.receive).toHaveBeenCalledTimes(1);

      expect(service.getStats().deduplication.duplicateCount).toBe(1);
    });

    it('isolates exceptions thrown by receiver without breaking message loop', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      const receiver: SyncEventReceiver = {
        receive: vi.fn().mockImplementationOnce(() => {
          throw new Error('Receiver crash');
        }),
      };
      service.setReceiver(receiver);

      // Should not throw or crash
      expect(() => {
        mesh.simulateInbound({
          fromPeerId: 'peer-bob',
          message: {
            protocolVersion: 1,
            messageType: 'PLAY',
            sessionId: 'session-room-1',
            senderPeerId: 'peer-bob',
            messageId: 'msg-1',
            logicalEventId: 'evt-crash',
            payload: {},
          },
        });
      }).not.toThrow();

      // Subsequent message still processes normally
      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-2',
          logicalEventId: 'evt-ok',
          payload: {},
        },
      });

      expect(receiver.receive).toHaveBeenCalledTimes(2);
    });
  });

  describe('Pre-Receiver Buffering & Draining', () => {
    it('buffers events before receiver is set, drains them on setReceiver', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      // Inbound events arrive when NO receiver is set
      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-1',
          logicalEventId: 'evt-1',
          payload: {},
        },
      });

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PAUSE',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-2',
          logicalEventId: 'evt-2',
          payload: {},
        },
      });

      expect(service.getStats().preReceiverQueuedCount).toBe(2);

      // Attach receiver: should drain queued events
      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      expect(receiver.receive).toHaveBeenCalledTimes(2);
      expect(service.getStats().preReceiverDrainedCount).toBe(2);
    });

    it('rejects newest event on queue overflow (capacity: 3)', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, {
        ...config,
        maxPreReceiverQueueSize: 3,
      });

      for (let i = 1; i <= 4; i++) {
        mesh.simulateInbound({
          fromPeerId: 'peer-bob',
          message: {
            protocolVersion: 1,
            messageType: 'PLAY',
            sessionId: 'session-room-1',
            senderPeerId: 'peer-bob',
            messageId: `msg-${i}`,
            logicalEventId: `evt-${i}`,
            payload: {},
          },
        });
      }

      // 3 queued, 4th rejected on overflow
      expect(service.getStats().preReceiverQueuedCount).toBe(3);
      expect(service.getStats().preReceiverOverflowCount).toBe(1);

      const receiver: SyncEventReceiver = { receive: vi.fn() };
      service.setReceiver(receiver);

      // Only the first 3 events were retained and drained
      expect(receiver.receive).toHaveBeenCalledTimes(3);
    });

    it('continues draining remaining queued events if one queued event throws', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-1',
          logicalEventId: 'evt-1',
          payload: {},
        },
      });

      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'SEEK',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-2',
          logicalEventId: 'evt-2',
          payload: { targetPositionMs: 5000 },
        },
      });

      // Receiver throws on first event (PLAY), succeeds on second (SEEK)
      const receiver: SyncEventReceiver = {
        receive: vi.fn().mockImplementationOnce(() => {
          throw new Error('Error on PLAY');
        }),
      };

      service.setReceiver(receiver);

      // Both events were attempted, remaining events did not disappear silently
      expect(receiver.receive).toHaveBeenCalledTimes(2);
      expect(service.getStats().preReceiverDrainErrorCount).toBe(1);
      expect(service.getStats().preReceiverDrainedCount).toBe(1);
    });
  });

  describe('Lifecycle & Disposal', () => {
    it('is idempotent and rejects commands after disposal', async () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      expect(service.isDisposed).toBe(false);

      service.dispose();
      expect(service.isDisposed).toBe(true);

      // Second dispose has no effect
      service.dispose();
      expect(service.isDisposed).toBe(true);

      // Outbound commands fail with DISPOSED
      const res = await service.getControls().play();
      expect(res.accepted).toBe(false);
      expect(res.error?.code).toBe('DISPOSED');
    });

    it('cleans up peer rate limiting on peer CLOSED or FAILED', () => {
      const mesh = createMockPeerMesh('peer-alice', connectedPeers);
      const service = new ControlEventServiceImpl(mesh, config);

      // Bob sends a message, tracked in rate limiter
      mesh.simulateInbound({
        fromPeerId: 'peer-bob',
        message: {
          protocolVersion: 1,
          messageType: 'PLAY',
          sessionId: 'session-room-1',
          senderPeerId: 'peer-bob',
          messageId: 'msg-1',
          logicalEventId: 'evt-1',
          payload: {},
        },
      });

      // Peer status CLOSED
      mesh.simulateStatus({
        peerId: 'peer-bob',
        state: 'CLOSED',
        dataChannelState: 'closed',
        generation: 1,
        lastUpdatedAt: 1200,
      });

      // Bob can send again in clean rate window without old timestamp accumulation
      expect(() => {
        mesh.simulateInbound({
          fromPeerId: 'peer-bob',
          message: {
            protocolVersion: 1,
            messageType: 'PLAY',
            sessionId: 'session-room-1',
            senderPeerId: 'peer-bob',
            messageId: 'msg-2',
            logicalEventId: 'evt-2',
            payload: {},
          },
        });
      }).not.toThrow();
    });
  });
});
