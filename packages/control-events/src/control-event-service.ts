/**
 * Control Event Service.
 *
 * Implements the core pipeline for Phase 5:
 * - Translates high-level user intents into validated ProtocolMessages.
 * - Outbound: validates, checks local rate limit, dispatches locally, broadcasts to mesh.
 * - Inbound: filters, validates session/sender, enforces rate limits, deduplicates, dispatches to receiver.
 * - Manages bounded pre-receiver queues, composite-key deduplication, and lifecycle.
 */

import { validateMessage } from '@syncplay/protocol';
import type { PeerMesh, InboundMessage, PeerStatus, Unsubscribe } from '@syncplay/peer-mesh';
import type {
  ControlEventService,
  ControlEventServiceConfig,
  SyncPlayControls,
  SyncEventReceiver,
  CommandResult,
  PublishResult,
  PublishReport,
  ServiceStats,
  Clock,
  AnyControlSyncEvent,
  ControlIntent,
} from './types.js';
import { DEFAULT_CONTROL_CONFIG } from './types.js';
import { createControlEvent, createRetryEvent } from './event-factory.js';
import { DeduplicationCache } from './dedup-cache.js';
import { BoundedRateLimiter } from './rate-limiter.js';
import { ControlsImpl } from './controls.js';
import type { IntentExecutor } from './controls.js';
import type { CommandErrorCode } from './errors.js';

export class ControlEventServiceImpl implements ControlEventService, IntentExecutor {
  private readonly config: ControlEventServiceConfig;
  private readonly mesh: PeerMesh;
  private readonly clock: Clock;
  private readonly dedupCache: DeduplicationCache;
  private readonly rateLimiter: BoundedRateLimiter;
  private readonly controls: SyncPlayControls;

  private receiver: SyncEventReceiver | null = null;
  private readonly preReceiverQueue: AnyControlSyncEvent[] = [];
  private readonly maxPreReceiverQueueSize: number;

  private unsubMessage: Unsubscribe | null = null;
  private unsubPeerStatus: Unsubscribe | null = null;

  private disposed = false;

  private readonly stats: ServiceStats = {
    inboundReceivedCount: 0,
    inboundDispatchedCount: 0,
    inboundDroppedCount: 0,
    outboundCreatedCount: 0,
    outboundDispatchedCount: 0,
    preReceiverQueuedCount: 0,
    preReceiverOverflowCount: 0,
    preReceiverDrainedCount: 0,
    preReceiverDrainErrorCount: 0,
    deduplication: { size: 0, duplicateCount: 0, evictedCount: 0 },
  };

  constructor(mesh: PeerMesh, config: ControlEventServiceConfig) {
    this.mesh = mesh;
    this.config = config;
    this.clock = config.clock ?? { now: () => performance.now() };

    this.maxPreReceiverQueueSize =
      config.maxPreReceiverQueueSize ?? DEFAULT_CONTROL_CONFIG.maxPreReceiverQueueSize;

    this.dedupCache = new DeduplicationCache({
      maxEntries: config.maxDeduplicationEntries ?? DEFAULT_CONTROL_CONFIG.maxDeduplicationEntries,
      ttlMs: config.deduplicationTtlMs ?? DEFAULT_CONTROL_CONFIG.deduplicationTtlMs,
      clock: this.clock,
    });

    this.rateLimiter = new BoundedRateLimiter({
      maxOperationsPerSecond:
        config.maxOperationsPerSecond ?? DEFAULT_CONTROL_CONFIG.maxOperationsPerSecond,
      maxTrackedPeers:
        config.maxTrackedRateLimitPeers ?? DEFAULT_CONTROL_CONFIG.maxTrackedRateLimitPeers,
      clock: this.clock,
    });

    this.controls = new ControlsImpl(this);

    // Subscribe to PeerMesh transport events
    this.unsubMessage = this.mesh.onMessage((msg) => this.handleInbound(msg));
    this.unsubPeerStatus = this.mesh.onPeerStatus((status) => this.handlePeerStatus(status));
  }

  // ── Public Accessors ─────────────────────────────────────────

  public get isDisposed(): boolean {
    return this.disposed;
  }

  public getControls(): SyncPlayControls {
    return this.controls;
  }

  public setReceiver(receiver: SyncEventReceiver): void {
    if (this.disposed) return;
    this.receiver = receiver;

    // Drain queued events in FIFO order
    while (this.preReceiverQueue.length > 0) {
      const event = this.preReceiverQueue.shift()!;
      try {
        this.receiver.receive(event);
        this.stats.preReceiverDrainedCount++;
        this.stats.inboundDispatchedCount++;
      } catch {
        // If receiver throws, log and continue draining remaining queued events (Requirement 4)
        this.stats.preReceiverDrainErrorCount++;
      }
    }
  }

  public clearReceiver(): void {
    this.receiver = null;
  }

  public getStats(): ServiceStats {
    return {
      ...this.stats,
      deduplication: this.dedupCache.getStats(),
    };
  }

  // ── Outbound Intent Execution (IntentExecutor) ───────────────

  public async executeIntent(intent: ControlIntent): Promise<CommandResult> {
    if (this.disposed) {
      return {
        accepted: false,
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
        error: { code: 'DISPOSED', message: 'ControlEventService has been disposed' },
      };
    }

    // 1. Check outbound rate limit
    if (!this.rateLimiter.checkOutbound()) {
      return {
        accepted: false,
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
        error: { code: 'RATE_LIMITED', message: 'Local command rate limit exceeded' },
      };
    }

    // 2. Create and semantically validate event candidate
    const eventResult = createControlEvent(intent, this.config, this.clock);
    if (!eventResult.ok) {
      const code: CommandErrorCode =
        eventResult.error.code === 'INVALID_ARGUMENT' ? 'INVALID_ARGUMENT' : 'ENGINE_ERROR';
      return {
        accepted: false,
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
        error: { code, message: eventResult.error.message },
      };
    }

    const event = eventResult.value;
    const logicalEventId = event.message.logicalEventId!;
    this.stats.outboundCreatedCount++;

    // 3. Pre-record into deduplication cache
    this.dedupCache.checkAndRecord(this.config.localPeerId, logicalEventId);

    // 4. Local dispatch to SyncEventReceiver
    if (this.receiver !== null) {
      try {
        this.receiver.receive(event);
        this.stats.outboundDispatchedCount++;
      } catch (err) {
        // If local receiver throws:
        // - Do NOT broadcast to the mesh (Requirement 3)
        // - Remove local deduplication entry (Requirement 3)
        this.dedupCache.remove(this.config.localPeerId, logicalEventId);

        return {
          accepted: false,
          logicalEventId,
          peerCount: 0,
          deliveredCount: 0,
          partialDelivery: false,
          error: {
            code: 'ENGINE_ERROR',
            message: err instanceof Error ? err.message : String(err),
          },
        };
      }
    }

    // 5. Broadcast to connected peers in mesh
    const publishResult = await this.publish(event);

    return {
      accepted: true,
      logicalEventId,
      peerCount: publishResult.peerCount,
      deliveredCount: publishResult.deliveredCount,
      partialDelivery: publishResult.partialDelivery,
    };
  }

  public async executeRetry(event: AnyControlSyncEvent): Promise<CommandResult> {
    if (this.disposed) {
      return {
        accepted: false,
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
        error: { code: 'DISPOSED', message: 'ControlEventService has been disposed' },
      };
    }

    // Outbound rate check
    if (!this.rateLimiter.checkOutbound()) {
      return {
        accepted: false,
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
        error: { code: 'RATE_LIMITED', message: 'Local command rate limit exceeded' },
      };
    }

    // Allocate fresh messageId while preserving original logicalEventId
    const retryEvent = createRetryEvent(event, this.clock);
    const logicalEventId = retryEvent.message.logicalEventId!;

    // Avoid redispatching to local receiver (Requirement 1)
    // Broadcast only to currently connected peers
    const publishResult = await this.publish(retryEvent);

    return {
      accepted: true,
      logicalEventId,
      peerCount: publishResult.peerCount,
      deliveredCount: publishResult.deliveredCount,
      partialDelivery: publishResult.partialDelivery,
    };
  }

  // ── Network Publishing ───────────────────────────────────────

  public async publish(event: AnyControlSyncEvent): Promise<PublishResult> {
    const logicalEventId = event.message.logicalEventId!;
    const messageId = event.message.messageId;

    if (this.disposed) {
      return {
        logicalEventId,
        messageId,
        recipients: [],
        peerCount: 0,
        deliveredCount: 0,
        partialDelivery: false,
      };
    }

    // Identify active remote peers
    const remotePeers = this.mesh
      .listPeers()
      .filter((p) => p.state === 'CONNECTED' && p.peerId !== this.config.localPeerId);

    const reports: PublishReport[] = [];
    let deliveredCount = 0;

    for (const peer of remotePeers) {
      try {
        const sendResult = this.mesh.send(peer.peerId, event.message);
        if (sendResult.ok) {
          deliveredCount++;
          reports.push({ peerId: peer.peerId, ok: true });
        } else {
          reports.push({ peerId: peer.peerId, ok: false, error: sendResult.error });
        }
      } catch (err) {
        reports.push({
          peerId: peer.peerId,
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    const peerCount = remotePeers.length;
    const partialDelivery = deliveredCount < peerCount && peerCount > 0;

    return {
      logicalEventId,
      messageId,
      recipients: reports,
      peerCount,
      deliveredCount,
      partialDelivery,
    };
  }

  // ── Inbound Pipeline ─────────────────────────────────────────

  private handleInbound(inbound: InboundMessage): void {
    if (this.disposed) return;
    this.stats.inboundReceivedCount++;

    const msg = inbound.message;
    if (!msg) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 1. Message Type Scope: only control events handled here
    if (
      msg.messageType !== 'PLAY' &&
      msg.messageType !== 'PAUSE' &&
      msg.messageType !== 'SEEK' &&
      msg.messageType !== 'RATE_CHANGE'
    ) {
      // Non-control messages (PING, PONG, state exchange) are handled elsewhere
      return;
    }

    // 2. Session verification
    if (msg.sessionId !== this.config.sessionId) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 3. Sender spoofing check: claimed sender must match transport identity
    if (msg.senderPeerId !== inbound.fromPeerId) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 4. Self-reflection guard: ignore our own messages if reflected
    if (msg.senderPeerId === this.config.localPeerId) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 5. Require logicalEventId
    if (!msg.logicalEventId || typeof msg.logicalEventId !== 'string') {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 6. Protocol envelope & payload schema validation
    const valResult = validateMessage(msg);
    if (!valResult.ok) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 7. Inbound per-peer rate limiting
    if (!this.rateLimiter.checkInbound(inbound.fromPeerId)) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 8. Deduplication check using composite key: `${senderPeerId}:${logicalEventId}`
    const isDuplicate = this.dedupCache.checkAndRecord(inbound.fromPeerId, msg.logicalEventId);
    if (isDuplicate) {
      this.stats.inboundDroppedCount++;
      return;
    }

    // 9. Wrap in local SyncEvent representation (origin: 'remote')
    const syncEvent = {
      message: valResult.value,
      localTimestamp: this.clock.now(),
      origin: 'remote' as const,
    } as AnyControlSyncEvent;

    // 10. Dispatch to receiver or pre-receiver queue
    if (this.receiver !== null) {
      try {
        this.receiver.receive(syncEvent);
        this.stats.inboundDispatchedCount++;
      } catch {
        // Receiver error caught safely to protect transport event loop (Requirement 3)
      }
    } else {
      // Pre-receiver buffering
      if (this.preReceiverQueue.length < this.maxPreReceiverQueueSize) {
        this.preReceiverQueue.push(syncEvent);
        this.stats.preReceiverQueuedCount++;
      } else {
        // Reject newest event on overflow (Requirement 4)
        this.stats.preReceiverOverflowCount++;
        this.stats.inboundDroppedCount++;
      }
    }
  }

  // ── Peer Status Handling ─────────────────────────────────────

  private handlePeerStatus(status: PeerStatus): void {
    if (this.disposed) return;
    if (status.state === 'CLOSED' || status.state === 'FAILED') {
      // Clean up rate limiting records for disconnected peers (Requirement 5)
      this.rateLimiter.removePeer(status.peerId);
    }
  }

  // ── Lifecycle Teardown ───────────────────────────────────────

  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    if (this.unsubMessage) {
      this.unsubMessage();
      this.unsubMessage = null;
    }

    if (this.unsubPeerStatus) {
      this.unsubPeerStatus();
      this.unsubPeerStatus = null;
    }

    this.receiver = null;
    this.preReceiverQueue.length = 0;
    this.dedupCache.clear();
    this.rateLimiter.clear();
  }
}
