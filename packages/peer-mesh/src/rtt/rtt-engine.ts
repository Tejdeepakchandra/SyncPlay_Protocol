/**
 * RttEngine — measures round-trip time between connected peers.
 *
 * Implements the RttService contract (INTERFACE_CONTRACTS.md §3).
 * Follows PEER_MESH_RTT_ENGINE_LLD §7–9 and Phase 4 requirements.
 *
 * - Monotonic clock only (performance.now() or injected Clock).
 * - Matches PONG responses to pending PING probes.
 * - Handles missing, delayed, duplicated, and unexpected PONG responses.
 * - Enforces maxPendingProbes and probe timeouts.
 * - Invalidates pending probes and timers on peer generation change / disconnect.
 * - Maintains bounded sample history per peer.
 * - Exposes statistical metrics without affecting consensus or playback state.
 */
import {
  type PeerId,
  type SessionId,
  type ConnectionGeneration,
  type ProtocolMessage,
  MessageType,
  PROTOCOL_VERSION,
} from '@syncplay/protocol';

import type {
  Clock,
  RttSample,
  RttEstimate,
  RttEngineConfig,
  RttService,
} from './types.js';
import { DEFAULT_RTT_ENGINE_CONFIG } from './types.js';
import { RttHistory, isValidRtt } from './rtt-history.js';

interface PendingProbe {
  readonly probeId: string;
  readonly generation: ConnectionGeneration;
  readonly sentAt: number;
  readonly timeoutTimer: ReturnType<typeof setTimeout>;
}

interface PeerRttState {
  generation: ConnectionGeneration;
  scheduleTimer: ReturnType<typeof setTimeout> | null;
  readonly pendingProbes: Map<string, PendingProbe>;
  readonly history: RttHistory;
}

export type SendFunction = (peerId: PeerId, message: ProtocolMessage) => { ok: boolean; error?: string };

export class RttEngine implements RttService {
  private readonly localPeerId: PeerId;
  private readonly sessionId: SessionId;
  private readonly sender: SendFunction;
  private readonly config: RttEngineConfig;
  private readonly clock: Clock;
  private readonly peers = new Map<PeerId, PeerRttState>();
  private readonly sampleHandlers = new Set<(sample: RttSample) => void>();
  private probeCounter = 0;
  private disposed = false;

  constructor(
    localPeerId: PeerId,
    sessionId: SessionId,
    sender: SendFunction,
    config: Partial<RttEngineConfig> = {},
    clock: Clock = { now: () => performance.now() },
  ) {
    this.localPeerId = localPeerId;
    this.sessionId = sessionId;
    this.sender = sender;
    this.config = { ...DEFAULT_RTT_ENGINE_CONFIG, ...config };
    this.clock = clock;
  }

  /**
   * Start active RTT probing for a peer at the specified connection generation.
   *
   * If the peer was already active with an older generation, this cancels
   * all pending probes and resets the history for the new generation.
   */
  start(peerId: PeerId, generation: ConnectionGeneration): void {
    if (this.disposed) return;

    let state = this.peers.get(peerId);

    if (state) {
      if (state.generation !== generation) {
        // Peer reconnected with a new generation: cancel all pending probes and timers
        this.clearPeerTimersAndPending(state);
        state.generation = generation;
        state.history.resetForGeneration(generation);
      } else {
        // Already active with same generation; clear existing schedule to restart cleanly
        if (state.scheduleTimer) {
          clearTimeout(state.scheduleTimer);
          state.scheduleTimer = null;
        }
      }
    } else {
      state = {
        generation,
        scheduleTimer: null,
        pendingProbes: new Map(),
        history: new RttHistory(
          peerId,
          generation,
          this.config.maxHistorySamples,
          this.config.staleThresholdMs,
        ),
      };
      this.peers.set(peerId, state);
    }

    // Send initial probe immediately, then schedule recurring probes
    this.dispatchProbe(peerId);
  }

  /**
   * Stop probing and cancel all scheduled and timeout timers for a peer.
   * Retains the collected history until peer is explicitly removed or reconnected.
   */
  stop(peerId: PeerId): void {
    const state = this.peers.get(peerId);
    if (!state) return;
    this.clearPeerTimersAndPending(state);
  }

  /**
   * Remove a peer completely, stopping all timers and deleting its history.
   */
  removePeer(peerId: PeerId): void {
    const state = this.peers.get(peerId);
    if (!state) return;
    this.clearPeerTimersAndPending(state);
    this.peers.delete(peerId);
  }

  /**
   * Handle an inbound PONG response.
   *
   * Validates probe matching, connection generation, and measurement validity.
   * Silently ignores unexpected, delayed, duplicate, or stale-generation PONGs.
   */
  handlePong(fromPeerId: PeerId, payload: { probeId: string }): void {
    if (this.disposed) return;
    if (!payload || typeof payload.probeId !== 'string') return;

    const state = this.peers.get(fromPeerId);
    if (!state) return;

    const pending = state.pendingProbes.get(payload.probeId);
    if (!pending) {
      // Unexpected, already timed-out, or duplicate PONG: ignore safely
      return;
    }

    // Clear timeout timer and remove from pending
    clearTimeout(pending.timeoutTimer);
    state.pendingProbes.delete(payload.probeId);

    // If generation does not match current state, reject as stale
    if (pending.generation !== state.generation) {
      return;
    }

    const now = this.clock.now();
    const rttMs = now - pending.sentAt;

    // Reject negative or invalid RTT (never clamp with Math.max)
    if (!isValidRtt(rttMs)) {
      return;
    }

    const sample: RttSample = {
      peerId: fromPeerId,
      probeId: payload.probeId,
      generation: pending.generation,
      rttMs,
      timestamp: now,
    };

    const added = state.history.addSample(sample);
    if (added) {
      this.emitSample(sample);
    }
  }

  /**
   * Retrieve current RTT statistics and estimate for a peer.
   * Returns undefined if peer is unknown or has no valid measurements.
   */
  getEstimate(peerId: PeerId): RttEstimate | undefined {
    const state = this.peers.get(peerId);
    if (!state) return undefined;
    return state.history.getEstimate(this.clock.now());
  }

  /**
   * Subscribe to new valid RTT samples.
   */
  onSample(handler: (sample: RttSample) => void): () => void {
    this.sampleHandlers.add(handler);
    return () => {
      this.sampleHandlers.delete(handler);
    };
  }

  /**
   * Clean up all timers, pending probes, and resources.
   */
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    for (const state of this.peers.values()) {
      this.clearPeerTimersAndPending(state);
    }
    this.peers.clear();
    this.sampleHandlers.clear();
  }

  // ── Internal Probe Lifecycle ─────────────────────────────────

  private dispatchProbe(peerId: PeerId): void {
    if (this.disposed) return;

    const state = this.peers.get(peerId);
    if (!state) return;

    // Enforce maxPendingProbes: if there is already an in-flight probe,
    // skip dispatching a new one to prevent queue buildup.
    if (state.pendingProbes.size < this.config.maxPendingProbes) {
      const probeId = this.generateProbeId();
      const sentAt = this.clock.now();
      const generation = state.generation;

      const timeoutTimer = setTimeout(() => {
        this.handleProbeTimeout(peerId, probeId);
      }, this.config.probeTimeoutMs);

      state.pendingProbes.set(probeId, {
        probeId,
        generation,
        sentAt,
        timeoutTimer,
      });

      state.history.recordProbeSent();

      const pingMessage: ProtocolMessage = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.PING,
        sessionId: this.sessionId,
        senderPeerId: this.localPeerId,
        messageId: this.generateMessageId(),
        payload: {
          probeId,
          senderTimestamp: sentAt,
        },
      };

      const result = this.sender(peerId, pingMessage);
      if (!result.ok) {
        // Failed to send (e.g. channel not open yet); clean up immediately
        clearTimeout(timeoutTimer);
        state.pendingProbes.delete(probeId);
      }
    }

    // Schedule next probe interval
    state.scheduleTimer = setTimeout(() => {
      this.dispatchProbe(peerId);
    }, this.config.probeIntervalMs);
  }

  private handleProbeTimeout(peerId: PeerId, probeId: string): void {
    const state = this.peers.get(peerId);
    if (!state) return;

    const pending = state.pendingProbes.get(probeId);
    if (!pending) return;

    state.pendingProbes.delete(probeId);
    state.history.recordProbeTimeout();
    // Note: Timed-out probe is never recorded as 0ms or added to sample history.
  }

  private clearPeerTimersAndPending(state: PeerRttState): void {
    if (state.scheduleTimer) {
      clearTimeout(state.scheduleTimer);
      state.scheduleTimer = null;
    }
    for (const pending of state.pendingProbes.values()) {
      clearTimeout(pending.timeoutTimer);
    }
    state.pendingProbes.clear();
  }

  private emitSample(sample: RttSample): void {
    for (const handler of this.sampleHandlers) {
      try {
        handler(sample);
      } catch {
        // Isolate subscriber errors
      }
    }
  }

  private generateProbeId(): string {
    this.probeCounter++;
    return `probe-${this.localPeerId}-${this.probeCounter}-${Date.now()}`;
  }

  private generateMessageId(): string {
    return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }
}
