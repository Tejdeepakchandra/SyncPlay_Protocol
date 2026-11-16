/**
 * SyncPlay WebRTC Peer Mesh, Control Events & Baseline SyncEngine Demo.
 *
 * Demonstrates:
 * 1. WebRTC DataChannel full mesh via Bootstrap Server.
 * 2. PING/PONG and monotonic RTT measurement.
 * 3. Bidirectional control events (PLAY, PAUSE, SEEK, RATE_CHANGE, RETRY).
 * 4. Locally authoritative SyncEngine state reduction and continuous position extrapolation.
 */
import { SignalingClient, PeerManager } from '@syncplay/peer-mesh';
import type { PeerStatus, InboundMessage } from '@syncplay/peer-mesh';
import { PROTOCOL_VERSION, MessageType } from '@syncplay/protocol';
import { ControlEventServiceImpl } from '@syncplay/control-events';
import type {
  ControlEventService,
  SyncPlayControls,
  AnyControlSyncEvent,
} from '@syncplay/control-events';
import { BaselineSyncEngine } from '@syncplay/sync-engine';
import type { SyncEngine, PlaybackState } from '@syncplay/sync-engine';

// ── Logging & UI Helpers ─────────────────────────────────────

function log(panelId: string, text: string, cls: 'info' | 'success' | 'error' | 'msg' | 'engine' = 'info'): void {
  const el = document.getElementById(`log-${panelId}`);
  if (!el) return;
  const line = document.createElement('div');
  line.className = cls;
  const ts = new Date().toLocaleTimeString();
  line.textContent = `[${ts}] ${text}`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function setStatus(panelId: string, text: string, state: string = 'new'): void {
  const el = document.getElementById(`status-${panelId}`);
  if (!el) return;
  el.textContent = `Status: ${text}`;
  el.className = `status status-${state}`;
}

function setControlButtonsState(panelId: string, enabled: boolean): void {
  const ids = ['play', 'pause', 'seek', 'seek0', 'rate', 'retry', 'err', 'stats'];
  for (const id of ids) {
    const el = document.getElementById(`btn-${id}-${panelId}`) as HTMLButtonElement | null;
    if (el) el.disabled = !enabled;
  }
}

function formatTime(ms: number): string {
  const totalSeconds = Math.max(0, ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = (totalSeconds % 60).toFixed(1);
  return `${minutes.toString().padStart(2, '0')}:${seconds.padStart(4, '0')}`;
}

function renderEngineState(panelId: string, playbackState: PlaybackState): void {
  const badge = document.getElementById(`engine-status-badge-${panelId}`);
  if (badge) {
    badge.textContent = playbackState.status.toUpperCase();
    badge.className = playbackState.status === 'playing' ? 'badge badge-playing' : 'badge badge-paused';
  }

  const rateEl = document.getElementById(`engine-rate-${panelId}`);
  if (rateEl) rateEl.textContent = `${playbackState.playbackRate.toFixed(2)}x`;

  const verEl = document.getElementById(`engine-ver-${panelId}`);
  if (verEl) verEl.textContent = `v${playbackState.stateVersion}`;

  const anchorEl = document.getElementById(`engine-anchor-${panelId}`);
  if (anchorEl) anchorEl.textContent = `${playbackState.anchorPositionMs.toFixed(0)} ms`;

  const lastOpEl = document.getElementById(`engine-last-op-${panelId}`);
  if (lastOpEl) {
    if (playbackState.lastAppliedEvent) {
      const op = playbackState.lastAppliedEvent;
      lastOpEl.textContent = `${op.messageType} (${op.senderPeerId}) [${op.logicalEventId.slice(0, 8)}...]`;
    } else {
      lastOpEl.textContent = 'none (initial state)';
    }
  }
}

// ── State ────────────────────────────────────────────────────

export interface PeerState {
  signaling: SignalingClient | null;
  manager: PeerManager | null;
  controlService: ControlEventService | null;
  syncEngine: SyncEngine | null;
  controls: SyncPlayControls | null;
  roomId: string | null;
  peerId: string | null;
  token: string | null;
  pingCount: number;
  simulateReceiverError: boolean;
  lastOutboundEvent: AnyControlSyncEvent | null;
  lastReceivedEvent: AnyControlSyncEvent | null;
}

export const stateA: PeerState = {
  signaling: null,
  manager: null,
  controlService: null,
  syncEngine: null,
  controls: null,
  roomId: null,
  peerId: null,
  token: null,
  pingCount: 0,
  simulateReceiverError: false,
  lastOutboundEvent: null,
  lastReceivedEvent: null,
};

export const stateB: PeerState = {
  signaling: null,
  manager: null,
  controlService: null,
  syncEngine: null,
  controls: null,
  roomId: null,
  peerId: null,
  token: null,
  pingCount: 0,
  simulateReceiverError: false,
  lastOutboundEvent: null,
  lastReceivedEvent: null,
};

function generateId(): string {
  return Math.random().toString(36).slice(2, 10);
}

// ── Peer A: Create Room ──────────────────────────────────────

async function createRoom(): Promise<void> {
  const serverUrl = (document.getElementById('server-url-a') as HTMLInputElement).value;
  log('a', `Creating room at ${serverUrl}...`);

  try {
    const res = await fetch(`${serverUrl}/api/v1/rooms`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });

    if (!res.ok) {
      log('a', `Failed to create room: ${res.statusText}`, 'error');
      return;
    }

    const data = await res.json();
    stateA.roomId = data.roomId;
    stateA.peerId = data.peerId ?? data.creatorPeerId;
    stateA.token = data.membershipToken ?? data.creatorToken;

    log('a', `Room created: ${data.roomId}`, 'success');
    log('a', `Peer ID: ${stateA.peerId}`);

    // Auto-fill Peer B inputs for easy testing
    (document.getElementById('room-id-b') as HTMLInputElement).value = data.roomId;
    (document.getElementById('token-b') as HTMLInputElement).value = stateA.token ?? '';

    // Connect signaling
    stateA.signaling = new SignalingClient();
    stateA.signaling.connect(serverUrl);

    stateA.signaling.onConnect(() => {
      log('a', 'Socket connected');
      joinAndSetup('a', stateA);
    });

    stateA.signaling.onDisconnect((reason) => {
      log('a', `Socket disconnected: ${reason}`, 'error');
      setStatus('a', 'Disconnected', 'failed');
    });

    setStatus('a', 'Connecting...', 'signaling');
    (document.getElementById('btn-create-a') as HTMLButtonElement).disabled = true;
  } catch (err) {
    log('a', `Error: ${err}`, 'error');
  }
}

// ── Peer B: Join Room ────────────────────────────────────────

async function joinRoom(): Promise<void> {
  const serverUrl = (document.getElementById('server-url-b') as HTMLInputElement).value;
  const roomId = (document.getElementById('room-id-b') as HTMLInputElement).value.trim();
  const token = (document.getElementById('token-b') as HTMLInputElement).value.trim();

  if (!roomId) {
    log('b', 'Please enter a Room ID', 'error');
    return;
  }

  log('b', `Joining room ${roomId} at ${serverUrl}...`);

  try {
    // If stateB already has peerId (reconnect scenario), preserve it
    if (!stateB.peerId) {
      const joinRes = await fetch(`${serverUrl}/api/v1/rooms/${encodeURIComponent(roomId)}/join`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });

      let peerId = generateId();
      let peerToken = token;

      if (joinRes.ok) {
        const joinData = await joinRes.json();
        peerId = joinData.peerId;
        peerToken = joinData.membershipToken ?? joinData.peerToken ?? token;
        log('b', `Registered with bootstrap: peerId=${peerId}`);
      } else {
        log('b', 'Join API route not available, using client-generated peerId', 'info');
      }

      stateB.roomId = roomId;
      stateB.peerId = peerId;
      stateB.token = peerToken;
    }

    // Connect signaling
    stateB.signaling = new SignalingClient();
    stateB.signaling.connect(serverUrl);

    stateB.signaling.onConnect(() => {
      log('b', 'Socket connected');
      joinAndSetup('b', stateB);
    });

    stateB.signaling.onDisconnect((reason) => {
      log('b', `Socket disconnected: ${reason}`, 'error');
      setStatus('b', 'Disconnected', 'failed');
    });

    setStatus('b', 'Connecting...', 'signaling');
    (document.getElementById('btn-join-b') as HTMLButtonElement).disabled = true;
  } catch (err) {
    log('b', `Error: ${err}`, 'error');
  }
}

// ── Join via Socket.IO and setup PeerManager, ControlEvents & SyncEngine ─

async function joinAndSetup(panelId: string, state: PeerState): Promise<void> {
  if (!state.signaling || !state.roomId || !state.peerId || !state.token) return;

  log(panelId, `Joining room via Socket.IO as ${state.peerId}...`);

  const ack = await state.signaling.joinRoom(state.roomId, state.peerId, state.token);

  if (!ack.ok) {
    log(panelId, `Join failed: ${JSON.stringify(ack.error)}`, 'error');
    return;
  }

  log(panelId, 'Joined room via Socket.IO', 'success');

  // Get generation from ack snapshot or data
  const snapshotPeers = (ack as any).snapshot?.peers as Array<{ peerId: string; generation?: number }> | undefined;
  const myMember = snapshotPeers?.find((p) => p.peerId === state.peerId);
  const generation = myMember?.generation ?? (ack.data?.['generation'] as number) ?? 1;
  log(panelId, `Generation: ${generation}`);

  // Create PeerManager
  state.manager = new PeerManager(
    state.peerId,
    state.roomId,
    generation,
    state.signaling,
    {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
      ],
    },
  );

  state.manager.bindToRoom(state.roomId);

  // Initialize Phase 5 ControlEventService
  state.controlService = new ControlEventServiceImpl(state.manager, {
    sessionId: state.roomId,
    localPeerId: state.peerId,
  });
  state.controls = state.controlService.getControls();

  // Initialize Phase 6 BaselineSyncEngine
  state.syncEngine = new BaselineSyncEngine({
    sessionId: state.roomId,
    localPeerId: state.peerId,
  });

  // Connect ControlEventService to BaselineSyncEngine via SyncEventReceiver
  if (state.simulateReceiverError) {
    // Intercept receiver to simulate failure
    state.controlService.setReceiver({
      receive(event: AnyControlSyncEvent) {
        log(panelId, `💥 Simulated receiver failure on ${event.message.messageType}!`, 'error');
        throw new Error(`Simulated receiver failure on ${event.message.messageType}`);
      },
    });
  } else {
    state.controlService.setReceiver(state.syncEngine);
  }

  // Subscribe UI to SyncEngine state mutations
  state.syncEngine.onStateChange((playbackState) => {
    renderEngineState(panelId, playbackState);
    log(
      panelId,
      `🎬 State Mutated [v${playbackState.stateVersion}]: ${playbackState.status.toUpperCase()} at ${playbackState.anchorPositionMs.toFixed(0)}ms (${playbackState.playbackRate}x)`,
      'engine',
    );
  });

  // Subscribe UI to SyncEngine decision logging
  state.syncEngine.onDecision((event, decision) => {
    const originTag = event.origin === 'local' ? 'LOCAL' : 'REMOTE';
    log(
      panelId,
      `🎯 Decision [${decision.code}] on ${event.message.messageType} (${originTag}, from=${event.message.senderPeerId}) -> accepted=${decision.accepted}, v${decision.stateVersion}`,
      decision.accepted ? 'success' : 'error',
    );
    state.lastReceivedEvent = event;
  });

  // Render initial clean state
  renderEngineState(panelId, state.syncEngine.getState());

  // Handle peer status changes
  state.manager.onPeerStatus((status: PeerStatus) => {
    log(
      panelId,
      `Peer ${status.peerId}: ${status.state} (DC: ${status.dataChannelState ?? 'N/A'})`,
      status.state === 'CONNECTED' ? 'success' : 'info',
    );

    if (status.state === 'CONNECTED' || status.dataChannelState === 'open') {
      setStatus(panelId, `Connected to ${status.peerId}`, 'connected');
      (document.getElementById(`btn-ping-${panelId}`) as HTMLButtonElement).disabled = false;
      (document.getElementById(`btn-disconnect-${panelId}`) as HTMLButtonElement).disabled = false;
      setControlButtonsState(panelId, true);
    } else if (status.state === 'CLOSED' || status.state === 'FAILED') {
      setStatus(panelId, `${status.state}: ${status.peerId}`, 'failed');
      setControlButtonsState(panelId, false);
    }
  });

  // Handle RTT samples
  state.manager.getRttService().onSample((sample) => {
    const est = state.manager?.getRttService().getEstimate(sample.peerId);
    if (est) {
      log(
        panelId,
        `⏱ RTT [${sample.peerId}]: latest=${sample.rttMs.toFixed(1)}ms, avg=${est.avgRttMs.toFixed(1)}ms (n=${est.sampleCount})`,
        'success',
      );
    }
  });

  // Handle peer joined events
  state.signaling.onPeerJoined((event) => {
    log(panelId, `Peer joined: ${event.peerId} (gen: ${event.generation})`);
    if (event.peerId !== state.peerId && state.manager) {
      state.manager
        .connect({
          peerId: event.peerId,
          sessionId: state.roomId!,
          generation: event.generation,
        })
        .then(() => {
          log(panelId, `Initiated connection to ${event.peerId}`, 'info');
        })
        .catch((err) => {
          log(panelId, `Connection failed: ${err}`, 'error');
        });
    }
  });

  // If peers already in room from snapshot, connect to them
  const peers =
    ((ack as any).snapshot?.peers as Array<{ peerId: string; generation?: number }> | undefined) ??
    (ack.data?.['peers'] as Array<{ peerId: string; generation?: number }> | undefined);
  if (peers) {
    for (const peer of peers) {
      if (peer.peerId !== state.peerId) {
        log(panelId, `Existing peer found: ${peer.peerId}`);
        state.manager
          .connect({
            peerId: peer.peerId,
            sessionId: state.roomId,
            generation: peer.generation ?? 1,
          })
          .then(() => {
            log(panelId, `Initiated connection to existing peer ${peer.peerId}`, 'info');
          })
          .catch((err) => {
            log(panelId, `Connection to existing peer failed: ${err}`, 'error');
          });
      }
    }
  }

  // Handle messages from peers
  state.manager.onMessage((inbound: InboundMessage) => {
    log(
      panelId,
      `📩 DataChannel message from ${inbound.fromPeerId}: type=${inbound.message.messageType}`,
      'msg',
    );
  });
}

// ── Control Operations ───────────────────────────────────────

async function handlePlay(panelId: string, state: PeerState, targetPositionMs?: number): Promise<void> {
  if (!state.controls) return;
  const posLabel = targetPositionMs !== undefined ? `(${targetPositionMs}ms)` : '';
  const res = await state.controls.play(targetPositionMs);
  log(
    panelId,
    `▶ Play${posLabel} -> accepted=${res.accepted}, peers=${res.deliveredCount}/${res.peerCount}, evtId=${res.logicalEventId?.slice(0, 8)}...${res.error ? ', err=' + res.error.message : ''}`,
    res.accepted ? 'success' : 'error',
  );
  if (res.accepted && state.lastReceivedEvent) {
    state.lastOutboundEvent = state.lastReceivedEvent;
  }
}

async function handlePause(panelId: string, state: PeerState): Promise<void> {
  if (!state.controls) return;
  const res = await state.controls.pause();
  log(
    panelId,
    `⏸ Pause -> accepted=${res.accepted}, peers=${res.deliveredCount}/${res.peerCount}, evtId=${res.logicalEventId?.slice(0, 8)}...${res.error ? ', err=' + res.error.message : ''}`,
    res.accepted ? 'success' : 'error',
  );
  if (res.accepted && state.lastReceivedEvent) {
    state.lastOutboundEvent = state.lastReceivedEvent;
  }
}

async function handleSeek(panelId: string, state: PeerState, targetPositionMs: number = 15000): Promise<void> {
  if (!state.controls) return;
  const res = await state.controls.seek(targetPositionMs);
  log(
    panelId,
    `⏩ Seek(${targetPositionMs}ms) -> accepted=${res.accepted}, peers=${res.deliveredCount}/${res.peerCount}, evtId=${res.logicalEventId?.slice(0, 8)}...${res.error ? ', err=' + res.error.message : ''}`,
    res.accepted ? 'success' : 'error',
  );
  if (res.accepted && state.lastReceivedEvent) {
    state.lastOutboundEvent = state.lastReceivedEvent;
  }
}

async function handleRate(panelId: string, state: PeerState, playbackRate: number = 1.5): Promise<void> {
  if (!state.controls) return;
  const res = await state.controls.setPlaybackRate(playbackRate);
  log(
    panelId,
    `⚡ Rate(${playbackRate}x) -> accepted=${res.accepted}, peers=${res.deliveredCount}/${res.peerCount}, evtId=${res.logicalEventId?.slice(0, 8)}...${res.error ? ', err=' + res.error.message : ''}`,
    res.accepted ? 'success' : 'error',
  );
  if (res.accepted && state.lastReceivedEvent) {
    state.lastOutboundEvent = state.lastReceivedEvent;
  }
}

async function handleRetry(panelId: string, state: PeerState): Promise<void> {
  if (!state.controls) return;
  if (!state.lastOutboundEvent) {
    log(panelId, 'No previous outbound event to retry', 'error');
    return;
  }
  const res = await state.controls.retry(state.lastOutboundEvent);
  log(
    panelId,
    `🔁 Retry -> accepted=${res.accepted}, peers=${res.deliveredCount}/${res.peerCount}, evtId=${res.logicalEventId?.slice(0, 8)}...${res.error ? ', err=' + res.error.message : ''}`,
    res.accepted ? 'success' : 'error',
  );
}

function handleToggleError(panelId: string, state: PeerState): void {
  state.simulateReceiverError = !state.simulateReceiverError;
  const btn = document.getElementById(`btn-err-${panelId}`);
  if (btn) btn.textContent = state.simulateReceiverError ? 'Sim Error: ON' : 'Sim Error: OFF';

  // Toggle receiver on ControlEventService
  if (state.controlService) {
    if (state.simulateReceiverError) {
      state.controlService.setReceiver({
        receive(event: AnyControlSyncEvent) {
          log(panelId, `💥 Simulated receiver failure on ${event.message.messageType}!`, 'error');
          throw new Error(`Simulated receiver failure on ${event.message.messageType}`);
        },
      });
    } else if (state.syncEngine) {
      state.controlService.setReceiver(state.syncEngine);
    }
  }

  log(
    panelId,
    `Simulate receiver failure: ${state.simulateReceiverError ? 'ON' : 'OFF'}`,
    state.simulateReceiverError ? 'error' : 'info',
  );
}

function handleStats(panelId: string, state: PeerState): void {
  if (state.controlService) {
    const s = state.controlService.getStats();
    log(
      panelId,
      `📊 Control Stats: inRecv=${s.inboundReceivedCount}, inDisp=${s.inboundDispatchedCount}, inDrop=${s.inboundDroppedCount}, outCreated=${s.outboundCreatedCount}, outDisp=${s.outboundDispatchedCount}, dedupSize=${s.deduplication.size}, dedupDups=${s.deduplication.duplicateCount}`,
      'info',
    );
  }
  if (state.syncEngine) {
    const es = state.syncEngine.getStats();
    log(
      panelId,
      `🎬 Engine Stats: recv=${es.eventsReceivedCount}, applied=${es.eventsAppliedCount}, noOp=${es.eventsNoOpCount}, rej=${es.eventsRejectedCount}, errs=${es.listenerErrorCount}`,
      'info',
    );
  }
}

// ── Disconnect ───────────────────────────────────────────────

async function disconnectPeer(panelId: string, state: PeerState): Promise<void> {
  if (state.syncEngine) {
    state.syncEngine.dispose();
    state.syncEngine = null;
  }
  if (state.controlService) {
    state.controlService.dispose();
    state.controlService = null;
    state.controls = null;
  }
  if (state.manager) {
    await state.manager.dispose();
    state.manager = null;
  }
  if (state.signaling) {
    state.signaling.dispose();
    state.signaling = null;
  }

  log(panelId, 'Disconnected and cleaned up resources');
  setStatus(panelId, 'Disconnected', 'failed');
  setControlButtonsState(panelId, false);

  (document.getElementById(`btn-ping-${panelId}`) as HTMLButtonElement).disabled = true;
  (document.getElementById(`btn-disconnect-${panelId}`) as HTMLButtonElement).disabled = true;

  if (panelId === 'a') {
    (document.getElementById('btn-create-a') as HTMLButtonElement).disabled = false;
  } else {
    (document.getElementById('btn-join-b') as HTMLButtonElement).disabled = false;
  }

  // Reset state box
  const badge = document.getElementById(`engine-status-badge-${panelId}`);
  if (badge) {
    badge.textContent = 'DISPOSED';
    badge.className = 'badge badge-paused';
  }
}

function sendPing(panelId: string, state: PeerState): void {
  if (!state.manager || !state.roomId || !state.peerId) return;
  state.pingCount++;
  const peers = state.manager.listPeers().filter((p) => p.dataChannelState === 'open');
  if (peers.length === 0) {
    log(panelId, 'No peers with open DataChannel', 'error');
    return;
  }
  const probeId = `probe-${panelId}-${state.pingCount}`;
  for (const peer of peers) {
    const msg = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.PING,
      sessionId: state.roomId,
      senderPeerId: state.peerId,
      messageId: `msg-${generateId()}`,
      payload: { probeId },
    };
    const res = state.manager.send(peer.peerId, msg);
    if (res.ok) {
      log(panelId, `→ PING sent to ${peer.peerId} (probeId: ${probeId})`, 'msg');
    } else {
      log(panelId, `PING failed: ${res.error}`, 'error');
    }
  }
}

// ── Continuous Playback Position Animation Loop ──────────────

setInterval(() => {
  const updatePos = (panelId: string, state: PeerState) => {
    if (state.syncEngine && !state.syncEngine.isDisposed) {
      const currentMs = state.syncEngine.getCurrentPositionMs();
      const posEl = document.getElementById(`engine-pos-${panelId}`);
      if (posEl) {
        posEl.textContent = `${currentMs.toFixed(0)} ms (${formatTime(currentMs)})`;
      }
    }
  };

  updatePos('a', stateA);
  updatePos('b', stateB);
}, 100);

// ── Bind Event Listeners ─────────────────────────────────────

document.getElementById('btn-create-a')?.addEventListener('click', createRoom);
document.getElementById('btn-join-b')?.addEventListener('click', joinRoom);

document.getElementById('btn-ping-a')?.addEventListener('click', () => sendPing('a', stateA));
document.getElementById('btn-ping-b')?.addEventListener('click', () => sendPing('b', stateB));

document.getElementById('btn-disconnect-a')?.addEventListener('click', () => disconnectPeer('a', stateA));
document.getElementById('btn-disconnect-b')?.addEventListener('click', () => disconnectPeer('b', stateB));

// Phase 6 Controls
document.getElementById('btn-play-a')?.addEventListener('click', () => handlePlay('a', stateA));
document.getElementById('btn-play-b')?.addEventListener('click', () => handlePlay('b', stateB));

document.getElementById('btn-pause-a')?.addEventListener('click', () => handlePause('a', stateA));
document.getElementById('btn-pause-b')?.addEventListener('click', () => handlePause('b', stateB));

document.getElementById('btn-seek-a')?.addEventListener('click', () => handleSeek('a', stateA, 15000));
document.getElementById('btn-seek-b')?.addEventListener('click', () => handleSeek('b', stateB, 15000));

document.getElementById('btn-seek0-a')?.addEventListener('click', () => handleSeek('a', stateA, 0));
document.getElementById('btn-seek0-b')?.addEventListener('click', () => handleSeek('b', stateB, 0));

document.getElementById('btn-rate-a')?.addEventListener('click', () => {
  const cur = stateA.syncEngine?.getState().playbackRate ?? 1.0;
  const next = cur === 1.0 ? 1.5 : cur === 1.5 ? 2.0 : 1.0;
  handleRate('a', stateA, next);
});
document.getElementById('btn-rate-b')?.addEventListener('click', () => {
  const cur = stateB.syncEngine?.getState().playbackRate ?? 1.0;
  const next = cur === 1.0 ? 1.5 : cur === 1.5 ? 2.0 : 1.0;
  handleRate('b', stateB, next);
});

document.getElementById('btn-retry-a')?.addEventListener('click', () => handleRetry('a', stateA));
document.getElementById('btn-retry-b')?.addEventListener('click', () => handleRetry('b', stateB));

document.getElementById('btn-err-a')?.addEventListener('click', () => handleToggleError('a', stateA));
document.getElementById('btn-err-b')?.addEventListener('click', () => handleToggleError('b', stateB));

document.getElementById('btn-stats-a')?.addEventListener('click', () => handleStats('a', stateA));
document.getElementById('btn-stats-b')?.addEventListener('click', () => handleStats('b', stateB));

// ── Export for Browser Verification / CDP ────────────────────

(window as any).syncplayDemo = {
  stateA,
  stateB,
  createRoom,
  joinRoom,
  disconnectPeer,
  handlePlay,
  handlePause,
  handleSeek,
  handleRate,
  handleRetry,
  sendPing,
};
