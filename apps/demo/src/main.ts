/**
 * SyncPlay WebRTC Peer Mesh Demo.
 *
 * Two peers (A = creator, B = joiner) connect through the Bootstrap Server,
 * negotiate WebRTC, open a DataChannel, and exchange PING/PONG messages.
 *
 * This demo is for verifying the transport layer only.
 * No playback state. No SyncEngine. No media.
 */
import { SignalingClient, PeerManager } from '@syncplay/peer-mesh';
import { PROTOCOL_VERSION, MessageType } from '@syncplay/protocol';
import type { PeerStatus, InboundMessage } from '@syncplay/peer-mesh';

// ── Logging ──────────────────────────────────────────────────

function log(panelId: string, text: string, cls: 'info' | 'success' | 'error' | 'msg' = 'info'): void {
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

// ── State ────────────────────────────────────────────────────

interface PeerState {
  signaling: SignalingClient | null;
  manager: PeerManager | null;
  roomId: string | null;
  peerId: string | null;
  token: string | null;
  pingCount: number;
}

const stateA: PeerState = { signaling: null, manager: null, roomId: null, peerId: null, token: null, pingCount: 0 };
const stateB: PeerState = { signaling: null, manager: null, roomId: null, peerId: null, token: null, pingCount: 0 };

function generateId(): string {
  return Math.random().toString(36).slice(2, 10);
}

// ── Peer A: Create Room ──────────────────────────────────────

async function createRoom(): Promise<void> {
  const serverUrl = (document.getElementById('server-url-a') as HTMLInputElement).value;
  log('a', `Connecting to ${serverUrl}...`);

  try {
    // Create room via HTTP
    const res = await fetch(`${serverUrl}/api/v1/rooms`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ maxPeers: 4 }),
    });

    if (!res.ok) {
      const err = await res.json();
      log('a', `Failed: ${JSON.stringify(err)}`, 'error');
      return;
    }

    const data = await res.json();
    stateA.roomId = data.roomId;
    stateA.peerId = data.peerId;
    stateA.token = data.membershipToken;

    log('a', `Room created: ${data.roomId}`, 'success');
    log('a', `Peer ID: ${data.peerId}`);
    log('a', `Token: ${data.membershipToken}`);

    // Pre-fill Peer B's fields
    (document.getElementById('room-id-b') as HTMLInputElement).value = data.roomId;
    (document.getElementById('token-b') as HTMLInputElement).value = data.membershipToken;

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
  const roomId = (document.getElementById('room-id-b') as HTMLInputElement).value;
  const token = (document.getElementById('token-b') as HTMLInputElement).value;

  if (!roomId || !token) {
    log('b', 'Room ID and token required', 'error');
    return;
  }

  log('b', `Joining room ${roomId}...`);

  try {
    let peerId: string;
    let peerToken = token;

    // If reconnecting an existing peer, reuse its peerId and token so the server increments generation
    if (stateB.peerId && stateB.token && stateB.roomId === roomId) {
      peerId = stateB.peerId;
      peerToken = stateB.token;
      log('b', `Reconnecting as existing peer ${peerId}...`);
    } else {
      // First time join via HTTP to get peerId and member token
      const res = await fetch(`${serverUrl}/api/v1/rooms/${roomId}/join`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ membershipToken: token }),
      });

      if (res.ok) {
        const data = await res.json();
        peerId = data.peerId;
        if (data.membershipToken) {
          peerToken = data.membershipToken;
        }
      } else {
        peerId = `peer-${generateId()}`;
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

// ── Join via Socket.IO and setup PeerManager ─────────────────

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

  // Handle peer status changes
  state.manager.onPeerStatus((status: PeerStatus) => {
    log(panelId, `Peer ${status.peerId}: ${status.state} (DC: ${status.dataChannelState ?? 'N/A'})`, 
      status.state === 'CONNECTED' ? 'success' : 'info');
    
    if (status.state === 'CONNECTED' || status.dataChannelState === 'open') {
      setStatus(panelId, `Connected to ${status.peerId}`, 'connected');
      (document.getElementById(`btn-ping-${panelId}`) as HTMLButtonElement).disabled = false;
    } else if (status.state === 'CLOSED' || status.state === 'FAILED') {
      setStatus(panelId, `${status.state}: ${status.peerId}`, 'failed');
    }
  });

  // Handle RTT samples and statistics
  state.manager.getRttService().onSample((sample) => {
    const est = state.manager?.getRttService().getEstimate(sample.peerId);
    if (est) {
      log(
        panelId,
        `⏱ RTT [${sample.peerId}]: latest=${sample.rttMs.toFixed(1)}ms, min=${est.minRttMs.toFixed(1)}ms, max=${est.maxRttMs.toFixed(1)}ms, avg=${est.avgRttMs.toFixed(1)}ms, med=${est.medianRttMs.toFixed(1)}ms, p90=${est.p90RttMs.toFixed(1)}ms (n=${est.sampleCount}, gen=${est.generation})`,
        'success',
      );
    }
  });

  // Handle incoming application messages
  state.manager.onMessage((inbound: InboundMessage) => {
    log(panelId, `← Received ${inbound.message.messageType} from ${inbound.fromPeerId}`, 'msg');
  });

  // Handle peer joined events — connect to new peers
  state.signaling.onPeerJoined((event) => {
    log(panelId, `Peer joined: ${event.peerId} (gen: ${event.generation})`);
    if (event.peerId !== state.peerId && state.manager) {
      state.manager.connect({
        peerId: event.peerId,
        sessionId: state.roomId!,
        generation: event.generation,
      }).then(() => {
        log(panelId, `Initiated connection to ${event.peerId}`, 'info');
      }).catch((err) => {
        log(panelId, `Connection failed: ${err}`, 'error');
      });
    }
  });

  // If peers already in room from the ack snapshot, connect to them
  const peers = (ack as any).snapshot?.peers as Array<{ peerId: string; generation?: number }> | undefined
    ?? ack.data?.['peers'] as Array<{ peerId: string; generation?: number }> | undefined;
  if (peers) {
    for (const peer of peers) {
      if (peer.peerId !== state.peerId) {
        log(panelId, `Existing peer found: ${peer.peerId}`);
        state.manager.connect({
          peerId: peer.peerId,
          sessionId: state.roomId,
          generation: peer.generation ?? 1,
        }).then(() => {
          log(panelId, `Initiated connection to ${peer.peerId}`, 'info');
        }).catch((err) => {
          log(panelId, `Connection failed: ${err}`, 'error');
        });
      }
    }
  }

  setStatus(panelId, 'In room, waiting for peer...', 'signaling');
  (document.getElementById(`btn-disconnect-${panelId}`) as HTMLButtonElement).disabled = false;
}

// ── Send PING / Check RTT ────────────────────────────────────

function sendPing(panelId: string, state: PeerState): void {
  if (!state.manager) return;

  const peers = state.manager.listPeers();
  if (peers.length === 0) {
    log(panelId, 'No peers connected', 'error');
    return;
  }

  for (const peer of peers) {
    const est = state.manager.getRttService().getEstimate(peer.peerId);
    if (est) {
      log(
        panelId,
        `📊 Current RTT [${peer.peerId}]: latest=${est.latestRttMs.toFixed(1)}ms, min=${est.minRttMs.toFixed(1)}ms, max=${est.maxRttMs.toFixed(1)}ms, avg=${est.avgRttMs.toFixed(1)}ms, med=${est.medianRttMs.toFixed(1)}ms, p90=${est.p90RttMs.toFixed(1)}ms (n=${est.sampleCount}, gen=${est.generation}, stale=${est.isStale})`,
        'info',
      );
    } else {
      log(panelId, `📊 Current RTT [${peer.peerId}]: collecting samples...`, 'info');
    }
  }
}

// ── Disconnect ───────────────────────────────────────────────

async function disconnectPeer(panelId: string, state: PeerState): Promise<void> {
  if (state.manager) {
    await state.manager.dispose();
    state.manager = null;
  }
  if (state.signaling) {
    state.signaling.dispose();
    state.signaling = null;
  }
  setStatus(panelId, 'Disconnected', 'new');
  log(panelId, 'Disconnected', 'info');
  (document.getElementById(`btn-ping-${panelId}`) as HTMLButtonElement).disabled = true;
  (document.getElementById(`btn-disconnect-${panelId}`) as HTMLButtonElement).disabled = true;

  if (panelId === 'a') {
    (document.getElementById('btn-create-a') as HTMLButtonElement).disabled = false;
  } else {
    (document.getElementById('btn-join-b') as HTMLButtonElement).disabled = false;
  }
}

// ── Wire up buttons ──────────────────────────────────────────

document.getElementById('btn-create-a')?.addEventListener('click', () => createRoom());
document.getElementById('btn-join-b')?.addEventListener('click', () => joinRoom());
document.getElementById('btn-ping-a')?.addEventListener('click', () => sendPing('a', stateA));
document.getElementById('btn-ping-b')?.addEventListener('click', () => sendPing('b', stateB));
document.getElementById('btn-disconnect-a')?.addEventListener('click', () => disconnectPeer('a', stateA));
document.getElementById('btn-disconnect-b')?.addEventListener('click', () => disconnectPeer('b', stateB));

log('a', 'Peer A ready. Click "Create Room" to start.');
log('b', 'Peer B ready. Paste Room ID and Token, then click "Join Room".');

