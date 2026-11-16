/**
 * Tests demonstrating and documenting arrival-order divergence under concurrent operations.
 *
 * Requirements:
 * "Include tests that demonstrate the known limitations rather than hiding them."
 * "Consider two peers issuing conflicting operations close together:
 *  - Peer A sends PLAY.
 *  - Peer B sends PAUSE.
 *  - Different peers may receive these events in different orders."
 */

import { describe, it, expect } from 'vitest';
import { MessageType } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import { BaselineSyncEngine } from '../baseline-sync-engine.js';

describe('BaselineSyncEngine — Documented Concurrency Limitations', () => {
  it('demonstrates arrival-order state divergence under concurrent PLAY and PAUSE', () => {
    let timeA = 1000;
    let timeB = 1000;

    const engineA = new BaselineSyncEngine({
      sessionId: 'room-mesh',
      localPeerId: 'peer-a',
      clock: { now: () => timeA },
    });

    const engineB = new BaselineSyncEngine({
      sessionId: 'room-mesh',
      localPeerId: 'peer-b',
      clock: { now: () => timeB },
    });

    // Initial state: both peers paused at 0ms
    expect(engineA.getState().status).toBe('paused');
    expect(engineB.getState().status).toBe('paused');

    // Physical Time T = 10ms: Peer A triggers PLAY
    timeA = 1010;
    const playFromA: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PLAY,
        sessionId: 'room-mesh',
        senderPeerId: 'peer-a',
        messageId: 'msg-play-a',
        logicalEventId: 'evt-play-a',
        payload: { targetPositionMs: 1000 },
      },
      localTimestamp: timeA,
      origin: 'local',
    } as AnyControlSyncEvent;

    // Peer A applies PLAY locally
    engineA.receive(playFromA);
    expect(engineA.getState().status).toBe('playing');

    // Physical Time T = 12ms: Peer B triggers PAUSE (simultaneous / concurrent)
    timeB = 1012;
    const pauseFromB: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.PAUSE,
        sessionId: 'room-mesh',
        senderPeerId: 'peer-b',
        messageId: 'msg-pause-b',
        logicalEventId: 'evt-pause-b',
        payload: {},
      },
      localTimestamp: timeB,
      origin: 'local',
    } as AnyControlSyncEvent;

    // Peer B applies PAUSE locally
    engineB.receive(pauseFromB);
    // Since Peer B was already paused, it remains paused
    expect(engineB.getState().status).toBe('paused');

    // --- Network Transit Window (e.g. 50ms latency) ---

    // At T = 60ms: Peer A receives PAUSE from Peer B over DataChannel (remote)
    timeA = 1060;
    const pauseReceivedAtA: AnyControlSyncEvent = {
      ...pauseFromB,
      origin: 'remote',
    };
    engineA.receive(pauseReceivedAtA);

    // At T = 62ms: Peer B receives PLAY from Peer A over DataChannel (remote)
    timeB = 1062;
    const playReceivedAtB: AnyControlSyncEvent = {
      ...playFromA,
      origin: 'remote',
    };
    engineB.receive(playReceivedAtB);

    // --- Formal Assertion of State Divergence ---
    // Peer A execution sequence: [PLAY, PAUSE] -> reaches PAUSED
    // Peer B execution sequence: [PAUSE, PLAY] -> reaches PLAYING
    const finalStateA = engineA.getState();
    const finalStateB = engineB.getState();

    expect(finalStateA.status).toBe('paused');
    expect(finalStateB.status).toBe('playing');

    // Proof that the two replicas diverge in the absence of a global consensus protocol:
    expect(finalStateA.status).not.toBe(finalStateB.status);
    expect(finalStateA.lastAppliedEvent?.logicalEventId).toBe('evt-pause-b');
    expect(finalStateB.lastAppliedEvent?.logicalEventId).toBe('evt-play-a');
  });

  it('demonstrates arrival-order state divergence under concurrent SEEK operations', () => {
    let timeA = 1000;
    let timeB = 1000;

    const engineA = new BaselineSyncEngine({
      sessionId: 'room-mesh',
      localPeerId: 'peer-a',
      clock: { now: () => timeA },
    });

    const engineB = new BaselineSyncEngine({
      sessionId: 'room-mesh',
      localPeerId: 'peer-b',
      clock: { now: () => timeB },
    });

    // Peer A seeks to 10000ms
    const seekA: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.SEEK,
        sessionId: 'room-mesh',
        senderPeerId: 'peer-a',
        messageId: 'msg-seek-a',
        logicalEventId: 'evt-seek-a',
        payload: { targetPositionMs: 10000 },
      },
      localTimestamp: 1010,
      origin: 'local',
    } as AnyControlSyncEvent;

    // Peer B seeks to 90000ms
    const seekB: AnyControlSyncEvent = {
      message: {
        protocolVersion: 1,
        messageType: MessageType.SEEK,
        sessionId: 'room-mesh',
        senderPeerId: 'peer-b',
        messageId: 'msg-seek-b',
        logicalEventId: 'evt-seek-b',
        payload: { targetPositionMs: 90000 },
      },
      localTimestamp: 1012,
      origin: 'local',
    } as AnyControlSyncEvent;

    // Peer A applies seekA, then receives seekB
    engineA.receive(seekA);
    engineA.receive({ ...seekB, origin: 'remote' });

    // Peer B applies seekB, then receives seekA
    engineB.receive(seekB);
    engineB.receive({ ...seekA, origin: 'remote' });

    // Peer A final position is 90000; Peer B final position is 10000
    expect(engineA.getState().anchorPositionMs).toBe(90000);
    expect(engineB.getState().anchorPositionMs).toBe(10000);

    // Documented position divergence:
    expect(engineA.getState().anchorPositionMs).not.toBe(engineB.getState().anchorPositionMs);
  });
});
