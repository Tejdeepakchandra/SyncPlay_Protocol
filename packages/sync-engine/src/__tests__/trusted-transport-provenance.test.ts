/**
 * Phase 7.4 M3 — Final Trusted Transport Provenance Audit Tests.
 *
 * Verifies:
 * 1. A genuine packet from the authorized supplier via TrustedTransportAdapter is accepted.
 * 2. A payload claiming another supplier than the transport sender is rejected.
 * 3. A caller-supplied plain context is rejected.
 * 4. A registered context from the wrong session is rejected.
 * 5. A stale-generation packet is rejected after reconnect via TrustedTransportAdapter.
 * 6. Context reuse safety under documented packet-binding and request-correlation guarantee.
 * 7. An unauthenticated REPAIR_REJECTED cannot mutate request lifecycle state.
 * 8. Ordinary public SDK consumers cannot access or forge the registration capability.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  LeaderlessSyncEngine,
  createTransportRegistrationCapability,
  createEpochTransitionCapability,
  createAuthenticatedSupplierContext,
  TrustedTransportAdapter,
  type TransportRegistrationCapability,
  type EpochTransitionCapability,
} from '../engine/leaderless-sync-engine.js';
import {
  AntiEntropyManager,
  generateUUIDv4,
} from '../replication/index.js';
import type {
  SchedulerClock,
  AntiEntropyCallbacks,
} from '../replication/types.js';
import {
  type OperationBatchPayload,
  type RepairRejectedPayload,
  type SerializedControlOperation,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';

class DeterministicClock implements SchedulerClock {
  private currentTime = 1000;
  private nextTimerId = 1;
  private readonly timers = new Map<number, { callback: () => void; dueTime: number }>();

  public now(): number {
    return this.currentTime;
  }

  public setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextTimerId++;
    this.timers.set(id, {
      callback,
      dueTime: this.currentTime + Math.max(0, delayMs),
    });
    return id;
  }

  public clearTimeout(timerId: unknown): void {
    if (typeof timerId === 'number') {
      this.timers.delete(timerId);
    }
  }

  public advanceTime(ms: number): void {
    this.currentTime += ms;
    const due = Array.from(this.timers.entries())
      .filter(([_, t]) => t.dueTime <= this.currentTime)
      .sort((a, b) => a[1].dueTime - b[1].dueTime);

    for (const [id, timer] of due) {
      if (this.timers.has(id)) {
        this.timers.delete(id);
        timer.callback();
      }
    }
  }
}

describe('Phase 7.4 M3: Trusted Transport Provenance Audit', () => {
  let clock: DeterministicClock;
  let engine: LeaderlessSyncEngine;
  let coordinatorCap: EpochTransitionCapability;
  let transportCap: TransportRegistrationCapability;
  let transportAdapter: TrustedTransportAdapter;
  let antiEntropyManager: AntiEntropyManager;
  let sentRequests: Array<{ targetPeerId: string; request: any }>;
  let callbacks: AntiEntropyCallbacks;

  beforeEach(() => {
    clock = new DeterministicClock();
    sentRequests = [];

    coordinatorCap = createEpochTransitionCapability(
      'peer-coord',
      'test-session-1',
      'secret-coord-key',
    );
    transportCap = createTransportRegistrationCapability(
      'transport-driver',
      'test-session-1',
      'secret-transport-key',
    );

    engine = new LeaderlessSyncEngine({
      sessionId: 'test-session-1',
      localPeerId: 'peer-local',
      clock: { now: () => clock.now() },
      epochCapability: coordinatorCap,
      transportCapability: transportCap,
    });

    callbacks = {
      onSendDigest: vi.fn(),
      onSendRepairRequest: (targetPeerId, request) => {
        sentRequests.push({ targetPeerId, request });
      },
      onHistoryExhausted: vi.fn(),
    };

    antiEntropyManager = new AntiEntropyManager(
      engine,
      callbacks,
      { repairRequestTimeoutMs: 1500 },
      clock,
    );

    transportAdapter = new TrustedTransportAdapter(engine, transportCap, antiEntropyManager);
  });

  function makeSampleOp(
    origin: string,
    seq: number,
    logicalClock: number,
    causalContext: Record<string, number> = {},
    epoch = 1,
  ): SerializedControlOperation {
    return {
      logicalEventId: `evt-${origin}-${seq}-${epoch}`,
      originPeerId: origin,
      membershipEpoch: epoch,
      messageType: 'PLAY',
      logicalTimestamp: logicalClock,
      originSequenceNumber: seq,
      causalContext,
      payload: { positionMs: seq * 1000 },
    };
  }

  function makeSampleBatch(
    requestId: string,
    supplierPeerId: string,
    originPeerId: string,
    fromSeq: number,
    toSeq: number,
    epoch = 1,
  ): OperationBatchPayload {
    const operations: SerializedControlOperation[] = [];
    for (let s = fromSeq; s <= toSeq; s++) {
      const prevContext: Record<string, number> = s > 1 ? { [originPeerId]: s - 1 } : {};
      operations.push(makeSampleOp(originPeerId, s, s, prevContext, epoch));
    }
    return {
      requestId,
      sessionId: 'test-session-1',
      supplierPeerId,
      originPeerId,
      fromSequence: fromSeq,
      toSequence: toSeq,
      membershipEpoch: epoch,
      operations,
    };
  }

  it('1. A genuine packet from the authorized supplier via TrustedTransportAdapter is accepted', () => {
    // 1. Authorize peer-S at connection generation 1
    engine.authorizePeerGeneration('peer-S', 1);

    // 2. Peer-S advertises replication frontier 5
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    // 3. Reconcile gaps: request dispatched to peer-S
    expect(sentRequests.length).toBe(1);
    expect(sentRequests[0]!.targetPeerId).toBe('peer-S');
    const reqId = sentRequests[0]!.request.requestId;

    // 4. Inbound packet arrives over WebRTC DataChannel
    // The transport driver invokes TrustedTransportAdapter with transport-level peerId and generation:
    const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);
    const decisions = transportAdapter.handleInboundRepairBatch(batch, 'peer-S', 1);

    // 5. Verification: accepted through production pipeline
    expect(decisions.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(engine.logSize).toBe(5);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);
    expect(antiEntropyManager.getStatus().totalRepairsCompleted).toBe(1);

    antiEntropyManager.dispose();
  });

  it('2. A payload claiming another supplier than the transport sender is rejected', () => {
    engine.authorizePeerGeneration('peer-S1', 1);
    engine.authorizePeerGeneration('peer-S2', 1);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    expect(sentRequests.length).toBe(1);
    const reqId = sentRequests[0]!.request.requestId;

    // Packet arrives on physical DataChannel from peer-S1, but payload claims to be from peer-S2:
    const forgedBatch = makeSampleBatch(reqId, 'peer-S2', 'peer-author', 1, 5);
    const decisions = transportAdapter.handleInboundRepairBatch(forgedBatch, 'peer-S1', 1);

    expect(decisions.length).toBe(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toContain("supplierPeerId 'peer-S2' does not match requested supplier 'peer-S1'");

    // Engine replication frontier remains untouched
    expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    expect(engine.logSize).toBe(0);

    antiEntropyManager.dispose();
  });

  it('3. A caller-supplied plain context is rejected', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    const reqId = sentRequests[0]!.request.requestId;
    const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

    // Attacker fabricates plain context object without capability
    const plainContext = {
      supplierPeerId: 'peer-S',
      connectionGeneration: 1,
    };

    const decisions = antiEntropyManager.handleRepairBatch(batch, plainContext as any);

    expect(decisions.length).toBe(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toContain('missing or invalid authenticated supplier transport context');

    expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    expect(engine.logSize).toBe(0);

    antiEntropyManager.dispose();
  });

  it('4. A registered context from the wrong session is rejected', () => {
    // Engine A in session-1
    engine.authorizePeerGeneration('peer-S', 1);

    // Capability B and Engine B in session-2
    const capB = createTransportRegistrationCapability('transport-B', 'session-2', 'secret-B');
    const engineB = new LeaderlessSyncEngine({
      sessionId: 'session-2',
      localPeerId: 'peer-B',
      clock: { now: () => 1000 },
      transportCapability: capB,
    });

    // Context registered on engine B
    const contextFromSessionB = engineB.registerInboundSupplierContext('peer-S', 1, capB);

    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    const reqId = sentRequests[0]!.request.requestId;
    const batch = makeSampleBatch(reqId, 'peer-S', 'peer-author', 1, 5);

    // Ingest into engine A with context registered on engine B
    const decisions = antiEntropyManager.handleRepairBatch(batch, contextFromSessionB);

    expect(decisions.length).toBe(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');
    expect(decisions[0]!.message).toContain('missing or invalid authenticated supplier transport context');

    // Attempting to register context on engine A with capB throws UNAUTHORIZED
    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, capB);
    }).toThrow(SyncEngineError);

    antiEntropyManager.dispose();
  });

  it('5. A stale-generation packet is rejected after reconnect via TrustedTransportAdapter', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    const reqIdGen1 = sentRequests[0]!.request.requestId;

    // Peer reconnects: generation increments to 2
    engine.authorizePeerGeneration('peer-S', 2);

    // Stale batch arrives from generation 1 DataChannel
    const staleBatch = makeSampleBatch(reqIdGen1, 'peer-S', 'peer-author', 1, 5);
    const decisions = transportAdapter.handleInboundRepairBatch(staleBatch, 'peer-S', 1);

    expect(decisions.length).toBe(1);
    expect(decisions[0]!.accepted).toBe(false);
    expect(decisions[0]!.code).toBe('REJECTED_INVALID');

    // Frontier unmutated
    expect(engine.replicationFrontier['peer-author']).toBeUndefined();
    expect(engine.logSize).toBe(0);

    antiEntropyManager.dispose();
  });

  it('6. Context reuse safety under documented packet-binding and request-correlation guarantee', () => {
    engine.authorizePeerGeneration('peer-S', 1);
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 10 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });

    const reqId1 = sentRequests[0]!.request.requestId;
    const batch1 = makeSampleBatch(reqId1, 'peer-S', 'peer-author', 1, 5);

    // Mint a genuine context
    const genuineContext = transportAdapter.registerContext('peer-S', 1);

    // First use: fulfills reqId1
    const d1 = antiEntropyManager.handleRepairBatch(batch1, genuineContext);
    expect(d1.every((d) => d.accepted)).toBe(true);
    expect(engine.replicationFrontier['peer-author']).toBe(5);

    // Attempted reuse 1: replaying the same packet with the same context
    const dReplay = antiEntropyManager.handleRepairBatch(batch1, genuineContext);
    expect(dReplay.length).toBe(1);
    expect(dReplay[0]!.accepted).toBe(false);
    expect(dReplay[0]!.code).toBe('REJECTED_INVALID');
    expect(dReplay[0]!.message).toContain('not an active in-flight request');

    // Attempted reuse 2: injecting an unsolicited packet with random requestId
    const rogueBatch = makeSampleBatch(generateUUIDv4(), 'peer-S', 'peer-author', 6, 10);
    const dRogue = antiEntropyManager.handleRepairBatch(rogueBatch, genuineContext);
    expect(dRogue.length).toBe(1);
    expect(dRogue[0]!.accepted).toBe(false);
    expect(dRogue[0]!.code).toBe('REJECTED_INVALID');
    expect(dRogue[0]!.message).toContain('not an active in-flight request');

    // Engine frontier strictly preserved at 5
    expect(engine.replicationFrontier['peer-author']).toBe(5);
    expect(engine.logSize).toBe(5);

    antiEntropyManager.dispose();
  });

  it('7. An unauthenticated REPAIR_REJECTED cannot mutate request lifecycle state', () => {
    engine.authorizePeerGeneration('peer-S1', 1);
    engine.authorizePeerGeneration('peer-S2', 1);
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S1',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'a'.repeat(64) },
    });
    antiEntropyManager.processRemoteDigest({
      formatVersion: 2,
      sessionId: 'test-session-1',
      senderPeerId: 'peer-S2',
      connectionGeneration: 1,
      membershipEpoch: 1,
      stateVersion: 1,
      sentAt: 1000,
      replicationVector: { 'peer-author': 5 },
      snapshotFrontier: { 'peer-author': 0 },
      historyFingerprints: { 'peer-author': 'b'.repeat(64) },
    });

    expect(sentRequests.length).toBe(1);
    const reqId = sentRequests[0]!.request.requestId;
    const targetPeer = sentRequests[0]!.targetPeerId;

    const rejection: RepairRejectedPayload = {
      requestId: reqId,
      sessionId: 'test-session-1',
      supplierPeerId: targetPeer,
      targetOriginPeerId: 'peer-author',
      fromSequence: 1,
      toSequence: 5,
      reason: 'RATE_LIMITED',
    };

    // 1. Attacker attempts unauthenticated rejection (plain object context)
    const fakeContext = { supplierPeerId: targetPeer, connectionGeneration: 1 };
    antiEntropyManager.handleRepairRejected(rejection, fakeContext as any);

    // Request is still active; no failover triggered; retry count unmutated
    expect(sentRequests.length).toBe(1);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(1);
    expect(antiEntropyManager.getStatus().degraded).toBe(false);

    // 2. Mismatched peer rejection via transport adapter (packet arrived on peer-S2 channel)
    const wrongPeer = targetPeer === 'peer-S1' ? 'peer-S2' : 'peer-S1';
    transportAdapter.handleInboundRepairRejected(
      { ...rejection, supplierPeerId: wrongPeer },
      wrongPeer,
      1,
    );

    // Still dropped; request to targetPeer remains in flight
    expect(sentRequests.length).toBe(1);
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(1);

    // 3. Genuine transport rejection from targeted supplier
    transportAdapter.handleInboundRepairRejected(rejection, targetPeer, 1);

    // Backoff timer set, request removed from inFlight
    expect(antiEntropyManager.getStatus().activeRequestsCount).toBe(0);

    // Advance clock to fire backoff retry to second supplier
    clock.advanceTime(1000);
    expect(sentRequests.length).toBe(2);
    expect(sentRequests[1]!.targetPeerId).not.toBe(targetPeer);

    antiEntropyManager.dispose();
  });

  it('8. Ordinary public SDK consumers cannot access the registration capability', () => {
    // 1. Engine public API does not expose a getter or accessor for the capability
    expect((engine as any).transportCapability).toBeUndefined();
    expect((engine as any).getTransportCapability).toBeUndefined();
    expect((engine as any).getSupplierCapability).toBeUndefined();

    // 2. Separately created capability cannot register context
    const forgedCap = createTransportRegistrationCapability(
      'transport-driver',
      'test-session-1',
      'secret-transport-key',
    );
    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, forgedCap);
    }).toThrow(SyncEngineError);

    try {
      engine.registerInboundSupplierContext('peer-S', 1, forgedCap);
    } catch (err) {
      expect((err as SyncEngineError).code).toBe('UNAUTHORIZED');
      expect((err as SyncEngineError).message).toContain(
        'Supplier context registration requires authorized TransportRegistrationCapability',
      );
    }

    // 3. Cloned genuine capability is rejected by object-identity check
    const clonedCap = { ...transportCap };
    expect(() => {
      engine.registerInboundSupplierContext('peer-S', 1, clonedCap as any);
    }).toThrow(SyncEngineError);

    // 4. Calling helper function with forged capability throws UNAUTHORIZED
    expect(() => {
      createAuthenticatedSupplierContext(engine, 'peer-S', 1, forgedCap);
    }).toThrow(SyncEngineError);

    antiEntropyManager.dispose();
  });
});
