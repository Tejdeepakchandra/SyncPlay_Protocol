/**
 * LeaderlessSyncEngine implementation.
 *
 * Implements Phase 7 specifications:
 * - Public SyncEngine and SyncEventReceiver contract compatibility
 * - Exact Lamport clock management (ILogicalClockManager)
 * - Deterministic total-order operation log (IOrderedOperationLog)
 * - Missing-dependency buffering and repair trigger (PendingDependencyQueue)
 * - Deterministic conflict resolution (IConflictResolver)
 * - Safe rollback and replay across retained operations
 * - Component-wise causal frontier merging and receiver observation isolation
 */

import type { StateSnapshotPayload } from '@syncplay/protocol';
import type { AnyControlSyncEvent } from '@syncplay/control-events';
import type {
  SyncEngine,
  PlaybackState,
  EngineDecision,
  EngineStatus,
  EngineStats,
  Clock,
  SyncEngineConfig,
  StateChangeListener,
  DecisionListener,
  Unsubscribe,
} from '../types.js';
import { SyncEngineError } from '../errors.js';
import { validateSyncEngineConfig, type ResolvedSyncEngineConfig } from '../config.js';
import { calculateExtrapolatedPosition } from '../reducer.js';
import { exportEngineSnapshot, validateSnapshotPayload } from '../snapshot.js';
import { LogicalClockManager } from '../clock/logical-clock-manager.js';
import { OrderedOperationLog } from '../log/ordered-operation-log.js';
import { PendingDependencyQueue } from '../log/pending-dependency-queue.js';
import { ConflictResolver } from '../conflict/conflict-resolver.js';
import { computeOperationFingerprint } from '../log/operation-fingerprint.js';
import type { StoredOperation } from '../log/types.js';
import type { ReducerContext } from '../conflict/types.js';
import {
  type OperationBatchPayload,
  type StateSnapshotPayloadV2,
  type StateDigestPayload,
  StateDigestPayloadSchema,
} from '@syncplay/protocol';
import { compareDigests } from '../replication/replication-comparator.js';
import type { DigestComparisonResult } from '../replication/types.js';

interface MutableStats {
  eventsReceivedCount: number;
  eventsAppliedCount: number;
  eventsNoOpCount: number;
  eventsRejectedCount: number;
  stateChangeNotificationsCount: number;
  decisionNotificationsCount: number;
  listenerErrorCount: number;
}

export interface EpochTransitionCapability {
  readonly authority: string;
  readonly sessionId: string;
  readonly token: string;
}

/**
 * Creates an authorized EpochTransitionCapability for trusted coordinator initialization.
 */
export function createEpochTransitionCapability(
  authority: string,
  sessionId: string,
  token?: string,
): EpochTransitionCapability {
  return Object.freeze({
    authority,
    sessionId,
    token:
      token ??
      `epoch-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
}

/**
 * Authenticated transport supplier metadata supplied by the trusted transport layer.
 */
export interface AuthenticatedSupplierContext {
  readonly supplierPeerId: string;
  readonly connectionGeneration?: number;
}

export interface TransportRegistrationCapability {
  readonly authority: string;
  readonly sessionId: string;
  readonly token: string;
}

/**
 * Creates an authorized TransportRegistrationCapability for trusted transport adapter initialization.
 */
export function createTransportRegistrationCapability(
  authority: string,
  sessionId: string,
  token?: string,
): TransportRegistrationCapability {
  return Object.freeze({
    authority,
    sessionId,
    token:
      token ??
      `transport-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
}

export type SupplierContextRegistrationCapability = TransportRegistrationCapability;
export const createSupplierContextRegistrationCapability = createTransportRegistrationCapability;

/**
 * Creates an authenticated supplier transport context registered with the target engine.
 * Requires the authorized TransportRegistrationCapability held by the trusted transport adapter.
 */
export function createAuthenticatedSupplierContext(
  engine: LeaderlessSyncEngine,
  supplierPeerId: string,
  connectionGeneration: number,
  capability: TransportRegistrationCapability,
): AuthenticatedSupplierContext {
  return engine.registerInboundSupplierContext(supplierPeerId, connectionGeneration, capability);
}

/**
 * Trusted transport adapter that wraps the engine and holds the transport registration capability.
 * Created by the composition root and provided to transport drivers (PeerMesh, ControlEventService).
 */
export class TrustedTransportAdapter {
  constructor(
    private readonly engine: LeaderlessSyncEngine,
    private readonly capability: TransportRegistrationCapability,
  ) {
    if (!engine) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'TrustedTransportAdapter requires an engine instance');
    }
    if (!capability) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        'TrustedTransportAdapter requires an authorized TransportRegistrationCapability',
      );
    }
  }

  public registerContext(
    supplierPeerId: string,
    connectionGeneration: number,
  ): AuthenticatedSupplierContext {
    return this.engine.registerInboundSupplierContext(
      supplierPeerId,
      connectionGeneration,
      this.capability,
    );
  }

  public ingestRepairBatch(
    batch: OperationBatchPayload,
    supplierPeerId: string,
    connectionGeneration: number,
  ): EngineDecision[] {
    const context = this.registerContext(supplierPeerId, connectionGeneration);
    return this.engine.ingestRepairBatch(batch, context);
  }
}

export interface LeaderlessSyncEngineConfig extends SyncEngineConfig {
  readonly initialClock?: number;
  readonly logCapacity?: number;
  readonly epochCapability?: EpochTransitionCapability;
  readonly transportCapability?: TransportRegistrationCapability;
  readonly supplierCapability?: TransportRegistrationCapability;
}

export class LeaderlessSyncEngine implements SyncEngine {
  private readonly config: ResolvedSyncEngineConfig;
  private readonly clock: Clock;

  private state: PlaybackState;
  private baseSnapshot: PlaybackState;
  private baseSnapshotFrontier: Record<string, number> = {};
  private lastDecision?: EngineDecision;
  private disposed = false;

  private readonly clockManager: LogicalClockManager;
  private readonly operationLog: OrderedOperationLog;
  private readonly pendingQueue: PendingDependencyQueue;
  private readonly conflictResolver: ConflictResolver;

  // Causal vectors
  private readonly localCausalContext: Record<string, number> = {};
  private readonly replicationVector: Record<string, number> = {};
  private readonly peerGenerations = new Map<string, number>();
  private readonly authorizedEpochCapability?: EpochTransitionCapability;
  private readonly authorizedTransportCapability?: TransportRegistrationCapability;
  private readonly validSupplierContexts = new WeakSet<AuthenticatedSupplierContext>();
  private localOriginSequence = 0;
  private currentEpoch = 1;

  private readonly stateChangeListeners = new Set<StateChangeListener>();
  private readonly decisionListeners = new Set<DecisionListener>();

  private readonly mutableStats: MutableStats = {
    eventsReceivedCount: 0,
    eventsAppliedCount: 0,
    eventsNoOpCount: 0,
    eventsRejectedCount: 0,
    stateChangeNotificationsCount: 0,
    decisionNotificationsCount: 0,
    listenerErrorCount: 0,
  };

  constructor(config: LeaderlessSyncEngineConfig) {
    this.config = validateSyncEngineConfig(config);
    this.clock = config.clock ?? { now: () => performance.now() };

    const initialNow = this.clock.now();
    if (typeof initialNow !== 'number' || !Number.isFinite(initialNow) || initialNow < 0) {
      throw new SyncEngineError(
        'CLOCK_ERROR',
        `Injected clock returned invalid initial timestamp: ${initialNow}`,
      );
    }

    const initialState = Object.freeze<PlaybackState>({
      status: 'paused',
      anchorPositionMs: 0,
      anchorLocalTimestamp: initialNow,
      playbackRate: 1.0,
      stateVersion: 0,
    });

    this.state = initialState;
    this.baseSnapshot = initialState;

    this.authorizedEpochCapability = config.epochCapability
      ? Object.freeze(config.epochCapability)
      : undefined;

    const transportCap = config.transportCapability ?? config.supplierCapability;
    this.authorizedTransportCapability = transportCap
      ? Object.freeze(transportCap)
      : undefined;

    const logCapacity = config.logCapacity ?? 256;
    this.clockManager = new LogicalClockManager(config.initialClock ?? 0);
    const reducerFn = (state: PlaybackState, op: StoredOperation): PlaybackState => {
      const reducerContext = this.getReducerContext();
      return this.conflictResolver.reduce(state, op, this.clock.now(), reducerContext).nextState;
    };
    this.operationLog = new OrderedOperationLog(logCapacity, initialState, reducerFn);
    this.pendingQueue = new PendingDependencyQueue(logCapacity);
    this.conflictResolver = new ConflictResolver();
  }

  public get isDisposed(): boolean {
    return this.disposed;
  }

  public get logicalClock(): number {
    return this.clockManager.clock;
  }

  public get localFrontier(): Readonly<Record<string, number>> {
    return { ...this.localCausalContext };
  }

  public get replicationFrontier(): Readonly<Record<string, number>> {
    return { ...this.replicationVector };
  }

  public get pendingQueueSize(): number {
    return this.pendingQueue.size;
  }

  public get logSize(): number {
    return this.operationLog.size;
  }

  public get epoch(): number {
    return this.currentEpoch;
  }

  /**
   * Authoritatively transitions the engine to a new membership epoch.
   *
   * Authorization:
   * Requires possession of the authorized EpochTransitionCapability that was injected
   * during trusted engine initialization. An arbitrary caller without the capability
   * or attempting to use a raw string token is rejected with SyncEngineError('UNAUTHORIZED').
   *
   * State Transition Invariants:
   * 1. currentEpoch: advances strictly forward to newEpoch.
   * 2. pendingQueue: any buffered pending operations with membershipEpoch < newEpoch are purged
   *    so obsolete-epoch operations cannot leak into the new epoch.
   * 3. orderedOperationLog & baseSnapshot: preserved; valid state remains authoritative.
   * 4. localCausalContext & replicationVector & Lamport clock: preserved monotonically.
   * 5. peerGenerations: preserved across epoch boundary.
   */
  public transitionEpoch(
    newEpoch: number,
    capability: EpochTransitionCapability,
  ): void {
    if (
      !this.authorizedEpochCapability ||
      !capability ||
      capability !== this.authorizedEpochCapability
    ) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        'Epoch transition requires authorized EpochTransitionCapability',
      );
    }

    if (
      typeof newEpoch !== 'number' ||
      !Number.isInteger(newEpoch) ||
      newEpoch <= this.currentEpoch
    ) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid epoch transition: newEpoch ${newEpoch} must be integer > currentEpoch ${this.currentEpoch}`,
      );
    }
    this.currentEpoch = newEpoch;
    // Purge obsolete-epoch operations from pending dependency queue
    this.pendingQueue.pruneObsoleteEpoch(newEpoch);
  }

  /**
   * Authoritative lifecycle registration for a peer's connection generation.
   * Informs the engine of the trusted local peer connection manager's authorized generation.
   * Incoming events cannot unilaterally advance or set this generation.
   */
  public setPeerGeneration(peerId: string, generation: number): void {
    this.authorizePeerGeneration(peerId, generation);
  }

  public authorizePeerGeneration(peerId: string, generation: number): void {
    if (typeof generation !== 'number' || !Number.isInteger(generation) || generation < 1) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid connection generation ${generation}: must be positive integer`,
      );
    }
    if (this.peerGenerations.size >= 64 && !this.peerGenerations.has(peerId)) {
      const firstKey = this.peerGenerations.keys().next().value;
      if (firstKey) this.peerGenerations.delete(firstKey);
    }
    this.peerGenerations.set(peerId, generation);
  }

  public getPeerGeneration(peerId: string): number | undefined {
    return this.peerGenerations.get(peerId);
  }

  /**
   * Registers an authenticated supplier transport context from the trusted transport adapter (ControlEventService / PeerMesh).
   * Requires the exact authorized TransportRegistrationCapability injected at engine construction.
   * Contexts registered here are branded and retained in a private WeakSet to ensure unforgeable provenance.
   * Plain objects constructed by arbitrary application callers without registration or without the capability are rejected.
   */
  public registerInboundSupplierContext(
    supplierPeerId: string,
    connectionGeneration: number,
    capability: TransportRegistrationCapability,
  ): AuthenticatedSupplierContext {
    if (
      !this.authorizedTransportCapability ||
      !capability ||
      capability !== this.authorizedTransportCapability
    ) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        'Supplier context registration requires authorized TransportRegistrationCapability',
      );
    }
    if (!supplierPeerId || typeof supplierPeerId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'supplierPeerId must be non-empty string');
    }
    if (
      typeof connectionGeneration !== 'number' ||
      !Number.isInteger(connectionGeneration) ||
      connectionGeneration < 1
    ) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid connection generation ${connectionGeneration}: must be positive integer`,
      );
    }
    const context: AuthenticatedSupplierContext = Object.freeze({
      supplierPeerId,
      connectionGeneration,
    });
    this.validSupplierContexts.add(context);
    return context;
  }

  public getState(): Readonly<PlaybackState> {
    return this.state;
  }

  public getStatus(): EngineStatus {
    return {
      state: this.disposed ? 'disposed' : 'active',
      sessionId: this.config.sessionId,
      localPeerId: this.config.localPeerId,
      stateVersion: this.state.stateVersion,
    };
  }

  public getStats(): Readonly<EngineStats> {
    return { ...this.mutableStats };
  }

  public getLastDecision(): Readonly<EngineDecision> | undefined {
    return this.lastDecision;
  }

  public getCurrentPositionMs(atTimestamp?: number): number {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'SyncEngine is disposed');
    }
    const target = atTimestamp ?? this.clock.now();
    if (typeof target !== 'number' || !Number.isFinite(target) || target < 0) {
      throw new SyncEngineError(
        'CLOCK_ERROR',
        `getCurrentPositionMs received invalid timestamp: ${target}`,
      );
    }
    return calculateExtrapolatedPosition(this.state, target);
  }

  public onStateChange(listener: StateChangeListener): Unsubscribe {
    this.stateChangeListeners.add(listener);
    return () => this.stateChangeListeners.delete(listener);
  }

  public onDecision(listener: DecisionListener): Unsubscribe {
    this.decisionListeners.add(listener);
    return () => this.decisionListeners.delete(listener);
  }

  /**
   * Consumes a control event candidate.
   * Local events throw SyncEngineError on validation failure.
   * Remote events isolate failures safely without throwing.
   */
  public receive(event: AnyControlSyncEvent): void {
    const isLocal = event.origin === 'local';
    const now = this.clock.now();

    if (this.disposed) {
      if (isLocal) {
        throw new SyncEngineError('DISPOSED', 'SyncEngine is disposed');
      }
      this.recordDecision(event, {
        accepted: false,
        code: 'REJECTED_DISPOSED',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: 'Engine is disposed',
      });
      return;
    }

    if (isLocal) {
      this.receiveLocal(event, now);
    } else {
      try {
        this.receiveRemote(event, now);
      } catch (err) {
        // Remote error isolation: do not throw to caller
        const code =
          err instanceof SyncEngineError && err.code === 'SEQUENCE_COLLISION_DETECTED'
            ? 'REJECTED_SEQUENCE_CONFLICT'
            : err instanceof SyncEngineError && err.code === 'IDENTITY_CONFLICT_DETECTED'
              ? 'REJECTED_IDENTITY_CONFLICT'
              : err instanceof SyncEngineError && err.code === 'REJECTED_CAPACITY'
                ? 'REJECTED_CAPACITY'
                : err instanceof SyncEngineError && err.code === 'REJECTED_CLOCK_INFLATION'
                  ? 'REJECTED_CLOCK_INFLATION'
                  : err instanceof SyncEngineError &&
                      (err.code === 'INVALID_CAUSAL_TIMESTAMP' ||
                        err.code === 'INVALID_LOGICAL_TIMESTAMP' ||
                        err.code === 'CLOCK_REGRESSION')
                    ? 'REJECTED_CAUSAL_VIOLATION'
                    : err instanceof SyncEngineError && err.code === 'REJECTED_OUTSIDE_HORIZON'
                      ? 'REJECTED_OUTSIDE_HORIZON'
                      : 'REJECTED_INVALID';

        this.recordDecision(event, {
          accepted: false,
          code,
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  /**
   * Process local outbound event.
   */
  private receiveLocal(event: AnyControlSyncEvent, now: number): void {
    const logicalEventId = event.message.logicalEventId;
    if (!logicalEventId || typeof logicalEventId !== 'string' || !logicalEventId.trim()) {
      throw new SyncEngineError('INVALID_PAYLOAD', 'Local control event requires logicalEventId');
    }

    // Local sequence advance
    this.localOriginSequence += 1;
    const originSeq = this.localOriginSequence;

    // Snapshot immutable causal context representing author's complete contiguous frontier
    const causalContext = { ...this.localCausalContext };
    if (originSeq > 1) {
      causalContext[this.config.localPeerId] = originSeq - 1;
    }

    // Resolve exact timestamps of complete causal frontier
    const dependencyClocks: number[] = [];
    for (const [depOrigin, depSeq] of Object.entries(causalContext)) {
      if (depSeq === 0) continue;
      const depOp = this.operationLog.getOperationAt(depOrigin, depSeq);
      if (depOp) {
        dependencyClocks.push(depOp.logicalTimestamp);
      } else {
        const cpTs = this.operationLog.getCompactedCheckpoint(depOrigin, depSeq);
        if (cpTs !== undefined) {
          dependencyClocks.push(cpTs);
        } else {
          throw new SyncEngineError(
            'REJECTED_OUTSIDE_HORIZON',
            `Local event causal dependency ${depOrigin}:${depSeq} is outside retained history`,
          );
        }
      }
    }

    let maxFrontierTimestamp = 0;
    for (const clk of dependencyClocks) {
      if (clk > maxFrontierTimestamp) {
        maxFrontierTimestamp = clk;
      }
    }

    // If local clock exceeds represented frontier, throw to prevent authoring an inflated event
    if (this.clockManager.clock > maxFrontierTimestamp && dependencyClocks.length > 0) {
      throw new SyncEngineError(
        'INVALID_LOGICAL_TIMESTAMP',
        `Local clock ${this.clockManager.clock} exceeds maximum represented frontier timestamp ${maxFrontierTimestamp}`,
      );
    }

    // Exact Lamport creation invariant: L(e) = 1 + max(dependencies in causalContext)
    const stampedTimestamp = dependencyClocks.length === 0 ? 1 : 1 + maxFrontierTimestamp;
    this.clockManager.reseed(stampedTimestamp);

    // Component-wise merge origin sequence into author's local causal frontier and replication vector
    this.localCausalContext[this.config.localPeerId] = originSeq;
    this.replicationVector[this.config.localPeerId] = originSeq;

    const storedOp: StoredOperation = {
      logicalEventId,
      originPeerId: this.config.localPeerId,
      senderPeerId: this.config.localPeerId,
      membershipEpoch: this.currentEpoch,
      messageType: event.message.messageType as any,
      logicalTimestamp: stampedTimestamp,
      originSequenceNumber: originSeq,
      causalContext,
      appliedAtMonotonic: now,
      payload: event.message.payload,
    };

    const insertResult = this.operationLog.insert(storedOp);
    const reducerContext = this.getReducerContext();

    if (insertResult.isHead) {
      const allOps = this.operationLog.getAllOperations();
      const priors = allOps.slice(0, allOps.length - 1);
      const { nextState, decision } = this.conflictResolver.reduce(
        this.state,
        storedOp,
        now,
        reducerContext,
        priors,
      );
      this.commitState(nextState, decision, event);
    } else {
      // Replay forward from base snapshot
      const baseSnap = this.operationLog.getBaseSnapshot() ?? this.baseSnapshot;
      const { finalState, decisions } = this.conflictResolver.replay(
        baseSnap,
        this.operationLog.getAllOperations(),
        now,
        reducerContext,
      );
      this.commitState(finalState, decisions[decisions.length - 1]!, event);
    }
  }

  /**
   * Process remote inbound event.
   */
  private receiveRemote(
    event: AnyControlSyncEvent,
    now: number,
    validationContext: 'LIVE_BROADCAST' | 'REPAIR_BATCH' = 'LIVE_BROADCAST',
  ): void {
    const logicalEventId = event.message.logicalEventId;
    if (!logicalEventId || typeof logicalEventId !== 'string' || !logicalEventId.trim()) {
      throw new SyncEngineError('INVALID_PAYLOAD', 'Remote event requires logicalEventId');
    }

    const originPeerId = event.message.originPeerId ?? event.message.senderPeerId;
    const senderPeerId = event.message.senderPeerId;

    // Strict connection generation check:
    // senderPeerId is the immediate transport sender (author in LIVE_BROADCAST, supplier in REPAIR_BATCH).
    // Validates that senderPeerId's claimed connectionGeneration matches authorized generation.
    // In REPAIR_BATCH, this validates the supplier's generation without restricting historical originPeerId.
    const activeGen = this.peerGenerations.get(senderPeerId) ?? 1;
    const claimedGen = event.message.connectionGeneration;

    if (claimedGen !== undefined) {
      if (claimedGen < activeGen) {
        this.recordDecision(event, {
          accepted: false,
          code: 'REJECTED_INVALID',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Event from stale connection generation ${claimedGen} < active ${activeGen}`,
        });
        return;
      }
      if (claimedGen > activeGen) {
        // Incoming event cannot independently authorize a generation increase!
        this.recordDecision(event, {
          accepted: false,
          code: 'REJECTED_INVALID',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Event claims unannounced connection generation ${claimedGen} > authorized ${activeGen}`,
        });
        return;
      }
    }

    // Strict epoch fencing
    if (event.message.membershipEpoch !== undefined) {
      if (event.message.membershipEpoch < this.currentEpoch) {
        this.recordDecision(event, {
          accepted: false,
          code: 'REJECTED_OBSOLETE_EPOCH',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Event from obsolete epoch ${event.message.membershipEpoch} < ${this.currentEpoch}`,
        });
        return;
      }
      if (event.message.membershipEpoch > this.currentEpoch) {
        this.recordDecision(event, {
          accepted: false,
          code: 'REJECTED_OBSOLETE_EPOCH',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Event from unestablished future epoch ${event.message.membershipEpoch} > ${this.currentEpoch}`,
        });
        return;
      }
    }

    // Strict No-Forwarding for live broadcasts
    if (validationContext === 'LIVE_BROADCAST' && event.message.originPeerId) {
      if (event.message.originPeerId !== senderPeerId) {
        throw new SyncEngineError(
          'REJECTED_UNAUTHORIZED_ORIGIN',
          `Live broadcast originPeerId ${event.message.originPeerId} !== senderPeerId ${senderPeerId}`,
        );
      }
    }

    const originSeq = event.message.originSequenceNumber ?? 1;
    const causalContext = event.message.causalContext ?? {};

    const logicalTimestamp = event.message.logicalTimestamp;
    if (typeof logicalTimestamp !== 'number') {
      throw new SyncEngineError('INVALID_CLOCK', 'Remote event missing logicalTimestamp');
    }

    const storedOp: StoredOperation = {
      logicalEventId,
      originPeerId,
      senderPeerId,
      membershipEpoch: event.message.membershipEpoch ?? this.currentEpoch,
      messageType: event.message.messageType as any,
      logicalTimestamp,
      originSequenceNumber: originSeq,
      causalContext,
      appliedAtMonotonic: now,
      payload: event.message.payload,
    };

    // 1. Check duplicate by logicalEventId with identity reuse verification
    const existingById = this.operationLog.get(logicalEventId);
    if (existingById) {
      const incomingFp = computeOperationFingerprint(storedOp);
      const existingFp = computeOperationFingerprint(existingById);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${logicalEventId}: immutable content altered`,
        );
      }
      this.recordDecision(event, {
        accepted: true,
        code: 'IGNORED_DUPLICATE',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
      });
      return;
    }

    // 2. Check collision on same sequence
    const existingBySeq = this.operationLog.getOperationAt(originPeerId, originSeq);
    if (existingBySeq) {
      if (existingBySeq.logicalEventId !== logicalEventId) {
        throw new SyncEngineError(
          'SEQUENCE_COLLISION_DETECTED',
          `Sequence collision detected for origin ${originPeerId} seq ${originSeq}`,
        );
      }
      const incomingFp = computeOperationFingerprint(storedOp);
      const existingFp = computeOperationFingerprint(existingBySeq);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${logicalEventId}: immutable content altered`,
        );
      }
      this.recordDecision(event, {
        accepted: true,
        code: 'IGNORED_DUPLICATE',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
      });
      return;
    }

    // 3. Check if already compacted below base snapshot frontier
    if (originSeq <= (this.baseSnapshotFrontier[originPeerId] ?? 0)) {
      const cpRecord = this.operationLog.getCompactedCheckpointRecord?.(originPeerId, originSeq);
      if (cpRecord) {
        if (cpRecord.logicalEventId && cpRecord.logicalEventId !== logicalEventId) {
          throw new SyncEngineError(
            'SEQUENCE_COLLISION_DETECTED',
            `Sequence collision detected for compacted origin ${originPeerId} seq ${originSeq}: compacted ${cpRecord.logicalEventId} vs incoming ${logicalEventId}`,
          );
        }
        if (cpRecord.fingerprint) {
          const incomingFp = computeOperationFingerprint(storedOp);
          if (incomingFp !== cpRecord.fingerprint) {
            throw new SyncEngineError(
              'IDENTITY_CONFLICT_DETECTED',
              `Identity conflict detected for compacted event ${logicalEventId}: immutable content altered`,
            );
          }
        }
        this.recordDecision(event, {
          accepted: true,
          code: 'IGNORED_ALREADY_COMPACTED',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
        });
        return;
      } else {
        // Identity metadata expired beyond retention horizon -> cannot distinguish duplicate from conflict
        this.recordDecision(event, {
          accepted: false,
          code: 'REJECTED_OUTSIDE_HORIZON',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Operation ${logicalEventId} (${originPeerId}:${originSeq}) predates retention horizon and cannot be verified`,
        });
        return;
      }
    }

    // 4. For any sequence > 1, author's immediate predecessor MUST be declared in causalContext
    if (originSeq > 1 && causalContext[originPeerId] !== originSeq - 1) {
      throw new SyncEngineError(
        'INVALID_CAUSAL_TIMESTAMP',
        `Event from ${originPeerId} seq ${originSeq} missing immediate predecessor seq ${originSeq - 1} in causalContext`,
      );
    }

    // Check missing dependencies and same-origin sequence gaps
    const currentOriginSeq = this.replicationVector[originPeerId] ?? 0;
    let missingDep = false;

    if (originSeq !== currentOriginSeq + 1) {
      missingDep = true;
    }

    for (const [depOrigin, reqSeq] of Object.entries(causalContext)) {
      const haveSeq = this.replicationVector[depOrigin] ?? 0;
      if (reqSeq > haveSeq) {
        missingDep = true;
        break;
      }
    }

    if (missingDep) {
      // Buffer in PendingDependencyQueue. Clock and vector DO NOT advance!
      try {
        this.pendingQueue.enqueue(storedOp);
        this.recordDecision(event, {
          accepted: false,
          code: 'BUFFERED_PENDING_DEPENDENCY',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Buffered operation ${logicalEventId} awaiting missing causal dependencies`,
        });
      } catch (err) {
        if (err instanceof SyncEngineError && err.code === 'REJECTED_CAPACITY') {
          this.recordDecision(event, {
            accepted: false,
            code: 'REJECTED_CAPACITY',
            stateVersion: this.state.stateVersion,
            decidedAt: now,
            message: err.message,
          });
          return;
        }
        throw err;
      }
      return;
    }

    // All dependencies present: admit operation
    this.admitOperation(storedOp, event, now, validationContext);
  }

  /**
   * Admits an operation whose causal dependencies are satisfied.
   */
  private admitOperation(
    op: StoredOperation,
    originalEvent: AnyControlSyncEvent | undefined,
    now: number,
    validationContext: 'LIVE_BROADCAST' | 'REPAIR_BATCH',
  ): void {
    // 0. Check duplicate by logicalEventId or origin sequence with identity conflict verification
    const existingById = this.operationLog.get(op.logicalEventId);
    if (existingById) {
      const incomingFp = computeOperationFingerprint(op);
      const existingFp = computeOperationFingerprint(existingById);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${op.logicalEventId}: immutable content altered`,
        );
      }
      this.recordDecision(originalEvent ?? ({ message: op } as any), {
        accepted: true,
        code: 'IGNORED_DUPLICATE',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
      });
      return;
    }

    const existingBySeq = this.operationLog.getOperationAt(op.originPeerId, op.originSequenceNumber);
    if (existingBySeq) {
      if (existingBySeq.logicalEventId !== op.logicalEventId) {
        throw new SyncEngineError(
          'SEQUENCE_COLLISION_DETECTED',
          `Sequence collision detected for origin ${op.originPeerId} seq ${op.originSequenceNumber}`,
        );
      }
      const incomingFp = computeOperationFingerprint(op);
      const existingFp = computeOperationFingerprint(existingBySeq);
      if (incomingFp !== existingFp) {
        throw new SyncEngineError(
          'IDENTITY_CONFLICT_DETECTED',
          `Operation identity conflict for event ${op.logicalEventId}: immutable content altered`,
        );
      }
      this.recordDecision(originalEvent ?? ({ message: op } as any), {
        accepted: true,
        code: 'IGNORED_DUPLICATE',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
      });
      return;
    }

    // Check compacted checkpoints
    const cp = this.operationLog.getCompactedCheckpointRecord?.(op.originPeerId, op.originSequenceNumber);
    if (cp) {
      if (cp.logicalEventId && cp.logicalEventId !== op.logicalEventId) {
        throw new SyncEngineError(
          'SEQUENCE_COLLISION_DETECTED',
          `Sequence collision detected for compacted origin ${op.originPeerId} seq ${op.originSequenceNumber}`,
        );
      }
      if (cp.fingerprint) {
        const incomingFp = computeOperationFingerprint(op);
        if (incomingFp !== cp.fingerprint) {
          throw new SyncEngineError(
            'IDENTITY_CONFLICT_DETECTED',
            `Identity conflict detected for compacted event ${op.logicalEventId}: immutable content altered`,
          );
        }
      }
      this.recordDecision(originalEvent ?? ({ message: op } as any), {
        accepted: true,
        code: 'IGNORED_ALREADY_COMPACTED',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
      });
      return;
    }

    // 1. Resolve exact dependency timestamps across tiers (Log or Compacted Checkpoints)
    const dependencyClocks: number[] = [];
    for (const [depOrigin, depSeq] of Object.entries(op.causalContext)) {
      if (depSeq === 0) continue;
      const depOp = this.operationLog.getOperationAt(depOrigin, depSeq);
      if (depOp) {
        dependencyClocks.push(depOp.logicalTimestamp);
      } else {
        const checkpointTimestamp = this.operationLog.getCompactedCheckpoint(depOrigin, depSeq);
        if (checkpointTimestamp !== undefined) {
          dependencyClocks.push(checkpointTimestamp);
        } else {
          throw new SyncEngineError(
            'REJECTED_OUTSIDE_HORIZON',
            `Dependency ${depOrigin}:${depSeq} predates retained history and checkpoint horizon`,
          );
        }
      }
    }

    // 2. Validate exact Lamport timestamp and advance local clock (without +1)
    this.clockManager.validateAndAdvanceRemote(
      op.originPeerId,
      op.membershipEpoch,
      op.logicalTimestamp,
      dependencyClocks,
      now,
      validationContext,
    );

    // 3. Component-wise causal frontier merge
    this.localCausalContext[op.originPeerId] = op.originSequenceNumber;
    this.replicationVector[op.originPeerId] = op.originSequenceNumber;

    // 4. Total-order log insertion
    const insertResult = this.operationLog.insert(op);
    if (!insertResult.inserted) {
      if (insertResult.code === 'IGNORED_ALREADY_COMPACTED') {
        this.recordDecision(originalEvent ?? ({ message: op } as any), {
          accepted: true,
          code: 'IGNORED_ALREADY_COMPACTED',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
        });
        return;
      }
      if (insertResult.code === 'REJECTED_OUTSIDE_HORIZON') {
        throw new SyncEngineError(
          'REJECTED_OUTSIDE_HORIZON',
          `Operation ${op.logicalEventId} falls before compacted horizon and cannot be verified`,
        );
      }
      return;
    }

    const reducerContext = this.getReducerContext();

    if (insertResult.isHead) {
      const allOps = this.operationLog.getAllOperations();
      const priors = allOps.slice(0, allOps.length - 1);
      const { nextState, decision } = this.conflictResolver.reduce(
        this.state,
        op,
        now,
        reducerContext,
        priors,
      );
      this.commitState(nextState, decision, originalEvent);
    } else {
      // Speculative rollback/replay from base snapshot
      const baseSnap = this.operationLog.getBaseSnapshot() ?? this.baseSnapshot;
      const { finalState, decisions } = this.conflictResolver.replay(
        baseSnap,
        this.operationLog.getAllOperations(),
        now,
        reducerContext,
      );
      this.commitState(finalState, decisions[decisions.length - 1]!, originalEvent);
    }

    // 5. Check if any buffered pending operations are now unblocked
    this.drainPendingOperations(now);
  }

  /**
   * Drains satisfied operations from PendingDependencyQueue and applies them.
   */
  private drainPendingOperations(now: number): void {
    const unblocked = this.pendingQueue.drainSatisfied(this.replicationVector);
    for (const op of unblocked) {
      try {
        this.admitOperation(op, undefined, now, 'REPAIR_BATCH');
      } catch (err) {
        // If an unblocked operation fails validation, log rejection
        this.mutableStats.eventsRejectedCount++;
      }
    }
  }

  /**
   * Ingests a repair batch supplying missing historical operations.
   *
   * Authenticated transport binding:
   * - Requires mandatory AuthenticatedSupplierContext supplied by the trusted transport layer.
   * - Resolves the trusted transport supplier identity strictly from AuthenticatedSupplierContext.
   * - Requires valid connectionGeneration from the local peer lifecycle; absent/invalid is rejected.
   * - Rejects forged supplier identities if the payload claims a supplier differing from transport identity.
   * - Validates supplier connection generation against authorized peerGenerations.
   * - Stale or unannounced supplier generations are rejected with REJECTED_INVALID.
   * - Validates that batch.originPeerId matches each rawOp.originPeerId (rejects origin rewriting).
   * - Historical author generation is not compared against supplier generation.
   * - Catches identity conflicts and sequence collisions, returning specific rejection codes.
   */
  public ingestRepairBatch(
    batch: OperationBatchPayload,
    transportContext?: AuthenticatedSupplierContext,
  ): EngineDecision[] {
    const now = this.clock.now();
    const decisions: EngineDecision[] = [];

    // 1. Validate mandatory authenticated supplier transport context
    if (!transportContext || typeof transportContext !== 'object') {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: 'Repair batch requires authenticated supplier transport context',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    if (
      !transportContext.supplierPeerId ||
      typeof transportContext.supplierPeerId !== 'string'
    ) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: 'Repair batch requires valid supplierPeerId in transport context',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    // 2. Validate mandatory supplier connection generation from local peer lifecycle
    if (
      typeof transportContext.connectionGeneration !== 'number' ||
      !Number.isInteger(transportContext.connectionGeneration) ||
      transportContext.connectionGeneration < 1
    ) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: 'Repair batch requires valid supplier connectionGeneration',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    // 3. Validate unforgeable transport registration
    if (!this.validSupplierContexts.has(transportContext)) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message:
          'Fabricated or unauthenticated supplier transport context: context must be locally registered by transport adapter',
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    const transportSupplierPeerId = transportContext.supplierPeerId;
    const transportSupplierGen = transportContext.connectionGeneration;

    // 4. Reject forged supplier identity if payload claims a different peer than authenticated transport
    if (batch.supplierPeerId && batch.supplierPeerId !== transportSupplierPeerId) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: `Forged supplier identity: payload claims ${batch.supplierPeerId} but authenticated transport supplier is ${transportSupplierPeerId}`,
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    // 5. Authoritative supplier authorization & generation match (No ?? 1 fallback)
    const activeSupplierGen = this.peerGenerations.get(transportSupplierPeerId);
    if (activeSupplierGen === undefined) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: `Unknown or unauthorized repair supplier: ${transportSupplierPeerId}`,
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    if (transportSupplierGen < activeSupplierGen) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: `Repair batch from stale supplier connection generation ${transportSupplierGen} < active ${activeSupplierGen}`,
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }
    if (transportSupplierGen > activeSupplierGen) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: `Repair batch claims unannounced supplier connection generation ${transportSupplierGen} > authorized ${activeSupplierGen}`,
      };
      this.lastDecision = decision;
      this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
      this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
      return [decision];
    }

    // Sort batch operations by sequence number to ensure contiguous ingestion
    const sortedOps = [...batch.operations].sort(
      (a, b) => a.originSequenceNumber - b.originSequenceNumber,
    );

    for (const rawOp of sortedOps) {
      // Validate that supplier cannot rewrite originPeerId
      if (batch.originPeerId && rawOp.originPeerId !== batch.originPeerId) {
        this.mutableStats.eventsReceivedCount++;
        this.mutableStats.eventsRejectedCount++;
        const decision: EngineDecision = {
          accepted: false,
          code: 'REJECTED_INVALID',
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: `Repair batch operation originPeerId ${rawOp.originPeerId} does not match batch originPeerId ${batch.originPeerId}`,
        };
        this.lastDecision = decision;
        decisions.push(decision);
        continue;
      }

      const storedOp: StoredOperation = {
        logicalEventId: rawOp.logicalEventId,
        originPeerId: rawOp.originPeerId,
        senderPeerId: transportSupplierPeerId,
        membershipEpoch: rawOp.membershipEpoch,
        messageType: rawOp.messageType,
        logicalTimestamp: rawOp.logicalTimestamp,
        originSequenceNumber: rawOp.originSequenceNumber,
        causalContext: rawOp.causalContext,
        appliedAtMonotonic: now,
        payload: rawOp.payload,
      };

      try {
        this.admitOperation(storedOp, undefined, now, 'REPAIR_BATCH');
        if (this.lastDecision) {
          decisions.push(this.lastDecision);
        }
      } catch (err) {
        this.mutableStats.eventsReceivedCount++;
        this.mutableStats.eventsRejectedCount++;
        const code =
          err instanceof SyncEngineError && err.code === 'IDENTITY_CONFLICT_DETECTED'
            ? 'REJECTED_IDENTITY_CONFLICT'
            : err instanceof SyncEngineError && err.code === 'SEQUENCE_COLLISION_DETECTED'
              ? 'REJECTED_SEQUENCE_CONFLICT'
              : 'REJECTED_INVALID';
        const decision: EngineDecision = {
          accepted: false,
          code,
          stateVersion: this.state.stateVersion,
          decidedAt: now,
          message: err instanceof Error ? err.message : String(err),
        };
        this.lastDecision = decision;
        decisions.push(decision);
      }
    }

    return decisions;
  }

  /**
   * Commits state changes and emits notifications.
   */
  private commitState(
    nextState: PlaybackState,
    decision: EngineDecision,
    event?: AnyControlSyncEvent,
  ): void {
    this.mutableStats.eventsReceivedCount++;
    this.lastDecision = decision;

    if (decision.accepted) {
      if (decision.code === 'APPLIED') {
        this.mutableStats.eventsAppliedCount++;
      } else {
        this.mutableStats.eventsNoOpCount++;
      }
    } else {
      this.mutableStats.eventsRejectedCount++;
    }

    const stateChanged = nextState !== this.state;
    this.state = nextState;

    if (stateChanged) {
      this.mutableStats.stateChangeNotificationsCount++;
      for (const listener of this.stateChangeListeners) {
        try {
          listener(this.state);
        } catch {
          this.mutableStats.listenerErrorCount++;
        }
      }
    }

    if (event) {
      this.notifyDecision(event, decision);
    }
  }

  private recordDecision(event: AnyControlSyncEvent, decision: EngineDecision): void {
    this.mutableStats.eventsReceivedCount++;
    this.lastDecision = decision;
    if (decision.accepted) {
      this.mutableStats.eventsNoOpCount++;
    } else {
      this.mutableStats.eventsRejectedCount++;
    }
    this.notifyDecision(event, decision);
  }

  private notifyDecision(event: AnyControlSyncEvent, decision: EngineDecision): void {
    this.mutableStats.decisionNotificationsCount++;
    for (const listener of this.decisionListeners) {
      try {
        listener(event, decision);
      } catch {
        this.mutableStats.listenerErrorCount++;
      }
    }
  }

  private getReducerContext(): ReducerContext {
    return {
      sessionId: this.config.sessionId,
      localPeerId: this.config.localPeerId,
      minPlaybackRate: this.config.minPlaybackRate,
      maxPlaybackRate: this.config.maxPlaybackRate,
      positionEpsilonMs: this.config.positionEpsilonMs,
    };
  }

  public exportSnapshot(snapshotId?: string): StateSnapshotPayload {
    return exportEngineSnapshot(this.state, this.config.sessionId, snapshotId);
  }

  public importSnapshot(payload: StateSnapshotPayload): EngineDecision {
    const now = this.clock.now();
    if (this.disposed) {
      return {
        accepted: false,
        code: 'REJECTED_DISPOSED',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: 'Engine is disposed',
      };
    }

    let validated;
    try {
      validated = validateSnapshotPayload(
        payload,
        this.config.sessionId,
        this.config.minPlaybackRate,
        this.config.maxPlaybackRate,
      );
    } catch (err) {
      const decision: EngineDecision = {
        accepted: false,
        code: 'REJECTED_INVALID',
        stateVersion: this.state.stateVersion,
        decidedAt: now,
        message: err instanceof Error ? err.message : String(err),
      };
      this.lastDecision = decision;
      this.mutableStats.eventsRejectedCount++;
      return decision;
    }

    const { data } = validated;
    const newVersion = this.state.stateVersion + 1;

    const lastOp = data.lastAppliedOperation
      ? Object.freeze({
          logicalEventId: data.lastAppliedOperation.logicalEventId,
          senderPeerId: data.lastAppliedOperation.senderPeerId,
          messageType: data.lastAppliedOperation.messageType,
          appliedAtLocalTimestamp: now,
          isImportedProvenance: true,
        })
      : undefined;

    const importedState = Object.freeze<PlaybackState>({
      status: data.status,
      anchorPositionMs: data.anchorPositionMs,
      anchorLocalTimestamp: now,
      playbackRate: data.playbackRate,
      stateVersion: newVersion,
      lastAppliedEvent: lastOp,
    });

    this.state = importedState;
    this.baseSnapshot = importedState;
    this.operationLog.reset(importedState);
    this.pendingQueue.clear();

    const decision: EngineDecision = {
      accepted: true,
      code: 'APPLIED',
      stateVersion: newVersion,
      decidedAt: now,
      message: `Snapshot ${validated.snapshotId} imported successfully`,
    };

    this.mutableStats.eventsAppliedCount++;
    this.mutableStats.stateChangeNotificationsCount++;
    this.lastDecision = decision;

    for (const listener of this.stateChangeListeners) {
      try {
        listener(this.state);
      } catch {
        this.mutableStats.listenerErrorCount++;
      }
    }

    return decision;
  }

  /**
   * Phase 7 snapshot import adhering to exact Lamport and causal recovery rules.
   */
  public importSnapshotV2(snapshot: StateSnapshotPayloadV2): EngineDecision {
    const now = this.clock.now();

    // 1. Reseed Lamport clock: L = max(L_local, maxLogicalTimestamp) (without +1)
    this.clockManager.reseed(snapshot.maxLogicalTimestamp);

    // 2. Initialize local causal context and replication vector to snapshot frontier
    for (const key of Object.keys(this.localCausalContext)) {
      delete this.localCausalContext[key];
    }
    for (const key of Object.keys(this.replicationVector)) {
      delete this.replicationVector[key];
    }
    for (const [peer, seq] of Object.entries(snapshot.snapshotFrontier)) {
      this.localCausalContext[peer] = seq;
      this.replicationVector[peer] = seq;
      this.baseSnapshotFrontier[peer] = seq;
    }

    // 3. Update playback state and reset log to new base
    const rawState = snapshot.state as Partial<PlaybackState>;
    const importedState = Object.freeze<PlaybackState>({
      status: rawState.status ?? 'paused',
      anchorPositionMs: rawState.anchorPositionMs ?? 0,
      anchorLocalTimestamp: now,
      playbackRate: rawState.playbackRate ?? 1.0,
      stateVersion: this.state.stateVersion + 1,
      lastAppliedEvent: Object.freeze({
        logicalEventId: `snap-${snapshot.snapshotId}`,
        senderPeerId: snapshot.authorPeerId,
        messageType: 'SNAPSHOT_IMPORT',
        appliedAtLocalTimestamp: now,
        isImportedProvenance: true,
      }),
    });

    this.state = importedState;
    this.baseSnapshot = importedState;
    this.operationLog.reset(importedState, snapshot.snapshotFrontier);
    this.pendingQueue.clear();

    // 4. Ingest compacted checkpoints after log reset
    for (const cp of snapshot.compactedCheckpoints) {
      this.operationLog.setCompactedCheckpoint(
        cp.originPeerId,
        cp.sequenceNumber,
        cp.logicalTimestamp,
        (cp as any).logicalEventId ?? '',
        (cp as any).fingerprint ?? '',
      );
    }
    // Also ingest frontier timestamps if not already present in compactedCheckpoints
    for (const [peer, seq] of Object.entries(snapshot.snapshotFrontier)) {
      const ts = snapshot.originFrontierTimestamps[peer];
      if (ts !== undefined && this.operationLog.getCompactedCheckpoint(peer, seq) === undefined) {
        this.operationLog.setCompactedCheckpoint(peer, seq, ts, '');
      }
    }

    const decision: EngineDecision = {
      accepted: true,
      code: 'APPLIED',
      stateVersion: this.state.stateVersion,
      decidedAt: now,
    };

    this.mutableStats.eventsAppliedCount++;
    this.mutableStats.stateChangeNotificationsCount++;
    this.lastDecision = decision;

    for (const listener of this.stateChangeListeners) {
      try {
        listener(this.state);
      } catch {
        this.mutableStats.listenerErrorCount++;
      }
    }

    return decision;
  }

  /**
   * Generates a consistent point-in-time StateDigestPayload snapshot for broadcasting.
   * Conforms to Phase 7.4 multi-vector formatVersion 2.
   *
   * SEMANTICS:
   * - replicationVector[origin]: highest contiguous applied sequence (1..k).
   * - snapshotFrontier[origin]: highest contiguous sequence already incorporated into the
   *   compacted base snapshot. Sequences <= snapshotFrontier are compacted out of the log.
   * - historyFingerprints[origin]: canonical SHA-256 fingerprint at sequence k = replicationVector[origin].
   */
  public createLocalDigest(sentAtOverride?: number): StateDigestPayload {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot create digest from disposed engine');
    }

    // 1. Consistent snapshot read
    const replicationVector: Record<string, number> = { ...this.replicationVector };
    const snapshotFrontier: Record<string, number> = {
      ...(this.operationLog.getBaseSnapshotFrontier
        ? this.operationLog.getBaseSnapshotFrontier()
        : {}),
    };

    // 2. Extract tail fingerprints for all origins with sequence > 0
    const historyFingerprints: Record<string, string> = {};
    for (const [origin, seq] of Object.entries(replicationVector)) {
      if (seq > 0) {
        const fp = this.operationLog.getTailFingerprint?.(origin, seq);
        if (fp && fp.length === 64) {
          historyFingerprints[origin] = fp;
        }
      }
    }

    const payload: StateDigestPayload = {
      formatVersion: 2,
      sessionId: this.config.sessionId,
      senderPeerId: this.config.localPeerId,
      connectionGeneration: this.peerGenerations.get(this.config.localPeerId) ?? 1,
      membershipEpoch: this.currentEpoch,
      stateVersion: this.state.stateVersion,
      replicationVector,
      snapshotFrontier,
      historyFingerprints,
      sentAt: sentAtOverride ?? this.clock.now(),
    };

    // 3. Schema validation to guarantee structural invariants
    return StateDigestPayloadSchema.parse(payload);
  }

  /**
   * Evaluates an incoming remote digest against local replication knowledge.
   * Handles format version discrimination, schema validation, and pure comparison.
   *
   * SECURITY & TRUST BOUNDARY INVARIANT:
   * A remote digest's payload fields (senderPeerId, connectionGeneration, vectors) are
   * untrusted remote claims. Evaluating a digest does NOT authorize the sender as a repair
   * supplier, does NOT authenticate its connection generation, and does NOT permit operation
   * ingestion. Authoritative supplier authorization and repair batch ingestion strictly require
   * Phase 7.3-R5-H capability-gated registration via AuthenticatedSupplierContext.
   */
  public compareRemoteDigest(remoteDigest: unknown): DigestComparisonResult {
    if (this.disposed) {
      throw new SyncEngineError('DISPOSED', 'Cannot compare digest on disposed engine');
    }

    const senderPeerId =
      typeof remoteDigest === 'object' &&
      remoteDigest !== null &&
      typeof (remoteDigest as any).senderPeerId === 'string'
        ? (remoteDigest as any).senderPeerId
        : 'unknown';

    // 1. Legacy format discrimination (Phase 2 legacy digests lacking formatVersion or formatVersion !== 2)
    if (
      typeof remoteDigest !== 'object' ||
      remoteDigest === null ||
      (remoteDigest as any).formatVersion === undefined ||
      'digestId' in remoteDigest
    ) {
      return Object.freeze({
        remotePeerId: senderPeerId,
        missingFromLocal: Object.freeze([]),
        missingFromRemote: Object.freeze([]),
        forkDetected: false,
        conflictingOrigins: Object.freeze([]),
        outsideLocalHorizon: false,
        classification: 'INCONCLUSIVE',
        outsideRemoteHorizon: false,
        requiresSnapshotRecovery: false,
        unprovableOrigins: Object.freeze([]),
        hasUnverifiedHistory: false,
        originDetails: Object.freeze([]),
        diagnosticReasons: Object.freeze([
          'Ignoring legacy Phase 2 state digest: incompatible format',
        ]),
      });
    }

    // 2. Future version discrimination (formatVersion > 2)
    if (
      typeof (remoteDigest as any).formatVersion === 'number' &&
      (remoteDigest as any).formatVersion > 2
    ) {
      return Object.freeze({
        remotePeerId: senderPeerId,
        missingFromLocal: Object.freeze([]),
        missingFromRemote: Object.freeze([]),
        forkDetected: false,
        conflictingOrigins: Object.freeze([]),
        outsideLocalHorizon: false,
        classification: 'INCONCLUSIVE',
        outsideRemoteHorizon: false,
        requiresSnapshotRecovery: false,
        unprovableOrigins: Object.freeze([]),
        hasUnverifiedHistory: false,
        originDetails: Object.freeze([]),
        diagnosticReasons: Object.freeze([
          `Unsupported future protocol format version: ${(remoteDigest as any).formatVersion}`,
        ]),
      });
    }

    // 3. Strict schema validation
    const parsed = StateDigestPayloadSchema.safeParse(remoteDigest);
    if (!parsed.success) {
      return Object.freeze({
        remotePeerId: senderPeerId,
        missingFromLocal: Object.freeze([]),
        missingFromRemote: Object.freeze([]),
        forkDetected: false,
        conflictingOrigins: Object.freeze([]),
        outsideLocalHorizon: false,
        classification: 'INCONCLUSIVE',
        outsideRemoteHorizon: false,
        requiresSnapshotRecovery: false,
        unprovableOrigins: Object.freeze([]),
        hasUnverifiedHistory: false,
        originDetails: Object.freeze([]),
        diagnosticReasons: Object.freeze(
          parsed.error.issues.map((e) => `${e.path.join('.')}: ${e.message}`),
        ),
      });
    }

    // 4. Pure deterministic comparison
    const localDigest = this.createLocalDigest();
    return compareDigests(localDigest, parsed.data);
  }

  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.stateChangeListeners.clear();
    this.decisionListeners.clear();
    this.operationLog.dispose();
    this.pendingQueue.clear();
  }
}
