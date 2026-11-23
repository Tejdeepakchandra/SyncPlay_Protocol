/**
 * @module @syncplay/sync-engine/recovery
 *
 * Implements Phase 7.5 M4.2d Snapshot Inbound Pipeline:
 * - Deterministic, bounded inbound snapshot reception and window ingestion (W = 4).
 * - Option A Authoritative Active-Map Fencing & Logical Revocation:
 *   WeakSet provenance tracking + Map authoritative admission gating.
 * - 6-tuple envelope fencing on inbound chunks:
 *   (transferId, chunkIndex, sessionId, membershipEpoch, supplierPeerId, connectionGeneration).
 * - Unified historical acceptance invariant across Path A and Path B:
 *   Inbound chunks are admitted if and only if authentic DataChannel dispatch evidence exists.
 * - Bounded FIFO attempt evidence history (max 4 attempts/chunk, <= 138 total, 512 defensive cap).
 * - Safe Base64 decoding, canonical partition geometry validation, and computed SHA-256 verification.
 * - 3-phase failover protocol with irreversible ChunkAssembler commit point and Dispatch Boundary.
 * - 10-step fail-closed terminal abort with queue purge and isolated post-fence SNAPSHOT_ABORT emission.
 */

import {
  PROTOCOL_VERSION,
  MessageType,
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  decodeBase64,
  SnapshotOfferPayloadSchema,
  SnapshotChunkPayloadSchema,
  type ProtocolMessage,
  type SnapshotOfferPayload,
  type SnapshotRequestPayload,
  type SnapshotChunkPayload,
  type SnapshotChunkRequestPayload,
  type SnapshotAbortPayload,
  type SnapshotAbortReason,
  type StateSnapshotPayloadV2,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import { sha256 } from '../log/operation-fingerprint.js';
import {
  ChunkAssembler,
  type FailoverCandidateParams,
  type ChunkEnvelopeContext,
  type AssembledSnapshot,
} from './chunk-assembler.js';
import {
  CandidateOfferRegistry,
  MAX_OFFER_CANDIDATES,
} from './candidate-offer-registry.js';
import {
  INBOUND_CAPABILITY_BRAND,
  type InboundTransportCapability,
  assertInboundTransportCapability,
} from './inbound-custody.js';
import { SnapshotSerializer } from './snapshot-serializer.js';

export const DEFAULT_SLIDING_WINDOW_SIZE = 4;
export const DEFAULT_CHUNK_TIMEOUT_MS = 5_000;
export const DEFAULT_OVERALL_TIMEOUT_MS = 60_000;
export const DEFAULT_MAX_RETRIES_PER_CHUNK = 3;
export const DEFAULT_MAX_TRANSFER_RETRIES = 10;
export const DEFAULT_MAX_TRANSFER_CONTEXT_TRANSITIONS = 8;
export const DEFAULT_MAX_LOCAL_REDISPATCH_ATTEMPTS = 3;
export const MAX_EVIDENCE_RECORDS = 512;

function generateUUID(): string {
  if (typeof globalThis !== 'undefined' && globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function deepFreeze<T>(obj: T): Readonly<T> {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }
  Object.freeze(obj);
  for (const key of Object.keys(obj)) {
    const val = (obj as any)[key];
    if (val !== null && typeof val === 'object' && !Object.isFrozen(val)) {
      deepFreeze(val);
    }
  }
  return obj;
}

export type SupplierContextLifecycleState =
  | 'PREPARED_LOCAL'
  | 'ACTIVE_AUTHORIZED'
  | 'REVOKED_LOGICAL';

export interface AuthenticatedSupplierContext {
  readonly supplierPeerId: string;
  readonly sessionId?: string;
  readonly membershipEpoch?: number;
  readonly connectionGeneration?: number;
  readonly sctpMaxMessageSize?: number;
}

export type InboundChunkState =
  | 'UNREQUESTED'
  | 'REQUESTED'
  | 'RECEIVED_VERIFIED';

export type AttemptLifecycleState =
  | 'LOCAL_PENDING'
  | 'LOCALLY_FAILED'
  | 'LOCALLY_TIMED_OUT'
  | 'DATA_CHANNEL_ACCEPTED'
  | 'REMOTE_RESPONSE_TIMED_OUT'
  | 'SUPERSEDED_BY_RETRY'
  | 'FULFILLED'
  | 'CANCELLED';

export type AcceptedEvidenceStatus =
  | 'ACCEPTED_PENDING'
  | 'ACCEPTED_TIMED_OUT'
  | 'SUPERSEDED_ADMISSIBLE'
  | 'FULFILLED_DUPLICATE_ADMISSIBLE'
  | 'RETIRED_INADMISSIBLE';

export interface AcceptedAttemptRecord {
  readonly attemptToken: string;
  readonly transferId: string;
  readonly chunkIndex: number;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly supplierPeerId: string;
  readonly connectionGeneration: number;
  readonly messageId: string;
  readonly acceptedAtMs: number;
  status: AcceptedEvidenceStatus;
}

export interface ChunkEvidenceHistory {
  readonly chunkIndex: number;
  readonly attempts: AcceptedAttemptRecord[]; // Bounded FIFO array (max 4 entries)
  hasActiveAcceptedAttempt: boolean;
  verifiedChunkSha256?: string;
}

export type OutboundMessageType =
  | 'SNAPSHOT_REQUEST'
  | 'SNAPSHOT_CHUNK_REQUEST'
  | 'SNAPSHOT_ABORT';

export interface OutboundHandoffRequest {
  readonly message: ProtocolMessage<any>;
  readonly messageType: OutboundMessageType;
  readonly transferId: string;
  readonly handoffAttemptToken: string;
  readonly chunkIndices?: ReadonlyArray<number>;
}

export type InboundTransferState =
  | 'IDLE'
  | 'ADMITTED'
  | 'TRANSFERRING'
  | 'REASSEMBLING'
  | 'VERIFYING'
  | 'COMPLETED'
  | 'ABORTED';

export interface InboundTransferSession {
  readonly transferId: string;
  readonly sessionId: string;
  supplierPeerId: string;
  readonly membershipEpoch: number;
  connectionGeneration: number;
  state: InboundTransferState;
  readonly snapshotId: string;
  readonly totalSizeBytes: number;
  readonly totalChunks: number;
  readonly nominalChunkSizeBytes: number;
  readonly expectedSha256Digest: string;
  readonly receivedChunks: Set<number>;
  readonly inflightRequests: Set<number>;
  readonly missingChunks: Set<number>;
  readonly chunkRetries: Map<number, number>;
  cumulativeRetries: number;
  contextTransitionCount: number;
  readonly offer: Readonly<SnapshotOfferPayload>;
  failureReason?: SnapshotAbortReason;
  failureDetail?: string;
  isTerminal: boolean;
}

export interface SnapshotTransferResult {
  readonly transferId: string;
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly authorPeerId: string;
  readonly totalSizeBytes: number;
  readonly sha256Digest: string;
  readonly snapshot: Readonly<StateSnapshotPayloadV2>;
  readonly rawBytes: Uint8Array;
}

export interface InboundPipelineConfig {
  readonly transportCapability: InboundTransportCapability;
  readonly slidingWindowSize?: number;                // Default: 4
  readonly chunkTimeoutMs?: number;                   // Default: 5,000ms
  readonly overallTimeoutMs?: number;                 // Default: 60,000ms
  readonly maxRetriesPerChunk?: number;               // Default: 3
  readonly maxTransferRetries?: number;               // Default: 10
  readonly maxTransferContextTransitions?: number;    // Default: 8
  readonly maxLocalRedispatchAttempts?: number;       // Default: 3
  readonly nowFn?: () => number;                      // Monotonic millisecond provider
  readonly onTransferComplete: (result: SnapshotTransferResult) => Promise<void> | void;
  readonly onEmitMessage: (handoff: OutboundHandoffRequest) => Promise<void> | void;
  readonly onDiagnosticError?: (error: unknown, context: string) => void;
}

interface ResolvedConfig {
  readonly transportCapability: InboundTransportCapability;
  readonly slidingWindowSize: number;
  readonly chunkTimeoutMs: number;
  readonly overallTimeoutMs: number;
  readonly maxRetriesPerChunk: number;
  readonly maxTransferRetries: number;
  readonly maxTransferContextTransitions: number;
  readonly maxLocalRedispatchAttempts: number;
  readonly nowFn: () => number;
  readonly onTransferComplete: (result: SnapshotTransferResult) => Promise<void> | void;
  readonly onEmitMessage: (handoff: OutboundHandoffRequest) => Promise<void> | void;
  readonly onDiagnosticError?: (error: unknown, context: string) => void;
}

export class SnapshotInboundPipeline {
  readonly #authorizedCapability: InboundTransportCapability;
  readonly #config: ResolvedConfig;
  readonly #assembler: ChunkAssembler;
  readonly #candidateRegistry: CandidateOfferRegistry;

  // Option A Custody & Fencing:
  readonly #validSupplierContexts = new WeakSet<AuthenticatedSupplierContext>();
  readonly #activeSupplierContexts = new Map<string, AuthenticatedSupplierContext>();

  // Attempt & Evidence History (bounded FIFO per chunk):
  readonly #chunkEvidence = new Map<number, ChunkEvidenceHistory>();
  // Chunk state tracking:
  readonly #chunkStates = new Map<number, InboundChunkState>();
  // Watchdog timers per chunk:
  readonly #chunkTimers = new Map<number, ReturnType<typeof setTimeout>>();

  // Active transfer session:
  #session: InboundTransferSession | null = null;
  // Terminal state:
  #isTerminal = false;
  // Overall session timeout timer:
  #overallTimer: ReturnType<typeof setTimeout> | null = null;
  // Consecutive local handoff failures:
  #consecutiveLocalFailures = 0;
  // Outbound dispatch queue:
  readonly #outboundDispatchQueue: OutboundHandoffRequest[] = [];
  // Dispatch pump active flag:
  #isFlushingDispatchQueue = false;

  constructor(config: InboundPipelineConfig) {
    assertInboundTransportCapability(config.transportCapability);
    this.#authorizedCapability = config.transportCapability;

    const nowFn = config.nowFn ?? (() => Date.now());

    this.#config = {
      transportCapability: config.transportCapability,
      slidingWindowSize: config.slidingWindowSize ?? DEFAULT_SLIDING_WINDOW_SIZE,
      chunkTimeoutMs: config.chunkTimeoutMs ?? DEFAULT_CHUNK_TIMEOUT_MS,
      overallTimeoutMs: config.overallTimeoutMs ?? DEFAULT_OVERALL_TIMEOUT_MS,
      maxRetriesPerChunk: config.maxRetriesPerChunk ?? DEFAULT_MAX_RETRIES_PER_CHUNK,
      maxTransferRetries: config.maxTransferRetries ?? DEFAULT_MAX_TRANSFER_RETRIES,
      maxTransferContextTransitions:
        config.maxTransferContextTransitions ?? DEFAULT_MAX_TRANSFER_CONTEXT_TRANSITIONS,
      maxLocalRedispatchAttempts:
        config.maxLocalRedispatchAttempts ?? DEFAULT_MAX_LOCAL_REDISPATCH_ATTEMPTS,
      nowFn,
      onTransferComplete: config.onTransferComplete,
      onEmitMessage: config.onEmitMessage,
      onDiagnosticError: config.onDiagnosticError,
    };

    this.#assembler = new ChunkAssembler({
      maxSnapshotBytes: MAX_SNAPSHOT_BYTES,
      maxChunks: MAX_CHUNKS_PER_SNAPSHOT,
      maxChunkBytes: MAX_CHUNK_BYTES,
      minRawChunkBytes: MIN_RAW_CHUNK_BYTES,
    });

    this.#candidateRegistry = new CandidateOfferRegistry(nowFn);
  }

  /**
   * Registers an unforgeable AuthenticatedSupplierContext using an authorized InboundTransportCapability.
   * Context provenance is tracked in #validSupplierContexts: WeakSet.
   */
  public registerSupplierContext(
    supplierPeerId: string,
    connectionGeneration: number,
    sctpMaxMessageSize: number,
    capability: InboundTransportCapability,
    membershipEpoch?: number,
  ): AuthenticatedSupplierContext {
    assertInboundTransportCapability(capability, this.#authorizedCapability.sessionId);

    if (!supplierPeerId || typeof supplierPeerId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'supplierPeerId must be a non-empty string');
    }
    if (typeof connectionGeneration !== 'number' || connectionGeneration < 0) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'connectionGeneration must be a non-negative number');
    }

    const context: AuthenticatedSupplierContext = Object.freeze({
      supplierPeerId,
      sessionId: this.#authorizedCapability.sessionId,
      membershipEpoch: membershipEpoch ?? this.#session?.membershipEpoch ?? 1,
      connectionGeneration,
      sctpMaxMessageSize,
    });

    this.#validSupplierContexts.add(context);
    return context;
  }

  /**
   * Logically revokes a supplier context by deleting it from #activeSupplierContexts.
   */
  public revokeSupplierContext(
    supplierPeerId: string,
    connectionGeneration: number,
    capability: InboundTransportCapability,
  ): void {
    assertInboundTransportCapability(capability, this.#authorizedCapability.sessionId);

    const active = this.#activeSupplierContexts.get(supplierPeerId);
    if (active && (active.connectionGeneration === undefined || active.connectionGeneration === connectionGeneration)) {
      this.#activeSupplierContexts.delete(supplierPeerId);
    }
  }

  /**
   * Handles an inbound SNAPSHOT_OFFER message.
   */
  public handleOffer(
    message: ProtocolMessage<SnapshotOfferPayload>,
    context: AuthenticatedSupplierContext,
  ): { admitted: boolean; role: 'ACTIVE' | 'CANDIDATE' | 'REJECTED'; reason?: SnapshotAbortReason; detail?: string } {
    if (this.#isTerminal) {
      return { admitted: false, role: 'REJECTED', reason: 'LOCAL_ABORT', detail: 'Pipeline is terminal' };
    }

    // Provenance check
    if (!this.#validSupplierContexts.has(context)) {
      return { admitted: false, role: 'REJECTED', reason: 'IDENTITY_MISMATCH', detail: 'Unauthenticated context provenance' };
    }

    // Message validation
    if (message.messageType !== MessageType.SNAPSHOT_OFFER) {
      return { admitted: false, role: 'REJECTED', reason: 'OFFER_INCONSISTENT', detail: 'Invalid message type' };
    }
    if (message.senderPeerId !== context.supplierPeerId || message.payload.supplierPeerId !== context.supplierPeerId) {
      return { admitted: false, role: 'REJECTED', reason: 'IDENTITY_MISMATCH', detail: 'Supplier peer ID mismatch' };
    }

    const parseResult = SnapshotOfferPayloadSchema.safeParse(message.payload);
    if (!parseResult.success) {
      return { admitted: false, role: 'REJECTED', reason: 'OFFER_INCONSISTENT', detail: parseResult.error.message };
    }

    const offer = parseResult.data;
    if (offer.sessionId !== this.#authorizedCapability.sessionId) {
      return { admitted: false, role: 'REJECTED', reason: 'IDENTITY_MISMATCH', detail: 'Session ID mismatch' };
    }

    // Case 1: No active session -> initialize active transfer
    if (this.#session === null) {
      const transferId = generateUUID();
      const nominalChunkSizeBytes =
        offer.nominalChunkSizeBytes ?? Math.ceil(offer.totalSizeBytes / offer.totalChunks);

      try {
        this.#assembler.initTransfer({
          transferId,
          sessionId: offer.sessionId,
          snapshotId: offer.snapshotId,
          totalChunks: offer.totalChunks,
          totalSizeBytes: offer.totalSizeBytes,
          nominalChunkSizeBytes,
          expectedSha256Digest: offer.sha256Digest,
          supplierPeerId: context.supplierPeerId,
          membershipEpoch: offer.membershipEpoch,
          connectionGeneration: context.connectionGeneration,
        });
      } catch (err) {
        return {
          admitted: false,
          role: 'REJECTED',
          reason: 'LOCAL_ABORT',
          detail: `Assembler initialization failed: ${err instanceof Error ? err.message : String(err)}`,
        };
      }

      this.#activeSupplierContexts.set(context.supplierPeerId, context);

      const receivedChunks = new Set<number>();
      const inflightRequests = new Set<number>();
      const missingChunks = new Set<number>(Array.from({ length: offer.totalChunks }, (_, i) => i));
      const chunkRetries = new Map<number, number>();

      this.#session = {
        transferId,
        sessionId: offer.sessionId,
        supplierPeerId: context.supplierPeerId,
        membershipEpoch: offer.membershipEpoch,
        connectionGeneration: context.connectionGeneration ?? 0,
        state: 'ADMITTED',
        snapshotId: offer.snapshotId,
        totalSizeBytes: offer.totalSizeBytes,
        totalChunks: offer.totalChunks,
        nominalChunkSizeBytes,
        expectedSha256Digest: offer.sha256Digest,
        receivedChunks,
        inflightRequests,
        missingChunks,
        chunkRetries,
        cumulativeRetries: 0,
        contextTransitionCount: 0,
        offer: Object.freeze({ ...offer }),
        isTerminal: false,
      };

      for (let i = 0; i < offer.totalChunks; i++) {
        this.#chunkStates.set(i, 'UNREQUESTED');
        this.#chunkEvidence.set(i, {
          chunkIndex: i,
          attempts: [],
          hasActiveAcceptedAttempt: false,
        });
      }

      this.#overallTimer = setTimeout(() => {
        this.abortTransfer('TRANSFER_TIMEOUT', 'Overall transfer timeout exceeded (60,000ms)');
      }, this.#config.overallTimeoutMs);

      return { admitted: true, role: 'ACTIVE' };
    }

    // Case 2: Active transfer session already exists
    // If offer is from the same supplier but with newer generation, or from candidate peer:
    const isSameSupplier = context.supplierPeerId === this.#session.supplierPeerId;
    if (isSameSupplier) {
      if (
        context.connectionGeneration !== undefined &&
        this.#session.connectionGeneration !== undefined &&
        context.connectionGeneration <= this.#session.connectionGeneration
      ) {
        // Duplicate or stale offer from active supplier
        return { admitted: true, role: 'ACTIVE' };
      }
    }

    // Check candidate compatibility against active transfer
    const isCompatible =
      offer.sessionId === this.#session.sessionId &&
      offer.membershipEpoch === this.#session.membershipEpoch &&
      offer.snapshotId === this.#session.snapshotId &&
      offer.totalSizeBytes === this.#session.totalSizeBytes &&
      offer.totalChunks === this.#session.totalChunks &&
      offer.sha256Digest === this.#session.expectedSha256Digest;

    if (!isCompatible) {
      return {
        admitted: false,
        role: 'REJECTED',
        reason: 'OFFER_INCONSISTENT',
        detail: 'Candidate offer geometry conflicts with active transfer session',
      };
    }

    const regRes = this.#candidateRegistry.addCandidate(offer, context, this.#config.nowFn());
    if (regRes.admitted) {
      return { admitted: true, role: 'CANDIDATE' };
    } else {
      return { admitted: false, role: 'REJECTED', reason: regRes.reason, detail: regRes.detail };
    }
  }

  /**
   * Starts transferring chunks for an ADMITTED session.
   */
  public async startTransfer(): Promise<boolean> {
    if (this.#isTerminal || !this.#session || this.#session.state !== 'ADMITTED') {
      return false;
    }

    this.#session.state = 'TRANSFERRING';

    const windowSize = Math.min(this.#config.slidingWindowSize, this.#session.totalChunks);
    const initialIndices: number[] = [];
    for (let i = 0; i < windowSize; i++) {
      initialIndices.push(i);
      this.#chunkStates.set(i, 'REQUESTED');
      this.#session.inflightRequests.add(i);
    }

    const requestPayload: SnapshotRequestPayload = {
      transferId: this.#session.transferId,
      sessionId: this.#session.sessionId,
      membershipEpoch: this.#session.membershipEpoch,
      requesterPeerId: this.#authorizedCapability.authority ?? 'local-peer',
      supplierPeerId: this.#session.supplierPeerId,
      snapshotId: this.#session.snapshotId,
      requestedChunkWindow: windowSize,
      missingChunkIndices: initialIndices,
    };

    const handoffAttemptToken = generateUUID();
    const handoff: OutboundHandoffRequest = {
      message: {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_REQUEST,
        senderPeerId: requestPayload.requesterPeerId,
        sessionId: requestPayload.sessionId,
        membershipEpoch: requestPayload.membershipEpoch,
        messageId: generateUUID(),
        payload: requestPayload,
      },
      messageType: 'SNAPSHOT_REQUEST',
      transferId: this.#session.transferId,
      handoffAttemptToken,
      chunkIndices: initialIndices,
    };

    this.#outboundDispatchQueue.push(handoff);

    // Arm per-chunk watchdog timers
    for (const chunkIndex of initialIndices) {
      this.#armChunkTimer(chunkIndex);
    }

    await this.#flushOutboundDispatchQueue();
    return true;
  }

  /**
   * Records DataChannel buffer acceptance for an outbound request attempt.
   * Invariant: Once accepted, acceptance is irreversible.
   */
  public handleHandoffSuccess(
    transferId: string,
    chunkIndex: number,
    messageId: string,
    attemptToken: string,
    context: AuthenticatedSupplierContext,
  ): void {
    // 1. Session existence, terminal state, and transfer identity guards
    if (this.#isTerminal || !this.#session || this.#session.isTerminal) return;
    if (this.#session.transferId !== transferId) return;

    // 2. Context authentication & provenance
    if (!this.#validSupplierContexts.has(context)) return;
    if (this.#activeSupplierContexts.get(context.supplierPeerId) !== context) return;
    if (context.sessionId !== undefined && context.sessionId !== this.#session.sessionId) return;

    // 3. Attempt token and message identity validation
    if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim().length === 0) return;
    if (!messageId || typeof messageId !== 'string' || messageId.trim().length === 0) return;

    // 4. Strict chunkIndex boundary guard: non-negative integer within [0, totalChunks - 1]
    if (
      typeof chunkIndex !== 'number' ||
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= this.#session.totalChunks
    ) {
      return;
    }

    this.#consecutiveLocalFailures = 0;

    let history = this.#chunkEvidence.get(chunkIndex);
    if (!history) {
      history = {
        chunkIndex,
        attempts: [],
        hasActiveAcceptedAttempt: false,
      };
      this.#chunkEvidence.set(chunkIndex, history);
    }

    // Defensive check: total records cap (512)
    let totalRecords = 0;
    for (const h of this.#chunkEvidence.values()) {
      totalRecords += h.attempts.length;
    }
    if (totalRecords >= MAX_EVIDENCE_RECORDS) {
      this.abortTransfer(
        'LOCAL_ABORT',
        `MEMORY_EXCEEDED: Historical evidence records exceeded defensive cap of ${MAX_EVIDENCE_RECORDS}`,
      );
      return;
    }

    const record: AcceptedAttemptRecord = {
      attemptToken,
      transferId,
      chunkIndex,
      sessionId: context.sessionId ?? this.#session.sessionId,
      membershipEpoch: this.#session.membershipEpoch,
      supplierPeerId: context.supplierPeerId,
      connectionGeneration: context.connectionGeneration ?? 0,
      messageId,
      acceptedAtMs: this.#config.nowFn(),
      status: 'ACCEPTED_PENDING',
    };

    // FIFO bounded array: max 4 entries
    if (history.attempts.length >= 4) {
      history.attempts.shift();
    }
    history.attempts.push(record);
    history.hasActiveAcceptedAttempt = true;
  }

  /**
   * Handles local transport handoff failure.
   */
  public handleHandoffFailure(
    transferId: string,
    chunkIndex: number,
    messageId: string,
    attemptToken: string,
    error: unknown,
    context: AuthenticatedSupplierContext,
  ): void {
    if (this.#isTerminal || !this.#session || this.#session.isTerminal) return;
    if (this.#session.transferId !== transferId) return;
    if (!this.#validSupplierContexts.has(context)) return;
    if (this.#activeSupplierContexts.get(context.supplierPeerId) !== context) return;
    if (context.sessionId !== undefined && context.sessionId !== this.#session.sessionId) return;

    if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim().length === 0) return;
    if (!messageId || typeof messageId !== 'string' || messageId.trim().length === 0) return;

    if (
      typeof chunkIndex !== 'number' ||
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= this.#session.totalChunks
    ) {
      return;
    }

    // Invariant: If already accepted on DataChannel, DO NOT REVERSE
    const history = this.#chunkEvidence.get(chunkIndex);
    if (history) {
      const existing = history.attempts.find((a) => a.attemptToken === attemptToken);
      if (existing && existing.status !== 'RETIRED_INADMISSIBLE') {
        return; // Already accepted; ignore post-acceptance handoff failure
      }
    }

    this.#consecutiveLocalFailures++;
    if (this.#consecutiveLocalFailures >= this.#config.maxLocalRedispatchAttempts) {
      this.abortTransfer(
        'LOCAL_ABORT',
        `LOCAL_HANDOFF_FAILURE_EXCEEDED: ${this.#consecutiveLocalFailures} consecutive local dispatch failures`,
      );
    }
  }

  /**
   * Ingests an inbound SNAPSHOT_CHUNK protocol message.
   */
  public async handleChunk(
    message: ProtocolMessage<SnapshotChunkPayload>,
    context: AuthenticatedSupplierContext,
  ): Promise<{ accepted: boolean; duplicate: boolean; completed: boolean }> {
    if (this.#isTerminal || !this.#session) {
      return { accepted: false, duplicate: false, completed: false };
    }

    // 1. Authoritative active-map fence & context validation
    if (!this.#validSupplierContexts.has(context)) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (this.#activeSupplierContexts.get(context.supplierPeerId) !== context) {
      return { accepted: false, duplicate: false, completed: false };
    }

    // 2. Envelope & metadata validation
    if (message.messageType !== MessageType.SNAPSHOT_CHUNK) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (message.senderPeerId !== context.supplierPeerId) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (message.sessionId !== this.#session.sessionId) {
      return { accepted: false, duplicate: false, completed: false };
    }
    const messageEpoch = message.membershipEpoch ?? message.payload.membershipEpoch;
    if (messageEpoch !== this.#session.membershipEpoch) {
      return { accepted: false, duplicate: false, completed: false };
    }

    const payload = message.payload;
    if (payload.transferId !== this.#session.transferId) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (payload.sessionId !== this.#session.sessionId) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (payload.supplierPeerId !== context.supplierPeerId) {
      return { accepted: false, duplicate: false, completed: false };
    }
    if (payload.snapshotId !== this.#session.snapshotId) {
      return { accepted: false, duplicate: false, completed: false };
    }

    const chunkIndex = payload.chunkIndex;
    const history = this.#chunkEvidence.get(chunkIndex);
    if (!history) {
      return { accepted: false, duplicate: false, completed: false };
    }

    // 3. Historical acceptance authority check (6-tuple match)
    const matchingAttempt = history.attempts.find((a) =>
      a.transferId === payload.transferId &&
      a.chunkIndex === chunkIndex &&
      a.sessionId === payload.sessionId &&
      a.membershipEpoch === messageEpoch &&
      a.supplierPeerId === context.supplierPeerId &&
      a.connectionGeneration === (context.connectionGeneration ?? 0) &&
      a.status !== 'RETIRED_INADMISSIBLE'
    );

    if (!matchingAttempt) {
      // Unsolicited or Case B: REJECT AND DROP AT TRANSPORT BOUNDARY
      return { accepted: false, duplicate: false, completed: false };
    }

    // 4. Safe Base64 decode and SHA-256 pre-verification
    let rawBytes: Uint8Array;
    try {
      rawBytes = decodeBase64(payload.dataBase64);
    } catch {
      return { accepted: false, duplicate: false, completed: false };
    }

    if (rawBytes.byteLength !== payload.chunkSizeBytes) {
      return { accepted: false, duplicate: false, completed: false };
    }

    const computedSha256 = sha256(rawBytes);
    if (computedSha256 !== payload.chunkSha256) {
      // Transmission corrupted: drop frame, transfer does NOT abort, awaits timeout
      return { accepted: false, duplicate: false, completed: false };
    }

    const currentState = this.#chunkStates.get(chunkIndex);
    const envelopeContext: ChunkEnvelopeContext = {
      sessionId: message.sessionId,
      membershipEpoch: messageEpoch,
      senderPeerId: message.senderPeerId,
      connectionGeneration: context.connectionGeneration,
    };

    // 5. PATH B: Duplicate Verification Pipeline
    if (currentState === 'RECEIVED_VERIFIED') {
      if (history.verifiedChunkSha256 !== undefined && history.verifiedChunkSha256 !== computedSha256) {
        // FATAL CONFLICTING DUPLICATE -> immediate terminal abort
        await this.abortTransfer('CHUNK_CORRUPTED', `Conflicting duplicate chunk ${chunkIndex}`);
        return { accepted: false, duplicate: false, completed: false };
      }

      // Identical duplicate: call assembler for duplicate confirmation
      const res = this.#assembler.ingestChunk(payload, envelopeContext);
      if (res.ok && res.duplicate) {
        this.#clearChunkTimer(chunkIndex);
        return { accepted: true, duplicate: true, completed: false };
      }
      return { accepted: false, duplicate: true, completed: false };
    }

    // 6. PATH A: First-Time Ingestion & Reconciliation
    const receivedIndices = this.#assembler.getReceivedChunkIndices();
    const alreadyInAssembler = receivedIndices.includes(chunkIndex);

    if (alreadyInAssembler) {
      // Case A: Safe reconciliation
      const res = this.#assembler.ingestChunk(payload, envelopeContext);
      if (res.ok && res.duplicate) {
        this.#commitChunkSuccess(chunkIndex, computedSha256);
        return { accepted: true, duplicate: true, completed: false };
      } else if (!res.ok) {
        await this.abortTransfer(res.reason, res.detail);
        return { accepted: false, duplicate: false, completed: false };
      }
    }

    // Regular Path A ingestion
    const ingestRes = this.#assembler.ingestChunk(payload, envelopeContext);
    if (!ingestRes.ok) {
      if (ingestRes.reason === 'CHUNK_CORRUPTED') {
        // Transmission corrupted: drop frame without abort
        return { accepted: false, duplicate: false, completed: false };
      }
      await this.abortTransfer(ingestRes.reason, ingestRes.detail);
      return { accepted: false, duplicate: false, completed: false };
    }

    // Successful first-time ingestion commit
    this.#commitChunkSuccess(chunkIndex, computedSha256);

    // Check completion
    if (ingestRes.completed || this.#assembler.isComplete()) {
      const finalizeRes = this.#assembler.finalizeAssembly();
      if (!finalizeRes.ok) {
        await this.abortTransfer(finalizeRes.reason, finalizeRes.detail);
        return { accepted: false, duplicate: false, completed: false };
      }

      await this.#handleTransferCompletion(finalizeRes.snapshot);
      return { accepted: true, duplicate: false, completed: true };
    }

    // Advance sliding window and dispatch eligible missing chunks
    await this.#advanceSlidingWindow();
    return { accepted: true, duplicate: false, completed: false };
  }

  /**
   * Handles remote supplier abort message.
   */
  public handleRemoteAbort(
    message: ProtocolMessage<SnapshotAbortPayload>,
    context: AuthenticatedSupplierContext,
  ): void {
    if (this.#isTerminal) return;
    if (!this.#validSupplierContexts.has(context)) return;
    if (this.#activeSupplierContexts.get(context.supplierPeerId) !== context) return;
    if (!this.#session || message.payload.transferId !== this.#session.transferId) return;

    this.abortTransfer(
      message.payload.reason ?? 'SUPPLIER_REJECTED',
      message.payload.detail ?? 'Remote supplier aborted transfer',
    );
  }

  /**
   * Three-Phase Failover Protocol triggered upon supplier disconnect.
   */
  public async handleSupplierDisconnected(
    context: AuthenticatedSupplierContext,
    capability: InboundTransportCapability,
  ): Promise<{ failoverSuccess: boolean }> {
    // 1. Authenticate notification: strict referential equality + brand assert
    if (capability !== this.#authorizedCapability) {
      throw new SyncEngineError('UNAUTHORIZED', 'Invalid or unauthorized InboundTransportCapability');
    }
    assertInboundTransportCapability(capability, this.#authorizedCapability.sessionId);

    // Provenance check
    if (!this.#validSupplierContexts.has(context)) {
      return { failoverSuccess: false };
    }

    // Idempotent & stale checks
    if (!this.#session) {
      return { failoverSuccess: false };
    }
    if (context.supplierPeerId !== this.#session.supplierPeerId) {
      // Duplicate/stale notification from non-active supplier -> dropped as no-op
      return { failoverSuccess: false };
    }
    if (
      context.connectionGeneration !== undefined &&
      this.#session.connectionGeneration !== undefined &&
      context.connectionGeneration < this.#session.connectionGeneration
    ) {
      // Stale generation notification -> dropped as no-op
      return { failoverSuccess: false };
    }
    if (this.#session.state !== 'TRANSFERRING') {
      return { failoverSuccess: false };
    }

    // Transition budget check: MAX_TRANSFER_CONTEXT_TRANSITIONS = 8
    if (this.#session.contextTransitionCount >= this.#config.maxTransferContextTransitions) {
      await this.abortTransfer('SUPPLIER_DISCONNECTED', 'Context transition budget exhausted (8)');
      return { failoverSuccess: false };
    }

    // Select candidate
    const candidate = this.#candidateRegistry.findEligibleCandidate(
      {
        sessionId: this.#session.sessionId,
        membershipEpoch: this.#session.membershipEpoch,
        snapshotId: this.#session.snapshotId,
        totalSizeBytes: this.#session.totalSizeBytes,
        totalChunks: this.#session.totalChunks,
        nominalChunkSizeBytes: this.#session.nominalChunkSizeBytes,
        expectedSha256Digest: this.#session.expectedSha256Digest,
      },
      this.#config.nowFn(),
    );

    if (!candidate) {
      // No candidate available -> abort transfer
      await this.abortTransfer('SUPPLIER_DISCONNECTED', 'No eligible candidate offer found in registry');
      return { failoverSuccess: false };
    }

    const candidateContext = candidate.context;
    this.#validSupplierContexts.add(candidateContext);

    // PHASE 2: Assembler Commit Point
    const candidateParams: FailoverCandidateParams = {
      transferId: this.#session.transferId,
      supplierPeerId: candidate.offer.supplierPeerId,
      membershipEpoch: candidate.offer.membershipEpoch,
      connectionGeneration: candidateContext.connectionGeneration,
      snapshotId: candidate.offer.snapshotId,
      expectedSha256Digest: candidate.offer.sha256Digest,
      totalChunks: candidate.offer.totalChunks,
      totalSizeBytes: candidate.offer.totalSizeBytes,
      nominalChunkSizeBytes: candidate.offer.nominalChunkSizeBytes ?? this.#session.nominalChunkSizeBytes,
      sessionId: candidate.offer.sessionId,
    };

    const prepResult = this.#assembler.prepareForFailover(candidateParams);

    if (!prepResult.ok) {
      // Candidate rejected by assembler: pipeline remains unmutated
      this.#candidateRegistry.evict(candidateContext.supplierPeerId);
      return { failoverSuccess: false };
    }

    // Outcome B: SUCCESS -> IRREVERSIBLE COMMIT POINT
    try {
      // PHASE 3: Post-Commit Synchronous Finalization (Zero External Callbacks)
      // 1. Revoke old supplier context from active map
      this.#activeSupplierContexts.delete(this.#session.supplierPeerId);

      // 2. Retire in-flight attempts to CANCELLED
      for (const hist of this.#chunkEvidence.values()) {
        for (const attempt of hist.attempts) {
          if (attempt.status === 'ACCEPTED_PENDING') {
            attempt.status = 'RETIRED_INADMISSIBLE';
          }
        }
        hist.hasActiveAcceptedAttempt = false;
      }

      // 3. Clear active chunk response and watchdog timers
      for (const timer of this.#chunkTimers.values()) {
        clearTimeout(timer);
      }
      this.#chunkTimers.clear();

      // 4. Purge old historical evidence
      this.#chunkEvidence.clear();

      // 5. Update session identity
      this.#session.supplierPeerId = candidateContext.supplierPeerId;
      this.#session.connectionGeneration = candidateContext.connectionGeneration ?? 0;

      // 6. Increment context transition counter
      this.#session.contextTransitionCount++;

      // 7. Publish candidate context to active map
      this.#activeSupplierContexts.set(candidateContext.supplierPeerId, candidateContext);

      // 8. Reset scheduling state: unverified chunks reset to UNREQUESTED
      const receivedSet = new Set(this.#assembler.getReceivedChunkIndices());
      this.#session.inflightRequests.clear();
      this.#session.receivedChunks.clear();
      this.#session.missingChunks.clear();

      for (let i = 0; i < this.#session.totalChunks; i++) {
        if (receivedSet.has(i)) {
          this.#session.receivedChunks.add(i);
          this.#chunkStates.set(i, 'RECEIVED_VERIFIED');
          this.#chunkEvidence.set(i, {
            chunkIndex: i,
            attempts: [],
            hasActiveAcceptedAttempt: false,
          });
        } else {
          this.#session.missingChunks.add(i);
          this.#chunkStates.set(i, 'UNREQUESTED');
          this.#chunkEvidence.set(i, {
            chunkIndex: i,
            attempts: [],
            hasActiveAcceptedAttempt: false,
          });
        }
      }

      // 9. Enqueue window requests into #outboundDispatchQueue
      this.#candidateRegistry.evict(candidateContext.supplierPeerId);
      this.#enqueueWindowRequests();

      // DISPATCH BOUNDARY: Drain queue strictly after Phase 3 completion
      await this.#flushOutboundDispatchQueue();
      return { failoverSuccess: true };
    } catch (phase3Error) {
      // Synchronous failure during Phase 3 -> Emergency Fail-Closed Terminal Abort
      await this.abortTransfer('LOCAL_ABORT', 'FAILOVER_COMMIT_FINALIZATION_FAILED');
      return { failoverSuccess: false };
    }
  }

  /**
   * Reconciled 10-step fail-closed terminal-abort procedure.
   */
  public async abortTransfer(reason: SnapshotAbortReason, detail?: string): Promise<void> {
    if (this.#isTerminal) return;

    // Step 1: Set terminal fence
    this.#isTerminal = true;
    if (this.#session) {
      this.#session.state = 'ABORTED';
      this.#session.failureReason = reason;
      this.#session.failureDetail = detail;
      this.#session.isTerminal = true;
    }

    // Step 2: Purge outbound dispatch queue (R18-3)
    this.#outboundDispatchQueue.length = 0;

    // Step 3: Block inbound admissions (governed by #isTerminal === true)

    // Step 4: Disarm pipeline timers & delegate registry cleanup (R18-2)
    try {
      if (this.#overallTimer !== null) {
        clearTimeout(this.#overallTimer);
        this.#overallTimer = null;
      }
      for (const timer of this.#chunkTimers.values()) {
        clearTimeout(timer);
      }
      this.#chunkTimers.clear();
      this.#candidateRegistry.clear();
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:disarmTimers');
    }

    // Step 5: Retire in-flight attempts
    try {
      for (const hist of this.#chunkEvidence.values()) {
        for (const attempt of hist.attempts) {
          if (attempt.status === 'ACCEPTED_PENDING') {
            attempt.status = 'RETIRED_INADMISSIBLE';
          }
        }
        hist.hasActiveAcceptedAttempt = false;
      }
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:retireAttempts');
    }

    // Step 6: Purge historical evidence
    try {
      this.#chunkEvidence.clear();
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:purgeEvidence');
    }

    // Step 7: Authoritative Active-Map Clear (Logical Revocation, R18-1)
    try {
      this.#activeSupplierContexts.clear();
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:clearActiveContexts');
    }

    // Step 8: Dispose ChunkAssembler
    try {
      this.#assembler.dispose();
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:disposeAssembler');
    }

    // Step 9: Best-effort isolation completed via try/catch blocks

    // Step 10: Post-Fence External Callbacks (R18-3)
    try {
      if (this.#session) {
        const abortPayload: SnapshotAbortPayload = {
          transferId: this.#session.transferId,
          sessionId: this.#session.sessionId,
          membershipEpoch: this.#session.membershipEpoch,
          senderPeerId: this.#authorizedCapability.authority ?? 'local-peer',
          snapshotId: this.#session.snapshotId,
          reason,
          detail: detail ? detail.slice(0, 256) : undefined,
        };

        const abortHandoff: OutboundHandoffRequest = {
          message: {
            protocolVersion: PROTOCOL_VERSION,
            messageType: MessageType.SNAPSHOT_ABORT,
            senderPeerId: abortPayload.senderPeerId,
            sessionId: abortPayload.sessionId,
            membershipEpoch: abortPayload.membershipEpoch,
            messageId: generateUUID(),
            payload: abortPayload,
          },
          messageType: 'SNAPSHOT_ABORT',
          transferId: this.#session.transferId,
          handoffAttemptToken: generateUUID(),
        };

        await this.#config.onEmitMessage(abortHandoff);
      }
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'abortTransfer:emitAbort');
    }
  }

  public getActiveSession(): InboundTransferSession | null {
    return this.#session;
  }

  public dispose(): void {
    if (this.#isTerminal) {
      this.#candidateRegistry.dispose();
      this.#assembler.dispose();
      return;
    }
    this.abortTransfer('LOCAL_ABORT', 'Pipeline disposed');
  }

  // ── Private Internal Helpers ───────────────────────────────────────────

  #commitChunkSuccess(chunkIndex: number, computedSha256: string): void {
    this.#chunkStates.set(chunkIndex, 'RECEIVED_VERIFIED');
    this.#clearChunkTimer(chunkIndex);

    if (this.#session) {
      this.#session.receivedChunks.add(chunkIndex);
      this.#session.missingChunks.delete(chunkIndex);
      this.#session.inflightRequests.delete(chunkIndex);
    }

    const history = this.#chunkEvidence.get(chunkIndex);
    if (history) {
      history.verifiedChunkSha256 = computedSha256;
      for (const attempt of history.attempts) {
        if (attempt.status === 'ACCEPTED_PENDING' || attempt.status === 'ACCEPTED_TIMED_OUT' || attempt.status === 'SUPERSEDED_ADMISSIBLE') {
          attempt.status = 'FULFILLED_DUPLICATE_ADMISSIBLE';
        }
      }
      history.hasActiveAcceptedAttempt = false;
    }
  }

  async #handleTransferCompletion(assembled: AssembledSnapshot): Promise<void> {
    if (!this.#session) return;

    this.#session.state = 'COMPLETED';
    this.#isTerminal = true;

    if (this.#overallTimer !== null) {
      clearTimeout(this.#overallTimer);
      this.#overallTimer = null;
    }
    for (const timer of this.#chunkTimers.values()) {
      clearTimeout(timer);
    }
    this.#chunkTimers.clear();
    this.#candidateRegistry.clear();

    let parsedJson: unknown;
    try {
      const jsonText = new TextDecoder('utf-8', { fatal: true }).decode(assembled.buffer);
      parsedJson = JSON.parse(jsonText);
    } catch (err) {
      await this.abortTransfer('CHUNK_CORRUPTED', `Failed to parse assembled snapshot JSON: ${err instanceof Error ? err.message : String(err)}`);
      return;
    }

    let validatedSnapshot: StateSnapshotPayloadV2;
    try {
      validatedSnapshot = SnapshotSerializer.validateSnapshot(parsedJson);
    } catch (err) {
      await this.abortTransfer('CHUNK_CORRUPTED', `Snapshot post-assembly validation failed: ${err instanceof Error ? err.message : String(err)}`);
      return;
    }

    const transferResult: SnapshotTransferResult = {
      transferId: this.#session.transferId,
      snapshotId: assembled.snapshotId,
      sessionId: this.#session.sessionId,
      membershipEpoch: this.#session.membershipEpoch,
      authorPeerId: this.#session.offer.authorPeerId,
      totalSizeBytes: assembled.totalSizeBytes,
      sha256Digest: assembled.sha256Digest,
      snapshot: deepFreeze(validatedSnapshot),
      rawBytes: assembled.buffer.slice(0),
    };

    try {
      await this.#config.onTransferComplete(transferResult);
    } catch (err) {
      this.#config.onDiagnosticError?.(err, 'onTransferComplete');
    }
  }

  async #advanceSlidingWindow(): Promise<void> {
    if (this.#isTerminal || !this.#session || this.#session.state !== 'TRANSFERRING') {
      return;
    }

    this.#enqueueWindowRequests();
    await this.#flushOutboundDispatchQueue();
  }

  #enqueueWindowRequests(): void {
    if (!this.#session) return;

    // Determine lowest unreceived chunk index (base pointer)
    let basePointer = -1;
    for (let i = 0; i < this.#session.totalChunks; i++) {
      if (!this.#session.receivedChunks.has(i)) {
        basePointer = i;
        break;
      }
    }

    if (basePointer === -1) return; // All chunks received

    const windowEnd = Math.min(basePointer + this.#config.slidingWindowSize, this.#session.totalChunks);

    for (let i = basePointer; i < windowEnd; i++) {
      if (!this.#session.receivedChunks.has(i) && !this.#session.inflightRequests.has(i)) {
        this.#chunkStates.set(i, 'REQUESTED');
        this.#session.inflightRequests.add(i);

        const chunkReqPayload: SnapshotChunkRequestPayload = {
          transferId: this.#session.transferId,
          sessionId: this.#session.sessionId,
          membershipEpoch: this.#session.membershipEpoch,
          requesterPeerId: this.#authorizedCapability.authority ?? 'local-peer',
          supplierPeerId: this.#session.supplierPeerId,
          snapshotId: this.#session.snapshotId,
          missingChunkIndices: [i],
        };

        const handoff: OutboundHandoffRequest = {
          message: {
            protocolVersion: PROTOCOL_VERSION,
            messageType: MessageType.SNAPSHOT_CHUNK_REQUEST,
            senderPeerId: chunkReqPayload.requesterPeerId,
            sessionId: chunkReqPayload.sessionId,
            membershipEpoch: chunkReqPayload.membershipEpoch,
            messageId: generateUUID(),
            payload: chunkReqPayload,
          },
          messageType: 'SNAPSHOT_CHUNK_REQUEST',
          transferId: this.#session.transferId,
          handoffAttemptToken: generateUUID(),
          chunkIndices: [i],
        };

        this.#outboundDispatchQueue.push(handoff);
        this.#armChunkTimer(i);
      }
    }
  }

  #armChunkTimer(chunkIndex: number): void {
    this.#clearChunkTimer(chunkIndex);

    const timer = setTimeout(() => {
      this.#handleChunkTimeout(chunkIndex);
    }, this.#config.chunkTimeoutMs);

    this.#chunkTimers.set(chunkIndex, timer);
  }

  #clearChunkTimer(chunkIndex: number): void {
    const existing = this.#chunkTimers.get(chunkIndex);
    if (existing !== undefined) {
      clearTimeout(existing);
      this.#chunkTimers.delete(chunkIndex);
    }
  }

  async #handleChunkTimeout(chunkIndex: number): Promise<void> {
    if (this.#isTerminal || !this.#session || this.#session.state !== 'TRANSFERRING') {
      return;
    }

    if (this.#session.receivedChunks.has(chunkIndex)) {
      return;
    }

    const currentRetries = (this.#session.chunkRetries.get(chunkIndex) ?? 0) + 1;
    this.#session.chunkRetries.set(chunkIndex, currentRetries);
    this.#session.cumulativeRetries++;

    // Mark previous accepted attempt as timed out
    const history = this.#chunkEvidence.get(chunkIndex);
    if (history) {
      for (const attempt of history.attempts) {
        if (attempt.status === 'ACCEPTED_PENDING') {
          attempt.status = 'ACCEPTED_TIMED_OUT';
        }
      }
      history.hasActiveAcceptedAttempt = false;
    }

    // Budget checks: max 3 retries per chunk (4 attempts total), max 10 cumulative
    if (currentRetries > this.#config.maxRetriesPerChunk || this.#session.cumulativeRetries > this.#config.maxTransferRetries) {
      await this.abortTransfer('RETRY_BUDGET_EXHAUSTED', `Retry budget exhausted for chunk ${chunkIndex} (retries: ${currentRetries}, cumulative: ${this.#session.cumulativeRetries})`);
      return;
    }

    // Re-dispatch chunk request
    const chunkReqPayload: SnapshotChunkRequestPayload = {
      transferId: this.#session.transferId,
      sessionId: this.#session.sessionId,
      membershipEpoch: this.#session.membershipEpoch,
      requesterPeerId: this.#authorizedCapability.authority ?? 'local-peer',
      supplierPeerId: this.#session.supplierPeerId,
      snapshotId: this.#session.snapshotId,
      missingChunkIndices: [chunkIndex],
    };

    const handoff: OutboundHandoffRequest = {
      message: {
        protocolVersion: PROTOCOL_VERSION,
        messageType: MessageType.SNAPSHOT_CHUNK_REQUEST,
        senderPeerId: chunkReqPayload.requesterPeerId,
        sessionId: chunkReqPayload.sessionId,
        membershipEpoch: chunkReqPayload.membershipEpoch,
        messageId: generateUUID(),
        payload: chunkReqPayload,
      },
      messageType: 'SNAPSHOT_CHUNK_REQUEST',
      transferId: this.#session.transferId,
      handoffAttemptToken: generateUUID(),
      chunkIndices: [chunkIndex],
    };

    this.#outboundDispatchQueue.push(handoff);
    this.#armChunkTimer(chunkIndex);

    await this.#flushOutboundDispatchQueue();
  }

  async #flushOutboundDispatchQueue(): Promise<void> {
    if (this.#isFlushingDispatchQueue) return;
    this.#isFlushingDispatchQueue = true;

    try {
      while (this.#outboundDispatchQueue.length > 0) {
        if (this.#isTerminal) {
          this.#outboundDispatchQueue.length = 0;
          break;
        }

        const item = this.#outboundDispatchQueue.shift();
        if (!item) break;

        try {
          await this.#config.onEmitMessage(item);
        } catch (err) {
          this.#config.onDiagnosticError?.(err, 'onEmitMessage');
          if (item.chunkIndices && item.chunkIndices.length > 0 && item.chunkIndices[0] !== undefined && this.#session) {
            const context = this.#activeSupplierContexts.get(this.#session.supplierPeerId);
            if (context) {
              this.handleHandoffFailure(
                item.transferId,
                item.chunkIndices[0],
                item.message.messageId,
                item.handoffAttemptToken,
                err,
                context,
              );
            }
          }
        }
      }
    } finally {
      this.#isFlushingDispatchQueue = false;
    }
  }
}
