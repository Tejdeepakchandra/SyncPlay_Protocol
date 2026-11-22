/**
 * @module @syncplay/sync-engine/recovery
 *
 * Implements Phase 7.5 M4.2c Snapshot Outbound Pipeline:
 * - Deterministic, bounded outbound snapshot-transfer orchestration.
 * - Formats SNAPSHOT_OFFER ProtocolMessages satisfying SnapshotOfferPayloadSchema.
 * - Formats SNAPSHOT_CHUNK ProtocolMessages with mandatory exact UTF-8 complete-message
 *   byte length measurement at the send boundary (new TextEncoder().encode(serializedJson).byteLength).
 * - Enforces partition immutability on send-boundary overshoot (never resizes chunk slices mid-transfer).
 * - Coordinates sequential snapshot preparation via SnapshotPreparationQueue (concurrency admission <= 2).
 * - Manages explicit artifact lease lifecycle: retains lease across final-chunk production and
 *   retransmissions; guarantees release on completion, cancellation, timeout, or failure.
 * - Enforces bounded in-flight transmission window (default W = 4) and deterministic retry limits.
 * - Enforces strict capability-gated AuthenticatedRecipientContext fencing on all remote operations:
 *   validates recipientPeerId, sessionId, membershipEpoch, and connectionGeneration.
 * - Provides bounded, idempotent handoff-failure handling to prevent stranded in-flight reservations.
 * - Guarantees terminal state machine transitions (PREPARING, PREPARED, ACTIVE, WAITING_ACK,
 *   COMPLETED, ABORTED, CANCELLED, TIMED_OUT) with immunity to late or stale callbacks.
 */

import {
  PROTOCOL_VERSION,
  MessageType,
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  SnapshotOfferPayloadSchema,
  SnapshotChunkPayloadSchema,
  SnapshotChunkEnvelopeSchema,
  type SnapshotOfferPayload,
  type SnapshotChunkPayload,
  type SnapshotAbortReason,
  type ProtocolMessage,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import { SerializedSnapshotArtifact } from './snapshot-serializer.js';
import {
  SnapshotChunkProducer,
  type PartitionedSnapshot,
  type ProducedChunkMessage,
} from './snapshot-chunk-producer.js';
import { SnapshotPreparationQueue } from './snapshot-preparation-queue.js';

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

/**
 * Capability held by trusted transport adapter / composition root.
 * Required to register unforgeable AuthenticatedRecipientContext instances.
 */
export interface OutboundTransportCapability {
  readonly authority: string;
  readonly sessionId: string;
  readonly token: string;
}

/**
 * Creates an authorized OutboundTransportCapability for trusted transport adapter initialization.
 */
export function createOutboundTransportCapability(
  authority: string,
  sessionId: string,
  token?: string
): OutboundTransportCapability {
  if (!authority || typeof authority !== 'string') {
    throw new SyncEngineError('INVALID_ARGUMENT', 'authority must be a non-empty string');
  }
  if (!sessionId || typeof sessionId !== 'string') {
    throw new SyncEngineError('INVALID_ARGUMENT', 'sessionId must be a non-empty string');
  }
  return Object.freeze({
    authority,
    sessionId,
    token: token ?? `outbound-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
}

/**
 * Authenticated recipient context derived strictly from authenticated transport connection state.
 * Validated against transfer records to enforce recipient, session, epoch, and generation fencing.
 */
export interface AuthenticatedRecipientContext {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration: number;
}

/**
 * Parameters for registering an inbound recipient context.
 */
export interface InboundRecipientContextParams {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration: number;
}

/**
 * Context required to format a SNAPSHOT_OFFER protocol message.
 */
export interface OutboundOfferContext {
  readonly transferId: string;
  readonly supplierPeerId: string;
  readonly recipientPeerId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration?: number;
  readonly snapshotFrontier?: Record<string, number>;
  readonly maxLogicalTimestamp?: number;
}

/**
 * Context required to format a SNAPSHOT_CHUNK protocol message.
 */
export interface OutboundChunkContext {
  readonly transferId: string;
  readonly supplierPeerId: string;
  readonly recipientPeerId: string;
  readonly membershipEpoch: number;
  readonly sctpMaxMessageSize: number;
  readonly connectionGeneration?: number;
}

/**
 * Result of preparing a SNAPSHOT_CHUNK protocol message at the send boundary.
 */
export type OutboundChunkResult =
  | {
      readonly ok: true;
      readonly protocolMessage: ProtocolMessage<SnapshotChunkPayload>;
      readonly serializedJson: string;
      readonly byteLength: number;
    }
  | {
      readonly ok: false;
      readonly reason: SnapshotAbortReason;
      readonly detail: string;
    };

/**
 * Produced chunk frame decorated with attempt identity metadata.
 */
export interface OutboundPreparedChunk extends ProducedChunkMessage {
  readonly messageId: string;
  readonly attemptSeq: number;
  readonly attemptToken: string;
}

/**
 * Finite state machine states for an outbound transfer session.
 */
export type OutboundTransferState =
  | 'PREPARING'
  | 'PREPARED'
  | 'ACTIVE'
  | 'WAITING_ACK'
  | 'COMPLETED'
  | 'ABORTED'
  | 'CANCELLED'
  | 'TIMED_OUT';

/**
 * Public descriptor of an outbound transfer session.
 */
export interface OutboundTransferSession {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly supplierPeerId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration: number;
  readonly sctpMaxMessageSize: number;
  readonly state: OutboundTransferState;
  readonly totalChunks: number;
  readonly totalSizeBytes: number;
  readonly nominalChunkSizeBytes: number;
  readonly acknowledgedChunks: ReadonlySet<number>;
  readonly inflightChunks: ReadonlySet<number>;
  readonly transmittedChunks: ReadonlySet<number>;
  readonly retryCounts: ReadonlyMap<number, number>;
  readonly confirmedRetries: ReadonlyMap<number, number>;
  readonly activeAttempts: ReadonlyMap<number, { messageId: string; attemptSeq: number; attemptToken: string; isRetransmission: boolean }>;
  readonly handedOffAttempts: ReadonlySet<string>;
  readonly failedAttempts: ReadonlySet<string>;
  readonly offerMessage?: ProtocolMessage<SnapshotOfferPayload>;
  readonly failureReason?: SnapshotAbortReason;
  readonly failureDetail?: string;
  readonly isTerminal: boolean;
}

/**
 * Configuration options for SnapshotOutboundPipeline.
 */
export interface OutboundPipelineConfig {
  readonly transportCapability?: OutboundTransportCapability;
  readonly preparationQueue?: SnapshotPreparationQueue;
  readonly maxActiveTransfers?: number;
  readonly slidingWindowSize?: number;
  readonly maxRetriesPerChunk?: number;
  readonly transferTimeoutMs?: number;
  readonly nowFn?: () => number;
}

/**
 * Parameters to initiate an outbound snapshot transfer.
 */
export interface StartTransferParams {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly supplierPeerId: string;
  readonly sctpMaxMessageSize: number;
  readonly membershipEpoch: number;
  readonly connectionGeneration?: number;
  readonly captureAndSerializeFn?: () => Promise<SerializedSnapshotArtifact> | SerializedSnapshotArtifact;
  readonly artifact?: SerializedSnapshotArtifact;
  readonly autoDisposeArtifact?: boolean;
  readonly snapshotFrontier?: Record<string, number>;
  readonly maxLogicalTimestamp?: number;
}

/**
 * Internal state holder for an active transfer.
 */
interface InternalTransferRecord {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly supplierPeerId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration: number;
  readonly sctpMaxMessageSize: number;
  readonly snapshotFrontier: Record<string, number>;
  readonly maxLogicalTimestamp: number;
  readonly startTimeMs: number;
  readonly ownsArtifact: boolean;
  state: OutboundTransferState;
  artifact?: SerializedSnapshotArtifact;
  producer?: SnapshotChunkProducer;
  offerMessage?: ProtocolMessage<SnapshotOfferPayload>;
  failureReason?: SnapshotAbortReason;
  failureDetail?: string;
  readonly acknowledgedChunks: Set<number>;
  readonly inflightChunks: Set<number>;
  readonly transmittedChunks: Set<number>;
  readonly handedOffChunks: Set<number>;
  readonly handedOffAttempts: Set<string>;
  readonly failedAttempts: Set<string>;
  readonly activeAttempts: Map<number, { messageId: string; attemptSeq: number; attemptToken: string; isRetransmission: boolean }>;
  readonly attemptCounters: Map<number, number>;
  readonly retryCounts: Map<number, number>;
  readonly confirmedRetries: Map<number, number>;
  readonly preparedFrames: Map<number, ProducedChunkMessage>;
  readonly chunkMessageIds: Map<number, string>;
  readonly lastHandedOffMessageIds: Map<number, string>;
  leaseHeld: boolean;
  timeoutTimer?: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Creates an authenticated recipient transport context registered with the target outbound pipeline.
 * Requires the authorized OutboundTransportCapability held by the trusted transport adapter.
 */
export function createAuthenticatedRecipientContext(
  pipeline: SnapshotOutboundPipeline,
  params: InboundRecipientContextParams,
  capability: OutboundTransportCapability
): AuthenticatedRecipientContext {
  if (!pipeline || !(pipeline instanceof SnapshotOutboundPipeline)) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'Valid SnapshotOutboundPipeline instance is required');
  }
  return pipeline.registerInboundRecipientContext(params, capability);
}

/**
 * Orchestrator for outbound state recovery transfers.
 * Integrates SnapshotPreparationQueue, SnapshotSerializer, and SnapshotChunkProducer
 * into a bounded, deterministic, lifecycle-safe, capability-fenced pipeline.
 */
export class SnapshotOutboundPipeline {
  public static readonly DEFAULT_MAX_ACTIVE_TRANSFERS = 2;
  public static readonly DEFAULT_SLIDING_WINDOW_SIZE = 4;
  public static readonly DEFAULT_MAX_RETRIES_PER_CHUNK = 3;
  public static readonly DEFAULT_TRANSFER_TIMEOUT_MS = 30_000;
  public static readonly MAX_TERMINAL_HISTORY_ENTRIES = 32;

  // ─────────────────────────────────────────────────────────────
  // Static Wire Message Formatters (Contract Specification 3.4)
  // ─────────────────────────────────────────────────────────────

  /**
   * Formats a SNAPSHOT_OFFER ProtocolMessage for transmission to a recipient peer.
   * Validates metadata against SnapshotOfferPayloadSchema and ensures epoch consistency.
   */
  public static createOfferMessage(
    partition: PartitionedSnapshot,
    context: OutboundOfferContext
  ): ProtocolMessage<SnapshotOfferPayload> {
    if (!partition) {
      throw new SyncEngineError('INVALID_ARGUMENT', 'PartitionedSnapshot is required');
    }
    if (!context || typeof context !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'OutboundOfferContext is required');
    }
    if (!context.transferId || typeof context.transferId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'Valid transferId string is required');
    }
    if (partition.artifact.isDisposed) {
      throw new SyncEngineError('ARTIFACT_DISPOSED', 'Cannot create offer from disposed artifact');
    }

    if (context.membershipEpoch !== partition.artifact.membershipEpoch) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Offer context membershipEpoch (${context.membershipEpoch}) does not match artifact epoch (${partition.artifact.membershipEpoch})`
      );
    }

    const offerPayload: SnapshotOfferPayload = {
      offerId: context.transferId,
      sessionId: partition.artifact.sessionId,
      membershipEpoch: context.membershipEpoch,
      supplierPeerId: context.supplierPeerId,
      snapshotId: partition.artifact.snapshotId,
      authorPeerId: partition.artifact.authorPeerId,
      snapshotFrontier: context.snapshotFrontier ?? {},
      maxLogicalTimestamp: context.maxLogicalTimestamp ?? 0,
      totalSizeBytes: partition.artifact.totalSizeBytes,
      totalChunks: partition.totalChunks,
      nominalChunkSizeBytes: partition.nominalChunkSizeBytes,
      sha256Digest: partition.artifact.sha256Digest,
    };

    const parseResult = SnapshotOfferPayloadSchema.safeParse(offerPayload);
    if (!parseResult.success) {
      throw new SyncEngineError(
        'INVALID_PAYLOAD',
        `Generated offer payload failed validation: ${parseResult.error.message}`
      );
    }

    const message: ProtocolMessage<SnapshotOfferPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_OFFER,
      sessionId: partition.artifact.sessionId,
      senderPeerId: context.supplierPeerId,
      messageId: `msg-${generateUUID()}`,
      membershipEpoch: context.membershipEpoch,
      connectionGeneration: context.connectionGeneration ?? 1,
      payload: parseResult.data,
    };

    return message;
  }

  /**
   * Formats a SNAPSHOT_CHUNK ProtocolMessage with mandatory exact complete-message
   * UTF-8 byte length measurement at the send boundary.
   *
   * Invariant: Never shrinks an individual chunk slice. If the exact complete-message
   * UTF-8 byte length exceeds context.sctpMaxMessageSize, returns ok: false with
   * reason: 'TRANSPORT_LIMIT_EXCEEDED'.
   */
  public static prepareChunkMessage(
    partition: PartitionedSnapshot,
    chunkIndex: number,
    context: OutboundChunkContext
  ): OutboundChunkResult {
    if (!partition) {
      return {
        ok: false,
        reason: 'LOCAL_ABORT',
        detail: 'PartitionedSnapshot is required',
      };
    }
    if (!context || typeof context !== 'object') {
      return {
        ok: false,
        reason: 'LOCAL_ABORT',
        detail: 'OutboundChunkContext is required',
      };
    }
    if (partition.artifact.isDisposed) {
      return {
        ok: false,
        reason: 'LOCAL_ABORT',
        detail: 'Underlying SerializedSnapshotArtifact has been disposed',
      };
    }
    if (context.membershipEpoch !== partition.artifact.membershipEpoch) {
      return {
        ok: false,
        reason: 'EPOCH_MISMATCH',
        detail: `Context epoch ${context.membershipEpoch} does not match artifact epoch ${partition.artifact.membershipEpoch}`,
      };
    }

    if (
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= partition.totalChunks
    ) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Requested chunkIndex ${chunkIndex} is out of bounds [0, ${partition.totalChunks})`,
      };
    }

    const slice = partition.getSlice(chunkIndex);
    if (!slice) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Slice descriptor missing at index ${chunkIndex}`,
      };
    }

    let dataBase64: string;
    try {
      dataBase64 = partition.encodeChunkBase64(chunkIndex);
    } catch (err) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Failed to encode chunk Base64: ${err instanceof Error ? err.message : String(err)}`,
      };
    }

    const rawPayload: SnapshotChunkPayload = {
      transferId: context.transferId,
      sessionId: partition.artifact.sessionId,
      membershipEpoch: context.membershipEpoch,
      supplierPeerId: context.supplierPeerId,
      snapshotId: partition.artifact.snapshotId,
      chunkIndex,
      totalChunks: partition.totalChunks,
      chunkSizeBytes: slice.chunkSizeBytes,
      totalSizeBytes: partition.artifact.totalSizeBytes,
      chunkSha256: slice.chunkSha256,
      dataBase64,
    };

    const payloadResult = SnapshotChunkPayloadSchema.safeParse(rawPayload);
    if (!payloadResult.success) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Chunk payload schema validation failed: ${payloadResult.error.message}`,
      };
    }

    const rawMessage: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: payloadResult.data.sessionId,
      senderPeerId: context.supplierPeerId,
      messageId: `msg-${generateUUID()}`,
      membershipEpoch: payloadResult.data.membershipEpoch,
      connectionGeneration: context.connectionGeneration ?? 1,
      payload: payloadResult.data,
    };

    const envelopeResult = SnapshotChunkEnvelopeSchema.safeParse(rawMessage);
    if (!envelopeResult.success) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Chunk envelope validation failed: ${envelopeResult.error.message}`,
      };
    }

    const serializedJson = JSON.stringify(envelopeResult.data);
    const byteLength = new TextEncoder().encode(serializedJson).byteLength;

    if (byteLength > context.sctpMaxMessageSize) {
      return {
        ok: false,
        reason: 'TRANSPORT_LIMIT_EXCEEDED',
        detail: `Complete serialized chunk envelope byte length (${byteLength} B) exceeds negotiated SCTP limit (${context.sctpMaxMessageSize} B)`,
      };
    }

    return {
      ok: true,
      protocolMessage: envelopeResult.data as ProtocolMessage<SnapshotChunkPayload>,
      serializedJson,
      byteLength,
    };
  }

  // ─────────────────────────────────────────────────────────────
  // Instance-Based Outbound Transfer Pipeline Orchestrator
  // ─────────────────────────────────────────────────────────────

  readonly #authorizedCapability?: OutboundTransportCapability;
  readonly #validRecipientContexts = new WeakSet<AuthenticatedRecipientContext>();
  readonly #preparationQueue: SnapshotPreparationQueue;
  readonly #maxActiveTransfers: number;
  readonly #slidingWindowSize: number;
  readonly #maxRetriesPerChunk: number;
  readonly #transferTimeoutMs: number;
  readonly #now: () => number;

  #isDisposed = false;
  readonly #transfers = new Map<string, InternalTransferRecord>();

  constructor(config: OutboundPipelineConfig = {}) {
    this.#authorizedCapability = config.transportCapability
      ? Object.freeze(config.transportCapability)
      : undefined;
    this.#preparationQueue = config.preparationQueue ?? new SnapshotPreparationQueue();
    this.#maxActiveTransfers = config.maxActiveTransfers ?? SnapshotOutboundPipeline.DEFAULT_MAX_ACTIVE_TRANSFERS;
    this.#slidingWindowSize = config.slidingWindowSize ?? SnapshotOutboundPipeline.DEFAULT_SLIDING_WINDOW_SIZE;
    this.#maxRetriesPerChunk = config.maxRetriesPerChunk ?? SnapshotOutboundPipeline.DEFAULT_MAX_RETRIES_PER_CHUNK;
    this.#transferTimeoutMs = config.transferTimeoutMs ?? SnapshotOutboundPipeline.DEFAULT_TRANSFER_TIMEOUT_MS;
    this.#now = config.nowFn ?? (() => Date.now());
  }

  public get isDisposed(): boolean {
    return this.#isDisposed;
  }

  /**
   * Registers an unforgeable, authenticated recipient context bound to transport parameters.
   * Requires the authorized OutboundTransportCapability.
   */
  public registerInboundRecipientContext(
    params: InboundRecipientContextParams,
    capability: OutboundTransportCapability
  ): AuthenticatedRecipientContext {
    this.#assertNotDisposed();

    if (
      !this.#authorizedCapability ||
      !capability ||
      capability !== this.#authorizedCapability
    ) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        'Recipient context registration requires authorized OutboundTransportCapability'
      );
    }

    if (!params || typeof params !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'InboundRecipientContextParams is required');
    }
    if (!params.transferId || typeof params.transferId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'transferId must be a non-empty string');
    }
    if (!params.recipientPeerId || typeof params.recipientPeerId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'recipientPeerId must be a non-empty string');
    }
    if (!params.sessionId || typeof params.sessionId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'sessionId must be a non-empty string');
    }
    if (
      typeof params.membershipEpoch !== 'number' ||
      !Number.isInteger(params.membershipEpoch) ||
      params.membershipEpoch < 0
    ) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid membershipEpoch ${params.membershipEpoch}: must be non-negative integer`
      );
    }
    if (
      typeof params.connectionGeneration !== 'number' ||
      !Number.isInteger(params.connectionGeneration) ||
      params.connectionGeneration < 1
    ) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Invalid connectionGeneration ${params.connectionGeneration}: must be positive integer`
      );
    }

    const context: AuthenticatedRecipientContext = Object.freeze({
      transferId: params.transferId,
      recipientPeerId: params.recipientPeerId,
      sessionId: params.sessionId,
      membershipEpoch: params.membershipEpoch,
      connectionGeneration: params.connectionGeneration,
    });

    this.#validRecipientContexts.add(context);
    return context;
  }

  /**
   * Returns the count of currently active (non-terminal) outbound transfers.
   */
  public get activeTransferCount(): number {
    let count = 0;
    for (const record of this.#transfers.values()) {
      if (!this.#isTerminalState(record.state)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Returns the total count of prepared chunk frames currently retained in cache across all transfers.
   */
  public get totalRetainedPreparedFrames(): number {
    let count = 0;
    for (const record of this.#transfers.values()) {
      count += record.preparedFrames.size;
    }
    return count;
  }

  public get preparationQueue(): SnapshotPreparationQueue {
    return this.#preparationQueue;
  }

  /**
   * Starts a new outbound transfer.
   * If an artifact is provided directly, binds the lease and initializes the producer immediately.
   * If a captureAndSerializeFn is provided, enqueues in SnapshotPreparationQueue, adhering to
   * the strict A + P <= 2 concurrency ceiling.
   */
  public async startTransfer(params: StartTransferParams): Promise<OutboundTransferSession> {
    this.#assertNotDisposed();

    if (!params || typeof params !== 'object') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'StartTransferParams is required');
    }
    if (!params.transferId || typeof params.transferId !== 'string') {
      throw new SyncEngineError('INVALID_ARGUMENT', 'Valid transferId string is required');
    }
    const existing = this.#transfers.get(params.transferId);
    if (existing && !this.#isTerminalState(existing.state)) {
      throw new SyncEngineError('INVALID_STATE', `Transfer ${params.transferId} is already active`);
    }
    if (this.activeTransferCount >= this.#maxActiveTransfers) {
      throw new SyncEngineError(
        'LOCAL_ABORT',
        `Maximum concurrent active outbound transfers reached (${this.#maxActiveTransfers})`
      );
    }
    if (params.sctpMaxMessageSize < MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER) {
      throw new SyncEngineError(
        'TRANSPORT_LIMIT_EXCEEDED',
        `Negotiated transport limit (${params.sctpMaxMessageSize} B) is below minimum required (${MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER} B)`
      );
    }

    const ownsArtifact = params.autoDisposeArtifact ?? !params.artifact;

    const record: InternalTransferRecord = {
      transferId: params.transferId,
      recipientPeerId: params.recipientPeerId,
      supplierPeerId: params.supplierPeerId,
      membershipEpoch: params.membershipEpoch,
      connectionGeneration: params.connectionGeneration ?? 1,
      sctpMaxMessageSize: params.sctpMaxMessageSize,
      snapshotFrontier: params.snapshotFrontier ?? {},
      maxLogicalTimestamp: params.maxLogicalTimestamp ?? 0,
      startTimeMs: this.#now(),
      ownsArtifact,
      state: 'PREPARING',
      acknowledgedChunks: new Set(),
      inflightChunks: new Set(),
      transmittedChunks: new Set(),
      handedOffChunks: new Set(),
      handedOffAttempts: new Set(),
      failedAttempts: new Set(),
      activeAttempts: new Map(),
      attemptCounters: new Map(),
      retryCounts: new Map(),
      confirmedRetries: new Map(),
      preparedFrames: new Map(),
      chunkMessageIds: new Map(),
      lastHandedOffMessageIds: new Map(),
      leaseHeld: false,
    };

    this.#transfers.set(params.transferId, record);

    // Setup transfer timeout guard
    this.#armTimeout(record);

    try {
      let artifact: SerializedSnapshotArtifact;
      if (params.artifact) {
        artifact = params.artifact;
      } else if (params.captureAndSerializeFn) {
        artifact = await this.#preparationQueue.enqueue(
          {
            transferId: params.transferId,
            recipientPeerId: params.recipientPeerId,
            sctpMaxMessageSize: params.sctpMaxMessageSize,
            timestampMs: record.startTimeMs,
          },
          params.captureAndSerializeFn
        );
      } else {
        throw new SyncEngineError('INVALID_ARGUMENT', 'Either artifact or captureAndSerializeFn must be provided');
      }

      // Check if transfer was cancelled or timed out while waiting in preparation queue
      if (record.state !== 'PREPARING') {
        // If state changed while preparing, discard artifact immediately
        if (ownsArtifact && !artifact.isDisposed) {
          try {
            artifact.dispose();
          } catch {
            // Safe disposal
          }
        }
        return this.#toSessionView(record);
      }

      record.artifact = artifact;

      // Acquire transfer lease on artifact
      artifact.acquireLease(params.transferId);
      record.leaseHeld = true;

      // Construct immutable partition producer
      const producer = new SnapshotChunkProducer(artifact, {
        sctpMaxMessageSize: params.sctpMaxMessageSize,
        transferId: params.transferId,
        acquireLease: false, // lease already acquired above
      });

      record.producer = producer;
      record.state = 'PREPARED';

      // Format initial SNAPSHOT_OFFER message
      record.offerMessage = SnapshotOutboundPipeline.createOfferMessage(producer, {
        transferId: params.transferId,
        supplierPeerId: params.supplierPeerId,
        recipientPeerId: params.recipientPeerId,
        membershipEpoch: params.membershipEpoch,
        connectionGeneration: record.connectionGeneration,
        snapshotFrontier: record.snapshotFrontier,
        maxLogicalTimestamp: record.maxLogicalTimestamp,
      });

      return this.#toSessionView(record);
    } catch (err) {
      this.#terminate(record, 'ABORTED', err instanceof Error ? err.message : String(err));
      throw err;
    }
  }

  /**
   * Retrieves the current snapshot of an outbound transfer session.
   */
  public getTransfer(transferId: string): OutboundTransferSession | undefined {
    const record = this.#transfers.get(transferId);
    return record ? this.#toSessionView(record) : undefined;
  }

  /**
   * Lists all current active (non-terminal) outbound transfer sessions.
   */
  public getActiveTransfers(): OutboundTransferSession[] {
    return Array.from(this.#transfers.values())
      .filter((r) => !this.#isTerminalState(r.state))
      .map((r) => this.#toSessionView(r));
  }

  /**
   * Produces or retrieves prepared chunk frames for the requested chunk indices.
   * Gated by AuthenticatedRecipientContext: enforces recipient peer, session, epoch, and generation fencing.
   * Enforces:
   * 1. Window bounds: number of concurrently retained in-flight frames <= slidingWindowSize.
   * 2. Per-chunk retry bounding: initial transmission does not consume retry budget;
   *    retransmissions capped at maxRetriesPerChunk (aborts with RETRY_BUDGET_EXHAUSTED if exceeded).
   * 3. Deterministic messageId preservation across retransmissions even after frame cache eviction.
   * 4. Send-boundary byte verification: aborts with TRANSPORT_LIMIT_EXCEEDED if frame overshoots.
   * 5. Idempotent return of exact serializedJson string.
   */
  public handleChunkRequest(
    transferId: string,
    requestedChunkIndices: number[],
    context: AuthenticatedRecipientContext
  ): OutboundPreparedChunk[] {
    this.#assertNotDisposed();

    const record = this.#assertActiveTransfer(transferId);
    this.#validateContext(record, transferId, context);

    if (record.state === 'PREPARED') {
      record.state = 'ACTIVE';
    }

    const producer = record.producer!;
    const results: OutboundPreparedChunk[] = [];
    const uniqueIndices = Array.from(new Set(requestedChunkIndices));

    for (const chunkIndex of uniqueIndices) {
      if (!Number.isInteger(chunkIndex) || chunkIndex < 0 || chunkIndex >= producer.totalChunks) {
        throw new SyncEngineError(
          'INVALID_RANGE',
          `Requested chunkIndex (${chunkIndex}) out of valid range [0, ${producer.totalChunks})`
        );
      }

      if (record.acknowledgedChunks.has(chunkIndex)) {
        throw new SyncEngineError('INVALID_STATE', `Chunk ${chunkIndex} is already acknowledged`);
      }

      // Safe Coalescing Policy (Finding D1):
      // If this chunk already has an active attempt pending local handoff callback,
      // produce no additional dispatchable handoff to prevent duplicate physical transmission
      // and preserve the pending attempt's identity and callbacks intact.
      const active = record.activeAttempts.get(chunkIndex);
      if (active && !record.handedOffAttempts.has(active.attemptToken)) {
        continue;
      }

      const currentMessageId = record.chunkMessageIds.get(chunkIndex);
      const lastHandedOffMessageId = record.lastHandedOffMessageIds.get(chunkIndex);

      let messageId: string;
      let isRetransmission: boolean;

      if (currentMessageId === undefined) {
        // Initial transmission check: enforce sliding window capacity
        if (!record.inflightChunks.has(chunkIndex) && record.inflightChunks.size >= this.#slidingWindowSize) {
          throw new SyncEngineError(
            'LOCAL_ABORT',
            `In-flight sliding window capacity (${this.#slidingWindowSize}) reached for transfer ${transferId}`
          );
        }

        // Initial transmission attempt: allocate initial messageId
        messageId = `msg-${generateUUID()}`;
        record.chunkMessageIds.set(chunkIndex, messageId);
        record.transmittedChunks.add(chunkIndex);
        isRetransmission = false;
      } else {
        const currentMessageWasHandedOff =
          lastHandedOffMessageId !== undefined && lastHandedOffMessageId === currentMessageId;

        if (currentMessageWasHandedOff) {
          // Current message was ALREADY successfully handed off to the transport send buffer.
          // This new request is a GENUINE NETWORK RETRANSMISSION!
          const confirmedRetries = record.confirmedRetries.get(chunkIndex) ?? 0;
          if (confirmedRetries >= this.#maxRetriesPerChunk) {
            this.#terminate(
              record,
              'ABORTED',
              `Retry budget (${this.#maxRetriesPerChunk}) exhausted for chunk ${chunkIndex}`,
              'RETRY_BUDGET_EXHAUSTED'
            );
            throw new SyncEngineError(
              'LOCAL_ABORT',
              `Retry budget exhausted for chunk ${chunkIndex} in transfer ${transferId}`
            );
          }

          // Rule 7: Subsequent network retransmission receives a new messageId
          messageId = `msg-${generateUUID()}`;
          record.chunkMessageIds.set(chunkIndex, messageId);
          // Reserve retry count for this new retransmission attempt
          record.retryCounts.set(chunkIndex, confirmedRetries + 1);
          isRetransmission = true;
        } else {
          // Rule 5: Failed local handoff preserves messageId for next local attempt
          // Zero network retries consumed; reuse currentMessageId
          messageId = currentMessageId;
          const isPriorRetransmissionChunk = record.handedOffChunks.has(chunkIndex);
          if (isPriorRetransmissionChunk) {
            isRetransmission = true;
            const confirmed = record.confirmedRetries.get(chunkIndex) ?? 0;
            record.retryCounts.set(chunkIndex, confirmed + 1);
          } else {
            isRetransmission = false;
            if (!record.inflightChunks.has(chunkIndex) && record.inflightChunks.size >= this.#slidingWindowSize) {
              throw new SyncEngineError(
                'LOCAL_ABORT',
                `In-flight sliding window capacity (${this.#slidingWindowSize}) reached for transfer ${transferId}`
              );
            }
            record.transmittedChunks.add(chunkIndex);
          }
        }
      }

      // Re-use already prepared frame from sliding window cache if available and matching messageId
      let frame = record.preparedFrames.get(chunkIndex);
      if (!frame || frame.message.messageId !== messageId) {
        // Enforce sliding window capacity: evict oldest unacknowledged non-in-flight frame if needed
        if (!record.preparedFrames.has(chunkIndex) && record.preparedFrames.size >= this.#slidingWindowSize) {
          this.#pruneOldestPreparedFrame(record);
        }

        try {
          frame = producer.produceChunk(chunkIndex, {
            messageId,
            connectionGeneration: record.connectionGeneration,
          });
        } catch (err) {
          this.#terminate(
            record,
            'ABORTED',
            `Chunk production failed for index ${chunkIndex}: ${err instanceof Error ? err.message : String(err)}`,
            err instanceof SyncEngineError && err.code === 'TRANSPORT_LIMIT_EXCEEDED'
              ? 'TRANSPORT_LIMIT_EXCEEDED'
              : 'LOCAL_ABORT'
          );
          throw err;
        }

        record.preparedFrames.set(chunkIndex, frame);
      }

      // Rule 1: handleChunkRequest() must return the exact messageId associated with the returned frame
      const attemptSeq = (record.attemptCounters.get(chunkIndex) ?? 0) + 1;
      record.attemptCounters.set(chunkIndex, attemptSeq);
      const attemptToken = `${messageId}:${attemptSeq}`;

      // Return attempt-bound OutboundPreparedChunk descriptor
      const attemptFrame: OutboundPreparedChunk = Object.freeze({
        ...frame,
        messageId,
        attemptSeq,
        attemptToken,
      });

      // Rule 3: Track active attempt using at least (transferId, chunkIndex, messageId)
      record.activeAttempts.set(chunkIndex, { messageId, attemptSeq, attemptToken, isRetransmission });
      record.inflightChunks.add(chunkIndex);
      results.push(attemptFrame);
    }

    if (record.inflightChunks.size >= this.#slidingWindowSize) {
      record.state = 'WAITING_ACK';
    }

    return results;
  }

  /**
   * Handles chunk delivery acknowledgement from the recipient.
   * Gated by AuthenticatedRecipientContext: enforces recipient peer, session, epoch, and generation fencing.
   * Evicts the delivered frame from the sliding window cache to free memory.
   * If all chunks are acknowledged, transitions to COMPLETED and releases all leases.
   * Stale/late acknowledgements on already completed/terminated transfers are ignored.
   */
  public handleChunkAck(
    transferId: string,
    chunkIndex: number,
    context: AuthenticatedRecipientContext
  ): void {
    const record = this.#transfers.get(transferId);
    if (!record) {
      // Stale or unknown transfer: safely ignore late ACK
      return;
    }
    if (this.#isTerminalState(record.state)) {
      // Transfer already terminated: ignore late ACK without resurrecting
      return;
    }

    this.#validateContext(record, transferId, context);

    if (
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= (record.producer?.totalChunks ?? 0)
    ) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `Acknowledged chunkIndex ${chunkIndex} is out of bounds`
      );
    }

    if (record.acknowledgedChunks.has(chunkIndex)) {
      // Duplicate ACK: idempotent no-op
      return;
    }

    record.acknowledgedChunks.add(chunkIndex);
    record.inflightChunks.delete(chunkIndex);
    record.activeAttempts.delete(chunkIndex);

    // Evict delivered frame from sliding window cache immediately to bound memory
    record.preparedFrames.delete(chunkIndex);
    record.chunkMessageIds.delete(chunkIndex);

    // If waiting for ACK and in-flight count dropped below window, return to ACTIVE
    if (record.state === 'WAITING_ACK' && record.inflightChunks.size < this.#slidingWindowSize) {
      record.state = 'ACTIVE';
    }

    // Completion Check: all canonical chunks acknowledged!
    if (record.acknowledgedChunks.size === record.producer!.totalChunks) {
      this.#terminate(record, 'COMPLETED');
    }
  }

  /**
   * Handles successful transport handoff reported by the transport layer.
   * Confirms that the message entered the local DataChannel send buffer.
   * Gated by AuthenticatedRecipientContext and attempt messageId.
   * Does NOT claim remote receipt or acknowledge the chunk (Rule 6, 10).
   */
  public handleHandoffSuccess(
    transferId: string,
    chunkIndex: number,
    messageId: string,
    attemptToken: string,
    context: AuthenticatedRecipientContext
  ): void {
    this.#assertNotDisposed();

    const record = this.#transfers.get(transferId);
    if (!record || this.#isTerminalState(record.state)) {
      return;
    }

    this.#validateContext(record, transferId, context);

    if (
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= (record.producer?.totalChunks ?? 0)
    ) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `Handoff success chunkIndex ${chunkIndex} is out of bounds`
      );
    }

    if (record.acknowledgedChunks.has(chunkIndex)) {
      return;
    }

    if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim() === '') {
      return;
    }
    if (!messageId || typeof messageId !== 'string' || messageId.trim() === '') {
      return;
    }

    // Finding D2 — Idempotent Success Callback Guard:
    // If this exact attemptToken has ALREADY been successfully handed off,
    // repeated success callbacks must be a strict NO-OP.
    if (record.handedOffAttempts.has(attemptToken)) {
      return;
    }

    // Retired attempt guard: an attempt that failed cannot later succeed
    if (record.failedAttempts.has(attemptToken)) {
      return;
    }

    const active = record.activeAttempts.get(chunkIndex);
    if (!active) {
      return;
    }
    if (active.messageId !== messageId || active.attemptToken !== attemptToken) {
      // Stale, duplicate, or mismatched attempt: NO-OP (Rule 4, 8)
      return;
    }

    // Record entry into local send buffer (Rule 6, 10)
    record.handedOffAttempts.add(attemptToken);
    record.handedOffChunks.add(chunkIndex);
    record.lastHandedOffMessageIds.set(chunkIndex, messageId);

    if (active.isRetransmission) {
      const currentConfirmed = record.confirmedRetries.get(chunkIndex) ?? 0;
      const newConfirmed = currentConfirmed + 1;
      record.confirmedRetries.set(chunkIndex, newConfirmed);
      record.retryCounts.set(chunkIndex, newConfirmed);
    }
  }

  /**
   * Handles transport handoff failure reported by the transport layer.
   * Releases the in-flight reservation for the failed chunk, reopening the window.
   * Does NOT mark the chunk acknowledged, does NOT dispose artifact or leases,
   * and preserves cached frame for subsequent local re-attempt (Rules 4, 5, 8, 9).
   */
  public handleHandoffFailure(
    transferId: string,
    chunkIndex: number,
    messageId: string,
    attemptToken: string,
    context: AuthenticatedRecipientContext
  ): void {
    this.#assertNotDisposed();

    const record = this.#transfers.get(transferId);
    if (!record || this.#isTerminalState(record.state)) {
      return;
    }

    this.#validateContext(record, transferId, context);

    if (
      !Number.isInteger(chunkIndex) ||
      chunkIndex < 0 ||
      chunkIndex >= (record.producer?.totalChunks ?? 0)
    ) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `Handoff failure chunkIndex ${chunkIndex} is out of bounds`
      );
    }

    // If chunk was already acknowledged, handoff failure cannot un-acknowledge it
    if (record.acknowledgedChunks.has(chunkIndex)) {
      return;
    }

    if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim() === '') {
      return;
    }
    if (!messageId || typeof messageId !== 'string' || messageId.trim() === '') {
      return;
    }

    // Finding D3 — Failure-After-Success Guard:
    // If this exact attemptToken has ALREADY been successfully handed off,
    // late failure callbacks must be a strict NO-OP.
    // We must NOT un-reserve in-flight status, NOT delete active attempt, and NOT roll back retries!
    if (record.handedOffAttempts.has(attemptToken)) {
      return;
    }

    // Duplicate Failure Guard:
    // If this exact attemptToken was already marked as failed, repeated failure callback is a strict NO-OP.
    if (record.failedAttempts.has(attemptToken)) {
      return;
    }

    const active = record.activeAttempts.get(chunkIndex);
    if (!active) {
      return;
    }

    // Rule 4 & 8: A callback whose messageId or attemptToken does not match the active attempt must be a strict no-op.
    if (active.messageId !== messageId || active.attemptToken !== attemptToken) {
      return;
    }

    // Mark attempt as failed locally and remove from active attempt tracking
    record.activeAttempts.delete(chunkIndex);
    record.failedAttempts.add(attemptToken);

    // Release in-flight reservation
    if (record.inflightChunks.has(chunkIndex)) {
      record.inflightChunks.delete(chunkIndex);

      if (active.isRetransmission) {
        // Roll back uncompleted retry reservation (Section 4, Rule 9)
        const confirmed = record.confirmedRetries.get(chunkIndex) ?? 0;
        if (confirmed === 0) {
          record.retryCounts.delete(chunkIndex);
        } else {
          record.retryCounts.set(chunkIndex, confirmed);
        }
      } else {
        // Failed initial transmission handoff: chunk was never successfully handed off
        record.transmittedChunks.delete(chunkIndex);
      }

      // If waiting for ACK and in-flight count dropped below window, return to ACTIVE
      if (record.state === 'WAITING_ACK' && record.inflightChunks.size < this.#slidingWindowSize) {
        record.state = 'ACTIVE';
      }
    }
  }

  /**
   * Handles remote peer rejection/abort of the transfer.
   * Gated by AuthenticatedRecipientContext: enforces recipient peer, session, epoch, and generation fencing.
   */
  public handleRemoteAbort(
    transferId: string,
    reason: SnapshotAbortReason,
    context: AuthenticatedRecipientContext,
    detail?: string
  ): void {
    const record = this.#transfers.get(transferId);
    if (!record || this.#isTerminalState(record.state)) {
      return;
    }

    this.#validateContext(record, transferId, context);

    this.#terminate(record, 'ABORTED', detail ?? `Remote abort: ${reason}`, reason);
  }

  /**
   * Cancels an active or pending outbound transfer locally.
   * If pending in the preparation queue, cancels the queue item.
   * If active, transitions to CANCELLED and disposes the producer and leases.
   */
  public cancelTransfer(transferId: string, reason = 'Transfer cancelled locally'): boolean {
    const record = this.#transfers.get(transferId);
    if (!record) {
      return false;
    }
    if (this.#isTerminalState(record.state)) {
      return false;
    }

    if (record.state === 'PREPARING') {
      this.#preparationQueue.cancel(transferId);
    }

    this.#terminate(record, 'CANCELLED', reason, 'LOCAL_ABORT');
    return true;
  }

  /**
   * Explicitly disposes the outbound pipeline, cancelling all active transfers
   * and releasing all held leases.
   */
  public dispose(): void {
    if (this.#isDisposed) {
      return;
    }
    this.#isDisposed = true;

    for (const record of this.#transfers.values()) {
      if (!this.#isTerminalState(record.state)) {
        if (record.state === 'PREPARING') {
          this.#preparationQueue.cancel(record.transferId);
        }
        this.#terminate(record, 'CANCELLED', 'Pipeline disposed', 'LOCAL_ABORT');
      }
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Private Lifecycle & Memory Management Helpers
  // ─────────────────────────────────────────────────────────────

  #assertNotDisposed(): void {
    if (this.#isDisposed) {
      throw new SyncEngineError('DISPOSED', 'SnapshotOutboundPipeline has been disposed');
    }
  }

  #assertActiveTransfer(transferId: string): InternalTransferRecord {
    const record = this.#transfers.get(transferId);
    if (!record) {
      throw new SyncEngineError('INVALID_STATE', `Transfer ${transferId} not found`);
    }
    if (this.#isTerminalState(record.state)) {
      throw new SyncEngineError(
        'INVALID_STATE',
        `Transfer ${transferId} is already in terminal state ${record.state}`
      );
    }
    if (!record.producer) {
      throw new SyncEngineError('INVALID_STATE', `Transfer ${transferId} producer is not initialized`);
    }
    return record;
  }

  #validateContext(
    record: InternalTransferRecord,
    transferId: string,
    context: AuthenticatedRecipientContext
  ): void {
    if (!context || typeof context !== 'object') {
      throw new SyncEngineError('UNAUTHORIZED', 'Authenticated recipient context is required');
    }
    if (!this.#validRecipientContexts.has(context)) {
      throw new SyncEngineError('UNAUTHORIZED', 'Provided recipient context is not registered or authenticated');
    }
    if (context.transferId !== transferId || context.transferId !== record.transferId) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        `Context transferId '${context.transferId}' does not match transfer '${transferId}'`
      );
    }
    if (context.recipientPeerId !== record.recipientPeerId) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        `Authenticated peer '${context.recipientPeerId}' does not match transfer recipient '${record.recipientPeerId}'`
      );
    }
    if (record.artifact && context.sessionId !== record.artifact.sessionId) {
      throw new SyncEngineError(
        'INVALID_ARGUMENT',
        `Context sessionId '${context.sessionId}' does not match artifact sessionId '${record.artifact.sessionId}'`
      );
    }
    if (context.membershipEpoch !== record.membershipEpoch) {
      throw new SyncEngineError(
        'REJECTED_OBSOLETE_EPOCH',
        `Context membershipEpoch ${context.membershipEpoch} does not match transfer epoch ${record.membershipEpoch}`
      );
    }
    if (context.connectionGeneration !== record.connectionGeneration) {
      throw new SyncEngineError(
        'UNAUTHORIZED',
        `Context connectionGeneration ${context.connectionGeneration} does not match transfer generation ${record.connectionGeneration}`
      );
    }
  }

  #isTerminalState(state: OutboundTransferState): boolean {
    return (
      state === 'COMPLETED' ||
      state === 'ABORTED' ||
      state === 'CANCELLED' ||
      state === 'TIMED_OUT'
    );
  }

  #armTimeout(record: InternalTransferRecord): void {
    if (this.#transferTimeoutMs > 0) {
      record.timeoutTimer = setTimeout(() => {
        if (!this.#isTerminalState(record.state)) {
          this.#terminate(
            record,
            'TIMED_OUT',
            `Outbound transfer timed out after ${this.#transferTimeoutMs} ms`,
            'TRANSFER_TIMEOUT'
          );
        }
      }, this.#transferTimeoutMs);

      // Node.js timer unref if available to prevent process hang in tests
      if (record.timeoutTimer && typeof (record.timeoutTimer as any).unref === 'function') {
        (record.timeoutTimer as any).unref();
      }
    }
  }

  #pruneOldestPreparedFrame(record: InternalTransferRecord): void {
    for (const [idx] of record.preparedFrames.entries()) {
      record.preparedFrames.delete(idx);
      break;
    }
  }

  /**
   * Idempotent terminal cleanup. Releases leases, clears frames, disarms timers.
   */
  #terminate(
    record: InternalTransferRecord,
    targetState: OutboundTransferState,
    detail?: string,
    reason?: SnapshotAbortReason
  ): void {
    if (this.#isTerminalState(record.state)) {
      return;
    }

    record.state = targetState;
    record.failureDetail = detail;
    record.failureReason = reason;

    if (record.timeoutTimer) {
      clearTimeout(record.timeoutTimer);
      record.timeoutTimer = undefined;
    }

    // Clear prepared frames cache to immediately reclaim string and buffer memory
    record.preparedFrames.clear();
    record.inflightChunks.clear();

    // Release producer and artifact leases
    if (record.producer) {
      try {
        record.producer.dispose();
      } catch {
        // Idempotent exception safety
      }
    }

    if (record.ownsArtifact && record.artifact && !record.artifact.isDisposed) {
      try {
        record.artifact.dispose();
      } catch {
        // Safe disposal
      }
    }

    if (record.leaseHeld && record.artifact) {
      try {
        record.artifact.releaseLease(record.transferId);
      } catch {
        // Idempotent exception safety
      }
      record.leaseHeld = false;
    }

    this.#pruneTerminalHistory();
  }

  #pruneTerminalHistory(): void {
    const terminalKeys: string[] = [];
    for (const [id, r] of this.#transfers.entries()) {
      if (this.#isTerminalState(r.state)) {
        terminalKeys.push(id);
      }
    }
    const excess = terminalKeys.length - SnapshotOutboundPipeline.MAX_TERMINAL_HISTORY_ENTRIES;
    if (excess > 0) {
      for (let i = 0; i < excess; i++) {
        const key = terminalKeys[i];
        if (key !== undefined) {
          this.#transfers.delete(key);
        }
      }
    }
  }

  #toSessionView(record: InternalTransferRecord): OutboundTransferSession {
    const producer = record.producer;
    return {
      transferId: record.transferId,
      recipientPeerId: record.recipientPeerId,
      supplierPeerId: record.supplierPeerId,
      membershipEpoch: record.membershipEpoch,
      connectionGeneration: record.connectionGeneration,
      sctpMaxMessageSize: record.sctpMaxMessageSize,
      state: record.state,
      totalChunks: producer?.totalChunks ?? 0,
      totalSizeBytes: record.artifact?.totalSizeBytes ?? 0,
      nominalChunkSizeBytes: producer?.nominalChunkSizeBytes ?? 0,
      acknowledgedChunks: new Set(record.acknowledgedChunks),
      inflightChunks: new Set(record.inflightChunks),
      transmittedChunks: new Set(record.transmittedChunks),
      retryCounts: new Map(record.retryCounts),
      confirmedRetries: new Map(record.confirmedRetries),
      activeAttempts: new Map(record.activeAttempts),
      handedOffAttempts: new Set(record.handedOffAttempts),
      failedAttempts: new Set(record.failedAttempts),
      offerMessage: record.offerMessage,
      failureReason: record.failureReason,
      failureDetail: record.failureDetail,
      isTerminal: this.#isTerminalState(record.state),
    };
  }
}
