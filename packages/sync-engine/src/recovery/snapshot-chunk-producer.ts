/**
 * @module @syncplay/sync-engine/recovery
 *
 * Implements Phase 7.5 M4.2b Snapshot Chunk Producer & Canonical Partitioning:
 * - Deterministic, immutable canonical snapshot chunk partitioning.
 * - Upfront transport admission and dynamic nominal chunk sizing via calculateChunkCapacity().
 * - Encapsulated chunk Base64 encoding and per-chunk SHA-256 computation via SerializedSnapshotArtifact.
 * - Complete ProtocolMessage wire envelope construction satisfying SnapshotChunkEnvelopeSchema.
 * - Mandatory exact UTF-8 byte length measurement (new TextEncoder().encode(serializedJson).byteLength)
 *   enforced at the send boundary against the negotiated transport limit.
 * - Complete lease lifecycle management: guaranteed lease release on completion, explicit disposal,
 *   cancellation, producer failure, or invalid input.
 */

import {
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  PROTOCOL_VERSION,
  MessageType,
  calculateChunkCapacity,
  SnapshotChunkPayloadSchema,
  SnapshotChunkEnvelopeSchema,
  type SnapshotChunkPayload,
  type ProtocolMessage,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import { SerializedSnapshotArtifact } from './snapshot-serializer.js';

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
 * Metadata descriptor for a canonical chunk slice.
 */
export interface SnapshotChunkSlice {
  readonly chunkIndex: number;
  readonly totalChunks: number;
  readonly chunkSizeBytes: number;
  readonly chunkSha256: string;
}

/**
 * Immutable snapshot partition containing canonical chunk slices.
 */
export interface PartitionedSnapshot {
  readonly artifact: SerializedSnapshotArtifact;
  readonly nominalChunkSizeBytes: number;
  readonly totalChunks: number;
  readonly slices: ReadonlyArray<SnapshotChunkSlice>;

  /** Retrieves chunk slice metadata by index, or undefined if out of range. */
  getSlice(chunkIndex: number): SnapshotChunkSlice | undefined;

  /**
   * Encodes chunk slice to Base64 via artifact encapsulation.
   * Raw buffer views never escape.
   */
  encodeChunkBase64(chunkIndex: number): string;
}

/**
 * Configuration parameters for SnapshotChunkProducer and canonical partitioning.
 */
export interface ChunkProducerConfig {
  /** Target nominal chunk size (bytes). If omitted, derived from sctpMaxMessageSize. */
  nominalChunkSizeBytes?: number;
  /** Transport message size limit (bytes). Used to calculate chunk capacity if nominalChunkSizeBytes omitted. */
  sctpMaxMessageSize?: number;
  /** Testing flag to allow 65,536 B fallback when sctpMaxMessageSize is unexposed. */
  allowSctpFallback?: boolean;
  /** Transfer identifier for lease acquisition on the artifact. If provided or created as a producer, binds lease. */
  transferId?: string;
  /** Whether to acquire a transfer lease on the artifact. Defaults to true when a transferId is provided or instantiated. */
  acquireLease?: boolean;
}

/**
 * Context parameters required when producing individual chunk wire envelopes.
 */
export interface ChunkProductionContext {
  readonly transferId?: string;
  readonly supplierPeerId?: string;
  readonly recipientPeerId?: string;
  readonly membershipEpoch?: number;
  readonly connectionGeneration?: number;
  readonly messageId?: string;
}

/**
 * Result of producing a complete, verified chunk message.
 */
export interface ProducedChunkMessage {
  readonly ok: true;
  readonly payload: SnapshotChunkPayload;
  readonly message: ProtocolMessage<SnapshotChunkPayload>;
  readonly serializedJson: string;
  readonly byteLength: number;
}

/**
 * Producer responsible for establishing deterministic canonical partition geometry,
 * encoding chunks via encapsulated artifact APIs, and enforcing exact complete-message
 * UTF-8 byte limits at the send boundary.
 */
export class SnapshotChunkProducer implements PartitionedSnapshot {
  public readonly artifact: SerializedSnapshotArtifact;
  public readonly nominalChunkSizeBytes: number;
  public readonly totalChunks: number;
  public readonly slices: ReadonlyArray<SnapshotChunkSlice>;
  public readonly transferId: string;
  public readonly effectiveLimit: number;

  #hasLease = false;
  #isDisposed = false;

  constructor(artifact: SerializedSnapshotArtifact, config: ChunkProducerConfig = {}) {
    if (!artifact || !(artifact instanceof SerializedSnapshotArtifact)) {
      throw new SyncEngineError('PARTITION_ERROR', 'SnapshotChunkProducer requires a valid SerializedSnapshotArtifact');
    }
    if (artifact.isDisposed) {
      throw new SyncEngineError('ARTIFACT_DISPOSED', 'Cannot partition a disposed SerializedSnapshotArtifact');
    }

    this.artifact = artifact;

    // 1. Resolve transport limit and effective limit
    const fallbackLimit = 65536;
    let effectiveLimit: number;
    if (config.sctpMaxMessageSize !== undefined) {
      if (
        typeof config.sctpMaxMessageSize !== 'number' ||
        !Number.isFinite(config.sctpMaxMessageSize) ||
        config.sctpMaxMessageSize <= 0
      ) {
        throw new SyncEngineError(
          'TRANSPORT_LIMIT_EXCEEDED',
          `Invalid sctpMaxMessageSize: ${config.sctpMaxMessageSize}`
        );
      }
      effectiveLimit = Math.min(fallbackLimit, config.sctpMaxMessageSize);
    } else if (config.allowSctpFallback) {
      effectiveLimit = fallbackLimit;
    } else {
      throw new SyncEngineError(
        'TRANSPORT_LIMIT_EXCEEDED',
        'SCTP transport message size is unnegotiated or unavailable'
      );
    }

    if (effectiveLimit < MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER) {
      throw new SyncEngineError(
        'TRANSPORT_LIMIT_EXCEEDED',
        `Effective transport limit (${effectiveLimit} B) is below minimum supported chunk transport limit (${MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER} B)`
      );
    }
    this.effectiveLimit = effectiveLimit;

    // 2. Calculate chunk binary capacity
    const chunkCapacity = calculateChunkCapacity(effectiveLimit);
    if (chunkCapacity < MIN_RAW_CHUNK_BYTES) {
      throw new SyncEngineError(
        'TRANSPORT_LIMIT_EXCEEDED',
        `Chunk binary capacity (${chunkCapacity} B) is below minimum raw chunk size (${MIN_RAW_CHUNK_BYTES} B)`
      );
    }

    // 3. Resolve nominalChunkSizeBytes
    if (config.nominalChunkSizeBytes !== undefined) {
      if (
        typeof config.nominalChunkSizeBytes !== 'number' ||
        !Number.isInteger(config.nominalChunkSizeBytes) ||
        config.nominalChunkSizeBytes < MIN_RAW_CHUNK_BYTES ||
        config.nominalChunkSizeBytes > MAX_CHUNK_BYTES
      ) {
        throw new SyncEngineError(
          'PARTITION_ERROR',
          `nominalChunkSizeBytes (${config.nominalChunkSizeBytes}) must be an integer between ${MIN_RAW_CHUNK_BYTES} and ${MAX_CHUNK_BYTES}`
        );
      }
      if (config.nominalChunkSizeBytes > chunkCapacity) {
        throw new SyncEngineError(
          'PARTITION_ERROR',
          `Requested nominalChunkSizeBytes (${config.nominalChunkSizeBytes} B) exceeds transport chunk capacity (${chunkCapacity} B)`
        );
      }
      this.nominalChunkSizeBytes = config.nominalChunkSizeBytes;
    } else {
      this.nominalChunkSizeBytes = chunkCapacity;
    }

    // 4. Calculate totalChunks
    const totalSizeBytes = artifact.totalSizeBytes;
    if (totalSizeBytes <= 0 || totalSizeBytes > MAX_SNAPSHOT_BYTES) {
      throw new SyncEngineError(
        'PARTITION_ERROR',
        `Artifact totalSizeBytes (${totalSizeBytes} B) is outside permitted bounds [1, ${MAX_SNAPSHOT_BYTES}]`
      );
    }

    const calculatedTotalChunks = Math.ceil(totalSizeBytes / this.nominalChunkSizeBytes);
    if (calculatedTotalChunks < 1 || calculatedTotalChunks > MAX_CHUNKS_PER_SNAPSHOT) {
      throw new SyncEngineError(
        'PARTITION_ERROR',
        `Calculated chunk count (${calculatedTotalChunks}) exceeds MAX_CHUNKS_PER_SNAPSHOT (${MAX_CHUNKS_PER_SNAPSHOT}) on ${effectiveLimit} B transport`
      );
    }
    this.totalChunks = calculatedTotalChunks;

    // 5. Establish transfer lease
    const shouldAcquireLease = config.acquireLease ?? (config.transferId !== undefined);
    this.transferId = config.transferId ?? generateUUID();

    if (shouldAcquireLease) {
      try {
        this.artifact.acquireLease(this.transferId);
        this.#hasLease = true;
      } catch (err) {
        throw err instanceof SyncEngineError ? err : new SyncEngineError('INVALID_STATE', String(err));
      }
    }

    // 6. Establish immutable canonical slices with failure safety
    try {
      const slices: SnapshotChunkSlice[] = new Array(this.totalChunks);
      for (let i = 0; i < this.totalChunks; i++) {
        const offset = i * this.nominalChunkSizeBytes;
        const chunkSize =
          i === this.totalChunks - 1
            ? totalSizeBytes - (this.totalChunks - 1) * this.nominalChunkSizeBytes
            : this.nominalChunkSizeBytes;

        if (chunkSize <= 0 || chunkSize > this.nominalChunkSizeBytes) {
          throw new SyncEngineError(
            'PARTITION_ERROR',
            `Invalid calculated chunk size ${chunkSize} at index ${i}`
          );
        }

        const chunkSha256 = this.artifact.computeSliceSha256(offset, chunkSize);

        slices[i] = Object.freeze({
          chunkIndex: i,
          totalChunks: this.totalChunks,
          chunkSizeBytes: chunkSize,
          chunkSha256,
        });
      }

      this.slices = Object.freeze(slices);
    } catch (err) {
      // Lease cleanup on any failure during partition initialization
      if (this.#hasLease) {
        this.artifact.releaseLease(this.transferId);
        this.#hasLease = false;
      }
      throw err;
    }
  }

  /**
   * Static factory establishing a canonical PartitionedSnapshot.
   * If config.transferId is provided, acquires a transfer lease; otherwise creates partition
   * without holding a lease.
   */
  public static partition(
    artifact: SerializedSnapshotArtifact,
    config: ChunkProducerConfig = {}
  ): PartitionedSnapshot {
    return new SnapshotChunkProducer(artifact, {
      ...config,
      acquireLease: config.acquireLease ?? (config.transferId !== undefined),
    });
  }

  #assertReadable(): void {
    if (this.#isDisposed || this.artifact.isDisposed) {
      throw new SyncEngineError(
        'ARTIFACT_DISPOSED',
        '[ARTIFACT_DISPOSED] SnapshotChunkProducer or backing SerializedSnapshotArtifact has been disposed'
      );
    }
  }

  /**
   * Retrieves chunk slice metadata by index, or undefined if out of range.
   */
  public getSlice(chunkIndex: number): SnapshotChunkSlice | undefined {
    return this.slices[chunkIndex];
  }

  /**
   * Encodes a chunk slice into a canonical Base64 string via the encapsulated artifact.
   * Raw buffer views never escape.
   */
  public encodeChunkBase64(chunkIndex: number): string {
    this.#assertReadable();
    const slice = this.slices[chunkIndex];
    if (!slice) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `Chunk index ${chunkIndex} is out of bounds for totalChunks ${this.totalChunks}`
      );
    }
    const offset = chunkIndex * this.nominalChunkSizeBytes;
    return this.artifact.encodeSliceToBase64(offset, slice.chunkSizeBytes);
  }

  /**
   * Produces a fully validated SnapshotChunkPayload conforming to the frozen M4.1 wire schema.
   */
  public produceChunkPayload(
    chunkIndex: number,
    context?: Partial<ChunkProductionContext>
  ): SnapshotChunkPayload {
    this.#assertReadable();
    const slice = this.slices[chunkIndex];
    if (!slice) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `Chunk index ${chunkIndex} is out of bounds for totalChunks ${this.totalChunks}`
      );
    }

    const transferId = context?.transferId ?? this.transferId;
    const sessionId = this.artifact.sessionId;
    const membershipEpoch = context?.membershipEpoch ?? this.artifact.membershipEpoch;
    if (context?.membershipEpoch !== undefined && context.membershipEpoch !== this.artifact.membershipEpoch) {
      throw new SyncEngineError(
        'REJECTED_OBSOLETE_EPOCH',
        `Context membershipEpoch (${context.membershipEpoch}) does not match artifact membershipEpoch (${this.artifact.membershipEpoch})`
      );
    }
    const supplierPeerId = context?.supplierPeerId ?? this.artifact.authorPeerId;
    const snapshotId = this.artifact.snapshotId;

    const dataBase64 = this.encodeChunkBase64(chunkIndex);

    const payload: SnapshotChunkPayload = {
      transferId,
      sessionId,
      membershipEpoch,
      supplierPeerId,
      snapshotId,
      chunkIndex,
      totalChunks: this.totalChunks,
      chunkSizeBytes: slice.chunkSizeBytes,
      totalSizeBytes: this.artifact.totalSizeBytes,
      chunkSha256: slice.chunkSha256,
      dataBase64,
    };

    // Strict validation against frozen M4.1 schema (Base64 length, syntax, roundtrip, consistency)
    const parseResult = SnapshotChunkPayloadSchema.safeParse(payload);
    if (!parseResult.success) {
      throw new SyncEngineError(
        'PARTITION_ERROR',
        `Generated chunk payload failed validation: ${parseResult.error.message}`
      );
    }

    return parseResult.data;
  }

  /**
   * Produces a complete ProtocolMessage<SnapshotChunkPayload> wire envelope, measures exact
   * complete-message UTF-8 byte length, and strictly verifies it against the effective transport limit.
   * Throws SyncEngineError('TRANSPORT_LIMIT_EXCEEDED') if the complete serialized frame exceeds the limit.
   */
  public produceChunk(
    chunkIndex: number,
    context?: Partial<ChunkProductionContext>
  ): ProducedChunkMessage {
    const payload = this.produceChunkPayload(chunkIndex, context);

    const supplierPeerId = payload.supplierPeerId;
    const messageId = context?.messageId ?? `msg-${generateUUID()}`;
    const connectionGeneration = context?.connectionGeneration ?? 1;

    const message: ProtocolMessage<SnapshotChunkPayload> = {
      protocolVersion: PROTOCOL_VERSION,
      messageType: MessageType.SNAPSHOT_CHUNK,
      sessionId: payload.sessionId,
      senderPeerId: supplierPeerId,
      messageId,
      membershipEpoch: payload.membershipEpoch,
      connectionGeneration,
      payload,
    };

    // Strict validation against wire envelope schema (prohibiting replication metadata, stripping unknown keys)
    const envelopeResult = SnapshotChunkEnvelopeSchema.safeParse(message);
    if (!envelopeResult.success) {
      throw new SyncEngineError(
        'PARTITION_ERROR',
        `Generated chunk envelope message failed validation: ${envelopeResult.error.message}`
      );
    }

    const serializedJson = JSON.stringify(envelopeResult.data);
    const byteLength = new TextEncoder().encode(serializedJson).byteLength;

    if (byteLength > this.effectiveLimit) {
      throw new SyncEngineError(
        'TRANSPORT_LIMIT_EXCEEDED',
        `Complete serialized chunk message byte length (${byteLength} B) exceeds effective transport limit (${this.effectiveLimit} B)`
      );
    }

    return {
      ok: true,
      payload,
      message: envelopeResult.data as ProtocolMessage<SnapshotChunkPayload>,
      serializedJson,
      byteLength,
    };
  }

  /**
   * Releases the transfer lease acquired on the artifact.
   * Returns true if a lease was actively held and released; false if already released.
   */
  public releaseLease(): boolean {
    if (!this.#hasLease) {
      return false;
    }
    this.#hasLease = false;
    return this.artifact.releaseLease(this.transferId);
  }

  /**
   * Explicitly disposes the producer, releasing any active transfer lease.
   */
  public dispose(): void {
    if (this.#isDisposed) {
      return;
    }
    this.#isDisposed = true;
    this.releaseLease();
  }

  public get isDisposed(): boolean {
    return this.#isDisposed || this.artifact.isDisposed;
  }

  public get isLeaseActive(): boolean {
    return this.#hasLease;
  }
}
