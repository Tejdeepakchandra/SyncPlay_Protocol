/**
 * Isolated Bounded Chunk Assembler.
 *
 * Implements Phase 7.5 M4.1 recovery component:
 * - Collects, validates, hashes, and reassembles snapshot chunks.
 * - Enforces single active transfer context and transport limits.
 * - Idempotently handles duplicate chunks; rejects conflicting chunks.
 * - Supports failover chunk preservation on identical snapshot match.
 * - Computes and verifies overall SHA-256 before releasing assembled buffer.
 * - Immediately dereferences chunk slice memory upon finalization (AC-M4-MEM-3).
 * - Strictly isolated: no snapshot installation, clock reseeding, playback
 *   mutation, or network calls.
 */

import {
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  type SnapshotAbortReason,
  type SnapshotChunkPayload,
} from '@syncplay/protocol';
import { sha256 } from '../log/operation-fingerprint.js';

export interface ChunkAssemblerConfig {
  maxSnapshotBytes?: number;
  maxChunks?: number;
  maxChunkBytes?: number;
  minRawChunkBytes?: number;
}

export interface InitTransferParams {
  transferId: string;
  sessionId: string;
  snapshotId: string;
  totalChunks: number;
  totalSizeBytes: number;
  nominalChunkSizeBytes: number;
  expectedSha256Digest: string;
  supplierPeerId: string;
  membershipEpoch: number;
  connectionGeneration?: number;
}

export interface ChunkEnvelopeContext {
  sessionId: string;
  senderPeerId: string;
  membershipEpoch: number;
  connectionGeneration?: number;
}

export type IngestChunkResult =
  | { ok: true; duplicate: boolean; completed: boolean }
  | { ok: false; reason: SnapshotAbortReason; detail: string };

export interface AssembledSnapshot {
  buffer: Uint8Array;
  snapshotId: string;
  sha256Digest: string;
  totalSizeBytes: number;
}

export type FinalizeResult =
  | { ok: true; snapshot: AssembledSnapshot }
  | { ok: false; reason: SnapshotAbortReason; detail: string };

export interface TransferProgress {
  receivedChunks: number;
  totalChunks: number;
  receivedBytes: number;
  totalSizeBytes: number;
}

export class ChunkAssembler {
  private readonly maxSnapshotBytes: number;
  private readonly maxChunks: number;
  private readonly maxChunkBytes: number;
  private readonly minRawChunkBytes: number;

  private transferParams: InitTransferParams | null = null;
  private chunks: Array<Uint8Array | null> | null = null;
  private chunkHashes: Array<string | null> | null = null;
  private receivedChunksCount = 0;
  private receivedBytesCount = 0;
  private disposed = false;

  constructor(config: ChunkAssemblerConfig = {}) {
    this.maxSnapshotBytes = config.maxSnapshotBytes ?? MAX_SNAPSHOT_BYTES;
    this.maxChunks = config.maxChunks ?? MAX_CHUNKS_PER_SNAPSHOT;
    this.maxChunkBytes = config.maxChunkBytes ?? MAX_CHUNK_BYTES;
    this.minRawChunkBytes = config.minRawChunkBytes ?? MIN_RAW_CHUNK_BYTES;
  }

  /**
   * Initializes a new recovery transfer session.
   * Throws if an existing transfer is active (must be aborted or reset first).
   * Validates exact canonical partition invariants before allocating storage.
   */
  public initTransfer(params: InitTransferParams): void {
    if (this.disposed) {
      throw new Error('ChunkAssembler is disposed');
    }
    if (this.transferParams !== null) {
      throw new Error('Transfer already active. Call reset() or prepareForFailover() first.');
    }
    if (params.totalSizeBytes <= 0 || params.totalSizeBytes > this.maxSnapshotBytes) {
      throw new Error(
        `totalSizeBytes (${params.totalSizeBytes}) must be positive and not exceed maxSnapshotBytes (${this.maxSnapshotBytes})`
      );
    }
    if (
      params.nominalChunkSizeBytes < this.minRawChunkBytes ||
      params.nominalChunkSizeBytes > this.maxChunkBytes
    ) {
      throw new Error(
        `nominalChunkSizeBytes (${params.nominalChunkSizeBytes}) must be between ${this.minRawChunkBytes} and ${this.maxChunkBytes}`
      );
    }
    const expectedTotalChunks = Math.ceil(params.totalSizeBytes / params.nominalChunkSizeBytes);
    if (params.totalChunks !== expectedTotalChunks) {
      throw new Error(
        `totalChunks (${params.totalChunks}) does not match expected canonical partition count (${expectedTotalChunks}) for totalSizeBytes (${params.totalSizeBytes}) and nominalChunkSizeBytes (${params.nominalChunkSizeBytes})`
      );
    }
    if (params.totalChunks < 1 || params.totalChunks > this.maxChunks) {
      throw new Error(
        `totalChunks (${params.totalChunks}) must be between 1 and ${this.maxChunks}`
      );
    }
    const expectedFinalChunkSize =
      params.totalSizeBytes - (params.totalChunks - 1) * params.nominalChunkSizeBytes;
    if (expectedFinalChunkSize < 1 || expectedFinalChunkSize > params.nominalChunkSizeBytes) {
      throw new Error(
        `Final chunk size (${expectedFinalChunkSize}) must be at least 1 and not exceed nominalChunkSizeBytes (${params.nominalChunkSizeBytes})`
      );
    }

    this.transferParams = { ...params };
    this.chunks = new Array<Uint8Array | null>(params.totalChunks).fill(null);
    this.chunkHashes = new Array<string | null>(params.totalChunks).fill(null);
    this.receivedChunksCount = 0;
    this.receivedBytesCount = 0;
  }

  /**
   * Ingests a snapshot chunk payload with optional envelope validation context.
   * Performs trust boundary, session, epoch, size, and individual chunk SHA-256 verification.
   */
  public ingestChunk(
    payload: SnapshotChunkPayload,
    envelopeContext?: ChunkEnvelopeContext
  ): IngestChunkResult {
    if (this.disposed) {
      return { ok: false, reason: 'LOCAL_ABORT', detail: 'ChunkAssembler is disposed' };
    }
    if (!this.transferParams || !this.chunks || !this.chunkHashes) {
      return { ok: false, reason: 'IDENTITY_MISMATCH', detail: 'No active transfer session' };
    }

    // Envelope context verification if provided
    if (envelopeContext) {
      if (envelopeContext.sessionId !== this.transferParams.sessionId) {
        return {
          ok: false,
          reason: 'IDENTITY_MISMATCH',
          detail: `Envelope sessionId '${envelopeContext.sessionId}' does not match active transfer sessionId '${this.transferParams.sessionId}'`,
        };
      }
      if (envelopeContext.membershipEpoch !== this.transferParams.membershipEpoch) {
        return {
          ok: false,
          reason: 'EPOCH_MISMATCH',
          detail: `Envelope membershipEpoch ${envelopeContext.membershipEpoch} does not match active transfer epoch ${this.transferParams.membershipEpoch}`,
        };
      }
      if (envelopeContext.senderPeerId !== this.transferParams.supplierPeerId) {
        return {
          ok: false,
          reason: 'IDENTITY_MISMATCH',
          detail: `Envelope senderPeerId '${envelopeContext.senderPeerId}' does not match expected supplierPeerId '${this.transferParams.supplierPeerId}'`,
        };
      }
      if (
        envelopeContext.connectionGeneration !== undefined &&
        this.transferParams.connectionGeneration !== undefined &&
        envelopeContext.connectionGeneration !== this.transferParams.connectionGeneration
      ) {
        return {
          ok: false,
          reason: 'IDENTITY_MISMATCH',
          detail: `Envelope connectionGeneration ${envelopeContext.connectionGeneration} does not match expected generation ${this.transferParams.connectionGeneration}`,
        };
      }
    }

    // Payload metadata verification against active transfer
    if (payload.transferId !== this.transferParams.transferId) {
      return {
        ok: false,
        reason: 'IDENTITY_MISMATCH',
        detail: `Payload transferId '${payload.transferId}' does not match active '${this.transferParams.transferId}'`,
      };
    }
    if (payload.sessionId !== this.transferParams.sessionId) {
      return {
        ok: false,
        reason: 'IDENTITY_MISMATCH',
        detail: `Payload sessionId '${payload.sessionId}' does not match active '${this.transferParams.sessionId}'`,
      };
    }
    if (payload.snapshotId !== this.transferParams.snapshotId) {
      return {
        ok: false,
        reason: 'IDENTITY_MISMATCH',
        detail: `Payload snapshotId '${payload.snapshotId}' does not match active '${this.transferParams.snapshotId}'`,
      };
    }
    if (payload.supplierPeerId !== this.transferParams.supplierPeerId) {
      return {
        ok: false,
        reason: 'IDENTITY_MISMATCH',
        detail: `Payload supplierPeerId '${payload.supplierPeerId}' does not match active '${this.transferParams.supplierPeerId}'`,
      };
    }
    if (payload.membershipEpoch !== this.transferParams.membershipEpoch) {
      return {
        ok: false,
        reason: 'EPOCH_MISMATCH',
        detail: `Payload membershipEpoch ${payload.membershipEpoch} does not match active ${this.transferParams.membershipEpoch}`,
      };
    }
    if (
      payload.totalChunks !== this.transferParams.totalChunks ||
      payload.totalSizeBytes !== this.transferParams.totalSizeBytes
    ) {
      return {
        ok: false,
        reason: 'OFFER_INCONSISTENT',
        detail: 'Payload totalChunks or totalSizeBytes does not match active transfer parameters',
      };
    }
    if (payload.chunkIndex < 0 || payload.chunkIndex >= this.transferParams.totalChunks) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `chunkIndex ${payload.chunkIndex} out of bounds [0, ${this.transferParams.totalChunks})`,
      };
    }
    if (payload.chunkSizeBytes > this.maxChunkBytes) {
      return {
        ok: false,
        reason: 'TRANSPORT_LIMIT_EXCEEDED',
        detail: `chunkSizeBytes (${payload.chunkSizeBytes}) exceeds maxChunkBytes (${this.maxChunkBytes})`,
      };
    }

    // F-1: Canonical partition expected size verification for chunkIndex
    const expectedChunkSize =
      payload.chunkIndex < this.transferParams.totalChunks - 1
        ? this.transferParams.nominalChunkSizeBytes
        : this.transferParams.totalSizeBytes -
          (this.transferParams.totalChunks - 1) * this.transferParams.nominalChunkSizeBytes;

    if (payload.chunkSizeBytes !== expectedChunkSize) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Chunk at index ${payload.chunkIndex} declared chunkSizeBytes (${payload.chunkSizeBytes}), expected canonical partition size (${expectedChunkSize})`,
      };
    }

    // Decode Base64 data
    let rawBytes: Uint8Array;
    try {
      const maybeBuffer = (globalThis as unknown as { Buffer?: { from: (str: string, enc: string) => Uint8Array } }).Buffer;
      if (typeof maybeBuffer !== 'undefined' && typeof maybeBuffer.from === 'function') {
        const buf = maybeBuffer.from(payload.dataBase64, 'base64');
        rawBytes = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
      } else {
        const binaryString = atob(payload.dataBase64);
        rawBytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
          rawBytes[i] = binaryString.charCodeAt(i);
        }
      }
    } catch (e) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Failed to decode dataBase64: ${e instanceof Error ? e.message : String(e)}`,
      };
    }

    if (rawBytes.byteLength !== payload.chunkSizeBytes) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Decoded byte length (${rawBytes.byteLength}) does not match declared chunkSizeBytes (${payload.chunkSizeBytes})`,
      };
    }

    // Verify SHA-256 digest of raw chunk binary bytes
    const computedHash = sha256(rawBytes).toLowerCase();
    if (computedHash !== payload.chunkSha256.toLowerCase()) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Chunk SHA-256 digest mismatch: expected ${payload.chunkSha256}, computed ${computedHash}`,
      };
    }

    // Check if chunk at chunkIndex was already received
    const existingHash = this.chunkHashes[payload.chunkIndex];
    if (existingHash !== null) {
      if (existingHash === computedHash) {
        // Idempotent duplicate: identical chunk data
        return { ok: true, duplicate: true, completed: this.isComplete() };
      } else {
        // Conflicting duplicate: different data for same index
        return {
          ok: false,
          reason: 'CHUNK_CORRUPTED',
          detail: `Conflicting chunk data received for chunkIndex ${payload.chunkIndex}`,
        };
      }
    }

    // Store verified chunk
    this.chunks[payload.chunkIndex] = rawBytes;
    this.chunkHashes[payload.chunkIndex] = computedHash;
    this.receivedChunksCount++;
    this.receivedBytesCount += rawBytes.byteLength;

    return {
      ok: true,
      duplicate: false,
      completed: this.isComplete(),
    };
  }

  /**
   * Prepares the assembler for failover to a new supplier or epoch.
   * If snapshotId, expectedSha256Digest, totalChunks, and totalSizeBytes match bit-exact,
   * all previously received and verified chunks are retained.
   * Otherwise, all state is purged immediately.
   */
  public prepareForFailover(params: {
    transferId: string;
    supplierPeerId: string;
    membershipEpoch: number;
    connectionGeneration?: number;
    snapshotId: string;
    expectedSha256Digest: string;
    totalChunks: number;
    totalSizeBytes: number;
    nominalChunkSizeBytes: number;
  }): { retainedChunks: number } {
    if (this.disposed) {
      throw new Error('ChunkAssembler is disposed');
    }
    if (!this.transferParams) {
      throw new Error('No active transfer session to failover');
    }

    const isExactMatch =
      this.transferParams.snapshotId === params.snapshotId &&
      this.transferParams.expectedSha256Digest.toLowerCase() ===
        params.expectedSha256Digest.toLowerCase() &&
      this.transferParams.totalChunks === params.totalChunks &&
      this.transferParams.totalSizeBytes === params.totalSizeBytes &&
      this.transferParams.nominalChunkSizeBytes === params.nominalChunkSizeBytes;

    if (isExactMatch) {
      // Retain verified chunks and update context
      this.transferParams.transferId = params.transferId;
      this.transferParams.supplierPeerId = params.supplierPeerId;
      this.transferParams.membershipEpoch = params.membershipEpoch;
      this.transferParams.connectionGeneration = params.connectionGeneration;
      return { retainedChunks: this.receivedChunksCount };
    } else {
      // Incompatible snapshot: purge everything
      const sessionId = this.transferParams.sessionId;
      this.abortTransfer();
      this.initTransfer({
        ...params,
        sessionId,
      });
      return { retainedChunks: 0 };
    }
  }

  /** Returns true if all chunks for the active transfer have been received. */
  public isComplete(): boolean {
    return (
      this.transferParams !== null &&
      this.receivedChunksCount === this.transferParams.totalChunks
    );
  }

  /** Returns array of missing chunk indices. */
  public getMissingChunkIndices(): number[] {
    if (!this.transferParams || !this.chunks) return [];
    const missing: number[] = [];
    for (let i = 0; i < this.transferParams.totalChunks; i++) {
      if (this.chunks[i] === null) {
        missing.push(i);
      }
    }
    return missing;
  }

  /** Returns array of received chunk indices. */
  public getReceivedChunkIndices(): number[] {
    if (!this.transferParams || !this.chunks) return [];
    const received: number[] = [];
    for (let i = 0; i < this.transferParams.totalChunks; i++) {
      if (this.chunks[i] !== null) {
        received.push(i);
      }
    }
    return received;
  }

  /** Returns current transfer progress metrics. */
  public getProgress(): TransferProgress | null {
    if (!this.transferParams) return null;
    return {
      receivedChunks: this.receivedChunksCount,
      totalChunks: this.transferParams.totalChunks,
      receivedBytes: this.receivedBytesCount,
      totalSizeBytes: this.transferParams.totalSizeBytes,
    };
  }

  /** Returns active transfer metadata copy, or null if inactive. */
  public getActiveTransfer(): Readonly<InitTransferParams> | null {
    if (!this.transferParams) return null;
    return { ...this.transferParams };
  }

  /**
   * Finalizes snapshot assembly.
   * If complete, concatenates slices, verifies overall SHA-256,
   * immediately nullifies slice storage (AC-M4-MEM-3), and returns AssembledSnapshot.
   * Never exposes partial buffers.
   */
  public finalizeAssembly(): FinalizeResult {
    if (this.disposed) {
      return { ok: false, reason: 'LOCAL_ABORT', detail: 'ChunkAssembler is disposed' };
    }
    if (!this.transferParams || !this.chunks) {
      return { ok: false, reason: 'IDENTITY_MISMATCH', detail: 'No active transfer session' };
    }
    if (!this.isComplete()) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: 'Cannot finalize incomplete snapshot assembly',
      };
    }
    if (this.receivedBytesCount !== this.transferParams.totalSizeBytes) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Total received bytes (${this.receivedBytesCount}) does not match declared totalSizeBytes (${this.transferParams.totalSizeBytes})`,
      };
    }

    // Allocate contiguous buffer
    const assembled = new Uint8Array(this.transferParams.totalSizeBytes);
    let offset = 0;
    for (let i = 0; i < this.transferParams.totalChunks; i++) {
      const chunk = this.chunks[i];
      if (!chunk) {
        return {
          ok: false,
          reason: 'CHUNK_CORRUPTED',
          detail: `Missing chunk slice at index ${i}`,
        };
      }
      assembled.set(chunk, offset);
      offset += chunk.byteLength;
    }

    if (offset !== this.transferParams.totalSizeBytes) {
      return {
        ok: false,
        reason: 'CHUNK_CORRUPTED',
        detail: `Assembled byte length (${offset}) does not match expected (${this.transferParams.totalSizeBytes})`,
      };
    }

    // AC-M4-MEM-3: Nullify slice arrays immediately to release memory
    this.chunks = null;
    this.chunkHashes = null;

    // Verify overall SHA-256 digest
    const overallHash = sha256(assembled).toLowerCase();
    if (overallHash !== this.transferParams.expectedSha256Digest.toLowerCase()) {
      const expected = this.transferParams.expectedSha256Digest;
      this.abortTransfer();
      return {
        ok: false,
        reason: 'HASH_MISMATCH',
        detail: `Overall snapshot SHA-256 digest mismatch: expected ${expected}, computed ${overallHash}`,
      };
    }

    const snapshot: AssembledSnapshot = {
      buffer: assembled,
      snapshotId: this.transferParams.snapshotId,
      sha256Digest: overallHash,
      totalSizeBytes: this.transferParams.totalSizeBytes,
    };

    // F-4: Reset transfer state on successful finalization so new transfers can initialize cleanly
    this.transferParams = null;
    this.receivedChunksCount = 0;
    this.receivedBytesCount = 0;

    return { ok: true, snapshot };
  }

  /** Aborts active transfer and purges all buffered chunk data immediately. */
  public abortTransfer(): void {
    this.chunks = null;
    this.chunkHashes = null;
    this.transferParams = null;
    this.receivedChunksCount = 0;
    this.receivedBytesCount = 0;
  }

  /** Resets assembler state. Alias for abortTransfer. */
  public reset(): void {
    this.abortTransfer();
  }

  /** Disposes assembler and permanently releases all references. */
  public dispose(): void {
    this.disposed = true;
    this.abortTransfer();
  }
}
