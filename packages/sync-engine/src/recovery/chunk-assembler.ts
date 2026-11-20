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

export interface FailoverCandidateParams {
  transferId: string;
  supplierPeerId: string;
  membershipEpoch: number;
  connectionGeneration?: number;
  snapshotId: string;
  expectedSha256Digest: string;
  totalChunks: number;
  totalSizeBytes: number;
  nominalChunkSizeBytes: number;
  sessionId?: string;
}

export type FailoverRejectReason =
  | 'NO_ACTIVE_TRANSFER'
  | 'DISPOSED'
  | 'INVALID_PARAMETERS'
  | 'SESSION_MISMATCH'
  | 'EPOCH_MISMATCH'
  | 'SNAPSHOT_ID_MISMATCH'
  | 'DIGEST_MISMATCH'
  | 'GEOMETRY_MISMATCH';

export type PrepareForFailoverResult =
  | {
      ok: true;
      retainedChunks: number;
    }
  | {
      ok: false;
      reason: FailoverRejectReason;
      detail: string;
      retainedChunks: 0;
    };

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
      throw new Error('Transfer already active. Call abortTransfer(), reset(), or restartTransfer() first.');
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
   * Prepares the assembler for failover to a new supplier in the same membership epoch.
   *
   * Verifies that the failover candidate matches all immutable transfer identity and geometry:
   * sessionId (if provided), membershipEpoch, snapshotId, expectedSha256Digest, totalSizeBytes,
   * totalChunks, and nominalChunkSizeBytes.
   *
   * Non-destructive: if the candidate is incompatible or invalid, it is rejected with a specific
   * reason, and the active transfer, its received chunks, and state remain completely unchanged.
   *
   * Only upon passing all compatibility and parameter checks are the new transferId,
   * supplierPeerId, and connectionGeneration installed, retaining all verified chunks.
   */
  public prepareForFailover(params: FailoverCandidateParams): PrepareForFailoverResult {
    if (this.disposed) {
      return {
        ok: false,
        reason: 'DISPOSED',
        detail: 'ChunkAssembler is disposed',
        retainedChunks: 0,
      };
    }
    if (!this.transferParams) {
      return {
        ok: false,
        reason: 'NO_ACTIVE_TRANSFER',
        detail: 'No active transfer session to failover',
        retainedChunks: 0,
      };
    }

    // 1. Candidate parameter validation (must not modify active transfer on invalid inputs)
    if (params === null || typeof params !== 'object') {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'Failover candidate parameters must be an object',
        retainedChunks: 0,
      };
    }
    if (typeof params.transferId !== 'string' || params.transferId.trim() === '') {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'transferId must be a non-empty string',
        retainedChunks: 0,
      };
    }
    if (typeof params.supplierPeerId !== 'string' || params.supplierPeerId.trim() === '') {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'supplierPeerId must be a non-empty string',
        retainedChunks: 0,
      };
    }
    if (
      typeof params.membershipEpoch !== 'number' ||
      !Number.isInteger(params.membershipEpoch) ||
      params.membershipEpoch < 0
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'membershipEpoch must be a non-negative integer',
        retainedChunks: 0,
      };
    }
    if (
      params.connectionGeneration !== undefined &&
      (typeof params.connectionGeneration !== 'number' ||
        !Number.isInteger(params.connectionGeneration) ||
        params.connectionGeneration < 0)
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'connectionGeneration must be a non-negative integer when provided',
        retainedChunks: 0,
      };
    }
    if (typeof params.snapshotId !== 'string' || params.snapshotId.trim() === '') {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'snapshotId must be a non-empty string',
        retainedChunks: 0,
      };
    }
    if (
      typeof params.expectedSha256Digest !== 'string' ||
      !/^[0-9a-fA-F]{64}$/.test(params.expectedSha256Digest)
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: 'expectedSha256Digest must be a valid 64-character hexadecimal SHA-256 hash',
        retainedChunks: 0,
      };
    }
    if (
      typeof params.totalSizeBytes !== 'number' ||
      !Number.isInteger(params.totalSizeBytes) ||
      params.totalSizeBytes <= 0 ||
      params.totalSizeBytes > this.maxSnapshotBytes
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: `totalSizeBytes (${params.totalSizeBytes}) must be positive and not exceed maxSnapshotBytes (${this.maxSnapshotBytes})`,
        retainedChunks: 0,
      };
    }
    if (
      typeof params.nominalChunkSizeBytes !== 'number' ||
      !Number.isInteger(params.nominalChunkSizeBytes) ||
      params.nominalChunkSizeBytes < this.minRawChunkBytes ||
      params.nominalChunkSizeBytes > this.maxChunkBytes
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: `nominalChunkSizeBytes (${params.nominalChunkSizeBytes}) must be between ${this.minRawChunkBytes} and ${this.maxChunkBytes}`,
        retainedChunks: 0,
      };
    }
    const expectedChunks = Math.ceil(params.totalSizeBytes / params.nominalChunkSizeBytes);
    if (
      typeof params.totalChunks !== 'number' ||
      !Number.isInteger(params.totalChunks) ||
      params.totalChunks !== expectedChunks
    ) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: `totalChunks (${params.totalChunks}) does not match expected canonical partition count (${expectedChunks})`,
        retainedChunks: 0,
      };
    }
    if (params.totalChunks < 1 || params.totalChunks > this.maxChunks) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: `totalChunks (${params.totalChunks}) must be between 1 and ${this.maxChunks}`,
        retainedChunks: 0,
      };
    }
    const finalChunkSize =
      params.totalSizeBytes - (params.totalChunks - 1) * params.nominalChunkSizeBytes;
    if (finalChunkSize < 1 || finalChunkSize > params.nominalChunkSizeBytes) {
      return {
        ok: false,
        reason: 'INVALID_PARAMETERS',
        detail: `Final chunk size (${finalChunkSize}) must be at least 1 and not exceed nominalChunkSizeBytes (${params.nominalChunkSizeBytes})`,
        retainedChunks: 0,
      };
    }

    // 2. Compatibility predicate check against active transfer
    // Session binding: candidate must match active session (if provided)
    if (params.sessionId !== undefined && params.sessionId !== this.transferParams.sessionId) {
      return {
        ok: false,
        reason: 'SESSION_MISMATCH',
        detail: `Candidate sessionId '${params.sessionId}' does not match active transfer sessionId '${this.transferParams.sessionId}'`,
        retainedChunks: 0,
      };
    }

    // Epoch fencing: snapshot recovery is strictly epoch-scoped
    if (params.membershipEpoch !== this.transferParams.membershipEpoch) {
      return {
        ok: false,
        reason: 'EPOCH_MISMATCH',
        detail: `Candidate membershipEpoch ${params.membershipEpoch} does not match active transfer epoch ${this.transferParams.membershipEpoch}`,
        retainedChunks: 0,
      };
    }

    // Snapshot ID match
    if (params.snapshotId !== this.transferParams.snapshotId) {
      return {
        ok: false,
        reason: 'SNAPSHOT_ID_MISMATCH',
        detail: `Candidate snapshotId '${params.snapshotId}' does not match active transfer snapshotId '${this.transferParams.snapshotId}'`,
        retainedChunks: 0,
      };
    }

    // Expected digest match (case-insensitive)
    if (
      params.expectedSha256Digest.toLowerCase() !==
      this.transferParams.expectedSha256Digest.toLowerCase()
    ) {
      return {
        ok: false,
        reason: 'DIGEST_MISMATCH',
        detail: `Candidate digest '${params.expectedSha256Digest}' does not match active transfer digest '${this.transferParams.expectedSha256Digest}'`,
        retainedChunks: 0,
      };
    }

    // Geometry match (totalSizeBytes, totalChunks, nominalChunkSizeBytes)
    if (
      params.totalSizeBytes !== this.transferParams.totalSizeBytes ||
      params.totalChunks !== this.transferParams.totalChunks ||
      params.nominalChunkSizeBytes !== this.transferParams.nominalChunkSizeBytes
    ) {
      return {
        ok: false,
        reason: 'GEOMETRY_MISMATCH',
        detail: `Candidate geometry (size: ${params.totalSizeBytes}, chunks: ${params.totalChunks}, nominal: ${params.nominalChunkSizeBytes}) does not match active transfer (size: ${this.transferParams.totalSizeBytes}, chunks: ${this.transferParams.totalChunks}, nominal: ${this.transferParams.nominalChunkSizeBytes})`,
        retainedChunks: 0,
      };
    }

    // 3. Candidate is compatible: update supplier and transfer attempt context
    this.transferParams.transferId = params.transferId;
    this.transferParams.supplierPeerId = params.supplierPeerId;
    this.transferParams.connectionGeneration = params.connectionGeneration;

    return {
      ok: true,
      retainedChunks: this.receivedChunksCount,
    };
  }

  /**
   * Explicitly restarts the recovery transfer session with new parameters.
   * Purges all previously buffered chunks and metadata before initializing.
   */
  public restartTransfer(params: InitTransferParams): void {
    this.abortTransfer();
    this.initTransfer(params);
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
