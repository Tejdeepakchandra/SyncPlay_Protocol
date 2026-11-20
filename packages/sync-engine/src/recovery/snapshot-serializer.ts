/**
 * @module @syncplay/sync-engine/recovery
 *
 * Implements Phase 7.5 M4.2a Snapshot Serialization & JCS Digest:
 * - Deterministic RFC 8785 JSON canonicalization.
 * - Direct SHA-256 computation over master Uint8Array, avoiding duplicate TextEncoder allocation.
 * - Immediate preimage and canonical JSON string dereferencing post-encoding.
 * - Encapsulated SerializedSnapshotArtifact with private backing buffer and lease ownership.
 */

import {
  MAX_SNAPSHOT_BYTES,
  type StateSnapshotPayloadV2,
  type CompactedCheckpointItem,
} from '@syncplay/protocol';
import { canonicalizeJson, sha256 } from '../log/operation-fingerprint.js';
import { SyncEngineError } from '../errors.js';
import { exportEngineSnapshotV2 } from '../snapshot.js';
import type { PlaybackState } from '../types.js';

/**
 * Parameters required to construct a SerializedSnapshotArtifact.
 */
export interface SerializedSnapshotArtifactParams {
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly authorPeerId: string;
  readonly totalSizeBytes: number;
  readonly sha256Digest: string;
  readonly masterBuffer: ArrayBuffer;
  readonly onDisposed?: () => void;
}

/**
 * Helper to encode a Uint8Array slice into Base64 across Node and browser runtimes.
 */
function uint8ArrayToBase64(bytes: Uint8Array): string {
  const maybeBuffer = (
    globalThis as unknown as {
      Buffer?: { from: (data: Uint8Array) => { toString: (enc: string) => string } };
    }
  ).Buffer;
  if (typeof maybeBuffer !== 'undefined' && typeof maybeBuffer.from === 'function') {
    return maybeBuffer.from(bytes).toString('base64');
  }
  let binary = '';
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  return btoa(binary);
}

/**
 * Encapsulated artifact holding canonical, deterministically serialized snapshot bytes.
 * The underlying ArrayBuffer is private and exclusively owned by the artifact.
 * Range encoding and hashing methods return immutable primitive strings.
 * Zero mutable typed-array views are exposed publicly.
 */
export class SerializedSnapshotArtifact {
  #masterBuffer: ArrayBuffer | null;
  #masterBytes: Uint8Array | null;

  readonly #activeLeases = new Set<string>();
  #disposalRequested = false;
  #isDisposed = false;
  readonly #onDisposed?: () => void;

  public readonly snapshotId: string;
  public readonly sessionId: string;
  public readonly membershipEpoch: number;
  public readonly authorPeerId: string;
  public readonly totalSizeBytes: number;
  public readonly sha256Digest: string;

  constructor(params: SerializedSnapshotArtifactParams) {
    if (params.totalSizeBytes <= 0 || params.totalSizeBytes > MAX_SNAPSHOT_BYTES) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `Artifact totalSizeBytes must be between 1 and ${MAX_SNAPSHOT_BYTES}, got ${params.totalSizeBytes}`
      );
    }
    if (params.masterBuffer.byteLength !== params.totalSizeBytes) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `masterBuffer.byteLength (${params.masterBuffer.byteLength}) must equal totalSizeBytes (${params.totalSizeBytes})`
      );
    }
    this.snapshotId = params.snapshotId;
    this.sessionId = params.sessionId;
    this.membershipEpoch = params.membershipEpoch;
    this.authorPeerId = params.authorPeerId;
    this.totalSizeBytes = params.totalSizeBytes;
    this.sha256Digest = params.sha256Digest.toLowerCase();
    this.#onDisposed = params.onDisposed;

    this.#masterBuffer = params.masterBuffer;
    this.#masterBytes = new Uint8Array(this.#masterBuffer);
  }

  #assertReadable(): void {
    if (this.#isDisposed || this.#masterBytes === null || this.#masterBuffer === null) {
      throw new SyncEngineError(
        'ARTIFACT_DISPOSED',
        '[ARTIFACT_DISPOSED] SerializedSnapshotArtifact has been disposed'
      );
    }
  }

  #assertRange(offset: number, length: number): void {
    this.#assertReadable();
    if (
      !Number.isInteger(offset) ||
      !Number.isInteger(length) ||
      offset < 0 ||
      length < 0 ||
      offset + length > this.totalSizeBytes
    ) {
      throw new SyncEngineError(
        'INVALID_RANGE',
        `[INVALID_RANGE] Invalid slice range: offset ${offset}, length ${length}, totalSizeBytes ${this.totalSizeBytes}`
      );
    }
  }

  /**
   * Synchronously Base64-encodes a slice range.
   * Master buffer and typed-array views NEVER leave the artifact instance.
   * Returns an immutable primitive string.
   * Throws ARTIFACT_DISPOSED if disposed; throws INVALID_RANGE if offset/length out of bounds.
   */
  public encodeSliceToBase64(offset: number, length: number): string {
    this.#assertRange(offset, length);
    const slice = this.#masterBytes!.subarray(offset, offset + length);
    return uint8ArrayToBase64(slice);
  }

  /**
   * Synchronously computes SHA-256 hex digest for a slice range.
   * Returns an immutable 64-character lowercase hex string.
   * Throws ARTIFACT_DISPOSED if disposed; throws INVALID_RANGE if offset/length out of bounds.
   */
  public computeSliceSha256(offset: number, length: number): string {
    this.#assertRange(offset, length);
    const slice = this.#masterBytes!.subarray(offset, offset + length);
    return sha256(slice).toLowerCase();
  }

  /**
   * Public accessor returning a freshly allocated, completely unaliased deep copy of a byte range.
   */
  public copyByteRange(offset: number, length: number): Uint8Array {
    this.#assertRange(offset, length);
    return this.#masterBytes!.slice(offset, offset + length);
  }

  /**
   * Public accessor returning a complete unaliased deep copy of the entire master buffer.
   */
  public cloneBytes(): Uint8Array {
    this.#assertReadable();
    return new Uint8Array(this.#masterBuffer!.slice(0));
  }

  /**
   * Registers an active transfer lease.
   * Throws ARTIFACT_DISPOSED if disposal has been requested or finalized.
   * Throws INVALID_STATE if the transferId already holds an active lease.
   */
  public acquireLease(transferId: string): void {
    if (this.#isDisposed || this.#disposalRequested) {
      throw new SyncEngineError(
        'ARTIFACT_DISPOSED',
        '[ARTIFACT_DISPOSED] Cannot acquire lease on disposed artifact or artifact pending disposal'
      );
    }
    if (this.#activeLeases.has(transferId)) {
      throw new SyncEngineError(
        'INVALID_STATE',
        `[INVALID_STATE] Transfer lease already acquired for transferId: ${transferId}`
      );
    }
    this.#activeLeases.add(transferId);
  }

  /**
   * Releases an active transfer lease.
   * Idempotent: returns false if transferId was not active, true if released.
   * If disposal was requested and activeLeaseCount reaches 0, detaches buffer immediately.
   */
  public releaseLease(transferId: string): boolean {
    if (!this.#activeLeases.has(transferId)) {
      return false;
    }
    this.#activeLeases.delete(transferId);
    if (this.#disposalRequested && this.#activeLeases.size === 0) {
      this.#finalizeDisposal();
    }
    return true;
  }

  /**
   * Disposes the artifact.
   * If active leases exist, marks disposalRequested = true and defers buffer detachment.
   * If activeLeaseCount === 0, detaches buffer immediately (#masterBuffer = null, #masterBytes = null).
   */
  public dispose(): void {
    if (this.#isDisposed) {
      return;
    }
    if (this.#activeLeases.size > 0) {
      this.#disposalRequested = true;
    } else {
      this.#finalizeDisposal();
    }
  }

  #finalizeDisposal(): void {
    this.#isDisposed = true;
    this.#disposalRequested = false;
    this.#masterBuffer = null;
    this.#masterBytes = null;
    this.#activeLeases.clear();
    this.#onDisposed?.();
  }

  public get isDisposed(): boolean {
    return this.#isDisposed;
  }

  public get isDisposalRequested(): boolean {
    return this.#disposalRequested;
  }

  public get activeLeaseCount(): number {
    return this.#activeLeases.size;
  }
}

/**
 * Deterministic serializer for StateSnapshotPayloadV2 conforming to Phase 7.5 M4.2.
 */
export class SnapshotSerializer {
  /**
   * Serializes a validated StateSnapshotPayloadV2 into a deterministic artifact.
   * Encodes canonical bytes directly to master ArrayBuffer before computing SHA-256
   * to eliminate duplicate 4.19 MiB TextEncoder buffer allocations.
   * Immediately dereferences preimage and JSON string upon master buffer allocation.
   * Throws SyncEngineError('SNAPSHOT_ERROR') if serialization fails or exceeds MAX_SNAPSHOT_BYTES.
   */
  public static serialize(snapshot: StateSnapshotPayloadV2): SerializedSnapshotArtifact {
    if (!snapshot || typeof snapshot !== 'object') {
      throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot must be a non-null object');
    }

    if (!snapshot.snapshotId || typeof snapshot.snapshotId !== 'string') {
      throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot must have a valid snapshotId string');
    }
    if (!snapshot.sessionId || typeof snapshot.sessionId !== 'string') {
      throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot must have a valid sessionId string');
    }
    if (
      typeof snapshot.membershipEpoch !== 'number' ||
      !Number.isInteger(snapshot.membershipEpoch) ||
      snapshot.membershipEpoch < 0
    ) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        'Snapshot must have a non-negative integer membershipEpoch'
      );
    }
    if (!snapshot.authorPeerId || typeof snapshot.authorPeerId !== 'string') {
      throw new SyncEngineError('SNAPSHOT_ERROR', 'Snapshot must have a valid authorPeerId string');
    }

    // Pre-serialization structural bounds:
    // 1. snapshotFrontier keys <= 64
    if (snapshot.snapshotFrontier && typeof snapshot.snapshotFrontier === 'object') {
      const frontierKeys = Object.keys(snapshot.snapshotFrontier);
      if (frontierKeys.length > 64) {
        throw new SyncEngineError(
          'SNAPSHOT_ERROR',
          `Pre-serialization error: snapshotFrontier key count exceeds 64 (${frontierKeys.length} > 64)`
        );
      }
    }

    // 2. originFrontierTimestamps keys <= 64
    if (snapshot.originFrontierTimestamps && typeof snapshot.originFrontierTimestamps === 'object') {
      const tsKeys = Object.keys(snapshot.originFrontierTimestamps);
      if (tsKeys.length > 64) {
        throw new SyncEngineError(
          'SNAPSHOT_ERROR',
          `Pre-serialization error: originFrontierTimestamps key count exceeds 64 (${tsKeys.length} > 64)`
        );
      }
    }

    // 3. compactedCheckpoints <= 256
    if (Array.isArray(snapshot.compactedCheckpoints) && snapshot.compactedCheckpoints.length > 256) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `Pre-serialization error: compactedCheckpoints count exceeds 256 (${snapshot.compactedCheckpoints.length} > 256)`
      );
    }

    // 4. state must be a valid plain object with finite numeric fields
    if (!snapshot.state || typeof snapshot.state !== 'object') {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        'Pre-serialization error: state must be a valid object'
      );
    }
    const stateObj = snapshot.state as Record<string, unknown>;
    const anchorPos = stateObj['anchorPositionMs'];
    const pRate = stateObj['playbackRate'];
    if (
      typeof anchorPos !== 'number' ||
      !Number.isFinite(anchorPos) ||
      typeof pRate !== 'number' ||
      !Number.isFinite(pRate)
    ) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        'Pre-serialization error: state must have finite numeric anchorPositionMs and playbackRate'
      );
    }

    const snapshotId = snapshot.snapshotId;
    const sessionId = snapshot.sessionId;
    const membershipEpoch = snapshot.membershipEpoch;
    const authorPeerId = snapshot.authorPeerId;

    // Prepare preimage: omit sha256Digest and sort compactedCheckpoints deterministically
    let sortedCheckpoints: CompactedCheckpointItem[] | undefined;
    if (Array.isArray(snapshot.compactedCheckpoints)) {
      sortedCheckpoints = [...snapshot.compactedCheckpoints].sort((a, b) => {
        const cmp = String(a.originPeerId).localeCompare(String(b.originPeerId));
        if (cmp !== 0) return cmp;
        return Number(a.sequenceNumber) - Number(b.sequenceNumber);
      });
    }

    const { sha256Digest: _, ...rest } = snapshot as Record<string, unknown>;

    let preimage: Record<string, unknown> | null = {
      ...rest,
      ...(sortedCheckpoints ? { compactedCheckpoints: sortedCheckpoints } : {}),
    };

    let canonical: string | null = null;
    try {
      canonical = canonicalizeJson(preimage);
    } catch (e) {
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `Snapshot canonicalization failed: ${e instanceof Error ? e.message : String(e)}`
      );
    }

    // Allocate master buffer
    let masterBytes: Uint8Array | null = new TextEncoder().encode(canonical);

    // Immediate dereference: drop intermediate preimage object and canonical JSON string
    preimage = null;
    canonical = null;

    if (masterBytes.byteLength > MAX_SNAPSHOT_BYTES) {
      const len = masterBytes.byteLength;
      masterBytes = null;
      throw new SyncEngineError(
        'SNAPSHOT_ERROR',
        `Serialized snapshot exceeds MAX_SNAPSHOT_BYTES (${len} > ${MAX_SNAPSHOT_BYTES})`
      );
    }

    // Direct SHA-256 hash on masterBytes (Uint8Array, avoids duplicate TextEncoder allocation)
    const sha256Digest = sha256(masterBytes).toLowerCase();

    // Ensure masterBuffer is unaliased and byteLength matches
    let masterBuffer: ArrayBuffer;
    if (masterBytes.byteOffset === 0 && masterBytes.byteLength === masterBytes.buffer.byteLength) {
      masterBuffer = masterBytes.buffer as ArrayBuffer;
    } else {
      masterBuffer = masterBytes.buffer.slice(
        masterBytes.byteOffset,
        masterBytes.byteOffset + masterBytes.byteLength
      ) as ArrayBuffer;
    }

    const totalSizeBytes = masterBytes.byteLength;
    masterBytes = null;

    return new SerializedSnapshotArtifact({
      snapshotId,
      sessionId,
      membershipEpoch,
      authorPeerId,
      totalSizeBytes,
      sha256Digest,
      masterBuffer,
    });
  }

  /**
   * Exports and serializes an active engine state into a deterministic artifact.
   * Captures state inside the serialized gate.
   */
  public static serializeFromEngine(
    state: PlaybackState,
    context: {
      sessionId: string;
      membershipEpoch: number;
      authorPeerId: string;
      snapshotFrontier: Record<string, number>;
      originFrontierTimestamps: Record<string, number>;
      compactedCheckpoints: ReadonlyArray<CompactedCheckpointItem>;
      snapshotId?: string;
    }
  ): SerializedSnapshotArtifact {
    const snapshot = exportEngineSnapshotV2(
      state,
      context.sessionId,
      context.membershipEpoch,
      context.authorPeerId,
      context.snapshotFrontier,
      context.originFrontierTimestamps,
      context.compactedCheckpoints,
      context.snapshotId
    );
    return SnapshotSerializer.serialize(snapshot);
  }
}
