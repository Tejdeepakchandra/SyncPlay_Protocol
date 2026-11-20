/**
 * @module @syncplay/sync-engine/recovery
 *
 * Implements Phase 7.5 M4.2a Snapshot Preparation Queue:
 * - Sequential execution with MAX_CONCURRENT_PREPARATIONS = 1.
 * - Strict Concurrency Admission: retainedArtifactCount + (isPreparing ? 1 : 0) <= 2.
 * - Queue depth bounding at MAX_PREPARATION_QUEUE_DEPTH = 4, rejecting with LOCAL_ABORT.
 * - Queue resumption when a retained artifact is released and disposed.
 */

import { SyncEngineError } from '../errors.js';
import type { SerializedSnapshotArtifact } from './snapshot-serializer.js';

export interface QueuedPreparationRequest {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly sctpMaxMessageSize: number;
  readonly requestedSnapshotId?: string;
  readonly timestampMs: number;
}

interface QueuedItem {
  readonly request: QueuedPreparationRequest;
  readonly captureAndSerializeFn: () => Promise<SerializedSnapshotArtifact> | SerializedSnapshotArtifact;
  readonly resolve: (artifact: SerializedSnapshotArtifact) => void;
  readonly reject: (error: unknown) => void;
}

export class SnapshotPreparationQueue {
  public static readonly MAX_CONCURRENT_PREPARATIONS = 1;
  public static readonly MAX_PREPARATION_QUEUE_DEPTH = 4;
  public static readonly MAX_TOTAL_ARTIFACTS = 2;

  #isPreparing = false;
  #retainedArtifactCount = 0;
  #queue: QueuedItem[] = [];

  /**
   * Enqueues an outbound snapshot preparation request.
   * Strict Concurrency Admission:
   *   retainedArtifactCount + (isPreparing ? 1 : 0) <= 2
   * If 2 artifacts are already retained, preparation does not start until one is released.
   * If queue is saturated (depth >= 4), rejects immediately with LOCAL_ABORT ('Preparation queue saturated').
   */
  public async enqueue(
    request: QueuedPreparationRequest,
    captureAndSerializeFn: () => Promise<SerializedSnapshotArtifact> | SerializedSnapshotArtifact
  ): Promise<SerializedSnapshotArtifact> {
    if (this.#queue.length >= SnapshotPreparationQueue.MAX_PREPARATION_QUEUE_DEPTH) {
      throw new SyncEngineError('LOCAL_ABORT', 'Preparation queue saturated');
    }

    return new Promise<SerializedSnapshotArtifact>((resolve, reject) => {
      this.#queue.push({
        request,
        captureAndSerializeFn,
        resolve,
        reject,
      });

      this.#drainQueue();
    });
  }

  #drainQueue(): void {
    if (this.#isPreparing) {
      return;
    }
    if (this.#queue.length === 0) {
      return;
    }
    // Strict Concurrency Admission:
    // retainedArtifactCount + (isPreparing ? 1 : 0) <= 2
    // If a preparation starts now, isPreparing becomes 1.
    // Hence retainedArtifactCount must be strictly less than MAX_TOTAL_ARTIFACTS (2).
    if (this.#retainedArtifactCount >= SnapshotPreparationQueue.MAX_TOTAL_ARTIFACTS) {
      return;
    }

    const item = this.#queue.shift()!;
    this.#isPreparing = true;

    (async () => {
      try {
        const artifact = await item.captureAndSerializeFn();
        this.#retainedArtifactCount++;
        item.resolve(artifact);
      } catch (err) {
        item.reject(err);
      } finally {
        this.#isPreparing = false;
        this.#drainQueue();
      }
    })();
  }

  /**
   * Called when a retained artifact is disposed, waking queued preparation requests.
   */
  public notifyArtifactDisposed(): void {
    if (this.#retainedArtifactCount > 0) {
      this.#retainedArtifactCount--;
    }
    this.#drainQueue();
  }

  /** Explicitly records that an artifact is retained (e.g. prepared externally). */
  public trackArtifactRetained(): void {
    this.#retainedArtifactCount++;
  }

  /** Explicitly records that a retained artifact was released/disposed. */
  public trackArtifactReleased(): void {
    this.notifyArtifactDisposed();
  }

  /** Number of pending preparation requests waiting in the queue. */
  public get depth(): number {
    return this.#queue.length;
  }

  /** Whether a preparation is currently executing. */
  public get isBusy(): boolean {
    return this.#isPreparing;
  }

  /** Current count of retained artifacts. */
  public get retainedArtifactCount(): number {
    return this.#retainedArtifactCount;
  }

  /** Clears the queue, rejecting all pending requests with LOCAL_ABORT. */
  public clear(): void {
    const error = new SyncEngineError('LOCAL_ABORT', 'Preparation queue cleared');
    const pending = [...this.#queue];
    this.#queue = [];
    for (const item of pending) {
      item.reject(error);
    }
  }
}
