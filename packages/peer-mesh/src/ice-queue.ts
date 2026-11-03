/**
 * ICE candidate queue.
 *
 * Queues ICE candidates received before setRemoteDescription() completes.
 * Flushes them in order once the remote description is set.
 * Bounded to prevent unbounded memory growth.
 */

export interface QueuedCandidate {
  readonly candidate: RTCIceCandidateInit;
  readonly generation: number;
}

export class IceQueue {
  private readonly queue: QueuedCandidate[] = [];
  private readonly maxSize: number;
  private remoteDescriptionSet = false;

  constructor(maxSize = 64) {
    if (maxSize < 1) throw new Error('maxSize must be >= 1');
    this.maxSize = maxSize;
  }

  /** Number of queued candidates. */
  get size(): number {
    return this.queue.length;
  }

  get isRemoteDescriptionSet(): boolean {
    return this.remoteDescriptionSet;
  }

  /**
   * Enqueue a candidate. Returns false if the queue is full (candidate dropped).
   * Returns true if queued or if remote description is already set (caller should add directly).
   */
  enqueue(candidate: RTCIceCandidateInit, generation: number): 'queued' | 'ready' | 'dropped' {
    if (this.remoteDescriptionSet) {
      return 'ready'; // Caller should add directly via addIceCandidate
    }
    if (this.queue.length >= this.maxSize) {
      return 'dropped';
    }
    this.queue.push({ candidate, generation });
    return 'queued';
  }

  /**
   * Mark remote description as set and return all queued candidates.
   * After this call, enqueue() returns 'ready' for all future candidates.
   */
  flush(): readonly QueuedCandidate[] {
    this.remoteDescriptionSet = true;
    const flushed = [...this.queue];
    this.queue.length = 0;
    return flushed;
  }

  /**
   * Discard all queued candidates for a specific generation or all generations.
   * Does NOT reset the remoteDescriptionSet flag.
   */
  discardForGeneration(generation?: number): number {
    if (generation === undefined) {
      const count = this.queue.length;
      this.queue.length = 0;
      return count;
    }
    const before = this.queue.length;
    const kept = this.queue.filter((q) => q.generation !== generation);
    this.queue.length = 0;
    this.queue.push(...kept);
    return before - this.queue.length;
  }

  /**
   * Full reset: clear queue and mark remote description as not set.
   * Used when creating a new connection (new generation).
   */
  reset(): void {
    this.queue.length = 0;
    this.remoteDescriptionSet = false;
  }
}

