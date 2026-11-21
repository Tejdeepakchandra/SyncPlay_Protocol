import { describe, it, expect, vi } from 'vitest';
import {
  SnapshotPreparationQueue,
  type QueuedPreparationRequest,
} from '../recovery/snapshot-preparation-queue.js';
import {
  SnapshotSerializer,
  SerializedSnapshotArtifact,
} from '../recovery/snapshot-serializer.js';
import { SyncEngineError } from '../errors.js';

describe('Phase 7.5 M4.2a — SnapshotPreparationQueue', () => {
  const sampleState = {
    status: 'paused' as const,
    anchorPositionMs: 5000,
    playbackRate: 1.0,
  };

  const sampleContext = {
    sessionId: 'session-queue-test',
    membershipEpoch: 1,
    authorPeerId: 'peer-supplier-1',
    snapshotFrontier: { 'peer-supplier-1': 1 },
    originFrontierTimestamps: { 'peer-supplier-1': 100 },
    compactedCheckpoints: [],
  };

  function createMockRequest(id: string): QueuedPreparationRequest {
    return {
      transferId: `transfer-${id}`,
      recipientPeerId: `peer-recipient-${id}`,
      sctpMaxMessageSize: 65536,
      timestampMs: Date.now(),
    };
  }

  function createArtifact(): SerializedSnapshotArtifact {
    return SnapshotSerializer.serializeFromEngine(sampleState, sampleContext);
  }

  // ─────────────────────────────────────────────────────────────
  // Finding F2 / AC-M4-CONC-1: Automatic Lifecycle Resumption on Disposal
  // ─────────────────────────────────────────────────────────────
  it('Finding F2 & AC-M4-CONC-1: automatically resumes waiting queue when artifact is disposed without manual notifyArtifactDisposed', async () => {
    const queue = new SnapshotPreparationQueue();

    // Prepare 1st artifact: starts immediately
    const req1 = createMockRequest('1');
    const artifact1 = await queue.enqueue(req1, () => createArtifact());
    expect(queue.retainedArtifactCount).toBe(1);
    expect(queue.isBusy).toBe(false);
    expect(queue.depth).toBe(0);

    // Prepare 2nd artifact: starts immediately because A=1, P=1 => A+P=2 <= 2
    const req2 = createMockRequest('2');
    const artifact2 = await queue.enqueue(req2, () => createArtifact());
    expect(queue.retainedArtifactCount).toBe(2);
    expect(queue.isBusy).toBe(false);
    expect(queue.depth).toBe(0);

    // 3rd preparation request: A=2, so A + P = 2 + 1 = 3 > 2. It MUST be held in queue!
    const req3 = createMockRequest('3');
    const captureFn3 = vi.fn(async () => createArtifact());

    const promise3 = queue.enqueue(req3, captureFn3);

    // Give event loop ticks to ensure no premature execution
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(queue.depth).toBe(1);
    expect(captureFn3).not.toHaveBeenCalled();
    expect(queue.isBusy).toBe(false);
    expect(queue.retainedArtifactCount).toBe(2);

    // DISPOSE ARTIFACT 1: DO NOT call queue.notifyArtifactDisposed() manually!
    // The artifact's internal disposal listener MUST automatically notify the queue.
    artifact1.dispose();

    // The queue should immediately resume and execute captureFn3
    const artifact3 = await promise3;
    expect(captureFn3).toHaveBeenCalledTimes(1);
    expect(artifact3).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(queue.depth).toBe(0);
    expect(queue.retainedArtifactCount).toBe(2); // 1 (artifact2) + 1 (artifact3) = 2
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F2: Automatic Resumption on Deferred Disposal (Last Lease Release)
  // ─────────────────────────────────────────────────────────────
  it('Finding F2: automatically resumes waiting queue when deferred disposal finalizes on last lease release', async () => {
    const queue = new SnapshotPreparationQueue();

    const artifact1 = await queue.enqueue(createMockRequest('l-1'), () => createArtifact());
    const artifact2 = await queue.enqueue(createMockRequest('l-2'), () => createArtifact());
    expect(queue.retainedArtifactCount).toBe(2);

    // Artifact 1 has an active transfer lease
    artifact1.acquireLease('transfer-active-1');
    expect(artifact1.activeLeaseCount).toBe(1);

    // Enqueue 3rd preparation request -> held in queue
    const captureFn3 = vi.fn(async () => createArtifact());
    const promise3 = queue.enqueue(createMockRequest('l-3'), captureFn3);

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(queue.depth).toBe(1);
    expect(captureFn3).not.toHaveBeenCalled();

    // Request disposal of artifact 1 while lease is still active
    artifact1.dispose();
    expect(artifact1.isDisposalRequested).toBe(true);
    expect(artifact1.isDisposed).toBe(false);

    // Queue must STILL be held because artifact 1 is not yet detached/disposed
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(captureFn3).not.toHaveBeenCalled();
    expect(queue.depth).toBe(1);

    // Active transfer completes and releases its lease
    artifact1.releaseLease('transfer-active-1');
    expect(artifact1.isDisposed).toBe(true);

    // Now queue should have resumed automatically!
    const artifact3 = await promise3;
    expect(captureFn3).toHaveBeenCalledTimes(1);
    expect(artifact3).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(queue.depth).toBe(0);
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F2: Per-Request Cancellation via queue.cancel(transferId)
  // ─────────────────────────────────────────────────────────────
  it('Finding F2: cancels an individual waiting request via cancel(transferId) without clearing entire queue', async () => {
    const queue = new SnapshotPreparationQueue();

    // Saturate retention
    queue.trackArtifactRetained();
    queue.trackArtifactRetained();
    expect(queue.retainedArtifactCount).toBe(2);

    const p1 = queue.enqueue(createMockRequest('can-1'), () => createArtifact());
    const p2 = queue.enqueue(createMockRequest('can-2'), () => createArtifact());
    const p3 = queue.enqueue(createMockRequest('can-3'), () => createArtifact());

    expect(queue.depth).toBe(3);

    // Cancel transfer can-2 specifically
    const cancelled = queue.cancel('transfer-can-2');
    expect(cancelled).toBe(true);
    expect(queue.depth).toBe(2);

    // p2 should be rejected with LOCAL_ABORT
    await expect(p2).rejects.toThrow(SyncEngineError);
    await expect(p2).rejects.toThrow(/Preparation request cancelled for transferId: transfer-can-2/);

    // Cancelling unknown transfer returns false
    expect(queue.cancel('unknown-transfer')).toBe(false);
    expect(queue.depth).toBe(2);

    // Remaining items p1 and p3 should still be in queue
    queue.clear();
    await Promise.allSettled([p1, p3]);
  });

  // ─────────────────────────────────────────────────────────────
  // Finding F2: Idempotent Disposal Notification
  // ─────────────────────────────────────────────────────────────
  it('Finding F2: repeated artifact.dispose() does not decrement retained count more than once', async () => {
    const queue = new SnapshotPreparationQueue();

    const artifact1 = await queue.enqueue(createMockRequest('idem-1'), () => createArtifact());
    expect(queue.retainedArtifactCount).toBe(1);

    artifact1.dispose();
    expect(queue.retainedArtifactCount).toBe(0);

    // Call dispose again
    artifact1.dispose();
    expect(queue.retainedArtifactCount).toBe(0); // Remains 0, does not go negative
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-CONC-2: Queue Depth Bounding & Saturation Rejection
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-CONC-2: bounded preparation queue rejects immediately with LOCAL_ABORT at depth 4', async () => {
    const queue = new SnapshotPreparationQueue();

    queue.trackArtifactRetained();
    queue.trackArtifactRetained();
    expect(queue.retainedArtifactCount).toBe(2);

    const pendingPromises: Promise<any>[] = [];
    const dummyFn = vi.fn(async () => createArtifact());

    // Enqueue 4 requests (saturates queue)
    for (let i = 1; i <= 4; i++) {
      const p = queue.enqueue(createMockRequest(`sat-${i}`), dummyFn);
      pendingPromises.push(p);
    }

    expect(queue.depth).toBe(4);

    // 5th request MUST reject immediately with LOCAL_ABORT
    await expect(
      queue.enqueue(createMockRequest('sat-5'), dummyFn)
    ).rejects.toThrow(SyncEngineError);

    await expect(
      queue.enqueue(createMockRequest('sat-5'), dummyFn)
    ).rejects.toThrow(/Preparation queue saturated/);

    expect(dummyFn).not.toHaveBeenCalled();

    queue.clear();
    await Promise.allSettled(pendingPromises);
  });

  // ─────────────────────────────────────────────────────────────
  // AC-M4-CONC-3: Sequential Execution (MAX_CONCURRENT_PREPARATIONS = 1)
  // ─────────────────────────────────────────────────────────────
  it('AC-M4-CONC-3: enforces sequential execution of preparations with MAX_CONCURRENT_PREPARATIONS = 1', async () => {
    const queue = new SnapshotPreparationQueue();

    let concurrentExecutions = 0;
    let maxConcurrent = 0;

    const runWithDelay = async () => {
      concurrentExecutions++;
      maxConcurrent = Math.max(maxConcurrent, concurrentExecutions);
      await new Promise((resolve) => setTimeout(resolve, 25));
      concurrentExecutions--;
      return createArtifact();
    };

    const p1 = queue.enqueue(createMockRequest('seq-1'), runWithDelay);
    const p2 = queue.enqueue(createMockRequest('seq-2'), runWithDelay);

    expect(queue.isBusy).toBe(true);

    const [a1, a2] = await Promise.all([p1, p2]);
    expect(a1).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(a2).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(maxConcurrent).toBe(1);
    expect(queue.isBusy).toBe(false);
  });

  // ─────────────────────────────────────────────────────────────
  // Error Handling & State Recovery
  // ─────────────────────────────────────────────────────────────
  it('recovers cleanly when captureAndSerializeFn throws an error', async () => {
    const queue = new SnapshotPreparationQueue();

    const failingFn = vi.fn(async () => {
      throw new Error('Capture failed');
    });

    await expect(queue.enqueue(createMockRequest('fail-1'), failingFn)).rejects.toThrow('Capture failed');

    expect(queue.isBusy).toBe(false);
    expect(queue.retainedArtifactCount).toBe(0);
    expect(queue.depth).toBe(0);

    const successArtifact = await queue.enqueue(createMockRequest('succ-1'), () => createArtifact());
    expect(successArtifact).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(queue.retainedArtifactCount).toBe(1);
  });

  it('rejects when captureAndSerializeFn returns a non-artifact object without leaking retainedArtifactCount', async () => {
    const queue = new SnapshotPreparationQueue();

    const badFn = vi.fn(async () => {
      return { not: 'an artifact' } as any;
    });

    await expect(queue.enqueue(createMockRequest('bad-1'), badFn)).rejects.toThrow(SyncEngineError);
    await expect(queue.enqueue(createMockRequest('bad-1'), badFn)).rejects.toThrow(
      /captureAndSerializeFn must return an instance of SerializedSnapshotArtifact/
    );

    expect(queue.isBusy).toBe(false);
    expect(queue.retainedArtifactCount).toBe(0); // MUST NOT LEAK!
    expect(queue.depth).toBe(0);

    // Queue remains completely functional
    const successArtifact = await queue.enqueue(createMockRequest('succ-2'), () => createArtifact());
    expect(successArtifact).toBeInstanceOf(SerializedSnapshotArtifact);
    expect(queue.retainedArtifactCount).toBe(1);
  });

  // ─────────────────────────────────────────────────────────────
  // Clear Method
  // ─────────────────────────────────────────────────────────────
  it('clear() rejects all pending requests with LOCAL_ABORT', async () => {
    const queue = new SnapshotPreparationQueue();

    queue.trackArtifactRetained();
    queue.trackArtifactRetained();

    const p1 = queue.enqueue(createMockRequest('c-1'), () => createArtifact());
    const p2 = queue.enqueue(createMockRequest('c-2'), () => createArtifact());

    expect(queue.depth).toBe(2);

    queue.clear();
    expect(queue.depth).toBe(0);

    await expect(p1).rejects.toThrow(SyncEngineError);
    await expect(p1).rejects.toThrow(/Preparation queue cleared/);
    await expect(p2).rejects.toThrow(SyncEngineError);
  });
});
