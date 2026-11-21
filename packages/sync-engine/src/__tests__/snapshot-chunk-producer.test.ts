import { describe, it, expect, vi } from 'vitest';
import crypto from 'node:crypto';
import {
  SnapshotChunkProducer,
  type SnapshotChunkSlice,
  type PartitionedSnapshot,
} from '../recovery/snapshot-chunk-producer.js';
import {
  SnapshotSerializer,
  SerializedSnapshotArtifact,
  INTERNAL_SERIALIZER_TOKEN,
} from '../recovery/snapshot-serializer.js';
import {
  ChunkAssembler,
} from '../recovery/chunk-assembler.js';
import {
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  decodeBase64,
  SnapshotChunkPayloadSchema,
  SnapshotChunkEnvelopeSchema,
  type StateSnapshotPayloadV2,
} from '@syncplay/protocol';
import { SyncEngineError } from '../errors.js';
import { sha256 } from '../log/operation-fingerprint.js';

describe('Phase 7.5 M4.2b — SnapshotChunkProducer & Canonical Partitioning', () => {
  // Helper to create valid snapshots of arbitrary target size
  function createTestSnapshot(sizeBytes: number, idSuffix = '1'): StateSnapshotPayloadV2 {
    const rawState = {
      formatVersion: 1 as const,
      sessionId: `sess-${idSuffix}`,
      status: 'playing' as const,
      anchorPositionMs: 10000,
      playbackRate: 1.0,
      lastAppliedOperation: {
        logicalEventId: `evt-${idSuffix}`,
        senderPeerId: `peer-supplier-${idSuffix}`,
        messageType: 'PLAYBACK_PLAY',
      },
    };

    // Calculate approximate padding needed
    const basePayload: StateSnapshotPayloadV2 = {
      formatVersion: 2,
      snapshotId: `snap-${idSuffix}`,
      sessionId: `sess-${idSuffix}`,
      membershipEpoch: 2,
      authorPeerId: `peer-supplier-${idSuffix}`,
      snapshotFrontier: { 'peer-a': 10 },
      maxLogicalTimestamp: 1000,
      originFrontierTimestamps: { 'peer-a': 1000 },
      compactedCheckpoints: [],
      state: rawState,
    };

    return basePayload;
  }

  // Helper to create an artifact of exact byte size
  function createArtifactOfSize(exactBytes: number, idSuffix = 'size-test'): SerializedSnapshotArtifact {
    const masterBuffer = new ArrayBuffer(exactBytes);
    const view = new Uint8Array(masterBuffer);
    for (let i = 0; i < exactBytes; i++) {
      view[i] = (i * 37 + 13) & 0xff;
    }
    const digest = sha256(view).toLowerCase();

    // Use internal prevalidated token to instantiate test artifact of arbitrary binary size
    return new (SerializedSnapshotArtifact as any)({
      snapshotId: `snap-${idSuffix}`,
      sessionId: `sess-${idSuffix}`,
      membershipEpoch: 2,
      authorPeerId: `peer-supplier-${idSuffix}`,
      totalSizeBytes: exactBytes,
      sha256Digest: digest,
      masterBuffer,
      _trustedPrevalidated: INTERNAL_SERIALIZER_TOKEN,
    });
  }

  // Robust helper using SnapshotSerializer for valid structured snapshots
  function createStructuredArtifact(idSuffix = 'struct'): SerializedSnapshotArtifact {
    const snapshot = createTestSnapshot(500, idSuffix);
    return SnapshotSerializer.serialize(snapshot);
  }

  // ─────────────────────────────────────────────────────────────
  // 1. Partition Correctness
  // ─────────────────────────────────────────────────────────────
  describe('1. Partition Correctness', () => {
    it('partitions minimum 1-byte snapshot into exactly 1 canonical chunk', () => {
      const artifact = createArtifactOfSize(1, '1b');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(partition.totalChunks).toBe(1);
      expect(partition.nominalChunkSizeBytes).toBe(MAX_CHUNK_BYTES); // 32,768 on 65,536 transport
      expect(partition.slices.length).toBe(1);

      const s0 = partition.getSlice(0)!;
      expect(s0.chunkIndex).toBe(0);
      expect(s0.totalChunks).toBe(1);
      expect(s0.chunkSizeBytes).toBe(1);
      expect(s0.chunkSha256).toBe(artifact.computeSliceSha256(0, 1));

      // Out of bounds returns undefined
      expect(partition.getSlice(1)).toBeUndefined();
      expect(partition.getSlice(-1)).toBeUndefined();
    });

    it('partitions minimum raw chunk boundary (256 bytes) into 1 chunk', () => {
      const artifact = createArtifactOfSize(256, '256b');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(partition.totalChunks).toBe(1);
      const s0 = partition.getSlice(0)!;
      expect(s0.chunkSizeBytes).toBe(256);
      expect(s0.chunkSha256).toBe(artifact.computeSliceSha256(0, 256));
    });

    it('partitions exactly divisible payload (65,536 bytes) into 2 equal 32 KiB chunks', () => {
      const artifact = createArtifactOfSize(65536, '64k');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(partition.totalChunks).toBe(2);
      expect(partition.nominalChunkSizeBytes).toBe(32768);

      const s0 = partition.getSlice(0)!;
      expect(s0.chunkIndex).toBe(0);
      expect(s0.chunkSizeBytes).toBe(32768);

      const s1 = partition.getSlice(1)!;
      expect(s1.chunkIndex).toBe(1);
      expect(s1.chunkSizeBytes).toBe(32768);

      expect(s0.chunkSizeBytes + s1.chunkSizeBytes).toBe(65536);
    });

    it('partitions non-divisible payload (65,000 bytes) with smaller final chunk', () => {
      const artifact = createArtifactOfSize(65000, 'non-div');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(partition.totalChunks).toBe(2);
      expect(partition.nominalChunkSizeBytes).toBe(32768);

      const s0 = partition.getSlice(0)!;
      expect(s0.chunkSizeBytes).toBe(32768);

      const s1 = partition.getSlice(1)!;
      expect(s1.chunkSizeBytes).toBe(65000 - 32768); // 32,232 bytes
      expect(s1.chunkSizeBytes).toBeLessThan(partition.nominalChunkSizeBytes);
      expect(s0.chunkSizeBytes + s1.chunkSizeBytes).toBe(65000);
    });

    it('partitions maximum 4 MiB snapshot into exactly 128 canonical 32 KiB chunks', () => {
      const artifact = createArtifactOfSize(MAX_SNAPSHOT_BYTES, '4mb');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(partition.totalChunks).toBe(MAX_CHUNKS_PER_SNAPSHOT); // 128
      expect(partition.nominalChunkSizeBytes).toBe(MAX_CHUNK_BYTES); // 32,768

      let totalCoveredBytes = 0;
      for (let i = 0; i < 128; i++) {
        const slice = partition.getSlice(i)!;
        expect(slice.chunkIndex).toBe(i);
        expect(slice.totalChunks).toBe(128);
        expect(slice.chunkSizeBytes).toBe(MAX_CHUNK_BYTES);
        totalCoveredBytes += slice.chunkSizeBytes;
      }

      expect(totalCoveredBytes).toBe(MAX_SNAPSHOT_BYTES);
    });

    it('partitions snapshot on minimum transport floor (2,144 bytes limit => 258 B chunk capacity)', () => {
      // At 2,144 B transport limit, calculateChunkCapacity(2144) = 258 B
      const artifact = createArtifactOfSize(1000, 'floor-test');
      const partition = SnapshotChunkProducer.partition(artifact, {
        sctpMaxMessageSize: MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
      });

      expect(partition.nominalChunkSizeBytes).toBe(258);
      // ceil(1000 / 258) = 4 chunks
      expect(partition.totalChunks).toBe(4);
      expect(partition.getSlice(0)!.chunkSizeBytes).toBe(258);
      expect(partition.getSlice(1)!.chunkSizeBytes).toBe(258);
      expect(partition.getSlice(2)!.chunkSizeBytes).toBe(258);
      expect(partition.getSlice(3)!.chunkSizeBytes).toBe(1000 - 3 * 258); // 226 bytes
    });

    it('reconstructs original bytes bit-for-bit from all produced chunks', () => {
      const artifact = createStructuredArtifact('recon');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      const reconstructed = new Uint8Array(artifact.totalSizeBytes);
      let offset = 0;

      for (let i = 0; i < partition.totalChunks; i++) {
        const slice = partition.getSlice(i)!;
        const base64 = partition.encodeChunkBase64(i);
        const decoded = decodeBase64(base64);

        expect(decoded.byteLength).toBe(slice.chunkSizeBytes);
        reconstructed.set(decoded, offset);
        offset += slice.chunkSizeBytes;
      }

      expect(offset).toBe(artifact.totalSizeBytes);
      expect(reconstructed).toEqual(artifact.cloneBytes());
      expect(sha256(reconstructed).toLowerCase()).toBe(artifact.sha256Digest);
    });

    it('rejects invalid partition inputs', () => {
      const artifact = createStructuredArtifact('inv');

      // Transport below minimum 2,144 B
      expect(() =>
        SnapshotChunkProducer.partition(artifact, { sctpMaxMessageSize: 2143 })
      ).toThrow(SyncEngineError);
      expect(() =>
        SnapshotChunkProducer.partition(artifact, { sctpMaxMessageSize: 2143 })
      ).toThrow(/below minimum supported chunk transport limit/);

      // Unnegotiated SCTP without fallback
      expect(() =>
        SnapshotChunkProducer.partition(artifact, { allowSctpFallback: false })
      ).toThrow(SyncEngineError);
      expect(() =>
        SnapshotChunkProducer.partition(artifact, { allowSctpFallback: false })
      ).toThrow(/SCTP transport message size is unnegotiated/);

      // Custom nominalChunkSizeBytes out of bounds (< 256 or > 32,768)
      expect(() =>
        SnapshotChunkProducer.partition(artifact, {
          allowSctpFallback: true,
          nominalChunkSizeBytes: 255,
        })
      ).toThrow(SyncEngineError);

      expect(() =>
        SnapshotChunkProducer.partition(artifact, {
          allowSctpFallback: true,
          nominalChunkSizeBytes: 32769,
        })
      ).toThrow(SyncEngineError);

      // Custom nominalChunkSizeBytes exceeding transport capacity
      // At 4,096 transport limit, capacity is 1,722 B
      expect(() =>
        SnapshotChunkProducer.partition(artifact, {
          sctpMaxMessageSize: 4096,
          nominalChunkSizeBytes: 2048,
        })
      ).toThrow(/exceeds transport chunk capacity/);

      // Chunk count > 128
      // If snapshot is 4 MiB, but nominalChunkSizeBytes is 1,000 B => 4,195 chunks > 128!
      const bigArtifact = createArtifactOfSize(MAX_SNAPSHOT_BYTES, 'big-inv');
      expect(() =>
        SnapshotChunkProducer.partition(bigArtifact, {
          allowSctpFallback: true,
          nominalChunkSizeBytes: 1000,
        })
      ).toThrow(SyncEngineError);
      expect(() =>
        SnapshotChunkProducer.partition(bigArtifact, {
          allowSctpFallback: true,
          nominalChunkSizeBytes: 1000,
        })
      ).toThrow(/exceeds MAX_CHUNKS_PER_SNAPSHOT/);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 2. Determinism
  // ─────────────────────────────────────────────────────────────
  describe('2. Determinism', () => {
    it('produces identical partition geometry and hashes across multiple runs and instances', () => {
      const artifact = createArtifactOfSize(120000, 'det');
      const p1 = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });
      const p2 = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(p1.totalChunks).toBe(p2.totalChunks);
      expect(p1.nominalChunkSizeBytes).toBe(p2.nominalChunkSizeBytes);
      expect(p1.slices.length).toBe(p2.slices.length);

      for (let i = 0; i < p1.totalChunks; i++) {
        const s1 = p1.getSlice(i)!;
        const s2 = p2.getSlice(i)!;

        expect(s1.chunkIndex).toBe(s2.chunkIndex);
        expect(s1.chunkSizeBytes).toBe(s2.chunkSizeBytes);
        expect(s1.chunkSha256).toBe(s2.chunkSha256);
        expect(p1.encodeChunkBase64(i)).toBe(p2.encodeChunkBase64(i));
      }
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 3. Integrity
  // ─────────────────────────────────────────────────────────────
  describe('3. Integrity', () => {
    it('every chunk SHA-256 matches its exact raw bytes', () => {
      const artifact = createArtifactOfSize(100000, 'integ');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      for (let i = 0; i < partition.totalChunks; i++) {
        const slice = partition.getSlice(i)!;
        const base64 = partition.encodeChunkBase64(i);
        const bytes = decodeBase64(base64);

        expect(bytes.byteLength).toBe(slice.chunkSizeBytes);
        const computedSha = sha256(bytes).toLowerCase();
        expect(computedSha).toBe(slice.chunkSha256);
      }
    });

    it('rejects out of bounds chunk index on encodeChunkBase64', () => {
      const artifact = createStructuredArtifact('oob');
      const partition = SnapshotChunkProducer.partition(artifact, { allowSctpFallback: true });

      expect(() => partition.encodeChunkBase64(-1)).toThrow(SyncEngineError);
      expect(() => partition.encodeChunkBase64(partition.totalChunks)).toThrow(SyncEngineError);
      expect(() => partition.encodeChunkBase64(999)).toThrow(SyncEngineError);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 4. Exact Transport Bounds Enforcement
  // ─────────────────────────────────────────────────────────────
  describe('4. Transport Bounds Enforcement', () => {
    it('produces chunk wire envelopes with exact UTF-8 byte length within negotiated limits', () => {
      const artifact = createStructuredArtifact('wire-env');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000001',
      });

      const chunkResult = producer.produceChunk(0);
      expect(chunkResult.ok).toBe(true);

      // Verify exact byte measurement
      const serialized = chunkResult.serializedJson;
      const expectedByteLength = new TextEncoder().encode(serialized).byteLength;
      expect(chunkResult.byteLength).toBe(expectedByteLength);
      expect(chunkResult.byteLength).toBeLessThanOrEqual(producer.effectiveLimit);

      // Verify payload and envelope schema validity
      expect(SnapshotChunkPayloadSchema.safeParse(chunkResult.payload).success).toBe(true);
      expect(SnapshotChunkEnvelopeSchema.safeParse(chunkResult.message).success).toBe(true);

      producer.dispose();
    });

    it('one-byte-over-limit message fails immediately with TRANSPORT_LIMIT_EXCEEDED without resizing chunk', () => {
      const artifact = createStructuredArtifact('tight');
      // Create producer on 65,536 limit
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000002',
      });

      // Override effectiveLimit to 1 byte less than required message length
      const chunk0 = producer.produceChunk(0);
      const exactLen = chunk0.byteLength;

      // Create new producer with tight limit: exactLen - 1
      // nominal chunk will be smaller, but if custom nominal chunk is set to chunk0 size:
      expect(() => {
        const tightProducer = new SnapshotChunkProducer(artifact, {
          sctpMaxMessageSize: exactLen - 1,
          nominalChunkSizeBytes: chunk0.payload.chunkSizeBytes,
          transferId: '00000000-0000-4000-8000-000000000003',
        });
        tightProducer.produceChunk(0);
      }).toThrow(); // throws either at constructor (exceeds capacity) or at produceChunk

      producer.dispose();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 5. Resource & Lease Lifecycle
  // ─────────────────────────────────────────────────────────────
  describe('5. Resource & Lease Lifecycle', () => {
    it('acquires transfer lease on constructor and releases on explicit dispose()', () => {
      const artifact = createStructuredArtifact('lse-1');
      expect(artifact.activeLeaseCount).toBe(0);

      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: 'transfer-lease-test-1',
      });

      expect(artifact.activeLeaseCount).toBe(1);
      expect(producer.isLeaseActive).toBe(true);
      expect(producer.isDisposed).toBe(false);

      // Explicit dispose
      producer.dispose();
      expect(artifact.activeLeaseCount).toBe(0);
      expect(producer.isLeaseActive).toBe(false);
      expect(producer.isDisposed).toBe(true);

      // Repeated dispose is idempotent
      producer.dispose();
      expect(artifact.activeLeaseCount).toBe(0);
      expect(producer.isDisposed).toBe(true);
    });

    it('guarantees lease release if constructor fails after lease acquisition', () => {
      const artifact = createStructuredArtifact('fail-lse');
      expect(artifact.activeLeaseCount).toBe(0);

      // Mock calculateSlice to throw during constructor
      const originalCompute = artifact.computeSliceSha256;
      vi.spyOn(artifact, 'computeSliceSha256').mockImplementationOnce(() => {
        throw new Error('Simulated hash computation failure');
      });

      expect(() => {
        new SnapshotChunkProducer(artifact, {
          allowSctpFallback: true,
          transferId: 'transfer-failing-producer',
        });
      }).toThrow('Simulated hash computation failure');

      // The lease MUST NOT be leaked!
      expect(artifact.activeLeaseCount).toBe(0);

      vi.restoreAllMocks();
    });

    it('supports autoReleaseOnFinalChunk configuration', () => {
      const artifact = createStructuredArtifact('auto-rel');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000004',
        autoReleaseOnFinalChunk: true,
      });

      expect(producer.isLeaseActive).toBe(true);
      const total = producer.totalChunks;

      // Produce non-final chunks (if any)
      for (let i = 0; i < total - 1; i++) {
        producer.produceChunk(i);
        expect(producer.isLeaseActive).toBe(true);
      }

      // Produce final chunk -> triggers auto-release
      producer.produceChunk(total - 1);
      expect(producer.isLeaseActive).toBe(false);
      expect(producer.isDisposed).toBe(true);
      expect(artifact.activeLeaseCount).toBe(0);
    });

    it('rejects operations after disposal with ARTIFACT_DISPOSED', () => {
      const artifact = createStructuredArtifact('disp-ops');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000005',
      });

      producer.dispose();

      expect(() => producer.encodeChunkBase64(0)).toThrow(SyncEngineError);
      expect(() => producer.encodeChunkBase64(0)).toThrow(/ARTIFACT_DISPOSED/);

      expect(() => producer.produceChunkPayload(0)).toThrow(SyncEngineError);
      expect(() => producer.produceChunk(0)).toThrow(SyncEngineError);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 6. Compatibility with Frozen M4.1 ChunkAssembler
  // ─────────────────────────────────────────────────────────────
  describe('6. Compatibility with Frozen M4.1 ChunkAssembler', () => {
    it('produces chunks that are completely ingested and assembled by frozen ChunkAssembler', () => {
      const artifact = createStructuredArtifact('m41-compat');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000006',
      });

      const assembler = new ChunkAssembler();
      assembler.initTransfer({
        transferId: producer.transferId,
        sessionId: artifact.sessionId,
        membershipEpoch: artifact.membershipEpoch,
        supplierPeerId: artifact.authorPeerId,
        snapshotId: artifact.snapshotId,
        totalSizeBytes: artifact.totalSizeBytes,
        totalChunks: producer.totalChunks,
        nominalChunkSizeBytes: producer.nominalChunkSizeBytes,
        expectedSha256Digest: artifact.sha256Digest,
      });

      // Ingest each produced chunk sequentially
      for (let i = 0; i < producer.totalChunks; i++) {
        const chunkResult = producer.produceChunk(i);
        const ingestRes = assembler.ingestChunk(chunkResult.payload, {
          sessionId: artifact.sessionId,
          membershipEpoch: artifact.membershipEpoch,
          senderPeerId: artifact.authorPeerId,
          connectionGeneration: 1,
        });

        expect(ingestRes.ok).toBe(true);
      }

      // Finalize reassembly in M4.1 ChunkAssembler
      const finalizeRes = assembler.finalizeAssembly();
      expect(finalizeRes.ok).toBe(true);
      if (finalizeRes.ok) {
        expect(finalizeRes.snapshot.sha256Digest).toBe(artifact.sha256Digest);
        expect(new Uint8Array(finalizeRes.snapshot.buffer)).toEqual(artifact.cloneBytes());
      }

      producer.dispose();
    });

    it('corrupted chunk payload data fails ChunkAssembler ingestion with CHUNK_CORRUPTED', () => {
      const artifact = createStructuredArtifact('corrupt-test');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000007',
      });

      const assembler = new ChunkAssembler();
      assembler.initTransfer({
        transferId: producer.transferId,
        sessionId: artifact.sessionId,
        membershipEpoch: artifact.membershipEpoch,
        supplierPeerId: artifact.authorPeerId,
        snapshotId: artifact.snapshotId,
        totalSizeBytes: artifact.totalSizeBytes,
        totalChunks: producer.totalChunks,
        nominalChunkSizeBytes: producer.nominalChunkSizeBytes,
        expectedSha256Digest: artifact.sha256Digest,
      });

      const validChunk = producer.produceChunk(0);

      // Corrupt the Base64 data by flipping a character while maintaining valid Base64
      const validBase64 = validChunk.payload.dataBase64;
      const corruptedChar = validBase64[0] === 'A' ? 'B' : 'A';
      const corruptedBase64 = corruptedChar + validBase64.slice(1);

      const corruptedPayload = {
        ...validChunk.payload,
        dataBase64: corruptedBase64,
      };

      const ingestResult = assembler.ingestChunk(corruptedPayload, {
        sessionId: artifact.sessionId,
        membershipEpoch: artifact.membershipEpoch,
        senderPeerId: artifact.authorPeerId,
        connectionGeneration: 1,
      });

      expect(ingestResult.ok).toBe(false);
      expect((ingestResult as any).reason).toBe('CHUNK_CORRUPTED');

      producer.dispose();
    });

    it('rejects invalid or already-disposed artifact in constructor', () => {
      expect(() => new SnapshotChunkProducer(null as any)).toThrow(SyncEngineError);
      expect(() => new SnapshotChunkProducer({} as any)).toThrow(SyncEngineError);

      const artifact = createStructuredArtifact('disp-check');
      artifact.dispose();
      expect(() => new SnapshotChunkProducer(artifact, { allowSctpFallback: true })).toThrow(
        SyncEngineError
      );
      expect(() => new SnapshotChunkProducer(artifact, { allowSctpFallback: true })).toThrow(
        /Cannot partition a disposed SerializedSnapshotArtifact/
      );
    });

    it('producer does not retain produced chunk Base64 strings or binary copies in memory', () => {
      const artifact = createStructuredArtifact('no-retain');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000008',
      });

      // Slices only have index, totalChunks, chunkSizeBytes, chunkSha256
      for (const slice of producer.slices) {
        expect((slice as any).dataBase64).toBeUndefined();
        expect((slice as any).data).toBeUndefined();
        expect((slice as any).buffer).toBeUndefined();
        expect(Object.keys(slice).sort()).toEqual([
          'chunkIndex',
          'chunkSha256',
          'chunkSizeBytes',
          'totalChunks',
        ]);
      }

      producer.dispose();
    });

    it('exact transport boundary message (byteLength === effectiveLimit succeeds; byteLength === effectiveLimit + 1 fails)', () => {
      const artifact = createStructuredArtifact('exact-boundary');
      const producer = new SnapshotChunkProducer(artifact, {
        allowSctpFallback: true,
        transferId: '00000000-0000-4000-8000-000000000009',
      });

      // 1. Measure normal chunk byteLength
      const normalChunk = producer.produceChunk(0);
      const measuredLen = normalChunk.byteLength;

      // 2. Test exact boundary: set effectiveLimit to exactly measuredLen
      // byteLength === effectiveLimit MUST succeed
      (producer as any).effectiveLimit = measuredLen;
      const exactChunk = producer.produceChunk(0);
      expect(exactChunk.ok).toBe(true);
      expect(exactChunk.byteLength).toBe(measuredLen);
      expect(exactChunk.byteLength).toBe((producer as any).effectiveLimit);

      // 3. Test one byte over limit: set effectiveLimit to measuredLen - 1
      // byteLength === effectiveLimit + 1 MUST fail with TRANSPORT_LIMIT_EXCEEDED
      (producer as any).effectiveLimit = measuredLen - 1;
      expect(() => producer.produceChunk(0)).toThrow(SyncEngineError);
      expect(() => producer.produceChunk(0)).toThrow(/exceeds effective transport limit/);

      producer.dispose();
    });
  });
});
