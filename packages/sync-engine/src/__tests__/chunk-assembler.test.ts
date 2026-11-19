/**
 * Comprehensive Unit Tests for ChunkAssembler.
 *
 * Validates Phase 7.5 M4.1 isolated chunk assembly:
 * - Single-chunk and multi-chunk assembly
 * - Out-of-order chunk ingestion
 * - Missing chunk index tracking
 * - Idempotent identical duplicate ingestion
 * - Conflicting duplicate rejection (CHUNK_CORRUPTED)
 * - Bit-flip / corrupt chunk hash rejection (CHUNK_CORRUPTED)
 * - Session, epoch, transfer, supplier, and metadata validation
 * - Failover chunk retention on bit-exact snapshot match
 * - Failover chunk purge on snapshot ID or digest mismatch
 * - Overall SHA-256 mismatch on finalization (HASH_MISMATCH)
 * - Memory release on finalization (AC-M4-MEM-3) and disposal
 * - Incomplete assembly rejection (never exposes partial buffer)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ChunkAssembler, type InitTransferParams } from '../recovery/chunk-assembler.js';
import { sha256 } from '../log/operation-fingerprint.js';
import type { SnapshotChunkPayload } from '@syncplay/protocol';

describe('ChunkAssembler (Phase 7.5 M4.1)', () => {
  let assembler: ChunkAssembler;

  const sampleTransferId = '12345678-1234-4234-8234-123456789abc';
  const sampleSessionId = 'session-alpha';
  const sampleSnapshotId = 'snap-001';
  const sampleSupplier = 'peer-supplier';
  const sampleEpoch = 2;
  const sampleGeneration = 1;

  // Helper to create test chunks from a raw buffer
  function createChunksFromBuffer(
    buffer: Uint8Array,
    nominalChunkSize: number,
    transferId: string = sampleTransferId,
    sessionId: string = sampleSessionId,
    snapshotId: string = sampleSnapshotId,
    supplierPeerId: string = sampleSupplier,
    membershipEpoch: number = sampleEpoch
  ): {
    totalChunks: number;
    totalSizeBytes: number;
    overallSha256: string;
    chunks: SnapshotChunkPayload[];
  } {
    const totalSizeBytes = buffer.byteLength;
    const totalChunks = Math.ceil(totalSizeBytes / nominalChunkSize);
    const overallSha256 = sha256(buffer);
    const chunks: SnapshotChunkPayload[] = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * nominalChunkSize;
      const end = Math.min(start + nominalChunkSize, totalSizeBytes);
      const slice = buffer.slice(start, end);
      const chunkSizeBytes = slice.byteLength;
      const chunkSha256 = sha256(slice);
      const dataBase64 = Buffer.from(slice).toString('base64');

      chunks.push({
        transferId,
        sessionId,
        membershipEpoch,
        supplierPeerId,
        snapshotId,
        chunkIndex: i,
        totalChunks,
        chunkSizeBytes,
        totalSizeBytes,
        chunkSha256,
        dataBase64,
      });
    }

    return { totalChunks, totalSizeBytes, overallSha256, chunks };
  }

  beforeEach(() => {
    assembler = new ChunkAssembler();
  });

  describe('Single-chunk assembly', () => {
    it('should assemble a valid single-chunk snapshot', () => {
      const rawData = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        rawData.byteLength
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        connectionGeneration: sampleGeneration,
      });

      expect(assembler.isComplete()).toBe(false);
      expect(assembler.getMissingChunkIndices()).toEqual([0]);

      const ingestResult = assembler.ingestChunk(chunks[0]!);
      expect(ingestResult.ok).toBe(true);
      if (ingestResult.ok) {
        expect(ingestResult.duplicate).toBe(false);
        expect(ingestResult.completed).toBe(true);
      }

      expect(assembler.isComplete()).toBe(true);
      expect(assembler.getMissingChunkIndices()).toEqual([]);

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(true);
      if (finalizeResult.ok) {
        expect(finalizeResult.snapshot.snapshotId).toBe(sampleSnapshotId);
        expect(finalizeResult.snapshot.totalSizeBytes).toBe(rawData.byteLength);
        expect(finalizeResult.snapshot.sha256Digest).toBe(overallSha256);
        expect(Array.from(finalizeResult.snapshot.buffer)).toEqual(Array.from(rawData));
      }
    });
  });

  describe('Multi-chunk and Out-of-Order Ingestion', () => {
    it('should assemble a 4-chunk snapshot ingested in reverse order', () => {
      // 1000 bytes across 4 chunks (256, 256, 256, 232)
      const rawData = new Uint8Array(1000);
      for (let i = 0; i < rawData.byteLength; i++) {
        rawData[i] = i % 251; // pseudo-arbitrary pattern
      }

      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );
      expect(totalChunks).toBe(4);

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      expect(assembler.getMissingChunkIndices()).toEqual([0, 1, 2, 3]);

      // Ingest chunks in reverse: 3, 2, 1, 0
      const order = [3, 2, 1, 0];
      for (const idx of order) {
        const res = assembler.ingestChunk(chunks[idx]!);
        expect(res.ok).toBe(true);
        if (res.ok) {
          expect(res.duplicate).toBe(false);
          expect(res.completed).toBe(idx === 0);
        }
      }

      expect(assembler.isComplete()).toBe(true);
      expect(assembler.getMissingChunkIndices()).toEqual([]);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1, 2, 3]);

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(true);
      if (finalizeResult.ok) {
        expect(finalizeResult.snapshot.buffer.byteLength).toBe(1000);
        expect(Array.from(finalizeResult.snapshot.buffer)).toEqual(Array.from(rawData));
      }
    });

    it('should accurately report progress and missing indices during partial assembly', () => {
      const rawData = new Uint8Array(1024);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest chunk 1 first
      assembler.ingestChunk(chunks[1]!);
      expect(assembler.getMissingChunkIndices()).toEqual([0, 2, 3]);
      expect(assembler.getReceivedChunkIndices()).toEqual([1]);

      let progress = assembler.getProgress();
      expect(progress).toEqual({
        receivedChunks: 1,
        totalChunks: 4,
        receivedBytes: 256,
        totalSizeBytes: 1024,
      });

      // Ingest chunk 3 next
      assembler.ingestChunk(chunks[3]!);
      expect(assembler.getMissingChunkIndices()).toEqual([0, 2]);
      expect(assembler.getReceivedChunkIndices()).toEqual([1, 3]);

      progress = assembler.getProgress();
      expect(progress?.receivedChunks).toBe(2);
      expect(progress?.receivedBytes).toBe(512);
    });
  });

  describe('Duplicate Chunk Handling', () => {
    it('should idempotently accept identical duplicate chunk', () => {
      const rawData = new Uint8Array(512);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // First ingestion
      const res1 = assembler.ingestChunk(chunks[0]!);
      expect(res1.ok).toBe(true);
      if (res1.ok) {
        expect(res1.duplicate).toBe(false);
      }

      // Duplicate ingestion with exact same chunk
      const res2 = assembler.ingestChunk(chunks[0]!);
      expect(res2.ok).toBe(true);
      if (res2.ok) {
        expect(res2.duplicate).toBe(true);
        expect(res2.completed).toBe(false);
      }

      // Progress should not double count
      const progress = assembler.getProgress();
      expect(progress?.receivedChunks).toBe(1);
      expect(progress?.receivedBytes).toBe(256);
    });

    it('should reject conflicting duplicate chunk with CHUNK_CORRUPTED', () => {
      const rawData = new Uint8Array(512);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest chunk 0
      assembler.ingestChunk(chunks[0]!);

      // Construct a conflicting chunk at index 0 with different data and its own valid sha
      const conflictingBytes = new Uint8Array(256).fill(0xff);
      const conflictingChunk: SnapshotChunkPayload = {
        ...chunks[0]!,
        chunkSha256: sha256(conflictingBytes),
        dataBase64: Buffer.from(conflictingBytes).toString('base64'),
      };

      const resConflict = assembler.ingestChunk(conflictingChunk);
      expect(resConflict.ok).toBe(false);
      if (!resConflict.ok) {
        expect(resConflict.reason).toBe('CHUNK_CORRUPTED');
        expect(resConflict.detail).toContain('Conflicting chunk data');
      }
    });
  });

  describe('Integrity Verification & Corruption Rejection', () => {
    it('should reject bit-flipped or corrupted chunk with CHUNK_CORRUPTED', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Tamper with dataBase64 while keeping original chunkSha256
      const tamperedBytes = new Uint8Array(256);
      tamperedBytes[10] = 0x99;
      const tamperedChunk: SnapshotChunkPayload = {
        ...chunks[0]!,
        dataBase64: Buffer.from(tamperedBytes).toString('base64'),
      };

      const res = assembler.ingestChunk(tamperedChunk);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('CHUNK_CORRUPTED');
        expect(res.detail).toContain('Chunk SHA-256 digest mismatch');
      }
    });

    it('should reject overall SHA-256 digest mismatch on finalization', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, chunks } = createChunksFromBuffer(rawData, 256);

      // Initialize with wrong expected overall hash
      const fakeOverallHash = '0'.repeat(64);
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: fakeOverallHash,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      expect(assembler.isComplete()).toBe(true);

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(false);
      if (!finalizeResult.ok) {
        expect(finalizeResult.reason).toBe('HASH_MISMATCH');
        expect(finalizeResult.detail).toContain('Overall snapshot SHA-256 digest mismatch');
      }
    });

    it('should refuse to finalize an incomplete snapshot and never expose partial buffer', () => {
      const rawData = new Uint8Array(512);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest only chunk 0 (chunk 1 missing)
      assembler.ingestChunk(chunks[0]!);
      expect(assembler.isComplete()).toBe(false);

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(false);
      if (!finalizeResult.ok) {
        expect(finalizeResult.reason).toBe('CHUNK_CORRUPTED');
        expect(finalizeResult.detail).toContain('Cannot finalize incomplete');
      }
    });
  });

  describe('Identity, Epoch, and Boundary Enforcement', () => {
    it('should reject chunk with mismatched transferId', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      const badChunk: SnapshotChunkPayload = {
        ...chunks[0]!,
        transferId: '99999999-9999-4999-8999-999999999999',
      };

      const res = assembler.ingestChunk(badChunk);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('IDENTITY_MISMATCH');
      }
    });

    it('should reject chunk with mismatched membershipEpoch', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      const badChunk: SnapshotChunkPayload = {
        ...chunks[0]!,
        membershipEpoch: sampleEpoch + 1,
      };

      const res = assembler.ingestChunk(badChunk);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('EPOCH_MISMATCH');
      }
    });

    it('should validate envelope context matching supplier and epoch', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        connectionGeneration: sampleGeneration,
      });

      // Mismatched sender in envelope
      const resBadSender = assembler.ingestChunk(chunks[0]!, {
        sessionId: sampleSessionId,
        senderPeerId: 'malicious-impostor',
        membershipEpoch: sampleEpoch,
        connectionGeneration: sampleGeneration,
      });
      expect(resBadSender.ok).toBe(false);
      if (!resBadSender.ok) {
        expect(resBadSender.reason).toBe('IDENTITY_MISMATCH');
      }

      // Valid envelope context
      const resValid = assembler.ingestChunk(chunks[0]!, {
        sessionId: sampleSessionId,
        senderPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        connectionGeneration: sampleGeneration,
      });
      expect(resValid.ok).toBe(true);
    });
  });

  describe('Failover Handling (AC-M4-TR-4)', () => {
    it('should preserve verified chunks on exact snapshot failover match', () => {
      const rawData = new Uint8Array(1024);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest chunks 0 and 1
      assembler.ingestChunk(chunks[0]!);
      assembler.ingestChunk(chunks[1]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);

      // Failover to a new supplier and new epoch with exact same snapshot
      const newTransferId = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
      const newSupplier = 'peer-supplier-2';
      const newEpoch = sampleEpoch + 1;

      const failoverResult = assembler.prepareForFailover({
        transferId: newTransferId,
        supplierPeerId: newSupplier,
        membershipEpoch: newEpoch,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: overallSha256,
        totalChunks,
        totalSizeBytes,
      });

      expect(failoverResult.retainedChunks).toBe(2);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);
      expect(assembler.getMissingChunkIndices()).toEqual([2, 3]);

      // Now chunks 2 and 3 must carry the new transferId, epoch, and supplier
      const chunk2New: SnapshotChunkPayload = {
        ...chunks[2]!,
        transferId: newTransferId,
        membershipEpoch: newEpoch,
        supplierPeerId: newSupplier,
      };
      const chunk3New: SnapshotChunkPayload = {
        ...chunks[3]!,
        transferId: newTransferId,
        membershipEpoch: newEpoch,
        supplierPeerId: newSupplier,
      };

      expect(assembler.ingestChunk(chunk2New).ok).toBe(true);
      expect(assembler.ingestChunk(chunk3New).ok).toBe(true);
      expect(assembler.isComplete()).toBe(true);

      const finalize = assembler.finalizeAssembly();
      expect(finalize.ok).toBe(true);
      if (finalize.ok) {
        expect(finalize.snapshot.buffer.byteLength).toBe(1024);
      }
    });

    it('should purge all chunks immediately when failover snapshot differs', () => {
      const rawData = new Uint8Array(512);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0]);

      // Failover to a completely new snapshotId
      const failoverResult = assembler.prepareForFailover({
        transferId: 'bbbbbbbb-cccc-4ddd-8eee-ffffffffffff',
        supplierPeerId: 'peer-supplier-3',
        membershipEpoch: sampleEpoch + 1,
        snapshotId: 'snap-002-newer',
        expectedSha256Digest: 'f'.repeat(64),
        totalChunks: 2,
        totalSizeBytes: 512,
      });

      expect(failoverResult.retainedChunks).toBe(0);
      expect(assembler.getReceivedChunkIndices()).toEqual([]);
      expect(assembler.getMissingChunkIndices()).toEqual([0, 1]);
    });
  });

  describe('Memory Release & Disposal (AC-M4-MEM-3)', () => {
    it('should release chunk slice references immediately upon successful finalization', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(true);

      // Calling finalizeAssembly again should indicate session is no longer active
      const secondFinalize = assembler.finalizeAssembly();
      expect(secondFinalize.ok).toBe(false);

      // Ingesting should also fail because chunks array was released
      const secondIngest = assembler.ingestChunk(chunks[0]!);
      expect(secondIngest.ok).toBe(false);
    });

    it('should permanently clean up resources upon dispose()', () => {
      const rawData = new Uint8Array(256);
      const { totalChunks, totalSizeBytes, overallSha256, chunks } = createChunksFromBuffer(
        rawData,
        256
      );

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks,
        totalSizeBytes,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.dispose();

      expect(assembler.isComplete()).toBe(false);
      expect(assembler.getProgress()).toBeNull();

      const ingestResult = assembler.ingestChunk(chunks[0]!);
      expect(ingestResult.ok).toBe(false);
      if (!ingestResult.ok) {
        expect(ingestResult.reason).toBe('LOCAL_ABORT');
      }

      const finalizeResult = assembler.finalizeAssembly();
      expect(finalizeResult.ok).toBe(false);
      if (!finalizeResult.ok) {
        expect(finalizeResult.reason).toBe('LOCAL_ABORT');
      }
    });
  });
});
