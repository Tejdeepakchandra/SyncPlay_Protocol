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
      const rawData = new Uint8Array(256).fill(42);
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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

  describe('Failover Handling & Non-Destructive Incompatible Rejection (AC-M4-TR-4)', () => {
    // 3.A Valid same-epoch supplier failover
    it('should retain verified chunks on valid same-epoch supplier failover', () => {
      const rawData = new Uint8Array(1024);
      for (let i = 0; i < rawData.byteLength; i++) rawData[i] = i % 256;
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
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        connectionGeneration: 1,
      });

      // Ingest chunks 0 and 1
      expect(assembler.ingestChunk(chunks[0]!).ok).toBe(true);
      expect(assembler.ingestChunk(chunks[1]!).ok).toBe(true);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);

      // Failover to a new supplier and new transfer ID in the SAME epoch with new generation
      const newTransferId = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
      const newSupplier = 'peer-supplier-2';
      const newGeneration = 5;

      const failoverResult = assembler.prepareForFailover({
        transferId: newTransferId,
        supplierPeerId: newSupplier,
        membershipEpoch: sampleEpoch, // Same epoch
        connectionGeneration: newGeneration,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: overallSha256,
        totalChunks,
        totalSizeBytes,
        nominalChunkSizeBytes: 256,
      });

      expect(failoverResult.ok).toBe(true);
      expect(failoverResult.retainedChunks).toBe(2);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);
      expect(assembler.getMissingChunkIndices()).toEqual([2, 3]);

      // Active transfer context must be updated
      const active = assembler.getActiveTransfer();
      expect(active?.transferId).toBe(newTransferId);
      expect(active?.supplierPeerId).toBe(newSupplier);
      expect(active?.connectionGeneration).toBe(newGeneration);
      expect(active?.membershipEpoch).toBe(sampleEpoch);

      // Chunks carrying the OLD transferId or supplier must be rejected
      const oldSupplierChunk2: SnapshotChunkPayload = {
        ...chunks[2]!,
        transferId: sampleTransferId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
      };
      const oldRes = assembler.ingestChunk(oldSupplierChunk2);
      expect(oldRes.ok).toBe(false);
      if (!oldRes.ok) {
        expect(oldRes.reason).toBe('IDENTITY_MISMATCH');
      }

      // Chunks carrying the NEW transferId, supplier, and generation must be accepted
      const chunk2New: SnapshotChunkPayload = {
        ...chunks[2]!,
        transferId: newTransferId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: newSupplier,
      };
      const chunk3New: SnapshotChunkPayload = {
        ...chunks[3]!,
        transferId: newTransferId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: newSupplier,
      };

      expect(
        assembler.ingestChunk(chunk2New, {
          sessionId: sampleSessionId,
          senderPeerId: newSupplier,
          membershipEpoch: sampleEpoch,
          connectionGeneration: newGeneration,
        }).ok
      ).toBe(true);

      expect(
        assembler.ingestChunk(chunk3New, {
          sessionId: sampleSessionId,
          senderPeerId: newSupplier,
          membershipEpoch: sampleEpoch,
          connectionGeneration: newGeneration,
        }).ok
      ).toBe(true);

      expect(assembler.isComplete()).toBe(true);
      const finalize = assembler.finalizeAssembly();
      expect(finalize.ok).toBe(true);
      if (finalize.ok) {
        expect(finalize.snapshot.buffer.byteLength).toBe(1024);
        expect(finalize.snapshot.sha256Digest).toBe(overallSha256);
      }
    });

    // 3.B Cross-epoch candidate rejection
    it('should reject failover candidate from a different membership epoch without mutating active transfer', () => {
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
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      assembler.ingestChunk(chunks[1]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);

      // Attempt failover using Epoch E+1 with otherwise identical snapshot and geometry
      const failoverResult = assembler.prepareForFailover({
        transferId: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
        supplierPeerId: 'peer-supplier-2',
        membershipEpoch: sampleEpoch + 1, // DIFFERENT EPOCH!
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: overallSha256,
        totalChunks,
        totalSizeBytes,
        nominalChunkSizeBytes: 256,
      });

      // Must be non-destructively rejected
      expect(failoverResult.ok).toBe(false);
      if (!failoverResult.ok) {
        expect(failoverResult.reason).toBe('EPOCH_MISMATCH');
        expect(failoverResult.retainedChunks).toBe(0);
      }

      // Active transfer state must remain UNCHANGED
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);
      const active = assembler.getActiveTransfer();
      expect(active?.membershipEpoch).toBe(sampleEpoch);
      expect(active?.supplierPeerId).toBe(sampleSupplier);
      expect(active?.transferId).toBe(sampleTransferId);

      // Verify no E+1 chunk can be accepted into the E transfer
      const crossEpochChunk: SnapshotChunkPayload = {
        ...chunks[2]!,
        membershipEpoch: sampleEpoch + 1,
      };
      const ingestCrossEpoch = assembler.ingestChunk(crossEpochChunk);
      expect(ingestCrossEpoch.ok).toBe(false);
      if (!ingestCrossEpoch.ok) {
        expect(ingestCrossEpoch.reason).toBe('EPOCH_MISMATCH');
      }

      // Active transfer is still healthy and can accept E chunk from original supplier
      expect(assembler.ingestChunk(chunks[2]!).ok).toBe(true);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1, 2]);
    });

    // 3.C Stale snapshot candidate rejection
    it('should reject failover candidate with mismatched snapshotId without mutating active transfer', () => {
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
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      assembler.ingestChunk(chunks[1]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);

      // Failover candidate offering a different snapshotId
      const failoverResult = assembler.prepareForFailover({
        transferId: 'bbbbbbbb-cccc-4ddd-8eee-ffffffffffff',
        supplierPeerId: 'peer-supplier-3',
        membershipEpoch: sampleEpoch,
        snapshotId: 'stale-snapshot-999',
        expectedSha256Digest: overallSha256,
        totalChunks,
        totalSizeBytes,
        nominalChunkSizeBytes: 256,
      });

      expect(failoverResult.ok).toBe(false);
      if (!failoverResult.ok) {
        expect(failoverResult.reason).toBe('SNAPSHOT_ID_MISMATCH');
        expect(failoverResult.retainedChunks).toBe(0);
      }

      // Original chunks and transfer state must be completely preserved
      expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);
      const active = assembler.getActiveTransfer();
      expect(active?.snapshotId).toBe(sampleSnapshotId);
      expect(active?.supplierPeerId).toBe(sampleSupplier);
      expect(active?.transferId).toBe(sampleTransferId);

      // Verify active transfer remains usable
      expect(assembler.ingestChunk(chunks[2]!).ok).toBe(true);
    });

    // 3.D Digest mismatch rejection
    it('should reject failover candidate with mismatched digest without mutating active transfer', () => {
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
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0]);

      // Candidate offers same snapshotId but different SHA-256 digest
      const failoverResult = assembler.prepareForFailover({
        transferId: 'cccccccc-dddd-4eee-8fff-000000000000',
        supplierPeerId: 'peer-supplier-4',
        membershipEpoch: sampleEpoch,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: 'e'.repeat(64),
        totalChunks,
        totalSizeBytes,
        nominalChunkSizeBytes: 256,
      });

      expect(failoverResult.ok).toBe(false);
      if (!failoverResult.ok) {
        expect(failoverResult.reason).toBe('DIGEST_MISMATCH');
        expect(failoverResult.retainedChunks).toBe(0);
      }

      // Existing transfer state must remain intact
      expect(assembler.getReceivedChunkIndices()).toEqual([0]);
      expect(assembler.getActiveTransfer()?.expectedSha256Digest).toBe(overallSha256);
      expect(assembler.ingestChunk(chunks[1]!).ok).toBe(true);
    });

    // 3.E Geometry mismatch rejection
    describe('Geometry mismatch rejection', () => {
      it('should reject candidate with mismatched totalSizeBytes without mutating active transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'dddddddd-eeee-4fff-8aaa-111111111111',
          supplierPeerId: 'peer-supplier-5',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks: 3,
          totalSizeBytes: 768, // Mismatched size
          nominalChunkSizeBytes: 256,
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('GEOMETRY_MISMATCH');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
        expect(assembler.getActiveTransfer()?.totalSizeBytes).toBe(1024);
      });

      it('should reject candidate with mismatched totalChunks without mutating active transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'dddddddd-eeee-4fff-8aaa-222222222222',
          supplierPeerId: 'peer-supplier-5',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks: 2, // Mismatched chunk count
          totalSizeBytes: 1024,
          nominalChunkSizeBytes: 512,
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('GEOMETRY_MISMATCH');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
        expect(assembler.getActiveTransfer()?.totalChunks).toBe(4);
      });

      it('should reject candidate with mismatched nominalChunkSizeBytes without mutating active transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'dddddddd-eeee-4fff-8aaa-333333333333',
          supplierPeerId: 'peer-supplier-5',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks: 4,
          totalSizeBytes: 1024,
          nominalChunkSizeBytes: 512, // Mismatched nominal size
        });

        expect(failoverResult.ok).toBe(false);
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
        expect(assembler.getActiveTransfer()?.nominalChunkSizeBytes).toBe(256);
      });
    });

    // 3.F Invalid candidate parameter rejection
    describe('Invalid candidate parameter rejection', () => {
      it('should reject candidate with invalid nominalChunkSizeBytes without destroying transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'eeeeeeee-ffff-4aaa-8bbb-000000000001',
          supplierPeerId: 'peer-supplier-6',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks,
          totalSizeBytes,
          nominalChunkSizeBytes: 100, // < MIN_RAW_CHUNK_BYTES (256)
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('INVALID_PARAMETERS');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
        expect(assembler.ingestChunk(chunks[1]!).ok).toBe(true);
      });

      it('should reject candidate with invalid partition chunk count without destroying transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'eeeeeeee-ffff-4aaa-8bbb-000000000002',
          supplierPeerId: 'peer-supplier-6',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks: 99, // Inconsistent with ceil(1024 / 256) = 4
          totalSizeBytes,
          nominalChunkSizeBytes: 256,
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('INVALID_PARAMETERS');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
      });

      it('should reject candidate with malformed digest without destroying transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'eeeeeeee-ffff-4aaa-8bbb-000000000003',
          supplierPeerId: 'peer-supplier-6',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: 'invalid-non-hex-hash',
          totalChunks,
          totalSizeBytes,
          nominalChunkSizeBytes: 256,
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('INVALID_PARAMETERS');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
      });

      it('should reject candidate with mismatched sessionId without destroying transfer', () => {
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
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks[0]!);

        const failoverResult = assembler.prepareForFailover({
          transferId: 'eeeeeeee-ffff-4aaa-8bbb-000000000004',
          supplierPeerId: 'peer-supplier-6',
          membershipEpoch: sampleEpoch,
          snapshotId: sampleSnapshotId,
          expectedSha256Digest: overallSha256,
          totalChunks,
          totalSizeBytes,
          nominalChunkSizeBytes: 256,
          sessionId: 'different-session-xyz', // Mismatched sessionId
        });

        expect(failoverResult.ok).toBe(false);
        if (!failoverResult.ok) {
          expect(failoverResult.reason).toBe('SESSION_MISMATCH');
        }
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);
      });
    });

    // 3.G Explicit restart
    describe('Explicit transfer restart', () => {
      it('should support explicit restart via abortTransfer() and initTransfer()', () => {
        const rawDataE = new Uint8Array(1024).fill(11);
        const chunksE = createChunksFromBuffer(
          rawDataE,
          256,
          sampleTransferId,
          sampleSessionId,
          'snap-epoch-1',
          sampleSupplier,
          sampleEpoch
        );

        assembler.initTransfer({
          transferId: sampleTransferId,
          sessionId: sampleSessionId,
          snapshotId: 'snap-epoch-1',
          totalChunks: chunksE.totalChunks,
          totalSizeBytes: chunksE.totalSizeBytes,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: chunksE.overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        // Ingest chunks in epoch E
        assembler.ingestChunk(chunksE.chunks[0]!);
        assembler.ingestChunk(chunksE.chunks[1]!);
        expect(assembler.getReceivedChunkIndices()).toEqual([0, 1]);

        // Explicit caller action: coordinator aborts transfer
        assembler.abortTransfer();
        expect(assembler.getActiveTransfer()).toBeNull();
        expect(assembler.getProgress()).toBeNull();

        // Initialize new transfer in epoch E+1 with new snapshot
        const rawDataEPlus1 = new Uint8Array(512).fill(22);
        const newTransferId = 'ffffffff-0000-4111-8222-333333333333';
        const newEpoch = sampleEpoch + 1;
        const chunksEPlus1 = createChunksFromBuffer(
          rawDataEPlus1,
          256,
          newTransferId,
          sampleSessionId,
          'snap-epoch-2',
          'peer-supplier-new',
          newEpoch
        );

        assembler.initTransfer({
          transferId: newTransferId,
          sessionId: sampleSessionId,
          snapshotId: 'snap-epoch-2',
          totalChunks: chunksEPlus1.totalChunks,
          totalSizeBytes: chunksEPlus1.totalSizeBytes,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: chunksEPlus1.overallSha256,
          supplierPeerId: 'peer-supplier-new',
          membershipEpoch: newEpoch,
        });

        // Verify no old chunks remain
        expect(assembler.getReceivedChunkIndices()).toEqual([]);
        expect(assembler.getMissingChunkIndices()).toEqual([0, 1]);

        // Old-epoch chunks must be rejected
        const oldChunkRes = assembler.ingestChunk(chunksE.chunks[0]!);
        expect(oldChunkRes.ok).toBe(false);

        // New-epoch chunks must be accepted
        expect(assembler.ingestChunk(chunksEPlus1.chunks[0]!).ok).toBe(true);
        expect(assembler.ingestChunk(chunksEPlus1.chunks[1]!).ok).toBe(true);

        expect(assembler.isComplete()).toBe(true);
        const finalSnapshot = assembler.finalizeAssembly();
        expect(finalSnapshot.ok).toBe(true);
      });

      it('should support explicit restart via restartTransfer() method', () => {
        const rawData1 = new Uint8Array(512).fill(7);
        const chunks1 = createChunksFromBuffer(
          rawData1,
          256,
          sampleTransferId,
          sampleSessionId,
          'snap-1',
          sampleSupplier,
          sampleEpoch
        );

        assembler.initTransfer({
          transferId: sampleTransferId,
          sessionId: sampleSessionId,
          snapshotId: 'snap-1',
          totalChunks: 2,
          totalSizeBytes: 512,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: chunks1.overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });

        assembler.ingestChunk(chunks1.chunks[0]!);
        expect(assembler.getReceivedChunkIndices()).toEqual([0]);

        // Call explicit restartTransfer
        const rawData2 = new Uint8Array(256).fill(8);
        const newTransferId = '00000000-1111-4222-8333-444444444444';
        const chunks2 = createChunksFromBuffer(
          rawData2,
          256,
          newTransferId,
          sampleSessionId,
          'snap-2',
          'peer-supplier-2',
          sampleEpoch + 1
        );

        assembler.restartTransfer({
          transferId: newTransferId,
          sessionId: sampleSessionId,
          snapshotId: 'snap-2',
          totalChunks: 1,
          totalSizeBytes: 256,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: chunks2.overallSha256,
          supplierPeerId: 'peer-supplier-2',
          membershipEpoch: sampleEpoch + 1,
        });

        expect(assembler.getReceivedChunkIndices()).toEqual([]);
        expect(assembler.getActiveTransfer()?.snapshotId).toBe('snap-2');
        expect(assembler.getActiveTransfer()?.membershipEpoch).toBe(sampleEpoch + 1);

        const ingestRes = assembler.ingestChunk(chunks2.chunks[0]!);
        expect(ingestRes.ok).toBe(true);
        expect(assembler.isComplete()).toBe(true);
      });
    });

    it('should reject prepareForFailover when assembler is disposed or has no active transfer', () => {
      // Disposed assembler
      const disposedAssembler = new ChunkAssembler();
      disposedAssembler.dispose();
      const disposedRes = disposedAssembler.prepareForFailover({
        transferId: sampleTransferId,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: 'a'.repeat(64),
        totalChunks: 1,
        totalSizeBytes: 256,
        nominalChunkSizeBytes: 256,
      });
      expect(disposedRes.ok).toBe(false);
      if (!disposedRes.ok) {
        expect(disposedRes.reason).toBe('DISPOSED');
      }

      // Inactive assembler (no transfer active)
      const inactiveAssembler = new ChunkAssembler();
      const inactiveRes = inactiveAssembler.prepareForFailover({
        transferId: sampleTransferId,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: 'a'.repeat(64),
        totalChunks: 1,
        totalSizeBytes: 256,
        nominalChunkSizeBytes: 256,
      });
      expect(inactiveRes.ok).toBe(false);
      if (!inactiveRes.ok) {
        expect(inactiveRes.reason).toBe('NO_ACTIVE_TRANSFER');
      }
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
        nominalChunkSizeBytes: 256,
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
        nominalChunkSizeBytes: 256,
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

  describe('F-1: Canonical Chunk Partition Verification & Adversarial Ingestion', () => {
    it('should reject adversarial transfer with irregular chunks [3000, 1000, 2000, 4000] (S=10000, N=4, C=2500)', () => {
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 4,
        totalSizeBytes: 10000,
        nominalChunkSizeBytes: 2500,
        expectedSha256Digest: 'a'.repeat(64),
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Chunk 0 claims chunkSizeBytes = 3000 (expected canonical: 2500)
      const chunk0Bytes = new Uint8Array(3000).fill(1);
      const chunk0: SnapshotChunkPayload = {
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 0,
        totalChunks: 4,
        chunkSizeBytes: 3000,
        totalSizeBytes: 10000,
        chunkSha256: sha256(chunk0Bytes),
        dataBase64: Buffer.from(chunk0Bytes).toString('base64'),
      };

      const res0 = assembler.ingestChunk(chunk0);
      expect(res0.ok).toBe(false);
      if (!res0.ok) {
        expect(res0.reason).toBe('CHUNK_CORRUPTED');
        expect(res0.detail).toContain('expected canonical partition size (2500)');
      }
    });

    it('should reject adversarial transfer with irregular chunks [3000, 2000, 3000, 2000] (S=10000, N=4, C=2500)', () => {
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 4,
        totalSizeBytes: 10000,
        nominalChunkSizeBytes: 2500,
        expectedSha256Digest: 'b'.repeat(64),
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      const chunk0Bytes = new Uint8Array(3000).fill(2);
      const chunk0: SnapshotChunkPayload = {
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 0,
        totalChunks: 4,
        chunkSizeBytes: 3000,
        totalSizeBytes: 10000,
        chunkSha256: sha256(chunk0Bytes),
        dataBase64: Buffer.from(chunk0Bytes).toString('base64'),
      };

      const res0 = assembler.ingestChunk(chunk0);
      expect(res0.ok).toBe(false);
      if (!res0.ok) {
        expect(res0.reason).toBe('CHUNK_CORRUPTED');
        expect(res0.detail).toContain('expected canonical partition size (2500)');
      }
    });

    it('should reject non-uniform middle chunk (size !== nominalChunkSizeBytes)', () => {
      // S=1000, C=256, N=4. Expected: [256, 256, 256, 232]
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 4,
        totalSizeBytes: 1000,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: 'c'.repeat(64),
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Chunk 1 claims 200 instead of 256
      const bytes = new Uint8Array(200);
      const chunk1: SnapshotChunkPayload = {
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 1,
        totalChunks: 4,
        chunkSizeBytes: 200,
        totalSizeBytes: 1000,
        chunkSha256: sha256(bytes),
        dataBase64: Buffer.from(bytes).toString('base64'),
      };

      const res = assembler.ingestChunk(chunk1);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('CHUNK_CORRUPTED');
        expect(res.detail).toContain('expected canonical partition size (256)');
      }
    });

    it('should reject non-uniform final chunk (size !== expectedFinalChunkSize)', () => {
      // S=1000, C=256, N=4. Final chunk must be 1000 - 3*256 = 232
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 4,
        totalSizeBytes: 1000,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: 'd'.repeat(64),
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Chunk 3 claims 256 instead of 232
      const bytes = new Uint8Array(256);
      const chunk3: SnapshotChunkPayload = {
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 3,
        totalChunks: 4,
        chunkSizeBytes: 256,
        totalSizeBytes: 1000,
        chunkSha256: sha256(bytes),
        dataBase64: Buffer.from(bytes).toString('base64'),
      };

      const res = assembler.ingestChunk(chunk3);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('CHUNK_CORRUPTED');
        expect(res.detail).toContain('expected canonical partition size (232)');
      }
    });

    it('should reject out-of-order chunk violating canonical partition even before chunk 0 arrives', () => {
      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 4,
        totalSizeBytes: 1000,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: 'e'.repeat(64),
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest chunk 2 first, but with size 250 instead of 256
      const bytes = new Uint8Array(250);
      const chunk2: SnapshotChunkPayload = {
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 2,
        totalChunks: 4,
        chunkSizeBytes: 250,
        totalSizeBytes: 1000,
        chunkSha256: sha256(bytes),
        dataBase64: Buffer.from(bytes).toString('base64'),
      };

      const res = assembler.ingestChunk(chunk2);
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.reason).toBe('CHUNK_CORRUPTED');
        expect(res.detail).toContain('expected canonical partition size (256)');
      }
    });

    it('should reject invalid initTransfer parameters violating canonical partition equations before allocating storage', () => {
      // 1. totalChunks != ceil(S / C)
      expect(() =>
        assembler.initTransfer({
          transferId: sampleTransferId,
          sessionId: sampleSessionId,
          snapshotId: sampleSnapshotId,
          totalChunks: 5, // ceil(1000 / 256) is 4, not 5
          totalSizeBytes: 1000,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: 'f'.repeat(64),
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        })
      ).toThrow(/canonical partition count/i);

      // 2. nominalChunkSizeBytes < minRawChunkBytes (256)
      expect(() =>
        assembler.initTransfer({
          transferId: sampleTransferId,
          sessionId: sampleSessionId,
          snapshotId: sampleSnapshotId,
          totalChunks: 10,
          totalSizeBytes: 1000,
          nominalChunkSizeBytes: 100,
          expectedSha256Digest: 'f'.repeat(64),
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        })
      ).toThrow(/nominalChunkSizeBytes/i);

      // 3. totalSizeBytes <= 0
      expect(() =>
        assembler.initTransfer({
          transferId: sampleTransferId,
          sessionId: sampleSessionId,
          snapshotId: sampleSnapshotId,
          totalChunks: 1,
          totalSizeBytes: 0,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: 'f'.repeat(64),
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        })
      ).toThrow(/totalSizeBytes.*positive/i);
    });

    it('should reject candidate and preserve active chunks when failover nominalChunkSizeBytes differs from active transfer', () => {
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
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      // Ingest chunk 0
      assembler.ingestChunk(chunks[0]!);
      expect(assembler.getReceivedChunkIndices()).toEqual([0]);

      // Failover with different nominalChunkSizeBytes (e.g. 512)
      const failoverResult = assembler.prepareForFailover({
        transferId: 'cccccccc-dddd-4eee-8fff-000000000000',
        supplierPeerId: 'peer-supplier-4',
        membershipEpoch: sampleEpoch,
        snapshotId: sampleSnapshotId,
        expectedSha256Digest: overallSha256,
        totalChunks: 2,
        totalSizeBytes: 1024,
        nominalChunkSizeBytes: 512, // Different partition!
      });

      // Candidate must be rejected non-destructively; active transfer chunks preserved
      expect(failoverResult.ok).toBe(false);
      expect(failoverResult.retainedChunks).toBe(0);
      expect(assembler.getReceivedChunkIndices()).toEqual([0]);
    });
  });

  describe('F-4: ChunkAssembler Lifecycle & Transfer Reset Invariants', () => {
    it('should allow immediate initTransfer for next transfer without reset() after successful finalizeAssembly()', () => {
      const transferId1 = '11111111-1111-4111-8111-111111111111';
      const snapshotId1 = 'snap-1';
      const rawData1 = new Uint8Array(256).fill(11);
      const transfer1 = createChunksFromBuffer(
        rawData1,
        256,
        transferId1,
        sampleSessionId,
        snapshotId1,
        sampleSupplier,
        sampleEpoch
      );

      assembler.initTransfer({
        transferId: transferId1,
        sessionId: sampleSessionId,
        snapshotId: snapshotId1,
        totalChunks: 1,
        totalSizeBytes: 256,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: transfer1.overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(transfer1.chunks[0]!);
      const finalResult1 = assembler.finalizeAssembly();
      expect(finalResult1.ok).toBe(true);

      // Verify that a second transfer can immediately be initialized without calling reset()
      const transferId2 = '22222222-2222-4222-8222-222222222222';
      const snapshotId2 = 'snap-2';
      const rawData2 = new Uint8Array(256).fill(22);
      const transfer2 = createChunksFromBuffer(
        rawData2,
        256,
        transferId2,
        sampleSessionId,
        snapshotId2,
        sampleSupplier,
        sampleEpoch
      );

      expect(() => {
        assembler.initTransfer({
          transferId: transferId2,
          sessionId: sampleSessionId,
          snapshotId: snapshotId2,
          totalChunks: 1,
          totalSizeBytes: 256,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: transfer2.overallSha256,
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });
      }).not.toThrow();

      expect(assembler.getMissingChunkIndices()).toEqual([0]);
      assembler.ingestChunk(transfer2.chunks[0]!);
      const finalResult2 = assembler.finalizeAssembly();
      expect(finalResult2.ok).toBe(true);
      if (finalResult2.ok) {
        expect(finalResult2.snapshot.snapshotId).toBe('snap-2');
      }
    });

    it('should reject ingestChunk after successful finalizeAssembly() with IDENTITY_MISMATCH', () => {
      const rawData = new Uint8Array(256).fill(33);
      const { chunks, overallSha256 } = createChunksFromBuffer(rawData, 256);

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 1,
        totalSizeBytes: 256,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: overallSha256,
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      const finalizeRes = assembler.finalizeAssembly();
      expect(finalizeRes.ok).toBe(true);

      // Ingesting again should fail with IDENTITY_MISMATCH because no transfer is active
      const lateIngest = assembler.ingestChunk(chunks[0]!);
      expect(lateIngest.ok).toBe(false);
      if (!lateIngest.ok) {
        expect(lateIngest.reason).toBe('IDENTITY_MISMATCH');
      }
    });

    it('should reset transfer state upon hash mismatch during finalizeAssembly(), allowing immediate initTransfer()', () => {
      const rawData = new Uint8Array(256).fill(44);
      const { chunks } = createChunksFromBuffer(rawData, 256);

      assembler.initTransfer({
        transferId: sampleTransferId,
        sessionId: sampleSessionId,
        snapshotId: sampleSnapshotId,
        totalChunks: 1,
        totalSizeBytes: 256,
        nominalChunkSizeBytes: 256,
        expectedSha256Digest: '0'.repeat(64), // deliberately wrong hash
        supplierPeerId: sampleSupplier,
        membershipEpoch: sampleEpoch,
      });

      assembler.ingestChunk(chunks[0]!);
      const finalizeRes = assembler.finalizeAssembly();
      expect(finalizeRes.ok).toBe(false);
      if (!finalizeRes.ok) {
        expect(finalizeRes.reason).toBe('HASH_MISMATCH');
      }

      // Proves transfer was aborted and reset: immediate initTransfer for new transfer succeeds
      expect(() => {
        assembler.initTransfer({
          transferId: '33333333-3333-4333-8333-333333333333',
          sessionId: sampleSessionId,
          snapshotId: 'snap-3',
          totalChunks: 1,
          totalSizeBytes: 256,
          nominalChunkSizeBytes: 256,
          expectedSha256Digest: 'a'.repeat(64),
          supplierPeerId: sampleSupplier,
          membershipEpoch: sampleEpoch,
        });
      }).not.toThrow();
    });
  });
});
