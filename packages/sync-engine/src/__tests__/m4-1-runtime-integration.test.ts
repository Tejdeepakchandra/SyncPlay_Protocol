/**
 * Phase 7.5 M4.1 Runtime Integration Test.
 *
 * Verifies end-to-end integration across @syncplay/protocol and @syncplay/sync-engine:
 * 1. Simulates inbound DataChannel message receiving raw JSON string.
 * 2. Parses and validates through parseAndValidateMessage() (production boundary).
 * 3. Proves strict SnapshotChunkEnvelopeSchema enforcement:
 *    - Valid chunk message succeeds.
 *    - Invalid chunk message (missing epoch, prohibited causal fields, malformed Base64, extra fields)
 *      is rejected at the network boundary.
 * 4. Passes successfully parsed payload directly to ChunkAssembler.
 * 5. Verifies complete assembly and SHA-256 verification through this pipeline.
 */

import { describe, it, expect } from 'vitest';
import {
  PROTOCOL_VERSION,
  SYNC_MESSAGE_TYPES,
  MessageType,
  parseAndValidateMessage,
  type SnapshotChunkPayload,
} from '@syncplay/protocol';
import { ChunkAssembler, sha256 } from '../index.js';

describe('Phase 7.5 M4.1 Runtime Integration — Network Boundary & Chunk Assembler', () => {
  const sampleUuid = '12345678-1234-4234-8234-123456789abc';
  const sampleSessionId = 'session-integration-room';
  const sampleSupplier = 'peer-supplier-42';
  const sampleSnapshotId = 'snapshot-alpha-v1';
  const sampleEpoch = 3;
  const sampleGeneration = 1;

  it('should accept valid snapshot chunk over simulated wire, validate, and assemble in ChunkAssembler', () => {
    // 1. Prepare raw snapshot data (512 bytes split into 2 chunks of 256 bytes)
    const rawData = new Uint8Array(512);
    for (let i = 0; i < 512; i++) rawData[i] = (i * 7) % 256;
    const overallSha = sha256(rawData);

    const chunk0Bytes = rawData.slice(0, 256);
    const chunk1Bytes = rawData.slice(256, 512);

    const chunk0Payload: SnapshotChunkPayload = {
      transferId: sampleUuid,
      sessionId: sampleSessionId,
      membershipEpoch: sampleEpoch,
      supplierPeerId: sampleSupplier,
      snapshotId: sampleSnapshotId,
      chunkIndex: 0,
      totalChunks: 2,
      chunkSizeBytes: 256,
      totalSizeBytes: 512,
      chunkSha256: sha256(chunk0Bytes),
      dataBase64: Buffer.from(chunk0Bytes).toString('base64'),
    };

    const chunk1Payload: SnapshotChunkPayload = {
      transferId: sampleUuid,
      sessionId: sampleSessionId,
      membershipEpoch: sampleEpoch,
      supplierPeerId: sampleSupplier,
      snapshotId: sampleSnapshotId,
      chunkIndex: 1,
      totalChunks: 2,
      chunkSizeBytes: 256,
      totalSizeBytes: 512,
      chunkSha256: sha256(chunk1Bytes),
      dataBase64: Buffer.from(chunk1Bytes).toString('base64'),
    };

    const wireMessage0 = JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: sampleSessionId,
      senderPeerId: sampleSupplier,
      messageId: 'msg-chunk-0',
      membershipEpoch: sampleEpoch,
      connectionGeneration: sampleGeneration,
      payload: chunk0Payload,
    });

    const wireMessage1 = JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: sampleSessionId,
      senderPeerId: sampleSupplier,
      messageId: 'msg-chunk-1',
      membershipEpoch: sampleEpoch,
      connectionGeneration: sampleGeneration,
      payload: chunk1Payload,
    });

    // 2. Initialize ChunkAssembler
    const assembler = new ChunkAssembler();
    assembler.initTransfer({
      transferId: sampleUuid,
      sessionId: sampleSessionId,
      snapshotId: sampleSnapshotId,
      totalChunks: 2,
      totalSizeBytes: 512,
      expectedSha256Digest: overallSha,
      supplierPeerId: sampleSupplier,
      membershipEpoch: sampleEpoch,
      connectionGeneration: sampleGeneration,
    });

    // 3. Ingest chunk 0 through network boundary
    const parseResult0 = parseAndValidateMessage(wireMessage0);
    expect(parseResult0.ok).toBe(true);
    if (parseResult0.ok) {
      const msg = parseResult0.value;
      const ingest0 = assembler.ingestChunk(msg.payload as SnapshotChunkPayload, {
        sessionId: msg.sessionId,
        senderPeerId: msg.senderPeerId,
        membershipEpoch: msg.membershipEpoch!,
        connectionGeneration: msg.connectionGeneration,
      });
      expect(ingest0.ok).toBe(true);
      if (ingest0.ok) {
        expect(ingest0.completed).toBe(false);
      }
    }

    // 4. Ingest chunk 1 through network boundary
    const parseResult1 = parseAndValidateMessage(wireMessage1);
    expect(parseResult1.ok).toBe(true);
    if (parseResult1.ok) {
      const msg = parseResult1.value;
      const ingest1 = assembler.ingestChunk(msg.payload as SnapshotChunkPayload, {
        sessionId: msg.sessionId,
        senderPeerId: msg.senderPeerId,
        membershipEpoch: msg.membershipEpoch!,
        connectionGeneration: msg.connectionGeneration,
      });
      expect(ingest1.ok).toBe(true);
      if (ingest1.ok) {
        expect(ingest1.completed).toBe(true);
      }
    }

    // 5. Finalize assembly
    const finalizeResult = assembler.finalizeAssembly();
    expect(finalizeResult.ok).toBe(true);
    if (finalizeResult.ok) {
      expect(finalizeResult.snapshot.snapshotId).toBe(sampleSnapshotId);
      expect(finalizeResult.snapshot.sha256Digest).toBe(overallSha);
      expect(Array.from(finalizeResult.snapshot.buffer)).toEqual(Array.from(rawData));
    }
  });

  it('should reject snapshot chunk with prohibited fields at parseAndValidateMessage boundary', () => {
    const rawData = new Uint8Array(256);
    const wireWithProhibited = JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: sampleSessionId,
      senderPeerId: sampleSupplier,
      messageId: 'msg-chunk-prohibited',
      membershipEpoch: sampleEpoch,
      connectionGeneration: sampleGeneration,
      causalContext: { 'peer-a': 5 }, // PROHIBITED on snapshot chunk!
      payload: {
        transferId: sampleUuid,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 0,
        totalChunks: 1,
        chunkSizeBytes: 256,
        totalSizeBytes: 256,
        chunkSha256: sha256(rawData),
        dataBase64: Buffer.from(rawData).toString('base64'),
      },
    });

    const parseResult = parseAndValidateMessage(wireWithProhibited);
    expect(parseResult.ok).toBe(false);
    if (!parseResult.ok) {
      expect(parseResult.error.code).toBe('INVALID_ENVELOPE');
      expect(parseResult.error.message).toContain('causalContext');
    }
  });

  it('should reject snapshot chunk with missing membershipEpoch at boundary', () => {
    const rawData = new Uint8Array(256);
    const wireMissingEpoch = JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: sampleSessionId,
      senderPeerId: sampleSupplier,
      messageId: 'msg-chunk-no-epoch',
      // membershipEpoch omitted!
      connectionGeneration: sampleGeneration,
      payload: {
        transferId: sampleUuid,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 0,
        totalChunks: 1,
        chunkSizeBytes: 256,
        totalSizeBytes: 256,
        chunkSha256: sha256(rawData),
        dataBase64: Buffer.from(rawData).toString('base64'),
      },
    });

    const parseResult = parseAndValidateMessage(wireMissingEpoch);
    expect(parseResult.ok).toBe(false);
    if (!parseResult.ok) {
      expect(parseResult.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  it('should reject snapshot chunk with noncanonical Base64 at boundary', () => {
    const wireBadBase64 = JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: sampleSessionId,
      senderPeerId: sampleSupplier,
      messageId: 'msg-chunk-bad-b64',
      membershipEpoch: sampleEpoch,
      connectionGeneration: sampleGeneration,
      payload: {
        transferId: sampleUuid,
        sessionId: sampleSessionId,
        membershipEpoch: sampleEpoch,
        supplierPeerId: sampleSupplier,
        snapshotId: sampleSnapshotId,
        chunkIndex: 0,
        totalChunks: 1,
        chunkSizeBytes: 256,
        totalSizeBytes: 256,
        chunkSha256: '0'.repeat(64),
        dataBase64: 'A'.repeat(344), // Invalid Base64: trailing bits non-zero / non-canonical
      },
    });

    const parseResult = parseAndValidateMessage(wireBadBase64);
    expect(parseResult.ok).toBe(false);
    if (!parseResult.ok) {
      expect(parseResult.error.code).toBe('INVALID_ENVELOPE');
    }
  });
});
