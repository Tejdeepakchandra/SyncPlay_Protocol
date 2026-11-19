/**
 * Comprehensive test suite for Phase 7.5 M4.1 snapshot recovery wire schemas.
 *
 * Validates:
 * - SnapshotOfferPayloadSchema
 * - SnapshotRequestPayloadSchema
 * - SnapshotChunkPayloadSchema
 * - SnapshotChunkRequestPayloadSchema
 * - SnapshotAbortPayloadSchema
 * - SnapshotChunkEnvelopeSchema
 * - Production validateMessage runtime integration
 */
import { describe, it, expect } from 'vitest';
import {
  PROTOCOL_VERSION,
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_ENVELOPE_OVERHEAD_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MIN_BASE64_CHUNK_BYTES,
  MIN_TRANSPORT_LIMIT_FULL_CHUNK,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  MessageType,
  SYNC_MESSAGE_TYPES,
  SnapshotChunkEnvelopeSchema,
  SnapshotChunkPayloadSchema,
  SnapshotOfferPayloadSchema,
  SnapshotRequestPayloadSchema,
  SnapshotChunkRequestPayloadSchema,
  SnapshotAbortPayloadSchema,
  validateMessage,
  parseAndValidateMessage,
  computeUtf8ByteLength,
  serializeAndCheckSize,
} from '../index.js';

describe('Phase 7.5 M4.1 — Snapshot Recovery Wire Schemas', () => {
  const sampleUuid = '12345678-1234-4234-8234-123456789abc';
  const sampleSha256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

  function createValidChunkPayload(overrides: Record<string, unknown> = {}) {
    const chunkSizeBytes = 256;
    const rawBytes = Buffer.alloc(chunkSizeBytes, 0x42);
    const dataBase64 = rawBytes.toString('base64');
    return {
      transferId: sampleUuid,
      sessionId: 'session-001',
      membershipEpoch: 1,
      supplierPeerId: 'peer-supplier-1',
      snapshotId: 'snap-001',
      chunkIndex: 0,
      totalChunks: 4,
      chunkSizeBytes,
      totalSizeBytes: 1024,
      chunkSha256: sampleSha256,
      dataBase64,
      ...overrides,
    };
  }

  function createValidChunkEnvelope(overrides: Record<string, unknown> = {}) {
    return {
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: 'session-001',
      senderPeerId: 'peer-supplier-1',
      messageId: 'msg-001',
      membershipEpoch: 1,
      connectionGeneration: 1,
      payload: createValidChunkPayload(),
      ...overrides,
    };
  }

  // ── Schema Validation: SnapshotChunkEnvelopeSchema ──────────────────────────

  describe('SnapshotChunkEnvelopeSchema', () => {
    it('should accept valid maximum-size chunk envelope', () => {
      const chunkBytes = Buffer.alloc(MAX_CHUNK_BYTES, 0x5a);
      const dataBase64 = chunkBytes.toString('base64');
      expect(dataBase64.length).toBe(43692);

      const maxEnvelope = {
        protocolVersion: PROTOCOL_VERSION,
        messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
        sessionId: 's'.repeat(128),
        senderPeerId: 'p'.repeat(128),
        messageId: 'm'.repeat(128),
        membershipEpoch: Number.MAX_SAFE_INTEGER,
        connectionGeneration: Number.MAX_SAFE_INTEGER,
        payload: {
          transferId: sampleUuid,
          sessionId: 's'.repeat(128),
          membershipEpoch: Number.MAX_SAFE_INTEGER,
          supplierPeerId: 'p'.repeat(64),
          snapshotId: 'k'.repeat(128),
          chunkIndex: 127,
          totalChunks: 128,
          chunkSizeBytes: MAX_CHUNK_BYTES,
          totalSizeBytes: MAX_SNAPSHOT_BYTES,
          chunkSha256: sampleSha256,
          dataBase64,
        },
      };

      const result = SnapshotChunkEnvelopeSchema.safeParse(maxEnvelope);
      expect(result.success).toBe(true);

      // Verify exact framing overhead and full serialized message
      const json = JSON.stringify(maxEnvelope);
      const byteLength = Buffer.byteLength(json, 'utf-8');
      expect(byteLength).toBe(44906); // 1,214 B framing + 43,692 B base64
      expect(byteLength).toBeLessThanOrEqual(MIN_TRANSPORT_LIMIT_FULL_CHUNK);
    });

    it('should reject missing required membershipEpoch', () => {
      const msg = createValidChunkEnvelope();
      delete (msg as Record<string, unknown>).membershipEpoch;
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should reject missing required connectionGeneration', () => {
      const msg = createValidChunkEnvelope();
      delete (msg as Record<string, unknown>).connectionGeneration;
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should reject incorrect protocolVersion', () => {
      const msg = createValidChunkEnvelope({ protocolVersion: 99 });
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should reject incorrect messageType', () => {
      const msg = createValidChunkEnvelope({ messageType: 'PING' });
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should reject Unicode identifiers in envelope', () => {
      const msg = createValidChunkEnvelope({ sessionId: 'session-🚀-123' });
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should reject oversized identifiers exceeding maximum lengths', () => {
      const msg = createValidChunkEnvelope({ sessionId: 's'.repeat(129) });
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
    });

    it('should strictly reject all prohibited operation and causal fields', () => {
      const prohibitedFields = [
        { causalContext: { 'peer-1': 5 } },
        { logicalEventId: 'op-event-1' },
        { originPeerId: 'peer-origin' },
        { logicalTimestamp: 100 },
        { originSequenceNumber: 10 },
      ];

      for (const field of prohibitedFields) {
        const msg = createValidChunkEnvelope(field);
        const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
        expect(result.success).toBe(false);
      }
    });

    it('should strictly reject unknown fields on envelope (.strict())', () => {
      const msg = createValidChunkEnvelope({ unexpectedMetadata: 'malicious' });
      const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.issues[0]?.code).toBe('unrecognized_keys');
      }
    });

    it('should reject unsafe integer epochs and generations', () => {
      const msg1 = createValidChunkEnvelope({ membershipEpoch: Number.MAX_SAFE_INTEGER + 10 });
      expect(SnapshotChunkEnvelopeSchema.safeParse(msg1).success).toBe(false);

      const msg2 = createValidChunkEnvelope({ connectionGeneration: Number.MAX_SAFE_INTEGER + 10 });
      expect(SnapshotChunkEnvelopeSchema.safeParse(msg2).success).toBe(false);
    });
  });

  // ── Schema Validation: SnapshotChunkPayloadSchema ───────────────────────────

  describe('SnapshotChunkPayloadSchema — Base64 & Partition Verification', () => {
    it('should reject malformed Base64 (invalid characters)', () => {
      const payload = createValidChunkPayload({
        dataBase64: '!!!NOT_BASE_64!!!===',
      });
      const result = SnapshotChunkPayloadSchema.safeParse(payload);
      expect(result.success).toBe(false);
    });

    it('should reject noncanonical Base64 (invalid trailing bits or padding)', () => {
      // 1 byte (0x00) canonical Base64 is 'AA=='. 'AB==' has non-zero unused trailing bits.
      const payload = createValidChunkPayload({
        chunkSizeBytes: 1,
        totalSizeBytes: 1,
        totalChunks: 1,
        chunkIndex: 0,
        dataBase64: 'AB==',
      });
      const result = SnapshotChunkPayloadSchema.safeParse(payload);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.issues.some((i) => i.message.includes('non-canonical'))).toBe(true);
      }
    });

    it('should reject Base64 decoded-size mismatch', () => {
      // Declares 256 bytes, but dataBase64 encodes 100 bytes
      const actual100Bytes = Buffer.alloc(100, 0x01).toString('base64');
      const payload = createValidChunkPayload({
        chunkSizeBytes: 256,
        dataBase64: actual100Bytes,
      });
      const result = SnapshotChunkPayloadSchema.safeParse(payload);
      expect(result.success).toBe(false);
    });

    it('should reject chunk index greater than or equal to totalChunks', () => {
      const payload = createValidChunkPayload({
        chunkIndex: 4,
        totalChunks: 4, // chunkIndex must be < 4 (0..3)
      });
      const result = SnapshotChunkPayloadSchema.safeParse(payload);
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.issues.some((i) => i.message.includes('strictly less than'))).toBe(true);
      }
    });

    it('should reject invalid hash representation (not 64 hex characters)', () => {
      const payload1 = createValidChunkPayload({ chunkSha256: 'not-hex-characters' });
      expect(SnapshotChunkPayloadSchema.safeParse(payload1).success).toBe(false);

      const payload2 = createValidChunkPayload({ chunkSha256: 'a'.repeat(63) });
      expect(SnapshotChunkPayloadSchema.safeParse(payload2).success).toBe(false);

      const payload3 = createValidChunkPayload({ chunkSha256: 'a'.repeat(65) });
      expect(SnapshotChunkPayloadSchema.safeParse(payload3).success).toBe(false);
    });

    it('should reject chunk size exceeding totalSizeBytes', () => {
      const payload = createValidChunkPayload({
        chunkSizeBytes: 256,
        totalSizeBytes: 100, // chunk cannot be larger than entire snapshot
        dataBase64: Buffer.alloc(256).toString('base64'),
      });
      const result = SnapshotChunkPayloadSchema.safeParse(payload);
      expect(result.success).toBe(false);
    });

    it('should reject inconsistent single-chunk transfer declaration', () => {
      // totalChunks is 1, but chunkIndex is 1
      const payload1 = createValidChunkPayload({
        chunkIndex: 1,
        totalChunks: 1,
      });
      expect(SnapshotChunkPayloadSchema.safeParse(payload1).success).toBe(false);

      // totalChunks is 1, but chunkSizeBytes !== totalSizeBytes
      const payload2 = createValidChunkPayload({
        chunkIndex: 0,
        totalChunks: 1,
        chunkSizeBytes: 256,
        totalSizeBytes: 512,
        dataBase64: Buffer.alloc(256).toString('base64'),
      });
      expect(SnapshotChunkPayloadSchema.safeParse(payload2).success).toBe(false);
    });
  });

  // ── Production Validation Integration ───────────────────────────────────────

  describe('Production validateMessage & parseAndValidateMessage Integration', () => {
    it('should validate and accept valid snapshot chunk message through parseAndValidateMessage', () => {
      const msg = createValidChunkEnvelope();
      const rawJson = JSON.stringify(msg);

      const result = parseAndValidateMessage(rawJson);
      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.messageType).toBe(SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK);
        expect(result.value.sessionId).toBe('session-001');
      }
    });

    it('should reject snapshot chunk message with prohibited fields at validateMessage boundary', () => {
      const msg = createValidChunkEnvelope({
        causalContext: { 'peer-evil': 99 },
      });
      const rawJson = JSON.stringify(msg);

      const result = parseAndValidateMessage(rawJson);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error.code).toBe('INVALID_ENVELOPE');
        expect(result.error.message).toMatch(/causalContext/i);
      }
    });

    it('should reject snapshot chunk message with malformed Base64 at validateMessage boundary', () => {
      const msg = createValidChunkEnvelope({
        payload: createValidChunkPayload({
          dataBase64: 'invalid-base64!',
        }),
      });
      const rawJson = JSON.stringify(msg);

      const result = parseAndValidateMessage(rawJson);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error.code).toBe('INVALID_ENVELOPE');
      }
    });
  });

  // ── Supporting Recovery Schemas ─────────────────────────────────────────────

  describe('Supporting Recovery Schemas', () => {
    it('should validate SnapshotOfferPayloadSchema', () => {
      const validOffer = {
        offerId: sampleUuid,
        sessionId: 'session-001',
        membershipEpoch: 1,
        supplierPeerId: 'peer-supplier-1',
        snapshotId: 'snap-001',
        authorPeerId: 'peer-author-1',
        snapshotFrontier: { 'peer-author-1': 10 },
        maxLogicalTimestamp: 500,
        totalSizeBytes: 1024,
        totalChunks: 4,
        sha256Digest: sampleSha256,
      };
      const result = SnapshotOfferPayloadSchema.safeParse(validOffer);
      expect(result.success).toBe(true);
    });

    it('should validate SnapshotRequestPayloadSchema', () => {
      const validRequest = {
        transferId: sampleUuid,
        sessionId: 'session-001',
        membershipEpoch: 1,
        requesterPeerId: 'peer-req-1',
        supplierPeerId: 'peer-sup-1',
        snapshotId: 'snap-001',
        requestedChunkWindow: 4,
        missingChunkIndices: [0, 1, 2, 3],
      };
      const result = SnapshotRequestPayloadSchema.safeParse(validRequest);
      expect(result.success).toBe(true);
    });

    it('should validate SnapshotChunkRequestPayloadSchema', () => {
      const validChunkReq = {
        transferId: sampleUuid,
        sessionId: 'session-001',
        membershipEpoch: 1,
        requesterPeerId: 'peer-req-1',
        supplierPeerId: 'peer-sup-1',
        snapshotId: 'snap-001',
        missingChunkIndices: [2, 3],
      };
      const result = SnapshotChunkRequestPayloadSchema.safeParse(validChunkReq);
      expect(result.success).toBe(true);
    });

    it('should validate SnapshotAbortPayloadSchema', () => {
      const validAbort = {
        transferId: sampleUuid,
        sessionId: 'session-001',
        membershipEpoch: 1,
        senderPeerId: 'peer-sender-1',
        snapshotId: 'snap-001',
        reason: 'CHUNK_CORRUPTED',
        detail: 'SHA-256 mismatch on chunk 2',
      };
      const result = SnapshotAbortPayloadSchema.safeParse(validAbort);
      expect(result.success).toBe(true);
    });
  });
});
