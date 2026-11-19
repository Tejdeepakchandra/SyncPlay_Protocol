/**
 * Tests for the protocol message envelope.
 *
 * Validates the ProtocolMessageSchema Zod schema against
 * valid, invalid, and edge-case inputs.
 */
import { describe, it, expect } from 'vitest';
import { ProtocolMessageSchema, SnapshotChunkEnvelopeSchema } from '../envelope.js';
import {
  PROTOCOL_VERSION,
  MAX_ENVELOPE_OVERHEAD_BYTES,
  MIN_RAW_CHUNK_BYTES,
  MIN_BASE64_CHUNK_BYTES,
  MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER,
  MIN_TRANSPORT_LIMIT_FULL_CHUNK,
} from '../constants.js';
import { MessageType, SYNC_MESSAGE_TYPES } from '../message-types.js';
import { computeUtf8ByteLength, serializeAndCheckSize } from '../validation.js';

/** A valid base message for reuse in tests. */
function validMessage(overrides: Record<string, unknown> = {}) {
  return {
    protocolVersion: PROTOCOL_VERSION,
    messageType: MessageType.PING,
    sessionId: 'session-001',
    senderPeerId: 'peer-abc',
    messageId: 'msg-123',
    payload: { probeId: 'probe-1', senderTimestamp: 100 },
    ...overrides,
  };
}

describe('ProtocolMessageSchema', () => {
  it('should accept a valid message with all required fields', () => {
    const result = ProtocolMessageSchema.safeParse(validMessage());
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(result.data.messageType).toBe(MessageType.PING);
      expect(result.data.sessionId).toBe('session-001');
      expect(result.data.senderPeerId).toBe('peer-abc');
      expect(result.data.messageId).toBe('msg-123');
    }
  });

  it('should accept a message with optional logicalEventId', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ logicalEventId: 'op-456' }),
    );
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.logicalEventId).toBe('op-456');
    }
  });

  it('should accept a message without logicalEventId', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.logicalEventId).toBeUndefined();
    }
  });

  it('should strip unknown fields (forward compatibility)', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ unknownField: 'should-be-removed', metadata: {} }),
    );
    expect(result.success).toBe(true);
    if (result.success) {
      expect('unknownField' in result.data).toBe(false);
      expect('metadata' in result.data).toBe(false);
    }
  });

  // ── Missing required fields ────────────────────────────────

  it('should reject when protocolVersion is missing', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['protocolVersion'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(false);
  });

  it('should reject when messageType is missing', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['messageType'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(false);
  });

  it('should reject when sessionId is missing', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['sessionId'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(false);
  });

  it('should reject when senderPeerId is missing', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['senderPeerId'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(false);
  });

  it('should reject when messageId is missing', () => {
    const msg = validMessage();
    delete (msg as Record<string, unknown>)['messageId'];
    const result = ProtocolMessageSchema.safeParse(msg);
    expect(result.success).toBe(false);
  });

  // ── Invalid types ──────────────────────────────────────────

  it('should reject non-integer protocolVersion', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ protocolVersion: 1.5 }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject non-positive protocolVersion', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ protocolVersion: 0 }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject negative protocolVersion', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ protocolVersion: -1 }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string messageType', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ messageType: '' }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string sessionId', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ sessionId: '' }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string senderPeerId', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ senderPeerId: '' }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string messageId', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ messageId: '' }),
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string logicalEventId', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ logicalEventId: '' }),
    );
    expect(result.success).toBe(false);
  });

  // ── Boundary: field length limits ──────────────────────────

  it('should reject messageType exceeding max length', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ messageType: 'X'.repeat(65) }),
    );
    expect(result.success).toBe(false);
  });

  it('should accept messageType at max length', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ messageType: 'X'.repeat(64) }),
    );
    expect(result.success).toBe(true);
  });

  it('should reject sessionId exceeding max length', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({ sessionId: 'S'.repeat(129) }),
    );
    expect(result.success).toBe(false);
  });

  it('should accept a message with valid Phase 7 distributed metadata', () => {
    const result = ProtocolMessageSchema.safeParse(
      validMessage({
        logicalEventId: 'op-456',
        originPeerId: 'peer-abc',
        membershipEpoch: 1,
        logicalTimestamp: 14,
        originSequenceNumber: 5,
        causalContext: { 'peer-abc': 4, 'peer-xyz': 2 },
      }),
    );
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.originPeerId).toBe('peer-abc');
      expect(result.data.membershipEpoch).toBe(1);
      expect(result.data.logicalTimestamp).toBe(14);
      expect(result.data.originSequenceNumber).toBe(5);
      expect(result.data.causalContext).toEqual({ 'peer-abc': 4, 'peer-xyz': 2 });
    }
  });

  it('should reject invalid Phase 7 metadata (negative timestamp or sequence)', () => {
    expect(
      ProtocolMessageSchema.safeParse(validMessage({ logicalTimestamp: -1 })).success,
    ).toBe(false);
    expect(
      ProtocolMessageSchema.safeParse(validMessage({ originSequenceNumber: 0 })).success,
    ).toBe(false);
    expect(
      ProtocolMessageSchema.safeParse(validMessage({ membershipEpoch: -1 })).success,
    ).toBe(false);
  });

  it('should reject non-object input', () => {
    expect(ProtocolMessageSchema.safeParse(null).success).toBe(false);
  });

  it('should reject undefined', () => {
    expect(ProtocolMessageSchema.safeParse(undefined).success).toBe(false);
  });

  it('should reject a string', () => {
    expect(ProtocolMessageSchema.safeParse('hello').success).toBe(false);
  });

  it('should reject a number', () => {
    expect(ProtocolMessageSchema.safeParse(42).success).toBe(false);
  });

  it('should reject an array', () => {
    expect(ProtocolMessageSchema.safeParse([1, 2, 3]).success).toBe(false);
  });

  it('should reject an empty object', () => {
    expect(ProtocolMessageSchema.safeParse({}).success).toBe(false);
  });

  describe('Option A ASCII identifier restrictions & worst-case envelope sizing', () => {
    it('should reject non-ASCII characters, whitespace, and JSON control characters in identifiers', () => {
      // sessionId
      expect(ProtocolMessageSchema.safeParse(validMessage({ sessionId: 'session with spaces' })).success).toBe(false);
      expect(ProtocolMessageSchema.safeParse(validMessage({ sessionId: 'session\nnewline' })).success).toBe(false);
      expect(ProtocolMessageSchema.safeParse(validMessage({ sessionId: 'session"quote' })).success).toBe(false);
      expect(ProtocolMessageSchema.safeParse(validMessage({ sessionId: 'session\\backslash' })).success).toBe(false);
      expect(ProtocolMessageSchema.safeParse(validMessage({ sessionId: 'session🔥unicode' })).success).toBe(false);

      // senderPeerId
      expect(ProtocolMessageSchema.safeParse(validMessage({ senderPeerId: 'peer space' })).success).toBe(false);
      expect(ProtocolMessageSchema.safeParse(validMessage({ senderPeerId: 'peer\tcontrol' })).success).toBe(false);

      // messageId
      expect(ProtocolMessageSchema.safeParse(validMessage({ messageId: 'msg/slash' })).success).toBe(false);

      // logicalEventId
      expect(ProtocolMessageSchema.safeParse(validMessage({ logicalEventId: 'event@symbol' })).success).toBe(false);

      // originPeerId
      expect(ProtocolMessageSchema.safeParse(validMessage({ originPeerId: 'peer#hash' })).success).toBe(false);

      // causalContext origin keys
      expect(
        ProtocolMessageSchema.safeParse(
          validMessage({ causalContext: { 'peer space': 1 } }),
        ).success,
      ).toBe(false);
    });

    it('should accept valid ASCII identifiers matching [a-zA-Z0-9_.:-]', () => {
      const validChars = 'abc-ABC_123.test:id';
      const result = ProtocolMessageSchema.safeParse(
        validMessage({
          sessionId: validChars,
          senderPeerId: validChars,
          messageId: validChars,
          logicalEventId: validChars,
          originPeerId: validChars,
          causalContext: { [validChars]: 5 },
        }),
      );
      expect(result.success).toBe(true);
    });

    it('should demonstrate that generic ProtocolMessage allows causalContext while bounded by MAX_VECTOR_ENTRIES', () => {
      // Build a generic control envelope with 3 causal peers
      const threePeerCausal: Record<string, number> = {};
      for (let i = 0; i < 3; i++) {
        threePeerCausal[`peer_${i}_` + 'k'.repeat(128 - `peer_${i}_`.length)] = Number.MAX_SAFE_INTEGER;
      }
      const genericMsg = {
        protocolVersion: 1,
        messageType: 'control:event',
        sessionId: 's'.repeat(128),
        senderPeerId: 'p'.repeat(128),
        messageId: 'm'.repeat(128),
        logicalEventId: 'e'.repeat(128),
        originPeerId: 'o'.repeat(128),
        membershipEpoch: Number.MAX_SAFE_INTEGER,
        connectionGeneration: Number.MAX_SAFE_INTEGER,
        logicalTimestamp: Number.MAX_SAFE_INTEGER,
        originSequenceNumber: Number.MAX_SAFE_INTEGER,
        causalContext: threePeerCausal,
        payload: '',
      };
      const parsed = ProtocolMessageSchema.safeParse(genericMsg);
      expect(parsed.success).toBe(true);

      const byteLength = Buffer.byteLength(JSON.stringify(genericMsg), 'utf-8');
      expect(byteLength).toBe(1403);
    });
  });
});

// ── Strict Snapshot-Chunk Envelope Contract Tests ────────────────────────────
describe('SnapshotChunkEnvelopeSchema — Strict Wire Contract & Bounded Envelope Overhead', () => {
  const sampleUuid = '12345678-1234-4234-8234-123456789abc';
  const sampleSha256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
  const default256BytesB64 = Buffer.alloc(256, 0x42).toString('base64');

  function createValidPayload(overrides: Record<string, unknown> = {}) {
    return {
      transferId: sampleUuid,
      sessionId: 's'.repeat(128),
      membershipEpoch: 1,
      supplierPeerId: 'p'.repeat(64),
      snapshotId: 'k'.repeat(128),
      chunkIndex: 0,
      totalChunks: 4,
      chunkSizeBytes: 256,
      totalSizeBytes: 1024,
      chunkSha256: sampleSha256,
      dataBase64: default256BytesB64,
      ...overrides,
    };
  }

  function validChunkEnvelope(overrides: Record<string, unknown> = {}) {
    return {
      protocolVersion: PROTOCOL_VERSION,
      messageType: SYNC_MESSAGE_TYPES.SNAPSHOT_CHUNK,
      sessionId: 's'.repeat(128),
      senderPeerId: 'p'.repeat(128),
      messageId: 'm'.repeat(128),
      membershipEpoch: 1,
      connectionGeneration: 1,
      payload: createValidPayload(),
      ...overrides,
    };
  }

  it('should accept valid snapshot-chunk envelope with maximum-length valid identifiers', () => {
    const msg = validChunkEnvelope();
    const result = SnapshotChunkEnvelopeSchema.safeParse(msg);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(result.data.sessionId).toBe('s'.repeat(128));
      expect(result.data.senderPeerId).toBe('p'.repeat(128));
      expect(result.data.messageId).toBe('m'.repeat(128));
      expect(result.data.membershipEpoch).toBe(1);
      expect(result.data.connectionGeneration).toBe(1);
    }
  });

  it('should prove maximum valid serialized chunk message framing overhead is strictly <= 1800 bytes', () => {
    // Construct absolute worst-case valid chunk message framing:
    // - exact supported messageType: 'sync:snapshot-chunk'
    // - maximum 128-character sessionId, senderPeerId, messageId
    // - maximum safe integer numeric fields (membershipEpoch, connectionGeneration)
    // - full chunk payload framing with max-length IDs, 64-char hex SHA-256, and empty dataBase64
    const worstCaseFraming = {
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
        chunkSizeBytes: 32768,
        totalSizeBytes: 4194304,
        chunkSha256: sampleSha256,
        dataBase64: '',
      },
    };

    // Measure exact framing serialization overhead via JSON.stringify and TextEncoder / Buffer.byteLength
    const serializedJson = JSON.stringify(worstCaseFraming);
    const textEncoderBytes = new TextEncoder().encode(serializedJson).byteLength;
    const bufferBytes = Buffer.byteLength(serializedJson, 'utf-8');
    const computeBytes = computeUtf8ByteLength(worstCaseFraming);

    // All UTF-8 measurement methods must agree
    expect(textEncoderBytes).toBe(bufferBytes);
    expect(textEncoderBytes).toBe(computeBytes);

    // Worst-case framing without dataBase64 is measured at exactly 1214 bytes
    expect(textEncoderBytes).toBeLessThanOrEqual(MAX_ENVELOPE_OVERHEAD_BYTES);
    expect(textEncoderBytes).toBe(1214);

    // Verified safety margin against 1,800 B bound:
    // 1800 - 1214 = 586 bytes headroom (48.3% safety margin)
    const safetyMargin = MAX_ENVELOPE_OVERHEAD_BYTES - textEncoderBytes;
    expect(safetyMargin).toBe(586);

    // Full 32 KB chunk message verification:
    // 43,692 Base64 bytes + 1,214 B framing = 44,906 bytes
    const fullChunkBytes = Buffer.alloc(32768, 0x41).toString('base64');
    expect(fullChunkBytes.length).toBe(43692);

    const fullChunkMessage = {
      ...worstCaseFraming,
      payload: {
        ...worstCaseFraming.payload,
        dataBase64: fullChunkBytes,
      },
    };
    const fullMessageBytes = computeUtf8ByteLength(fullChunkMessage);
    expect(fullMessageBytes).toBe(44906);
    expect(fullMessageBytes).toBeLessThanOrEqual(MIN_TRANSPORT_LIMIT_FULL_CHUNK);

    // Headroom against 45,492 threshold
    expect(MIN_TRANSPORT_LIMIT_FULL_CHUNK - fullMessageBytes).toBe(586);
  });

  // ── Negative Tests: Prohibited Fields ───────────────────────────────────────
  it('should strictly reject snapshot-chunk envelope containing causalContext', () => {
    const withCausal = validChunkEnvelope({
      causalContext: { 'peer-remote': 5 },
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withCausal);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toMatch(/must not contain causalContext/i);
    }
  });

  it('should strictly reject snapshot-chunk envelope containing logicalEventId', () => {
    const withLogicalEvent = validChunkEnvelope({
      logicalEventId: 'op-event-12345',
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withLogicalEvent);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toMatch(/must not contain logicalEventId/i);
    }
  });

  it('should strictly reject snapshot-chunk envelope containing originPeerId', () => {
    const withOriginPeer = validChunkEnvelope({
      originPeerId: 'peer-author-origin',
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withOriginPeer);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toMatch(/must not contain originPeerId/i);
    }
  });

  it('should strictly reject snapshot-chunk envelope containing logicalTimestamp', () => {
    const withLogicalTimestamp = validChunkEnvelope({
      logicalTimestamp: 100,
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withLogicalTimestamp);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toMatch(/must not contain logicalTimestamp/i);
    }
  });

  it('should strictly reject snapshot-chunk envelope containing originSequenceNumber', () => {
    const withOriginSeq = validChunkEnvelope({
      originSequenceNumber: 42,
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withOriginSeq);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toMatch(/must not contain originSequenceNumber/i);
    }
  });

  it('should strictly reject snapshot-chunk envelope with unrecognized extra fields (.strict())', () => {
    const withExtra = validChunkEnvelope({
      unrecognizedField: 'malicious-data',
    });
    const result = SnapshotChunkEnvelopeSchema.safeParse(withExtra);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.code).toBe('unrecognized_keys');
    }
  });

  // ── Correct Constants & Minimum Transport Capacity Arithmetic ───────────────
  it('should satisfy all transport and chunk capacity arithmetic invariants', () => {
    // 1. Minimum raw chunk is 256 bytes
    expect(MIN_RAW_CHUNK_BYTES).toBe(256);

    // 2. Base64 encoding of 256 bytes is Math.ceil(256 / 3) * 4 = 86 * 4 = 344 bytes
    const base64BytesFor256 = Math.ceil(MIN_RAW_CHUNK_BYTES / 3) * 4;
    expect(base64BytesFor256).toBe(344);
    expect(MIN_BASE64_CHUNK_BYTES).toBe(344);

    // 3. Guaranteed envelope bound is 1,800 bytes
    expect(MAX_ENVELOPE_OVERHEAD_BYTES).toBe(1800);

    // 4. Minimum transport limit is 1,800 + 344 = 2,144 bytes
    expect(MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER).toBe(MAX_ENVELOPE_OVERHEAD_BYTES + MIN_BASE64_CHUNK_BYTES);
    expect(MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER).toBe(2144);

    // 5. Full 32 KB chunk transport limit: 43,692 Base64 bytes + 1,800 envelope = 45,492 bytes
    const base64BytesFor32K = Math.ceil(32768 / 3) * 4;
    expect(base64BytesFor32K).toBe(43692);
    expect(MIN_TRANSPORT_LIMIT_FULL_CHUNK).toBe(MAX_ENVELOPE_OVERHEAD_BYTES + base64BytesFor32K);
    expect(MIN_TRANSPORT_LIMIT_FULL_CHUNK).toBe(45492);
  });

  it('should verify send-boundary exact string measurement and unchanged-string transmission', () => {
    const rawPayload = validChunkEnvelope({
      payload: createValidPayload({
        dataBase64: Buffer.alloc(256, 0x41).toString('base64'),
      }),
    });

    // Production serialization path
    const serializeResult = serializeAndCheckSize(rawPayload, MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER);
    expect(serializeResult.ok).toBe(true);

    if (serializeResult.ok) {
      const { json, byteLength } = serializeResult.value;

      // The exact string measured must equal the serialized JSON
      expect(typeof json).toBe('string');
      expect(byteLength).toBe(new TextEncoder().encode(json).byteLength);

      // Total message with 344 B Base64 is ~1,538 bytes, well within 2,144 B transport limit
      expect(byteLength).toBeLessThanOrEqual(MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER);

      // Verify that the exact string passes through unchanged and matches re-measurement
      const verifyLength = computeUtf8ByteLength(json);
      expect(verifyLength).toBe(byteLength);
    }
  });
});
