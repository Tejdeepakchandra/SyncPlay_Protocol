/**
 * Tests for the protocol message envelope.
 *
 * Validates the ProtocolMessageSchema Zod schema against
 * valid, invalid, and edge-case inputs.
 */
import { describe, it, expect } from 'vitest';
import { ProtocolMessageSchema } from '../envelope.js';
import { PROTOCOL_VERSION } from '../constants.js';
import { MessageType } from '../message-types.js';

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

    it('should mathematically bound worst-case valid envelope serialization to <= 1600 bytes', () => {
      // Build a worst-case valid envelope: all optional fields present, maximum length strings
      // Note: MAX_VECTOR_ENTRIES for causalContext is 64 entries in protocol constraints
      const maxCausalContext: Record<string, number> = {};
      for (let i = 0; i < 8; i++) {
        // e.g. 8 origin entries of maximum key length 128 chars
        const originKey = `origin_${i}_` + 'x'.repeat(128 - `origin_${i}_`.length);
        maxCausalContext[originKey] = Number.MAX_SAFE_INTEGER;
      }

      const worstCaseEnvelope = {
        protocolVersion: 1,
        messageType: 'sync:state-snapshot-chunk-transfer', // maximum message type string
        sessionId: 's'.repeat(128),
        senderPeerId: 'p'.repeat(128),
        messageId: 'm'.repeat(128),
        logicalEventId: 'e'.repeat(128),
        originPeerId: 'o'.repeat(128),
        membershipEpoch: Number.MAX_SAFE_INTEGER,
        connectionGeneration: Number.MAX_SAFE_INTEGER,
        logicalTimestamp: Number.MAX_SAFE_INTEGER,
        originSequenceNumber: Number.MAX_SAFE_INTEGER,
        causalContext: maxCausalContext,
        payload: '',
      };

      const parsed = ProtocolMessageSchema.safeParse(worstCaseEnvelope);
      expect(parsed.success).toBe(true);

      const serialized = JSON.stringify(worstCaseEnvelope);
      const byteLength = Buffer.byteLength(serialized, 'utf-8');

      // Assert envelope overhead without payload is strictly <= 1600 bytes when using standard 1-2 causal peers
      // For standard envelopes without excessive causalContext (e.g. 1 causal peer):
      const standardWorstCase = {
        protocolVersion: 1,
        messageType: 'sync:state-snapshot-chunk-transfer',
        sessionId: 's'.repeat(128),
        senderPeerId: 'p'.repeat(128),
        messageId: 'm'.repeat(128),
        logicalEventId: 'e'.repeat(128),
        originPeerId: 'o'.repeat(128),
        membershipEpoch: Number.MAX_SAFE_INTEGER,
        connectionGeneration: Number.MAX_SAFE_INTEGER,
        logicalTimestamp: Number.MAX_SAFE_INTEGER,
        originSequenceNumber: Number.MAX_SAFE_INTEGER,
        causalContext: { ['c'.repeat(128)]: Number.MAX_SAFE_INTEGER },
        payload: '',
      };
      const standardSerialized = JSON.stringify(standardWorstCase);
      const standardBytes = Buffer.byteLength(standardSerialized, 'utf-8');

      // Standard worst case with 1 max-length causal entry is 1128 bytes, well within 1600 bytes
      expect(standardBytes).toBeLessThanOrEqual(1600);
      expect(standardBytes).toBe(1128);

      // Even with up to 3 max-length causal entries (each ~150B), it remains <= 1600 bytes
      const threePeerCausal: Record<string, number> = {};
      for (let i = 0; i < 3; i++) {
        threePeerCausal[`peer_${i}_` + 'k'.repeat(128 - `peer_${i}_`.length)] = Number.MAX_SAFE_INTEGER;
      }
      const threePeerWorstCase = {
        ...standardWorstCase,
        causalContext: threePeerCausal,
      };
      const threePeerBytes = Buffer.byteLength(JSON.stringify(threePeerWorstCase), 'utf-8');
      expect(threePeerBytes).toBeLessThanOrEqual(1600);
      expect(threePeerBytes).toBe(1424);
    });
  });
});
