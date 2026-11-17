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
});
