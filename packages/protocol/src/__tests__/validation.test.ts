/**
 * Tests for the validation module.
 *
 * Tests validateMessage, validatePayload, checkMessageSize,
 * and parseAndValidateMessage as integrated validation pipelines.
 */
import { describe, it, expect } from 'vitest';
import {
  validateMessage,
  validatePayload,
  checkMessageSize,
  checkPayloadSize,
  parseAndValidateMessage,
  computeUtf8ByteLength,
  serializeAndCheckSize,
} from '../validation.js';
import {
  PROTOCOL_VERSION,
  MAX_MESSAGE_BYTES,
  MAX_PAYLOAD_BYTES,
  MAX_BATCH_PAYLOAD_BYTES,
} from '../constants.js';
import { MessageType, SYNC_MESSAGE_TYPES } from '../message-types.js';
import {
  PAYLOAD_SCHEMAS,
  OperationBatchPayloadSchema,
  CompatibilityOperationBatchPayloadSchema,
} from '../payloads/index.js';

/** Helper: create a valid full message object. */
function fullMessage(
  type: string = MessageType.PLAY,
  payload: unknown = {},
  overrides: Record<string, unknown> = {},
) {
  return {
    protocolVersion: PROTOCOL_VERSION,
    messageType: type,
    sessionId: 'session-001',
    senderPeerId: 'peer-abc',
    messageId: 'msg-001',
    logicalEventId: 'op-001',
    payload,
    ...overrides,
  };
}

// ── validateMessage ──────────────────────────────────────────

describe('validateMessage', () => {
  it('should accept a valid PLAY message', () => {
    const result = validateMessage(
      fullMessage(MessageType.PLAY, { targetPositionMs: 5000 }),
    );
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.PLAY);
      expect(result.value.protocolVersion).toBe(PROTOCOL_VERSION);
    }
  });

  it('should accept a valid PAUSE message', () => {
    const result = validateMessage(
      fullMessage(MessageType.PAUSE, { observedPositionMs: 12000 }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid SEEK message', () => {
    const result = validateMessage(
      fullMessage(MessageType.SEEK, { targetPositionMs: 30000 }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid RATE_CHANGE message', () => {
    const result = validateMessage(
      fullMessage(MessageType.RATE_CHANGE, { playbackRate: 1.5 }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid PING message', () => {
    const result = validateMessage(
      fullMessage(MessageType.PING, {
        probeId: 'probe-1',
        senderTimestamp: 100.5,
      }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid PONG message', () => {
    const result = validateMessage(
      fullMessage(MessageType.PONG, { probeId: 'probe-1' }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid STATE_REQUEST message', () => {
    const result = validateMessage(
      fullMessage(MessageType.STATE_REQUEST, {
        requestId: 'req-001',
        reason: 'INITIAL_SYNC',
      }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid STATE_SNAPSHOT message', () => {
    const result = validateMessage(
      fullMessage(MessageType.STATE_SNAPSHOT, {
        snapshotId: 'snap-001',
        stateVersion: 0,
        state: { status: 'paused' },
      }),
    );
    expect(result.ok).toBe(true);
  });

  it('should accept a valid STATE_DIGEST message', () => {
    const result = validateMessage(
      fullMessage(MessageType.STATE_DIGEST, {
        digestId: 'digest-001',
        stateVersion: 2,
        digest: 'sha256:abc',
      }),
    );
    expect(result.ok).toBe(true);
  });

  // ── Version checks ────────────────────────────────────────

  it('should reject unsupported protocol version', () => {
    const result = validateMessage(
      fullMessage(MessageType.PLAY, {}, { protocolVersion: 99 }),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('UNSUPPORTED_VERSION');
    }
  });

  it('should reject protocol version 0', () => {
    const result = validateMessage(
      fullMessage(MessageType.PLAY, {}, { protocolVersion: 0 }),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      // version 0 is rejected by Zod (positive integer) → INVALID_ENVELOPE
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  // ── Message type checks ───────────────────────────────────

  it('should reject unknown message type', () => {
    const result = validateMessage(
      fullMessage('UNKNOWN_TYPE', {}),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('UNKNOWN_MESSAGE_TYPE');
    }
  });

  // ── Payload checks ────────────────────────────────────────

  it('should reject invalid payload for known type', () => {
    // SEEK requires targetPositionMs
    const result = validateMessage(
      fullMessage(MessageType.SEEK, {}),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });

  it('should reject RATE_CHANGE with zero rate', () => {
    const result = validateMessage(
      fullMessage(MessageType.RATE_CHANGE, { playbackRate: 0 }),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });

  it('should reject PING with missing probeId', () => {
    const result = validateMessage(
      fullMessage(MessageType.PING, { senderTimestamp: 100 }),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });

  // ── Envelope structure checks ─────────────────────────────

  it('should reject non-object input', () => {
    const result = validateMessage('not an object');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  it('should reject null input', () => {
    const result = validateMessage(null);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  it('should reject empty object', () => {
    const result = validateMessage({});
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  it('should reject array input', () => {
    const result = validateMessage([1, 2, 3]);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  // ── messageId vs logicalEventId distinction ────────────────

  it('should keep messageId and logicalEventId as distinct fields', () => {
    const result = validateMessage(
      fullMessage(MessageType.PLAY, {}, {
        messageId: 'msg-retry-2',
        logicalEventId: 'op-original',
      }),
    );
    expect(result.ok).toBe(true);
    if (result.ok) {
      // These are distinct: same operation, different transmission
      expect(result.value.messageId).toBe('msg-retry-2');
      expect(result.value.logicalEventId).toBe('op-original');
      expect(result.value.messageId).not.toBe(result.value.logicalEventId);
    }
  });

  it('should allow messageId and logicalEventId to have the same value', () => {
    // On first transmission, they may coincidentally be equal
    const result = validateMessage(
      fullMessage(MessageType.PLAY, {}, {
        messageId: 'same-id',
        logicalEventId: 'same-id',
      }),
    );
    expect(result.ok).toBe(true);
  });

  // ── Unknown fields stripped ────────────────────────────────

  it('should strip unknown envelope fields from validated output', () => {
    const input = fullMessage(MessageType.PLAY, {}, {
      createdAt: Date.now(),
      metadata: { custom: true },
    });
    const result = validateMessage(input);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect('createdAt' in result.value).toBe(false);
      expect('metadata' in result.value).toBe(false);
    }
  });

  // ── logicalEventId enforcement ─────────────────────────────

  it('should reject PLAY without logicalEventId', () => {
    const msg = fullMessage(MessageType.PLAY, {});
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MISSING_LOGICAL_EVENT_ID');
    }
  });

  it('should reject PAUSE without logicalEventId', () => {
    const msg = fullMessage(MessageType.PAUSE, {});
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MISSING_LOGICAL_EVENT_ID');
    }
  });

  it('should reject SEEK without logicalEventId', () => {
    const msg = fullMessage(MessageType.SEEK, { targetPositionMs: 5000 });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MISSING_LOGICAL_EVENT_ID');
    }
  });

  it('should reject RATE_CHANGE without logicalEventId', () => {
    const msg = fullMessage(MessageType.RATE_CHANGE, { playbackRate: 1.5 });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MISSING_LOGICAL_EVENT_ID');
    }
  });

  it('should accept PING without logicalEventId', () => {
    const msg = fullMessage(MessageType.PING, {
      probeId: 'probe-1',
      senderTimestamp: 100,
    });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should accept PONG without logicalEventId', () => {
    const msg = fullMessage(MessageType.PONG, { probeId: 'probe-1' });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should accept STATE_REQUEST without logicalEventId', () => {
    const msg = fullMessage(MessageType.STATE_REQUEST, {
      requestId: 'req-1',
      reason: 'INITIAL_SYNC',
    });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should accept STATE_SNAPSHOT without logicalEventId', () => {
    const msg = fullMessage(MessageType.STATE_SNAPSHOT, {
      snapshotId: 'snap-1',
      stateVersion: 0,
      state: null,
    });
    delete (msg as Record<string, unknown>)['logicalEventId'];
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  // ── Retry semantics ───────────────────────────────────────

  it('should accept retry with same logicalEventId but different messageId', () => {
    const original = validateMessage(
      fullMessage(MessageType.SEEK, { targetPositionMs: 5000 }, {
        messageId: 'msg-001',
        logicalEventId: 'op-seek-1',
      }),
    );
    const retry = validateMessage(
      fullMessage(MessageType.SEEK, { targetPositionMs: 5000 }, {
        messageId: 'msg-002',
        logicalEventId: 'op-seek-1',
      }),
    );

    expect(original.ok).toBe(true);
    expect(retry.ok).toBe(true);
    if (original.ok && retry.ok) {
      expect(original.value.logicalEventId).toBe(retry.value.logicalEventId);
      expect(original.value.messageId).not.toBe(retry.value.messageId);
    }
  });
});

// ── validatePayload ──────────────────────────────────────────

describe('validatePayload', () => {
  it('should validate PLAY payload independently', () => {
    const result = validatePayload(MessageType.PLAY, {
      targetPositionMs: 1000,
    });
    expect(result.ok).toBe(true);
  });

  it('should reject invalid SEEK payload independently', () => {
    const result = validatePayload(MessageType.SEEK, {});
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });

  it('should return error for unregistered message type', () => {
    const result = validatePayload('NONEXISTENT', {});
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('UNKNOWN_MESSAGE_TYPE');
    }
  });
});

// ── checkMessageSize ─────────────────────────────────────────

describe('checkMessageSize', () => {
  it('should accept a message within the size limit', () => {
    const small = JSON.stringify(
      fullMessage(MessageType.PLAY, { targetPositionMs: 5000 }),
    );
    const result = checkMessageSize(small);
    expect(result.ok).toBe(true);
  });

  it('should reject a message exceeding the default limit', () => {
    // Create a string larger than MAX_MESSAGE_BYTES
    const huge = 'X'.repeat(MAX_MESSAGE_BYTES + 1);
    const result = checkMessageSize(huge);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MESSAGE_TOO_LARGE');
    }
  });

  it('should accept a message at exactly the limit', () => {
    // Create a string of exactly MAX_MESSAGE_BYTES ASCII bytes
    const exact = 'A'.repeat(MAX_MESSAGE_BYTES);
    const result = checkMessageSize(exact);
    expect(result.ok).toBe(true);
  });

  it('should respect a custom maxBytes limit', () => {
    const msg = 'Hello, world!'; // 13 bytes
    expect(checkMessageSize(msg, 100).ok).toBe(true);
    expect(checkMessageSize(msg, 5).ok).toBe(false);
  });

  it('should handle multi-byte UTF-8 characters correctly', () => {
    // Each emoji is 4 bytes in UTF-8
    const emojis = '😀😀😀'; // 12 bytes
    const result = checkMessageSize(emojis, 11);
    expect(result.ok).toBe(false);

    const result2 = checkMessageSize(emojis, 12);
    expect(result2.ok).toBe(true);
  });

  it('should accept ArrayBuffer input', () => {
    const buffer = new TextEncoder().encode('test data').buffer;
    const result = checkMessageSize(buffer, 1000);
    expect(result.ok).toBe(true);
  });

  it('should reject oversized ArrayBuffer', () => {
    const buffer = new Uint8Array(100).buffer;
    const result = checkMessageSize(buffer, 50);
    expect(result.ok).toBe(false);
  });
});

// ── parseAndValidateMessage ──────────────────────────────────

describe('parseAndValidateMessage', () => {
  it('should parse and validate a valid JSON message', () => {
    const json = JSON.stringify(
      fullMessage(MessageType.PLAY, { targetPositionMs: 5000 }),
    );
    const result = parseAndValidateMessage(json);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.PLAY);
    }
  });

  it('should reject invalid JSON', () => {
    const result = parseAndValidateMessage('{invalid json}');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_JSON');
    }
  });

  it('should reject oversized JSON before parsing', () => {
    const huge = JSON.stringify({ data: 'X'.repeat(MAX_MESSAGE_BYTES) });
    const result = parseAndValidateMessage(huge);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MESSAGE_TOO_LARGE');
    }
  });

  it('should reject valid JSON that is not a valid message', () => {
    const result = parseAndValidateMessage(JSON.stringify({ foo: 'bar' }));
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_ENVELOPE');
    }
  });

  it('should reject valid envelope with wrong version', () => {
    const json = JSON.stringify(
      fullMessage(MessageType.PLAY, {}, { protocolVersion: 999 }),
    );
    const result = parseAndValidateMessage(json);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('UNSUPPORTED_VERSION');
    }
  });

  it('should respect custom maxBytes', () => {
    const json = JSON.stringify(
      fullMessage(MessageType.PLAY, { targetPositionMs: 0 }),
    );
    // Set an unreasonably small limit
    const result = parseAndValidateMessage(json, 10);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('MESSAGE_TOO_LARGE');
    }
  });
});

// ── checkPayloadSize ─────────────────────────────────────────

describe('checkPayloadSize', () => {
  it('should accept a small payload', () => {
    const result = checkPayloadSize({ targetPositionMs: 5000 });
    expect(result.ok).toBe(true);
  });

  it('should reject a payload exceeding MAX_PAYLOAD_BYTES', () => {
    // Create a payload larger than 32 KB but that would fit in 64 KB total
    const largePayload = { data: 'X'.repeat(MAX_PAYLOAD_BYTES + 1) };
    const result = checkPayloadSize(largePayload);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });

  it('should accept a payload at exactly MAX_PAYLOAD_BYTES', () => {
    // Dynamically compute JSON overhead so we don't miscount
    const overhead = new TextEncoder().encode(JSON.stringify({ d: '' })).byteLength;
    const exactPayload = { d: 'A'.repeat(MAX_PAYLOAD_BYTES - overhead) };
    const serialized = JSON.stringify(exactPayload);
    const byteLen = new TextEncoder().encode(serialized).byteLength;
    expect(byteLen).toBe(MAX_PAYLOAD_BYTES);
    const result = checkPayloadSize(exactPayload);
    expect(result.ok).toBe(true);
  });

  it('should reject a payload 1 byte over MAX_PAYLOAD_BYTES', () => {
    const overhead = new TextEncoder().encode(JSON.stringify({ d: '' })).byteLength;
    const overPayload = { d: 'A'.repeat(MAX_PAYLOAD_BYTES - overhead + 1) };
    const serialized = JSON.stringify(overPayload);
    const byteLen = new TextEncoder().encode(serialized).byteLength;
    expect(byteLen).toBe(MAX_PAYLOAD_BYTES + 1);
    const result = checkPayloadSize(overPayload);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });

  it('should respect a custom maxBytes limit', () => {
    const payload = { x: 'hello' };
    expect(checkPayloadSize(payload, 1000).ok).toBe(true);
    expect(checkPayloadSize(payload, 5).ok).toBe(false);
  });
});

// ── Payload size in validateMessage ──────────────────────────

describe('validateMessage payload size enforcement', () => {
  it('should reject oversized payload even when total message is under 64 KB', () => {
    // Payload > 32 KB but total message < 64 KB
    const bigPayloadData = 'X'.repeat(MAX_PAYLOAD_BYTES + 100);
    const msg = fullMessage(MessageType.STATE_SNAPSHOT, {
      snapshotId: 'snap-1',
      stateVersion: 1,
      state: { bulk: bigPayloadData },
    });

    // Verify total message is under 64 KB
    const totalBytes = new TextEncoder().encode(JSON.stringify(msg)).byteLength;
    expect(totalBytes).toBeLessThan(MAX_MESSAGE_BYTES);

    // But payload alone exceeds 32 KB
    const payloadBytes = new TextEncoder().encode(JSON.stringify(msg.payload)).byteLength;
    expect(payloadBytes).toBeGreaterThan(MAX_PAYLOAD_BYTES);

    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });

  it('should accept a payload at exactly MAX_PAYLOAD_BYTES via validateMessage', () => {
    // Build a STATE_SNAPSHOT whose payload serializes to exactly MAX_PAYLOAD_BYTES
    // {"snapshotId":"s","stateVersion":0,"state":"AAA..."}
    // Overhead: {"snapshotId":"s","stateVersion":0,"state":""} = 46 bytes
    const filler = 'A'.repeat(MAX_PAYLOAD_BYTES - 46);
    const msg = fullMessage(MessageType.STATE_SNAPSHOT, {
      snapshotId: 's',
      stateVersion: 0,
      state: filler,
    });

    const payloadBytes = new TextEncoder().encode(JSON.stringify(msg.payload)).byteLength;
    expect(payloadBytes).toBe(MAX_PAYLOAD_BYTES);

    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should reject oversized payload through parseAndValidateMessage', () => {
    const bigPayloadData = 'Y'.repeat(MAX_PAYLOAD_BYTES + 100);
    const msg = fullMessage(MessageType.STATE_SNAPSHOT, {
      snapshotId: 'snap-2',
      stateVersion: 2,
      state: { bulk: bigPayloadData },
    });
    const json = JSON.stringify(msg);

    // Total under 64 KB
    expect(new TextEncoder().encode(json).byteLength).toBeLessThan(MAX_MESSAGE_BYTES);

    const result = parseAndValidateMessage(json);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });

  it('should accept small payload in normal control message', () => {
    const msg = fullMessage(MessageType.PLAY, { targetPositionMs: 5000 });
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });
});

// ── Phase 7.4 Message Envelope & Payload Validation ────────────

describe('Phase 7.4 Message Envelope & Payload Validation', () => {
  const validUuidV4 = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';
  const sha256A = 'a'.repeat(64);

  const validV2DigestPayload = {
    formatVersion: 2 as const,
    sessionId: 'session-room-1',
    senderPeerId: 'peer-sender-1',
    connectionGeneration: 1,
    membershipEpoch: 1,
    stateVersion: 10,
    replicationVector: { 'peer-sender-1': 5, 'peer-remote-2': 3 },
    snapshotFrontier: { 'peer-sender-1': 0, 'peer-remote-2': 0 },
    historyFingerprints: { 'peer-sender-1': sha256A },
    sentAt: Date.now(),
  };

  const validRepairRequestPayload = {
    requestId: validUuidV4,
    sessionId: 'session-room-1',
    requesterPeerId: 'peer-req-1',
    targetOriginPeerId: 'peer-orig-1',
    fromSequence: 1,
    toSequence: 10,
    membershipEpoch: 1,
  };

  const validBatchPayload = {
    requestId: validUuidV4,
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    originPeerId: 'peer-orig-1',
    fromSequence: 1,
    toSequence: 2,
    membershipEpoch: 1,
    operations: [
      {
        logicalEventId: 'op-1',
        originPeerId: 'peer-orig-1',
        membershipEpoch: 1,
        messageType: 'PLAY' as const,
        logicalTimestamp: 1,
        originSequenceNumber: 1,
        causalContext: {},
        payload: { targetPositionMs: 1000 },
      },
      {
        logicalEventId: 'op-2',
        originPeerId: 'peer-orig-1',
        membershipEpoch: 1,
        messageType: 'PAUSE' as const,
        logicalTimestamp: 2,
        originSequenceNumber: 2,
        causalContext: { 'peer-orig-1': 1 },
        payload: { observedPositionMs: 1500 },
      },
    ],
  };

  const validRepairRejectedPayload = {
    requestId: validUuidV4,
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    targetOriginPeerId: 'peer-orig-1',
    fromSequence: 1,
    toSequence: 10,
    reason: 'OUTSIDE_HORIZON' as const,
    retainedHorizonSequence: 15,
  };

  it('should validate MessageType.STATE_DIGEST with v2 payload', () => {
    const msg = fullMessage(MessageType.STATE_DIGEST, validV2DigestPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.STATE_DIGEST);
    }
  });

  it('should validate SYNC_MESSAGE_TYPES.STATE_DIGEST wire format', () => {
    const msg = fullMessage(SYNC_MESSAGE_TYPES.STATE_DIGEST, validV2DigestPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should validate MessageType.REPAIR_REQUEST with payload', () => {
    const msg = fullMessage(MessageType.REPAIR_REQUEST, validRepairRequestPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.REPAIR_REQUEST);
    }
  });

  it('should validate SYNC_MESSAGE_TYPES.REPAIR_REQUEST wire format', () => {
    const msg = fullMessage(SYNC_MESSAGE_TYPES.REPAIR_REQUEST, validRepairRequestPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should validate MessageType.OPERATION_BATCH with payload', () => {
    const msg = fullMessage(MessageType.OPERATION_BATCH, validBatchPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.OPERATION_BATCH);
    }
  });

  it('should validate SYNC_MESSAGE_TYPES.OPERATION_BATCH wire format', () => {
    const msg = fullMessage(SYNC_MESSAGE_TYPES.OPERATION_BATCH, validBatchPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should validate MessageType.REPAIR_REJECTED with payload', () => {
    const msg = fullMessage(MessageType.REPAIR_REJECTED, validRepairRejectedPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.messageType).toBe(MessageType.REPAIR_REJECTED);
    }
  });

  it('should validate SYNC_MESSAGE_TYPES.REPAIR_REJECTED wire format', () => {
    const msg = fullMessage(SYNC_MESSAGE_TYPES.REPAIR_REJECTED, validRepairRejectedPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);
  });

  it('should round-trip parseAndValidateMessage for all Phase 7.4 messages', () => {
    const msgs = [
      fullMessage(MessageType.STATE_DIGEST, validV2DigestPayload),
      fullMessage(MessageType.REPAIR_REQUEST, validRepairRequestPayload),
      fullMessage(MessageType.OPERATION_BATCH, validBatchPayload),
      fullMessage(MessageType.REPAIR_REJECTED, validRepairRejectedPayload),
    ];

    for (const msg of msgs) {
      const serialized = JSON.stringify(msg);
      const parsed = parseAndValidateMessage(serialized);
      expect(parsed.ok).toBe(true);
      if (parsed.ok) {
        expect(parsed.value.messageType).toBe(msg.messageType);
      }
    }
  });

  it('should reject malformed payload with INVALID_PAYLOAD code', () => {
    const badRequest = {
      ...validRepairRequestPayload,
      fromSequence: 10,
      toSequence: 2, // invalid: inverted range
    };
    const msg = fullMessage(MessageType.REPAIR_REQUEST, badRequest);
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });

  it('documents that payload identity claims do not constitute transport authentication', () => {
    // Contract invariant: Protocol schemas validate message structure only.
    // The engine layer in M2/M3/M4/M5 MUST cross-check supplierPeerId and connectionGeneration
    // against the authenticated transport context registered via TransportRegistrationCapability.
    expect(validBatchPayload.supplierPeerId).toBe('peer-supplier-1');
  });
});

// ── Phase 7.4 Batch Payload Size & WebRTC Limits ───────────────

describe('Phase 7.4 Batch Payload Size & WebRTC Limits', () => {
  const validUuidV4 = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

  it('should permit OPERATION_BATCH payload between 32 KB and 64 KB', () => {
    // Generate an operation batch that is ~45 KB
    const filler = 'X'.repeat(4000);
    const ops = Array.from({ length: 10 }, (_, i) => ({
      logicalEventId: `op-${i + 1}`,
      originPeerId: 'peer-orig-1',
      membershipEpoch: 1,
      messageType: 'PLAY' as const,
      logicalTimestamp: i + 1,
      originSequenceNumber: i + 1,
      causalContext: {},
      payload: { targetPositionMs: i * 1000, padding: filler },
    }));

    const batchPayload = {
      requestId: validUuidV4,
      sessionId: 'session-room-1',
      supplierPeerId: 'peer-supplier-1',
      originPeerId: 'peer-orig-1',
      fromSequence: 1,
      toSequence: 10,
      membershipEpoch: 1,
      operations: ops,
    };

    const payloadByteLength = computeUtf8ByteLength(batchPayload);
    // Verify it exceeds 32 KB and is well within 64 KB
    expect(payloadByteLength).toBeGreaterThan(MAX_PAYLOAD_BYTES);
    expect(payloadByteLength).toBeLessThan(MAX_BATCH_PAYLOAD_BYTES);

    // Validate OPERATION_BATCH message
    const msg = fullMessage(MessageType.OPERATION_BATCH, batchPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(true);

    // Conversely, verify normal control message (STATE_SNAPSHOT) with > 32 KB payload is rejected
    const snapshotMsg = fullMessage(MessageType.STATE_SNAPSHOT, {
      snapshotId: 'snap-large',
      stateVersion: 1,
      state: 'X'.repeat(MAX_PAYLOAD_BYTES + 500),
    });
    const snapshotResult = validateMessage(snapshotMsg);
    expect(snapshotResult.ok).toBe(false);
    if (!snapshotResult.ok) {
      expect(snapshotResult.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });

  it('should reject OPERATION_BATCH payload exceeding MAX_BATCH_PAYLOAD_BYTES (65,536)', () => {
    const filler = 'Y'.repeat(7000);
    const ops = Array.from({ length: 12 }, (_, i) => ({
      logicalEventId: `op-${i + 1}`,
      originPeerId: 'peer-orig-1',
      membershipEpoch: 1,
      messageType: 'PLAY' as const,
      logicalTimestamp: i + 1,
      originSequenceNumber: i + 1,
      causalContext: {},
      payload: { targetPositionMs: i * 1000, padding: filler },
    }));

    const batchPayload = {
      requestId: validUuidV4,
      sessionId: 'session-room-1',
      supplierPeerId: 'peer-supplier-1',
      originPeerId: 'peer-orig-1',
      fromSequence: 1,
      toSequence: 12,
      membershipEpoch: 1,
      operations: ops,
    };

    const payloadByteLength = computeUtf8ByteLength(batchPayload);
    expect(payloadByteLength).toBeGreaterThan(MAX_BATCH_PAYLOAD_BYTES);

    const msg = fullMessage(MessageType.OPERATION_BATCH, batchPayload);
    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
    }
  });
});

// ── computeUtf8ByteLength & serializeAndCheckSize ──────────────

describe('computeUtf8ByteLength & serializeAndCheckSize', () => {
  it('should compute exact byte length for standard ASCII strings', () => {
    expect(computeUtf8ByteLength('hello world')).toBe(11);
    expect(computeUtf8ByteLength('')).toBe(0);
  });

  it('should accurately compute UTF-8 bytes for multibyte characters', () => {
    // 'café': 4 characters, 5 bytes (é is 2 bytes in UTF-8)
    expect(computeUtf8ByteLength('café')).toBe(5);

    // '🚀': 2 UTF-16 code units, 4 UTF-8 bytes
    expect(computeUtf8ByteLength('🚀')).toBe(4);

    // '日本語': 3 characters, 9 UTF-8 bytes (3 bytes each)
    expect(computeUtf8ByteLength('日本語')).toBe(9);
  });

  it('should accurately compute UTF-8 bytes for complex objects', () => {
    const obj = { text: 'Hello 🌍', numbers: [1, 2, 3] };
    const expected = new TextEncoder().encode(JSON.stringify(obj)).byteLength;
    expect(computeUtf8ByteLength(obj)).toBe(expected);
  });

  it('should succeed in serializeAndCheckSize when within limit', () => {
    const data = { valid: 'content', id: 42 };
    const result = serializeAndCheckSize(data, 1024);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.byteLength).toBe(new TextEncoder().encode(JSON.stringify(data)).byteLength);
      expect(result.value.json).toBe(JSON.stringify(data));
    }
  });

  it('should reject in serializeAndCheckSize when exceeding limit with accurate byte count', () => {
    const data = { large: 'Z'.repeat(100) };
    const expectedBytes = new TextEncoder().encode(JSON.stringify(data)).byteLength;
    const result = serializeAndCheckSize(data, 50);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('PAYLOAD_TOO_LARGE');
      expect(result.error.byteLength).toBe(expectedBytes);
      expect(result.error.maxBytes).toBe(50);
      expect(result.error.message).toContain(String(expectedBytes));
    }
  });

  it('should return SERIALIZATION_FAILED gracefully for circular references', () => {
    const circular: Record<string, unknown> = { a: 1 };
    circular.self = circular;
    const result = serializeAndCheckSize(circular, 1024);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('SERIALIZATION_FAILED');
      expect(result.error.message).toContain('Failed to serialize payload');
    }
  });

  it('should return SERIALIZATION_FAILED gracefully for BigInt values', () => {
    const hasBigInt = { val: BigInt(9007199254740991) };
    const result = serializeAndCheckSize(hasBigInt, 1024);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('SERIALIZATION_FAILED');
    }
  });
});

describe('Compatibility Schema Isolation & Inbound Wire Validation', () => {
  const validUuidV4 = 'f47ac10b-58cc-4372-a567-0e02b2c3d479';

  const validBatchPayload = {
    requestId: validUuidV4,
    sessionId: 'session-room-1',
    supplierPeerId: 'peer-supplier-1',
    originPeerId: 'peer-orig-1',
    fromSequence: 1,
    toSequence: 1,
    membershipEpoch: 1,
    operations: [
      {
        logicalEventId: 'op-1',
        originPeerId: 'peer-orig-1',
        membershipEpoch: 1,
        messageType: 'PLAY' as const,
        logicalTimestamp: 1,
        originSequenceNumber: 1,
        causalContext: {},
        payload: { targetPositionMs: 1000 },
      },
    ],
  };

  it('verifies PAYLOAD_SCHEMAS registers OperationBatchPayloadSchema, NOT CompatibilityOperationBatchPayloadSchema', () => {
    expect(PAYLOAD_SCHEMAS.get(MessageType.OPERATION_BATCH)).toBe(OperationBatchPayloadSchema);
    expect(PAYLOAD_SCHEMAS.get(SYNC_MESSAGE_TYPES.OPERATION_BATCH)).toBe(OperationBatchPayloadSchema);
    expect(PAYLOAD_SCHEMAS.get(MessageType.OPERATION_BATCH)).not.toBe(CompatibilityOperationBatchPayloadSchema);
  });

  it('rejects inbound OPERATION_BATCH wire message lacking requestId and does NOT fall back to compatibility schema', () => {
    const { requestId, ...batchWithoutRequestId } = validBatchPayload;
    const msg = fullMessage(MessageType.OPERATION_BATCH, batchWithoutRequestId);

    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
      expect(result.error.message).toContain('requestId');
    }
  });

  it('rejects inbound OPERATION_BATCH wire message lacking sessionId and does NOT fall back to compatibility schema', () => {
    const { sessionId, ...batchWithoutSessionId } = validBatchPayload;
    const msg = fullMessage(MessageType.OPERATION_BATCH, batchWithoutSessionId);

    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
      expect(result.error.message).toContain('sessionId');
    }
  });

  it('rejects inbound OPERATION_BATCH wire message lacking both requestId and sessionId', () => {
    const { requestId, sessionId, ...legacyBatchPayload } = validBatchPayload;
    const msg = fullMessage(MessageType.OPERATION_BATCH, legacyBatchPayload);

    const result = validateMessage(msg);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('INVALID_PAYLOAD');
    }
  });
});
