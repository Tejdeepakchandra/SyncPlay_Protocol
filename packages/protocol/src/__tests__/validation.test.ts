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
} from '../validation.js';
import { PROTOCOL_VERSION, MAX_MESSAGE_BYTES, MAX_PAYLOAD_BYTES } from '../constants.js';
import { MessageType } from '../message-types.js';

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
