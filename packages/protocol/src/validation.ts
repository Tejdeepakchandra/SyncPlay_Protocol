/**
 * Protocol message validation.
 *
 * Provides runtime validation at network boundaries.
 * Every inbound message (from DataChannel, Socket.IO, or HTTP)
 * must pass through these validators before dispatch.
 *
 * Validation layers:
 * 1. Size check (serialized bytes)
 * 2. JSON parse
 * 3. Envelope structure (Zod)
 * 4. Protocol version check
 * 5. Message type check
 * 6. logicalEventId required for control events
 * 7. Payload schema validation
 * 8. Payload size check (independent of total message size)
 *
 * Returns typed Result<T> — no exceptions for expected invalid input.
 */
import type { z } from 'zod';
import { ProtocolMessageSchema } from './envelope.js';
import type { ProtocolMessage } from './envelope.js';
import { PROTOCOL_VERSION, MAX_MESSAGE_BYTES, MAX_PAYLOAD_BYTES } from './constants.js';
import { KNOWN_MESSAGE_TYPES, REQUIRES_LOGICAL_EVENT_ID } from './message-types.js';
import { PAYLOAD_SCHEMAS } from './payloads/index.js';
import type { Result, ValidationError, ValidationErrorCode } from './result.js';

// ── Helpers ──────────────────────────────────────────────────

function fail(
  code: ValidationErrorCode,
  message: string,
  path?: ReadonlyArray<string | number>,
): Result<never> {
  return { ok: false, error: { code, message, path } };
}

function formatZodError(zodError: z.ZodError): string {
  return zodError.issues
    .map((issue) => {
      const path = issue.path.length > 0 ? `${issue.path.join('.')}: ` : '';
      return `${path}${issue.message}`;
    })
    .join('; ');
}

function zodErrorPath(zodError: z.ZodError): ReadonlyArray<string | number> {
  const firstIssue = zodError.issues[0];
  if (!firstIssue) return [];
  // Zod v4 paths may include symbols; filter to string|number only
  return firstIssue.path.filter(
    (segment): segment is string | number =>
      typeof segment === 'string' || typeof segment === 'number',
  );
}

// ── Size checks ──────────────────────────────────────────────

/**
 * Check whether a serialized message is within the size limit.
 *
 * Call this BEFORE parsing JSON to reject oversized payloads early.
 *
 * @param serialized - The raw serialized message (string or ArrayBuffer).
 * @param maxBytes - Override the default MAX_MESSAGE_BYTES limit.
 */
export function checkMessageSize(
  serialized: string | ArrayBuffer,
  maxBytes: number = MAX_MESSAGE_BYTES,
): Result<void> {
  const byteLength =
    typeof serialized === 'string'
      ? new TextEncoder().encode(serialized).byteLength
      : serialized.byteLength;

  if (byteLength > maxBytes) {
    return fail(
      'MESSAGE_TOO_LARGE',
      `Message is ${byteLength} bytes, exceeding the ${maxBytes} byte limit`,
    );
  }

  return { ok: true, value: undefined };
}

/**
 * Check whether a payload is within the payload size limit.
 *
 * Called AFTER payload schema validation on the serialized payload.
 * This is independent of the total message size check.
 *
 * @param payload - The validated payload value.
 * @param maxBytes - Override the default MAX_PAYLOAD_BYTES limit.
 */
export function checkPayloadSize(
  payload: unknown,
  maxBytes: number = MAX_PAYLOAD_BYTES,
): Result<void> {
  const serialized = JSON.stringify(payload);
  const byteLength = new TextEncoder().encode(serialized).byteLength;

  if (byteLength > maxBytes) {
    return fail(
      'PAYLOAD_TOO_LARGE',
      `Payload is ${byteLength} bytes, exceeding the ${maxBytes} byte payload limit`,
      ['payload'],
    );
  }

  return { ok: true, value: undefined };
}

// ── Payload validation ───────────────────────────────────────

/**
 * Validate a payload against the schema registered for a message type.
 *
 * @param messageType - The message type string.
 * @param payload - The raw payload value.
 * @returns The validated payload on success, or a validation error.
 */
export function validatePayload(
  messageType: string,
  payload: unknown,
): Result<unknown> {
  const schema = PAYLOAD_SCHEMAS.get(messageType);
  if (!schema) {
    // No schema registered — this should not happen if messageType
    // was already checked against KNOWN_MESSAGE_TYPES
    return fail(
      'UNKNOWN_MESSAGE_TYPE',
      `No payload schema registered for message type: ${messageType}`,
    );
  }

  const result = schema.safeParse(payload);
  if (!result.success) {
    return fail(
      'INVALID_PAYLOAD',
      `Invalid ${messageType} payload: ${formatZodError(result.error)}`,
      zodErrorPath(result.error),
    );
  }

  return { ok: true, value: result.data };
}

// ── Full message validation ──────────────────────────────────

/**
 * Validate a parsed message object.
 *
 * Checks:
 * 1. Envelope structure (Zod schema)
 * 2. Protocol version (must equal PROTOCOL_VERSION)
 * 3. Message type (must be in KNOWN_MESSAGE_TYPES)
 * 4. logicalEventId required for control events (PLAY, PAUSE, SEEK, RATE_CHANGE)
 * 5. Payload schema validation (type-specific Zod schema)
 * 6. Payload size check (independent of total message size)
 *
 * @param raw - The parsed (but unvalidated) message object.
 * @returns The validated ProtocolMessage on success.
 */
export function validateMessage(raw: unknown): Result<ProtocolMessage> {
  // 1. Validate envelope structure
  const envelopeResult = ProtocolMessageSchema.safeParse(raw);
  if (!envelopeResult.success) {
    return fail(
      'INVALID_ENVELOPE',
      `Invalid message envelope: ${formatZodError(envelopeResult.error)}`,
      zodErrorPath(envelopeResult.error),
    );
  }

  const message = envelopeResult.data;

  // 2. Check protocol version
  if (message.protocolVersion !== PROTOCOL_VERSION) {
    return fail(
      'UNSUPPORTED_VERSION',
      `Unsupported protocol version ${message.protocolVersion}, expected ${PROTOCOL_VERSION}`,
      ['protocolVersion'],
    );
  }

  // 3. Check message type
  if (!KNOWN_MESSAGE_TYPES.has(message.messageType)) {
    return fail(
      'UNKNOWN_MESSAGE_TYPE',
      `Unknown message type: ${message.messageType}`,
      ['messageType'],
    );
  }

  // 4. Require logicalEventId for control events
  if (
    REQUIRES_LOGICAL_EVENT_ID.has(message.messageType) &&
    (message.logicalEventId == null || message.logicalEventId === '')
  ) {
    return fail(
      'MISSING_LOGICAL_EVENT_ID',
      `Message type ${message.messageType} requires logicalEventId for deduplication`,
      ['logicalEventId'],
    );
  }

  // 5. Validate payload against type-specific schema
  const payloadResult = validatePayload(message.messageType, message.payload);
  if (!payloadResult.ok) {
    return payloadResult as Result<never>;
  }

  // 6. Check payload size independently
  const payloadSizeResult = checkPayloadSize(payloadResult.value);
  if (!payloadSizeResult.ok) {
    return payloadSizeResult as Result<never>;
  }

  // Return with the validated payload
  const validated: ProtocolMessage = {
    ...message,
    payload: payloadResult.value,
  };

  return { ok: true, value: validated };
}

// ── Combined parse + validate ────────────────────────────────

/**
 * Parse a raw JSON string and validate it as a protocol message.
 *
 * Combines size check, JSON parsing, and full message validation
 * (including independent payload size check).
 * Use this at network boundaries (DataChannel onmessage, etc.).
 *
 * @param raw - The raw JSON string received from the network.
 * @param maxBytes - Override the default MAX_MESSAGE_BYTES limit.
 * @returns The validated ProtocolMessage on success.
 */
export function parseAndValidateMessage(
  raw: string,
  maxBytes: number = MAX_MESSAGE_BYTES,
): Result<ProtocolMessage> {
  // 1. Check total message size
  const sizeResult = checkMessageSize(raw, maxBytes);
  if (!sizeResult.ok) {
    return sizeResult as Result<never>;
  }

  // 2. Parse JSON
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    return fail('INVALID_JSON', 'Failed to parse message as JSON');
  }

  // 3. Validate (includes payload size check)
  return validateMessage(parsed);
}
