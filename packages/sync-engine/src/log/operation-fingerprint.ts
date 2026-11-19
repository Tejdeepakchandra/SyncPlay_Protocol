/**
 * Deterministic operation fingerprinting and canonical serialization.
 *
 * Implements Phase 7.3-R3 exact operation identity verification:
 * - RFC 8785 canonical JSON representation for immutable operation fields.
 * - Platform-independent synchronous SHA-256 (FIPS 180-4).
 * - Binds originPeerId, originSequenceNumber, logicalEventId, logicalTimestamp,
 *   causalContext, messageType, payload, and membershipEpoch into a 64-char hex digest.
 */

/**
 * Serializes any JavaScript value into a deterministic RFC 8785 canonical JSON string.
 *
 * Conforms strictly to RFC 8785 (JSON Canonicalization Scheme):
 * - Input values must be valid JSON data (RFC 8259).
 * - Rejects non-finite numbers (NaN, Infinity, -Infinity).
 * - Rejects undefined (at top-level, in objects, or in arrays).
 * - Rejects functions, symbols, BigInt, and cyclic object references.
 * - Rejects unsupported object instances (Map, Set, Date, RegExp, etc.).
 * - Canonicalizes -0 to 0.
 * - Serializes numbers using IEEE 754 canonical representation.
 * - Sorts object keys lexicographically by UTF-16 code units.
 * - Preserves deterministic nested object and array serialization.
 */
export function canonicalizeJson(value: unknown, seen: Set<object> = new Set()): string {
  if (value === undefined) {
    throw new TypeError('Cannot canonicalize invalid JSON value: undefined');
  }
  if (typeof value === 'function') {
    throw new TypeError('Cannot canonicalize unsupported type: function');
  }
  if (typeof value === 'symbol') {
    throw new TypeError('Cannot canonicalize unsupported type: symbol');
  }
  if (typeof value === 'bigint') {
    throw new TypeError('Cannot canonicalize unsupported type: bigint');
  }
  if (value === null) {
    return 'null';
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError(`Cannot canonicalize non-finite number: ${value}`);
    }
    if (Object.is(value, -0)) {
      return '0';
    }
    return JSON.stringify(value);
  }
  if (typeof value === 'string') {
    return JSON.stringify(value);
  }

  // Object or Array
  if (typeof value !== 'object') {
    throw new TypeError(`Cannot canonicalize unsupported type: ${typeof value}`);
  }

  if (seen.has(value)) {
    throw new TypeError('Cannot canonicalize cyclic object reference');
  }

  seen.add(value);
  try {
    if (Array.isArray(value)) {
      const items: string[] = [];
      for (let i = 0; i < value.length; i++) {
        const item = value[i];
        if (item === undefined) {
          throw new TypeError(`Cannot canonicalize invalid JSON array element at index ${i}: undefined`);
        }
        items.push(canonicalizeJson(item, seen));
      }
      return `[${items.join(',')}]`;
    }

    // Check plain object: prototype must be Object.prototype or null
    const proto = Object.getPrototypeOf(value);
    if (proto !== null && proto !== Object.prototype) {
      const typeName = (value as any).constructor?.name ?? 'CustomObject';
      throw new TypeError(`Cannot canonicalize unsupported object type: ${typeName}`);
    }

    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    const entries: string[] = [];

    for (const key of keys) {
      const val = obj[key];
      if (val === undefined) {
        throw new TypeError(`Cannot canonicalize invalid JSON property "${key}": undefined`);
      }
      entries.push(`${JSON.stringify(key)}:${canonicalizeJson(val, seen)}`);
    }

    return `{${entries.join(',')}}`;
  } finally {
    seen.delete(value);
  }
}

/**
 * Pure TypeScript synchronous SHA-256 (FIPS 180-4) implementation.
 * Deterministic across all environments (Node.js, Vite/browsers, Web Workers).
 */
export function sha256(input: string | Uint8Array): string {
  function rightRotate(value: number, amount: number): number {
    return (value >>> amount) | (value << (32 - amount));
  }

  const K: number[] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ];

  const H: number[] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ];

  const utf8 = typeof input === 'string' ? new TextEncoder().encode(input) : input;
  const bitLength = utf8.length * 8;
  const wordCount = (((bitLength + 64) >>> 9) << 4) + 16;
  const words = new Int32Array(wordCount);

  for (let i = 0; i < utf8.length; i++) {
    const wordIdx = i >>> 2;
    words[wordIdx] = (words[wordIdx]! | (utf8[i]! << (24 - (i % 4) * 8))) | 0;
  }

  const bitWordIdx = bitLength >>> 5;
  words[bitWordIdx] = (words[bitWordIdx]! | (0x80 << (24 - (bitLength % 32)))) | 0;
  words[wordCount - 1] = bitLength;

  const W = new Int32Array(64);

  for (let i = 0; i < words.length; i += 16) {
    let [a, b, c, d, e, f, g, h] = H as [
      number,
      number,
      number,
      number,
      number,
      number,
      number,
      number,
    ];

    for (let j = 0; j < 64; j++) {
      if (j < 16) {
        W[j] = words[i + j]! | 0;
      } else {
        const gamma0 = rightRotate(W[j - 15]!, 7) ^ rightRotate(W[j - 15]!, 18) ^ (W[j - 15]! >>> 3);
        const gamma1 = rightRotate(W[j - 2]!, 17) ^ rightRotate(W[j - 2]!, 19) ^ (W[j - 2]! >>> 10);
        W[j] = (W[j - 16]! + gamma0 + W[j - 7]! + gamma1) | 0;
      }

      const ch = (e & f) ^ (~e & g);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const sigma0 = rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22);
      const sigma1 = rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25);
      const t1 = (h + sigma1 + ch + K[j]! + W[j]!) | 0;
      const t2 = (sigma0 + maj) | 0;

      h = g;
      g = f;
      f = e;
      e = (d + t1) | 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) | 0;
    }

    H[0] = (H[0]! + a) | 0;
    H[1] = (H[1]! + b) | 0;
    H[2] = (H[2]! + c) | 0;
    H[3] = (H[3]! + d) | 0;
    H[4] = (H[4]! + e) | 0;
    H[5] = (H[5]! + f) | 0;
    H[6] = (H[6]! + g) | 0;
    H[7] = (H[7]! + h) | 0;
  }

  let hex = '';
  for (let i = 0; i < 8; i++) {
    hex += (H[i]! >>> 0).toString(16).padStart(8, '0');
  }
  return hex;
}

export interface OperationFingerprintInput {
  readonly originPeerId: string;
  readonly originSequenceNumber: number;
  readonly logicalEventId: string;
  readonly logicalTimestamp: number;
  readonly causalContext: Record<string, number>;
  readonly messageType: string;
  readonly payload: unknown;
  readonly membershipEpoch?: number;
}

/**
 * Computes a deterministic 64-char hex SHA-256 fingerprint covering all immutable
 * fields of an operation.
 */
export function computeOperationFingerprint(op: OperationFingerprintInput): string {
  const preimage = {
    originPeerId: op.originPeerId,
    originSequenceNumber: op.originSequenceNumber,
    logicalEventId: op.logicalEventId,
    logicalTimestamp: op.logicalTimestamp,
    causalContext: op.causalContext,
    messageType: op.messageType,
    payload: op.payload,
    membershipEpoch: op.membershipEpoch ?? 1,
  };
  const canonicalJson = canonicalizeJson(preimage);
  return sha256(canonicalJson);
}
