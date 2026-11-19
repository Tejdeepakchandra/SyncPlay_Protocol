/**
 * Wire protocol payloads for Phase 7.5 state recovery snapshot transfer.
 * Single source of truth for snapshot transfer wire schemas.
 */
import { z } from 'zod';
import {
  ASCII_IDENTIFIER_REGEX,
  MAX_SNAPSHOT_BYTES,
  MAX_CHUNK_BYTES,
  MAX_CHUNKS_PER_SNAPSHOT,
  MAX_CONCURRENT_CHUNK_REQUESTS,
} from '../constants.js';
import { UUIDv4Schema } from './repair.js';

// ── Abort Reason Taxonomy ──────────────────────────────────────────────

export const SnapshotAbortReasonSchema = z.enum([
  'TRANSFER_TIMEOUT',
  'SUPPLIER_DISCONNECTED',
  'EPOCH_MISMATCH',
  'CHUNK_CORRUPTED',
  'HASH_MISMATCH',
  'QUARANTINE_OVERFLOW',
  'SUPPLIER_REJECTED',
  'LOCAL_ABORT',
  'TRANSPORT_LIMIT_EXCEEDED',
  'CHUNK_COUNT_EXCEEDED',
  'OFFER_INCONSISTENT',
  'STALE_SNAPSHOT',
  'CHECKPOINT_DIVERGENCE',
  'IDENTITY_MISMATCH',
  'RETRY_BUDGET_EXHAUSTED',
]);

export type SnapshotAbortReason = z.infer<typeof SnapshotAbortReasonSchema>;

// ── SNAPSHOT_OFFER Payload ─────────────────────────────────────────────

export const SnapshotOfferPayloadSchema = z
  .object({
    offerId: UUIDv4Schema,
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    supplierPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    authorPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotFrontier: z.record(
      z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
      z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    ),
    maxLogicalTimestamp: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    totalSizeBytes: z.number().int().positive().max(MAX_SNAPSHOT_BYTES),
    totalChunks: z.number().int().positive().max(MAX_CHUNKS_PER_SNAPSHOT),
    sha256Digest: z.string().regex(/^[0-9a-f]{64}$/i, {
      message: 'Must be canonical 64-character hex SHA-256 digest',
    }),
  })
  .strict();

export type SnapshotOfferPayload = z.infer<typeof SnapshotOfferPayloadSchema>;

// ── SNAPSHOT_REQUEST Payload ───────────────────────────────────────────

export const SnapshotRequestPayloadSchema = z
  .object({
    transferId: UUIDv4Schema,
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    requesterPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    supplierPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    requestedChunkWindow: z
      .number()
      .int()
      .positive()
      .max(MAX_CONCURRENT_CHUNK_REQUESTS)
      .default(4),
    clientMaxChunkBytes: z.number().int().positive().max(MAX_CHUNK_BYTES).optional(),
    missingChunkIndices: z
      .array(z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1))
      .max(MAX_CHUNKS_PER_SNAPSHOT)
      .optional(),
  })
  .strict();

export type SnapshotRequestPayload = z.infer<typeof SnapshotRequestPayloadSchema>;

// ── SNAPSHOT_CHUNK Payload ─────────────────────────────────────────────

export const SnapshotChunkPayloadSchema = z
  .object({
    transferId: UUIDv4Schema,
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    supplierPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    chunkIndex: z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1),
    totalChunks: z.number().int().positive().max(MAX_CHUNKS_PER_SNAPSHOT),
    chunkSizeBytes: z.number().int().positive().max(MAX_CHUNK_BYTES),
    totalSizeBytes: z.number().int().positive().max(MAX_SNAPSHOT_BYTES),
    chunkSha256: z.string().regex(/^[0-9a-f]{64}$/i, {
      message: 'Must be 64-character hex SHA-256 of raw chunk binary payload',
    }),
    dataBase64: z.string().min(1),
  })
  .strict()
  .superRefine((data, ctx) => {
    // 1. Chunk index strictly less than totalChunks
    if (data.chunkIndex >= data.totalChunks) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `chunkIndex (${data.chunkIndex}) must be strictly less than totalChunks (${data.totalChunks})`,
        path: ['chunkIndex'],
      });
    }

    // 2. Base64 length must equal canonical Base64 length for chunkSizeBytes
    const expectedBase64Len = Math.ceil(data.chunkSizeBytes / 3) * 4;
    if (data.dataBase64.length !== expectedBase64Len) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `dataBase64 length (${data.dataBase64.length}) does not match expected canonical Base64 length (${expectedBase64Len}) for chunkSizeBytes (${data.chunkSizeBytes})`,
        path: ['dataBase64'],
      });
      return;
    }

    // 3. Base64 syntax check
    if (!/^[A-Za-z0-9+/]+={0,2}$/.test(data.dataBase64)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'dataBase64 contains non-Base64 characters or invalid padding syntax',
        path: ['dataBase64'],
      });
      return;
    }

    // 4. Decoded byte length and canonical roundtrip check
    let decoded: Buffer;
    try {
      decoded = Buffer.from(data.dataBase64, 'base64');
    } catch (e) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Failed to decode dataBase64: ${e instanceof Error ? e.message : String(e)}`,
        path: ['dataBase64'],
      });
      return;
    }

    if (decoded.byteLength !== data.chunkSizeBytes) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Decoded byte length (${decoded.byteLength}) does not match declared chunkSizeBytes (${data.chunkSizeBytes})`,
        path: ['dataBase64'],
      });
    }

    if (decoded.toString('base64') !== data.dataBase64) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'dataBase64 is non-canonical (invalid padding or non-zero unused trailing bits)',
        path: ['dataBase64'],
      });
    }

    // 5. Partitioning consistency rules
    if (data.totalChunks === 1) {
      if (data.chunkIndex !== 0) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `Single-chunk transfer must have chunkIndex 0, received ${data.chunkIndex}`,
          path: ['chunkIndex'],
        });
      }
      if (data.chunkSizeBytes !== data.totalSizeBytes) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `Single-chunk transfer chunkSizeBytes (${data.chunkSizeBytes}) must equal totalSizeBytes (${data.totalSizeBytes})`,
          path: ['chunkSizeBytes'],
        });
      }
    } else {
      // Multi-chunk transfer: totalSizeBytes must exceed non-final chunk sizes
      if (data.totalSizeBytes <= data.chunkSizeBytes && data.chunkIndex < data.totalChunks - 1) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `totalSizeBytes (${data.totalSizeBytes}) must exceed chunkSizeBytes (${data.chunkSizeBytes}) for multi-chunk transfer`,
          path: ['totalSizeBytes'],
        });
      }
      // Any chunk size cannot exceed declared total snapshot size
      if (data.chunkSizeBytes > data.totalSizeBytes) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `chunkSizeBytes (${data.chunkSizeBytes}) cannot exceed totalSizeBytes (${data.totalSizeBytes})`,
          path: ['chunkSizeBytes'],
        });
      }
      // For chunk 0: nominal capacity multiplied by totalChunks must cover totalSizeBytes,
      // and totalChunks - 1 nominal chunks must not already exceed totalSizeBytes
      if (data.chunkIndex === 0) {
        if (data.chunkSizeBytes * data.totalChunks < data.totalSizeBytes) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Declared totalChunks (${data.totalChunks}) with nominal chunk size (${data.chunkSizeBytes}) cannot cover totalSizeBytes (${data.totalSizeBytes})`,
            path: ['totalChunks'],
          });
        }
        if (data.chunkSizeBytes * (data.totalChunks - 1) >= data.totalSizeBytes) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Declared totalChunks (${data.totalChunks}) is excessive for totalSizeBytes (${data.totalSizeBytes}) with chunk size (${data.chunkSizeBytes})`,
            path: ['totalChunks'],
          });
        }
      }
    }
  });

export type SnapshotChunkPayload = z.infer<typeof SnapshotChunkPayloadSchema>;

// ── SNAPSHOT_CHUNK_REQUEST Payload ─────────────────────────────────────

export const SnapshotChunkRequestPayloadSchema = z
  .object({
    transferId: UUIDv4Schema,
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    requesterPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    supplierPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    missingChunkIndices: z
      .array(z.number().int().nonnegative().max(MAX_CHUNKS_PER_SNAPSHOT - 1))
      .min(1)
      .max(MAX_CONCURRENT_CHUNK_REQUESTS),
  })
  .strict();

export type SnapshotChunkRequestPayload = z.infer<typeof SnapshotChunkRequestPayloadSchema>;

// ── SNAPSHOT_ABORT Payload ─────────────────────────────────────────────

export const SnapshotAbortPayloadSchema = z
  .object({
    transferId: UUIDv4Schema,
    sessionId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    membershipEpoch: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
    senderPeerId: z.string().min(1).max(64).regex(ASCII_IDENTIFIER_REGEX),
    snapshotId: z.string().min(1).max(128).regex(ASCII_IDENTIFIER_REGEX),
    reason: SnapshotAbortReasonSchema,
    detail: z.string().max(256).regex(/^[\x20-\x7E]*$/).optional(),
  })
  .strict();

export type SnapshotAbortPayload = z.infer<typeof SnapshotAbortPayloadSchema>;
