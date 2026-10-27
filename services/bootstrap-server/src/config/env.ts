/**
 * Validated server configuration.
 *
 * All provisional defaults are from BOOTSTRAP_SERVER_LLD §12.
 * Every value is configurable via environment variables and
 * validated with Zod at startup.
 */
import { z } from 'zod';

/** Configuration schema with documented defaults. */
const ConfigSchema = z.object({
  /** HTTP server port. */
  PORT: z.coerce.number().int().positive().default(3001),

  /** Hostname to bind to. */
  HOST: z.string().min(1).default('0.0.0.0'),

  /** Default maximum peers per room (BOOTSTRAP_SERVER_LLD §12). */
  DEFAULT_MAX_PEERS: z.coerce.number().int().min(2).max(20).default(4),

  /** Absolute maximum peers per room (hard cap). */
  ABSOLUTE_MAX_PEERS: z.coerce.number().int().min(2).max(50).default(10),

  /** Room time-to-live in milliseconds (default: 2 hours). */
  ROOM_TTL_MS: z.coerce.number().int().positive().default(2 * 60 * 60 * 1000),

  /** Grace period for empty rooms in milliseconds (default: 60s). */
  EMPTY_ROOM_GRACE_MS: z.coerce.number().int().positive().default(60_000),

  /** Grace period for disconnected peers in milliseconds (default: 30s). */
  DISCONNECTED_GRACE_MS: z.coerce.number().int().positive().default(30_000),

  /** Maximum signaling payload size in bytes. */
  MAX_SIGNALING_BYTES: z.coerce.number().int().positive().default(65_536),

  /** Comma-separated list of allowed CORS origins. */
  ALLOWED_ORIGINS: z.string().default('http://localhost:3000,http://localhost:5173'),

  /** Node environment. */
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
});

export type ServerConfig = z.infer<typeof ConfigSchema>;

/**
 * Load and validate server configuration from environment variables.
 *
 * @throws {z.ZodError} if environment variables fail validation.
 */
export function loadConfig(env: Record<string, string | undefined> = process.env): ServerConfig {
  return ConfigSchema.parse(env);
}
