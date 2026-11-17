/**
 * Replication and Anti-Entropy Subsystem Facade.
 *
 * Implements Phase 7.4 Milestone M2:
 * - Deterministic vector and fingerprint comparison (ReplicationComparator)
 * - Anti-Entropy scheduling and rate limiting (AntiEntropyManager)
 * - Structured comparison contracts and interfaces
 */

export * from './types.js';
export { compareDigests } from './replication-comparator.js';
export {
  AntiEntropyManager,
  defaultSchedulerClock,
  DEFAULT_PERIODIC_INTERVAL_MS,
  DEFAULT_JITTER_MS,
  DEFAULT_RATE_LIMIT_MIN_INTERVAL_MS,
  DEFAULT_RATE_LIMIT_BURST,
} from './anti-entropy-manager.js';
export {
  RepairRequestPipeline,
  generateUUIDv4,
  DEFAULT_REPAIR_REQUEST_TIMEOUT_MS,
  MAX_REPAIR_RETRIES,
  DEFAULT_BACKOFF_BASE_MS,
  MAX_CONCURRENT_REQUESTS_PER_PEER,
  MAX_CONCURRENT_REQUESTS_TOTAL,
  MAX_OPERATIONS_PER_REPAIR_BATCH,
} from './repair-request-pipeline.js';
