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
