/**
 * @module @syncplay/sync-engine
 *
 * Baseline SyncEngine package for SyncPlay.
 * Maintains locally authoritative playback state, calculates continuous position,
 * enforces listener exception isolation, and provides atomic snapshot exchange.
 */

// ── Types ────────────────────────────────────────────────────
export type {
  PlaybackStatus,
  LastAppliedOperation,
  PlaybackState,
  EngineDecisionCode,
  EngineDecision,
  EngineLifecycleState,
  EngineStatus,
  EngineStats,
  Clock,
  SyncEngineConfig,
  StateChangeListener,
  DecisionListener,
  Unsubscribe,
  SyncEngine,
} from './types.js';

export { DEFAULT_SYNC_ENGINE_CONFIG } from './types.js';

// ── Errors ───────────────────────────────────────────────────
export { SyncEngineError } from './errors.js';
export type { SyncEngineErrorCode } from './errors.js';

// ── Configuration ────────────────────────────────────────────
export { validateSyncEngineConfig } from './config.js';
export type { ResolvedSyncEngineConfig } from './config.js';

// ── Reducer & Calculations ───────────────────────────────────
export { calculateExtrapolatedPosition, reduceState } from './reducer.js';
export type { ReducerResult } from './reducer.js';

// ── Snapshots ────────────────────────────────────────────────
export {
  EngineSnapshotDataSchema,
  exportEngineSnapshot,
  validateSnapshotPayload,
  computeSnapshotDigestV2,
  exportEngineSnapshotV2,
  validateSnapshotPayloadV2,
} from './snapshot.js';
export type {
  EngineSnapshotData,
  ValidatedSnapshotResult,
  ValidatedSnapshotV2Result,
} from './snapshot.js';

export {
  captureMembershipView,
  calculateMembershipSafeFrontier,
  calculateLocalPressureFrontier,
  executeCompaction,
  CompactionManager,
} from './compaction/compaction-manager.js';
export type {
  PeerConnectionDescriptor,
  CapturedMembershipView,
  CompactionHoldReason,
  CompactionAbortReason,
  CompactionFrontierCandidate,
  CompactionExecutionResult,
  CompactionExecutionContext,
} from './compaction/compaction-manager.js';

// ── Implementation ───────────────────────────────────────────
export { BaselineSyncEngine } from './baseline-sync-engine.js';
export {
  LeaderlessSyncEngine,
  createEpochTransitionCapability,
  createTransportRegistrationCapability,
  createSupplierContextRegistrationCapability,
  createAuthenticatedSupplierContext,
  createAuthenticatedRequesterContext,
  TrustedTransportAdapter,
} from './engine/leaderless-sync-engine.js';
export type {
  LeaderlessSyncEngineConfig,
  EpochTransitionCapability,
  TransportRegistrationCapability,
  SupplierContextRegistrationCapability,
  AuthenticatedSupplierContext,
  AuthenticatedRequesterContext,
} from './engine/leaderless-sync-engine.js';

// ── Phase 7 Distributed Ordering & Conflict Components ───────
export { LogicalClockManager } from './clock/logical-clock-manager.js';
export type { ILogicalClockManager, ClockValidationContext } from './clock/types.js';

export { OrderedOperationLog } from './log/ordered-operation-log.js';
export { PendingDependencyQueue } from './log/pending-dependency-queue.js';
export {
  canonicalizeJson,
  sha256,
  computeOperationFingerprint,
} from './log/operation-fingerprint.js';
export type { OperationFingerprintInput } from './log/operation-fingerprint.js';
export type {
  StoredOperation,
  InsertOperationCode,
  InsertOperationResult,
  IOrderedOperationLog,
} from './log/types.js';

export { ConflictResolver, happensBefore, areConcurrent } from './conflict/conflict-resolver.js';
export type { IConflictResolver, ReducerContext } from './conflict/types.js';

export { compareOperations } from './ordering/comparator.js';
export type { OperationOrderingKey } from './ordering/comparator.js';

// ── Phase 7.4 State Replication & Anti-Entropy Components ──────
export * from './replication/index.js';

// ── Phase 7.5 Snapshot Recovery Components ─────────────────────
export { ChunkAssembler } from './recovery/chunk-assembler.js';
export type {
  ChunkAssemblerConfig,
  InitTransferParams,
  ChunkEnvelopeContext,
  IngestChunkResult,
  AssembledSnapshot,
  FinalizeResult,
  TransferProgress,
  FailoverCandidateParams,
  FailoverRejectReason,
  PrepareForFailoverResult,
} from './recovery/chunk-assembler.js';

