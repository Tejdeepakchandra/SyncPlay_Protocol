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

export {
  SerializedSnapshotArtifact,
  SnapshotSerializer,
  StrictCompactedCheckpointItemSchema,
  StrictEngineSnapshotDataSchema,
  StrictStateSnapshotPayloadV2Schema,
  compareCheckpointsDeterministic,
} from './recovery/snapshot-serializer.js';
export type {
  SerializedSnapshotArtifactParams,
} from './recovery/snapshot-serializer.js';

export {
  SnapshotPreparationQueue,
} from './recovery/snapshot-preparation-queue.js';
export type {
  QueuedPreparationRequest,
} from './recovery/snapshot-preparation-queue.js';

export {
  SnapshotChunkProducer,
} from './recovery/snapshot-chunk-producer.js';
export type {
  SnapshotChunkSlice,
  PartitionedSnapshot,
  ChunkProducerConfig,
  ChunkProductionContext,
  ProducedChunkMessage,
} from './recovery/snapshot-chunk-producer.js';

export {
  SnapshotOutboundPipeline,
  createOutboundTransportCapability,
  createAuthenticatedRecipientContext,
} from './recovery/snapshot-outbound-pipeline.js';
export type {
  OutboundTransportCapability,
  AuthenticatedRecipientContext,
  InboundRecipientContextParams,
  OutboundOfferContext,
  OutboundChunkContext,
  OutboundChunkResult,
  OutboundTransferState,
  OutboundTransferSession,
  OutboundPipelineConfig,
  StartTransferParams,
} from './recovery/snapshot-outbound-pipeline.js';

export {
  SnapshotInboundPipeline,
  DEFAULT_SLIDING_WINDOW_SIZE,
  DEFAULT_CHUNK_TIMEOUT_MS,
  DEFAULT_OVERALL_TIMEOUT_MS,
  DEFAULT_MAX_RETRIES_PER_CHUNK,
  DEFAULT_MAX_TRANSFER_RETRIES,
  DEFAULT_MAX_TRANSFER_CONTEXT_TRANSITIONS,
  DEFAULT_MAX_LOCAL_REDISPATCH_ATTEMPTS,
  MAX_EVIDENCE_RECORDS,
} from './recovery/snapshot-inbound-pipeline.js';
export type {
  InboundChunkState,
  AttemptLifecycleState,
  AcceptedEvidenceStatus,
  AcceptedAttemptRecord,
  ChunkEvidenceHistory,
  OutboundMessageType,
  OutboundHandoffRequest,
  InboundTransferState,
  InboundTransferSession,
  SnapshotTransferResult,
  InboundPipelineConfig,
  SupplierContextLifecycleState,
} from './recovery/snapshot-inbound-pipeline.js';

export {
  CandidateOfferRegistry,
  MAX_OFFER_CANDIDATES,
  CANDIDATE_OFFER_TTL_MS,
} from './recovery/candidate-offer-registry.js';
export type {
  CandidateOfferEntry,
  CandidateMatchQuery,
  AddCandidateResult,
} from './recovery/candidate-offer-registry.js';

export {
  createInboundTransportCapability,
  assertInboundTransportCapability,
} from './recovery/inbound-custody.js';
export type {
  InboundTransportCapability,
} from './recovery/inbound-custody.js';

// ── Phase 7.5 M4.3a Recovery State & Generational Quarantine ─────
export {
  RecoveryStateMachine,
} from './recovery/recovery-state-machine.js';
export type {
  RecoveryState,
  RecoverySubstate,
  RecoveryStateListener,
  RecoverySubstateListener,
} from './recovery/recovery-state-machine.js';

export {
  QuarantineBuffer,
  MAX_QUARANTINE_OPERATIONS,
  MAX_QUARANTINE_TOTAL_BYTES,
  MAX_QUARANTINE_EVENT_BYTES,
  QUARANTINE_WARNING_THRESHOLD_BYTES,
} from './recovery/quarantine-buffer.js';
export type {
  QuarantinedOperation,
  QuarantineDrainResult,
  QuarantineBufferConfig,
} from './recovery/quarantine-buffer.js';

export {
  FutureEpochBuffer,
  MAX_FUTURE_EPOCH_OPERATIONS,
  MAX_FUTURE_EPOCH_BYTES,
  MAX_FUTURE_EPOCH_EVENT_BYTES,
  DEFAULT_EPOCH_CONVERGENCE_GRACE_MS,
} from './recovery/future-epoch-buffer.js';
export type {
  FutureEpochBufferConfig,
} from './recovery/future-epoch-buffer.js';

export {
  RecoveryAdmissionController,
} from './recovery/recovery-admission-controller.js';
export type {
  LocalAdmissionResult,
  RemoteAdmissionAction,
  RemoteAdmissionResult,
  RecoveryAdmissionControllerConfig,
} from './recovery/recovery-admission-controller.js';


