/**
 * Typed errors for SyncEngine operations.
 */

export type SyncEngineErrorCode =
  | 'DISPOSED'
  | 'INVALID_ARGUMENT'
  | 'INVALID_PAYLOAD'
  | 'CLOCK_ERROR'
  | 'SNAPSHOT_ERROR'
  | 'CLOCK_OVERFLOW'
  | 'CLOCK_REGRESSION'
  | 'INVALID_CLOCK'
  | 'INVALID_CAUSAL_TIMESTAMP'
  | 'INVALID_LOGICAL_TIMESTAMP'
  | 'REJECTED_CLOCK_INFLATION'
  | 'REJECTED_UNAUTHORIZED_ORIGIN'
  | 'SEQUENCE_REGRESSION'
  | 'SEQUENCE_COLLISION_DETECTED'
  | 'INVALID_ORIGIN_SEQUENCE'
  | 'REJECTED_OBSOLETE_EPOCH'
  | 'REJECTED_OUTSIDE_HORIZON'
  | 'REPAIR_GAP_EXCEEDED'
  | 'SNAPSHOT_STALE'
  | 'DIGEST_MISMATCH'
  | 'DEGRADED_OUT_OF_SYNC'
  | 'INVALID_VERSION_VECTOR'
  | 'BATCH_OVERFLOW';

export class SyncEngineError extends Error {
  public readonly code: SyncEngineErrorCode;

  constructor(code: SyncEngineErrorCode, message: string) {
    super(message);
    this.name = 'SyncEngineError';
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
