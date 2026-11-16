/**
 * Typed errors for SyncEngine operations.
 */

export type SyncEngineErrorCode =
  | 'DISPOSED'
  | 'INVALID_ARGUMENT'
  | 'INVALID_PAYLOAD'
  | 'CLOCK_ERROR'
  | 'SNAPSHOT_ERROR';

export class SyncEngineError extends Error {
  public readonly code: SyncEngineErrorCode;

  constructor(code: SyncEngineErrorCode, message: string) {
    super(message);
    this.name = 'SyncEngineError';
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
