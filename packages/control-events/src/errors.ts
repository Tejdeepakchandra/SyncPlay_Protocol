/**
 * Control Events error types and error codes.
 *
 * Distinguishes user/caller errors (e.g. invalid arguments) from
 * transport or engine-level failures.
 */

export type CommandErrorCode =
  | 'INVALID_ARGUMENT'
  | 'RATE_LIMITED'
  | 'DISPOSED'
  | 'ENGINE_ERROR';

export interface CommandError {
  readonly code: CommandErrorCode;
  readonly message: string;
}

export type ControlErrorCode =
  | 'INVALID_ARGUMENT'
  | 'SESSION_MISMATCH'
  | 'SENDER_MISMATCH'
  | 'SELF_MESSAGE'
  | 'UNSUPPORTED_TYPE'
  | 'RATE_LIMITED'
  | 'DISPOSED'
  | 'ENGINE_ERROR'
  | 'VALIDATION_FAILED'
  | 'QUEUE_OVERFLOW';

export class ControlError extends Error {
  public readonly code: ControlErrorCode;

  constructor(code: ControlErrorCode, message: string) {
    super(message);
    this.name = 'ControlError';
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
