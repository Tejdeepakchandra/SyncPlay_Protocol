/**
 * Result and error types for protocol validation.
 *
 * Uses a discriminated union (ok: true/false) instead of exceptions
 * for expected validation failures. Exceptions are reserved for
 * unexpected internal faults.
 */

/** Validation error codes. */
export type ValidationErrorCode =
  | 'INVALID_ENVELOPE'
  | 'UNSUPPORTED_VERSION'
  | 'UNKNOWN_MESSAGE_TYPE'
  | 'INVALID_PAYLOAD'
  | 'MESSAGE_TOO_LARGE'
  | 'PAYLOAD_TOO_LARGE'
  | 'INVALID_JSON'
  | 'MISSING_LOGICAL_EVENT_ID';

/** Structured validation error. */
export interface ValidationError {
  readonly code: ValidationErrorCode;
  readonly message: string;
  readonly path?: ReadonlyArray<string | number>;
}

/**
 * Discriminated union result type.
 *
 * @typeParam T - The success value type.
 * @typeParam E - The error type (defaults to ValidationError).
 */
export type Result<T, E = ValidationError> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: E };
