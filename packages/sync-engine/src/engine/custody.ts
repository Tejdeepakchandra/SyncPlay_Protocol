/**
 * In-process API custody token for registering peer digests.
 *
 * Implements Phase 7.5 M4.0c specification:
 * - Documents an in-process API custody boundary, not an OS or hardware security sandbox.
 * - Protects against inadvertent bypass of the transport identity verification pipeline
 *   by application consumers calling public facade methods.
 * - Internal transport adapters and test harnesses can import this token from this internal
 *   module path, but it is strictly NOT exported through public package barrels.
 */
export const INTERNAL_DIGEST_ACCESS_TOKEN: unique symbol = Symbol('INTERNAL_DIGEST_ACCESS_TOKEN');

export type InternalDigestAccessToken = typeof INTERNAL_DIGEST_ACCESS_TOKEN;
