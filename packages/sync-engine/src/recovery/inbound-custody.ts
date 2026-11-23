/**
 * @module @syncplay/sync-engine/recovery
 *
 * Capability branding and custody module for Phase 7.5 M4.2d Snapshot Inbound Pipeline.
 *
 * Trust Model & Custody Semantics:
 * Under the cooperative in-process application trust model:
 * 1. The factory createInboundTransportCapability() is intentionally exposed in the public package
 *    API so that the trusted application composition root (which initializes WebRTC DataChannels,
 *    peer mesh, and sync engine) can mint transport capabilities during bootstrap.
 * 2. INBOUND_CAPABILITY_BRAND is a unique symbol kept package-private (never re-exported through
 *    package entry points), preventing accidental duck-typing or object literal fabrication.
 * 3. In a shared JavaScript realm, this provides structural defensiveness against configuration errors
 *    and unvalidated inputs, not cryptographic or security isolation against malicious in-realm code.
 * 4. The `authority` parameter is an informational provenance label (validated as a non-empty string),
 *    not an independently authenticated cryptographic principal or registry secret.
 * 5. InboundTransportCapability is strictly required by SnapshotInboundPipeline to register or revoke
 *    supplier contexts, and is enforced via strict referential equality on disconnect notifications.
 */

import { SyncEngineError } from '../errors.js';

export const INBOUND_CAPABILITY_BRAND: unique symbol = Symbol('INBOUND_CAPABILITY_BRAND');
export type InboundCapabilityBrand = typeof INBOUND_CAPABILITY_BRAND;

export interface InboundTransportCapability {
  readonly [INBOUND_CAPABILITY_BRAND]: true;
  readonly sessionId: string;
  readonly authority: string;
  readonly token?: string;
}

/**
 * Creates an authorized InboundTransportCapability for trusted transport adapter initialization.
 * Package-private / internal factory.
 */
export function createInboundTransportCapability(
  sessionId: string,
  authority = 'trusted-transport',
  token?: string,
): InboundTransportCapability {
  if (!sessionId || typeof sessionId !== 'string' || sessionId.trim().length === 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'sessionId must be a non-empty string');
  }
  if (!authority || typeof authority !== 'string' || authority.trim().length === 0) {
    throw new SyncEngineError('INVALID_ARGUMENT', 'authority must be a non-empty string');
  }

  const cap: InboundTransportCapability = Object.freeze({
    [INBOUND_CAPABILITY_BRAND]: true as const,
    sessionId,
    authority,
    token:
      token ??
      `inbound-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
  });
  return cap;
}

/**
 * Validates that the provided capability is a genuine, unforgeable InboundTransportCapability
 * branded with the internal INBOUND_CAPABILITY_BRAND symbol.
 *
 * Throws SyncEngineError('UNAUTHORIZED') if the capability is missing, invalid, or forged.
 */
export function assertInboundTransportCapability(
  capability: unknown,
  expectedSessionId?: string,
): asserts capability is InboundTransportCapability {
  if (
    !capability ||
    typeof capability !== 'object' ||
    (capability as any)[INBOUND_CAPABILITY_BRAND] !== true
  ) {
    throw new SyncEngineError(
      'UNAUTHORIZED',
      'Operation requires an authorized InboundTransportCapability branded with INBOUND_CAPABILITY_BRAND',
    );
  }

  const typedCap = capability as InboundTransportCapability;
  if (!typedCap.sessionId || typeof typedCap.sessionId !== 'string') {
    throw new SyncEngineError(
      'UNAUTHORIZED',
      'InboundTransportCapability contains invalid sessionId',
    );
  }

  if (expectedSessionId !== undefined && typedCap.sessionId !== expectedSessionId) {
    throw new SyncEngineError(
      'UNAUTHORIZED',
      `InboundTransportCapability sessionId '${typedCap.sessionId}' does not match expected sessionId '${expectedSessionId}'`,
    );
  }
}
