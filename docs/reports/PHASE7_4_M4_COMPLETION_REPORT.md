# Phase 7.4 Milestone M4 Completion Report: Supplier Range Servicing & Safety Audit

**Milestone:** Phase 7.4 — Milestone M4 (Supplier Range Servicing & Final Safety Audit)  
**Status:** AUDITED, VERIFIED, & ARCHITECTURALLY ACCEPTED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Milestone M1 Baseline:** `309b808` (amending `5cadc03`)  
**Milestone M2 Baseline:** `2d5e9f3` (docs: `4926c52`)  
**Milestone M3 Baseline:** `c143972` (docs: `a42c0e7`)  
**Initial M4 Implementation Baseline:** `5001199` (docs: `a40852e`)  
**Audited M4 Implementation Baseline:** `dd94004`  
**Branch:** `main`  
**Working Tree Status:** Clean  
**Date:** 2026-11-19  

---

## 1. Executive Summary

Milestone M4 implements and authoritatively verifies the **Supplier-Side Lifecycle** of the Phase 7.4 anti-entropy repair protocol for the SyncPlay Protocol.

While Milestone M3 implemented requester-side repair tracking, supplier selection, retries, and admission, Milestone M4 enables a peer to authoritatively and safely service incoming `REPAIR_REQUEST` messages from its retained operation log without ever fabricating, inferring, or reconstructing missing history, without mutating local playback state, and without exposing the peer to resource exhaustion or response amplification attacks.

Following the initial implementation, a comprehensive safety and transport-boundary audit was conducted covering:
1. **Duplicate response caching, replay idempotence, and amplification suppression.**
2. **Capability custody and requester identity verification.**
3. **Actual transport message-size accounting, envelope overhead margins, and RFC 8841 SCTP semantics.**
4. **Synchronous servicing safety, concurrency tracking resilience, pre-delivery currency checks, and epoch transition guarantees.**
5. **Rejection semantics, permanent vs temporary failure handling, and contiguous range verification.**

---

## 2. Comprehensive Audit Findings & Implemented Protections

### Audit 1: Duplicate Response Cache and Replay Amplification
- **Conflicting Reused Request ID Detection:**  
  Previously, the cache indexed responses solely by `${requesterPeerId}:${requestId}`. If an adversarial or buggy client reused a `requestId` with *different* request parameters (different sequence range, origin, or session), it could receive an unrelated cached response.  
  *Fix Implemented:* `CachedResponse` now records `targetOriginPeerId`, `fromSequence`, `toSequence`, and `sessionId`. On cache hit, if any request parameter differs from the cached entry, `RepairSupplierService` throws `SyncEngineError('INVALID_ARGUMENT', `Reused requestId '${request.requestId}' with conflicting request parameters`)`.
- **Duplicate Retransmission vs. Amplification Suppression:**  
  Legitimate network drop re-requests must be re-sent, but unchecked retransmissions could permit an outbound amplification attack where an attacker floods duplicate requests to exhaust supplier bandwidth.  
  *Fix Implemented:* On cache hit, `consumeRateLimitToken()` is called. If the peer is within its rate limit, the cached response is retransmitted via `onSendRepairResponse` and returned. If the peer exceeds its rate limit, the supplier rejects with `REPAIR_REJECTED` (`reason: 'RATE_LIMITED'`), strictly capping outbound bandwidth to the configured rate limit.
- **Cache Eviction and Lifecycle Conditions:**  
  The response cache is bounded to 64 entries by default with FIFO eviction when full. Furthermore:
  - Disconnecting a peer (`onPeerDisconnected(peerId)`) evicts all cached responses for that peer.
  - Epoch transition (`onEpochTransition(newEpoch)`) instantly wipes the entire cache.
  - Disposal (`dispose()`) clears the cache and resets all state.
- **Cache Insertion Timing:**  
  The response is inserted into `responseCache` *before* invoking `onSendRepairResponse`. If the transport send fails, the response remains cached so a subsequent retry from the requester can be served idempotently without re-querying the log.

### Audit 2: Capability Custody and Requester Identity
1. **Where is the trusted capability stored?**  
   Stored in a private instance field `authorizedTransportCapability` on `LeaderlessSyncEngine`. It is never exposed via public getter or property.
2. **How does the transport adapter get access to it?**  
   Passed once during engine initialization/transport binding (`registerTransport`) in the trusted composition root.
3. **Can an untrusted caller instantiate a valid capability?**  
   **No.** `createTransportRegistrationCapability()` produces a new distinct object reference. Validation in `registerInboundRequesterContext` uses strict reference identity (`===`). External objects matching the TypeScript interface fail reference equality and throw `SyncEngineError('UNAUTHORIZED')`.
4. **Can an untrusted caller construct or forge an `AuthenticatedRequesterContext`?**  
   **No.** Valid contexts are tracked in a private `WeakSet<AuthenticatedRequesterContext>` on `LeaderlessSyncEngine`. An untrusted caller cannot add an object to this private `WeakSet`.
5. **How is the requester's connection generation verified?**  
   `RepairSupplierService` queries `engine.getPeerGeneration(context.requesterPeerId)`. If `context.connectionGeneration` is missing or does not match `activeGen`, it throws `SyncEngineError('UNAUTHORIZED')`. Furthermore, `TrustedTransportAdapter.handleInboundRepairRequest` populates requester identity and connection generation directly from trusted WebRTC connection state, never from untrusted payload claims.

### Audit 3: Actual Transport Message-Size Accounting and Envelope Overhead
- **Transport Framing & Envelope Overhead:**  
  Wire messages are transmitted as framed JSON envelopes (`MessageEnvelope`), adding roughly 100–250 bytes of JSON framing over the payload.  
  *Fix Implemented:* Added `envelopeOverheadBytes` option (default: `DEFAULT_SUPPLIER_ENVELOPE_OVERHEAD_BYTES = 256` bytes). The effective payload ceiling is computed as:
  $$\text{effectiveTransportLimit} = \min(\text{maxPayloadBytes}, \text{negotiatedSctpSize} > 0 ? \text{negotiatedSctpSize} : \text{maxPayloadBytes})$$
  $$\text{maxBytes} = \max(1, \text{effectiveTransportLimit} - \text{envelopeOverhead})$$
- **Negotiated SCTP Limit Semantics (RFC 8841 / W3C WebRTC §6.2):**  
  - `undefined`: falls back to the application cap of $65,536$ bytes, reserving envelope overhead ($65,280$ bytes max payload).
  - `0`: RFC 8841 defines 0 as "remote endpoint can handle messages of any size" (unconstrained). This is clamped to the local application cap of $65,536$ bytes, reserving envelope overhead.
  - Finite value $< 65,536$ (e.g. 2,000 bytes): clamped to 2,000 bytes, reserving envelope overhead ($1,744$ bytes max payload).
  - Finite value $> 65,536$ (e.g. 100,000 bytes): clamped to local application cap of $65,536$ bytes, reserving envelope overhead.
- **Exact Boundary Enforcement & Batch Slicing:**  
  Unit tests verify that a single operation whose serialized batch length equals `maxBytes` is admitted into an `OperationBatchPayload`, while an operation with serialized batch length of `maxBytes + 1` is immediately rejected with `REPAIR_REJECTED` (`reason: 'BUSY'`). A multi-operation batch exceeding `maxBytes` is sliced down to $[fromSequence, fromSequence + M - 1]$ to fit within the transport limit, ensuring that `payload + envelopeOverhead <= negotiatedSctpSize`.

### Audit 4: Epoch Transition, Pre-Delivery Checks, and Synchronous Servicing Safety
- **Servicing Synchrony:**  
  `RepairSupplierService.serveRepairRequest()` is strictly synchronous. It contains zero asynchronous ticks, zero promises, and zero deferred callbacks. Log lookups, rate limiting, batch slicing, and response generation all happen within a single execution turn.
- **Pre-Delivery Currency Verification:**  
  Immediately prior to inserting into `responseCache` and calling `onSendRepairResponse`, `serveRepairRequest()` re-validates:
  1. `request.membershipEpoch === this.engine.epoch`: if the epoch changed, rejects immediately with `EPOCH_MISMATCH`.
  2. `context.connectionGeneration === this.engine.getPeerGeneration(context.requesterPeerId)`: if the peer reconnected/transitioned generation, throws `SyncEngineError('UNAUTHORIZED')`.
- **Proof of No Old-Epoch Delivery:**  
  Because execution is synchronous and pre-delivery checks guard caching and dispatch:
  - An epoch transition cannot occur undetected during servicing.
  - If a transition occurred prior to the call, Gate 4 rejects with `EPOCH_MISMATCH`.
  - If a transition occurs after the call, all previous cached responses have already been wiped by `onEpochTransition()`.
  - Re-entrant callbacks that trigger an epoch transition safely wipe the cache and do not leave obsolete responses in the cache.
- **Concurrency Tracking Exception Safety:**  
  `activeConcurrentRequests` and `activeRequestsPerPeer` are managed with a strict `try ... finally` block. Even if an unhandled error occurs during history querying or serialization, active request counters cleanly decrement in the `finally` block, using `Math.max(0, ...)` to ensure counters never desynchronize or become negative.

### Audit 5: Rejection Semantics and Retry Behavior
- **Temporary Unavailability (Requester Backs Off / Retries / Fails Over):**
  - `BUSY`: Supplier concurrency is full, or log lacks a contiguous range (hole in log, or sequence beyond current frontier). Requester backs off or fails over to another eligible supplier.
  - `RATE_LIMITED`: Supplier token bucket exhausted. Requester backs off.
- **Permanent History Gaps (Requester Terminates Retries / Hands Off to Snapshot Recovery):**
  - `OUTSIDE_HORIZON`: Sequence is compacted into base snapshot ($fromSeq \le \text{snapshotFrontier}$) or predates active log horizon ($fromSeq < \text{oldestRetained}$). Includes `retainedHorizonSequence = oldestRetained ?? (baseSnapshotFrontier + 1)`. Requester updates supplier knowledge and marks history exhausted if no other peers retain it, triggering Phase 7.5 snapshot recovery handoff.
  - `ORIGIN_UNKNOWN`: Supplier has never observed the target origin. Requester marks supplier ineligible for that origin.
  - `EPOCH_MISMATCH`: Request or supplier belongs to a different epoch. Requester cancels the request.
- **Contiguity Guarantee:**  
  The supplier iterates every sequence $s \in [fromSequence, toSequence]$. If any sequence is missing, it returns `OUTSIDE_HORIZON` (if $\le baseSeq$) or `BUSY` (if missing in active log). It **never** returns a partial batch that skips a missing sequence, and **never** returns an empty batch (`operations: []`).

---

## 3. Files Added and Modified

### Added:
1. `packages/sync-engine/src/replication/repair-supplier-service.ts`:
   - Full M4 supplier service with request validation, rate limiting, concurrency bounds, duplicate response cache, retained log queries, batch slicing, envelope overhead reserve, pre-delivery currency checks, and rejection creation.
2. `packages/sync-engine/src/__tests__/repair-fulfillment.test.ts`:
   - 56 unit and adversarial tests covering validation, history verification, sizing, lifecycle, and all audit conditions.

### Modified:
1. `packages/sync-engine/src/replication/types.ts`:
   - Added `envelopeOverheadBytes?: number` to `RepairSupplierOptions`.
2. `packages/sync-engine/src/replication/index.ts`:
   - Exported `DEFAULT_SUPPLIER_ENVELOPE_OVERHEAD_BYTES = 256`.
3. `packages/sync-engine/src/replication/repair-supplier-service.ts`:
   - Added conflicting request parameter detection on cache hits.
   - Added rate limit token consumption on cache hits.
   - Added retransmission via `onSendRepairResponse` on cache hits.
   - Added `envelopeOverheadBytes` accounting and RFC 8841 SCTP limit parsing.
   - Added pre-delivery epoch and generation currency checks.
4. `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
   - Integrated `AuthenticatedRequesterContext`, `registerInboundRequesterContext`, and `serveRepairRequest`.
5. `packages/sync-engine/src/log/ordered-operation-log.ts` & `log/types.ts`:
   - Added `getOldestRetainedSequence()`.
6. `packages/sync-engine/src/replication/anti-entropy-manager.ts`:
   - Integrated `RepairSupplierService`.

---

## 4. Verification & Test Reconciliation

### Monorepo Test Results (`npm test`):
- `@syncplay/control-events`: **49 passed** (6 test files)
- `@syncplay/peer-mesh`: **101 passed** (6 test files)
- `@syncplay/protocol`: **273 passed** (4 test files)
- `@syncplay/sync-engine`: **412 passed** (24 test files) — includes all 56 tests in `repair-fulfillment.test.ts`
- `@syncplay/bootstrap-server`: **91 passed** (3 test files)
- **Monorepo Total:** **926 passed across 43 test files with 0 failures, 0 skipped**.
- **Monorepo Build (`npm run build`):** Clean compilation across all packages with zero TypeScript or Vite errors.

---

## 5. Answers to the 10 Explicit Stop Conditions

1. **Can duplicate requests cause unbounded outbound response amplification?**  
   **NO.** Duplicate requests consume rate-limiting tokens from the requesting peer's token bucket. When the token budget is exhausted, the supplier rejects with `REPAIR_REJECTED` (`reason: 'RATE_LIMITED'`), strictly bounding outbound bandwidth.
2. **Can a reused request ID with different request content receive an unrelated cached response?**  
   **NO.** `CachedResponse` records the canonical request parameters (`targetOriginPeerId`, `fromSequence`, `toSequence`, `sessionId`). A reused request ID with conflicting parameters throws `SyncEngineError('INVALID_ARGUMENT')`.
3. **Can ordinary SDK consumers obtain the trusted capability or register arbitrary requester contexts?**  
   **NO.** The capability is private to `LeaderlessSyncEngine` and `TrustedTransportAdapter`. Valid contexts are stored in an engine-private `WeakSet`. Attempting to pass forged or cross-engine contexts fails reference equality and throws `SyncEngineError('UNAUTHORIZED')`.
4. **Does byte accounting preserve the 65,536-byte application cap and the actual negotiated transport limit?**  
   **YES.** The supplier clamps payload size to $\min(65536, \text{negotiatedSctpSize})$, reserving an envelope overhead margin (default: 256 bytes) to guarantee the framed message over the wire never exceeds the transport limit.
5. **Are unavailable and zero-valued negotiated SCTP limits handled correctly?**  
   **YES.** `undefined` falls back to the 64 KB cap ($65,536$). `0` is interpreted per RFC 8841 as unconstrained remote peer and clamped to the 64 KB local cap ($65,536$).
6. **Can old-epoch or old-generation work deliver a response after a transition?**  
   **NO.** Supplier servicing is strictly synchronous within a single turn and includes pre-delivery currency validation. Epoch transitions purge the response cache and reset concurrency counters. Requests from older epochs or stale connection generations are immediately rejected with `EPOCH_MISMATCH` or `UNAUTHORIZED`.
7. **Can a permanent history gap trigger unbounded retries?**  
   **NO.** Permanent gaps return `OUTSIDE_HORIZON` (with `retainedHorizonSequence`) or `ORIGIN_UNKNOWN`. M3 treats these as non-retryable for that supplier, terminating retries and triggering `onHistoryExhausted` when no suppliers retain the history.
8. **Are all caches, counters, and queues bounded?**  
   **YES.** Response cache is bounded to 64 entries (FIFO/LRU). Active requests are bounded room-wide ($\le 16$) and per-peer ($\le 4$). Rate limiters are bounded by token bucket burst (20 tokens).
9. **Are all frozen regression suites passing?**  
   **YES.** All 926 tests across all 5 monorepo packages pass with zero regressions.
10. **Is the final audited M4 baseline committed with a clean working tree?**  
    **YES.** Committed with a clean working tree.

---

## 6. Scope Confirmation

Milestone M5 (Transport Integration) and Phase 7.5 (Snapshot Recovery) have **not** been started.

Phase 7.4 Milestone M4 is fully audited, verified, and complete.
