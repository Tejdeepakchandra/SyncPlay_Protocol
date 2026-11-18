# Phase 7.4 Milestone M4 Completion Report: Supplier Range Servicing

**Milestone:** Phase 7.4 — Milestone M4 (Supplier Range Servicing)  
**Status:** COMPLETED & ARCHITECTURALLY VERIFIED  
**Architecture Specification Baseline:** `0289ad5a62e03264b30a10b429d29eb0ca1691a1`  
**Approved Causal Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Phase 7.3-R5-H Trust Boundary Baseline:** `e49b81a`  
**Milestone M1 Baseline:** `309b808` (amending `5cadc03`)  
**Milestone M2 Baseline:** `2d5e9f3` (docs: `4926c52`)  
**Milestone M3 Baseline:** `c143972` (docs: `a42c0e7`)  
**Milestone M4 Implementation Baseline:** `5001199`  
**Milestone M4 Docs Baseline:** `66d1069`  
**Parent Commit:** `a42c0e7`  
**Branch:** `main`  
**Working Tree Status:** Clean  
**Date:** 2026-11-18  

---

## 1. Executive Summary

Milestone M4 implements the **Supplier-Side Lifecycle** of the Phase 7.4 anti-entropy repair protocol for the SyncPlay Protocol. 

While Milestone M3 implemented requester-side repair tracking, supplier selection, retries, and admission, Milestone M4 enables a peer to authoritatively and safely service incoming `REPAIR_REQUEST` messages from its retained operation log without ever fabricating, inferring, or reconstructing missing history, and without mutating local playback state.

### Core Architectural Guarantees Established in M4:
1. **Mandatory Capability-Gated Requester Authentication:**
   Every incoming `REPAIR_REQUEST` strictly requires an `AuthenticatedRequesterContext` minted exclusively via reference equality (`===`) against the private `TransportRegistrationCapability` held by the `TrustedTransportAdapter`. Requester identity and connection generation are derived solely from trusted WebRTC transport state, never from untrusted payload claims.
2. **Authoritative Retained-History Verification (No Guesswork or Fabrication):**
   The local `OrderedOperationLog` is the single source of truth for servicing. The supplier verifies that every requested sequence in $[fromSequence, toSequence]$ is contiguous, uncompacted, and present in the log. If any sequence is compacted to base snapshot ($fromSeq \le \text{snapshotFrontier}$) or predates the active log, it rejects with `OUTSIDE_HORIZON` (and reports `retainedHorizonSequence`). If a hole exists or the requested range extends beyond the supplier's frontier, it rejects with `BUSY`. It never fabricates missing operations.
3. **Strict Bounded Frame Sizing & Contiguous Slicing:**
   Enforces both the 32-operation batch ceiling and the 64 KB ($65,536$ bytes) WebRTC DataChannel message size limit. If a serialized `OPERATION_BATCH` exceeds $\min(65536, \text{negotiatedSctpMaxMessageSize})$, the supplier slices the batch into a smaller contiguous sub-range $[fromSeq, fromSeq + M - 1]$ that strictly satisfies the byte cap. If even a single operation cannot fit within the size cap, it rejects with `BUSY`.
4. **Bounded Concurrency, Rate Limiting & Replay Idempotence:**
   Enforces room-wide ($\le 16$) and per-peer ($\le 4$) concurrent request limits, a per-peer token bucket rate limiter (10 req/s, burst 20), and a bounded LRU/FIFO response cache (64 entries). Duplicate request IDs return the cached response immediately with zero redundant log lookups and zero serialization overhead.
5. **Epoch & Lifecycle Isolation:**
   Membership epoch transitions instantly wipe the response cache and reject requests from older epochs with `EPOCH_MISMATCH`. Disconnections purge peer-specific rate limiters and tracking. Servicing repairs causes zero playback state mutations, zero clock advances, and zero vector corruption on the supplier.

---

## 2. Files Added and Modified

### Added:
1. `packages/sync-engine/src/replication/repair-supplier-service.ts`:
   - Dedicated supplier-side service implementing request validation, rate limiting, concurrency bounds, duplicate response caching, retained log querying, batch slicing, and rejection construction.
2. `packages/sync-engine/src/__tests__/repair-fulfillment.test.ts`:
   - Comprehensive 38-test test suite verifying all request validation, history verification, response correctness, and adversarial lifecycle conditions.

### Modified:
1. `packages/sync-engine/src/log/types.ts` & `packages/sync-engine/src/log/ordered-operation-log.ts`:
   - Added `getOldestRetainedSequence(originPeerId: string): number | undefined` to accurately identify the oldest sequence currently retained in the active log for `OUTSIDE_HORIZON` rejection reporting.
2. `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
   - Exported `AuthenticatedRequesterContext` interface and `createAuthenticatedRequesterContext()` helper.
   - Added `registerInboundRequesterContext()` and `isValidRequesterContext()` backed by private `validRequesterContexts = new WeakSet<AuthenticatedRequesterContext>()`.
   - Added `registerRequesterContext()` and `handleInboundRepairRequest()` to `TrustedTransportAdapter`.
   - Added `getOperationLog(): OrderedOperationLog` and `serveRepairRequest()`.
3. `packages/sync-engine/src/replication/types.ts`:
   - Exported `RepairSupplierOptions` interface.
   - Updated `IAntiEntropyManager.serveRepairRequest(request: RepairRequestPayload, context?: AuthenticatedRequesterContext)`.
4. `packages/sync-engine/src/replication/anti-entropy-manager.ts`:
   - Instantiated and integrated `RepairSupplierService`.
   - Wired `serveRepairRequest()`, `onPeerDisconnected()`, and `onEpochTransition()`.
5. `packages/sync-engine/src/replication/index.ts` & `packages/sync-engine/src/index.ts`:
   - Exported `RepairSupplierService`, `AuthenticatedRequesterContext`, and supplier constants.
6. `packages/sync-engine/src/__tests__/anti-entropy-manager.test.ts`:
   - Updated separation test to verify transport authentication on `serveRepairRequest()`.

---

## 3. Public Interfaces Added and Modified

```typescript
// packages/sync-engine/src/engine/leaderless-sync-engine.ts
export interface AuthenticatedRequesterContext {
  readonly requesterPeerId: string;
  readonly connectionGeneration?: number;
}

export function createAuthenticatedRequesterContext(
  engine: LeaderlessSyncEngine,
  requesterPeerId: string,
  connectionGeneration: number,
  capability: TransportRegistrationCapability,
): AuthenticatedRequesterContext;

// TrustedTransportAdapter extensions:
export class TrustedTransportAdapter {
  public registerRequesterContext(requesterPeerId: string, connectionGeneration: number): AuthenticatedRequesterContext;
  public handleInboundRepairRequest(
    request: RepairRequestPayload,
    transportRequesterPeerId: string,
    transportConnectionGeneration: number,
  ): OperationBatchPayload | RepairRejectedPayload;
}

// packages/sync-engine/src/replication/types.ts
export interface RepairSupplierOptions {
  readonly maxConcurrentRequests?: number;
  readonly maxConcurrentRequestsPerPeer?: number;
  readonly rateLimitMaxRequestsPerSecond?: number;
  readonly rateLimitBurst?: number;
  readonly responseCacheCapacity?: number;
  readonly maxOperationsPerBatch?: number;
  readonly maxPayloadBytes?: number;
  readonly negotiatedSctpMaxMessageSizeProvider?: (peerId: string) => number | undefined;
}
```

---

## 4. Request Validation Rules

Before inspecting retained history, every incoming request passes through 8 strict pre-flight validation gates:

| # | Validation Rule | Layer | Failure Outcome |
|---|---|---|---|
| 1 | **Transport Context Authentication** | Capability WeakSet | Throws `SyncEngineError('UNAUTHORIZED')` if context is missing, plain object, or unregistered. |
| 2 | **Connection Generation Currency** | Engine Peer Table | Throws `SyncEngineError('UNAUTHORIZED')` if `context.connectionGeneration !== engine.getPeerGeneration(peer)`. |
| 3 | **Wire Schema Validation** | Zod Schema | Throws `SyncEngineError('INVALID_ARGUMENT')` on malformed schema or invalid UUID v4 `requestId`. |
| 4 | **Integer & Range Bounds** | Integer Safety Check | Throws `SyncEngineError('INVALID_ARGUMENT')` if sequences are non-integer, negative, $to < from$, or count $> 32$. |
| 5 | **Session Scoping** | Room Identity Check | Throws `SyncEngineError('INVALID_ARGUMENT')` if `request.sessionId !== engine.sessionId`. |
| 6 | **Requester Identity Binding** | Anti-Spoofing Check | Throws `SyncEngineError('UNAUTHORIZED')` if payload `requesterPeerId !== context.requesterPeerId`. |
| 7 | **Membership Epoch Matching** | Epoch State Machine | Returns `REPAIR_REJECTED` (`reason: 'EPOCH_MISMATCH'`) if `request.membershipEpoch !== engine.epoch`. |
| 8 | **Rate & Concurrency Limits** | Token Bucket & Counter | Returns `REPAIR_REJECTED` (`reason: 'RATE_LIMITED'` or `'BUSY'`). |

---

## 5. Retained-History Verification and Rejection Semantics

The supplier inspects `OrderedOperationLog` using exact index lookups (`getOperationAt(origin, seq)`):

1. **Origin Known Verification:**
   The origin is checked across `replicationFrontier`, `baseSnapshotFrontier`, and active log operations. If completely unobserved, returns `REPAIR_REJECTED` (`reason: 'ORIGIN_UNKNOWN'`).
2. **Compacted Base Snapshot Boundary:**
   If `request.fromSequence <= baseSnapshotFrontier[targetOrigin]`, the operations were folded into the snapshot. The supplier returns `REPAIR_REJECTED` (`reason: 'OUTSIDE_HORIZON'`), providing `retainedHorizonSequence = oldestRetained ?? (baseSnapshotFrontier + 1)`.
3. **Active Log Retention Horizon:**
   If `oldestRetained !== undefined && request.fromSequence < oldestRetained`, the requested history predates the ring buffer retention horizon. Returns `REPAIR_REJECTED` (`reason: 'OUTSIDE_HORIZON'`, `retainedHorizonSequence = oldestRetained`).
4. **Contiguous Range Completeness:**
   Every sequence $s \in [fromSequence, toSequence]$ is retrieved from `OrderedOperationLog`.
   - If any sequence is missing (hole in log, or extends beyond frontier): returns `REPAIR_REJECTED` (`reason: 'BUSY'`).
   - If an operation in the log has a mismatched origin or epoch: returns `REPAIR_REJECTED` (`reason: 'BUSY'` or `'EPOCH_MISMATCH'`).
   - The supplier never invents, reconstructs, or hallucinates missing operations.

---

## 6. Bounded Response Sizing and Slicing

1. **Exact Stored Contents:** Operations are converted from `StoredOperation` to `SerializedControlOperation`, strictly preserving `logicalEventId`, `originPeerId`, `membershipEpoch`, `messageType`, `logicalTimestamp`, `originSequenceNumber`, `causalContext`, and `payload`.
2. **Size Enforcement (64 KB Cap):**
   The candidate `OperationBatchPayload` is serialized to UTF-8 JSON. Its byte length is compared against $\min(65536, \text{negotiatedSctpMaxMessageSize})$.
3. **Contiguous Slicing:**
   If serialized size exceeds the byte cap, the batch is sliced down to $[fromSequence, fromSequence + M - 1]$ (where $M \ge 1$) until it fits within the limit. `toSequence` is updated to match the last included operation, guaranteeing that `validateContiguousSequenceBatch` refinements pass.
4. **Single-Operation Oversized Handling:**
   If even a single operation ($M = 1$) exceeds the maximum permitted frame size, it returns `REPAIR_REJECTED` (`reason: 'BUSY'`).

---

## 7. Duplicate Handling and Resource Bounds

1. **Response Cache:**
   A bounded FIFO/LRU cache ($K=64$) indexes responses by `${requesterPeerId}:${requestId}`. Re-delivered requests with the same `requestId` are served immediately from the cache with zero log reads, zero redundant serialization, and zero response amplification.
2. **Fresh Requests for Same Range:**
   Requests for the same sequence range under a new `requestId` are processed as fresh requests.
3. **Per-Peer Rate Limiting:**
   Each peer is governed by a token bucket (refill rate: 10 tokens/s, burst: 20 tokens). Excess requests return `REPAIR_REJECTED` (`reason: 'RATE_LIMITED'`).
4. **Concurrency Bounds:**
   Room-wide active concurrent requests are capped at 16; per-peer active requests are capped at 4. Saturated requests return `REPAIR_REJECTED` (`reason: 'BUSY'`).
5. **Epoch Transitions & Disconnections:**
   On `onEpochTransition(newEpoch)`, response caches are wiped and active counters reset. On `onPeerDisconnected(peerId)`, rate limiters, active counts, and cached responses for that peer are evicted.

---

## 8. Verification Results

### 8.1 Focused Test Suite: `repair-fulfillment.test.ts` (38 tests)
- **Request Validation (13 tests):** Valid request, invalid schema, wrong session, wrong epoch, unauthenticated, fabricated context, cross-engine context, identity spoofing, stale generation, unknown origin, invalid sequence range, unsafe integer, range $> 32$.
- **Retained History Verification (9 tests):** Entire range available, first missing (compacted), last missing (beyond frontier), middle missing (hole), compacted into base snapshot, partly compacted, beyond frontier, advertised vs actual log mismatch, cross-epoch op in log.
- **Response Correctness (6 tests):** Exact operation content preserved, origin/epoch/requestId echoed, contiguous sequence coverage, 64 KB cap slicing, single op oversized rejection (`BUSY`), negotiated SCTP max message size slicing.
- **Lifecycle and Adversarial Cases (10 tests):** Duplicate request ID cache hit, new request ID fresh servicing, epoch transition cache purge, peer disconnect cleanup, outbound callback invocation, throwing callback re-entrancy safety, token bucket rate limiting, zero playback state mutation, `TrustedTransportAdapter` routing, and full round-trip receiver ingestion (`ingestRepairBatch`).

### 8.2 Monorepo Test Results (`npm test`)
- `@syncplay/control-events`: **49 passed** (6 test files)
- `@syncplay/peer-mesh`: **101 passed** (6 test files)
- `@syncplay/protocol`: **273 passed** (4 test files)
- `@syncplay/sync-engine`: **394 passed** (24 test files)
- `@syncplay/bootstrap-server`: **91 passed** (3 test files)
- **Monorepo Total:** **908 passed across 43 test files with 0 failures, 0 skipped**.
- **Monorepo Build (`npm run build`):** Clean TypeScript typecheck (`tsc`) and Vite bundle build across all packages with zero errors.

---

## 9. Answers to Final Stop Conditions

1. **Can the supplier prove every returned operation exists in its actual retained log?**  
   **YES.** Every returned operation is retrieved via `OrderedOperationLog.getOperationAt(origin, seq)` and validated for exact origin and epoch matching.
2. **Can any request produce a fabricated, incomplete, cross-origin, or cross-epoch batch?**  
   **NO.** If any sequence in the requested range is missing, incomplete, from a different epoch, or cross-origin, the supplier rejects with an explicit rejection (`OUTSIDE_HORIZON`, `ORIGIN_UNKNOWN`, `EPOCH_MISMATCH`, or `BUSY`). All outgoing batches are verified with `OperationBatchPayloadSchema.parse()`.
3. **Can a stale or unauthenticated requester obtain history?**  
   **NO.** Requests without an unforgeable `AuthenticatedRequesterContext` registered via capability reference equality, or requests with connection generations that do not match the engine's active generation, are rejected with `SyncEngineError('UNAUTHORIZED')` before reading log history.
4. **Are duplicate requests and response amplification bounded?**  
   **YES.** Duplicate `(requesterPeerId, requestId)` hits the bounded LRU cache ($K=64$) and returns the existing response with zero log queries. Rate limits bound incoming request frequencies.
5. **Are all response sizes and operation counts bounded before sending?**  
   **YES.** Operation counts are capped at 32 operations. Serialized frame bytes are capped at $\min(65536, \text{negotiatedSctpMaxMessageSize})$ with contiguous slicing down to smaller sub-ranges.
6. **Are M1, M2, M3, and Phase 7.3 regression suites passing?**  
   **YES.** All 908 monorepo tests pass cleanly with zero regressions.
7. **Is M4 committed with a clean working tree?**  
   **YES.** Implementation committed at `5001199`.

---

## 10. Scope Confirmation

Milestone M5 (Transport Integration) and Phase 7.5 (Snapshot Recovery) have **not** been started.

Phase 7.4 Milestone M4 is complete, verified, and ready for architectural review.
