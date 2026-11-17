# Phase 7.3-R5-G: Final Supplier Authorization Closure Report

**Milestone:** Phase 7.3-R5-G (Event Ordering & Conflict Engine — Final Authorization & Closure)  
**Date:** 2026-11-17  
**Baseline Commit:** `1658f81` (`docs(reports): update commit hash in Phase 7.3-R5-F closure report`)  
**Approved Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Status:** Verification Complete — Phase 7.3 Frozen and Ready for Phase 7.4 Planning  

---

## Executive Summary

Phase 7.3-R5-G resolves the final concrete supplier authorization defect and transport-boundary ambiguity identified during the Phase 7.3-R5-F review:

1. **Removal of Implicit Supplier Generation Defaulting (`?? 1` Eliminated):**
   In previous iterations, missing peer authorization records in `peerGenerations` fell back to default generation 1 (`this.peerGenerations.get(supplier) ?? 1`). This opened a security hole where unauthenticated or unregistered peers could deliver repair traffic at generation 1. In Phase 7.3-R5-G, the `?? 1` fallback is completely eliminated. The engine retrieves `activeSupplierGen = this.peerGenerations.get(transportSupplierPeerId)`. If `activeSupplierGen === undefined`, the batch is immediately rejected with `REJECTED_INVALID` (`Unknown or unauthorized repair supplier: <id>`). Exact match with the authorized generation is required (`transportSupplierGen !== activeSupplierGen` rejects both stale and unannounced generations). Rejected repair batches guarantee zero mutation across the operation log, pending queue, Lamport clock, causal frontier, replication vector, and playback state.
2. **Unforgeable Transport Provenance & Authenticated Supplier Context Boundary:**
   TypeScript interfaces do not provide runtime security against arbitrary caller-instantiated plain objects. To enforce genuine transport provenance, `LeaderlessSyncEngine` maintains a private `validSupplierContexts = new WeakSet<AuthenticatedSupplierContext>()`. Genuine contexts must be registered via `engine.registerInboundSupplierContext(supplierPeerId, connectionGeneration)` or `createAuthenticatedSupplierContext(engine, supplierPeerId, connectionGeneration)` by the trusted transport adapter (`ControlEventService` / `PeerMesh`). Arbitrary plain objects (`{ supplierPeerId, connectionGeneration }`) or shallow clones (`{ ...context }`) are rejected with `REJECTED_INVALID` (`Fabricated or unauthenticated supplier transport context: context must be locally registered by transport adapter`).
3. **WebRTC Transport Stack & Cryptographic Identity Correction:**
   WebRTC DataChannels use **SCTP encapsulated over DTLS** (RFC 8831 / RFC 8261). SRTP is reserved exclusively for audio/video RTP media streams and is not present on DataChannels. DTLS provides channel encryption and message integrity using ephemeral self-signed certificates, but does not inherently bind cryptographic keys to application-level `peerId` identities. Real-world identity binding is established via authenticated signaling channels, SDP DTLS certificate fingerprint verification, and monotonic `ConnectionGeneration` tracking in `PeerManager`.
4. **Comprehensive Test & Build Verification:**
   Added dedicated test suite `closure-r5g.test.ts` (11 tests) and updated existing test suites, bringing `@syncplay/sync-engine` to 231 tests across 16 test files, and the monorepo to 613 tests across 35 test files (0 failures). Production TypeScript compilation (`tsc`) and client demo bundle build (`vite build`) passed cleanly with zero errors.

---

## 1. Task 1: Complete Elimination of Implicit Supplier Generation Authorization

### 1.1 The Vulnerability of `?? 1` Fallback

In Phase 7.3-R5-F, `LeaderlessSyncEngine.ingestRepairBatch()` inspected the supplier generation with:
```ts
// Flawed implementation in R5-F:
const activeSupplierGen = this.peerGenerations.get(transportSupplierPeerId) ?? 1;
```
If `peerGenerations` had never observed or authorized `transportSupplierPeerId`, `activeSupplierGen` evaluated to `1`. Consequently:
- Any arbitrary unknown remote peer could submit repair batches claiming generation 1 without having undergone peer authorization via `PeerManager.authorizePeerGeneration()`.
- Untrusted repair traffic could inject historical operations without the peer being registered as a live member in the current session.

### 1.2 Strict Authoritative Supplier Verification

In Phase 7.3-R5-G, the `?? 1` fallback is completely removed:
```ts
// Strict R5-G implementation:
const activeSupplierGen = this.peerGenerations.get(transportSupplierPeerId);
if (activeSupplierGen === undefined) {
  const decision: EngineDecision = {
    accepted: false,
    code: 'REJECTED_INVALID',
    stateVersion: this.state.stateVersion,
    decidedAt: now,
    message: `Unknown or unauthorized repair supplier: ${transportSupplierPeerId}`,
  };
  this.lastDecision = decision;
  this.mutableStats.eventsReceivedCount += Math.max(1, batch.operations.length);
  this.mutableStats.eventsRejectedCount += Math.max(1, batch.operations.length);
  return [decision];
}

if (transportSupplierGen < activeSupplierGen) {
  // Stale connection generation rejected
  ...
}

if (transportSupplierGen > activeSupplierGen) {
  // Unannounced connection generation rejected
  ...
}
```

### 1.3 Generation Rules Enforced

1. **Unknown Supplier (Gen 1 or Gen > 1):** If `activeSupplierGen === undefined`, reject immediately as `REJECTED_INVALID`. Peer authorization must be explicitly registered prior to processing repair traffic.
2. **Exact Active Match:** Only `transportSupplierGen === activeSupplierGen` is accepted.
3. **Stale Generation ($< active$):** Reject immediately as `REJECTED_INVALID` (`Stale supplier connection generation`).
4. **Unannounced Generation ($> active$):** Reject immediately as `REJECTED_INVALID` (`Unannounced supplier connection generation`).
5. **Zero State Mutation:** Rejected repair traffic does NOT advance the local Lamport clock, causal frontier, replication vector, pending dependency queue, operation log, or playback state.

---

## 2. Task 2: Unforgeable Transport Context Boundary & Provenance

### 2.1 The Interface Fallacy in TypeScript

A TypeScript interface such as `AuthenticatedSupplierContext` exists only at compile time and is completely erased during JavaScript emission. If an API accepts `{ supplierPeerId: string, connectionGeneration: number }`, any caller in the JavaScript runtime can fabricate an object literal:
```ts
// Anyone could bypass the boundary:
engine.ingestRepairBatch(batch, { supplierPeerId: 'peer-S', connectionGeneration: 2 });
```
This fails to guarantee that the context was produced by the transport adapter upon physical packet arrival.

### 2.2 Unforgeable Registration via Private `WeakSet`

To establish genuine transport provenance:
1. `LeaderlessSyncEngine` creates a private `WeakSet`:
   ```ts
   private readonly validSupplierContexts = new WeakSet<AuthenticatedSupplierContext>();
   ```
2. The trusted transport layer (`ControlEventService` / `PeerMesh`) registers incoming contexts using:
   ```ts
   public registerInboundSupplierContext(
     supplierPeerId: string,
     connectionGeneration: number,
   ): AuthenticatedSupplierContext {
     if (!supplierPeerId || typeof supplierPeerId !== 'string') {
       throw new SyncEngineError('INVALID_ARGUMENT', 'supplierPeerId must be non-empty string');
     }
     if (
       typeof connectionGeneration !== 'number' ||
       !Number.isInteger(connectionGeneration) ||
       connectionGeneration < 1
     ) {
       throw new SyncEngineError(
         'INVALID_ARGUMENT',
         `Invalid connection generation ${connectionGeneration}: must be positive integer`,
       );
     }
     const context: AuthenticatedSupplierContext = Object.freeze({
       supplierPeerId,
       connectionGeneration,
     });
     this.validSupplierContexts.add(context);
     return context;
   }
   ```
3. In `ingestRepairBatch()`:
   ```ts
   if (!this.validSupplierContexts.has(transportContext)) {
     const decision: EngineDecision = {
       accepted: false,
       code: 'REJECTED_INVALID',
       stateVersion: this.state.stateVersion,
       decidedAt: now,
       message: 'Fabricated or unauthenticated supplier transport context: context must be locally registered by transport adapter',
     };
     this.lastDecision = decision;
     ...
     return [decision];
   }
   ```

### 2.3 Honest In-Process Realm Boundary Statement

> [!NOTE] In-Process Execution Boundary
> The `WeakSet` registration pattern enforces that callers cannot pass arbitrary plain objects or cloned objects without explicitly routing through `registerInboundSupplierContext()`. However, within standard JavaScript execution in a shared realm, code with unrestricted access to engine instances, prototypes, or private fields (via debugger/reflection) cannot be cryptographically isolated. This mechanism functions as an architectural trust boundary between internal transport layers and application code, preventing accidental or unauthorized fabrication in normal runtime conditions.

---

## 3. Task 3: WebRTC Transport Protocol Correction

### 3.1 SCTP over DTLS vs SRTP

The WebRTC architecture defines two distinct transport pathways:
1. **WebRTC DataChannels (RFC 8831 / RFC 8261):**
   DataChannels carry arbitrary binary or text data using **SCTP (Stream Control Transmission Protocol) encapsulated within DTLS (Datagram Transport Layer Security)** over UDP. The cryptographic security of DataChannels is provided entirely by DTLS.
2. **WebRTC Media Streams (RFC 3550 / RFC 3711):**
   Audio and video tracks are packetized as RTP and secured using **SRTP (Secure Real-time Transport Protocol)**. SRTP keying is typically negotiated via DTLS-SRTP (RFC 5764), but SRTP is strictly applied to media streams, **never to DataChannels**.

The correct protocol stack for SyncPlay protocol frames and repair batches is:
$$\text{SyncPlay Frame} \longrightarrow \text{SCTP} \longrightarrow \text{DTLS 1.3} \longrightarrow \text{UDP} \longrightarrow \text{IP}$$

### 3.2 Binding Transport Identity to Application Identity

DTLS uses self-signed certificates generated dynamically by each browser or client instance. Consequently, a successful DTLS handshake alone proves only that the peer possesses the private key corresponding to its self-signed certificate; it does **not** bind that connection to a specific `peerId` or user identity.

To achieve cryptographically verified identity binding in SyncPlay:
1. **Signaling Server Authentication:** The peer authenticates to the signaling server using its session credential. The signaling server assigns or validates the peer's canonical `peerId`.
2. **SDP Fingerprint Verification:** The SDP offer/answer exchanged via the authenticated signaling server contains the SHA-256 certificate fingerprint (`a=fingerprint:sha-256 ...`). The local WebRTC stack verifies that the remote DTLS certificate matches the fingerprint declared in the signaling payload.
3. **Monotonic Connection Generation:** When a peer reconnects, `PeerManager` increments and authorizes the new `ConnectionGeneration`, preventing stale or replayed repair packets from being ingested under previous transport states.

---

## 4. Verification Evidence

### 4.1 Test Suites

1. **`closure-r5g.test.ts` (11 dedicated tests):**
   - Unknown supplier with claimed gen 1 rejected as `REJECTED_INVALID`.
   - Unknown supplier with claimed gen > 1 rejected as `REJECTED_INVALID`.
   - Known supplier with exact authorized active gen accepted.
   - Known supplier with stale gen rejected.
   - Known supplier with unannounced gen rejected.
   - Rejected unknown supplier does not mutate engine state.
   - Fabricated context rejected.
   - Cloned context rejected.
   - Genuine registered context succeeds.
   - Wire payload supplier cannot override transport identity.
   - Historical event authorship remains independent of supplier generation.
2. **`closure-r5.test.ts` (34 tests):**
   - All tests updated and passing with `registerInboundSupplierContext()`.
3. **`trust-boundary-r4.test.ts` (27 tests), `deterministic-simulation.test.ts` (17 tests), `identity-authorization-r3.test.ts` (25 tests):**
   - All updated and passing.

### 4.2 Test Suite Execution Results

```
Test Files  35 passed (35)
Tests       613 passed (613)
```

Breakdown by package:
| Package | Test Files | Tests | Status |
|---|---|---|---|
| `@syncplay/control-events` | 6 | 49 | **PASS** |
| `@syncplay/peer-mesh` | 6 | 101 | **PASS** |
| `@syncplay/protocol` | 4 | 141 | **PASS** |
| `@syncplay/sync-engine` | 16 | 231 | **PASS** |
| `@syncplay/bootstrap-server` | 3 | 91 | **PASS** |
| **Monorepo Total** | **35** | **613** | **ALL PASSED** |

### 4.3 Production Build Results

1. **TypeScript Compilation:**
   ```
   npm run build --workspaces --if-present
   @syncplay/control-events: tsc -> 0 errors
   @syncplay/peer-mesh: tsc -> 0 errors
   @syncplay/protocol: tsc -> 0 errors
   @syncplay/sync-engine: tsc -> 0 errors
   @syncplay/bootstrap-server: tsc -> 0 errors
   ```
2. **Demo Vite Production Bundle:**
   ```
   vite build -> 171 modules transformed
   dist/index.html                  9.01 kB
   dist/assets/index-BCFYniTR.js  197.20 kB
   built in 1.11s -> 0 errors
   ```

---

## 5. Summary of Files Changed

| File | Change Description |
|---|---|
| `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | Removed `?? 1` fallback; rejected unknown suppliers; added `validSupplierContexts = new WeakSet()`, `registerInboundSupplierContext()`, `createAuthenticatedSupplierContext()`; validated context registration. |
| `packages/sync-engine/src/index.ts` | Exported `createAuthenticatedSupplierContext`. |
| `packages/sync-engine/src/__tests__/closure-r5g.test.ts` | New test suite verifying all Task 1 & Task 2 closure requirements (11 tests). |
| `packages/sync-engine/src/__tests__/closure-r5.test.ts` | Updated genuine transport context calls to use `engine.registerInboundSupplierContext()`. |
| `packages/sync-engine/src/__tests__/trust-boundary-r4.test.ts` | Updated 8 test calls to use `engine.registerInboundSupplierContext('peer-S', gen)`. |
| `packages/sync-engine/src/__tests__/deterministic-simulation.test.ts` | Added supplier authorization and context registration for repair batch. |
| `packages/sync-engine/src/__tests__/identity-authorization-r3.test.ts` | Updated context call to use `registerInboundSupplierContext`. |

---

## 6. Strict Scope Confirmation

- **No Phase 7.4 Implementation:** Anti-entropy repair protocol, state digest broadcasts, missing-range reconciliation, repair requests, and periodic repair scheduling have NOT been started or authorized.
- **No Phase 7.5 Implementation:** Dynamic snapshot recovery, full fault-tolerance, and cluster synchronization have NOT been started or authorized.
- **Phase 7.2-R9 Causal Model Preserved:** Exact Lamport timestamps ($L(e) = 1 + \max(L(p,s))$), exact equality validation, receiver-side clock update without $+1$, and total-order tie-breaking are fully preserved.

---

**Phase 7.3 is frozen and ready for Phase 7.4 planning. Phase 7.4 implementation has not started and remains subject to separate authorization.**
