# Phase 7.4 Milestone M1: Protocol Schemas & Serialization — Final Inbound Routing & Runtime-Scope Report

**Milestone:** Phase 7.4 Milestone M1 — Protocol Schemas & Serialization (Final Inbound Routing & Runtime-Scope Corrections)  
**Phase:** 7.4 — State Replication & Anti-Entropy  
**Date:** 2026-11-17  
**Base Architecture Specification Commit:** `0289ad5` (`0289ad5a62e03264b30a10b429d29eb0ca1691a1`)  
**Parent Implementation Commit:** `daf9c2b` (`daf9c2be0b48074d28362b53b0e3606f7166e409`)  
**M1 Final Routing Revision Commit:** `5cadc03` (`5cadc036d07d1a58bc25585093e0b23023023e98`)  
**Approved Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Previous Implementation:** Phase 7.3-R5-H (`e49b81a`)  
**Scope Status:** Milestone M1 Final Corrections Complete — M2–M5 Strictly Deferred  

---

## Executive Summary

This report documents the resolution of inbound message routing, runtime-scope boundaries, and transport send-size semantics for **Milestone M1 (Protocol Schemas & Serialization)** of Phase 7.4 (State Replication & Anti-Entropy).

Following the senior distributed-systems architectural review:
1. **Push-Based Delivery Removed from Active Contract:** Anti-entropy operation batch delivery in Phase 7.4 is strictly request-driven (`STATE_DIGEST` $\to$ diff detection $\to$ `REPAIR_REQUEST` $\to$ `OPERATION_BATCH` / `REPAIR_REJECTED`). Unsolicited or push-based operation batches are **not** required by the approved Phase 7.4 architecture and have been cleanly removed from the active M1 network contract. `OperationBatchPayloadSchema` is the single canonical schema for all operation batch delivery on the wire, requiring mandatory UUID v4 `requestId`, `sessionId`, `supplierPeerId`, `originPeerId`, `fromSequence`, `toSequence`, `membershipEpoch`, and contiguous operations. Any future push-based delivery path is deferred and requires explicit architectural authorization.
2. **Accurate M1 Scope vs. Future Runtime Capacity Enforcement:** The report distinguishes implemented M1 structural limits from documented future engine requirements. M1 enforces that protocol schemas reject collections containing $> 64$ origin entries at the network serialization/validation boundary. M1 does **not** implement runtime origin admission tracking, engine-level degraded-state transitions (`isDegraded`, `degradedReason`), runtime operation rejections (`REJECTED_CAPACITY`), or automatic snapshot compaction/pruning. In accordance with architectural principles, the engine must never silently evict an active origin entry (which would break causal consistency and cause split-brain states). Safe origin reclamation requires a correctly implemented snapshot and membership barrier, which remain future engine/coordinator responsibilities (M3/M4/Phase 7.5).
3. **Correct WebRTC Send-Size Contract:** Removed any suggestion that oversized messages may be truncated. Explicitly documented the send-size contract: the negotiated limit is read from `RTCPeerConnection.sctp?.maxMessageSize`; the sender must measure actual UTF-8 byte length before sending; oversized sends must be prevented upfront or treated as unrecoverable send failures; applications must never rely on truncation (WebRTC DataChannel does not support truncation); SCTP packet fragmentation is transport-level packetization, not a substitute for application-level compliance; and application-level pagination/slicing is an M5 responsibility.
4. **Clean Verification Across Monorepo:** Verified 273 tests in `@syncplay/protocol` (+132 tests over the 141-test baseline), 755 tests across the full monorepo (0 failures), and clean TypeScript (`tsc`) and Vite production builds across all packages with zero modifications to frozen Phase 7.3 engine code.

---

## 1. Active Wire Message Types & Schema Mappings

In `@syncplay/protocol`, `PAYLOAD_SCHEMAS` defines the authoritative registry for all inbound message validation. Every active wire message type maps to its designated Zod schema:

| Wire Message Type String | Enum Identifier | Registered Payload Schema | Delivery / Validation Semantics |
| :--- | :--- | :--- | :--- |
| `'ping'` | `MessageType.PING` | `PingPayloadSchema` | Two-way latency probe |
| `'pong'` | `MessageType.PONG` | `PongPayloadSchema` | Two-way latency response |
| `'play'` | `MessageType.PLAY` | `PlayPayloadSchema` | Playback control command |
| `'pause'` | `MessageType.PAUSE` | `PausePayloadSchema` | Playback control command |
| `'seek'` | `MessageType.SEEK` | `SeekPayloadSchema` | Playback position change |
| `'rate-change'` | `MessageType.RATE_CHANGE` | `RateChangePayloadSchema` | Playback rate change |
| `'state-request'` | `MessageType.STATE_REQUEST` | `StateRequestPayloadSchema` | Full state synchronization request |
| `'state-snapshot'` | `MessageType.STATE_SNAPSHOT` | `StateSnapshotPayloadSchema` | Monolithic state snapshot payload |
| `'state-digest'` | `MessageType.STATE_DIGEST` | `StateDigestWireSchema` | Union: accepts Phase 7.4 V2 multi-vector digest or Phase 2 V1 legacy digest |
| `'operation-batch'` | `MessageType.OPERATION_BATCH` | `OperationBatchPayloadSchema` | **Request-driven only:** requires UUID v4 `requestId`, `sessionId`, `membershipEpoch`, contiguous sequences |
| `'repair-request'` | `MessageType.REPAIR_REQUEST` | `RepairRequestPayloadSchema` | Anti-entropy missing sequence request (UUID v4 `requestId`, $\le 32$ ops) |
| `'repair-rejected'` | `MessageType.REPAIR_REJECTED` | `RepairRejectedPayloadSchema` | Explicit repair rejection echoing `requestId` with machine-readable reason code |
| `'sync:control-event'` | `SYNC_MESSAGE_TYPES.CONTROL_EVENT` | `z.unknown()` | Generic sync event envelope |
| `'sync:operation-batch'` | `SYNC_MESSAGE_TYPES.OPERATION_BATCH` | `OperationBatchPayloadSchema` | **Request-driven only:** identical schema to `MessageType.OPERATION_BATCH` |
| `'sync:state-digest'` | `SYNC_MESSAGE_TYPES.STATE_DIGEST` | `StateDigestPayloadSchema` | Strictly Phase 7.4 V2 multi-vector digest (`formatVersion: 2`) |
| `'sync:repair-request'` | `SYNC_MESSAGE_TYPES.REPAIR_REQUEST` | `RepairRequestPayloadSchema` | Correlated anti-entropy repair request |
| `'sync:repair-rejected'` | `SYNC_MESSAGE_TYPES.REPAIR_REJECTED` | `RepairRejectedPayloadSchema` | Correlated anti-entropy repair rejection |

### Schema Isolation & Rejection Guarantees:
- **`CompatibilityOperationBatchPayloadSchema`:** Strictly isolated to internal Phase 7.3 test fixtures. It is **NOT** present in `PAYLOAD_SCHEMAS`. Inbound wire validation (`validateMessage`) rejects any batch missing `requestId` or `sessionId` with `INVALID_PAYLOAD` and never falls back to the compatibility schema.
- **Push-Based Delivery Status:** **DEFERRED.** Unsolicited push operation batch delivery is not part of the approved Phase 7.4 architecture and has been removed from `@syncplay/protocol`. Both `MessageType.OPERATION_BATCH` and `SYNC_MESSAGE_TYPES.OPERATION_BATCH` require valid UUID v4 `requestId` correlation. Any future push delivery mechanism will require dedicated architectural design, a distinct wire message type, and explicit authorization.

---

## 2. Accurate Distinction: 64-Origin Schema Limits vs. Runtime Capacity Enforcement

### Implemented M1 Behavior (Current Repository Baseline)
1. **Schema-Level Boundary Rejection:**
   `ReplicationVectorSchema`, `SnapshotFrontierSchema`, and `HistoryFingerprintsSchema` enforce:
   ```typescript
   .refine((val) => Object.keys(val).length <= MAX_VECTOR_ENTRIES, {
     message: `Collection cannot exceed ${MAX_VECTOR_ENTRIES} origin entries`,
   })
   ```
   Any incoming `STATE_DIGEST` containing $> 64$ origin entries is rejected at the network boundary during schema validation (`INVALID_PAYLOAD`).
2. **Zero Runtime Engine Mutations in M1:**
   Milestone M1 does **not** implement:
   - Runtime origin admission control or peer tracking.
   - Setting `isDegraded` or `degradedReason = 'MAX_ORIGINS_EXCEEDED'`.
   - Rejecting operations with `REJECTED_CAPACITY`.
   - Automatic triggers for snapshot compaction or membership pruning.
3. **No Silent Eviction Rule:**
   No origin may be silently evicted to satisfy the 64-origin limit. Silently evicting an origin breaks causal dependency tracking in `PendingDependencyQueue`, causing surviving nodes to deadlock on unsatisfied causal dependencies and inducing split-brain state divergence.

### Documented Future Architectural Requirements (M3/M4/Phase 7.5 Responsibilities)
- **Runtime Origin Admission & Degraded State:** When 64 origins are active in the local engine and a 65th unique peer attempts to emit an operation, the future runtime engine (M3/M4) will transition to an explicit degraded state rather than silently dropping historical origins.
- **Safe Origin Reclamation:** Origin entries can only be safely reclaimed when three conditions are met simultaneously:
  1. *Compaction Horizon:* All operations for that origin are subsumed into the stable base snapshot (`snapshotFrontier[p] == replicationVector[p]`).
  2. *Zero Live Dependencies:* No retained operation in the log or pending queue references that origin.
  3. *Membership Epoch Barrier:* A consensus epoch transition acknowledges the peer's exclusion.
- These runtime lifecycle mechanisms remain strictly future responsibilities and are not present in M1.

---

## 3. Corrected WebRTC Send-Size Contract & Transport Semantics

The documentation in [packages/protocol/src/constants.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/constants.ts) was revised to eliminate any suggestion of message truncation and formally define the transport contract:

```typescript
/**
 * Maximum application payload size for anti-entropy operation batches (64 KB = 65,536 bytes).
 *
 * NOTE: This is an application-level batch payload cap derived from RFC 8831 / RFC 8841
 * SCTP interoperability recommendations, NOT a universal browser WebRTC guarantee.
 *
 * Standard WebRTC 1.0 (W3C) defines negotiated message size limits on the underlying
 * SCTP transport: `RTCPeerConnection.sctp?.maxMessageSize`, NOT on `RTCDataChannel`.
 *
 * Send-Size Contract & Transport Semantics:
 * 1. Negotiated Limit Discovery: The remote peer's maximum incoming message size limit
 *    is read from `RTCPeerConnection.sctp?.maxMessageSize`.
 * 2. Conservative Fallback: If SCTP information is unavailable (e.g. `sctp` is undefined or
 *    `maxMessageSize` is null/unannounced), the implementation must handle it conservatively
 *    by falling back to the standard baseline limit of 65,536 bytes.
 * 3. Pre-Send Byte-Length Verification: Before transmitting an operation batch or repair message
 *    over the DataChannel, the sender must measure the actual UTF-8 byte length of the serialized
 *    message (`serializeAndCheckSize`).
 * 4. Prevention of Oversized Sends: Any send where the serialized UTF-8 byte length exceeds
 *    `min(MAX_BATCH_PAYLOAD_BYTES, sctp.maxMessageSize)` must be prevented upfront by the
 *    application layer or treated as an unrecoverable send failure.
 * 5. No Truncation or Partial Delivery: The application MUST NEVER rely on truncation or partial
 *    delivery. WebRTC DataChannels do not support frame truncation; transmitting an oversized
 *    frame triggers an immediate RTCError / transport failure.
 * 6. SCTP Fragmentation Boundaries: Lower-layer SCTP packet fragmentation and reassembly handles
 *    wire-level packetization, but is NOT a substitute for application-level message-size compliance.
 * 7. Application-Level Slicing: Application-level pagination, slicing, and sub-batching into
 *    contiguous sequence ranges that strictly conform to the negotiated message-size envelope
 *    is an M5 (Transport Integration) responsibility.
 */
export const MAX_BATCH_PAYLOAD_BYTES = 65_536;
```

---

## 4. Test Verification & Accounting

### 4.1 Protocol Test Suite Accounting

| Test File | Preceding Phase 7.3 Baseline | Phase 7.4 M1 Initial | Phase 7.4 M1 Final Routing Revision | Net Growth Over Baseline |
| :--- | :--- | :--- | :--- | :--- |
| `envelope.test.ts` | 28 | 28 | 28 | 0 |
| `exports.test.ts` | 9 | 11 | 11 | +2 |
| `payloads.test.ts` | 57 | 139 | 151 | +94 |
| `validation.test.ts` | 47 | 76 | 83 | +36 |
| **`@syncplay/protocol` Total** | **141** | **254** | **273** | **+132 tests** |

### 4.2 Monorepo Test Suite Verification

| Package | Test Files | Tests Passed | Tests Failed | TypeScript / Build Status |
| :--- | :--- | :--- | :--- | :--- |
| `@syncplay/control-events` | 6 | 49 | 0 | `tsc` clean (0 errors) |
| `@syncplay/peer-mesh` | 6 | 101 | 0 | `tsc` clean (0 errors) |
| `@syncplay/protocol` | 4 | 273 | 0 | `tsc` clean (0 errors) |
| `@syncplay/sync-engine` | 17 | 241 | 0 | `tsc` clean (0 errors) |
| `@syncplay/bootstrap-server` | 3 | 91 | 0 | `tsc` clean (0 errors) |
| `@syncplay/demo` | — | — | — | `vite build` clean (1.55s, 0 errors) |
| **Monorepo Total** | **36** | **755** | **0** | **Clean (0 errors)** |

---

## 5. Architectural Invariants Preserved

1. **Frozen Phase 7.3 Engine Untouched:** Zero lines of code in `packages/sync-engine/src/engine/` were modified. Test fixtures in `packages/sync-engine/src/__tests__/` continue to provide conforming `requestId`, `sessionId`, and `membershipEpoch: 1`.
2. **Causal Model Untouched:** Lamport clocks, conflict resolution, causal context maps, and reduction invariants from Phase 7.2-R9 remain identical.
3. **Capability Boundaries Untouched:** Strict referential equality checks for `EpochTransitionCapability` and `TransportRegistrationCapability` (Phase 7.3-R5-H) are completely preserved.
4. **Milestone Boundary Discipline:** Zero code was written for M2 (Digest Exchange), M3 (Repair Request Pipeline), M4 (AntiEntropyManager/Supplier Servicing), M5 (Transport Integration), or Phase 7.5 (Snapshot Recovery).

---

## 6. Modified Files Inventory

- [packages/protocol/src/constants.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/constants.ts)
- [packages/protocol/src/payloads/repair.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/repair.ts)
- [packages/protocol/src/payloads/index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/index.ts)
- [packages/protocol/src/index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/index.ts)
- [packages/protocol/src/__tests__/exports.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/exports.test.ts)
- [packages/protocol/src/__tests__/payloads.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/payloads.test.ts)
- [packages/protocol/src/__tests__/validation.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/validation.test.ts)
- [docs/reports/PHASE7_4_M1_COMPLETION_REPORT.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/reports/PHASE7_4_M1_COMPLETION_REPORT.md)

---

**PHASE 7.4 M1 — FINAL INBOUND ROUTING AND SCOPE CORRECTIONS COMPLETE. AWAITING ARCHITECTURAL ACCEPTANCE AND EXPLICIT AUTHORIZATION FOR M2.**
