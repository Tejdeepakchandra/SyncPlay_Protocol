# Phase 7.4 Milestone M1: Protocol Schemas & Serialization Completion Report

**Milestone:** Phase 7.4 Milestone M1 — Protocol Schemas & Serialization  
**Phase:** 7.4 — State Replication & Anti-Entropy  
**Date:** 2026-11-17  
**Base Architecture Specification Commit:** `0289ad5` (`0289ad5a62e03264b30a10b429d29eb0ca1691a1`)  
**Approved Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Previous Implementation:** Phase 7.3-R5-H (`e49b81a`)  
**Scope Status:** Milestone M1 Implementation Complete — M2–M5 Strictly Deferred  

---

## Executive Summary

Milestone M1 establishes the protocol-level foundation for Phase 7.4 (State Replication & Anti-Entropy). All protocol message types, runtime Zod validation schemas, discriminator logic, WebRTC DataChannel sizing bounds, and serialization safety utilities have been implemented, tested, and integrated into `@syncplay/protocol`.

### Key Achievements

1. **Versioned Multi-Vector State Digest (`StateDigestPayloadSchema`):**
   - Features a strict `formatVersion: z.literal(2)` discriminator to prevent schema confusion with Phase 2 single-hash digests.
   - Encapsulates sender identity, connection generation, membership epoch, state version, causal replication vector, snapshot frontier, bounded history tail fingerprints ($\le 128$ hex entries), and transmission timestamp.
2. **Bounded Repair Request Schema (`RepairRequestPayloadSchema`):**
   - Enforces unique `requestId` correlation, valid session scoping, positive membership epoch, contiguous sequence ranges (`toSequence >= fromSequence`), and a hard upper bound of $\le 32$ operations per request.
3. **Correlated Operation Batch Schema (`OperationBatchPayloadSchema`):**
   - Supports request-driven repairs via optional `requestId?: string` and preserves backward compatibility with Phase 7.3 test suites via optional `sessionId?: string`.
   - Enforces a maximum batch size of 32 operations (`MAX_OPERATIONS_PER_BATCH`) and strict sequence bounds.
4. **Structured Repair Rejection Schema (`RepairRejectedPayloadSchema`):**
   - Provides machine-readable rejection reasons (`OUTSIDE_HORIZON`, `ORIGIN_UNKNOWN`, `RATE_LIMITED`, `EPOCH_MISMATCH`, `BUSY`) with an optional `retainedHorizonSequence` integer for horizon guidance.
5. **WebRTC Sizing & Serialization Safety (`constants.ts` & `validation.ts`):**
   - Defined `MAX_BATCH_PAYLOAD_BYTES = 65_536` (64 KB) in compliance with WebRTC DataChannel SCTP message limits (RFC 8831 / RFC 8841), allowing operation batches to use the full DataChannel payload envelope while maintaining the 32 KB limit on standard control messages.
   - Implemented `computeUtf8ByteLength` using `TextEncoder` for accurate multibyte character measurement.
   - Implemented `serializeAndCheckSize` with structured `SerializedPayload` returns and graceful handling of non-serializable values (`SERIALIZATION_FAILED`).
6. **100% Verification Across Monorepo:**
   - `@syncplay/protocol` test suite expanded to **203 tests** across 4 test files (+38 tests, 100% pass).
   - Full monorepo passes **685 tests** across 36 test files (0 failures).
   - TypeScript compilation (`tsc`) and Vite production build pass cleanly with 0 errors.
   - Frozen Phase 7.3 engine code was completely untouched.

---

## 1. Milestone Scope & Delivery Boundaries

Per the strict authorization instructions for Phase 7.4:
- **Delivered in M1:**
  - Protocol message types (`OPERATION_BATCH`, `REPAIR_REQUEST`, `REPAIR_REJECTED`, and `SYNC_MESSAGE_TYPES` wire equivalents).
  - Discriminator-based Zod schemas in `@syncplay/protocol` for State Digest (v2), Repair Request, Operation Batch, and Repair Rejection.
  - Sizing constants (`MAX_BATCH_PAYLOAD_BYTES = 65_536`, `MAX_OPERATIONS_PER_BATCH = 32`).
  - Validation engine updates in `validation.ts` and `result.ts`.
  - Type guards (`isStateDigestV2`, `isLegacyStateDigest`).
  - Serialization utilities (`computeUtf8ByteLength`, `serializeAndCheckSize`).
  - Unit and integration tests covering all 15 required test areas in `@syncplay/protocol`.
- **Deferred (Strictly Prohibited in M1):**
  - M2: Digest Exchange & Diff Detection (periodic ticker, comparison logic, RTT supplier ranking).
  - M3: Repair Request Pipeline & Out-of-Order Assembly (in-flight tracking, deduplication, retry queues).
  - M4: Supplier-Side Servicing & Horizon Management (rate limiting, queue limits, rejection responses).
  - M5: End-to-End Integration, Simulation & Verification (network partitions, multi-peer churn).
  - Phase 7.5: Production Hardening.

---

## 2. Schema Inventory & Field-by-Field Specification

### 2.1 State Digest Payload Schema (`formatVersion: 2`)

Exported as `StateDigestPayloadSchema` (alias `StateDigestPayloadV2Schema`). Type: `StateDigestPayload` (alias `StateDigestPayloadV2`).

| Field | Type | Constraints / Validation | Architectural Justification |
| :--- | :--- | :--- | :--- |
| `formatVersion` | `z.literal(2)` | Must equal literal `2` | Discriminator distinguishing Phase 7.4 multi-vector digests from legacy Phase 2 single-hash digests. |
| `sessionId` | `string` | `.min(1).max(128)` | Scopes digest to the active room session; prevents cross-session state leakage. |
| `senderPeerId` | `string` | `.min(1).max(64)` | Identifies the transmitting peer for supplier selection and vector attribution. |
| `connectionGeneration` | `number` | `int().positive()` | Enforces peer generation fencing; prevents stale pre-reconnection digests from polluting state. |
| `membershipEpoch` | `number` | `int().positive()` | Fences digests across membership transitions; digests with stale epochs are safely dropped. |
| `stateVersion` | `number` | `int().nonnegative()` | Monotonically non-decreasing snapshot/state version counter. |
| `replicationVector` | `Record<string, number>` | Keys: peer IDs, Values: `int().nonnegative()` | Causal frontier mapping each known origin peer ID to the highest contiguous sequence number received. |
| `snapshotFrontier` | `Record<string, number>` | Keys: peer IDs, Values: `int().nonnegative()` | Compacted history horizon mapping origin peer IDs to base snapshot sequence numbers. |
| `historyFingerprints` | `Record<string, string>` | Keys: peer IDs, Values: `hex string (16-64 chars)`. Max 128 entries. | Tail operation SHA-256 fingerprint digest for rapid causal divergence detection without transferring full logs. |
| `sentAt` | `number` | `finite().nonnegative()` | Epoch millisecond timestamp when digest was generated; used for RTT sampling and staleness checks. |

### 2.2 Repair Request Payload Schema

Exported as `RepairRequestPayloadSchema`. Type: `RepairRequestPayload`.

| Field | Type | Constraints / Validation | Architectural Justification |
| :--- | :--- | :--- | :--- |
| `requestId` | `string` | `.min(1).max(128)` | Unique UUID correlating this request with incoming operation batches or rejection responses. |
| `sessionId` | `string` | `.min(1).max(128)` | Validates session membership; prevents cross-room repairs. |
| `requesterPeerId` | `string` | `.min(1).max(64)` | Identifies the requesting peer. |
| `targetOriginPeerId` | `string` | `.min(1).max(64)` (aliased by `originPeerId`) | Origin peer whose causal sequence gap is being requested. |
| `fromSequence` | `number` | `int().positive()` | Inclusive start sequence of the requested missing range. |
| `toSequence` | `number` | `int().positive()` | Inclusive end sequence of the requested missing range. |
| `membershipEpoch` | `number` | `int().positive()` | Membership epoch under which repair is requested; prevents cross-epoch repair pollution. |
| *Refinements* | — | `toSequence >= fromSequence`<br>`(toSequence - fromSequence + 1) <= 32` | Prevents inverted sequence bounds and bounds maximum requested window to 32 operations per request. |

### 2.3 Operation Batch Payload Schema

Exported as `OperationBatchPayloadSchema`. Type: `OperationBatchPayload`.

| Field | Type | Constraints / Validation | Architectural Justification |
| :--- | :--- | :--- | :--- |
| `requestId` | `string?` | Optional, `.min(1).max(128)` | Correlates batch with a prior `RepairRequestPayload`. Optional for push-based repairs. |
| `sessionId` | `string?` | Optional, `.min(1).max(128)` | Scopes batch to session. Made optional to preserve backward compatibility with Phase 7.3 tests. |
| `supplierPeerId` | `string` | `.min(1).max(64)` | Identifies the serving peer supplying the historical operations. |
| `originPeerId` | `string` | `.min(1).max(64)` | Author/origin of the contained operations. |
| `fromSequence` | `number` | `int().positive()` | Inclusive lower sequence bound of operations in this batch. |
| `toSequence` | `number` | `int().positive()` | Inclusive upper sequence bound of operations in this batch. |
| `operations` | `Array<ControlOp>` | `.max(32)` | Array of serialized control operations ($\le 32$ items) ordered by sequence number. |
| *Refinements* | — | `toSequence >= fromSequence` | Enforces monotonically valid sequence intervals. |

### 2.4 Repair Rejected Payload Schema

Exported as `RepairRejectedPayloadSchema`. Type: `RepairRejectedPayload`.

| Field | Type | Constraints / Validation | Architectural Justification |
| :--- | :--- | :--- | :--- |
| `requestId` | `string` | `.min(1).max(128)` | Correlates rejection with the client's original `RepairRequestPayload`. |
| `sessionId` | `string` | `.min(1).max(128)` | Session context identifier. |
| `supplierPeerId` | `string` | `.min(1).max(64)` | Serving peer that declined or failed to service the repair request. |
| `targetOriginPeerId` | `string` | `.min(1).max(64)` (aliased by `originPeerId`) | Target origin peer of the unfulfilled repair request. |
| `fromSequence` | `number` | `int().positive()` | Requested start sequence. |
| `toSequence` | `number` | `int().positive()` | Requested end sequence. |
| `reason` | `enum` | `'OUTSIDE_HORIZON' \| 'ORIGIN_UNKNOWN' \| 'RATE_LIMITED' \| 'EPOCH_MISMATCH' \| 'BUSY'` | Standardized machine-readable rejection code for deterministic client fallback handling. |
| `retainedHorizonSequence` | `number?` | Optional, `int().positive()` | Provides lowest retained sequence when `OUTSIDE_HORIZON` occurs, guiding snapshot fallback. |

---

## 3. Message Type Integration & Registry Mapping

### 3.1 Registry Additions in `@syncplay/protocol`

In [message-types.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/message-types.ts):
```ts
export const MessageType = {
  // Existing Phase 1-7.3 types...
  PLAY: 'PLAY',
  PAUSE: 'PAUSE',
  SEEK: 'SEEK',
  RATE_CHANGE: 'RATE_CHANGE',
  PING: 'PING',
  PONG: 'PONG',
  STATE_REQUEST: 'STATE_REQUEST',
  STATE_SNAPSHOT: 'STATE_SNAPSHOT',
  STATE_DIGEST: 'STATE_DIGEST',
  // Phase 7.4 additions:
  OPERATION_BATCH: 'OPERATION_BATCH',
  REPAIR_REQUEST: 'REPAIR_REQUEST',
  REPAIR_REJECTED: 'REPAIR_REJECTED',
} as const;

export const SYNC_MESSAGE_TYPES = {
  EVENT: 'control:event',
  OPERATION_BATCH: 'sync:operation-batch',
  STATE_DIGEST: 'sync:state-digest',
  REPAIR_REQUEST: 'sync:repair-request',
  REPAIR_REJECTED: 'sync:repair-rejected',
} as const;
```

`KNOWN_MESSAGE_TYPES` now holds 17 registered message types:
1. `PLAY`
2. `PAUSE`
3. `SEEK`
4. `RATE_CHANGE`
5. `PING`
6. `PONG`
7. `STATE_REQUEST`
8. `STATE_SNAPSHOT`
9. `STATE_DIGEST`
10. `OPERATION_BATCH`
11. `REPAIR_REQUEST`
12. `REPAIR_REJECTED`
13. `control:event`
14. `sync:operation-batch`
15. `sync:state-digest`
16. `sync:repair-request`
17. `sync:repair-rejected`

### 3.2 Dual-Path Routing & Union Wire Schema

To maintain complete backward compatibility with Phase 2 test suites and early wire message formats while enforcing strict multi-vector verification on sync channels:
- `StateDigestWireSchema` was defined in `payloads/index.ts` as `z.union([StateDigestPayloadSchema, StateDigestPayloadV1Schema])`.
- In `PAYLOAD_SCHEMAS`:
  - `MessageType.STATE_DIGEST`: validates against `StateDigestWireSchema`. Legacy Phase 2 digests and Phase 7.4 V2 digests both parse successfully at the envelope layer.
  - `SYNC_MESSAGE_TYPES.STATE_DIGEST` (`sync:state-digest`): strictly validates against `StateDigestPayloadSchema` (`formatVersion: 2`).
  - Unsupported versions (e.g. `formatVersion: 3`) fail validation deterministically.

---

## 4. Discriminator and Versioning Design

### 4.1 Discriminator Architecture

The `formatVersion` discriminator solves the structural collision between legacy Phase 2 digests and modern multi-vector digests:

```mermaid
flowchart TD
    RAW["Inbound Wire Message (JSON)"]
    PARSE["parseAndValidateMessage()"]
    ENV["Envelope & Type Check"]
    DISCRIM{"formatVersion Field?"}
    V2["StateDigestPayloadSchema (formatVersion: 2)"]
    V1["StateDigestPayloadV1Schema (No formatVersion)"]
    REJ["Reject: Unsupported Version (e.g. formatVersion: 3)"]

    RAW --> PARSE
    PARSE --> ENV
    ENV -->|messageType: STATE_DIGEST| DISCRIM
    DISCRIM -->|formatVersion === 2| V2
    DISCRIM -->|undefined (legacy)| V1
    DISCRIM -->|formatVersion !== 2| REJ
```

### 4.2 Runtime Type Guards

Two type guards provide safe branching without runtime exceptions:
1. `isStateDigestV2(payload: unknown): payload is StateDigestPayloadV2`:
   Verifies `typeof payload === 'object'`, `payload !== null`, and `(payload as Record<string, unknown>).formatVersion === 2`.
2. `isLegacyStateDigest(payload: unknown): payload is LegacyStateDigestPayload`:
   Verifies `digestId` and `digest` strings exist, and `formatVersion` is `undefined`.

---

## 5. Serialization and Sizing Bounds

### 5.1 WebRTC DataChannel Capacity Rationale

RFC 8831 (SCTP-Based Media Transport on WebRTC) and RFC 8841 specify practical SCTP message sizing constraints. Standard control events are strictly bounded to `MAX_PAYLOAD_BYTES = 32_768` (32 KB).

However, anti-entropy operation batches require transporting up to 32 serialized historical events. To prevent false truncation:
- Defined `MAX_BATCH_PAYLOAD_BYTES = 65_536` (64 KB) in `constants.ts`.
- In `validateMessage()`, `maxPayloadBytes` is dynamically set to `MAX_BATCH_PAYLOAD_BYTES` for `OPERATION_BATCH` and `sync:operation-batch`, while maintaining `MAX_PAYLOAD_BYTES` (32 KB) for all other message types.

### 5.2 Accurate Multibyte UTF-8 Sizing (`computeUtf8ByteLength`)

In JavaScript, `'string'.length` counts UTF-16 code units, not serialized bytes. For example:
- `'café'.length` is 4, but UTF-8 encoded size is 5 bytes.
- `'🚀'.length` is 2, but UTF-8 encoded size is 4 bytes.
- `'日本語'.length` is 3, but UTF-8 encoded size is 9 bytes.

`computeUtf8ByteLength` uses `new TextEncoder().encode(str).byteLength`, ensuring that byte quotas are accurate regardless of Unicode characters, emojis, or international metadata.

### 5.3 Safe Serialization (`serializeAndCheckSize`)

`serializeAndCheckSize(payload, maxBytes)` provides a hardened serialization pipeline:
- Returns `{ ok: true, value: { json, byteLength } }` when within limit.
- Returns `{ ok: false, error: { code: 'PAYLOAD_TOO_LARGE', byteLength, maxBytes } }` when exceeding limit.
- Gracefully intercepts circular structures and `BigInt` values, returning `{ ok: false, error: { code: 'SERIALIZATION_FAILED' } }` without throwing uncaught runtime exceptions.

---

## 6. Test Verification & Results

### 6.1 Protocol Package Test Breakdown

The `@syncplay/protocol` test suite passes **203 tests across 4 test files** (0 failures):

1. [envelope.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/envelope.test.ts) — **28 tests**:
   - Protocol versioning, session IDs, peer IDs, logical event ID isolation, unknown field stripping.
2. [exports.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/exports.test.ts) — **11 tests**:
   - Verification of all 17 `KNOWN_MESSAGE_TYPES`, all exported schemas, and serialization helpers.
3. [payloads.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/payloads.test.ts) — **89 tests**:
   - Phase 1-2 controls, ping/pong, snapshots.
   - Legacy V1 state digest tests.
   - V2 state digest: formatVersion 2, negative sequence rejection, hex fingerprint validation, collection boundaries.
   - Repair request: 32-op range limit, inverted sequence rejection, positive epoch, single-op requests.
   - Operation batch: 32-op bound, 33-op rejection, optional requestId/sessionId compatibility.
   - Repair rejection: all 5 rejection reasons, retainedHorizonSequence integer validation, invalid reason rejection.
4. [validation.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/validation.test.ts) — **75 tests**:
   - Envelope validation for all 4 Phase 7.4 messages and wire formats.
   - Round-trip `parseAndValidateMessage` serialization.
   - Batch payload sizing between 32 KB and 64 KB accepted for `OPERATION_BATCH`.
   - Control message payload > 32 KB rejected for standard messages.
   - `computeUtf8ByteLength` ASCII, emoji, accented, CJK, and object tests.
   - `serializeAndCheckSize` boundary checks, circular object safety, BigInt safety.

### 6.2 Monorepo Verification Summary

| Package | Test Files | Passed | Failed | Build Status |
| :--- | :--- | :--- | :--- | :--- |
| `@syncplay/control-events` | 6 | 49 | 0 | `tsc` passed |
| `@syncplay/peer-mesh` | 6 | 101 | 0 | `tsc` passed |
| `@syncplay/protocol` | 4 | 203 | 0 | `tsc` passed |
| `@syncplay/sync-engine` | 17 | 241 | 0 | `tsc` passed |
| `@syncplay/bootstrap-server` | 3 | 91 | 0 | `tsc` passed |
| `@syncplay/demo` | — | — | — | `vite build` passed (1.39s) |
| **Monorepo Total** | **36** | **685** | **0** | **Clean (0 errors)** |

---

## 7. Architectural Invariants Verification

1. **Frozen Engine Invariants Intact:**
   Zero lines of code in `packages/sync-engine` were modified. All 241 sync-engine tests passed without modification.
2. **Phase 7.2-R9 Causal Invariants Preserved:**
   Lamport logical clock manager, ordered operation log, conflict resolver, causal context dependency checking, and deterministic replay remain identical to their approved baseline.
3. **Phase 7.3-R5-H Boundary Invariants Preserved:**
   Referential equality checking for `EpochTransitionCapability` and `TransportRegistrationCapability`, authenticated supplier contexts, and RFC 8785 canonical SHA-256 fingerprinting remain untouched and enforced.
4. **Forward Readiness for M2:**
   The schemas provided by M1 export all necessary types (`StateDigestPayloadV2`, `RepairRequestPayload`, `OperationBatchPayload`, `RepairRejectedPayload`) required for Milestone M2 (Digest Exchange & Diff Detection) to implement periodic anti-entropy timers and vector comparison.

---

## 8. Modified Files Inventory

All modifications were strictly confined to `packages/protocol`:
- [constants.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/constants.ts)
- [message-types.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/message-types.ts)
- [payloads/state.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/state.ts)
- [payloads/repair.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/repair.ts)
- [payloads/index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/index.ts)
- [result.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/result.ts)
- [validation.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/validation.ts)
- [index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/index.ts)
- [__tests__/exports.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/exports.test.ts)
- [__tests__/payloads.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/payloads.test.ts)
- [__tests__/validation.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/validation.test.ts)

---

## 9. Next Steps (Pending Review & Authorization)

Upon architectural review and explicit user authorization for Milestone M2:
1. Implement M2 Digest Generation in `LeaderlessSyncEngine`: compute multi-vector digests from `replicationVector`, `baseSnapshot`, and tail fingerprints.
2. Implement M2 Periodic Exchange Ticker: periodic timer with configured jitter emitting `sync:state-digest` wire messages.
3. Implement M2 Diff Detection: vector comparison identifying missing sequence intervals across peers.
4. Implement M2 Supplier Selection: RTT-ranked eligible supplier candidate selection avoiding stale/churning peers.

---

**PHASE 7.4 — M1 COMPLETE. AWAITING ARCHITECTURAL REVIEW AND EXPLICIT AUTHORIZATION FOR M2.**
