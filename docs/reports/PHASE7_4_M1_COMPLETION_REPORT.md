# Phase 7.4 Milestone M1: Protocol Schemas & Serialization — Final Structural Validation & Transport Contract Audit Report

**Milestone:** Phase 7.4 Milestone M1 — Protocol Schemas & Serialization (Final Structural Validation & Transport Contract Audit)  
**Phase:** 7.4 — State Replication & Anti-Entropy  
**Date:** 2026-11-17  
**Base Architecture Specification Commit:** `0289ad5` (`0289ad5a62e03264b30a10b429d29eb0ca1691a1`)  
**Parent Implementation Commit:** `2df6973` (`2df69735d4d38c64cf4f4751be6cb7ce3a2d53bf`)  
**M1 Audit Implementation Commit:** `d641fcd` (`d641fcda3ca2c99ae659b8be4da5f25ae7cf52db`)  
**Approved Architecture Baseline:** Phase 7.2-R9 (`1825de1`)  
**Frozen Previous Implementation:** Phase 7.3-R5-H (`e49b81a`)  
**Scope Status:** Milestone M1 Final Structural Validation Complete — M2–M5 Strictly Deferred  

---

## Executive Summary

This report documents the completion of the final structural validation and transport contract audit for **Milestone M1 (Protocol Schemas & Serialization)** of Phase 7.4 (State Replication & Anti-Entropy).

Following the senior distributed-systems architectural audit, four focused corrections were implemented and verified across `@syncplay/protocol` and the monorepo test suite:
1. **OperationBatch Exact Sequence Coverage & Contiguity Invariant:** Implemented complete contiguous sequence coverage refinement (`validateContiguousSequenceBatch`) on `OperationBatchPayloadSchema` and `PushOperationBatchPayloadSchema`, enforcing strict ascending sequence order, zero gaps, zero duplicates, origin/epoch matching across all operations, exact count matching, and formal prohibition of empty batches.
2. **MAX_VECTOR_ENTRIES (64) Lifecycle Semantics:** Formally defined the distributed-systems lifecycle for origin entries in replication vectors, snapshot frontiers, and history fingerprints across peer departures, eviction, rejoins, capacity boundaries (65th peer), and causal dependency tracking, backed by schema-level boundary tests.
3. **Correct WebRTC Negotiated Message-Size API Documentation:** Corrected all API references in constants and documentation to accurately cite the standard W3C WebRTC 1.0 attribute `RTCPeerConnection.sctp?.maxMessageSize` (on `RTCSctpTransport`) instead of `RTCDataChannel`, clarifying lower-layer SCTP fragmentation versus application-level payload limits.
4. **Compatibility Schema Isolation & Inbound Wire Validation:** Proved that `CompatibilityOperationBatchPayloadSchema` is strictly isolated to Phase 7.3 backward-compatibility tests, that `PAYLOAD_SCHEMAS` maps `OPERATION_BATCH` exclusively to `OperationBatchPayloadSchema`, and that inbound wire validation rejects batches lacking `requestId` or `sessionId` without fallback.

### Verification Summary
- **`@syncplay/protocol` Test Suite:** **275 tests passed** across 4 test files (+134 tests over the frozen Phase 7.3 baseline of 141 tests).
- **Monorepo Test Suite:** **757 tests passed** across 36 test files with **zero failures**.
- **Monorepo Production Builds:** All 5 TypeScript packages and the Vite client demo compiled with **zero errors**.
- **Frozen Engine Code:** Zero lines of code in `packages/sync-engine/src/engine/` were modified.
- **Milestone Scope Integrity:** Zero lines of M2, M3, M4, M5, or Phase 7.5 were implemented.

---

## 1. Targeted Audit Corrections Detail

### 1.1 OperationBatch Exact Sequence Coverage & Contiguity Invariant

#### Architectural Concern
Previously, `OperationBatchPayloadSchema` validated only that `toSequence >= fromSequence` and `operations.length <= 32`. A malicious or buggy peer could transmit:
- Batches with duplicate sequence numbers (e.g., sequences `[1, 1]`).
- Batches with missing internal sequences (e.g., sequences `[1, 3]` claiming range `1..3`).
- Batches with out-of-order operations (e.g., `[2, 1]`).
- Operations belonging to a different `originPeerId` or mismatched `membershipEpoch`.
- Empty batches (`operations.length === 0`) masquerading as valid sequence coverage.

#### Implementation & Refinement
We added explicit `membershipEpoch: z.number().int().positive().max(Number.MAX_SAFE_INTEGER)` to both `OperationBatchPayloadSchema` and `PushOperationBatchPayloadSchema` and implemented `validateContiguousSequenceBatch` attached via `.superRefine()`:

```typescript
export interface ContiguousSequenceBatchData {
  originPeerId: string;
  membershipEpoch: number;
  fromSequence: number;
  toSequence: number;
  operations: ReadonlyArray<SerializedControlOperation>;
}

export function validateContiguousSequenceBatch(
  data: ContiguousSequenceBatchData,
  ctx: z.RefinementCtx,
): void {
  // 1. Range bounds check
  if (data.toSequence < data.fromSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `toSequence (${data.toSequence}) must be greater than or equal to fromSequence (${data.fromSequence})`,
      path: ['toSequence'],
    });
    return;
  }

  // 2. Empty batch prohibition
  if (data.operations.length === 0) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message:
        'Empty operation batches are prohibited. Suppliers unable to satisfy requests must send REPAIR_REJECTED with an appropriate reason code.',
      path: ['operations'],
    });
    return;
  }

  // 3. Expected count based on declared sequence range
  const expectedCount = data.toSequence - data.fromSequence + 1;

  // 4. Maximum batch size check
  if (expectedCount > MAX_OPERATIONS_PER_BATCH) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Batch sequence range ${data.fromSequence}..${data.toSequence} (${expectedCount} operations) exceeds maximum allowed ${MAX_OPERATIONS_PER_BATCH}`,
      path: ['toSequence'],
    });
    return;
  }

  // 5. Operation count exact match
  if (data.operations.length !== expectedCount) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Operation count (${data.operations.length}) does not match declared sequence range ${data.fromSequence}..${data.toSequence} (expected ${expectedCount})`,
      path: ['operations'],
    });
  }

  // 6. First operation sequence matches fromSequence
  const firstOp = data.operations[0];
  if (firstOp && firstOp.originSequenceNumber !== data.fromSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `First operation originSequenceNumber (${firstOp.originSequenceNumber}) does not match fromSequence (${data.fromSequence})`,
      path: ['operations', 0, 'originSequenceNumber'],
    });
  }

  // 7. Last operation sequence matches toSequence
  const lastOp = data.operations[data.operations.length - 1];
  if (lastOp && lastOp.originSequenceNumber !== data.toSequence) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Last operation originSequenceNumber (${lastOp.originSequenceNumber}) does not match toSequence (${data.toSequence})`,
      path: ['operations', data.operations.length - 1, 'originSequenceNumber'],
    });
  }

  // 8. Strict contiguous sequence, origin matching, and epoch matching for every operation
  for (let i = 0; i < data.operations.length; i++) {
    const op = data.operations[i];
    if (!op) continue;
    const expectedSeq = data.fromSequence + i;

    if (op.originSequenceNumber !== expectedSeq) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has originSequenceNumber ${op.originSequenceNumber}, expected strictly contiguous ${expectedSeq}`,
        path: ['operations', i, 'originSequenceNumber'],
      });
    }

    if (op.originPeerId !== data.originPeerId) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has originPeerId '${op.originPeerId}', which does not match batch originPeerId '${data.originPeerId}'`,
        path: ['operations', i, 'originPeerId'],
      });
    }

    if (op.membershipEpoch !== data.membershipEpoch) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Operation at index ${i} has membershipEpoch ${op.membershipEpoch}, which does not match batch membershipEpoch ${data.membershipEpoch}`,
        path: ['operations', i, 'membershipEpoch'],
      });
    }
  }
}
```

#### Empty Batch Handling Semantics
- **Decision:** In SyncPlay anti-entropy, empty operation batches are **strictly prohibited** at schema validation.
- **Protocol Flow:** If a supplier receives a `REPAIR_REQUEST` and has no operations to serve (e.g. because operations are outside its retention horizon, the origin is unobserved, or it is operating on a different epoch), the supplier **MUST NOT** transmit an empty `OPERATION_BATCH`. Instead, it must send a `REPAIR_REJECTED` payload with an explicit machine-readable reason code:
  - `'OUTSIDE_HORIZON'`: Requested sequences have already been compacted out of the supplier's log.
  - `'ORIGIN_UNKNOWN'`: Supplier has never observed the requested origin peer.
  - `'RATE_LIMITED'`: Supplier is shedding repair load.
  - `'EPOCH_MISMATCH'`: Supplier is operating on a different membership epoch.
  - `'BUSY'`: Supplier queue is full or undergoing catch-up.

#### Valid Partial Contiguous Batches
If a requester asks for sequences `1..10`, but a supplier can only serve the first 5 operations (e.g. due to MTU size limits or local log boundaries), the supplier sends `fromSequence: 1, toSequence: 5, operations: [op1..op5]`. Because `expectedCount` is $5 - 1 + 1 = 5$ and `operations.length === 5`, this valid contiguous partial response is fully accepted by schema validation.

---

### 1.2 MAX_VECTOR_ENTRIES (64) Lifecycle Semantics

The schemas enforce `MAX_VECTOR_ENTRIES = 64` on `ReplicationVectorSchema`, `SnapshotFrontierSchema`, and `HistoryFingerprintsSchema`. The distributed-systems lifecycle semantics are defined below:

#### 1. Retention Duration Upon Member Departure
When a room member leaves or disconnects, its origin entry in `replicationVector`, `snapshotFrontier`, and `historyFingerprints` is **retained indefinitely** during active session operation. The departed peer's sequence number remains frozen at its last applied sequence number. This guarantees that surviving peers can evaluate causal dependencies referencing operations emitted by that peer prior to departure.

#### 2. Exact Conditions for Origin Entry Removal
An origin entry can **ONLY** be removed from the vector collections when all three of the following conditions are simultaneously met:
1. **Compaction Horizon Condition:** All historical operations authored by that origin have been incorporated into the stable base snapshot state and compacted out of the local ordered log (`snapshotFrontier[origin] == replicationVector[origin]`).
2. **Zero Live Dependency Condition:** No retained operation in the log or in the pending dependency queue retains a causal dependency on that origin (`∀ op ∈ log, op.causalContext[origin] ≤ snapshotFrontier[origin]`).
3. **Epoch Barrier & Membership Consensus Condition:** A membership epoch transition has occurred where the departed peer was formally excluded from the membership roster, and all surviving active members have acknowledged the new membership epoch. At this epoch barrier, the origin's historical causal contribution is fully subsumed by the base snapshot.

#### 3. Standard Compaction vs. Base Snapshot Compaction
- **Sliding-window log compaction** does **NOT** remove the origin entry from the vector; it only advances `snapshotFrontier[origin]`.
- Complete origin entry removal only occurs at a global base snapshot checkpoint (Phase 7.5).
- If a late operation arrives from an evicted origin, the engine rejects it with `REJECTED_STALE_EPOCH` / `REJECTED_INVALID`.
- If a repair request arrives referencing an evicted origin, the supplier responds with `REPAIR_REJECTED` (`reason: 'OUTSIDE_HORIZON'` or `'ORIGIN_UNKNOWN'`).

#### 4. Peer Departure, Eviction, and Rejoining
In SyncPlay (Phase 7.2-R9 §2.3 and Phase 7.3-R3), peer IDs are ephemeral per-connection cryptographic identifiers (`peerId = crypto.randomUUID()`). A rejoining user/client connects with a fresh `PeerId` and a new connection generation. It **never re-assumes the evicted origin identity**. Any gap between the rejoining peer's old state and current room state is bridged by fetching the current base snapshot and current replication vector via Phase 7.5 snapshot catch-up.

#### 5. Room Boundary at 64 Origins (65th Peer Joining)
If a room has observed operations from 64 unique peers and a 65th peer attempts to emit an operation:
1. **Network Boundary:** Any inbound `STATE_DIGEST` carrying $> 64$ vector entries is rejected immediately by Zod validation (`INVALID_PAYLOAD`).
2. **Engine Boundary:** The engine **NEVER silently evicts an active origin entry**, because silent eviction breaks causal dependency tracking and causes split-brain divergence across nodes.
3. **Degraded State:** The engine enters an explicit **Degraded State** (`isDegraded = true`, `degradedReason = 'MAX_ORIGINS_EXCEEDED'`).
4. **Operation Rejection:** New operations from unobserved origins are rejected with `REJECTED_CAPACITY`, and the engine signals the application layer / coordinator to initiate a forced base snapshot compaction and membership pruning cycle (Phase 7.5).
5. **Practical Sizing:** Standard SyncPlay rooms operate with 4–10 concurrent peers (default: 4, bootstrap server ceiling: 50). A limit of 64 accommodates all active peers plus over $5\times$ churn before compaction is triggered.

#### 6. Interaction with Causal Dependency Tracking
Because eviction is strictly conditioned on **Zero Live Dependencies**, no active or retained operation can reference an evicted origin outside the compacted snapshot horizon. In `PendingDependencyQueue`, dependencies $\le$ the base snapshot horizon are evaluated as satisfied by the snapshot. If an origin entry were unsafely evicted while retained operations depended on it, `PendingDependencyQueue.isDependencySatisfied()` would find no entry and permanently stall. Thus, unconditional vector retention until global compaction is mathematically necessary for causal liveness.

#### 7. Schema Boundary Verification
- Standalone and digest tests verify that collections with exactly 64 entries are accepted.
- Collections with 65 entries are rejected with descriptive Zod errors:
  - `Replication vector cannot exceed 64 origin entries`
  - `Snapshot frontier cannot exceed 64 origin entries`
  - `History fingerprints cannot exceed 64 origin entries`

---

### 1.3 Correct WebRTC Negotiated Message-Size API

#### Correction Detail
In `packages/protocol/src/constants.ts`, the documentation comments previously referenced `RTCDataChannel.maxMessageSize`. Standard W3C WebRTC 1.0 does not define `maxMessageSize` on `RTCDataChannel`; rather, the negotiated maximum incoming message size is defined on the underlying SCTP transport:

$$\text{Negotiated Message Size} = \text{RTCPeerConnection.sctp?.maxMessageSize}$$

The documentation was updated to reflect the exact W3C WebRTC 1.0 specification:
```typescript
/**
 * Maximum application payload size for anti-entropy operation batches (64 KB = 65,536 bytes).
 *
 * NOTE: This is an application-level batch payload cap derived from RFC 8831 / RFC 8841
 * SCTP interoperability recommendations, NOT a universal browser WebRTC guarantee.
 *
 * Standard WebRTC 1.0 (W3C) defines negotiated message size limits on the underlying
 * SCTP transport: `RTCPeerConnection.sctp?.maxMessageSize` (or the default 65,536 bytes if not signaled),
 * rather than on `RTCDataChannel`. The SCTP layer handles lower-level packet fragmentation
 * and reassembly, but application-level frames exceeding the remote peer's negotiated
 * `sctp?.maxMessageSize` will truncate or fail at the transport boundary.
 *
 * In Milestone M5 (Transport Integration), the transport adapter layer must inspect
 * `RTCPeerConnection.sctp?.maxMessageSize` (handling undefined gracefully with 65,536 fallback)
 * and paginate, slice, or sub-batch frames accordingly so that the serialized envelope
 * stays strictly within min(65,536, sctp.maxMessageSize).
 */
export const MAX_BATCH_PAYLOAD_BYTES = 65_536;
```

---

### 1.4 Compatibility Schema Isolation & Inbound Wire Validation

#### Schema Registration Verification
In `packages/protocol/src/payloads/index.ts`, `PAYLOAD_SCHEMAS` maps message types to schemas:
```typescript
  [MessageType.OPERATION_BATCH, OperationBatchPayloadSchema],
  [SYNC_MESSAGE_TYPES.OPERATION_BATCH, OperationBatchPayloadSchema],
```
`CompatibilityOperationBatchPayloadSchema` is **NOT** registered in `PAYLOAD_SCHEMAS`. It is exported solely for backward-compatible test fixtures in `packages/sync-engine/src/__tests__/`.

#### Inbound Wire Rejection Test Verification
In `packages/protocol/src/__tests__/validation.test.ts`, dedicated unit tests verify:
1. `PAYLOAD_SCHEMAS.get(MessageType.OPERATION_BATCH)` is strictly `OperationBatchPayloadSchema`.
2. An inbound `OPERATION_BATCH` message lacking `requestId` is rejected with `INVALID_PAYLOAD` and does **not** fall back to the compatibility schema.
3. An inbound `OPERATION_BATCH` message lacking `sessionId` is rejected with `INVALID_PAYLOAD` and does **not** fall back to the compatibility schema.
4. An inbound `OPERATION_BATCH` message lacking both `requestId` and `sessionId` is rejected with `INVALID_PAYLOAD`.

---

## 2. Test Verification & Accounting

### 2.1 Protocol Test Suite Accounting

| Test File | Preceding Phase 7.3 Baseline | Phase 7.4 M1 Initial | Phase 7.4 M1 Final Audit | Net Growth Over Baseline |
| :--- | :--- | :--- | :--- | :--- |
| `envelope.test.ts` | 28 | 28 | 28 | 0 |
| `exports.test.ts` | 9 | 11 | 11 | +2 |
| `payloads.test.ts` | 57 | 139 | 156 | +99 |
| `validation.test.ts` | 47 | 76 | 80 | +33 |
| **`@syncplay/protocol` Total** | **141** | **254** | **275** | **+134 tests** |

### 2.2 Monorepo Full Test Suite Verification

| Package | Test Files | Tests Passed | Tests Failed | TypeScript / Build Status |
| :--- | :--- | :--- | :--- | :--- |
| `@syncplay/control-events` | 6 | 49 | 0 | `tsc` passed |
| `@syncplay/peer-mesh` | 6 | 101 | 0 | `tsc` passed |
| `@syncplay/protocol` | 4 | 275 | 0 | `tsc` passed |
| `@syncplay/sync-engine` | 17 | 241 | 0 | `tsc` passed |
| `@syncplay/bootstrap-server` | 3 | 91 | 0 | `tsc` passed |
| `@syncplay/demo` | — | — | — | `vite build` passed (1.21s) |
| **Monorepo Total** | **36** | **757** | **0** | **Clean (0 errors)** |

---

## 3. Preserved Architecture Invariants

1. **Frozen Phase 7.3 Implementation Untouched:**
   Zero lines of code in `packages/sync-engine/src/engine/` were modified. Test fixtures in `packages/sync-engine/src/__tests__/` were updated with conforming `membershipEpoch: 1`.
2. **Phase 7.2-R9 Causal Ordering Untouched:**
   Lamport logical clock update rules, total conflict resolution order, and causal dependency tracking remain identical to the approved baseline.
3. **Phase 7.3-R5-H Capability Boundaries Untouched:**
   Exact referential identity checking for `EpochTransitionCapability` and `TransportRegistrationCapability` remains active and verified.
4. **Strict Scope Discipline Maintained:**
   Zero code was written for M2 (Digest Exchange), M3 (Repair Request Pipeline), M4 (AntiEntropyManager/Supplier Servicing), M5 (Transport Integration), or Phase 7.5 (Snapshot Recovery).

---

## 4. Modified Files Inventory

- [packages/protocol/src/constants.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/constants.ts)
- [packages/protocol/src/payloads/repair.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/repair.ts)
- [packages/protocol/src/payloads/index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/payloads/index.ts)
- [packages/protocol/src/index.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/index.ts)
- [packages/protocol/src/__tests__/exports.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/exports.test.ts)
- [packages/protocol/src/__tests__/payloads.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/payloads.test.ts)
- [packages/protocol/src/__tests__/validation.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/protocol/src/__tests__/validation.test.ts)
- [packages/sync-engine/src/__tests__/closure-r5g.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/__tests__/closure-r5g.test.ts)
- [packages/sync-engine/src/__tests__/closure-r5h.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/__tests__/closure-r5h.test.ts)
- [packages/sync-engine/src/__tests__/deterministic-simulation.test.ts](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/__tests__/deterministic-simulation.test.ts)
- [docs/reports/PHASE7_4_M1_COMPLETION_REPORT.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/reports/PHASE7_4_M1_COMPLETION_REPORT.md)

---

**PHASE 7.4 M1 — FINAL STRUCTURAL VALIDATION COMPLETE. AWAITING ARCHITECTURAL ACCEPTANCE AND EXPLICIT AUTHORIZATION FOR M2.**
