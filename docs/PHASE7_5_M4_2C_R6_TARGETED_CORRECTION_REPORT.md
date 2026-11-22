# Phase 7.5 M4.2c-R6 — Targeted Correction Report
## Public Session-View Mutable Object Leak Closure & Attempt Immutability

**Author:** Senior TypeScript Distributed-Systems Engineer  
**Baseline Commit:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5)  
**Direct Parent Commit:** `091ecaa5bc5cd98599bfc1b6b2d22a353c6f26a4` (M4.2c-R4)  
**Frozen Baselines:**
- M4.1 Baseline: `fd23edae5bdb52cff32674860229277a79cb1aca`
- M4.2a Baseline: `e3245912626758c094d83edbd4d9264a922070f5`
- M4.2b Baseline: `8f509f94a39be9ece4060003ee498dc747efb813`
**Mode:** Targeted Implementation Correction & Adversarial Closure  
**Date:** November 23, 2026  

---

## 1. Executive Summary

Phase 7.5 Sub-Milestone M4.2c-R6 implements the targeted correctness closure for the HIGH-severity encapsulation vulnerability identified in the independent read-only audit of M4.2c-R5:

- **Finding E1 (Public Session-View Mutable Object Leak):** In M4.2c-R5, `OutboundTransferSession.activeAttempts` was returned as `new Map(record.activeAttempts)`. While the Map container was a shallow copy, the nested value objects `{ messageId, attemptSeq, attemptToken, isRetransmission }` were plain object references that were neither cloned nor frozen. Consequently, callers obtaining `getTransfer()` or `getActiveTransfers()` could mutate attempt objects directly, altering the pipeline's internal state.

M4.2c-R6 establishes dual-layer immutability:
1. **Internal Immutability at Insertion:** Stored attempt records in `record.activeAttempts` are strictly frozen with `Object.freeze()` upon creation in `handleChunkRequest()`.
2. **Defensive Cloning and Freezing at Public Boundary:** In `#toSessionView()`, `activeAttempts` entries are mapped to distinct, newly allocated, frozen object instances (`Object.freeze({ ...attempt })`).
3. **Audit of All Session-View Fields:** Verified all 22 fields of `OutboundTransferSession`. `offerMessage` and its nested payload are now deep-frozen upon construction and session exposure.
4. **Adapter Contract Requirement B1:** Recorded explicit contractual requirements for the forthcoming Milestone M4.3 WebRTC DataChannel transport adapter.

All previously accepted M4.2c invariants (D1 active-attempt coalescing, D2 duplicate success idempotency, D3 failure-after-success immunity, retry accounting, Decision D8 retransmission identity) and frozen baselines (M4.1, M4.2a, M4.2b) remain 100% intact.

---

## 2. Root Cause Analysis & Detailed Corrections

### 2.1 Finding E1 — Public Session-View Mutable Object Leak

#### Root Cause
In `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`:
```typescript
activeAttempts: new Map(record.activeAttempts),
```
`record.activeAttempts` stores `{ messageId, attemptSeq, attemptToken, isRetransmission }`.
`new Map()` copied only the outer Map entries, preserving the identical object references for the attempt records. When external consumers called `getTransfer()` or `getActiveTransfers()`, accessing `session.activeAttempts.get(0)` gave direct access to the live internal attempt object. Mutating `attempt.attemptToken = 'tampered'` altered the internal record, breaking token matching for subsequent callbacks.

#### Implementation Correction
1. **Internal Storage Immutability:** In `handleChunkRequest()`, the attempt record is explicitly frozen upon creation:
   ```typescript
   // Rule 3: Track active attempt using at least (transferId, chunkIndex, messageId)
   const attemptRecord = Object.freeze({
     messageId,
     attemptSeq,
     attemptToken,
     isRetransmission,
   });
   record.activeAttempts.set(chunkIndex, attemptRecord);
   ```
2. **Public Boundary Defensive Copy:** In `#toSessionView()`, each attempt record is cloned and frozen:
   ```typescript
   activeAttempts: new Map(
     Array.from(record.activeAttempts.entries()).map(([chunkIndex, attempt]) => [
       chunkIndex,
       Object.freeze({ ...attempt }),
     ])
   ),
   ```
3. **TypeScript Type Enforcement:** Updated `OutboundTransferSession` and `InternalTransferRecord` interfaces:
   ```typescript
   readonly activeAttempts: ReadonlyMap<number, Readonly<{ messageId: string; attemptSeq: number; attemptToken: string; isRetransmission: boolean }>>;
   ```

---

## 3. Comprehensive Session-View Encapsulation Audit

Every field exposed by `#toSessionView()` was audited for reference sharing and mutability:

| Field Name | Type | Classification | Protection Mechanism |
| :--- | :--- | :--- | :--- |
| `transferId` | `string` | Primitive | Immutable |
| `recipientPeerId` | `string` | Primitive | Immutable |
| `supplierPeerId` | `string` | Primitive | Immutable |
| `membershipEpoch` | `number` | Primitive | Immutable |
| `connectionGeneration` | `number` | Primitive | Immutable |
| `sctpMaxMessageSize` | `number` | Primitive | Immutable |
| `state` | `OutboundTransferState` | Primitive Union | Immutable |
| `totalChunks` | `number` | Primitive | Immutable |
| `totalSizeBytes` | `number` | Primitive | Immutable |
| `nominalChunkSizeBytes` | `number` | Primitive | Immutable |
| `acknowledgedChunks` | `ReadonlySet<number>` | Defensive Copy | `new Set(record.acknowledgedChunks)` (primitive elements) |
| `inflightChunks` | `ReadonlySet<number>` | Defensive Copy | `new Set(record.inflightChunks)` (primitive elements) |
| `transmittedChunks` | `ReadonlySet<number>` | Defensive Copy | `new Set(record.transmittedChunks)` (primitive elements) |
| `retryCounts` | `ReadonlyMap<number, number>` | Defensive Copy | `new Map(record.retryCounts)` (primitive keys/values) |
| `confirmedRetries` | `ReadonlyMap<number, number>` | Defensive Copy | `new Map(record.confirmedRetries)` (primitive keys/values) |
| `activeAttempts` | `ReadonlyMap<number, ...>` | Defensive Copy | `new Map(...)` with newly cloned, `Object.freeze()` values |
| `handedOffAttempts` | `ReadonlySet<string>` | Defensive Copy | `new Set(record.handedOffAttempts)` (primitive string elements) |
| `failedAttempts` | `ReadonlySet<string>` | Defensive Copy | `new Set(record.failedAttempts)` (primitive string elements) |
| `offerMessage` | `ProtocolMessage<...>` | Deep Frozen Object | `Object.freeze()` with frozen `payload` and `snapshotFrontier` |
| `failureReason` | `SnapshotAbortReason` | Primitive / Undefined | Immutable |
| `failureDetail` | `string` | Primitive / Undefined | Immutable |
| `isTerminal` | `boolean` | Primitive | Immutable |

---

## 4. Architectural Note B1: M4.3 Transport Adapter Contract

The M4.2c-R5 independent audit noted that because safe coalescing drops repeated chunk requests while an attempt is actively pending handoff, abandoning a returned frame without calling `handleHandoffSuccess()` or `handleHandoffFailure()` leaves that chunk pending until the 60-second transfer timeout.

As M4.3 is dedicated to WebRTC DataChannel transport integration and coordinator logic, the transport adapter contract for Milestone M4.3 is formally defined with the following invariant:

> **Milestone M4.3 Transport Adapter Invariant (B1):**  
> Every `OutboundPreparedChunk` frame returned by `SnapshotOutboundPipeline.handleChunkRequest()` is exclusively owned by the transport adapter dispatch loop and **MUST terminate in exactly one local handoff outcome**:
> 1. `handleHandoffSuccess(transferId, chunkIndex, messageId, attemptToken, context)` upon successful acceptance into the SCTP DataChannel send buffer; OR
> 2. `handleHandoffFailure(transferId, chunkIndex, messageId, attemptToken, context)` upon send-buffer backpressure rejection, socket exception, serialization overshoot, cancellation, or abandoned dispatch.
> 
> The adapter dispatch loop must wrap dispatch in a `try/finally` or error boundary guaranteeing that `handleHandoffFailure()` is unconditionally called if dispatch cannot be completed.

---

## 5. Verification & Test Results

### 5.1 Focused Unit Suite (`snapshot-outbound-pipeline.test.ts`)
- **Command:** `npx vitest run packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts`
- **Result:** **94/94 passed** (0 failed, 0 skipped, 140 ms).
- **Dedicated R6 Regression Tests:**
  - `E1-A`: `activeAttempts` obtained from `getTransfer()` cannot be mutated to alter internal pipeline state (throws `TypeError`, internal state intact).
  - `E1-B`: `activeAttempts` obtained from `getActiveTransfers()` cannot be mutated to alter internal state.
  - `E1-C`: Defensive-copy identity: repeated session reads return distinct object instances (`attempt1 !== attempt2`), and clearing returned Map does not affect internal state.
  - `E1-D`: Legitimate callbacks succeed and maintain exact accounting after external mutation attempts.
  - `E1-E`: Regression coverage: `offerMessage` is deep-frozen and cannot be mutated externally (throws `TypeError`).

### 5.2 Full Sync-Engine Test Suite
- **Command:** `npx vitest run packages/sync-engine/src/__tests__/`
- **Result:** **34/34 test files passed, 718/718 tests passed** (4.50 s).

### 5.3 Monorepo Test Suite
- **Command:** `npx vitest run`
- **Result:** **55/55 test files passed, 1,303/1,303 tests passed** (7.29 s).

### 5.4 Monorepo Production Builds
- **Command:** `npm run build`
- **Result:** Exit code 0 across all 6 workspaces:
  - `@syncplay/protocol`: `tsc` clean (0 errors)
  - `@syncplay/peer-mesh`: `tsc` clean (0 errors)
  - `@syncplay/control-events`: `tsc` clean (0 errors)
  - `@syncplay/sync-engine`: `tsc` clean (0 errors)
  - `@syncplay/bootstrap-server`: `tsc` clean (0 errors)
  - `@syncplay/demo`: `vite build` completed in 1.26 s

### 5.5 Frozen Baseline Integrity
- M4.1 baseline (`fd23edae5bdb52cff32674860229277a79cb1aca`): **0 diffs**
- M4.2a baseline (`e3245912626758c094d83edbd4d9264a922070f5`): **0 diffs**
- M4.2b baseline (`8f509f94a39be9ece4060003ee498dc747efb813`): **0 diffs**

---

## 6. Scope of Changes

Modifications are strictly limited to the authorized files:
1. `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`:
   - Updated `activeAttempts` in `OutboundTransferSession` and `InternalTransferRecord` to use `Readonly<{ ... }>`.
   - Freezing attempt record in `handleChunkRequest()`.
   - Freezing `record.offerMessage` in `startTransfer()`.
   - Cloning and freezing `activeAttempts` value objects and `offerMessage` in `#toSessionView()`.
2. `packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts`:
   - Added 5 dedicated regression tests `E1-A` through `E1-E`.
3. `docs/PHASE7_5_M4_2C_R6_TARGETED_CORRECTION_REPORT.md`:
   - Technical completion report.

---

## 7. Stop Condition & Next Steps

Targeted correction M4.2c-R6 is complete. Finding E1 is closed. All 1,303 tests in the monorepo pass cleanly, all builds succeed, and all frozen baselines are intact.

**STOP.** Sub-milestone M4.2c is **NOT YET FROZEN**. In accordance with instructions, do not start M4.2d or M4.3. This commit is ready for an independent read-only correctness audit.
