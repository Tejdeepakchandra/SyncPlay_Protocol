# Phase 7.5 M4.2c-R5 — Targeted Correction Report
## Active Attempt Coalescing & Callback Idempotency Closure

**Author:** Senior TypeScript Distributed-Systems Engineer  
**Baseline Commit:** `091ecaa5bc5cd98599bfc1b6b2d22a353c6f26a4` (M4.2c-R4)  
**Direct Parent Commit:** `2c7c2a9d1bd1816aefe483053845f8c8016a2110` (M4.2c-R3)  
**Frozen Baselines:**
- M4.1 Baseline: `fd23edae5bdb52cff32674860229277a79cb1aca`
- M4.2a Baseline: `e3245912626758c094d83edbd4d9264a922070f5`
- M4.2b Baseline: `8f509f94a39be9ece4060003ee498dc747efb813`
**Mode:** Targeted Implementation Correction & Adversarial Closure  
**Date:** November 23, 2026  

---

## 1. Executive Summary

Phase 7.5 Sub-Milestone M4.2c-R5 implements the targeted correctness closure for the three HIGH-severity lifecycle and concurrency defects identified in the independent read-only audit of M4.2c-R4:

1. **Finding D1 (Safe Active-Attempt Coalescing):** Prevented `handleChunkRequest()` from generating new attempt sequences or overwriting active attempts when a chunk already has an attempt actively pending transport handoff. Implemented the safe coalescing policy where repeated requests for an actively pending chunk produce no additional dispatchable handoff (`[]`), preserving the active attempt's `messageId`, `attemptSeq`, `attemptToken`, `serializedJson`, in-flight accounting, and pending callbacks.
2. **Finding D2 (Idempotent Successful Handoff Callbacks):** Added an entry guard in `handleHandoffSuccess()` checking `if (record.handedOffAttempts.has(attemptToken)) return;` before checking `active` or modifying state. Repeated success callbacks for already handed-off attempts are strict no-ops and never inflate `confirmedRetries` or `retryCounts`.
3. **Finding D3 (Failure-After-Success Immunity):** Added an entry guard in `handleHandoffFailure()` checking `if (record.handedOffAttempts.has(attemptToken)) return;`. Delayed or out-of-order failure callbacks for an attempt that already succeeded handoff are strict no-ops and can never unreserve in-flight status, delete active attempts, or roll back retries.

All previously accepted M4.2c-R4 invariants and frozen baselines (M4.1, M4.2a, M4.2b) remain 100% intact.

---

## 2. Root Cause Analysis & Detailed Corrections

### 2.1 Finding D1 — Active-Attempt Overwrite

#### Root Cause
In `handleChunkRequest()`, when a chunk index was requested, the pipeline checked whether `currentMessageId === undefined` or `currentMessageWasHandedOff`. However, if the chunk was previously requested and had returned a frame with attempt $T_1$, but the transport adapter had **not yet reported** either `handleHandoffSuccess` or `handleHandoffFailure`, the pipeline unconditionally incremented `attemptCounters` to generate $T_2$ and overwrote `record.activeAttempts.get(chunkIndex)`.
When the transport adapter subsequently completed the physical handoff of $T_1$ and invoked `handleHandoffSuccess(..., T1)`, the callback carried $T_1$, which no longer matched `active.attemptToken` ($T_2$). The callback was silently dropped as an invalid attempt, leaving the chunk stranded if $T_2$ was never physically dispatched.

#### Selected Coalescing Policy
Per protocol requirements, a chunk must not acquire a second independent active handoff attempt while its existing attempt is still active in transport dispatch. Furthermore, returning the existing prepared frame as another dispatchable frame could cause the transport adapter to transmit duplicate physical frames under the same attempt token.

Therefore, the pipeline implements **Safe Drop/Filter Coalescing**:
- When `handleChunkRequest()` encounters a `chunkIndex` where `record.activeAttempts.has(chunkIndex)` and `!record.handedOffAttempts.has(active.attemptToken)`:
  - The chunk is actively pending transport handoff.
  - The loop executes `continue;` without allocating a new attempt, without producing another frame, and without touching sliding-window capacity or retry reservations.
  - The method returns an array excluding the active chunk (returning `[]` if only that chunk was requested).
  - The transport adapter receives 0 frames to send, making duplicate dispatch physically impossible.
  - Original attempt's `messageId`, `attemptSeq`, `attemptToken`, `serializedJson`, `inflightChunks`, and pending callbacks remain 100% valid.

```typescript
// Safe Coalescing Policy (Finding D1):
// If this chunk already has an active attempt pending local handoff callback,
// produce no additional dispatchable handoff to prevent duplicate physical transmission
// and preserve the pending attempt's identity and callbacks intact.
const active = record.activeAttempts.get(chunkIndex);
if (active && !record.handedOffAttempts.has(active.attemptToken)) {
  continue;
}
```

---

### 2.2 Finding D2 — Duplicate Success Callback Idempotency

#### Root Cause
In `handleHandoffSuccess()`, when an active attempt was a network retransmission (`active.isRetransmission === true`), the pipeline unconditionally incremented `record.confirmedRetries` and `record.retryCounts`. Because the attempt remained in `activeAttempts` awaiting remote application ACK, duplicate success callbacks from transport retries or multi-channel notifications repeatedly incremented `confirmedRetries` and `retryCounts`, prematurely exhausting the transfer retry budget.

#### Correction
`handleHandoffSuccess()` now verifies whether `record.handedOffAttempts.has(attemptToken)` immediately after context and argument validation:

```typescript
// Finding D2 — Idempotent Success Callback Guard:
// If this exact attemptToken has ALREADY been successfully handed off,
// repeated success callbacks must be a strict NO-OP.
if (record.handedOffAttempts.has(attemptToken)) {
  return;
}

// Retired attempt guard: an attempt that failed cannot later succeed
if (record.failedAttempts.has(attemptToken)) {
  return;
}
```

If the token was already handed off, the callback returns immediately. `confirmedRetries`, `retryCounts`, `lastHandedOffMessageIds`, `inflightChunks`, and `activeAttempts` remain unchanged.

---

### 2.3 Finding D3 — Failure-After-Success Strict No-Op

#### Root Cause
When an attempt succeeded handoff, it remained in `record.activeAttempts` awaiting remote ACK. If the transport layer later emitted a delayed failure callback for that same attempt (e.g., from an internal retry or delayed error handler), `handleHandoffFailure()` matched `active.attemptToken`, deleted the attempt from `activeAttempts`, removed the chunk from `inflightChunks`, and rolled back retry reservations, corrupting the in-flight state of a message already queued in the network send buffer.

#### Correction
`handleHandoffFailure()` now verifies whether `record.handedOffAttempts.has(attemptToken)` before any mutation:

```typescript
// Finding D3 — Failure-After-Success Guard:
// If this exact attemptToken has ALREADY been successfully handed off,
// late failure callbacks must be a strict NO-OP.
// We must NOT un-reserve in-flight status, NOT delete active attempt, and NOT roll back retries!
if (record.handedOffAttempts.has(attemptToken)) {
  return;
}

// Duplicate Failure Guard:
// If this exact attemptToken was already marked as failed, repeated failure callback is a strict NO-OP.
if (record.failedAttempts.has(attemptToken)) {
  return;
}
```

If the attempt already succeeded handoff, the failure callback is dropped as a strict no-op, preserving the ACK-waiting lifecycle intact.

---

## 3. Before/After State-Transition Traces

### Trace 1: Concurrent Same-Chunk Request (Finding D1)

| Step | Operation | Before (M4.2c-R4) | After (M4.2c-R5) |
| :--- | :--- | :--- | :--- |
| 1 | `handleChunkRequest([0])` | Returns frame with $T_1 = M_1:1$; `activeAttempts = {0: T1}` | Returns frame with $T_1 = M_1:1$; `activeAttempts = {0: T1}` |
| 2 | `handleChunkRequest([0])` (concurrent) | Generates $T_2 = M_1:2$; overwrites `activeAttempts = {0: T2}`; returns 2nd frame | Coalesced: returns `[]` (0 frames); `activeAttempts = {0: T1}` untouched |
| 3 | `handleHandoffSuccess(..., T1)` | $T_1 \ne T_2$ mismatch $\to$ **DROPPED** (chunk stranded) | $T_1 = T_1$ match $\to$ **ACCEPTED**; `handedOffAttempts.add(T1)` |
| 4 | Transport Dispatch | Duplicate frame dispatched under $T_2$ | No duplicate frame dispatched; single physical send |

### Trace 2: Duplicate Success Callback on Retransmission (Finding D2)

| Step | Operation | Before (M4.2c-R4) | After (M4.2c-R5) |
| :--- | :--- | :--- | :--- |
| 1 | Retransmission $M_2$ requested | `retryCounts = 1`; `confirmedRetries = 0` | `retryCounts = 1`; `confirmedRetries = 0` |
| 2 | `handleHandoffSuccess(..., M2, T2)` | `confirmedRetries = 1`; `retryCounts = 1` | `confirmedRetries = 1`; `retryCounts = 1` |
| 3 | Duplicate `handleHandoffSuccess(..., M2, T2)` | `confirmedRetries = 2`; `retryCounts = 2` (**INFLATED**) | Idempotent guard $\to$ **STRICT NO-OP** (`confirmedRetries = 1`) |
| 4 | Subsequent Retransmission $M_3$ | `retryCounts = 3` (premature exhaustion) | `retryCounts = 2`; `confirmedRetries = 2` (exact accounting) |

### Trace 3: Failure Callback Arriving After Success (Finding D3)

| Step | Operation | Before (M4.2c-R4) | After (M4.2c-R5) |
| :--- | :--- | :--- | :--- |
| 1 | `handleHandoffSuccess(..., M1, T1)` | `handedOffAttempts = {T1}`; `inflight = {0}` | `handedOffAttempts = {T1}`; `inflight = {0}` |
| 2 | Late `handleHandoffFailure(..., M1, T1)` | Matches active; deletes $T_1$; deletes from `inflight` (**CORRUPTED**) | Guard catches $T_1 \in \text{handedOff}$; **STRICT NO-OP** |
| 3 | In-flight State | `inflightChunks.has(0) === false` (erroneous window reopening) | `inflightChunks.has(0) === true` (correct in-flight tracking) |
| 4 | Remote `handleChunkAck(0)` | Accepted; normal completion | Accepted; normal completion |

---

## 4. Verification & Test Results

### 4.1 Focused Test Suite (`snapshot-outbound-pipeline.test.ts`)
- **Command:** `npx vitest run packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts`
- **Result:** **89/89 passed** (0 failed, 0 skipped) across all test suites.
- **R5 Tests Added:**
  - `R5-D1-1`: repeated request while attempt T1 is active produces no additional dispatchable handoff and preserves T1.
  - `R5-D1-2`: repeated request while T1 is active preserves T1 and accepts T1 failure callback if failure occurs first.
  - `R5-D1-3`: duplicate polling cannot create additional retry reservations or message IDs.
  - `R5-D1-4`: batch requests coalesce active chunks while allowing inactive chunks to utilize sliding-window capacity.
  - `R5-D2-1`: duplicate success callbacks on initial transmission are idempotent and do not corrupt state.
  - `R5-D2-2`: duplicate success callbacks on retransmissions do not inflate confirmedRetries or retryCounts.
  - `R5-D3-1`: failure callback arriving after successful handoff on initial transmission is a strict no-op.
  - `R5-D3-2`: failure-after-success on retransmission preserves inflightChunks, retries, and active attempt.
  - `R5-ADV-1`: Failure $\to$ duplicate failure is idempotent and does not corrupt state.
  - `R5-ADV-2`: Failure $\to$ success using retired token is safely dropped.
  - `R5-ADV-3`: Success M1 $\to$ new retransmission M2 $\to$ delayed failure for M1 does not touch M2.
  - `R5-ADV-4`: Success $\to$ ACK $\to$ late success and failure callbacks are strict no-ops.
  - `R5-ADV-5`: Callbacks after pipeline disposal throw DISPOSED and do not mutate state.

### 4.2 Full Sync-Engine Test Suite
- **Command:** `npx vitest run packages/sync-engine/src/__tests__/`
- **Result:** **34/34 test files passed, 713/713 tests passed** (4.34 s).

### 4.3 Monorepo Test Suite
- **Command:** `npx vitest run`
- **Result:** **55/55 test files passed, 1,298/1,298 tests passed** (10.10 s).

### 4.4 Monorepo Production Build
- **Command:** `npm run build`
- **Result:** Exit code 0 across all 6 workspaces:
  - `@syncplay/protocol`: `tsc` clean (0 errors)
  - `@syncplay/peer-mesh`: `tsc` clean (0 errors)
  - `@syncplay/control-events`: `tsc` clean (0 errors)
  - `@syncplay/sync-engine`: `tsc` clean (0 errors)
  - `@syncplay/bootstrap-server`: `tsc` clean (0 errors)
  - `@syncplay/demo`: `vite build` completed in 2.04 s

### 4.5 Frozen Baseline Integrity
- `git diff fd23edae5bdb52cff32674860229277a79cb1aca -- packages/sync-engine/src/recovery/chunk-assembler.ts packages/sync-engine/src/recovery/snapshot-recovery.ts packages/sync-engine/src/snapshot.ts`: **0 diffs**
- `git diff e3245912626758c094d83edbd4d9264a922070f5 -- packages/sync-engine/src/recovery/snapshot-serializer.ts`: **0 diffs**
- `git diff 8f509f94a39be9ece4060003ee498dc747efb813 -- packages/sync-engine/src/recovery/snapshot-preparation-queue.ts packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`: **0 diffs**

---

## 5. Scope of Changes

Modifications are strictly limited to the outbound pipeline and its dedicated unit test file:
1. `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`:
   - Added safe coalescing guard in `handleChunkRequest()`.
   - Added idempotency guard in `handleHandoffSuccess()`.
   - Added failure-after-success guard in `handleHandoffFailure()`.
   - Updated `OutboundTransferSession` and `#toSessionView()` to expose readonly snapshots of attempt state.
2. `packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts`:
   - Updated test `R4-5` to assert safe coalescing semantics.
   - Added 13 new unit and adversarial regression tests in `describe('Phase 7.5 M4.2c-R5')`.
3. `docs/PHASE7_5_M4_2C_R5_TARGETED_CORRECTION_REPORT.md`:
   - Dedicated completion report.

---

## 6. Stop Condition & Next Steps

Targeted correction M4.2c-R5 is complete. All 1,298 tests in the monorepo pass cleanly, all builds succeed, and all frozen baselines are intact.

**STOP.** In accordance with protocol instructions, do not start M4.2d or M4.3. This commit is ready for independent read-only correctness audit.
