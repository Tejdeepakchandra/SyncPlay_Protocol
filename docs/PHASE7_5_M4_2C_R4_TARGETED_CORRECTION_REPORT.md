# Phase 7.5 M4.2c-R4 — Final Targeted Correctness Closure Report
## Mandatory Attempt Tokens, Attempt-Identity Storage & Retransmission Lifecycle Correction

**Milestone:** Phase 7.5 Sub-Milestone M4.2c-R4  
**Implementation Baseline:** `2c7c2a9d1bd1816aefe483053845f8c8016a2110` (M4.2c-R3)  
**Direct Parent:** `8467123191c6d2bd7affbbdee649f8bf634f616f` (M4.2c-R2)  
**Frozen M4.2b Baseline:** `8f509f94a39be9ece4060003ee498dc747efb813`  
**Frozen M4.2a Baseline:** `e3245912626758c094d83edbd4d9264a922070f5`  
**Frozen M4.1 Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Role:** Senior TypeScript Distributed-Systems Engineer  
**Date:** November 22, 2026  

---

## 1. Executive Summary

This report documents the final targeted correctness closure for Phase 7.5 Sub-Milestone M4.2c (**M4.2c-R4**). Following the findings of the Phase 7.5 M4.2c-R3 independent read-only audit, three targeted defects were addressed:

1. **Mandatory Attempt Tokens (Finding A):** The public callback APIs `handleHandoffSuccess` and `handleHandoffFailure` now strictly mandate `attemptToken: string`. Legacy overloads and optionality have been eliminated. Omitted, undefined, null, empty (`""`), whitespace, or malformed tokens are rejected/safely ignored as strict no-ops. Both `messageId` and `attemptToken` must match the active attempt.
2. **Attempt-Identity Storage Keying (Finding B):** `handedOffAttempts` and `failedAttempts` now key and store individual handoff attempts by `attemptToken` (`${messageId}:${attemptSeq}`) rather than by bare `messageId`. Distinct local attempts sharing one `messageId` remain fully distinguishable and isolated.
3. **Deterministic Retransmission Classification & Retry Accounting (Finding C):** Retransmission detection is no longer inferred from frame return (`transmittedChunks`). A chunk transition is classified as a genuine network retransmission if and only if the current message allocated to that chunk was already successfully handed off to the transport send buffer (`lastHandedOffMessageIds.get(chunkIndex) === currentMessageId`). A fresh `messageId` and a retry budget increment occur strictly on genuine network retransmissions. Local handoff failures consume zero network retries, roll back uncompleted retry reservations, and preserve the `messageId` for subsequent local attempts.

---

## 2. Root Cause Analysis & Detailed Remedies

### 2.1 Finding A: Mandatory Attempt Tokens
- **Root Cause:** In M4.2c-R3, `attemptToken` was made optional (`attemptToken?: string | number`) in method signatures and runtime guards (`if (attemptToken !== undefined)`). When callbacks omitted `attemptToken`, validation fell back to matching on `messageId` alone. Under Rule 5, failed local handoffs preserve `messageId` for the next local attempt; thus, delayed duplicate failure callbacks for attempt $H_1$ matching on `messageId` alone erroneously released active attempt $H_2$.
- **Correction:**
  - Standardized public callback signatures:
    ```typescript
    public handleHandoffSuccess(
      transferId: string,
      chunkIndex: number,
      messageId: string,
      attemptToken: string,
      context: AuthenticatedRecipientContext
    ): void;

    public handleHandoffFailure(
      transferId: string,
      chunkIndex: number,
      messageId: string,
      attemptToken: string,
      context: AuthenticatedRecipientContext
    ): void;
    ```
  - Eliminated legacy 3-argument overloads.
  - Implemented strict token validation:
    ```typescript
    if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim() === '') {
      return; // Safe no-op
    }
    if (!messageId || typeof messageId !== 'string' || messageId.trim() === '') {
      return; // Safe no-op
    }
    const active = record.activeAttempts.get(chunkIndex);
    if (!active) {
      return;
    }
    if (active.messageId !== messageId || active.attemptToken !== attemptToken) {
      return; // Stale, duplicate, or mismatched: strict no-op
    }
    ```
  - Exported `OutboundPreparedChunk` interface extending `ProducedChunkMessage`:
    ```typescript
    export interface OutboundPreparedChunk extends ProducedChunkMessage {
      readonly messageId: string;
      readonly attemptSeq: number;
      readonly attemptToken: string;
    }
    ```

### 2.2 Finding B: Attempt-Identity Storage Keying
- **Root Cause:** `handedOffAttempts` and `failedAttempts` were sets of `string` keyed by `messageId`. When local attempts failed, multiple local attempts sharing the same `messageId` collapsed into a single set entry, conflating attempt history.
- **Correction:**
  - `handedOffAttempts: Set<string>` and `failedAttempts: Set<string>` now store the unique `attemptToken` (formatted as `${messageId}:${attemptSeq}`).
  - Because `attemptCounters.get(chunkIndex)` strictly increments on every frame request, each local attempt receives a globally distinct token.
  - On handoff failure: `record.failedAttempts.add(attemptToken)`. Failed tokens are permanently recorded and never deleted.
  - On handoff success: `record.handedOffAttempts.add(attemptToken)`.

### 2.3 Finding C: Retransmission Classification & Retry Accounting
- **Root Cause:** In M4.2c-R1 through R3, `isRetransmission` was evaluated as `record.transmittedChunks.has(chunkIndex)`. Because `transmittedChunks` was populated upon returning the prepared frame, requesting the frame a second time before handoff was reported was classified as a network retransmission, allocating a fresh `messageId` and consuming retry budget.
- **Correction:**
  - Added `lastHandedOffMessageIds: Map<number, string>` and `confirmedRetries: Map<number, number>` to `InternalTransferRecord`.
  - When `handleChunkRequest` is called for `chunkIndex`:
    - If `currentMessageId === undefined`: initial transmission. Enforces sliding window admission ($W=4$), allocates initial `messageId`, adds chunk to `transmittedChunks`. `isRetransmission = false`.
    - If `currentMessageId !== undefined`:
      - Evaluates `currentMessageWasHandedOff = (lastHandedOffMessageIds.get(chunkIndex) === currentMessageId)`.
      - **Genuine Network Retransmission (`currentMessageWasHandedOff === true`):**
        - Verifies `confirmedRetries < maxRetriesPerChunk` (aborts with `RETRY_BUDGET_EXHAUSTED` if exceeded).
        - Allocates fresh `messageId = msg-${generateUUID()}` per Decision D8.
        - Reserves retry: `record.retryCounts.set(chunkIndex, confirmedRetries + 1)`.
        - `isRetransmission = true`.
      - **Local Retry / Prior to Handoff (`currentMessageWasHandedOff === false`):**
        - Reuses `currentMessageId` per Rule 5.
        - Zero network retries consumed.
        - If the chunk was previously handed off (`handedOffChunks.has(chunkIndex)`), maintains active retransmission reservation (`confirmedRetries + 1`).
        - If initial transmission, re-asserts `transmittedChunks.add(chunkIndex)`.
  - On handoff failure:
    - If `active.isRetransmission`: rolls back uncompleted retry reservation (`retryCounts` set to `confirmedRetries`, or deleted if 0). Zero network retries consumed.
    - If initial transmission: rolls back `transmittedChunks.delete(chunkIndex)`.
  - On handoff success:
    - Sets `record.lastHandedOffMessageIds.set(chunkIndex, messageId)`.
    - Sets `record.handedOffChunks.add(chunkIndex)`.
    - If `active.isRetransmission`: increments `confirmedRetries = confirmedRetries + 1` and updates `retryCounts.set(chunkIndex, confirmedRetries)`.

---

## 3. Mandatory Attempt & Message Identity Lifecycle

The reconciled attempt and message identity lifecycle follows the deterministic sequence:

| Step | Operation | messageId | attemptToken | inflight | retryCounts | handedOff? | Classification |
|---|---|---|---|:---:|:---:|:---:|---|
| 1 | `handleChunkRequest([0])` | M1 | M1:1 | true | undefined | false | Initial local attempt |
| 2 | `handleHandoffFailure(..., M1, M1:1)` | M1 | — | false | undefined | false | Initial local failure (0 retries) |
| 3 | `handleChunkRequest([0])` | M1 | M1:2 | true | undefined | false | Initial local retry (preserves M1) |
| 4 | `handleHandoffSuccess(..., M1, M1:2)` | M1 | — | true | undefined | true (M1) | Initial successful handoff (0 retries) |
| 5 | `handleChunkRequest([0])` | M2 | M2:1 | true | 1 | false (M2) | 1st network retransmission (fresh M2) |
| 6 | `handleHandoffFailure(..., M2, M2:1)` | M2 | — | false | undefined | false (M2) | M2 local failure (reservation rolled back) |
| 7 | `handleChunkRequest([0])` | M2 | M2:2 | true | 1 | false (M2) | M2 local retry (preserves M2, 1 reserved) |
| 8 | `handleHandoffSuccess(..., M2, M2:2)` | M2 | — | true | 1 | true (M2) | M2 successful handoff (1 confirmed retry) |
| 9 | `handleChunkRequest([0])` | M3 | M3:1 | true | 2 | false (M3) | 2nd network retransmission (fresh M3) |

---

## 4. Adversarial Regression Test Suite (R4-1 Through R4-10)

Ten dedicated adversarial regression tests were added in `packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts` under `Phase 7.5 M4.2c-R4: Adversarial Attempt Identity & Retransmission Lifecycle Tests`:

- **R4-1 — Omitted Attempt Token:** Attempt $H_1$ fails. $H_2$ requested with same $M_1$ and token $T_2$. Stale callbacks for $H_1$ omitting `attemptToken` are safely ignored; $H_2$ in-flight reservation and transmission state remain completely unmutated.
- **R4-2 — Empty and Malformed Tokens:** Runtime fuzzing with `''`, `'   '`, `null`, `undefined`, numbers, objects, arrays, and booleans verifies that all malformed tokens are dropped without unhandled errors or state corruption.
- **R4-3 — Stale Success and Failure:** Callbacks carrying retired token $T_1$ cannot release, mutate, or prematurely mark active attempt $H_2$ as handed off.
- **R4-4 — Attempt-History Isolation:** $H_1$ (failed) and $H_2$ (handed off) share $M_1$, but their attempt histories in `failedAttempts` and `handedOffAttempts` remain strictly independent.
- **R4-5 — Frame Return Is Not Successful Handoff:** Requesting chunk 0 twice consecutively without reporting handoff preserves the exact same $M_1$, allocates zero new `messageId`s, and consumes zero retries.
- **R4-6 — Initial Failure and Retry:** Four repeated initial handoff failures preserve $M_1$ across 4 distinct attempt tokens, consuming zero network retries. Fifth attempt succeeds with 0 retries consumed.
- **R4-7 — Retransmission Local Failure:** $M_1$ succeeds. $M_2$ allocated as retransmission, but fails local handoff. Reservation rolls back to undefined. Subsequent local attempt reuses $M_2$ with token $T_3$ (does NOT create $M_3$). Upon success, confirmed retries = 1. Next genuine retransmission allocates $M_3$.
- **R4-8 — Multiple Retransmissions:** Verifies sequential successful handoffs for $M_1$ (initial), $M_2$ (retry 1), $M_3$ (retry 2), and $M_4$ (retry 3). 4th retransmission exceeds `maxRetriesPerChunk` (3) and aborts with `RETRY_BUDGET_EXHAUSTED`.
- **R4-9 — Delayed Callback After Newer Successful Attempt:** Stale failure for $M_1$ arriving after $M_2$ succeeds is a strict no-op. Full ACK lifecycle completes normally. Post-ACK late callbacks are harmless no-ops.
- **R4-10 — Context and Terminal Fencing:** Validates that callbacks with imposter peer, invalid session, stale membership epoch, or stale connection generation throw `UNAUTHORIZED`. Duplicate callbacks after `cancelTransfer` are harmless no-ops.

---

## 5. Verification Results

### 5.1 Focused Outbound Pipeline Tests
```
✓ packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts (76 tests) 147ms
Test Files  1 passed (1)
Tests       76 passed (76)
```

### 5.2 Full Sync-Engine Package Suite
```
Test Files  34 passed (34)
Tests       700 passed (700)
Duration    3.95s
```

### 5.3 Full Monorepo Test Suite
```
Test Files  55 passed (55)
Tests       1285 passed (1285)
Duration    7.88s
```

### 5.4 Production Build Verification
```
> npm run build --workspaces --if-present
✓ @syncplay/control-events@0.1.0 build (tsc)
✓ @syncplay/peer-mesh@0.1.0 build (tsc)
✓ @syncplay/protocol@0.1.0 build (tsc)
✓ @syncplay/sync-engine@0.1.0 build (tsc)
✓ @syncplay/bootstrap-server@0.1.0 build (tsc)
✓ @syncplay/demo@0.1.0 build (vite build)
Exit code: 0
```

### 5.5 Frozen Baseline Integrity
Git diff against frozen baselines:
- M4.1 (`chunk-assembler.ts`, `snapshot-recovery.ts`, `snapshot.ts`): **0 diffs**
- M4.2a (`snapshot-serializer.ts`): **0 diffs**
- M4.2b (`snapshot-preparation-queue.ts`, `snapshot-chunk-producer.ts`): **0 diffs**

---

## 6. Changed Files

| File | Changes | Purpose |
|---|:---:|---|
| `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts` | +65 / -53 | Mandate `attemptToken`, re-key attempt history, correct retransmission detection and retry rollback |
| `packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts` | +545 / -65 | Update callback call sites to 5 args; add adversarial tests R4-1 through R4-10 |
| `docs/PHASE7_5_M4_2C_R4_TARGETED_CORRECTION_REPORT.md` | +210 / -0 | Technical completion report for M4.2c-R4 |

---

## 7. Conclusion & Recommendation

All three findings from the M4.2c-R3 independent audit are closed:
- `attemptToken` is strictly mandatory with zero fallback to bare `messageId`.
- Attempt history is tracked by unique attempt tokens, eliminating conflation.
- Retransmission detection is grounded in confirmed handoffs, adhering strictly to Decision D8 and retry reservation rules.
- All 1,285 monorepo tests pass, with 0 changes to any frozen baselines.

**Status:** Ready for independent final audit. M4.2d and M4.3 remain strictly untouched.
