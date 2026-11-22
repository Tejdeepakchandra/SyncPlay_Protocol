# Phase 7.5 M4.2c-R4 — Independent Final Implementation Audit Report
## Mandatory Attempt Tokens, Concurrent Attempt Overwrite & Callback Race Audit

**Audit Target Commit:** `091ecaa5bc5cd98599bfc1b6b2d22a353c6f26a4`  
**Direct Parent Commit:** `2c7c2a9d1bd1816aefe483053845f8c8016a2110` (M4.2c-R3)  
**Frozen M4.2b Baseline:** `8f509f94a39be9ece4060003ee498dc747efb813`  
**Frozen M4.2a Baseline:** `e3245912626758c094d83edbd4d9264a922070f5`  
**Frozen M4.1 Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Audit Role:** Independent Senior TypeScript Distributed-Systems Engineer  
**Audit Mode:** Strictly Read-Only  
**Date:** November 22, 2026  

---

## 1. Executive Summary & Audit Verdict

A strict, adversarial, independent read-only implementation and specification audit was conducted on commit `091ecaa5bc5cd98599bfc1b6b2d22a353c6f26a4` (Phase 7.5 Sub-Milestone M4.2c-R4).

The audit verified that commit `091ecaa` correctly implements mandatory attempt token validation in public TypeScript APIs and runtime guards, keys attempt histories by unique attempt tokens, and successfully demonstrates the full $M_1 \to M_2 \to M_3$ retransmission lifecycle with zero network retries consumed on local failures.

However, deep adversarial analysis and empirical execution revealed **two high-severity callback race defects** and **one architectural concurrency defect**:

1. **Concurrent Same-Chunk Requests Overwrite Active Attempt (HIGH):** When `handleChunkRequest()` is called repeatedly for a chunk while an earlier handoff attempt is still active in the transport adapter, the pipeline generates a new `attemptSeq` and unconditionally overwrites `record.activeAttempts.get(chunkIndex)`. When the first in-flight handoff completes, its callback carries the original token $T_1$, which is rejected as mismatched and **silently dropped**, stranding the chunk if the second attempt is never dispatched.
2. **Duplicate Success Callbacks Inflate Retry Accounting (HIGH):** `handleHandoffSuccess()` does not verify whether `record.handedOffAttempts.has(attemptToken)` before incrementing `confirmedRetries` on active retransmissions. Consequently, duplicate transport success callbacks inflate `confirmedRetries` and `retryCounts`, prematurely exhausting the retry budget.
3. **Failure Callback After Success Corrupts In-Flight State (HIGH):** If a late or out-of-order failure callback arrives for an attempt that already succeeded (`handedOffAttempts.has(attemptToken)`), `handleHandoffFailure()` does not check if the attempt already succeeded. It removes the chunk from `inflightChunks` and rolls back retry accounting, corrupting state for a message already accepted into the network send buffer.

### Audit Verdict Summary

| Evaluation Area | Audit Result | Severity | Core Finding |
| :--- | :---: | :---: | :--- |
| **Commit Provenance & Baselines** | **PASS** | — | Clean git status, exact parent `2c7c2a9`, 0 diffs across frozen M4.1, M4.2a, M4.2b. |
| **Finding A — Mandatory Attempt Tokens** | **PASS** | — | `attemptToken: string` is mandatory in TypeScript signatures and runtime validation. No fallback to bare `messageId`. |
| **Finding B — Attempt Storage Keying** | **PASS** | — | `handedOffAttempts` and `failedAttempts` store `${messageId}:${attemptSeq}`, preserving attempt history isolation. |
| **Finding C — Retransmission Lifecycle** | **PASS** | — | $M_1 \to M_2 \to M_3$ sequence strictly follows Decision D8 and confirmed handoffs. Local failures consume 0 retries. |
| **Finding D1 — Concurrent Same-Chunk Requests** | **FAIL** | **HIGH** | Duplicate request for an active chunk overwrites `activeAttempts` slot without coalescing or rejecting, dropping first callback. |
| **Finding D2 — Duplicate Success Callback Idempotency** | **FAIL** | **HIGH** | Duplicate success callback on active retransmission inflates `confirmedRetries` and `retryCounts`. |
| **Finding D3 — Failure-After-Success Race** | **FAIL** | **HIGH** | Failure callback arriving after handoff success deletes in-flight reservation and rolls back retries for handed-off chunk. |
| **Regression Suite & Production Build** | **PASS** | — | 76/76 unit tests, 700/700 sync-engine tests, 1,285/1,285 monorepo tests pass. Build code 0. |

### Overall Verdict: **FAIL (DO NOT FREEZE M4.2c)**
Sub-Milestone M4.2c is **NOT eligible** to be approved or frozen until Findings D1, D2, and D3 are resolved in a final targeted closure commit (**M4.2c-R5**).

---

## 2. Commit Provenance & Working Tree Verification

- **Audit Target Commit:** `091ecaa5bc5cd98599bfc1b6b2d22a353c6f26a4`
- **Direct Parent Commit:** `2c7c2a9d1bd1816aefe483053845f8c8016a2110` (M4.2c-R3)
- **Git Status:** Clean (`git status --short` output is empty).
- **Files Modified in Commit:**
  - `docs/PHASE7_5_M4_2C_R3_INDEPENDENT_FINAL_AUDIT_REPORT.md` (audit report)
  - `docs/PHASE7_5_M4_2C_R4_TARGETED_CORRECTION_REPORT.md` (correction report)
  - `packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts` (test suite)
  - `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts` (implementation)
- **Frozen Baselines Verified (0 diffs):**
  - M4.1 baseline `fd23edae5bdb52cff32674860229277a79cb1aca`: 0 diffs
  - M4.2a baseline `e3245912626758c094d83edbd4d9264a922070f5`: 0 diffs
  - M4.2b baseline `8f509f94a39be9ece4060003ee498dc747efb813`: 0 diffs

---

## 3. Mandatory Attempt-Token Audit (Finding A Verification)

Inspection of [`packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts#L1000-L1142):

1. **Public API Signatures:**
   ```typescript
   public handleHandoffSuccess(
     transferId: string,
     chunkIndex: number,
     messageId: string,
     attemptToken: string,
     context: AuthenticatedRecipientContext
   ): void

   public handleHandoffFailure(
     transferId: string,
     chunkIndex: number,
     messageId: string,
     attemptToken: string,
     context: AuthenticatedRecipientContext
   ): void
   ```
   All optional token parameters, union overloads, and legacy 3-argument overloads have been removed.
2. **Runtime Validation:**
   ```typescript
   if (!attemptToken || typeof attemptToken !== 'string' || attemptToken.trim() === '') {
     return;
   }
   if (!messageId || typeof messageId !== 'string' || messageId.trim() === '') {
     return;
   }
   const active = record.activeAttempts.get(chunkIndex);
   if (!active) {
     return;
   }
   if (active.messageId !== messageId || active.attemptToken !== attemptToken) {
     return; // Stale, duplicate, or mismatched attempt: NO-OP
   }
   ```
   Inputs of `undefined`, `null`, `""`, `"   "`, numbers, booleans, objects, and arrays are dropped safely without throwing or mutating state. Matching strictly requires both `active.messageId === messageId` AND `active.attemptToken === attemptToken`.
3. **Collision Safety:**
   `attemptToken` is formatted as `${messageId}:${attemptSeq}` where `messageId` is generated with 122-bit UUID v4 entropy and `attemptSeq` is strictly incremented monotonically (`(attemptCounters.get(chunkIndex) ?? 0) + 1`). This format guarantees collision-freedom across all attempts within the transfer session.

---

## 4. Critical Finding D1: Concurrent Requests for One Chunk Overwrite Active Attempt

### Description
In [`packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts#L880-L915):
```typescript
const attemptSeq = (record.attemptCounters.get(chunkIndex) ?? 0) + 1;
record.attemptCounters.set(chunkIndex, attemptSeq);
const attemptToken = `${messageId}:${attemptSeq}`;

const attemptFrame: OutboundPreparedChunk = Object.freeze({
  ...frame,
  messageId,
  attemptSeq,
  attemptToken,
});

record.activeAttempts.set(chunkIndex, { messageId, attemptSeq, attemptToken, isRetransmission });
record.inflightChunks.add(chunkIndex);
results.push(attemptFrame);
```
When `handleChunkRequest()` is called for `chunkIndex`:
- If an attempt $H_1$ is already active and in flight in the transport adapter (`activeAttempts.has(chunkIndex)`), the pipeline neither rejects the duplicate request nor coalesces it.
- Instead, it increments `attemptCounters` to generate token $T_2$ and **overwrites** `record.activeAttempts.set(chunkIndex, { ..., attemptToken: T2 })`.
- When the transport adapter finishes handoff of attempt $H_1$ and invokes:
  `handleHandoffSuccess(transferId, chunkIndex, M1, T1, context)`:
  Because `active.attemptToken` is now $T_2$, $T_1$ does not match!
  The handoff callback for $H_1$ is **silently dropped as an invalid attempt**.

### Empirical Demonstration
```javascript
const [f1] = pipeline.handleChunkRequest(transferId, [0], ctx); // f1 token: msg-...:1
const [f2] = pipeline.handleChunkRequest(transferId, [0], ctx); // f2 token: msg-...:2

pipeline.handleHandoffSuccess(transferId, 0, f1.message.messageId, f1.attemptToken, ctx);

// Querying chunk 0 demonstrates f1 handoff was dropped:
const [f3] = pipeline.handleChunkRequest(transferId, [0], ctx);
// f3 reuses M1 because lastHandedOffMessageIds was NEVER set by f1!
```
- **Consequence:** The first attempt entered the local transport send buffer, but the pipeline failed to record its handoff. If the adapter never dispatches $f_2$ (e.g. $f_2$ was an accidental duplicate poll), the chunk will never be marked as handed off and will remain stuck.

### Required Remedy for M4.2c-R5
In `handleChunkRequest()`, if `record.activeAttempts.has(chunkIndex)`:
**Coalesce and return the active attempt frame without overwriting:**
```typescript
const existingActive = record.activeAttempts.get(chunkIndex);
if (existingActive) {
  const existingFrame = record.preparedFrames.get(chunkIndex);
  if (existingFrame && existingFrame.message.messageId === existingActive.messageId) {
    results.push(Object.freeze({
      ...existingFrame,
      messageId: existingActive.messageId,
      attemptSeq: existingActive.attemptSeq,
      attemptToken: existingActive.attemptToken,
    }));
    continue;
  }
}
```
This preserves the single in-flight attempt $T_1$, prevents concurrent token churn, and guarantees $T_1$'s callback succeeds!

---

## 5. Critical Finding D2: Duplicate Success Callbacks Inflate Retry Accounting

### Description
In [`packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts#L1048-L1058):
```typescript
// Record entry into local send buffer (Rule 6, 10)
record.handedOffAttempts.add(attemptToken);
record.handedOffChunks.add(chunkIndex);
record.lastHandedOffMessageIds.set(chunkIndex, messageId);

if (active.isRetransmission) {
  const currentConfirmed = record.confirmedRetries.get(chunkIndex) ?? 0;
  const newConfirmed = currentConfirmed + 1;
  record.confirmedRetries.set(chunkIndex, newConfirmed);
  record.retryCounts.set(chunkIndex, newConfirmed);
}
```
`activeAttempts` is intentionally preserved until remote ACK (or new retransmission request).
If a transport layer reports duplicate success callbacks for the active attempt $H$ before ACK:
1. First success callback: `confirmedRetries` becomes 1, `retryCounts` becomes 1.
2. Second duplicate success callback arrives with identical $M$ and $T$:
   - Passes token validation (`active.attemptToken === attemptToken`).
   - Executes `newConfirmed = currentConfirmed + 1`!
   - `confirmedRetries` becomes 2!
   - `retryCounts` becomes 2!

### Empirical Demonstration
```
M2 requested: retryCounts = 1
M2 first success callback: retryCounts = 1
M2 duplicate success callback: retryCounts = 2 (INFLATED!)
```
- **Consequence:** Duplicate handoff success notifications from the transport adapter falsely consume retry budget, violating the requirement: *"Duplicate success, duplicate failure, success-after-failure, and failure-after-success are safe no-ops."*

### Required Remedy for M4.2c-R5
In `handleHandoffSuccess()`, check `handedOffAttempts` before mutating retry state:
```typescript
if (record.handedOffAttempts.has(attemptToken)) {
  // Duplicate success callback: idempotent no-op
  return;
}
```

---

## 6. Critical Finding D3: Failure Callback After Success Corrupts In-Flight State

### Description
In [`packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts#L1105-L1139):
When an attempt $H$ has already successfully handed off:
- `record.handedOffAttempts.has(attemptToken)` is `true`.
- `record.activeAttempts` still contains $H$ (waiting for remote ACK).
If a delayed, duplicate, or out-of-order failure callback arrives for attempt $H$:
- `handleHandoffFailure()` matches $H$ in `activeAttempts`.
- It deletes `activeAttempts.delete(chunkIndex)`.
- It removes chunk from in-flight: `record.inflightChunks.delete(chunkIndex)`.
- It rolls back retry accounting: `record.retryCounts.delete(chunkIndex)`.

### Empirical Demonstration
```
Before failure callback: inflight = true
After failure callback: inflight = false (CORRUPTED!)
```
- **Consequence:** A message that already successfully entered the network send buffer is prematurely unreserved from `inflightChunks` and marked as failed, corrupting transfer session tracking before remote ACK arrives.

### Required Remedy for M4.2c-R5
In `handleHandoffFailure()`, check if the attempt has already succeeded:
```typescript
if (record.handedOffAttempts.has(attemptToken)) {
  // Attempt already successfully handed off: failure callback is a strict no-op
  return;
}
```

---

## 7. Retransmission Lifecycle & Decision D8 Audit

The audit traced the complete mandatory sequence through runtime instrumentation:

```
Step 1: Initial transmission M1: msg-14519482... Token: ...:1 | retryCounts = undefined | inflight = true
Step 2: M1 local failure reported                              | retryCounts = undefined | inflight = false
Step 3: Local retry reuses M1 (reused = true) Token: ...:2     | retryCounts = undefined | inflight = true
Step 4: M1 successful handoff recorded                         | retryCounts = undefined | inflight = true
Step 5: 1st genuine retransmission allocates M2 Token: ...:3   | retryCounts = 1         | inflight = true
Step 6: M2 local failure reported (reservation rolled back)    | retryCounts = undefined | inflight = false
Step 7: Local retry reuses M2 (reused = true) Token: ...:4     | retryCounts = 1         | inflight = true
Step 8: M2 successful handoff recorded                         | retryCounts = 1         | inflight = true
Step 9: 2nd genuine retransmission allocates M3 Token: ...:5   | retryCounts = 2         | inflight = true
Step 10: M3 successful handoff recorded                        | retryCounts = 2         | inflight = true
Step 11: 3rd genuine retransmission allocates M4               | retryCounts = 3         | inflight = true
Step 12: 4th retransmission exceeds maxRetries (3)             | Throws LOCAL_ABORT     | State = ABORTED
```

- **Decision D8 Compliance:** Confirmed. Fresh `messageId`s are generated on genuine network retransmissions, while failed local handoffs strictly preserve `messageId`.
- **Zero Retries on Local Failures:** Confirmed. Initial failures consume 0 retries. Retransmission local failures roll back retry reservations.
- **Retry Bound:** Confirmed. Exactly 3 successful retransmissions beyond the initial handoff are permitted; the 4th request throws and transitions the transfer to `ABORTED` with `RETRY_BUDGET_EXHAUSTED`.

---

## 8. Full Monorepo Test & Build Verification

Executed clean in the audit environment:
1. **Outbound Pipeline Unit Tests:**
   `npx vitest run packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts`
   **76 / 76 passed** (147 ms)
2. **Sync-Engine Package Suite:**
   `npx vitest run packages/sync-engine/src/__tests__/`
   **700 / 700 passed** across 34 test files (3.95 s)
3. **Full Monorepo Suite:**
   `npx vitest run`
   **1,285 / 1,285 passed** across 55 test files (7.88 s)
4. **Production Build:**
   `npm run build`
   Clean compilation exit code 0 across all 6 workspaces (`control-events`, `peer-mesh`, `protocol`, `sync-engine`, `bootstrap-server`, `demo`).

---

## 9. Final Recommendations & Roadmap to M4.2c-R5 Closure

Commit `091ecaa` represents major architectural progress, closing the previous attempt-token optionality and identity-storage defects. However, it cannot be approved or frozen due to Findings D1, D2, and D3.

### Required Actions for M4.2c-R5:
1. **In `handleChunkRequest()`:** If `record.activeAttempts.has(chunkIndex)`, coalesce by returning the active prepared frame and its existing `attemptToken` (preventing slot overwrite).
2. **In `handleHandoffSuccess()`:** Add `if (record.handedOffAttempts.has(attemptToken)) return;` before retry calculations (guaranteeing duplicate success idempotency).
3. **In `handleHandoffFailure()`:** Add `if (record.handedOffAttempts.has(attemptToken)) return;` (guaranteeing failure-after-success is a strict no-op).
4. **Unit Tests:** Add adversarial regression tests for:
   - Repeated `handleChunkRequest()` while attempt is active (confirming identical attemptToken returned and no callback invalidation).
   - Duplicate `handleHandoffSuccess()` on retransmission (confirming retryCounts does not inflate).
   - `handleHandoffFailure()` after `handleHandoffSuccess()` (confirming inflightChunks and retryCounts are not corrupted).

**Milestone Approval Recommendation:** **FAIL (DO NOT FREEZE M4.2c)**. Require M4.2c-R5 closure.
