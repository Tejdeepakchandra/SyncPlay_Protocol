# Phase 7.5 M4.2c-R3 — Independent Final Implementation Audit Report
## Attempt Identity Fencing, Identity Storage & Decision D8 Message Lifecycle Audit

**Audit Target Commit:** `2c7c2a9d1bd1816aefe483053845f8c8016a2110`  
**Direct Parent Commit:** `8467123191c6d2bd7affbbdee649f8bf634f616f` (M4.2c-R2)  
**Frozen M4.2b Baseline:** `8f509f94a39be9ece4060003ee498dc747efb813`  
**Frozen M4.2a Baseline:** `e3245912626758c094d83edbd4d9264a922070f5`  
**Frozen M4.1 Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Audit Role:** Independent Senior Distributed-Systems Engineer  
**Audit Mode:** Strictly Read-Only  
**Date:** November 22, 2026  

---

## 1. Executive Summary & Audit Verdict

A strict, adversarial, independent read-only implementation audit was conducted on commit `2c7c2a9d1bd1816aefe483053845f8c8016a2110` (Phase 7.5 Sub-Milestone M4.2c-R3).

The audit evaluated whether M4.2c-R3 definitively resolves the attempt-specific handoff failure fencing and message identity defects identified in earlier audits.

### Audit Verdict Summary

| Evaluation Area | Audit Result | Severity | Core Finding |
| :--- | :---: | :---: | :--- |
| **Critical Finding A — Attempt Token Optionality** | **FAIL** | **Blocking Defect** | `attemptToken` was made optional in both TypeScript signatures and runtime validation. When omitted on callbacks, the pipeline collapses to matching on `messageId`. Because Rule 5 preserves `messageId` across failed local attempts, a delayed duplicate failure for attempt $H_1$ (omitting `attemptToken`) erroneously mutates and releases active attempt $H_2$. |
| **Critical Finding B — Identity Storage Keying** | **FAIL** | **Design Defect** | `failedAttempts` and `handedOffAttempts` are sets keyed by `messageId`. Because `messageId` is reused across failed local attempts, these sets conflate distinct local attempts sharing that `messageId`. |
| **Critical Finding C — Message Identity & Decision D8** | **CONDITIONAL PASS** | **Inconsistency** | Fresh `messageId`s are generated on retransmission (fulfilling Decision D8), but `isRetransmission` is evaluated against `transmittedChunks` (populated upon frame return) rather than confirmed handoffs (`handedOffChunks`). |
| **Critical Finding D — Handoff Outcome Semantics** | **CONDITIONAL PASS** | **Conditional on Token** | Definitive outcome semantics hold only when explicit `attemptToken`s are supplied. With optional tokens, duplicate callbacks compromise idempotency. |
| **Contextual & Capability Fencing** | **PASS** | **Verified** | Context provenance, peer ID, session ID, membership epoch, and connection generation fencing execute strictly before any state mutation. |
| **Frozen Baselines Integrity (M4.1, M4.2a, M4.2b)** | **PASS** | **Verified** | 0 diff against frozen baselines `fd23eda`, `e324591`, and `8f509f9`. |
| **Overall M4.2c-R3 Milestone Verdict** | **FAIL** | **Blocking** | **DO NOT FREEZE M4.2c.** A targeted closure (M4.2c-R4) making `attemptToken` mandatory is required before freezing or proceeding to M4.2d/M4.3. |

---

## 2. Provenance & Working Tree Verification

The repository provenance and clean state were established before auditing:

```text
Commit SHA: 2c7c2a9d1bd1816aefe483053845f8c8016a2110
Commit Message: fix(phase-7.5-m4): attempt-specific handoff failure fencing and message identity reconciliation (m4.2c-r3)
Author: Tejdeepakchandra <tejdeepak2005@gmail.com>
Date: Sun Nov 22 23:03:25 2026 +0530
Direct Parent: 8467123191c6d2bd7affbbdee649f8bf634f616f (M4.2c-R2)
Branch: main
Working Tree: Clean (0 uncommitted changes, 0 untracked files)
```

Diff checks against frozen baselines confirmed zero modifications:
- `packages/sync-engine/src/recovery/chunk-assembler.ts` $\equiv$ `fd23eda` (0 lines)
- `packages/protocol/src/payloads/snapshot-recovery.ts` $\equiv$ `fd23eda` (0 lines)
- `packages/sync-engine/src/snapshot.ts` $\equiv$ `fd23eda` (0 lines)
- `packages/sync-engine/src/recovery/snapshot-serializer.ts` $\equiv$ `e324591` (0 lines)
- `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts` $\equiv$ `e324591` (0 lines)
- `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts` $\equiv$ `8f509f9` (0 lines)

---

## 3. Critical Finding A: The Optional Attempt Token Defect

### 3.1 Structural Source Code Analysis

In `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`, the callback signatures and runtime matching logic are implemented as follows:

```typescript
public handleHandoffFailure(
  transferId: string,
  chunkIndex: number,
  messageId: string,
  context: AuthenticatedRecipientContext,
  attemptToken?: string | number   // <--- OPTIONAL
): void;
...
// Runtime validation in handleHandoffFailure:
const active = record.activeAttempts.get(chunkIndex);

if (messageId !== undefined) {
  if (!active || active.messageId !== messageId) {
    return;
  }
  // The token is checked ONLY if defined:
  if (
    attemptToken !== undefined &&
    attemptToken !== active.attemptToken &&
    attemptToken !== active.attemptSeq
  ) {
    return;
  }
}
```

### 3.2 The Defect Mechanism

1. **Rule 5 Invariant:** A failed local handoff preserves `messageId` for the next local attempt.
2. **Consequence:** Attempt $H_1$ and subsequent local attempt $H_2$ share the **identical `messageId` ($M$)**, but carry distinct attempt tokens ($T_1 = `${M}:1$`, $T_2 = `${M}:2$`).
3. **The Bypass:** When a caller or delayed callback invokes `handleHandoffFailure(transferId, chunkIndex, M, context)` without supplying `attemptToken`:
   - `attemptToken` is `undefined`.
   - The runtime condition `if (attemptToken !== undefined && ...)` is **skipped**.
   - The check evaluates `active.messageId === M`, which is **true** for active attempt $H_2$.
   - The pipeline proceeds to mutate state:
     - `record.activeAttempts.delete(chunkIndex);`
     - `record.inflightChunks.delete(chunkIndex);`
     - `record.transmittedChunks.delete(chunkIndex);`
4. **Impact:** A delayed or duplicate callback from an earlier, failed attempt ($H_1$) that omits `attemptToken` cancels the active in-flight reservation of $H_2$ and rolls back its transmission status.

---

## 4. Adversarial Test Results (Scenarios B1–B9)

An isolated adversarial test suite was executed against the compiled `SnapshotOutboundPipeline` in Node 22 without altering git-tracked repository files. The empirical results are detailed below:

| Test Scenario | Description | Empirical Result | Audit Finding |
| :--- | :--- | :---: | :--- |
| **B1 (Mandatory)** | Delayed duplicate failure for $H_1$ arrives with $M$ but omitting `attemptToken` while $H_2$ is active. | **DEFECT DETECTED** | Active attempt $H_2$'s in-flight reservation was deleted (`inflightChunks.has(0) === false`), and transmission status was cleared. |
| **B1 (Success Variant)** | Delayed success for $H_1$ arrives with $M$ but omitting `attemptToken` while $H_2$ is active. | **DEFECT DETECTED** | $H_1$'s stale callback matched $H_2$ and recorded handoff success, resurrecting an attempt that had failed locally. |
| **B2** | $H_1$ failure callback with explicit $T_1$ arrives after $H_2$ is active with $T_2$. | **PASS** | Mismatch $T_1 \neq T_2$ detected; callback dropped as strict no-op. $H_2$ in-flight retained. |
| **B3** | $H_1$ success callback with explicit $T_1$ arrives after $H_2$ is active with $T_2$. | **PASS** | Mismatch $T_1 \neq T_2$ detected; callback dropped as strict no-op. |
| **B4** | Callback with unknown token (`token-unknown-999`). | **PASS** | Token mismatch detected; dropped as strict no-op. |
| **B5** | Callback with token from another chunk (chunk 1 token passed for chunk 0). | **PASS** | Token mismatch detected; dropped as strict no-op. |
| **B6** | Correct `messageId` but incorrect token. | **PASS** | Token mismatch detected; dropped as strict no-op. |
| **B7** | Correct token but incorrect `messageId`. | **PASS** | `messageId` mismatch detected; dropped as strict no-op. |
| **B8** | Duplicate callback after attempt retired. | **PASS** | `activeAttempts.get(chunkIndex)` is `undefined`; dropped as strict no-op. |
| **B9** | Callbacks with empty string (`''`) or `null` token. | **PASS (for empty/null)** | Non-matching values trigger token inequality check and are dropped. Only `undefined` (omitted) bypasses the check. |

---

## 5. Critical Finding B: Identity Storage & Attempt Lifecycle

### 5.1 Storage Structure Keying Analysis

| Structure | Key Type | Value Type | Conflation Risk |
| :--- | :---: | :---: | :--- |
| `activeAttempts` | `chunkIndex` (number) | `{ messageId, attemptSeq, attemptToken }` | **None.** Accurately models a single active attempt per chunk. |
| `handedOffAttempts` | `messageId` (string) | N/A (`Set<string>`) | **HIGH.** If local attempt $H_1$ fails and $H_2$ reuses $M$, this set cannot distinguish whether $H_1$ or $H_2$ entered the send buffer. |
| `failedAttempts` | `messageId` (string) | N/A (`Set<string>`) | **HIGH.** Keyed by $M$, conflating separate attempts that share $M$. Deleted when $H_2$ is requested, but re-added if a duplicate callback arrives for $H_1$. |
| `attemptCounters` | `chunkIndex` (number) | `number` | **None.** Monotonically incremented sequence counter per chunk. |
| `inflightChunks` | `chunkIndex` (number) | N/A (`Set<number>`) | **None.** Tracks active sliding-window reservations. |
| `transmittedChunks` | `chunkIndex` (number) | N/A (`Set<number>`) | **Medium.** Set upon frame return, not confirmed handoff. |
| `retryCounts` | `chunkIndex` (number) | `number` | **None.** Accurately bounded to `maxRetriesPerChunk`. |
| `chunkMessageIds` | `chunkIndex` (number) | `string` | **None.** Holds current `messageId` for the chunk. |

### 5.2 Attempt Lifecycle Separation
The audit confirmed that the system conceptually attempts to track four distinct lifecycle states:
1. **Message Identity (`messageId`):** Identifies the wire payload format.
2. **Local Attempt Identity (`attemptToken`):** Identifies a specific dispatch invocation (`${messageId}:${attemptSeq}`).
3. **Confirmed Local Handoff (`handedOffAttempts`):** Confirms entry into the local DataChannel send buffer.
4. **Remote Delivery Acknowledgment (`acknowledgedChunks`):** Confirms remote receiver assembly and hash verification.

However, because `handedOffAttempts` and `failedAttempts` are sets of `messageId` rather than `attemptToken`, local attempts sharing a `messageId` cannot be independently tracked in history.

---

## 6. Critical Finding C: Message Identity & Decision D8 Compliance

The audit traced the message identity lifecycle against Decision D8:

```text
Decision D8: "messageId = one transmission; logicalEventId = one logical operation across retries."
```

### Evaluation of Transmission Situations

1. **Cache Eviction & Re-materialization:** **COMPLIANT.** Evicted frames re-produce bit-for-bit identical JSON with preserved `messageId`.
2. **Local Adapter Re-read:** **COMPLIANT.** Preserves `messageId`.
3. **Failed Local Handoff:** **COMPLIANT.** Preserves `messageId` for the next local attempt.
4. **Physical Network Retransmission:** **COMPLIANT.** When a retransmission is requested, a fresh `messageId` is generated, fulfilling Decision D8 without impacting receiver deduplication on `(transferId, chunkIndex, chunkSha256)`.
5. **Generation Failover:** **COMPLIANT.** Transfers do not mutate generation; a new transfer with new IDs is required.

### Specification Inconsistency in `handleChunkRequest()`
In `snapshot-outbound-pipeline.ts` line 805:
```typescript
const isRetransmission = record.transmittedChunks.has(chunkIndex);
```
`record.transmittedChunks.add(chunkIndex)` is invoked in `handleChunkRequest()` when the initial frame is returned. If a transport adapter requests a chunk frame a second time before reporting handoff outcome (e.g., duplicate poll or multi-channel dispatch), the pipeline immediately classifies it as a network retransmission, allocates a new `messageId`, and increments `retryCounts`.

To comply strictly with the requirement that *"a fresh `messageId` is generated only after a previous successful handoff, not merely because a frame was returned"*, retransmission classification should check `record.handedOffChunks.has(chunkIndex)` rather than `transmittedChunks`.

---

## 7. Critical Finding D: Handoff Outcome Semantics

1. **Local Send Buffer Entry vs. Remote Delivery:** Verified. `handleHandoffSuccess` marks `handedOffAttempts` and keeps chunks in `inflightChunks`. It does not mark chunks acknowledged or complete transfers.
2. **Remote Acknowledgment Immunity:** Verified. Once a chunk is acknowledged via `handleChunkAck`, subsequent handoff failure callbacks are completely ignored (`if (record.acknowledgedChunks.has(chunkIndex)) return;`).
3. **Definitive Outcome Limitation:** The pipeline cannot guarantee definitive outcomes when `attemptToken` is omitted. If an attempt fails locally, and a subsequent local attempt is issued with the same `messageId`, a late success callback omitting `attemptToken` will be accepted by the pipeline, resurrecting the failed attempt.

---

## 8. Contextual and Capability Fencing

Contextual and capability fencing was verified across all callback paths:
- `handleChunkRequest`, `handleChunkAck`, `handleHandoffSuccess`, `handleHandoffFailure`, and `handleRemoteAbort` all invoke `#validateContext()` as their very first operation.
- Validates:
  - Context registered with pipeline (`#validRecipientContexts.has(context)`).
  - Transfer ID match (`context.transferId === record.transferId`).
  - Recipient peer ID match (`context.recipientPeerId === record.recipientPeerId`).
  - Session ID match (`context.sessionId === record.artifact.sessionId`).
  - Membership epoch and connection generation match.
- Zero state mutations occur if validation fails.

---

## 9. Test Suite & Build Verification Results

All tests and builds were executed and verified:

```bash
# 1. Snapshot Outbound Pipeline Suite (66 tests)
npx vitest run packages/sync-engine/src/__tests__/snapshot-outbound-pipeline.test.ts
# Result: 66 passed (1 file, 168 ms)

# 2. Entire Sync-Engine Package Suite (690 tests)
npx vitest run packages/sync-engine/src/__tests__/
# Result: 690 passed (34 files, 4.03 s)

# 3. Full Monorepo Test Suite (1,275 tests)
npx vitest run
# Result: 1,275 passed (55 files, 6.26 s)

# 4. Production Build Across Workspaces
npm run build
# Result: Code 0 (tsc & Vite production bundle)

# 5. Git Working Tree Status
git status
# Result: Working tree clean, 0 untracked files
```

---

## 10. Defects Identified & Reproduction Sequences

### Defect 1 (Severity: BLOCKING) — Optional Attempt Token Permits Active Attempt Corruption
- **Mechanism:** `attemptToken?: string | number` allows callbacks to omit the token. Because `messageId` is preserved across failed local attempts, a delayed duplicate failure for $H_1$ matches active attempt $H_2$'s `messageId`, releasing $H_2$'s in-flight reservation.
- **Reproduction:** Adversarial scenario B1 (see Section 4).
- **Remedy:** Make `attemptToken: string` **MANDATORY** in `handleHandoffSuccess` and `handleHandoffFailure`. If omitted or mismatched, drop immediately as a no-op.

### Defect 2 (Severity: MEDIUM) — Retransmission Triggered by Frame Return Rather Than Confirmed Handoff
- **Mechanism:** `isRetransmission` checks `record.transmittedChunks.has(chunkIndex)`, which is populated on frame production.
- **Remedy:** Check `record.handedOffChunks.has(chunkIndex)` so a fresh `messageId` and retry increment occur only after a confirmed network handoff.

### Defect 3 (Severity: MEDIUM) — Storage Conflation in `handedOffAttempts` and `failedAttempts`
- **Mechanism:** Keyed by `messageId` rather than `attemptToken`.
- **Remedy:** Key attempt history by `attemptToken` (e.g. `Set<string>` storing `${messageId}:${attemptSeq}`).

---

## 11. Final Recommendation

### Overall Verdict: **FAIL (Blocking Defect on Optional Attempt Token)**

### Recommendation
**DO NOT FREEZE SUB-MILESTONE M4.2c.**

Sub-Milestone M4.2c must undergo one final targeted correction (**M4.2c-R4**) before milestone sign-off and before authorizing M4.2d/M4.3.

#### Required M4.2c-R4 Scope:
1. Make `attemptToken: string` **mandatory** in `handleHandoffSuccess` and `handleHandoffFailure`:
   ```typescript
   handleHandoffSuccess(
     transferId: string,
     chunkIndex: number,
     messageId: string,
     attemptToken: string,
     context: AuthenticatedRecipientContext
   ): void;

   handleHandoffFailure(
     transferId: string,
     chunkIndex: number,
     messageId: string,
     attemptToken: string,
     context: AuthenticatedRecipientContext
   ): void;
   ```
2. Strictly reject or drop any callback where `attemptToken` does not match `active.attemptToken`.
3. Update `handedOffAttempts` and `failedAttempts` to store `attemptToken` instead of `messageId`.
4. Tie `isRetransmission` directly to `record.handedOffChunks.has(chunkIndex)`.
5. Update unit tests to provide `attemptToken` across all handoff callbacks.
