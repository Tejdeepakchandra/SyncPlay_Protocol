# Phase 7.5 M4.3a — Final Independent Source Audit & Git Closure Report

**Auditor:** Senior TypeScript Distributed-Systems Auditor  
**Audit Mode:** STRICTLY READ-ONLY SOURCE AUDIT  
**Milestone:** Phase 7.5 Sub-Milestone M4.3a  
**Authoritative Baseline Hash:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Approved Architecture Commit:** `2bb117c5db474a13307be65c6f68845f2bd74720`  
**Implementation Commit Audited:** `9bb37491b2359e46cf7449a87152bf30600b0e7c`  
**Date:** November 24, 2026  

---

## 1. Executive Summary & Audit Verdict

A rigorous independent source-code audit was conducted on the completed M4.3a implementation commit `9bb37491b2359e46cf7449a87152bf30600b0e7c`.

All four core components (`RecoveryStateMachine`, `QuarantineBuffer`, `FutureEpochBuffer`, and `RecoveryAdmissionController`) were inspected against the approved M4.3-R4 specifications and acceptance criteria. All unit tests (**59 / 59 passed**) and monorepo tests (**1,422 / 1,422 passed**) execute cleanly under Vitest.

However, during whole-workspace production compilation (`npm run build`), TypeScript's compiler (`tsc`) failed in `@syncplay/sync-engine` with **17 compiler errors** (`TS4111`) in `packages/sync-engine/src/recovery/recovery-admission-controller.ts`. The error is caused by property dot-access on objects typed with `Record<string, unknown>` under the project's strict `"noPropertyAccessFromIndexSignature": true` compiler option defined in `tsconfig.base.json:17`.

In accordance with strict audit instructions ("Do not conceal a failed assertion behind an overall PASS"), the audit outcome is:

**Audit Verdict:** **CORRECTION REQUIRED (BLOCKING TS4111 COMPILER ERRORS)**  
**Final Status:** `M4.3a INDEPENDENT AUDIT FAIL — CORRECTION REQUIRED`

---

## 2. Git Closure & Repository Baseline Audit

### 2.1 Git Commands and Exact Outputs
- `git status --short`:
  ```text
  (clean working tree; no output)
  ```
- `git branch --show-current`:
  ```text
  main
  ```
- `git rev-parse HEAD`:
  ```text
  9bb37491b2359e46cf7449a87152bf30600b0e7c
  ```
- `git rev-parse HEAD^`:
  ```text
  2bb117c5db474a13307be65c6f68845f2bd74720
  ```
- `git log -2 --oneline`:
  ```text
  9bb3749 feat(phase-7.5-m4): implement recovery state machine and bounded quarantine (m4.3a)
  2bb117c docs(phase-7.5-m4): approve m4.3 recovery orchestration architecture and specifications (m4.3-r4)
  ```

### 2.2 Frozen Baseline Diff Verification
Executed `git diff aafa366cc48fdb2b83940f00e80d92d813e7c52e` across all frozen paths:
- `packages/protocol/*`: **0 diffs**
- `packages/sync-engine/src/recovery/chunk-assembler.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-serializer.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/candidate-offer-registry.ts`: **0 diffs**
- `packages/sync-engine/src/recovery/inbound-custody.ts`: **0 diffs**

**Frozen Files Integrity:** **100% UNTOUCHED (0 lines modified)**  
**Classification:** **PASS**

---

## 3. Detailed Audit Area Findings

### 3.1 Audit 1 — Serialized-Byte Accounting
**Inspected Files:**
- `packages/sync-engine/src/recovery/recovery-admission-controller.ts:209-236`
- `packages/sync-engine/src/recovery/quarantine-buffer.ts:149-192, 240-248, 260-267, 280-287`
- `packages/sync-engine/src/recovery/future-epoch-buffer.ts:160-215, 255-263, 280-288, 300-310`

**Evaluation:**
1. **Provenance:** `serializedByteLength` is accepted as an explicit parameter by `interceptRemoteEvent(event, serializedByteLength, currentEpoch)` and stored in `QuarantinedOperation`. In the runtime transport pipeline, this value originates from WebRTC `RTCDataChannel.onmessage` (`event.data: string`), where wire size is measured prior to JSON deserialization.
2. **Under-Reporting Risk:** Neither `QuarantineBuffer` nor `RecoveryAdmissionController` independently re-serializes the event. They validate that the parameter is a valid finite positive number within individual limits ($0 < \text{size} \le 4\text{ KiB}$). Binding wire size directly to the transport layer before admission prevents under-reporting by untrusted callers.
3. **UTF-8 Byte Length Measurement:** The byte accounting is strictly byte-based (`serializedByteLength`). Unit tests test exact byte boundaries (e.g. 4,096 bytes vs 4,097 bytes).
4. **Individual 4 KiB Limit Enforcement:** Enforced strictly before retention:
   - `quarantine-buffer.ts:154`: `op.serializedByteLength > this.maxEventBytes` throws `REJECTED_INVALID`.
   - `future-epoch-buffer.ts:165`: `op.serializedByteLength > this.maxEventBytes` throws `REJECTED_INVALID`.
   - `recovery-admission-controller.ts:213`: `serializedByteLength > this.quarantineBuffer.maxEventBytes` returns `REJECTED_INVALID`.
5. **Aggregate Byte Accounting Exactness:**
   - Enqueue/Buffer adds `serializedByteLength` to `_activeBytes` or `_totalBytes`.
   - `swapDrainBatch()` transfers `_activeBytes` into `_drainBytes`.
   - `shiftDrain()` subtracts `op.serializedByteLength` from `_drainBytes`.
   - `unshiftDrain()` re-adds `op.serializedByteLength` to `_drainBytes`.
   - `clear()` resets all byte counters to 0.
   - Tested in `quarantine-buffer.test.ts: admits a single valid operation and maintains exact byte accounting` and `future-epoch-buffer.test.ts: enforces maximum 512 KiB aggregate byte boundary`.
6. **Boundary Rejections:** Non-numbers, NaN, Infinity, zero, and negative values throw `REJECTED_INVALID` (verified in `quarantine-buffer.test.ts: rejects invalid serialized sizes`).
7. **Duplicate Accounting:** Duplicates are detected via `${originPeerId}:${sequenceNumber}` and rejected with `false` before byte or count counters increment.
8. **Release Exactness:** `shiftDrain()`, `releaseWithCapability()`, `handleGraceTimeout()`, `clear()`, and `dispose()` deduct accounting exactly once.

**Classification:** **PASS**

---

### 3.2 Audit 2 — Quarantine Shifted-Operation Ownership
**Inspected Files:**
- `packages/sync-engine/src/recovery/quarantine-buffer.ts:220-270`
- `packages/sync-engine/src/__tests__/quarantine-buffer.test.ts:250-290`

**Evaluation:**
1. **Multiple Shifts:** `shiftDrain()` can be invoked sequentially by callers. `QuarantineBuffer` does not restrict shifting to a single concurrent in-flight item; it acts as a bounded FIFO container. Strict single-operation custody ($N_{\text{in-flight}} \le 1$) is the responsibility of M4.3c `IQuarantineDrainer`.
2. **Deduplication State on Shift:** When `shiftDrain()` is called, `this.identityIndex.delete(`${op.originPeerId}:${op.sequenceNumber}`)` is invoked immediately upon dequeue.
3. **Byte Accounting on Shift:** Shifted operations decrement `_drainBytes` immediately upon shift.
4. **Duplicate Admission While Shifted:** If a duplicate operation arrives while the shifted operation is in-flight, `hasOperation()` returns false and the duplicate would be admitted into `activeQueue`. In M4.3c, the drainer coordinator maintains custody until destination confirmation.
5. **`unshiftDrain()` Safety:** Restores an unconfirmed operation to index 0 of `drainBatch`, re-adds bytes, and re-registers the identity index. Preserves FIFO retry order.
6. **Stale Generation Handles:** Arrays returned by `swapDrainBatch()`, `getActiveQueue()`, and `getDrainBatch()` are frozen copies (`Object.freeze([...])`). Stale external handles cannot mutate internal state.
7. **Multiple Drain Generations:** If `swapDrainBatch()` is called while items remain in `drainBatch`, remaining items are prepended in FIFO order (`this.drainBatch = this.drainBatch.concat(this.activeQueue)`). Deterministic ordering is preserved.
8. **M4.3c Compatibility:** The `peekDrain()`, `shiftDrain()`, and `unshiftDrain()` primitives provide the necessary primitives for M4.3c `DRAINER_IN_FLIGHT` without exposing internal mutable collections.

**Classification:** **PASS (with Non-Blocking architectural note for M4.3c drainer custody)**

---

### 3.3 Audit 3 — FutureEpochBuffer Capability Binding
**Inspected Files:**
- `packages/sync-engine/src/recovery/future-epoch-buffer.ts:225-295`
- `packages/sync-engine/src/engine/leaderless-sync-engine.ts:84-105`
- `packages/sync-engine/src/__tests__/future-epoch-buffer.test.ts:180-230`

**Evaluation:**
1. **Capability Provenance:** `EpochTransitionCapability` is verified for non-empty string fields (`authority`, `sessionId`, `token`). If `authorizedCapability` is configured, strict token/reference matching is enforced.
2. **Target Epoch Binding:** `releaseWithCapability(capability, targetEpoch)` extracts operations strictly from `this.epochBuckets.get(targetEpoch)`.
3. **Epoch Matching:** Only operations whose `membershipEpoch === targetEpoch` are released.
4. **Replayability:** Released epoch buckets are deleted (`this.epochBuckets.delete(targetEpoch)`). Replaying the capability returns an empty frozen array `[]` without double-release.
5. **Cross-Epoch Isolation:** A capability for epoch $E$ cannot release operations for epoch $E+1$. Operations for $E+1$ remain in bucket $E+1$ under their own grace timer.
6. **Mismatched / Unrelated Capability:** Throws `SyncEngineError('UNAUTHORIZED')` (tested in `future-epoch-buffer.test.ts`).
7. **Races & Timing:**
   - Release cancels timer via `this.cancelEpochTimer(targetEpoch)`.
   - Expiration via `handleGraceTimeout(targetEpoch)` clears bucket before delayed release can execute.
   - Disposal cancels all timers and clears storage; delayed callbacks fence via `if (this._isDisposed) return;`.
8. **Replication Vector & Playback Immutability:** `FutureEpochBuffer` does not reference or mutate engine replication vectors or playback state.

**Classification:** **PASS**

---

### 3.4 Audit 4 — RecoveryStateMachine and Admission
**Inspected Files:**
- `packages/sync-engine/src/recovery/recovery-state-machine.ts:49-160`
- `packages/sync-engine/src/recovery/recovery-admission-controller.ts:90-155, 240-275`
- `packages/sync-engine/src/__tests__/recovery-state-machine.test.ts`
- `packages/sync-engine/src/__tests__/recovery-admission.test.ts`

**Evaluation:**
1. **Approved Transition Matrix:** Strictly enforces the M4.3-R4 transition table:
   - `ACTIVE -> RECOVERING, DEGRADED`
   - `RECOVERING -> RECONCILING, DEGRADED`
   - `RECONCILING -> ACTIVE, RECOVERING, DEGRADED`
   - `DEGRADED -> (none)`
2. **Invalid Transitions:** Throws `INVALID_STATE_TRANSITION` without mutating state or substate.
3. **DEGRADED Terminal Behavior:** `VALID_STATE_TRANSITIONS.DEGRADED` is an empty set; transitions out of `DEGRADED` are permanently rejected.
4. **Disposal & Callback Fencing:** Sets `_isDisposed = true`, transitions to `DEGRADED`/`IDLE`, clears listeners. Transitions after disposal throw `DISPOSED`.
5. **Listener Exception Isolation:** Listener invocations are wrapped in `try/catch`; throwing listeners do not abort committed state transitions.
6. **Local Intent Rejection:** Rejected in `RECOVERING` and `RECONCILING` with `REJECTED_RECOVERING`, and in `DEGRADED` with `REJECTED_DEGRADED` synchronously before any engine state or reducer can be invoked.
7. **Remote Admission vs Application:** Routes events to queues or returns status codes (`PASS_THROUGH`, `QUARANTINED`, `BUFFERED_FUTURE_EPOCH`, `DISCARD_OBSOLETE_EPOCH`, `REJECTED_DEGRADED`). Does not apply events.
8. **ACTIVE Compatibility:** Local intents return `{ allowed: true }`; current-epoch remote events return `PASS_THROUGH`. Full Phase 6/7.3 backward compatibility is preserved.
9. **Zero Vector Mutation:** Admission does not mutate or advance replication vectors.

**Classification:** **PASS**

---

### 3.5 Audit 5 — Test and Build Evidence

#### Test Execution Evidence:
- **Focused M4.3a Suites:**
  - `recovery-state-machine.test.ts`: **19 / 19 passed**
  - `quarantine-buffer.test.ts`: **14 / 14 passed**
  - `future-epoch-buffer.test.ts`: **12 / 12 passed**
  - `recovery-admission.test.ts`: **14 / 14 passed**
  - Total: **59 / 59 passed** (duration: 1.24s)
- **Full Monorepo Suites (`npm test`):**
  - `@syncplay/control-events`: **49 / 49 passed**
  - `@syncplay/peer-mesh`: **108 / 108 passed**
  - `@syncplay/protocol`: **337 / 337 passed**
  - `@syncplay/sync-engine`: **837 / 837 passed**
  - `@syncplay/bootstrap-server`: **91 / 91 passed**
  - Total: **1,422 / 1,422 passed** (0 failed, 0 skipped)
- **Test Integrity:** Grep verification confirmed zero occurrences of `.only(` or `.skip(` in test files.

#### Production Build Evidence (`npm run build`):
- `@syncplay/control-events`: **Pass** (`tsc` clean)
- `@syncplay/peer-mesh`: **Pass** (`tsc` clean)
- `@syncplay/protocol`: **Pass** (`tsc` clean)
- `@syncplay/bootstrap-server`: **Pass** (`tsc` clean)
- `@syncplay/demo`: **Pass** (`vite build` clean in 1.62s)
- `@syncplay/sync-engine`: **FAIL** (`tsc` failed with 17 `TS4111` errors)

**Compiler Output:**
```text
> @syncplay/sync-engine@0.1.0 build
> tsc

src/recovery/recovery-admission-controller.ts(176,33): error TS4111: Property 'message' comes from an index signature, so it must be accessed with ['message'].
src/recovery/recovery-admission-controller.ts(177,31): error TS4111: Property 'clock' comes from an index signature, so it must be accessed with ['clock'].
src/recovery/recovery-admission-controller.ts(180,24): error TS4111: Property 'originPeerId' comes from an index signature, so it must be accessed with ['originPeerId'].
src/recovery/recovery-admission-controller.ts(180,61): error TS4111: Property 'originPeerId' comes from an index signature, so it must be accessed with ['originPeerId'].
src/recovery/recovery-admission-controller.ts(181,24): error TS4111: Property 'senderPeerId' comes from an index signature, so it must be accessed with ['senderPeerId'].
src/recovery/recovery-admission-controller.ts(181,61): error TS4111: Property 'senderPeerId' comes from an index signature, so it must be accessed with ['senderPeerId'].
src/recovery/recovery-admission-controller.ts(182,22): error TS4111: Property 'originPeerId' comes from an index signature, so it must be accessed with ['originPeerId'].
src/recovery/recovery-admission-controller.ts(182,57): error TS4111: Property 'originPeerId' comes from an index signature, so it must be accessed with ['originPeerId'].
src/recovery/recovery-admission-controller.ts(183,27): error TS4111: Property 'issuerPeerId' comes from an index signature, so it must be accessed with ['issuerPeerId'].
src/recovery/recovery-admission-controller.ts(183,68): error TS4111: Property 'issuerPeerId' comes from an index signature, so it must be accessed with ['issuerPeerId'].
src/recovery/recovery-admission-controller.ts(187,16): error TS4111: Property 'originSequenceNumber' comes from an index signature, so it must be accessed with ['originSequenceNumber'].
src/recovery/recovery-admission-controller.ts(188,16): error TS4111: Property 'logicalTimestamp' comes from an index signature, so it must be accessed with ['logicalTimestamp'].
src/recovery/recovery-admission-controller.ts(189,14): error TS4111: Property 'sequenceNumber' comes from an index signature, so it must be accessed with ['sequenceNumber'].
src/recovery/recovery-admission-controller.ts(190,14): error TS4111: Property 'logicalTimestamp' comes from an index signature, so it must be accessed with ['logicalTimestamp'].
src/recovery/recovery-admission-controller.ts(195,16): error TS4111: Property 'membershipEpoch' comes from an index signature, so it must be accessed with ['membershipEpoch'].
src/recovery/recovery-admission-controller.ts(196,14): error TS4111: Property 'membershipEpoch' comes from an index signature, so it must be accessed with ['membershipEpoch'].
src/recovery/recovery-admission-controller.ts(197,19): error TS4111: Property 'membershipEpoch' comes from an index signature, so it must be accessed with ['membershipEpoch'].
```

**Classification:** **BLOCKING**

---

## 4. Findings Classification

| ID | Severity | Component | Finding Description |
|---|---|---|---|
| **F-M4.3a-1** | **BLOCKING** | `recovery-admission-controller.ts:176-197` | **TS4111 Compilation Failure:** Casting `event` to `Record<string, unknown>` and accessing properties via dot syntax violates `"noPropertyAccessFromIndexSignature": true` in `tsconfig.base.json:17`. Breaks package build (`npm run build`). |
| **F-M4.3a-2** | **NON-BLOCKING** | `quarantine-buffer.ts:243-246` | **In-Flight Shift Identity Gap:** `shiftDrain()` immediately removes operation identity from `identityIndex`. In M4.3c, the coordinator must ensure that duplicate admissions during in-flight drain are reconciled. |
| **F-M4.3a-3** | **PASS** | `recovery-state-machine.ts` | Complete 4-state lifecycle machine with transition table validation and exception-isolated notification. |
| **F-M4.3a-4** | **PASS** | `quarantine-buffer.ts` | Exact dual-constraint boundary enforcement (1,000 count, 4 MiB bytes, 4 KiB event cap) and generational double-buffering. |
| **F-M4.3a-5** | **PASS** | `future-epoch-buffer.ts` | Bounded future-epoch buffer with authentic capability verification and deterministic 5-second grace period timer. |
| **F-M4.3a-6** | **PASS** | Frozen Baseline | 100% untouched frozen M4.1–M4.2d baseline files (0 lines changed). |

---

## 5. Required Minimal Correction (For Implementation Mode)

To resolve **F-M4.3a-1** in subsequent authorized implementation mode:

In `packages/sync-engine/src/recovery/recovery-admission-controller.ts`:
Replace lines 174–198:
```typescript
interface InboundCandidateMessage {
  readonly originPeerId?: string;
  readonly senderPeerId?: string;
  readonly originSequenceNumber?: number;
  readonly logicalTimestamp?: number;
  readonly membershipEpoch?: number;
}

interface InboundCandidateClock {
  readonly originPeerId?: string;
  readonly sequenceNumber?: number;
  readonly logicalTimestamp?: number;
  readonly membershipEpoch?: number;
}

interface InboundCandidateEvent {
  readonly message?: InboundCandidateMessage;
  readonly clock?: InboundCandidateClock;
  readonly issuerPeerId?: string;
  readonly membershipEpoch?: number;
}
```
And typecast `event` to `InboundCandidateEvent` instead of `Record<string, unknown>`. Because `InboundCandidateEvent` defines named optional properties rather than an index signature, dot-property access compiles cleanly under `"noPropertyAccessFromIndexSignature": true`, eliminating all 17 TS4111 errors while preserving flexible event extraction.

---

## 6. Audit Conclusion & Final Declaration

The architectural structure, bounded algorithms, lifecycle transitions, and test suites of M4.3a are implemented to high distributed-systems standards. However, because `npm run build` fails with 17 TypeScript compilation errors, M4.3a cannot be accepted as clean and complete until this build failure is corrected.

**Final Audit Status:**

`M4.3a INDEPENDENT AUDIT FAIL — CORRECTION REQUIRED`
