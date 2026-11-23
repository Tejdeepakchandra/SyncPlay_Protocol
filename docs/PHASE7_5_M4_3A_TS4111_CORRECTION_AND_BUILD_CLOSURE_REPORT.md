# Phase 7.5 M4.3a — TS4111 Correction & Final Build Closure Report

**Author:** Senior TypeScript Engineer  
**Mode:** STRICTLY LIMITED IMPLEMENTATION CORRECTION  
**Milestone:** Phase 7.5 M4.3a  
**Audited Implementation Baseline:** `9bb37491b2359e46cf7449a87152bf30600b0e7c`  
**Approved Architecture Parent:** `2bb117c5db474a13307be65c6f68845f2bd74720`  
**Date:** November 24, 2026  

---

## 1. Original Blocking Finding

During the independent source-code audit of M4.3a (`9bb37491b2359e46cf7449a87152bf30600b0e7c`), whole-workspace production compilation (`npm run build`) failed with **17 compiler errors (`TS4111`)** in `@syncplay/sync-engine`:

```text
src/recovery/recovery-admission-controller.ts(176,33): error TS4111: Property 'message' comes from an index signature, so it must be accessed with ['message'].
src/recovery/recovery-admission-controller.ts(177,31): error TS4111: Property 'clock' comes from an index signature, so it must be accessed with ['clock'].
src/recovery/recovery-admission-controller.ts(180,24): error TS4111: Property 'originPeerId' comes from an index signature, so it must be accessed with ['originPeerId'].
...
src/recovery/recovery-admission-controller.ts(197,19): error TS4111: Property 'membershipEpoch' comes from an index signature, so it must be accessed with ['membershipEpoch'].
```

### Root Cause:
In `recovery-admission-controller.ts:175-177`, incoming events were cast to `Record<string, unknown>`. Because `tsconfig.base.json:17` sets `"noPropertyAccessFromIndexSignature": true`, TypeScript prohibits accessing properties of index-signature types using dot notation (e.g. `eventRecord.message` or `message.originPeerId`).

---

## 2. Exact Source Changes and Rationale

Modifications were strictly limited to `packages/sync-engine/src/recovery/recovery-admission-controller.ts`.

### 2.1 Defined Structural Interfaces
Introduced narrow structural interfaces representing the optional candidate fields inspected by the admission controller:

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

### 2.2 Replaced Broad Record Cast with Typed Structural Assertion
In `RecoveryAdmissionController.prototype.interceptRemoteEvent`:

```diff
-    const eventRecord = event as unknown as Record<string, unknown>;
-    const message = eventRecord.message as Record<string, unknown> | undefined;
-    const clock = eventRecord.clock as Record<string, unknown> | undefined;
+    const candidate = event as unknown as InboundCandidateEvent;
+    const message = candidate.message;
+    const clock = candidate.clock;

     const originPeerId =
       (typeof message?.originPeerId === 'string' && message.originPeerId.trim()) ||
       (typeof message?.senderPeerId === 'string' && message.senderPeerId.trim()) ||
       (typeof clock?.originPeerId === 'string' && clock.originPeerId.trim()) ||
-      (typeof eventRecord.issuerPeerId === 'string' && eventRecord.issuerPeerId.trim()) ||
+      (typeof candidate.issuerPeerId === 'string' && candidate.issuerPeerId.trim()) ||
       '';

     const rawSeq =
@@ -194,7 +216,7 @@ export class RecoveryAdmissionController {
     const rawEpoch =
       message?.membershipEpoch ??
       clock?.membershipEpoch ??
-      eventRecord.membershipEpoch;
+      candidate.membershipEpoch;
     const membershipEpoch =
       typeof rawEpoch === 'number' && Number.isInteger(rawEpoch) ? rawEpoch : null;
```

### Rationale:
Because `InboundCandidateEvent`, `InboundCandidateMessage`, and `InboundCandidateClock` define explicit named optional properties rather than index signatures (`[key: string]: unknown`), dot-notation access conforms fully to `"noPropertyAccessFromIndexSignature": true`, resolving all 17 TS4111 errors without using `any` or disabling compiler safety flags.

---

## 3. Explanation of Preserved Runtime Behavior

The changes are compile-time typing corrections only. Runtime execution is 100% identical to the audited implementation:

1. **Pre-Validation:** `if (!event || typeof event !== 'object')` continues to reject non-object or null events immediately with `REJECTED_INVALID`.
2. **Defensive Field Checking:** All optional chaining and type validations (`typeof message?.originPeerId === 'string'`, `Number.isInteger(rawSeq)`, etc.) remain active and untouched.
3. **Identity & Sequence Extraction:** The exact four-tier identity resolution (`message.originPeerId` -> `message.senderPeerId` -> `clock.originPeerId` -> `candidate.issuerPeerId`) and sequence number resolution are preserved.
4. **Validation Rejections:** Missing identity, non-integer sequence, non-integer epoch, or non-finite/oversized byte length reject with `REJECTED_INVALID`.
5. **Epoch Routing:**
   - Obsolete epoch ($E < \text{current}$): Discarded with `DISCARD_OBSOLETE_EPOCH`.
   - Future epoch ($E > \text{current}$): Buffered in `FutureEpochBuffer` (max 100 ops, 512 KiB).
   - Current epoch ($E = \text{current}$):
     - `ACTIVE`: `PASS_THROUGH`.
     - `RECOVERING` / `RECONCILING`: Retained in `QuarantineBuffer` (max 1,000 ops, 4 MiB).
     - `DEGRADED`: Rejected with `REJECTED_DEGRADED`.
6. **Local Intents:** Preserved fail-closed rejection in non-ACTIVE states (`REJECTED_RECOVERING` or `REJECTED_DEGRADED`).
7. **Buffer & Byte Limits:** The 1,000-op / 4 MiB quarantine limit, 4 KiB per-event cap, and 100-op / 512 KiB future-epoch limit are completely unchanged.

---

## 4. Verification Evidence

### 4.1 Focused Recovery Admission Tests
Executed: `npx vitest run packages/sync-engine/src/__tests__/recovery-admission.test.ts`
```text
 ✓ packages/sync-engine/src/__tests__/recovery-admission.test.ts (14 tests) 20ms

 Test Files  1 passed (1)
      Tests  14 passed (14)
   Duration  543ms
```

### 4.2 All Four M4.3a Focused Test Suites
Executed: `npx vitest run packages/sync-engine/src/__tests__/recovery-state-machine.test.ts packages/sync-engine/src/__tests__/quarantine-buffer.test.ts packages/sync-engine/src/__tests__/future-epoch-buffer.test.ts packages/sync-engine/src/__tests__/recovery-admission.test.ts`
```text
 ✓ packages/sync-engine/src/__tests__/recovery-state-machine.test.ts (19 tests) 22ms
 ✓ packages/sync-engine/src/__tests__/quarantine-buffer.test.ts (14 tests) 48ms
 ✓ packages/sync-engine/src/__tests__/recovery-admission.test.ts (14 tests) 18ms
 ✓ packages/sync-engine/src/__tests__/future-epoch-buffer.test.ts (12 tests) 33ms

 Test Files  4 passed (4)
      Tests  59 passed (59)
   Duration  1.25s
```

### 4.3 Full Monorepo Test Suites (`npm test`)
```text
> @syncplay/control-events@0.1.0 test: 49 passed (6 test files)
> @syncplay/peer-mesh@0.1.0 test: 108 passed (6 test files)
> @syncplay/protocol@0.1.0 test: 337 passed (6 test files)
> @syncplay/sync-engine@0.1.0 test: 837 passed (40 test files)
> @syncplay/bootstrap-server@0.1.0 test: 91 passed (3 test files)

Total Tests: 1,422 passed / 1,422 total (0 failed, 0 skipped)
```

### 4.4 Full Production Build (`npm run build`)
```text
> syncplay-protocol@0.0.0 build
> npm run build --workspaces --if-present

> @syncplay/control-events@0.1.0 build
> tsc (exit code 0)

> @syncplay/peer-mesh@0.1.0 build
> tsc (exit code 0)

> @syncplay/protocol@0.1.0 build
> tsc (exit code 0)

> @syncplay/sync-engine@0.1.0 build
> tsc (exit code 0)  <-- PREVIOUSLY FAILED WITH 17 TS4111 ERRORS; NOW COMPILES WITH 0 ERRORS

> @syncplay/bootstrap-server@0.1.0 build
> tsc (exit code 0)

> @syncplay/demo@0.1.0 build
> vite build (built in 1.65s, exit code 0)
```

---

## 5. Frozen Baseline Diff Verification

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

**Frozen Implementation Integrity:** **100% UNTOUCHED (0 lines modified)**

---

## 6. Git Closure & Working Tree Status

- **Audited Implementation Baseline:** `9bb37491b2359e46cf7449a87152bf30600b0e7c`
- **Correction Commit Hash:** `398d045bf017c96bbb69af198846e66b59bf7aaa`
- **Correction Commit Message:** `fix(phase-7.5-m4): resolve ts4111 index signature property access in recovery admission controller (m4.3a)`
- **Parent Commit (HEAD^):** `9bb37491b2359e46cf7449a87152bf30600b0e7c`
- **Current Branch:** `main`
- **Modified Source File:** `packages/sync-engine/src/recovery/recovery-admission-controller.ts`
- **Working Tree:** Clean (`git status --short` is empty).

---

## 7. Remaining Limitations and Non-Blocking Observations

- **F-M4.3a-2 (Deferred to M4.3c):** In `quarantine-buffer.ts:243-246`, `shiftDrain()` removes operation identity from `identityIndex`. As scheduled in the approved M4.3-R4 roadmap, single-operation in-flight custody ($N_{\text{in-flight}} \le 1$) and concurrent duplicate arrival reconciliation will be managed by `DRAINER_IN_FLIGHT` within `IQuarantineDrainer` during M4.3c.

---

## 8. Final Status Declaration

`M4.3a CORRECTION COMPLETE — READY FOR FINAL ACCEPTANCE`
