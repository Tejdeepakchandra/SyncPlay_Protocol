# Phase 7.5 Sub-Milestone M5.0-R12 Correction Report: Atomic Reset Retirement, Settlement Barrier Reconciliation & Terminal Archive Governance

**Target:** Phase 7.5 Sub-Milestone M5.0-R12
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Starting Baseline Commit (M5.0-R11):** `7b7ab434f0418ebbcd72ee5c3bd20b20cabf392a`
**Direct Parent of Baseline Commit:** `2cbcb4064f20fe562db3777ec7440b405158d3bc` (M5.0-R10)
**Authoritative Accepted Implementation Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## 1. Executive Summary & Mandatory Scope Statement

This document records the architectural resolutions for Sub-Milestone M5.0-R12 in response to the Engineering Authority's review of M5.0-R11.

In accordance with strict architectural governance:
- **Zero Production Code Modified:** No files in `packages/*/src/**/*.ts` or `packages/*/src/**/*.tsx` were modified.
- **Zero Test Files Modified:** No files in `packages/**/__tests__/**` were modified.
- **Zero Package Manifests / Lockfiles Modified:** `package.json` and `package-lock.json` remain untouched.
- **Zero Configuration Modified:** `tsconfig*.json`, build scripts, and linter configs remain untouched.
- **Zero Frozen Phase 1–7.4 or M4 Baselines Modified:** All prior baselines remain immutable.
- **Documentation Only:** Modifications are strictly restricted to the 5 M5 specifications:
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`
  - `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`
  and this correction report `docs/PHASE7_5_M5_0_R12_CORRECTION_REPORT.md`.

---

## 2. Mandatory Repository & Source Code Baseline Audit

### Repository Baseline Verification
- **Branch:** `main`
- **Initial Working Tree:** Clean
- **Starting HEAD:** `7b7ab434f0418ebbcd72ee5c3bd20b20cabf392a` (M5.0-R11)
- **Direct Parent of Starting HEAD:** `2cbcb4064f20fe562db3777ec7440b405158d3bc` (M5.0-R10)
- **R11 Changed Files:** Exactly 6 files (5 specifications + `PHASE7_5_M5_0_R11_CORRECTION_REPORT.md`), 345 insertions(+), 115 deletions(-). Direct descent verified from M5.0-R10.

### Exact Source Citations & Implementation Separation

| Source File | Line Range | Existing M4.3 Baseline Behavior | Proposed M5 Architecture Specification |
|---|---|---|---|
| `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 26–50, 150–190 | `MAX_QUARANTINE_OPERATIONS = 1000`, `MAX_QUARANTINE_TOTAL_BYTES = 4194304`, `MAX_QUARANTINE_EVENT_BYTES = 4096`. Dual count/byte limits enforced on enqueue. | Unchanged limits; source buffer for R12-A retirement transaction. |
| `packages/sync-engine/src/recovery/quarantine-drainer.ts` | 55–75, 290–345 | `DestinationAcceptanceReceipt` defines 6 variants (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, `CAPACITY_REJECTED`). | Extends receipt with proposed `ATTEMPT_INVALIDATED` variant; adds proposed `drainSettledBarrier()` promise. |
| `packages/sync-engine/src/recovery/recovery-coordinator.ts` | 110–145, 237–239, 668–686 | Exposes `public get state(): RecoveryState`. `resetRecovery(initiatedBy: string)` in M4.3 increments `activeAttemptId` and clears timers. | Proposes `resetRecovery(capability, resetAction)` with R12-B 10-step lifecycle and R12-A retirement transaction. |
| `packages/sync-engine/src/recovery/engine-core-state.ts` | 80–115 | `EngineCoreState` sealed via `Object.seal()`. Top-level container property additions/deletions blocked. | Proposes R11-B Candidate Isolation contract: fresh/CoW allocation of nested mutable structures. |
| `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | 883–909, 929–960 | `replaceCoreStateAtomic` verifies capability reference identity; replaces `this.core = candidate;`. `flushDeferredNotifications` dispatches to listeners with try/catch. | Preserved; neither method synthesizes events automatically, confirming Step 10 zero-notification contract. |
| `packages/sync-engine/src/recovery/snapshot-installer.ts` | 1–60 | 4-stage validation, staging, and atomic commit architecture. | Preserved; candidate core held in private `WeakMap`. |
| `TerminalQuarantineArchive` | N/A | Does not exist in accepted M4.3 source code. | Formally specified as a **Proposed M5.1 Component** with 500 entries / 2 MiB bounds and 2-stage export. |

---

## 3. R12-A — Atomic Clean-Reset Quarantine Retirement

### Problem Statement
A fundamental capacity mismatch exists between `QuarantineBuffer` (`MAX_QUARANTINE_OPERATIONS = 1000`, `MAX_QUARANTINE_TOTAL_BYTES = 4 MiB`) and `TerminalQuarantineArchive` (`MAX_TERMINAL_ARCHIVE_ENTRIES = 500`, `MAX_TERMINAL_ARCHIVE_TOTAL_BYTES = 2 MiB`). A quarantine buffer containing e.g. 800 operations cannot be transferred into an archive with only 400 free entries. Unconditional retirement could cause partial retirement, silent dropping, or silent eviction.

### Architectural Solution: Multi-Stage Reservation & Staged Batch Commit Transaction
`REINITIALIZE_CLEAN` executes an atomic multi-stage transaction:

1. **Stage 1: Retirement Set Discovery & Size Calculation (Non-Destructive):**
   - Inspects all operations currently in `QuarantineBuffer` (active generation and any detached drain generation) + any un-linearized in-flight operation.
   - Computes:
     - `requiredEntryCount = quarantineBuffer.totalRetainedCount` (including in-flight operation if restored).
     - `requiredTotalBytes = quarantineBuffer.totalRetainedBytes` (sum of exact `serializedByteLength`).
   - Queries available archive capacity:
     - `availableEntries = MAX_TERMINAL_ARCHIVE_ENTRIES - terminalArchive.size`.
     - `availableBytes = MAX_TERMINAL_ARCHIVE_TOTAL_BYTES - terminalArchive.totalBytes`.
2. **Stage 2: Capacity Verification & Pre-Reservation Guard:**
   - Evaluates: `requiredEntryCount <= availableEntries` AND `requiredTotalBytes <= availableBytes`.
   - **Fail-Closed Guard:** If capacity is insufficient (either entry count or byte capacity):
     - **DO NOT** partially retire the quarantine buffer!
     - **DO NOT** commit the clean baseline anchor!
     - **DO NOT** release ownership of any operation!
     - Preserves old engine root (`this.core` is untouched).
     - Preserves complete quarantine ownership in `QuarantineBuffer`.
     - Keeps recovery mode locked (`engine.setRecoveryMode(true)`).
     - Keeps live inputs blocked (`REJECTED_DEGRADED`).
     - Transitions/remains in `DEGRADED` with failure reason: `TERMINAL_ARCHIVE_CAPACITY_INSUFFICIENT` or `TERMINAL_ARCHIVE_BYTE_LIMIT_INSUFFICIENT` (diagnostics include required vs available entries and bytes).
3. **Stage 3: Staged Archive Entry Construction & Atomic Batch Commit:**
   - If capacity is sufficient, constructs detached array of `TerminalQuarantineEntry` objects with `rejectionStatus: 'REINITIALIZE_CLEAN_PURGED'`.
   - Atomically inserts the entire batch into `TerminalQuarantineArchive` (`terminalArchive.archiveBatch(stagedBatch)`).
4. **Stage 4: Acknowledged Quarantine Retirement:**
   - Strictly **after** `archiveBatch()` confirms successful insertion:
   - Clears `QuarantineBuffer` (`quarantineBuffer.clear()`).
   - Ownership successfully transitions from `QuarantineBuffer` to `TerminalQuarantineArchive`.
5. **Stage 5: Clean-Baseline Installation & Notification Release:**
   - Executes Step 8 (`replaceCoreStateAtomic`), Step 9 (transition `ACTIVE`), and Step 10 (`flushDeferredNotifications`).

### Failure Matrix for Retirement Stages

| Failure Scenario | Engine Core State | QuarantineBuffer State | TerminalArchive State | Lifecycle Outcome |
|---|---|---|---|---|
| **Insufficient entry capacity** | Completely untouched | Retains all operations ($N$) | Untouched (no insertions) | Enters `DEGRADED` (`TERMINAL_ARCHIVE_CAPACITY_INSUFFICIENT`); live inputs blocked |
| **Insufficient byte capacity** | Completely untouched | Retains all operations ($N$) | Untouched (no insertions) | Enters `DEGRADED` (`TERMINAL_ARCHIVE_BYTE_LIMIT_INSUFFICIENT`); live inputs blocked |
| **Archive insertion exception** | Completely untouched | Retains all operations ($N$) | Uncommitted batch rolled back | Enters `DEGRADED` (`TERMINAL_ARCHIVE_INSERTION_FAILED`); live inputs blocked |
| **Quarantine mutation during prep** | Completely untouched | Gated/frozen | Untouched | Stale transaction continuation aborts via monotonic attempt fencing |
| **Attempt invalidated during prep** | Completely untouched | Retains all operations | Untouched | Continuation aborts immediately with zero side-effects |
| **Process retry / re-submission** | Evaluated afresh | Intact | Intact | New attempt runs verification afresh; idempotent; zero duplicate archiving |
| **Post-archive failure before clear** | Staged/Untouched | Retains buffer operations | Batch committed | Run-to-completion prevents failure in synchronous memory; recovery re-sync clears buffer |

---

## 4. R12-B — Settlement Barrier and Clean-Reset Commit Reconciliation

### Problem Statement
R11 required awaiting `drainSettledBarrier()` before clean-baseline install, but also described an old in-flight operation settling after clean-baseline commit. If clean baseline commit waits for `drainSettledBarrier()`, an in-flight operation cannot settle after the commit.

### Authoritative 10-Step Lifecycle Sequence
1. **Step 1: Administrative Reset Requested:** Invocation of `resetRecovery(capability, resetAction)` strictly from `DEGRADED`.
2. **Step 2: Attempt Invalidation & Monotonic Advance:** `newAttemptId = ++this.currentAttemptId`. All prior attempt timers, transfer pipelines, and callbacks are fenced.
3. **Step 3: Pre-Commit Drainer Fencing:** Drainer flags `_inFlightRecord.invalidated = true` (if an operation is in flight). Drainer prohibits any new dequeue ($N_{\text{in-flight}} \le 1$).
4. **Step 4: Mandatory Settlement Barrier Execution:** Coordinator awaits `await this.quarantineDrainer.drainSettledBarrier()`.
   - **Watchdog Timeout Invariant:** The settlement barrier is guarded by `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`. A timeout is NOT cancellation: it does not cancel the underlying Promise and does NOT clear the in-flight slot. If the barrier times out, coordinator transitions to `DEGRADED` (`DRAIN_HANG_TIMEOUT`), the in-flight slot remains owned ($N_{\text{in-flight}} = 1$), and the clean baseline is **NOT** committed!
5. **Step 5: In-Flight Operation Settlement & Custody Resolution:** The underlying asynchronous operation actually resolves or rejects on the JavaScript event loop.
   - Mandatory attempt-ID fencing at `admitDrainedOperation` validates `attemptId === coordinator.currentAttemptId`. Because the attempt was fenced/invalidated, the engine returns typed receipt `ATTEMPT_INVALIDATED` (`STALE_ATTEMPT_FENCED`) with **zero engine state mutation**!
   - In-flight custody is resolved: unshifted back to `QuarantineBuffer` head via `unshiftDrain(op)`.
   - In-flight slot is released: `this._inFlightRecord = null` ($N_{\text{in-flight}} \to 0$).
6. **Step 6: Settlement Barrier Resolution:** The barrier promise resolves. The in-flight slot is verified empty ($N_{\text{in-flight}} == 0$). No new drain pass can begin until barrier resolution.
7. **Step 7: Quarantine Retirement Transaction (Multi-Stage Atomic Batch — R12-A):** Under `REINITIALIZE_CLEAN`, the multi-stage retirement transaction executes (pre-reservation, staging, batch commit to `TerminalQuarantineArchive`, acknowledged quarantine clear). Under `PURGE_AND_RETRY`, operations remain in `QuarantineBuffer` for the new attempt.
8. **Step 8: State Replacement (Irrevocable Single Root Pointer Replacement):** Under `REINITIALIZE_CLEAN`, `engine.replaceCoreStateAtomic(cleanBaseline, capability)` executes single synchronous root-pointer replacement. Under `PURGE_AND_RETRY`, the existing log is preserved.
9. **Step 9: Lifecycle Transition:** Under `REINITIALIZE_CLEAN`, `engine.setRecoveryMode(false)` and coordinator transitions to `ACTIVE`. Under `PURGE_AND_RETRY`, coordinator resets to `RECOVERING` under `newAttemptId`, maintaining `setRecoveryMode(true)`.
10. **Step 10: Notification Release:** Under `REINITIALIZE_CLEAN`, `engine.flushDeferredNotifications()` dispatches deferred notifications via non-throwing dispatch (delivering 0 notifications if none were enqueued).

### Reconciliation of Settlement Timing Cases

| Case | Lifecycle Occurrence | Protocol Mandated Behavior |
|---|---|---|
| **Case 1: Operation unsettled when reset requested** | In-flight drain step pending during `resetRecovery` call | Fences old attempt; awaits `drainSettledBarrier()`; preserves in-flight slot until actual settlement; proceeds only after barrier resolves and $N_{\text{in-flight}} = 0$. |
| **Case 2: Operation settles before clean-baseline commit** | In-flight drain step settles at Step 5 while barrier is awaited | Receives `ATTEMPT_INVALIDATED`; unshifted back into `QuarantineBuffer`; included in complete Step 7 retirement set before committing clean baseline. |
| **Case 3: Decoupled callback continuation resumes post-commit** | Asynchronous transport/logging callbacks resume after Step 8 commit | Attempt ID and lifecycle guards check `myAttemptId === currentAttemptId` and `state === 'RECONCILING'`; any late callback mutation or external side-effect is suppressed as inert. |

---

## 5. R12-C — Terminal Archive Administration Authority & Acknowledged Export

### Problem Statement
R11 proposed `RecoveryInstallationCapability` for archive export/clear, conflating engine root installation authority with diagnostic archive administration authority.

### Dedicated Capability Contract (Proposed M5.1 Interface)
```typescript
// Declared in packages/sync-engine/src/recovery/terminal-archive-custody.ts [PROPOSED — M5.1]
const TERMINAL_ARCHIVE_BRAND: unique symbol = Symbol('TERMINAL_ARCHIVE_BRAND');

export interface TerminalArchiveAdministrationCapability {
  readonly [TERMINAL_ARCHIVE_BRAND]: true;
  readonly sessionId: string;
  readonly authority: 'terminal-archive-administration';
  readonly token: string;
}
```

- **Strict Privilege Confinement:** `TerminalArchiveAdministrationCapability` authorizes ONLY archive inspection, export preparation, and export acknowledgement. It CANNOT authorize engine root replacement, snapshot staging/committing, recovery attempt creation, quarantine mutation, or playback-state changes.
- **Safe Two-Stage Export Process:**
  1. **Stage 1: Prepare Export (`prepareArchiveExport(capability): TerminalArchiveExportBatch`):**
     - Produces an immutable, detached snapshot of all archived entries.
     - Generates `exportToken = archive-export-${archiveGeneration}-${Date.now()}-${random}`.
     - Computes batch metadata: `entryCount`, `totalSerializedBytes`, `archiveGeneration`, and RFC 8785 SHA-256 batch digest.
     - **Inviolable Invariant:** The archive is NOT cleared or mutated! Entries remain safely retained.
  2. **Stage 2: Acknowledged Clear (`acknowledgeArchiveExport(exportToken, capability): boolean`):**
     - Requires explicit authorized acknowledgement passing the `exportToken` and capability.
     - Clears ONLY the exact entries covered by that export batch. Any entries added after export preparation are strictly preserved!
     - If acknowledgement fails, times out, or throws: all archive entries remain safely retained.
- **Protocol Preservation Boundary Statement:** The protocol guarantees custody preservation until explicit caller acknowledgement; it does not claim to verify external disk durability if the administrative caller's external storage subsystem fails after acknowledgement.
- **Capability Custody:** Created at composition root, transferred privately to coordinator/administration harness via `AuthorizedArchiveCapabilities`, verified via exact object reference identity (`===`).

---

## 6. R12-D — Strict Terminal Archive Memory Bounds

### Explicit Hard Bound Constants
- `MAX_TERMINAL_ARCHIVE_ENTRIES = 500`
- `MAX_TERMINAL_ARCHIVE_TOTAL_BYTES = 2097152` bytes (2.0 MiB)
- `MAX_TERMINAL_ENTRY_BYTES = 4096` bytes (4.0 KiB, matching `MAX_QUARANTINE_EVENT_BYTES`)
- `MAX_TERMINAL_ERROR_LENGTH = 512` characters
- `MAX_TERMINAL_REASON_LENGTH = 256` characters
- `MAX_TERMINAL_PEER_ID_LENGTH = 128` characters
- `MAX_TERMINAL_LOGICAL_EVENT_ID_LENGTH = 128` characters

### Worst-Case Theoretical Memory Bound Calculation
1. **Serialized Wire Payload Weight:**
   $$\text{Max Wire Bytes} = \min(500 \times 4096, 2 \text{ MiB}) = 2,048,000 \text{ bytes} \le 2.0 \text{ MiB}$$
2. **V8 JavaScript Object Heap Overhead:**
   - Per-entry object overhead: Container object (~64 bytes) + `QuarantinedOperation` envelope (~64 bytes) + numeric timestamp/epoch properties (~32 bytes).
   - Bounded string references: `error` (max 512 chars, ~536 bytes UTF-16), `reason` (max 256 chars, ~280 bytes), `peerId` (max 128 chars, ~152 bytes), `logicalEventId` (max 128 chars, ~152 bytes).
   - Total per-entry heap footprint: $\le 1.25 \text{ KiB}$ object overhead + $4.0 \text{ KiB}$ payload $\approx 5.25 \text{ KiB}$.
   - Worst-case total heap memory for 500 entries:
     $$\text{Max Heap Footprint} = 500 \times 5.25 \text{ KiB} \approx 2,625 \text{ KiB} \approx 2.56 \text{ MiB}$$
3. **Oversized Entry & Capacity Behavior:**
   - An entry exceeding `MAX_TERMINAL_ENTRY_BYTES` (4096) or string limits is treated as a **Custody-Transfer Failure**: identity fields are NOT truncated, in-flight slot is NOT released, and the coordinator enters `DEGRADED` (`TERMINAL_ENTRY_OVERSIZED`).
   - Aggregate capacity breach enters `DEGRADED` (`TERMINAL_ARCHIVE_OVERFLOW`). No silent eviction.

---

## 7. Unified Reset-Policy Quarantine Fate Matrix

| Reset Strategy | In-Flight Un-linearized Operation Fate | QuarantineBuffer Pending Operations Fate | Already-Linearized Log Operations Fate | When Can Draining Resume? |
|---|---|---|---|---|
| **`PURGE_AND_RETRY`** | Receives `ATTEMPT_INVALIDATED`; restored to head of `QuarantineBuffer` via `unshiftDrain(op)` | Retained in buffer custody; re-evaluated against new recovery attempt | Retained in `operationLog`; deduplicated during retry (`ALREADY_REPRESENTED` / `ALREADY_OWNED_DUPLICATE`) | Only after settlement barrier resolves, slot is empty ($N_{\text{in-flight}} = 0$), and new attempt enters `RECONCILING` |
| **`REINITIALIZE_CLEAN`** | Settles at Step 5 prior to commit; restored to buffer; included in Step 7 atomic retirement transaction | All pending operations atomically committed to `TerminalQuarantineArchive` via Stage 3 transaction (`REINITIALIZE_CLEAN_PURGED`) | Entire engine root replaced with clean baseline anchor via `replaceCoreStateAtomic`; prior log discarded | Pre-reset quarantine is permanently retired; live operation ingestion begins fresh in `ACTIVE` state |

---

## 8. Notification Contract & Zero-Notification Clarification

In existing accepted source (`leaderless-sync-engine.ts:883-960`), neither `replaceCoreStateAtomic` nor `setRecoveryMode(false)` pushes to `deferredStateChanges`. Therefore, Step 10 may deliver **zero notifications** unless an explicit clean baseline state change notification is specifically enqueued by caller or implementation. No clean baseline notification is guaranteed to exist automatically.
- **Observer Boundary Invariant:** No notification is EVER delivered to external listeners before Step 9 reaches `ACTIVE` and recovery mode is `false`. If pre-ACTIVE steps fail, recovery mode is restored, deferred notifications are never flushed, and the engine halts in `DEGRADED`.

---

## 9. Verification & Document Cross-Audit Summary

| Document | Sections Modified | Purpose |
|---|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Status line, 2.1.2, 3.4 (1–10 sequence, R12-A transaction, R12-B barrier, R12-C export, R12-D bounds) | Formalized multi-stage retirement transaction, 10-step lifecycle, dedicated archive capability, strict numeric bounds, and zero-notification clarification. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Status line, Section 3.1 (`IRuntimeRecoveryCoordinator`), Section 3.2.1 (`ITerminalQuarantineArchive`) | Added `TerminalArchiveAdministrationCapability`, 2-stage export methods (`prepareArchiveExport`, `acknowledgeArchiveExport`), and strict numeric bound constants. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Status line, M5.0, M5.1 | Added implementation tasks and verification deliverables for R12-A, R12-B, R12-C, R12-D. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Status line, AC-M5-01, AC-M5-27 | Updated criteria to enforce multi-stage retirement transaction, 10-step barrier reconciliation, strict archive memory bounds, and dedicated archive capability. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Status line, R-M5-06, R-M5-08 | Added mitigations and controls for retirement transaction pre-reservation, 10-step barrier reconciliation, archive capability separation, and numeric memory bounds. |

---

## 10. Conclusion & Final Readiness Declaration

All four findings from the M5.0-R11 review have been resolved with mathematical precision, grounded directly in the accepted TypeScript engine implementation, `EngineCoreState` container architecture, and Node.js/JavaScript run-to-completion concurrency semantics.

- Milestone M5.0-R12 is **COMPLETE**.
- Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED**.
- The M5.0-R12 documentation baseline is fully consistent and ready for final architecture freeze review.
