# Phase 7.5 Sub-Milestone M5.0-R13 Correction Report: Transactional Quarantine Ownership Transfer, Latched Timeout Fencing & Archive Export Identity

**Target:** Phase 7.5 Sub-Milestone M5.0-R13
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Starting Baseline Commit (M5.0-R12):** `59df88fdf7d649f2cce59ccab2f69332beb91f15`
**Direct Parent of Baseline Commit:** `7b7ab434f0418ebbcd72ee5c3bd20b20cabf392a` (M5.0-R11)
**Authoritative Accepted Implementation Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## 1. Executive Summary & Mandatory Scope Statement

This document records the architectural resolutions for Sub-Milestone M5.0-R13 in response to the Engineering Authority's review of M5.0-R12.

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
  and this correction report `docs/PHASE7_5_M5_0_R13_CORRECTION_REPORT.md`.

---

## 2. Mandatory Repository & Source Code Baseline Audit

### Repository Baseline Verification
- **Branch:** `main`
- **Initial Working Tree:** Clean
- **Starting HEAD:** `59df88fdf7d649f2cce59ccab2f69332beb91f15` (M5.0-R12)
- **Direct Parent of Starting HEAD:** `7b7ab434f0418ebbcd72ee5c3bd20b20cabf392a` (M5.0-R11)
- **R12 Changed Files:** Exactly 6 files (5 specifications + `PHASE7_5_M5_0_R12_CORRECTION_REPORT.md`), 447 insertions(+), 128 deletions(-). Direct descent verified from M5.0-R11.

### Exact Source Citations & Implementation Separation

| Source File | Line Range | Existing M4.3 Baseline Behavior | Proposed M5 Architecture Specification |
|---|---|---|---|
| `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 26–50, 150–190 | `MAX_QUARANTINE_OPERATIONS = 1000`, `MAX_QUARANTINE_TOTAL_BYTES = 4194304`, `MAX_QUARANTINE_EVENT_BYTES = 4096`. Dual limits enforced on enqueue. | Unchanged limits; source buffer for R13-A retirement transaction. |
| `packages/sync-engine/src/recovery/quarantine-drainer.ts` | 55–75, 284–345 | `DestinationAcceptanceReceipt` defines 6 variants. Single in-flight record `_inFlightRecord` held synchronously. | Extends receipt with proposed `ATTEMPT_INVALIDATED` variant; adds proposed `drainSettledBarrier()` promise. |
| `packages/sync-engine/src/recovery/recovery-coordinator.ts` | 110–145, 668–710 | `resetRecovery(initiatedBy: string)` in M4.3 increments `activeAttemptId` and clears timers. | Proposes `resetRecovery(capability, resetAction)` with R13-B latched attempt states and R13-A retirement transaction. |
| `packages/sync-engine/src/recovery/recovery-state-machine.ts` | 49–54 | `VALID_STATE_TRANSITIONS` defines: `DEGRADED: new Set(['RECOVERING'])`. Direct transition `DEGRADED -> ACTIVE` throws. | Proposes authoritative 2-step transition `DEGRADED -> RECOVERING -> ACTIVE` for `REINITIALIZE_CLEAN` (R13-E). |
| `packages/sync-engine/src/recovery/engine-core-state.ts` | 80–115 | `EngineCoreState` sealed via `Object.seal()`. Top-level container property additions/deletions blocked. | Proposes R11-B Candidate Isolation contract: fresh/CoW allocation of nested mutable structures. |
| `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | 883–909, 929–960 | `replaceCoreStateAtomic` verifies capability reference identity; replaces `this.core = candidate;`. `flushDeferredNotifications` dispatches via try/catch. | Preserved; neither method synthesizes events automatically, confirming Step 10 zero-notification contract. |
| `packages/sync-engine/src/recovery/snapshot-installer.ts` | 1–60 | 4-stage validation, staging, and atomic commit architecture. | Preserved; candidate core held in private `WeakMap`. |
| `TerminalQuarantineArchive` | N/A | **Does not exist in accepted M4.3 source code.** | Formally specified as a **Proposed M5.1 Component** with 500 entries / 2 MiB bounds and single-lease export. |

---

## 3. R13-A — Transactional Quarantine Ownership Transfer

### Problem Statement
In R12, inserting into `TerminalQuarantineArchive` and clearing `QuarantineBuffer` were described as sequential steps. In JavaScript run-to-completion, an unhandled exception or process termination between these operations could leave the system in an ambiguous state: either duplicate entries would be inserted upon retry, or operations would be silently lost.

### Explicit Idempotent Retirement Transaction Record
To guarantee absolute custody preservation and idempotent retry recovery, every clean reset executes under an explicit `QuarantineRetirementTransaction`:
```typescript
export interface QuarantineRetirementTransaction {
  readonly retirementTransactionId: string; // e.g. "retire-tx-${quarantineGen}-${attemptId}-${uuid}"
  readonly quarantineGeneration: number;     // Exact generation of QuarantineBuffer being retired
  readonly operationIdentities: readonly string[]; // Array of unique logicalEventId strings
  readonly entryCount: number;               // Exact count of operations in retirement set
  readonly totalSerializedBytes: number;     // Sum of exact UTF-8 wire lengths of operations
  readonly archiveReservationId: string;     // Reservation token returned by TerminalQuarantineArchive
  status:
    | 'PREPARING'
    | 'RESERVED'
    | 'STAGED'
    | 'COMMITTED_ARCHIVE'
    | 'RETIRED_SOURCE'
    | 'COMPLETED'
    | 'FAILED'
    | 'ROLLED_BACK';
  terminalOutcome?:
    | 'SUCCESS'
    | 'CAPACITY_INSUFFICIENT'
    | 'GENERATION_MISMATCH'
    | 'INSERTION_FAILED'
    | 'ATTEMPT_FENCED'
    | 'TIMEOUT';
}
```

### 10-Stage Idempotent Transaction Lifecycle
1. **Stage 1 (Quarantine Generation Freeze):** Coordinator freezes current quarantine generation (`quarantineBuffer.freezeGeneration()`). Newly arriving operations in `DEGRADED` are rejected (`REJECTED_DEGRADED`) or buffered into a subsequent generation without touching the retirement generation.
2. **Stage 2 (Settle All In-Flight Work):** Confirms Step 6 has resolved and $N_{	ext{in-flight}} == 0$. If an in-flight operation was restored to buffer head, it is now frozen inside the retirement generation.
3. **Stage 3 (Non-Destructive Retirement Set Construction):** Constructs an immutable snapshot of all operations in the frozen generation without mutating buffer ownership.
4. **Stage 4 (Identity, Count & Wire Byte Verification):** Verifies exact `entryCount` and exact `totalSerializedBytes` (sum of `rawSerializedLength` / canonical UTF-8 bytes). Validates that every operation has a valid `logicalEventId` and wire length $\le 4096$ bytes.
5. **Stage 5 (Capacity Pre-Reservation):** Requests reservation from archive against the current archive generation:
   `reservationId = terminalArchive.reserveCapacity(entryCount, totalSerializedBytes, terminalArchive.archiveGeneration)`.
   If available entries ($500 - 	ext{size}$) or available bytes ($2	ext{ MiB} - 	ext{totalBytes}$) are insufficient, the reservation throws `CAPACITY_INSUFFICIENT` and transaction transitions to `FAILED`.
6. **Stage 6 (Staged Detached Entry Assembly):** Assembles detached `TerminalQuarantineEntry` records (`rejectionStatus: 'REINITIALIZE_CLEAN_PURGED'`). Records are deeply frozen (`Object.freeze`).
7. **Stage 7 (Idempotent Archive Batch Commit):** Commits the batch to archive under `retirementTransactionId`:
   `receipt = terminalArchive.commitRetirementBatch(reservationId, stagedBatch, retirementTransactionId)`.
   If the archive already committed this `retirementTransactionId` (e.g. on retry), it returns the existing confirmation without duplicate insertion. Transaction status transitions to `COMMITTED_ARCHIVE`.
8. **Stage 8 (Acknowledged Source Retirement):** Strictly after Step 7 confirmation, coordinator instructs buffer:
   `quarantineBuffer.retireCommittedBatch(operationIdentities)`.
   The buffer removes exactly the specified operations and decrements retained counters. Transaction status transitions to `RETIRED_SOURCE`.
9. **Stage 9 (Zero-Custody Verification):** Coordinator asserts:
   `quarantineBuffer.hasAny(operationIdentities) === false` AND `quarantineDrainer.isInFlight() === false`.
   Every operation in the retirement set now has unambiguous terminal custody in `TerminalQuarantineArchive`. Transaction status transitions to `COMPLETED`.
10. **Stage 10 (Authorize Clean Root Installation):** Only upon reaching `COMPLETED` is the coordinator permitted to invoke `engine.replaceCoreStateAtomic(cleanBaseline, capability)`.

### In-Process Atomicity vs. Process-Crash Durability Limitation
- **Strict In-Process Atomicity:** The transaction provides complete in-memory consistency, exception isolation, and idempotent retry recovery within the running JavaScript process.
- **Process-Crash Durability Boundary:** `QuarantineBuffer` and `TerminalQuarantineArchive` are in-memory data structures. A process termination, power loss, or unhandled OS signal obliterates the Node.js / browser heap. The protocol does **NOT** claim process-crash durability across process termination when running without an external persistent WAL. Upon process restart, the engine initializes from clean configuration or external snapshot.

### Complete Transaction Failure Recovery Matrix (R13-A)

| Failure Scenario | Engine Core State | QuarantineBuffer State | TerminalArchive State | Transaction Status & Outcome | Recovery Behavior |
|---|---|---|---|---|---|
| **1. Capacity reservation failure** | Completely untouched | Retains all operations ($N$) | Untouched (0 reserved) | `FAILED` (`CAPACITY_INSUFFICIENT`) | Aborts to `DEGRADED`; zero buffer mutation; diagnostic logged |
| **2. Archive generation changed after reservation** | Completely untouched | Retains all operations ($N$) | Reservation invalidated | `ROLLED_BACK` (`GENERATION_MISMATCH`) | Releases reservation; restarts transaction afresh at Stage 5 |
| **3. Quarantine generation changed after staging** | Completely untouched | Gated / preserved | Untouched | `FAILED` (`GENERATION_MISMATCH`) | Aborts transaction; verifies buffer invariants; transitions `DEGRADED` |
| **4. Archive insertion throws before commit** | Completely untouched | Retains all operations ($N$) | Batch discarded; reservation freed | `FAILED` (`INSERTION_FAILED`) | Aborts to `DEGRADED` (`TERMINAL_ARCHIVE_INSERTION_FAILED`); zero loss |
| **5. Archive commit succeeds but source retirement throws** | Completely untouched | Retains operations | Batch committed under `retirementTxId` | `COMMITTED_ARCHIVE` | **Recoverable State:** Retry discovers existing transaction via `hasTransaction(txId)`; skips duplicate archive insert; retries Stage 8 |
| **6. Duplicate transaction submission** | Evaluated safely | Intact | Returns existing receipt | `COMPLETED` | Idempotent; zero duplicate entries; returns success |
| **7. Retry after uncertain outcome** | Query via `txId` | Intact | Query via `txId` | Reconciled via query | If archive holds `txId`, proceed to Stage 8; otherwise restart from Stage 1 |
| **8. Attempt invalidation during preparation** | Completely untouched | Retains operations | Reservation cancelled | `ROLLED_BACK` (`ATTEMPT_FENCED`) | Stale continuation aborts immediately; zero mutations |
| **9. Operation in detached generation & in-flight slot** | Completely untouched | Deduplicated by ID | Untouched | Reconciled at Stage 2 | Stage 2 enforces settlement before set construction; operation exists in exactly 1 location |
| **10. Process termination before source retirement** | Heap destroyed | Heap destroyed | Heap destroyed | Process death | Node process restart reboots from clean baseline; memory limitation stated directly |

**Hard Invariant:** Clean-root installation is strictly forbidden until every operation in the retirement set has unambiguous terminal custody.

---

## 4. R13-B — Latched Settlement-Timeout Fencing & Race Analysis

### Problem Statement
R12 retained the in-flight slot after a watchdog timeout. However, if the underlying asynchronous Promise settled later, the original continuation microtask could wake up and attempt to resume the reset lifecycle.

### Three-Way Distinction
1. **Underlying Asynchronous Work Settlement:** The Promise / I/O operation settling (resolving/rejecting) on the platform event loop.
2. **Promise Continuation Execution:** The `.then()` / `.catch()` microtask running on the JavaScript event loop.
3. **Coordinator Lifecycle Permission:** The coordinator evaluating attempt validity and granting permission to mutate engine state.

### Latched Terminal Outcome
When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` fires:
- The attempt is marked `attempt.status = 'TIMED_OUT'` with `terminalReason = 'SETTLEMENT_WATCHDOG_EXPIRED'`.
- The timeout is a **latched terminal outcome**. The attempt can never be revived.
- In-flight slot remains owned ($N_{	ext{in-flight}} = 1$) to prevent new drains while an operation is pending.
- When the operation settles later, its continuation observes `attempt.status === 'TIMED_OUT'`, executes custody resolution (unshifts op to buffer head, sets `_inFlightRecord = null`), and **strictly terminates**. It CANNOT continue the reset lifecycle.

### Event Sequence Timelines for Race Scenarios (R13-B)

```
Scenario 1: Operation Settles Before Watchdog (Happy Path)
Coordinator              Watchdog Timer (5s)         Drainer / In-Flight Op
    |                           |                              |
    |-- resetRecovery() ------->| (start timer)                |
    |-- await barrier --------->|                              | (operation running)
    |                           |                              |-- op resolves ---->
    |                           |<-- cancel timer -------------|-- unshiftDrain() ->
    |                           |                              |-- slot = null ---->
    |<-- barrier resolves ------|                              |
    | (status = SETTLED)        |                              |
    |-- proceed to Step 7 ----->|                              |

Scenario 2: Watchdog Expires Before Operation Settles
Coordinator              Watchdog Timer (5s)         Drainer / In-Flight Op
    |                           |                              |
    |-- resetRecovery() ------->| (start timer)                |
    |-- await barrier --------->|                              | (slow operation...)
    |                           |-- timer fires! ------------->|
    |<-- timeout triggered -----|                              |
    | (status = TIMED_OUT)      |                              |
    |-- abort to DEGRADED ----->|                              | (slot remains owned!)
    |                           |                              |
    | (Later: op settles)       |                              |-- op resolves ---->
    |                           |                              |-- see TIMED_OUT -->
    |                           |                              |-- unshiftDrain() ->
    |                           |                              |-- slot = null ---->
    |                           |                              |-- HALT! (no revive)

Scenario 3: Operation Settles Immediately Around Watchdog Expiry
- Event Loop Concurrency: If Timer Task executes first, attempt status is latched to TIMED_OUT;
  subsequent microtask sees TIMED_OUT, restores custody, clears slot, and halts.
- If Settlement Microtask executes first, timer is cancelled, status becomes SETTLED;
  subsequent timer task sees cancelled/SETTLED and no-ops.

Scenario 4: Second Reset Requested While Old Operation Unsettled
Coordinator (Attempt 1)     Coordinator (Attempt 2)        In-Flight Operation
    |                                 |                             |
    | (status = WAITING_SETTLEMENT)   |                             | (op running...)
    |                                 |-- resetRecovery() --------->|
    | (Attempt 1 INVALIDATED)         | (Attempt 2 minted)          |
    |                                 |-- check slot occupied? ---->|
    |                                 | (N_in-flight == 1)          |
    |                                 |-- joins barrier wait ------>|
    |                                 |                             |-- op settles ---->
    |                                 |                             |-- fences Attempt 1
    |                                 |<-- barrier resolves --------|-- slot = null --->
    |                                 | (Attempt 2 proceeds)        |

Scenario 5: Old Operation Settles After New Reset Attempt Created
- Callback evaluates: myAttemptId (1) !== coordinator.currentAttemptId (2).
- Returns typed receipt: ATTEMPT_INVALIDATED (reason: STALE_ATTEMPT_FENCED).
- Restores operation to QuarantineBuffer head; releases slot (N_in-flight -> 0).
- Attempt 2 observes barrier resolution and empty slot, proceeding safely.

Scenario 6: Stale Continuation Resumes After Newer Attempt Begun
- Stale continuation wakes up on event loop tick.
- Asserts: this.attemptId === coordinator.currentAttemptId AND this.attempt.status === 'WAITING_FOR_SETTLEMENT'.
- Assertion fails immediately! Stale continuation returns with ZERO mutations and ZERO side-effects.
```

---

## 5. R13-C — Exact Archive Export Identity, Acknowledgement & Concurrency Governance

### Dedicated Capability Contract
Archive administration requires `TerminalArchiveAdministrationCapability` (`TERMINAL_ARCHIVE_BRAND`). Possession of `RecoveryInstallationCapability` does NOT grant archive export authority.

### Immutable Export Batch Structure
```typescript
export interface TerminalArchiveExportBatch {
  readonly exportId: string;                 // "arch-exp-${archiveGen}-${sequence}-${cryptoRandomHex(16)}"
  readonly entryIds: readonly string[];      // Exact array of TerminalQuarantineEntry.id strings
  readonly archiveGeneration: number;        // Archive generation counter at preparation
  readonly entryCount: number;               // Exact count of entries in batch
  readonly totalSerializedBytes: number;     // Exact sum of UTF-8 wire bytes for all entries
  readonly canonicalDigest: string;          // SHA-256 hex digest of RFC 8785 canonical JSON
  readonly entries: readonly TerminalQuarantineEntry[]; // Detached, deeply-frozen array
  readonly preparedAtMs: number;             // Timestamp of preparation
}
```
- **Unique Export ID:** Formatted as `arch-exp-${archiveGeneration}-${exportSequence}-${cryptoRandomHex(16)}`. Wall-clock timestamps are NEVER used as the sole uniqueness mechanism.

### Single-Lease Concurrency Governance (`MAX_OUTSTANDING_EXPORTS = 1`)
- To prevent overlapping batch conflicts and race conditions on clearance, `TerminalQuarantineArchive` enforces `MAX_OUTSTANDING_EXPORTS = 1`.
- At most **one** active export batch may exist at any time.
- A call to `prepareArchiveExport()` while an unacknowledged export is outstanding is rejected with `SyncEngineError('ARCHIVE_EXPORT_IN_PROGRESS')`.
- Callers must either acknowledge the export via `acknowledgeArchiveExport()` or explicitly cancel it via `cancelArchiveExport(exportId, capability)`.

### Acknowledgement Semantics
- `acknowledgeArchiveExport(exportId, capability)`:
  1. Validates capability exact reference identity.
  2. Matches `exportId` against current active export lease. If unknown or expired, throws `ARCHIVE_EXPORT_UNKNOWN` or `ARCHIVE_EXPORT_EXPIRED`; clears nothing!
  3. Clears **only** the exact `entryIds` contained in that export batch.
  4. Any entries added to the archive *after* preparation (which incremented `archiveGeneration`) are strictly preserved!
  5. Repeated acknowledgement with the same `exportId` is idempotent: returns `{ acknowledged: true, alreadyAcknowledged: true, clearedCount: 0 }`.
  6. If acknowledgement fails, times out, or throws: all entries remain safely retained.
- **Protocol Custody Preservation Boundary:** The protocol guarantees in-memory custody until explicit acknowledgement. It does not verify external storage hardware durability if the caller's external storage subsystem fails after acknowledgement.

---

## 6. R13-D — Enforceable Memory Bounds & Heap Limitations

### Explicit Hard Bound Constants
| Metric | Hard Bound Constant | Value | Description |
|---|---|---|---|
| **Maximum Archive Entries** | `MAX_TERMINAL_ARCHIVE_ENTRIES` | 500 entries | Hard ceiling on retained terminal entries |
| **Maximum Aggregate Serialized Bytes** | `MAX_TERMINAL_ARCHIVE_TOTAL_BYTES` | 2,097,152 bytes (2.0 MiB) | Enforceable canonical wire payload ceiling |
| **Maximum Serialized Entry Bytes** | `MAX_TERMINAL_ENTRY_BYTES` | 4,096 bytes (4.0 KiB) | Maximum serialized byte length per individual entry |
| **Maximum Error String Length** | `MAX_TERMINAL_ERROR_LENGTH` | 512 characters | Diagnostic error text length ceiling |
| **Maximum Reason String Length**| `MAX_TERMINAL_REASON_LENGTH` | 256 characters | Rejection reason description ceiling |
| **Maximum Peer Identifier Length**| `MAX_TERMINAL_PEER_ID_LENGTH` | 128 characters | Printable ASCII peer domain identifier limit |
| **Maximum Event Identifier Length**| `MAX_TERMINAL_LOGICAL_EVENT_ID_LENGTH`| 128 characters | Logical event identifier limit |
| **Maximum Outstanding Exports** | `MAX_OUTSTANDING_EXPORTS` | 1 export lease | Single active export lease ceiling |
| **Maximum Export Batch Wire Bytes** | `MAX_EXPORT_BATCH_TOTAL_BYTES` | 2,097,152 bytes (2.0 MiB) | Separate explicit memory budget for export snapshot |

### Enforceable Serialized-Data Calculation vs. V8 Heap Reality
1. **Enforceable Serialized Payload Bound:**
   $$\text{Max Serialized Wire Bytes} = \min(500 \times 4096, 2097152) = 2,048,000 \text{ bytes} \le 2.0 \text{ MiB}$$
2. **V8 JavaScript Object Heap Reality Disclaimer:**
   In garbage-collected runtimes like Node.js and V8, JavaScript object heap overhead cannot be deterministically enforced by application code. Heap usage varies with hidden classes, string internalization, 32-bit pointer compression, generational GC cycles, and heap fragmentation.
   - The protocol **strictly enforces hard bounds on serialized wire payloads and string lengths**.
   - The previously estimated $\approx 2.56\text{ MiB}$ heap footprint is an **empirical approximation**, NOT an enforceable theoretical guarantee. The protocol explicitly disclaims any theoretical guarantee of portable V8 heap memory ceilings.
3. **Canonical Serialization & UTF-8 Byte Counting:**
   - **Serialization Standard:** RFC 8785 JSON Canonicalization Scheme (JCS). Deterministic key sorting, standard number formatting, UTF-8 output.
   - **Byte Measurement:** Wire bytes are measured using `Buffer.byteLength(canonicalJson, 'utf8')` (Node.js) or `new TextEncoder().encode(canonicalJson).length` (browser/universal).
   - **Unicode & Multi-Byte Handling:** String fields must satisfy character length limits AND serialized entry must satisfy `byteLength <= 4096`. Multi-byte characters (2–4 bytes) cannot bypass the 4096 byte ceiling.
4. **Detached Representations & Peak Memory Budget:**
   - The archive retains detached, deeply frozen canonical records.
   - Detached export batches hold independent immutable snapshots.
   - With `MAX_OUTSTANDING_EXPORTS = 1` and `MAX_EXPORT_BATCH_TOTAL_BYTES = 2.0 MiB`, peak serialized payload is strictly bounded to:
     $$\text{Peak Wire Bytes} = 2.0 \text{ MiB (Archive)} + 2.0 \text{ MiB (Export Snapshot)} = 4.0 \text{ MiB}$$
5. **Fail-Closed Overflow Behavior:**
   - Any entry exceeding 4096 serialized bytes or string length bounds is rejected fail-closed (`TERMINAL_ENTRY_OVERSIZED`). Identity fields are **NEVER** truncated to make an entry fit.
   - Capacity overflow breaches trigger fail-closed transition to `DEGRADED` (`TERMINAL_ARCHIVE_OVERFLOW`). Silent eviction is strictly prohibited.

---

## 7. R13-E — Unified Reset-Policy Quarantine Fate Matrix & State Machine Transitions

### Authoritative Transition Path from DEGRADED
In accordance with accepted M4.3 `VALID_STATE_TRANSITIONS` (`recovery-state-machine.ts:49-54`), `DEGRADED` can only transition to `RECOVERING`.
- **`REINITIALIZE_CLEAN`:** `DEGRADED` $\to$ `RECOVERING` $\to$ `ACTIVE`. Clean root replacement occurs in `RECOVERING` under `replaceCoreStateAtomic()`; recovery mode is disengaged (`setRecoveryMode(false)`); coordinator transitions to `ACTIVE`; deferred notifications are flushed.
- **`PURGE_AND_RETRY`:** `DEGRADED` $\to$ `RECOVERING` $\to$ `RECONCILING`. Coordinator enters `RECOVERING` under `newAttemptId`, maintaining `setRecoveryMode(true)`. Un-drained buffer operations and operation log history are preserved. Draining resumes in `RECONCILING` once settlement barrier resolves and slot is empty.

### Unified Reset-Policy Quarantine Fate Matrix

| Reset Strategy | In-Flight Un-linearized Operation Fate | QuarantineBuffer Pending Operations Fate | Already-Linearized Log Operations Fate | When Can Draining Resume? |
|---|---|---|---|---|
| **`PURGE_AND_RETRY`** | Receives `ATTEMPT_INVALIDATED`; restored to head of `QuarantineBuffer` via `unshiftDrain(op)` | Retained in buffer custody; re-evaluated against new recovery attempt | Retained in `operationLog`; deduplicated during retry (`ALREADY_REPRESENTED` / `ALREADY_OWNED_DUPLICATE`) | Only after settlement barrier resolves, slot is empty ($N_{\text{in-flight}} = 0$), and new attempt enters `RECONCILING` |
| **`REINITIALIZE_CLEAN`** | Settles at Step 5 prior to commit; restored to buffer; included in Step 7 atomic retirement transaction | All pending operations atomically committed to `TerminalQuarantineArchive` via Stage 7 transaction (`REINITIALIZE_CLEAN_PURGED`) | Entire engine root replaced with clean baseline anchor via `replaceCoreStateAtomic`; prior log discarded | Pre-reset quarantine is permanently retired; live operation ingestion begins fresh in `ACTIVE` state |

### Notification Contract & Zero-Notification Clarification
In existing accepted source (`leaderless-sync-engine.ts:883-960`), neither `replaceCoreStateAtomic` nor `setRecoveryMode(false)` pushes to `deferredStateChanges`. Therefore, Step 10 may deliver **zero notifications** unless an explicit clean baseline state change notification is specifically enqueued by caller or implementation.
- **Observer Boundary Invariant:** No notification is EVER delivered to external listeners before Step 9 reaches `ACTIVE` and recovery mode is `false`.

---

## 8. Cross-Document Consistency Audit Summary

| Document | Sections Modified | Purpose |
|---|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Status line, 2.1.2, 3.4 (1–11 sequence, R13-A transaction, R13-B latched timeout, R13-C single-lease export, R13-D memory limits, R13-E state machine paths) | Formalized 10-stage idempotent retirement transaction, latched timeout state machine, 6 race analysis event timelines, single-lease export governance, enforceable serialized bounds vs V8 heap reality, and legal state machine transitions. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Status line, Section 3.1 (`IRuntimeRecoveryCoordinator`), Section 3.3 (`ITerminalQuarantineArchive` & contracts) | Added `ITerminalQuarantineArchive`, `QuarantineRetirementTransaction`, `TerminalArchiveExportBatch`, `TerminalArchiveExportReceipt`, `ResetAttemptRecord`, `ResetAttemptState`, and strict numeric constants. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Status line, M5.0, M5.1 | Added implementation tasks and verification deliverables for R13-A, R13-B, R13-C, R13-D, R13-E. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Status line, AC-M5-01, AC-M5-27 | Updated criteria to enforce 10-stage retirement transaction, latched timeout state machine, single-lease export governance, and enforceable serialized bounds. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Status line, R-M5-04, R-M5-06, R-M5-08 | Added mitigations and controls for transactional retirement, latched timeout fencing, single-lease export, and enforceable serialized limits. |

---

## 9. Git Verification Evidence & Scope Declaration

- **Zero Production Code Modified:** Confirmed via `git diff --stat 59df88fdf7d649f2cce59ccab2f69332beb91f15 HEAD`.
- **Zero Test Files Modified:** Confirmed.
- **Zero Manifest / Configuration Modified:** Confirmed.
- **Strictly 6 Documentation Files Changed:** The 5 M5 specifications and this report.

---

## 10. Conclusion & Final Decision Boundary

All findings from the M5.0-R12 review have been resolved with mathematical precision, grounded directly in the accepted TypeScript engine implementation, `RecoveryStateMachine` transition graph, and Node.js/JavaScript event-loop concurrency semantics.

- Milestone M5.0-R13 is **COMPLETE — READY FOR FINAL M5.0 ACCEPTANCE REVIEW**.
- Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED**.
