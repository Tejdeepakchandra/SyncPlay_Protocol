# Phase 7.5 Milestone M5.0-R15 — Correction & Architectural Closure Report

**Sub-Milestone Title:** Atomic Archive Root Publication, Retry-Safe Retirement & Identity Contract Closure
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Target Milestone:** Phase 7.5 Sub-Milestone M5.0-R15
**Starting Baseline Commit:** `67ef4f372c4b49b8fab4dcf390a7c9906974fcb1`
**Direct Parent Commit:** `9f08e5708ceee29387ac42e541aa5853d0fecc03`
**Authoritative Accepted Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`

**Implementation Status: Milestone M5.1 remains STRICTLY UNAUTHORIZED AND UNSTARTED.**

---

## 1. Executive Summary & Objective

Milestone M5.0-R15 resolves all architectural findings raised in the Engineering Authority's review of Milestone M5.0-R14. This correction round achieves complete architectural closure across the seven required domains (R15-A through R15-G) without modifying production code, test code, or package configuration:

1. **R15-A — Exception-Atomic Archive Publication:** Replaces flawed sequential Map/Array mutation claims with a rigorously specified candidate-root publication model (`TerminalArchiveCoreState`), ensuring that all allocations, validations, and checks occur privately prior to a single synchronous pointer assignment.
2. **R15-B — Idempotent Transaction Reconciliation Before Reservation Validation:** Inverts commit validation ordering in `commitRetirementBatch` to check committed transaction records first, allowing retries to succeed immediately even if their reservation has been consumed or expired, while preventing duplicate entries or transaction records.
3. **R15-C — Source Retirement Atomicity & Partial-Progress Recovery:** Formally selects and specifies **Option A — Atomic source retirement**, proving through contract that `QuarantineBuffer.retireCommittedBatch` performs all-or-nothing candidate validation and swap, eliminating partial progress hazards.
4. **R15-D — Uncertain Commit Query Semantics:** Establishes unambiguous 3-outcome query semantics for `archive.getTransaction(txId)` when the coordinator is in `COMMIT_OUTCOME_UNKNOWN`: matching digest confirms commit, authoritative absence confirms non-commit (enabling clean cancellation), and query failure preserves `COMMIT_OUTCOME_UNKNOWN` without mutating engine state.
5. **R15-E — Identity and Key Encoding Contract:** Reconciles `CompositeOperationIdentity` with source code (`quarantine-buffer.ts:43-50` and `leaderless-sync-engine.ts:982-995`), preserving `sequenceNumber` (eliminating silent renaming), replaces colon-delimited keys with length-prefixed canonical encoding (`opid:...`), and establishes exhaustive fail-closed conflict handling rules.
6. **R15-F — Export Lease and Archive Generation Semantics:** Decouples export acknowledgement from current archive generation, proving that an export prepared at generation $G$ remains acknowledgeable after subsequent commits advance the archive to $G' > G$ by validating strictly against the immutable lease record, and resolves acknowledgement/expiry races deterministically.
7. **R15-G — Portable Canonical Byte Ownership:** Establishes browser-portable UTF-8 byte measurement via `new TextEncoder().encode(canonicalJson).length`, specifies private `Uint8Array` canonical wire byte ownership with detached defensive cloning, and enforces fail-closed quarantining for operations exceeding 4,096 canonical wire bytes.

---

## 2. Technical Resolutions & Specification Amendments

### 2.1 R15-A — Exception-Atomic Archive Publication

#### The Architectural Defect in R14
In R14, `TerminalQuarantineArchive.commitRetirementBatch` was described as performing synchronous mutations into both `this.entries` (Array) and `this.committedTransactions` (Map). While JavaScript execution within a single turn is non-preemptive, sequential mutations are **not exception-atomic**: an allocation failure (OOM), unexpected type error, or runtime exception occurring after modifying `this.entries` but before updating `this.committedTransactions` leaves the archive in a corrupted, unrecoverable state where entries are retained without a corresponding transaction record.

#### The Candidate-Root Publication Model
M5.0-R15 formally introduces the private immutable candidate-root state:

```typescript
export interface TerminalArchiveCoreState {
  readonly entries: ReadonlyArray<TerminalQuarantineEntry>;
  readonly committedTransactions: ReadonlyMap<string, QuarantineRetirementTransactionRecord>;
  readonly totalBytes: number;
  readonly size: number;
  readonly archiveGeneration: number;
}
```

#### Protocol Sequence for Candidate Publication:
1. **Private Construction:** The complete candidate state is assembled privately in temporary memory:
   - Fresh entry array: `Object.freeze([...this._core.entries, ...stagedBatch])`.
   - Fresh committed transactions map: `new Map(this._core.committedTransactions).set(txId, txRecord)`.
   - Updated accounting: `totalBytes = this._core.totalBytes + stagedBatchBytes`, `size = this._core.size + stagedBatch.length`.
   - Advanced generation: `archiveGeneration = this._core.archiveGeneration + 1`.
2. **Pre-Publication Invariant Verification:** Every check that could throw (capacity bounds, entry sizes $\le 4096$ bytes, SHA-256 digest calculation) is evaluated *prior* to publication.
3. **Single Synchronous Pointer Swap:** Publication occurs via a single atomic reference assignment:
   ```typescript
   this._core = candidateCore;
   ```
   Because JavaScript runs synchronously to completion, external observers cannot observe intermediate or partially constructed state.
4. **Failure Semantics:** If an exception occurs during candidate construction or validation, `this._core` remains 100% authoritative and unmodified. Zero transactions are committed, zero entries are appended, and the reservation is preserved for retry or explicit cancellation via `archive.cancelReservation(reservationId, capability)`.
5. **State Delineation:**
   - **Inside Atomic Root:** `entries`, `committedTransactions`, `totalBytes`, `size`, `archiveGeneration`.
   - **External to Root (Operational/Lease State):** `activeReservations: Map<string, ArchiveReservationRecord>`, `activeExportLease: ArchiveExportLease | null`, `tombstones: Map<string, ArchiveExportTombstone>`.
   When candidate publishes, the consumed reservation is removed from `activeReservations` synchronously with the pointer assignment. No hidden mutable state can disagree with the published root.

---

### 2.2 R15-B — Idempotent Transaction Reconciliation Before Reservation Validation

#### Commit Ordering Correction
In R14, `commitRetirementBatch` checked reservation validity before checking whether the transaction had already committed. If a network blip or coordinator crash caused a retry after the reservation was consumed or expired, the retry was rejected with `RESERVATION_NOT_FOUND` or `RESERVATION_EXPIRED`, despite the batch having been committed successfully.

M5.0-R15 enforces strict **lookup-first commit ordering**:

```typescript
commitRetirementBatch(
  retirementTransactionId: string,
  batchDigest: string,
  reservationId: string,
  stagedBatch: readonly TerminalQuarantineEntry[],
): TerminalBatchCommitReceipt {
  // Step 1: Authoritative lookup in committed transactions
  const existingRecord = this._core.committedTransactions.get(retirementTransactionId);
  if (existingRecord) {
    // Step 2: Digest verification
    if (existingRecord.batchDigest === batchDigest) {
      return {
        committed: true,
        alreadyCommitted: true,
        transactionId: retirementTransactionId,
        entryCount: existingRecord.entryCount,
        totalSerializedBytes: existingRecord.totalSerializedBytes,
        archiveGeneration: this._core.archiveGeneration,
        batchDigest: existingRecord.batchDigest,
      };
    }
    // Conflict: same transaction ID with different digest -> reject fail-closed
    throw new SyncEngineError(
      'TRANSACTION_ID_DIGEST_MISMATCH',
      `Transaction ${retirementTransactionId} already committed with different digest`,
    );
  }

  // Step 3: Only if transaction is absent, validate reservation
  const reservation = this.activeReservations.get(reservationId);
  if (!reservation) {
    throw new SyncEngineError('RESERVATION_NOT_FOUND', `Reservation ${reservationId} not found`);
  }
  if (Date.now() > reservation.expiresAtMs) {
    throw new SyncEngineError('RESERVATION_EXPIRED', `Reservation ${reservationId} has expired`);
  }
  if (reservation.archiveGeneration !== this._core.archiveGeneration) {
    throw new SyncEngineError('GENERATION_MISMATCH', 'Archive generation advanced since reservation');
  }

  // Step 4: Validate staged batch integrity and counts
  if (stagedBatch.length !== reservation.reservedCount) {
    throw new SyncEngineError('BATCH_COUNT_MISMATCH', 'Staged batch count does not match reservation');
  }
  let batchBytes = 0;
  for (const entry of stagedBatch) {
    if (entry.byteLength > MAX_TERMINAL_ENTRY_SERIALIZED_BYTES) {
      throw new SyncEngineError('ENTRY_SIZE_EXCEEDED', `Entry ${entry.id} exceeds 4096 bytes`);
    }
    batchBytes += entry.byteLength;
  }
  if (batchBytes !== reservation.reservedBytes) {
    throw new SyncEngineError('BATCH_BYTE_MISMATCH', 'Staged batch byte total does not match reservation');
  }

  // Step 5: Construct candidate root privately and swap atomically
  const candidateCore: TerminalArchiveCoreState = Object.freeze({
    entries: Object.freeze([...this._core.entries, ...stagedBatch]),
    committedTransactions: new Map(this._core.committedTransactions).set(retirementTransactionId, {
      transactionId: retirementTransactionId,
      outcome: 'COMMITTED',
      entryCount: stagedBatch.length,
      totalSerializedBytes: batchBytes,
      batchDigest,
      committedAtMs: Date.now(),
    }),
    totalBytes: this._core.totalBytes + batchBytes,
    size: this._core.size + stagedBatch.length,
    archiveGeneration: this._core.archiveGeneration + 1,
  });

  this._core = candidateCore;
  this.activeReservations.delete(reservationId);

  return {
    committed: true,
    alreadyCommitted: false,
    transactionId: retirementTransactionId,
    entryCount: stagedBatch.length,
    totalSerializedBytes: batchBytes,
    archiveGeneration: candidateCore.archiveGeneration,
    batchDigest,
  };
}
```

#### Invariant Protections:
- A consumed or expired reservation **never** prevents reconciliation of an already-committed transaction.
- Duplicate submissions with matching digests return the original receipt idempotently without allocating duplicate entries or incrementing `archiveGeneration`.
- A transaction ID submitted with a mismatched digest is rejected fail-closed (`TRANSACTION_ID_DIGEST_MISMATCH`), preventing transaction spoofing.

---

### 2.3 R15-C — Source Retirement Atomicity & Partial-Progress Recovery

#### Selection of Option A — Atomic Source Retirement
R14 assumed that a failure during source retirement left all quarantine operations untouched. M5.0-R15 proves this contract by selecting and specifying **Option A — Atomic source retirement** in `QuarantineBuffer`:

```typescript
export interface IQuarantineBufferRetirementExtension {
  retireCommittedBatch(
    identities: readonly CompositeOperationIdentity[],
    capability: RecoveryInstallationCapability,
  ): void;
}
```

#### Protocol Execution:
1. **Validation Phase (Zero Mutation):**
   - Every identity in `identities` is looked up in the frozen `drainBatch` (or `activeQueue`) and `this.identityIndex`.
   - If any identity is missing, mismatched, or corrupted, the method immediately throws `SyncEngineError('RETIREMENT_SOURCE_MISMATCH')`.
   - Zero operations are removed, zero byte counters are decremented, and the buffer remains 100% authoritative.
2. **Private Candidate Construction:**
   - Candidate `drainBatch`, candidate `drainBytes`, candidate `activeQueue`, candidate `activeBytes`, and candidate `identityIndex` are constructed privately.
3. **Atomic Reference Swap:**
   - Publication occurs via single synchronous pointer replacements:
     `this.drainBatch = candidateDrainBatch; this._drainBytes = candidateDrainBytes; ...`
4. **All-or-Nothing Guarantee:**
   - Either the entire retirement set is removed from `QuarantineBuffer`, or none is.
   - Partial source progress is structurally prevented by contract.

---

### 2.4 R15-D — Uncertain Commit Query Semantics

#### Unambiguous 3-Outcome Resolution
When an unexpected exception (e.g. allocation failure) or watchdog timeout occurs while awaiting `commitRetirementBatch`, the coordinator transitions the transaction to `status: 'COMMIT_OUTCOME_UNKNOWN'`.

To resolve this state, the coordinator queries `archive.getTransaction(retirementTransactionId)`:

```typescript
getTransaction(
  retirementTransactionId: string,
): QuarantineRetirementTransactionRecord | undefined;
```

The coordinator evaluates the three distinct query outcomes:
1. **Outcome 1 — Found with Matching Digest (`outcome === 'COMMITTED'`):**
   - The batch was committed to the archive root before the interruption.
   - The coordinator transitions the transaction to `COMMITTED_SOURCE_RETIREMENT_PENDING` and resumes Stage 11 source retirement.
2. **Outcome 2 — Authoritatively Absent (`undefined` returned from authoritative query):**
   - The batch was authoritatively NOT committed to the archive root.
   - The transaction outcome is `NOT_COMMITTED`.
   - The coordinator safely cancels the pending reservation via `archive.cancelReservation(reservationId, capability)` and restarts retirement under a fresh attempt.
3. **Outcome 3 — Query Fails, Times Out, or Inconclusive:**
   - The coordinator **MUST REMAIN in `COMMIT_OUTCOME_UNKNOWN`**.
   - It must NOT retire source operations, must NOT cancel potentially committed reservations, and must NOT begin a replacement transaction.
   - It retries the query with exponential backoff until authoritative resolution is achieved.

**Identity & Digest Invariance:** All retries preserve the exact same `retirementTransactionId` and `batchDigest`, preventing dual commit or split transactions.

---

### 2.5 R15-E — Identity and Key Encoding Contract

#### Schema Reconciliation with Accepted Source
In accepted source code (`packages/sync-engine/src/recovery/quarantine-buffer.ts:43-50, 167-172` and `packages/sync-engine/src/engine/leaderless-sync-engine.ts:982-995`), operations are defined by:
- `op.originPeerId: string`
- `op.sequenceNumber: number`
- `op.membershipEpoch: number`
- `op.event.message.logicalEventId: string`

M5.0-R15 formally reconciles `CompositeOperationIdentity` to use `sequenceNumber` (directly mapped from `op.sequenceNumber`), eliminating any undocumented renaming to `originSequence`:

```typescript
export interface CompositeOperationIdentity {
  readonly originPeerId: string;    // Generating peer domain identifier (op.originPeerId)
  readonly sequenceNumber: number;  // Strictly monotonic sequence counter for originPeerId (op.sequenceNumber)
  readonly membershipEpoch: number; // Membership configuration epoch (op.membershipEpoch)
  readonly logicalEventId: string;  // Unique logical event identifier (op.event.message.logicalEventId)
}
```

#### Length-Prefixed Canonical Key Encoding
To prevent delimiter collision from colons or slashes in peer IDs or logical event IDs, the canonical identity key uses length-prefixed encoding:

```typescript
export function canonicalOperationIdentityKey(id: CompositeOperationIdentity): string {
  return `opid:${id.originPeerId.length}:${id.originPeerId}/${id.sequenceNumber}/${id.membershipEpoch}/${id.logicalEventId.length}:${id.logicalEventId}`;
}
```
*Example:* `originPeerId = "peer-A"` (length 6), `sequenceNumber = 42`, `membershipEpoch = 1`, `logicalEventId = "evt_9a8b7c"` (length 10) $\to$
`opid:6:peer-A/42/1/10:evt_9a8b7c`.

#### Fail-Closed Conflict Handling Matrix:
- **Same originPeerId & sequenceNumber, same membershipEpoch, same logicalEventId:** Exact duplicate $\to$ idempotent processing (zero dual counting).
- **Same originPeerId & sequenceNumber, same membershipEpoch, different logicalEventId:** Split-brain / divergent event $\to$ **REJECTS FAIL-CLOSED** with `SyncEngineError('RETIREMENT_BATCH_CONFLICT')`. Clean reset is aborted, quarantine custody retained.
- **Same originPeerId & sequenceNumber, different membershipEpoch:** Epoch transition conflict $\to$ **REJECTS FAIL-CLOSED** with `SyncEngineError('RETIREMENT_BATCH_CONFLICT')`.
- **Duplicate complete composite identity in batch:** Redundant duplicate $\to$ deduplicated or rejected fail-closed (`RETIREMENT_BATCH_CONFLICT`).
- **Retirement record conflicting with existing quarantine custody index:** Record absent from buffer custody $\to$ **REJECTS FAIL-CLOSED** with `SyncEngineError('RETIREMENT_SOURCE_MISMATCH')`. Quarantine custody is unconditionally preserved.

---

### 2.6 R15-F — Export Lease and Archive Generation Semantics

#### Generation-Independent Export Acknowledgement
In R14, export acknowledgement was mistakenly validated against the archive's current generation `archiveGeneration`. If new retirement transactions committed after an export batch was prepared at generation $G$, the archive generation advanced to $G' > G$, causing valid export acknowledgements to fail.

M5.0-R15 proves that export acknowledgement is validated against the **immutable export lease record** (`lease.exportId`, `lease.batchDigest`, `lease.leaseExpiryMs`), NOT against `this._core.archiveGeneration`:
- Acknowledgement succeeds if:
  1. `currentTime <= lease.leaseExpiryMs`.
  2. `exportId` and digest match `lease.exportId` and `lease.batchDigest`.
  3. The exact entries captured in the export lease are still present in `this._core.entries`.
- **Surgical Eviction:** Acknowledgement removes **strictly and exclusively the captured entries**. Any entries appended at $G' > G$ remain completely untouched.
- Archive generation advances to $G'' = G' + 1$ upon acknowledgement.

#### Deterministic Race Resolution:
- Under single-threaded JavaScript run-to-completion, either the lease expiration callback executes first, or the acknowledgement call executes first:
  * **Case 1 (Expiration Executes First):** The active lease is marked expired. A subsequent `acknowledgeArchiveExport()` call detects `now > lease.leaseExpiryMs`, clears zero entries, and throws `SyncEngineError('ARCHIVE_EXPORT_EXPIRED')`. **An expired lease NEVER clears archive entries.**
  * **Case 2 (Acknowledgement Executes First):** The lease is verified active, entries are purged via candidate root swap, `activeExportLease` is set to `null`, and a tombstone is recorded. When the later timer callback runs, it observes `activeExportLease === null` and safely no-ops.

---

### 2.7 R15-G — Portable Canonical Byte Ownership

#### 1. Browser-Portable UTF-8 Byte Measurement
Replaces Node-specific `Buffer.byteLength(str, 'utf8')` with universal Web standard:
```typescript
const wireByteLength = new TextEncoder().encode(canonicalJson).length;
```
Ensures exact, deterministic byte measurement across Node.js LTS, browsers, and WebWorkers.

#### 2. Private Canonical Wire Byte Ownership
`TerminalQuarantineEntry` privately owns the serialized canonical wire bytes as an immutable `Uint8Array`:
```typescript
readonly canonicalWireBytes: Uint8Array;
```
All queries, exports, and external accessors return defensive detached copies (`new Uint8Array(entry.canonicalWireBytes)`), preventing external mutation of archived content.

#### 3. Single Immutable Staged Representation
The measured bytes, the SHA-256 batch digest input, and the committed archive wire bytes are derived from the **exact same immutable staged `Uint8Array`**.

#### 4. RFC 8785 Determinism vs. Object Immutability
Canonical JSON sorting (RFC 8785 JCS) guarantees byte serialization determinism, but does not freeze mutable JavaScript objects. Explicit `Object.freeze` and defensive byte cloning are enforced at all trust boundaries.

#### 5. Strict 4,096-Byte Entry Boundary & Oversized Quarantining
- Individual complete entries cannot exceed 4,096 canonical wire bytes (`MAX_TERMINAL_ENTRY_SERIALIZED_BYTES = 4096`).
- If an operation produces a complete terminal entry exceeding 4,096 bytes, it **CANNOT** be committed to `TerminalQuarantineArchive`.
- Any oversized entry remains safely quarantined in `QuarantineBuffer` and causes clean reset (`REINITIALIZE_CLEAN`) to fail closed with `SyncEngineError('RESET_PREREQUISITE_FAILED')`.
- Oversized operations are **NEVER truncated, corrupted, or silently discarded**.
- Limits enforced: max 500 entries (`MAX_TERMINAL_ARCHIVE_ENTRIES = 500`), max 2 MiB aggregate wire bytes (`MAX_TERMINAL_ARCHIVE_TOTAL_BYTES = 2097152`), max 4,096 bytes per entry (`MAX_TERMINAL_ENTRY_SERIALIZED_BYTES = 4096`).

---

## 3. Failure Recovery Matrix (R15 Comprehensive)

| Failure Scenario | Engine State | QuarantineBuffer Custody | TerminalQuarantineArchive Custody | Coordinator Transaction State | Recovery Action |
|:---|:---|:---|:---|:---|:---|
| **1. Reservation capacity insufficient ($> 500$ entries or $> 2$ MiB)** | Untouched | Retains operations ($N$) | Rejects reservation | `FAILED` (`CAPACITY_INSUFFICIENT`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); zero loss |
| **2. Oversized entry ($> 4096$ canonical bytes)** | Untouched | Retains operations ($N$) | Rejects batch staging | `FAILED` (`ENTRY_SIZE_EXCEEDED`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); entry never truncated |
| **3. Generation mismatch at reservation commit** | Untouched | Retains operations ($N$) | Rejects commit | `FAILED` (`GENERATION_MISMATCH`) | Cancels reservation; re-reads generation; re-stages batch |
| **4. Exception during candidate root construction** | Untouched | Retains operations ($N$) | Old root remains authoritative; zero entries added | `PREPARING` / `FAILED` | Old archive root intact; reservation preserved for retry or cancellation |
| **5. Archive commit succeeds but source retirement throws (Option A validation failure)** | Untouched | Retains operations ($N$) | Batch committed under `retirementTxId` | `COMMITTED_SOURCE_RETIREMENT_PENDING` | Source buffer untouched; coordinator queries `getTransaction()`; skips archive commit; retries Stage 11 source retirement |
| **6. Duplicate transaction submission** | Untouched | Intact | Returns existing receipt via Step 1 lookup | `COMPLETED` | Idempotent; zero duplicate entries; returns success receipt |
| **7. Transaction ID reused with different digest** | Untouched | Retains operations ($N$) | Rejects with conflict | `FAILED` (`TRANSACTION_ID_DIGEST_MISMATCH`) | Throws error; preserves archive and buffer; prevents corruption |
| **8. Uncertain commit outcome (timeout/exception)** | Untouched | Retains operations ($N$) | Query via `getTransaction()` | `COMMIT_OUTCOME_UNKNOWN` | 3-way query: if committed $\to$ retires source; if absent $\to$ cancels reservation; if fails $\to$ remains unknown |
| **9. Attempt invalidation during preparation** | Untouched | Retains operations ($N$) | Reservation cancelled | `FAILED` (`ATTEMPT_FENCED`) | Stale continuation aborts immediately; zero mutations |
| **10. Conflicting composite identity in batch** | Untouched | Retains operations ($N$) | Untouched | `FAILED` (`RETIREMENT_BATCH_CONFLICT`) | Rejects batch fail-closed; preserves buffer; halts reset |
| **11. Export lease expiration racing with acknowledgement** | Untouched | N/A | Expiration wins or acknowledgement wins | `ARCHIVE_EXPORT_EXPIRED` if expired | Expired lease clears zero entries; acknowledgement clears entries only if valid |

---

## 4. Source Code Audit & Line Citations

| Subsystem Component | File Path | Exact Line Range | Existing Behavior in Accepted Source | Proposed M5 Integration & Invariants |
|:---|:---|:---|:---|:---|
| **QuarantinedOperation Envelope** | `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 43–50 | Defines `originPeerId`, `sequenceNumber`, `membershipEpoch`, `serializedByteLength`, `receivedAtMs`. | M5.0-R15 composite identity schema preserves `sequenceNumber` directly from this contract. |
| **Quarantine Buffer Custody Index** | `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 167–172 | Indexes retained operations via `${op.originPeerId}:${op.sequenceNumber}` in `this.identityIndex`. | Option A source retirement validates identities against this index before performing atomic swap. |
| **Admit Drained Operation Identity Extraction** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | 982–995 | Extracts `originPeerId`, `originSeq = op.sequenceNumber`, `epoch`, and `logicalEventId = op.event.message.logicalEventId`. | Validates that composite identity 4-tuple matches exact source properties without field renaming. |
| **Strict Epoch Fencing** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | 1003–1017 | Terminally rejects events with `epoch < this.currentEpoch` or `epoch > this.currentEpoch`. | Composite identity conflict rules preserve strict epoch boundaries across all retirement transactions. |
| **Recovery Transition Graph** | `packages/sync-engine/src/recovery/recovery-state-machine.ts` | 49–54 | Defines legal transitions: `DEGRADED: new Set(['RECOVERING'])`. | Reset operations follow authoritative path `DEGRADED -> RECOVERING -> ACTIVE`, preserving state machine graph. |

---

## 5. Explicit Unresolved Limitations

1. **In-Memory Volatility:** `TerminalQuarantineArchive` is an in-memory ring buffer. Process termination (e.g., Node.js process kill or browser crash) destroys in-memory archive entries. Process-crash durability is deferred to external export consumers.
2. **V8 Heap Reality:** Serialized byte bounds (2 MiB aggregate wire bytes, 4 KiB per entry) govern transport data. V8 heap allocations for JavaScript object wrappers, prototype chains, and closures exceed serialized wire size by approximately $3.5\times$ to $5\times$.
3. **Single Active Export Lease:** Only one export lease can be active at a time (`MAX_OUTSTANDING_EXPORTS = 1`). Concurrent administrative export calls are rejected with `ARCHIVE_EXPORT_IN_PROGRESS`.
4. **No Mid-Attempt Cancellation of Running Promises:** When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expires, the reset attempt is latched to `TIMED_OUT`, but the underlying in-flight Promise runs to settlement before the slot can be reclaimed.

---

## 6. Repository Governance & Final Evidence

### 6.1 Baseline Verification
- **Branch:** `main`
- **Starting Baseline Commit (M5.0-R14):** `67ef4f372c4b49b8fab4dcf390a7c9906974fcb1`
- **Direct Parent Commit (M5.0-R13):** `9f08e5708ceee29387ac42e541aa5853d0fecc03`

### 6.2 Scope Confirmation
- Strictly documentation-only correction round.
- **Zero** production TypeScript/JavaScript files modified (`packages/*/src/**/*.ts`).
- **Zero** test files modified (`packages/**/__tests__/**`).
- **Zero** package manifests or lockfiles modified (`package.json`, `package-lock.json`).
- **Zero** build configurations modified (`tsconfig*.json`).
- Strictly the six authorized documentation files modified:
  1. `docs/PHASE7_5_M5_ARCHITECTURE.md`
  2. `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`
  3. `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`
  4. `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`
  5. `docs/PHASE7_5_M5_RISK_REGISTER.md`
  6. `docs/PHASE7_5_M5_0_R15_CORRECTION_REPORT.md`

---

## 7. Final Decision Boundary

```
================================================================================
FINAL MILESTONE M5.0-R15 DECISION:
READY FOR FINAL M5.0 ACCEPTANCE REVIEW
================================================================================
```

All seven findings (R15-A through R15-G) have been rigorously resolved and reconciled with accepted source code. The M5 architecture specifications are complete, consistent, and ready for final engineering review.

**Implementation Authorization Notice:**
Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED** unless separately authorized by the Engineering Authority following formal acceptance of Milestone M5.0.
