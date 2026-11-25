# Phase 7.5 M5.0-R18-R10 — Authoritative Settlement Evidence and Outcome-Specific Custody Reconciliation

**Role:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Mode:** STRICTLY DOCUMENTATION-ONLY ARCHITECTURAL CORRECTION
**Target Document:** `docs/PHASE7_5_M5_0_R18_CONCURRENT_RECOVERY_ARBITRATION_DECISION.md`
**Baseline Commit:** `4363f99efc28bc311db662dc58004f3f96f39ec7`
**Direct Parent Commit:** `4363f99efc28bc311db662dc58004f3f96f39ec7`
**Implementation Authorization:** Milestone M5.1 remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R10 Objectives

The M5.0-R18-R9 architectural decision formalized the five-field owner-token equivalence check and introduced generation-scoped settlement barriers. However, rigorous distributed-systems auditing identified five critical proof and authorization vulnerabilities:

1. **Claims Confounded with Authoritative Evidence:** R18-R9 defined `SettlementReconciliationProof` with caller-supplied public Boolean fields (`executionSettled: true`, `custodyReconciled: true`, `sourceBufferDisjoint: true`). In a TypeScript runtime, any caller capable of constructing the object can forge these claims. A public interface with Boolean properties is an assertion, not evidence. Evidence must be substantiated by unforgeable authoritative receipts produced directly by the responsible subsystem components.
2. **Missing Receipt-to-Identity Binding:** R18-R9 lacked a mandatory rule binding every intermediate receipt to the complete five-field custody identity (`recoveryAttemptId`, `custodyGeneration`, `operationIdentity`, `transactionId`, `drainerOwnershipGeneration`). A receipt from an earlier attempt or different transaction must not be reused.
3. **Undifferentiated Outcome Reconciliation:** R18-R9 used generic fields (`custodyReconciled`, `sourceBufferDisjoint`) that conflate the opposing custody consequences of commit versus abort. In a commit, the operation must be absent from source quarantine (`sourceBufferDisjoint === true`); in an abort/fence, the operation must be restored to source quarantine (`sourceBufferContainsOperation === true`).
4. **Omission of Drainer Release Evidence:** The barrier proof validator did not require explicit proof that the drainer had published its release. A barrier must never resolve merely because an in-flight slot appears empty.
5. **Undefined Proof Linearization Point & Retirement Ordering:** The sequence among receipt validation, exact-owner release, proof assembly, barrier resolution, and barrier retirement lacked a strict, failure-safe execution order.

### Core Architectural Guarantees Established in R18-R10:
- **Authoritative-Evidence Model:** Replaces Boolean claims with authoritative, component-issued receipts (`ExecutionSettlementReceipt`, `ArchiveDispositionReceipt`, `CustodyReconciliationReceipt`, `DrainerCustodyReleaseReceipt`).
- **Complete Five-Field Receipt Identity Binding:** Enforces `isMatchingReceiptIdentity` across all five fields on every single receipt. Earlier-attempt or mismatched receipts are rejected fail-closed.
- **Discriminated Outcome-Specific Custody Reconciliation:** Separates `SourceQuarantineRetirementReceipt` (for `COMMITTED`) from `SourceQuarantineRestorationReceipt` (for `ABORTED_FENCED`), proving exact custody location for both terminal outcomes.
- **Explicit Drainer Release Receipt:** Requires `DrainerCustodyReleaseReceipt` produced only after five-field owner validation and atomic custody publication.
- **Final Authoritative Proof Predicate:** Defines `assertAuthoritativeSettlementEvidence`, establishing the exact linearization point at which a barrier may resolve.
- **Preserved Watchdog & Late-Continuation Semantics:** Guarantees that a 5-second watchdog timeout never settles Promises, releases custody, or resolves barriers.
- **Twenty-Four Deterministic Future Test Requirements:** Codifies 24 exhaustive test requirements for future M5.1 suites.

---

## 2. Separation of the Three Independent Conditions

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                        THREE INDEPENDENT LIFECYCLE CONDITIONS                           │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ CONDITION A: DESTINATION TRANSACTION OUTCOME (Archive State)                            │
│ Authoritative state in TerminalArchiveCoreState:                                        │
│ • COMMITTED: Irrevocably recorded in #committedTransactions via atomic swap.           │
│ • ABORTED_FENCED: Irrevocably recorded in #fencedTransactions; late commits rejected.   │
│ • COMMIT_PENDING: Reservation active; commit operation is executing. Non-terminal.     │
│ • NOT_FOUND_AT_QUERY: Absent at query linearization point. Non-terminal point-in-time!  │
│ • INCONCLUSIVE: Archive query threw, timed out, or memory corrupted. Preserves custody. │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ CONDITION B: UNDERLYING ASYNCHRONOUS EXECUTION SETTLEMENT (Event Loop State)           │
│ Physical settlement of the original destination Promise:                                │
│ • FULFILLED: Promise settled with success receipt.                                      │
│ • REJECTED: Promise settled with an Error (post-commit execution failure or transport). │
│ Invariant: Watchdogs, receipts, timeouts, and lookups NEVER prove Promise settlement!   │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ CONDITION C: CUSTODY RECONCILIATION (Storage Representation)                            │
│ Verified representation in authoritative ownership boundary:                            │
│ • PROTOCOL A: Operation committed in archive AND retired from source buffer.            │
│ • PROTOCOL B: Operation terminally fenced in archive AND restored to buffer head.       │
│ • RECONCILIATION BLOCKED: Unresolved outcome retains drainer custody fail-closed.       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### The Generation-Scoped Universal Settlement-Barrier Invariant:
$$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{NO\_IN\_FLIGHT\_OWNER}(g)$$

$$\mathbf{BARRIER\_PENDING}(g) \land \neg\mathbf{CUSTODY\_RECONCILED}(g) \implies \mathbf{CUSTODY\_REMAINS\_OWNED\_OR\_EXPLICITLY\_QUARANTINED}(g)$$

---

## 3. Three Independent Ownership Concepts

1. **Quarantine-Buffer Ownership:** Operations resided in `QuarantineBuffer._queue` with composite identities in `_identityIndex`. An operation is buffer-owned if and only if it is present in `_queue` and accounted for in `retainedBytes` / `retainedCount`.
2. **Drainer In-Flight Custody:** A dequeued operation held exclusively in `QuarantineDrainer._custodyState`. The drainer possesses sole execution custody. It is not owned by the buffer, nor is it owned by the archive.
3. **Terminal Archive Representation:** Operations immutably recorded in `TerminalArchiveCoreState.#committedTransactions` and appended to `#entries`.

```
┌────────────────────────┐      drainNext()      ┌─────────────────────────┐   Commit + Retirement   ┌───────────────────────────┐
│ QuarantineBuffer       │ ────────────────────► │ QuarantineDrainer       │ ──────────────────────► │ TerminalArchive           │
│ (Buffer Ownership)     │                       │ (In-Flight Custody)     │                         │ (Terminal Representation) │
│                        │ ◄──────────────────── │                         │                         │                           │
└────────────────────────┘     Protocol B        └─────────────────────────┘                         └───────────────────────────┘
                           (ABORTED_FENCED + Settlement)
```

---

## 4. Separation of the Three Evidence Levels

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          THREE EVIDENCE LEVELS                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ LEVEL 1: TRANSPORT-LEVEL OUTCOME                                                │
│ Evaluates transport frames over WebRTC DataChannel, local loop, or pipes.       │
│ Signals: DELIVERED, TRANSPORT_REJECTED, CHANNEL_CLOSED, TIMEOUT.                │
│ Invariant: Transport drop does NOT prove destination produced no side effect!   │
├─────────────────────────────────────────────────────────────────────────────────┤
│ LEVEL 2: DESTINATION APPLICATION-LEVEL OUTCOME                                  │
│ Evaluates application admission controller responses.                          │
│ Signals: ACCEPTED_REDUCED, ATTEMPT_INVALIDATED, TERMINALLY_REJECTED, etc.       │
│ Invariant: ATTEMPT_INVALIDATED proves attempt fencing, NOT archive absence!     │
├─────────────────────────────────────────────────────────────────────────────────┤
│ LEVEL 3: AUTHORITATIVE ARCHIVE TRANSACTION OUTCOME                              │
│ Direct atomic arbitration against TerminalArchiveCoreState.                     │
│ Signals: COMMITTED, ABORTED_FENCED, NOT_FOUND_AT_QUERY, INCONCLUSIVE.            │
│ Invariant: Authoritative state is governed SOLELY by Level 3 atomic decisions.  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Transaction Absence vs. Terminal Non-Commit & Disposition Consistency

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                      TRANSACTION OUTCOME TAXONOMY                                      │
├────────────────────────┬───────────────────────────────────────────────────────────────┤
│ State                  │ Definition & Architectural Operational Meaning                │
├────────────────────────┼───────────────────────────────────────────────────────────────┤
│ 1. NOT_FOUND_AT_QUERY  │ The transaction ID is absent from #committedTransactions      │
│                        │ at the query's linearization point. Asynchronous commit may   │
│                        │ still be executing. MUST NOT authorize buffer restoration!    │
├────────────────────────┼───────────────────────────────────────────────────────────────┤
│ 2. COMMIT_PENDING      │ The transaction has registered reservation or staged batch,   │
│                        │ and its commit operation is actively executing. Non-terminal. │
├────────────────────────┼───────────────────────────────────────────────────────────────┤
│ 3. COMMITTED           │ The transaction has committed via atomic root replacement     │
│                        │ into #committedTransactions. Irrevocable terminal success.    │
├────────────────────────┼───────────────────────────────────────────────────────────────┤
│ 4. ABORTED_FENCED      │ The archive has atomically recorded terminal cancellation and │
│                        │ fencing evidence. Any subsequent commit attempt is rejected.  │
│                        │ Irrevocable terminal abort. SOLE authorizer of Protocol B!    │
├────────────────────────┼───────────────────────────────────────────────────────────────┤
│ 5. INCONCLUSIVE        │ Archive state cannot be established (e.g. query failure,      │
│                        │ memory corruption, timeout). Preserves fail-closed custody.   │
└────────────────────────┴───────────────────────────────────────────────────────────────┘
```

### Archive Disposition Consistency Invariant:
$$\mathbf{committedTransactions} \cap \mathbf{fencedTransactions} = \emptyset$$

Before any candidate root is published via `this.#core = candidateRoot`, the archive validates:
1. **Disjointness:** No `retirementTransactionId` exists in both `#committedTransactions` and `#fencedTransactions`.
2. **Reservation Consumption:** If committed, `reservationId` is removed from `#activeReservations`; if fenced, reserved capacity is subtracted from `reservedBytes`.
3. **Generation Floor Monotonicity:** `candidateRoot.fencingGenerationFloor >= currentRoot.fencingGenerationFloor`.

---

## 6. Commit-Path Fencing & Exact Immutable Retry Identity

### 6.1 Exact Immutable Identity Equality for Idempotent Retries
A retried commit request is authorized to receive a cached `TerminalBatchCommitReceipt` if and only if it represents the **exact same immutable transaction**.

Before returning a cached receipt, the archive performs a canonical comparison of the incoming request against the committed record across all 6 identity dimensions:
1. `txId`: Exact string equality.
2. `reservationId`: Exact string equality.
3. `fencingGeneration`: Exact numeric integer equality.
4. `membershipEpoch`: Exact string equality.
5. `batchDigest`: Exact SHA-256 hex string equality.
6. `operationIdentities`: Canonical identity collection equality:
   - **Ordering:** Canonical lexicographical sort of composite operation IDs (`streamId:sequenceNumber`).
   - **Duplicate Rejection:** If the incoming array contains duplicate IDs, it is rejected with `DUPLICATE_OPERATION_IDENTITY_DETECTED`.
   - **1-to-1 Match:** Every sorted composite string ID matches the sorted recorded ID exactly.

```typescript
// Proposed M5.1 Exact Immutable Identity Verification:
function assertExactImmutableIdentityMatch(
  committed: QuarantineRetirementTransactionRecord,
  incoming: CommitRetirementBatchParams,
): void {
  if (committed.retirementTransactionId !== incoming.retirementTransactionId) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: txId mismatch.');
  }
  if (committed.reservationId !== incoming.reservationId) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: reservationId mismatch.');
  }
  if (committed.fencingGeneration !== incoming.stagedGeneration) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: fencingGeneration mismatch.');
  }
  if (committed.membershipEpoch !== incoming.membershipEpoch) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: membershipEpoch mismatch.');
  }
  if (committed.batchDigest !== incoming.batchDigest) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: batchDigest mismatch.');
  }
  const incomingSorted = canonicalizeIdentities(incoming.operationIdentities);
  const committedSorted = committed.canonicalOperationIdentities;
  if (incomingSorted.length !== committedSorted.length) {
    throw new Error('TRANSACTION_IDENTITY_MISMATCH_CONFLICT: operation count mismatch.');
  }
  for (let i = 0; i < incomingSorted.length; i++) {
    if (incomingSorted[i] !== committedSorted[i]) {
      throw new Error(`TRANSACTION_IDENTITY_MISMATCH_CONFLICT: operation identity mismatch at [${i}].`);
    }
  }
}
```

---

### 6.2 The Synchronous 9-Step Commit Protocol
```
[Asynchronous Payload Preparation & Hashing]
                 │
                 ▼
═════════════════════════════════════════════════════════════════════════════
SYNCHRONOUS ATOMIC PUBLICATION SEGMENT (Single Non-Interleavable Microtask Turn)
═════════════════════════════════════════════════════════════════════════════
1. Read Authoritative Archive Root: const currentRoot = this.#core;
2. Verify Transaction Disposition & Immutable Identity:
   if (currentRoot.fencedTransactions.has(txId)) throw TRANSACTION_ABORTED_FENCED;
   if (currentRoot.committedTransactions.has(txId)) {
     assertExactImmutableIdentityMatch(currentRoot.committedTransactions.get(txId), incoming);
     return currentRoot.committedTransactions.get(txId).receipt;
   }
3. Verify Fencing Generation:
   if (stagedGeneration <= currentRoot.fencingGenerationFloor ||
       stagedGeneration < currentRoot.fencingGeneration) throw GENERATION_OBSOLETE;
4. Verify Reservation Validity:
   if (!currentRoot.activeReservations.has(reservationId)) throw RESERVATION_NOT_FOUND_OR_FENCED;
5. Verify Immutable Batch Digest & Identity Invariants:
   if (currentRoot.activeReservations.get(reservationId).digest !== stagedDigest) throw DIGEST_MISMATCH;
6. Construct Committed Candidate Root from currentRoot (private Map clones, append entry, consume reservation);
7. Validate Disjointness Invariant: assertDisjoint(candidateRoot.committedTransactions, candidateRoot.fencedTransactions);
8. Advance Archive Generation: candidateRoot.archiveGeneration = currentRoot.archiveGeneration + 1;
9. LINEARIZATION POINT: this.#core = candidateRoot; (Synchronous Reference Swap)
═════════════════════════════════════════════════════════════════════════════
10. Return Authoritative TerminalBatchCommitReceipt;
```

---

## 7. Atomic Commit-versus-Cancellation Arbitration

```
                     ┌─────────────────────────────────────────┐
                     │ Atomic Candidate-Root Linearization     │
                     │ (Single Microtask JavaScript Turn)      │
                     └────────────────────┬────────────────────┘
                                          │
                 ┌────────────────────────┴────────────────────────┐
                 ▼                                                 ▼
     ┌───────────────────────┐                         ┌───────────────────────┐
     │      COMMIT WINS      │                         │   CANCELLATION WINS   │
     ├───────────────────────┤                         ├───────────────────────┤
     │ Candidate includes:   │                         │ Candidate includes:   │
     │ - Transaction commit  │                         │ - Fenced transaction  │
     │ - Entry appends       │                         │ - Released capacity   │
     │ - Reservation consumed│                         │ - Advanced generation │
     │ - Root swap committed │                         │ - Root swap committed │
     └───────────┬───────────┘                         └───────────┬───────────┘
                 │                                                 │
                 ▼                                                 ▼
      Protocol A Required                               Protocol B Permitted
     (Source Retirement +                              (Buffer Restoration +
      Promise Settlement)                               Promise Settlement)
```

---

## 8. Generation-Floor Scope & Strict Tombstone Backpressure Recovery

### 8.1 Scope & Monotonicity of Generations
- **Scope:** `fencingGeneration` is a monotonic counter strictly scoped to the local `TerminalQuarantineArchive` instance (`#core.fencingGeneration`).
- **Assignment to Reservations:** When a capacity reservation is created via `reserveCapacity(...)`, it receives:
  $$\text{reservation.generation} = \text{currentRoot.archiveGeneration}$$
  This generation is strictly greater than `fencingGenerationFloor`.

### 8.2 Safe FIFO Tombstone Eviction Rule
`fencingGenerationFloor` advances **if and only if** `#fencedTransactions` reaches capacity (`MAX_FENCED_TRANSACTIONS = 1000`) and the oldest tombstone can be safely evicted:
$$\text{candidateRoot.fencingGenerationFloor} = \max(\text{currentRoot.fencingGenerationFloor},\; \text{evictedTombstone.fencingGeneration})$$

### 8.3 Protection of Active Reservations & Strict Backpressure Recovery
To prevent advancing the floor past an active reservation while enforcing the hard 1,000-entry memory bound:
1. **Eviction Safety Condition:** Safe eviction requires:
   $$\mathbf{evictedTombstone.fencingGeneration} < \min_{r \in \mathbf{activeReservations}}(r.\mathbf{generation})$$
2. **Backpressure Trigger:** If `#fencedTransactions.size === 1000` and the eviction safety condition is violated, eviction is **strictly deferred**.
3. **Fail-Closed Backpressure State:**
   - Any new arbitration request that would add a 1,001st tombstone throws or returns:
     $$\mathbf{FENCING\_BACKPRESSURE\_CAPACITY\_EXHAUSTED}$$
   - New capacity reservations (`reserveCapacity`) are temporarily rejected with `CAPACITY_RESERVATION_BACKPRESSURE_BLOCKED`.
   - Existing active reservations and existing 1,000 tombstones are **100% preserved**.
   - The 1,000-entry bound is **never exceeded**.
   - Old tombstones are **never prematurely evicted**.
   - Floor **never advances past active reservations**.
4. **Synchronous Operational Recovery Path:**
   - When a blocking reservation settles (via `commitRetirementBatch` or `cancelReservation`), it is removed from `#activeReservations`.
   - In that **same synchronous microtask turn**, the archive evaluates whether deferred eviction can proceed:
     $$\text{newMinResGen} = \min_{r \in \text{activeReservations}}(r.\text{generation})$$
   - If the oldest tombstone's generation is now strictly $< \text{newMinResGen}$, the oldest tombstone is evicted immediately, advancing `fencingGenerationFloor`.
   - **Zero Unbounded Queues:** The archive does not maintain an unbounded queue of deferred fencing requests. Callers that received `FENCING_BACKPRESSURE_CAPACITY_EXHAUSTED` retry via caller-managed exponential backoff.
   - If active reservations remain blocking, the archive remains in backpressure fail-closed.

---

## 9. Process Failure Model

$$\mathbf{IN\-PROCESS,\; SINGLE\-RUNTIME\; MEMORY\; SAFETY\; ONLY}$$

1. **Volatile In-Memory Boundaries:** `TerminalArchiveCoreState` is held exclusively in JavaScript process memory (`this.#core`).
2. **Process Termination Effects:** Process crash, SIGKILL, browser tab closure, or host restart terminates process memory. All in-memory entries, committed transactions, active reservations, and `ABORTED_FENCED` tombstones are cleared.
3. **Restart Durability Disclaimer:** Restart durability is **NOT** claimed. An in-memory `ABORTED_FENCED` record protects against concurrent, delayed, or replayed execution **within the lifetime of the process**. It does not protect against external re-dispatch across process restarts.
4. **Future Architectural Dependency:** Durable persistence across process restarts or machine failures requires write-ahead logging (WAL) or transactional database backends. This is explicitly designated as an external dependency outside Phase 7.5 M5.1.

---

## 10. Disposition & Lost-Response Semantics

```typescript
getTransactionDisposition(
  retirementTransactionId: string,
): 'COMMITTED' | 'ABORTED_FENCED' | 'COMMIT_PENDING' | 'NOT_FOUND_AT_QUERY' | 'INCONCLUSIVE';
```

- **Lost Commit Response:** The client retries `commitRetirementBatch` with the identical immutable transaction 6-tuple. The archive detects `committedTransactions.has(txId)`, validates exact identity match, and idempotently returns the cached `TerminalBatchCommitReceipt`.
- **Lost Cancellation Response:** The client retries `arbitrateOrFenceTransaction(txId, resId, cap)`. The archive detects `fencedTransactions.has(txId)` and idempotently returns the cached `{ status: 'ABORTED_FENCED', ... }` receipt.
- **Lost Arbitration Response (Root Changed):** Caller queries `getTransactionDisposition(txId)`. Because the root swap was atomic, the query returns either `COMMITTED` or `ABORTED_FENCED`. The caller reconciles accordingly.
- **Transaction in Preparation:** Returns `COMMIT_PENDING` (if reservation exists) or `NOT_FOUND_AT_QUERY`. Does **NOT** authorize restoration.
- **Source Retirement Incomplete:** Query returns `COMMITTED`. Drainer resumes Protocol A source retirement.

---

## 11. Destination Outcome Classification & Post-Commit Promise Rejection

### A. Confirmed Committed (Level 3 Proof)
- **Condition:** Authoritative lookup `archive.getTransactionDisposition(txId) === 'COMMITTED'` confirms exact `txId`, matching `stagedBatchDigest`, and composite operation identities.
- **Required Action:** Execute Protocol A: retire source operations from `QuarantineBuffer`, verify buffer disjointness, await destination Promise settlement, publish attempt-fenced custody release, and resolve the settlement barrier upon verified authoritative evidence.

### B. Post-Commit Promise Rejection (Execution Failure)
- **Condition:** Archive transaction is confirmed `COMMITTED`, but the original destination Promise settles with a rejection (`Promise.reject(err)`).
- **Classification:** **Post-commit execution failure** (e.g. transport drop, post-commit listener exception).
- **Invariants:**
  1. The archive commit remains **100% authoritative and irrevocable**. Zero archive rollback!
  2. Protocol B restoration is **strictly prohibited** (preventing double replay).
  3. Execution is marked settled (`ExecutionSettlementReceipt.settlementType = 'REJECTED'`).
  4. Final custody reconciliation (source retirement verification) must still succeed.
  5. If reconciliation succeeds, construct `AuthoritativeSettlementEvidence`, publish custody release, and resolve the barrier. If reconciliation fails, enter `CUSTODY_RECONCILIATION_BLOCKED`.

### C. Aborted Fenced (Level 3 Proof)
- **Condition:** Atomic arbitration confirms `archive.arbitrateOrFenceTransaction(...)` returned `ABORTED_FENCED`.
- **Required Action:** Execute Protocol B: await destination Promise settlement, restore operation to `QuarantineBuffer` head via `unshiftDrain(op)`, verify buffer representation, construct authoritative settlement evidence, publish attempt-fenced custody release, and resolve the barrier.

### D. Inconclusive or Commit Pending (Level 3 Ambiguity)
- **Condition:** Arbitration throws, times out, or reports `NOT_FOUND_AT_QUERY` or `COMMIT_PENDING`.
- **Required Action:** Drainer in-flight custody is **strictly retained** ($N_{\text{in-flight}} = 1$). Gate transitions to `CUSTODY_RECONCILIATION_BLOCKED`. Settlement barrier remains **PENDING**.

---

## 12. Complete Custody Authorization & Settlement-Barrier Proof Contract

### 12.1 Five-Field Ownership Comparison & Equivalence Predicate (Mandatory Correction A)
To guarantee that a stale continuation cannot release custody owned by a newer recovery attempt, custody release requires an exact immutable `CustodyOwnershipToken` compared across **all five fields**:

```typescript
export interface CustodyOwnershipToken {
  readonly recoveryAttemptId: number;
  readonly custodyGeneration: number;
  readonly operationIdentity: string;
  readonly transactionId: string;
  readonly drainerOwnershipGeneration: number;
}

export function isEquivalentOwnerToken(
  current: CustodyOwnershipToken | null,
  expected: CustodyOwnershipToken,
): boolean {
  if (!current) return false;
  return (
    current.recoveryAttemptId === expected.recoveryAttemptId &&
    current.custodyGeneration === expected.custodyGeneration &&
    current.operationIdentity === expected.operationIdentity &&
    current.transactionId === expected.transactionId &&
    current.drainerOwnershipGeneration === expected.drainerOwnershipGeneration
  );
}
```

#### Adversarial Equivalence Example:
Consider an adversarial or confused delayed continuation:
- **Active Current State:** `Attempt 2`, `Custody Gen 1`, operating on `stream-A:seq-10` under `tx-2-res-1` (drainer gen 1).
- **Adversarial Caller Token:** `Attempt 2`, `Custody Gen 1`, but operating on `stream-B:seq-99` under `tx-2-res-99` (drainer gen 1).
- **Evaluation:** In R18-R8's pseudocode, comparing only attempt, custody generation, and drainer generation would have falsely succeeded. Under R18-R10, `isEquivalentOwnerToken` evaluates:
  $$\text{"stream-B:seq-99"} === \text{"stream-A:seq-10"} \implies \mathbf{false}$$
  $$\text{"tx-2-res-99"} === \text{"tx-2-res-1"} \implies \mathbf{false}$$
- **Result:** The release is rejected with `STALE_CUSTODY_RELEASE_REJECTED`. The active in-flight custody of `stream-A:seq-10` and its barrier are 100% untouched.

---

### 12.2 Distinguishing Claims from Authoritative Evidence (Mandatory Correction A & B)
To eliminate the vulnerability of caller-fabricated Boolean flags, the architecture enforces a strict tripartite distinction:
1. **Typed Outcome Description (Caller Claim):** A public data interface describing a desired state (e.g. `{ executionSettled: true }`). Trust level: ZERO. Any caller can construct it. It cannot authorize barrier resolution.
2. **Authoritative Subsystem Receipt (Component Evidence):** An unforgeable receipt issued SOLELY by the authoritative component executing the operation:
   - **Execution Settlement:** Issued by `DestinationExecutionObserver` upon Promise completion.
   - **Archive Disposition:** Issued by `TerminalQuarantineArchive` upon atomic candidate swap.
   - **Source Custody:** Issued by `QuarantineBuffer` upon verified retirement or restoration.
   - **Drainer Release:** Issued by `QuarantineDrainer` upon verified 5-field owner release.
3. **Validated Settlement Proof (Barrier Authorization):** The consolidated proof constructed by passing all four authoritative subsystem receipts through `assertAuthoritativeSettlementEvidence`.

---

### 12.3 Complete Five-Field Custody Identity & Receipt Binding (Mandatory Correction B)
Every authoritative receipt accepted by the recovery subsystem must immutably embed the exact five-field `CustodyOwnershipIdentity`:

```typescript
export interface CustodyOwnershipIdentity {
  readonly recoveryAttemptId: number;
  readonly custodyGeneration: number;
  readonly operationIdentity: string;
  readonly transactionId: string;
  readonly drainerOwnershipGeneration: number;
}

export function isMatchingReceiptIdentity(
  receiptIdentity: CustodyOwnershipIdentity,
  expectedOwner: CustodyOwnershipIdentity,
): boolean {
  return (
    receiptIdentity.recoveryAttemptId === expectedOwner.recoveryAttemptId &&
    receiptIdentity.custodyGeneration === expectedOwner.custodyGeneration &&
    receiptIdentity.operationIdentity === expectedOwner.operationIdentity &&
    receiptIdentity.transactionId === expectedOwner.transactionId &&
    receiptIdentity.drainerOwnershipGeneration === expectedOwner.drainerOwnershipGeneration
  );
}
```

---

### 12.4 Discriminated Outcome-Specific Custody Reconciliation (Mandatory Correction C)
```typescript
export type CustodyReconciliationReceipt =
  | SourceQuarantineRetirementReceipt
  | SourceQuarantineRestorationReceipt;

export interface SourceQuarantineRetirementReceipt {
  readonly outcome: 'COMMITTED';
  readonly identity: CustodyOwnershipIdentity;
  readonly retiredOperationCount: number;
  readonly retiredBytes: number;
  readonly sourceBufferDisjoint: true;
  readonly retirementDigest: string;
}

export interface SourceQuarantineRestorationReceipt {
  readonly outcome: 'ABORTED_FENCED';
  readonly identity: CustodyOwnershipIdentity;
  readonly restoredOperationCount: number;
  readonly restoredBytes: number;
  readonly sourceBufferContainsOperation: true;
  readonly restorationDigest: string;
}
```

---

### 12.5 Explicit Drainer Custody Release Evidence (Mandatory Correction D)
The barrier coordinator must not assume custody has been released merely because `QuarantineDrainer._custodyState.inFlightRecord === null`. The drainer must explicitly issue a branded `DrainerCustodyReleaseReceipt`:

```typescript
export interface DrainerCustodyReleaseReceipt {
  readonly outcome: 'CUSTODY_RELEASED';
  readonly identity: CustodyOwnershipIdentity;
  readonly releasedAtMonotonicMs: number;
  readonly releasedInFlightSlot: true;
  readonly gateStateSettled: true;
}
```

---

### 12.6 The Final Authoritative Barrier Proof Predicate (Mandatory Correction E)
Before any settlement barrier can resolve, all four component receipts must be assembled into `AuthoritativeSettlementEvidence` and verified via `assertAuthoritativeSettlementEvidence`:

```typescript
export interface ExecutionSettlementReceipt {
  readonly outcome: 'EXECUTION_SETTLED';
  readonly identity: CustodyOwnershipIdentity;
  readonly settlementType: 'FULFILLED' | 'REJECTED';
  readonly settledAtMonotonicMs: number;
}

export type ArchiveDispositionReceipt =
  | { readonly outcome: 'COMMITTED'; readonly identity: CustodyOwnershipIdentity; readonly commitReceipt: TerminalBatchCommitReceipt }
  | { readonly outcome: 'ABORTED_FENCED'; readonly identity: CustodyOwnershipIdentity; readonly fencingToken: string; readonly fencingGeneration: number };

export interface AuthoritativeSettlementEvidence {
  readonly executionSettlement: ExecutionSettlementReceipt;
  readonly archiveDisposition: ArchiveDispositionReceipt;
  readonly custodyReconciliation: CustodyReconciliationReceipt;
  readonly drainerRelease: DrainerCustodyReleaseReceipt;
}

export function assertAuthoritativeSettlementEvidence(
  evidence: AuthoritativeSettlementEvidence,
  expectedOwner: CustodyOwnershipIdentity,
): void {
  // 1. Validate All Receipts Exist
  if (!evidence.executionSettlement || !evidence.archiveDisposition ||
      !evidence.custodyReconciliation || !evidence.drainerRelease) {
    throw new Error('BARRIER_EVIDENCE_INCOMPLETE: One or more authoritative receipts are missing.');
  }

  // 2. Validate Five-Field Identity Binding Across All Receipts
  if (!isMatchingReceiptIdentity(evidence.executionSettlement.identity, expectedOwner)) {
    throw new Error('BARRIER_EVIDENCE_IDENTITY_MISMATCH: Execution settlement identity mismatch.');
  }
  if (!isMatchingReceiptIdentity(evidence.archiveDisposition.identity, expectedOwner)) {
    throw new Error('BARRIER_EVIDENCE_IDENTITY_MISMATCH: Archive disposition identity mismatch.');
  }
  if (!isMatchingReceiptIdentity(evidence.custodyReconciliation.identity, expectedOwner)) {
    throw new Error('BARRIER_EVIDENCE_IDENTITY_MISMATCH: Custody reconciliation identity mismatch.');
  }
  if (!isMatchingReceiptIdentity(evidence.drainerRelease.identity, expectedOwner)) {
    throw new Error('BARRIER_EVIDENCE_IDENTITY_MISMATCH: Drainer release identity mismatch.');
  }

  // 3. Validate Outcome-Specific Custody Matching
  if (evidence.archiveDisposition.outcome === 'COMMITTED') {
    if (evidence.custodyReconciliation.outcome !== 'COMMITTED' ||
        evidence.custodyReconciliation.sourceBufferDisjoint !== true) {
      throw new Error('BARRIER_EVIDENCE_OUTCOME_MISMATCH: Committed archive requires verified source retirement.');
    }
  } else if (evidence.archiveDisposition.outcome === 'ABORTED_FENCED') {
    if (evidence.custodyReconciliation.outcome !== 'ABORTED_FENCED' ||
        evidence.custodyReconciliation.sourceBufferContainsOperation !== true) {
      throw new Error('BARRIER_EVIDENCE_OUTCOME_MISMATCH: Fenced archive requires verified source restoration.');
    }
  } else {
    throw new Error('BARRIER_EVIDENCE_INVALID: Archive disposition is not terminal.');
  }

  // 4. Validate Drainer Release Confirmation
  if (evidence.drainerRelease.releasedInFlightSlot !== true ||
      evidence.drainerRelease.gateStateSettled !== true) {
    throw new Error('BARRIER_EVIDENCE_INVALID: Drainer custody release not confirmed.');
  }
}
```

### Barrier Resolution Linearization Point:
Inside `GenerationSettlementBarrier.resolveWithEvidence(evidence)`, synchronously after `assertAuthoritativeSettlementEvidence(evidence, this.#ownerIdentity)` succeeds:
```typescript
this.#resolved = true;
this.#resolver(); // LINEARIZATION POINT: Barrier Promise Resolves
```

---

### 12.7 Closed Generation-Scoped Barrier Lifecycle (Mandatory Correction C)
Barriers are strictly scoped per **in-flight custody generation** ($g \in \mathbb{N}$):

```typescript
export class GenerationSettlementBarrier {
  public readonly generation: number;
  public readonly ownerToken: Readonly<CustodyOwnershipToken>;
  private readonly _promise: Promise<void>;
  private _resolver!: () => void;
  private _resolved: boolean = false;

  constructor(generation: number, ownerToken: CustodyOwnershipToken) {
    this.generation = generation;
    this.ownerToken = Object.freeze({ ...ownerToken });
    this._promise = new Promise<void>((resolve) => {
      this._resolver = resolve;
    });
  }

  public get promise(): Promise<void> {
    return this._promise;
  }

  public resolveWithEvidence(evidence: AuthoritativeSettlementEvidence): void {
    if (this._resolved) return;
    assertAuthoritativeSettlementEvidence(evidence, this.ownerToken);
    this._resolved = true;
    this._resolver();
  }

  public get isResolved(): boolean {
    return this._resolved;
  }
}
```

#### Lifecycle State Transitions:
1. **Creation:** When `drainNext()` dequeues operation $g$, `custodyGeneration` advances ($g \to g+1$), creating `new GenerationSettlementBarrier(g+1, ownerToken)`.
2. **Registration of Waiters:** Any coordinator or caller invoking `drainSettledBarrier()` awaits `this._activeBarrier.promise`. Concurrent callers share the identical Promise.
3. **Owner Association:** Bound immutably to `ownerToken`.
4. **Resolution:** `publishCustodyRelease(expectedOwnerToken, proof)` invokes `barrier.resolveWithEvidence(evidence)`.
5. **Retirement & Safe Replacement:** Active barrier reference is cleared only when the retiring barrier's generation matches:
   ```typescript
   if (this._activeBarrier && this._activeBarrier.generation === expectedOwnerToken.custodyGeneration) {
     this._activeBarrier = null;
   }
   ```
6. **Bypass Prohibition:** A new barrier cannot be created while `this._custodyState.inFlightRecord !== null`. `drainNext()` strictly fails closed.
7. **Failed Release Behavior:** If `publishCustodyRelease` throws or fails pre-verification, the barrier is **neither resolved nor retired**; it remains pending fail-closed.

---

### 12.8 Barrier Resolution and Retirement Execution Ordering (Mandatory Correction G)
To prevent partial success from being represented as full settlement, the subsystem executes operations in a strict, failure-safe order:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                    STRICT FINALIZATION & RETIREMENT ORDER                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ Step 1: Execution Settlement ──► Destination Promise settles ──► ExecutionSettlementReceipt│
│ Step 2: Archive Disposition  ──► Atomic arbitration swap   ──► ArchiveDispositionReceipt │
│ Step 3: Custody Reconcile    ──► Buffer retire / unshift   ──► CustodyReconciliationReceipt│
│ Step 4: Drainer Release      ──► 5-Field Match + Root Swap ──► DrainerCustodyReleaseReceipt│
│ Step 5: Proof Assembly       ──► Combine all 4 receipts into AuthoritativeSettlementEvidence│
│ Step 6: Proof Validation     ──► assertAuthoritativeSettlementEvidence(...) validates all│
│ Step 7: LINEARIZATION POINT  ──► activeBarrier(g).resolveWithEvidence(evidence)           │
│ Step 8: Barrier Retirement   ──► this._retireBarrier(g, expectedOwner) clears reference │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 12.9 Deep Immutability of Published Custody Records
```typescript
export interface DrainerCustodyState {
  readonly inFlightRecord: Readonly<DrainerInFlightRecord> | null;
  readonly isDraining: boolean;
  readonly gateState: CustodyGateState;
  readonly ownerToken: Readonly<CustodyOwnershipToken> | null;
}
```
- Internal records use ECMAScript `#` private fields.
- Nested operation identity arrays (`canonicalOperationIdentities`) are defensively cloned into detached instances.
- The snapshot object and all nested records are recursively frozen with `Object.freeze()`. Callers holding prior references cannot mutate the active custody state.

---

### 12.10 Protocol A: Drainer $\to$ Terminal Archive (Execution Settlement & Attempt Fenced)

```
[Level 3 Authoritative Commit Confirmed]
                 │
                 ▼
1. Authoritative Archive Lookup (archive.getTransactionDisposition(txId) === 'COMMITTED')
   └──► Yields ArchiveDispositionReceipt (COMMITTED)
                 │
                 ▼
2. Verify Commit Digest & Composite Operation Identities (assertExactImmutableIdentityMatch)
                 │
                 ▼
3. Source Quarantine Retirement (quarantineBuffer.retireCommittedBatch(identities, cap))
   └──► Yields SourceQuarantineRetirementReceipt (sourceBufferDisjoint === true)
                 │
                 ▼
4. Verify Source Buffer Disjointness (hasOperation(...) === false for all identities)
                 │
                 ▼
5. RETAIN DRAINER CUSTODY WHILE DESTINATION PROMISE IS PENDING
   [_custodyState.inFlightRecord remains occupied; activeBarrier(g) remains PENDING]
                 │
                 ▼
6. Await Actual Destination Promise Settlement (Fulfilled or Rejected)
   └──► Yields ExecutionSettlementReceipt (FULFILLED or REJECTED)
                 │
                 ▼
7. Reconcile Settlement Against Confirmed Commit:
   ├─ Fulfill: Standard success path.
   └─ Reject: Classify as post-commit execution failure; suppress error; Protocol B PROHIBITED!
                 │
                 ▼
8. ATTEMPT-FENCED CUSTODY RELEASE: this.publishCustodyRelease(ownerToken);
   [Verifies all 5 fields via isEquivalentOwnerToken; simultaneously publishes inFlightRecord = null and gateState = 'CUSTODY_SETTLED']
   └──► Yields DrainerCustodyReleaseReceipt
                 │
                 ▼
9. Assemble AuthoritativeSettlementEvidence & Validate:
   assertAuthoritativeSettlementEvidence(evidence, ownerToken)
                 │
                 ▼
10. Resolve Generation Barrier Exactly Once: activeBarrier(g).resolveWithEvidence(evidence);
                 │
                 ▼
11. Retire Generation Barrier: this._retireBarrier(expectedOwnerToken.custodyGeneration);
```

---

### 12.11 Protocol B: Drainer $\to$ Quarantine Buffer (12-Step Fencing Sequence)

```
[Settlement / Fencing Trigger (Promise Completed / Rejected / Invalidation Received)]
                 │
                 ▼
1. Identify Exact Transaction & Reservation (txId, reservationId, batchDigest, opIdentities)
                 │
                 ▼
2. Request Atomic Commit-versus-Cancellation Arbitration:
   archive.arbitrateOrFenceTransaction(txId, reservationId, capability)
   ├─ If COMMITTED ────────────────────────► DIVERT TO PROTOCOL A (Source Retirement)
   ├─ If INCONCLUSIVE / ERROR ─────────────► TRANSITION TO CUSTODY_RECONCILIATION_BLOCKED (Halt)
   └─ If ABORTED_FENCED ───────────────────▼
   └──► Yields ArchiveDispositionReceipt (ABORTED_FENCED)
                 │
                 ▼
3. Obtain Authoritative Terminal Outcome (Verify ArchiveArbitrationReceipt.status === 'ABORTED_FENCED')
                 │
                 ▼
4. Verify Fencing Identity & Cancellation Evidence (fencingToken, released capacity confirmed)
                 │
                 ▼
5. Await Actual Destination Promise Settlement (Fulfilled or Rejected)
   └──► Yields ExecutionSettlementReceipt
                 │
                 ▼
6. Safe Idempotent Buffer Head Restoration (quarantineBuffer.unshiftDrain(op))
   └──► Yields SourceQuarantineRestorationReceipt (sourceBufferContainsOperation === true)
                 │
                 ▼
7. Verify Buffer Representation (hasOperation(...) === true & retainedCount++)
                 │
                 ▼
8. ATTEMPT-FENCED CUSTODY RELEASE: this.publishCustodyRelease(ownerToken);
   [Verifies all 5 fields via isEquivalentOwnerToken; simultaneously publishes inFlightRecord = null and gateState = 'CUSTODY_SETTLED']
   └──► Yields DrainerCustodyReleaseReceipt
                 │
                 ▼
9. Assemble AuthoritativeSettlementEvidence & Validate:
   assertAuthoritativeSettlementEvidence(evidence, ownerToken)
                 │
                 ▼
10. Resolve Generation Barrier Exactly Once: activeBarrier(g).resolveWithEvidence(evidence);
                 │
                 ▼
11. Retire Generation Barrier: this._retireBarrier(expectedOwnerToken.custodyGeneration);
```

---

## 13. The Custody-Reconciliation Gate State Machine

```mermaid
stateDiagram-v2
    [*] --> CUSTODY_SETTLED: Initial Engine Startup (N=0)

    CUSTODY_SETTLED --> SETTLEMENT_PENDING: drainNext() dequeues op (N=1, Barrier g created)

    SETTLEMENT_PENDING --> CUSTODY_RECONCILIATION_REQUIRED: Fencing Trigger fired or Promise Settled

    CUSTODY_RECONCILIATION_REQUIRED --> CUSTODY_SETTLED: Protocol A or B Complete (publishCustodyRelease succeeds, Barrier g resolves)
    CUSTODY_RECONCILIATION_REQUIRED --> CUSTODY_RECONCILIATION_BLOCKED: Arbitration Inconclusive / Verification Threw

    CUSTODY_RECONCILIATION_BLOCKED --> CUSTODY_RECONCILIATION_REQUIRED: Verified query / Idempotent retry initiated
    CUSTODY_RECONCILIATION_BLOCKED --> [*]: Reset strictly BLOCKED; No automated root swap
```

### State Operational Rules

| Gate State | Operational Meaning | Reset Request Admission | Drainer Dequeue Permitted? | Candidate Root Staging Permitted? | Root Replacement Permitted? | Barrier Status | Next Action Owner | Transition Conditions |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| **`CUSTODY_SETTLED`** | Operation has exactly one established terminal or recoverable owner. In-flight slot empty ($N_{\text{in-flight}} = 0$). | **PERMITTED** from `DEGRADED`. | **YES**, if buffer non-empty. | **PERMITTED** by active attempt. | **PERMITTED** after Step 7 zero-custody verification. | **RESOLVED** (Idle). | Recovery Coordinator / Drainer loop. | Transitions to `SETTLEMENT_PENDING` when `drainNext()` dequeues an operation. |
| **`SETTLEMENT_PENDING`** | Destination Promise has not settled. Drainer retains in-flight record ($N_{\text{in-flight}} = 1$). | **PERMITTED** from `DEGRADED` strictly into Step 4 barrier wait. | **STRICTLY PROHIBITED** ($N_{\text{in-flight}} \le 1$). | **STRICTLY PROHIBITED.** | **STRICTLY PROHIBITED.** | **PENDING** (Unresolved). | Asynchronous Destination Promise. | Transitions to `CUSTODY_RECONCILIATION_REQUIRED` when destination Promise completes. Watchdog timeout does NOT change gate. |
| **`CUSTODY_RECONCILIATION_REQUIRED`** | Settlement trigger fired; atomic arbitration or custody transfer active (e.g. arbitration call, unshift, source retirement). | **REJECTED** fail-closed (`RECOVERY_ALREADY_IN_PROGRESS`). | **STRICTLY PROHIBITED.** | **STRICTLY PROHIBITED.** | **STRICTLY PROHIBITED.** | **PENDING** (Unresolved). | Drainer Reconciliation Handler. | Transitions to `CUSTODY_SETTLED` upon attempt-fenced custody release; transitions to `CUSTODY_RECONCILIATION_BLOCKED` on failure. |
| **`CUSTODY_RECONCILIATION_BLOCKED`** | Arbitration or reconciliation threw or produced an inconclusive result that cannot safely be resolved automatically. | **STRICTLY REJECTED** (`CUSTODY_RECONCILIATION_BLOCKED`). | **STRICTLY PROHIBITED.** | **STRICTLY PROHIBITED.** | **STRICTLY PROHIBITED.** | **PENDING** (Blocked). | Administrative Investigation / Retry API. | Can transition to `CUSTODY_RECONCILIATION_REQUIRED` ONLY through a verified query or successful idempotent reconciliation retry. |

---

## 14. Comprehensive Analysis of Cases A through H Across Operational States

| Operational State Case | Archive Transaction Status | Source Quarantine Status | Reservation Status | Drainer In-Flight Slot ($N_{\text{in-flight}}$) | Settlement Barrier Status | Custody Gate Status | Reset Admission Result | Outstanding Disposition | Condition Permitting Slot Release |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| **Case A: Watchdog timeout; destination Promise pending** | N/A (Not reached) | Retained in buffer / in-flight | None | 1 (Occupied by `op1`) | Pending (Unresolved) | `SETTLEMENT_PENDING` | **ACCEPTED into Step 4.** Awaits barrier under new Attempt 2. | Awaiting destination Promise resolution. | Promise settles AND Level 3 reconciliation finishes. |
| **Case B: Destination Promise settled; normal reconciliation** | Staging / Querying | Retained until Step 4 | Active | 1 (Reconciling) | Pending (Resolving) | `CUSTODY_RECONCILIATION_REQUIRED` | **REJECTED.** Throws `RECOVERY_ALREADY_IN_PROGRESS`. | Reconciliation in flight. | Protocol A or B completes fully. |
| **Case C: Destination settled; `unshiftDrain()` threw error** | `ABORTED_FENCED` | Restoration failed; stranded | Cancelled | 1 (Occupied) | Pending (Blocked) | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** Throws `CUSTODY_RECONCILIATION_BLOCKED`. | Buffer restoration unconfirmed. | Successful unshiftDrain retry confirms buffer custody. |
| **Case D: Archive commit outcome inconclusive** | Inconclusive (Arbitration required) | Retained in buffer / in-flight | Active / Unconsumed | 1 (Occupied) | Pending (Blocked) | `CUSTODY_RECONCILIATION_REQUIRED` | **STRICTLY REJECTED.** Throws `CUSTODY_RECONCILIATION_REQUIRED`. | Level 3 archive query not yet resolved. | Level 3 arbitration confirms status AND disposition completes. |
| **Case E: Archive query confirms committed (Transitional)** | Confirmed Committed | **Source retirement pending** | Consumed | 1 (Occupied) | Pending (Resolving) | `CUSTODY_RECONCILIATION_REQUIRED` (Transitional) | **REJECTED** while retiring; **ACCEPTED** once settled. | Source retirement in `QuarantineBuffer`. | `retireCommittedBatch()` completes, promise settles, AND zero source ownership confirmed. |
| **Case F: Archive query confirms absent (Transitional)** | `ABORTED_FENCED` | Restoration in progress | Safely Cancelled | 1 (Occupied) | Resolves post-unshift | `CUSTODY_RECONCILIATION_REQUIRED` (Transitional) | **ACCEPTED** once unshift completed. | Source restoration via `unshiftDrain(op)`. | Fencing verified, promise settled, AND `unshiftDrain(op)` verified. |
| **Case G: Archive query remains inconclusive** | Permanently Inconclusive | Retained in drainer | Active / Orphaned | 1 (Occupied) | Pending (Blocked) | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** Throws `CUSTODY_RECONCILIATION_BLOCKED`. | Transaction state unknown. | Verified Level 3 query succeeds; blind re-publish/restore is prohibited. |
| **Case H: Permanent custody fault / unrecoverable failure** | Inconclusive / Failed | Stranded in drainer | Faulted | 1 (Occupied) | Permanently Pending | `CUSTODY_RECONCILIATION_BLOCKED` | **STRICTLY REJECTED.** Throws `CUSTODY_RECONCILIATION_BLOCKED`. | Unresolved custody fault. | **No automated root replacement.** Emergency abandonment requires a separate unapproved authority. |

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| Case # | Scenario Description | Archive Transaction State | Execution Settlement State | Custody Reconciliation State | Drainer Release State | Identity Match Status | Barrier Status | Custody Gate State | Permitted Next Action |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| **1** | Caller fabricates Boolean claims | N/A | Forged claim | Forged claim | Unreleased | Rejected (No receipts) | **PENDING (g)** | `CUSTODY_RECONCILIATION_REQUIRED` | Throws `BARRIER_EVIDENCE_INCOMPLETE`; barrier pending. |
| **2** | Valid 4-receipt exact identity match | `COMMITTED` | Settled (`FULFILLED`) | `SourceRetirementReceipt` | `ReleaseReceipt` | **100% Match** | **RESOLVED (g)**| `CUSTODY_SETTLED` | Barrier resolves; custody cleanly settled. |
| **3** | Valid 4-receipt exact match (Aborted) | `ABORTED_FENCED`| Settled (`REJECTED`) | `SourceRestorationReceipt`| `ReleaseReceipt` | **100% Match** | **RESOLVED (g)**| `CUSTODY_SETTLED` | Barrier resolves; restored to buffer. |
| **4** | Stale execution receipt from Attempt 1 | `COMMITTED` | Attempt 1 receipt | Current Attempt 2 | Current Attempt 2 | **Mismatch (Attempt)**| **PENDING (g)** | `SETTLEMENT_PENDING` | Throws `IDENTITY_MISMATCH`; Attempt 2 barrier pending. |
| **5** | Archive receipt for wrong transaction ID | `COMMITTED` (Tx B)| Settled (Tx A) | Reconciled (Tx A) | Released (Tx A) | **Mismatch (TxId)** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `IDENTITY_MISMATCH`; barrier pending. |
| **6** | Archive receipt for wrong custody gen | `COMMITTED` | Settled (Gen 1) | Reconciled (Gen 1) | Released (Gen 1) | **Mismatch (Gen)** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `IDENTITY_MISMATCH`; barrier pending. |
| **7** | Mismatched drainer ownership gen | Any | Any | Any | Gen Mismatch | **Mismatch (DrainGen)**| **PENDING (g)**| `CUSTODY_RECONCILIATION_BLOCKED` | Throws `IDENTITY_MISMATCH`; barrier pending. |
| **8** | Committed tx with restoration receipt | `COMMITTED` | Settled | Restoration (Invalid)| Released | **Outcome Mismatch** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `OUTCOME_MISMATCH`; barrier pending. |
| **9** | Fenced tx with retirement receipt | `ABORTED_FENCED`| Settled | Retirement (Invalid) | Released | **Outcome Mismatch** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `OUTCOME_MISMATCH`; barrier pending. |
| **10** | Promise rejected without archive receipt | Inconclusive | Settled (`REJECTED`) | None | Unreleased | Incomplete | **PENDING (g)** | `CUSTODY_RECONCILIATION_REQUIRED` | Awaits Level 3 arbitration; zero blind restoration. |
| **11** | Post-commit Promise rejection | `COMMITTED` | Settled (`REJECTED`) | `SourceRetirementReceipt` | `ReleaseReceipt` | **100% Match** | **RESOLVED (g)**| `CUSTODY_SETTLED` | Rejection suppressed; Protocol A completes cleanly. |
| **12** | Caller fabricates drainer release receipt | Any | Any | Any | Forged token | Invalid signature | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `DRAINER_RELEASE_INVALID`; barrier pending. |
| **13** | Stale release receipt for older generation | Current Gen 2 | Settled | Reconciled | Gen 1 release | **Mismatch (Gen)** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Throws `IDENTITY_MISMATCH`; barrier 2 pending. |
| **14** | Barrier evaluated before Promise settles | `COMMITTED` | **Pending** | Reconciled | Released | Incomplete | **PENDING (g)** | `SETTLEMENT_PENDING` | Missing execution receipt; barrier pending. |
| **15** | Barrier evaluated before archive swap | Pending | Settled | None | Unreleased | Incomplete | **PENDING (g)** | `SETTLEMENT_PENDING` | Missing archive receipt; barrier pending. |
| **16** | Barrier evaluated before buffer retire | `COMMITTED` | Settled | Pending | Unreleased | Incomplete | **PENDING (g)** | `CUSTODY_RECONCILIATION_REQUIRED` | Missing custody receipt; barrier pending. |
| **17** | Barrier evaluated before drainer release | `COMMITTED` | Settled | Reconciled | Unreleased | Incomplete | **PENDING (g)** | `CUSTODY_RECONCILIATION_REQUIRED` | Missing release receipt; barrier pending. |
| **18** | Watchdog expires at 5,000 ms | Pending | Pending | None | None | None | **PENDING (g)** | `DEGRADED` | Status latched; barrier remains pending fail-closed. |
| **19** | Late continuation attempts newer barrier | Current Gen 2 | Late Gen 1 settled | Gen 1 | Gen 1 | Stale generation | **PENDING (2)** | `SETTLEMENT_PENDING` | Gen 1 evidence rejected; Gen 2 barrier untouched. |
| **20** | Failure during barrier resolution | `COMMITTED` | Settled | Reconciled | Released | Exception thrown | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | Barrier remains pending; obligations preserved. |
| **21** | Duplicate resolution of barrier g | Resolved | Settled | Reconciled | Released | Already resolved | **RESOLVED (g)**| `CUSTODY_SETTLED` | Evaluates `#resolved`; no secondary resolution. |
| **22** | Tombstone collection at 1,000 capacity | 1000 Tombs | Normal | Normal | Normal | Normal | Normal | Normal | Throws `FENCING_BACKPRESSURE_CAPACITY_EXHAUSTED`. |
| **23** | Blocking reservation settles | 1000 Tombs | Res settled | Res settled | Normal | Normal | Normal | Normal | Deferred eviction runs; backpressure relieved. |
| **24** | Caller attempts mutation of receipt | Frozen | Any | Any | Any | Immutable | Normal | Normal | Mutation throws in strict mode; evidence untouched. |

---

## 16. Mandatory Barrier and Drain Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{NO\_IN\_FLIGHT\_OWNER}(g)$$
2. **Attempt-Fenced Custody Release Invariant:**
   $$\mathbf{STALE\_ATTEMPT} \implies \mathbf{CANNOT\_RELEASE\_NEWER\_CUSTODY}$$
3. **Execution Settlement Precedence:** The settlement barrier **CANNOT** resolve while the destination Promise remains pending.
4. **Five-Field Owner-Token Verification Precedence:** Custody release **CANNOT** execute unless `isEquivalentOwnerToken` evaluates to `true` across all 5 fields.
5. **Authoritative Evidence Precedence:** A barrier **CANNOT** resolve unless `assertAuthoritativeSettlementEvidence` validates all four component receipts.
6. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
7. **Active Reservation Floor Protection:** `fencingGenerationFloor` **CANNOT** advance past $\min_{r \in \text{activeReservations}}(r.\text{generation})$.
8. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this._custodyState.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
9. **Watchdog Immunity:** A watchdog timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) **CANNOT** release drainer custody or cancel the destination Promise.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Release Drainer} \longrightarrow \text{4. Validate Proof} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R10 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`) and latched `TIMED_OUT`. Its continuation is completely fenced and cannot mutate roots or stage batches.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's continuation attempts `publishCustodyRelease(token1)`, which is rejected because Attempt 2 owns the coordinator. The coordinator reconciles custody, executes `publishCustodyRelease(token2)`, and resolves the barrier with verified authoritative evidence. Attempt 2 unblocks with clean custody and proceeds to Step 7 with exclusive authority.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

Sub-Milestone M5.1 test suites must verify the following twenty-four deterministic test sequences:

- **Test R18-R10-T1 (Caller-Created Boolean Claims Rejected):** Pass object with Boolean flags set to true; assert `assertAuthoritativeSettlementEvidence` throws `BARRIER_EVIDENCE_INCOMPLETE`.
- **Test R18-R10-T2 (Only Execution-Owner-Issued Settlement Evidence Accepted):** Provide genuine `ExecutionSettlementReceipt`; assert validation succeeds.
- **Test R18-R10-T3 (Stale Execution Receipt From Earlier Attempt Rejected):** Pass Attempt 1 execution receipt to Attempt 2 barrier; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T4 (Archive Receipt for Different Transaction Rejected):** Pass receipt with mismatched `transactionId`; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T5 (Archive Receipt for Correct Tx But Wrong Custody Gen Rejected):** Pass receipt with mismatched `custodyGeneration`; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T6 (Mismatched Drainer Ownership Generation Rejected):** Pass receipt with mismatched `drainerOwnershipGeneration`; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T7 (Committed Transaction Requires Authoritative Source Retirement):** Provide `SourceQuarantineRestorationReceipt` for `COMMITTED` disposition; assert throws `BARRIER_EVIDENCE_OUTCOME_MISMATCH`.
- **Test R18-R10-T8 (Aborted Transaction Requires Authoritative Source Restoration):** Provide `SourceQuarantineRetirementReceipt` for `ABORTED_FENCED` disposition; assert throws `BARRIER_EVIDENCE_OUTCOME_MISMATCH`.
- **Test R18-R10-T9 (Promise Rejection Without Archive Receipt Does Not Prove Abort):** Destination Promise rejects; assert barrier remains pending until Level 3 arbitration receipt provided.
- **Test R18-R10-T10 (Promise Rejection After Commit Remains Post-Commit Failure):** Promise rejects after archive commit; assert `ExecutionSettlementReceipt` with `settlementType: 'REJECTED'` resolves barrier under Protocol A without restoration.
- **Test R18-R10-T11 (Fabricated Drainer Release Receipt Rejected):** Fabricate release receipt without calling `publishCustodyRelease`; assert rejected by internal token verification.
- **Test R18-R10-T12 (Stale Drainer Release Receipt Cannot Resolve Later Barrier):** Pass generation 1 release receipt to generation 2 barrier; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T13 (Barrier Cannot Resolve Before Actual Promise Settlement):** Omit execution settlement receipt; assert barrier remains pending.
- **Test R18-R10-T14 (Barrier Cannot Resolve Before Terminal Archive Disposition):** Omit archive disposition receipt; assert barrier remains pending.
- **Test R18-R10-T15 (Barrier Cannot Resolve Before Custody Reconciliation):** Omit custody reconciliation receipt; assert barrier remains pending.
- **Test R18-R10-T16 (Barrier Cannot Resolve Before Exact-Owner Drainer Release):** Omit drainer release receipt; assert barrier remains pending.
- **Test R18-R10-T17 (All Receipts Must Match Five-Field Identity 1-to-1):** Provide 4 receipts with one field altered on one receipt; assert throws `BARRIER_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R10-T18 (Late Continuation Cannot Mutate Newer Generation):** Late Promise settlement for Attempt 1 cannot resolve Attempt 2 barrier.
- **Test R18-R10-T19 (Barrier Resolution Failure Preserves Custody Obligations):** Mock throw in `assertAuthoritativeSettlementEvidence`; assert custody remains held fail-closed.
- **Test R18-R10-T20 (Watchdog Timeout Leaves Barrier Pending):** Fire 5,000 ms watchdog; assert status latches `DEGRADED`; assert barrier remains unresolved.
- **Test R18-R10-T21 (Resolved Barrier Cannot Be Reused for Later Generation):** Resolve barrier 1; assert barrier 2 requires discrete resolution.
- **Test R18-R10-T22 (Rejected Backpressure Requests Do Not Accumulate Work):** Trigger backpressure; assert rejected calls leave zero timers or queues.
- **Test R18-R10-T23 (Tombstone Capacity Remains At or Below 1,000 Bound):** Repeatedly trigger fencing under capacity pressure; assert `#fencedTransactions.size <= 1000`.
- **Test R18-R10-T24 (Every Operation Remains Accounted For Across Commit/Abort):** Audit operations across all transitions; assert zero operations left ownerless.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R10):
================================================================================
1. [AUTHORITATIVE EVIDENCE VS CLAIMS]: Does the Engineering Authority approve
   replacing caller-supplied Boolean claims with unforgeable component receipts
   (ExecutionSettlementReceipt, ArchiveDispositionReceipt, CustodyReconciliationReceipt,
   DrainerCustodyReleaseReceipt)?

2. [COMPLETE 5-FIELD IDENTITY BINDING]: Does the Engineering Authority approve
   enforcing isMatchingReceiptIdentity across all five fields on every single receipt,
   preventing cross-attempt or cross-generation receipt reuse?

3. [OUTCOME-SPECIFIC CUSTODY RECONCILIATION]: Does the Engineering Authority approve
   discriminating between SourceQuarantineRetirementReceipt (for COMMITTED) and
   SourceQuarantineRestorationReceipt (for ABORTED_FENCED)?

4. [EXPLICIT DRAINER RELEASE EVIDENCE]: Does the Engineering Authority approve
   requiring DrainerCustodyReleaseReceipt issued exclusively upon exact-owner
   validation and atomic custody publication?

5. [PROOF LINEARIZATION & RETIREMENT ORDER]: Does the Engineering Authority approve
   the strict 8-step execution order, ensuring barriers resolve only upon verified
   proof and retirement never erases unresolved custody obligations?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R10 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R10 proposal achieves definitive mathematical and distributed-systems closure: it eliminates caller-fabricated claims by requiring unforgeable authoritative subsystem receipts, enforces 1-to-1 five-field identity binding across all evidence, discriminates outcome-specific custody reconciliation, includes explicit drainer release receipts, and establishes a singular, validated barrier linearization point.

### Mandatory Implementation Authorization Statement
**"M5.0-R18-R10 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."**
