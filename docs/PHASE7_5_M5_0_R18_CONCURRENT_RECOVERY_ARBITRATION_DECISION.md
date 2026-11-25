# Phase 7.5 M5.0-R18-R8 — Stale Custody-Release Fencing, Barrier Lifecycle & Bounded Tombstone Backpressure

**Role:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Mode:** STRICTLY DOCUMENTATION-ONLY ARCHITECTURAL CORRECTION
**Target Document:** `docs/PHASE7_5_M5_0_R18_CONCURRENT_RECOVERY_ARBITRATION_DECISION.md`
**Baseline Commit:** `f77d4e188ae9f585be13f3605e05a9c004c062cf`
**Direct Parent Commit:** `f77d4e188ae9f585be13f3605e05a9c004c062cf`
**Implementation Authorization:** Milestone M5.1 remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R8 Objectives

The M5.0-R18-R7 architectural decision established exact 6-tuple immutable identity matching for idempotent retries, failure-safe custody publication (Option A), and generation-floor advancement rules. However, rigorous distributed-systems review revealed four critical lifecycle and concurrency hazards:

1. **Unfenced Custody-State Clearance:** In R18-R7, `_publishCustodyRelease()` unconditionally cleared whichever in-flight record was stored. If Attempt 1 timed out and was superseded by Attempt 2, a delayed asynchronous continuation from Attempt 1 executing `_publishCustodyRelease()` could clear Attempt 2's active in-flight custody and prematurely resolve Attempt 2's barrier. Custody release must be explicitly fenced by an immutable ownership token (`CustodyOwnershipToken`).
2. **Monolithic Barrier Lifecycle:** The lifetime-wide boolean flag `_barrierResolved` prevented subsequent operations from resolving their own barriers once an earlier barrier had resolved. Barriers must be scoped per in-flight custody generation ($g \in \mathbb{N}$), with once-only resolution per generation that does not prevent subsequent barriers from resolving.
3. **Tombstone Capacity Deadlock & Backpressure:** R18-R7 deferred tombstone eviction if advancing the generation floor would breach an active reservation, but did not define what occurs when the collection reaches its strict ceiling (`MAX_FENCED_TRANSACTIONS = 1000`) and safe eviction remains blocked. The archive must enforce a strict, fail-closed backpressure policy (`FENCING_BACKPRESSURE_CAPACITY_EXHAUSTED`).
4. **Shallow Immutability of Published Records:** `Object.freeze()` on the outer custody-state object leaves nested identity arrays mutable. Published custody records must enforce deep runtime immutability via defensive deep-cloning and private backing fields.

### Core Architectural Guarantees Established in R18-R8:
- **Attempt-Fenced Custody Release:** `publishCustodyRelease(expectedOwnerToken)` verifies that the current custody state matches the exact expected owner across 5 dimensions (`recoveryAttemptId`, `custodyGeneration`, `operationIdentity`, `transactionId`, `drainerOwnershipGeneration`). Stale continuations reject fail-closed without mutating newer custody:
  $$\mathbf{STALE\_ATTEMPT} \implies \mathbf{CANNOT\_RELEASE\_NEWER\_CUSTODY}$$
- **Generation-Scoped Settlement-Barrier Lifecycle:** Each in-flight drain operation allocates a discrete `SettlementBarrier(g)` tied to custody generation $g$. Each barrier resolves at most once, and resolution of generation $g$ does not impede generation $g+1$:
  $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{NO\_IN\_FLIGHT\_OWNER}(g)$$
- **Strict Tombstone Capacity & Backpressure Contract:** Preserves the hard 1,000-entry ceiling. If safe eviction is blocked by an active reservation, the archive enters fail-closed backpressure, rejecting new fencing and reservation requests while preserving existing valid state.
- **Deep Immutability & Encapsulation of Custody Records:** Guarantees that published custody snapshots and nested operation identity arrays are deeply cloned, private-backed, and frozen against external mutation.
- **Twenty Deterministic Future Test Requirements:** Codifies 20 exhaustive test scenarios for future M5.1 verification suites.

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

## 8. Generation-Floor Scope & Strict Tombstone Backpressure

### 8.1 Scope & Monotonicity of Generations
- **Scope:** `fencingGeneration` is a monotonic counter strictly scoped to the local `TerminalQuarantineArchive` instance (`#core.fencingGeneration`).
- **Assignment to Reservations:** When a capacity reservation is created via `reserveCapacity(...)`, it receives:
  $$\text{reservation.generation} = \text{currentRoot.archiveGeneration}$$
  This generation is strictly greater than `fencingGenerationFloor`.

### 8.2 Safe FIFO Tombstone Eviction Rule
`fencingGenerationFloor` advances **if and only if** `#fencedTransactions` reaches capacity (`MAX_FENCED_TRANSACTIONS = 1000`) and the oldest tombstone can be safely evicted:
$$\text{candidateRoot.fencingGenerationFloor} = \max(\text{currentRoot.fencingGenerationFloor},\; \text{evictedTombstone.fencingGeneration})$$

### 8.3 Protection of Active Reservations & Strict Backpressure
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
4. **Capacity Recovery:** As soon as the oldest blocking active reservation settles (via commit or expiry/timeout), its generation is cleared from active reservations, allowing deferred tombstone eviction to proceed safely and restoring normal operation.

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
- **Required Action:** Execute Protocol A: retire source operations from `QuarantineBuffer`, verify buffer disjointness, await destination Promise settlement, publish attempt-fenced custody release, and resolve the generation-scoped settlement barrier.

### B. Post-Commit Promise Rejection (Execution Failure)
- **Condition:** Archive transaction is confirmed `COMMITTED`, but the original destination Promise settles with a rejection (`Promise.reject(err)`).
- **Classification:** **Post-commit execution failure** (e.g. transport drop, post-commit listener exception).
- **Invariants:**
  1. The archive commit remains **100% authoritative and irrevocable**. Zero archive rollback!
  2. Protocol B restoration is **strictly prohibited** (preventing double replay).
  3. Execution is marked settled (`EXECUTION_SETTLED = true`).
  4. Final custody reconciliation (source retirement verification) must still succeed.
  5. If reconciliation succeeds, publish attempt-fenced custody release and resolve the barrier. If reconciliation fails, enter `CUSTODY_RECONCILIATION_BLOCKED`.

### C. Aborted Fenced (Level 3 Proof)
- **Condition:** Atomic arbitration confirms `archive.arbitrateOrFenceTransaction(...)` returned `ABORTED_FENCED`.
- **Required Action:** Execute Protocol B: await destination Promise settlement, restore operation to `QuarantineBuffer` head via `unshiftDrain(op)`, verify buffer representation, publish attempt-fenced custody release, and resolve the barrier.

### D. Inconclusive or Commit Pending (Level 3 Ambiguity)
- **Condition:** Arbitration throws, times out, or reports `NOT_FOUND_AT_QUERY` or `COMMIT_PENDING`.
- **Required Action:** Drainer in-flight custody is **strictly retained** ($N_{\text{in-flight}} = 1$). Gate transitions to `CUSTODY_RECONCILIATION_BLOCKED`. Settlement barrier remains **PENDING**.

---

## 12. Attempt-Fenced Custody Release & Settlement-Barrier Lifecycle

### 12.1 Attempt-Fenced Custody Release (Mandatory Correction A)
To guarantee that a stale continuation cannot clear custody owned by a newer recovery attempt, custody release requires an exact immutable `CustodyOwnershipToken`:

```typescript
export interface CustodyOwnershipToken {
  readonly recoveryAttemptId: number;
  readonly custodyGeneration: number;
  readonly operationIdentity: string;
  readonly transactionId: string;
  readonly drainerOwnershipGeneration: number;
}

export interface DrainerCustodyState {
  readonly inFlightRecord: Readonly<DrainerInFlightRecord> | null;
  readonly isDraining: boolean;
  readonly gateState: CustodyGateState;
  readonly ownerToken: Readonly<CustodyOwnershipToken> | null;
}
```

#### Attempt-Fenced Release Protocol:
```typescript
class QuarantineDrainer {
  private _custodyState: DrainerCustodyState = {
    inFlightRecord: null,
    isDraining: false,
    gateState: 'CUSTODY_SETTLED',
    ownerToken: null,
  };
  private _activeBarrier: GenerationSettlementBarrier | null = null;

  public publishCustodyRelease(expectedOwnerToken: CustodyOwnershipToken): void {
    const current = this._custodyState;

    // 1. STALE ATTEMPT FENCING CHECK:
    if (!current.ownerToken ||
        current.ownerToken.recoveryAttemptId !== expectedOwnerToken.recoveryAttemptId ||
        current.ownerToken.custodyGeneration !== expectedOwnerToken.custodyGeneration ||
        current.ownerToken.drainerOwnershipGeneration !== expectedOwnerToken.drainerOwnershipGeneration) {
      throw new Error(`STALE_CUSTODY_RELEASE_REJECTED: Stale attempt ${expectedOwnerToken.recoveryAttemptId} (gen: ${expectedOwnerToken.custodyGeneration}) cannot release custody owned by attempt ${current.ownerToken?.recoveryAttemptId} (gen: ${current.ownerToken?.custodyGeneration}).`);
    }

    // 2. SYNCHRONOUS ATOMIC PUBLICATION (Option A):
    this._custodyState = Object.freeze({
      inFlightRecord: null,
      isDraining: false,
      gateState: 'CUSTODY_SETTLED',
      ownerToken: null,
    });

    // 3. RESOLVE GENERATION-SPECIFIC BARRIER:
    if (this._activeBarrier && this._activeBarrier.generation === expectedOwnerToken.custodyGeneration) {
      this._activeBarrier.resolveOnce();
    }
  }
}
```

- **Fencing Guarantee:** If Attempt 1 times out and Attempt 2 takes custody, `current.ownerToken.recoveryAttemptId === 2`. A delayed continuation from Attempt 1 passes `recoveryAttemptId: 1`, immediately throwing `STALE_CUSTODY_RELEASE_REJECTED`. Attempt 2's in-flight record and barrier are **untouched**.
$$\mathbf{STALE\_ATTEMPT} \implies \mathbf{CANNOT\_RELEASE\_NEWER\_CUSTODY}$$

---

### 12.2 Generation-Scoped Settlement-Barrier Lifecycle (Mandatory Correction B)
Barriers are strictly scoped per **in-flight custody generation** ($g \in \mathbb{N}$):

```typescript
class GenerationSettlementBarrier {
  public readonly generation: number;
  private readonly _promise: Promise<void>;
  private _resolver!: () => void;
  private _resolved: boolean = false;

  constructor(generation: number) {
    this.generation = generation;
    this._promise = new Promise<void>((resolve) => {
      this._resolver = resolve;
    });
  }

  public get promise(): Promise<void> {
    return this._promise;
  }

  public resolveOnce(): void {
    if (this._resolved) return;
    this._resolved = true;
    this._resolver();
  }

  public get isResolved(): boolean {
    return this._resolved;
  }
}
```

#### Lifecycle Rules:
1. **Creation:** When `drainNext()` dequeues an operation, `custodyGeneration` advances ($g \to g+1$), creating a new `GenerationSettlementBarrier(g+1)`.
2. **Pending:** Any coordinator or caller invoking `drainSettledBarrier()` awaits `this._activeBarrier.promise`. Concurrent callers share the identical Promise.
3. **Once-Only Resolution:** `resolveOnce()` evaluates `#resolved`. Duplicate calls are no-ops.
4. **Subsequent Operations:** When operation $g$ settles, `barrier(g)` is resolved. When operation $g+1$ starts later, a fresh `GenerationSettlementBarrier(g+1)` is created. Resolution of $g$ does not block $g+1$.
5. **Bypass Prohibition:** A new barrier cannot be created while `this._custodyState.inFlightRecord !== null`. `drainNext()` fails closed.

---

### 12.3 Deep Immutability of Published Custody Records (Mandatory Correction D)
To eliminate reference aliasing and mutation vulnerabilities:
1. **Private Storage:** Internal records use ECMAScript `#` private fields.
2. **Defensive Deep Cloning:** Any array of composite operation IDs (`canonicalOperationIdentities`) or byte buffers is defensively cloned into detached instances.
3. **Deep Freeze:** The snapshot object and all nested records are recursively frozen with `Object.freeze()`. Callers holding prior references cannot mutate the active custody state.

---

### 12.4 Protocol A: Drainer $\to$ Terminal Archive (Execution Settlement & Attempt Fenced)

```
[Level 3 Authoritative Commit Confirmed]
                 │
                 ▼
1. Authoritative Archive Lookup (archive.getTransactionDisposition(txId) === 'COMMITTED')
                 │
                 ▼
2. Verify Commit Digest & Composite Operation Identities (assertExactImmutableIdentityMatch)
                 │
                 ▼
3. Source Quarantine Retirement (quarantineBuffer.retireCommittedBatch(identities, cap))
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
                 │
                 ▼
7. Reconcile Settlement Against Confirmed Commit:
   ├─ Fulfill: Standard success path.
   └─ Reject: Classify as post-commit execution failure; suppress error; Protocol B PROHIBITED!
                 │
                 ▼
8. Verify Exact Final Representation (Present in Archive, Absent in Buffer)
                 │
                 ▼
9. ATTEMPT-FENCED CUSTODY RELEASE: this.publishCustodyRelease(ownerToken);
   [Verifies ownerToken; simultaneously publishes inFlightRecord = null and gateState = 'CUSTODY_SETTLED']
                 │
                 ▼
10. Resolve Generation Barrier Exactly Once: activeBarrier(g).resolveOnce();
```

---

### 12.5 Protocol B: Drainer $\to$ Quarantine Buffer (12-Step Fencing Sequence)

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
3. Obtain Authoritative Terminal Outcome (Verify ArchiveArbitrationReceipt.status === 'ABORTED_FENCED')
                 │
                 ▼
4. Verify Fencing Identity & Cancellation Evidence (fencingToken, released capacity confirmed)
                 │
                 ▼
5. Await Actual Destination Promise Settlement (Fulfilled or Rejected)
                 │
                 ▼
6. Safe Idempotent Buffer Head Restoration (quarantineBuffer.unshiftDrain(op))
                 │
                 ▼
7. Verify Buffer Representation (hasOperation(...) === true & retainedCount++)
                 │
                 ▼
8. Verify Late-Commit Exclusion (Assert stale commits with txId or obsolete generation will reject)
                 │
                 ▼
9. ATTEMPT-FENCED CUSTODY RELEASE: this.publishCustodyRelease(ownerToken);
   [Verifies ownerToken; simultaneously publishes inFlightRecord = null and gateState = 'CUSTODY_SETTLED']
                 │
                 ▼
10. Resolve Generation Barrier Exactly Once: activeBarrier(g).resolveOnce();
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

## 15. Exhaustive Failure and Retry Matrix (20 Deterministic Cases)

| Case # | Scenario Description | Archive Transaction State | Execution Settlement State | Commit-vs-Cancel Winner | Fencing Generation | Buffer Representation | Drainer Custody | In-Flight Slot | Barrier Status | Custody Gate State | Restoration Permitted? | Source Retirement Required? | Late Commit Possible? | Permitted Next Action |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| **1** | Attempt A stale continuation attempts release | Any | Any | N/A | $G_0$ | Retained | In-flight (Attempt B) | **YES** | **PENDING** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | Throws `STALE_CUSTODY_RELEASE_REJECTED`; Attempt B untouched. |
| **2** | Stale release: matching op identity, wrong attempt | Any | Any | N/A | $G_0$ | Retained | In-flight (Attempt B) | **YES** | **PENDING** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | Attempt mismatch detected; throws `STALE_CUSTODY_RELEASE_REJECTED`. |
| **3** | Stale release: matching attempt, wrong custody gen | Any | Any | N/A | $G_0$ | Retained | In-flight (Gen 2) | **YES** | **PENDING** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | Gen mismatch detected; throws `STALE_CUSTODY_RELEASE_REJECTED`. |
| **4** | Exact current owner releases custody | `COMMITTED` or `FENCED` | **Settled** | Commit / Cancel | $G_0$ | Reconciled | Relinquished | **NO** | **RESOLVED (g)** | `CUSTODY_SETTLED` | If Fenced | If Committed | **NO** | Release published; barrier $g$ resolves. |
| **5** | Failed pre-publication check leaves state untouched | `ABORTED_FENCED` | Settled | Cancellation | $G_1$ | Unconfirmed | In-flight | **YES** | **PENDING (g)** | `CUSTODY_RECONCILIATION_BLOCKED` | **NO** | **NO** | **NO** | Pre-verification throw keeps slot occupied; barrier pending. |
| **6** | Duplicate release of same owner is idempotent | `CUSTODY_SETTLED` | Settled | Resolved | $G_0$ | Reconciled | Empty ($N=0$) | **NO** | **RESOLVED (g)** | `CUSTODY_SETTLED` | Already Done | Already Done | **NO** | Duplicate call returns cleanly; barrier remains resolved. |
| **7** | Earlier barrier resolves; new barrier resolves later | Sequential | Settled | Both | $G_0, G_1$ | Reconciled | Sequential | **NO** | **RESOLVED (g+1)**| `CUSTODY_SETTLED` | As appropriate | As appropriate | **NO** | Independent barrier lifecycle per generation. |
| **8** | Duplicate resolution of barrier g | Any | Any | N/A | $G_0$ | Any | Any | Any | **RESOLVED (g)** | Any | N/A | N/A | **NO** | Evaluates `#resolved`; does NOT resolve barrier $g+1$. |
| **9** | Concurrent waiters share same barrier Promise | In-flight | Pending | N/A | $G_0$ | In-flight | In-flight | **YES** | **PENDING (g)** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | All callers await `activeBarrier.promise`; unblock together. |
| **10** | New barrier blocked while previous unresolved | In-flight | Pending | N/A | $G_0$ | In-flight | In-flight | **YES** | **PENDING (g)** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | `drainNext()` fails closed; cannot bypass unresolved slot. |
| **11** | Tombstone collection reaches 1,000 entries | 1000 Tombs | Normal | Cancel | $G_{1000}$ | Normal | Normal | Normal | Normal | Normal | Normal | Normal | **NO** | Hard capacity ceiling maintained. |
| **12** | Safe eviction advances generation floor | Evicted | Normal | Cancel | Floor advanced | Reconciled | Relinquished | **NO** | **RESOLVED** | `CUSTODY_SETTLED` | Normal | Normal | **NO** | Eviction advances floor; stale commits rejected. |
| **13** | Unsafe eviction deferred by active reservation | 1000 Tombs | Active res | N/A | Floor unadvanced| Normal | Normal | Normal | Normal | Normal | Normal | Normal | **NO** | Active res protected; eviction deferred. |
| **14** | Capacity exhaustion rejects new fencing | 1000 Tombs | Active res | Blocked | Fixed | Normal | In-flight | **YES** | **PENDING** | Normal | **NO** | **NO** | **NO** | Throws `FENCING_BACKPRESSURE_CAPACITY_EXHAUSTED`. |
| **15** | Existing reservations preserved under backpressure | 1000 Tombs | Active res | Preserved | Fixed | Active res ok | Normal | Normal | Normal | Normal | Normal | Normal | **NO** | Valid reservations 100% preserved; no eviction corruption. |
| **16** | Capacity recovers after blocking reservation settles | 999 Tombs | Settled res | Res settled | Res removed | Normal | Relinquished | **NO** | **RESOLVED** | `CUSTODY_SETTLED` | Normal | Normal | **NO** | Res cleared; deferred eviction executes; backpressure relieved. |
| **17** | Caller attempts to mutate published custody record | Frozen | Any | N/A | N/A | Unchanged | Frozen snapshot | N/A | N/A | N/A | N/A | N/A | **NO** | Mutation throws or fails silently in strict mode; state safe. |
| **18** | Timeout + late settlement cannot release new custody | Attempt 2 | Attempt 1 late | Mismatch | $G_1$ | Reconciled 2 | In-flight (2) | **YES** | **PENDING (2)** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | Attempt 1 continuation rejected by owner token check. |
| **19** | Settlement barrier pending during stale release reject | Any | Any | Mismatch | Any | Retained | In-flight | **YES** | **PENDING** | `SETTLEMENT_PENDING` | **NO** | **NO** | **NO** | Stale throw preserves barrier pending state. |
| **20** | Barrier resolves only after exact-owner release | Reconciled | Settled | Match | Any | Reconciled | Relinquished | **NO** | **RESOLVED** | `CUSTODY_SETTLED` | Verified | Verified | **NO** | Verified release resolves barrier exactly once. |

---

## 16. Mandatory Barrier and Drain Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{NO\_IN\_FLIGHT\_OWNER}(g)$$
2. **Attempt-Fenced Custody Release Invariant:**
   $$\mathbf{STALE\_ATTEMPT} \implies \mathbf{CANNOT\_RELEASE\_NEWER\_CUSTODY}$$
3. **Execution Settlement Precedence:** The settlement barrier **CANNOT** resolve while the destination Promise remains pending.
4. **Owner-Token Verification Precedence:** Custody release **CANNOT** execute unless `ownerToken` matches the exact current owner across all 5 dimensions.
5. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
6. **Active Reservation Floor Protection:** `fencingGenerationFloor` **CANNOT** advance past $\min_{r \in \text{activeReservations}}(r.\text{generation})$.
7. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this._custodyState.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
8. **Watchdog Immunity:** A watchdog timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) **CANNOT** release drainer custody or cancel the destination Promise.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. } \mathbf{publishCustodyRelease(token)} \longrightarrow \text{4. } \mathbf{activeBarrier(g).resolveOnce()}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R8 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`) and latched `TIMED_OUT`. Its continuation is completely fenced and cannot mutate roots or stage batches.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's continuation attempts `publishCustodyRelease(token1)`, which is rejected because Attempt 2 owns the coordinator. The coordinator reconciles custody, executes `publishCustodyRelease(token2)`, and resolves the barrier. Attempt 2 unblocks with clean custody and proceeds to Step 7 with exclusive authority.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

Sub-Milestone M5.1 test suites must verify the following twenty deterministic test sequences:

- **Test R18-R8-T1 (Stale Attempt Delayed Release Cannot Clear Newer In-Flight Record):** Attempt 1 times out; Attempt 2 dequeues new op; Attempt 1 continuation invokes release; assert throws `STALE_CUSTODY_RELEASE_REJECTED`; assert Attempt 2 slot and barrier untouched.
- **Test R18-R8-T2 (Stale Release With Matching Op Identity But Wrong Attempt Rejected):** Attempt release with current op ID but stale attempt ID; assert throws `STALE_CUSTODY_RELEASE_REJECTED`.
- **Test R18-R8-T3 (Stale Release With Matching Attempt But Wrong Custody Gen Rejected):** Attempt release with matching attempt ID but stale custody generation; assert throws `STALE_CUSTODY_RELEASE_REJECTED`.
- **Test R18-R8-T4 (Exact Current Owner Successfully Releases Custody):** Invoke release with exact matching `ownerToken`; assert state transitions to settled; assert barrier resolves.
- **Test R18-R8-T5 (Failed Pre-Publication Check Leaves Custody State Untouched):** Mock verification throw before publication; assert `_custodyState` remains occupied fail-closed.
- **Test R18-R8-T6 (Duplicate Release of Same Owner Is Idempotent):** Invoke `publishCustodyRelease` twice with identical token; assert second call returns cleanly with zero secondary mutations.
- **Test R18-R8-T7 (Sequential Generation Barriers Resolve Independently):** Drain and settle op 1; assert barrier 1 resolves; drain and settle op 2; assert barrier 2 resolves cleanly.
- **Test R18-R8-T8 (Duplicate Resolution of Barrier g Does Not Resolve Barrier g+1):** Call `barrier(g).resolveOnce()` twice; assert barrier $g+1$ remains pending until its own exact-owner release.
- **Test R18-R8-T9 (Concurrent Waiters Share Same Barrier Promise):** Attach multiple callers to `drainSettledBarrier()`; assert all await the identical underlying Promise.
- **Test R18-R8-T10 (New Barrier Cannot Bypass Unresolved Previous Custody):** Assert `drainNext()` fails closed while previous in-flight custody is occupied.
- **Test R18-R8-T11 (Tombstone Capacity Reaches Hard 1,000-Entry Bound):** Populate `#fencedTransactions` to 1,000; assert size is exactly 1,000.
- **Test R18-R8-T12 (Safe Eviction Advances Floor Monotonically):** Evict tombstone when no active reservations conflict; assert floor advances to evicted tombstone's generation.
- **Test R18-R8-T13 (Unsafe Eviction Deferred When Active Reservation Conflicts):** Attempt eviction when oldest tombstone generation exceeds active reservation; assert eviction is deferred.
- **Test R18-R8-T14 (Capacity Exhaustion Enforces Backpressure Without Exceeding Bound):** Attempt 1,001st fence when eviction deferred; assert throws `FENCING_BACKPRESSURE_CAPACITY_EXHAUSTED`; assert size remains $\le 1000$.
- **Test R18-R8-T15 (Existing Reservations Preserved Under Backpressure):** Assert active reservations remain valid and uncorrupted during capacity backpressure.
- **Test R18-R8-T16 (Capacity Recovers After Blocking Reservation Settles):** Settle blocking reservation; assert deferred eviction executes; assert backpressure is relieved.
- **Test R18-R8-T17 (Caller Cannot Mutate Published Record Via Old Reference):** Mutate external array reference; assert internal published custody snapshot is completely unaffected.
- **Test R18-R8-T18 (Timeout Followed by Late Settlement Cannot Release Newer Custody):** Mock timeout of Attempt 1; late Promise settles; assert Attempt 1 cannot release Attempt 2's custody.
- **Test R18-R8-T19 (Settlement Barrier Remains Pending During Stale Rejection):** Assert stale release rejection does not resolve the pending barrier.
- **Test R18-R8-T20 (Barrier Resolves Only After Exact-Owner Publication):** Verify barrier resolver is invoked if and only if `publishCustodyRelease` succeeds with exact owner match.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R8):
================================================================================
1. [ATTEMPT-FENCED CUSTODY RELEASE]: Does the Engineering Authority approve
   requiring CustodyOwnershipToken verification in publishCustodyRelease, ensuring
   stale continuations cannot release custody owned by newer attempts?

2. [GENERATION-SCOPED BARRIER LIFECYCLE]: Does the Engineering Authority approve
   scoping SettlementBarrier per in-flight custody generation, ensuring each barrier
   resolves at most once without impeding subsequent barriers?

3. [STRICT TOMBSTONE BACKPRESSURE]: Does the Engineering Authority approve the
   fail-closed backpressure policy, enforcing the hard 1,000-entry ceiling when
   safe eviction is blocked by active reservations?

4. [DEEP IMMUTABILITY & ENCAPSULATION]: Does the Engineering Authority approve
   deep runtime immutability and defensive cloning for all published custody
   records and nested identity arrays?

5. [POST-COMMIT FAILURE & RECONCILIATION]: Does the Engineering Authority confirm
   that post-commit Promise rejections are non-fatal to the archive commit, strictly
   prohibiting Protocol B restoration while requiring custody reconciliation?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R8 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R8 proposal achieves comprehensive, bulletproof recovery closure: it enforces attempt-fenced custody release (`STALE_ATTEMPT => CANNOT_RELEASE_NEWER_CUSTODY`), defines generation-scoped settlement-barrier lifecycles with once-only resolution, formalizes strict 1,000-entry tombstone backpressure when eviction is blocked by active reservations, and guarantees deep runtime immutability of published custody records.

### Mandatory Implementation Authorization Statement
**"M5.0-R18-R8 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."**
