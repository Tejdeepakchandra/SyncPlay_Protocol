# Phase 7.5 M5.0-R18-R11 — Custody Release and Barrier Resolution Atomicity Closure
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Baseline Commit:** `ce4f9efd8aff6b8f5ac928f3e0079620f1c95a5c` (M5.0-R18-R10)
**Direct Parent Commit:** `ce4f9efd8aff6b8f5ac928f3e0079620f1c95a5c`
**Authorization Scope:** M5.0-R18-R11 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package exports, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1 remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R11 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R10, we established an authoritative evidence model that eliminated caller-asserted Boolean claims and replaced them with component-issued, five-field identity-bound receipts. However, the subsequent audit identified six precision gaps in the coordination protocol between custody release and barrier resolution:

1. **The Custody-Release / Barrier-Resolution Gap:** R18-R10 specified that drainer custody is atomically released and a `DrainerCustodyReleaseReceipt` is produced before the final barrier proof is resolved. If an unhandled exception or coordinator failure occurs after release publication but before barrier resolution, the system is left with no in-flight owner while its settlement barrier remains unresolved.
2. **Impossible General Rollback Claims:** R18-R10 described a six-step ordering with generic "rollback on intermediate throw". This is physically impossible because preceding operations (e.g. archive commit publication, source quarantine item splicing, or published custody release) are irreversible. Generic rollback claims must be replaced with explicit, phase-specific failure semantics.
3. **Unspecified Release-to-Resolution Protocol:** The exact sequence and synchronization relationship between pre-validation, release publication, proof acceptance, barrier resolution, and barrier retirement lacked a unified linearization boundary and recovery specification.
4. **Unqualified Receipt Security Boundary:** Describing component-issued receipts as "unforgeable" overstates the security boundary. In a single JavaScript realm, receipts cannot provide Byzantine fault tolerance or cryptographic tamper resistance against malicious code. Their protection is based on package-internal symbol branding, private construction, defensive freezing, and cooperative non-Byzantine isolation.
5. **Undifferentiated Abort Custody Outcomes:** R18-R10 permitted restoration or authorized transfer after `ABORTED_FENCED`, but forced `sourceBufferContainsOperation: true` on both, which violates the semantics of destination transfer.
6. **Unstated Tombstone FIFO / Generation Ordering Invariant:** R18-R10 permitted FIFO eviction when the oldest tombstone's generation is strictly below the minimum active reservation generation, without documenting whether insertion order guarantees generation monotonicity or how the archive handles out-of-order tombstones and empty active reservation sets.

### Core Architectural Guarantees Established in R18-R11:

- **Two-Phase Release-to-Resolution Protocol:** Distinguishes reversible pre-validation from an atomic synchronous publication turn. If executed across decoupled components, introduces the explicit intermediate state `CUSTODY_RELEASED_BARRIER_PENDING` with generation-scoped settlement obligations to ensure no obligation is orphaned.
- **Phase-Specific Failure Semantics:** Strictly distinguishes reversible preparation, irrevocable commit, irreversible source retirement, published custody release, post-publication recovery obligation, and terminal settlement. Generic rollback claims are eliminated.
- **Documented Receipt Security Boundary:** Explicitly scopes receipt integrity to the package boundary within a cooperative, crash-fault-tolerant, non-Byzantine trust model using module-private symbol branding and defensive object freezing.
- **Genuinely Discriminated Abort Custody Invariants:** Differentiates `RESTORED_TO_SOURCE` (exclusive source ownership) from `TRANSFERRED_TO_DESTINATION` (exclusive authorized destination ownership), guaranteeing zero ownerless or multiply-owned operations.
- **Documented Tombstone Generation Ordering Invariant & Floor Advancement:** Proves monotonic fencing generation assignment, provides explicit minimal generation selection if out-of-order tombstones exist, establishes the safe eviction floor when active reservations are empty, and enforces the strict 1,000-entry memory bound under fail-closed backpressure.
- **Exhaustive Deterministic Test Specifications:** Codifies twenty deterministic future test scenarios for Sub-Milestone M5.1.

---

## 2. Separation of the Three Independent Conditions

A correct recovery arbitration architecture must strictly distinguish three independent system conditions:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. Administrative Attempt Ownership (Recovery Coordinator)                 │
│    - Governed by monotonic recoveryAttemptId (e.g. Attempt 1 vs. Attempt 2).│
│    - Fenced via Attempt Fencing Tokens and watchdog timers.                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. Drainer Operation Custody (QuarantineDrainer)                            │
│    - Governed by drainerOwnershipGeneration and in-flight operation record.  │
│    - Holds exclusive execution rights over op:stream-1:seq-42.              │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. Settlement Barrier State (Generation-Scoped Barrier)                     │
│    - Governed by custodyGeneration and AuthoritativeSettlementEvidence.     │
│    - Resolves if and only if: Promise settled AND Custody reconciled.       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Generation-Scoped Universal Settlement-Barrier Invariant:

$$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{ALL\_RECEIPTS\_MATCH}(g)$$

Under no circumstances may a settlement barrier for generation $g$ resolve while the asynchronous execution Promise remains pending, the archive disposition remains unresolved, source/destination custody remains unreconciled, or the drainer release receipt is absent.

---

## 3. Three Independent Ownership Concepts

To eliminate conceptual conflation, the system defines three distinct ownership lifecycles:

1. **Quarantine-Buffer Ownership:**
   - The operation $op$ resides in `QuarantineBuffer` with state `QUEUED` or `BUFFERED`.
   - The buffer owns the authoritative queued state of the operation.
2. **Drainer In-Flight Custody:**
   - When dequeued via `drainNext()`, the drainer acquires exclusive execution custody.
   - Represented by `this._custodyState.inFlightRecord`.
   - While in-flight, the operation cannot be redrained ($N_{\text{in-flight}} \le 1$).
3. **Terminal Archive Representation:**
   - Once committed via `commitRetirementBatch()`, the transaction is immutably recorded in the archive's `#committedTransactions` map.
   - The archive owns the terminal committed state.

Ownership transfer is strictly single-owner: at any point in time, exactly one component owns exclusive authority over the operation.

---

## 4. Separation of the Three Evidence Levels

Evidence of transaction and custody status is categorized into three strictly ordered levels:

- **Level 1: Local / Administrative Invalidation Receipts:**
  - Receipts such as `ATTEMPT_INVALIDATED` or `WATCHDOG_TIMEOUT`.
  - Proves only that an administrative recovery attempt was superseded.
  - **Does NOT prove** that the destination Promise settled, that the archive transaction committed or aborted, or that custody was released.
- **Level 2: Transport / Execution Signals:**
  - Promise resolution or rejection from the destination peer.
  - A rejected Promise **does NOT prove** that the destination produced no side effect, nor does it prove that the archive transaction aborted.
- **Level 3: Authoritative Subsystem Receipts:**
  - Component-issued, frozen receipts produced by the authoritative subsystems:
    - `ExecutionSettlementReceipt` (from the asynchronous execution owner).
    - `ArchiveDispositionReceipt` (from `TerminalQuarantineArchive`).
    - `CustodyReconciliationReceipt` (from the custody reconciler: retirement, restoration, or transfer).
    - `DrainerCustodyReleaseReceipt` (from `QuarantineDrainer`).
  - Only Level 3 evidence can satisfy the settlement barrier predicate.

---

## 5. Transaction Absence vs. Terminal Non-Commit & Disposition Consistency

A point-in-time absence of a transaction ID in `#committedTransactions` does not prove terminal non-commit; an in-flight asynchronous operation could commit a microtask later.

### Archive Disposition Consistency Invariant:

For any transaction ID $txId$, the archive must report exactly one immutable terminal disposition:

$$\mathbf{Disposition}(txId) \in \{\mathbf{COMMITTED},\; \mathbf{ABORTED\_FENCED}\}$$

Once a transaction enters `COMMITTED`, it can never transition to `ABORTED_FENCED`. Once entered into `#fencedTransactions` as `ABORTED_FENCED`, any subsequent attempt to commit that transaction is unconditionally rejected with `TRANSACTION_ALREADY_FENCED`.

---

## 6. Commit-Path Fencing & Exact Immutable Retry Identity

### 6.1 Exact Immutable Identity Equality for Idempotent Retries

To safely support idempotent commit retries without creating race conditions, a cached commit receipt is returned **if and only if** the retry request matches the original committed transaction across all four immutable identity fields:

$$\mathbf{EqualIdentity}(T_1, T_2) \iff \begin{cases}
T_1.\text{transactionId} === T_2.\text{transactionId} \\
\land\; T_1.\text{stagedBatchDigest} === T_2.\text{stagedBatchDigest} \\
\land\; T_1.\text{compositeOperationIdentities} = T_2.\text{compositeOperationIdentities} \\
\land\; T_1.\text{archiveGeneration} === T_2.\text{archiveGeneration}
\end{cases}$$

If the transaction ID matches an existing committed transaction but any other identity field differs, the commit is rejected with:
$$\mathbf{TRANSACTION\_IDENTITY\_MUTATION\_PROHIBITED}$$

### 6.2 The Synchronous 9-Step Commit Protocol

```
[commitRetirementBatch(candidateRoot, txId, reservationId, capability)]
                  │
                  ▼
1. Validate Caller Capability & Archive Open State
                  │
                  ▼
2. Check Existing Committed Transactions (#committedTransactions.has(txId))
   ├─ Match Identity? ──► Return Cached Receipt (Idempotent Success)
   └─ Mutated Identity? ─► Throw TRANSACTION_IDENTITY_MUTATION_PROHIBITED
                  │
                  ▼
3. Check Fenced Transactions (#fencedTransactions.has(txId))
   └─ Present? ──────────► Throw TRANSACTION_ALREADY_FENCED (Late-Commit Blocked)
                  │
                  ▼
4. Check Generation Floor (candidateRoot.archiveGeneration <= fencingGenerationFloor)
   └─ Stale Generation? ─► Throw TRANSACTION_GENERATION_FENCED
                  │
                  ▼
5. Validate Active Capacity Reservation (reservationId must match txId and generation)
   └─ Invalid? ──────────► Throw RESERVATION_INVALID_OR_EXPIRED
                  │
                  ▼
6. Pre-Allocate Candidate Archive Root & Verify Accounting Limits
   └─ Throws Error? ─────► Abort Pre-Allocation; State 100% Intact
                  │
                  ▼
7. Consume Capacity Reservation (Remove from #activeReservations)
                  │
                  ▼
8. ATOMIC SYNCHRONOUS ROOT REPLACEMENT:
   this.#core = candidateRoot;
   this.#committedTransactions.set(txId, commitRecord);
                  │
                  ▼
9. Return Authoritative Immutable Commit Receipt
```

---

## 7. Atomic Commit-versus-Cancellation Arbitration

Cancellation and commit arbitration are unified inside `TerminalQuarantineArchive.arbitrateOrFenceTransaction(...)`. This method executes synchronously within a single turn:

1. If $txId$ is in `#committedTransactions`: returns `COMMITTED` with the immutable commit receipt.
2. If $txId$ is already in `#fencedTransactions`: returns `ABORTED_FENCED` with the existing tombstone.
3. If $txId$ is neither committed nor fenced:
   - Synchronously releases capacity reservation $reservationId$ from `#activeReservations`.
   - Synchronously creates a tombstone in `#fencedTransactions` with generation `this.#core.archiveGeneration`.
   - Returns `ABORTED_FENCED`.

Late-commit exclusion is absolute: any subsequent `commitRetirementBatch` call for $txId$ will hit Step 3 and throw `TRANSACTION_ALREADY_FENCED`.

---

## 8. Generation-Floor Scope, Monotonic Ordering & Strict Tombstone Backpressure Recovery

### 8.1 Scope & Monotonicity of Generations

- **Scope:** `fencingGeneration` is a strictly monotonic sequence counter local to the `TerminalQuarantineArchive` instance.
- **Monotonic Assignment Invariant:** Every time a transaction is fenced or a reservation is created, the archive assigns:
  $$\text{fencingGeneration} = \text{this.\#core.archiveGeneration}$$
  Because `this.#core.archiveGeneration` is strictly non-decreasing, tombstones are generated with monotonically non-decreasing generation numbers over physical time.

### 8.2 Safe Tombstone Eviction & Generation Ordering Invariants

Tombstones are maintained in `this.#fencedTransactions = new Map<string, FencedTransactionTombstone>()`. Under standard insertion, JavaScript `Map` preserves insertion order, which reflects non-decreasing generation order.

To eliminate reliance on implicit assumptions:

1. **Documented Ordering Invariant:**
   The archive guarantees that insertion order into `#fencedTransactions` matches non-decreasing generation order:
   $$i < j \implies \text{tombstone}_i.\text{generation} \le \text{tombstone}_j.\text{generation}$$
2. **Explicit Minimal Generation Selection:**
   Eviction must never blindly delete the FIFO head without generation verification. When eviction is evaluated, the archive inspects the candidate tombstone:
   $$t_{\text{candidate}} = \arg\min_{t \in \text{\#fencedTransactions}}(t.\text{generation})$$
   Under the insertion invariant, $t_{\text{candidate}}$ is the FIFO head (`this.#fencedTransactions.values().next().value`). If out-of-order tombstones could exist, the archive scans or indexes tombstones to locate the true minimum generation.
3. **Safe Eviction Condition:**
   A tombstone $t$ can be evicted if and only if its generation is strictly less than the active reservation generation floor:
   $$t.\text{generation} < \mathbf{activeFloor}$$
4. **Advancement of Fencing Generation Floor:**
   Upon safe eviction of $t$, the archive synchronously advances the floor:
   $$\text{candidateRoot.fencingGenerationFloor} = \max(\text{currentRoot.fencingGenerationFloor},\; t.\text{generation})$$

### 8.3 Protection of Active Reservations, No-Active-Reservations Rule & Strict Backpressure Recovery

1. **Calculating the Active Generation Floor:**
   - **When `#activeReservations.size > 0`:**
     $$\mathbf{activeFloor} = \min_{r \in \text{\#activeReservations}}(r.\text{generation})$$
   - **When `#activeReservations.size === 0` (No Active Reservations):**
     $$\mathbf{activeFloor} = \text{this.\#core.archiveGeneration}$$
     When no reservations are active, any tombstone with generation strictly less than the current archive generation may be safely evicted. If all tombstones have generation equal to `currentArchiveGeneration`, eviction of the oldest tombstone (by insertion order) advances `fencingGenerationFloor` to that generation, ensuring safe garbage collection without compromising active work.
2. **Backpressure Trigger & Hard Capacity Ceiling:**
   `MAX_FENCED_TRANSACTIONS = 1000` is an absolute memory ceiling.
   If `#fencedTransactions.size === 1000` and the oldest eligible tombstone cannot be safely evicted ($t.\text{generation} \ge \mathbf{activeFloor}$):
   - Eviction is **strictly prohibited**.
   - The archive enters **Fail-Closed Backpressure**.
   - Any new arbitration request that would require adding a 1,001st tombstone is rejected with:
     $$\mathbf{FENCING\_BACKPRESSURE\_CAPACITY\_EXHAUSTED}$$
   - Any new capacity reservation request (`reserveCapacity`) is rejected with:
     $$\mathbf{CAPACITY\_RESERVATION\_BACKPRESSURE\_BLOCKED}$$
   - All 1,000 existing tombstones and all active reservations are **100% preserved**.
   - Zero unbounded queues; callers must handle rejection and retry explicitly.
3. **Synchronous Operational Recovery:**
   When an active reservation completes or cancels, it is removed from `#activeReservations`.
   In that **same synchronous microtask turn**, the archive reevaluates:
   $$\mathbf{newActiveFloor} = \begin{cases}
   \min_{r \in \text{\#activeReservations}}(r.\text{generation}) & \text{if } \text{\#activeReservations.size} > 0 \\
   \text{this.\#core.archiveGeneration} & \text{if } \text{\#activeReservations.size} === 0
   \end{cases}$$
   If the oldest tombstone satisfies $t_{\text{oldest}}.\text{generation} < \mathbf{newActiveFloor}$, it is evicted immediately, advancing `fencingGenerationFloor` and reopening capacity for incoming requests.

---

## 9. Process Failure Model

The recovery architecture operates under a single-node, process-volatile failure model:

- Node crash / process death results in loss of volatile memory (in-memory buffers, drainer records, uncommitted transactions).
- Durability across process crashes is governed by persistent append-logs outside M5.0.
- Single-threaded event loop execution guarantees that synchronous mutations within a turn cannot be interleaved by other JavaScript microtasks.
- Asynchronous Promise settlement, timer events, and transport I/O interleave between execution turns.

---

## 10. Disposition & Lost-Response Semantics

When an asynchronous network request or destination Promise fails or drops:

1. The client recovery coordinator cannot assume the remote peer produced no side effect.
2. The coordinator queries the authoritative archive.
3. If the archive committed the transaction, the outcome is `COMMITTED` (Protocol A).
4. If the archive fenced the transaction, the outcome is `ABORTED_FENCED` (Protocol B).
5. If the archive query is inconclusive, custody is retained fail-closed.

---

## 11. Destination Outcome Classification & Post-Commit Promise Rejection

### A. Confirmed Committed (Level 3 Proof)
- **Condition:** Authoritative lookup confirms `archive.getTransactionDisposition(txId) === 'COMMITTED'`.
- **Required Action:** Execute Protocol A: retire source operations from `QuarantineBuffer`, verify buffer disjointness, await destination Promise settlement, publish attempt-fenced custody release, and resolve the settlement barrier upon verified authoritative evidence.

### B. Post-Commit Promise Rejection (Execution Failure)
- **Condition:** Archive transaction is confirmed `COMMITTED`, but the original destination Promise settles with a rejection (`Promise.reject(err)`).
- **Classification:** **Post-commit execution failure** (e.g. transport disconnect or post-commit notification error).
- **Invariants:**
  1. The archive commit remains **100% authoritative and irrevocable**. Zero archive rollback!
  2. Protocol B restoration or destination transfer is **strictly prohibited** (preventing double replay).
  3. Execution is marked settled (`ExecutionSettlementReceipt.settlementType = 'REJECTED'`).
  4. Source quarantine retirement must be verified (`sourceBufferDisjoint === true`).
  5. Assembled evidence reflects commit with execution failure. The barrier resolves with this evidence.

### C. Aborted Fenced: Restored to Source vs. Transferred to Destination (Level 3 Proof)
- **Condition:** Atomic arbitration confirms `archive.arbitrateOrFenceTransaction(...)` returned `ABORTED_FENCED`.
- **Required Action:** Execute Protocol B: await destination Promise settlement, reconcile custody via restoration or transfer, verify exclusive ownership, publish attempt-fenced custody release, and resolve the settlement barrier.
  - **Variant 1 (`RESTORED_TO_SOURCE`):** Operation restored to source quarantine buffer head; source retains exclusive custody.
  - **Variant 2 (`TRANSFERRED_TO_DESTINATION`):** Operation transferred to an authorized named destination; destination accepts exclusive custody.

### D. Inconclusive or Commit Pending (Level 3 Ambiguity)
- **Condition:** Arbitration throws, times out, or reports `NOT_FOUND_AT_QUERY`.
- **Required Action:** Drainer in-flight custody is **strictly retained** ($N_{\text{in-flight}} = 1$). Gate transitions to `CUSTODY_RECONCILIATION_BLOCKED`. Settlement barrier remains **PENDING**.

---

## 12. Complete Custody Authorization, Receipt Boundary & Release-to-Resolution Protocol

### 12.1 Five-Field Ownership Comparison & Equivalence Predicate

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

---

### 12.2 Authoritative Evidence Model & Qualified Receipt Security Boundary

#### The Documented Package Security Boundary:
1. **Module-Internal Private Branding:**
   Receipts are constructed using private symbols not exported from `@syncplay/protocol`:
   ```typescript
   const EXECUTION_RECEIPT_BRAND = Symbol('ExecutionSettlementReceiptBrand');
   const ARCHIVE_RECEIPT_BRAND = Symbol('ArchiveDispositionReceiptBrand');
   const RECONCILIATION_RECEIPT_BRAND = Symbol('CustodyReconciliationReceiptBrand');
   const DRAINER_RELEASE_BRAND = Symbol('DrainerCustodyReleaseBrand');
   ```
2. **Private Capability Custody:**
   Receipt factory functions are package-private and accessible only through internal dependency injection or subsystem instances.
3. **Defensive Object Freezing:**
   Every receipt is deeply frozen via `Object.freeze` to prevent property mutation by cooperative callers.

#### Explicit Non-Claims & Trust Model Qualification:
- **No In-Realm Malicious Protection:** We do **not** claim protection against arbitrary malicious code executing in the same JavaScript realm (which could use prototype manipulation, reflection, or V8 heap tampering).
- **No Byzantine Fault Tolerance:** We do **not** claim cryptographic authenticity, digital signatures, HMACs, or Byzantine tolerance.
- **Cooperative Trust Model:** The architecture operates within a cooperative, crash-fault-tolerant TypeScript distributed system where private branding and strong typing defend against accidental forgery, concurrency races, stale continuations, and logic errors.

---

### 12.3 Complete Five-Field Custody Identity & Receipt Binding

Every authoritative receipt must bind to the complete 5-field owner identity:

```typescript
export interface ExecutionSettlementReceipt {
  readonly [EXECUTION_RECEIPT_BRAND]: true;
  readonly identity: CustodyOwnershipToken;
  readonly settlementType: 'FULFILLED' | 'REJECTED';
  readonly settledAtTimestamp: number;
  readonly errorDetails?: string;
}

export interface ArchiveDispositionReceipt {
  readonly [ARCHIVE_RECEIPT_BRAND]: true;
  readonly identity: CustodyOwnershipToken;
  readonly disposition: 'COMMITTED' | 'ABORTED_FENCED';
  readonly confirmedGeneration: number;
  readonly archivedBatchDigest: string;
  readonly arbitratedAtTimestamp: number;
}
```

Canonical identity matching is enforced on every receipt before acceptance:
```typescript
export function isMatchingReceiptIdentity(
  receipt: { readonly identity: CustodyOwnershipToken },
  expected: CustodyOwnershipToken,
): boolean {
  return isEquivalentOwnerToken(receipt.identity, expected);
}
```

---

### 12.4 Discriminated Outcome-Specific Custody Reconciliation

To prevent conflation between commit, abort restoration, and abort transfer, `CustodyReconciliationReceipt` is an explicitly discriminated union:

```typescript
// For COMMITTED transactions:
export interface SourceQuarantineRetirementReceipt {
  readonly [RECONCILIATION_RECEIPT_BRAND]: true;
  readonly reconciliationType: 'COMMITTED_RETIRED';
  readonly archiveDisposition: 'COMMITTED';
  readonly identity: CustodyOwnershipToken;
  readonly sourceBufferDisjoint: true;
  readonly retiredOperationCount: number;
  readonly retiredTimestamp: number;
}

// For ABORTED_FENCED transactions restored to source:
export interface RestoredToSourceReceipt {
  readonly [RECONCILIATION_RECEIPT_BRAND]: true;
  readonly reconciliationType: 'RESTORED_TO_SOURCE';
  readonly archiveDisposition: 'ABORTED_FENCED';
  readonly identity: CustodyOwnershipToken;
  readonly sourceBufferContainsOperation: true;
  readonly destinationExclusiveOwnershipReleased: true;
  readonly sourceRetainsExclusiveOwnership: true;
  readonly accountedFor: true;
  readonly restoredTimestamp: number;
}

// For ABORTED_FENCED transactions transferred to an authorized destination:
export interface TransferredToDestinationReceipt {
  readonly [RECONCILIATION_RECEIPT_BRAND]: true;
  readonly reconciliationType: 'TRANSFERRED_TO_DESTINATION';
  readonly archiveDisposition: 'ABORTED_FENCED';
  readonly identity: CustodyOwnershipToken;
  readonly destinationIdentifier: string;
  readonly sourceRetainsOwnership: false;
  readonly destinationExclusiveOwnershipAccepted: true;
  readonly accountedFor: true;
  readonly transferredTimestamp: number;
}

export type AbortedFencedReconciliationReceipt =
  | RestoredToSourceReceipt
  | TransferredToDestinationReceipt;

export type CustodyReconciliationReceipt =
  | SourceQuarantineRetirementReceipt
  | AbortedFencedReconciliationReceipt;
```

#### Exclusive Ownership Invariants:
1. Under `COMMITTED_RETIRED`, the operation is retired from the source buffer and committed to the terminal archive.
2. Under `RESTORED_TO_SOURCE`, the operation is exclusively owned by the source quarantine buffer (`sourceRetainsExclusiveOwnership === true`).
3. Under `TRANSFERRED_TO_DESTINATION`, the operation is exclusively owned by the named destination (`destinationExclusiveOwnershipAccepted === true`).
4. At no time is any operation left ownerless or multiply owned.

---

### 12.5 Explicit Drainer Custody Release Evidence

The settlement barrier cannot resolve by inferring release from an empty in-flight slot. An explicit `DrainerCustodyReleaseReceipt` is required:

```typescript
export interface DrainerCustodyReleaseReceipt {
  readonly [DRAINER_RELEASE_BRAND]: true;
  readonly identity: CustodyOwnershipToken;
  readonly releasedCustodyGeneration: number;
  readonly previousInFlightDurationMs: number;
  readonly releasedAtTimestamp: number;
}
```

A `DrainerCustodyReleaseReceipt` is produced **if and only if**:
1. Published custody state is inspected.
2. All five fields of `CustodyOwnershipToken` match the active in-flight record.
3. Pre-release validations pass.
4. The drainer atomically publishes `inFlightRecord = null` and transitions the published custody state.

---

### 12.6 The Final Authoritative Barrier Proof Predicate

```typescript
export interface AuthoritativeSettlementEvidence {
  readonly expectedOwner: CustodyOwnershipToken;
  readonly executionSettlement: ExecutionSettlementReceipt;
  readonly archiveDisposition: ArchiveDispositionReceipt;
  readonly custodyReconciliation: CustodyReconciliationReceipt;
  readonly drainerRelease: DrainerCustodyReleaseReceipt;
}

export function assertAuthoritativeSettlementEvidence(
  evidence: AuthoritativeSettlementEvidence,
  expectedOwner: CustodyOwnershipToken,
): void {
  // 1. Verify brands
  if (!evidence.executionSettlement?.[EXECUTION_RECEIPT_BRAND]) {
    throw new Error('INVALID_EXECUTION_RECEIPT_BRAND');
  }
  if (!evidence.archiveDisposition?.[ARCHIVE_RECEIPT_BRAND]) {
    throw new Error('INVALID_ARCHIVE_RECEIPT_BRAND');
  }
  if (!evidence.custodyReconciliation?.[RECONCILIATION_RECEIPT_BRAND]) {
    throw new Error('INVALID_CUSTODY_RECONCILIATION_RECEIPT_BRAND');
  }
  if (!evidence.drainerRelease?.[DRAINER_RELEASE_BRAND]) {
    throw new Error('INVALID_DRAINER_RELEASE_RECEIPT_BRAND');
  }

  // 2. Verify complete 5-field identity across all receipts
  if (!isMatchingReceiptIdentity(evidence.executionSettlement, expectedOwner)) {
    throw new Error('EXECUTION_RECEIPT_IDENTITY_MISMATCH');
  }
  if (!isMatchingReceiptIdentity(evidence.archiveDisposition, expectedOwner)) {
    throw new Error('ARCHIVE_RECEIPT_IDENTITY_MISMATCH');
  }
  if (!isMatchingReceiptIdentity(evidence.custodyReconciliation, expectedOwner)) {
    throw new Error('CUSTODY_RECONCILIATION_IDENTITY_MISMATCH');
  }
  if (!isMatchingReceiptIdentity(evidence.drainerRelease, expectedOwner)) {
    throw new Error('DRAINER_RELEASE_IDENTITY_MISMATCH');
  }

  // 3. Verify disposition consistency
  if (evidence.archiveDisposition.disposition === 'COMMITTED') {
    if (evidence.custodyReconciliation.reconciliationType !== 'COMMITTED_RETIRED') {
      throw new Error('COMMITTED_DISPOSITION_REQUIRES_RETIREMENT_RECONCILIATION');
    }
  } else if (evidence.archiveDisposition.disposition === 'ABORTED_FENCED') {
    if (
      evidence.custodyReconciliation.reconciliationType !== 'RESTORED_TO_SOURCE' &&
      evidence.custodyReconciliation.reconciliationType !== 'TRANSFERRED_TO_DESTINATION'
    ) {
      throw new Error('ABORTED_DISPOSITION_REQUIRES_RESTORATION_OR_TRANSFER');
    }
  } else {
    throw new Error('UNKNOWN_ARCHIVE_DISPOSITION');
  }
}
```

---

### 12.7 Closed Generation-Scoped Barrier Lifecycle

Every barrier instance is explicitly tied to a single generation $g$:

```typescript
export interface GenerationSettlementBarrier {
  readonly generation: number;
  readonly isSettled: boolean;
  readonly resolvedEvidence: AuthoritativeSettlementEvidence | null;
  readonly promise: Promise<AuthoritativeSettlementEvidence>;
  resolveWithEvidence(evidence: AuthoritativeSettlementEvidence): void;
}
```

#### Lifecycle State Transitions:
1. `ACTIVE`: Created during `drainNext()`. `isSettled = false`.
2. `RESOLVED`: Linearization point reached. `isSettled = true`, `resolvedEvidence = evidence`.
3. `TERMINAL_DEGRADED`: Watchdog timed out or catastrophic failure. Latched permanently.
4. `RETIRED`: Cleaned up from coordinator's active barrier map. Resolved evidence is archived.

---

### 12.8 The Precise Release-to-Resolution Protocol & Intermediate Recovery State

To eliminate the gap where drainer custody is released but the barrier remains unresolved, the architecture defines a strict two-phase protocol with an explicit intermediate recovery state.

#### The 9-Step Protocol:

1. **Step 1 (Execution Settlement Validation):** Validate `ExecutionSettlementReceipt` brand and 5-field identity.
2. **Step 2 (Archive Disposition Validation):** Validate `ArchiveDispositionReceipt` brand and 5-field identity.
3. **Step 3 (Custody Reconciliation Validation):** Validate `CustodyReconciliationReceipt` brand, 5-field identity, and outcome-specific fields.
4. **Step 4 (Active Owner Verification):** Validate active `CustodyOwnershipToken` against in-flight record.
5. **Step 5 (Reversible Pre-Allocation & Proof Preparation):** Pre-allocate `DrainerCustodyReleaseReceipt` and assemble `AuthoritativeSettlementEvidence`. If memory allocation throws, state is 100% untouched.
6. **Step 6 (Atomic Release Publication):** In a non-yielding synchronous turn, publish `_inFlightRecord = null` and issue the release receipt.
7. **Step 7 (Final Proof Acceptance):** Execute `assertAuthoritativeSettlementEvidence(evidence, expectedOwner)`.
8. **Step 8 (Barrier Resolution Linearization Point):** Synchronously invoke `barrier.resolveWithEvidence(evidence)`.
9. **Step 9 (Barrier Retirement):** Remove barrier from active map and record settled evidence in generation audit history.

#### Unified Synchronous Linearization Boundary:
In standard coordinator operation, Steps 5 through 8 execute within a **single synchronous execution turn** (zero microtask yields, zero await):
```typescript
public commitSettlementAndRelease(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
): AuthoritativeSettlementEvidence {
  // Phase 1: Reversible Validation (Steps 1-5)
  this.assertPreValidation(preparedEvidence, expectedOwner);

  // Phase 2: Synchronous Atomic State Transition (Steps 6-8)
  const releaseReceipt = this._drainer.executeSynchronousRelease(expectedOwner);
  const finalEvidence = this.assembleEvidence(preparedEvidence, releaseReceipt);

  // THE LINEARIZATION POINT:
  this._activeBarrier.resolveWithEvidence(finalEvidence);
  this._gateState = 'TERMINALLY_SETTLED';

  return finalEvidence;
}
```

#### Decoupled / Asynchronous Intermediate State: `CUSTODY_RELEASED_BARRIER_PENDING`
If release publication (Step 6) and barrier resolution (Step 8) occur across distinct method calls, the coordinator introduces an explicit intermediate state:
$$\mathbf{CUSTODY\_RELEASED\_BARRIER\_PENDING}(\text{generation}, \text{ownerToken})$$

#### Protocol Across the Three Failure Intervals:

##### Interval A: Failure Before Custody-Release Publication
- **Owner:** Original owner remains authoritative.
- **In-Flight Record:** `_inFlightRecord` remains populated.
- **Barrier Status:** `PENDING`.
- **Receipts:** Zero release receipts created.
- **Permitted Retries:** Current attempt may retry pre-validation or wait for settlement.
- **Prohibited Transitions:** Barrier resolution prohibited; newer attempts fenced.

##### Interval B: Failure After Release Publication But Before Barrier Resolution
- **Owner:** Operation has exited in-flight status, but the generation's settlement obligation is retained in the coordinator's ledger:
  `_pendingSettlementObligations.set(generation, { ownerToken, releaseReceipt, reconciliationReceipt })`.
- **Barrier Status:** `CUSTODY_RELEASED_BARRIER_PENDING`.
- **Strict Invariants:**
  1. **Zero Simulated Rollback:** Released custody is **never** restored to in-flight.
  2. **Zero Commit Reversal:** An irrevocable archive commit is **never** reversed.
  3. **Zero Source Resurrection:** A retired source operation is **never** resurrected.
  4. **Newer Generation Fencing:** A newer attempt or generation attempting to acquire custody is strictly blocked by:
     $$\mathbf{assertNoPendingSettlementObligations}() \implies \text{throws } \mathbf{GENERATION\_SETTLEMENT\_PENDING}$$
- **Recovery Resolution:** The recovery watchdog or coordinator reevaluates the obligation ledger, observes that all receipts are already generated, and invokes `barrier.resolveWithEvidence(assembledEvidence)`. If fatal corruption prevents resolution, the barrier latches into `TERMINAL_DEGRADED` and gates recovery into `CUSTODY_RECONCILIATION_BLOCKED`.

##### Interval C: Failure After Barrier Resolution But Before Retirement
- **Barrier Status:** `RESOLVED`.
- **Invariants:**
  1. Barrier resolution is permanent for generation $g$.
  2. The barrier can never be re-resolved or reused for generation $g+1$.
  3. Retirement failure does not erase terminal evidence or unresolved obligations.
  4. Retry of retirement (`this._retireBarrier(g)`) is idempotent and bounded.

---

### 12.9 Explicit Failure Semantics vs. Impossible General Rollback

To eliminate misleading "rollback" claims, the architecture defines strict failure semantics for each category of operation:

| Category | Typical Operations | Irreversibility / Failure Semantics |
| :--- | :--- | :--- |
| **Reversible Preparation** | Receipt validation, 5-field token equality, memory pre-allocation, defensive freezing. | **100% Reversible.** Any throw aborts the call with zero state mutations. Original owner and barrier unchanged. |
| **Irrevocable Commit** | `commitRetirementBatch` in `#committedTransactions`. | **Irreversible.** Cannot be rolled back. Destination Promise rejection is a post-commit execution failure, not an abort. |
| **Irreversible Source Retirement** | Splicing operation out of `QuarantineBuffer` after commit. | **Irreversible.** Operation is permanently retired. Cannot be resurrected into source buffer on subsequent coordinator throw. |
| **Published Custody Release** | Publishing `_inFlightRecord = null` in `QuarantineDrainer`. | **Irreversible.** In-flight custody is surrendered. Cannot be restored to in-flight without violating single-owner rules. |
| **Post-Publication Obligation** | Gate state `CUSTODY_RELEASED_BARRIER_PENDING`. | **Recoverable via Ledger.** Retained in `_pendingSettlementObligations`. Resolved by completion reevaluation; blocks newer generations. |
| **Terminal Settlement** | `barrier.resolveWithEvidence(evidence)`. | **Irreversible & Terminal.** Settlement is final. Retirement failure cannot undo or reuse resolved barrier. |

---

### 12.10 Deep Immutability of Published Custody Records

All published custody state objects, ownership tokens, and evidence receipts are deeply frozen:
```typescript
function deepFreeze<T extends object>(obj: T): Readonly<T> {
  Object.freeze(obj);
  for (const key of Object.getOwnPropertyNames(obj)) {
    const val = (obj as any)[key];
    if (val && typeof val === 'object' && !Object.isFrozen(val)) {
      deepFreeze(val);
    }
  }
  return obj;
}
```

---

### 12.11 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

```
[Settlement / Drain Trigger (Promise Completed / Invalidation Received)]
                  │
                  ▼
1. Query Authoritative Archive: archive.getTransactionDisposition(txId)
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
8. PRE-VALIDATE & ASSEMBLE EVIDENCE (Steps 1-5 of Protocol):
   Assert all brands, 5-field identities, and disposition consistency.
                  │
                  ▼
9. SYNCHRONOUS LINEARIZATION (Steps 6-8 of Protocol):
   this._drainer.publishCustodyRelease(ownerToken);
   activeBarrier(g).resolveWithEvidence(evidence);
   gateState = 'TERMINALLY_SETTLED';
                  │
                  ▼
10. Retire Generation Barrier: this._retireBarrier(expectedOwnerToken.custodyGeneration);
```

---

### 12.12 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

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
6. Reconcile Custody (Restoration or Transfer):
   ├─ If Restore to Source:
   │  quarantineBuffer.unshiftDrain(op);
   │  └──► Yields RestoredToSourceReceipt (sourceRetainsExclusiveOwnership === true)
   └─ If Transfer to Authorized Destination:
      authorizedDestination.acceptCustody(op);
      └──► Yields TransferredToDestinationReceipt (destinationExclusiveOwnershipAccepted === true)
                  │
                  ▼
7. Verify Custody Representation (Single Exclusive Owner Confirmed)
                  │
                  ▼
8. PRE-VALIDATE & ASSEMBLE EVIDENCE (Steps 1-5 of Protocol):
   Assert all brands, 5-field identities, and reconciliation invariants.
                  │
                  ▼
9. SYNCHRONOUS LINEARIZATION (Steps 6-8 of Protocol):
   this._drainer.publishCustodyRelease(ownerToken);
   activeBarrier(g).resolveWithEvidence(evidence);
   gateState = 'TERMINALLY_SETTLED';
                  │
                  ▼
10. Retire Generation Barrier: this._retireBarrier(expectedOwnerToken.custodyGeneration);
```

---

## 13. The Custody-Reconciliation Gate State Machine

```
                      ┌─────────────────────────┐
                      │ 1. DRAIN_IN_PROGRESS    │◄──────────────┐
                      └────────────┬────────────┘               │
                                   │ Dequeue                    │
                                   ▼                            │
                      ┌─────────────────────────┐               │
                      │ 2. IN_FLIGHT_HELD       │               │
                      └────────────┬────────────┘               │
                                   │ Promise Settled            │
                                   ▼                            │
                      ┌─────────────────────────┐               │
                      │ 3. RECONCILING_CUSTODY  │               │
                      └──────┬───────────┬──────┘               │
            Success (Sync)   │           │ Decoupled / Throw    │
                             │           ▼                      │
                             │   ┌───────────────────────────┐  │
                             │   │ 4. RELEASED_BARRIER_      │  │
                             │   │    PENDING (Obligation)   │  │
                             │   └───────────┬───────────────┘  │
                             │               │ Settle Barrier   │
                             ▼               ▼                  │
                      ┌─────────────────────────┐               │
                      │ 5. TERMINALLY_SETTLED   │───────────────┘
                      └─────────────────────────┘   Next Drain
                                   │
                                   ▼ (Unresolved Error)
                      ┌─────────────────────────┐
                      │ 6. RECONCILIATION_      │
                      │    BLOCKED (Fail-Closed)│
                      └─────────────────────────┘
```

---

## 14. Comprehensive Analysis of Cases A through H Across Operational States

| Case | Attempt Ownership | In-Flight Record | Barrier Status | Reconciliation Action | Final Result |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **A** | Attempt 1 (Active) | Occupied | PENDING | Normal commit, source retired. | Terminal settlement via Protocol A. |
| **B** | Attempt 1 (Active) | Occupied | PENDING | Post-commit Promise rejection. | Classified execution failure; Protocol A completes. |
| **C** | Attempt 1 (Active) | Occupied | PENDING | Aborted fenced; restored to source. | Restored to source; Protocol B completes. |
| **D** | Attempt 1 (Active) | Occupied | PENDING | Aborted fenced; transferred to destination. | Transferred to destination; Protocol B completes. |
| **E** | Attempt 1 (Timed Out) | Occupied | PENDING | Attempt 2 admitted; waits at barrier. | Attempt 1 completes; Attempt 2 unblocks cleanly. |
| **F** | Attempt 1 (Timed Out) | Released | RELEASED_PENDING | Post-release barrier throw in Attempt 1. | Ledger reevaluated; barrier resolved; Attempt 2 resumes. |
| **G** | Attempt 1 (Stale) | Empty | RESOLVED | Stale continuation attempts custody release. | Rejected with `STALE_CUSTODY_RELEASE_REJECTED`. |
| **H** | Any | Occupied | PENDING | Archive arbitration throws or times out. | `CUSTODY_RECONCILIATION_BLOCKED` fail-closed. |

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| # | Trigger Event | Immediate System State | Authoritative Owner | Barrier Status | Permitted Action | Prohibited Action |
| :- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | Destination Promise rejection after commit | In-flight held | Drainer | PENDING | Protocol A source retirement | Protocol B restoration |
| **2** | Destination Promise rejection before commit | In-flight held | Drainer | PENDING | Arbitration query | Protocol A retirement |
| **3** | Archive arbitration returns `COMMITTED` | In-flight held | Drainer | PENDING | Protocol A source retirement | Protocol B restoration |
| **4** | Archive arbitration returns `ABORTED_FENCED` | In-flight held | Drainer | PENDING | Protocol B restoration/transfer | Protocol A retirement |
| **5** | Source retirement throws error | In-flight held | Drainer | PENDING | Retry retirement | Release drainer custody |
| **6** | Pre-validation throws (Interval A) | In-flight held | Drainer | PENDING | Fix inputs / retry validation | Release custody / resolve barrier |
| **7** | Post-release throw (Interval B) | Released | Obligation Ledger | RELEASED_PENDING | Completion reevaluation | Resurrect custody / newer attempt drain |
| **8** | Post-resolution throw (Interval C) | Released | Terminal Archive | RESOLVED | Retry retirement cleanup | Reuse barrier / overwrite proof |
| **9** | Stale continuation release attempt | Released/Held | Active Owner | Untouched | Log warning & ignore | Release newer custody |
| **10** | Watchdog timeout (5,000 ms) | In-flight held | Drainer | PENDING | Admit Attempt 2 to wait | Cancel Promise / release custody |
| **11** | Idempotent commit retry (identical digest) | Committed | Archive | Resolved | Return cached commit receipt | Commit new root |
| **12** | Mutated commit retry (altered digest) | Committed | Archive | Resolved | Throw `IDENTITY_MUTATION` | Overwrite transaction |
| **13** | Late commit on fenced transaction | Fenced | Archive | Aborted | Throw `ALREADY_FENCED` | Overwrite tombstone |
| **14** | Capacity reservation expired/fenced | Active | Archive | Pending | Reject commit | Commit unreserved batch |
| **15** | Archive root pre-allocation throws | Active | Archive | Pending | Abort pre-allocation cleanly | Commit partial state |
| **16** | Fenced map reaches 1,000 capacity | Backpressure | Archive | Fenced | Reject new fencing / evict safe | Exceed 1,000 entries |
| **17** | Active reservation blocks eviction | Backpressure | Archive | Blocked | Reject new reservation requests | Advance floor past reservation |
| **18** | Reservation settles under backpressure | Capacity open | Archive | Unblocked | Evict oldest tombstone in turn | Unbounded queue |
| **19** | Zero active reservations eviction | Capacity open | Archive | Normal | Evict tombstone < current Gen | Advance floor past current Gen |
| **20** | Caller passes fabricated Boolean proof | In-flight held | Drainer | PENDING | Throw `INVALID_BRAND` | Resolve barrier |
| **21** | Caller passes mismatched 5-field identity | In-flight held | Drainer | PENDING | Throw `IDENTITY_MISMATCH` | Resolve barrier |
| **22** | Source restoration succeeds | Restored | Source Buffer | PENDING | Settle barrier via Protocol B | Transfer to destination |
| **23** | Destination transfer succeeds | Transferred | Destination | PENDING | Settle barrier via Protocol B | Restore to source buffer |
| **24** | Catastrophic proof corruption | Blocked | Audit Admin | DEGRADED | Latch `RECONCILIATION_BLOCKED` | Optimistic barrier resolution |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{EVIDENCE\_MATCHED}(g)$$
2. **Attempt-Fenced Custody Release Invariant:**
   $$\mathbf{STALE\_ATTEMPT} \implies \mathbf{CANNOT\_RELEASE\_NEWER\_CUSTODY}$$
3. **Execution Settlement Precedence:** The settlement barrier **CANNOT** resolve while the destination Promise remains pending.
4. **Five-Field Owner-Token Verification Precedence:** Custody release **CANNOT** execute unless `isEquivalentOwnerToken` evaluates to `true` across all 5 fields.
5. **Authoritative Evidence Precedence:** A barrier **CANNOT** resolve unless `assertAuthoritativeSettlementEvidence` validates all four component receipts.
6. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
7. **Active Reservation Floor Protection:** `fencingGenerationFloor` **CANNOT** advance past $\min_{r \in \text{activeReservations}}(r.\text{generation})$.
8. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this._custodyState.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
9. **Watchdog Immunity:** A watchdog timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) **CANNOT** release drainer custody or cancel the destination Promise.
10. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate Evidence} \longrightarrow \text{4. Release Drainer} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R11 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`) and latched `TIMED_OUT`. Its continuation is completely fenced and cannot mutate roots or stage batches.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's continuation attempts `publishCustodyRelease(token1)`, which is rejected because Attempt 2 owns the coordinator. The coordinator reconciles custody, executes `publishCustodyRelease(token2)`, and resolves the barrier with verified authoritative evidence. Attempt 2 unblocks with clean custody and proceeds to Step 7 with exclusive authority.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

Sub-Milestone M5.1 test suites must verify the following twenty deterministic test specifications:

- **Test R18-R11-T01 (Failure Before Release Leaves Original Owner Intact):** Trigger throw during pre-validation (Interval A); verify in-flight record remains owned by original `CustodyOwnershipToken`, barrier remains pending, and zero release receipts exist.
- **Test R18-R11-T02 (Failure After Release Cannot Resurrect Retired Source Custody):** Release custody for committed transaction, then mock throw in barrier resolution; verify source quarantine operation is NOT resurrected back into source buffer.
- **Test R18-R11-T03 (Release Publication Followed by Coordinator Failure Preserves Obligation):** Mock failure immediately following `publishCustodyRelease`; verify generation obligation is retained in `_pendingSettlementObligations` and recoverable.
- **Test R18-R11-T04 (Newer Generation Blocked by Unresolved Settlement Obligation):** With barrier in `CUSTODY_RELEASED_BARRIER_PENDING`, attempt to dequeue or advance generation; verify throws `GENERATION_SETTLEMENT_PENDING`.
- **Test R18-R11-T05 (Barrier Resolution Requires Exact-Owner Release Evidence):** Omit `DrainerCustodyReleaseReceipt`; verify `assertAuthoritativeSettlementEvidence` throws and barrier cannot resolve.
- **Test R18-R11-T06 (Barrier Resolution and Release Publication Follow Linearization Model):** Verify unified synchronous linearization executes release publication, barrier resolution, and gate transition atomically in one turn.
- **Test R18-R11-T07 (Failure After Barrier Resolution Cannot Make Barrier Reusable):** Resolve barrier 1, then simulate throw during retirement; verify barrier 1 remains resolved and cannot be re-resolved or reused for generation 2.
- **Test R18-R11-T08 (Retirement Failure Cannot Erase Terminal Settlement Evidence):** Mock throw in `_retireBarrier`; verify resolved proof remains in generation audit history.
- **Test R18-R11-T09 (Post-Commit Promise Rejection Never Reverses Irrevocable Commit):** Reject destination Promise after confirmed commit; verify transaction remains committed in archive and Protocol B restoration is rejected.
- **Test R18-R11-T10 (Abort Restoration Returns Exclusive Ownership to Source):** Trigger `ABORTED_FENCED` restoration; verify `RestoredToSourceReceipt` proves `sourceRetainsExclusiveOwnership === true`.
- **Test R18-R11-T11 (Abort Transfer Produces Exclusive Ownership at Destination):** Trigger `ABORTED_FENCED` transfer; verify `TransferredToDestinationReceipt` proves `destinationExclusiveOwnershipAccepted === true` and `sourceRetainsOwnership === false`.
- **Test R18-R11-T12 (No Operation Becomes Ownerless During Abort Restoration or Transfer):** Trace custody during abort reconciliation; verify at every microtask turn the operation is owned by either drainer, source, or named destination.
- **Test R18-R11-T13 (No Operation Becomes Multiply Owned During Custody Transition):** Verify single-owner invariant ($N_{\text{owners}} = 1$) holds across dequeue, commit, abort restoration, and transfer.
- **Test R18-R11-T14 (FIFO Tombstone Eviction Safe Under Monotonic Generation Invariant):** Verify FIFO head of `#fencedTransactions` has generation $< \min(r.\text{generation})$ before eviction.
- **Test R18-R11-T15 (Out-of-Order Tombstone Insertion Cannot Advance Floor Unsafely):** Insert out-of-order tombstone; verify eviction selects true minimal generation or preserves floor.
- **Test R18-R11-T16 (No-Active-Reservation Eviction Follows Explicit Rule):** Drain all active reservations; verify eviction allows tombstones with generation $< \text{currentArchiveGeneration}$ and advances floor correctly.
- **Test R18-R11-T17 (Strict 1,000-Tombstone Bound Holds Under Repeated Pressure):** Trigger 1,500 fencing requests under backpressure; verify `#fencedTransactions.size <= 1000` and surplus requests are rejected.
- **Test R18-R11-T18 (Private Receipt Construction Unreachable via Public Exports):** Assert receipt branding symbols and constructors are not exported from `@syncplay/protocol`.
- **Test R18-R11-T19 (Stale Receipt & Continuation Fencing Effective Across Transitions):** Provide Attempt 1 receipt to Attempt 2 transition; assert rejected with `IDENTITY_MISMATCH`.
- **Test R18-R11-T20 (Watchdog Timeout Never Creates Execution Settlement Evidence):** Fire 5,000 ms watchdog; assert status latches `DEGRADED`; assert no `ExecutionSettlementReceipt` is produced.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R11):
================================================================================
1. [CUSTODY-RELEASE / BARRIER RESOLUTION CLOSURE]: Does the Engineering Authority
   approve the two-phase release-to-resolution protocol, establishing a unified
   synchronous linearization point and the intermediate recovery state
   CUSTODY_RELEASED_BARRIER_PENDING to eliminate ownerless or unresolved gaps?

2. [PHASE-SPECIFIC FAILURE SEMANTICS]: Does the Engineering Authority approve
   replacing generic rollback claims with explicit failure semantics distinguishing
   reversible preparation, irrevocable commit, irreversible retirement, and
   post-publication obligations?

3. [QUALIFIED RECEIPT SECURITY BOUNDARY]: Does the Engineering Authority approve
   scoping receipt integrity to module-internal private symbol branding and defensive
   freezing within a cooperative, non-Byzantine trust model?

4. [DISCRIMINATED ABORT CUSTODY OUTCOMES]: Does the Engineering Authority approve
   the discriminated union between RestoredToSourceReceipt and
   TransferredToDestinationReceipt, enforcing exclusive single ownership?

5. [TOMBSTONE ORDERING & GENERATION FLOOR ADVANCEMENT]: Does the Engineering Authority
   approve the documented monotonic tombstone generation invariant, minimal generation
   selection, and the explicit no-active-reservations eviction floor rule?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R11 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R11 proposal completes the architectural specification for concurrent recovery arbitration. It closes the custody-release/barrier-resolution timing gap, eliminates impossible general rollback claims, provides explicit failure semantics, qualifies the receipt security model, genuinely discriminates abort custody outcomes, and formalizes tombstone generation ordering and safe eviction floors.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R11 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
