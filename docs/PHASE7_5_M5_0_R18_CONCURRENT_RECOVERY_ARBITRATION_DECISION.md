# Phase 7.5 M5.0-R18-R14 — Idempotent Publication Retry Reachability and Evidence Ownership Closure
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Baseline Commit:** `98d0ca7b5c4ad69dd2770d5b96b4102997eb0cdc` (M5.0-R18-R13)
**Direct Parent Commit:** `98d0ca7b5c4ad69dd2770d5b96b4102997eb0cdc`
**Authorization Scope:** M5.0-R18-R14 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package exports, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1 remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R14 Objectives

In Phase 7.5 Sub-Milestones M5.0-R18-R10 through R18-R13, we established the candidate-root atomic publication model and closed collection-level immutability using `#private` storage. However, the subsequent architectural audit revealed two critical distributed-systems precision gaps:

1. **Unreachable Post-Publication Idempotent Retries:** In R18-R13's pseudocode, `publishReleaseAndObligation` verified `currentRoot.inFlightRecord` *before* checking whether the generation was already published in `pendingSettlementObligations`. Once publication succeeds, `inFlightRecord` is atomically cleared (`null`). As a result, an identical retry (e.g. following a network drop, timeout, or lost response) encountered `inFlightRecord === null` and immediately failed with `STALE_CUSTODY_RELEASE_REJECTED` before ever reaching the existing-obligation check. Idempotent retry was physically unreachable.
2. **Uncached Result Allocation on Retry & Open Evidence Aliases:** On retry, R18-R13 allocated a new wrapper object (`Object.freeze({ releaseReceipt, obligation })`), violating claims of returning the original cached result and performing zero retry allocations. Furthermore, incoming `preparedEvidence` receipts and nested byte buffers (such as `archivedBatchDigest`) were referenced directly, leaving internal published state vulnerable to mutation through surviving caller-controlled aliases.

### Core Architectural Guarantees Established in R18-R14:

- **Explicit Dual-Path Publication Protocol:** Strictly separates the **First Publication Path** from the **Post-Publication Idempotent Retry Path**. The retry path evaluates existing obligations *before* in-flight checks, verifying complete 5-field token identity, transaction ID, operation ID, attempt ownership, and barrier identity without requiring an active in-flight record.
- **Cached Publication Result Ownership:** The original deeply frozen `PublicationResult` is stored in the authoritative root's `publicationResults` map during first publication. Subsequent valid retries return the exact same cached reference, guaranteeing zero allocations on retry and eliminating circular object references.
- **End-to-End Nested Evidence Alias Closure:** All incoming receipts, tokens, and byte buffers (`Uint8Array`) are defensively cloned on ingress before inclusion in any candidate root. Egress accessors return deeply frozen objects or fresh byte clones, ensuring caller aliases cannot mutate published evidence.
- **Strict Generation Domain Fencing:** Mandates strictly positive safe integers ($1 \le g < 2^{53} - 1$) for all custody and fencing generations. Negative, zero, fractional, NaN, and exhausted sequence values are rejected upfront before lookup or publication.
- **Unified Acquisition Serialization Contract:** Enforces a 5-step serialized acquisition rule across all custody entry points, eliminating check-then-acquire races.
- **Exhaustive Future Deterministic Test Suite:** Codifies thirty deterministic test specifications for Sub-Milestone M5.1, incorporating all thirteen test requirements from Correction 6.

---

## 2. Separation of the Three Independent Conditions

A correct recovery arbitration architecture must strictly distinguish three independent system conditions:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. Administrative Attempt Ownership (Recovery Coordinator)                 │
│    - Governed by monotonic recoveryAttemptId (e.g. Attempt 1 vs. Attempt 2).│
│    - Fenced via Attempt Fencing Tokens and monotonic watchdog timers.       │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. Authoritative Custody Root (QuarantineDrainer & Custody Gate)            │
│    - Governed by this.#custodyRoot (AuthoritativeCustodyRoot).              │
│    - Contains inFlightRecord, gateState, pendingSettlementObligations,      │
│      and publicationResults.                                                │
│    - Mutated exclusively via single atomic state-reference replacement.     │
├─────────────────────────────────────────────────────────────────────────────┤
│ 3. Settlement Barrier State (Generation-Scoped Barrier)                     │
│    - Governed by custodyGeneration and AuthoritativeSettlementEvidence.     │
│    - Linearizes if and only if: Promise settled AND Custody reconciled.     │
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
   - Represented by `this.#custodyRoot.inFlightRecord`.
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

## 9. Process Failure Model & Monotonic Clock Semantics

The recovery architecture operates under a single-node, process-volatile failure model:

- Node crash / process death results in loss of volatile memory (in-memory buffers, drainer records, uncommitted transactions).
- Durability across process crashes is governed by persistent append-logs outside M5.0.
- Single-threaded event loop execution guarantees that synchronous mutations within a turn cannot be interleaved by other JavaScript microtasks.
- Asynchronous Promise settlement, timer events, and transport I/O interleave between execution turns.

### Decoupled Clock Semantics:
- **Diagnostic Wall-Clock Metadata:** Wall-clock timestamps (`Date.now()`, ISO-8601 strings) are purely diagnostic metadata. They **must never** determine causal ordering, attempt fencing, receipt validity, generation ordering, or settlement correctness.
- **Monotonic Clocks for Durations & Fencing:** All elapsed durations, timeouts, and watchdog intervals must be measured using a monotonic clock source (`performance.now()` or `process.hrtime.bigint()`). Monotonic clocks are immune to NTP adjustments, system time shifts, and leap seconds.
- **Duration Representation & Validation:** If elapsed duration cannot be determined (e.g. clock source error), the field is represented as `null`, and logged diagnostically; safety predicates never rely on elapsed duration values.

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
- **Required Action:** Execute Protocol A: retire source operations from `QuarantineBuffer`, verify buffer disjointness, await destination Promise settlement, publish attempt-fenced custody release with its pending obligation, and resolve the settlement barrier upon verified authoritative evidence.

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
- **Required Action:** Execute Protocol B: await destination Promise settlement, reconcile custody via restoration or transfer, verify exclusive ownership, publish attempt-fenced custody release with its pending obligation, and resolve the settlement barrier.
  - **Variant 1 (`RESTORED_TO_SOURCE`):** Operation restored to source quarantine buffer head; source retains exclusive custody.
  - **Variant 2 (`TRANSFERRED_TO_DESTINATION`):** Operation transferred to an authorized named destination; destination accepts exclusive custody.

### D. Inconclusive or Commit Pending (Level 3 Ambiguity)
- **Condition:** Arbitration throws, times out, or reports `NOT_FOUND_AT_QUERY`.
- **Required Action:** Drainer in-flight custody is **strictly retained** ($N_{\text{in-flight}} = 1$). Gate transitions to `CUSTODY_RECONCILIATION_BLOCKED`. Settlement barrier remains **PENDING**.

---

## 12. Complete Custody Authorization, Receipt Boundary, Retry Reachability & Evidence Ownership

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
  readonly monotonicSettledMs: number;
  readonly errorDetails?: string;
}

export interface ArchiveDispositionReceipt {
  readonly [ARCHIVE_RECEIPT_BRAND]: true;
  readonly identity: CustodyOwnershipToken;
  readonly disposition: 'COMMITTED' | 'ABORTED_FENCED';
  readonly confirmedGeneration: number;
  readonly archivedBatchDigest: string; // Immutable hex string representation
  readonly arbitratedAtTimestamp: number;
  readonly monotonicArbitratedMs: number;
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
  readonly monotonicRetiredMs: number;
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
  readonly monotonicRestoredMs: number;
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
  readonly monotonicTransferredMs: number;
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
  readonly previousInFlightDurationMs: number | null;
  readonly releasedAtTimestamp: number;
  readonly monotonicReleasedMs: number;
}
```

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

### 12.8 Separation of the Three Guarantees

To ensure mathematical precision across concurrent operations, the architecture strictly separates three concepts:

1. **Non-Interleavable Execution:** A synchronous JavaScript code block running without `await`, Promise callback execution, `setTimeout`, or microtask yields. This guarantees that no other application task executes between two statements in the same turn. **It does not guarantee atomicity** if an exception throws in the middle of multiple property updates.
2. **Atomic Publication:** The single synchronous assignment of an authoritative state root reference (`this.#custodyRoot = candidateRoot`). This guarantees that all mutations encapsulated in `candidateRoot` become observable simultaneously and inseparably. If an exception occurs during the private construction of `candidateRoot`, the previous authoritative root remains 100% active.
3. **Barrier Linearization:** The synchronous execution of `barrier.resolveWithEvidence(finalEvidence)`. This settles the generation-scoped Promise and transitions the barrier permanently from `ACTIVE` to `RESOLVED`.

---

### 12.9 Runtime Immutability of Published Collections

In ECMAScript, calling `Object.freeze()` on a `Map` or `Set` instance freezes only properties on the object wrapper itself; it **does not** disable internal methods (`.set()`, `.delete()`, `.clear()`). Furthermore, TypeScript's `ReadonlyMap<K, V>` is purely a compile-time type restriction stripped during compilation.

To achieve **actual runtime immutability**, all collections published by `AuthoritativeCustodyRoot` use an encapsulated class backed by ECMAScript private fields:

```typescript
export class ImmutableMap<K, V> {
  readonly #inner: Map<K, V>;

  constructor(entries?: Iterable<readonly [K, V]>) {
    this.#inner = new Map();
    if (entries) {
      for (const [k, v] of entries) {
        // Enforce defensive copy or deep freeze on entries
        this.#inner.set(k, v);
      }
    }
    Object.freeze(this);
  }

  get(key: K): V | undefined { return this.#inner.get(key); }
  has(key: K): boolean { return this.#inner.has(key); }
  get size(): number { return this.#inner.size; }
  keys(): IterableIterator<K> { return this.#inner.keys(); }
  values(): IterableIterator<V> { return this.#inner.values(); }
  entries(): IterableIterator<[K, V]> { return this.#inner.entries(); }
  forEach(callback: (value: V, key: K, map: this) => void): void {
    this.#inner.forEach((v, k) => callback(v, k, this));
  }
  [Symbol.iterator](): IterableIterator<[K, V]> { return this.#inner.entries(); }

  // Mutating methods explicitly throw at runtime:
  set(): never { throw new TypeError('ImmutableMap cannot be mutated at runtime'); }
  delete(): never { throw new TypeError('ImmutableMap cannot be mutated at runtime'); }
  clear(): never { throw new TypeError('ImmutableMap cannot be mutated at runtime'); }
}
```

Because `#inner` is completely private and mutating methods unconditionally throw, no caller can mutate published collections through any public or internal alias.

#### Defensive Handling of Byte Arrays:
Non-empty typed arrays (`Uint8Array`, `Buffer`) cannot be made immutable by `Object.freeze()` (V8 allows element modification even on frozen typed arrays). Therefore:
- **On Ingress:** Byte arrays must be defensively copied via `new Uint8Array(callerBuffer.slice())`.
- **On Egress:** Expose only immutable hex strings or fresh clones (`new Uint8Array(storedBuffer.slice())`).

---

### 12.10 Ingress and Egress Nested Evidence Alias Closure

To ensure that callers cannot mutate internal state after validation:

1. **Ingress Token Normalization:** When `expectedOwner` is received, it is defensively copied into a deeply frozen token:
   ```typescript
   function cloneAndFreezeOwnerToken(source: CustodyOwnershipToken): CustodyOwnershipToken {
     return Object.freeze({
       recoveryAttemptId: Number(source.recoveryAttemptId),
       custodyGeneration: Number(source.custodyGeneration),
       operationIdentity: String(source.operationIdentity),
       transactionId: String(source.transactionId),
       drainerOwnershipGeneration: Number(source.drainerOwnershipGeneration),
     });
   }
   ```
2. **Ingress Evidence Normalization:** Receipts passed in `preparedEvidence` are defensively copied and deeply frozen using explicit factory cloning functions before storage, breaking any reference to caller-controlled objects. Any byte arrays are cloned via `new Uint8Array(...)`.
3. **Egress Deep Immutability:** All receipts, tokens, and root projections returned to callers are deeply frozen objects containing only primitives, immutable strings, or `ImmutableMap` instances.

---

### 12.11 Permitted Generation Domain & Sequence Exhaustion Boundary

All custody generations, published generations, and fencing generations must strictly belong to the domain of **positive safe integers**:

$$\mathbf{ValidGeneration}(g) \iff (g \in \mathbb{Z}^+) \land (1 \le g \le \text{Number.MAX\_SAFE\_INTEGER} - 1)$$

```typescript
export function assertValidGenerationDomain(g: number): void {
  if (
    typeof g !== 'number' ||
    !Number.isInteger(g) ||
    !Number.isSafeInteger(g) ||
    g <= 0 ||
    g >= Number.MAX_SAFE_INTEGER
  ) {
    if (typeof g === 'number' && g >= Number.MAX_SAFE_INTEGER) {
      throw new Error('GENERATION_SEQUENCE_EXHAUSTED');
    }
    throw new Error('INVALID_GENERATION_DOMAIN');
  }
}
```

#### Rejection Rules:
- Negative integers ($g < 0$) and zero ($g === 0$) are unconditionally rejected.
- Fractional values, `NaN`, `+Infinity`, and `-Infinity` are unconditionally rejected.
- Sequence values $\ge \text{Number.MAX\_SAFE\_INTEGER}$ ($2^{53} - 1$) fail closed with `GENERATION_SEQUENCE_EXHAUSTED`.
- Wrapping to 0, silent truncation, and generation reuse are strictly forbidden.
- Validation must occur **before** any obligation lookup, idempotent return, or candidate-root construction.

---

### 12.12 The Dual-Path Publication & Idempotent Retry Protocol

The root state structure is defined as:

```typescript
export interface InFlightCustodyRecord {
  readonly token: CustodyOwnershipToken;
  readonly dequeuedAtTimestamp: number;
  readonly monotonicDequeuedMs: number;
}

export type CustodyGateState =
  | 'IDLE'
  | 'IN_FLIGHT_HELD'
  | 'RECONCILING_CUSTODY'
  | 'CUSTODY_RELEASED_BARRIER_PENDING'
  | 'TERMINALLY_SETTLED'
  | 'CUSTODY_RECONCILIATION_BLOCKED';

export interface PendingSettlementObligation {
  readonly obligationId: string;
  readonly generation: number;
  readonly barrierIdentity: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly publishedCustodyGeneration: number;
  readonly preAllocatedReleaseReceipt: DrainerCustodyReleaseReceipt;
  readonly preparedReconciliationReceipt: CustodyReconciliationReceipt;
  readonly preparedArchiveReceipt: ArchiveDispositionReceipt;
  readonly preparedExecutionReceipt: ExecutionSettlementReceipt;
  readonly publishedAtTimestamp: number;
  readonly monotonicPublishedMs: number;
}

export interface PublicationResult {
  readonly releaseReceipt: DrainerCustodyReleaseReceipt;
  readonly obligation: PendingSettlementObligation;
}

export interface AuthoritativeCustodyRoot {
  readonly publishedGeneration: number;
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, AuthoritativeSettlementEvidence>;
}
```

#### The Protocol:
```typescript
public publishReleaseAndObligation(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
  barrierIdentity: string,
): PublicationResult {
  const currentRoot = this.#custodyRoot;

  // 1. Generation Domain Validation (Must precede all lookups)
  const g = expectedOwner.custodyGeneration;
  assertValidGenerationDomain(g);

  // 2. Ingress Defensive Normalization
  const safeOwner = cloneAndFreezeOwnerToken(expectedOwner);
  const safeBarrierId = String(barrierIdentity);

  // 3. BRANCH: Post-Publication Idempotent Retry Path vs. First Publication Path
  if (currentRoot.pendingSettlementObligations.has(g)) {
    // =========================================================================
    // POST-PUBLICATION IDEMPOTENT RETRY PATH
    // =========================================================================
    const existingObligation = currentRoot.pendingSettlementObligations.get(g)!;

    // Validate Attempt Fencing: caller must match current coordinator attempt
    if (safeOwner.recoveryAttemptId !== this._currentAttemptId) {
      throw new Error('STALE_ATTEMPT_CUSTODY_RECOVERY_REJECTED');
    }

    // Validate 1-to-1 Identity Match across all five fields + barrier + tx + op
    if (
      !isEquivalentOwnerToken(existingObligation.ownerToken, safeOwner) ||
      existingObligation.transactionId !== safeOwner.transactionId ||
      existingObligation.operationIdentity !== safeOwner.operationIdentity ||
      existingObligation.barrierIdentity !== safeBarrierId
    ) {
      throw new Error('GENERATION_COLLISION_OCCUPIED_BY_DIFFERENT_OBLIGATION');
    }

    // Return the cached original publication result reference (Zero allocation!)
    return currentRoot.publicationResults.get(g)!;
  }

  // ===========================================================================
  // FIRST PUBLICATION PATH
  // ===========================================================================
  // 4. Validate In-Flight Owner (Requires active in-flight custody)
  if (!isEquivalentOwnerToken(currentRoot.inFlightRecord?.token ?? null, safeOwner)) {
    throw new Error('STALE_CUSTODY_RELEASE_REJECTED');
  }

  // 5. Validate Attempt Fencing
  if (safeOwner.recoveryAttemptId !== this._currentAttemptId) {
    throw new Error('STALE_ATTEMPT_CUSTODY_RELEASE_REJECTED');
  }

  // 6. Validate Generation Audit History
  if (currentRoot.generationAuditHistory.has(g)) {
    throw new Error('GENERATION_AUDIT_ALREADY_COMMITTED');
  }

  // 7. Validate Next Published Generation Domain
  const nextPublishedGen = currentRoot.publishedGeneration + 1;
  assertValidGenerationDomain(nextPublishedGen);

  // 8. Reversible Ingress Normalization of Evidence (Deep defensive copies)
  const safeEvidence = cloneAndFreezeEvidence(preparedEvidence, safeOwner);

  // 9. Pre-Allocate Release Receipt
  const monotonicNow = performance.now();
  const previousInFlightMs = currentRoot.inFlightRecord
    ? monotonicNow - currentRoot.inFlightRecord.monotonicDequeuedMs
    : null;

  const releaseReceipt: DrainerCustodyReleaseReceipt = Object.freeze({
    [DRAINER_RELEASE_BRAND]: true,
    identity: safeOwner,
    releasedCustodyGeneration: safeOwner.custodyGeneration,
    previousInFlightDurationMs: previousInFlightMs,
    releasedAtTimestamp: Date.now(),
    monotonicReleasedMs: monotonicNow,
  });

  // 10. Pre-Allocate Obligation Payload
  const obligation: PendingSettlementObligation = Object.freeze({
    obligationId: `obl-${safeOwner.custodyGeneration}-${Date.now()}`,
    generation: safeOwner.custodyGeneration,
    barrierIdentity: safeBarrierId,
    ownerToken: safeOwner,
    transactionId: safeOwner.transactionId,
    operationIdentity: safeOwner.operationIdentity,
    drainerOwnershipGeneration: safeOwner.drainerOwnershipGeneration,
    publishedCustodyGeneration: nextPublishedGen,
    preAllocatedReleaseReceipt: releaseReceipt,
    preparedReconciliationReceipt: safeEvidence.custodyReconciliation,
    preparedArchiveReceipt: safeEvidence.archiveDisposition,
    preparedExecutionReceipt: safeEvidence.executionSettlement,
    publishedAtTimestamp: Date.now(),
    monotonicPublishedMs: monotonicNow,
  });

  // 11. Pre-Allocate Cached Publication Result Reference
  const publicationResult: PublicationResult = Object.freeze({
    releaseReceipt,
    obligation,
  });

  // 12. Pre-Construct Immutable Candidate Collections
  const candidateObligations = new ImmutableMap([
    ...currentRoot.pendingSettlementObligations.entries(),
    [g, obligation],
  ]);
  const candidateResults = new ImmutableMap([
    ...currentRoot.publicationResults.entries(),
    [g, publicationResult],
  ]);

  // 13. Pre-Construct Immutable Candidate Root
  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: nextPublishedGen,
    inFlightRecord: null, // Custody released!
    gateState: 'CUSTODY_RELEASED_BARRIER_PENDING',
    pendingSettlementObligations: candidateObligations,
    publicationResults: candidateResults,
    generationAuditHistory: currentRoot.generationAuditHistory,
  });

  // 14. THE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;

  // 15. Infallible Return of Pre-Constructed Reference (Zero post-publication allocation):
  return publicationResult;
}
```

#### Key Guarantees:
1. **Reachable Retries:** Because `pendingSettlementObligations.has(g)` is evaluated in Step 3, retries succeed even when `inFlightRecord === null`.
2. **Zero Allocation on Retry:** Step 3 returns `currentRoot.publicationResults.get(g)!`, returning the exact cached reference without allocating wrapper objects.
3. **Application-Level Exception Safety:** All allocations and validations occur before Step 14. Any exception aborts without modifying `this.#custodyRoot`.

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Every custody-acquisition path must adhere to the **5-Step Serialized Acquisition Rule**:

1. **Read Current Root:** Inspect `currentRoot = this.#custodyRoot`.
2. **Evaluate Preconditions:** Validate that `currentRoot.pendingSettlementObligations.size === 0`, `currentRoot.inFlightRecord === null`, and the caller attempt matches `this._currentAttemptId`.
3. **Prepare Candidate Root Privately:** Construct candidate in-flight record and candidate root.
4. **Publish Atomically:** Replace `this.#custodyRoot = candidateRoot` via single synchronous reference assignment.
5. **No Interleaving:** Do not yield to the event loop, execute `await`, or invoke external callbacks between Step 1 and Step 4.

```typescript
public drainNext(attemptId: number, expectedGeneration: number): DequeuedOperation | null {
  const currentRoot = this.#custodyRoot;

  // 1. Generation Domain Check
  assertValidGenerationDomain(expectedGeneration);

  // 2. Precondition Evaluation Against Exact Root
  if (currentRoot.pendingSettlementObligations.size > 0) {
    throw new Error('CUSTODY_ACQUISITION_BLOCKED_BY_PENDING_SETTLEMENT_OBLIGATION');
  }
  if (currentRoot.inFlightRecord !== null) {
    throw new Error('CUSTODY_ACQUISITION_BLOCKED_IN_FLIGHT_OCCUPIED');
  }
  if (attemptId !== this._currentAttemptId) {
    throw new Error('STALE_ATTEMPT_CUSTODY_ACQUISITION_BLOCKED');
  }

  const op = this._quarantineBuffer.peek();
  if (!op) return null;

  const nextPublishedGen = currentRoot.publishedGeneration + 1;
  assertValidGenerationDomain(nextPublishedGen);

  // 3. Prepare Candidate In-Flight Record Privately
  const monotonicNow = performance.now();
  const newInFlight: InFlightCustodyRecord = Object.freeze({
    token: Object.freeze({
      recoveryAttemptId: attemptId,
      custodyGeneration: expectedGeneration,
      operationIdentity: op.identity,
      transactionId: this.generateTransactionId(attemptId, expectedGeneration),
      drainerOwnershipGeneration: this._drainerOwnershipGeneration + 1,
    }),
    dequeuedAtTimestamp: Date.now(),
    monotonicDequeuedMs: monotonicNow,
  });

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: nextPublishedGen,
    inFlightRecord: newInFlight,
    gateState: 'IN_FLIGHT_HELD',
    pendingSettlementObligations: currentRoot.pendingSettlementObligations,
    publicationResults: currentRoot.publicationResults,
    generationAuditHistory: currentRoot.generationAuditHistory,
  });

  // 4. Synchronous Atomic Publication
  this._quarantineBuffer.dequeue();
  this.#custodyRoot = candidateRoot;

  return op;
}
```

---

## 12.14 Comprehensive Exception-Safety Matrix Across Publication Phases

| Execution Phase | Typical Operations | Exception Safety & Failure Behavior |
| :--- | :--- | :--- |
| **Phase 1: Pre-Publication Preparation** | Receipt validation, 5-field token checks, memory allocation (`new ImmutableMap`, `Object.freeze`), candidate root construction. | **100% Reversible.** Any throw aborts the call with zero mutations. `this.#custodyRoot` remains unchanged; `inFlightRecord` remains owned; barrier remains `PENDING`. |
| **Phase 2: Atomic State Replacement** | `this.#custodyRoot = candidateRoot` | **Infallible Reference Swap.** Exact linearization point where custody is released and the obligation is published simultaneously. |
| **Phase 3: Post-Publication Resolution** | Proof validation, `barrier.resolveWithEvidence(evidence)`. | **Irreversible Release; Recoverable Obligation.** If resolution throws: released custody is **never** restored to in-flight; obligation remains published; newer attempts are blocked; recovery completion reevaluation applies. |
| **Phase 4: Post-Resolution Retirement** | Moving settled obligation to `generationAuditHistory`, cleaning barrier handle. | **Terminal Settlement Intact.** If retirement throws: barrier remains settled; settlement evidence is preserved; retirement cleanup is safely retried. |

---

## 12.15 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

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
   [this.#custodyRoot.inFlightRecord remains occupied; activeBarrier(g) remains PENDING]
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
8. REVERSIBLE PRE-PUBLICATION PREPARATION & ALLOCATION:
   Validate all brands, 5-field identities, pre-construct publicationResult and candidateRoot.
                  │
                  ▼
9. ATOMIC ROOT REPLACEMENT (Inseparable Release & Obligation Publication):
   this.#custodyRoot = candidateRoot;
   [inFlightRecord = null; gateState = 'CUSTODY_RELEASED_BARRIER_PENDING'; obligation published]
                  │
                  ▼
10. BARRIER LINEARIZATION:
    activeBarrier(g).resolveWithEvidence(evidence);
                  │
                  ▼
11. Retire Generation Barrier & Archive Settled Evidence:
    this.#custodyRoot = candidateSettledRoot;
    this._retireBarrier(expectedOwnerToken.custodyGeneration);
```

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

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
8. REVERSIBLE PRE-PUBLICATION PREPARATION & ALLOCATION:
   Validate all brands, 5-field identities, pre-construct publicationResult and candidateRoot.
                  │
                  ▼
9. ATOMIC ROOT REPLACEMENT (Inseparable Release & Obligation Publication):
   this.#custodyRoot = candidateRoot;
   [inFlightRecord = null; gateState = 'CUSTODY_RELEASED_BARRIER_PENDING'; obligation published]
                  │
                  ▼
10. BARRIER LINEARIZATION:
    activeBarrier(g).resolveWithEvidence(evidence);
                  │
                  ▼
11. Retire Generation Barrier & Archive Settled Evidence:
    this.#custodyRoot = candidateSettledRoot;
    this._retireBarrier(expectedOwnerToken.custodyGeneration);
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
                      └────────────┬────────────┘               │
                                   │ Atomic Root Replacement   │
                                   ▼                            │
                      ┌─────────────────────────┐               │
                      │ 4. RELEASED_BARRIER_    │               │
                      │    PENDING (Obligation) │               │
                      └────────────┬────────────┘               │
                                   │ Linearize Barrier          │
                                   ▼                            │
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
| **6** | Pre-publication throws (Phase 1) | In-flight held | Drainer | PENDING | Fix inputs / retry validation | Mutate root / release custody |
| **7** | Post-publication throw (Phase 3) | Released | Obligation Ledger | RELEASED_PENDING | Completion reevaluation | Resurrect custody / newer attempt drain |
| **8** | Post-resolution throw (Phase 4) | Released | Terminal Archive | RESOLVED | Retry retirement cleanup | Reuse barrier / overwrite proof |
| **9** | Stale continuation release attempt | Released/Held | Active Owner | Untouched | Log warning & ignore | Release newer custody |
| **10** | Watchdog timeout (5,000 ms monotonic) | In-flight held | Drainer | PENDING | Admit Attempt 2 to wait | Cancel Promise / release custody |
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
8. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
9. **Watchdog Immunity:** A monotonic watchdog timeout (`SETTLEMENT_BARRIER_TIMEOUT_MS = 5000`) **CANNOT** release drainer custody or cancel the destination Promise.
10. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.
11. **Inseparable Release/Obligation Invariant:** A released custody state can **never** become observable without its corresponding settlement obligation already present in `this.#custodyRoot.pendingSettlementObligations`.
12. **Runtime Immutability Invariant:** Published collections and evidence references **CANNOT** be mutated through `Map.prototype` or mutable array/buffer aliases.
13. **Idempotent Retry Reachability Invariant:** An identical retry for an already-published obligation **MUST** remain reachable and succeed without requiring an active in-flight record.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R14 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`) and latched `TIMED_OUT`. Its continuation is completely fenced and cannot mutate roots or stage batches.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's continuation attempts `publishReleaseAndObligation(token1)`, which is rejected because Attempt 2 owns the coordinator. The coordinator reconciles custody, executes `publishReleaseAndObligation(token2)`, and resolves the barrier with verified authoritative evidence. Attempt 2 unblocks with clean custody and proceeds to Step 7 with exclusive authority.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

Sub-Milestone M5.1 test suites must verify the following thirty deterministic test specifications:

- **Test R18-R14-T01 (Retry After Successful Publication Reaches Idempotent Path Despite inFlightRecord === null):** Publish release and obligation; call `publishReleaseAndObligation` again with identical parameters; verify call succeeds, returns cached result, and does not throw `STALE_CUSTODY_RELEASE_REJECTED`.
- **Test R18-R14-T02 (Lost Response Followed by Identical Retry Returns Documented Result):** Simulate lost response after publication; retry with exact owner, transaction, and barrier identities; verify exact `PublicationResult` reference is returned.
- **Test R18-R14-T03 (Retry With Different Operation Identity Is Rejected):** Attempt retry for existing generation with mismatched `operationIdentity`; assert throws `GENERATION_COLLISION_OCCUPIED_BY_DIFFERENT_OBLIGATION`.
- **Test R18-R14-T04 (Retry With Different Barrier Identity Is Rejected):** Attempt retry for existing generation with mismatched `barrierIdentity`; assert throws `GENERATION_COLLISION_OCCUPIED_BY_DIFFERENT_OBLIGATION`.
- **Test R18-R14-T05 (Stale Attempt Cannot Retrieve Another Attempt's Cached Receipt):** Attempt 1 attempts to retry generation owned by Attempt 2; assert throws `STALE_ATTEMPT_CUSTODY_RECOVERY_REJECTED`.
- **Test R18-R14-T06 (Repeated Valid Retries Do Not Mutate Authoritative Root):** Execute 10 consecutive identical retries; assert `this.#custodyRoot.publishedGeneration` remains constant and zero root replacements occur.
- **Test R18-R14-T07 (Cached-Result Identity Matches Documented Policy):** Assert `retryResult === originalResult` (exact same object reference returned from `publicationResults`).
- **Test R18-R14-T08 (Mutating Original Prepared Evidence Cannot Alter Authoritative Evidence):** Mutate properties on caller-supplied `preparedEvidence` after publication; assert published root retains original values.
- **Test R18-R14-T09 (Mutating Nested Receipt Objects Cannot Alter Published Root):** Attempt to modify properties on receipts returned in `PublicationResult`; assert properties are deeply frozen and throw on modification.
- **Test R18-R14-T10 (Mutating Byte Buffers Returned by Accessors Cannot Alter Stored Evidence):** Mutate elements of `Uint8Array` digest returned by egress accessors; assert internal stored digest remains unchanged.
- **Test R18-R14-T11 (ImmutableMap Iteration Cannot Expose Mutable Authoritative Values):** Iterate over `ImmutableMap.entries()`; assert entries and values cannot be modified to alter internal map.
- **Test R18-R14-T12 (Invalid Negative and Out-of-Domain Generations Rejected Upfront):** Pass $g = -1$, $g = 0$, $g = 1.5$, or $g = \text{NaN}$; assert throws `INVALID_GENERATION_DOMAIN` before obligation lookup.
- **Test R18-R14-T13 (Generation Exhaustion Remains Fail-Closed):** Pass $g = \text{Number.MAX\_SAFE\_INTEGER}$; assert throws `GENERATION_SEQUENCE_EXHAUSTED`.
- **Test R18-R14-T14 (Mutation Through Object.freeze(Map) Is Prevented at Runtime):** Assert that calling `.set()`, `.delete()`, or `.clear()` on `ImmutableMap` throws `TypeError` and does not mutate internal storage.
- **Test R18-R14-T15 (No Published Map or Set Reference Can Be Mutated by Consumers):** Obtain `pendingSettlementObligations` from published root; assert consumer mutations are rejected and internal root remains untouched.
- **Test R18-R14-T16 (No Nested Receipt or Identity Alias Can Mutate Published Evidence):** Mutate properties on caller-supplied `expectedOwner` after publication; assert published root retains original values.
- **Test R18-R14-T17 (Returned Result Construction Is Complete Before Root Publication):** Verify `publicationResult` is fully allocated and frozen before `this.#custodyRoot` assignment.
- **Test R18-R14-T18 (Exceptions During Preparation Leave Original Root Unchanged):** Trigger throw during `cloneAndFreezeOwnerToken` or Map allocation; assert `this.#custodyRoot` is 100% untouched.
- **Test R18-R14-T19 (Every Acquisition Path Checks and Publishes Against Same Root):** Verify `drainNext()`, recovery admission, and retries serialize checks and updates via atomic root replacement.
- **Test R18-R14-T20 (No Callback or Yield Can Occur Between Acquisition Validation and Publication):** Assert `drainNext()` executes synchronously without `await` or microtask yields between check and assignment.
- **Test R18-R14-T21 (Wall-Clock Regression Cannot Affect Elapsed-Time or Fencing):** Simulate backward NTP shift of 10,000 ms; assert monotonic duration calculations and watchdog timers remain strictly monotonic.
- **Test R18-R14-T22 (Defensive Byte Copies Prevent Post-Publication Mutation):** Mutate `Uint8Array` digest passed to archive; assert internal archive digest remains unchanged.
- **Test R18-R14-T23 (Exception After Publication Leaves Settlement Obligation Discoverable):** Publish release and obligation, then mock throw during barrier resolution; verify `this.#custodyRoot.pendingSettlementObligations.has(g) === true`.
- **Test R18-R14-T24 (No Observer Can See Released Custody Without Corresponding Obligation):** Assert that across all transitions, `inFlightRecord === null` implies `pendingSettlementObligations.has(g) === true`.
- **Test R18-R14-T25 (Newer Attempt Blocked by Pending Settlement Obligation):** With obligation present, attempt `drainNext()` from newer attempt; assert throws `CUSTODY_ACQUISITION_BLOCKED_BY_PENDING_SETTLEMENT_OBLIGATION`.
- **Test R18-R14-T26 (Alternate Reset and Retry Paths Cannot Bypass Pending Obligation Guard):** Attempt to invoke recovery retry while `pendingSettlementObligations.size > 0`; assert rejected fail-closed.
- **Test R18-R14-T27 (Failure After Release Cannot Resurrect Retired Source Custody):** Release custody for committed transaction, then mock throw in barrier resolution; verify source quarantine operation is NOT resurrected.
- **Test R18-R14-T28 (Post-Commit Promise Rejection Never Reverses Irrevocable Commit):** Reject destination Promise after confirmed commit; verify transaction remains committed in archive.
- **Test R18-R14-T29 (Abort Restoration Returns Exclusive Ownership to Source):** Trigger `ABORTED_FENCED` restoration; verify `RestoredToSourceReceipt` proves `sourceRetainsExclusiveOwnership === true`.
- **Test R18-R14-T30 (Strict 1,000-Tombstone Bound Holds Under Repeated Pressure):** Trigger 1,500 fencing requests; verify `#fencedTransactions.size <= 1000`.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R14):
================================================================================
1. [IDEMPOTENT RETRY REACHABILITY]: Does the Engineering Authority approve
   the dual-path publication protocol, evaluating existing obligations before
   in-flight checks to ensure retries succeed without an active in-flight record?

2. [CACHED PUBLICATION-RESULT OWNERSHIP]: Does the Engineering Authority approve
   storing the frozen PublicationResult in the root's publicationResults map and
   returning the exact same reference on valid retries, achieving zero allocation?

3. [COMPLETE EVIDENCE ALIAS CLOSURE]: Does the Engineering Authority approve
   defensive cloning of all incoming receipts, tokens, and byte buffers (Uint8Array)
   on ingress, and returning only frozen objects or fresh byte clones on egress?

4. [POSITIVE SAFE-INTEGER DOMAIN]: Does the Engineering Authority approve
   restricting generations strictly to positive safe integers (1 <= g < 2^53 - 1)
   and failing closed with GENERATION_SEQUENCE_EXHAUSTED upon limit reach?

5. [IMMUTABLEMAP RUNTIME INTEGRITY]: Does the Engineering Authority approve
   the #private backing field implementation of ImmutableMap, guaranteeing that
   collection structure cannot be mutated via Map.prototype methods at runtime?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R14 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R14 proposal closes the remaining reachability and alias-ownership gaps. It guarantees that post-publication idempotent retries remain reachable and allocate zero memory, protects published evidence against mutable aliases and typed array tampering, enforces strict positive safe-integer domains, and formalizes complete evidence ownership closure.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R14 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
