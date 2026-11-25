# Phase 7.5 M5.0-R18-R16 — Settled Retry Attempt Independence & Authoritative Audit Reconciliation
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Recovery-Systems Auditor
**Baseline Commit:** `16eb3e26e67bf7017a88c029c3f4cd0dbe080474` (M5.0-R18-R15)
**Direct Parent Commit:** `16eb3e26e67bf7017a88c029c3f4cd0dbe080474`
**Authorization Scope:** M5.0-R18-R16 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package exports, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1 remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R16 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R15, we established the complete publication retry lifecycle (never published, pending settlement, settled retained, and expired) and capped result retention at a strict 1,000-entry bound. However, rigorous architectural audit identified five critical lifecycle and consistency defects in R18-R15 that must be resolved:

1. **Settled Retries Incorrectly Dependent on Active Recovery Attempt:** R18-R15 required `safeOwner.recoveryAttemptId === this._currentAttemptId` in both pending and settled retry branches. This caused legitimate settled retries to fail if the coordinator's current attempt ID had cleared (e.g. `_currentAttemptId === null` between recovery operations, after clean shutdown, or when idle) or had advanced to a newer recovery attempt (e.g. Attempt 2 running after an Attempt 1 watchdog timeout). Because settled retries return already-committed immutable receipts without mutating state, coupling them to the active attempt violated distributed idempotency.
2. **Missing Runtime-Style Cross-Collection Audit Consistency Checks:** R18-R15 pseudocode checked map presence without verifying structural consistency across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory`. A pending obligation could theoretically exist alongside contradictory terminal audit evidence, or a settled result could be returned without authoritative audit backing.
3. **Incomplete Enforcement of Active-Operation Retention Pins:** R18-R15 stated conceptually that active retry and settlement operations pin results against eviction, but its pseudocode did not define pin representation, did not inspect pins during FIFO eviction, and did not specify pin lifecycle transitions across success, failure, cancellation, and watchdog timeouts.
4. **Ambiguous Optional Byte-Budget Semantics:** R18-R15 described a 10 MB byte budget as optional in prose but included future tests requiring byte-budget enforcement, creating an unresolved specification contradiction.
5. **Undefined Empty-Cache Generation-Floor Invariants:** R18-R15 used a minimum retained generation to define the floor invariant, leaving undefined the behavior when `publicationResults` is empty—both initially before any publications and historically after complete FIFO eviction.

### Core Architectural Guarantees Established in R18-R16:

- **Settled Retry Attempt Independence:** First publication strictly requires active-attempt ownership (`safeOwner.recoveryAttemptId === this._currentAttemptId`), active in-flight custody, and monotonic generation increment ($g = \text{publishedGeneration} + 1$). In contrast, settled retries are completely independent of `_currentAttemptId`. A settled retry validates the complete 5-field owner token (including the original `recoveryAttemptId`), transaction ID, operation ID, and barrier ID against retained evidence, succeeding even if `_currentAttemptId` is null, idle, or belongs to a later recovery attempt.
- **Enforced Cross-Collection Audit Reconciliation:** Pseudocode rigorously verifies structural invariants across collections. A pending obligation must have a matching cached result and zero contradictory terminal audit records. A settled retry must have an absent pending obligation, a retained publication result, and matching authoritative audit evidence with a verified terminal outcome (`CONFIRMED_COMMITTED` or `ABORTED_FENCED`). Contradictory evidence fails closed.
- **Operational Retention Pins with Strict FIFO Backpressure:** Formalizes `activeRetentionPins` in `AuthoritativeCustodyRoot`. Settlement barriers and long-running retries acquire pins via candidate-root replacement. FIFO eviction strictly inspects the oldest generation: if pinned, eviction CANNOT skip the entry (preserving contiguous monotonic generation floors) and instead applies fail-closed backpressure (`CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`).
- **Watchdog Timeout Pin Immunity:** Watchdog timeouts log warnings but **CANNOT** release retention pins whose underlying asynchronous Promises remain unsettled, preventing FIFO eviction from destroying results while external I/O remains in-flight.
- **Authoritative Count-Only Retention (`MAX_RETAINED_PUBLICATION_RESULTS = 1000`):** Resolves policy ambiguity by establishing count-based retention as the sole authoritative requirement. Byte-budget enforcement is formally classified as an optional implementation extension with exact UTF-8 serialization rules.
- **Empty-Cache and Monotonic Floor Semantics:** Formally defines the initial floor ($\mathbf{custodyGenerationFloor} = 0$), the fully evicted floor ($\mathbf{custodyGenerationFloor} = \mathbf{publishedGeneration} = N$), proves why $g \le \mathbf{custodyGenerationFloor}$ can never re-enter first publication, and proves that sequential publication proceeds seamlessly from a fully emptied cache.
- **Comprehensive Future Deterministic Test Suite (30 Test Cases):** Codifies thirty deterministic future test specifications in Section 19 covering attempt independence, audit reconciliation, retention pin lifecycles, and empty-cache floor semantics.

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
│    - Encapsulates publishedGeneration, custodyGenerationFloor,              │
│      inFlightRecord, gateState, pendingSettlementObligations,               │
│      publicationResults, generationAuditHistory, and activeRetentionPins.   │
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
  - Indicates transport delivery or socket state.
  - **Does NOT prove** terminal storage commitment or irreversible custody transfer.
- **Level 3: Authoritative State Proofs (Irreversible):**
  - Cryptographically or monotonically bound receipts: `DrainerCustodyReleaseReceipt`, `CustodyReconciliationReceipt`, `ArchiveDispositionReceipt`, and `ExecutionSettlementReceipt`.
  - Together, these constitute `AuthoritativeSettlementEvidence`, which is required to resolve a settlement barrier.

---

## 5. Transaction Absence vs. Terminal Non-Commit & Disposition Consistency

A missing transaction in the archive can mean two entirely different things:

1. **Unregistered / Non-Committed Transaction:** The transaction was never submitted, was aborted before staging, or was fenced before pre-reservation.
2. **Evicted Historical Tombstone:** The transaction was committed or aborted in a previous epoch, and its tombstone was subsequently evicted after exceeding capacity or aging past the retention window.

### Archive Disposition Consistency Invariant:

$$\mathbf{IsCommitted}(txId) \land \mathbf{IsCancelled}(txId) \equiv \mathbf{FALSE}$$

A transaction cannot simultaneously be committed and cancelled. If a transaction is committed, any subsequent cancellation request must return the cached commit receipt or fail closed with `TRANSACTION_ALREADY_COMMITTED`. If cancelled, commit must fail closed with `TRANSACTION_ALREADY_CANCELLED`.

---

## 6. Commit-Path Fencing & Exact Immutable Retry Identity

### 6.1 Exact Immutable Identity Equality for Idempotent Retries

An idempotent commit retry is recognized if and only if the presented transaction matches the cached committed transaction across all immutable fields:

```typescript
export function isExactImmutableTransactionMatch(
  cached: CommittedTransactionRecord,
  retry: TransactionSubmission,
): boolean {
  if (cached.transactionId !== retry.transactionId) return false;
  if (cached.membershipEpoch !== retry.membershipEpoch) return false;
  if (cached.operationIdentity !== retry.operationIdentity) return false;
  if (cached.payloadDigest !== retry.payloadDigest) return false;
  return true;
}
```

If `retry.transactionId === cached.transactionId` but any other field differs, the coordinator must reject the request with `TRANSACTION_ID_COLLISION_MUTATED_PAYLOAD`.

### 6.2 The Synchronous 9-Step Commit Protocol

To prevent interleaved mutations, `commitRetirementBatch` executes as a single synchronous sequence within one event-loop turn:

1. **Validate Coordinator Authorization:** Verify active coordinator lease epoch.
2. **Verify Monotonic Generation:** Verify submission generation exceeds generation floor.
3. **Check Cancellation Collisions:** Ensure no transaction in the batch is marked cancelled.
4. **Check Existing Commits:** If already committed, verify exact immutable identity match.
5. **Validate Archive Capacity:** Ensure batch size does not exceed remaining capacity.
6. **Pre-Reserve Archive Entries:** Atomically reserve space in the pending commitment table.
7. **Stage Batch Entries:** Copy frozen transaction records into the candidate committed map.
8. **Atomic Reference Replacement:** Swap the internal archive state pointer.
9. **Return Immutable Receipts:** Return frozen `ArchiveDispositionReceipt` instances.

---

## 7. Atomic Commit-versus-Cancellation Arbitration

Arbitration between concurrent commit and cancellation requests for the same transaction ID is resolved via first-write-wins semantics on the internal state map:

- If `commitRetirementBatch` linearizes first, `#committedTransactions.set(txId, record)` succeeds. Any subsequent `cancelTransaction(txId)` reads `#committedTransactions.has(txId)` and throws `TRANSACTION_ALREADY_COMMITTED`.
- If `cancelTransaction` linearizes first, `#cancelledTransactions.set(txId, record)` succeeds. Any subsequent `commitRetirementBatch` throws `TRANSACTION_ALREADY_CANCELLED`.
- Race conditions are impossible because both operations execute synchronously within the single-threaded event loop turn before any asynchronous continuation.

---

## 8. Generation-Floor Scope, Monotonic Ordering, Empty-Cache Semantics & Strict Tombstone Backpressure Recovery

### 8.1 Scope & Monotonicity of Generations

The system enforces strict monotonic ordering of generations:

- **Publication Generation Counter:** `publishedGeneration` tracks the highest generation published to the custody root. Initialized to `0`. Strictly monotonic ($g_{\text{new}} = g_{\text{current}} + 1$).
- **Custody Generation Floor:** `custodyGenerationFloor` represents the generation floor below which all detailed publication results have been safely evicted. Initialized to `0`. Strictly non-decreasing ($\mathbf{custodyGenerationFloor}_{\text{new}} \ge \mathbf{custodyGenerationFloor}_{\text{current}}$).
- **Core Invariant:**
  $$0 \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$

### 8.2 Safe Tombstone Eviction & Generation Ordering Invariants

To bound memory while preventing stale execution:

- Tombstones older than `fencingGenerationFloor` may be evicted if and only if no active reservations reference generations $\le \mathbf{fencingGenerationFloor}$.
- If eviction occurs, the generation floor advances monotonically:
  $$\mathbf{fencingGenerationFloor} = \max(\mathbf{fencingGenerationFloor},\; g_{\text{evicted}})$$

### 8.3 Empty-Cache and Generation-Floor Semantics

A robust distributed recovery engine must define exact invariants when `publicationResults` is empty:

1. **Initial State (Never Published):**
   - At system initialization: `publishedGeneration = 0`, `custodyGenerationFloor = 0`, `publicationResults.size = 0`, `pendingSettlementObligations.size = 0`, `generationAuditHistory.size = 0`, `activeRetentionPins.size = 0`.
   - Any query for $g \le 0$ fails with `INVALID_GENERATION_DOMAIN`.
   - The first valid first-publication generation is $g = 1$ ($1 = 0 + 1$).
2. **Empty Cache After Historical Eviction (Fully Evicted):**
   - Suppose generations $1 \dots N$ were published and subsequently all $N$ detailed results were evicted via safe FIFO eviction.
   - The state is: `publishedGeneration = N`, `custodyGenerationFloor = N`, `publicationResults.size = 0`, `pendingSettlementObligations.size = 0`.
   - The system distinguishes this state from the initial uninitialized state by inspecting `publishedGeneration`:
     - If `publishedGeneration === 0`: Cache is empty because no generation has ever been published.
     - If `publishedGeneration > 0`: Cache is empty because all published generations $1 \dots N$ have been evicted.
3. **Monotonic Floor Immunity Against Republication:**
   - Any retry presented for $g \le \mathbf{custodyGenerationFloor}$ (where $\mathbf{custodyGenerationFloor} = N$) immediately matches the floor check:
     $$g \le \mathbf{custodyGenerationFloor} \implies \text{Reject with } \mathbf{CUSTODY\_RECEIPT\_EXPIRED}$$
   - **Mathematical Proof of First-Publication Immunity:**
     First publication requires $g === \mathbf{publishedGeneration} + 1 = N + 1$.
     Since $g \le \mathbf{custodyGenerationFloor}$ and $\mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$, we have $g \le N < N + 1$.
     Therefore, an evicted generation $g \le \mathbf{custodyGenerationFloor}$ can **never** satisfy the first-publication sequence requirement $g === \mathbf{publishedGeneration} + 1$, guaranteeing it can never re-enter first publication, reacquire custody, or fabricate duplicate obligations.
4. **Seamless Sequential Publication Resumption:**
   - When the cache is completely empty after historical eviction (`custodyGenerationFloor = N`, `publishedGeneration = N`), the next valid first-publication is $g = N + 1$.
   - The first-publication precondition $g === \text{currentRoot.publishedGeneration} + 1$ evaluates to $N + 1 === N + 1 \implies \mathbf{true}$.
   - Candidate root construction sets `candidatePublishedGeneration = N + 1`, leaves `candidateGenerationFloor = N` (floor does not advance on insertion), and inserts the new result at key $N + 1$.
   - The resulting cache contains exactly 1 entry covering $[N+1, N+1]$. Sequential publication proceeds without disruption.

### 8.4 Protection of Active Reservations, No-Active-Reservations Rule & Strict Backpressure Recovery

- If `MAX_TOMBSTONES = 1000` is reached, eviction selects the lowest generation $g$.
- If $g$ belongs to an active reservation, eviction is blocked.
- **Strict FIFO Preservation:** Eviction **cannot** skip active reservations. Skipping would create generation gaps in the tombstone map and violate monotonic floor invariants.
- Instead, the coordinator applies **strict backpressure**: new reservation requests are rejected with `TOMBSTONE_CAPACITY_EXHAUSTED` until active operations complete and release their pins.

---

## 9. Process Failure Model & Monotonic Clock Semantics

### Decoupled Clock Semantics:

To prevent clock skew, leap-second adjustments, and NTP resets from corrupting recovery state:

- **Monotonic Clocks (`performance.now()`):** Used exclusively for timeouts, durations, and state transitions (e.g. `monotonicDequeuedMs`, `monotonicPublishedMs`, `SETTLEMENT_BARRIER_TIMEOUT_MS`).
- **Wall Clocks (`Date.now()`):** Used exclusively for informational logging and telemetry metadata (e.g. `releasedAtTimestamp`). Wall clocks never participate in fencing or arbitration predicates.

---

## 10. Disposition & Lost-Response Semantics

When a destination peer processes a message but the transport connection drops before the response is returned:

- The coordinator does not guess the outcome.
- The coordinator queries the authoritative archive for the transaction ID.
- If the transaction is confirmed committed in the archive (Level 3 Proof), custody reconciliation proceeds via Protocol A.
- If the transaction is confirmed absent/aborted and fenced, custody reconciliation proceeds via Protocol B.
- If the archive state remains unresolved, the recovery gate latches `CUSTODY_RECONCILIATION_BLOCKED` and halts.

---

## 11. Destination Outcome Classification & Post-Commit Promise Rejection

### A. Confirmed Committed (Level 3 Proof)
- Transaction confirmed present in archive `#committedTransactions`.
- Action: Reconcile custody via Protocol A; record `CONFIRMED_COMMITTED`.

### B. Post-Commit Promise Rejection (Execution Failure)
- Destination Promise rejects with execution error (e.g. out of memory, disk failure), but transaction was already committed to archive.
- **Irreversibility Invariant:** The commit cannot be undone. Protocol B restoration is strictly prohibited.
- Action: Settle execution failure as terminal commit receipt; record `CONFIRMED_COMMITTED` with execution warning.

### C. Aborted Fenced: Restored to Source vs. Transferred to Destination (Level 3 Proof)
- Transaction aborted and fenced in archive.
- Action: Reconcile custody via Protocol B. If destination is healthy, transfer operation; otherwise restore to source `QuarantineBuffer`.

### D. Inconclusive or Commit Pending (Level 3 Ambiguity)
- Promise pending or archive lookup inconclusive.
- Action: Maintain drainer custody; do not resolve barrier; await resolution or escalate to `CUSTODY_RECONCILIATION_BLOCKED`.

---

## 12. Complete Custody Authorization, Receipt Boundary, Complete Lifecycle Retry & Bounded Result Retention

### 12.1 Five-Field Ownership Comparison & Equivalence Predicate

To prevent any possibility of ownership spoofing or accidental collision across recovery epochs, ownership validation requires exact equality across all five identifying fields:

```typescript
export interface CustodyOwnershipToken {
  readonly coordinatorInstanceId: string;
  readonly membershipEpoch: number;
  readonly recoveryAttemptId: number;
  readonly leaderLeaseEpoch: number;
  readonly inFlightCustodyFenceNumber: number;
  readonly custodyGeneration: number;
  readonly drainerOwnershipGeneration: number;
  readonly transactionId: string;
  readonly operationIdentity: string;
}

export function isEquivalentOwnerToken(
  a: CustodyOwnershipToken | null | undefined,
  b: CustodyOwnershipToken | null | undefined,
): boolean {
  if (!a || !b) return false;
  return (
    a.coordinatorInstanceId === b.coordinatorInstanceId &&
    a.membershipEpoch === b.membershipEpoch &&
    a.recoveryAttemptId === b.recoveryAttemptId &&
    a.leaderLeaseEpoch === b.leaderLeaseEpoch &&
    a.inFlightCustodyFenceNumber === b.inFlightCustodyFenceNumber
  );
}
```

### 12.2 Authoritative Evidence Model & Qualified Receipt Security Boundary

#### The Documented Package Security Boundary:
Receipts are private brand-validated TypeScript interfaces within the `@syncplay/protocol` boundary. Private symbols and internal brand properties guarantee that external callers cannot fabricate receipts.

#### Explicit Non-Claims & Trust Model Qualification:
All nodes within the cluster run trusted `@syncplay/protocol` code. Protection is designed against distributed concurrency races, stale recovery attempts, split-brain coordinators, and asynchronous timeouts—not against arbitrary in-process memory corruption or hostile code execution within the same JavaScript realm.

### 12.3 Complete Five-Field Custody Identity & Receipt Binding

Every authoritative receipt emitted by the recovery subsystem is bound to the exact 5-field owner token, transaction ID, operation ID, and barrier identity:

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

### 12.4 Discriminated Outcome-Specific Custody Reconciliation

Custody reconciliation produces a discriminated outcome:

- `CustodyReconciliationReceipt_Committed`: Terminal commitment proof; operation retired.
- `CustodyReconciliationReceipt_RestoredToSource`: Restored to source `QuarantineBuffer`.
- `CustodyReconciliationReceipt_TransferredToDestination`: Transferred to destination node.

#### Exclusive Ownership Invariants:
$$\mathbf{ExclusiveOwnership}(op) \implies (\mathbf{InQuarantineBuffer}(op) \oplus \mathbf{InDrainerFlight}(op) \oplus \mathbf{InArchive}(op) \oplus \mathbf{InDestination}(op))$$

### 12.5 Explicit Drainer Custody Release Evidence

Drainer custody release is certified exclusively via `DrainerCustodyReleaseReceipt`. Custody cannot be released implicitly, lazily, or via timeout.

### 12.6 The Final Authoritative Barrier Proof Predicate

A settlement barrier resolves if and only if `assertAuthoritativeSettlementEvidence` validates all four Level 3 receipts:

$$\mathbf{AuthoritativeSettlementEvidence} \equiv \langle \mathbf{ReleaseReceipt},\; \mathbf{ReconciliationReceipt},\; \mathbf{ArchiveReceipt},\; \mathbf{ExecutionReceipt} \rangle$$

All four receipts must match across generation $g$, owner token, transaction ID, and barrier ID.

### 12.7 Closed Generation-Scoped Barrier Lifecycle

#### Lifecycle State Transitions:
1. `ALLOCATED` $\longrightarrow$ 2. `PENDING_SETTLEMENT` $\longrightarrow$ 3. `RESOLVED_WITH_EVIDENCE` (or `LATCHED_BLOCKED`).

Each barrier resolves at most once. A barrier cannot be reused for a different generation.

### 12.8 Separation of the Three Guarantees

1. **Safety:** No duplicate execution; no lost operations; single custody at all times.
2. **Liveness:** Bounded retries; non-blocking settlement under normal conditions; graceful backpressure under saturation.
3. **Auditability:** Complete, immutable, verifiable receipt chain for every generation.

### 12.9 Runtime Immutability of Published Collections

`Object.freeze()` on a native `Map` or `Set` does not prevent calls to `.set()`, `.delete()`, or `.clear()`. Therefore, all published collections are encapsulated in `ImmutableMap` and `ImmutableSet` wrappers whose mutation methods throw `UNSUPPORTED_MUTATION_ON_IMMUTABLE_COLLECTION`.

#### Defensive Handling of Byte Arrays:
All byte arrays, buffers, and payload digests (`Uint8Array`) are defensively copied via `.slice()` before receipt construction and frozen to prevent external mutations from corrupting published state.

### 12.10 Ingress and Egress Nested Evidence Alias Closure

All objects entering `AuthoritativeCustodyRoot` are deeply cloned and frozen on ingress. All objects returned from publication methods are immutable cached references, preventing reference sharing and memory corruption.

### 12.11 Permitted Generation Domain & Sequence Exhaustion Boundary

- Permitted generations: integers $g \in [1, \text{Number.MAX\_SAFE\_INTEGER}]$.
- Out-of-domain values ($g \le 0$, non-integers, NaN, $\infty$) are rejected upfront with `INVALID_GENERATION_DOMAIN`.
- Sequence exhaustion ($g = \text{Number.MAX\_SAFE\_INTEGER}$) triggers fail-closed `GENERATION_SEQUENCE_EXHAUSTED`.

### 12.12 The Complete Lifecycle Publication, Cross-Collection Audit Reconciliation, Operational Retention Pins & Bounded Retry Retention Protocol

#### The Complete Custody Root State Structure:

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

export type RetentionPinType = 'SETTLEMENT_BARRIER' | 'IN_TURN_RETRY';

export interface ImmutableRetentionPin {
  readonly pinId: string; // `${generation}:${operationId}:${pinType}:${nonce}`
  readonly generation: number;
  readonly operationId: string;
  readonly pinType: RetentionPinType;
  readonly ownerToken: CustodyOwnershipToken;
  readonly acquiredAtTimestamp: number;
  readonly monotonicAcquiredMs: number;
}

export interface AuthoritativeCustodyRoot {
  readonly publishedGeneration: number;
  readonly custodyGenerationFloor: number; // Monotonic floor below which detailed results are evicted
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, AuthoritativeSettlementEvidence>;
  readonly activeRetentionPins: ImmutableMap<string, ImmutableRetentionPin>;
}
```

#### Four Authoritative Publication States:
For any requested generation $g$:
1. **Never Published:** $g > \text{currentRoot.publishedGeneration}$.
2. **Published with Settlement Pending:** $g \le \text{currentRoot.publishedGeneration} \land g \in \text{pendingSettlementObligations}$.
3. **Published and Fully Settled:** $g \le \text{currentRoot.publishedGeneration} \land g \notin \text{pendingSettlementObligations} \land g \in \text{publicationResults}$.
4. **Published but Expired Outside Retention Horizon:** $g \le \text{currentRoot.custodyGenerationFloor} \lor (g \le \text{currentRoot.publishedGeneration} \land g \notin \text{publicationResults})$.

#### Cross-Collection Consistency Invariants:
1. **Pending Obligation Result Invariant:**
   $$\forall g \in \mathbf{pendingSettlementObligations},\;\; g \in \mathbf{publicationResults}$$
   A pending obligation cannot exist without its corresponding cached publication result.
2. **Pending Non-Settled Audit Invariant:**
   $$\forall g \in \mathbf{pendingSettlementObligations},\;\; g \notin \mathbf{generationAuditHistory}_{\text{terminal}}$$
   A generation with a pending settlement obligation cannot possess a terminal audit record in `generationAuditHistory`.
3. **Settled Result Mandatory Audit Invariant:**
   $$\forall g \in (\mathbf{publicationResults} \setminus \mathbf{pendingSettlementObligations}),\;\; g \in \mathbf{generationAuditHistory}$$
   A settled generation retained in `publicationResults` **must** possess a corresponding authoritative audit record in `generationAuditHistory`.
4. **Identity Invariant Across Collections:**
   For any generation $g$ present across collections, `obligation.ownerToken`, `result.releaseReceipt.identity`, and `audit.expectedOwner` must match 1-to-1 across all five identity fields, transaction ID, operation ID, and barrier ID.
5. **Monotonic Floor and Contiguous Range Invariant:**
   $$0 \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
   When `publicationResults.size > 0`, `publicationResults` holds the exact contiguous generation sequence:
   $$[\mathbf{custodyGenerationFloor} + 1,\;\; \mathbf{custodyGenerationFloor} + \mathbf{publicationResults.size}]$$
   No pending obligation may exist at or below `custodyGenerationFloor`.

#### Operational Retention-Pin Lifecycle & Strict FIFO Backpressure:
1. **Acquiring Operations:**
   - Settlement barrier executions (`SETTLEMENT_BARRIER`) acquire a pin before initiating external archive or destination I/O, protecting generation $g$'s publication result from eviction while settlement is unresolved.
   - Long-running in-turn retries (`IN_TURN_RETRY`) acquire a pin before executing asynchronous reconciliation.
2. **Pin Identity Binding:**
   - Each pin is bound to generation $g$, `operationId`, `pinType`, the caller's 5-field `ownerToken`, and a unique timestamp/monotonic nonce:
     $$\mathbf{pinId} = `${g}:${operationId}:${pinType}:${nonce}`$$
3. **Atomic Root Reflection:**
   - Both pin acquisition and pin release are atomic transitions of `AuthoritativeCustodyRoot` via candidate-root replacement.
4. **Attempt Fencing:**
   - A pin can only be released by a caller presenting an owner token matching the pin's `ownerToken` across all 5 fields. A stale attempt cannot release a newer attempt's pin.
5. **Pin Release on Settlement:**
   - Upon Promise settlement (fulfillment or rejection) of the guarded operation, the coordinator removes the pin from candidate root and atomically publishes the new root. Release is idempotent.
6. **Watchdog Timeout Pin Immunity:**
   - **CRITICAL DISTRIBUTED SYSTEMS INVARIANT:** A watchdog timeout **CANNOT** release a retention pin whose underlying asynchronous Promise remains unsettled. In JavaScript/TypeScript, promises cannot be cancelled externally; timing out a timer does not abort network/storage I/O. Releasing the pin on timeout would permit FIFO eviction to destroy the result before late-settling I/O finishes, causing state corruption. The pin remains held until the underlying Promise settles or the process terminates.
7. **Strict FIFO Eviction Check:**
   - Eviction identifies the oldest retained generation: $g_{\text{oldest}} = \mathbf{custodyGenerationFloor} + 1$.
   - Eviction checks if any pin exists in `activeRetentionPins` for $g_{\text{oldest}}$.
8. **No Skipping Pinned Entries (Strict FIFO Ordering):**
   - Eviction **CANNOT** skip a pinned oldest entry to evict a newer entry. Skipping would break the single contiguous generation floor.
   - If $g_{\text{oldest}}$ is pinned (or has a pending obligation), eviction applies **strict backpressure**: new publications fail closed with `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`.
9. **Race Immunity:**
   - Pin acquisition and FIFO eviction are synchronous candidate-root operations executed within the event loop turn. A caller cannot acquire a pin on an already-evicted generation because pin acquisition validates $g > \mathbf{custodyGenerationFloor} \land g \in \mathbf{publicationResults}$.

#### Authoritative Count-Only Retention Policy (`MAX_RETAINED_PUBLICATION_RESULTS = 1000`):
- **Core Policy:** Bounded retention of publication results is governed strictly by the authoritative count limit `MAX_RETAINED_PUBLICATION_RESULTS = 1000`.
- **Optional Byte-Budget Extension:** Enforcing a memory byte limit (e.g. 10 MB) is classified as an optional implementation extension. For implementations enabling the optional extension:
  - Accounting uses deterministic UTF-8 serialized byte size of `PublicationResult`.
  - Shared immutable references are counted once per root snapshot.
  - Payloads exceeding `MAX_BYTES` are rejected upfront with `CUSTODY_PUBLICATION_PAYLOAD_TOO_LARGE`.
  - Section 19 tests verify count bounding as mandatory and classify byte tests under the optional extension.

#### Complete Corrected Pseudocode:
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

  // 3. PRIOR PUBLICATION EVALUATION:
  if (g <= currentRoot.publishedGeneration) {
    // Generation was previously published. It can NEVER enter first publication!

    // Case 3A: Published with Settlement Pending
    if (currentRoot.pendingSettlementObligations.has(g)) {
      const existingObligation = currentRoot.pendingSettlementObligations.get(g)!;

      // Invariant Check 1: Obligation must have cached publication result
      const cachedResult = currentRoot.publicationResults.get(g);
      if (!cachedResult) {
        throw new Error('STATE_INCONSISTENT_PENDING_MISSING_RESULT');
      }

      // Invariant Check 2: Obligation must NOT have terminal audit record
      if (currentRoot.generationAuditHistory.has(g)) {
        const audit = currentRoot.generationAuditHistory.get(g)!;
        if (audit.terminalOutcome === 'CONFIRMED_COMMITTED' || audit.terminalOutcome === 'ABORTED_FENCED') {
          throw new Error('STATE_INCONSISTENT_PENDING_ALREADY_AUDITED');
        }
      }

      // Invariant Check 3: Obligation and cached result must agree 1-to-1
      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, cachedResult.releaseReceipt.identity) ||
        existingObligation.transactionId !== cachedResult.releaseReceipt.identity.transactionId ||
        existingObligation.operationIdentity !== cachedResult.releaseReceipt.identity.operationIdentity ||
        existingObligation.barrierIdentity !== cachedResult.obligation.barrierIdentity ||
        existingObligation.generation !== g ||
        cachedResult.releaseReceipt.releasedCustodyGeneration !== g
      ) {
        throw new Error('STATE_INCONSISTENT_PENDING_RESULT_MISMATCH');
      }

      // Pending Retry Attempt Fencing Rule:
      // An idempotent pending retry is an inquiry on an already-published obligation.
      // It validates the caller's token against the immutable obligation identity.
      // It does NOT require safeOwner.recoveryAttemptId === this._currentAttemptId,
      // enabling the original attempt to observe its pending obligation during transition.
      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, safeOwner) ||
        existingObligation.transactionId !== safeOwner.transactionId ||
        existingObligation.operationIdentity !== safeOwner.operationIdentity ||
        existingObligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      // Return the cached original publication result reference (Zero allocation!)
      return cachedResult;
    }

    // Case 3B: Published and Fully Settled (Retained in publicationResults)
    if (currentRoot.publicationResults.has(g)) {
      const cachedResult = currentRoot.publicationResults.get(g)!;

      // Invariant Check 1: Pending obligation must be absent (verified by if-branch)

      // Invariant Check 2: Authoritative audit evidence MUST exist for settled generations
      const auditEvidence = currentRoot.generationAuditHistory.get(g);
      if (!auditEvidence) {
        throw new Error('STATE_INCONSISTENT_SETTLED_AUDIT_MISSING');
      }

      // Invariant Check 3: Audit evidence must agree with cached result
      if (
        !isEquivalentOwnerToken(auditEvidence.expectedOwner, cachedResult.releaseReceipt.identity) ||
        auditEvidence.generation !== g ||
        auditEvidence.transactionId !== cachedResult.releaseReceipt.identity.transactionId ||
        auditEvidence.operationIdentity !== cachedResult.releaseReceipt.identity.operationIdentity ||
        auditEvidence.barrierIdentity !== cachedResult.obligation.barrierIdentity
      ) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RESULT_MISMATCH');
      }

      // Invariant Check 4: Audit outcome must be a recognized terminal state
      if (auditEvidence.terminalOutcome !== 'CONFIRMED_COMMITTED' && auditEvidence.terminalOutcome !== 'ABORTED_FENCED') {
        throw new Error('STATE_INCONSISTENT_AUDIT_TERMINAL_OUTCOME_CONTRADICTION');
      }

      // SETTLED RETRY ATTEMPT INDEPENDENCE RULE:
      // Settled retries DO NOT check safeOwner.recoveryAttemptId === this._currentAttemptId!
      // A settled retry succeeds even if this._currentAttemptId is null or belongs to a later attempt.
      // It validates the complete 5-field owner token (including original recoveryAttemptId)
      // against the immutable cached receipt.
      if (
        !isEquivalentOwnerToken(cachedResult.releaseReceipt.identity, safeOwner) ||
        cachedResult.releaseReceipt.identity.transactionId !== safeOwner.transactionId ||
        cachedResult.releaseReceipt.identity.operationIdentity !== safeOwner.operationIdentity ||
        cachedResult.obligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      // Return the cached original publication result reference (Zero allocation!)
      return cachedResult;
    }

    // Case 3C: Published but Outside Retention Horizon (Evicted / Floor Advanced)
    // Generation was published and settled, but detailed result has expired
    throw new Error('CUSTODY_RECEIPT_EXPIRED');
  }

  // ===========================================================================
  // FIRST PUBLICATION PATH (g === currentRoot.publishedGeneration + 1)
  // ===========================================================================
  // 4. Sequence Continuity Verification
  if (g !== currentRoot.publishedGeneration + 1) {
    throw new Error('GENERATION_OUT_OF_SEQUENCE_GAP_DETECTED');
  }

  // 5. Validate Active In-Flight Owner
  if (!isEquivalentOwnerToken(currentRoot.inFlightRecord?.token ?? null, safeOwner)) {
    throw new Error('STALE_CUSTODY_RELEASE_REJECTED');
  }

  // 6. Validate First-Publication Attempt Fencing: MUST match active coordinator attempt
  if (safeOwner.recoveryAttemptId !== this._currentAttemptId) {
    throw new Error('STALE_ATTEMPT_CUSTODY_RELEASE_REJECTED');
  }

  // 7. Validate Generation Audit History: first publication cannot already be audited
  if (currentRoot.generationAuditHistory.has(g)) {
    throw new Error('GENERATION_AUDIT_ALREADY_COMMITTED');
  }

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
    publishedCustodyGeneration: g,
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

  // 12. Bounded Retention, Retention Pin Check, and Strict FIFO Eviction:
  let candidateFloor = currentRoot.custodyGenerationFloor;
  const candidateResultsMap = new Map(currentRoot.publicationResults.entries());

  if (candidateResultsMap.size >= 1000) {
    // Locate oldest retained generation (must equal candidateFloor + 1)
    let minGen = Number.MAX_SAFE_INTEGER;
    for (const k of candidateResultsMap.keys()) {
      if (k < minGen) minGen = k;
    }

    // Eviction Safety Check 1: Cannot evict if pending settlement obligation
    if (currentRoot.pendingSettlementObligations.has(minGen)) {
      throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    // Eviction Safety Check 2: Cannot evict if active retention pin exists
    for (const pin of currentRoot.activeRetentionPins.values()) {
      if (pin.generation === minGen) {
        // Strict FIFO preservation: CANNOT skip pinned oldest entry!
        // Backpressure must be applied to preserve contiguous monotonic generation floors.
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
      }
    }

    // Safe to evict oldest generation
    candidateResultsMap.delete(minGen);
    candidateFloor = minGen; // Monotonic floor advance
  }

  candidateResultsMap.set(g, publicationResult);

  // 13. Pre-Construct Immutable Candidate Collections & Root
  const candidateObligations = new ImmutableMap([
    ...currentRoot.pendingSettlementObligations.entries(),
    [g, obligation],
  ]);
  const candidateResults = new ImmutableMap(candidateResultsMap.entries());

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: g,
    custodyGenerationFloor: candidateFloor,
    inFlightRecord: null, // Custody released!
    gateState: 'CUSTODY_RELEASED_BARRIER_PENDING',
    pendingSettlementObligations: candidateObligations,
    publicationResults: candidateResults,
    generationAuditHistory: currentRoot.generationAuditHistory,
    activeRetentionPins: currentRoot.activeRetentionPins,
  });

  // 14. THE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;

  // 15. Infallible Return of Pre-Constructed Reference (Zero post-publication allocation):
  return publicationResult;
}

public acquireRetentionPin(
  generation: number,
  operationId: string,
  pinType: RetentionPinType,
  callerToken: CustodyOwnershipToken,
): string {
  const currentRoot = this.#custodyRoot;
  assertValidGenerationDomain(generation);

  // Validate generation is within retained window
  if (generation <= currentRoot.custodyGenerationFloor || !currentRoot.publicationResults.has(generation)) {
    throw new Error('CUSTODY_RECEIPT_EXPIRED');
  }

  const safeOwner = cloneAndFreezeOwnerToken(callerToken);
  const pinId = `${generation}:${operationId}:${pinType}:${performance.now()}`;

  const pin: ImmutableRetentionPin = Object.freeze({
    pinId,
    generation,
    operationId,
    pinType,
    ownerToken: safeOwner,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
  });

  const candidatePins = new ImmutableMap([
    ...currentRoot.activeRetentionPins.entries(),
    [pinId, pin],
  ]);

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    ...currentRoot,
    activeRetentionPins: candidatePins,
  });

  this.#custodyRoot = candidateRoot;
  return pinId;
}

public releaseRetentionPin(pinId: string, callerToken: CustodyOwnershipToken): void {
  const currentRoot = this.#custodyRoot;
  const pin = currentRoot.activeRetentionPins.get(pinId);

  // Idempotent: already released
  if (!pin) return;

  // Stale attempt fencing: caller must match pin's ownerToken
  if (!isEquivalentOwnerToken(pin.ownerToken, callerToken)) {
    throw new Error('STALE_ATTEMPT_PIN_RELEASE_REJECTED');
  }

  const candidatePinsMap = new Map(currentRoot.activeRetentionPins.entries());
  candidatePinsMap.delete(pinId);

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    ...currentRoot,
    activeRetentionPins: new ImmutableMap(candidatePinsMap.entries()),
  });

  this.#custodyRoot = candidateRoot;
}
```

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Under single-realm JavaScript execution:
- Candidate-root preparation, invariant verification, and reference replacement occur synchronously within one microtask turn.
- No other event loop callback can observe or mutate `this.#custodyRoot` between the check and the replacement.
- Retention pin acquisition and FIFO eviction are mutually serialized, preventing race conditions that could expose an evicted result as active.

---

## 12.14 Comprehensive Exception-Safety Matrix Across Publication Phases

| Publication Phase | Failure Mode | Rollback Action | State Invariant Preserved |
|---|---|---|---|
| **Phase 1: Validation** | Out-of-domain generation | Synchronous throw | Root unchanged; no allocation |
| **Phase 2: Ingress Copy** | Deep copy memory failure | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 3: Pre-Allocation**| Object instantiation fail| Synchronous throw | Root unchanged; in-flight retained |
| **Phase 4: Candidate Root**| Map construction fail | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 5: Eviction Check**| Oldest entry pinned | Backpressure throw | Root unchanged; capacity preserved |
| **Phase 6: Atomic Swap** | Pointer replacement | Infallible | Single linearization point |
| **Phase 7: Return Ref** | Return cached pointer | Infallible | Zero post-publication allocation |

---

## 12.15 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

Protocol A linearizes commitment of the operation and settles execution:
1. `commitRetirementBatch` synchronously stages and commits the transaction in the archive.
2. Custody gate transitions to `CUSTODY_RELEASED_BARRIER_PENDING`.
3. Pre-allocated `DrainerCustodyReleaseReceipt` and `PendingSettlementObligation` are published atomically.
4. Barrier resolves with `AuthoritativeSettlementEvidence` containing `CONFIRMED_COMMITTED`.

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

Protocol B handles cancelled or uncommitted operations:
1. Verify Level 3 proof of non-commitment (`TRANSACTION_ABORTED_FENCED`).
2. Atomically fence transaction ID in archive.
3. Transition custody gate to `RECONCILING_CUSTODY`.
4. If destination is healthy, transfer operation; otherwise restore to source buffer.
5. Publish release and pending obligation.
6. Resolve barrier with `ABORTED_FENCED` evidence.

---

## 13. The Custody-Reconciliation Gate State Machine

```
   [ IDLE ]
      │  drainNext()
      ▼
   [ IN_FLIGHT_HELD ] ─────────────────────────┐
      │                                        │
      │ Destination Promise Settles            │ Recovery Watchdog Timeout
      ▼                                        ▼
   [ RECONCILING_CUSTODY ]               [ DEGRADED / TIMED_OUT ]
      │                                        │
      │ publishReleaseAndObligation()          │ Attempt 2 Admitted (Step 4 Wait)
      ▼                                        ▼
   [ CUSTODY_RELEASED_BARRIER_PENDING ]  [ BARRIER_SETTLED ]
      │                                        │
      │ resolveWithEvidence()                  │ Exclusive Resumption
      ▼                                        ▼
   [ TERMINALLY_SETTLED ]                [ IDLE ]
```

---

## 14. Comprehensive Analysis of Cases A through H Across Operational States

- **Case A (Normal Commit):** Promise resolves; archive commits; Protocol A resolves barrier.
- **Case B (Early Rejection):** Promise rejects before staging; Protocol B restores to buffer.
- **Case C (Post-Commit Rejection):** Commit succeeded; Promise rejected; irrevocable commit honored; barrier resolved committed.
- **Case D (Network Split / Timeout):** Watchdog fires; attempt 1 invalidated; barrier held; attempt 2 waits at Step 4; resolves cleanly when I/O completes.
- **Case E (Split-Brain Leader):** Fencing token mismatch; stale leader rejected; active leader reconciles.
- **Case F (Tombstone Capacity Saturation):** Oldest tombstone pinned; backpressure applied; zero generation floor gaps.
- **Case G (Duplicate Retry After Eviction):** Floor advanced past generation; returns `CUSTODY_RECEIPT_EXPIRED`; zero republication.
- **Case H (Delayed Settled Retry After Attempt Change):** Settled retry arrives with Attempt 1 token while Attempt 2 is active or attempt is null; identity matched against cached receipt; returns cached receipt immediately without error.

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| Case | Scenario | Gate State | Custody Owner | Barrier State | Correct Action | Forbidden Action |
|---|---|---|---|---|---|---|
| **1** | Attempt 1 times out; Attempt 2 admitted | `BARRIER_PENDING` | Drainer | PENDING | Attempt 2 waits at Step 4 | Attempt 2 steals custody |
| **2** | Attempt 1 Promise resolves post-timeout | `BARRIER_PENDING` | Drainer | PENDING | Settle barrier; unblock Attempt 2 | Drop Attempt 1 result |
| **3** | Settled retry with null `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Return cached result | Throw stale attempt error |
| **4** | Settled retry with newer `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Return cached result | Throw stale attempt error |
| **5** | Settled retry with mismatched token | `SETTLED` | Archive | RESOLVED | Throw `TOKEN_MISMATCH` | Return cached result |
| **6** | Pending retry with original token | `BARRIER_PENDING` | Drainer | PENDING | Return cached result | Reject retry |
| **7** | Pending retry with conflicting audit | `BARRIER_PENDING` | Drainer | PENDING | Fail closed (corrupted) | Return cached result |
| **8** | Settled retry with missing audit | `SETTLED` | Archive | RESOLVED | Fail closed (missing audit)| Return cached result |
| **9** | First publication with stale attempt | `IN_FLIGHT` | Drainer | UNALLOCATED | Reject stale attempt | Publish candidate root |
| **10** | First publication with out-of-order $g$ | `IN_FLIGHT` | Drainer | UNALLOCATED | Throw generation gap | Advance publishedGeneration|
| **11** | FIFO eviction with pinned oldest entry | `CAPACITY_MAX` | Drainer | PENDING | Throw backpressure | Skip pinned entry |
| **12** | Watchdog timeout on pinned entry | `DEGRADED` | Drainer | PENDING | Maintain retention pin | Release pin on timeout |
| **13** | Pin release by stale attempt | `ANY` | Any | ANY | Reject stale release | Remove pin from root |
| **14** | Pin release on actual settlement | `SETTLED` | Archive | RESOLVED | Remove pin atomically | Leak retention pin |
| **15** | Retry on evicted generation ($g \le \text{floor}$)| `ANY` | Archive | RESOLVED | Throw `RECEIPT_EXPIRED` | Fall through to first pub |
| **16** | First pub after cache completely emptied | `IDLE` | Drainer | UNALLOCATED | Publish $N+1$ cleanly | Reject as out-of-order |
| **17** | Empty cache before any publications | `IDLE` | Drainer | UNALLOCATED | Floor is 0; pub $g=1$ | Allow $g=0$ or $g=2$ |
| **18** | Out-of-domain generation $g \le 0$ | `ANY` | Any | ANY | Throw `INVALID_DOMAIN` | Process publication |
| **19** | Generation sequence exhaustion | `ANY` | Any | ANY | Throw `SEQUENCE_EXHAUSTED`| Wrap around to 0 |
| **20** | Duplicate commit retry with mutated op | `COMMITTED` | Archive | RESOLVED | Throw payload mismatch | Overwrite commit |
| **21** | Concurrent commit and cancel | `RESERVED` | Archive | PENDING | First-write-wins | Allow dual disposition |
| **22** | Byte budget exceeded (optional mode) | `PUBLISHING` | Drainer | PENDING | Throw payload too large | Silently truncate |
| **23** | Destination transfer succeeds | `TRANSFERRED` | Destination | PENDING | Settle barrier Protocol B | Restore to source buffer |
| **24** | Catastrophic proof corruption | `BLOCKED` | Audit Admin | DEGRADED | Latch `RECONCILIATION_BLOCKED` | Optimistic barrier resolution |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{EVIDENCE\_MATCHED}(g)$$
2. **Attempt-Fenced First Publication Invariant:**
   $$\mathbf{FIRST\_PUBLICATION}(g) \implies (\mathbf{callerAttempt} === \mathbf{\_currentAttemptId}) \land (\mathbf{inFlightRecord.token} === \mathbf{callerToken}) \land (g === \mathbf{publishedGeneration} + 1)$$
3. **Settled Retry Attempt Independence Invariant:**
   A settled retry validates exact 1-to-1 token identity against cached evidence and authoritative audit history, completely independent of `_currentAttemptId`.
4. **Cross-Collection Structural Consistency Invariant:**
   Any structural discrepancy among `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory` fails closed.
5. **Operational Retention Pin and Strict FIFO Invariant:**
   A pinned oldest generation CANNOT be evicted; eviction cannot skip pinned entries; capacity exhaustion applies fail-closed backpressure.
6. **Watchdog Pin Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins whose underlying Promises remain unsettled.
7. **Empty-Cache Floor Invariant:**
   $$0 \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
   An evicted generation ($g \le \mathbf{custodyGenerationFloor}$) can never re-enter first publication.
8. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
9. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
10. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.
11. **Inseparable Release/Obligation Invariant:** A released custody state can **never** become observable without its corresponding settlement obligation already present in `this.#custodyRoot.pendingSettlementObligations`.
12. **Runtime Immutability Invariant:** Published collections and evidence references **CANNOT** be mutated through `Map.prototype` or mutable array/buffer aliases.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R16 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution & Settled Retry Independence:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. If Attempt 1 later retries `publishReleaseAndObligation(token1)` to obtain its receipt, the settled retry path recognizes generation $g$ in `publicationResults`, verifies 1-to-1 identity against `token1` and authoritative audit history, and returns the cached receipt immediately—even though `_currentAttemptId` is now 2. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following thirty deterministic future test specifications are formally codified for Sub-Milestone M5.1:

- **Test R18-R16-T01 (Settled Retry Succeeds When `_currentAttemptId` Is Null):** Settle generation $g=1$ under Attempt 1. Set `coordinator._currentAttemptId = null`. Present retry with Attempt 1 token; assert returns original `cachedResult.releaseReceipt` without error.
- **Test R18-R16-T02 (Settled Retry Succeeds When `_currentAttemptId` Belongs to Later Attempt):** Settle generation $g=1$ under Attempt 1. Advance coordinator to Attempt 2 (`_currentAttemptId = 2`). Present retry with Attempt 1 token; assert returns cached receipt cleanly.
- **Test R18-R16-T03 (Settled Retry Rejects Different Original Recovery Attempt Identity):** Settle generation $g=1$ under Attempt 1. Present retry presenting Attempt 2 token for $g=1$; assert throws `CUSTODY_PUBLICATION_TOKEN_MISMATCH`.
- **Test R18-R16-T04 (First Publication Strictly Requires Active Attempt):** Attempt 1 in-flight for $g=1$. Advance coordinator to `_currentAttemptId = 2`. Call `publishReleaseAndObligation` presenting Attempt 1 token; assert throws `STALE_ATTEMPT_CUSTODY_RELEASE_REJECTED`.
- **Test R18-R16-T05 (Pending Retry Follows Explicit Attempt-Fencing Rule):** Publish pending obligation $g=1$ under Attempt 1. Present pending retry with Attempt 1 token while `_currentAttemptId = 2`; assert returns cached receipt without error; assert root is not mutated.
- **Test R18-R16-T06 (Pending Obligation with Missing Result Fails Closed):** Artificially inject pending obligation $g=1$ without corresponding entry in `publicationResults`. Call publish retry for $g=1$; assert throws `STATE_INCONSISTENT_PENDING_MISSING_RESULT`.
- **Test R18-R16-T07 (Pending Obligation with Mismatched Result Identity Fails Closed):** Inject pending obligation $g=1$ with token A, but cached result with token B. Present retry; assert throws `STATE_INCONSISTENT_PENDING_RESULT_MISMATCH`.
- **Test R18-R16-T08 (Pending Obligation with Contradictory Settled Audit Evidence Fails Closed):** Inject pending obligation $g=1$ and inject audit record with `CONFIRMED_COMMITTED`. Present retry; assert throws `STATE_INCONSISTENT_PENDING_ALREADY_AUDITED`.
- **Test R18-R16-T09 (Settled Result with Missing Mandatory Audit Evidence Fails Closed):** Inject settled result $g=1$ into `publicationResults` without entry in `generationAuditHistory`. Present retry; assert throws `STATE_INCONSISTENT_SETTLED_AUDIT_MISSING`.
- **Test R18-R16-T10 (Settled Result with Mismatched Audit Identity Fails Closed):** Inject settled result with token A and audit record with token B. Present retry; assert throws `STATE_INCONSISTENT_AUDIT_RESULT_MISMATCH`.
- **Test R18-R16-T11 (Settled Result with Contradictory Terminal Outcome Fails Closed):** Inject audit record with non-terminal or corrupted status. Present retry; assert throws `STATE_INCONSISTENT_AUDIT_TERMINAL_OUTCOME_CONTRADICTION`.
- **Test R18-R16-T12 (Valid Settled Retry Returns Exact Original Cached Result Reference):** Settle generation $g=1$. Present valid retry; assert returned receipt is referentially identical (`===`) to pre-allocated receipt.
- **Test R18-R16-T13 (Valid Settled Retry Does Not Mutate Authoritative Root):** Capture root reference `rootBefore = coordinator.custodyRoot`. Execute 10 valid settled retries; assert `coordinator.custodyRoot === rootBefore`.
- **Test R18-R16-T14 (Active Settlement Pin Prevents Eviction):** Populate 1,000 entries. Acquire `SETTLEMENT_BARRIER` pin on $g=1$. Attempt first publication of $g=1001$; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`; assert $g=1$ is not evicted.
- **Test R18-R16-T15 (Active Retry Pin Prevents Eviction):** Populate 1,000 entries. Acquire `IN_TURN_RETRY` pin on $g=1$. Attempt publication of $g=1001$; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`.
- **Test R18-R16-T16 (Stale Attempt Cannot Acquire or Release Another Attempt's Pin):** Pin acquired by Attempt 1 token. Attempt 2 calls `releaseRetentionPin`; assert throws `STALE_ATTEMPT_PIN_RELEASE_REJECTED`; assert pin remains held.
- **Test R18-R16-T17 (Watchdog Timeout Does Not Release Pin of Unsettled Promise):** Acquire settlement pin on $g=1$. Fire 5,000 ms watchdog timer; assert coordinator status latches `DEGRADED`; assert `activeRetentionPins.has(pinId)` remains `true`.
- **Test R18-R16-T18 (Actual Settlement Releases Correct Pin):** Complete settlement barrier execution for $g=1$; assert pin is cleanly removed from `activeRetentionPins`.
- **Test R18-R16-T19 (Pin Release Is Idempotent and Generation-Fenced):** Call `releaseRetentionPin` twice with same token; assert second call returns void without error or root corruption.
- **Test R18-R16-T20 (Pinned Oldest FIFO Entry Triggers Documented Backpressure):** 1,000 entries occupied; oldest entry $g=1$ pinned; newer entries $g=2 \dots 1000$ unpinned. Attempt publication of $g=1001$; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`; assert eviction does NOT skip $g=1$.
- **Test R18-R16-T21 (No Eviction Occurs Before Pin and Settlement Checks Complete):** Construct publication call where evidence verification fails; assert `publicationResults` size and `custodyGenerationFloor` are completely unchanged.
- **Test R18-R16-T22 (Concurrent Pin Acquisition and Eviction Serialize Cleanly):** Attempt pin acquisition on generation currently being evaluated for eviction; assert either pin succeeds and blocks eviction, or eviction succeeds and pin throws `CUSTODY_RECEIPT_EXPIRED`.
- **Test R18-R16-T23 (Count Limit Remains Authoritatively Bounded at 1,000):** Continuously publish and settle 2,500 generations with unpinned oldest entries; assert `publicationResults.size <= 1000` at every single step; assert `custodyGenerationFloor == 1500`.
- **Test R18-R16-T24 (Byte-Budget Behavior Matches Selected Optional Policy):** When optional byte budget is disabled, assert large payloads succeed up to count 1,000. When enabled, assert byte accounting triggers eviction at threshold.
- **Test R18-R16-T25 (Oversized Result Follows Specified Policy):** In optional byte-budget mode, pass receipt payload exceeding maximum byte budget; assert throws `CUSTODY_PUBLICATION_PAYLOAD_TOO_LARGE`.
- **Test R18-R16-T26 (Empty-Cache Floor Invariant Holds Before Any Publication):** At uninitialized state, assert `publishedGeneration === 0`, `custodyGenerationFloor === 0`, `publicationResults.size === 0`.
- **Test R18-R16-T27 (Empty-Cache Floor Invariant Holds After Complete Historical Eviction):** Evict all retained results until cache is empty; assert `publishedGeneration === N`, `custodyGenerationFloor === N`, `publicationResults.size === 0`.
- **Test R18-R16-T28 (Evicted Generation Cannot Re-Enter First Publication):** For evicted $g \le \mathbf{custodyGenerationFloor}$, call first publication; assert throws `CUSTODY_RECEIPT_EXPIRED` (or sequence gap); assert never treated as unpublished.
- **Test R18-R16-T29 (Sequential Publication Remains Possible After Complete Safe Eviction):** From fully evicted cache ($g_{\text{floor}} = N$, $g_{\text{pub}} = N$), publish $g = N + 1$; assert succeeds cleanly; assert new cache size is 1.
- **Test R18-R16-T30 (Existing R18-R14/R15 Alias-Closure, Defensive Byte-Copy, and Generation-Domain Tests Remain Required):** Execute full suite of defensive byte-copy, prototype freeze, and generation domain validation tests; assert zero regressions.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R16):
================================================================================
1. [SETTLED RETRY ATTEMPT INDEPENDENCE]: Does the Engineering Authority approve
   decoupling settled publication retries from the coordinator's current recovery
   attempt (_currentAttemptId), allowing valid retries to succeed when _currentAttemptId
   is null or belongs to a later attempt, while maintaining strict attempt fencing
   for first publications?

2. [CROSS-COLLECTION AUDIT RECONCILIATION]: Does the Engineering Authority approve
   enforcing structural consistency checks across pending obligations, cached
   results, and authoritative audit history, failing closed on missing evidence,
   identity mismatch, or contradictory terminal outcomes?

3. [OPERATIONAL RETENTION PINS & STRICT FIFO BACKPRESSURE]: Does the Engineering
   Authority approve the formal activeRetentionPins model, requiring eviction to
   apply fail-closed backpressure when the oldest FIFO generation is pinned,
   strictly prohibiting skipping pinned entries to preserve contiguous generation floors?

4. [WATCHDOG TIMEOUT PIN IMMUNITY]: Does the Engineering Authority approve the rule
   that a watchdog timeout cannot release a retention pin whose underlying
   asynchronous Promise remains unsettled, preventing premature FIFO eviction?

5. [AUTHORITATIVE COUNT BOUND & EMPTY-CACHE FLOOR SEMANTICS]: Does the Engineering
   Authority approve establishing count-only retention (1,000 entries) as the
   authoritative requirement (with byte budgeting as an optional extension),
   and formalizing empty-cache generation-floor semantics?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R16 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R16 proposal completes the formal recovery arbitration architecture. It resolves attempt coupling for settled retries, enforces cross-collection audit reconciliation, makes retention pins operational with strict FIFO backpressure, guarantees watchdog pin immunity, confirms authoritative count-only retention, and defines rigorous empty-cache generation-floor invariants.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R16 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
