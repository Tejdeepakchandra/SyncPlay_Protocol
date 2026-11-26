# Phase 7.5 M5.0-R18-R18 — Atomic Retry Pinning, Exact Retention Contiguity, Tombstone Publication & Settlement Evidence Provenance
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `7e200f2b36d7d2a127464f11fbd9626420c882d3` (M5.0-R18-R17)
**Direct Parent Commit:** `7e200f2b36d7d2a127464f11fbd9626420c882d3`
**Authorization Scope:** M5.0-R18-R18 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R18 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R17, we introduced capability-gated settlement-pin release and defined a 12-state audit matrix. However, an exhaustive architectural audit identified four remaining critical blockers across retry pinning, retention contiguity, tombstone lifecycles, and evidence provenance:

1. **Unpinned Retry Return Gap:** The R18-R17 pending and settled retry branches returned `cachedResult` without actually acquiring an `IN_TURN_RETRY` pin in `AuthoritativeCustodyRoot`. This exposed an interval where a retry caller received a receipt while the underlying entry remained unprotected in the root, leaving it vulnerable to eviction before the caller completed its turn.
2. **Key-Size Contiguity Fallacy:** R18-R17 asserted contiguity by checking `resultsMap.size === publishedGeneration - custodyGenerationFloor` only when nonempty. Equal size does not prove that the map contains the exact sequence of contiguous keys; a map with missing or out-of-range keys of equal count would pass undetected. Furthermore, corrupted empty states were left unvalidated.
3. **Missing Tombstone Publication on Eviction:** R18-R17 introduced `ImmutableCompactAuditTombstone`, but its eviction pseudocode deleted results without creating or publishing tombstones into `generationAuditHistory`. Consequently, evicted results lost their terminal audit backing, and lookups below the floor returned `CUSTODY_RECEIPT_EXPIRED` without verifying tombstone existence within the retention horizon.
4. **Unspecified Evidence Provenance & Validation:** R18-R17 required `AuthoritativeSettlementEvidence` for settlement-pin release, but did not define the trusted component authorized to issue evidence, how facts are verified against authoritative state, single-use vs. idempotent replay bounds, or the exact failure semantics when Promise rejection occurs without authoritative custody reconciliation.

### Core Architectural Guarantees Established in R18-R18:

- **Atomic Retry Pin Acquisition in Candidate Root:** Pending and settled retry paths validate full publication identity, atomically acquire an `IN_TURN_RETRY` pin in the candidate root, and linearize via reference swap *before* returning the cached result and pin handle. Zero unpinned interval exists.
- **Exact Unconditional Retention Contiguity:** Formally defines and validates $\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1, \dots, \mathbf{publishedGeneration}\}$. Validates every key unconditionally before eviction and publication, rejecting key gaps, out-of-range keys, and empty-range corruptions. Pre-existing capacity violations fail closed with backpressure without bulk eviction.
- **Atomic Tombstone Publication & Bounded Horizon:** When $g_{\text{oldest}}$ is evicted from `publicationResults`, an `ImmutableCompactAuditTombstone` is constructed and published into `generationAuditHistory` in the same atomic candidate root. Lookups within the tombstone horizon ($\mathbf{auditTombstoneFloor} < g \le \mathbf{custodyGenerationFloor}$) verify tombstone identity; missing or mismatched tombstones fail closed as state corruption.
- **Strict Settlement Evidence Provenance & Validation:** The internal `SettlementEvidenceFactory` is established as the sole authorized issuer of Level 3 evidence. Evidence is cryptographically and monotonically bound across all 6 identity dimensions and verified against authoritative coordinator state before settlement pin release. Watchdog timeouts and unclassified rejections cannot authorize release.
- **Exhaustive Deterministic Test Suite (30 Test Cases):** Codifies thirty deterministic future test specifications in Section 19 covering atomic retry pinning, exact contiguity, tombstone publication, and evidence provenance.

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
│      auditTombstoneFloor, inFlightRecord, gateState,                        │
│      pendingSettlementObligations, publicationResults,                      │
│      generationAuditHistory, and activeRetentionPins.                       │
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
  - Together, these constitute `AuthoritativeSettlementEvidence`, which is required to resolve a settlement barrier and authorize `SETTLEMENT_BARRIER` pin release.

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
- **Audit Tombstone Floor:** `auditTombstoneFloor` represents the floor below which compact audit tombstones have been safely evicted. Initialized to `0`. Strictly non-decreasing ($\mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$).
- **Core Invariant:**
  $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$

### 8.2 Safe Tombstone Eviction & Generation Ordering Invariants

To bound memory while preventing stale execution:

- Tombstones older than `fencingGenerationFloor` may be evicted if and only if no active reservations reference generations $\le \mathbf{fencingGenerationFloor}$.
- If eviction occurs, the generation floor advances monotonically:
  $$\mathbf{fencingGenerationFloor} = \max(\mathbf{fencingGenerationFloor},\; g_{\text{evicted}})$$

### 8.3 Empty-Cache and Generation-Floor Semantics

A robust distributed recovery engine must define exact invariants when `publicationResults` is empty:

1. **Initial State (Never Published):**
   - At system initialization: `publishedGeneration = 0`, `custodyGenerationFloor = 0`, `auditTombstoneFloor = 0`, `publicationResults.size = 0`, `pendingSettlementObligations.size = 0`, `generationAuditHistory.size = 0`, `activeRetentionPins.size = 0`.
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
     $$g \le \mathbf{custodyGenerationFloor} \implies \text{Evaluated against tombstone horizon}$$
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

### 12.6 The Final Authoritative Barrier Proof Predicate & Evidence Provenance

A settlement barrier resolves if and only if `assertAuthoritativeSettlementEvidence` validates all four Level 3 receipts issued by the trusted internal `SettlementEvidenceFactory`:

$$\mathbf{AuthoritativeSettlementEvidence} \equiv \langle \mathbf{ReleaseReceipt},\; \mathbf{ReconciliationReceipt},\; \mathbf{ArchiveReceipt},\; \mathbf{ExecutionReceipt} \rangle$$

All four receipts must match across generation $g$, owner token, transaction ID, operation ID, barrier ID, and drainer ownership generation.

#### Evidence Issuance Contract:
- **Sole Authorized Issuer:** `SettlementEvidenceFactory` (internal to `@syncplay/protocol`).
- **Required Verified Facts:**
  1. The underlying asynchronous Promise has actually settled (resolved or rejected post-commit).
  2. The settlement outcome is classified (`CONFIRMED_COMMITTED` or `ABORTED_FENCED`).
  3. Archive disposition is committed or fenced in authoritative tables.
  4. Custody reconciliation is complete.
  5. Drainer custody is relinquished.
- **Verification Against Coordinator State:** The factory checks internal records directly; it never accepts caller-supplied boolean claims.
- **Replay Protection:** Evidence is single-use for resolving the specific barrier and releasing the specific `SETTLEMENT_BARRIER` pin for generation $g$. It cannot be applied to another generation or obligation.

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

### 12.12 The Complete Lifecycle Publication, Bidirectional Audit Reconciliation, Atomic Retry Pinning & Exact Contiguity

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

export interface PublicationResponse {
  readonly publicationResult: PublicationResult;
  readonly retryPinId: string | null; // Non-null on retries; null on first publication
}

export type RetentionPinType = 'SETTLEMENT_BARRIER' | 'IN_TURN_RETRY';

export interface ImmutableRetentionPin {
  readonly pinId: string; // pin:${coordinatorInstanceId}:${generation}:${operationId}:${pinType}:${seq}
  readonly generation: number;
  readonly operationId: string;
  readonly pinType: RetentionPinType;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly barrierIdentity: string;
  readonly acquiredAtTimestamp: number;
  readonly monotonicAcquiredMs: number;
}

export interface ImmutableCompactAuditTombstone {
  readonly generation: number;
  readonly expectedOwner: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly terminalOutcome: 'CONFIRMED_COMMITTED' | 'ABORTED_FENCED';
  readonly settledAtTimestamp: number;
  readonly monotonicSettledMs: number;
  readonly evidenceDigest: Uint8Array; // SHA-256 digest of AuthoritativeSettlementEvidence
}

export interface AuthoritativeCustodyRoot {
  readonly publishedGeneration: number;
  readonly custodyGenerationFloor: number; // Floor below which results are evicted
  readonly auditTombstoneFloor: number;    // Floor below which tombstones are evicted
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, AuthoritativeSettlementEvidence | ImmutableCompactAuditTombstone>;
  readonly activeRetentionPins: ImmutableMap<string, ImmutableRetentionPin>;
}
```

#### The Complete 12-State Bidirectional Audit Reconciliation Matrix:

| State | `pendingObligations` | `publicationResults` | `generationAuditHistory` | Valid? | Retry Action | Expired? | Authoritative Engine Action |
|---|---|---|---|---|---|---|---|
| **1** | Absent ($g > \text{pubGen}$) | Absent | Absent | **VALID** | None | No | Eligible for first publication if $g === \text{publishedGeneration} + 1$; sequence gap if higher. |
| **2** | Present | Present | Absent (or In-Prog) | **VALID** | Pending Retry | No | Valid pending obligation. Acquires retry pin; returns cached result. Invariant: audit terminal status MUST NOT exist. |
| **3** | Absent | Present | Present (Terminal) | **VALID** | Settled Retry | No | Valid settled generation. Acquires retry pin; returns cached result. Invariant: audit identity MUST match 1-to-1. |
| **4** | Absent | Absent (Evicted) | Present (Tombstone) | **VALID** | Expired | **YES** | Legitimate historical eviction ($\text{auditFloor} < g \le \text{custodyFloor}$). Returns `CUSTODY_RECEIPT_EXPIRED` with tombstone. |
| **5** | Absent | Absent (Evicted) | Absent (Past Horiz) | **VALID** | Expired | **YES** | Historical eviction past tombstone window ($g \le \text{auditFloor} \le \text{pubGen}$). Returns `CUSTODY_RECEIPT_EXPIRED`. |
| **6** | Present | Absent | Any | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_ORPHANED_PENDING_OBLIGATION`. Latches degraded; alerts operator. |
| **7** | Absent ($g > \text{pubGen}$) | Absent | Present | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_FUTURE_GENERATION_AUDITED`. Fails closed. |
| **8** | Absent | Present | Absent | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT`. Fails closed. |
| **9** | Present | Present | Present (Terminal) | **INVALID**| Fail Closed | No | State contradiction: `STATE_INCONSISTENT_PENDING_OBLIGATION_CONTRADICTS_AUDIT`. Fails closed. |
| **10**| Any | Present | Present (Mismatched)| **INVALID**| Fail Closed | No | Identity corruption: `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`. Fails closed. |
| **11**| Any | Present | Present (Conflicting)|**INVALID**| Fail Closed | No | Outcome conflict: `STATE_INCONSISTENT_TERMINAL_OUTCOME_CONFLICT`. Fails closed. |
| **12**| Any | Any | Duplicate/Conflicting|**INVALID**| Fail Closed | No | Audit corruption: `STATE_INCONSISTENT_DUPLICATE_AUDIT_RECORD`. Fails closed. |

#### Atomic Retry Pin Acquisition Protocol:

To eliminate the unprotected return gap identified in R18-R17:
1. When a pending or settled retry is recognized, the coordinator validates all identity dimensions.
2. The coordinator checks `currentRoot.activeRetentionPins`:
   - If a matching pin for `(generation, operationId, 'IN_TURN_RETRY')` already exists with identical parameters, it is reused.
   - If conflicting parameters are presented, it fails closed with `RETENTION_PIN_COLLISION_MUTATED_IDENTITY`.
   - If no pin exists, a new monotonic `pinId` is allocated and added to candidate pins.
3. Candidate root with the new pin is published via synchronous reference swap:
   `this.#custodyRoot = candidateRoot;`
4. The method returns `PublicationResponse` containing both the cached publication result and the active `retryPinId`.
5. The retry entry is authoritatively protected in the root *before* the caller receives the receipt reference.
6. Upon turn completion (fulfillment, rejection, cancellation, or timeout), the caller releases the pin via `releaseRetryPin(retryPinId, callerToken)`.

#### Exact Unconditional Retention-Map Contiguity:

The invariant:
$$\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1,\;\; \mathbf{custodyGenerationFloor} + 2,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
is validated unconditionally via `assertRetentionMapContiguity`:
- If `custodyGenerationFloor === publishedGeneration`: map size MUST be 0.
- If `custodyGenerationFloor < publishedGeneration`: map size MUST equal `publishedGeneration - custodyGenerationFloor`, and every integer $g \in [\text{floor} + 1, \text{publishedGeneration}]$ MUST exist in the map with matching generation metadata.
- Any key gap, out-of-range key, or size mismatch throws fail-closed without mutating roots.
- If `publicationResults.size > 1000` prior to publication, the coordinator throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED` rather than performing unmonitored bulk eviction.

#### Atomic Tombstone Creation on FIFO Eviction:

When eviction of $g_{\text{oldest}} = \mathbf{custodyGenerationFloor} + 1$ occurs:
1. `candidateResultsMap.delete(g_oldest)`.
2. The coordinator retrieves the terminal settlement evidence for $g_{\text{oldest}}$ from `currentRoot.generationAuditHistory`.
3. An `ImmutableCompactAuditTombstone` is constructed with SHA-256 evidence digest.
4. `candidateAuditHistory.set(g_oldest, tombstone)`.
5. `candidateFloor = g_oldest`.
All four updates are applied atomically in the candidate root before the publication swap.

#### Complete Corrected Pseudocode:

```typescript
public assertRetentionMapContiguity(
  resultsMap: Map<number, PublicationResult> | ImmutableMap<number, PublicationResult>,
  floor: number,
  publishedGeneration: number,
): void {
  if (!Number.isSafeInteger(floor) || floor < 0) {
    throw new Error('STATE_INCONSISTENT_INVALID_FLOOR');
  }
  if (!Number.isSafeInteger(publishedGeneration) || publishedGeneration < 0) {
    throw new Error('STATE_INCONSISTENT_INVALID_PUBLISHED_GENERATION');
  }
  if (floor > publishedGeneration) {
    throw new Error('STATE_INCONSISTENT_FLOOR_EXCEEDS_PUBLISHED_GENERATION');
  }

  // Empty state check
  if (floor === publishedGeneration) {
    if (resultsMap.size !== 0) {
      throw new Error('STATE_INCONSISTENT_EMPTY_RANGE_NON_EMPTY_MAP');
    }
    return;
  }

  // Non-empty range check
  const expectedCount = publishedGeneration - floor;
  if (resultsMap.size !== expectedCount) {
    throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
  }

  for (let g = floor + 1; g <= publishedGeneration; g++) {
    if (!resultsMap.has(g)) {
      throw new Error(`STATE_INCONSISTENT_RESULT_KEY_GAP_DETECTED: missing generation ${g}`);
    }
    const record = resultsMap.get(g)!;
    if (record.releaseReceipt.releasedCustodyGeneration !== g || record.obligation.generation !== g) {
      throw new Error(`STATE_INCONSISTENT_RESULT_GENERATION_MISMATCH: key ${g}`);
    }
  }

  for (const k of resultsMap.keys()) {
    if (!Number.isSafeInteger(k) || k <= floor || k > publishedGeneration) {
      throw new Error(`STATE_INCONSISTENT_RESULT_GENERATION_OUT_OF_BOUNDS: key ${k}`);
    }
  }
}

public publishReleaseAndObligation(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
  barrierIdentity: string,
): PublicationResponse {
  const currentRoot = this.#custodyRoot;

  // 1. Generation Domain Validation
  const g = expectedOwner.custodyGeneration;
  assertValidGenerationDomain(g);

  // 2. Ingress Defensive Normalization
  const safeOwner = cloneAndFreezeOwnerToken(expectedOwner);
  const safeBarrierId = String(barrierIdentity);
  const safeTxId = String(safeOwner.transactionId);
  const safeOpId = String(safeOwner.operationIdentity);

  // 3. PRIOR PUBLICATION EVALUATION:
  if (g <= currentRoot.publishedGeneration) {
    // Case 3A: Published with Settlement Pending (State 2)
    if (currentRoot.pendingSettlementObligations.has(g)) {
      const existingObligation = currentRoot.pendingSettlementObligations.get(g)!;

      const cachedResult = currentRoot.publicationResults.get(g);
      if (!cachedResult) throw new Error('STATE_INCONSISTENT_ORPHANED_PENDING_OBLIGATION');

      if (currentRoot.generationAuditHistory.has(g)) {
        const audit = currentRoot.generationAuditHistory.get(g)!;
        if (audit.terminalOutcome === 'CONFIRMED_COMMITTED' || audit.terminalOutcome === 'ABORTED_FENCED') {
          throw new Error('STATE_INCONSISTENT_PENDING_OBLIGATION_CONTRADICTS_AUDIT');
        }
      }

      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, cachedResult.releaseReceipt.identity) ||
        existingObligation.transactionId !== cachedResult.releaseReceipt.identity.transactionId ||
        existingObligation.operationIdentity !== cachedResult.releaseReceipt.identity.operationIdentity ||
        existingObligation.barrierIdentity !== cachedResult.obligation.barrierIdentity ||
        existingObligation.generation !== g ||
        cachedResult.releaseReceipt.releasedCustodyGeneration !== g
      ) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH');
      }

      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, safeOwner) ||
        existingObligation.transactionId !== safeTxId ||
        existingObligation.operationIdentity !== safeOpId ||
        existingObligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      // ATOMIC RETRY PIN ACQUISITION:
      const { candidatePins, pinId } = this._getOrAllocateRetryPin(
        currentRoot, g, safeOpId, safeOwner, safeTxId, safeBarrierId
      );

      if (candidatePins !== currentRoot.activeRetentionPins) {
        this.#custodyRoot = Object.freeze({
          ...currentRoot,
          activeRetentionPins: candidatePins,
        });
      }

      return { publicationResult: cachedResult, retryPinId: pinId };
    }

    // Case 3B: Published and Fully Settled (State 3)
    if (currentRoot.publicationResults.has(g)) {
      const cachedResult = currentRoot.publicationResults.get(g)!;

      const auditEvidence = currentRoot.generationAuditHistory.get(g);
      if (!auditEvidence) {
        throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');
      }

      if (
        !isEquivalentOwnerToken(auditEvidence.expectedOwner, cachedResult.releaseReceipt.identity) ||
        auditEvidence.generation !== g ||
        auditEvidence.transactionId !== cachedResult.releaseReceipt.identity.transactionId ||
        auditEvidence.operationIdentity !== cachedResult.releaseReceipt.identity.operationIdentity ||
        auditEvidence.barrierIdentity !== cachedResult.obligation.barrierIdentity
      ) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH');
      }

      if (auditEvidence.terminalOutcome !== 'CONFIRMED_COMMITTED' && auditEvidence.terminalOutcome !== 'ABORTED_FENCED') {
        throw new Error('STATE_INCONSISTENT_TERMINAL_OUTCOME_CONFLICT');
      }

      // Settled retry attempt independence: caller does not check safeOwner.recoveryAttemptId === this._currentAttemptId
      if (
        !isEquivalentOwnerToken(cachedResult.releaseReceipt.identity, safeOwner) ||
        cachedResult.releaseReceipt.identity.transactionId !== safeTxId ||
        cachedResult.releaseReceipt.identity.operationIdentity !== safeOpId ||
        cachedResult.obligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      // ATOMIC RETRY PIN ACQUISITION:
      const { candidatePins, pinId } = this._getOrAllocateRetryPin(
        currentRoot, g, safeOpId, safeOwner, safeTxId, safeBarrierId
      );

      if (candidatePins !== currentRoot.activeRetentionPins) {
        this.#custodyRoot = Object.freeze({
          ...currentRoot,
          activeRetentionPins: candidatePins,
        });
      }

      return { publicationResult: cachedResult, retryPinId: pinId };
    }

    // Case 3C: Published but Outside Retention Horizon (Evicted / States 4 & 5)
    if (g <= currentRoot.custodyGenerationFloor) {
      if (g > currentRoot.auditTombstoneFloor) {
        // Within promised tombstone horizon: verify compact audit tombstone
        const tombstone = currentRoot.generationAuditHistory.get(g) as ImmutableCompactAuditTombstone | undefined;
        if (!tombstone) {
          throw new Error('STATE_INCONSISTENT_TOMBSTONE_MISSING_IN_HORIZON');
        }
        if (
          !isEquivalentOwnerToken(tombstone.expectedOwner, safeOwner) ||
          tombstone.transactionId !== safeTxId ||
          tombstone.operationIdentity !== safeOpId ||
          tombstone.barrierIdentity !== safeBarrierId
        ) {
          throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
        }
      }
      throw new Error('CUSTODY_RECEIPT_EXPIRED');
    }

    throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
  }

  // ===========================================================================
  // FIRST PUBLICATION PATH (g === currentRoot.publishedGeneration + 1)
  // ===========================================================================
  if (g !== currentRoot.publishedGeneration + 1) {
    throw new Error('GENERATION_OUT_OF_SEQUENCE_GAP_DETECTED');
  }

  if (!isEquivalentOwnerToken(currentRoot.inFlightRecord?.token ?? null, safeOwner)) {
    throw new Error('STALE_CUSTODY_RELEASE_REJECTED');
  }

  if (safeOwner.recoveryAttemptId !== this._currentAttemptId) {
    throw new Error('STALE_ATTEMPT_CUSTODY_RELEASE_REJECTED');
  }

  if (currentRoot.generationAuditHistory.has(g)) {
    throw new Error('STATE_INCONSISTENT_FUTURE_GENERATION_AUDITED');
  }

  // Pre-eviction unconditional contiguity assertion on current map
  this.assertRetentionMapContiguity(
    currentRoot.publicationResults,
    currentRoot.custodyGenerationFloor,
    currentRoot.publishedGeneration
  );

  const safeEvidence = cloneAndFreezeEvidence(preparedEvidence, safeOwner);
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

  const obligation: PendingSettlementObligation = Object.freeze({
    obligationId: `obl-${safeOwner.custodyGeneration}-${Date.now()}`,
    generation: safeOwner.custodyGeneration,
    barrierIdentity: safeBarrierId,
    ownerToken: safeOwner,
    transactionId: safeTxId,
    operationIdentity: safeOpId,
    drainerOwnershipGeneration: safeOwner.drainerOwnershipGeneration,
    publishedCustodyGeneration: g,
    preAllocatedReleaseReceipt: releaseReceipt,
    preparedReconciliationReceipt: safeEvidence.custodyReconciliation,
    preparedArchiveReceipt: safeEvidence.archiveDisposition,
    preparedExecutionReceipt: safeEvidence.executionSettlement,
    publishedAtTimestamp: Date.now(),
    monotonicPublishedMs: monotonicNow,
  });

  const publicationResult: PublicationResult = Object.freeze({
    releaseReceipt,
    obligation,
  });

  // Strict FIFO Contiguity Verification, Eviction & Atomic Tombstone Publication
  let candidateFloor = currentRoot.custodyGenerationFloor;
  const candidateResultsMap = new Map(currentRoot.publicationResults.entries());
  const candidateAuditHistory = new Map(currentRoot.generationAuditHistory.entries());

  if (candidateResultsMap.size >= 1000) {
    if (candidateResultsMap.size > 1000) {
      throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    const g_oldest = candidateFloor + 1;

    if (!candidateResultsMap.has(g_oldest)) {
      throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
    }

    if (currentRoot.pendingSettlementObligations.has(g_oldest)) {
      throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    for (const pin of currentRoot.activeRetentionPins.values()) {
      if (pin.generation === g_oldest) {
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
      }
    }

    // Atomic Tombstone Creation
    const evictedResult = candidateResultsMap.get(g_oldest)!;
    const existingAudit = candidateAuditHistory.get(g_oldest) as AuthoritativeSettlementEvidence;
    if (!existingAudit) {
      throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');
    }

    const tombstone: ImmutableCompactAuditTombstone = Object.freeze({
      generation: g_oldest,
      expectedOwner: existingAudit.expectedOwner,
      transactionId: existingAudit.transactionId,
      operationIdentity: existingAudit.operationIdentity,
      barrierIdentity: existingAudit.barrierIdentity,
      terminalOutcome: existingAudit.terminalOutcome,
      settledAtTimestamp: existingAudit.settledAtTimestamp,
      monotonicSettledMs: existingAudit.monotonicSettledMs,
      evidenceDigest: computeEvidenceDigest(existingAudit),
    });

    candidateResultsMap.delete(g_oldest);
    candidateAuditHistory.set(g_oldest, tombstone);
    candidateFloor = g_oldest;
  }

  candidateResultsMap.set(g, publicationResult);

  // Post-insertion unconditional contiguity assertion on candidate map
  this.assertRetentionMapContiguity(candidateResultsMap, candidateFloor, g);

  const candidateObligations = new ImmutableMap([
    ...currentRoot.pendingSettlementObligations.entries(),
    [g, obligation],
  ]);
  const candidateResults = new ImmutableMap(candidateResultsMap.entries());
  const candidateAudit = new ImmutableMap(candidateAuditHistory.entries());

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: g,
    custodyGenerationFloor: candidateFloor,
    auditTombstoneFloor: currentRoot.auditTombstoneFloor,
    inFlightRecord: null, // Custody released!
    gateState: 'CUSTODY_RELEASED_BARRIER_PENDING',
    pendingSettlementObligations: candidateObligations,
    publicationResults: candidateResults,
    generationAuditHistory: candidateAudit,
    activeRetentionPins: currentRoot.activeRetentionPins,
  });

  // THE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;
  return { publicationResult, retryPinId: null };
}

private _getOrAllocateRetryPin(
  root: AuthoritativeCustodyRoot,
  generation: number,
  operationId: string,
  ownerToken: CustodyOwnershipToken,
  transactionId: string,
  barrierIdentity: string,
): { candidatePins: ImmutableMap<string, ImmutableRetentionPin>; pinId: string } {
  for (const existingPin of root.activeRetentionPins.values()) {
    if (
      existingPin.generation === generation &&
      existingPin.operationId === operationId &&
      existingPin.pinType === 'IN_TURN_RETRY'
    ) {
      if (
        isEquivalentOwnerToken(existingPin.ownerToken, ownerToken) &&
        existingPin.transactionId === transactionId &&
        existingPin.barrierIdentity === barrierIdentity
      ) {
        return { candidatePins: root.activeRetentionPins, pinId: existingPin.pinId };
      }
      throw new Error('RETENTION_PIN_COLLISION_MUTATED_IDENTITY');
    }
  }

  const seq = ++this._pinSequenceCounter;
  const pinId = `pin:${this._coordinatorInstanceId}:${generation}:${operationId}:IN_TURN_RETRY:${seq}`;

  if (root.activeRetentionPins.has(pinId)) {
    throw new Error('RETENTION_PIN_ID_COLLISION');
  }

  const pin: ImmutableRetentionPin = Object.freeze({
    pinId,
    generation,
    operationId,
    pinType: 'IN_TURN_RETRY',
    ownerToken,
    transactionId,
    barrierIdentity,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
  });

  const candidatePins = new ImmutableMap([
    ...root.activeRetentionPins.entries(),
    [pinId, pin],
  ]);

  return { candidatePins, pinId };
}
```

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Under single-realm JavaScript execution:
- Candidate-root preparation, invariant verification, and reference replacement occur synchronously within one microtask turn.
- No other event loop callback can observe or mutate `this.#custodyRoot` between the check and the replacement.
- Retention pin acquisition, retry pinning, and FIFO eviction are mutually serialized, preventing race conditions that could expose an evicted result as active.

---

## 12.14 Comprehensive Exception-Safety Matrix Across Publication Phases

| Publication Phase | Failure Mode | Rollback Action | State Invariant Preserved |
|---|---|---|---|
| **Phase 1: Domain Check** | Out-of-domain generation | Synchronous throw | Root unchanged; zero allocation |
| **Phase 2: Ingress Copy** | Deep copy memory failure | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 3: Pre-Contiguity**| Key gaps in existing map | Fail-closed throw | Root unchanged; alerts operator |
| **Phase 4: Pre-Allocation**| Object instantiation fail| Synchronous throw | Root unchanged; in-flight retained |
| **Phase 5: Eviction Check**| Oldest entry pinned | Backpressure throw | Root unchanged; capacity preserved |
| **Phase 6: Tombstone Staging**| Missing audit record | Fail-closed throw | Root unchanged; no eviction |
| **Phase 7: Post-Contiguity**| Key gap in candidate map | Fail-closed throw | Candidate root discarded |
| **Phase 8: Atomic Swap** | Pointer replacement | Infallible | Single linearization point |
| **Phase 9: Return Ref** | Return cached pointer | Infallible | Zero post-publication allocation |

---

## 12.15 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

Protocol A linearizes commitment of the operation and settles execution:
1. `commitRetirementBatch` synchronously stages and commits the transaction in the archive.
2. Custody gate transitions to `CUSTODY_RELEASED_BARRIER_PENDING`.
3. Pre-allocated `DrainerCustodyReleaseReceipt` and `PendingSettlementObligation` are published atomically.
4. Barrier resolves with `AuthoritativeSettlementEvidence` containing `CONFIRMED_COMMITTED`.
5. `releaseSettlementBarrierPin` is authorized using the verified `AuthoritativeSettlementEvidence`.

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

Protocol B handles cancelled or uncommitted operations:
1. Verify Level 3 proof of non-commitment (`TRANSACTION_ABORTED_FENCED`).
2. Atomically fence transaction ID in archive.
3. Transition custody gate to `RECONCILING_CUSTODY`.
4. If destination is healthy, transfer operation; otherwise restore to source buffer.
5. Publish release and pending obligation.
6. Resolve barrier with `ABORTED_FENCED` evidence.
7. `releaseSettlementBarrierPin` is authorized using the verified `AuthoritativeSettlementEvidence`.

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

- **Case A (Normal Commit):** Promise resolves; archive commits; Protocol A resolves barrier and authorizes settlement pin release.
- **Case B (Early Rejection):** Promise rejects before staging; Protocol B restores to buffer and authorizes pin release.
- **Case C (Post-Commit Rejection):** Commit succeeded; Promise rejected; irrevocable commit honored; barrier resolved committed; settlement pin released with commit evidence.
- **Case D (Network Split / Timeout):** Watchdog fires; attempt 1 invalidated; barrier held; settlement pin remains held; attempt 2 waits at Step 4; resolves cleanly when I/O completes.
- **Case E (Split-Brain Leader):** Fencing token mismatch; stale leader rejected; active leader reconciles.
- **Case F (Tombstone Capacity Saturation):** Oldest tombstone pinned; backpressure applied; zero generation floor gaps.
- **Case G (Duplicate Retry After Eviction):** Floor advanced past generation; returns `CUSTODY_RECEIPT_EXPIRED` with compact audit tombstone verification; zero republication.
- **Case H (Delayed Settled Retry After Attempt Change):** Settled retry arrives with Attempt 1 token while Attempt 2 is active or attempt is null; identity matched against cached receipt and audit record; acquires retry pin; returns cached receipt immediately without error.

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| Case | Scenario | Gate State | Custody Owner | Barrier State | Correct Action | Forbidden Action |
|---|---|---|---|---|---|---|
| **1** | Attempt 1 times out; Attempt 2 admitted | `BARRIER_PENDING` | Drainer | PENDING | Attempt 2 waits at Step 4 | Attempt 2 steals custody |
| **2** | Attempt 1 Promise resolves post-timeout | `BARRIER_PENDING` | Drainer | PENDING | Settle barrier; release pin with evidence; unblock Attempt 2 | Drop Attempt 1 result |
| **3** | Settled retry with null `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Acquire retry pin; return cached result | Throw stale attempt error |
| **4** | Settled retry with newer `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Acquire retry pin; return cached result | Throw stale attempt error |
| **5** | Settled retry with mismatched token | `SETTLED` | Archive | RESOLVED | Throw `TOKEN_MISMATCH` | Return cached result |
| **6** | Pending retry with original token | `BARRIER_PENDING` | Drainer | PENDING | Acquire retry pin; return cached result | Reject retry |
| **7** | Pending retry with conflicting audit | `BARRIER_PENDING` | Drainer | PENDING | Fail closed (corrupted) | Return cached result |
| **8** | Settled retry with missing audit | `SETTLED` | Archive | RESOLVED | Fail closed (missing audit)| Return cached result |
| **9** | First publication with stale attempt | `IN_FLIGHT` | Drainer | UNALLOCATED | Reject stale attempt | Publish candidate root |
| **10** | First publication with out-of-order $g$ | `IN_FLIGHT` | Drainer | UNALLOCATED | Throw generation gap | Advance publishedGeneration|
| **11** | FIFO eviction with pinned oldest entry | `CAPACITY_MAX` | Drainer | PENDING | Throw backpressure | Skip pinned entry |
| **12** | Watchdog timeout on pinned entry | `DEGRADED` | Drainer | PENDING | Maintain retention pin | Release pin on timeout |
| **13** | Settlement pin release without Level 3 proof | `BARRIER_PENDING` | Drainer | PENDING | Throw evidence required | Release pin on token alone |
| **14** | Pin release on actual Level 3 settlement | `SETTLED` | Archive | RESOLVED | Remove pin atomically | Leak retention pin |
| **15** | Retry on evicted generation ($g \le \text{floor}$)| `ANY` | Archive | RESOLVED | Check tombstone; return expired | Fall through to first pub |
| **16** | First pub after cache completely emptied | `IDLE` | Drainer | UNALLOCATED | Publish $N+1$ cleanly | Reject as out-of-order |
| **17** | Empty cache before any publications | `IDLE` | Drainer | UNALLOCATED | Floor is 0; pub $g=1$ | Allow $g=0$ or $g=2$ |
| **18** | Out-of-domain generation $g \le 0$ | `ANY` | Any | ANY | Throw `INVALID_DOMAIN` | Process publication |
| **19** | Generation sequence exhaustion | `ANY` | Any | ANY | Throw `SEQUENCE_EXHAUSTED`| Wrap around to 0 |
| **20** | Duplicate commit retry with mutated op | `COMMITTED` | Archive | RESOLVED | Throw payload mismatch | Overwrite commit |
| **21** | Concurrent commit and cancel | `RESERVED` | Archive | PENDING | First-write-wins | Allow dual disposition |
| **22** | Pre-eviction result map has key gaps | `PUBLISHING` | Drainer | PENDING | Throw non-contiguous | Silently choose min key |
| **23** | Destination transfer succeeds | `TRANSFERRED` | Destination | PENDING | Settle barrier Protocol B | Restore to source buffer |
| **24** | Catastrophic proof corruption | `BLOCKED` | Audit Admin | DEGRADED | Latch `RECONCILIATION_BLOCKED` | Optimistic barrier resolution |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{EVIDENCE\_MATCHED}(g)$$
2. **Capability-Gated Settlement-Pin Release Invariant:**
   $$\mathbf{RELEASE\_SETTLEMENT\_PIN}(pinId) \implies \mathbf{AssertAuthoritativeSettlementEvidence}(evidence) \land (\mathbf{pin.identity} \equiv \mathbf{evidence.identity})$$
3. **Atomic Retry Pinning Invariant:**
   A retry receipt cannot be returned to a caller without its corresponding `IN_TURN_RETRY` pin being authoritatively published in `AuthoritativeCustodyRoot`.
4. **Exact Retention Contiguity Invariant:**
   $\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1, \dots, \mathbf{publishedGeneration}\}$; key gaps, duplicate keys, or out-of-range keys fail closed.
5. **Atomic Tombstone Publication Invariant:**
   Eviction of $g_{\text{oldest}}$ from `publicationResults` and creation of its `ImmutableCompactAuditTombstone` in `generationAuditHistory` must linearize in the same candidate-root replacement.
6. **Attempt-Fenced First Publication Invariant:**
   $$\mathbf{FIRST\_PUBLICATION}(g) \implies (\mathbf{callerAttempt} === \mathbf{\_currentAttemptId}) \land (\mathbf{inFlightRecord.token} === \mathbf{callerToken}) \land (g === \mathbf{publishedGeneration} + 1)$$
7. **Settled Retry Attempt Independence Invariant:**
   A settled retry validates exact 1-to-1 token identity against cached evidence and authoritative audit history, completely independent of `_currentAttemptId`.
8. **Bidirectional Audit Consistency Invariant:**
   Any structural discrepancy across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory` fails closed.
9. **Operational Retention Pin and Strict FIFO Invariant:**
   A pinned oldest generation CANNOT be evicted; eviction cannot skip pinned entries; capacity exhaustion applies fail-closed backpressure.
10. **Watchdog Pin Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins whose underlying Promises remain unsettled.
11. **Empty-Cache Floor Invariant:**
   $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
   An evicted generation ($g \le \mathbf{custodyGenerationFloor}$) can never re-enter first publication.
12. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
13. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
14. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.
15. **Inseparable Release/Obligation Invariant:** A released custody state can **never** become observable without its corresponding settlement obligation already present in `this.#custodyRoot.pendingSettlementObligations`.
16. **Runtime Immutability Invariant:** Published collections and evidence references **CANNOT** be mutated through `Map.prototype` or mutable array/buffer aliases.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)} \longrightarrow \text{6. Release Settlement Pin}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R18 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution, Capability-Gated Pin Release & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. The settlement barrier resolves with verified `AuthoritativeSettlementEvidence`. This evidence authorizes `releaseSettlementBarrierPin`, freeing generation $g$ for eventual FIFO eviction. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following thirty deterministic future test specifications are formally codified for Sub-Milestone M5.1:

- **Test R18-R18-T01 (Pending Retry Pin Acquisition and Root Publication Ordering):** Present valid pending retry for $g=1$; assert `AuthoritativeCustodyRoot` contains `IN_TURN_RETRY` pin before return; assert returned response contains active `retryPinId`.
- **Test R18-R18-T02 (Settled Retry Pin Acquisition and Attempt Independence):** Set `_currentAttemptId = null`; present valid settled retry for $g=1$; assert candidate root atomically publishes `IN_TURN_RETRY` pin; assert receipt returned cleanly.
- **Test R18-R18-T03 (Retry Idempotency Returns Same Logical Pin Identity):** Issue two identical retry calls for $g=1$ concurrently; assert second call returns existing `retryPinId`; assert `activeRetentionPins.size` does not double.
- **Test R18-R18-T04 (Conflicting Retry Identity Fails Closed):** Issue retry with matching generation and operation ID but mutated payload digest or transaction ID; assert throws `RETENTION_PIN_COLLISION_MUTATED_IDENTITY`.
- **Test R18-R18-T05 (Retry Pin Release and Pin-Type Isolation):** Call `releaseRetryPin` passing `retryPinId`; assert pin removed from root; assert calling `releaseRetryPin` on `SETTLEMENT_BARRIER` pin throws `RETENTION_PIN_TYPE_MISMATCH_EXPECTED_RETRY`.
- **Test R18-R18-T06 (Empty-Map Contiguity Corruption Fails Closed):** Artificially inject empty map with `custodyGenerationFloor = 0` but `publishedGeneration = 2`; call publication; assert throws `STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS`.
- **Test R18-R18-T07 (Same-Size Map with Missing Key Fails Closed):** Populate map with keys `{1, 3}` and set `floor = 0, pubGen = 3` (size 2 != 3); assert throws `STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS`.
- **Test R18-R18-T08 (Same-Size Map with Out-of-Range Key Fails Closed):** Populate map with keys `{1, 4}` and set `floor = 0, pubGen = 2` (size 2 == 2); assert throws `STATE_INCONSISTENT_RESULT_KEY_GAP_DETECTED` (missing key 2).
- **Test R18-R18-T09 (Initial and Fully Evicted Valid States Pass Contiguity Check):** Assert initial state (`floor = 0, pubGen = 0, size = 0`) and fully evicted state (`floor = N, pubGen = N, size = 0`) evaluate cleanly without error.
- **Test R18-R18-T10 (Strict FIFO Eviction with Pinned Oldest Entry Throws Backpressure):** 1,000 entries populated; $g=1$ has `IN_TURN_RETRY` pin; $g=2 \dots 1000$ unpinned. Attempt publication of $g=1001$; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`; assert $g=2$ is NOT evicted.
- **Test R18-R18-T11 (Atomic Result Eviction and Tombstone Creation Linearize in Same Root):** At capacity, publish $g=1001$; assert $g=1$ deleted from `publicationResults` AND `ImmutableCompactAuditTombstone` for $g=1$ added to `generationAuditHistory` in single atomic root replacement.
- **Test R18-R18-T12 (Matching Tombstone Query Confirms Expiry):** Query evicted generation $g=1$ within tombstone horizon; assert returns `CUSTODY_RECEIPT_EXPIRED` with verified tombstone reference.
- **Test R18-R18-T13 (Missing or Contradictory Tombstone Within Retention Horizon Fails Closed):** Delete tombstone for $g=1$ where $\mathbf{auditTombstoneFloor} < 1 \le \mathbf{custodyGenerationFloor}$; query $g=1$; assert throws `STATE_INCONSISTENT_TOMBSTONE_MISSING_IN_HORIZON`.
- **Test R18-R18-T14 (Tombstone Horizon Exhaustion and Backpressure):** Reach `MAX_AUDIT_TOMBSTONES = 5000`; assert oldest tombstone evicts safely; if pinned, assert throws backpressure.
- **Test R18-R18-T15 (Settlement Evidence Issuer Authorization):** Attempt to pass caller-constructed mock evidence to `releaseSettlementBarrierPin`; assert throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.
- **Test R18-R18-T16 (Evidence Bound to Every Required Identity Dimension):** Present evidence with mismatched `drainerOwnershipGeneration`; assert throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R18-T17 (Unsettled Promise and Watchdog Timeout Immunity):** Fire 5,000 ms watchdog; attempt to release `SETTLEMENT_BARRIER` pin; assert throws evidence required; assert pin remains held.
- **Test R18-R18-T18 (Promise Rejection Without Authoritative Reconciliation Fails Closed):** Destination promise rejects with network error but archive unconfirmed; attempt pin release; assert throws `SETTLEMENT_EVIDENCE_INCOMPLETE`.
- **Test R18-R18-T19 (Stale and Replayed Settlement Evidence Rejected):** Present previously resolved evidence for $g=1$ to release pin for $g=2$; assert throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R18-T20 (Incomplete Settlement Evidence Missing Component Receipt Fails Closed):** Omit `CustodyReconciliationReceipt`; assert throws `SETTLEMENT_EVIDENCE_INCOMPLETE`.
- **Test R18-R18-T21 (Concurrent Retry, Eviction, and Settlement-Pin Release Preserve Exact Contiguity):** Simulate microtask interleaved operations; assert all intermediate and final roots satisfy `assertRetentionMapContiguity`.
- **Test R18-R18-T22 (Generation Sequence Exhaustion Fails Closed):** Pass $g = \text{Number.MAX\_SAFE\_INTEGER}$; assert throws `GENERATION_SEQUENCE_EXHAUSTED`.
- **Test R18-R18-T23 (Negative and Non-Integer Generations Rejected Upfront):** Pass $g = -1$, $g = 1.5$; assert throws `INVALID_GENERATION_DOMAIN`.
- **Test R18-R18-T24 (Map Pre-Existing Cap Violation Fails Closed Without Bulk Eviction):** Inject map with 1,005 entries; call publication; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`.
- **Test R18-R18-T25 (Sequential Publication Resumes Cleanly From Fully Empty Cache):** From fully evicted cache ($g_{\text{floor}} = N$, $g_{\text{pub}} = N$), publish $g = N + 1$; assert succeeds cleanly; assert new map size is 1.
- **Test R18-R18-T26 (Settled Retry Succeeds When `_currentAttemptId` Belongs to Later Attempt):** Advance attempt to 2; query settled generation 1 with Attempt 1 token; assert returns cached result and active `retryPinId`.
- **Test R18-R18-T27 (Stale Attempt Cannot Acquire or Release Newer Attempt's Pin):** Attempt 1 tries to release Attempt 2's retry pin; assert throws `STALE_ATTEMPT_PIN_RELEASE_REJECTED`.
- **Test R18-R18-T28 (Existing Alias-Closure and Defensive Byte-Copy Tests Intact):** Mutating input buffers does not affect published receipts.
- **Test R18-R18-T29 (Prototype Freeze on Published Collections Prevents Map Mutation):** Invoking `.set()` or `.delete()` on `ImmutableMap` throws `UNSUPPORTED_MUTATION_ON_IMMUTABLE_COLLECTION`.
- **Test R18-R18-T30 (Universal M5.0 Invariant Regression Suite):** Full regression across all Phase 7.5 single-owner, single-drainer, and archive arbitration invariants.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R18):
================================================================================
1. [ATOMIC RETRY PIN ACQUISITION]: Does the Engineering Authority approve
   requiring pending and settled retries to acquire an IN_TURN_RETRY pin in
   AuthoritativeCustodyRoot before returning cached publication results,
   eliminating the unpinned return gap and protecting active retries from eviction?

2. [EXACT RETENTION CONTIGUITY VERIFICATION]: Does the Engineering Authority approve
   the unconditional verification of publicationResults.keys() === {floor + 1, ..., pubGen},
   validating every key sequence explicitly before and after eviction/publication,
   and failing closed on key gaps or pre-existing cap violations?

3. [ATOMIC TOMBSTONE PUBLICATION ON EVICTION]: Does the Engineering Authority approve
   constructing and publishing an ImmutableCompactAuditTombstone into generationAuditHistory
   atomically during result eviction, and failing closed on missing tombstones within
   the promised tombstone retention horizon?

4. [SETTLEMENT EVIDENCE PROVENANCE & VALIDATION]: Does the Engineering Authority approve
   authorizing Level 3 settlement evidence exclusively through the internal
   SettlementEvidenceFactory, verifying all 5 core facts against coordinator state
   prior to settlement pin release, with single-use replay protection?

5. [CLOSURE OF ALL FOUR ARCHITECTURAL BLOCKERS]: Does the Engineering Authority agree
   that B1 (retry pinning), B2 (retention contiguity), B3 (tombstone publication),
   and B4 (evidence provenance) are fully closed without introducing new gaps?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R18 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R18 proposal closes the four remaining architectural blockers. It linearizes retry pin acquisition atomically in candidate roots, enforces exact key contiguity unconditionally, couples result eviction with atomic tombstone publication, defines authoritative settlement evidence provenance, and codifies thirty deterministic future test specifications.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R18 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
