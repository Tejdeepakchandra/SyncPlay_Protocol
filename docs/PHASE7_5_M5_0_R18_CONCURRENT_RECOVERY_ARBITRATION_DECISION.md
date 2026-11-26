# Phase 7.5 M5.0-R18-R19 — Retry Pin Lease Lifetime, Bounded Tombstone Retirement & Complete Evidence Consistency
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `6043264b616e46b042f19de287c6e6afbbf14526` (M5.0-R18-R18)
**Direct Parent Commit:** `6043264b616e46b042f19de287c6e6afbbf14526`
**Authorization Scope:** M5.0-R18-R19 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R19 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R18, we formalized atomic retry pinning, exact retention contiguity, and tombstone creation on eviction. However, an in-depth systems audit revealed four remaining critical lifecycle and consistency gaps:

1. **Shared Pin Identity & Premature Consumer Eviction:** In R18-R18, `_getOrAllocateRetryPin()` returned the same pin ID for all matching requests without distinguishing concurrent consumers. If Consumer A and Consumer B simultaneously queried generation $g$, Consumer A's early completion and release deleted the shared pin from the root, prematurely exposing the entry to FIFO eviction while Consumer B was still reading the result.
2. **Unbounded Tombstone Retention & Missing Floor Retirement:** R18-R18 staged tombstones on result eviction, but lacked concrete retirement logic and floor advancement for `auditTombstoneFloor`. Without bounded FIFO retirement, tombstones would accumulate indefinitely, violating bounded-memory requirements.
3. **Incomplete Retained-Record Identity Consistency:** While R18-R18 validated key sequence contiguity, it did not verify structural cross-field consistency (tokens, transaction IDs, operation identities, barrier identities, drainer ownership generations) across all retained entries in `publicationResults` and `generationAuditHistory`.
4. **Unspecified Evidence Factory Provenance & Verification:** R18-R18 referenced `AuthoritativeSettlementEvidence`, but lacked an explicit factory contract defining how the recovery kernel verifies the 5 core settlement facts against authoritative coordinator state, single-use replay protection, and the handling of post-commit rejections vs. ambiguous pre-commit failures.

### Core Architectural Guarantees Established in R18-R19:

- **Independent Consumer Retry Leases (`ImmutableRetryLease`):** Formalizes multi-consumer lease ownership over retained generations. Concurrent consumers receive distinct `leaseId`s. Early release by one consumer removes only its lease, keeping the generation protected until all leases are released. Retransmission by the same consumer is recognized idempotently via `(consumerId, clientRequestId)` without leaking references.
- **Strictly Bounded Tombstone Lifecycle (`MAX_AUDIT_TOMBSTONES = 5000`):** Implements exact FIFO tombstone retirement and monotonic advancement of `auditTombstoneFloor`. Enforces key contiguity over $\mathbf{generationAuditHistory.keys()} \equiv \{\mathbf{auditTombstoneFloor} + 1, \dots, \mathbf{publishedGeneration}\}$. Lookups within the tombstone horizon verify tombstone existence; missing tombstones fail closed as state corruption.
- **Complete Retained-Record Identity Consistency:** Enforces strict 6-dimensional identity verification across every retained publication result, obligation, and audit tombstone. Distinguishes attempt-independent publication bindings from coordinator active-attempt evolution.
- **Auditable Evidence Provenance via `SettlementEvidenceFactory`:** Authorizes the internal `SettlementEvidenceFactory` as the sole issuer of Level 3 evidence. Pre-validates Promise settlement, classified outcomes, archive disposition, custody reconciliation, and drainer release against authoritative coordinator state before settlement pin release. Watchdog timeouts cannot manufacture settlement evidence.
- **Exhaustive Deterministic Test Suite (30 Test Cases):** Codifies thirty deterministic future test specifications in Section 19 covering multi-consumer lease lifecycles, bounded tombstone retirement, complete identity verification, and evidence provenance.

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
│      generationAuditHistory, activeSettlementPins, and activeRetryLeases.   │
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
   - At system initialization: `publishedGeneration = 0`, `custodyGenerationFloor = 0`, `auditTombstoneFloor = 0`, `publicationResults.size = 0`, `pendingSettlementObligations.size = 0`, `generationAuditHistory.size = 0`, `activeSettlementPins.size = 0`, `activeRetryLeases.size = 0`.
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

#### Evidence Issuance & Verification Contract:
- **Authorized Component Issuers:**
  - `ExecutionSettlementReceipt`: Issued exclusively by `DestinationExecutionBridge` upon Promise settlement.
  - `ArchiveDispositionReceipt`: Issued exclusively by `TerminalArchiveManager` upon synchronous commit or abort fencing.
  - `CustodyReconciliationReceipt`: Issued exclusively by `CustodyReconciliationGate` upon verifying source/destination custody transfer.
  - `DrainerCustodyReleaseReceipt`: Issued exclusively by `QuarantineDrainer` upon in-flight custody release.
  - `AuthoritativeSettlementEvidence`: Synthesized and sealed exclusively by `SettlementEvidenceFactory` within `@syncplay/protocol`.
- **Pre-Validation Against Authoritative Coordinator State:**
  Prior to authorizing barrier resolution or settlement pin release, the coordinator verifies:
  1. The underlying asynchronous Promise has actually settled (Promise callback executed; watchdog timeout NEVER counts as settlement).
  2. The settlement outcome is classified (`CONFIRMED_COMMITTED` or `ABORTED_FENCED`).
  3. Archive disposition is recorded in `#committedTransactions` or `#fencedTransactions`.
  4. Custody reconciliation completed without error.
  5. Drainer custody was relinquished for generation $g$.
  6. All receipts match 1-to-1 against `pendingSettlementObligations.get(g)`.
- **Replay Protection:** Evidence is single-use for resolving the specific barrier and releasing the specific `SETTLEMENT_BARRIER` pin for generation $g$. Replaying evidence for another generation or obligation is rejected with `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

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

### 12.12 The Complete Lifecycle Publication, Retry Lease Lifetime, Bounded Tombstone Retirement & Complete Consistency

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
  readonly retryLeaseId: string | null; // Non-null on retries; null on first publication
}

export interface ImmutableSettlementPin {
  readonly pinId: string; // pin:${coordinatorInstanceId}:${generation}:${barrierIdentity}:${seq}
  readonly generation: number;
  readonly barrierIdentity: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly acquiredAtTimestamp: number;
  readonly monotonicAcquiredMs: number;
}

export interface ImmutableRetryLease {
  readonly leaseId: string; // lease:${coordinatorInstanceId}:${generation}:${consumerId}:${seq}
  readonly generation: number;
  readonly consumerId: string;
  readonly clientRequestId: string;
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
  readonly auditTombstoneFloor: number;    // Floor below which tombstones are retired
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, AuthoritativeSettlementEvidence | ImmutableCompactAuditTombstone>;
  readonly activeSettlementPins: ImmutableMap<string, ImmutableSettlementPin>;
  readonly activeRetryLeases: ImmutableMap<string, ImmutableRetryLease>;
}
```

#### Independent Consumer Retry Lease Model (B1):

To resolve the shared-pin premature release vulnerability:
1. **Independent Consumer Leases:** Each concurrent retry consumer receives an independent `ImmutableRetryLease` identified by `leaseId`.
2. **Idempotent Retransmission Recovery:** When a consumer retransmits a retry request with matching `(generation, consumerId, clientRequestId)`, the coordinator recovers the existing `leaseId` without allocating a duplicate lease or leaking references. Conflicting identity fields fail closed with `RETRY_LEASE_COLLISION_MUTATED_IDENTITY`.
3. **Isolation of Consumer Release:** When Consumer 1 calls `releaseRetryLease(leaseId_A)`, only `leaseId_A` is removed. Consumer 2's `leaseId_B` remains active in `activeRetryLeases`.
4. **Eviction Protection Rule:** Generation $g$ is eviction-eligible if and only if:
   $$\forall p \in \mathbf{activeSettlementPins},\; p.\text{generation} \ne g \;\;\land\;\; \forall l \in \mathbf{activeRetryLeases},\; l.\text{generation} \ne g$$
5. **Watchdog Immunity:** Watchdog timeouts log telemetry but **CANNOT** release retry leases or settlement pins.
6. **Linearization:** Lease acquisition and release are atomic candidate-root replacements executed synchronously before returning to the caller.

#### Genuinely Bounded Tombstone Lifecycle & Contiguity (B2):

To enforce bounded memory while guaranteeing auditability:
1. **Capacity Budget:** `MAX_AUDIT_TOMBSTONES = 5000` is an authoritative ceiling.
2. **Dual Exact Contiguity Invariants:**
   - **Publication Results:**
     $$\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
   - **Generation Audit History:**
     $$\mathbf{generationAuditHistory.keys()} \equiv \{\mathbf{auditTombstoneFloor} + 1,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
   subject to empty-cache states ($\mathbf{floor} === \mathbf{publishedGeneration} \iff \mathbf{size} === 0$).
3. **FIFO Tombstone Retirement:**
   When $\mathbf{tombstoneCount} = \mathbf{custodyGenerationFloor} - \mathbf{auditTombstoneFloor} \ge 5000$, oldest tombstone $g_{\text{tomb}} = \mathbf{auditTombstoneFloor} + 1$ is retired if and only if no active retry lease or settlement pin protects it. If protected, retirement applies fail-closed backpressure (`TOMBSTONE_CAPACITY_EXHAUSTED`).
4. **Lookup Horizons:**
   - $\mathbf{custodyGenerationFloor} < g \le \mathbf{publishedGeneration}$: Retained detailed result.
   - $\mathbf{auditTombstoneFloor} < g \le \mathbf{custodyGenerationFloor}$: Promised tombstone horizon. Tombstone must exist; missing tombstone throws `STATE_INCONSISTENT_TOMBSTONE_MISSING_IN_HORIZON`.
   - $g \le \mathbf{auditTombstoneFloor}$: Legitimate historical expiry beyond tombstone horizon. Returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.

#### Complete Retained-Record Identity Consistency (C1):

For every entry $g \in \mathbf{publicationResults}$, the coordinator unconditionally validates:
$$\mathbf{result.releaseReceipt.releasedCustodyGeneration} === g === \mathbf{result.obligation.generation}$$
$$\mathbf{isEquivalentOwnerToken}(\mathbf{result.releaseReceipt.identity},\; \mathbf{result.obligation.ownerToken}) \equiv \mathbf{true}$$
$$\mathbf{result.obligation.transactionId} === \mathbf{result.releaseReceipt.identity.transactionId}$$
$$\mathbf{result.obligation.operationIdentity} === \mathbf{result.releaseReceipt.identity.operationIdentity}$$
$$\mathbf{result.obligation.drainerOwnershipGeneration} === \mathbf{result.releaseReceipt.identity.drainerOwnershipGeneration}$$
If pending, `obligation` matches `pendingSettlementObligations.get(g)`. If settled, `generationAuditHistory.get(g)` matches across all identity dimensions. Any discrepancy fails closed with `STATE_INCONSISTENT_RETAINED_RECORD_CORRUPTED`.

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

  if (floor === publishedGeneration) {
    if (resultsMap.size !== 0) {
      throw new Error('STATE_INCONSISTENT_EMPTY_RANGE_NON_EMPTY_MAP');
    }
    return;
  }

  const expectedCount = publishedGeneration - floor;
  if (resultsMap.size !== expectedCount) {
    throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
  }

  for (let g = floor + 1; g <= publishedGeneration; g++) {
    if (!resultsMap.has(g)) {
      throw new Error(`STATE_INCONSISTENT_RESULT_KEY_GAP_DETECTED: missing generation ${g}`);
    }
    const record = resultsMap.get(g)!;
    // Complete Retained-Record Identity Consistency (C1)
    if (
      record.releaseReceipt.releasedCustodyGeneration !== g ||
      record.obligation.generation !== g ||
      record.obligation.publishedCustodyGeneration !== g ||
      !isEquivalentOwnerToken(record.releaseReceipt.identity, record.obligation.ownerToken) ||
      record.obligation.transactionId !== record.releaseReceipt.identity.transactionId ||
      record.obligation.operationIdentity !== record.releaseReceipt.identity.operationIdentity ||
      record.obligation.drainerOwnershipGeneration !== record.releaseReceipt.identity.drainerOwnershipGeneration
    ) {
      throw new Error(`STATE_INCONSISTENT_RETAINED_RECORD_CORRUPTED: key ${g}`);
    }
  }

  for (const k of resultsMap.keys()) {
    if (!Number.isSafeInteger(k) || k <= floor || k > publishedGeneration) {
      throw new Error(`STATE_INCONSISTENT_RESULT_GENERATION_OUT_OF_BOUNDS: key ${k}`);
    }
  }
}

public assertAuditHistoryContiguity(
  auditMap: Map<number, any> | ImmutableMap<number, any>,
  auditFloor: number,
  publishedGeneration: number,
  custodyFloor: number,
): void {
  if (auditFloor === publishedGeneration) {
    if (auditMap.size !== 0) throw new Error('STATE_INCONSISTENT_EMPTY_AUDIT_NON_EMPTY_MAP');
    return;
  }
  const expectedCount = publishedGeneration - auditFloor;
  if (auditMap.size !== expectedCount) throw new Error('STATE_INCONSISTENT_AUDIT_MAP_NON_CONTIGUOUS');

  for (let g = auditFloor + 1; g <= publishedGeneration; g++) {
    if (!auditMap.has(g)) throw new Error(`STATE_INCONSISTENT_AUDIT_KEY_GAP: missing generation ${g}`);
    const record = auditMap.get(g);
    if (g <= custodyFloor && !record.evidenceDigest) {
      throw new Error(`STATE_INCONSISTENT_TOMBSTONE_SCHEMA_VIOLATION: key ${g}`);
    }
  }
}

public publishReleaseAndObligation(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
  barrierIdentity: string,
  retryContext?: { consumerId: string; clientRequestId: string },
): PublicationResponse {
  const currentRoot = this.#custodyRoot;

  const g = expectedOwner.custodyGeneration;
  assertValidGenerationDomain(g);

  const safeOwner = cloneAndFreezeOwnerToken(expectedOwner);
  const safeBarrierId = String(barrierIdentity);
  const safeTxId = String(safeOwner.transactionId);
  const safeOpId = String(safeOwner.operationIdentity);

  // 3. PRIOR PUBLICATION EVALUATION:
  if (g <= currentRoot.publishedGeneration) {
    // Case 3A: Published with Settlement Pending
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

      // ATOMIC RETRY LEASE ACQUISITION (B1):
      const consumerId = retryContext?.consumerId ?? `consumer-${safeOpId}`;
      const clientReqId = retryContext?.clientRequestId ?? `req-${performance.now()}`;
      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, consumerId, clientReqId, safeOwner, safeTxId, safeBarrierId
      );

      if (candidateLeases !== currentRoot.activeRetryLeases) {
        this.#custodyRoot = Object.freeze({
          ...currentRoot,
          activeRetryLeases: candidateLeases,
        });
      }

      return { publicationResult: cachedResult, retryLeaseId: leaseId };
    }

    // Case 3B: Published and Fully Settled
    if (currentRoot.publicationResults.has(g)) {
      const cachedResult = currentRoot.publicationResults.get(g)!;

      const auditEvidence = currentRoot.generationAuditHistory.get(g);
      if (!auditEvidence) throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');

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

      if (
        !isEquivalentOwnerToken(cachedResult.releaseReceipt.identity, safeOwner) ||
        cachedResult.releaseReceipt.identity.transactionId !== safeTxId ||
        cachedResult.releaseReceipt.identity.operationIdentity !== safeOpId ||
        cachedResult.obligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      // ATOMIC RETRY LEASE ACQUISITION (B1):
      const consumerId = retryContext?.consumerId ?? `consumer-${safeOpId}`;
      const clientReqId = retryContext?.clientRequestId ?? `req-${performance.now()}`;
      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, consumerId, clientReqId, safeOwner, safeTxId, safeBarrierId
      );

      if (candidateLeases !== currentRoot.activeRetryLeases) {
        this.#custodyRoot = Object.freeze({
          ...currentRoot,
          activeRetryLeases: candidateLeases,
        });
      }

      return { publicationResult: cachedResult, retryLeaseId: leaseId };
    }

    // Case 3C: Published but Outside Retention Horizon (Evicted / Bounded Tombstone Horizon)
    if (g <= currentRoot.custodyGenerationFloor) {
      if (g > currentRoot.auditTombstoneFloor) {
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

  // Unconditional Pre-Contiguity Assertions
  this.assertRetentionMapContiguity(currentRoot.publicationResults, currentRoot.custodyGenerationFloor, currentRoot.publishedGeneration);
  this.assertAuditHistoryContiguity(currentRoot.generationAuditHistory, currentRoot.auditTombstoneFloor, currentRoot.publishedGeneration, currentRoot.custodyGenerationFloor);

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

  // Allocate Settlement Pin for First Publication
  const settlementPinId = `pin:${this._coordinatorInstanceId}:${g}:${safeBarrierId}:${++this._pinSequenceCounter}`;
  const settlementPin: ImmutableSettlementPin = Object.freeze({
    pinId: settlementPinId,
    generation: g,
    barrierIdentity: safeBarrierId,
    ownerToken: safeOwner,
    transactionId: safeTxId,
    operationIdentity: safeOpId,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
  });

  // Strict FIFO Contiguity Verification, Eviction & Atomic Tombstone Publication
  let candidateFloor = currentRoot.custodyGenerationFloor;
  let candidateAuditFloor = currentRoot.auditTombstoneFloor;
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

    // Protection check against settlement pins and retry leases
    for (const pin of currentRoot.activeSettlementPins.values()) {
      if (pin.generation === g_oldest) throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }
    for (const lease of currentRoot.activeRetryLeases.values()) {
      if (lease.generation === g_oldest) throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    // Atomic Tombstone Staging
    const existingAudit = candidateAuditHistory.get(g_oldest) as AuthoritativeSettlementEvidence;
    if (!existingAudit) throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');

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

    // Genuinely Bounded Tombstone Retirement (B2):
    const tombstoneCount = candidateFloor - candidateAuditFloor;
    if (tombstoneCount >= 5000) {
      const g_tomb_oldest = candidateAuditFloor + 1;
      // Ensure oldest tombstone is not protected by an active lease
      for (const lease of currentRoot.activeRetryLeases.values()) {
        if (lease.generation === g_tomb_oldest) throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED');
      }
      candidateAuditHistory.delete(g_tomb_oldest);
      candidateAuditFloor = g_tomb_oldest;
    }
  }

  candidateResultsMap.set(g, publicationResult);

  // Post-insertion unconditional contiguity assertions
  this.assertRetentionMapContiguity(candidateResultsMap, candidateFloor, g);
  this.assertAuditHistoryContiguity(candidateAuditHistory, candidateAuditFloor, g, candidateFloor);

  const candidateObligations = new ImmutableMap([
    ...currentRoot.pendingSettlementObligations.entries(),
    [g, obligation],
  ]);
  const candidateResults = new ImmutableMap(candidateResultsMap.entries());
  const candidateAudit = new ImmutableMap(candidateAuditHistory.entries());
  const candidatePins = new ImmutableMap([
    ...currentRoot.activeSettlementPins.entries(),
    [settlementPinId, settlementPin],
  ]);

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: g,
    custodyGenerationFloor: candidateFloor,
    auditTombstoneFloor: candidateAuditFloor,
    inFlightRecord: null, // Custody released!
    gateState: 'CUSTODY_RELEASED_BARRIER_PENDING',
    pendingSettlementObligations: candidateObligations,
    publicationResults: candidateResults,
    generationAuditHistory: candidateAudit,
    activeSettlementPins: candidatePins,
    activeRetryLeases: currentRoot.activeRetryLeases,
  });

  // THE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;
  return { publicationResult, retryLeaseId: null };
}

private _getOrAllocateRetryLease(
  root: AuthoritativeCustodyRoot,
  generation: number,
  consumerId: string,
  clientRequestId: string,
  ownerToken: CustodyOwnershipToken,
  transactionId: string,
  barrierIdentity: string,
): { candidateLeases: ImmutableMap<string, ImmutableRetryLease>; leaseId: string } {
  // Idempotent recovery for same consumer and request ID
  for (const existingLease of root.activeRetryLeases.values()) {
    if (
      existingLease.generation === generation &&
      existingLease.consumerId === consumerId &&
      existingLease.clientRequestId === clientRequestId
    ) {
      if (
        isEquivalentOwnerToken(existingLease.ownerToken, ownerToken) &&
        existingLease.transactionId === transactionId &&
        existingLease.barrierIdentity === barrierIdentity
      ) {
        return { candidateLeases: root.activeRetryLeases, leaseId: existingLease.leaseId };
      }
      throw new Error('RETRY_LEASE_COLLISION_MUTATED_IDENTITY');
    }
  }

  const seq = ++this._pinSequenceCounter;
  const leaseId = `lease:${this._coordinatorInstanceId}:${generation}:${consumerId}:${seq}`;

  if (root.activeRetryLeases.has(leaseId)) {
    throw new Error('RETRY_LEASE_ID_COLLISION');
  }

  const lease: ImmutableRetryLease = Object.freeze({
    leaseId,
    generation,
    consumerId,
    clientRequestId,
    ownerToken,
    transactionId,
    barrierIdentity,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
  });

  const candidateLeases = new ImmutableMap([
    ...root.activeRetryLeases.entries(),
    [leaseId, lease],
  ]);

  return { candidateLeases, leaseId };
}

public releaseRetryLease(leaseId: string, callerToken: CustodyOwnershipToken): void {
  const currentRoot = this.#custodyRoot;
  const lease = currentRoot.activeRetryLeases.get(leaseId);
  if (!lease) return; // Idempotent

  if (!isEquivalentOwnerToken(lease.ownerToken, callerToken)) {
    throw new Error('STALE_ATTEMPT_LEASE_RELEASE_REJECTED');
  }

  const candidateLeasesMap = new Map(currentRoot.activeRetryLeases.entries());
  candidateLeasesMap.delete(leaseId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeRetryLeases: new ImmutableMap(candidateLeasesMap.entries()),
  });
}

public releaseSettlementPin(pinId: string, settlementEvidence: AuthoritativeSettlementEvidence): void {
  const currentRoot = this.#custodyRoot;
  const pin = currentRoot.activeSettlementPins.get(pinId);
  if (!pin) return; // Idempotent

  assertAuthoritativeSettlementEvidence(settlementEvidence);

  if (
    !isEquivalentOwnerToken(pin.ownerToken, settlementEvidence.expectedOwner) ||
    pin.generation !== settlementEvidence.generation ||
    pin.transactionId !== settlementEvidence.transactionId ||
    pin.operationIdentity !== settlementEvidence.operationIdentity ||
    pin.barrierIdentity !== settlementEvidence.barrierIdentity ||
    pin.ownerToken.recoveryAttemptId !== settlementEvidence.expectedOwner.recoveryAttemptId
  ) {
    throw new Error('SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH');
  }

  const candidatePinsMap = new Map(currentRoot.activeSettlementPins.entries());
  candidatePinsMap.delete(pinId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeSettlementPins: new ImmutableMap(candidatePinsMap.entries()),
  });
}
```

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Under single-realm JavaScript execution:
- Candidate-root preparation, invariant verification, and reference replacement occur synchronously within one microtask turn.
- No other event loop callback can observe or mutate `this.#custodyRoot` between the check and the replacement.
- Retry lease acquisition, settlement pin acquisition, and FIFO eviction are mutually serialized, preventing race conditions that could expose an evicted result as active.

---

## 12.14 Comprehensive Exception-Safety Matrix Across Publication Phases

| Publication Phase | Failure Mode | Rollback Action | State Invariant Preserved |
|---|---|---|---|
| **Phase 1: Domain Check** | Out-of-domain generation | Synchronous throw | Root unchanged; zero allocation |
| **Phase 2: Ingress Copy** | Deep copy memory failure | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 3: Pre-Contiguity**| Key gaps in existing map | Fail-closed throw | Root unchanged; alerts operator |
| **Phase 4: Identity Consistency**| Retained field discrepancy| Fail-closed throw | Root unchanged; alerts operator |
| **Phase 5: Pre-Allocation**| Object instantiation fail| Synchronous throw | Root unchanged; in-flight retained |
| **Phase 6: Eviction Check**| Oldest entry pinned/leased| Backpressure throw | Root unchanged; capacity preserved |
| **Phase 7: Tombstone Staging**| Missing audit record | Fail-closed throw | Root unchanged; no eviction |
| **Phase 8: Tombstone Retir.**| Tombstone pinned by lease| Backpressure throw | Root unchanged; capacity preserved |
| **Phase 9: Post-Contiguity**| Key gap in candidate map | Fail-closed throw | Candidate root discarded |
| **Phase 10: Atomic Swap** | Pointer replacement | Infallible | Single linearization point |
| **Phase 11: Return Ref** | Return cached pointer | Infallible | Zero post-publication allocation |

---

## 12.15 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

Protocol A linearizes commitment of the operation and settles execution:
1. `commitRetirementBatch` synchronously stages and commits the transaction in the archive.
2. Custody gate transitions to `CUSTODY_RELEASED_BARRIER_PENDING`.
3. Pre-allocated `DrainerCustodyReleaseReceipt` and `PendingSettlementObligation` are published atomically.
4. Barrier resolves with `AuthoritativeSettlementEvidence` containing `CONFIRMED_COMMITTED`.
5. `releaseSettlementPin` is authorized using the verified `AuthoritativeSettlementEvidence`.

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

Protocol B handles cancelled or uncommitted operations:
1. Verify Level 3 proof of non-commitment (`TRANSACTION_ABORTED_FENCED`).
2. Atomically fence transaction ID in archive.
3. Transition custody gate to `RECONCILING_CUSTODY`.
4. If destination is healthy, transfer operation; otherwise restore to source buffer.
5. Publish release and pending obligation.
6. Resolve barrier with `ABORTED_FENCED` evidence.
7. `releaseSettlementPin` is authorized using the verified `AuthoritativeSettlementEvidence`.

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
- **Case F (Tombstone Capacity Saturation):** Oldest tombstone pinned by lease; backpressure applied; zero generation floor gaps.
- **Case G (Duplicate Retry After Eviction):** Floor advanced past generation; returns `CUSTODY_RECEIPT_EXPIRED` with compact audit tombstone verification; zero republication.
- **Case H (Delayed Settled Retry After Attempt Change):** Settled retry arrives with Attempt 1 token while Attempt 2 is active or attempt is null; identity matched against cached receipt and audit record; acquires independent retry lease; returns cached receipt immediately without error.

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| Case | Scenario | Gate State | Custody Owner | Barrier State | Correct Action | Forbidden Action |
|---|---|---|---|---|---|---|
| **1** | Attempt 1 times out; Attempt 2 admitted | `BARRIER_PENDING` | Drainer | PENDING | Attempt 2 waits at Step 4 | Attempt 2 steals custody |
| **2** | Attempt 1 Promise resolves post-timeout | `BARRIER_PENDING` | Drainer | PENDING | Settle barrier; release pin with evidence; unblock Attempt 2 | Drop Attempt 1 result |
| **3** | Settled retry with null `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Acquire retry lease; return cached result | Throw stale attempt error |
| **4** | Settled retry with newer `_currentAttemptId` | `SETTLED` | Archive | RESOLVED | Acquire retry lease; return cached result | Throw stale attempt error |
| **5** | Settled retry with mismatched token | `SETTLED` | Archive | RESOLVED | Throw `TOKEN_MISMATCH` | Return cached result |
| **6** | Pending retry with original token | `BARRIER_PENDING` | Drainer | PENDING | Acquire retry lease; return cached result | Reject retry |
| **7** | Pending retry with conflicting audit | `BARRIER_PENDING` | Drainer | PENDING | Fail closed (corrupted) | Return cached result |
| **8** | Settled retry with missing audit | `SETTLED` | Archive | RESOLVED | Fail closed (missing audit)| Return cached result |
| **9** | First publication with stale attempt | `IN_FLIGHT` | Drainer | UNALLOCATED | Reject stale attempt | Publish candidate root |
| **10** | First publication with out-of-order $g$ | `IN_FLIGHT` | Drainer | UNALLOCATED | Throw generation gap | Advance publishedGeneration|
| **11** | FIFO eviction with active retry lease | `CAPACITY_MAX` | Drainer | PENDING | Throw backpressure | Evict leased entry |
| **12** | Watchdog timeout on leased entry | `DEGRADED` | Drainer | PENDING | Maintain retry lease | Release lease on timeout |
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
3. **Independent Retry Lease Ownership Invariant:**
   Each concurrent retry consumer must acquire an independent `ImmutableRetryLease`; early release by one consumer cannot remove protection required by another.
4. **Complete Retained-Record Identity Consistency Invariant:**
   Every retained entry in `publicationResults` must match 1-to-1 across generation, 5-field token, transaction ID, operation ID, and drainer ownership generation.
5. **Exact Dual-Map Contiguity Invariant:**
   Both `publicationResults` and `generationAuditHistory` must satisfy exact integer key sequence contiguity from their respective floors to `publishedGeneration`.
6. **Bounded Tombstone Retirement Invariant:**
   Tombstone count cannot exceed 5,000; FIFO retirement must advance `auditTombstoneFloor` atomically with tombstone deletion; pinned tombstones apply fail-closed backpressure.
7. **Attempt-Fenced First Publication Invariant:**
   $$\mathbf{FIRST\_PUBLICATION}(g) \implies (\mathbf{callerAttempt} === \mathbf{\_currentAttemptId}) \land (\mathbf{inFlightRecord.token} === \mathbf{callerToken}) \land (g === \mathbf{publishedGeneration} + 1)$$
8. **Settled Retry Attempt Independence Invariant:**
   A settled retry validates exact 1-to-1 token identity against cached evidence and authoritative audit history, completely independent of `_currentAttemptId`.
9. **Bidirectional Audit Consistency Invariant:**
   Any structural discrepancy across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory` fails closed.
10. **Watchdog Pin and Lease Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins or retry leases whose underlying operations remain unresolved.
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
- That separate decision is outside the scope of R18-R19 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution, Capability-Gated Pin Release & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. The settlement barrier resolves with verified `AuthoritativeSettlementEvidence`. This evidence authorizes `releaseSettlementPin`, freeing generation $g$ for eventual FIFO eviction. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following thirty deterministic future test specifications are formally codified for Sub-Milestone M5.1:

- **Test R18-R19-T01 (Two Concurrent Consumers Acquire Distinct Leases):** Consumer 1 and Consumer 2 query settled $g=1$; assert both acquire distinct `leaseId_A` and `leaseId_B`; assert `activeRetryLeases.size === 2`.
- **Test R18-R19-T02 (Early Release by One Consumer Preserves Other Consumer Protection):** Consumer 1 releases `leaseId_A`; assert `leaseId_B` remains active in root; attempt publication of $g=1001$; assert eviction of $g=1$ blocked by backpressure.
- **Test R18-R19-T03 (Lost-Response Retransmission Recovers Original Lease Idempotently):** Consumer 1 resends identical retry with matching `clientRequestId`; assert returns existing `leaseId_A`; assert `activeRetryLeases.size` does not increase.
- **Test R18-R19-T04 (Duplicate Lease Release Is Idempotent No-Op):** Consumer 1 calls `releaseRetryLease(leaseId_A)` twice; assert second call returns void without error or root mutation.
- **Test R18-R19-T05 (Consumer Failure Cleans Up Only Failing Consumer Lease):** Simulate Consumer 1 connection abort; abort cleanup releases `leaseId_A`; assert Consumer 2's `leaseId_B` remains intact.
- **Test R18-R19-T06 (Final Lease Release Enables Generation Eviction):** Consumer 2 releases `leaseId_B`; assert $g=1$ has 0 leases and 0 pins; assert subsequent publication of $g=1001$ evicts $g=1$ cleanly.
- **Test R18-R19-T07 (Exact Tombstone Range Key Contiguity Enforced):** Populate tombstones; artificially delete middle tombstone key; attempt publication; assert throws `STATE_INCONSISTENT_AUDIT_KEY_GAP`.
- **Test R18-R19-T08 (Atomic Result Eviction and Tombstone Creation Linearize in Same Root):** At capacity, publish $g=1001$; assert $g=1$ deleted from `publicationResults` AND `ImmutableCompactAuditTombstone` for $g=1$ added to `generationAuditHistory` in single atomic root replacement.
- **Test R18-R19-T09 (FIFO Tombstone Retirement at 5,000 Capacity):** Populate 5,000 tombstones; evict next result; assert oldest tombstone $g_{\text{tomb}}$ deleted and `auditTombstoneFloor` advanced to $g_{\text{tomb}}$.
- **Test R18-R19-T10 (Pinned Oldest Tombstone Blocks Retirement via Backpressure):** Acquire retry lease on $g=1$ (now in tombstone range); reach 5,000 tombstones; assert next eviction throws `TOMBSTONE_CAPACITY_EXHAUSTED`.
- **Test R18-R19-T11 (Lookup Within Tombstone Horizon Validates Compact Tombstone):** Query $g=1$ where $\mathbf{auditTombstoneFloor} < 1 \le \mathbf{custodyGenerationFloor}$; assert returns `CUSTODY_RECEIPT_EXPIRED` with tombstone confirmation.
- **Test R18-R19-T12 (Missing Tombstone Inside Horizon Fails Closed as State Corruption):** Delete tombstone for $g=1$ within horizon; query $g=1$; assert throws `STATE_INCONSISTENT_TOMBSTONE_MISSING_IN_HORIZON`.
- **Test R18-R19-T13 (Lookup Below Tombstone Floor Returns Historical Expiry):** Query $g \le \mathbf{auditTombstoneFloor}$; assert returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.
- **Test R18-R19-T14 (Complete Retained-Record Identity Consistency Validated):** Mutate `operationIdentity` in `obligation` of retained result; assert `assertRetentionMapContiguity` throws `STATE_INCONSISTENT_RETAINED_RECORD_CORRUPTED`.
- **Test R18-R19-T15 (Drainer Ownership Generation Discrepancy Fails Closed):** Inject mismatched `drainerOwnershipGeneration` in release receipt; assert throws `STATE_INCONSISTENT_RETAINED_RECORD_CORRUPTED`.
- **Test R18-R19-T16 (Settled Audit Record Identity Mismatch Fails Closed):** Inject audit record with differing `transactionId`; call retry; assert throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.
- **Test R18-R19-T17 (Attempt-Independent Settled Retry Bound to Original Token):** Advance coordinator attempt to 2; present retry for $g=1$ presenting Attempt 1 token; assert succeeds cleanly with independent lease.
- **Test R18-R19-T18 (Settled Retry Presenting Current Attempt Token Rejected When Original Token Differs):** Present retry for $g=1$ with Attempt 2 token; assert throws `CUSTODY_PUBLICATION_TOKEN_MISMATCH`.
- **Test R18-R19-T19 (Settlement Evidence Factory Authorization Required):** Pass caller-constructed mock evidence to `releaseSettlementPin`; assert throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.
- **Test R18-R19-T20 (Evidence Pre-Validation of Five Coordinator Facts):** Attempt evidence creation before archive confirms commit; assert factory refuses issuance.
- **Test R18-R19-T21 (Watchdog Timeout Cannot Manufacture Settlement Evidence):** Fire 5,000 ms watchdog; assert status latches `DEGRADED`; assert factory rejects evidence issuance; assert settlement pin remains held.
- **Test R18-R19-T22 (Post-Commit Promise Rejection Settles Committed with Warning):** Promise rejects after commit confirmed; assert factory issues `CONFIRMED_COMMITTED` evidence with execution warning; assert settlement pin released cleanly.
- **Test R18-R19-T23 (Ambiguous Pre-Commit Promise Rejection Latches Blocked):** Promise rejects but archive status inconclusive; assert coordinator latches `CUSTODY_RECONCILIATION_BLOCKED`; assert no evidence issued.
- **Test R18-R19-T24 (Settlement Evidence Replay Rejected):** Attempt to release pin for $g=2$ using evidence for $g=1$; assert throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R19-T25 (Release Retry Lease with Settlement Pin ID Rejected):** Call `releaseRetryLease` with settlement pin ID; assert throws `LEASE_TYPE_MISMATCH`.
- **Test R18-R19-T26 (Release Settlement Pin with Retry Lease ID Rejected):** Call `releaseSettlementPin` with retry lease ID; assert throws `PIN_TYPE_MISMATCH`.
- **Test R18-R19-T27 (Concurrent Retry Acquisition Racing with FIFO Eviction):** Synchronously serialize candidate root replacement; assert either lease acquired before eviction or eviction advances floor before lease.
- **Test R18-R19-T28 (Map Pre-Existing Cap Violation Fails Closed Without Bulk Eviction):** Inject map with 1,005 entries; call publication; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`.
- **Test R18-R19-T29 (Prototype Freeze on Published Collections Prevents Map Mutation):** Invoking `.set()` or `.delete()` on `ImmutableMap` throws `UNSUPPORTED_MUTATION_ON_IMMUTABLE_COLLECTION`.
- **Test R18-R19-T30 (Universal M5.0 Invariant Regression Suite):** Full regression across all Phase 7.5 single-owner, single-drainer, and archive arbitration invariants.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R19):
================================================================================
1. [INDEPENDENT RETRY LEASE OWNERSHIP]: Does the Engineering Authority approve
   replacing shared retry pins with independent consumer retry leases (ImmutableRetryLease),
   ensuring early release by one consumer cannot remove eviction protection required
   by another concurrent consumer?

2. [GENUINELY BOUNDED TOMBSTONE LIFECYCLE]: Does the Engineering Authority approve
   capping generationAuditHistory tombstones at 5,000 entries with strict FIFO retirement,
   enforcing dual key contiguity invariants across results and tombstones, and failing
   closed on missing tombstones within the promised horizon?

3. [COMPLETE RETAINED-RECORD IDENTITY CONSISTENCY]: Does the Engineering Authority approve
   enforcing 6-dimensional identity consistency (generation, 5-field token, tx, op,
   barrier, drainer generation) across all retained records, permanently binding
   original publication tokens to historical generations?

4. [AUDITABLE SETTLEMENT EVIDENCE PROVENANCE]: Does the Engineering Authority approve
   authorizing Level 3 evidence exclusively through SettlementEvidenceFactory,
   pre-validating all five coordinator facts prior to settlement pin release, with
   strict single-use replay protection?

5. [CLOSURE OF ALL ARCHITECTURAL BLOCKERS]: Does the Engineering Authority agree
   that B1 (lease lifetime), B2 (bounded tombstones), C1 (identity consistency),
   and C2 (evidence provenance) are fully closed without introducing new gaps?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R19 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR INDEPENDENT ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R19 proposal completes the formal recovery arbitration architecture. It resolves concurrent consumer retry lease lifetimes, bounds tombstone retention with exact contiguity and FIFO retirement, enforces 6-dimensional identity consistency across all retained records, establishes auditable settlement evidence provenance, and codifies thirty deterministic future test specifications.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R19 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
