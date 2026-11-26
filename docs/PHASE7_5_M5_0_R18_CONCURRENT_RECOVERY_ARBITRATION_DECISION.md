# Phase 7.5 M5.0-R18-R17 — Retention-Pin Authorization, Retry Pinning & Bidirectional Audit Reconciliation
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `521b87f2dcd147f492dc6bea73949d23879b29d8` (M5.0-R18-R16)
**Direct Parent Commit:** `521b87f2dcd147f492dc6bea73949d23879b29d8`
**Authorization Scope:** M5.0-R18-R17 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R17 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R16, we decoupled settled retries from the coordinator's active recovery attempt, capped result retention at 1,000 entries, and introduced retention pins conceptually. However, an independent recovery-systems audit revealed four fundamental architectural vulnerabilities and consistency gaps in R18-R16 that must be corrected:

1. **Unprotected Settlement-Pin Release Authorization:** R18-R16 claimed that watchdog timeout cannot release a retention pin while its underlying Promise remains unsettled. However, the provided `releaseRetentionPin()` pseudocode authorized pin deletion solely by checking caller owner-token equality (`isEquivalentOwnerToken(pin.ownerToken, callerToken)`). This allowed any caller presenting a matching token—or a timeout continuation—to release the pin before Promise settlement or archive commit completed.
2. **Missing Retry Pin Acquisition & Intermediate Exposure:** R18-R16 claimed that retries acquire retention pins, but neither the pending nor settled retry pseudocode actually acquired one in `AuthoritativeCustodyRoot`. A retry caller was told its result was protected while the root contained no pin, exposing the entry to immediate FIFO eviction by concurrent publications.
3. **Unvalidated Pin Identity & Weak Nonce Collision:** `acquireRetentionPin()` accepted caller parameters without validating them against retained publication evidence, and generated pin IDs using `performance.now()`, which is neither unique across microtasks nor collision-safe.
4. **Incomplete Bidirectional Audit Reconciliation:** R18-R16 performed ad-hoc map lookups without formalizing the complete state matrix across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory`, leaving undefined the distinction between legitimately evicted generations, compact audit tombstones, and corrupted state.
5. **Vulnerable FIFO Eviction & Map Non-Contiguity:** R18-R16 equated the oldest generation with `custodyGenerationFloor + 1` while searching for `min(keys())`. These are mathematically equivalent only if the retained result map is strictly contiguous. If an internal anomaly created a key gap, or if the map exceeded 1,000 entries, R18-R16 lacked pre-eviction contiguity verification and fail-closed backpressure.

### Core Architectural Guarantees Established in R18-R17:

- **Capability-Gated Settlement-Pin Release:** Discards caller-token-only release for `SETTLEMENT_BARRIER` pins. Release strictly requires authoritative Level 3 evidence (`AuthoritativeSettlementEvidence`) proving actual Promise settlement, outcome classification, archive disposition, custody reconciliation, and drainer release, matching all 5 token fields, transaction ID, operation ID, barrier ID, attempt ID, and generation. Watchdog timeouts cannot authorize pin release.
- **Atomic Retry Pin Acquisition in Candidate Root:** In-turn pending and settled retries requiring retention protection acquire an `IN_TURN_RETRY` pin via atomic candidate-root replacement before returning the cached result, closing all interleave and gap vulnerabilities.
- **Strict Pin Identity Validation & Monotonic Collision-Free Nonces:** Pin acquisition validates exact generation, full 5-field owner token, transaction ID, operation ID, and barrier ID against `publicationResults`, rejects mismatched pin types, and generates collision-free IDs via coordinator monotonic counter `_pinSequenceCounter: bigint`. Duplicate identical requests are handled idempotently.
- **Complete 12-State Bidirectional Audit Reconciliation Matrix:** Exhaustively defines every permitted and forbidden state across pending obligations, publication results, and audit history, formalizing `ImmutableCompactAuditTombstone` to preserve authoritative proof of historically evicted generations.
- **Strict FIFO Contiguity Verification & Map-Cap Backpressure:** Enforces $\mathbf{publicationResults.keys()} = \{\mathbf{custodyGenerationFloor} + 1, \dots, \mathbf{publishedGeneration}\}$. Pre-eviction verifies contiguity, floor ordering, bounds, and unpinned status. If the map exceeds 1,000 entries, publication fails closed with backpressure, strictly prohibiting bulk eviction.
- **Comprehensive Future Deterministic Test Suite (30 Test Cases):** Codifies thirty deterministic future test specifications in Section 19 covering capability-gated pin release, retry pinning, bidirectional audit reconciliation, and strict FIFO contiguity.

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

### 12.12 The Complete Lifecycle Publication, Bidirectional Audit Reconciliation, Operational Retention-Pin Authorization & Strict FIFO Contiguity

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
  readonly custodyGenerationFloor: number; // Monotonic floor below which detailed results are evicted
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, AuthoritativeSettlementEvidence | ImmutableCompactAuditTombstone>;
  readonly activeRetentionPins: ImmutableMap<string, ImmutableRetentionPin>;
}
```

#### The Complete 12-State Bidirectional Audit Reconciliation Matrix:

To guarantee complete bidirectional consistency across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory`, the recovery engine validates every generation lookup against the authoritative state matrix:

| State | `pendingObligations` | `publicationResults` | `generationAuditHistory` | Valid? | Retry Action | Expired? | Authoritative Engine Action |
|---|---|---|---|---|---|---|---|
| **1** | Absent ($g > \text{pubGen}$) | Absent | Absent | **VALID** | None | No | Eligible for first publication if $g === \text{publishedGeneration} + 1$; sequence gap if higher. |
| **2** | Present | Present | Absent (or In-Prog) | **VALID** | Pending Retry | No | Valid pending obligation. Returns cached result. Invariant: audit terminal status MUST NOT exist. |
| **3** | Absent | Present | Present (Terminal) | **VALID** | Settled Retry | No | Valid settled generation. Returns cached result. Invariant: audit identity MUST match 1-to-1. |
| **4** | Absent | Absent (Evicted) | Present (Tombstone) | **VALID** | Expired | **YES** | Legitimate historical eviction ($g \le \text{floor}$). Returns `CUSTODY_RECEIPT_EXPIRED` with tombstone verification. |
| **5** | Absent | Absent (Evicted) | Absent (Past Horiz) | **VALID** | Expired | **YES** | Historical eviction past tombstone window ($g \le \text{floor} \le \text{pubGen}$). Returns `CUSTODY_RECEIPT_EXPIRED`. |
| **6** | Present | Absent | Any | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_ORPHANED_PENDING_OBLIGATION`. Latches degraded; alerts operator. |
| **7** | Absent ($g > \text{pubGen}$) | Absent | Present | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_FUTURE_GENERATION_AUDITED`. Fails closed. |
| **8** | Absent | Present | Absent | **INVALID**| Fail Closed | No | State corruption: `STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT`. Fails closed. |
| **9** | Present | Present | Present (Terminal) | **INVALID**| Fail Closed | No | State contradiction: `STATE_INCONSISTENT_PENDING_OBLIGATION_CONTRADICTS_AUDIT`. Fails closed. |
| **10**| Any | Present | Present (Mismatched)| **INVALID**| Fail Closed | No | Identity corruption: `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`. Fails closed. |
| **11**| Any | Present | Present (Conflicting)|**INVALID**| Fail Closed | No | Outcome conflict: `STATE_INCONSISTENT_TERMINAL_OUTCOME_CONFLICT`. Fails closed. |
| **12**| Any | Any | Duplicate/Conflicting|**INVALID**| Fail Closed | No | Audit corruption: `STATE_INCONSISTENT_DUPLICATE_AUDIT_RECORD`. Fails closed. |

#### Operational Retention-Pin Authorization & Dual-Lifecycle Management:

1. **Distinct Release Rules by Pin Type:**
   - **`SETTLEMENT_BARRIER` Pin Release:**
     Release is strictly capability-gated. It **CANNOT** be authorized by caller owner-token equality alone, public booleans, or unvalidated parameters. Release **REQUIRES** presenting `AuthoritativeSettlementEvidence` (Level 3 Proof) certifying:
     1. The underlying asynchronous Promise has actually settled (`ExecutionSettlementReceipt` present).
     2. The terminal settlement outcome is classified (`CONFIRMED_COMMITTED` or `ABORTED_FENCED`).
     3. The terminal archive disposition is authoritatively recorded (`ArchiveDispositionReceipt` present).
     4. Custody reconciliation has completed (`CustodyReconciliationReceipt` present).
     5. The drainer has actually relinquished the relevant custody (`DrainerCustodyReleaseReceipt` present).
     6. All six identity fields (5 token fields, transaction ID, operation ID, barrier ID, attempt ID, and generation) match the pin 1-to-1.
   - **`IN_TURN_RETRY` Pin Release:**
     Release is authorized upon completion or failure of the retry turn. Validates caller's 5-field `ownerToken` against `pin.ownerToken`. A stale attempt cannot release a newer attempt's retry pin.
2. **Watchdog Timeout Pin Immunity:**
   A watchdog timeout logs diagnostics and latches status `DEGRADED`. It **CANNOT** manufacture Promise settlement, archive disposition, custody reconciliation, or pin release authorization. A `SETTLEMENT_BARRIER` pin remains pinned until actual Level 3 settlement evidence is presented or the process terminates.
3. **In-Turn Retry Pin Acquisition in Candidate Root:**
   When an in-turn retry arrives (pending or settled), the coordinator validates the 5-field token, transaction ID, operation ID, and barrier ID against `publicationResults`. Before returning the receipt, if retention protection is required across turns, the coordinator acquires an `IN_TURN_RETRY` pin via synchronous candidate-root replacement.
4. **Collision-Free Monotonic Pin Identity:**
   Pin IDs do not rely on `performance.now()`. They use a coordinator-scoped monotonic sequence counter:
   $$\mathbf{pinId} = `\text{pin}:${this.\_coordinatorInstanceId}:${generation}:${operationId}:${pinType}:${++this.\_pinSequenceCounter}`$$
5. **Idempotent Duplicate-Acquisition Semantics:**
   If a pin already exists with matching `(generation, operationId, pinType)`:
   - If all identity fields match 1-to-1: returns existing `pinId` without mutating root.
   - If any field conflicts: fails closed with `RETENTION_PIN_COLLISION_MUTATED_IDENTITY`.

#### Strict FIFO Contiguity and Capacity Invariants:

1. **The Contiguous Generation Formula:**
   Subject to the empty-cache case ($\mathbf{publicationResults.size} = 0 \iff \mathbf{custodyGenerationFloor} = \mathbf{publishedGeneration}$), the retained result map must hold the exact contiguous sequence:
   $$\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1,\;\; \mathbf{custodyGenerationFloor} + 2,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
2. **Pre-Eviction Verification:**
   Before evicting generation $g_{\text{oldest}} = \mathbf{custodyGenerationFloor} + 1$:
   - Verify size matches generation span: $\mathbf{publicationResults.size} === \mathbf{publishedGeneration} - \mathbf{custodyGenerationFloor}$. If unequal, fail closed with `STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS`.
   - Verify no key is $\le \mathbf{custodyGenerationFloor}$ or $> \mathbf{publishedGeneration}$.
   - Verify $g_{\text{oldest}}$ is present.
   - Verify $g_{\text{oldest}}$ has no pending settlement obligation.
   - Verify $g_{\text{oldest}}$ has no active retention pin.
3. **Capacity Ceiling Backpressure (No Bulk Eviction):**
   If `publicationResults.size > 1000` prior to publication, a single eviction cannot restore the bound. The coordinator **MUST NOT** silently bulk-evict multiple generations. Instead, publication fails closed with `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED` and enters an explicit reconciliation path.
4. **Strict FIFO Preservation:**
   Eviction **CANNOT** skip a pinned oldest entry to evict a newer entry. Skipping would create a gap in the contiguous generation range and violate monotonic floor invariants.

#### Complete Corrected Pseudocode:

```typescript
public publishReleaseAndObligation(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
  barrierIdentity: string,
): PublicationResult {
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

      // Cross-Collection Audit Check: Pending obligation CANNOT have terminal audit
      if (currentRoot.generationAuditHistory.has(g)) {
        const audit = currentRoot.generationAuditHistory.get(g)!;
        if (audit.terminalOutcome === 'CONFIRMED_COMMITTED' || audit.terminalOutcome === 'ABORTED_FENCED') {
          throw new Error('STATE_INCONSISTENT_PENDING_OBLIGATION_CONTRADICTS_AUDIT');
        }
      }

      // Bidirectional Identity Check across obligation, cached result, and safeOwner
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

      // Validate caller token matches immutable obligation
      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, safeOwner) ||
        existingObligation.transactionId !== safeTxId ||
        existingObligation.operationIdentity !== safeOpId ||
        existingObligation.barrierIdentity !== safeBarrierId
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      return cachedResult;
    }

    // Case 3B: Published and Fully Settled (State 3)
    if (currentRoot.publicationResults.has(g)) {
      const cachedResult = currentRoot.publicationResults.get(g)!;

      // Mandatory Audit Evidence Check for settled generations
      const auditEvidence = currentRoot.generationAuditHistory.get(g);
      if (!auditEvidence) {
        throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');
      }

      // Bidirectional Identity and Terminal Outcome Check
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

      return cachedResult;
    }

    // Case 3C: Published but Outside Retention Horizon (Evicted / States 4 & 5)
    if (g <= currentRoot.custodyGenerationFloor) {
      throw new Error('CUSTODY_RECEIPT_EXPIRED');
    }

    // If g <= publishedGeneration but not in results and not below floor -> gap corruption!
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

  // First-Publication Attempt Fencing: MUST match active coordinator attempt
  if (safeOwner.recoveryAttemptId !== this._currentAttemptId) {
    throw new Error('STALE_ATTEMPT_CUSTODY_RELEASE_REJECTED');
  }

  // Audit History Check: Unpublished generation cannot have audit record (State 7)
  if (currentRoot.generationAuditHistory.has(g)) {
    throw new Error('STATE_INCONSISTENT_FUTURE_GENERATION_AUDITED');
  }

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

  // Strict FIFO Contiguity Verification and Bounded Eviction
  let candidateFloor = currentRoot.custodyGenerationFloor;
  const candidateResultsMap = new Map(currentRoot.publicationResults.entries());

  if (candidateResultsMap.size > 0) {
    // Verify contiguity invariant: size === publishedGeneration - custodyGenerationFloor
    if (candidateResultsMap.size !== currentRoot.publishedGeneration - currentRoot.custodyGenerationFloor) {
      throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
    }
  }

  // Map Capacity Check
  if (candidateResultsMap.size >= 1000) {
    // If map already exceeds 1,000, reject publication to prevent unmonitored bulk eviction
    if (candidateResultsMap.size > 1000) {
      throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    const g_oldest = candidateFloor + 1;

    // Verify oldest entry is present
    if (!candidateResultsMap.has(g_oldest)) {
      throw new Error('STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS');
    }

    // Safety Check 1: Cannot evict if pending settlement obligation
    if (currentRoot.pendingSettlementObligations.has(g_oldest)) {
      throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }

    // Safety Check 2: Cannot evict if active retention pin exists
    for (const pin of currentRoot.activeRetentionPins.values()) {
      if (pin.generation === g_oldest) {
        // Strict FIFO preservation: CANNOT skip pinned oldest entry!
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
      }
    }

    candidateResultsMap.delete(g_oldest);
    candidateFloor = g_oldest; // Monotonic advance
  }

  candidateResultsMap.set(g, publicationResult);

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

  // THE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;
  return publicationResult;
}

public acquireRetentionPin(
  generation: number,
  operationId: string,
  pinType: RetentionPinType,
  callerToken: CustodyOwnershipToken,
  transactionId: string,
  barrierIdentity: string,
): string {
  const currentRoot = this.#custodyRoot;
  assertValidGenerationDomain(generation);

  // 1. Generation domain and horizon check
  if (generation <= currentRoot.custodyGenerationFloor || !currentRoot.publicationResults.has(generation)) {
    throw new Error('CUSTODY_RECEIPT_EXPIRED');
  }

  const cachedResult = currentRoot.publicationResults.get(generation)!;
  const safeOwner = cloneAndFreezeOwnerToken(callerToken);
  const safeTxId = String(transactionId);
  const safeOpId = String(operationId);
  const safeBarrierId = String(barrierIdentity);

  // 2. Validate full 5-field token against cached publication result
  if (!isEquivalentOwnerToken(cachedResult.releaseReceipt.identity, safeOwner)) {
    throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
  }

  // 3. Validate transaction, operation, and barrier identity
  if (
    cachedResult.obligation.transactionId !== safeTxId ||
    cachedResult.obligation.operationIdentity !== safeOpId ||
    cachedResult.obligation.barrierIdentity !== safeBarrierId
  ) {
    throw new Error('CUSTODY_RETRY_ID_MISMATCH');
  }

  // 4. Validate permitted pin type for publication state
  if (pinType === 'SETTLEMENT_BARRIER') {
    if (!currentRoot.pendingSettlementObligations.has(generation)) {
      throw new Error('RETENTION_PIN_INVALID_TYPE_FOR_SETTLED_GENERATION');
    }
  }

  // 5. Idempotent Deduplication Check
  for (const existingPin of currentRoot.activeRetentionPins.values()) {
    if (
      existingPin.generation === generation &&
      existingPin.operationId === safeOpId &&
      existingPin.pinType === pinType
    ) {
      if (
        isEquivalentOwnerToken(existingPin.ownerToken, safeOwner) &&
        existingPin.transactionId === safeTxId &&
        existingPin.barrierIdentity === safeBarrierId
      ) {
        return existingPin.pinId; // Idempotent return of existing pin
      }
      throw new Error('RETENTION_PIN_COLLISION_MUTATED_IDENTITY');
    }
  }

  // 6. Monotonic Nonce Generation (Collision-Free)
  const seq = ++this._pinSequenceCounter;
  const pinId = `pin:${this._coordinatorInstanceId}:${generation}:${safeOpId}:${pinType}:${seq}`;

  if (currentRoot.activeRetentionPins.has(pinId)) {
    throw new Error('RETENTION_PIN_ID_COLLISION');
  }

  const pin: ImmutableRetentionPin = Object.freeze({
    pinId,
    generation,
    operationId: safeOpId,
    pinType,
    ownerToken: safeOwner,
    transactionId: safeTxId,
    barrierIdentity: safeBarrierId,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
  });

  const candidatePins = new ImmutableMap([
    ...currentRoot.activeRetentionPins.entries(),
    [pinId, pin],
  ]);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeRetentionPins: candidatePins,
  });

  return pinId;
}

public releaseSettlementBarrierPin(
  pinId: string,
  settlementEvidence: AuthoritativeSettlementEvidence,
): void {
  const currentRoot = this.#custodyRoot;
  const pin = currentRoot.activeRetentionPins.get(pinId);

  // Idempotent: already released
  if (!pin) return;

  if (pin.pinType !== 'SETTLEMENT_BARRIER') {
    throw new Error('RETENTION_PIN_TYPE_MISMATCH_EXPECTED_SETTLEMENT');
  }

  // STRICT CAPABILITY-GATED RELEASE AUTHORIZATION:
  // Requires Level 3 AuthoritativeSettlementEvidence matching all 6 identity fields
  assertAuthoritativeSettlementEvidence(settlementEvidence);

  if (
    !isEquivalentOwnerToken(pin.ownerToken, settlementEvidence.expectedOwner) ||
    pin.generation !== settlementEvidence.generation ||
    pin.transactionId !== settlementEvidence.transactionId ||
    pin.operationId !== settlementEvidence.operationIdentity ||
    pin.barrierIdentity !== settlementEvidence.barrierIdentity ||
    pin.ownerToken.recoveryAttemptId !== settlementEvidence.expectedOwner.recoveryAttemptId
  ) {
    throw new Error('SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH');
  }

  const candidatePinsMap = new Map(currentRoot.activeRetentionPins.entries());
  candidatePinsMap.delete(pinId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeRetentionPins: new ImmutableMap(candidatePinsMap.entries()),
  });
}

public releaseRetryPin(
  pinId: string,
  callerToken: CustodyOwnershipToken,
): void {
  const currentRoot = this.#custodyRoot;
  const pin = currentRoot.activeRetentionPins.get(pinId);

  if (!pin) return; // Idempotent

  if (pin.pinType !== 'IN_TURN_RETRY') {
    throw new Error('RETENTION_PIN_TYPE_MISMATCH_EXPECTED_RETRY');
  }

  // Stale attempt fencing: caller must match pin's ownerToken
  if (!isEquivalentOwnerToken(pin.ownerToken, callerToken)) {
    throw new Error('STALE_ATTEMPT_PIN_RELEASE_REJECTED');
  }

  const candidatePinsMap = new Map(currentRoot.activeRetentionPins.entries());
  candidatePinsMap.delete(pinId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeRetentionPins: new ImmutableMap(candidatePinsMap.entries()),
  });
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
| **Phase 1: Validation** | Out-of-domain generation | Synchronous throw | Root unchanged; no allocation |
| **Phase 2: Ingress Copy** | Deep copy memory failure | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 3: Pre-Allocation**| Object instantiation fail| Synchronous throw | Root unchanged; in-flight retained |
| **Phase 4: Candidate Root**| Map construction fail | Synchronous throw | Root unchanged; in-flight retained |
| **Phase 5: Contiguity Check**| Map has key gaps | Fail-closed throw | Root unchanged; alerts operator |
| **Phase 6: Eviction Check**| Oldest entry pinned | Backpressure throw | Root unchanged; capacity preserved |
| **Phase 7: Atomic Swap** | Pointer replacement | Infallible | Single linearization point |
| **Phase 8: Return Ref** | Return cached pointer | Infallible | Zero post-publication allocation |

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
- **Case H (Delayed Settled Retry After Attempt Change):** Settled retry arrives with Attempt 1 token while Attempt 2 is active or attempt is null; identity matched against cached receipt and audit record; returns cached receipt immediately without error.

---

## 15. Exhaustive Failure and Retry Matrix (24 Deterministic Cases)

| Case | Scenario | Gate State | Custody Owner | Barrier State | Correct Action | Forbidden Action |
|---|---|---|---|---|---|---|
| **1** | Attempt 1 times out; Attempt 2 admitted | `BARRIER_PENDING` | Drainer | PENDING | Attempt 2 waits at Step 4 | Attempt 2 steals custody |
| **2** | Attempt 1 Promise resolves post-timeout | `BARRIER_PENDING` | Drainer | PENDING | Settle barrier; release pin with evidence; unblock Attempt 2 | Drop Attempt 1 result |
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
| **13** | Settlement pin release without Level 3 proof | `BARRIER_PENDING` | Drainer | PENDING | Throw evidence required | Release pin on token alone |
| **14** | Pin release on actual Level 3 settlement | `SETTLED` | Archive | RESOLVED | Remove pin atomically | Leak retention pin |
| **15** | Retry on evicted generation ($g \le \text{floor}$)| `ANY` | Archive | RESOLVED | Throw `RECEIPT_EXPIRED` | Fall through to first pub |
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
3. **Attempt-Fenced First Publication Invariant:**
   $$\mathbf{FIRST\_PUBLICATION}(g) \implies (\mathbf{callerAttempt} === \mathbf{\_currentAttemptId}) \land (\mathbf{inFlightRecord.token} === \mathbf{callerToken}) \land (g === \mathbf{publishedGeneration} + 1)$$
4. **Settled Retry Attempt Independence Invariant:**
   A settled retry validates exact 1-to-1 token identity against cached evidence and authoritative audit history, completely independent of `_currentAttemptId`.
5. **Bidirectional Audit Consistency Invariant:**
   Any structural discrepancy across `pendingSettlementObligations`, `publicationResults`, and `generationAuditHistory` fails closed.
6. **Strict FIFO Contiguity Invariant:**
   $\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1, \dots, \mathbf{publishedGeneration}\}$; key gaps fail closed.
7. **Operational Retention Pin and Strict FIFO Invariant:**
   A pinned oldest generation CANNOT be evicted; eviction cannot skip pinned entries; capacity exhaustion applies fail-closed backpressure.
8. **Watchdog Pin Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins whose underlying Promises remain unsettled.
9. **Empty-Cache Floor Invariant:**
   $$0 \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
   An evicted generation ($g \le \mathbf{custodyGenerationFloor}$) can never re-enter first publication.
10. **Strict Tombstone Capacity Ceiling:** `#fencedTransactions.size` **CANNOT** exceed 1,000 under any condition.
11. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
12. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.
13. **Inseparable Release/Obligation Invariant:** A released custody state can **never** become observable without its corresponding settlement obligation already present in `this.#custodyRoot.pendingSettlementObligations`.
14. **Runtime Immutability Invariant:** Published collections and evidence references **CANNOT** be mutated through `Map.prototype` or mutable array/buffer aliases.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)} \longrightarrow \text{6. Release Settlement Pin}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R17 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution, Capability-Gated Pin Release & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. The settlement barrier resolves with verified `AuthoritativeSettlementEvidence`. This evidence authorizes `releaseSettlementBarrierPin`, freeing generation $g$ for eventual FIFO eviction. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following thirty deterministic future test specifications are formally codified for Sub-Milestone M5.1:

- **Test R18-R17-T01 (Settlement Pin Cannot Be Released Before Actual Promise Settlement):** Acquire `SETTLEMENT_BARRIER` pin on $g=1$. Call `releaseSettlementBarrierPin` presenting caller token and null evidence; assert throws `AUTHORITATIVE_SETTLEMENT_EVIDENCE_REQUIRED`; assert pin remains held in `activeRetentionPins`.
- **Test R18-R17-T02 (Watchdog Timeout Does Not Release Settlement Pin):** Acquire `SETTLEMENT_BARRIER` pin on $g=1$. Fire 5,000 ms watchdog timer; assert coordinator status latches `DEGRADED`; assert `activeRetentionPins.has(pinId)` remains `true`.
- **Test R18-R17-T03 (Promise Rejection After Watchdog Timeout Permits Only Correctly Classified Settlement Path):** Fire watchdog on $g=1$; subsequent transport rejection arrives; assert reconciliation executes Protocol B; assert pin cannot be released with Protocol A commit evidence; assert pin release succeeds only with verified Protocol B aborted evidence.
- **Test R18-R17-T04 (Archive Commit and Custody Reconciliation Both Required Before Settlement Pin Release):** Present settlement evidence missing `CustodyReconciliationReceipt`; assert throws `SETTLEMENT_EVIDENCE_INCOMPLETE`; assert pin remains held.
- **Test R18-R17-T05 (Drainer Release Evidence Must Match Exact Ownership Generation):** Present settlement evidence where `drainerRelease.releasedCustodyGeneration !== pin.generation`; assert throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.
- **Test R18-R17-T06 (Stale Owner Cannot Release Newer Attempt's Pin):** Pin acquired with Attempt 2 token. Present release call with Attempt 1 token; assert throws `STALE_ATTEMPT_PIN_RELEASE_REJECTED`.
- **Test R18-R17-T07 (Retry Acquires Pin Before Reporting Retention Protection):** Call retry inquiry on $g=1$ requiring guarded turn; assert candidate root contains `IN_TURN_RETRY` pin before method returns; assert returned receipt is protected.
- **Test R18-R17-T08 (Retry Registration and Pin Publication Cannot Expose Intermediate Unprotected State):** Concurrently observe root during retry; assert zero microtask window where retry is acknowledged but pin is absent from `activeRetentionPins`.
- **Test R18-R17-T09 (Eviction Cannot Remove Pinned Result):** 1,000 entries populated; $g=1$ has active `IN_TURN_RETRY` pin. Call first publication of $g=1001$; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`; assert $g=1$ remains in `publicationResults`.
- **Test R18-R17-T10 (Identical Retry Acquisition Follows Defined Idempotency Semantics):** Call `acquireRetentionPin` twice with identical parameters; assert second call returns existing `pinId`; assert `activeRetentionPins.size` does not increase.
- **Test R18-R17-T11 (Conflicting Duplicate Pin Identity Fails Closed):** Call `acquireRetentionPin` for existing `(generation, operationId, pinType)` but mutated transaction ID; assert throws `RETENTION_PIN_COLLISION_MUTATED_IDENTITY`.
- **Test R18-R17-T12 (Pin ID Collision Is Detected and Fails Closed):** Artificially inject pin with existing monotonic sequence counter; assert throws `RETENTION_PIN_ID_COLLISION`.
- **Test R18-R17-T13 (Pin Acquisition Rejects Mismatched Transaction Identity):** Call `acquireRetentionPin` with transaction ID differing from `publicationResults.get(g).obligation.transactionId`; assert throws `CUSTODY_RETRY_ID_MISMATCH`.
- **Test R18-R17-T14 (Pin Acquisition Rejects Mismatched Operation Identity):** Call `acquireRetentionPin` with operation ID differing from cached obligation; assert throws `CUSTODY_RETRY_ID_MISMATCH`.
- **Test R18-R17-T15 (Pin Acquisition Rejects Mismatched Barrier Identity):** Call `acquireRetentionPin` with barrier ID differing from cached obligation; assert throws `CUSTODY_RETRY_ID_MISMATCH`.
- **Test R18-R17-T16 (Audit Record Without Result Classified According to Explicit Tombstone Rules):** Generation $g=1$ evicted ($g \le \text{floor}$); query generation with compact audit tombstone present; assert returns `CUSTODY_RECEIPT_EXPIRED` with tombstone confirmation.
- **Test R18-R17-T17 (Result Without Terminal Audit Cannot Be Treated as Settled):** Artificially inject result into `publicationResults` without pending obligation and without audit record; call retry; assert throws `STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT`.
- **Test R18-R17-T18 (Pending Obligation with Terminal Audit Fails Closed):** Obligation present in `pendingSettlementObligations` and audit record present with `CONFIRMED_COMMITTED`; call retry; assert throws `STATE_INCONSISTENT_PENDING_OBLIGATION_CONTRADICTS_AUDIT`.
- **Test R18-R17-T19 (Conflicting Terminal Audit Outcomes Fail Closed):** Audit record contains unrecognized or conflicting status; assert throws `STATE_INCONSISTENT_TERMINAL_OUTCOME_CONFLICT`.
- **Test R18-R17-T20 (Evicted Result with Valid Audit Tombstone Returns Defined Expired-Result Behavior):** Query evicted $g \le \mathbf{custodyGenerationFloor}$; assert throws `CUSTODY_RECEIPT_EXPIRED`; assert never treated as unpublished.
- **Test R18-R17-T21 (Unknown Generation with No Audit Evidence Distinguished from Contradictory Evidence):** Query $g = \text{publishedGeneration} + 5$; assert throws `GENERATION_OUT_OF_SEQUENCE_GAP_DETECTED` (distinct from consistency failure).
- **Test R18-R17-T22 (Contiguous Retained-Generation Invariant Is Checked Before Eviction):** Artificially delete middle key from `publicationResults` (creating gap); attempt publication; assert throws `STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS`.
- **Test R18-R17-T23 (Gap in Retained Generations Fails Closed):** Verify that key gaps prevent eviction and never silently default to `min(keys())`.
- **Test R18-R17-T24 (Generation Above publishedGeneration Fails Closed):** Inject result with $g > \mathbf{publishedGeneration}$; attempt eviction check; assert throws `STATE_INCONSISTENT_RESULT_GENERATION_OUT_OF_BOUNDS`.
- **Test R18-R17-T25 (Map Already Exceeding Retention Cap Follows Explicit Backpressure Rule):** Inject map with 1,005 entries; attempt publication; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED` without bulk-evicting entries.
- **Test R18-R17-T26 (Pinned Oldest Result Blocks Eviction Without Skipping to Newer Result):** Oldest entry $g=1$ has `SETTLEMENT_BARRIER` pin; entries $g=2 \dots 1000$ unpinned. Attempt publication; assert throws `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`; assert $g=2$ is NOT evicted.
- **Test R18-R17-T27 (Fully Empty Cache Can Publish Next Sequential Generation):** Cache fully evicted ($g_{\text{floor}} = N$, $g_{\text{pub}} = N$); publish $g = N + 1$; assert succeeds cleanly; assert new cache size is 1.
- **Test R18-R17-T28 (Existing Retry Attempt-Independence Tests Remain Passing):** Execute settled retries with null `_currentAttemptId` and newer attempt ID; assert succeeds cleanly with zero root mutation.
- **Test R18-R17-T29 (Existing Alias-Closure and Defensive-Copy Requirements Remain Intact):** Mutating input tokens or digests does not alter published candidate root.
- **Test R18-R17-T30 (Existing M5.0 Watchdog, Archive, Settlement, Custody, and Barrier Invariants Remain Covered):** Verify full regression suite across all Phase 7.5 architectural invariants.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R17):
================================================================================
1. [CAPABILITY-GATED SETTLEMENT-PIN RELEASE]: Does the Engineering Authority approve
   requiring AuthoritativeSettlementEvidence (Level 3 Proof) for releasing
   SETTLEMENT_BARRIER pins, strictly prohibiting release on token equality alone
   or via watchdog timeout?

2. [ATOMIC RETRY PINNING IN CANDIDATE ROOT]: Does the Engineering Authority approve
   acquiring IN_TURN_RETRY pins via atomic candidate-root replacement before
   returning retry receipts, closing all interleave and gap vulnerabilities?

3. [STRICT PIN VALIDATION & MONOTONIC NONCES]: Does the Engineering Authority approve
   validating full 5-field token, transaction ID, operation ID, and barrier ID
   against publicationResults prior to pin acquisition, using coordinator-scoped
   monotonic sequence counters for collision-free pin IDs?

4. [BIDIRECTIONAL AUDIT RECONCILIATION MATRIX]: Does the Engineering Authority approve
   the complete 12-state bidirectional audit matrix across pending obligations,
   publication results, and audit history, including compact audit tombstones?

5. [STRICT FIFO CONTIGUITY & CAPABILITY BACKPRESSURE]: Does the Engineering Authority
   approve enforcing strict key contiguity in publicationResults prior to eviction,
   and failing closed with backpressure without bulk eviction if capacity exceeds 1,000?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R17 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R17 proposal completes the formal recovery arbitration architecture. It capability-gates settlement-pin release with Level 3 authoritative evidence, closes retry pinning gaps, enforces full pin identity validation with monotonic nonces, codifies the complete 12-state bidirectional audit matrix, establishes strict FIFO contiguity verification, and codifies thirty deterministic future test specifications.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R17 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
