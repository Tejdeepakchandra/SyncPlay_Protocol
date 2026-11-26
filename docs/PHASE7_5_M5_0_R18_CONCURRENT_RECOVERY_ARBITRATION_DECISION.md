# Phase 7.5 M5.0-R18-R20 — Retry Lease Idempotency, Audit Lifecycle Consistency & Evidence Authorization Closure
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `06c696a45a4cfabec1e2784d1a816d731fdc4dcb` (M5.0-R18-R19)
**Direct Parent Commit:** `06c696a45a4cfabec1e2784d1a816d731fdc4dcb`
**Authorization Scope:** M5.0-R18-R20 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R20 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R19, we introduced independent consumer retry leases and bounded tombstone FIFO retirement. However, a rigorous recovery-systems audit revealed four critical architectural blockers in the R18-R19 model:

1. **Unstable Retry Request Identity & Cross-Consumer Release Exposure (B1):**
   - In R18-R19, `publishReleaseAndObligation` permitted `clientRequestId = req-${performance.now()}` when `retryContext` was omitted. This defeated retransmission idempotency because a client retrying after a lost network response would generate a fresh timestamp identifier and allocate redundant leases.
   - Furthermore, `releaseRetryLease()` checked the caller recovery owner token but failed to independently verify that the caller consumer owns the lease (`lease.consumerId === params.consumerId`). Consequently, Consumer 1 could erroneously release Consumer 2's lease merely because both operated in the same recovery attempt.
2. **Missing Current-Generation Audit History & Incomplete Tombstone Protection (B2):**
   - In R18-R19 first publication, `publishReleaseAndObligation` staged `publicationResults` and `pendingSettlementObligations`, and then asserted `generationAuditHistory` contiguity through $g$, without ever inserting generation $g$ into `generationAuditHistory`. This created a contradictory invariant where contiguity assertions either passed falsely or failed closed.
   - When retiring the oldest tombstone at `auditTombstoneFloor + 1`, R18-R19 checked active retry leases but neglected to check active settlement pins or pending settlement obligations.
   - Capacity enforcement lacked strict pre-existing error handling, and floor invariants were not bounded by non-negative safe integers.
3. **Incomplete Retained-Record Identity Consistency (C1):**
   - While key sequence contiguity was checked, structural identity verification across `generationAuditHistory` did not employ discriminated schemas for pending vs. terminal vs. tombstoned records, leaving historical attempts vulnerable to token identity confusion.
4. **Unspecified Evidence Factory Authorization & Cross-Resource Semantics (C2 & Cross-Resource):**
   - R18-R19 referenced `AuthoritativeSettlementEvidence` without defining a private internal capability or registry mechanism within `@syncplay/protocol` to prevent mock or caller-constructed evidence.
   - Passing a settlement pin ID to `releaseRetryLease` or a retry lease ID to `releaseSettlementPin` was ambiguous or conflated.

### Core Architectural Guarantees Established in R18-R20:

- **Mandatory Stable Request Identity & Independent Release Authority (B1):**
  - `RetryRequestContext` (`consumerId`, `clientRequestId`) is strictly mandatory on retry acquisition. Omitting, mutating, or supplying malformed/oversized (> 256 chars) context fails closed.
  - `releaseRetryLease()` requires explicit 7-dimensional binding: `leaseId`, `consumerId`, `clientRequestId`, `callerToken`, `transactionId`, `operationIdentity`, and `barrierIdentity`. Consumer 1 is strictly unauthorized to release Consumer 2's lease (`UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`).
  - Full lease lifecycle defined across lost responses, retransmissions, timeouts, duplicate releases, and attempt advancement.
- **Formal Option A Audit-History Model & Complete Tombstone Retirement Protection (B2):**
  - Formally selects and documents **Option A**: `generationAuditHistory` contains lifecycle-discriminated records: `ImmutablePendingAuditRecord` (`PENDING_PUBLICATION`), `AuthoritativeSettlementEvidence` (`TERMINAL_SETTLEMENT`), and `ImmutableCompactAuditTombstone` (`COMPACT_TOMBSTONE`).
  - On first publication, `ImmutablePendingAuditRecord` is published atomically into `generationAuditHistory`, guaranteeing unconditional key contiguity $g \in [\mathbf{auditTombstoneFloor} + 1, \dots, \mathbf{publishedGeneration}]$.
  - Tombstone retirement checks all authoritative references: active retry leases, active settlement pins, and pending settlement obligations. If any reference protects the oldest tombstone, retirement applies fail-closed backpressure (`TOMBSTONE_CAPACITY_EXHAUSTED`).
  - Strict capacity enforcement (`MAX_AUDIT_TOMBSTONES = 5000`): Pre-existing counts > 5000 fail closed (`TOMBSTONE_CAPACITY_CORRUPTED`). Oldest tombstone must be verified as a compact tombstone before deletion. `auditTombstoneFloor` advances atomically with deletion.
  - Safe integer floor invariants: $0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$.
- **Complete Retained-Record Identity Consistency (C1):**
  - Formal discriminated schemas enforce 6-dimensional verification across map keys, owner tokens, transaction IDs, operation identities, barrier identities, and drainer generations. Historical records remain permanently bound to original publication tokens.
- **Auditable Evidence Factory Authorization & Single-Use Replay Protection (C2):**
  - Authorizes `SettlementEvidenceFactory` as the sole internal issuer within `@syncplay/protocol` via private symbol and internal capability registry.
  - Validates 5 core coordinator facts prior to evidence issuance. Watchdog timeout produces Level 1 receipts and can never manufacture Level 3 settlement evidence.
  - Enforces single-use replay protection per barrier and obligation ID.
- **Distinct Cross-Resource Identifier Semantics:**
  - `releaseRetryLease(pinId)` throws `LEASE_TYPE_MISMATCH`.
  - `releaseSettlementPin(leaseId)` throws `PIN_TYPE_MISMATCH`.
  - Unknown or never-issued identifiers throw `RESOURCE_NOT_FOUND`.
  - Duplicate release by the authorized caller remains an idempotent no-op.
- **Exhaustive Deterministic Test Suite (30 Test Cases):**
  - Codifies thirty deterministic future test specifications in Section 19 covering every R18-R20 requirement.

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
- **Core Safe-Integer Floor Invariant:**
  $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
  All floors and generations must be non-negative safe integers (`Number.isSafeInteger(x) && x >= 0`).

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

- If `MAX_TOMBSTONES = 1000` is reached in archive or `MAX_AUDIT_TOMBSTONES = 5000` in custody, eviction selects the lowest generation $g$.
- If $g$ belongs to an active retry lease, settlement pin, or pending obligation, eviction is blocked.
- **Strict FIFO Preservation:** Eviction **cannot** skip active reservations. Skipping would create generation gaps in the tombstone map and violate monotonic floor invariants.
- Instead, the coordinator applies **strict backpressure**: new requests are rejected with `TOMBSTONE_CAPACITY_EXHAUSTED` until active operations complete and release their pins or leases.

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
- **Replay Protection:** Evidence is single-use for resolving the specific barrier and releasing the specific `SETTLEMENT_BARRIER` pin for generation $g$. Replaying evidence for another generation or obligation is rejected with `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

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

### 12.12 The Complete Lifecycle Publication, Retry Lease Idempotency, Bounded Tombstone Retirement & Complete Consistency

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

export interface RetryRequestContext {
  readonly consumerId: string;
  readonly clientRequestId: string;
}

export interface ImmutableRetryLease {
  readonly leaseId: string; // lease:${coordinatorInstanceId}:${generation}:${consumerId}:${seq}
  readonly generation: number;
  readonly consumerId: string;
  readonly clientRequestId: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly acquiredAtTimestamp: number;
  readonly monotonicAcquiredMs: number;
  readonly expiresAtMonotonicMs: number;
}

// Option A: Lifecycle-Discriminated Audit Records
export interface ImmutablePendingAuditRecord {
  readonly kind: 'PENDING_PUBLICATION';
  readonly generation: number;
  readonly expectedOwner: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly publishedAtTimestamp: number;
  readonly monotonicPublishedMs: number;
}

export interface AuthoritativeSettlementEvidence {
  readonly kind: 'TERMINAL_SETTLEMENT';
  readonly [SETTLEMENT_EVIDENCE_BRAND]: true;
  readonly generation: number;
  readonly expectedOwner: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly terminalOutcome: 'CONFIRMED_COMMITTED' | 'ABORTED_FENCED';
  readonly settledAtTimestamp: number;
  readonly monotonicSettledMs: number;
  readonly releaseReceipt: DrainerCustodyReleaseReceipt;
  readonly reconciliationReceipt: CustodyReconciliationReceipt;
  readonly archiveReceipt: ArchiveDispositionReceipt;
  readonly executionReceipt: ExecutionSettlementReceipt;
  readonly evidenceDigest: Uint8Array;
}

export interface ImmutableCompactAuditTombstone {
  readonly kind: 'COMPACT_TOMBSTONE';
  readonly generation: number;
  readonly expectedOwner: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly terminalOutcome: 'CONFIRMED_COMMITTED' | 'ABORTED_FENCED';
  readonly settledAtTimestamp: number;
  readonly monotonicSettledMs: number;
  readonly evidenceDigest: Uint8Array;
}

export type GenerationAuditRecord =
  | ImmutablePendingAuditRecord
  | AuthoritativeSettlementEvidence
  | ImmutableCompactAuditTombstone;

export interface AuthoritativeCustodyRoot {
  readonly publishedGeneration: number;
  readonly custodyGenerationFloor: number; // Floor below which results are evicted
  readonly auditTombstoneFloor: number;    // Floor below which tombstones are retired
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, GenerationAuditRecord>;
  readonly activeSettlementPins: ImmutableMap<string, ImmutableSettlementPin>;
  readonly activeRetryLeases: ImmutableMap<string, ImmutableRetryLease>;
}

export interface ReleaseRetryLeaseParams {
  readonly leaseId: string;
  readonly consumerId: string;
  readonly clientRequestId: string;
  readonly callerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
}
```

#### Independent Consumer Retry Lease Model & Release Authorization (B1):

To resolve the shared-pin premature release vulnerability and establish retransmission idempotency:
1. **Mandatory Stable Request Identity:** `RetryRequestContext` (`consumerId`, `clientRequestId`) is mandatory on all retry paths. Both fields must be non-empty strings of at most 256 characters. Using newly generated timestamps or random values at runtime is strictly prohibited. Missing context throws `INVALID_RETRY_REQUEST_CONTEXT`. Malformed context throws `MALFORMED_RETRY_REQUEST_CONTEXT`. Oversized context throws `RETRY_REQUEST_CONTEXT_OVERSIZED`.
2. **Idempotent Retransmission Recovery:** When a consumer retransmits a retry request with matching `(generation, consumerId, clientRequestId)`, the coordinator recovers the existing `leaseId` without allocating a duplicate lease or leaking references. Conflicting identity fields fail closed with `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.
3. **Independent Consumer Release Authority:** `releaseRetryLease(params: ReleaseRetryLeaseParams)` binds and verifies all seven dimensions: `leaseId`, `consumerId`, `clientRequestId`, `callerToken`, `transactionId`, `operationIdentity`, and `barrierIdentity`. Consumer 1 cannot release Consumer 2's lease (`UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`).
4. **Complete Lease Lifecycle:**
   - *Lost Acquisition Response:* Consumer retransmits with same `(consumerId, clientRequestId)`; existing lease returned idempotently.
   - *Repeated Acquisition with Identical Identity:* Returns existing `ImmutableRetryLease`.
   - *Repeated Acquisition with Conflicting Identity:* Throws `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.
   - *Consumer Termination / Timeout:* Monotonic expiration at `expiresAtMonotonicMs` ensures crashed consumers do not leak memory forever.
   - *Duplicate Release:* Authorized consumer releasing an already released lease receives `{ status: 'ALREADY_RELEASED' }` idempotently.
   - *Unauthorized Cross-Consumer Release:* Fails closed with `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`.
   - *Coordinator Attempt Advancement:* Existing historical leases remain bound to the generation and original publication token.
   - *Settlement Completion While Leases Active:* Barrier resolution does not evict generation $g$ from `publicationResults` while active retry leases exist.
   - *Retry Lease Release While Settlement Pin Active:* Generation remains protected until all leases and pins are released.
5. **Eviction Protection Rule:** Generation $g$ is eviction-eligible if and only if:
   $$\forall p \in \mathbf{activeSettlementPins},\; p.\text{generation} \ne g \;\;\land\;\; \forall l \in \mathbf{activeRetryLeases},\; l.\text{generation} \ne g \;\;\land\;\; g \notin \mathbf{pendingSettlementObligations}$$
6. **Watchdog Immunity:** Watchdog timeouts log telemetry but **CANNOT** release retry leases or settlement pins.

#### Option A Audit-History Model & Bounded Tombstone Retirement (B2):

To guarantee unconditional key contiguity and bounded memory:
1. **Option A Adoption:** `generationAuditHistory` holds lifecycle-discriminated records:
   - On first publication: `ImmutablePendingAuditRecord` (`PENDING_PUBLICATION`) is inserted for $g$.
   - On barrier settlement: `AuthoritativeSettlementEvidence` (`TERMINAL_SETTLEMENT`) replaces the pending record.
   - On result eviction: `ImmutableCompactAuditTombstone` (`COMPACT_TOMBSTONE`) replaces the evidence.
   - On tombstone retirement: the oldest entry is deleted from `generationAuditHistory`.
2. **Dual Unconditional Key Contiguity Invariants:**
   - **Publication Results:**
     $$\mathbf{publicationResults.keys()} \equiv \{\mathbf{custodyGenerationFloor} + 1,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
   - **Generation Audit History:**
     $$\mathbf{generationAuditHistory.keys()} \equiv \{\mathbf{auditTombstoneFloor} + 1,\;\; \dots,\;\; \mathbf{publishedGeneration}\}$$
   subject to empty-cache states ($\mathbf{floor} === \mathbf{publishedGeneration} \iff \mathbf{size} === 0$).
3. **Complete Tombstone Retirement Protection:**
   The oldest tombstone $g_{\text{tomb\_oldest}} = \mathbf{auditTombstoneFloor} + 1$ can be retired if and only if:
   - It is of kind `COMPACT_TOMBSTONE`.
   - No active retry lease references $g_{\text{tomb\_oldest}}$: `!activeRetryLeases.some(l => l.generation === g && l.expiresAtMonotonicMs > now)`.
   - No active settlement pin references $g_{\text{tomb\_oldest}}$: `!activeSettlementPins.some(p => p.generation === g)`.
   - No pending settlement obligation references $g_{\text{tomb\_oldest}}$: `!pendingSettlementObligations.has(g)`.
   If protected, throws `TOMBSTONE_CAPACITY_EXHAUSTED`.
4. **Strict Capacity Enforcement (`MAX_AUDIT_TOMBSTONES = 5000`):**
   - Pre-existing tombstone counts $> 5000$ fail closed with `TOMBSTONE_CAPACITY_CORRUPTED`.
   - Normal operation retires exactly the single oldest eligible tombstone per eviction, advancing `auditTombstoneFloor` atomically.
5. **Safe Integer Floor Invariants:**
   $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
   All floors and generations must be non-negative safe integers.

#### Complete Retained-Record Identity Consistency (C1):

For every entry in `publicationResults` and `generationAuditHistory`, all fields (generation, original owner token, transaction ID, operation ID, barrier ID, drainer ownership generation) are strictly verified. Historical records remain permanently bound to their original owner token across coordinator attempt advancements.

#### Settlement Evidence Factory Authorization & Replay Protection (C2):

- **Sole Authorized Issuer:** `SettlementEvidenceFactory` within `@syncplay/protocol` using private Symbol `SETTLEMENT_EVIDENCE_BRAND` and internal `AUTHORIZED_EVIDENCE_REGISTRY`.
- **Verified Facts:** Promise settled callback executed (watchdog timeout CANNOT manufacture evidence), terminal outcome classified (`CONFIRMED_COMMITTED` or `ABORTED_FENCED`), archive disposition verified, custody reconciliation verified, drainer relinquished custody.
- **Single-Use Replay Protection:** Evidence consumption is tracked in `CONSUMED_EVIDENCE_REGISTRY`. Replay across generations or obligations throws `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

#### Cross-Resource Identifier Semantics:
- `releaseRetryLease(pinId)` -> throws `LEASE_TYPE_MISMATCH`.
- `releaseSettlementPin(leaseId)` -> throws `PIN_TYPE_MISMATCH`.
- Unknown or never-issued ID -> throws `RESOURCE_NOT_FOUND`.
- Duplicate release by authorized caller -> returns `{ status: 'ALREADY_RELEASED' }` idempotently.

---

#### Complete Corrected Architectural Pseudocode:

```typescript
const DRAINER_RELEASE_BRAND = Symbol('@syncplay/DrainerCustodyReleaseReceipt');
const SETTLEMENT_EVIDENCE_BRAND = Symbol('@syncplay/SettlementEvidence');
const AUTHORIZED_EVIDENCE_REGISTRY = new WeakSet<AuthoritativeSettlementEvidence>();
const CONSUMED_EVIDENCE_REGISTRY = new Set<string>();

export function validateRetryRequestContext(ctx: unknown): asserts ctx is RetryRequestContext {
  if (!ctx || typeof ctx !== 'object') {
    throw new Error('INVALID_RETRY_REQUEST_CONTEXT: retryContext is mandatory on retry');
  }
  const { consumerId, clientRequestId } = ctx as Partial<RetryRequestContext>;
  if (typeof consumerId !== 'string' || typeof clientRequestId !== 'string') {
    throw new Error('MALFORMED_RETRY_REQUEST_CONTEXT: consumerId and clientRequestId must be strings');
  }
  if (consumerId.trim() === '' || clientRequestId.trim() === '') {
    throw new Error('MALFORMED_RETRY_REQUEST_CONTEXT: consumerId and clientRequestId cannot be empty');
  }
  if (consumerId.length > 256 || clientRequestId.length > 256) {
    throw new Error('RETRY_REQUEST_CONTEXT_OVERSIZED: consumerId and clientRequestId must be <= 256 chars');
  }
}

export class SettlementEvidenceFactory {
  public static createEvidence(params: {
    generation: number;
    expectedOwner: CustodyOwnershipToken;
    transactionId: string;
    operationIdentity: string;
    barrierIdentity: string;
    drainerOwnershipGeneration: number;
    terminalOutcome: 'CONFIRMED_COMMITTED' | 'ABORTED_FENCED';
    promiseSettled: boolean;
    isWatchdogTimeout: boolean;
    archiveDispositionConfirmed: boolean;
    custodyReconciledConfirmed: boolean;
    drainerReleasedConfirmed: boolean;
    releaseReceipt: DrainerCustodyReleaseReceipt;
    reconciliationReceipt: CustodyReconciliationReceipt;
    archiveReceipt: ArchiveDispositionReceipt;
    executionReceipt: ExecutionSettlementReceipt;
  }): AuthoritativeSettlementEvidence {
    if (params.isWatchdogTimeout || !params.promiseSettled) {
      throw new Error('CANNOT_ISSUE_SETTLEMENT_EVIDENCE_ON_TIMEOUT_OR_UNSETTLED_PROMISE');
    }
    if (!params.archiveDispositionConfirmed || !params.custodyReconciledConfirmed || !params.drainerReleasedConfirmed) {
      throw new Error('CANNOT_ISSUE_SETTLEMENT_EVIDENCE_UNVERIFIED_COORDINATOR_FACTS');
    }
    if (params.terminalOutcome !== 'CONFIRMED_COMMITTED' && params.terminalOutcome !== 'ABORTED_FENCED') {
      throw new Error('INVALID_TERMINAL_OUTCOME_CLASSIFICATION');
    }

    const evidence: AuthoritativeSettlementEvidence = Object.freeze({
      kind: 'TERMINAL_SETTLEMENT',
      [SETTLEMENT_EVIDENCE_BRAND]: true,
      generation: params.generation,
      expectedOwner: cloneAndFreezeOwnerToken(params.expectedOwner),
      transactionId: params.transactionId,
      operationIdentity: params.operationIdentity,
      barrierIdentity: params.barrierIdentity,
      drainerOwnershipGeneration: params.drainerOwnershipGeneration,
      terminalOutcome: params.terminalOutcome,
      settledAtTimestamp: Date.now(),
      monotonicSettledMs: performance.now(),
      releaseReceipt: params.releaseReceipt,
      reconciliationReceipt: params.reconciliationReceipt,
      archiveReceipt: params.archiveReceipt,
      executionReceipt: params.executionReceipt,
      evidenceDigest: computeEvidenceDigest(params),
    });

    AUTHORIZED_EVIDENCE_REGISTRY.add(evidence);
    return evidence;
  }

  public static assertAuthoritativeEvidenceProvenance(evidence: unknown): asserts evidence is AuthoritativeSettlementEvidence {
    if (!evidence || typeof evidence !== 'object') {
      throw new Error('INVALID_SETTLEMENT_EVIDENCE_PROVENANCE: evidence must be an object');
    }
    const ev = evidence as AuthoritativeSettlementEvidence;
    if (!ev[SETTLEMENT_EVIDENCE_BRAND] || !AUTHORIZED_EVIDENCE_REGISTRY.has(ev)) {
      throw new Error('INVALID_SETTLEMENT_EVIDENCE_PROVENANCE: unauthorized issuer or mock evidence rejected');
    }
  }

  public static consumeEvidence(evidence: AuthoritativeSettlementEvidence, pinId: string): void {
    const replayKey = `${evidence.generation}:${evidence.barrierIdentity}:${pinId}`;
    if (CONSUMED_EVIDENCE_REGISTRY.has(replayKey)) {
      throw new Error(`SETTLEMENT_EVIDENCE_REPLAY_DETECTED: evidence for key ${replayKey} has already been consumed`);
    }
    CONSUMED_EVIDENCE_REGISTRY.add(replayKey);
  }
}

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
  auditMap: Map<number, GenerationAuditRecord> | ImmutableMap<number, GenerationAuditRecord>,
  auditFloor: number,
  publishedGeneration: number,
  custodyFloor: number,
  publicationResults?: Map<number, PublicationResult> | ImmutableMap<number, PublicationResult>,
): void {
  // Validate non-negative safe integers and monotonic ordering
  if (!Number.isSafeInteger(auditFloor) || auditFloor < 0) {
    throw new Error('STATE_INCONSISTENT_INVALID_AUDIT_FLOOR');
  }
  if (!Number.isSafeInteger(custodyFloor) || custodyFloor < 0) {
    throw new Error('STATE_INCONSISTENT_INVALID_CUSTODY_FLOOR');
  }
  if (!Number.isSafeInteger(publishedGeneration) || publishedGeneration < 0) {
    throw new Error('STATE_INCONSISTENT_INVALID_PUBLISHED_GENERATION');
  }
  if (auditFloor > custodyFloor || custodyFloor > publishedGeneration) {
    throw new Error('STATE_INCONSISTENT_FLOOR_ORDER_VIOLATION');
  }

  if (auditFloor === publishedGeneration) {
    if (auditMap.size !== 0) throw new Error('STATE_INCONSISTENT_EMPTY_AUDIT_NON_EMPTY_MAP');
    return;
  }

  const expectedCount = publishedGeneration - auditFloor;
  if (auditMap.size !== expectedCount) {
    throw new Error(`STATE_INCONSISTENT_AUDIT_MAP_NON_CONTIGUOUS: expected ${expectedCount}, got ${auditMap.size}`);
  }

  for (let g = auditFloor + 1; g <= publishedGeneration; g++) {
    if (!auditMap.has(g)) {
      throw new Error(`STATE_INCONSISTENT_AUDIT_KEY_GAP: missing generation ${g}`);
    }
    const record = auditMap.get(g)!;

    // Complete Audit Record Identity Consistency (C1)
    if (record.generation !== g) {
      throw new Error(`AUDIT_RECORD_GENERATION_MISMATCH: key ${g} !== record.generation ${record.generation}`);
    }

    if (g <= custodyFloor) {
      // Must be a compact tombstone in the tombstone horizon
      if (record.kind !== 'COMPACT_TOMBSTONE') {
        throw new Error(`STATE_INCONSISTENT_TOMBSTONE_SCHEMA_VIOLATION: key ${g} in tombstone horizon must be COMPACT_TOMBSTONE, got ${record.kind}`);
      }
      if (!record.evidenceDigest || !(record.evidenceDigest instanceof Uint8Array) || record.evidenceDigest.length !== 32) {
        throw new Error(`STATE_INCONSISTENT_TOMBSTONE_DIGEST_CORRUPTED: key ${g}`);
      }
    } else {
      // Above custodyFloor: must be PENDING_PUBLICATION or TERMINAL_SETTLEMENT
      if (record.kind !== 'PENDING_PUBLICATION' && record.kind !== 'TERMINAL_SETTLEMENT') {
        throw new Error(`STATE_INCONSISTENT_AUDIT_SCHEMA_VIOLATION: key ${g} must be PENDING_PUBLICATION or TERMINAL_SETTLEMENT, got ${record.kind}`);
      }
      if (record.kind === 'TERMINAL_SETTLEMENT') {
        if (record.terminalOutcome !== 'CONFIRMED_COMMITTED' && record.terminalOutcome !== 'ABORTED_FENCED') {
          throw new Error(`STATE_INCONSISTENT_TERMINAL_OUTCOME_CORRUPTED: key ${g}`);
        }
      }
    }

    // Cross-check against publicationResults if provided
    if (publicationResults && publicationResults.has(g)) {
      const pub = publicationResults.get(g)!;
      if (
        !isEquivalentOwnerToken(record.expectedOwner, pub.obligation.ownerToken) ||
        record.transactionId !== pub.obligation.transactionId ||
        record.operationIdentity !== pub.obligation.operationIdentity ||
        record.barrierIdentity !== pub.obligation.barrierIdentity ||
        record.drainerOwnershipGeneration !== pub.obligation.drainerOwnershipGeneration
      ) {
        throw new Error(`STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH: key ${g}`);
      }
    }
  }

  for (const k of auditMap.keys()) {
    if (!Number.isSafeInteger(k) || k <= auditFloor || k > publishedGeneration) {
      throw new Error(`STATE_INCONSISTENT_AUDIT_GENERATION_OUT_OF_BOUNDS: key ${k}`);
    }
  }
}

public publishReleaseAndObligation(
  preparedEvidence: PreparedEvidenceContext,
  expectedOwner: CustodyOwnershipToken,
  barrierIdentity: string,
  retryContext?: RetryRequestContext,
): PublicationResponse {
  const currentRoot = this.#custodyRoot;

  const g = expectedOwner.custodyGeneration;
  assertValidGenerationDomain(g);

  const safeOwner = cloneAndFreezeOwnerToken(expectedOwner);
  const safeBarrierId = String(barrierIdentity);
  const safeTxId = String(safeOwner.transactionId);
  const safeOpId = String(safeOwner.operationIdentity);

  // 3. PRIOR PUBLICATION EVALUATION (RETRY PATH):
  if (g <= currentRoot.publishedGeneration) {
    // Mandatory stable request context on retry (B1.1)
    validateRetryRequestContext(retryContext);

    // Case 3A: Published with Settlement Pending
    if (currentRoot.pendingSettlementObligations.has(g)) {
      const existingObligation = currentRoot.pendingSettlementObligations.get(g)!;

      const cachedResult = currentRoot.publicationResults.get(g);
      if (!cachedResult) throw new Error('STATE_INCONSISTENT_ORPHANED_PENDING_OBLIGATION');

      if (currentRoot.generationAuditHistory.has(g)) {
        const audit = currentRoot.generationAuditHistory.get(g)!;
        if (audit.kind === 'TERMINAL_SETTLEMENT' || audit.kind === 'COMPACT_TOMBSTONE') {
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

      // ATOMIC RETRY LEASE ACQUISITION WITH STABLE CONTEXT (B1):
      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, retryContext.consumerId, retryContext.clientRequestId, safeOwner, safeTxId, safeOpId, safeBarrierId
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

      const auditRecord = currentRoot.generationAuditHistory.get(g);
      if (!auditRecord) throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');

      if (
        !isEquivalentOwnerToken(auditRecord.expectedOwner, cachedResult.releaseReceipt.identity) ||
        auditRecord.generation !== g ||
        auditRecord.transactionId !== cachedResult.releaseReceipt.identity.transactionId ||
        auditRecord.operationIdentity !== cachedResult.releaseReceipt.identity.operationIdentity ||
        auditRecord.barrierIdentity !== cachedResult.obligation.barrierIdentity
      ) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH');
      }

      if (auditRecord.kind !== 'TERMINAL_SETTLEMENT' && auditRecord.kind !== 'COMPACT_TOMBSTONE') {
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

      // ATOMIC RETRY LEASE ACQUISITION WITH STABLE CONTEXT (B1):
      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, retryContext.consumerId, retryContext.clientRequestId, safeOwner, safeTxId, safeOpId, safeBarrierId
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
        const tombstone = currentRoot.generationAuditHistory.get(g);
        if (!tombstone || tombstone.kind !== 'COMPACT_TOMBSTONE') {
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
  this.assertAuditHistoryContiguity(currentRoot.generationAuditHistory, currentRoot.auditTombstoneFloor, currentRoot.publishedGeneration, currentRoot.custodyGenerationFloor, currentRoot.publicationResults);

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

  // Option A: Immutable Pending Audit Record published atomically on first publication
  const pendingAuditRecord: ImmutablePendingAuditRecord = Object.freeze({
    kind: 'PENDING_PUBLICATION',
    generation: g,
    expectedOwner: safeOwner,
    transactionId: safeTxId,
    operationIdentity: safeOpId,
    barrierIdentity: safeBarrierId,
    drainerOwnershipGeneration: safeOwner.drainerOwnershipGeneration,
    publishedAtTimestamp: Date.now(),
    monotonicPublishedMs: monotonicNow,
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
    monotonicAcquiredMs: monotonicNow,
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
      if (lease.generation === g_oldest && lease.expiresAtMonotonicMs > monotonicNow) {
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
      }
    }

    // Atomic Tombstone Staging
    const existingAudit = candidateAuditHistory.get(g_oldest);
    if (!existingAudit || existingAudit.kind !== 'TERMINAL_SETTLEMENT') {
      throw new Error('STATE_INCONSISTENT_SETTLED_RESULT_MISSING_AUDIT');
    }

    const tombstone: ImmutableCompactAuditTombstone = Object.freeze({
      kind: 'COMPACT_TOMBSTONE',
      generation: g_oldest,
      expectedOwner: existingAudit.expectedOwner,
      transactionId: existingAudit.transactionId,
      operationIdentity: existingAudit.operationIdentity,
      barrierIdentity: existingAudit.barrierIdentity,
      drainerOwnershipGeneration: existingAudit.drainerOwnershipGeneration,
      terminalOutcome: existingAudit.terminalOutcome,
      settledAtTimestamp: existingAudit.settledAtTimestamp,
      monotonicSettledMs: existingAudit.monotonicSettledMs,
      evidenceDigest: computeEvidenceDigest(existingAudit),
    });

    candidateResultsMap.delete(g_oldest);
    candidateAuditHistory.set(g_oldest, tombstone);
    candidateFloor = g_oldest;

    // Strict Capacity Enforcement (MAX_AUDIT_TOMBSTONES = 5000) & Complete Protection (B2):
    const tombstoneCount = candidateFloor - candidateAuditFloor;
    if (tombstoneCount > 5000) {
      throw new Error('TOMBSTONE_CAPACITY_CORRUPTED: pre-existing tombstone count exceeds 5000');
    }
    if (tombstoneCount === 5000) {
      const g_tomb_oldest = candidateAuditFloor + 1;
      const tombEntry = candidateAuditHistory.get(g_tomb_oldest);
      if (!tombEntry || tombEntry.kind !== 'COMPACT_TOMBSTONE') {
        throw new Error('STATE_INCONSISTENT_TOMBSTONE_RETIREMENT_TYPE_VIOLATION');
      }

      // Check active retry leases
      for (const lease of currentRoot.activeRetryLeases.values()) {
        if (lease.generation === g_tomb_oldest && lease.expiresAtMonotonicMs > monotonicNow) {
          throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by active retry lease');
        }
      }
      // Check active settlement pins
      for (const pin of currentRoot.activeSettlementPins.values()) {
        if (pin.generation === g_tomb_oldest) {
          throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by active settlement pin');
        }
      }
      // Check pending settlement obligations
      if (currentRoot.pendingSettlementObligations.has(g_tomb_oldest)) {
        throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by pending settlement obligation');
      }

      candidateAuditHistory.delete(g_tomb_oldest);
      candidateAuditFloor = g_tomb_oldest;
    }
  }

  // Insert publication result and pending audit record
  candidateResultsMap.set(g, publicationResult);
  candidateAuditHistory.set(g, pendingAuditRecord); // Option A: unconditional contiguity!

  // Post-insertion unconditional contiguity assertions
  this.assertRetentionMapContiguity(candidateResultsMap, candidateFloor, g);
  this.assertAuditHistoryContiguity(candidateAuditHistory, candidateAuditFloor, g, candidateFloor, candidateResultsMap);

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
  operationIdentity: string,
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
        existingLease.operationIdentity === operationIdentity &&
        existingLease.barrierIdentity === barrierIdentity
      ) {
        return { candidateLeases: root.activeRetryLeases, leaseId: existingLease.leaseId };
      }
      throw new Error('IDEMPOTENT_RETRY_IDENTITY_CONFLICT');
    }
  }

  const seq = ++this._pinSequenceCounter;
  const leaseId = `lease:${this._coordinatorInstanceId}:${generation}:${consumerId}:${seq}`;

  if (root.activeRetryLeases.has(leaseId)) {
    throw new Error('RETRY_LEASE_ID_COLLISION');
  }

  const monotonicNow = performance.now();
  const lease: ImmutableRetryLease = Object.freeze({
    leaseId,
    generation,
    consumerId,
    clientRequestId,
    ownerToken,
    transactionId,
    operationIdentity,
    barrierIdentity,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: monotonicNow,
    expiresAtMonotonicMs: monotonicNow + 30000, // 30-second bounded lease duration
  });

  const candidateLeases = new ImmutableMap([
    ...root.activeRetryLeases.entries(),
    [leaseId, lease],
  ]);

  return { candidateLeases, leaseId };
}

public releaseRetryLease(params: ReleaseRetryLeaseParams): { leaseId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
  const { leaseId, consumerId, clientRequestId, callerToken, transactionId, operationIdentity, barrierIdentity } = params;

  // Cross-Resource Type Checking
  if (leaseId.startsWith('pin:')) {
    throw new Error('LEASE_TYPE_MISMATCH: identifier is a settlement pin, not a retry lease');
  }
  if (!leaseId.startsWith('lease:')) {
    throw new Error('RESOURCE_NOT_FOUND: unknown lease identifier format');
  }

  const currentRoot = this.#custodyRoot;
  const lease = currentRoot.activeRetryLeases.get(leaseId);
  if (!lease) {
    // Idempotent return for already-released lease
    return { leaseId, status: 'ALREADY_RELEASED' };
  }

  // Independent Consumer Authorization Check (B1.2)
  if (lease.consumerId !== consumerId) {
    throw new Error(`UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE: caller consumer ${consumerId} does not own lease ${leaseId} owned by ${lease.consumerId}`);
  }
  if (lease.clientRequestId !== clientRequestId) {
    throw new Error(`LEASE_CLIENT_REQUEST_MISMATCH: clientRequestId ${clientRequestId} does not match lease ${lease.clientRequestId}`);
  }
  if (!isEquivalentOwnerToken(lease.ownerToken, callerToken)) {
    throw new Error('UNAUTHORIZED_LEASE_RELEASE_TOKEN_MISMATCH: caller token does not match lease owner token');
  }
  if (lease.transactionId !== transactionId) {
    throw new Error(`LEASE_TRANSACTION_ID_MISMATCH: transactionId ${transactionId} does not match lease`);
  }
  if (lease.operationIdentity !== operationIdentity) {
    throw new Error(`LEASE_OPERATION_IDENTITY_MISMATCH: operationIdentity ${operationIdentity} does not match lease`);
  }
  if (lease.barrierIdentity !== barrierIdentity) {
    throw new Error(`LEASE_BARRIER_IDENTITY_MISMATCH: barrierIdentity ${barrierIdentity} does not match lease`);
  }

  const candidateLeasesMap = new Map(currentRoot.activeRetryLeases.entries());
  candidateLeasesMap.delete(leaseId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeRetryLeases: new ImmutableMap(candidateLeasesMap.entries()),
  });

  return { leaseId, status: 'RELEASED' };
}

public releaseSettlementPin(pinId: string, settlementEvidence: AuthoritativeSettlementEvidence): { pinId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
  // Cross-Resource Type Checking
  if (pinId.startsWith('lease:')) {
    throw new Error('PIN_TYPE_MISMATCH: identifier is a retry lease, not a settlement pin');
  }
  if (!pinId.startsWith('pin:')) {
    throw new Error('RESOURCE_NOT_FOUND: unknown pin identifier format');
  }

  const currentRoot = this.#custodyRoot;
  const pin = currentRoot.activeSettlementPins.get(pinId);
  if (!pin) {
    return { pinId, status: 'ALREADY_RELEASED' };
  }

  // Settlement Evidence Factory Provenance and Capability Validation (C2)
  SettlementEvidenceFactory.assertAuthoritativeEvidenceProvenance(settlementEvidence);

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

  // Replay Protection: Mark evidence consumed in internal registry
  SettlementEvidenceFactory.consumeEvidence(settlementEvidence, pinId);

  const candidatePinsMap = new Map(currentRoot.activeSettlementPins.entries());
  candidatePinsMap.delete(pinId);

  this.#custodyRoot = Object.freeze({
    ...currentRoot,
    activeSettlementPins: new ImmutableMap(candidatePinsMap.entries()),
  });

  return { pinId, status: 'RELEASED' };
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
| **Phase 8: Tombstone Retir.**| Tombstone pinned by lease/pin/obligation | Backpressure throw | Root unchanged; capacity preserved |
| **Phase 9: Post-Contiguity**| Key gap in candidate map | Fail-closed throw | Candidate root discarded |
| **Phase 10: Atomic Swap** | Pointer replacement | Infallible | Single linearization point |
| **Phase 11: Return Ref** | Return cached pointer | Infallible | Zero post-publication allocation |

---

## 12.15 Protocol A: Drainer $\to$ Terminal Archive (Commit Reconciliation & Execution Settlement)

Protocol A linearizes commitment of the operation and settles execution:
1. `commitRetirementBatch` synchronously stages and commits the transaction in the archive.
2. Custody gate transitions to `CUSTODY_RELEASED_BARRIER_PENDING`.
3. Pre-allocated `DrainerCustodyReleaseReceipt`, `PendingSettlementObligation`, and `ImmutablePendingAuditRecord` are published atomically into the root.
4. Barrier resolves with `AuthoritativeSettlementEvidence` containing `CONFIRMED_COMMITTED`, updating `generationAuditHistory`.
5. `releaseSettlementPin` is authorized using the verified `AuthoritativeSettlementEvidence`.

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

Protocol B handles cancelled or uncommitted operations:
1. Verify Level 3 proof of non-commitment (`TRANSACTION_ABORTED_FENCED`).
2. Atomically fence transaction ID in archive.
3. Transition custody gate to `RECONCILING_CUSTODY`.
4. If destination is healthy, transfer operation; otherwise restore to source buffer.
5. Publish release, pending obligation, and pending audit record.
6. Resolve barrier with `ABORTED_FENCED` evidence, updating `generationAuditHistory`.
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
- **Case F (Tombstone Capacity Saturation):** Oldest tombstone pinned by lease or pin; backpressure applied; zero generation floor gaps.
- **Case G (Duplicate Retry After Eviction):** Floor advanced past generation; returns `CUSTODY_RECEIPT_EXPIRED` with compact audit tombstone verification; zero republication.
- **Case H (Delayed Settled Retry After Attempt Change):** Settled retry arrives with Attempt 1 token while Attempt 2 is active or attempt is null; identity matched against cached receipt and audit record; acquires independent retry lease; returns cached receipt immediately without error.

---

## 15. Exhaustive Failure and Retry Matrix (28 Deterministic Cases)

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
| **25** | Retry request without stable context | `ANY` | Any | ANY | Throw `INVALID_RETRY_REQUEST_CONTEXT` | Generate random/timestamp ID |
| **26** | Consumer 1 releases Consumer 2's lease | `ANY` | Any | ANY | Throw `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE` | Delete Consumer 2's lease |
| **27** | Settlement pin passed to lease release | `ANY` | Any | ANY | Throw `LEASE_TYPE_MISMATCH` | Delete pin as lease |
| **28** | Retry lease passed to pin release | `ANY` | Any | ANY | Throw `PIN_TYPE_MISMATCH` | Delete lease as pin |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{EVIDENCE\_MATCHED}(g)$$
2. **Capability-Gated Settlement-Pin Release Invariant:**
   $$\mathbf{RELEASE\_SETTLEMENT\_PIN}(pinId) \implies \mathbf{AssertAuthoritativeSettlementEvidence}(evidence) \land (\mathbf{pin.identity} \equiv \mathbf{evidence.identity})$$
3. **Mandatory Stable Retry Request Identity Invariant:**
   All retry requests must supply non-empty, string-typed `(consumerId, clientRequestId)` bounded to $\le 256$ characters. The coordinator cannot substitute timestamps or nonces.
4. **Independent Consumer Release Authority Invariant:**
   A retry lease is releaseable only by the owning consumer matching all seven identity dimensions (`leaseId`, `consumerId`, `clientRequestId`, `callerToken`, `transactionId`, `operationIdentity`, `barrierIdentity`). Cross-consumer release fails closed.
5. **Option A Audit-History Invariant:**
   `generationAuditHistory` maintains lifecycle-discriminated records (`PENDING_PUBLICATION`, `TERMINAL_SETTLEMENT`, `COMPACT_TOMBSTONE`) such that $\mathbf{generationAuditHistory.keys()} \equiv \{\mathbf{auditTombstoneFloor} + 1, \dots, \mathbf{publishedGeneration}\}$ holds unconditionally.
6. **Complete Tombstone Retirement Protection Invariant:**
   Oldest tombstone cannot be retired while any active retry lease, settlement pin, or pending obligation references it.
7. **Strict Capacity Ceiling Invariant:**
   `MAX_AUDIT_TOMBSTONES = 5000` is an authoritative ceiling; pre-existing violations fail closed; retirement operates strictly FIFO.
8. **Monotonic Safe-Integer Floor Invariant:**
   $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
9. **Complete Retained-Record Identity Consistency Invariant:**
   Every retained entry in `publicationResults` and `generationAuditHistory` must match 1-to-1 across generation, 5-field token, transaction ID, operation ID, and drainer ownership generation.
10. **Settled Retry Attempt Independence Invariant:**
   A settled retry validates exact 1-to-1 token identity against cached evidence and authoritative audit history, completely independent of `_currentAttemptId`.
11. **Evidence Factory Provenance & Replay Invariant:**
   Level 3 settlement evidence is issued exclusively by `SettlementEvidenceFactory` within `@syncplay/protocol` and is single-use per barrier and obligation.
12. **Cross-Resource Identifier Distinction Invariant:**
   `releaseRetryLease` rejects settlement pins with `LEASE_TYPE_MISMATCH`. `releaseSettlementPin` rejects retry leases with `PIN_TYPE_MISMATCH`.
13. **Watchdog Pin and Lease Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins or retry leases whose underlying operations remain unresolved.
14. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
15. **Irreversibility Precedence:** Post-commit execution failure **CANNOT** reverse an irrevocable commit or trigger Protocol B restoration.
16. **Runtime Immutability Invariant:** Published collections and evidence references **CANNOT** be mutated through `Map.prototype` or mutable array/buffer aliases.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)} \longrightarrow \text{6. Release Settlement Pin}$$

---

## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R20 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution, Capability-Gated Pin Release & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. The settlement barrier resolves with verified `AuthoritativeSettlementEvidence`. This evidence authorizes `releaseSettlementPin`, freeing generation $g$ for eventual FIFO eviction. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following thirty deterministic future test specifications are formally codified for Sub-Milestone M5.1:

### Test R18-R20-T01: Retry Acquisition Without Stable Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext === undefined`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `INVALID_RETRY_REQUEST_CONTEXT`.

### Test R18-R20-T02: Retry Acquisition With Malformed or Empty Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext = { consumerId: "   ", clientRequestId: "" }`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `MALFORMED_RETRY_REQUEST_CONTEXT`.

### Test R18-R20-T03: Retry Acquisition With Oversized Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` with `consumerId` of length 257 characters.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `RETRY_REQUEST_CONTEXT_OVERSIZED`.

### Test R18-R20-T04: Lost-Response Retransmission Recovers Original Lease Idempotently
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 acquires lease for $g=1$ with `clientRequestId = "req-101"`; receives `leaseId_A`.
  2. Consumer 1 retransmits identical request with matching `(consumerId, clientRequestId)`.
- **Expected Final Root:** `activeRetryLeases.size === 1`, containing `leaseId_A`.
- **Expected Result / Error:** Second call returns existing `leaseId_A` idempotently.

### Test R18-R20-T05: Repeated Retry Acquisition With Conflicting Operation Identity
- **Initial Authoritative Root:** Settled result at $g=1$; lease active for `(consumer1, req-101)`.
- **Operation Sequence:** Consumer 1 submits request with `clientRequestId = "req-101"` but altered `operationIdentity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.

### Test R18-R20-T06: Two Distinct Consumers Acquire Independent Leases Under Same Recovery Owner Token
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 requests lease for $g=1$ with `consumerId = "c1"`.
  2. Consumer 2 requests lease for $g=1$ with `consumerId = "c2"`.
- **Expected Final Root:** `activeRetryLeases.size === 2`, containing `leaseId_1` and `leaseId_2`.
- **Expected Result / Error:** Both acquire distinct leases successfully.

### Test R18-R20-T07: Consumer 1 Attempting to Release Consumer 2's Lease Fails Closed
- **Initial Authoritative Root:** Active leases for Consumer 1 (`leaseId_1`) and Consumer 2 (`leaseId_2`).
- **Operation Sequence:** Consumer 1 invokes `releaseRetryLease` passing `leaseId_2` with `consumerId = "c1"`.
- **Expected Final Root:** Unchanged; both leases remain active.
- **Expected Result / Error:** Throws `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`.

### Test R18-R20-T08: Duplicate Lease Release by Authorized Consumer Is Idempotent
- **Initial Authoritative Root:** Active lease `leaseId_1` for Consumer 1.
- **Operation Sequence:**
  1. Consumer 1 calls `releaseRetryLease` with all matching parameters; returns `{ status: 'RELEASED' }`.
  2. Consumer 1 calls `releaseRetryLease` again with identical parameters.
- **Expected Final Root:** `activeRetryLeases.has(leaseId_1) === false`.
- **Expected Result / Error:** Second call returns `{ status: 'ALREADY_RELEASED' }` without error.

### Test R18-R20-T09: First Publication With Option A Lifecycle Model
- **Initial Authoritative Root:** `publishedGeneration = 0`, `auditTombstoneFloor = 0`, `custodyGenerationFloor = 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` for $g=1$.
- **Expected Final Root:**
  - `publishedGeneration === 1`.
  - `publicationResults.has(1) === true`.
  - `pendingSettlementObligations.has(1) === true`.
  - `generationAuditHistory.get(1).kind === 'PENDING_PUBLICATION'`.
- **Expected Result / Error:** Success; contiguity invariant holds unconditionally.

### Test R18-R20-T10: Pending-to-Terminal Audit History Transition
- **Initial Authoritative Root:** Generation $g=1$ published; audit record is `PENDING_PUBLICATION`.
- **Operation Sequence:** Barrier for $g=1$ settles with `AuthoritativeSettlementEvidence` (`CONFIRMED_COMMITTED`).
- **Expected Final Root:** `generationAuditHistory.get(1).kind === 'TERMINAL_SETTLEMENT'`.
- **Expected Result / Error:** Success; record upgraded in place.

### Test R18-R20-T11: Eviction and Compact Tombstone Conversion
- **Initial Authoritative Root:** Result at $g=1$ settled (`TERMINAL_SETTLEMENT`); 1,000 results retained.
- **Operation Sequence:** Publish $g=1001$; triggers FIFO eviction of $g=1$.
- **Expected Final Root:**
  - `publicationResults.has(1) === false`.
  - `custodyGenerationFloor === 1`.
  - `generationAuditHistory.get(1).kind === 'COMPACT_TOMBSTONE'`.
  - `generationAuditHistory.get(1).evidenceDigest` is 32-byte SHA-256.
- **Expected Result / Error:** Success.

### Test R18-R20-T12: Tombstone Retirement Blocked by Active Settlement Pin
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active settlement pin.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R20-T13: Tombstone Retirement Blocked by Active Retry Lease
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active retry lease.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R20-T14: Tombstone Retirement Blocked by Pending Settlement Obligation
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ references pending obligation.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R20-T15: Normal FIFO Tombstone Retirement at 5,000 Capacity
- **Initial Authoritative Root:** 5,000 tombstones retained ($[1 \dots 5000]$); zero pins/leases on $g=1$.
- **Operation Sequence:** Publish new generation causing eviction and tombstone retirement.
- **Expected Final Root:**
  - Tombstone $g=1$ deleted from `generationAuditHistory`.
  - `auditTombstoneFloor === 1`.
  - Total tombstones remaining: 5,000 ($[2 \dots 5001]$).
- **Expected Result / Error:** Success; contiguity preserved over $[2 \dots publishedGeneration]$.

### Test R18-R20-T16: Pre-Existing Tombstone Count Above Cap Fails Closed
- **Initial Authoritative Root:** Artificially injected root with 5,005 tombstones.
- **Operation Sequence:** Call `publishReleaseAndObligation`.
- **Expected Final Root:** Unchanged; zero bulk deletions performed.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_CORRUPTED`.

### Test R18-R20-T17: Missing Tombstone Inside Promised Horizon Fails Closed
- **Initial Authoritative Root:** `auditTombstoneFloor = 10`, `custodyGenerationFloor = 20`; tombstone $g=15$ deleted.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_KEY_GAP`.

### Test R18-R20-T18: Expiry Below Tombstone Floor Returns Historical Expiry
- **Initial Authoritative Root:** `auditTombstoneFloor = 50`.
- **Operation Sequence:** Query generation $g=30$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.

### Test R18-R20-T19: Floor Order Invariant Violation Fails Closed
- **Initial Authoritative Root:** Artificially set `auditTombstoneFloor = 50`, `custodyGenerationFloor = 40`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_FLOOR_ORDER_VIOLATION`.

### Test R18-R20-T20: Audit Record With Mismatched Generation Fails Closed
- **Initial Authoritative Root:** Audit history key is 5, but record payload has `generation: 4`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `AUDIT_RECORD_GENERATION_MISMATCH`.

### Test R18-R20-T21: Audit Record With Mismatched Owner Token Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `membershipEpoch`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R20-T22: Audit Record With Mismatched Drainer Ownership Generation Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `drainerOwnershipGeneration`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R20-T23: Cross-Generation Settlement Evidence Replay Rejected
- **Initial Authoritative Root:** Active pin for $g=2$; evidence constructed for $g=1$.
- **Operation Sequence:** Call `releaseSettlementPin("pin:...:g2:...", evidence_g1)`.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R20-T24: Cross-Obligation Settlement Evidence Replay Rejected
- **Initial Authoritative Root:** Evidence for $g=1$ already consumed on `pin_1`.
- **Operation Sequence:** Attempt to reuse identical evidence on `pin_2`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

### Test R18-R20-T25: Caller-Constructed Mock Evidence Rejected by Provenance Check
- **Initial Authoritative Root:** Active pin for $g=1$.
- **Operation Sequence:** Caller constructs object matching `AuthoritativeSettlementEvidence` schema without factory.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R20-T26: Watchdog Timeout Cannot Release Settlement Protection
- **Initial Authoritative Root:** Barrier pending for $g=1$; watchdog fires after 5,000 ms.
- **Operation Sequence:** Watchdog handler attempts to call `releaseSettlementPin` with watchdog receipt.
- **Expected Final Root:** Pin remains active; gate state latches `DEGRADED`.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R20-T27: Retry Lease ID Passed to Settlement Pin Release Fails Closed
- **Initial Authoritative Root:** Active lease `lease:1:1:c1:1`.
- **Operation Sequence:** Call `releaseSettlementPin("lease:1:1:c1:1", evidence)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `PIN_TYPE_MISMATCH`.

### Test R18-R20-T28: Settlement Pin ID Passed to Retry Lease Release Fails Closed
- **Initial Authoritative Root:** Active pin `pin:1:1:b1:1`.
- **Operation Sequence:** Call `releaseRetryLease({ leaseId: "pin:1:1:b1:1", ... })`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TYPE_MISMATCH`.

### Test R18-R20-T29: Unknown Identifier Passed to Release APIs
- **Initial Authoritative Root:** Clean root.
- **Operation Sequence:**
  1. Call `releaseRetryLease` with `leaseId = "lease:unknown"`.
  2. Call `releaseSettlementPin` with `pinId = "pin:unknown"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Both return `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R20-T30: Universal M5.0 Invariant Regression Suite
- **Initial Authoritative Root:** Initialized system ($g=0$).
- **Operation Sequence:** Execute sequential publication, concurrent retries, barrier settlements, FIFO result evictions, tombstone retirements, and attempt handovers across 10,000 operations.
- **Expected Final Root:** All contiguity, identity, floor, and single-owner invariants verified.
- **Expected Result / Error:** All assertions pass.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R20):
================================================================================
1. [MANDATORY STABLE RETRY REQUEST IDENTITY]: Does the Engineering Authority approve
   making RetryRequestContext (consumerId, clientRequestId) strictly mandatory for
   retry acquisitions, eliminating runtime timestamp fallbacks and guaranteeing
   idempotency on lost-response retransmissions?

2. [INDEPENDENT CONSUMER RELEASE AUTHORITY]: Does the Engineering Authority approve
   binding leaseId, consumerId, clientRequestId, callerToken, txId, opId, and
   barrierId into releaseRetryLease(), strictly preventing unauthorized cross-consumer
   lease releases?

3. [OPTION A AUDIT-HISTORY MODEL & CONTINUOUS CONTIGUITY]: Does the Engineering
   Authority approve Option A, publishing ImmutablePendingAuditRecord on first
   publication to guarantee generationAuditHistory key contiguity unconditionally
   across all lifecycle stages?

4. [COMPLETE TOMBSTONE RETIREMENT PROTECTION]: Does the Engineering Authority approve
   protecting tombstones from retirement whenever active retry leases, active
   settlement pins, or pending settlement obligations reference them, enforcing
   MAX_AUDIT_TOMBSTONES = 5000 with fail-closed backpressure?

5. [EVIDENCE FACTORY AUTHORIZATION & CROSS-RESOURCE ENFORCEMENT]: Does the
   Engineering Authority approve authorizing Level 3 evidence exclusively via
   internal SettlementEvidenceFactory, rejecting watchdog receipts, and enforcing
   strict LEASE_TYPE_MISMATCH and PIN_TYPE_MISMATCH cross-resource error semantics?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R20 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR INDEPENDENT ENGINEERING AUTHORITY REVIEW
================================================================================
```

This R18-R20 proposal completes the formal recovery arbitration architecture. It resolves retry lease idempotency and independent consumer release authority, formalizes the Option A audit-history lifecycle with continuous key contiguity, establishes complete tombstone retirement protection and safe integer floor invariants, enforces auditable settlement evidence provenance with single-use replay protection, specifies distinct cross-resource identifier semantics, and codifies thirty deterministic future test specifications.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R20 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
