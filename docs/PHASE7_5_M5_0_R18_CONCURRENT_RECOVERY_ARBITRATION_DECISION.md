# Phase 7.5 M5.0-R18-R24 — Durable Boot Fencing, Encapsulated Immutable Custody, Byte-Digest Equality & Strict Replay Invariant Verification
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `3b9651695ba33bdb104a2744e07a16771cb6cd72` (M5.0-R18-R23)
**Direct Parent Commit:** `3b9651695ba33bdb104a2744e07a16771cb6cd72`
**Authorization Scope:** M5.0-R18-R24 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R24 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R23, we established the fundamental principles of restart-safe resource identification and pre-existing capacity overflow fencing. However, the subsequent independent architectural review identified six critical implementation-contract and operational correctness gaps in the concrete pseudocode and specifications:

1. **Durable Boot Epoch Fencing & Concurrent Coordinator Protection (Reviewer Issue 1):**
   - *Previous Defect:* R18-R23 proposed `coordinatorBootId` using UUIDv4 logged in metadata. UUIDv4 is collision-resistant but only probabilistically unique, and metadata logging alone does not guarantee that two concurrently active (or split-brain) coordinators cannot allocate conflicting namespaces or collide with prior process generations.
   - *Architectural Correction in R24:* Formally define the coordinator boot epoch as a compound, durable fencing token:
     `coordinatorBootEpoch: { clusterTerm: number; bootGeneration: number; bootInstanceId: string }`
     Canonical formatted string representation:
     `coordinatorBootId = `${clusterTerm}.${bootGeneration}.${bootInstanceId}``
     - `bootGeneration`: A strictly monotonic, consensus-allocated durable sequence counter incremented on every node startup via distributed consensus or monotonic WAL epoch prior to accepting traffic.
     - `clusterTerm`: The active Raft/consensus term under which this coordinator instance was granted recovery leadership. Fences out split-brain or partitioned coordinators immediately via lease checks.
     - `bootInstanceId`: A cryptographically secure collision-resistant UUIDv4 identifying this specific process memory instance.
     - *Guarantee:* Monotonic non-reusability across restarts is mathematically guaranteed by consensus-allocated `bootGeneration`, split-brain leader safety is enforced by `clusterTerm`, and instance collision resistance is guaranteed by `bootInstanceId`. An identifier from a prior process lifetime can NEVER be reissued in subsequent lifetimes.

2. **Full Lifecycle Parity for Retry Leases (Reviewer Issue 2):**
   - *Previous Defect:* R18-R23 specified boot-epoch templates for retry leases but omitted explicit allocation and release pseudocode demonstrating symmetrical pre-existing overflow checks and exact-capacity retirement.
   - *Architectural Correction in R24:* Fully codify and display both `_getOrAllocateRetryLease()` and `releaseRetryLease()` pseudocode, establishing exact parity with settlement pins:
     - Lease template: `lease:${this._coordinatorBootId}:${generation}:${consumerId}:${seq}`
     - Symmetrical pre-existing overflow check: `if (currentRoot.retiredRetryLeases.size > 5000) throw new Error('RETIRED_LEASES_CAPACITY_CORRUPTED');`
     - Exact-capacity retirement at 5,000 entries with eligible tombstone eviction and full 7-dimensional identity verification.

3. **Genuinely Encapsulated Immutable Custody Root Collections (Reviewer Issue 3 & 4):**
   - *Previous Defect:* R18-R23 pseudocode utilized `Object.freeze(nextActive)` on native JavaScript `Map` and `Set` instances. In JavaScript, `Object.freeze()` on a `Map` or `Set` does NOT prevent mutations via `.set()`, `.delete()`, or `.clear()`, violating the immutable custody-root invariant.
   - *Architectural Correction in R24:* Eliminate all naive `Object.freeze(new Map())` patterns. Re-establish canonical, deeply encapsulated `ImmutableMap<K, V>` and `ImmutableSet<T>` class wrappers where internal state is completely private, mutating operations throw or return brand-new immutable instances, and native mutating methods are completely inaccessible.

4. **Byte-for-Byte Digest Comparison & Complete Structured Owner-Token Identity (Reviewer Issue 3 & 4):**
   - *Previous Defect:* R18-R23 pseudocode performed reference comparison `retiredPin.evidenceDigest !== settlementEvidence.evidenceDigest`. For byte containers (`Uint8Array`), reference equality fails even for identical digest contents. Additionally, owner token identity had been simplified to plain string comparisons in illustrative summaries.
   - *Architectural Correction in R24:*
     - Restore constant-time, byte-for-byte digest comparison: `areEqualByteArrays(a: Uint8Array, b: Uint8Array): boolean` with mandatory defensive byte copying (`cloneByteArray(src)`) on ingress and egress.
     - Restore complete structured owner token identity: `CustodyOwnershipToken` (`recoveryAttemptId`, `custodyGeneration`, `drainerOwnershipGeneration`, `transactionId`, `operationIdentity`, `barrierIdentity`, `leaseTerm`, `ownerSignature: Uint8Array`) compared via `isEquivalentOwnerToken()`.

5. **Deterministic Oldest Eligible Tombstone Eviction with Backpressure (Reviewer Issue 2A):**
   - *Previous Defect:* R18-R23 assumed the oldest entry in `candidateRetired` was unconditionally eligible for eviction. If the oldest entry was protected by an active retry lease, active settlement pin, or pending settlement obligation, silent eviction would violate active retention guarantees.
   - *Architectural Correction in R24:* Codify explicit eligibility search methods:
     `_findOldestEligibleRetiredPin(currentRoot, candidateRetired)` and `_findOldestEligibleRetiredLease(currentRoot, candidateRetired)`.
     If collection size is exactly 5,000 and all 5,000 entries are currently protected, the coordinator fails closed with `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE` (or `RETIRED_LEASES_CAPACITY_EXHAUSTED_BACKPRESSURE`), strictly refusing to evict protected entries.

6. **Strict Replay-Key Lockstep & Post-Publication Invariant Verification (Reviewer Issue 2B, 5, 6):**
   - *Previous Defect:* R18-R23 called `candidateConsumed.delete(oldestReplayKey)` without checking whether the key existed or corresponded to the evicted record. If missing, `delete` returned false, allowing the subsequent insertion to push `candidateConsumed.size` to 5,001 entries, silently violating the bound.
   - *Architectural Correction in R24:*
     - Before deleting from `candidateConsumed`, the coordinator validates that `candidateConsumed.has(oldestReplayKey)` is strictly true; if false, it throws `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED`.
     - Implement a mandatory pre-swap validation method: `_validatePostPublicationInvariants(candidateRoot)` that unconditionally verifies collection sizes $\le 5000$, verifies that every retired pin has its exact replay key in `consumedSettlementEvidence`, and confirms that collection sizes remain in exact lockstep (`retiredSettlementPins.size === consumedSettlementEvidence.size`).

### Core Architectural Guarantees Established in R18-R24:

- **Compound Durable Boot Epoch Fencing:** Binds consensus term, monotonic WAL generation, and instance UUID into all allocated pins and retry leases, guaranteeing zero cross-restart or split-brain collisions.
- **Symmetrical Retry Lease Lifecycle:** Explicit allocation, strict pre-existing overflow fencing, eligible tombstone eviction, and full 7-dimensional validation for retry leases.
- **Genuine Collection Encapsulation:** Enforces deep immutability via `ImmutableMap` and `ImmutableSet` across all custody root structures.
- **Constant-Time Byte-Digest & Structured Owner Identity:** Enforces bitwise byte comparison for evidence digests and full 8-field verification for owner tokens.
- **Eligible Tombstone Eviction with Backpressure:** Guarantees that protected resources are never evicted, failing closed with backpressure when retention capacity is saturated.
- **Post-Publication Cross-Collection Integrity:** Enforces exact lockstep eviction and post-publication invariant checks, making collection overflows mathematically impossible.
- **Exhaustive Deterministic Test Suite (44 Test Cases):** Codifies forty-four deterministic future test specifications in Section 19 covering boot fencing, encapsulated immutable collections, byte digest equality, and post-publication invariant validation.

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
│      generationAuditHistory, activeSettlementPins, activeRetryLeases,       │
│      retiredRetryLeases, retiredSettlementPins, and                         │
│      consumedSettlementEvidence.                                            │
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

### 8.2 Safe Tombstone Eviction & Compound Boot Epoch Fencing

To bound memory while preventing stale execution:

- Tombstones older than `fencingGenerationFloor` may be evicted if and only if no active reservations reference generations $\le \mathbf{fencingGenerationFloor}$.
- If eviction occurs, the generation floor advances monotonically:
  $$\mathbf{fencingGenerationFloor} = \max(\mathbf{fencingGenerationFloor},\; g_{\text{evicted}})$$

#### Compound Durable Coordinator Boot Epoch:
To guarantee that resource identifiers are mathematically non-reusable across process restarts and split-brain coordinator transitions:
- Every coordinator instance is bound to a compound durable fencing token:
  $$\mathbf{coordinatorBootEpoch} = \langle \mathbf{clusterTerm},\; \mathbf{bootGeneration},\; \mathbf{bootInstanceId} \rangle$$
  formatted canonically as:
  $$\mathbf{coordinatorBootId} = \text{"}\mathbf{clusterTerm}.\mathbf{bootGeneration}.\mathbf{bootInstanceId}\text{"}$$
- **Cluster Term ($\mathbf{clusterTerm}$):** Monotonically increasing consensus term (e.g. Raft term) under which recovery leadership was granted. Fences out stale or partitioned coordinators immediately upon leadership transfer.
- **Boot Generation ($\mathbf{bootGeneration}$):** Monotonically increasing sequence number persisted and fsynced to Write-Ahead Log (WAL) or consensus state before coordinator traffic admission. Guarantees mathematical non-reusability across process restarts on the same or migrating node.
- **Boot Instance ID ($\mathbf{bootInstanceId}$):** Cryptographically secure, collision-resistant UUIDv4 uniquely identifying the in-memory process execution lifetime.
- Every allocated settlement pin and retry lease binds $\mathbf{coordinatorBootId}$ as a prefix. Because $\mathbf{bootGeneration}$ never decreases and $\mathbf{clusterTerm}$ fences split-brain leaders, an evicted identifier from an earlier session can never be re-issued in subsequent sessions.

### 8.3 Empty-Cache and Generation-Floor Semantics

A robust distributed recovery engine must define exact invariants when `publicationResults` is empty:

1. **Initial State (Never Published):**
   - At system initialization: `publishedGeneration = 0`, `custodyGenerationFloor = 0`, `auditTombstoneFloor = 0`, `publicationResults.size = 0`, `pendingSettlementObligations.size = 0`, `generationAuditHistory.size = 0`, `activeSettlementPins.size = 0`, `activeRetryLeases.size = 0`, `retiredRetryLeases.size = 0`, `retiredSettlementPins.size = 0`, `consumedSettlementEvidence.size = 0`.
   - The first valid publication generation is strictly $g = 1$.
   - Any query or retry for generation $g \le 0$ throws `INVALID_GENERATION_DOMAIN`.
2. **Post-Eviction Empty State (Cache Completely Trimmed):**
   - If all retained results in `publicationResults` are evicted to `COMPACT_TOMBSTONE` records in `generationAuditHistory`:
     `publishedGeneration = N`, `custodyGenerationFloor = N`, `publicationResults.size = 0`.
   - The contiguity predicate holds vacuously for the empty set: $\text{keys}(publicationResults) \equiv \emptyset \equiv \{N + 1, \dots, N\}$.
   - In this state:
     - The next valid first-publication generation is strictly $g = N + 1$.
     - Any retry for generation $g \le N$:
       - If $g > \text{auditTombstoneFloor}$, it locates the tombstone in `generationAuditHistory` and throws `CUSTODY_RECEIPT_EXPIRED` (with validated token equality).
       - If $g \le \text{auditTombstoneFloor}$, the tombstone has been pruned; the coordinator throws `CUSTODY_RECEIPT_EXPIRED`.
     - In neither case can an empty cache be misinterpreted as a system-reset or permit publication of an arbitrary out-of-order generation.

### 8.4 Protection of Active Reservations, Eligible Tombstone Search & Strict Backpressure Recovery

A tombstone or retired resource cannot be evicted while active reservations or dependent consumers rely upon it:

1. **Active Settlement Pin Protection:**
   - A retired pin or tombstone for generation $g$ cannot be evicted if an active settlement pin exists in `activeSettlementPins` for generation $g$.
2. **Active Retry Lease Protection:**
   - A retired pin or tombstone for generation $g$ cannot be evicted if an active retry lease exists in `activeRetryLeases` for generation $g$ with `expiresAtMonotonicMs > performance.now()`.
3. **Pending Settlement Obligation Protection:**
   - A retired pin or tombstone for generation $g$ cannot be evicted if `pendingSettlementObligations.has(g)` is true.
4. **Deterministic Eligible Tombstone Search & Fail-Closed Backpressure:**
   - When collection capacity reaches 5,000 entries, the coordinator searches deterministically for the oldest *eligible* entry that satisfies all three protection rules.
   - If all 5,000 entries are currently protected by active pins, active leases, or pending obligations, the coordinator strictly refuses to perform silent or unsafe eviction.
   - Instead, the operation aborts and fails closed with `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE` (or `RETIRED_LEASES_CAPACITY_EXHAUSTED_BACKPRESSURE` / `CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED`), signaling callers to retry after active consumers have released their protection.
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
- **Atomic In-Root Replay Protection:** Evidence replay is checked and consumed directly inside the atomic candidate root publication (`consumedSettlementEvidence: ImmutableSet<string>`). Replaying evidence for another pin, obligation, or generation fails closed with `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

### 12.7 Closed Generation-Scoped Barrier Lifecycle

#### Lifecycle State Transitions:
1. `ALLOCATED` $\longrightarrow$ 2. `PENDING_SETTLEMENT` $\longrightarrow$ 3. `RESOLVED_WITH_EVIDENCE` (or `LATCHED_BLOCKED`).

Each barrier resolves at most once. A barrier cannot be reused for a different generation.

### 12.8 Separation of the Three Guarantees

1. **Safety:** No duplicate execution; no lost operations; single custody at all times.
2. **Liveness:** Bounded retries; non-blocking settlement under normal conditions; graceful backpressure under saturation.
3. **Auditability:** Complete, immutable, verifiable receipt chain for every generation.

### 12.9 Runtime Immutability of Published Collections

In ECMAScript environments, applying `Object.freeze()` to native `Map` or `Set` instances freezes only the object prototype and property descriptors; it does **not** intercept internal slots, meaning `.set()`, `.delete()`, and `.clear()` continue to mutate the collection contents in place.

To enforce absolute custody-root immutability, all published collections are encapsulated in genuinely immutable wrapper classes (`ImmutableMap<K, V>` and `ImmutableSet<T>`). These wrappers encapsulate the internal map/set in private backing storage, omit mutating methods, provide read-only iteration interfaces, and return new immutable instances upon copy-on-write operations (`.set()`, `.delete()`, `.add()`).

```typescript
export class ImmutableMap<K, V> implements Iterable<[K, V]> {
  private readonly _inner: ReadonlyMap<K, V>;

  public constructor(entries?: Iterable<readonly [K, V]> | ReadonlyMap<K, V>) {
    this._inner = entries instanceof Map ? new Map(entries) : new Map(entries ?? []);
    Object.freeze(this);
  }

  public get size(): number {
    return this._inner.size;
  }

  public get(key: K): V | undefined {
    return this._inner.get(key);
  }

  public has(key: K): boolean {
    return this._inner.has(key);
  }

  public entries(): IterableIterator<[K, V]> {
    return this._inner.entries();
  }

  public keys(): IterableIterator<K> {
    return this._inner.keys();
  }

  public values(): IterableIterator<V> {
    return this._inner.values();
  }

  public [Symbol.iterator](): IterableIterator<[K, V]> {
    return this._inner[Symbol.iterator]();
  }

  public set(key: K, value: V): ImmutableMap<K, V> {
    const next = new Map(this._inner);
    next.set(key, value);
    return new ImmutableMap(next);
  }

  public delete(key: K): ImmutableMap<K, V> {
    const next = new Map(this._inner);
    next.delete(key);
    return new ImmutableMap(next);
  }
}

export class ImmutableSet<T> implements Iterable<T> {
  private readonly _inner: ReadonlySet<T>;

  public constructor(values?: Iterable<T> | ReadonlySet<T>) {
    this._inner = values instanceof Set ? new Set(values) : new Set(values ?? []);
    Object.freeze(this);
  }

  public get size(): number {
    return this._inner.size;
  }

  public has(value: T): boolean {
    return this._inner.has(value);
  }

  public values(): IterableIterator<T> {
    return this._inner.values();
  }

  public [Symbol.iterator](): IterableIterator<T> {
    return this._inner[Symbol.iterator]();
  }

  public add(value: T): ImmutableSet<T> {
    const next = new Set(this._inner);
    next.add(value);
    return new ImmutableSet(next);
  }

  public delete(value: T): ImmutableSet<T> {
    const next = new Set(this._inner);
    next.delete(value);
    return new ImmutableSet(next);
  }
}
```

#### Defensive Handling of Byte Arrays & Bitwise Digest Comparison:
All byte arrays, buffers, and cryptographic payload digests (`Uint8Array`) are defensively cloned via `new Uint8Array(src)` on ingress before storage and on egress before return.

Equality comparisons across byte containers **never** use reference equality (`===` or `!==`), which fail for independently allocated arrays with identical bytes. Instead, equality is evaluated strictly via constant-time bitwise comparison:

```typescript
export function areEqualByteArrays(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) {
    mismatch |= a[i] ^ b[i];
  }
  return mismatch === 0;
}

export function cloneByteArray(src: Uint8Array): Uint8Array {
  const dest = new Uint8Array(src.length);
  dest.set(src);
  return dest;
}
```

#### Structured Owner Token Identity Preservation:
Owner token equality is strictly preserved across all eight structural dimensions, including attempt ID, generation, transaction, operation, barrier, lease term, and digital signature:

```typescript
export function isEquivalentOwnerToken(a: CustodyOwnershipToken | null, b: CustodyOwnershipToken | null): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.recoveryAttemptId === b.recoveryAttemptId &&
    a.custodyGeneration === b.custodyGeneration &&
    a.drainerOwnershipGeneration === b.drainerOwnershipGeneration &&
    a.transactionId === b.transactionId &&
    a.operationIdentity === b.operationIdentity &&
    a.barrierIdentity === b.barrierIdentity &&
    a.leaseTerm === b.leaseTerm &&
    areEqualByteArrays(a.ownerSignature, b.ownerSignature)
  );
}
```

### 12.10 Ingress and Egress Nested Evidence Alias Closure

All objects entering `AuthoritativeCustodyRoot` are deeply cloned and frozen on ingress. All objects returned from publication methods are immutable cached references, preventing reference sharing and memory corruption.
### 12.11 Permitted Generation Domain & Sequence Exhaustion Boundary

- Permitted generations: integers $g \in [1, \text{Number.MAX\_SAFE\_INTEGER}]$.
- Out-of-domain values ($g \le 0$, non-integers, NaN, $\infty$) are rejected upfront with `INVALID_GENERATION_DOMAIN`.
- Sequence exhaustion ($g = \text{Number.MAX\_SAFE\_INTEGER}$) triggers fail-closed `GENERATION_SEQUENCE_EXHAUSTED`.

### 12.12 The Complete Lifecycle Publication, Durable Boot Fencing, Encapsulated Immutable Custody & Strict Replay Invariant Verification

#### The Complete Custody Root State Structure:

```typescript
export interface CoordinatorBootEpoch {
  readonly clusterTerm: number;       // Monotonic consensus term under which leadership was granted
  readonly bootGeneration: number;    // Durable monotonically incrementing boot sequence (WAL/consensus)
  readonly bootInstanceId: string;    // Collision-resistant cryptographically secure UUIDv4
}

export interface CustodyOwnershipToken {
  readonly recoveryAttemptId: string;
  readonly custodyGeneration: number;
  readonly drainerOwnershipGeneration: number;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly leaseTerm: number;
  readonly ownerSignature: Uint8Array;
}

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

// B1 & C1: Bound 7-dimensional active and retired records with compound durable boot epoch
export interface ImmutableSettlementPin {
  readonly pinId: string; // pin:${coordinatorBootId}:${generation}:${barrierIdentity}:${seq}
  readonly generation: number;
  readonly barrierIdentity: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly acquiredAtTimestamp: number;
  readonly monotonicAcquiredMs: number;
  readonly allocatedBootEpoch: CoordinatorBootEpoch;
}

export interface ImmutableRetiredPinRecord {
  readonly pinId: string;
  readonly generation: number;
  readonly barrierIdentity: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly drainerOwnershipGeneration: number;
  readonly releasedAtTimestamp: number;
  readonly monotonicReleasedMs: number;
  readonly evidenceDigest: Uint8Array;
  readonly allocatedBootEpoch: CoordinatorBootEpoch;
}

export interface RetryRequestContext {
  readonly consumerId: string;
  readonly clientRequestId: string;
}

export interface ImmutableRetryLease {
  readonly leaseId: string; // lease:${coordinatorBootId}:${generation}:${consumerId}:${seq}
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
  readonly allocatedBootEpoch: CoordinatorBootEpoch;
}

export interface ImmutableRetiredLeaseRecord {
  readonly leaseId: string;
  readonly generation: number;
  readonly consumerId: string;
  readonly clientRequestId: string;
  readonly ownerToken: CustodyOwnershipToken;
  readonly transactionId: string;
  readonly operationIdentity: string;
  readonly barrierIdentity: string;
  readonly releasedAtTimestamp: number;
  readonly monotonicReleasedMs: number;
  readonly allocatedBootEpoch: CoordinatorBootEpoch;
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
  readonly custodyGenerationFloor: number;
  readonly auditTombstoneFloor: number;
  readonly inFlightRecord: InFlightCustodyRecord | null;
  readonly gateState: CustodyGateState;
  readonly pendingSettlementObligations: ImmutableMap<number, PendingSettlementObligation>;
  readonly publicationResults: ImmutableMap<number, PublicationResult>;
  readonly generationAuditHistory: ImmutableMap<number, GenerationAuditRecord>;
  readonly activeSettlementPins: ImmutableMap<string, ImmutableSettlementPin>;
  readonly activeRetryLeases: ImmutableMap<string, ImmutableRetryLease>;
  readonly retiredRetryLeases: ImmutableMap<string, ImmutableRetiredLeaseRecord>;
  readonly retiredSettlementPins: ImmutableMap<string, ImmutableRetiredPinRecord>;
  readonly consumedSettlementEvidence: ImmutableSet<string>;
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

#### Durable Coordinator Boot Epoch Fencing Contract:

To prevent resource identifier collisions across process restarts and split-brain coordinator transitions:
1. **Compound Durable Boot Epoch (`coordinatorBootEpoch`):**
   - At coordinator startup, the node establishes a compound boot epoch:
     `this._coordinatorBootEpoch = { clusterTerm, bootGeneration, bootInstanceId }`
   - Formatted canonical string:
     `this._coordinatorBootId = `${clusterTerm}.${bootGeneration}.${bootInstanceId}``
   - `clusterTerm`: Monotonic consensus term under which recovery leadership was granted; stale coordinators lose lease validity and cannot execute arbitration.
   - `bootGeneration`: Monotonically incrementing sequence number persisted and fsynced to Write-Ahead Log (WAL) or consensus state before coordinator traffic admission; mathematically prevents sequence counter reuse across node restarts.
   - `bootInstanceId`: Cryptographically secure, collision-resistant UUIDv4 uniquely identifying this specific process memory execution.
2. **Canonical Non-Reusable Identifier Templates:**
   - Settlement Pin: `pin:${this._coordinatorBootId}:${generation}:${barrierIdentity}:${seq}`
   - Retry Lease: `lease:${this._coordinatorBootId}:${generation}:${consumerId}:${seq}`
3. **Allocation Interruption Semantics:**
   - Pin and lease allocations execute synchronously during candidate root construction.
   - If candidate construction or invariant validation throws an exception prior to the atomic pointer replacement `this.#custodyRoot = candidateRoot`, the reserved sequence counter `seq` is consumed/discarded, and the ID is never published to active collections, guaranteeing zero identifier collisions.
4. **Cross-Restart Delayed Release Rejection:**
   - Any delayed release request presenting an identifier from an earlier process lifetime (`bootGeneration_old`) fails closed as `RESOURCE_NOT_FOUND` because the new instance (`bootGeneration_new`) has never allocated or retired that identifier.

#### Symmetrical Pre-Existing Overflow Fencing & Backpressure:

1. **Pre-Existing Capacity Fencing:**
   - Before any retired record insertion, the coordinator validates that pre-existing collection sizes do not exceed the authoritative 5,000-entry cap:
     ```typescript
     if (currentRoot.retiredSettlementPins.size > 5000) {
       throw new Error('RETIRED_PINS_CAPACITY_CORRUPTED: pre-existing count exceeds 5000');
     }
     if (currentRoot.retiredRetryLeases.size > 5000) {
       throw new Error('RETIRED_LEASES_CAPACITY_CORRUPTED: pre-existing count exceeds 5000');
     }
     if (currentRoot.consumedSettlementEvidence.size > 5000) {
       throw new Error('CONSUMED_EVIDENCE_CAPACITY_CORRUPTED: pre-existing count exceeds 5000');
     }
     ```
   - If pre-existing corruption exists ($> 5000$), the system fails closed immediately. It **does not** perform silent single-entry trimming.
2. **Deterministic Oldest Eligible Tombstone Search & Backpressure:**
   - At exactly 5,000 entries (`candidateRetired.size === 5000`), the coordinator searches for the oldest *eligible* entry that is not protected by active retry leases, active settlement pins, or pending settlement obligations.
   - If all 5,000 entries are currently protected, the operation aborts and fails closed with `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE` (or `RETIRED_LEASES_CAPACITY_EXHAUSTED_BACKPRESSURE`), strictly refusing to evict protected entries.
3. **Lockstep Atomic Replay Retention & Post-Publication Invariant Verification:**
   - For retired pins, the coordinator validates that the replay key `${oldestRecord.generation}:${oldestRecord.barrierIdentity}:${oldestPinId}` strictly exists in `candidateConsumed`; if missing, it throws `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED`.
   - The oldest pin and its replay key are deleted in lockstep, and the new pin and new replay key are inserted.
   - Before publication, `_validatePostPublicationInvariants()` verifies that all three collections are $\le 5000$, confirms that every retired pin has its exact replay key, and verifies size lockstep (`retiredSettlementPins.size === consumedSettlementEvidence.size`).

---

#### Complete Corrected Architectural Pseudocode:

```typescript
const DRAINER_RELEASE_BRAND = Symbol('@syncplay/DrainerCustodyReleaseReceipt');
const SETTLEMENT_EVIDENCE_BRAND = Symbol('@syncplay/SettlementEvidence');
const AUTHORIZED_EVIDENCE_REGISTRY = new WeakSet<AuthoritativeSettlementEvidence>();

export function areEqualByteArrays(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) {
    mismatch |= a[i] ^ b[i];
  }
  return mismatch === 0;
}

export function cloneByteArray(src: Uint8Array): Uint8Array {
  const dest = new Uint8Array(src.length);
  dest.set(src);
  return dest;
}

export function isEquivalentOwnerToken(a: CustodyOwnershipToken | null, b: CustodyOwnershipToken | null): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.recoveryAttemptId === b.recoveryAttemptId &&
    a.custodyGeneration === b.custodyGeneration &&
    a.drainerOwnershipGeneration === b.drainerOwnershipGeneration &&
    a.transactionId === b.transactionId &&
    a.operationIdentity === b.operationIdentity &&
    a.barrierIdentity === b.barrierIdentity &&
    a.leaseTerm === b.leaseTerm &&
    areEqualByteArrays(a.ownerSignature, b.ownerSignature)
  );
}

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

    if (
      params.releaseReceipt.releasedCustodyGeneration !== params.generation ||
      params.releaseReceipt.identity.drainerOwnershipGeneration !== params.drainerOwnershipGeneration ||
      params.releaseReceipt.identity.transactionId !== params.transactionId ||
      params.releaseReceipt.identity.operationIdentity !== params.operationIdentity ||
      !isEquivalentOwnerToken(params.releaseReceipt.identity, params.expectedOwner)
    ) {
      throw new Error('SETTLEMENT_EVIDENCE_RECEIPT_IDENTITY_CORRUPTED');
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
      evidenceDigest: cloneByteArray(computeEvidenceDigest(params)),
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
}

public assertRetentionMapContiguity(
  resultsMap: ImmutableMap<number, PublicationResult>,
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
  auditMap: ImmutableMap<number, GenerationAuditRecord>,
  auditFloor: number,
  publishedGeneration: number,
  custodyFloor: number,
  publicationResults?: ImmutableMap<number, PublicationResult>,
): void {
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

    if (record.generation !== g) {
      throw new Error(`AUDIT_RECORD_GENERATION_MISMATCH: key ${g} !== record.generation ${record.generation}`);
    }

    if (g <= custodyFloor) {
      if (record.kind !== 'COMPACT_TOMBSTONE') {
        throw new Error(`STATE_INCONSISTENT_TOMBSTONE_SCHEMA_VIOLATION: key ${g} must be COMPACT_TOMBSTONE, got ${record.kind}`);
      }
      if (!record.evidenceDigest || !(record.evidenceDigest instanceof Uint8Array) || record.evidenceDigest.length !== 32) {
        throw new Error(`STATE_INCONSISTENT_TOMBSTONE_DIGEST_CORRUPTED: key ${g}`);
      }
    } else {
      if (record.kind !== 'PENDING_PUBLICATION' && record.kind !== 'TERMINAL_SETTLEMENT') {
        throw new Error(`STATE_INCONSISTENT_AUDIT_SCHEMA_VIOLATION: key ${g} must be PENDING_PUBLICATION or TERMINAL_SETTLEMENT, got ${record.kind}`);
      }
      if (record.kind === 'TERMINAL_SETTLEMENT') {
        if (record.terminalOutcome !== 'CONFIRMED_COMMITTED' && record.terminalOutcome !== 'ABORTED_FENCED') {
          throw new Error(`STATE_INCONSISTENT_TERMINAL_OUTCOME_CORRUPTED: key ${g}`);
        }
      }
    }

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

private _findOldestEligibleRetiredPin(
  root: AuthoritativeCustodyRoot,
  candidateRetired: Map<string, ImmutableRetiredPinRecord>
): [string, ImmutableRetiredPinRecord] | undefined {
  const monotonicNow = performance.now();
  for (const [pinId, record] of candidateRetired) {
    // 1. Must not be pinned by active retry lease
    let isRetainedByLease = false;
    for (const lease of root.activeRetryLeases.values()) {
      if (
        lease.generation === record.generation &&
        lease.barrierIdentity === record.barrierIdentity &&
        lease.expiresAtMonotonicMs > monotonicNow
      ) {
        isRetainedByLease = true;
        break;
      }
    }
    if (isRetainedByLease) continue;

    // 2. Must not be protected by active settlement pin in same barrier
    let isRetainedByPin = false;
    for (const pin of root.activeSettlementPins.values()) {
      if (pin.generation === record.generation && pin.barrierIdentity === record.barrierIdentity) {
        isRetainedByPin = true;
        break;
      }
    }
    if (isRetainedByPin) continue;

    // 3. Must not belong to an active pending settlement obligation
    if (root.pendingSettlementObligations.has(record.generation)) continue;

    // Eligible tombstone found
    return [pinId, record];
  }
  return undefined;
}

private _findOldestEligibleRetiredLease(
  root: AuthoritativeCustodyRoot,
  candidateRetired: Map<string, ImmutableRetiredLeaseRecord>
): [string, ImmutableRetiredLeaseRecord] | undefined {
  const monotonicNow = performance.now();
  for (const [leaseId, record] of candidateRetired) {
    let isRetained = false;
    for (const active of root.activeRetryLeases.values()) {
      if (
        active.generation === record.generation &&
        active.consumerId === record.consumerId &&
        active.expiresAtMonotonicMs > monotonicNow
      ) {
        isRetained = true;
        break;
      }
    }
    if (isRetained) continue;
    return [leaseId, record];
  }
  return undefined;
}

private _validatePostPublicationInvariants(root: AuthoritativeCustodyRoot): void {
  // 1. Strict capacity upper bound
  if (root.retiredSettlementPins.size > 5000) {
    throw new Error(`POST_PUBLICATION_RETIRED_PINS_OVERFLOW: size ${root.retiredSettlementPins.size} > 5000`);
  }
  if (root.retiredRetryLeases.size > 5000) {
    throw new Error(`POST_PUBLICATION_RETIRED_LEASES_OVERFLOW: size ${root.retiredRetryLeases.size} > 5000`);
  }
  if (root.consumedSettlementEvidence.size > 5000) {
    throw new Error(`POST_PUBLICATION_CONSUMED_EVIDENCE_OVERFLOW: size ${root.consumedSettlementEvidence.size} > 5000`);
  }

  // 2. Strict replay-key cross-collection mapping
  for (const [pinId, pin] of root.retiredSettlementPins) {
    const expectedKey = `${pin.generation}:${pin.barrierIdentity}:${pinId}`;
    if (!root.consumedSettlementEvidence.has(expectedKey)) {
      throw new Error(`POST_PUBLICATION_REPLAY_KEY_MISSING: missing replay key '${expectedKey}' for retired pin '${pinId}'`);
    }
  }

  // 3. Exact lockstep size invariant
  if (root.retiredSettlementPins.size !== root.consumedSettlementEvidence.size) {
    throw new Error(`POST_PUBLICATION_COLLECTION_SIZE_DESYNCHRONIZATION: retired pins ${root.retiredSettlementPins.size} !== consumed evidence ${root.consumedSettlementEvidence.size}`);
  }
}

private _getOrAllocateSettlementPin(
  currentRoot: AuthoritativeCustodyRoot,
  generation: number,
  barrierIdentity: string,
  ownerToken: CustodyOwnershipToken,
  drainerGeneration: number
): { pinId: string; nextPins: ImmutableMap<string, ImmutableSettlementPin> } {
  // 1. Check for existing active settlement pin for this generation and barrier
  for (const [pinId, pin] of currentRoot.activeSettlementPins) {
    if (pin.generation === generation && pin.barrierIdentity === barrierIdentity) {
      if (!isEquivalentOwnerToken(pin.ownerToken, ownerToken)) {
        throw new Error('SETTLEMENT_PIN_OWNER_TOKEN_MISMATCH');
      }
      if (pin.drainerOwnershipGeneration !== drainerGeneration) {
        throw new Error('SETTLEMENT_PIN_DRAINER_GENERATION_MISMATCH');
      }
      return { pinId, nextPins: currentRoot.activeSettlementPins };
    }
  }

  // 2. Compound Durable Boot Epoch Pin Allocation
  const seq = ++this._pinSequenceCounter;
  const pinId = `pin:${this._coordinatorBootId}:${generation}:${barrierIdentity}:${seq}`;

  const newPin: ImmutableSettlementPin = Object.freeze({
    pinId,
    generation,
    barrierIdentity,
    ownerToken: cloneAndFreezeOwnerToken(ownerToken),
    transactionId: ownerToken.transactionId,
    operationIdentity: ownerToken.operationIdentity,
    drainerOwnershipGeneration: drainerGeneration,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: performance.now(),
    allocatedBootEpoch: this._coordinatorBootEpoch,
  });

  const nextPins = currentRoot.activeSettlementPins.set(pinId, newPin);
  return { pinId, nextPins };
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
        existingLease.barrierIdentity === barrierIdentity &&
        existingLease.ownerToken.drainerOwnershipGeneration === ownerToken.drainerOwnershipGeneration
      ) {
        return { candidateLeases: root.activeRetryLeases, leaseId: existingLease.leaseId };
      }
      throw new Error('IDEMPOTENT_RETRY_IDENTITY_CONFLICT');
    }
  }

  // Compound Durable Boot Epoch Lease Allocation
  const seq = ++this._leaseSequenceCounter;
  const leaseId = `lease:${this._coordinatorBootId}:${generation}:${consumerId}:${seq}`;

  if (root.activeRetryLeases.has(leaseId)) {
    throw new Error('RETRY_LEASE_ID_COLLISION');
  }

  const monotonicNow = performance.now();
  const lease: ImmutableRetryLease = Object.freeze({
    leaseId,
    generation,
    consumerId,
    clientRequestId,
    ownerToken: cloneAndFreezeOwnerToken(ownerToken),
    transactionId,
    operationIdentity,
    barrierIdentity,
    acquiredAtTimestamp: Date.now(),
    monotonicAcquiredMs: monotonicNow,
    expiresAtMonotonicMs: monotonicNow + 30000,
    allocatedBootEpoch: this._coordinatorBootEpoch,
  });

  const candidateLeases = root.activeRetryLeases.set(leaseId, lease);
  return { candidateLeases, leaseId };
}

public releaseRetryLease(params: ReleaseRetryLeaseParams): { leaseId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
  const { leaseId, consumerId, clientRequestId, callerToken, transactionId, operationIdentity, barrierIdentity } = params;

  // Cross-Resource Type Checking
  if (leaseId.startsWith('pin:')) {
    throw new Error('LEASE_TYPE_MISMATCH: identifier is a settlement pin, not a retry lease');
  }
  if (!leaseId.startsWith('lease:')) {
    throw new Error(`RESOURCE_NOT_FOUND: unknown retry lease identifier '${leaseId}'`);
  }

  const currentRoot = this.#custodyRoot;
  const activeLease = currentRoot.activeRetryLeases.get(leaseId);

  // Authoritative Resource Retirement Resolution
  if (!activeLease) {
    const retiredLease = currentRoot.retiredRetryLeases.get(leaseId);
    if (retiredLease) {
      // Complete 7-dimensional identity validation on retired path
      if (retiredLease.consumerId !== consumerId) {
        throw new Error(`UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE: caller consumer '${consumerId}' does not match retired lease owner '${retiredLease.consumerId}'`);
      }
      if (retiredLease.clientRequestId !== clientRequestId) {
        throw new Error(`LEASE_CLIENT_REQUEST_MISMATCH: clientRequestId '${clientRequestId}' does not match retired lease`);
      }
      if (!isEquivalentOwnerToken(retiredLease.ownerToken, callerToken)) {
        throw new Error('UNAUTHORIZED_LEASE_RELEASE_TOKEN_MISMATCH: caller token does not match retired lease owner token');
      }
      if (retiredLease.transactionId !== transactionId) {
        throw new Error(`LEASE_TRANSACTION_ID_MISMATCH: transactionId '${transactionId}' does not match retired lease`);
      }
      if (retiredLease.operationIdentity !== operationIdentity) {
        throw new Error(`LEASE_OPERATION_IDENTITY_MISMATCH: operationIdentity '${operationIdentity}' does not match retired lease`);
      }
      if (retiredLease.barrierIdentity !== barrierIdentity) {
        throw new Error(`LEASE_BARRIER_IDENTITY_MISMATCH: barrierIdentity '${barrierIdentity}' does not match retired lease`);
      }
      return { leaseId, status: 'ALREADY_RELEASED' };
    }
    // Genuinely unknown or never-issued lease (or from prior reboot)
    throw new Error(`RESOURCE_NOT_FOUND: retry lease '${leaseId}' was never issued or is unknown`);
  }

  // Active Lease Authorization Check across all 7 dimensions
  if (activeLease.consumerId !== consumerId) {
    throw new Error(`UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE: caller consumer ${consumerId} does not own lease ${leaseId} owned by ${activeLease.consumerId}`);
  }
  if (activeLease.clientRequestId !== clientRequestId) {
    throw new Error(`LEASE_CLIENT_REQUEST_MISMATCH: clientRequestId ${clientRequestId} does not match lease ${activeLease.clientRequestId}`);
  }
  if (!isEquivalentOwnerToken(activeLease.ownerToken, callerToken)) {
    throw new Error('UNAUTHORIZED_LEASE_RELEASE_TOKEN_MISMATCH: caller token does not match lease owner token');
  }
  if (activeLease.transactionId !== transactionId) {
    throw new Error(`LEASE_TRANSACTION_ID_MISMATCH: transactionId ${transactionId} does not match lease`);
  }
  if (activeLease.operationIdentity !== operationIdentity) {
    throw new Error(`LEASE_OPERATION_IDENTITY_MISMATCH: operationIdentity ${operationIdentity} does not match lease`);
  }
  if (activeLease.barrierIdentity !== barrierIdentity) {
    throw new Error(`LEASE_BARRIER_IDENTITY_MISMATCH: barrierIdentity ${barrierIdentity} does not match lease`);
  }

  // Pre-Existing Overflow Fencing
  if (currentRoot.retiredRetryLeases.size > 5000) {
    throw new Error('RETIRED_LEASES_CAPACITY_CORRUPTED: pre-existing retired leases count exceeds 5000');
  }

  const candidateActiveLeases = currentRoot.activeRetryLeases.delete(leaseId);
  const candidateRetiredLeasesMap = new Map(currentRoot.retiredRetryLeases.entries());

  // Deterministic Eligible Tombstone Eviction with Backpressure
  if (candidateRetiredLeasesMap.size === 5000) {
    const eligibleEntry = this._findOldestEligibleRetiredLease(currentRoot, candidateRetiredLeasesMap);
    if (!eligibleEntry) {
      throw new Error('RETIRED_LEASES_CAPACITY_EXHAUSTED_BACKPRESSURE: all 5000 retired leases are protected');
    }
    const [oldestLeaseId] = eligibleEntry;
    candidateRetiredLeasesMap.delete(oldestLeaseId);
  }

  const retiredRecord: ImmutableRetiredLeaseRecord = Object.freeze({
    leaseId,
    generation: activeLease.generation,
    consumerId,
    clientRequestId,
    ownerToken: activeLease.ownerToken,
    transactionId,
    operationIdentity,
    barrierIdentity,
    releasedAtTimestamp: Date.now(),
    monotonicReleasedMs: performance.now(),
    allocatedBootEpoch: activeLease.allocatedBootEpoch,
  });
  candidateRetiredLeasesMap.set(leaseId, retiredRecord);

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    ...currentRoot,
    activeRetryLeases: candidateActiveLeases,
    retiredRetryLeases: new ImmutableMap(candidateRetiredLeasesMap.entries()),
  });

  this._validatePostPublicationInvariants(candidateRoot);
  this.#custodyRoot = candidateRoot;
  return { leaseId, status: 'RELEASED' };
}

public releaseSettlementPin(pinId: string, settlementEvidence: AuthoritativeSettlementEvidence): { pinId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
  // Cross-Resource Type Checking
  if (pinId.startsWith('lease:')) {
    throw new Error('PIN_TYPE_MISMATCH: identifier is a retry lease, not a settlement pin');
  }
  if (!pinId.startsWith('pin:')) {
    throw new Error(`RESOURCE_NOT_FOUND: unknown settlement pin identifier '${pinId}'`);
  }

  // Mandatory Unconditional Evidence Provenance Validation
  SettlementEvidenceFactory.assertAuthoritativeEvidenceProvenance(settlementEvidence);

  const currentRoot = this.#custodyRoot;
  const activePin = currentRoot.activeSettlementPins.get(pinId);

  // Authoritative Resource Retirement Resolution
  if (!activePin) {
    const retiredPin = currentRoot.retiredSettlementPins.get(pinId);
    if (retiredPin) {
      // Complete 7-dimensional identity validation on retired pin
      if (
        !isEquivalentOwnerToken(retiredPin.ownerToken, settlementEvidence.expectedOwner) ||
        retiredPin.generation !== settlementEvidence.generation ||
        retiredPin.transactionId !== settlementEvidence.transactionId ||
        retiredPin.operationIdentity !== settlementEvidence.operationIdentity ||
        retiredPin.barrierIdentity !== settlementEvidence.barrierIdentity ||
        retiredPin.drainerOwnershipGeneration !== settlementEvidence.drainerOwnershipGeneration ||
        retiredPin.ownerToken.recoveryAttemptId !== settlementEvidence.expectedOwner.recoveryAttemptId
      ) {
        throw new Error('SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH: evidence does not match retired pin identity');
      }

      // Constant-time bitwise byte digest comparison
      if (!areEqualByteArrays(retiredPin.evidenceDigest, settlementEvidence.evidenceDigest)) {
        throw new Error('SETTLEMENT_EVIDENCE_DIGEST_MISMATCH: evidence digest does not match recorded retired pin disposition');
      }

      return { pinId, status: 'ALREADY_RELEASED' };
    }
    // Genuinely unknown or never-issued pin (or from prior reboot)
    throw new Error(`RESOURCE_NOT_FOUND: settlement pin '${pinId}' was never issued or is unknown`);
  }

  // Complete 7-dimensional identity validation including drainerOwnershipGeneration
  if (
    !isEquivalentOwnerToken(activePin.ownerToken, settlementEvidence.expectedOwner) ||
    activePin.generation !== settlementEvidence.generation ||
    activePin.transactionId !== settlementEvidence.transactionId ||
    activePin.operationIdentity !== settlementEvidence.operationIdentity ||
    activePin.barrierIdentity !== settlementEvidence.barrierIdentity ||
    activePin.drainerOwnershipGeneration !== settlementEvidence.drainerOwnershipGeneration ||
    activePin.ownerToken.recoveryAttemptId !== settlementEvidence.expectedOwner.recoveryAttemptId
  ) {
    throw new Error('SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH');
  }

  // In-Root Evidence Consumption Protocol
  const replayKey = `${settlementEvidence.generation}:${settlementEvidence.barrierIdentity}:${pinId}`;
  if (currentRoot.consumedSettlementEvidence.has(replayKey)) {
    throw new Error(`SETTLEMENT_EVIDENCE_REPLAY_DETECTED: evidence for replay key '${replayKey}' already consumed in custody root`);
  }

  // Pre-Existing Overflow Fencing
  if (currentRoot.retiredSettlementPins.size > 5000) {
    throw new Error('RETIRED_PINS_CAPACITY_CORRUPTED: pre-existing retired pins count exceeds 5000');
  }
  if (currentRoot.consumedSettlementEvidence.size > 5000) {
    throw new Error('CONSUMED_EVIDENCE_CAPACITY_CORRUPTED: pre-existing consumed evidence count exceeds 5000');
  }

  const candidatePinsMap = new Map(currentRoot.activeSettlementPins.entries());
  candidatePinsMap.delete(pinId);

  const retiredPinRecord: ImmutableRetiredPinRecord = Object.freeze({
    pinId,
    generation: activePin.generation,
    barrierIdentity: activePin.barrierIdentity,
    ownerToken: activePin.ownerToken,
    transactionId: activePin.transactionId,
    operationIdentity: activePin.operationIdentity,
    drainerOwnershipGeneration: activePin.drainerOwnershipGeneration,
    releasedAtTimestamp: Date.now(),
    monotonicReleasedMs: performance.now(),
    evidenceDigest: cloneByteArray(settlementEvidence.evidenceDigest),
    allocatedBootEpoch: activePin.allocatedBootEpoch,
  });

  const candidateRetiredPins = new Map(currentRoot.retiredSettlementPins.entries());
  const candidateConsumed = new Set(currentRoot.consumedSettlementEvidence.values());

  // Deterministic Oldest Eligible Tombstone Eviction with Backpressure
  if (candidateRetiredPins.size === 5000) {
    const eligibleEntry = this._findOldestEligibleRetiredPin(currentRoot, candidateRetiredPins);
    if (!eligibleEntry) {
      throw new Error('RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE: all 5000 retired settlement pins are protected by active leases, pins, or obligations');
    }
    const [oldestPinId, oldestRecord] = eligibleEntry;
    const oldestReplayKey = `${oldestRecord.generation}:${oldestRecord.barrierIdentity}:${oldestPinId}`;

    // Strict Replay-Key Existence Check
    if (!candidateConsumed.has(oldestReplayKey)) {
      throw new Error(`REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED: replay key '${oldestReplayKey}' missing for retired pin '${oldestPinId}'`);
    }

    candidateRetiredPins.delete(oldestPinId);
    candidateConsumed.delete(oldestReplayKey);
  }

  candidateRetiredPins.set(pinId, retiredPinRecord);
  candidateConsumed.add(replayKey);

  // Construct Candidate Root with Genuinely Encapsulated Immutable Collections
  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    ...currentRoot,
    activeSettlementPins: new ImmutableMap(candidatePinsMap.entries()),
    retiredSettlementPins: new ImmutableMap(candidateRetiredPins.entries()),
    consumedSettlementEvidence: new ImmutableSet(candidateConsumed),
  });

  // Post-Publication Cross-Collection Invariant Check
  this._validatePostPublicationInvariants(candidateRoot);

  // SINGLE ATOMIC PUBLICATION LINEARIZATION POINT:
  this.#custodyRoot = candidateRoot;
  return { pinId, status: 'RELEASED' };
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
        existingObligation.drainerOwnershipGeneration !== cachedResult.releaseReceipt.identity.drainerOwnershipGeneration ||
        cachedResult.releaseReceipt.releasedCustodyGeneration !== g
      ) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH');
      }

      if (
        !isEquivalentOwnerToken(existingObligation.ownerToken, safeOwner) ||
        existingObligation.transactionId !== safeTxId ||
        existingObligation.operationIdentity !== safeOpId ||
        existingObligation.barrierIdentity !== safeBarrierId ||
        existingObligation.drainerOwnershipGeneration !== safeOwner.drainerOwnershipGeneration
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, retryContext.consumerId, retryContext.clientRequestId, safeOwner, safeTxId, safeOpId, safeBarrierId
      );

      if (candidateLeases !== currentRoot.activeRetryLeases) {
        const nextRoot: AuthoritativeCustodyRoot = Object.freeze({
          ...currentRoot,
          activeRetryLeases: candidateLeases,
        });
        this._validatePostPublicationInvariants(nextRoot);
        this.#custodyRoot = nextRoot;
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
        auditRecord.barrierIdentity !== cachedResult.obligation.barrierIdentity ||
        auditRecord.drainerOwnershipGeneration !== cachedResult.releaseReceipt.identity.drainerOwnershipGeneration
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
        cachedResult.obligation.barrierIdentity !== safeBarrierId ||
        cachedResult.releaseReceipt.identity.drainerOwnershipGeneration !== safeOwner.drainerOwnershipGeneration
      ) {
        throw new Error('CUSTODY_PUBLICATION_TOKEN_MISMATCH');
      }

      const { candidateLeases, leaseId } = this._getOrAllocateRetryLease(
        currentRoot, g, retryContext.consumerId, retryContext.clientRequestId, safeOwner, safeTxId, safeOpId, safeBarrierId
      );

      if (candidateLeases !== currentRoot.activeRetryLeases) {
        const nextRoot: AuthoritativeCustodyRoot = Object.freeze({
          ...currentRoot,
          activeRetryLeases: candidateLeases,
        });
        this._validatePostPublicationInvariants(nextRoot);
        this.#custodyRoot = nextRoot;
      }

      return { publicationResult: cachedResult, retryLeaseId: leaseId };
    }

    // Case 3C: Published but Outside Retention Horizon (Evicted / Tombstone Horizon)
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
          tombstone.barrierIdentity !== safeBarrierId ||
          tombstone.drainerOwnershipGeneration !== safeOwner.drainerOwnershipGeneration
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

  const { pinId: settlementPinId, nextPins: candidatePins } = this._getOrAllocateSettlementPin(
    currentRoot, g, safeBarrierId, safeOwner, safeOwner.drainerOwnershipGeneration
  );

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

    for (const pin of currentRoot.activeSettlementPins.values()) {
      if (pin.generation === g_oldest) throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
    }
    for (const lease of currentRoot.activeRetryLeases.values()) {
      if (lease.generation === g_oldest && lease.expiresAtMonotonicMs > monotonicNow) {
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED');
      }
    }

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
      evidenceDigest: cloneByteArray(computeEvidenceDigest(existingAudit)),
    });

    candidateResultsMap.delete(g_oldest);
    candidateAuditHistory.set(g_oldest, tombstone);
    candidateFloor = g_oldest;

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

      for (const lease of currentRoot.activeRetryLeases.values()) {
        if (lease.generation === g_tomb_oldest && lease.expiresAtMonotonicMs > monotonicNow) {
          throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by active retry lease');
        }
      }
      for (const pin of currentRoot.activeSettlementPins.values()) {
        if (pin.generation === g_tomb_oldest) {
          throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by active settlement pin');
        }
      }
      if (currentRoot.pendingSettlementObligations.has(g_tomb_oldest)) {
        throw new Error('TOMBSTONE_CAPACITY_EXHAUSTED: oldest tombstone protected by pending settlement obligation');
      }

      candidateAuditHistory.delete(g_tomb_oldest);
      candidateAuditFloor = g_tomb_oldest;
    }
  }

  candidateResultsMap.set(g, publicationResult);
  candidateAuditHistory.set(g, pendingAuditRecord);

  const candidateObligations = currentRoot.pendingSettlementObligations.set(g, obligation);
  const candidateResults = new ImmutableMap(candidateResultsMap.entries());
  const candidateAudit = new ImmutableMap(candidateAuditHistory.entries());

  this.assertRetentionMapContiguity(candidateResults, candidateFloor, g);
  this.assertAuditHistoryContiguity(candidateAudit, candidateAuditFloor, g, candidateFloor, candidateResults);

  const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
    publishedGeneration: g,
    custodyGenerationFloor: candidateFloor,
    auditTombstoneFloor: candidateAuditFloor,
    inFlightRecord: null,
    gateState: 'CUSTODY_RELEASED_BARRIER_PENDING',
    pendingSettlementObligations: candidateObligations,
    publicationResults: candidateResults,
    generationAuditHistory: candidateAudit,
    activeSettlementPins: candidatePins,
    activeRetryLeases: currentRoot.activeRetryLeases,
    retiredRetryLeases: currentRoot.retiredRetryLeases,
    retiredSettlementPins: currentRoot.retiredSettlementPins,
    consumedSettlementEvidence: currentRoot.consumedSettlementEvidence,
  });

  this._validatePostPublicationInvariants(candidateRoot);
  this.#custodyRoot = candidateRoot;
  return { publicationResult, retryLeaseId: null };
}
```
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
5. `releaseSettlementPin` is authorized using the verified `AuthoritativeSettlementEvidence`, atomically retiring the pin and consuming the evidence.

---

## 12.16 Protocol B: Drainer $\to$ Quarantine Buffer or Destination Transfer (12-Step Fencing Sequence)

Protocol B handles cancelled or uncommitted operations:
1. Verify Level 3 proof of non-commitment (`TRANSACTION_ABORTED_FENCED`).
2. Atomically fence transaction ID in archive.
3. Transition custody gate to `RECONCILING_CUSTODY`.
4. If destination is healthy, transfer operation; otherwise restore to source buffer.
5. Publish release, pending obligation, and pending audit record.
6. Resolve barrier with `ABORTED_FENCED` evidence, updating `generationAuditHistory`.
7. `releaseSettlementPin` is authorized using the verified `AuthoritativeSettlementEvidence`, atomically retiring the pin and consuming the evidence.

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

## 15. Exhaustive Failure and Retry Matrix (44 Deterministic Cases)

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
| **9** | First publication with stale attempt | `IN_FLIGHT` | Drainer | UNALLOCATED | Reject stale attempt | Commit publication |
| **10** | Release attempt with missing token | `BARRIER_PENDING` | Drainer | PENDING | Reject unauthenticated | Release pin |
| **11** | Release attempt with wrong barrier ID | `BARRIER_PENDING` | Drainer | PENDING | Throw barrier mismatch | Release pin |
| **12** | Release attempt with wrong generation | `BARRIER_PENDING` | Drainer | PENDING | Throw generation mismatch | Release pin |
| **13** | Release attempt after watchdog timeout | `BARRIER_PENDING` | Drainer | PENDING | Settle barrier; release pin | Throw timeout error |
| **14** | Eviction of unpinned tombstone | `TOMBSTONE` | Coordinator | RESOLVED | Prune tombstone FIFO | Prune active pin |
| **15** | Eviction blocked by active retry lease | `TOMBSTONE` | Coordinator | RESOLVED | Backpressure / preserve | Silently drop tombstone |
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
| **29** | Truly unknown ID passed to release APIs | `ANY` | Any | ANY | Throw `RESOURCE_NOT_FOUND` | Treat as `ALREADY_RELEASED` |
| **30** | Pin release with mismatched drainer gen | `SETTLED` | Drainer | RESOLVED | Throw `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH` | Authorize release |
| **31** | Retired lease release with altered txId | `ANY` | Any | ANY | Throw `LEASE_TRANSACTION_ID_MISMATCH` | Return `ALREADY_RELEASED` |
| **32** | Retired pin release with unverified provenance| `ANY` | Any | ANY | Throw `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE` | Return `ALREADY_RELEASED` |
| **33** | Pre-existing retired pins count > 5,000 | `ANY` | Any | ANY | Throw `RETIRED_PINS_CAPACITY_CORRUPTED` | Silently trim one |
| **34** | Pre-existing retired leases count > 5,000 | `ANY` | Any | ANY | Throw `RETIRED_LEASES_CAPACITY_CORRUPTED` | Silently trim one |
| **35** | Pre-existing consumed evidence count > 5,000 | `ANY` | Any | ANY | Throw `CONSUMED_EVIDENCE_CAPACITY_CORRUPTED` | Silently trim one |
| **36** | Delayed release presenting pin ID from prior boot | `ANY` | Any | ANY | Throw `RESOURCE_NOT_FOUND` | Match new session pin |
| **37** | Candidate throw consumes sequence counter | `IN_FLIGHT` | Drainer | UNALLOCATED | Discard counter; zero collision | Reuse sequence ID |
| **38** | All 5,000 retired pins protected at capacity | `SETTLED` | Coordinator | RESOLVED | Throw `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE` | Evict protected pin |
| **39** | All 5,000 retired leases protected at capacity | `SETTLED` | Coordinator | RESOLVED | Throw `RETIRED_LEASES_CAPACITY_EXHAUSTED_BACKPRESSURE` | Evict protected lease |
| **40** | Replay key missing for retired pin on eviction | `SETTLED` | Coordinator | RESOLVED | Throw `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED` | Silently delete pin |
| **41** | Duplicate pin release with new Uint8Array copy | `SETTLED` | Coordinator | RESOLVED | Compare byte-for-byte; return `ALREADY_RELEASED` | Reject due to `!==` |
| **42** | Duplicate pin release with mutated digest | `SETTLED` | Coordinator | RESOLVED | Throw `SETTLEMENT_EVIDENCE_DIGEST_MISMATCH` | Return `ALREADY_RELEASED` |
| **43** | Candidate root retired pins !== consumed evidence | `ANY` | Coordinator | ANY | Throw `POST_PUBLICATION_COLLECTION_SIZE_DESYNCHRONIZATION` | Commit desynchronized root |
| **44** | Candidate root retired pin missing replay key | `ANY` | Coordinator | ANY | Throw `POST_PUBLICATION_REPLAY_KEY_MISSING` | Commit unmapped replay key |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **The Universal Generation-Scoped Settlement-Barrier Invariant:**
   $$\mathbf{BARRIER\_RESOLVED}(g) \implies \mathbf{EXECUTION\_SETTLED}(g) \land \mathbf{CUSTODY\_RECONCILED}(g) \land \mathbf{DRAINER\_RELEASED}(g) \land \mathbf{ALL\_RECEIPTS\_MATCH}(g)$$
2. **Capability-Gated Settlement-Pin Release Invariant:**
   $$\mathbf{RELEASE\_SETTLEMENT\_PIN}(pinId) \implies \mathbf{AssertAuthoritativeSettlementEvidence}(evidence) \land (\mathbf{pin.identity} \equiv \mathbf{evidence.identity})$$
3. **Compound Durable Boot-Fenced Resource Allocation Invariant:**
   Every settlement pin and retry lease binds a compound durable fencing token $\mathbf{coordinatorBootEpoch} = \langle \mathbf{clusterTerm},\; \mathbf{bootGeneration},\; \mathbf{bootInstanceId} \rangle$. Monotonic non-reusability across restarts is mathematically guaranteed by consensus-allocated $\mathbf{bootGeneration}$, split-brain coordinators are fenced out by $\mathbf{clusterTerm}$, and instance uniqueness is guaranteed by $\mathbf{bootInstanceId}$. Evicted identifiers can never be re-issued.
4. **Strict Pre-Existing Overflow Fencing, Eligible Tombstone Eviction & Backpressure Invariant:**
   Pre-existing collection sizes $> 5000$ in `retiredSettlementPins`, `retiredRetryLeases`, or `consumedSettlementEvidence` fail closed immediately with dedicated corruption errors. At exactly 5,000 entries, eviction selects the oldest *eligible* entry not retained by active retry leases, active settlement pins, or pending settlement obligations; if all 5,000 entries are protected, it fails closed with `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE`.
5. **Lockstep Atomic Replay Retention & Post-Publication Invariant Verification:**
   `consumedSettlementEvidence` and `retiredSettlementPins` are strictly bounded to `MAX_RETIRED_RESOURCES = 5000` entries and trimmed in lockstep. Before any candidate root is published to `this.#custodyRoot`, `_validatePostPublicationInvariants()` verifies that the evicted record's replay key was removed, all retired pins have exact corresponding replay keys, and collections are strictly $\le 5000$.
6. **Complete Retired-Resource Identity Invariant:**
   A resource absent from active collections is recognized as `ALREADY_RELEASED` if and only if its identity matches an authoritative entry in `retiredRetryLeases` or `retiredSettlementPins` across all seven dimensions (`consumerId`, `clientRequestId`, `callerToken`, `transactionId`, `operationIdentity`, `barrierIdentity`). Altered identities fail closed.
7. **Universal Evidence Provenance, Disposition & Byte-for-Byte Digest Invariant:**
   Both active and retired settlement-pin releases unconditionally require validated `SettlementEvidenceFactory` provenance. A retired pin release verifies that `settlementEvidence.evidenceDigest` matches `retiredPin.evidenceDigest` byte-for-byte via constant-time bitwise comparison `areEqualByteArrays()`.
8. **Atomic Evidence Consumption Invariant:**
   Evidence consumption disposition (`consumedSettlementEvidence`) and settlement pin removal must linearize together via single atomic candidate-root replacement. If publication throws, evidence remains unconsumed and retryable.
9. **Seven-Dimensional Pin Identity Binding Invariant:**
   Settlement pins bind and enforce exact equality across generation, complete owner token, transaction ID, operation identity, barrier identity, and drainer ownership generation.
10. **Mandatory Stable Retry Request Identity Invariant:**
   All retry requests must supply non-empty, string-typed `(consumerId, clientRequestId)` bounded to $\le 256$ characters. The coordinator cannot substitute timestamps or nonces.
11. **Option A Audit-History Invariant:**
   `generationAuditHistory` maintains lifecycle-discriminated records (`PENDING_PUBLICATION`, `TERMINAL_SETTLEMENT`, `COMPACT_TOMBSTONE`) such that $\mathbf{generationAuditHistory.keys()} \equiv \{\mathbf{auditTombstoneFloor} + 1, \dots, \mathbf{publishedGeneration}\}$ holds unconditionally.
12. **Complete Tombstone Retirement Protection Invariant:**
   Oldest tombstone cannot be retired while any active retry lease, settlement pin, or pending obligation references it.
13. **Strict Capacity Ceiling Invariant:**
   `MAX_AUDIT_TOMBSTONES = 5000` is an authoritative ceiling; pre-existing violations fail closed; retirement operates strictly FIFO.
14. **Monotonic Safe-Integer Floor Invariant:**
   $$0 \le \mathbf{auditTombstoneFloor} \le \mathbf{custodyGenerationFloor} \le \mathbf{publishedGeneration}$$
15. **Cross-Resource Identifier Distinction Invariant:**
   `releaseRetryLease` rejects settlement pins with `LEASE_TYPE_MISMATCH`. `releaseSettlementPin` rejects retry leases with `PIN_TYPE_MISMATCH`.
16. **Watchdog Pin and Lease Immunity Invariant:**
   Watchdog timeouts CANNOT release retention pins or retry leases whose underlying operations remain unresolved.
17. **Single In-Flight Drain Invariant:** A second drain **CANNOT** dequeue an operation while `this.#custodyRoot.inFlightRecord !== null` ($N_{\text{in-flight}} \le 1$).
18. **Encapsulated Immutable Custody & Defensive Byte Immutability Invariant:**
   Published collections and evidence references are encapsulated within `ImmutableMap` and `ImmutableSet` wrappers whose state is private and whose mutating operations produce new instances. All byte arrays (`Uint8Array`) are defensively cloned and compared bitwise.
19. **Complete Structured Owner-Token Identity Invariant:**
   Every active and retired resource binds and verifies the complete structured `CustodyOwnershipToken` (`recoveryAttemptId`, `custodyGeneration`, `drainerOwnershipGeneration`, `transactionId`, `operationIdentity`, `barrierIdentity`, `leaseTerm`, `ownerSignature`) using `isEquivalentOwnerToken()`.
20. **Post-Publication Cross-Collection Lockstep Invariant:**
   The authoritative custody root must satisfy $\mathbf{retiredSettlementPins.size} \equiv \mathbf{consumedSettlementEvidence.size} \le 5000$ at every publication linearization point.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate \& Pre-Allocate} \longrightarrow \text{4. Atomic Root Replacement} \longrightarrow \text{5. } \mathbf{activeBarrier(g).resolveWithEvidence(evidence)} \longrightarrow \text{6. Release Settlement Pin}$$

---
## 17. Removal of Unapproved Emergency Reset Authority

The proposal excises all references to `forceEmergencyReset(capability)`:
- Ordinary recovery APIs cannot bypass `CUSTODY_RECONCILIATION_BLOCKED`.
- No automated root replacement is permitted while custody remains unresolved.
- Emergency data abandonment, if ever required, needs a separate architectural decision defining operator authorization, auditability, late-settling Promise behavior, and data-loss semantics.
- That separate decision is outside the scope of R18-R23 and is **not authorized** by this proposal.

---

## 18. Attempt Overlap Resolution After Watchdog Timeout

1. **Admitted Under New Monotonic Token:** Attempt 2 is admitted from `DEGRADED` when gate status is `SETTLEMENT_PENDING`, incrementing `currentAttemptId = 2`.
2. **Permanent Invalidation of Attempt 1 First Publication:** Attempt 1 is superseded (`1 !== currentAttemptId (2)`). Its continuation is completely fenced from first publication or root replacement.
3. **Step 4 Serialized Barrier Wait:** Attempt 2 enters `RECOVERING`, advances to Step 4, and halts at `await drainer.drainSettledBarrier()`. Candidate root staging, archive pre-reservation, and root replacement are strictly prohibited while in Step 4.
4. **Resolution, Capability-Gated Pin Release & Exclusive Resumption:** When Attempt 1's destination Promise finishes and Level 3 custody reconciliation completes, Attempt 1's obligation is settled. The settlement barrier resolves with verified `AuthoritativeSettlementEvidence`. This evidence authorizes `releaseSettlementPin`, freeing generation $g$ for eventual FIFO eviction. Attempt 2 unblocks from Step 4 with clean custody and proceeds to Step 5.

---

## 19. Deterministic Failure Scenarios and Future Test Requirements

The following forty-four deterministic future test specifications are formally codified for Sub-Milestone M5.1:

### Test R18-R24-T01: Retry Acquisition Without Stable Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext === undefined`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `INVALID_RETRY_REQUEST_CONTEXT`.

### Test R18-R24-T02: Retry Acquisition With Malformed or Empty Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext = { consumerId: "   ", clientRequestId: "" }`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `MALFORMED_RETRY_REQUEST_CONTEXT`.

### Test R18-R24-T03: Retry Acquisition With Oversized Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` with `consumerId` of length 257 characters.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `RETRY_REQUEST_CONTEXT_OVERSIZED`.

### Test R18-R24-T04: Lost-Response Retransmission Recovers Original Lease Idempotently
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 acquires lease for $g=1$ with `clientRequestId = "req-101"`; receives `leaseId_A`.
  2. Consumer 1 retransmits identical request with matching `(consumerId, clientRequestId)`.
- **Expected Final Root:** `activeRetryLeases.size === 1`, containing `leaseId_A`.
- **Expected Result / Error:** Second call returns existing `leaseId_A` idempotently.

### Test R18-R24-T05: Repeated Retry Acquisition With Conflicting Operation Identity
- **Initial Authoritative Root:** Settled result at $g=1$; lease active for `(consumer1, req-101)`.
- **Operation Sequence:** Consumer 1 submits request with `clientRequestId = "req-101"` but altered `operationIdentity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.

### Test R18-R24-T06: Two Distinct Consumers Acquire Independent Leases Under Same Recovery Owner Token
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 requests lease for $g=1$ with `consumerId = "c1"`.
  2. Consumer 2 requests lease for $g=1$ with `consumerId = "c2"`.
- **Expected Final Root:** `activeRetryLeases.size === 2`, containing `leaseId_1` and `leaseId_2`.
- **Expected Result / Error:** Both acquire distinct leases successfully.

### Test R18-R24-T07: Consumer 1 Attempting to Release Consumer 2's Lease Fails Closed
- **Initial Authoritative Root:** Active leases for Consumer 1 (`leaseId_1`) and Consumer 2 (`leaseId_2`).
- **Operation Sequence:** Consumer 1 invokes `releaseRetryLease` passing `leaseId_2` with `consumerId = "c1"`.
- **Expected Final Root:** Unchanged; both leases remain active.
- **Expected Result / Error:** Throws `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`.

### Test R18-R24-T08: Duplicate Lease Release by Authorized Consumer Against Retired Record Is Idempotent
- **Initial Authoritative Root:** Active lease `leaseId_1` for Consumer 1.
- **Operation Sequence:**
  1. Consumer 1 calls `releaseRetryLease` with all matching parameters; returns `{ status: 'RELEASED' }`.
  2. Consumer 1 calls `releaseRetryLease` again with identical parameters.
- **Expected Final Root:** `activeRetryLeases.has(leaseId_1) === false`; `retiredRetryLeases.has(leaseId_1) === true`.
- **Expected Result / Error:** Second call matches retired record and returns `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R24-T09: First Publication With Option A Lifecycle Model
- **Initial Authoritative Root:** `publishedGeneration = 0`, `auditTombstoneFloor = 0`, `custodyGenerationFloor = 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` for $g=1$.
- **Expected Final Root:**
  - `publishedGeneration === 1`.
  - `publicationResults.has(1) === true`.
  - `pendingSettlementObligations.has(1) === true`.
  - `generationAuditHistory.get(1).kind === 'PENDING_PUBLICATION'`.
- **Expected Result / Error:** Success; contiguity invariant holds unconditionally.

### Test R18-R24-T10: Pending-to-Terminal Audit History Transition
- **Initial Authoritative Root:** Generation $g=1$ published; audit record is `PENDING_PUBLICATION`.
- **Operation Sequence:** Barrier for $g=1$ settles with `AuthoritativeSettlementEvidence` (`CONFIRMED_COMMITTED`).
- **Expected Final Root:** `generationAuditHistory.get(1).kind === 'TERMINAL_SETTLEMENT'`.
- **Expected Result / Error:** Success; record upgraded in place.

### Test R18-R24-T11: Eviction and Compact Tombstone Conversion
- **Initial Authoritative Root:** Result at $g=1$ settled (`TERMINAL_SETTLEMENT`); 1,000 results retained.
- **Operation Sequence:** Publish $g=1001$; triggers FIFO eviction of $g=1$.
- **Expected Final Root:**
  - `publicationResults.has(1) === false`.
  - `custodyGenerationFloor === 1`.
  - `generationAuditHistory.get(1).kind === 'COMPACT_TOMBSTONE'`.
  - `generationAuditHistory.get(1).evidenceDigest` is 32-byte SHA-256.
- **Expected Result / Error:** Success.

### Test R18-R24-T12: Tombstone Retirement Blocked by Active Settlement Pin
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active settlement pin.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R24-T13: Tombstone Retirement Blocked by Active Retry Lease
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active retry lease.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R24-T14: Tombstone Retirement Blocked by Pending Settlement Obligation
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ references pending obligation.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R24-T15: Normal FIFO Tombstone Retirement at 5,000 Capacity
- **Initial Authoritative Root:** 5,000 tombstones retained ($[1 \dots 5000]$); zero pins/leases on $g=1$.
- **Operation Sequence:** Publish new generation causing eviction and tombstone retirement.
- **Expected Final Root:**
  - Tombstone $g=1$ deleted from `generationAuditHistory`.
  - `auditTombstoneFloor === 1`.
  - Total tombstones remaining: 5,000 ($[2 \dots 5001]$).
- **Expected Result / Error:** Success; contiguity preserved over $[2 \dots publishedGeneration]$.

### Test R18-R24-T16: Pre-Existing Tombstone Count Above Cap Fails Closed
- **Initial Authoritative Root:** Artificially injected root with 5,005 tombstones.
- **Operation Sequence:** Call `publishReleaseAndObligation`.
- **Expected Final Root:** Unchanged; zero bulk deletions performed.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_CORRUPTED`.

### Test R18-R24-T17: Missing Tombstone Inside Promised Horizon Fails Closed
- **Initial Authoritative Root:** `auditTombstoneFloor = 10`, `custodyGenerationFloor = 20`; tombstone $g=15$ deleted.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_KEY_GAP`.

### Test R18-R24-T18: Expiry Below Tombstone Floor Returns Historical Expiry
- **Initial Authoritative Root:** `auditTombstoneFloor = 50`.
- **Operation Sequence:** Query generation $g=30$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.

### Test R18-R24-T19: Floor Order Invariant Violation Fails Closed
- **Initial Authoritative Root:** Artificially set `auditTombstoneFloor = 50`, `custodyGenerationFloor = 40`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_FLOOR_ORDER_VIOLATION`.

### Test R18-R24-T20: Audit Record With Mismatched Generation Fails Closed
- **Initial Authoritative Root:** Audit history key is 5, but record payload has `generation: 4`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `AUDIT_RECORD_GENERATION_MISMATCH`.

### Test R18-R24-T21: Audit Record With Mismatched Owner Token Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `membershipEpoch`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R24-T22: Audit Record With Mismatched Drainer Ownership Generation Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `drainerOwnershipGeneration`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R24-T23: Cross-Generation Settlement Evidence Replay Rejected
- **Initial Authoritative Root:** Active pin for $g=2$; evidence constructed for $g=1$.
- **Operation Sequence:** Call `releaseSettlementPin("pin:...:g2:...", evidence_g1)`.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R24-T24: Cross-Obligation Settlement Evidence Replay Rejected via Atomic In-Root Registry
- **Initial Authoritative Root:** Evidence for $g=1$ already consumed on `pin_1`; `consumedSettlementEvidence.has("1:barrier:pin_1") === true`.
- **Operation Sequence:** Attempt to reuse identical evidence on `pin_2`.
- **Expected Final Root:** Unchanged; `pin_2` remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

### Test R18-R24-T25: Caller-Constructed Mock Evidence Rejected by Provenance Check
- **Initial Authoritative Root:** Active pin for $g=1$.
- **Operation Sequence:** Caller constructs object matching `AuthoritativeSettlementEvidence` schema without factory.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R24-T26: Watchdog Timeout Cannot Release Settlement Pin
- **Initial Authoritative Root:** Barrier pending for $g=1$; watchdog fires after 5,000 ms.
- **Operation Sequence:** Watchdog handler attempts to call `releaseSettlementPin` with watchdog receipt.
- **Expected Final Root:** Pin remains active; gate state latches `DEGRADED`.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R24-T27: Retry Lease ID Passed to Settlement Pin Release Fails Closed
- **Initial Authoritative Root:** Active lease `lease:1:1:c1:1`.
- **Operation Sequence:** Call `releaseSettlementPin("lease:1:1:c1:1", evidence)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `PIN_TYPE_MISMATCH`.

### Test R18-R24-T28: Settlement Pin ID Passed to Retry Lease Release Fails Closed
- **Initial Authoritative Root:** Active pin `pin:1:1:b1:1`.
- **Operation Sequence:** Call `releaseRetryLease({ leaseId: "pin:1:1:b1:1", ... })`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TYPE_MISMATCH`.

### Test R18-R24-T29: Genuinely Unknown Identifier Passed to Release APIs Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Clean root; active and retired maps empty.
- **Operation Sequence:**
  1. Call `releaseRetryLease` with never-issued `leaseId = "lease:999:unknown"`.
  2. Call `releaseSettlementPin` with never-issued `pinId = "pin:999:unknown"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Both calls fail closed with `RESOURCE_NOT_FOUND` (neither returns `ALREADY_RELEASED`).

### Test R18-R24-T30: Duplicate Release Against Retired Pin With Matching Provenance and Digest Returns Already Released
- **Initial Authoritative Root:** Active pin `pin_1` for $g=1$.
- **Operation Sequence:**
  1. Release `pin_1` with valid evidence; returns `{ status: 'RELEASED' }`.
  2. Repeat `releaseSettlementPin("pin_1", validEvidence)`.
- **Expected Final Root:** `activeSettlementPins.has("pin_1") === false`; `retiredSettlementPins.has("pin_1") === true`.
- **Expected Result / Error:** Second call validates provenance and digest, returning `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R24-T31: Settlement Pin Release With Mismatched Drainer Generation Fails Closed (Blocker C1)
- **Initial Authoritative Root:** Active pin for $g=1$ with `drainerOwnershipGeneration = 2`.
- **Operation Sequence:** Caller presents `AuthoritativeSettlementEvidence` with `drainerOwnershipGeneration = 1`.
- **Expected Final Root:** Unchanged; pin remains active; evidence remains unconsumed.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R24-T32: Pre-Publication Throw During Pin Release Leaves Evidence Usable for Retry (Blocker C2)
- **Initial Authoritative Root:** Active pin for $g=1$; `consumedSettlementEvidence.size === 0`.
- **Operation Sequence:**
  1. Initiate `releaseSettlementPin`; candidate root construction simulates memory allocation exception before pointer swap.
  2. Retry `releaseSettlementPin` with the exact same evidence.
- **Expected Final Root:** Second call succeeds; `pin_1` retired; evidence replay key added to `consumedSettlementEvidence`.
- **Expected Result / Error:** First call throws error without consuming evidence in authoritative root; second call completes release cleanly.

### Test R18-R24-T33: Duplicate Release on Retired Lease With Altered Transaction ID Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Retired lease `lease_1` recorded for `transactionId = "tx-100"`.
- **Operation Sequence:** Call `releaseRetryLease` passing `lease_1` with `transactionId = "tx-200"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TRANSACTION_ID_MISMATCH`.

### Test R18-R24-T34: Duplicate Release on Retired Pin With Unverified Provenance Fails Closed (Blocker C2)
- **Initial Authoritative Root:** Retired pin `pin_1` recorded for $g=1$.
- **Operation Sequence:** Caller constructs mock evidence object with matching fields but without factory issuance.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R24-T35: Pre-Existing Retired Pins Overflow Rejection (Reviewer Issue 2)
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredSettlementPins`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_PINS_CAPACITY_CORRUPTED`.

### Test R18-R24-T36: Pre-Existing Retired Leases Overflow Rejection (Reviewer Issue 2)
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredRetryLeases`.
- **Operation Sequence:** Call `releaseRetryLease` for active lease `lease_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_LEASES_CAPACITY_CORRUPTED`.

### Test R18-R24-T37: Pre-Existing Consumed Evidence Overflow Rejection (Reviewer Issue 2)
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted.
- **Expected Result / Error:** Throws `CONSUMED_EVIDENCE_CAPACITY_CORRUPTED`.

### Test R18-R24-T38: Cross-Restart Delayed Release Rejection via Compound Durable Boot Epoch (Reviewer Issue 1)
- **Initial Authoritative Root:** Restarted coordinator with `coordinatorBootEpoch = { clusterTerm: 2, bootGeneration: 5, bootInstanceId: "uuid-new" }`.
- **Operation Sequence:** Caller submits delayed release for `pinId = "pin:1.4.uuid-old:1:barrier:100"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `RESOURCE_NOT_FOUND` (cannot collide with newly issued pins under `bootGeneration: 5`).

### Test R18-R24-T39: Allocation Interruption Discards Counter Without Collision
- **Initial Authoritative Root:** `_pinSequenceCounter = 10`.
- **Operation Sequence:**
  1. Call `publishReleaseAndObligation`; sequence increments to 11; candidate allocation throws out-of-memory.
  2. Repeat `publishReleaseAndObligation`; sequence increments to 12; publication succeeds.
- **Expected Final Root:** Active pin published with `seq = 12`; zero active records for `seq = 11`.
- **Expected Result / Error:** Second call succeeds cleanly; discarded sequence 11 causes zero collisions.

### Test R18-R24-T40: Normal Exactly-At-Capacity Lockstep Retirement at 5,000 Entries
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`, `consumedSettlementEvidence.size === 5000`. Oldest retired pin has zero active references.
- **Operation Sequence:** Release active pin `pin_new` with valid evidence.
- **Expected Final Root:**
  - Oldest retired pin deleted.
  - Oldest replay key deleted from `consumedSettlementEvidence`.
  - `retiredSettlementPins.size === 5000`.
  - `consumedSettlementEvidence.size === 5000`.
- **Expected Result / Error:** Release succeeds cleanly with exact capacity preservation and post-publication invariant validation.

### Test R18-R24-T41: All-Protected Retired Pins Exhaustion Triggers Fail-Closed Backpressure (Reviewer Issue 2A)
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. Every single entry in `retiredSettlementPins` is retained by an active retry lease or active pin.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero pins evicted; zero state mutations committed.
- **Expected Result / Error:** Throws `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE`.

### Test R18-R24-T42: Replay Key Desynchronization on Eviction Fails Closed (Reviewer Issue 2B)
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. The replay key corresponding to the oldest eligible retired pin is artificially absent from `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero mutations committed.
- **Expected Result / Error:** Throws `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED`.

### Test R18-R24-T43: Reference-Independent Byte-for-Byte Digest Match Returns Already Released (Reviewer Issue 4)
- **Initial Authoritative Root:** Pin `pin_retired` is in `retiredSettlementPins` with recorded digest `[0x01, 0x02, ..., 0x20]`.
- **Operation Sequence:** Caller invokes `releaseSettlementPin` for `pin_retired` with independently allocated `Uint8Array` containing identical bytes (`digestA !== digestB` by reference, but byte-equal).
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `{ pinId: 'pin_retired', status: 'ALREADY_RELEASED' }`.

### Test R18-R24-T44: Post-Publication Invariant Rejection on Desynchronized Replay State (Reviewer Issue 5 & 6)
- **Initial Authoritative Root:** Candidate root construction artificially desynchronizes collection sizes (`retiredSettlementPins.size = 5000`, `consumedSettlementEvidence.size = 5001`).
- **Operation Sequence:** Candidate root passed to `_validatePostPublicationInvariants()`.
- **Expected Final Root:** Pointer swap rejected; `this.#custodyRoot` remains unchanged.
- **Expected Result / Error:** Throws `POST_PUBLICATION_COLLECTION_SIZE_DESYNCHRONIZATION`.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R24):
================================================================================
1. [COMPOUND DURABLE BOOT EPOCH & FENCING]: Does the Engineering Authority approve
   binding a compound durable fencing token coordinatorBootEpoch = <clusterTerm,
   bootGeneration, bootInstanceId> into all allocated settlement pins and retry
   leases, guaranteeing mathematical non-reusability across restarts via consensus
   bootGeneration and split-brain coordinator fencing via clusterTerm?

2. [GENUINE COLLECTION ENCAPSULATION & DEEP IMMUTABILITY]: Does the Engineering
   Authority approve encapsulating all custody root collections within ImmutableMap
   and ImmutableSet classes with private backing stores and non-mutating copy-on-write
   semantics, eliminating naive Object.freeze() on mutable collections?

3. [CONSTANT-TIME BYTE DIGEST EQUALITY & STRUCTURED OWNER IDENTITY]: Does the
   Engineering Authority approve constant-time bitwise byte comparison (areEqualByteArrays)
   with mandatory defensive cloning for evidence digests, and verifying all 8 fields
   of CustodyOwnershipToken across active and retired release paths?

4. [ELIGIBLE TOMBSTONE EVICTION & BACKPRESSURE]: Does the Engineering Authority
   approve searching deterministically for the oldest eligible unpinned tombstone,
   and failing closed with RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE if all 5,000
   entries are protected by active leases, pins, or obligations?

5. [POST-PUBLICATION CROSS-COLLECTION INVARIANT VERIFICATION]: Does the Engineering
   Authority approve validating that every retired pin has its exact replay key,
   verifying collections are <= 5,000, and confirming exact lockstep size equality
   prior to the atomic pointer replacement?

6. [CLOSURE OF ALL CORRECTNESS GAPS]: Does the Engineering Authority agree that all
   six review issues are fully resolved, and that the recovery arbitration
   architecture is ready for holistic M5.0 final acceptance review?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R24 ARCHITECTURAL DECISION PROPOSAL VERDICT:
DECISION PROPOSAL COMPLETE — READY FOR HOLISTIC M5.0 FINAL ACCEPTANCE REVIEW
================================================================================
```

This R18-R24 proposal completes the formal recovery arbitration architecture. It establishes compound durable boot fencing, restores deep collection encapsulation via `ImmutableMap` and `ImmutableSet`, enforces constant-time bitwise byte digest comparison and structured owner token identity, guarantees deterministic eligible tombstone eviction with backpressure, verifies post-publication cross-collection invariants, and codifies forty-four deterministic future test specifications.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R24 is an architectural documentation correction only. No production implementation has been started. M5.1, including M5.1a, remains strictly unauthorized pending explicit Engineering Authority approval."
