# Phase 7.5 M5.0-R18-R33 — Static Custody API Immutability & Full-Identity Linearizable Reconciliation Fencing
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `2dfc2d1a13aea47dc942fddff2170c580e640023` (M5.0-R18-R32)
**Direct Parent Commit:** `2dfc2d1a13aea47dc942fddff2170c580e640023`
**Authorization Scope:** M5.0-R18-R33 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R33 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R32, we introduced direct private-field access (`source.#bytes`, `other.#canonicalHex`) within `EncapsulatedByteArray`, frozen class prototype, fail-closed `SharedArrayBuffer` brand checking, Proxy-safe trusted-instance verification, verified property-name regular expressions, and deterministic sequence conflict precedence.

The independent architectural review of R32 conditionally accepted these improvements while identifying two concrete correctness blockers:
1. **Blocker A (Static Custody API Immutability):** In R32, while `Object.freeze(EncapsulatedByteArray.prototype)` protected instance methods, the class constructor itself remained unfrozen. A consumer with access to the exported class could replace `EncapsulatedByteArray.isTrustedInstance = () => true;`. Furthermore, the constructor called this public static method. The custody boundary must not depend on mutable public static functions.
2. **Blocker B (Linearizable Reconciliation Fencing & Full Publication Identity Binding):** In R32, terminal tombstones were bound only to $\langle \text{publicationSequence}, \text{attemptId} \rangle$ and lacked formal specification of the shared linearizable ordering domain between the publication commit path and the reconciliation fencing path. Without a shared consensus log or single-partition transactional compare-and-set linearization boundary, an in-flight publication request might commit after a reconciliation query observed absence, causing a committed record to be misclassified as uncommitted.

Sub-Milestone M5.0-R18-R33 resolves both blockers definitively:

- **Objective R33-1 (Lexical Private Brand Helper & Completely Frozen Static API - Blocker A):**
  1. Define an unexported module-scoped lexical brand-check function `isLexicallyTrustedEncapsulatedByteArray(val)` initialized via an ECMAScript 2022 `static {}` initialization block inside `EncapsulatedByteArray`. It directly verifies `#bytes in val` and exact prototype identity. Because it is held in module scope, it cannot be accessed, monkey-patched, or replaced by external code.
  2. Perform all internal custody decisions (in constructor, `deepCloneAndFreezeValue()`, and `.equals()`) exclusively via `isLexicallyTrustedEncapsulatedByteArray(val)`.
  3. Explicitly freeze both the prototype and the class constructor itself:
     `Object.freeze(EncapsulatedByteArray.prototype);`
     `Object.freeze(EncapsulatedByteArray);`
- **Objective R33-2 (Full 8-Dimensional Publication Identity Tombstone Binding - Blocker B):**
  Bind terminal fencing tombstones to the complete, immutable 8-dimensional tuple:
  $$\mathbf{publicationIdentity} = \langle \mathbf{clusterTerm}, \mathbf{bootGeneration}, \mathbf{bootInstanceId}, \mathbf{publicationSequence}, \mathbf{attemptId}, \mathbf{candidateRootDigest}, \mathbf{expectedPriorSequence}, \mathbf{expectedPriorRootDigest} \rangle$$
- **Objective R33-3 (Shared Linearizable Store Ordering Boundary - Blocker B):**
  Formalize that the publication commit path and the reconciliation fencing path serialize through the identical authoritative store ordering domain (single consensus log index or atomic compare-and-set transaction):
  1. Publication commit and reconciliation fencing cannot both succeed for the same attempt.
  2. If publication commits first, reconciliation queries the linearizable log and returns `RECONCILED_COMMITTED`.
  3. If reconciliation fencing executes first, the store atomically commits `TERMINAL_ATTEMPT_FENCED` bound to the complete 8D tuple. Every delayed publication for that exact attempt is permanently rejected with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION`.
  4. If neither outcome can be established (e.g. partition or write failure), reconciliation returns `UNKNOWN_IN_PROGRESS`, keeping the coordinator fail-closed in `RECONCILING_CUSTODY`.
- **Objective R33-4 (Documentation of Fail-Closed SharedArrayBuffer Capability Requirement):** Document the compatibility consequence of fail-closed `isSharedArrayBufferBacking`: environments lacking `%SharedArrayBuffer%` intrinsics deliberately prohibit binary ingestion fail-closed.
- **Objective R33-5 (Deterministic Conflict Precedence Enforcement):** Codify that sequence collision under differing `attemptId` (`DUPLICATE_SEQUENCE_CONFLICT`) takes absolute precedence over candidate digest mismatch (`CONFLICTING_DIGEST_AT_SEQUENCE`).
- **Objective R33-6 (Expansion to 138 Deterministic Cases & Test Specifications):** Expand the failure matrix to 138 cases (Cases 1–138) and codify 138 future test specifications (`Test R18-R33-T01` to `Test R18-R33-T138`), adding eight targeted specifications (T131–T138) for static API immutability, reconciliation races, lost-response recovery, and fencing boundaries.

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

### 8.2 Safe Tombstone Eviction, Compound Boot Epoch Fencing & Authoritative Boundary Fencing

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
- **Cluster Term ($\mathbf{clusterTerm}$):** Monotonically increasing consensus term (e.g. Raft term) under which recovery leadership was granted.
- **Boot Generation ($\mathbf{bootGeneration}$):** Monotonically increasing sequence number persisted and fsynced to Write-Ahead Log (WAL) or consensus state before coordinator traffic admission. Guarantees non-reusability across process restarts on the same or migrating node.
- **Boot Instance ID ($\mathbf{bootInstanceId}$):** Cryptographically secure, collision-resistant UUIDv4 uniquely identifying the in-memory process execution lifetime.

#### The Authoritative Store as the Distributed Fencing Boundary:
In distributed recovery, a process-local check or in-memory lock cannot serve as the distributed fencing authority. The architecture defines the Authoritative Custody Store (`IAuthoritativeCustodyStore`) as the final fencing boundary:

1. **The Complete 8-Dimensional Authoritative Publication Identity Tuple (R30-3):**
   Every publication request and confirmation is bound to an immutable 8-dimensional identity:
   $$\mathbf{publicationIdentity} = \langle \mathbf{clusterTerm},\; \mathbf{bootGeneration},\; \mathbf{bootInstanceId},\; \mathbf{publicationSequence},\; \mathbf{attemptId},\; \mathbf{candidateRootDigest},\; \mathbf{expectedPriorSequence},\; \mathbf{expectedPriorRootDigest} \rangle$$
   This binds process lifetime, leadership consensus epoch, monotonic publication sequence, local attempt identity, candidate payload content, and expected linearizable prior version into a single unambiguous tuple.

2. **Authoritative Atomic Validation Rules:**
   The authoritative store atomically evaluates the fencing token and attempt context against cluster consensus state before committing candidate records:
   - **Stale Term Rejection:** The store checks $\mathbf{fencingToken.clusterTerm} \ge \mathbf{store.highestCommittedTerm}$. If the token's term is strictly lower, the mutation is rejected with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
   - **Boot Identity Validation:**
     - If $\mathbf{fencingToken.clusterTerm} == \mathbf{store.highestCommittedTerm}$, the store verifies that $\mathbf{bootGeneration} == \mathbf{store.activeBootGeneration}$ and $\mathbf{bootInstanceId} == \mathbf{store.activeBootInstanceId}$. Any discrepancy indicates a superseded or partitioned coordinator instance and is rejected with `SUPERSEDED_COORDINATOR_INSTANCE_FENCED`.
     - If $\mathbf{fencingToken.clusterTerm} > \mathbf{store.highestCommittedTerm}$, the store registers the newly elected leader epoch: it updates `highestCommittedTerm = token.clusterTerm`, `activeBootGeneration = token.bootGeneration`, `activeBootInstanceId = token.bootInstanceId`, and initializes `expectedPublicationSequence = 1`.
   - **Publication Sequence Contiguity & Version Verification:** Within an established term and instance, the store verifies that $\mathbf{fencingToken.publicationSequence} == \mathbf{store.expectedPublicationSequence}$ (strictly contiguous increment $1, 2, 3, \dots$) and matches $\mathbf{expectedPriorSequence}$ and $\mathbf{expectedPriorRootDigest}$. Sequence gaps, duplicates, or out-of-order writes are rejected with `OUT_OF_ORDER_PUBLICATION_SEQUENCE`. Upon successful commit, `store.expectedPublicationSequence` is incremented.

3. **Outcome Classification & Exact Confirmation Binding (R30-3):**
   The authoritative store classifies publication outcomes into three mutually exclusive categories:
   - `COMMITTED`: The fencing token was validated, and the candidate root was durably committed to consensus quorum storage. The store returns an `AuthoritativeCommittedConfirmation` strictly echoing all identity fields, including `attemptId`. The coordinator asserts that every returned field matches its active `attemptContext`. A delayed confirmation from a previous attempt cannot match a newer attempt.
   - `FENCED_REJECTED`: The fencing token was invalid, stale, superseded, out of sequence, or presented a digest mismatch. The candidate root is discarded, and the write is permanently rejected.
   - `UNCERTAIN`: The commit outcome is ambiguous due to network timeouts, transport partition, or cluster consensus unavailability before acknowledgement is received.

4. **Authoritative Store Idempotency, Conflict Precedence & Full-Identity Linearizable Reconciliation Fencing (R31-5, R31-6, R32-2, R32-6, R33-2, R33-3):**
   The store's publication and reconciliation contract (`reconcilePublication`) enforces six deterministic linearizable invariants serialized through a shared consensus log or single-partition transactional compare-and-set state machine:
   1. **Identical Query Idempotency (Rule 1):** If `reconcilePublication(query)` or a retransmitted publication request presents the identical $\mathbf{publicationIdentity}$ (same term, boot generation, instance ID, sequence $S$, attempt ID $\mathbf{attemptId}$, candidate root digest $D$, and expected prior sequence/digest), the store returns `COMMITTED` (or `RECONCILED_COMMITTED`) idempotently with the exact original committed root without re-execution.
   2. **Deterministic Sequence Conflict Precedence (Rule 2 & Rule 2b - R32-6):** When a publication request or reconciliation query presents an already-committed sequence $S$:
      - *Precedence Step 1 (Attempt Identity Check):* If the request presents a differing `attemptId` (regardless of whether `candidateRootDigest` matches or differs), the store permanently rejects the operation with `DUPLICATE_SEQUENCE_CONFLICT: sequence already committed under different attempt identity`. Attempt identity mismatch takes absolute precedence.
      - *Precedence Step 2 (Candidate Digest Check):* If `attemptId` matches the committed attempt but `candidateRootDigest` differs ($D' \neq D$), the store permanently rejects the operation with `CONFLICTING_DIGEST_AT_SEQUENCE`.
   3. **Strict Sequencing of Uncertainty (Rule 3):** An uncertain attempt must be reconciled before any new logical publication sequence can be allocated by the coordinator. The coordinator gate remains latched in `RECONCILING_CUSTODY`.
   4. **Strict Immutable Retry & Reconciliation Identity (Rule 4):** When a coordinator reconciles an `UNCERTAIN` publication attempt, it MUST reuse the exact immutable $\mathbf{publicationIdentity}$ tuple—including the identical `attemptId`—that was generated for the initial publication attempt.
   5. **Full-Identity Linearizable Reconciliation Fencing (Rule 5 - Blocker B / R33-2, R33-3):**
      The publication commit path and the reconciliation fencing path serialize through the identical linearizable ordering domain:
      - The store maintains two mutually exclusive state-machine sets for each sequence $S$: `committedPublications` and `terminalFencedAttempts`.
      - When `reconcilePublication(query)` executes for an uncommitted sequence (absence of committed record):
        - The store CANNOT treat mere absence as proof of non-commitment, because an in-flight publication request might still be processing.
        - The store atomically commits a durable terminal fencing tombstone (`TERMINAL_ATTEMPT_FENCED`) bound to the complete 8-dimensional tuple $\mathbf{publicationIdentity} = \langle \mathbf{clusterTerm}, \mathbf{bootGeneration}, \mathbf{bootInstanceId}, \mathbf{publicationSequence}, \mathbf{attemptId}, \mathbf{candidateRootDigest}, \mathbf{expectedPriorSequence}, \mathbf{expectedPriorRootDigest} \rangle$.
        - Any in-flight or delayed publication request evaluating after this tombstone is committed is guaranteed to be rejected with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION` and prohibited from committing.
        - If the atomic terminal tombstone is successfully persisted, the store returns `NOT_COMMITTED_FINAL` with the current authoritative root.
        - If the store cannot establish the linearizable outcome (e.g., quorum partition, consensus write timeout, or storage failure), it MUST return `UNKNOWN_IN_PROGRESS`. The coordinator receiving `UNKNOWN_IN_PROGRESS` CANNOT treat the publication as uncommitted; it remains fail-closed in `RECONCILING_CUSTODY` and retries reconciliation.
   6. **Reconciliation Candidate Digest Binding (Rule 6):** Store reconciliation results are strictly bound to the queried candidate digest; a match confirms commitment of that specific candidate, while a verified `NOT_COMMITTED_FINAL` tombstone proves that candidate was terminally aborted.

5. **Full Lock Lifecycle Across Leadership Loss & Invalidation (R31-5):**
   The coordinator-local `#publicationLock` manages local concurrency while strictly adhering to distributed leadership boundaries:
   - **Queue Purging on Step-Down:** When `leadershipFence.stepDown(newTerm)` is invoked (or leadership lease expires), all queued publication callers waiting in `#waitQueue` are immediately rejected with `COORDINATOR_STOPPED: coordinator stepped down` and purged. Queued work is NEVER retained or executed after step-down or under a subsequent term.
   - **In-Flight Store Persistence:** An asynchronous network request already dispatched to `publishFencedRoot` cannot be cancelled over the wire if network transit has begun. However, the local coordinator's post-await completion check (`assertPostAwaitEligibility`) detects leadership loss upon response arrival. If term advanced, lease expired, or step-down occurred, the response is discarded, local pointer replacement is strictly prohibited, and the coordinator throws `CUSTODY_PUBLICATION_STALE_LEADERSHIP`.
   - **Lock Release vs. Gate Latching on UNCERTAIN:** When an outcome is `UNCERTAIN`, the local `#publicationLock` is released in `finally` so the Node.js event loop does not deadlock. Crucially, releasing the local mutex does NOT open the custody gate! The gate remains fail-closed in `RECONCILING_CUSTODY`. Any subsequent mutation call immediately throws `CUSTODY_GATE_RECONCILING`.
   - **Term Segregation:** Each publication attempt is explicitly bound to the leadership term at the time of allocation (`attemptContext.localClusterTerm`). A newly elected coordinator must complete authoritative store synchronization (`reconcileAuthoritativeCustody`) before admitting any new publication attempts under the new term.

6. **Defined RECONCILING_CUSTODY Gate Semantics:**
   While in `RECONCILING_CUSTODY`, the coordinator enforces fail-closed isolation across all recovery operations:

| Operation | Gate Policy | Enforcement Behavior |
|---|---|---|
| **New Custody Publication** | **PROHIBITED** | Rejects with `CUSTODY_GATE_RECONCILING`; zero candidate roots admitted |
| **New Candidate-Root Creation** | **PROHIBITED** | Rejects with `CUSTODY_GATE_RECONCILING`; unverified state cannot be derived |
| **Recovery Installation** | **PROHIBITED** | Recovery pipeline halts until reconciliation resolves |
| **Authoritative Store Read** | **PERMITTED** | Exclusively via trusted store reconciliation path (`reconcilePublication`) |
| **Local Cached Root Read** | **UNAVAILABLE / STALE** | External mutation queries rejected; reads return `CUSTODY_RECONCILIATION_IN_PROGRESS` |
| **Leadership Lease Renewal** | **RESTRICTED** | Permitted to maintain consensus membership, but does NOT clear reconciliation gate |
| **Coordinator Restart** | **RECONCILE FIRST** | Must query authoritative store and restore committed root before opening gate |
| **Reconciliation I/O Failure** | **FAIL-CLOSED** | Gate remains latched in `RECONCILING_CUSTODY`; never clears on timeout or peer report |

#### Explicit Distributed Systems Model Assumptions:
The guarantees established by this architecture hold under the following explicitly qualified assumptions:
1. **Monotonic Clocks:** Local lease tracking uses `performance.now()`, which is strictly non-decreasing and immune to NTP step adjustments or leap seconds.
2. **Bounded Clock Drift Rate:** Maximum clock drift rate between cluster nodes is bounded by $\rho \le 10^{-4}$ (standard quartz oscillator drift $< 100\,\mu\text{s/s}$).
3. **Consensus Lease Non-Overlap Invariant:** The consensus cluster quorum guarantees that no candidate for Term $T+1$ can be granted a leadership lease until:
   $$t \ge t_{\text{term } T\text{ lease granted}} + \tau_{\text{lease}} + \epsilon_{\text{drift}}$$
   where $\tau_{\text{lease}}$ is the consensus lease duration (default 5,000ms) and $\epsilon_{\text{drift}}$ is the maximum drift allowance.
4. **Local Guard Margin:** The coordinator checks that $\text{monotonicNow} + \Delta_{\text{guard}} < \mathbf{leaseExpiresAtMonotonicMs}$, where $\Delta_{\text{guard}} = 1,000\,\text{ms}$, protecting against local execution jitter.
5. **Process Suspension Handling:** If a coordinator process is suspended (e.g. OS thread descheduling, VM migration, or GC pause) for longer than its remaining lease, any write attempt after resuming will be rejected either locally via elapsed monotonic time or by the Authoritative Custody Store CAS.

#### Adversarial Interleaving Scenario: Partitioned Stale Leader Race

The following concrete timeline illustrates how the authoritative store boundary and post-await completion check prevent a stale partitioned coordinator from committing a publication or updating local state after a newer term becomes authoritative:

| Time | Coordinator A (Term 1 Leader) | Cluster Consensus Quorum | Coordinator B (Term 2 Candidate) | Authoritative Custody Store | System Safety State |
|---|---|---|---|---|---|
| $t_0$ | Holds active lease for Term 1 until $t_0 + 5000\text{ms}$. | Quorum recognizes Coord A as leader for Term 1. | Follower in Term 1. | Highest committed term = 1; expected seq = 2. | Normal operation under Term 1. |
| $t_1$ | Network partition isolates Coord A. Dequeues op $g=2$; captures attempt context $\text{att}_1$. | Heartbeats from Coord A lost. | Awaiting election timeout. | Highest committed term = 1; expected seq = 2. | Partition active; Coord A preparing root. |
| $t_2$ | Enters OS process suspension / GC pause (duration 6,500ms). | Election timeout expires ($t_2 = t_0 + 5100\text{ms}$). Lease for Term 1 has definitively elapsed ($> 5000\text{ms}$). | Initiates election for Term 2. | Highest committed term = 1; expected seq = 2. | **Lease Non-Overlap Invariant holds:** Coord A lease expired before Term 2 granted. |
| $t_3$ | Still suspended. | Quorum elects Coord B leader for Term 2; grants lease valid $t_3 \to t_3 + 5000\text{ms}$. | Elected leader for Term 2. | Quorum establishes Term 2; highest committed term = 2; expected seq = 1. | Coord B is sole valid cluster leader. |
| $t_4$ | Suspension ends at $t_4 = t_0 + 6500\text{ms}$. Resumes execution and invokes `executeFencedPublication()`. | Active in Term 2 under Coord B. | Processing admissions in Term 2. | Highest committed term = 2; expected seq = 1. | Coord A attempts to publish stale candidate. |
| $t_5$ | **Local Guard Check:** `monotonicNow` ($t_4$) exceeds `#leaseExpiresAtMonotonicMs` ($t_0 + 5000\text{ms}$). Throws `STALE_COORDINATOR_LEADERSHIP_FENCED`. | Active in Term 2. | Active in Term 2. | Highest committed term = 2; expected seq = 1. | **Local Fencing:** Candidate discarded; `#custodyRoot` untouched. |
| $t_{5\text{alt}}$ | *(Alternative: local clock stalled)* Coord A submits CAS token $\langle 1, 1, \text{uuid-A}, 2 \rangle$ to Authoritative Store. | Quorum in Term 2. | Active in Term 2. | **Authoritative Store CAS:** Evaluates token Term 1 < Store Term 2. Rejects write with `STALE_COORDINATOR_LEADERSHIP_FENCED`. | **Authoritative Fencing:** Write rejected; store untouched; split-brain impossible. |
| $t_{5\text{post}}$ | *(Alternative: store committed before election, response delayed to $t_4$)* Coord A receives delayed `COMMITTED`. | Quorum in Term 2. | Active in Term 2. | Highest committed term = 2. | **Post-Await Completion Fence:** Coord A checks local lease; detects Term 1 expired. Discards response; withholds local `#custodyRoot` update; steps down. Coord B recovers authoritative root. |

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

### 12.9 The Restricted Custody-Value Universe, Encapsulated Binary Storage & Deep Immutability Contract

In ECMAScript environments, applying `Object.freeze()` to native `Map` or `Set` instances freezes only the object prototype and property descriptors; it does **not** intercept internal slots, meaning native prototype methods such as `Map.prototype.set.call(frozenMap, key, value)`, `.delete()`, and `.clear()` continue to mutate the collection contents in place. Furthermore, applying `Object.freeze()` to a parent object freezes only its own shallow properties; any nested object references remain fully mutable through returned references unless recursively cloned and frozen.

Crucially, in standard ECMAScript runtimes (including V8/Node.js), calling `Object.freeze()` on a non-empty `Uint8Array` can throw a `TypeError` (attempting to make elements non-writable on typed array views), and typed-array element storage cannot be made conventionally immutable through ordinary `Object.freeze()`. Direct exposure of a `Uint8Array` inside a frozen custody record permits callers to execute indexing mutations (e.g. `cloned.payload[0] = 0xff`), corrupting supposedly immutable state.

To enforce absolute custody-root immutability across every access path without undefined semantics, prototype pollution, runtime exceptions, or mutable buffer exposure, the architecture defines a **Restricted Custody-Value Universe** and an enforceable private encapsulation contract:

#### 1. The Restricted Custody-Value Universe:
A value $v$ is an admissible custody value if and only if it belongs to one of four categories:
1. **Admissible Primitives (R29-A4):**
   - `string`
   - Finite `number`:
     - Finite fractional numbers are accepted (`Number.isFinite(v) === true`).
     - Integer numbers are accepted if and only if `Number.isSafeInteger(v) === true`. Integers outside $[-(2^{53}-1), 2^{53}-1]$ are strictly rejected with `UNSAFE_INTEGER_PROHIBITED`.
     - Non-finite numbers (`NaN`, `+Infinity`, `-Infinity`) are strictly rejected with `INVALID_NUMERIC_VALUE`.
     - Negative zero (`-0`) is canonicalized to `0` for RFC 8785 canonical snapshot serialization and digest stability.
   - `boolean`
   - `null`
   - *Explicit Primitive Exclusions:*
     - `undefined`: prohibited as values in custody records (`UNSUPPORTED_PRIMITIVE_TYPE: undefined values prohibited; use explicit null or omit property`).
     - `bigint`: prohibited as values in custody records (`UNSUPPORTED_PRIMITIVE_TYPE: bigint values prohibited; use safe integer numbers or canonical strings`).
     - `symbol` values: prohibited as values in custody records (`UNSUPPORTED_PRIMITIVE_TYPE: symbol values prohibited in custody records`).
2. **Encapsulated Byte Arrays (`EncapsulatedByteArray`) (R30-1 / R30-2):**
   - Direct `Uint8Array` inputs are encapsulated within an immutable `EncapsulatedByteArray` value object.
   - *Private Storage Encapsulation:*
     - The underlying buffer is held exclusively in a private `#bytes` field.
     - The custody graph **never exposes mutable `Uint8Array` instances directly**.
     - Attempts to mutate via indexing (`cloned.payload[0] = 0xff`) have zero effect on internal storage (fails in non-strict mode; throws TypeError in strict mode because the wrapper is frozen with `Object.freeze()`).
     - Every public byte read (`.asDetachedUint8Array()`) returns a **fresh detached copy** (`new Uint8Array(...)`).
     - Mutation of a returned detached copy does **not** alter internal custody bytes.
   - *Intrinsic Brand Checks & Getter Immunity (R30-2):*
     - Validates `ArrayBuffer.isView(v) === true`.
     - Strictly asserts prototype identity `Object.getPrototypeOf(v) === Uint8Array.prototype`, immediately rejecting subclasses without code execution.
     - Asserts `Object.getOwnPropertySymbols(v).length === 0`, rejecting own `Symbol.toStringTag` accessors.
     - Asserts own property names do not shadow `%TypedArray%.prototype` accessors (`'buffer'`, `'byteOffset'`, `'byteLength'`).
     - Extracts buffer, offset, and length directly from `%TypedArray%.prototype` intrinsic getters via `Reflect.apply`, guaranteeing zero getter execution.
     - Detects `SharedArrayBuffer` cross-realm backing stores via `SharedArrayBuffer.prototype` brand checking and rejects with `UNSUPPORTED_BINARY_VALUE_TYPE`.
   - *Deterministic Canonical Binary Encoding:*
     - Snapshot export and hashing natively serialize bytes via `.toCanonicalHex()` as lowercase hexadecimal strings (`[0-9a-f]{2*n}`). This encoding is unambiguous, length-preserving, and provides a strict 1-to-1 bijection with byte contents without base64 padding ambiguity.
3. **Plain Dense Arrays (R29-A2):**
   - Strictly `Object.getPrototypeOf(v) === Array.prototype`.
   - Array length must be a non-negative safe integer.
   - *Array Descriptor Validation Without Getter Execution:*
     - For every integer index $0 \le i < \text{length}$:
       1. Retrieve its own property descriptor via `Object.getOwnPropertyDescriptor(v, String(i))`.
       2. If the descriptor is missing (`undefined`), the array is sparse; reject immediately with `SPARSE_ARRAY_PROHIBITED`.
       3. If `desc.get !== undefined || desc.set !== undefined`, reject with `ACCESSOR_PROPERTIES_PROHIBITED` without executing getter or setter functions.
       4. If `!desc.enumerable`, reject with `NON_ENUMERABLE_PROPERTY_PROHIBITED`.
       5. Recursively clone the descriptor's `value`.
       6. Define the cloned indexed element using `Object.defineProperty(cloneArr, String(i), { value, writable: true, enumerable: true, configurable: true })`.
       7. Never read the source array element using ordinary property access (`v[i]`), guaranteeing complete getter immunity.
   - *Precise Array-Index Property Validation:*
     - All own property names of the array (`Object.getOwnPropertyNames(v)`) must strictly be valid array index strings or `'length'`.
     - Valid index strings satisfy: `String(Number(key) >>> 0) === key && (Number(key) >>> 0) !== 0xFFFFFFFF && (Number(key) >>> 0) < length`.
     - Any property key that is non-digit, or numeric-looking but not a valid index (such as `"01"`, `"00"`, `"4294967295"`, or out-of-bounds integers), is strictly rejected with `UNSUPPORTED_ARRAY_PROPERTY`.
     - Symbol properties on arrays are strictly rejected with `SYMBOL_PROPERTIES_PROHIBITED_IN_CANONICAL_VALUES`.
   - The cloned array is frozen with `Object.freeze(cloneArr)`.
4. **Plain Structural Records & Safe Property Creation (R29-A3):**
   - Strictly `Object.getPrototypeOf(v) === Object.prototype` or `Object.getPrototypeOf(v) === null`. Custom prototypes or class instances are rejected with `UNSUPPORTED_PROTOTYPE_INSTANCE` by reference comparison, without reading `proto.constructor`.
   - Null-prototype input records preserve a null prototype (`Object.create(null)`).
   - *Controlled Own-Property Creation via `Object.defineProperty`:*
     - All cloned properties are defined using `Object.defineProperty(cloneObj, key, { value, writable: true, enumerable: true, configurable: true })` prior to `Object.freeze()`.
     - Ordinary property assignment (`cloneObj[key] = ...`) is **strictly forbidden**, ensuring that own data properties named `'__proto__'` remain own data properties and never invoke the legacy `Object.prototype.__proto__` setter or mutate object prototypes.
     - Permitted own data properties such as `'constructor'` and `'prototype'` are safely preserved as own data properties without executing getters.
     - Accessor variants (`desc.get !== undefined || desc.set !== undefined`) are rejected with `ACCESSOR_PROPERTIES_PROHIBITED`.
     - Non-enumerable properties are rejected with `NON_ENUMERABLE_PROPERTY_PROHIBITED`.
   - Canonical Serialization Compatibility (RFC 8785): Symbol-keyed properties are strictly prohibited in all data values subject to canonical snapshot hashing (`SYMBOL_PROPERTIES_PROHIBITED_IN_CANONICAL_VALUES`).
   - The resulting record is frozen with `Object.freeze(cloneObj)`.

#### 2. Strictly Prohibited Types & Ingestion Trust Boundaries (R29-A5 / R29-A6):
Any value containing the following types triggers an immediate synchronous throw:
- Functions, generator functions, async functions $\to$ `UNSUPPORTED_FUNCTION_VALUE_TYPE`
- Native `Map` or `Set` $\to` `UNSUPPORTED_COLLECTION_INSTANCE`
- `Date` $\to$ `UNSUPPORTED_TEMPORAL_VALUE_TYPE` (monotonic timestamp numbers required)
- `RegExp` $\to$ `UNSUPPORTED_REGEXP_VALUE_TYPE`
- `ArrayBuffer`, `SharedArrayBuffer`, `DataView`, or non-Uint8Array typed arrays $\to$ `UNSUPPORTED_BINARY_VALUE_TYPE`
- True cyclic object graphs $\to$ `CIRCULAR_REFERENCE_DETECTED` (detected via active ancestor path tracking)
- Unsafe integers $\to$ `UNSAFE_INTEGER_PROHIBITED`
- Non-finite numbers $\to$ `INVALID_NUMERIC_VALUE`

#### Documented Ingestion Trust Boundary:
1. **Supported Typed Array Contract:**
   The custody ingestion boundary accepts **ordinary, direct `Uint8Array` instances belonging to the coordinator realm**. Subclasses, custom typed views, and cross-realm objects with non-standard prototypes are rejected fail-closed via intrinsic prototype verification.
2. **Proxy-Backed Objects Boundary:**
   The architecture explicitly establishes that **Proxy-backed objects are outside the supported ingestion contract**. While ordinary accessor properties on plain objects are detected through property descriptors without invoking getters or setters, reflective JavaScript operations (`Object.getOwnPropertyDescriptor`, `Object.getPrototypeOf`, `Reflect.ownKeys`) unavoidably trigger Proxy traps if a caller supplies an ECMAScript `Proxy`. The custody ingestion trust boundary assumes schema-validated domain data produced by trusted deserialization and rejects untrusted arbitrary Proxy graphs.

```typescript
// Intrinsic %TypedArray%.prototype property getters
const TypedArray_prototype = Object.getPrototypeOf(Uint8Array.prototype);
const Uint8Array_buffer_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'buffer')!.get!;
const Uint8Array_byteOffset_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'byteOffset')!.get!;
const Uint8Array_byteLength_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'byteLength')!.get!;

/**
 * Validates that an object is an ordinary, direct Uint8Array instance without executing
 * user-controlled getters or prototype traps (R30-2).
 */
export function assertValidOrdinaryUint8Array(val: unknown): asserts val is Uint8Array {
  if (!val || typeof val !== 'object') {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: binary value must be an object');
  }

  // Intrinsic brand check: must be a typed array view
  if (!ArrayBuffer.isView(val)) {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: value is not an ArrayBuffer view');
  }

  // Strict prototype comparison: must be direct Uint8Array, rejecting subclasses
  if (Object.getPrototypeOf(val) !== Uint8Array.prototype) {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: only direct Uint8Array instances are supported; subclasses prohibited');
  }

  // Reject own symbol keys (e.g. hostile own Symbol.toStringTag getters)
  if (Object.getOwnPropertySymbols(val).length > 0) {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: symbol properties on typed arrays are prohibited');
  }

  // Inspect own properties: ensure no prototype accessors are shadowed
  const ownNames = Object.getOwnPropertyNames(val);
  for (const name of ownNames) {
    if (!/^\d+$/.test(name)) {
      throw new Error(`UNSUPPORTED_BINARY_VALUE_TYPE: typed array contains non-index property '${name}'`);
    }
  }

  // Extract buffer via intrinsic prototype getter
  const buffer = Reflect.apply(Uint8Array_buffer_get, val, []) as ArrayBuffer;

  // Reject SharedArrayBuffer backing stores (both in-realm and cross-realm brand check)
  if (
    (typeof SharedArrayBuffer !== 'undefined' && buffer instanceof SharedArrayBuffer) ||
    Object.prototype.toString.call(buffer) === '[object SharedArrayBuffer]'
  ) {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: SharedArrayBuffer-backed views are prohibited');
  }

  if (Object.prototype.toString.call(buffer) !== '[object ArrayBuffer]') {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: invalid buffer backing store');
  }
}

/**
 * Deterministic lowercase hexadecimal binary encoding (R29-A1 / R30-1).
 */
export function encodeCanonicalBinaryHex(bytes: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex;
}

/**
 * Immutable Encapsulated Byte Array (R30-1).
 *
 * Enforces genuine private byte encapsulation. Prevents indexing mutation inside
 * frozen custody records. Public reads return fresh detached copies.
 */
// Value object encapsulating binary byte storage (R30-1, R31-2)
// Non-exported module-scoped lexical helper for internal trust decisions (R33-1 - Blocker A)
// Completely inaccessible outside this module; immune to constructor or prototype monkey-patching.
let isLexicallyTrustedEncapsulatedByteArray: (val: unknown) => val is EncapsulatedByteArray;

// Value object encapsulating binary byte storage (R30-1, R31-2, R32-1, R32-4, R33-1)
export class EncapsulatedByteArray {
  readonly #bytes: Uint8Array;
  readonly #canonicalHex: string;

  static {
    // ECMAScript static initialization block binds the lexical brand checker (R33-1)
    isLexicallyTrustedEncapsulatedByteArray = function(val: unknown): val is EncapsulatedByteArray {
      if (typeof val !== 'object' || val === null) {
        return false;
      }
      try {
        if (Object.getPrototypeOf(val) !== EncapsulatedByteArray.prototype) {
          return false;
        }
        return #bytes in val;
      } catch {
        return false; // Safely handles throwing Proxy traps or hostile objects without side-effects
      }
    };
  }

  /**
   * Public static brand check delegating to unexported lexical checker (R33-1).
   * Internal custody operations never invoke this public static method.
   */
  public static isTrustedInstance(val: unknown): val is EncapsulatedByteArray {
    return isLexicallyTrustedEncapsulatedByteArray(val);
  }

  public constructor(source: Uint8Array | EncapsulatedByteArray) {
    if (isLexicallyTrustedEncapsulatedByteArray(source)) {
      // Direct private-field access across instances (R32-1, R33-1 - Blocker A)
      // Reads source.#bytes and source.#canonicalHex directly; zero public method dispatch.
      const srcBytes = source.#bytes;
      const copy = new Uint8Array(srcBytes.byteLength);
      copy.set(srcBytes);
      this.#bytes = copy;
      this.#canonicalHex = source.#canonicalHex;
    } else if (
      source instanceof EncapsulatedByteArray ||
      (typeof source === 'object' && source !== null && EncapsulatedByteArray.prototype.isPrototypeOf(source))
    ) {
      throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: EncapsulatedByteArray subclasses and untrusted instances prohibited');
    } else {
      assertValidOrdinaryUint8Array(source);
      const byteLength = Reflect.apply(Uint8Array_byteLength_get, source, []) as number;
      const byteOffset = Reflect.apply(Uint8Array_byteOffset_get, source, []) as number;
      const buffer = Reflect.apply(Uint8Array_buffer_get, source, []) as ArrayBuffer;

      // Allocate fresh detached local buffer
      const copy = new Uint8Array(byteLength);
      copy.set(new Uint8Array(buffer, byteOffset, byteLength));
      this.#bytes = copy;
      this.#canonicalHex = encodeCanonicalBinaryHex(copy);
    }
    Object.freeze(this);
  }

  public get byteLength(): number {
    return this.#bytes.byteLength;
  }

  /**
   * Returns a fresh detached copy of the encapsulated bytes.
   */
  public asDetachedUint8Array(): Uint8Array {
    const detached = new Uint8Array(this.#bytes.byteLength);
    detached.set(this.#bytes);
    return detached;
  }

  /**
   * Returns the canonical lowercase hexadecimal representation for snapshot serialization.
   */
  public toCanonicalHex(): string {
    return this.#canonicalHex;
  }

  public equals(other: unknown): boolean {
    if (this === other) return true;
    if (!isLexicallyTrustedEncapsulatedByteArray(other)) return false;
    // Direct private-field comparison across instances (R32-1, R33-1 - Blocker A)
    return this.#canonicalHex === other.#canonicalHex;
  }
}

// Explicitly freeze both the class prototype AND the constructor static API (R32-1, R33-1 - Blocker A)
Object.freeze(EncapsulatedByteArray.prototype);
Object.freeze(EncapsulatedByteArray);

// Intrinsic accessors extracted once at module evaluation to prevent prototype poisoning
const TypedArray_prototype = Object.getPrototypeOf(Uint8Array.prototype);
const Uint8Array_buffer_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'buffer')!.get!;
const Uint8Array_byteOffset_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'byteOffset')!.get!;
const Uint8Array_byteLength_get = Object.getOwnPropertyDescriptor(TypedArray_prototype, 'byteLength')!.get!;

// Intrinsic SharedArrayBuffer byteLength getter for cross-realm internal-slot brand checking (R31-1, R32-3)
const SharedArrayBuffer_byteLength_get = typeof SharedArrayBuffer !== 'undefined'
  ? Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, 'byteLength')?.get
  : undefined;

/**
 * Detects whether an ArrayBuffer backing store is a SharedArrayBuffer across any
 * execution realm by invoking the intrinsic SharedArrayBuffer.prototype.byteLength getter (R31-1, R32-3).
 *
 * Fail-Closed Boundary: If the host environment does not expose the intrinsic
 * SharedArrayBuffer byteLength getter, binary ingestion throws UNSUPPORTED_BINARY_VALUE_TYPE
 * fail-closed rather than assuming safety (R32-3).
 * Compatibility Note: Environments lacking standard SharedArrayBuffer intrinsics deliberately
 * reject binary ingestion fail-closed, ensuring no unverified shared memory enters custody.
 */
export function isSharedArrayBufferBacking(buf: unknown): boolean {
  if (typeof SharedArrayBuffer === 'undefined' || !SharedArrayBuffer_byteLength_get) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: intrinsic SharedArrayBuffer brand checker unavailable in host realm; binary ingestion prohibited fail-closed');
  }
  try {
    Reflect.apply(SharedArrayBuffer_byteLength_get, buf, []);
    return true; // Successfully accessed [[SharedArrayBufferData]] internal slot across any realm!
  } catch {
    return false; // Ordinary ArrayBuffer throws TypeError: Method get SharedArrayBuffer.prototype.byteLength called on incompatible receiver
  }
}

/**
 * Validates that an object is an ordinary, direct Uint8Array instance without executing
 * user-controlled getters or prototype traps (R30-2, R31-1, R31-3, R31-4, R32-3, R32-5).
 */
export function assertValidOrdinaryUint8Array(val: unknown): asserts val is Uint8Array {
  if (!val || typeof val !== 'object') {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: binary value must be an object');
  }

  // Intrinsic brand check: must be a typed array view
  if (!ArrayBuffer.isView(val)) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: value is not an ArrayBuffer view');
  }

  // Strict prototype comparison: supported-input boundary restriction (R31-4)
  // Input Uint8Array views must originate from the coordinator's current execution realm.
  if (Object.getPrototypeOf(val) !== Uint8Array.prototype) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: only direct Uint8Array instances from current realm are supported; subclasses and cross-realm views prohibited');
  }

  // Reject own symbol keys (e.g. hostile own Symbol.toStringTag getters)
  if (Object.getOwnPropertySymbols(val).length > 0) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: symbol properties on typed arrays are prohibited');
  }

  // Inspect own properties: extra own string properties strictly rejected (R31-3, R32-5)
  // Regex strictly enforces canonical non-negative integer indices ^(?:0|[1-9]\d*)$
  const ownNames = Object.getOwnPropertyNames(val);
  for (const name of ownNames) {
    if (!/^(?:0|[1-9]\d*)$/.test(name)) {
      throw new TypeError(`UNSUPPORTED_ARRAY_PROPERTY: extra own string properties on typed arrays prohibited; found '${name}'`);
    }
  }

  // Extract backing buffer via intrinsic prototype getter
  const buffer = Reflect.apply(Uint8Array_buffer_get, val, []) as ArrayBuffer;

  // Cross-realm SharedArrayBuffer detection via internal-slot brand check (R31-1, R32-3)
  if (isSharedArrayBufferBacking(buffer)) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: SharedArrayBuffer backing stores prohibited');
  }
}

/**
 * Recursively validates, clones, and encapsulates all nested structural records,
 * dense arrays, and byte arrays within the Restricted Custody-Value Universe.
 *
 * Guarantees:
 * 1. Immediate function rejection at top boundary.
 * 2. Exact primitive universe with safe integers and canonical numbers.
 * 3. Array descriptor inspection without getter execution; rejection of accessors and sparse arrays.
 * 4. Controlled own-property creation via Object.defineProperty (__proto__ safe).
 * 5. Private byte encapsulation via EncapsulatedByteArray with direct private-field access (R32-1, R33-1).
 * 6. Trusted-instance brand check with unexported lexical helper & completely frozen static API (R33-1).
 * 7. Intrinsic brand checks and fail-closed cross-realm SharedArrayBuffer detection (R31-1, R32-3).
 * 8. Rejection of extra own string properties on typed arrays via ^(?:0|[1-9]\d*)$ (R31-3, R32-5).
 * 9. Cycle detection with active ancestor stack while supporting acyclic DAGs.
 */
export function deepCloneAndFreezeValue<T>(
  val: T,
  activeStack: Set<object> = new Set(),
  cloneMap: WeakMap<object, any> = new WeakMap()
): T {
  // 1. Function rejection at the absolute entry boundary (R28-A)
  if (typeof val === 'function') {
    throw new Error('UNSUPPORTED_FUNCTION_VALUE_TYPE: functions cannot be stored in custody records');
  }

  // 2. Primitive handling & explicit primitive universe validation (R29-A4)
  if (val === null || typeof val !== 'object') {
    if (val === undefined) {
      throw new Error('UNSUPPORTED_PRIMITIVE_TYPE: undefined values prohibited in custody records; use explicit null or omit property');
    }
    if (typeof val === 'bigint') {
      throw new Error('UNSUPPORTED_PRIMITIVE_TYPE: bigint values prohibited in custody records; use safe integer numbers or canonical strings');
    }
    if (typeof val === 'symbol') {
      throw new Error('UNSUPPORTED_PRIMITIVE_TYPE: symbol values prohibited in custody records');
    }
    if (typeof val === 'number') {
      if (!Number.isFinite(val)) {
        throw new Error(`INVALID_NUMERIC_VALUE: non-finite number '${String(val)}' prohibited in custody records`);
      }
      if (Number.isInteger(val) && !Number.isSafeInteger(val)) {
        throw new Error(`UNSAFE_INTEGER_PROHIBITED: integer '${String(val)}' exceeds safe integer range [-(2^53-1), 2^53-1]`);
      }
      // Canonicalize negative zero to positive zero for RFC 8785 stability
      if (Object.is(val, -0)) {
        return 0 as unknown as T;
      }
    }
    return val;
  }

  const objVal = val as object;

  // 3. Cycle Detection vs. Acyclic Shared Reference Handling (R28-A)
  if (activeStack.has(objVal)) {
    throw new Error('CIRCULAR_REFERENCE_DETECTED: circular object graph cannot be cloned into custody storage');
  }

  if (cloneMap.has(objVal)) {
    return cloneMap.get(objVal) as T;
  }

  // 4. Encapsulated Byte Array Handling & Subclass Rejection (R30-1, R31-2, R32-1, R33-1)
  if (isLexicallyTrustedEncapsulatedByteArray(objVal)) {
    cloneMap.set(objVal, objVal);
    return objVal as unknown as T;
  }

  // Reject EncapsulatedByteArray subclasses and prototype-polluted instances
  if (
    objVal instanceof EncapsulatedByteArray ||
    EncapsulatedByteArray.prototype.isPrototypeOf(objVal)
  ) {
    throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: EncapsulatedByteArray subclasses and untrusted instances prohibited');
  }

  // 5. Uint8Array View Ingestion & Brand Checking (R30-1, R30-2, R31-1, R31-3, R31-4, R32-3)
  if (ArrayBuffer.isView(objVal)) {
    if (Object.getPrototypeOf(objVal) !== Uint8Array.prototype) {
      throw new TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: only direct Uint8Array instances from current realm are supported');
    }
    assertValidOrdinaryUint8Array(objVal as Uint8Array);
    const encapsulated = new EncapsulatedByteArray(objVal as Uint8Array);
    cloneMap.set(objVal, encapsulated);
    return encapsulated as unknown as T;
  }

  activeStack.add(objVal);
  try {
    // 6. Fail-closed rejection of prohibited binary, temporal, and collection types
    if (
      objVal instanceof ArrayBuffer ||
      (typeof SharedArrayBuffer !== 'undefined' && objVal instanceof SharedArrayBuffer) ||
      isSharedArrayBufferBacking(objVal)
    ) {
      throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: raw ArrayBuffer and SharedArrayBuffer cannot be stored directly; use EncapsulatedByteArray');
    }
    if (objVal instanceof Date) {
      throw new Error('UNSUPPORTED_TEMPORAL_VALUE_TYPE: Date instances prohibited; use monotonic integer timestamps or ISO-8601 strings');
    }
    if (objVal instanceof Map || objVal instanceof Set) {
      throw new Error('UNSUPPORTED_PROTOTYPE_INSTANCE: standard Map/Set prohibited; use ImmutableMap/ImmutableSet');
    }
    if (objVal instanceof RegExp || objVal instanceof Promise || objVal instanceof Error) {
      throw new Error(`UNSUPPORTED_PROTOTYPE_INSTANCE: instances of ${objVal.constructor.name} prohibited in custody records`);
    }

    // 7. Array cloning via descriptor inspection (R28-A, R29-A2)
    if (Array.isArray(objVal)) {
      if (Object.getPrototypeOf(objVal) !== Array.prototype) {
        throw new Error('UNSUPPORTED_PROTOTYPE_INSTANCE: array subclasses prohibited in custody records');
      }

      const len = objVal.length;
      const ownNames = Object.getOwnPropertyNames(objVal);

      // Verify dense array: own properties must be exactly indices 0 to len-1 plus 'length'
      if (ownNames.length !== len + 1) {
        throw new Error('SPARSE_ARRAY_PROHIBITED: sparse arrays and non-index array properties prohibited in custody records');
      }

      const clonedArray: any[] = [];
      cloneMap.set(objVal, clonedArray);

      for (let i = 0; i < len; i++) {
        const desc = Object.getOwnPropertyDescriptor(objVal, String(i));
        if (!desc || desc.get || desc.set) {
          throw new Error(`ACCESSOR_PROPERTIES_PROHIBITED: array index '${i}' has accessor descriptors`);
        }
        if (!desc.enumerable) {
          throw new Error(`NON_ENUMERABLE_PROPERTY_PROHIBITED: array index '${i}' is non-enumerable`);
        }
        clonedArray[i] = deepCloneAndFreezeValue(desc.value, activeStack, cloneMap);
      }

      return Object.freeze(clonedArray) as unknown as T;
    }

    // 8. Plain Object cloning via descriptor inspection and Object.defineProperty (R28-A, R29-A3)
    const proto = Object.getPrototypeOf(objVal);
    if (proto !== Object.prototype && proto !== null) {
      throw new Error('UNSUPPORTED_PROTOTYPE_INSTANCE: non-plain objects and class instances prohibited in custody records');
    }

    // Prohibit symbol keys on custody records (R28-A)
    const symKeys = Object.getOwnPropertySymbols(objVal);
    if (symKeys.length > 0) {
      throw new Error('SYMBOL_PROPERTIES_PROHIBITED_IN_CANONICAL_VALUES: symbol-keyed properties prohibited in custody records');
    }

    // Create cloned object preserving null-prototype semantics safely
    const clonedRecord: Record<string, any> = proto === null ? Object.create(null) : {};
    cloneMap.set(objVal, clonedRecord);

    const ownProps = Object.getOwnPropertyNames(objVal);
    // Sort property names canonically according to RFC 8785 byte order
    ownProps.sort();

    for (let i = 0; i < ownProps.length; i++) {
      const prop = ownProps[i];
      const desc = Object.getOwnPropertyDescriptor(objVal, prop);
      if (!desc) {
        continue;
      }
      if (desc.get || desc.set) {
        throw new Error(`ACCESSOR_PROPERTIES_PROHIBITED: property '${prop}' has accessor descriptors`);
      }
      if (!desc.enumerable) {
        throw new Error(`NON_ENUMERABLE_PROPERTY_PROHIBITED: property '${prop}' is non-enumerable`);
      }

      const clonedPropValue = deepCloneAndFreezeValue(desc.value, activeStack, cloneMap);

      // Define property directly on target to prevent prototype poisoning attacks (R29-A3)
      Object.defineProperty(clonedRecord, prop, {
        value: clonedPropValue,
        writable: false,
        enumerable: true,
        configurable: false
      });
    }

    return Object.freeze(clonedRecord) as T;
  } finally {
    activeStack.delete(objVal);
  }
}

/**
 * ImmutableMap with runtime private storage and deep cloning.
 */
export class ImmutableMap<K extends string | number, V> implements Iterable<[K, V]> {
  #inner: Map<K, V>;

  public constructor(entries?: Iterable<readonly [K, V]> | ReadonlyMap<K, V>) {
    const map = new Map<K, V>();
    if (entries) {
      for (const [k, v] of entries) {
        if (typeof k !== 'string' && typeof k !== 'number') {
          throw new Error('INVALID_COLLECTION_KEY_TYPE: keys must be primitive string or number');
        }
        if (typeof k === 'number') {
          if (!Number.isFinite(k)) {
            throw new Error('INVALID_NUMERIC_VALUE: map key cannot be NaN or Infinity');
          }
          if (Number.isInteger(k) && !Number.isSafeInteger(k)) {
            throw new Error('UNSAFE_INTEGER_PROHIBITED: map key exceeds safe integer range');
          }
        }
        map.set(k, deepCloneAndFreezeValue(v));
      }
    }
    this.#inner = map;
    Object.freeze(this);
  }

  public get size(): number {
    return this.#inner.size;
  }

  public get(key: K): V | undefined {
    const val = this.#inner.get(key);
    if (val === undefined) return undefined;
    return deepCloneAndFreezeValue(val);
  }

  public has(key: K): boolean {
    return this.#inner.has(key);
  }

  public *entries(): IterableIterator<[K, V]> {
    for (const [k, v] of this.#inner.entries()) {
      yield [k, deepCloneAndFreezeValue(v)];
    }
  }

  public *keys(): IterableIterator<K> {
    for (const k of this.#inner.keys()) {
      yield k;
    }
  }

  public *values(): IterableIterator<V> {
    for (const v of this.#inner.values()) {
      yield deepCloneAndFreezeValue(v);
    }
  }

  public [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.entries();
  }

  public set(key: K, value: V): ImmutableMap<K, V> {
    if (typeof key !== 'string' && typeof key !== 'number') {
      throw new Error('INVALID_COLLECTION_KEY_TYPE: keys must be primitive string or number');
    }
    if (typeof key === 'number') {
      if (!Number.isFinite(key)) {
        throw new Error('INVALID_NUMERIC_VALUE: map key cannot be NaN or Infinity');
      }
      if (Number.isInteger(key) && !Number.isSafeInteger(key)) {
        throw new Error('UNSAFE_INTEGER_PROHIBITED: map key exceeds safe integer range');
      }
    }
    const next = new Map(this.#inner);
    next.set(key, deepCloneAndFreezeValue(value));
    return new ImmutableMap(next);
  }

  public delete(key: K): ImmutableMap<K, V> {
    const next = new Map(this.#inner);
    next.delete(key);
    return new ImmutableMap(next);
  }
}

/**
 * ImmutableSet with runtime private storage.
 */
export class ImmutableSet<T extends string | number> implements Iterable<T> {
  #inner: Set<T>;

  public constructor(values?: Iterable<T> | ReadonlySet<T>) {
    const set = new Set<T>();
    if (values) {
      for (const v of values) {
        if (typeof v !== 'string' && typeof v !== 'number') {
          throw new Error('INVALID_COLLECTION_ELEMENT_TYPE: set elements must be primitive string or number');
        }
        if (typeof v === 'number') {
          if (!Number.isFinite(v)) {
            throw new Error('INVALID_NUMERIC_VALUE: set element cannot be NaN or Infinity');
          }
          if (Number.isInteger(v) && !Number.isSafeInteger(v)) {
            throw new Error('UNSAFE_INTEGER_PROHIBITED: set element exceeds safe integer range');
          }
        }
        set.add(v);
      }
    }
    this.#inner = set;
    Object.freeze(this);
  }

  public get size(): number {
    return this.#inner.size;
  }

  public has(value: T): boolean {
    return this.#inner.has(value);
  }

  public *values(): IterableIterator<T> {
    for (const v of this.#inner.values()) {
      yield v;
    }
  }

  public [Symbol.iterator](): IterableIterator<T> {
    return this.values();
  }

  public add(value: T): ImmutableSet<T> {
    if (typeof value !== 'string' && typeof value !== 'number') {
      throw new Error('INVALID_COLLECTION_ELEMENT_TYPE: set elements must be primitive string or number');
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new Error('INVALID_NUMERIC_VALUE: set element cannot be NaN or Infinity');
      }
      if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
        throw new Error('UNSAFE_INTEGER_PROHIBITED: set element exceeds safe integer range');
      }
    }
    const next = new Set(this.#inner);
    next.add(value);
    return new ImmutableSet(next);
  }

  public delete(value: T): ImmutableSet<T> {
    const next = new Set(this.#inner);
    next.delete(value);
    return new ImmutableSet(next);
  }
}

#### Precisely Qualified Byte Array Comparison:
Equality comparisons across byte containers evaluate byte contents bitwise without content-dependent early exits:

```typescript
/**
 * Fixed-iteration, content-independent comparison for equal-length byte arrays.
 */
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

All objects entering `AuthoritativeCustodyRoot` are deeply cloned and frozen on ingress. All binary byte arrays are converted into immutable `EncapsulatedByteArray` value objects whose internal buffers are inaccessible from outside and whose public methods return freshly allocated detached copies on egress, preventing external reference mutation and memory corruption.

### 12.11 Permitted Generation Domain & Sequence Exhaustion Boundary

- Permitted generations: integers $g \in [1, \text{Number.MAX\_SAFE\_INTEGER}]$.
- Out-of-domain values ($g \le 0$, non-integers, NaN, $\infty$) are rejected upfront with `INVALID_GENERATION_DOMAIN`.
- Sequence exhaustion ($g = \text{Number.MAX\_SAFE\_INTEGER}$) triggers fail-closed `GENERATION_SEQUENCE_EXHAUSTED`.

### 12.12 The Complete Lifecycle Publication, Authoritative Boundary Fencing, Encapsulated Immutable Custody & Replay Bijection Verification

#### The Complete Custody Root State Structure:

```typescript
export interface CoordinatorBootEpoch {
  readonly clusterTerm: number;       // Monotonic consensus term under which leadership was granted
  readonly bootGeneration: number;    // Durable monotonically incrementing boot sequence (WAL/consensus)
  readonly bootInstanceId: string;    // Collision-resistant cryptographically secure UUIDv4
}

export interface CustodyFencingToken {
  readonly clusterTerm: number;
  readonly bootGeneration: number;
  readonly bootInstanceId: string;
  readonly publicationSequence: number;
}

export interface PublicationAttemptContext {
  readonly attemptId: string;
  readonly fencingToken: CustodyFencingToken;
  readonly candidateRootDigest: string;
  readonly expectedPriorSequence: number;
  readonly expectedPriorRootDigest: string;
  readonly localBootGeneration: number;
  readonly localBootInstanceId: string;
  readonly localClusterTerm: number;
}

export interface AuthoritativeCommittedConfirmation {
  readonly status: 'COMMITTED';
  readonly clusterTerm: number;
  readonly bootGeneration: number;
  readonly bootInstanceId: string;
  readonly committedSequence: number;
  readonly attemptId: string;
  readonly committedRootDigest: string;
  readonly expectedPriorSequence: number;
  readonly expectedPriorRootDigest: string;
}

export type AuthoritativePublicationOutcome =
  | AuthoritativeCommittedConfirmation
  | { readonly status: 'FENCED_REJECTED'; readonly higherTerm?: number; readonly reason: string }
  | { readonly status: 'UNCERTAIN'; readonly error: Error };

export interface CustodyReconciliationQuery {
  readonly fencingToken: CustodyFencingToken;
  readonly attemptId: string; // Exact immutable attemptId bound to initial publication attempt (R31-5)
  readonly candidateRootDigest: string;
  readonly expectedPriorSequence: number;
  readonly expectedPriorRootDigest: string;
}

export type CustodyReconciliationResult =
  | { readonly status: 'RECONCILED_COMMITTED'; readonly committedSequence: number; readonly committedRoot: AuthoritativeCustodyRoot }
  | { readonly status: 'NOT_COMMITTED_FINAL'; readonly sequence: number; readonly terminalTombstoneId: string; readonly publicationIdentity: PublicationAttemptContext; readonly authoritativeCurrentSequence: number; readonly authoritativeCurrentRoot: AuthoritativeCustodyRoot }
  | { readonly status: 'UNKNOWN_IN_PROGRESS'; readonly sequence: number; readonly reason: string }
  | { readonly status: 'RECONCILED_CONFLICT'; readonly reason: 'CONFLICTING_DIGEST_AT_SEQUENCE' | 'DUPLICATE_SEQUENCE_CONFLICT' | string }
  | { readonly status: 'RECONCILED_FENCED_REJECTED'; readonly higherTerm: number; readonly reason: string };

export interface IAuthoritativeCustodyStore {
  /**
   * Atomically evaluates the complete 4-dimensional fencing token and attempt context
   * against consensus state and persists candidateRoot.
   */
  publishFencedRoot(
    fencingToken: CustodyFencingToken,
    candidateRoot: AuthoritativeCustodyRoot,
    attemptContext: PublicationAttemptContext
  ): Promise<AuthoritativePublicationOutcome>;

  /**
   * Idempotent reconciliation of uncertain publication attempts (R29-B2).
   */
  reconcilePublication(
    query: CustodyReconciliationQuery
  ): Promise<CustodyReconciliationResult>;

  /**
   * Retrieves the authoritative cluster custody root and metadata for reconciliation.
   */
  getAuthoritativeRoot(): Promise<{
    readonly root: AuthoritativeCustodyRoot;
    readonly highestCommittedTerm: number;
    readonly activeBootGeneration: number;
    readonly activeBootInstanceId: string;
    readonly lastCommittedSequence: number;
    readonly lastCommittedRootDigest: string;
  }>;

  getHighestCommittedTerm(): number;
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

export class LeadershipEpochFence {
  #publicationLockHeld: boolean = false;
  #waitQueue: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  #currentTerm: number;
  #leaseExpiresAtMonotonicMs: number;
  #guardMarginMs: number;
  #isSteppedDown: boolean = false;
  #activePublicationAttempt: PublicationAttemptContext | null = null;

  public constructor(
    initialTerm: number,
    initialLeaseDurationMs: number,
    guardMarginMs: number = 1000
  ) {
    this.#currentTerm = initialTerm;
    this.#leaseExpiresAtMonotonicMs = performance.now() + initialLeaseDurationMs;
    this.#guardMarginMs = guardMarginMs;
  }

  public stepDown(newTerm?: number): void {
    this.#isSteppedDown = true;
    if (newTerm !== undefined && newTerm > this.#currentTerm) {
      this.#currentTerm = newTerm;
    }
    // Abort all queued publication attempts
    while (this.#waitQueue.length > 0) {
      const waiter = this.#waitQueue.shift()!;
      waiter.reject(new Error('COORDINATOR_STOPPED: coordinator stepped down while awaiting publication lock'));
    }
  }

  public renewLease(term: number, durationMs: number): void {
    if (this.#isSteppedDown) {
      throw new Error('LEADERSHIP_FENCE_ALREADY_STEPPED_DOWN');
    }
    if (term < this.#currentTerm) {
      throw new Error('STALE_TERM_RENEWAL_REJECTED');
    }
    this.#currentTerm = term;
    this.#leaseExpiresAtMonotonicMs = performance.now() + durationMs;
  }

  public get currentTerm(): number {
    return this.#currentTerm;
  }

  public get isSteppedDown(): boolean {
    return this.#isSteppedDown;
  }

  public get leaseExpiresAtMonotonicMs(): number {
    return this.#leaseExpiresAtMonotonicMs;
  }

  public get activeAttempt(): PublicationAttemptContext | null {
    return this.#activePublicationAttempt;
  }

  public setActiveAttempt(attempt: PublicationAttemptContext): void {
    this.#activePublicationAttempt = attempt;
  }

  public clearActiveAttempt(): void {
    this.#activePublicationAttempt = null;
  }

  /**
   * Asynchronous Process-Local Publication Lock (R29-B4).
   * Enforces deterministic FIFO queuing without busy-waiting.
   */
  public async acquirePublicationLock(): Promise<void> {
    if (this.#isSteppedDown) {
      throw new Error('LEADERSHIP_FENCE_ALREADY_STEPPED_DOWN: cannot acquire publication lock');
    }
    if (!this.#publicationLockHeld) {
      this.#publicationLockHeld = true;
      return;
    }
    return new Promise<void>((resolve, reject) => {
      this.#waitQueue.push({ resolve, reject });
    });
  }

  public releasePublicationLock(): void {
    if (this.#waitQueue.length > 0) {
      const next = this.#waitQueue.shift()!;
      if (this.#isSteppedDown) {
        next.reject(new Error('COORDINATOR_STOPPED: coordinator stepped down while waiting for publication lock'));
      } else {
        next.resolve();
      }
    } else {
      this.#publicationLockHeld = false;
    }
  }

  public assertLocalLeaseValid(expectedTerm: number): void {
    if (this.#isSteppedDown) {
      throw new Error('STALE_COORDINATOR_LEADERSHIP_FENCED: coordinator has stepped down');
    }
    if (this.#currentTerm !== expectedTerm) {
      throw new Error(`STALE_COORDINATOR_LEADERSHIP_FENCED: term mismatch (expected ${expectedTerm}, current ${this.#currentTerm})`);
    }
    const monotonicNow = performance.now();
    if (monotonicNow + this.#guardMarginMs >= this.#leaseExpiresAtMonotonicMs) {
      throw new Error(
        `STALE_COORDINATOR_LEADERSHIP_FENCED: leadership lease expired or within guard margin (now ${monotonicNow}ms + guard ${this.#guardMarginMs}ms >= expiry ${this.#leaseExpiresAtMonotonicMs}ms)`
      );
    }
  }

  /**
   * Post-Await Completion Fencing (R29-B1).
   * Prohibits local pointer replacement if leadership was lost while awaiting store response.
   */
  public assertPostAwaitEligibility(attempt: PublicationAttemptContext): void {
    if (this.#isSteppedDown) {
      throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: coordinator stepped down while awaiting store response');
    }
    if (this.#currentTerm !== attempt.localClusterTerm) {
      throw new Error(`CUSTODY_PUBLICATION_STALE_LEADERSHIP: term advanced from ${attempt.localClusterTerm} to ${this.#currentTerm} during store persistence`);
    }
    const monotonicNow = performance.now();
    if (monotonicNow + this.#guardMarginMs >= this.#leaseExpiresAtMonotonicMs) {
      throw new Error(`CUSTODY_PUBLICATION_STALE_LEADERSHIP: leadership lease expired while awaiting store response`);
    }
    if (!this.#activePublicationAttempt || this.#activePublicationAttempt.attemptId !== attempt.attemptId) {
      throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: active publication attempt was superseded or cleared');
    }
  }
}

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
  readonly pinId: string; // pin:${clusterTerm}.${bootGeneration}.${bootInstanceId}:${generation}:${barrierIdentity}:${seq}
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
  readonly leaseId: string; // lease:${clusterTerm}.${bootGeneration}.${bootInstanceId}:${generation}:${consumerId}:${seq}
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

#### Authoritative Boundary Fencing & Two-Phase Publication (R28-B):

To guarantee linearizability without exposing uncommitted state or unsafe callbacks:
1. Every custody root mutation carries a complete 4-dimensional fencing token $\langle \mathbf{clusterTerm}, \mathbf{bootGeneration}, \mathbf{bootInstanceId}, \mathbf{publicationSequence} \rangle$.
2. Mutual exclusion within the process is enforced via `#publicationLock`.
3. Publication executes strictly as an internal two-phase sequence without arbitrary caller callbacks:
   - **Phase 1 (Asynchronous Authoritative Commit):** The coordinator submits candidate root and token to `IAuthoritativeCustodyStore.publishFencedRoot()`. The store validates term, boot generation, instance, and sequence monotonicity against cluster consensus.
   - **Phase 2 (Synchronous Local Pointer Replacement):** Only when the store returns `{ status: 'COMMITTED' }` does the coordinator synchronously execute `this.#custodyRoot = candidateRoot`.
4. If the store returns `FENCED_REJECTED`, the candidate root is discarded, `#custodyRoot` is untouched, local custody enters `CUSTODY_RECONCILIATION_BLOCKED`, and the barrier is left unresolved.
5. If the store returns `UNCERTAIN`, the coordinator withholds local pointer replacement, transitions to `RECONCILING_CUSTODY`, and verifies state via `store.getAuthoritativeRoot()` before proceeding.

#### Cross-Restart Retirement Semantics (R25-3):

1. **Active Resources at Restart:** At coordinator restart, volatile memory is reinitialized; active collections initialize empty (`size = 0`). Any unreleased active resource from a prior crash is abandoned and can never acquire new custody or affect a new recovery attempt. Any release request presenting an active identifier from an earlier boot epoch fails closed with `RESOURCE_NOT_FOUND`.
2. **Retired Resources Restored from Checkpoint:** When retired records are restored from a persistent consensus checkpoint, delayed release requests presenting an identifier from an earlier boot epoch locate the retired record in `retiredSettlementPins` (or `retiredRetryLeases`), verify that `retiredRecord.allocatedBootEpoch` strictly matches the identifier's embedded epoch, verify complete 7-dimensional identity, and verify byte-for-byte evidence digest equality, returning `ALREADY_RELEASED`. If absent from retired records, it fails closed with `RESOURCE_NOT_FOUND`.

#### The Replay-Key Bijection Domain (R25-5):

1. `consumedSettlementEvidence` contains **only** replay keys corresponding to records in `retiredSettlementPins`.
2. Canonical Replay Key: `${pin.generation}:${pin.barrierIdentity}:${pinId}`.
3. Strict Bijection Invariant:
   $$\forall (pinId, pin) \in \mathbf{retiredSettlementPins}: \mathbf{makeReplayKey}(pin) \in \mathbf{consumedSettlementEvidence}$$
   $$\forall replayKey \in \mathbf{consumedSettlementEvidence}: \exists! (pinId, pin) \in \mathbf{retiredSettlementPins} \text{ s.t. } \mathbf{makeReplayKey}(pin) = replayKey$$
   $$\implies |\mathbf{retiredSettlementPins}| \equiv |\mathbf{consumedSettlementEvidence}| \le 5000$$

---

#### Complete Corrected Architectural Pseudocode:

```typescript
const DRAINER_RELEASE_BRAND = Symbol('@syncplay/DrainerCustodyReleaseReceipt');
const SETTLEMENT_EVIDENCE_BRAND = Symbol('@syncplay/SettlementEvidence');
const AUTHORIZED_EVIDENCE_REGISTRY = new WeakSet<AuthoritativeSettlementEvidence>();

export function cloneAndFreezeOwnerToken(tok: CustodyOwnershipToken): CustodyOwnershipToken {
  return deepCloneAndFreezeValue(tok);
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
    settledAtTimestamp: number;
    monotonicSettledMs: number;
    releaseReceipt: DrainerCustodyReleaseReceipt;
    reconciliationReceipt: CustodyReconciliationReceipt;
    archiveReceipt: ArchiveDispositionReceipt;
    executionReceipt: ExecutionSettlementReceipt;
  }): AuthoritativeSettlementEvidence {
    if (!Number.isSafeInteger(params.generation) || params.generation <= 0) {
      throw new Error('INVALID_SETTLEMENT_EVIDENCE_GENERATION');
    }
    if (params.terminalOutcome !== 'CONFIRMED_COMMITTED' && params.terminalOutcome !== 'ABORTED_FENCED') {
      throw new Error('INVALID_SETTLEMENT_EVIDENCE_OUTCOME');
    }

    const payload = `${params.generation}:${params.transactionId}:${params.operationIdentity}:${params.barrierIdentity}:${params.terminalOutcome}:${params.drainerOwnershipGeneration}:${params.expectedOwner.recoveryAttemptId}`;
    const rawDigest = computeDeterministicSha256(payload);

    const evidence: AuthoritativeSettlementEvidence = Object.freeze({
      kind: 'TERMINAL_SETTLEMENT' as const,
      [SETTLEMENT_EVIDENCE_BRAND]: true as const,
      generation: params.generation,
      expectedOwner: cloneAndFreezeOwnerToken(params.expectedOwner),
      transactionId: params.transactionId,
      operationIdentity: params.operationIdentity,
      barrierIdentity: params.barrierIdentity,
      drainerOwnershipGeneration: params.drainerOwnershipGeneration,
      terminalOutcome: params.terminalOutcome,
      settledAtTimestamp: params.settledAtTimestamp,
      monotonicSettledMs: params.monotonicSettledMs,
      releaseReceipt: deepCloneAndFreezeValue(params.releaseReceipt),
      reconciliationReceipt: deepCloneAndFreezeValue(params.reconciliationReceipt),
      archiveReceipt: deepCloneAndFreezeValue(params.archiveReceipt),
      executionReceipt: deepCloneAndFreezeValue(params.executionReceipt),
      evidenceDigest: cloneByteArray(rawDigest),
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

export class AuthoritativeCustodyCoordinator {
  #custodyRoot: AuthoritativeCustodyRoot;
  #coordinatorBootEpoch: CoordinatorBootEpoch;
  #coordinatorBootId: string;
  #pinSequenceCounter: number = 0;
  #leaseSequenceCounter: number = 0;
  #publicationSequenceCounter: number = 0;
  #leadershipFence: LeadershipEpochFence;
  #authoritativeStore: IAuthoritativeCustodyStore;
  #custodyGateState: CustodyGateState = CustodyGateState.IDLE;

  public constructor(
    bootEpoch: CoordinatorBootEpoch,
    leadershipFence: LeadershipEpochFence,
    authoritativeStore: IAuthoritativeCustodyStore,
    initialRoot?: AuthoritativeCustodyRoot
  ) {
    this.#coordinatorBootEpoch = Object.freeze({ ...bootEpoch });
    this.#coordinatorBootId = `${bootEpoch.clusterTerm}.${bootEpoch.bootGeneration}.${bootEpoch.bootInstanceId}`;
    this.#leadershipFence = leadershipFence;
    this.#authoritativeStore = authoritativeStore;
    this.#custodyRoot = initialRoot ?? createInitialCustodyRoot();
  }

  public get custodyRoot(): AuthoritativeCustodyRoot {
    return this.#custodyRoot;
  }

  public get custodyGateState(): CustodyGateState {
    return this.#custodyGateState;
  }

  private _computeRootDigest(root: AuthoritativeCustodyRoot): string {
    return computeDeterministicSha256(
      `root:${this.#coordinatorBootId}:${root.publishedGeneration}:${root.custodyGenerationFloor}:${root.publicationResults.size}:${root.generationAuditHistory.size}`
    );
  }

  /**
   * Full Asynchronous Publication Lock Lifecycle Across 8 Phases (R29-B4).
   *
   * Executes candidate invariant verification, version capture, sequence and attempt allocation,
   * local lease assertion, asynchronous store-level fenced persistence, exact confirmation verification,
   * post-await completion fencing, and strictly post-commit synchronous local pointer update.
   */
  private async _executeTwoPhasePublication(
    candidateRoot: AuthoritativeCustodyRoot
  ): Promise<void> {
    // Phase 1: Lock Acquisition (Queued in FIFO order)
    await this.#leadershipFence.acquirePublicationLock();
    try {
      // Phase 2: Pre-Publication Candidate Root Invariant Verification (R25-5 / R28-A)
      AuthoritativeCustodyCoordinator.validateCandidateRootPrePublicationInvariants(candidateRoot);

      // Phase 3: Prior Root & Version Capture under lock
      const priorRoot = this.#custodyRoot;
      const expectedPriorSequence = this.#publicationSequenceCounter;
      const expectedPriorRootDigest = this._computeRootDigest(priorRoot);
      const candidateRootDigest = this._computeRootDigest(candidateRoot);

      // Phase 4: Sequence & Attempt Context Allocation under lock
      const seq = ++this.#publicationSequenceCounter;
      const fencingToken: CustodyFencingToken = Object.freeze({
        clusterTerm: this.#leadershipFence.currentTerm,
        bootGeneration: this.#coordinatorBootEpoch.bootGeneration,
        bootInstanceId: this.#coordinatorBootEpoch.bootInstanceId,
        publicationSequence: seq,
      });

      const attemptContext: PublicationAttemptContext = Object.freeze({
        attemptId: `att:${this.#coordinatorBootEpoch.bootInstanceId}:${seq}`,
        fencingToken,
        candidateRootDigest,
        expectedPriorSequence,
        expectedPriorRootDigest,
        localBootGeneration: this.#coordinatorBootEpoch.bootGeneration,
        localBootInstanceId: this.#coordinatorBootEpoch.bootInstanceId,
        localClusterTerm: this.#leadershipFence.currentTerm,
      });

      this.#leadershipFence.setActiveAttempt(attemptContext);

      // Linearized Consensus Leadership Fencing Check (Local Pre-Check with Guard Margin)
      this.#leadershipFence.assertLocalLeaseValid(fencingToken.clusterTerm);

      // Phase 5: Asynchronous Authoritative Store Persistence (Distributed Fencing Boundary)
      let outcome: AuthoritativePublicationOutcome;
      try {
        outcome = await this.#authoritativeStore.publishFencedRoot(fencingToken, candidateRoot, attemptContext);
      } catch (err: any) {
        outcome = { status: 'UNCERTAIN', error: err instanceof Error ? err : new Error(String(err)) };
      }

      // Phase 6: Outcome Validation & Post-Await Fencing
      if (outcome.status === 'FENCED_REJECTED') {
        this.#leadershipFence.stepDown(outcome.higherTerm);
        throw new Error(`STALE_COORDINATOR_LEADERSHIP_FENCED: authoritative store rejected publication: ${outcome.reason}`);
      }

      if (outcome.status === 'UNCERTAIN') {
        this.#custodyGateState = CustodyGateState.RECONCILING_CUSTODY;
        throw new Error(`PUBLICATION_OUTCOME_UNCERTAIN: store response ambiguous: ${outcome.error.message}`);
      }

      // Validate exact COMMITTED confirmation including attemptId (R30-3)
      if (
        outcome.clusterTerm !== attemptContext.localClusterTerm ||
        outcome.bootGeneration !== attemptContext.localBootGeneration ||
        outcome.bootInstanceId !== attemptContext.localBootInstanceId ||
        outcome.committedSequence !== attemptContext.fencingToken.publicationSequence ||
        outcome.attemptId !== attemptContext.attemptId ||
        outcome.committedRootDigest !== attemptContext.candidateRootDigest ||
        outcome.expectedPriorSequence !== attemptContext.expectedPriorSequence ||
        outcome.expectedPriorRootDigest !== attemptContext.expectedPriorRootDigest
      ) {
        this.#custodyGateState = CustodyGateState.RECONCILING_CUSTODY;
        throw new Error('INVALID_COMMITTED_CONFIRMATION: authoritative store returned inconsistent commit confirmation');
      }

      // Post-await leadership completion check (R29-B1)
      this.#leadershipFence.assertPostAwaitEligibility(attemptContext);

      // Phase 7: Synchronous Local Pointer Replacement (Post-Commit Linearization Point)
      this.#custodyRoot = candidateRoot;
      this.#custodyGateState = CustodyGateState.IDLE;
    } finally {
      this.#leadershipFence.clearActiveAttempt();
      // Phase 8: Guaranteed Lock Release
      this.#leadershipFence.releasePublicationLock();
    }
  }

  /**
   * Authoritative Reconciliation Protocol for UNCERTAIN Outcomes (R29-B2 / R29-B5).
   */
  public async reconcileAuthoritativeCustody(): Promise<void> {
    if (this.#custodyGateState !== CustodyGateState.RECONCILING_CUSTODY) {
      return;
    }
    const attempt = this.#leadershipFence.activeAttempt;
    if (!attempt) {
      const storeState = await this.#authoritativeStore.getAuthoritativeRoot();
      this.#custodyRoot = storeState.root;
      this.#custodyGateState = CustodyGateState.IDLE;
      return;
    }

    const query: CustodyReconciliationQuery = Object.freeze({
      fencingToken: attempt.fencingToken,
      attemptId: attempt.attemptId, // Exact immutable attemptId reused across publication and reconciliation (R31-5)
      candidateRootDigest: attempt.candidateRootDigest,
      expectedPriorSequence: attempt.expectedPriorSequence,
      expectedPriorRootDigest: attempt.expectedPriorRootDigest,
    });

    const result = await this.#authoritativeStore.reconcilePublication(query);

    if (result.status === 'RECONCILED_COMMITTED') {
      this.#leadershipFence.assertPostAwaitEligibility(attempt);
      this.#custodyRoot = result.committedRoot;
      this.#custodyGateState = CustodyGateState.IDLE;
      return;
    }

    if (result.status === 'NOT_COMMITTED_FINAL') {
      // Store atomically persisted a durable terminal fencing tombstone bound to the complete 8D tuple (R32-2, R33-2)
      if (result.publicationIdentity.attemptId !== attempt.attemptId) {
        throw new Error('INVALID_RECONCILIATION_CONFIRMATION: tombstone attempt identity mismatch');
      }
      this.#custodyRoot = result.authoritativeCurrentRoot;
      this.#custodyGateState = CustodyGateState.IDLE;
      return;
    }

    if (result.status === 'UNKNOWN_IN_PROGRESS') {
      // Store cannot prove terminal non-commitment; original request may still be in flight; remain fail-closed (R32-2)
      throw new Error(`CUSTODY_GATE_RECONCILING: store reconciliation outcome in progress: ${result.reason}`);
    }

    if (result.status === 'RECONCILED_CONFLICT') {
      this.#leadershipFence.stepDown(this.#leadershipFence.clusterTerm);
      throw new Error(`CUSTODY_RECONCILIATION_CONFLICT: sequence collision detected: ${result.reason}`);
    }

    if (result.status === 'RECONCILED_FENCED_REJECTED') {
      this.#leadershipFence.stepDown(result.higherTerm);
      throw new Error(`CUSTODY_RECONCILIATION_FENCED: store rejected reconciliation: ${result.reason}`);
    }
  }

  public static validateCandidateRootPrePublicationInvariants(candidateRoot: AuthoritativeCustodyRoot): void {
    // 1. Bijection between retiredSettlementPins and consumedSettlementEvidence (R25-5)
    if (candidateRoot.retiredSettlementPins.size !== candidateRoot.consumedSettlementEvidence.size) {
      throw new Error(
        `STATE_INCONSISTENT_REPLAY_EVIDENCE_CARDINALITY_MISMATCH: retired pins (${candidateRoot.retiredSettlementPins.size}) != consumed evidence (${candidateRoot.consumedSettlementEvidence.size})`
      );
    }

    for (const [pinId, pin] of candidateRoot.retiredSettlementPins) {
      const expectedKey = `${pin.generation}:${pin.barrierIdentity}:${pinId}`;
      if (!candidateRoot.consumedSettlementEvidence.has(expectedKey)) {
        throw new Error(`STATE_INCONSISTENT_REPLAY_KEY_BIJECTION_VIOLATION: missing key '${expectedKey}'`);
      }
    }

    // 2. Maximum Capacity Fencing
    if (candidateRoot.retiredSettlementPins.size > 5000) {
      throw new Error('RETIRED_PINS_CAPACITY_CORRUPTED: exceeds 5000 entries');
    }
    if (candidateRoot.retiredRetryLeases.size > 5000) {
      throw new Error('RETIRED_LEASES_CAPACITY_CORRUPTED: exceeds 5000 entries');
    }

    // 3. Safe Integer & Monotonic Non-Negative Floors
    if (!Number.isSafeInteger(candidateRoot.publishedGeneration) || candidateRoot.publishedGeneration < 0) {
      throw new Error('STATE_INCONSISTENT_INVALID_PUBLISHED_GENERATION');
    }
    if (!Number.isSafeInteger(candidateRoot.custodyGenerationFloor) || candidateRoot.custodyGenerationFloor < 0) {
      throw new Error('STATE_INCONSISTENT_INVALID_FLOOR');
    }
    if (!Number.isSafeInteger(candidateRoot.auditTombstoneFloor) || candidateRoot.auditTombstoneFloor < 0) {
      throw new Error('STATE_INCONSISTENT_INVALID_AUDIT_FLOOR');
    }
    if (candidateRoot.auditTombstoneFloor > candidateRoot.custodyGenerationFloor) {
      throw new Error('STATE_INCONSISTENT_AUDIT_FLOOR_EXCEEDS_CUSTODY_FLOOR');
    }
    if (candidateRoot.custodyGenerationFloor > candidateRoot.publishedGeneration) {
      throw new Error('STATE_INCONSISTENT_FLOOR_EXCEEDS_PUBLISHED_GENERATION');
    }
  }

  public static assertRetentionMapContiguity(
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

  public static assertAuditHistoryContiguity(
    auditMap: ImmutableMap<number, GenerationAuditRecord>,
    auditFloor: number,
    publishedGeneration: number,
    custodyFloor: number,
    publicationResults?: ImmutableMap<number, PublicationResult>,
  ): void {
    if (!Number.isSafeInteger(auditFloor) || auditFloor < 0) {
      throw new Error('STATE_INCONSISTENT_INVALID_AUDIT_FLOOR');
    }
    if (!Number.isSafeInteger(publishedGeneration) || publishedGeneration < 0) {
      throw new Error('STATE_INCONSISTENT_INVALID_PUBLISHED_GENERATION');
    }
    if (auditFloor > publishedGeneration) {
      throw new Error('STATE_INCONSISTENT_AUDIT_FLOOR_EXCEEDS_PUBLISHED_GENERATION');
    }

    if (auditFloor === publishedGeneration) {
      if (auditMap.size !== 0) {
        throw new Error('STATE_INCONSISTENT_EMPTY_AUDIT_RANGE_NON_EMPTY_MAP');
      }
      return;
    }

    const expectedCount = publishedGeneration - auditFloor;
    if (auditMap.size !== expectedCount) {
      throw new Error('STATE_INCONSISTENT_AUDIT_MAP_NON_CONTIGUOUS');
    }

    for (let g = auditFloor + 1; g <= publishedGeneration; g++) {
      if (!auditMap.has(g)) {
        throw new Error(`STATE_INCONSISTENT_AUDIT_KEY_GAP_DETECTED: missing generation ${g}`);
      }
      const record = auditMap.get(g)!;
      if (record.generation !== g) {
        throw new Error(`STATE_INCONSISTENT_AUDIT_RECORD_CORRUPTED: generation field mismatch at key ${g}`);
      }

      if (g <= custodyFloor && record.kind !== 'COMPACT_TOMBSTONE') {
        throw new Error(`STATE_INCONSISTENT_EVICTED_RESULT_WITHOUT_TOMBSTONE: key ${g}`);
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
      let pinnedByLease = false;
      for (const lease of root.activeRetryLeases.values()) {
        if (lease.generation === record.generation && lease.expiresAtMonotonicMs > monotonicNow) {
          pinnedByLease = true;
          break;
        }
      }
      if (pinnedByLease) continue;

      // 2. Must not be pinned by active settlement pin
      let pinnedByPin = false;
      for (const pin of root.activeSettlementPins.values()) {
        if (pin.generation === record.generation) {
          pinnedByPin = true;
          break;
        }
      }
      if (pinnedByPin) continue;

      // 3. Must not be pinned by pending settlement obligation
      if (root.pendingSettlementObligations.has(record.generation)) {
        continue;
      }

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
      let pinnedByLease = false;
      for (const lease of root.activeRetryLeases.values()) {
        if (lease.generation === record.generation && lease.expiresAtMonotonicMs > monotonicNow) {
          pinnedByLease = true;
          break;
        }
      }
      if (pinnedByLease) continue;

      let pinnedByPin = false;
      for (const pin of root.activeSettlementPins.values()) {
        if (pin.generation === record.generation) {
          pinnedByPin = true;
          break;
        }
      }
      if (pinnedByPin) continue;

      if (root.pendingSettlementObligations.has(record.generation)) {
        continue;
      }

      return [leaseId, record];
    }
    return undefined;
  }

  private _getOrAllocateSettlementPin(
    currentRoot: AuthoritativeCustodyRoot,
    generation: number,
    barrierIdentity: string,
    ownerToken: CustodyOwnershipToken,
    drainerGeneration: number
  ): { pinId: string; nextPins: ImmutableMap<string, ImmutableSettlementPin> } {
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

    const seq = ++this.#pinSequenceCounter;
    const pinId = `pin:${this.#coordinatorBootId}:${generation}:${barrierIdentity}:${seq}`;

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
      allocatedBootEpoch: this.#coordinatorBootEpoch,
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

    const seq = ++this.#leaseSequenceCounter;
    const leaseId = `lease:${this.#coordinatorBootId}:${generation}:${consumerId}:${seq}`;

    if (root.activeRetryLeases.has(leaseId)) {
      throw new Error('RETRY_LEASE_ID_COLLISION');
    }

    const monotonicNow = performance.now();
    const newLease: ImmutableRetryLease = Object.freeze({
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
      allocatedBootEpoch: this.#coordinatorBootEpoch,
    });

    const candidateLeases = root.activeRetryLeases.set(leaseId, newLease);
    return { candidateLeases, leaseId };
  }

  public async releaseRetryLease(params: ReleaseRetryLeaseParams): Promise<{ leaseId: string; status: 'RELEASED' | 'ALREADY_RELEASED' }> {
    const { leaseId, consumerId, clientRequestId, callerToken, transactionId, operationIdentity, barrierIdentity } = params;

    if (leaseId.startsWith('pin:')) {
      throw new Error('LEASE_TYPE_MISMATCH: identifier is a settlement pin, not a retry lease');
    }
    if (!leaseId.startsWith('lease:')) {
      throw new Error(`RESOURCE_NOT_FOUND: unknown retry lease identifier '${leaseId}'`);
    }

    const currentRoot = this.#custodyRoot;
    const activeLease = currentRoot.activeRetryLeases.get(leaseId);

    if (!activeLease) {
      const retiredLease = currentRoot.retiredRetryLeases.get(leaseId);
      if (retiredLease) {
        const expectedPrefix = `lease:${retiredLease.allocatedBootEpoch.clusterTerm}.${retiredLease.allocatedBootEpoch.bootGeneration}.${retiredLease.allocatedBootEpoch.bootInstanceId}:`;
        if (!leaseId.startsWith(expectedPrefix)) {
          throw new Error('RETIRED_LEASE_BOOT_EPOCH_MISMATCH: identifier does not match recorded allocation epoch');
        }

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
      throw new Error(`RESOURCE_NOT_FOUND: retry lease '${leaseId}' was never issued or is unknown`);
    }

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

    if (currentRoot.retiredRetryLeases.size > 5000) {
      throw new Error('RETIRED_LEASES_CAPACITY_CORRUPTED: pre-existing retired leases count exceeds 5000');
    }

    const candidateActiveLeases = currentRoot.activeRetryLeases.delete(leaseId);
    const candidateRetiredLeasesMap = new Map(currentRoot.retiredRetryLeases.entries());

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

    const token = this._nextFencingToken();

    // Execute internal two-phase publication (R28-B)
    await this._executeTwoPhasePublication(token, candidateRoot);
    return { leaseId, status: 'RELEASED' };
  }

  public async releaseSettlementPin(
    pinId: string,
    if (this.#custodyGateState === CustodyGateState.RECONCILING_CUSTODY) {
      throw new Error('CUSTODY_GATE_RECONCILING: custody state uncertain; mutations prohibited pending authoritative reconciliation');
    }

    settlementEvidence: AuthoritativeSettlementEvidence
  ): Promise<{ pinId: string; status: 'RELEASED' | 'ALREADY_RELEASED' }> {
    if (pinId.startsWith('lease:')) {
      throw new Error('PIN_TYPE_MISMATCH: identifier is a retry lease, not a settlement pin');
    }
    if (!pinId.startsWith('pin:')) {
      throw new Error(`RESOURCE_NOT_FOUND: unknown settlement pin identifier '${pinId}'`);
    }

    SettlementEvidenceFactory.assertAuthoritativeEvidenceProvenance(settlementEvidence);

    const currentRoot = this.#custodyRoot;
    const activePin = currentRoot.activeSettlementPins.get(pinId);

    if (!activePin) {
      const retiredPin = currentRoot.retiredSettlementPins.get(pinId);
      if (retiredPin) {
        const expectedPrefix = `pin:${retiredPin.allocatedBootEpoch.clusterTerm}.${retiredPin.allocatedBootEpoch.bootGeneration}.${retiredPin.allocatedBootEpoch.bootInstanceId}:`;
        if (!pinId.startsWith(expectedPrefix)) {
          throw new Error('RETIRED_PIN_BOOT_EPOCH_MISMATCH: identifier does not match recorded allocation epoch');
        }

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

        if (!areEqualByteArrays(retiredPin.evidenceDigest, settlementEvidence.evidenceDigest)) {
          throw new Error('SETTLEMENT_EVIDENCE_DIGEST_MISMATCH: evidence digest does not match recorded retired pin disposition');
        }

        return { pinId, status: 'ALREADY_RELEASED' };
      }
      throw new Error(`RESOURCE_NOT_FOUND: settlement pin '${pinId}' was never issued or is unknown`);
    }

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

    const replayKey = `${settlementEvidence.generation}:${settlementEvidence.barrierIdentity}:${pinId}`;
    if (currentRoot.consumedSettlementEvidence.has(replayKey)) {
      throw new Error(`SETTLEMENT_EVIDENCE_REPLAY_DETECTED: evidence for replay key '${replayKey}' already consumed in custody root`);
    }

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

    if (candidateRetiredPins.size === 5000) {
      const eligibleEntry = this._findOldestEligibleRetiredPin(currentRoot, candidateRetiredPins);
      if (!eligibleEntry) {
        throw new Error('RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE: all 5000 retired settlement pins are protected by active leases, pins, or obligations');
      }
      const [oldestPinId, oldestRecord] = eligibleEntry;
      const oldestReplayKey = `${oldestRecord.generation}:${oldestRecord.barrierIdentity}:${oldestPinId}`;

      if (!candidateConsumed.has(oldestReplayKey)) {
        throw new Error(`REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED: replay key '${oldestReplayKey}' missing for retired pin '${oldestPinId}'`);
      }

      candidateRetiredPins.delete(oldestPinId);
      candidateConsumed.delete(oldestReplayKey);
    }

    candidateRetiredPins.set(pinId, retiredPinRecord);
    candidateConsumed.add(replayKey);

    const candidateRoot: AuthoritativeCustodyRoot = Object.freeze({
      ...currentRoot,
      activeSettlementPins: new ImmutableMap(candidatePinsMap.entries()),
      retiredSettlementPins: new ImmutableMap(candidateRetiredPins.entries()),
      consumedSettlementEvidence: new ImmutableSet(candidateConsumed),
    });

    const token = this._nextFencingToken();

    // Execute internal two-phase publication (R28-B)
    await this._executeTwoPhasePublication(token, candidateRoot);
    return { pinId, status: 'RELEASED' };
  }

  public async publishReleaseAndObligation(
    preparedEvidence: PreparedEvidenceContext,
    expectedOwner: CustodyOwnershipToken,
    barrierIdentity: string,
    retryContext?: RetryRequestContext,
  ): Promise<PublicationResponse> {
    const currentRoot = this.#custodyRoot;
    const g = preparedEvidence.releasedCustodyGeneration;

    // Phase 1: Domain Boundary Validation
    if (!Number.isSafeInteger(g) || g <= 0) {
      throw new Error(`INVALID_GENERATION_DOMAIN: generation ${g} is out of permitted range [1, MAX_SAFE_INTEGER]`);
    }
    if (g === Number.MAX_SAFE_INTEGER) {
      throw new Error(`GENERATION_SEQUENCE_EXHAUSTED: generation reached theoretical limit ${g}`);
    }

    const safeOwner = cloneAndFreezeOwnerToken(expectedOwner);
    const safeTxId = safeOwner.transactionId;
    const safeOpId = safeOwner.operationIdentity;
    const safeBarrierId = barrierIdentity;

    // Case 3: Retried Operation
    if (g <= currentRoot.publishedGeneration) {
      validateRetryRequestContext(retryContext);

      // Case 3A: Published and In-Flight Barrier Pending
      if (currentRoot.pendingSettlementObligations.has(g)) {
        const existingObligation = currentRoot.pendingSettlementObligations.get(g)!;
        const cachedResult = currentRoot.publicationResults.get(g);
        if (!cachedResult) {
          throw new Error('STATE_INCONSISTENT_PENDING_OBLIGATION_MISSING_RESULT');
        }

        const auditRecord = currentRoot.generationAuditHistory.get(g);
        if (!auditRecord || auditRecord.kind !== 'PENDING_PUBLICATION') {
          throw new Error('STATE_INCONSISTENT_PENDING_OBLIGATION_AUDIT_MISMATCH');
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
          await this._executeTwoPhasePublication(nextRoot);
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
          await this._executeTwoPhasePublication(nextRoot);
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
            throw new Error('CUSTODY_RECEIPT_EXPIRED_TOKEN_MISMATCH');
          }
          throw new Error(`CUSTODY_RECEIPT_EXPIRED: generation ${g} retained receipt evicted; audit tombstone confirms outcome ${tombstone.terminalOutcome}`);
        }
        throw new Error(`CUSTODY_RECEIPT_EXPIRED: generation ${g} has been pruned beyond audit tombstone floor ${currentRoot.auditTombstoneFloor}`);
      }

      throw new Error(`STATE_INCONSISTENT_RETENTION_INDEX_CORRUPTED: generation ${g} <= published ${currentRoot.publishedGeneration} but missing from results and obligations`);
    }

    // Case 1: First Publication
    if (g !== currentRoot.publishedGeneration + 1) {
      throw new Error(`INVALID_PUBLICATION_SEQUENCE: attempted out-of-order generation ${g}; expected ${currentRoot.publishedGeneration + 1}`);
    }

    if (currentRoot.inFlightRecord === null) {
      throw new Error('INVALID_CUSTODY_GATE_STATE_NO_IN_FLIGHT: cannot publish without active in-flight custody');
    }

    const inFlightToken = currentRoot.inFlightRecord.token;
    if (
      !isEquivalentOwnerToken(inFlightToken, safeOwner) ||
      inFlightToken.transactionId !== safeTxId ||
      inFlightToken.operationIdentity !== safeOpId ||
      inFlightToken.barrierIdentity !== safeBarrierId ||
      inFlightToken.drainerOwnershipGeneration !== safeOwner.drainerOwnershipGeneration
    ) {
      throw new Error('CUSTODY_PUBLICATION_IN_FLIGHT_TOKEN_MISMATCH');
    }

    const { pinId, nextPins: candidatePins } = this._getOrAllocateSettlementPin(
      currentRoot,
      g,
      safeBarrierId,
      safeOwner,
      safeOwner.drainerOwnershipGeneration
    );

    const obligationId = `obl:${this.#coordinatorBootId}:${g}:${safeBarrierId}`;
    const nowTimestamp = Date.now();
    const monotonicNow = performance.now();

    const releaseReceipt: DrainerCustodyReleaseReceipt = Object.freeze({
      [DRAINER_RELEASE_BRAND]: true as const,
      releasedCustodyGeneration: g,
      releasedAtTimestamp: nowTimestamp,
      identity: safeOwner,
      monotonicReleasedMs: monotonicNow,
    });

    const obligation: PendingSettlementObligation = Object.freeze({
      obligationId,
      generation: g,
      barrierIdentity: safeBarrierId,
      ownerToken: safeOwner,
      transactionId: safeTxId,
      operationIdentity: safeOpId,
      drainerOwnershipGeneration: safeOwner.drainerOwnershipGeneration,
      publishedCustodyGeneration: g,
      preAllocatedReleaseReceipt: releaseReceipt,
      preparedReconciliationReceipt: deepCloneAndFreezeValue(preparedEvidence.preparedReconciliationReceipt),
      preparedArchiveReceipt: deepCloneAndFreezeValue(preparedEvidence.preparedArchiveReceipt),
      preparedExecutionReceipt: deepCloneAndFreezeValue(preparedEvidence.preparedExecutionReceipt),
      publishedAtTimestamp: nowTimestamp,
      monotonicPublishedMs: monotonicNow,
    });

    const publicationResult: PublicationResult = Object.freeze({
      releaseReceipt,
      obligation,
    });

    const pendingAuditRecord: ImmutablePendingAuditRecord = Object.freeze({
      kind: 'PENDING_PUBLICATION' as const,
      generation: g,
      expectedOwner: safeOwner,
      transactionId: safeTxId,
      operationIdentity: safeOpId,
      barrierIdentity: safeBarrierId,
      drainerOwnershipGeneration: safeOwner.drainerOwnershipGeneration,
      publishedAtTimestamp: nowTimestamp,
      monotonicPublishedMs: monotonicNow,
    });

    const candidateResultsMap = new Map(currentRoot.publicationResults.entries());
    const candidateAuditHistory = new Map(currentRoot.generationAuditHistory.entries());
    let candidateFloor = currentRoot.custodyGenerationFloor;
    let candidateAuditFloor = currentRoot.auditTombstoneFloor;

    const count = g - 1 - candidateFloor;
    if (count > 5000) {
      throw new Error('RETENTION_CAPACITY_CORRUPTED: pre-existing publication count exceeds 5000');
    }

    if (count === 5000) {
      const g_oldest = candidateFloor + 1;
      const oldestResult = candidateResultsMap.get(g_oldest);
      if (!oldestResult) {
        throw new Error('STATE_INCONSISTENT_RESULT_EVICTION_GAP');
      }

      for (const lease of currentRoot.activeRetryLeases.values()) {
        if (lease.generation === g_oldest && lease.expiresAtMonotonicMs > monotonicNow) {
          throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED: oldest result protected by active retry lease');
        }
      }
      for (const pin of currentRoot.activeSettlementPins.values()) {
        if (pin.generation === g_oldest) {
          throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED: oldest result protected by active settlement pin');
        }
      }
      if (currentRoot.pendingSettlementObligations.has(g_oldest)) {
        throw new Error('CUSTODY_PUBLICATION_BACKPRESSURE_EXHAUSTED: oldest result protected by pending settlement obligation');
      }

      const existingAudit = candidateAuditHistory.get(g_oldest);
      if (!existingAudit) {
        throw new Error('STATE_INCONSISTENT_AUDIT_RECORD_MISSING_FOR_EVICTION');
      }
      if (existingAudit.kind !== 'TERMINAL_SETTLEMENT') {
        throw new Error('STATE_INCONSISTENT_EVICTION_AUDIT_NOT_SETTLED');
      }

      const tombstone: ImmutableCompactAuditTombstone = Object.freeze({
        kind: 'COMPACT_TOMBSTONE' as const,
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

    AuthoritativeCustodyCoordinator.assertRetentionMapContiguity(candidateResults, candidateFloor, g);
    AuthoritativeCustodyCoordinator.assertAuditHistoryContiguity(candidateAudit, candidateAuditFloor, g, candidateFloor, candidateResults);

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

    const token = this._nextFencingToken();

    // Execute internal two-phase publication (R28-B)
    await this._executeTwoPhasePublication(token, candidateRoot);
    return { publicationResult, retryLeaseId: null };
  }
}
```

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Under single-realm JavaScript execution:
- Candidate-root preparation, invariant verification, and reference replacement occur synchronously within one microtask turn.
- Mutual exclusion is enforced via `#publicationLock`: concurrent calls within the same process fail deterministically with `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.
- Publication operates via a two-phase commit: Phase 1 asynchronously awaits Authoritative Custody Store fenced CAS confirmation; Phase 2 executes the synchronous local pointer replacement `this.#custodyRoot = candidateRoot`.
- No arbitrary caller callbacks are exposed or executed within the publication lock.
- No other event loop callback can observe or mutate `this.#custodyRoot` between confirmed authoritative commit and local replacement.
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
| **Phase 10: Authoritative CAS** | Stale token / expired lease / term loss | Fail-closed throw (`STALE_COORDINATOR_LEADERSHIP_FENCED`) | Root untouched; authoritative CAS rejected |
| **Phase 11: Return Ref** | Return deeply cached reference | Infallible | Zero post-publication allocation |

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

## 15. Exhaustive Failure and Retry Matrix (138 Deterministic Cases)

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
| **43** | Candidate root retired pins !== consumed evidence | `ANY` | Coordinator | ANY | Throw `REPLAY_KEY_BIJECTION_CARDINALITY_MISMATCH` | Commit desynchronized root |
| **44** | Candidate root retired pin missing replay key | `ANY` | Coordinator | ANY | Throw `REPLAY_KEY_BIJECTION_VIOLATION` | Commit unmapped replay key |
| **45** | Consensus leadership lost prior to pointer swap | `RECONCILING` | Fenced Leader | PENDING | Throw `STALE_COORDINATOR_LEADERSHIP_FENCED` | Commit publication |
| **46** | Delayed release of unretired prior-epoch resource | `ANY` | Any | ANY | Throw `RESOURCE_NOT_FOUND` | Authorize release |
| **47** | Restored retired pin from earlier boot epoch | `SETTLED` | Coordinator | RESOLVED | Verify boot epoch & digest; return `ALREADY_RELEASED` | Reject valid idempotent retry |
| **48** | Consumer attempts property access on `#inner` | `ANY` | Any | ANY | Rejected at runtime by ECMAScript syntax | Access private backing Map |
| **49** | Caller attempts property mutation on retrieved `ImmutableMap` value | `ANY` | Any | ANY | Mutation rejected by frozen object / TypeError | In-place mutation of stored custody record |
| **50** | Caller mutates bytes of retrieved `ownerSignature` or `evidenceDigest` | `ANY` | Any | ANY | Egress defensive cloning keeps internal buffer pristine | Shared buffer corruption of custody root |
| **51** | Caller attempts collection mutation via `entries()` or `values()` iterator | `ANY` | Any | ANY | Iterators yield deeply frozen defensive clones; root untouched | In-place collection corruption via iterator |
| **52** | Non-primitive key or element passed to `ImmutableMap` or `ImmutableSet` | `ANY` | Any | ANY | Throw `INVALID_COLLECTION_KEY_TYPE` / `ELEMENT_TYPE` | Admit mutable reference as key/element |
| **53** | Concurrent publication attempted during active publication lock | `PUBLISHING` | Coordinator | PENDING | Throw `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION` | Interleave concurrent candidate roots |
| **54** | Expired lease or stepdown during publication | `PUBLISHING` | Fenced Leader | PENDING | Throw `STALE_COORDINATOR_LEADERSHIP_FENCED`; root untouched | Commit stale root after lease expiry |
| **55** | Symbol-keyed property holds mutable record or array | `ANY` | Any | ANY | Recursively clone and freeze symbol property value | Copy symbol property by reference |
| **56** | Object instance with custom prototype passed to custody collection | `ANY` | Any | ANY | Throw `UNSUPPORTED_PROTOTYPE_INSTANCE` | Strip prototype or treat as plain record |
| **57** | Object containing getter/setter accessors passed to custody collection | `ANY` | Any | ANY | Reject with `ACCESSOR_PROPERTIES_PROHIBITED` without executing getter | Execute getter or store accessor |
| **58** | Cyclic object graph passed to custody cloning | `ANY` | Any | ANY | Detect cycle via `activeStack` and throw `CIRCULAR_REFERENCE_DETECTED` | Infinite recursion / call stack overflow |
| **59** | Prohibited binary/temporal types (`Date`, `ArrayBuffer`, `DataView`) passed | `ANY` | Any | ANY | Throw `UNSUPPORTED_BINARY_VALUE_TYPE` or `UNSUPPORTED_TEMPORAL_VALUE_TYPE` | Store empty record or raw buffer |
| **60** | Egress `Uint8Array` copy mutated by caller | `ANY` | Any | ANY | Egress is detached mutable copy; root backing store unaffected | Mutate internal custody byte buffer |
| **61** | Lease expires after local check but before publication | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS rejects write; local custody degraded | Commit stale write to authoritative store |
| **62** | Coordinator resumes after OS process pause with expired lease | `PUBLISHING` | Coordinator | PENDING | Local check or Authoritative Store rejects with `STALE_COORDINATOR_LEADERSHIP_FENCED` | Execute publication with expired lease |
| **63** | Two independent coordinator instances attempt publication under different terms | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS commits higher term; rejects lower term | Allow split-brain dual publication |
| **64** | Asynchronous commit callback passed to publication execution | `PUBLISHING` | Coordinator | PENDING | Replaced with internal two-phase publication; zero caller callbacks | Asynchronous escape from publication lock |
| **65** | Leadership term changes during candidate root preparation | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS detects term mismatch; rejects publication | Overwrite newly elected leader's state |
| **66** | Stale coordinator attempts post-term recovery mutation | `BLOCKED` | Stale Leader | PENDING | Authoritative Store rejects token; coordinator latches `CUSTODY_RECONCILIATION_BLOCKED` | Stale mutation of shared recovery state |
| **67** | Shared acyclic references (DAG) passed to deep cloning | `ANY` | Any | ANY | Memoize clone via `cloneMap`; preserve reference identity; clone cleanly | False rejection as circular reference |
| **68** | Sparse array with missing indices passed to custody cloning | `ANY` | Any | ANY | Verify index density; reject holes with `SPARSE_ARRAY_PROHIBITED` | Admit uninitialized array holes |
| **69** | Function value passed in any position | `ANY` | Any | ANY | Immediate upfront check rejects with `UNSUPPORTED_FUNCTION_VALUE_TYPE` | Bypass rejection and store function |
| **70** | Custom prototype defines hostile getter on constructor | `ANY` | Any | ANY | Reference check rejects without reading `proto.constructor`; getter uncalled | Invoke hostile getter during validation |
| **71** | Arbitrary symbol-keyed properties in canonical snapshot payloads | `ANY` | Any | ANY | Reject with `SYMBOL_PROPERTIES_PROHIBITED_IN_CANONICAL_VALUES` | Produce invalid/unmapped RFC 8785 digest |
| **72** | Asynchronous authoritative store write delayed in flight | `PUBLISHING` | Coordinator | PENDING | Local pointer replacement withheld until confirmed `COMMITTED` outcome | Prematurely replace local cached pointer |
| **73** | Authoritative store returns `UNCERTAIN` or `FENCED_REJECTED` | `PUBLISHING` | Coordinator | PENDING | Local root unchanged; latch `RECONCILING_CUSTODY` or `BLOCKED` | Silently diverge local and store state |
| **74** | Candidate presents invalid term, boot generation, or sequence gap | `PUBLISHING` | Coordinator | PENDING | Authoritative store validates all 4 token dimensions; rejects invalid write | Admit out-of-order or superseded write |
| **75** | Non-empty `Uint8Array` ingested into custody storage | `ANY` | Any | ANY | Clone bytes into detached local array without calling `Object.freeze()`; encapsulate privately | Call `Object.freeze()` on typed array throwing runtime `TypeError` |
| **76** | Empty `Uint8Array` ingested into custody storage | `ANY` | Any | ANY | Clone 0-byte buffer into detached array; preserve empty byte array | Reject or corrupt empty byte arrays |
| **77** | Caller mutates returned egress `Uint8Array` copy | `ANY` | Consumer | ANY | Egress reads return fresh detached copies; internal custody bytes unaffected | Expose mutable internal buffer reference to external callers |
| **78** | Repeated shared references to same `Uint8Array` in DAG | `ANY` | Any | ANY | Memoize clone via `cloneMap`; return memoized instance; preserve DAG topology | Throw false `CIRCULAR_REFERENCE_DETECTED` on acyclic shared bytes |
| **79** | Snapshot export and import of binary byte arrays | `ANY` | Any | ANY | Encode via `encodeCanonicalBinaryHex`; import creates independent local buffer | Ambiguous base64 padding or length corruption |
| **80** | Canonical digest computation on equivalent byte contents | `ANY` | Any | ANY | Equivalent byte sequences produce identical SHA-256 digest regardless of view offset | Produce non-deterministic digests across buffer views |
| **81** | Prohibited binary backing storage (`SharedArrayBuffer`, raw `ArrayBuffer`, `DataView`) | `ANY` | Any | ANY | Detect via brand and prototype checks; reject with `UNSUPPORTED_BINARY_VALUE_TYPE` | Permit shared concurrent memory or unaligned views |
| **82** | Array index zero defines accessor property with getter | `ANY` | Any | ANY | Inspect `Object.getOwnPropertyDescriptor`; reject with `ACCESSOR_PROPERTIES_PROHIBITED`; getter not invoked | Read `arr[0]` executing user-controlled getter |
| **83** | Array later index defines accessor property with getter | `ANY` | Any | ANY | Inspect descriptor for index $i$; reject with `ACCESSOR_PROPERTIES_PROHIBITED`; getter not invoked | Read `arr[i]` executing user-controlled getter |
| **84** | Array indexed element defined as non-enumerable | `ANY` | Any | ANY | Inspect descriptor; reject with `NON_ENUMERABLE_PROPERTY_PROHIBITED` | Omit or silently drop non-enumerable array element |
| **85** | Missing array index (sparse array hole `[1, , 3]`) | `ANY` | Any | ANY | Missing descriptor check detects hole; reject with `SPARSE_ARRAY_PROHIBITED` | Admit sparse array with undefined holes |
| **86** | Array defines non-canonical digit property `"01"` | `ANY` | Any | ANY | Strict index verification `String(u32) === key` rejects `"01"` with `UNSUPPORTED_ARRAY_PROPERTY` | Admit non-index named properties on arrays |
| **87** | Array defines out-of-range digit property `"4294967295"` | `ANY` | Any | ANY | Strict index check `u32 !== 0xFFFFFFFF` rejects with `UNSUPPORTED_ARRAY_PROPERTY` | Admit non-index $2^{32}-1$ property on arrays |
| **88** | Valid dense array with nested records and DAG shared references | `ANY` | Any | ANY | Deeply clone all indexed elements via `Object.defineProperty`; freeze array cleanly | Throw false validation error or permit reference sharing |
| **89** | Object defines own data property named `'__proto__'` | `ANY` | Any | ANY | Define property on clone via `Object.defineProperty`; preserve as own data property; prototype untouched | Use assignment `clone[k] = v` invoking legacy prototype setter |
| **90** | Object defines own data property named `'constructor'` or `'prototype'` | `ANY` | Any | ANY | Define property via `Object.defineProperty`; verify data descriptor; getter uncalled | Execute user-defined constructor or prototype getter |
| **91** | Negative zero (`-0`) passed in numeric property | `ANY` | Any | ANY | Canonicalize `Object.is(v, -0)` to `0` for RFC 8785 canonical serialization stability | Emit `-0` causing non-canonical snapshot hashes |
| **92** | Positive and negative safe integer boundaries | `ANY` | Any | ANY | Accept `Number.MAX_SAFE_INTEGER` and `Number.MIN_SAFE_INTEGER` as valid numeric primitives | Reject valid safe integers |
| **93** | Unsafe integer outside safe range (`Number.MAX_SAFE_INTEGER + 1`) | `ANY` | Any | ANY | `!Number.isSafeInteger(v)` detects precision loss; reject with `UNSAFE_INTEGER_PROHIBITED` | Admit precision-degraded unsafe integers |
| **94** | Finite fractional floating-point numbers (`3.14159`) | `ANY` | Any | ANY | `Number.isFinite(v)` accepts finite floats; preserve exact value | Reject valid finite floating-point numbers |
| **95** | Non-finite numeric primitives (`NaN`, `+Infinity`, `-Infinity`) | `ANY` | Any | ANY | Detect non-finite numbers; reject with `INVALID_NUMERIC_VALUE` | Permit NaN or Infinity causing invalid JSON serialization |
| **96** | Cross-realm `Uint8Array` instance passed to cloner | `ANY` | Any | ANY | Intrinsic brand check `[object Uint8Array]` recognizes view; clone to local detached buffer | Reject valid cross-realm typed array or share buffer |
| **97** | Hostile ECMAScript `Proxy` object passed to cloner | `ANY` | Caller | ANY | Documented trust boundary: Proxy is outside contract; reflective operations trigger traps; fail-closed | Claim fictional Proxy detection without trap execution |
| **98** | Leadership term advances while store publication is pending | `PUBLISHING` | Coordinator | PENDING | Post-await completion check detects term mismatch; throws `CUSTODY_PUBLICATION_STALE_LEADERSHIP` | Update local `#custodyRoot` after losing cluster leadership |
| **99** | Store commits publication but local coordinator lease expires before ack arrives | `PUBLISHING` | Stale Leader | PENDING | Post-await fence verifies lease expiry; withholds local pointer swap; new leader reconciles | Local coordinator applies write as stale leader |
| **100** | Stale partitioned coordinator receives delayed `COMMITTED` confirmation | `BLOCKED` | Stale Leader | PENDING | Coordinator steps down; discards response; local cached root remains untouched | Overwrite active cluster state with stale local cache |
| **101** | Store commits sequence $S$ but acknowledgement is lost in transit | `RECONCILING` | Coordinator | PENDING | Coordinator runs `reconcilePublication`; store returns `RECONCILED_COMMITTED`; coordinator updates root | Re-publish as duplicate sequence or fail closed permanently |
| **102** | Idempotent retry submitted with identical token, sequence, and root digest | `RECONCILING` | Coordinator | PENDING | Store matches exact token and digest; returns original committed confirmation | Treat identical idempotent retry as duplicate conflict |
| **103** | Conflicting publication submitted with same token/sequence but different digest | `RECONCILING` | Coordinator | PENDING | Store detects digest conflict; rejects with `CONFLICTING_DIGEST_AT_SEQUENCE` | Overwrite committed record with conflicting candidate |
| **104** | Authoritative store confirmation returns mismatched sequence | `PUBLISHING` | Coordinator | PENDING | Coordinator detects sequence mismatch; throws `INVALID_COMMITTED_CONFIRMATION`; enters `RECONCILING` | Accept mismatched sequence confirmation |
| **105** | Authoritative store confirmation returns mismatched candidate root digest | `PUBLISHING` | Coordinator | PENDING | Coordinator detects digest mismatch; throws `INVALID_COMMITTED_CONFIRMATION`; enters `RECONCILING` | Accept mismatched root confirmation |
| **106** | Authoritative store confirmation returns mismatched prior root digest | `PUBLISHING` | Coordinator | PENDING | Coordinator detects prior digest mismatch; throws `INVALID_COMMITTED_CONFIRMATION` | Permit sequence gap or version divergence |
| **107** | Concurrent asynchronous publication calls invoked in same event loop | `PUBLISHING` | Multiple | PENDING | Asynchronous `#publicationLock` queues callers in deterministic FIFO order | Execute interleaved publication attempts concurrently |
| **108** | Publication throws unexpected exception during store persistence | `PUBLISHING` | Coordinator | PENDING | `finally` block guarantees `#publicationLock.release()`; dequeues next waiting caller | Permanently lock publication pipeline on error |
| **109** | Coordinator process crashes and restarts during store persistence | `RECOVERING` | New Instance | PENDING | New process increments `bootGeneration`; uncommitted local state discarded; store recovered | Reuse prior process sequence or instance ID |
| **110** | Authoritative store remains unreachable during reconciliation | `RECONCILING` | Coordinator | PENDING | Gate remains latched in `RECONCILING_CUSTODY` (fail-closed); zero timeouts | Clear gate on timeout or fall back to unverified cache |
| **111** | Mutating recovery operations attempted during `RECONCILING_CUSTODY` | `RECONCILING` | Caller | PENDING | Gate checks reject with `CUSTODY_GATE_RECONCILING`; candidate creation blocked | Permit unverified mutations while custody is uncertain |
| **112** | Newly elected leader initializes recovery coordination | `RECOVERING` | New Leader | IDLE | Queries store via `getAuthoritativeRoot()`; installs committed root before opening gate | Publish candidate roots based on empty or stale state |
| **113** | Caller attempts indexing write on `EncapsulatedByteArray` inside frozen custody record (`cloned.payload[0] = 0xff`) | `ANY` | Consumer | ANY | Immutable wrapper encapsulates private `#bytes`; indexing write fails; public read returns unmodified bytes | Mutate byte array storage nested inside frozen custody record |
| **114** | Typed array instance defines own `Symbol.toStringTag` accessor getter | `ANY` | Any | ANY | Symbol key inspection `Object.getOwnPropertySymbols(v)` detects property; rejects without executing getter | Invoke user-defined string tag getter during brand validation |
| **115** | Typed array shadows prototype accessor (`buffer`, `byteOffset`, `byteLength`) with own property | `ANY` | Any | ANY | Rejects non-index own properties; retrieves internal slots via intrinsic unbound prototype getters | Invoke hostile own property getter during buffer extraction |
| **116** | Subclass of `Uint8Array` (`class SubArray extends Uint8Array`) passed to cloner | `ANY` | Any | ANY | Strict prototype comparison `Object.getPrototypeOf(v) === Uint8Array.prototype` rejects subclass fail-closed | Admit custom typed array subclass with overridden methods |
| **117** | Cross-realm `SharedArrayBuffer` backing store passed in typed view | `ANY` | Any | ANY | Intrinsic brand check `[object SharedArrayBuffer]` detects cross-realm shared memory; rejects with `UNSUPPORTED_BINARY_VALUE_TYPE` | Permit cross-realm shared memory inside custody storage |
| **118** | Authoritative store confirmation returns mismatched `attemptId` | `PUBLISHING` | Coordinator | PENDING | Coordinator asserts `outcome.attemptId === attemptContext.attemptId`; detects mismatch; throws `INVALID_COMMITTED_CONFIRMATION` | Accept delayed confirmation from older publication attempt |
| **119** | Queued publication callers waiting in `#waitQueue` when coordinator steps down | `PUBLISHING` | Waiting Callers | PENDING | `stepDown()` immediately rejects all queued promises with `COORDINATOR_STOPPED` and purges queue | Retain or execute queued attempts under a superseded or subsequent term |
| **120** | In-flight store persistence completes after coordinator leadership lease has expired | `PUBLISHING` | Stale Leader | PENDING | Post-await completion check asserts lease non-expiry; detects elapsed lease; withholds local `#custodyRoot` update; steps down | Overwrite local coordinator state after losing cluster leadership |
| **121** | Store reconciliation queried for uncertain attempt while higher sequence is committed | `RECONCILING` | Coordinator | PENDING | Store verifies queried candidate digest against exact sequence; proves whether queried attempt committed | Misclassify uncertain write as committed based on higher sequence |
| **122** | Conflicting candidate root digest submitted at same sequence during store reconciliation | `RECONCILING` | Coordinator | PENDING | Store detects digest conflict at sequence $S$; rejects with `CONFLICTING_DIGEST_AT_SEQUENCE` | Overwrite durably committed sequence with conflicting candidate |
| **123** | Cross-realm SharedArrayBuffer backing store ingested via Uint8Array view | `RECOVERY_PENDING` | Coordinator | PENDING | Backing buffer evaluated against intrinsic `%SharedArrayBuffer%.prototype.byteLength` getter via `Reflect.apply`; getter succeeds; coordinator rejects with `UNSUPPORTED_BINARY_VALUE_TYPE: SharedArrayBuffer backing stores prohibited` | Admission of shared mutable memory across thread/realm boundaries into custody records |
| **124** | Subclassed EncapsulatedByteArray instance submitted to custody cloning pipeline | `RECOVERY_PENDING` | Coordinator | PENDING | Evaluated against `EncapsulatedByteArray.isTrustedInstance(val)`; fails prototype check and private `#bytes in val` verification; rejected with `UNSUPPORTED_BINARY_VALUE_TYPE: EncapsulatedByteArray subclasses and untrusted instances prohibited` | Subclass method override (`asDetachedUint8Array`) hijacking binary custody reads |
| **125** | Uint8Array view containing extra own string properties (`arr.customProp = 123`) submitted to cloning pipeline | `RECOVERY_PENDING` | Coordinator | PENDING | Own property validation inspects property names; detects non-index property `'customProp'`; throws `UNSUPPORTED_ARRAY_PROPERTY: extra own string properties on typed arrays prohibited; found 'customProp'` | Silent discarding of caller-attached typed-array metadata properties |
| **126** | Publication request arrives presenting an already-committed sequence with a differing attempt ID | `PUBLISHING` | Authoritative Store | FAILED | Store detects sequence collision under different attempt identity; rejects with `DUPLICATE_SEQUENCE_CONFLICT: sequence already committed under different attempt identity` | Replay or interleaving of separate attempts under same publication sequence |
| **127** | Prototype monkey-patching attack replaces `EncapsulatedByteArray.prototype.asDetachedUint8Array` | `RECOVERY_PENDING` | Coordinator | PENDING | Copy constructor reads `source.#bytes` and `source.#canonicalHex` directly via non-overridable private fields, and prototype is frozen via `Object.freeze()`; tampering has zero effect; internal bytes protected | Prototype method tampering hijacking internal custody state reads |
| **128** | In-flight store publication race during UNCERTAIN reconciliation query | `RECONCILING` | Authoritative Store | FAILED | Query observes uncommitted sequence; store atomically persists `TERMINAL_ATTEMPT_FENCED` tombstone; in-flight publication arriving subsequently is rejected with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION`; store returns `NOT_COMMITTED_FINAL` | In-flight publication committing after reconciliation observed absence |
| **129** | Store reconciliation unable to atomically persist terminal fencing tombstone (partition/failure) | `RECONCILING` | Authoritative Store | PENDING | Store cannot prove terminal non-commitment; returns `UNKNOWN_IN_PROGRESS`; coordinator remains fail-closed in `RECONCILING_CUSTODY`; mutations prohibited | Misclassifying unresolved in-flight publication as uncommitted |
| **130** | Sequence collision presenting BOTH differing attemptId AND differing candidate digest at committed sequence | `PUBLISHING` | Authoritative Store | FAILED | Store applies deterministic conflict precedence: attempt identity check takes absolute precedence; permanently rejected with `DUPLICATE_SEQUENCE_CONFLICT` | Ambiguous error classification during multi-attribute sequence collision |
| **131** | Attempted monkey-patching of static `EncapsulatedByteArray.isTrustedInstance` method | `RECOVERY_PENDING` | Coordinator | PENDING | Blocked by `Object.freeze(EncapsulatedByteArray)`; furthermore, internal cloning and constructor invoke unexported lexical helper `isLexicallyTrustedEncapsulatedByteArray`, rendering static modification completely irrelevant | Tampering with public static brand-check method hijacking internal trust decisions |
| **132** | Attempted mutation of `EncapsulatedByteArray.prototype` after class declaration | `RECOVERY_PENDING` | Coordinator | PENDING | Blocked by `Object.freeze(EncapsulatedByteArray.prototype)`; throws `TypeError` in strict mode; prototype methods remain immutable | Prototype monkey-patching overriding internal instance behaviors |
| **133** | Linearizable commit race: publication commits immediately before reconciliation query evaluates | `RECONCILING` | Authoritative Store | PENDING | Linearizable store log evaluates commit at index $K$; subsequent reconciliation query at index $K+1$ reads committed state and returns `RECONCILED_COMMITTED` with original root | Misclassifying successfully committed publication as uncommitted |
| **134** | Linearizable fencing race: reconciliation fences attempt immediately before delayed publication commits | `RECONCILING` | Authoritative Store | FAILED | Reconciliation commits `TERMINAL_ATTEMPT_FENCED` tombstone at index $K$; delayed in-flight publication at index $K+1$ observes tombstone and is rejected with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION` | Delayed publication committing after reconciliation observed absence |
| **135** | Lost publication response followed by reconciliation query | `RECONCILING` | Authoritative Store | PENDING | Publication durably committed at sequence $S$, but response was lost to coordinator; coordinator reconciles presenting identical 8D tuple; store returns `RECONCILED_COMMITTED` idempotently | Duplicate execution or uncommitted classification after lost response |
| **136** | Publication request presents already-committed sequence and matching attemptId but conflicting candidate digest | `PUBLISHING` | Authoritative Store | FAILED | Store evaluates Precedence Step 2; detects candidate digest mismatch; permanently rejected with `CONFLICTING_DIGEST_AT_SEQUENCE` | Re-binding committed sequence number to different state root |
| **137** | Delayed publication carrying stale term or boot generation arrives after store has committed terminal tombstone | `PUBLISHING` | Authoritative Store | FAILED | Evaluated against linearizable store state; rejected with `CUSTODY_PUBLICATION_STALE_LEADERSHIP` and `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION` | Stale-term publication overwriting or corrupting newer epoch state |
| **138** | Store reconciliation unable to atomically persist terminal fencing tombstone (consensus quorum failure) | `RECONCILING` | Authoritative Store | PENDING | Store cannot prove terminal non-commitment; returns `UNKNOWN_IN_PROGRESS`; coordinator remains fail-closed in `RECONCILING_CUSTODY`; mutations prohibited | Misclassifying unprovable in-flight publication outcome as uncommitted |

---

## 16. Mandatory Barrier, Drain, and Settlement Invariants

1. **Gate Transition Authorization Invariant:**
   The custody-reconciliation gate can transition **ONLY** when authorized by the active drainer possessing valid monotonic lease ownership.
2. **Single Primary Drainer Invariant:**
   At most one active drainer may hold custody across all partitions.
3. **Irrevocable Commitment Invariant:**
   Once staged in the archive (`ARCHIVE_COMMITTED`), the transaction is permanent and cannot be rolled back.
4. **Non-Commitment Proof Invariant:**
   Destructive cleanup or buffer restore requires Level 3 fencing proof (`TRANSACTION_ABORTED_FENCED`).
5. **Disposition Exclusivity Invariant:**
   A transaction ID can be finalized in exactly one disposition state.
6. **Drainer Ownership Invariant:**
   Every recovery attempt increments `drainerOwnershipGeneration`. Operations tagged with stale generations are rejected.
7. **Monotonic Retry Increment Invariant:**
   Each recovery attempt uses a strictly higher `recoveryAttemptId`.
8. **Barrier Lifecycle Invariant:**
   A barrier must be created per operation and cannot be reused across attempts.
9. **Atomic Settlement Invariant:**
   Barrier resolution and custody clearance occur in a single atomic step.
10. **Contiguity Invariant for Active Publications:**
   `publicationResults` must form an unbroken contiguous sequence:
   $$\mathbf{keys}(publicationResults) \equiv \{\mathbf{custodyGenerationFloor} + 1, \dots, \mathbf{publishedGeneration}\}$$
   Gaps within this range throw `STATE_INCONSISTENT_RESULT_MAP_NON_CONTIGUOUS`.
11. **Strict Contiguity Invariant for Generation Audit History:**
   `generationAuditHistory` must form an unbroken contiguous sequence:
   $$\mathbf{keys}(generationAuditHistory) \equiv \{\mathbf{auditTombstoneFloor} + 1, \dots, \mathbf{publishedGeneration}\}$$
   Any key gap in this range throws `STATE_INCONSISTENT_AUDIT_MAP_NON_CONTIGUOUS`.
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
18. **Restricted Custody-Value Universe, Encapsulated Binary Storage & Canonical Serialization Invariant (R30-1, R31-1, R31-2, R31-3, R31-4, R32-1, R32-3, R32-4, R32-5, R33-1):**
   All custody root collections enforce runtime `#private` backing storage (`#inner`), strictly admissible primitives (string, safe integers, finite floats, null), strict prohibition of `undefined`, `bigint`, and `symbol`, negative-zero canonicalization to `0`, array density (zero holes), prototype verification without constructor reading, accessor prohibition without getter execution, cycle detection without false rejection of shared acyclic DAG references, private binary storage encapsulation via `EncapsulatedByteArray` with direct private-field reading across instances (`#bytes`, `#canonicalHex`) bypassing all public methods, unexported lexical brand checking (`isLexicallyTrustedEncapsulatedByteArray`) completely immune to external monkey-patching, frozen class constructor and frozen prototype (`Object.freeze(EncapsulatedByteArray)`, `Object.freeze(EncapsulatedByteArray.prototype)`), Proxy-safe trusted-instance verification, supported-input boundary restriction rejecting cross-realm `Uint8Array` views, strict rejection of extra own string properties on typed arrays via `/^(?:0|[1-9]\d*)$/` (`UNSUPPORTED_ARRAY_PROPERTY`), fail-closed detection of cross-realm `SharedArrayBuffer` backing stores via intrinsic `%SharedArrayBuffer%.prototype.byteLength` getter brand checking, canonical lowercase hexadecimal binary serialization (`[0-9a-f]{2*n}`), and strict prohibition of symbol-keyed properties in canonical snapshot payloads.
19. **Complete Structured Owner-Token Identity Invariant:**
   Every active and retired resource binds and verifies the complete structured `CustodyOwnershipToken` (`recoveryAttemptId`, `custodyGeneration`, `drainerOwnershipGeneration`, `transactionId`, `operationIdentity`, `barrierIdentity`, `leaseTerm`, `ownerSignature`) using `isEquivalentOwnerToken()`.
20. **Authoritative Boundary Fencing, Full Lock Lifecycle & Post-Await Fencing Invariant (R30-3, R30-5, R31-5, R32-2, R33-2, R33-3):**
   All custody root publications execute strictly via internal two-phase publication using the complete 8-dimensional `publicationIdentity` tuple $\langle \mathbf{clusterTerm}, \mathbf{bootGeneration}, \mathbf{bootInstanceId}, \mathbf{publicationSequence}, \mathbf{attemptId}, \mathbf{candidateRootDigest}, \mathbf{expectedPriorSequence}, \mathbf{expectedPriorRootDigest} \rangle$. Reconciling an `UNCERTAIN` publication attempt MUST reuse the exact immutable $\mathbf{publicationIdentity}$ tuple including the original `attemptId`. Store publication and reconciliation fencing serialize through the identical shared linearizable consensus log or single-partition transactional compare-and-set state machine. Reporting non-commitment requires an atomically persisted durable terminal tombstone (`TERMINAL_ATTEMPT_FENCED`) bound to the complete 8D tuple that guarantees in-flight attempts cannot commit, or returns `UNKNOWN_IN_PROGRESS` fail-closed. The local coordinator instance enforces process-local FIFO mutual exclusion via asynchronous `#publicationLock`, immediately purges and rejects queued attempts on step-down (`COORDINATOR_STOPPED`), verifies local lease non-expiry with guard margin $\Delta_{\text{guard}}$, asserts exact `AuthoritativeCommittedConfirmation` matching including `attemptId`, and enforces post-await completion fencing prohibiting local `#custodyRoot` updates if leadership was lost while awaiting store response.
21. **Cross-Restart Retirement Idempotency & Active Boot Isolation Invariant:**
   Active collections initialize empty across process restarts; unreleased active resources from prior epochs fail closed as `RESOURCE_NOT_FOUND`. Retired records restored from persistent checkpoints remain authoritative and return `ALREADY_RELEASED` when presented with matching boot epoch, 7-dimensional identity, and evidence digest.
22. **Safe Record Own-Property Creation, Array Descriptor Validation & Intrinsic Inspection Invariant (R29-A2, R30-2, R31-1, R31-6, R32-6, R33-5):**
   All structural records are cloned using `Object.defineProperty` for every own property, guaranteeing that own data properties named `'__proto__'`, `'constructor'`, or `'prototype'` never invoke prototype setters or alter object prototypes. Array indexed elements are validated strictly via `Object.getOwnPropertyDescriptor` without reading elements via property access (`arr[i]`), rejecting accessor descriptors and non-enumerable elements without executing user getters. Non-index array properties and non-canonical digit strings (`"01"`, `"4294967295"`) are strictly rejected. Typed array inputs are validated via intrinsic prototype inspection without invoking user getters or consulting spoofable string tags.
23. **Deterministic Idempotent Authoritative Reconciliation & Fail-Closed Gate Invariant (R30-4, R32-2, R33-3):**
   Any ambiguous store publication outcome (`UNCERTAIN` or I/O timeout) immediately latches the coordinator into `RECONCILING_CUSTODY`. While in this state, all new publications, candidate root creations, and recovery installations are strictly prohibited (fail-closed). Authoritative reconciliation via `reconcilePublication` enforces five deterministic rules: identical identity query returns original committed root; conflicting digest at same sequence is permanently rejected (`CONFLICTING_DIGEST_AT_SEQUENCE`); uncertain attempts must be reconciled before allocating new sequences; later committed publications cannot misclassify earlier uncertain attempts; and reconciliation failure remains fail-closed.

### Strict Ordering of Finalization:
$$\text{1. Settle Promise} \longrightarrow \text{2. Reconcile Custody} \longrightarrow \text{3. Pre-Validate Candidate Root} \longrightarrow \text{4. Authoritative Fenced CAS} \longrightarrow \text{5. Synchronous Local Pointer Swap} \longrightarrow \text{6. Resolve Barrier With Evidence} \longrightarrow \text{7. Release Settlement Pin}$$

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

The following one hundred and thirty-eight deterministic future test specifications are formally codified for Sub-Milestone M5.1:

### Test R18-R33-T01: Retry Acquisition Without Stable Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext === undefined`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `INVALID_RETRY_REQUEST_CONTEXT`.

### Test R18-R33-T02: Retry Acquisition With Malformed or Empty Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext = { consumerId: "   ", clientRequestId: "" }`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `MALFORMED_RETRY_REQUEST_CONTEXT`.

### Test R18-R33-T03: Retry Acquisition With Oversized Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` with `consumerId` of length 257 characters.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `RETRY_REQUEST_CONTEXT_OVERSIZED`.

### Test R18-R33-T04: Lost-Response Retransmission Recovers Original Lease Idempotently
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 acquires lease for $g=1$ with `clientRequestId = "req-101"`; receives `leaseId_A`.
  2. Consumer 1 retransmits identical request with matching `(consumerId, clientRequestId)`.
- **Expected Final Root:** `activeRetryLeases.size === 1`, containing `leaseId_A`.
- **Expected Result / Error:** Second call returns existing `leaseId_A` idempotently.

### Test R18-R33-T05: Repeated Retry Acquisition With Conflicting Operation Identity
- **Initial Authoritative Root:** Settled result at $g=1$; lease active for `(consumer1, req-101)`.
- **Operation Sequence:** Consumer 1 submits request with `clientRequestId = "req-101"` but altered `operationIdentity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.

### Test R18-R33-T06: Two Distinct Consumers Acquire Independent Leases Under Same Recovery Owner Token
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 requests lease for $g=1$ with `consumerId = "c1"`.
  2. Consumer 2 requests lease for $g=1$ with `consumerId = "c2"`.
- **Expected Final Root:** `activeRetryLeases.size === 2`, containing `leaseId_1` and `leaseId_2`.
- **Expected Result / Error:** Both acquire distinct leases successfully.

### Test R18-R33-T07: Consumer 1 Attempting to Release Consumer 2's Lease Fails Closed
- **Initial Authoritative Root:** Active leases for Consumer 1 (`leaseId_1`) and Consumer 2 (`leaseId_2`).
- **Operation Sequence:** Consumer 1 invokes `releaseRetryLease` passing `leaseId_2` with `consumerId = "c1"`.
- **Expected Final Root:** Unchanged; both leases remain active.
- **Expected Result / Error:** Throws `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`.

### Test R18-R33-T08: Duplicate Lease Release by Authorized Consumer Against Retired Record Is Idempotent
- **Initial Authoritative Root:** Active lease `leaseId_1` for Consumer 1.
- **Operation Sequence:**
  1. Consumer 1 calls `releaseRetryLease` with all matching parameters; returns `{ status: 'RELEASED' }`.
  2. Consumer 1 calls `releaseRetryLease` again with identical parameters.
- **Expected Final Root:** `activeRetryLeases.has(leaseId_1) === false`; `retiredRetryLeases.has(leaseId_1) === true`.
- **Expected Result / Error:** Second call matches retired record and returns `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R33-T09: First Publication With Option A Lifecycle Model
- **Initial Authoritative Root:** `publishedGeneration = 0`, `auditTombstoneFloor = 0`, `custodyGenerationFloor = 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` for $g=1$.
- **Expected Final Root:**
  - `publishedGeneration === 1`.
  - `publicationResults.has(1) === true`.
  - `pendingSettlementObligations.has(1) === true`.
  - `generationAuditHistory.get(1).kind === 'PENDING_PUBLICATION'`.
- **Expected Result / Error:** Success; contiguity invariant holds unconditionally.

### Test R18-R33-T10: Pending-to-Terminal Audit History Transition
- **Initial Authoritative Root:** Generation $g=1$ published; audit record is `PENDING_PUBLICATION`.
- **Operation Sequence:** Barrier for $g=1$ settles with `AuthoritativeSettlementEvidence` (`CONFIRMED_COMMITTED`).
- **Expected Final Root:** `generationAuditHistory.get(1).kind === 'TERMINAL_SETTLEMENT'`.
- **Expected Result / Error:** Success; record upgraded in place.

### Test R18-R33-T11: Eviction and Compact Tombstone Conversion
- **Initial Authoritative Root:** Result at $g=1$ settled (`TERMINAL_SETTLEMENT`); 1,000 results retained.
- **Operation Sequence:** Publish $g=1001$; triggers FIFO eviction of $g=1$.
- **Expected Final Root:**
  - `publicationResults.has(1) === false`.
  - `custodyGenerationFloor === 1`.
  - `generationAuditHistory.get(1).kind === 'COMPACT_TOMBSTONE'`.
  - `generationAuditHistory.get(1).evidenceDigest` is 32-byte SHA-256.
- **Expected Result / Error:** Success.

### Test R18-R33-T12: Tombstone Retirement Blocked by Active Settlement Pin
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active settlement pin.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R33-T13: Tombstone Retirement Blocked by Active Retry Lease
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active retry lease.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R33-T14: Tombstone Retirement Blocked by Pending Settlement Obligation
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ references pending obligation.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R33-T15: Normal FIFO Tombstone Retirement at 5,000 Capacity
- **Initial Authoritative Root:** 5,000 tombstones retained ($[1 \dots 5000]$); zero pins/leases on $g=1$.
- **Operation Sequence:** Publish new generation causing eviction and tombstone retirement.
- **Expected Final Root:**
  - Tombstone $g=1$ deleted from `generationAuditHistory`.
  - `auditTombstoneFloor === 1`.
  - Total tombstones remaining: 5,000 ($[2 \dots 5001]$).
- **Expected Result / Error:** Success; contiguity preserved over $[2 \dots publishedGeneration]$.

### Test R18-R33-T16: Pre-Existing Tombstone Count Above Cap Fails Closed
- **Initial Authoritative Root:** Artificially injected root with 5,005 tombstones.
- **Operation Sequence:** Call `publishReleaseAndObligation`.
- **Expected Final Root:** Unchanged; zero bulk deletions performed.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_CORRUPTED`.

### Test R18-R33-T17: Missing Tombstone Inside Promised Horizon Fails Closed
- **Initial Authoritative Root:** `auditTombstoneFloor = 10`, `custodyGenerationFloor = 20`; tombstone $g=15$ deleted.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_KEY_GAP`.

### Test R18-R33-T18: Expiry Below Tombstone Floor Returns Historical Expiry
- **Initial Authoritative Root:** `auditTombstoneFloor = 50`.
- **Operation Sequence:** Query generation $g=30$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.

### Test R18-R33-T19: Floor Order Invariant Violation Fails Closed
- **Initial Authoritative Root:** Artificially set `auditTombstoneFloor = 50`, `custodyGenerationFloor = 40`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_FLOOR_ORDER_VIOLATION`.

### Test R18-R33-T20: Audit Record With Mismatched Generation Fails Closed
- **Initial Authoritative Root:** Audit history key is 5, but record payload has `generation: 4`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `AUDIT_RECORD_GENERATION_MISMATCH`.

### Test R18-R33-T21: Audit Record With Mismatched Owner Token Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `membershipEpoch`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R33-T22: Audit Record With Mismatched Drainer Ownership Generation Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `drainerOwnershipGeneration`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R33-T23: Cross-Generation Settlement Evidence Replay Rejected
- **Initial Authoritative Root:** Active pin for $g=2$; evidence constructed for $g=1$.
- **Operation Sequence:** Call `releaseSettlementPin("pin:...:g2:...", evidence_g1)`.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R33-T24: Cross-Obligation Settlement Evidence Replay Rejected via Atomic In-Root Registry
- **Initial Authoritative Root:** Evidence for $g=1$ already consumed on `pin_1`; `consumedSettlementEvidence.has("1:barrier:pin_1") === true`.
- **Operation Sequence:** Attempt to reuse identical evidence on `pin_2`.
- **Expected Final Root:** Unchanged; `pin_2` remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

### Test R18-R33-T25: Caller-Constructed Mock Evidence Rejected by Provenance Check
- **Initial Authoritative Root:** Active pin for $g=1$.
- **Operation Sequence:** Caller constructs object matching `AuthoritativeSettlementEvidence` schema without factory.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R33-T26: Watchdog Timeout Cannot Release Settlement Pin
- **Initial Authoritative Root:** Barrier pending for $g=1$; watchdog fires after 5,000 ms.
- **Operation Sequence:** Watchdog handler attempts to call `releaseSettlementPin` with watchdog receipt.
- **Expected Final Root:** Pin remains active; gate state latches `DEGRADED`.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R33-T27: Retry Lease ID Passed to Settlement Pin Release Fails Closed
- **Initial Authoritative Root:** Active lease `lease:1:1:c1:1`.
- **Operation Sequence:** Call `releaseSettlementPin("lease:1:1:c1:1", evidence)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `PIN_TYPE_MISMATCH`.

### Test R18-R33-T28: Settlement Pin ID Passed to Retry Lease Release Fails Closed
- **Initial Authoritative Root:** Active pin `pin:1:1:b1:1`.
- **Operation Sequence:** Call `releaseRetryLease({ leaseId: "pin:1:1:b1:1", ... })`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TYPE_MISMATCH`.

### Test R18-R33-T29: Genuinely Unknown Identifier Passed to Release APIs Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Clean root; active and retired maps empty.
- **Operation Sequence:**
  1. Call `releaseRetryLease` with never-issued `leaseId = "lease:999:unknown"`.
  2. Call `releaseSettlementPin` with never-issued `pinId = "pin:999:unknown"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Both calls fail closed with `RESOURCE_NOT_FOUND` (neither returns `ALREADY_RELEASED`).

### Test R18-R33-T30: Duplicate Release Against Retired Pin With Matching Provenance and Digest Returns Already Released
- **Initial Authoritative Root:** Active pin `pin_1` for $g=1$.
- **Operation Sequence:**
  1. Release `pin_1` with valid evidence; returns `{ status: 'RELEASED' }`.
  2. Repeat `releaseSettlementPin("pin_1", validEvidence)`.
- **Expected Final Root:** `activeSettlementPins.has("pin_1") === false`; `retiredSettlementPins.has("pin_1") === true`.
- **Expected Result / Error:** Second call validates provenance and digest, returning `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R33-T31: Settlement Pin Release With Mismatched Drainer Generation Fails Closed (Blocker C1)
- **Initial Authoritative Root:** Active pin for $g=1$ with `drainerOwnershipGeneration = 2`.
- **Operation Sequence:** Caller presents `AuthoritativeSettlementEvidence` with `drainerOwnershipGeneration = 1`.
- **Expected Final Root:** Unchanged; pin remains active; evidence remains unconsumed.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R33-T32: Pre-Publication Throw During Pin Release Leaves Evidence Usable for Retry (Blocker C2)
- **Initial Authoritative Root:** Active pin for $g=1$; `consumedSettlementEvidence.size === 0`.
- **Operation Sequence:**
  1. Initiate `releaseSettlementPin`; candidate root construction simulates memory allocation exception before pointer swap.
  2. Retry `releaseSettlementPin` with the exact same evidence.
- **Expected Final Root:** Second call succeeds; `pin_1` retired; evidence replay key added to `consumedSettlementEvidence`.
- **Expected Result / Error:** First call throws error without consuming evidence in authoritative root; second call completes release cleanly.

### Test R18-R33-T33: Duplicate Release on Retired Lease With Altered Transaction ID Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Retired lease `lease_1` recorded for `transactionId = "tx-100"`.
- **Operation Sequence:** Call `releaseRetryLease` passing `lease_1` with `transactionId = "tx-200"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TRANSACTION_ID_MISMATCH`.

### Test R18-R33-T34: Duplicate Release on Retired Pin With Unverified Provenance Fails Closed (Blocker C2)
- **Initial Authoritative Root:** Retired pin `pin_1` recorded for $g=1$.
- **Operation Sequence:** Caller constructs mock evidence object with matching fields but without factory issuance.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R33-T35: Pre-Existing Retired Pins Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredSettlementPins`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_PINS_CAPACITY_CORRUPTED`.

### Test R18-R33-T36: Pre-Existing Retired Leases Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredRetryLeases`.
- **Operation Sequence:** Call `releaseRetryLease` for active lease `lease_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_LEASES_CAPACITY_CORRUPTED`.

### Test R18-R33-T37: Pre-Existing Consumed Evidence Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted.
- **Expected Result / Error:** Throws `CONSUMED_EVIDENCE_CAPACITY_CORRUPTED`.

### Test R18-R33-T38: Cross-Restart Delayed Release Rejection for Unretired Active Pin (R25-3)
- **Initial Authoritative Root:** Restarted coordinator with `coordinatorBootEpoch = { clusterTerm: 2, bootGeneration: 5, bootInstanceId: "uuid-new" }`. Active collections empty (`size === 0`).
- **Operation Sequence:** Caller submits delayed release for unretired pin `pinId = "pin:1.4.uuid-old:1:barrier:100"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `RESOURCE_NOT_FOUND` (unretired active pins from earlier boot generations cannot be released after restart).

### Test R18-R33-T39: Allocation Interruption Discards Counter Without Collision
- **Initial Authoritative Root:** `_pinSequenceCounter = 10`.
- **Operation Sequence:**
  1. Call `publishReleaseAndObligation`; sequence increments to 11; candidate allocation throws out-of-memory.
  2. Repeat `publishReleaseAndObligation`; sequence increments to 12; publication succeeds.
- **Expected Final Root:** Active pin published with `seq = 12`; zero active records for `seq = 11`.
- **Expected Result / Error:** Second call succeeds cleanly; discarded sequence 11 causes zero collisions.

### Test R18-R33-T40: Normal Exactly-At-Capacity Lockstep Retirement at 5,000 Entries
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`, `consumedSettlementEvidence.size === 5000`. Oldest retired pin has zero active references.
- **Operation Sequence:** Release active pin `pin_new` with valid evidence.
- **Expected Final Root:**
  - Oldest retired pin deleted.
  - Oldest replay key deleted from `consumedSettlementEvidence`.
  - `retiredSettlementPins.size === 5000`.
  - `consumedSettlementEvidence.size === 5000`.
- **Expected Result / Error:** Release succeeds cleanly with exact capacity preservation and pre-publication candidate-root validation.

### Test R18-R33-T41: All-Protected Retired Pins Exhaustion Triggers Fail-Closed Backpressure
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. Every single entry in `retiredSettlementPins` is retained by an active retry lease or active pin.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero pins evicted; zero state mutations committed.
- **Expected Result / Error:** Throws `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE`.

### Test R18-R33-T42: Replay Key Desynchronization on Eviction Fails Closed
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. The replay key corresponding to the oldest eligible retired pin is artificially absent from `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero mutations committed.
- **Expected Result / Error:** Throws `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED`.

### Test R18-R33-T43: Reference-Independent Byte-for-Byte Digest Match Returns Already Released (R25-4)
- **Initial Authoritative Root:** Pin `pin_retired` is in `retiredSettlementPins` with recorded digest `[0x01, 0x02, ..., 0x20]`.
- **Operation Sequence:** Caller invokes `releaseSettlementPin` for `pin_retired` with independently allocated `Uint8Array` containing identical bytes (`digestA !== digestB` by reference, but byte-equal).
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `{ pinId: 'pin_retired', status: 'ALREADY_RELEASED' }`.

### Test R18-R33-T44: Candidate-Root Pre-Publication Invariant Rejection on Desynchronized Replay State (R25-5)
- **Initial Authoritative Root:** Candidate root construction artificially desynchronizes collection sizes (`retiredSettlementPins.size = 5000`, `consumedSettlementEvidence.size = 5001`).
- **Operation Sequence:** Candidate root passed to `_validateCandidateRootPrePublicationInvariants()`.
- **Expected Final Root:** Pointer swap rejected; `this.#custodyRoot` remains unchanged.
- **Expected Result / Error:** Throws `REPLAY_KEY_BIJECTION_CARDINALITY_MISMATCH`.

### Test R18-R33-T45: ECMAScript #inner Backing Storage Inaccessible at Runtime (R25-1)
- **Initial Authoritative Root:** Initialized `AuthoritativeCustodyRoot` with `ImmutableMap` instance.
- **Operation Sequence:** Caller attempts `(root.activeSettlementPins as any).#inner`, `(root.activeSettlementPins as any)._inner`, or `Object.getOwnPropertySymbols(root.activeSettlementPins)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `SyntaxError` or returns `undefined`; backing Map is strictly inaccessible.

### Test R18-R33-T46: Consensus Leadership Loss Prior to Pointer Swap Discards Candidate Root (R25-2)
- **Initial Authoritative Root:** `clusterTerm = 2`. Coordinator prepares candidate root for $g = 2$.
- **Operation Sequence:** Cluster consensus advances to `term = 3` (new leader elected) before Step 2. `this.#consensusLease.assertActiveLeadership(2)` executes.
- **Expected Final Root:** Unchanged; candidate root discarded; local custody marked degraded; settlement barrier left unresolved.
- **Expected Result / Error:** Throws `STALE_COORDINATOR_LEADERSHIP_FENCED`.

### Test R18-R33-T47: Cross-Restart Restored Retired Pin Returns Already Released with Verified Epoch (R25-3)
- **Initial Authoritative Root:** Coordinator restarted under `clusterTerm = 2`, `bootGeneration = 5`. `retiredSettlementPins` restored from persistent checkpoint, containing `pin:1.4.uuid-old:1:barrier:200` with `allocatedBootEpoch = { clusterTerm: 1, bootGeneration: 4, bootInstanceId: "uuid-old" }`.
- **Operation Sequence:** Caller submits delayed release for `pinId = "pin:1.4.uuid-old:1:barrier:200"` with matching 7-dimensional token and matching evidence digest.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `{ pinId: 'pin:1.4.uuid-old:1:barrier:200', status: 'ALREADY_RELEASED' }`.

### Test R18-R33-T48: Pre-Publication Replay-Key Bijection Violation Fails Closed (R25-5)
- **Initial Authoritative Root:** Candidate root constructed with a retired pin whose replay key is absent from `candidateConsumed`.
- **Operation Sequence:** Candidate root evaluated in `_validateCandidateRootPrePublicationInvariants()`.
- **Expected Final Root:** Pointer swap aborted; candidate root discarded.
- **Expected Result / Error:** Throws `REPLAY_KEY_BIJECTION_VIOLATION`.

---

### Test R18-R33-T49: Deep Property Mutation Rejection on Retrieved ImmutableMap Value (R26-1)
- **Initial Authoritative Root:** Initialized root with published result at $g=1$.
- **Operation Sequence:** Caller retrieves record via `root.publicationResults.get(1)` and attempts in-place property modification: `(record as any).obligation.transactionId = "corrupted"`.
- **Expected Final Root:** Unchanged; stored record remains intact with original `transactionId`.
- **Expected Result / Error:** Throws `TypeError` (frozen object property modification) or fails silently without mutating internal state; subsequent `root.publicationResults.get(1)` returns pristine original `transactionId`.

### Test R18-R33-T50: Defensive Byte Array Cloning Prevents Buffer Tampering on Egress (R26-1)
- **Initial Authoritative Root:** Initialized root with active settlement pin containing `ownerToken.ownerSignature = new Uint8Array([0x01, 0x02, 0x03, 0x04])`.
- **Operation Sequence:**
  1. Caller retrieves pin via `const pin = root.activeSettlementPins.get(pinId)!`.
  2. Caller mutates byte buffer directly: `pin.ownerToken.ownerSignature[0] = 0xFF`.
- **Expected Final Root:** Internal backing storage unaffected. Subsequent call to `root.activeSettlementPins.get(pinId)` yields buffer with original `ownerSignature[0] === 0x01`.
- **Expected Result / Error:** Defensive byte cloning on egress ensures caller mutates an isolated detached copy; internal state remains pristine.

### Test R18-R33-T51: Iterator Aliasing and Mutation Immunity on Entries and Values (R26-1)
- **Initial Authoritative Root:** Published results at $g=1$ and $g=2$ in `publicationResults`.
- **Operation Sequence:**
  1. Caller iterates using `for (const [k, v] of root.publicationResults.entries())` or `for (const v of root.publicationResults.values())`.
  2. Caller attempts in-place property mutation or byte array modification on yielded values.
- **Expected Final Root:** Unchanged; internal root and subsequent iterations produce identical unmodified records.
- **Expected Result / Error:** Deep defensive cloning on each yielded element guarantees complete reference isolation across all iterator access paths.

### Test R18-R33-T52: Key and Element Type Invariant Rejection on ImmutableMap and ImmutableSet (R26-1)
- **Initial Authoritative Root:** Initialized `ImmutableMap` and `ImmutableSet`.
- **Operation Sequence:**
  1. Caller attempts to instantiate `ImmutableMap` with an object key: `new ImmutableMap([[({} as any), "val"]])` or call `.set({} as any, "val")`.
  2. Caller attempts to instantiate `ImmutableSet` with an object element: `new ImmutableSet([({} as any)])` or call `.add({} as any)`.
- **Expected Final Root:** Unchanged; zero elements inserted.
- **Expected Result / Error:** Throws `INVALID_COLLECTION_KEY_TYPE: keys must be primitive string or number` and `INVALID_COLLECTION_ELEMENT_TYPE: set elements must be primitive string or number`.

### Test R18-R33-T53: Non-Reentrant Single-Writer Execution Lock Prevents Concurrent Publications (R26-2)
- **Initial Authoritative Root:** Coordinator initialized with `LeadershipEpochFence`.
- **Operation Sequence:**
  1. Primary publication begins executing inside `executeFencedPublication()`.
  2. While the lock is held, a concurrent callback or reentrant call attempts `executeFencedPublication()`.
- **Expected Final Root:** Primary publication completes or rolls back; concurrent attempt is rejected upfront before touching candidate roots.
- **Expected Result / Error:** Throws `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.

### Test R18-R33-T54: Synchronous Leadership Stepdown and Lease Expiry Fencing (R26-2)
- **Initial Authoritative Root:** Coordinator with active lease for Term 1 expiring at monotonic $t_{\text{expire}}$.
- **Operation Sequence:**
  1. Case A: Coordinator receives stepdown notification (`leadershipFence.stepDown()`), then attempts `executeFencedPublication(expectedTerm: 1)`.
  2. Case B: Monotonic clock advances past $t_{\text{expire}}$, then coordinator attempts `executeFencedPublication(expectedTerm: 1)`.
- **Expected Final Root:** Pointer swap aborted in both cases; `#custodyRoot` untouched; candidate root discarded; barrier remains unresolved.
- **Expected Result / Error:** Both cases throw `STALE_COORDINATOR_LEADERSHIP_FENCED`.

---

### Test R18-R33-T55: Symbol-Keyed Mutable Value Recursively Cloned and Frozen (R27-A)
- **Initial State:** Authoritative custody root with published result at $g=1$. A candidate record includes a proprietary symbol property `[Symbol('syncplay/ext')] = { payload: new Uint8Array([0xAA, 0xBB]) }`.
- **Exact Interleaving or Injected Fault:** Candidate record passed to `deepCloneAndFreezeValue()`. Caller subsequently mutates original symbol property: `original[sym].payload[0] = 0x00`.
- **Expected Result:** Cloned symbol value is deeply cloned and frozen as an own data property (`Object.isFrozen(cloned[sym]) === true`). Internal custody record payload remains `0xAA`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Custody remains securely owned; zero reference aliasing.

### Test R18-R33-T56: Unsupported Object Prototypes Rejected (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller attempts to insert an object instance of a custom class (`class CustomTelemetry {}`, `new CustomTelemetry()`) into publication results.
- **Expected Result:** Throws `UNSUPPORTED_PROTOTYPE_INSTANCE: objects with custom prototypes of type 'CustomTelemetry' are prohibited in custody values`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery until valid record presented.

### Test R18-R33-T57: Accessor Properties Rejected Without Invoking Getters (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller passes an object with a getter accessor: `{ get hostile() { throw new Error('GETTER_INVOKED_SIDE_EFFECT'); } }`.
- **Expected Result:** Property descriptor inspection detects `desc.get !== undefined` without evaluating `obj.hostile`. Throws `ACCESSOR_PROPERTIES_PROHIBITED: property 'hostile' has getter/setter accessors which are forbidden in custody records`. The getter is never invoked.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R33-T58: Cyclic Structures Fail Closed (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller passes a self-referential cyclic object graph (`const cycle: any = {}; cycle.self = cycle;`).
- **Expected Result:** `WeakSet` cycle detection catches `seen.has(cycle)` and throws `CIRCULAR_REFERENCE_DETECTED: circular object graph cannot be cloned into custody storage`. Zero call stack overflow.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R33-T59: Unsupported Typed Arrays and Binary/Temporal Types Rejected (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller attempts to store `new Float32Array([1.0, 2.0])`, `new ArrayBuffer(16)`, `new DataView(new ArrayBuffer(8))`, or `new Date()`.
- **Expected Result:** Throws `UNSUPPORTED_BINARY_VALUE_TYPE: only Uint8Array is supported for binary custody data` or `UNSUPPORTED_TEMPORAL_VALUE_TYPE: Date objects prohibited; use monotonic timestamp numbers`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R33-T60: Byte-Array Egress Mutation Cannot Alter Custody Storage (R27-A)
- **Initial State:** Active settlement pin recorded in `activeSettlementPins` with `ownerToken.ownerSignature = new Uint8Array([0x10, 0x20, 0x30])`.
- **Exact Interleaving or Injected Fault:** Caller invokes `root.activeSettlementPins.get(pinId)!`, receiving detached byte array. Caller mutates `egress.ownerToken.ownerSignature[0] = 0xFF`.
- **Expected Result:** The returned `Uint8Array` is a detached mutable copy. Backing `#inner` storage is untouched. A subsequent call to `root.activeSettlementPins.get(pinId)!` yields fresh buffer with `ownerSignature[0] === 0x10`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Custody remains securely owned; internal storage immutable.

### Test R18-R33-T61: Lease Expires After Local Check but Before Authoritative Publication (R27-B)
- **Initial State:** Coordinator holds lease for Term 1 expiring at $t_{\text{expire}} = t_0 + 5000\text{ms}$. Candidate root prepared at $t_0 + 4900\text{ms}$.
- **Exact Interleaving or Injected Fault:** An artificial thread pause delays submission to the Authoritative Custody Store until $t_0 + 5100\text{ms}$ (lease expired). Authoritative Custody Store evaluates token against cluster consensus.
- **Expected Result:** Authoritative Custody Store CAS rejects write with `STALE_COORDINATOR_LEADERSHIP_FENCED`. Local coordinator discards candidate root; `#custodyRoot` untouched.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Local custody enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`); barrier left unresolved.

### Test R18-R33-T62: Coordinator Resumes After Process Suspension and Attempts Publication with Expired Lease (R27-B)
- **Initial State:** Coordinator holding Term 1 lease prepares candidate root.
- **Exact Interleaving or Injected Fault:** OS process suspension (SIGSTOP / major GC pause) suspends coordinator for 10 seconds. On resumption at $t_0 + 10000\text{ms}$, coordinator invokes `executeFencedPublication()`.
- **Expected Result:** Step 2 local fencing check (with guard margin $\Delta_{\text{guard}}$) detects `performance.now() + 1000ms >= leaseExpiresAtMonotonicMs` and throws `STALE_COORDINATOR_LEADERSHIP_FENCED`. Zero write submitted to authoritative store.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Candidate discarded; `#custodyRoot` untouched; custody enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`).

### Test R18-R33-T63: Two Independent Coordinator Instances Attempt Publication Under Different Terms (R27-B)
- **Initial State:** Partitioned Coordinator A operates under Term 1; newly elected Coordinator B operates under Term 2. Authoritative Custody Store has admitted Term 2.
- **Exact Interleaving or Injected Fault:** Both Coordinator A and Coordinator B submit candidate publications to Authoritative Custody Store.
- **Expected Result:** Authoritative Custody Store atomically accepts Coordinator B (Term 2) and rejects Coordinator A (Term 1) with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
- **Which Invariant Is Protected:** Invariant 2 (Single Primary Drainer Invariant) and Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Coordinator B holds valid cluster custody; Coordinator A enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`).

### Test R18-R33-T64: Asynchronous Commit Callback Prohibited and Rejected (R27-B)
- **Initial State:** Coordinator initialized with `LeadershipEpochFence`.
- **Exact Interleaving or Injected Fault:** Caller invokes `executeFencedPublication()` passing an `async` function or a callback that returns a Promise (`async (root) => { await delay(10); }`).
- **Expected Result:** Step 4 detects thenable return and throws `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED: commit callback must be strictly synchronous`. In `finally`, `#publicationLock` is reset to `false`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Root unchanged; lock released cleanly; zero async escape.

### Test R18-R33-T65: Leadership Changes During Publication Protocol (R27-B)
- **Initial State:** Coordinator A begins candidate root staging for Term 1.
- **Exact Interleaving or Injected Fault:** Cluster consensus quorum elects Coordinator B for Term 2 and updates `store.highestCommittedTerm = 2` before Coordinator A's CAS arrives.
- **Expected Result:** Authoritative Custody Store CAS rejects Coordinator A's token with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Coordinator A's candidate root discarded; barrier unresolved; local custody latched into `CUSTODY_RECONCILIATION_BLOCKED`.

### Test R18-R33-T66: Stale Coordinator Cannot Mutate Shared Recovery State After Term Loss (R27-B)
- **Initial State:** Coordinator A was partitioned and deposed in Term 1; cluster is active in Term 2.
- **Exact Interleaving or Injected Fault:** Coordinator A attempts post-term recovery mutation (`publishReleaseAndObligation` or `releaseSettlementPin`).
- **Expected Result:** Authoritative store rejects write with `STALE_COORDINATOR_LEADERSHIP_FENCED`. Coordinator A marks local in-flight custody degraded, leaves barrier pending, and latches `CUSTODY_RECONCILIATION_BLOCKED`.
- **Which Invariant Is Protected:** Invariant 4 (Non-Commitment Proof Invariant) and Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Shared recovery state completely protected; stale coordinator fenced permanently.

---

### Test R18-R33-T67: Shared Acyclic References Are Accepted Without False Cycle Rejection (R28-A)
- **Initial State:** Authoritative custody root with published result at $g=1$. A candidate record forms a directed acyclic graph (DAG) where sub-object `sharedNode = { data: 'immutable' }` is referenced by both `candidate.branchA.node` and `candidate.branchB.node`.
- **Exact Interleaving or Injected Fault:** Candidate record passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Traversal memoizes `sharedNode` via `cloneMap` and removes ancestors from `activeStack` upon branch completion. Successfully clones and freezes the record. Reference equality is preserved (`cloned.branchA.node === cloned.branchB.node`), and zero false cycle error is thrown.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & DAG Cycle Detection).
- **Custody Disposition:** Custody remains securely owned; valid DAG structures accepted without error.

### Test R18-R33-T68: Sparse Arrays with Missing Indices Are Rejected (R28-A)
- **Initial State:** Coordinator preparing candidate root.
- **Exact Interleaving or Injected Fault:** Caller attempts to pass a sparse array containing missing indices / uninitialized holes (e.g. `const arr = [1]; arr[3] = 4;` or `new Array(5)`).
- **Expected Result:** Array density validation detects that `Object.prototype.hasOwnProperty.call(arr, '1') === false`. Throws `SPARSE_ARRAY_PROHIBITED: sparse arrays with missing index '1' are prohibited in custody records`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Array Density).
- **Custody Disposition:** Root unchanged; candidate discarded; custody enters degraded recovery.

### Test R18-R33-T69: Function Values Are Rejected in All Positions (R28-A)
- **Initial State:** Coordinator preparing candidate root.
- **Exact Interleaving or Injected Fault:** Caller attempts to pass a function value at the top level or nested inside a plain record: `{ handler: () => true }`.
- **Expected Result:** Immediate check at the absolute entry boundary detects `typeof val === 'function'`. Throws `UNSUPPORTED_FUNCTION_VALUE_TYPE: functions cannot be stored in custody records`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe).
- **Custody Disposition:** Root unchanged; candidate discarded; custody enters degraded recovery.

### Test R18-R33-T70: Custom Prototype Constructor Getters Are Never Invoked (R28-A)
- **Initial State:** Coordinator preparing candidate root.
- **Exact Interleaving or Injected Fault:** Caller constructs an object inheriting from an adversarial custom prototype that defines an active getter on `constructor`:
  `const hostileProto = Object.create(Object.prototype, { constructor: { get() { throw new Error('HOSTILE_GETTER_SIDE_EFFECT'); } } }); const obj = Object.create(hostileProto);`
- **Expected Result:** Direct reference comparison detects `Object.getPrototypeOf(obj) !== Object.prototype && proto !== null`. Rejection occurs without reading `proto.constructor` or inspecting prototype properties. Throws `UNSUPPORTED_PROTOTYPE_INSTANCE`. The hostile getter is never executed.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe).
- **Custody Disposition:** Root unchanged; candidate discarded; hostile side effects completely prevented.

### Test R18-R33-T71: Symbol-Keyed Values Follow Documented Canonical Serialization Policy (R28-A)
- **Initial State:** Coordinator preparing candidate root for canonical snapshot hashing.
- **Exact Interleaving or Injected Fault:** Caller passes a record containing arbitrary user-defined symbol keys into a payload intended for RFC 8785 canonical snapshot hashing.
- **Expected Result:** Key validation detects `typeof key === 'symbol'`. Throws `SYMBOL_PROPERTIES_PROHIBITED_IN_CANONICAL_VALUES: symbol-keyed properties are prohibited in RFC 8785 canonical serialization`.
- **Which Invariant Is Protected:** Invariant 18 (Canonical Serialization Compatibility).
- **Custody Disposition:** Root unchanged; candidate discarded; canonical hash integrity guaranteed.

### Test R18-R33-T72: Asynchronous Authoritative Publication Cannot Cause Premature Local Root Replacement (R28-B)
- **Initial State:** Coordinator holding active lease prepares candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Coordinator initiates `publishReleaseAndObligation()`. Phase 1 submits write to `store.publishFencedRoot()`. While the network request is in flight, an asynchronous concurrent reader inspects `coordinator.custodyRoot`.
- **Expected Result:** Local `custodyRoot` remains strictly at $g=1$. Local pointer replacement `this.#custodyRoot = candidateRoot` is executed in Phase 2 strictly after `store.publishFencedRoot()` resolves with `{ status: 'COMMITTED' }`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Two-Phase Publication).
- **Custody Disposition:** Linearizability preserved; zero premature local root exposure.

### Test R18-R33-T73: Rejected or Uncertain Authoritative Publication Cannot Silently Diverge Local and Authoritative Roots (R28-B)
- **Initial State:** Coordinator prepares candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Authoritative Custody Store returns `{ status: 'UNCERTAIN', reason: 'network_timeout' }` or `{ status: 'FENCED_REJECTED', reason: 'term_superseded' }`.
- **Expected Result:** Local `#custodyRoot` remains strictly at $g=1$. Pointer update is withheld. For `FENCED_REJECTED`, throws `STALE_COORDINATOR_LEADERSHIP_FENCED` and transitions to `CUSTODY_RECONCILIATION_BLOCKED`. For `UNCERTAIN`, throws `PUBLICATION_OUTCOME_UNCERTAIN` and initiates reconciliation query via `store.getAuthoritativeRoot()`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Zero silent divergence; custody latched degraded/blocked until authoritative reconciliation completes.

### Test R18-R33-T74: Every Fencing-Token Component Is Validated at the Authoritative Store Boundary (R28-B)
- **Initial State:** Authoritative Custody Store holding committed Term 2, Boot Generation 3, Boot Instance UUID-B, Sequence 5.
- **Exact Interleaving or Injected Fault:** Submitting candidate roots with:
  1. Term 1 (stale term).
  2. Term 2, Boot Generation 2 (stale generation).
  3. Term 2, Boot Generation 3, Boot Instance UUID-A (stale instance).
  4. Term 2, Boot Generation 3, Boot Instance UUID-B, Sequence 7 (sequence gap).
- **Expected Result:** Authoritative store validates all four dimensions: Case 1 returns `{ status: 'FENCED_REJECTED', reason: 'stale_term' }`; Cases 2 and 3 return `{ status: 'FENCED_REJECTED', reason: 'superseded_instance' }`; Case 4 returns `{ status: 'FENCED_REJECTED', reason: 'out_of_order_sequence' }`. All mutations are rejected before touching persistent state.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Complete 4-Dimensional Token Validation).
- **Custody Disposition:** Authoritative store state protected across all four token dimensions.

---

### Test R18-R33-T75: Non-Empty Uint8Array Ingestion Without Invalid Object.freeze (R29-A1)
- **Preconditions:** Coordinator preparing candidate custody record containing a non-empty `Uint8Array` (e.g. 32-byte cryptographic digest).
- **Trigger:** Record ingested via `deepCloneAndFreezeValue()`.
- **Expected Result:** Byte contents copied into an `EncapsulatedByteArray`. The typed array view is NOT passed to `Object.freeze()`, preventing runtime `TypeError`. The internal array is encapsulated privately; egress reads return fresh detached copies.
- **Invariants Protected:** Invariant 18 (Restricted Custody-Value Universe & Binary Encapsulation).
- **Classification:** Future M5.1 implementation test (architecture requirement).

### Test R18-R33-T76: Empty Uint8Array Ingestion & Detached Copying (R29-A1)
- **Preconditions:** Candidate custody record containing a 0-byte `new Uint8Array(0)`.
- **Trigger:** Ingested via `deepCloneAndFreezeValue()`.
- **Expected Result:** Clones successfully into a 0-byte `EncapsulatedByteArray`. Public accessors return a 0-byte detached copy.
- **Invariants Protected:** Invariant 18 (Binary Encapsulation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T77: Egress Copy Mutation Isolation for Byte Arrays (R29-A1)
- **Preconditions:** Authoritative custody root contains byte array `[0x01, 0x02, 0x03]` in a published record.
- **Trigger:** Caller retrieves the record via `.get()` and mutates the returned byte array: `bytes[0] = 0xFF`.
- **Expected Result:** Subsequent `.get()` calls on the coordinator return the original unmodified bytes `[0x01, 0x02, 0x03]`. Mutation of the detached egress copy has zero effect on internal custody bytes.
- **Invariants Protected:** Invariant 18 (Binary Encapsulation & Egress Isolation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T78: Repeated Shared References to Same Uint8Array in DAG (R29-A1)
- **Preconditions:** A candidate record defines `{ a: sharedBytes, b: sharedBytes }` referencing the identical `Uint8Array` instance.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `cloneMap` memoizes the cloned `EncapsulatedByteArray` on first encounter. Both `cloned.a` and `cloned.b` reference the same instance. No circular reference exception is thrown.
- **Invariants Protected:** Invariant 18 (DAG Cycle Detection & Binary Encapsulation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T79: Snapshot Export and Import Byte Equality via Canonical Hex Encoding (R29-A1)
- **Preconditions:** Custody record with byte array `new Uint8Array([0xde, 0xad, 0xbe, 0xef])`.
- **Trigger:** Export snapshot using `encodeCanonicalBinaryHex()` and import into a new coordinator instance.
- **Expected Result:** Snapshot encodes bytes as lowercase hex `"deadbeef"`. Import reconstructs a detached `Uint8Array` with exact byte equality (`areEqualByteArrays` returns true).
- **Invariants Protected:** Invariant 18 (Canonical Binary Serialization).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T80: Canonical Digest Stability Across Equivalent Byte Content (R29-A1)
- **Preconditions:** Two `Uint8Array` views created from different backing buffers and offsets, but containing identical byte sequences `[1, 2, 3, 4]`.
- **Trigger:** Compute canonical snapshot digest for both records.
- **Expected Result:** Both produce identical lowercase hexadecimal representations and identical SHA-256 digests.
- **Invariants Protected:** Invariant 18 (Canonical Digest Stability).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T81: Rejection of Prohibited Binary Backing Storage (R29-A1)
- **Preconditions:** Candidate custody record containing:
  1. A `Uint8Array` backed by a `SharedArrayBuffer`.
  2. A raw `ArrayBuffer`.
  3. A `DataView` or non-Uint8Array typed array (`Int32Array`).
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** All three variations throw `UNSUPPORTED_BINARY_VALUE_TYPE` synchronously.
- **Invariants Protected:** Invariant 18 (Restricted Custody Universe).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T82: Array Index Zero Accessor Rejection Without Getter Execution (R29-A2)
- **Preconditions:** An array defined with an accessor property at index 0:
  `Object.defineProperty([], '0', { get() { getterCalled = true; return 1; }, enumerable: true, configurable: true })`.
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Throws `ACCESSOR_PROPERTIES_PROHIBITED: array index '0' has accessor descriptor`. `getterCalled` remains strictly `false`.
- **Invariants Protected:** Invariant 22 (Array Descriptor Validation Without Getter Execution).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T83: Array Later Index Accessor Rejection Without Getter Execution (R29-A2)
- **Preconditions:** An array `[10, 20]` with an accessor defined at index 2:
  `Object.defineProperty(arr, '2', { get() { getterCalled = true; return 30; }, enumerable: true, configurable: true })`.
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Throws `ACCESSOR_PROPERTIES_PROHIBITED: array index '2' has accessor descriptor`. `getterCalled` remains strictly `false`.
- **Invariants Protected:** Invariant 22 (Array Accessor Getter Immunity).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T84: Non-Enumerable Array Index Rejection (R29-A2)
- **Preconditions:** An array defined with index 1 set to `enumerable: false`.
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Descriptor validation throws `NON_ENUMERABLE_PROPERTY_PROHIBITED: array index '1' must be enumerable`.
- **Invariants Protected:** Invariant 22 (Array Descriptor Invariants).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T85: Sparse Array Hole Rejection via Descriptor Inspection (R29-A2)
- **Preconditions:** A sparse array with missing indices (e.g. `[1, , 3]` where index 1 is a hole).
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `Object.getOwnPropertyDescriptor(arr, '1')` returns `undefined`. Throws `SPARSE_ARRAY_PROHIBITED: sparse arrays with missing index '1' are prohibited`.
- **Invariants Protected:** Invariant 18 & 22 (Array Density & Hole Prohibition).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T86: Non-Canonical Digit Array Property "01" Rejection (R29-A2)
- **Preconditions:** An array defined with named property `"01"`:
  `const arr = [1, 2]; (arr as any)['01'] = 'hostile';`.
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `String(Number('01') >>> 0) !== '01'`. Validation throws `UNSUPPORTED_ARRAY_PROPERTY: array contains non-index property '01'`.
- **Invariants Protected:** Invariant 22 (Precise Array Index Property Validation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T87: Out-of-Range Digit Array Property "4294967295" Rejection (R29-A2)
- **Preconditions:** An array defined with named property `"4294967295"` ($2^{32}-1$, which is not a valid ECMAScript array index).
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Index boundary check `u32 !== 0xFFFFFFFF` rejects with `UNSUPPORTED_ARRAY_PROPERTY`.
- **Invariants Protected:** Invariant 22 (Array Index Domain Bounds).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T88: Valid Dense Arrays with Nested Records and Shared DAG References (R29-A2)
- **Preconditions:** A dense array `[ { id: 1 }, sharedObj, [ sharedObj ] ]` where `sharedObj = { val: 'safe' }`.
- **Trigger:** Array passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Successfully clones all elements using `Object.defineProperty`. Reference identity of `sharedObj` is preserved across branches. Cloned array is frozen with `Object.freeze()`.
- **Invariants Protected:** Invariant 18 & 22 (Dense Array Cloning & DAG Preservation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T89: Own Data Property '__proto__' Preserved via Object.defineProperty Without Mutating Prototype (R29-A3)
- **Preconditions:** An input object created with an own data property named `'__proto__'`:
  `const obj = Object.defineProperty({}, '__proto__', { value: { payload: 'safe' }, enumerable: true, writable: true, configurable: true });`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Cloned object defines `'__proto__'` via `Object.defineProperty`. The cloned object's prototype remains `Object.prototype` (or null). `Object.getPrototypeOf(cloned) === Object.prototype`. `cloned.hasOwnProperty('__proto__') === true`. The legacy setter was never invoked.
- **Invariants Protected:** Invariant 22 (Safe Record Own-Property Creation & Prototype Isolation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T90: Own Data Properties 'constructor' and 'prototype' Preserved Without Invoking Hostile Getters (R29-A3)
- **Preconditions:** An input record containing own data properties:
  `{ constructor: 'custom-constructor', prototype: 'custom-prototype' }`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Descriptors verified as data properties (`desc.get === undefined`). Defined on clone via `Object.defineProperty`. No constructor inspection or getter execution occurs. Record is frozen.
- **Invariants Protected:** Invariant 22 (Safe Own-Property Creation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T91: Numeric Negative Zero (-0) Canonicalized to Positive Zero (0) for RFC 8785 Stability (R29-A4)
- **Preconditions:** A candidate record containing property `count: -0`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Cloner detects `Object.is(val, -0)` and returns `0`. `Object.is(cloned.count, 0) === true` and `Object.is(cloned.count, -0) === false`. Snapshot digest matches positive zero record.
- **Invariants Protected:** Invariant 18 (Canonical Numeric Serialization).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T92: Safe Integer Boundary Ingestion (R29-A4)
- **Preconditions:** Candidate records containing `Number.MAX_SAFE_INTEGER` ($9007199254740991$) and `Number.MIN_SAFE_INTEGER` ($-9007199254740991$).
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Both numbers pass `Number.isSafeInteger(val)` and are preserved without modification.
- **Invariants Protected:** Invariant 18 (Exact Numeric Contract).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T93: Unsafe Integer Rejection Outside Safe Integer Range (R29-A4)
- **Preconditions:** Candidate record containing `Number.MAX_SAFE_INTEGER + 1` ($9007199254740992$) or unsafe integer $2^{60}$.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Throws `UNSAFE_INTEGER_PROHIBITED: integer exceeds safe integer range`.
- **Invariants Protected:** Invariant 18 (Unsafe Integer Prohibition).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T94: Finite Fractional Floating-Point Numbers Accepted (R29-A4)
- **Preconditions:** Candidate record containing finite floats: `3.141592653589793`, `0.00001`, `-123.456`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** All pass `Number.isFinite(val)` and are preserved accurately.
- **Invariants Protected:** Invariant 18 (Finite Float Ingestion).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T95: Non-Finite Numbers (NaN, +Infinity, -Infinity) Rejected (R29-A4)
- **Preconditions:** Candidate records containing `NaN`, `+Infinity`, `-Infinity`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** All three values throw `INVALID_NUMERIC_VALUE: non-finite number prohibited`.
- **Invariants Protected:** Invariant 18 (Non-Finite Numeric Rejection).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T96: Cross-Realm Uint8Array Brand Check Recognition and Detached Cloning (R29-A6)
- **Preconditions:** A `Uint8Array` instance instantiated in a separate V8 context (`vm.createContext()`).
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `ArrayBuffer.isView(val)` recognizes the typed view; bytes are copied into an `EncapsulatedByteArray` belonging to the host coordinator realm.
- **Invariants Protected:** Invariant 18 (Cross-Realm Binary Recognition & Realm Isolation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T97: Hostile ECMAScript Proxy Ingestion Trust Boundary Enforcement (R29-A6)
- **Preconditions:** Caller constructs a `new Proxy({}, { get() { ... }, getOwnPropertyDescriptor() { ... } })`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Documented boundary: Reflective operations invoke Proxy traps; Proxy objects are outside the supported ingestion contract. Ingestion assumes trusted schema-validated inputs.
- **Invariants Protected:** Invariant 18 & 22 (Documented Proxy Ingestion Trust Boundary).
- **Classification:** Architecture-level specification requirement.

### Test R18-R33-T98: Leadership Term Advances While Asynchronous Store Persistence is Awaited (R29-B1)
- **Preconditions:** Coordinator A (Term 1) initiates publication sequence 10; invokes `await store.publishFencedRoot()`.
- **Trigger:** While store request is pending, consensus quorum elects Coordinator B for Term 2; Coordinator A's `leadershipFence.stepDown(2)` is invoked. Store then returns `COMMITTED`.
- **Expected Result:** Coordinator A's post-await completion check `assertPostAwaitEligibility()` detects term advance ($1 \neq 2$). Throws `CUSTODY_PUBLICATION_STALE_LEADERSHIP`. Local `#custodyRoot` is NOT replaced with candidate root.
- **Invariants Protected:** Invariant 20 (Post-Await Leadership Completion Fencing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T99: Successful Store Commit with Delayed Response Arriving After Leadership Lease Expiry (R29-B1)
- **Preconditions:** Coordinator A (Term 1) submits publication sequence 12 to authoritative store. Store commits candidate root. Response is delayed across a 6,000ms network pause.
- **Trigger:** Coordinator A's monotonic clock passes `#leaseExpiresAtMonotonicMs` during the pause. Store response `COMMITTED` arrives.
- **Expected Result:** Post-await check detects that `performance.now() + guardMargin >= leaseExpiresAtMonotonicMs`. Throws `CUSTODY_PUBLICATION_STALE_LEADERSHIP`. Local `#custodyRoot` is untouched. Newly elected Term 2 leader recovers committed root from store.
- **Invariants Protected:** Invariant 20 (Post-Await Lease Fencing & Local Pointer Isolation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T100: Stale Partitioned Coordinator Receives Delayed COMMITTED Confirmation (R29-B1)
- **Preconditions:** Partitioned coordinator whose lease has definitively elapsed receives a late `COMMITTED` confirmation.
- **Trigger:** Coordinator resumes execution turn.
- **Expected Result:** Post-await fence rejects the confirmation; coordinator steps down. Zero stale local pointer modification occurs.
- **Invariants Protected:** Invariant 20 (Stale Partitioned Leader Discard).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T101: Store Commits Sequence S but Response is Lost; Reconciliation Recovers Committed Root (R29-B2)
- **Preconditions:** Store durably commits publication sequence 15, but network drop causes the coordinator's await to time out (`status: 'UNCERTAIN'`).
- **Trigger:** Coordinator latches into `RECONCILING_CUSTODY` and executes `reconcileAuthoritativeCustody()`.
- **Expected Result:** `store.reconcilePublication(query)` returns `{ status: 'RECONCILED_COMMITTED', committedRoot }`. Coordinator verifies post-await eligibility, updates `#custodyRoot = committedRoot`, and returns gate state to `IDLE`.
- **Invariants Protected:** Invariant 23 (Idempotent Store Reconciliation for Lost Responses).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T102: Idempotent Publication Retry with Identical Fencing Token, Sequence, and Root Digest (R29-B2)
- **Preconditions:** Store committed sequence 18 for candidate root digest $D$.
- **Trigger:** Coordinator retries publication with identical token, sequence 18, and digest $D$.
- **Expected Result:** Store recognizes idempotent retry; returns original `COMMITTED` confirmation without creating a second logical publication or throwing sequence collision error.
- **Invariants Protected:** Invariant 23 (Deterministic Idempotent Retry).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T103: Conflicting Publication Attempt with Same Token and Sequence but Differing Root Digest (R29-B2)
- **Preconditions:** Store committed sequence 20 with candidate root digest $D_1$.
- **Trigger:** A publication request arrives with sequence 20 but differing root digest $D_2$.
- **Expected Result:** Authoritative store detects state inconsistency; permanently rejects the attempt with `CONFLICTING_DIGEST_AT_SEQUENCE`.
- **Invariants Protected:** Invariant 20 & 23 (Publication Digest Invariant).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T104: Store Confirmation with Mismatched Sequence Rejected (R29-B3)
- **Preconditions:** Coordinator submits publication sequence 25.
- **Trigger:** Authoritative store returns `{ status: 'COMMITTED', committedSequence: 24, ... }`.
- **Expected Result:** Confirmation validation detects sequence discrepancy ($24 \neq 25$). Throws `INVALID_COMMITTED_CONFIRMATION: authoritative store returned inconsistent commit confirmation`. Coordinator latches into `RECONCILING_CUSTODY`; local pointer untouched.
- **Invariants Protected:** Invariant 20 (Exact Confirmation Validation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T105: Store Confirmation with Mismatched Candidate Root Digest Rejected (R29-B3)
- **Preconditions:** Coordinator submits candidate root with digest $D_{\text{cand}}$.
- **Trigger:** Store returns `COMMITTED` confirmation with `committedRootDigest: "corrupted_digest"`.
- **Expected Result:** Digest validation fails. Throws `INVALID_COMMITTED_CONFIRMATION`. Coordinator latches into `RECONCILING_CUSTODY`; local pointer update withheld.
- **Invariants Protected:** Invariant 20 (Exact Candidate Digest Confirmation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T106: Store Confirmation with Mismatched Expected Prior Sequence or Digest Rejected (R29-B3)
- **Preconditions:** Coordinator captures prior sequence 10 and prior digest $D_{\text{prior}}$.
- **Trigger:** Store returns `COMMITTED` with `expectedPriorSequence: 9`.
- **Expected Result:** Prior version validation fails. Throws `INVALID_COMMITTED_CONFIRMATION`. Latches into `RECONCILING_CUSTODY`.
- **Invariants Protected:** Invariant 20 (Prior Version Contiguity Confirmation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T107: Concurrent Asynchronous Publication Attempts Serialized by #publicationLock FIFO Queue (R29-B4)
- **Preconditions:** Two concurrent asynchronous operations invoke `_executeTwoPhasePublication` in the same event loop tick.
- **Trigger:** Operation 1 enters Phase 1 and acquires lock; awaits store persistence. Operation 2 invokes `acquirePublicationLock()`.
- **Expected Result:** Operation 2 is enqueued in `#waitQueue` without throwing or busy-waiting. When Operation 1 completes its `finally` block, Operation 2's promise resolves in FIFO order. Both publications execute contiguously without interleaving.
- **Invariants Protected:** Invariant 20 (Process-Local Publication Lock Lifecycle).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T108: Publication Lock Guaranteed Release in finally After Unexpected Exception (R29-B4)
- **Preconditions:** Publication attempt active under `#publicationLock`.
- **Trigger:** Store throws an unhandled network error or rejection during `publishFencedRoot()`.
- **Expected Result:** `_executeTwoPhasePublication` catches error, clears `#activePublicationAttempt`, and executes `releasePublicationLock()` in `finally`. Next queued caller acquires lock cleanly; lock is never orphaned.
- **Invariants Protected:** Invariant 20 (Publication Lock Exception Safety).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T109: Coordinator Process Crashes and Restarts During In-Flight Store Persistence (R29-B4)
- **Preconditions:** Coordinator crashes while a store publication request is in flight.
- **Trigger:** Node restarts coordinator on same host; boots with persisted WAL `bootGeneration = G + 1` and fresh `bootInstanceId`.
- **Expected Result:** In-memory queue and active attempt are wiped cleanly. Uncommitted local root candidate is discarded. Coordinator runs `getAuthoritativeRoot()` to synchronize with store before admitting new traffic.
- **Invariants Protected:** Invariant 21 (Cross-Restart Boot Isolation & Clean Slate).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T110: Store Unavailability During Reconciliation Remains Fail-Closed in RECONCILING_CUSTODY (R29-B5)
- **Preconditions:** Coordinator latched in `RECONCILING_CUSTODY`.
- **Trigger:** `reconcileAuthoritativeCustody()` fails due to store connection timeout.
- **Expected Result:** Gate state remains strictly in `RECONCILING_CUSTODY`. Zero automated fallback to local cache, zero timeout-based gate clearance, zero peer report clearance. Fail-closed isolation upheld.
- **Invariants Protected:** Invariant 23 (Fail-Closed Reconciliation Gate).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T111: Recovery Operations Attempted During RECONCILING_CUSTODY Rejected Fail-Closed (R29-B5)
- **Preconditions:** Coordinator latched in `RECONCILING_CUSTODY`.
- **Trigger:** External callers invoke `publishReleaseAndObligation()`, `releaseRetryLease()`, or `releaseSettlementPin()`.
- **Expected Result:** All mutating operations throw `CUSTODY_GATE_RECONCILING: custody state uncertain; mutations prohibited pending authoritative reconciliation` synchronously.
- **Invariants Protected:** Invariant 23 (Reconciliation Gate Operation Isolation).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T112: Newly Elected Leader Recovers Committed Authoritative Root Before Opening Gate (R29-B5)
- **Preconditions:** Coordinator A (Term 1) successfully committed sequence 12 in authoritative store but lost leadership before updating local pointer. Coordinator B elected for Term 2.
- **Trigger:** Coordinator B initializes recovery subsystem.
- **Expected Result:** Coordinator B invokes `store.getAuthoritativeRoot()`, installs committed sequence 12 root as its initial `#custodyRoot`, initializes `#publicationSequenceCounter = 12`, and opens the publication gate in Term 2. Zero data loss or state divergence.
- **Invariants Protected:** Invariant 20 & 23 (New Leader Authoritative State Recovery).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T113: Indexing Mutation on EncapsulatedByteArray Fails to Mutate Underlying Bytes (R30-1)
- **Preconditions:** Candidate custody record containing a byte array is cloned via `deepCloneAndFreezeValue()`.
- **Trigger:** Caller obtains the frozen cloned record and attempts to mutate byte contents via indexed assignment: `cloned.payload[0] = 0xff;`.
- **Expected Result:** `cloned.payload` is an `EncapsulatedByteArray`. The assignment fails to alter the underlying `#bytes` storage (in strict mode, throws a `TypeError` because the wrapper is frozen via `Object.freeze()`; in non-strict mode, has zero effect). Calling `cloned.payload.asDetachedUint8Array()` returns the original unmodified bytes.
- **Invariants Protected:** Invariant 18 (Restricted Custody Universe & Encapsulated Binary Storage).
- **Classification:** Future M5.1 implementation test (architecture requirement).

### Test R18-R33-T114: Typed-Array Instance with Own Symbol.toStringTag Accessor Getter Rejected Without Getter Execution (R30-2)
- **Preconditions:** Caller constructs a `Uint8Array` instance and defines an own `Symbol.toStringTag` accessor property with a hostile getter:
  `const u8 = new Uint8Array(4); Object.defineProperty(u8, Symbol.toStringTag, { get() { getterCalled = true; return 'Uint8Array'; }, configurable: true });`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `assertValidOrdinaryUint8Array` inspects `Object.getOwnPropertySymbols(u8)`. Detecting a symbol property, it immediately throws `UNSUPPORTED_BINARY_VALUE_TYPE: symbol properties on typed arrays are prohibited`. `getterCalled` remains strictly `false`.
- **Invariants Protected:** Invariant 18 & 22 (Intrinsic Typed-Array Inspection & Getter Immunity).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T115: Typed-Array Instance with Own Shadowed Buffer Properties Evaluated via Intrinsic Prototype Accessors (R30-2)
- **Preconditions:** A `Uint8Array` instance defines an own property shadowing `'buffer'` with a hostile getter:
  `const u8 = new Uint8Array(4); Object.defineProperty(u8, 'buffer', { get() { getterCalled = true; throw new Error('HOSTILE_GETTER'); }, configurable: true });`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** `assertValidOrdinaryUint8Array` detects non-index property name `'buffer'` and throws `UNSUPPORTED_BINARY_VALUE_TYPE: typed array contains non-index property 'buffer'`. `getterCalled` remains strictly `false`.
- **Invariants Protected:** Invariant 22 (Typed-Array Property Shadowing Prohibition).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T116: Typed-Array Subclass Rejected via Prototype Identity Check (R30-2)
- **Preconditions:** Caller constructs an instance of a `Uint8Array` subclass:
  `class CustomBuffer extends Uint8Array {} const custom = new CustomBuffer(8);`.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Prototype comparison detects `Object.getPrototypeOf(custom) !== Uint8Array.prototype`. Immediately throws `UNSUPPORTED_BINARY_VALUE_TYPE: only direct Uint8Array instances are supported; subclasses prohibited` without invoking subclass methods or getters.
- **Invariants Protected:** Invariant 18 & 22 (Subclass Rejection & Prototype Strictness).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T117: Cross-Realm SharedArrayBuffer Backing Store Rejected via Intrinsic Brand Check (R30-2)
- **Preconditions:** A `Uint8Array` instance created over a `SharedArrayBuffer` originating from a separate execution realm.
- **Trigger:** Passed to `deepCloneAndFreezeValue()`.
- **Expected Result:** Intrinsic brand check evaluates `Object.prototype.toString.call(buffer) === '[object SharedArrayBuffer]'`. Throws `UNSUPPORTED_BINARY_VALUE_TYPE: SharedArrayBuffer-backed views are prohibited`.
- **Invariants Protected:** Invariant 18 (Prohibited Shared Memory).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T118: Authoritative Commit Confirmation Mismatch on attemptId Triggers Fail-Closed Invalidation (R30-3)
- **Preconditions:** Coordinator submits publication attempt with `attemptId = "att:uuid-A:10"`.
- **Trigger:** Authoritative store returns a delayed `AuthoritativeCommittedConfirmation` containing `attemptId = "att:uuid-A:9"` (from a superseded attempt).
- **Expected Result:** Coordinator confirmation validation detects `outcome.attemptId !== attemptContext.attemptId`. Throws `INVALID_COMMITTED_CONFIRMATION: authoritative store returned inconsistent commit confirmation`. Coordinator latches into `RECONCILING_CUSTODY`; local `#custodyRoot` update is withheld.
- **Invariants Protected:** Invariant 20 (Exact 8-Dimensional Publication Identity Binding).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T119: Queued Publication Callers in Wait Queue Immediately Rejected with COORDINATOR_STOPPED on Step-Down (R30-5)
- **Preconditions:** Coordinator has one active in-flight publication and two callers queued in `#waitQueue` awaiting `#publicationLock`.
- **Trigger:** Consensus leadership is lost; coordinator executes `leadershipFence.stepDown(newTerm)`.
- **Expected Result:** `stepDown()` immediately iterates `#waitQueue`, rejects all queued callers with `COORDINATOR_STOPPED: coordinator stepped down while awaiting publication lock`, and clears the queue. Zero queued attempts are executed under the old or subsequent term.
- **Invariants Protected:** Invariant 20 (Lock Lifecycle Queue Purging on Step-Down).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T120: In-Flight Store Persistence Arrives After Leadership Lease Expiry and is Safely Discarded (R30-5)
- **Preconditions:** Coordinator dispatches store persistence call under Term 1. Store commits candidate root. Response is delayed across a network pause exceeding the coordinator's lease duration.
- **Trigger:** Delayed store confirmation arrives after monotonic clock exceeds `#leaseExpiresAtMonotonicMs`.
- **Expected Result:** Post-await completion check `assertPostAwaitEligibility()` detects lease expiration. Throws `CUSTODY_PUBLICATION_STALE_LEADERSHIP`. Local `#custodyRoot` is NOT updated. Coordinator steps down; new leader recovers committed root from store.
- **Invariants Protected:** Invariant 20 (Post-Await Lease Expiry Fencing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T121: Authoritative Reconciliation Proves Candidate Root Digest Equality and Rejects Mismatched Sequences (R30-4)
- **Preconditions:** Store contains committed sequence 15 with digest $D_{15}$. An uncertain attempt is queried for sequence 14 with digest $D_{14}$.
- **Trigger:** Coordinator runs `reconcilePublication(query)` for sequence 14.
- **Expected Result:** Store verifies that queried sequence 14 with digest $D_{14}$ does not match active committed sequence 15. Returns `RECONCILED_NOT_COMMITTED` for sequence 14 with `authoritativeCurrentSequence = 15`. Coordinator does not misclassify sequence 14 as committed merely because sequence 15 exists.
- **Invariants Protected:** Invariant 23 (Unambiguous Reconciliation Identity Semantics).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T122: Authoritative Store Rejects Conflicting Candidate Digest at Same Sequence with CONFLICTING_DIGEST_AT_SEQUENCE (R30-4)
- **Preconditions:** Store durably committed sequence 20 with candidate root digest $D_{\text{alpha}}$.
- **Trigger:** A delayed or retried request arrives with sequence 20 but candidate root digest $D_{\text{beta}} \neq D_{\text{alpha}}$.
- **Expected Result:** Authoritative store detects state conflict; permanently rejects the request with `CONFLICTING_DIGEST_AT_SEQUENCE`. Sequence 20 remains immutably bound to $D_{\text{alpha}}$.
- **Invariants Protected:** Invariant 20 & 23 (Publication Digest Immutability & Conflict Rejection).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T123: Cross-Realm SharedArrayBuffer Backing Store Rejection via Intrinsic Getter (R31-1)
- **Preconditions:** Caller constructs a `SharedArrayBuffer` in another JavaScript realm (e.g. via `vm.createContext()` or worker context) and wraps it in a local `Uint8Array` view.
- **Trigger:** Submitted to `deepCloneAndFreezeValue()`.
- **Expected Result:** Prototype equality check passes (view was wrapped in current realm), but `assertValidOrdinaryUint8Array` extracts backing buffer and invokes intrinsic `%SharedArrayBuffer%.prototype.byteLength` getter via `Reflect.apply`. Succeeded call detects `[[SharedArrayBufferData]]` internal slot across realm boundaries. Throws `TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: SharedArrayBuffer backing stores prohibited')`.
- **Invariants Protected:** Invariant 18 (Cross-Realm Binary Encapsulation & Shared Memory Prohibition).
- **Classification:** Future M5.1 implementation test (architecture requirement).

### Test R18-R33-T124: Rejection of Subclassed EncapsulatedByteArray via isTrustedInstance (R31-2)
- **Preconditions:** Caller defines `class CustomBytes extends EncapsulatedByteArray { asDetachedUint8Array() { return new Uint8Array([0xff]); } }` and instantiates `new CustomBytes(new Uint8Array([1, 2, 3]))`.
- **Trigger:** Submitted to `deepCloneAndFreezeValue()` or passed as source to `new EncapsulatedByteArray(sub)`.
- **Expected Result:** `EncapsulatedByteArray.isTrustedInstance(sub)` checks prototype identity (`Object.getPrototypeOf(sub) !== EncapsulatedByteArray.prototype`) and returns false. Subclass branch triggers and throws `TypeError('UNSUPPORTED_BINARY_VALUE_TYPE: EncapsulatedByteArray subclasses and untrusted instances prohibited')`. Overridden methods are never invoked.
- **Invariants Protected:** Invariant 18 (Trusted Binary Value Object Recognition).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T125: Rejection of Uint8Array with Extra Own String Properties (R31-3)
- **Preconditions:** Caller creates `const arr = new Uint8Array([1, 2, 3]); arr.customMetadata = "secret";`.
- **Trigger:** Submitted to `deepCloneAndFreezeValue()`.
- **Expected Result:** `assertValidOrdinaryUint8Array` inspects own property names; detects property `'customMetadata'` which does not match canonical integer index pattern `^(?:0|[1-9]\d*)$`. Throws `TypeError("UNSUPPORTED_ARRAY_PROPERTY: extra own string properties on typed arrays prohibited; found 'customMetadata'")`. Metadata is not silently dropped.
- **Invariants Protected:** Invariant 18 (Typed Array Property Canonicalization).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T126: Store Rejection of Already-Committed Sequence with Differing attemptId (R31-6)
- **Preconditions:** Store has durably committed publication sequence $S = 40$ under `attemptId = "att-40-A"` with candidate digest $D$.
- **Trigger:** A new publication request arrives for sequence $S = 40$ presenting the identical candidate digest $D$ but a differing `attemptId = "att-40-B"`.
- **Expected Result:** Store applies Rule 2b. Rejects request with `DUPLICATE_SEQUENCE_CONFLICT: sequence already committed under different attempt identity`. Coordinator latches into `RECONCILING_CUSTODY`.
- **Invariants Protected:** Invariant 20 & 22 (Sequence Uniqueness & Store Idempotency).
- **Classification:** Future M5.1 implementation test.

---

### Test R18-R33-T127: EncapsulatedByteArray Prototype Method Tampering Immunity (R32-1)
- **Preconditions:** Hostile caller attempts monkey-patching `EncapsulatedByteArray.prototype.asDetachedUint8Array = () => new Uint8Array([0xff])`.
- **Trigger:** An authentic instance `source` is passed to `new EncapsulatedByteArray(source)` or `.equals(other)`.
- **Expected Result:** `Object.freeze(EncapsulatedByteArray.prototype)` prohibits prototype mutation (throws `TypeError` in strict mode). Furthermore, constructor and `equals()` access `source.#bytes` and `source.#canonicalHex` directly via private fields with zero dynamic method dispatch. Overridden prototype methods are never called. Original bytes `[1, 2, 3]` are preserved immutably.
- **Invariants Protected:** Invariant 18 (Direct Private Field Encapsulation).
- **Classification:** Future M5.1 implementation test (architecture requirement).

### Test R18-R33-T128: Linearizable Store Reconciliation Fencing Prevents In-Flight Commit (R32-2)
- **Preconditions:** Coordinator submits publication sequence $S=50$ (`attemptId = "att-50"`); times out and records `UNCERTAIN`. Original request is still in-flight at authoritative store.
- **Trigger:** Coordinator invokes `reconcilePublication(query)`. Query sees sequence $S=50$ uncommitted; store atomically persists `TERMINAL_ATTEMPT_FENCED` tombstone for `"att-50"` and returns `NOT_COMMITTED_FINAL`. Subsequently, delayed original publication request finishes execution and attempts to commit sequence 50.
- **Expected Result:** Store detects durable terminal fencing tombstone; rejects delayed attempt with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION`. Local coordinator never misclassifies a committed record.
- **Invariants Protected:** Invariant 20 & 23 (Linearizable Reconciliation Fencing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T129: Store Reconciliation Unable to Atomically Fence Returns UNKNOWN_IN_PROGRESS Fail-Closed (R32-2)
- **Preconditions:** Coordinator invokes `reconcilePublication()` for an uncertain attempt, but store encounters a network partition / disk failure and cannot durably persist the `TERMINAL_ATTEMPT_FENCED` tombstone.
- **Trigger:** Store executes reconciliation query.
- **Expected Result:** Store returns `UNKNOWN_IN_PROGRESS: store unable to persist terminal fencing tombstone`. Coordinator throws `CUSTODY_GATE_RECONCILING` and remains strictly fail-closed in `RECONCILING_CUSTODY`. Zero mutations permitted.
- **Invariants Protected:** Invariant 23 (Fail-Closed Gate State).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T130: Sequence Collision with Differing Attempt ID Takes Precedence Over Digest Mismatch (R32-6)
- **Preconditions:** Store has durably committed sequence $S=60$ (`attemptId = "att-60-A"`, digest $D_A$).
- **Trigger:** Publication request arrives for sequence $S=60$ presenting BOTH a differing attempt ID (`attemptId = "att-60-B"`) AND a differing candidate digest ($D_B \neq D_A$).
- **Expected Result:** Store evaluates deterministic conflict precedence: attempt identity check takes absolute precedence. Permanently rejected with `DUPLICATE_SEQUENCE_CONFLICT: sequence already committed under different attempt identity`.
- **Invariants Protected:** Invariant 22 (Deterministic Sequence Conflict Precedence).
- **Classification:** Future M5.1 implementation test.

---

### Test R18-R33-T131: Static isTrustedInstance Monkey-Patching Blocked and Irrelevant to Internal Custody (R33-1)
- **Preconditions:** Hostile consumer attempts to replace `EncapsulatedByteArray.isTrustedInstance = () => true;`.
- **Trigger:** Attempt assignment and pass forged plain object to `deepCloneAndFreezeValue()`.
- **Expected Result:** `Object.freeze(EncapsulatedByteArray)` prohibits assignment (throws `TypeError` in strict mode). Furthermore, `deepCloneAndFreezeValue()` and constructor execute strictly via unexported lexical helper `isLexicallyTrustedEncapsulatedByteArray`, which evaluates `#bytes in val` and exact prototype identity. Forged object is rejected with `UNSUPPORTED_PROTOTYPE_INSTANCE`.
- **Invariants Protected:** Invariant 18 (Static API Immutability & Lexical Brand Checking).
- **Classification:** Future M5.1 implementation test (architecture requirement).

### Test R18-R33-T132: Constructor and Prototype Tampering Blocked by Object.freeze (R33-1)
- **Preconditions:** Caller attempts to mutate `EncapsulatedByteArray.prototype.toCanonicalHex = () => "forged"` and `EncapsulatedByteArray.someProp = 123`.
- **Trigger:** Executed in strict mode.
- **Expected Result:** Both mutations throw `TypeError: Cannot add/assign property on frozen object`. Original prototype methods and constructor remain strictly immutable.
- **Invariants Protected:** Invariant 18 (Prototype and Constructor Freezing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T133: Linearizable Commit Race: Publication Commits Immediately Before Reconciliation (R33-3)
- **Preconditions:** Coordinator submits publication sequence $S=70$ (`attemptId = "att-70"`). Times out and triggers `reconcilePublication()`.
- **Trigger:** Store consensus log commits publication request at log index 100 immediately before reconciliation query evaluates at index 101.
- **Expected Result:** Reconciliation query reads log index 100; detects committed publication under identical 8D tuple; returns `RECONCILED_COMMITTED` with committed root. Zero misclassification.
- **Invariants Protected:** Invariant 20 (Linearizable Shared Log Commit Ordering).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T134: Linearizable Fencing Race: Reconciliation Fences Attempt Immediately Before Delayed Commit (R33-3)
- **Preconditions:** Coordinator submits publication sequence $S=71$ (`attemptId = "att-71"`). Times out and triggers reconciliation.
- **Trigger:** Reconciliation query executes at log index 200, observes uncommitted sequence, and commits `TERMINAL_ATTEMPT_FENCED` tombstone. Delayed original publication request evaluates at index 201.
- **Expected Result:** Delayed publication request detects durable terminal tombstone matching its `attemptId`; permanently rejected with `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION`. Fenced attempt cannot commit.
- **Invariants Protected:** Invariant 20 & 23 (Linearizable Reconciliation Fencing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T135: Lost Publication Response Idempotently Recovered via Reconciliation (R33-3)
- **Preconditions:** Store commits publication sequence $S=72$ under 8D tuple, but acknowledgment is lost over network.
- **Trigger:** Coordinator records `UNCERTAIN` and invokes `reconcilePublication()` presenting identical 8D tuple.
- **Expected Result:** Store queries linearizable state; matches identical 8D tuple; returns `RECONCILED_COMMITTED` idempotently with original committed root. Coordinator installs root and recovers cleanly.
- **Invariants Protected:** Invariant 20 & 22 (Lost-Response Idempotent Recovery).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T136: Sequence Collision with Matching attemptId but Conflicting Candidate Digest (R33-5)
- **Preconditions:** Store has committed sequence $S=73$ under `attemptId = "att-73"` with digest $D_A$.
- **Trigger:** Publication request arrives for sequence $S=73$ with `attemptId = "att-73"` but conflicting candidate digest $D_B \neq D_A$.
- **Expected Result:** Store applies Precedence Step 2; detects candidate digest mismatch; permanently rejected with `CONFLICTING_DIGEST_AT_SEQUENCE`.
- **Invariants Protected:** Invariant 22 (Candidate Digest Collision Precedence).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T137: Delayed Publication Carrying Stale Term After Terminal Fencing (R33-3)
- **Preconditions:** Coordinator term 1 was terminally fenced with `TERMINAL_ATTEMPT_FENCED` tombstone. Store consensus advances to term 2.
- **Trigger:** A delayed publication packet from term 1 arrives at the store.
- **Expected Result:** Evaluated against shared consensus state; rejected with `CUSTODY_PUBLICATION_STALE_LEADERSHIP` and `ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION`. Stale term cannot alter store state.
- **Invariants Protected:** Invariant 20 (Multi-Epoch Leadership Fencing).
- **Classification:** Future M5.1 implementation test.

### Test R18-R33-T138: Store Quorum Failure During Reconciliation Fencing Returns UNKNOWN_IN_PROGRESS Fail-Closed (R33-3)
- **Preconditions:** Coordinator reconciles uncommitted sequence $S=74$, but store consensus log encounters network partition and loses quorum during terminal tombstone write.
- **Trigger:** Store executes reconciliation query.
- **Expected Result:** Store cannot prove linearizable outcome; returns `UNKNOWN_IN_PROGRESS: consensus write failed to achieve quorum`. Coordinator throws `CUSTODY_GATE_RECONCILING` and remains strictly fail-closed in `RECONCILING_CUSTODY`. Zero mutations permitted.
- **Invariants Protected:** Invariant 23 (Fail-Closed Gate State on Consensus Uncertainty).
- **Classification:** Future M5.1 implementation test.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
M5.0-R18-R33 FORMAL QUESTIONS FOR ENGINEERING AUTHORITY
================================================================================
1. [LEXICAL BRAND CHECK HELPER & FROZEN STATIC API (R33-1 / BLOCKER A)]: Does
   the Engineering Authority approve executing all internal custody brand checks
   exclusively via the unexported lexical helper isLexicallyTrustedEncapsulatedByteArray,
   bound via a static {} initialization block, combined with freezing both the
   class prototype and constructor API (Object.freeze(EncapsulatedByteArray.prototype)
   and Object.freeze(EncapsulatedByteArray))?

2. [FULL 8D IDENTITY TOMBSTONE BINDING (R33-2 / BLOCKER B)]: Does the
   Engineering Authority approve binding the terminal fencing tombstone
   TERMINAL_ATTEMPT_FENCED to the complete 8-dimensional tuple <clusterTerm,
   bootGeneration, bootInstanceId, publicationSequence, attemptId, candidateRootDigest,
   expectedPriorSequence, expectedPriorRootDigest>?

3. [SHARED LINEARIZABLE STORE ORDERING BOUNDARY (R33-3 / BLOCKER B)]: Does the
   Engineering Authority approve serializing publication commit and reconciliation
   fencing through the identical shared consensus log / transactional CAS state
   machine, ensuring publication commit and terminal tombstone cannot both win,
   rejecting delayed attempts with ATTEMPT_TERMINALLY_ABORTED_BY_RECONCILIATION,
   and returning UNKNOWN_IN_PROGRESS fail-closed if finality cannot be established?

4. [FAIL-CLOSED SharedArrayBuffer CAPABILITY BOUNDARY (R32-3)]: Does the
   Engineering Authority approve the fail-closed boundary throwing
   UNSUPPORTED_BINARY_VALUE_TYPE if the intrinsic SharedArrayBuffer byteLength
   getter is unavailable in the host realm?

5. [PROXY-SAFE TRUSTED-INSTANCE INSPECTION (R32-4)]: Does the Engineering
   Authority approve wrapping prototype inspection in try...catch inside the
   lexical brand checker to cleanly return false on hostile throwing Proxy traps?

6. [DETERMINISTIC CONFLICT PRECEDENCE (R32-6)]: Does the Engineering Authority
   approve that sequence collisions under differing attemptId (DUPLICATE_SEQUENCE_CONFLICT)
   take absolute precedence over candidate digest mismatches (CONFLICTING_DIGEST_AT_SEQUENCE)?

7. [DIRECT PRIVATE-FIELD ACCESS (R32-1)]: Does the Engineering Authority
   approve reading source.#bytes and source.#canonicalHex directly across instances
   within the class lexical scope, bypassing all public method dispatch?

8. [EIGHT-DIMENSIONAL PUBLICATION IDENTITY TUPLE]: Does the Engineering
   Authority approve the complete 8-dimensional publication identity tuple bound
   across publication, store confirmation, and reconciliation?

9. [LOCK QUEUE PURGING ON LEADERSHIP LOSS]: Does the Engineering Authority
   approve immediately rejecting all queued publication callers with
   COORDINATOR_STOPPED and purging #waitQueue upon leadership stepDown()?

10. [CLOSURE OF ALL CORRECTNESS BLOCKERS & FINAL ACCEPTANCE READINESS]: Does
    the Engineering Authority agree that all review issues across R18 through R33
    are fully resolved, that custody immutability and linearizable reconciliation
    fencing are genuinely enforced, and that Milestone M5.0 is ready for final
    acceptance?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R33 ARCHITECTURAL DECISION PROPOSAL VERDICT:
PASS — READY FOR HOLISTIC M5.0 FINAL ACCEPTANCE REVIEW
================================================================================
```

This R18-R33 proposal achieves complete architectural closure of the recovery arbitration subsystem. It establishes static custody API immutability via an unexported lexical brand-check helper (`isLexicallyTrustedEncapsulatedByteArray`) and freezes both the class constructor and prototype (`Object.freeze(EncapsulatedByteArray)`, `Object.freeze(EncapsulatedByteArray.prototype)`), eliminating all public static monkey-patching vulnerabilities; establishes full 8-dimensional publication identity tombstone binding; formalizes the shared linearizable store ordering boundary between publication commit and reconciliation fencing; enforces fail-closed `SharedArrayBuffer` brand checking; implements Proxy-safe trusted-instance verification; establishes deterministic sequence conflict precedence; and codifies one hundred and thirty-eight deterministic future test specifications.

*Note on Acceptance Discipline:* Completion of Milestone M5.0-R18-R33 constitutes architectural readiness. Formal M5.0 milestone acceptance requires holistic review and explicit Engineering Authority sign-off prior to authorizing Milestone M5.1.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R33 is a documentation-only architectural correction. No production implementation has been authorized. M5.1, including M5.1a, remains strictly unauthorized pending holistic M5.0 acceptance and explicit Engineering Authority approval."
