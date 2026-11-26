# Phase 7.5 M5.0-R18-R27 — Final Deep-Immutability and Leadership-Fencing Closure
**Concurrent Recovery Arbitration & Authoritative Settlement Architecture Decision**

**Status:** PROPOSED ARCHITECTURAL AMENDMENT TO FROZEN M5.0 SPECIFICATIONS (DECISION DOCUMENT ONLY)
**Author:** Principal TypeScript Distributed-Systems Architect and Independent Recovery-Systems Auditor
**Baseline Commit:** `863a6ec0dd063ebc75214d8a1a45e24facf1ed0f` (M5.0-R18-R26)
**Direct Parent Commit:** `863a6ec0dd063ebc75214d8a1a45e24facf1ed0f`
**Authorization Scope:** M5.0-R18-R27 is strictly a documentation-only architectural proposal. No production source code, tests, configuration, package manifests, or frozen M5.0 specifications may be modified. Implementation of Milestone M5.1, including M5.1a, remains **STRICTLY BLOCKED AND UNAUTHORIZED**.

---

## 1. Problem Statement & R18-R27 Objectives

In Phase 7.5 Sub-Milestone M5.0-R18-R26, we encapsulated collection backing storage using ECMAScript `#private` fields and introduced single-writer lease fencing. However, the subsequent independent architectural review identified two remaining concrete correctness blockers and requested targeted adversarial test expansion:

1. **Restricted Custody-Value Universe and Deep Cloning Contract (R27-A):**
   - *Previous Defect:* The R26 `deepCloneAndFreezeValue()` helper did not safely support all object forms it accepted: symbol-keyed properties were copied by reference; custom class prototypes were stripped to plain objects; property accessors (getters/setters) could execute during cloning; non-enumerable properties were silently dropped; arrays with non-index properties were unhandled; cyclic references caused unhandled recursion; `Uint8Array` instances were incorrectly described as frozen; and unsupported binary or temporal types (`Date`, `ArrayBuffer`, `DataView`, non-Uint8Array typed arrays) were not explicitly rejected.
   - *Architectural Resolution in R27:*
     - Define a deliberately restricted and enforceable custody-value universe:
       - **Supported Primitives:** `string`, finite safe `number` (NaN and $\pm\infty$ rejected), `boolean`, `null`, `undefined`.
       - **Plain Records:** strictly objects whose prototype is `Object.prototype` or `null`. All own properties (strings and symbols) must be enumerable data properties (no accessors).
       - **Plain Arrays:** strictly `Array.prototype` instances with index-only elements and no custom properties.
       - **Byte Arrays:** `Uint8Array` cloned to fresh buffers on ingress and egress. Explicitly documented that egress copies are detached, mutable `Uint8Array` buffers whose modification has zero effect on custody storage.
     - Enforce cycle detection via `WeakSet<object>`, failing closed with `CIRCULAR_REFERENCE_DETECTED`.
     - Inspect property descriptors via `Object.getOwnPropertyDescriptor()`: reject getters/setters with `ACCESSOR_PROPERTIES_PROHIBITED` without executing them; reject non-enumerable properties with `NON_ENUMERABLE_PROPERTY_PROHIBITED`.
     - Symbol-keyed properties are recursively cloned and defined as non-writable, non-configurable own data properties.
     - Fail closed on unsupported types: `Map`, `Set`, `Date`, `RegExp`, `ArrayBuffer`, `SharedArrayBuffer`, `DataView`, non-Uint8Array typed arrays, and functions.
     - Apply equivalent rigor across `ImmutableMap` and `ImmutableSet`.

2. **Authoritative Boundary Fencing & Linearization Closure (R27-B):**
   - *Previous Defect:* In R26, local lease validation and local pointer swap were separate sequential operations within the coordinator process. JavaScript run-to-completion prevents microtask interleaving, but does not prevent operating system thread preemption, hypervisor scheduling pauses, or garbage collection stalls from letting monotonic time elapse past lease expiry before the pointer swap. Furthermore, a process-local `#publicationLock` cannot serialize independent coordinator processes.
   - *Architectural Resolution in R27:*
     - Specify the **Authoritative Boundary Fencing Model**: authoritative custody state is owned by an Authoritative Custody Store (`IAuthoritativeCustodyStore`) backed by cluster consensus or durable shared storage.
     - Every candidate root mutation carries a compound fencing token $\langle \mathbf{clusterTerm}, \mathbf{bootGeneration}, \mathbf{bootInstanceId}, \mathbf{publicationSequence} \rangle$.
     - The authoritative store performs an **Atomic Fenced Compare-and-Swap (CAS)**: if the coordinator's lease has expired, its term has been superseded by election of a newer leader, or another coordinator committed a higher term/sequence, the authoritative store atomically rejects the mutation with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
     - The local coordinator instance's pointer swap `this.#custodyRoot = candidateRoot` occurs **if and only if** the authoritative store confirms atomic fenced commitment.
     - Local single-writer fencing enforces that `commitFn` is strictly synchronous: returning a Promise or yielding to the event loop is rejected with `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED`.
     - Local lease checks incorporate a configurable safety margin $\Delta_{\text{guard}}$ ($\text{monotonicNow} + \Delta_{\text{guard}} < \mathbf{leaseExpiresAtMonotonicMs}$), bounding local execution against thread pause jitter.
     - Formally define all assumptions regarding monotonic clocks, drift, lease duration, renewal, pause bounds, and the authoritative mutation boundary.

3. **Targeted Adversarial Verification Expansion (R27-C):**
   - Expand the deterministic failure matrix from 54 to 66 cases (Cases 55–66).
   - Codify sixty-six deterministic future test specifications (`Test R18-R27-T01` through `Test R18-R27-T66`), providing exact initial states, injected faults, expected outcomes, protected invariants, and custody ownership dispositions.

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

#### The Dual-Tier Fencing Architecture:
Distributed recovery cannot rely on local in-memory checks alone to fence out stale leaders. The architecture enforces two complementary fencing tiers:

1. **Tier 1: Authoritative Boundary Fencing (Authoritative Shared State):**
   - The authoritative custody state is governed by an Authoritative Custody Store (`IAuthoritativeCustodyStore`) backed by cluster consensus (e.g. Raft state machine) or durable shared storage.
   - Every candidate root mutation carries a compound fencing token:
     $$\mathbf{fencingToken} = \langle \mathbf{clusterTerm},\; \mathbf{bootGeneration},\; \mathbf{bootInstanceId},\; \mathbf{publicationSequence} \rangle$$
   - The authoritative store enforces **Atomic Fenced Compare-and-Swap (CAS)**:
     $$\mathbf{store.publishFencedRoot}(\mathbf{fencingToken},\; \mathbf{candidateRoot})$$
   - If the coordinator's term has been superseded ($\mathbf{clusterTerm} < \mathbf{store.highestCommittedTerm}$), its lease in consensus has expired, or another leader committed a publication, the authoritative store atomically rejects the mutation with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
   - Even if a coordinator process experiences an uncoordinated operating system suspension, hypervisor pause, or garbage collection stall of arbitrary duration and resumes after its lease has expired, its write is rejected at the authoritative boundary.
   - The local coordinator instance updates its cached pointer `this.#custodyRoot = candidateRoot` **if and only if** the authoritative store confirms atomic fenced commitment.

2. **Tier 2: Coordinator-Local In-Memory Fencing & Single-Writer Execution:**
   - **Process-Local Mutual Exclusion:** All publications on a coordinator instance execute inside `LeadershipEpochFence.executeFencedPublication()`, which acquires a non-reentrant single-writer execution lock (`#publicationLock`). Concurrent calls within the same process fail immediately with `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.
   - **Strictly Synchronous Commit Function:** `commitFn` must be strictly synchronous. Any callback returning a Promise or yielding to the event loop is rejected with `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED`.
   - **Immutable Candidate Root:** The candidate root is fully constructed and frozen before validation. Callbacks receive an immutable reference and cannot mutate the candidate root after validation.
   - **Guard Margin ($\Delta_{\text{guard}}$):** Local lease assertion checks:
     $$\text{monotonicNow} + \Delta_{\text{guard}} < \mathbf{leaseExpiresAtMonotonicMs}$$
     where $\Delta_{\text{guard}}$ (default 1,000ms) accounts for bounded local event-loop scheduling latency.

#### Formal System Assumptions for Split-Brain Exclusion:
Split-brain publication exclusion is guaranteed under the following explicit distributed systems model:
1. **Monotonic Clocks:** Local lease tracking uses `performance.now()`, which is monotonic, non-decreasing, and unaffected by NTP wall-clock steps or leap seconds.
2. **Bounded Clock Drift Rate:** Maximum clock drift between cluster nodes is bounded by $\rho \le 10^{-4}$ (standard quartz oscillator drift $< 100\,\mu\text{s/s}$).
3. **Consensus Lease Non-Overlap Invariant:** The cluster consensus quorum strictly enforces that no candidate for Term $T+1$ can be elected or receive a leadership lease until:
   $$t \ge t_{\text{term } T\text{ lease granted}} + \tau_{\text{lease}} + \epsilon_{\text{drift}}$$
   where $\tau_{\text{lease}}$ is the consensus lease duration (default 5,000ms) and $\epsilon_{\text{drift}}$ is the maximum drift allowance.
4. **Authoritative Boundary Enforcement:** Any state mutation that commits custody transition must be acknowledged by a quorum of consensus nodes validating $\mathbf{fencingToken.clusterTerm} \ge \mathbf{quorum.currentTerm}$. Stale terms are rejected.
5. **Failure Disposition:** When fencing fails at either tier:
   - The candidate root is discarded.
   - `this.#custodyRoot` remains strictly untouched.
   - In-flight custody is retained locally as degraded.
   - The settlement barrier is left unresolved.
   - The coordinator transitions to `CUSTODY_RECONCILIATION_BLOCKED`.

#### Adversarial Interleaving Scenario: Partitioned Stale Leader Race

The following concrete timeline illustrates how the dual-tier fencing model prevents a stale partitioned coordinator from committing a publication after a newer term becomes authoritative:

| Time | Coordinator A (Term 1 Leader) | Cluster Consensus Quorum | Coordinator B (Term 2 Candidate) | Authoritative Custody Store | System Safety State |
|---|---|---|---|---|---|
| $t_0$ | Holds active lease for Term 1 until $t_0 + 5000\text{ms}$. | Quorum recognizes Coord A as leader for Term 1. | Follower in Term 1. | Highest committed term = 1. | Normal operation under Term 1. |
| $t_1$ | Network partition isolates Coord A. Dequeues op $g=2$; constructs `candidateRoot`. | Heartbeats from Coord A lost. | Awaiting election timeout. | Highest committed term = 1. | Partition active; Coord A preparing root. |
| $t_2$ | Enters OS process suspension / GC pause (duration 6,500ms). | Election timeout expires ($t_2 = t_0 + 5100\text{ms}$). Lease for Term 1 has definitively elapsed ($> 5000\text{ms}$). | Initiates election for Term 2. | Highest committed term = 1. | **Lease Non-Overlap Invariant holds:** Coord A lease expired before Term 2 granted. |
| $t_3$ | Still suspended. | Quorum elects Coord B leader for Term 2; grants lease valid $t_3 \to t_3 + 5000\text{ms}$. | Elected leader for Term 2. | Highest committed term updated to 2. | Coord B is sole valid cluster leader. |
| $t_4$ | Suspension ends at $t_4 = t_0 + 6500\text{ms}$. Resumes execution and invokes `executeFencedPublication(expectedTerm: 1)`. | Active in Term 2 under Coord B. | Processing admissions in Term 2. | Highest committed term = 2. | Coord A attempts to publish stale candidate. |
| $t_5$ | **Tier 1 / Local Check:** `monotonicNow` ($t_4$) exceeds `#leaseExpiresAtMonotonicMs` ($t_0 + 5000\text{ms}$). Throws `STALE_COORDINATOR_LEADERSHIP_FENCED`. | Active in Term 2. | Active in Term 2. | Highest committed term = 2. | **Local Fencing:** Candidate discarded; `#custodyRoot` untouched. |
| $t_{5\text{alt}}$ | *(Alternative: local clock stalled)* Coord A bypasses local check and submits CAS to Authoritative Store. | Quorum in Term 2. | Active in Term 2. | **Authoritative Store CAS:** Rejects token Term 1 < Current Term 2 with `STALE_COORDINATOR_LEADERSHIP_FENCED`. | **Authoritative Fencing:** Write rejected; store untouched; split-brain impossible. |

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

### 12.9 The Restricted Custody-Value Universe and Deep Immutability Contract

In ECMAScript environments, applying `Object.freeze()` to native `Map` or `Set` instances freezes only the object prototype and property descriptors; it does **not** intercept internal slots, meaning native prototype methods such as `Map.prototype.set.call(frozenMap, key, value)`, `.delete()`, and `.clear()` continue to mutate the collection contents in place. Furthermore, applying `Object.freeze()` to a parent object freezes only its own shallow properties; any nested object references or typed array buffers (`Uint8Array`) remain fully mutable through returned references unless recursively cloned and frozen.

To enforce absolute custody-root immutability across every access path without undefined semantics or prototype pollution, the architecture defines a **Restricted Custody-Value Universe** and an enforceable deep cloning contract:

#### 1. The Restricted Custody-Value Universe:
A value $v$ is an admissible custody value if and only if it belongs to one of four categories:
1. **Admissible Primitives:**
   - `string`
   - Finite `number` (`Number.isFinite(v) === true`; `NaN`, `+Infinity`, and `-Infinity` are strictly rejected with `INVALID_NUMERIC_VALUE`)
   - `boolean`
   - `null`
   - `undefined`
2. **Byte Arrays (`Uint8Array`):**
   - Strictly `instanceof Uint8Array`.
   - On ingress and copy-on-write, the underlying buffer is cloned to a newly allocated `Uint8Array`.
   - On egress (`.get()`, `.values()`, `.entries()`, iterators), a detached, newly allocated mutable `Uint8Array` copy is returned.
   - *Architectural Distinction:* The returned byte array is a detached mutable copy. Callers may modify its bytes without mutating or affecting the internal authoritative custody root backing store.
3. **Plain Arrays:**
   - Strictly `Object.getPrototypeOf(v) === Array.prototype`.
   - Array length must be a non-negative safe integer.
   - All own property keys of the array must be valid index strings (`0`, `1`, ..., `length - 1`) or `'length'`. Any non-index property (string or symbol) is rejected with `UNSUPPORTED_ARRAY_PROPERTY`.
   - Every element must recursively belong to the restricted custody-value universe.
   - The array is frozen with `Object.freeze()`.
4. **Plain Structural Records:**
   - Strictly `Object.getPrototypeOf(v) === Object.prototype` or `Object.getPrototypeOf(v) === null`. Custom prototypes or class instances (e.g. `EventEmitter`, `Error`, custom domain classes) are rejected with `UNSUPPORTED_PROTOTYPE_INSTANCE`.
   - Property descriptors are inspected via `Object.getOwnPropertyDescriptor()`:
     - Accessors (`descriptor.get !== undefined || descriptor.set !== undefined`) are rejected with `ACCESSOR_PROPERTIES_PROHIBITED` without executing getter functions.
     - Non-enumerable properties are rejected with `NON_ENUMERABLE_PROPERTY_PROHIBITED`.
   - Symbol-keyed properties are inspected, their values are recursively cloned and frozen, and they are defined on the copy as non-writable, non-configurable own data properties.
   - All property values must recursively belong to the restricted custody-value universe.
   - The resulting record is frozen with `Object.freeze()`.

#### 2. Strictly Prohibited Types (Fail-Closed Rejections):
Any value containing the following types triggers an immediate synchronous throw:
- Native `Map` or `Set` $\to$ `UNSUPPORTED_MUTABLE_COLLECTION_VALUE`
- `Date` $\to$ `UNSUPPORTED_TEMPORAL_VALUE_TYPE` (monotonic timestamp numbers required)
- `RegExp` $\to$ `UNSUPPORTED_REGEXP_VALUE_TYPE`
- `ArrayBuffer`, `SharedArrayBuffer`, `DataView`, or non-Uint8Array typed arrays (`Int8Array`, `Uint16Array`, `Float32Array`, etc.) $\to$ `UNSUPPORTED_BINARY_VALUE_TYPE`
- Functions, generator functions, async functions $\to$ `UNSUPPORTED_FUNCTION_VALUE_TYPE`
- Cyclic object graphs $\to$ `CIRCULAR_REFERENCE_DETECTED` (detected via `WeakSet<object>`)

```typescript
/**
 * Recursively validates, clones, and freezes all nested structural records and byte arrays
 * within the Restricted Custody-Value Universe.
 *
 * Enforces cycle detection, accessor prohibition, prototype verification,
 * array index purity, and symbol-value cloning.
 */
export function deepCloneAndFreezeValue<T>(val: T, seen: WeakSet<object> = new WeakSet()): T {
  // 1. Primitive handling
  if (val === null || typeof val !== 'object') {
    if (typeof val === 'number' && !Number.isFinite(val)) {
      throw new Error(`INVALID_NUMERIC_VALUE: non-finite number '${String(val)}' prohibited in custody records`);
    }
    if (typeof val === 'function') {
      throw new Error('UNSUPPORTED_FUNCTION_VALUE_TYPE: functions cannot be stored in custody records');
    }
    return val;
  }

  // 2. Cycle detection
  if (seen.has(val as object)) {
    throw new Error('CIRCULAR_REFERENCE_DETECTED: circular object graph cannot be cloned into custody storage');
  }
  seen.add(val as object);

  // 3. Byte Array Handling (Uint8Array)
  if (val instanceof Uint8Array) {
    const copy = new Uint8Array(val.length);
    copy.set(val);
    return copy as unknown as T;
  }

  // 4. Fail-closed rejection of prohibited binary and temporal types
  if (
    val instanceof ArrayBuffer ||
    (typeof SharedArrayBuffer !== 'undefined' && val instanceof SharedArrayBuffer) ||
    (ArrayBuffer.isView(val) && !(val instanceof Uint8Array))
  ) {
    throw new Error('UNSUPPORTED_BINARY_VALUE_TYPE: only Uint8Array is supported for binary custody data');
  }

  if (val instanceof Date) {
    throw new Error('UNSUPPORTED_TEMPORAL_VALUE_TYPE: Date objects prohibited; use monotonic timestamp numbers');
  }

  if (val instanceof RegExp) {
    throw new Error('UNSUPPORTED_REGEXP_VALUE_TYPE: RegExp objects prohibited in custody records');
  }

  if (val instanceof Map || val instanceof Set) {
    throw new Error('UNSUPPORTED_MUTABLE_COLLECTION_VALUE: native Map/Set cannot be nested inside immutable collections');
  }

  // 5. Plain Array Handling
  if (Array.isArray(val)) {
    if (Object.getPrototypeOf(val) !== Array.prototype) {
      throw new Error('UNSUPPORTED_PROTOTYPE_INSTANCE: array with custom prototype prohibited');
    }

    // Verify index purity: no non-index properties or symbol properties
    for (const key of Reflect.ownKeys(val)) {
      if (key === 'length') continue;
      if (typeof key === 'symbol' || !/^\d+$/.test(key) || Number(key) >= val.length) {
        throw new Error(`UNSUPPORTED_ARRAY_PROPERTY: array contains non-index property '${String(key)}'`);
      }
      const desc = Object.getOwnPropertyDescriptor(val, key)!;
      if (desc.get !== undefined || desc.set !== undefined) {
        throw new Error(`ACCESSOR_PROPERTIES_PROHIBITED: array index '${String(key)}' has accessor descriptor`);
      }
      if (!desc.enumerable) {
        throw new Error(`NON_ENUMERABLE_PROPERTY_PROHIBITED: array index '${String(key)}' must be enumerable`);
      }
    }

    const copy = new Array(val.length);
    for (let i = 0; i < val.length; i++) {
      copy[i] = deepCloneAndFreezeValue(val[i], seen);
    }
    return Object.freeze(copy) as unknown as T;
  }

  // 6. Plain Record Handling
  const proto = Object.getPrototypeOf(val);
  if (proto !== Object.prototype && proto !== null) {
    const typeName = proto?.constructor?.name ?? 'custom';
    throw new Error(`UNSUPPORTED_PROTOTYPE_INSTANCE: objects with custom prototypes of type '${typeName}' are prohibited in custody values`);
  }

  const copy: Record<string | symbol, unknown> = proto === null ? Object.create(null) : {};

  // Inspect all own string and symbol keys
  for (const key of Reflect.ownKeys(val)) {
    const desc = Object.getOwnPropertyDescriptor(val, key);
    if (!desc) continue;

    // Reject accessors without invoking getters
    if (desc.get !== undefined || desc.set !== undefined) {
      throw new Error(`ACCESSOR_PROPERTIES_PROHIBITED: property '${String(key)}' has getter/setter accessors which are forbidden in custody records`);
    }

    // Reject non-enumerable properties
    if (!desc.enumerable) {
      throw new Error(`NON_ENUMERABLE_PROPERTY_PROHIBITED: property '${String(key)}' is non-enumerable; all custody record properties must be standard enumerable data properties`);
    }

    // Recursively clone and freeze property value
    const clonedValue = deepCloneAndFreezeValue(desc.value, seen);

    Object.defineProperty(copy, key, {
      value: clonedValue,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }

  return Object.freeze(copy) as T;
}

export class ImmutableMap<K extends string | number, V> implements Iterable<[K, V]> {
  #inner: Map<K, V>;

  public constructor(entries?: Iterable<readonly [K, V]> | ReadonlyMap<K, V>) {
    const map = new Map<K, V>();
    if (entries) {
      for (const [k, v] of entries) {
        if (typeof k !== 'string' && typeof k !== 'number') {
          throw new Error('INVALID_COLLECTION_KEY_TYPE: keys must be primitive string or number');
        }
        if (typeof k === 'number' && !Number.isFinite(k)) {
          throw new Error('INVALID_NUMERIC_VALUE: map key cannot be NaN or Infinity');
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
    if (typeof key === 'number' && !Number.isFinite(key)) {
      throw new Error('INVALID_NUMERIC_VALUE: map key cannot be NaN or Infinity');
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

export class ImmutableSet<T extends string | number> implements Iterable<T> {
  #inner: Set<T>;

  public constructor(values?: Iterable<T> | ReadonlySet<T>) {
    const set = new Set<T>();
    if (values) {
      for (const v of values) {
        if (typeof v !== 'string' && typeof v !== 'number') {
          throw new Error('INVALID_COLLECTION_ELEMENT_TYPE: set elements must be primitive string or number');
        }
        if (typeof v === 'number' && !Number.isFinite(v)) {
          throw new Error('INVALID_NUMERIC_VALUE: set element cannot be NaN or Infinity');
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
    if (typeof value === 'number' && !Number.isFinite(value)) {
      throw new Error('INVALID_NUMERIC_VALUE: set element cannot be NaN or Infinity');
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
```

#### Precisely Qualified Byte Array Comparison:
Equality comparisons across byte containers evaluate byte contents bitwise without content-dependent early exits:

```typescript
/**
 * Fixed-iteration, content-independent comparison for equal-length byte arrays.
 *
 * Note on Timing Guarantees:
 * This helper avoids value-dependent early-exit branching across byte contents
 * by accumulating bitwise XOR differences across all indices. However, ordinary
 * ECMAScript/V8 execution engines (JIT optimization, garbage collection pauses,
 * CPU cache lines, memory alignment) do NOT provide cryptographic constant-time
 * hardware guarantees.
 *
 * Length-Mismatch Policy:
 * Arrays with mismatched lengths (a.length !== b.length) fail immediately (return false)
 * without inspecting contents, because cryptographic digests and signatures within
 * this protocol adhere to fixed, standardized byte lengths (e.g. 32 bytes for SHA-256).
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

All objects entering `AuthoritativeCustodyRoot` are deeply cloned and frozen on ingress. All objects returned from publication methods are immutable cached references, preventing reference sharing and memory corruption.

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

export interface IAuthoritativeCustodyStore {
  /**
   * Atomically evaluates the fencing token against the authoritative consensus state
   * and persists candidateRoot. If term is stale, lease is expired, or a higher term
   * committed, throws STALE_COORDINATOR_LEADERSHIP_FENCED.
   */
  publishFencedRoot(fencingToken: CustodyFencingToken, candidateRoot: AuthoritativeCustodyRoot): void;
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
  #publicationLock: boolean = false;
  #currentTerm: number;
  #leaseExpiresAtMonotonicMs: number;
  #guardMarginMs: number;
  #isSteppedDown: boolean = false;
  #authoritativeStore: IAuthoritativeCustodyStore;

  public constructor(
    initialTerm: number,
    initialLeaseDurationMs: number,
    authoritativeStore: IAuthoritativeCustodyStore,
    guardMarginMs: number = 1000
  ) {
    this.#currentTerm = initialTerm;
    this.#leaseExpiresAtMonotonicMs = performance.now() + initialLeaseDurationMs;
    this.#authoritativeStore = authoritativeStore;
    this.#guardMarginMs = guardMarginMs;
  }

  public stepDown(newTerm?: number): void {
    this.#isSteppedDown = true;
    if (newTerm !== undefined && newTerm > this.#currentTerm) {
      this.#currentTerm = newTerm;
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

  /**
   * Serialized Single-Writer Lease-Guarded Fencing Execution (R27-B).
   *
   * Coordinates local candidate-root validation, local lease non-expiry check with guard margin,
   * atomic fenced CAS at the Authoritative Custody Store, and strictly synchronous
   * local pointer replacement under a non-reentrant single-writer execution lock.
   */
  public executeFencedPublication<T>(
    fencingToken: CustodyFencingToken,
    candidateRoot: AuthoritativeCustodyRoot,
    commitFn: (root: AuthoritativeCustodyRoot) => T
  ): T {
    if (this.#publicationLock) {
      throw new Error('CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION: publication lock is already held');
    }
    this.#publicationLock = true;
    try {
      // Step 1: Pre-publication candidate root invariant verification (R25-5 / R27-A)
      AuthoritativeCustodyCoordinator.validateCandidateRootPrePublicationInvariants(candidateRoot);

      // Step 2: Linearized Consensus Leadership Fencing Check (Local Pre-Check with Guard Margin)
      if (this.#isSteppedDown) {
        throw new Error('STALE_COORDINATOR_LEADERSHIP_FENCED: coordinator has stepped down');
      }
      if (this.#currentTerm !== fencingToken.clusterTerm) {
        throw new Error(`STALE_COORDINATOR_LEADERSHIP_FENCED: term mismatch (expected ${fencingToken.clusterTerm}, current ${this.#currentTerm})`);
      }
      const monotonicNow = performance.now();
      if (monotonicNow + this.#guardMarginMs >= this.#leaseExpiresAtMonotonicMs) {
        throw new Error(
          `STALE_COORDINATOR_LEADERSHIP_FENCED: leadership lease expired or within guard margin (now ${monotonicNow}ms + guard ${this.#guardMarginMs}ms >= expiry ${this.#leaseExpiresAtMonotonicMs}ms)`
        );
      }

      // Step 3: Authoritative Boundary Fencing (Tier 1: Atomic CAS at Consensus / Store Boundary)
      this.#authoritativeStore.publishFencedRoot(fencingToken, candidateRoot);

      // Step 4: Run-to-Completion Local Pointer Update (Strictly Synchronous Commit Function)
      const result = commitFn(candidateRoot);

      // Prohibit asynchronous callbacks escaping the protected publication boundary
      if (result !== null && typeof result === 'object' && typeof (result as Promise<unknown>).then === 'function') {
        throw new Error('ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED: commit callback must be strictly synchronous');
      }

      return result;
    } finally {
      this.#publicationLock = false;
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

#### Authoritative Boundary Fencing (R27-B):

To guarantee that split-brain partitioned coordinators can never commit stale mutations:
1. Every custody root mutation must validate active consensus lease ownership inside `LeadershipEpochFence.executeFencedPublication()`.
2. Mutual exclusion is enforced via `#publicationLock`: concurrent attempts to publish fail immediately with `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.
3. If leadership was lost or superseded (e.g. `clusterTerm` advanced by cluster election, lease expired, or within $\Delta_{\text{guard}}$), the local check throws `STALE_COORDINATOR_LEADERSHIP_FENCED`.
4. Even if an uncoordinated process pause occurs after the local check, the Authoritative Custody Store (`IAuthoritativeCustodyStore`) performs atomic fenced compare-and-swap on `publishFencedRoot(fencingToken, candidateRoot)`, rejecting stale writes.
5. The candidate root is discarded; `#custodyRoot` is untouched; local in-flight custody is marked degraded; the settlement barrier is left unresolved; callers are redirected to the new leader.
6. The commit callback `commitFn` is strictly synchronous; returning a Promise or yielding to the event loop is prohibited and rejected with `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED`.

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

  private _nextFencingToken(): CustodyFencingToken {
    return Object.freeze({
      clusterTerm: this.#coordinatorBootEpoch.clusterTerm,
      bootGeneration: this.#coordinatorBootEpoch.bootGeneration,
      bootInstanceId: this.#coordinatorBootEpoch.bootInstanceId,
      publicationSequence: ++this.#publicationSequenceCounter,
    });
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

  public releaseRetryLease(params: ReleaseRetryLeaseParams): { leaseId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
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

    // Authoritative Boundary Fenced Publication (R27-B)
    return this.#leadershipFence.executeFencedPublication(
      token,
      candidateRoot,
      (root) => {
        this.#custodyRoot = root;
        return { leaseId, status: 'RELEASED' as const };
      }
    );
  }

  public releaseSettlementPin(pinId: string, settlementEvidence: AuthoritativeSettlementEvidence): { pinId: string; status: 'RELEASED' | 'ALREADY_RELEASED' } {
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

    // Authoritative Boundary Fenced Publication (R27-B)
    return this.#leadershipFence.executeFencedPublication(
      token,
      candidateRoot,
      (root) => {
        this.#custodyRoot = root;
        return { pinId, status: 'RELEASED' as const };
      }
    );
  }

  public publishReleaseAndObligation(
    preparedEvidence: PreparedEvidenceContext,
    expectedOwner: CustodyOwnershipToken,
    barrierIdentity: string,
    retryContext?: RetryRequestContext,
  ): PublicationResponse {
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
          const token = this._nextFencingToken();
          this.#leadershipFence.executeFencedPublication(
            token,
            nextRoot,
            (root) => {
              this.#custodyRoot = root;
            }
          );
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
          const token = this._nextFencingToken();
          this.#leadershipFence.executeFencedPublication(
            token,
            nextRoot,
            (root) => {
              this.#custodyRoot = root;
            }
          );
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

    // Authoritative Boundary Fenced Publication (R27-B)
    return this.#leadershipFence.executeFencedPublication(
      token,
      candidateRoot,
      (root) => {
        this.#custodyRoot = root;
        return { publicationResult, retryLeaseId: null };
      }
    );
  }
}
```

---

## 12.13 Acquisition Serialization & Check-Then-Acquire Race Immunity

Under single-realm JavaScript execution:
- Candidate-root preparation, invariant verification, and reference replacement occur synchronously within one microtask turn.
- Mutual exclusion is enforced via `#publicationLock`: concurrent calls within the same process fail deterministically with `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.
- The commit callback `commitFn` is strictly synchronous; returning a Promise or yielding to the event loop throws `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED`.
- Zero async turns exist between local lease validation, authoritative CAS publication, and local reference replacement `this.#custodyRoot = root`.
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

## 15. Exhaustive Failure and Retry Matrix (66 Deterministic Cases)

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
| **53** | Concurrent publication attempted during active `executeFencedPublication` | `PUBLISHING` | Coordinator | PENDING | Throw `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION` | Interleave concurrent candidate roots |
| **54** | Expired lease or stepdown during `executeFencedPublication` | `PUBLISHING` | Fenced Leader | PENDING | Throw `STALE_COORDINATOR_LEADERSHIP_FENCED`; root untouched | Commit stale root after lease expiry |
| **55** | Symbol-keyed property holds mutable record or array | `ANY` | Any | ANY | Recursively clone and freeze symbol property value | Copy symbol property by reference |
| **56** | Object instance with custom prototype passed to custody collection | `ANY` | Any | ANY | Throw `UNSUPPORTED_PROTOTYPE_INSTANCE` | Strip prototype or treat as plain record |
| **57** | Object containing getter/setter accessors passed to custody collection | `ANY` | Any | ANY | Reject with `ACCESSOR_PROPERTIES_PROHIBITED` without executing getter | Execute getter or store accessor |
| **58** | Cyclic object graph passed to custody cloning | `ANY` | Any | ANY | Detect cycle via `WeakSet` and throw `CIRCULAR_REFERENCE_DETECTED` | Infinite recursion / call stack overflow |
| **59** | Prohibited binary/temporal types (`Date`, `ArrayBuffer`, `DataView`) passed | `ANY` | Any | ANY | Throw `UNSUPPORTED_BINARY_VALUE_TYPE` or `UNSUPPORTED_TEMPORAL_VALUE_TYPE` | Store empty record or raw buffer |
| **60** | Egress `Uint8Array` copy mutated by caller | `ANY` | Any | ANY | Egress is detached mutable copy; root backing store unaffected | Mutate internal custody byte buffer |
| **61** | Lease expires after local check but before publication | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS rejects write; local custody degraded | Commit stale write to authoritative store |
| **62** | Coordinator resumes after OS process pause with expired lease | `PUBLISHING` | Coordinator | PENDING | Local check or Authoritative Store rejects with `STALE_COORDINATOR_LEADERSHIP_FENCED` | Execute publication with expired lease |
| **63** | Two independent coordinator instances attempt publication under different terms | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS commits higher term; rejects lower term | Allow split-brain dual publication |
| **64** | Asynchronous commit callback passed to `executeFencedPublication` | `PUBLISHING` | Coordinator | PENDING | Throw `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED`; lock released cleanly | Asynchronous escape from publication lock |
| **65** | Leadership term changes during candidate root preparation | `PUBLISHING` | Coordinator | PENDING | Authoritative Store CAS detects term mismatch; rejects publication | Overwrite newly elected leader's state |
| **66** | Stale coordinator attempts post-term recovery mutation | `BLOCKED` | Stale Leader | PENDING | Authoritative Store rejects token; coordinator latches `CUSTODY_RECONCILIATION_BLOCKED` | Stale mutation of shared recovery state |

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
18. **Restricted Custody-Value Universe & Deep Immutability Invariant:**
   All custody root collections (`publicationResults`, `generationAuditHistory`, `activeSettlementPins`, `activeRetryLeases`, `retiredRetryLeases`, `retiredSettlementPins`, `consumedSettlementEvidence`, `pendingSettlementObligations`) enforce runtime `#private` backing storage (`#inner`), primitive keys and elements (`string` or finite safe `number`), deep recursive freezing on ingress (`deepCloneAndFreezeValue`), cycle detection, accessor prohibition, custom prototype rejection, and detached mutable byte array cloning on egress. No consumer-visible reference can mutate authoritative custody storage.
19. **Complete Structured Owner-Token Identity Invariant:**
   Every active and retired resource binds and verifies the complete structured `CustodyOwnershipToken` (`recoveryAttemptId`, `custodyGeneration`, `drainerOwnershipGeneration`, `transactionId`, `operationIdentity`, `barrierIdentity`, `leaseTerm`, `ownerSignature`) using `isEquivalentOwnerToken()`.
20. **Authoritative Boundary Fencing & Linearized Single-Writer Invariant:**
   All custody root publications execute strictly inside `LeadershipEpochFence.executeFencedPublication()` using `CustodyFencingToken`. The Authoritative Custody Store enforces atomic fenced CAS across cluster partitions, rejecting stale terms and expired leases. The local coordinator instance enforces process-local single-writer mutual exclusion via `#publicationLock`, verifies local lease non-expiry with guard margin $\Delta_{\text{guard}}$, strictly prohibits asynchronous commit callbacks, and updates local `#custodyRoot` only upon authoritative CAS confirmation.
21. **Cross-Restart Retirement Idempotency & Active Boot Isolation Invariant:**
   Active collections initialize empty across process restarts; unreleased active resources from prior epochs fail closed as `RESOURCE_NOT_FOUND`. Retired records restored from persistent checkpoints remain authoritative and return `ALREADY_RELEASED` when presented with matching boot epoch, 7-dimensional identity, and evidence digest.

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

The following sixty-six deterministic future test specifications are formally codified for Sub-Milestone M5.1:

### Test R18-R27-T01: Retry Acquisition Without Stable Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext === undefined`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `INVALID_RETRY_REQUEST_CONTEXT`.

### Test R18-R27-T02: Retry Acquisition With Malformed or Empty Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` presenting retry token for $g=1$ with `retryContext = { consumerId: "   ", clientRequestId: "" }`.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `MALFORMED_RETRY_REQUEST_CONTEXT`.

### Test R18-R27-T03: Retry Acquisition With Oversized Request Context
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` with `consumerId` of length 257 characters.
- **Expected Final Root:** Unchanged; zero leases allocated.
- **Expected Result / Error:** Throws `RETRY_REQUEST_CONTEXT_OVERSIZED`.

### Test R18-R27-T04: Lost-Response Retransmission Recovers Original Lease Idempotently
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 acquires lease for $g=1$ with `clientRequestId = "req-101"`; receives `leaseId_A`.
  2. Consumer 1 retransmits identical request with matching `(consumerId, clientRequestId)`.
- **Expected Final Root:** `activeRetryLeases.size === 1`, containing `leaseId_A`.
- **Expected Result / Error:** Second call returns existing `leaseId_A` idempotently.

### Test R18-R27-T05: Repeated Retry Acquisition With Conflicting Operation Identity
- **Initial Authoritative Root:** Settled result at $g=1$; lease active for `(consumer1, req-101)`.
- **Operation Sequence:** Consumer 1 submits request with `clientRequestId = "req-101"` but altered `operationIdentity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `IDEMPOTENT_RETRY_IDENTITY_CONFLICT`.

### Test R18-R27-T06: Two Distinct Consumers Acquire Independent Leases Under Same Recovery Owner Token
- **Initial Authoritative Root:** Settled result at $g=1$; `activeRetryLeases.size === 0`.
- **Operation Sequence:**
  1. Consumer 1 requests lease for $g=1$ with `consumerId = "c1"`.
  2. Consumer 2 requests lease for $g=1$ with `consumerId = "c2"`.
- **Expected Final Root:** `activeRetryLeases.size === 2`, containing `leaseId_1` and `leaseId_2`.
- **Expected Result / Error:** Both acquire distinct leases successfully.

### Test R18-R27-T07: Consumer 1 Attempting to Release Consumer 2's Lease Fails Closed
- **Initial Authoritative Root:** Active leases for Consumer 1 (`leaseId_1`) and Consumer 2 (`leaseId_2`).
- **Operation Sequence:** Consumer 1 invokes `releaseRetryLease` passing `leaseId_2` with `consumerId = "c1"`.
- **Expected Final Root:** Unchanged; both leases remain active.
- **Expected Result / Error:** Throws `UNAUTHORIZED_CROSS_CONSUMER_LEASE_RELEASE`.

### Test R18-R27-T08: Duplicate Lease Release by Authorized Consumer Against Retired Record Is Idempotent
- **Initial Authoritative Root:** Active lease `leaseId_1` for Consumer 1.
- **Operation Sequence:**
  1. Consumer 1 calls `releaseRetryLease` with all matching parameters; returns `{ status: 'RELEASED' }`.
  2. Consumer 1 calls `releaseRetryLease` again with identical parameters.
- **Expected Final Root:** `activeRetryLeases.has(leaseId_1) === false`; `retiredRetryLeases.has(leaseId_1) === true`.
- **Expected Result / Error:** Second call matches retired record and returns `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R27-T09: First Publication With Option A Lifecycle Model
- **Initial Authoritative Root:** `publishedGeneration = 0`, `auditTombstoneFloor = 0`, `custodyGenerationFloor = 0`.
- **Operation Sequence:** Call `publishReleaseAndObligation` for $g=1$.
- **Expected Final Root:**
  - `publishedGeneration === 1`.
  - `publicationResults.has(1) === true`.
  - `pendingSettlementObligations.has(1) === true`.
  - `generationAuditHistory.get(1).kind === 'PENDING_PUBLICATION'`.
- **Expected Result / Error:** Success; contiguity invariant holds unconditionally.

### Test R18-R27-T10: Pending-to-Terminal Audit History Transition
- **Initial Authoritative Root:** Generation $g=1$ published; audit record is `PENDING_PUBLICATION`.
- **Operation Sequence:** Barrier for $g=1$ settles with `AuthoritativeSettlementEvidence` (`CONFIRMED_COMMITTED`).
- **Expected Final Root:** `generationAuditHistory.get(1).kind === 'TERMINAL_SETTLEMENT'`.
- **Expected Result / Error:** Success; record upgraded in place.

### Test R18-R27-T11: Eviction and Compact Tombstone Conversion
- **Initial Authoritative Root:** Result at $g=1$ settled (`TERMINAL_SETTLEMENT`); 1,000 results retained.
- **Operation Sequence:** Publish $g=1001$; triggers FIFO eviction of $g=1$.
- **Expected Final Root:**
  - `publicationResults.has(1) === false`.
  - `custodyGenerationFloor === 1`.
  - `generationAuditHistory.get(1).kind === 'COMPACT_TOMBSTONE'`.
  - `generationAuditHistory.get(1).evidenceDigest` is 32-byte SHA-256.
- **Expected Result / Error:** Success.

### Test R18-R27-T12: Tombstone Retirement Blocked by Active Settlement Pin
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active settlement pin.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R27-T13: Tombstone Retirement Blocked by Active Retry Lease
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ has an active retry lease.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged; zero tombstones deleted; zero results evicted.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R27-T14: Tombstone Retirement Blocked by Pending Settlement Obligation
- **Initial Authoritative Root:** 5,000 tombstones retained; oldest tombstone $g=1$ references pending obligation.
- **Operation Sequence:** Next result eviction attempts to retire tombstone $g=1$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_EXHAUSTED`.

### Test R18-R27-T15: Normal FIFO Tombstone Retirement at 5,000 Capacity
- **Initial Authoritative Root:** 5,000 tombstones retained ($[1 \dots 5000]$); zero pins/leases on $g=1$.
- **Operation Sequence:** Publish new generation causing eviction and tombstone retirement.
- **Expected Final Root:**
  - Tombstone $g=1$ deleted from `generationAuditHistory`.
  - `auditTombstoneFloor === 1`.
  - Total tombstones remaining: 5,000 ($[2 \dots 5001]$).
- **Expected Result / Error:** Success; contiguity preserved over $[2 \dots publishedGeneration]$.

### Test R18-R27-T16: Pre-Existing Tombstone Count Above Cap Fails Closed
- **Initial Authoritative Root:** Artificially injected root with 5,005 tombstones.
- **Operation Sequence:** Call `publishReleaseAndObligation`.
- **Expected Final Root:** Unchanged; zero bulk deletions performed.
- **Expected Result / Error:** Throws `TOMBSTONE_CAPACITY_CORRUPTED`.

### Test R18-R27-T17: Missing Tombstone Inside Promised Horizon Fails Closed
- **Initial Authoritative Root:** `auditTombstoneFloor = 10`, `custodyGenerationFloor = 20`; tombstone $g=15$ deleted.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_KEY_GAP`.

### Test R18-R27-T18: Expiry Below Tombstone Floor Returns Historical Expiry
- **Initial Authoritative Root:** `auditTombstoneFloor = 50`.
- **Operation Sequence:** Query generation $g=30$.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `CUSTODY_RECEIPT_EXPIRED` with `horizonExpired: true`.

### Test R18-R27-T19: Floor Order Invariant Violation Fails Closed
- **Initial Authoritative Root:** Artificially set `auditTombstoneFloor = 50`, `custodyGenerationFloor = 40`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_FLOOR_ORDER_VIOLATION`.

### Test R18-R27-T20: Audit Record With Mismatched Generation Fails Closed
- **Initial Authoritative Root:** Audit history key is 5, but record payload has `generation: 4`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `AUDIT_RECORD_GENERATION_MISMATCH`.

### Test R18-R27-T21: Audit Record With Mismatched Owner Token Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `membershipEpoch`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R27-T22: Audit Record With Mismatched Drainer Ownership Generation Fails Closed
- **Initial Authoritative Root:** Audit record at $g=1$ has altered `drainerOwnershipGeneration`.
- **Operation Sequence:** Call `assertAuditHistoryContiguity` with publicationResults.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `STATE_INCONSISTENT_AUDIT_RESULT_IDENTITY_MISMATCH`.

### Test R18-R27-T23: Cross-Generation Settlement Evidence Replay Rejected
- **Initial Authoritative Root:** Active pin for $g=2$; evidence constructed for $g=1$.
- **Operation Sequence:** Call `releaseSettlementPin("pin:...:g2:...", evidence_g1)`.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R27-T24: Cross-Obligation Settlement Evidence Replay Rejected via Atomic In-Root Registry
- **Initial Authoritative Root:** Evidence for $g=1$ already consumed on `pin_1`; `consumedSettlementEvidence.has("1:barrier:pin_1") === true`.
- **Operation Sequence:** Attempt to reuse identical evidence on `pin_2`.
- **Expected Final Root:** Unchanged; `pin_2` remains active.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_REPLAY_DETECTED`.

### Test R18-R27-T25: Caller-Constructed Mock Evidence Rejected by Provenance Check
- **Initial Authoritative Root:** Active pin for $g=1$.
- **Operation Sequence:** Caller constructs object matching `AuthoritativeSettlementEvidence` schema without factory.
- **Expected Final Root:** Unchanged; pin remains active.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R27-T26: Watchdog Timeout Cannot Release Settlement Pin
- **Initial Authoritative Root:** Barrier pending for $g=1$; watchdog fires after 5,000 ms.
- **Operation Sequence:** Watchdog handler attempts to call `releaseSettlementPin` with watchdog receipt.
- **Expected Final Root:** Pin remains active; gate state latches `DEGRADED`.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R27-T27: Retry Lease ID Passed to Settlement Pin Release Fails Closed
- **Initial Authoritative Root:** Active lease `lease:1:1:c1:1`.
- **Operation Sequence:** Call `releaseSettlementPin("lease:1:1:c1:1", evidence)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `PIN_TYPE_MISMATCH`.

### Test R18-R27-T28: Settlement Pin ID Passed to Retry Lease Release Fails Closed
- **Initial Authoritative Root:** Active pin `pin:1:1:b1:1`.
- **Operation Sequence:** Call `releaseRetryLease({ leaseId: "pin:1:1:b1:1", ... })`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TYPE_MISMATCH`.

### Test R18-R27-T29: Genuinely Unknown Identifier Passed to Release APIs Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Clean root; active and retired maps empty.
- **Operation Sequence:**
  1. Call `releaseRetryLease` with never-issued `leaseId = "lease:999:unknown"`.
  2. Call `releaseSettlementPin` with never-issued `pinId = "pin:999:unknown"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Both calls fail closed with `RESOURCE_NOT_FOUND` (neither returns `ALREADY_RELEASED`).

### Test R18-R27-T30: Duplicate Release Against Retired Pin With Matching Provenance and Digest Returns Already Released
- **Initial Authoritative Root:** Active pin `pin_1` for $g=1$.
- **Operation Sequence:**
  1. Release `pin_1` with valid evidence; returns `{ status: 'RELEASED' }`.
  2. Repeat `releaseSettlementPin("pin_1", validEvidence)`.
- **Expected Final Root:** `activeSettlementPins.has("pin_1") === false`; `retiredSettlementPins.has("pin_1") === true`.
- **Expected Result / Error:** Second call validates provenance and digest, returning `{ status: 'ALREADY_RELEASED' }` idempotently.

### Test R18-R27-T31: Settlement Pin Release With Mismatched Drainer Generation Fails Closed (Blocker C1)
- **Initial Authoritative Root:** Active pin for $g=1$ with `drainerOwnershipGeneration = 2`.
- **Operation Sequence:** Caller presents `AuthoritativeSettlementEvidence` with `drainerOwnershipGeneration = 1`.
- **Expected Final Root:** Unchanged; pin remains active; evidence remains unconsumed.
- **Expected Result / Error:** Throws `SETTLEMENT_EVIDENCE_IDENTITY_MISMATCH`.

### Test R18-R27-T32: Pre-Publication Throw During Pin Release Leaves Evidence Usable for Retry (Blocker C2)
- **Initial Authoritative Root:** Active pin for $g=1$; `consumedSettlementEvidence.size === 0`.
- **Operation Sequence:**
  1. Initiate `releaseSettlementPin`; candidate root construction simulates memory allocation exception before pointer swap.
  2. Retry `releaseSettlementPin` with the exact same evidence.
- **Expected Final Root:** Second call succeeds; `pin_1` retired; evidence replay key added to `consumedSettlementEvidence`.
- **Expected Result / Error:** First call throws error without consuming evidence in authoritative root; second call completes release cleanly.

### Test R18-R27-T33: Duplicate Release on Retired Lease With Altered Transaction ID Fails Closed (Blocker B1)
- **Initial Authoritative Root:** Retired lease `lease_1` recorded for `transactionId = "tx-100"`.
- **Operation Sequence:** Call `releaseRetryLease` passing `lease_1` with `transactionId = "tx-200"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `LEASE_TRANSACTION_ID_MISMATCH`.

### Test R18-R27-T34: Duplicate Release on Retired Pin With Unverified Provenance Fails Closed (Blocker C2)
- **Initial Authoritative Root:** Retired pin `pin_1` recorded for $g=1$.
- **Operation Sequence:** Caller constructs mock evidence object with matching fields but without factory issuance.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `INVALID_SETTLEMENT_EVIDENCE_PROVENANCE`.

### Test R18-R27-T35: Pre-Existing Retired Pins Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredSettlementPins`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_PINS_CAPACITY_CORRUPTED`.

### Test R18-R27-T36: Pre-Existing Retired Leases Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `retiredRetryLeases`.
- **Operation Sequence:** Call `releaseRetryLease` for active lease `lease_A`.
- **Expected Final Root:** Unchanged; zero entries deleted; zero mutations committed.
- **Expected Result / Error:** Throws `RETIRED_LEASES_CAPACITY_CORRUPTED`.

### Test R18-R27-T37: Pre-Existing Consumed Evidence Overflow Rejection
- **Initial Authoritative Root:** Artificially injected root with 5,005 entries in `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_A`.
- **Expected Final Root:** Unchanged; zero entries deleted.
- **Expected Result / Error:** Throws `CONSUMED_EVIDENCE_CAPACITY_CORRUPTED`.

### Test R18-R27-T38: Cross-Restart Delayed Release Rejection for Unretired Active Pin (R25-3)
- **Initial Authoritative Root:** Restarted coordinator with `coordinatorBootEpoch = { clusterTerm: 2, bootGeneration: 5, bootInstanceId: "uuid-new" }`. Active collections empty (`size === 0`).
- **Operation Sequence:** Caller submits delayed release for unretired pin `pinId = "pin:1.4.uuid-old:1:barrier:100"`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `RESOURCE_NOT_FOUND` (unretired active pins from earlier boot generations cannot be released after restart).

### Test R18-R27-T39: Allocation Interruption Discards Counter Without Collision
- **Initial Authoritative Root:** `_pinSequenceCounter = 10`.
- **Operation Sequence:**
  1. Call `publishReleaseAndObligation`; sequence increments to 11; candidate allocation throws out-of-memory.
  2. Repeat `publishReleaseAndObligation`; sequence increments to 12; publication succeeds.
- **Expected Final Root:** Active pin published with `seq = 12`; zero active records for `seq = 11`.
- **Expected Result / Error:** Second call succeeds cleanly; discarded sequence 11 causes zero collisions.

### Test R18-R27-T40: Normal Exactly-At-Capacity Lockstep Retirement at 5,000 Entries
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`, `consumedSettlementEvidence.size === 5000`. Oldest retired pin has zero active references.
- **Operation Sequence:** Release active pin `pin_new` with valid evidence.
- **Expected Final Root:**
  - Oldest retired pin deleted.
  - Oldest replay key deleted from `consumedSettlementEvidence`.
  - `retiredSettlementPins.size === 5000`.
  - `consumedSettlementEvidence.size === 5000`.
- **Expected Result / Error:** Release succeeds cleanly with exact capacity preservation and pre-publication candidate-root validation.

### Test R18-R27-T41: All-Protected Retired Pins Exhaustion Triggers Fail-Closed Backpressure
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. Every single entry in `retiredSettlementPins` is retained by an active retry lease or active pin.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero pins evicted; zero state mutations committed.
- **Expected Result / Error:** Throws `RETENTION_CAPACITY_EXHAUSTED_BACKPRESSURE`.

### Test R18-R27-T42: Replay Key Desynchronization on Eviction Fails Closed
- **Initial Authoritative Root:** `retiredSettlementPins.size === 5000`. The replay key corresponding to the oldest eligible retired pin is artificially absent from `consumedSettlementEvidence`.
- **Operation Sequence:** Call `releaseSettlementPin` for active pin `pin_new`.
- **Expected Final Root:** Unchanged; zero mutations committed.
- **Expected Result / Error:** Throws `REPLAY_KEY_DESYNCHRONIZATION_CORRUPTED`.

### Test R18-R27-T43: Reference-Independent Byte-for-Byte Digest Match Returns Already Released (R25-4)
- **Initial Authoritative Root:** Pin `pin_retired` is in `retiredSettlementPins` with recorded digest `[0x01, 0x02, ..., 0x20]`.
- **Operation Sequence:** Caller invokes `releaseSettlementPin` for `pin_retired` with independently allocated `Uint8Array` containing identical bytes (`digestA !== digestB` by reference, but byte-equal).
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `{ pinId: 'pin_retired', status: 'ALREADY_RELEASED' }`.

### Test R18-R27-T44: Candidate-Root Pre-Publication Invariant Rejection on Desynchronized Replay State (R25-5)
- **Initial Authoritative Root:** Candidate root construction artificially desynchronizes collection sizes (`retiredSettlementPins.size = 5000`, `consumedSettlementEvidence.size = 5001`).
- **Operation Sequence:** Candidate root passed to `_validateCandidateRootPrePublicationInvariants()`.
- **Expected Final Root:** Pointer swap rejected; `this.#custodyRoot` remains unchanged.
- **Expected Result / Error:** Throws `REPLAY_KEY_BIJECTION_CARDINALITY_MISMATCH`.

### Test R18-R27-T45: ECMAScript #inner Backing Storage Inaccessible at Runtime (R25-1)
- **Initial Authoritative Root:** Initialized `AuthoritativeCustodyRoot` with `ImmutableMap` instance.
- **Operation Sequence:** Caller attempts `(root.activeSettlementPins as any).#inner`, `(root.activeSettlementPins as any)._inner`, or `Object.getOwnPropertySymbols(root.activeSettlementPins)`.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Throws `SyntaxError` or returns `undefined`; backing Map is strictly inaccessible.

### Test R18-R27-T46: Consensus Leadership Loss Prior to Pointer Swap Discards Candidate Root (R25-2)
- **Initial Authoritative Root:** `clusterTerm = 2`. Coordinator prepares candidate root for $g = 2$.
- **Operation Sequence:** Cluster consensus advances to `term = 3` (new leader elected) before Step 2. `this.#consensusLease.assertActiveLeadership(2)` executes.
- **Expected Final Root:** Unchanged; candidate root discarded; local custody marked degraded; settlement barrier left unresolved.
- **Expected Result / Error:** Throws `STALE_COORDINATOR_LEADERSHIP_FENCED`.

### Test R18-R27-T47: Cross-Restart Restored Retired Pin Returns Already Released with Verified Epoch (R25-3)
- **Initial Authoritative Root:** Coordinator restarted under `clusterTerm = 2`, `bootGeneration = 5`. `retiredSettlementPins` restored from persistent checkpoint, containing `pin:1.4.uuid-old:1:barrier:200` with `allocatedBootEpoch = { clusterTerm: 1, bootGeneration: 4, bootInstanceId: "uuid-old" }`.
- **Operation Sequence:** Caller submits delayed release for `pinId = "pin:1.4.uuid-old:1:barrier:200"` with matching 7-dimensional token and matching evidence digest.
- **Expected Final Root:** Unchanged.
- **Expected Result / Error:** Returns `{ pinId: 'pin:1.4.uuid-old:1:barrier:200', status: 'ALREADY_RELEASED' }`.

### Test R18-R27-T48: Pre-Publication Replay-Key Bijection Violation Fails Closed (R25-5)
- **Initial Authoritative Root:** Candidate root constructed with a retired pin whose replay key is absent from `candidateConsumed`.
- **Operation Sequence:** Candidate root evaluated in `_validateCandidateRootPrePublicationInvariants()`.
- **Expected Final Root:** Pointer swap aborted; candidate root discarded.
- **Expected Result / Error:** Throws `REPLAY_KEY_BIJECTION_VIOLATION`.

---

### Test R18-R27-T49: Deep Property Mutation Rejection on Retrieved ImmutableMap Value (R26-1)
- **Initial Authoritative Root:** Initialized root with published result at $g=1$.
- **Operation Sequence:** Caller retrieves record via `root.publicationResults.get(1)` and attempts in-place property modification: `(record as any).obligation.transactionId = "corrupted"`.
- **Expected Final Root:** Unchanged; stored record remains intact with original `transactionId`.
- **Expected Result / Error:** Throws `TypeError` (frozen object property modification) or fails silently without mutating internal state; subsequent `root.publicationResults.get(1)` returns pristine original `transactionId`.

### Test R18-R27-T50: Defensive Byte Array Cloning Prevents Buffer Tampering on Egress (R26-1)
- **Initial Authoritative Root:** Initialized root with active settlement pin containing `ownerToken.ownerSignature = new Uint8Array([0x01, 0x02, 0x03, 0x04])`.
- **Operation Sequence:**
  1. Caller retrieves pin via `const pin = root.activeSettlementPins.get(pinId)!`.
  2. Caller mutates byte buffer directly: `pin.ownerToken.ownerSignature[0] = 0xFF`.
- **Expected Final Root:** Internal backing storage unaffected. Subsequent call to `root.activeSettlementPins.get(pinId)` yields buffer with original `ownerSignature[0] === 0x01`.
- **Expected Result / Error:** Defensive byte cloning on egress ensures caller mutates an isolated detached copy; internal state remains pristine.

### Test R18-R27-T51: Iterator Aliasing and Mutation Immunity on Entries and Values (R26-1)
- **Initial Authoritative Root:** Published results at $g=1$ and $g=2$ in `publicationResults`.
- **Operation Sequence:**
  1. Caller iterates using `for (const [k, v] of root.publicationResults.entries())` or `for (const v of root.publicationResults.values())`.
  2. Caller attempts in-place property mutation or byte array modification on yielded values.
- **Expected Final Root:** Unchanged; internal root and subsequent iterations produce identical unmodified records.
- **Expected Result / Error:** Deep defensive cloning on each yielded element guarantees complete reference isolation across all iterator access paths.

### Test R18-R27-T52: Key and Element Type Invariant Rejection on ImmutableMap and ImmutableSet (R26-1)
- **Initial Authoritative Root:** Initialized `ImmutableMap` and `ImmutableSet`.
- **Operation Sequence:**
  1. Caller attempts to instantiate `ImmutableMap` with an object key: `new ImmutableMap([[({} as any), "val"]])` or call `.set({} as any, "val")`.
  2. Caller attempts to instantiate `ImmutableSet` with an object element: `new ImmutableSet([({} as any)])` or call `.add({} as any)`.
- **Expected Final Root:** Unchanged; zero elements inserted.
- **Expected Result / Error:** Throws `INVALID_COLLECTION_KEY_TYPE: keys must be primitive string or number` and `INVALID_COLLECTION_ELEMENT_TYPE: set elements must be primitive string or number`.

### Test R18-R27-T53: Non-Reentrant Single-Writer Execution Lock Prevents Concurrent Publications (R26-2)
- **Initial Authoritative Root:** Coordinator initialized with `LeadershipEpochFence`.
- **Operation Sequence:**
  1. Primary publication begins executing inside `executeFencedPublication()`.
  2. While the lock is held, a concurrent callback or reentrant call attempts `executeFencedPublication()`.
- **Expected Final Root:** Primary publication completes or rolls back; concurrent attempt is rejected upfront before touching candidate roots.
- **Expected Result / Error:** Throws `CONCURRENT_PUBLICATION_LOCK_ACQUISITION_VIOLATION`.

### Test R18-R27-T54: Synchronous Leadership Stepdown and Lease Expiry Fencing (R26-2)
- **Initial Authoritative Root:** Coordinator with active lease for Term 1 expiring at monotonic $t_{\text{expire}}$.
- **Operation Sequence:**
  1. Case A: Coordinator receives stepdown notification (`leadershipFence.stepDown()`), then attempts `executeFencedPublication(expectedTerm: 1)`.
  2. Case B: Monotonic clock advances past $t_{\text{expire}}$, then coordinator attempts `executeFencedPublication(expectedTerm: 1)`.
- **Expected Final Root:** Pointer swap aborted in both cases; `#custodyRoot` untouched; candidate root discarded; barrier remains unresolved.
- **Expected Result / Error:** Both cases throw `STALE_COORDINATOR_LEADERSHIP_FENCED`.

---

### Test R18-R27-T55: Symbol-Keyed Mutable Value Recursively Cloned and Frozen (R27-A)
- **Initial State:** Authoritative custody root with published result at $g=1$. A candidate record includes a proprietary symbol property `[Symbol('syncplay/ext')] = { payload: new Uint8Array([0xAA, 0xBB]) }`.
- **Exact Interleaving or Injected Fault:** Candidate record passed to `deepCloneAndFreezeValue()`. Caller subsequently mutates original symbol property: `original[sym].payload[0] = 0x00`.
- **Expected Result:** Cloned symbol value is deeply cloned and frozen as an own data property (`Object.isFrozen(cloned[sym]) === true`). Internal custody record payload remains `0xAA`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Custody remains securely owned; zero reference aliasing.

### Test R18-R27-T56: Unsupported Object Prototypes Rejected (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller attempts to insert an object instance of a custom class (`class CustomTelemetry {}`, `new CustomTelemetry()`) into publication results.
- **Expected Result:** Throws `UNSUPPORTED_PROTOTYPE_INSTANCE: objects with custom prototypes of type 'CustomTelemetry' are prohibited in custody values`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery until valid record presented.

### Test R18-R27-T57: Accessor Properties Rejected Without Invoking Getters (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller passes an object with a getter accessor: `{ get hostile() { throw new Error('GETTER_INVOKED_SIDE_EFFECT'); } }`.
- **Expected Result:** Property descriptor inspection detects `desc.get !== undefined` without evaluating `obj.hostile`. Throws `ACCESSOR_PROPERTIES_PROHIBITED: property 'hostile' has getter/setter accessors which are forbidden in custody records`. The getter is never invoked.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R27-T58: Cyclic Structures Fail Closed (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller passes a self-referential cyclic object graph (`const cycle: any = {}; cycle.self = cycle;`).
- **Expected Result:** `WeakSet` cycle detection catches `seen.has(cycle)` and throws `CIRCULAR_REFERENCE_DETECTED: circular object graph cannot be cloned into custody storage`. Zero call stack overflow.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R27-T59: Unsupported Typed Arrays and Binary/Temporal Types Rejected (R27-A)
- **Initial State:** Coordinator ready to publish candidate root for $g=2$.
- **Exact Interleaving or Injected Fault:** Caller attempts to store `new Float32Array([1.0, 2.0])`, `new ArrayBuffer(16)`, `new DataView(new ArrayBuffer(8))`, or `new Date()`.
- **Expected Result:** Throws `UNSUPPORTED_BINARY_VALUE_TYPE: only Uint8Array is supported for binary custody data` or `UNSUPPORTED_TEMPORAL_VALUE_TYPE: Date objects prohibited; use monotonic timestamp numbers`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Root unchanged; candidate discarded; custody remains in degraded recovery.

### Test R18-R27-T60: Byte-Array Egress Mutation Cannot Alter Custody Storage (R27-A)
- **Initial State:** Active settlement pin recorded in `activeSettlementPins` with `ownerToken.ownerSignature = new Uint8Array([0x10, 0x20, 0x30])`.
- **Exact Interleaving or Injected Fault:** Caller invokes `root.activeSettlementPins.get(pinId)!`, receiving detached byte array. Caller mutates `egress.ownerToken.ownerSignature[0] = 0xFF`.
- **Expected Result:** The returned `Uint8Array` is a detached mutable copy. Backing `#inner` storage is untouched. A subsequent call to `root.activeSettlementPins.get(pinId)!` yields fresh buffer with `ownerSignature[0] === 0x10`.
- **Which Invariant Is Protected:** Invariant 18 (Restricted Custody-Value Universe & Deep Immutability Invariant).
- **Custody Disposition:** Custody remains securely owned; internal storage immutable.

### Test R18-R27-T61: Lease Expires After Local Check but Before Authoritative Publication (R27-B)
- **Initial State:** Coordinator holds lease for Term 1 expiring at $t_{\text{expire}} = t_0 + 5000\text{ms}$. Candidate root prepared at $t_0 + 4900\text{ms}$.
- **Exact Interleaving or Injected Fault:** An artificial thread pause delays submission to the Authoritative Custody Store until $t_0 + 5100\text{ms}$ (lease expired). Authoritative Custody Store evaluates token against cluster consensus.
- **Expected Result:** Authoritative Custody Store CAS rejects write with `STALE_COORDINATOR_LEADERSHIP_FENCED`. Local coordinator discards candidate root; `#custodyRoot` untouched.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Local custody enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`); barrier left unresolved.

### Test R18-R27-T62: Coordinator Resumes After Process Suspension and Attempts Publication with Expired Lease (R27-B)
- **Initial State:** Coordinator holding Term 1 lease prepares candidate root.
- **Exact Interleaving or Injected Fault:** OS process suspension (SIGSTOP / major GC pause) suspends coordinator for 10 seconds. On resumption at $t_0 + 10000\text{ms}$, coordinator invokes `executeFencedPublication()`.
- **Expected Result:** Step 2 local fencing check (with guard margin $\Delta_{\text{guard}}$) detects `performance.now() + 1000ms >= leaseExpiresAtMonotonicMs` and throws `STALE_COORDINATOR_LEADERSHIP_FENCED`. Zero write submitted to authoritative store.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Candidate discarded; `#custodyRoot` untouched; custody enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`).

### Test R18-R27-T63: Two Independent Coordinator Instances Attempt Publication Under Different Terms (R27-B)
- **Initial State:** Partitioned Coordinator A operates under Term 1; newly elected Coordinator B operates under Term 2. Authoritative Custody Store has admitted Term 2.
- **Exact Interleaving or Injected Fault:** Both Coordinator A and Coordinator B submit candidate publications to Authoritative Custody Store.
- **Expected Result:** Authoritative Custody Store atomically accepts Coordinator B (Term 2) and rejects Coordinator A (Term 1) with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
- **Which Invariant Is Protected:** Invariant 2 (Single Primary Drainer Invariant) and Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Coordinator B holds valid cluster custody; Coordinator A enters degraded recovery (`CUSTODY_RECONCILIATION_BLOCKED`).

### Test R18-R27-T64: Asynchronous Commit Callback Prohibited and Rejected (R27-B)
- **Initial State:** Coordinator initialized with `LeadershipEpochFence`.
- **Exact Interleaving or Injected Fault:** Caller invokes `executeFencedPublication()` passing an `async` function or a callback that returns a Promise (`async (root) => { await delay(10); }`).
- **Expected Result:** Step 4 detects thenable return and throws `ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED: commit callback must be strictly synchronous`. In `finally`, `#publicationLock` is reset to `false`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Root unchanged; lock released cleanly; zero async escape.

### Test R18-R27-T65: Leadership Changes During Publication Protocol (R27-B)
- **Initial State:** Coordinator A begins candidate root staging for Term 1.
- **Exact Interleaving or Injected Fault:** Cluster consensus quorum elects Coordinator B for Term 2 and updates `store.highestCommittedTerm = 2` before Coordinator A's CAS arrives.
- **Expected Result:** Authoritative Custody Store CAS rejects Coordinator A's token with `STALE_COORDINATOR_LEADERSHIP_FENCED`.
- **Which Invariant Is Protected:** Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Coordinator A's candidate root discarded; barrier unresolved; local custody latched into `CUSTODY_RECONCILIATION_BLOCKED`.

### Test R18-R27-T66: Stale Coordinator Cannot Mutate Shared Recovery State After Term Loss (R27-B)
- **Initial State:** Coordinator A was partitioned and deposed in Term 1; cluster is active in Term 2.
- **Exact Interleaving or Injected Fault:** Coordinator A attempts post-term recovery mutation (`publishReleaseAndObligation` or `releaseSettlementPin`).
- **Expected Result:** Authoritative store rejects write with `STALE_COORDINATOR_LEADERSHIP_FENCED`. Coordinator A marks local in-flight custody degraded, leaves barrier pending, and latches `CUSTODY_RECONCILIATION_BLOCKED`.
- **Which Invariant Is Protected:** Invariant 4 (Non-Commitment Proof Invariant) and Invariant 20 (Authoritative Boundary Fencing & Linearized Single-Writer Invariant).
- **Custody Disposition:** Shared recovery state completely protected; stale coordinator fenced permanently.

---

## 20. Updated Formal Approval Questions for Engineering Authority

```
================================================================================
ENGINEERING AUTHORITY APPROVAL QUESTIONS (MILESTONE M5.0-R18-R27):
================================================================================
1. [RESTRICTED CUSTODY-VALUE UNIVERSE (R27-A)]: Does the Engineering Authority
   approve restricting custody values strictly to supported primitives, plain
   records (Object.prototype/null), index-only arrays, and Uint8Array buffers,
   while fail-closed rejecting custom prototypes, accessors (without executing
   getters), non-enumerable properties, circular graphs, and unsupported
   binary/temporal types?

2. [DETACHED MUTABLE BYTE-ARRAY EGRESS COPIES (R27-A)]: Does the Engineering
   Authority approve the explicit distinction that returned Uint8Array instances
   are detached, mutable copies that callers may manipulate freely without
   altering or affecting the internal runtime-encapsulated custody root?

3. [AUTHORITATIVE BOUNDARY FENCING (R27-B)]: Does the Engineering Authority
   approve enforcing atomic fenced Compare-and-Swap (CAS) at the Authoritative
   Custody Store (IAuthoritativeCustodyStore) using compound fencing tokens,
   ensuring that partitioned or paused coordinators are rejected across cluster
   partitions even if an uncoordinated OS/GC pause elapses after local checks?

4. [SYNCHRONOUS COMMIT CONSTRAINT & GUARD MARGIN (R27-B)]: Does the Engineering
   Authority approve enforcing that commitFn must be strictly synchronous
   (rejecting Promises with ASYNCHRONOUS_COMMIT_CALLBACK_PROHIBITED) and that
   local lease validation requires a safety margin Delta_guard (1,000ms)?

5. [TARGETED ADVERSARIAL VERIFICATION (R27-C)]: Does the Engineering Authority
   approve the expanded 66-case failure matrix and 66-case deterministic future
   test specifications covering symbol cloning, custom prototype rejection,
   accessor non-execution, cycle detection, egress mutation isolation,
   post-check lease expiry, OS pauses, and dual coordinator races?

6. [CLOSURE OF ALL CORRECTNESS GAPS]: Does the Engineering Authority agree that
   all review issues from R26 are fully resolved, and that the recovery arbitration
   architecture is ready for holistic M5.0 final acceptance review?
================================================================================
```

---

## 21. Final Verdict & Mandatory Authorization Statement

```
================================================================================
M5.0-R18-R27 ARCHITECTURAL DECISION PROPOSAL VERDICT:
PASS — READY FOR HOLISTIC M5.0 FINAL ACCEPTANCE REVIEW
================================================================================
```

This R18-R27 proposal achieves complete architectural closure of the recovery arbitration subsystem. It establishes a strictly restricted and enforceable custody-value universe with comprehensive cycle detection and accessor prohibition, enforces dual-tier authoritative boundary fencing with atomic fenced CAS at the consensus store, qualifies distributed split-brain exclusion under explicit clock drift and lease non-overlap invariants, and codifies sixty-six deterministic future test specifications.

*Note on Acceptance Discipline:* Completion of Milestone M5.0-R18-R27 constitutes architectural readiness. Formal M5.0 milestone acceptance requires holistic review and explicit Engineering Authority sign-off prior to authorizing Milestone M5.1.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R27 is a documentation-only architectural correction. No production implementation has been authorized. M5.1, including M5.1a, remains strictly unauthorized pending holistic M5.0 acceptance and explicit Engineering Authority approval."
