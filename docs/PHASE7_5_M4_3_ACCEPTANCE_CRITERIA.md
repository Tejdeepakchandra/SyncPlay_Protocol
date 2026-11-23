# Phase 7.5 M4.3 — Recovery Orchestration & Lifecycle Integration Acceptance Criteria

**Author:** Senior TypeScript Distributed-Systems Architect  
**Status:** M4.3 ARCHITECTURE READY FOR FINAL ACCEPTANCE — IMPLEMENTATION UNAUTHORIZED  
**Mode:** STRICTLY READ-ONLY SPECIFICATION (IMPLEMENTATION UNAUTHORIZED)  
**Milestone:** Phase 7.5 Sub-Milestone M4.3  
**Date:** November 24, 2026  
**Revision:** R4 Final Ownership Closure  
**Authoritative Baseline Commit:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Direct Parent:** `79919a0f8cb0bb81cb80cc010fe02918bef79787` (M4.2c-R6 closure)  
**Parent of Direct Parent:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5 closure)  

---

## 1. Overview & Verification Framework

This document establishes the granular, testable pass/fail acceptance criteria for Sub-Milestone M4.3: **Recovery Orchestration & Lifecycle Integration**, updated for R3 contract closure.

Each acceptance criterion specifies:
- Formal invariant.
- Test verification procedure.
- Pass/fail condition.
- Target sub-milestone (M4.3a–M4.3d).

---

## 2. Acceptance Criteria Catalog

### 2.1 AC-REC-STATE: Deterministic 4-State Recovery Machine
- **Target:** M4.3a
- **Invariant:** The recovery lifecycle must transition strictly through the 4 defined states (`ACTIVE`, `RECOVERING`, `RECONCILING`, `DEGRADED`) and 8 substates. Direct transitions between disallowed states (e.g. `ACTIVE` $\to$ `RECONCILING`) must be rejected.
- **Verification:**
  1. Unit test drives state machine through all valid transition sequences.
  2. Drive state machine through illegal transition attempts.
- **Pass Condition:** All valid transitions succeed and emit state change notifications. All invalid transitions throw descriptive errors and leave current state unchanged.

---

### 2.2 AC-REC-ELIG: P7.4-Aligned Candidate Eligibility & No Tie-Breaking
- **Target:** M4.3c
- **Invariant:** Candidate snapshot offers must be evaluated strictly against P7.4 `ReplicationComparator` digest classifications:
  - `IDENTICAL`: Rejected (no recovery needed).
  - `FRONTIER_AGREEMENT_ONLY`: Rejected (cannot overwrite state without verified history).
  - `STRICTLY_DOMINATES`: Admitted as candidate.
  - `INCOMPARABLE_CONCURRENT`: Strictly rejected without arbitrary Lamport or peer-ID tie-breaking.
  - `DOMINATED_BY_LOCAL`: Rejected.
  - `OLDER_EPOCH`: Rejected.
  - `FUTURE_EPOCH`: Buffered in `FutureEpochBuffer` for 5s grace period.
- **Verification:**
  - Submit mock offers matching each of the 8 classifications; verify expected admission or rejection reason code.
- **Pass Condition:** Zero arbitrary tie-breaking; only strictly dominating offers from matching epochs are selected.

---

### 2.3 AC-REC-ROOT: Sealed Single-Root Atomic State Swap & Reference Ownership (R4-4)
- **Target:** M4.3b
- **Invariant:** Stage 3 atomic installation (`commitAtomic()` / `LeaderlessSyncEngine.replaceCoreStateAtomic()`) must execute via a single synchronous reference assignment under JavaScript run-to-completion semantics:
  $$\text{this.core} = \text{candidateCore}$$
  The candidate core container is sealed (`Object.seal(core)`), but `Object.seal()` is strictly shallow. Nested playback states are deeply frozen via `Object.freeze()`, while mutable internal structures are strictly confined to private engine transition methods. External consumers cannot obtain mutable references. The creator (`SnapshotInstaller`) drops all local references upon completion (`candidateCore = null`). The commit operation CANNOT throw. The old root becomes eligible for garbage collection without false claims of deterministic GC.
- **Verification:**
  1. Inspect `replaceCoreStateAtomic` in `LeaderlessSyncEngine`.
  2. Verify that `this.core` is replaced in a single assignment statement.
  3. Verify that mutating an old dereferenced root object has zero effect on `this.core`.
  4. Verify that creator references are nullified post-handoff.
  5. Verify that all getters (`playbackState`, `logicalClock`, `replicationVector`, `operationLog`) immediately return the new candidate values without intermediate states.
- **Pass Condition:** Single reference assignment proven; shallow seal and recursive freeze verified; creator references released; old root isolated.

---

### 2.4 AC-REC-QUAR: Dual-Constraint Bounded FIFO Quarantine with Generational Handoff
- **Target:** M4.3a
- **Invariant:** Remote control operations arriving while the engine is in `RECOVERING` or `RECONCILING` must be enqueued into `QuarantineBuffer` in FIFO order. The buffer enforces two independent hard bounds:
  - Count limit: $N \le 1,000$ operations.
  - Byte limit: $B \le 4 \text{ MiB}$ serialized UTF-8 bytes.
  - Per-event limit: $S \le 4 \text{ KiB}$.
  Exceeding either bound must immediately trigger a fail-closed error with code `QUARANTINE_OVERFLOW` and cause transition to `DEGRADED`.
- **Verification:**
  1. Enqueue 1,000 operations under 4 MiB; verify all accepted. Attempt operation 1,001; verify `QUARANTINE_OVERFLOW`.
  2. Enqueue 500 operations totaling 4.1 MiB; verify `QUARANTINE_OVERFLOW` on byte limit breach.
  3. Verify that `swapDrainBatch()` atomically hands off the current active batch to drain while new arriving operations enter a fresh active queue without dropping or reordering.
- **Pass Condition:** Dual count and byte bounds enforced; generational swap operates without loss or reordering.

---

### 2.5 AC-REC-DRAIN: 8-State Drain Lifecycle & Exactly-Once Resumption
- **Target:** M4.3c
- **Invariant:** Every operation in recovery must reside in exactly one unambiguous state (`NOT_YET_DRAINED`, `DRAINER_IN_FLIGHT`, `SUCCESSFULLY_REDUCED`, `SUCCESSFULLY_INSERTED_PENDING`, `ALREADY_REPRESENTED_BY_FRONTIER`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, `RECOVERABLE_DESTINATION_REJECTION`). When a 100-operation drain batch encounters a missing causal dependency at operation 41:
  1. Operations 1..40 stay committed in `OrderedOperationLog` and advance `replicationVector`.
  2. Operation 41 is buffered in `PendingDependencyQueue`; `replicationVector` **does not advance across the gap**.
  3. Operations 42..100 depending on 41 are buffered in `PendingDependencyQueue`.
  4. No operation is ambiguously owned by two queues simultaneously.
  5. `AntiEntropyManager.RepairRequestPipeline` requests the missing sequence range.
  6. When the missing operation arrives via repair, `drainPendingOperations()` automatically unblocks ops 41..100 in causal order without re-applying ops 1..40 or losing ops 42..100.
- **Verification:**
  - Execute partial drain simulation test with injected causal gap at op 41. Verify step-by-step vector states, exact queue ownership, and unblocking sequence.
- **Pass Condition:** Exactly-once application; zero operations lost; zero duplicate reducer executions; zero vector advancement across gaps; zero dual queue ownership.

---

### 2.6 AC-REC-INFLIGHT: Explicit In-Flight Ownership & Receipt Acknowledgment (R4-1 & R4-2)
- **Target:** M4.3c
- **Invariant:** When an operation is dequeued from `QuarantineBuffer.activeQueue`, it must transition to `DRAINER_IN_FLIGHT` owned exclusively by `QuarantineDrainer.inFlightRecord`. Cardinality is strictly bounded: $N_{\text{in-flight}} \le 1$.
  The operation is submitted to the engine, which must return a typed `DestinationAcceptanceReceipt`. Drainer releases in-flight ownership (`inFlightRecord = null`) ONLY upon receiving a confirmed status (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`). If destination rejects due to capacity (`CAPACITY_REJECTED`) or an exception is thrown, the in-flight operation **CANNOT DISAPPEAR**: it remains in `inFlightRecord` (or is unshifted back to `activeQueue`), and recovery enters `DEGRADED`.
- **Verification:**
  1. Unit test verifying $N_{\text{in-flight}} \le 1$ during drain.
  2. Simulate capacity rejection from `PendingDependencyQueue` (256/256); assert operation remains in `inFlightRecord` or head of `activeQueue`.
  3. Simulate crash between dequeue and destination confirmation; assert resume detects or recovers operation without duplication.
- **Pass Condition:** In-flight ownership lifecycle strictly enforced; zero lost operations on capacity failure or crash.

---

### 2.7 AC-REC-HANDOFF: Strict Reentrancy Exclusion & Zero-Loss ACTIVE Handoff
- **Target:** M4.3c
- **Invariant:** No externally supplied callback, transport adapter, observer, or user hook may execute inside the critical drain and `ACTIVE`-handoff section. Observer notifications and outbound repair messages are deferred. The final transition gate executes in a single synchronous JS turn:
  $$\text{activeQueue.size} === 0 \land \text{pendingQueue.size} === 0 \implies \text{admissionFence} = \text{'ACTIVE'}$$
  Proving zero operations can arrive un-quarantined or bypass admission at the boundary. Local submissions during transition return `{ allowed: false, code: 'REJECTED_RECOVERING' }` (not queued).
- **Verification:**
  - Inject concurrent operations and observer reentrancy attempts during drain and at the transition gate. Verify 100% exclusion during critical section and zero operations lost or bypassed.
- **Pass Condition:** Strict reentrancy exclusion verified; zero lost operations; zero queue bypassing.

---

### 2.7 AC-REC-FUTURE: Bounded Future-Epoch Buffer Contract
- **Target:** M4.3a / M4.3c
- **Invariant:** Operations originating from a future epoch ($epoch > installedEpoch$) must be stored in `FutureEpochBuffer` (bounded: max 100 ops, max 512 KiB). They **must never advance the current epoch's replication vector** or be applied to the older snapshot state. A 5-second grace timer awaits `EpochTransitionCapability`. If capability arrives, transition occurs; if timeout, recovery transitions to `DEGRADED`.
- **Verification:**
  1. Enqueue future-epoch operations; verify they buffer without touching engine state.
  2. Verify 5s timeout transitions to `DEGRADED` (`UNRESOLVED_FUTURE_EPOCH`).
  3. Inject `EpochTransitionCapability` before 5s; verify successful epoch transition.
  4. Attempt to enqueue 101 operations; verify `FUTURE_EPOCH_OVERFLOW`.
- **Pass Condition:** Strict future-epoch isolation; bounded secondary buffer; safe fallback to `DEGRADED`.

---

### 2.8 AC-REC-MEM: Honest Memory Accounting & Boundary Separation
- **Target:** M4.3a
- **Invariant:** Explicitly distinguishes hard enforced limits ($N \le 1000, B \le 4\text{ MiB}$, event cap 4 KiB) from analytical estimates ($\approx 12.6\text{ MiB}$ steady-state heap). Acknowledges transient string/object coexistence during JSON decode and non-deterministic GC.
- **Verification:**
  - Enqueue 1,000 maximum-size control events (3.5 KiB serialized, 64-peer causal vector) and 100 future epoch events. Measure analytical heap footprint and document transient decoding buffers.
- **Pass Condition:** Memory model verified with explicit boundary separation; analytical estimate verified at $\approx 12.6\text{ MiB}$.

---

### 2.9 AC-REC-LISTEN: Listener Coalescing, Ordering & Exception Isolation
- **Target:** M4.3b / M4.3c
- **Invariant:** `stateChangeListeners` are coalesced and flushed **exactly once** upon transition to `ACTIVE`. `decisionListeners` are flushed in exact FIFO causal order. Every listener is invoked inside a `try / catch` block. A listener exception **must never roll back engine state** or cause duplicate re-application.
- **Verification:**
  1. Register listener that throws an error.
  2. Complete recovery.
  3. Verify engine remains in `ACTIVE`, `mutableStats.listenerErrorCount` increments, and subsequent listeners execute.
- **Pass Condition:** Exception isolation proven; zero state rollback on listener error.

---

### 2.10 AC-REC-POST: Post-Commit Failure Semantics
- **Target:** M4.3c
- **Invariant:** Stage 3 is the irreversible commit point. If a failure occurs in Stage 4 (reconciliation timeout, unresolvable causal divergence, quarantine overflow), the engine **must not attempt rollback** (old history compacted). The coordinator halts drain and transitions safely to `DEGRADED`.
- **Verification:**
  - Inject 15s timeout or causal divergence during Stage 4. Verify engine state retains new snapshot base and recovery state transitions to `DEGRADED`.
- **Pass Condition:** Irreversible commit semantics verified; safe freeze in `DEGRADED`.

---

### 2.12 AC-REC-MATRIX: Exhaustive 36-Scenario Acceptance Matrix (R4-3)
- **Target:** M4.3d
- **Invariant:** The implementation must pass all 36 acceptance test scenarios defined in `PHASE7_5_M4_3_ARCHITECTURE.md` §13 with zero state corruption, zero resource leaks, and 100% adherence to expected states. This includes all 10 interrupted-drain adversarial failure cases (Cases 27–36: post-dequeue exceptions, pending queue capacity rejection, log insert failure, reducer error, post-acceptance crash before cleanup, in-flight transition to `DEGRADED`, resume without duplicate apply, duplicate arriving during in-flight, terminal rejection, and resume after capacity unblocks).
- **Verification:**
  - Execute dedicated test suite `recovery-acceptance-matrix.test.ts`.
- **Pass Condition:** 36/36 scenarios passing.

---

### 2.13 AC-REC-FROZEN: Zero Regression on Frozen Milestone Baselines
- **Target:** M4.3a–M4.3d
- **Invariant:** Frozen source files from prior milestones must not be modified:
  - `packages/protocol/*`
  - `packages/sync-engine/src/recovery/chunk-assembler.ts`
  - `packages/sync-engine/src/recovery/snapshot-serializer.ts`
  - `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`
  - `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`
  - `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`
  - `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts`
  - `packages/sync-engine/src/recovery/candidate-offer-registry.ts`
  - `packages/sync-engine/src/recovery/inbound-custody.ts`
- **Verification:**
  - Run `git diff <baseline-commit> -- <frozen-files>`.
- **Pass Condition:** 0 lines modified across all frozen files.

---

### 2.14 AC-REC-REGRESSION: Monorepo Test Suite Integrity
- **Target:** M4.3a–M4.3d
- **Invariant:** All 1,357 existing monorepo tests must continue passing without modification.
- **Verification:**
  - Run `pnpm test`.
- **Pass Condition:** 100% pass rate across all packages (`@syncplay/protocol`, `@syncplay/common`, `@syncplay/sync-engine`).
