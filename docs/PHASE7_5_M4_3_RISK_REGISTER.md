# Phase 7.5 M4.3 — Recovery Orchestration & Lifecycle Integration Risk Register

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

## 1. Executive Summary & Risk Governance

Sub-Milestone M4.3 governs the core lifecycle and recovery transitions in the leaderless distributed synchronization engine: **incorporating foreign snapshot state into an active, running node without split-brain, state corruption, reference leakage, reentrancy anomalies, or message loss**.

Following the R4 architectural review, this register details eight concrete distributed-systems, memory, and concurrency risks, their root causes, severity ratings, automated architectural defenses, telemetry triggers, and test verification strategies. In particular, R4 resolves:
1. **Explicit In-Flight Drain Ownership (R4-1):** Dedicated `DRAINER_IN_FLIGHT` state with $N_{\text{in-flight}} \le 1$ cardinality. Full 6-step lifecycle with explicit `DestinationAcceptanceReceipt` confirmation before release, guaranteeing zero operation loss on capacity rejection or post-dequeue crash.
2. **Destination-Specific Acceptance (R4-2):** Rigorous separation of destination acceptance states (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, `CAPACITY_REJECTED`). Failed destination insertions are never treated as successful transfers.
3. **Interrupted-Drain Adversarial Coverage (R4-3):** 36-scenario acceptance matrix covering all 10 adversarial failure cases with explicit final ownership and vector advance invariants.
4. **Root Reference Ownership Clarification (R4-4):** Explicit documentation that `Object.seal()` is shallow; recursive freeze applied to playback states; controlled internal mutation strictly confined to the engine; candidate creator drops all mutable aliases post-handoff; old root garbage collection eligibility without false claims of deterministic GC.

---

## 2. Risk Matrix & Severity Summary

| Risk ID | Title | Likelihood | Impact | Severity | Primary Mitigation |
| :---: | :--- | :---: | :---: | :---: | :--- |
| **RISK-M4.3-1** | Partial Multi-Field State Swap Failure & Reference Leakage | Low | Critical | **High** | Shallow-sealed `EngineCoreState` container, deeply frozen playback states, engine-confined mutation, single run-to-completion reference assignment, and creator dereferencing (`candidateCore = null`). |
| **RISK-M4.3-2** | Memory Breach & Transient Decode Coexistence Underestimation | Medium | High | **High** | Hard dual cap ($N \le 1,000$ ops, $B \le 4\text{ MiB}$ serialized UTF-8 bytes, 4 KiB per-event cap); honest classification of $\approx 12.6\text{ MiB}$ as analytical steady-state estimate; fail-closed `DEGRADED` abort. |
| **RISK-M4.3-3** | In-Flight Drain Disappearance & Destination Rejection Stalls | Medium | High | **High** | Explicit `DRAINER_IN_FLIGHT` state ($N_{\text{in-flight}} \le 1$); typed `DestinationAcceptanceReceipt`; in-flight unshift/recovery on capacity failure or crash; contiguous vector advancement; AE repair requests. |
| **RISK-M4.3-4** | Future-Epoch Event Poisoning or Replay Inversion | Medium | High | **High** | Bounded `FutureEpochBuffer` (100 ops, 512 KiB) with 5s grace period; strict prohibition of replaying on old state; fail-closed transition to `DEGRADED` upon timeout. |
| **RISK-M4.3-5** | Synchronous Reentrancy Corruptions & Premature Interface Emission | Medium | High | **High** | Zero external callbacks/emissions inside critical drain; deferring observers and outbound repair messages; rejecting local submissions with `REJECTED_RECOVERING`; 4-step critical handoff. |
| **RISK-M4.3-6** | Incomparable Candidate Arbitrary Tie-Breaking | Low | Critical | **High** | P7.4 digest semantics; strict rejection of incomparable offers (`INCOMPARABLE_CONCURRENT`) without arbitrary tie-breaking. |
| **RISK-M4.3-7** | Uncoordinated Timeout & Retry Budget Conflicts | Medium | Medium | **Medium** | Strict timeout hierarchy; outer 120s timer never resets or replenishes inner 10-retry or 8-context-transition budgets. |
| **RISK-M4.3-8** | Post-Commit Rollback Impossibility | Low | Critical | **High** | Stage 3 irreversible commit point; post-commit failures freeze safely in `DEGRADED`. |

---

## 3. Granular Risk Analysis

### RISK-M4.3-1: Partial Multi-Field State Swap Failure & Reference Leakage (R4-4)
- **Trigger Conditions:** An exception is thrown midway through sequential property assignments, or candidate references remain retained by the creator after installation, causing memory leaks and dual-mutation hazards, or relying on `Object.seal()` to freeze nested properties.
- **Impact:** Engine left in an inconsistent intermediate state where clock, playback position, and causal frontiers contradict each other, or multiple subsystems hold mutable references to active engine internals.
- **Architectural Defense:**
  1. **Shallow Sealing with Encapsulated Engine Mutation:** `EngineCoreState` is sealed via `Object.seal()` to prevent property injection/deletion. The architecture acknowledges that sealing is shallow: nested `playbackState` is deeply frozen via `Object.freeze()`, while mutable vectors, logs, and queues are strictly confined to private engine transition methods. External accessors expose defensive frozen projections (`Object.freeze({ ...replicationVector })`) or readonly interfaces.
  2. **Out-of-Band Staging (Stage 2):** All allocations, validations, checkpoint ingestions, and vector constructions execute out-of-band to assemble a new candidate `EngineCoreState`.
  3. **Single-Root Reference Replacement (Stage 3):** The commit point is a single reference assignment:
     $$\text{this.core} = \text{candidateCore}$$
     Under JavaScript run-to-completion semantics, this synchronous assignment cannot interleave with external reads or writes and cannot throw. Zero intermediate state is observable.
  4. **Creator Dereferencing:** Upon completing Stage 2 handoff to the engine, `SnapshotInstaller` explicitly nullifies its internal reference:
     $$\text{candidateCore} = \text{null}$$
     ensuring no external creator retains a mutable alias to the live core state.
  5. **GC Eligibility:** The old root is dereferenced and becomes eligible for garbage collection once local call frames finish, without false claims of deterministic GC.
- **Telemetry Counter:** `syncplay_recovery_swap_failed_total`
- **Verification:** Unit test throwing simulated errors during validation and staging, confirming live engine core before and after remains 100% intact; reference-leak assertion verifying candidate references are nullified post-install.

---

### RISK-M4.3-2: Memory Breach & Transient Decode Coexistence Underestimation
- **Trigger Conditions:** High-throughput mesh generation of live operations during snapshot transfer over a high-latency link, or treating analytical memory estimates as hard runtime ceilings without accounting for transient JSON decode coexistence.
- **Impact:** Memory exhaustion (OOM), browser tab crash, or uncontrolled GC pauses.
- **Architectural Defense:**
  1. **Hard Enforced Programmatic Limits:**
     - Quarantine count limit: $N \le 1,000$ operations.
     - Quarantine byte limit: $B \le 4 \text{ MiB}$ serialized UTF-8 bytes.
     - Per-event byte limit: $S \le 4 \text{ KiB}$ (strictly justified by max control event + 64-peer causal vector).
     - Future-epoch limits: $N_{\text{future}} \le 100$ operations, $B_{\text{future}} \le 512\text{ KiB}$.
     - Pending dependency queue capacity: 256 operations.
  2. **Honest Memory Accounting & Transient Decode Model:**
     - The architecture explicitly models transient string and parsed object coexistence during `JSON.parse()` before garbage collection reclaims wire buffers.
     - The $\approx 12.6\text{ MiB}$ bound is classified as an **analytical steady-state estimate** based on 64-bit V8 heap overhead ($2.5\times$ to $3.0\times$ serialized size), not an impossibly rigid V8 hard ceiling.
  3. **Fail-Closed Protection:** Enqueuing an operation that breaches either count or byte limit immediately triggers fail-closed abort with error code `QUARANTINE_OVERFLOW`, safely freezing the node in `DEGRADED`.
- **Telemetry Counter:** `syncplay_recovery_quarantine_overflow_total`, `syncplay_recovery_quarantine_bytes`
- **Verification:** Adversarial test flooding quarantine buffer with 1,001 operations and 4.1 MiB of payload, verifying immediate fail-closed transition to `DEGRADED`.

---

### RISK-M4.3-3: In-Flight Drain Disappearance & Destination Rejection Stalls (R4-1 & R4-2)
- **Trigger Conditions:** An operation is dequeued via `activeQueue.shift()`, but destination acceptance fails due to `PendingDependencyQueue` capacity (256/256), a post-dequeue exception, or an interrupted turn, causing the operation to disappear from both the queue and the engine.
- **Impact:** Operation loss, silent drop of committed user intents, broken causal chains, or advancing the replication vector across an unapplied gap.
- **Architectural Defense:**
  1. **Explicit In-Flight Ownership:** When dequeued, the operation enters `DRAINER_IN_FLIGHT` owned exclusively by `QuarantineDrainer.inFlightRecord`.
  2. **Strict Cardinality ($N_{\text{in-flight}} \le 1$):** At most one operation is in-flight at any instant, eliminating batch splitting or ambiguous partial rollbacks.
  3. **Typed Destination Confirmation Receipts:** Drainer releases in-flight ownership (`inFlightRecord = null`) ONLY upon receiving a confirmed status (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`).
  4. **Recoverable Failure Handling:** If destination rejects due to capacity (`CAPACITY_REJECTED`) or an error occurs, the operation remains preserved in `inFlightRecord` (or is restored to `activeQueue` via `unshift()`). Drain halts immediately, and recovery enters `DEGRADED`.
  5. **Contiguous Vector Advancement:** Ops 1..40 commit to `OrderedOperationLog` and advance `replicationVector`. Op 41 is buffered in `PendingDependencyQueue`; `replicationVector` **does not advance across the gap**. Ops 42..100 depending on 41 buffer in `PendingDependencyQueue`.
  6. **Exactly-Once Resumption via Anti-Entropy:** Outbound `REPAIR_REQUEST` is queued in `deferredRepairRequests`. When the missing operation arrives via repair, `drainPendingOperations()` automatically unblocks ops 41..100 in causal order without re-applying ops 1..40 or losing ops 42..100.
  7. **Single Authoritative Owner Invariant:** At every microsecond, exactly one container owns the operation: `activeQueue`, `inFlightRecord`, `operationLog`, or `pendingQueue`.
- **Telemetry Counter:** `syncplay_recovery_inflight_degraded_total`, `syncplay_recovery_partial_drain_gap_total`, `syncplay_recovery_drain_unblocked_total`
- **Verification:** Acceptance matrix tests 27–36 verifying that post-dequeue crashes, capacity rejections, and resume cycles preserve operations without loss or duplication.

---

### RISK-M4.3-4: Future-Epoch Event Poisoning or Replay Inversion
- **Trigger Conditions:** The peer mesh advances to membership epoch $E+1$ while the local node is transferring or reconciling a snapshot from epoch $E$.
- **Impact:** Applying operations from a future epoch against an older snapshot base corrupts epoch causality and violates epoch fencing invariants.
- **Architectural Defense:**
  1. Quarantined operations with $epoch > installedEpoch$ are NEVER applied to the older snapshot base.
  2. Routed to `FutureEpochBuffer` (bounded: max 100 operations, max 512 KiB) with a 5-second convergence grace timer (`DEFAULT_EPOCH_CONVERGENCE_GRACE_MS`).
  3. If an authorized `EpochTransitionCapability` arrives within 5 seconds, local engine transitions epoch and re-evaluates operations; if grace period expires without transition, coordinator transitions to `DEGRADED`.
- **Telemetry Counter:** `syncplay_recovery_future_epoch_buffered_total`, `syncplay_recovery_future_epoch_timeout_total`
- **Verification:** Test feeding future epoch events into quarantine; verifying they buffer without applying to engine, and verify transition to `DEGRADED` upon timeout.

---

### RISK-M4.3-5: Synchronous Reentrancy Corruptions & Premature Interface Emission
- **Trigger Conditions:** `LeaderlessSyncEngine` notifies external subscribers, emits transport messages, or invokes repair callbacks during the critical drain loop or ACTIVE handoff, allowing user or network callbacks to interleave reentrantly.
- **Impact:** Observer flicker, unexpected reentrant mutations during reconciliation, broken state machine invariants, or duplicate repair loops.
- **Architectural Defense:**
  1. **Strict Reentrancy Exclusion Rule:** Zero external callbacks or adapter emissions inside the critical drain and handoff section.
  2. **Enumeration & Isolation of All 6 Interfaces:**
     - `stateChangeListeners`: Deferred in private buffer; coalesced into a single final invocation post-handoff.
     - `decisionListeners`: Deferred in private FIFO buffer; flushed in causal order post-handoff.
     - `peerGenerationListeners`: Deferred in private buffer; flushed post-handoff.
     - `TransportAdapter.onEmitMessage`: Outbound repair messages buffered in `deferredRepairRequests`; flushed only after transition to `ACTIVE`.
     - `RepairRequestPipeline`: Repair callbacks isolated; no synchronous pipeline triggers inside drain loop.
     - Public Intent API (`engine.play()`): Synchronously rejected with `{ allowed: false, code: 'REJECTED_RECOVERING' }` (not queued).
  3. **Synchronous 4-Step Critical Section:**
     ```typescript
     // Single synchronous call frame (Zero await, Zero yields, Zero callbacks)
     // Step 1: Verify drain completion
     if (this.activeQueue.size === 0 && this.engine.pendingQueueSize === 0) {
       // Step 2: Flip admission fence
       this.admissionFence = 'ACTIVE';
       // Step 3: Transition recovery state machine
       this.stateMachine.transition('ACTIVE');
       // Step 4: Deferral handoff & flush
       this.engine.setRecoveryMode(false);
       this.engine.flushDeferredNotifications();
       this.flushDeferredRepairRequests();
     }
     ```
  4. Individual listener calls execute inside `try / catch` blocks to isolate subscriber exceptions and protect reentrancy.
- **Telemetry Counter:** `syncplay_recovery_deferred_notifications_flushed_total`
- **Verification:** Test asserting 0 listener invocations and 0 transport emissions during Stage 3 and Stage 4; verifying reentrant submissions return `REJECTED_RECOVERING`; verifying exactly one coalesced state notification upon return to `ACTIVE`.

---

### RISK-M4.3-6: Incomparable Candidate Arbitrary Tie-Breaking
- **Trigger Conditions:** Two or more peers send snapshot offers with concurrent, incomparable frontiers ($\exists o_1: A[o_1] > B[o_1] \land \exists o_2: B[o_2] > A[o_2]$).
- **Impact:** Arbitrary tie-breaking (e.g. by peer ID or Lamport timestamp) permanently discards committed operations from the rejected history branch, violating Strong Eventual Consistency (SEC).
- **Architectural Defense:**
  1. Per P7.2/P7.3 consistency invariants, arbitrary tie-breaking for incomparable snapshots is **STRICTLY PROHIBITED**.
  2. Candidate eligibility validator rejects incomparable offers (`INCOMPARABLE_CONCURRENT`).
  3. If no strictly dominating candidate is available in the mesh, recovery halts in `DEGRADED` to prevent silent split-brain.
- **Telemetry Counter:** `syncplay_recovery_incomparable_rejected_total`
- **Verification:** Test presenting two incomparable offers to the coordinator, verifying both are rejected and recovery does not guess or tie-break.

---

### RISK-M4.3-7: Uncoordinated Timeout & Retry Budget Conflicts
- **Trigger Conditions:** An outer recovery timeout (120s) resets or replenishes the inner retry budgets of `SnapshotInboundPipeline` (10 transfer retries, 8 context transitions), causing unconstrained request amplification.
- **Impact:** Thundering herd of chunk requests, network congestion, or infinite retry loops.
- **Architectural Defense:**
  1. **Strict Timeout & Budget Decoupling:**
     - The 120s recovery timeout is an absolute outer deadline for the workflow.
     - It DOES NOT reset or replenish the 10-retry or 8-context-transition budgets of the inbound pipeline.
     - If the inbound pipeline exhausts its retry budget, it aborts transfer immediately and coordinator transitions to `DEGRADED`.
- **Telemetry Counter:** `syncplay_recovery_timeout_triggered_total`
- **Verification:** Concurrency simulation test verifying that pipeline retry exhaustion transitions to `DEGRADED` immediately without waiting for outer 120s timer.

---

### RISK-M4.3-8: Post-Commit Rollback Impossibility
- **Trigger Conditions:** An error occurs in Stage 4 (reconciliation timeout, causal conflict, quarantine overflow) after Stage 3 atomic commit has executed.
- **Impact:** Attempting rollback to pre-recovery state would corrupt compacted history and cause state divergence.
- **Architectural Defense:**
  1. Stage 3 is the irreversible commit point. Once `this.core = candidateCore` executes, rollback is prohibited.
  2. The new snapshot base remains authoritative.
  3. The coordinator halts drain and transitions safely to `DEGRADED`.
  4. The node can only recover via a newer strictly dominating snapshot offer from an equal or newer epoch.
- **Telemetry Counter:** `syncplay_recovery_post_commit_degraded_total`
- **Verification:** Test injecting 15s reconciliation timeout after Stage 3 commit; verify engine freezes safely in `DEGRADED` without rolling back.

---

## 4. Summary of Residual Risk

With the R4 architectural defenses enforced:
- **State Mutation & Reference Leakage Risk:** Eliminated via shallow-sealed `EngineCoreState`, recursive freeze on state projections, engine-confined mutation, candidate creator dereferencing (`candidateCore = null`), and single run-to-completion pointer assignment.
- **Memory & Resource Accounting Risk:** Strictly bounded by programmatic limits ($N \le 1,000, B \le 4\text{ MiB}, S \le 4\text{ KiB}$) with honest analytical separation of steady-state estimates ($\approx 12.6\text{ MiB}$) and transient decode coexistence.
- **Reentrancy & Observer Risk:** Eliminated via complete reentrancy exclusion, isolation of all 6 external interfaces, deferred repair buffers, and synchronous 4-step handoff.
- **In-Flight Drain Ownership & Causal Progress Risk:** Eliminated via explicit `DRAINER_IN_FLIGHT` ownership state ($N_{\text{in-flight}} \le 1$), typed destination confirmation receipts (`DestinationAcceptanceReceipt`), in-flight unshift/recovery on capacity failure or crash, contiguous vector advancement, and 36-scenario adversarial verification.
- **Protocol Regression Risk:** 0 modifications to frozen code ensure 100% backward and forward compatibility.

The residual risk is classified as **LOW**, fully meeting the criteria for final architectural acceptance.
