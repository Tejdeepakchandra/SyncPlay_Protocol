# Phase 7.5 M4.3-R1 — Architecture Consistency Closure Report

**Author:** Senior TypeScript Distributed-Systems Architect  
**Status:** M4.3 ARCHITECTURE READY FOR FINAL INDEPENDENT REVIEW — IMPLEMENTATION UNAUTHORIZED  
**Mode:** STRICTLY READ-ONLY SPECIFICATION (IMPLEMENTATION UNAUTHORIZED)  
**Milestone:** Phase 7.5 Sub-Milestone M4.3  
**Date:** November 23, 2026  
**Revision:** R1 Architecture Consistency Closure  
**Authoritative Baseline Commit:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Direct Parent:** `79919a0f8cb0bb81cb80cc010fe02918bef79787` (M4.2c-R6 closure)  
**Parent of Direct Parent:** `699019ee46e44e9ce1dfdfb4f7d1262bce239048` (M4.2c-R5 closure)  

---

## 1. Executive Summary & Authoritative Verification

This report provides the exhaustive architectural resolution and consistency closure for Sub-Milestone M4.3: **Recovery Orchestration & Lifecycle Integration**, closing all 10 challenges (R1-1 through R1-10) identified in the independent review.

### 1.1 Mandatory Execution Boundaries
- **Implementation Authorization:** NOT GRANTED. Zero runtime source, tests, schemas, exports, dependencies, or configuration modified.
- **Frozen Baselines Verified:** All files across M4.1 (`chunk-assembler.ts`), M4.2a (`snapshot-serializer.ts`, `snapshot-preparation-queue.ts`), M4.2b (`snapshot-chunk-producer.ts`), M4.2c (`snapshot-outbound-pipeline.ts`), and M4.2d (`snapshot-inbound-pipeline.ts`, `candidate-offer-registry.ts`, `inbound-custody.ts`) remain 100% byte-for-byte intact.
- **Repository Cleanliness:** Verified against HEAD `aafa366cc48fdb2b83940f00e80d92d813e7c52e`.

---

## 2. R1-1 — Genuine Single-Root Atomic Installation Proof

### 2.1 The Vulnerability in Multi-Field Synchronous Assignment
The initial specification claimed that executing sequential assignments synchronously in one JavaScript call turn guarantees atomicity due to run-to-completion semantics.  
**Independent Review Finding:** Run-to-completion prevents asynchronous event-loop interleaving, but provides **zero protection against synchronous runtime exceptions**. If an exception occurs on line 4 of an 8-property mutation (e.g. checkpoint deserialization error, clock reseeding boundary breach, or allocation failure), the live engine is left in a corrupted intermediate state.

### 2.2 The `EngineCoreState` Single-Root Architecture
To eliminate partial state corruption, all mutable execution structures of `LeaderlessSyncEngine` are unified into a single root container:

```typescript
export interface EngineCoreState {
  readonly state: PlaybackState;
  readonly baseSnapshot: PlaybackState;
  readonly baseSnapshotFrontier: Readonly<Record<string, number>>;
  readonly operationLog: OrderedOperationLog;
  readonly clockManager: LogicalClockManager;
  readonly pendingQueue: PendingDependencyQueue;
  readonly replicationVector: Readonly<Record<string, number>>;
  readonly localCausalContext: Readonly<Record<string, number>>;
  readonly lastAppliedEventId?: string;
  readonly stateVersion: number;
}
```

In `LeaderlessSyncEngine`:
- `private core: EngineCoreState;`
- All public and internal accessors (`playbackState`, `logicalClock`, `replicationVector`, `operationLog`) delegate directly to `this.core`.

### 2.3 The Four-Stage Installation Sequence & Zero-Throw Guarantee
1. **Stage 1 (Dry-Run Validation):** Pure out-of-band JSON, schema, RFC 8785 digest, epoch, and vector dominance checks. **Zero engine mutation.**
2. **Stage 2 (Staging Construction):** Pre-constructs candidate `LogicalClockManager`, candidate `OrderedOperationLog`, pre-ingests all compacted checkpoints out-of-band, instantiates candidate `PendingDependencyQueue`, and assembles `candidateCore: EngineCoreState`. **All potentially throwing work executes here; zero engine mutation.**
3. **Stage 3 (Irreversible Synchronous Atomic Commit Point):**
   ```typescript
   // Single machine-word pointer replacement:
   this.core = candidateCore;
   ```
   **Proof of Zero Throw:** Replacing an un-frozen property reference on an object in JavaScript cannot throw. Zero allocations, zero method calls, zero external listeners invoked.
4. **Stage 4 (Reconciliation Drain & Deferred Notification):** Drains quarantine buffer, replays advancing operations, and flushes deferred notifications in a single cohesive batch.

### 2.4 Behavior Across Failure Boundaries
- **Failure Before Commit (Stage 1 / Stage 2):** Live engine `this.core` is 100% untouched. Candidate objects are garbage-collected. Zero rollback required.
- **Failure During Commit (Stage 3):** Impossible. Pointer assignment is atomic and cannot throw.
- **Failure After Commit (Stage 4):** Installed snapshot base is authoritative. Unresolvable causal divergence or timeout transitions the engine safely to `DEGRADED`. Old state is not resurrected (preserves compaction safety).

---

## 3. R1-2 — Complete Membership-Epoch Drain Rules

Quarantined operations arriving during recovery are evaluated against the installed snapshot's membership epoch according to **five explicit cases**:

| Epoch Case | Relationship | Criteria | Action & Invariant |
| :---: | :--- | :--- | :--- |
| **Case 1** | **Malformed / Missing** | `epoch` is undefined, null, non-integer, or $< 1$ | **REJECT & DISCARD**. Emits protocol violation warning. Never admitted or replayed. |
| **Case 2** | **Older Epoch** | $op.epoch < installedEpoch$ | **STRICTLY DISCARDED**. The snapshot installed at `installedEpoch` already supersedes all operations from prior epochs. |
| **Case 3a** | **Matching Epoch Duplicate** | $op.epoch === installedEpoch \land op.seq \le installedFrontier[origin]$ | **DISCARDED**. Already incorporated into the compacted snapshot base state. Prevents double-apply. |
| **Case 3b** | **Matching Epoch Advancing** | $op.epoch === installedEpoch \land op.seq > installedFrontier[origin]$ | **APPLIED**. Replayed causally via `engine.receive()`. If causal gaps exist, buffered in `PendingDependencyQueue` and triggers repair. |
| **Case 4** | **Future Epoch** | $op.epoch > installedEpoch$ | **BUFFERED IN `FutureEpochBuffer`** (max 100 ops) for 5 seconds (`DEFAULT_EPOCH_CONVERGENCE_GRACE_MS`). **MUST NOT** be replayed against older snapshot base. If `EpochTransitionCapability` arrives, transition & drain; if timeout, transition to `DEGRADED`. |

---

## 4. R1-3 — Quarantine Admission & Drain Concurrency (Generational Handoff)

### 4.1 Admission Fence
The admission fence is owned exclusively by `RecoveryCoordinator`. In `RECOVERING` and `RECONCILING`, remote events are intercepted and routed to `QuarantineBuffer.activeQueue`.

### 4.2 Generational Double-Buffered Drain Algorithm
To guarantee that no operations are lost, duplicated, or bypass quarantine during drain:
1. `QuarantineBuffer` maintains two internal FIFO queues: `activeQueue` and `drainBatch`.
2. At the start of a drain pass, an atomic handoff occurs in a single synchronous JS turn:
   ```typescript
   this.drainBatch = this.activeQueue;
   this.activeQueue = new FIFOQueue<QuarantinedOperation>();
   ```
3. Coordinator iterates through `drainBatch` applying the 5-rule epoch filter and submitting advancing operations to `engine.receive()`.
4. If new operations arrive during the drain, they enter the new `activeQueue`.
5. When `drainBatch` is empty, the coordinator checks `activeQueue.size`. If $> 0$, it repeats the generational swap.
6. **The Final Transition Gate to `ACTIVE`:** Executed in a **single uninterrupted synchronous JavaScript turn**:
   ```typescript
   if (this.activeQueue.size === 0 && this.engine.pendingQueueSize === 0) {
     this.admissionMode = 'ACTIVE'; // Flips admission fence
     this.stateMachine.transition('ACTIVE');
     this.engine.flushDeferredNotifications();
   }
   ```
   **Proof:** Because emptiness check, fence flip, and state transition execute in the same synchronous turn, no operation can interleave. Every operation arriving before the flip is quarantined and drained; every operation arriving after enters `engine.receive()` directly.

---

## 5. R1-4 — Dual-Constraint Quarantine Memory Bounds

A simple 1,000-operation count limit is vulnerable to memory exhaustion under oversized payloads.

### 5.1 Dual-Constraint Enforcements
1. **Count Bound:** $N_{\text{max}} = 1,000$ operations (`MAX_QUARANTINE_OPERATIONS = 1000`).
2. **Byte Bound:** $B_{\text{max}} = 4 \text{ MiB}$ (4,194,304 bytes) (`MAX_QUARANTINE_TOTAL_BYTES = 4194304`).
3. **Per-Event Cap:** $S_{\text{max}} = 4 \text{ KiB}$ (4,096 bytes) (`MAX_QUARANTINE_EVENT_BYTES = 4096`). Events exceeding 4 KiB are rejected upon admission.

### 5.2 Byte Measurement & Memory Model
- **Measurement Standard:** Measured as UTF-8 byte length of the serialized message (`Buffer.byteLength(JSON.stringify(event))`).
- **Single-Representation Retention:** Only parsed `AnyControlSyncEvent` object graphs are retained. Raw strings and wire buffers are released immediately upon admission.
- **Analytical V8 Heap Accounting:**  
  In V8 (64-bit), a parsed JS object graph has an overhead of roughly $2.5\times$ to $3.0\times$ serialized JSON bytes:
  $$\text{Heap}_{\text{quarantine}} \le 4 \text{ MiB} \times 3.0 \approx 12 \text{ MiB}$$
  This guarantees bounded memory consumption on all browser and Node.js targets.

### 5.3 Overflow Behavior
- **During `RECOVERING`:** Breaching either 1,000 operations or 4 MiB triggers immediate fail-closed abort: calls `inboundPipeline.abortTransfer('LOCAL_ABORT', 'QUARANTINE_OVERFLOW')`, clears quarantine, and transitions to `DEGRADED`.
- **During `RECONCILING` (Post-Commit):** Draining halts immediately; engine freezes in `DEGRADED`. The installed snapshot base remains authoritative.

---

## 6. R1-5 — Terminal State, Disposal & Restart Lifecycle

### 6.1 Absorbing Terminal Flag (`isDisposed`)
- `private disposed = false;`
- Once `dispose()` is invoked, `disposed = true;`. This is an absorbing terminal state.
- `dispose()` is strictly idempotent: `if (this.disposed) return;`.
- All callbacks, timer triggers, and transport messages check `if (this.disposed) return;` at entry. No late callback can resurrect a terminated session.

### 6.2 Component Cleanup Ownership Matrix
- `SnapshotInboundPipeline`: Disposed transitively by coordinator. Disarms chunk watchdogs, overall timer, and calls `registry.dispose()` and `assembler.dispose()`.
- `QuarantineBuffer`: Cleared by coordinator (`quarantineBuffer.clear()`).
- Workflow Timers: `clearTimeout()` invoked for recovery (120s), reconciliation (15s), and offer await (10s) timers.
- Deferred Notifications: Dropped without emission to prevent post-disposal callbacks.
- `SnapshotOutboundPipeline`: Independent supplier lifecycle; NOT disposed by recovery coordinator.

### 6.3 Restart Semantics from `DEGRADED`
In `DEGRADED`, the coordinator is frozen waiting for external intervention or an admissible snapshot offer. When an eligible `SNAPSHOT_OFFER` arrives whose frontier strictly dominates the local replication vector, the coordinator resets retry counters, clears old errors, transitions `DEGRADED` $\to$ `RECOVERING`, and initiates a new transfer.

---

## 7. R1-6 — Listener & Side-Effect Suppression Catalog

| Observer / Callback | Installation Disposition | Reconciliation Disposition | Post-Reconciliation Emission |
| :--- | :--- | :--- | :--- |
| **`stateChangeListeners`** | **SUPPRESSED** | **SUPPRESSED** | Flushed **EXACTLY ONCE** in Stage 4 with the final converged `PlaybackState`. |
| **`decisionListeners`** | **SUPPRESSED** | **BUFFERED** | Flushed in FIFO order in Stage 4 for all advancing replayed operations. |
| **`peerGenerationListeners`** | **NOT SUPPRESSED** | **NOT SUPPRESSED** | Emits immediately (transport lifecycle independent of recovery). |
| **`mutableStats`** | Updates internal counters | Updates internal counters | Reflected immediately on `engine.stats`. |
| **`AntiEntropyManager`** | **PAUSED** | **ACTIVE** | Paused during transfer; active during reconciliation gap resolution. |

### 7.1 Reentrancy & Exception Isolation
`flushDeferredNotifications()` invokes each listener inside a `try / catch` block:
- If a subscriber throws an error, it is caught, logged, and `mutableStats.listenerErrorCount` is incremented. Subsequent listeners continue executing.
- If a listener re-entrantly calls an engine method, it is safe because the engine has already completed transition to `ACTIVE`.

---

## 8. R1-7 — Source Code & API Inventory Verification

| Module / Class | File Path | Method / Property | Category | Status / Verification |
| :--- | :--- | :--- | :--- | :--- |
| **`LeaderlessSyncEngine`** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | `exportSnapshotV2()` | Existing | Returns `StateSnapshotPayloadV2`. |
| **`LeaderlessSyncEngine`** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | `importSnapshotV2()` | Existing | Sequential import (M4.3 adds single-root alternative). |
| **`LeaderlessSyncEngine`** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | `replaceCoreStateAtomic()` | New Proposal | Single-root pointer assignment (`this.core = candidateCore`). |
| **`LeaderlessSyncEngine`** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | `setRecoveryMode()` | New Proposal | Enables input blocking and notification buffering. |
| **`LeaderlessSyncEngine`** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | `flushDeferredNotifications()` | New Proposal | Flushes deferred state and decision notifications. |
| **`OrderedOperationLog`** | `packages/sync-engine/src/log/ordered-operation-log.ts` | `reset()`, `insert()`, `setCompactedCheckpoint()` | Existing | Ingests checkpoints and resets log frontier. |
| **`PendingDependencyQueue`** | `packages/sync-engine/src/log/pending-dependency-queue.ts` | `enqueue()`, `drainSatisfied()`, `clear()` | Existing | Buffers operations with causal gaps. |
| **`AntiEntropyManager`** | `packages/sync-engine/src/replication/anti-entropy-manager.ts` | `processRemoteDigest()` | Existing | Compares digests via `ReplicationComparator`. |
| **`AntiEntropyManager`** | `packages/sync-engine/src/replication/anti-entropy-manager.ts` | `onSnapshotRecoveryRequired` | Extension | Callback emitted when comparison yields `REQUIRES_SNAPSHOT_RECOVERY`. |
| **`SnapshotInboundPipeline`**| `packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts`| `handleOffer()`, `startTransfer()`, `abortTransfer()` | Existing | Frozen M4.2d pipeline. |

---

## 9. R1-8 — Timeout and Retry Reconciliation

| Timer / Budget | Duration / Limit | Owner Subsystem | Replenished by Outer Timer? | Purpose |
| :--- | :--- | :--- | :---: | :--- |
| **Chunk Watchdog Timer** | 5,000ms | `SnapshotInboundPipeline` | **NO** | Detects individual missing chunk responses. |
| **Per-Chunk Retry Limit** | 3 retries | `SnapshotInboundPipeline` | **NO** | Max retries before triggering failover or abort. |
| **Transfer Retry Budget** | 10 retries | `SnapshotInboundPipeline` | **NO** | Cumulative retry budget across transfer session. |
| **Context Transition Ceiling** | 8 failovers | `SnapshotInboundPipeline` | **NO** | Max candidate promotions / generation replacements. |
| **Candidate Offer TTL** | 30,000ms | `CandidateOfferRegistry` | **NO** | Evicts stale candidate offers from registry. |
| **Transfer Session Timeout** | 60,000ms | `SnapshotInboundPipeline` | **NO** | Max duration for chunk assembly session. |
| **Offer Await Timeout** | 10,000ms | `RecoveryCoordinator` | **NO** | Max time to wait for an eligible offer before DEGRADED. |
| **Reconciliation Timeout** | 15,000ms | `RecoveryCoordinator` | **NO** | Max time allowed for Stage 4 quarantine drain. |
| **Overall Recovery Timeout** | 120,000ms | `RecoveryCoordinator` | **NO** | Absolute outer backstop deadline for recovery workflow. |

**The Non-Replenishment Invariant:** The 120s recovery timeout is an outer backstop. It **never resets or replenishes** inner pipeline retry counters. If `SnapshotInboundPipeline` exhausts its 10 retries, it aborts immediately, and the coordinator transitions to `DEGRADED`.

---

## 10. R1-9 — Candidate Snapshot Eligibility Policy

| Candidate Snapshot Frontier | Local Replication Vector | Eligibility Decision | Justification |
| :--- | :--- | :---: | :--- |
| $\text{offerEpoch} < \text{localEpoch}$ | Any | **STRICTLY REJECTED** (`OLDER_EPOCH`) | Obsolete membership generation. |
| $\text{offerEpoch} > \text{localEpoch}$ | Any | **BUFFERED (5s grace)** | Awaits epoch transition capability; admitted if transition occurs. |
| $\forall o: \text{offer}[o] \le \text{local}[o] \land \exists o: \text{offer}[o] < \text{local}[o]$ | Local is ahead | **STRICTLY REJECTED** (`DOMINATED_BY_LOCAL`) | Local node already has all data and is ahead. |
| $\forall o: \text{offer}[o] === \text{local}[o]$ | Frontiers equal | **STRICTLY REJECTED** (`EQUAL_TO_LOCAL`) | State already synchronized. No snapshot needed. |
| $\forall o: \text{offer}[o] \ge \text{local}[o] \land \exists o: \text{offer}[o] > \text{local}[o]$ | Remote dominates | **ADMISSIBLE & PREFERRED** | Causal superset of local knowledge. |
| $\exists o_1: \text{offer}[o_1] > \text{local}[o_1] \land \exists o_2: \text{local}[o_2] > \text{offer}[o_2]$ | Incomparable | **STRICTLY REJECTED** (`INCOMPARABLE_CONCURRENT`) | Arbitrary tie-breaking prohibited per P7.2/P7.3. |
| Missing sequence gap $> K=256$ log horizon | Outside horizon | **STRICTLY REJECTED** (`OUTSIDE_COMPACTED_HORIZON`) | Cannot reconcile missing history after install. |

---

## 11. R1-10 — Adversarial Integration Matrix (28 Scenarios)

The comprehensive 28-scenario adversarial matrix is formalized in `PHASE7_5_M4_3_ARCHITECTURE.md` §13, covering:
1. Candidate construction failures.
2. Pre-commit validation failures.
3. Multi-field staging exceptions.
4. Single-root swap guarantees.
5. Post-commit reconciliation failures.
6–9. Operation arrivals during validation, staging, installation, and drain.
10–12. Duplicate operations, causal dependency gaps, and future membership epochs.
13–15. Dual quarantine overflow (count, bytes, and post-commit overflow).
16–19. Mid-lifecycle disposal, late callbacks, and duplicate dispose calls.
20. Safe recovery restart from `DEGRADED`.
21–23. Incomparable candidates, candidate TTL expirations, and timer races.
24–25. Listener exceptions and decision listener reentrancy.
26–28. Supplier loss during installation, epoch changes during drain, and bootstrap outages.

Every scenario details expected engine state, recovery state, replication vector, cleanup action, and retry eligibility.

---

## 12. Final Status & Conclusion

All 10 architectural challenges raised in M4.3-R1 have been resolved with formal proofs, source code verification, and complete document synchronization.

**Final Status:**  
**`M4.3 ARCHITECTURE READY FOR FINAL INDEPENDENT REVIEW — IMPLEMENTATION UNAUTHORIZED`**
