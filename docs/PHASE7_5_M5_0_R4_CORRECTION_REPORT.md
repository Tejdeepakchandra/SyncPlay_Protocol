# Phase 7.5 Sub-Milestone M5.0-R4 Correction Report
## Final Quarantine Bound, Snapshot Replay & Commit Evidence Closure

**Sub-Milestone:** Phase 7.5 M5.0-R4 (Final Quarantine Bound, Snapshot Replay & Commit Evidence Closure)
**Author / Architect:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Direct Parent of R4 Commit:** `77285d6d0d7a079aabc47cfe296e4afae4b5ba87` (M5.0-R3 baseline)
**Authoritative Accepted Baselines:**
- M4.3d Formally Accepted: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- M4.3c Accepted: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3b Accepted Correction: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- Approved Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`
**Status:** **READY FOR INDEPENDENT FINAL REVIEW**

---

## 1. Executive Summary & Verification Confirmation

This report documents the resolution of all review findings for Milestone M5.0-R4 ("Final Quarantine Bound, Snapshot Replay & Commit Evidence Closure").

Every technical finding from the Engineering Authority review has been audited directly against the accepted source code baselines (`packages/sync-engine/src/...`, `packages/protocol/src/...`) and systematically harmonized across all five authoritative specification documents in `docs/`.

### Absolute Scope Compliance Confirmation
- **Production Code Modified:** Exactly 0 files, 0 lines (`packages/*/src/**/*.ts`, `packages/*/src/**/*.tsx` diff is empty).
- **Test Code Modified:** Exactly 0 files, 0 lines (`packages/**/__tests__/**` diff is empty).
- **Package Manifests & Locks Modified:** Exactly 0 files (`package.json`, `package-lock.json` diff is empty).
- **Configuration Modified:** Exactly 0 files (`tsconfig*.json` diff is empty).
- **Documentation Modified:** Exactly the 5 existing M5 specification files plus this correction report under `docs/`.
- **Implementation Status:** Milestone M5.1 implementation remains **strictly unstarted and unauthorized**.

---

## 2. Actual Files Changed in M5.0-R4

| File Path | Nature of Modification |
|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Reconciled `QuarantineBuffer` exact source memory bounds (1,000 ops / 4 MiB aggregate / 4 KiB event), added formal 6-category deterministic replay contract, 8 non-negotiable replay invariants, explicit distinction between snapshot installation atomicity and replay completion, and post-installation replay failure state exposure. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Explicitly added `DestinationAcceptanceReceipt` (6-variant union) and `DrainedInFlightRecord` type definitions, documented replay admission mapping, and verified buffer boundaries. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Updated Milestone M5.4 and M5.5 scopes to explicitly include 6-category replay verification, post-installation replay failure isolation, and exact quarantine bounds. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Updated `INV-M5-08` and Category 5 criteria (`AC-M5-26` through `AC-M5-30`) to formalize the deterministic 6-category replay contract, post-installation replay failure isolation, and exact quarantine source constants. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Updated `R-M5-04` (reconciling quarantine limits to 4 MiB source baseline), `R-M5-09` (deterministic 6-category replay contract & 8 invariants), and `R-M5-12` (post-installation replay failure handling). |
| `docs/PHASE7_5_M5_0_R4_CORRECTION_REPORT.md` | Authored comprehensive M5.0-R4 correction report. |

---

## 3. R4-01: Source Audit & Reconciliation of Actual Quarantine Memory Limits

### 3.1 Accepted Source Inspection
Direct source inspection of `packages/sync-engine/src/recovery/quarantine-buffer.ts` (lines 26–30, 34–38, 71–92, 149–188) establishes the authoritative implementation:

```typescript
// packages/sync-engine/src/recovery/quarantine-buffer.ts:26-29
export const MAX_QUARANTINE_OPERATIONS = 1000;
export const MAX_QUARANTINE_TOTAL_BYTES = 4 * 1024 * 1024; // 4 MiB (4,194,304 bytes)
export const MAX_QUARANTINE_EVENT_BYTES = 4096; // 4 KiB (4,096 bytes)
export const QUARANTINE_WARNING_THRESHOLD_BYTES = 3_355_443; // ~3.2 MiB (80%)
```

### 3.2 Authoritative Limit Values
1. **Maximum Retained Operation Count:** **1,000 operations** (`MAX_QUARANTINE_OPERATIONS = 1000`). Enforced in `enqueue()`: `if (this.totalRetainedCount + 1 > this.maxOperations) throw new SyncEngineError('QUARANTINE_OVERFLOW', ...)`.
2. **Maximum Aggregate Retained Wire Bytes:** **4 MiB** (4,194,304 bytes, `MAX_QUARANTINE_TOTAL_BYTES = 4 * 1024 * 1024`). Enforced in `enqueue()`: `if (this.totalRetainedBytes + op.serializedByteLength > this.maxTotalBytes) throw new SyncEngineError('QUARANTINE_OVERFLOW', ...)`.
3. **Maximum Individual Operation Wire Bytes:** **4 KiB** (4,096 bytes, `MAX_QUARANTINE_EVENT_BYTES = 4096`). Enforced in `enqueue()`: `if (op.serializedByteLength > this.maxEventBytes) throw new SyncEngineError('REJECTED_INVALID', ...)`.
4. **Configurability vs Hard-Coded:** Limits are **configurable** via constructor option `QuarantineBufferConfig { maxOperations?: number; maxTotalBytes?: number; maxEventBytes?: number; }`, defaulting to the above exported constants when omitted (`config?.maxTotalBytes ?? MAX_QUARANTINE_TOTAL_BYTES`). In `RecoveryCoordinator.ts:205`, `this.quarantineBuffer = config.quarantineBuffer ?? new QuarantineBuffer()`, thus inheriting the standard 1,000 ops / 4 MiB / 4 KiB default limits.
5. **Memory and Ownership of Rejected Operations:** Rejected operations retain **0 bytes** and **0 ownership** in `QuarantineBuffer`. If an individual event exceeds 4 KiB or causes a count or aggregate byte breach, `enqueue()` throws before mutating `this.activeQueue` or `this.identityIndex`, and before incrementing `this._activeBytes`.
6. **Serialized Wire Weight vs. JavaScript Heap Footprint:** The 4 MiB bound strictly constrains the retained serialized UTF-8 wire byte weight as received from transport frames, ensuring a deterministic, platform-independent boundary. In-memory JavaScript object graph allocations (V8 object headers, hidden class descriptors, internal array slots) naturally incur additional runtime heap overhead beyond raw wire bytes; this heap footprint is strictly bounded by the concurrent count limit ($K=1,000$ operations) and is completely reclaimed upon post-commit generational drain.
7. **Discrepancy Reconciliation:** The errant text reference to "64 MB" appearing in M5.0-R3 report sections 4.6 and 6 was an accidental drafting typo. The actual accepted implementation in `quarantine-buffer.ts:27` and all five M5.0-R2/R3 specification documents have consistently specified 4 MiB (4,194,304 bytes). That typo is now formally acknowledged, corrected, and reconciled across all documents.

---

## 4. R4-02: Formalization of Safe Local Operation Stashing and Deterministic Replay

When an incomparable snapshot ($V_{\text{candidate}} \parallel V_{\text{local}}$) is installed, local operations whose sequence numbers exceed the candidate snapshot's frontier ($S_{\text{local}} > S_{\text{snapshot}}$) are stashed in a `StashedReplayQueue` prior to snapshot commit. Following atomic snapshot installation, all stashed operations are sequentially reconciled under deterministic total-order state machine replication.

### 4.1 Dual Replay Trust Contexts: LOCAL_REPLAY vs. REPAIR_BATCH
To preserve the authoritative Phase 7.2 trust and validation boundaries (`docs/PHASE7_2_R5_FINAL_CORRECTIONS.md`), post-snapshot reconciliation strictly distinguishes two separate operation streams:

1. **Locally Committed Stashed Operations (`LOCAL_REPLAY` Context):**
   - **Nature:** Operations that were already validated, accepted, and committed in the local `OrderedOperationLog` prior to snapshot installation.
   - **Trust Boundary:** Trusted internal replay. Cannot introduce new origin peer IDs, new sequence numbers, or altered payloads.
   - **Validation Semantics:** Exempt from token-bucket arrival rate limits and remote clock inflation checks (`LogicalClockManager.validateAndAdvanceRemote` with `context === 'LOCAL_REPLAY'` returns `this.currentClock` without mutating Lamport clocks).
   - **Admission Evaluation:** Evaluated on top of the newly installed snapshot base. Their causal dependencies must be satisfied by the snapshot base checkpoints or earlier replayed operations.
   - **Fork / Failure Action:** If a locally committed stashed operation encounters a sequence collision, identity conflict ($H(op) \ne H(log)$), or missing dependency that cannot be resolved, this proves an irreconcilable history divergence between local committed state and the snapshot. Recovery **must immediately abort to `DEGRADED`**. Locally committed progress is NEVER silently dropped.
2. **Remotely Quarantined Operations (`REPAIR_BATCH` Context):**
   - **Nature:** Remote operations that arrived over the network while recovery was in progress (`RECOVERING` or `RECONCILING`). They were buffered in `QuarantineBuffer` and have never been admitted to the local log.
   - **Trust Boundary:** Untrusted remote historical/repair operations.
   - **Validation Semantics:** Evaluated under `REPAIR_BATCH` semantics via `engine.admitDrainedOperation(op)`. They bypass wall-clock token-bucket arrival rate limits, but must pass complete structural validation: epoch fencing, per-origin Lamport monotonicity, dependency resolution across snapshot checkpoints and log, and collision checks.
   - **Admission Evaluation:** Admitted into `OrderedOperationLog` only upon satisfying all causal dependencies (`ACCEPTED_REDUCED`) or buffered in `PendingDependencyQueue` (`ACCEPTED_PENDING`).

### 4.2 Deterministic Total-Order SMR Evaluation
Post-snapshot operation evaluation operates strictly under **Deterministic Total-Order State Machine Replication (SMR)** with operation-specific conflict resolution (not generic CRDT or OT).

The deterministic total ordering key is:
$$\text{OperationOrderingKey} = \langle \text{logicalTimestamp}, \text{originPeerId}, \text{logicalEventId} \rangle$$
enforced by `compareOperations(e1, e2)` (`packages/sync-engine/src/ordering/comparator.ts`):
1. Primary order: Lamport `logicalTimestamp` (ascending numerical).
2. Secondary order: Deterministic ordinal ASCII `originPeerId` tie-breaker.
3. Tertiary order: Stable `logicalEventId` tie-breaker.

Reducer entry points:
- **Incremental Admission:** `ConflictResolver.reduce(currentState, operation, monotonicNow, context, priorOperations)` (`packages/sync-engine/src/conflict/conflict-resolver.ts:40`).
- **Base Snapshot Replay:** `ConflictResolver.replay(baseState, operations, monotonicNow, context)` (`packages/sync-engine/src/conflict/conflict-resolver.ts:403`).

### 4.3 Deterministic 6-Category Replay Decision Matrix

| Category | Description & Condition | Engine Check in `admitDrainedOperation()` | Typed Receipt Returned | Replay Drain Action | Invariant Enforced |
|---|---|---|---|---|---|
| **Cat 1: Already Represented** | Covered by base snapshot frontier (`originSeq <= baseSnapshotFrontier[originPeerId]`). | `baseFrontier` check | `{ status: 'ALREADY_REPRESENTED', vectorAdvanced: false }` | **Discarded.** Safe retirement; in-flight custody released. | Never applied twice; idempotent retirement. |
| **Cat 2: Not Represented (Satisfied)** | Ahead of snapshot base (`originSeq > baseFrontier`), causal dependencies satisfied (`originSeq === haveSeq + 1` and all $reqSeq \le repVec$). | Causal context check | `{ status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: originSeq }` | **Retained & Reduced.** Admitted into `operationLog`, reduced into state, vector advanced. | Zero operation loss; deterministic reduction on snapshot base. |
| **Cat 3: Concurrent with Frontier** | Concurrent with snapshot frontier ($V_{\text{op}} \parallel V_{\text{snapshot}}$), causal dependencies satisfied by snapshot. | Total-order SMR evaluation (`compareOperations`) | `{ status: 'ACCEPTED_REDUCED', vectorAdvanced: true, newSequence: originSeq }` | **Retained & Reduced.** Deterministic reduction via `ConflictResolver.reduce()` / `replay()`. | Total order tie-break on $\langle L, \text{peerId}, \text{eventId} \rangle$ strictly preserved. |
| **Cat 4: Missing Dependencies** | Causal dependencies not yet satisfied (`originSeq !== haveSeq + 1` or missing dependency in $repVec$). | Missing dependency check | `{ status: 'ACCEPTED_PENDING', vectorAdvanced: false, reason: 'MISSING_DEPENDENCY' }` | **Retained in Queue.** Buffered in `PendingDependencyQueue`. Anti-entropy log repair requested. If queue full ($K=256$), drain halts immediately on `CAPACITY_REJECTED` $\to$ **Abort to `DEGRADED`**. | Never fabricate dependencies; in-flight custody preserved on queue overflow. |
| **Cat 5: Duplicate / Existing Ownership** | Already present in `replicationVector`, `operationLog`, or `pendingQueue`. | Duplicate index check | `{ status: 'ALREADY_OWNED_DUPLICATE', vectorAdvanced: false }` | **Discarded.** In-flight custody released without duplicate execution. | Exactly-once reduction guarantee; duplicate-safe. |
| **Cat 6: Validation / Reduction Failure** | Fails schema, epoch mismatch, identity conflict ($H(op) \ne H(log)$), or pre-horizon dependency breach. | Validation / clock checks | `{ status: 'TERMINALLY_REJECTED', vectorAdvanced: false, error: ... }` or throws | **Action depends on trust context:**<br/>• Quarantined remote (`REPAIR_BATCH`): Discarded (`DISCARDED`).<br/>• **Locally committed stashed (`LOCAL_REPLAY`):** **Abort to `DEGRADED`**. | **Never silently discard committed local progress!** Abort recovery if local history cannot safely reduce. |

### 4.4 The Eight Non-Negotiable Replay Invariants
1. **Never Silently Discard Committed Local Operations:** A locally committed operation that cannot be safely reduced onto the snapshot base must cause recovery to abort into `DEGRADED`. Silent dropping is strictly prohibited.
2. **Never Apply an Operation Twice:** Re-admission strictly fences against `baseSnapshotFrontier`, `replicationVector`, and `operationLog`, preventing duplicate state mutations.
3. **Never Fabricate Causal Dependencies:** If causal dependencies are missing, the operation enters `PendingDependencyQueue` and triggers delta repair; dependencies are never synthesized or assumed.
4. **Preserve Original Identity & Sequence:** Stashed operations retain their immutable `logicalEventId`, `originPeerId`, `originSequenceNumber`, `logicalTimestamp`, `causalContext`, and `payload`. Re-numbering or rewriting event identity is strictly forbidden.
5. **Preserve `LOCAL_REPLAY` vs `REPAIR_BATCH` Trust Distinction:** `LOCAL_REPLAY` applies strictly to already-accepted local operations re-anchoring on the new base state. `REPAIR_BATCH` applies to unadmitted network arrivals.
6. **Reuse Accepted M4.3c Single-Owner Drain:** Quarantined and stashed operations are processed with single-in-flight custody ($N_{\text{in-flight}} \le 1$) via `QuarantineDrainer`.
7. **Single Admission Pipeline:** All operations post-commit enter exclusively via `engine.admitDrainedOperation()`. No secondary or out-of-band admission paths may be introduced.
8. **Abort When Safe Replay Is Unprovable:** If historical compatibility cannot be proven, or if stashed operations cannot reduce cleanly, snapshot recovery must abort to `DEGRADED` rather than risking divergence or data loss.

### 4.5 Snapshot Installation Atomicity vs. Replay Completion & Degraded State
- **Snapshot Installation Atomicity:** Snapshot installation (`SnapshotInstaller.commitAtomic()`) is a **synchronous, indivisible, single-pointer swap** (`this.core = candidateCore;`) under JavaScript run-to-completion semantics. Intermediate states are impossible.
- **Irrevocable Base Replacement:** Once `replaceCoreStateAtomic(candidateCore, capability)` executes, the previous `EngineCoreState` is permanently superseded by the candidate `EngineCoreState`. The engine core now holds the newly installed base snapshot state.
- **Replay Completion:** Replay and quarantine drain are **asynchronous, multi-step batch operations** executed post-commit.
- **Post-Installation Failure Behavior:** If replay subsequently fails or halts (e.g. `CAPACITY_REJECTED`, validation failure on stashed local operation, unhandled exception, or timeout):
  1. The engine does NOT roll back to the pre-installation state (the previous root is permanently unanchored). The engine remains anchored on the newly installed base snapshot with an incomplete replay log.
  2. `RecoveryCoordinator` transitions immediately to `state: 'DEGRADED'`, `substate: 'DRAIN_FAILED'`.
  3. `engine.setRecoveryMode(true)` is **maintained** (operational deferral remains locked; recovery mode is NOT cleared).
  4. `engine.flushDeferredNotifications()` is **NEVER called**. External listeners receive zero intermediate or corrupted notifications.
  5. The failing operation remains preserved in `QuarantineDrainer.inFlightRecord`.
  6. Un-drained operations remain in `QuarantineBuffer` custody.
  7. Public engine queries expose `isRecoveryInProgress() === false` and `recoveryState === 'DEGRADED'`. Under no circumstances can recovery report or transition to `'ACTIVE'`.
  8. Coordinator exposes diagnostics via `getMetrics()` and error events for operator inspection or automated session reset.

---

## 5. R4-03: Verification of Committed Scope

### 5.1 Verification of the Starting Documentation Baseline (M5.0-R3 Commit)
The direct parent of this M5.0-R4 work is the accepted M5.0-R3 commit:
- **R3 Commit:** `77285d6d0d7a079aabc47cfe296e4afae4b5ba87`
- **R3 Parent:** `c70e487f028f157fec1a190f32ca75446a24b79f`

The range `c70e487f028f157fec1a190f32ca75446a24b79f..77285d6d0d7a079aabc47cfe296e4afae4b5ba87` represents the M5.0-R3 changes:
```bash
$ git diff --name-status c70e487f028f157fec1a190f32ca75446a24b79f 77285d6d0d7a079aabc47cfe296e4afae4b5ba87
A	docs/PHASE7_5_M5_0_R3_CORRECTION_REPORT.md
M	docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md
M	docs/PHASE7_5_M5_ARCHITECTURE.md
M	docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md
M	docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md
M	docs/PHASE7_5_M5_RISK_REGISTER.md
```

### 5.2 Scope Verification of M5.0-R4 Changes
The M5.0-R4 changes over `77285d6d0d7a079aabc47cfe296e4afae4b5ba87` contain strictly the five existing specification documents plus this M5.0-R4 correction report.

Zero production source files (`packages/*/src/**/*.ts`), zero test files (`packages/**/__tests__/**`), zero configuration files (`tsconfig*.json`), and zero package files (`package.json`, `package-lock.json`) were touched.

All trailing whitespace has been eliminated, ensuring `git diff --check` passes with zero warnings.

---

## 6. Open Issues & Hardening Backlog

- **ISSUE-M5-01: Aggregate Byte Limit on `PendingDependencyQueue`**
  - *Current Accepted Implementation:* `PendingDependencyQueue` (lines 20–58) enforces `DEFAULT_CAPACITY = 256` as an operation count limit only.
  - *Architectural Risk:* An adversary or malfunctioning peer transmitting 256 operations with maximal allowed payloads could consume considerable heap memory before triggering `REJECTED_CAPACITY`.
  - *Interim M5 Mitigation:* During recovery, `QuarantineBuffer` strictly enforces an aggregate wire byte bound of 4 MiB (4,194,304 bytes). Operations rejected by quarantine never enter `PendingDependencyQueue`.
  - *Action Item:* Deferred to post-M5 engine hardening; `PendingDependencyQueue` should be refactored to accept an optional byte capacity configuration and track aggregate byte weight.

---

## 7. Implementation Authorization & Status

- **M5.1 Implementation Status:** **UNAUTHORIZED AND UNSTARTED**.
- **Production Code Changes in M5.0-R4:** **0 lines**.
- **Test Code Changes in M5.0-R4:** **0 lines**.
- **Final Verdict:** **READY FOR INDEPENDENT FINAL REVIEW**.
