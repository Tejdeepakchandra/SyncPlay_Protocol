# Phase 7.5 Sub-Milestone M5.0-R3 Correction Report
## Final Architecture Consistency & Evidence Closure

**Sub-Milestone:** Phase 7.5 M5.0-R3 (Final Architecture Consistency & Evidence Closure)  
**Author / Architect:** Senior TypeScript Distributed-Systems Architect  
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE VERIFICATION  
**Starting Documentation Commit:** `c70e487f028f157fec1a190f32ca75446a24b79f`  
**Direct Parent of Starting Commit:** `aad02bd4fd57e68a5182ed03459c4b7bd55d52c4`  
**Authoritative Accepted Baselines:**
- M4.3d Formally Accepted: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- M4.3c Accepted: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3b Accepted Correction: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- Approved Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`  
**Status:** **READY FOR INDEPENDENT FINAL REVIEW**

---

## 1. Executive Summary & Verification Confirmation

This report documents the resolution of all remaining review findings for Milestone M5.0-R3 ("Final Architecture Consistency & Evidence Closure"). 

Every finding (R3-01 through R3-08) identified in the architectural review has been reconciled against the accepted source code baselines (M4.3b, M4.3c, and M4.3d) and systematically harmonized across all five authoritative specification documents in `docs/`.

### Absolute Scope Compliance Confirmation
- **Production Code Modified:** Exactly 0 files, 0 lines (`packages/*/src/**/*.ts`, `packages/*/src/**/*.tsx` diff is empty).
- **Test Code Modified:** Exactly 0 files, 0 lines (`packages/**/__tests__/**` diff is empty).
- **Package Manifests & Locks Modified:** Exactly 0 files (`package.json`, `package-lock.json` diff is empty).
- **Configuration Modified:** Exactly 0 files (`tsconfig*.json` diff is empty).
- **Documentation Modified:** Exactly the 5 existing M5 specification files plus this correction report under `docs/`.
- **Implementation Status:** Milestone M5.1 implementation remains **strictly unstarted and unauthorized**.

---

## 2. Actual Files Changed in M5.0-R3

| File Path | Nature of Modification |
|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | Amended with browser-compatible wire byte accounting (`rawSerializedLength` preservation / `TextEncoder` fallback; zero re-serialization), safe incomparable snapshot recovery (prioritizing delta log repair; pre-install local operation stashing; checkpoint chain evidence requirements), three-tier lifecycle vs. operational mode taxonomy (`this.recoveryActive` notification-deferral clarification), overflow decision contract (`REJECTED_CAPACITY`, zero buffer retention, diagnostic preservation), protocol-stable ordinal ASCII peer-ID comparator (`comparePeerIds`), pending-queue capacity contract ($K=256$, count-only; missing byte bound identified as open design issue), capability custody failure & disposal semantics (reference identity, constructor failure unwinding, disposal dead-ending), and performance qualification (Node LTS dedicated CI runner vs browser execution characteristics; active fast-path zero allocation). |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | Amended with complete 7-variant `RemoteAdmissionDecision` (including `REJECTED_CAPACITY` with zero buffer retention), `CandidateSupplierSelectionResult` (7 distinct cases, explicitly distinguishing `INCOMPARABLE_COMPATIBLE` from `INSUFFICIENT_EVIDENCE`), normative `comparePeerIds()` ordinal ASCII comparator over `/^[a-zA-Z0-9_.:-]+$/`, and capability object-identity verification contracts. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | Amended with M5.0-R3 scopes, gating rules with regression floors ($\ge 1,550$ monorepo tests, $\ge 965$ sync-engine tests, $\ge 42$ recovery acceptance tests), and explicit execution stages for M5.1–M5.5 reflecting all R3 requirements. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | Amended with invariants `INV-M5-01` through `INV-M5-09` and criteria `AC-M5-01` through `AC-M5-29` reflecting wire byte accounting, safe incomparable recovery rules, three-tier lifecycle, overflow decisions, ordinal ASCII ordering, pending queue contracts, capability custody, and Node LTS performance verification. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | Amended with risks `R-M5-01` through `R-M5-12` incorporating mitigations for wire byte re-serialization overhead, incomparable snapshot data loss, lifecycle state desynchronization, overflow connection drops, locale-dependent peer ID sorts, pending queue memory exhaustion, capability leakage, and browser execution environments. |
| `docs/PHASE7_5_M5_0_R3_CORRECTION_REPORT.md` | Authored comprehensive final consistency report. |

---

## 3. Source References for Accepted Baseline Implementations

The architectural corrections in M5.0-R3 are grounded in direct source code inspections of the accepted baselines:

1. **Wire Byte Accounting & Transport Handoff:**
   - `packages/sync-engine/src/recovery/recovery-coordinator.ts` (lines 594–653):
     `interceptRemoteEvent(event: RemoteOperationEvent, rawSerializedLength?: number)` directly receives the wire-level byte count from the network transport adapter, passing it to `quarantineBuffer.enqueue(event, rawSerializedLength)`. No runtime re-serialization with `JSON.stringify` or Node `Buffer` occurs.
2. **Pending Queue Capacity & Log Sizing:**
   - `packages/sync-engine/src/log/pending-dependency-queue.ts` (lines 20–58):
     `PendingDependencyQueue` enforces `DEFAULT_CAPACITY = 256` matching `logCapacity`. Its capacity check `if (this.queue.length >= this.capacity) throw new SyncEngineError('REJECTED_CAPACITY', ...)` is strictly an operation **count** bound. There is currently no byte limit in the class.
3. **Peer ID Validation Domain:**
   - `packages/protocol/src/constants.ts` (line 87) & `packages/protocol/src/envelope.ts`:
     `ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/` defines the printable ASCII domain for all peer IDs, session IDs, and snapshot IDs (lengths 1..64 or 1..128).
4. **Lifecycle State vs. Operational Mode:**
   - `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (lines 911–924, 2311–2355):
     `this.recoveryActive` is an internal boolean flag controlling `notifyStateChange` and `notifyDecision` buffering to `deferredStateChanges` and `deferredDecisions`.
   - `packages/sync-engine/src/recovery/recovery-coordinator.ts` (lines 505–520):
     `RecoveryCoordinator.stateMachine.state` is the sole lifecycle state authority (`ACTIVE`, `SYNCHRONIZING`, `DRAINING`, `DEGRADED`, etc.). The engine flag is an operational notification-deferral switch slaved to this coordinator.
5. **Capability Custody & Object Identity:**
   - `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (lines 132–153, 911–924):
     `RecoveryInstallationCapability` uses reference identity verification: `if (capability !== this.authorizedRecoveryCapability) throw new SyncEngineError('FORBIDDEN', ...)`.
6. **Drain Receipts & In-Flight Bounds:**
   - `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (lines 977–1100):
     `admitDrainedOperation` returns one of 6 terminal/admission receipts (`ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, `CAPACITY_REJECTED`). Drain operates sequentially with $N_{\text{in-flight}} \le 1$.

---

## 4. Systematic Resolution of Review Findings (R3-01 through R3-08)

### 4.1 R3-01: Browser-Compatible Exact Byte Accounting Without Re-Serialization
- **Original Contradiction / Ambiguity:** Earlier documentation drafts suggested using Node-specific `Buffer.byteLength()` or re-serializing events via `JSON.stringify()` to compute byte footprints for quarantine accounting. Re-serialization introduces severe CPU overhead, double-serialization jitter, and discrepancies between transport byte frames and in-memory JSON representations.
- **Accepted Source Evidence Inspected:** `packages/sync-engine/src/recovery/recovery-coordinator.ts` (lines 594–653) defines `interceptRemoteEvent(event, rawSerializedLength)`. The transport layer (WebRTC data channel / WebSocket) already possesses the raw incoming frame length.
- **Final Normative Rule Adopted:**
  1. Wire byte lengths must be preserved from the transport layer and passed directly as `rawSerializedLength` into `interceptRemoteEvent()` and `quarantineBuffer.enqueue()`.
  2. Runtime re-serialization of incoming events via `JSON.stringify()` is strictly prohibited.
  3. When an event originates locally or arrives as a raw string without transport framing, browser-standard Web API `new TextEncoder().encode(wireString).length` is used. Node-specific `Buffer.byteLength()` is strictly forbidden in platform-agnostic packages.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 4.5.3 (Wire Byte Accounting Without Re-Serialization).
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-12b`, `AC-M5-12c`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-04` mitigation.
- **Remaining Limitations / Qualifications:** In memory, V8 object graphs incur internal pointer overhead beyond wire serialization byte lengths. Quarantine memory bounds constrain the transport-level wire representation.

---

### 4.2 R3-02: Safe Incomparable-Compatible Snapshot Recovery
- **Original Contradiction / Ambiguity:** Wording in M5.0-R2 indicated that an incomparable snapshot ($V_A \parallel V_B$) from an epoch-compatible peer could be installed directly. Overwriting active engine state with an incomparable snapshot would irrevocably erase locally originated or committed operations ahead of the snapshot ($S_{\text{local}} > S_{\text{snapshot}}$), violating causal durability and leading to divergence.
- **Accepted Source Evidence Inspected:** Causal lattice properties in `packages/sync-engine/src/causality/frontier-ordering.ts` and candidate selection in `packages/sync-engine/src/recovery/candidate-offer-registry.ts`.
- **Final Normative Rule Adopted:**
  1. An incomparable snapshot offer ($V_{\text{candidate}} \parallel V_{\text{local}}$) must NOT be installed as an atomic state wipe if the local engine has committed operations not represented in the snapshot.
  2. **Anti-Entropy Priority:** The coordinator must first attempt delta repair via `RepairRequestPipeline` / log anti-entropy before resorting to snapshot installation.
  3. **Local Operation Stashing:** If snapshot installation is required (e.g., local log truncated below snapshot horizon), any local operations ahead of or concurrent with the snapshot base must be stashed prior to snapshot commit and re-admitted via `admitDrainedOperation()` on top of the newly installed snapshot state.
  4. **Strict Evidence Gate:** Incomparable offers require verified full checkpoint chains and frontier logical timestamps; matching frontier fingerprints alone are insufficient evidence to rule out history forks.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 4.2 (Incomparable-Compatible Snapshot Recovery & Anti-Entropy Priority).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 3.2 (`CandidateSupplierSelectionResult` taxonomy: `INCOMPARABLE_COMPATIBLE` vs `INSUFFICIENT_EVIDENCE`).
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-05`, `AC-M5-06`, `INV-M5-01`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-09` mitigation.
- **Remaining Limitations / Qualifications:** If local operations cannot be re-applied onto the snapshot due to irreconcilable domain schema conflicts, recovery must abort to `DEGRADED` rather than silently dropping them.

---

### 4.3 R3-03: Three-Tier Recovery Lifecycle vs. Operational Recovery Mode
- **Original Contradiction / Ambiguity:** In previous drafts, the engine's internal flag `this.recoveryActive` and the coordinator's lifecycle state `RecoveryCoordinator.stateMachine.state` were sometimes conflated, creating confusion over which component owns recovery lifecycle authority.
- **Accepted Source Evidence Inspected:** `LeaderlessSyncEngine.isRecoveryActive()` / `setRecoveryMode(active)` (lines 911–924) and notification buffering in lines 2311–2355; `RecoveryCoordinator.transition()` in lines 505–520.
- **Final Normative Rule Adopted:** Explicit three-tier architectural hierarchy:
  - **Tier 1 (Sole Lifecycle Authority):** `RecoveryCoordinator.stateMachine.state` (`IDLE`, `SYNCHRONIZING`, `DRAINING`, `ACTIVE`, `DEGRADED`, etc.). All lifecycle transitions are initiated and governed exclusively here.
  - **Tier 2 (Public Engine State Queries):** Read-only external queries on `LeaderlessSyncEngine` (`isRecovering()`, `getRecoveryStatus()`) delegate directly to the coordinator.
  - **Tier 3 (Internal Operational Switch):** `LeaderlessSyncEngine.recoveryActive` is an internal operational notification-deferral flag. When `true`, it diverts `notifyStateChange` and `notifyDecision` to `deferredStateChanges` and `deferredDecisions` to prevent external subscribers from observing transient recovery intermediates. It possesses zero lifecycle decision authority and is strictly slaved to Tier 1.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 3.1 & 3.2 (Three-Tier Recovery Lifecycle vs. Operational Recovery Mode).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 1.1 & 2.1.
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `INV-M5-02`, `AC-M5-02`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-08`.
- **Remaining Limitations / Qualifications:** While notification deferral is active, subscribers do not receive updates until recovery handoff successfully commits and `flushDeferredNotifications()` is called.

---

### 4.4 R3-04: Complete Overflow Decision Contract Across Boundaries
- **Original Contradiction / Ambiguity:** Wording in M5.0-R1/R2 was inconsistent on how buffer overflow is exposed at the engine boundary—whether as an exception, connection termination, or explicit decision code, and whether rejected operations occupy any buffer memory.
- **Accepted Source Evidence Inspected:** `packages/sync-engine/src/recovery/recovery-coordinator.ts` lines 594–653; `packages/sync-engine/src/engine/leaderless-sync-engine.ts` lines 977–1100; `SyncEngineError('QUARANTINE_OVERFLOW')`.
- **Final Normative Rule Adopted:**
  1. When incoming operations exceed quarantine count or byte limits, `QuarantineBuffer.enqueue()` throws `SyncEngineError('QUARANTINE_OVERFLOW')`.
  2. `RecoveryCoordinator.interceptRemoteEvent()` catches the error, marks coordinator state as `DEGRADED`, and re-throws the error to the engine boundary.
  3. `LeaderlessSyncEngine.receive()` catches the capacity error, isolates the remote event error without tearing down transport connections, and records an `EngineDecision` with `code: 'REJECTED_CAPACITY'`.
  4. The rejected operation retains **0 bytes** of storage and zero buffer ownership; full diagnostics (`peerId`, `sequence`, `retainedCount`, `retainedBytes`) are recorded in the decision payload for auditability.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 4.5.4 (Complete Overflow Decision Contract).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 1.3 (`RemoteAdmissionDecision` 7-variant union: `REJECTED_CAPACITY`).
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-12h`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-04`.
- **Remaining Limitations / Qualifications:** Repeated capacity rejections from a specific remote peer do not automatically terminate the transport connection; backpressure signaling must be handled at the transport adaptation layer (M6).

---

### 4.5 R3-05: Protocol-Stable Ordinal ASCII Peer-ID Ordering
- **Original Contradiction / Ambiguity:** Using JavaScript's `String.prototype.localeCompare()` produces non-deterministic tie-breaking across different host environments, node locales, and browser runtime engines, which would break deterministic supplier selection.
- **Accepted Source Evidence Inspected:** `packages/protocol/src/constants.ts` line 87 (`ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/`), defining all peer identifiers as restricted printable ASCII.
- **Final Normative Rule Adopted:**
  1. The peer ID domain is strictly validated by `ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/` (length 1..64 or 1..128).
  2. Comparison must be performed using strictly ordinal ASCII character code comparisons:
     ```typescript
     export function comparePeerIds(a: string, b: string): number {
       return a < b ? -1 : (a > b ? 1 : 0);
     }
     ```
  3. `localeCompare()` is strictly prohibited in all tie-breaking and protocol-ordering logic.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 4.4 (Protocol-Stable Ordinal ASCII Peer-ID Ordering).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 3.1 (`comparePeerIds`).
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-10`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-09`.
- **Remaining Limitations / Qualifications:** Non-ASCII peer IDs fail schema validation at the envelope parser boundary and are rejected before reaching ordering routines.

---

### 4.6 R3-06: Pending-Queue Capacity Contract, Receipts Mapping, and Drain Ownership
- **Original Contradiction / Ambiguity:** In previous drafts, the capacity contract of `PendingDependencyQueue` was described as both count and byte-bounded, and the receipts returned when draining operations into the engine were not explicitly mapped.
- **Accepted Source Evidence Inspected:** `packages/sync-engine/src/log/pending-dependency-queue.ts` (lines 20–58); `packages/sync-engine/src/engine/leaderless-sync-engine.ts` (lines 977–1100).
- **Final Normative Rule Adopted:**
  1. **Count-Only Limit:** In the accepted source code, `PendingDependencyQueue` has `DEFAULT_CAPACITY = 256` (matching `logCapacity`). It enforces an operation count bound only.
  2. **Open Design Issue Formally Identified:** The missing aggregate byte bound on `PendingDependencyQueue` is explicitly identified as an open design issue deferred to post-M5 hardening.
  3. **Receipts Mapping:** Each operation admitted during quarantine drain via `admitDrainedOperation` maps to one of 6 terminal receipts: `ACCEPTED_REDUCED`, `ACCEPTED_PENDING`, `ALREADY_REPRESENTED`, `ALREADY_OWNED_DUPLICATE`, `TERMINALLY_REJECTED`, or `CAPACITY_REJECTED`.
  4. **Strict Sequential Drain:** Drain is single-threaded and enforces $N_{\text{in-flight}} \le 1$. An operation is dequeued from quarantine only after the engine completes processing and returns a receipt.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 4.5.1 & Section 7 (Open Issues).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 2.2.
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-13`, `AC-M5-14`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-12`.
- **Remaining Limitations / Qualifications:** A peer transmitting 256 pending operations with large payloads could consume excessive memory before triggering `REJECTED_CAPACITY`. This is mitigated during recovery by `QuarantineBuffer`'s aggregate byte bound (64 MB), but remains an open design issue for post-recovery pending queue usage.

---

### 4.7 R3-07: Capability Failure and Attachment Semantics
- **Original Contradiction / Ambiguity:** Previous documents described hypothetical runtime "capability revocation" mechanisms that do not exist in the accepted M4.3b source code, creating ambiguity regarding lifecycle attachment.
- **Accepted Source Evidence Inspected:** `packages/sync-engine/src/engine/leaderless-sync-engine.ts` lines 132–153, 911–924, and atomic replacement methods.
- **Final Normative Rule Adopted:**
  1. Capability custody relies strictly on object-identity reference equality: `capability === this.authorizedRecoveryCapability`.
  2. No dynamic runtime "revocation" token mutation exists.
  3. If coordinator construction fails, initialization unwinds cleanly; the capability reference is not retained by unstarted subsystems.
  4. Once an engine is disposed (`this.disposed === true`), all subsequent capability-guarded methods dead-end synchronously with `SyncEngineError('ENGINE_DISPOSED')`.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 3.3 (Capability Failure and Attachment Semantics).
  - `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`: Section 1.2.
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-03`, `INV-M5-06`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-02`.
- **Remaining Limitations / Qualifications:** Any component with legitimate reference custody of the capability object holds execution authority until engine disposal.

---

### 4.8 R3-08: Performance Contract & Runtime Environment Qualification
- **Original Contradiction / Ambiguity:** Unqualified latency targets (e.g., "$< 5\ \mu\text{s}$ p99") were stated without specifying the execution environment, and assertions of "zero allocation" were ungrounded.
- **Accepted Source Evidence Inspected:** Node.js V8 execution profiles and benchmark runner specifications.
- **Final Normative Rule Adopted:**
  1. The $5\ \mu\text{s}$ p99 fast-path interception budget applies specifically to the **Node.js LTS reference environment on dedicated CI hardware**.
  2. For browser runtimes, the performance requirement is qualified: execution must be non-blocking and avoid scheduling macrotasks or long microtask queues during fast-path event ingress.
  3. Allocation claims are clarified: in the `ACTIVE` fast path, interceptors avoid instantiating queues, wrapper objects, or promises, allowing V8 young-generation escape analysis to achieve zero persistent heap allocations.
- **Documents Amended:**
  - `docs/PHASE7_5_M5_ARCHITECTURE.md`: Section 6.2 (Performance Contract & Execution Environments).
  - `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`: `AC-M5-28`, `AC-M5-29`.
  - `docs/PHASE7_5_M5_RISK_REGISTER.md`: `R-M5-04`.
- **Remaining Limitations / Qualifications:** Browser thread contention (e.g., UI rendering or heavy DOM manipulation) can influence microtask timing; browser-specific SLAs will be baselined under M6 browser harness testing.

---

## 5. Cross-Document Consistency Matrix

The following matrix confirms absolute semantic alignment across all five authoritative specification documents following M5.0-R3 corrections:

| Requirement / Concept | ARCHITECTURE.md | INTERFACE_SPECIFICATION.md | IMPLEMENTATION_ROADMAP.md | ACCEPTANCE_CRITERIA.md | RISK_REGISTER.md | Status |
|---|---|---|---|---|---|---|
| **Wire Byte Accounting (R3-01)** | Section 4.5.3: wire preservation, `TextEncoder` fallback, no re-serialization | Section 1.3: `rawSerializedLength` parameter | Phase M5.3 / M5.4 test scopes | `AC-M5-12b`, `AC-M5-12c` | `R-M5-04`: byte accounting risk mitigation | **ALIGNED** |
| **Incomparable Recovery (R3-02)** | Section 4.2: anti-entropy priority, local stashing, checkpoint proof | Section 3.2: `INCOMPARABLE_COMPATIBLE` vs `INSUFFICIENT_EVIDENCE` | Stage M5.2 candidate logic | `AC-M5-05`, `AC-M5-06`, `INV-M5-01` | `R-M5-09`: vector divergence mitigation | **ALIGNED** |
| **Three-Tier Lifecycle (R3-03)** | Section 3.1: Tier 1 coordinator, Tier 2 queries, Tier 3 internal flag | Section 1.1 / 2.1: public queries vs coordinator state | Stage M5.1 lifecycle coupling | `INV-M5-02`, `AC-M5-02` | `R-M5-08`: state desynchronization mitigation | **ALIGNED** |
| **Overflow Contract (R3-04)** | Section 4.5.4: `REJECTED_CAPACITY`, 0 byte retention, diagnostics | Section 1.3: 7-variant `RemoteAdmissionDecision` | Stage M5.3 overflow tests | `AC-M5-12h` | `R-M5-04`: buffer overflow mitigation | **ALIGNED** |
| **Ordinal ASCII Peer IDs (R3-05)** | Section 4.4: `/^[a-zA-Z0-9_.:-]+$/`, ordinal `<` / `>`, no `localeCompare` | Section 3.1: normative `comparePeerIds()` | Stage M5.2 tie-break verification | `AC-M5-10` | `R-M5-09`: tie-break determinism mitigation | **ALIGNED** |
| **Pending Queue Contract (R3-06)** | Section 4.5.1: count $K=256$, drain receipts, $N_{\text{in-flight}} \le 1$ | Section 2.2: drain receipt signatures | Stage M5.3 drain sequencing | `AC-M5-13`, `AC-M5-14` | `R-M5-12`: drain capacity mitigation | **ALIGNED** |
| **Capability Custody (R3-07)** | Section 3.3: object identity, constructor unwind, disposal dead-end | Section 1.2: capability interface and verification | Stage M5.1 capability gating | `AC-M5-03`, `INV-M5-06` | `R-M5-02`: capability forgery mitigation | **ALIGNED** |
| **Performance Contract (R3-08)** | Section 6.2: Node LTS dedicated runner, browser non-blocking, zero heap alloc | Section 1.3: synchronous fast path | Stage M5.5 benchmark gates | `AC-M5-28`, `AC-M5-29` | `R-M5-04`: latency regression mitigation | **ALIGNED** |
| **Regression Floor Invariants** | Section 1.1: $\ge 1,550$ monorepo, $\ge 965$ engine, $\ge 42$ recovery | Roadmap Section 1: explicit floors | Roadmap Section 4: stage gating | `INV-M5-09`: non-negotiable floor | Section 1: regression guardrails | **ALIGNED** |

---

## 6. Open Issues & Hardening Backlog

In compliance with rigorous engineering governance, the following item is formally recorded:

- **ISSUE-M5-01: Aggregate Byte Limit on `PendingDependencyQueue`**
  - *Current Accepted Implementation:* `PendingDependencyQueue` (lines 20–58) enforces `DEFAULT_CAPACITY = 256` as an operation count limit only.
  - *Architectural Risk:* An adversary or malfunctioning peer transmitting 256 operations with maximal allowed payloads could consume considerable heap memory before triggering `REJECTED_CAPACITY`.
  - *Interim M5 Mitigation:* During recovery, `QuarantineBuffer` strictly enforces an aggregate wire byte bound (default 64 MB). Operations rejected by quarantine never enter `PendingDependencyQueue`.
  - *Action Item:* Deferred to post-M5 engine hardening; `PendingDependencyQueue` should be refactored to accept an optional byte capacity configuration and track aggregate byte weight.

---

## 7. Implementation Authorization & Status

- **M5.1 Implementation Status:** **UNAUTHORIZED AND UNSTARTED**.
- **Production Code Changes in M5.0-R3:** **0 lines**.
- **Test Code Changes in M5.0-R3:** **0 lines**.
- **Final Verdict:** **READY FOR INDEPENDENT FINAL REVIEW**.
