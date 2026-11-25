# Phase 7.5 Milestone M5.0-R7 — Capability Custody & Reset Failure Semantics Closure Report

**Milestone:** Phase 7.5 Sub-Milestone M5.0-R7 (Architecture Consistency & Custody Closure)<br/>
**Date:** 2026-11-25<br/>
**Author:** Senior TypeScript Distributed-Systems Architect<br/>
**Authoritative Starting Baseline:** `06fc77626422a85fa51bbb592605fdbd039b9002` (Milestone M5.0-R6 Committed)<br/>
**Preceding Baselines:** M5.0-R5 (`0e15f31`), M5.0-R4 (`d945591`), M4.3d (`40b11fd`), M4.3c (`eab2428`), M4.3b (`f3c29b6`), Approved Architecture (`2bb117c`), Frozen M4.2d (`aafa366`)<br/>
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT<br/>
**Milestone M5.1 Implementation Status:** STRICTLY UNAUTHORIZED AND UNSTARTED

---

## 1. Executive Summary & Review Context

Following the Engineering Authority's independent review of Milestone M5.0-R6 ("Documentation closure evidence: PASS / Technical acceptance: conditional / Milestone M5.1: Not authorized"), this sub-milestone (M5.0-R7) formally resolves the three identified architectural correctness issues across the M5 specification suite:

1. **R6-01 — Capability Export, Minting, and Private Transfer Contract:** Formalized the separation between the module-private branding symbol (`DRAIN_ADMISSION_BRAND: unique symbol`), internal factories with runtime deep freezing (`Object.freeze()`), the private transfer bundle (`AuthorizedDrainCapabilities`) passed via `coordinator.bindEngine()`, the strict absence of public accessors/getters, the restriction of public package exports strictly to TypeScript type declarations, and the clarification of the cooperative in-realm defensive model.
2. **R6-02 — Reset Operations Failure-Atomic Lifecycle Semantics:** Formalized failure-atomic semantics for both administrative reset actions:
   - `PURGE_AND_RETRY`: Enforces monotonic attempt sequence fencing (`const newAttemptId = ++this.currentAttemptId;`) to fence out all stale asynchronous callbacks and timers from prior attempts; synchronously cancels `QuarantineDrainer` in-flight custody ($N_{\text{in-flight}} \le 1$ preserved at all times); and permanently suppresses deferred notifications.
   - `REINITIALIZE_CLEAN`: Enforces the strict 5-step M4.3c completion order: (1) capability-gated atomic baseline install via `replaceCoreStateAtomic`, (2) quarantine/reconciliation cleanup, (3) `setRecoveryMode(false)`, (4) coordinator transition to `ACTIVE`, (5) `flushDeferredNotifications()`. The clean baseline root swap is irrevocable once committed under JavaScript run-to-completion semantics; if any post-installation step fails, the catch block restores `engine.setRecoveryMode(true)`, locks the engine in `DEGRADED`, and suppresses all notifications without rolling back to an unanchored pre-reset root.
3. **R6-03 — Inviolable Distinction: Capability Authority vs. Operation Provenance:** Established that capability possession proves caller authority to request an admission mode, but does NOT independently validate operation payload provenance. Operations admitted under `TrustedLocalReplayCapability` must originate strictly and exclusively from `stashedLocalQueue`, populated solely from previously committed, immutable local operations in `OrderedOperationLog`. Remote operations can never be inserted into `stashedLocalQueue` or relabeled as local. Identity hash verification ($H(op) == H(log)$) and causal context checks against the snapshot base are enforced on replay, immediately aborting to `DEGRADED` on conflict.

Consistent with absolute scope restrictions, **zero lines of production code, test code, build configuration, or package manifests were created or modified**. Milestone M5.1 implementation remains completely unstarted.

---

## 2. Technical Resolution of Review Findings

### 2.1 Resolution of R6-01: Capability Export, Minting & Private Transfer Contract

#### The Review Finding:
The R6 report showed `export const DRAIN_ADMISSION_BRAND: unique symbol = Symbol(...)` while simultaneously describing it as package-private. An exported symbol is not package-private. The specification required distinguishing:
- The internal symbol used to brand capability objects.
- The internal factories that create those objects.
- The composition root that distributes each capability to its authorized owner.
- The public SDK exports, which must expose neither factories nor capability instances.
- The transfer protocol between `LeaderlessSyncEngine`, `RecoveryCoordinator`, and `QuarantineDrainer` without public getters.
- Runtime immutability via `Object.freeze()`.
- Explicit qualification of the in-realm defensive model.

#### Architectural Resolution & Contract:
Following the accepted pattern in `packages/sync-engine/src/recovery/inbound-custody.ts` (where `INBOUND_CAPABILITY_BRAND` is module-internal and never exported in `packages/sync-engine/src/index.ts`):

1. **Module-Private Branding Symbol:**
   ```typescript
   // Declared strictly within packages/sync-engine/src/recovery/drain-admission-custody.ts
   // Module-private: NEVER re-exported from packages/sync-engine/src/index.ts
   const DRAIN_ADMISSION_BRAND: unique symbol = Symbol('DRAIN_ADMISSION_BRAND');
   ```
   Ordinary SDK consumers cannot import or fabricate a branded capability literal with `[DRAIN_ADMISSION_BRAND]: true`.

2. **Internal Capability Types & Distribution Container:**
   ```typescript
   export interface TrustedLocalReplayCapability {
     readonly [DRAIN_ADMISSION_BRAND]: true;
     readonly kind: 'LOCAL_REPLAY';
     readonly sessionId: string;
     readonly authority: string;
     readonly token: string;
   }

   export interface TrustedRepairBatchCapability {
     readonly [DRAIN_ADMISSION_BRAND]: true;
     readonly kind: 'REPAIR_BATCH';
     readonly sessionId: string;
     readonly authority: string;
     readonly token: string;
   }

   export type DrainAdmissionContext =
     | TrustedLocalReplayCapability
     | TrustedRepairBatchCapability;

   export interface AuthorizedDrainCapabilities {
     readonly localReplayCapability: TrustedLocalReplayCapability;
     readonly repairBatchCapability: TrustedRepairBatchCapability;
   }
   ```

3. **Internal Factories & Runtime Deep Freezing:**
   ```typescript
   export function createLocalReplayCapability(
     sessionId: string,
     authority = 'local-reconciliation',
     token?: string,
   ): TrustedLocalReplayCapability {
     return Object.freeze({
       [DRAIN_ADMISSION_BRAND]: true as const,
       kind: 'LOCAL_REPLAY' as const,
       sessionId,
       authority,
       token:
         token ??
         `local-replay-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
     });
   }

   export function createRepairBatchCapability(
     sessionId: string,
     authority = 'quarantine-drainer',
     token?: string,
   ): TrustedRepairBatchCapability {
     return Object.freeze({
       [DRAIN_ADMISSION_BRAND]: true as const,
       kind: 'REPAIR_BATCH' as const,
       sessionId,
       authority,
       token:
         token ??
         `repair-batch-cap-${sessionId}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
     });
   }
   ```

4. **Private Transfer via Composition Root (`bindEngine`):**
   - `LeaderlessSyncEngine` mints its own authorized `localReplayCapability` and `repairBatchCapability` during initialization, bundled into an immutable `AuthorizedDrainCapabilities` container (`Object.freeze(...)`).
   - The capabilities are transferred privately to `RecoveryCoordinator` via:
     ```typescript
     coordinator.bindEngine(
       this,
       this.authorizedRecoveryCapability,
       drainCapabilities,
     );
     ```
   - **Zero Public Accessors:** Neither `LeaderlessSyncEngine` nor `RecoveryCoordinator` nor `QuarantineDrainer` exposes public getters or properties (`engine.localReplayCapability` and `coordinator.repairBatchCapability` do not exist).
   - Inside `RecoveryCoordinator`:
     - Retains `drainCapabilities.localReplayCapability` in a private field for the local stashed reconciliation workflow.
     - Initializes `QuarantineDrainer` passing strictly `drainCapabilities.repairBatchCapability`. `QuarantineDrainer` has zero access to `TrustedLocalReplayCapability`.

5. **Public SDK Export Boundary (`packages/sync-engine/src/index.ts`):**
   - Exports **strictly TypeScript type declarations**:
     `export type { DrainAdmissionContext, TrustedLocalReplayCapability, TrustedRepairBatchCapability, AuthorizedDrainCapabilities } from './recovery/drain-admission-custody.js';`
   - Exports **NEVER** include `DRAIN_ADMISSION_BRAND`, nor the minting factories, nor capability instances.

6. **In-Realm Defensive Model Clarification:**
   In a cooperative JavaScript runtime, unique symbols and opaque objects provide structural encapsulation and defensiveness against API misuse, configuration errors, and duck-typing, but do not construct a cryptographic sandbox against hostile code running in the identical JavaScript realm. Actual protection is achieved through private capability custody, exact object-reference checks (`===`), and fail-closed validation.

---

### 2.2 Resolution of R6-02: Reset Operations Failure-Atomic Lifecycle Semantics

#### The Review Finding:
The reset design lacked explicit failure-atomic lifecycle semantics for partial failure during `PURGE_AND_RETRY` and `REINITIALIZE_CLEAN`:
- `PURGE_AND_RETRY`: Needs attempt ID incrementing and fencing of stale asynchronous work; in-flight drainer continuation cancellation; preservation of single-owner drain invariant ($N_{\text{in-flight}} \le 1$).
- `REINITIALIZE_CLEAN`: Needs explicit preservation of the accepted M4.3c 5-step completion order; failure rollback restoring recovery mode with zero notification emission; clean baseline swap irrevocability once committed.

#### Architectural Resolution & Contract:

#### A. `PURGE_AND_RETRY` Failure-Atomic Lifecycle & Attempt Fencing Semantics:
1. **Monotonic Attempt Sequence Fencing:**
   - Before executing any purge or retry logic, the coordinator increments its attempt sequence counter:
     ```typescript
     const newAttemptId = ++this.currentAttemptId;
     ```
   - All asynchronous recovery workflows (chunk arrivals, transfer callbacks, timeout timers, discovery responses) check `attemptId === this.currentAttemptId`. Any frame or timer belonging to an earlier attempt is dropped immediately as a stale no-op.
2. **Synchronous In-Flight Drain Cancellation ($N_{\text{in-flight}} \le 1$ Preservation):**
   - `QuarantineDrainer.reset()` is invoked synchronously.
   - It sets `inFlightRecord = null`, resets `haltedOnCapacity = false`, and purges both active and detached quarantine generations.
   - Any in-flight drain continuation from the previous attempt that resolves or rejects after this point detects that its in-flight record has been cleared and aborts without releasing or modifying ownership belonging to `newAttemptId`.
   - The accepted M4.3c single-owner drain invariant ($N_{\text{in-flight}} \le 1$) is strictly preserved at all times (immediately becomes 0).
3. **Queue & Buffer Discard:**
   - Discards all un-drained quarantined operations from `QuarantineBuffer` and resets `FutureEpochBuffer`.
   - Purges `stashedLocalQueue` and any incomplete reconciliation state.
4. **Permanent Notification Suppression:**
   - `deferredStateChanges` and `deferredDecisions` are permanently dropped and purged; they are **never flushed to listeners**.
5. **State Transition & Mode Maintenance:**
   - Resets the recovery coordinator state machine to `RECOVERING` under `newAttemptId`, while locking engine recovery mode (`engine.setRecoveryMode(true)`).
   - Initiates a fresh, full recovery discovery and transfer cycle.
   - The engine does NOT return to `ACTIVE` until this new attempt completes successfully. If it fails, it aborts to `DEGRADED`.

#### B. `REINITIALIZE_CLEAN` Exact 5-Step Order & Irrevocable Root Swap:
1. **Strict 5-Step M4.3c Completion Order:**
   - **Step 1: Capability-Gated Baseline Installation:** Atomically replace `EngineCoreState` with the clean baseline anchor via `engine.replaceCoreStateAtomic(cleanBaseline, capability)`.
   - **Step 2: Quarantine & Reconciliation Cleanup:** Purge all quarantine buffers (`QuarantineBuffer`, `FutureEpochBuffer`), reset `QuarantineDrainer` ($N_{\text{in-flight}} = 0$), clear `stashedLocalQueue`, and discard all deferred notification queues.
   - **Step 3: Disengage Engine Recovery Mode:** Clear internal engine recovery mode via `engine.setRecoveryMode(false)`. Operational notification deferral is now unlocked.
   - **Step 4: Transition Coordinator to ACTIVE:** Advance authoritative lifecycle state via `stateMachine.transition('ACTIVE', ...)`.
   - **Step 5: Flush Notifications:** Only after Step 4 completes, permit deferred notifications to be flushed via `engine.flushDeferredNotifications()`.
2. **Failure Atomicity & Irrevocable Root Swap:**
   - The clean baseline root replacement in Step 1 executes under JavaScript run-to-completion semantics (`this.core = candidateCore;`). Once executed, this reference swap is **irrevocable**: the engine cannot and must not roll back to the previous unanchored or corrupted root.
   - If ANY post-installation step (Step 2, 3, 4, or 5) fails or throws an exception:
     - The exception is caught.
     - `engine.setRecoveryMode(true)` is immediately restored in the catch block (notification deferral locked).
     - Authoritative lifecycle state remains in or transitions to `DEGRADED`.
     - Deferred notifications are **NEVER flushed**.
     - The engine remains anchored on the clean baseline in fail-closed `DEGRADED` state (`submit` throws `REJECTED_DEGRADED`), preventing stale or inconsistent event broadcast.

---

### 2.3 Resolution of R6-03: Distinction Between Capability Authority and Operation Provenance

#### The Review Finding:
The capability establishes that the caller is authorized to request a particular admission mode (`LOCAL_REPLAY` vs `REPAIR_BATCH`). It does not independently prove that the supplied operation actually came from the corresponding queue.

#### Architectural Resolution & Contract:
1. **Inviolable Distinction: Authority vs. Provenance:**
   - **Capability Authority:** Possessing `TrustedLocalReplayCapability` grants authority to invoke `admitDrainedOperation(op, capability)` in local replay mode (bypassing token-bucket rate limits and remote clock inflation checks).
   - **Operation Provenance:** Capability possession alone does NOT validate payload provenance. Operation data must originate from strictly isolated, verified queues.
2. **`stashedLocalQueue` Provenance Exclusivity:**
   - Operations admitted under `TrustedLocalReplayCapability` must originate **strictly and exclusively** from the private `stashedLocalQueue`.
   - `stashedLocalQueue` is populated strictly by extracting previously committed, immutable local operations from `OrderedOperationLog` where `originPeerId === this.localPeerId` and `originSequenceNumber > baseSnapshotFrontier[this.localPeerId]`.
3. **Prohibition of Remote Injection or Relabeling:**
   - Remote operations can **NEVER** be inserted into `stashedLocalQueue`.
   - Remote operations can **NEVER** be relabeled or masqueraded as local operations through metadata, headers, or property mutations.
   - The population of `stashedLocalQueue` is an internal engine operation reading solely from frozen, verified local log entries.
4. **Identity Hash & Causal Context Verification:**
   - Even when presented with `TrustedLocalReplayCapability`, each replayed operation is verified against committed history:
     - Identity hash verification: $H(op) == H(log)$. The operation's `logicalEventId`, `originPeerId`, `originSequenceNumber`, `logicalTimestamp`, `causalContext`, and `payload` must match the committed log record.
     - Causal context check: Causal dependencies must be satisfied by the snapshot base checkpoints or preceding replayed operations.
     - Any identity conflict or unsatisfied dependency proves history divergence; recovery **immediately aborts to `DEGRADED`**. Local operations are never dropped.
5. **`REPAIR_BATCH` Operations Pipeline:**
   - Operations presented with `TrustedRepairBatchCapability` originate strictly from `QuarantineBuffer` (remote network arrivals).
   - They undergo complete structural validation, epoch fencing, Lamport monotonicity checks, dependency resolution across snapshot checkpoints, and duplicate detection.

---

## 3. Specification & Document Traceability Matrix

| Review Finding | Primary Document | Affected Section | Formalized Architectural Contract |
|---|---|---|---|
| **R6-01 / R7-01** (Capability Export, Minting & Custody) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | §2.1.2<br/>§2.1<br/>§2.2 (M5.1) | `DRAIN_ADMISSION_BRAND: unique symbol` is module-private to `drain-admission-custody.ts` and non-exported in public entry point. Capability factories apply `Object.freeze()`. Transferred privately via `AuthorizedDrainCapabilities` during `bindEngine()`. Zero public getters. Public SDK exports strictly TypeScript types. In-realm defensive trust model explicit. |
| **R6-02 / R7-02** (Reset Failure-Atomic Lifecycle Semantics) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §3.4<br/>§3.1<br/>AC-M5-01<br/>R-M5-08, R-M5-10 | `PURGE_AND_RETRY` increments `currentAttemptId`, fences stale async callbacks/timers, synchronously cancels in-flight drainer custody ($N_{\text{in-flight}} \le 1$), and suppresses notifications.<br/>`REINITIALIZE_CLEAN` executes strict 5-step M4.3c sequence; clean baseline swap is irrevocable; failure restores `setRecoveryMode(true)`, locks engine in `DEGRADED`, and suppresses notifications without rolling back. |
| **R6-03 / R7-03** (Authority vs. Provenance Distinction) | `docs/PHASE7_5_M5_ARCHITECTURE.md`<br/>`docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`<br/>`docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`<br/>`docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`<br/>`docs/PHASE7_5_M5_RISK_REGISTER.md` | §5.2<br/>§2.2<br/>§2.5 (M5.4)<br/>AC-M5-27<br/>R-M5-09 | Capability establishes admission mode authority but does not validate payload provenance. `LOCAL_REPLAY` operations originate exclusively from `stashedLocalQueue` (immutable local log entries; remote operations prohibited from entering or being relabeled). Identity hash $H(op) == H(log)$ and causal dependencies verified, aborting to `DEGRADED` on conflict. |

---

## 4. Verification & Source Baseline Audit

All specifications have been verified against the accepted codebase baselines:
- **`packages/sync-engine/src/recovery/inbound-custody.ts`:** Confirms unforgeable capability pattern using `unique symbol`, `Object.freeze()`, package-private symbol non-export in `index.ts`, and exact reference identity checking.
- **`packages/sync-engine/src/recovery/recovery-coordinator.ts`:** Confirms attempt sequence counter `currentAttemptId`, timer inventory categories, and single-attachment `bindEngine` semantics.
- **`packages/sync-engine/src/engine/leaderless-sync-engine.ts`:** Confirms `replaceCoreStateAtomic` synchronous single-pointer replacement under run-to-completion semantics, `admitDrainedOperation` entry point, and notification deferral buffering.
- **`packages/sync-engine/src/recovery/quarantine-drainer.ts`:** Confirms $N_{\text{in-flight}} \le 1$ single-owner drain invariant and synchronous `reset()` invalidation.

---

## 5. Scope Restrictions Compliance

- **Production Code Changes:** 0 files, 0 lines.
- **Test Code Changes:** 0 files, 0 lines.
- **Build / Manifest Changes:** 0 files, 0 lines.
- **Documentation Changes:** Strictly M5 specification documentation under `docs/PHASE7_5_M5_*.md`.
- **M5.1 Implementation Status:** UNAUTHORIZED AND UNSTARTED.

---

## 6. Conclusion & Recommendation

With the formalization of:
1. Exact capability export, minting, private custody transfer, and non-exported branding symbol,
2. Failure-atomic reset lifecycle semantics for `PURGE_AND_RETRY` and `REINITIALIZE_CLEAN`,
3. The inviolable distinction between capability authority and operation provenance,

Milestone M5.0-R7 successfully resolves all outstanding findings from the M5.0-R6 review. The architecture, interface specification, roadmap, acceptance criteria, and risk register are completely reconciled, consistent, and ready for unconditional formal acceptance.
