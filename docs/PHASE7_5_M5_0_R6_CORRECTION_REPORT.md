# Phase 7.5 Milestone M5.0-R6 — Final Trust Context Capability Closure Report

**Milestone:** Phase 7.5 Sub-Milestone M5.0-R6 (Trust Context Capability Closure & Architecture Consistency)<br/>
**Role:** Senior TypeScript Distributed-Systems Architect<br/>
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT<br/>
**Date:** 2026-11-25<br/>
**Starting Documentation Baseline (Parent):** `0e15f311ff462698b68fc6078adeede08621c350`<br/>
**Accepted Implementation Baseline:** M4.3d `40b11fdafcd247c7a289723f90ec0e2029c94f64`<br/>
**Accepted M4.3c Baseline:** `eab2428681eb8a05b1c6e71906a932f671730fc8`<br/>
**Accepted M4.3b Baseline:** `f3c29b62eea63f6b06647218e4485eacba86fb50`<br/>
**Frozen M4.2d Baseline:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e`<br/>
**Objective:** Resolve the final trust-boundary defect identified in the Engineering Authority's review on Milestone M5.0-R5 by formalizing an unforgeable capability-gated architecture for post-snapshot replay trust provenance (`DrainAdmissionContext`), formalizing administrative `resetRecovery()` guardrails, and preserving canonical ordering feasibility qualifications. Implementation of Milestone M5.1 remains strictly **UNAUTHORIZED AND UNSTARTED**.

---

## 1. Executive Summary & Verification Scope

In Milestone M5.0-R5, the compacted-snapshot ordering hazard ($A \prec B$ hazard) was resolved through a canonical ordering feasibility check and explicit incompatibility gate, and public recovery lifecycle state queries were disambiguated.

However, the Engineering Authority's review placed technical acceptance on HOLD, identifying one critical trust-boundary defect:
- **Trust Context Provenance:** A caller-supplied string (`'LOCAL_REPLAY' | 'REPAIR_BATCH'`) is merely a declaration of context, not proof of provenance. If `engine.admitDrainedOperation()` accepts a string, any caller capable of invoking the method could supply `'LOCAL_REPLAY'`, allowing untrusted remote operations to bypass arrival-rate and clock-inflation protections intended exclusively for previously committed local operations.

Milestone M5.0-R6 eliminates this vulnerability by establishing a capability-gated trust provenance architecture following the accepted M4.3b/M4.3c capability patterns (`RecoveryInstallationCapability`, `InboundTransportCapability`).

---

## 2. Technical Resolution of Review Findings

### 2.1 Capability-Gated Trust Context Provenance (Closing R6-01)

#### 1. Why Caller-Supplied Strings Failed
In M5.0-R5, the proposed method was:
```typescript
admitDrainedOperation(
  op: QuarantinedOperation,
  trustContext: 'LOCAL_REPLAY' | 'REPAIR_BATCH' = 'REPAIR_BATCH',
): DestinationAcceptanceReceipt;
```
As correctly identified by the Engineering Authority:
- A typed string is an assertion of caller intent, not verifiable proof of custody.
- If an external caller or remote transport handler invokes `engine.admitDrainedOperation(remoteOp, 'LOCAL_REPLAY')`, the engine would accept the string and evaluate the untrusted remote operation under `LOCAL_REPLAY` semantics, bypassing token-bucket rate limits and clock inflation validation.
- Even if documented as "internal," in a TypeScript SDK where methods may be accessible across module boundaries, typed strings provide zero structural defense against accidental trust escalation.

#### 2. Unforgeable Capability Architecture
Following the accepted pattern in `packages/sync-engine/src/recovery/inbound-custody.ts`, Milestone M5 formalizes `DrainAdmissionContext`:

```typescript
export const DRAIN_ADMISSION_BRAND: unique symbol = Symbol('DRAIN_ADMISSION_BRAND');

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
```

#### 3. Method Contract
```typescript
admitDrainedOperation(
  op: QuarantinedOperation,
  capability: DrainAdmissionContext,
): DestinationAcceptanceReceipt;
```

#### 4. Seven Non-Negotiable Capability Properties (Closing Review Finding 2)
1. **Unforgeable in Normal API:** `DRAIN_ADMISSION_BRAND` is a package-private unique symbol. Ordinary SDK consumers cannot fabricate a valid capability object literal with `[DRAIN_ADMISSION_BRAND]: true`.
2. **Created & Retained by Composition Root / Engine:** `LeaderlessSyncEngine` creates and retains private references to both `authorizedLocalReplayCapability` and `authorizedRepairBatchCapability` upon construction.
3. **Exact Reference Identity Check:** `admitDrainedOperation(op, capability)` checks exact object-reference identity (`===`):
   - If `capability.kind === 'LOCAL_REPLAY'`: requires `capability === this.authorizedLocalReplayCapability`.
   - If `capability.kind === 'REPAIR_BATCH'`: requires `capability === this.authorizedRepairBatchCapability`.
   - Mismatched, forged, or unbranded capabilities throw `SyncEngineError('UNAUTHORIZED')`.
4. **Physical Custody Segregation:**
   - `QuarantineDrainer` is initialized strictly with `TrustedRepairBatchCapability`. It has **zero reference** to `TrustedLocalReplayCapability`. Remote quarantine draining can NEVER obtain or present local replay capability.
   - The local stashed reconciliation workflow is owned privately by `RecoveryCoordinator` and provided with `TrustedLocalReplayCapability`.
5. **No Wire or Payload Inference:** The operation's wire payload, headers, and mutable fields can never influence the selected trust context. Trust is determined solely by the capability presented.
6. **Fail-Closed on Invalid/Missing Capabilities:** Any invocation with a missing, null, undefined, forged, or mismatched capability throws `SyncEngineError('UNAUTHORIZED')` immediately.
7. **Preservation of Accepted Drain Invariants:** M4.3b capability custody, M4.3c single-owner drain ($N_{\text{in-flight}} \le 1$), and M4.3d production orchestration contracts remain 100% intact.

---

### 2.2 Administrative Recovery Reset Semantics & Guardrails (Closing R6-02)

To prevent `resetRecovery()` from being used as an escape hatch that clears degradation while exposing an incomplete or corrupted state root as healthy:

#### 1. Method Signature & Strict State Guard
```typescript
resetRecovery(
  capability: RecoveryInstallationCapability,
  resetAction: 'PURGE_AND_RETRY' | 'REINITIALIZE_CLEAN',
): void;
```
- Allowed **strictly from the `DEGRADED` state** (`this.stateMachine.state === 'DEGRADED'`). Invocations from `ACTIVE`, `RECOVERING`, `RECONCILING`, or `DISPOSED` throw `SyncEngineError('INVALID_STATE')`.
- Requires authorized `RecoveryInstallationCapability` checked via strict object-reference identity (`capability === this.authorizedRecoveryCapability`). Unauthorized invocations throw `SyncEngineError('UNAUTHORIZED')`.

#### 2. Safe Reset Actions
1. **`PURGE_AND_RETRY` (Autonomous Clean Retry):**
   - Discards all un-drained quarantined operations from `QuarantineBuffer` and resets `FutureEpochBuffer`.
   - Purges `stashedLocalQueue` and any incomplete reconciliation state.
   - Permanently discards `deferredStateChanges` and `deferredDecisions` (they are **never flushed to listeners**).
   - Resets coordinator state machine to `RECOVERING` (attempt ID incremented), keeping engine recovery mode locked (`engine.setRecoveryMode(true)`).
   - Initiates a fresh, full recovery discovery and transfer cycle. Does NOT transition to `ACTIVE`.
2. **`REINITIALIZE_CLEAN` (Administrative Baseline Restoration):**
   - Used when application session restarts from a known clean genesis state or agreed anchor snapshot rather than continuing peer recovery.
   - Atomically replaces `EngineCoreState` with the clean baseline anchor via capability-gated replacement.
   - Purges all quarantine buffers, drain records, and deferred notifications.
   - Clears engine recovery mode (`engine.setRecoveryMode(false)`) and transitions coordinator to `ACTIVE` **only after** the clean verified baseline is installed.

---

### 2.3 Preserved Canonical Ordering Feasibility Qualifications (Closing Review Finding 1)

Milestone M5 preserves the exact mathematical formulations and guarantees requested by the Engineering Authority:

1. **Conservative Sufficient Condition:**
   $$L(A) > \text{snapshot.maxLogicalTimestamp}$$
   provided `snapshot.maxLogicalTimestamp` genuinely represents the maximum Lamport timestamp across the entire compacted history.
2. **Per-Origin Frontier Condition:**
   $$\forall p \in \text{compacted origins},\ L(A) > \text{snapshot.originFrontierTimestamps}[p]$$
   with the non-negotiable guarantees that:
   - Every compacted origin is explicitly represented in `originFrontierTimestamps`.
   - Each origin's frontier timestamp is the exact Lamport timestamp of its highest compacted sequence (not an approximation or an unrelated later checkpoint).
3. **Critical Qualification on Causal Dependencies:**
   The canonical ordering feasibility check proves that $A$ sorts strictly after all compacted operations in the canonical total order. It does **not** independently prove that all causal dependencies of $A$ are satisfied. Causal dependencies must still be validated by the engine's causal admission pipeline upon replay.

---

## 3. Summary of Document Amendments

| Document | Key Corrections in Milestone M5.0-R6 |
|---|---|
| `docs/PHASE7_5_M5_ARCHITECTURE.md` | • Status amended to M5.0-R6.<br/>• Section 2.1: added Section 2.1.2 detailing capability-gated drain admission context (`DrainAdmissionContext`) and the 7 non-negotiable capability properties.<br/>• Section 3: added Section 3.4 formalizing administrative `resetRecovery()` capability authorization and safe reset semantics (`PURGE_AND_RETRY` vs `REINITIALIZE_CLEAN`).<br/>• Section 5.2: replaced string-based trust context with capability-gated architecture; re-affirmed exact canonical ordering feasibility conditions and causal dependency qualification. |
| `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md` | • Status amended to M5.0-R6.<br/>• Section 2.1: added `DRAIN_ADMISSION_BRAND`, `TrustedLocalReplayCapability`, `TrustedRepairBatchCapability`, `DrainAdmissionContext`, and internal factory functions.<br/>• Section 2.2: updated `admitDrainedOperation(op, capability: DrainAdmissionContext)` signature with capability documentation.<br/>• Section 3.1: updated `resetRecovery(capability, resetAction)` signature.<br/>• Section 7: updated `UNAUTHORIZED` and added `INVALID_STATE` error codes. |
| `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md` | • Status amended to M5.0-R6.<br/>• Section 2: updated M5.0, M5.1, and M5.4 with capability-gated trust context provenance, custody segregation (`QuarantineDrainer` holding only `TrustedRepairBatchCapability`), and authorized `resetRecovery()` semantics. |
| `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md` | • Status amended to M5.0-R6.<br/>• Section 2: updated `AC-M5-01` to require capability authorization and safe reset semantics for `resetRecovery()`.<br/>• Section 2: updated `AC-M5-20b` with exact `maxLogicalTimestamp`, `originFrontierTimestamps`, and causal validation qualifications.<br/>• Section 2: updated `AC-M5-27` to enforce capability-gated trust context provenance via strict reference identity. |
| `docs/PHASE7_5_M5_RISK_REGISTER.md` | • Status amended to M5.0-R6.<br/>• Section 3: expanded `R-M5-09` to detail Capability-Gated Trust Provenance eliminating caller-supplied string vulnerability.<br/>• Section 3: expanded `R-M5-10` to cover administrative `resetRecovery()` guardrails. |

---

## 4. Scope & Invariant Conformance

- **Production Code Modified:** 0 files (0 lines changed).
- **Test Code Modified:** 0 files (0 lines changed).
- **Package Manifests / Lockfiles Modified:** 0 files.
- **TypeScript Configuration Modified:** 0 files.
- **Frozen Baselines Intact:** Phase 1–7.4 and M4 baselines completely untouched.
- **Milestone M5.1 Status:** UNAUTHORIZED AND UNSTARTED.
