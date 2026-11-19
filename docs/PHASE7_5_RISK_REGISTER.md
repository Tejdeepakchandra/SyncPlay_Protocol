# Phase 7.5 — Fault Tolerance & State Recovery: Risk Register

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `30b8e5f`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Risk Register Summary Matrix

| Risk ID | Description | Pre-Mitigation Risk | Post-Mitigation Risk | Primary Mitigation |
| :--- | :--- | :---: | :---: | :--- |
| **RSK-7.5-01** | Memory exhaustion via unbounded chunk reassembly | **HIGH** | **LOW** | 4 MB total cap, 1 active transfer, chunk size bounds |
| **RSK-7.5-02** | Split-brain divergence from incomparable snapshots | **CRITICAL** | **LOW** | Strict vector domination + prefix checkpoint check; reject tie-break; enter `DEGRADED` |
| **RSK-7.5-03** | Quarantine buffer overflow under high traffic churn | **HIGH** | **LOW** | 1,000 operation ring buffer, fail-safe synchronous abort to `DEGRADED` |
| **RSK-7.5-04** | Mid-transfer connection drop or epoch increment | **HIGH** | **LOW** | Synchronous abort, timer cleanup, generation binding |
| **RSK-7.5-05** | Lamport clock inflation attack or clock regression | **HIGH** | **LOW** | Exact $\max(L_{\text{local}}, L_{\text{snap}})$ reseed without $+1$, exact Phase 7.2 invariant |
| **RSK-7.5-06** | Checkpoint table bloat from ephemeral peers | **MEDIUM** | **LOW** | Fixed cap (256 checkpoints), pruning retired origins |
| **RSK-7.5-07** | Re-entrant event execution during atomic install | **CRITICAL** | **LOW** | Staged immutable instance replacement, deferred listener notification |
| **RSK-7.5-08** | SCTP transport fragmentation / 64 KB overflow | **HIGH** | **LOW** | Normative 7-step iterative UTF-8 send boundary loop |
| **RSK-7.5-09** | Concealed intermediate history divergence | **HIGH** | **LOW** | Full prefix checkpoint verification across $[1, k]$; fail to `DEGRADED` if gaps exist |

---

## 2. Detailed Risk Analysis and Mitigation Strategies

### RSK-7.5-01: Memory Exhaustion via Unbounded Chunk Reassembly
- **Description:** Malfunctioning or malicious peers flood chunk buffers or open multiple simultaneous transfer sessions.
- **Pre-Mitigation Impact:** Browser process crash / Out-of-Memory (OOM).
- **Mitigation Strategy:**
  1. Single active transfer rule: At most ONE active transfer per recovering peer (`activeTransferId !== null`).
  2. Bounded total snapshot size: `MAX_SNAPSHOT_BYTES = 4194304` (4 MB).
  3. Bounded chunk count: `MAX_CHUNKS_PER_SNAPSHOT = 128`.
  4. Memory pre-check: Snapshot requests exceeding 4 MB are rejected before allocating buffer memory.
- **Verification Plan:** Fuzzing tests attempting $>4\text{ MB}$ transfers and asserting synchronous rejection.

---

### RSK-7.5-02: Split-Brain Divergence from Incomparable Snapshots
- **Description:** Partitions heal with mutually divergent frontiers ($V_A > V_B$ on origin 1, but $V_B > V_A$ on origin 2). Arbitrary tie-breaking (e.g. lowest peerId) would destroy legitimate playback state.
- **Pre-Mitigation Impact:** Irrecoverable state divergence across peers; broken causality.
- **Mitigation Strategy:**
  1. Pure vector domination check: $S$ is eligible if and only if $V_S[p] \ge V_L[p]$ for all known origins.
  2. Strict prohibition of arbitrary tie-breaking: Incomparable snapshots are NEVER resolved by arbitrary heuristics.
  3. Explicit degraded state: Transitions to `DEGRADED` and dispatches `onRecoveryDegraded`.
- **Verification Plan:** Unit test feeding incomparable snapshot offers to `RecoveryCoordinator` and asserting transition to `DEGRADED` with zero state mutation.

---

### RSK-7.5-03: Quarantine Buffer Overflow Under High Traffic Churn
- **Description:** Peers broadcast hundreds of operations while a slow chunk transfer is underway, overflowing memory.
- **Pre-Mitigation Impact:** Unbounded memory growth, UI thread freeze.
- **Mitigation Strategy:**
  1. Hard capacity limit: `maxQuarantinedOperations = 1000`.
  2. Synchronous overflow abort: If quarantined operations exceed 1,000, transfer is aborted synchronously with reason `QUARANTINE_OVERFLOW`.
  3. Safe fallback: Engine transitions to `DEGRADED`, protecting system responsiveness.
- **Verification Plan:** Stress test buffering 1,001 operations during transfer and asserting synchronous abort and graceful degradation.

---

### RSK-7.5-04: Mid-Transfer Connection Drop or Epoch Increment
- **Description:** While a multi-chunk transfer is in flight, the supplier disconnects or the membership epoch changes.
- **Pre-Mitigation Impact:** Hanging transfers, orphan timers, or installing a snapshot from an obsolete epoch.
- **Mitigation Strategy:**
  1. Epoch and generation binding on all recovery payloads.
  2. Transport disconnect hooks trigger synchronous `abortActiveTransfer()`.
  3. Synchronous cleanup: Chunk buffers and timers are purged immediately.
- **Verification Plan:** Simulated disconnect test cutting the DataChannel during chunk 3 of 8 and verifying zero lingering timers.

---

### RSK-7.5-05: Lamport Clock Inflation Attack or Clock Regression
- **Description:** Corrupt snapshot carries an inflated `maxLogicalTimestamp` or lower timestamp.
- **Pre-Mitigation Impact:** Causal inversion or clock overflow.
- **Mitigation Strategy:**
  1. Exact Phase 7.2 invariant: $S.\text{maxLogicalTimestamp} \equiv \max_{p} S.\text{originFrontierTimestamps}[p] \le \text{Number.MAX\_SAFE\_INTEGER}$.
  2. Exact reseed rule: $L_{\text{local}} = \max(L_{\text{local}}, T_{\text{snap}})$ strictly without adding $+1$.
- **Verification Plan:** Unit test verifying clock monotonicity and exact value matching across varied clock configurations.

---

### RSK-7.5-06: Checkpoint Table Bloat from Ephemeral Peers
- **Description:** Long sessions with high turnover accumulate dead origin checkpoints.
- **Pre-Mitigation Impact:** Snapshot size bloat.
- **Mitigation Strategy:**
  1. Checkpoint cap: Hard-capped at 256 entries in `StateSnapshotPayloadV2Schema`.
  2. Pruning inactive origins whose sequences have been subsumed by all active members.
- **Verification Plan:** Test verifying checkpoint table size enforcement and rejection of snapshots with $>256$ checkpoints.

---

### RSK-7.5-07: Re-entrant Event Execution During Atomic Install
- **Description:** Microtasks or event callbacks trigger local inputs or remote event processing while snapshot install is incomplete.
- **Pre-Mitigation Impact:** Inconsistent state, corrupted replication vectors.
- **Mitigation Strategy:**
  1. Staged Immutable Instance Replacement: Staged `OrderedOperationLog`, state, and replication vectors are built out-of-band.
  2. Atomic reference swap: Synchronous property swap executes in a single run-to-completion step.
  3. Deferred listener notification: Listeners are invoked ONLY AFTER reconciliation finishes.
- **Verification Plan:** Test attempting to invoke `engine.executeLocalOperation()` while `installSnapshotAtomic()` is running and verifying strict rejection.

---

### RSK-7.5-08: SCTP Transport Fragmentation / 64 KB Overflow
- **Description:** A chunk exceeds the negotiated SCTP limit on constrained WebRTC links, causing silent packet drops.
- **Pre-Mitigation Impact:** Silent transfer hang or DataChannel crash.
- **Mitigation Strategy:**
  1. Dynamic chunk sizing heuristic: Payload size estimate via `calculateDynamicChunkSize(sctpMaxMessageSize)`.
  2. Normative 7-step iterative send-boundary loop: Formats, serializes to `wireJson`, measures exact UTF-8 bytes, and reduces chunk iteratively before calling `RTCDataChannel.send()`.
  3. Minimum MTU enforcement: Aborts if binary chunk size drops below 256 bytes without fitting.
- **Verification Plan:** Direct serialization measurement test asserting every serialized chunk message is $\le \text{effectiveLimit}$ on the wire.

---

### RSK-7.5-09: Concealed Intermediate History Divergence
- **Description:** Because operation fingerprints commit only to individual operations and not a hash chain, equality at sequence $k = \min(V_L[p], V_S[p])$ can conceal conflicting operations authored at sequence $i < k$.
- **Pre-Mitigation Impact:** Silently adopting an incompatible fork with divergent historical state.
- **Mitigation Strategy:**
  1. Full prefix verification: Check all common retained sequence checkpoints across $[1, k]$.
  2. Missing checkpoint rule: If any intermediate sequence has missing checkpoints on either side, classify as `REQUIRES_HISTORY_REPAIR`.
  3. Fail-safe degradation: If missing checkpoints cannot be retrieved via anti-entropy, transition to `DEGRADED`.
- **Verification Plan:** Counterexample test with identical fingerprints at sequence 3 but divergent operation at sequence 1, asserting rejection.

---

## 3. Open Decisions Register

| ID | 1. Question | 2. Why It Matters | 3. Available Alternatives | 4. Recommended Choice | 5. Can Proceed to M3? |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **OD-7.5-1** | How should the 1,000-operation quarantine buffer handle overflow during transfer? | High-traffic churn during long transfers could cause unbounded memory bloat if unconstrained. | **Alt A:** Drop older idempotent events.<br/>**Alt B:** Synchronously abort transfer to `DEGRADED`. | **Alt B (Abort to DEGRADED):** Preserves strict causal correctness; prevents partial state corruption. | **YES** (Decided: Alt B) |
| **OD-7.5-2** | How should `SNAPSHOT_OFFER` messages be advertised? | Flooding offers on every timer tick wastes WebRTC bandwidth on mobile/constrained clients. | **Alt A:** Periodic broadcast.<br/>**Alt B:** Request-driven / targeted responses to `onHistoryExhausted`. | **Alt B (Request-driven):** Eliminates unneeded signaling overhead; offers are published only when missing history is declared. | **YES** (Decided: Alt B) |
| **OD-7.5-3** | How should Explicit Manual Reset be exposed to applications when in `DEGRADED` state? | When two partitions have incomparable histories, users need a way to force convergence without causing silent causal overwrites. | **Alt A:** Automatic tie-breaking (e.g. lowest peerId).<br/>**Alt B:** Provide explicit `resetSession({ newSessionId })` API with UI prompt.<br/>**Alt C:** Defer Manual Reset to Phase 8. | **Alt C (Defer to Phase 8):** Phase 7.5 provides the mathematical safety, fencing, and `DEGRADED` state. Application-level session recreation is a Phase 8 SDK concern. | **YES** (Decided: Alt C) |
| **OD-7.5-4** | What is the minimum supported SCTP max message size? | If a WebRTC link negotiates an extremely small MTU ($< 2\text{ KB}$), chunk framing overhead exceeds data payload. | **Alt A:** Reject connections with SCTP limit $< 64\text{ KB}$.<br/>**Alt B:** Support down to 2,048 bytes; abort if $< 2048$ bytes. | **Alt B:** Supports standard constrained networks (e.g. 16 KB) while protecting against pathologically tiny MTUs. | **YES** (Decided: Alt B) |
| **OD-7.5-5** | How should unverified intermediate sequence gaps below compaction horizon be resolved? | Since checkpoints do not chain previous operations, missing checkpoints mean history cannot be verified. | **Alt A:** Assume equality if vector dominates.<br/>**Alt B:** Declare `REQUIRES_HISTORY_REPAIR` and fail to `DEGRADED` if unrepairable. | **Alt B (Require repair / fail to DEGRADED):** Prevents silent branch overwriting when intermediate divergence exists. | **YES** (Decided: Alt B) |
