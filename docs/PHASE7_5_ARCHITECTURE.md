# Phase 7.5 — Fault Tolerance & State Recovery: Architecture Specification

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL SAFETY CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `6fddcf9` (Phase 7.5 M3 Implemented & Frozen)  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`), Phase 7.5 M3 (`6fddcf9`)  
**Detailed M4 Milestone Specification:** See [PHASE7_5_M4_ARCHITECTURE.md](./PHASE7_5_M4_ARCHITECTURE.md)  

---

## 1. System Context Diagram

```
+----------------------------------------------------------------------------------------------------+
|                                    SYNCPLAY ARCHITECTURE CONTEXT                                   |
|                                                                                                    |
|  +----------------------------------------------------------------------------------------------+  |
|  |                                  APPLICATION / SDK LAYER                                     |  |
|  |  PlaybackControls, HTML5 Media Adapter, UI State Listeners, EngineHealthCallbacks             |  |
|  +-----------------------------------------------^----------------------------------------------+  |
|                                                  | State / Health Notifications (Deferred post-swap)
|  +-----------------------------------------------v----------------------------------------------+  |
|  |                                     SYNC ENGINE LAYER                                        |  |
|  |                                                                                              |  |
|  |  +------------------------------------+      +--------------------------------------------+  |  |
|  |  |   RecoveryCoordinator (Phase 7.5)   |      |        AntiEntropyManager (Phase 7.4)      |  |  |
|  |  |  - RecoveryStateMachine            |      |  - RepairRequestPipeline (Requester M3)    |  |  |
|  |  |  - SnapshotTransferManager         |<---->|  - RepairSupplierService (Supplier M4)     |  |  |
|  |  |  - SnapshotInstaller (Staged Swap) |      |  - ReplicationComparator                   |  |  |
|  |  |  - QuarantineBuffer (Cap: 1000)    |      +----------------------^---------------------+  |  |
|  |  +------------------^-----------------+                             |                         |  |
|  |                     | Staged Instance Replacement                   |                         |  |
|  |  +------------------v-----------------------------------------------v---------------------+  |  |
|  |  |                           LeaderlessSyncEngine (Phase 7.3 Core)                        |  |  |
|  |  |  - OrderedOperationLog (Compacted checkpoints, history pruning, base snapshot)         |  |  |
|  |  |  - LogicalClockManager (Lamport clock tracking, exact causal advancement, reseed)        |  |  |
|  |  |  - Causal frontier vectors (replicationVector, snapshotFrontier, localCausalContext)   |  |  |
|  |  +------------------------------------------^---------------------------------------------+  |  |
|  +---------------------------------------------|------------------------------------------------+  |
|                                                | Trusted Capability Registration                   |
|  +---------------------------------------------v------------------------------------------------+  |
|  |                            TRUSTED TRANSPORT ADAPTER (Phase 7.4 M5)                          |  |
|  |  - Capability-gated inbound context creation (AuthenticatedSupplierContext / RequesterContext) |
|  |  - Iterative UTF-8 wire size measurement & send boundary (min(64KB, sctpLimit))              |  |
|  |  - Epoch & generation currency validation immediately before serialization                   |  |
|  +---------------------------------------------^------------------------------------------------+  |
|                                                | Unchanged Serialized Wire Bytes                   |
|  +---------------------------------------------v------------------------------------------------+  |
|  |                                   PEER MESH LAYER (WebRTC)                                   |  |
|  |  PeerManager, WebRTCTransport, DataChannels, ConnectionGeneration, RTTEngine                 |  |
|  +----------------------------------------------------------------------------------------------+  |
+----------------------------------------------------------------------------------------------------+
```

---

## 2. Core Architectural Principles & Invariants

1. **Mandatory Local Peer Inclusion in $V_{\text{epoch\_safe}}$:**
   The local engine's own replication vector $V_{\text{local}}$ is an explicit member of every compaction frontier calculation:
   $$V_{\text{epoch\_safe}}[p] = \min \Big( V_{\text{local}}[p], \min_{(r, gen) \in \mathcal{P}} D_r.\text{replicationVector}[p] \Big)$$
   A local engine never compacts beyond its own applied sequence frontier. If remote membership is empty, $V_{\text{epoch\_safe}} = V_{\text{local}}$.
2. **Historical Compatibility via Prefix Verification (Four Outcomes):**
   Because single-operation fingerprints do not chain history, compatibility requires verifying all common retained checkpoints across $[1, \min(V_L[p], V_S[p])]$. Four mutually exclusive outcomes are enforced: `HISTORY_VERIFIED`, `HISTORY_CONFLICT`, `HISTORY_INCOMPLETE`, and `HISTORY_UNAVAILABLE`.
3. **No Arbitrary Tie-Breaking:**
   If two snapshots have incomparable frontiers, the engine transitions to `DEGRADED` state and notifies the application. Arbitrary tie-breaking (by peer ID or wall clock) is mathematically forbidden.
4. **Normative Iterative Transport Sizing:**
   `calculateDynamicChunkSize` serves solely as an initial estimation. Outbound chunks undergo a normative 7-step iterative send-boundary loop that serializes, measures exact UTF-8 bytes, and reduces binary payload until fitting within the negotiated SCTP limit before calling `RTCDataChannel.send()`.
5. **Exact Phase 7.2 Lamport Invariant Reinstated:**
   $$S.\text{maxLogicalTimestamp} \equiv \max_{p \in \text{origins}(V_S)} S.\text{originFrontierTimestamps}[p]$$
   Reseeding executes strictly as $L_{\text{local}} = \max(L_{\text{local}}, S.\text{maxLogicalTimestamp})$ **without $+1$**.
6. **Staged Immutable Engine-State Replacement & Clock Audit:**
   Installation constructs a new `OrderedOperationLog`, state, and replication vectors out-of-band. Once validated, an atomic reference swap executes in a single synchronous run-to-completion step across all 14 mutable components. External listeners are notified only post-swap and post-reconciliation.
7. **Quarantine Buffer & Local Input Fencing:**
   Local control operations are blocked while recovering. Incoming live operations are quarantined (capacity: 1,000 operations). Exceeding capacity synchronously aborts transfer to `DEGRADED`.

---

## 3. Four-State Recovery State Machine

```mermaid
stateDiagram-v2
    [*] --> ACTIVE
    ACTIVE --> RECOVERING : onHistoryExhausted / REQUIRES_SNAPSHOT_RECOVERY
    
    state RECOVERING {
        [*] --> SelectingSupplier
        SelectingSupplier --> TransferringChunks : SNAPSHOT_REQUEST sent
        TransferringChunks --> ValidatingSnapshot : All chunks assembled
        ValidatingSnapshot --> TransferringChunks : Retransmit missing/corrupt chunk
    }

    RECOVERING --> RECONCILING : validateCandidate() OK & stagedReferenceSwap() OK
    RECOVERING --> DEGRADED : Incomparable snapshot / Retries exhausted / Quarantine overflow / HISTORY_UNAVAILABLE
    RECOVERING --> ACTIVE : Transfer aborted (revert to local state)

    state RECONCILING {
        [*] --> DrainingQuarantineBuffer
        DrainingQuarantineBuffer --> EvaluatingRemainingGaps : Apply causal ops
        EvaluatingRemainingGaps --> [*] : Frontiers aligned
    }

    RECONCILING --> ACTIVE : All live events applied & listeners notified
    RECONCILING --> DEGRADED : Reconciliation timeout / Unresolvable gap
    DEGRADED --> RECOVERING : New dominating snapshot offer received
    DEGRADED --> [*]
```

### State Definitions and Behaviors

| State | Local Control Inputs | Incoming Broadcast Operations | Inbound Snapshot Transfers | Description |
| :--- | :--- | :--- | :--- | :--- |
| **`ACTIVE`** | Allowed (processed immediately) | Processed via standard Phase 7.3 causal ordering | None | Normal playback synchronization. |
| **`RECOVERING`** | **Blocked** (rejects or queues locally) | **Quarantined** in `QuarantineBuffer` (cap: 1000) | Exactly 1 active transfer | Inbound snapshot chunk transfer and pre-validation. |
| **`RECONCILING`** | **Blocked** | **Quarantined** until buffer drains | None | Staged reference swap complete; draining quarantine in causal order. |
| **`DEGRADED`** | **Blocked** | Discarded or held; state frozen | None | Causal divergence detected or retries exhausted; requires application intervention. |

---

## 4. Compaction Frontier Procedure & Revalidation

### 4.1 Membership View $\mathcal{V}$ Capture
The local peer captures:
$$\mathcal{V} = \Big( E_{\text{local}}, V_{\text{local\_snapshot}}, \mathcal{P} = \{ (p, gen_p) \mid \text{dataChannelState}(p) === \text{'open'} \} \Big)$$

### 4.2 Current Digest Requirement
For every peer in $\mathcal{P}$, the local engine must hold an authenticated `STATE_DIGEST` $D_p$ matching $(p, gen_p, E_{\text{local}})$.
- If any peer has not supplied a current digest, view $\mathcal{V}$ is incomplete. Compaction at $V_{\text{epoch\_safe}}$ is held.
- If remote membership $\mathcal{P} = \emptyset$, $V_{\text{epoch\_safe}} = V_{\text{local}}$.
- Missing origins are treated strictly as sequence $0$ ($V[p] ?? 0$).
- If memory requires pruning ($K=256$), the engine prunes locally under $V_{\text{local\_cand}}$ without peer safety guarantees.

### 4.3 Pre-Commit Revalidation
Immediately prior to invoking `compactAtFrontier(V_epoch_safe)`:
1. Verify `engine.epoch === V.epoch`.
2. Verify active connected peer set in `PeerManager` matches $\mathcal{P}$ exactly.
3. Verify $V_{\text{epoch\_safe}}[p] \le \text{engine.replicationVector}[p]$ for all origins $p$.
4. If any peer disconnected, generation changed, epoch incremented, or local vector rolled back: **abort compaction immediately**.

---

## 5. Chunked Transfer and Transport Boundary Integration

### 5.1 Iterative Sizing and Send Procedure
1. Initial estimation: `calculateDynamicChunkSize(sctpMaxMessageSize)` provides starting slice size.
2. Binary payload is sliced and formatted into `ProtocolMessage<'SNAPSHOT_CHUNK'>`.
3. `wireJson = JSON.stringify(msg)` is generated.
4. Exact UTF-8 byte length is measured: `new TextEncoder().encode(wireJson).byteLength`.
5. If `byteLength <= effectiveLimit`:
   - Pass `wireJson` unchanged to `PeerManager.sendSerialized()`.
   - `PeerManager` checks generation and DataChannel state immediately prior to `RTCDataChannel.send()`.
6. If `byteLength > effectiveLimit`:
   - Calculate overshoot $\Delta = \lceil (\text{byteLength} - \text{effectiveLimit}) \times 0.75 \rceil + 32$.
   - Reduce chunk binary slice and repeat from Step 2.
   - If chunk binary size drops below 256 bytes without fitting: abort with `TRANSPORT_LIMIT_EXCEEDED` $\to$ `DEGRADED`.

---

## 6. Staged Immutable Engine-State Replacement

### 6.1 Replacement Workflow
```
+----------------------------------------------------------------------------------------------------+
| STAGE 1: Pure Dry-Run Validation (validateCandidate)                                               |
| - Validates StateSnapshotPayloadV2Schema, canonical SHA-256 digest, epoch, vector domination, and  |
|   evaluates historical prefix verification producing one of the 4 outcomes.                        |
| - ZERO engine state is touched.                                                                    |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ HISTORY_VERIFIED
+----------------------------------------------------------------------------------------------------+
| STAGE 2: Out-of-Band Staged Instance Construction                                                  |
| - Construct StagedPlaybackState with stateVersion = localVersion + 1.                              |
| - Construct new OrderedOperationLog pre-loaded with snapshot base and checkpoints.                 |
| - Construct staged LogicalClockManager or stage reseed clock value without throwing.               |
| - Construct new replicationVector, localCausalContext, and empty pendingQueue.                     |
| - If any constructor throws: discard staged instances. Old engine state remains 100% active.       |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ Staging Succeeded
+----------------------------------------------------------------------------------------------------+
| STAGE 3: Atomic Reference Swap (installSnapshotAtomic)                                             |
| - Synchronous run-to-completion property swap across 14 mutable components:                       |
|     this.state = stagedState;                                                                      |
|     this.baseSnapshot = stagedState;                                                               |
|     this.operationLog = stagedLog;                                                                 |
|     this.replicationVector = stagedVector;                                                         |
|     this.localCausalContext = stagedCausalContext;                                                 |
|     this.clockManager.reseed(snapshot.maxLogicalTimestamp);                                        |
|     this.pendingQueue = stagedQueue;                                                               |
| - Transition state to RECONCILING.                                                                 |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ Post-Swap
+----------------------------------------------------------------------------------------------------+
| STAGE 4: Quarantine Drain & Deferred Listener Notification                                         |
| - Drain QuarantineBuffer in causal order.                                                          |
| - Upon successful drain and transition to ACTIVE:                                                  |
|     Notify registered stateChangeListeners(this.state).                                            |
| - Listeners NEVER observe intermediate recovery states.                                            |
+----------------------------------------------------------------------------------------------------+
```

### 6.2 Complete 14-Component State Inventory
1. `state`: `PlaybackState` & incremented `stateVersion`
2. `clockManager`: Lamport clock (`currentClock`, `maxObservedByOrigin`, `originRateStates`)
3. `replicationVector`: `Record<string, number>`
4. `localCausalContext`: `Record<string, number>`
5. `baseSnapshot`: `PlaybackState | undefined`
6. `baseSnapshotFrontier`: `Record<string, number>`
7. `operationLog`: `OrderedOperationLog` (operations array, idIndex, originSeqIndex, compactedCheckpoints map, originFrontierTimestamps map)
8. `pendingQueue`: `PendingDependencyQueue`
9. `dedupCache`: deduplication cache
10. `lastDecision`: `EngineDecision`
11. `mutableStats`: stats counters
12. `recoveryState`: `RecoveryState` (`ACTIVE`, `RECOVERING`, `RECONCILING`, `DEGRADED`)
13. `quarantineBuffer`: `QuarantineBuffer`
14. `stateChangeListeners`: deferred listener notifications

---

## 7. Boundary Between Ordinary Recovery and Manual Reset

- **Ordinary Recovery:** Fully automated and mathematically constrained by causal consistency. Any incomparable snapshot, fork, or unresolvable gap halts recovery and enters `DEGRADED`.
- **Manual Reset:** An out-of-band administrative intervention where a user intentionally discards incompatible history and generates a new session identity (`newSessionId`) with an epoch fence barrier ($E_{\text{reset}} = E + 1000$).
- **Phase 7.5 Scope:** Ordinary recovery and `DEGRADED` fencing are fully implemented in Phase 7.5. Manual reset workflows and UI interactions are **explicitly deferred to Phase 8**.
