# Phase 7.5 — Fault Tolerance & State Recovery: Research and Problem Definition

**Status:** APPROVED ARCHITECTURAL DESIGN (RE-AUDITED)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `3a3d9e5`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Problem Statement and Scope

In Phase 7.4, the SyncPlay protocol established a request-driven, peer-to-peer anti-entropy repair protocol capable of reconciling missing operation sequences across active mesh members. However, Phase 7.4 operates under a fundamental constraint: **anti-entropy repair can only service sequence gaps that exist within the active operation logs of connected suppliers.**

In real-world distributed media playback across unstable WebRTC peer meshes, peers routinely experience conditions that exceed the bounded history retention horizon of active logs:
1. **Extended Disconnections / Late Joiners:** A peer joins or rejoins after being disconnected for minutes or hours, during which hundreds or thousands of playback operations occurred and were compacted.
2. **Log Compaction Horizon:** To prevent unbounded memory consumption, operation logs are pruned up to a stable consensus frontier ($V_{\text{snap}}$). Operations older than $V_{\text{snap}}$ are collapsed into a base playback snapshot and permanently discarded from memory.
3. **No Supplier Retains History:** When an anti-entropy request triggers `onHistoryExhausted` (due to `OUTSIDE_HORIZON` or `NO_ELIGIBLE_REPAIR_SUPPLIER`), operation repair is mathematically impossible.
4. **Network Partitions and Divergent Snapshots:** Multiple network partitions may independently compact different subsets of history or operate at different frontiers.
5. **Epoch Transitions During Recovery:** Membership changes trigger epoch increments while a recovery handshake is in-flight.

**Phase 7.5 establishes the state recovery architecture** that enables peers to safely catch up across compaction horizons, heal partitions, and recover from extended disconnections without sacrificing causal correctness, Lamport clock monotonicity, or leaderless autonomy.

---

## 2. Invalidation of Common Anti-Patterns

Snapshot recovery in distributed media sync is plagued by several tempting but dangerous shortcuts:

### Anti-Pattern 1: "Downloading the Newest Snapshot"
- **The Fallacy:** A rejoining peer simply queries connected peers for their latest snapshot and adopts the one with the highest `stateVersion` or newest wall-clock timestamp.
- **Why It Fails:** In a leaderless peer-to-peer system, a higher `stateVersion` or timestamp does not imply causal dominance. A partitioned peer could have generated 50 rapid pause/play toggles in isolation, resulting in a high `stateVersion` that is causally stale or conflicted with the main group. Installing the "newest" snapshot can overwrite legitimate group playback with isolated partition noise.

### Anti-Pattern 2: "Matching Hashes Prove Causal Equivalence"
- **The Fallacy:** If two peers calculate the same hash over their current playback state (e.g. `status: paused, position: 45000`), their histories and causal vectors must be identical.
- **Why It Fails:** State convergence can happen coincidentally (e.g. two independent seeks to the same chapter marker). Matching state payloads do not prove identical causal vectors or identical Lamport clocks. Reseeding a clock based solely on state hashes causes causal regressions.

### Anti-Pattern 3: "Replication Vectors Prove Historical Equivalence"
- **The Fallacy:** If $V_A[\text{peer-1}] = V_B[\text{peer-1}] = 100$, both peers have executed the same historical operations.
- **Why It Fails:** Without historical fingerprint chaining or checkpoint verification, a sequence number collision (two conflicting events authored with the same sequence) could exist below the frontier.

### Anti-Pattern 4: "Arbitrary Tie-Breaking on Incomparable Snapshots"
- **The Fallacy:** When two snapshots have incomparable frontiers ($V_A > V_B$ on origin 1, but $V_B > V_A$ on origin 2), resolve the conflict by picking the lowest peer ID or highest timestamp.
- **Why It Fails:** Arbitrary tie-breaking violates causality and creates state divergence. Unless missing operations between the frontiers can be retrieved and reduced in total order, incomparable snapshots represent unresolvable historical divergence that must enter an explicit degraded state.

---

## 3. Deep-Dive Answers to the 18 Core Research Questions

### Q1: What exactly makes a snapshot eligible for installation?
A snapshot $S$ is eligible for atomic installation if and only if all eight conditions hold:
1. **Valid Format Version:** `formatVersion === 2`.
2. **Bit-Exact SHA-256 Digest:** Canonical RFC 8785 JSON representation of $S$ produces a SHA-256 digest identical to $S.\text{sha256Digest}$.
3. **Strict Vector Domination:** For every origin $p$ known locally:
   $$V_S[p] \ge V_{\text{local}}[p]$$
4. **Historical Checkpoint Compatibility:** For every common sequence $k = \min(V_S[p], V_{\text{local}}[p])$, checkpoint fingerprints match:
   $$\text{digest}_S(p, k) == \text{digest}_{\text{local}}(p, k)$$
5. **Lamport Clock Monotonicity:** $S.\text{maxLogicalTimestamp} \ge \max_p S.\text{originFrontierTimestamps}[p]$ and is within valid integer bounds ($\le \text{Number.MAX\_SAFE\_INTEGER}$).
6. **Epoch Compatibility:** $S.\text{membershipEpoch} \le \text{currentEpoch}$.
7. **Identity Authorization:** $S.\text{authorPeerId}$ is a recognized participant in the session.
8. **Generation Currency:** Arrives over an authenticated DataChannel whose connection generation matches current active generation.

### Q2: How does a peer detect that log repair is impossible?
Log repair is declared impossible when:
- An anti-entropy repair request to all eligible suppliers returns `REPAIR_REJECTED` with reason `OUTSIDE_HORIZON`.
- `ReplicationComparator.compareDigests()` evaluates remote digest as `REQUIRES_SNAPSHOT_RECOVERY`.
- `RepairRequestPipeline` exhausts retries and invokes `callbacks.onHistoryExhausted`.

### Q3: How is the compaction horizon safely determined?
See Section 4 for detailed re-evaluation of compaction frontiers. Compaction is bounded by the membership-scoped safe frontier ($V_{\text{epoch\_safe}}$), representing the component-wise minimum of connected peers' acknowledged vectors in the active epoch.

### Q4: What prevents snapshot corruption during transmission?
- Multi-tier validation: each chunk carries its own SHA-256 hash (`chunkSha256`).
- Full reassembled byte array is verified against $S.\text{sha256Digest}$ prior to parsing.
- Zod schema validation (`StateSnapshotPayloadV2Schema`) executes before staging.

### Q5: How is snapshot chunking bounded for WebRTC?
- WebRTC DataChannel hard ceiling: 65,536 bytes.
- Dynamic chunk sizing (Section 7) bounds payload to $\le 32,768$ bytes, ensuring Base64 plus JSON overhead stays well below negotiated SCTP limits.
- Total snapshot size hard-capped at 4,194,304 bytes (4 MB = 128 chunks).

### Q6: How does Lamport clock reseeding avoid clock runaway?
Phase 7.2-R7 and R8 established the exact reseeding invariant:
$$L_{\text{local}} = \max(L_{\text{local}}, S.\text{maxLogicalTimestamp})$$
**Strictly WITHOUT adding $+1$.** Subsequent local events add $+1$, preserving monotonicity without artificial clock inflation across repeated recovery cycles.

### Q7: How are live control events handled during recovery?
Incoming broadcast operations are diverted to an in-memory `QuarantineBuffer` (capacity 1,000 operations). Local control inputs are blocked. Post-installation, the buffer is drained in causal order.

### Q8: What happens when two partitions heal with incomparable snapshots?
Neither snapshot causally dominates. Arbitrary tie-breaking is strictly prohibited. The engine enters `DEGRADED` state and notifies the application via `onRecoveryDegraded`.

### Q9: How is atomic installation guaranteed?
Staged validation (`validateCandidate`) performs dry-run schema and digest checks. The engine commit runs in a single synchronous run-to-completion step with state backup and rollback on exception.

### Q10: How does a peer verify historical continuity below the compaction horizon?
Via `compactedCheckpoints` tables stored in `StateSnapshotPayloadV2`. Each origin stores checkpoints with `originSequenceNumber`, `logicalTimestamp`, and SHA-256 `fingerprint`.

### Q11: How are membership epochs handled during transfer?
If the room epoch increments during an active transfer, the transfer is synchronously aborted (`EPOCH_MISMATCH`) and all intermediate chunk buffers are discarded.

### Q12: How are supplier failures handled mid-transfer?
`SnapshotTransferManager` detects DataChannel close or timeouts and aborts the transfer, allowing failover to an alternative supplier.

### Q13: How is local state version advanced?
$$V_{\text{state, local}} = V_{\text{state, local}} + 1$$
Local sequence monotonicity is preserved; remote `stateVersion` is recorded in metadata.

### Q14: How does a recovering peer prevent replay amplification?
The peer maintains at most ONE active inbound transfer. Duplicate chunks are deduplicated by index bitmask.

### Q15: How does snapshot transfer interact with SCTP backpressure?
Chunk requests are throttled using a sliding window of at most 4 concurrent in-flight chunks (`MAX_CONCURRENT_CHUNK_REQUESTS = 4`).

### Q16: How does the system prevent memory exhaustion during reassembly?
Pre-allocation checks reject snapshots $>4\text{ MB}$ or chunks $>32\text{ KB}$ before allocating buffer memory. Only 1 active transfer is permitted.

### Q17: What metadata must a snapshot retain for future anti-entropy?
`snapshotFrontier`, `originFrontierTimestamps`, and `compactedCheckpoints` are preserved and injected into `OrderedOperationLog`.

### Q18: What is the formal boundary between Ordinary Recovery and Manual Reset?
Ordinary recovery is strictly causally consistent. Manual reset abandons causality and creates a new session identity (Section 6).

---

## 4. Compaction Horizon & Membership Dynamics: Re-Evaluation

The computation of the compaction horizon must be grounded in the concrete realities of SyncPlay's leaderless architecture.

### 4.1 Answers to the 10 Membership & Compaction Audit Questions

1. **What precisely defines the acknowledgement membership set?**
   In SyncPlay's peer-to-peer mesh, there is NO centralized membership registry. The acknowledgement membership set $\mathcal{M}_{\text{local}}$ is strictly the set of peers currently connected to the local peer via open WebRTC DataChannels in `PeerManager.listPeers()`.
2. **Is that set fixed for one membership epoch?**
   No. While an epoch transition occurs when peers join or leave via signaling, WebRTC DataChannels can open, close, or reset independently within an epoch due to network drops, ICE renegotiations, or browser tab freezes before signaling catches up.
3. **What exactly does an acknowledgement certify?**
   When a peer broadcasts a `STATE_DIGEST` containing `replicationVector[origin] = seq`, it certifies **local causal admission and application**—meaning that peer has verified, applied, and incorporated all operations up to sequence `seq` into its local playback state and log. It does **NOT** certify that other peers have received it, nor that a snapshot has been generated.
4. **Does it certify receipt, structural validation, causal admission, or committed incorporation into a snapshot?**
   It certifies structural validation and causal admission into the local engine. It does not certify committed incorporation into a snapshot.
5. **How is an acknowledgement bound to peer identity, generation, and membership epoch?**
   The `STATE_DIGEST` carries `senderPeerId`, `connectionGeneration`, and `membershipEpoch`. `TrustedTransportAdapter` verifies that `senderPeerId` matches the WebRTC peer, `connectionGeneration` matches `peerGenerations.get(senderPeerId)`, and `membershipEpoch` matches `this.currentEpoch`.
6. **What happens if a peer disconnects while the GLB is being calculated?**
   If a peer disconnects, its vector is omitted from subsequent calculations. If compaction executes based on an in-flight calculation that included the disconnected peer, the resulting frontier may be further advanced than what an offline peer retains.
7. **Can an excluded peer later rejoin with stale history?**
   **YES.** A peer that disconnected at sequence 20 may rejoin minutes later when the remaining peers have advanced to sequence 500 and compacted up to 450.
8. **Can two peers independently calculate different compaction frontiers?**
   **YES.** Because there is no consensus leader or atomic distributed barrier, Peer A (observing digests from {P1, P2}) and Peer B (observing digests from {P2, P3}, or observing P1 with network delay) will independently calculate different GLB frontiers.
9. **What prevents a peer from claiming an acknowledgement it never actually completed under the cooperative trust model?**
   Under Cooperative Non-Byzantine Model (Phase 7.2-R4 Option C), peers are honest and follow the protocol without malicious manipulation. Premature advancement is prevented by advancing `replicationVector` only within `applyOperation()`.
10. **What happens when membership changes before compaction completes?**
    An epoch transition increments `currentEpoch`. In-flight digests from the prior epoch are immediately stale and discarded.

### 4.2 Three Levels of Compaction Frontiers

To prevent unsafe assumptions, the architecture strictly distinguishes three concepts:

```
+----------------------------------------------------------------------------------------------------+
| 1. Local Compaction Candidate (V_local_cand):                                                      |
|    Operations older than capacity K=256 that the local peer wishes to prune for memory reasons.     |
|    SAFETY: None for other peers. Only ensures local memory boundedness.                            |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼
+----------------------------------------------------------------------------------------------------+
| 2. Membership-Scoped Safe Frontier (V_epoch_safe):                                                 |
|    Component-wise GLB of replication vectors received from all currently connected peers:           |
|    V_epoch_safe[p] = min_{m in M_connected} V_m[p]                                                 |
|    SAFETY: Safe for currently connected peers in the active epoch.                                 |
|    LIMITATION: Does NOT protect disconnected, partitioned, or newly joining peers.                |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼
+----------------------------------------------------------------------------------------------------+
| 3. Globally Agreed Stable Frontier (V_stable):                                                     |
|    A frontier certified by a 100% atomic consensus round or total-order commit across all peers.  |
|    STATUS IN SYNCPLAY: DOES NOT EXIST. SyncPlay is leaderless without Raft/Paxos rounds.          |
+----------------------------------------------------------------------------------------------------+
```

### 4.3 Architectural Consequence: Snapshot Recovery as a First-Class Necessity
Because a **Globally Agreed Stable Frontier does not exist**, peers will inevitably compact history that some disconnected or partitioned peer later needs.

**Conservative Architectural Decision:**
1. A peer is permitted to compact operations up to $V_{\text{epoch\_safe}}$ (or up to its local ring buffer capacity $K=256$ when memory limits require).
2. Compacting peers **must retain compacted checkpoints** for all pruned origins.
3. The protocol formally embraces that a supplier will return `REPAIR_REJECTED (OUTSIDE_HORIZON)` when a reconnecting peer requests compacted history.
4. **Snapshot recovery is not an exceptional failure mode; it is the fundamental, expected recovery mechanism for leaderless operation under churn.**

---

## 5. Formal Snapshot Eligibility & Historical Compatibility Procedure

Vector domination ($V_S \ge V_L$) is a necessary condition, but **does not guarantee historical compatibility**.

### 5.1 Formal Compatibility Algorithm

Given local state $(V_L, CP_L, E_L)$ and candidate snapshot $S = (V_S, FT_S, CP_S, E_S, H_S)$:

```
function evaluateSnapshotEligibility(S, local):
    // 1. Schema & Integrity Validation
    if not StateSnapshotPayloadV2Schema.safeParse(S).success:
        return REJECTED("Schema validation failed")
    if sha256(canonicalJson(S.payload)) != S.sha256Digest:
        return REJECTED("SHA-256 digest mismatch")

    // 2. Epoch Currency Check
    if S.membershipEpoch > local.currentEpoch:
        return REJECTED("Snapshot from future epoch")

    // 3. Vector Domination Check
    for each origin p in keys(local.V_L):
        if S.V_S[p] === undefined or S.V_S[p] < local.V_L[p]:
            if exists q where S.V_S[q] > local.V_L[q]:
                return DEGRADED("Incomparable snapshots: partial divergence")
            else:
                return REJECTED("Stale snapshot: local state is newer")

    // 4. Metadata Completeness Check
    for each origin p in keys(S.V_S):
        if S.V_S[p] > 0 and S.originFrontierTimestamps[p] === undefined:
            return REJECTED("Missing origin frontier timestamp for " + p)
    if S.maxLogicalTimestamp < max_p(S.originFrontierTimestamps[p]):
        return REJECTED("Inconsistent maxLogicalTimestamp")

    // 5. Historical Checkpoint Compatibility Check
    for each origin p in keys(local.V_L):
        commonSeq = min(local.V_L[p], S.V_S[p])
        if commonSeq == 0:
            continue
            
        localCP = local.getCheckpoint(p, commonSeq)
        remoteCP = S.getCheckpoint(p, commonSeq)
        
        if localCP is defined and remoteCP is defined:
            if localCP.fingerprint != remoteCP.fingerprint or 
               localCP.logicalTimestamp != remoteCP.logicalTimestamp:
                return REJECTED("Checkpoint divergence: history fork on " + p)
        else:
            // Checkpoint missing at exact common sequence
            if local.hasRetainedOperation(p, commonSeq):
                localFp = computeFingerprint(local.getOperation(p, commonSeq))
                if remoteCP is defined and remoteCP.fingerprint != localFp:
                    return REJECTED("History fork detected against stored operation")
            else:
                // Neither side retains proof of common history at sequence commonSeq
                return REQUIRES_HISTORY_REPAIR("Common checkpoint unavailable")

    return ELIGIBLE
```

### 5.2 Concrete Verification Examples

| Case | Local State ($V_L, CP_L$) | Candidate Snapshot ($V_S, CP_S$) | Causal / Checkpoint Evaluation | Exact Result |
| :--- | :--- | :--- | :--- | :---: |
| **A: Valid Dominating Snapshot** | $V_L = \{ A: 10, B: 5 \}$<br/>$CP_L(A,10) = \text{fp}_1$<br/>$CP_L(B,5) = \text{fp}_2$ | $V_S = \{ A: 15, B: 8, C: 2 \}$<br/>$CP_S(A,10) = \text{fp}_1$<br/>$CP_S(B,5) = \text{fp}_2$ | $\forall p: V_S[p] \ge V_L[p]$. Common checkpoints match bit-exact. C causal extension. | **ELIGIBLE** |
| **B: Incomparable Snapshots** | $V_L = \{ A: 12, B: 4 \}$ | $V_S = \{ A: 8, B: 10 \}$ | $V_S[A] < V_L[A]$ while $V_S[B] > V_L[B]$. Neither dominates. | **DEGRADED** |
| **C: Conflicting Checkpoint** | $V_L = \{ A: 10 \}$<br/>$CP_L(A,10) = \text{fp}_{\text{alpha}}$ | $V_S = \{ A: 15 \}$<br/>$CP_S(A,10) = \text{fp}_{\text{beta}}$ | $V_S \ge V_L$, but fingerprints at common sequence 10 conflict ($\text{fp}_{\alpha} \ne \text{fp}_{\beta}$). History fork! | **REJECTED** |
| **D: Unavailable Checkpoint** | $V_L = \{ A: 10 \}$<br/>$CP_L(A,10) = \text{fp}_1$ | $V_S = \{ A: 100 \}$<br/>$CP_S$ only has sequences $\ge 80$ | $V_S \ge V_L$, but no common checkpoint exists between sequence 10 and 80. Continuity unproven. | **REQUIRES_HISTORY_REPAIR** |
| **E: Missing Frontier Timestamp** | $V_L = \{ A: 5 \}$ | $V_S = \{ A: 20, B: 10 \}$<br/>$FT_S = \{ A: 50 \}$ (B missing) | Malformed snapshot metadata: origin $B$ has sequence 10 but missing frontier timestamp. | **REJECTED** |

---

## 6. Manual Recovery Versus Causal Convergence

A strict boundary is drawn between autonomous distributed recovery and administrative intervention.

### 6.1 Ordinary Recovery (Causally Consistent)
- **Scope:** Automated, algorithmic state reconciliation.
- **Guarantee:** Never adopts a snapshot that cannot be proven to causally dominate local history.
- **Behavior on Incomparable Snapshots:** The engine halts automated reconciliation, transitions to `DEGRADED`, and dispatches `onRecoveryDegraded`. It **NEVER** silently guesses or forces one branch over another.

### 6.2 Explicit Manual Reset (Administrative Intervention)
If an application or user decides to recover from a `DEGRADED` state by forcibly choosing one peer's playback position, this is an **intentional abandonment of causal history**, NOT causal reconciliation.

**Formal Specification for Manual Reset:**
1. **New Session Identity:** The initiator must generate a fresh `sessionId = UUIDv4()`.
2. **Epoch Fence:** Membership epoch is reset or incremented by a fence barrier ($E_{\text{reset}} = E + 1000$).
3. **Fencing Stale History:** `TrustedTransportAdapter` and `PeerManager` immediately drop and reject any message or snapshot carrying the old `sessionId` or an epoch $< E_{\text{reset}}$.
4. **Application Notification:** The engine fires `onHistoryAbandoned({ previousSessionId, newSessionId, authorPeerId })` to explicitly alert the UI layer.
5. **Phase Scope:** Implementing administrative manual reset workflows involves UI prompts and signaling session renegotiation. **Manual Reset is explicitly DEFERRED to Phase 8 / Application Layer.** Phase 7.5 provides the detection, safety fences, and `DEGRADED` state triggers.

---

## 7. Actual Transport Limits & Dynamic Chunk Sizing

Phase 7.5 snapshot transfers run over WebRTC DataChannels and must strictly comply with Phase 7.4 M5's transport architecture.

### 7.1 Re-evaluation of Static 32 KB Chunking
In the initial proposal, chunks were fixed at 32 KB (`MAX_CHUNK_BYTES = 32768`).
- 32 KB raw binary $\xrightarrow{\text{Base64}}$ $\approx 43,692$ bytes.
- JSON envelope overhead $\approx 500$ bytes.
- Total message size $\approx 44.2$ KB.

**Defect Identified:** RFC 8841 permits WebRTC implementations to negotiate SCTP maximum message sizes smaller than 64 KB (e.g., 16,384 bytes, 8,192 bytes, or 1,280 bytes). If a peer's `sctpMaxMessageSize` is 16 KB, a fixed 32 KB chunk will fail `PeerManager.sendSerialized()` every time with:
`Message exceeds maximum size (45250 bytes > 16384 limit)`.

### 7.2 Dynamic Chunk Sizing Formula
Phase 7.5 reuses `PeerManager.getSctpMaxMessageSize(peerId)` to dynamically calculate the maximum chunk payload size per supplier:

$$\text{effectiveLimit} = \min(65536, \text{sctpLimit} \text{ if defined and } > 0 \text{ else } 65536)$$
$$\text{envelopeBudget} = 768 \text{ bytes (JSON keys, UUIDs, chunkSha256, session metadata)}$$
$$\text{availableBase64Bytes} = \max(0, \text{effectiveLimit} - \text{envelopeBudget})$$
$$\text{dynamicChunkBytes} = \min(32768, \lfloor \text{availableBase64Bytes} \times \frac{3}{4} \rfloor)$$

- Under standard 64 KB limit: $\text{dynamicChunkBytes} = \min(32768, \lfloor 64768 \times 0.75 \rfloor) = 32,768$ bytes (32 KB).
- Under 16 KB SCTP limit: $\text{dynamicChunkBytes} = \lfloor (16384 - 768) \times 0.75 \rfloor = 11,712$ bytes.
- Minimum threshold: If $\text{effectiveLimit} < 2048$ bytes, chunk transfer over DataChannel is unfeasible. The transfer aborts with `TRANSPORT_LIMIT_EXCEEDED` and enters `DEGRADED`.

### 7.3 Reuse of M5 Send Boundary
1. The serialized JSON message is constructed and measured once:
   $$\text{byteLength} = \text{new TextEncoder}().\text{encode}(\text{wireJson}).\text{byteLength}$$
2. The exact string is passed unchanged to `PeerManager.sendSerialized()`.
3. `PeerManager.sendSerialized()` performs the final currency checks:
   - Target peer exists in `peers` map.
   - Target connection generation matches `entry.generation`.
   - DataChannel `readyState === 'open'`.
   - Byte length does not exceed `effectiveLimit`.
4. The exact string is passed directly to `RTCDataChannel.send()`.

---

## 8. Snapshot Identity, Provenance, and Transfer Concurrency

### 8.1 Snapshot Identity: Creation-Unique vs Content-Addressed
- **`snapshotId`:** A creation-unique UUIDv4 generated by the authoring peer. It identifies the snapshot instance across transfer requests.
- **`sha256Digest`:** The content-addressed hash computed over the canonical RFC 8785 JSON representation of the payload. It guarantees payload immutability and byte-exact integrity.

### 8.2 Author vs Supplier
- **`authorPeerId`:** The peer that originally created the snapshot and computed its state.
- **`supplierPeerId`:** The currently connected peer transmitting the snapshot over WebRTC. A supplier may serve a snapshot authored by another peer (or itself).

### 8.3 Supplier Retention & Offer Invalidation
- A supplier advertises a snapshot via `SNAPSHOT_OFFER`.
- Offers are invalidated immediately upon:
  1. Peer disconnection.
  2. Room epoch increment (`membershipEpoch`).
  3. Supplier local compaction superseding the base snapshot.
- If a supplier fails to serve chunks, chunk timeouts trigger retry failover to another supplier.

### 8.4 Multiple Suppliers
- **Identical Snapshots:** If two suppliers advertise the same `sha256Digest`, they hold identical data. The requester can download chunks from either, or failover seamlessly mid-transfer.
- **Identical Frontiers with Different Digests:** If two suppliers advertise identical $V_S$ but different `sha256Digest`, their histories diverge (different checkpoints or state). The requester flags this as divergent and rejects both offers.

### 8.5 Concurrency Model Clarification
- **Concurrent Transfers:** Strictly **1 active transfer** per recovering peer (`activeTransfer = null | transferId`). A recovering peer never downloads multiple snapshots simultaneously.
- **In-Flight Chunks:** Within the single active transfer, the requester pipelines up to **4 concurrent chunk requests** (`MAX_CONCURRENT_CHUNK_REQUESTS = 4`) to maximize WebRTC throughput.

---

## 9. Quarantine Mechanics & Atomic Installation Analysis

### 9.1 Inspection of `LeaderlessSyncEngine.importSnapshotV2()`
Inspection of `packages/sync-engine/src/engine/leaderless-sync-engine.ts:1981` reveals:
1. **Validation Deficiencies:** `importSnapshotV2()` performs **zero pre-validation**. It does not check schema, digest, epoch, vector domination, or checkpoint compatibility.
2. **Lack of Atomicity:** It directly mutates engine properties step-by-step:
   - Reseeds clock (`this.clockManager.reseed`).
   - Wipes and overwrites `localCausalContext` and `replicationVector`.
   - Replaces `this.state` with `stateVersion + 1`.
   - Resets `operationLog` and clears `pendingQueue`.
   - Ingests checkpoints.
   If any checkpoint insertion throws or a listener fails, the engine is left partially mutated with no rollback.

### 9.2 Architecture of `SnapshotInstaller`
Phase 7.5 introduces `SnapshotInstaller` to wrap and isolate installation:
1. **Stage 1 (Pre-Commit Dry Run):** `validateCandidate()` executes all checks in Section 5. Zero engine state is touched.
2. **Stage 2 (Synchronous Atomic Commit):** Captures previous state, replication vector, clock, and queues. Executes `importSnapshotV2()`. On any thrown exception, synchronously restores backup state.
3. **Stage 3 (Post-Commit Reconcile):** Quarantined operations are drained and revalidated against the newly installed frontier.

### 9.3 Precise State Machine Transitions
```
                onHistoryExhausted / REQUIRES_SNAPSHOT_RECOVERY
       [ACTIVE] ──────────────────────────────────────────────> [RECOVERING]
          ▲                                                          │
          │                                                          │ validateCandidate() OK
          │                                                          │ & installAtomic() OK
          │                                                          ▼
          │         Quarantine drained & gaps closed            [RECONCILING]
          └──────────────────────────────────────────────────────────┘
                                      │
                                      │ Failure / Timeout / Overflow / Fork
                                      ▼
                                 [DEGRADED]
```

- **`ACTIVE` $\to$ `RECOVERING`:** Triggered when anti-entropy reports `onHistoryExhausted` or digest comparison requires snapshot recovery.
- **`RECOVERING` $\to$ `RECONCILING`:** Triggered when all chunks are assembled, pre-validation succeeds, and atomic installation commits.
- **`RECONCILING` $\to$ `ACTIVE`:** Triggered when the quarantine buffer drains and any remaining causal gaps are resolved.
- **Transitions to `DEGRADED`:**
  - `INCOMPARABLE_SNAPSHOT`: Incomparable frontiers detected.
  - `RETRIES_EXHAUSTED`: 3 failed transfer attempts.
  - `QUARANTINE_OVERFLOW`: Quarantined events exceed 1,000.
  - `CHECKPOINT_DIVERGENCE`: Checkpoint hash mismatch (history fork).
  - `RECONCILIATION_TIMEOUT`: Quarantine drain exceeds 10 seconds.
