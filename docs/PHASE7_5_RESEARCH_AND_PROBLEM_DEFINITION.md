# Phase 7.5 — Fault Tolerance & State Recovery: Research and Problem Definition

**Status:** APPROVED ARCHITECTURAL DESIGN (FINAL SAFETY CLOSURE)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `377c906`  
**Frozen Baselines:** Phase 7.3 (`e49b81a`), Phase 7.4 M1–M5 (`dbd5217`, `3a3d9e5`)  

---

## 1. Problem Statement and Scope

In Phase 7.4, the SyncPlay protocol established a request-driven, peer-to-peer anti-entropy repair protocol capable of reconciling missing operation sequences across active mesh members. However, Phase 7.4 operates under a fundamental constraint: **anti-entropy repair can only service sequence gaps that exist within the active operation logs of connected suppliers.**

In real-world distributed media playback across unstable WebRTC peer meshes, peers routinely experience conditions that exceed the bounded history retention horizon of active logs:
1. **Extended Disconnections / Late Joiners:** A peer joins or rejoins after being disconnected for minutes or hours, during which hundreds or thousands of playback operations occurred and were compacted.
2. **Log Compaction Horizon:** To prevent unbounded memory consumption, operation logs are pruned up to a safe frontier ($V_{\text{epoch\_safe}}$). Operations older than the compaction horizon are collapsed into a base playback snapshot and permanently discarded from memory.
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
4. **Historical Prefix Verification (`HISTORY_VERIFIED`):** Complete common sequence prefix $[1, \min(V_S[p], V_{\text{local}}[p])]$ must have matching fingerprints across all retained checkpoints, with zero unverified gaps (Section 5).
5. **Exact Phase 7.2 Lamport Invariant Reinstated:**
   $$S.\text{maxLogicalTimestamp} \equiv \max_{p \in \text{origins}(V_S)} S.\text{originFrontierTimestamps}[p]$$
   and is within integer bounds ($\le \text{Number.MAX\_SAFE\_INTEGER}$).
6. **Epoch Compatibility & Revalidation:** $S.\text{membershipEpoch} \le \text{currentEpoch}$ adhering to formal epoch revalidation rules (Section 6).
7. **Identity Authorization:** $S.\text{authorPeerId}$ is a recognized participant in the session.
8. **Generation Currency:** Arrives over an authenticated DataChannel whose connection generation matches the active generation.

### Q2: How does a peer detect that log repair is impossible?
Log repair is declared impossible when:
- An anti-entropy repair request to all eligible suppliers returns `REPAIR_REJECTED` with reason `OUTSIDE_HORIZON`.
- `ReplicationComparator.compareDigests()` evaluates remote digest as `REQUIRES_SNAPSHOT_RECOVERY`.
- `RepairRequestPipeline` exhausts retries and invokes `callbacks.onHistoryExhausted`.

### Q3: How is the compaction horizon safely determined?
See Section 4. Compaction is bounded by $V_{\text{epoch\_safe}}$ calculated across all connected peers **AND explicitly including the local peer's replication vector**, or bounded by local ring buffer capacity ($K=256$).

### Q4: What prevents snapshot corruption during transmission?
- Multi-tier validation: each chunk carries its own SHA-256 hash (`chunkSha256`).
- Full reassembled byte array is verified against $S.\text{sha256Digest}$ prior to parsing.
- Zod schema validation (`StateSnapshotPayloadV2Schema`) executes before staging.

### Q5: How is snapshot chunking bounded for WebRTC?
- WebRTC DataChannel hard ceiling: 65,536 bytes.
- Dynamic chunk sizing (Section 7) uses `calculateDynamicChunkSize` as an initial estimate, with an iterative normative send-boundary loop measuring exact serialized UTF-8 bytes against negotiated SCTP limits.
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
Staged validation (`validateCandidate`) executes purely out-of-engine. Installation uses a **Staged Immutable Engine-State Replacement** pattern, atomically swapping engine references only after all structures and checkpoints are validated (Section 9). Listeners are notified only post-swap.

### Q10: How does a peer verify historical continuity below the compaction horizon?
Via `compactedCheckpoints` tables stored in `StateSnapshotPayloadV2`. Checkpoints across the common sequence prefix are cross-checked (Section 5).

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

## 4. Compaction Frontier Procedure: Mandatory Local Peer Inclusion

The computation of the compaction horizon must be grounded in the concrete realities of SyncPlay's leaderless architecture.

### 4.1 Mandatory Local Peer Inclusion in $V_{\text{epoch\_safe}}$
A critical safety invariant of distributed log compaction is: **a node must never compact beyond its own applied sequence frontier.**

If a compaction frontier is calculated strictly from remote peers:
- Example: $V_{\text{local}} = \{ A: 5, B: 8 \}$, while remote peers report $V_{\text{remote}} = \{ A: 10, B: 12 \}$.
- Calculating the GLB over remote peers alone yields $\{ A: 10, B: 12 \}$.
- Calling `compactAtFrontier({ A: 10, B: 12 })` would attempt to prune operations up to sequence 10 for $A$ and 12 for $B$.
- **Disaster:** The local engine has not applied those operations! Pruning beyond the applied sequence corrupts the base snapshot, folds non-existent operations into playback state, and permanently loses local history integrity.

**The Normative Formula:**
$$V_{\text{epoch\_safe}}[p] = \min \Big( V_{\text{local}}[p], \min_{(r, gen) \in \mathcal{P}} D_r.\text{replicationVector}[p] \Big)$$
**The local engine's replication vector $V_{\text{local}}$ MUST BE an explicit member of every compaction-frontier calculation.**

### 4.2 Formal Operational Specifications

1. **Empty Remote Membership ($\mathcal{P} = \emptyset$):**
   When running disconnected or as the sole peer in a mesh, $V_{\text{epoch\_safe}} = V_{\text{local}}$. Autonomous log pruning is bounded by $V_{\text{local}}$ and local ring buffer capacity ($K=256$).
2. **Missing Origin Entries:**
   If an origin $p$ is missing from any peer's replication vector (remote or local), its sequence is strictly defined as $0$:
   $$\text{seq}(V, p) = V[p] ?? 0$$
   Consequently, if any peer in the view has not yet observed origin $p$, the minimum is $0$, preventing any compaction of origin $p$ under $V_{\text{epoch\_safe}}$.
3. **Local Vector Capture Time:**
   $V_{\text{local}}$ is captured synchronously at the exact moment the membership view $\mathcal{V}$ is assembled:
   $$\mathcal{V} = \Big( E_{\text{local}}, V_{\text{local\_snapshot}}, \mathcal{P} = \{ (p, gen_p) \mid \text{dataChannelState}(p) === \text{'open'} \} \Big)$$
4. **Behavior on Local Vector Advancement:**
   Between calculation and commit, the local engine may apply new operations, advancing $V_{\text{local}}$ ($V_{\text{local}}'[p] \ge V_{\text{local\_snapshot}}[p]$).
   Because $V_{\text{local}}$ only advances monotonically during normal playback, the previously calculated $V_{\text{epoch\_safe}}[p] \le V_{\text{local\_snapshot}}[p] \le V_{\text{local}}'[p]$ remains safely $\le V_{\text{local}}'[p]$.
5. **Pre-Commit Revalidation:**
   Immediately prior to committing `compactAtFrontier(V_epoch_safe)`:
   - Re-read active epoch: `assert(engine.epoch === V.epoch)`.
   - Re-read connected peer set in `PeerManager`: `assert(currentConnected === V.P)`.
   - Re-read local vector: `assert(V_epoch_safe[p] <= engine.replicationVector[p] for all p)`.
   - If ANY check fails (epoch changed, peer dropped/reconnected, generation changed, or local vector rolled back):
     **ABORT COMPACTION IMMEDIATELY.**
6. **Incomplete Digest View:**
   If any peer in $\mathcal{P}$ has not supplied a current, authenticated `STATE_DIGEST` for the active epoch and connection generation:
   - View $\mathcal{V}$ is **INCOMPLETE**.
   - `V_epoch_safe` **CANNOT** be computed over $\mathcal{P}$.
   - Compaction at `V_epoch_safe` is **HELD**.

### 4.3 The Three Compaction Frontiers
```
+----------------------------------------------------------------------------------------------------+
| 1. Local Compaction Candidate (V_local_cand):                                                      |
|    Operations older than capacity K=256 that the local peer prunes for local memory bounds.        |
|    SAFETY: Purely local. Provides zero safety guarantees for remote peers.                         |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼
+----------------------------------------------------------------------------------------------------+
| 2. Membership-Scoped Safe Frontier (V_epoch_safe):                                                 |
|    Component-wise GLB of V_local AND all connected peers in view V:                                |
|    V_epoch_safe[p] = min( V_local[p], min_{r in P} D_r.replicationVector[p] )                      |
|    SAFETY: Safe ONLY for peers actively connected in P during epoch E_local.                       |
|    LIMITATION: Does NOT protect disconnected, partitioned, or newly joining peers.                |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼
+----------------------------------------------------------------------------------------------------+
| 3. Globally Agreed Stable Frontier (V_stable):                                                     |
|    A frontier certified by an atomic consensus round or total-order commit across all peers.       |
|    STATUS IN SYNCPLAY: DOES NOT EXIST. SyncPlay is leaderless without Raft/Paxos rounds.          |
+----------------------------------------------------------------------------------------------------+
```

---

## 5. Historical Compatibility Proof: The Checkpoint Commitment Reality

### 5.1 What Does a Phase 7.3 Operation Fingerprint Actually Commit To?
Inspection of `packages/sync-engine/src/log/operation-fingerprint.ts` shows:
```typescript
const preimage = {
  originPeerId: op.originPeerId,
  originSequenceNumber: op.originSequenceNumber,
  logicalEventId: op.logicalEventId,
  logicalTimestamp: op.logicalTimestamp,
  causalContext: op.causalContext,
  messageType: op.messageType,
  payload: op.payload,
  membershipEpoch: op.membershipEpoch ?? 1,
};
return sha256(canonicalizeJson(preimage));
```

**Formal Findings:**
1. `op.causalContext` contains only integer sequence watermarks: `{ [peerId]: sequenceNumber }`.
2. `computeOperationFingerprint` **does NOT chain previous operation hashes** ($H_k \ne \text{hash}(H_{k-1}, op_k)$).
3. A fingerprint at sequence $k$ commits strictly to the identity of **that single operation at sequence $k$** and the sequence watermarks in its causal context.
4. **Equality of one operation fingerprint at sequence $k$ DOES NOT prove equality of preceding operations $i < k$.**

### 5.2 Concrete Counterexample: Concealed Intermediate Divergence
Consider origin $P$ authoring three operations in two divergent network partitions:
- **Partition 1:** $op_1 = \text{PAUSE}$, $op_2 = \text{PLAY}$, $op_3 = \text{SEEK(100)}$ with `causalContext: { P: 2 }`.
- **Partition 2:** $op_1 = \text{RATE\_CHANGE(2.0)}$ *(Divergent!)*, $op_2 = \text{PLAY}$, $op_3 = \text{SEEK(100)}$ with `causalContext: { P: 2 }`.

At sequence 3, both partitions produced an operation $op_3$ with identical fields:
`{ originPeerId: 'P', originSequenceNumber: 3, logicalEventId: 'seek-100', logicalTimestamp: 10, causalContext: { P: 2 }, messageType: 'SEEK', payload: { positionMs: 100 }, membershipEpoch: 1 }`.

If the compatibility check evaluates **only** $k = \min(V_1[P], V_2[P]) = 3$:
$$\text{fingerprint}_1(P, 3) === \text{fingerprint}_2(P, 3)$$
The fingerprints at sequence 3 match bit-for-bit, yet at sequence 1 the partitions executed conflicting operations (`PAUSE` vs `RATE_CHANGE`). Evaluating only sequence $k$ falsely claims causal compatibility!

### 5.3 The Four Historical Verification Outcomes
To provide mathematically defensible recovery, Phase 7.5 evaluates the common prefix $[1, k]$ across all common origins and produces one of four explicit outcomes:

```
                                  Common Prefix Scan [1, k]
                                              │
               ┌──────────────────────────────┴──────────────────────────────┐
               ▼                                                             ▼
     All Fingerprints Match?                                       Any Fingerprint Differs?
               │                                                             │
        ┌──────┴──────┐                                                      ▼
       Yes            No                                            HISTORY_CONFLICT
        │              │                                          (Snapshot REJECTED)
        ▼              ▼
Any Gaps in Prefix?  Missing Proof Available
        │            from Active Mesh Logs?
   ┌────┴────┐         ┌───────────┴───────────┐
  No        Yes       Yes                      No
   │          │        │                        │
   ▼          ▼        ▼                        ▼
HISTORY_  HISTORY_ INCOMPLETE              HISTORY_UNAVAILABLE
VERIFIED  (REQUIRES_HISTORY_REPAIR)       (Permanent horizon breach ->
(ELIGIBLE)                                 DEGRADED)
```

1. **`HISTORY_VERIFIED`:**
   For all common origins $p$, for every sequence $i \in [1, \min(V_L[p], V_S[p])]$, retained checkpoints or stored operations exist on both sides and their fingerprints match bit-exact. Causal compatibility is proven. **Outcome: Snapshot is ELIGIBLE.**
2. **`HISTORY_CONFLICT`:**
   At any common sequence $i \in [1, k]$, a retained checkpoint or stored operation on the snapshot side has a fingerprint that differs from the local checkpoint or stored operation. A history fork is detected. **Outcome: Snapshot is REJECTED.**
3. **`HISTORY_INCOMPLETE`:**
   Checkpoints match where present, but intermediate sequence checkpoints in $[1, k]$ are missing from either local or remote retained checkpoints, **AND** connected suppliers still retain the missing operations or checkpoints in their active logs or checkpoints tables.
   - Missing proof may still be retrieved from the active mesh.
   - **Outcome: REQUIRES_HISTORY_REPAIR.**
4. **`HISTORY_UNAVAILABLE`:**
   Intermediate sequence checkpoints are missing on either side, **AND** no connected supplier retains the missing operations or checkpoints (all suppliers have compacted past the gap without retaining checkpoints for those sequences, or return `OUTSIDE_HORIZON`).
   - Phase 7.4 log repair cannot retrieve compacted history. Proof is permanently unavailable across the mesh.
   - **Outcome: Immediate transition to DEGRADED.** Causal convergence cannot be mathematically proven; the engine halts automated adoption.

---

## 6. Snapshot Epoch Compatibility & Fencing

### 6.1 Reconciling Epoch Fencing with Historical Snapshots
In Phase 7.3-R3, live operation broadcasts are strictly epoch-fenced: an operation authored in epoch $E_1$ is discarded by peers in epoch $E_2 > E_1$.

However, state snapshots represent compacted historical bases. A room that has been idle in epoch 1 may transition to epoch 2 due to a member leaving. Discarding the epoch 1 snapshot would discard the only record of historical state.

### 6.2 Mechanically Testable Predicate for Older-Epoch Snapshots
A snapshot $S$ carrying $S.\text{membershipEpoch} < E_{\text{local}}$ is eligible for installation if and only if all four mechanical conditions hold:

1. **Strict Session Identity Binding:**
   $$S.\text{sessionId} === \text{engine.sessionId}$$
   Cross-session snapshots are unconditionally rejected.
2. **Current Transport Envelope Currency:**
   The snapshot must be served inside recovery wire envelopes (`SNAPSHOT_OFFER`, `SNAPSHOT_REQUEST`, `SNAPSHOT_CHUNK`) that carry the **CURRENT active epoch** ($E_{\text{local}}$) and current connection generation. Stale transport messages carrying epoch $< E_{\text{local}}$ are dropped at the transport boundary.
3. **Current Supplier Certification:**
   The supplier must certify in its current-epoch `SNAPSHOT_OFFER` (signed and sent at $E_{\text{local}}$) that $S$ represents its active, committed base snapshot.
4. **Exact Mechanical Non-Supersession Predicate:**
   $$\forall p \in \text{origins}(V_L): \quad V_L[p] \le S.V_S[p]$$
   AND
   $$\forall op \in \text{localOperationLog}: \quad op.\text{membershipEpoch} > S.\text{membershipEpoch} \implies op.\text{originSequenceNumber} \le S.V_S[op.\text{originPeerId}]$$
   *(If ANY operation exists in the local log authored in an epoch $> S.\text{membershipEpoch}$ with a sequence exceeding $S.V_S$, the local peer has already advanced beyond the snapshot in a newer epoch. $S$ is strictly **REJECTED** as an obsolete historical branch).*

**M5 Guardrail:**
If the local engine cannot mechanically evaluate this predicate, or if older-epoch snapshots cannot be verified until M5 installer rules are implemented:
The snapshot is **classified as a historical candidate that CANNOT be installed automatically until the M5 snapshot-installer rules are implemented and verified**. It halts in `RECOVERING` or transitions to `DEGRADED`.

---

## 7. Exact Transport Sizing & Iterative Send Boundary

### 7.1 Initial Heuristic vs Normative Send Loop
`calculateDynamicChunkSize(sctpMaxMessageSize)` serves solely as an **initial estimation heuristic** used to slice binary buffers.

The **normative transport requirement** reuses Phase 7.4 M5's send boundary via an exact 7-step iterative loop:
1. Slice binary chunk payload (size $\le \text{estimatedChunkSize}$).
2. Construct full `ProtocolMessage<'SNAPSHOT_CHUNK'>`.
3. Serialize via `JSON.stringify()` to `wireJson`.
4. Measure exact UTF-8 byte length: `new TextEncoder().encode(wireJson).byteLength`.
5. Compare against $\text{effectiveLimit} = \min(65536, \text{sctpLimit} \text{ if } > 0 \text{ else } 65536)$.
6. If `byteLength <= effectiveLimit`:
   - Pass `wireJson` unchanged to `PeerManager.sendSerialized()`.
   - `PeerManager` checks generation and DataChannel state immediately prior to `RTCDataChannel.send()`.
7. If `byteLength > effectiveLimit`:
   - Calculate overshoot $\Delta = \lceil (\text{byteLength} - \text{effectiveLimit}) \times 0.75 \rceil + 32$.
   - Reduce chunk binary slice and repeat from Step 2.
   - If chunk binary size drops below `MIN_CHUNK_BYTES` (256 bytes) without fitting: abort with `TRANSPORT_LIMIT_EXCEEDED` $\to$ `DEGRADED`.

---

## 8. Metadata and Lifecycle Consistency

### 8.1 Reinstatement of Exact Phase 7.2 Invariant
The exact Phase 7.2-R7 / R9 Lamport snapshot invariant is reinstated:
$$S.\text{maxLogicalTimestamp} \equiv \max_{p \in \text{origins}(V_S)} S.\text{originFrontierTimestamps}[p]$$
*(For empty frontiers, $\text{maxLogicalTimestamp} = 0$.)*

### 8.2 Cross-Metadata Consistency Checks
Candidate snapshot $S$ must pass all cross-metadata consistency rules:
1. $\text{origins}(V_S) == \text{origins}(FT_S)$ for all origins where $V_S[p] > 0$.
2. $\forall p: FT_S[p] \le S.\text{maxLogicalTimestamp}$, with at least one origin attaining the maximum.
3. $\forall (p, s) \in CP_S: s \le V_S[p]$ and $CP_S(p, s).\text{logicalTimestamp} \le FT_S[p]$.
4. Upon installation, local $V_L$ and `localCausalContext` are both updated to $V_S$.

### 8.3 Authorship vs Supplier Identity
- **`authorPeerId`:** Immutable author of base snapshot.
- **`supplierPeerId`:** Active connected peer transmitting chunks. A supplier may serve its own snapshot or forward a snapshot authored by another peer.

### 8.4 Transition from `RECONCILING` to `ACTIVE`
The engine transitions from `RECONCILING` to `ACTIVE` if and only if:
1. `QuarantineBuffer` is completely drained ($0$ pending operations).
2. All drained operations have been causally applied or filtered.
3. Any sequence gaps between $V_S$ and live events have been resolved via Phase 7.4 anti-entropy.
4. Engine replication vector is contiguous with zero unfulfilled dependencies.

---

## 9. Atomic Snapshot Installation & Staged Clock Replacement Audit

### 9.1 Audit of `LogicalClockManager`
Inspection of `packages/sync-engine/src/clock/logical-clock-manager.ts` demonstrates:
```typescript
public reseed(maxObservedClock: number): void {
  if (!Number.isSafeInteger(maxObservedClock) || maxObservedClock < 0) {
    throw new SyncEngineError('INVALID_CLOCK', `Malformed reseed timestamp: ${maxObservedClock}`);
  }
  this.currentClock = Math.max(this.currentClock, Math.floor(maxObservedClock));
  this.originRateStates.clear();
}
```

**Audit Findings for Atomic Replacement:**
1. **Can a new staged clock manager be constructed out-of-band?**
   **YES.** A new `LogicalClockManager` can be constructed out-of-band:
   ```typescript
   const stagedClock = new LogicalClockManager(Math.max(this.clockManager.clock, snapshot.maxLogicalTimestamp));
   for (const [peer, ts] of Object.entries(snapshot.originFrontierTimestamps)) {
     stagedClock.setOriginClock(peer, ts);
   }
   ```
2. **Atomic Reference Swap Feasibility in M5:**
   In `LeaderlessSyncEngine`, `clockManager` is currently declared as `private readonly clockManager`.
   In M5 implementation:
   - Option A: Make `clockManager` non-readonly (`private clockManager: LogicalClockManager`) allowing `this.clockManager = stagedClock` in the synchronous atomic swap.
   - Option B: If keeping the reference, reseeding `this.clockManager.reseed(snapshot.maxLogicalTimestamp)` is **guaranteed safe and cannot throw** because `snapshot.maxLogicalTimestamp` is validated in Stage 1 to be a safe non-negative integer ($\le \text{MAX\_SAFE\_INTEGER}$). Saving `const previousClock = this.clockManager.clock` before reseeding permits 100% exact rollback if downstream steps fail.

### 9.2 Complete Mutable Engine-State Inventory (14 Components)
The atomic installation procedure must stage or restore all 14 mutable components:
1. `state`: `PlaybackState` (status, anchorPositionMs, anchorLocalTimestamp, playbackRate, stateVersion, lastAppliedEvent)
2. `clockManager`: Lamport clock (`currentClock`, `maxObservedByOrigin`, `originRateStates`)
3. `replicationVector`: `Record<string, number>`
4. `localCausalContext`: `Record<string, number>`
5. `baseSnapshot`: `PlaybackState | undefined`
6. `baseSnapshotFrontier`: `Record<string, number>`
7. `operationLog`: `OrderedOperationLog` (operations array, idIndex, originSeqIndex, compactedCheckpoints map, originFrontierTimestamps map)
8. `pendingQueue`: `PendingDependencyQueue` (buffered out-of-order events)
9. `dedupCache`: deduplication cache
10. `lastDecision`: `EngineDecision`
11. `mutableStats`: telemetry counters
12. `recoveryState`: `RecoveryState` (`ACTIVE`, `RECOVERING`, `RECONCILING`, `DEGRADED`)
13. `quarantineBuffer`: `QuarantineBuffer`
14. `stateChangeListeners`: array of external callback listeners (deferred until `ACTIVE`)

### 9.3 Deferred Listener Dispatch
Listeners (`this.stateChangeListeners`) are **NEVER notified during staging, reference swap, or reconciliation**. They are dispatched strictly after `QuarantineBuffer` has completely drained and the engine has safely transitioned to `ACTIVE`.
