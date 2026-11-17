# Phase 7.2-R2 — Final Consistency, Membership & Recovery Review

> **Document Version:** 1.0.0  
> **Status:** AUTHORITATIVE ARCHITECTURAL SPECIFICATION (Revision P7.2-R2)  
> **Baseline Commit:** `55e3240`  
> **Mode:** Specification & Architecture Only — Strict Stop Condition Observed  
> **Target Subsystems:** `@syncplay/sync-engine`, `@syncplay/protocol`, `@syncplay/control-events`, `@syncplay/peer-mesh`  

---

## 1. Executive Summary & Purpose

During the final architectural review (Revision P7.2-R2), four critical areas requiring mathematical and semantic precision were resolved:
1. **Separation of Causal Context and Local Replication State:** Explicit delineation between the immutable, author-stamped `causalContext` and the local, contiguous `replicationVector`, with bounded gap tracking.
2. **Membership Fencing and Offline Partition Semantics:** Adoption of the **Strict Epoch Fencing with Snapshot Catch-Up** policy for partitioned peers generating offline events, delineating Bootstrap membership coordination from non-authoritative playback.
3. **Snapshot Integrity versus Semantic Consistency:** Clarification of what SHA-256 proves (byte-level integrity only) versus semantic consistency, RFC 8785 canonical serialization, and deterministic tie-breaking for incomparable version vectors.
4. **Exact Lamport Clock Rules:** Rigorous mathematical specification of event creation ($L(e) = 1 + \max(L_{\text{local}}, \max_{\text{deps}} L)$), causal dependency validation, per-origin monotonicity, rate-based inflation detection, and `Number.MAX_SAFE_INTEGER` safety.

This document serves as the authoritative final correction record for **Revision P7.2-R2**.

---

## 2. Comprehensive Correction Register (P7.2-R2)

---

### Correction A: Separation of Causal Context and Replication State

#### 1. Independent Definitions & Meanings

| Concept | Structure | Ownership & Mutability | Exact Semantic Meaning |
| :--- | :--- | :--- | :--- |
| **Event Causal Context** | `causalContext: Record<originPeerId, number>` | Stamped by operation author at creation instant; **immutable**. | Highest consecutive sequence number from each origin that the author had received, validated, and reduced into its local engine state prior to generating this operation. |
| **Local Replication Vector** | `replicationVector: Record<originPeerId, number>` | Maintained locally by each node; **mutable**. | Highest **consecutive** sequence number from each origin that the local node has received, validated, and applied to its engine state and operation log. |

#### 2. Advancement, Gaps, and Out-of-Order Handling
- **Contiguous Advancement Invariant:** `replicationVector[p]` advances **if and only if** operation $(p, s)$ has been:
  1. Received over transport,
  2. Structurally and cryptographically validated,
  3. Causally unblocked (all operations in its `causalContext` are already applied), AND
  4. Applied to the engine's operation log and state reducer.
- **Out-of-Order Events Do NOT Advance the Vector:**
  - *Example:* If a peer receives $A_1$ and $A_3$ without $A_2$, `replicationVector[A]` remains $1$.
  - Under no circumstances does the node advertise $A_3$ in `replicationVector[A]`. Advertising $A_3$ would falsely signal to peers that $A_2$ has been received, corrupting stable compaction.
- **Bounded Gap Tracking (`PendingDependencyQueue` & `SparseGapTracker`):**
  - Out-of-order operation $A_3$ is buffered in `PendingDependencyQueue` (capacity bounded by $K = 256$).
  - Gaps are represented locally by `SparseGapTracker`: tracking the missing sequence range $[\text{replicationVector}[p] + 1, \text{highestBufferedSeq}]$.
  - A targeted `REPAIR_REQUEST` is immediately dispatched for the missing slice $[A_2]$.
  - When $A_2$ arrives and is applied, `replicationVector[A]` cascades forward to $3$.

#### 3. Representation of Compacted History
- The compacted baseline is represented by `baseSnapshot.snapshotFrontier: Record<originPeerId, number>`.
- **Compaction Invariant:**
  $$V_{\text{baseSnap}}[p] \le \text{replicationVector}[p] \quad \forall p$$
- Any operation with sequence $s \le V_{\text{baseSnap}}[p]$ is known to be subsumed into the base state.

#### 4. Causal Dependency Checking Rules
When receiving an incoming operation $e$ from origin $o$:
- For every origin $p \in \text{keys}(e.\text{causalContext})$:
  - If $e.\text{causalContext}[p] \le \text{replicationVector}[p]$, the dependency is satisfied.
  - If $e.\text{causalContext}[p] > \text{replicationVector}[p]$, the dependency is missing.
- **Handling Missing Dependencies:** The operation $e$ **MUST REMAIN PENDING** in `PendingDependencyQueue`. It is **NOT** rejected. The node dispatches a `REPAIR_REQUEST` for sequence range $[\text{replicationVector}[p] + 1, e.\text{causalContext}[p]]$.

#### 5. Obsolete Identities and Membership Epochs in Dependencies
- If $e.\text{causalContext}$ references a departed peer $D$ that was active in a prior epoch, the dependency is satisfied if $e.\text{causalContext}[D] \le V_{\text{baseSnap}}[D]$.
- If $D$'s sequence exceeds $V_{\text{baseSnap}}[D]$ and $D$ has departed, the operation is requested via anti-entropy from active peers who were present in that epoch.

---

### Correction B: Membership Fencing & Offline Partition Semantics

#### 1. Partition Scenario Analysis
Consider the scenario:
1. Peers A, B, and C are active in `membershipEpoch: 5`.
2. Peer C becomes partitioned from the network.
3. Peers A and B observe C's departure via Bootstrap signaling, advance to `membershipEpoch: 6`, and compact history.
4. Peer C continues playing and generates control operations under `membershipEpoch: 5`.
5. Peer C heals its partition and reconnects to the room.

#### 2. Policy Selection: Strict Epoch Fencing with Snapshot Catch-Up (Selected MVP Policy)
- **Policy Definition:** Control operations carry the author's active `membershipEpoch`. Operations stamped with an obsolete epoch ($e.\text{membershipEpoch} < \text{activeEpoch}$) are **FENCED AND REJECTED** upon reconnection with error code `REJECTED_OBSOLETE_EPOCH`.
- **Explicit Architectural Trade-Off:**
  - *Trade-off:* Control intents generated by a partitioned peer during network isolation are **discarded**.
  - *Justification:* In a real-time collaborative media player, accepting offline playback operations generated minutes earlier causes catastrophic UX regressions (e.g., video suddenly jumping backward to 00:05 while active participants watched until 35:00). Furthermore, supporting offline reconciliation after history has been compacted requires unbounded operation logs, violating the fundamental $K=256$ bounded memory invariant.
- **Recovery Procedure for Rejoining Peer C:**
  1. Peer C connects to Bootstrap and receives the authoritative active roster with `membershipEpoch: 6`.
  2. Peer C detects $\text{localEpoch} (5) < \text{meshEpoch} (6)$.
  3. Peer C purges its uncommitted speculative offline operations.
  4. Peer C transitions its engine status to `'recovering'`.
  5. Peer C dispatches `STATE_REQUEST` to an active peer (A or B) and installs `StateSnapshotPayloadV2` of epoch 6, fast-forwarding cleanly to the active room state.

#### 3. Membership Authority & Bootstrap Invariants
- **Who creates epochs:** The Bootstrap Server coordinates room membership rosters and monotonically increments `membershipEpoch` strictly on membership mutations (`peer-joined`, `peer-left`, `peer-timeout`).
- **Bootstrap Non-Authoritative Invariant:** Bootstrap is strictly a signaling and membership coordinator. It **never** receives playback control events, inspects media positions, assigns logical timestamps, or acts as a playback sequencer.
- **Bootstrap Unavailability Handling:**
  - If the Bootstrap Server crashes or becomes unreachable, **active peers continue uninterrupted playback and distributed synchronization** over established WebRTC DataChannels.
  - While Bootstrap is offline, no new peers can join, and membership epochs remain frozen.
  - History compaction over connected peers continues normally. If a peer drops its DataChannel while Bootstrap is down, surviving peers detect WebRTC channel closure, treat the peer as disconnected, and maintain history up to $K=256$.
- **Peer-Left vs. Consensus Decisions:** A `session:peer-left` notification from Bootstrap is a transport liveness event, not a playback state consensus decision. It updates the active peer set used to compute the stable compaction frontier $W_{\text{stable}}$.

---

### Correction C: Snapshot Integrity versus Semantic Consistency

#### 1. What Each Verification Layer Proves

| Verification Mechanism | What It Proves | What It Does NOT Prove |
| :--- | :--- | :--- |
| **`stateDigestSha256`** | **Byte-Level Integrity:** Proves that the received snapshot payload bytes have not suffered in-transit bit rot or tampering since generation by the author. | Does **NOT** prove that the snapshot's playback state (`anchorPositionMs`, `status`) was truthfully or correctly calculated from history. |
| **Transport Envelope Context** | **Provenance:** Proves the immediate sender and author peer ID verified by WebRTC DTLS and session tokens. | Does not prove the author's local engine logic is bug-free. |
| **`snapshotFrontier`** | **Event-Set Completeness:** Proves the exact sequence boundary from each origin that is claimed to be folded into the base state. | Does not prove that the state corresponds to those events without reduction. |
| **Semantic Validation Rules** | **Semantic Plausibility:** Validates finite non-negative anchor positions, playback rates in $[minRate, maxRate]$, and valid monotonic epochs. | Does not replace full deterministic replay from origin zero. |

#### 2. Canonical Serialization (RFC 8785)
To ensure identical SHA-256 digests across diverse browser JavaScript engines, snapshots are serialized using the JSON Canonicalization Scheme (RFC 8785):
- Object keys sorted lexicographically by UTF-16 code units.
- Whitespace strictly removed.
- Digested Fields:
  $$\text{Payload} = \{\text{anchorPositionMs}, \text{formatVersion}, \text{maxLogicalTimestamp}, \text{membershipEpoch}, \text{playbackRate}, \text{sessionId}, \text{snapshotAuthorPeerId}, \text{snapshotFrontier}, \text{status}\}$$

#### 3. Handling Incomparable Version Vectors ($V_A \not\ge V_B \land V_B \not\ge V_A$)
- **Strict Prohibition:** Nodes **MUST NEVER** compare snapshots by summing version vector entries ($\sum V[p]$). Summing sequence numbers across independent origins has zero causal meaning.
- **Resolution Procedure (Updated in P7.2-R3):**
  1. If two active peers offer incomparable snapshot frontiers, the receiver requests missing operation logs from both peers to bridge the concurrency gap.
  2. If the gap cannot be bridged via log replay (history compacted beyond $K=256$), the engine **MUST NOT silently pick a winner**. *(Note: The previous R2 tie-breaker $\max(\text{epoch}) \to \max(L) \to \max_{\text{lex}}(\text{peerId})$ is REMOVED in P7.2-R3 as mathematically unsafe).*
  3. The engine transitions to `DEGRADED_OUT_OF_SYNC`, sets `EngineStatus.isDegraded = true` and `degradedReason = 'INCOMPARABLE_SNAPSHOT_DIVERGENCE'`, and prompts the user/host to perform an explicit authoritative manual re-seek or room reload.

---

### Correction D: Exact Lamport Clock Rules & Validation

#### 1. Universal Event Creation Rule
When any peer originates a new control event $e$:
$$L(e) = 1 + \max\left(L_{\text{local}}, \max_{d \in \text{dependencies}} L(d)\right)$$

- **First Event from an Origin:** $L(e) = 1 + L_{\text{local}}$, sequence $= 1$.
- **Normal Local Event Creation:** $L(e) = 1 + L_{\text{local}}$, sequence $= \text{seq}_{\text{local}} + 1$.
- **Remote Event Reception:**
  $$L_{\text{local}} \leftarrow 1 + \max(L_{\text{local}}, L(e))$$

#### 2. Comprehensive Validation & Monotonicity Rules
1. **Per-Origin Monotonicity:** Successive operations from origin $p$ must satisfy:
   $$L(e_{k+1}) > L(e_k) \quad \land \quad \text{originSequence}_{k+1} = \text{originSequence}_k + 1$$
2. **Causal Timestamp Integrity:** For every dependency $d$ in $e.\text{causalContext}$:
   $$L(d) < L(e)$$
   If $L(e) \le L(d)$, the event violates causal ordering and is rejected with `INVALID_CAUSAL_TIMESTAMP`.
3. **Duplicate Sequence Handling:** If $e.\text{seq} \le \text{replicationVector}[e.\text{originPeerId}]$:
   - If $e.\text{logicalEventId} == \text{storedOperation}.\text{logicalEventId}$, resolve as `IGNORED_DUPLICATE` (idempotent NO-OP).
   - If $e.\text{logicalEventId} \neq \text{storedOperation}.\text{logicalEventId}$, reject with `SEQUENCE_COLLISION_DETECTED` (critical protocol defect; isolates remote peer).
4. **Elimination of Fixed Clock-Skew Thresholds:** The arbitrary `MAX_CLOCK_SKEW = 10,000` rule is completely eliminated. Silent peers may legitimately lag by arbitrary Lamport ticks.
5. **Rate-Based Clock Inflation Detection (Refined in P7.2-R3):**
   - Applies **only to live broadcasts**; repair batches (`OPERATION_BATCH`) and log replays are exempt.
   - Evaluated via a per-origin token bucket: refill rate $10\text{ tokens/sec}$, burst capacity $50\text{ tokens}$.
   - Suspicious events are rejected with `REJECTED_CLOCK_INFLATION`.
   - **Crucial Invariant:** Rejected events **never advance the local Lamport clock**.
   - Explicitly recognized as a heuristic sanity filter against runaway loops, not a cryptographic security guarantee.
6. **Safe Integer Overflow Prevention:** If $L \ge \text{Number.MAX\_SAFE\_INTEGER} - 1$ ($2^{53} - 1$), the engine throws `CLOCK_OVERFLOW`, enters degraded mode, and initiates room-wide snapshot re-anchoring.

---

## 3. Conflict Semantics & Conditional Convergence Revalidation

### 3.1. Concurrency-Aware PAUSE Priority Revalidation
Using the finalized `causalContext: Record<originPeerId, number>`:
- **Causally Concurrent ($e_{\text{pause}} \parallel e_{\text{play}}$):**
  $$e_{\text{pause}}.\text{seq} > e_{\text{play}}.\text{causalContext}[p_{\text{pause}}] \quad \land \quad e_{\text{play}}.\text{seq} > e_{\text{pause}}.\text{causalContext}[p_{\text{play}}]$$
  **PAUSE takes priority.** State transitions to `'paused'` and halts at the pause anchor position, eliminating accidental audio blast hazards.
- **Causally Sequential ($e_{\text{pause}} \to e_{\text{play}}$):**
  $$e_{\text{pause}}.\text{seq} \le e_{\text{play}}.\text{causalContext}[p_{\text{pause}}]$$
  User observed the pause and deliberately pressed play. **PLAY succeeds**; state transitions to `'playing'`.
- **Causally Sequential ($e_{\text{play}} \to e_{\text{pause}}$):** User observed play and pressed pause. **PAUSE succeeds**; state transitions to `'paused'`.
- **Media Hardware Isolation:** The Phase 7 `SyncEngine` outputs purely mathematical `PlaybackState` decisions. Media muting, Web Audio volume ramping, and HTML5 element interaction remain strictly isolated in Phase 8 (`MediaAdapter`).

### 3.2. Formally Guarded Conditional Convergence Statement (Corrected in P7.2-R3)
A set of distributed SyncPlay replicas $\mathcal{R}$ participating in session $\mathcal{S}$ within active membership epoch $E$ will eventually converge to identical `PlaybackState` under the following conditions:
1. **Connected Communication Graph:** Open WebRTC DataChannels or active anti-entropy repair paths exist between all replicas in $\mathcal{R}$.
2. **Consistent Membership Epoch & Fencing:** Replicas agree on the active `membershipEpoch` issued by Bootstrap; operations generated under obsolete epochs are strictly fenced and discarded.
3. **Identical Accepted Event Sets:** Every replica in $\mathcal{R}$ eventually receives and admits into its log the complete set of valid, non-fenced operations originated in epoch $E$.
4. **Causal Dependency Admissibility:** No replica applies an operation until all operations declared in its `causalContext` have been received and applied.
5. **Deterministic Total Ordering & Reduction:** Replicas order accepted operations using the deterministic key $\langle L(e), \text{originPeerId}(e), \text{logicalEventId}(e) \rangle$ and reduce them via the pure functional reducer.
6. **Successful History Recovery:** Any replica with missing history either repairs gaps within the retained horizon $K = 256$ or successfully imports an authoritative snapshot dominating its local history.
7. **Eventual Quiescence:** Replicas enter a period of quiescence where no new control operations are generated, allowing anti-entropy digests and pending repair requests to settle.

*(Note: The phrase "if and only if" and the expression $\tau \ge 2\Delta + \text{digestInterval} + \text{repairRTT}$ were removed in P7.2-R3; the timing duration is an engineering/test assumption, not a standalone proof).*

---

## 4. Document Update & Traceability Matrix (P7.2-R2 / R3)

| Document | Primary Revisions Applied in P7.2-R2 / R3 |
| :--- | :--- |
| `docs/PHASE7_2_R3_FINAL_CORRECTIONS.md` | Final R3 specification resolving tie-breakers, no-forwarding identity, token-bucket inflation, and conditional convergence. |
| `docs/PHASE7_2_HLD.md` | Delineated `causalContext` vs `replicationVector`; added Strict No-Forwarding policy, Strict Epoch Fencing, and Bootstrap non-authoritative invariants. |
| `docs/PHASE7_2_LLD.md` | Removed snapshot tie-breaker; formalized contiguous advancement, `SparseGapTracker`, token-bucket Lamport checks, and RFC 8785 canonical digest. |
| `docs/PHASE7_2_PROTOCOL_EVOLUTION_RFC.md` | Updated envelope and payload definitions for `originPeerId === senderPeerId` no-forwarding, `replicationVector` in digests, and canonical snapshot serialization. |
| `docs/design/PHASE7_INTERFACE_CONTRACTS.md` | Updated `ILogicalClockManager`, `IOrderedOperationLog`, `IReplicationController`, and `IRecoveryManager` contracts with R3 semantics. |
| `docs/PHASE7_2_SIMULATION_TEST_STRATEGY.md` | Updated suites to test `DEGRADED_OUT_OF_SYNC` on incomparable snapshots and token-bucket clock inflation. |
| `docs/PHASE7_2_COMPATIBILITY_AND_MIGRATION.md` | Documented strict no-forwarding limitation, offline event discard trade-off, and degraded recovery paths. |
| `docs/PHASE7_2_FINAL_DESIGN_REVIEW.md` | Updated decisions AD-701 to AD-710 to reflect R3 refinements and verified acceptance criteria. |
| `docs/PHASE7_2_ARCHITECTURAL_CORRECTIONS.md` | Cross-referenced P7.2-R3 additions and updated correction register. |
| `docs/design/README.md` | Added `PHASE7_2_R2_FINAL_CORRECTIONS.md` to design index. |

