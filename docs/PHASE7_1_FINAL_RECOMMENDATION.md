# Phase 7.1 — Final Architectural Recommendation & Decisions

> **Document Version:** 1.0.0  
> **Status:** COMPLETED & VERIFIED (Milestone P7.1 Final Deliverable)  
> **Context:** SyncPlay Protocol — Phase 7 Leaderless Distributed Synchronization Engine  

---

## 1. Explicit Answers to the 10 Core Architectural Questions

### 1. Which ordering mechanism should SyncPlay use?
**Decision:** **Lamport Logical Clocks with Deterministic Lexicographic Tie-Breaking.**
- Every operation is ordered by the composite key:
  $$\text{Key}(e) = \langle \text{logicalTimestamp}(e), \text{senderPeerId}(e), \text{logicalEventId}(e) \rangle$$
- **Justification:** Establishes an unambiguous, irreflexive, antisymmetric total order across all operations with only 8 bytes of wire overhead and zero coupling to dynamic peer rosters.

### 2. How will concurrent operations be resolved?
**Decision:** **Operation-Specific Deterministic Conflict Policies.**
- **`SEEK` vs. `SEEK`:** Highest total-order key wins (LWW); earlier concurrent seeks are superseded.
- **`PAUSE` vs. `PLAY`:** **Intent-Preserving Pause Priority.** Concurrent pause overrides concurrent play to prevent audio blast hazards.
- **`RATE_CHANGE`:** Total-order scalar LWW within validated $[0.25, 4.0]$ bounds.
- **Identical Operations:** Processed as `NO_OP` if already applied within position epsilon ($\le 5\text{ ms}$).

### 3. How will peers detect missing events?
**Decision:** **Per-Origin Version Vector State Digests (`STATE_DIGEST`).**
- A simple scalar count fails in multi-origin meshes. Peers exchange compact maps of $\text{PeerId} \to \text{SeqNum}$.
- Dispatched reactively upon peer connection / gap observation, and periodically at low frequency ($0.2\text{ Hz}$).

### 4. How will events be repaired?
**Decision:** **Targeted Range Requests (`REPAIR_REQUEST`) over WebRTC DataChannels.**
- A peer observing a sequence gap requests the exact missing range from the origin peer or an active replica holding the log.
- The peer replies with an `OPERATION_BATCH`. Missing operations are inserted into the ring buffer and re-evaluated in total order.

### 5. How will peers recover after reconnecting?
**Decision:** **Generation-Aware Two-Tier Catch-Up.**
- Rejoining peer enters `RECOVERING` status under its incremented `ConnectionGeneration`.
- *Tier 1 (Within Horizon):* If the gap is $< 256$ operations, execute rapid anti-entropy repair.
- *Tier 2 (Beyond Horizon):* If disconnected longer than $K = 256$ operations, request and import an authoritative `StateSnapshotPayload` (`importSnapshot`), fast-forwarding the base anchor cleanly.

### 6. How will history be safely compacted?
**Decision:** **Leaderless Acknowledged Stable Frontier Pruning.**
- Active connected peers report their acknowledged version vectors.
- Operations older than $W_{\text{stable}} = \min_{p \in \text{ActivePeers}}(V_p)$ are folded into the base snapshot and evicted from memory.
- Unresponsive or partitioned peers are evicted from the frontier calculation once the ring buffer hits maximum capacity ($K = 256$).

### 7. What happens during a network partition?
**Decision:** **Optimistic Island Execution with Deterministic Total-Order Reconciliation.**
- SyncPlay chooses **Availability (AP)**: both partitioned islands continue operating locally with an indicator of degraded mesh health.
- Upon partition healing, anti-entropy digest exchange activates. The island with the logically latest operation wins deterministically, pulling both islands back into perfect alignment.

### 8. What guarantees are realistically achievable?
**Decision:** **Strong Eventual Consistency (SEC) under Quiescence.**
- If message delivery quiesces for $\tau > 2\Delta$ and the mesh is non-partitioned, all non-faulty peers converge to identical `PlaybackState`.
- Safety and deduplication invariants $I_1 - I_6$ are strictly guaranteed.
- Frame-perfect audio/video rendering and hardware clock drift compensation are explicitly deferred to Phase 8.

### 9. Which design decisions must be deferred to P7.2?
**Decision:** The following wire-level schema contracts are deferred to Milestone P7.2:
- Exact wire placement of `logicalTimestamp` (envelope field vs. payload metadata).
- Formal Zod v4 schemas for `STATE_DIGEST`, `REPAIR_REQUEST`, and `OPERATION_BATCH`.
- Exact internal TypeScript method signatures for `OrderedOperationLog`.

### 10. What is the simplest viable implementation path?
**Decision:** Implement the five modular components (`LogicalClockManager`, `OrderedOperationLog`, `ConflictResolver`, `ReplicationController`, `RecoveryManager`) behind the existing `SyncEngine` boundary in `packages/sync-engine`, accompanied by a headless permutation test harness.

---

## 2. Rejection of Alternative Designs

### Alternative Design 1: Raft / Paxos Consensus
- **Reason for Rejection:** Requires majority quorums ($\lfloor N/2 \rfloor + 1$). In 2-peer rooms (the most common use case), if one peer drops, quorum is lost and the remaining peer is completely blocked from playing or pausing. Violates Non-Negotiable Principle 1 (No Permanent Leader) and Principle 2 (Server-Minimized).

### Alternative Design 2: Pure CRDT (LWW-Register with Vector Clocks)
- **Reason for Rejection:** Media controls are inherently non-commutative. A pure CRDT approach degrades into an LWW-Register over the entire playback state, which re-introduces the need for a total-order tie-breaker while incurring the dynamic roster resizing overhead of vector clocks on every single control event.

### Alternative Design 3: Centralized Bootstrap Event Sequencer
- **Reason for Rejection:** Routing all playback controls through the Bootstrap Server to establish total order destroys peer-to-peer architecture, introduces server latency bottlenecks, and directly violates Fixed Decision D1.

---

## 3. Explicitly Marked Unresolved Research Questions

The following secondary trade-offs are explicitly marked as **unresolved research questions** to be prototyped and empirically compared in P7.2 / P7.3:
1. **Dynamic Log Horizon Sizing:** Should the ring buffer capacity $K = 256$ be statically fixed, or should it adapt dynamically based on measured RTT and peer churn? (Preliminary: keep static $K=256$ for MVP simplicity).
2. **Audio UX Dampening Threshold:** When a late operation causes a state rollback of less than $200\text{ ms}$, should the UI immediately jump or apply a smooth rate ramp? (Belongs to Phase 8 Media Adapter integration).

---

## 4. Proposed P7.2 Scope & Governance Review Gates

Upon review and approval of P7.1 deliverables, Milestone P7.2 will execute:
1. High-Level Design (`PHASE7_HLD.md`) and Low-Level Design (`PHASE7_LLD.md`).
2. Protocol Evolution RFC: define exact Zod schemas for `STATE_DIGEST`, `REPAIR_REQUEST`, and `OPERATION_BATCH`.
3. Formal TypeScript interface specifications in `docs/design/PHASE7_INTERFACE_CONTRACTS.md`.
4. Headless simulation test harness architecture specification.

**Gate 2 Sign-Off:** Formal user authorization must be obtained after P7.2 before any source code is modified in P7.3.

