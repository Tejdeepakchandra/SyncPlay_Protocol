# Phase 7.2-R1 — Architectural Corrections & Consistency Review

> **Document Version:** 1.0.0  
> **Status:** APPROVED ARCHITECTURAL SPECIFICATION (Revision P7.2-R1)  
> **Baseline Commit:** `9176ed8`  
> **Mode:** Specification & Architecture Only — No Implementation Authorized  
> **Target Subsystems:** `@syncplay/sync-engine`, `@syncplay/protocol`, `@syncplay/control-events`, `@syncplay/peer-mesh`  

---

## 1. Executive Summary & Purpose

During the initial review of Phase 7.2 (`9176ed8`), seven critical architectural issues were identified across causal metadata, per-origin sequencing, membership epochs, safe compaction, snapshot recovery, clock validation, and formal convergence guarantees.

This document serves as the authoritative architectural correction record for **Revision P7.2-R1**. It details each defect, the flawed original design, the mathematically sound corrected design, the technical rationale, and the traceability to updated specifications.

---

## 2. Comprehensive Correction Register

---

### Correction A: Causal Context & True Concurrency Detection

- **Defect Identified:**
  The previous design relied on a single predecessor pointer (`predecessorEventId = latest operation applied by author's engine`). A single predecessor cannot represent concurrent DAG branches.
  *Failure Example:* Peer C receives concurrent events $A_1$ and $B_1$. Peer C creates $C_1$ after observing both. If $C_1$ only references $B_1$, the causal relationship $A_1 \to C_1$ is lost. Any peer receiving $A_1$ and $C_1$ without $B_1$ cannot determine that $C_1$ causally succeeded $A_1$.
- **Original Design:** Scalar Lamport timestamp + single `predecessorEventId?: string`.
- **Corrected Design:** **Explicit Causal Context Vector (`causalContext: Record<string, number>`)** attached to every control operation.
  - Represents the Version Vector snapshot observed by the author at the instant the operation was created:
    $$\text{causalContext}(e) = \text{Map}\langle \text{originPeerId}, \text{originSequence} \rangle$$
  - **Formal Causality Test ($e_1 \to e_2$):**
    $$e_1 \to e_2 \iff e_1.\text{originSequence} \le (e_2.\text{causalContext}[e_1.\text{originPeerId}] \ ?? \ 0)$$
  - **Formal Concurrency Test ($e_1 \parallel e_2$):**
    $$e_1 \parallel e_2 \iff (e_1 \not\to e_2) \land (e_2 \not\to e_1)$$
- **Technical Reasoning:**
  In a small room ($N \le 8$), a causal context mapping 4–8 short peer IDs to small integers requires $\le 120\text{ bytes}$ per control event. Because media control intents occur at human interactive rates ($\le 5\text{ intents/sec}$), this provides exact, mathematically sound concurrency detection without network strain.
- **Traceability:** Enables mathematically sound `PAUSE` priority under true concurrency. Updates `PHASE7_2_HLD.md`, `PHASE7_2_LLD.md`, `PHASE7_2_PROTOCOL_EVOLUTION_RFC.md`, and `PHASE7_INTERFACE_CONTRACTS.md`.

---

### Correction B: Per-Origin Event Sequencing & Identity Model

- **Defect Identified:**
  Ambiguity between network transport identity (`fromPeerId`), logical event identity (`logicalEventId`), and sequence numbering, and lack of an explicit policy for browser reload / sequence restart.
- **Original Design:** `senderPeerId` overloaded for both transport and origin; sequence numbers tied loosely to peers without lifecycle rules.
- **Corrected Design:** Strictly delineated 7-part identity model:
  1. `originPeerId`: Permanent logical author identity within the session/epoch.
  2. `originSequence`: Strictly monotonic positive integer per origin ($1, 2, 3\dots$).
  3. `logicalEventId`: Unique nano-ID identifying the logical user operation, preserved across transmission retries.
  4. `logicalTimestamp`: Lamport clock counter (scalar integer) used for the deterministic total-order key $\langle L, \text{originPeerId}, \text{logicalEventId} \rangle$.
  5. `fromPeerId`: Transport-verified peer identity of the immediate WebRTC hop.
  6. `connectionGeneration`: Monotonic integer from Bootstrap tracking the WebRTC connection lifecycle.
  7. `membershipEpoch`: Monotonic integer tracking the room membership roster configuration.
- **Browser Reload & Ephemeral Identity Policy:**
  In a browser runtime without durable IndexedDB storage, a page reload (F5) wipes in-memory sequence numbers. A reloading peer **MUST obtain a fresh `peerId` from the Bootstrap Server upon rejoining**. It starts sequence numbers at $1$ under the new identity. Its old `peerId` is marked departed via `session:peer-left`. This prevents sequence collisions without requiring complex disk persistence.
- **Traceability:** Updates `PHASE7_2_LLD.md`, `PHASE7_2_PROTOCOL_EVOLUTION_RFC.md`.

---

### Correction C: Membership Epochs & Safe History Compaction

- **Defect Identified:**
  The formula $W_{\text{stable}}[p] = \min_{i \in \text{ActivePeers}}(V_i[p])$ failed during network partitions where peers A and B considered C departed while C still considered itself active. Naive local pruning risked permanent data loss for partitioned peers.
- **Original Design:** Naive component-wise minimum over local active peer list.
- **Corrected Design:** **Epoch-Gated Stable Frontier with Snapshot Fallback.**
  1. **Membership Epochs:** Bootstrap Server manages room membership rosters and increments `membershipEpoch` on every join/leave.
  2. **Active Frontier:** $W_{\text{stable}}$ is calculated strictly over peers confirmed active in the current `membershipEpoch` and connected via open DataChannels.
  3. **Meaning of Acknowledgment:** An acknowledgment in `STATE_DIGEST` means the peer has **received, validated, and applied** the operation into its in-memory engine and log.
  4. **Conservative Compaction & Hard Horizon ($K = 256$):**
     - Operations older than $W_{\text{stable}}$ are folded into the base snapshot and evicted from the ring buffer.
     - If a peer is partitioned or offline longer than $K = 256$ operations, it is evicted from the frontier calculation.
     - **Safety Guarantee:** An excluded peer that returns later is **guaranteed to recover via snapshot** (`importSnapshot`) rather than log replay.
- **Traceability:** Updates `PHASE7_2_HLD.md`, `PHASE7_2_LLD.md`, `PHASE7_2_FINAL_DESIGN_REVIEW.md`.

---

### Correction D: Snapshot Provenance & Integrity Verification

- **Defect Identified:**
  The Phase 6 `importSnapshot()` method accepted snapshots without validating their relationship to the replicated operation history or verifying the claimed frontier. An active peer offering a snapshot was treated as an unverified authority.
- **Original Design:** Basic structural Zod validation of local snapshot payload.
- **Corrected Design:** **Cryptographically Verified Snapshot Handshake.**
  1. `StateSnapshotPayloadV2` includes: `snapshotId`, `sessionId`, `membershipEpoch`, `snapshotAuthorPeerId`, `snapshotFrontier` (Version Vector), `maxLogicalTimestamp`, `stateDigestSha256`, and playback parameters.
  2. **Frontier Verification:** The receiving engine verifies that `snapshotFrontier` dominates or matches known history ($V_{\text{snap}} \ge V_{\text{local}}$). A stale snapshot ($V_{\text{snap}} < V_{\text{local}}$) is rejected immediately.
  3. **Atomic Installation:** Engine re-seeds Lamport clock, resets base snapshot, and replays any locally held operations that are strictly newer than the snapshot frontier ($V > V_{\text{snap}}$).
- **Traceability:** Updates `PHASE7_2_LLD.md`, `PHASE7_2_PROTOCOL_EVOLUTION_RFC.md`, `PHASE7_INTERFACE_CONTRACTS.md`.

---

### Correction E: Bounded History & Explicit Event Rejection Paths

- **Defect Identified:**
  Operations older than the $K = 256$ ring buffer horizon were either silently dropped or caused undefined behavior.
- **Original Design:** Vague rejection without classifying historical state.
- **Corrected Design:** **5-Way Exhaustive Event Horizon Classification:**
  - *Class 1 (Subsumed by Base Snapshot):* $e.\text{originSequence} \le V_{\text{baseSnap}}[e.\text{originPeerId}]$. Resolved as `IGNORED_ALREADY_COMPACTED` (idempotent NO-OP; zero state mutation).
  - *Class 2 (Missing but Recoverable from Log):* Gap detected within recent history. Buffered in `PendingDependencyQueue`; triggers targeted `REPAIR_REQUEST`.
  - *Class 3 (Compacted by Peers & Recoverable via Snapshot):* Gap exceeds connected peers' logs. Engine enters `RECOVERING` status and fetches `StateSnapshotPayload`.
  - *Class 4 (Invalid / Malformed / Conflicting Origin):* Fails schema or sequence regression. Rejected with `REJECTED_INVALID_ORIGIN` or `REJECTED_SEQUENCE_CONFLICT`.
  - *Class 5 (Conflicts with Installed Snapshot Frontier):* Operation predates installed snapshot but contradicts its state. Engine enters `DEGRADED` status and requests authoritative snapshot resynchronization.
- **Traceability:** Updates `PHASE7_2_LLD.md`, `PHASE7_INTERFACE_CONTRACTS.md`.

---

### Correction F: Clock Validation & Concurrency-Aware Conflict Semantics

- **Defect Identified:**
  1. The fixed `MAX_CLOCK_SKEW = 10,000` rule falsely rejected legitimate operations from peers that were active while the receiver was idle.
  2. PAUSE priority was underspecified and could prevent playback from ever resuming.
- **Original Design:** Fixed scalar distance check ($L_r - L_l \le 10,000$); blanket pause priority.
- **Corrected Design:**
  1. **Per-Origin Monotonicity Validation:**
     Replace scalar distance check with per-origin sequence monotonicity ($\text{seq}_{k+1} = \text{seq}_k + 1$), per-origin Lamport monotonicity ($L_{k+1} > L_k$), and safe integer limits ($L \le 2^{53} - 1$).
  2. **Concurrency-Aware PAUSE Priority:**
     - *Causally Concurrent ($e_{\text{pause}} \parallel e_{\text{play}}$ via Causal Context):* `PAUSE` takes precedence. Playback halts to prevent audio blast hazards.
     - *Causally Sequential ($e_{\text{pause}} \to e_{\text{play}}$):* User intentionally clicked play after seeing pause. `PLAY` succeeds.
     - *Causally Sequential ($e_{\text{play}} \to e_{\text{pause}}$):* User intentionally clicked pause after seeing play. `PAUSE` succeeds.
  3. **Media Hardware Isolation:** All audio muting, buffer adjustments, and video element controls remain strictly in Phase 8 (`MediaAdapter`). The Phase 7 engine outputs only pure `PlaybackState` decisions.
- **Traceability:** Updates `PHASE7_2_LLD.md`, `PHASE7_2_CONFLICT_SEMANTICS.md`.

---

### Correction G: Formal Conditional Convergence Guarantee

- **Defect Identified:**
  Previous documents claimed Strong Eventual Consistency using $\tau > 2\Delta$ as a standalone proof without stating all necessary distributed preconditions.
- **Original Design:** Broad unconditioned SEC claim.
- **Corrected Design:** **Formally Guarded Conditional Convergence Guarantee.**
  Convergence is guaranteed **if and only if** all of the following preconditions are satisfied:
  1. *Partial Synchrony:* Physical time $t > GST$, and message delays are bounded by $\Delta$.
  2. *Connected Mesh:* Open DataChannels exist directly or via anti-entropy relay between all non-faulty peers.
  3. *Dissemination & Anti-Entropy Completion:* All generated operations have been delivered, and all pending `REPAIR_REQUEST` handshakes have completed.
  4. *Quiescence:* No peer generates new control intents for $\tau \ge 2\Delta + \text{digestInterval} + \text{repairRTT}$.
  5. *Deterministic Reducer:* All peers execute the pure functional reducer over the identical total-order key $\langle L, \text{originPeerId}, \text{logicalEventId} \rangle$.
- **Traceability:** Updates `PHASE7_2_HLD.md`, `PHASE7_2_CONSISTENCY_MODEL.md`.

---

### Correction H: Unverified Performance Claims Labeled as Targets

- **Defect Identified:** Claiming that replaying 256 operations takes $< 1\text{ ms}$ without empirical benchmarks.
- **Original Design:** Asserted as an established fact.
- **Corrected Design:** Labeled as an **Unverified Performance Target (Hypothesis)**:
  - *"Target: In-memory replay of 256 operations through pure functional reducer targeted to execute in $< 2.0\text{ ms}$ on modern browser engines."*
  - Benchmark methodology formally specified in `PHASE7_2_SIMULATION_TEST_STRATEGY.md` for execution during Milestone P7.7.
- **Traceability:** Updates `PHASE7_2_LLD.md`, `PHASE7_2_FINAL_DESIGN_REVIEW.md`.

---

## 3. Impact Analysis & Document Traceability Matrix

| Document | Primary Corrections Applied |
| :--- | :--- |
| `docs/PHASE7_2_HLD.md` | Causal context propagation, trust boundaries, multi-origin data flows. |
| `docs/PHASE7_2_LLD.md` | Formal causality tests, 5-way horizon classification, concurrency-aware pause priority. |
| `docs/PHASE7_2_PROTOCOL_EVOLUTION_RFC.md` | Added `originSequenceNumber` and `causalContext` to envelope schema. |
| `docs/design/PHASE7_INTERFACE_CONTRACTS.md` | Added `causalContext` to `StoredOperation`, updated `ILogicalClockManager` and `IOrderedOperationLog`. |
| `docs/PHASE7_2_SIMULATION_TEST_STRATEGY.md` | Added tests for causal context, concurrency races, and snapshot frontier verification. |
| `docs/PHASE7_2_COMPATIBILITY_AND_MIGRATION.md` | Updated migration rules for `causalContext` and ephemeral reload identity. |
| `docs/PHASE7_2_FINAL_DESIGN_REVIEW.md` | Revised decisions AD-701 to AD-710, updated acceptance criteria assessment. |
| `docs/design/README.md` | Added `PHASE7_2_ARCHITECTURAL_CORRECTIONS.md` to design index. |
