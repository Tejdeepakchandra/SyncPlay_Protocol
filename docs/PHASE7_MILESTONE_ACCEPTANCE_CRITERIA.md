# Phase 7 — Milestone Acceptance Criteria & Verification Standards

> **Document Version:** 1.0.0  
> **Status:** PROPOSED ACCEPTANCE SPECIFICATION  
> **Context:** SyncPlay Protocol — Phase 7 Distributed Synchronization Engine  

---

## 1. Overview & Verification Philosophy

Phase 7 acceptance cannot be granted merely because two browser peers can exchange messages under happy-path conditions. Acceptance requires empirical proof that participating browser peers converge to identical playback states when subjected to:
1. Concurrent, conflicting control events generated simultaneously by multiple peers.
2. Asynchronous, reordered, or delayed network delivery across independent WebRTC DataChannels.
3. Transient peer dropouts, tab throttling/backgrounding, and generation-2 reconnections.
4. Stress testing under bounded memory and resource constraints.

Every milestone from P7.1 through P7.7 has explicit, testable entry and exit criteria detailed below.

---

## 2. Granular Milestone Acceptance Criteria

### P7.1 — Distributed Systems Research & Problem Definition
**Objective:** Formalize the distributed media synchronization problem, establish the network and failure model, and produce an analytical evaluation of candidate algorithms.

- [ ] **AC-7.1.1 (Formal Problem Formulation):** Deliver a documented mathematical problem statement defining concurrent event interleaving, causality, and divergence scenarios for media playback controls.
- [ ] **AC-7.1.2 (System & Network Model):** Define explicit operational assumptions:
  - Partially synchronous network with bounded delay $\Delta$ during stable periods.
  - Crash-recovery failure model with persistent identity and monotonic connection generations.
  - Non-Byzantine participant environment (no message tampering or forgery).
- [ ] **AC-7.1.3 (Algorithm Trade-off Matrix):** Produce an analytical comparison of at least four candidate paradigms:
  1. Lamport Timestamps with Deterministic Lexicographic Tie-Breaking.
  2. Vector Clocks with Causal Retransmission.
  3. Operation-based / State-based CRDTs (e.g., LWW-Element-Set adapted for media anchors).
  4. State-Machine Replication with Bounded Optimistic Rollback.
  *Each candidate must be scored on: interaction latency, message complexity, memory overhead, split-brain resilience, implementation complexity, and browser resource footprint.*
- [ ] **AC-7.1.4 (Recommended Direction):** Select and defend an optimal baseline algorithm and a proposed evolution path for small-room WebRTC full-mesh environments.
- [ ] **AC-7.1.5 (Zero Implementation Rule):** Zero production code or package API changes implemented during P7.1.

---

### P7.2 — Synchronization Architecture & Interface Design
**Objective:** Produce the High-Level Design (HLD), Low-Level Design (LLD), interface contracts, and protocol evolution specifications.

- [ ] **AC-7.2.1 (Interface Stability):** Define public contracts ensuring `SyncEngine` extends the Phase 5 `SyncEventReceiver` contract (`receive(event: AnyControlSyncEvent): void`) without modifications.
- [ ] **AC-7.2.2 (Protocol Evolution Spec):** Document necessary protocol schema additions in `@syncplay/protocol`:
  - Wire representation of distributed logical timestamps (e.g., envelope extension or payload metadata).
  - Schema for state exchange messages: `STATE_DIGEST`, `OPERATION_BATCH`, and `REPAIR_REQUEST`.
  - Strict backward compatibility: v0.1 parsers must safely discard or pass unknown forward fields without crashing.
- [ ] **AC-7.2.3 (Component Decomposition):** Define clean module boundaries within `@syncplay/sync-engine`:
  - `LogicalClockManager`: Tracks local and remote logical timestamps.
  - `OperationLog`: Bounded, ordered historical buffer with deterministic indexation.
  - `ConflictResolver`: Pure functional comparator and state reducer.
  - `ReplicationController`: Orchestrates digest comparisons and anti-entropy repairs.
- [ ] **AC-7.2.4 (Sequence & State Transition Diagrams):** Complete UML sequence and state transition diagrams covering:
  - Simultaneous concurrent seek conflict resolution.
  - Late-arriving event insertion and speculative state replay.
  - Digest comparison and missing event repair handshake.
- [ ] **AC-7.2.5 (Deterministic Simulation Spec):** Define the architecture of an in-memory headless multi-peer test harness capable of injecting arbitrary network delays, drops, and reorderings.

---

### P7.3 — Distributed Event Ordering & Conflict Resolution
**Objective:** Implement and verify the logical ordering mechanism, bounded operation log, and deterministic conflict resolution engine.

- [ ] **AC-7.3.1 (Deterministic Total Order):** Implement total-order comparison satisfying strict weak ordering (irreflexive, asymmetric, transitive):
  $$\text{Key}(e) = \langle \text{logicalTimestamp}(e), \text{senderPeerId}(e), \text{logicalEventId}(e) \rangle$$
- [ ] **AC-7.3.2 (Logical Clock Correctness):** Verify Lamport logical clock invariant:
  $$e_1 \to e_2 \implies \text{logicalTimestamp}(e_1) < \text{logicalTimestamp}(e_2)$$
  Local operations increment clock by $1$; received operations advance local clock to $\max(\text{local}, \text{received}) + 1$.
- [ ] **AC-7.3.3 (Operation-Specific Conflict Resolution):**
  - **SEEK vs. SEEK:** Deterministically select the operation with the highest total-order key; earlier concurrent seeks are superseded.
  - **PLAY vs. PAUSE:** Deterministically evaluate concurrent play/pause intents; verify safe, predictable audio state.
  - **RATE_CHANGE:** Monotonically apply scalar rate updates based on total order.
- [ ] **AC-7.3.4 (Bounded Operation History & Compaction):** Implement circular/bounded operation history (configurable max operations, default $256$). Expired operations beyond the horizon are pruned cleanly without memory leaks.
- [ ] **AC-7.3.5 (Adversarial Permutation Tests):** Automated unit tests executing at least $100$ randomized delivery permutations of concurrent operations across $3$ simulated peers; **100% of permutations must result in identical final state**.

---

### P7.4 — Peer State Replication & Convergence
**Objective:** Implement anti-entropy digest exchange, missing event detection, and multi-peer state reconciliation over WebRTC DataChannels.

- [ ] **AC-7.4.1 (State Digest Protocol):** Implement lightweight `STATE_DIGEST` message exchange carrying current logical clock watermarks and operation counts.
- [ ] **AC-7.4.2 (Gossip / Anti-Entropy Repair):** When a peer receives a digest indicating a gap in its operation log:
  - It issues a targeted `REPAIR_REQUEST` specifying the missing range of `logicalEventId`s or sequence watermarks.
  - The recipient responds with a bounded `OPERATION_BATCH`.
  - The missing operations are inserted into the local log and applied deterministically.
- [ ] **AC-7.4.3 (Convergence Verification under Loss):** In headless integration tests with up to $20\%$ simulated packet drop on WebRTC DataChannels, all connected peers successfully detect gaps and converge to identical state within a configurable timeout ($< 1500\text{ms}$).
- [ ] **AC-7.4.4 (Traffic Minimization):** Periodic anti-entropy checks must impose zero wire traffic when state is already synchronized (e.g., periodic digest size $< 128$ bytes, throttled to low frequency during idle playback).

---

### P7.5 — Fault Tolerance & State Recovery
**Objective:** Orchestrate state recovery for peers experiencing transient dropouts, browser tab backgrounding, or generation-2 reconnections.

- [ ] **AC-7.5.1 (Generation-Aware Event Filtering):** Verify that operations received from stale connection generations (older than the active `ConnectionGeneration` recorded by PeerMesh) are dropped safely without corrupting the active operation log.
- [ ] **AC-7.5.2 (Snapshot Fallback Handshake):** If a rejoining peer is partitioned longer than the operation log retention horizon ($>256$ operations):
  - The engine initiates snapshot recovery using `exportSnapshot()` / `importSnapshot()`.
  - The imported snapshot establishes a new base anchor; recent operations are replayed on top.
  - Monotonic `stateVersion` advances correctly ($v_{\text{local}} = v_{\text{prev}} + 1$).
- [ ] **AC-7.5.3 (Partition Healing):** If a network partition splits a 4-peer room into two 2-peer islands:
  - Both islands remain operational.
  - Upon partition heal, the reconciliation mechanism exchanges missing logs and converges both islands to the single highest-precedence total-order state.
- [ ] **AC-7.5.4 (Resource Disposal):** Calling `dispose()` on any engine instance cleanly terminates all background anti-entropy timers, clears logs, and drops all peer subscriptions.

---

### P7.6 — Adversarial Multi-Peer Browser Testing
**Objective:** Validate real browser execution in Google Chrome using Chrome DevTools Protocol (CDP port 9222) across $3$ to $4$ independent browser peer instances.

- [ ] **AC-7.6.1 (Multi-Tab CDP Driver):** Build an automated test driver managing at least $3$ concurrent browser tabs connected to a live Bootstrap Server.
- [ ] **AC-7.6.2 (Concurrent Multi-Peer Contention):** Trigger simultaneous user actions from multiple tabs:
  - Peer A clicks `SEEK(15000)`.
  - Peer B simultaneously clicks `SEEK(45000)`.
  - Peer C simultaneously clicks `PAUSE`.
  - **Pass Condition:** All 3 browser peer UIs display identical status, identical anchor position, identical playback rate, and identical state version.
- [ ] **AC-7.6.3 (Tab Throttling / Sleep Simulation):** Simulate background tab throttling by pausing CDP execution on Peer B for $5000\text{ms}$ while Peers A and C execute playback controls. Resume Peer B.
  - **Pass Condition:** Peer B detects missing operations, repairs state via anti-entropy, and catches up to Peers A and C without manual reload.
- [ ] **AC-7.6.4 (Disconnect / Rejoin under Active Playback):** Disconnect Peer C during active forward playback, advance playback state on Peers A and B, then rejoin Peer C with Generation 2.
  - **Pass Condition:** Peer C re-negotiates WebRTC, completes state recovery, and resumes synchronized playback.
- [ ] **AC-7.6.5 (Assertion Completeness):** The automated browser test suite executes $\ge 40$ assertions with 100% pass rate.

---

### P7.7 — Performance Evaluation & Phase Acceptance
**Objective:** Benchmark latency, network overhead, and memory consumption, document formal guarantees, and conclude Phase 7.

- [ ] **AC-7.7.1 (Convergence Latency Benchmark):** Measure 95th-percentile convergence latency across $2$, $4$, and $6$ peer meshes:
  - Local intent execution: $< 5\text{ms}$.
  - Peer-to-peer event convergence across local mesh: $< 50\text{ms}$ (excluding physical network RTT).
- [ ] **AC-7.7.2 (Wire Overhead Measurement):** Document exact byte overhead:
  - Control event message size: $< 512$ bytes.
  - Periodic anti-entropy digest size: $< 256$ bytes.
  - CPU utilization on modern browser thread: $< 2\%$ during idle playback, $< 5\%$ during active control bursts.
- [ ] **AC-7.7.3 (Room Size Bounds):** Empirically document maximum recommended room size for full-mesh operation (target: up to 6–8 peers in browser runtime).
- [ ] **AC-7.7.4 (Zero Vulnerabilities & Clean Monorepo):**
  - All automated tests in the monorepo pass cleanly ($\ge 450$ total tests).
  - TypeScript type check clean (`tsc --noEmit` with 0 errors).
  - Production build clean.
  - `npm audit` reports 0 vulnerabilities.
- [ ] **AC-7.7.5 (Comprehensive Documentation):** Deliver final Performance Evaluation Report, Phase 7 Completion Report, and Phase 8 Media Synchronization Architecture Brief.

---

## 3. Explicit Non-Goals (What Phase 7 Does NOT Deliver)

To maintain architectural focus and avoid scope creep, the following capabilities are explicitly out of scope for Phase 7:

1. **Byzantine Fault Tolerance (BFT):** Protection against deliberately malicious, compromised, or adversarial peers injecting forged signatures or fabricated timestamps.
2. **Internet-Scale Rooms:** Support for dozens or hundreds of peers in a single room (full-mesh WebRTC is inherently constrained to small groups).
3. **Hardware Frame-Accurate A/V Synchronization:** Micro-second audio sample synchronization or video decoder drift compensation (reserved for Phase 8).
4. **Persistent Server-Side Global History:** Storing playback event logs in a server database (violates the server-minimized principle).
5. **Decentralized NAT Traversal:** Eliminating the need for STUN/TURN relays (standard WebRTC ICE infrastructure remains necessary).

