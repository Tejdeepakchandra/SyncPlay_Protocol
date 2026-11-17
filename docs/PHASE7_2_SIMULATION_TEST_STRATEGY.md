# Phase 7.2 — Deterministic Simulation Test Strategy & Harness Specification

> **Document Version:** 1.0.0  
> **Status:** SPECIFICATION ONLY — NO IMPLEMENTATION AUTHORIZED  
> **Parent Milestone:** Phase 7.2 (Architecture & Interface Design)  
> **Baseline Commit:** `926c575`  
> **Target Subsystem:** Test infrastructure for `@syncplay/sync-engine` (P7.3+)  

---

## 1. Overview & Verification Philosophy

Testing distributed systems exclusively in real browsers is slow, flaky, and unable to reliably reproduce rare race conditions (e.g., a packet arriving exactly $1\text{ ms}$ before another). 

To ensure mathematical correctness before browser deployment, Phase 7 introduces a **Headless Deterministic Simulation Test Harness** (`SimulatedNetworkMesh`). The harness executes entirely in Node.js (Vitest), runs without real WebRTC sockets, uses a discrete virtual clock, and enables reproducible adversarial permutation testing via seeded pseudo-random number generation.

---

## 2. Simulation Harness Architecture

```mermaid
flowchart TD
    subgraph Harness ["SimulatedNetworkMesh (Deterministic Harness)"]
        VC["VirtualMonotonicClock<br/>(Tick-driven, discrete event queue)"]
        PRNG["PseudoRandomGenerator<br/>(Seedable PRNG e.g. Mulberry32)"]
        NetQueue["SimulatedPacketQueue<br/>(Delay, Jitter, Loss, Reorder, Partition)"]
    end

    subgraph SimulatedPeers ["Simulated Browser Peers"]
        PeerA["Peer A Engine Instance"]
        PeerB["Peer B Engine Instance"]
        PeerC["Peer C Engine Instance"]
        PeerD["Peer D Engine Instance"]
    end

    VC --> NetQueue
    PRNG --> NetQueue
    PeerA <--> NetQueue
    PeerB <--> NetQueue
    PeerC <--> NetQueue
    PeerD <--> NetQueue
```

### 2.1. Discrete Virtual Clock
- Replaces `performance.now()` with an injectable virtual clock `VirtualClock`.
- Time advances only when the simulator ticks or pops the next scheduled event from the priority queue:
  $$T_{\text{next}} = \min_{e \in \text{Queue}} (e.\text{deliveryTime})$$
- Zero reliance on Node.js `setTimeout` or OS system timers; runs thousands of virtual seconds in hundreds of milliseconds.

### 2.2. Seeded Pseudo-Random Number Generator (PRNG)
- Uses a deterministic 32-bit generator (e.g., Mulberry32):
  ```ts
  class SeededPRNG {
    constructor(private seed: number) {}
    next(): number {
      let t = (this.seed += 0x6d2b79f5);
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    }
  }
  ```
- Every randomized test run prints its `TEST_SEED`. If a randomized run discovers a convergence failure, the exact failure can be reproduced deterministically by supplying `SEED=<number>`.

---

## 3. Simulated Network Impairment Capabilities

The `SimulatedNetworkMesh` routes packets between simulated peer engines with configurable impairment knobs:

```ts
interface NetworkImpairmentConfig {
  /** Minimum network one-way latency in virtual ms (e.g., 20 ms). */
  minLatencyMs: number;
  /** Maximum network one-way latency in virtual ms (e.g., 150 ms). */
  maxLatencyMs: number;
  /** Probability of packet drop [0.0, 1.0] (e.g., 0.15 = 15% loss). */
  dropRate: number;
  /** Probability of duplicating a packet [0.0, 1.0] (e.g., 0.05 = 5%). */
  duplicateRate: number;
  /** Probability of out-of-order reordering [0.0, 1.0]. */
  reorderRate: number;
  /** Active partition topology (pairs forbidden from communicating). */
  partitions: Array<[PeerId, PeerId]>;
}
```

### Supported Failure Injections:
1. **Asymmetric Network Delays:** Packet from A to B takes $20\text{ ms}$, but packet from C to B takes $180\text{ ms}$.
2. **Packet Loss with Anti-Entropy Recovery:** 20% of broadcast control events are dropped; tests verify that `ReplicationController` detects the missing sequences via `STATE_DIGEST` and repairs them via `REPAIR_REQUEST`.
3. **Network Partitions & Healing:** Partition $\{A, B\}$ from $\{C, D\}$. Drive concurrent playback controls in both islands. Heal the partition after 10 virtual seconds. Verify that both islands converge to the identical state.
4. **Peer Crash & Restart (Churn):** Simulate tab close by destroying Peer C's engine. Advance the room by 50 operations. Recreate Peer C with Generation 2. Verify state catch-up via snapshot handshake.

---

## 4. Test Suite Scenarios & Assertions

### Suite 1: Randomized Permutation Testing ($N = 3, 4$ Peers)
- **Execution:** Generate $M = 20$ concurrent playback operations (`PLAY`, `PAUSE`, `SEEK`, `RATE_CHANGE`) across 3 peers simultaneously.
- **Permutation:** Deliver the messages to the 3 peers in 100 randomized arrival orders.
- **Assertion:** Once all queues are empty, all 3 peers must hold:
  1. `stateA.status === stateB.status === stateC.status`
  2. `stateA.anchorPositionMs === stateB.anchorPositionMs === stateC.anchorPositionMs`
  3. `stateA.playbackRate === stateB.playbackRate === stateC.playbackRate`
  4. `stateA.stateVersion === stateB.stateVersion === stateC.stateVersion`
  - **Pass Threshold:** 100 / 100 runs pass with 0 divergence.

### Suite 2: Bounded History Compaction Stress Test
- **Execution:** Run a simulated session emitting 1,000 sequential operations.
- **Constraint:** Operation log limit is $K = 256$.
- **Assertion:**
  1. Engine memory never exceeds 256 stored operations.
  2. `OrderedOperationLog.size <= 256` throughout the entire 1,000-operation run.
  3. Stable frontier $W_{\text{stable}}$ advances continuously.
  4. At virtual end, all active peers are fully converged.

### Suite 3: Anti-Entropy Packet Loss Soak Test
- **Execution:** Set `dropRate = 0.25` (25% packet loss on all channels).
- **Activity:** Emit 50 control operations across 4 peers.
- **Assertion:**
  1. Periodic anti-entropy detects all gaps.
  2. All `REPAIR_REQUEST` calls are fulfilled within $< 2000$ virtual ms.
  3. Final state is identical across all 4 peers.

### Suite 4: Causal Context & True Concurrency Verification
- **Execution:**
  1. Generate concurrent operations $A_1$ (`PAUSE`) and $B_1$ (`PLAY`) such that $A_1 \parallel B_1$ ($A_1.\text{originSequence} > B_1.\text{causalContext}[A]$ and vice versa).
  2. Verify that all peers resolve state to `'paused'` (intent-preserving pause priority).
  3. Peer B observes $A_1$ and subsequently generates $B_2$ (`PLAY`) with $A_1.\text{originSequence} \le B_2.\text{causalContext}[A]$.
  4. Verify that all peers resolve state to `'playing'` (intentional sequential resumption).
  5. **Multi-Branch Dependency Merge:** Peer C observes concurrent operations $A_2$ and $B_3$ and generates $C_1$. Peer D is injected with $A_2$ and $C_1$ while $B_3$ is delayed.
  6. Assert that Peer D holds $C_1$ in `PendingDependencyQueue`, dispatches `REPAIR_REQUEST` for $B_3$, and only applies $C_1$ once $B_3$ arrives.

### Suite 5: Snapshot Frontier & Reconnect Recovery Verification
- **Execution:**
  1. Peer D rejoins after missing $> 256$ operations (Class 3 horizon gap).
  2. Peer D requests `STATE_SNAPSHOT` from Peer A.
  3. **Stale Snapshot Test:** Peer C (slow peer) attempts to offer an outdated snapshot ($V_{\text{snap}} < V_{\text{local}}$). Assert Peer D rejects with `SNAPSHOT_STALE`.
  4. **Corrupt Digest Test:** Inject a snapshot where `stateDigestSha256` does not match the computed hash of the bound canonical preimage `{ formatVersion: 2, sessionId, membershipEpoch, snapshotAuthorPeerId, snapshotFrontier, maxLogicalTimestamp, state }`. Assert rejection with `DIGEST_MISMATCH`.
  5. **Atomic Installation & Local Replay:** Peer D holds 2 locally generated pending operations created during reconnect. Peer D receives valid snapshot from Peer A, atomically installs base state, and successfully replays the 2 local operations (`context: 'LOCAL_REPLAY'`).
  6. **No Supplier Available Test:** If all peers provide stale or corrupt snapshots, assert Peer D remains in `RECOVERING` without state corruption or speculative advances.
  7. Assert Peer D transitions from `'recovering'` to `'active'` upon valid install and converges with Peer A.

### Suite 6: Empirical Replay Latency & Performance Benchmark Harness
- **Objective:** Empirically evaluate the unverified performance target hypothesis: *"In-memory replay of 256 operations through pure functional reducer executes in $< 2.0\text{ ms}$."*
- **Harness Specification:**
  - Dedicated benchmark file: `packages/sync-engine/test/benchmark/replay-performance.bench.ts`.
  - Runs in clean Node.js / V8 environment using `performance.now()` with high-resolution timers.
  - Generates 1,000 iterations of 256 mixed operations (`PLAY`, `PAUSE`, `SEEK`, `RATE_CHANGE`).
  - Simulates late-arrival speculative rollback where an operation arrives at index 0, forcing a full 256-operation replay loop from base snapshot.
  - Measures execution time per replay loop.
- **Reporting Metrics:**
  - $p50$ (median latency), $p95$, and $p99$ tail latency.
  - Garbage collection overhead and heap allocation per replay.
- **Pass / Fail Threshold:** $p99 < 2.0\text{ ms}$ on baseline testing machines. Results will be published in the Milestone P7.7 Performance Evaluation Report.

### Suite 7: Strict Membership Epoch Fencing & Partition Recovery
- **Execution:**
  1. Initialize mesh with Peers A, B, and C in `membershipEpoch: 5`.
  2. Partition Peer C.
  3. Bootstrap advances room to `membershipEpoch: 6` (Peer C marked departed).
  4. Peers A and B emit 30 operations in epoch 6 and compact history past the old frontier.
  5. Peer C generates 5 local playback operations (`SEEK`, `PLAY`) under `membershipEpoch: 5`.
  6. Heal partition between C and {A, B}.
- **Assertions:**
  1. Peers A and B reject C's epoch-5 operations with code `REJECTED_OBSOLETE_EPOCH`.
  2. Peer C detects epoch mismatch, purges speculative intents, and transitions to `'recovering'`.
  3. Peer C requests `STATE_SNAPSHOT` from Peer A (epoch 6) and installs it cleanly.
  4. Final state across all 3 peers is 100% identical.

### Suite 8: Out-of-Order Gap Tracking & Non-Advancement of Replication Vector
- **Execution:**
  1. Peer A emits sequential operations $A_1$, $A_2$, and $A_3$.
  2. Network delivers $A_1$ and $A_3$ to Peer B, while dropping $A_2$.
- **Assertions:**
  1. Peer B applies $A_1$; `replicationVector[A] === 1`.
  2. Peer B receives $A_3$; verifies $A_3$ depends on $A_2$ ($A_3.\text{causalContext}[A] === 2$).
  3. Peer B buffers $A_3$ in `PendingDependencyQueue`; `replicationVector[A]` remains $1$ (does NOT advance to 3).
  4. Peer B emits `REPAIR_REQUEST` for sequence range $[2, 2]$.
  5. Network delivers $A_2$; Peer B applies $A_2$, cascades to unblock $A_3$, and advances `replicationVector[A]` to $3$.

### Suite 9: Incomparable Snapshot Frontiers & Degraded State Handling
- **Execution:**
  1. Simulate temporary split-brain partition creating incomparable snapshots between sub-meshes.
  2. Sub-mesh 1 produces Snapshot 1 with frontier $\{A: 10, B: 5\}$ at epoch 6.
  3. Sub-mesh 2 produces Snapshot 2 with frontier $\{A: 5, B: 10\}$ at epoch 6.
- **Assertions:**
  1. Verifies receiver never chooses a snapshot by comparing entry sums ($\sum V$).
  2. Receiver attempts log repair to bridge the causal gap if missing operations are retained in log history.
  3. If log history is compacted beyond horizon ($K = 256$), receiver **strictly avoids arbitrary tie-breaking** (no $\max(\text{epoch}) \to \max(L) \to \max_{\text{lex}}$ selection).
  4. Receiver transitions engine to `'DEGRADED_OUT_OF_SYNC'` lifecycle state.
  5. Assert explicit host re-seek or room reload recovers synchrony and returns engine to active state.

### Suite 10: Token-Bucket Clock Inflation, Causal Integrity & Strict No-Forwarding
- **Execution:**
  1. **Integrity Violation:** Peer B emits operation with $L(e) = 5$, but declares dependency with $L(d) = 10$.
  2. Assert receiver rejects with `INVALID_CAUSAL_TIMESTAMP`.
  3. **Inflation Violation:** Peer attempts to advance Lamport clock faster than token-bucket capacity (refill 10 tokens/sec, burst 50 tokens).
  4. Assert receiver rejects with `REJECTED_CLOCK_INFLATION` without state corruption, and **verifies that rejected events NEVER advance the local Lamport clock**.
  5. **Repair Exemption & Invariant Enforcement:** Replay 100 historical operations via repair batch (`context: 'REPAIR_BATCH'`); assert all 100 operations are accepted and clocks advance without triggering `REJECTED_CLOCK_INFLATION`. Inject a repaired operation with non-monotonic Lamport timestamp ($L \le L_{\text{prev}}$); assert receiver rejects with `CLOCK_REGRESSION` and verifies local Lamport clock and replication vector do NOT advance.
  6. **Strict No-Forwarding Violation:** Peer C sends a live control event with `originPeerId = "peerA"` and `senderPeerId = "peerB"`. Assert receiver rejects with `REJECTED_UNAUTHORIZED_ORIGIN`.

