# Phase 7.1 — Distributed Synchronization Problem Specification

> **Document Version:** 1.0.0  
> **Status:** COMPLETED & VERIFIED (Milestone P7.1)  
> **Context:** SyncPlay Protocol — Phase 7 Leaderless Distributed Synchronization Engine  

---

## 1. Executive Summary & Problem Context

The core mission of SyncPlay is to provide a peer-to-peer media synchronization protocol and SDK for small-room browser sessions without relying on a centralized playback server.

Through Phases 1 to 6, SyncPlay implemented a functional transport and control pipeline:
- `@syncplay/protocol`: Versioned message envelopes with distinct `messageId` and `logicalEventId`.
- `@syncplay/bootstrap-server`: Room membership, signaling relay, and monotonic `ConnectionGeneration`.
- `@syncplay/peer-mesh`: WebRTC DataChannels (`'syncplay-data'`), deterministic offerer policy, and RTT probing.
- `@syncplay/control-events`: Control intent dispatching, composite deduplication (`senderPeerId + logicalEventId`), rate limiting, and `SyncEventReceiver`.
- `@syncplay/sync-engine`: Pure deterministic reducer (`reduceState`), continuous position extrapolation (`calculateExtrapolatedPosition`), and snapshot import/export.

### The Fundamental Distributed Defect
The `BaselineSyncEngine` delivered in Phase 6 processes events strictly in **local network arrival order**.

While WebRTC DataChannels guarantee in-order delivery between a single pair of peers (e.g., Peer A to Peer B), SCTP provides **no total ordering across multiple senders**. In an asynchronous browser network with multiple participants, the arrival order of concurrent operations is non-deterministic. Consequently, replicas process concurrent operations in different sequences, causing permanent playback divergence.

---

## 2. In-Depth Analysis of 12 Distributed Scenarios

The following 12 scenarios formalize the failure modes, divergence risks, desired behaviors, and required distributed guarantees.

---

### Scenario 1: Concurrent SEEK Operations

- **Initial State:** Peer A, Peer B, Peer C at position $t = 10000\text{ ms}$, status = `'playing'`.
- **Events Generated:**
  - Peer A issues $e_1 = \text{SEEK}(30000)$ at local monotonic tick $T_A$.
  - Peer B issues $e_2 = \text{SEEK}(60000)$ at local monotonic tick $T_B$.
  - Both events are generated within a concurrent window ($|T_A - T_B| < \text{network RTT}$).
- **Events Received:**
  - Peer A receives: $[e_1 (\text{local}), e_2 (\text{remote})]$.
  - Peer B receives: $[e_2 (\text{local}), e_1 (\text{remote})]$.
  - Peer C receives: $[e_1 (\text{remote}), e_2 (\text{remote})]$ or $[e_2 (\text{remote}), e_1 (\text{remote})]$ depending on network path jitter.
- **Current Baseline Behavior:**
  - Peer A applies $e_1$ (jumps to 30s), then applies $e_2$ (jumps to 60s). Final state: $60\text{ s}$.
  - Peer B applies $e_2$ (jumps to 60s), then applies $e_1$ (jumps to 30s). Final state: $30\text{ s}$.
  - Peer C ends up at either $30\text{ s}$ or $60\text{ s}$.
- **Divergence:** Permanent state split. Replicas disagree on playback position by $30\text{ seconds}$.
- **Desired Behavior:** All peers must converge to the exact same seek position regardless of reception order.
- **Required Guarantee:** Deterministic Total Ordering of concurrent `SEEK` events (e.g., the operation with the highest total-order key wins; the other is superseded).
- **Unavoidable Limitation:** Optimistic local execution may cause Peer B to momentarily render $60\text{ s}$ before rewinding to $30\text{ s}$ (or vice versa) upon receiving the winning event.

| Peer | Local Event Order Applied | Baseline Final Position | Desired Final Position |
| :--- | :--- | :--- | :--- |
| **Peer A** | $e_1 \to e_2$ | $60000\text{ ms}$ | $60000\text{ ms}$ (assuming $e_2$ wins tie-breaker) |
| **Peer B** | $e_2 \to e_1$ | $30000\text{ ms}$ | $60000\text{ ms}$ (rewinds/advances to match winner) |
| **Peer C** | $e_1 \to e_2$ | $60000\text{ ms}$ | $60000\text{ ms}$ |

---

### Scenario 2: Concurrent PLAY and PAUSE

- **Initial State:** Replicas playing at $t = 25000\text{ ms}$.
- **Events Generated:**
  - Peer A encounters buffering or hits pause: $e_{\text{pause}} = \text{PAUSE}(\text{observedPos} = 25200)$.
  - Peer B concurrently triggers play: $e_{\text{play}} = \text{PLAY}(\text{targetPos} = 25200)$.
- **Events Received:**
  - Peer A receives: $[e_{\text{pause}}, e_{\text{play}}]$.
  - Peer B receives: $[e_{\text{play}}, e_{\text{pause}}]$.
- **Current Baseline Behavior:**
  - Peer A executes pause, then executes play. Final state: `'playing'`.
  - Peer B executes play, then executes pause. Final state: `'paused'`.
- **Divergence:** Half the room is playing media while the other half is paused.
- **Desired Behavior:** All peers must agree on status (`'playing'` vs. `'paused'`) and anchor position.
- **Required Guarantee:** Consistent conflict resolution for antagonistic operations.
- **Semantic Consideration:** In media playback, uncommanded sound is a severe UX bug. Giving priority to `PAUSE` when causally concurrent prevents unwanted audio playback.

---

### Scenario 3: Concurrent RATE_CHANGE Operations

- **Initial State:** Playback rate = $1.0\text{x}$.
- **Events Generated:**
  - Peer A selects $1.5\text{x}$ ($e_{\text{rate1}}$).
  - Peer B selects $2.0\text{x}$ ($e_{\text{rate2}}$).
- **Current Baseline Behavior:** Arrival order determines rate. Peer A may finish at $2.0\text{x}$ while Peer B finishes at $1.5\text{x}$.
- **Divergence:** Peers extrapolate playback position at different speeds ($1.5\text{x}$ vs $2.0\text{x}$), compounding position drift exponentially over time ($500\text{ ms}$ error every second).
- **Desired Behavior:** Uniform playback rate across all replicas.
- **Required Guarantee:** Deterministic total ordering of scalar updates.

---

### Scenario 4: Duplicate Delivery and Retry

- **Initial State:** Peer A emits $e_1 = \text{SEEK}(45000)$ with $\text{logicalEventId} = \text{"evt-1"}$ and $\text{messageId} = \text{"msg-1"}$.
- **Network Glitch:** Peer A observes partial delivery timeout (e.g., DataChannel buffer full) and calls `controls.retry(e1)`.
- **Events Generated:** Retry produces $\text{messageId} = \text{"msg-2"}$ with identical $\text{logicalEventId} = \text{"evt-1"}$.
- **Events Received by Peer B:** Both $\text{"msg-1"}$ and $\text{"msg-2"}$ eventually arrive at Peer B.
- **Current Baseline Behavior:** Phase 5 `DedupCache` filters by $\text{senderPeerId} + \text{logicalEventId}$. $\text{"msg-2"}$ is successfully suppressed as a duplicate.
- **Edge Case Risk:** If the deduplication cache is too small ($< 1000$ entries) or evicts $\text{"evt-1"}$ before $\text{"msg-2"}$ arrives, the operation will execute twice.
- **Required Guarantee:** Idempotence under retry. Operation history must index by `logicalEventId` to prevent duplicate state reductions.

---

### Scenario 5: Delayed and Out-of-Order Delivery

- **Initial State:** Playback at $0\text{ s}$.
- **Events Generated:**
  - Peer A sends $e_1 = \text{SEEK}(10000)$ at $t = 0\text{ ms}$.
  - Peer A sends $e_2 = \text{SEEK}(20000)$ at $t = 50\text{ ms}$.
- **Network Anomaly:** WebRTC DataChannel experiences SCTP packet retransmission or path congestion; $e_1$ is delayed by $800\text{ ms}$, arriving *after* $e_2$.
- **Events Received by Peer B:** $[e_2, e_1]$.
- **Current Baseline Behavior:** Peer B executes $e_2$ (jumps to 20s), then executes $e_1$ (jumps backward to 10s).
- **Divergence:** Peer A is at $20\text{ s}$; Peer B is at $10\text{ s}$.
- **Desired Behavior:** Peer B recognizes that $e_1$ has an older logical timestamp than $e_2$, ignoring or subordinating $e_1$ as superseded.
- **Required Guarantee:** Monotonic logical time validation and out-of-order rejection/re-ordering.

---

### Scenario 6: Partial Event Delivery Across Multiple Peers

- **Initial State:** 3-peer mesh: A, B, C.
- **Events Generated:** Peer A issues $e_1 = \text{PAUSE}(50000)$ and broadcasts to B and C.
- **Failure:** Peer A crashes or loses Wi-Fi immediately after transmitting $e_1$ to B over DataChannel A-B. The packet to C over DataChannel A-C is never sent.
- **Events Received:** Peer B receives $e_1$; Peer C never receives $e_1$. Peer A is gone.
- **Current Baseline Behavior:** Peer B pauses at 50s. Peer C continues playing indefinitely.
- **Divergence:** Permanent split between remaining alive peers (B and C).
- **Desired Behavior:** Peer B and Peer C detect that their operational logs differ and reconcile; Peer B forwards $e_1$ to Peer C.
- **Required Guarantee:** Transitive event dissemination / Anti-Entropy gossip across surviving mesh members.

---

### Scenario 7: Peer Disconnection and Reconnection

- **Initial State:** Peer B disconnects at video minute 1:00.
- **Session Progress:** Peers A and C continue watching for 10 minutes, generating multiple seeks, pauses, and rate changes up to minute 11:00.
- **Rejoin:** Peer B reconnects via Bootstrap Server, receives a fresh membership token and Generation 2 context.
- **Hazard:** If Peer B's engine re-broadcasts its local 1:00 state or applies queued local operations with stale timestamps, it could corrupt the active room state.
- **Desired Behavior:** Peer B is placed in a quarantine `RECOVERING` state, imports an authoritative `StateSnapshotPayload` from Peer A or C, fast-forwards its logical clock, and resumes playback at 11:00.
- **Required Guarantee:** State recovery protocol integrating with `ConnectionGeneration`.

---

### Scenario 8: Browser Reload and Logical Clock Restart

- **Initial State:** Peer A is at logical clock $L = 45$. The user presses F5 (hard page reload).
- **Consequence:** All in-memory state, including the Lamport counter and operation log, is wiped to zero ($L = 0$).
- **Hazard:** If Peer A rejoins with $L = 0$ and immediately generates an operation, its timestamp will appear to be in the distant past relative to other peers ($L = 46$), causing the room to reject its operations as stale.
- **Desired Behavior:** Upon reloading, the rejoining peer must adopt the maximum logical clock observed from room peers ($\max(L_{\text{local}}, L_{\text{remote}}) + 1$) during the join handshake before issuing new control intents.
- **Required Guarantee:** Safe logical clock re-seeding upon state initialization.

---

### Scenario 9: Dynamic Room Membership Changes

- **Initial State:** 2 peers (A and B).
- **Dynamic Action:** Peer C joins the room mid-session; Peer B leaves.
- **Hazard:** Vector clock approaches require an entry per peer ($V = \langle v_A, v_B, v_C \rangle$). Adding and removing peers requires dynamic vector clock resizing, garbage collecting dead peer entries, and handling vector dimension mismatches.
- **Desired Behavior:** Synchronization mechanism must operate without requiring fixed-dimension vector arrays tied to permanent peer rosters.
- **Required Guarantee:** Roster-independent ordering primitives.

---

### Scenario 10: Network Partition and Partition Healing

- **Initial State:** 4 peers in full mesh: $\{A, B, C, D\}$.
- **Partition:** Network fault creates two disjoint components: Island 1 $\{A, B\}$ and Island 2 $\{C, D\}$. DataChannels between the two islands fail.
- **Partition Activity:**
  - Users in Island 1 issue $\text{SEEK}(120000)$ and continue playing.
  - Users in Island 2 issue $\text{PAUSE}(45000)$.
- **Healing:** Network connectivity is restored after 60 seconds; DataChannels re-open.
- **Divergence:** Both islands have advanced along different timelines.
- **Desired Behavior:** Anti-entropy reconciliation activates; the island with the logically higher precedence operation (by deterministic total order) wins, or a designated conflict merge aligns both islands.
- **Required Guarantee:** Deterministic partition reconciliation without manual intervention or server mediation.

---

### Scenario 11: Browser Background Suspension (Tab Throttling)

- **Initial State:** Peer B minimizes the browser window.
- **Browser Policy:** Chrome throttles timers to 1 Hz or suspends background execution. DataChannel messages queue up in OS/browser socket buffers.
- **Session Progress:** Peer A issues 20 control operations over 30 seconds.
- **Wakeup:** User focuses Peer B's tab. Browser discharges a burst of 20 queued messages simultaneously.
- **Hazard:** High risk of out-of-order event dispatching and timer skew.
- **Desired Behavior:** The engine sorts the arriving burst by total-order key, executes a single coalesced state update, and checks state digests to fetch any dropped frames.
- **Required Guarantee:** Resilience against bursty, delayed event delivery caused by tab throttling.

---

### Scenario 12: Events Received from a Stale Connection Generation

- **Initial State:** Peer A is on Generation 1. Network drops; Peer A reconnects as Generation 2.
- **Hazard:** An in-flight WebRTC packet from Generation 1 was buffered in an intermediate NAT router and arrives at Peer B *after* Peer B has already acknowledged Peer A's Generation 2 connection.
- **Current Baseline Behavior:** PeerMesh derives `senderGeneration` from transport context.
- **Desired Behavior:** The SyncEngine or Control Events layer rejects any operation carrying or arriving over a connection generation older than the peer's current active generation.
- **Required Guarantee:** Monotonic generation gating preventing zombie packet injection.

---

## 3. Summary of Fundamental Requirements

From these 12 scenarios, five mandatory requirements emerge for Phase 7:

1. **Total Order Ordering Primitive:** A deterministic comparator that assigns every operation a unique, universal order agreed upon by all replicas.
2. **Re-ordering & Speculative Rollback:** Ability to re-evaluate local state when late-arriving concurrent operations arrive.
3. **Anti-Entropy Dissemination:** A gossip or digest exchange protocol to detect and repair dropped or partially delivered events.
4. **Bounded History with Snapshot Fallback:** A strictly bounded operation ring buffer coupled with snapshots for long-term catch-up.
5. **Generation & Lifecycle Gating:** Integration with monotonic connection generations to prevent zombie and reload corruption.

