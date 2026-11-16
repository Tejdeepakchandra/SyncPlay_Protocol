# Phase 7 — Architectural & Technical Risk Register

> **Document Version:** 1.0.0  
> **Status:** ACTIVE RISK MANAGEMENT PLAN  
> **Context:** SyncPlay Protocol — Phase 7 Leaderless Distributed Synchronization Engine  

---

## 1. Risk Matrix Overview

| Risk ID | Title | Severity | Likelihood | Impact Area | Mitigation Status |
| :---: | :--- | :---: | :---: | :--- | :--- |
| **RSK-701** | Split-Brain Divergence under Network Partition | **HIGH** | Medium | State Consistency & Convergence | Mitigated via Quorum / Quiescent Anti-Entropy |
| **RSK-702** | Multi-Sender Interleaving on WebRTC DataChannels | **HIGH** | High | Distributed Event Ordering | Mitigated via Lamport Total Order Comparator |
| **RSK-703** | Browser Background Tab Throttling / Sleep | **HIGH** | High | Liveness & Heartbeats | Mitigated via Burst Processing & Anti-Entropy |
| **RSK-704** | Log Explosion & Browser Memory Exhaustion | **HIGH** | Medium | Browser Stability & Memory Footprint | Mitigated via Bounded Circular Horizon & Compaction |
| **RSK-705** | Rejoining Peer Injecting Stale State | **HIGH** | Medium | State Corruption | Mitigated via Monotonic Connection Generations |
| **RSK-706** | Local State Rollback Glitches in Media UX | **MEDIUM** | Medium | User Experience & Visual Hitching | Mitigated via Speculative Execution & Dampening |
| **RSK-707** | Quadratic $O(N^2)$ Mesh Scalability Bottleneck | **MEDIUM** | High | Room Scaling & CPU Utilization | Mitigated via Small Room Boundary ($N \le 8$) |
| **RSK-708** | Rapid User "Seek Wars" Flooding the Mesh | **MEDIUM** | Medium | Network Bandwidth & Queue Flooding | Mitigated via Phase 5 Leaky-Bucket Rate Limiter |
| **RSK-709** | Direct WebRTC DataChannel Traversal Failures | **MEDIUM** | Low | Peer-to-Peer Connectivity | Mitigated via STUN/TURN Architecture Boundary |

---

## 2. Detailed Risk Profiles

### RSK-701: Split-Brain Divergence under Network Partition
- **Description:** A network partition splits a 4-peer room into two isolated 2-peer partitions ($\{A, B\}$ and $\{C, D\}$). Users in both partitions continue issuing playback controls independently.
- **Root Cause:** CAP theorem: during a network partition, a distributed system must choose between Availability (allowing both partitions to proceed) and Consistency (halting progress in one or both). SyncPlay prioritizes local interactive responsiveness.
- **Impact:** The two partitions diverge in both playback position and operation history.
- **Mitigation Strategy:**
  1. *Optimistic Island Operation:* Allow both islands to operate locally while setting a degraded partition health flag in `EngineStatus`.
  2. *Deterministic Partition Healing:* When the partition heals and WebRTC connectivity is restored, the anti-entropy reconciliation protocol exchanges missing logs.
  3. *Deterministic Total Order Winner:* The partition with the logically latest valid operation (by Lamport key) takes precedence, deterministically pulling the other island into alignment.
- **Trigger / Detection:** PeerMesh reports loss of connectivity to all peers in the remote partition; RTT probes time out.
- **Contingency Plan:** If the divergence is too wide ($>256$ operations), trigger an explicit `StateSnapshotPayload` recovery to cleanly align the lagging partition without throwing replay exceptions.

---

### RSK-702: Multi-Sender Interleaving on WebRTC DataChannels
- **Description:** Two peers issue concurrent operations. Because WebRTC DataChannels only order packets per-pair, a third peer receives them in reverse order relative to another peer.
- **Root Cause:** SCTP DataChannels are strictly point-to-point. There is no global physical clock or broadcast sequencer across a multi-peer WebRTC mesh.
- **Impact:** Replicas process operations in different arrival orders, resulting in permanent state divergence under the naive BaselineSyncEngine reducer.
- **Mitigation Strategy:**
  1. Implement a **Deterministic Total-Order Comparator** based on Lamport timestamps and peer ID tie-breaking: $\langle L(e), \text{peerId}, \text{eventId} \rangle$.
  2. Maintain a **Bounded Historical Re-ordering Buffer**: when an operation arrives with a lower logical timestamp than recently applied operations (within a bounded horizon), the engine rewinds to the pre-operation anchor and re-applies the operations in strictly sorted total order.
- **Trigger / Detection:** Reception of an event whose `logicalTimestamp` is less than the engine's current state `logicalTimestamp`.
- **Contingency Plan:** Ensure re-evaluation logic is pure functional and executes within $< 2\text{ms}$ to avoid perceptible UI stutter.

---

### RSK-703: Browser Background Tab Throttling / Sleep
- **Description:** A user navigates away to another browser tab or minimizes the window. The browser suspends JavaScript timers (`setInterval`, `setTimeout`) or throttles execution to 1 Hz.
- **Root Cause:** Modern browser resource-saving policies (Chrome, Firefox, Safari power-saving timers).
- **Impact:** Heartbeats stall, peer connection may appear degraded, and inbound WebRTC DataChannel messages accumulate in browser queues without immediate processing.
- **Mitigation Strategy:**
  1. Do not use recurring wall-clock timers for core synchronization invariants; drive processing strictly off incoming DataChannel `message` events.
  2. Use Web Workers or `AudioContext` keep-alive if active background processing is strictly necessary.
  3. Upon tab focus / wake-up, drain the queued message backlog in logical total order and execute an anti-entropy digest check to fetch any dropped packets.
- **Trigger / Detection:** Monotonic timestamp jump ($\Delta t > 2000\text{ms}$) between consecutive animation ticks.
- **Contingency Plan:** Force an automatic digest exchange upon window `focus` or document `visibilitychange` events.

---

### RSK-704: Log Explosion & Browser Memory Exhaustion
- **Description:** In a long-running playback session with frequent scrubbing or continuous adjustments, retaining all past operations exhausts browser memory.
- **Root Cause:** Unbounded append-only operation logs.
- **Impact:** Memory leaks, eventual browser tab crash (OOM).
- **Mitigation Strategy:**
  1. **Strictly Bounded History Horizon:** Enforce an immutable maximum capacity on the operation log (e.g., $K = 256$ operations).
  2. **Snapshot Compaction & Truncation:** Once all active connected peers have acknowledged receiving operations up to sequence watermark $W$, operations older than $W$ are collapsed into the base playback snapshot and safely deleted from memory.
  3. **Snapshot Import for Late Peers:** Peers partitioned longer than $K$ operations are served a fresh snapshot rather than the missing log slice.
- **Trigger / Detection:** Engine statistics `operationLogSize` approaching configured maximum limit.
- **Contingency Plan:** Force immediate log compaction when memory exceeds $75\%$ of threshold.

---

### RSK-705: Rejoining Peer Injecting Stale State
- **Description:** Peer B disconnects at video minute 1:00, the room progresses to minute 15:00, and Peer B reconnects and broadcasts its old 1:00 playback state, overwriting the room.
- **Root Cause:** Reconnecting peers re-broadcasting cached local state before receiving room truth.
- **Impact:** Annoying playback rewinds for all participants; state corruption.
- **Mitigation Strategy:**
  1. **Generation Guarding:** Integrate with Phase 2/3 `ConnectionGeneration`. Operations must be stamped with or validated against the active connection generation.
  2. **Quarantine / Catch-Up State:** A rejoining peer enters a local `RECOVERING` lifecycle state. In this state, local control input is disabled or buffered, and the peer cannot broadcast operations until it completes an anti-entropy or snapshot recovery handshake with an existing peer.
- **Trigger / Detection:** Inbound connection initialized with a higher generation counter ($G \ge 2$).
- **Contingency Plan:** Silently drop any outbound control intents initiated during the recovery handshake.

---

### RSK-706: Local State Rollback Glitches in Media UX
- **Description:** When a late-arriving concurrent operation causes the engine to re-order and re-evaluate state, the logical position or status jumps abruptly.
- **Root Cause:** Optimistic local execution followed by distributed conflict resolution.
- **Impact:** Visual jitter, audio glitching, or jarring user experience if media playback snaps backward.
- **Mitigation Strategy:**
  1. **Decouple Logical State from Immediate Media Hard-Seeks:** Differentiate between micro-conflicts (position differences $< \epsilon = 250\text{ms}$) and major conflicts.
  2. In Phase 8, the media adapter applies continuous rate adjustment for small discrepancies, and reserves hard seeks only for intentional seek operations or large displacements ($> 1000\text{ms}$).
  3. Keep the pure logical engine free of UX hacks, but surface clean `onDecision` metadata (`'REORDERED'` / `'SUPERSEDED'`) so UI layers can animate transitions smoothly.
- **Trigger / Detection:** State re-evaluation producing an anchor shift opposite to current playback direction.
- **Contingency Plan:** Throttle notification emissions to the UI layer to prevent multiple redraws within the same animation frame ($16.6\text{ms}$).

---

### RSK-707: Quadratic $O(N^2)$ Mesh Scalability Bottleneck
- **Description:** Attempting to scale a full-mesh WebRTC room to 15+ participants leads to excessive CPU and upstream bandwidth consumption.
- **Root Cause:** Full-mesh topology requires $N(N-1)$ unidirectional channels. Every broadcast generates $N-1$ DataChannel transmissions.
- **Impact:** Packet loss, latency spikes, sluggish UI, mobile browser tab termination.
- **Mitigation Strategy:**
  1. Explicitly bound the room size at the Bootstrap Server and SDK levels (default max peers: $4$, absolute max: $8$).
  2. Document full-mesh architectural bounds honestly. (Larger rooms in future phases would require Selective Forwarding Units / SFUs, which belong to long-term roadmap discussions).
- **Trigger / Detection:** Bootstrap room join requests exceeding configured room capacity.
- **Contingency Plan:** Bootstrap server rejects join attempts with `ROOM_FULL` HTTP 409 status.

---

### RSK-708: Rapid User "Seek Wars" Flooding the Mesh
- **Description:** Multiple users rapidly scrub the video seek bar back and forth, generating hundreds of control events per second.
- **Root Cause:** Unbounded user interaction without debounce or throttle.
- **Impact:** Queue saturation, high latency, thrashing state reducers.
- **Mitigation Strategy:**
  1. Rely on the Phase 5 `RateLimiter` (leaky-bucket rate limiter enforcing max $5$ intents/sec per peer).
  2. In UI adapters, emit `SEEK` operations only on slider release (`change` event), not continuously during dragging (`input` event).
- **Trigger / Detection:** Rate limiter dropping excessive outbound intents with `RATE_LIMIT_EXCEEDED`.
- **Contingency Plan:** UI displays rate limit warning banner to user.

---

### RSK-709: Direct WebRTC DataChannel Traversal Failures
- **Description:** Two peers behind restrictive or symmetric corporate NATs fail to establish direct WebRTC peer connections.
- **Root Cause:** Symmetric NAT IP/port translation blocking STUN hole punching.
- **Impact:** Peers cannot communicate over DataChannels.
- **Mitigation Strategy:**
  1. Support standard TURN relay configurations in WebRTC `RTCConfiguration` (Phase 3 baseline).
  2. Verify that TURN relays packets transparently without acting as an application authority (Principle 3).
- **Trigger / Detection:** WebRTC connection state transitioning to `'failed'`.
- **Contingency Plan:** Fault Manager alerts application to supply valid TURN relay credentials.
