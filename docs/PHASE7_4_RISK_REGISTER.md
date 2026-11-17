# Phase 7.4: Risk Register & Failure Mode Analysis

**Milestone:** Phase 7.4 (State Replication & Anti-Entropy Risk Management)  
**Status:** SPECIFICATION ONLY — IMPLEMENTATION NOT AUTHORIZED  
**Approved Baseline:** Phase 7.2-R9 (`1825de1`), Phase 7.3-R5-H (`e49b81a` / docs `30e6d0d`)  
**Date:** 2026-11-17  

---

## 1. Risk Matrix Overview

The introduction of decentralized state replication and anti-entropy introduces distributed systems risks that do not exist during normal broadcast playback. The risk register classifies these risks across severity, likelihood, trigger conditions, automated mitigations, and fallback contingencies.

```mermaid
quadrantChart
    title Risk Severity vs Likelihood Matrix
    x-axis Low Likelihood --> High Likelihood
    y-axis Low Severity --> High Severity
    quadrant-1 High Impact / High Likelihood (Critical Focus)
    quadrant-2 High Impact / Low Likelihood (Catastrophic Defense)
    quadrant-3 Low Impact / Low Likelihood (Monitor)
    quadrant-4 Low Impact / High Likelihood (Operational Mitigation)
    
    "R-7.4-01: Repair Amplification Storm": [0.65, 0.75]
    "R-7.4-02: Silent Fork / Sequence Collision": [0.25, 0.90]
    "R-7.4-03: Stale Supplier Replay Attack": [0.35, 0.85]
    "R-7.4-04: Pending Queue Memory Bloat": [0.60, 0.70]
    "R-7.4-05: Churn & Partition Oscillations": [0.55, 0.65]
    "R-7.4-06: WebRTC DataChannel Starvation": [0.45, 0.60]
    "R-7.4-07: Incomparable Horizon Deadlock": [0.30, 0.80]
```

---

## 2. Detailed Risk Inventory

### R-7.4-01: Repair Request Amplification Storms
- **Category:** Network Congestion / Denial of Service.
- **Severity:** High
- **Likelihood:** High (common during high packet loss or bursty user activity).
- **Trigger Condition:** Multiple peers observe the same missing operation simultaneously and flood the author or mesh with duplicate `REPAIR_REQUEST`s. The supplier attempts to serve all requests simultaneously, saturating its outbound DataChannel and inducing further packet loss.
- **Architectural Mitigations:**
  1. *Per-Peer Concurrency Cap:* A peer can have at most $2$ active in-flight repair requests per supplier.
  2. *In-Flight Deduplication:* If a request for origin $P$ sequences $[4..7]$ is already in flight, identical or overlapping requests are queued, not dispatched.
  3. *Token-Bucket Rate Limiter:* Inbound requests to a supplier are throttled (refill rate 5 req/sec, burst 10). Excess requests are rejected immediately with `REPAIR_REJECTED` (`reason: 'RATE_LIMITED'`).
  4. *Exponential Backoff with Jitter:* Failed or timed-out requests retry with randomized delays: $1000\text{ms} \pm 250\text{ms}, 2000\text{ms} \pm 500\text{ms}, 4000\text{ms} \pm 1000\text{ms}$.
- **Contingency Plan:** If rate-limiting persists for $> 6000\text{ms}$, the engine marks that origin degraded and delays further repairs until the next periodic digest window.

---

### R-7.4-02: Silent Forking / Byzantine Sequence Substitution
- **Category:** State Consistency / Integrity.
- **Severity:** Critical
- **Likelihood:** Low (requires malicious peer or severe software defect).
- **Trigger Condition:** A peer authors two distinct operations with the same `(originPeerId, originSequenceNumber)`, sending variant $\alpha$ to Peer A and variant $\beta$ to Peer B.
- **Architectural Mitigations:**
  1. *Tail Fingerprint Comparison in Digest:* `StateDigestPayload.historyFingerprints` carries the SHA-256 fingerprint of the latest operation for each origin. If Peer A and Peer B compare vectors and find $V_A[P] == V_B[P]$ but $H_A[P] \ne H_B[P]$, divergence is flagged immediately.
  2. *Immutable Content Verification:* When variant $\beta$ is subsequently delivered to Peer A, `OrderedOperationLog.insert()` checks `this.originSeqIndex`. It detects that sequence $k$ already has a different `logicalEventId` and throws `SyncEngineError('SEQUENCE_COLLISION_DETECTED')`.
  3. *No-Overwrite Rule:* An existing accepted operation is **never** silently overwritten or mutated.
- **Contingency Plan:** The engine rejects the colliding event with `REJECTED_SEQUENCE_CONFLICT`, freezes replication progress for that specific origin, flags `isDegraded = true`, and preserves the existing uncontaminated playback state.

---

### R-7.4-03: Stale Supplier Ingestion & Replay
- **Category:** Transport Security / Boundary Violation.
- **Severity:** High
- **Likelihood:** Medium (occurs during frequent peer reconnects or network flapping).
- **Trigger Condition:** A disconnected peer reconnects and receives a new `connectionGeneration = 3`, but an old repair batch carrying `connectionGeneration = 1` arrives delayed over an old socket or replayed by an attacker.
- **Architectural Mitigations:**
  1. *Capability-Gated Transport Context (R5-H):* The transport context must be registered with the engine using the private `TransportRegistrationCapability` reference. Plain objects and clones are rejected.
  2. *Zero Default Fallback:* In `ingestRepairBatch()`, `peerGenerations.get(supplierPeerId)` is queried. If the record does not exist or if $transportGen < activeGen$, the batch is rejected as `REJECTED_INVALID` (`Repair batch from stale supplier connection generation`).
  3. *Zero State Mutation:* Rejected batches produce zero mutation across log, clock, vectors, or playback state.
- **Contingency Plan:** Stale packets are dropped at the transport boundary without triggering engine rollbacks.

---

### R-7.4-04: Memory Bloat from Unfulfillable Gaps
- **Category:** Resource Exhaustion.
- **Severity:** High
- **Likelihood:** Medium (occurs when a peer leaves permanently without supplying its sequence).
- **Trigger Condition:** Peer D generates $D_1$ and $D_3$, but departs permanently before sending $D_2$. Remote peers buffer $D_3$ indefinitely waiting for missing $D_2$. As more concurrent operations accumulate, the pending buffer grows.
- **Architectural Mitigations:**
  1. *Hard Capacity Limit:* `PendingDependencyQueue` has a strict capacity of $K = 256$ operations.
  2. *Deterministic Rejection:* If the queue reaches 256, newly arriving dependent events are rejected with `REJECTED_CAPACITY` rather than causing unbounded heap allocations.
  3. *Epoch Pruning:* When the Bootstrap Server detects Peer D's departure, it initiates a membership epoch transition ($E \to E + 1$). The engine executes `pendingQueue.pruneObsoleteEpoch(newEpoch)`, purging all operations authored in obsolete epochs.
- **Contingency Plan:** If a gap cannot be repaired within 3 retries, the engine triggers `onHistoryExhausted(originPeerId, missingSequence)`, allowing application-level recovery or Phase 7.5 snapshot reset.

---

### R-7.4-05: Churn & Partition Oscillations
- **Category:** Distributed Synchronization Stability.
- **Severity:** Medium
- **Likelihood:** Medium (poor Wi-Fi / cellular connections).
- **Trigger Condition:** Peers rapidly disconnect, reconnect, and split into transient partitions, generating dozens of epoch transitions and digest exchanges per minute.
- **Architectural Mitigations:**
  1. *Signaling Debounce:* PeerMesh signaling enforces a debounce window ($500\text{ms}$) on rapid ICE renegotiations.
  2. *Digest Rate Limiter:* Outbound digest broadcasts are capped at 1 per 2 seconds per peer.
  3. *Epoch Monotonicity:* An epoch transition is valid only if $newEpoch > currentEpoch$. Stale epoch transitions are rejected.
- **Contingency Plan:** During high churn, live playback continues optimistically using the local clock; anti-entropy repair pauses until the connection generation stabilizes for $\ge 1500\text{ms}$.

---

### R-7.4-06: WebRTC SCTP DataChannel Buffer Starvation
- **Category:** Transport Layer Performance.
- **Severity:** Medium
- **Likelihood:** Medium (sending large repair batches over saturated uplink).
- **Trigger Condition:** A peer requests a large range of operations, and the supplier sends a monolithic payload that exceeds the WebRTC DataChannel `bufferedAmountLowThreshold` (typically $16\text{ KB} - 64\text{ KB}$), blocking live control messages.
- **Architectural Mitigations:**
  1. *Batch Size Limit:* `OperationBatchPayload.operations` is strictly capped at $32$ items.
  2. *Payload Size Cap:* Maximum serialized message size is capped at $64\text{ KB}$.
  3. *Separate SCTP Channels:* If supported by browser WebRTC implementation, repair traffic is sent on a secondary DataChannel (`syncplay-repair`), leaving `syncplay-control` unobstructed for live user actions (PLAY/PAUSE).
- **Contingency Plan:** If `dataChannel.bufferedAmount > 64 * 1024`, outbound repair responses are paused until `onbufferedamountlow` fires.

---

### R-7.4-07: Incomparable History & Compaction Deadlock
- **Category:** Architectural Boundary / Horizon Failure.
- **Severity:** High
- **Likelihood:** Low (occurs after prolonged disconnection $> 10$ minutes).
- **Trigger Condition:** A returning peer requests operations from sequence $5$, but all connected peers have already compacted their history up to sequence $50$ into their base snapshots ($W_{\text{stable}} \ge 50$).
- **Architectural Mitigations:**
  1. *Explicit Negative Response:* Suppliers check their log horizon. Since $5 < \text{snapshotFrontier}$, they immediately return `REPAIR_REJECTED` (`reason: 'OUTSIDE_HORIZON'`, `retainedHorizonSequence: 50`).
  2. *No Unsafe Partial Merges:* The requester does not attempt to fabricate or guess the missing operations.
  3. *Phase 7.5 Handoff:* The requester detects that all connected peers have compacted the requested sequence, triggers `onHistoryExhausted`, sets `isDegraded = true`, and transitions into Phase 7.5 atomic snapshot catch-up.
- **Contingency Plan:** The UI alerts the user that history has expired and displays a "Resync with Room" prompt.

---

## 3. Risk Monitoring & Diagnostic Telemetry

To ensure real-time observability of anti-entropy health, `LeaderlessSyncEngine.getStats()` will be extended with the following diagnostic counters:

| Counter Name | Type | Warning Threshold | Description |
|---|---|---|---|
| `antiEntropyDigestsSent` | Counter | $> 10\text{ /sec}$ | Total digests broadcasted. |
| `antiEntropyDigestsReceived` | Counter | $> 20\text{ /sec}$ | Total digests processed. |
| `antiEntropyRepairRequestsSent` | Counter | $> 5\text{ /sec}$ | Total repair requests issued. |
| `antiEntropyRepairRequestsReceived` | Counter | $> 10\text{ /sec}$ | Total repair requests served. |
| `antiEntropyBatchesIngested` | Counter | N/A | Successful repair batches applied. |
| `antiEntropyTimeoutsCount` | Counter | $> 3\text{ consecutive}$ | Repair requests that expired without response. |
| `antiEntropyRejectionsCount` | Counter | $> 5$ | Repair batches rejected by validation. |
| `antiEntropyHorizonExceededCount` | Counter | $> 0$ | Requests rejected because history is outside horizon. |

---

**Phase 7.4 is in planning state. Implementation has not started and remains subject to separate authorization.**
