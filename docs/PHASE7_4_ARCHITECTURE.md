# Phase 7.4: State Replication & Anti-Entropy Architecture Specification

**Milestone:** Phase 7.4 (State Replication & Anti-Entropy)  
**Status:** ARCHITECTURE DESIGN & PLANNING ONLY — IMPLEMENTATION NOT AUTHORIZED  
**Approved Baseline:** Phase 7.2-R9 (`1825de1`)  
**Previous Implementation Baseline:** Phase 7.3-R5-H (`e49b81a` / docs `30e6d0d`)  
**Date:** 2026-11-17  

---

## Executive Summary

Phase 7.4 designs the decentralized state replication and anti-entropy subsystem for the SyncPlay Protocol. 

In Phase 7.3, the engine implemented deterministic event ordering, immutable operation fingerprinting, capability-gated transport context verification, exact Lamport timestamp validation, and bounded in-memory dependency queues. However, Phase 7.3 relied on incoming live broadcasts or manually provided repair batches. 

Phase 7.4 solves the fundamental replication problem: **In an asynchronous, lossy, and partitioned WebRTC peer-to-peer network, different peers accumulate divergent subsets of operation history. The protocol must autonomously discover divergence, identify missing sequence ranges, fetch missing operations from peer logs, validate them through the immutable Phase 7.3 admission pipeline, and converge all eligible peers to identical playback states without introducing a centralized coordinator or playback server.**

```mermaid
flowchart LR
    A["Peer A<br/>V_rep = {A:5, B:2}"] <-->|"1. STATE_DIGEST Exchange"| B["Peer B<br/>V_rep = {A:3, B:6}"]
    A -->|"2. REPAIR_REQUEST (B: 3..6)"| B
    B -->|"3. OPERATION_BATCH (B: 3..6)"| A
    B -->|"4. REPAIR_REQUEST (A: 4..5)"| A
    A -->|"5. OPERATION_BATCH (A: 4..5)"| B
    A -.->|"Both Converged"| C["V_rep = {A:5, B:6}<br/>StateVersion & Playback Synced"]
    B -.->|"Both Converged"| C
```

---

## 1. Frozen Architectural Invariants

Phase 7.4 strictly adheres to the frozen Phase 7.2-R9 and Phase 7.3 architectural invariants:

1. **Server-Minimized, Leaderless Decentralization:** The Bootstrap Server provides discovery, authentication, signaling, and room membership rosters only. It has zero awareness of playback state, logical clocks, operation logs, or anti-entropy repair.
2. **Authorship vs Transport Provenance Decoupling:** `originPeerId` immutably identifies the logical operation author. `supplierPeerId` identifies the immediate transport peer delivering the repair payload. Historical event authorship is never conflated with supplier transport identity.
3. **Exact Lamport Event Creation & Validation:**
   $$L(e) = 1 + \max_{(p, s) \in \text{causalContext}} L(p, s)$$
   Validation enforces exact equality. Receiving an event updates the receiver's local clock using $\max(L_{\text{local}}, L_{\text{received}})$ without $+1$.
4. **Contiguous Replication Frontier Invariant:** `replicationVector[p] = k` guarantees that the local peer has received, schema-validated, causally unblocked, admitted, inserted into the ordered operation log, and reduced all operations originated by peer $p$ from sequence $1$ through $k$. Out-of-order or pending events **never** advance `replicationVector`.
5. **Phase 7.3 Capability-Gated Transport Boundary:** Ingestion of repair batches strictly requires an `AuthenticatedSupplierContext` minted via `TransportRegistrationCapability` reference equality (`===`) and validated against `peerGenerations` (exact active generation match, no `?? 1` fallback).
6. **Immutable Content Verification:** Deduplication and collision detection enforce RFC 8785 canonical JSON serialization and SHA-256 fingerprint verification (`computeOperationFingerprint`).
7. **Strict Resource Bounds:** Memory capacity ($K = 256$), batch sizes ($\le 32$ ops), frame sizes ($\le 64\text{ KB}$), request concurrency, and backoff limits are hard invariants.

---

## 2. Replication Knowledge Model

### 2.1 Formal Meaning of the Replication Vector

The local replication vector $V_{\text{rep}} \in \mathbb{N}^{\text{Peers}}$ represents strictly contiguous, verified, and applied operation history:

$$V_{\text{rep}}[p] = k \iff \forall s \in \{1, \dots, k\}, \text{op}(p, s) \in \text{CommittedHistory}$$

An operation $\text{op}(p, s)$ is considered part of `CommittedHistory` if and only if it has passed all seven stages of the operation lifecycle:

```mermaid
flowchart TD
    S1["1. Transport Delivery<br/>(SCTP over DTLS DataChannel)"] --> S2["2. Packet Parsing & Envelope Validation<br/>(Zod schema check)"]
    S2 --> S3["3. Transport Authorization Check<br/>(Capability match, active peerGenerations check)"]
    S3 --> S4["4. Causal & Continuity Check<br/>(s == V_rep[p] + 1 and causalContext <= V_rep)"]
    S4 -->|Missing Dependency| Buffer["Buffer in PendingDependencyQueue<br/>(NO clock advance, NO V_rep advance)"]
    S4 -->|Satisfied| S5["5. Engine Admission & Verification<br/>(Lamport L(e) == 1 + max deps, SHA-256 fingerprint)"]
    S5 --> S6["6. Ordered Log Insertion & State Reduction<br/>(Total-order insertion, ConflictResolver reduction)"]
    S6 --> S7["7. Replication Frontier Advance<br/>(V_rep[p] <- s, Cascade unblock buffered ops)"]
```

| Stage | Subsystem | Action | Effect on $V_{\text{rep}}$ |
|---|---|---|---|
| **1. Transport Delivery** | `webrtc-transport` | Raw packet received over SCTP/DTLS DataChannel. | None. |
| **2. Envelope Validation** | `control-event-service` | Validates session ID, wire schema, and timestamp types. | None. |
| **3. Transport Authorization** | `leaderless-sync-engine` | Verifies `AuthenticatedSupplierContext` capability and generation. | None. |
| **4. Causal & Continuity** | `leaderless-sync-engine` | Verifies $s = V_{\text{rep}}[p] + 1$ and all causal dependencies are satisfied. | If unsatisfied, buffered in `PendingDependencyQueue`. $V_{\text{rep}}$ **does not advance**. |
| **5. Engine Admission** | `logical-clock-manager` | Verifies exact Lamport formula, absence of sequence/identity collisions. | Advances logical clock without $+1$. $V_{\text{rep}}$ does not advance yet. |
| **6. Ordered Reduction** | `ordered-operation-log` & `conflict-resolver` | Inserts into ring buffer in total order; reduces or deterministically replays. | State version increments. |
| **7. Frontier Advance** | `leaderless-sync-engine` | Updates $V_{\text{rep}}[p] \leftarrow s$. Triggers `PendingDependencyQueue.drainSatisfied()`. | **$V_{\text{rep}}$ advances.** |

> [!IMPORTANT] Non-Advance Invariant
> An operation can **never** be advertised as replicated in a `STATE_DIGEST` or supplied to another peer in an `OPERATION_BATCH` until it has completed Stage 7. Advertising unreduced or buffered operations would violate causal monotonicity across the mesh.

### 2.2 Sparse Gap Representation

When a peer observes an out-of-order operation ($s > V_{\text{rep}}[p] + 1$) or an operation whose `causalContext` references an unknown sequence ($reqSeq > V_{\text{rep}}[q]$):
1. The operation is buffered in `PendingDependencyQueue` (bounded to capacity $K = 256$).
2. The sequence gap is defined as the discrete interval:
   $$\text{Gap}(p) = [V_{\text{rep}}[p] + 1, \dots, s - 1]$$
3. `PendingDependencyQueue.getMissingRanges(V_{\text{rep}})` extracts all discrete missing ranges across all buffered events:
   $$\text{MissingRanges} = \left\{ (origin, fromSeq, toSeq) \mid fromSeq \le toSeq \right\}$$
4. These missing ranges form the input to the Anti-Entropy Repair scheduler.

---

## 3. Decentralized State & History Comparison

### 3.1 Limitations of Scalar State Hashes

A naive distributed systems approach compares a single scalar checksum (e.g. SHA-256 of current playback state). In an interactive leaderless media player, scalar state hashing is fatally flawed:
- **No Diagnostic Value:** A hash mismatch indicates that states differ, but provides zero information about *which* peer or sequence is missing.
- **Continuous Extrapolation:** In playback state, `anchorLocalTimestamp` and extrapolated position drift with wall-clock time; scalar hashes fail to match even when peers are causally synchronized.
- **Premature Convergence False Positives:** Two peers might coincidentally match a coarse state hash while holding divergent causal logs.

### 3.2 The Hybrid Anti-Entropy Digest Model

SyncPlay Phase 7.4 implements a multi-component state digest:

```mermaid
classDiagram
    class StateDigestPayload {
        +string sessionId
        +string senderPeerId
        +number connectionGeneration
        +number membershipEpoch
        +number stateVersion
        +Record~string, number~ replicationVector
        +Record~string, number~ snapshotFrontier
        +Record~string, string~ historyFingerprints
        +number sentAt
    }
```

1. **`replicationVector` ($V_{\text{rep}}$):** Contiguous highest applied sequence per origin. Directly pinpoints missing sequence ranges.
2. **`snapshotFrontier` ($V_{\text{baseSnap}}$):** Oldest retained sequence per origin below which operations have been folded into the base snapshot.
3. **`historyFingerprints` ($H_{\text{origin}}$):** SHA-256 tail fingerprint of the highest applied operation for each origin ($H[p] = \text{fingerprint}(\text{op}(p, V_{\text{rep}}[p]))$). Detects sequence collisions or conflicting histories where two peers claim the same sequence count but possess different operation content.

### 3.3 Vector Comparison & Divergence Classification

When Peer A receives `StateDigestPayload` from Peer B ($V_B$):

```mermaid
flowchart TD
    Compare["Compare V_A[p] with V_B[p] for each origin p"] --> Check{V_A[p] vs V_B[p]}
    Check -->|V_A[p] < V_B[p]| Missing["Peer A is missing range [V_A[p] + 1, V_B[p]]<br/>-> Issue REPAIR_REQUEST to B"]
    Check -->|V_A[p] > V_B[p]| Ahead["Peer B is lagging on origin p<br/>-> Awaits REPAIR_REQUEST from B"]
    Check -->|V_A[p] == V_B[p]| FPCheck{"Check Tail Fingerprint<br/>H_A[p] == H_B[p]?"}
    FPCheck -->|Match| Identical["Histories for origin p are identical"]
    FPCheck -->|Mismatch| Fork["Sequence Collision / Fork Detected!<br/>Trigger IDENTITY_CONFLICT diagnostic"]
```

#### Vector Relationship Formalism:
- **Domination ($V_A \ge V_B$):** $\forall p, V_A[p] \ge V_B[p]$. Peer A has observed a superset of Peer B's operations. No repair needed by Peer A.
- **Lagging ($V_A < V_B$):** $\forall p, V_A[p] \le V_B[p]$ and $\exists p, V_A[p] < V_B[p]$. Peer A issues `REPAIR_REQUEST` for missing sequences.
- **Incomparable Concurrent Progress ($V_A \not\le V_B \land V_B \not\le V_A$):** Peer A is missing operations from origins where $V_A[p] < V_B[p]$; Peer B is missing operations from origins where $V_B[q] < V_A[q]$. Both peers issue independent, non-conflicting repair requests for their respective deficits.
- **Horizon Boundary Verification:** If $V_A[p] < V_B.\text{snapshotFrontier}[p]$, the operations Peer A needs have already been compacted by Peer B. Peer B cannot fulfill an operation repair; this triggers a handoff to Phase 7.5 Snapshot Recovery.

---

## 4. Missing-Range Detection & Repair Protocol

### 4.1 Repair Workflow Sequence

```mermaid
sequenceDiagram
    participant Requester as Peer A (Requester)
    participant Supplier as Peer B (Supplier)

    Note over Requester: 1. Digest Comparison or Gap Detection
    Requester->>Requester: Detects missing origin sequence: peer-O [4..7]
    
    Note over Requester: 2. Dispatch Bounded Request
    Requester->>Supplier: REPAIR_REQUEST { requestId: "req-1", originPeerId: "peer-O", fromSequence: 4, toSequence: 7 }
    
    Note over Supplier: 3. Supplier Horizon & Log Lookup
    Supplier->>Supplier: Check OrderedOperationLog.getRange("peer-O", 4, 7)
    
    alt Operations Retained in Log
        Supplier->>Requester: OPERATION_BATCH { supplierPeerId: "peer-B", originPeerId: "peer-O", fromSeq: 4, toSeq: 7, operations: [...] }
        Note over Requester: 4. Ingest via LeaderlessSyncEngine
        Requester->>Requester: ingestRepairBatch(batch, transportContext)
        Requester->>Requester: Causal unblock -> V_rep advances
    else Operations Compacted (Outside Horizon)
        Supplier->>Requester: REPAIR_REJECTED { requestId: "req-1", reason: "OUTSIDE_HORIZON", retainedHorizon: 8 }
        Note over Requester: 5. Handoff to Phase 7.5 Recovery
        Requester->>Requester: Trigger onHistoryExhausted("peer-O", gap)
    end
```

### 4.2 Supplier Selection Algorithm

When a missing range $(origin, fromSeq, toSeq)$ is identified:
1. **Author Preference:** If the original author $origin$ is currently connected with an authorized connection generation and advertised $V_{\text{author}}[origin] \ge toSeq$, direct the request to $origin$.
2. **RTT-Weighted Secondary Supplier:** If the author is disconnected or unresponsive, query the local peer table for all connected peers whose advertised $V_{\text{peer}}[origin] \ge toSeq$. Select the peer with the lowest smoothed round-trip time ($\text{SRTT}$) calculated by `@syncplay/peer-mesh` RTT engine.
3. **Failover on Timeout:** If a request times out ($1500\text{ms}$) or the selected supplier disconnects, blacklist that supplier for that range and re-dispatch to the next eligible supplier.

### 4.3 Pagination and Framing Constraints

- **Operation Limit per Batch:** Maximum 32 operations per `OPERATION_BATCH`.
- **Payload Size Cap:** Maximum 64 KB per serialized message. If 32 operations exceed 64 KB, the supplier slices the response to stay under the size cap.
- **Contiguous Slicing:** The supplier always serves a contiguous sub-range $[fromSeq, fromSeq + M - 1]$. The requester ingests the partial batch, advances $V_{\text{rep}}$, and automatically dispatches a subsequent request for the remainder $[fromSeq + M, toSeq]$.

---

## 5. Repair Validation and Ingestion Pipeline

Repaired operations arrive over the WebRTC DataChannel and are passed directly to `LeaderlessSyncEngine.ingestRepairBatch()`. 

> [!CAUTION] Zero-Bypass Security Invariant
> Repaired operations are **never** trusted implicitly. They must pass through the identical validation gauntlet as live operations, ensuring that a compromised or buggy peer cannot inject invalid operations via repair batches.

```mermaid
flowchart TD
    Wire["Inbound OPERATION_BATCH over DataChannel"] --> V1{1. Valid Transport Context?<br/>(WeakSet & capability check)}
    V1 -->|No| R1["REJECTED_INVALID: Fabricated transport context"]
    V1 -->|Yes| V2{2. Supplier Gen Match?<br/>(active == transportGen, no ?? 1)}
    V2 -->|No: Stale or Future| R2["REJECTED_INVALID: Stale/unannounced supplier generation"]
    V2 -->|Yes| V3{3. Origin Match?<br/>(batch.originPeerId == op.originPeerId)}
    V3 -->|No| R3["REJECTED_INVALID: Origin rewriting attempt"]
    V3 -->|Yes| V4{4. Duplicate / Fingerprint Collision?<br/>(Check idIndex & originSeqIndex)}
    V4 -->|Duplicate with identical hash| D1["IGNORED_DUPLICATE: Safe Idempotency"]
    V4 -->|Duplicate with altered hash| R4["REJECTED_IDENTITY_CONFLICT: Fingerprint mismatch"]
    V4 -->|Sequence collision| R5["REJECTED_SEQUENCE_CONFLICT: Conflicting event"]
    V4 -->|New Operation| V5{5. Exact Lamport Validation?<br/>L(e) == 1 + max(causalContext)}
    V5 -->|Mismatch| R6["REJECTED_INVALID: Lamport timestamp invalid"]
    V5 -->|Exact Match| V6["6. Advance Logical Clock (without +1)"]
    V6 --> V7["7. Insert to OrderedOperationLog in Total Order"]
    V7 --> V8["8. ConflictResolver: Deterministic Reduce or Replay"]
    V8 --> V9["9. Advance V_rep[p] <- s & Drain Pending Queue"]
```

### Complete Validation Matrix:
1. **Schema Check:** Conforms to `OperationBatchPayloadSchema` (zod).
2. **Provenance Check:** `validSupplierContexts.has(transportContext)` proves local transport adapter registration.
3. **Generation Check:** `peerGenerations.get(supplierPeerId)` exists and equals `transportContext.connectionGeneration`.
4. **Anti-Spoofing Check:** `rawOp.originPeerId === batch.originPeerId`.
5. **Fingerprint Verification:** `computeOperationFingerprint(rawOp) === existing.fingerprint`.
6. **Compacted Checkpoint Verification:** If already compacted, confirms immutable metadata matches.
7. **Lamport Invariant:** $L(e) = 1 + \max_{(q, d) \in \text{context}} L(q, d)$ validated using exact timestamps from the log or compacted checkpoints.
8. **Deterministic State Mutation:** Executed by `ConflictResolver.reduce()` or `ConflictResolver.replay()`.

---

## 6. Anti-Entropy Scheduling & Flow Control

To prevent network congestion, DataChannel buffer saturation, and CPU thrashing, anti-entropy execution is bounded by strict scheduling policies:

```mermaid
stateDiagram-v2
    [*] --> Idle
    
    Idle --> DigestScheduled: Periodic Timer (5000ms ± 1000ms jitter)
    Idle --> DigestImmediate: On Peer Connect / Epoch Transition
    Idle --> RepairDispatch: Gap Detected in Pending Queue
    
    DigestScheduled --> SendingDigest: Rate Limiter Permit Available
    DigestScheduled --> Idle: Throttled
    
    DigestImmediate --> SendingDigest: DataChannel Ready
    
    SendingDigest --> AwaitingResponse: Broadcast STATE_DIGEST
    SendingDigest --> Idle
    
    RepairDispatch --> Requesting: In-flight requests < MAX_CONCURRENT (2)
    RepairDispatch --> Idle: Max concurrent requests reached
    
    Requesting --> Ingesting: Received OPERATION_BATCH
    Requesting --> TimeoutBackoff: Timer Expired (1500ms)
    
    Ingesting --> Idle: Batch Ingested & Queue Drained
    TimeoutBackoff --> RepairDispatch: Retry Count < MAX_RETRIES (3)
    TimeoutBackoff --> Handoff75: Retry Count Exhausted / Outside Horizon
    
    Handoff75 --> Idle: Trigger Phase 7.5 Recovery
```

### 6.1 Concrete Scheduling Parameters

| Parameter | Value | Justification |
|---|---|---|
| **Periodic Digest Interval** | $5000\text{ms}$ | Low background overhead ($0.2\text{ Hz}$). |
| **Periodic Digest Jitter** | $\pm 1000\text{ms}$ | Prevents synchronization spikes across the mesh. |
| **Digest Rate Limit** | $1\text{ digest/sec}$ per peer (burst 2) | Protects transport from runaway digest floods. |
| **Repair Request Timeout** | $1500\text{ms}$ | Accommodates typical P2P WAN latencies ($100-300\text{ms}$) + batch creation. |
| **Max Concurrent Requests per Peer** | $2$ | Prevents overwhelming a single supplier. |
| **Max Room Concurrent Requests** | $16$ | Global engine budget to bound memory allocation. |
| **Max Repair Retries** | $3$ | Exponential backoff ($1\text{s}, 2\text{s}, 4\text{s}$) before concluding failure. |
| **Max Missing Ranges per Request** | $1$ | Atomic range scoping simplifies tracking and failure isolation. |

---

## 7. Multi-Peer Topology & Convergence Dynamics

### 7.1 Three-Peer Triangle Convergence Example

Consider three peers $A$, $B$, and $C$. Peer $A$ produces operation $A_1$, Peer $B$ produces $B_1$, and network drops prevent direct $A \leftrightarrow B$ delivery, but $C$ receives both:

```mermaid
sequenceDiagram
    participant A as Peer A (V_A = {A:1, B:0, C:0})
    participant C as Peer C (V_C = {A:1, B:1, C:0})
    participant B as Peer B (V_B = {A:0, B:1, C:0})

    Note over A,B: Direct live messages A1 and B1 lost in transit
    Note over C: Peer C received A1 from A and B1 from B
    
    C->>A: STATE_DIGEST { V_C: {A:1, B:1, C:0} }
    C->>B: STATE_DIGEST { V_C: {A:1, B:1, C:0} }
    
    Note over A: A compares V_A with V_C -> Missing B: [1..1]
    Note over B: B compares V_B with V_C -> Missing A: [1..1]
    
    A->>C: REPAIR_REQUEST { origin: "peer-B", from: 1, to: 1 }
    B->>C: REPAIR_REQUEST { origin: "peer-A", from: 1, to: 1 }
    
    C->>A: OPERATION_BATCH { supplier: "peer-C", origin: "peer-B", ops: [B1] }
    C->>B: OPERATION_BATCH { supplier: "peer-C", origin: "peer-A", ops: [A1] }
    
    Note over A: A ingests B1 -> V_A becomes {A:1, B:1, C:0}
    Note over B: B ingests A1 -> V_B becomes {A:1, B:1, C:0}
    
    Note over A,B: All peers have converged to {A:1, B:1, C:0}!
```

In this scenario:
- Neither $A$ nor $B$ acts as a central server.
- Peer $C$ acts as an opportunistic anti-entropy supplier for operations it did not author ($originPeerId \ne supplierPeerId$).
- Deterministic conflict resolution on $A$ and $B$ ensures identical state execution regardless of the order in which $A_1$ and $B_1$ were admitted.

### 7.2 Network Partitions and Reconnection

When a partition separates the mesh into $\{A, B\}$ and $\{C, D\}$:
1. Within partition $\{A, B\}$, $A$ and $B$ exchange digests and stay synchronized.
2. Within partition $\{C, D\}$, $C$ and $D$ exchange digests and stay synchronized.
3. Operations originating in the opposite partition are buffered if referenced, or missing from $V_{\text{rep}}$.
4. **Partition Healing:** When a connection between $B$ and $C$ is established:
   - $B$ and $C$ exchange `STATE_DIGEST`.
   - Both detect significant sequence gaps in the other partition's origins.
   - Paged `REPAIR_REQUEST`s fetch missing operations in batches of 32.
   - Operations are ingested, total orders are computed via binary insertion into `OrderedOperationLog`, and the state is replayed from the base snapshot if necessary.
   - Full convergence is restored across all 4 peers.

---

## 8. Explicit Boundary with Phase 7.5 (Snapshot Recovery)

Phase 7.4 is strictly operation-history replication. It intentionally does not implement snapshot transfer.

```mermaid
flowchart LR
    AE["Phase 7.4: Anti-Entropy Log Repair<br/>(Missing sequence <= K=256)"] --> HorizonCheck{Gap within Retained Horizon?}
    HorizonCheck -->|Yes| Repaired["Reconciliation Successful via OPERATION_BATCH"]
    HorizonCheck -->|No| Handoff["Phase 7.5 Handoff: onHistoryExhausted()<br/>Trigger StateSnapshotPayloadV2 Catch-up"]
```

### Handoff Contract:
1. **Trigger Condition:** A missing sequence range cannot be fulfilled because all connected suppliers report `REPAIR_REJECTED` (`OUTSIDE_HORIZON`), or a newly joined peer has a gap exceeding $K = 256$.
2. **Engine Action:** `LeaderlessSyncEngine` transitions status to `EngineStatus.isDegraded = true` with reason `'HISTORY_HORIZON_EXCEEDED'`.
3. **Notification:** The engine emits an event `onHistoryExhausted({ originPeerId, missingSequence, supplierPeerId })`.
4. **Phase 7.5 Responsibility:** Phase 7.5's `RecoveryManager` subscribes to this notification and initiates the atomic snapshot recovery protocol (`STATE_REQUEST` $\to$ `STATE_SNAPSHOT` $\to$ `importSnapshotV2`).

---

## 9. Security, Trust Model & Abuse Boundaries

SyncPlay operates under a **Cooperative, Non-Byzantine Trust Model**:
- Peers are presumed to run standard software and not deliberately tamper with internal memory structures.
- However, network boundaries are treated as untrusted: malformed, corrupted, delayed, replayed, or spliced packets are actively defended against.

### Threat Analysis & Defenses:

| Threat Vector | Severity | Mechanism of Defense in Phase 7.4 |
|---|---|---|
| **Forged Transport Context** | Critical | Capability boundary (R5-H): contexts must be registered by trusted adapter with unforgeable capability reference. |
| **Stale Supplier Replay** | High | Supplier connection generation fencing: rejected if $gen < active$. |
| **Unannounced Supplier Spoofing** | High | Supplier connection generation fencing: rejected if $gen > active$. |
| **Origin Rewriting** | High | `rawOp.originPeerId === batch.originPeerId` check prevents supplier from re-attributing events. |
| **Immutable Content Mutation** | High | Canonical JSON (RFC 8785) + SHA-256 fingerprint verification rejects altered operations as `REJECTED_IDENTITY_CONFLICT`. |
| **Sequence Collision Injection** | High | `originSeqIndex` check rejects duplicate sequence with different event ID as `REJECTED_SEQUENCE_CONFLICT`. |
| **Repair Request Flood (DoS)** | Medium | Token bucket rate limiting (1/sec) and per-peer concurrency cap (2 active requests). |
| **DataChannel Memory Exhaustion** | High | Maximum 32 ops / 64 KB per batch; pending queue capped at $K = 256$ with explicit `REJECTED_CAPACITY`. |
| **Byzantine Malicious Author** | High | *Limitation:* Protocol detects collisions if contradictory events reach the same peer, but cannot prevent an author from sending divergent signed events to different peers without cryptographic signatures (deferred to post-MVP). |

---

## 10. Summary of Architectural Decisions

| Decision | Selected Approach | Alternatives Considered | Rationale |
|---|---|---|---|
| **D-7.4-1: State Comparison** | Hybrid Digest ($V_{\text{rep}}$ + $V_{\text{baseSnap}}$ + Tail Fingerprint) | Single scalar state hash, Vector clock only, Full log exchange | Scalar hash lacks diagnostic locality; full log is bandwidth prohibitive; hybrid digest pinpoints exact missing ranges in $O(\text{origins})$ time. |
| **D-7.4-2: Repair Granularity** | Bounded contiguous sequence ranges ($[from, to] \le 32$) | Individual event fetch, Entire log transfer | Individual events create request flooding; entire log exceeds DataChannel limits. Range batching is optimal for SCTP streaming. |
| **D-7.4-3: Supplier Selection** | Author preferred, fallback to min SRTT peer | Always author, Random peer, Flood request to all | Relying only on author fails if author disconnects; flooding causes severe bandwidth storms; RTT-weighted selection minimizes latency. |
| **D-7.4-4: Repair Ingestion** | Full reuse of Phase 7.3 `admitOperation()` | Separate fast-path repair ingestion | A secondary ingestion path creates subtle security/causal bypass bugs. Reusing `admitOperation()` guarantees uniform invariants. |
| **D-7.4-5: Scheduling Model** | Hybrid: Reactive on gap + Periodic ($5\text{s}$ jittered) | Pure periodic, Pure reactive | Pure reactive misses silent drops; pure periodic introduces high reconciliation latency. Hybrid combines immediate recovery with robust fallback. |

---

**Phase 7.4 is in planning state. Implementation has not started and remains subject to separate authorization.**
