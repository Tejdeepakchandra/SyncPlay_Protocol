# Phase 7.2-R5 — Logical Timestamp Integrity & Replay Boundaries

> **Document Version:** 1.1.0  
> **Status:** SUPERSEDED WHERE AMENDED BY REVISION P7.2-R6 (See [`PHASE7_2_R6_FINAL_CORRECTIONS.md`](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_2_R6_FINAL_CORRECTIONS.md))  
> **Baseline Commit:** `16dfe7a` (Amended in `990b3f4`, Final Freeze in P7.2-R6)  
> **Mode:** Specification & Architecture Only — Strict Stop Condition Observed  
> **Target Subsystems:** `@syncplay/sync-engine`, `@syncplay/protocol`, `@syncplay/control-events`, `@syncplay/peer-mesh`  

> [!NOTE]
> Revision P7.2-R6 establishes the definitive mathematical specifications:
> 1. **Exact Timestamp Equality:** Replaces lower-bound validation ($L(e) > \max L(d)$) with exact deterministic equality: $L(e) \equiv \text{expectedTimestamp}(e) = 1 + \max_{(p, s) \in e.\text{causalContext}} L(p, s)$. Any deviation is rejected with `INVALID_LOGICAL_TIMESTAMP`.
> 2. **Bounded Sequence Checkpoints:** Replaces the scalar `originFrontierTimestamps` heuristic with exact bounded sequence checkpoints ($K=256$) to resolve arbitrary historical dependencies without substituting later frontier timestamps.
> 3. **Global Maximum Timestamp Role:** Strictly bounds `maxLogicalTimestamp` as a snapshot diagnostic/reseeding ceiling, prohibiting its substitution for individual dependency sequences.  

---

## 1. Executive Summary & Purpose

Following the architectural corrections of Revision P7.2-R4 (`16dfe7a`), this final consistency review (Revision P7.2-R5) resolves the remaining operational and verification rules governing **logical timestamp integrity**, **anomalous timestamp jump handling**, and **strict replay boundaries**:

1. **Exact Logical Timestamp Validation:** Specifies how a receiving peer evaluates $L(e) = 1 + \max(L_{\text{local}}, \max_{d \in \text{deps}} L(d))$ against its retained history and compacted base snapshots. Clarifies that the token-bucket rate limiter throttles only event frequency, not timestamp magnitude. Establishes the rule that missing dependencies force buffering rather than premature evaluation, and compacted history without sufficient metadata triggers deterministic degraded handling.
2. **Classification of Anomalous Timestamp Jumps:** Replaces vague heuristics with five deterministic outcomes: Valid Advance, Missing Causal Dependency (buffered), Causal Inconsistency (rejected with `INVALID_CAUSAL_TIMESTAMP` or `CLOCK_REGRESSION`), Safe Integer Overflow (rejected with `CLOCK_OVERFLOW` and degraded transition), and Unverifiable Compacted History (rejected with `REJECTED_OUTSIDE_HORIZON`).
3. **Strict Replay Boundaries & Isolation:** Formally restricts `LOCAL_REPLAY` exclusively to immutable events already accepted and retained in `OrderedOperationLog`. Specifies that `REPAIR_BATCH` events must pass all structural, causal, and timestamp checks. Enforces that duplicate detection never masks sequence collisions (`SEQUENCE_COLLISION_DETECTED`), and guarantees that rejected events **never advance the Lamport clock or replication vector**.

This document serves as the final, binding specification freeze for Milestone P7.2.

---

## 2. Comprehensive Correction Register (P7.2-R5)

---

### Correction 1: Exact Logical Timestamp Validation & Dependency Frontier Evaluation

#### 1. The Validation Challenge
The universal event creation rule established in P7.2-R2 is:
$$L(e) = 1 + \max\left(L_{\text{local}}, \max_{d \in \text{dependencies}} L(d)\right)$$

While the author computes this easily using its local state at creation time, a receiving peer must validate that $L(e)$ legitimately reflects causal precedence without blindly trusting the author's scalar claim.

#### 2. Dependency Frontier Identification via `causalContext`
- An operation's `causalContext: Record<string, number>` represents the exact causal frontier observed by the author at the moment $e$ was created.
- For each entry $(p, s) \in e.\text{causalContext}$:
  - Origin $p$ has contributed an operation at sequence number $s$.
  - The dependency operation $d_{(p, s)}$ is the operation authored by $p$ with $\text{originSequenceNumber} = s$.
  - By induction, all prior operations $(p, 1 \dots s-1)$ are causally antecedent to $d_{(p, s)}$.

#### 3. Resolving the Maximum Dependency Timestamp ($L_{\text{max\_dep}}$)
When receiver $R$ evaluates incoming operation $e$, it computes $L_{\text{max\_dep}}$ across three possible storage tiers:

```mermaid
flowchart TD
    Dep["For each (p, s) in e.causalContext"] --> CheckSeq{Where does (p, s) reside<br/>in local receiver state?}
    
    CheckSeq -->|s > replicationVector[p]| Tier1["Tier 1: MISSING DEPENDENCY<br/>Operation s has not arrived yet.<br/>BUFFER in PendingDependencyQueue.<br/>DO NOT advance clock or replication vector!"]
    
    CheckSeq -->|s in OrderedOperationLog| Tier2["Tier 2: RETAINED IN LOG<br/>Look up log.get(p, s).logicalTimestamp.<br/>L_dep = storedOperation.logicalTimestamp."]
    
    CheckSeq -->|s <= W_stable[p]| Tier3["Tier 3: COMPACTED IN BASE SNAPSHOT<br/>Operation was folded into base state.<br/>Bounded: L_dep <= baseSnapshot.maxLogicalTimestamp."]
    
    Tier2 --> Aggregate["Aggregate L_max_dep = max(all L_dep)"]
    Tier3 --> Aggregate
    
    Aggregate --> Validate{"Does L(e) satisfy:<br/>L(e) > L_max_dep AND<br/>L(e) > L_prev(e.origin)?"}
    
    Validate -->|Yes| Admit["ADMIT OPERATION<br/>Advance Clock: L_local = max(L_local, L(e)) + 1"]
    Validate -->|No| Reject["REJECT: INVALID_CAUSAL_TIMESTAMP<br/>OR CLOCK_REGRESSION<br/>NEVER advance local clock!"]
```

1. **Tier 1 — Missing Dependency ($s > \text{replicationVector}[p]$):**
   - The dependency operation has not yet been received or applied by $R$.
   - **Action:** Receiver cannot compute $L(d)$ and cannot validate $L(e)$. The operation $e$ is **BUFFERED** in `PendingDependencyQueue`.
   - **Clock Invariant:** $R$'s local Lamport clock and replication vector **MUST NOT ADVANCE** while $e$ is buffered.
   - Anti-entropy dispatches `REPAIR_REQUEST` for sequence range $[\text{replicationVector}[p] + 1, s]$.
2. **Tier 2 — Retained in Log ($s \in \text{OrderedOperationLog}$):**
   - The dependency operation $d$ is present in $R$'s in-memory log.
   - $R$ directly reads $L(d) = d.\text{logicalTimestamp}$.
3. **Tier 3 — Compacted in Base Snapshot ($s \le W_{\text{stable}}[p]$):**
   - The dependency was compacted into the base snapshot during safe compaction.
   - In P7.2-R5, every base snapshot and compacted state retains:
     - `snapshotFrontier: Record<originPeerId, sequenceNumber>`
     - `maxLogicalTimestamp: number` (the maximum Lamport timestamp among all compacted operations)
     - `originFrontierTimestamps: Record<originPeerId, number>` (the exact timestamp of sequence $W_{\text{stable}}[p]$ for each origin).
   - $R$ uses `originFrontierTimestamps[p]` as the exact upper bound for $L(d)$. If per-origin timestamp maps are omitted, $R$ bounds $L(d) \le \text{baseSnapshot.maxLogicalTimestamp}$.
4. **Tier 4 — Unverifiable Pre-Horizon Dependency:**
   - If $e$ declares a dependency on an unknown origin or a sequence predating all retained snapshots and log horizons, the dependency cannot be verified under the cooperative trust model.
   - **Action:** $e$ is rejected with `REJECTED_OUTSIDE_HORIZON`. If this creates an unresolvable causal stall, the engine transitions to `DEGRADED_OUT_OF_SYNC`.

#### 4. Token Bucket vs. Timestamp Magnitude Clarification
- **Architectural Fact:** The token-bucket rate limiter ($10\text{ ops/sec}$, burst $50$) restricts **only the arrival frequency** of live events over wall-clock time ($\Delta t_{\text{monotonic}}$).
- **No Magnitude Guarantee:** A valid operation can legitimately advance $L$ by an arbitrary integer if it incorporates high-clock dependencies from another partition (e.g. merging two sub-meshes). The token bucket does **not** cap scalar timestamp magnitude.
- **Magnitude Bounds:** The scalar value of $L(e)$ is bounded strictly by:
  1. Monotonic increment rules: $L(e) \ge 1 + \max(L_{\text{local}}, L_{\text{max\_dep}})$.
  2. Safe integer ceiling: $L(e) < \text{Number.MAX\_SAFE\_INTEGER} - 1$ ($2^{53} - 1$).

---

### Correction 2: Deterministic Handling of Anomalous Timestamp Jumps

Rather than relying on arbitrary jump thresholds, the engine classifies and handles all timestamp behaviors through five rigorous states:

| Scenario / Condition | Diagnostic Classification | Engine State Transition | Local Clock Advancement | Replication Vector Advancement |
| :--- | :--- | :--- | :---: | :---: |
| **1. Valid Advancement**<br>$L(e) > \max(L_{\text{prev}}(p), L_{\text{max\_dep}})$ and $L < 2^{53}-1$ and (live) $\Delta L \le \text{tokens}$. | `VALID_OPERATION` | Stored in `OrderedOperationLog`; reduced to state. | **ADVANCES**<br>$L = \max(L, L(e)) + 1$ | **ADVANCES**<br>$\text{repVec}[p] = \text{seq}$ |
| **2. Missing Causal Dependency**<br>$\exists p: e.\text{causalContext}[p] > \text{repVec}[p]$. | `BUFFERED_PENDING_DEPENDENCY` | Placed in `PendingDependencyQueue`; emits `REPAIR_REQUEST`. | **HELD (No advance)** | **HELD (No advance)** |
| **3. Causal Inconsistency**<br>$L(e) \le L_{\text{max\_dep}}$ OR $L(e) \le L_{\text{prev}}(p)$. | `INVALID_CAUSAL_TIMESTAMP`<br>or `CLOCK_REGRESSION` | Operation **REJECTED & DISCARDED**; origin flagged suspicious. | **NO (Zero advance)** | **NO (Zero advance)** |
| **4. Safe Integer Overflow**<br>$L(e) \ge \text{Number.MAX\_SAFE\_INTEGER} - 1$. | `CLOCK_OVERFLOW` | Operation **REJECTED**; engine transitions to `DEGRADED_OUT_OF_SYNC`. | **NO (Zero advance)** | **NO (Zero advance)** |
| **5. Unverifiable Compacted History**<br>$e$ predates compaction horizon and cannot be verified via base snapshot. | `REJECTED_OUTSIDE_HORIZON` | Operation **REJECTED**; if partition stall occurs, transitions to `DEGRADED_OUT_OF_SYNC`. | **NO (Zero advance)** | **NO (Zero advance)** |

#### Detailed Failure Actions:
1. **Rejection Safety:** Under scenarios 3, 4, and 5, the event is immediately isolated and rejected. **The local Lamport clock MUST NOT advance.**
2. **Quarantine & Degradation:**
   - Under `CLOCK_OVERFLOW`, the local engine ceases originating or reducing events, setting `EngineStatus.isDegraded = true` and `degradedReason = 'CLOCK_OVERFLOW'`. Recovery requires an explicit snapshot re-anchor resetting clocks to 0.
   - Under `INVALID_CAUSAL_TIMESTAMP` or `CLOCK_REGRESSION`, the sending peer is recorded in `EngineStats.quarantinedEventsCount`. If violations exceed 3 consecutive events from the same peer, the connection is closed.

---

### Correction 3: Tightened Replay Boundaries & Immutable Operation Contracts

#### 1. Contract for `LOCAL_REPLAY`
`LOCAL_REPLAY` is invoked during snapshot installation or speculative rollback to re-evaluate locally held operations newer than the installed snapshot frontier.
- **Strict Invariance:** `LOCAL_REPLAY` applies **exclusively to immutable operations already accepted and retained in local memory** (`OrderedOperationLog`).
- **Forbidden Actions During Replay:**
  - A local replay **CANNOT** introduce a new origin, a new sequence number, or a new logical timestamp.
  - A local replay **CANNOT** re-evaluate token-bucket rate limits or invoke `validateAndAdvanceRemote`.
  - A local replay **CANNOT** alter `replicationVector` or local Lamport clock.
- **Execution Semantics:** Local replay executes purely through `ConflictResolver.replay(baseState, operations, monotonicNow, context)`. It produces a new `PlaybackState` and a sequence of local audio/timeline `EngineDecision` objects without side-effects on network or clock managers.

#### 2. Contract for `REPAIR_BATCH` Operations
Historical operations arriving in `OPERATION_BATCH` are often labeled "catch-up," but they are **NEW** to the receiver.
- **Exemption Scope:** Repaired operations bypass **ONLY** the physical wall-clock token-bucket rate limiter.
- **Mandatory Invariants:** Every repaired operation must be independently validated against:
  1. Envelope and typed payload schemas.
  2. Recognized origin identity in the room.
  3. Strict origin sequence continuity (must match expected repair gap).
  4. Collision absence (reject conflicting `logicalEventId` for same sequence).
  5. Causal dependency integrity ($L(e) > L_{\text{max\_dep}}$).
  6. Per-origin Lamport monotonicity ($L(e) > L_{\text{prev}}(p)$).
  7. Safe integer limits ($L(e) < 2^{53}-1$).
- **Clock Advancement:** If and only if all validations pass, the local Lamport clock advances:
  $$L_{\text{local}} \leftarrow \max(L_{\text{local}}, L(e)) + 1$$
  If any check fails, the operation is rejected and the clock is untouched.

#### 3. Duplicate Detection Cannot Mask Collisions
In `OrderedOperationLog.insert(operation)`:
- If $(p, s)$ has already been reduced or indexed:
  - If incoming `logicalEventId === storedOperation.logicalEventId` **AND** canonical payloads match: classified as `IGNORED_DUPLICATE` (idempotent NO-OP; clock does not advance).
  - If incoming `logicalEventId !== storedOperation.logicalEventId` **OR** payloads differ: classified as `SEQUENCE_COLLISION_DETECTED`.
  - **Invariant:** Duplicate detection **never** silently accepts a conflicting event. Collisions trigger origin isolation and immediate rejection.

---

## 3. Consistency Review & Architectural Verifications

### 1. Causal Context vs. Replication Vector Separation
- `causalContext`: Stamped by author at creation; sparse map of direct dependencies; immutable across wire and replay; used for causality validation and concurrency detection ($e_1 \parallel e_2$).
- `replicationVector`: Maintained locally by receiver; contiguous vector of consecutive sequences reduced into local state; used for anti-entropy gap detection and digest broadcasting.
- **Status:** 100% separated across all specifications.

### 2. Strict Epoch Fencing
- Bootstrap is the sole coordinator of `membershipEpoch`.
- Partitioned peers cannot independently advance epochs.
- Operations from obsolete epochs ($e.\text{membershipEpoch} < \text{currentEpoch}$) are strictly fenced and rejected upon reconnect (`REJECTED_OBSOLETE_EPOCH`).
- **Status:** 100% consistent across HLD, LLD, and Interface Contracts.

### 3. Snapshot Acceptance Under Cooperative Trust
- Byte integrity (RFC 8785 canonical SHA-256) binds `{ formatVersion: 2, sessionId, membershipEpoch, snapshotAuthorPeerId, snapshotFrontier, maxLogicalTimestamp, state }`.
- Component-wise frontier dominance ($V_{\text{snap}} \ge V_{\text{local}}$) is mandatory.
- SHA-256 and vector dominance do **not** prove semantic correctness without complete replay from Genesis. Semantic correctness is accepted under the Cooperative Non-Byzantine Trust Model (Option C).
- Incomparable snapshots that cannot be repaired trigger `DEGRADED_OUT_OF_SYNC` (arbitrary tie-breaking strictly eliminated).
- **Status:** 100% consistent across all documents.

### 4. Conditional Eventual Convergence
- Theorem preconditions rely strictly on discrete logical properties: connected communication graph, consistent epoch fencing, identical accepted event sets ($E_A = E_B$), causal dependency admissibility, deterministic total order reduction, dominating snapshot recovery, and quiescence.
- Physical clock drift is completely excluded from logical convergence preconditions.
- **Status:** Formally verified and consistent.

---

## 4. Document Traceability Matrix (P7.2-R5)

| Document | Primary Revisions Applied in P7.2-R5 | Status |
| :--- | :--- | :--- |
| `docs/PHASE7_2_R4_FINAL_CORRECTIONS.md` | Annotated header indicating supersession/refinement by R5 for exact timestamp validation, anomalous jumps, and replay boundaries. | Active reference |
| `docs/PHASE7_2_LLD.md` | Section 1.4 updated with exact $L_{\text{max\_dep}}$ 3-tier lookup, anomalous jump classification table, and tightened `LOCAL_REPLAY` / `REPAIR_BATCH` contracts. | Updated |
| `docs/PHASE7_2_PROTOCOL_EVOLUTION_RFC.md` | Envelope and `OperationBatchPayload` documentation updated to reflect exact timestamp validation rules and collision invariants. | Updated |
| `docs/design/PHASE7_INTERFACE_CONTRACTS.md` | `ILogicalClockManager` and `IOrderedOperationLog` updated with exact validation contracts, error codes, and replay boundary invariants. | Updated |
| `docs/PHASE7_2_SIMULATION_TEST_STRATEGY.md` | Suite 10 updated to assert exact 3-tier dependency timestamp validation and sequence collision rejection. | Updated |
| `docs/PHASE7_2_FINAL_DESIGN_REVIEW.md` | AD-701, AD-706, AD-709 updated with R5 exact validation rules and replay boundary contracts. | Updated |
| `docs/design/README.md` | Index updated to feature `PHASE7_2_R5_FINAL_CORRECTIONS.md` as the definitive final specification freeze. | Updated |

