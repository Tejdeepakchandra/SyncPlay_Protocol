# Phase 7.1 — Operation-Specific Conflict Semantics Specification

> **Document Version:** 1.0.0  
> **Status:** COMPLETED & VERIFIED (Milestone P7.1)  
> **Context:** SyncPlay Protocol — Phase 7 Leaderless Distributed Synchronization Engine  

---

## 1. The Necessity of Operation-Specific Conflict Resolution

A common flaw in distributed designs is treating all application mutations as generic opaque writes governed by a uniform "Last-Write-Wins" (LWW) policy. In media playback, operations possess radically different physical and psychoacoustic characteristics:
- A `SEEK` is a non-linear timeline displacement.
- A `PAUSE` is an immediate stop command to freeze playback.
- A `PLAY` is a command to resume forward time progression.
- A `RATE_CHANGE` is a scaling factor modifying position extrapolation velocity.

Each operation pair must be analyzed independently to preserve both mathematical determinism and acceptable user experience.

---

## 2. Independent Operation Analysis

---

### 2.1. SEEK Operations
- **Mathematical Nature:** Non-commutative ($S_{t_1} \circ S_{t_2} \neq S_{t_2} \circ S_{t_1}$).
- **The Concurrency Race:** Peer A seeks to $30\text{ s}$; Peer B seeks to $60\text{ s}$.
- **Resolution Policy: Strict Deterministic Total Order (Highest Lamport Key Wins)**
  $$\text{Winner} = \max_{\text{Key}} (e_A, e_B)$$
  - The operation with the highest total-order key $\langle L, \text{peerId}, \text{eventId} \rangle$ establishes the new `anchorPositionMs`.
  - The superseded seek is recorded in the operational decision history with code `'SUPERSEDED'`.
- **Identical Seek Targets:** If Peer A seeks to $30\text{ s}$ and Peer B seeks to $30\text{ s}$ concurrently, both operations intend the same destination. Applying either results in the identical logical position. The tie-breaker deterministically picks one to be the authoritative `lastAppliedEvent`.

---

### 2.2. PLAY and PAUSE Operations (The Safety Dilemma)
When Peer A issues `PLAY` while Peer B issues `PAUSE` concurrently:

```text
                  CONCURRENT PLAY vs. PAUSE RACE

    Peer A: PLAY(targetPos = 12000)   Peer B: PAUSE(observedPos = 12050)
                   \                               /
                    \                             /
                     ▼                           ▼
        Option A: Strict Total Order     Option B: Pause Priority
        (Highest Lamport Key Wins)       (Pause Always Overrides Concurrent Play)
        ────────────────────────────     ────────────────────────────────────────
        If Play has higher key,          Regardless of key, state pauses at
        state resumes playing.           the observed position.
        Hazard: Sudden audio blast       Advantage: Safe, predictable audio.
        if user needed silence!          User can cleanly resume afterward.
```

#### Evaluation of Options:
- **Option A (Strict Mathematical Total Order):**  
  Treat `PLAY` and `PAUSE` symmetrically based on $\max_{\text{Key}}$. While mathematically pure, it introduces a severe user hazard: if a user clicks `PAUSE` because a phone rang or a baby woke up, and another peer's concurrent `PLAY` has a slightly higher tie-breaker, the video continues blaring sound.
- **Option B (Intent-Preserving Pause Priority — RECOMMENDED):**  
  If a `PAUSE` and a `PLAY` are causally concurrent ($L_{\text{play}} \approx L_{\text{pause}}$ within the conflict window), **`PAUSE` takes precedence**. The playback state halts at the pause anchor position. Any participant can subsequently click play once the room is aligned.
- **Implementation Mechanism:** The reducer evaluates whether an incoming operation is causally concurrent with an antagonistic state transition. If an in-flight pause conflicts with an in-flight play, the pause anchor is committed.

---

### 2.3. RATE_CHANGE Operations
- **Mathematical Nature:** Modifies the continuous velocity multiplier $r \in [0.25, 4.0]$.
- **The Concurrency Race:** Peer A sets $1.5\text{x}$; Peer B sets $2.0\text{x}$.
- **Resolution Policy: Total-Order Scalar LWW**
  - The operation with the highest total-order key establishes the new `playbackRate`.
  - The anchor position is extrapolated up to the moment of rate change and locked as the new `anchorPositionMs`.
- **No-Op Filter:** If the incoming rate matches the current rate ($|r_{\text{new}} - r_{\text{current}}| < 10^{-4}$), it is processed as `NO_OP` without mutating `stateVersion` (preserving Phase 6 invariant).

---

### 2.4. Identical Operations from Multiple Peers
- **Scenario:** The movie finishes or hits an exciting scene; Peer A and Peer B both click `PAUSE` within $100\text{ ms}$ of each other.
- **Deduplication vs. Idempotence:**
  - Are they duplicates? **No.** They have different `senderPeerId` and different `logicalEventId`. Phase 5 deduplication correctly passes both to the SyncEngine.
  - How does the engine handle them?
    1. The first operation (by total order key) is processed as `APPLIED`, setting status to `'paused'` and advancing `stateVersion = v + 1`.
    2. The second operation arrives. Since status is already `'paused'`, the reducer evaluates the operation against the frozen anchor position. If the position difference is within the configured epsilon ($\le 5\text{ ms}$), it is resolved as `NO_OP`.
    3. `stateVersion` is preserved; zero UI thrashing occurs.

---

## 3. Pairwise Concurrent Conflict Matrix

The following exhaustive matrix defines the reducer outcome for every pair of concurrent operations $e_1 \parallel e_2$:

| $e_1 \ \backslash \ e_2$ | `PLAY` | `PAUSE` | `SEEK` | `RATE_CHANGE` |
| :--- | :--- | :--- | :--- | :--- |
| **`PLAY`** | State: `'playing'`. Anchor = $\max_{\text{Key}}$ position. | **Pause Priority:** State transitions to `'paused'`. Audio silenced. | State transitions to target seek position. Status remains `'playing'`. | Rate updated to $e_2$. Playback continues forward at new speed. |
| **`PAUSE`** | **Pause Priority:** State remains `'paused'`. Audio silenced. | Idempotent convergence: State remains `'paused'`. Anchor = $\min(\text{pos}_1, \text{pos}_2)$. | State transitions to target seek position. Status remains `'paused'`. | Rate updated to $e_2$. State remains `'paused'`. |
| **`SEEK`** | State transitions to seek position. Status set to `'playing'`. | State transitions to seek position. Status set to `'paused'`. | **Total Order LWW:** Position set to $\max_{\text{Key}}(e_1, e_2)$. Earlier seek superseded. | Position set to seek target. Rate set to $e_2$. |
| **`RATE_CHANGE`** | Playback resumes forward at rate $e_1$. | State transitions to `'paused'`. Rate set to $e_1$. | Position set to seek target. Rate set to $e_1$. | **Total Order LWW:** Rate set to $\max_{\text{Key}}(e_1, e_2)$. |

---

## 4. Edge Cases and Defensive State Guarantees

1. **Simultaneous Seek-to-Zero and Seek-to-End:**
   - Peer A clicks "Restart" ($\text{SEEK}(0)$); Peer B clicks "Skip to Credits" ($\text{SEEK}(5400000)$).
   - *Resolution:* Pure deterministic total-order key breaks the tie. Whichever operation has the higher Lamport key wins unconditionally. Both peers arrive at the exact same frame.
2. **Buffering-Induced Pauses vs. User Seeks:**
   - If a peer emits an auto-pause due to video buffering while another peer seeks, the seek takes precedence because a seek establishes a new playback timeline origin.
3. **Floating-Point Extrapolation Epsilon:**
   - In accordance with Phase 6 configuration, positions within $\epsilon = 5.0\text{ ms}$ are considered identical to prevent floating-point representation noise from triggering false state version advances.

