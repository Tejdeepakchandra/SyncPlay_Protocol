# Phase 7.5 Milestone M4 — Snapshot Transfer Risk Register

**Status:** APPROVED FOR PLANNING (DOCUMENTATION-ONLY)  
**Date:** 2026-11-19  
**Refinement Date:** 2026-11-19  
**Branch Baseline:** `main` at `3b90116`  
**Parent Implementation Baseline:** `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`  
**Specification Baseline:** `119da194c3feb883cfd61b6928cb81d4b200ac5d`  

---

## 1. Risk Matrix Overview

| Risk ID | Severity | Likelihood | Impact Area | Mitigation Strategy | Pre-M4 / In-M4 Scope |
|:---|:---:|:---:|:---|:---|:---:|
| **R-M4-1** | **CRITICAL** | High | Transport Boundary | Normative 7-step iterative send-boundary loop measuring exact UTF-8 bytes | In-M4 (M4.2) |
| **R-M4-2** | High | Medium | Constrained Networks | Fail-fast abort if SCTP limit $< 2,048$ bytes or chunk $< 256$ bytes | In-M4 (M4.2) |
| **R-M4-3** | High | Low | Memory Consumption | Strict single active transfer constraint, bounded buffers, max 128 chunks | In-M4 (M4.1, M4.3) |
| **R-M4-4** | High | Medium | Data Integrity | Two-tier hash verification: per-chunk SHA-256 and full canonical RFC 8785 SHA-256 | In-M4 (M4.1, M4.4) |
| **R-M4-5** | High | Medium | Connection Stability | Transport lifecycle event listeners aborting immediately on disconnect or gen change | In-M4 (M4.4) |
| **R-M4-6** | High | Low | Causal Consistency | Rejection of offers with stale epoch or non-dominating frontier | In-M4 (M4.3) |
| **R-M4-7** | **CRITICAL** | Medium | Trust Boundary | Strict transport identity binding (`senderPeerId === fromPeerId`) | **Pre-M4 (M4.0a)** |
| **R-M4-8** | High | Medium | Clock Metadata | Strict sequence-specific timestamp resolution (no loose fallback) | **Pre-M4 (M4.0b)** |
| **R-M4-9** | High | Low | Engine Compaction | Staged candidate base snapshot reduction before splicing active operations | **Pre-M4 (M4.0d)** |
| **R-M4-10** | High | Low | Failover Integrity | Strict snapshot identity match (`snapshotId` & `sha256Digest`) before retaining chunks | In-M4 (M4.1, M4.3) |
| **R-M4-11** | Medium | Medium | Availability | Dual retry budget: max 3 per chunk, max 10 total transfer retries, 60s deadline | In-M4 (M4.3) |

---

## 2. Detailed Threat Analysis & Mitigation Plans

### R-M4-1: SCTP Message Size Overshoot During Base64 Expansion
- **Threat Description:** WebRTC DataChannels strictly drop messages exceeding negotiated SCTP limits. Base64 encoding expands binary data by $\approx 33\%$, and JSON envelope strings add variable UTF-8 character lengths (e.g. multi-byte characters, UUIDs). A naive static binary slice calculation will exceed the SCTP limit on edge sizes.
- **Root Cause:** Estimating serialized wire size from binary byte count without accounting for exact JSON formatting and Base64 padding.
- **Mitigation:**
  1. The normative 7-step iterative send-boundary loop.
  2. The message is fully constructed, serialized to JSON, and measured via `TextEncoder().encode().byteLength`.
  3. If $\text{byteLength} > \text{effectiveLimit}$, the binary slice is mathematically reduced by $\Delta = \lceil \text{overshoot} \times 0.75 \rceil + 32$ and re-serialized until fitting.
  4. The exact tested string is passed unchanged to `RTCDataChannel.send()`.
- **Verification Plan:** Unit test suite transmitting chunks across simulated SCTP boundaries of 65,536, 32,768, 16,384, and 2,048 bytes, verifying with zero exceptions that measured bytes never exceed the limit.

---

### R-M4-2: Chunk Framing Starvation on Severely Constrained Links ($< 2\text{ KB}$)
- **Threat Description:** If a link negotiates an extremely small MTU/SCTP limit ($< 2,048$ bytes), the JSON envelope overhead ($\approx 768$ bytes) consumes the majority of the message budget, leaving trivial binary payload slices ($< 256$ bytes) that require thousands of chunks, starving the transfer.
- **Root Cause:** Pathologically constrained network links attempting large state snapshot recovery.
- **Mitigation:**
  1. `calculateDynamicChunkSize` throws if `effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT` (2,048 bytes).
  2. The iterative loop aborts with `TRANSPORT_LIMIT_EXCEEDED` if slice size drops below `MIN_CHUNK_BYTES = 256`.
  3. Transfer terminates cleanly to `DEGRADED`, prompting application intervention rather than hanging.
- **Verification Plan:** Test case with SCTP limit set to 1,024 bytes confirms immediate, clean rejection.

---

### R-M4-3: Memory Exhaustion from Interleaved or Concurrent Transfers
- **Threat Description:** Malicious or buggy peers sending concurrent `SNAPSHOT_OFFER` or `SNAPSHOT_CHUNK` messages could cause unbounded allocation of reassembly buffers, exhausting client memory.
- **Root Cause:** Lack of concurrency control and buffering limits on incoming recovery messages.
- **Mitigation:**
  1. Strict single active transfer constraint: A recovering node permits at most 1 active inbound transfer context.
  2. Hard bounds on buffer: Max chunks $\le 128$, max total size $\le 4\text{ MB}$.
  3. Pre-allocated sparse array bounded to declared `totalChunks`.
  4. Immediate cleanup of buffers on abort, timeout, or epoch change via explicit `dispose()`.
- **Verification Plan:** Unit test flooding concurrent offers while a transfer is in flight confirms subsequent offers are rejected without allocating secondary buffers.

---

### R-M4-4: Poison Chunk Attack or Payload Bit-Flips
- **Threat Description:** Bit-flips in WebRTC transit or a buggy supplier sending corrupted chunk data could poison the reassembly buffer or crash JSON deserialization.
- **Root Cause:** Assembling payload without verifying individual slice integrity.
- **Mitigation:**
  1. Two-tier cryptographic verification:
     - Tier 1: Each `SNAPSHOT_CHUNK` includes `chunkSha256`. The raw binary bytes are hashed immediately upon Base64 decoding; mismatches are rejected before touching the assembler array.
     - Tier 2: The reassembled contiguous buffer is verified against the offer's `sha256Digest` using canonical RFC 8785 JSON SHA-256 before emitting to M5.
- **Verification Plan:** Unit tests with single-bit mutations in Base64 chunk payloads and tampered final digests confirm rejection at both Tier 1 and Tier 2.

---

### R-M4-5: Supplier Disconnect or Split-Brain During Multi-Chunk Transfer
- **Threat Description:** A supplier peer disconnects or experiences a network partition halfway through a 128-chunk transfer.
- **Root Cause:** Unhandled transport lifecycle events leaving transfer state machines permanently stuck in `RECOVERING`.
- **Mitigation:**
  1. `SnapshotTransferManager` listens to `PeerManager` status events.
  2. If supplier status transitions to `DISCONNECTED`, `CLOSED`, or `FAILED`: active transfer triggers failover if an alternate eligible supplier is known, or aborts with `SUPPLIER_DISCONNECTED`.
  3. Per-chunk timeout (5,000ms) and overall transfer timeout (60,000ms) serve as fallback tripwires.
- **Verification Plan:** Test case simulating supplier disconnect during chunk 4 of 8 confirms failover with chunk retention when replacement matches, and clean abort when no replacement exists.

---

### R-M4-6: Stale Snapshot Offered by Lagging or Partitioned Peer
- **Threat Description:** A peer that was previously partitioned rejoins and offers a snapshot from an older epoch or with an obsolete causal frontier.
- **Root Cause:** Accepting snapshot offers without evaluating epoch currency and frontier domination.
- **Mitigation:**
  1. Offers with `membershipEpoch < activeEpoch` are rejected immediately.
  2. In M5 pre-validation, snapshot frontier must dominate or equal local frontier; older-epoch snapshots are checked against the mechanical non-supersession predicate.
- **Verification Plan:** Test case verifying rejection of offers with stale membership epochs.

---

### R-M4-7: Sender Identity Spoofing in Transport Messages (Limitation A)
- **Threat Description:** `TrustedTransportAdapter` routes messages based on payload contents without verifying that `msg.payload.senderPeerId === inbound.fromPeerId`. An untrusted peer could send messages claiming to be a different peer.
- **Root Cause:** Missing identity assertion at the transport adapter boundary.
- **Mitigation:**
  1. Designated as **Milestone M4.0a (Prerequisite)**.
  2. Assert `msg.payload.senderPeerId === inbound.fromPeerId` and `msg.payload.connectionGeneration === peerStatus.generation` for all recovery messages.
  3. Discard violations with zero engine or anti-entropy mutation.
- **Verification Plan:** Test case injecting spoofed `senderPeerId` over valid DataChannel confirms complete drop.

---

### R-M4-8: Inexact Frontier Timestamp Resolution (Limitation B)
- **Threat Description:** `getOriginFrontierTimestamps` falls back to `this.originFrontierTimestamps.get(origin)`, which tracks only the latest pruned timestamp without sequence binding, potentially substituting a later sequence's timestamp.
- **Root Cause:** Reusing a single per-origin tracker map for sequence-specific queries.
- **Mitigation:**
  1. Designated as **Milestone M4.0b (Prerequisite)**.
  2. Strict sequence-specific resolution: check active log and compacted checkpoints only; return explicit unresolvable/unavailable status if missing.
  3. Never substitute a later or earlier timestamp.
  4. Snapshot export and offer evaluation reject snapshots with incomplete exact metadata.
- **Verification Plan:** Test case requesting sequence 5 when only sequence 6 was evicted confirms explicit unresolvable result instead of sequence 6 timestamp.

---

### R-M4-9: Compaction Exception Atomicity (Limitation C)
- **Threat Description:** If `this.reducer` throws in loop 2 of `compactAtFrontier()`, operations have already been spliced from `this.operations`, leaving partial log mutation.
- **Root Cause:** Splicing operations before verifying that state reduction succeeds.
- **Mitigation:**
  1. Designated as **Milestone M4.0d (Prerequisite)**.
  2. Transactional candidate staging: compute candidate base state reduction on an out-of-band clone before splicing active arrays or mutating indexes.
  3. If reduction throws, zero mutations occur.
- **Verification Plan:** Test case with throwing reducer in `compactAtFrontier` confirms active operations and checkpoints remain 100% intact.

---

### R-M4-10: Chunk Mixing Across Different Snapshot Identities During Failover
- **Threat Description:** When a supplier disconnects mid-transfer, an alternate supplier is selected that offers a different snapshot ID or different digest. Ingesting chunks from both suppliers into the same assembler corrupts state.
- **Root Cause:** Retaining chunk assembler buffer across failover without validating snapshot ID and digest equivalence.
- **Mitigation:**
  1. In `ChunkAssembler.prepareForFailover()`, compare `newSnapshotId === activeSnapshotId` and `newSha256 === activeSha256`.
  2. If both match bit-exact, retained chunks are preserved.
  3. If either differs, **all previously received chunks are purged immediately**. Mixing chunks from different snapshot identities is impossible.
- **Verification Plan:** Test case offering differing snapshot ID during failover confirms complete chunk purge and clean fresh transfer initiation.

---

### R-M4-11: Unbounded Retransmission Loops on Chronically Degraded Links
- **Threat Description:** On a link with persistent packet loss, retransmission requests could repeat endlessly without terminating.
- **Root Cause:** Per-chunk retry timers without an overall transfer retry budget or wall-clock deadline.
- **Mitigation:**
  1. Per-chunk retry limit: Max 3 retries per chunk index (`MAX_CHUNK_RETRIES = 3`).
  2. Total transfer retry budget: Max 10 retransmissions across all chunks (`MAX_TRANSFER_RETRIES = 10`).
  3. Hard wall-clock transfer deadline: 60 seconds (`OVERALL_TRANSFER_TIMEOUT_MS = 60000`).
  4. Exceeding any budget triggers clean termination with `TRANSFER_TIMEOUT` or `RETRY_BUDGET_EXHAUSTED`.
- **Verification Plan:** Test case simulating 100% loss on a single chunk confirms termination after exactly 3 retries.
