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
| **R-M4-12** | High | Low | Transport State | Deferral until SCTP negotiation complete; reject zero/non-finite limits | In-M4 (M4.2, M4.3) |

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
  5. Hard operational bound: loop terminates in at most $K_{\text{max}} = 5$ attempts per chunk (`MAX_SIZING_ITERATIONS = 5`); aborts with `TRANSPORT_LIMIT_EXCEEDED` if exceeded.
  6. Synchronous send failure protection: If `sendSerialized()` returns `{ ok: false }`, chunk is NOT marked as sent, retry counters are preserved, and counters are not corrupted.
- **Verification Plan:** Unit test suite transmitting chunks across simulated SCTP boundaries of 65,536, 32,768, 16,384, and 2,048 bytes, verifying with zero exceptions that measured bytes never exceed the limit and converge in $\le 5$ iterations.

---

### R-M4-2: Chunk Framing Starvation on Severely Constrained Links ($< 2\text{ KB}$)
- **Threat Description:** If a link negotiates an extremely small MTU/SCTP limit ($< 2,048$ bytes), the JSON envelope overhead ($\approx 1600$ bytes under worst-case schema field lengths) consumes the majority of the message budget, leaving trivial binary payload slices ($< 256$ bytes) that require thousands of chunks, starving the transfer. Furthermore, a 4 MiB snapshot would require thousands of chunks over a 2 KiB link, contradicting the 128-chunk bound.
- **Root Cause:** Attempting large state snapshot recovery over pathologically constrained network links without evaluating capacity invariants.
- **Mitigation:**
  1. Exact Upfront Admission Gate: The protocol computes exact required chunks upfront:
     $$\text{chunkBinaryCapacity} = \min\left(32768, \left\lfloor \frac{\text{effectiveLimit} - 1600}{4} \right\rfloor \times 3\right)$$
     $$\text{calculatedRequiredChunks} = \left\lceil \frac{\text{totalSizeBytes}}{\text{chunkBinaryCapacity}} \right\rceil$$
  2. If $\text{calculatedRequiredChunks} > \text{MAX\_CHUNKS\_PER\_SNAPSHOT}$ (128), the transfer is rejected immediately before allocating reassembly buffer memory (`CHUNK_COUNT_EXCEEDED`).
  3. If $\text{offer.totalChunks} !== \text{calculatedRequiredChunks}$, the offer is rejected upfront with `OFFER_INCONSISTENT`.
  4. The protocol explicitly **does NOT claim universal 4 MiB snapshot transfer support on 2 KiB transports**; effective capacity on 2 KiB transports is bounded to $\approx 42.0\text{ KiB}$ (43,008 B) for 128 chunks. Full 32 KB binary chunks require an effective transport limit $\ge 45,292$ bytes (43,692 B Base64 + 1,600 B envelope).
  5. Option A ASCII identifier restriction (`/^[a-zA-Z0-9_.:-]+$/`) guarantees no JSON escaping overhead expands identifier fields beyond their character lengths.
  6. `calculateChunkCapacity` throws if `effectiveLimit < MIN_SUPPORTED_SCTP_LIMIT` (2,048 bytes).
  7. The iterative loop aborts with `TRANSPORT_LIMIT_EXCEEDED` if slice size drops below `MIN_CHUNK_BYTES = 256`.
  8. Transfer terminates cleanly to `DEGRADED`, prompting application intervention rather than hanging.
- **Verification Plan:** Test case with SCTP limit set to 1,024 bytes confirms immediate, clean rejection; test case with 4 MiB snapshot on 2,048-byte SCTP limit confirms upfront admission rejection without buffer allocation; test case with inconsistent `totalChunks` confirms rejection before buffer allocation.

---

### R-M4-3: Memory Exhaustion from Interleaved or Concurrent Transfers
- **Threat Description:** Malicious or buggy peers sending concurrent `SNAPSHOT_OFFER` or `SNAPSHOT_CHUNK` messages could cause unbounded allocation of reassembly buffers, exhausting client memory. Additionally, previous estimates underestimated peak memory by omitting V8 heap representations.
- **Root Cause:** Lack of concurrency control and buffering limits, plus unaccounted V8 string and object graph memory.
- **Mitigation:**
  1. Strict single active transfer constraint: A recovering node permits at most 1 active inbound transfer context.
  2. Hard bounds on buffer: Max chunks $\le 128$, max total size $\le 4\text{ MB}$.
  3. Conservative theoretical memory budget established: $\approx 28\text{–}32\text{ MiB}$ upper bound accounting for sparse slices, contiguous buffer, V8 Latin-1/UTF-16 string heap, parsed `StateSnapshotPayloadV2` object tree, canonical RFC 8785 JSON string, SHA-256 buffer, and transient parser allocations.
  4. Copy-minimizing design: In `ChunkAssembler.finalizeAssembly()`, sparse chunk slice array is immediately dereferenced (`this.chunks = null`) upon contiguous `Uint8Array` allocation.
  5. Immediate cleanup of buffers, maps, retry state, and timers on abort, timeout, or epoch change via explicit `dispose()`.
  6. Explicit acknowledgment that JavaScript GC is asynchronous and non-deterministic; reference nullification ensures immediate eligibility for collection.
- **Verification Plan:** Unit test flooding concurrent offers while a transfer is in flight confirms subsequent offers are rejected without allocating secondary buffers; heap profile test confirms immediate reference release on finalization and disposal.

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
- **Threat Description:** If `this.reducer` throws in loop 2 of `compactAtFrontier()`, operations have already been spliced from `this.operations`, leaving partial log mutation and corrupting the replication engine.
- **Root Cause:** Splicing operations before verifying that state reduction succeeds.
- **Mitigation:**
  1. Designated as **Milestone M4.0d (Prerequisite)**.
  2. Complete Staged Mutation Inventory: Out-of-band staging across all 8 mutable fields:
     - `baseSnapshot`: Deep clone via `structuredClone(this.baseSnapshot)`.
     - `operations`: Shallow array filter containing retained `StoredOperation` references.
     - `idIndex` & `originSeqIndex`: Reconstructed Maps populated from candidate operations.
     - `compactedCheckpoints`: Shallow Map clone with immutable records and 256-entry FIFO eviction.
     - `originFrontierTimestamps`: Shallow Map clone with 64-entry FIFO eviction.
     - `baseSnapshotFrontier`: Shallow Record clone `{ ...this.baseSnapshotFrontier }`.
     - `hasPrunedOperations`: Scalar boolean copy.
  3. No active state structure or reference is modified before all candidate computations and reductions complete successfully.
  4. If reduction throws, candidate objects are discarded, the exception propagates, and zero active mutations occur (`ReducerFailure => ActiveStateAfter === ActiveStateBefore`).
- **Verification Plan:** Test case with throwing reducer in `compactAtFrontier` confirms active operations, indexes, and checkpoints remain 100% intact with identical references and content.

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

---

### R-M4-12: Unnegotiated or Invalid SCTP Transport State
- **Threat Description:** Initiating snapshot transfer requests while `RTCSctpTransport.maxMessageSize` is still `undefined` (handshake in progress) or when the transport reports non-finite or zero values, leading to erroneous chunk sizing or silent frame drops.
- **Root Cause:** Sizing wire frames against unnegotiated or invalid SCTP transport parameters.
- **Mitigation:**
  1. Transfer initiation is strictly deferred until transport connection state is `open` and SCTP parameters are settled.
  2. Values $\le 0$ or non-finite are rejected as invalid transport states.
  3. Fallback of 65,536 bytes on open channels is strictly documented as a conservative local implementation cap, not proof of remote capacity.
  4. Synchronous `RTCDataChannel.send()` failures preserve chunk retry state and transfer counters without marking unsent frames as complete.
- **Verification Plan:** Unit tests verifying transfer deferral on `undefined` SCTP limits, immediate rejection on zero/negative limits, and accurate retry accounting on synchronous send exceptions.

---

### R-M4-13: Facade Capability Custody Bypass (Milestone M4.0c)
- **Threat Description:** Ordinary application consumers invoke `LeaderlessSyncEngine.recordPeerDigest()` directly, bypassing `TrustedTransportAdapter` session, sender, and epoch validations to inject unverified digests into `CompactionManager`.
- **Root Cause:** Publicly exposed registration entry point without in-process capability gating.
- **Mitigation:**
  1. Designated as **Milestone M4.0c (Prerequisite)**.
  2. Restrict `recordPeerDigest()` via in-process capability token `INTERNAL_DIGEST_ACCESS_TOKEN` or internal coordinator interface.
  3. Classify explicitly as an in-process API custody boundary, not a security sandbox.
- **Verification Plan:** Unit test verifying that external invocation without internal token is rejected at compile/run time while internal composition remains functional.
