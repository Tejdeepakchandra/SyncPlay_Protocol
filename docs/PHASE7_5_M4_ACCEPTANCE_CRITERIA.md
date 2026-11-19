# Phase 7.5 Milestone M4 — Snapshot Transfer Acceptance Criteria

**Status:** APPROVED FOR PLANNING (DOCUMENTATION-ONLY)  
**Date:** 2026-11-19  
**Refinement Date:** 2026-11-19  
**Branch Baseline:** `main` at `3b90116`  
**Parent Implementation Baseline:** `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`  
**Specification Baseline:** `119da194c3feb883cfd61b6928cb81d4b200ac5d`  

---

## 1. Overview & Verification Framework

This document defines the formal, measurable acceptance criteria for Phase 7.5 Milestone M4 (Chunked Snapshot Transfer Protocol). Each criterion specifies exact pass/fail thresholds, test conditions, and associated failure codes.

---

## 2. Pre-M4 Security, Metadata & Compaction Criteria (AC-M4-PRE)

### AC-M4-PRE-1: `STATE_DIGEST` Transport Sender Identity Binding
- **Condition:** Inbound `STATE_DIGEST` message received via `TrustedTransportAdapter`.
- **Pass Threshold:**
  1. `msg.payload.senderPeerId === inbound.fromPeerId` is verified.
  2. `msg.payload.connectionGeneration === peerStatus.generation` is verified.
  3. If either field differs, the message is immediately dropped with zero mutations to `CompactionManager` or `AntiEntropyManager`.
  4. Unit test confirms that forged digests from Peer X claiming `senderPeerId: 'peer-y'` are completely discarded.

### AC-M4-PRE-2: Strict Sequence-Specific Timestamp Resolution
- **Condition:** `getOriginFrontierTimestamps(frontier)` is called with origin sequences that have been pruned and whose checkpoints were evicted from the 256-entry FIFO cache.
- **Pass Threshold:**
  1. If an exact sequence timestamp is not present in active log or retained checkpoints, it is reported as unresolvable / unavailable (`null`).
  2. The function **NEVER** returns a timestamp belonging to a different sequence number.
  3. Snapshot creation fails if required frontier timestamps cannot be established exactly.

### AC-M4-PRE-3: Compaction Exception Atomicity (8-Component Mutation Inventory)
- **Condition:** `compactAtFrontier()` is invoked with a throwing reducer callback.
- **Pass Threshold:**
  1. Candidate mutation set across all 8 mutable fields is staged out-of-band:
     - `baseSnapshot`: Deep clone via `structuredClone(this.baseSnapshot)`.
     - `operations`: Shallow array filter containing retained `StoredOperation` references.
     - `idIndex`: Reconstructed `Map<string, StoredOperation>` populated from candidate operations.
     - `originSeqIndex`: Reconstructed `Map<string, StoredOperation>` populated from candidate operations.
     - `compactedCheckpoints`: Shallow Map clone `new Map(this.compactedCheckpoints)` with immutable records and 256-entry FIFO eviction.
     - `originFrontierTimestamps`: Shallow Map clone `new Map(this.originFrontierTimestamps)` with 64-entry FIFO eviction.
     - `baseSnapshotFrontier`: Shallow Record clone `{ ...this.baseSnapshotFrontier }`.
     - `hasPrunedOperations`: Scalar boolean copy.
  2. If the reducer throws at any point during candidate reduction, the exception is propagated, all candidate objects are discarded, and `this.baseSnapshot`, `this.operations`, `this.idIndex`, `this.originSeqIndex`, `this.compactedCheckpoints`, `this.originFrontierTimestamps`, `this.baseSnapshotFrontier`, and `this.hasPrunedOperations` remain 100% untouched.
  3. Formal Proof Criterion:
     $$\mathbf{\text{ReducerFailure}} \implies \mathbf{\text{ActiveStateAfter} \equiv \text{ActiveStateBefore}}$$

---

## 3. Snapshot Identity & Authorship Criteria (AC-M4-AUTH)

### AC-M4-AUTH-1: Immutable Snapshot Identity vs Ephemeral Transfer Identity
- **Condition:** Generation of snapshot offer and transfer requests across multiple forwarding peers.
- **Pass Threshold:**
  1. `authorPeerId` is bound into `StateSnapshotPayloadV2` and canonical SHA-256 digest preimage.
  2. Forwarding supplier `supplierPeerId` is excluded from the digest preimage and is never recorded as the author.
  3. `transferId` is regenerated uniquely per transfer attempt.

### AC-M4-AUTH-2: Supplier Possession Verification
- **Condition:** Supplier transmits chunks for an advertised offer.
- **Pass Threshold:**
  1. Every chunk's Base64 payload decodes to raw binary matching `chunkSha256`.
  2. Reassembled contiguous buffer decodes as valid `StateSnapshotPayloadV2`.
  3. Canonical RFC 8785 JSON SHA-256 digest of reassembled payload matches `offer.sha256Digest` bit-exact.
  4. If supplier fails chunk hash or overall digest verification, transfer immediately aborts with `CHUNK_CORRUPTED` or `HASH_MISMATCH`.

### AC-M4-AUTH-3: M3 Snapshot Digest Compatibility Verification
- **Condition:** Audit of M4 wire structures against frozen Milestone M3 runtime implementation at commit `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`.
- **Pass Threshold:**
  1. Direct code inspection verifies that `snapshotId`, `authorPeerId`, `originFrontierTimestamps`, and `compactedCheckpoints` already exist in `StateSnapshotPayloadV2Schema` and `computeSnapshotDigestV2` preimage.
  2. Zero new fields are added to the snapshot digest preimage in M4.
  3. Format version remains `formatVersion: 2` with zero schema drift.

---

## 4. Upfront Admission & Capacity Criteria (AC-M4-ADMIT)

### AC-M4-ADMIT-1: Upfront Admission Calculation & Chunk Ceiling Invariant
- **Condition:** Inbound `SNAPSHOT_OFFER` received or outbound `SNAPSHOT_REQUEST` handled over any supported transport.
- **Pass Threshold:**
  1. `calculateSnapshotAdmission()` executes exact sizing:
     $$\text{chunkBinaryCapacity} = \min\left(32768, \left\lfloor \frac{\text{effectiveLimit} - 1024}{4} \right\rfloor \times 3\right)$$
     $$\text{calculatedRequiredChunks} = \left\lceil \frac{\text{totalSizeBytes}}{\text{chunkBinaryCapacity}} \right\rceil$$
  2. Invariant 1 verified: $\text{calculatedRequiredChunks} \le \text{MAX\_CHUNKS\_PER\_SNAPSHOT}$ (128). If exceeded, rejected upfront with `CHUNK_COUNT_EXCEEDED` before allocating reassembly buffer memory.
  3. Invariant 2 verified: $\text{offer.totalChunks} === \text{calculatedRequiredChunks}$. If inconsistent, rejected upfront with `OFFER_INCONSISTENT` before allocating reassembly buffer memory.
  4. Explicitly verified that universal 4 MiB transfer is **NOT** claimed on a 2 KiB SCTP transport (which admits snapshots up to 96 KiB for 128 chunks).

### AC-M4-ADMIT-2: Minimum Supported SCTP Limit Floor
- **Condition:** Transport reporting SCTP limit $< 2,048$ bytes (`MIN_SUPPORTED_SCTP_LIMIT`).
- **Pass Threshold:**
  1. Transfer is immediately rejected/aborted with `TRANSPORT_LIMIT_EXCEEDED`.
  2. Zero chunk requests or transfer buffers are allocated.

### AC-M4-ADMIT-3: SCTP Negotiation Deferral and State Handling
- **Condition:** Peer transport where SCTP parameters are in various negotiation states.
- **Pass Threshold:**
  1. If `sctpMaxMessageSize` is `undefined` (negotiation underway), transfer initiation is deferred until negotiation completes.
  2. If `sctpMaxMessageSize` is $\le 0$ or non-finite, transfer is deferred or rejected as invalid transport state.
  3. Fallback of 65,536 bytes on open channels is verified to be a local application cap, not proof of remote capacity.

---

## 5. Supplier Failover Criteria (AC-M4-FAILOVER)

### AC-M4-FAILOVER-1: Chunk Retention on Matching Snapshot Failover
- **Condition:** Supplier disconnects after transmitting chunks $0 \dots 3$ of an 8-chunk snapshot. Alternate supplier offers identical `snapshotId` and `sha256Digest`.
- **Pass Threshold:**
  1. Active transfer attempt with supplier 1 is invalidated.
  2. Chunks $0 \dots 3$ are retained in `ChunkAssembler`.
  3. A new `transferId` is generated for supplier 2.
  4. `SNAPSHOT_REQUEST` requests only missing chunks $4 \dots 7$.
  5. Transfer completes successfully.

### AC-M4-FAILOVER-2: Complete Chunk Purge on Mismatched Snapshot Failover
- **Condition:** Supplier disconnects after transmitting chunks $0 \dots 3$. Alternate supplier offers a different `snapshotId` or different `sha256Digest`.
- **Pass Threshold:**
  1. All previously received chunks $0 \dots 3$ are purged immediately from `ChunkAssembler`.
  2. Fresh transfer begins requesting chunks $0 \dots N$ of the new snapshot.
  3. Chunks from different snapshot identities are **never mixed**.

### AC-M4-FAILOVER-3: Safe Termination When All Suppliers Fail
- **Condition:** Active supplier disconnects and no alternate eligible supplier exists.
- **Pass Threshold:**
  1. Transfer aborts with `SUPPLIER_DISCONNECTED`.
  2. Engine state remains in recovery without modifying active playback state or causal vectors.

---

## 6. Duplicate, Stale & Corrupt Chunk Criteria (AC-M4-CHUNK)

### AC-M4-CHUNK-1: Idempotent Duplicate Chunk Handling
- **Condition:** Duplicate `SNAPSHOT_CHUNK` with identical `chunkIndex` and `chunkSha256` arrives.
- **Pass Threshold:**
  1. Duplicate is silently ignored.
  2. Chunk is not re-decoded, re-inserted, or re-counted in received bytes.
  3. Memory usage remains constant.

### AC-M4-CHUNK-2: Corrupt Chunk Collision Abort
- **Condition:** Incoming `SNAPSHOT_CHUNK` has `chunkIndex` matching an already received chunk but differs in content or `chunkSha256`, or fails raw binary SHA-256 verification.
- **Pass Threshold:**
  1. Chunk is rejected immediately.
  2. Transfer terminates with `CHUNK_CORRUPTED`.
  3. `SNAPSHOT_ABORT` is emitted.

### AC-M4-CHUNK-3: Stale Context Discard
- **Condition:** Late chunk arrives with cancelled `transferId`, stale `membershipEpoch`, or stale supplier generation.
- **Pass Threshold:**
  1. Chunk is dropped at transport boundary with zero allocation.

### AC-M4-CHUNK-4: Missing Index Validation
- **Condition:** `SNAPSHOT_CHUNK_REQUEST` received from requester.
- **Pass Threshold:**
  1. All indices verified to be unique and in range $[0, \text{totalChunks}-1]$.
  2. Number of requested indices strictly $\le \text{MAX\_CONCURRENT\_CHUNK\_REQUESTS} = 4$.

---

## 7. Retry & Timeout Budget Criteria (AC-M4-BUDGET)

### AC-M4-BUDGET-1: Bounded Per-Chunk Retries
- **Condition:** A chunk index experiences repeated transmission failure.
- **Pass Threshold:**
  1. At most 3 retransmission requests are issued for the same chunk index.
  2. The 4th failure triggers immediate transfer abort with `TRANSFER_TIMEOUT`.

### AC-M4-BUDGET-2: Total Transfer Retransmission Budget
- **Condition:** Intermittent packet drops across multiple chunk indices.
- **Pass Threshold:**
  1. Cumulative retransmissions across the entire transfer capped at 10 (`MAX_TRANSFER_RETRIES = 10`).
  2. Exceeding 10 cumulative retransmissions aborts the transfer.

### AC-M4-BUDGET-3: Absolute Wall-Clock Deadline
- **Condition:** Transfer remains incomplete after 60 seconds (`OVERALL_TRANSFER_TIMEOUT_MS = 60000`).
- **Pass Threshold:**
  1. Transfer unconditionally aborts with `TRANSFER_TIMEOUT`.
  2. All allocated assembler memory is released.

---

## 8. Dynamic Sizing & Termination Criteria (AC-M4-SIZE)

### AC-M4-SIZE-1: Iterative Send-Boundary Measurement & Strict Monotonic Decrease
- **Condition:** `SnapshotTransferManager` prepares chunks across negotiated SCTP limits from 2,048 to 65,536 bytes.
- **Pass Threshold:**
  1. Each unsuccessful sizing iteration strictly reduces binary slice size by at least 33 bytes ($\Delta = \lceil \text{overshoot} \times 0.75 \rceil + 32$).
  2. Loop strictly terminates in at most 993 theoretical iterations (proven bounded by initial slice $\le 32,768$ bytes).
  3. Measured UTF-8 bytes of final serialized JSON string strictly $\le \text{effectiveLimit}$.
  4. The exact tested string is passed unchanged to `RTCDataChannel.send()`.

### AC-M4-SIZE-2: Minimum Size Abort
- **Condition:** SCTP limit $< 2,048$ bytes or chunk binary slice $< 256$ bytes.
- **Pass Threshold:**
  1. Sizing loop halts and transfer aborts with `TRANSPORT_LIMIT_EXCEEDED`.

### AC-M4-SIZE-3: Hard Operational Sizing Iteration Cap
- **Condition:** Repeated sizing reduction on pathological framing edge cases.
- **Pass Threshold:**
  1. Number of serialization and sizing attempts per chunk strictly capped at $K_{\text{max}} = 5$ (`MAX_SIZING_ITERATIONS = 5`).
  2. If the serialized frame does not fit within 5 iterations, transfer aborts immediately with `TRANSPORT_LIMIT_EXCEEDED`.
  3. Zero oversized or corrupted frames are ever transmitted to the network.

### AC-M4-SIZE-4: Synchronous Send Failure Handling
- **Condition:** Underlying `RTCDataChannel.send()` throws or `PeerManager.sendSerialized()` returns `{ ok: false }`.
- **Pass Threshold:**
  1. The unsent chunk is strictly **NOT** marked as transmitted or in-flight; window credit is not consumed.
  2. Retry state is preserved: `chunkRetryCount[chunkIndex]` is incremented without desynchronization.
  3. Transfer counters (`receivedBytes`, `sentBytes`, `percentComplete`) remain un-incremented and uncorrupted.
  4. If failure is due to disconnected peer or closed channel, transfer cleanly aborts with `SUPPLIER_DISCONNECTED`.
  5. If failure is due to transient buffer backpressure and retries remain, supplier re-attempts transmission according to documented backoff.
  6. The complete `ProtocolMessage` UTF-8 byte measurement and unchanged-string send boundary are 100% preserved.

---

## 9. Memory & Resource Budget Criteria (AC-M4-MEM)

### AC-M4-MEM-1: Hard Concurrency & Conservative Memory Budget
- **Condition:** High-load multi-peer operation and 4 MiB snapshot transfer.
- **Pass Threshold:**
  1. Requester permits strictly 1 active inbound transfer context.
  2. Supplier permits at most 2 active outbound transfers.
  3. Pre-allocated reassembly buffer bounded to declared `totalChunks` ($\le 128$).
  4. Theoretical peak memory during reassembly and verification bounded to $\le 32\text{ MiB}$ (accounting for sparse binary slices, contiguous buffer, V8 Latin-1/UTF-16 string heap, parsed `StateSnapshotPayloadV2` object tree, canonical RFC 8785 JSON string, SHA-256 byte buffer, and parser transient allocations).

### AC-M4-MEM-2: Complete Lifecycle Memory Dereferencing
- **Condition:** Transfer completion, timeout, cancellation, or epoch increment.
- **Pass Threshold:**
  1. `dispose()` sets all slice references (`this.chunks = null`) and assembled buffer references (`this.assembledBuffer = null`).
  2. All active timers (`overallTimeout`, `chunkTimeout`) are cleared via `clearTimeout()`.
  3. Transfer descriptors and retry maps are deleted.

### AC-M4-MEM-3: Copy Minimization and Asynchronous GC Reality
- **Condition:** Reassembly finalization in `ChunkAssembler.finalizeAssembly()`.
- **Pass Threshold:**
  1. Sparse chunk slice array is dereferenced (`this.chunks = null`) immediately upon contiguous `Uint8Array` allocation, preventing simultaneous retention of duplicate binary representations during JSON parsing.
  2. The specification and tests explicitly acknowledge that JavaScript garbage collection is asynchronous and non-deterministic; nullifying references ensures prompt eligibility for V8 garbage collection cycles without memory retention leaks.

---

## 10. M5 Handoff Boundary Criteria (AC-M4-HANDOFF)

### AC-M4-HANDOFF-1: Clean Handoff to M5 Callback
- **Condition:** Successful completion of snapshot reassembly and verification.
- **Pass Threshold:**
  1. `SnapshotTransferResult` containing verified `StateSnapshotPayloadV2` and raw bytes is emitted to registered handoff callback.
  2. M4 code **DOES NOT** call `engine.importSnapshot()`, touch `PlaybackState`, reset `OrderedOperationLog`, or reseed clocks.
