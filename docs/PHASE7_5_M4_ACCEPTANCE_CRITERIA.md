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

### AC-M4-PRE-3: Compaction Exception Atomicity
- **Condition:** `compactAtFrontier()` is invoked with a throwing reducer callback.
- **Pass Threshold:**
  1. Candidate base state reduction is staged on an out-of-band clone before modifying active log state.
  2. If the reducer throws, the exception is propagated, and `this.operations`, `idIndex`, `originSeqIndex`, and checkpoints remain 100% untouched.

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

---

## 4. Supplier Failover Criteria (AC-M4-FAILOVER)

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

## 5. Duplicate, Stale & Corrupt Chunk Criteria (AC-M4-CHUNK)

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

## 6. Retry & Timeout Budget Criteria (AC-M4-BUDGET)

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

## 7. Dynamic Sizing & Termination Criteria (AC-M4-SIZE)

### AC-M4-SIZE-1: Iterative Send-Boundary Measurement & Termination
- **Condition:** `SnapshotTransferManager` prepares chunks across negotiated SCTP limits from 2,048 to 65,536 bytes.
- **Pass Threshold:**
  1. Each unsuccessful sizing iteration strictly reduces binary slice size by at least 33 bytes ($\Delta = \lceil \text{overshoot} \times 0.75 \rceil + 32$).
  2. Loop strictly terminates in at most 993 iterations (proven bounded by initial slice $\le 32,768$ bytes).
  3. Measured UTF-8 bytes of final serialized JSON string strictly $\le \text{effectiveLimit}$.
  4. The exact tested string is passed unchanged to `RTCDataChannel.send()`.

### AC-M4-SIZE-2: Minimum Size Abort
- **Condition:** SCTP limit $< 2,048$ bytes or chunk binary slice $< 256$ bytes.
- **Pass Threshold:**
  1. Sizing loop halts and transfer aborts with `TRANSPORT_LIMIT_EXCEEDED`.

---

## 8. Memory & Resource Budget Criteria (AC-M4-MEM)

### AC-M4-MEM-1: Hard Concurrency & Memory Ceilings
- **Condition:** High-load multi-peer operation.
- **Pass Threshold:**
  1. Requester permits strictly 1 active inbound transfer.
  2. Supplier permits at most 2 active outbound transfers.
  3. Pre-allocated reassembly buffer bounded to declared `totalChunks` ($\le 128$).
  4. Total peak transient memory during reassembly strictly $\le 8.5\text{ MB}$.

### AC-M4-MEM-2: Complete Lifecycle Memory Dereferencing
- **Condition:** Transfer completion, timeout, cancellation, or epoch increment.
- **Pass Threshold:**
  1. `dispose()` sets all slice references and assembled buffer references to `null`.
  2. Verified zero retained snapshot memory after disposal.

---

## 9. M5 Handoff Boundary Criteria (AC-M4-HANDOFF)

### AC-M4-HANDOFF-1: Clean Handoff to M5 Callback
- **Condition:** Successful completion of snapshot reassembly and verification.
- **Pass Threshold:**
  1. `SnapshotTransferResult` containing verified `StateSnapshotPayloadV2` and raw bytes is emitted to registered handoff callback.
  2. M4 code **DOES NOT** call `engine.importSnapshot()`, touch `PlaybackState`, reset `OrderedOperationLog`, or reseed clocks.
