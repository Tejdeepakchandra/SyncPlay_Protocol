# Phase 7.5 Milestone M4 — Snapshot Transfer Acceptance Criteria

**Status:** APPROVED FOR PLANNING (DOCUMENTATION-ONLY)  
**Date:** 2026-11-19  
**Branch Baseline:** `main` at `6fddcf90de5d4cb0e82bd711e7e217cd570c88a4`  
**Specification Baseline:** `119da194c3feb883cfd61b6928cb81d4b200ac5d`  

---

## 1. Overview & Verification Framework

This document defines the formal, measurable acceptance criteria for Phase 7.5 Milestone M4 (Chunked Snapshot Transfer Protocol). Each criterion specifies exact pass/fail thresholds, test conditions, and associated failure codes.

---

## 2. Pre-M4 Security & Metadata Criteria (AC-M4-PRE)

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
  1. If an exact sequence timestamp is not present in active log or retained checkpoints, it is reported as unresolvable / unavailable.
  2. The function **NEVER** returns a timestamp belonging to a different sequence number.
  3. Snapshot creation fails if required frontier timestamps cannot be established exactly.

---

## 3. Wire Protocol & Sizing Criteria (AC-M4-WIRE)

### AC-M4-WIRE-1: Wire Schema Conformity
- **Condition:** Processing of `SNAPSHOT_OFFER`, `SNAPSHOT_REQUEST`, `SNAPSHOT_CHUNK`, `SNAPSHOT_CHUNK_REQUEST`, and `SNAPSHOT_ABORT`.
- **Pass Threshold:**
  1. Payloads conforming to Zod schemas parse successfully.
  2. Payloads violating bounds (e.g. `chunkIndex >= totalChunks`, `chunkSizeBytes > MAX_CHUNK_BYTES`, `totalChunks > 128`, `totalSizeBytes > 4 MB`) fail schema validation immediately.
  3. Invalid payloads are rejected at the transport boundary without side effects.

### AC-M4-WIRE-2: Normative 7-Step Iterative Sizing Enforcement
- **Condition:** Outbound `SNAPSHOT_CHUNK` transmission across negotiated SCTP limits from 2,048 to 65,536 bytes.
- **Pass Threshold:**
  1. The complete serialized `ProtocolMessage` JSON string is measured via `TextEncoder().encode(wireJson).byteLength`.
  2. The measured UTF-8 byte length strictly satisfies:
     $$\text{byteLength} \le \min(65536, \text{sctpMaxMessageSize})$$
  3. If the initial estimate overshoots, the iterative reduction loop dynamically decreases the binary slice until the serialized message fits.
  4. The exact serialized string validated is passed unchanged to `RTCDataChannel.send()`.
  5. If effective limit is $< 2,048$ bytes or chunk binary size drops below 256 bytes without fitting, transfer is cleanly aborted with `TRANSPORT_LIMIT_EXCEEDED`.

---

## 4. Flow Control & Assembly Criteria (AC-M4-FLOW)

### AC-M4-FLOW-1: Strict Single Inbound Transfer Invariant
- **Condition:** Multiple `SNAPSHOT_OFFER` messages received by a recovering peer.
- **Pass Threshold:**
  1. A recovering node maintains at most 1 active inbound transfer at any time.
  2. Concurrent offers from the same epoch are queued or ignored.
  3. An offer from a higher epoch terminates the current transfer with `EPOCH_MISMATCH` before starting a new transfer.

### AC-M4-FLOW-2: Sliding-Window Flow Control
- **Condition:** Multi-chunk transfer over WebRTC DataChannel.
- **Pass Threshold:**
  1. Number of concurrently in-flight unacknowledged chunks strictly $\le \text{MAX\_CONCURRENT\_CHUNK\_REQUESTS} = 4$.
  2. Chunks are dispatched in accordance with granted window credits.
  3. `SNAPSHOT_CHUNK_REQUEST` advances the window as chunks are verified.

### AC-M4-FLOW-3: Selective Retransmission Recovery
- **Condition:** Simulated 10% packet drop and arbitrary chunk reordering on transport link.
- **Pass Threshold:**
  1. Out-of-order chunks are accepted and placed in the correct sparse index in `ChunkAssembler`.
  2. Duplicate chunks are idempotently dropped without error or double-counting.
  3. Gaps trigger `SNAPSHOT_CHUNK_REQUEST` specifying missing indices upon chunk timeout (5,000ms).
  4. 100% of dropped chunks are recovered within max 3 retries, completing full snapshot assembly.

---

## 5. Integrity & Verification Criteria (AC-M4-INTEG)

### AC-M4-INTEG-1: Per-Chunk Binary Hash Verification
- **Condition:** Receipt of `SNAPSHOT_CHUNK` with Base64 payload.
- **Pass Threshold:**
  1. Base64 payload is decoded to raw binary bytes.
  2. $\text{SHA-256}(\text{rawBinary}) \equiv \text{chunk.chunkSha256}$ is verified.
  3. Any chunk with bit-flips or corrupted Base64 is rejected immediately and re-requested.

### AC-M4-INTEG-2: Full-Payload Canonical SHA-256 Verification
- **Condition:** Collation of all chunks $[0, \text{totalChunks}-1]$ in `ChunkAssembler`.
- **Pass Threshold:**
  1. Reassembled contiguous buffer is parsed as UTF-8 JSON into `StateSnapshotPayloadV2`.
  2. Canonical RFC 8785 JSON SHA-256 digest computed over payload preimage matches `offer.sha256Digest` bit-exact.
  3. If overall digest fails to match, all assembled memory is discarded and transfer aborts with `HASH_MISMATCH`.

---

## 6. Fencing & Handoff Criteria (AC-M4-FENCE)

### AC-M4-FENCE-1: Disconnection Fencing
- **Condition:** Supplier peer disconnects or closes DataChannel mid-transfer.
- **Pass Threshold:**
  1. Transfer is aborted immediately with reason `SUPPLIER_DISCONNECTED`.
  2. Partial reassembly buffers are cleared from memory.
  3. Engine state remains 100% untouched.

### AC-M4-FENCE-2: Epoch Fencing
- **Condition:** Membership epoch increments mid-transfer.
- **Pass Threshold:**
  1. Transfer is aborted immediately with reason `EPOCH_MISMATCH`.
  2. Outbound `SNAPSHOT_ABORT` is emitted if DataChannel is open.
  3. Stale transfer state is cleared.

### AC-M4-FENCE-3: Clean Handoff to M5
- **Condition:** Successful completion of snapshot reassembly and verification.
- **Pass Threshold:**
  1. `SnapshotTransferResult` containing verified `StateSnapshotPayloadV2` and raw bytes is emitted to registered handoff callback.
  2. M4 code **DOES NOT** call `engine.importSnapshot()`, touch `PlaybackState`, reset `OrderedOperationLog`, or reseed clocks.
