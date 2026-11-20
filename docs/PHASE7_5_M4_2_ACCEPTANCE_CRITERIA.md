# Phase 7.5 M4.2 — Acceptance Criteria: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.1.0  
**Status:** Approved Acceptance Criteria (Targeted Planning Correction Applied)  
**Target Milestone:** M4.2  
**Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  

---

## 1. Acceptance Criteria Matrix

| Criterion ID | Category | Objective | Verification Method |
| :--- | :--- | :--- | :--- |
| **AC-M4-SER-1** | Serialization | Deterministic RFC 8785 JSON canonicalization. | Unit & property tests verifying identical byte outputs for identical states. |
| **AC-M4-SER-2** | Integrity | Bit-exact overall SHA-256 digest computation. | Comparison with `computeSnapshotDigestV2()`. |
| **AC-M4-SER-3** | Validation | Rejection of non-serializable objects and sizes $> 4$ MiB. | Negative testing passing `NaN`, symbols, and oversized buffers. |
| **AC-M4-CONC-1** | Concurrency | Bounded preparation concurrency (`MAX_CONCURRENT_PREPARATIONS = 1`). | Concurrency test proving capture occurs sequentially inside serialized gate. |
| **AC-M4-CONC-2** | Concurrency | Bounded preparation queue (`MAX_PREPARATION_QUEUE_DEPTH = 4`). | Saturation test asserting immediate rejection with `LOCAL_ABORT`. |
| **AC-M4-MEM-1** | Memory | Peak transient application allocation bounded $\le 24.0$ MiB. | Theoretical heap model & allocation accounting on full 4 MiB preparation. |
| **AC-M4-MEM-2** | Memory | Steady-state retained memory bounded $\le 9.0$ MiB for 2 artifacts. | Memory footprint assertions across 2 active retained artifacts. |
| **AC-M4-MEM-3** | Memory | Immediate dereferencing of preimage object and JSON string. | Verification that artifact does not hold references to preimage or string. |
| **AC-M4-OWN-1** | Encapsulation | Private `#masterBuffer` with zero mutable public exposure. | Interface inspection proving no public mutable typed-array property. |
| **AC-M4-OWN-2** | Encapsulation | Unaliased copies via `cloneBytes()` and `copyByteRange()`. | Mutation test proving edits to clone do not mutate artifact or slices. |
| **AC-M4-OWN-3** | Integrity | Hash-to-encoding byte identity. | Invariant test asserting transmitted Base64 bytes match hashed bytes. |
| **AC-M4-OWN-4** | Safety | Safe sharing across concurrent transfers. | Multi-transfer test proving concurrent sends from 1 artifact do not interfere. |
| **AC-M4-OWN-5** | Lifecycle | Explicit disposal throws `ARTIFACT_DISPOSED` on subsequent reads. | Negative testing invoking access methods after `artifact.dispose()`. |
| **AC-M4-PRT-1** | Partitioning | Strict canonical partition count and sizing: $N = \lceil S / C \rceil$. | Boundary testing across edge-case snapshot sizes ($1$ B to $4$ MiB). |
| **AC-M4-PRT-2** | Partitioning | Zero mid-transfer resizing: canonical geometry immutable for transfer. | Asserting chunk sizes remain strictly fixed to canonical partition. |
| **AC-M4-PRT-3** | Partitioning | Zero-copy internal borrowing: slices reference master buffer. | Asserting `getBorrowedSlice()` references `#masterBuffer` memory. |
| **AC-M4-PRT-4** | Partitioning | Per-chunk SHA-256 hash integrity. | Re-hashing raw slices against `chunkSha256`. |
| **AC-M4-SIZ-1** | Sizing Check | Mandatory exact complete-message UTF-8 byte measurement. | Comparing measured `byteLength` with `new TextEncoder().encode()`. |
| **AC-M4-SIZ-2** | Sizing Check | Transport ceiling enforcement: abort on oversize message. | Negative test asserting abort with `TRANSPORT_LIMIT_EXCEEDED` on oversize. |
| **AC-M4-SIZ-3** | Sizing Check | Transport floor enforcement: reject limits $< 2,144$ B upfront. | Rejection testing with limits from 0 to 2,143 B during admission. |
| **AC-M4-ISO-1** | Isolation | Zero side effects on engine playback state or logs. | Deep equality checks on engine state before/after generation. |
| **AC-M4-COM-1** | Compatibility | Bit-exact round-trip reassembly in frozen M4.1 `ChunkAssembler`. | End-to-end ingestion and assembly of M4.2 output into M4.1 assembler. |

---

## 2. Detailed Verification Specifications

### AC-M4-SER-1: Deterministic RFC 8785 Serialization
- **Statement:** For any given `StateSnapshotPayloadV2`, `SnapshotSerializer.serialize()` MUST produce the exact same byte sequence regardless of property insertion order, runtime platform, or garbage collection state.
- **Test:** Pass two snapshot objects with randomly shuffled object keys and differently ordered `compactedCheckpoints`. Verify that `artifact1.cloneBytes()` and `artifact2.cloneBytes()` are byte-for-byte identical.

### AC-M4-SER-2: Bit-Exact Overall SHA-256 Digest
- **Statement:** The digest in `SerializedSnapshotArtifact.sha256Digest` MUST equal `sha256(artifact.cloneBytes())` and match `computeSnapshotDigestV2(snapshot)` bit-exact.
- **Test:** Assert `artifact.sha256Digest === sha256(artifact.cloneBytes())` and verify lowercase 64-character hexadecimal format.

### AC-M4-CONC-1 & AC-M4-CONC-2: Bounded Concurrency & Queue Saturation
- **Statement:** Snapshot preparation MUST be strictly serialized (`MAX_CONCURRENT_PREPARATIONS = 1`). Snapshot capture MUST NOT occur prior to entering the serialized section. Queued preparation requests MUST be bounded to $\le 4$ and store only metadata. Saturated requests MUST be rejected with `LOCAL_ABORT` ('Preparation queue saturated').
- **Test:**
  1. Trigger 2 concurrent preparation requests. Assert that the second capture function is not called until the first preparation completes.
  2. Enqueue 4 pending requests, then enqueue a 5th request. Assert that the 5th request immediately rejects with `reason: 'LOCAL_ABORT'`, `detail: 'Preparation queue saturated'`.
  3. Verify queued request descriptors contain only string IDs, numbers, and timestamps ($< 1$ KiB total).

### AC-M4-MEM-1, AC-M4-MEM-2 & AC-M4-MEM-3: Memory Bounds & Dereferencing
- **Statement:** Peak transient application-owned allocation MUST NOT exceed 24.0 MiB during a 4 MiB preparation. Steady-state retained memory for 2 active artifacts MUST NOT exceed 9.0 MiB. Preimage objects and JSON strings MUST be dereferenced immediately.
- **Test:**
  1. Assert that `SerializedSnapshotArtifact` does not have a `snapshot` property or hold references to `SnapshotPreimage` or the serialized JSON string.
  2. Verify that two simultaneously retained 4 MiB artifacts consume $\le 8.4$ MiB of buffer memory.

### AC-M4-OWN-1 & AC-M4-OWN-2: Private Buffer Encapsulation & Unaliased Copies
- **Statement:** `SerializedSnapshotArtifact` MUST encapsulate its master `ArrayBuffer` privately. No public property may expose the master buffer directly. External calls to `cloneBytes()` or `copyByteRange()` MUST return independent copies that do not alias the master buffer.
- **Test:**
  1. Inspect `SerializedSnapshotArtifact` instance keys; assert no public typed array or `ArrayBuffer` property exists.
  2. Call `const clone = artifact.cloneBytes()`. Mutate `clone[0] = 0xAA`. Call `const clone2 = artifact.cloneBytes()`. Assert `clone2[0] !== 0xAA` and `artifact.sha256Digest` remains valid.

### AC-M4-OWN-3 & AC-M4-OWN-4: Byte Identity & Concurrent Transfer Safety
- **Statement:** The binary bytes read for Base64 chunk transmission MUST be bit-exact identical to the bytes hashed for `sha256Digest`. Concurrent outbound transfers sharing the same artifact MUST execute without data corruption.
- **Test:**
  1. Reconstruct binary buffer from all generated chunk Base64 strings. Assert `sha256(reconstructedBuffer) === artifact.sha256Digest`.
  2. Run two concurrent outbound transfer pipelines reading from the same `PartitionedSnapshot`. Verify both generate identical, valid chunk streams.

### AC-M4-OWN-5: Explicit Artifact Disposal
- **Statement:** Calling `artifact.dispose()` MUST detach references to `#masterBuffer` and mark `isDisposed === true`. Subsequent method calls MUST throw `SyncEngineError('ARTIFACT_DISPOSED')`.
- **Test:** Invoke `artifact.dispose()`. Verify `artifact.isDisposed === true`. Assert that calls to `artifact.getBorrowedSliceView()`, `artifact.copyByteRange()`, and `artifact.cloneBytes()` all throw `SyncEngineError` with code `ARTIFACT_DISPOSED`.

### AC-M4-PRT-1 & AC-M4-PRT-2: Canonical Partition Geometry & Immutability
- **Statement:** Slices MUST strictly follow canonical partition formulas ($N = \lceil S / C \rceil$, non-final $C$, final $S - (N-1) \cdot C$). Chunk slice sizes MUST NEVER be modified mid-transfer.
- **Test:** Test boundary sizes: $S = 1$, $S = 255$, $S = 256$, $S = 32,767$, $S = 32,768$, $S = 32,769$, $S = 4,194,304$. Verify all chunks $0 \dots N-2$ have size $C$, and chunk $N-1$ has size $B_{\text{final}}$. Verify that `prepareChunkMessage()` never alters declared slice sizes.

### AC-M4-SIZ-1 & AC-M4-SIZ-2: Exact UTF-8 Measurement & Transport Ceiling Enforcement
- **Statement:** Every generated chunk message MUST be measured for exact UTF-8 byte length via `new TextEncoder().encode(serializedJson).byteLength`. If a message exceeds `sctpMaxMessageSize`, `prepareChunkMessage()` MUST return `ok: false` with `reason: 'TRANSPORT_LIMIT_EXCEEDED'` and MUST NOT transmit a non-canonical chunk.
- **Test:**
  1. Verify `result.byteLength === new TextEncoder().encode(result.serializedJson).byteLength`.
  2. Mock an artificially tight `sctpMaxMessageSize` (e.g. 500 bytes) on a normal chunk. Assert `result.ok === false`, `result.reason === 'TRANSPORT_LIMIT_EXCEEDED'`.

### AC-M4-COM-1: Frozen M4.1 Receiver Round-Trip Compatibility
- **Statement:** Any chunk messages produced by `SnapshotOutboundPipeline` MUST be immediately ingestible by the frozen M4.1 `ChunkAssembler`, passing all per-chunk hash checks, duplicate checks, and final reassembly.
- **Test:** Ingest all generated chunks into `ChunkAssembler`. Finalize assembly and assert:
  1. `finalizeResult.ok === true`
  2. `finalizeResult.snapshot.buffer` is byte-for-byte identical to `artifact.cloneBytes()`.
  3. `finalizeResult.snapshot.sha256Digest === artifact.sha256Digest`.
