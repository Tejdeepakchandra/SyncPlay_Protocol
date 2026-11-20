# Phase 7.5 M4.2 — Acceptance Criteria: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.2.0  
**Status:** Approved Acceptance Criteria (Final Planning Correction Applied)  
**Target Milestone:** M4.2  
**Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  

---

## 1. Acceptance Criteria Matrix

| Criterion ID | Category | Objective | Verification Method |
| :--- | :--- | :--- | :--- |
| **AC-M4-SER-1** | Serialization | Deterministic RFC 8785 JSON canonicalization. | Unit & property tests verifying identical byte outputs for identical states. |
| **AC-M4-SER-2** | Integrity | Bit-exact overall SHA-256 digest computation over master buffer. | Comparison with `computeSnapshotDigestV2()`. |
| **AC-M4-SER-3** | Validation | Pre-serialization structural bounds & 4 MiB size ceiling. | Rejection testing on checkpoints $> 256$, frontier keys $> 64$, and buffers $> 4$ MiB. |
| **AC-M4-CONC-1** | Concurrency | Strict concurrency admission: $A + P \le 2$. | Test verifying 3rd preparation is held in queue until a retained artifact is released. |
| **AC-M4-CONC-2** | Concurrency | Bounded preparation queue ($\le 4$, metadata only $< 1$ KiB). | Saturation test asserting immediate rejection with `LOCAL_ABORT` at depth 4. |
| **AC-M4-CONC-3** | Concurrency | Queue resumption on lease release; proof of no stall. | Verification that releasing the last lease of an artifact unblocks the waiting queue. |
| **AC-M4-MEM-1** | Memory | Peak transient application allocation bounded $\le 24.0$ MiB. | Theoretical heap model & allocation accounting on full 4 MiB preparation. |
| **AC-M4-MEM-2** | Memory | Steady-state retained memory bounded $\le 9.0$ MiB for 2 artifacts. | Memory footprint assertions across 2 active retained artifacts. |
| **AC-M4-MEM-3** | Memory | Immediate dereferencing of preimage object and JSON string. | Verification that artifact does not hold references to preimage or string. |
| **AC-M4-OWN-1** | Encapsulation | Private backing storage with zero mutable public exposure. | Interface inspection proving no public mutable typed-array property or view. |
| **AC-M4-OWN-2** | Encapsulation | Primitive string encoding: `encodeSliceToBase64()` & `computeSliceSha256()`. | Verification that slicing methods return immutable primitive strings. |
| **AC-M4-OWN-3** | Encapsulation | Unaliased copies via `cloneBytes()` and `copyByteRange()`. | Mutation test proving edits to clone do not mutate master buffer. |
| **AC-M4-OWN-4** | Integrity | Hash-to-encoding byte identity. | Invariant test asserting transmitted Base64 bytes match hashed master bytes. |
| **AC-M4-LSE-1** | Lease Lifecycle | Transfer lease acquisition & idempotent release. | Negative testing: duplicate lease throws `INVALID_STATE`; duplicate release is safe. |
| **AC-M4-LSE-2** | Lease Lifecycle | Deferred disposal: detachment deferred until active leases reach 0. | Test proving active transfers can encode chunks after `dispose()` is requested. |
| **AC-M4-LSE-3** | Lease Lifecycle | Post-disposal assertion: calls after disposal throw `ARTIFACT_DISPOSED`. | Negative testing invoking access methods after final detachment. |
| **AC-M4-PRT-1** | Partitioning | Strict canonical partition count and sizing: $N = \lceil S / C \rceil$. | Boundary testing across edge-case snapshot sizes ($1$ B to $4$ MiB). |
| **AC-M4-PRT-2** | Partitioning | Zero mid-transfer resizing: canonical geometry immutable for transfer. | Asserting chunk sizes remain strictly fixed to canonical partition. |
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

### AC-M4-SER-3: Pre-Serialization Structural Bounds & Size Ceiling
- **Statement:** `SnapshotSerializer.serialize()` MUST enforce pre-serialization bounds (checkpoints $\le 256$, frontier entries $\le 64$) and post-serialization size ceiling $\le 4,194,304$ B.
- **Test:** Pass snapshots exceeding structural bounds or 4 MiB buffer ceiling. Verify immediate rejection with `SNAPSHOT_ERROR`.

### AC-M4-CONC-1, AC-M4-CONC-2 & AC-M4-CONC-3: Strict Concurrency Admission & Queue Resumption
- **Statement:** The system MUST enforce $\text{retainedArtifactCount} + (\text{isPreparing} \mathrel{?} 1 : 0) \le 2$. When 2 artifacts are retained, a 3rd preparation MUST be held in queue. When an active artifact's last lease is released and disposed, the queue MUST automatically resume and start preparation.
- **Test:**
  1. Create 2 retained artifacts. Submit a 3rd preparation request. Verify that the 3rd capture function is not called.
  2. Release the leases on Artifact 1 and call `artifact1.dispose()`. Verify that the 3rd preparation automatically begins execution.
  3. Enqueue 4 pending requests, then enqueue a 5th. Assert immediate rejection with `LOCAL_ABORT` ('Preparation queue saturated').

### AC-M4-MEM-1, AC-M4-MEM-2 & AC-M4-MEM-3: Memory Bounds & Dereferencing
- **Statement:** Peak transient application-owned allocation MUST NOT exceed 24.0 MiB under the strict $A + P \le 2$ policy. Steady-state retained memory for 2 active artifacts MUST NOT exceed 9.0 MiB. Preimage objects and JSON strings MUST be dereferenced immediately.
- **Test:**
  1. Verify that `SerializedSnapshotArtifact` does not hold references to `SnapshotPreimage` or the serialized JSON string.
  2. Verify that two simultaneously retained 4 MiB artifacts consume $\le 8.4$ MiB of buffer memory.

### AC-M4-OWN-1 & AC-M4-OWN-2: Private Buffer Encapsulation & Range Encoding
- **Statement:** `SerializedSnapshotArtifact` MUST encapsulate `#masterBuffer` privately. Slicing and encoding MUST be performed via `encodeSliceToBase64()` and `computeSliceSha256()`, returning immutable primitive strings. No raw mutable `Uint8Array` view may escape.
- **Test:**
  1. Inspect `SerializedSnapshotArtifact` instance keys; assert no public typed array or `ArrayBuffer` property or method returning a view exists.
  2. Verify `artifact.encodeSliceToBase64(offset, length)` returns a valid Base64 string whose decoded bytes match the master buffer slice.

### AC-M4-OWN-3: Unaliased Public Copies
- **Statement:** Calls to `cloneBytes()` or `copyByteRange()` MUST return freshly allocated, unaliased deep copies.
- **Test:** Mutate `clone[0] = 0xAA`. Call `cloneBytes()` again and verify the byte at index 0 remains unmodified.

### AC-M4-LSE-1, AC-M4-LSE-2 & AC-M4-LSE-3: Transfer Lease Lifecycle & Deferred Disposal
- **Statement:** Transfers MUST acquire leases before encoding chunks and release them upon terminal transition. Calling `dispose()` with active leases MUST defer buffer detachment until the last lease is released. After final disposal, all read operations MUST throw `ARTIFACT_DISPOSED`.
- **Test:**
  1. Call `artifact.acquireLease('t1')`. Verify `artifact.activeLeaseCount === 1`.
  2. Calling `artifact.acquireLease('t1')` again MUST throw `INVALID_STATE`.
  3. Call `artifact.dispose()`. Verify `artifact.isDisposalRequested === true` and `artifact.isDisposed === false`. Verify encoding methods still succeed.
  4. Call `artifact.releaseLease('t1')`. Verify `artifact.isDisposed === true`.
  5. Subsequent calls to `encodeSliceToBase64`, `computeSliceSha256`, or `cloneBytes` MUST throw `ARTIFACT_DISPOSED`.

### AC-M4-PRT-1 & AC-M4-PRT-2: Canonical Partition Geometry & Immutability
- **Statement:** Slices MUST strictly follow canonical partition formulas ($N = \lceil S / C \rceil$, non-final $C$, final $S - (N-1) \cdot C$). Chunk slice sizes MUST NEVER be modified mid-transfer.
- **Test:** Test boundary sizes: $S = 1$, $S = 255$, $S = 256$, $S = 32,767$, $S = 32,768$, $S = 32,769$, $S = 4,194,304$. Verify all chunks $0 \dots N-2$ have size $C$, and chunk $N-1$ has size $B_{\text{final}}$.

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
