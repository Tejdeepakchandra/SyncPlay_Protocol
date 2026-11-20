# Phase 7.5 M4.2 — Acceptance Criteria: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.0.0  
**Status:** Approved Acceptance Criteria  
**Target Milestone:** M4.2  
**Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  

---

## 1. Acceptance Criteria Matrix

| Criterion ID | Category | Objective | Verification Method |
| :--- | :--- | :--- | :--- |
| **AC-M4-SER-1** | Serialization | Deterministic RFC 8785 JSON canonicalization. | Unit & property tests verifying identical byte outputs for identical states. |
| **AC-M4-SER-2** | Integrity | Bit-exact overall SHA-256 digest computation. | Comparison with `computeSnapshotDigestV2()`. |
| **AC-M4-SER-3** | Validation | Rejection of non-serializable objects and sizes $> 4$ MiB. | Negative testing passing `NaN`, symbols, and oversized buffers. |
| **AC-M4-PRT-1** | Partitioning | Strict canonical partition count: $N = \lceil S / C \rceil$. | Boundary testing across edge-case snapshot sizes. |
| **AC-M4-PRT-2** | Partitioning | Exact final partial chunk: $S - (N - 1) \cdot C \in (0, C]$. | Mathematical assertions on chunk byte length arrays. |
| **AC-M4-PRT-3** | Partitioning | Per-chunk SHA-256 hash integrity. | Re-hashing raw slices against `chunkSha256`. |
| **AC-M4-PRT-4** | Memory | Zero-copy slicing view over underlying buffer. | Verifying `sliceView.buffer === rawBytes.buffer`. |
| **AC-M4-SIZ-1** | Sizing Loop | Normative 7-step sizing loop converges in $\le 5$ iterations. | Testing against synthetic tight transport limits. |
| **AC-M4-SIZ-2** | Sizing Loop | Exact UTF-8 byte measurement via `TextEncoder`. | Comparing reported byte length with byte array length. |
| **AC-M4-SIZ-3** | Sizing Loop | Transport floor enforcement: reject limits $< 2,144$ B. | Rejection testing with limits from 0 to 2,143 B. |
| **AC-M4-SIZ-4** | Compliance | Every serialized message strictly $\le \text{effectiveLimit}$. | Asserting `byteLength <= sctpMaxMessageSize` on all generated messages. |
| **AC-M4-B64-1** | Encoding | Isomorphic browser/Node.js Base64 consistency. | Cross-environment testing of `encodeBase64()`. |
| **AC-M4-MEM-1** | Memory | Peak outbound memory footprint bounded $\le 16$ MiB. | Heap delta inspection on full 4 MiB transfer generation. |
| **AC-M4-ISO-1** | Isolation | Zero side effects on engine playback state or logs. | Deep equality checks on engine state before/after generation. |
| **AC-M4-COM-1** | Compatibility | Bit-exact round-trip reassembly in M4.1 `ChunkAssembler`. | End-to-end ingestion and assembly of M4.2 output into M4.1 assembler. |

---

## 2. Detailed Verification Specifications

### AC-M4-SER-1: Deterministic RFC 8785 Serialization
- **Statement:** For any given `StateSnapshotPayloadV2`, `SnapshotSerializer.serialize()` MUST produce the exact same byte sequence regardless of property insertion order, runtime platform, or garbage collection state.
- **Test:** Pass two snapshot objects with randomly shuffled object keys and differently ordered `compactedCheckpoints`. Verify that `artifact1.rawBytes` and `artifact2.rawBytes` are byte-for-byte identical.

### AC-M4-SER-2: Bit-Exact Overall SHA-256 Digest
- **Statement:** The digest in `SerializedSnapshotArtifact.sha256Digest` MUST equal `sha256(artifact.rawBytes)` and match `computeSnapshotDigestV2(snapshot)` bit-exact.
- **Test:** Assert `artifact.sha256Digest === sha256(artifact.rawBytes)` and verify lowercase 64-character hexadecimal format.

### AC-M4-PRT-1 & AC-M4-PRT-2: Canonical Partition Geometry
- **Statement:** The number of chunks $N$ and the final chunk size $B_{\text{final}}$ MUST strictly follow the canonical partition formulas:
  $$N = \left\lceil \frac{S}{C} \right\rceil, \quad B_{\text{final}} = S - (N - 1) \cdot C$$
  where $S = \text{totalSizeBytes}$ and $C = \text{nominalChunkSizeBytes}$.
- **Test:** Test boundary sizes: $S = 1$, $S = 255$, $S = 256$, $S = 32,767$, $S = 32,768$, $S = 32,769$, $S = 4,194,304$. Verify all chunks $0 \dots N-2$ have size $C$, and chunk $N-1$ has size $B_{\text{final}}$.

### AC-M4-PRT-4: Zero-Copy Memory Guarantee
- **Statement:** `SnapshotChunkProducer` MUST NOT copy slice memory. Slices MUST be typed array views referencing the identical underlying `ArrayBuffer`.
- **Test:** Assert `slice[i].sliceView.buffer === artifact.rawBytes.buffer` and `slice[i].sliceView.byteOffset === i * nominalChunkSizeBytes`.

### AC-M4-SIZ-1 & AC-M4-SIZ-4: Sizing Loop Convergence & Enforcement
- **Statement:** For any admitted snapshot and valid transport limit, `SnapshotOutboundPipeline.prepareChunkMessage()` MUST produce a message whose exact UTF-8 byte length is $\le \text{effectiveLimit}$ in $\le 5$ iterations without ever exceeding the limit.
- **Test:** Run sizing loop across limits: 65,536 B, 45,492 B, 32,768 B, 16,384 B, 4,096 B, 2,144 B. Assert `result.ok === true`, `result.byteLength <= limit`, and `result.iterations <= 5`.

### AC-M4-COM-1: Round-Trip Assembly Compatibility
- **Statement:** Any chunk message produced by `SnapshotOutboundPipeline` MUST be immediately ingestible by the frozen M4.1 `ChunkAssembler`, passing all per-chunk hash checks, duplicate checks, and final reassembly.
- **Test:** Ingest all generated chunks into `ChunkAssembler`. Finalize assembly and assert:
  1. `finalizeResult.ok === true`
  2. `finalizeResult.snapshot.buffer` is byte-for-byte identical to `artifact.rawBytes`.
  3. `finalizeResult.snapshot.sha256Digest === artifact.sha256Digest`.
