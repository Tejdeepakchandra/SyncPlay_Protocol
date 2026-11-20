# Phase 7.5 M4.2 — Risk Register: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.1.0  
**Status:** Approved Risk Register (Targeted Planning Correction Applied)  
**Target Milestone:** M4.2  
**Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  

---

## 1. Risk Register Summary Matrix

| Risk ID | Severity | Likelihood | Category | Summary Description | Mitigation Strategy |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **R-M4-2-1** | **CRITICAL** | Low | Transport Boundary | Serialized chunk exceeds negotiated SCTP limit at send boundary. | 1,800 B conservative reservation (569 B cushion) + mandatory exact UTF-8 byte measurement via `TextEncoder`. |
| **R-M4-2-2** | **HIGH** | Medium | Asymmetric Transport | Sender's transport limit differs from receiver's inbound capacity. | Supplier sizes chunks to recipient's limit; receiver admission gate rejects incompatible offers upfront. |
| **R-M4-2-3** | **HIGH** | Low | Canonical Geometry | Mid-transfer chunk resizing violates canonical partition rules. | Zero mid-transfer resizing; canonical partition is immutable; send-boundary overshoot triggers clean `TRANSPORT_LIMIT_EXCEEDED` abort. |
| **R-M4-2-4** | **MEDIUM** | Low | Memory Management | Overlapping snapshot preparations or multiple full copies cause OOM. | Sequential preparation gate (`MAX_CONCURRENT_PREPARATIONS = 1`), metadata queue (depth $\le 4$), capture inside gate, immediate dereferencing, $\le 24.0$ MiB transient budget. |
| **R-M4-2-5** | **HIGH** | Low | Data Integrity | Non-deterministic JSON serialization causes digest mismatch. | RFC 8785 JSON Canonicalization Scheme (JCS) with deterministic checkpoint sorting. |
| **R-M4-2-6** | **MEDIUM** | Low | Lifecycle Fencing | Epoch changes while outbound transfer preparation is in-flight. | Epoch binding in offer and chunk context; reject preparation if epoch has incremented. |
| **R-M4-2-7** | **HIGH** | Low | Buffer Safety | External caller mutates underlying snapshot buffer during transfer. | Master buffer held privately in `#masterBuffer`; public access via unaliased `cloneBytes()`; borrowed views restricted to recovery pipeline. |
| **R-M4-2-8** | **LOW** | Low | Browser Compatibility | `String.fromCharCode` stack overflow on large binary buffers in browsers. | Isomorphic `encodeBase64()` uses 8,192-byte chunking for browser `btoa` conversion. |

---

## 2. Detailed Risk Analysis & Mitigation Strategies

### R-M4-2-1: Serialized Chunk Exceeds SCTP Limit at Send Boundary
- **Impact:** If `PeerManager.sendSerialized()` transmits a frame exceeding the underlying SCTP data channel buffer or MTU, the browser WebRTC stack silently drops the packet or closes the data channel.
- **Root Cause:** Estimating JSON message length from character count or estimating Base64 expansion rather than measuring actual UTF-8 encoded bytes.
- **Mitigation:**
  1. `calculateChunkCapacity()` deducts `MAX_ENVELOPE_OVERHEAD_BYTES = 1,800` bytes upfront, providing an empirical 569-byte cushion over the worst-case measured envelope overhead (1,231 B).
  2. `SnapshotOutboundPipeline` measures exact byte length using `new TextEncoder().encode(serializedJson).byteLength`.
  3. The exact measured string is returned in `OutboundChunkResult.serializedJson` and passed unchanged to `sendSerialized()`.

---

### R-M4-2-2: Asymmetric Transport Sizing & Offer Divergence
- **Impact:** Sender creates an offer with chunk capacity $C_A$, but receiver's local SCTP limit permits only $C_B < C_A$. Receiver admission rejects the offer with `OFFER_INCONSISTENT` or `TRANSPORT_LIMIT_EXCEEDED`.
- **Root Cause:** Asymmetric SDP `a=max-message-size` declarations between peers.
- **Mitigation:**
  1. The supplier evaluates `calculateSnapshotAdmission()` against the specific recipient's transport limit before generating the offer.
  2. In `SNAPSHOT_REQUEST`, the receiver includes `clientMaxChunkBytes`.
  3. If admission fails at the receiver, the receiver cleanly returns `SNAPSHOT_ABORT`, leaving active engine state untouched. Mid-transfer geometry mutation is strictly prohibited.

---

### R-M4-2-3: Mid-Transfer Chunk Resizing Violates Canonical Partition
- **Impact:** Shrinking an individual chunk slice mid-transfer breaks the frozen M4.1 receiver rule ($S_i = C$ for $i < N - 1$, $S_{N-1} = S - (N-1) \cdot C$), causing `ChunkAssembler` to reject the chunk with `CHUNK_CORRUPTED`.
- **Root Cause:** Attempting dynamic iterative slice reduction at chunk transmission time rather than upfront admission.
- **Mitigation:**
  1. Strict canonical partition geometry is committed upon `PartitionedSnapshot` creation and is immutable for the transfer.
  2. Upfront reservation (1,800 B) guarantees the chunk fits within the transport limit.
  3. If a pathological send-boundary overshoot occurs ($L_i > \text{effectiveLimit}$), the pipeline **never** shrinks the individual chunk; it immediately aborts the transfer with `TRANSPORT_LIMIT_EXCEEDED` and notifies the peer via `SNAPSHOT_ABORT`.

---

### R-M4-2-4: Outbound Memory Exhaustion (OOM)
- **Impact:** Sender serving multiple snapshot requests runs out of memory on constrained mobile browsers.
- **Root Cause:** Capturing multiple 6 MiB snapshot objects concurrently, holding JSON strings alive, or duplicating buffers into 128 independent chunk arrays.
- **Mitigation:**
  1. **Sequential Preparation Gate (`MAX_CONCURRENT_PREPARATIONS = 1`):** Captures and serializes snapshots one at a time. Snapshot capture occurs inside the gate, preventing concurrent snapshot object graphs.
  2. **Metadata-Only Queue (`depth <= 4`):** Queue footprint is $< 1$ KiB. Saturated requests are rejected with `LOCAL_ABORT`.
  3. **Immediate Dereferencing:** Snapshot objects and canonical JSON strings are dereferenced immediately upon master buffer allocation.
  4. **Contiguous Allocation & Borrowed Slices:** Serializer creates exactly one master `ArrayBuffer`. Slices are zero-copy borrowed views.
  5. **On-Demand Base64:** Base64 strings are generated strictly per transmitted chunk.
  6. **Budget Ceilings:** Application-owned allocations are bounded to $\le 9.0$ MiB retained and $\le 24.0$ MiB peak transient.

---

### R-M4-2-5: Non-Deterministic JSON Serialization Digest Mismatch
- **Impact:** Receiver reassembles contiguous buffer, but computed SHA-256 does not match `expectedSha256Digest`, causing receiver to abort with `HASH_MISMATCH`.
- **Root Cause:** Unsorted object keys, arbitrary checkpoint ordering, or platform-dependent number formatting.
- **Mitigation:**
  1. RFC 8785 JSON Canonicalization Scheme (`canonicalizeJson`) guarantees lexicographical key sorting and IEEE-754 canonical number formatting.
  2. `compactedCheckpoints` are deterministically sorted by `(originPeerId, sequenceNumber)` before serialization.
  3. Preimage explicitly excludes the `sha256Digest` field itself to avoid circular hash dependencies.

---

### R-M4-2-6: Membership Epoch Transitions During Outbound Preparation
- **Impact:** Sender transmits a snapshot offer or chunks under Epoch $E$, but the session has already transitioned to Epoch $E+1$, causing receivers to reject chunks with `EPOCH_MISMATCH`.
- **Mitigation:**
  1. `OutboundContext` binds `membershipEpoch`.
  2. `SnapshotOutboundPipeline` validates that `context.membershipEpoch === partition.artifact.membershipEpoch`.
  3. If epoch increments, the outbound transfer is cancelled synchronously with zero state leakage.

---

### R-M4-2-7: External Buffer Mutation
- **Impact:** A caller modifies bytes in the snapshot buffer, corrupting chunk hashes, reassembly, or concurrent transfers sharing the artifact.
- **Root Cause:** JavaScript TypedArray views allow element mutations at runtime even if typed as `readonly` or if `Object.freeze()` is called on the TypedArray.
- **Mitigation:**
  1. `SerializedSnapshotArtifact` holds `#masterBuffer: ArrayBuffer` privately.
  2. No public property exposes `#masterBuffer`.
  3. Public consumers call `cloneBytes()` or `copyByteRange()`, which return unaliased copies. Mutating copies has zero effect on `#masterBuffer`.
  4. Internal recovery components use package-private `getBorrowedSliceView()`, which is used strictly synchronously for Base64 encoding and never escapes.

---

### R-M4-2-8: Browser `btoa` Call Stack Limit Exceeded
- **Impact:** Calling `String.fromCharCode(...largeArray)` in browser throws `RangeError: Maximum call stack size exceeded`.
- **Mitigation:**
  1. M4.1 isomorphic Base64 encoder `encodeBase64()` implements internal 8,192-byte chunking (`CHUNK_SIZE = 8192`), preventing stack overflows across all browser versions.
