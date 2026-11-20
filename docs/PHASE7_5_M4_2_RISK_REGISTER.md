# Phase 7.5 M4.2 — Risk Register: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.0.0  
**Status:** Approved Risk Register  
**Target Milestone:** M4.2  
**Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  

---

## 1. Risk Register Summary Matrix

| Risk ID | Severity | Likelihood | Category | Summary Description | Mitigation Strategy |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **R-M4-2-1** | **CRITICAL** | Low | Transport Boundary | Serialized chunk exceeds negotiated SCTP limit at send boundary. | Normative 7-step iterative sizing loop measuring exact UTF-8 bytes via `TextEncoder`. |
| **R-M4-2-2** | **HIGH** | Medium | Asymmetric Transport | Sender's transport limit differs from receiver's inbound capacity. | Supplier sizes chunks to recipient's limit; receiver admission gate rejects incompatible offers upfront. |
| **R-M4-2-3** | **HIGH** | Low | Sizing Loop | Iterative reduction fails to converge or oscillates. | Hard operational bound: $\Delta_k \ge 33$ B guaranteed monotonic drop; abort after $\le 5$ iterations. |
| **R-M4-2-4** | **MEDIUM** | Low | Memory Management | Holding multiple full snapshot copies causes browser tab OOM. | Zero-copy `subarray()` views over contiguous buffer; on-demand chunk Base64 framing. |
| **R-M4-2-5** | **HIGH** | Low | Data Integrity | Non-deterministic JSON serialization causes digest mismatch. | RFC 8785 JSON Canonicalization Scheme (JCS) with deterministic checkpoint sorting. |
| **R-M4-2-6** | **MEDIUM** | Low | Lifecycle Fencing | Epoch changes while outbound transfer preparation is in-flight. | Epoch binding in offer and chunk context; reject preparation if epoch has incremented. |
| **R-M4-2-7** | **MEDIUM** | Low | Buffer Safety | External caller mutates underlying snapshot buffer during transfer. | Artifacts return `readonly` views; internal buffer ownership is encapsulated. |
| **R-M4-2-8** | **LOW** | Low | Browser Compatibility | `String.fromCharCode` stack overflow on large binary buffers in browsers. | Isomorphic `encodeBase64()` uses 8,192-byte chunking for browser `btoa` conversion. |

---

## 2. Detailed Risk Analysis & Mitigation Strategies

### R-M4-2-1: Serialized Chunk Exceeds SCTP Limit at Send Boundary
- **Impact:** If `PeerManager.sendSerialized()` transmits a frame exceeding the underlying SCTP data channel buffer or MTU, the browser WebRTC stack silently drops the packet or closes the data channel.
- **Root Cause:** Estimating JSON message length from character count or estimating Base64 expansion rather than measuring actual UTF-8 encoded bytes.
- **Mitigation:**
  1. `SnapshotOutboundPipeline` measures exact byte length using `new TextEncoder().encode(serializedJson).byteLength`.
  2. The exact measured string is returned in `OutboundChunkResult.serializedJson` and passed unchanged to `sendSerialized()`.
  3. `MAX_ENVELOPE_OVERHEAD_BYTES = 1,800` provides an empirical 569-byte safety cushion over worst-case framing (1,231 B).

---

### R-M4-2-2: Asymmetric Transport Sizing & Offer Divergence
- **Impact:** Sender creates an offer with chunk capacity $C_A$, but receiver's local SCTP limit permits only $C_B < C_A$. Receiver admission rejects the offer with `OFFER_INCONSISTENT` or `TRANSPORT_LIMIT_EXCEEDED`.
- **Root Cause:** Asymmetric SDP `a=max-message-size` declarations between peers.
- **Mitigation:**
  1. The supplier evaluates `calculateSnapshotAdmission()` against the specific recipient's transport limit before generating the offer.
  2. In `SNAPSHOT_REQUEST`, the receiver includes `clientMaxChunkBytes`.
  3. If admission fails at the receiver, the receiver cleanly returns `SNAPSHOT_ABORT`, leaving active engine state untouched. Mid-transfer geometry mutation is strictly prohibited.

---

### R-M4-2-3: Sizing Loop Non-Convergence or Oscillation
- **Impact:** Loop executes indefinitely, blocking the main event loop.
- **Root Cause:** Binary slice reduction formula is too conservative, or envelope overhead grows faster than binary reduction.
- **Mitigation:**
  1. Binary reduction step is strictly monotonic:
     $$\Delta_k = \left\lceil O_k \times \frac{3}{4} \right\rceil + 32 \ge 33 \text{ bytes}$$
  2. Envelope overhead is structurally invariant (fixed keys and bounded IDs); framing length does not increase as slice size decreases.
  3. Hard operational bound: abort with `TRANSPORT_LIMIT_EXCEEDED` if iteration count exceeds `MAX_SIZING_ITERATIONS = 5` or slice drops $< 256$ B.

---

### R-M4-2-4: Outbound Memory Exhaustion (OOM)
- **Impact:** Sender serving multiple snapshot requests runs out of memory on constrained mobile browsers.
- **Root Cause:** Duplicating 4 MiB buffers into 128 independent chunk array buffers.
- **Mitigation:**
  1. Contiguous allocation: Serializer creates exactly one 4 MiB `Uint8Array`.
  2. Zero-copy slices: Chunk producer creates `subarray()` views sharing the same underlying `ArrayBuffer`.
  3. Transient Base64: Base64 strings and framed JSON are created on-demand when sending individual chunks, not pre-allocated upfront for all 128 chunks simultaneously.
  4. Peak memory is strictly bounded $\le 15$ MiB for up to 2 concurrent outbound transfers.

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
- **Impact:** A caller modifies bytes in `SerializedSnapshotArtifact.rawBytes`, corrupting chunk hashes or reassembly.
- **Mitigation:**
  1. TypeScript interfaces declare `rawBytes` as `readonly Uint8Array`.
  2. In internal pipeline methods, slices are accessed as read-only views.

---

### R-M4-2-8: Browser `btoa` Call Stack Limit Exceeded
- **Impact:** Calling `String.fromCharCode(...largeArray)` in browser throws `RangeError: Maximum call stack size exceeded`.
- **Mitigation:**
  1. M4.1 isomorphic Base64 encoder `encodeBase64()` implements internal 8,192-byte chunking (`CHUNK_SIZE = 8192`), preventing stack overflows across all browser versions.
