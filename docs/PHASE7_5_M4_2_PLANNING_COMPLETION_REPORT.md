# Phase 7.5 M4.2 — Snapshot Serialization & Outbound Pipeline Planning Completion Report

**Target Milestone:** Milestone M4.2: Snapshot Serialization & Outbound Pipeline  
**Document Version:** 1.1.0  
**Status:** Approved Planning Correction Complete (Ready for Implementation Freeze)  
**Original Planning Commit:** `647399a0fa160a7190ba7effa705d0f962d9e94a`  
**Frozen Implementation Baseline (Parent):** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Direct Parent Commit:** `647399a0fa160a7190ba7effa705d0f962d9e94a`  
**Mode:** Architecture Research, Design, and Planning Only (Zero Runtime Code)  
**Date:** 2026-11-20  

---

## 1. Executive Summary

Milestone M4.2 establishes the definitive architectural design, interface specifications, implementation roadmap, acceptance criteria, and risk mitigations for the **sender-side snapshot serialization and outbound transfer pipeline**.

Following the independent planning audit of commit `647399a0fa160a7190ba7effa705d0f962d9e94a` (which yielded a verdict of **CONDITIONAL PASS**), this targeted planning correction package comprehensively resolves all three audit findings:
1. **F-M4-2-1:** Corrected the sender memory model, separated transient allocations from retained memory, established a sequential preparation gate with snapshot capture inside the gate, accounted for V8 two-byte UTF-16 Unicode strings, and formally defined the application-owned allocation budget ($\le 9.0$ MiB retained, $\le 24.0$ MiB peak transient).
2. **F-M4-2-2:** Strengthened buffer ownership and immutability by replacing open TypedArray exposure with private `#masterBuffer` encapsulation, package-private borrowed views restricted to trusted recovery components, unaliased public copies, and explicit lifecycle disposal.
3. **F-M4-2-3:** Enforced strict canonical partition geometry, prohibited mid-transfer chunk resizing, clarified the role of upfront 1,800 B capacity reservation (569 B cushion) vs. mandatory exact UTF-8 complete-message send-boundary measurement, and removed flawed 5-iteration sizing assumptions.

All changes are strictly confined to planning documents. **No runtime code or tests were created, modified, or deleted.**

---

## 2. Audit Findings & Detailed Resolutions

### Finding F-M4-2-1: Correct the Sender Memory Model

#### Problem Identified in Audit
The previous planning claim that peak outbound memory is bounded to $\le 14.8$ MiB for two concurrent transfers was mathematically unproven:
- Exporting a 4 MiB snapshot temporarily allocates the V8 object graph ($\approx 6$ MiB), the canonical JSON string ($4.19\text{–}8.39$ MiB), and the UTF-8 `ArrayBuffer` ($4.19$ MiB) simultaneously, creating a transient spike of $\approx 14.4\text{–}18.6$ MiB during preparation alone.
- If two distinct snapshots were serialized concurrently, peak transient heap would reach $\approx 24.8\text{–}28.8$ MiB before garbage collection.
- Retaining the preimage object alongside the buffer would consume $> 20$ MiB steady-state.
- The planning document did not account for two-byte UTF-16 string representations in V8 for non-ASCII Unicode strings.

#### Resolution Implemented
1. **Ten-Part Allocation Partitioning:** Formally separated and bounded all sender allocations:
   - Snapshot Preimage Object ($\approx 4.0\text{–}6.0$ MiB, active only during capture inside the gate).
   - Canonical JSON String ($4.19\text{–}8.39$ MiB, accounting for V8 two-byte UTF-16 worst-case).
   - Master UTF-8 Buffer ($4.194$ MiB contiguous `ArrayBuffer`, privately held by artifact).
   - SHA-256 Working State ($< 64$ KiB transient).
   - Retained Serialized Artifacts ($\approx 4.2$ MiB per artifact; max 2 artifacts retained simultaneously $= 8.4$ MiB).
   - Per-Chunk Base64 Strings ($\approx 43.7$ KiB max, generated on-demand per sent chunk).
   - Serialized Protocol Messages ($\approx 45.0$ KiB max transient JSON frame).
   - UTF-8 Message-Size Measurement Buffers ($\approx 45.0$ KiB max transient `Uint8Array`).
   - Queued Preparation Requests ($< 1$ KiB total for max depth of 4; metadata only).
   - Concurrent Active Transfers ($2 \times 135\text{ KiB} \approx 270\text{ KiB}$ for 2 simultaneous sends).
2. **Lifecycle & Memory Categorization:**
   - Explicitly distinguished: *Logically Retained Memory*, *Transient Live Allocations*, *Allocations Eligible for Collection*, and *Process Heap / RSS*.
   - Explicitly documented that the specification establishes a bounded **Application-Owned Allocation Budget** rather than a hard OS process-level RSS ceiling.
3. **Sequential Preparation Gate & Request Queue:**
   - Introduced `SnapshotPreparationQueue`:
     - `MAX_CONCURRENT_PREPARATIONS = 1` non-reentrant gate.
     - **Snapshot capture (`exportEngineSnapshotV2()`) occurs inside the serialized gate**, preventing concurrent snapshot object graphs.
     - `MAX_PREPARATION_QUEUE_DEPTH = 4`: queue holds lightweight metadata descriptors only ($< 1$ KiB).
     - Saturated requests are immediately rejected with `SNAPSHOT_ABORT` (`reason: 'LOCAL_ABORT'`, detail: `'Preparation queue saturated'`).
4. **Immediate Resource Dereferencing:**
   - Preimage object and canonical JSON string are dereferenced immediately upon master buffer instantiation, becoming eligible for GC before the next queued preparation begins.
5. **Formally Defined Budget Ceilings:**
   - **Steady-State Retained Budget:** $\mathbf{\le 9.0 \text{ MiB}}$ (up to 2 active 4 MiB artifacts + descriptors).
   - **Peak Transient Allocation Budget:** $\mathbf{\le 24.0 \text{ MiB}}$ (1 active retained artifact + 1 sequential preparation with conservative 2-byte UTF-16 representation + active chunk framing).

---

### Finding F-M4-2-2: Strengthen Buffer Ownership and Immutability

#### Problem Identified in Audit
The previous specification declared `rawBytes: readonly Uint8Array`. In JavaScript runtimes:
- TypeScript `readonly` provides zero runtime protection against byte modification (`slice[0] = 0xAA`).
- `Object.freeze()` on typed arrays does not freeze underlying `ArrayBuffer` binary elements.
- `subarray()` views share the identical backing buffer. Any mutation by an external consumer or event listener would corrupt the master buffer and all concurrent transfers sharing that artifact.

#### Resolution Implemented
1. **Private Master Buffer Encapsulation:**
   - `SerializedSnapshotArtifact` holds `#masterBuffer: ArrayBuffer` in a private field.
   - The master buffer is never exposed directly through any public property or getter.
2. **Access Tiers & Escape Boundary:**
   - **Internal Recovery Components:** Access slices via package-private `getBorrowedSliceView(offset, length)` for high-performance zero-copy slicing. Borrowed views are used strictly synchronously within `SnapshotOutboundPipeline` to produce Base64 strings. Borrowed views **never escape** to public APIs, event listeners, or untrusted consumers.
   - **Public Consumers:** Must call `copyByteRange(offset, length)` or `cloneBytes()`, which return freshly allocated, completely unaliased deep copies. Mutating an unaliased clone has zero effect on `#masterBuffer`.
3. **Hash-to-Encoding Byte Identity:**
   - `sha256Digest` is computed directly over the private `#masterBuffer` upon artifact instantiation.
   - Because `#masterBuffer` is privately encapsulated, the bytes read by `getBorrowedSliceView()` during chunk production are guaranteed to be **bit-exact identical** to the bytes that were hashed.
4. **Explicit Disposal:**
   - Added `artifact.dispose()`: detaches buffer references and marks `isDisposed = true`. Subsequent reads throw `SyncEngineError('ARTIFACT_DISPOSED')`.

---

### Finding F-M4-2-3: Enforce Canonical Partition Geometry

#### Problem Identified in Audit
The previous planning documents described a 7-step sizing loop that dynamically reduced individual chunk slice sizes by $\Delta_k \ge 33$ B during chunk framing.
- In frozen M4.1, `ChunkAssembler.ingestChunk()` strictly enforces canonical partition equality:
  $$S_i = C \text{ for } i < N - 1, \quad S_{N-1} = S - (N-1) \cdot C$$
  If `payload.chunkSizeBytes !== expectedChunkSize`, the receiver immediately aborts with `CHUNK_CORRUPTED`.
- Shrinking an individual chunk mid-transfer breaks canonical partition equality and triggers deterministic receiver abort.
- The 5-iteration convergence proof relied on an unproven 40-byte framing variance assumption.

#### Resolution Implemented
1. **Strict Canonical Partition Geometry:**
   - Nominal chunk size $C$, chunk count $N = \lceil S / C \rceil$, and slice sizes are committed upon `PartitionedSnapshot` creation.
   - **Zero Mid-Transfer Resizing Rule:** Once `SNAPSHOT_OFFER` is emitted, individual chunk slices are **never resized**.
2. **Upfront Capacity Reservation:**
   - Sizing is performed *before* partitioning via `calculateChunkCapacity(effectiveLimit)`.
   - Deducts `MAX_ENVELOPE_OVERHEAD_BYTES = 1,800` B upfront.
   - With worst-case measured envelope overhead at 1,231 B, this guarantees an empirical **569-byte safety cushion**. Initial framing is guaranteed to fit within `effectiveLimit`.
3. **Mandatory Send-Boundary Exact UTF-8 Measurement:**
   - `SnapshotOutboundPipeline.prepareChunkMessage()` measures exact complete-message wire bytes:
     $$L = \text{new TextEncoder}().\text{encode}(\text{serializedJson}).\text{byteLength}$$
   - If $L \le \text{effectiveLimit}$: transmission authorized.
   - If $L > \text{effectiveLimit}$: transfer immediately aborts with `SyncEngineError('TRANSPORT_LIMIT_EXCEEDED')` (emitting `SNAPSHOT_ABORT`). Non-canonical chunks are never sent.
4. **Removal of Flawed 5-Iteration Assumptions:**
   - Removed the claim that dynamic per-chunk slice reduction converges in 5 iterations based on 40-byte framing variance.
   - Replaced with the definitive contract: upfront conservative reservation + mandatory exact final UTF-8 measurement at the send boundary.

---

## 3. Authoritative System Limits Summary

| Parameter | Frozen Constant | Authoritative Value | Source Location | Enforcement Point |
| :--- | :--- | :---: | :--- | :--- |
| **Max Snapshot Payload** | `MAX_SNAPSHOT_BYTES` | **4,194,304 B (4 MiB)** | `protocol/src/constants.ts#L81` | Export, schemas, admission, assembler |
| **Max Binary Chunk** | `MAX_CHUNK_BYTES` | **32,768 B (32 KiB)** | `protocol/src/constants.ts#L127` | Schemas, admission, capacity calculation |
| **Max Chunks per Transfer** | `MAX_CHUNKS_PER_SNAPSHOT` | **128 chunks** | `protocol/src/constants.ts#L132` | Offer, request, chunk schemas, admission |
| **Min Binary Chunk** | `MIN_RAW_CHUNK_BYTES` | **256 B** | `protocol/src/constants.ts#L102` | Admission floor, offer validation |
| **Envelope Framing Bound** | `MAX_ENVELOPE_OVERHEAD_BYTES` | **1,800 B** | `protocol/src/constants.ts#L97` | Upfront capacity reservation |
| **Transport Floor** | `MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER` | **2,144 B** | `protocol/src/constants.ts#L116` | Absolute minimum for 256 B chunk (1,800 + 344) |
| **Full Chunk Floor** | `MIN_TRANSPORT_LIMIT_FULL_CHUNK` | **45,492 B** | `protocol/src/constants.ts#L122` | Minimum for 32 KiB chunk (1,800 + 43,692) |
| **Max Concurrent Preps** | `MAX_CONCURRENT_PREPARATIONS` | **1 preparation** | `SnapshotPreparationQueue` | Sequential critical section gate |
| **Max Preparation Queue** | `MAX_PREPARATION_QUEUE_DEPTH` | **4 requests** | `SnapshotPreparationQueue` | Bounded metadata queue (< 1 KiB) |
| **Max Active Artifacts** | `MAX_ACTIVE_ARTIFACTS` | **2 artifacts** | Engine supplier state | Retained artifact cache ceiling |
| **Max Concurrent Transfers**| `MAX_SUPPLIER_CONCURRENT_TRANSFERS` | **2 transfers** | Engine supplier state | Outbound concurrent transfer limit |

$$\frac{\text{MAX\_SNAPSHOT\_BYTES}}{\text{MAX\_CHUNK\_BYTES}} = \frac{4,194,304 \text{ B}}{32,768 \text{ B}} = \mathbf{128 \text{ chunks exactly}}$$

---

## 4. Updated Acceptance Criteria Matrix Summary

The acceptance criteria suite in `docs/PHASE7_5_M4_2_ACCEPTANCE_CRITERIA.md` now contains 22 granular, testable criteria across 8 categories:

1. **Serialization (`AC-M4-SER-1` to `3`):** Deterministic RFC 8785 canonicalization, bit-exact overall SHA-256 digest, 4 MiB size rejection.
2. **Concurrency (`AC-M4-CONC-1` to `2`):** Sequential execution gate (`MAX_CONCURRENT_PREPARATIONS = 1`), capture inside gate, bounded queue depth ($\le 4$), immediate saturation rejection with `LOCAL_ABORT`.
3. **Memory (`AC-M4-MEM-1` to `3`):** Peak transient application allocation bounded $\le 24.0$ MiB, steady-state retained memory bounded $\le 9.0$ MiB for 2 artifacts, immediate dereferencing of preimage object and JSON string.
4. **Encapsulation & Safety (`AC-M4-OWN-1` to `5`):** Private `#masterBuffer`, unaliased public copies via `cloneBytes()`, hash-to-encoding byte identity, concurrent transfer safety without mutation, explicit disposal throwing `ARTIFACT_DISPOSED`.
5. **Partitioning (`AC-M4-PRT-1` to `4`):** Strict canonical partition math ($N = \lceil S / C \rceil$), zero mid-transfer resizing, zero-copy internal borrowing, per-chunk SHA-256 hashes.
6. **Send-Boundary Checks (`AC-M4-SIZ-1` to `3`):** Mandatory exact complete-message UTF-8 measurement via `TextEncoder`, transport ceiling abort with `TRANSPORT_LIMIT_EXCEEDED`, transport floor rejection $< 2,144$ B.
7. **Engine Isolation (`AC-M4-ISO-1`):** Zero side effects on playback state, clock managers, or logs.
8. **M4.1 Compatibility (`AC-M4-COM-1`):** Bit-exact round-trip reassembly in frozen M4.1 `ChunkAssembler`.

---

## 5. Exact Files Modified in Planning Correction

| File Path | Description of Changes |
| :--- | :--- |
| `docs/PHASE7_5_M4_2_ARCHITECTURE.md` | Overhauled memory footprint analysis, added 10-part allocation breakdown, sequential gate, private buffer encapsulation, canonical partition enforcement, and send-boundary measurement. |
| `docs/PHASE7_5_M4_2_INTERFACE_SPECIFICATION.md` | Defined `SnapshotPreparationQueue`, updated `SerializedSnapshotArtifact` with private `#masterBuffer` and unaliased copy methods, removed `snapshot` object property, updated `SnapshotOutboundPipeline` with exact measurement and zero-resizing rules. |
| `docs/PHASE7_5_M4_2_IMPLEMENTATION_ROADMAP.md` | Added `SnapshotPreparationQueue` and `SerializedSnapshotArtifact` tasks to M4.2a, updated M4.2b and M4.2c tasks for canonical partition and send-boundary verification. |
| `docs/PHASE7_5_M4_2_ACCEPTANCE_CRITERIA.md` | Expanded criteria matrix from 15 to 22 granular, testable specifications covering concurrency, memory accounting, buffer encapsulation, disposal, and exact measurement. |
| `docs/PHASE7_5_M4_2_RISK_REGISTER.md` | Updated risk analysis and mitigations for memory exhaustion (R-M4-2-4), canonical geometry conflict (R-M4-2-3), and external buffer mutation (R-M4-2-7). |
| `docs/PHASE7_5_M4_2_PLANNING_COMPLETION_REPORT.md` | Updated completion report documenting all three audit findings, exact resolutions, limits, memory models, and scope compliance. |
| `docs/design/README.md` | Updated design document index descriptions for M4.2 planning suite. |

---

## 6. Scope Compliance & Verification

- **Zero Runtime Source Code Modification:** No files in `packages/` were created, modified, or deleted.
- **Zero Test Suite Modification:** Frozen M4.1 unit and integration test suites remain 100% untouched.
- **Frozen Baseline Integrity:** Commit `fd23edae5bdb52cff32674860229277a79cb1aca` remains completely unchanged.
- **Milestone Scope Integrity:** No code for M4.2 implementation, M4.3 (Requester pipeline / coordinator), M4.4 (Lifecycle), or M5 (State installation) was authored.
- **Working Tree Status:** All planning documentation changes are staged for commit.
- **Implementation Status:** **M4.2 RUNTIME IMPLEMENTATION HAS NOT STARTED.**

---

## 7. Next Steps

Awaiting your review and authorization to commit this planning correction and proceed to M4.2 implementation.
