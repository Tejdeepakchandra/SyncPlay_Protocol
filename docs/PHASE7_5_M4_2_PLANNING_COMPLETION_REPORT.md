# Phase 7.5 M4.2 — Snapshot Serialization & Outbound Pipeline Planning Completion Report

**Planning Target:** Milestone M4.2: Snapshot Serialization & Outbound Pipeline  
**Frozen Baseline Commit:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Direct Parent Commit:** `55bdf80d7fb4bc573d8b5abe672d06dec969678d`  
**Mode:** Architecture Research, Design, and Planning Only (Zero Runtime Code)  
**Date:** 2026-11-20  

---

## 1. Executive Summary

Milestone M4.2 establishes the architectural design, formal interfaces, implementation roadmap, acceptance criteria, and risk mitigation strategies for the **sender-side snapshot serialization and outbound transfer pipeline**.

This component prepares validated engine snapshots (`StateSnapshotPayloadV2`) for network transmission over WebRTC SCTP DataChannels. It deterministically serializes snapshot preimages into canonical RFC 8785 JSON bytes, partitions the contiguous buffer into zero-copy chunk slices, calculates per-chunk SHA-256 digests, encodes slices into browser-compatible Base64, and formats strictly bounded wire protocol messages (`SNAPSHOT_OFFER`, `SNAPSHOT_CHUNK`) that are guaranteed to stay within negotiated transport limits via a normative 7-step iterative sizing loop.

All investigations, specifications, and planning documents are complete. No runtime code or tests were modified.

---

## 2. Investigation Findings & Resolution of Limits Inconsistency

### 2.1 The 4 MiB vs. 64 MiB Discrepancy
An exhaustive source and document audit reconciled all historical limit references:
1. **The 64 MiB / 262,144 Chunks Origin:** During the Phase 7.5 M4.1 audit questionnaire, a theoretical upper bound of 64 MiB was referenced ($64 \text{ MiB} / 256 \text{ B} = 262,144$ chunks). This was an unconstrained hypothetical consideration from early RFC discussions.
2. **The Production Reality:** Across the entire committed repository—`packages/protocol/src/constants.ts`, `packages/protocol/src/payloads/snapshot-recovery.ts`, `packages/protocol/src/admission.ts`, `packages/sync-engine/src/snapshot.ts`, and `packages/sync-engine/src/recovery/chunk-assembler.ts`—the codebase uniformly enforces:
   - `MAX_SNAPSHOT_BYTES = 4,194,304` (4 MiB)
   - `MAX_CHUNK_BYTES = 32,768` (32 KiB)
   - `MAX_CHUNKS_PER_SNAPSHOT = 128`
   - `MIN_RAW_CHUNK_BYTES = 256`
   - `MAX_ENVELOPE_OVERHEAD_BYTES = 1,800`

### 2.2 Comparison Table of All System Limits

| Parameter | Frozen Constant | Authoritative Value | Source Location | Status |
| :--- | :--- | :---: | :--- | :--- |
| **Max Snapshot Payload** | `MAX_SNAPSHOT_BYTES` | **4,194,304 B (4 MiB)** | `protocol/src/constants.ts#L81` | Enforced across export, schemas, and assembler |
| **Max Binary Chunk** | `MAX_CHUNK_BYTES` | **32,768 B (32 KiB)** | `protocol/src/constants.ts#L127` | Enforced in schemas and admission |
| **Max Chunks per Transfer** | `MAX_CHUNKS_PER_SNAPSHOT` | **128 chunks** | `protocol/src/constants.ts#L132` | Enforced in offer, request, and chunk schemas |
| **Min Binary Chunk** | `MIN_RAW_CHUNK_BYTES` | **256 B** | `protocol/src/constants.ts#L102` | Enforced in admission and sizing loop |
| **Envelope Framing Bound** | `MAX_ENVELOPE_OVERHEAD_BYTES` | **1,800 B** | `protocol/src/constants.ts#L97` | Enforced in chunk capacity calculation |
| **Transport Floor** | `MIN_TRANSPORT_LIMIT_CHUNK_TRANSFER` | **2,144 B** | `protocol/src/constants.ts#L116` | Absolute minimum for 256 B chunk (1,800 + 344) |
| **Full Chunk Floor** | `MIN_TRANSPORT_LIMIT_FULL_CHUNK` | **45,492 B** | `protocol/src/constants.ts#L122` | Minimum for 32 KiB chunk (1,800 + 43,692) |
| **Max Concurrent Requests** | `MAX_CONCURRENT_CHUNK_REQUESTS` | **4 chunks** | `protocol/src/constants.ts#L137` | Flow control pipelining ceiling |
| **Max Sizing Iterations** | `MAX_SIZING_ITERATIONS` | **5 iterations** | Architecture §6.2 | Hard bound on iterative send loop |

### 2.3 Mathematical Proof of Limit Harmonization
$$\frac{\text{MAX\_SNAPSHOT\_BYTES}}{\text{MAX\_CHUNK\_BYTES}} = \frac{4,194,304 \text{ B}}{32,768 \text{ B}} = \mathbf{128 \text{ chunks exactly}}$$
The system exhibits mathematical closure: at maximum raw chunk capacity, a full 4 MiB snapshot partitions into exactly 128 chunks. On constrained transports ($< 45,492$ B), admission scales the allowable snapshot size proportionately ($S \le 128 \times C$), rejecting oversized snapshots upfront without memory or network waste.

---

## 3. Memory Budget Analysis & Zero-Copy Guarantee

1. **Zero-Copy Slicing:** Rather than allocating 128 independent chunk buffers, `SnapshotChunkProducer` uses `rawBytes.subarray(start, end)`, creating lightweight TypedArray views referencing the identical underlying `ArrayBuffer`. View object overhead is $< 10$ KiB total.
2. **On-Demand Base64 Framing:** Base64 encoding and JSON wire message creation occur on-demand when sending individual chunks, not pre-allocated upfront for all 128 chunks simultaneously.
3. **Peak Memory Footprint:**
   - Active Snapshot Preimage Object: ~6 MiB
   - Contiguous Serialized Buffer: 4.19 MiB
   - Chunk Slices (128 views): ~10 KiB
   - Active In-Flight Chunk Frame: ~45 KiB
   - Peak Footprint (serving 2 concurrent transfers): **$\le 14.8$ MiB**
   - Strictly bounded within existing $\le 16$ MiB budget ceiling.

---

## 4. Deterministic Serialization & Digest Architecture

1. **Canonical Scheme:** Implements RFC 8785 JSON Canonicalization Scheme (JCS) via `canonicalizeJson`.
2. **Preimage Scope:** Covers all snapshot fields (`formatVersion`, `snapshotId`, `sessionId`, `membershipEpoch`, `authorPeerId`, `snapshotFrontier`, `maxLogicalTimestamp`, `originFrontierTimestamps`, `compactedCheckpoints`, and `state`) while excluding `sha256Digest`.
3. **Deterministic Checkpoint Order:** `compactedCheckpoints` are sorted by `(originPeerId, sequenceNumber)` prior to serialization.
4. **Isomorphic Cryptography:** Pure TypeScript synchronous SHA-256 (`sha256()`) guarantees bit-exact digests matching `computeSnapshotDigestV2()` across Node.js, Chromium, Safari, and Web Workers.

---

## 5. Transport Sizing & Negotiation Contract

1. **Effective Limit Derivation:** Sender queries the recipient's WebRTC DataChannel limit clamped to local policy:
   $$\text{effectiveLimit} = \min(65536, \text{peerConnection.dataChannel.maxMessageSize} \mathbin{??} 65536)$$
2. **Asymmetric Transport Policy:** Senders size chunk offers and slices to the specific recipient's transport capacity.
3. **Immutable Geometry Guarantee:** Once a `SNAPSHOT_OFFER` is generated, the sender **never** mutates `nominalChunkSizeBytes`, `totalChunks`, or chunk indices mid-flight.
4. **Receiver Admission Alignment:** If the recipient cannot accept the offered geometry, it returns `SNAPSHOT_ABORT` with `OFFER_INCONSISTENT` or `TRANSPORT_LIMIT_EXCEEDED`. Renegotiated transfers require a fresh `transferId` and new offer.
5. **Normative 7-Step Iterative Sizing Loop:** Guaranteed monotonic reduction by $\Delta_k \ge 33$ B per step with a hard termination bound of $\le 5$ iterations. Exact UTF-8 byte length is verified using `new TextEncoder().encode()`.

---

## 6. Proposed M4.2 Interfaces Summary

1. `SnapshotSerializer`:
   - `serialize(snapshot: StateSnapshotPayloadV2): SerializedSnapshotArtifact`
   - `serializeFromEngine(state, context): SerializedSnapshotArtifact`
2. `SnapshotChunkProducer`:
   - `partition(artifact: SerializedSnapshotArtifact, config: ChunkProducerConfig): PartitionedSnapshot`
3. `SnapshotOutboundPipeline`:
   - `createOfferMessage(partition, context): ProtocolMessage<SnapshotOfferPayload>`
   - `prepareChunkMessage(partition, chunkIndex, context): OutboundChunkResult`

All interfaces return immutable `Readonly` structures, execute synchronously, and perform zero network I/O or WebRTC API manipulation.

---

## 7. Deliverable Documents Produced

The following five coherent planning documents have been authored and added to the repository:
1. [docs/PHASE7_5_M4_2_ARCHITECTURE.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_2_ARCHITECTURE.md): Complete architecture, limit proofs, zero-copy memory model, transport sizing, and iterative sizing algorithm.
2. [docs/PHASE7_5_M4_2_INTERFACE_SPECIFICATION.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_2_INTERFACE_SPECIFICATION.md): Formal TypeScript interfaces, types, buffer ownership rules, and error contracts.
3. [docs/PHASE7_5_M4_2_IMPLEMENTATION_ROADMAP.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_2_IMPLEMENTATION_ROADMAP.md): Four-stage implementation plan (M4.2a–M4.2d) with clear deliverables and exit criteria.
4. [docs/PHASE7_5_M4_2_ACCEPTANCE_CRITERIA.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_2_ACCEPTANCE_CRITERIA.md): 15 granular, testable pass/fail acceptance criteria (AC-M4-SER, AC-M4-PRT, AC-M4-SIZ, AC-M4-B64, AC-M4-MEM, AC-M4-ISO, AC-M4-COM).
5. [docs/PHASE7_5_M4_2_RISK_REGISTER.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/PHASE7_5_M4_2_RISK_REGISTER.md): 8 architectural risks, severity ratings, triggers, and automated mitigations.
6. [docs/design/README.md](file:///c:/Users/tejde/OneDrive/Desktop/SyncPlay_Protocol/SyncPlay_Protocol/docs/design/README.md): Updated master design document index.

---

## 8. Scope Compliance & Verification

- **Zero Runtime Code Modification:** No code in `packages/` was created, edited, or deleted.
- **Zero Test Modification:** Frozen M4.1 test suites remain 100% untouched.
- **Milestone Boundaries Preserved:** No work for M4.3 (Requester pipeline / coordinator), M4.4 (Lifecycle), M5 (Installation), or M6 (Autonomous recovery) was started.
- **Milestone M4.2 Planning Status:** **COMPLETE & AWAITING IMPLEMENTATION AUTHORIZATION.**
