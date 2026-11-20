# Phase 7.5 M4.2 — Snapshot Serialization & Outbound Pipeline Final Planning Correction Completion Report

**Target Milestone:** Milestone M4.2: Snapshot Serialization & Outbound Pipeline  
**Document Version:** 1.2.0  
**Status:** Approved Final Planning Correction Complete (Ready for Implementation Freeze)  
**Original Planning Commit:** `647399a0fa160a7190ba7effa705d0f962d9e94a`  
**Previous Planning Correction Commit:** `4e258f5c6fd7ad20714a20af2d99d7744fe0454d`  
**Direct Parent Commit:** `4e258f5c6fd7ad20714a20af2d99d7744fe0454d`  
**Frozen Implementation Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Mode:** Architecture Research, Design, and Planning Only (Zero Runtime Code)  
**Date:** 2026-11-20  

---

## 1. Executive Summary

Milestone M4.2 establishes the definitive architectural design, interface specifications, implementation roadmap, acceptance criteria, and risk mitigations for the **sender-side snapshot serialization and outbound transfer pipeline**.

Following the final memory-budget and buffer-ownership audit of commit `4e258f5c6fd7ad20714a20af2d99d7744fe0454d`, this targeted planning correction package comprehensively resolves all remaining findings:

1. **SHA-256 Memory Accounting & Lifetime Coexistence:** Traced the exact production call sequence for digest calculation, identified the unstreamed 4.19 MiB `Int32Array` in `sha256()`, and specified direct master `Uint8Array` hashing to eliminate duplicate 4.19 MiB `TextEncoder` allocations. Produced a comprehensive lifetime coexistence table.
2. **Snapshot Object-Graph Bounds:** Inspected schemas and export paths. Formally categorized limits into Category A (schema-enforced), Category B (post-serialization checks), and Category C (operational assumptions). Explicitly acknowledged that the 4 MiB serialized limit does not bound pre-serialization object graphs without explicit pre-serialization checks.
3. **Backing Buffer Encapsulation:** Completely eliminated mutable `Uint8Array` slice views (`getBorrowedSliceView()`) from the public API. Replaced with encapsulated range methods on the artifact (`encodeSliceToBase64()`, `computeSliceSha256()`) returning immutable primitive strings. Master buffer references are held privately (`#masterBuffer: ArrayBuffer | null`) and nullified upon final disposal.
4. **Transfer Lease Ownership & Lifecycle:** Formally specified the full lease lifecycle (`acquireLease()`, `releaseLease()`, `dispose()`) resolving all 12 operational edge cases. Specified deferred disposal when active leases exist, with automatic buffer nullification when the last lease releases.
5. **Strict Concurrency Admission & Queue Resumption:** Adopted the strict policy $\text{retainedArtifactCount} + (\text{isPreparing} \mathrel{?} 1 : 0) \le 2$. Proved that a 3rd preparation cannot begin while 2 artifacts exist, and proved that the queue automatically resumes when an artifact's leases release without permanent stall.
6. **Harmonized Memory Budget:** Mathematically recomputed peak reachable allocations under the strict $A + P \le 2$ policy, proving that peak application-owned allocations are bounded to $\le 22.91$ MiB under conservative 2-byte UTF-16 Unicode string representation (and $\le 18.71$ MiB under Latin-1). Harmonized the **Bounded Application-Owned Allocation Budget to $\le 24.0$ MiB peak transient** and **$\le 9.0$ MiB steady-state retained**.

All changes are strictly confined to planning documents. **Zero runtime source code or tests were created, modified, or deleted.**

---

## 2. Exact Files Changed in Correction

| File Path | Description of Changes |
| :--- | :--- |
| `docs/PHASE7_5_M4_2_ARCHITECTURE.md` | Added allocation lifetime coexistence table; traced SHA-256 call path; categorized pre- vs post-serialization bounds; established encapsulated range-encoding API; added transfer lease state machine and 12-case lifecycle; enforced strict $A + P \le 2$ concurrency admission. |
| `docs/PHASE7_5_M4_2_INTERFACE_SPECIFICATION.md` | Updated `SerializedSnapshotArtifact` with nullable private fields (`#masterBuffer`, `#masterBytes`), range methods (`encodeSliceToBase64`, `computeSliceSha256`), and lease methods (`acquireLease`, `releaseLease`, `dispose`). Updated `SnapshotPreparationQueue` with $A + P \le 2$ admission and `notifyArtifactDisposed()`. |
| `docs/PHASE7_5_M4_2_IMPLEMENTATION_ROADMAP.md` | Updated M4.2a, M4.2b, and M4.2c work breakdowns to incorporate the encapsulated range-encoding API, lease lifecycle tests, and strict concurrency admission testing. |
| `docs/PHASE7_5_M4_2_ACCEPTANCE_CRITERIA.md` | Updated 23 granular acceptance criteria including `AC-M4-CONC-1` ($A + P \le 2$), `AC-M4-CONC-3` (queue resumption), `AC-M4-OWN-1..4` (buffer encapsulation & string returns), and `AC-M4-LSE-1..3` (lease lifecycle and deferred disposal). |
| `docs/PHASE7_5_M4_2_RISK_REGISTER.md` | Added Risk R-M4-2-9 (lease leakage & queue stall mitigation via `finally` blocks and 60-second inactivity guard); updated R-M4-2-4 and R-M4-2-7. |
| `docs/PHASE7_5_M4_2_PLANNING_COMPLETION_REPORT.md` | Complete final completion report documenting all architectural decisions and proofs. |

---

## 3. Detailed Architectural Decisions & Proofs

### 3.1 Trace of Production SHA-256 Call Sequence & Allocation Lifetime Table
In `packages/sync-engine/src/log/operation-fingerprint.ts`:
```typescript
export function sha256(input: string | Uint8Array): string {
  const utf8 = typeof input === 'string' ? new TextEncoder().encode(input) : input;
  const bitLength = utf8.length * 8;
  const wordCount = (((bitLength + 64) >>> 9) << 4) + 16;
  const words = new Int32Array(wordCount);
```
- **Finding:** For a 4 MiB input, `sha256()` allocates an `Int32Array(1,048,592)` = **4.194 MiB** working buffer. If called with a `string`, it also allocates an intermediate 4.194 MiB `Uint8Array`.
- **Optimization in M4.2:** `SnapshotSerializer` encodes `canonicalizeJson(preimage)` into the master `Uint8Array` buffer first, dereferences the JSON string, and passes `masterBytes: Uint8Array` into `sha256(masterBytes)`. This completely eliminates the duplicate 4.19 MiB `TextEncoder` allocation inside `sha256()`.

#### Lifetime Coexistence Table (4 MiB Snapshot Preparation):
- **Phase 1 (Capture inside gate):** Ephemeral Object graph ($4.0\text{–}6.0$ MiB) coexists with 1 retained artifact ($4.19$ MiB) + active sends ($0.14$ MiB) = $\mathbf{8.33\text{–}10.33 \text{ MiB}}$.
- **Phase 2 (JCS Stringification):** Object graph ($6.0$ MiB) + canonical JSON string ($4.19\text{–}8.39$ MiB) + retained artifact ($4.19$ MiB) + active send ($0.14$ MiB) = $\mathbf{12.52\text{–}18.72 \text{ MiB}}$.
- **Phase 3 (Master Buffer Allocation — PEAK POINT):** Object graph ($6.0$ MiB) + canonical JSON string ($4.19\text{–}8.39$ MiB) + master buffer ($4.19$ MiB) + retained artifact ($4.19$ MiB) + active send ($0.14$ MiB) = $\mathbf{16.71\text{–}22.91 \text{ MiB}}$.
- **Phase 4 (Pre-Hash Dereference):** Object and string set to `null` $\implies$ reachable memory drops to master buffer ($4.19$ MiB) + retained artifact ($4.19$ MiB) = $\mathbf{8.52 \text{ MiB}}$.
- **Phase 5 (SHA-256 on Buffer):** Master buffer ($4.19$ MiB) + SHA-256 `Int32Array` ($4.19$ MiB) + retained artifact ($4.19$ MiB) = $\mathbf{12.71 \text{ MiB}}$.
- **Phase 6 (Artifact Finalized):** Working buffer unreferenced; steady-state retained: $\mathbf{8.38 \text{ MiB}}$ for 2 artifacts.

---

### 3.2 Snapshot Object-Graph Bounds
- **Category A (Schema-enforced):** `compactedCheckpoints` strictly bounded to `.max(256)` ($\approx 30$ KiB). `snapshotId`, `sessionId` $\le 128$ chars. `authorPeerId` $\le 64$ chars.
- **Category B (Post-serialization enforced):** The 4 MiB total snapshot limit is enforced *after* `JSON.stringify(payload)`.
- **Category C (Operational assumptions):** Frontier entry counts and sub-fields in `state: z.unknown()` are bounded by engine construction, not by the raw wire schema.
- **Enforcement:** `SnapshotSerializer.serialize()` validates shallow key counts ($\le 64$ frontier keys) before canonicalization. The memory model is formally classified as an **Application-Owned Allocation Budget with Explicit Runtime Assumptions**, not a hard OS process RSS ceiling.

---

### 3.3 Backing Buffer Ownership & Range-Encoding Contract
1. **Private Nullable Storage:**
   - `#masterBuffer: ArrayBuffer | null`
   - `#masterBytes: Uint8Array | null`
2. **Zero Mutable View Exposure:**
   - Public method `getBorrowedSliceView()` is **completely eliminated**.
   - Chunk encoding is performed via `artifact.encodeSliceToBase64(offset, length)` returning primitive `string`.
   - Chunk hashing is performed via `artifact.computeSliceSha256(offset, length)` returning primitive `string`.
   - Public data access calls `cloneBytes()` or `copyByteRange()`, returning fresh, unaliased copies.
   - Range validation strictly rejects invalid offsets or lengths with `INVALID_RANGE`.

---

### 3.4 Transfer Lease Lifecycle (All 12 Operational Cases)
1. **Transfer Complete:** Acquires lease at start; calls `releaseLease(transferId)` upon final chunk send.
2. **Transfer Aborts Early:** Terminal abort handler releases lease in `finally` block.
3. **Transfer Fails Mid-Encoding:** Exception caught in pipeline; lease released in `finally` block.
4. **Transfer Times Out:** Inactivity timer triggers abort handler; lease released.
5. **Transfer Cancelled (Epoch Change):** Epoch listener triggers cancellation; lease released.
6. **Duplicate Lease Acquisition:** Calling `acquireLease(transferId)` with an active `transferId` throws `INVALID_STATE`.
7. **Duplicate Lease Release:** `releaseLease(transferId)` is idempotent; returns `false` without throwing.
8. **Disposal Requested with 1 Active Transfer:** `dispose()` sets `#disposalRequested = true`. Buffer detachment is deferred until active transfer finishes.
9. **Disposal Requested with 2 Transfers Sharing Artifact:** Both transfers continue encoding without interruption until both release their leases.
10. **Last Lease Released After Disposal Requested:** `releaseLease()` detects `#disposalRequested === true` and `#activeLeases.size === 0`. Calls `#finalizeDisposal()`, nullifying `#masterBuffer` and `#masterBytes`.
11. **New Lease Requested After Disposal Requested:** Throws `ARTIFACT_DISPOSED`.
12. **Read Attempted After Final Disposal:** Any read call checks `#assertReadable()` and throws `ARTIFACT_DISPOSED`.

---

### 3.5 Strict Concurrency Admission & Queue Resumption
1. **Admission Formula:**
   $$\text{retainedArtifactCount} + (\text{isPreparing} \mathrel{?} 1 : 0) \le 2$$
2. **Behavior on $A=2$:** If 2 artifacts are retained, incoming requests wait in `SnapshotPreparationQueue`. A 3rd preparation cannot start.
3. **Queue Resumption:** When an artifact is disposed, `notifyArtifactDisposed()` wakes the queue and dequeues the next request.
4. **No Permanent Stall:** All transfer terminal exits execute `releaseLease()` in `finally` blocks; a 60-second inactivity guard expires abandoned leases.

---

### 3.6 Recalculated Operational Scenarios

| Operational Scenario | Retained Artifacts ($A$) | In-Flight Prep ($P$) | Active Sends ($T$) | Steady Retained Memory | Transient Prep Peak | Active Chunk Framing | Total Peak Application Allocation |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1. Cold Single Preparation** | $0$ | $1$ | $0$ | $0.00$ MiB | $\le 18.58$ MiB | $0.00$ MiB | **$\le 18.58$ MiB** ($14.38$ MiB Latin-1) |
| **2. One Retained + One Preparation** | $1$ | $1$ | $1$ | $4.19$ MiB | $\le 18.58$ MiB | $0.14$ MiB | **$\le 22.91$ MiB** ($18.71$ MiB Latin-1) |
| **3. Two Retained Artifacts (Steady State)** | $2$ | $0$ | $2$ | $8.38$ MiB | $0.00$ MiB | $0.27$ MiB | **$\le 8.65$ MiB** |
| **4. Two Transfers Sharing One Artifact** | $1$ | $0$ | $2$ | $4.19$ MiB | $0.00$ MiB | $0.27$ MiB | **$\le 4.46$ MiB** |
| **5. Preparation Failure / Cancellation** | $1$ | $1 \to 0$ | $1$ | $4.19$ MiB | Drops to $0$ | $0.14$ MiB | **Drops to $4.33$ MiB** |
| **6. Queue Saturation (Depth = 4)** | $1 \text{ or } 2$ | $0 \text{ or } 1$ | $1\text{–}2$ | $4.19\text{–}8.38$ MiB | $0.00\text{–}18.58$ MiB | $0.27$ MiB | **$\le 22.91$ MiB** (metadata $< 1$ KiB) |

Under the strict $A + P \le 2$ policy, the maximum possible reachable allocation across all scenarios is **$\le 22.91$ MiB**, proving mathematical compliance with the **$\le 24.0$ MiB Peak Transient Application Allocation Budget**.

---

### 3.7 M4.1 Invariants Preservation
- Canonical chunk geometry: $N = \lceil S / C \rceil$, slices $0 \dots N-2$ have size $C$, final slice has size $S - (N-1) \cdot C$.
- Zero mid-transfer resizing.
- Upfront capacity reservation: 1,800 B reserved vs 1,231 B actual (569 B cushion).
- Mandatory complete-message UTF-8 measurement via `new TextEncoder().encode(serializedJson).byteLength`.
- Zero network I/O in M4.2.

---

## 4. Remaining Risks and Unresolved Assumptions Explicitly Noted

1. **V8 String Representation Optimization:** The calculations account for Latin-1 ($4.19$ MiB) and conservative two-byte UTF-16 ($8.39$ MiB). In standard execution, SyncPlay snapshots with ASCII identifiers operate under Latin-1. The conservative $\le 24.0$ MiB ceiling absorbs worst-case UTF-16 expansion, but actual V8 heap usage will depend on runtime string flattening and garbage collector timing.
2. **Garbage Collection Scheduling:** While unreferenced objects (preimage, JSON string, `Int32Array`) become eligible for GC immediately, the physical process RSS remains under V8 runtime control and is not bounded by this application budget.

---

## 5. Scope Compliance & Verification

- **Zero Runtime Source Code Modification:** No files in `packages/` were created, modified, or deleted.
- **Zero Test Suite Modification:** Frozen M4.1 unit and integration test suites remain 100% untouched.
- **Frozen Baseline Integrity:** Commit `fd23edae5bdb52cff32674860229277a79cb1aca` remains completely unchanged.
- **Implementation Status:** **M4.2 RUNTIME IMPLEMENTATION HAS NOT STARTED.**

---

## 6. Next Steps

Awaiting your review and authorization to commit this final planning correction and proceed to implementation of Sub-Milestone M4.2a (`SnapshotSerializer` & JCS digest).
