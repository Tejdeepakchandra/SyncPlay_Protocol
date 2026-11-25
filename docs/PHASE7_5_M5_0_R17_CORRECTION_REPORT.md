# Phase 7.5 Milestone M5.0-R17 — Correction & Architectural Closure Report

**Sub-Milestone Title:** Published Map Encapsulation & Runtime Ownership Correction
**Role:** Senior TypeScript Distributed-Systems Architect
**Mode:** STRICTLY DOCUMENTATION-ONLY CORRECTION AND SOURCE AUDIT
**Target Milestone:** Phase 7.5 Sub-Milestone M5.0-R17
**Starting Baseline Commit:** `a6b34fd15340fbabb558f4cdc32a02854b73df2f` (M5.0-R16)
**Direct Parent Commit:** `32203a3df4c70b379fc6cb4ba0dbc335de8858b6` (M5.0-R15)
**Authoritative Accepted Baselines:**
- M4.3b: `f3c29b62eea63f6b06647218e4485eacba86fb50`
- M4.3c: `eab2428681eb8a05b1c6e71906a932f671730fc8`
- M4.3d: `40b11fdafcd247c7a289723f90ec0e2029c94f64`
- Approved M4.3 Architecture: `2bb117c5db474a13307be65c6f68845f2bd74720`
- Frozen M4.2d Baseline: `aafa366cc48fdb2b83940f00e80d92d813e7c52e`

**Implementation Status: Milestone M5.1 remains STRICTLY UNAUTHORIZED AND UNSTARTED.**

---

## 1. Executive Summary & Objective

Milestone M5.0-R17 resolves the core runtime ownership defect identified by the Engineering Authority following the Milestone M5.0-R16 review:

1. **R17-A — Correct Runtime Map Ownership:**
   - Explicitly refutes the erroneous assertion that `Object.freeze(map)` makes a JavaScript `Map` immutable.
   - Under the ECMAScript 2024 specification (ECMA-262 §24.1.3), `Object.freeze()` operates exclusively on own properties of the object wrapper (setting `[[Configurable]]: false` and `[[Writable]]: false`). It has **zero effect** on internal slots such as `[[MapData]]`. As a result, `map.set()`, `map.delete()`, and `map.clear()` remain fully functional and mutable on a frozen `Map` instance.
   - Establishes the **Eight Rules of Map Ownership & Encapsulation** across `TerminalArchiveCoreState` and `QuarantineBufferCoreState`.
   - Guarantees immutability and linearizability through private collection encapsulation, Copy-on-Write (CoW) allocation, and strict prohibition of direct `Map` reference exposure.
2. **R17-B — Nested Records & Canonical Bytes Protection:**
   - Formalizes the operational reality that `Object.freeze()` is strictly shallow in JavaScript engines. Nested plain objects (such as `QuarantineRetirementTransactionRecord` and `ArchiveReservationRecord`) must be explicitly frozen prior to candidate assembly.
   - Clarifies that TypedArrays (`Uint8Array`) **cannot be frozen** via `Object.freeze()`; in V8/strict mode, executing `Object.freeze()` on a non-empty `Uint8Array` throws a runtime `TypeError: Cannot freeze array buffer views with elements`.
   - Mandates that `entry.canonicalWireBytes` is managed as private backing storage, with all public accessors, receipts, and export batches returning detached clones (`new Uint8Array(entry.canonicalWireBytes)`).
3. **R17-C — Architecture Invariant Preservation:**
   - Fully preserves 100% of established R16 contracts: Option A candidate-root linearization point (`this._core = candidateCore;`), Option A reservation consumption in `TerminalArchiveCoreState`, lookup-first commit idempotency, Option A atomic source retirement in `QuarantineBuffer`, M4.3c settlement barrier, one-in-flight drainer rule ($N_{\text{in-flight}} \le 1$), dual zero-custody verification, composite operation identity (`sequenceNumber`, `originPeerId`, `membershipEpoch`, `logicalEventId`), length-prefixed UTF-8 key encoding (`opid:...`), and generation-independent export lease governance.

---

## 2. Technical Resolutions & Specification Amendments

### 2.1 R17-A — Correct Runtime Map Ownership

#### The ECMAScript Runtime Reality of `Object.freeze(map)`
In JavaScript engines (V8, JavaScriptCore, SpiderMonkey), `Map` storage is held in private internal slots (`[[MapData]]`). Calling `Object.freeze(map)` prevents adding or deleting properties on the wrapper object, but:
```typescript
const map = new Map<string, number>();
Object.freeze(map);

// The internal collection remains completely mutable:
map.set('key', 123); // SUCCEEDS: [[MapData]] is updated!
map.delete('key');   // SUCCEEDS: [[MapData]] entry removed!
map.clear();          // SUCCEEDS: [[MapData]] cleared!
```
Therefore, claiming that `Object.freeze(map)` prevents entries from being mutated at runtime is factually false.

Furthermore, TypeScript compile-time types (`readonly`, `ReadonlyMap<K, V>`) are erased during compilation to JavaScript. They provide zero runtime immutability against internal aliasing or external caller mutations.

#### The Eight Rules of Map Ownership & Encapsulation
To achieve bulletproof immutability and isolation at runtime without relying on invalid engine assumptions, `TerminalQuarantineArchive` and `QuarantineBuffer` enforce the Eight Rules of Map Ownership & Encapsulation:

1. **Private Encapsulation Rule:**
   Backing `Map` instances (`committedTransactions`, `activeReservations`, `identityIndex`) are strictly private implementation details. They are stored within the core state root and are NEVER directly returned to external callers or public consumers.
2. **Authoritative Root Exclusive Ownership:**
   A published root object exclusively owns its `Map` instances. No external reference to these maps exists anywhere outside `this._core`.
3. **Zero Direct Map Leakage:**
   No public method, getter, callback, receipt, or export batch ever returns or leaks a backing `Map` instance or iterator.
4. **Lexical Builder Isolation:**
   In every state transition, candidate maps are allocated fresh in local lexical scope (`const nextMap = new Map(...)`). Once the atomic pointer swap occurs, builder variables go out of scope and cannot be retained, exported, or mutated.
5. **No Post-Publication Mutation:**
   No method, continuation, or callback ever calls `.set()`, `.delete()`, or `.clear()` on a `Map` belonging to an already-published authoritative root.
6. **Copy-on-Write (CoW) State Transitions:**
   Every state modification (reservation allocation, cancellation, commit, export) constructs a fresh `Map` and a fresh candidate root object, publishing via a single reference assignment.
7. **Failure Isolation:**
   If candidate construction, allocation, or validation throws, the authoritative published root remains 100% unaltered.
8. **Safe Detached Public Read Operations:**
   Public read queries return safe representations:
   - `archive.getTransaction(id)` returns a shallow-frozen record copy: `existingRecord ? Object.freeze({ ...existingRecord }) : undefined`.
   - `archive.hasTransaction(id)` returns a primitive boolean.
   - `archive.getEntries()` returns a shallow-frozen detached Array copy: `Object.freeze([...this._core.entries])`.

#### Specification Core State Definitions
```typescript
/**
 * Authoritative immutable candidate-root state for TerminalQuarantineArchive.
 * Outer root object and entries array are shallow-frozen with Object.freeze().
 * Backing Map instances (committedTransactions, activeReservations) are strictly
 * private encapsulated structures owned exclusively by the root: they are never
 * mutated post-publication, never exposed directly to external callers, and
 * cannot be made immutable via Object.freeze(map) in JavaScript (R17-A).
 */
export interface TerminalArchiveCoreState {
  readonly entries: ReadonlyArray<TerminalQuarantineEntry>;
  readonly committedTransactions: ReadonlyMap<string, QuarantineRetirementTransactionRecord>;
  readonly activeReservations: ReadonlyMap<string, ArchiveReservationRecord>;
  readonly totalBytes: number;
  readonly size: number;
  readonly archiveGeneration: number;
}

/**
 * Authoritative candidate-root state for QuarantineBuffer under Option A.
 * Outer root object and queues are shallow-frozen with Object.freeze().
 * identityIndex Map is privately encapsulated and never mutated post-publication (R17-A).
 */
export interface QuarantineBufferCoreState {
  readonly activeQueue: ReadonlyArray<QuarantinedOperation>;
  readonly drainBatch: ReadonlyArray<QuarantinedOperation>;
  readonly identityIndex: ReadonlyMap<string, number>;
  readonly activeBytes: number;
  readonly drainBytes: number;
  readonly generationId: number;
  readonly drainGenerationId: number | null;
}
```

---

### 2.2 R17-B — Nested Records & Canonical Bytes Protection

#### Shallow Freeze Reality & Nested Object Freezing
`Object.freeze()` is strictly shallow. When freezing `TerminalArchiveCoreState` or `QuarantineRetirementTransactionRecord`, nested plain objects must be independently frozen before inclusion in candidate roots:
```typescript
const txRecord: QuarantineRetirementTransactionRecord = Object.freeze({
  transactionId: retirementTransactionId,
  outcome: 'COMMITTED',
  entryCount: stagedBatch.length,
  totalSerializedBytes: batchBytes,
  batchDigest,
  committedAtMs: Date.now(),
});
nextTransactions.set(retirementTransactionId, txRecord);
```

#### TypedArray Non-Freezability
In V8 and conformant ECMAScript environments, calling `Object.freeze()` on a non-empty `Uint8Array` throws:
```
TypeError: Cannot freeze array buffer views with elements
```
Attempting to freeze typed arrays is an error.
Therefore, `canonicalWireBytes: Uint8Array` is held as **private backing storage**. Immutability is enforced through defensive cloning:
1. When constructing `TerminalQuarantineEntry`, canonical bytes are validated and held privately.
2. The public accessor or receipt returns a detached copy:
   ```typescript
   get canonicalWireBytes(): Uint8Array {
     return new Uint8Array(this._entry.canonicalWireBytes);
   }
   ```
3. Digest calculation, measured wire bytes, and committed archive representation are bound strictly to this private canonical byte representation. External modification of the returned clone has zero effect on the archive's internal state.

---

### 2.3 R17-C — Architecture Invariant Preservation

Milestone M5.0-R17 preserves without modification all established R16 contracts:

1. **Candidate-Root Linearization Point (Option A):**
   ```typescript
   // Atomic publication linearization point:
   this._core = candidateCore;
   ```
   Entry publication, transaction recording, and reservation consumption execute simultaneously at this single synchronous assignment.
2. **Lookup-First Commit Idempotency:**
   Before staging or reservation checks, `this._core.committedTransactions.get(transactionId)` is evaluated. If present and the batch digest matches, the existing receipt is returned immediately with `alreadyCommitted: true`.
3. **QuarantineBuffer Option A Source Retirement:**
   `retireCommittedBatch()` in `QuarantineBuffer` constructs a candidate root with dequeued operations removed, byte counts subtracted, and identity index entries cleared. The swap is failure-atomic and leaves the buffer unaltered if validation fails.
4. **Step 6 Settlement Barrier Pre-Condition:**
   Retirement stages begin strictly after the settlement barrier has settled, confirming $N_{\text{in-flight}} == 0$ (`quarantineDrainer.inFlightRecord === null`) and `quarantineDrainer.isDraining === false`.
5. **Dual Zero-Custody Assertion:**
   Following Stage 11 source retirement, the coordinator asserts:
   ```typescript
   if (this.quarantineBuffer.totalRetainedCount !== 0 || this.quarantineDrainer.inFlightRecord !== null) {
     throw new SyncEngineError('CUSTODY_LEAK_DETECTED', 'Zero-custody verification failed after retirement');
   }
   ```
6. **Portable Composite Operation Identity:**
   - Identity tuple: `originPeerId`, `sequenceNumber`, `membershipEpoch`, `logicalEventId`.
   - Length-prefixed UTF-8 canonical key: `opid:${peerIdBytes}:${originPeerId}/${sequenceNumber}/${membershipEpoch}/${eventIdBytes}:${logicalEventId}`.
   - Full 4-field comparison executed upon index hit.
7. **Generation-Independent Export Lease Governance:**
   At most one active export lease (`MAX_OUTSTANDING_EXPORTS = 1`), with generation-independent acknowledgement matching snapshot lease token.

---

## 3. Failure Recovery Matrix (R17 Comprehensive)

| Failure Scenario | Engine State | QuarantineBuffer Custody | TerminalQuarantineArchive Custody | Coordinator Transaction State | Recovery Action |
|:---|:---|:---|:---|:---|:---|
| **1. Reservation capacity insufficient ($> 500$ entries or $> 2$ MiB)** | Untouched | Retains operations ($N$) | Rejects reservation | `FAILED` (`CAPACITY_INSUFFICIENT`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); zero loss |
| **2. Oversized entry ($> 4096$ canonical bytes)** | Untouched | Retains operations ($N$) | Rejects batch staging | `FAILED` (`ENTRY_SIZE_EXCEEDED`) | Aborts clean reset; enters `DEGRADED` (`RESET_PREREQUISITE_FAILED`); entry never truncated |
| **3. Generation mismatch at reservation commit** | Untouched | Retains operations ($N$) | Rejects commit | `FAILED` (`GENERATION_MISMATCH`) | Cancels reservation; re-reads generation; re-stages batch |
| **4. Exception during candidate root construction** | Untouched | Retains operations ($N$) | Old root remains authoritative; zero entries added; reservation unconsumed; backing Maps intact | `PREPARING` / `FAILED` | Old archive root intact; reservation preserved for retry or cancellation |
| **5. Archive commit succeeds but source retirement throws (Option A validation failure)** | Untouched | Retains operations ($N$) | Batch committed under `retirementTxId`; reservation consumed | `COMMITTED_SOURCE_RETIREMENT_PENDING` | Source buffer untouched; coordinator queries `getTransaction()`; skips archive commit; retries Stage 11 source retirement |
| **6. Duplicate transaction submission** | Untouched | Intact | Returns existing receipt via Step 1 lookup | `COMPLETED` | Idempotent; zero duplicate entries; returns success receipt |
| **7. Transaction ID reused with different digest** | Untouched | Retains operations ($N$) | Rejects with conflict | `FAILED` (`TRANSACTION_ID_DIGEST_MISMATCH`) | Throws error; preserves archive and buffer; prevents corruption |
| **8. Uncertain commit outcome (timeout/exception)** | Untouched | Retains operations ($N$) | Query via `getTransaction()` | `COMMIT_OUTCOME_UNKNOWN` | 3-way query: if committed $\to$ retires source; if absent $\to$ cancels reservation; if fails $\to$ remains unknown |
| **9. Attempt invalidation during preparation** | Untouched | Retains operations ($N$) | Reservation cancelled | `FAILED` (`ATTEMPT_FENCED`) | Stale continuation aborts immediately; zero mutations |
| **10. Conflicting composite identity in batch** | Untouched | Retains operations ($N$) | Untouched | `FAILED` (`RETIREMENT_BATCH_CONFLICT`) | Rejects batch fail-closed; preserves buffer; halts reset |
| **11. Settlement watchdog timeout during Step 4** | Untouched | Retains operations ($N$) | Untouched | `TIMED_OUT` | Attempt latched to `TIMED_OUT`; clean reset aborts to `DEGRADED`; zero corruption |
| **12. Dual zero-custody verification failure** | Untouched | Detected leak | Committed | `FAILED` (`CUSTODY_LEAK_DETECTED`) | Aborts clean-root install; enters `DEGRADED`; prevents uncommitted operation loss |

---

## 4. Source Code Audit Grounding

| Subsystem Component | File Path | Line Range | Existing Behavior in Accepted Source | Proposed M5 Invariant |
|:---|:---|:---|:---|:---|
| **In-Flight Record Storage** | `packages/sync-engine/src/recovery/quarantine-drainer.ts` | 119, 292–305 | `private _inFlightRecord: DrainedInFlightRecord \| null = null;`. Sets record on dequeue. | In-flight operation owned exclusively by drainer ($N_{\text{in-flight}} = 1$); not present in buffer. |
| **Dequeuing from QuarantineBuffer** | `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 243–257 | `shiftDrain()` removes operation from `drainBatch`, decrements `_drainBytes`, deletes from `identityIndex`. | Physical removal from buffer during drain pass. |
| **Restoration to QuarantineBuffer** | `packages/sync-engine/src/recovery/quarantine-drainer.ts` | 381–392 | `restoreInFlightToQueue()` calls `quarantineBuffer.unshiftDrain(op)` and clears `_inFlightRecord`. | Restores physical ownership to buffer upon capacity rejection or attempt invalidation. |
| **QuarantinedOperation Envelope** | `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 43–50 | Defines `originPeerId`, `sequenceNumber`, `membershipEpoch`, `serializedByteLength`, `receivedAtMs`. | Schema property `sequenceNumber` preserved directly without renaming. |
| **Quarantine Buffer Custody Index** | `packages/sync-engine/src/recovery/quarantine-buffer.ts` | 167–172 | Indexes retained operations via `${op.originPeerId}:${op.sequenceNumber}` in `this.identityIndex`. | Backing Map is private implementation detail; Option A source retirement validates identities against this index. |
| **Admit Drained Operation Identity** | `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | 982–995 | Extracts `originPeerId`, `originSeq = op.sequenceNumber`, `epoch`, and `logicalEventId = op.event.message.logicalEventId`. | Composite identity tuple matches exact source property names without semantic drift. |
| **Identifier Character Regex** | `packages/protocol/src/constants.ts` | 87 | `ASCII_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.:-]+$/`. | Character domain strictly bounded to printable ASCII; UTF-8 byte length equals ASCII length. |
| **Recovery Transition Graph** | `packages/sync-engine/src/recovery/recovery-state-machine.ts` | 49–54 | Defines legal transitions: `DEGRADED: new Set(['RECOVERING'])`. | Reset operations follow authoritative path `DEGRADED -> RECOVERING -> ACTIVE`. |

---

## 5. Explicit Unresolved Limitations

1. **In-Memory Volatility:** `TerminalQuarantineArchive` is an in-memory ring buffer. Process termination destroys in-memory entries; external durability is deferred to administrative export consumers.
2. **V8 Heap Expansion vs. Wire Bounds:** Serialized byte bounds (2 MiB aggregate wire bytes, 4 KiB per entry) govern transport data. V8 heap allocations for JavaScript object wrappers, Maps, and closures exceed serialized wire size by approximately $3.5\times$ to $5\times$.
3. **Single Active Export Lease:** Only one export lease can be active at a time (`MAX_OUTSTANDING_EXPORTS = 1`). Concurrent administrative export calls are rejected with `ARCHIVE_EXPORT_IN_PROGRESS`.
4. **Non-Cancelable In-Flight Promises:** When `SETTLEMENT_BARRIER_TIMEOUT_MS = 5000` expires, the reset attempt is latched to `TIMED_OUT`, but the underlying in-flight Promise runs to settlement before the slot can be reclaimed.

---

## 6. Repository Governance & Final Evidence

### 6.1 Baseline Verification
- **Branch:** `main`
- **Starting Baseline Commit (M5.0-R16):** `a6b34fd15340fbabb558f4cdc32a02854b73df2f`
- **Direct Parent Commit (M5.0-R15):** `32203a3df4c70b379fc6cb4ba0dbc335de8858b6`

### 6.2 Scope Confirmation
- Strictly documentation-only correction round.
- **Zero** production TypeScript/JavaScript files modified (`packages/*/src/**/*.ts`).
- **Zero** test files modified (`packages/**/__tests__/**`).
- **Zero** package manifests or lockfiles modified (`package.json`, `package-lock.json`).
- **Zero** build configurations modified (`tsconfig*.json`).
- Strictly the six authorized documentation files modified:
  1. `docs/PHASE7_5_M5_ARCHITECTURE.md`
  2. `docs/PHASE7_5_M5_INTERFACE_SPECIFICATION.md`
  3. `docs/PHASE7_5_M5_IMPLEMENTATION_ROADMAP.md`
  4. `docs/PHASE7_5_M5_ACCEPTANCE_CRITERIA.md`
  5. `docs/PHASE7_5_M5_RISK_REGISTER.md`
  6. `docs/PHASE7_5_M5_0_R17_CORRECTION_REPORT.md`

---

## 7. Final Decision Boundary

```
================================================================================
FINAL MILESTONE M5.0-R17 DECISION:
READY FOR FINAL M5.0 ACCEPTANCE REVIEW
================================================================================
```

All review findings from R16 and R17 (R17-A through R17-C) have been rigorously resolved and reconciled with accepted source code. The M5 architecture specifications are complete, consistent, and ready for final engineering review.

**Implementation Authorization Notice:**
Milestone M5.1 implementation remains **STRICTLY UNAUTHORIZED AND UNSTARTED** unless separately authorized by the Engineering Authority following formal acceptance of Milestone M5.0.
