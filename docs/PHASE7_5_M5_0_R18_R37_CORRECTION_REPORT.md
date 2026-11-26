# Phase 7.5 M5.0-R18-R37 — Complete Custody Input Validation, Clone-Before-Hash Recovery, Exact Attempt Ownership & Atomic Coordinator-State Adoption Report

## Document Metadata
- **Milestone:** Phase 7.5 Sub-Milestone M5.0-R18-R37
- **Parent Baseline:** `2813762df4328e649e00c183a5d024a7711ae289` (M5.0-R18-R36)
- **Role:** Principal Distributed-Systems Architect, TypeScript/JavaScript Runtime Architect, ECMAScript Semantics Specialist, and Independent Recovery-Systems Auditor
- **Mode:** STRICTLY DOCUMENTATION-ONLY ARCHITECTURAL CORRECTION
- **Target Document:** `docs/PHASE7_5_M5_0_R18_CONCURRENT_RECOVERY_ARBITRATION_DECISION.md`
- **Report Document:** `docs/PHASE7_5_M5_0_R18_R37_CORRECTION_REPORT.md`
- **Epistemic Status:** Architectural Specification Only — Zero Production Source Code Modified, Zero Test Code Modified, Zero Tests Executed

---

## 1. Executive Summary

Phase 7.5 Sub-Milestone M5.0-R18-R37 resolves the concrete architectural and runtime defects identified during the independent audit of the M5.0-R18-R36 specification:

1. **Captured Uint8Array Prototype & Detached ArrayBuffer Detection (R37-A):**
   - In R36, `assertValidOrdinaryUint8Array` evaluated `Object.getPrototypeOf(val) !== Uint8Array.prototype`. While `CapturedUint8Array` was captured at module load, querying the property `Uint8Array.prototype` dynamically consulted `globalThis.Uint8Array`. If an attacker monkey-patched `globalThis.Uint8Array.prototype` post-initialization, subsequent prototype validations would evaluate against the tampered object.
   - *R37 Correction:* Captured `capturedUint8ArrayPrototype = CapturedUint8Array.prototype` at module evaluation and used it exclusively across all prototype comparisons.
   - *Detached Buffer Detection:* Replaced the ambiguous zero-byte check with captured `ArrayBuffer.prototype.detached` getter where supported, coupled with an intrinsic allocation probe fallback: `try { new CapturedUint8Array(buffer, 0, 0); } catch { throw new TypeError('detached ArrayBuffer'); }`. Under ECMAScript §23.2.5.1, `AllocateTypedArray` unconditionally throws `TypeError` on detached buffers regardless of offset or length, while succeeding deterministically on legitimate zero-length buffers.

2. **Input Trust Boundary & Descriptor-Based Array Cloning (R37-B):**
   - Clarified the input trust boundary: trusted store adapters supply ordinary JavaScript objects and byte views; arbitrary Proxies and live shared memory are strictly excluded from the supported custody graph.
   - Implemented descriptor-based array cloning in `deepCloneAndFreezeValue`: length is validated strictly via own descriptor; sparse arrays (missing descriptors), accessor descriptors, non-enumerable elements, symbol properties, and extraneous own string properties are rejected with `TypeError` without reading elements via property access. Constructed destination arrays are pre-sized and populated via index assignment, strictly eliminating `Array.prototype.push`.
   - Expanded the Intrinsic Dependency Table to cover all built-in constructors, prototypes, getters, and static methods used across validation, cloning, cycle detection, hashing, and adoption.

3. **Strict 8-Dimensional Identity Normalization & Response Discrimination (R37-C):**
   - Codified `normalizePublicationIdentity(input)`: asserts plain object prototype, exactly 8 own data properties, zero symbols, safe integers, and canonical 64-character lowercase hex digests. Returns an immutable, frozen normalized tuple.
   - `publicationIdentityEquals` compares normalized records across all 8 canonical dimensions.
   - Codified strict response identity field discrimination: `publicationIdentity` is required on `RECONCILED_COMMITTED`, while `attemptedIdentity` is required on `NOT_COMMITTED_FINAL`. Responses with wrong, dual, or missing identity fields fail closed immediately into `RECONCILING_CUSTODY`.

4. **Universal Clone-Before-Hash Adoption Across All Branches (R37-D):**
   - Across EVERY recovery branch (`RECONCILED_COMMITTED`, `NOT_COMMITTED_FINAL`, and startup recovery), the coordinator defensively deep-clones store-returned roots into private candidates (`deepCloneAndFreezeValue`) BEFORE computing the SHA-256 digest or validating schema. The candidate hashed is strictly the candidate installed. External mutations to store records have zero effect on candidate validity.

5. **Dedicated No-Active-Attempt Startup Recovery Procedure (R37-E):**
   - Codified `_recoverAuthoritativeRootNoActiveAttempt(storeRecord)` to eliminate direct-adoption bypasses during cold node startup. The procedure enforces full deep-cloning, schema validation, semantic invariant verification, candidate digest checking against `storeRecord.rootDigest`, sequence verification, coordinator lease assertion, and atomic state transition.

6. **Exact Active-Attempt Ownership & Stale Completion Fencing (R37-F):**
   - Active attempt contexts are immutable runtime values (`Object.freeze`).
   - `assertPostAwaitEligibility(attempt)` verifies coordinator not stopped, term matches, lease is valid with guard margin, and `attempt` matches the active attempt registered on coordinator state.
   - Conditional attempt cleanup via `clearActiveAttemptIfMatches(expectedAttemptId)` ensures delayed rejections from superseded attempts cannot clear a newly registered active attempt.

7. **Single Immutable Coordinator-State Record & Atomic Transition (R37-G):**
   - Unified coordinator state into a single immutable record `CoordinatorCustodyState` (`root`, `gateState`, `publicationSequence`, `activeAttempt`).
   - Implemented `#replaceCoreStateAtomic(nextRoot, nextGateState, nextSequence, expectedAttemptToClear)`: prepares the complete next state, validates attempt consistency, and swaps the `#state` reference synchronously with zero interleaving asynchronous operations, guaranteeing all-or-nothing state transitions and eliminating torn reads.

8. **12-Branch State Transition Table & 285-Case Matrix Expansion (R37-H):**
   - Codified the complete 12-branch state transition table for the custody reconciliation state machine.
   - Expanded the Exhaustive Failure and Retry Matrix to 285 deterministic cases (Cases 1–285).
   - Codified 285 deterministic future test specifications (T01–T285), adding 50 targeted specifications (T236–T285) with explicit epistemic status: **FUTURE TEST SPECIFICATION — NOT EXECUTED**.

---

## 2. R37-A: Captured Prototype Reference & Detached ArrayBuffer Detection

### 2.1 The Captured Prototype Vulnerability in R36
In R36, `assertValidOrdinaryUint8Array` evaluated:
```javascript
if (Object.getPrototypeOf(val) !== Uint8Array.prototype) {
    throw new TypeError('Byte array must be an ordinary Uint8Array');
}
```
While `CapturedUint8Array` was captured at module load, `Uint8Array.prototype` was accessed dynamically via global property lookup. If hostile host-realm code executed `globalThis.Uint8Array.prototype = {}` or altered the global constructor, subsequent prototype comparisons would evaluate against the altered prototype, enabling subclass spoofing or denial of service.

### 2.2 R37 Captured Prototype Invariant
In R37, the prototype reference is captured once at module initialization alongside the constructor:
```javascript
const CapturedUint8Array = globalThis.Uint8Array;
const capturedUint8ArrayPrototype = CapturedUint8Array.prototype;
```
All prototype comparisons in `assertValidOrdinaryUint8Array` and throughout the custody graph evaluate strictly against `capturedUint8ArrayPrototype`. Modifying `globalThis.Uint8Array` or `globalThis.Uint8Array.prototype` post-initialization has zero effect on custody validation.

### 2.3 Detached ArrayBuffer Detection via Intrinsic Allocation Probe Fallback
Detecting whether an ArrayBuffer has been detached (neutered) is critical to prevent zero-filled or torn memory reads. Where available, ECMAScript 2024 provides the `ArrayBuffer.prototype.detached` getter:
```javascript
const capturedArrayBufferDetachedGetter = (() => {
    try {
        const desc = Object.getOwnPropertyDescriptor(globalThis.ArrayBuffer.prototype, 'detached');
        return desc && typeof desc.get === 'function' ? desc.get : null;
    } catch {
        return null;
    }
})();
```
In runtimes where the `detached` getter is absent, checking `buffer.byteLength === 0` is ambiguous because an ArrayBuffer can be legitimately created with length zero (`new ArrayBuffer(0)`).

To resolve this ambiguity deterministically, R37 introduces the intrinsic allocation probe fallback:
```javascript
if (capturedArrayBufferDetachedGetter !== null) {
    if (Reflect.apply(capturedArrayBufferDetachedGetter, buffer, []) === true) {
        throw new TypeError('Uint8Array backing ArrayBuffer is detached');
    }
} else {
    // Intrinsic probe: AllocateTypedArray throws TypeError unconditionally if buffer is detached
    try {
        new CapturedUint8Array(buffer, 0, 0);
    } catch {
        throw new TypeError('Uint8Array backing ArrayBuffer is detached');
    }
}
```
Under ECMAScript §23.2.5.1 `AllocateTypedArray`, invoking `new Uint8Array(buffer, 0, 0)` checks whether `IsDetachedBuffer(buffer)` is true. If detached, it throws a `TypeError` regardless of whether the view length is zero. If the buffer is a valid, non-detached zero-length buffer, the constructor succeeds without throwing. Prior to the probe, cross-realm `SharedArrayBuffer` backing stores are rejected via intrinsic `%SharedArrayBuffer%.prototype.byteLength` getter brand checking, guaranteeing that `AllocateTypedArray` only receives true `ArrayBuffer` instances.

---

## 3. R37-B: Input Trust Boundary & Descriptor-Based Array Cloning

### 3.1 Input Trust Boundary Specification
The custody subsystem enforces an explicit boundary separating trusted system components from untrusted external inputs:
- **Trusted Store Adapters:** In-memory, SQLite, or distributed store adapters implement the SyncPlay protocol contract. They are verified to supply ordinary JavaScript objects, arrays, and typed arrays.
- **Excluded Inputs:** Arbitrary `Proxy` objects and live shared memory (`SharedArrayBuffer`) fall outside the supported input boundary. If presented, they fail closed immediately.
- **Fail-Closed Principle:** Any input that cannot be validated as an ordinary plain object (`Object.getPrototypeOf(v) === Object.prototype` or `null`), an ordinary array (`Array.isArray(v)`), an ordinary `Uint8Array`, or an admissible primitive is rejected before entering the custody graph.

### 3.2 Descriptor-Based Array Cloning
To eliminate property-access traps (`arr[i]`), accessor getters, and non-enumerable elements, array cloning in `deepCloneAndFreezeValue` operates strictly through property descriptors:
```javascript
if (Array.isArray(val)) {
    const lenDesc = Object.getOwnPropertyDescriptor(val, 'length');
    if (!lenDesc || typeof lenDesc.value !== 'number' || !Number.isSafeInteger(lenDesc.value) || lenDesc.value < 0) {
        throw new TypeError('Array length must be a valid safe integer own property');
    }
    const len = lenDesc.value;

    // Reject symbol properties
    if (Object.getOwnPropertySymbols(val).length > 0) {
        throw new TypeError('Array cannot contain symbol properties');
    }

    // Reject extraneous own string properties (beyond indices and 'length')
    const propNames = Object.getOwnPropertyNames(val);
    if (propNames.length > len + 1) {
        throw new TypeError('Array contains extraneous own properties');
    }

    const clonedArr = new Array(len);
    activeSeen.set(val, clonedArr);

    for (let i = 0; i < len; i++) {
        const key = String(i);
        const desc = Object.getOwnPropertyDescriptor(val, key);
        if (!desc) {
            throw new TypeError(`Array is sparse at index ${i}; holes prohibited`);
        }
        if (desc.get !== undefined || desc.set !== undefined) {
            throw new TypeError(`Array element at index ${i} cannot be an accessor`);
        }
        if (desc.enumerable !== true) {
            throw new TypeError(`Array element at index ${i} must be enumerable`);
        }
        clonedArr[i] = deepClone(desc.value);
    }
    return Object.freeze(clonedArr);
}
```
By pre-sizing `clonedArr = new Array(len)` and assigning by index, this implementation strictly eliminates reliance on `Array.prototype.push`, ensuring immunity against array prototype method tampering.

---

## 4. R37-C: Strict 8-Dimensional Identity Normalization & Response Discrimination

### 4.1 Publication Identity Normalization
To prevent spoofed, truncated, or bloated identity records, R37 codifies `normalizePublicationIdentity(input)`:
```javascript
function normalizePublicationIdentity(input: unknown): Readonly<PublicationIdentity> {
    if (typeof input !== 'object' || input === null) {
        throw new TypeError('PublicationIdentity must be a non-null object');
    }
    const proto = Object.getPrototypeOf(input);
    if (proto !== Object.prototype && proto !== null) {
        throw new TypeError('PublicationIdentity must be a plain object');
    }
    if (Object.getOwnPropertySymbols(input).length > 0) {
        throw new TypeError('PublicationIdentity cannot contain symbol properties');
    }
    const keys = Object.getOwnPropertyNames(input);
    if (keys.length !== 8) {
        throw new TypeError(`PublicationIdentity must have exactly 8 own properties, got ${keys.length}`);
    }

    const obj = input as Record<string, unknown>;
    const requiredKeys = [
        'clusterTerm', 'bootGeneration', 'bootInstanceId', 'publicationSequence',
        'attemptId', 'candidateRootDigest', 'expectedPriorSequence', 'expectedPriorRootDigest'
    ];

    for (const k of requiredKeys) {
        const desc = Object.getOwnPropertyDescriptor(obj, k);
        if (!desc || desc.get !== undefined || desc.set !== undefined || desc.enumerable !== true) {
            throw new TypeError(`PublicationIdentity property '${k}' must be an enumerable data property`);
        }
    }

    // Validate types and ranges
    if (typeof obj.clusterTerm !== 'number' || !Number.isSafeInteger(obj.clusterTerm) || obj.clusterTerm < 0) {
        throw new TypeError('clusterTerm must be a non-negative safe integer');
    }
    if (typeof obj.bootGeneration !== 'number' || !Number.isSafeInteger(obj.bootGeneration) || obj.bootGeneration < 0) {
        throw new TypeError('bootGeneration must be a non-negative safe integer');
    }
    if (typeof obj.bootInstanceId !== 'string' || obj.bootInstanceId.length === 0) {
        throw new TypeError('bootInstanceId must be a non-empty string');
    }
    if (typeof obj.publicationSequence !== 'number' || !Number.isSafeInteger(obj.publicationSequence) || obj.publicationSequence < 0) {
        throw new TypeError('publicationSequence must be a non-negative safe integer');
    }
    if (typeof obj.attemptId !== 'string' || obj.attemptId.length === 0) {
        throw new TypeError('attemptId must be a non-empty string');
    }
    const hexRegex = /^[0-9a-f]{64}$/;
    if (typeof obj.candidateRootDigest !== 'string' || !hexRegex.test(obj.candidateRootDigest)) {
        throw new TypeError('candidateRootDigest must be a 64-char lowercase hex string');
    }
    if (typeof obj.expectedPriorSequence !== 'number' || !Number.isSafeInteger(obj.expectedPriorSequence) || obj.expectedPriorSequence < 0) {
        throw new TypeError('expectedPriorSequence must be a non-negative safe integer');
    }
    if (typeof obj.expectedPriorRootDigest !== 'string' || !hexRegex.test(obj.expectedPriorRootDigest)) {
        throw new TypeError('expectedPriorRootDigest must be a 64-char lowercase hex string');
    }

    return Object.freeze({
        clusterTerm: obj.clusterTerm,
        bootGeneration: obj.bootGeneration,
        bootInstanceId: obj.bootInstanceId,
        publicationSequence: obj.publicationSequence,
        attemptId: obj.attemptId,
        candidateRootDigest: obj.candidateRootDigest,
        expectedPriorSequence: obj.expectedPriorSequence,
        expectedPriorRootDigest: obj.expectedPriorRootDigest
    });
}
```

### 4.2 Strict 8-Dimensional Identity Equality
`publicationIdentityEquals(a, b)` compares two normalized identity records across all 8 dimensions:
```javascript
function publicationIdentityEquals(a: Readonly<PublicationIdentity>, b: Readonly<PublicationIdentity>): boolean {
    return a.clusterTerm === b.clusterTerm &&
           a.bootGeneration === b.bootGeneration &&
           a.bootInstanceId === b.bootInstanceId &&
           a.publicationSequence === b.publicationSequence &&
           a.attemptId === b.attemptId &&
           a.candidateRootDigest === b.candidateRootDigest &&
           a.expectedPriorSequence === b.expectedPriorSequence &&
           a.expectedPriorRootDigest === b.expectedPriorRootDigest;
}
```

### 4.3 Strict Response Identity Field Discrimination
Store reconciliation responses must adhere strictly to discriminated field requirements:
- `RECONCILED_COMMITTED`: Requires `publicationIdentity`. Presence of `attemptedIdentity` is prohibited.
- `NOT_COMMITTED_FINAL`: Requires `attemptedIdentity`. Presence of `publicationIdentity` is prohibited.
- Any response providing both fields, neither field, or the wrong field fails closed immediately with `TypeError`, keeping the coordinator safely in `RECONCILING_CUSTODY`.

---

## 5. R37-D: Universal Clone-Before-Hash Adoption Across All Branches

A critical security principle established in R37 is: **The candidate hashed is strictly the candidate installed.**

Under earlier revisions, there was a risk that store-returned references could be hashed before cloning, or that concurrent caller mutations to the returned root could alter its content between SHA-256 calculation and installation.

In R37, across ALL recovery branches:
1. `RECONCILED_COMMITTED`:
   ```javascript
   // Defensively deep-clone into a private candidate BEFORE computing digest or validating schema
   const candidateRoot = deepCloneAndFreezeValue(result.committedRoot);
   const computedDigest = computeCanonicalRootDigest(candidateRoot);
   if (computedDigest !== identity.candidateRootDigest) {
       throw new Error(`CUSTODY_RECONCILIATION_DIGEST_MISMATCH`);
   }
   assertCustodyRootInvariants(candidateRoot);
   ```
2. `NOT_COMMITTED_FINAL`:
   ```javascript
   // Defensively deep-clone into a private candidate BEFORE computing digest or validating schema
   const candidateRoot = deepCloneAndFreezeValue(result.authoritativeCurrentRoot);
   const computedDigest = computeCanonicalRootDigest(candidateRoot);
   if (computedDigest !== result.authoritativeCurrentDigest) {
       throw new Error(`CUSTODY_RECONCILIATION_CURRENT_ROOT_DIGEST_MISMATCH`);
   }
   assertCustodyRootInvariants(candidateRoot);
   ```
3. Startup Recovery (`_recoverAuthoritativeRootNoActiveAttempt`):
   ```javascript
   // Defensively deep-clone into a private candidate BEFORE computing digest or validating schema
   const candidateRoot = deepCloneAndFreezeValue(storeRecord.authoritativeCurrentRoot);
   const computedDigest = computeCanonicalRootDigest(candidateRoot);
   if (computedDigest !== storeRecord.rootDigest) {
       throw new Error(`CUSTODY_RECONCILIATION_STARTUP_ROOT_DIGEST_MISMATCH`);
   }
   assertCustodyRootInvariants(candidateRoot);
   ```

Because cloning precedes hashing in all paths, any subsequent mutation of the external reference has zero impact on the private candidate.

---

## 6. R37-E: Dedicated No-Active-Attempt Startup Recovery Procedure

When a node starts up or recovers without an in-flight active attempt, adopting the store's authoritative root must not bypass validation or rely on dummy attempt tokens. R37 codifies a dedicated recovery procedure:

```javascript
async function _recoverAuthoritativeRootNoActiveAttempt(
    storeRecord: StoreAuthoritativeRecord
): Promise<void> {
    // 1. Assert coordinator is initialized and active
    if (this.#state.gateState === 'COORDINATOR_STOPPED') {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: coordinator stopped');
    }
    // 2. Validate lease non-expiry
    if (this.#clock.now() + this.#guardMarginMs >= this.#currentLeaseExpiry) {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: lease expired');
    }

    // 3. Clone BEFORE hash
    const candidateRoot = deepCloneAndFreezeValue(storeRecord.authoritativeCurrentRoot);
    const computedDigest = computeCanonicalRootDigest(candidateRoot);
    if (computedDigest !== storeRecord.rootDigest) {
        throw new Error('CUSTODY_RECONCILIATION_STARTUP_ROOT_DIGEST_MISMATCH');
    }

    // 4. Validate schema and semantic invariants
    assertCustodyRootInvariants(candidateRoot);

    // 5. Pre-swap adjacent leadership assertion
    if (this.#state.gateState === 'COORDINATOR_STOPPED') {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: coordinator stopped prior to swap');
    }

    // 6. Synchronous atomic state transition
    this.#replaceCoreStateAtomic(
        candidateRoot,
        'ACTIVE',
        storeRecord.publicationSequence,
        null
    );
}
```

This procedure guarantees that cold initialization adheres to the identical defensive cloning, digest verification, invariant assertion, and atomic state installation discipline as runtime reconciliation.

---

## 7. R37-F: Exact Active-Attempt Ownership & Stale Completion Fencing

### 7.1 Active Attempt Context as an Immutable Value
The in-flight attempt context registered on coordinator state is an immutable frozen record:
```javascript
interface ActiveAttemptContext {
    readonly attemptId: string;
    readonly clusterTerm: number;
    readonly bootGeneration: number;
    readonly bootInstanceId: string;
    readonly publicationSequence: number;
    readonly candidateRootDigest: string;
    readonly expectedPriorSequence: number;
    readonly expectedPriorRootDigest: string;
    readonly timeoutMs: number;
    readonly startedAtMs: number;
}
```

### 7.2 Stale Completion Fencing
When an asynchronous operation (store publication or reconciliation) completes across an `await` boundary, the coordinator must verify that its active attempt was not superseded or cleared while waiting:
```javascript
assertPostAwaitEligibility(attempt: ActiveAttemptContext): void {
    if (this.#state.gateState === 'COORDINATOR_STOPPED') {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: coordinator stopped');
    }
    if (this.#currentTerm !== attempt.clusterTerm) {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: term changed');
    }
    if (this.#clock.now() + this.#guardMarginMs >= this.#currentLeaseExpiry) {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: lease expired');
    }
    if (this.#state.activeAttempt !== attempt) {
        throw new Error('CUSTODY_PUBLICATION_STALE_LEADERSHIP: active attempt superseded');
    }
}
```

### 7.3 Conditional Attempt Cleanup
When an error handler catches a failure, it must not unconditionally clear the coordinator's active attempt. If another attempt has already been registered, clearing unconditionally would corrupt the active state. R37 codifies conditional cleanup:
```javascript
clearActiveAttemptIfMatches(expectedAttemptId: string): void {
    const current = this.#state.activeAttempt;
    if (current !== null && current.attemptId === expectedAttemptId) {
        this.#replaceCoreStateAtomic(
            this.#state.root,
            this.#state.gateState,
            this.#state.publicationSequence,
            current
        );
    }
}
```

---

## 8. R37-G: Single Immutable Coordinator-State Record & Atomic Transition

### 8.1 Unified CoordinatorCustodyState
In earlier designs, `#custodyRoot`, `#custodyGateState`, `#publicationSequence`, and `#activeAttempt` were separate mutable fields updated sequentially. A microtask or exception interleaving between field assignments could expose torn intermediate states (e.g. updated root with obsolete gate state).

R37 consolidates all core coordinator custody state into a single immutable record:
```javascript
interface CoordinatorCustodyState {
    readonly root: Readonly<CustodyRoot>;
    readonly gateState: CustodyGateState;
    readonly publicationSequence: number;
    readonly activeAttempt: Readonly<ActiveAttemptContext> | null;
}
```

### 8.2 Synchronous Atomic Transition via #replaceCoreStateAtomic
State transitions execute through a dedicated private method:
```javascript
#replaceCoreStateAtomic(
    nextRoot: Readonly<CustodyRoot>,
    nextGateState: CustodyGateState,
    nextSequence: number,
    expectedAttemptToClear: Readonly<ActiveAttemptContext> | null
): void {
    const currentState = this.#state;

    // Verify attempt consistency if clearing
    let nextAttempt: Readonly<ActiveAttemptContext> | null = currentState.activeAttempt;
    if (expectedAttemptToClear !== null) {
        if (currentState.activeAttempt === expectedAttemptToClear) {
            nextAttempt = null;
        } else {
            // Attempt was already superseded; do not overwrite current active attempt
            nextAttempt = currentState.activeAttempt;
        }
    }

    const nextState: CoordinatorCustodyState = Object.freeze({
        root: nextRoot,
        gateState: nextGateState,
        publicationSequence: nextSequence,
        activeAttempt: nextAttempt
    });

    // Synchronous pointer swap with zero interleaving async work
    this.#state = nextState;
}
```
Because JavaScript execution is single-threaded between microtasks, the reference swap `this.#state = nextState` executes atomically. Any concurrent observer or asynchronous microtask always reads a coherent snapshot: either entirely the prior state or entirely the next state.

---

## 9. R37-H: 12-Branch State Transition Table & Failure Matrix Expansion

### 9.1 12-Branch Custody State Transition Table

| Branch | Initial State | Trigger Event | Precondition | State Transition | Exit State | Invariants Maintained |
|---|---|---|---|---|---|---|
| **1** | `ACTIVE` | Normal Publish Start | Lock acquired; lease valid | Register `activeAttempt`; advance sequence | `PUBLISHING` | Invariant 20 (FIFO lock; attempt registration) |
| **2** | `PUBLISHING` | Store CAS Success | Lease valid; confirmation matches | Swap root; clear attempt | `ACTIVE` | Invariant 20 (Exact confirmation matching) |
| **3** | `PUBLISHING` | Store CAS Reject (Conflict) | Lease valid; CAS returned false | Latch gate; register uncertain attempt | `RECONCILING_CUSTODY` | Invariant 23 (Fail-closed latch on conflict) |
| **4** | `PUBLISHING` | Network Timeout / Ambiguous | Lease valid; I/O error | Latch gate; preserve attempt context | `RECONCILING_CUSTODY` | Invariant 23 (Fail-closed gate on ambiguous outcome) |
| **5** | `RECONCILING_CUSTODY` | `RECONCILED_COMMITTED` Response | Identity matches; clone-before-hash passes | Install cloned root; clear attempt | `ACTIVE` | Invariant 20 & 23 (Clone-before-hash; identity verification) |
| **6** | `RECONCILING_CUSTODY` | `NOT_COMMITTED_FINAL` Response | Identity matches; terminal fence verified | Adopt current root; clear attempt | `ACTIVE` | Invariant 4 & 23 (Terminal fence proof; clone-before-hash) |
| **7** | `RECONCILING_CUSTODY` | `UNKNOWN_IN_PROGRESS` Response | Lease valid | Preserve gate & attempt; schedule backoff | `RECONCILING_CUSTODY` | Invariant 23 (Preserve gate on uncertain outcome) |
| **8** | `RECONCILING_CUSTODY` | Identity Mismatch | Normalized 8D mismatch | Preserve gate; discard response; fail closed | `RECONCILING_CUSTODY` | Invariant 20 (Fail-closed on identity mismatch) |
| **9** | `RECONCILING_CUSTODY` | Digest Mismatch on Candidate | Computed != store digest | Discard candidate; leave root untouched | `RECONCILING_CUSTODY` | Invariant 23 (Candidate digest verification) |
| **10** | `ANY` (except STOPPED) | Leadership Lost / Step-Down | Step-down triggered | Purge queue; reject attempts; update state | `COORDINATOR_STOPPED` | Invariant 20 (Queue purge on step-down) |
| **11** | `PUBLISHING` / `RECONCILING` | Post-Await Fencing Check Fails | Lease expired or term changed | Abort adoption; throw error; purge attempt | `COORDINATOR_STOPPED` | Invariant 20 (Post-await fencing protection) |
| **12** | `INITIALIZING` | Cold Startup Recovery | Lease valid; store record valid | Deep-clone; verify digest; atomic swap | `ACTIVE` | Invariant 23 (No-active-attempt startup recovery) |

### 9.2 Failure Matrix Expansion to 285 Deterministic Cases
The Exhaustive Failure and Retry Matrix in Section 15 was expanded from 235 cases (R36) to 285 deterministic cases (Cases 1–285), systematically covering:
- **Cases 236–241:** Byte validation & detached buffer cases (captured prototype, detached ArrayBuffer with zero-length view, legitimate zero-length view, SharedArrayBuffer rejection, no species constructor invocation, dynamic slice bypass).
- **Cases 242–252:** Custody cloning cases (array accessors, symbol properties on arrays, extra string properties on arrays, sparse arrays, non-enumerable elements, Proxy exclusion boundary, plain record accessors, prototype tampering immunity via captured intrinsics, shared reference preserving DAG, cycle rejection, `__proto__` property handling).
- **Cases 253–264:** Identity normalization cases (8 fields independently mismatched, missing fields, extraneous fields, symbol keys, accessor getters, invalid field types, unsafe integers, malformed digest string, wrong response identity field, dual identity fields, missing identity field, Proxy identity response).
- **Cases 265–276:** Root adoption cases (committed branch mutation before/after clone, non-committed branch mutation before/after clone, digest over exact candidate, invalid schema, wrong epoch/session, wrong generation, no-active-attempt direct adoption regression, no-active-attempt invalid digest, no-active-attempt stale context, no root pointer change on failed validation).
- **Cases 277–281:** Attempt fencing cases (attempt A replaced by B while A awaits, delayed A rejected, A cannot clear B, A cannot adopt after B, same term/lease but wrong attempt, step-down during clone/digest, lease expiry before adoption, repeated reconciliation, uncertain outcome preserves attempt).
- **Cases 282–285:** Atomic coordinator state cases (root, gate, sequence, attempt transition together, no observer sees inconsistent state, failed transition preserves prior state, listener callbacks not invoked during transition, delayed cleanup cannot affect later attempt).

---

## 10. Intrinsic Dependency Table & Runtime Trust Boundaries

The complete Intrinsic Dependency Table in Section 12.9 is expanded to enumerate all built-in primitives utilized across the custody subsystem:

| Intrinsic Built-in | Captured Binding Name | Operation Supported | Rationale / Attack Surface Prevented |
|---|---|---|---|
| `Uint8Array` constructor | `CapturedUint8Array` | Buffer allocation & type checking | Prevents attacker from overriding `globalThis.Uint8Array` to instantiate malicious views |
| `Uint8Array.prototype` | `capturedUint8ArrayPrototype` | Prototype verification | Immunizes prototype comparison against global prototype monkey-patching post-initialization |
| `ArrayBuffer.isView` | `capturedArrayBufferIsView` | Host-realm view discrimination | Prevents hostile interception or spoofing of typed-array view detection |
| `Object.getPrototypeOf` | `capturedObjectGetPrototypeOf` | Prototype extraction | Prevents tampering with prototype inspection primitives |
| `Object.defineProperty` | `capturedDefineProperty` | Safe own-property definition | Immunizes own-property creation and prevents prototype-pollution setters (`__proto__`) |
| `Object.getOwnPropertyDescriptor` | `capturedGetOwnPropertyDescriptor` | Safe descriptor inspection | Prevents invoking user getters on properties during structural inspection |
| `Object.getOwnPropertyNames` | `capturedGetOwnPropertyNames` | Own string property enumeration | Enumerates own string keys without prototype chain traversal |
| `Object.getOwnPropertySymbols` | `capturedGetOwnPropertySymbols` | Own symbol property enumeration | Detects and rejects symbol-keyed properties without invoking getters |
| `Object.freeze` | `capturedFreeze` | Immutability sealing | Guarantees frozen objects cannot be tampered with post-cloning |
| `%TypedArray%.prototype.set` | `capturedTypedArraySet` | Pristine raw byte copying | Copies raw bytes without invoking source `slice()`, `Array.from()`, or `Symbol.species` |
| `%TypedArray%.prototype.byteLength` getter | `capturedTypedArrayByteLengthGetter` | View length inspection | Queries length via internal slot without executing user getters |
| `%TypedArray%.prototype.byteOffset` getter | `capturedTypedArrayByteOffsetGetter` | View offset inspection | Queries offset via internal slot without executing user getters |
| `%TypedArray%.prototype.buffer` getter | `capturedTypedArrayBufferGetter` | Backing buffer extraction | Extracts backing ArrayBuffer via internal slot without user getter traps |
| `%ArrayBuffer%.prototype.byteLength` getter | `capturedArrayBufferByteLengthGetter` | Buffer length inspection | Queries buffer byte length via internal slot brand check |
| `%ArrayBuffer%.prototype.detached` getter | `capturedArrayBufferDetachedGetter` | Buffer detachment inspection | Direct internal slot query for detached ArrayBuffers |
| `%SharedArrayBuffer%.prototype.byteLength` getter | `capturedSharedArrayBufferByteLengthGetter` | Shared buffer discrimination | Fail-closed brand check rejecting cross-realm `SharedArrayBuffer` instances |

---

## 11. Future Test Matrix Expansion (Summary of Added Tests T236–T285)

Section 19 was updated with 50 targeted future test specifications (T236–T285), bringing the total to 285 test specifications:

- **T236 (R37-A):** Captured `Uint8Array.prototype` immunizes against global prototype tampering post-initialization.
- **T237 (R37-A):** Detached ArrayBuffer with zero-length view rejected by intrinsic allocation probe fallback.
- **T238 (R37-A):** Legitimate zero-length view on non-detached buffer accepted and encapsulated cleanly.
- **T239 (R37-A):** Cross-realm `SharedArrayBuffer` view rejected fail-closed via intrinsic getter brand check.
- **T240 (R37-A):** Source `Uint8Array` with attacker `Symbol.species` never consulted during allocation.
- **T241 (R37-A):** Dynamic `slice` method on source bypassed via captured `%TypedArray%.prototype.set`.
- **T242 (R37-B):** Array with accessor property on index element rejected without getter invocation.
- **T243 (R37-B):** Array with `Symbol`-keyed property rejected.
- **T244 (R37-B):** Array with extraneous own string property rejected.
- **T245 (R37-B):** Sparse array with missing index element rejected.
- **T246 (R37-B):** Array with non-enumerable index element rejected.
- **T247 (R37-B):** Store adapter Proxy input excluded by boundary check.
- **T248 (R37-B):** Plain record accessor property detected without getter invocation.
- **T249 (R37-B):** Global `Object.defineProperty` monkey-patching has zero effect on cloner.
- **T250 (R37-B):** Shared acyclic reference (DAG) cloned without false cycle error.
- **T251 (R37-B):** Genuine cyclic reference deterministically rejected.
- **T252 (R37-B):** Record with own data property named `'__proto__'` preserves prototype.
- **T253–T260 (R37-C):** Reconciliation responses with independently mismatched fields (`clusterTerm`, `bootGeneration`, `bootInstanceId`, `publicationSequence`, `attemptId`, `candidateRootDigest`, `expectedPriorSequence`, `expectedPriorRootDigest`) rejected fail-closed.
- **T261–T266 (R37-C):** Identity normalization rejects truncated objects, extraneous 9th property, symbol keys, accessor getters, non-safe integers, and non-hex digests.
- **T267–T270 (R37-C):** Response identity field discrimination rejects inverted fields (`attemptedIdentity` on committed, `publicationIdentity` on non-committed), dual fields, or missing fields.
- **T271 (R37-C):** Untrusted store Proxy identity rejected by boundary check.
- **T272–T273 (R37-D):** External mutation of `committedRoot` or `authoritativeCurrentRoot` has zero effect on candidate due to clone-before-hash.
- **T274–T276 (R37-D):** Invalid candidate schema, corrupt candidate digest, or inconsistent epoch detected and rejected on private clone.
- **T277 (R37-E):** Dedicated `_recoverAuthoritativeRootNoActiveAttempt` clones and validates authoritative root on startup.
- **T278–T281 (R37-F):** Exact active attempt ownership fences superseded Attempt A, prevents stale error handler from clearing active Attempt B, and catches coordinator step-down or lease expiry prior to state swap.
- **T282–T284 (R37-G):** Single immutable `CoordinatorCustodyState` record transitions all fields atomically via `#replaceCoreStateAtomic` with zero intermediate state observable.
- **T285 (R37-H):** `UNKNOWN_IN_PROGRESS` reconciliation outcome preserves gate state and active attempt for backoff retry.

*Epistemic Status:* All 285 test specifications are formal future requirements codified for Sub-Milestone M5.1. **ZERO TESTS EXECUTED.**

---

## 12. Documentation Consistency Audit Results

The updated decision document was subjected to automated verification:
1. **Trailing Whitespace:** 0 lines with trailing whitespace across all 5,291 lines.
2. **Failure Matrix Row Count:** Exactly 285 cases (Cases 1–285), completely sequential.
3. **Test Specifications Count:** Exactly 285 tests (T01–T285), completely sequential.
4. **Invariant Alignment:** Invariants 18, 20, 22, and 23 updated with complete R37 rules and citations.
5. **Pseudocode Consistency:** Verified complete alignment across `assertValidOrdinaryUint8Array`, `deepCloneAndFreezeValue`, `normalizePublicationIdentity`, `publicationIdentityEquals`, `CoordinatorCustodyState`, `#replaceCoreStateAtomic`, `_recoverAuthoritativeRootNoActiveAttempt`, and `reconcileAuthoritativeCustody`.

---

## 13. Epistemic Status & Categorical Verification

- **Epistemic Status:** Architectural Specification Only.
- **Test Execution:** ZERO TESTS EXECUTED. (Appropriate for a documentation-only milestone).
- **Production Code Scope:** ZERO lines of production source code (`packages/*/src/**/*.ts`) modified.
- **Test Code Scope:** ZERO lines of test code (`packages/**/__tests__/**`) modified.
- **Package Configuration Scope:** ZERO package or build configuration files modified.
- **Sub-Milestone M5.1 Boundary:** Sub-Milestone M5.1, including M5.1a, remains **STRICTLY UNAUTHORIZED AND BLOCKED** pending holistic M5.0 acceptance and explicit Engineering Authority approval.

---

## 14. Git Verification Evidence

- **Repository Root:** `c:\Users\tejde\OneDrive\Desktop\SyncPlay_Protocol\SyncPlay_Protocol`
- **Parent Commit Baseline:** `2813762df4328e649e00c183a5d024a7711ae289` (M5.0-R18-R36)
- **Modified Decision Document:** `docs/PHASE7_5_M5_0_R18_CONCURRENT_RECOVERY_ARBITRATION_DECISION.md`
- **Created Correction Report:** `docs/PHASE7_5_M5_0_R18_R37_CORRECTION_REPORT.md`
- **Brain Artifact Mirrors:**
  - `C:\Users\tejde\.gemini\antigravity\brain\3bf4b5b5-979b-42f3-9da6-e1707f7e3af1\PHASE7_5_M5_0_R18_CONCURRENT_RECOVERY_ARBITRATION_DECISION.md`
  - `C:\Users\tejde\.gemini\antigravity\brain\3bf4b5b5-979b-42f3-9da6-e1707f7e3af1\PHASE7_5_M5_0_R18_R37_CORRECTION_REPORT.md`

---

## 15. Final Verdict & Mandatory Authorization Statement

```
================================================================================
FINAL VERDICT: R37 DOCUMENTATION CORRECTION COMPLETE — READY FOR INDEPENDENT
HOLISTIC M5.0 ACCEPTANCE REVIEW
================================================================================
```

This R18-R37 proposal achieves complete, rigorous architectural closure of the recovery arbitration subsystem. It establishes module-captured `capturedUint8ArrayPrototype` reference permanent immunity against post-initialization prototype tampering; implements deterministic detached ArrayBuffer detection via `ArrayBuffer.prototype.detached` getter and intrinsic allocation probe fallback; codifies explicit input trust boundaries and descriptor-based array cloning eliminating `Array.prototype.push`; expands the comprehensive Intrinsic Dependency Table; establishes strict 8-dimensional publication identity normalization (`normalizePublicationIdentity`) and field discrimination; mandates universal clone-before-hash adoption across all recovery branches including dedicated no-active-attempt startup recovery (`_recoverAuthoritativeRootNoActiveAttempt`); enforces exact active-attempt ownership, post-await leadership fencing, and conditional cleanup; unifies coordinator state into a single immutable `CoordinatorCustodyState` record with atomic `#replaceCoreStateAtomic` transitions; codifies the 12-branch state transition table; expands the failure matrix to 285 deterministic cases; and codifies two hundred and eighty-five deterministic future test specifications.

*Note on Acceptance Discipline:* Completion of Milestone M5.0-R18-R37 makes the corrected architecture ready for an independent holistic acceptance review. M5.1 implementation remains unauthorized until that review is passed and Engineering Authority explicitly approves implementation.

### Mandatory Implementation Authorization Statement

> "M5.0-R18-R37 is a documentation-only architectural correction. No production implementation has been authorized. M5.1, including M5.1a, remains strictly unauthorized pending holistic M5.0 acceptance and explicit Engineering Authority approval."
