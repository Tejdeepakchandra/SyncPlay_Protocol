# Phase 7.5 M4.3b — Targeted Correctness Closure Report
## Root Encapsulation, Authorized Atomic Installation & Recovery Counter Preservation

**Milestone:** Phase 7.5 Sub-Milestone M4.3b  
**Mode:** Targeted Implementation Corrections  
**Starting Audited Commit:** `a15ae9b183cb650a3443a5f3c40238cef45ca351`  
**Direct Parent:** `2589e33b5adb3056cfad1f1eeb3176dc69272530`  
**Approved Architecture Reference:** `2bb117c5db474a13307be65c6f68845f2bd74720`  
**Frozen M4.1–M4.2d Baseline:** `aafa366cc48fdb2b83940f00e80d92d813e7c52e` (0 diffs across frozen files)  
**Final Status:** `M4.3b TARGETED CORRECTIONS COMPLETE — READY FOR INDEPENDENT RE-AUDIT`

---

## 1. Executive Summary

All six findings identified in the Phase 7.5 M4.3b independent source audit have been resolved through targeted corrections:

1. **FINDING-M4.3b-01 (Resolved):** Eliminated the public `coreState` getter. Implemented `getDiagnosticCoreSnapshot()` returning deeply frozen detached copies and primitives without exposing any mutable operational classes.
2. **FINDING-M4.3b-02 (Resolved):** Capability-gated `replaceCoreStateAtomic(candidate, capability)` with `RecoveryInstallationCapability`, enforcing exact object identity verification. Unauthenticated calls throw `UNAUTHORIZED`.
3. **FINDING-M4.3b-03 (Resolved):** Preserved Lamport clock monotonicity in `SnapshotInstaller.stage()` using $L_{\text{candidate}} = \max(L_{\text{local}}, \text{maxLogicalTimestamp})$. Candidate clock never regresses below active engine clock.
4. **FINDING-M4.3b-04 (Resolved):** Preserved local origin sequence counter in `SnapshotInstaller.stage()` using $S_{\text{candidate}} = \max(S_{\text{local}}, V_{\text{snapshot}}[\text{localPeerId}] \mathbin{\text{??}} 0)$, preventing sequence number reuse and collisions.
5. **FINDING-M4.3b-05 & Correction E (Resolved):** Closed candidate alias ownership via an **opaque staged handle architecture**. The staged handle is frozen and contains only read-only metadata. The candidate `EngineCoreState` and operational collections are held strictly inside `SnapshotInstaller`'s private `WeakMap`. Forged or reused handles are rejected with `INVALID_STAGED_HANDLE`.
6. **FINDING-M4.3b-06 (Resolved):** Synchronized `this.core.lastAppliedEventId` in `LeaderlessSyncEngine.state` setter: `this.core.lastAppliedEventId = val.lastAppliedEvent?.logicalEventId;`.

---

## 2. Before / After Public API Surface

| Area | Before Correction (`a15ae9b1`) | After Correction |
|---|---|---|
| **Live Root Accessor** | `public get coreState(): Readonly<EngineCoreState>` (returned live mutable container) | **Removed.** Replaced by `public getDiagnosticCoreSnapshot(): EngineCoreDiagnosticSnapshot` (returns detached, frozen copies). |
| **Root Replacement** | `public replaceCoreStateAtomic(candidate: EngineCoreState): void` (unrestricted public method) | `public replaceCoreStateAtomic(candidate: EngineCoreState, capability: RecoveryInstallationCapability): void` (capability-gated by exact identity). |
| **Capability Creation** | None | `createRecoveryInstallationCapability(authority: string, sessionId: string, token?: string): RecoveryInstallationCapability` |
| **Engine Config** | `epochCapability`, `transportCapability` | Added `recoveryInstallationCapability?: RecoveryInstallationCapability` (and alias `recoveryCapability`). |
| **Staged Result** | `StagedSnapshotInstallation` exposed mutable `candidateCore: EngineCoreState` | `StagedSnapshotInstallation` is an **opaque handle** (`handleId`, `sessionId`, `targetEpoch`, `targetFrontier`, `canonicalDigest`, `maxLogicalTimestamp`). No operational objects exposed. |
| **Stage Options** | `stage(val, config, now?: number, reducerFn?)` | `stage(val, config, optionsOrNow?: SnapshotStageOptions \| number, legacyReducerFn?)` accepting `currentLogicalClock` and `currentLocalOriginSequence`. |
| **Commit Signature** | `commitAtomic(engine, staged)` | `commitAtomic(engine, staged, capability: RecoveryInstallationCapability)` validating capability and consuming handle once. |
| **Error Codes** | 4 recovery codes | Added `INVALID_STAGED_HANDLE` to `SyncEngineErrorCode`. |

---

## 3. Detailed Corrections & Architectural Invariants

### 3.1 Correction A: Public Root Exposure Elimination
- In `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
  - Removed `public get coreState(): Readonly<EngineCoreState>`.
  - Added `EngineCoreDiagnosticSnapshot` interface containing only read-only primitives and frozen dictionaries.
  - Implemented `getDiagnosticCoreSnapshot()`:
    - `replicationFrontier`: `Object.freeze({ ...this.core.replicationVector })`
    - `localFrontier`: `Object.freeze({ ...this.core.localCausalContext })`
    - `baseSnapshotFrontier`: `Object.freeze({ ...this.core.baseSnapshotFrontier })`
    - Zero operational class instances (`clockManager`, `operationLog`, `pendingQueue`) are exposed.

### 3.2 Correction B: Capability-Gated Atomic Root Replacement
- In `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
  - Introduced `RecoveryInstallationCapability` and `createRecoveryInstallationCapability(authority, sessionId, token?)`.
  - Injected via `config.recoveryInstallationCapability` (or `config.recoveryCapability`) at trusted composition boundary.
  - `LeaderlessSyncEngine` stores `private readonly authorizedRecoveryCapability?: RecoveryInstallationCapability;`.
  - Does NOT expose any capability getter.
  - `replaceCoreStateAtomic(candidate: EngineCoreState, capability: RecoveryInstallationCapability)` enforces:
    1. Rejects if engine is disposed (`ENGINE_DISPOSED`).
    2. Rejects if `capability !== this.authorizedRecoveryCapability` (`UNAUTHORIZED`).
    3. Rejects if `!candidate || typeof candidate !== 'object'` (`INVALID_ARGUMENT`).
    4. Rejects if `!candidate.operationLog || !candidate.clockManager || !candidate.pendingQueue` (`INVALID_CANDIDATE_ROOT`).
    5. Replaces `this.core = candidate` in a single synchronous reference assignment.

### 3.3 Correction C: Lamport Clock Monotonicity Preservation
- In `packages/sync-engine/src/recovery/snapshot-installer.ts`:
  - `SnapshotStageOptions` accepts `currentLogicalClock: number`.
  - Validates `currentLogicalClock` is a non-negative finite integer (`INVALID_ARGUMENT` if malformed).
  - Formula:
    $$L_{\text{candidate}} = \max(L_{\text{local}}, \text{maxLogicalTimestamp})$$
  - Snapshot installation reseeds candidate clock without `+1` (adhering to Phase 7 snapshot reseeding invariant).
  - Failed staging leaves active engine clock untouched.

### 3.4 Correction D: Local Origin Sequence Counter Preservation
- In `packages/sync-engine/src/recovery/snapshot-installer.ts`:
  - `SnapshotStageOptions` accepts `currentLocalOriginSequence: number`.
  - Validates `currentLocalOriginSequence` is a non-negative finite integer (`INVALID_ARGUMENT` if malformed).
  - Formula:
    $$S_{\text{candidate}} = \max(S_{\text{local}}, V_{\text{snapshot}}[\text{localPeerId}] \mathbin{\text{??}} 0)$$
  - Rationale:
    - If local peer has originated $S_{\text{local}}$ operations prior to recovery, resetting to zero would cause sequence reuse (collision on `(localPeerId, sequenceNumber)`).
    - If snapshot already contains compacted operations from `localPeerId` up to $V_{\text{snapshot}}[\text{localPeerId}]$, the candidate must not be lower than that frontier to prevent re-issuing compacted sequence numbers.
    - If snapshot contains no entry for `localPeerId`, $S_{\text{candidate}} = S_{\text{local}}$.

### 3.5 Correction E: Opaque Staged Handle & Alias Ownership
- In `packages/sync-engine/src/recovery/snapshot-installer.ts`:
  - `StagedSnapshotInstallation` is an **opaque handle**:
    ```typescript
    export interface StagedSnapshotInstallation {
      readonly handleId: string;
      readonly sessionId: string;
      readonly targetEpoch: number;
      readonly targetFrontier: Readonly<Record<string, number>>;
      readonly canonicalDigest: string;
      readonly maxLogicalTimestamp: number;
    }
    ```
  - `SnapshotInstaller` maintains private candidate storage:
    `private readonly stagedRoots = new WeakMap<StagedSnapshotInstallation, EngineCoreState>();`
  - In `stage()`:
    - Assembles sealed `candidateCore` out-of-band.
    - Freezes `handle.targetFrontier` and `handle` via `Object.freeze()`.
    - Stores mapping in `stagedRoots.set(handle, candidateCore)`.
    - Returns `handle`. External callers have zero access to `candidateCore`.
  - In `commitAtomic(engine, staged, capability)`:
    - Verifies capability.
    - Retrieves candidate: `const candidateCore = this.stagedRoots.get(staged);`.
    - If undefined, throws `SyncEngineError('INVALID_STAGED_HANDLE', ...)`.
    - Immediately consumes handle: `this.stagedRoots.delete(staged);`.
    - Commits via `engine.replaceCoreStateAtomic(candidateCore, capability);`.

### 3.6 Correction F: Synchronization of `lastAppliedEventId`
- In `packages/sync-engine/src/engine/leaderless-sync-engine.ts`:
  - In `private set state(val: PlaybackState)`:
    ```typescript
    this.core.playbackState = val;
    this.core.state = val;
    this.core.stateVersion = val.stateVersion;
    this.core.lastAppliedEventId = val.lastAppliedEvent?.logicalEventId;
    ```
  - On snapshot import, `importedState.lastAppliedEvent.logicalEventId` (`snap-${snapshotId}`) updates `this.core.lastAppliedEventId`.
  - On normal operations, reducer's `lastAppliedEvent.logicalEventId` updates `this.core.lastAppliedEventId`.
  - No-op / rejected operations never call `this.state = ...`, preventing erroneous advance.

---

## 4. Test Verification & Adversarial Coverage

### 4.1 Focused Test Suite Results
- `engine-core-state.test.ts`: **9 / 9 passed** (+2 new tests)
  - `A1`: sealed container with all 12 authoritative fields.
  - `A2`: engine initializes with authoritative root and delegates accessors.
  - `A3`: public root exposure eliminated; diagnostic snapshot returns frozen detached projection.
  - `A4`: capability-gated atomic root replacement succeeds when authorized.
  - `A5`: unauthorized calls (missing or forged capability) rejected with `UNAUTHORIZED`.
  - `A6`: candidate shape validation (`INVALID_ARGUMENT` / `INVALID_CANDIDATE_ROOT`).
  - `A7`: rejection on disposed engine (`ENGINE_DISPOSED`).
  - `A8`: recovery mode notification deferral.
  - `A9`: `lastAppliedEventId` strictly synchronized with `playbackState.lastAppliedEvent`.
- `snapshot-installer.test.ts`: **28 / 28 passed** (+8 new tests)
  - `C1`–`C8`: eligibility evaluation (identical, agreement-only, dominates, incomparable, dominated, future, repairable).
  - `B1`–`B6`: out-of-band validation (schema, JSON, session ID, digest, timestamps, checkpoint sorting).
  - `B7`: opaque frozen handle construction with zero engine mutation.
  - `B8`: staging invalid validation result rejected.
  - `B9`: mutation of source payload does not affect candidate.
  - `C10`: Lamport clock monotonicity (local 500 vs snapshot 100 $\rightarrow$ 500; local 50 vs snapshot 100 $\rightarrow$ 100; equal 100 $\rightarrow$ 100).
  - `C11`: clock validation rejects non-integer / negative values.
  - `S1`: origin sequence counter preservation (local 15 vs snap 10 $\rightarrow$ 15; local 10 vs snap 20 $\rightarrow$ 20; no local peer in snap $\rightarrow$ 12).
  - `S2`: sequence validation rejects non-integer / negative values.
  - `D1`: commit with authorized capability updates engine state atomically.
  - `D2`: missing or forged capability rejected with `UNAUTHORIZED`.
  - `D3`: forged staged handle rejected with `INVALID_STAGED_HANDLE`.
  - `D4`: double commit / handle reuse rejected with `INVALID_STAGED_HANDLE`.
  - `D5`: validation failure leaves active engine 100% unchanged.
  - `D6`: post-commit listener exceptions do not corrupt or roll back committed state.
  - `D7`: commit on disposed engine rejected with `ENGINE_DISPOSED`.

### 4.2 Full Monorepo Test Suite Results
```text
Test Suites: 63 passed (63 total)
Tests:       1,459 passed (1,459 total, +10 new tests)
Snapshots:   0 total
Time:        ~13.2s
Duration:    All 5 workspace packages passed cleanly (0 failed, 0 skipped)
  - @syncplay/control-events: 49 passed (6 test files)
  - @syncplay/peer-mesh:      108 passed (6 test files)
  - @syncplay/protocol:       337 passed (6 test files)
  - @syncplay/sync-engine:    874 passed (42 test files)
  - @syncplay/bootstrap-server: 91 passed (3 test files)
```

### 4.3 Static Checks & Build Verification
- `.only()` / `.skip()` scan across `packages/sync-engine/src/__tests__/*.test.ts`: **0 occurrences**.
- Production build (`npm run build`): Clean exit code 0 across all workspaces.
- Demo Vite build (`vite build`): Clean exit code 0 (195 modules transformed, 212 kB JS bundle).
- TypeScript compilation (`tsc`): Clean 0 errors across all packages.

---

## 5. Frozen Baseline Protection

Verified diff against frozen baseline commit `aafa366cc48fdb2b83940f00e80d92d813e7c52e`:
```bash
git diff aafa366cc48fdb2b83940f00e80d92d813e7c52e..HEAD -- \
  packages/protocol \
  packages/sync-engine/src/recovery/chunk-assembler.ts \
  packages/sync-engine/src/recovery/snapshot-serializer.ts \
  packages/sync-engine/src/recovery/snapshot-preparation-queue.ts \
  packages/sync-engine/src/recovery/snapshot-chunk-producer.ts \
  packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts \
  packages/sync-engine/src/recovery/snapshot-inbound-pipeline.ts \
  packages/sync-engine/src/recovery/candidate-offer-registry.ts \
  packages/sync-engine/src/recovery/inbound-custody.ts
```
**Result:** Empty diff (0 lines changed across frozen paths).

---

## 6. Modified Files Summary

| File | Changes |
|---|---|
| `packages/sync-engine/src/errors.ts` | Added `INVALID_STAGED_HANDLE` to `SyncEngineErrorCode` |
| `packages/sync-engine/src/engine/leaderless-sync-engine.ts` | Added `RecoveryInstallationCapability`, `EngineCoreDiagnosticSnapshot`, removed `coreState`, gated `replaceCoreStateAtomic`, synchronized `lastAppliedEventId` |
| `packages/sync-engine/src/recovery/snapshot-installer.ts` | Opaque `StagedSnapshotInstallation` handle, private `stagedRoots` WeakMap, Lamport clock & origin sequence preservation in `stage()`, capability-gated `commitAtomic()` |
| `packages/sync-engine/src/index.ts` | Exported `RecoveryInstallationCapability`, `createRecoveryInstallationCapability`, `EngineCoreDiagnosticSnapshot`, `SnapshotStageOptions` |
| `packages/sync-engine/src/__tests__/engine-core-state.test.ts` | Updated tests for root encapsulation, capability authorization, and `lastAppliedEventId` sync |
| `packages/sync-engine/src/__tests__/snapshot-installer.test.ts` | Updated tests for opaque handle safety, clock/sequence monotonicity, handle re-use prevention, and capability verification |

---

## 7. Deferred Scope Confirmation

- **M4.3c (Recovery Coordinator & Quarantine Drainer with `DRAINER_IN_FLIGHT`):** Strictly deferred.
- **M4.3d (End-to-End Multi-Peer Recovery Matrix):** Strictly deferred.

---

## 8. Final Status Declaration

```text
M4.3b TARGETED CORRECTIONS COMPLETE — READY FOR INDEPENDENT RE-AUDIT
```
