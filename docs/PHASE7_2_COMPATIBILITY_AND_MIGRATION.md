# Phase 7.2 — Compatibility, Migration & Rollback Strategy

> **Document Version:** 1.0.0  
> **Status:** SPECIFICATION ONLY — NO IMPLEMENTATION AUTHORIZED  
> **Parent Milestone:** Phase 7.2 (Architecture & Interface Design)  
> **Baseline Commit:** `926c575`  
> **Target Subsystems:** Monorepo package compatibility and migration roadmap  

---

## 1. Upstream Package Compatibility (Phases 1–6)

Phase 7 is designed to integrate cleanly without breaking or rewriting any completed upstream package:

| Subsystem Package | Status in Phase 7 | Compatibility Mechanism |
| :--- | :---: | :--- |
| **`@syncplay/protocol`** | Extended (Additive) | Additive optional fields in envelope (`logicalTimestamp`). Forward-stripping Zod schemas prevent parsing errors in older code. |
| **`@syncplay/bootstrap-server`** | **100% Unchanged** | Bootstrap server remains strictly non-authoritative for playback. Zero routing or schema changes required on the server. |
| **`@syncplay/peer-mesh`** | **100% Unchanged** | WebRTC DataChannels (`'syncplay-data'`), offerer policy, and connection generations are utilized as-is without modification. |
| **`@syncplay/control-events`** | **100% Unchanged** | Dispatches to `SyncEventReceiver`. Enforces rate-limiting and deduplication. Wire encoding remains transparent. |
| **`@syncplay/sync-engine`** | Extended (Additive) | `BaselineSyncEngine` remains untouched as a reference implementation. `LeaderlessSyncEngine` is added as an alternative implementation of `SyncEngine`. |
| **`apps/demo`** | Drop-in Upgrade | Uses the identical public `SyncEngine` interface; requires only changing the constructor call. |

---

## 2. Wire Protocol Compatibility & Version Migration

### 2.1. Forward-Stripping Zod Validation
The canonical envelope schema defined in `packages/protocol/src/envelope.ts`:
```ts
export const ProtocolMessageSchema = z.object({
  protocolVersion: z.number().int().positive(),
  messageType: z.string().min(1).max(64),
  sessionId: z.string().min(1).max(128),
  senderPeerId: z.string().min(1).max(128),
  messageId: z.string().min(1).max(128),
  logicalEventId: z.string().min(1).max(128).optional(),
});
```
Because Zod's default object behavior is **strip unknown fields**, an existing Phase 1–6 client receiving a Phase 7 message containing `logicalTimestamp` will cleanly strip the field and process the envelope without throwing validation errors.

### 2.2. Protocol Versioning Strategy
- **Additive Phase 7 (PROTOCOL_VERSION = 1):** The new fields (`originPeerId`, `logicalTimestamp`, `originSequenceNumber`, `causalContext`) are optional in the v1 envelope. Phase 7 peers negotiate over v1 DataChannels.
- **Strict Protocol Bump (PROTOCOL_VERSION = 2):** If a deployment requires enforcing that all participating peers possess the leaderless engine, `PROTOCOL_VERSION = 2` is used. During room join, peers advertising `protocolVersion < 2` are rejected.

---

## 3. Snapshot Compatibility & Migration

In Phase 6, `EngineSnapshotDataSchema` in `packages/sync-engine/src/snapshot.ts` defined:
```ts
export const EngineSnapshotDataSchema = z.object({
  formatVersion: z.literal(1),
  sessionId: z.string().min(1).max(128),
  status: z.enum(['paused', 'playing']),
  anchorPositionMs: z.number().finite().nonnegative(),
  playbackRate: z.number().finite().positive(),
  lastAppliedOperation: z.object({...}).optional(),
});
```

### 3.1. Phase 7 Snapshot Extension (`formatVersion: 2`)
Phase 7 extends the snapshot data with version vector metadata and cryptographic digest verification while maintaining backward-compatible parsing:
```ts
export const EngineSnapshotDataV2Schema = z.object({
  formatVersion: z.literal(2),
  sessionId: z.string().min(1).max(128),
  status: z.enum(['paused', 'playing']),
  anchorPositionMs: z.number().finite().nonnegative(),
  playbackRate: z.number().finite().positive(),
  lastAppliedOperation: z.object({
    logicalEventId: z.string().min(1).max(128),
    originPeerId: z.string().min(1).max(128),
    messageType: z.string().min(1).max(64),
    logicalTimestamp: z.number().int().nonnegative(),
    appliedAtMonotonic: z.number().finite().nonnegative(),
    isImportedProvenance: z.boolean().optional(),
  }).optional(),
});

export const StateSnapshotPayloadV2Schema = z.object({
  snapshotId: z.string().min(1).max(128),
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  snapshotAuthorPeerId: z.string().min(1).max(128),
  snapshotFrontier: z.record(z.string().min(1).max(128), z.number().int().nonnegative()),
  maxLogicalTimestamp: z.number().int().nonnegative(),
  stateDigestSha256: z.string().min(64).max(64),
  state: EngineSnapshotDataV2Schema,
});
```
- If a Phase 7 engine receives a Phase 6 snapshot (`formatVersion: 1`), it defaults `snapshotFrontier` to empty and seeds its Lamport clock from 0.
- If a Phase 6 engine receives a Phase 7 snapshot, unknown fields (`snapshotFrontier`, `stateDigestSha256`, `membershipEpoch`) are stripped cleanly.

### 3.2. Browser Reload & Ephemeral Identity Policy
In a browser client without durable IndexedDB storage, page reload (F5) purges local memory, including sequence counters and operation logs.
- **Policy Invariant:** A reloading peer **MUST request a fresh `peerId` from the Bootstrap Server upon rejoining**.
- **Sequence Reset:** The reloading peer begins originating operations at sequence $1$ under its fresh `peerId`.
- **Roster Cleanup:** The Bootstrap Server observes the disconnection of the previous connection, broadcasts `session:peer-left`, and all surviving peers increment `membershipEpoch` and evict the departed `peerId` from their active frontiers.
- **Benefit:** Eliminates sequence collisions, stale clock overwrites, and complex client-side disk persistence.

### 3.3. Strict Epoch Fencing & Offline Partition Policy (MVP Trade-Off)
In Revision P7.2-R2, the engine adopts **Strict Epoch Fencing** for handling partitioned nodes:
- **Offline Operations Discarded:** If a peer becomes partitioned, continues generating operations under epoch $E$, while surviving peers advance to $E+1$ and compact history, any operations generated by the partitioned peer under epoch $E$ are **fenced and discarded** upon reconnect (`REJECTED_OBSOLETE_EPOCH`).
- **Documented Trade-Off:** The application accepts the loss of offline playback inputs generated during network isolation in exchange for:
  1. Guaranteed bounded memory ($K=256$) without risk of log overflow or memory leaks.
  2. Prevention of severe UX regressions (e.g. video jumping backward to an obsolete timestamp from 10 minutes ago).
- **Bootstrap Downtime Tolerance:** Active peers continue synchronized playback over established WebRTC DataChannels even if the Bootstrap Server is temporarily unreachable. Bootstrap is strictly non-authoritative for playback.

### 3.4. Strict No-Forwarding Policy & Direct Mesh Invariant
Revision P7.2-R3 formalizes the **Strict No-Forwarding Policy** for live control broadcasts:
- **Direct Author Broadcast:** Every live control event MUST be broadcast directly by its author to all mesh peers. The transport peer identity (`senderPeerId` / `fromPeerId`) verified at the WebRTC layer MUST equal `originPeerId`.
- **Relay Prohibition:** Live relayed events are rejected with `REJECTED_UNAUTHORIZED_ORIGIN`. Unauthenticated peer relaying without public-key digital signatures cannot safely distinguish benign forwarding from sender impersonation or replay attacks.
- **Repair Batch Exemption:** Anti-entropy repair batches (`OPERATION_BATCH`) explicitly decouple `supplierPeerId` (immediate sender fulfilling repair) from `originPeerId` (historical author), and state snapshots decouple `supplierPeerId` from `snapshotAuthorPeerId`.
- **Topology Limitation:** Direct full mesh is required for live event exchange ($N \le 8$). Multi-hop overlay routing is out of scope for Phase 7.

### 3.5. Incomparable Snapshot Resolution & Explicit Degraded Recovery
In Revision P7.2-R3, arbitrary scalar tie-breaking ($\max(\text{epoch}) \to \max(L) \to \max_{\text{lex}}(\text{peerId})$) for incomparable snapshots is completely eliminated:
- **Component-Wise Domination Only:** A snapshot is installed if and only if its frontier dominates local known history ($V_{\text{snap}} \ge V_{\text{local}}$).
- **Log Repair First:** If frontiers are incomparable ($V_{\text{snap}} \parallel V_{\text{local}}$), the engine attempts anti-entropy log repair to fill the missing operations.
- **Degraded State Transition:** If missing history has already been compacted beyond the capacity horizon ($K = 256$), the engine refuses to guess or pick an arbitrary branch. It transitions to `'DEGRADED_OUT_OF_SYNC'` and suspends playback mutation.
- **Recovery Procedures:**
  1. **Host Re-Seek:** A host or active peer issues a fresh authoritative control operation (`SEEK` or `PLAY`), establishing a new monotonic sequence that dominates both divergent histories.
  2. **Room Reload:** The degraded peer reloads the page (triggering Section 3.2 Ephemeral Identity Reset), obtains a fresh `peerId`, and pulls the current canonical snapshot cleanly.

---

## 4. Demo Application Integration Plan

In `apps/demo/src/main.ts`, the demo currently instantiates:
```ts
state.syncEngine = new BaselineSyncEngine({
  sessionId: state.roomId,
  localPeerId: state.peerId,
});
```

### 4.1. Seamless Migration
Because `LeaderlessSyncEngine` implements the identical `SyncEngine` contract, switching the demo requires changing only one line:
```ts
state.syncEngine = new LeaderlessSyncEngine({
  sessionId: state.roomId,
  localPeerId: state.peerId,
  mesh: state.manager, // for anti-entropy digest exchange
});
```
All UI state subscriptions (`onStateChange`, `onDecision`, `getCurrentPositionMs()`, `exportSnapshot()`) remain 100% identical.

---

## 5. Rollback Strategy

If an unexpected algorithmic or edge-case defect is discovered during Phase 7 implementation (P7.3–P7.6):
1. **Zero Monorepo Contamination:** Because all changes in `@syncplay/sync-engine` are isolated in new modular files (`LeaderlessSyncEngine`, `OrderedOperationLog`, etc.), the unmodified `BaselineSyncEngine` remains active.
2. **Instant Demo Rollback:** Reverting `apps/demo` to `BaselineSyncEngine` takes under 10 seconds.
3. **No Database or Server Migration:** The Bootstrap Server stores zero playback state, meaning rollback requires zero database migrations or server rollbacks.

