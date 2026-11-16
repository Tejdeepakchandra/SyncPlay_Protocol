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
- **Additive Phase 7 (PROTOCOL_VERSION = 1):** The new fields (`logicalTimestamp`, `originSequenceNumber`, `predecessorEventId`) are optional in the v1 envelope. Phase 7 peers negotiate over v1 DataChannels.
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
Phase 7 extends the snapshot data with version vector metadata while maintaining backward-compatible parsing:
```ts
export const EngineSnapshotDataV2Schema = z.object({
  formatVersion: z.union([z.literal(1), z.literal(2)]),
  sessionId: z.string().min(1).max(128),
  status: z.enum(['paused', 'playing']),
  anchorPositionMs: z.number().finite().nonnegative(),
  playbackRate: z.number().finite().positive(),
  lastAppliedOperation: z.object({...}).optional(),
  
  // Phase 7 Additions:
  frontier: z.record(z.string(), z.number().int().nonnegative()).optional(),
  logicalTimestamp: z.number().int().nonnegative().optional(),
});
```
- If a Phase 7 engine receives a Phase 6 snapshot (`formatVersion: 1`), it defaults `frontier` to empty and seeds its Lamport clock from 0.
- If a Phase 6 engine receives a Phase 7 snapshot, unknown fields (`frontier`, `logicalTimestamp`) are stripped cleanly.

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
