# Phase 7.2 — Protocol Evolution RFC: Logical Timestamps, State Digests & Repair Payloads

> **Document Version:** 1.0.0  
> **Status:** SPECIFICATION ONLY — NO IMPLEMENTATION AUTHORIZED  
> **Parent Milestone:** Phase 7.2 (Architecture & Interface Design)  
> **Baseline Commit:** `926c575`  
> **Target Package:** `@syncplay/protocol` (Documentation specification only; no source edits permitted)  

---

## 1. Executive Summary & Purpose

Phase 7 introduces distributed logical ordering, multi-origin anti-entropy, and state recovery to the SyncPlay protocol. To support these capabilities without breaking existing transport, bootstrap, or control-event components, this RFC defines the necessary wire protocol message additions and envelope evolutions.

> [!IMPORTANT]
> **Strict Authorization Boundary:**
> This document specifies proposed schemas as documentation and pseudocode only. **No modifications to `@syncplay/protocol` source code or Zod schemas are authorized during Phase 7.2.**

---

## 2. Canonical Envelope Evolution: `logicalTimestamp`

### 2.1. Analysis of Current Phase 1 Envelope
In Phase 1, `packages/protocol/src/envelope.ts` standardized the following canonical envelope:
```ts
export interface ProtocolMessage<TPayload = unknown> {
  readonly protocolVersion: number;
  readonly messageType: string;
  readonly sessionId: SessionId;
  readonly senderPeerId: PeerId;
  readonly messageId: MessageId;
  readonly logicalEventId?: LogicalEventId;
  readonly payload: TPayload;
}
```
Decision D19 explicitly deferred an open-ended `metadata` field, and D20 deferred a wall-clock `createdAt` field.

### 2.2. Proposed Evolution: First-Class Logical Metadata & Causal Context
To support deterministic total ordering and exact concurrency detection across merging distributed branches, the envelope is extended with optional, first-class logical and causal metadata:
```ts
export interface ProtocolMessageV2<TPayload = unknown> {
  readonly protocolVersion: number;
  readonly messageType: string;
  readonly sessionId: SessionId;
  readonly senderPeerId: PeerId; // Immediate transport peer (hop)
  readonly messageId: MessageId;
  readonly logicalEventId?: LogicalEventId;
  
  /**
   * Logical author identity of the operation.
   * Stamped by originating peer; immutable across multi-hop relay.
   */
  readonly originPeerId?: PeerId;

  /**
   * Monotonic Lamport logical timestamp.
   * Required for all control events ('PLAY', 'PAUSE', 'SEEK', 'RATE_CHANGE').
   * Optional for transport/diagnostics ('PING', 'PONG') and signaling messages.
   */
  readonly logicalTimestamp?: number;

  /**
   * Origin-specific sequence counter for anti-entropy version vector tracking.
   * Stamped by the originating peer; immutable across retransmissions.
   */
  readonly originSequenceNumber?: number;

  /**
   * Causal Context Version Vector.
   * Snapshot of highest consecutive origin sequences observed by the author
   * at the instant this operation was created: Record<originPeerId, originSequence>.
   * Used for exact concurrency detection: e1 || e2 <=> (e1 !-> e2) && (e2 !-> e1).
   */
  readonly causalContext?: Record<string, number>;

  readonly payload: TPayload;
}
```

> [!NOTE]
> **Superseding Single Predecessor Pointers:**
> A single predecessor pointer (`predecessorEventId`) failed because it could not represent multiple concurrent branches merging into a single causal history. The `causalContext` vector provides complete, mathematically rigorous concurrency detection. For an 8-peer room, `causalContext` adds only $\approx 120\text{ bytes}$ per control event, which is negligible at human interaction rates ($\le 5\text{ ops/sec}$).

### 2.3. Backward Compatibility & Forward Extensibility
1. **Zod Forward-Strip Behavior:** The existing Phase 1 Zod validator `ProtocolMessageSchema` in `packages/protocol` uses Zod's default behavior: unknown fields are stripped cleanly rather than throwing errors. Existing Phase 1–6 nodes receiving messages with `logicalTimestamp` will strip the field and process the envelope without failing.
2. **Protocol Version Bump (PROTOCOL_VERSION):**
   - The wire protocol version can remain `PROTOCOL_VERSION = 1` if `logicalTimestamp` is an optional additive field.
   - If strict validation of `logicalTimestamp` is required across all nodes, the version increments to `PROTOCOL_VERSION = 2`, and nodes reject v1 envelopes during negotiation.

---

## 3. New Message Types & Payload Schemas

The following four message types are added to the authoritative `MessageType` registry in `@syncplay/protocol`:

```ts
export const Phase7MessageTypes = {
  STATE_DIGEST: 'STATE_DIGEST',
  REPAIR_REQUEST: 'REPAIR_REQUEST',
  OPERATION_BATCH: 'OPERATION_BATCH',
  STATE_SNAPSHOT: 'STATE_SNAPSHOT',
} as const;
```

---

### 3.1. Message Type 1: `STATE_DIGEST`

- **Purpose:** Periodic or reactive announcement of a peer's received operation frontier for anti-entropy gap detection.
- **Direction:** Broadcast to all connected peers over WebRTC DataChannels.
- **Frequency:** Every $5000\text{ ms}$ ($0.2\text{ Hz}$) during active playback, or immediately upon membership/generation changes.

#### Proposed TypeScript Schema (Documentation Only):
```ts
export interface StateDigestPayload {
  /** Active session/room ID. */
  readonly sessionId: string;

  /** Peer ID of the node emitting the digest. */
  readonly senderPeerId: string;

  /** Active connection generation of the emitting node. */
  readonly connectionGeneration: number;

  /** Room membership epoch from the Bootstrap Server. */
  readonly membershipEpoch: number;

  /**
   * Version Vector mapping Origin Peer ID to highest consecutively received sequence number.
   * Key: PeerId, Value: Non-negative integer.
   */
  readonly versionVector: Record<string, number>;

  /**
   * Version Vector frontier covered by this peer's base snapshot.
   */
  readonly snapshotFrontier: Record<string, number>;

  /** Identifier of the base snapshot preceding this peer's active log. */
  readonly baseSnapshotId: string;

  /** State version represented by the base snapshot. */
  readonly baseStateVersion: number;
}
```

#### Proposed Zod Schema (Documentation Only):
```ts
export const StateDigestPayloadSchema = z.object({
  sessionId: z.string().min(1).max(128),
  senderPeerId: z.string().min(1).max(128),
  connectionGeneration: z.number().int().positive(),
  membershipEpoch: z.number().int().nonnegative(),
  versionVector: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative()
  ),
  snapshotFrontier: z.record(
    z.string().min(1).max(128),
    z.number().int().nonnegative()
  ),
  baseSnapshotId: z.string().min(1).max(128),
  baseStateVersion: z.number().int().nonnegative(),
});
```

#### Byte Overhead Calculation:
- Base JSON envelope & fields: $\approx 200\text{ bytes}$.
- Version Vector entry per peer: $\approx 24\text{ bytes}$ (e.g., `"peer_abc123": 45`).
- For a 4-peer room: $200 + (4 \times 24 \times 2) \approx \mathbf{392\text{ bytes}}$.
- For an 8-peer room (maximum capacity): $200 + (8 \times 24 \times 2) \approx \mathbf{584\text{ bytes}}$.
- Well within the protocol limit of $\text{MAX\_PAYLOAD\_BYTES} = 65,536\text{ bytes}$.

---

### 3.2. Message Type 2: `REPAIR_REQUEST`

- **Purpose:** Request a missing slice of historical operations from an origin peer or a peer holding the log.
- **Direction:** Targeted unicast over a specific WebRTC DataChannel.

#### Proposed TypeScript Schema (Documentation Only):
```ts
export interface RepairRequestPayload {
  /** Active session/room ID. */
  readonly sessionId: string;

  /** Peer ID requesting the missing operations. */
  readonly requesterPeerId: string;

  /** Origin peer whose operations are missing. */
  readonly targetOriginPeerId: string;

  /** Starting sequence number (inclusive). */
  readonly fromSequence: number;

  /** Ending sequence number (inclusive). */
  readonly toSequence: number;
}
```

#### Proposed Zod Schema (Documentation Only):
```ts
export const RepairRequestPayloadSchema = z.object({
  sessionId: z.string().min(1).max(128),
  requesterPeerId: z.string().min(1).max(128),
  targetOriginPeerId: z.string().min(1).max(128),
  fromSequence: z.number().int().positive(),
  toSequence: z.number().int().positive(),
}).refine((data) => data.toSequence >= data.fromSequence, {
  message: 'toSequence must be greater than or equal to fromSequence',
});
```

---

### 3.3. Message Type 3: `OPERATION_BATCH`

- **Purpose:** Response to a `REPAIR_REQUEST`, delivering missing operations.
- **Direction:** Targeted unicast over WebRTC DataChannel.
- **Bounding:** Maximum of $32$ operations per batch.

#### Proposed TypeScript Schema (Documentation Only):
```ts
export interface SerializedControlOperation {
  readonly logicalEventId: string;
  readonly originPeerId: string;
  readonly senderPeerId: string;
  readonly messageType: 'PLAY' | 'PAUSE' | 'SEEK' | 'RATE_CHANGE';
  readonly logicalTimestamp: number;
  readonly originSequenceNumber: number;
  readonly causalContext: Record<string, number>;
  readonly payload: unknown;
}

export interface OperationBatchPayload {
  readonly sessionId: string;
  readonly originPeerId: string;
  readonly fromSequence: number;
  readonly toSequence: number;
  readonly operations: ReadonlyArray<SerializedControlOperation>;
}
```

#### Proposed Zod Schema (Documentation Only):
```ts
export const SerializedControlOperationSchema = z.object({
  logicalEventId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(128),
  senderPeerId: z.string().min(1).max(128),
  messageType: z.enum(['PLAY', 'PAUSE', 'SEEK', 'RATE_CHANGE']),
  logicalTimestamp: z.number().int().nonnegative(),
  originSequenceNumber: z.number().int().positive(),
  causalContext: z.record(z.string().min(1).max(128), z.number().int().nonnegative()),
  payload: z.record(z.string(), z.unknown()),
});

export const OperationBatchPayloadSchema = z.object({
  sessionId: z.string().min(1).max(128),
  originPeerId: z.string().min(1).max(128),
  fromSequence: z.number().int().positive(),
  toSequence: z.number().int().positive(),
  operations: z.array(SerializedControlOperationSchema).max(32),
}).refine((data) => data.operations.length <= (data.toSequence - data.fromSequence + 1), {
  message: 'Operations array length exceeds requested range',
});
```

---

### 3.4. Message Type 4: `STATE_SNAPSHOT` (`StateSnapshotPayloadV2`)

- **Purpose:** Full authoritative baseline snapshot transfer for rejoining peers or peers whose gap exceeds the retained history horizon ($> K$).
- **Direction:** Targeted unicast over WebRTC DataChannel in response to `STATE_REQUEST`.

#### Proposed TypeScript Schema (Documentation Only):
```ts
export interface StateSnapshotPayloadV2 {
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly snapshotAuthorPeerId: string;
  readonly snapshotFrontier: Record<string, number>;
  readonly maxLogicalTimestamp: number;
  readonly stateDigestSha256: string;
  readonly state: {
    readonly formatVersion: 2;
    readonly sessionId: string;
    readonly status: 'paused' | 'playing';
    readonly anchorPositionMs: number;
    readonly playbackRate: number;
    readonly lastAppliedOperation?: {
      readonly logicalEventId: string;
      readonly originPeerId: string;
      readonly messageType: string;
      readonly logicalTimestamp: number;
      readonly appliedAtMonotonic: number;
      readonly isImportedProvenance?: boolean;
    };
  };
}
```

#### Proposed Zod Schema (Documentation Only):
```ts
export const StateSnapshotPayloadV2Schema = z.object({
  snapshotId: z.string().min(1).max(128),
  sessionId: z.string().min(1).max(128),
  membershipEpoch: z.number().int().nonnegative(),
  snapshotAuthorPeerId: z.string().min(1).max(128),
  snapshotFrontier: z.record(z.string().min(1).max(128), z.number().int().nonnegative()),
  maxLogicalTimestamp: z.number().int().nonnegative(),
  stateDigestSha256: z.string().min(64).max(64),
  state: z.object({
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
  }),
});
```

---

## 4. Size Bounding & Validation Invariants

1. **Global Size Cap:** All payloads must serialize to $\le 65,536\text{ bytes}$ (`MAX_PAYLOAD_BYTES`).
2. **Bounded Batches:** An `OPERATION_BATCH` is strictly capped at $32$ operations. An operation typically serializes to $\approx 250\text{ bytes}$, yielding a batch size of $\approx 8\text{ KB}$, well within SCTP MTU and buffer limits.
3. **Safe Integer Enforcement:** All counters (`logicalTimestamp`, `originSequenceNumber`, `fromSequence`, `toSequence`, `membershipEpoch`) are validated with `z.number().int().nonnegative()` and `Number.isSafeInteger()`.
4. **Origin Validation:** In `OPERATION_BATCH`, the receiving peer validates that all operations in the batch have `originPeerId == targetOriginPeerId`, preventing peers from injecting forged operations on behalf of third parties.
5. **Cryptographic Integrity & Stale Snapshot Rejection:** In `STATE_SNAPSHOT`, the receiver verifies the SHA-256 digest of the serialized state and rejects the snapshot if `snapshotFrontier` is older than local known history ($V_{\text{snap}} < V_{\text{local}}$).

