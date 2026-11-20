# Phase 7.5 M4.2 — Interface Specification: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.1.0  
**Status:** Approved Specification (Targeted Planning Correction Applied)  
**Frozen Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Specification Target:** `@syncplay/sync-engine/recovery`  

---

## 1. Overview & Architecture Placement

This specification establishes the contracts, data structures, and operational boundaries for the M4.2 outbound snapshot transfer pipeline.

```mermaid
classDiagram
    class SnapshotPreparationQueue {
        +enqueue(request: QueuedPreparationRequest): Promise~SerializedSnapshotArtifact~
        +depth: number
        +isBusy: boolean
    }

    class SnapshotSerializer {
        +serialize(snapshot: StateSnapshotPayloadV2): SerializedSnapshotArtifact
        +serializeFromEngine(state: PlaybackState, context: SnapshotContext): SerializedSnapshotArtifact
    }

    class SerializedSnapshotArtifact {
        -ArrayBuffer #masterBuffer
        -number #byteLength
        +string snapshotId
        +string sessionId
        +number membershipEpoch
        +string authorPeerId
        +number totalSizeBytes
        +string sha256Digest
        +boolean isDisposed
        +getBorrowedSliceView(offset: number, length: number) Uint8Array
        +copyByteRange(offset: number, length: number) Uint8Array
        +cloneBytes() Uint8Array
        +dispose() void
    }

    class SnapshotChunkProducer {
        +partition(artifact: SerializedSnapshotArtifact, config: ChunkProducerConfig): PartitionedSnapshot
    }

    class PartitionedSnapshot {
        +SerializedSnapshotArtifact artifact
        +number nominalChunkSizeBytes
        +number totalChunks
        +ReadonlyArray~SnapshotChunkSlice~ slices
        +getSlice(index: number) SnapshotChunkSlice
        +getBorrowedSlice(index: number) Uint8Array
    }

    class SnapshotChunkSlice {
        +number chunkIndex
        +number totalChunks
        +number chunkSizeBytes
        +string chunkSha256
    }

    class SnapshotOutboundPipeline {
        +createOfferMessage(partition: PartitionedSnapshot, context: OutboundOfferContext): ProtocolMessage~SnapshotOfferPayload~
        +prepareChunkMessage(partition: PartitionedSnapshot, chunkIndex: number, context: OutboundChunkContext): OutboundChunkResult
    }

    SnapshotPreparationQueue --> SnapshotSerializer : invokes sequentially
    SnapshotSerializer --> SerializedSnapshotArtifact : produces
    SnapshotChunkProducer --> PartitionedSnapshot : produces
    PartitionedSnapshot *-- SnapshotChunkSlice : contains
    SnapshotOutboundPipeline ..> PartitionedSnapshot : consumes
```

---

## 2. Core Data Structures & Interfaces

### 2.1 Sequential Preparation Queue Types

```typescript
/**
 * Lightweight preparation request descriptor.
 * Queued prior to snapshot capture; retains zero snapshot or buffer memory (< 1 KiB footprint).
 */
export interface QueuedPreparationRequest {
  readonly transferId: string;
  readonly recipientPeerId: string;
  readonly sctpMaxMessageSize: number;
  readonly requestedSnapshotId?: string;
  readonly timestampMs: number;
}
```

---

### 2.2 Serialized Snapshot Artifact Contract

```typescript
/**
 * Encapsulated artifact holding canonical, deterministically serialized snapshot bytes.
 * The underlying ArrayBuffer is private and exclusively owned by the artifact.
 * Guaranteed to conform to MAX_SNAPSHOT_BYTES (4 MiB) and RFC 8785 JCS.
 */
export class SerializedSnapshotArtifact {
  readonly #masterBuffer: ArrayBuffer;
  readonly #byteLength: number;
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly authorPeerId: string;
  readonly totalSizeBytes: number;
  readonly sha256Digest: string;
  #isDisposed: boolean;

  /**
   * Internal package-private accessor for trusted recovery components.
   * Returns a borrowed Uint8Array view referencing the private master buffer.
   * Borrowed views MUST NOT escape into public APIs, event listeners, or untrusted consumers.
   */
  public getBorrowedSliceView(offset: number, length: number): Uint8Array;

  /**
   * Public accessor returning a fresh, unaliased Uint8Array copy of a byte range.
   */
  public copyByteRange(offset: number, length: number): Uint8Array;

  /**
   * Public accessor returning a complete unaliased deep copy of the entire master buffer.
   */
  public cloneBytes(): Uint8Array;

  /**
   * Disposes the artifact, detaching references to the master buffer.
   */
  public dispose(): void;

  public get isDisposed(): boolean;
}
```

#### Invariants:
1. `#byteLength === totalSizeBytes`.
2. `totalSizeBytes > 0 && totalSizeBytes <= MAX_SNAPSHOT_BYTES` (4,194,304 bytes).
3. `sha256Digest === sha256(new Uint8Array(#masterBuffer))`.
4. `#masterBuffer` contains the exact UTF-8 byte encoding of `canonicalizeJson(preimage)`.
5. Preimage object and canonical JSON string are dereferenced immediately post-instantiation; they are not retained by the artifact.
6. Once `dispose()` is invoked, `#isDisposed === true`, and any subsequent access throws `SyncEngineError('ARTIFACT_DISPOSED')`.

---

### 2.3 Chunk Slice & Partitioned Snapshot

```typescript
/**
 * Metadata descriptor for a canonical chunk slice.
 */
export interface SnapshotChunkSlice {
  readonly chunkIndex: number;
  readonly totalChunks: number;
  readonly chunkSizeBytes: number;
  readonly chunkSha256: string;
}

/**
 * Immutable snapshot partition containing canonical chunk slices.
 */
export interface PartitionedSnapshot {
  readonly artifact: SerializedSnapshotArtifact;
  readonly nominalChunkSizeBytes: number;
  readonly totalChunks: number;
  readonly slices: ReadonlyArray<SnapshotChunkSlice>;

  /** Retrieves chunk slice metadata by index, or undefined if out of range. */
  getSlice(chunkIndex: number): SnapshotChunkSlice | undefined;

  /**
   * Package-private reader: returns a borrowed Uint8Array view of the slice bytes.
   * Accessible only to trusted recovery components during chunk framing.
   */
  getBorrowedSlice(chunkIndex: number): Uint8Array;
}
```

#### Invariants:
1. `slices.length === totalChunks`.
2. $1 \le \text{totalChunks} \le \text{MAX\_CHUNKS\_PER\_SNAPSHOT}$ (128).
3. $\text{nominalChunkSizeBytes} \in [\text{MIN\_RAW\_CHUNK\_BYTES}, \text{MAX\_CHUNK\_BYTES}]$ ([256, 32,768]).
4. $\text{totalChunks} === \lceil \text{totalSizeBytes} / \text{nominalChunkSizeBytes} \rceil$.
5. For all $i \in [0, \text{totalChunks} - 1)$: $\text{slices}[i].\text{chunkSizeBytes} === \text{nominalChunkSizeBytes}$.
6. Final chunk: $\text{slices}[\text{totalChunks} - 1].\text{chunkSizeBytes} === \text{totalSizeBytes} - (\text{totalChunks} - 1) \times \text{nominalChunkSizeBytes} \in (0, \text{nominalChunkSizeBytes}]$.
7. For each slice $i$, `getBorrowedSlice(i)` returns a borrowed view of $\text{artifact.getBorrowedSliceView}(i \cdot C, \text{slices}[i].\text{chunkSizeBytes})$.
8. For each slice $i$, $\text{chunkSha256} === \text{sha256}(\text{getBorrowedSlice}(i))$.
9. Canonical partition geometry is locked immutably upon creation and is **never** modified mid-transfer.

---

### 2.4 Outbound Message Preparation Types

```typescript
export interface OutboundOfferContext {
  readonly transferId: string;
  readonly supplierPeerId: string;
  readonly recipientPeerId: string;
  readonly membershipEpoch: number;
  readonly connectionGeneration?: number;
}

export interface OutboundChunkContext {
  readonly transferId: string;
  readonly supplierPeerId: string;
  readonly recipientPeerId: string;
  readonly membershipEpoch: number;
  readonly sctpMaxMessageSize: number;
  readonly connectionGeneration?: number;
}

export type OutboundChunkResult =
  | {
      readonly ok: true;
      readonly protocolMessage: ProtocolMessage<SnapshotChunkPayload>;
      readonly serializedJson: string;
      readonly byteLength: number;
    }
  | {
      readonly ok: false;
      readonly reason: SnapshotAbortReason;
      readonly detail: string;
    };
```

---

## 3. Component Contracts

### 3.1 SnapshotPreparationQueue
**Module:** `packages/sync-engine/src/recovery/snapshot-preparation-queue.ts`

```typescript
export class SnapshotPreparationQueue {
  public static readonly MAX_CONCURRENT_PREPARATIONS = 1;
  public static readonly MAX_PREPARATION_QUEUE_DEPTH = 4;

  /**
   * Enqueues an outbound snapshot preparation request.
   * If the queue is saturated (>= 4 requests waiting), throws/rejects immediately
   * with SyncEngineError('LOCAL_ABORT', 'Preparation queue saturated').
   * Guarantees that snapshot capture occurs inside the serialized gate.
   */
  public enqueue(
    request: QueuedPreparationRequest,
    captureAndSerializeFn: () => Promise<SerializedSnapshotArtifact> | SerializedSnapshotArtifact
  ): Promise<SerializedSnapshotArtifact>;

  public get depth(): number;
  public get isBusy(): boolean;
  public clear(): void;
}
```

---

### 3.2 SnapshotSerializer
**Module:** `packages/sync-engine/src/recovery/snapshot-serializer.ts`

```typescript
export class SnapshotSerializer {
  /**
   * Serializes a validated StateSnapshotPayloadV2 into a deterministic artifact.
   * Immediately dereferences preimage and JSON string upon master buffer allocation.
   * Throws SyncEngineError('SNAPSHOT_ERROR') if serialization fails or exceeds MAX_SNAPSHOT_BYTES.
   */
  public static serialize(snapshot: StateSnapshotPayloadV2): SerializedSnapshotArtifact;

  /**
   * Exports and serializes an active engine state into a deterministic artifact.
   */
  public static serializeFromEngine(
    state: PlaybackState,
    context: {
      sessionId: string;
      membershipEpoch: number;
      authorPeerId: string;
      snapshotFrontier: Record<string, number>;
      originFrontierTimestamps: Record<string, number>;
      compactedCheckpoints: ReadonlyArray<CompactedCheckpointItem>;
      snapshotId?: string;
    }
  ): SerializedSnapshotArtifact;
}
```

- **Thread-Safety & Synchronization:** Fully synchronous within the preparation gate.
- **Resource Usage:** Allocates a single `ArrayBuffer` of size $\le 4$ MiB.
- **Immediate Dereferencing:** Preimage object and canonical JSON string are dereferenced immediately.
- **No Side Effects:** Does not alter `PlaybackState`, clock managers, or operation logs.

---

### 3.3 SnapshotChunkProducer
**Module:** `packages/sync-engine/src/recovery/snapshot-chunk-producer.ts`

```typescript
export interface ChunkProducerConfig {
  /** Target nominal chunk size (bytes). If omitted, derived from sctpMaxMessageSize. */
  nominalChunkSizeBytes?: number;
  /** Transport message size limit. Used to calculate chunk capacity if nominalChunkSizeBytes omitted. */
  sctpMaxMessageSize?: number;
  /** Testing mock flag to allow 65,536 B fallback when sctpMaxMessageSize is unexposed. */
  allowSctpFallback?: boolean;
}

export class SnapshotChunkProducer {
  /**
   * Partitions a serialized snapshot artifact into canonical chunk slices.
   * Enforces admission invariants and zero-copy borrowed slice views.
   * Throws SyncEngineError('PARTITION_ERROR') on geometry or admission violation.
   */
  public static partition(
    artifact: SerializedSnapshotArtifact,
    config: ChunkProducerConfig = {}
  ): PartitionedSnapshot;
}
```

- **Zero-Copy Guarantee:** Uses `artifact.getBorrowedSliceView()` internally. Does not clone chunk payload memory.
- **Upfront Admission Gating:** Calls `calculateSnapshotAdmission()`; aborts immediately if required chunks exceed 128 or slice capacity $< 256$ B.

---

### 3.4 SnapshotOutboundPipeline
**Module:** `packages/sync-engine/src/recovery/snapshot-outbound-pipeline.ts`

```typescript
export class SnapshotOutboundPipeline {
  /**
   * Formats a SNAPSHOT_OFFER ProtocolMessage for transmission to a recipient peer.
   */
  public static createOfferMessage(
    partition: PartitionedSnapshot,
    context: OutboundOfferContext
  ): ProtocolMessage<SnapshotOfferPayload>;

  /**
   * Formats a SNAPSHOT_CHUNK ProtocolMessage with mandatory exact complete-message
   * UTF-8 byte length measurement at the send boundary.
   *
   * Invariant: Never shrinks an individual chunk slice. If the exact complete-message
   * UTF-8 byte length exceeds context.sctpMaxMessageSize, returns ok: false with
   * reason: 'TRANSPORT_LIMIT_EXCEEDED'.
   */
  public static prepareChunkMessage(
    partition: PartitionedSnapshot,
    chunkIndex: number,
    context: OutboundChunkContext
  ): OutboundChunkResult;
}
```

- **Exact Complete-Message Measurement:** Measures exact UTF-8 wire bytes via `new TextEncoder().encode(serializedJson).byteLength`.
- **Zero Chunk Shrinking:** Strictly maintains canonical partition geometry. Never resizes a chunk mid-transfer.
- **Transport Delivery Readiness:** Provides `serializedJson` ready to pass unchanged to `PeerManager.sendSerialized()`.

---

## 4. Error Handling & Failure Semantics

1. **Size Exceedance:** If exported snapshot $> 4,194,304$ bytes $\implies$ `SNAPSHOT_ERROR`.
2. **Unsupported Slicing Limit:** If calculated chunk capacity $< 256$ bytes $\implies$ `TRANSPORT_LIMIT_EXCEEDED`.
3. **Partition Count Ceiling:** If required chunks $> 128 \implies$ `CHUNK_COUNT_EXCEEDED`.
4. **Queue Saturation:** If $\ge 4$ requests in preparation queue $\implies$ `LOCAL_ABORT` ('Preparation queue saturated').
5. **Artifact Disposed:** Method call on disposed artifact $\implies$ `ARTIFACT_DISPOSED`.
6. **Send-Boundary Overshoot:** If exact UTF-8 complete-message size exceeds `sctpMaxMessageSize` $\implies$ `TRANSPORT_LIMIT_EXCEEDED`.
7. **No State Pollution:** All failure paths terminate synchronously with zero modifications to engine state or logs.
