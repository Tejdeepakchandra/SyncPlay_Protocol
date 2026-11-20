# Phase 7.5 M4.2 — Interface Specification: Snapshot Serialization & Outbound Pipeline

**Document Version:** 1.0.0  
**Status:** Approved Specification  
**Frozen Baseline:** `fd23edae5bdb52cff32674860229277a79cb1aca`  
**Specification Target:** `@syncplay/sync-engine/recovery`  

---

## 1. Overview & Architecture Placement

This specification establishes the contracts, data structures, and operational boundaries for the M4.2 outbound snapshot transfer pipeline.

```mermaid
classDiagram
    class SnapshotSerializer {
        +serialize(snapshot: StateSnapshotPayloadV2): SerializedSnapshotArtifact
        +serializeFromEngine(state: PlaybackState, context: SnapshotContext): SerializedSnapshotArtifact
    }

    class SerializedSnapshotArtifact {
        +string snapshotId
        +string sessionId
        +number membershipEpoch
        +string authorPeerId
        +Uint8Array rawBytes
        +number totalSizeBytes
        +string sha256Digest
        +StateSnapshotPayloadV2 snapshot
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
    }

    class SnapshotChunkSlice {
        +number chunkIndex
        +number totalChunks
        +number chunkSizeBytes
        +string chunkSha256
        +Uint8Array sliceView
    }

    class SnapshotOutboundPipeline {
        +createOffer(partition: PartitionedSnapshot, context: OutboundContext): ProtocolMessage~SnapshotOfferPayload~
        +prepareChunk(partition: PartitionedSnapshot, chunkIndex: number, context: OutboundContext): OutboundChunkResult
    }

    SnapshotSerializer --> SerializedSnapshotArtifact : produces
    SnapshotChunkProducer --> PartitionedSnapshot : produces
    PartitionedSnapshot *-- SnapshotChunkSlice : contains
    SnapshotOutboundPipeline ..> PartitionedSnapshot : consumes
```

---

## 2. Core Data Structures & Interfaces

### 2.1 Serialized Snapshot Artifact

```typescript
/**
 * Immutable artifact holding canonical, deterministically serialized snapshot bytes.
 * Guaranteed to conform to MAX_SNAPSHOT_BYTES (4 MiB) and RFC 8785 JCS.
 */
export interface SerializedSnapshotArtifact {
  readonly snapshotId: string;
  readonly sessionId: string;
  readonly membershipEpoch: number;
  readonly authorPeerId: string;
  readonly totalSizeBytes: number;
  readonly sha256Digest: string;
  readonly rawBytes: Uint8Array;
  readonly snapshot: Readonly<StateSnapshotPayloadV2>;
}
```

#### Invariants:
1. `rawBytes.byteLength === totalSizeBytes`.
2. `totalSizeBytes > 0 && totalSizeBytes <= MAX_SNAPSHOT_BYTES` (4,194,304 bytes).
3. `sha256Digest === sha256(rawBytes)`.
4. `rawBytes` contains the exact UTF-8 byte encoding of `canonicalizeJson(preimage)`.

---

### 2.2 Chunk Slice & Partitioned Snapshot

```typescript
/**
 * Zero-copy slice view over the contiguous snapshot buffer.
 */
export interface SnapshotChunkSlice {
  readonly chunkIndex: number;
  readonly totalChunks: number;
  readonly chunkSizeBytes: number;
  readonly chunkSha256: string;
  readonly sliceView: Uint8Array; // Zero-copy subarray view
}

/**
 * Immutable snapshot partition containing canonical chunk slices.
 */
export interface PartitionedSnapshot {
  readonly artifact: SerializedSnapshotArtifact;
  readonly nominalChunkSizeBytes: number;
  readonly totalChunks: number;
  readonly slices: ReadonlyArray<SnapshotChunkSlice>;

  /** Retrieves a chunk slice by index, or undefined if out of range. */
  getSlice(chunkIndex: number): SnapshotChunkSlice | undefined;
}
```

#### Invariants:
1. `slices.length === totalChunks`.
2. $1 \le \text{totalChunks} \le \text{MAX\_CHUNKS\_PER\_SNAPSHOT}$ (128).
3. $\text{nominalChunkSizeBytes} \in [\text{MIN\_RAW\_CHUNK\_BYTES}, \text{MAX\_CHUNK\_BYTES}]$ ([256, 32,768]).
4. $\text{totalChunks} === \lceil \text{totalSizeBytes} / \text{nominalChunkSizeBytes} \rceil$.
5. For all $i \in [0, \text{totalChunks} - 1)$: $\text{slices}[i].\text{chunkSizeBytes} === \text{nominalChunkSizeBytes}$.
6. Final chunk: $\text{slices}[\text{totalChunks} - 1].\text{chunkSizeBytes} === \text{totalSizeBytes} - (\text{totalChunks} - 1) \times \text{nominalChunkSizeBytes} \in (0, \text{nominalChunkSizeBytes}]$.
7. For each slice $i$, $\text{sliceView}$ is an identical zero-copy view of $\text{artifact.rawBytes.subarray}(i \cdot C, \min(S, (i + 1) \cdot C))$.
8. For each slice $i$, $\text{chunkSha256} === \text{sha256}(\text{sliceView})$.

---

### 2.3 Outbound Message Preparation Types

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
      readonly iterations: number;
    }
  | {
      readonly ok: false;
      readonly reason: SnapshotAbortReason;
      readonly detail: string;
    };
```

---

## 3. Component Contracts

### 3.1 SnapshotSerializer
**Module:** `packages/sync-engine/src/recovery/snapshot-serializer.ts`

```typescript
export class SnapshotSerializer {
  /**
   * Serializes a validated StateSnapshotPayloadV2 into a deterministic artifact.
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

- **Thread-Safety & Synchronization:** Fully synchronous.
- **Resource Usage:** Allocates a single `Uint8Array` of size $\le 4$ MiB.
- **No Side Effects:** Does not alter `PlaybackState`, clock managers, or operation logs.

---

### 3.2 SnapshotChunkProducer
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
   * Enforces admission invariants and zero-copy memory slicing.
   * Throws SyncEngineError('PARTITION_ERROR') on geometry or admission violation.
   */
  public static partition(
    artifact: SerializedSnapshotArtifact,
    config: ChunkProducerConfig = {}
  ): PartitionedSnapshot;
}
```

- **Zero-Copy Guarantee:** Uses `artifact.rawBytes.subarray()` exclusively. Does not clone chunk payload memory.
- **Upfront Admission Gating:** Calls `calculateSnapshotAdmission()`; aborts immediately if required chunks exceed 128.

---

### 3.3 SnapshotOutboundPipeline
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
   * Formats a SNAPSHOT_CHUNK ProtocolMessage with normative 7-step iterative sizing.
   * Guarantees that the resulting serialized JSON string byte length does not exceed
   * context.sctpMaxMessageSize.
   */
  public static prepareChunkMessage(
    partition: PartitionedSnapshot,
    chunkIndex: number,
    context: OutboundChunkContext
  ): OutboundChunkResult;
}
```

- **Iterative Sizing Compliance:** Enforces `MAX_SIZING_ITERATIONS = 5`.
- **Exact Measurement:** Measures exact byte length via `new TextEncoder().encode(serializedJson).byteLength`.
- **Transport Delivery Readiness:** Provides `serializedJson` ready to pass unchanged to `PeerManager.sendSerialized()`.

---

## 4. Error Handling & Failure Semantics

1. **Size Exceedance:** If exported snapshot $> 4,194,304$ bytes $\implies$ `SNAPSHOT_ERROR`.
2. **Unsupported Slicing Limit:** If calculated chunk capacity $< 256$ bytes $\implies$ `TRANSPORT_LIMIT_EXCEEDED`.
3. **Partition Count Ceiling:** If required chunks $> 128 \implies$ `CHUNK_COUNT_EXCEEDED`.
4. **Sizing Non-Convergence:** If 5 iterations fail to bring serialized message $\le \text{effectiveLimit} \implies$ `TRANSPORT_LIMIT_EXCEEDED`.
5. **No State Pollution:** All failure paths terminate synchronously with zero modifications to engine state or logs.
