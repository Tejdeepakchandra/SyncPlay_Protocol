/**
 * Deterministic total-order comparator for SyncPlay control operations.
 *
 * Implements Phase 7 specifications:
 * - PHASE7_2_LLD.md §1.1 & §1.2
 *
 * Total Ordering Key: <logicalTimestamp, originPeerId, logicalEventId>
 */

export interface OperationOrderingKey {
  readonly logicalTimestamp: number;
  readonly originPeerId: string;
  readonly logicalEventId: string;
}

/**
 * Strict weak total-order relation:
 * 1. Primary order: Lamport logical timestamp (ascending).
 * 2. Secondary order: Deterministic lexicographical originPeerId tie-breaker.
 * 3. Tertiary order: Stable logicalEventId (breaks rare same-tick same-peer collisions).
 *
 * Returns:
 *  - negative if e1 < e2
 *  - positive if e1 > e2
 *  - 0 if e1 is identical to e2
 */
export function compareOperations(
  e1: OperationOrderingKey,
  e2: OperationOrderingKey,
): number {
  if (e1.logicalTimestamp !== e2.logicalTimestamp) {
    return e1.logicalTimestamp < e2.logicalTimestamp ? -1 : 1;
  }

  if (e1.originPeerId !== e2.originPeerId) {
    return e1.originPeerId < e2.originPeerId ? -1 : 1;
  }

  if (e1.logicalEventId !== e2.logicalEventId) {
    return e1.logicalEventId < e2.logicalEventId ? -1 : 1;
  }

  return 0;
}
