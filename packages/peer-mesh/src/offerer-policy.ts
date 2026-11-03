/**
 * Deterministic initial offerer policy.
 *
 * Per PEER_MESH_RTT_ENGINE_LLD §3:
 * "Only the designated initiator creates the initial offer for each peer pair."
 *
 * The peer with the lexicographically smaller peerId is the initial offerer.
 * This eliminates negotiation to decide who offers first and prevents
 * simultaneous-offer glare in the common case.
 *
 * For renegotiation glare (both peers send offers simultaneously),
 * the non-designated offerer should roll back and accept the offer.
 */
import type { PeerId } from '@syncplay/protocol';

/**
 * Determine whether the local peer should be the initial offerer
 * for a connection with the given remote peer.
 *
 * @param localPeerId - This peer's ID
 * @param remotePeerId - The remote peer's ID
 * @returns true if localPeerId should create the initial offer
 */
export function isInitialOfferer(localPeerId: PeerId, remotePeerId: PeerId): boolean {
  if (localPeerId === remotePeerId) {
    throw new Error('Cannot determine offerer for self-connection');
  }
  return localPeerId < remotePeerId;
}

/**
 * Resolve simultaneous-offer glare.
 *
 * When both peers send offers at the same time (renegotiation glare),
 * the designated offerer's offer wins. The non-designated offerer
 * should roll back its local description and process the incoming offer.
 *
 * @returns true if the local peer's offer should win (i.e., local is the designated offerer)
 */
export function shouldLocalOfferWinGlare(localPeerId: PeerId, remotePeerId: PeerId): boolean {
  return isInitialOfferer(localPeerId, remotePeerId);
}
