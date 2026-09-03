package protocol

import (
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
)

// relayCandidate is one connected, established peer's relay-relevant state,
// snapshotted by PeerManager.RelayBlock before selectRelayTargets runs. It
// carries no lock and no I/O of its own; see selectRelayTargets's doc
// comment for why the split exists.
type relayCandidate struct {
	peer *Peer

	// wantsHeaders is PeerInfo.WantsHeaders (net_processing.cpp
	// state->fPreferHeaders), read via Peer.WantsHeaders() before syncMu is
	// taken — the package's lock order forbids calling a Peer method while a
	// manager lock is held (see the note on PeerManager.syncMu).
	wantsHeaders bool

	// hasBlock reports that this peer already has this exact block, so no
	// announcement of any kind is worth sending it. This is the FULL
	// net_processing.cpp PeerHasHeader test (net_processing.cpp:314-327,
	// bitcoin-sv@879fc8b42), computed by the caller (RelayBlock) as EITHER of:
	//
	//   - peerHasHeader(idx, state, hash): PeerHasHeader's own two ancestor
	//     tests — pindexBestKnownBlock (what the peer told US, via inv or
	//     headers) or pindexBestHeaderSent (what WE already told the peer,
	//     via a getheaders reply or an earlier relay headers announcement);
	//   - peerSyncState.knownBlocks.has(hash): this port's stand-in for what
	//     neither ancestor test can see — a block WE already relayed to this
	//     peer by plain INV. Neither pindexBestKnownBlock nor
	//     pindexBestHeaderSent is updated by our own outbound inv sends (only
	//     PushMessage(HEADERS) touches pindexBestHeaderSent), so without this
	//     the duplicate-inv case would slip through PeerHasHeader entirely.
	//
	// SendBlockHeaders re-tests PeerHasHeader before EVERY send, not only the
	// headers branch: the inv fallback re-checks it immediately before
	// PushBlockInventory (net_processing.cpp:5453-5455, "If the peer's chain
	// has this block, don't inv it back"). hasBlock true suppresses BOTH
	// branches below for exactly that reason — fix round 1, review finding
	// I1 (headers) and Minor 2 (inv): the first implementation of this field
	// only ever fed the headers branch and only ever consulted the
	// knownBlocks half.
	hasBlock bool

	// hasParent reports that this peer already has the block's PARENT (or
	// the block is genesis), the per-hash connectivity test SendBlockHeaders
	// runs before it will commit to the headers branch:
	// `pindex->IsGenesis() || PeerHasHeader(state, pindex->GetPrev())`
	// (net_processing.cpp:5301-5307). A peer missing the parent cannot place
	// the header we would send — "nothing will connect" — so
	// SendBlockHeaders falls back to inv instead of announcing an orphan
	// (net_processing.cpp:5308-5312 sets fRevertToInv; the corresponding inv
	// send is the same PeerHasHeader-gated push cited on hasBlock above).
	// Meaningless when hasBlock is already true or wantsHeaders is false.
	// Fix round 1, review finding I2: the first implementation sent headers
	// to any wantsHeaders candidate with no parent test at all.
	hasParent bool
}

// relayDecision is one peer's announcement, chosen by selectRelayTargets.
type relayDecision struct {
	peer *Peer
	msg  wire.Message
}

// selectRelayTargets is net_processing.cpp SendBlockHeaders (:5224+,
// bitcoin-sv@879fc8b42) reduced to a single block — this port relays one
// blocks-final event at a time, so the batching, ascending-height sort and
// multi-hash chain-connectivity walk SendBlockHeaders needs for a LIST of
// hashes (net_processing.cpp:5273-5314) have nothing to do here — and it is
// the legacy Go service's own split of the resulting decision:
// handleRelayBlockMsg / handleRelayBlockInvMsg
// (services/legacy/peer_server.go:2603, :2622), dispatched by
// handleRelayInvMsg (:2516): a peer that sent sendheaders and can connect
// the header gets a headers message; everyone else — no sendheaders, or
// sendheaders but missing the parent — gets a plain block inv (the PR 1554
// fallback, which is also SVNode's own fRevertToInv fallback). hasBlock and
// hasParent on relayCandidate are the per-hash halves of PeerHasHeader and
// the connectivity test that a multi-hash batch would otherwise need a loop
// for; see relayCandidate's doc comment for both, including why the first
// implementation of this function got each one wrong (review findings I1,
// I2, Minor 2).
//
// It performs no I/O and takes no lock: the caller (PeerManager.RelayBlock)
// snapshots every candidate field under the locks that own it before calling
// this, and sends the result with no lock held, matching every other machine
// in this package (spec §4.3).
func selectRelayTargets(candidates []relayCandidate, hash chainhash.Hash, header *wire.BlockHeader) []relayDecision {
	var out []relayDecision

	for _, c := range candidates {
		// PeerHasHeader gates BOTH branches below, not only the headers one
		// (net_processing.cpp:5453-5455) — see relayCandidate.hasBlock.
		if c.hasBlock {
			continue
		}

		if c.wantsHeaders && c.hasParent {
			msg := wire.NewMsgHeaders()
			if err := msg.AddBlockHeader(header); err != nil {
				// header is a single 80 byte block header; AddBlockHeader
				// only refuses past MaxBlockHeadersPerMsg (2000), so this
				// cannot fail. Not dropped silently in case that changes.
				continue
			}

			out = append(out, relayDecision{peer: c.peer, msg: msg})

			continue
		}

		// Either the peer never negotiated sendheaders, or it did but
		// cannot connect this header yet (hasParent false): SendBlockHeaders'
		// own fRevertToInv fallback covers both — the unconditional
		// non-headers-preferring branch (net_processing.cpp:5429-5431,
		// `else { fRevertToInv = true; }`) and the no-connect bailout
		// (:5308-5312) land in exactly the same inv send.
		msg := wire.NewMsgInv()
		if err := msg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)); err != nil {
			// A single-entry inv only fails past wire.MaxInvPerMsg; unreachable
			// here, kept for the same reason as above.
			continue
		}

		out = append(out, relayDecision{peer: c.peer, msg: msg})
	}

	return out
}

// peerHasHeader mirrors net_processing.cpp PeerHasHeader in full
// (net_processing.cpp:314-327, bitcoin-sv@879fc8b42): true if hash is on a
// chain this peer is already known to hold, via EITHER of CNodeState's two
// "does the peer have this" trackers.
//
//   - pindexBestKnownBlock: the best block the peer has ANNOUNCED to us —
//     via inv (blockdownload.go OnInv, updateBlockAvailability) or via
//     headers (headersync.go OnHeaders, updateBlockAvailability). This is
//     the half review finding I1 was about: the first implementation of
//     this relay never consulted it at all.
//   - pindexBestHeaderSent: the last header WE told the peer about, via a
//     getheaders reply (Serving.OnGetHeaders) or an earlier relay headers
//     announcement (RelayBlock, after this function is used to decide one).
//
// hash's ancestor at each candidate's own height is compared for equality,
// exactly as CBlockIndex::GetAncestor-based PeerHasHeader does; a candidate
// hash not in the index at all cannot be covered by either, since both
// trackers only ever point at indexed nodes.
//
// Requires PeerManager.syncMu: it reads the header index (whose own reads
// need only its own lock, HeaderIndex's doc comment) and peerSyncState
// (syncMu-guarded).
func peerHasHeader(idx *HeaderIndex, state *peerSyncState, hash chainhash.Hash) bool {
	if idx == nil || state == nil {
		return false
	}

	node, ok := idx.Lookup(hash)
	if !ok {
		return false
	}

	if ancestorMatches(idx, state.pindexBestKnownBlock, node) {
		return true
	}

	return ancestorMatches(idx, state.pindexBestHeaderSent, node)
}

// ancestorMatches reports whether best's ancestor at node's height is node
// itself — the shared shape of PeerHasHeader's two branches
// (net_processing.cpp:317-321 and :325-327). best == nil answers false,
// mirroring the C++ nullptr checks on both CNodeState fields.
func ancestorMatches(idx *HeaderIndex, best *HeaderNode, node HeaderNode) bool {
	if best == nil {
		return false
	}

	ancestor, ok := idx.Ancestor(best.Hash, node.Height)
	if !ok {
		return false
	}

	return ancestor.Hash == node.Hash
}

// TxHashAndFee is one transaction the tx announcement relay
// (manager.go RelayTxs, this file's selectTxRelayTargets) has been asked to
// announce — legacy netsync's own TxHashAndFee ported field for field
// (services/legacy/netsync/manager.go:361-365): TxHash to identify it, Fee
// (satoshis) and Size (serialized bytes) to test against a peer's feefilter.
type TxHashAndFee struct {
	TxHash chainhash.Hash
	Fee    uint64
	Size   uint64
}

// txRelayCandidate is one connected, established peer's tx-relay-relevant
// state, snapshotted by PeerManager.RelayTxs before selectTxRelayTargets
// runs — the tx-relay counterpart of relayCandidate; see that type's doc
// comment for why the snapshot/decide split exists (no I/O, no lock, inside
// the selection function itself).
type txRelayCandidate struct {
	peer *Peer

	// relayDisabled is Peer.RelayTxDisabled (PeerInfo.DisableRelayTx): a
	// peer that negotiated fRelayTxes=false gets no tx announcements at
	// all, mirroring legacy's own relayTxDisabled gate
	// (services/legacy/peer_server.go handleRelayInvMsg).
	relayDisabled bool

	// feeFilter is Peer.FeeFilter (PeerInfo.FeeFilter): the peer's last
	// announced minimum relay fee rate in satoshis/kB, or 0 for "no
	// filter". See selectTxRelayTargets for the feePerKB comparison this
	// gates, mirroring legacy's own handleRelayTxMsg
	// (services/legacy/peer_server.go:2565-2591).
	feeFilter int64

	// known reports that this exact tx has already been relayed to this
	// peer (peerSyncState.knownTxs — see that field's own doc comment for
	// why this is a "did WE already send it" set, not a "does the peer
	// already have it" one, unlike knownBlocks).
	known bool
}

// selectTxRelayTargets is net_processing.cpp SendTxnInventory (:5464,
// bitcoin-sv@879fc8b42) reduced to a single transaction, mirroring
// selectRelayTargets' own single-hash reduction of SendBlockHeaders for the
// same reason: this port relays one flushed batcher entry at a time, not a
// per-peer accumulated list — and it is legacy's own decision order at
// handleRelayTxMsg (services/legacy/peer_server.go:2565-2591), called from
// handleRelayInvMsg's InvTypeTx branch (:2534-2549):
//
//  1. a peer with fRelayTxes=false (relayDisabled) gets nothing, ever —
//     checked before the feefilter, matching legacy's own ordering
//     (handleRelayInvMsg tests relayTxDisabled first);
//  2. a peer this tx has already been relayed to (known) gets nothing
//     again — this port's dedup, doing the job legacy's QueueInventory /
//     knownInventory.Exists does at the send site (peer_server.go:2487-2490);
//  3. a positive feeFilter suppresses the tx when its fee-per-kB falls
//     below the filter, computed exactly as legacy does: fee*1000/size
//     rounding down (integer division), with a MaxInt64 sentinel when size
//     is 0 or the uint64->int64 conversion overflows, so a filter can never
//     suppress a tx it cannot rate — mirrors handleRelayTxMsg's own
//     feePerKB default and its safeconversion error handling exactly.
//
// It performs no I/O and takes no lock: the caller (PeerManager.RelayTxs)
// snapshots every candidate field under the locks that own it before
// calling this, and sends the result with no lock held, matching
// selectRelayTargets/RelayBlock's own split (spec §4.3).
func selectTxRelayTargets(candidates []txRelayCandidate, tx TxHashAndFee) []*Peer {
	var out []*Peer

	for _, c := range candidates {
		if c.relayDisabled || c.known {
			continue
		}

		if c.feeFilter > 0 && feePerKB(tx) < c.feeFilter {
			continue
		}

		out = append(out, c.peer)
	}

	return out
}

// feePerKB computes tx's fee rate in satoshis per 1000 bytes, rounding down
// via integer division — legacy's own computation at handleRelayTxMsg
// (services/legacy/peer_server.go:2565-2591). Returns math.MaxInt64 (legacy's
// own sentinel, "so it can never be suppressed by the filter check") when
// Size is 0 (division by zero) or either field overflows int64 on
// conversion — legacy uses safeconversion.Uint64ToInt64 and falls back to
// the same sentinel on its error return; a tx this port cannot rate is
// never suppressed, matching that fail-open choice exactly.
func feePerKB(tx TxHashAndFee) int64 {
	if tx.Size == 0 {
		return math.MaxInt64
	}

	fee, err := safeconversion.Uint64ToInt64(tx.Fee)
	if err != nil {
		return math.MaxInt64
	}

	size, err := safeconversion.Uint64ToInt64(tx.Size)
	if err != nil {
		return math.MaxInt64
	}

	return fee * 1000 / size
}
