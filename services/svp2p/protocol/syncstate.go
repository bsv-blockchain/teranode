package protocol

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

// requestedTxnsTTL mirrors legacy's own per-peer requestedTxns construction
// (netsync/manager.go:990, "allow the node 10 seconds to respond to the tx
// request") — the same TTL BlockDownloader's global requestedTxns uses
// (blockdownload.go), so a tx this peer was asked for is only ever
// remembered as "already requested" for 10 seconds, matching legacy on
// both the per-peer and the node-wide map.
const requestedTxnsTTL = 10 * time.Second

// peerSyncState is the net_processing.cpp CNodeState port: per-peer sync
// bookkeeping consumed by headers-first sync (Task 5) and block download
// scheduling (Task 6). Phase 2 keeps only the fields those two consumers
// need; the rest of CNodeState (pindexBestHeaderSent, nUnconnectingHeaders,
// and the block-download bookkeeping beyond the in-flight count) is not
// carried here and is a later-phase candidate if a consumer needs it.
//
// Locking: peerSyncState carries no lock of its own. SVNode guards the
// equivalent mapNodeState entries with cs_main; this port's plan (spec §11,
// Task 11) instead gives PeerManager one shared sync-state mutex covering
// every peer's state, so every method here assumes the caller already holds
// that mutex. This mirrors cs_main's role faithfully: one coarse lock
// serializing sync-state mutation, just scoped to this subsystem instead of
// the whole node.
type peerSyncState struct {
	// pindexBestKnownBlock mirrors CNodeState::pindexBestKnownBlock: the best
	// known block we know this peer has announced. nil mirrors the C++
	// nullptr — we don't have a known best block for this peer yet. The
	// comparison is SVNode's own nullptr-or-not-lower test against
	// nChainWork; Phase 2 compared Height instead because the header index
	// carried no work, and Phase 3 Task 1 restored the work compare with the
	// index field. HeaderNode is a value snapshot returned by
	// HeaderIndex.Lookup at the moment it was read, not a live pointer into
	// the index's tree: a later Lookup of the same hash returns an
	// equal-by-value HeaderNode, never the same address, so pointer equality
	// against a fresh Lookup result is meaningless here.
	pindexBestKnownBlock *HeaderNode

	// hashLastUnknownBlock mirrors CNodeState::hashLastUnknownBlock: the hash
	// of the last unknown block this peer has announced. The zero
	// chainhash.Hash mirrors the C++ uint256().SetNull() sentinel used by
	// IsNull().
	hashLastUnknownBlock chainhash.Hash

	// pindexLastCommonBlock mirrors CNodeState::pindexLastCommonBlock: the last
	// block on this peer's branch that we already have, which is where block
	// download resumes and where the download window starts. nil mirrors the
	// C++ nullptr and makes FindNextBlocksToDownload bootstrap it from our own
	// chain. Like pindexBestKnownBlock this is a HeaderNode value snapshot, not
	// a live pointer into the index tree, so pointer equality against a fresh
	// Lookup result is meaningless; compare Hash. Populated and consumed by
	// block download scheduling (Task 6); not touched here.
	pindexLastCommonBlock *HeaderNode

	// reanchorGen is the BlockDownloader.reanchorGen value this peer has
	// already acted on. It is how a release of a block we had recorded as held
	// reaches every peer's own anchor without the downloader holding a peer
	// registry: the walk compares the two counters and drops a stale anchor
	// once per release. See BlockDownloader.reanchorHeight.
	reanchorGen uint64

	// nBlocksInFlight mirrors CNodeState::nBlocksInFlight: the number of
	// blocks currently being downloaded from this peer. Populated and
	// consumed by block download scheduling (Task 6); not touched here.
	nBlocksInFlight int

	// vBlocksInFlight mirrors CNodeState::vBlocksInFlight (node_state.h:80)
	// reduced to what the timeout needs: the hashes this peer owes us, in the
	// order they were requested. Only the FRONT entry is ever read —
	// DetectStalling names it in the timeout log line and nDownloadingSince
	// measures it — but the whole order has to be kept, because which entry is
	// the front changes as blocks arrive out of order. The C++ list carries a
	// QueuedBlock per entry (the CBlockIndex pointer and the partial-block
	// buffer for compact blocks); neither has a consumer in this port.
	//
	// It is a slice rather than a linked list: removals are O(n) over a list
	// capped at MaxBlocksInTransitPerPeer, which is 16.
	vBlocksInFlight []chainhash.Hash

	// nDownloadingSince mirrors CNodeState::nDownloadingSince
	// (node_state.h:81-83): "When the first entry in vBlocksInFlight started
	// downloading. Don't care when vBlocksInFlight is empty." In microseconds
	// since the Unix epoch. It is armed when the peer's in-flight queue goes
	// from empty to one entry, and re-armed when the front entry leaves the
	// queue, so it always measures the block at the head.
	nDownloadingSince int64

	// nCarriedDownloadingSince holds the nDownloadingSince a sync-peer rotation
	// cleared while blocks were still owed. SVNode never zeroes the clock short
	// of a disconnect; legacy rotation (clearRequestedState) does, and on a
	// re-hand that let a block-withholding peer restart the per-block timeout
	// every rotation for ever (Task 27 finding 2, 2026-08-26). The next
	// MarkBlockAsInFlight that opens a batch inherits this clock instead of the
	// current time; a delivery (BlockReceived) clears it, because a peer that
	// delivers has paid the debt.
	nCarriedDownloadingSince int64

	// fSyncStarted mirrors CNodeState::fSyncStarted: whether we've started
	// headers synchronization with this peer. Populated and consumed by
	// headers-first sync (Task 5); not touched here.
	fSyncStarted bool

	// nStallingSince mirrors CNodeState::nStallingSince: since when we're
	// stalling block download progress, in microseconds since the Unix
	// epoch, or 0 when not stalling. Populated and consumed by block download
	// scheduling (Task 6); not touched here.
	nStallingSince int64

	// nIngestBytesLastSample and nIngestSampleMicros have no CNodeState
	// counterpart either. They carry legacy netsync manager.go
	// syncPeerState.assocReadBytesLastTick and the tick it was taken on: the
	// previous sample of how far the block this peer is ingesting has got,
	// which is what lets CheckStall compute a byte rate and keep a peer that
	// is still delivering a large block. Both are zero when no ingest is
	// running. Populated and consumed by block download scheduling (Task 6).
	nIngestBytesLastSample uint64
	nIngestSampleMicros    int64

	// nIngestBytesPerSec and nIngestRateMicros have no CNodeState counterpart
	// either. They are this port's stand-in for what SVNode reads off the
	// association: CAssociation::GetAverageBandwidth(BLOCK), the per-peer block
	// delivery rate IsBlockDownloadStallingFromPeer tests
	// (net_processing.cpp:105-109). Two consumers need that rate at moments when
	// no IngestSnapshot is to hand — the parallel-fetch branch asks it of every
	// holder of a block, not just the peer being walked — so CheckStall computes
	// it once per peer per tick and leaves it here.
	//
	// nIngestRateMicros is when it was computed, which is what lets a reader tell
	// a rate of zero from a rate nobody has measured lately. Both are zero when
	// no ingest has been sampled.
	nIngestBytesPerSec uint64
	nIngestRateMicros  int64

	// nLastProgressTime has no CNodeState counterpart. It carries legacy
	// netsync manager.go syncPeerState.lastBlockTime, the clock the Teranode
	// sync-peer rotation measures against maxLastBlockTime (PR 1067), in
	// microseconds since the Unix epoch. 0 means no progress has been observed
	// yet; the first stall check seeds it. Populated and consumed by block
	// download scheduling (Task 6); not touched here.
	nLastProgressTime int64

	// pindexBestHeaderSent mirrors CNodeState::pindexBestHeaderSent
	// (node_state.h): the last header we told this peer about, via a
	// getheaders reply OR a relay headers announcement. Serving.OnGetHeaders's
	// doc comment named this field as its own missing counterpart, deferred
	// to "the relay path (Task 12)" — this is that task. nil mirrors the C++
	// nullptr: neither has happened yet for this peer.
	//
	// Written in two places, matching the two places net_processing.cpp
	// writes it:
	//
	//   - Serving.OnGetHeaders, ProcessGetHeadersMessage's unconditional reset
	//     (net_processing.cpp:3044, bitcoin-sv@879fc8b42): "It is important
	//     that we simply reset the BestHeaderSent value here, and not
	//     max(BestHeaderSent, newHeaderSent)" — every getheaders reply
	//     overwrites it, never merges with what was there.
	//   - PeerManager.RelayBlock, SendBlockHeaders's own write right after it
	//     pushes the plain HEADERS message (net_processing.cpp:5372-5373 —
	//     verified by grepping every pindexBestHeaderSent site in the file
	//     rather than trusting a remembered line number; :5357 is the
	//     sibling compact-block branch this port does not take, Phase 4).
	//     A relay announcement is exactly as much "telling the peer about a
	//     header" as a getheaders reply is. This write is its OWN defect,
	//     not part of I1/I2's root cause: I1 was the dropped
	//     pindexBestKnownBlock branch and I2 was the absent parent test,
	//     both read-side; this write is what I2's parent test needs in
	//     order to see a PRIOR relay's headers send when it decides the
	//     NEXT block's hasParent (fix round 2, review Minor 2 correction).
	//
	// Read by the block announcement relay (relay.go peerHasHeader), which is
	// half of net_processing.cpp PeerHasHeader — see that function's doc
	// comment for the other half, pindexBestKnownBlock. This is its only
	// consumer; peerSyncState carries no field without one (see the file doc
	// comment above).
	pindexBestHeaderSent *HeaderNode

	// knownBlocks is this peer's known-block set: legacy peer.go's
	// knownInventory (mruInventoryMap, AddKnownInventory/QueueInventory.Exists)
	// reduced to blocks only, since this port relays no other inventory type
	// yet. It covers exactly what neither pindexBestKnownBlock nor
	// pindexBestHeaderSent can see: a block WE already relayed to this peer
	// by plain INV, which touches neither field (see relay.go relayCandidate's
	// doc comment on hasBlock). Combined with those two fields by the relay
	// (relay.go, manager.go RelayBlock), it is what makes "never announce to
	// the originating peer" and "never announce the same block twice" the
	// same rule: whichever peer told us about a hash first (an inv, or the
	// block body itself) has that hash marked here before the relay for it
	// ever runs (manager.go Inv, BlockDone), and the relay marks it for
	// whoever it sends the announcement to.
	//
	// Sized for the STEADY-STATE case only — see knownBlockCap's own doc
	// comment for why a single large inv batch can consume the whole cap in
	// one call, which is a real limitation of a count-based bound, not a
	// time-based one.
	knownBlocks *knownBlockSet

	// knownTxs is this peer's known-tx set — the SAME bookkeeping role
	// knownBlocks plays for blocks, but for transactions relayed by the tx
	// announcement relay (relay.go selectTxRelayTargets, manager.go
	// RelayTxs), and deliberately its OWN set rather than knownBlocks
	// itself: knownBlocks is documented as blocks-only by design (Task 12),
	// sized at knownBlockCap (288) against a once-every-ten-minutes rate
	// that a tx-scale cap would blow through in seconds. See knownTxCap's
	// own doc comment for the E3 sizing argument.
	//
	// Like knownBlocks, this set now has TWO writers (Task 16 added the
	// second): RelayTxs marks a hash for every peer it actually sends an
	// inv to (the "did WE already relay this tx to this peer" half), and
	// BlockDownloader.RequestTxs marks it for the peer that just announced
	// the hash back to us over the tx-inv round trip (the "peer already
	// told us they have it" half) — legacy's own peer.AddKnownInventory for
	// tx invs (netsync/manager.go:2371, run unconditionally once the
	// RUNNING gate has passed, BEFORE the headers-first check; see
	// RequestTxs' own doc comment for why that order matters). Before Task
	// 16 there was no peer-originated tx-inv path in this port at all, so
	// this set answered only the first half; it now answers both, the same
	// two-writer shape knownBlocks already had via Inv/BlockDone and
	// RelayBlock.
	knownTxs *knownBlockSet

	// requestedTxns is this peer's half of the tx-inv round trip's dedup
	// (Task 16, manager.go:2340-2347's `state.requestedTxns`, the per-peer
	// twin of BlockDownloader.requestedTxns below): a tx hash lands here the
	// moment a getdata for it goes out to THIS peer, so a second inv for the
	// same hash from the same peer inside the TTL window is not requested
	// again. Stopped on disconnect (blockdownload.go clearPeer), mirroring
	// legacy's own state.requestedTxns.Stop() at DonePeer
	// (netsync/manager.go:1140).
	requestedTxns *expiringmap.ExpiringMap[chainhash.Hash, struct{}]

	// fProvidesHeaderAndIDs mirrors CNodeState::fProvidesHeaderAndIDs
	// (net_processing.cpp ProcessSendCompactMessage:2400-2404, "used to 'lock
	// in' version of compact blocks we send"): true once this peer has ever
	// sent a version-1 sendcmpct. Set once and never cleared, matching
	// SVNode's own `if(!state->fProvidesHeaderAndIDs)` guard, which only ever
	// writes true. Populated by Task 6 (sendcmpct negotiation); Phase 5b's
	// announcement path is its only planned reader.
	fProvidesHeaderAndIDs bool

	// fPreferHeaderAndIDs mirrors CNodeState::fPreferHeaderAndIDs
	// (net_processing.cpp:2406): the announce bit of this peer's MOST RECENT
	// version-1 sendcmpct, overwritten on every one it sends (SVNode does not
	// guard this write the way it guards fProvidesHeaderAndIDs). Because this
	// port never announces its own blocks (spec §2 non-goal), nothing reads
	// this field yet; it is recorded for Phase 5b.
	fPreferHeaderAndIDs bool

	// fSupportsDesiredCmpctVersion mirrors CNodeState::fSupportsDesiredCmpctVersion
	// (net_processing.cpp:2407-2409): true once this peer has sent a
	// version-1 sendcmpct at all. Set once and never cleared, matching
	// SVNode's own `if(!state->fSupportsDesiredCmpctVersion)` guard. Read by
	// Task 6's own receive path is not needed yet — carried for parity with
	// CNodeState and as the seam Phase 6 (net_processing.cpp:3591) needs to
	// gate whether an announced compact block is even worth accepting.
	fSupportsDesiredCmpctVersion bool

	// compact is this peer's partial compact block, the port of
	// QueuedBlock::partialBlock (node_state.h:80): the PartiallyDownloadedBlock
	// C++ hangs off the in-flight record for the block a cmpctblock announced.
	// nil means no compact block from this peer is being reconstructed.
	//
	// AT MOST ONE PER PEER, which is narrower than C++, where every in-flight
	// entry can carry its own. It is what makes net_processing.cpp:3839-3844
	// ("Peer sent us compact block we were already syncing!") the rule for a
	// second announcement of ANY block while one is outstanding, not just of
	// the same block. The cost is a peer that announces two blocks back to back
	// getting only the first reconstructed compactly; the second takes the
	// ordinary getdata path, which is what this port does for every block
	// anyway.
	//
	// It holds a partial block ONLY while a getblocktxn is outstanding. The
	// moment the blocktxn fills it, it moves to compactIngest below — see that
	// field for why the handoff has to clear this one.
	//
	// Cleared on three occasions, which between them cover every way the claim
	// it belongs to can end: the blocktxn arriving (either outcome), the
	// dispatcher's own refusal paths, and clearPeer — a disconnect or a
	// sync-peer rotation.
	compact *compactState

	// compactIngest is the partial block whose assembled stream is being
	// ingested. It is where compact moves at the handoff, and it is separate
	// from compact for one reason: a partial block that is still reachable as
	// `compact` will accept a SECOND blocktxn for the same hash and start a
	// second ingest of the same block.
	//
	// That window is not narrow. BlockIngestor.Ingest releases the transaction
	// stream as soon as it is drained, which unparks the transport read loop,
	// and the block is not reported through BlockDone until the whole
	// post-stream pipeline tail has run — minutes on a large block. A peer that
	// simply sends its blocktxn twice would be ingested twice.
	//
	// SVNode has no equivalent field because it needs none: FillBlock is
	// followed immediately by MarkBlockAsReceived (net_processing.cpp:3646),
	// which destroys the QueuedBlock the partial block hangs off, so the second
	// copy throws in GetBlockDetails and takes the unsolicited path at
	// :3595-3606. This port cannot release the block that early — the
	// reconstruction is not proven until the stream has been read — so it keeps
	// the state under a name the blocktxn path does not look at.
	//
	// Read and cleared by BlockDone through takeCompactStatus, which is what
	// classifies a failure the assembler could only reach mid-stream. Also
	// cleared by clearPeer.
	compactIngest *compactState
}

// newPeerSyncState returns a zero-value peerSyncState: no best known block,
// no pending unknown announcement, sync not started, nothing in flight or
// stalling. This mirrors a freshly default-constructed CNodeState entry.
func newPeerSyncState() *peerSyncState {
	return &peerSyncState{
		knownBlocks:   newKnownBlockSet(knownBlockCap),
		knownTxs:      newKnownBlockSet(knownTxCap),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](requestedTxnsTTL),
	}
}

// knownBlockCap bounds knownBlockSet. Legacy's own cap (peer.go
// maxKnownInventory = wire.MaxInvPerMsg, 50000) sizes a set shared between
// every relayed transaction and every relayed block; this port's set holds
// blocks only, at roughly one every ten minutes on mainnet (the target
// spacing chaincfg.MainNetParams.TargetTimePerBlock encodes), so that cap
// would cover the better part of a year. 288 is sized against that
// STEADY-STATE rate: 48 hours of blocks at the target spacing, with what
// looks like headroom for a catch-up burst.
//
// It is NOT a time-based bound, and the "48 hours" framing above is an
// average, not a floor: the cap is consumed per mark() call, and Inv (in
// manager.go) marks every InvTypeBlock entry in a single inbound MsgInv —
// up to wire.MaxInvPerMsg (50,000) entries in ONE message. A single large
// inv batch, exactly the "multi-block catch-up burst after a reconnect"
// case this comment used to cite as the reason for the headroom, can evict
// most or all of the set in one call, so the real worst-case protection
// window is "until the next big inv batch", which can be much shorter than
// 48 hours. The consequence stays bounded to a wasted small message either
// way — see relay.go's doc comments on hasBlock for why eviction is
// wasteful rather than harmful — so the number is kept, with this correction
// to what it actually guarantees.
//
// Each entry costs roughly 75-80 bytes, not the 32 bytes of a bare
// chainhash.Hash (fix round 2, review Minor 4 correction: knownBlockSet
// stores every hash TWICE — once as a map key, with the map's own
// bucket/entry overhead, and once again in the order slice that makes
// eviction FIFO): call it 25 KB per peer at the cap, and roughly 23 MB in
// aggregate across 1000 connected peers. Still small against this
// service's other per-peer state (negligible next to Task 10's
// maxPendingGetData, whose cost analysis this mirrors), just not as small
// as a bare-hash estimate would suggest.
const knownBlockCap = 288

// knownTxCap bounds peerSyncState.knownTxs. Legacy's own maxKnownInventory
// (peer.go:53-55, wire.MaxInvPerMsg = 50,000) was sized for exactly this
// traffic — outbound send-side dedup across BOTH blocks and transactions,
// dominated in practice by transactions, since blocks arrive roughly once
// every ten minutes and transactions arrive continuously. Task 12 rejected
// reusing that number for knownBlockCap because blocks-only traffic is far
// slower than the number implies (knownBlockCap's own doc comment: 288
// already covers 48 hours of blocks). That argument does not carry over
// here — tx traffic is exactly the traffic legacy calibrated 50,000
// against, and this set is no longer sharing its budget with anything
// else, so reusing legacy's own number is the FAITHFUL restoration of what
// it was sized for, not a naive copy (spec §4.3 fidelity).
//
// The honest cost, disclosed rather than assumed away: at ~75-80 bytes per
// entry (knownBlockCap's own per-entry accounting applies unchanged here —
// every hash stored twice, once as a map key and once in the FIFO order
// slice), 50,000 entries costs roughly 4 MB per peer, versus knownBlocks'
// ~25 KB. At 1,000 connected peers that is single-digit GB aggregate, where
// knownBlocks' equivalent is ~23 MB — a real, order-of-magnitude increase
// in per-peer memory that a horizontally-scaled node with many peer
// connections should account for. Flagged as a concern in this task's
// report rather than silently sized down, because sizing it down would be
// exactly the kind of unfaithful port spec §4.3 warns against without a
// stated reason grounded in this port's own traffic, not legacy's.
const knownTxCap = wire.MaxInvPerMsg

// knownBlockSet is a bounded, insertion-ordered set of hashes: the backing
// store for peerSyncState.knownBlocks AND peerSyncState.knownTxs (see each
// field's own doc comment for what it means and who writes it) — one type,
// two independently-capped instances, since knownBlocks is blocks-only by
// design (Task 12) and knownTxs needs a very different cap (see
// knownTxCap). Bounded FIFO eviction rather than legacy's most-recently-used
// replacement (peer.go mruInventoryMap) — the two are equivalent for this
// set's actual access pattern, since every hash here is inserted exactly
// once, checked for existence any number of times, and never re-inserted
// after it is marked; there is no "most recently used" distinct from "most
// recently inserted" to preserve.
//
// Not safe for concurrent use on its own: every peerSyncState field is
// guarded by PeerManager.syncMu, and this is no exception.
type knownBlockSet struct {
	set      map[chainhash.Hash]struct{}
	order    []chainhash.Hash
	capacity int
}

func newKnownBlockSet(capacity int) *knownBlockSet {
	return &knownBlockSet{set: make(map[chainhash.Hash]struct{}), capacity: capacity}
}

// has reports whether hash is already known. A nil receiver (a peerSyncState
// built by a zero-value literal instead of newPeerSyncState, as some test
// doubles do) answers false rather than panicking: "not yet known" is the
// correct answer for a set that was never given anything to know.
func (k *knownBlockSet) has(hash chainhash.Hash) bool {
	if k == nil {
		return false
	}

	_, ok := k.set[hash]

	return ok
}

// mark records hash as known, evicting the oldest entry first if the set is
// already at its cap. A hash already in the set is left exactly where it
// is: this is a "known or not" set, not an LRU, so re-marking does not
// refresh an eviction order that does not exist (see the type's doc
// comment).
func (k *knownBlockSet) mark(hash chainhash.Hash) {
	if k == nil {
		return
	}

	if _, ok := k.set[hash]; ok {
		return
	}

	if len(k.order) >= k.capacity {
		oldest := k.order[0]
		k.order = k.order[1:]
		delete(k.set, oldest)
	}

	k.set[hash] = struct{}{}
	k.order = append(k.order, hash)
}

// updateBlockAvailability mirrors net_processing.cpp UpdateBlockAvailability.
// Requires the caller to hold PeerManager's shared sync-state mutex (see the
// locking note on peerSyncState), the port of SVNode's cs_main requirement.
func (s *peerSyncState) updateBlockAvailability(idx *HeaderIndex, hash chainhash.Hash) {
	s.processBlockAvailability(idx)

	// C++ additionally guards on pindex->nChainWork > 0, which detects a
	// header accepted into mapBlockIndex but not yet linked to the
	// genesis-rooted chain. No counterpart is needed here, but it takes two
	// guarantees rather than one:
	//
	//   - HeaderIndex.AddHeader only ever attaches a node to a parent already
	//     in the tree, so every Lookup hit is genesis-rooted, unlike SVNode's
	//     mapBlockIndex which can hold disconnected entries;
	//   - every header reaching AddHeader has a valid target, so the proof
	//     summed along that chain is non-zero.
	//
	// The second guarantee is not enforced by AddHeader, and genesis-rootedness
	// alone does not imply it: a chain of headers whose nBits all encode an
	// invalid target would accumulate zero work. It is enforced upstream, on
	// both paths that write to the index. Peer headers pass
	// HeaderSync.checkBlockHeaderPoW (headersync.go), whose target.Sign() <= 0
	// test rejects exactly the negative and overflowing encodings that make
	// GetBlockProof zero, before OnHeaders calls AddHeader. Headers from the
	// blockchain subscription come from Teranode's own validated store. A
	// future caller that writes to the index past both of those breaks this
	// invariant and would need the guard restored.
	node, ok := idx.Lookup(hash)
	if ok {
		// An actually better block was announced.
		if s.pindexBestKnownBlock == nil ||
			chainWorkOf(node).Cmp(chainWorkOf(*s.pindexBestKnownBlock)) >= 0 {
			s.pindexBestKnownBlock = &node
		}
	} else {
		// An unknown block was announced; just assume that the latest one is
		// the best one.
		s.hashLastUnknownBlock = hash
	}
}

// processBlockAvailability mirrors net_processing.cpp ProcessBlockAvailability:
// "Check whether the last unknown block a peer advertised is not yet known."
// Requires the caller to hold PeerManager's shared sync-state mutex (see the
// locking note on peerSyncState), the port of SVNode's cs_main requirement.
func (s *peerSyncState) processBlockAvailability(idx *HeaderIndex) {
	if s.hashLastUnknownBlock == (chainhash.Hash{}) {
		return
	}

	// C++ additionally guards on pindex->nChainWork > 0, which detects a
	// header accepted into mapBlockIndex but not yet linked to the
	// genesis-rooted chain. No counterpart is needed here, but it takes two
	// guarantees rather than one:
	//
	//   - HeaderIndex.AddHeader only ever attaches a node to a parent already
	//     in the tree, so every Lookup hit is genesis-rooted, unlike SVNode's
	//     mapBlockIndex which can hold disconnected entries;
	//   - every header reaching AddHeader has a valid target, so the proof
	//     summed along that chain is non-zero.
	//
	// The second guarantee is not enforced by AddHeader, and genesis-rootedness
	// alone does not imply it: a chain of headers whose nBits all encode an
	// invalid target would accumulate zero work. It is enforced upstream, on
	// both paths that write to the index. Peer headers pass
	// HeaderSync.checkBlockHeaderPoW (headersync.go), whose target.Sign() <= 0
	// test rejects exactly the negative and overflowing encodings that make
	// GetBlockProof zero, before OnHeaders calls AddHeader. Headers from the
	// blockchain subscription come from Teranode's own validated store. A
	// future caller that writes to the index past both of those breaks this
	// invariant and would need the guard restored.
	node, ok := idx.Lookup(s.hashLastUnknownBlock)
	if !ok {
		return
	}

	// The clear below is unconditional once the hash resolves, independent
	// of whether it actually raises pindexBestKnownBlock: net_processing.cpp
	// clears hashLastUnknownBlock as soon as the pending hash is found,
	// even when it turns out to be no better than what's already known.
	if s.pindexBestKnownBlock == nil ||
		chainWorkOf(node).Cmp(chainWorkOf(*s.pindexBestKnownBlock)) >= 0 {
		s.pindexBestKnownBlock = &node
	}

	s.hashLastUnknownBlock = chainhash.Hash{}
}

// recordSendCmpct mirrors net_processing.cpp ProcessSendCompactMessage
// (:2390-2411): a version-1 sendcmpct locks in fProvidesHeaderAndIDs and
// fSupportsDesiredCmpctVersion (set once, never cleared) and overwrites
// fPreferHeaderAndIDs with this message's announce bit every time. Any other
// version changes nothing, matching SVNode's own
// `if(nCMPCTBLOCKVersion == 1)` gate, which leaves the whole block unread for
// any other value — no score, no state.
//
// Requires the caller to hold PeerManager's shared sync-state mutex (see the
// locking note on peerSyncState).
func (s *peerSyncState) recordSendCmpct(msg *wire.MsgSendcmpct) {
	if msg.Version != 1 {
		return
	}

	if !s.fProvidesHeaderAndIDs {
		s.fProvidesHeaderAndIDs = true
	}

	s.fPreferHeaderAndIDs = msg.SendCmpct

	if !s.fSupportsDesiredCmpctVersion {
		s.fSupportsDesiredCmpctVersion = true
	}
}
