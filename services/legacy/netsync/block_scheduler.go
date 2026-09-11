package netsync

import (
	"sort"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// blockPeer pairs a connected peer with the netsync state that answers "how high
// a chain has it told us about".
type blockPeer struct {
	peer  *peerpkg.Peer
	state *peerSyncState
}

// assignerPeer is one peer's share of a download pass: how many more blocks it
// may be asked for, and the getdata being built for it.
type assignerPeer struct {
	peer    *peerpkg.Peer
	state   *peerSyncState
	budget  int
	getData *wire.MsgGetData
}

// downloadAssigner decides which peer each header in a download pass is asked
// of, and collects one getdata per peer to be sent once the header lock is
// released.
//
// Slices are handed out as contiguous runs rather than round-robin: the assigner
// keeps offering the same peer until its budget is spent and then moves to the
// next. A peer answers a getdata roughly in the order it was asked, so a
// contiguous ascending run arrives in chain order — the park drains it as one
// run, and the download frontier is owed by one peer for a whole run instead of
// changing hands every block. Round-robin would spread the "slowest peer holds
// the frontier" risk, but multiplies the number of gaps the park has to hold.
type downloadAssigner struct {
	peers []*assignerPeer
	// remaining is the node-wide budget left in this pass.
	remaining int
	// idx is how far down the peer list the contiguous runs have got. It only
	// ever moves forward: heights ascend through a pass, so a peer that cannot
	// serve one header cannot serve any later one either.
	idx int
}

// eligibleBlockPeers lists the peers that may be asked for a block body, sync
// peer first and then in peer-id order so a pass is deterministic.
//
// The test is deliberately short: a connected sync candidate we hold state for.
// There is no throughput test, because the only download-throughput sample this
// node keeps belongs to the sync peer; no demotion cooldown, because that
// governs election as sync peer and not the right to carry bodies; and no ban
// check, because the peer server already ban-checks on delivery.
//
// It must be called with headerMu released. peerStates.Range() copies the map
// under its own read lock, so iterating the result holds no lock at all — but
// taking it before headerMu keeps the two locks from ever nesting.
func (sm *SyncManager) eligibleBlockPeers() []blockPeer {
	if sm.peerStates == nil {
		return nil
	}

	sp := sm.loadSyncPeer()

	eligible := make([]blockPeer, 0, 8)

	for p, state := range sm.peerStates.Range() {
		if state == nil || !state.syncCandidate || !p.Connected() {
			continue
		}

		eligible = append(eligible, blockPeer{peer: p, state: state})
	}

	sort.Slice(eligible, func(i, j int) bool {
		if (eligible[i].peer == sp) != (eligible[j].peer == sp) {
			return eligible[i].peer == sp
		}

		return eligible[i].peer.ID() < eligible[j].peer.ID()
	})

	return eligible
}

// newDownloadAssigner works out this pass's budgets and the peers they are
// spread over. A nil answer means there is nothing to do — no eligible peer, or
// no budget left — and the caller must then not walk the header list at all.
//
// It takes no locks of its own beyond leaf locks (the block-size tracker, the
// download ledger, and peerStates' own read lock inside Range), and must be
// called with headerMu released.
func (sm *SyncManager) newDownloadAssigner() *downloadAssigner {
	if sm.blockSizeTracker == nil {
		return nil
	}

	// The block-size ladder is the node's only reaction to block size: 20 blocks
	// in flight below a 100MB average, stepping down to 1 above 2GB. Nothing
	// about downloading from several peers makes that judgement wrong, so it
	// still governs — as a ceiling on each peer's queue depth, and at its lower
	// rungs on the fan-out itself.
	ladder := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	if !sm.settings.Legacy.MultiPeerBlockDownload {
		return sm.singlePeerAssigner(ladder)
	}

	eligible := sm.eligibleBlockPeers()
	if len(eligible) == 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] no peer is eligible to be asked for a block")

		return nil
	}

	window := max(1, sm.settings.Legacy.BlockDownloadWindow)

	remaining := window - sm.blockDownloads.Len()
	if remaining <= 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] the node is at its block download window of %d, not requesting more", window)

		return nil
	}

	// The per-peer cap governs small blocks and the ladder governs large ones,
	// and at the ladder's lowest rungs it narrows the fan-out too: at a 2GB
	// average the node is back to one peer holding one block, which is exactly
	// what it does today. Every peer's read loop holds one fully decoded block
	// before the prefetch byte budget applies, so fanning out at that rung would
	// multiply the memory the ladder exists to protect.
	perPeer := min(max(1, sm.settings.Legacy.MaxBlocksInTransitPerPeer), ladder)
	fanout := min(len(eligible), ladder)

	peers := make([]*assignerPeer, 0, fanout)
	assignable := 0

	for _, candidate := range eligible {
		if len(peers) == fanout {
			break
		}

		budget := perPeer - sm.blockDownloads.CountForPeer(candidate.peer)
		if budget <= 0 {
			continue
		}

		peers = append(peers, &assignerPeer{peer: candidate.peer, state: candidate.state, budget: budget})
		assignable += budget
	}

	if len(peers) == 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] every eligible peer is at its per-peer limit of %d blocks", perPeer)

		return nil
	}

	// The pass budget is also the size of each round's snapshot, and every header
	// in a snapshot costs one "do we already have this?" round trip to the
	// blockchain service. So it is what the peers can actually take between them,
	// not the whole node-wide window: at the default window of 1024 and one peer
	// able to take 16, sizing the round by the window would make 1024 round trips
	// to place 16 blocks, on every arriving block.
	return &downloadAssigner{peers: peers, remaining: min(remaining, assignable)}
}

// singlePeerAssigner is the behaviour the node had before the scheduler: one
// budget, the block-size ladder's, spent on one peer, the sync peer. It is what
// legacy_multiPeerBlockDownload=false restores.
func (sm *SyncManager) singlePeerAssigner(ladder int) *downloadAssigner {
	sp := sm.loadSyncPeer()
	if sp == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no sync peer")

		return nil
	}

	state, exists := sm.peerStates.Get(sp)
	if !exists {
		sm.logger.Warnf("[fetchHeaderBlocks] sync peer state not found")

		return nil
	}

	inFlight := sm.blockDownloads.CountForPeer(sp)

	budget := ladder - inFlight
	if budget <= 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] Already at max in-flight blocks (%d/%d), not requesting more", inFlight, ladder)

		return nil
	}

	return &downloadAssigner{
		peers:     []*assignerPeer{{peer: sp, state: state, budget: budget}},
		remaining: budget,
	}
}

// take offers a header at this height to the peers in turn and reports which one
// will carry it. A false answer means the budgets are spent — the node-wide
// window, or every peer's own cap — and the caller must then stop with the
// download cursor still on that header, because advancing past a header nobody
// was asked for loses that block from the walk for good.
//
// The claimed-height test picks between peers; it is not a veto. When no peer
// with budget claims a chain that reaches this block, the first peer with budget
// is asked anyway. A claimed height is a lower bound that goes stale downward
// (see canServe), so "nobody claims it" routinely means we simply have not been
// told rather than that nobody has the block — and a scheduler that declines to
// ask anybody stops sync dead, which is far worse than one wasted request. A
// peer that really cannot serve it just does not answer, the hash becomes
// re-requestable after blockRequestRetryInterval, and the one block that
// actually gates progress is covered by the frontier race.
func (a *downloadAssigner) take(height int32) (*assignerPeer, bool) {
	if a == nil || a.remaining <= 0 {
		return nil, false
	}

	for a.idx < len(a.peers) && a.peers[a.idx].budget <= 0 {
		a.idx++
	}

	var fallback *assignerPeer

	for i := a.idx; i < len(a.peers); i++ {
		p := a.peers[i]
		if p.budget <= 0 {
			continue
		}

		if p.canServe(height) {
			return p, true
		}

		if fallback == nil {
			fallback = p
		}
	}

	if fallback == nil {
		return nil, false
	}

	return fallback, true
}

// canServe reports whether this peer has told us about a chain that reaches the
// given height.
//
// bestKnownHeight is a claim, not proof: it is seeded from the version handshake
// and raised by headers and blocks the peer actually delivers, and it never goes
// down. So it is a lower bound that goes stale downward, which excludes peers
// rather than wrongly including them — the conservative direction. A peer that
// claimed nothing at all (height zero) is treated as unknown rather than as
// useless, which is the same reading the frontier race takes of a peer's last
// known block.
//
// This is not svnode's pindexBestKnownBlock. svnode tracks the actual best
// header each peer has announced and drops a peer whose chain work is below our
// tip before any window arithmetic. Below the last hardcoded checkpoint — the
// regime this whole path runs in — every peer is on the same chain by
// construction, so a claim of height N is as good as proof; near the tip, or on
// a fork, it is weaker.
func (p *assignerPeer) canServe(height int32) bool {
	if p.state == nil || height <= 0 {
		return true
	}

	claimed := p.state.BestKnownHeight()

	return claimed <= 0 || claimed >= height
}

// recordRequest adds a block to this peer's getdata and spends a unit of both
// its own budget and the pass's. It is called under headerMu, straight after the
// download ledger has taken the assignment: pure memory, no lock, no send.
func (a *downloadAssigner) recordRequest(p *assignerPeer, hash *chainhash.Hash) error {
	if p.getData == nil {
		// Sized to what this peer may still be asked for, not to the header
		// list, which is often 2000 entries for a handful of used slots.
		p.getData = wire.NewMsgGetDataSizeHint(uint(max(1, p.budget))) // nolint:gosec
	}

	if err := p.getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, hash)); err != nil {
		return err
	}

	p.budget--
	a.remaining--

	return nil
}

// send hands each peer that got work its getdata. It must be called with
// headerMu released: a peer send may not run under that lock, and this is the
// one place in the pass that sends anything.
func (a *downloadAssigner) send(sm *SyncManager) {
	if a == nil {
		return
	}

	for _, p := range a.peers {
		if p.getData == nil || len(p.getData.InvList) == 0 {
			continue
		}

		sm.logger.Debugf("[fetchHeaderBlocks] Requesting %d block(s) from %s", len(p.getData.InvList), p.peer)

		p.peer.QueueMessage(p.getData, nil)
	}
}
