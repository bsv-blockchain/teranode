package netsync

import (
	"math"
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
	// rate is the peer's measured delivery rate in bytes a second, or the median of the measured
	// peers when it has none. The lowest block goes to the fastest peer with room.
	rate float64
}

// speedScaledDepth is a peer's share of depth by speed: depth times its rate over the fastest
// peer's, rounded, never below one. With no measured rate to compare against, the peer gets the full
// depth; an unmeasured peer is given the median rate by the caller.
//
// A peer sends its queue in order and nothing can reorder it, so a slow peer with a full queue buries
// blocks the chain will soon need. On 2026-09-25, with every peer at 16, blocks 755,236 and 755,244
// started 11 and 22 minutes after they were asked for, behind multi-GB blocks at peers delivering 5
// to 10 MB/s, while peers at 50 MB/s had fetched 800 blocks further ahead. This is not an SV Node
// rule: SV Node gives every peer 16 and drops a peer only below 100 KB/s.
func speedScaledDepth(depth int, rate, fastest float64) int {
	if fastest <= 0 || rate <= 0 || rate >= fastest {
		return depth
	}

	return max(1, int(math.Round(float64(depth)*rate/fastest)))
}

// unmeasuredPeerDepth is the most blocks a peer is given before its speed is known, as every peer
// is straight after a restart. On 2026-09-25 after a restart every peer was given 16 before any
// speed was known, and peers at 4 MB/s held 13 blocks against a speed-scaled depth of two or three.
const unmeasuredPeerDepth = 2

// peerQueueDepth is how many blocks p may be asked for at once with the park on: its speed-scaled
// share of depth once its speed is measured, and at most unmeasuredPeerDepth until then.
func (sm *SyncManager) peerQueueDepth(p *peerpkg.Peer, depth int, fastest float64) int {
	rate := sm.streams.peerRate(p)
	if rate <= 0 {
		return min(depth, unmeasuredPeerDepth)
	}

	return speedScaledDepth(depth, rate, fastest)
}

// parkBackstopBytes is the most block bytes the node holds ahead of the chain, parked and
// arriving, before it stops asking for blocks further ahead. It guards the disk and is not meant to
// pace the download: SV Node has no byte limit, only its 1,024-block window.
//
// It was 20 GiB until 2026-09-25, which is about five minutes of lead at height 754,000, and a
// block the chain needs must be asked for at least its own download time ahead. Block 754,562 is
// 4.0 GB and took 8 minutes 48 seconds from one peer at 7.6 MB/s, and the chain waited on it with
// 27.9 GB parked and six of eight peers idle.
const parkBackstopBytes = int64(100) << 30

// streamingPeerDepth is how many blocks a peer holds with the park on: legacy_maxBlocksInTransitPerPeer,
// 16 by default, as SV Node's MAX_BLOCKS_IN_TRANSIT_PER_PEER. It does not depend on block size.
//
// It was 2 until 2026-09-25, on the reasoning that a peer sends blocks in order so a deeper queue
// adds no speed. That holds for large blocks, which take longer to send than the next request takes
// to arrive. It fails for small ones: a peer sends two 200 KB blocks in milliseconds and then waits
// a round trip, plus a download pass, for the next request. SV Node keeps 16 queued at every size.
func (sm *SyncManager) streamingPeerDepth() int {
	if sm.settings == nil {
		return 1
	}

	return max(1, sm.settings.Legacy.MaxBlocksInTransitPerPeer)
}

// charge records one more block asked of this peer.
func (p *assignerPeer) charge() {
	p.budget--
}

// before reports whether p should be offered a block ahead of q: faster first, then more room,
// then the earlier peer.
func (p *assignerPeer) before(q *assignerPeer) bool {
	if p.rate != q.rate {
		return p.rate > q.rate
	}

	return p.budget > q.budget
}

// downloadAssigner decides which peer each header in a download pass is asked
// of, and collects one getdata per peer to be sent once the header lock is
// released.
//
// Each block, in height order, goes to the fastest peer with room. A peer answers
// a getdata in the order it was asked and nothing can reorder its queue, so the
// assigner used to fill one peer with a contiguous run: the next block the chain
// needed then sat behind others at that peer while other peers were idle. At
// height 705,000, with blocks of hundreds of megabytes, its bytes began one and
// a half to six minutes after it was asked for. Spreading puts the next blocks at
// the top of separate peers' queues. The park holds the gaps this opens, which is
// what it is for.
type downloadAssigner struct {
	peers []*assignerPeer
	// remaining is the node-wide budget left in this pass.
	remaining int
	// overBackstop says the bytes really held ahead of the chain have reached parkBackstopBytes,
	// so nothing above the highest held block is asked for in this pass. Blocks below it fill
	// gaps and are never stopped: filling a gap is what lets the park drain.
	overBackstop bool
	// full holds the eligible peers with no room left, for fastestAvoiding: a block re-asked
	// because its owner went quiet may go to one of them.
	full []*assignerPeer
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

	largest := sm.blockSizeTracker.largestRecentSize()

	// The block-size ladder governs only the single-peer fallback below: with
	// the park always on, every peer holds streamingPeerDepth blocks instead,
	// whatever the block size, and the largest recent block feeds only the
	// disk backstop.
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

	// This bounds blocks in flight on the wire. It is NOT what stops the
	// downloader racing ahead of the committer: that is wantedBlocks
	// (wanted_range_assign.go), which never names a height more than
	// legacy_blockDownloadWindow above the last committed block. A count
	// cannot do that job, because a block 5000 ahead and a block 1 ahead each
	// count as one.
	remaining := window - sm.blockDownloads.Len()

	if remaining <= 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] the node is at its block download window of %d, not requesting more", window)

		return nil
	}

	// Blocks stream straight to disk behind the admission budget and no read
	// loop holds a decoded block, so every eligible peer carries blocks: there
	// is no memory reason to narrow the fan-out the way the ladder once did.
	perPeer := sm.streamingPeerDepth()
	fanout := len(eligible)

	// Every peer is kept at streamingPeerDepth requests. Blocks are processed
	// faster than they arrive, so the park grows only while the chain waits on
	// one slow block, and no peer should ever be idle. The one brake is a
	// backstop for the disk: the bytes really held ahead of the chain, parked
	// and arriving, reaching parkBackstopBytes. A block asked for but not yet
	// arriving holds nothing and is not counted. It used to count at the
	// largest recent block, and after a 2.3 GB block on 2026-09-25 that filled
	// the budget with bytes that did not exist while four of eight peers idled.
	overBackstop := sm.bytesAhead(largest) >= parkBackstopBytes

	peers := make([]*assignerPeer, 0, fanout)
	assignable := 0

	var full []*assignerPeer

	fallbackRate := sm.streams.medianRate()

	var fastest float64
	for _, candidate := range eligible {
		fastest = max(fastest, sm.streams.peerRate(candidate.peer))
	}

	for _, candidate := range eligible {
		if len(peers) == fanout {
			break
		}

		rate := sm.streams.peerRate(candidate.peer)
		if rate <= 0 {
			rate = fallbackRate
		}

		depth := sm.peerQueueDepth(candidate.peer, perPeer, fastest)

		budget := depth - sm.blockDownloads.CountForPeer(candidate.peer)
		if budget <= 0 {
			full = append(full, &assignerPeer{peer: candidate.peer, state: candidate.state, rate: rate})

			continue
		}

		peers = append(peers, &assignerPeer{peer: candidate.peer, state: candidate.state, budget: budget, rate: rate})
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
	return &downloadAssigner{peers: peers, remaining: min(remaining, assignable), overBackstop: overBackstop, full: full}
}

// bytesAhead is how many bytes of blocks the node really holds ahead of the chain: parked blocks
// at their wire size and blocks arriving now at their declared size. A block asked for but not yet
// arriving holds nothing and is not counted. A parked block recovered from disk after a restart has
// no recorded wire size and counts at unknownSize.
func (sm *SyncManager) bytesAhead(unknownSize int64) int64 {
	parked := sm.blockPark.aheadBytes(unknownSize)
	arriving, _ := sm.streams.arrivingBytes()

	return parked + arriving
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
// with budget claims a chain that reaches this block, a peer with budget is
// asked anyway. A claimed height is a lower bound that goes stale downward
// (see canServe), so "nobody claims it" routinely means we simply have not been
// told rather than that nobody has the block — and a scheduler that declines to
// ask anybody stops sync dead, which is far worse than one wasted request. A
// peer that really cannot serve it just does not answer, the hash becomes
// re-requestable after blockRequestRetryInterval, and the one block that
// actually gates progress is covered by the frontier race.
func (a *downloadAssigner) take(height int32) (*assignerPeer, bool) {
	return a.takeAvoiding(height, nil)
}

// takeAvoiding is take with a preference: a peer the caller would rather not
// pick is chosen only when no peer without that mark can be found. avoid may be
// nil, which is exactly take, and take is the only thing the header walk uses —
// so this preference cannot change which peer that walk picks for anything.
//
// The caller that does use it is the wanted-range assignment pass, whose mark is
// "this peer already owes us this block". Asking such a peer for it again makes
// it send the block twice, and the second copy arrives after the first
// discharged its obligation, so it looks unrequested and costs an honest peer
// its whole association. A marked peer is still returned when it is all there
// is, because the caller has something useful to do with it that is not a second
// getdata; it just must never be preferred over a peer that could actually help.
//
// An unmarked peer that has not claimed a chain reaching this height beats a
// marked peer that has. canServe is a lower bound that goes stale downward, so
// "has not claimed it" routinely means only that we have not been told, and
// take's own reasoning already prefers one wasted request to asking nobody. A
// marked peer, by contrast, can contribute nothing new however high it claims.
//
// Within each of those tiers the fastest peer wins, with more room left breaking
// a tie, and then the earlier peer, the sync peer first.
func (a *downloadAssigner) takeAvoiding(height int32, avoid func(*peerpkg.Peer) bool) (*assignerPeer, bool) {
	if a == nil || a.remaining <= 0 {
		return nil, false
	}

	// server, fallback, avoidedServer, avoidedFallback, in order of preference.
	var tiers [4]*assignerPeer

	for _, p := range a.peers {
		if p.budget <= 0 {
			continue
		}

		tier := 0
		if avoid != nil && avoid(p.peer) {
			tier = 2
		}

		if !p.canServe(height) {
			tier++
		}

		if tiers[tier] == nil || p.before(tiers[tier]) {
			tiers[tier] = p
		}
	}

	for _, p := range tiers {
		if p != nil {
			return p, true
		}
	}

	return nil, false
}

// fastestAvoiding is takeAvoiding for a block re-asked because its owner went quiet: it offers the
// block to every eligible peer, full queue or not, in the same tiers, fastest first. The chain may
// be waiting on the block, and the peers with room are the slow ones.
func (a *downloadAssigner) fastestAvoiding(height int32, avoid func(*peerpkg.Peer) bool) (*assignerPeer, bool) {
	if a == nil {
		return nil, false
	}

	var tiers [4]*assignerPeer

	for _, p := range append(append([]*assignerPeer(nil), a.peers...), a.full...) {
		tier := 0
		if avoid != nil && avoid(p.peer) {
			tier = 2
		}

		if !p.canServe(height) {
			tier++
		}

		if tiers[tier] == nil || p.rate > tiers[tier].rate {
			tiers[tier] = p
		}
	}

	for _, p := range tiers {
		if p != nil {
			return p, true
		}
	}

	return nil, false
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
func (a *downloadAssigner) recordRequest(p *assignerPeer, hash *chainhash.Hash, overDepth bool) error {
	if p.getData == nil {
		// Sized to what this peer may still be asked for, not to the header
		// list, which is often 2000 entries for a handful of used slots.
		p.getData = wire.NewMsgGetDataSizeHint(uint(max(1, p.budget))) // nolint:gosec
	}

	if err := p.getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, hash)); err != nil {
		return err
	}

	p.charge()

	// A re-ask placed over a full peer's depth does not use the pass's room.
	if !overDepth {
		a.remaining--
	}

	return nil
}

// send hands each peer that got work its getdata. It must be called with
// headerMu released: a peer send may not run under that lock, and this is the
// one place in the pass that sends anything.
func (a *downloadAssigner) send(sm *SyncManager) {
	if a == nil {
		return
	}

	for _, p := range append(a.peers, a.full...) {
		if p.getData == nil || len(p.getData.InvList) == 0 {
			continue
		}

		sm.logger.Debugf("[fetchHeaderBlocks] Requesting %d block(s) from %s", len(p.getData.InvList), p.peer)

		p.peer.QueueMessage(p.getData, nil)
	}
}
