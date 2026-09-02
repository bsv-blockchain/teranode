package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// schedulerPeer connects a peer that records every getdata it is sent, registers
// it with the manager as a sync candidate, and tells the manager how high a
// chain the peer claims — which is what decides whether the scheduler will hand
// it a given header.
func schedulerPeer(t *testing.T, sm *SyncManager, idx uint8, claimedHeight int32) (*peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	p, _, rec := connectRacePeer(t, idx, claimedHeight)
	registerRacePeer(sm, p).noteBestKnownHeight(claimedHeight)

	return p, rec
}

// schedulerPeerBudget is how many blocks one peer may be asked for in a single
// pass: its own cap, floored by the block-size ladder — or the ladder on its
// own with the scheduler switched off, which is how the node sized a pass before
// the scheduler existed. The literal defaults are pinned by the tests in this
// file; callers elsewhere only need "a whole pass's worth".
func schedulerPeerBudget(sm *SyncManager) int {
	ladder := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	if !sm.settings.Legacy.MultiPeerBlockDownload {
		return ladder
	}

	return min(sm.settings.Legacy.MaxBlocksInTransitPerPeer, ladder)
}

// startHeaderHash reports the hash the download cursor is currently sitting on,
// and whether it is on anything at all.
func startHeaderHash(t *testing.T, sm *SyncManager) (chainhash.Hash, bool) {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	if sm.startHeader == nil {
		return chainhash.Hash{}, false
	}

	node, ok := sm.startHeader.Value.(*headerNode)
	require.True(t, ok)
	require.NotNil(t, node.hash)

	return *node.hash, true
}

// TestScheduler_SpreadsOneHeaderRunAcrossEveryEligiblePeer is the anchor test
// for the whole feature: one run of headers, three connected peers, and the run
// has to leave the node down all three sockets rather than one.
//
// Each peer's queue is capped at four blocks, so twelve headers cannot be
// carried by fewer than three peers. The slices are contiguous and ascending,
// starting with the sync peer, because a peer answers a getdata roughly in the
// order it was asked: a contiguous ascending run arrives in chain order and the
// park drains it as one run.
func TestScheduler_SpreadsOneHeaderRunAcrossEveryEligiblePeer(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd1}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 80, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 81, 1000)
	_, thirdRec := schedulerPeer(t, sm, 82, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool {
		return syncRec.count()+secondRec.count()+thirdRec.count() == len(hashes)
	}, 5*time.Second), "every seeded header should have been asked of somebody")

	require.Equal(t, hashes[0:4], syncRec.all(), "the sync peer takes the first contiguous run")
	require.Equal(t, hashes[4:8], secondRec.all(), "the second peer takes the next contiguous run")
	require.Equal(t, hashes[8:12], thirdRec.all(), "the third peer takes the last contiguous run")

	// The ledger has to agree with the wire, peer for peer, or a delivered block
	// arrives with nothing vouching for it and costs an honest peer its
	// connection.
	require.Equal(t, len(hashes), sm.blockDownloads.Len(), "every requested block is owed by somebody")
}

// TestScheduler_APeerAtItsCapIsNotAskedForMore pins the per-peer budget. The
// sync peer already owes as many blocks as its cap allows, so the whole run has
// to go to the peer behind it rather than piling onto the peer we are already
// waiting on.
func TestScheduler_APeerAtItsCapIsNotAskedForMore(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd2}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 83, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 84, 1000)

	// Four unrelated blocks already outstanding with the sync peer: its cap is
	// spent before this pass starts.
	for i := 0; i < 4; i++ {
		require.True(t, sm.blockDownloads.Add(syncPeer, chainhash.Hash{0xe0, byte(i)}))
	}

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return secondRec.count() == len(hashes) }, 5*time.Second),
		"the run should have gone to the peer with budget left")
	require.Equal(t, hashes, secondRec.all())
	require.Zero(t, syncRec.count(), "a peer at its cap must not be asked for another block")
}

// TestScheduler_RespectsTheNodeWideWindow pins the other budget: the sum over
// every peer. With the window set to three, four idle peers and ten headers to
// hand out, exactly three blocks may be outstanding, and the walk must stop with
// its cursor on the fourth header rather than dropping it.
func TestScheduler_RespectsTheNodeWideWindow(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd3}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.BlockDownloadWindow = 3

	syncPeer, syncRec := schedulerPeer(t, sm, 85, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 86, 1000)
	_, thirdRec := schedulerPeer(t, sm, 87, 1000)
	_, fourthRec := schedulerPeer(t, sm, 88, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 3 }, 5*time.Second),
		"the window's worth of blocks should have been requested")

	total := syncRec.count() + secondRec.count() + thirdRec.count() + fourthRec.count()
	require.Equal(t, 3, total, "the node-wide window must bound the sum over all peers")
	require.Equal(t, hashes[0:3], syncRec.all())
	require.Equal(t, 3, sm.blockDownloads.Len())

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the cursor must stay in the list")
	require.Equal(t, hashes[3], cursor, "the first header the window could not cover must still be next")
}

// TestScheduler_APeerThatHasNotClaimedTheHeightIsNotAsked pins the eligibility
// rule. A peer that has only ever told us about a chain shorter than the block
// being handed out is not asked for it — and the moment it claims a longer
// chain it is asked, so what is pinned is the rule and not merely a peer that
// never gets work.
func TestScheduler_APeerThatHasNotClaimedTheHeightIsNotAsked(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd4}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	// The short peer is the sync peer, so it is first in line: if the claimed
	// height were not consulted it would take the whole run.
	shortPeer, shortRec := schedulerPeer(t, sm, 89, 5)
	sm.storeSyncPeer(shortPeer, &syncPeerState{})

	longPeer, longRec := schedulerPeer(t, sm, 90, 1000)

	// The headers come from the long peer, because handing us headers up to
	// height N is itself a claim to have that chain: a peer that delivered these
	// headers would no longer be the short peer.
	//
	// Seeded from height 10, so every header is at 11 or above — out of reach of
	// a peer claiming height 5.
	sm.resetHeaderState(&anchor, 10)
	sm.headersFirstMode.Store(true)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: longPeer})
	require.Equal(t, len(hashes)+1, sm.headerListLen())

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return longRec.count() == len(hashes) }, 5*time.Second),
		"the peer that claims the chain should have been asked")
	require.Zero(t, shortRec.count(), "a peer whose claimed chain is shorter than the block must not be asked for it")

	// It claims the chain now, and a fresh run of headers proves the rule
	// reversed rather than the peer being written off.
	state, exists := sm.peerStates.Get(shortPeer)
	require.True(t, exists)
	state.noteBestKnownHeight(1000)

	more, moreHashes := linkedHeaders(hashes[len(hashes)-1], 3, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: more, peer: longPeer})

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return shortRec.count() == len(moreHashes) }, 5*time.Second),
		"once the peer claims the chain it must be asked")
	require.Equal(t, moreHashes, shortRec.all())
}

// TestScheduler_OffPathSendsOneGetDataToTheSyncPeer is the rollback lever. With
// the master switch off the node has to behave exactly as it did before the
// scheduler existed: one getdata, to the sync peer, holding the first
// block-size-ladder's worth of headers in list order, with every other
// connected peer left alone and the cursor on the first header it did not
// consider.
func TestScheduler_OffPathSendsOneGetDataToTheSyncPeer(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd5}
	msg, hashes := linkedHeaders(anchor, 30, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MultiPeerBlockDownload = false

	syncPeer, syncRec := schedulerPeer(t, sm, 91, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 92, 1000)
	_, thirdRec := schedulerPeer(t, sm, 93, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	ladder := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == ladder }, 5*time.Second),
		"the sync peer should have been asked for the ladder's worth of blocks")
	require.Equal(t, hashes[0:ladder], syncRec.all(), "in list order, exactly as before")
	require.Equal(t, 1, syncRec.messages(), "exactly one getdata")
	require.Zero(t, secondRec.count(), "no other peer is asked anything with the scheduler off")
	require.Zero(t, thirdRec.count())

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok)
	require.Equal(t, hashes[ladder], cursor, "the cursor is left on the first header not considered")
}

// TestScheduler_OffPathWithNoSyncPeerRequestsNothing keeps the other half of the
// old behaviour: with the scheduler off there is nowhere for a block request to
// go until a sync peer has been elected.
func TestScheduler_OffPathWithNoSyncPeerRequestsNothing(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd6}
	msg, _ := linkedHeaders(anchor, 5, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MultiPeerBlockDownload = false

	deliverer, delivererRec := schedulerPeer(t, sm, 94, 1000)
	_, otherRec := schedulerPeer(t, sm, 95, 1000)

	seedFetchHeaders(t, sm, deliverer, anchor, msg)
	sm.storeSyncPeer(nil, nil)

	sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return delivererRec.count()+otherRec.count() > 0 }, 500*time.Millisecond),
		"with the scheduler off, no sync peer means no block requests")
	require.Zero(t, sm.blockDownloads.Len())
}

// TestScheduler_RequestsBlocksWithNoSyncPeerAtAll is the line the scheduler
// deletes. A node between sync peers still has connected peers holding the
// blocks it needs, and the header list it walked is still good.
func TestScheduler_RequestsBlocksWithNoSyncPeerAtAll(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd7}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	deliverer, delivererRec := schedulerPeer(t, sm, 96, 1000)
	_, otherRec := schedulerPeer(t, sm, 97, 1000)

	seedFetchHeaders(t, sm, deliverer, anchor, msg)
	sm.storeSyncPeer(nil, nil)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return delivererRec.count()+otherRec.count() == len(hashes) }, 5*time.Second),
		"the run should have been asked of the connected peers even with no sync peer")
	require.Equal(t, len(hashes), sm.blockDownloads.Len())
}

// TestScheduler_NeverAsksASecondPeerForAHashSomebodyAlreadyOwes is the guard
// that stands between fanning out and the duplicate-commit storm this codebase
// has already had once: a peer was asked for a block that was still outstanding
// with another peer, both copies were admitted, and both were committed.
//
// The block at the front of the run is already owed by the sync peer, so the
// walk must step over it without handing it to anybody else, and must carry on
// with the rest of the run rather than stalling on it.
func TestScheduler_NeverAsksASecondPeerForAHashSomebodyAlreadyOwes(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd8}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, syncRec := schedulerPeer(t, sm, 98, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 99, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// Already in flight: asked for a moment ago and not yet delivered.
	require.True(t, sm.blockDownloads.Add(syncPeer, hashes[0]))

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool {
		return syncRec.count()+secondRec.count() >= len(hashes)-1
	}, 5*time.Second), "the rest of the run must still be asked for")

	for _, h := range syncRec.all() {
		require.NotEqual(t, hashes[0], h, "the in-flight block must not be asked for again")
	}

	for _, h := range secondRec.all() {
		require.NotEqual(t, hashes[0], h, "the in-flight block must never be handed to a second peer")
	}

	require.Equal(t, hashes[1:], syncRec.all(), "the walk carries on past the block it skipped")
}

// TestScheduler_StopsWithTheCursorOnAHeaderNobodyCanTake pins the cursor
// discipline. When the budgets run out part way through a run, the walk has to
// stop with the cursor on the first header it could not place. Advancing past it
// loses that block from the walk for good, and a "cursor is not nil" assertion
// is satisfied perfectly by that broken state — so the identity of the header is
// what gets asserted.
func TestScheduler_StopsWithTheCursorOnAHeaderNobodyCanTake(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd9}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 3

	syncPeer, syncRec := schedulerPeer(t, sm, 100, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 3 }, 5*time.Second),
		"the one peer's cap is what bounds this pass")
	require.Equal(t, hashes[0:3], syncRec.all())

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the cursor must stay in the list")
	require.Equal(t, hashes[3], cursor, "the cursor must be left on the header nobody could take")
}

// TestScheduler_HugeBlocksCollapseBackToOnePeerWithOneBlock is the memory
// ceiling, and the assertion that must never be allowed to rot. The block-size
// ladder is the node's only reaction to block size: at a two-gigabyte average it
// allows one block in flight, and fanning out must not turn that into one block
// per peer. Every peer's read loop holds a fully decoded block before the
// prefetch byte budget applies, so four peers at that rung is four times the
// memory the ladder was protecting.
func TestScheduler_HugeBlocksCollapseBackToOnePeerWithOneBlock(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xda}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	const threeGB = int64(3) * 1024 * 1024 * 1024
	for i := 0; i < 3; i++ {
		sm.blockSizeTracker.addBlockSize(threeGB)
	}

	require.Equal(t, 1, sm.blockSizeTracker.calculateMaxInFlightBlocks(), "sanity: the ladder is at its bottom rung")

	syncPeer, syncRec := schedulerPeer(t, sm, 101, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 102, 1000)
	_, thirdRec := schedulerPeer(t, sm, 103, 1000)
	_, fourthRec := schedulerPeer(t, sm, 104, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 1 }, 5*time.Second),
		"the one block the ladder allows should have been requested")

	total := syncRec.count() + secondRec.count() + thirdRec.count() + fourthRec.count()
	require.Equal(t, 1, total, "at the ladder's bottom rung the node asks for one block, from one peer")
	require.Equal(t, hashes[0:1], syncRec.all())
	require.Equal(t, 1, sm.blockDownloads.Len())
}

// TestScheduler_StallRaceStillAddsASecondPeerToTheFrontier proves the one
// deliberate exception survives the scheduler. Every other rule here says a hash
// belongs to exactly one peer; the frontier race says the single block holding
// up sync may be asked of a second peer, because everything else is queued
// behind it. Under the scheduler the frontier is owed by whichever peer got the
// first slice, which is the case the race has to keep working for.
func TestScheduler_StallRaceStillAddsASecondPeerToTheFrontier(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xdb}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 2

	syncPeer, _ := schedulerPeer(t, sm, 105, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	second, secondRec := schedulerPeer(t, sm, 106, 1000)
	third, thirdRec := schedulerPeer(t, sm, 107, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return sm.blockDownloads.Len() == len(hashes) }, 5*time.Second),
		"the run should have been handed out")

	frontier := hashes[0]
	require.True(t, sm.blockDownloads.HasOwner(syncPeer, frontier), "the first slice carries the frontier")

	// The peer that owes the frontier has gone quiet on it for half a minute,
	// comfortably past the 20 second default.
	sm.setFrontier(frontier, 11, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	racer := second
	racerRec := secondRec

	if sm.blockDownloads.HasOwner(third, frontier) {
		racer = third
		racerRec = thirdRec
	}

	require.True(t, sm.blockDownloads.HasOwner(racer, frontier),
		"a second peer must be put on the block holding up sync, and authorised to answer")
	require.True(t, WaitUntil(func() bool {
		for _, h := range racerRec.all() {
			if h == frontier {
				return true
			}
		}

		return false
	}, 5*time.Second), "the racer must actually be asked for the frontier block")
	require.True(t, sm.blockDownloads.HasOwner(syncPeer, frontier),
		"the original request stands: the race adds a copy, it does not move the block")
}

// TestScheduler_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup is the
// single-peer lock discipline test run against the multi-peer path, because that
// is now the path a node takes. The blockchain lookup is a gRPC round trip on a
// context with no deadline, and the block-queue consumer — the one goroutine
// that commits blocks in order — takes headerMu as its first act.
func TestScheduler_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	syncPeer, _ := schedulerPeer(t, sm, 108, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, _ = schedulerPeer(t, sm, 109, 1000)
	_, _ = schedulerPeer(t, sm, 110, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0xdc}
	msg, _ := linkedHeaders(anchor, 25, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.fetchHeaderBlocks()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("fetchHeaderBlocks never reached the blockchain lookup")
	}

	acquired := make(chan struct{})

	go func() {
		_ = sm.headerListLen()

		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		close(gate)
		<-done
		t.Fatal("reading the header list blocked while fetchHeaderBlocks waited on the blockchain lookup")
	}

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("fetchHeaderBlocks never returned")
	}
}

// TestScheduler_WhenNobodyClaimsTheHeightTheFirstPeerIsStillAsked is the other
// half of the eligibility rule, and the one that keeps it from being able to
// wedge sync. A claimed height is a lower bound that goes stale downward: a peer
// that has told us nothing since the handshake reads as shorter than it is. When
// no peer with budget claims a chain reaching the block, the walk asks the first
// peer with budget anyway rather than stopping — a wasted request costs one
// round trip, a scheduler that declines to ask anybody costs the whole sync.
func TestScheduler_WhenNobodyClaimsTheHeightTheFirstPeerIsStillAsked(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xdd}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	// Both peers claim a chain far below the headers, which are seeded from
	// height 10.
	shortPeer, shortRec := schedulerPeer(t, sm, 111, 3)
	sm.storeSyncPeer(shortPeer, &syncPeerState{})

	_, otherRec := schedulerPeer(t, sm, 112, 3)

	sm.resetHeaderState(&anchor, 10)
	sm.headersFirstMode.Store(true)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: shortPeer})
	require.Equal(t, len(hashes)+1, sm.headerListLen())

	// Delivering the headers is itself a claim, so put the claim back where the
	// test wants it: a peer that has told us about nothing above height 3.
	state, exists := sm.peerStates.Get(shortPeer)
	require.True(t, exists)
	state.bestKnownHeight.Store(3)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return shortRec.count() == len(hashes) }, 5*time.Second),
		"with nobody claiming the height the walk must still ask somebody")
	require.Equal(t, hashes, shortRec.all())
	require.Zero(t, otherRec.count(), "and only the first peer with budget, not everybody")
}

// blockHeaderLookups counts how many times a pass asked the blockchain service
// whether we already hold a block.
func blockHeaderLookups(t *testing.T, sm *SyncManager) int {
	t.Helper()

	client, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok)

	n := 0

	for _, call := range client.Calls {
		if call.Method == "GetBlockHeader" {
			n++
		}
	}

	return n
}

// TestScheduler_DoesNotAskTheBlockchainAboutBlocksItCannotHandOut bounds the
// cost of a pass. Each candidate header costs one "do we already have this?"
// question, and that is a gRPC round trip to the blockchain service on a context
// with no deadline. A pass must therefore only ask about the headers it could
// actually hand to a peer: with the node-wide window at its default of 1024 and
// one peer able to take 16, walking 60 headers would make 60 round trips to
// place 16 blocks, and this runs on every arriving block.
func TestScheduler_DoesNotAskTheBlockchainAboutBlocksItCannotHandOut(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xde}
	msg, _ := linkedHeaders(anchor, 60, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, syncRec := schedulerPeer(t, sm, 113, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	before := blockHeaderLookups(t, sm)

	sm.fetchHeaderBlocks()

	budget := schedulerPeerBudget(sm)

	require.True(t, WaitUntil(func() bool { return syncRec.count() == budget }, 5*time.Second),
		"the pass should have handed out one peer's budget")
	require.Equal(t, budget, blockHeaderLookups(t, sm)-before,
		"a pass must not ask the blockchain about headers it has no budget to hand out")
}

// TestScheduler_TheNodeWideWindowCountsWhatIsAlreadyInFlight is the other half
// of TestScheduler_RespectsTheNodeWideWindow. That one starts with an empty
// ledger, so it cannot tell a node-wide ceiling from a per-pass one: with
// nothing outstanding the two are the same number. This one starts with blocks
// already in flight, which is the state every pass after the first runs in.
//
// The three outstanding blocks are owed by a peer that takes no part in the pass,
// so the only budget they can touch is the node-wide window.
func TestScheduler_TheNodeWideWindowCountsWhatIsAlreadyInFlight(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd9}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.BlockDownloadWindow = 5

	syncPeer, syncRec := schedulerPeer(t, sm, 140, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, secondRec := schedulerPeer(t, sm, 141, 1000)

	elsewhere, _, _ := connectRacePeer(t, 142, 1000)
	for i := 0; i < 3; i++ {
		require.True(t, sm.blockDownloads.Add(elsewhere, chainhash.Hash{0xf0, byte(i)}))
	}

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 2 }, 5*time.Second),
		"the window's remaining two slots should have been requested")

	require.Equal(t, 2, syncRec.count()+secondRec.count(),
		"a node-wide window of 5 with 3 already in flight leaves 2, however many peers are available")
	require.Equal(t, hashes[0:2], syncRec.all())
	require.Equal(t, 5, sm.blockDownloads.Len(), "and the node is now at its window, not above it")

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the cursor must stay in the list")
	require.Equal(t, hashes[2], cursor, "the first header the window could not cover must still be next")
}

// TestScheduler_ADisconnectedPeerIsNotAskedForAnything is the fan-out's own
// admission test. QueueMessage returns silently for a peer whose socket has
// gone, so nothing on the wire says the request was lost — but the ledger would
// have recorded the blocks as owed by a peer that can never deliver them, and the
// walk would have advanced past them into exactly the stranded state a departing
// peer leaves behind.
//
// The disconnected peer is the only peer, so the pass has to place nothing at all
// and leave the cursor where it is: advancing past a header nobody was asked for
// loses that block from the walk for good.
func TestScheduler_ADisconnectedPeerIsNotAskedForAnything(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xda}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	gone, goneRec := schedulerPeer(t, sm, 143, 1000)
	sm.storeSyncPeer(gone, &syncPeerState{})

	seedFetchHeaders(t, sm, gone, anchor, msg)

	gone.DisconnectWithInfo("test: the peer's socket has gone")
	require.True(t, WaitUntil(func() bool { return !gone.Connected() }, 5*time.Second),
		"the peer should have registered as disconnected")

	sm.fetchHeaderBlocks()

	require.Zero(t, sm.blockDownloads.Len(),
		"a block owed by a peer that cannot deliver it is a block nothing will ever ask for again")
	require.Zero(t, goneRec.count())

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the cursor must stay in the list")
	require.Equal(t, hashes[0], cursor, "and on the first header, which nobody was asked for")
}

// TestScheduler_ANonCandidatePeerIsNotAskedForAnything is the same admission
// test for the other half of the rule. A peer we hold state for but have not
// accepted as a sync candidate is not a body source either.
func TestScheduler_ANonCandidatePeerIsNotAskedForAnything(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xdb}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	peer, peerRec := schedulerPeer(t, sm, 144, 1000)

	seedFetchHeaders(t, sm, peer, anchor, msg)

	state, exists := sm.peerStates.Get(peer)
	require.True(t, exists)
	state.syncCandidate = false

	sm.fetchHeaderBlocks()

	require.Zero(t, sm.blockDownloads.Len(), "a peer that is not a sync candidate must not be handed a slice")
	require.Zero(t, peerRec.count())

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the cursor must stay in the list")
	require.Equal(t, hashes[0], cursor)
}

// TestScheduler_NeverAsksTheSamePeerTwiceForAReopenedBlock pins the other half
// of the "never ask twice" rule. Its sibling above covers a block still live
// with another peer, which RequestedWithin catches. This covers the block the
// reopen deliberately made re-requestable while leaving its owner in place —
// where RequestedWithin answers false on purpose, and nothing kept the walk from
// landing the block back on the very peer that already holds the request.
//
// A peer asked twice answers twice. The first copy discharges its obligation, so
// the second arrives owned by nobody, and peer_server evicts that peer's whole
// association for sending a block we asked it for. The block must instead stay
// where it is, with the one peer that has the request.
func TestScheduler_NeverAsksTheSamePeerTwiceForAReopenedBlock(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd9}
	msg, hashes := linkedHeaders(anchor, 6, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	// One peer only, so the assigner has no choice but to offer the reopened
	// block back to the peer that already owes it.
	syncPeer, syncRec := schedulerPeer(t, sm, 96, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// Exactly the state a demotion leaves behind: the peer was asked, then let
	// off, so the block is re-requestable but the peer is still its owner.
	require.True(t, sm.blockDownloads.Add(syncPeer, hashes[0]))
	require.Equal(t, []chainhash.Hash{hashes[0]}, sm.blockDownloads.ForgetForRetryPeer(syncPeer, blockRequestRetryInterval))
	require.False(t, sm.blockDownloads.RequestedWithin(hashes[0], blockRequestRetryInterval),
		"sanity: the reopen is what makes the block re-requestable at all")

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() >= len(hashes)-1 }, 5*time.Second),
		"the rest of the run must still be asked for")

	require.Equal(t, hashes[1:], syncRec.all(),
		"the reopened block must not be asked for a second time, and the walk must carry on past it")

	require.True(t, sm.blockDownloads.HasOwner(syncPeer, hashes[0]),
		"the block has to stay owed by the peer that holds the request, or its copy arrives unowned")
}

// TestScheduler_DoesNotReadFurtherAheadThanTheLookaheadLimit pins
// legacy_blockDownloadLowerWindow, which is the one download bound svnode has and
// we did not.
//
// The two we already had count requests: how many the node may have outstanding,
// and how many any one peer may owe. Neither says anything about how far ahead of
// itself the node reads, and that is the quantity that decides how much disk the
// park needs — blocks commit strictly in order, so a block fetched a long way
// ahead of the one being waited on cannot be committed when it arrives and sits
// parked until everything between it and the chain has landed.
//
// The second half of the test is the part that matters: the limit has to be a rate
// and not a stop. Once the frontier moves, the window moves with it.
func TestScheduler_DoesNotReadFurtherAheadThanTheLookaheadLimit(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xda}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	// Budgets deliberately left wide, so the lookahead limit is the only thing
	// that can bind.
	sm.settings.Legacy.BlockDownloadLowerWindow = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 88, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// seedFetchHeaders anchors the list at height 10, so the seeded headers are
	// heights 11 upwards and a limit of 4 reaches height 14 — the first four.
	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() >= 4 }, 5*time.Second),
		"the blocks inside the window must still be asked for")

	require.Equal(t, hashes[0:4], syncRec.all(),
		"nothing beyond the lookahead limit may be asked for, however much budget is left")

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the walk must still have somewhere to resume from")
	require.Equal(t, hashes[4], cursor,
		"and it must stop ON the first header it would not ask for, or that block leaves the walk")

	// The frontier moves: the anchor block arrives and leaves the front of the
	// list, so the window slides up with it and the next header becomes fetchable.
	require.True(t, sm.blockDownloads.Add(syncPeer, anchor))
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() >= 5 }, 5*time.Second),
		"a limit that never lets go once the frontier moves is a stall, not a window")
	require.Equal(t, hashes[0:5], syncRec.all(),
		"exactly one more block comes into range for one block committed")
}

// TestScheduler_AReassertedAssignmentSpendsBudgetLikeARequest pins the half of
// the pass's arithmetic that used to exempt itself.
//
// When the assigner hands back the peer that already owns the hash, the walk
// re-arms the record it holds instead of asking twice, which is right. What it
// did not do was charge for it. ReassertOwner clears the forgiven flag, so the
// block is back in CountForPeer from that moment, while both budgets were
// computed before the pass ran with the forgiven records excluded. Every
// reassert therefore added one to the peer's live in-flight count and took
// nothing out of the pass, so a demoted peer whose slice had just been reopened
// could finish one pass owing its whole reopened slice plus another
// legacy_maxBlocksInTransitPerPeer on top.
//
// The peer here is at exactly that starting point: four blocks owed, all of them
// reopened. One pass may leave it owing four, never eight.
func TestScheduler_AReassertedAssignmentSpendsBudgetLikeARequest(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd8}
	msg, hashes := linkedHeaders(anchor, 8, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	perPeer := schedulerPeerBudget(sm)
	require.Equal(t, 4, perPeer, "sanity: the ladder must not be what caps this pass")

	peer, rec := schedulerPeer(t, sm, 92, 1000)
	sm.storeSyncPeer(peer, &syncPeerState{})

	for i := 0; i < perPeer; i++ {
		require.True(t, sm.blockDownloads.Add(peer, hashes[i]))
	}

	// Demotion reopens the peer's own slice: the records are kept, so the peer
	// may still deliver, but they are forgiven, so they spend no budget and the
	// walk is free to place the same hashes again.
	require.Len(t, sm.blockDownloads.ForgetForRetryPeer(peer, blockRequestRetryInterval), perPeer)
	require.Zero(t, sm.blockDownloads.CountForPeer(peer), "sanity: a reopened slice spends no budget")

	seedFetchHeaders(t, sm, peer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.LessOrEqual(t, sm.blockDownloads.CountForPeer(peer), perPeer,
		"one pass must not leave a peer owing more than its in-flight cap")
	require.Equal(t, perPeer, sm.blockDownloads.CountForPeer(peer),
		"the four reasserts are the whole of this pass's budget")
	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, time.Second),
		"the peer already holds all four requests, so nothing new may go out on the wire")
}
