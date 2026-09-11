package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newFetchLockManager builds a manager wired for fetchHeaderBlocks. Any hash in
// alreadyHave is answered as a block we already hold, so the walk skips it;
// everything else comes back not-found and is therefore requested. When gate is
// non-nil the blockchain lookup parks on it, and entered is closed the first
// time the lookup is reached, which is how a test gets a competing goroutine to
// run at the exact moment the lookup is in progress.
func newFetchLockManager(t *testing.T, alreadyHave []chainhash.Hash, gate, entered chan struct{}) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	// Specific expectations must be registered before the catch-all, because
	// testify takes the first matching one.
	for i := range alreadyHave {
		hash := alreadyHave[i]
		blockchainClient.Mock.On("GetBlockHeader", mock.Anything, &hash).
			Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	}

	lookup := blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	if gate != nil {
		var once sync.Once

		lookup.Run(func(mock.Arguments) {
			if entered != nil {
				once.Do(func() { close(entered) })
			}

			<-gate
		})
	}

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	// Far above anything these tests generate, so the checkpoint branches never
	// fire and the plain header-walk path is what runs.
	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	return sm
}

// seedFetchHeaders lands a prebuilt headers batch behind anchor, leaving
// startHeader on the first of them — exactly the state a real node is in when a
// headers batch has just arrived. The batch is passed in rather than built here
// because a caller may need the hashes before the manager exists.
func seedFetchHeaders(t *testing.T, sm *SyncManager, p *peerpkg.Peer, anchor chainhash.Hash, msg *wire.MsgHeaders) {
	t.Helper()

	sm.resetHeaderState(&anchor, 10)
	// resetHeaderState turns headers-first mode off; the walk under test only
	// runs in headers-first mode.
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: p})

	require.Equal(t, len(msg.Headers)+1, sm.headerListLen(), "the seeded headers should all have linked")
}

// TestFetchHeaderBlocks_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup is the
// stall this fix exists to remove. fetchHeaderBlocks asks the blockchain service
// whether we already have each candidate block, and that is a gRPC round-trip on
// a context with no deadline. Holding the header lock across it makes every
// other header-list user — including the block-queue consumer, the narrowest
// goroutine in the service — wait on a remote service.
//
// Bounding the number of round-trips is not the same as bounding the time they
// take, and time is the property that matters to a goroutine waiting on the
// lock.
func TestFetchHeaderBlocks_DoesNotHoldTheHeaderLockAcrossTheBlockchainLookup(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)

	syncPeer, _, _ := connectRacePeer(t, 40, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa1}
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

	// The block-queue consumer's first act in headers-first mode is to take this
	// same lock, so this stand-in for it must not be stuck behind a remote call.
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

// TestFetchHeaderBlocks_RequestsExactlyTheBlocksItRequestedBefore pins the
// contents of the getdata, so the restructure cannot quietly change what is
// asked for. With nothing racing and one peer to ask, one pass must ask for the
// first budget's worth of headers it does not already have, in list order,
// skipping the ones we hold without requesting them and without leaving them
// behind, and must leave startHeader on the first header it did not consider.
func TestFetchHeaderBlocks_RequestsExactlyTheBlocksItRequestedBefore(t *testing.T) {
	const seeded = 30

	// The batch is built before the manager so the mock can be told which of
	// these blocks we already hold.
	var nonce uint32

	anchor := chainhash.Hash{0xa2}
	msg, hashes := linkedHeaders(anchor, seeded, &nonce)

	// Two of the headers are blocks we already hold: one at the very front, so
	// the skip happens before anything is requested, and one in the middle.
	have := []chainhash.Hash{hashes[0], hashes[5]}

	sm := newFetchLockManager(t, have, nil, nil)

	syncPeer, _, rec := connectRacePeer(t, 42, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	// One pass, one peer: what bounds it is that peer's own budget.
	maxBlocks := schedulerPeerBudget(sm)

	// Everything in list order except the two we already hold, capped at the
	// in-flight limit.
	want := make([]chainhash.Hash, 0, maxBlocks)
	lastConsidered := 0

	for i := 0; i < seeded && len(want) < maxBlocks; i++ {
		lastConsidered = i

		if hashes[i] == have[0] || hashes[i] == have[1] {
			continue
		}

		want = append(want, hashes[i])
	}

	require.Eventually(t, func() bool { return rec.count() >= len(want) }, 5*time.Second, 10*time.Millisecond,
		"the getdata never arrived at the peer")
	require.Equal(t, want, rec.all(), "the getdata must ask for the same blocks, in the same order, as before")

	for _, h := range want {
		require.True(t, sm.blockDownloads.HasOwner(syncPeer, h), "every requested block must be recorded against the peer we asked")
	}

	for _, h := range have {
		require.False(t, sm.blockDownloads.HasOwner(syncPeer, h), "a block we already hold must not be recorded as requested")
	}

	sm.headerMu.Lock()
	startHeader := sm.startHeader
	sm.headerMu.Unlock()

	require.NotNil(t, startHeader, "startHeader must still be anchored in the list")

	node, ok := startHeader.Value.(*headerNode)
	require.True(t, ok)
	require.Equal(t, hashes[lastConsidered+1], *node.hash,
		"startHeader must be left on the first header the walk did not consider")
}

// TestFetchHeaderBlocks_DiscardsARoundWhoseHeaderListMovedUnderIt is the price of
// doing the lookups unlocked: what was read before the lock was dropped can no
// longer be trusted on the way back. Here the header state is reset while the
// lookups are in flight, which is what happens when sync moves to a new peer or
// leaves headers-first mode.
//
// A commit that trusted its snapshot would ask for blocks from the chain we just
// abandoned and re-anchor startHeader onto an element that is no longer in the
// list — container/list leaves a detached element's links intact, so the walk
// happily carries on through a list nobody can reach any more, and the header
// list never drains again. Nothing must be committed, and the next pass starts
// from the list as it now is.
func TestFetchHeaderBlocks_DiscardsARoundWhoseHeaderListMovedUnderIt(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)

	syncPeer, _, rec := connectRacePeer(t, 43, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa3}
	msg, hashes := linkedHeaders(anchor, 25, &nonce)

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

	// Sync starts over from a different point entirely while the lookups are
	// parked. The list, the index and startHeader all go with it.
	sm.resetHeaderState(&chainhash.Hash{0xb3}, 500)
	sm.headersFirstMode.Store(true)

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("fetchHeaderBlocks never returned")
	}

	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, 500*time.Millisecond),
		"no block from the abandoned chain may be asked for")

	for _, h := range hashes {
		require.False(t, sm.blockDownloads.HasOwner(syncPeer, h),
			"no block from the abandoned chain may be recorded as requested")
	}

	sm.headerMu.Lock()
	startHeader := sm.startHeader
	sm.headerMu.Unlock()

	require.Nil(t, startHeader,
		"the reset left nothing to fetch, and a discarded round must not re-anchor startHeader into the abandoned list")
}

// TestFetchHeaderBlocks_DiscardsARoundWhoseStartHeaderWasRemovedUnderIt is the
// case a pointer comparison cannot see, and the reason the commit consults the
// hash index as well. handleBlockMsg removes the front of the header list when
// that block arrives, and the front is startHeader itself whenever the block
// queue has caught up with the requests. startHeader is left pointing at the
// removed element, so it still compares equal to the element the round was
// walked from — but a removed container/list element answers Next() with nil, so
// a commit that trusted the comparison would ask again for the block that has
// just arrived and then advance startHeader to nil, abandoning every header
// queued behind it with no way back.
func TestFetchHeaderBlocks_DiscardsARoundWhoseStartHeaderWasRemovedUnderIt(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)

	syncPeer, _, rec := connectRacePeer(t, 44, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa4}
	msg, hashes := linkedHeaders(anchor, 25, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The anchor block arrives, so it leaves the front of the list and the first
	// unfetched header takes its place: front and startHeader are now the same
	// element. The ledger has to vouch for it or handleBlockMsg drops the peer
	// before it gets as far as the header list, and carrying no block makes it
	// return straight after the bookkeeping that matters here.
	sm.blockDownloads.Add(syncPeer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

	sm.headerMu.Lock()
	frontIsStartHeader := sm.headerList.Front() == sm.startHeader
	sm.headerMu.Unlock()

	require.True(t, frontIsStartHeader, "the front of the list should now be the header the walk starts from")

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

	// And now that same block arrives while the lookups are parked, taking the
	// element startHeader points at out of the list with it.
	sm.blockDownloads.Add(syncPeer, hashes[0])
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: hashes[0], peer: syncPeer})

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("fetchHeaderBlocks never returned")
	}

	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, 500*time.Millisecond),
		"the block that has already arrived must not be asked for again")

	sm.headerMu.Lock()
	startHeader := sm.startHeader
	remaining := sm.headerList.Len()
	sm.headerMu.Unlock()

	require.Equal(t, 24, remaining, "the headers behind the arrived block are still queued")
	require.NotNil(t, startHeader, "advancing startHeader off a removed element would abandon every header still queued")
}

// TestCommitHeaderCandidates_RefusesASecondCommitFromTheSameSnapshot pins the
// half of the guard that compares startHeader with the anchor the round was
// walked from. Two rounds of fetchHeaderBlocks run concurrently in a live node —
// the block-queue consumer starts one on every block that arrives and each
// headers message goroutine starts another — so two rounds walking from the same
// anchor is ordinary, not exotic. Both do their blockchain lookups unlocked and
// then both try to commit.
//
// The second one must commit nothing. Nothing was removed from the list, so the
// hash index still resolves the anchor perfectly and only the startHeader
// comparison can tell that the first round already consumed those headers.
// Without it the same run of blocks is requested from the same peer twice and
// startHeader is dragged backwards over headers already in flight.
//
// No goroutines and no timing: one snapshot, two commits.
func TestCommitHeaderCandidates_RefusesASecondCommitFromTheSameSnapshot(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 46, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa6}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// One reading of the list, shared by both rounds — exactly what two
	// concurrent walks from the same startHeader come back holding.
	snapshot, anchorEl, anchorHash, ok := sm.snapshotHeaderCandidates(len(hashes))
	require.True(t, ok)
	require.Equal(t, hashes, snapshot, "the snapshot should be the whole seeded run")

	alreadyHave := make([]bool, len(snapshot))

	firstAssigner := sm.newDownloadAssigner()
	require.NotNil(t, firstAssigner, "one connected sync candidate is all a pass needs")

	requested, _ := sm.commitHeaderCandidates(firstAssigner, anchorEl, anchorHash, snapshot, alreadyHave)
	require.Equal(t, len(snapshot), requested, "the first round should request every header it walked")
	require.Len(t, firstAssigner.peers[0].getData.InvList, len(snapshot))

	sm.headerMu.Lock()
	afterFirst := sm.startHeader
	sm.headerMu.Unlock()

	// The anchor is still in the list and the index still resolves it, so the
	// index half of the guard cannot catch this one.
	sm.headerMu.Lock()
	stillIndexed := sm.headerIndex[anchorHash] == anchorEl
	sm.headerMu.Unlock()
	require.True(t, stillIndexed, "the first commit removes nothing, so the anchor is still the live holder of its hash")

	secondAssigner := sm.newDownloadAssigner()
	require.NotNil(t, secondAssigner)

	requestedAgain, more := sm.commitHeaderCandidates(secondAssigner, anchorEl, anchorHash, snapshot, alreadyHave)

	require.Zero(t, requestedAgain, "a round whose headers another round already took must request nothing")
	require.Nil(t, secondAssigner.peers[0].getData, "no block may be asked for a second time off the same snapshot")

	require.False(t, more, "there is nothing more to do with a snapshot that has been overtaken")

	sm.headerMu.Lock()
	afterSecond := sm.startHeader
	sm.headerMu.Unlock()

	require.Equal(t, afterFirst, afterSecond, "the second round must not drag startHeader back over headers already in flight")
}
