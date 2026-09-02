package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ATickBetweenHeaderBatchesDoesNotWedgeTheCheckpointTransition
// is about where the download walk may be driven from.
//
// A checkpoint round runs in two halves. First the headers come in, batch after
// batch, and while that is happening the front of the header list is still the
// previous round's anchor — a block that is already in the chain and will never
// be delivered again, kept only so the next header can prove it links. The
// anchor is taken off exactly once, by the batch that reaches the checkpoint,
// and only then may blocks be asked for.
//
// Anything that fetches blocks in the first half wedges the second. The blocks
// arrive while the anchor is the front, so they never match it and never come
// off the list; then the anchor is trimmed and the front becomes a block that
// has already been delivered. Nothing after that ever matches the front again,
// so the checkpoint block is never recognised as the checkpoint, the next round
// of headers is never asked for, and sync stops until the 180-second stall
// detector rotates the peer and rebuilds the list. Mainnet checkpoint gaps run
// to 50,000 blocks — 25 sequential header round-trips — so a thirty-second tick
// lands in that window routinely.
//
// The end state asserted here is the one the round needs: after a tick has
// landed mid-headers, the checkpoint block still reports itself as the
// checkpoint when it arrives. Blocks are delivered only if they were actually
// asked for, which is what a peer does, so the test cannot heal a walk that
// asked for a block at the wrong moment.
func TestSyncManager_ATickBetweenHeaderBatchesDoesNotWedgeTheCheckpointTransition(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 80, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	// The round: four headers in the first batch, two more in the second, and
	// the checkpoint on the last of them.
	anchor := chainhash.Hash{0xa7}
	first, firstHashes := linkedHeaders(anchor, 4, &nonce)
	second, secondHashes := linkedHeaders(firstHashes[3], 2, &nonce)

	chain := append(append([]chainhash.Hash{}, firstHashes...), secondHashes...)
	checkpoint := chain[len(chain)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// The first batch lands. It does not reach the checkpoint, so the anchor is
	// still the front of the list.
	sm.handleHeadersMsg(&headersMsg{headers: first, peer: syncPeer})

	sm.headerMu.Lock()
	front := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, anchor.String(), front.hash.String(),
		"between header batches the front of the list is still the previous round's anchor")

	// Every block this node is asked for is delivered exactly once, in chain
	// order, the way a sync peer answers a getdata.
	delivered := make(map[chainhash.Hash]bool, len(chain))
	sawCheckpoint := false

	deliverWhatWasAskedFor := func() {
		for _, hash := range chain {
			if delivered[hash] || !sm.blockDownloads.RequestedWithin(hash, time.Minute) {
				continue
			}

			delivered[hash] = true

			if isCheckpoint, _ := sm.advanceHeaderListFor(hash); isCheckpoint {
				sawCheckpoint = true
			}
		}
	}

	// The park sweep's ticker fires here, in the middle of the header round.
	sm.resumeHeaderWalk()
	deliverWhatWasAskedFor()

	// The second batch reaches the checkpoint, so handleHeadersMsg trims the
	// anchor and starts fetching blocks.
	sm.handleHeadersMsg(&headersMsg{headers: second, peer: syncPeer})
	deliverWhatWasAskedFor()

	require.True(t, sawCheckpoint,
		"the checkpoint block must still be recognised as the checkpoint, or the round never ends and no further headers are ever asked for")

	sm.headerMu.Lock()
	remaining := sm.headerList.Len()
	last := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, 1, remaining, "every block of the round must have come off the front of the list")
	require.Equal(t, checkpoint.String(), last.hash.String(),
		"what is left is the checkpoint node, kept to anchor the next round")
}

// TestSyncManager_AFreedInFlightSlotIsToppedUpMidRound is the other half of the
// same rule, and it is what stops the guard above from being "simplified" into
// the thing that quietly switches sync down to one block at a time.
//
// fetchMoreHeaderBlocks exists because a block that stops being outstanding
// without being committed — parked, or committed later off disk — is a silent
// loss of one in-flight slot. Nothing else notices, so without the top-up the
// pipeline drains a slot at a time until nothing is outstanding at all and every
// block waits a full round trip for the one before it.
//
// The state it has to work in is the ordinary forward walk, where the cursor is
// deliberately AHEAD of the front: the front is the block we are waiting on and
// the cursor is past everything already asked for. So "the cursor is on the
// front" — right for driving the walk from a timer, and true of a rewound cursor
// — is exactly false here. Using it as the condition for the top-up as well
// leaves every test in the package green and turns the pipeline off.
func TestSyncManager_AFreedInFlightSlotIsToppedUpMidRound(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 81, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// More headers than one pipeline holds, so there is something left to top up
	// with. The checkpoint is the last of them, which is the only shape in which
	// a real round ever fetches anything.
	var nonce uint32

	anchor := chainhash.Hash{0xa8}
	msg, chain := linkedHeaders(anchor, 25, &nonce)
	checkpoint := chain[len(chain)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 35, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// The batch reaches the checkpoint, so the anchor comes off the front and
	// the round's first getdata goes out.
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	requested := 0

	for _, hash := range chain {
		if !sm.blockDownloads.RequestedWithin(hash, time.Minute) {
			break
		}

		requested++
	}

	require.Greater(t, requested, 1, "the round's first fetch must have filled the pipeline")
	require.Less(t, requested, len(chain), "the round must be longer than one pipeline, or there is nothing left to top up with")

	// This is the forward-walk state: the front is the block we are waiting on,
	// and the cursor is past everything already asked for.
	sm.headerMu.Lock()
	cursorOnTheFront := sm.startHeader == sm.headerList.Front()
	sm.headerMu.Unlock()

	require.False(t, cursorOnTheFront, "mid-round the cursor is ahead of the front; a top-up that only ran when it was not would never run at all")

	// One of the blocks in flight stops being outstanding without being
	// committed — it was parked, which releases the peer's obligation and puts
	// nothing in the chain. It is not the front, so the walk is not moved on by
	// it and nothing else will notice the free slot.
	sm.blockDownloads.RemoveOwner(syncPeer, chain[1])

	next := chain[requested]

	require.False(t, sm.blockDownloads.RequestedWithin(next, time.Minute))

	sm.fetchMoreHeaderBlocks(syncPeer)

	require.True(t, sm.blockDownloads.RequestedWithin(next, time.Minute),
		"a freed in-flight slot must be refilled, or the pipeline drains one block at a time until sync is serial")
}

// TestSyncManager_AGivenUpBlockAfterACheckpointTransitionIsAskedForAgain is the
// third state the same guard has to get right, and it is the one neither test
// above can see.
//
// A checkpoint transition does not rebuild the header list. It leaves the
// checkpoint node in it as the next round's anchor and moves the checkpoint on
// to the next one, which on mainnet is tens of thousands of blocks higher. So
// for as long as it takes the next round of headers to arrive, the list holds
// one node — the anchor — sitting far below the checkpoint height.
//
// A block from the round that has just finished can be given up on in that
// window: it was parked when it arrived, its time ran out, and the sweep puts
// the walk back on it. Its header node carries the epoch of a list that is still
// the live list, so it goes back in — and because the list runs in ascending
// height it goes in AHEAD of the anchor. The front of the list is now a block
// this node wants and nobody has asked for since.
//
// Reading the answer off the list's tail cannot tell that state from the one
// above it: the tail is the anchor either way, and it is below the checkpoint
// height either way. So the top-up refuses to run, and the one thing that would
// have asked for the given-up block again does nothing. Nothing else asks — the
// getblocks the drop paths send is thrown away by processInvMsg while
// headers-first mode is on — so the block is lost until the stall detector
// rotates the peer.
//
// The end state asserted is the one sync needs: after the transition, a block
// given up on is asked for again.
func TestSyncManager_AGivenUpBlockAfterACheckpointTransitionIsAskedForAgain(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, rec := connectRacePeer(t, 82, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	// A round of five headers ending on a checkpoint at a real mainnet
	// checkpoint height, so that the transition finds a real next checkpoint
	// above it — which is what puts the anchor below the checkpoint height.
	anchor := chainhash.Hash{0xa9}
	msg, round := linkedHeaders(anchor, 5, &nonce)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 11106)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 11111, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// The batch reaches the checkpoint, so the anchor is trimmed and the round's
	// first getdata goes out.
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	// The round is delivered in order. The second block's arrival takes its
	// header off the front, and that header node is what the block carries with
	// it into the park.
	var parked *headerNode

	sawCheckpoint := false

	for i, hash := range round {
		require.True(t, sm.blockDownloads.RequestedWithin(hash, time.Minute),
			"the round's first fetch must have asked for %s", hash)

		isCheckpoint, removed := sm.advanceHeaderListFor(hash)
		if isCheckpoint {
			sawCheckpoint = true
		}

		if i != 1 {
			continue
		}

		require.NotNil(t, removed, "the block was the front, so its header came off the list with it")

		parked = removed

		// Parking releases the delivering peer's obligation: the block stops
		// being outstanding, and nothing has gone into the chain for it.
		sm.blockDownloads.RemoveOwner(syncPeer, hash)
	}

	require.True(t, sawCheckpoint, "the round must reach its checkpoint, or there is no transition to test")

	// The transition. The checkpoint node stays in the list as the next round's
	// anchor, and the checkpoint moves on to the one above it.
	require.NoError(t, sm.checkpointBlockCommitted(syncPeer, checkpoint))

	sm.headerMu.Lock()
	afterTransition := sm.headerList.Front().Value.(*headerNode)
	nextCheckpointHeight := sm.nextCheckpoint.Height
	sm.headerMu.Unlock()

	require.True(t, afterTransition.isAnchor, "the transition leaves the checkpoint node behind as the next round's anchor")
	require.Greater(t, nextCheckpointHeight, afterTransition.height,
		"the next round's checkpoint is far above the anchor, which is the whole window this test is about")

	givenUp := round[1]

	askedBefore := 0

	for _, hash := range rec.all() {
		if hash.IsEqual(&givenUp) {
			askedBefore++
		}
	}

	// The parked block's time runs out. The sweep gives it up and puts the walk
	// back on it, ahead of the anchor.
	require.True(t, sm.rewindHeaderCursor(givenUp, parked),
		"the transition did not rebuild the list, so the header node still belongs in it")

	sm.headerMu.Lock()
	front := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, givenUp.String(), front.hash.String(),
		"the front of the list is the rewound header, not the anchor")

	sm.fetchMoreHeaderBlocks(syncPeer)

	require.True(t, sm.blockDownloads.RequestedWithin(givenUp, time.Minute),
		"a block given up on after a checkpoint transition must be asked for again; while headers-first mode is on, nothing else will")

	require.True(t, WaitUntil(func() bool {
		asked := 0

		for _, hash := range rec.all() {
			if hash.IsEqual(&givenUp) {
				asked++
			}
		}

		return asked > askedBefore
	}, 5*time.Second), "the getdata for the given-up block must actually reach the peer")
}
