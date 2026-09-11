package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ARewoundHeaderInFrontOfTheAnchorSurvivesTheCheckpointTrim is
// the first of the two ways the trim used to lose a real header.
//
// The checkpoint trim used to remove Front() on the strength of a comment: the
// first entry of the list is always the block already in the database. A rewind
// after a checkpoint transition makes that false. The transition does not rebuild
// the list — it leaves the old checkpoint node in it as the new anchor — so the
// epoch check that stops a stale header node going back into a rebuilt list does
// not fire, and reinsertHeaderLocked inserts by height, which puts a header from
// the round just finished IN FRONT of the anchor.
//
// Removing the front there deletes the block that was just put back to be asked
// for again, from the list and from the index, and leaves the anchor in place to
// wedge the front all over again. Nothing after it ever matches the front, so
// the checkpoint block is never recognised as the checkpoint, the next round of
// headers is never asked for, and sync sits until the 180-second stall detector
// rotates the peer.
//
// The end state asserted is the one the round needs: every block in the list is
// asked for — the rewound one included — and the checkpoint block still reports
// itself as the checkpoint when it arrives. Blocks are delivered only if they
// were actually asked for, which is what a peer does, so the test cannot heal a
// walk that asked for the wrong thing.
func TestSyncManager_ARewoundHeaderInFrontOfTheAnchorSurvivesTheCheckpointTrim(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 88, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	// The anchor: the block this node already has, at height 10.
	anchor := chainhash.Hash{0xa1}
	msg, round := linkedHeaders(anchor, 5, &nonce)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 15, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// A block from the round that has just finished was parked, so its header
	// node travelled with the parked entry, and now its time has run out. The
	// node carries the epoch of the list it left, and a checkpoint transition
	// does not bump the epoch, so this is a node the list still accepts.
	sm.headerMu.Lock()
	epoch := sm.headerListEpoch
	sm.headerMu.Unlock()

	givenUp := chainhash.Hash{0x51}
	require.True(t, sm.rewindHeaderCursor(givenUp, &headerNode{height: 5, hash: &givenUp, listEpoch: epoch}),
		"a block given up on after the transition is put back into the list")

	sm.headerMu.Lock()
	front := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, givenUp.String(), front.hash.String(),
		"the rewound header sits in front of the anchor, because the list runs in ascending height")

	// The round's headers land and reach the checkpoint, which is what trims the
	// anchor and sends the round's first getdata.
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	// Every block this node is asked for is delivered exactly once, in chain
	// order, the way a sync peer answers a getdata.
	wanted := append([]chainhash.Hash{givenUp}, round...)
	delivered := make(map[chainhash.Hash]bool, len(wanted))
	sawCheckpoint := false

	deliverWhatWasAskedFor := func() {
		for _, hash := range wanted {
			if delivered[hash] || !sm.blockDownloads.RequestedWithin(hash, time.Minute) {
				continue
			}

			delivered[hash] = true

			if isCheckpoint, _ := sm.advanceHeaderListFor(hash); isCheckpoint {
				sawCheckpoint = true
			}
		}
	}

	deliverWhatWasAskedFor()
	sm.fetchHeaderBlocks()
	deliverWhatWasAskedFor()

	require.True(t, delivered[givenUp],
		"the rewound block must still be asked for; the trim must not delete the header that was just put back for it")

	for _, hash := range round {
		require.True(t, delivered[hash], "every header in the round must be asked for, including %s", hash)
	}

	require.False(t, sm.blockDownloads.RequestedWithin(anchor, time.Minute),
		"the anchor is already in this node's database and must never be asked for")

	require.True(t, sawCheckpoint,
		"the checkpoint block must still be recognised as the checkpoint, or the round never ends and no further headers are ever asked for")

	sm.headerMu.Lock()
	remaining := sm.headerList.Len()
	last := sm.headerList.Front().Value.(*headerNode)
	_, anchorStillIndexed := sm.headerIndex[anchor]
	sm.headerMu.Unlock()

	require.Equal(t, 1, remaining, "every block of the round must have come off the front of the list")
	require.Equal(t, checkpoint.String(), last.hash.String(),
		"what is left is the checkpoint node, kept to anchor the next round")
	require.False(t, anchorStillIndexed, "the anchor must be out of the index as well as out of the list")
}

// TestSyncManager_AnAnchorDeliveredEarlyDoesNotCostTheRoundItsFirstBlock is the
// second way, and the cheaper one to reach.
//
// While a round's headers are still coming in, the front of the list is the
// anchor and the cursor is on the first real header behind it. A rewind in that
// window used to publish the anchor as the download frontier, so once the
// frontier had been stuck for legacy_blockSlowFetchTimeout the racer asked a
// second peer for it — and that peer has the block, because it is already in our
// chain. The reply takes the anchor off the front early.
//
// The checkpoint batch then removed the front, which was no longer the anchor
// but the round's FIRST REAL HEADER. It went from the list and from the index
// and was never requested, so in production every block above it in the round
// arrives as an orphan of a block nobody will ask for again, they all park, the
// checkpoint block parks with them, and the round stalls until the peer is
// rotated.
func TestSyncManager_AnAnchorDeliveredEarlyDoesNotCostTheRoundItsFirstBlock(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 89, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	// Four headers in the first batch, two more in the second, the checkpoint on
	// the last of them — a round in two batches, which is every round on
	// mainnet, where the gaps run to 50,000 blocks.
	anchor := chainhash.Hash{0xa2}
	first, firstHashes := linkedHeaders(anchor, 4, &nonce)
	second, secondHashes := linkedHeaders(firstHashes[3], 2, &nonce)

	round := append(append([]chainhash.Hash{}, firstHashes...), secondHashes...)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: first, peer: syncPeer})

	// The raced copy of the anchor lands. This is what an arriving block does to
	// the header list, and it is the only thing it does to it.
	isCheckpoint, removedFront := sm.advanceHeaderListFor(anchor)
	require.False(t, isCheckpoint)
	require.NotNil(t, removedFront, "the anchor was the front, so its arrival took it off the list")

	sm.headerMu.Lock()
	front := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, round[0].String(), front.hash.String(),
		"with the anchor gone the round's first real header is the front, and it is not the anchor")

	// The second batch reaches the checkpoint, so the trim runs — and has
	// nothing to remove.
	sm.handleHeadersMsg(&headersMsg{headers: second, peer: syncPeer})

	delivered := make(map[chainhash.Hash]bool, len(round))
	sawCheckpoint := false

	deliverWhatWasAskedFor := func() {
		for _, hash := range round {
			if delivered[hash] || !sm.blockDownloads.RequestedWithin(hash, time.Minute) {
				continue
			}

			delivered[hash] = true

			if isCP, _ := sm.advanceHeaderListFor(hash); isCP {
				sawCheckpoint = true
			}
		}
	}

	deliverWhatWasAskedFor()
	sm.fetchHeaderBlocks()
	deliverWhatWasAskedFor()

	for _, hash := range round {
		require.True(t, delivered[hash],
			"every header in the round must be asked for; %s was dropped from the list by the trim and never requested", hash)
	}

	require.True(t, sawCheckpoint,
		"the checkpoint block must still be recognised as the checkpoint")

	sm.headerMu.Lock()
	remaining := sm.headerList.Len()
	sm.headerMu.Unlock()

	require.Equal(t, 1, remaining, "every block of the round must have come off the front of the list")
}

// TestSyncManager_TheAnchorIsNeverRacedFromASecondPeer closes the route into the
// case above at its source.
//
// publishFrontierLocked clears the frontier when the front has not been asked
// for yet, which it checks by comparing the front with the cursor. In the first
// half of a round both of those are true of different nodes: the front is the
// anchor and the cursor is on the first real header behind it. So a rewind there
// published a block already in our own chain as the block holding up sync, and
// the racer duly asked another peer for it.
//
// Both halves are driven here, because a frontier that is never published is as
// broken as one that publishes the wrong block: the anchor must not be raced,
// and the round's real front must be.
func TestSyncManager_TheAnchorIsNeverRacedFromASecondPeer(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 90, 1000)
	other, _, otherRec := connectRacePeer(t, 91, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa3}
	first, firstHashes := linkedHeaders(anchor, 4, &nonce)
	second, secondHashes := linkedHeaders(firstHashes[3], 2, &nonce)

	round := append(append([]chainhash.Hash{}, firstHashes...), secondHashes...)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: first, peer: syncPeer})

	// A block already asked for in this round is dropped and rewound, which
	// republishes the frontier. The front of the list is the anchor.
	require.True(t, sm.rewindHeaderCursor(firstHashes[1], nil))

	// Long enough for the race timer to fire.
	raceAt := time.Now().Add(time.Hour)

	sm.raceFrontierBlock(raceAt)

	require.False(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 500*time.Millisecond),
		"a second peer must never be asked for the anchor: it is already in this node's chain and nobody is waiting for it")

	// The round reaches its checkpoint, the anchor goes, and the real front is a
	// block we are genuinely waiting on. That one must be raced.
	sm.handleHeadersMsg(&headersMsg{headers: second, peer: syncPeer})

	require.True(t, sm.blockDownloads.RequestedWithin(round[0], time.Minute),
		"the round's first block should have been asked for")

	// Its arrival moves the list on and republishes the frontier, which is now
	// the next block: asked for, not arrived, and holding up everything behind
	// it.
	sm.advanceHeaderListFor(round[0])

	sm.raceFrontierBlock(time.Now().Add(time.Hour))

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 5*time.Second),
		"a real front block that is stuck must still be raced, or this guard has switched the feature off")
	require.Equal(t, []chainhash.Hash{round[1]}, otherRec.all(),
		"the block raced must be the one holding up sync")
}

// TestSyncManager_TheRoundAfterACheckpointTrimsTheCheckpointNode is the trim
// working for the second round and every round after it, which is where the
// anchor comes from once sync is running: not from a reset, but from the
// checkpoint node the previous round left behind.
//
// advanceHeaderListFor keeps the checkpoint node in the list so the next round's
// first header can prove it links to it, which makes it the next round's anchor —
// a block now in this node's chain that no peer will deliver again. Nothing about
// its position says so: it is the whole list at that point, and after the next
// batch it is simply the front. So the transition is where it has to be recorded,
// and if it is not, the trim at the next checkpoint finds nothing to remove, the
// front stays on a block already in the chain, and every round from the second
// one onwards wedges.
func TestSyncManager_TheRoundAfterACheckpointTrimsTheCheckpointNode(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, rec := connectRacePeer(t, 92, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa4}
	firstRound, firstHashes := linkedHeaders(anchor, 3, &nonce)
	firstCheckpoint := firstHashes[len(firstHashes)-1]

	sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 13, Hash: &firstCheckpoint}
	sm.headersFirstMode.Store(true)

	// Round one, start to finish: the headers arrive, the anchor is trimmed, and
	// every block is delivered in order.
	sm.handleHeadersMsg(&headersMsg{headers: firstRound, peer: syncPeer})

	for _, hash := range firstHashes {
		require.True(t, sm.blockDownloads.RequestedWithin(hash, time.Minute), "%s should have been asked for", hash)

		isCheckpoint, _ := sm.advanceHeaderListFor(hash)
		if !isCheckpoint {
			continue
		}

		// What handleBlockMsg does when the checkpoint block commits.
		require.NoError(t, sm.checkpointBlockCommitted(syncPeer, hash))
	}

	sm.headerMu.Lock()
	afterRoundOne := sm.headerList.Len()
	frontAfterRoundOne := sm.headerList.Front().Value.(*headerNode)
	sm.headerMu.Unlock()

	require.Equal(t, 1, afterRoundOne, "the checkpoint node is all that is left")
	require.Equal(t, firstCheckpoint.String(), frontAfterRoundOne.hash.String())

	// Round two: its headers link off the checkpoint node, which is now the
	// anchor. The real checkpoint list is not this test's, so aim the next
	// checkpoint at the last header of the new round the way the mainnet list
	// would.
	secondRound, secondHashes := linkedHeaders(firstCheckpoint, 3, &nonce)
	secondCheckpoint := secondHashes[len(secondHashes)-1]

	sm.headerMu.Lock()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &secondCheckpoint}
	sm.headerMu.Unlock()

	sm.handleHeadersMsg(&headersMsg{headers: secondRound, peer: syncPeer})

	delivered := make(map[chainhash.Hash]bool, len(secondHashes))
	sawCheckpoint := false

	for pass := 0; pass < 2; pass++ {
		for _, hash := range secondHashes {
			if delivered[hash] || !sm.blockDownloads.RequestedWithin(hash, time.Minute) {
				continue
			}

			delivered[hash] = true

			if isCheckpoint, _ := sm.advanceHeaderListFor(hash); isCheckpoint {
				sawCheckpoint = true
			}
		}

		sm.fetchHeaderBlocks()
	}

	for _, hash := range secondHashes {
		require.True(t, delivered[hash], "every header of the second round must be asked for, including %s", hash)
	}

	// Asked for once, in round one, and never again: it is in the chain now.
	// The download ledger cannot answer this — it still holds round one's
	// request — so this counts what actually went out on the wire.
	require.True(t, WaitUntil(func() bool { return rec.count() >= len(firstHashes)+len(secondHashes) }, 5*time.Second),
		"both rounds' getdata messages should have reached the peer")

	timesAsked := 0

	for _, asked := range rec.all() {
		if asked.IsEqual(&firstCheckpoint) {
			timesAsked++
		}
	}

	require.Equal(t, 1, timesAsked,
		"the previous round's checkpoint block is in the chain; nobody may be asked for it a second time")

	require.True(t, sawCheckpoint,
		"the second round's checkpoint must be recognised too, or sync stops at the second checkpoint of every fresh node")

	sm.headerMu.Lock()
	remaining := sm.headerList.Len()
	_, oldStillIndexed := sm.headerIndex[firstCheckpoint]
	sm.headerMu.Unlock()

	require.Equal(t, 1, remaining, "only the new checkpoint node may be left")
	require.False(t, oldStillIndexed, "the previous round's checkpoint node must be out of the index as well as the list")
}
