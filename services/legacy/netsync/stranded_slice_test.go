package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// strandedRun puts eight headers behind an anchor, hands the first four to the
// sync peer and the last four to a fan-out peer, and returns the hashes, both
// recorders and the fan-out peer. Every peer's queue is capped at four, so the
// run cannot be carried by one peer and the split is deterministic.
//
// It leaves the node in the state both tests below are about: the download
// cursor is off the end of the list, and four of the blocks it walked past are
// owed by a peer that is about to stop delivering them.
func strandedRun(t *testing.T, sm *SyncManager, syncIdx, fanoutIdx uint8) ([]chainhash.Hash, *getDataRecorder, *getDataRecorder, *peerpkg.Peer) {
	t.Helper()

	var nonce uint32

	anchor := chainhash.Hash{syncIdx}
	msg, hashes := linkedHeaders(anchor, 8, &nonce)

	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	syncPeer, syncRec, _ := demotionPeer(t, sm, syncIdx, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{lastBlockTime: time.Now()})
	syncPeer.SetSyncPeer(true)

	fanout, fanoutRec, _ := demotionPeer(t, sm, fanoutIdx, 1000)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 4 && fanoutRec.count() == 4 }, 5*time.Second),
		"the run should have been split across both peers")
	require.Equal(t, hashes[0:4], syncRec.all(), "the sync peer takes the first contiguous run")
	require.Equal(t, hashes[4:8], fanoutRec.all(), "the fan-out peer takes the second")

	_, onHeader := startHeaderHash(t, sm)
	require.False(t, onHeader, "the walk is forward-only, so the cursor is now off the end of the list")

	return hashes, syncRec, fanoutRec, fanout
}

// TestDonePeer_TheDepartedPeersSliceIsAskedOfSomebodyElse is the whole of the
// lost-peer hazard. Block bodies now come from whichever peer the scheduler
// handed that slice to, and the walk that handed it out is forward-only — so
// when a fan-out peer goes away, the run it was carrying is behind the cursor
// and nothing asks for it again. It is also the front of the header list, so
// nothing behind it can commit either.
//
// The recovery the sync peer already has for exactly this (reopenDemotedPeerSlice)
// is what this pins for every other peer the scheduler hands work to: the
// departed peer's blocks have to end up asked of a peer that is still here.
//
// The rescue peer joins after the split, so the only thing standing between the
// stranded run and a fresh request is the cursor.
func TestDonePeer_TheDepartedPeersSliceIsAskedOfSomebodyElse(t *testing.T) {
	sm := newDemotionManager(t)

	hashes, syncData, _, fanout := strandedRun(t, sm, 120, 121)

	_, rescueData, _ := demotionPeer(t, sm, 122, 1000)

	sm.handleDonePeerMsg(fanout)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rescueData.count() == 4 }, 5*time.Second),
		"the departed peer's four blocks must be asked of a peer that is still here")
	require.Equal(t, hashes[4:8], rescueData.all(), "and it must be that run, not some other part of the list")

	require.Equal(t, hashes[0:4], syncData.all(),
		"the sync peer's own in-flight blocks must not be asked for a second time")
	require.Equal(t, 8, sm.blockDownloads.Len(), "every block in the run is owed by somebody again")
}

// TestNotFound_TheBlockComesBackToTheWalk is the same hazard reached without
// losing the peer. A peer that answers notfound has told us its copy is never
// coming — it may have pruned the block, or it may have been asked for a height
// it never claimed, which take() deliberately does. Leaving the hash owed for
// the hour-long assignment ceiling with the cursor already past it strands it
// exactly as a departure does, and costs that peer a queue slot for the hour.
//
// Who ends up carrying it is take()'s ordinary choice — in a two-peer node the
// peer with a free slot is the one that just answered, and it may well be asked
// again. What is pinned here is that the block comes back into the walk at all,
// and that the peer's obligation for it is discharged rather than held for an
// hour it can never satisfy.
func TestNotFound_TheBlockComesBackToTheWalk(t *testing.T) {
	sm := newDemotionManager(t)

	hashes, syncData, fanoutData, fanout := strandedRun(t, sm, 123, 124)

	notFound := wire.NewMsgNotFound()
	require.NoError(t, notFound.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hashes[5])))

	sm.NotFound(notFound, fanout)

	require.False(t, sm.blockDownloads.HasOwner(fanout, hashes[5]),
		"a peer that has told us it does not have the block must not still be down for it")
	require.True(t, sm.blockDownloads.HasOwner(fanout, hashes[6]),
		"and the rest of its run must be untouched")

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return fanoutData.count() == 5 }, 5*time.Second),
		"the block nobody owes any more must be asked for again")
	require.Equal(t, append(append([]chainhash.Hash{}, hashes[4:8]...), hashes[5]), fanoutData.all(),
		"and only that block: the rest of the run is still owed by a live peer")

	require.Equal(t, hashes[0:4], syncData.all(),
		"the other peer's in-flight blocks must not be asked for a second time")
}
