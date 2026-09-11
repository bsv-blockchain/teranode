package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestFetchHeaderBlocks_QueuedHeadersAreStillFetchedAfterTheAnchorLeavesTheList
// is the wedge. A round of fetchHeaderBlocks walks from startHeader with the
// header lock released, and while it is away the block that startHeader points
// at arrives: handleBlockMsg takes that element out of the list, leaving
// startHeader pointing at an element that is in no list at all.
//
// The round is right to commit nothing — it was walked from a reading of the
// list that no longer holds. What it must not do is leave startHeader detached,
// because nothing else ever repairs it: a detached container/list element
// answers Next() with nil, so every later round walks exactly one header and
// refuses again, and handleBlockMsg's "nothing left to fetch, ask for more"
// recovery reads the non-nil pointer as "there is still work queued" and never
// fires either. Sync then downloads nothing at all until the 180 second stall
// detector rotates the peer, forever.
//
// So the assertion is not about the pointer. It is that the headers still
// queued behind the arrived block actually get requested on a later pass.
func TestFetchHeaderBlocks_QueuedHeadersAreStillFetchedAfterTheAnchorLeavesTheList(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})

	sm := newFetchLockManager(t, nil, gate, entered)

	syncPeer, _, rec := connectRacePeer(t, 45, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa5}
	msg, hashes := linkedHeaders(anchor, 25, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The anchor block arrives and leaves the front of the list, so the first
	// unfetched header takes its place: the front of the list and the header the
	// walk starts from are now the same element. The ledger has to vouch for the
	// block or handleBlockMsg drops the peer before it reaches the header list,
	// and carrying no block makes it return right after the bookkeeping under
	// test here.
	sm.blockDownloads.Add(syncPeer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

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

	// And now that block arrives while the lookups are parked, taking the
	// element the walk was anchored on out of the list with it.
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

	require.Equal(t, 24, sm.headerListLen(), "the headers behind the arrived block are still queued")

	// A later pass — every block arrival runs one, and so does every headers
	// message. It must pick the queued headers up.
	sm.fetchHeaderBlocks()

	maxBlocks := schedulerPeerBudget(sm)

	want := make([]chainhash.Hash, 0, maxBlocks)
	for i := 1; i < len(hashes) && len(want) < maxBlocks; i++ {
		want = append(want, hashes[i])
	}

	require.True(t, WaitUntil(func() bool { return rec.count() >= len(want) }, 5*time.Second),
		"the headers queued behind the arrived block were never fetched — sync is wedged")
	require.Equal(t, want, rec.all(),
		"the next pass must ask for the queued headers, in list order, starting at the one after the arrived block")

	for _, h := range want {
		require.True(t, sm.blockDownloads.HasOwner(syncPeer, h),
			"every requested block must be recorded against the peer we asked")
	}
}
