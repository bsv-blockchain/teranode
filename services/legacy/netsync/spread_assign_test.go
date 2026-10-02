package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// A peer answers a getdata in the order it was asked, and there is no message to reorder its
// queue. Filling one peer with a run of blocks put the next block the chain needs behind four
// others at that peer: at height 705,000 its bytes began one and a half to six minutes after it
// was asked for, while other peers sat idle. Each block now goes, in height order, to the peer
// owing the fewest, so the next blocks the chain needs sit at the top of separate peers' queues.

func TestSchedulerGivesEachBlockToThePeerOwingTheFewest(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf1}
	msg, hashes := linkedHeaders(anchor, 3, &nonce)

	sm := schedulerManager(t)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 120, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	secondPeer, secondRec := schedulerPeer(t, sm, 121, 1000)
	wireStreamingPath(sm, syncPeer, secondPeer)

	// The sync peer already owes two unrelated blocks.
	for i := 0; i < 2; i++ {
		require.True(t, sm.blockDownloads.Add(syncPeer, chainhash.Hash{0xe1, byte(i)}))
	}

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count()+secondRec.count() == len(hashes) }, 5*time.Second))
	require.Equal(t, hashes[0:2], secondRec.all(), "the idle peer takes the lowest blocks until it owes as many as the busy one")
	require.Equal(t, hashes[2:3], syncRec.all(), "then the peers alternate, the sync peer winning a tie")
}

// With the park on every block streams straight to disk behind the admission budget, so every
// eligible peer carries blocks: with four headers and four peers, and no reason for any one
// peer to be preferred, each carries exactly one.
func TestSchedulerUsesEveryPeerWhenBlocksStreamToThePark(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf2}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm := schedulerManager(t)
	sm.ctx = context.Background()
	sm.blockPark, _ = newTestPark(t, "")

	syncPeer, syncRec := schedulerPeer(t, sm, 122, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	secondPeer, secondRec := schedulerPeer(t, sm, 123, 1000)
	thirdPeer, thirdRec := schedulerPeer(t, sm, 124, 1000)
	fourthPeer, fourthRec := schedulerPeer(t, sm, 125, 1000)
	wireStreamingPath(sm, syncPeer, secondPeer, thirdPeer, fourthPeer)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.fetchHeaderBlocks()

	recs := []*getDataRecorder{syncRec, secondRec, thirdRec, fourthRec}

	require.True(t, WaitUntil(func() bool {
		n := 0
		for _, r := range recs {
			n += r.count()
		}

		return n == len(hashes)
	}, 5*time.Second), "one block for each of the four peers")

	for i, r := range recs {
		require.Equal(t, hashes[i:i+1], r.all(), "peer %d carries one block, spread evenly across every eligible peer", i)
	}
}
