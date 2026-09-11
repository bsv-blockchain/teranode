package netsync

import (
	"container/list"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestResumeHeaderWalk_AsksAnIdlePeerWhileTheSyncPeerIsFull is the wedge the
// sweep's walk resume was gated into.
//
// The resume exists for the state where a block has been given up on and the
// cursor rewound onto it: nothing else will carry that cursor forward, because
// the block everything was queued behind is the one that was dropped. It used to
// go through fetchMoreHeaderBlocks with the sync peer, whose gate is that peer's
// own in-flight count against the block-size ladder. At the ladder's bottom rung
// the cap is one block, so a sync peer part-way through a multi-gigabyte
// transfer held the resume shut for the whole of that transfer, hours in the
// regime this PR targets, while an idle peer sat there able to serve it.
//
// The frontier race cannot rescue it: publishFrontierLocked clears the frontier
// when the front block has not been requested, and a rewound front is exactly
// that.
//
// So the end state pinned here is that the block gets asked for, not which
// mechanism asked. svnode schedules per peer and consults no sync peer for block
// bodies at all (FindNextBlocksToDownload, src/net/net_processing.cpp:5522);
// letting the assigner answer is that shape.
func TestResumeHeaderWalk_AsksAnIdlePeerWhileTheSyncPeerIsFull(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xc7}
	_, hashes := linkedHeaders(anchor, 3, &nonce)

	sm := newFetchLockManager(t, nil, nil, nil)

	// The block-size ladder at its bottom rung: one block in flight per peer,
	// which is the regime a rewind happens in and the one the old gate could not
	// tell from "this peer is finished".
	const threeGB = int64(3) * 1024 * 1024 * 1024
	for i := 0; i < 3; i++ {
		sm.blockSizeTracker.addBlockSize(threeGB)
	}

	require.Equal(t, 1, sm.blockSizeTracker.calculateMaxInFlightBlocks(), "sanity: the ladder is at its bottom rung")

	syncPeer, syncRec := schedulerPeer(t, sm, 150, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	_, idleRec := schedulerPeer(t, sm, 151, 1000)

	// The header list a rewind leaves behind: the block that was given up on is
	// back at the front and the cursor is sitting on it, so the walk has
	// somewhere to go and nobody owns the front block.
	sm.headerMu.Lock()
	sm.headerList = list.New()
	sm.headerIndex = nil

	for i := range hashes {
		hash := hashes[i]
		node := &headerNode{height: int32(i + 11), hash: &hash}
		e := sm.headerList.PushBack(node)
		sm.indexHeaderLocked(e, hash)

		if i == 0 {
			sm.startHeader = e
		}
	}
	sm.headerMu.Unlock()

	// The sync peer is mid-transfer on an unrelated block, which is all it takes
	// to spend its whole budget at this rung.
	require.True(t, sm.blockDownloads.Add(syncPeer, chainhash.Hash{0xe0}))
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(syncPeer), "sanity: the sync peer is at its cap")

	sm.resumeHeaderWalk()

	require.True(t, WaitUntil(func() bool { return idleRec.count() == 1 }, 5*time.Second),
		"the rewound front block must be asked of a peer that has room, not left until the sync peer's own transfer finishes")

	require.Equal(t, []chainhash.Hash{hashes[0]}, idleRec.all(),
		"and it must be the front block, the one everything else is queued behind")

	require.Zero(t, syncRec.count(), "the peer already at its cap must not be asked for another block")
}
