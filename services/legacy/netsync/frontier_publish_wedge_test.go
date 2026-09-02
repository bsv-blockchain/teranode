package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// seedStuckWalk builds the shape a wedged node is in: the header list still has
// the block everything is queued behind at its front, and the download cursor
// has already moved past it. front is the block holding up sync, so a walk that
// runs in this state must publish it as the frontier, whether or not it manages
// to ask anybody for anything.
func seedStuckWalk(t *testing.T, sm *SyncManager) chainhash.Hash {
	t.Helper()

	frontHash := chainhash.Hash{0xf1}
	nextHash := chainhash.Hash{0xf2}

	front := sm.headerList.PushBack(&headerNode{height: 75002, hash: &frontHash})
	next := sm.headerList.PushBack(&headerNode{height: 75003, hash: &nextHash})

	sm.headerIndex = map[chainhash.Hash]*list.Element{}
	sm.startHeader = next

	require.NotSame(t, front, sm.startHeader, "the cursor must be past the front for a frontier to exist")

	return frontHash
}

// TestFetchHeaderBlocks_PublishesTheFrontierWhenItCannotAskAnybody is the
// regression test for the wedge a mainnet soak hit at height 75,001.
//
// fetchHeaderBlocks used to publish the frontier only at the end of a successful
// pass. newDownloadAssigner answers nil when no peer is eligible, when the
// node-wide download window is spent, or when every peer is at its per-peer cap,
// and all three mean blocks are outstanding and undelivered. That is exactly the
// state raceFrontierBlock exists to rescue, and returning early left it with no
// target: the one block holding up sync was never asked of a second peer, so
// nothing committed, so the assigner stayed nil, so the walk never published.
// The node sat there for over four hours without recovering.
func TestFetchHeaderBlocks_PublishesTheFrontierWhenItCannotAskAnybody(t *testing.T) {
	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	frontHash := seedStuckWalk(t, sm)

	// No peer is registered, so eligibleBlockPeers finds nobody and the assigner
	// is nil. This is the early return that used to skip the publish.
	require.Nil(t, sm.newDownloadAssigner(), "precondition: the walk must be unable to ask anybody")

	sm.fetchHeaderBlocks()

	require.Equal(t, frontHash, sm.frontierHash,
		"a walk that cannot ask for anything must still publish what is stuck, or the frontier race has no target")
}

// TestFetchHeaderBlocks_PublishesTheFrontierWithNoStartHeader covers the other
// early return, which takes a different route out of the function.
func TestFetchHeaderBlocks_PublishesTheFrontierWithNoStartHeader(t *testing.T) {
	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	frontHash := chainhash.Hash{0xf1}
	nextHash := chainhash.Hash{0xf2}
	sm.headerList.PushBack(&headerNode{height: 75002, hash: &frontHash})
	sm.headerList.PushBack(&headerNode{height: 75003, hash: &nextHash})
	sm.startHeader = nil

	sm.fetchHeaderBlocks()

	require.Equal(t, frontHash, sm.frontierHash,
		"the no-start-header return must publish too; the walk having nothing to do is not the same as nothing being stuck")
}
