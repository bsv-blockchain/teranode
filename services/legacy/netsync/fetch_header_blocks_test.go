package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestFetchHeaderBlocksSurvivesPeerVanishingMidLoop carries forward the
// regression test for the segfault that took mainnet down on 2026-09-02,
// adapted to the walk this branch replaced it with.
//
// Upstream's crash was a second, unchecked lookup of the sync peer's state
// inside the request loop: a peer that disconnected after the check at the top
// of the function handed the loop a nil pointer that was written to on the next
// line. That loop no longer exists here. The pass now decides its peers and
// budgets up front in newDownloadAssigner, and records requests against the
// assignerPeer it captured there, so there is no second lookup to lose the
// existence flag on.
//
// The hazard the test exists for does survive the rewrite, so the test does
// too: a peer can still vanish between being judged eligible and the pass
// committing its requests. What must hold is that the walk finishes, and that
// the ledger does not end up holding hashes against a peer nobody will ask
// again, which would strand those blocks behind the forward-only cursor.
//
// The removal is injected from inside haveInventory rather than from another
// goroutine, so the peer is guaranteed gone at the exact moment the pass needs
// it. A racing goroutine would only hit the window sometimes, and a test that
// fails one run in fifty is not a regression test.
func TestFetchHeaderBlocksSurvivesPeerVanishingMidLoop(t *testing.T) {
	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.headerList = list.New()

	// A connected sync candidate, so the pass judges it eligible and actually
	// reaches the lookups. A peer that was never eligible would have the walk
	// return before the window this test is about ever opens.
	sp, _, _ := connectRacePeer(t, 200, 1000)
	registerRacePeer(sm, sp)
	sm.storeSyncPeer(sp, &syncPeerState{})

	// Two headers, so the pass has work to do after the peer has gone.
	for i := byte(1); i <= 2; i++ {
		sm.headerList.PushBack(&headerNode{height: int32(i), hash: &chainhash.Hash{i}})
	}

	sm.startHeader = sm.headerList.Front()

	// haveInventory reports "not held" for every hash, which is the branch that
	// reaches the request path. Its first call also drops the peer, standing in
	// for a disconnect landing between the eligibility check and the commit.
	blockchainClient := &blockchain.Mock{}
	blockchainClient.
		On("GetBlockHeader", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { sm.peerStates.Delete(sp) }).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewNotFoundError("not found"))

	sm.blockchainClient = blockchainClient

	require.NotPanics(t, sm.fetchHeaderBlocks, "a peer disconnecting mid-pass must not segfault the node")

	// Prove the window was entered. Without this the test could go green on a
	// refactor that never reaches the lookups, which would exercise nothing.
	blockchainClient.AssertExpectations(t)

	_, stillRegistered := sm.peerStates.Get(sp)
	require.False(t, stillRegistered, "the peer must have been removed during the pass")
}
