package netsync

import (
	"container/list"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestFetchHeaderBlocksSurvivesPeerVanishingMidLoop reproduces the segfault that took mainnet
// down on 2026-09-02.
//
// fetchHeaderBlocks looks the sync peer's state up once at the top and returns early if it is
// gone. The request loop then looked it up a SECOND time, per header, and threw the existence
// flag away. A peer that disconnected after the first check therefore handed the loop a nil
// pointer, which was written to on the next line:
//
//	panic: runtime error: invalid memory address or nil pointer dereference
//	[signal SIGSEGV: segmentation violation code=0x1 addr=0x18]
//	netsync.(*SyncManager).fetchHeaderBlocks  manager.go:2017
//
// The removal is injected from inside haveInventory rather than from another goroutine, so the
// peer is guaranteed to be gone at the exact moment the loop needs it. A racing goroutine would
// only hit the window sometimes, and a test that fails one run in fifty is not a regression test.
func TestFetchHeaderBlocksSurvivesPeerVanishingMidLoop(t *testing.T) {
	// A real peer rather than a zero value, for two reasons. QueueMessage logs through the
	// peer's logger, which is nil on a bare struct. And it is never Connected here, so the
	// getdata at the end of the function returns without needing a socket.
	sp := peerpkg.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peerpkg.Config{})

	// Both expiring maps spawn a cleanup goroutine each, so both are stopped on teardown
	// rather than left running for the rest of the package suite.
	sharedRequested := expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	t.Cleanup(sharedRequested.Stop)

	peerRequested := expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	t.Cleanup(peerRequested.Stop)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		requestedBlocks:  sharedRequested,
		blockSizeTracker: newBlockSizeTracker(10),
		headerList:       list.New(),
	}

	sm.peerStates.Set(sp, &peerSyncState{requestedBlocks: peerRequested})

	sm.storeSyncPeer(sp, &syncPeerState{})

	// Two headers, so the loop body runs at least once after the peer has gone.
	for i := byte(1); i <= 2; i++ {
		sm.headerList.PushBack(&headerNode{height: int32(i), hash: &chainhash.Hash{i}})
	}

	sm.startHeader = sm.headerList.Front()

	// haveInventory reports "not held" for every hash, which is the branch that reaches the
	// peer-state write. Its first call also drops the peer, standing in for a disconnect
	// landing between the check at the top of the function and the loop below it.
	blockchainClient := &blockchain.Mock{}
	blockchainClient.
		On("GetBlockHeader", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { sm.peerStates.Delete(sp) }).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewNotFoundError("not found"))

	sm.blockchainClient = blockchainClient

	require.NotPanics(t, sm.fetchHeaderBlocks, "a peer disconnecting mid-loop must not segfault the node")

	// Prove the regression window was actually entered. Without these the test could go green
	// on a future refactor that never reaches haveInventory, or one that leaves the peer in
	// place, and neither would exercise the crash it exists to catch.
	blockchainClient.AssertExpectations(t)

	_, stillRegistered := sm.peerStates.Get(sp)
	require.False(t, stillRegistered, "the peer must have been removed during the loop")

	// The requests were still recorded, so the fix is not "skip the write when the peer is
	// gone". Losing them would strand those blocks: nothing would re-request a hash that
	// requestedBlocks claims is already in flight.
	require.Equal(t, 2, sm.requestedBlocks.Len(), "both headers should still have been requested")
	require.Equal(t, 2, peerRequested.Len(), "both headers should still be recorded against the peer")
}
