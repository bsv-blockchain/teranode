package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newRewindManager builds a manager whose blockchain client answers every
// parent lookup with "block not found", so HandleBlockDirect fails the way it
// does for a block that arrives before its parent — the one path in
// handleBlockMsg that drops a fully downloaded block on the floor.
func newRewindManager(t *testing.T) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.Mock.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))
	blockchainClient.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	return sm
}

// TestSyncManager_ADroppedBlockIsAskedForAgain is the whole point of the
// rewind. The download walk is forward-only — commitHeaderCandidates advances
// startHeader past every header it considers and nothing in headers-first mode
// ever walks backwards — so a block that is downloaded and then dropped is
// never asked for again. The getblocks the drop path sends instead is inert in
// headers-first mode, because processInvMsg returns before it can request
// anything. Sync therefore stops, permanently, with no log line saying why.
//
// The assertion is the end state a stalled node does not reach: the peer is
// asked for the dropped block a second time.
func TestSyncManager_ADroppedBlockIsAskedForAgain(t *testing.T) {
	sm := newRewindManager(t)

	syncPeer, _, rec := connectRacePeer(t, 61, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa7}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The anchor block arrives and leaves the front of the list, so the first
	// unfetched header takes its place. That is what makes the drop below the
	// case a naive rewind misses: the dropped block IS the front, so its header
	// is removed and unindexed before it is ever validated.
	sm.blockDownloads.Add(syncPeer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

	// Ask for the whole batch, exactly as a node does when a headers message
	// lands. startHeader is now past every one of them.
	sm.fetchHeaderBlocks()
	require.True(t, WaitUntil(func() bool { return rec.count() == len(hashes) }, 5*time.Second),
		"the seeded headers should all have been requested")

	// The first of them arrives, and its parent is not stored. Today that means
	// the decoded block is thrown away.
	dropped := hashes[0]
	require.NoError(t, sm.handleBlockMsg(&blockQueueMsg{
		block:     &wire.MsgBlock{Header: *msg.Headers[0]},
		blockHash: dropped,
		peer:      syncPeer,
	}))

	// A later pass — every block arrival runs one, and so does every headers
	// message.
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool {
		for _, h := range rec.all()[len(hashes):] {
			if h.IsEqual(&dropped) {
				return true
			}
		}

		return false
	}, 5*time.Second), "the dropped block was never asked for again — sync is stalled with no way back")
}

// TestSyncManager_ARewindDoesNotReAskForBlocksAPeerStillOwes is the companion
// the rewind cannot ship without. Walking back to the dropped block puts the
// walk in front of blocks that are already in flight; re-requesting those makes
// the peer send each of them a second time, and the second copy arrives after
// the first one released that peer's obligation, so it looks unrequested and
// costs an honest peer its connection.
func TestSyncManager_ARewindDoesNotReAskForBlocksAPeerStillOwes(t *testing.T) {
	sm := newRewindManager(t)

	syncPeer, _, rec := connectRacePeer(t, 62, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xa8}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	sm.blockDownloads.Add(syncPeer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: syncPeer})

	sm.fetchHeaderBlocks()
	require.True(t, WaitUntil(func() bool { return rec.count() == len(hashes) }, 5*time.Second),
		"the seeded headers should all have been requested")

	before := len(rec.all())

	dropped := hashes[0]
	require.NoError(t, sm.handleBlockMsg(&blockQueueMsg{
		block:     &wire.MsgBlock{Header: *msg.Headers[0]},
		blockHash: dropped,
		peer:      syncPeer,
	}))

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return len(rec.all()) > before }, 5*time.Second),
		"the dropped block was never asked for again")

	require.Equal(t, []chainhash.Hash{dropped}, rec.all()[before:],
		"only the dropped block may be re-requested; the rest are still owed by this peer")
}
