package netsync

import (
	"container/list"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockMsg_CorruptCapDropsBeforeHandleBlockDirect proves the legacy per-hash corrupt cap
// gate (bitcoin-sv/teranode#4692): once a block hash has reached MaxCorruptAttemptsPerBlock corrupt
// deliveries within the cooldown window, the next delivery is DROPPED before the expensive
// HandleBlockDirect/decorate — it returns nil, does not reject the block to the peer, and does NOT
// set recentlyFailedBlocks (preserving the no-NOT_FOUND-cascade property). The drop is proven by the
// mock: GetBlockExists (the first RPC inside HandleBlockDirect) is asserted NOT called, so the
// expensive work was skipped rather than repeated.
func TestHandleBlockMsg_CorruptCapDropsBeforeHandleBlockDirect(t *testing.T) {
	prevHash := chainhash.Hash{0x01}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	// handleBlockMsg reads the FSM state before the corrupt gate; HandleBlockDirect's GetBlockExists
	// must NEVER be reached — no expectation is registered for it, so a call would fail the test.
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	// Drive the hash to the cap for THIS serving peer, exactly as repeated corrupt deliveries would.
	// The gate keys on (hash, peer address), so record against the same peer handleBlockMsg will read.
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.True(t, sm.corruptBlockAttemptsExhausted(blockHash, p.Addr()), "cap reached")

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err, "a capped corrupt hash is dropped quietly before HandleBlockDirect")

	// The drop skipped the expensive path: HandleBlockDirect (and its GetBlockExists RPC) never ran.
	blockchainClient.AssertNotCalled(t, "GetBlockExists", mock.Anything, mock.Anything)

	// And it did not poison the descendant cascade: recentlyFailedBlocks stays unset for this hash.
	_, failed := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, failed, "the cap drop must not mark the block failed (preserves the no-NOT_FOUND-cascade property)")

	// The gate does no pipeline maintenance either (bitcoin-sv/teranode#4692).
	// refillHeaderBlockPipeline's only route to the blockchain client is GetBestBlockHeader (via
	// current() and its own fallback), so its absence proves the refill did not run here.
	blockchainClient.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestHandleBlockMsg_CorruptCapDoesNotRefillHeaderPipeline pins the headers-first half of the
// corrupt-cap gate (bitcoin-sv/teranode#4692): the gate drops the delivery and refills NOTHING, so no
// getdata reaches the peer as a result of the drop, and the function still returns nil.
//
// The refill this replaces was counter-productive, not merely useless. In headers-first mode the
// header list is a linear chain, so every block a refill requests descends from the hash just
// dropped: each body crosses the wire in full, refreshes the sync peer's stall timer at receipt
// inside HandleBlockDirect, and then fails its parent lookup — and because the gate deliberately
// does not mark the hash failed, the descendant short-circuit does not stop them either. So the
// refill downloaded and discarded the remaining header window while postponing the sync-peer
// rotation that is this path's only recovery.
//
// The assertion is the outcome on the wire, not a blockchain-read count: a connected peer pair whose
// remote end records any block getdata that arrives. The fixture is deliberately arranged so a
// refill WOULD send one — a sync peer is stored, headerList holds pending nodes, startHeader points
// at the first of them, and the gate's own deletions leave requestedBlocks below the dynamic
// in-flight limit — which is exactly the fetchHeaderBlocks branch refillHeaderBlockPipeline takes.
//
// Mutation proof: restoring the headersFirstMode refill block on this gate puts a getdata on the
// wire and reddens the assertion. The positive control against over-applying the removal is
// TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock, which drives the SIBLING corrupt
// branch over the same pipeline fixture and asserts a getdata for a pending header hash — so
// deleting the refill from that branch too reddens it, and this test cannot pass by breaking refill
// everywhere.
func TestHandleBlockMsg_CorruptCapDoesNotRefillHeaderPipeline(t *testing.T) {
	prevHash := chainhash.Hash{0x02}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// What a refill would need on its way to the wire: haveInventory reports "not held" for every
	// pending header, which is the branch that requests it.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewNotFoundError("not found")).Maybe()
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(nil, nil, errors.NewServiceError("no best block header in this fixture")).Maybe()

	var gotGetData atomic.Bool
	remoteCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, msg *wire.MsgGetData) {
				for _, iv := range msg.InvList {
					if iv.Type == wire.InvTypeBlock {
						gotGetData.Store(true)
					}
				}
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}
	localCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	remote, p, err := MakeConnectedPeers(t, remoteCfg, localCfg, 120)
	require.NoError(t, err)
	require.True(t, remote.Connected())

	sm := newBackoffTestManagerForPeer(t, blockchainClient, blockHash, p)
	sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	// Headers-first, with a pipeline a refill could genuinely top up: pending header nodes, a
	// startHeader pointing at them, and this peer stored as the sync peer.
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	for i := byte(1); i <= 2; i++ {
		sm.headerList.PushBack(&headerNode{height: int32(100 + i), hash: &chainhash.Hash{i}})
	}
	sm.startHeader = sm.headerList.Front()
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.storeSyncPeer(p, &syncPeerState{})

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.True(t, sm.corruptBlockAttemptsExhausted(blockHash, p.Addr()), "cap reached")

	state, ok := sm.peerStates.Get(p)
	require.True(t, ok)

	err = sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err, "a capped corrupt hash is still dropped quietly")

	require.False(t, WaitUntil(func() bool { return gotGetData.Load() }, 750*time.Millisecond),
		"the cap drop must not put any block getdata on the wire — every block a refill would request descends from the dropped hash")

	blockchainClient.AssertNotCalled(t, "GetBlockExists", mock.Anything, mock.Anything)

	// Pipeline maintenance ONLY: a dropped delivery must not run accepted-block bookkeeping.
	_, failed := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, failed, "the cap drop must not mark the block failed")

	// The opposite of the sibling corrupt branch, deliberately: the cap-drop path must NOT re-arm
	// the request maps. Re-arming means a getdata to the very peer that is capped, whose delivery
	// this gate drops again after the full body has crossed the wire — one full block download per
	// iteration for the whole cooldown window. Recovery here is sync-peer rotation on the
	// unrefreshed stall timer, not a re-request (bitcoin-sv/teranode#4692).
	_, inGlobal := sm.requestedBlocks.Get(blockHash)
	require.False(t, inGlobal, "the cap drop must not re-arm sm.requestedBlocks — no getdata loop against a capped peer")
	_, inPeer := state.requestedBlocks.Get(blockHash)
	require.False(t, inPeer, "the cap drop must not re-arm state.requestedBlocks — no getdata loop against a capped peer")
}
