package netsync

import (
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

	// The gate does no pipeline maintenance either (bitcoin-sv/teranode#4692). The
	// wanted-range pass's first act is committedTip, which is a GetBestBlockHeader, so
	// its absence proves no pass ran on this path.
	blockchainClient.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestHandleBlockMsg_CorruptCapDoesNotRefillHeaderPipeline pins the headers-first half of the
// corrupt-cap gate (bitcoin-sv/teranode#4692): the gate drops the delivery and refills NOTHING, so no
// getdata reaches the peer as a result of the drop, and the function still returns nil.
//
// What the gate must not do is spend the delivery on more downloading. Every block the wanted-range
// pass would ask for sits above the hash just dropped, and the pass stops at the first height it
// cannot name, so asking again from inside the drop only re-enters the same loop against the same
// capped peer.
//
// The residual is stated rather than glossed, and it is the same one upstream's own comment admits:
// a pass triggered by some OTHER block committing will still name the dropped hash, because the
// ledger keeps no memory of the cap. What the cap bounds is the expensive half — the gate drops
// before HandleBlockDirect and its decorate — not the body crossing the wire.
//
// The assertion is the outcome on the wire, not a blockchain-read count: a connected peer pair whose
// remote end records any block getdata that arrives. The fixture is deliberately arranged so a pass
// WOULD send one — a connected sync candidate with budget, and a header cache naming heights above
// the committed tip.
//
// Mutation proof: add a sm.fetchHeaderBlocks() call to this gate and a getdata reaches the wire,
// reddening the assertion. The positive control against over-applying the removal is
// TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock, which drives the SIBLING corrupt
// branch over an equivalent fixture and asserts a getdata DOES arrive, so this test cannot pass by
// breaking the pass everywhere.
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

	// Headers-first, with a pipeline a pass could genuinely top up: a header cache naming
	// two heights above the committed tip, and this peer stored as the sync peer.
	sm.headersFirstMode.Store(true)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.storeSyncPeer(p, &syncPeerState{})

	tipHash := mockCommittedTip(t, sm, 100, 0x33)
	pendingHeaders, _ := linkedRun(tipHash, 2)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(tipHash, 101, pendingHeaders))

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.True(t, sm.corruptBlockAttemptsExhausted(blockHash, p.Addr()), "cap reached")

	state, ok := sm.peerStates.Get(p)
	require.True(t, ok)
	// eligibleBlockPeers only considers connected sync candidates; without this the pass
	// could find no assigner and the negative assertion below would pass for the wrong
	// reason.
	state.syncCandidate = true
	state.noteBestKnownHeight(200)

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

	// The opposite of the sibling corrupt branch, deliberately: the cap-drop path must NOT put the
	// hash back in the download ledger. An entry there is a request the node believes it made, and
	// a getdata to the very peer that is capped has its delivery dropped again after the full body
	// has crossed the wire — one full block download per iteration for the whole cooldown window.
	// Recovery here is the cooldown lapsing or sync-peer rotation on the unrefreshed stall timer
	// (bitcoin-sv/teranode#4692).
	require.False(t, sm.blockDownloads.RequestedWithin(blockHash, blockRequestRetryInterval),
		"the cap drop must not record a request — no getdata loop against a capped peer")
	require.False(t, sm.blockDownloads.HasOwner(p, blockHash),
		"the capped peer must owe nothing for this hash after the drop")
	_ = state
}
