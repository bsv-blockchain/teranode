package netsync

import (
	"container/list"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
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

	// This is also the NON-headers-first half of the mode gate added for freemans13 item 3: the
	// pipeline refill must fire only in headers-first mode. refillHeaderBlockPipeline's only route to
	// the blockchain client is GetBestBlockHeader (via current() and its own fallback), so its absence
	// proves the refill did not run here.
	blockchainClient.AssertNotCalled(t, "GetBestBlockHeader", mock.Anything)
}

// TestHandleBlockMsg_CorruptCapRefillsHeaderPipeline covers freemans13 item 3
// (bitcoin-sv/teranode#4692): the cap gate runs AFTER the headerList entry and the requestedBlocks
// slot have already been consumed, so in headers-first mode a capped delivery drains one in-flight
// slot. Without a refill the remaining in-flight blocks each fail on their missing parent and
// recovery waits on the ~180s stall timer — exactly the reason the sibling corrupt branch refills.
// The refill is proven by GetBestBlockHeader being reached, which only refillHeaderBlockPipeline can
// do on this path; HandleBlockDirect must still never run.
func TestHandleBlockMsg_CorruptCapRefillsHeaderPipeline(t *testing.T) {
	prevHash := chainhash.Hash{0x02}
	msgBlock := wire.NewMsgBlock(wire.NewBlockHeader(1, &prevHash, &chainhash.Hash{}, 0, 0))
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	// The refill's fallback branch reads the best header. Returning an error keeps this test off the
	// network (the refill logs and returns) while still recording that the call was made.
	blockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(nil, nil, errors.NewServiceError("no best block header in this fixture"))

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
	sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	// Headers-first with an empty header list and no start header: the refill takes its fallback
	// branch, which is enough to observe that it ran at all.
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.blockSizeTracker = newBlockSizeTracker(10)

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(blockHash, p.Addr()))
	require.True(t, sm.corruptBlockAttemptsExhausted(blockHash, p.Addr()), "cap reached")

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: 101,
		peer:        p,
	})

	require.NoError(t, err, "a capped corrupt hash is still dropped quietly")

	blockchainClient.AssertCalled(t, "GetBestBlockHeader", mock.Anything)
	blockchainClient.AssertNotCalled(t, "GetBlockExists", mock.Anything, mock.Anything)

	// Pipeline maintenance ONLY: a dropped delivery must not run accepted-block bookkeeping.
	_, failed := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, failed, "the cap drop must not mark the block failed")
}
