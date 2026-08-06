package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
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
}
