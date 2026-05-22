package blockassembly

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupBlockAssemblyTestWithUtxoStore is the same as setupBlockAssemblyTest but passes
// the UTXO store to NewBlockAssembler. This is required for tests that exercise
// reset() with moveForward blocks, because SubtreeProcessor.reset() calls
// processCoinbaseUtxos() which needs a non-nil utxoStore.
// NewBlockAssembler passes the utxoStore through to its internal SubtreeProcessor,
// so no separate SubtreeProcessor construction is needed.
func setupBlockAssemblyTestWithUtxoStore(t *testing.T) *baTestItems {
	t.Helper()

	items := baTestItems{}
	items.blobStore = nil
	items.txStore = nil
	items.newSubtreeChan = make(chan subtreeprocessor.NewSubtreeRequest, 100)

	ctx := t.Context()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	tSettings := createTestSettings(t)

	utxo, err := utxostoresql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	items.utxoStore = utxo

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	items.blockchainClient, err = blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	stats := gocore.NewStat("test")

	ba, err := NewBlockAssembler(
		t.Context(),
		ulogger.TestLogger{},
		tSettings,
		stats,
		items.utxoStore,
		nil, // blobStore
		items.blockchainClient,
		items.newSubtreeChan,
	)
	require.NoError(t, err)
	require.NotNil(t, ba.subtreeProcessor)

	t.Cleanup(func() {
		if ba.subtreeProcessor != nil {
			ba.subtreeProcessor.Stop(context.Background())
		}
	})

	ba.subtreeProcessor.Start(t.Context())
	items.blockAssembler = ba

	return &items
}

// addBlockWithMinedSet adds a block to the blockchain store with mined_set=true.
// This is required because reset() calls WaitForPendingBlocks which polls
// GetBlocksMinedNotSet — without mined_set=true the wait would hang indefinitely.
func addBlockWithMinedSet(ctx context.Context, t *testing.T, items *baTestItems, blockHeader *model.BlockHeader) {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	err = items.blockchainClient.AddBlock(ctx, &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}, "", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)
}

// TestResetWithBlockchainAhead_MissesIntermediateBlockProcessing covers a bug where,
// during reset with blockchain ahead by N blocks, intermediate moveForward blocks were
// not properly finalized.
//
// Before the fix, SubtreeProcessor.reset() only called finalizeBlockProcessing (and
// thus SetBlockProcessedAt) for the LAST moveForward block. Intermediate blocks never
// got processed_at set, meaning they were not recognized as fully processed.
//
// This test would have failed before the fix. The fix ensures reset finalizes each
// moveForward block so every intermediate block is marked processed.
func TestResetWithBlockchainAhead_MissesIntermediateBlockProcessing(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// Build chain: genesis → block1 → block2 → block3 → block4
	// All blocks have mined_set=true so WaitForPendingBlocks won't hang.
	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	addBlockWithMinedSet(ctx, t, items, blockHeader3)
	addBlockWithMinedSet(ctx, t, items, blockHeader4)

	// Set BA at block1 (height 1). Blockchain best is block4 (height 4).
	// This means blockchain is 3 blocks ahead of block assembly.
	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)

	// Trigger reset — this will target blockchain's best block (block4)
	// and fast-forward through blocks 2, 3, 4 in simplified mode.
	err := items.blockAssembler.reset(ctx, false)
	require.NoError(t, err, "reset should succeed")

	// Verify BA jumped to block4
	currentHeader, height := items.blockAssembler.CurrentBlock()
	require.Equal(t, uint32(4), height, "BA should be at height 4 after reset")
	require.True(t, currentHeader.Hash().IsEqual(blockHeader4.Hash()), "BA should be at block4")

	// Now check processed_at for each intermediate block.
	// BUG: SubtreeProcessor.reset() only calls finalizeBlockProcessing for the LAST
	// moveForward block (block4). Blocks 2 and 3 never get SetBlockProcessedAt called.

	_, meta2, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader2.Hash())
	require.NoError(t, err)
	assert.NotNil(t, meta2.ProcessedAt,
		"BUG: block2 (intermediate) should have processed_at set, but reset only finalizes the last block")

	_, meta3, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader3.Hash())
	require.NoError(t, err)
	assert.NotNil(t, meta3.ProcessedAt,
		"BUG: block3 (intermediate) should have processed_at set, but reset only finalizes the last block")

	// Block4 (last moveForward block) DOES get processed_at set via finalizeBlockProcessing
	_, meta4, err := items.blockchainClient.GetBlockHeader(ctx, blockHeader4.Hash())
	require.NoError(t, err)
	assert.NotNil(t, meta4.ProcessedAt,
		"block4 (last moveForward) should have processed_at set")
}

// TestHandleReorg_FallbackReset_ReturnsNilInsteadOfResetError covers a bug where
// handleReorg fell back to reset() (due to an invalid block or failed Reorg) but
// returned nil instead of ErrBlockAssemblyReset.
//
// Before the fix, processNewBlockAnnouncement (the caller) checked for
// ErrBlockAssemblyReset to decide whether to skip the subsequent setBestBlockHeader
// call. When handleReorg returned nil, the caller would overwrite BA's best block
// with a potentially stale value captured before the reset ran.
//
// The large-reorg path correctly returned ErrBlockAssemblyReset. This fix aligns
// the fallback-reset path with the same behavior.
//
// This test would have failed before the fix.
func TestHandleReorg_FallbackReset_ReturnsNilInsteadOfResetError(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// Build two forks from block1:
	//   Main chain:  genesis → block1 → block2
	//   Fork chain:  genesis → block1 → block2Alt (will be invalidated)
	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	addBlockWithMinedSet(ctx, t, items, blockHeader2Alt)

	// Invalidate block2Alt — blockchain best becomes block2
	_, err := items.blockchainClient.InvalidateBlock(ctx, blockHeader2Alt.Hash())
	require.NoError(t, err)

	// Verify blockchain best is now block2 (not block2Alt)
	bestHeader, bestMeta, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.True(t, bestHeader.Hash().IsEqual(blockHeader2.Hash()),
		"blockchain best should be block2 after invalidating block2Alt")
	require.Equal(t, uint32(2), bestMeta.Height)

	// Set BA on the invalid fork at block2Alt
	items.blockAssembler.setBestBlockHeader(blockHeader2Alt, 2)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader2Alt)

	// Call handleReorg to reorg from block2Alt → block2
	// handleReorg will detect hasInvalidBlock=true (block2Alt is invalid in moveBack),
	// which sets reset=true. After Reorg (or skip), it calls b.reset().
	// BUG: handleReorg returns nil after the fallback reset instead of ErrBlockAssemblyReset.
	err = items.blockAssembler.handleReorg(ctx, blockHeader2, 2)

	// The large-reorg path (line 1158-1163) correctly returns ErrBlockAssemblyReset.
	// The fallback-reset path (line 1191-1202) should do the same, but returns nil.
	require.Error(t, err, "BUG: handleReorg should return an error after fallback reset to prevent caller from overwriting best block")
	require.True(t, errors.Is(err, errors.ErrBlockAssemblyReset),
		"handleReorg should return ErrBlockAssemblyReset after fallback reset, got: %v", err)
}
