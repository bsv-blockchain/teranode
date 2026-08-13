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
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// setupBlockAssemblyTestWithUtxoStore is the same as setupBlockAssemblyTest but passes
// the UTXO store to NewBlockAssembler. This is required for tests that exercise
// reset() with moveForward blocks, because SubtreeProcessor.reset() calls
// processCoinbaseUtxos() which needs a non-nil utxoStore.
// NewBlockAssembler passes the utxoStore through to its internal SubtreeProcessor,
// so no separate SubtreeProcessor construction is needed.
// withSettings, if supplied, is applied to the test settings before any store
// or client is constructed. Mutating settings after this helper returns is not
// safe: the blockchain SQL store starts a background goroutine at construction
// that reads ChainCfgParams, so a later write races it. Anything a test needs
// to change about settings must therefore be changed here, up front.
func setupBlockAssemblyTestWithUtxoStore(t *testing.T, withSettings ...func(*settings.Settings)) *baTestItems {
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

	for _, apply := range withSettings {
		apply(tSettings)
	}

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

// TestReset_FallbackHeaderFailure_RestoresPreResetTip covers a bug where reset()
// optimistically writes the chain pointer (b.bestBlock) to the new tip before
// running subtreeProcessor.Reset. Normally the realign after Reset corrects this,
// but when Reset fails AND the fallback GetBlockHeader (looking up the subtree
// processor's own current header) also fails, reset() used to return immediately,
// leaving the chain pointer parked on the optimistic new tip while the coinbase
// state (tracked by the subtree processor) never moved. That divergence between
// the chain pointer and the coinbase state is exactly what this feature's recovery
// logic exists to fix — so reset() must not itself introduce it.
//
// This test would have failed before the fix: CurrentBlock() would have returned
// the optimistic new tip instead of the pre-reset tip.
func TestReset_FallbackHeaderFailure_RestoresPreResetTip(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// Blockchain has block1 -> block2. BA is parked behind, at block1 (height 1),
	// so reset() must move it forward to block2 (height 2) — this is the "optimistic
	// new tip" that reset() writes before subtreeProcessor.Reset runs. Without this
	// genuine gap between the pre-reset tip and the optimistic new tip, the bug this
	// test targets (the optimistic write never getting rolled back) can't be observed.
	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)

	// Inject an STP whose Reset fails and whose GetCurrentBlockHeader returns a
	// header the blockchain store does not know, so the fallback GetBlockHeader errors.
	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	unknown := &model.BlockHeader{Version: 1, HashPrevBlock: blockHeader2.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 99999, Bits: *bits}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{Err: errors.NewProcessingError("boom")})
	mockStp.On("GetCurrentBlockHeader").Return(unknown)
	injectMockStp(t, items, mockStp)

	preHeader, preHeight := items.blockAssembler.CurrentBlock()
	require.True(t, preHeader.Hash().IsEqual(blockHeader1.Hash()))
	require.Equal(t, uint32(1), preHeight)

	err := items.blockAssembler.reset(ctx)
	require.Error(t, err) // fallback header fetch fails, reset still returns error

	gotHeader, gotHeight := items.blockAssembler.CurrentBlock()
	require.Equal(t, preHeight, gotHeight, "pointer height must not advance past a failed reset")
	require.True(t, gotHeader.Hash().IsEqual(preHeader.Hash()), "pointer must be restored to pre-reset tip")
}

// setUpFailedRollbackReset builds the rollback shape the reset fallback has to
// cope with: the blockchain has block1 -> block2, block assembly sits on the
// abandoned block2Alt at the same height, so reset() produces one moveBack block
// (block2Alt) and one moveForward block (block2) over a common ancestor of
// block1. The subtree processor is replaced by a mock whose Reset always fails
// and whose current header is stpHeader, which is what the fallback then has to
// place.
func setUpFailedRollbackReset(ctx context.Context, t *testing.T, items *baTestItems, stpHeader *model.BlockHeader) {
	t.Helper()

	addBlockWithMinedSet(ctx, t, items, blockHeader1)
	addBlockWithMinedSet(ctx, t, items, blockHeader2)
	addBlockWithMinedSet(ctx, t, items, blockHeader2Alt)

	// Invalidating block2Alt makes block2 the chain tip while block assembly is
	// still pointed at block2Alt, which is what puts a block in moveBack.
	_, err := items.blockchainClient.InvalidateBlock(ctx, blockHeader2Alt.Hash())
	require.NoError(t, err)

	items.blockAssembler.setBestBlockHeader(blockHeader2Alt, 2)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{Err: errors.NewProcessingError("boom")})
	mockStp.On("GetCurrentBlockHeader").Return(stpHeader)
	injectMockStp(t, items, mockStp)
}

// blockHeaderFailingClient wraps the real blockchain client and fails
// GetBlockHeader for one specific hash, leaving every other call to answer
// normally. It exists so a test can prove the reset fallback resolves a height
// without a blockchain round-trip: the round-trip for that hash is guaranteed to
// fail, so a correct result can only have come from local state.
type blockHeaderFailingClient struct {
	blockchain.ClientI

	failFor *chainhash.Hash
}

func (c *blockHeaderFailingClient) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c.failFor != nil && hash.IsEqual(c.failFor) {
		return nil, nil, errors.NewStorageError("blockchain client cannot answer for %s", hash.String())
	}

	return c.ClientI.GetBlockHeader(ctx, hash)
}

// TestReset_FallbackHeaderFailure_RollbackDirectionStaysOffTheChainTip is the
// rollback-direction companion to TestReset_FallbackHeaderFailure_RestoresPreResetTip,
// which only exercises a pure fast-forward.
//
// SubtreeProcessor.reset deletes the moveBack blocks' coinbase UTXOs before
// almost every way it can subsequently fail, so once moveBack is non-empty the
// pre-reset tip is no longer guaranteed to match the coinbase state. Restoring it
// is still the right move, but for a different reason than the fast-forward case:
// it is the only one of the two tips guaranteed to differ from the chain tip, so
// the next reconcile fires instead of short-circuiting on "tips equal". That is
// the property this test pins.
func TestReset_FallbackHeaderFailure_RollbackDirectionStaysOffTheChainTip(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// A header no side of this reset ever saw and the blockchain store does not
	// know, so neither the local resolution nor the round-trip can place it.
	unknown := &model.BlockHeader{Version: 1, HashPrevBlock: blockHeader2.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 77777, Bits: *bits}
	setUpFailedRollbackReset(ctx, t, items, unknown)

	err := items.blockAssembler.reset(ctx)
	require.Error(t, err)

	gotHeader, gotHeight := items.blockAssembler.CurrentBlock()
	require.Equal(t, uint32(2), gotHeight)
	require.True(t, gotHeader.Hash().IsEqual(blockHeader2Alt.Hash()),
		"pointer must be restored to the pre-reset tip after a failed rollback reset")
	require.False(t, gotHeader.Hash().IsEqual(blockHeader2.Hash()),
		"the pointer must not be left level with the chain tip, or the next reconcile short-circuits and strands the divergence")
}

// TestReset_FailedRollbackResolvesHeightWithoutTheBlockchain pins the reason the
// branch above is now hard to reach at all. The reset fallback resolves the
// subtree processor's height from the block metadata the reset already fetched,
// so it does not depend on a blockchain lookup that can fail. Here that lookup is
// guaranteed to fail for the exact hash being resolved, and the fallback still
// lands on the right block at the right height.
//
// The reset itself must still report failure. Realigning onto the processor's
// own header is the right state to land on, but the processor's state was never
// rebuilt, so a caller told "no error" would believe a reset happened that did
// not -- and a failed reset is the drift this recovery exists to undo. The
// assertion this test is really about is the height and header below; the error
// is asserted so the contract cannot silently revert to success-on-failure.
func TestReset_FailedRollbackResolvesHeightWithoutTheBlockchain(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// The processor reports the new tip: SubtreeProcessor.reset stores each
	// moveForward block's header as it applies it, so a failure after that point
	// (for example in postProcess) leaves the processor genuinely holding block2.
	setUpFailedRollbackReset(ctx, t, items, blockHeader2)

	items.blockAssembler.blockchainClient = &blockHeaderFailingClient{
		ClientI: items.blockchainClient,
		failFor: blockHeader2.Hash(),
	}

	err := items.blockAssembler.reset(ctx)
	require.Error(t, err, "a reset whose subtree processor failed must not report success")
	require.Contains(t, err.Error(), "realigned",
		"the error must say the pointer was realigned, not imply nothing happened")

	gotHeader, gotHeight := items.blockAssembler.CurrentBlock()
	require.Equal(t, uint32(2), gotHeight, "the height must come from the metadata the reset already had")
	require.True(t, gotHeader.Hash().IsEqual(blockHeader2.Hash()),
		"block assembly must follow the subtree processor's own header even when the blockchain client cannot place it")
}
