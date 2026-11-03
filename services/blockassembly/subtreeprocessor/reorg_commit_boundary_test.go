package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newReorgCommitBoundaryProcessor builds a DiskTxMap-backed, started
// SubtreeProcessor whose current header is a fresh genesis-rooted header,
// ready to move forward to a block with a real (non-coinbase-only) subtree,
// matching the catchup-only path of reorgBlocks (moveForwardBlocks only, no
// moveBack). A non-empty subtree is required: an empty block takes
// moveForwardBlock's early-return branch, which never reaches
// resetSubtreeState/the commit-point check at all.
//
// Uses its own local header/coinbase fixtures rather than the package-level
// prevBlockHeader/blockHeader/coinbaseTx2 shared by other tests in this
// package, to stay independent of run order.
func newReorgCommitBoundaryProcessor(t *testing.T) (*SubtreeProcessor, *model.Block, *model.BlockHeader) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	txHash := chainhash.HashH([]byte("reorg-commit-boundary-tx"))
	subtreeHash := storeReorgSubtree(t, ctx, blobStore, []subtreepkg.Node{
		{Hash: txHash, Fee: 100, SizeInBytes: 250},
	})

	startHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2000000001,
		Bits:           model.NBit{},
		Nonce:          9001,
	}
	nextHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  startHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2000000002,
		Bits:           model.NBit{},
		Nonce:          9002,
	}

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fc00002f6d312d65752fec97bce568b53123b2adfe06ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, test.CreateBaseTestSettings(t), blobStore, blockchainClient, utxoStore, newSubtreeChan,
		WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
	require.NoError(t, err)

	// stp.Stop closes diskTxMap, diskTxMapShadow and diskTxMapRetired (via
	// closeRetiredDiskTxMaps); no separate cleanup needed for those. It does
	// not close diskTxMapAnchor, but this test's reorg always completes
	// (success or rollback) before Stop runs, so no anchor stays pinned.
	stp.InitCurrentBlockHeader(startHeader)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	block := &model.Block{
		Height:     1,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{subtreeHash},
		Header:     nextHeader,
	}

	_, err = utxoStore.Create(ctx, coinbaseTx, 1)
	require.NoError(t, err)

	return stp, block, startHeader
}

// A disk tx map error recorded before reorgBlocks' catchup-path commit point
// (moveForwardBlock's own commit, followed by finalizeBlockProcessing) must
// fail the reorg and leave the header where it was, so a retry succeeds -
// mirroring TestMoveForwardBlock_DiskTxMapErrorBeforeCommitFailsAndRollsBack
// one level up, through the exported Reorg entry point and its dispatcher
// rollback.
func TestReorgBlocks_CatchupPath_DiskTxMapErrorBeforeCommitFailsAndRollsBack(t *testing.T) {
	stp, block, startHeader := newReorgCommitBoundaryProcessor(t)

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	err := stp.Reorg([]*model.Block{}, []*model.Block{block})
	require.ErrorIs(t, err, boom)

	require.Equal(t, startHeader.Hash(), stp.GetCurrentBlockHeader().Hash(), "the header must not advance on a rolled-back reorg")
	require.True(t, stp.TakeResetRequested(), "a pre-commit failure still requests a reset: the drained error may be an earlier write that never reached disk, whose phantom the rollback keeps")

	retryErr := stp.Reorg([]*model.Block{}, []*model.Block{block})
	require.NoError(t, retryErr, "a retry after the transient error clears must succeed")
	require.Equal(t, block.Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "the retry must advance the header")
}

// The moveBack path of reorgBlocks (a moveBack block, no moveForward block:
// reorgBlocks' general/non-catchup branch, not the catchup shortcut) writes
// moved-back transactions into currentTxMap via moveBackBlockBulkBuild. A
// writer flush failure for those writes must fail the whole reorg at the
// pre-commit check before the external, irreversible work (UTXO marks,
// finalize) - the same flush-before-check contract as
// TestMoveForwardBlock_PreCommitCheckFlushesBeforeFailing, one level up.
func TestReorgBlocks_MoveBackPath_WriterFlushFailureBeforeCommitFailsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	parentHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2100000001,
		Bits:           model.NBit{},
		Nonce:          9101,
	}
	tipHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parentHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      2100000002,
		Bits:           model.NBit{},
		Nonce:          9102,
	}

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fc00002f6d312d65752fec97bce568b53123b2adfe06ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	// markNotOnLongestChain unconditionally marks every tx still in assembly
	// after a moveBack-only reorg as not-on-longest-chain, so the moved-back
	// tx must actually exist in the UTXO store - a real tx spending the
	// coinbase, not an arbitrary hash.
	movedBackTx := bt.NewTx()
	require.NoError(t, movedBackTx.From(coinbaseTx.TxIDChainHash().String(), 0,
		coinbaseTx.Outputs[0].LockingScript.String(), uint64(coinbaseTx.Outputs[0].Satoshis)))
	require.NoError(t, movedBackTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	movedBackTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	subtreeHash := storeReorgSubtree(t, ctx, blobStore, []subtreepkg.Node{
		{Hash: *movedBackTx.TxIDChainHash(), Fee: 100, SizeInBytes: 250},
	})

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, parentHeader.Hash()).Return(parentHeader, &model.BlockHeaderMeta{}, nil)
	// markNotOnLongestChain's invalidation special-case checks the moveBack
	// block's own header.
	blockchainClient.On("GetBlockHeader", mock.Anything, tipHeader.Hash()).Return(tipHeader, &model.BlockHeaderMeta{Invalid: false}, nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, test.CreateBaseTestSettings(t), blobStore, blockchainClient, utxoStore, newSubtreeChan,
		WithTxMapDirs([]string{t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	stp.InitCurrentBlockHeader(tipHeader)
	stp.Start(ctx)

	moveBackBlock := &model.Block{
		Height:     2,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{subtreeHash},
		Header:     tipHeader,
	}

	_, err = utxoStore.Create(ctx, coinbaseTx, 2)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, movedBackTx, 2)
	require.NoError(t, err)

	// Installed on every log segment before Reorg runs anything, so
	// moveBackBlockBulkBuild's write fails whichever segment it lands in.
	failDiskTxMapLogs(stp.diskTxMap, alwaysFailWrites, nil)

	err = stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{})
	require.Error(t, err, "a write flush failure during moveBack must fail the reorg before its commit point")
	require.ErrorContains(t, err, "disk tx map storage error before committing reorg",
		"must be this check's own error - not e.g. a panic from an unmocked blockchain call recovered into an error, "+
			"which would also make require.Error pass without the fix actually having run")
	require.ErrorContains(t, err, "write failed", "and must actually be the write failure")

	require.Equal(t, tipHeader.Hash(), stp.GetCurrentBlockHeader().Hash(), "the header must not move on a rolled-back reorg")
	require.True(t, stp.TakeResetRequested(), "a pre-commit failure still requests a reset: the drained error may be an earlier write that never reached disk, whose phantom the rollback keeps")
}
