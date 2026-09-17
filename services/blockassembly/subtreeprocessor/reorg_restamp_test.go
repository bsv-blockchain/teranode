package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createReorgTestTx creates a real transaction in the UTXO store, recorded as mined into
// block minedInBlockID on the longest chain — the pre-reorg state of a transaction that
// was already in a block. satoshis only exists to make each transaction distinct.
func createReorgTestTx(t *testing.T, ctx context.Context, store utxostore.Store, satoshis uint64, minedInBlockID uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))

	_, _, err := store.SpendAndCreate(ctx, tx, 2,
		utxostore.WithCreateOnly(),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
			BlockID:        minedInBlockID,
			BlockHeight:    2,
			OnLongestChain: true,
		}),
	)
	require.NoError(t, err)

	return tx
}

// TestReorgBlocksReStampsEveryTransactionOfAMovedForwardBlock pins that a reorg records the
// moved-forward block as the block that mined every one of its transactions.
//
// Before this, reorgBlocks only called the id-less MarkTransactionsOnLongestChain, and only
// for the transactions of a moved-forward block that were NOT also in a moved-back block. So
// a transaction present in both forks — the common case in a reorg, where most of the losing
// block's transactions are re-mined by the winning one — was never told which block now
// mines it, and neither was the moved-forward block's coinbase, whose create is skipped
// because the row already exists. A store that keeps settled transactions in a membership
// table keyed by (transaction, block) cannot settle them without that id.
func TestReorgBlocksReStampsEveryTransactionOfAMovedForwardBlock(t *testing.T) {
	const (
		moveBackBlockID    = uint32(11)
		moveForwardBlockID = uint32(22)
	)

	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()
	newSubtreeChan := make(chan NewSubtreeRequest, 10)

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.AnythingOfType("*chainhash.Hash"), mock.AnythingOfType("[]bool")).Return(nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	mockBlockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blobStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)

	// doubleSpendTx is mined by BOTH forks, forwardOnlyTx only by the winning one, and
	// backOnlyTx only by the losing one. All three start out recorded as mined into the
	// block that is about to be moved back.
	doubleSpendTx := createReorgTestTx(t, ctx, utxoStore, 1001, moveBackBlockID)
	backOnlyTx := createReorgTestTx(t, ctx, utxoStore, 1002, moveBackBlockID)
	forwardOnlyTx := createReorgTestTx(t, ctx, utxoStore, 1003, moveBackBlockID)

	backSubtreeHash := storeReorgSubtree(t, ctx, blobStore, []subtree.Node{
		{Hash: *doubleSpendTx.TxIDChainHash(), Fee: 100, SizeInBytes: 250},
		{Hash: *backOnlyTx.TxIDChainHash(), Fee: 150, SizeInBytes: 300},
	})
	forwardSubtreeHash := storeReorgSubtree(t, ctx, blobStore, []subtree.Node{
		{Hash: *doubleSpendTx.TxIDChainHash(), Fee: 100, SizeInBytes: 250},
		{Hash: *forwardOnlyTx.TxIDChainHash(), Fee: 200, SizeInBytes: 400},
	})

	// Both the moved-back tip and the winning block build on prevBlockHeader, which the
	// blockchain mock returns as the moved-back block's parent.
	block2Header := &model.BlockHeader{Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1900000002, Bits: model.NBit{}, Nonce: 902}
	blockNewHeader := &model.BlockHeader{Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1900000003, Bits: model.NBit{}, Nonce: 903}

	blockToMoveBack := &model.Block{
		ID:         moveBackBlockID,
		Height:     2,
		CoinbaseTx: coinbaseTx2,
		Subtrees:   []*chainhash.Hash{backSubtreeHash},
		Header:     block2Header,
	}
	blockToMoveForward := &model.Block{
		ID:         moveForwardBlockID,
		Height:     2,
		CoinbaseTx: coinbaseTx3,
		Subtrees:   []*chainhash.Hash{forwardSubtreeHash},
		Header:     blockNewHeader,
	}

	// Both coinbases already exist in the store with no block recorded — the state that
	// makes processCoinbaseUtxos skip its create with "already exist".
	_, err = utxoStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(block2Header)

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	require.NoError(t, stp.Reorg([]*model.Block{blockToMoveBack}, []*model.Block{blockToMoveForward}))

	blockIDsOf := func(hash *chainhash.Hash) []uint32 {
		txMeta, getErr := utxoStore.Get(ctx, hash, fields.BlockIDs)
		require.NoError(t, getErr)

		return txMeta.BlockIDs
	}

	require.Contains(t, blockIDsOf(doubleSpendTx.TxIDChainHash()), moveForwardBlockID,
		"a transaction mined by both forks must be recorded as mined into the winning block")
	require.Contains(t, blockIDsOf(forwardOnlyTx.TxIDChainHash()), moveForwardBlockID,
		"a transaction only the winning block mines must be recorded as mined into it")
	require.Contains(t, blockIDsOf(coinbaseTx3.TxIDChainHash()), moveForwardBlockID,
		"the winning block's coinbase must be recorded as mined into it even when its row already existed")
}
