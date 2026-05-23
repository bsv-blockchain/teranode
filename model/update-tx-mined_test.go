package model

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	tx0 = newTx(0)
	tx1 = newTx(1)
	tx2 = newTx(2)
	tx3 = newTx(3)
	tx4 = newTx(4)
	tx5 = newTx(5)
	tx6 = newTx(6)
	tx7 = newTx(7)
)

func TestUpdateTxMinedStatus(t *testing.T) {
	t.Run("set mined status for block transactions", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		tSettings.UtxoStore = settings.UtxoStoreSettings{
			UpdateTxMinedStatus: true,
			MaxMinedBatchSize:   1024,
			MaxMinedRoutines:    1,                // SQLite only supports one writer at a time
			DBTimeout:           30 * time.Second, // Increase timeout for SQLite in-memory operations
		}
		setWorkerSettings(tSettings)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_, err = utxoStore.Create(context.Background(), tx0, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx1, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx2, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx3, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx4, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx5, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx6, 1)
		require.NoError(t, err)
		_, err = utxoStore.Create(context.Background(), tx7, 1)
		require.NoError(t, err)

		block := &Block{}
		block.CoinbaseTx = tx0
		block.Subtrees = []*chainhash.Hash{
			tx1.TxIDChainHash(),
			tx2.TxIDChainHash(),
		}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{
						Hash: *subtree.CoinbasePlaceholderHash,
					},
					{
						Hash: *tx1.TxIDChainHash(),
					},
					{
						Hash: *tx2.TxIDChainHash(),
					},
					{
						Hash: *tx3.TxIDChainHash(),
					},
				},
			},
			{
				Nodes: []subtree.Node{
					{
						Hash: *tx4.TxIDChainHash(),
					},
					{
						Hash: *tx5.TxIDChainHash(),
					},
					{
						Hash: *tx6.TxIDChainHash(),
					},
					{
						Hash: *tx7.TxIDChainHash(),
					},
				},
			},
		}

		err = UpdateTxMinedStatus(
			ctx,
			logger,
			tSettings,
			utxoStore,
			block,
			1,
			[]uint32{0},
			true,
			nil,
			false,
		)
		require.NoError(t, err)

		txMeta, err := utxoStore.Get(ctx, tx0.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs) // tx0 is a coinbase tx, so it should not have any block IDs set by the SetMinedMulti process - its done in the block assembly process at the point of creating the coinbasetx

		txMeta, err = utxoStore.Get(ctx, tx1.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx2.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx3.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx4.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx5.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx6.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		txMeta, err = utxoStore.Get(ctx, tx7.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

		// Now unset the mined status
		err = UpdateTxMinedStatus(
			ctx,
			logger,
			tSettings,
			utxoStore,
			block,
			1,
			[]uint32{0},
			false,
			nil,
			true,
		)
		require.NoError(t, err)

		txMeta, err = utxoStore.Get(ctx, tx1.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx2.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx3.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx4.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx5.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx6.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)

		txMeta, err = utxoStore.Get(ctx, tx7.TxIDChainHash())
		require.NoError(t, err)
		assert.Empty(t, txMeta.BlockIDs)
	})
}

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime

	tx.Inputs = []*bt.Input{{
		UnlockingScript:    &bscript.Script{},
		PreviousTxOutIndex: 0,
	}}

	_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})

	return tx
}

// realMinedReturn returns a func that synthesizes a realistic blockIDsMap for use
// with testify mock.Call.Return. Each submitted hash maps to [blockID] so the
// SetMinedMulti coverage postcondition in UpdateTxMinedStatus is satisfied.
func realMinedReturn(blockID uint32) func(context.Context, []*chainhash.Hash, utxo.MinedBlockInfo) map[chainhash.Hash][]uint32 {
	return func(_ context.Context, hashes []*chainhash.Hash, _ utxo.MinedBlockInfo) map[chainhash.Hash][]uint32 {
		m := make(map[chainhash.Hash][]uint32, len(hashes))
		for _, h := range hashes {
			m[*h] = []uint32{blockID}
		}
		return m
	}
}

// TestUpdateTxMinedStatus_BlockIDCollisionDetection tests the critical new feature
// where transactions are checked against current chain block IDs
func TestUpdateTxMinedStatus_BlockIDCollisionDetection(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	mockStore := &utxo.MockUtxostore{}

	// Create test transactions
	testTx1 := newTx(100)
	testTx2 := newTx(200)

	// Create a block with these transactions
	block := &Block{}
	block.CoinbaseTx = newTx(0)
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{testTx1.TxIDChainHash()}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *testTx1.TxIDChainHash()},
				{Hash: *testTx2.TxIDChainHash()},
			},
		},
	}

	t.Run("should return BlockInvalidError when transaction already on chain", func(t *testing.T) {
		// Mock SetMinedMulti to return block IDs indicating tx is already mined on current chain.
		// The current blockID (15) is also present per the SetMinedMulti postcondition.
		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx1.TxIDChainHash(): {5, 15},  // Historically in block 5, now also 15.
			*testTx2.TxIDChainHash(): {10, 15}, // Historically in block 10, now also 15.
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		// Chain contains block IDs 5 and 10
		chainBlockIDs := []uint32{5, 10}

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDs, true, nil)

		// Should get BlockInvalidError because transaction was already mined in block 5 (on current chain)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block contains a transaction already on our chain")

		mockStore.AssertExpectations(t)
	})

	t.Run("should succeed when transaction mined in different chain", func(t *testing.T) {
		mockStore = &utxo.MockUtxostore{} // Reset mock

		// Mock SetMinedMulti to return block IDs from a different chain plus the current
		// blockID (15) per the postcondition.
		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx1.TxIDChainHash(): {99, 15},
			*testTx2.TxIDChainHash(): {100, 15},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		// Chain contains different block IDs
		chainBlockIDs := []uint32{5, 10, 15}

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDs, true, nil)

		// Should succeed because transactions are not on current chain
		require.NoError(t, err)

		mockStore.AssertExpectations(t)
	})

	t.Run("should succeed when same block ID as current being mined", func(t *testing.T) {
		mockStore = &utxo.MockUtxostore{} // Reset mock

		// Mock SetMinedMulti to return the same block ID we're currently mining,
		// for every submitted tx (postcondition requires coverage of all hashes).
		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx1.TxIDChainHash(): {15},
			*testTx2.TxIDChainHash(): {15},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		chainBlockIDs := []uint32{5, 10, 15}

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDs, true, nil)

		// Should succeed because it's the same block being mined
		require.NoError(t, err)

		mockStore.AssertExpectations(t)
	})

	t.Run("should handle empty chainBlockIDsMap", func(t *testing.T) {
		mockStore = &utxo.MockUtxostore{} // Reset mock

		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx1.TxIDChainHash(): {99, 15},
			*testTx2.TxIDChainHash(): {15},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		// Empty chain block IDs - should skip validation
		chainBlockIDs := []uint32{}

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDs, true, nil)

		require.NoError(t, err)

		mockStore.AssertExpectations(t)
	})

	t.Run("should detect transaction mined outside retention window", func(t *testing.T) {
		mockStore = &utxo.MockUtxostore{} // Reset mock

		// Simulate a transaction that was mined in block 1000 (very old).
		// The current blockID (1600) is also present per the SetMinedMulti
		// postcondition; testTx2 is in the same subtree so must be covered too.
		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx1.TxIDChainHash(): {1000, 1600},
			*testTx2.TxIDChainHash(): {1600},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		// Chain includes many blocks including the old block 1000
		// This simulates unlimited depth fetch (the security fix)
		chainBlockIDs := []uint32{1000, 1100, 1200, 1300, 1400, 1500, 1600}

		// Trying to mine block 1600 which contains a transaction already in block 1000
		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 1600, chainBlockIDs, true, nil)

		// Should get BlockInvalidError because transaction was already mined in block 1000 (on same chain)
		// even though it's way outside the old retention*2 window (which would have been ~576 blocks)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block contains a transaction already on our chain")
		assert.Contains(t, err.Error(), "1000") // Should mention the conflicting block ID

		mockStore.AssertExpectations(t)
	})
}

// TestUpdateTxMinedStatus_ContextCancellation tests context cancellation scenarios
func TestUpdateTxMinedStatus_ContextCancellation(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	mockStore := &utxo.MockUtxostore{}

	testTx := newTx(100)
	block := &Block{}
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *testTx.TxIDChainHash()},
			},
		},
	}

	t.Run("should handle context cancellation during processing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context before processing
		cancel()

		emptyBlockIDsMap := map[chainhash.Hash][]uint32{}
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(emptyBlockIDsMap, errors.NewStorageError("storage error")).Maybe()

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

}

// TestUpdateTxMinedStatus_DuplicateDetection tests the duplicate block processing detection
func TestUpdateTxMinedStatus_DuplicateDetection(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}

	testTx := newTx(100)
	block := &Block{}
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *testTx.TxIDChainHash()},
			},
		},
	}

	t.Run("should ignore duplicate calls for same block", func(t *testing.T) {
		// Reset worker settings and in-flight tracking for this test
		setWorkerSettings(tSettings)
		inFlightBlocksMu.Lock()
		inFlightBlocks = make(map[uint32]bool)
		inFlightBlocksMu.Unlock()

		mockStore := &utxo.MockUtxostore{}

		// Setup a controlled delay for SetMinedMulti to ensure we can test concurrent calls
		processingStarted := make(chan struct{})
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				close(processingStarted)
				time.Sleep(50 * time.Millisecond) // Delay to ensure the second call happens during processing
			}).
			Return(realMinedReturn(15), nil).Once()

		// Start the first call in a goroutine
		done1 := make(chan error)
		go func() {
			err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)
			done1 <- err
		}()

		// Wait for the first call to actually start processing
		<-processingStarted

		// Second call should be ignored immediately (duplicate detection)
		err2 := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)
		require.Error(t, err2) // Should return parent not mined error
		assert.Contains(t, err2.Error(), "already being processed")

		// Wait for first call to complete
		err1 := <-done1
		require.NoError(t, err1)

		// SetMinedMulti should only be called once (not twice)
		mockStore.AssertExpectations(t)
	})

	t.Run("should allow processing of different blocks concurrently", func(t *testing.T) {
		// Reset worker settings and in-flight tracking for this test
		setWorkerSettings(tSettings)
		inFlightBlocksMu.Lock()
		inFlightBlocks = make(map[uint32]bool)
		inFlightBlocksMu.Unlock()

		mockStore1 := &utxo.MockUtxostore{}
		mockStore2 := &utxo.MockUtxostore{}

		mockStore1.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(realMinedReturn(15), nil).Once()
		mockStore2.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(realMinedReturn(16), nil).Once()

		// Call with different block IDs
		done1 := make(chan error)
		done2 := make(chan error)

		go func() {
			err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore1, block, 15, []uint32{}, true, nil)
			done1 <- err
		}()

		go func() {
			err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore2, block, 16, []uint32{}, true, nil)
			done2 <- err
		}()

		// Both should complete successfully
		err1 := <-done1
		err2 := <-done2
		require.NoError(t, err1)
		require.NoError(t, err2)

		// Both should have called SetMinedMulti
		mockStore1.AssertExpectations(t)
		mockStore2.AssertExpectations(t)
	})
}

// TestUpdateTxMinedStatus_ConfigurationDisabled tests disabled configuration scenario
func TestUpdateTxMinedStatus_ConfigurationDisabled(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	testTx := newTx(100)
	block := &Block{}
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *testTx.TxIDChainHash()},
			},
		},
	}

	// Create a fresh mock for this test to avoid interference from previous tests
	freshMockStore := &utxo.MockUtxostore{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: false, // Disabled
	}
	setWorkerSettings(tSettings)

	// Should not call SetMinedMulti when disabled
	err := UpdateTxMinedStatus(ctx, logger, tSettings, freshMockStore, block, 15, []uint32{}, true, nil)

	require.NoError(t, err)
	// Allow some time for any async processing to complete
	time.Sleep(10 * time.Millisecond)
	freshMockStore.AssertNotCalled(t, "SetMinedMulti")
}

// TestUpdateTxMinedStatus_DifferentBatchSizes tests different batch size configurations
func TestUpdateTxMinedStatus_DifferentBatchSizes(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	// Create a fresh mock for this test to avoid interference from previous tests
	freshMockStore := &utxo.MockUtxostore{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   1, // Very small batch size
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	// Create block with multiple transactions to test batching
	multiTxBlock := &Block{}
	multiTxBlock.Height = 100
	multiTxHash := newTx(1).TxIDChainHash()
	multiTxBlock.Subtrees = []*chainhash.Hash{multiTxHash}
	multiTxBlock.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *newTx(1).TxIDChainHash()},
				{Hash: *newTx(2).TxIDChainHash()},
				{Hash: *newTx(3).TxIDChainHash()},
			},
		},
	}

	// Should be called multiple times due to small batch size
	// With 3 transactions and batch size 1:
	// - idx=0: added to batch
	// - idx=1: added to batch, condition met (1 > 0 && 1%1==0), calls SetMinedMulti with 2 hashes, clears batch
	// - idx=2: added to batch, condition met (2 > 0 && 2%1==0), calls SetMinedMulti with 1 hash, clears batch
	// - end: no remaining hashes, so no remainder call
	freshMockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
		Return(realMinedReturn(15), nil).Times(2) // 2 calls: first with 2 hashes, second with 1 hash

	err := UpdateTxMinedStatus(ctx, logger, tSettings, freshMockStore, multiTxBlock, 15, []uint32{}, true, nil)

	require.NoError(t, err)
	// Allow some time for any async processing to complete
	time.Sleep(10 * time.Millisecond)
	freshMockStore.AssertExpectations(t)
}

// TestUpdateTxMinedStatus_CoinbasePlaceholderHandling tests coinbase placeholder handling
func TestUpdateTxMinedStatus_CoinbasePlaceholderHandling(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	mockStore := &utxo.MockUtxostore{}

	testTx := newTx(100)
	block := &Block{}
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *subtree.CoinbasePlaceholderHash}, // Coinbase placeholder (should be skipped)
				{Hash: *testTx.TxIDChainHash()},          // Regular transaction
			},
		},
	}

	t.Run("should skip coinbase placeholder in first subtree", func(t *testing.T) {
		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {15},
		}

		// Should only be called once for the regular transaction (coinbase placeholder skipped)
		mockStore.On("SetMinedMulti", mock.Anything, mock.AnythingOfType("[]*chainhash.Hash"), mock.Anything).
			Run(func(args mock.Arguments) {
				hashes := args.Get(1).([]*chainhash.Hash)
				// Should only have 1 hash (the regular tx, not the coinbase placeholder)
				assert.Len(t, hashes, 1)
				assert.Equal(t, testTx.TxIDChainHash(), hashes[0])
			}).
			Return(expectedBlockIDsMap, nil).Once()

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("should warn about coinbase placeholder in wrong position", func(t *testing.T) {
		mockStore = &utxo.MockUtxostore{} // Reset mock

		// Create block with coinbase placeholder in wrong position
		wrongPosBlock := &Block{}
		wrongPosBlock.Height = 100
		wrongPosBlock.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		wrongPosBlock.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},          // Regular transaction first
					{Hash: *subtree.CoinbasePlaceholderHash}, // Coinbase placeholder in wrong position
				},
			},
		}

		expectedBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {15},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(expectedBlockIDsMap, nil).Once()

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, wrongPosBlock, 15, []uint32{}, true, nil)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})
}

// TestUpdateTxMinedStatus_ConcurrentProcessing tests concurrent processing of multiple subtrees
func TestUpdateTxMinedStatus_ConcurrentProcessing(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    3, // Allow concurrent processing
	}
	setWorkerSettings(tSettings)

	mockStore := &utxo.MockUtxostore{}

	// Create block with multiple subtrees
	block := &Block{}
	block.Height = 100
	block.Subtrees = []*chainhash.Hash{
		newTx(1).TxIDChainHash(),
		newTx(3).TxIDChainHash(),
		newTx(5).TxIDChainHash(),
	}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.Node{
				{Hash: *newTx(1).TxIDChainHash()},
				{Hash: *newTx(2).TxIDChainHash()},
			},
		},
		{
			Nodes: []subtree.Node{
				{Hash: *newTx(3).TxIDChainHash()},
				{Hash: *newTx(4).TxIDChainHash()},
			},
		},
		{
			Nodes: []subtree.Node{
				{Hash: *newTx(5).TxIDChainHash()},
			},
		},
	}

	t.Run("should process multiple subtrees concurrently", func(t *testing.T) {
		// Should be called 3 times (once per subtree)
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(realMinedReturn(15), nil).Times(3)

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})
}

// TestUpdateTxMinedStatus_MissingSubtree tests handling of missing subtree
func TestUpdateTxMinedStatus_MissingSubtree(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	mockStore := &utxo.MockUtxostore{}

	t.Run("should return error for missing subtree", func(t *testing.T) {
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{newTx(1).TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{nil} // Missing subtree

		err := UpdateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, []uint32{}, true, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing subtree")
	})
}

// Test_updateTxMinedStatus_Internal tests the internal updateTxMinedStatus function directly
func Test_updateTxMinedStatus_Internal(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   2, // Small batch size for testing
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	t.Run("should handle different batch remainder scenarios", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		// Create block with 5 transactions (will create 2 full batches + 1 remainder)
		block := &Block{}
		block.Height = 100
		subtreeHash := newTx(1).TxIDChainHash()
		block.Subtrees = []*chainhash.Hash{subtreeHash}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *newTx(1).TxIDChainHash()},
					{Hash: *newTx(2).TxIDChainHash()},
					{Hash: *newTx(3).TxIDChainHash()},
					{Hash: *newTx(4).TxIDChainHash()},
					{Hash: *newTx(5).TxIDChainHash()},
				},
			},
		}

		// With batch size 2 and 5 transactions:
		// idx=0,1: accumulate → hashes=[0,1]
		// idx=2: accumulate then trigger (2%2==0) → SetMinedMulti with 3 hashes [0,1,2], reset
		// idx=3: accumulate → hashes=[3]
		// idx=4: accumulate then trigger (4%2==0) → SetMinedMulti with 2 hashes [3,4], reset
		// So expect 2 calls: first with 3 hashes, second with 2 hashes
		mockStore.On("SetMinedMulti", mock.Anything, mock.AnythingOfType("[]*chainhash.Hash"), mock.Anything).
			Run(func(args mock.Arguments) {
				hashes := args.Get(1).([]*chainhash.Hash)
				// First call should have 3 hashes, second call should have 2 hashes
				assert.True(t, len(hashes) == 3 || len(hashes) == 2, "Batch size should be 3 or 2")
			}).
			Return(realMinedReturn(15), nil).Times(2)

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("should validate block IDs against current chain in internal function", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(100)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// Mock SetMinedMulti to return conflicting block IDs plus the current
		// blockID 15 (required by the coverage postcondition).
		conflictingBlockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {5, 10, 15},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(conflictingBlockIDsMap, nil).Once()

		// Chain contains block IDs that conflict
		chainBlockIDsMap := map[uint32]bool{5: true, 10: true, 15: true}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "block contains a transaction already on our chain")

		mockStore.AssertExpectations(t)
	})

	t.Run("should skip processing when UpdateTxMinedStatus disabled", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		disabledSettings := test.CreateBaseTestSettings(t)
		disabledSettings.UtxoStore = settings.UtxoStoreSettings{
			UpdateTxMinedStatus: false, // Disabled
		}

		testTx := newTx(100)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, disabledSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.NoError(t, err)
		mockStore.AssertNotCalled(t, "SetMinedMulti")
	})

	t.Run("should handle mixed block ID validation results", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		tx1 := newTx(1)
		tx2 := newTx(2)
		tx3 := newTx(3)

		block := &Block{}
		block.Height = 100
		subtreeHash := tx1.TxIDChainHash()
		block.Subtrees = []*chainhash.Hash{subtreeHash}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *tx1.TxIDChainHash()},
					{Hash: *tx2.TxIDChainHash()},
					{Hash: *tx3.TxIDChainHash()},
				},
			},
		}

		// Mixed results: tx1 conflicts, tx2 is new, tx3 is same block.
		// All slices include the current blockID 15 per the coverage postcondition.
		mixedBlockIDsMap := map[chainhash.Hash][]uint32{
			*tx1.TxIDChainHash(): {5, 15},  // Conflicts with current chain
			*tx2.TxIDChainHash(): {99, 15}, // Not on current chain (OK)
			*tx3.TxIDChainHash(): {15},     // Same block being mined (OK)
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(mixedBlockIDsMap, nil).Once()

		// Chain contains block ID 5 and 15
		chainBlockIDsMap := map[uint32]bool{5: true, 15: true}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		// Should fail because tx1 conflicts (block ID 5 is on current chain)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "block contains a transaction already on our chain")
		assert.Contains(t, err.Error(), "blockID 5")

		mockStore.AssertExpectations(t)
	})

	t.Run("should error when SetMinedMulti returns empty blockIDsMap (coverage gap)", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(100)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// Empty blockIDsMap — every submitted hash is unaccounted for, which the
		// SetMinedMulti coverage postcondition now treats as a hard error.
		emptyBlockIDsMap := map[chainhash.Hash][]uint32{}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(emptyBlockIDsMap, nil).Once()

		chainBlockIDsMap := map[uint32]bool{5: true, 10: true}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set mined status")

		mockStore.AssertExpectations(t)
	})

	t.Run("should error when SetMinedMulti omits the current blockID for a hash", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(101)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// The hash is present but its slice does not contain blockID 15.
		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {7, 9}, // current blockID 15 missing
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set mined status")

		mockStore.AssertExpectations(t)
	})

	t.Run("should error when SetMinedMulti partially omits hashes (expression path empty BlockIDs)", func(t *testing.T) {
		// Simulates the Aerospike expression path returning an empty state.BlockIDs
		// for one record: that hash gets dropped from blockIDsMap entirely while
		// other hashes in the same batch are returned correctly. The model-layer
		// coverage check must flag the missing hash as a postcondition violation.
		mockStore := &utxo.MockUtxostore{}

		tx1 := newTx(201)
		tx2 := newTx(202) // expression path returns empty BlockIDs → omitted from map
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{tx1.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *tx1.TxIDChainHash()},
					{Hash: *tx2.TxIDChainHash()},
				},
			},
		}

		blockIDsMap := map[chainhash.Hash][]uint32{
			*tx1.TxIDChainHash(): {15},
			// tx2 absent — simulates state.BlockIDs == 0 in the expression path
			// where `if len(state.BlockIDs) > 0 { blockIDs[*hash] = ... }` skips
			// the assignment.
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set mined status")

		mockStore.AssertExpectations(t)
	})

	t.Run("should tolerate empty blockIDsMap when unsetMined is true", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(102)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// Empty map is intentional during unset-mined (e.g. tx already gone).
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32{}, nil).Once()

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, true)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("coverage-only failure reports a coverage-specific error", func(t *testing.T) {
		// Pure coverage-gap scenario: SetMinedMulti returns nil error but omits the
		// current blockID for one hash. The final error must identify this as a
		// coverage gap, not as a batch I/O failure.
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(301)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{{Hash: *testTx.TxIDChainHash()}},
			},
		}

		// Hash present, but slice does not contain current blockID 15.
		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {7},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "coverage gap(s) from SetMinedMulti")
		assert.NotContains(t, err.Error(), "batches", "pure coverage-gap error must not mention batches")
		mockStore.AssertExpectations(t)
	})

	t.Run("I/O-only failure reports a batch-specific error", func(t *testing.T) {
		// Pure I/O scenario: SetMinedMulti returns an error. The model layer logs
		// and counts it, then surfaces a batches-failed error.
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(302)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{{Hash: *testTx.TxIDChainHash()}},
			},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32(nil), errors.NewNetworkTimeoutError("simulated timeout")).Once()

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set mined status for")
		assert.Contains(t, err.Error(), "1 batches")
		assert.NotContains(t, err.Error(), "coverage gap", "pure I/O error must not mention coverage gaps")
		mockStore.AssertExpectations(t)
	})

	t.Run("mixed I/O and coverage failures report a combined error", func(t *testing.T) {
		// Two SetMinedMulti calls: one returns success with a coverage gap, the
		// other returns an I/O error. The final error must mention both counts
		// so operators can tell them apart.
		//
		// With MaxMinedBatchSize=1 and 3 nodes the batching code flushes at
		// idx==1 (2 hashes) and again at idx==2 (1 hash). See the comment in
		// updateTxMinedStatus describing the idx > 0 && idx%MaxMinedBatchSize==0
		// trigger.
		freshSettings := test.CreateBaseTestSettings(t)
		freshSettings.UtxoStore = settings.UtxoStoreSettings{
			UpdateTxMinedStatus: true,
			MaxMinedBatchSize:   1,
			MaxMinedRoutines:    1,
		}
		setWorkerSettings(freshSettings)

		mockStore := &utxo.MockUtxostore{}

		tx1 := newTx(401)
		tx2 := newTx(402)
		tx3 := newTx(403)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{tx1.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *tx1.TxIDChainHash()},
					{Hash: *tx2.TxIDChainHash()},
					{Hash: *tx3.TxIDChainHash()},
				},
			},
		}

		// First batch ([tx1, tx2]): success but tx1's slice omits blockID 15 -> 1 coverage gap.
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32{
				*tx1.TxIDChainHash(): {7},  // coverage gap: 15 missing
				*tx2.TxIDChainHash(): {15}, // covered
			}, nil).Once()
		// Second batch ([tx3]): I/O failure -> 1 batch error.
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(map[chainhash.Hash][]uint32(nil), errors.NewNetworkTimeoutError("simulated timeout")).Once()

		err := updateTxMinedStatus(ctx, logger, freshSettings, mockStore, block, 15, map[uint32]bool{}, true, nil, false)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set mined status for 1 batches and 1 coverage gap(s) detected")
		mockStore.AssertExpectations(t)
	})
}

// Test_updateTxMinedStatus_EdgeCases tests additional edge cases and boundary conditions
func Test_updateTxMinedStatus_EdgeCases(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   10,
		MaxMinedRoutines:    1,
	}
	setWorkerSettings(tSettings)

	t.Run("should handle zero length subtree nodes", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		block := &Block{}
		block.Height = 100
		emptyHash := newTx(1).TxIDChainHash() // Placeholder hash for empty subtree
		block.Subtrees = []*chainhash.Hash{emptyHash}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{}, // Empty subtree
			},
		}

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		// Should succeed without calling SetMinedMulti (no nodes to process)
		require.NoError(t, err)
		mockStore.AssertNotCalled(t, "SetMinedMulti")
	})

	t.Run("should handle all coinbase placeholders in subtree", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		block := &Block{}
		block.Height = 100
		placeholderHash := subtree.CoinbasePlaceholderHash
		block.Subtrees = []*chainhash.Hash{placeholderHash}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *subtree.CoinbasePlaceholderHash}, // All placeholders
					{Hash: *subtree.CoinbasePlaceholderHash},
					{Hash: *subtree.CoinbasePlaceholderHash},
				},
			},
		}

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		// Should succeed without calling SetMinedMulti (all placeholders skipped)
		require.NoError(t, err)
		mockStore.AssertNotCalled(t, "SetMinedMulti")
	})

	t.Run("should handle very large batch processing", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		// Create large batch size settings
		largeBatchSettings := test.CreateBaseTestSettings(t)
		largeBatchSettings.UtxoStore = settings.UtxoStoreSettings{
			UpdateTxMinedStatus: true,
			MaxMinedBatchSize:   1000, // Very large batch
			MaxMinedRoutines:    1,
		}

		// Create subtree with many transactions
		nodes := make([]subtree.Node, 500)
		for i := 0; i < 500; i++ {
			nodes[i] = subtree.Node{Hash: *newTx(uint32(i + 1)).TxIDChainHash()}
		}

		block := &Block{}
		block.Height = 100
		largeBatchHash := newTx(1).TxIDChainHash()
		block.Subtrees = []*chainhash.Hash{largeBatchHash}
		block.SubtreeSlices = []*subtree.Subtree{{Nodes: nodes}}

		// Should be called once with all 500 transactions
		mockStore.On("SetMinedMulti", mock.Anything, mock.AnythingOfType("[]*chainhash.Hash"), mock.Anything).
			Run(func(args mock.Arguments) {
				hashes := args.Get(1).([]*chainhash.Hash)
				assert.Len(t, hashes, 500, "Should process all 500 transactions in one batch")
			}).
			Return(realMinedReturn(15), nil).Once()

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, largeBatchSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("should handle exact batch boundary conditions", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		boundarySettings := test.CreateBaseTestSettings(t)
		boundarySettings.UtxoStore = settings.UtxoStoreSettings{
			UpdateTxMinedStatus: true,
			MaxMinedBatchSize:   3, // Exact boundary testing
			MaxMinedRoutines:    1,
		}

		// Create exactly 3 transactions (should fit in one batch exactly)
		block := &Block{}
		block.Height = 100
		boundaryHash := newTx(1).TxIDChainHash()
		block.Subtrees = []*chainhash.Hash{boundaryHash}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *newTx(1).TxIDChainHash()},
					{Hash: *newTx(2).TxIDChainHash()},
					{Hash: *newTx(3).TxIDChainHash()},
				},
			},
		}

		// Should be called exactly once with all 3 transactions
		mockStore.On("SetMinedMulti", mock.Anything, mock.AnythingOfType("[]*chainhash.Hash"), mock.Anything).
			Run(func(args mock.Arguments) {
				hashes := args.Get(1).([]*chainhash.Hash)
				assert.Len(t, hashes, 3, "Should process exactly 3 transactions in one batch")
			}).
			Return(realMinedReturn(15), nil).Once()

		chainBlockIDsMap := map[uint32]bool{}

		err := updateTxMinedStatus(ctx, logger, boundarySettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("should continue processing all transactions even when SetMinedMulti errors occur", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		// Create block with multiple transactions
		tx1 := newTx(1)
		tx2 := newTx(2)
		tx3 := newTx(3)

		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{tx1.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *tx1.TxIDChainHash()},
					{Hash: *tx2.TxIDChainHash()},
					{Hash: *tx3.TxIDChainHash()},
				},
			},
		}

		emptyBlockIDsMap := map[chainhash.Hash][]uint32{}

		// Mock SetMinedMulti to return an error - simulating a timeout or storage error
		// The new behavior should log this error but continue processing
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(emptyBlockIDsMap, errors.NewNetworkTimeoutError("timeout error")).Once()

		chainBlockIDsMap := map[uint32]bool{}

		// Call with unsetMined=false (valid block) - errors should be returned
		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, false)

		// Should return error for valid blocks when SetMinedMulti fails
		require.Error(t, err)
		// Error message should be generic, not containing the original "timeout error" string
		assert.Contains(t, err.Error(), "failed to set mined status for")
		assert.Contains(t, err.Error(), "1 batches") // 1 batch failed

		mockStore.AssertExpectations(t)
	})

	t.Run("should not return error for invalid blocks when SetMinedMulti errors occur", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(100)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		emptyBlockIDsMap := map[chainhash.Hash][]uint32{}

		// Mock SetMinedMulti to return an error
		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(emptyBlockIDsMap, errors.NewStorageError("storage error")).Once()

		chainBlockIDsMap := map[uint32]bool{}

		// Call with unsetMined=true (invalid block) - errors should be logged but not returned
		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, nil, true)

		// Should NOT return error for invalid blocks - errors are logged only
		require.NoError(t, err)

		mockStore.AssertExpectations(t)
	})
}

// mockBlockchainClientForSlowPath is a simple mock implementation of blockchainClientI for testing slow-path detection
// We define it locally to avoid import cycles since services/blockchain imports model
type mockBlockchainClientForSlowPath struct {
	checkBlockResult bool
	checkBlockError  error
	calledWith       []uint32
	calledWithHash   *chainhash.Hash
	wasCalled        bool
}

func (m *mockBlockchainClientForSlowPath) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	m.calledWith = blockIDs
	m.calledWithHash = blockHash
	m.wasCalled = true
	return m.checkBlockResult, m.checkBlockError
}

func TestUpdateTxMinedStatus_SlowPathDetection(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UpdateTxMinedStatus = true
	setWorkerSettings(tSettings)

	t.Run("should detect collision via slow-path when block ID is not in chainBlockIDsMap", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(100)
		block := &Block{}
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// SetMinedMulti returns block ID 5 (which is NOT in chainBlockIDsMap)
		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {5},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		// chainBlockIDsMap only contains block ID 10 (not 5)
		chainBlockIDsMap := map[uint32]bool{10: true}

		// Mock blockchain client returns true (block 5 is on current chain)
		mockClient := &mockBlockchainClientForSlowPath{checkBlockResult: true}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, mockClient, false)

		// Should get BlockInvalidError because slow-path detected collision
		require.Error(t, err)
		assert.Contains(t, err.Error(), "slow path")
		assert.Contains(t, err.Error(), "transaction already on our chain")

		// Verify the blockchain client was called with block ID 5
		require.True(t, mockClient.wasCalled)
		require.Contains(t, mockClient.calledWith, uint32(5))

		mockStore.AssertExpectations(t)
	})

	t.Run("should not error when slow-path finds block IDs not on current chain", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(200)
		block := &Block{}
		block.Height = 200
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// SetMinedMulti returns block IDs 99 (historical, NOT in chainBlockIDsMap)
		// and 25 (the current blockID being mined; required by the postcondition).
		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {99, 25},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		// chainBlockIDsMap does not contain block ID 99
		chainBlockIDsMap := map[uint32]bool{10: true, 20: true}

		// Mock blockchain client returns false (block 99 is NOT on current chain - orphaned block)
		mockClient := &mockBlockchainClientForSlowPath{checkBlockResult: false}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 25, chainBlockIDsMap, true, mockClient, false)

		// Should succeed because block 99 is not on current chain
		require.NoError(t, err)

		// Verify the blockchain client was called
		require.True(t, mockClient.wasCalled)
		require.Contains(t, mockClient.calledWith, uint32(99))

		mockStore.AssertExpectations(t)
	})

	t.Run("should skip slow-path when unsetMined is true", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(300)
		block := &Block{}
		block.Height = 300
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		// SetMinedMulti returns block ID 5 (which is NOT in chainBlockIDsMap)
		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {5},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		chainBlockIDsMap := map[uint32]bool{10: true}

		// Mock blockchain client - should NOT be called when unsetMined is true
		mockClient := &mockBlockchainClientForSlowPath{checkBlockResult: true}

		// unsetMined=true should skip slow-path check
		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, mockClient, true)

		require.NoError(t, err)

		// Verify the blockchain client was NOT called
		require.False(t, mockClient.wasCalled)

		mockStore.AssertExpectations(t)
	})

	t.Run("should handle blockchain client error gracefully", func(t *testing.T) {
		mockStore := &utxo.MockUtxostore{}

		testTx := newTx(400)
		block := &Block{}
		block.Height = 400
		block.Subtrees = []*chainhash.Hash{testTx.TxIDChainHash()}
		block.SubtreeSlices = []*subtree.Subtree{
			{
				Nodes: []subtree.Node{
					{Hash: *testTx.TxIDChainHash()},
				},
			},
		}

		blockIDsMap := map[chainhash.Hash][]uint32{
			*testTx.TxIDChainHash(): {5},
		}

		mockStore.On("SetMinedMulti", mock.Anything, mock.Anything, mock.Anything).
			Return(blockIDsMap, nil).Once()

		chainBlockIDsMap := map[uint32]bool{10: true}

		// Mock blockchain client returns an error
		mockClient := &mockBlockchainClientForSlowPath{checkBlockError: errors.NewStorageError("database error")}

		err := updateTxMinedStatus(ctx, logger, tSettings, mockStore, block, 15, chainBlockIDsMap, true, mockClient, false)

		// Should return error from blockchain client
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check old block IDs")

		mockStore.AssertExpectations(t)
	})
}
