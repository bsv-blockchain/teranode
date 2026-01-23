package subtreevalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewStreamingProcessor(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	blockHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	blockIds[1] = true

	sp := newStreamingProcessor(server, blockHash, 100, blockIds)

	assert.NotNil(t, sp)
	assert.NotNil(t, sp.waitingOnParent)
	assert.NotNil(t, sp.readyBatch)
	assert.Equal(t, server, sp.server)
	assert.Equal(t, blockHash, sp.blockHash)
	assert.Equal(t, uint32(100), sp.blockHeight)
	assert.Equal(t, blockIds, sp.blockIds)
}

func TestFilterAlreadyValidated(t *testing.T) {
	t.Run("AllUnvalidated", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		transactions := []*bt.Tx{tx1, tx2}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock BatchDecorate to return nil data (unvalidated)
		server.utxoStore.(*utxo.MockUtxostore).ExpectedCalls = nil
		server.utxoStore.(*utxo.MockUtxostore).On("BatchDecorate",
			mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				// Leave data as nil to indicate unvalidated
			}).
			Return(nil)

		unvalidated, err := sp.filterAlreadyValidated(context.Background(), transactions)
		require.NoError(t, err)

		// All transactions should be returned as unvalidated
		assert.Len(t, unvalidated, 2)
	})

	t.Run("AllValidated", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		transactions := []*bt.Tx{tx1, tx2}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock BatchDecorate to return valid data (already validated)
		server.utxoStore.(*utxo.MockUtxostore).ExpectedCalls = nil
		server.utxoStore.(*utxo.MockUtxostore).On("BatchDecorate",
			mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, item := range batch {
					item.Data = &utxometa.Data{Creating: false}
				}
			}).
			Return(nil)

		unvalidated, err := sp.filterAlreadyValidated(context.Background(), transactions)
		require.NoError(t, err)

		// No transactions should be returned (all validated)
		assert.Len(t, unvalidated, 0)
		assert.Equal(t, int64(2), sp.skippedTransactions.Load())
	})

	t.Run("MixedValidation", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transactions
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		transactions := []*bt.Tx{tx1, tx2}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock BatchDecorate to return mixed results
		server.utxoStore.(*utxo.MockUtxostore).ExpectedCalls = nil
		server.utxoStore.(*utxo.MockUtxostore).On("BatchDecorate",
			mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				batch := args.Get(1).([]*utxo.UnresolvedMetaData)
				// First tx is validated, second is not
				if len(batch) > 0 {
					batch[0].Data = &utxometa.Data{Creating: false}
				}
				// Leave second as nil (unvalidated)
			}).
			Return(nil)

		unvalidated, err := sp.filterAlreadyValidated(context.Background(), transactions)
		require.NoError(t, err)

		// One transaction should be returned
		assert.Len(t, unvalidated, 1)
		assert.Equal(t, int64(1), sp.skippedTransactions.Load())
	})

	t.Run("SkipsCoinbase", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a coinbase transaction by parsing one from hex
		// This is a real coinbase transaction format
		coinbaseTxHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
		coinbaseTx, err := bt.NewTxFromString(coinbaseTxHex)
		require.NoError(t, err)
		require.True(t, coinbaseTx.IsCoinbase(), "Transaction should be coinbase")

		// Create regular transaction
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		require.False(t, tx1.IsCoinbase(), "Transaction should not be coinbase")

		transactions := []*bt.Tx{coinbaseTx, tx1}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock BatchDecorate
		server.utxoStore.(*utxo.MockUtxostore).ExpectedCalls = nil
		server.utxoStore.(*utxo.MockUtxostore).On("BatchDecorate",
			mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		unvalidated, err := sp.filterAlreadyValidated(context.Background(), transactions)
		require.NoError(t, err)

		// Only non-coinbase transaction should be returned
		assert.Len(t, unvalidated, 1)
	})

	t.Run("SkipsNil", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create regular transaction
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)

		transactions := []*bt.Tx{nil, tx1}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock BatchDecorate
		server.utxoStore.(*utxo.MockUtxostore).ExpectedCalls = nil
		server.utxoStore.(*utxo.MockUtxostore).On("BatchDecorate",
			mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		unvalidated, err := sp.filterAlreadyValidated(context.Background(), transactions)
		require.NoError(t, err)

		// Only non-nil transaction should be returned
		assert.Len(t, unvalidated, 1)
	})
}

func TestClassifyAndProcess(t *testing.T) {
	t.Run("AllIndependent", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create independent transactions (no dependencies between them)
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)
		tx2, err := createTestTransaction("tx2")
		require.NoError(t, err)

		transactions := []*bt.Tx{tx1, tx2}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Mock validator to return success
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = sp.classifyAndProcessForTest(context.Background(), transactions)
		require.NoError(t, err)

		// No dependencies means no waiting transactions
		assert.Equal(t, 0, len(sp.waitingOnParent))
		// Both transactions should have been processed
		assert.Equal(t, int64(2), sp.validatedTransactions.Load())
	})

	t.Run("WithDependencies", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create parent transaction
		parentTx, err := createTestTransaction("tx1")
		require.NoError(t, err)
		parentTxID := parentTx.TxIDChainHash()

		// Create child transaction that depends on parent
		// We need to construct a new tx that actually references the parent
		childTx := bt.NewTx()
		childTx.Version = 1
		childInput := &bt.Input{
			PreviousTxOutIndex: 0,
			SequenceNumber:     0xffffffff,
		}
		err = childInput.PreviousTxIDAdd(parentTxID)
		require.NoError(t, err)
		childTx.Inputs = append(childTx.Inputs, childInput)

		// Verify the child's input references the parent
		require.Equal(t, parentTxID.String(), childTx.Inputs[0].PreviousTxIDChainHash().String())

		transactions := []*bt.Tx{parentTx, childTx}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Add tx hashes to pending set (this is done before classification)
		// Mock validator to return success
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = sp.classifyAndProcessForTest(context.Background(), transactions)
		require.NoError(t, err)

		// Both parent and child should be fully processed (streaming processes everything)
		assert.Equal(t, int64(2), sp.validatedTransactions.Load())
		// After complete processing, waitingOnParent should be empty
		assert.Len(t, sp.waitingOnParent, 0, "waitingOnParent should be empty after complete processing")
		assert.Len(t, sp.readyBatch, 0, "readyBatch should be empty after complete processing")
	})

	t.Run("EmptyTransactions", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		transactions := []*bt.Tx{}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		err := sp.classifyAndProcessForTest(context.Background(), transactions)
		require.NoError(t, err)

		// assert.Equal(t, 0, maxDeps)
		assert.Equal(t, int64(0), sp.validatedTransactions.Load())
	})
}

func TestProcessBuckets(t *testing.T) {
	t.Run("EmptyBuckets", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		err := sp.processBuckets(context.Background(), 0)
		require.NoError(t, err)
	})

	t.Run("WithReadyBatch", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create test transaction
		tx1, err := createTestTransaction("tx1")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Add to ready batch
		sp.readyBatch = append(sp.readyBatch, &pendingTx{
			tx:            tx1,
			idx:           0,
			remainingDeps: 0,
		})

		// Mock validator
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = sp.processBuckets(context.Background(), 1)
		require.NoError(t, err)

		assert.Equal(t, int64(1), sp.validatedTransactions.Load())
	})
}

func TestOnTxProcessed(t *testing.T) {
	t.Run("NoChildren", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		tx, err := createTestTransaction("tx1")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// No children waiting
		sp.onTxProcessed(tx)

		// Should not panic, readyBatch should remain empty
		assert.Len(t, sp.readyBatch, 0)
	})

	t.Run("WithChildren", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		parentTx, err := createTestTransaction("tx1")
		require.NoError(t, err)
		childTx, err := createTestTransaction("tx2")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Set up child waiting on parent
		childPtx := &pendingTx{
			tx:            childTx,
			idx:           1,
			remainingDeps: 1,
		}
		sp.waitingOnParent[*parentTx.TxIDChainHash()] = []*pendingTx{childPtx}

		sp.onTxProcessed(parentTx)

		// Child should now be in ready batch
		assert.Len(t, sp.readyBatch, 1)
		assert.Equal(t, int32(0), atomic.LoadInt32(&childPtx.remainingDeps))
	})

	t.Run("MultipleChildren", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		parentTx, err := createTestTransaction("tx1")
		require.NoError(t, err)
		child1Tx, err := createTestTransaction("tx2")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Set up multiple children waiting on parent
		child1Ptx := &pendingTx{
			tx:            child1Tx,
			idx:           1,
			remainingDeps: 1,
		}
		child2Ptx := &pendingTx{
			tx:            child1Tx, // reuse for simplicity
			idx:           2,
			remainingDeps: 1,
		}
		sp.waitingOnParent[*parentTx.TxIDChainHash()] = []*pendingTx{child1Ptx, child2Ptx}

		sp.onTxProcessed(parentTx)

		// Both children should now be in ready batch
		assert.Len(t, sp.readyBatch, 2)
	})

	t.Run("ChildWithMultipleParents", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		parent1Tx, err := createTestTransaction("tx1")
		require.NoError(t, err)
		parent2Tx, err := createTestTransaction("tx2")
		require.NoError(t, err)
		childTx, err := createTestTransaction("default")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Child waiting on 2 parents
		childPtx := &pendingTx{
			tx:            childTx,
			idx:           2,
			remainingDeps: 2,
		}
		sp.waitingOnParent[*parent1Tx.TxIDChainHash()] = []*pendingTx{childPtx}
		sp.waitingOnParent[*parent2Tx.TxIDChainHash()] = []*pendingTx{childPtx}

		// Process first parent
		sp.onTxProcessed(parent1Tx)

		// Child should NOT be in ready batch yet (still has 1 dependency)
		assert.Len(t, sp.readyBatch, 0)
		assert.Equal(t, int32(1), atomic.LoadInt32(&childPtx.remainingDeps))

		// Process second parent
		sp.onTxProcessed(parent2Tx)

		// Now child should be in ready batch
		assert.Len(t, sp.readyBatch, 1)
		assert.Equal(t, int32(0), atomic.LoadInt32(&childPtx.remainingDeps))
	})
}

func TestProcessBatch(t *testing.T) {
	t.Run("EmptyBatch", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		err := sp.processBatch(context.Background(), []*pendingTx{})
		require.NoError(t, err)
	})

	t.Run("SuccessfulValidation", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		tx, err := createTestTransaction("tx1")
		require.NoError(t, err)

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)
		sp.validatorOptions = validator.ProcessOptions()

		batch := []*pendingTx{
			{tx: tx, idx: 0, remainingDeps: 0},
		}

		// Mock validator
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err = sp.processBatch(context.Background(), batch)
		require.NoError(t, err)

		assert.Equal(t, int64(1), sp.validatedTransactions.Load())
	})

	t.Run("ConcurrentValidation", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create multiple transactions
		var batch []*pendingTx
		for i := 0; i < 5; i++ {
			tx, err := createTestTransaction("tx1")
			require.NoError(t, err)
			tx.LockTime = uint32(i) // Make each tx unique
			batch = append(batch, &pendingTx{tx: tx, idx: i, remainingDeps: 0})
		}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)
		sp.validatorOptions = validator.ProcessOptions()

		// Mock validator
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		err := sp.processBatch(context.Background(), batch)
		require.NoError(t, err)

		assert.Equal(t, int64(5), sp.validatedTransactions.Load())
	})
}

func TestStreamingProcessorIntegration(t *testing.T) {
	t.Run("SimpleChain", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		// Create a simple chain: parent -> child
		parentTx, err := createTestTransaction("tx1")
		require.NoError(t, err)
		parentTxID := parentTx.TxIDChainHash()

		// Create child transaction that depends on parent
		childTx := bt.NewTx()
		childTx.Version = 1
		childInput := &bt.Input{
			PreviousTxOutIndex: 0,
			SequenceNumber:     0xffffffff,
		}
		err = childInput.PreviousTxIDAdd(parentTxID)
		require.NoError(t, err)
		childTx.Inputs = append(childTx.Inputs, childInput)

		// Transactions in topological order (parent first)
		transactions := []*bt.Tx{parentTx, childTx}

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)
		sp.validatorOptions = validator.ProcessOptions()

		// Build pending set
		// Mock validator
		mockValidator := server.validatorClient.(*validator.MockValidatorClient)
		mockValidator.UtxoStore = server.utxoStore

		// Mock blockchain client
		server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
			mock.Anything, blockchain.FSMStateRUNNING).
			Return(true, nil)

		// Classify and process (streaming API processes everything including buckets)
		err = sp.classifyAndProcessForTest(context.Background(), transactions)
		require.NoError(t, err)

		// Both parent and child should be fully processed
		assert.Equal(t, int64(2), sp.validatedTransactions.Load())
	})
}

func TestConcurrentAccess(t *testing.T) {
	t.Run("ConcurrentOnTxProcessed", func(t *testing.T) {
		server, cleanup := setupTestServer(t)
		defer cleanup()

		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)

		// Create many parent transactions
		var parents []*bt.Tx
		for i := 0; i < 10; i++ {
			tx, err := createTestTransaction("tx1")
			require.NoError(t, err)
			tx.LockTime = uint32(i)
			parents = append(parents, tx)

			// Each has one child waiting
			childTx, err := createTestTransaction("tx2")
			require.NoError(t, err)
			childTx.LockTime = uint32(i + 100)
			sp.waitingOnParent[*tx.TxIDChainHash()] = []*pendingTx{
				{tx: childTx, idx: i, remainingDeps: 1},
			}
		}

		// Process all parents concurrently
		var wg sync.WaitGroup
		for _, parent := range parents {
			wg.Add(1)
			go func(p *bt.Tx) {
				defer wg.Done()
				sp.onTxProcessed(p)
			}(parent)
		}
		wg.Wait()

		// All children should be in ready batch
		assert.Len(t, sp.readyBatch, 10)
	})
}
