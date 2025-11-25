package subtreeprocessor

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTxIDAndFee(t *testing.T) {
	// Create a test node
	hash := chainhash.HashH([]byte("test-tx"))
	node := subtreepkg.Node{
		Hash:        hash,
		Fee:         1000,
		SizeInBytes: 500,
	}

	// Create a new TxIDAndFee
	txIDAndFee := NewTxIDAndFee(node)

	// Verify the node was correctly set
	assert.Equal(t, node.Hash, txIDAndFee.node.Hash, "Hash should match")
	assert.Equal(t, node.Fee, txIDAndFee.node.Fee, "Fee should match")
	assert.Equal(t, node.SizeInBytes, txIDAndFee.node.SizeInBytes, "SizeInBytes should match")
}

func TestTxIDAndFeeBatch(t *testing.T) {
	// Create a new batch with size 3
	batch := NewTxIDAndFeeBatch(3)
	require.NotNil(t, batch, "Batch should not be nil")
	assert.Equal(t, 3, batch.size, "Batch size should be 3")
	assert.Equal(t, 0, len(batch.txs), "Batch should be empty initially")
	assert.Equal(t, 3, cap(batch.txs), "Batch capacity should be 3")

	// Create test transactions
	tx1 := NewTxIDAndFee(subtreepkg.Node{
		Hash:        chainhash.HashH([]byte("tx1")),
		Fee:         100,
		SizeInBytes: 200,
	})
	tx2 := NewTxIDAndFee(subtreepkg.Node{
		Hash:        chainhash.HashH([]byte("tx2")),
		Fee:         200,
		SizeInBytes: 300,
	})
	tx3 := NewTxIDAndFee(subtreepkg.Node{
		Hash:        chainhash.HashH([]byte("tx3")),
		Fee:         300,
		SizeInBytes: 400,
	})

	// Test adding transactions one by one
	result1 := batch.Add(tx1)
	assert.Nil(t, result1, "Result should be nil when batch is not full")
	assert.Equal(t, 1, len(batch.txs), "Batch should have 1 transaction")

	result2 := batch.Add(tx2)
	assert.Nil(t, result2, "Result should be nil when batch is not full")
	assert.Equal(t, 2, len(batch.txs), "Batch should have 2 transactions")

	// Adding the third transaction should return the full batch
	result3 := batch.Add(tx3)
	require.NotNil(t, result3, "Result should not be nil when batch is full")
	assert.Equal(t, 3, len(*result3), "Result batch should have 3 transactions")
	assert.Equal(t, tx1, (*result3)[0], "First transaction should be tx1")
	assert.Equal(t, tx2, (*result3)[1], "Second transaction should be tx2")
	assert.Equal(t, tx3, (*result3)[2], "Third transaction should be tx3")

	// The batch should be reset after returning the full batch
	assert.Equal(t, 0, len(batch.txs), "Batch should be empty after returning full batch")
	assert.Equal(t, 3, cap(batch.txs), "Batch capacity should still be 3")
}

func TestTxIDAndFeeBatchConcurrency(t *testing.T) {
	// Create a new batch with size 10
	batch := NewTxIDAndFeeBatch(10)

	// Create a wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(20) // 20 goroutines

	// Create a channel to collect results
	resultChan := make(chan *[]*TxIDAndFee, 5)

	// Launch 20 goroutines to add transactions concurrently
	for i := 0; i < 20; i++ {
		go func(id int) {
			defer wg.Done()

			// Create a unique transaction
			tx := NewTxIDAndFee(subtreepkg.Node{
				Hash:        chainhash.HashH([]byte("tx-concurrent-" + strconv.Itoa(id))),
				Fee:         uint64(id * 100),
				SizeInBytes: uint64(id * 200),
			})

			// Add the transaction to the batch
			result := batch.Add(tx)
			if result != nil {
				resultChan <- result
			}

			// Small delay to increase chance of race conditions
			time.Sleep(time.Millisecond)
		}(i)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect and verify results
	batchCount := 0
	totalTxs := 0
	for result := range resultChan {
		batchCount++
		totalTxs += len(*result)
		assert.Equal(t, 10, len(*result), "Each full batch should have 10 transactions")
	}

	// We should have at least one batch result
	assert.GreaterOrEqual(t, batchCount, 1, "Should have at least one batch result")

	// We should have at most 2 batch results (20 transactions / 10 batch size = 2)
	assert.LessOrEqual(t, batchCount, 2, "Should have at most 2 batch results")

	// The total number of transactions in all batches should be at most 20
	assert.LessOrEqual(t, totalTxs, 20, "Total transactions should be at most 20")

	// Check the final state of the batch
	assert.LessOrEqual(t, len(batch.txs), 10, "Final batch should have at most 10 transactions")
}
