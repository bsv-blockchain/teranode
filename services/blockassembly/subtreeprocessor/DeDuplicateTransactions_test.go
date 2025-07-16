package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	blob_memory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeDuplicateTransactions(t *testing.T) {
	// Setup
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()
	settings := test.CreateBaseTestSettings()
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	stp, err := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, blobStore, nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	// Create duplicate transactions
	hash1 := chainhash.HashH([]byte("tx1"))
	hash2 := chainhash.HashH([]byte("tx2"))
	hash3 := chainhash.HashH([]byte("tx3"))

	// Add the same transaction multiple times
	node1 := subtreepkg.SubtreeNode{Hash: hash1, Fee: 1, SizeInBytes: 100}
	node2 := subtreepkg.SubtreeNode{Hash: hash2, Fee: 2, SizeInBytes: 200}
	node3 := subtreepkg.SubtreeNode{Hash: hash3, Fee: 3, SizeInBytes: 300}

	// Add transactions to the processor
	stp.Add(node1, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash1}})
	stp.Add(node2, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash2}})
	stp.Add(node3, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash3}})

	// Add duplicates
	stp.Add(node1, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash1}})
	stp.Add(node2, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{hash2}})

	// Wait for the queue to be processed
	waitForSubtreeProcessorQueueToEmpty(t, stp)

	// Get initial transaction count
	initialTxCount := stp.TxCount()
	assert.Equal(t, uint64(5), initialTxCount, "Expected 5 transactions before deduplication")

	// Call DeDuplicateTransactions
	stp.DeDuplicateTransactions()

	// Wait for the deduplication to complete
	require.Eventually(t, func() bool {
		return stp.TxCount() == 4 // Expect 4 transactions after deduplication
	}, 1*time.Second, 10*time.Millisecond, "Deduplication did not complete in time")

	// Verify that duplicates were removed
	finalTxCount := stp.TxCount()

	// We should have 4 transactions: coinbase + 3 unique transactions
	assert.Equal(t, uint64(4), finalTxCount, "Expected 4 transactions after deduplication")

	// Verify the transactions are in the currentTxMap
	txMap := stp.GetCurrentTxMap()
	assert.Equal(t, 3, txMap.Length(), "Expected 3 transactions in the currentTxMap")

	// Verify each transaction is present
	_, ok := txMap.Get(hash1)
	assert.True(t, ok, "Expected hash1 to be in the currentTxMap")

	_, ok = txMap.Get(hash2)
	assert.True(t, ok, "Expected hash2 to be in the currentTxMap")

	_, ok = txMap.Get(hash3)
	assert.True(t, ok, "Expected hash3 to be in the currentTxMap")

	// Verify the subtree structure
	assert.NoError(t, stp.CheckSubtreeProcessor(), "Subtree processor should be in a valid state")
}
