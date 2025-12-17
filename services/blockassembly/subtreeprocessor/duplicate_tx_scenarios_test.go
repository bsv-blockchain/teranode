package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Helper Functions
// =============================================================================

// assertNoDuplicateTransactions checks that no transaction appears in multiple subtrees.
// Returns the duplicate transaction hash if found, or nil if no duplicates.
func assertNoDuplicateTransactions(t *testing.T, stp *SubtreeProcessor) *chainhash.Hash {
	t.Helper()

	seen := make(map[chainhash.Hash]int) // maps tx hash to subtree index where first seen

	// Use proper locking to access chainedSubtrees and copy all data
	stp.mu.RLock()

	// Copy chained subtrees nodes under lock
	chainedNodesSnapshot := make([][]subtreepkg.Node, len(stp.chainedSubtrees))
	for i, subtree := range stp.chainedSubtrees {
		chainedNodesSnapshot[i] = make([]subtreepkg.Node, len(subtree.Nodes))
		copy(chainedNodesSnapshot[i], subtree.Nodes)
	}

	// Copy current subtree nodes under lock
	var currentNodesSnapshot []subtreepkg.Node
	if currentSubtree := stp.currentSubtree.Load(); currentSubtree != nil {
		currentNodesSnapshot = make([]subtreepkg.Node, len(currentSubtree.Nodes))
		copy(currentNodesSnapshot, currentSubtree.Nodes)
	}
	stp.mu.RUnlock()

	// Check chained subtrees
	for subtreeIdx, nodes := range chainedNodesSnapshot {
		for _, node := range nodes {
			// Skip coinbase placeholder
			if node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				continue
			}

			if prevIdx, exists := seen[node.Hash]; exists {
				t.Errorf("DUPLICATE FOUND: Transaction %s appears in subtree %d and subtree %d",
					node.Hash.String()[:16], prevIdx, subtreeIdx)
				return &node.Hash
			}
			seen[node.Hash] = subtreeIdx
		}
	}

	// Check current subtree
	if currentNodesSnapshot != nil {
		currentIdx := len(chainedNodesSnapshot)
		for _, node := range currentNodesSnapshot {
			if node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				continue
			}

			if prevIdx, exists := seen[node.Hash]; exists {
				t.Errorf("DUPLICATE FOUND: Transaction %s appears in subtree %d and current subtree (idx %d)",
					node.Hash.String()[:16], prevIdx, currentIdx)
				return &node.Hash
			}
			seen[node.Hash] = currentIdx
		}
	}

	return nil
}

// countTotalTransactions counts all unique transactions across all subtrees
func countTotalTransactions(stp *SubtreeProcessor) int {
	seen := make(map[chainhash.Hash]bool)

	// Use proper locking to access chainedSubtrees and copy all nodes under lock
	stp.mu.RLock()

	// Copy all nodes under lock to avoid races
	chainedNodesSnapshot := make([][]subtreepkg.Node, len(stp.chainedSubtrees))
	for i, subtree := range stp.chainedSubtrees {
		chainedNodesSnapshot[i] = make([]subtreepkg.Node, len(subtree.Nodes))
		copy(chainedNodesSnapshot[i], subtree.Nodes)
	}

	var currentNodesSnapshot []subtreepkg.Node
	if currentSubtree := stp.currentSubtree.Load(); currentSubtree != nil {
		currentNodesSnapshot = make([]subtreepkg.Node, len(currentSubtree.Nodes))
		copy(currentNodesSnapshot, currentSubtree.Nodes)
	}
	stp.mu.RUnlock()

	for _, nodes := range chainedNodesSnapshot {
		for _, node := range nodes {
			if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				seen[node.Hash] = true
			}
		}
	}

	if currentNodesSnapshot != nil {
		for _, node := range currentNodesSnapshot {
			if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
				seen[node.Hash] = true
			}
		}
	}

	return len(seen)
}

// createTestHash creates a deterministic hash from a string
func createTestHash(seed string) chainhash.Hash {
	return chainhash.HashH([]byte(seed))
}

// createTestBlockHeader creates a valid test block header with all required fields
func createTestBlockHeader(timestamp uint32, nonce uint32) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      timestamp,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}

// setupDuplicateTestProcessor creates a SubtreeProcessor configured for duplicate detection tests
func setupDuplicateTestProcessor(t *testing.T, itemsPerSubtree int) (*SubtreeProcessor, blob.Store, chan NewSubtreeRequest) {
	t.Helper()

	ctx := context.Background()
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()
	settings := test.CreateBaseTestSettings(t)

	// Use small subtrees to trigger multiple subtrees quickly
	if itemsPerSubtree > 0 {
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree
	} else {
		settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 100)

	// Use a context to signal shutdown to the consumer goroutine
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())

	// Consume subtree requests to prevent blocking
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case req, ok := <-newSubtreeChan:
				if !ok {
					return
				}
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-consumerCtx.Done():
				// Drain any remaining requests
				for {
					select {
					case req, ok := <-newSubtreeChan:
						if !ok {
							return
						}
						if req.ErrChan != nil {
							req.ErrChan <- nil
						}
					default:
						return
					}
				}
			}
		}
	}()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.AnythingOfType("*chainhash.Hash"), mock.AnythingOfType("[]bool")).Return(nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{}, nil)
	mockBlockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{
		Height:     0,
		CoinbaseTx: coinbaseTx,
		Subtrees:   []*chainhash.Hash{},
		Header:     &model.BlockHeader{},
	}, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(ctx)

	t.Cleanup(func() {
		// First stop the processor to prevent new sends to newSubtreeChan
		stp.Stop(context.Background())
		// Then cancel the consumer context
		cancelConsumer()
		// Wait for consumer to finish draining
		<-consumerDone
	})

	return stp, blobStore, newSubtreeChan
}

// createBlockWithSubtrees creates a block with the specified subtrees stored in blob store
func createBlockWithSubtrees(t *testing.T, blobStore blob.Store, height int, prevHeader *model.BlockHeader, subtrees []*subtreepkg.Subtree) *model.Block {
	t.Helper()

	ctx := context.Background()
	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subtree := range subtrees {
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		hash := subtree.RootHash()
		subtreeHashes[i] = hash

		err = blobStore.Set(ctx, hash[:], fileformat.FileTypeSubtree, subtreeBytes)
		// Ignore BLOB_EXISTS errors - the subtree may already exist from previous operations
		if err != nil {
			if teraErr, ok := err.(*errors.Error); !ok || teraErr.Code() != errors.ERR_BLOB_EXISTS {
				require.NoError(t, err)
			}
		}

		// Store empty metadata
		err = blobStore.Set(ctx, hash[:], fileformat.FileTypeSubtreeMeta, []byte{})
		if err != nil {
			if teraErr, ok := err.(*errors.Error); !ok || teraErr.Code() != errors.ERR_BLOB_EXISTS {
				require.NoError(t, err)
			}
		}
	}

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          uint32(height),
	}

	return &model.Block{
		Height:     uint32(height),
		CoinbaseTx: &bt.Tx{},
		Subtrees:   subtreeHashes,
		Header:     header,
	}
}

// =============================================================================
// Test Scenarios
// =============================================================================

// TestDuplicateTx_RapidSequentialReorgs tests if rapid reorgs can cause duplicate transactions
func TestDuplicateTx_RapidSequentialReorgs(t *testing.T) {
	t.Run("multiple_reorgs_same_transactions", func(t *testing.T) {
		stp, blobStore, _ := setupDuplicateTestProcessor(t, 4)

		// Create transaction hashes that will be reused across reorgs
		txHashes := make([]chainhash.Hash, 10)
		for i := 0; i < 10; i++ {
			txHashes[i] = createTestHash(fmt.Sprintf("tx_%d", i))
		}

		// Create a chain of blocks
		genesisHeader := createTestBlockHeader(1000000000, 0)

		// Add initial transactions
		for i := 0; i < 5; i++ {
			stp.Add(subtreepkg.Node{
				Hash:        txHashes[i],
				Fee:         uint64(100 * (i + 1)),
				SizeInBytes: 250,
			}, subtreepkg.TxInpoints{})
		}

		time.Sleep(100 * time.Millisecond)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Perform multiple rapid reorgs
		for reorgNum := 0; reorgNum < 5; reorgNum++ {
			// Create subtrees with some transactions
			subtree1, _ := subtreepkg.NewTreeByLeafCount(4)
			_ = subtree1.AddCoinbaseNode()
			for i := 0; i < 2; i++ {
				idx := (reorgNum + i) % len(txHashes)
				_ = subtree1.AddSubtreeNode(subtreepkg.Node{
					Hash:        txHashes[idx],
					Fee:         100,
					SizeInBytes: 250,
				})
			}

			moveBackBlock := createBlockWithSubtrees(t, blobStore, reorgNum+1, genesisHeader, []*subtreepkg.Subtree{subtree1})

			subtree2, _ := subtreepkg.NewTreeByLeafCount(4)
			_ = subtree2.AddCoinbaseNode()
			for i := 2; i < 4; i++ {
				idx := (reorgNum + i) % len(txHashes)
				_ = subtree2.AddSubtreeNode(subtreepkg.Node{
					Hash:        txHashes[idx],
					Fee:         100,
					SizeInBytes: 250,
				})
			}

			moveForwardBlock := createBlockWithSubtrees(t, blobStore, reorgNum+1, genesisHeader, []*subtreepkg.Subtree{subtree2})

			// Perform reorg
			_ = stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{moveForwardBlock})

			// Check for duplicates after each reorg
			if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
				t.Fatalf("Duplicate found after reorg %d: %s", reorgNum, dup.String()[:16])
			}
		}

		t.Log("No duplicates found after rapid sequential reorgs")
	})
}

// TestDuplicateTx_ReorgDuringActiveProcessing tests if adding transactions during reorg can cause duplicates
func TestDuplicateTx_ReorgDuringActiveProcessing(t *testing.T) {
	t.Run("concurrent_add_during_reorg", func(t *testing.T) {
		stp, blobStore, _ := setupDuplicateTestProcessor(t, 8)

		// Create a pool of transactions
		txPool := make([]chainhash.Hash, 20)
		for i := 0; i < 20; i++ {
			txPool[i] = createTestHash(fmt.Sprintf("pool_tx_%d", i))
		}

		genesisHeader := createTestBlockHeader(1500000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Add some initial transactions
		for i := 0; i < 5; i++ {
			stp.Add(subtreepkg.Node{
				Hash:        txPool[i],
				Fee:         100,
				SizeInBytes: 250,
			}, subtreepkg.TxInpoints{})
		}
		time.Sleep(50 * time.Millisecond)

		// Create blocks for reorg
		subtree1, _ := subtreepkg.NewTreeByLeafCount(8)
		_ = subtree1.AddCoinbaseNode()
		for i := 0; i < 3; i++ {
			_ = subtree1.AddSubtreeNode(subtreepkg.Node{
				Hash:        txPool[i],
				Fee:         100,
				SizeInBytes: 250,
			})
		}

		moveBackBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{subtree1})

		subtree2, _ := subtreepkg.NewTreeByLeafCount(8)
		_ = subtree2.AddCoinbaseNode()
		for i := 5; i < 8; i++ {
			_ = subtree2.AddSubtreeNode(subtreepkg.Node{
				Hash:        txPool[i],
				Fee:         100,
				SizeInBytes: 250,
			})
		}

		moveForwardBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{subtree2})

		// Start adding transactions concurrently with reorg
		var wg sync.WaitGroup
		var duplicatesFound atomic.Int32

		// Goroutine to add transactions rapidly
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				txIdx := i % len(txPool)
				stp.Add(subtreepkg.Node{
					Hash:        txPool[txIdx],
					Fee:         uint64(100 + i),
					SizeInBytes: 250,
				}, subtreepkg.TxInpoints{})
				time.Sleep(time.Millisecond)
			}
		}()

		// Goroutine to perform reorg
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Slight delay to let adds start
			_ = stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{moveForwardBlock})
		}()

		// Goroutine to check for duplicates periodically
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				time.Sleep(10 * time.Millisecond)
				if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
					duplicatesFound.Add(1)
					t.Logf("Duplicate detected during concurrent processing: %s", dup.String()[:16])
				}
			}
		}()

		wg.Wait()

		// Final check
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("Duplicate found after concurrent reorg test: %s", dup.String()[:16])
		}

		if duplicatesFound.Load() > 0 {
			t.Logf("WARNING: %d duplicate detections during concurrent processing", duplicatesFound.Load())
		} else {
			t.Log("No duplicates found during concurrent reorg processing")
		}
	})
}

// TestDuplicateTx_OverlappingTransactions tests reorg where same TX is in both moveBack and moveForward blocks
func TestDuplicateTx_OverlappingTransactions(t *testing.T) {
	t.Run("same_tx_in_moveback_and_moveforward", func(t *testing.T) {
		stp, blobStore, _ := setupDuplicateTestProcessor(t, 8)

		// Create transaction that will appear in both blocks
		sharedTxHash := createTestHash("shared_transaction")
		uniqueBackTxHash := createTestHash("unique_back_tx")
		uniqueForwardTxHash := createTestHash("unique_forward_tx")

		genesisHeader := createTestBlockHeader(1600000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Add the shared transaction to the processor
		stp.Add(subtreepkg.Node{
			Hash:        sharedTxHash,
			Fee:         100,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		stp.Add(subtreepkg.Node{
			Hash:        uniqueBackTxHash,
			Fee:         200,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		time.Sleep(50 * time.Millisecond)

		// Create moveBack block with shared tx
		moveBackSubtree, _ := subtreepkg.NewTreeByLeafCount(8)
		_ = moveBackSubtree.AddCoinbaseNode()
		_ = moveBackSubtree.AddSubtreeNode(subtreepkg.Node{
			Hash:        sharedTxHash,
			Fee:         100,
			SizeInBytes: 250,
		})
		_ = moveBackSubtree.AddSubtreeNode(subtreepkg.Node{
			Hash:        uniqueBackTxHash,
			Fee:         200,
			SizeInBytes: 250,
		})

		moveBackBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{moveBackSubtree})

		// Create moveForward block with same shared tx
		moveForwardSubtree, _ := subtreepkg.NewTreeByLeafCount(8)
		_ = moveForwardSubtree.AddCoinbaseNode()
		_ = moveForwardSubtree.AddSubtreeNode(subtreepkg.Node{
			Hash:        sharedTxHash, // Same transaction!
			Fee:         100,
			SizeInBytes: 250,
		})
		_ = moveForwardSubtree.AddSubtreeNode(subtreepkg.Node{
			Hash:        uniqueForwardTxHash,
			Fee:         300,
			SizeInBytes: 250,
		})

		moveForwardBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{moveForwardSubtree})

		// Perform reorg
		err := stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{moveForwardBlock})
		if err != nil {
			t.Logf("Reorg returned error (may be expected): %v", err)
		}

		// Check for duplicates
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees after reorg with overlapping transactions", dup.String()[:16])
		} else {
			t.Log("No duplicates found after reorg with overlapping transactions")
		}
	})
}

// TestDuplicateTx_ResetDuringHighVolume tests if reset during high transaction volume can cause duplicates
func TestDuplicateTx_ResetDuringHighVolume(t *testing.T) {
	t.Run("flood_transactions_during_reset", func(t *testing.T) {
		stp, _, _ := setupDuplicateTestProcessor(t, 4)

		// Create a large pool of transactions
		txPool := make([]chainhash.Hash, 100)
		for i := 0; i < 100; i++ {
			txPool[i] = createTestHash(fmt.Sprintf("flood_tx_%d", i))
		}

		genesisHeader := createTestBlockHeader(1700000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		var wg sync.WaitGroup
		var duplicatesFound atomic.Int32

		// Goroutine to flood transactions
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 3; round++ {
				for i := 0; i < len(txPool); i++ {
					stp.Add(subtreepkg.Node{
						Hash:        txPool[i],
						Fee:         uint64(100 + i),
						SizeInBytes: 250,
					}, subtreepkg.TxInpoints{})
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		// Goroutine to perform resets
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				time.Sleep(20 * time.Millisecond)
				resetHeader := &model.BlockHeader{
					Version:   1,
					Timestamp: uint32(1700000000 + i),
					Nonce:     uint32(i),
				}
				_ = stp.Reset(resetHeader, nil, nil, false, nil)
			}
		}()

		// Goroutine to check for duplicates
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				time.Sleep(10 * time.Millisecond)
				if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
					duplicatesFound.Add(1)
					t.Logf("Duplicate detected during reset flood: %s", dup.String()[:16])
				}
			}
		}()

		wg.Wait()

		// Final check
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees after reset during high volume", dup.String()[:16])
		}

		if duplicatesFound.Load() > 0 {
			t.Logf("WARNING: %d duplicate detections during reset flood", duplicatesFound.Load())
		} else {
			t.Log("No duplicates found during reset with high transaction volume")
		}
	})
}

// TestDuplicateTx_ConcurrentTxAddition tests concurrent addition of the same transaction
func TestDuplicateTx_ConcurrentTxAddition(t *testing.T) {
	t.Run("same_tx_from_multiple_goroutines", func(t *testing.T) {
		stp, _, _ := setupDuplicateTestProcessor(t, 8)

		// Single transaction that all goroutines will try to add
		sharedTxHash := createTestHash("concurrent_shared_tx")

		genesisHeader := createTestBlockHeader(1800000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		var wg sync.WaitGroup
		numGoroutines := 10
		additionsPerGoroutine := 100

		// Launch multiple goroutines all trying to add the same transaction
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < additionsPerGoroutine; i++ {
					stp.Add(subtreepkg.Node{
						Hash:        sharedTxHash,
						Fee:         uint64(100 + goroutineID),
						SizeInBytes: 250,
					}, subtreepkg.TxInpoints{})
				}
			}(g)
		}

		wg.Wait()
		time.Sleep(100 * time.Millisecond) // Let processing complete

		// Check for duplicates
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears multiple times after concurrent addition", dup.String()[:16])
		}

		// Count occurrences in currentTxMap - should be exactly 1
		txMap := stp.GetCurrentTxMap()
		if _, exists := txMap.Get(sharedTxHash); !exists {
			t.Error("Shared transaction should exist in txMap")
		}

		// Count in subtrees
		count := 0
		for _, subtree := range stp.chainedSubtrees {
			for _, node := range subtree.Nodes {
				if node.Hash.Equal(sharedTxHash) {
					count++
				}
			}
		}
		if currentSubtree := stp.currentSubtree.Load(); currentSubtree != nil {
			for _, node := range currentSubtree.Nodes {
				if node.Hash.Equal(sharedTxHash) {
					count++
				}
			}
		}

		if count > 1 {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction appears %d times in subtrees (expected 1)", count)
		} else {
			t.Logf("Transaction correctly appears %d time(s) in subtrees after concurrent addition", count)
		}
	})
}

// TestDuplicateTx_SubtreeCompletionRace tests race condition at subtree completion boundary
func TestDuplicateTx_SubtreeCompletionRace(t *testing.T) {
	t.Run("tx_added_at_subtree_boundary", func(t *testing.T) {
		// Use small subtree size to hit completion more often
		stp, _, _ := setupDuplicateTestProcessor(t, 4)

		genesisHeader := createTestBlockHeader(1900000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Create transactions - exactly enough to fill multiple subtrees
		numSubtrees := 5
		txPerSubtree := 3 // 4 - 1 for coinbase placeholder
		totalTx := numSubtrees * txPerSubtree

		txHashes := make([]chainhash.Hash, totalTx)
		for i := 0; i < totalTx; i++ {
			txHashes[i] = createTestHash(fmt.Sprintf("boundary_tx_%d", i))
		}

		var wg sync.WaitGroup
		var duplicatesFound atomic.Int32

		// Add transactions from multiple goroutines to create race at boundaries
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				start := (goroutineID * totalTx) / 4
				end := ((goroutineID + 1) * totalTx) / 4
				for i := start; i < end; i++ {
					stp.Add(subtreepkg.Node{
						Hash:        txHashes[i],
						Fee:         uint64(100 + i),
						SizeInBytes: 250,
					}, subtreepkg.TxInpoints{})
					// Small random delay to vary timing
					if i%3 == 0 {
						time.Sleep(time.Microsecond * 100)
					}
				}
			}(g)
		}

		// Check for duplicates while processing
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				time.Sleep(5 * time.Millisecond)
				if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
					duplicatesFound.Add(1)
				}
			}
		}()

		wg.Wait()
		time.Sleep(100 * time.Millisecond)

		// Final check
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees at boundary", dup.String()[:16])
		}

		if duplicatesFound.Load() > 0 {
			t.Logf("WARNING: %d duplicate detections during subtree boundary processing", duplicatesFound.Load())
		} else {
			t.Log("No duplicates found at subtree completion boundaries")
		}
	})
}

// TestDuplicateTx_ProcessOwnBlockWithDuplicates tests processing a block that already has duplicates
func TestDuplicateTx_ProcessOwnBlockWithDuplicates(t *testing.T) {
	t.Run("block_with_same_tx_in_multiple_subtrees", func(t *testing.T) {
		stp, _, _ := setupDuplicateTestProcessor(t, 4)

		// Create a transaction that will appear in multiple subtrees
		duplicateTxHash := createTestHash("duplicate_in_block")
		uniqueTx1Hash := createTestHash("unique_1_in_block")
		uniqueTx2Hash := createTestHash("unique_2_in_block")

		genesisHeader := createTestBlockHeader(2000000000, 0)

		// Create subtree 1 with the duplicate transaction
		subtree1, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		_ = subtree1.AddCoinbaseNode()
		_ = subtree1.AddSubtreeNode(subtreepkg.Node{
			Hash:        duplicateTxHash,
			Fee:         100,
			SizeInBytes: 250,
		})
		_ = subtree1.AddSubtreeNode(subtreepkg.Node{
			Hash:        uniqueTx1Hash,
			Fee:         200,
			SizeInBytes: 250,
		})

		// Create subtree 2 with the SAME duplicate transaction
		subtree2, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		_ = subtree2.AddCoinbaseNode()
		_ = subtree2.AddSubtreeNode(subtreepkg.Node{
			Hash:        duplicateTxHash, // DUPLICATE!
			Fee:         100,
			SizeInBytes: 250,
		})
		_ = subtree2.AddSubtreeNode(subtreepkg.Node{
			Hash:        uniqueTx2Hash,
			Fee:         300,
			SizeInBytes: 250,
		})

		// Create subtree 3 with the SAME duplicate transaction again
		subtree3, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		_ = subtree3.AddCoinbaseNode()
		_ = subtree3.AddSubtreeNode(subtreepkg.Node{
			Hash:        duplicateTxHash, // DUPLICATE AGAIN!
			Fee:         100,
			SizeInBytes: 250,
		})

		// Create currentTxMap with all the transactions
		currentTxMap := txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
		parentHash := createTestHash("parent")
		currentTxMap.Set(duplicateTxHash, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}})
		currentTxMap.Set(uniqueTx1Hash, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}})
		currentTxMap.Set(uniqueTx2Hash, subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}})

		// Create a mock block
		mockBlock := &model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock: genesisHeader.Hash(),
				Version:       1,
				Timestamp:     2000000001,
			},
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{},
		}

		// Create current subtree
		currentSubtree, _ := subtreepkg.NewTreeByLeafCount(4)
		_ = currentSubtree.AddCoinbaseNode()

		// Process the block with duplicates - this simulates our own block being processed during reorg
		err = stp.processOwnBlockNodes(
			context.Background(),
			mockBlock,
			[]*subtreepkg.Subtree{subtree1, subtree2, subtree3},
			currentSubtree,
			currentTxMap,
			true, // skipNotification
		)
		require.NoError(t, err)

		// Check how many times the duplicate transaction appears
		count := 0
		for _, subtree := range stp.chainedSubtrees {
			for _, node := range subtree.Nodes {
				if node.Hash.Equal(duplicateTxHash) {
					count++
				}
			}
		}
		if currentSt := stp.currentSubtree.Load(); currentSt != nil {
			for _, node := range currentSt.Nodes {
				if node.Hash.Equal(duplicateTxHash) {
					count++
				}
			}
		}

		if count > 1 {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears %d times after processOwnBlockNodes (expected 1)", duplicateTxHash.String()[:16], count)
		} else {
			t.Logf("Transaction correctly appears %d time after processing block with duplicates", count)
		}

		// Also use the standard check
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("assertNoDuplicateTransactions found duplicate: %s", dup.String()[:16])
		}
	})
}

// TestDuplicateTx_LargeReorgTriggersReset tests that large reorgs (>= CoinbaseMaturity) use reset path
func TestDuplicateTx_LargeReorgTriggersReset(t *testing.T) {
	t.Run("reorg_exceeds_coinbase_maturity", func(t *testing.T) {
		// This test simulates what happens when the reorg is large enough to trigger full reset
		// CoinbaseMaturity is typically 100 blocks

		stp, blobStore, _ := setupDuplicateTestProcessor(t, 8)

		genesisHeader := createTestBlockHeader(2100000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Create a shared transaction that will be involved in the reorg
		sharedTxHash := createTestHash("large_reorg_tx")

		// Add the transaction
		stp.Add(subtreepkg.Node{
			Hash:        sharedTxHash,
			Fee:         100,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		time.Sleep(50 * time.Millisecond)

		// Create multiple blocks for a larger reorg scenario
		numBlocks := 5 // Not actually CoinbaseMaturity, but tests the pattern
		moveBackBlocks := make([]*model.Block, numBlocks)
		moveForwardBlocks := make([]*model.Block, numBlocks)

		prevHeader := genesisHeader
		for i := 0; i < numBlocks; i++ {
			// MoveBack blocks
			subtree1, _ := subtreepkg.NewTreeByLeafCount(8)
			_ = subtree1.AddCoinbaseNode()
			_ = subtree1.AddSubtreeNode(subtreepkg.Node{
				Hash:        sharedTxHash, // Same tx in all blocks
				Fee:         100,
				SizeInBytes: 250,
			})
			moveBackBlocks[i] = createBlockWithSubtrees(t, blobStore, i+1, prevHeader, []*subtreepkg.Subtree{subtree1})

			// MoveForward blocks
			subtree2, _ := subtreepkg.NewTreeByLeafCount(8)
			_ = subtree2.AddCoinbaseNode()
			_ = subtree2.AddSubtreeNode(subtreepkg.Node{
				Hash:        sharedTxHash, // Same tx in all blocks
				Fee:         100,
				SizeInBytes: 250,
			})
			moveForwardBlocks[i] = createBlockWithSubtrees(t, blobStore, i+1, prevHeader, []*subtreepkg.Subtree{subtree2})

			prevHeader = moveBackBlocks[i].Header
		}

		// Perform reorg with multiple blocks
		err := stp.Reorg(moveBackBlocks, moveForwardBlocks)
		if err != nil {
			t.Logf("Reorg returned error (may be expected for large reorg): %v", err)
		}

		// Check for duplicates
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees after large reorg", dup.String()[:16])
		} else {
			t.Log("No duplicates found after large reorg scenario")
		}
	})
}

// TestDuplicateTx_ErrorRecoveryPath tests state consistency after error during reorg
func TestDuplicateTx_ErrorRecoveryPath(t *testing.T) {
	t.Run("state_after_failed_reorg", func(t *testing.T) {
		stp, blobStore, _ := setupDuplicateTestProcessor(t, 4)

		txHash := createTestHash("error_recovery_tx")

		genesisHeader := createTestBlockHeader(2200000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Add transaction
		stp.Add(subtreepkg.Node{
			Hash:        txHash,
			Fee:         100,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		time.Sleep(50 * time.Millisecond)

		// Capture state before failed reorg
		stateBefore := countTotalTransactions(stp)

		// Create valid moveBack block
		subtree1, _ := subtreepkg.NewTreeByLeafCount(4)
		_ = subtree1.AddCoinbaseNode()
		_ = subtree1.AddSubtreeNode(subtreepkg.Node{
			Hash:        txHash,
			Fee:         100,
			SizeInBytes: 250,
		})
		moveBackBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{subtree1})

		// Create invalid moveForward block (header doesn't chain properly)
		badPrevHash := createTestHash("nonexistent_block")
		badHeader := &model.BlockHeader{
			Version:       1,
			HashPrevBlock: &badPrevHash, // Wrong prev hash
			Timestamp:     2200000002,
			Nonce:         999,
		}
		moveForwardBlock := &model.Block{
			Header:     badHeader,
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{},
			Height:     1,
		}

		// Attempt reorg - should fail due to header mismatch
		err := stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{moveForwardBlock})

		// Check state after failed reorg
		stateAfter := countTotalTransactions(stp)

		t.Logf("State before: %d transactions, after: %d transactions, error: %v", stateBefore, stateAfter, err)

		// The key check: no duplicates should exist regardless of success/failure
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees after error recovery", dup.String()[:16])
		} else {
			t.Log("No duplicates found after error recovery path")
		}
	})
}

// TestDuplicateTx_IncompleteLastSubtree tests edge case with incomplete last subtree
func TestDuplicateTx_IncompleteLastSubtree(t *testing.T) {
	t.Run("duplicate_in_incomplete_subtree", func(t *testing.T) {
		stp, _, _ := setupDuplicateTestProcessor(t, 4)

		genesisHeader := createTestBlockHeader(2300000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Create transactions to fill exactly 1.5 subtrees (6 transactions for subtree size 4)
		// This creates: [coinbase, tx1, tx2, tx3] [coinbase, tx4, tx5] (incomplete)
		txHashes := make([]chainhash.Hash, 6)
		for i := 0; i < 6; i++ {
			txHashes[i] = createTestHash(fmt.Sprintf("incomplete_tx_%d", i))
		}

		// Add transactions
		for i := 0; i < 6; i++ {
			stp.Add(subtreepkg.Node{
				Hash:        txHashes[i],
				Fee:         uint64(100 + i),
				SizeInBytes: 250,
			}, subtreepkg.TxInpoints{})
		}

		time.Sleep(100 * time.Millisecond)

		// Try to add a duplicate of one of the earlier transactions
		stp.Add(subtreepkg.Node{
			Hash:        txHashes[0], // Try to duplicate first tx
			Fee:         100,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		stp.Add(subtreepkg.Node{
			Hash:        txHashes[4], // Try to duplicate tx in incomplete subtree
			Fee:         104,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		time.Sleep(50 * time.Millisecond)

		// Check for duplicates
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Transaction %s appears in multiple subtrees with incomplete last subtree", dup.String()[:16])
		} else {
			t.Log("No duplicates found with incomplete last subtree")
		}
	})
}

// TestDuplicateTx_MoveBackBlockWithDuplicatesInSubtrees tests moveBack with a block that has duplicates across its subtrees
func TestDuplicateTx_MoveBackBlockWithDuplicatesInSubtrees(t *testing.T) {
	t.Run("moveback_block_has_internal_duplicates", func(t *testing.T) {
		stp, blobStore, _ := setupDuplicateTestProcessor(t, 4)

		duplicateTxHash := createTestHash("internal_dup_tx")
		uniqueTxHash := createTestHash("internal_unique_tx")

		genesisHeader := createTestBlockHeader(2400000000, 0)
		stp.InitCurrentBlockHeader(genesisHeader)

		// Create a moveBack block that ALREADY has the same TX in multiple subtrees
		// This simulates receiving a malformed block from the network
		subtree1, _ := subtreepkg.NewTreeByLeafCount(4)
		_ = subtree1.AddCoinbaseNode()
		_ = subtree1.AddSubtreeNode(subtreepkg.Node{
			Hash:        duplicateTxHash,
			Fee:         100,
			SizeInBytes: 250,
		})

		subtree2, _ := subtreepkg.NewTreeByLeafCount(4)
		_ = subtree2.AddCoinbaseNode()
		_ = subtree2.AddSubtreeNode(subtreepkg.Node{
			Hash:        duplicateTxHash, // Same TX in second subtree!
			Fee:         100,
			SizeInBytes: 250,
		})

		moveBackBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{subtree1, subtree2})

		// Create a clean moveForward block
		subtree3, _ := subtreepkg.NewTreeByLeafCount(4)
		_ = subtree3.AddCoinbaseNode()
		_ = subtree3.AddSubtreeNode(subtreepkg.Node{
			Hash:        uniqueTxHash,
			Fee:         200,
			SizeInBytes: 250,
		})

		moveForwardBlock := createBlockWithSubtrees(t, blobStore, 1, genesisHeader, []*subtreepkg.Subtree{subtree3})

		// Add the duplicate tx to the processor first
		stp.Add(subtreepkg.Node{
			Hash:        duplicateTxHash,
			Fee:         100,
			SizeInBytes: 250,
		}, subtreepkg.TxInpoints{})

		time.Sleep(50 * time.Millisecond)

		// Perform reorg with the malformed moveBack block
		err := stp.Reorg([]*model.Block{moveBackBlock}, []*model.Block{moveForwardBlock})
		if err != nil {
			t.Logf("Reorg returned error: %v", err)
		}

		// Check if we ended up with duplicates
		if dup := assertNoDuplicateTransactions(t, stp); dup != nil {
			t.Errorf("DUPLICATE BUG REPRODUCED: Processing moveBack block with internal duplicates caused duplicate %s in processor", dup.String()[:16])
		} else {
			t.Log("No duplicates found after processing moveBack block with internal duplicates")
		}
	})
}