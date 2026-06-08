package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// reorgBenchState holds all the state needed for a reorg benchmark run.
// Reusable across iterations to avoid setup cost in b.N loops.
type reorgBenchState struct {
	stp              *SubtreeProcessor
	subtreeStore     *blob_memory.Memory
	utxoStore        *sql.Store
	blockchainClient *blockchain.Mock
	newSubtreeChan   chan NewSubtreeRequest

	// Block being moved back (the "losing" block)
	moveBackBlock *model.Block

	// Block being moved forward (the "winning" block)
	moveForwardBlock *model.Block

	// Transactions in the mempool (not in either block)
	mempoolNodes    []subtreepkg.Node
	mempoolParents  []*subtreepkg.TxInpoints
	subtreeSize     int
	mempoolTxCount  int
	blockTxCount    int
	overlapTxCount  int
	overlapFraction float64
}

// setupReorgBenchmark creates a fully initialized benchmark scenario:
//
//   - A "losing" block with blockTxCount transactions across subtrees, stored in blob store
//   - A "winning" block with blockTxCount transactions (overlapFraction shared with losing block)
//   - A mempool with mempoolTxCount transactions already in the subtree processor
//
// The benchmark measures the full reorg path: moveBackBlock + moveForwardBlock.
func setupReorgBenchmark(b *testing.B, blockTxCount, mempoolTxCount int, subtreeSize int, overlapFraction float64) *reorgBenchState {
	b.Helper()

	ctx := context.Background()

	// Create stores
	subtreeStore := blob_memory.New()

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(b, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(b, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1024)

	done := make(chan struct{})
	b.Cleanup(func() {
		close(done)
	})

	// Drain subtree channel in background
	go func() {
		for {
			select {
			case req, ok := <-newSubtreeChan:
				if !ok {
					return
				}
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBlockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.AnythingOfType("*chainhash.Hash"), mock.AnythingOfType("[]bool")).Return(nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, subtreeStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(b, err)

	// Generate all transaction hashes upfront
	overlapCount := int(float64(blockTxCount) * overlapFraction)

	// Shared transactions (in both the losing and winning blocks)
	sharedTxHashes := make([]chainhash.Hash, overlapCount)
	for i := range sharedTxHashes {
		sharedTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("shared-tx-%d", i)))
	}

	// Losing block only transactions
	losingOnlyCount := blockTxCount - overlapCount
	losingOnlyTxHashes := make([]chainhash.Hash, losingOnlyCount)
	for i := range losingOnlyTxHashes {
		losingOnlyTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("losing-tx-%d", i)))
	}

	// Winning block only transactions
	winningOnlyCount := blockTxCount - overlapCount
	winningOnlyTxHashes := make([]chainhash.Hash, winningOnlyCount)
	for i := range winningOnlyTxHashes {
		winningOnlyTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("winning-tx-%d", i)))
	}

	// Mempool transactions (remainder after reorg)
	mempoolTxHashes := make([]chainhash.Hash, mempoolTxCount)
	for i := range mempoolTxHashes {
		mempoolTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("mempool-tx-%d", i)))
	}

	// Build the losing block's subtrees and store them
	losingBlockTxHashes := append(sharedTxHashes, losingOnlyTxHashes...)
	losingSubtreeHashes := buildAndStoreSubtrees(b, subtreeStore, losingBlockTxHashes, subtreeSize)

	// Build the winning block's subtrees and store them
	winningBlockTxHashes := append(sharedTxHashes, winningOnlyTxHashes...)
	winningSubtreeHashes := buildAndStoreSubtrees(b, subtreeStore, winningBlockTxHashes, subtreeSize)

	// Build block headers with proper chain linkage
	genesisHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          0,
	}

	// The losing block extends genesis
	losingBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567891,
		Bits:           model.NBit{},
		Nonce:          1,
	}

	// The winning block also extends genesis (fork)
	winningBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567892,
		Bits:           model.NBit{},
		Nonce:          2,
	}

	// Mock the GetBlockHeader call for the parent of moveBack block
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, genesisHeader.Hash()).Return(genesisHeader, &model.BlockHeaderMeta{}, nil)

	// Store coinbase UTXO
	_, err = utxoStore.Create(ctx, coinbaseTx, 1)
	require.NoError(b, err)

	losingBlock := &model.Block{
		Height:           1,
		ID:               1,
		CoinbaseTx:       coinbaseTx,
		Subtrees:         losingSubtreeHashes,
		Header:           losingBlockHeader,
		TransactionCount: uint64(blockTxCount),
	}

	winningBlock := &model.Block{
		Height:           1,
		ID:               2,
		CoinbaseTx:       coinbaseTx2,
		Subtrees:         winningSubtreeHashes,
		Header:           winningBlockHeader,
		TransactionCount: uint64(blockTxCount),
	}

	// Prepare mempool nodes and parents for populating the subtree processor
	mempoolNodes := make([]subtreepkg.Node, mempoolTxCount)
	mempoolParents := make([]*subtreepkg.TxInpoints, mempoolTxCount)
	parentHash := chainhash.HashH([]byte("parent-tx"))
	for i, hash := range mempoolTxHashes {
		mempoolNodes[i] = subtreepkg.Node{Hash: hash, Fee: 100, SizeInBytes: 250}
		mempoolParents[i] = &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}}
	}

	return &reorgBenchState{
		stp:              stp,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		blockchainClient: mockBlockchainClient,
		newSubtreeChan:   newSubtreeChan,
		moveBackBlock:    losingBlock,
		moveForwardBlock: winningBlock,
		mempoolNodes:     mempoolNodes,
		mempoolParents:   mempoolParents,
		subtreeSize:      subtreeSize,
		mempoolTxCount:   mempoolTxCount,
		blockTxCount:     blockTxCount,
		overlapTxCount:   overlapCount,
		overlapFraction:  overlapFraction,
	}
}

// buildAndStoreSubtrees creates subtrees from the given transaction hashes, serializes them,
// stores them in the blob store, and returns the subtree root hashes for the block.
func buildAndStoreSubtrees(b *testing.B, store *blob_memory.Memory, txHashes []chainhash.Hash, subtreeSize int) []*chainhash.Hash {
	b.Helper()
	ctx := context.Background()

	var subtreeRootHashes []*chainhash.Hash
	idx := 0

	for idx < len(txHashes) {
		// Determine how many txs go in this subtree
		remaining := len(txHashes) - idx
		count := subtreeSize
		if remaining < count {
			count = remaining
		}

		subtree, err := subtreepkg.NewTreeByLeafCount(subtreeSize)
		require.NoError(b, err)

		isFirst := len(subtreeRootHashes) == 0
		if isFirst {
			err = subtree.AddCoinbaseNode()
			require.NoError(b, err)
		}

		// The first subtree reserves slot 0 for the coinbase, so it can hold
		// one fewer real transaction. Track how many tx hashes we consume from
		// txHashes[idx:] explicitly so no hash is skipped (the coinbase occupies
		// the slot, not a tx hash).
		consumed := 0
		for i := 0; i < count; i++ {
			if isFirst && i == 0 {
				// Slot 0 is the coinbase in the first subtree; it does not
				// consume a tx hash from txHashes.
				continue
			}
			err = subtree.AddSubtreeNode(subtreepkg.Node{
				Hash:        txHashes[idx+consumed],
				Fee:         100,
				SizeInBytes: 250,
			})
			require.NoError(b, err)
			consumed++

			if subtree.IsComplete() {
				break
			}
		}

		// Serialize and store the subtree
		subtreeBytes, err := subtree.Serialize()
		require.NoError(b, err)

		rootHash := subtree.RootHash()
		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes, options.WithAllowOverwrite(true))
		require.NoError(b, err)

		// Create and store subtree meta
		subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
		parentHash := chainhash.HashH([]byte("bench-parent"))
		for j := range subtree.Nodes {
			_ = subtreeMeta.SetTxInpoints(j, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parentHash}, []uint32{0}))
		}
		metaBytes, err := subtreeMeta.Serialize()
		require.NoError(b, err)
		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeMeta, metaBytes, options.WithAllowOverwrite(true))
		require.NoError(b, err)

		hashCopy := *rootHash
		subtreeRootHashes = append(subtreeRootHashes, &hashCopy)

		// Move index forward by the number of tx hashes actually consumed.
		idx += consumed
	}

	return subtreeRootHashes
}

// populateMempool fills the subtree processor with mempool transactions
// to simulate pre-reorg state.
func populateMempool(b *testing.B, state *reorgBenchState) {
	b.Helper()

	// Set the current block header to the losing block's header
	// so moveForwardBlock's header validation passes
	state.stp.InitCurrentBlockHeader(state.moveBackBlock.Header)

	// Add mempool transactions via addNode
	for i, node := range state.mempoolNodes {
		err := state.stp.addNode(node, state.mempoolParents[i], true)
		if err != nil {
			b.Fatalf("failed to add mempool node %d: %v", i, err)
		}
	}
}

// resetProcessorState resets the subtree processor to a clean state for the next benchmark iteration.
func resetProcessorState(b *testing.B, state *reorgBenchState) {
	b.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = state.subtreeSize
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, state.subtreeStore, state.blockchainClient, state.utxoStore, state.newSubtreeChan)
	require.NoError(b, err)

	state.stp = stp
}

// BenchmarkMoveBackBlock benchmarks the moveBackBlock operation in isolation.
// This measures: blob reads + deserialization + per-node addNode calls for block txs + mempool txs.
func BenchmarkMoveBackBlock(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name           string
		blockTxCount   int
		mempoolTxCount int
		subtreeSize    int
	}{
		{"1K_block_1K_mempool", 1_000, 1_000, 1024},
		{"10K_block_10K_mempool", 10_000, 10_000, 1024},
		{"10K_block_50K_mempool", 10_000, 50_000, 4096},
		{"50K_block_50K_mempool", 50_000, 50_000, 4096},
		{"100K_block_100K_mempool", 100_000, 100_000, 8192},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			state := setupReorgBenchmark(b, bm.blockTxCount, bm.mempoolTxCount, bm.subtreeSize, 0.5)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetProcessorState(b, state)
				populateMempool(b, state)
				b.StartTimer()

				_, _, err := state.stp.moveBackBlock(context.Background(), state.moveBackBlock, false)
				if err != nil {
					b.Fatalf("moveBackBlock failed: %v", err)
				}
			}

			b.ReportMetric(float64(bm.blockTxCount+bm.mempoolTxCount), "total_txs")
			b.ReportMetric(float64(bm.blockTxCount), "block_txs")
			b.ReportMetric(float64(bm.mempoolTxCount), "mempool_txs")
		})
	}
}

// BenchmarkMoveForwardBlock benchmarks the moveForwardBlock operation in isolation.
// This measures: subtree filtering + transaction map creation + processRemainderTxHashes.
func BenchmarkMoveForwardBlock(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name           string
		blockTxCount   int
		mempoolTxCount int
		subtreeSize    int
	}{
		{"1K_block_1K_mempool", 1_000, 1_000, 1024},
		{"10K_block_10K_mempool", 10_000, 10_000, 1024},
		{"10K_block_50K_mempool", 10_000, 50_000, 4096},
		{"50K_block_50K_mempool", 50_000, 50_000, 4096},
		{"100K_block_100K_mempool", 100_000, 100_000, 8192},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			state := setupReorgBenchmark(b, bm.blockTxCount, bm.mempoolTxCount, bm.subtreeSize, 0.5)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetProcessorState(b, state)
				populateMempool(b, state)

				// First do a moveBackBlock to get into a post-moveBack state
				_, _, err := state.stp.moveBackBlock(context.Background(), state.moveBackBlock, false)
				if err != nil {
					b.Fatalf("moveBackBlock failed: %v", err)
				}

				// Set the header to genesis so moveForwardBlock can validate the winning block
				state.stp.currentBlockHeader.Store(&model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{},
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      1234567890,
					Bits:           model.NBit{},
					Nonce:          0,
				})

				b.StartTimer()

				_, _, err = state.stp.moveForwardBlock(context.Background(), state.moveForwardBlock, true, make(map[chainhash.Hash]struct{}), true, false)
				if err != nil {
					b.Fatalf("moveForwardBlock failed: %v", err)
				}
			}

			b.ReportMetric(float64(bm.blockTxCount+bm.mempoolTxCount), "total_txs")
		})
	}
}

// BenchmarkFullReorg benchmarks the complete reorg path: moveBackBlock + moveForwardBlock.
// This is the primary benchmark to track — it measures end-to-end reorg performance.
func BenchmarkFullReorg(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name            string
		blockTxCount    int
		mempoolTxCount  int
		subtreeSize     int
		overlapFraction float64
	}{
		// Small scale — fast iteration
		{"1K_block_1K_mempool_50pct_overlap", 1_000, 1_000, 1024, 0.5},

		// Medium scale — representative of small blocks
		{"10K_block_10K_mempool_50pct_overlap", 10_000, 10_000, 1024, 0.5},

		// Larger mempool than block — common production scenario
		{"10K_block_50K_mempool_50pct_overlap", 10_000, 50_000, 4096, 0.5},

		// Larger scale — starts to show contention effects
		{"50K_block_50K_mempool_50pct_overlap", 50_000, 50_000, 4096, 0.5},

		// Large scale — the target for optimization
		{"100K_block_100K_mempool_50pct_overlap", 100_000, 100_000, 8192, 0.5},

		// Overlap variation: 0% overlap (worst case — no shared txs between blocks)
		{"10K_block_10K_mempool_0pct_overlap", 10_000, 10_000, 1024, 0.0},

		// Overlap variation: 90% overlap (best case — nearly identical blocks)
		{"10K_block_10K_mempool_90pct_overlap", 10_000, 10_000, 1024, 0.9},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			state := setupReorgBenchmark(b, bm.blockTxCount, bm.mempoolTxCount, bm.subtreeSize, bm.overlapFraction)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				resetProcessorState(b, state)
				populateMempool(b, state)
				b.StartTimer()

				// Step 1: moveBackBlock
				_, _, err := state.stp.moveBackBlock(context.Background(), state.moveBackBlock, false)
				if err != nil {
					b.Fatalf("moveBackBlock failed: %v", err)
				}

				// Reset header to genesis (parent of both losing and winning blocks)
				state.stp.currentBlockHeader.Store(&model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{},
					HashMerkleRoot: &chainhash.Hash{},
					Timestamp:      1234567890,
					Bits:           model.NBit{},
					Nonce:          0,
				})

				// Step 2: moveForwardBlock
				_, _, err = state.stp.moveForwardBlock(context.Background(), state.moveForwardBlock, true, make(map[chainhash.Hash]struct{}), true, false)
				if err != nil {
					b.Fatalf("moveForwardBlock failed: %v", err)
				}
			}

			totalTxs := bm.blockTxCount + bm.mempoolTxCount
			b.ReportMetric(float64(totalTxs), "total_txs")
			b.ReportMetric(float64(bm.blockTxCount), "block_txs")
			b.ReportMetric(float64(bm.mempoolTxCount), "mempool_txs")
			b.ReportMetric(bm.overlapFraction*100, "overlap_pct")
		})
	}
}

// BenchmarkAddNodeSequential benchmarks the per-node addNode overhead in isolation.
// This isolates the mutex lock + append + IsComplete + map insert cost per transaction.
func BenchmarkAddNodeSequential(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name        string
		nodeCount   int
		subtreeSize int
	}{
		{"1K_nodes", 1_000, 1024},
		{"10K_nodes", 10_000, 4096},
		{"100K_nodes", 100_000, 8192},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := context.Background()
			tSettings := test.CreateBaseTestSettings(b)
			tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = bm.subtreeSize

			newSubtreeChan := make(chan NewSubtreeRequest, 1024)
			go func() {
				for req := range newSubtreeChan {
					if req.ErrChan != nil {
						req.ErrChan <- nil
					}
				}
			}()
			defer close(newSubtreeChan)

			// Pre-generate nodes and parents
			nodes := make([]subtreepkg.Node, bm.nodeCount)
			parents := make([]*subtreepkg.TxInpoints, bm.nodeCount)
			parentHash := chainhash.HashH([]byte("parent"))
			for i := range nodes {
				nodes[i] = subtreepkg.Node{
					Hash:        chainhash.HashH([]byte(fmt.Sprintf("seq-node-%d", i))),
					Fee:         100,
					SizeInBytes: 250,
				}
				parents[i] = &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}}
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				for j, node := range nodes {
					if err := stp.addNode(node, parents[j], true); err != nil {
						b.Fatalf("addNode failed at %d: %v", j, err)
					}
				}
			}

			b.ReportMetric(float64(bm.nodeCount), "nodes")
		})
	}
}

// BenchmarkProcessRemainderTxHashes benchmarks the remainder processing after moveForwardBlock.
// This isolates the filter + dedup + sequential addNode/addNodePreValidated cost.
func BenchmarkProcessRemainderTxHashes(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name            string
		chainedSubtrees int
		txsPerSubtree   int
		blockTxPct      float64 // fraction of txs that are in the block (filtered out)
	}{
		{"10_subtrees_1K_each_50pct_filtered", 10, 1024, 0.5},
		{"50_subtrees_1K_each_50pct_filtered", 50, 1024, 0.5},
		{"100_subtrees_1K_each_50pct_filtered", 100, 1024, 0.5},
		{"10_subtrees_4K_each_50pct_filtered", 10, 4096, 0.5},
		{"50_subtrees_4K_each_10pct_filtered", 50, 4096, 0.1},
		{"50_subtrees_4K_each_90pct_filtered", 50, 4096, 0.9},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := context.Background()
			tSettings := test.CreateBaseTestSettings(b)
			tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = bm.txsPerSubtree * 2 // large enough to not rotate

			newSubtreeChan := make(chan NewSubtreeRequest, 1024)
			go func() {
				for req := range newSubtreeChan {
					if req.ErrChan != nil {
						req.ErrChan <- nil
					}
				}
			}()
			defer close(newSubtreeChan)

			totalTxs := bm.chainedSubtrees * bm.txsPerSubtree
			blockTxCount := int(float64(totalTxs) * bm.blockTxPct)

			// Create chained subtrees
			chainedSubtrees := make([]*subtreepkg.Subtree, bm.chainedSubtrees)
			allTxHashes := make([]chainhash.Hash, 0, totalTxs)
			parentHash := chainhash.HashH([]byte("parent"))

			for s := 0; s < bm.chainedSubtrees; s++ {
				subtree, err := subtreepkg.NewTreeByLeafCount(bm.txsPerSubtree)
				require.NoError(b, err)

				if s == 0 {
					_ = subtree.AddCoinbaseNode()
				}

				for i := 0; i < bm.txsPerSubtree-1; i++ {
					txHash := chainhash.HashH([]byte(fmt.Sprintf("rem-tx-%d-%d", s, i)))
					err = subtree.AddSubtreeNode(subtreepkg.Node{Hash: txHash, Fee: 100, SizeInBytes: 250})
					require.NoError(b, err)
					allTxHashes = append(allTxHashes, txHash)

					if subtree.IsComplete() {
						break
					}
				}

				chainedSubtrees[s] = subtree
			}

			// Create block transaction map (subset of allTxHashes)
			transactionMap := NewSplitSwissMap(1024, blockTxCount)
			for i := 0; i < blockTxCount && i < len(allTxHashes); i++ {
				_ = transactionMap.Put(allTxHashes[i])
			}

			// Create currentTxMap with all transactions
			currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
			for _, hash := range allTxHashes {
				currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}})
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan)
				require.NoError(b, err)

				b.StartTimer()

				err = stp.processRemainderTxHashes(ctx, chainedSubtrees, transactionMap, nil, currentTxMap, true)
				if err != nil {
					b.Fatalf("processRemainderTxHashes failed: %v", err)
				}
			}

			b.ReportMetric(float64(totalTxs), "total_txs")
			b.ReportMetric(float64(totalTxs-blockTxCount), "remainder_txs")
			b.ReportMetric(bm.blockTxPct*100, "filtered_pct")
		})
	}
}

// BenchmarkReorgMemoryProfile runs a single reorg iteration and reports detailed memory stats.
// Not a proper benchmark (N=1), but useful for profiling memory usage.
func BenchmarkReorgMemoryProfile(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy reorg memory-profile benchmark; skipped in -short (CI passes -short)")
	}

	benchmarks := []struct {
		name           string
		blockTxCount   int
		mempoolTxCount int
		subtreeSize    int
	}{
		{"10K_block_10K_mempool", 10_000, 10_000, 1024},
		{"50K_block_50K_mempool", 50_000, 50_000, 4096},
		{"100K_block_100K_mempool", 100_000, 100_000, 8192},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			state := setupReorgBenchmark(b, bm.blockTxCount, bm.mempoolTxCount, bm.subtreeSize, 0.5)

			resetProcessorState(b, state)
			populateMempool(b, state)

			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			start := time.Now()

			// moveBackBlock
			_, _, err := state.stp.moveBackBlock(context.Background(), state.moveBackBlock, false)
			require.NoError(b, err)

			var memAfterMoveBack runtime.MemStats
			runtime.ReadMemStats(&memAfterMoveBack)

			moveBackDuration := time.Since(start)

			// Reset header to genesis
			state.stp.currentBlockHeader.Store(&model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1234567890,
				Bits:           model.NBit{},
				Nonce:          0,
			})

			moveForwardStart := time.Now()

			// moveForwardBlock
			_, _, err = state.stp.moveForwardBlock(context.Background(), state.moveForwardBlock, true, make(map[chainhash.Hash]struct{}), true, false)
			require.NoError(b, err)

			moveForwardDuration := time.Since(moveForwardStart)
			totalDuration := time.Since(start)

			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			b.ReportMetric(float64(moveBackDuration.Milliseconds()), "moveBack_ms")
			b.ReportMetric(float64(moveForwardDuration.Milliseconds()), "moveForward_ms")
			b.ReportMetric(float64(totalDuration.Milliseconds()), "total_ms")
			b.ReportMetric(float64(memAfterMoveBack.TotalAlloc-memBefore.TotalAlloc)/(1024*1024), "moveBack_alloc_MB")
			b.ReportMetric(float64(memAfter.TotalAlloc-memAfterMoveBack.TotalAlloc)/(1024*1024), "moveForward_alloc_MB")
			b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/(1024*1024), "total_alloc_MB")
			b.ReportMetric(float64(memAfter.Alloc)/(1024*1024), "heap_inuse_MB")
		})
	}
}

// TestReorgBenchmarkBaseline runs the full reorg at multiple scales and prints a summary table.
// This is a test (not benchmark) for easy "go test -run" invocation with human-readable output.
func TestReorgBenchmarkBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping baseline benchmark in short mode")
	}

	scales := []struct {
		name           string
		blockTxCount   int
		mempoolTxCount int
		subtreeSize    int
	}{
		{"1K", 1_000, 1_000, 1024},
		{"10K", 10_000, 10_000, 1024},
		{"50K", 50_000, 50_000, 4096},
		{"100K", 100_000, 100_000, 8192},
	}

	fmt.Println()
	fmt.Println("=== Reorg Baseline Performance ===")
	fmt.Printf("%-8s  %12s  %12s  %12s  %12s  %12s\n",
		"Scale", "MoveBack", "MoveForward", "Total", "Alloc(MB)", "Txs/sec")
	fmt.Println("--------  ------------  ------------  ------------  ------------  ------------")

	for _, s := range scales {
		state := setupReorgBenchmarkT(t, s.blockTxCount, s.mempoolTxCount, s.subtreeSize, 0.5)
		resetProcessorStateT(t, state)
		populateMempoolT(t, state)

		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		start := time.Now()

		_, _, err := state.stp.moveBackBlock(context.Background(), state.moveBackBlock, false)
		require.NoError(t, err)
		moveBackDuration := time.Since(start)

		state.stp.currentBlockHeader.Store(&model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{},
			Nonce:          0,
		})

		moveForwardStart := time.Now()
		_, _, err = state.stp.moveForwardBlock(context.Background(), state.moveForwardBlock, true, make(map[chainhash.Hash]struct{}), true, false)
		require.NoError(t, err)
		moveForwardDuration := time.Since(moveForwardStart)

		totalDuration := time.Since(start)

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		allocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)

		totalTxs := s.blockTxCount + s.mempoolTxCount
		txPerSec := float64(totalTxs) / totalDuration.Seconds()

		fmt.Printf("%-8s  %12s  %12s  %12s  %12.1f  %12.0f\n",
			s.name, moveBackDuration.Round(time.Millisecond), moveForwardDuration.Round(time.Millisecond),
			totalDuration.Round(time.Millisecond), allocMB, txPerSec)
	}

	fmt.Println()
}

// setupReorgBenchmark variant for *testing.T (used by TestReorgBenchmarkBaseline)
func setupReorgBenchmarkT(t *testing.T, blockTxCount, mempoolTxCount int, subtreeSize int, overlapFraction float64) *reorgBenchState {
	t.Helper()

	ctx := context.Background()
	subtreeStore := blob_memory.New()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1024)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBlockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, subtreeStore, mockBlockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	overlapCount := int(float64(blockTxCount) * overlapFraction)

	sharedTxHashes := make([]chainhash.Hash, overlapCount)
	for i := range sharedTxHashes {
		sharedTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("shared-tx-%d", i)))
	}

	losingOnlyCount := blockTxCount - overlapCount
	losingOnlyTxHashes := make([]chainhash.Hash, losingOnlyCount)
	for i := range losingOnlyTxHashes {
		losingOnlyTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("losing-tx-%d", i)))
	}

	winningOnlyCount := blockTxCount - overlapCount
	winningOnlyTxHashes := make([]chainhash.Hash, winningOnlyCount)
	for i := range winningOnlyTxHashes {
		winningOnlyTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("winning-tx-%d", i)))
	}

	mempoolTxHashes := make([]chainhash.Hash, mempoolTxCount)
	for i := range mempoolTxHashes {
		mempoolTxHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("mempool-tx-%d", i)))
	}

	losingBlockTxHashes := append(sharedTxHashes, losingOnlyTxHashes...)
	losingSubtreeHashes := buildAndStoreSubtreesT(t, subtreeStore, losingBlockTxHashes, subtreeSize)

	winningBlockTxHashes := append(sharedTxHashes, winningOnlyTxHashes...)
	winningSubtreeHashes := buildAndStoreSubtreesT(t, subtreeStore, winningBlockTxHashes, subtreeSize)

	genesisHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          0,
	}

	losingBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567891,
		Bits:           model.NBit{},
		Nonce:          1,
	}

	winningBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesisHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567892,
		Bits:           model.NBit{},
		Nonce:          2,
	}

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, genesisHeader.Hash()).Return(genesisHeader, &model.BlockHeaderMeta{}, nil)

	_, err = utxoStore.Create(ctx, coinbaseTx, 1)
	require.NoError(t, err)

	losingBlock := &model.Block{
		Height:           1,
		ID:               1,
		CoinbaseTx:       coinbaseTx,
		Subtrees:         losingSubtreeHashes,
		Header:           losingBlockHeader,
		TransactionCount: uint64(blockTxCount),
	}

	winningBlock := &model.Block{
		Height:           1,
		ID:               2,
		CoinbaseTx:       coinbaseTx2,
		Subtrees:         winningSubtreeHashes,
		Header:           winningBlockHeader,
		TransactionCount: uint64(blockTxCount),
	}

	mempoolNodes := make([]subtreepkg.Node, mempoolTxCount)
	mempoolParents := make([]*subtreepkg.TxInpoints, mempoolTxCount)
	parentHash := chainhash.HashH([]byte("parent-tx"))
	for i, hash := range mempoolTxHashes {
		mempoolNodes[i] = subtreepkg.Node{Hash: hash, Fee: 100, SizeInBytes: 250}
		mempoolParents[i] = &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}}
	}

	return &reorgBenchState{
		stp:              stp,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		blockchainClient: mockBlockchainClient,
		newSubtreeChan:   newSubtreeChan,
		moveBackBlock:    losingBlock,
		moveForwardBlock: winningBlock,
		mempoolNodes:     mempoolNodes,
		mempoolParents:   mempoolParents,
		subtreeSize:      subtreeSize,
		mempoolTxCount:   mempoolTxCount,
		blockTxCount:     blockTxCount,
		overlapTxCount:   overlapCount,
		overlapFraction:  overlapFraction,
	}
}

func buildAndStoreSubtreesT(t *testing.T, store *blob_memory.Memory, txHashes []chainhash.Hash, subtreeSize int) []*chainhash.Hash {
	t.Helper()
	ctx := context.Background()

	var subtreeRootHashes []*chainhash.Hash
	idx := 0

	for idx < len(txHashes) {
		remaining := len(txHashes) - idx
		count := subtreeSize
		if remaining < count {
			count = remaining
		}

		subtree, err := subtreepkg.NewTreeByLeafCount(subtreeSize)
		require.NoError(t, err)

		isFirst := len(subtreeRootHashes) == 0
		if isFirst {
			err = subtree.AddCoinbaseNode()
			require.NoError(t, err)
		}

		// The first subtree reserves slot 0 for the coinbase, so it can hold
		// one fewer real transaction. Track how many tx hashes we consume from
		// txHashes[idx:] explicitly so no hash is skipped.
		consumed := 0
		for i := 0; i < count; i++ {
			if isFirst && i == 0 {
				// Slot 0 is the coinbase in the first subtree; it does not
				// consume a tx hash from txHashes.
				continue
			}
			err = subtree.AddSubtreeNode(subtreepkg.Node{
				Hash:        txHashes[idx+consumed],
				Fee:         100,
				SizeInBytes: 250,
			})
			require.NoError(t, err)
			consumed++

			if subtree.IsComplete() {
				break
			}
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		rootHash := subtree.RootHash()
		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes, options.WithAllowOverwrite(true))
		require.NoError(t, err)

		subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
		parentHash := chainhash.HashH([]byte("bench-parent"))
		for j := range subtree.Nodes {
			_ = subtreeMeta.SetTxInpoints(j, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parentHash}, []uint32{0}))
		}
		metaBytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeMeta, metaBytes, options.WithAllowOverwrite(true))
		require.NoError(t, err)

		hashCopy := *rootHash
		subtreeRootHashes = append(subtreeRootHashes, &hashCopy)

		// Move index forward by the number of tx hashes actually consumed.
		idx += consumed
	}

	return subtreeRootHashes
}

func resetProcessorStateT(t *testing.T, state *reorgBenchState) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = state.subtreeSize
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, state.subtreeStore, state.blockchainClient, state.utxoStore, state.newSubtreeChan)
	require.NoError(t, err)

	state.stp = stp
}

func populateMempoolT(t *testing.T, state *reorgBenchState) {
	t.Helper()

	state.stp.InitCurrentBlockHeader(state.moveBackBlock.Header)

	for i, node := range state.mempoolNodes {
		err := state.stp.addNode(node, state.mempoolParents[i], true)
		require.NoError(t, err, "failed to add mempool node %d", i)
	}
}

// init ensures the unused import for bt is used
var _ *bt.Tx
