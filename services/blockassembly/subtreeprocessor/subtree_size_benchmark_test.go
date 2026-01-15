package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmappkg "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSubtreeProcessorSizePerformance tests the SubtreeProcessor's performance
// with different subtree sizes to understand the impact of small vs large subtrees.
// This tests the full SubtreeProcessor including queue management and notifications.
// Compare with TestDirectSubtreeCreationPerformance to isolate SubtreeProcessor overhead.
func TestSubtreeProcessorSizePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testCases := []struct {
		name            string
		txCount         int
		itemsPerSubtree int
	}{
		{name: "5k_tx_4_per_subtree", txCount: 5000, itemsPerSubtree: 4},
		{name: "5k_tx_64_per_subtree", txCount: 5000, itemsPerSubtree: 64},
		{name: "5k_tx_256_per_subtree", txCount: 5000, itemsPerSubtree: 256},
		{name: "5k_tx_1024_per_subtree", txCount: 5000, itemsPerSubtree: 1024},
		{name: "5k_tx_2048_per_subtree", txCount: 5000, itemsPerSubtree: 2048},
	}

	type result struct {
		name            string
		txCount         int
		itemsPerSubtree int
		subtreeCount    int
		addDuration     time.Duration
		txPerSecond     float64
	}
	results := make([]result, 0, len(testCases))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stp, cleanup := setupSubtreeProcessorForBench(t, tc.itemsPerSubtree)
			defer cleanup()

			// Generate real transactions using shared helper
			allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(tc.txCount))
			txHashes := make([]chainhash.Hash, len(allTxs))
			for i, tx := range allTxs {
				txHashes[i] = *tx.TxIDChainHash()
			}

			// Time adding transactions directly (bypassing queue for consistent timing)
			start := time.Now()
			for i, hash := range txHashes {
				err := stp.addNode(
					subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250},
					&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
					true, // skipNotification
				)
				require.NoError(t, err)
			}
			duration := time.Since(start)

			subtreeCount := len(stp.chainedSubtrees)
			if stp.currentSubtree.Load().Length() > 1 { // >1 because of coinbase placeholder
				subtreeCount++
			}

			txPerSec := float64(tc.txCount) / duration.Seconds()

			results = append(results, result{
				name:            tc.name,
				txCount:         tc.txCount,
				itemsPerSubtree: tc.itemsPerSubtree,
				subtreeCount:    subtreeCount,
				addDuration:     duration,
				txPerSecond:     txPerSec,
			})

			t.Logf("%s: %d subtrees, %v, %.0f tx/sec",
				tc.name, subtreeCount, duration, txPerSec)
		})
	}

	// Summary table
	t.Log("\n=== SubtreeProcessor Performance Summary ===")
	t.Log("Name                    | TxCount | ItemsPerSubtree | Subtrees | Duration    | TX/sec")
	t.Log("------------------------|---------|-----------------|----------|-------------|--------")
	for _, r := range results {
		t.Logf("%-23s | %7d | %15d | %8d | %11v | %6.0f",
			r.name, r.txCount, r.itemsPerSubtree, r.subtreeCount, r.addDuration, r.txPerSecond)
	}
}

// TestDirectSubtreeCreationPerformance tests direct subtree creation performance
// WITHOUT using the SubtreeProcessor. This isolates the raw go-subtree library performance.
// Compare with TestSubtreeProcessorSizePerformance to see SubtreeProcessor overhead.
func TestDirectSubtreeCreationPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testCases := []struct {
		name            string
		txCount         int
		itemsPerSubtree int
	}{
		{name: "5k_tx_4_per_subtree", txCount: 5000, itemsPerSubtree: 4},
		{name: "5k_tx_64_per_subtree", txCount: 5000, itemsPerSubtree: 64},
		{name: "5k_tx_256_per_subtree", txCount: 5000, itemsPerSubtree: 256},
		{name: "5k_tx_1024_per_subtree", txCount: 5000, itemsPerSubtree: 1024},
		{name: "5k_tx_2048_per_subtree", txCount: 5000, itemsPerSubtree: 2048},
	}

	type result struct {
		name            string
		txCount         int
		itemsPerSubtree int
		subtreeCount    int
		duration        time.Duration
		txPerSecond     float64
	}
	results := make([]result, 0, len(testCases))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate real transactions using shared helper
			allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(tc.txCount))
			txHashes := make([]chainhash.Hash, len(allTxs))
			for i, tx := range allTxs {
				txHashes[i] = *tx.TxIDChainHash()
			}

			// Time direct subtree creation (no SubtreeProcessor)
			start := time.Now()

			subtrees := make([]*subtreepkg.Subtree, 0)
			currentSubtree, err := subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
			require.NoError(t, err)

			// Add coinbase placeholder to first subtree
			err = currentSubtree.AddCoinbaseNode()
			require.NoError(t, err)

			for i, hash := range txHashes {
				if currentSubtree.IsComplete() {
					subtrees = append(subtrees, currentSubtree)
					currentSubtree, err = subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
					require.NoError(t, err)
				}

				err = currentSubtree.AddNode(hash, uint64(i), 250)
				require.NoError(t, err)
			}

			// Don't forget the last subtree
			if currentSubtree.Length() > 0 {
				subtrees = append(subtrees, currentSubtree)
			}

			duration := time.Since(start)
			subtreeCount := len(subtrees)
			txPerSec := float64(tc.txCount) / duration.Seconds()

			results = append(results, result{
				name:            tc.name,
				txCount:         tc.txCount,
				itemsPerSubtree: tc.itemsPerSubtree,
				subtreeCount:    subtreeCount,
				duration:        duration,
				txPerSecond:     txPerSec,
			})

			t.Logf("%s: %d subtrees, %v, %.0f tx/sec",
				tc.name, subtreeCount, duration, txPerSec)
		})
	}

	// Summary table
	t.Log("\n=== Direct Subtree Creation Performance Summary ===")
	t.Log("Name                    | TxCount | ItemsPerSubtree | Subtrees | Duration    | TX/sec")
	t.Log("------------------------|---------|-----------------|----------|-------------|--------")
	for _, r := range results {
		t.Logf("%-23s | %7d | %15d | %8d | %11v | %6.0f",
			r.name, r.txCount, r.itemsPerSubtree, r.subtreeCount, r.duration, r.txPerSecond)
	}
}

// benchmarkTxHashPool is a pre-generated pool of real transaction hashes for benchmarks.
// Using a pool allows benchmarks to use real tx hashes while supporting variable b.N.
const benchmarkTxPoolSize = 10000

var benchmarkTxHashPool []chainhash.Hash

func init() {
	// Pre-generate real transaction hashes at package init for benchmarks.
	// We use a minimal testing.T wrapper since CreateTestTransactionChainWithCount requires it.
	// Note: CreateTestTransactionChainWithCount returns count-1 transactions due to loop bounds,
	// so we request one extra to get exactly benchmarkTxPoolSize transactions.
	t := &testing.T{}
	allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(benchmarkTxPoolSize+2))
	benchmarkTxHashPool = make([]chainhash.Hash, len(allTxs))
	for i, tx := range allTxs {
		benchmarkTxHashPool[i] = *tx.TxIDChainHash()
	}
}

// BenchmarkDirectSubtreeAdd benchmarks direct subtree node addition
// WITHOUT SubtreeProcessor overhead. Compare with BenchmarkSubtreeProcessorAdd.
func BenchmarkDirectSubtreeAdd(b *testing.B) {
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "4_per_subtree", itemsPerSubtree: 4},
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "256_per_subtree", itemsPerSubtree: 256},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
		{name: "2048_per_subtree", itemsPerSubtree: 2048},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			subtrees := make([]*subtreepkg.Subtree, 0)
			currentSubtree, _ := subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
			_ = currentSubtree.AddCoinbaseNode()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if currentSubtree.IsComplete() {
					subtrees = append(subtrees, currentSubtree)
					currentSubtree, _ = subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
				}
				_ = currentSubtree.AddNode(benchmarkTxHashPool[i%benchmarkTxPoolSize], uint64(i), 250)
			}

			b.StopTimer()
			subtreeCount := len(subtrees)
			if currentSubtree.Length() > 0 {
				subtreeCount++
			}
			b.ReportMetric(float64(subtreeCount), "subtrees")
			b.ReportMetric(float64(tc.itemsPerSubtree), "items/subtree")
		})
	}
}

// BenchmarkSubtreeProcessorAdd benchmarks the SubtreeProcessor's Add performance
// with different subtree sizes. Compare with BenchmarkDirectSubtreeAdd.
func BenchmarkSubtreeProcessorAdd(b *testing.B) {
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "4_per_subtree", itemsPerSubtree: 4},
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "256_per_subtree", itemsPerSubtree: 256},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
		{name: "2048_per_subtree", itemsPerSubtree: 2048},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, tc.itemsPerSubtree)
			defer cleanup()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = stp.addNode(
					subtreepkg.Node{Hash: benchmarkTxHashPool[i%benchmarkTxPoolSize], Fee: uint64(i), SizeInBytes: 250},
					&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
					true,
				)
			}

			b.StopTimer()
			subtreeCount := len(stp.chainedSubtrees)
			b.ReportMetric(float64(subtreeCount), "subtrees")
			b.ReportMetric(float64(tc.itemsPerSubtree), "items/subtree")
		})
	}
}

// BenchmarkSubtreeProcessorRotate benchmarks the cost of subtree rotation
// (when a subtree fills up and a new one is created).
func BenchmarkSubtreeProcessorRotate(b *testing.B) {
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "4_per_subtree", itemsPerSubtree: 4},
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "256_per_subtree", itemsPerSubtree: 256},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, tc.itemsPerSubtree)
			defer cleanup()

			b.ResetTimer()
			b.ReportAllocs()

			rotations := 0
			for i := 0; i < b.N; i++ {
				prevLen := len(stp.chainedSubtrees)
				_ = stp.addNode(
					subtreepkg.Node{Hash: benchmarkTxHashPool[i%benchmarkTxPoolSize], Fee: uint64(i), SizeInBytes: 250},
					&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
					true,
				)
				if len(stp.chainedSubtrees) > prevLen {
					rotations++
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(rotations), "rotations")
		})
	}
}

func setupSubtreeProcessorForBench(t *testing.T, itemsPerSubtree int) (*SubtreeProcessor, func()) {
	t.Helper()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree
	settings.BlockAssembly.UseDynamicSubtreeSize = false // Keep fixed size for benchmarking

	newSubtreeChan := make(chan NewSubtreeRequest, 1000)
	done := make(chan struct{})

	// Drain channel to prevent blocking
	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	mockBlockchainClient := &blockchain.Mock{}

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	cleanup := func() {
		close(done)
	}

	return stp, cleanup
}

// =============================================================================
// GRANULAR OVERHEAD ISOLATION BENCHMARKS
// These benchmarks isolate individual components to identify overhead sources
// =============================================================================

// BenchmarkTxMapSetIfNotExists benchmarks the TxMap duplicate detection overhead.
// This is called for every transaction in addNode to check for duplicates.
func BenchmarkTxMapSetIfNotExists(b *testing.B) {
	txMap := txmappkg.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
	emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txMap.SetIfNotExists(benchmarkTxHashPool[i%benchmarkTxPoolSize], emptyInpoints)
	}
}

// BenchmarkTxMapSetIfNotExistsDuplicate benchmarks TxMap with duplicate checks.
// This measures the overhead when we repeatedly check for duplicates (all duplicate case).
func BenchmarkTxMapSetIfNotExistsDuplicate(b *testing.B) {
	txMap := txmappkg.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
	emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

	// Pre-populate with hashes from the pool
	for i := 0; i < 1000; i++ {
		txMap.SetIfNotExists(benchmarkTxHashPool[i], emptyInpoints)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// All lookups are duplicates
		txMap.SetIfNotExists(benchmarkTxHashPool[i%1000], emptyInpoints)
	}
}

// BenchmarkSubtreeNodeAddOnly benchmarks just the AddSubtreeNode call
// without any SubtreeProcessor overhead (no txMap, no channel send).
func BenchmarkSubtreeNodeAddOnly(b *testing.B) {
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "4_per_subtree", itemsPerSubtree: 4},
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "256_per_subtree", itemsPerSubtree: 256},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			currentSubtree, _ := subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
			_ = currentSubtree.AddCoinbaseNode()

			b.ResetTimer()
			b.ReportAllocs()

			rotations := 0
			for i := 0; i < b.N; i++ {
				if currentSubtree.IsComplete() {
					// Just create new subtree, don't track old one
					currentSubtree, _ = subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
					rotations++
				}
				_ = currentSubtree.AddSubtreeNode(subtreepkg.Node{
					Hash:        benchmarkTxHashPool[i%benchmarkTxPoolSize],
					Fee:         uint64(i),
					SizeInBytes: 250,
				})
			}

			b.StopTimer()
			b.ReportMetric(float64(rotations), "rotations")
		})
	}
}

// BenchmarkSubtreeCreationOnly benchmarks the cost of creating new subtrees.
// This isolates the NewTreeByLeafCount overhead that occurs on rotation.
func BenchmarkSubtreeCreationOnly(b *testing.B) {
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "4_per_subtree", itemsPerSubtree: 4},
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "256_per_subtree", itemsPerSubtree: 256},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
		{name: "2048_per_subtree", itemsPerSubtree: 2048},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _ = subtreepkg.NewTreeByLeafCount(tc.itemsPerSubtree)
			}
		})
	}
}

// BenchmarkChannelSendReceive benchmarks the synchronous channel send/receive
// that occurs in processCompleteSubtree.
func BenchmarkChannelSendReceive(b *testing.B) {
	ch := make(chan NewSubtreeRequest, 100)
	done := make(chan struct{})

	// Goroutine to drain channel
	go func() {
		for {
			select {
			case req := <-ch:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtree, _ := subtreepkg.NewTreeByLeafCount(4)
	txMap := NewSplitTxInpointsMap(1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		errCh := make(chan error)
		ch <- NewSubtreeRequest{
			Subtree:          subtree,
			ParentTxMap:      txMap,
			SkipNotification: true,
			ErrChan:          errCh,
		}
		<-errCh
	}

	b.StopTimer()
	close(done)
}

// BenchmarkSubtreeProcessorOverheadBreakdown provides a detailed breakdown
// of where time is spent in addNode by measuring each component.
func BenchmarkSubtreeProcessorOverheadBreakdown(b *testing.B) {
	// This test runs all components together but with detailed metrics
	testCases := []struct {
		name            string
		itemsPerSubtree int
	}{
		{name: "64_per_subtree", itemsPerSubtree: 64},
		{name: "1024_per_subtree", itemsPerSubtree: 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, tc.itemsPerSubtree)
			defer cleanup()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = stp.addNode(
					subtreepkg.Node{Hash: benchmarkTxHashPool[i%benchmarkTxPoolSize], Fee: uint64(i), SizeInBytes: 250},
					&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
					true,
				)
			}

			b.StopTimer()
			subtreeCount := len(stp.chainedSubtrees)
			if currentSubtree := stp.currentSubtree.Load(); currentSubtree != nil && currentSubtree.Length() > 0 {
				subtreeCount++
			}
			rotations := len(stp.chainedSubtrees)

			b.ReportMetric(float64(subtreeCount), "subtrees")
			b.ReportMetric(float64(rotations), "rotations")
			// Report rotations per 1000 ops to see frequency
			if b.N > 0 {
				b.ReportMetric(float64(rotations)*1000/float64(b.N), "rotations/1k_ops")
			}
		})
	}
}

// TestOverheadBreakdownDetailed runs a detailed timing analysis showing
// exactly where the overhead comes from in SubtreeProcessor vs direct creation.
func TestOverheadBreakdownDetailed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping detailed overhead test in short mode")
	}

	const txCount = 10000
	itemsPerSubtree := 64

	// Generate real transactions using shared helper
	// Note: CreateTestTransactionChainWithCount returns count-1 transactions
	allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(txCount+2))
	hashes := make([]chainhash.Hash, len(allTxs))
	for i, tx := range allTxs {
		hashes[i] = *tx.TxIDChainHash()
	}
	actualTxCount := len(hashes)

	// Measure 1: Raw AddNode only (no completion, no txmap)
	t.Run("1_RawAddNode", func(t *testing.T) {
		// Use a large power-of-2 size that won't complete during the test
		subtree, err := subtreepkg.NewTreeByLeafCount(16384)
		require.NoError(t, err)
		err = subtree.AddCoinbaseNode()
		require.NoError(t, err)

		start := time.Now()
		for i := 0; i < actualTxCount; i++ {
			_ = subtree.AddSubtreeNode(subtreepkg.Node{
				Hash:        hashes[i],
				Fee:         uint64(i),
				SizeInBytes: 250,
			})
		}
		duration := time.Since(start)
		t.Logf("Raw AddSubtreeNode only: %v (%.0f tx/sec)", duration, float64(actualTxCount)/duration.Seconds())
	})

	// Measure 2: AddNode with subtree rotation (no txmap)
	t.Run("2_AddNodeWithRotation", func(t *testing.T) {
		currentSubtree, err := subtreepkg.NewTreeByLeafCount(itemsPerSubtree)
		require.NoError(t, err)
		err = currentSubtree.AddCoinbaseNode()
		require.NoError(t, err)
		subtrees := make([]*subtreepkg.Subtree, 0)

		start := time.Now()
		for i := 0; i < actualTxCount; i++ {
			if currentSubtree.IsComplete() {
				subtrees = append(subtrees, currentSubtree)
				currentSubtree, _ = subtreepkg.NewTreeByLeafCount(itemsPerSubtree)
			}
			_ = currentSubtree.AddSubtreeNode(subtreepkg.Node{
				Hash:        hashes[i],
				Fee:         uint64(i),
				SizeInBytes: 250,
			})
		}
		duration := time.Since(start)
		t.Logf("AddNode + rotation (%d subtrees): %v (%.0f tx/sec)",
			len(subtrees), duration, float64(actualTxCount)/duration.Seconds())
	})

	// Measure 3: TxMap only
	t.Run("3_TxMapOnly", func(t *testing.T) {
		txMap := txmappkg.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
		emptyInpoints := subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}}

		start := time.Now()
		for i := 0; i < actualTxCount; i++ {
			txMap.SetIfNotExists(hashes[i], emptyInpoints)
		}
		duration := time.Since(start)
		t.Logf("TxMap SetIfNotExists only: %v (%.0f tx/sec)", duration, float64(actualTxCount)/duration.Seconds())
	})

	// Measure 4: Full SubtreeProcessor (without channel wait)
	t.Run("4_SubtreeProcessorNoChannelWait", func(t *testing.T) {
		// Create SubtreeProcessor but with buffered channel that's pre-drained
		stp, cleanup := setupSubtreeProcessorForBench(t, itemsPerSubtree)
		defer cleanup()

		start := time.Now()
		for i := 0; i < actualTxCount; i++ {
			_ = stp.addNode(
				subtreepkg.Node{Hash: hashes[i], Fee: uint64(i), SizeInBytes: 250},
				&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
				true,
			)
		}
		duration := time.Since(start)
		t.Logf("SubtreeProcessor addNode (%d subtrees): %v (%.0f tx/sec)",
			len(stp.chainedSubtrees), duration, float64(actualTxCount)/duration.Seconds())
	})

	// Summary
	t.Log("\n=== Overhead Breakdown Summary ===")
	t.Log("Compare the results above to identify where the overhead comes from:")
	t.Log("- If 1 vs 2 differ significantly: rotation overhead")
	t.Log("- If 3 is significant: TxMap duplicate detection overhead")
	t.Log("- If 4 > (2 + 3): channel/coordination overhead")
}

// =============================================================================
// PARALLEL PROCESSING BENCHMARKS
// These benchmarks test the parallelGetAndSetIfNotExists optimization
// =============================================================================

// BenchmarkParallelGetAndSetIfNotExists benchmarks the parallel processing
// optimization at various node counts.
func BenchmarkParallelGetAndSetIfNotExists(b *testing.B) {
	testCases := []struct {
		name      string
		nodeCount int
	}{
		{name: "1k_nodes", nodeCount: 1000},
		{name: "10k_nodes", nodeCount: 10000},
		{name: "50k_nodes", nodeCount: 50000},
		{name: "100k_nodes", nodeCount: 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, 1024)
			defer cleanup()

			// Pre-generate nodes and currentTxMap
			nodes := make([]subtreepkg.Node, tc.nodeCount)
			currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
			for i := 0; i < tc.nodeCount; i++ {
				hash := benchmarkTxHashPool[i%benchmarkTxPoolSize]
				nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
				currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Reset stp.currentTxMap for each iteration
				stp.currentTxMap = NewSplitTxInpointsMap(splitMapBuckets)
				wasInserted := make([]bool, len(nodes))

				_ = stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0,
					stp.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency, wasInserted)
			}

			b.StopTimer()
			b.ReportMetric(float64(tc.nodeCount), "nodes")
		})
	}
}

// BenchmarkSequentialGetAndSetIfNotExists benchmarks the sequential path
// for comparison with parallel processing.
func BenchmarkSequentialGetAndSetIfNotExists(b *testing.B) {
	testCases := []struct {
		name      string
		nodeCount int
	}{
		{name: "1k_nodes", nodeCount: 1000},
		{name: "10k_nodes", nodeCount: 10000},
		{name: "50k_nodes", nodeCount: 50000},
		{name: "100k_nodes", nodeCount: 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, 1024)
			defer cleanup()

			// Pre-generate nodes and currentTxMap
			nodes := make([]subtreepkg.Node, tc.nodeCount)
			currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
			for i := 0; i < tc.nodeCount; i++ {
				hash := benchmarkTxHashPool[i%benchmarkTxPoolSize]
				nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
				currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Reset stp.currentTxMap for each iteration
				stp.currentTxMap = NewSplitTxInpointsMap(splitMapBuckets)

				// Sequential path (original implementation)
				for _, node := range nodes {
					nodeParents, found := currentTxMap.Get(node.Hash)
					if !found {
						continue
					}
					stp.currentTxMap.SetIfNotExists(node.Hash, nodeParents)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(tc.nodeCount), "nodes")
		})
	}
}

// BenchmarkProcessOwnBlockSubtreeNodesParallel benchmarks the full processOwnBlockSubtreeNodes
// function with parallel processing enabled.
func BenchmarkProcessOwnBlockSubtreeNodesParallel(b *testing.B) {
	testCases := []struct {
		name      string
		nodeCount int
	}{
		{name: "1k_nodes", nodeCount: 1000},
		{name: "10k_nodes", nodeCount: 10000},
		{name: "100k_nodes", nodeCount: 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, 1024)
			defer cleanup()

			// Enable parallel path
			stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 100

			// Pre-generate nodes and currentTxMap
			nodes := make([]subtreepkg.Node, tc.nodeCount)
			currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
			for i := 0; i < tc.nodeCount; i++ {
				hash := benchmarkTxHashPool[i%benchmarkTxPoolSize]
				nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
				currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			}

			block := &model.Block{
				Header: blockHeader,
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Reset state for each iteration
				stp.currentTxMap = NewSplitTxInpointsMap(splitMapBuckets)
				stp.currentSubtree.Store(nil)
				stp.chainedSubtrees = nil

				_ = stp.processOwnBlockSubtreeNodes(block, nodes, currentTxMap, 0, nil, true)
			}

			b.StopTimer()
			b.ReportMetric(float64(tc.nodeCount), "nodes")
		})
	}
}

// BenchmarkProcessOwnBlockSubtreeNodesSequential benchmarks the full processOwnBlockSubtreeNodes
// function with sequential processing (high threshold to disable parallel).
func BenchmarkProcessOwnBlockSubtreeNodesSequential(b *testing.B) {
	testCases := []struct {
		name      string
		nodeCount int
	}{
		{name: "1k_nodes", nodeCount: 1000},
		{name: "10k_nodes", nodeCount: 10000},
		{name: "100k_nodes", nodeCount: 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			stp, cleanup := setupSubtreeProcessorForBenchB(b, 1024)
			defer cleanup()

			// Disable parallel path with high threshold
			stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 10_000_000

			// Pre-generate nodes and currentTxMap
			nodes := make([]subtreepkg.Node, tc.nodeCount)
			currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
			for i := 0; i < tc.nodeCount; i++ {
				hash := benchmarkTxHashPool[i%benchmarkTxPoolSize]
				nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
				currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
			}

			block := &model.Block{
				Header: blockHeader,
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Reset state for each iteration
				stp.currentTxMap = NewSplitTxInpointsMap(splitMapBuckets)
				stp.currentSubtree.Store(nil)
				stp.chainedSubtrees = nil

				_ = stp.processOwnBlockSubtreeNodes(block, nodes, currentTxMap, 0, nil, true)
			}

			b.StopTimer()
			b.ReportMetric(float64(tc.nodeCount), "nodes")
		})
	}
}

func setupSubtreeProcessorForBenchB(b *testing.B, itemsPerSubtree int) (*SubtreeProcessor, func()) {
	b.Helper()

	// Use a fake testing.T for settings creation
	t := &testing.T{}
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree
	settings.BlockAssembly.UseDynamicSubtreeSize = false

	newSubtreeChan := make(chan NewSubtreeRequest, 1000)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	subtreeStore := blob_memory.New()
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	utxoStoreURL, _ := url.Parse("sqlitememory:///test")
	utxoStore, _ := sql.New(ctx, logger, settings, utxoStoreURL)

	mockBlockchainClient := &blockchain.Mock{}

	stp, _ := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		subtreeStore,
		mockBlockchainClient,
		utxoStore,
		newSubtreeChan,
	)

	cleanup := func() {
		close(done)
	}

	return stp, cleanup
}
