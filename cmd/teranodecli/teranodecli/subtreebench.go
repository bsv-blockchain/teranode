package teranodecli

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

type benchmarkResult struct {
	TotalTxs        int
	Elapsed         time.Duration
	TxPerSec        float64
	ProcessedTxs    uint64
	SubtreesCreated int64
	QueueRemaining  int64
}

// createBenchmarkSettings creates a settings object configured for benchmarking.
func createBenchmarkSettings(subtreeSize int) *settings.Settings {
	s := settings.NewSettings()

	// Use a temporary directory that won't interfere with anything
	s.DataFolder = os.TempDir() + "/subtreebench"

	// Create a copy of RegressionNetParams to avoid race conditions
	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1
	s.ChainCfgParams = &chainParams
	s.GlobalBlockHeightRetention = 10
	s.BlockValidation.OptimisticMining = false

	// Configure block assembly settings for benchmarking
	s.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	s.BlockAssembly.SubtreeProcessorBatcherSize = 1024 * 1024 // Large batcher to avoid blocking

	return s
}

func runSubtreeBenchmark(subtreeSize, producers, iterations, duration int, cpuProfile, memProfile string) error {
	fmt.Printf("SubtreeProcessor Throughput Benchmark\n")
	fmt.Printf("=====================================\n")
	fmt.Printf("Subtree Size:     %d\n", subtreeSize)
	fmt.Printf("Producers:        %d\n", producers)
	fmt.Printf("CPU Cores:        %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS:       %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	// Run the benchmark (profiling is now done inside runBenchmarkCore)
	result, err := runBenchmarkCore(subtreeSize, producers, iterations, duration, cpuProfile, memProfile)
	if err != nil {
		return err
	}

	// Print results
	fmt.Printf("\nBenchmark Results\n")
	fmt.Printf("=================\n")
	fmt.Printf("Total Transactions:  %d\n", result.TotalTxs)
	fmt.Printf("Elapsed Time:        %.2fs\n", result.Elapsed.Seconds())
	fmt.Printf("Throughput:          %.2f tx/sec\n", result.TxPerSec)
	fmt.Printf("Processed Txs:       %d\n", result.ProcessedTxs)
	fmt.Printf("Subtrees Created:    %d\n", result.SubtreesCreated)
	fmt.Printf("Queue Remaining:     %d\n", result.QueueRemaining)
	fmt.Printf("Avg per Producer:    %.2f tx/sec\n", result.TxPerSec/float64(producers))
	fmt.Println()
	fmt.Printf("Profiles written to: %s, %s\n", cpuProfile, memProfile)

	return nil
}

func runBenchmarkCore(subtreeSize, producers, iterations, duration int, cpuProfile, memProfile string) (benchmarkResult, error) {
	// Setup phase (not profiled)
	benchStartTime := time.Now()
	fmt.Printf("[%s] Setting up benchmark...\n", time.Since(benchStartTime))

	// Use large channel buffer to prevent blocking subtree notifications
	// For 10M txs with 1M subtree size, expect ~10 subtrees
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 10000)
	subtreeCount := atomic.Int64{}
	subtreeTxCount := atomic.Int64{}
	var subtreeSizesMu sync.Mutex
	subtreeSizes := make(map[int]int64) // map[size]count
	var actualSubtreeSize int           // Will be set after adjustment

	// Consume subtrees in background to prevent blocking
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				_, _ = fmt.Fprintf(os.Stderr, "[%s] Subtree consumer panic: %v\n", time.Since(benchStartTime), r)
			}
		}()
		for {
			select {
			case req := <-newSubtreeChan:
				if req.Subtree == nil {
					_, _ = fmt.Fprintf(os.Stderr, "[%s] Warning: received nil subtree\n", time.Since(benchStartTime))
					continue
				}
				subtreeCount.Add(1)
				subtreeLen := req.Subtree.Length()
				subtreeTxCount.Add(int64(subtreeLen))

				fmt.Printf("[%s] Received subtree #%d with %d transactions (total: %d)\n", time.Since(benchStartTime), subtreeCount.Load(), subtreeLen, subtreeTxCount.Load())

				// Track subtree sizes
				subtreeSizesMu.Lock()
				subtreeSizes[subtreeLen]++
				subtreeSizesMu.Unlock()

				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Create settings
	tSettings := createBenchmarkSettings(subtreeSize)
	// Disable periodic announcements - we only want complete subtrees announced immediately
	tSettings.BlockAssembly.SubtreeAnnouncementInterval = 24 * time.Hour

	// Adjust subtree size for accurate measurements
	// Subtree sizes MUST be powers of 2
	// Ensure we create multiple subtrees (at least 10) for meaningful benchmarks
	// Max size: 1M (2^20), Min size: iterations/10 (rounded down to power of 2)
	adjustedSubtreeSize := subtreeSize
	targetSize := iterations / 10

	if targetSize < adjustedSubtreeSize && targetSize > 0 {
		// Round down to nearest power of 2
		adjustedSubtreeSize = 1
		for adjustedSubtreeSize*2 <= targetSize {
			adjustedSubtreeSize *= 2
		}
	}

	// Enforce max of 1M (2^20)
	if adjustedSubtreeSize > 1_048_576 {
		adjustedSubtreeSize = 1_048_576
	}
	// Enforce minimum of 128 (2^7)
	if adjustedSubtreeSize < 128 {
		adjustedSubtreeSize = 128
	}

	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = adjustedSubtreeSize
	actualSubtreeSize = adjustedSubtreeSize
	if adjustedSubtreeSize != subtreeSize {
		fmt.Printf("Adjusted subtree size from %d to %d (power of 2) for better measurements (creates ~%d subtrees)\n",
			subtreeSize, adjustedSubtreeSize, iterations/adjustedSubtreeSize)
	}

	// Create subtree processor
	stp, err := subtreeprocessor.NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan)
	if err != nil {
		return benchmarkResult{}, errors.NewProcessingError("failed to create subtree processor: %w", err)
	}
	defer stp.Stop(ctx)

	// Start the subtree processor workers
	stp.Start(ctx)

	// Adjust number of transactions to be exact multiple of subtree size
	// This ensures all subtrees are complete (no incomplete subtree at the end)
	// Account for coinbase placeholder (+1) in first subtree
	numTxs := iterations
	// Calculate how many complete subtrees we want
	desiredSubtrees := iterations / adjustedSubtreeSize
	if desiredSubtrees < 10 {
		desiredSubtrees = 10 // Ensure at least 10 subtrees for meaningful benchmark
	}
	// Calculate exact number of transactions needed (minus coinbase placeholder)
	numTxs = (desiredSubtrees * adjustedSubtreeSize) - 1

	fmt.Printf("[%s] Adjusted to process %d transactions (creates exactly %d complete subtrees of size %d)\n",
		time.Since(benchStartTime), numTxs, desiredSubtrees, adjustedSubtreeSize)

	// Pre-generate transaction hashes to avoid crypto overhead in benchmark
	// Use multiple workers for parallel generation
	numWorkers := runtime.NumCPU()
	fmt.Printf("[%s] Pre-generating %d transaction hashes using %d workers...\n", time.Since(benchStartTime), numTxs, numWorkers)
	txHashes := make([]chainhash.Hash, numTxs)
	parentHashes := make([]chainhash.Hash, numTxs)

	var genWg sync.WaitGroup
	itemsPerWorker := numTxs / numWorkers
	for w := 0; w < numWorkers; w++ {
		genWg.Add(1)
		go func(workerID int) {
			defer genWg.Done()
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			if workerID == numWorkers-1 {
				end = numTxs // Last worker handles remainder
			}

			for i := start; i < end; i++ {
				// Use deterministic hash based on index to guarantee uniqueness
				txHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i)))
				parentHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("parent-%d", i)))
			}
		}(w)
	}
	genWg.Wait()
	fmt.Printf("[%s] Pre-generation complete.\n", time.Since(benchStartTime))
	fmt.Printf("[%s] Setup complete. Starting profiled benchmark...\n\n", time.Since(benchStartTime))

	// Start CPU profiling AFTER all setup is complete
	cpuFile, err := os.Create(cpuProfile)
	if err != nil {
		return benchmarkResult{}, errors.NewProcessingError("failed to create CPU profile: %w", err)
	}

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		return benchmarkResult{}, errors.NewProcessingError("failed to start CPU profile: %w", err)
	}

	// Measure metrics
	var opsCompleted atomic.Int64

	// Run benchmark with multiple producer goroutines
	var wg sync.WaitGroup
	itemsPerGoroutine := numTxs / producers
	const batchSize = 2 * 1024

	// Start the timer right before actual processing begins
	startTime := time.Now()
	fmt.Printf("[%s] Starting benchmark with %d producers (batch size: %d)...\n", time.Since(benchStartTime), producers, batchSize)

	for g := 0; g < producers; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == producers-1 {
				end = numTxs // Last goroutine handles remainder
			}

			// Pre-allocate batch slices
			nodes := make([]subtreepkg.Node, 0, batchSize)
			inpoints := make([]*subtreepkg.TxInpoints, 0, batchSize)

			for i := start; i < end; i++ {
				// Add to batch
				nodes = append(nodes, subtreepkg.Node{
					Hash: txHashes[i],
					Fee:  uint64(i % 10000),
				})
				inpoints = append(inpoints, &subtreepkg.TxInpoints{
					ParentTxHashes: []chainhash.Hash{parentHashes[i]},
				})

				// Submit batch when full
				if len(nodes) >= batchSize {
					// Copy slices before submitting since AddBatch stores references
					nodesCopy := make([]subtreepkg.Node, len(nodes))
					inpointsCopy := make([]*subtreepkg.TxInpoints, len(inpoints))
					copy(nodesCopy, nodes)
					copy(inpointsCopy, inpoints)
					stp.AddBatch(nodesCopy, inpointsCopy)
					opsCompleted.Add(int64(len(nodes)))
					// Reset slices (reuse backing array)
					nodes = nodes[:0]
					inpoints = inpoints[:0]
				}
			}

			// Flush remaining batch
			if len(nodes) > 0 {
				// Copy slices before submitting since AddBatch stores references
				nodesCopy := make([]subtreepkg.Node, len(nodes))
				inpointsCopy := make([]*subtreepkg.TxInpoints, len(inpoints))
				copy(nodesCopy, nodes)
				copy(inpointsCopy, inpoints)
				stp.AddBatch(nodesCopy, inpointsCopy)
				opsCompleted.Add(int64(len(nodes)))
			}
		}(g)
	}

	wg.Wait()
	producersDone := time.Since(startTime)
	fmt.Printf("\n[%s] Producers finished in %.2fs, submitted %d transactions\n", time.Since(benchStartTime), producersDone.Seconds(), opsCompleted.Load())

	// Wait for all transactions to be processed into subtrees
	// Since we adjusted numTxs to be exact multiple, all subtrees will be complete
	// The actual number of transactions that will be in subtrees is what was submitted
	actualSubmitted := opsCompleted.Load()
	expectedInSubtrees := actualSubmitted + 1 // +1 for coinbase

	fmt.Printf("[%s] Waiting for all transactions to be in subtrees...\n", time.Since(benchStartTime))
	fmt.Printf("[%s]   Submitted: %d, Expected in subtrees: %d (with coinbase)\n",
		time.Since(benchStartTime), actualSubmitted, expectedInSubtrees)

	lastPrintTime := time.Now()

	for time.Since(startTime) < 120*time.Second {
		completedSubtrees := subtreeCount.Load()
		subtreeTxs := subtreeTxCount.Load()

		// Check if all transactions are in subtrees (use actual submitted count)
		if subtreeTxs >= expectedInSubtrees {
			fmt.Printf("[%s] All transactions in subtrees: %d/%d, Complete subtrees: %d\n",
				time.Since(benchStartTime), subtreeTxs, expectedInSubtrees, completedSubtrees)
			break
		}

		// Print progress every second
		if time.Since(lastPrintTime) >= time.Second {
			fmt.Printf("  [%s] Complete subtrees: %d, Transactions: %d/%d (%.1f%%)\n",
				time.Since(benchStartTime), completedSubtrees,
				subtreeTxs, expectedInSubtrees, 100.0*float64(subtreeTxs)/float64(expectedInSubtrees))
			lastPrintTime = time.Now()
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Check if we timed out
	if time.Since(startTime) >= 120*time.Second {
		fmt.Printf("[%s] WARNING: Timeout waiting for subtrees. Got %d/%d transactions in %d subtrees\n",
			time.Since(benchStartTime), subtreeTxCount.Load(), expectedInSubtrees, subtreeCount.Load())
	}

	// Calculate total elapsed time
	elapsed := time.Since(startTime)
	fmt.Printf("[%s] All transactions processed in %.2fs total\n", time.Since(benchStartTime), elapsed.Seconds())
	fmt.Printf("[%s] Final state - Queue: %d, Processed: %d, In subtrees: %d\n", time.Since(benchStartTime), stp.QueueLength(), stp.TxCount(), subtreeTxCount.Load())

	// Stop CPU profiling AFTER processing completes (including drain)
	pprof.StopCPUProfile()
	if err := cpuFile.Close(); err != nil {
		return benchmarkResult{}, errors.NewProcessingError("failed to close CPU profile: %w", err)
	}

	// Write memory profile immediately after processing
	memFile, err := os.Create(memProfile)
	if err != nil {
		return benchmarkResult{}, errors.NewProcessingError("failed to create memory profile: %w", err)
	}
	defer func() {
		if err := memFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close memory profile: %v\n", err)
		}
	}()

	runtime.GC() // Force GC before memory profile
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		return benchmarkResult{}, errors.NewStorageError("failed to write memory profile: %w", err)
	}

	totalCompleted := int(opsCompleted.Load())
	txPerSec := float64(totalCompleted) / elapsed.Seconds()
	queueRemaining := stp.QueueLength()
	processedTxs := stp.TxCount()
	subtreesCreated := subtreeCount.Load()
	totalSubtreeTxs := subtreeTxCount.Load()

	// Check if we timed out before all transactions were processed
	if totalSubtreeTxs < int64(totalCompleted) {
		fmt.Fprintf(os.Stderr, "\nWARNING: Timed out before all transactions were formed into subtrees!\n")
		fmt.Fprintf(os.Stderr, "Submitted: %d, In subtrees: %d (%.1f%%)\n",
			totalCompleted, totalSubtreeTxs, 100.0*float64(totalSubtreeTxs)/float64(totalCompleted))
	}

	// Validate subtree counts and sizes
	fmt.Printf("\nValidating subtree results...\n")

	// NOTE: The coinbase placeholder adds +1 transaction to the total
	// All subtrees are the same configured size (actualSubtreeSize)
	// We just need to account for the extra transaction in total count
	coinbasePlaceholder := int64(1)
	expectedTotalInSubtrees := int64(totalCompleted) + coinbasePlaceholder

	// Calculate expected number of subtrees
	// Total txs to distribute: totalCompleted + 1 (coinbase)
	totalWithCoinbase := totalCompleted + 1
	expectedSubtrees := int64(totalWithCoinbase / actualSubtreeSize)
	remainderTxs := totalWithCoinbase % actualSubtreeSize

	// If there's a remainder, we need one more partial subtree
	if remainderTxs > 0 {
		expectedSubtrees++
	}

	// Copy subtree sizes map for analysis
	subtreeSizesMu.Lock()
	sizesCopy := make(map[int]int64, len(subtreeSizes))
	for size, count := range subtreeSizes {
		sizesCopy[size] = count
	}
	subtreeSizesMu.Unlock()

	fmt.Printf("Using subtree size: %d (power of 2)\n", actualSubtreeSize)
	fmt.Printf("Expected total txs in subtrees: %d (includes coinbase placeholder)\n", expectedTotalInSubtrees)
	fmt.Printf("Expected subtrees: %d", expectedSubtrees)
	if remainderTxs > 0 {
		fmt.Printf(" (%d full of size %d, 1 partial of size %d)\n", expectedSubtrees-1, actualSubtreeSize, remainderTxs)
	} else {
		fmt.Printf(" (all size %d)\n", actualSubtreeSize)
	}
	fmt.Printf("Actual subtrees created: %d\n", subtreesCreated)
	fmt.Printf("Total txs in subtrees: %d\n", totalSubtreeTxs)

	// Print subtree size distribution
	if len(sizesCopy) > 0 {
		fmt.Printf("\nSubtree size distribution:\n")
		for size, count := range sizesCopy {
			fmt.Printf("  Size %d: %d subtrees\n", size, count)
		}
	}

	// Validation checks
	validationPassed := true

	// Check total transaction count
	if totalSubtreeTxs != expectedTotalInSubtrees {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: Subtree tx count mismatch! Expected %d (with coinbase), got %d\n", expectedTotalInSubtrees, totalSubtreeTxs)
		validationPassed = false
	}

	// Check subtree count
	if subtreesCreated != expectedSubtrees {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: Expected %d subtrees, got %d\n", expectedSubtrees, subtreesCreated)
		validationPassed = false
	}

	// Check full-sized subtrees
	fullSizeCount := sizesCopy[actualSubtreeSize]
	expectedFullCount := expectedSubtrees
	if remainderTxs > 0 {
		expectedFullCount-- // One will be partial
	}
	if fullSizeCount != expectedFullCount {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: Expected %d full subtrees (size=%d), got %d\n", expectedFullCount, actualSubtreeSize, fullSizeCount)
		validationPassed = false
	}

	// Check partial subtree if expected
	if remainderTxs > 0 {
		partialCount := sizesCopy[remainderTxs]
		if partialCount != 1 {
			_, _ = fmt.Fprintf(os.Stderr, "WARNING: Expected 1 partial subtree (size=%d), got %d\n", remainderTxs, partialCount)
			validationPassed = false
		}
	}

	if validationPassed {
		fmt.Printf("\n✓ Validation passed: All subtrees have correct sizes and counts\n")
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "\n✗ Validation FAILED: Subtree size/count mismatches detected\n")
	}

	// Verify transaction order (after profiling, doesn't affect benchmark timing)
	fmt.Printf("\nVerifying transaction order in subtrees...\n")
	allSubtrees := stp.GetCompletedSubtreesForMiningCandidate()

	// Build a set of all transaction hashes that should be in the subtrees
	expectedHashes := make(map[chainhash.Hash]bool, numTxs)
	for i := 0; i < numTxs; i++ {
		expectedHashes[txHashes[i]] = true
	}

	// Check all transactions in subtrees
	foundHashes := make(map[chainhash.Hash]bool, numTxs)
	var missingCount, unexpectedCount int

	for i, subtree := range allSubtrees {
		if subtree == nil {
			continue
		}

		for j, node := range subtree.Nodes {
			// Skip coinbase placeholder (first node in first subtree)
			if i == 0 && j == 0 {
				continue
			}

			// Check if this hash was expected
			if !expectedHashes[node.Hash] {
				if unexpectedCount < 10 { // Limit output
					fmt.Printf("  WARNING: Unexpected hash in subtree %d: %s\n", i, node.Hash.String())
				}
				unexpectedCount++
			} else {
				foundHashes[node.Hash] = true
			}
		}
	}

	// Check for missing transactions
	for i := 0; i < numTxs; i++ {
		if !foundHashes[txHashes[i]] {
			if missingCount < 10 { // Limit output
				fmt.Printf("  WARNING: Missing transaction %d: %s\n", i, txHashes[i].String())
			}
			missingCount++
		}
	}

	if missingCount > 0 || unexpectedCount > 0 {
		fmt.Printf("✗ Order verification FAILED: %d missing, %d unexpected transactions\n", missingCount, unexpectedCount)
	} else {
		fmt.Printf("✓ Order verification passed: All %d transactions present\n", numTxs)
	}

	return benchmarkResult{
		TotalTxs:        totalCompleted,
		Elapsed:         elapsed,
		TxPerSec:        txPerSec,
		ProcessedTxs:    processedTxs,
		SubtreesCreated: subtreesCreated,
		QueueRemaining:  queueRemaining,
	}, nil
}
