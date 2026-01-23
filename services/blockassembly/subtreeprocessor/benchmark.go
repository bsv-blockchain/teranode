package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// CreateTransactionMapBenchmarkResult holds results from the CreateTransactionMap benchmark
type CreateTransactionMapBenchmarkResult struct {
	NumSubtrees      int
	TxsPerSubtree    int
	TotalTxs         int
	Elapsed          time.Duration
	TxPerSec         float64
	MapLength        int
	ConflictingNodes int
	BenchErr         error
}

// ProcessRemainderBenchmarkResult holds results from the processRemainderTransactionsAndDequeue benchmark
type ProcessRemainderBenchmarkResult struct {
	NumChainedSubtrees int
	TxsPerSubtree      int
	TotalTxs           int
	Elapsed            time.Duration
	TxPerSec           float64
	RemainderCount     int
	BenchErr           error
}

func createBenchmarkSettings(txsPerSubtree int) *settings.Settings {
	s := settings.NewSettings()
	s.DataFolder = os.TempDir() + "/txmapbench"

	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1
	s.ChainCfgParams = &chainParams
	s.GlobalBlockHeightRetention = 10
	s.BlockValidation.OptimisticMining = false
	s.BlockAssembly.InitialMerkleItemsPerSubtree = txsPerSubtree

	return s
}

// RunCreateTransactionMapBenchmark runs the CreateTransactionMap benchmark with profiling
func RunCreateTransactionMapBenchmark(numSubtrees, txsPerSubtree int, cpuProfile, memProfile string) (CreateTransactionMapBenchmarkResult, error) {
	totalTxs := numSubtrees * txsPerSubtree
	benchStartTime := time.Now()

	fmt.Printf("CreateTransactionMap Benchmark\n")
	fmt.Printf("==============================\n")
	fmt.Printf("Subtrees:         %d\n", numSubtrees)
	fmt.Printf("Txs per subtree:  %d\n", txsPerSubtree)
	fmt.Printf("Total Txs:        %d\n", totalTxs)
	fmt.Printf("CPU Cores:        %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS:       %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	// ===== SETUP PHASE (not profiled) =====
	fmt.Printf("[%s] Setting up benchmark...\n", time.Since(benchStartTime))

	ctx := context.Background()
	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	defer close(newSubtreeChan)

	subtreeStore := blob_memory.New()

	tSettings := createBenchmarkSettings(txsPerSubtree)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to parse utxo store URL: %w", err)
	}

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	if err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to create utxo store: %w", err)
	}

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, newSubtreeChan)
	if err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to create subtree processor: %w", err)
	}

	stp.SetCurrentItemsPerFile(1024 * 1024)

	fmt.Printf("[%s] Creating %d subtrees with %d txs each...\n", time.Since(benchStartTime), numSubtrees, txsPerSubtree)

	blockSubtreesMap := make(map[chainhash.Hash]int, numSubtrees)

	for s := 0; s < numSubtrees; s++ {
		subtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree)
		if err != nil {
			return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to create subtree %d: %w", s, err)
		}

		if s == 0 {
			if err := subtree.AddCoinbaseNode(); err != nil {
				return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to add coinbase node: %w", err)
			}
		}

		for i := 0; i < txsPerSubtree-1; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("tx-%d-%d", s, i)))
			if err := subtree.AddNode(txHash, uint64(s*txsPerSubtree+i+1), 100); err != nil {
				return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to add node: %w", err)
			}
		}

		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to serialize subtree: %w", err)
		}

		if err := subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes); err != nil {
			return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to store subtree: %w", err)
		}

		blockSubtreesMap[*subtree.RootHash()] = s
	}
	fmt.Printf("[%s] Subtrees created and stored.\n", time.Since(benchStartTime))
	fmt.Printf("[%s] Setup complete. Starting profiled benchmark...\n\n", time.Since(benchStartTime))

	// ===== PROFILING PHASE =====
	cpuFile, err := os.Create(cpuProfile)
	if err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to create CPU profile: %w", err)
	}

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to start CPU profile: %w", err)
	}

	// Run the benchmark
	startTime := time.Now()
	transactionMap, conflictingNodes, benchErr := stp.CreateTransactionMap(ctx, blockSubtreesMap, numSubtrees, uint64(totalTxs))
	elapsed := time.Since(startTime)

	// Stop CPU profiling
	pprof.StopCPUProfile()
	if err := cpuFile.Close(); err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to close CPU profile: %w", err)
	}

	// Write memory profile
	runtime.GC()
	memFile, err := os.Create(memProfile)
	if err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to create memory profile: %w", err)
	}
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		memFile.Close()
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to write memory profile: %w", err)
	}
	if err := memFile.Close(); err != nil {
		return CreateTransactionMapBenchmarkResult{}, errors.NewProcessingError("failed to close memory profile: %w", err)
	}

	// Build result
	mapLength := 0
	if transactionMap != nil {
		mapLength = transactionMap.Length()
	}

	result := CreateTransactionMapBenchmarkResult{
		NumSubtrees:      numSubtrees,
		TxsPerSubtree:    txsPerSubtree,
		TotalTxs:         totalTxs,
		Elapsed:          elapsed,
		TxPerSec:         float64(totalTxs) / elapsed.Seconds(),
		MapLength:        mapLength,
		ConflictingNodes: len(conflictingNodes),
		BenchErr:         benchErr,
	}

	return result, nil
}

// RunProcessRemainderBenchmark runs the processRemainderTransactionsAndDequeue benchmark with profiling
func RunProcessRemainderBenchmark(numChainedSubtrees, txsPerSubtree int, cpuProfile, memProfile string) (ProcessRemainderBenchmarkResult, error) {
	totalTxs := (numChainedSubtrees + 1) * txsPerSubtree
	benchStartTime := time.Now()

	fmt.Printf("ProcessRemainderTransactionsAndDequeue Benchmark\n")
	fmt.Printf("================================================\n")
	fmt.Printf("Chained Subtrees:   %d\n", numChainedSubtrees)
	fmt.Printf("Txs per subtree:    %d\n", txsPerSubtree)
	fmt.Printf("Total Txs:          %d\n", totalTxs)
	fmt.Printf("CPU Cores:          %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS:         %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	// ===== SETUP PHASE (not profiled) =====
	fmt.Printf("[%s] Setting up benchmark...\n", time.Since(benchStartTime))

	ctx := context.Background()
	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	defer close(newSubtreeChan)

	tSettings := createBenchmarkSettings(txsPerSubtree * 2)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan)
	if err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create subtree processor: %w", err)
	}

	// Initialize current subtree
	newSubtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree * 2)
	if err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create new subtree: %w", err)
	}
	stp.currentSubtree.Store(newSubtree)
	stp.chainedSubtrees = nil
	_ = stp.GetCurrentSubtree().AddCoinbaseNode()

	fmt.Printf("[%s] Creating chained subtrees and transaction data...\n", time.Since(benchStartTime))

	// Create chained subtrees with transaction data
	chainedSubtrees := make([]*subtreepkg.Subtree, numChainedSubtrees)
	allTxHashes := make([]chainhash.Hash, 0, totalTxs)
	parentHash := chainhash.HashH([]byte("parent-tx-benchmark"))

	for s := 0; s < numChainedSubtrees; s++ {
		subtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree)
		if err != nil {
			return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create chained subtree %d: %w", s, err)
		}

		if s == 0 {
			_ = subtree.AddCoinbaseNode()
		}

		for i := 0; i < txsPerSubtree-1; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("tx-chained-%d-%d", s, i)))
			if err := subtree.AddSubtreeNode(subtreepkg.Node{Hash: txHash, Fee: 100}); err != nil {
				return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to add chained subtree node: %w", err)
			}
			allTxHashes = append(allTxHashes, txHash)
		}
		chainedSubtrees[s] = subtree
	}

	// Create current subtree with transactions
	currentSubtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree)
	if err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create current subtree: %w", err)
	}
	_ = currentSubtree.AddCoinbaseNode()
	for i := 0; i < txsPerSubtree-1; i++ {
		txHash := chainhash.HashH([]byte(fmt.Sprintf("tx-current-%d", i)))
		if err := currentSubtree.AddSubtreeNode(subtreepkg.Node{Hash: txHash, Fee: 100}); err != nil {
			return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to add current subtree node: %w", err)
		}
		allTxHashes = append(allTxHashes, txHash)
	}

	fmt.Printf("[%s] Created %d transaction hashes.\n", time.Since(benchStartTime), len(allTxHashes))

	// Create transaction map with all transactions (simulating external block)
	transactionMap := NewSplitSwissMap(1024, len(allTxHashes))
	for _, hash := range allTxHashes {
		if err := transactionMap.Put(hash); err != nil {
			return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to put hash in transaction map: %w", err)
		}
	}
	fmt.Printf("[%s] Transaction map created with %d entries.\n", time.Since(benchStartTime), transactionMap.Length())

	// Create current tx map for parent lookups
	currentTxMap := NewSplitTxInpointsMap(16)
	for _, hash := range allTxHashes {
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parentHash}})
	}
	fmt.Printf("[%s] Current tx map created with %d entries.\n", time.Since(benchStartTime), currentTxMap.Length())

	losingTxHashesMap := txmap.NewSplitSwissMap(4, 10)

	// Create mock block
	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), // nolint: gosec
			Bits:           model.NBit{},
			Nonce:          0,
		},
		CoinbaseTx: &bt.Tx{},
	}

	params := &RemainderTransactionParams{
		Block:             block,
		ChainedSubtrees:   chainedSubtrees,
		CurrentSubtree:    currentSubtree,
		TransactionMap:    transactionMap,
		LosingTxHashesMap: losingTxHashesMap,
		CurrentTxMap:      currentTxMap,
		SkipDequeue:       true,
		SkipNotification:  true,
	}

	fmt.Printf("[%s] Setup complete. Starting profiled benchmark...\n\n", time.Since(benchStartTime))

	// ===== PROFILING PHASE =====
	cpuFile, err := os.Create(cpuProfile)
	if err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create CPU profile: %w", err)
	}

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to start CPU profile: %w", err)
	}

	// Run the benchmark
	startTime := time.Now()
	benchErr := stp.processRemainderTransactionsAndDequeue(ctx, params)
	elapsed := time.Since(startTime)

	// Stop CPU profiling
	pprof.StopCPUProfile()
	if err := cpuFile.Close(); err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to close CPU profile: %w", err)
	}

	// Write memory profile
	runtime.GC()
	memFile, err := os.Create(memProfile)
	if err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to create memory profile: %w", err)
	}
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		memFile.Close()
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to write memory profile: %w", err)
	}
	if err := memFile.Close(); err != nil {
		return ProcessRemainderBenchmarkResult{}, errors.NewProcessingError("failed to close memory profile: %w", err)
	}

	// Count remainder nodes
	remainderCount := 0
	for _, st := range stp.GetChainedSubtrees() {
		remainderCount += st.Length()
	}
	remainderCount += stp.GetCurrentSubtree().Length()

	result := ProcessRemainderBenchmarkResult{
		NumChainedSubtrees: numChainedSubtrees,
		TxsPerSubtree:      txsPerSubtree,
		TotalTxs:           totalTxs,
		Elapsed:            elapsed,
		TxPerSec:           float64(totalTxs) / elapsed.Seconds(),
		RemainderCount:     remainderCount,
		BenchErr:           benchErr,
	}

	return result, nil
}
