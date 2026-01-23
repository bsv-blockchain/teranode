package teranodecli

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	aerospikeutil "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

func runLoadUnminedBenchmark(txCount int, fullScan bool, cpuProfile, memProfile, aerospikeURL string) error {
	fmt.Printf("LoadUnmined Benchmark\n")
	fmt.Printf("=====================\n")
	fmt.Printf("Transaction Count:      %d\n", txCount)
	fmt.Printf("Full Scan:              %t\n", fullScan)
	fmt.Printf("CPU Cores:              %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	ctx := context.Background()

	// Setup Aerospike
	var aerospikeStore utxo.Store
	var cleanup func() error

	if aerospikeURL == "" {
		fmt.Printf("[Setup] Starting Aerospike TestContainer...\n")
		var err error
		aerospikeURL, cleanup, err = aerospikeutil.InitAerospikeContainer()
		if err != nil {
			return errors.NewProcessingError("failed to initialize Aerospike container: %w", err)
		}
		defer func() {
			if err := cleanup(); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to cleanup Aerospike container: %v\n", err)
			}
		}()
		fmt.Printf("[Setup] Aerospike container ready\n")
	}

	// Create settings
	tSettings := createLoadUnminedBenchmarkSettings()

	// Parse Aerospike URL and create store
	aerospikeURI, err := url.Parse(aerospikeURL)
	if err != nil {
		return errors.NewProcessingError("failed to parse Aerospike URL: %w", err)
	}

	aerospikeStore, err = aerospike.New(ctx, ulogger.TestLogger{}, tSettings, aerospikeURI)
	if err != nil {
		return errors.NewProcessingError("failed to create Aerospike store: %w", err)
	}

	// Setup phase - populate transactions
	setupStart := time.Now()
	if err := populateAerospikeWithTransactions(ctx, aerospikeStore, txCount); err != nil {
		return err
	}

	// Wait for index if not full scan
	if !fullScan {
		if indexWaiter, ok := aerospikeStore.(interface {
			WaitForIndexReady(ctx context.Context, indexName string) error
		}); ok {
			fmt.Printf("[Setup] Waiting for UnminedSince index...\n")
			start := time.Now()
			err := indexWaiter.WaitForIndexReady(ctx, "unminedSinceIndex")
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("[Setup] Index wait failed after %.2fs: %v\n", duration.Seconds(), err)
			} else {
				fmt.Printf("[Setup] Index ready (%.0fms)\n", duration.Seconds()*1000)
			}
		}
	}

	setupDuration := time.Since(setupStart)
	fmt.Printf("[Setup] Total setup time: %.2fs\n\n", setupDuration.Seconds())

	// Start profiling
	cpuFile, err := os.Create(cpuProfile)
	if err != nil {
		return errors.NewProcessingError("failed to create CPU profile: %w", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		return errors.NewProcessingError("failed to start CPU profile: %w", err)
	}

	// Run benchmark - iterate through all unmined transactions
	fmt.Printf("[Benchmark] Running GetUnminedTxIterator...\n")
	benchmarkStart := time.Now()

	iterator, err := aerospikeStore.GetUnminedTxIterator(fullScan)
	if err != nil {
		pprof.StopCPUProfile()
		return errors.NewProcessingError("failed to get iterator: %w", err)
	}

	processedCount := int64(0)
	skippedCount := int64(0)

	for {
		batch, err := iterator.Next(ctx)
		if err != nil {
			if err.Error() == "no more transactions" {
				break
			}
			pprof.StopCPUProfile()
			return errors.NewProcessingError("iterator error: %w", err)
		}
		if batch == nil || len(batch) == 0 {
			break
		}

		for _, tx := range batch {
			if tx.Skip {
				skippedCount++
			} else {
				processedCount++
			}
		}
	}

	benchmarkDuration := time.Since(benchmarkStart)
	fmt.Printf("[Benchmark] Complete in %.2fs\n\n", benchmarkDuration.Seconds())

	// Stop profiling
	pprof.StopCPUProfile()

	// Write memory profile
	memFile, err := os.Create(memProfile)
	if err != nil {
		return errors.NewProcessingError("failed to create memory profile: %w", err)
	}
	defer memFile.Close()

	runtime.GC()
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		return errors.NewStorageError("failed to write memory profile: %w", err)
	}

	// Calculate throughput
	totalTx := processedCount + skippedCount
	txPerSec := float64(totalTx) / benchmarkDuration.Seconds()

	// Print benchmark results
	fmt.Printf("Benchmark Results\n")
	fmt.Printf("=================\n")
	fmt.Printf("  Transactions Processed:  %d\n", processedCount)
	fmt.Printf("  Transactions Skipped:    %d\n", skippedCount)
	fmt.Printf("  Total:                   %d\n", totalTx)
	fmt.Printf("  Benchmark Time:          %.2fs\n", benchmarkDuration.Seconds())
	fmt.Printf("  Throughput:              %.0f tx/sec\n", txPerSec)
	fmt.Println()
	fmt.Printf("Profiles written to: %s, %s\n", cpuProfile, memProfile)
	fmt.Println()

	return nil
}

func createLoadUnminedBenchmarkSettings() *settings.Settings {
	tSettings := settings.NewSettings()

	// Use a temporary directory
	tSettings.DataFolder = os.TempDir() + "/loadunmined_bench"
	_ = os.MkdirAll(tSettings.DataFolder, 0755)

	// Create a copy of RegressionNetParams
	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1
	tSettings.ChainCfgParams = &chainParams
	tSettings.GlobalBlockHeightRetention = 10
	tSettings.BlockValidation.OptimisticMining = false

	// Configure block assembly settings
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1_048_576
	tSettings.BlockAssembly.SubtreeProcessorBatcherSize = 1024 * 1024
	tSettings.BlockAssembly.SubtreeAnnouncementInterval = 24 * time.Hour
	tSettings.BlockAssembly.OnRestartValidateParentChain = false

	return tSettings
}

func populateAerospikeWithTransactions(ctx context.Context, store utxo.Store, count int) error {
	fmt.Printf("[Setup] Populating Aerospike with %d transactions...\n", count)

	startTime := time.Now()
	currentBlockHeight := uint32(100)

	g := errgroup.Group{}
	g.SetLimit(16 * 1024)

	for i := 0; i < count; i++ {
		i := i

		// Insert into store with unmined option
		g.Go(func() error {
			// Create minimal bt.Tx using NewTx
			tx := bt.NewTx()
			tx.Inputs = make([]*bt.Input, 1)

			parentTxID := chainhash.HashH([]byte(fmt.Sprintf("parent-%d", i)))
			unLockingScript, _ := bscript.NewFromASM("OP_TRUE")
			tx.Inputs[0] = &bt.Input{
				PreviousTxSatoshis: 5000000,
				PreviousTxScript:   &bscript.Script{},
				UnlockingScript:    unLockingScript,
				PreviousTxOutIndex: 0,
				SequenceNumber:     uint32(i), // nolint:gosec
			}
			_ = tx.Inputs[0].PreviousTxIDAdd(&parentTxID)

			// Add output
			lockingScript, _ := bscript.NewFromASM("OP_TRUE OP_EQUAL")
			tx.AddOutput(&bt.Output{
				Satoshis:      1000,
				LockingScript: lockingScript,
			})

			if _, err := store.Create(ctx, tx, currentBlockHeight); err != nil {
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to populate Aerospike: %w", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("[Setup] Population complete in %.2fs (%.0f tx/sec)\n", duration.Seconds(), float64(count)/duration.Seconds())

	return nil
}
