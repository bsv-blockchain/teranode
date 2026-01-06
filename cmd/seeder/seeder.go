// Package seeder provides functionality for processing blockchain headers and UTXO sets.
// It is designed to initialize the seeder service, handle headers and UTXOs, and manage
// related operations such as profiling and signal handling.
//
// Usage:
//
// This package is typically used as a command-line tool to process blockchain data
// from specified input files and store the results in appropriate stores.
//
// Functions:
//   - Seeder: Initializes the seeder service and orchestrates header and UTXO processing.
//
// Side effects:
//
// Functions in this package may interact with external stores, print to stdout, and
// handle system signals for graceful termination.
package seeder

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // This is used for internal profiling
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

const (
	errMsgFailedToReadUTXO = "failed to read UTXO set header"
)

// usage prints the usage message and exits the program with an error code.
func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}

	fmt.Printf("Usage: seeder -inputDir <folder> -hash <hash> [-skipHeaders] [-skipUTXOs]\n\n")

	os.Exit(1)
}

// Seeder initializes the seeder service, processes headers and UTXOs, and starts the profiler server.
//
// Parameters:
//   - logger: Logger instance for logging messages.
//   - appSettings: Application settings containing configuration values.
//   - inputDir: Directory containing input files.
//   - hash: Hash value used to locate specific files.
//   - skipHeaders: Boolean flag to skip processing headers.
//   - skipUTXOs: Boolean flag to skip processing UTXOs.
//
// Side effects:
//   - Starts a profiler server.
//   - Processes headers and UTXOs concurrently.
//   - Handles system signals for graceful termination.
//   - Prints messages to stdout.
//
//nolint:gocognit // Requires refactoring to reduce cognitive complexity
func Seeder(logger ulogger.Logger, appSettings *settings.Settings, inputDir string, hash string,
	skipHeaders bool, skipUTXOs bool) {
	profilerAddr := appSettings.ProfilerAddr
	if profilerAddr != "" {
		go func() {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			gocore.RegisterStatsHandlers()

			prefix := appSettings.StatsPrefix
			logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, prefix)

			server := &http.Server{
				Addr:         profilerAddr,
				Handler:      nil,
				ReadTimeout:  60 * time.Second,
				WriteTimeout: 60 * time.Second,
				IdleTimeout:  120 * time.Second,
			}

			// http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Errorf("Profiler server failed: %v", err)
			}
		}()
	}

	var (
		headerFile string
		utxoFile   string
	)

	if !skipHeaders {
		// Check the header file exists
		headerFile = filepath.Join(inputDir, hash+".utxo-headers")
		if _, err := os.Stat(headerFile); os.IsNotExist(err) {
			usage(fmt.Sprintf("Headers file %s does not exist", headerFile))
		}
	}

	if !skipUTXOs {
		// Check the UTXO file exists
		utxoFile = filepath.Join(inputDir, hash+".utxo-set")
		if _, err := os.Stat(utxoFile); os.IsNotExist(err) {
			usage(fmt.Sprintf("UTXO file %s does not exist", utxoFile))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle CTRL-C (SIGINT)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nCTRL-C pressed. Cancelling all operations...")
		cancel()
	}()

	// Start http server for the profiler
	go func() {
		logger.Errorf("%v", http.ListenAndServe(":6060", nil)) //nolint:gosec // needs enhanced options for timeouts
	}()

	wg := sync.WaitGroup{}

	if !skipHeaders {
		wg.Add(1)

		go func() {
			defer wg.Done()

			logger.Infof("Processing headers...")

			// Process the headers
			if err := processHeaders(ctx, logger, appSettings, headerFile); err != nil {
				logger.Errorf("Failed to process headers: %v", err)
				return
			}

			logger.Infof("Finished processing headers")
		}()
	}

	if !skipUTXOs {
		wg.Add(1)

		go func() {
			defer wg.Done()

			logger.Infof("Processing UTXOs...")

			// Process the UTXOs
			if err := processUTXOs(ctx, logger, appSettings, utxoFile); err != nil {
				logger.Errorf("Failed to process UTXOs: %v", err)
				return
			}

			logger.Infof("Finished processing UTXOs")
		}()
	}

	wg.Wait()
}

// processHeaders reads the UTXO headers from a file and stores them in the blockchain store.
//
//nolint:gocognit // Requires refactoring to reduce cognitive complexity
func processHeaders(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings, headersFile string) error {
	blockchainStoreURL := appSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return errors.NewConfigurationError("blockchain store URL not found in config")
	}

	q := blockchainStoreURL.Query()
	q.Set("seeder", "true")

	blockchainStoreURL.RawQuery = q.Encode()
	logger.Infof("Using blockchain store at %s", blockchainStoreURL)

	blockchainStore, err := blockchain.NewStore(logger, blockchainStoreURL, appSettings)
	if err != nil {
		logger.Fatalf("Failed to create blockchain client: %v", err)
	}

	// _, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	// if err != nil {
	// 	logger.Fatalf("Failed to get best block header: %v", err)
	// }

	// _ = meta

	var f *os.File

	f, err = os.Open(headersFile)
	if err != nil {
		return errors.NewStorageError("failed to open file", err)
	}

	defer func() {
		_ = f.Close()
	}()

	reader := bufio.NewReader(f)

	var header fileformat.Header

	header, err = fileformat.ReadHeader(reader)
	if err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	if header.FileType() != fileformat.FileTypeUtxoHeaders {
		return errors.NewProcessingError("Invalid file type: %s", header.FileType())
	}

	var (
		hash   chainhash.Hash
		height uint32
	)

	if err = binary.Read(reader, binary.LittleEndian, &hash); err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	if err = binary.Read(reader, binary.LittleEndian, &height); err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	// Note: Block persistence state is now tracked in the database via persisted_at column
	// No need to write to a state file anymore

	var (
		headersProcessed uint64
		txCount          uint64
	)

	var blockIndex *utxopersister.BlockIndex

	for {
		blockIndex, err = utxopersister.NewUTXOHeaderFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return errors.NewProcessingError("failed to read UTXO", err)
		}

		if blockIndex.Height == 0 {
			// The genesis block is already in the store
			continue
		}

		block := &model.Block{
			Header:           blockIndex.BlockHeader,
			TransactionCount: blockIndex.TxCount,
			Height:           blockIndex.Height,
		}

		_, _, err = blockchainStore.StoreBlock(
			ctx,
			block,
			"headers",
			blockchainoptions.WithMinedSet(true),
			blockchainoptions.WithSubtreesSet(true),
			blockchainoptions.WithPersistedAt(), // Mark as persisted now, since we're seeding and the block persister won't be able to do it later
		)
		if err != nil {
			return errors.NewProcessingError("failed to add block", err)
		}

		headersProcessed++
		txCount += blockIndex.TxCount

		if blockIndex.Height%10000 == 0 {
			fmt.Printf("Processed to block height %d\n", blockIndex.Height)
		}
	}

	logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(headersProcessed), formatNumber(txCount))

	return nil
}

// processUTXOs reads the UTXO set from a file and stores it in the UTXO store.
//
//nolint:gocognit // Requires refactoring to reduce cognitive complexity
func processUTXOs(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings, utxoFile string) error {
	blockStoreURL := appSettings.Block.BlockStore
	if blockStoreURL == nil {
		return errors.NewConfigurationError("blockstore URL not found in config")
	}

	logger.Infof("Using blockStore at %s", blockStoreURL)

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	if err != nil {
		return errors.NewStorageError("failed to create blockStore", err)
	}

	var exists bool

	exists, err = blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"))
	if err != nil {
		return errors.NewStorageError("failed to check if lastProcessed.dat exists", err)
	}

	if exists {
		logger.Errorf("lastProcessed.dat exists, skipping UTXOs")
		return nil
	}

	logger.Infof("Using utxostore at %s", appSettings.UtxoStore.UtxoStore)

	var utxoStore utxo.Store

	utxoStore, err = utxofactory.NewStore(ctx, logger, appSettings, "seeder", false)
	if err != nil {
		return errors.NewStorageError("failed to create utxostore", err)
	}

	channelSize, _ := gocore.Config().GetInt("channelSize", 1000)

	logger.Infof("Using channel size of %d", channelSize)
	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, channelSize)

	g, gCtx := errgroup.WithContext(ctx)

	workerCount, _ := gocore.Config().GetInt("workerCount", 500)
	logger.Infof("Starting %d workers", workerCount)

	for i := 0; i < workerCount; i++ {
		workerID := i

		g.Go(func() error {
			return worker(gCtx, logger, utxoStore, workerID, utxoWrapperCh)
		})
	}

	var f *os.File

	// Read the UTXO data from the store
	f, err = os.Open(utxoFile)
	if err != nil {
		return errors.NewStorageError("failed to open file", err)
	}

	defer func() {
		_ = f.Close()
	}()

	reader := bufio.NewReader(f)

	var header fileformat.Header

	header, err = fileformat.ReadHeader(reader)
	if err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	if header.FileType() != fileformat.FileTypeUtxoSet {
		return errors.NewProcessingError("invalid file type: %s", header.FileType)
	}

	var hash chainhash.Hash

	if err = binary.Read(reader, binary.LittleEndian, &hash); err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	var height uint32
	if err = binary.Read(reader, binary.LittleEndian, &height); err != nil {
		return errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	// With UTXOSets, we also read the previous block hash before we start reading the UTXOs
	var previousBlockHash [32]byte

	_, err = reader.Read(previousBlockHash[:])
	if err != nil {
		return errors.NewProcessingError("couldn't read previous block hash from file", err)
	}

	var (
		txProcessed    uint64
		utxosProcessed uint64
	)

	g.Go(func() error {
		defer close(utxoWrapperCh)

	OUTER:
		for {
			select {
			case <-gCtx.Done():
				// Context canceled, stop reading lines
				logger.Infof("Context cancelled, stopping reading UTXOWrapper")
				return gCtx.Err()

			default:
				var utxoWrapper *utxopersister.UTXOWrapper

				utxoWrapper, err = utxopersister.NewUTXOWrapperFromReader(gCtx, reader)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break OUTER
					}

					logger.Errorf("Failed to read UTXO: %v", err)
					return errors.NewProcessingError("failed to read UTXO", err)
				}

				select {
				case <-gCtx.Done():
					logger.Infof("Context cancelled while sending UTXO to channel, stopping UTXO processing")
					return gCtx.Err()

				case utxoWrapperCh <- utxoWrapper:
					txProcessed++
					utxosProcessed += uint64(len(utxoWrapper.UTXOs))

					if txProcessed%1_000_000 == 0 {
						logger.Infof("Processed %16s transactions with %16s utxos", formatNumber(txProcessed), formatNumber(utxosProcessed))
					}
				}
			}
		}

		logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(txProcessed), formatNumber(utxosProcessed))

		return nil
	})

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("error in worker", err)
	}

	logger.Infof("All workers finished successfully")

	heightStr := fmt.Sprintf("%d\n", height)

	if err = blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(heightStr), bloboptions.WithFilename("lastProcessed")); err != nil {
		return errors.NewStorageError("failed to write height of %d to lastProcessed.dat", height, err)
	}

	return nil
}

// worker processes UTXOWrapper messages from the channel and stores them in the UTXO store.
func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store,
	id int, utxoWrapperCh <-chan *utxopersister.UTXOWrapper) error {
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Worker %d received stop signal: %v", id, ctx.Err())
			return nil

		case utxoWrapper, ok := <-utxoWrapperCh:
			if !ok {
				// Channel closed, stop the worker
				return nil
			}

			if err := processUTXO(ctx, store, utxoWrapper); err != nil {
				logger.Errorf("Worker %d failed to process UTXO: %v", id, err)
				return err
			}
		}
	}
}

// processUTXO processes a single UTXOWrapper and stores it in the UTXO store.
func processUTXO(ctx context.Context, store utxo.Store, utxoWrapper *utxopersister.UTXOWrapper) error {
	if utxoWrapper == nil {
		return nil
	}

	tx := &bt.Tx{}

	padded := utxopersister.PadUTXOsWithNil(utxoWrapper.UTXOs)

	for _, u := range padded {
		var output *bt.Output
		if u != nil {
			output = &bt.Output{}
			output.Satoshis = u.Value
			output.LockingScript = bscript.NewFromBytes(u.Script)
		}

		tx.Outputs = append(tx.Outputs, output)
	}

	if gocore.Config().GetBool("skipStore", false) {
		return nil
	}

	if _, err := store.Create(
		ctx,
		tx,
		utxoWrapper.Height,
		utxo.WithTXID(&utxoWrapper.TxID),
		utxo.WithSetCoinbase(utxoWrapper.Coinbase),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 0, BlockHeight: utxoWrapper.Height, SubtreeIdx: 0}),
	); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			return nil
		}

		return err
	}

	return nil
}

// formatNumber formats a number with commas as thousands separators.
func formatNumber(n uint64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]string, 0, len(in)+(len(in)-1)/3)

	for i, c := range in {
		if i > 0 && (len(in)-i)%3 == 0 {
			out = append(out, ",")
		}

		out = append(out, string(c))
	}

	return strings.Join(out, "")
}
