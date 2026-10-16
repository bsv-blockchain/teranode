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
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // This is used for internal profiling
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

const (
	errMsgFailedToReadUTXO = "failed to read UTXO set header"

	// checksumSidecarExtension matches the sidecar extension the blob file
	// store writes alongside every blob it stores (stores/blob/file), so a
	// snapshot produced by the UTXO persister and copied into inputDir
	// carries its checksum sidecar with it.
	checksumSidecarExtension = ".sha256"
)

// utxoSetTip identifies the block at which an imported UTXO set is complete.
// The BlockAssembler checkpoint is keyed to this tip, since block assembly may
// only safely resume on top of the block up to which the UTXO set (including
// coinbase UTXOs) has been fully materialised.
type utxoSetTip struct {
	hash   chainhash.Hash
	height uint32
}

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
	skipHeaders bool, skipUTXOs bool, force bool, skipChecksum bool) error {
	profilerAddr := appSettings.ProfilerAddr
	if profilerAddr != "" {
		go func() {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			util.RegisterGocoreStatsHandlers(nil)

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

	// The headers-file path is always derived (not only when processing headers):
	// the UTXO pass reads it to recover the authoritative coinbase transactions
	// so seeded coinbases keep their input. Existence is only enforced when the
	// header pass will actually consume it.
	headerFile := filepath.Join(inputDir, hash+".utxo-headers")
	utxoFile := filepath.Join(inputDir, hash+".utxo-set")

	if !skipHeaders {
		// Check the header file exists
		if _, err := os.Stat(headerFile); os.IsNotExist(err) {
			usage(fmt.Sprintf("Headers file %s does not exist", headerFile))
		}

		if !skipChecksum {
			if err := verifyChecksum(logger, headerFile); err != nil {
				return errors.NewProcessingError("checksum verification failed for headers file %s", headerFile, err)
			}
		}
	} else if !skipUTXOs && !skipChecksum {
		// The header pass itself is skipped, but processUTXOs still reads
		// headerFile back (via loadCoinbaseTxs) to recover coinbase inputs, so a
		// corrupted headers file must not be silently consumed just because
		// -skipHeaders was passed. Unlike the pass above, absence is fine here —
		// loadCoinbaseTxs already tolerates a missing headers file.
		if _, err := os.Stat(headerFile); err == nil {
			if err := verifyChecksum(logger, headerFile); err != nil {
				return errors.NewProcessingError("checksum verification failed for headers file %s", headerFile, err)
			}
		}
	}

	if !skipUTXOs {
		// Check the UTXO file exists
		if _, err := os.Stat(utxoFile); os.IsNotExist(err) {
			usage(fmt.Sprintf("UTXO file %s does not exist", utxoFile))
		}

		if !skipChecksum {
			if err := verifyChecksum(logger, utxoFile); err != nil {
				return errors.NewProcessingError("checksum verification failed for UTXO file %s", utxoFile, err)
			}
		}
	}

	if skipChecksum {
		logger.Warnf("-skipChecksum set: not verifying %s / %s against their checksum sidecars; the caller must have verified them", headerFile, utxoFile)
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

	// A single blockchain store handle is shared across header import, the
	// BlockAssembler-state pre-flight check and the final state write. Opening
	// it once (rather than per pass) is required for correctness: an in-memory
	// store is per-handle, and concurrent handles would race on schema creation.
	var (
		blockchainStore blockchain.Store
		headerErr       error
		utxoErr         error
		utxoTip         *utxoSetTip
	)

	if !skipHeaders || !skipUTXOs {
		var err error

		blockchainStore, err = newBlockchainStore(logger, appSettings)
		if err != nil {
			logger.Fatalf("Failed to create blockchain store: %v", err)
		}

		defer func() {
			_ = blockchainStore.Close(context.Background())
		}()
	}

	if !skipHeaders {
		wg.Add(1)

		go func() {
			defer wg.Done()

			logger.Infof("Processing headers...")
			logger.Infof("Blockchain store: %s", urlutil.Redact(appSettings.BlockChain.StoreURL))

			// Process the headers
			if err := processHeaders(ctx, logger, blockchainStore, headerFile); err != nil {
				headerErr = err
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
			logger.Infof("UTXO store: %s", urlutil.Redact(appSettings.UtxoStore.UtxoStore))

			// Process the UTXOs
			tip, err := processUTXOs(ctx, logger, appSettings, blockchainStore, utxoFile, headerFile, force)
			if err != nil {
				utxoErr = err
				logger.Errorf("Failed to process UTXOs: %v", err)

				return
			}

			utxoTip = tip

			logger.Infof("Finished processing UTXOs")
		}()
	}

	wg.Wait()

	if headerErr != nil {
		return errors.NewProcessingError("seeder failed to process headers", headerErr)
	}

	if utxoErr != nil {
		return errors.NewProcessingError("seeder failed to process UTXOs", utxoErr)
	}

	// Persist the BlockAssembler checkpoint only after the UTXO set has been
	// imported successfully. The checkpoint reflects the UTXO store's
	// completeness (block assembly is the sole creator of coinbase UTXOs), so it
	// must never be advanced when UTXOs were skipped or failed. When UTXOs were
	// skipped because lastProcessed.dat already existed, utxoTip is nil and any
	// existing checkpoint is left untouched. A failure here must be fatal: the
	// UTXO set is imported but the node would otherwise start with no checkpoint
	// and a plain re-run skips the (now-complete) UTXO pass, never retrying it.
	if !skipUTXOs && utxoTip != nil {
		if err := writeBlockAssemblerState(ctx, logger, blockchainStore, utxoTip); err != nil {
			return errors.NewProcessingError("seeder failed to set BlockAssembler state", err)
		}
	}

	return nil
}

// processHeaders reads the UTXO headers from a file and stores them in the blockchain store.
//
//nolint:gocognit // Requires refactoring to reduce cognitive complexity
func processHeaders(ctx context.Context, logger ulogger.Logger, blockchainStore blockchain.Store, headersFile string) error {
	var (
		f   *os.File
		err error
	)

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

	// Determine if this is V1 (without coinbase) or V2 (with coinbase)
	isV1 := header.IsUtxoHeadersV1()
	if isV1 {
		logger.Infof("Reading V1 utxo-headers (without coinbase transactions)")
	} else {
		logger.Infof("Reading V2 utxo-headers (with coinbase transactions)")
	}

	var blockIndex *utxopersister.BlockIndex

	for {
		blockIndex, err = utxopersister.NewUTXOHeaderFromReader(reader, isV1)
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
			CoinbaseTx:       blockIndex.CoinbaseTx,
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

	logger.Infof("FINISHED  %16s headers with %16s transactions", formatNumber(headersProcessed), formatNumber(txCount))

	return nil
}

// processUTXOs reads the UTXO set from a file and stores it in the UTXO store.
//
//nolint:gocognit // Requires refactoring to reduce cognitive complexity
func processUTXOs(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings, blockchainStore blockchain.Store, utxoFile string, headersFile string, force bool) (*utxoSetTip, error) {
	blockStoreURL := appSettings.Block.BlockStore
	if blockStoreURL == nil {
		return nil, errors.NewConfigurationError("blockstore URL not found in config")
	}

	var err error

	hashPrefix := -2

	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			panic(err)
		}
	}

	logger.Infof("Using blockStore at %s with hashPrefix %d", urlutil.Redact(blockStoreURL), hashPrefix)

	blockStore, err := blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewStorageError("failed to create blockStore", err)
	}

	if !force {
		var exists bool

		exists, err = blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
		if err != nil {
			return nil, errors.NewStorageError("failed to check if lastProcessed.dat exists", err)
		}

		if exists {
			// The store has already been fully seeded. Skip idempotently and
			// leave any existing BlockAssembler checkpoint untouched (returning a
			// nil tip signals the caller not to write state).
			logger.Errorf("lastProcessed.dat exists, skipping UTXOs")
			return nil, nil
		}

		// No seed marker, but if a BlockAssembler checkpoint already exists then
		// this UTXO store is owned by a running/seeded assembler. Overwriting its
		// UTXO set would corrupt live state, so refuse unless explicitly forced.
		var stateExists bool

		stateExists, err = blockAssemblerStateExists(ctx, blockchainStore)
		if err != nil {
			return nil, errors.NewStorageError("failed to check BlockAssembler state", err)
		}

		if stateExists {
			return nil, errors.NewProcessingError("BlockAssembler state already exists in the blockchain store — refusing to seed UTXOs over a store already owned by a block assembler; use -force to override")
		}
	}

	// Seeding-only store tuning. The default "data" still fsyncs each external
	// blob's content but skips the per-blob directory fsync: after a host crash
	// a blob is either complete or its name is gone, and a re-run rewrites the
	// missing ones. One syncfs of the external store before lastProcessed.dat is
	// written makes the directory entries of the completed seed durable. See
	// seedingExternalStoreURL for what is (not) changed.
	fsyncMode, _ := gocore.Config().Get("seeder_externalStoreFsyncMode", "data")

	storeURL, syncPath, err := seedingExternalStoreURL(appSettings.UtxoStore.UtxoStore, fsyncMode)
	if err != nil {
		return nil, err
	}

	appSettings.UtxoStore.UtxoStore = storeURL

	if resumeUnsafeAfterHostCrash(storeURL) {
		logger.Warnf("External store fsyncMode is none: if the host crashes during this seed, wipe the UTXO store and re-seed; a plain re-run can keep partially written blobs")
	}

	externalStoreConcurrency, _ := gocore.Config().GetInt("seeder_externalStoreConcurrency", 256)
	appSettings.UtxoStore.ExternalStoreConcurrency = externalStoreConcurrency

	logger.Infof("Using utxostore at %s with external store concurrency %d", urlutil.Redact(appSettings.UtxoStore.UtxoStore), externalStoreConcurrency)

	var utxoStore utxo.Store

	utxoStore, err = utxofactory.NewStore(ctx, logger, appSettings, "seeder", false)
	if err != nil {
		return nil, errors.NewStorageError("failed to create utxostore", err)
	}

	// Probe the final sync now (the store has created its directory), so an
	// unsupported platform or an unusable path fails before hours of import
	// rather than just before lastProcessed.dat.
	if syncPath != "" {
		if err = syncFilesystem(syncPath); err != nil {
			return nil, errors.NewStorageError("cannot sync the external store filesystem at %s", syncPath, err)
		}
	}

	// Recover the authoritative coinbase transactions from the V2 utxo-headers
	// file. The UTXO set only records outputs, so a coinbase rebuilt from it
	// alone has no input; these let processUTXO store the real coinbase instead.
	// Best-effort: an absent/legacy/unreadable file just means coinbases keep
	// the output-only representation rather than failing the whole import.
	coinbaseTxs, err := loadCoinbaseTxs(logger, headersFile)
	if err != nil {
		logger.Warnf("[processUTXOs] could not load coinbase transactions from %s; coinbases will be stored without their input: %v", headersFile, err)

		coinbaseTxs = map[chainhash.Hash]*bt.Tx{}
	}

	logger.Infof("[processUTXOs] loaded %s coinbase transactions for input restoration", formatNumber(uint64(len(coinbaseTxs))))

	// Read the tip this UTXO set is complete at. importUTXOSet re-opens the
	// file for each of its passes.
	var (
		hash   chainhash.Hash
		height uint32
	)

	if err = func() error {
		f, _, tipHash, tipHeight, openErr := openUTXOSetFile(utxoFile)
		if openErr != nil {
			return openErr
		}

		hash, height = tipHash, tipHeight

		return f.Close()
	}(); err != nil {
		return nil, err
	}

	// Multi-record txs go through a much slower create path, so they get their
	// own pass (see importUTXOSet). Their workers only wait on the external
	// store semaphore beyond its size, so the default matches it.
	defaultMultiRecordWorkers := externalStoreConcurrency
	if defaultMultiRecordWorkers <= 0 {
		defaultMultiRecordWorkers = 1024
	}

	channelSize, _ := gocore.Config().GetInt("channelSize", 1000)
	// Each worker blocks until its create has been written, so workerCount is
	// the number of txs in flight. It must comfortably exceed the store batch
	// size (utxostore_storeBatcherSize) for batches to fill instead of flushing
	// on the batch timer; 16384 was the measured knee against Aerospike.
	workerCount, _ := gocore.Config().GetInt("workerCount", 16384)
	multiRecordWorkerCount, _ := gocore.Config().GetInt("multiRecordWorkerCount", defaultMultiRecordWorkers)

	opts := importOptions{
		workerCount:            workerCount,
		multiRecordWorkerCount: multiRecordWorkerCount,
		channelSize:            channelSize,
		utxoBatchSize:          appSettings.UtxoStore.UtxoBatchSize,
		skipStore:              gocore.Config().GetBool("skipStore", false),
		coinbaseTxs:            coinbaseTxs,
	}

	if err = importUTXOSet(ctx, logger, utxoStore, utxoFile, opts); err != nil {
		return nil, err
	}

	if syncPath != "" {
		logger.Infof("Syncing the external store filesystem at %s so it is durable before marking the seed complete", syncPath)

		if err = syncFilesystem(syncPath); err != nil {
			return nil, errors.NewStorageError("failed to sync external store filesystem", err)
		}
	}

	heightStr := fmt.Sprintf("%d\n", height)

	if err = blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(heightStr), bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()); err != nil {
		return nil, errors.NewStorageError("failed to write height of %d to lastProcessed.dat", height, err)
	}

	return &utxoSetTip{hash: hash, height: height}, nil
}

// maxWorkerCount caps each pass's worker goroutines. Far above any useful
// value (16384 was the measured knee), it only catches a mistyped setting that
// would spawn millions of goroutines whose stacks the GC scans every cycle.
const maxWorkerCount = 1 << 20

// importOptions configures importUTXOSet.
type importOptions struct {
	workerCount            int // workers for the single-record pass
	multiRecordWorkerCount int // workers for the multi-record pass
	channelSize            int
	utxoBatchSize          int // store outputs-per-record limit; <= 0 imports in one pass
	skipStore              bool
	coinbaseTxs            map[chainhash.Hash]*bt.Tx
}

// importUTXOSet imports every record of the .utxo-set file at utxoFile into
// store, returning once all are stored or the first error occurred.
//
// Txs that span multiple store records take a much slower create path (lock
// record, external blob, several round trips each). Sharing one worker pool
// with them lets the pool fill up with slow multi-record creates and starves
// the fast path, so the file is read in two concurrent, independent passes:
// one creating only single-record txs, one only multi-record txs. Each pass has
// its own reader, channel and workers, so neither can block the other, and the
// import takes as long as the slower pass. Both run in one errgroup: the first
// error in either cancels both, and the seed is then re-run from the start.
func importUTXOSet(ctx context.Context, logger ulogger.Logger, store utxo.Store, utxoFile string, opts importOptions) error {
	if opts.workerCount < 1 || opts.multiRecordWorkerCount < 1 || opts.workerCount > maxWorkerCount || opts.multiRecordWorkerCount > maxWorkerCount {
		return errors.NewConfigurationError("workerCount (%d) and multiRecordWorkerCount (%d) must both be between 1 and %d", opts.workerCount, opts.multiRecordWorkerCount, maxWorkerCount)
	}

	// A named pipe delivers each byte to one reader only, so two passes would
	// split the stream between them and each would parse the other's gaps as
	// records. Refuse it before reading anything.
	if opts.utxoBatchSize > 0 {
		if info, err := os.Stat(utxoFile); err == nil && info.Mode()&os.ModeNamedPipe != 0 {
			return errors.NewConfigurationError("%s is a named pipe, but utxostore_utxoBatchSize %d makes the import read it in two concurrent passes; a pipe needs a single pass (utxostore_utxoBatchSize 0, on a store that accepts it) or the file unpacked to disk", utxoFile, opts.utxoBatchSize)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	startPass := func(name string, workerCount int, accept func(maxIndex uint32) bool) {
		g.Go(func() error {
			f, reader, _, _, err := openUTXOSetFile(utxoFile)
			if err != nil {
				return err
			}

			defer func() {
				_ = f.Close()
			}()

			logger.Infof("[%s pass] starting %d workers, channel size %d", name, workerCount, opts.channelSize)

			frameCh := make(chan []byte, opts.channelSize)

			pg, pCtx := errgroup.WithContext(gCtx)

			for i := 0; i < workerCount; i++ {
				workerID := i

				pg.Go(func() error {
					return worker(pCtx, logger, store, workerID, frameCh, opts.coinbaseTxs, opts.skipStore)
				})
			}

			pg.Go(func() error {
				return readUTXOFrames(pCtx, logger, f, reader, frameCh, name, accept)
			})

			return pg.Wait()
		})
	}

	if opts.utxoBatchSize > 0 {
		startPass("single-record", opts.workerCount, func(maxIndex uint32) bool {
			return !spansMultipleRecords(maxIndex, opts.utxoBatchSize)
		})
		startPass("multi-record", opts.multiRecordWorkerCount, func(maxIndex uint32) bool {
			return spansMultipleRecords(maxIndex, opts.utxoBatchSize)
		})
	} else {
		startPass("all", opts.workerCount, nil)
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("error in worker", err)
	}

	// Workers stop on cancellation without an error, possibly after the
	// readers finished cleanly, so buffered records may have been dropped.
	if err := ctx.Err(); err != nil {
		return errors.NewProcessingError("UTXO import cancelled", err)
	}

	logger.Infof("All workers finished successfully")

	return nil
}

// openUTXOSetFile opens a .utxo-set file, validates its header and returns a
// reader positioned at the first record, plus the block hash and height the
// set is complete at.
func openUTXOSetFile(utxoFile string) (*os.File, *bufio.Reader, chainhash.Hash, uint32, error) {
	var (
		hash   chainhash.Hash
		height uint32
	)

	f, err := os.Open(utxoFile)
	if err != nil {
		return nil, nil, hash, 0, errors.NewStorageError("failed to open file", err)
	}

	fail := func(err error) (*os.File, *bufio.Reader, chainhash.Hash, uint32, error) {
		_ = f.Close()
		return nil, nil, hash, 0, err
	}

	reader := bufio.NewReader(f)

	header, err := fileformat.ReadHeader(reader)
	if err != nil {
		return fail(errors.NewProcessingError(errMsgFailedToReadUTXO, err))
	}

	if header.FileType() != fileformat.FileTypeUtxoSet {
		return fail(errors.NewProcessingError("invalid file type: %s", header.FileType()))
	}

	// block hash (32) + height (4) + previous block hash (32)
	var preamble [68]byte
	if _, err = io.ReadFull(reader, preamble[:]); err != nil {
		return fail(errors.NewProcessingError(errMsgFailedToReadUTXO, err))
	}

	copy(hash[:], preamble[:32])
	height = binary.LittleEndian.Uint32(preamble[32:36])

	return f, reader, hash, height, nil
}

// resumeUnsafeAfterHostCrash reports whether utxoStoreURL has a file:// external
// store with fsyncMode=none. Such a store can keep a zero-length or partial
// blob across a host crash, and a re-run treats it as already written.
func resumeUnsafeAfterHostCrash(utxoStoreURL *url.URL) bool {
	externalURL, err := url.Parse(utxoStoreURL.Query().Get("externalStore"))
	if err != nil || externalURL.Scheme != "file" {
		return false
	}

	return strings.ToLower(externalURL.Query().Get("fsyncMode")) == "none"
}

// seedingExternalStoreURL returns utxoStoreURL with its external blob store
// switched to fsyncMode when that store is a file store whose fsyncMode the
// operator has not set; any other URL is returned unchanged. syncPath is the
// external store's directory when its effective fsync mode is weaker than
// "full" (the caller must sync that filesystem before declaring the seed
// done), and "" otherwise. The input URL is never mutated.
func seedingExternalStoreURL(utxoStoreURL *url.URL, fsyncMode string) (*url.URL, string, error) {
	// Case-insensitive, like the file blob store's own parseFsyncMode.
	fsyncMode = strings.ToLower(fsyncMode)

	switch fsyncMode {
	case "", "full", "data", "none":
	default:
		return nil, "", errors.NewConfigurationError("invalid seeder_externalStoreFsyncMode %q (must be full|data|none)", fsyncMode)
	}

	out := *utxoStoreURL

	q := out.Query()

	raw := q.Get("externalStore")
	if raw == "" {
		return &out, "", nil
	}

	externalURL, err := url.Parse(raw)
	if err != nil {
		// raw is a whole blob store URL and can carry a working credential, and
		// url.Parse's error quotes it, so neither goes into the message.
		return nil, "", errors.NewConfigurationError("invalid externalStore URL", urlutil.ParseErrorReason(err))
	}

	if externalURL.Scheme != "file" {
		return &out, "", nil
	}

	eq := externalURL.Query()

	if eq.Get("fsyncMode") == "" && fsyncMode != "" && fsyncMode != "full" {
		eq.Set("fsyncMode", fsyncMode)
		externalURL.RawQuery = eq.Encode()
		q.Set("externalStore", externalURL.String())
		out.RawQuery = q.Encode()
	}

	if effective := strings.ToLower(externalURL.Query().Get("fsyncMode")); effective == "" || effective == "full" {
		return &out, "", nil
	}

	// Resolve the directory the same way the file blob store does.
	if externalURL.Host == "." {
		return &out, strings.TrimPrefix(externalURL.Path, "/"), nil
	}

	return &out, externalURL.Path, nil
}

// readUTXOFrames cuts the UTXOWrapper records from reader (backed by f) into
// raw frames (utxopersister.ReadUTXOWrapperFrame) and sends them to frameCh
// until the record stream is exhausted, then closes the channel. Decoding is
// left to the workers: with a single reader decoding every record, the reader
// was the import's ceiling.
//
// The EOF detection (errors.Is(err, io.EOF)) cannot tell a clean
// end-of-records boundary apart from a file truncated mid-record: both surface
// as an error whose message contains "EOF" (a real truncation trips
// io.ErrUnexpectedEOF, whose message "unexpected EOF" contains "EOF" as a
// substring), so errors.Is treats them identically via this package's
// substring-matching Is fallback. To make truncation detection exact, every
// snapshot file carries a trailing 16-byte footer recording the
// txCount/utxoCount it was written with; once the read loop ends for any
// reason, that footer is compared against what was actually read, and a
// mismatch is reported as an error instead of silently treated as success.
// The footer is taken from the stream when the records end exactly at it
// (utxopersister.ErrRecordBoundary), so this reader can validate a pipe;
// otherwise it is read by seeking (utxopersister.GetFooter). A stream that
// cannot seek and did not end at a footer is reported as truncated. Whether
// the seeder as a whole can take a pipe is decided in importUTXOSet, which
// refuses one when the import would need two passes.
//
// Only frames for which accept returns true (all, when accept is nil) are
// sent; accept gets the record's highest output index. Every record is still
// read and counted, so each pass validates the whole file against the footer.
// pass names the pass in log lines.
func readUTXOFrames(ctx context.Context, logger ulogger.Logger, f *os.File, reader *bufio.Reader, frameCh chan<- []byte,
	pass string, accept func(maxIndex uint32) bool) error {
	defer close(frameCh)

	var (
		txProcessed    uint64
		utxosProcessed uint64
		txsSent        uint64
		scratch        []byte
		footerBytes    []byte
		endErr         error
	)

	for {
		if err := ctx.Err(); err != nil {
			logger.Infof("Context cancelled, stopping reading UTXOWrapper")
			return err
		}

		// Read into a reused buffer; only records this pass sends get their own
		// exactly-sized copy, so skipped records cost no allocation.
		frame, maxIndex, err := utxopersister.ReadUTXOWrapperFrame(reader, scratch)
		if err != nil {
			// The records end where exactly the 16-byte footer remains, and the
			// reader hands those bytes back. Keeping them means a stream that
			// cannot seek, such as a pipe, is validated just like a file.
			var boundary *utxopersister.ErrRecordBoundary
			if errors.As(err, &boundary) {
				footerBytes = boundary.FooterBytes[:]
				break
			}

			if errors.Is(err, io.EOF) {
				endErr = err
				break
			}

			logger.Errorf("Failed to read UTXO: %v", err)

			return errors.NewProcessingError("failed to read UTXO", err)
		}

		scratch = frame

		if err = checkOutputIndex(maxIndex); err != nil {
			return errors.NewProcessingError("invalid UTXO record %d", txProcessed+1, err)
		}

		txProcessed++
		utxosProcessed += uint64(utxopersister.FrameUTXOCount(frame))

		if txProcessed%1_000_000 == 0 {
			logger.Infof("[%s pass] read %16s transactions with %16s utxos, %s sent to workers", pass, formatNumber(txProcessed), formatNumber(utxosProcessed), formatNumber(txsSent))
		}

		if accept != nil && !accept(maxIndex) {
			continue
		}

		select {
		case <-ctx.Done():
			logger.Infof("Context cancelled while sending UTXO to channel, stopping UTXO processing")
			return ctx.Err()

		case frameCh <- bytes.Clone(frame):
			txsSent++
		}
	}

	// The read loop above only knows the record stream ended - it cannot
	// distinguish a clean footer boundary from a truncated file (see the
	// doc comment above). Validate against the file's own footer counts so
	// a genuinely truncated snapshot is reported as an error rather than
	// silently accepted.
	var (
		expectedTxCount, expectedUTXOCount uint64
		footerErr                          error
	)

	if footerBytes != nil {
		expectedTxCount, expectedUTXOCount, footerErr = utxopersister.DecodeFooter(footerBytes)
		if footerErr != nil {
			return errors.NewProcessingError("failed to read snapshot footer", footerErr)
		}
	} else {
		expectedTxCount, expectedUTXOCount, footerErr = utxopersister.GetFooter(f)
		if footerErr != nil {
			// The stream did not end at a footer, and there is none to seek to,
			// as on a pipe. Either way the snapshot is incomplete, so say that
			// rather than surfacing the seek error.
			return errors.NewProcessingError(
				"snapshot file truncated: the records ended without a footer after %d transactions/%d UTXOs (%v), and the footer could not be read by seeking",
				txProcessed, utxosProcessed, endErr, footerErr)
		}
	}

	if expectedTxCount != txProcessed || expectedUTXOCount != utxosProcessed {
		return errors.NewProcessingError(
			"snapshot file truncated: expected %d transactions/%d UTXOs, processed %d/%d",
			expectedTxCount, expectedUTXOCount, txProcessed, utxosProcessed)
	}

	logger.Infof("[%s pass] FINISHED %16s transactions with %16s utxos, %s sent to workers", pass, formatNumber(txProcessed), formatNumber(utxosProcessed), formatNumber(txsSent))

	return nil
}

// checkOutputIndex rejects an output index from the (untrusted) snapshot file
// that no valid transaction can have. processUTXO sizes a tx's outputs by it,
// so an unchecked 0xFFFFFFFF would wrap to an empty slice and panic, and one
// just below would attempt a ~32 GiB allocation.
func checkOutputIndex(maxIndex uint32) error {
	if maxIndex > utxopersister.MaxOutputIndex {
		return errors.NewProcessingError("output index %d exceeds the maximum %d a valid transaction can have", maxIndex, utxopersister.MaxOutputIndex)
	}

	return nil
}

// spansMultipleRecords reports whether the UTXO store will split a tx whose
// highest unspent output index is maxIndex over more than one record. The
// split is by padded output count (highest unspent index + 1), matching
// PadUTXOsWithNil, not by the number of unspent outputs.
func spansMultipleRecords(maxIndex uint32, utxoBatchSize int) bool {
	return utxoBatchSize > 0 && int64(maxIndex) >= int64(utxoBatchSize)
}

// worker decodes the frames from the channel and stores them in the UTXO store.
func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store,
	id int, frameCh <-chan []byte, coinbaseTxs map[chainhash.Hash]*bt.Tx, skipStore bool) error {
	// A plain receive rather than a select on ctx.Done(): with thousands of
	// workers, locking the shared Done channel on every item dominated CPU. The
	// reader closes frameCh when it finishes or ctx is cancelled, which wakes
	// idle workers. A worker that stops on cancellation drops the remaining
	// buffered records; importUTXOSet reports that as an error.
	for frame := range frameCh {
		if ctx.Err() != nil {
			logger.Infof("Worker %d received stop signal: %v", id, ctx.Err())
			return nil
		}

		utxoWrapper, err := utxopersister.DecodeUTXOWrapperFrame(frame)
		if err != nil {
			logger.Errorf("Worker %d failed to decode UTXO: %v", id, err)
			return errors.NewProcessingError("failed to decode UTXO", err)
		}

		if err = processUTXO(ctx, store, utxoWrapper, coinbaseTxs, skipStore); err != nil {
			logger.Errorf("Worker %d failed to process UTXO: %v", id, err)
			return err
		}
	}

	return nil
}

// The two WithSetCoinbase options, built once instead of a closure per tx.
var (
	setCoinbaseTrue  = utxo.WithSetCoinbase(true)
	setCoinbaseFalse = utxo.WithSetCoinbase(false)
)

func setCoinbaseOption(coinbase bool) utxo.CreateOption {
	if coinbase {
		return setCoinbaseTrue
	}

	return setCoinbaseFalse
}

// processUTXO processes a single UTXOWrapper and stores it in the UTXO store.
// coinbaseTxs maps coinbase txid to the authoritative coinbase transaction
// recovered from the V2 utxo-headers file; it may be empty. skipStore builds the
// transaction but does not write it (a debugging knob, read once by the caller
// because gocore config lookups are too expensive to run per transaction).
func processUTXO(ctx context.Context, store utxo.Store, utxoWrapper *utxopersister.UTXOWrapper, coinbaseTxs map[chainhash.Hash]*bt.Tx, skipStore bool) error {
	if utxoWrapper == nil {
		return nil
	}

	// Same layout as utxopersister.PadUTXOsWithNil (each unspent output at its
	// index, spent positions nil), but the outputs and their scripts come from
	// one array each instead of two allocations per output: at mainnet scale
	// these allocations drove GC to half the process.
	var maxIndex uint32
	for _, u := range utxoWrapper.UTXOs {
		maxIndex = max(maxIndex, u.Index)
	}

	// The reader already rejects such records; checked here too because this
	// function sizes the outputs from maxIndex and must be safe on its own.
	if err := checkOutputIndex(maxIndex); err != nil {
		return err
	}

	outputs := make([]bt.Output, len(utxoWrapper.UTXOs))
	scripts := make([]bscript.Script, len(utxoWrapper.UTXOs))
	tx := &bt.Tx{Outputs: make([]*bt.Output, int(maxIndex)+1)}

	for i, u := range utxoWrapper.UTXOs {
		scripts[i] = bscript.Script(u.Script)
		outputs[i] = bt.Output{Satoshis: u.Value, LockingScript: &scripts[i]}
		tx.Outputs[u.Index] = &outputs[i]
	}

	// A coinbase rebuilt from the UTXO set alone has no input (the coinbase
	// scriptSig is input data, not a UTXO) and hashes to the wrong txid. When the
	// real coinbase is available, restore its input so the stored transaction is
	// faithful and hashes back to the txid the block commits to.
	if utxoWrapper.Coinbase {
		restoreCoinbaseInput(tx, coinbaseTxs[utxoWrapper.TxID], &utxoWrapper.TxID)
	}

	if skipStore {
		return nil
	}

	if _, _, err := store.SpendAndCreate(
		ctx,
		tx,
		utxoWrapper.Height,
		utxo.WithCreateOnly(),
		utxo.WithTXID(&utxoWrapper.TxID),
		setCoinbaseOption(utxoWrapper.Coinbase),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 0, BlockHeight: utxoWrapper.Height, SubtreeIdx: 0}),
	); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			return nil
		}

		return err
	}

	return nil
}

// restoreCoinbaseInput copies the version, lock time and coinbase input from the
// authoritative coinbase transaction (recovered from a V2 utxo-headers file)
// onto the output-only transaction rebuilt from the UTXO set.
//
// It only does so when the result is byte-faithful to the real coinbase — every
// output still unspent and present — for two reasons:
//   - Once a transaction has inputs, the UTXO store derives each output's UTXO
//     hash from the transaction's own hash rather than the override txid; a
//     non-faithful reconstruction would persist UTXO hashes that no future spend
//     could ever match.
//   - Serialising a transaction that has input(s) and nil output holes panics.
//
// When the reconstruction is not faithful (a coinbase output was already spent),
// tx is left untouched, preserving the safe output-only representation.
func restoreCoinbaseInput(tx *bt.Tx, coinbaseTx *bt.Tx, txid *chainhash.Hash) {
	if coinbaseTx == nil || len(coinbaseTx.Inputs) == 0 {
		return
	}

	// A faithful reconstruction has exactly the coinbase's outputs, all present.
	// A differing count means a trailing output was spent; a nil hole means an
	// earlier output was spent.
	if len(tx.Outputs) != len(coinbaseTx.Outputs) {
		return
	}

	for _, o := range tx.Outputs {
		if o == nil {
			return
		}
	}

	origVersion, origLockTime, origInputs := tx.Version, tx.LockTime, tx.Inputs

	tx.Version = coinbaseTx.Version
	tx.LockTime = coinbaseTx.LockTime
	tx.Inputs = coinbaseTx.Inputs

	// Confirm the outputs also match (values and scripts), so the stored tx truly
	// hashes to the coinbase txid. Otherwise revert to the output-only form.
	if !tx.TxIDChainHash().IsEqual(txid) {
		tx.Version, tx.LockTime, tx.Inputs = origVersion, origLockTime, origInputs
	}
}

// loadCoinbaseTxs reads the V2 utxo-headers file and returns a map from coinbase
// txid to the coinbase transaction. Legacy V1 files carry no coinbase
// transactions and yield an empty map, as does an absent file: coinbase input
// restoration is best-effort and never blocks the UTXO import.
func loadCoinbaseTxs(logger ulogger.Logger, headersFile string) (map[chainhash.Hash]*bt.Tx, error) {
	coinbaseTxs := make(map[chainhash.Hash]*bt.Tx)

	if headersFile == "" {
		return coinbaseTxs, nil
	}

	f, err := os.Open(headersFile)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Infof("[loadCoinbaseTxs] headers file %s not present; coinbase inputs will not be restored", headersFile)
			return coinbaseTxs, nil
		}

		return nil, errors.NewStorageError("failed to open headers file", err)
	}

	defer func() {
		_ = f.Close()
	}()

	reader := bufio.NewReader(f)

	header, err := fileformat.ReadHeader(reader)
	if err != nil {
		return nil, errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	if header.FileType() != fileformat.FileTypeUtxoHeaders {
		return nil, errors.NewProcessingError("invalid file type: %s", header.FileType())
	}

	if header.IsUtxoHeadersV1() {
		logger.Infof("[loadCoinbaseTxs] V1 utxo-headers carry no coinbase transactions; coinbase inputs will not be restored")
		return coinbaseTxs, nil
	}

	// Skip the tip hash (32 bytes) and height (4 bytes) preamble that precedes
	// the per-block BlockIndex entries.
	var preamble [36]byte
	if _, err = io.ReadFull(reader, preamble[:]); err != nil {
		return nil, errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	for {
		var blockIndex *utxopersister.BlockIndex

		blockIndex, err = utxopersister.NewUTXOHeaderFromReader(reader, false)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, errors.NewProcessingError("failed to read UTXO header", err)
		}

		if blockIndex.CoinbaseTx != nil {
			coinbaseTxs[*blockIndex.CoinbaseTx.TxIDChainHash()] = blockIndex.CoinbaseTx
		}
	}

	return coinbaseTxs, nil
}

// newBlockchainStore opens the blockchain store in bulk-import (seeder) mode.
// A single shared handle is used for header import, the BlockAssembler-state
// pre-flight check and the final state write.
func newBlockchainStore(logger ulogger.Logger, appSettings *settings.Settings) (blockchain.Store, error) {
	blockchainStoreURL := appSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return nil, errors.NewConfigurationError("blockchain store URL not found in config")
	}

	q := blockchainStoreURL.Query()
	q.Set("seeder", "true")
	blockchainStoreURL.RawQuery = q.Encode()

	return blockchain.NewStore(logger, blockchainStoreURL, appSettings)
}

// blockAssemblerStateExists reports whether a BlockAssembler checkpoint is
// already persisted in the blockchain store. A missing checkpoint is the normal
// state of an unseeded node and is not treated as an error.
func blockAssemblerStateExists(ctx context.Context, store blockchain.Store) (bool, error) {
	data, err := store.GetState(ctx, blockassembly.StateKey)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			return false, nil
		}

		return false, err
	}

	return len(data) > 0, nil
}

// writeBlockAssemblerState persists the BlockAssembler checkpoint at the given
// utxo-set tip, so block assembly can resume on top of the seeded chain. The
// tip's block header is looked up from the blockchain store; if it is absent the
// seed is inconsistent and we refuse to write, rather than let block assembly
// adopt a header the node has no record of.
func writeBlockAssemblerState(ctx context.Context, logger ulogger.Logger, store blockchain.Store, tip *utxoSetTip) error {
	header, _, err := store.GetBlockHeader(ctx, &tip.hash)
	if err != nil {
		return errors.NewProcessingError("cannot set BlockAssembler state: header for utxo-set tip %s (height %d) not found in blockchain store", tip.hash.String(), tip.height, err)
	}

	if err = store.SetState(ctx, blockassembly.StateKey, blockassembly.EncodeState(header, tip.height)); err != nil {
		return errors.NewStorageError("failed to set BlockAssembler state", err)
	}

	logger.Infof("Set BlockAssembler state to utxo-set tip %s at height %d", tip.hash.String(), tip.height)

	return nil
}

// verifyChecksum checks a snapshot file against its "<file>.sha256" checksum
// sidecar, matching the sidecar format the blob file store (stores/blob/file)
// writes alongside every blob: hex-encoded SHA-256, optionally followed by
// whitespace and the filename (the standard sha256sum layout).
//
// If no sidecar is present — e.g. an older snapshot predating checksum
// sidecars, or one fetched from a source that doesn't produce one — the
// import proceeds with only a warning logged: mandating a sidecar for every
// possible snapshot source is not realistic. If a sidecar IS present but
// doesn't match the file's actual content, the import is refused: a
// corrupted-but-count-consistent file must never be silently imported.
func verifyChecksum(logger ulogger.Logger, filePath string) error {
	sidecarPath := filePath + checksumSidecarExtension

	// maxSidecarSize bounds the sidecar read: a genuine sidecar is a hex digest
	// plus an optional filename, well under 1KB. Anything larger is not a
	// sidecar (e.g. the snapshot file itself, copied to the wrong name), and
	// reading it in full would pull an arbitrarily large file into memory.
	const maxSidecarSize = 4096

	f, err := os.Open(sidecarPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warnf("[verifyChecksum] no checksum sidecar %s found for %s; proceeding without checksum verification", sidecarPath, filePath)
			return nil
		}

		return errors.NewStorageError("failed to read checksum sidecar %s", sidecarPath, err)
	}

	sidecar, err := io.ReadAll(io.LimitReader(f, maxSidecarSize))
	_ = f.Close()

	if err != nil {
		return errors.NewStorageError("failed to read checksum sidecar %s", sidecarPath, err)
	}

	fields := strings.Fields(string(sidecar))
	if len(fields) == 0 {
		return errors.NewProcessingError("checksum sidecar %s is empty", sidecarPath)
	}

	expected := strings.ToLower(fields[0])

	if len(expected) != sha256.Size*2 {
		return errors.NewProcessingError("checksum sidecar %s is malformed: %q is not a hex-encoded SHA-256 digest", sidecarPath, fields[0])
	}

	if _, err := hex.DecodeString(expected); err != nil {
		return errors.NewProcessingError("checksum sidecar %s is malformed: %q is not a hex-encoded SHA-256 digest", sidecarPath, fields[0])
	}

	// The standard sha256sum layout names the file the checksum belongs to as
	// the second field. When present, it must match filePath's own basename —
	// otherwise the sidecar was paired with the wrong snapshot, and reporting
	// that as a checksum mismatch would misdirect the operator.
	if len(fields) > 1 && fields[1] != filepath.Base(filePath) {
		return errors.NewProcessingError("checksum sidecar %s belongs to %q, not to %s", sidecarPath, fields[1], filepath.Base(filePath))
	}

	if st, statErr := os.Stat(filePath); statErr == nil {
		logger.Infof("[verifyChecksum] verifying %s (%s bytes) against %s; this reads the whole file", filePath, formatNumber(uint64(st.Size())), sidecarPath)
	}

	sf, err := os.Open(filePath)
	if err != nil {
		return errors.NewStorageError("failed to open %s for checksum verification", filePath, err)
	}

	defer func() {
		_ = sf.Close()
	}()

	h := sha256.New()

	if _, err = io.Copy(h, sf); err != nil {
		return errors.NewStorageError("failed to read %s for checksum verification", filePath, err)
	}

	actual := hex.EncodeToString(h.Sum(nil))

	if actual != expected {
		return errors.NewProcessingError("checksum mismatch for %s: sidecar %s says %s, actual content hashes to %s (file may be corrupted)",
			filePath, sidecarPath, expected, actual)
	}

	logger.Infof("[verifyChecksum] checksum verified for %s against %s", filePath, sidecarPath)

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
