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
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // This is used for internal profiling
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
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

const (
	errMsgFailedToReadUTXO = "failed to read UTXO set header"
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
	skipHeaders bool, skipUTXOs bool, force bool) error {
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
	}

	if !skipUTXOs {
		// Check the UTXO file exists
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
			logger.Infof("Blockchain store: %s", appSettings.BlockChain.StoreURL)

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
			logger.Infof("UTXO store: %s", appSettings.UtxoStore.UtxoStore.String())

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

	logger.Infof("Using blockStore at %s with hashPrefix %d", blockStoreURL, hashPrefix)

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

	logger.Infof("Using utxostore at %s", appSettings.UtxoStore.UtxoStore)

	var utxoStore utxo.Store

	utxoStore, err = utxofactory.NewStore(ctx, logger, appSettings, "seeder", false)
	if err != nil {
		return nil, errors.NewStorageError("failed to create utxostore", err)
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

	channelSize, _ := gocore.Config().GetInt("channelSize", 1000)

	logger.Infof("Using channel size of %d", channelSize)
	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, channelSize)

	g, gCtx := errgroup.WithContext(ctx)

	workerCount, _ := gocore.Config().GetInt("workerCount", 500)
	logger.Infof("Starting %d workers", workerCount)

	for i := 0; i < workerCount; i++ {
		workerID := i

		g.Go(func() error {
			return worker(gCtx, logger, utxoStore, workerID, utxoWrapperCh, coinbaseTxs)
		})
	}

	var f *os.File

	// Read the UTXO data from the store
	f, err = os.Open(utxoFile)
	if err != nil {
		return nil, errors.NewStorageError("failed to open file", err)
	}

	defer func() {
		_ = f.Close()
	}()

	reader := bufio.NewReader(f)

	var header fileformat.Header

	header, err = fileformat.ReadHeader(reader)
	if err != nil {
		return nil, errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	if header.FileType() != fileformat.FileTypeUtxoSet {
		return nil, errors.NewProcessingError("invalid file type: %s", header.FileType)
	}

	var hash chainhash.Hash

	if err = binary.Read(reader, binary.LittleEndian, &hash); err != nil {
		return nil, errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	var height uint32
	if err = binary.Read(reader, binary.LittleEndian, &height); err != nil {
		return nil, errors.NewProcessingError(errMsgFailedToReadUTXO, err)
	}

	// With UTXOSets, we also read the previous block hash before we start reading the UTXOs
	var previousBlockHash [32]byte

	_, err = reader.Read(previousBlockHash[:])
	if err != nil {
		return nil, errors.NewProcessingError("couldn't read previous block hash from file", err)
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
		return nil, errors.NewProcessingError("error in worker", err)
	}

	logger.Infof("All workers finished successfully")

	heightStr := fmt.Sprintf("%d\n", height)

	if err = blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(heightStr), bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()); err != nil {
		return nil, errors.NewStorageError("failed to write height of %d to lastProcessed.dat", height, err)
	}

	return &utxoSetTip{hash: hash, height: height}, nil
}

// worker processes UTXOWrapper messages from the channel and stores them in the UTXO store.
func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store,
	id int, utxoWrapperCh <-chan *utxopersister.UTXOWrapper, coinbaseTxs map[chainhash.Hash]*bt.Tx) error {
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

			if err := processUTXO(ctx, store, utxoWrapper, coinbaseTxs); err != nil {
				logger.Errorf("Worker %d failed to process UTXO: %v", id, err)
				return err
			}
		}
	}
}

// processUTXO processes a single UTXOWrapper and stores it in the UTXO store.
// coinbaseTxs maps coinbase txid to the authoritative coinbase transaction
// recovered from the V2 utxo-headers file; it may be empty.
func processUTXO(ctx context.Context, store utxo.Store, utxoWrapper *utxopersister.UTXOWrapper, coinbaseTxs map[chainhash.Hash]*bt.Tx) error {
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

	// A coinbase rebuilt from the UTXO set alone has no input (the coinbase
	// scriptSig is input data, not a UTXO) and hashes to the wrong txid. When the
	// real coinbase is available, restore its input so the stored transaction is
	// faithful and hashes back to the txid the block commits to.
	if utxoWrapper.Coinbase {
		restoreCoinbaseInput(tx, coinbaseTxs[utxoWrapper.TxID], &utxoWrapper.TxID)
	}

	if gocore.Config().GetBool("skipStore", false) {
		return nil
	}

	if _, _, err := store.SpendAndCreate(
		ctx,
		tx,
		utxoWrapper.Height,
		utxo.WithCreateOnly(),
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
