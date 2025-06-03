package seeder

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockpersister"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blob_options "github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	blockchain_options "github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	utxo_factory "github.com/bitcoin-sv/teranode/stores/utxo/factory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	inputDir    string
	hash        string
	skipHeaders bool
	skipUTXOs   bool
)

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}

	fmt.Printf("Usage: seeder -inputDir <folder> -hash <hash> [-skipHeaders] [-skipUTXOs]\n\n")

	os.Exit(1)
}

func Seeder(logger ulogger.Logger, tSettings *settings.Settings, inputDir string, hash string, skipHeaders bool, skipUTXOs bool) {
	profilerAddr := tSettings.ProfilerAddr
	if profilerAddr != "" {
		go func() {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			gocore.RegisterStatsHandlers()

			prefix := tSettings.StatsPrefix
			logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, prefix)

			server := &http.Server{
				Addr:         profilerAddr,
				Handler:      nil,
				ReadTimeout:  60 * time.Second,
				WriteTimeout: 60 * time.Second,
				IdleTimeout:  120 * time.Second,
			}

			// http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Errorf("Profiler server failed: %v", err)
			}
		}()
	}

	var (
		headerFile string
		utxoFile   string
	)

	if !skipHeaders {
		// Check the headers file exists
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
		logger.Errorf("%v", http.ListenAndServe(":6060", nil))
	}()

	wg := sync.WaitGroup{}

	if !skipHeaders {
		wg.Add(1)

		go func() {
			defer wg.Done()

			logger.Infof("Processing headers...")

			// Process the headers
			if err := processHeaders(ctx, logger, tSettings, headerFile); err != nil {
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
			if err := processUTXOs(ctx, logger, tSettings, utxoFile); err != nil {
				logger.Errorf("Failed to process UTXOs: %v", err)
				return
			}

			logger.Infof("Finished processing UTXOs")
		}()
	}

	wg.Wait()
}

func processHeaders(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, headersFile string) error {
	blockchainStoreURL := tSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return errors.NewConfigurationError("blockchain store URL not found in config")
	}

	q := blockchainStoreURL.Query()
	q.Set("seeder", "true")

	blockchainStoreURL.RawQuery = q.Encode()
	logger.Infof("Using blockchain store at %s", blockchainStoreURL)

	blockchainStore, err := blockchain.NewStore(logger, blockchainStoreURL, tSettings)
	if err != nil {
		logger.Fatalf("Failed to create blockchain client: %v", err)
	}

	// _, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	// if err != nil {
	// 	logger.Fatalf("Failed to get best block header: %v", err)
	// }

	// _ = meta

	f, err := os.Open(headersFile)
	if err != nil {
		return errors.NewStorageError("Failed to open file", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	header, err := fileformat.ReadHeader(reader)
	if err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	if header.FileType() != fileformat.FileTypeUtxoHeaders {
		return errors.NewProcessingError("Invalid file type: %s", header.FileType())
	}

	var (
		hash   *chainhash.Hash
		height uint32
	)

	if err := binary.Read(reader, binary.LittleEndian, hash); err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &height); err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	// Write the last block height and hash to the blockpersister_state.txt file
	_ = blockpersister.New(ctx, nil, tSettings, nil, nil, nil, nil, blockpersister.WithSetInitialState(height, hash))

	var (
		headersProcessed uint64
		txCount          uint64
	)

	for {
		header, err := utxopersister.NewUTXOHeaderFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if header == nil {
					logger.Errorf("EOF marker not found")
				} else {
					logger.Infof("EOF marker found")
				}

				break
			}

			return errors.NewProcessingError("Failed to read UTXO", err)
		}

		if header.Height == 0 {
			// The genesis block is already in the store
			continue
		}

		block := &model.Block{
			Header:           header.BlockHeader,
			TransactionCount: header.TxCount,
			Height:           header.Height,
		}

		_, _, err = blockchainStore.StoreBlock(
			ctx,
			block,
			"headers",
			blockchain_options.WithMinedSet(true),
			blockchain_options.WithSubtreesSet(true),
		)
		if err != nil {
			return errors.NewProcessingError("Failed to add block", err)
		}

		headersProcessed++
		txCount += header.TxCount

		if header.Height%10000 == 0 {
			fmt.Printf("Processed to block height %d\n", header.Height)
		}
	}

	logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(headersProcessed), formatNumber(txCount))

	return nil
}

func processUTXOs(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoFile string) error {
	blockStoreURL := tSettings.Block.BlockStore
	if blockStoreURL == nil {
		return errors.NewConfigurationError("blockstore URL not found in config")
	}

	logger.Infof("Using blockStore at %s", blockStoreURL)

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	if err != nil {
		return errors.NewStorageError("Failed to create blockStore", err)
	}

	exists, err := blockStore.Exists(ctx, nil, fileformat.FileTypeDat, blob_options.WithFilename("lastProcessed"))
	if err != nil {
		return errors.NewStorageError("Failed to check if lastProcessed.dat exists", err)
	}

	if exists {
		logger.Errorf("lastProcessed.dat exists, skipping UTXOs")
		return nil
	}

	logger.Infof("Using utxostore at %s", tSettings.UtxoStore.UtxoStore)

	utxoStore, err := utxo_factory.NewStore(ctx, logger, tSettings, "seeder", false)
	if err != nil {
		return errors.NewStorageError("Failed to create utxostore", err)
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

	// Read the UTXO data from the store
	f, err := os.Open(utxoFile)
	if err != nil {
		return errors.NewStorageError("Failed to open file", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	header, err := fileformat.ReadHeader(reader)
	if err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	if header.FileType() != fileformat.FileTypeUtxoSet {
		return errors.NewProcessingError("Invalid file type: %s", header.FileType)
	}

	var hash *chainhash.Hash

	if err := binary.Read(reader, binary.LittleEndian, &hash); err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	var height uint32
	if err := binary.Read(reader, binary.LittleEndian, &height); err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	// With UTXOSets, we also read the previous block hash before we start reading the UTXOs
	var previousBlockHash [32]byte

	_, err = reader.Read(previousBlockHash[:])
	if err != nil {
		return errors.NewProcessingError("Couldn't read previous block hash from file", err)
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
				utxoWrapper, err := utxopersister.NewUTXOWrapperFromReader(gCtx, reader)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if utxoWrapper == nil {
							logger.Errorf("EOF marker not found")
						} else {
							logger.Infof("EOF marker found")
						}
						break OUTER
					}

					logger.Errorf("Failed to read UTXO: %v", err)
					return errors.NewProcessingError("Failed to read UTXO", err)
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

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("Error in worker", err)
	}

	logger.Infof("All workers finished successfully")

	heightStr := fmt.Sprintf("%d\n", height)

	if err := blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(heightStr), blob_options.WithFilename("lastProcessed")); err != nil {
		return errors.NewStorageError("Failed to write height of %d to lastProcessed.dat", height, err)
	}

	return nil
}

func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store, id int, utxoWrapperCh <-chan *utxopersister.UTXOWrapper) error {
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
