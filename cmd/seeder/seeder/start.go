package seeder

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	// nolint:gci,gosec
	_ "net/http/pprof"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blockchain/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
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

// nolint: gocognit
func Start() {
	flag.StringVar(&inputDir, "inputDir", "", "Input directory for UTXO set and headers.")
	flag.StringVar(&hash, "hash", "", "Hash of the UTXO set / headers to process.")
	flag.BoolVar(&skipHeaders, "skipHeaders", false, "Skip processing headers.")
	flag.BoolVar(&skipUTXOs, "skipUTXOs", false, "Skip processing UTXOs.")

	flag.Usage = func() { usage("") }

	flag.Parse()

	if inputDir == "" {
		usage("Please provide an inputDir")
	}

	if hash == "" {
		usage("Please provide a hash")
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

	logger := ulogger.NewGoCoreLogger("seed")
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
		// nolint:gosec
		logger.Errorf("%v", http.ListenAndServe(":6060", nil))
	}()

	if !skipHeaders {
		// Process the headers
		if err := processHeaders(ctx, logger, headerFile); err != nil {
			logger.Errorf("Failed to process headers: %v", err)
			return
		}
	}

	if !skipUTXOs {
		// Process the UTXOs
		if err := processUTXOs(ctx, logger, utxoFile); err != nil {
			logger.Errorf("Failed to process UTXOs: %v", err)
			return
		}
	}
}

func processHeaders(ctx context.Context, logger ulogger.Logger, headersFile string) error {
	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil || !found {
		logger.Fatalf("Failed to get blockchain store URL")
	}

	blockchainStore, err := blockchain.NewStore(logger, blockchainStoreURL)
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

	magic, _, _, err := utxopersister.GetHeaderFromReader(reader)
	if err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	if magic != "U-H-1.0" {
		return errors.NewProcessingError("Invalid magic number: %s", magic)
	}

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
			options.WithMinedSet(true),
			options.WithSubtreesSet(true),
		)
		if err != nil {
			return errors.NewProcessingError("Failed to add block", err)
		}

		headersProcessed++
		txCount += header.TxCount

		fmt.Printf("Processed block %d\n", header.Height)
	}

	logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(headersProcessed), formatNumber(txCount))

	return nil
}

func processUTXOs(ctx context.Context, logger ulogger.Logger, utxoFile string) error {
	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil || !found {
		return errors.NewConfigurationError("utxostore URL not found in config", err)
	}

	logger.Infof("Using utxostore at %s", utxoStoreURL)

	utxoStore, err := utxo_factory.NewStore(ctx, logger, utxoStoreURL, "seeder", false)
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

	magic, _, _, _, err := utxopersister.GetUTXOSetHeaderFromReader(reader)
	if err != nil {
		return errors.NewProcessingError("Failed to read UTXO set header", err)
	}

	if magic != "U-S-1.0" {
		return errors.NewProcessingError("Invalid magic number: %s", magic)
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

	logger.Infof("Waiting for workers to finish")

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("Error in worker", err)
	}

	logger.Infof("All workers finished successfully")

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
		utxo.WithBlockIDs(0),
	); err != nil {
		if errors.Is(err, errors.ErrTxAlreadyExists) {
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
