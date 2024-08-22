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
	"strings"
	"syscall"

	// nolint:gci,gosec
	_ "net/http/pprof"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// nolint: gocognit
func Start() {
	inFile := flag.String("in", "", "Input filename for UTXO set.")
	flag.Parse()

	logger := ulogger.NewGoCoreLogger("seed")

	go func() {
		// nolint:gosec
		logger.Errorf("%v", http.ListenAndServe(":6060", nil))
	}()

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil || !found {
		logger.Errorf("blockstore URL not found in config: %v", err)
		return
	}

	logger.Infof("Using utxostore at %s", utxoStoreURL)

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

	utxoStore, err := utxo_factory.NewStore(ctx, logger, utxoStoreURL, "seeder", false)
	if err != nil {
		logger.Errorf("Failed to create utxostore: %v", err)
		return
	}

	channelSize, _ := gocore.Config().GetInt("channelSize", 10_000)

	logger.Infof("Using channel size of %d", channelSize)
	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, channelSize)
	// utxoWrapperCh := make(chan *utxopersister.UTXOWrapper)

	g, gCtx := errgroup.WithContext(ctx)

	workerCount, _ := gocore.Config().GetInt("workerCount", 1_000)
	logger.Infof("Starting %d workers", workerCount)

	for i := 0; i < workerCount; i++ {
		workerID := i

		g.Go(func() error {
			return worker(gCtx, logger, utxoStore, workerID, utxoWrapperCh)
		})
	}

	// Read the UTXO data from the store
	f, err := os.Open(*inFile)
	if err != nil {
		logger.Errorf("Failed to open file: %v", err)
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	magic, _, _, _, err := utxopersister.GetUTXOSetHeaderFromReader(reader)
	if err != nil {
		logger.Errorf("Failed to read UTXO set header: %v", err)
		return
	}

	if magic != "U-S-1.0" {
		logger.Errorf("Invalid magic number: %s", magic)
		return
	}

	var (
		txWritten    uint64
		utxosWritten uint64
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
				utxoWrapper, err := utxopersister.NewUTXOWrapperFromReader(reader)
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
					txWritten++
					utxosWritten += uint64(len(utxoWrapper.UTXOs))

					if txWritten%1_000_000 == 0 {
						logger.Infof("Processed %16s transactions with %16s utxos", formatNumber(txWritten), formatNumber(utxosWritten))
					}
				}
			}
		}

		logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(txWritten), formatNumber(utxosWritten))

		return nil
	})

	logger.Infof("Waiting for workers to finish")

	if err := g.Wait(); err != nil {
		logger.Errorf("Error in worker: %v", err)
		return
	}

	logger.Infof("All workers finished successfully")
}

func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store, id int, utxoWrapperCh <-chan *utxopersister.UTXOWrapper) error {
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Worker %d received stop signal: %v", id, ctx.Err())
			return ctx.Err()

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
		utxo.WithTXID(utxoWrapper.TxID),
		utxo.WithSetCoinbase(utxoWrapper.Coinbase),
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
