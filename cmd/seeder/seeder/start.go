package seeder

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

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

func Start() {
	go func() {
		srv := &http.Server{
			Addr:         ":6060",
			ReadTimeout:  time.Second * 5,
			WriteTimeout: time.Second * 10,
		}
		_ = srv.ListenAndServe()
	}()

	inFile := flag.String("in", "", "Input filename for UTXO set.")
	flag.Parse()

	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("seed")

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil || !found {
		logger.Errorf("blockstore URL not found in config: %v", err)
		return
	}

	logger.Infof("Using utxostore at %s", utxoStoreURL)

	utxoStore, err := utxo_factory.NewStore(ctx, logger, utxoStoreURL, "seeder", false)
	if err != nil {
		logger.Errorf("Failed to create utxostore: %v", err)
		return
	}

	channelSize, _ := gocore.Config().GetInt("channelSize", 10_000)

	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, channelSize)

	g, gCtx := errgroup.WithContext(context.Background())

	workerCount, _ := gocore.Config().GetInt("workerCount", 1_000)

	for i := 0; i < workerCount; i++ {
		g.Go(worker(gCtx, utxoStore, utxoWrapperCh))
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
	OUTER:
		for {
			select {
			case <-ctx.Done():
				logger.Infof("Context cancelled, stopping UTXO processing")
				return ctx.Err()

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
					return fmt.Errorf("failed to read UTXO: %w", err)
				}

				// Attempt to send the UTXO to the channel, but only if ctx.Done() is not active
				select {
				case utxoWrapperCh <- utxoWrapper:
					txWritten++
					utxosWritten += uint64(len(utxoWrapper.UTXOs))

					if txWritten%1_000_000 == 0 {
						logger.Infof("Processed %16s transactions with %16s utxos", formatNumber(txWritten), formatNumber(utxosWritten))
					}

				case <-ctx.Done():
					logger.Infof("Context cancelled while sending UTXO to channel, stopping UTXO processing")
					return ctx.Err()
				}
			}
		}

		logger.Infof("FINISHED  %16s transactions with %16s utxos", formatNumber(txWritten), formatNumber(utxosWritten))

		close(utxoWrapperCh)

		return nil
	})

	logger.Infof("Waiting for workers to finish")

	if err := g.Wait(); err != nil {
		logger.Errorf("Error in worker: %v", err)
		return
	}

	logger.Infof("All workers finished successfully")
}

func worker(ctx context.Context, store utxo.Store, utxoCh chan *utxopersister.UTXOWrapper) func() error {
	return func() error {
		for {
			select {
			case utxoWrapper := <-utxoCh:
				if err := processUTXO(ctx, store, utxoWrapper); err != nil {
					return err
				}

			case <-ctx.Done():
				return ctx.Err()
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
