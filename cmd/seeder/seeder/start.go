package seeder

import (
	"bufio"
	"context"
	"flag"
	"io"
	"os"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func Start() {
	inFile := flag.String("in", "chainstate.utxo-set", "Input filename for UTXO set.")
	flag.Parse()

	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("tnbs")

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil || !found {
		logger.Errorf("blockstore URL not found in config: %v", err)
		return
	}

	logger.Infof("Using utxostore at %s", utxoStoreURL)

	utxoStore, err := _factory.NewStore(ctx, logger, utxoStoreURL, "seeder", false)
	if err != nil {
		logger.Errorf("Failed to create utxostore: %v", err)
		return
	}

	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, 10000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	// Create 1000 workers to process the UTXO data
	for i := 0; i < 1000; i++ {
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

	for {
		utxoWrapper, err := utxopersister.NewUTXOWrapperFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if utxoWrapper == nil {
					logger.Errorf("EOF marker not found")
				} else {
					logger.Infof("EOF marker found")
				}

				break
			}

			logger.Errorf("Failed to read UTXO: %v", err)

			return
		}

		utxoWrapperCh <- utxoWrapper
	}

	close(utxoWrapperCh)

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
			case utxoWrapper, isOpen := <-utxoCh:
				if !isOpen {
					return nil
				}

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
