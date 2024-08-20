package seeder

import (
	"bufio"
	"context"
	"flag"
	"os"
	"time"

	utxopersister_service "github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils/batcher"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type UTXO struct {
	utxo  *utxopersister_service.UTXO
	errCh chan error
}

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

	utxoStore, err := utxo_factory.NewStore(ctx, logger, utxoStoreURL, "seeder", false)
	if err != nil {
		logger.Errorf("Failed to create utxostore: %v", err)
		return
	}

	utxoWrapperCh := make(chan *utxopersister_service.UTXOWrapper, 10000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	// Create 1000 workers to process the UTXO data
	for i := 0; i < 1000; i++ {
		g.Go(worker(gCtx, logger, utxoStore, utxoWrapperCh))
	}

	// Read the UTXO data from the store
	f, err := os.Open(*inFile)
	if err != nil {
		logger.Errorf("Failed to open file: %v", err)
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		if ctx.Done() != nil {
			break
		}

		utxoWrapper, err := utxopersister_service.NewUTXOWrapperFromReader(reader)
		if err != nil {
			logger.Errorf("Failed to read UTXO: %v", err)
			break
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

func worker(ctx context.Context, logger ulogger.Logger, store utxo.Store, utxoCh chan *utxopersister_service.UTXOWrapper) func() error {

	batchSize, _ := gocore.Config().GetInt("seeder_batcherSize", 256)
	batchDurationStr, _ := gocore.Config().GetInt("seeder_batcherDurationMillis", 10)
	batchDuration := time.Duration(batchDurationStr) * time.Millisecond
	batcher := batcher.New[UTXO](batchSize, batchDuration, saveBatch, false)
	_ = batcher

	_ = store

	_ = logger

	return func() error {
		for {
			select {
			case utxo := <-utxoCh:
				_ = utxo
				// errCh := make(chan error)
				// batcher.Put(&UTXOWrapper{utxo: utxo, errCh: errCh})
				// // err := <-errCh
				// if err != nil {
				// 	return err
				// }

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func saveBatch(batch []*UTXO) {
	_ = batch[0].errCh
	_ = batch[0].utxo

	// Save the batch of UTXOs
	// store.SaveBatch(ctx, batch)
}
