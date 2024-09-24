package utxopersister

import (
	"context"
	"net/http"
	_ "net/http/pprof" // nolint:gosec

	utxopersister_service "github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

func Start() {
	ctx, _, deferFn := tracing.StartTracing(
		context.Background(),
		"utxopersister",
	)
	defer deferFn()

	logger := ulogger.NewGoCoreLogger("utxopd")

	profilerAddr, found := gocore.Config().Get("profilerAddr")
	if !found {
		logger.Warnf("profilerAddr not found in config")
	} else {
		logger.Infof("Profiler available at http://%s/debug/pprof", profilerAddr)
		gocore.RegisterStatsHandlers()
		prefix, _ := gocore.Config().Get("stats_prefix")
		logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, prefix)

		// Start http server for the profiler
		go func() {
			// nolint:gosec
			logger.Errorf("%v", http.ListenAndServe(profilerAddr, nil))
		}()
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil || !found {
		logger.Errorf("blockstore URL not found in config: %v", err)
		return
	}

	logger.Infof("Using blockStore at %s", blockStoreUrl)

	blockStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		logger.Errorf("Failed to create blockStore: %v", err)
		return
	}

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil || !found {
		logger.Errorf("blockchain_store URL not found in config: %v", err)
		return
	}

	logger.Infof("Using blockchainStore at %s", blockchainStoreURL)

	blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	if err != nil {
		logger.Errorf("Failed to create blockchainStore: %v", err)
		return
	}

	logger.Infof("Creating utxopersister service")

	service, err := utxopersister_service.NewDirect(ctx, logger, blockStore, blockchainStore)
	if err != nil {
		logger.Errorf("Failed to create utxopersister service: %v", err)
		return
	}

	logger.Infof("Starting utxopersister service")

	if err := service.Init(ctx); err != nil {
		logger.Errorf("Failed to init utxopersister service: %v", err)
		return
	}

	if err := service.Start(ctx); err != nil {
		logger.Errorf("Failed utxopersister service: %v", err)
		return
	}
}
