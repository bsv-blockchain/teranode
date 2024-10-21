package utxopersister

import (
	"context"
	"net/http"
	_ "net/http/pprof" // nolint:gosec

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	utxopersister_service "github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/felixge/fgprof"
	"github.com/ordishs/gocore"
)

func Start() {
	ctx, _, deferFn := tracing.StartTracing(
		context.Background(),
		"utxopersister",
	)
	defer deferFn()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("utxopd", ulogger.WithLevel(logLevelStr))

	profilerAddr, found := gocore.Config().Get("profilerAddr")
	if !found {
		logger.Warnf("profilerAddr not found in config")
	} else {
		logger.Infof("Profiler available at http://%s/debug/pprof", profilerAddr)

		gocore.RegisterStatsHandlers()
		prefix, _ := gocore.Config().Get("stats_prefix")
		logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, prefix)

		http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
		logger.Infof("FGProf available at http://%s/debug/fgprof", profilerAddr)

		// Start http server for the profiler
		go func() {
			// nolint:gosec
			logger.Errorf("%v", http.ListenAndServe(profilerAddr, nil))
		}()
	}

	blockStoreURL, err, found := gocore.Config().GetURL("blockstore")
	if err != nil || !found {
		logger.Errorf("blockstore URL not found in config: %v", err)
		return
	}

	logger.Infof("Using blockStore at %s", blockStoreURL)

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	if err != nil {
		logger.Errorf("Failed to create blockStore: %v", err)
		return
	}

	var service *utxopersister_service.Server

	if gocore.Config().GetBool("direct", true) {
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

		service, err = utxopersister_service.NewDirect(ctx, logger, blockStore, blockchainStore)
		if err != nil {
			logger.Errorf("Failed to create utxopersister service: %v", err)
			return
		}
	} else {
		blockchainClient, err := blockchain.NewClient(ctx, logger, "test")
		if err != nil {
			logger.Errorf("Failed to create blockchainClient: %v", err)
			return
		}

		logger.Infof("Creating utxopersister service")

		service = utxopersister_service.New(ctx, logger, blockStore, blockchainClient)
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
