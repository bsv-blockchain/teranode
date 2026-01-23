// Package utxopersister provides the implementation for persisting UTXO (Unspent Transaction Output) data.
//
// Usage:
//
// This package is typically used as a service to persist UTXO data from a blockchain source
// into a storage backend, such as a blob store or a direct blockchain store.
//
// Functions:
//   - RunUtxoPersister: The main entry point for initializing and running the UTXO persister service.
//
// Side effects:
//
// Functions in this package may start HTTP servers for profiling and statistics, log messages,
// and interact with external storage systems and blockchain clients.
package utxopersister

import (
	"context"
	"net/http"
	_ "net/http/pprof" // nolint:gosec
	"strconv"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxopersisterservice "github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/felixge/fgprof"
	"github.com/ordishs/gocore"
)

// RunUtxoPersister initializes and runs the UTXO persister service.
//
// This function sets up tracing, starts an optional profiler server, and initializes the required
// storage backends and services for persisting UTXO data. Depending on the configuration, it either
// uses a direct blockchain store or a blockchain client to interact with the blockchain data.
//
// Parameters:
//   - logger: The logger instance for logging messages.
//   - settings: The settings object containing configuration values.
//
// Side effects:
//   - Starts HTTP servers for profiling and statistics if configured.
//   - Logs messages and errors.
//   - Interacts with external storage systems and blockchain clients.
//
// Errors:
//   - Logs and exits on critical errors such as missing configuration or failed service initialization.
func RunUtxoPersister(logger ulogger.Logger, settings *settings.Settings) {
	// Start tracing
	ctx, _, endFn := tracing.Tracer("utxopersister").Start(context.Background(), "RunUtxoPersister")
	defer endFn()

	// If a profiler address is set, register the statistics handlers and start the profiler server
	profilerAddr := settings.ProfilerAddr
	if profilerAddr == "" {
		logger.Warnf("ProfilerAddr not found in config")
	} else {
		logger.Infof("Profiler available at http://%s/debug/pprof", profilerAddr)

		gocore.RegisterStatsHandlers()

		logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, settings.StatsPrefix)

		http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
		logger.Infof("FGProf available at http://%s/debug/fgprof", profilerAddr)

		// Start http server for the profiler
		go func() {
			// nolint:gosec
			logger.Errorf("%v", http.ListenAndServe(profilerAddr, nil))
		}()
	}

	// Get the block store URL from settings
	blockStoreURL := settings.Block.BlockStore
	if blockStoreURL == nil {
		logger.Errorf("Blockstore URL not found in config")
		return
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

	// Create the block store
	blockStore, err := blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		logger.Errorf("Failed to create blockStore: %v", err)
		return
	}

	var service *utxopersisterservice.Server

	// If UTXOPersisterDirect is enabled, create a direct blockchain store
	if settings.Block.UTXOPersisterDirect {
		blockchainStoreURL := settings.BlockChain.StoreURL
		if blockchainStoreURL == nil {
			logger.Errorf("Variable: blockchain_store URL not found in config")
			return
		}

		logger.Infof("Using blockchainStore at %s", blockchainStoreURL)

		var blockchainStore blockchainstore.Store

		blockchainStore, err = blockchainstore.NewStore(logger, blockchainStoreURL, settings)
		if err != nil {
			logger.Errorf("Failed to create blockchainStore: %v", err)
			return
		}

		service, err = utxopersisterservice.NewDirect(ctx, logger, settings, blockStore, blockchainStore)
		if err != nil {
			logger.Errorf("Failed to create utxopersister service: %v", err)
			return
		}
	} else {
		var blockchainClient blockchain.ClientI

		// Create a blockchain client
		blockchainClient, err = blockchain.NewClient(ctx, logger, settings, "test")
		if err != nil {
			logger.Errorf("Failed to create blockchainClient: %v", err)
			return
		}

		logger.Infof("Creating utxopersister service")

		// Create the UTXO persister service
		service = utxopersisterservice.New(ctx, logger, settings, blockStore, blockchainClient)
	}

	logger.Infof("Starting utxopersister service...")

	// Initialize the service
	if err = service.Init(ctx); err != nil {
		logger.Errorf("Failed to init utxopersister service: %v", err)
		return
	}

	// Create a channel to signal when the service is ready
	readyCh := make(chan struct{}, 1)

	// Start the service
	if err = service.Start(ctx, readyCh); err != nil {
		logger.Errorf("Failed utxopersister service: %v", err)
		return
	}

	<-readyCh
}
