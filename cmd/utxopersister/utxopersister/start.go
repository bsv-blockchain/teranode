package utxopersister

import (
	"context"

	utxopersister_service "github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

func Start() {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("utxopd")

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

	if err := service.Start(ctx); err != nil {
		logger.Errorf("Failed utxopersister service: %v", err)
		return
	}

	return
}
