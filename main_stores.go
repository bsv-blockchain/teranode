package main

import (
	"context"
	"errors"

	"github.com/bitcoin-sv/ubsv/services/asset"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	txStore                 blob.Store
	subtreeStore            blob.Store
	blockStore              blob.Store
	utxoStore               utxostore.Store
	blockchainClient        blockchain.ClientI
	assetClient             *asset.Client
	validatorClient         validator.Interface
	subtreeValidationClient subtreevalidation.Interface
	blockValidationClient   blockvalidation.Interface
)

func getUtxoStore(ctx context.Context, logger ulogger.Logger) utxostore.Store {
	if utxoStore != nil {
		return utxoStore
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}
	utxoStore, err = utxofactory.NewStore(ctx, logger, utxoStoreURL, "main")
	if err != nil {
		panic(err)
	}

	return utxoStore
}

func getSubtreeValidationClient(ctx context.Context, logger ulogger.Logger) subtreevalidation.Interface {
	if subtreeValidationClient != nil {
		return subtreeValidationClient
	}

	subtreeValidationClient = subtreevalidation.NewClient(ctx, logger)

	return subtreeValidationClient
}

func getBlockValidationClient(ctx context.Context, logger ulogger.Logger) blockvalidation.Interface {
	if blockValidationClient != nil {
		return blockValidationClient
	}

	blockValidationClient = blockvalidation.NewClient(ctx, logger)

	return blockValidationClient
}

func getBlockchainClient(ctx context.Context, logger ulogger.Logger) blockchain.ClientI {
	if blockchainClient != nil {
		return blockchainClient
	}

	var err error
	blockchainClient, err = blockchain.NewClient(ctx, logger)
	if err != nil {
		panic(err)
	}

	return blockchainClient
}

func getValidatorClient(ctx context.Context, logger ulogger.Logger) validator.Interface {
	if validatorClient != nil {
		return validatorClient
	}

	var err error
	localValidator := gocore.Config().GetBool("useLocalValidator", false)
	if localValidator {
		logger.Infof("[Validator] Using local validator")
		validatorClient, err = validator.New(ctx,
			logger,
			getUtxoStore(ctx, logger),
		)
		if err != nil {
			logger.Fatalf("could not create validator [%v]", err)
		}

	} else {
		validatorClient, err = validator.NewClient(ctx, logger)
		if err != nil {
			panic(err)
		}
	}

	return validatorClient
}

func getAssetClient(ctx context.Context, logger ulogger.Logger) *asset.Client {
	if assetClient != nil {
		return assetClient
	}

	assetAddr, ok := gocore.Config().Get("asset_grpcAddress")
	if !ok {
		panic(errors.New("no asset_grpcAddress setting found"))
	}

	var err error
	assetClient, err = asset.NewClient(ctx, logger, assetAddr)
	if err != nil {
		panic(err)
	}

	return assetClient
}

func getTxStore(logger ulogger.Logger) blob.Store {
	if txStore != nil {
		return txStore
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStore, err = blob.NewStore(logger, txStoreUrl)
	if err != nil {
		panic(err)
	}

	return txStore
}

func getSubtreeStore(logger ulogger.Logger) blob.Store {
	if subtreeStore != nil {
		return subtreeStore
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithPrefixDirectory(10))
	if err != nil {
		panic(err)
	}

	return subtreeStore
}

func getBlockStore(logger ulogger.Logger) blob.Store {
	if blockStore != nil {
		return blockStore
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}
	blockStore, err = blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	return blockStore
}
