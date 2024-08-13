package main

import (
	"context"

	"github.com/bitcoin-sv/ubsv/errors"
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
	validatorClient         validator.Interface
	subtreeValidationClient subtreevalidation.Interface
	blockValidationClient   blockvalidation.Interface
)

func getUtxoStore(ctx context.Context, logger ulogger.Logger) (utxostore.Store, error) {
	if utxoStore != nil {
		return utxoStore, nil
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.NewConfigurationError("no utxostore setting found")
	}
	utxoStore, err = utxofactory.NewStore(ctx, logger, utxoStoreURL, "main")
	if err != nil {
		return nil, err
	}

	return utxoStore, nil
}

//func getSubtreeValidationClient(ctx context.Context, logger ulogger.Logger) (subtreevalidation.Interface, error) {
//	if subtreeValidationClient != nil {
//		return subtreeValidationClient, nil
//	}
//
//	var err error
//	subtreeValidationClient, err = subtreevalidation.NewClient(ctx, logger, "main_stores")
//
//	return subtreeValidationClient, err
//}

func getBlockValidationClient(ctx context.Context, logger ulogger.Logger) (blockvalidation.Interface, error) {
	if blockValidationClient != nil {
		return blockValidationClient, nil
	}

	var err error
	blockValidationClient, err = blockvalidation.NewClient(ctx, logger, "main_stores")

	return blockValidationClient, err
}

func getBlockchainClient(ctx context.Context, logger ulogger.Logger) (blockchain.ClientI, error) {
	if blockchainClient != nil {
		return blockchainClient, nil
	}

	var err error
	blockchainClient, err = blockchain.NewClient(ctx, logger, "main_stores")

	return blockchainClient, err
}

func getValidatorClient(ctx context.Context, logger ulogger.Logger) (validator.Interface, error) {
	if validatorClient != nil {
		return validatorClient, nil
	}

	var err error
	localValidator := gocore.Config().GetBool("useLocalValidator", false)
	if localValidator {
		logger.Infof("[Validator] Using local validator")
		utxoStore, err := getUtxoStore(ctx, logger)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator client", err)
		}
		validatorClient, err = validator.New(ctx,
			logger,
			utxoStore,
		)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator", err)
		}

	} else {
		validatorClient, err = validator.NewClient(ctx, logger)
		if err != nil {
			return nil, errors.NewServiceError("could not create validator client", err)
		}
	}

	return validatorClient, nil
}

func getTxStore(logger ulogger.Logger) (blob.Store, error) {
	if txStore != nil {
		return txStore, nil
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		return nil, errors.NewConfigurationError("txstore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("no txstore setting found")
	}
	txStore, err = blob.NewStore(logger, txStoreUrl)
	if err != nil {
		return nil, errors.NewServiceError("could not create tx store", err)
	}

	return txStore, nil
}

func getSubtreeStore(logger ulogger.Logger) (blob.Store, error) {
	if subtreeStore != nil {
		return subtreeStore, nil
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		return nil, errors.NewConfigurationError("subtreestore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("subtreestore config not found")
	}
	subtreeStore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithPrefixDirectory(10))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return subtreeStore, nil
}

func getBlockStore(logger ulogger.Logger) (blob.Store, error) {
	if blockStore != nil {
		return blockStore, nil
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		return nil, errors.NewConfigurationError("blockstore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}
	blockStore, err = blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		return nil, errors.NewServiceError("could not create block store", err)
	}

	return blockStore, nil
}
