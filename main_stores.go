package main

import (
	"context"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	mainTxstore                 blob.Store
	mainSubtreestore            blob.Store
	mainTempStore               blob.Store
	mainBlockStore              blob.Store
	mainBlockPersisterStore     blob.Store
	mainUtxoStore               utxostore.Store
	mainValidatorClient         validator.Interface
	mainSubtreeValidationClient subtreevalidation.Interface
	mainBlockValidationClient   blockvalidation.Interface
)

func getUtxoStore(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (utxostore.Store, error) {
	if mainUtxoStore != nil {
		return mainUtxoStore, nil
	}

	mainUtxoStore, err := utxofactory.NewStore(ctx, logger, tSettings, "main")
	if err != nil {
		return nil, err
	}

	return mainUtxoStore, nil
}

func getSubtreeValidationClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (subtreevalidation.Interface, error) {
	if mainSubtreeValidationClient != nil {
		return mainSubtreeValidationClient, nil
	}

	var err error
	mainSubtreeValidationClient, err = subtreevalidation.NewClient(ctx, logger, tSettings, "main_stores")

	return mainSubtreeValidationClient, err
}

func getBlockValidationClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (blockvalidation.Interface, error) {
	if mainBlockValidationClient != nil {
		return mainBlockValidationClient, nil
	}

	var err error
	mainBlockValidationClient, err = blockvalidation.NewClient(ctx, logger, tSettings, "main_stores")

	return mainBlockValidationClient, err
}

func getBlockchainClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (blockchain.ClientI, error) {
	// don't use a global client, otherwise we don't know the source
	return blockchain.NewClient(ctx, logger, tSettings, source)
}

func getValidatorClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (validator.Interface, error) {
	if mainValidatorClient != nil {
		return mainValidatorClient, nil
	}

	var err error
	localValidator := gocore.Config().GetBool("useLocalValidator", false)
	if localValidator {
		logger.Infof("[Validator] Using local validator")
		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator client", err)
		}

		txMetaKafkaProducerClient, err := getKafkaTxmetaAsyncProducer(ctx, logger)
		if err != nil {
			return nil, errors.NewServiceError("could not create txmeta kafka producer for local validator", err)
		}

		rejectedTxKafkaProducerClient, err := getKafkaRejectedTxAsyncProducer(ctx, logger)
		if err != nil {
			return nil, errors.NewServiceError("could not create rejectedTx kafka producer for local validator", err)
		}

		mainValidatorClient, err = validator.New(ctx,
			logger,
			tSettings,
			utxoStore,
			txMetaKafkaProducerClient,
			rejectedTxKafkaProducerClient,
		)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator", err)
		}

	} else {
		mainValidatorClient, err = validator.NewClient(ctx, logger, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create validator client", err)
		}
	}

	return mainValidatorClient, nil
}

func getTxStore(logger ulogger.Logger) (blob.Store, error) {
	if mainTxstore != nil {
		return mainTxstore, nil
	}

	txStoreURL, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		return nil, errors.NewConfigurationError("txstore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("no txstore setting found")
	}

	mainTxstore, err = blob.NewStore(logger, txStoreURL)
	if err != nil {
		return nil, errors.NewServiceError("could not create tx store", err)
	}

	return mainTxstore, nil
}

func getSubtreeStore(logger ulogger.Logger) (blob.Store, error) {
	if mainSubtreestore != nil {
		return mainSubtreestore, nil
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		return nil, errors.NewConfigurationError("subtreestore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("subtreestore config not found")
	}

	mainSubtreestore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithHashPrefix(2))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return mainSubtreestore, nil
}

func getTempStore(logger ulogger.Logger) (blob.Store, error) {
	if mainTempStore != nil {
		return mainTempStore, nil
	}

	tempStoreURL, err, found := gocore.Config().GetURL("tempStore", "file://./tmp")
	if err != nil {
		return nil, errors.NewConfigurationError("tempStore setting error", err)
	}

	if !found {
		return nil, errors.NewConfigurationError("tempStore config not found")
	}

	mainTempStore, err = blob.NewStore(logger, tempStoreURL)
	if err != nil {
		return nil, errors.NewServiceError("could not create tempStore", err)
	}

	return mainTempStore, nil
}

func getBlockStore(logger ulogger.Logger) (blob.Store, error) {
	if mainBlockStore != nil {
		return mainBlockStore, nil
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		return nil, errors.NewConfigurationError("blockstore setting error", err)
	}
	if !found {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	mainBlockStore, err = blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		return nil, errors.NewServiceError("could not create block store", err)
	}

	return mainBlockStore, nil
}

func getBlockPersisterStore(logger ulogger.Logger) (blob.Store, error) {
	if mainBlockPersisterStore != nil {
		return mainBlockPersisterStore, nil
	}

	blockStoreURL, err, found := gocore.Config().GetURL("blockPersisterStore")
	if err != nil {
		return nil, errors.NewConfigurationError("blockPersisterStore setting error", err)
	}

	if !found {
		return nil, errors.NewConfigurationError("blockPersisterStore config not found")
	}

	mainBlockPersisterStore, err = blob.NewStore(logger, blockStoreURL)
	if err != nil {
		return nil, errors.NewServiceError("could not create block persister store", err)
	}

	return mainBlockPersisterStore, nil
}
