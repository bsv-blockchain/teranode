package daemon

import (
	"context"
	"strconv"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	utxofactory "github.com/bitcoin-sv/teranode/stores/utxo/_factory"
	"github.com/bitcoin-sv/teranode/ulogger"
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

// GetUtxoStore returns the main UTXO store instance. If the store hasn't been initialized yet,
// it creates a new one using the provided settings. This function ensures only one instance
// of the UTXO store exists by maintaining a singleton pattern.
func GetUtxoStore(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (utxostore.Store, error) {
	if mainUtxoStore != nil {
		return mainUtxoStore, nil
	}

	mainUtxoStore, err := utxofactory.NewStore(ctx, logger, tSettings, "main")
	if err != nil {
		return nil, err
	}

	return mainUtxoStore, nil
}

// GetSubtreeValidationClient returns the main subtree validation client instance. If the client
// hasn't been initialized yet, it creates a new one using the provided settings. This function
// ensures only one instance of the subtree validation client exists.
func GetSubtreeValidationClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (subtreevalidation.Interface, error) {
	if mainSubtreeValidationClient != nil {
		return mainSubtreeValidationClient, nil
	}

	var err error
	mainSubtreeValidationClient, err = subtreevalidation.NewClient(ctx, logger, tSettings, "main_stores")

	return mainSubtreeValidationClient, err
}

// GetBlockValidationClient returns the main block validation client instance. If the client
// hasn't been initialized yet, it creates a new one using the provided settings. This function
// ensures only one instance of the block validation client exists.
func GetBlockValidationClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (blockvalidation.Interface, error) {
	if mainBlockValidationClient != nil {
		return mainBlockValidationClient, nil
	}

	var err error
	mainBlockValidationClient, err = blockvalidation.NewClient(ctx, logger, tSettings, "main_stores")

	return mainBlockValidationClient, err
}

// GetBlockchainClient creates and returns a new blockchain client instance. Unlike other store
// getters, this function always creates a new client instance to maintain source information.
// The source parameter identifies the origin or purpose of the client.
func GetBlockchainClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (blockchain.ClientI, error) {
	// don't use a global client, otherwise we don't know the source
	return blockchain.NewClient(ctx, logger, tSettings, source)
}

// GetValidatorClient returns the main validator client instance. If the client hasn't been
// initialized yet, it creates either a local validator or a remote client based on configuration.
// For local validators, it sets up necessary dependencies including UTXO store and Kafka producers.
func GetValidatorClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (validator.Interface, error) {
	if mainValidatorClient != nil {
		return mainValidatorClient, nil
	}

	var err error

	localValidator := tSettings.Validator.UseLocalValidator

	if localValidator {
		logger.Infof("[Validator] Using local validator")

		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator client", err)
		}

		txMetaKafkaProducerClient, err := getKafkaTxmetaAsyncProducer(ctx, logger, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create txmeta kafka producer for local validator", err)
		}

		rejectedTxKafkaProducerClient, err := getKafkaRejectedTxAsyncProducer(ctx, logger, tSettings)
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

// GetTxStore returns the main transaction store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This function ensures only one
// instance of the transaction store exists.
func GetTxStore(logger ulogger.Logger) (blob.Store, error) {
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

	hashPrefix := 2
	if txStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(txStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("txstore hashPrefix config error", err)
		}
	}

	mainTxstore, err = blob.NewStore(logger, txStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create tx store", err)
	}

	return mainTxstore, nil
}

// GetSubtreeStore returns the main subtree store instance. If the store hasn't been initialized yet,
// it creates a new one using the URL from settings. The store is configured with a hash prefix
// of 2 for optimized storage organization.
func GetSubtreeStore(logger ulogger.Logger, tSettings *settings.Settings) (blob.Store, error) {
	if mainSubtreestore != nil {
		return mainSubtreestore, nil
	}

	var err error

	subtreeStoreURL := tSettings.SubtreeValidation.SubtreeStore

	if subtreeStoreURL == nil {
		return nil, errors.NewConfigurationError("subtreestore config not found")
	}

	hashPrefix := 2
	if subtreeStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(subtreeStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("subtreestore hashPrefix config error", err)
		}
	}

	mainSubtreestore, err = blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return mainSubtreestore, nil
}

// GetTempStore returns the main temporary store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings, defaulting to "./tmp" if not specified.
// This store is used for temporary data storage during processing.
func GetTempStore(logger ulogger.Logger) (blob.Store, error) {
	if mainTempStore != nil {
		return mainTempStore, nil
	}

	tempStoreURL, err, found := gocore.Config().GetURL("temp_store", "file://./tmp")
	if err != nil {
		return nil, errors.NewConfigurationError("temp_store setting error", err)
	}

	if !found {
		return nil, errors.NewConfigurationError("temp_store config not found")
	}

	mainTempStore, err = blob.NewStore(logger, tempStoreURL)
	if err != nil {
		return nil, errors.NewServiceError("could not create temp_store", err)
	}

	return mainTempStore, nil
}

// GetBlockStore returns the main block store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This store is responsible for
// persisting blockchain blocks.
func GetBlockStore(logger ulogger.Logger) (blob.Store, error) {
	if mainBlockStore != nil {
		return mainBlockStore, nil
	}

	blockStoreURL, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		return nil, errors.NewConfigurationError("blockstore setting error", err)
	}

	if !found {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	hashPrefix := 2
	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blockstore hashPrefix config error", err)
		}
	}

	mainBlockStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create block store", err)
	}

	return mainBlockStore, nil
}

// GetBlockPersisterStore returns the main block persister store instance. If the store hasn't been
// initialized yet, it creates a new one using the configured URL from settings. This store is
// specifically used for block persistence operations.
func GetBlockPersisterStore(logger ulogger.Logger) (blob.Store, error) {
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

	hashPrefix := 2
	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blockPersisterStore hashPrefix config error", err)
		}
	}

	mainBlockPersisterStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create block persister store", err)
	}

	return mainBlockPersisterStore, nil
}
