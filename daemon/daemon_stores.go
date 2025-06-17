package daemon

import (
	"context"
	"strconv"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	utxofactory "github.com/bitcoin-sv/teranode/stores/utxo/factory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
)

type Stores struct {
	mainBlockPersisterStore     blob.Store
	mainBlockStore              blob.Store
	mainBlockValidationClient   blockvalidation.Interface
	mainSubtreeStore            blob.Store
	mainSubtreeValidationClient subtreevalidation.Interface
	mainTempStore               blob.Store
	mainTxStore                 blob.Store
	mainUtxoStore               utxostore.Store
	mainValidatorClient         validator.Interface
}

// GetUtxoStore returns the main UTXO store instance. If the store hasn't been initialized yet,
// it creates a new one using the provided settings. This function ensures only one instance
// of the UTXO store exists by maintaining a singleton pattern.
func (d *Stores) GetUtxoStore(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (utxostore.Store, error) {
	if d.mainUtxoStore != nil {
		return d.mainUtxoStore, nil
	}

	var err error

	d.mainUtxoStore, err = utxofactory.NewStore(ctx, logger, appSettings, "main")
	if err != nil {
		return nil, err
	}

	return d.mainUtxoStore, nil
}

// GetSubtreeValidationClient returns the main subtree validation client instance. If the client
// hasn't been initialized yet, it creates a new one using the provided settings. This function
// ensures only one instance of the subtree validation client exists.
func (d *Stores) GetSubtreeValidationClient(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (subtreevalidation.Interface, error) {
	if d.mainSubtreeValidationClient != nil {
		return d.mainSubtreeValidationClient, nil
	}

	var err error

	d.mainSubtreeValidationClient, err = subtreevalidation.NewClient(ctx, logger, appSettings, "main_stores")

	return d.mainSubtreeValidationClient, err
}

// GetBlockValidationClient returns the main block validation client instance. If the client
// hasn't been initialized yet, it creates a new one using the provided settings. This function
// ensures only one instance of the block validation client exists.
func (d *Stores) GetBlockValidationClient(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (blockvalidation.Interface, error) {
	if d.mainBlockValidationClient != nil {
		return d.mainBlockValidationClient, nil
	}

	var err error

	d.mainBlockValidationClient, err = blockvalidation.NewClient(ctx, logger, appSettings, "main_stores")

	return d.mainBlockValidationClient, err
}

// GetBlockchainClient creates and returns a new blockchain client instance. Unlike other store
// getters, this function always creates a new client instance to maintain source information.
// The source parameter identifies the origin or purpose of the client.
func (d *Stores) GetBlockchainClient(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings,
	source string) (blockchain.ClientI, error) {
	// don't use a global client, otherwise we don't know the source
	return blockchain.NewClient(ctx, logger, appSettings, source)
}

// GetBlockAssemblyClient creates and returns a new block assembly client instance.
func GetBlockAssemblyClient(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (blockassembly.ClientI, error) {
	// don't use a global client, otherwise we don't know the source
	return blockassembly.NewClient(ctx, logger, appSettings)
}

// GetValidatorClient returns the main validator client instance. If the client hasn't been
// initialized yet, it creates either a local validator or a remote client based on configuration.
// For local validators, it sets up necessary dependencies including UTXO store and Kafka producers.
func (d *Stores) GetValidatorClient(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (validator.Interface, error) {
	if d.mainValidatorClient != nil {
		return d.mainValidatorClient, nil
	}

	var err error

	localValidator := appSettings.Validator.UseLocalValidator

	if localValidator {
		logger.Infof("[Validator] Using local validator")

		var utxoStore utxostore.Store

		utxoStore, err = d.GetUtxoStore(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator client", err)
		}

		var txMetaKafkaProducerClient *kafka.KafkaAsyncProducer

		txMetaKafkaProducerClient, err = getKafkaTxmetaAsyncProducer(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create txmeta kafka producer for local validator", err)
		}

		var rejectedTxKafkaProducerClient *kafka.KafkaAsyncProducer

		rejectedTxKafkaProducerClient, err = getKafkaRejectedTxAsyncProducer(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create rejectedTx kafka producer for local validator", err)
		}

		var blockAssemblyClient blockassembly.ClientI

		blockAssemblyClient, err = GetBlockAssemblyClient(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create block assembly client for local validator", err)
		}

		var validatorClient validator.Interface

		validatorClient, err = validator.New(ctx,
			logger,
			appSettings,
			utxoStore,
			txMetaKafkaProducerClient,
			rejectedTxKafkaProducerClient,
			blockAssemblyClient,
		)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator", err)
		}

		return validatorClient, nil

	} else {
		d.mainValidatorClient, err = validator.NewClient(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create validator client", err)
		}
	}

	return d.mainValidatorClient, nil
}

// GetTxStore returns the main transaction store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This function ensures only one
// instance of the transaction store exists.
func (d *Stores) GetTxStore(logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainTxStore != nil {
		return d.mainTxStore, nil
	}

	txStoreURL := appSettings.Block.TxStore
	if txStoreURL == nil {
		return nil, errors.NewConfigurationError("txstore config not found")
	}

	var err error

	hashPrefix := 2
	if txStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(txStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("txstore hashPrefix config error", err)
		}
	}

	d.mainTxStore, err = blob.NewStore(logger, txStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create tx store", err)
	}

	return d.mainTxStore, nil
}

// GetSubtreeStore returns the main subtree store instance. If the store hasn't been initialized yet,
// it creates a new one using the URL from settings. The store is configured with a hash prefix
// of 2 for optimized storage organization.
func (d *Stores) GetSubtreeStore(logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainSubtreeStore != nil {
		return d.mainSubtreeStore, nil
	}

	var err error

	subtreeStoreURL := appSettings.SubtreeValidation.SubtreeStore

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

	d.mainSubtreeStore, err = blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return d.mainSubtreeStore, nil
}

// GetTempStore returns the main temporary store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings, defaulting to "./tmp" if not specified.
// This store is used for temporary data storage during processing.
func (d *Stores) GetTempStore(logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainTempStore != nil {
		return d.mainTempStore, nil
	}

	tempStoreURL := appSettings.Legacy.TempStore
	if tempStoreURL == nil {
		return nil, errors.NewConfigurationError("temp_store config not found")
	}

	var err error

	d.mainTempStore, err = blob.NewStore(logger, tempStoreURL)
	if err != nil {
		return nil, errors.NewServiceError("could not create temp_store", err)
	}

	return d.mainTempStore, nil
}

// GetBlockStore returns the main block store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This store is responsible for
// persisting blockchain blocks.
func (d *Stores) GetBlockStore(logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainBlockStore != nil {
		return d.mainBlockStore, nil
	}

	blockStoreURL := appSettings.Block.BlockStore

	if blockStoreURL == nil {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	var err error

	hashPrefix := 2
	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blockstore hashPrefix config error", err)
		}
	}

	d.mainBlockStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create block store", err)
	}

	return d.mainBlockStore, nil
}

// GetBlockPersisterStore returns the main block persister store instance. If the store hasn't been
// initialized yet, it creates a new one using the configured URL from settings. This store is
// specifically used for block persistence operations.
func (d *Stores) GetBlockPersisterStore(logger ulogger.Logger,
	appSettings *settings.Settings) (blob.Store, error) {
	if d.mainBlockPersisterStore != nil {
		return d.mainBlockPersisterStore, nil
	}

	blockStoreURL := appSettings.Block.PersisterStore

	if blockStoreURL == nil {
		return nil, errors.NewConfigurationError("blockPersisterStore config not found")
	}

	var err error

	hashPrefix := 2
	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blockPersisterStore hashPrefix config error", err)
		}
	}

	d.mainBlockPersisterStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewServiceError("could not create block persister store", err)
	}

	return d.mainBlockPersisterStore, nil
}
