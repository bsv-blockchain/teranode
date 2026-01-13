package daemon

import (
	"context"
	"strconv"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

type Stores struct {
	mainBlockPersisterStore     blob.Store
	mainBlockStore              blob.Store
	mainBlockValidationClient   blockvalidation.Interface
	mainBlockAssemblyClient     blockassembly.ClientI
	mainP2PClient               p2p.ClientI
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

// GetP2PClient creates and returns a new P2P client instance. Unlike other store getters, this function
// always creates a new client instance to maintain source information. The source parameter
// identifies the origin or purpose of the client.
//
// Parameters:
//   - ctx: The context for managing the client's lifecycle.
//   - logger: The logger instance for logging client activities.
//   - appSettings: The application settings containing configuration details.
//
// Returns:
//   - p2p.ClientI: The newly created P2P client instance.
//   - error: An error object if the client creation fails; otherwise, nil.
func (d *Stores) GetP2PClient(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (p2p.ClientI, error) {
	if d.mainP2PClient != nil {
		return d.mainP2PClient, nil
	}

	p2pClient, err := p2p.NewClient(ctx, logger, appSettings)
	if err != nil {
		return nil, err
	}

	d.mainP2PClient = p2pClient

	return p2pClient, nil
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
func (d *Stores) GetBlockAssemblyClient(ctx context.Context, logger ulogger.Logger,
	appSettings *settings.Settings) (blockassembly.ClientI, error) {
	if d.mainBlockAssemblyClient != nil {
		return d.mainBlockAssemblyClient, nil
	}

	var err error

	client, err := blockassembly.NewClient(ctx, logger, appSettings)
	if err != nil {
		return nil, err
	}

	d.mainBlockAssemblyClient = client

	return client, nil
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

		blockAssemblyClient, err = d.GetBlockAssemblyClient(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create block assembly client for local validator", err)
		}

		var validatorClient validator.Interface

		var blockchainClient blockchain.ClientI

		blockchainClient, err = d.GetBlockchainClient(ctx, logger, appSettings, "validator")
		if err != nil {
			return nil, errors.NewServiceError("could not create block validation client for local validator", err)
		}

		validatorClient, err = validator.New(ctx,
			logger,
			appSettings,
			utxoStore,
			txMetaKafkaProducerClient,
			rejectedTxKafkaProducerClient,
			blockAssemblyClient,
			blockchainClient,
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
func (d *Stores) GetSubtreeStore(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
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

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "subtree")
	if err != nil {
		return nil, errors.NewServiceError("could not create blockchain client for subtree store", err)
	}

	ch, err := getBlockHeightTrackerCh(ctx, logger, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("could not create block height tracker channel", err)
	}

	d.mainSubtreeStore, err = blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(hashPrefix), options.WithBlockHeightCh(ch))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return d.mainSubtreeStore, nil
}

// GetTempStore returns the main temporary store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings, defaulting to "./tmp" if not specified.
// This store is used for temporary data storage during processing.
func (d *Stores) GetTempStore(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainTempStore != nil {
		return d.mainTempStore, nil
	}

	tempStoreURL := appSettings.Legacy.TempStore
	if tempStoreURL == nil {
		return nil, errors.NewConfigurationError("temp_store config not found")
	}

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "temp")
	if err != nil {
		return nil, errors.NewServiceError("could not create blockchain client for temp store", err)
	}

	ch, err := getBlockHeightTrackerCh(ctx, logger, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("could not create block height tracker channel", err)
	}

	d.mainTempStore, err = blob.NewStore(logger, tempStoreURL, options.WithBlockHeightCh(ch))
	if err != nil {
		return nil, errors.NewServiceError("could not create temp_store", err)
	}

	return d.mainTempStore, nil
}

// GetBlockStore returns the main block store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This store is responsible for
// persisting blockchain blocks.
func (d *Stores) GetBlockStore(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainBlockStore != nil {
		return d.mainBlockStore, nil
	}

	blockStoreURL := appSettings.Block.BlockStore

	if blockStoreURL == nil {
		return nil, errors.NewConfigurationError("blockstore config not found")
	}

	var err error

	hashPrefix := -2
	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("blockstore hashPrefix config error", err)
		}
	}

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "block")
	if err != nil {
		return nil, errors.NewServiceError("could not create blockchain client for block store", err)
	}

	ch, err := getBlockHeightTrackerCh(ctx, logger, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("could not create block height tracker channel", err)
	}

	d.mainBlockStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix), options.WithBlockHeightCh(ch))
	if err != nil {
		return nil, errors.NewServiceError("could not create block store", err)
	}

	return d.mainBlockStore, nil
}

// GetBlockPersisterStore returns the main block persister store instance. If the store hasn't been
// initialized yet, it creates a new one using the configured URL from settings. This store is
// specifically used for block persistence operations.
func (d *Stores) GetBlockPersisterStore(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
	if d.mainBlockPersisterStore != nil {
		return d.mainBlockPersisterStore, nil
	}

	blockStoreURL := appSettings.BlockPersister.Store

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

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "blockpersister")
	if err != nil {
		return nil, errors.NewServiceError("could not create blockchain client for block persister store", err)
	}

	ch, err := getBlockHeightTrackerCh(ctx, logger, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("could not create block height tracker channel", err)
	}

	d.mainBlockPersisterStore, err = blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix), options.WithBlockHeightCh(ch))
	if err != nil {
		return nil, errors.NewServiceError("could not create block persister store", err)
	}

	return d.mainBlockPersisterStore, nil
}

// Cleanup resets all singleton stores. This is particularly important for tests
// where stores may persist between test runs.
func (d *Stores) Cleanup() {
	d.mainBlockPersisterStore = nil
	d.mainBlockStore = nil
	d.mainBlockValidationClient = nil
	d.mainSubtreeStore = nil
	d.mainSubtreeValidationClient = nil
	d.mainTempStore = nil
	d.mainTxStore = nil
	d.mainUtxoStore = nil
	d.mainValidatorClient = nil
	d.mainP2PClient = nil

	// Reset the Aerospike cleanup service singleton if it exists
	// This prevents state leakage between test runs
	aerospike.ResetPrunerServiceForTests()
}
