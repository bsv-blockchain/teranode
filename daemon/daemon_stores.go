package daemon

import (
	"context"
	"strconv"
	"sync"

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
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
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
	mainBlockchainStore         blockchainstore.Store
	mainSubtreeValidationClient subtreevalidation.Interface
	mainTempStore               blob.Store
	mainTxStore                 blob.Store
	mainUtxoStore               utxostore.Store
	mainValidatorClient         validator.Interface
	mainBlobDeletionScheduler   options.BlobDeletionScheduler
	mainPeerRegistryClient      blockchain.PeerRegistryClientI
	// peerRegistryClientOnce guards the singleton init in
	// GetPeerRegistryClient against concurrent first-callers. The other
	// singleton getters in this file have the same race shape; addressing
	// them is out of scope for this PR.
	peerRegistryClientOnce sync.Once
	peerRegistryClientErr  error

	// constructedClients retains every gRPC client the daemon constructs
	// (both daemonStores singletons and the fresh-per-construction clients
	// otherwise discarded into locals) so daemon.closeClients can close each
	// exactly once on shutdown. The daemon is the sole owner; receiving
	// services borrow these clients and must not close them.
	constructedClients   []clientCloser
	constructedClientsMu sync.Mutex
}

// retainClient records a daemon-constructed client for shutdown close. It
// type-asserts to clientCloser, so callers can pass any client interface value;
// clients without a Close() error method (e.g. in-process locals) are ignored.
// Each constructed instance must be retained exactly once.
func (d *Stores) retainClient(client any) {
	cc, ok := client.(clientCloser)
	if !ok {
		return
	}

	d.constructedClientsMu.Lock()
	d.constructedClients = append(d.constructedClients, cc)
	d.constructedClientsMu.Unlock()
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
	if err == nil {
		d.retainClient(d.mainSubtreeValidationClient)
	}

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
	if err == nil {
		d.retainClient(d.mainBlockValidationClient)
	}

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
	d.retainClient(p2pClient)

	return p2pClient, nil
}

// GetBlockchainClient creates and returns a new blockchain client instance. Unlike other store
// getters, this function always creates a new client instance to maintain source information.
// The source parameter identifies the origin or purpose of the client.
func (d *Stores) GetBlockchainClient(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings,
	source string) (blockchain.ClientI, error) {
	// don't use a global client, otherwise we don't know the source
	client, err := blockchain.NewClient(ctx, logger, appSettings, source)
	if err != nil {
		return nil, err
	}

	// Fresh instance per call (not a singleton); retain each so closeClients
	// closes every constructed blockchain client exactly once.
	d.retainClient(client)

	return client, nil
}

// GetPeerRegistryClient returns a singleton client to the centralized peer
// registry hosted by the blockchain service. The same connection is reused
// across all callers within a daemon process; use GetBlockchainClient when
// you need a per-source labelled connection instead.
func (d *Stores) GetPeerRegistryClient(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings,
	_ string) (blockchain.PeerRegistryClientI, error) {
	d.peerRegistryClientOnce.Do(func() {
		client, err := blockchain.NewPeerRegistryClient(ctx, appSettings.BlockChain.GRPCAddress, appSettings)
		if err != nil {
			d.peerRegistryClientErr = err
			return
		}
		// Plumb the logger so non-fatal proto-decode warnings surface via the
		// structured logger instead of stderr. SetLogger is a no-op on the
		// in-memory localPeerRegistryClient; here the concrete type is
		// *PeerRegistryClient which honours it.
		if setter, ok := client.(interface{ SetLogger(ulogger.Logger) }); ok {
			setter.SetLogger(logger)
		}
		d.mainPeerRegistryClient = client
	})
	if d.peerRegistryClientErr != nil {
		return nil, d.peerRegistryClientErr
	}
	return d.mainPeerRegistryClient, nil
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
	d.retainClient(client)

	return client, nil
}

// GetValidatorClient returns the main validator client instance. If the client hasn't been
// initialized yet, it creates either a local validator or a remote client based on configuration.
// For local validators, it sets up necessary dependencies including UTXO store and Kafka producers.
// Compile-time guard: the local-validator path retains a *validator.Validator and
// relies on the daemon's clientCloser contract (Close() error) to drain+stop it on
// shutdown. If Validator.Close's signature ever drifts, retainClient would silently
// skip it (it type-asserts at runtime); this assertion turns that drift into a build
// failure instead.
var _ clientCloser = (*validator.Validator)(nil)

// localValidatorKafkaProducers builds the three async Kafka producers owned by a
// local validator (txmeta, rejectedTx, policyRejected). It is a package var so
// tests can substitute recording producers to exercise the construction-error
// cleanup path in GetValidatorClient.
//
// On a creation failure it returns the producers created so far (a true-nil
// interface for the rest) plus the error, so the caller's deferred cleanup can
// Stop the orphans. policyRejected may legitimately be nil when no topic is
// configured; it is normalised to a true-nil interface so caller nil-checks work.
var localValidatorKafkaProducers = func(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (
	kafka.KafkaAsyncProducerI, kafka.KafkaAsyncProducerI, kafka.KafkaAsyncProducerI, error,
) {
	txmeta, err := getKafkaTxmetaAsyncProducer(ctx, logger, appSettings)
	if err != nil {
		return nil, nil, nil, errors.NewServiceError("could not create txmeta kafka producer for local validator", err)
	}

	rejectedTx, err := getKafkaRejectedTxAsyncProducer(ctx, logger, appSettings)
	if err != nil {
		// txmeta is already created — hand it back so the caller's cleanup Stops it.
		return txmeta, nil, nil, errors.NewServiceError("could not create rejectedTx kafka producer for local validator", err)
	}

	policyRejected, err := getKafkaTxPolicyRejectedAsyncProducer(ctx, logger, appSettings)
	if err != nil {
		return txmeta, rejectedTx, nil, errors.NewServiceError("could not create policy-rejected tx kafka producer for local validator", err)
	}

	if policyRejected == nil {
		// No policy-rejected topic configured: return a true-nil interface so the
		// caller's nil-checks (cleanup loop, validator.New) behave correctly.
		return txmeta, rejectedTx, nil, nil
	}

	return txmeta, rejectedTx, policyRejected, nil
}

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

		// The async Kafka producers are created before the *Validator that will
		// own them exists. Declare the holders and register the cleanup BEFORE
		// creating any of them, so a failure partway through producer creation —
		// or in any dependent-client / validator.New step below — Stops whichever
		// producers were already created instead of leaking them. (Registering the
		// defer only after all three are created would miss a failure at the 2nd
		// or 3rd producer.) The cleanup is a no-op once ownership transfers to the
		// retained *Validator, whose Close drives the real drain+stop on shutdown;
		// producer Stop is idempotent.
		var (
			txMetaKafkaProducerClient           kafka.KafkaAsyncProducerI
			rejectedTxKafkaProducerClient       kafka.KafkaAsyncProducerI
			policyRejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
		)

		ownershipTransferred := false

		defer func() {
			if ownershipTransferred {
				return
			}

			for _, p := range []kafka.KafkaAsyncProducerI{
				txMetaKafkaProducerClient,
				rejectedTxKafkaProducerClient,
				policyRejectedTxKafkaProducerClient,
			} {
				if p != nil {
					_ = p.Stop()
				}
			}
		}()

		txMetaKafkaProducerClient, rejectedTxKafkaProducerClient, policyRejectedTxKafkaProducerClient, err =
			localValidatorKafkaProducers(ctx, logger, appSettings)
		if err != nil {
			return nil, err
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
			policyRejectedTxKafkaProducerClient,
			blockAssemblyClient,
			blockchainClient,
		)
		if err != nil {
			return nil, errors.NewServiceError("could not create local validator", err)
		}

		// Memoize and retain the local validator, matching the gRPC branch below:
		// the daemon owns this *Validator instance and closes it (Validator.Close,
		// via the clientCloser contract) during closeClients. Without retaining it,
		// closeClients could not reach it and its producers/batcher would only
		// self-close on ctx-cancel — outside the bounded shutdown window.
		d.mainValidatorClient = validatorClient
		d.retainClient(d.mainValidatorClient)
		ownershipTransferred = true

		return d.mainValidatorClient, nil
	} else {
		d.mainValidatorClient, err = validator.NewClient(ctx, logger, appSettings)
		if err != nil {
			return nil, errors.NewServiceError("could not create validator client", err)
		}

		d.retainClient(d.mainValidatorClient)
	}

	return d.mainValidatorClient, nil
}

// GetBlobDeletionScheduler returns a blob deletion scheduler (blockchain client).
// The blockchain client implements BlobDeletionScheduler interface directly.
func (d *Stores) GetBlobDeletionScheduler(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (options.BlobDeletionScheduler, error) {
	if d.mainBlobDeletionScheduler != nil {
		return d.mainBlobDeletionScheduler, nil
	}

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "blob-deletion")
	if err != nil {
		return nil, errors.NewServiceError("failed to create blockchain client for blob deletion scheduling", err)
	}

	d.mainBlobDeletionScheduler = blockchainClient
	logger.Infof("Blob deletion scheduling enabled via blockchain service")
	return d.mainBlobDeletionScheduler, nil
}

// GetTxStore returns the main transaction store instance. If the store hasn't been initialized yet,
// it creates a new one using the configured URL from settings. This function ensures only one
// instance of the transaction store exists.
func (d *Stores) GetTxStore(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blob.Store, error) {
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

	// Get blob deletion scheduler (blockchain client)
	blobDeletionScheduler, err := d.GetBlobDeletionScheduler(ctx, logger, appSettings)
	if err != nil {
		return nil, errors.NewServiceError("could not get blob deletion scheduler for tx store", err)
	}

	d.mainTxStore, err = blob.NewStore(logger, txStoreURL,
		options.WithHashPrefix(hashPrefix),
		options.WithBlobDeletionScheduler(blobDeletionScheduler),
		options.WithStoreType(storetypes.TXSTORE))
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

	// Get blob deletion scheduler (blockchain client)
	blobDeletionScheduler, err := d.GetBlobDeletionScheduler(ctx, logger, appSettings)
	if err != nil {
		return nil, errors.NewServiceError("could not get blob deletion scheduler for subtree store", err)
	}

	d.mainSubtreeStore, err = blob.NewStore(logger, subtreeStoreURL,
		options.WithHashPrefix(hashPrefix),
		options.WithBlockHeightCh(ch),
		options.WithBlobDeletionScheduler(blobDeletionScheduler),
		options.WithStoreType(storetypes.SUBTREESTORE))
	if err != nil {
		return nil, errors.NewServiceError("could not create subtree store", err)
	}

	return d.mainSubtreeStore, nil
}

func (d *Stores) GetBlockchainStore(_ context.Context, logger ulogger.Logger, appSettings *settings.Settings) (blockchainstore.Store, error) {
	if d.mainBlockchainStore != nil {
		return d.mainBlockchainStore, nil
	}

	// Create the blockchain store url from the app settings
	blockchainStoreURL := appSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return nil, errors.NewStorageError("blockchain store url not found")
	}

	// Create the blockchain store
	blockchainStore, err := blockchainstore.NewStore(logger, blockchainStoreURL, appSettings)
	if err != nil {
		return nil, err
	}

	d.mainBlockchainStore = blockchainStore
	return blockchainStore, nil
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

	var err error

	hashPrefix := 0
	if tempStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(tempStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewConfigurationError("tempstore hashPrefix config error", err)
		}
	}

	blockchainClient, err := d.GetBlockchainClient(ctx, logger, appSettings, "temp")
	if err != nil {
		return nil, errors.NewServiceError("could not create blockchain client for temp store", err)
	}

	ch, err := getBlockHeightTrackerCh(ctx, logger, blockchainClient)
	if err != nil {
		return nil, errors.NewServiceError("could not create block height tracker channel", err)
	}

	// Get blob deletion scheduler (blockchain client)
	blobDeletionScheduler, err := d.GetBlobDeletionScheduler(ctx, logger, appSettings)
	if err != nil {
		return nil, errors.NewServiceError("could not get blob deletion scheduler for temp store", err)
	}

	d.mainTempStore, err = blob.NewStore(logger, tempStoreURL,
		options.WithHashPrefix(hashPrefix),
		options.WithBlockHeightCh(ch),
		options.WithBlobDeletionScheduler(blobDeletionScheduler),
		options.WithStoreType(storetypes.TEMPSTORE))
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

	// Get blob deletion scheduler (blockchain client)
	blobDeletionScheduler, err := d.GetBlobDeletionScheduler(ctx, logger, appSettings)
	if err != nil {
		return nil, errors.NewServiceError("could not get blob deletion scheduler for block store", err)
	}

	d.mainBlockStore, err = blob.NewStore(logger, blockStoreURL,
		options.WithHashPrefix(hashPrefix),
		options.WithBlockHeightCh(ch),
		options.WithBlobDeletionScheduler(blobDeletionScheduler),
		options.WithStoreType(storetypes.BLOCKSTORE))
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

	// Get blob deletion scheduler (blockchain client)
	blobDeletionScheduler, err := d.GetBlobDeletionScheduler(ctx, logger, appSettings)
	if err != nil {
		return nil, errors.NewServiceError("could not get blob deletion scheduler for block persister store", err)
	}

	d.mainBlockPersisterStore, err = blob.NewStore(logger, blockStoreURL,
		options.WithHashPrefix(hashPrefix),
		options.WithBlockHeightCh(ch),
		options.WithBlobDeletionScheduler(blobDeletionScheduler),
		options.WithStoreType(storetypes.BLOCKPERSISTERSTORE))
	if err != nil {
		return nil, errors.NewServiceError("could not create block persister store", err)
	}

	return d.mainBlockPersisterStore, nil
}

// Cleanup resets all singleton stores. This is particularly important for tests
// where stores may persist between test runs.
func (d *Stores) Cleanup() {
	// closeStores (the daemon's deferred shutdown drain) reads these store
	// pointers under globalStoreMutex.RLock and can run concurrently with test
	// teardown, which calls Cleanup. Take the write lock so the two are
	// synchronised — otherwise the -race detector flags a read/write data race
	// on the store fields during daemon shutdown.
	globalStoreMutex.Lock()
	d.mainBlockPersisterStore = nil
	d.mainBlockStore = nil
	// closeStores now closes mainBlockchainStore (DC9); nil it here too so
	// GetBlockchainStore constructs a fresh store on reuse instead of handing
	// back a closed cached handle. The other closed singletons (block /
	// block-persister stores, peer-registry client) are already nil'd below.
	d.mainBlockchainStore = nil
	d.mainBlockValidationClient = nil
	d.mainSubtreeStore = nil
	d.mainSubtreeValidationClient = nil
	d.mainTempStore = nil
	d.mainTxStore = nil
	d.mainUtxoStore = nil
	d.mainValidatorClient = nil
	d.mainP2PClient = nil
	d.mainPeerRegistryClient = nil
	globalStoreMutex.Unlock()

	// Reset the Aerospike cleanup service singleton if it exists
	// This prevents state leakage between test runs
	aerospike.ResetPrunerServiceForTests()
}
