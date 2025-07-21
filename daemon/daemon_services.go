package daemon

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/alert"
	"github.com/bitcoin-sv/teranode/services/asset"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockpersister"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/legacy"
	"github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/bitcoin-sv/teranode/services/p2p"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/services/rpc"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// serviceStarter defines a service that can be started based on command line arguments or config settings.
type serviceStarter struct {
	shouldStart bool
	startFunc   func() error
}

// startServices starts the services based on the command line arguments, and the config file
// nolint:gocognit // This function is complex due to the number of services and their dependencies.
func (d *Daemon) startServices(ctx context.Context, logger ulogger.Logger, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, args []string, readyCh chan<- struct{}) error {
	var (
		closeOnce sync.Once
	)

	// Create logger using the factory
	createLogger := d.loggerFactory

	// Check all the command line arguments to determine which services to start
	help := d.shouldStart(serviceHelp, args)
	startBlockchain := d.shouldStart(serviceBlockchainFormal, args)
	startBlockAssembly := d.shouldStart(serviceBlockAssemblyFormal, args)
	startSubtreeValidation := d.shouldStart(serviceSubtreeValidationFormal, args)
	startBlockValidation := d.shouldStart(serviceBlockValidationFormal, args)
	startValidator := d.shouldStart(serviceValidatorFormal, args)
	startPropagation := d.shouldStart(servicePropagationFormal, args)
	startP2P := d.shouldStart(serviceNameP2PFormal, args)
	startAsset := d.shouldStart(serviceAssetFormal, args)
	startBlockPersister := d.shouldStart(serviceBlockPersisterFormal, args)
	startUTXOPersister := d.shouldStart(serviceUtxoPersisterFormal, args)
	startLegacy := d.shouldStart(serviceLegacyFormal, args)
	startRPC := d.shouldStart(serviceRPCFormal, args)
	startAlert := d.shouldStart(serviceAlertFormal, args)

	// Create the application count based on the services that are going to be started
	d.appCount += len(d.externalServices)

	// If no services are started, print usage and exit
	if help || d.appCount == 0 {
		printUsage()
		return nil
	}

	// start the profiler if enabled
	startProfiler(logger, appSettings)

	if appSettings.UseDatadogProfiler {
		deferFn := datadogProfiler()
		defer deferFn()
	}

	// start prometheus metrics endpoint if enabled
	prometheusEndpoint := appSettings.PrometheusEndpoint
	if prometheusEndpoint != "" && !metricsRegistered.Load() {
		metricsRegistered.Store(true)
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	// start tracing if enabled
	if appSettings.TracingEnabled {
		logger.Infof("Starting tracer")

		err := tracing.InitTracer(appSettings)
		if err != nil {
			logger.Warnf("failed to initialize tracer: %v", err)
		}
	}

	// Create a slice of service starters
	starters := []serviceStarter{
		{startBlockchain, func() error { return startBlockchainService(ctx, appSettings, sm, args, createLogger) }},
		{startP2P, func() error { return startP2PService(ctx, d, appSettings, sm, createLogger) }},
		{startAsset, func() error { return startAssetService(ctx, d, appSettings, sm, createLogger) }},
		{startRPC, func() error { return startRPCService(ctx, d, appSettings, sm, createLogger) }},
		{startAlert, func() error { return startAlertService(ctx, d, appSettings, sm, createLogger) }},
		{startBlockPersister, func() error { return startBlockPersisterService(ctx, d, appSettings, sm, createLogger) }},
		{startUTXOPersister, func() error { return startUTXOPersisterService(ctx, d, appSettings, sm, createLogger) }},
		{startBlockAssembly, func() error { return startBlockAssemblyService(ctx, d, appSettings, sm, createLogger) }},
		{startSubtreeValidation, func() error { return startSubtreeValidationService(ctx, d, appSettings, sm, createLogger) }},
		{startBlockValidation, func() error { return startBlockValidationService(ctx, d, appSettings, sm, createLogger) }},
		{startValidator, func() error { return startValidatorService(ctx, d, appSettings, sm, createLogger) }},
		{startPropagation, func() error { return startPropagationService(ctx, d, appSettings, sm, createLogger) }},
		{startLegacy, func() error { return startLegacyService(ctx, d, appSettings, sm, createLogger, logger) }},
	}

	// Loop through and start each service if needed
	for _, s := range starters {
		if s.shouldStart {
			if err := s.startFunc(); err != nil {
				return err
			}
		}
	}

	// look through all external services and add them to the ServiceManager
	for _, exService := range d.externalServices {
		service, err := exService.InitFunc()
		if err != nil {
			return err
		}

		if err = sm.AddService(exService.Name, service); err != nil {
			return err
		}
	}

	// If the ready channel is provided, wait for the service to be ready
	if readyCh != nil {
		sm.WaitForServiceToBeReady()
		closeOnce.Do(func() { close(readyCh) })
	}

	return nil
}

// startProfiler initializes and starts the profiler if the address is set in the app settings.
func startProfiler(logger ulogger.Logger, appSettings *settings.Settings) {
	profilerAddr := appSettings.ProfilerAddr
	if profilerAddr != "" && !pprofRegistered.Load() {
		pprofRegistered.Store(true)

		go func() {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			gocore.RegisterStatsHandlers()

			prefix := appSettings.StatsPrefix
			logger.Infof("StatsServer listening on http://%s/%s/stats", profilerAddr, prefix)

			server := &http.Server{
				Addr:         profilerAddr,
				Handler:      nil,
				ReadTimeout:  60 * time.Second,
				WriteTimeout: 60 * time.Second,
				IdleTimeout:  120 * time.Second,
			}

			// http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
			logger.Fatalf("%v", server.ListenAndServe())
		}()
	}
}

// startBlockchainService initializes and starts the Blockchain service.
func startBlockchainService(ctx context.Context, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, args []string, createLogger func(string) ulogger.Logger) error {
	// Create the blockchain store url from the app settings
	blockchainStoreURL := appSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return errors.NewStorageError("blockchain store url not found")
	}

	// Create the blockchain store
	blockchainStore, err := blockchainstore.NewStore(
		createLogger(loggerBlockchainSQL), blockchainStoreURL, appSettings,
	)
	if err != nil {
		return err
	}

	// Create the Kafka async producer for final blocks
	var blocksFinalKafkaAsyncProducer *kafka.KafkaAsyncProducer

	blocksFinalKafkaAsyncProducer, err = getKafkaBlocksFinalAsyncProducer(
		ctx, createLogger(loggerKafkaProducerBlockFinal), appSettings,
	)
	if err != nil {
		return err
	}

	var localTestStartFromState string

	// Look through the command line arguments to find the local test start from state
	for _, cmd := range args {
		if strings.HasPrefix(cmd, "-localTestStartFromState=") {
			localTestStartFromState = strings.SplitN(cmd, "=", 2)[1]
			break
		}
	}

	if localTestStartFromState == "" {
		localTestStartFromState = appSettings.LocalTestStartFromState
	}

	// Create new blockchain service
	var blockchainService *blockchain.Blockchain

	blockchainService, err = blockchain.New(
		ctx, createLogger(loggerBlockchain), appSettings, blockchainStore,
		blocksFinalKafkaAsyncProducer, localTestStartFromState,
	)
	if err != nil {
		return err
	}

	// Add the blockchain service to the ServiceManager
	return sm.AddService(serviceBlockchainFormal, blockchainService)
}

// startP2PService initializes and starts the P2P service.
func startP2PService(ctx context.Context, daemon *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Create a blockchain client for the P2P service
	blockchainClient, err := daemon.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceNameP2P,
	)
	if err != nil {
		return err
	}

	// Create a Kafka consumer group for rejected transactions
	var rejectedTxKafkaConsumerClient *kafka.KafkaConsumerGroup

	rejectedTxKafkaConsumerClient, err = getKafkaRejectedTxConsumerGroup(
		createLogger(loggerKafkaProducerRejectedTx), appSettings, serviceNameP2P+"."+appSettings.ClientName,
	)
	if err != nil {
		return err
	}

	invalidBlocksKafkaConsumerClient, err := getKafkaInvalidBlocksConsumerGroup(createLogger("kpib"), appSettings, serviceNameP2P+"."+appSettings.ClientName)
	if err != nil {
		return err
	}

	invalidSubtreeKafkaConsumerClient, err := getKafkaInvalidSubtreeConsumerGroup(createLogger("kpis"), appSettings, serviceNameP2P+"."+appSettings.ClientName)
	if err != nil {
		return err
	}

	// Create Kafka producers for subtrees and blocks
	var subtreeKafkaProducerClient *kafka.KafkaAsyncProducer

	subtreeKafkaProducerClient, err = getKafkaSubtreesAsyncProducer(
		ctx, createLogger(loggerKafkaProducerSubtree), appSettings,
	)
	if err != nil {
		return err
	}

	// Create Kafka producer for blocks
	var blocksKafkaProducerClient *kafka.KafkaAsyncProducer

	blocksKafkaProducerClient, err = getKafkaBlocksAsyncProducer(
		ctx, createLogger(loggerKafkaProducerBlocks), appSettings,
	)
	if err != nil {
		return err
	}

	p2pLogger := createLogger(loggerP2P)
	p2pLogger.SetLogLevel(appSettings.LogLevel)

	// Initialize the P2P server with the necessary parts
	var p2pService *p2p.Server

	p2pService, err = p2p.NewServer(
		ctx, p2pLogger, appSettings, blockchainClient,
		rejectedTxKafkaConsumerClient,
		invalidBlocksKafkaConsumerClient,
		invalidSubtreeKafkaConsumerClient,
		subtreeKafkaProducerClient,
		blocksKafkaProducerClient,
	)
	if err != nil {
		return err
	}

	// Add the P2P service to the ServiceManager
	return sm.AddService(serviceNameP2PFormal, p2pService)
}

// startAssetService initializes and starts the Asset service.
func startAssetService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Get the UTXO store for the Asset service
	utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Get the transaction store for the Asset service
	var txStore blob.Store

	txStore, err = d.daemonStores.GetTxStore(createLogger(loggerTransactions), appSettings)
	if err != nil {
		return err
	}

	// Get the subtree store for the Asset service
	var subtreeStore blob.Store

	subtreeStore, err = d.daemonStores.GetSubtreeStore(
		ctx, createLogger(loggerSubtrees), appSettings,
	)
	if err != nil {
		return err
	}

	// Get the block persister store for the Asset service
	var blockPersisterStore blob.Store

	blockPersisterStore, err = d.daemonStores.GetBlockPersisterStore(
		ctx, createLogger(loggerBlockPersisterStore), appSettings,
	)
	if err != nil {
		return err
	}

	// Get the blockchain client for the Asset service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceAsset,
	)
	if err != nil {
		return err
	}

	// Initialize the Asset service with the necessary parts
	return sm.AddService(serviceAssetFormal, asset.NewServer(
		createLogger(serviceAsset),
		appSettings,
		utxoStore,
		txStore,
		subtreeStore,
		blockPersisterStore,
		blockchainClient,
	))
}

// startRPCService initializes and adds the RPC service to the ServiceManager.
func startRPCService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Create blockchain client for the RPC service
	blockchainClient, err := d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceRPC,
	)
	if err != nil {
		return err
	}

	// Create UTXO store for the RPC service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Create the RPC server with the necessary parts
	var rpcServer *rpc.RPCServer

	rpcServer, err = rpc.NewServer(createLogger(loggerRPC), appSettings, blockchainClient, utxoStore)
	if err != nil {
		return err
	}

	// Add the RPC service to the ServiceManager
	return sm.AddService(serviceRPCFormal, rpcServer)
}

// startAlertService initializes and adds the Alert service to the ServiceManager.
func startAlertService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Create the blockchain client for the Alert service
	blockchainClient, err := d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceAlert,
	)
	if err != nil {
		return err
	}

	// Create the UTXO store for the Alert service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Create the block assembly client for the Alert service
	var blockAssemblyClient *blockassembly.Client

	blockAssemblyClient, err = blockassembly.NewClient(ctx, createLogger(loggerBlockAssembly), appSettings)
	if err != nil {
		return err
	}

	// Create the peer client for the Alert service
	var peerClient peer.ClientI

	peerClient, err = peer.NewClient(ctx, createLogger(loggerPeerClient), appSettings)
	if err != nil {
		return err
	}

	// Create the P2P client for the Alert service
	var p2pClient p2p.ClientI

	p2pClient, err = p2p.NewClient(ctx, createLogger(loggerP2P), appSettings)
	if err != nil {
		return err
	}

	// Create the Alert service with the necessary parts
	return sm.AddService(serviceAlertFormal, alert.New(
		createLogger(serviceAlert),
		appSettings,
		blockchainClient,
		utxoStore,
		blockAssemblyClient,
		peerClient,
		p2pClient,
	))
}

// startBlockPersisterService initializes and adds the BlockPersister service to the ServiceManager.
func startBlockPersisterService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Create the block store for the BlockPersister service
	blockStore, err := d.daemonStores.GetBlockStore(ctx, createLogger(loggerBlockPersisterStore), appSettings)
	if err != nil {
		return err
	}

	// Create the subtree store for the BlockPersister service
	var subtreeStore blob.Store

	subtreeStore, err = d.daemonStores.GetSubtreeStore(ctx, createLogger(loggerSubtrees), appSettings)
	if err != nil {
		return err
	}

	// Create the UTXO store for the BlockPersister service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Create the blockchain client for the BlockPersister service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceBlockPersister,
	)
	if err != nil {
		return err
	}

	// Add the BlockPersister service to the ServiceManager
	return sm.AddService(serviceBlockPersisterFormal, blockpersister.New(ctx,
		createLogger(serviceBlockPersister),
		appSettings,
		blockStore,
		subtreeStore,
		utxoStore,
		blockchainClient,
	))
}

// startUTXOPersisterService initializes and adds the UTXOPersister service to the ServiceManager.
func startUTXOPersisterService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Create the block store for the UTXOPersister service
	blockStore, err := d.daemonStores.GetBlockStore(ctx, createLogger(loggerBlockPersisterStore), appSettings)
	if err != nil {
		return err
	}

	// Create the blockchain client for the UTXOPersister service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceUtxoPersister,
	)
	if err != nil {
		return err
	}

	// Add the UTXOPersister service to the ServiceManager
	return sm.AddService(serviceUtxoPersisterFormal, utxopersister.New(ctx,
		createLogger(serviceUtxoPersister),
		appSettings,
		blockStore,
		blockchainClient,
	))
}

// startBlockAssemblyService initializes and adds the BlockAssembly service to the ServiceManager.
func startBlockAssemblyService(ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Check if the BlockAssembly service should be started
	if appSettings.BlockAssembly.GRPCListenAddress == "" {
		return nil
	}

	// Create the transaction store for the BlockAssembly service
	txStore, err := d.daemonStores.GetTxStore(createLogger(loggerTransactions), appSettings)
	if err != nil {
		return err
	}

	// Create the UTXO store for the BlockAssembly service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Create the subtree store for the BlockAssembly service
	var subtreeStore blob.Store

	subtreeStore, err = d.daemonStores.GetSubtreeStore(ctx, createLogger(loggerSubtrees), appSettings)
	if err != nil {
		return err
	}

	// Create the blockchain client for the BlockAssembly service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceBlockAssembly,
	)
	if err != nil {
		return err
	}

	// Add the BlockAssembly service to the ServiceManager
	return sm.AddService(serviceBlockAssemblyFormal, blockassembly.New(
		createLogger(serviceBlockAssembly),
		appSettings,
		txStore,
		utxoStore,
		subtreeStore,
		blockchainClient,
	))
}

// startSubtreeValidationService initializes and adds the SubtreeValidation service to the ServiceManager.
func startSubtreeValidationService(
	ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager,
	createLogger func(string) ulogger.Logger,
) error {
	return startValidationService(ctx, d, appSettings, sm, createLogger, serviceSubtreeValidation)
}

// startBlockValidationService initializes and adds the BlockValidation service to the ServiceManager.
func startBlockValidationService(
	ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager,
	createLogger func(string) ulogger.Logger,
) error {
	return startValidationService(ctx, d, appSettings, sm, createLogger, serviceBlockValidation)
}

// startValidationService consolidates shared setup logic for both SubtreeValidation and
// BlockValidation services and registers the resulting service with the ServiceManager.
// Pass serviceSubtreeValidation or serviceBlockValidation to start the desired service.
func startValidationService(
	ctx context.Context, d *Daemon,
	appSettings *settings.Settings, sm *servicemanager.ServiceManager,
	createLogger func(string) ulogger.Logger, validationType string,
) error {
	// Common dependencies shared by all validation services
	subtreeStore, err := d.daemonStores.GetSubtreeStore(ctx, createLogger(loggerSubtrees), appSettings)
	if err != nil {
		return err
	}

	// Get the tx store for the validation service
	var txStore blob.Store

	txStore, err = d.daemonStores.GetTxStore(createLogger(loggerTransactions), appSettings)
	if err != nil {
		return err
	}

	// Get the UTXO store for the validation service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Get the validator client for the validation service
	var validatorClient validator.Interface

	validatorClient, err = d.daemonStores.GetValidatorClient(ctx, createLogger(loggerTxValidator), appSettings)
	if err != nil {
		return err
	}

	// Get the blockchain client for the validation service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx,
		createLogger(loggerBlockchainClient),
		appSettings,
		validationType,
	)
	if err != nil {
		return err
	}

	switch validationType {
	case serviceSubtreeValidation:
		// Kafka consumers for subtree validation
		var subtreeConsumerClient *kafka.KafkaConsumerGroup

		subtreeConsumerClient, err = getKafkaSubtreesConsumerGroup(
			createLogger(loggerKafkaConsumerSubtree),
			appSettings,
			serviceSubtreeValidation+"."+appSettings.ClientName,
		)
		if err != nil {
			return err
		}

		// Get the Kafka consumer group for transaction metadata
		var txMetaConsumerClient *kafka.KafkaConsumerGroup

		txMetaConsumerClient, err = getKafkaTxmetaConsumerGroup(
			createLogger(loggerKafkaConsumerTxMeta),
			appSettings,
			serviceSubtreeValidation+"."+appSettings.ClientName,
		)
		if err != nil {
			return err
		}

		// Create the SubtreeValidation service
		var service *subtreevalidation.Server

		service, err = subtreevalidation.New(
			ctx,
			createLogger(loggerSubtreeValidation),
			appSettings,
			subtreeStore,
			txStore,
			utxoStore,
			validatorClient,
			blockchainClient,
			subtreeConsumerClient,
			txMetaConsumerClient,
		)
		if err != nil {
			return err
		}

		// Add the SubtreeValidation service to the ServiceManager
		return sm.AddService(serviceSubtreeValidationFormal, service)

	case serviceBlockValidation:
		// Skip if disabled via config
		if appSettings.BlockValidation.GRPCListenAddress == "" {
			return nil
		}

		// Kafka consumer for blocks
		var kafkaConsumerClient *kafka.KafkaConsumerGroup

		kafkaConsumerClient, err = getKafkaBlocksConsumerGroup(
			createLogger(loggerKafkaConsumerBlocks),
			appSettings,
			serviceBlockValidation+"."+appSettings.ClientName,
		)
		if err != nil {
			return err
		}

		// Create the BlockValidation service
		d.blockValidationSrv = blockvalidation.New(
			createLogger(loggerBlockValidation),
			appSettings,
			subtreeStore,
			txStore,
			utxoStore,
			validatorClient,
			blockchainClient,
			kafkaConsumerClient,
		)

		// Add the BlockValidation service to the ServiceManager
		return sm.AddService(serviceBlockValidationFormal, d.blockValidationSrv)

	default:
		// Return an error if the validation type is unknown
		return errors.New(9, "unknown validation type: "+validationType)
	}
}

// startValidatorService initializes and adds the Validator service to the ServiceManager.
func startValidatorService(
	ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Check if the Validator service should be started
	if appSettings.Validator.GRPCListenAddress == "" {
		return nil
	}

	var err error

	// Get the utxo store for the Validator service
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, createLogger(loggerUtxos), appSettings)
	if err != nil {
		return err
	}

	// Get the blockchain client for the Validator service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, serviceValidator,
	)
	if err != nil {
		return err
	}

	// Get the consumer client for the Validator service
	var consumerClient kafka.KafkaConsumerGroupI

	consumerClient, err = getKafkaTxConsumerGroup(
		createLogger(loggerKafkaConsumerTransaction), appSettings, serviceValidator+"."+appSettings.ClientName,
	)
	if err != nil {
		return err
	}

	// Get the kafka producer clients for transactions and rejected transactions
	var txMetaKafkaProducerClient *kafka.KafkaAsyncProducer

	txMetaKafkaProducerClient, err = getKafkaTxmetaAsyncProducer(
		ctx, createLogger(loggerKafkaConsumerTxMeta), appSettings,
	)
	if err != nil {
		return err
	}

	// Get the kafka producer client for rejected transactions
	var rejectedTxKafkaProducerClient *kafka.KafkaAsyncProducer

	rejectedTxKafkaProducerClient, err = getKafkaRejectedTxAsyncProducer(
		ctx, createLogger(loggerKafkaConsumerRejectedTx), appSettings,
	)
	if err != nil {
		return err
	}

	// Create the BlockAssembly client for the Validator service
	var blockAssemblyClient *blockassembly.Client

	blockAssemblyClient, err = blockassembly.NewClient(
		ctx, createLogger(loggerBlockAssembly), appSettings,
	)
	if err != nil {
		return err
	}

	// Add the Validator service to the ServiceManager
	return sm.AddService(serviceValidatorFormal, validator.NewServer(
		createLogger(serviceValidator),
		appSettings,
		utxoStore,
		blockchainClient,
		consumerClient,
		txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient,
		blockAssemblyClient,
	))
}

// startPropagationService initializes and adds the Propagation service to the ServiceManager.
func startPropagationService(
	ctx context.Context, d *Daemon, appSettings *settings.Settings,
	sm *servicemanager.ServiceManager, createLogger func(string) ulogger.Logger) error {
	// Check if the Propagation service should be started
	if appSettings.Propagation.GRPCListenAddress == "" {
		return nil
	}

	var err error

	// Get the transaction store for the Propagation service
	var txStore blob.Store

	txStore, err = d.daemonStores.GetTxStore(createLogger(loggerTransactions), appSettings)
	if err != nil {
		return err
	}

	// Get the validator client for the Propagation service
	var validatorClient validator.Interface

	validatorClient, err = d.daemonStores.GetValidatorClient(
		ctx, createLogger(loggerTxValidator), appSettings,
	)
	if err != nil {
		return err
	}

	// Get the blockchain client for the Propagation service
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(
		ctx, createLogger(loggerBlockchainClient), appSettings, servicePropagation,
	)
	if err != nil {
		return err
	}

	// Create the logger for the Propagation service
	var validatorKafkaProducerClient kafka.KafkaAsyncProducerI

	validatorKafkaProducerClient, err = getKafkaTxAsyncProducer(ctx, createLogger(loggerKafkaConsumerTransaction), appSettings)
	if err != nil {
		return err
	}

	// Add the Propagation service to the ServiceManager
	return sm.AddService(servicePropagationFormal, propagation.New(
		createLogger(loggerPropagation),
		appSettings,
		txStore,
		validatorClient,
		blockchainClient,
		validatorKafkaProducerClient,
	))
}

// startLegacyService initializes and adds the Legacy service to the ServiceManager.
func startLegacyService(
	ctx context.Context, d *Daemon,
	appSettings *settings.Settings, sm *servicemanager.ServiceManager,
	createLogger func(string) ulogger.Logger, logger ulogger.Logger) error {
	// if appSettings.ChainCfgParams.Net == chaincfg.RegressionNetParams.Net {
	// 	logger.Warnf("legacy service not supported in regtest mode. Skipping legacy service...")
	// 	return nil
	// }
	var err error

	// Get the subtree store
	var subtreeStore blob.Store

	subtreeStore, err = d.daemonStores.GetSubtreeStore(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the temporary store
	var tempStore blob.Store

	tempStore, err = d.daemonStores.GetTempStore(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the UTXO store
	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the validator client
	var validatorClient validator.Interface

	validatorClient, err = d.daemonStores.GetValidatorClient(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the blockchain client
	var blockchainClient blockchain.ClientI

	blockchainClient, err = d.daemonStores.GetBlockchainClient(ctx, logger, appSettings, serviceLegacy)
	if err != nil {
		return err
	}

	// Get the subtree validation client
	var subtreeValidationClient subtreevalidation.Interface

	subtreeValidationClient, err = d.daemonStores.GetSubtreeValidationClient(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the block validation client
	var blockValidationClient blockvalidation.Interface

	blockValidationClient, err = d.daemonStores.GetBlockValidationClient(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Get the block assembly client
	var blockassemblyClient *blockassembly.Client

	blockassemblyClient, err = blockassembly.NewClient(ctx, logger, appSettings)
	if err != nil {
		return err
	}

	// Add the Legacy service to the ServiceManager
	return sm.AddService(serviceLegacyFormal, legacy.New(
		createLogger(serviceLegacy),
		appSettings,
		blockchainClient,
		validatorClient,
		subtreeStore,
		tempStore,
		utxoStore,
		subtreeValidationClient,
		blockValidationClient,
		blockassemblyClient,
	))
}
