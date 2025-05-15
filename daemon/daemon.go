package daemon

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	pprofRegistered   atomic.Bool
	metricsRegistered atomic.Bool
	healthRegistered  atomic.Bool
	traceCloser       io.Closer
	globalStoreMu     sync.RWMutex
)

// Option is a functional option type for configuring the Daemon.
type Option func(*Daemon)

// WithLoggerFactory provides a custom logger factory for the Daemon and its services.
func WithLoggerFactory(factory func(serviceName string) ulogger.Logger) Option {
	return func(d *Daemon) {
		d.loggerFactory = factory
	}
}

func WithContext(ctx context.Context) Option {
	return func(d *Daemon) {
		d.Ctx = ctx
	}
}

type externalService struct {
	Name     string
	InitFunc func() (servicemanager.Service, error)
}

type Daemon struct {
	Ctx           context.Context
	doneCh        chan struct{}
	closeDoneOnce sync.Once

	stopCh           chan struct{} // Channel to signal when all services have stopped
	closeStopOnce    sync.Once
	serverMu         sync.Mutex
	server           *http.Server // Add this field
	ServiceManager   *servicemanager.ServiceManager
	externalServices []*externalService
	loggerFactory    func(serviceName string) ulogger.Logger // Factory for creating loggers
	daemonStores     *DaemonStores
	appCount         int
}

func New(opts ...Option) *Daemon {
	ctx := context.Background()

	d := &Daemon{
		Ctx:              ctx,
		closeDoneOnce:    sync.Once{},
		closeStopOnce:    sync.Once{},
		doneCh:           make(chan struct{}),
		stopCh:           make(chan struct{}),
		server:           nil,
		externalServices: make([]*externalService, 0),
		// Default logger factory
		loggerFactory: func(serviceName string) ulogger.Logger {
			return ulogger.New(serviceName)
		},
		daemonStores: &DaemonStores{},
	}

	// Apply functional options
	for _, opt := range opts {
		opt(d)
	}

	// Initialize ServiceManager with the configured logger factory
	d.ServiceManager = servicemanager.NewServiceManager(d.Ctx, d.loggerFactory("ServiceManager"))

	return d
}

func (d *Daemon) AddExternalService(name string, initFunc func() (servicemanager.Service, error)) {
	d.externalServices = append(d.externalServices, &externalService{
		Name:     name,
		InitFunc: initFunc,
	})
}

func (d *Daemon) Stop(timeout ...time.Duration) error {
	logger := d.loggerFactory("Daemon")

	if traceCloser != nil {
		_ = traceCloser.Close()
		traceCloser = nil
	}

	d.serverMu.Lock()
	// Gracefully shutdown the HTTP server if it exists
	if d.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := d.server.Shutdown(ctx); err != nil {
			// Log error but continue with shutdown
			logger.Warnf("Error shutting down health check server: %v", err)
		}
	}
	d.serverMu.Unlock()

	// Use sync.Once to ensure channels are closed only once
	d.closeDoneOnce.Do(func() { close(d.doneCh) })

	if d.appCount == 0 {
		// Ensure stopCh is closed only once as well
		d.closeStopOnce.Do(func() { close(d.stopCh) })
		return nil
	}

	// Default timeout of 10 seconds if not provided
	shutdownTimeout := 10 * time.Second
	if len(timeout) > 0 {
		shutdownTimeout = timeout[0]
	}

	// Set up a timeout channel
	timeoutCh := time.After(shutdownTimeout)

	// Wait for services to shut down or timeout
	select {
	case <-d.stopCh: // Wait for all services to complete
		return nil
	case <-timeoutCh:
		logger.Warnf("Timeout waiting for services to stop after %v", shutdownTimeout)

		d.updateServiceStatuses(logger)

		return errors.NewProcessingError("timeout waiting for services to stop after %v", shutdownTimeout)
	}
}

func (d *Daemon) updateServiceStatuses(logger ulogger.Logger) {
	// Get detailed information about all services
	type serviceStatus struct {
		Name   string
		Status string
	}

	serviceStatuses := make([]serviceStatus, 0)

	// Directly access ServiceManager fields using reflection to extract service names
	// This is only used for debugging purposes during timeout
	smValue := reflect.ValueOf(d.ServiceManager).Elem()
	servicesField := smValue.FieldByName("services")

	if servicesField.IsValid() && servicesField.Kind() == reflect.Slice {
		for i := 0; i < servicesField.Len(); i++ {
			serviceWrapper := servicesField.Index(i)
			nameField := serviceWrapper.FieldByName("name")

			if nameField.IsValid() && nameField.Kind() == reflect.String {
				serviceName := nameField.String()
				serviceStatuses = append(serviceStatuses, serviceStatus{
					Name:   serviceName,
					Status: "Running",
				})
			}
		}
	}

	if len(serviceStatuses) > 0 {
		logger.Warnf("The following services are still running:")

		for _, status := range serviceStatuses {
			logger.Warnf("  - %s: %s", status.Name, status.Status)
		}
	} else {
		logger.Infof("No services were identified as running, but stopCh was not closed")
	}
}

func (d *Daemon) Start(logger ulogger.Logger, args []string, tSettings *settings.Settings, readyCh ...chan struct{}) {
	// Before continuing, if the command line contains "-wait_for_postgres=1", wait for postgres to be ready
	if d.shouldStart("wait_for_postgres", args) {
		if err := waitForPostgresToStart(logger, tSettings.PostgresCheckAddress); err != nil {
			logger.Errorf("error waiting for postgres: %v", err)
			return
		}
	}

	sm := d.ServiceManager

	var readyChInternal chan struct{}
	if len(readyCh) > 0 {
		readyChInternal = readyCh[0]
	}

	err := d.startServices(sm.Ctx, logger, tSettings, sm, args, readyChInternal)
	if err != nil {
		logger.Errorf("error starting services: %v", err)
		sm.ForceShutdown()
		// d.closeDoneOnce.Do(func() { close(d.doneCh) })
		d.closeDoneOnce.Do(func() { close(d.doneCh) })
	}

	util.RegisterPrometheusMetrics()

	mux := http.NewServeMux()
	healthFunc := func(liveness bool) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			status, details, err := sm.HealthHandler(sm.Ctx, liveness)
			if err != nil {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(details))

				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(details))
		}
	}
	mux.HandleFunc("/health", healthFunc(false))
	mux.HandleFunc("/health/readiness", healthFunc(false))
	mux.HandleFunc("/health/liveness", healthFunc(true))

	if !healthRegistered.Load() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("STOP USING THIS ENDPOINT - use port 8000/health/readiness or 8000/health/liveness"))
		})
		healthRegistered.Store(true)
	}

	port := tSettings.HealthCheckPort

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 20 * time.Second,  // Prevent Slowloris attacks
		ReadTimeout:       60 * time.Second,  // Maximum duration for reading entire request
		WriteTimeout:      60 * time.Second,  // Maximum duration before timing out writes of response
		IdleTimeout:       120 * time.Second, // Maximum amount of time to wait for the next request
	}

	d.serverMu.Lock()
	d.server = server // Store the reference
	d.serverMu.Unlock()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Error starting server: %v", err)
		}
	}()

	logger.Infof("Health check endpoint listening on http://localhost:%d/health", port)

	// Create a channel to receive the wait result
	waitErr := make(chan error, 1)
	go func() {
		waitErr <- sm.Wait()
	}()

	// Wait for either services to complete or doneCh to be closed
	select {
	case err := <-waitErr:
		if err != nil {
			logger.Errorf("services failed: %v", err)
		}
	case <-d.doneCh:
		logger.Infof("daemon shutdown requested")

		err = server.Shutdown(sm.Ctx)
		if err != nil {
			logger.Errorf("error shutting down server: %v", err)
		}

		// Close stores safely
		d.closeStores(logger, sm)

		sm.ForceShutdown()

		logger.Infof("daemon shutdown waiting for services to finish")

		// Wait for services to complete shutdown using the existing waitCh
		if err := <-waitErr; err != nil {
			logger.Errorf("error during service shutdown: %v", err)
		}

		logger.Infof("daemon shutdown completed")
	}

	d.closeStopOnce.Do(func() { close(d.stopCh) })
}

func (d *Daemon) closeStores(logger ulogger.Logger, sm *servicemanager.ServiceManager) {
	globalStoreMu.RLock()
	txStoreToClose := d.daemonStores.mainTxstore
	subtreeStoreToClose := d.daemonStores.mainSubtreestore
	tempStoreToClose := d.daemonStores.mainTempStore
	globalStoreMu.RUnlock()

	if txStoreToClose != nil {
		logger.Debugf("closing tx store")

		_ = txStoreToClose.Close(sm.Ctx)
	}

	if subtreeStoreToClose != nil {
		logger.Debugf("closing subtree store")

		_ = subtreeStoreToClose.Close(sm.Ctx)
	}

	if tempStoreToClose != nil {
		logger.Debugf("closing temp store")

		_ = tempStoreToClose.Close(sm.Ctx)
	}
}

// startServices starts the services based on the command line arguments and the config file
// nolint:gocognit
func (d *Daemon) startServices(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, sm *servicemanager.ServiceManager, args []string, readyCh chan<- struct{}) error {
	var (
		closeOnce sync.Once
	)

	// Create logger using the factory
	createLogger := d.loggerFactory

	help := d.shouldStart("help", args)
	startBlockchain := d.shouldStart("Blockchain", args)
	startBlockAssembly := d.shouldStart("BlockAssembly", args)
	startSubtreeValidation := d.shouldStart("SubtreeValidation", args)
	startBlockValidation := d.shouldStart("BlockValidation", args)
	startValidator := d.shouldStart("Validator", args)
	startPropagation := d.shouldStart("Propagation", args)
	startP2P := d.shouldStart("P2P", args)
	startAsset := d.shouldStart("Asset", args)
	startBlockPersister := d.shouldStart("BlockPersister", args)
	startUTXOPersister := d.shouldStart("UTXOPersister", args)
	startLegacy := d.shouldStart("Legacy", args)
	startRPC := d.shouldStart("RPC", args)
	startAlert := d.shouldStart("Alert", args)

	d.appCount += len(d.externalServices)

	if help || d.appCount == 0 {
		printUsage()
		return nil
	}

	profilerAddr := tSettings.ProfilerAddr
	if profilerAddr != "" && !pprofRegistered.Load() {
		pprofRegistered.Store(true)

		go func() {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			gocore.RegisterStatsHandlers()

			prefix := tSettings.StatsPrefix
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

	if tSettings.UseDatadogProfiler {
		deferFn := datadogProfiler()
		defer deferFn()
	}

	prometheusEndpoint := tSettings.PrometheusEndpoint
	if prometheusEndpoint != "" && !metricsRegistered.Load() {
		metricsRegistered.Store(true)
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	if tSettings.UseOpenTracing {
		logger.Infof("Starting tracer")
		// closeTracer := tracing.InitOtelTracer()
		// defer closeTracer()
		samplingRateStr := tSettings.TracingSampleRate

		samplingRate, err := strconv.ParseFloat(samplingRateStr, 64)
		if err != nil {
			logger.Errorf("error parsing sampling rate: %v", err)

			samplingRate = 0.01
		}

		serviceName := tSettings.ServiceName

		closer, err := tracing.InitOpenTracer(serviceName, samplingRate, tSettings)
		if err != nil {
			logger.Warnf("failed to initialize tracer: %v", err)
		}

		if closer != nil {
			traceCloser = closer
		}
	}

	var blockchainService *blockchain.Blockchain

	// blockchain service
	if startBlockchain {
		blockchainStoreURL := tSettings.BlockChain.StoreURL
		if blockchainStoreURL == nil {
			return errors.NewStorageError("blockchain store url not found")
		}

		blockchainStore, err := blockchain_store.NewStore(createLogger("bcsql"), blockchainStoreURL, tSettings)
		if err != nil {
			return err
		}

		blocksFinalKafkaAsyncProducer, err := getKafkaBlocksFinalAsyncProducer(ctx, createLogger("kpbf"), tSettings)
		if err != nil {
			return err
		}

		var localTestStartFromState string

		// Check if the command line has the -localTestStartFromState flag, if so use that value as initial FSM state
		for _, cmd := range args {
			if strings.HasPrefix(cmd, "-localTestStartFromState=") {
				localTestStartFromState = strings.SplitN(cmd, "=", 2)[1]
				break
			}
		}

		// if flag is not found, check the config if the flag is set
		if localTestStartFromState == "" {
			// read the config param
			localTestStartFromState = tSettings.LocalTestStartFromState
		}

		blockchainService, err = blockchain.New(ctx, createLogger("bchn"), tSettings, blockchainStore, blocksFinalKafkaAsyncProducer, localTestStartFromState)
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockChainService", blockchainService); err != nil {
			return err
		}
	}

	// p2p server
	if startP2P {
		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "p2p")
		if err != nil {
			return err
		}

		rejectedTxKafkaConsumerClient, err := getKafkaRejectedTxConsumerGroup(createLogger("kprtx"), tSettings, "p2p"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		subtreeKafkaProducerClient, err := getKafkaSubtreesAsyncProducer(ctx, createLogger("kps"), tSettings)
		if err != nil {
			return err
		}

		blocksKafkaProducerClient, err := getKafkaBlocksAsyncProducer(ctx, createLogger("kpb"), tSettings)
		if err != nil {
			return err
		}

		p2pService, err := p2p.NewServer(ctx,
			createLogger("p2p"),
			tSettings,
			blockchainClient,
			rejectedTxKafkaConsumerClient,
			subtreeKafkaProducerClient,
			blocksKafkaProducerClient,
		)
		if err != nil {
			return err
		}

		if err = sm.AddService("P2P", p2pService); err != nil {
			return err
		}
	}

	// asset service
	if startAsset {
		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
		if err != nil {
			return err
		}

		txStore, err := d.daemonStores.GetTxStore(createLogger("txs"), tSettings)
		if err != nil {
			return err
		}

		subtreeStore, err := d.daemonStores.GetSubtreeStore(createLogger("subtrees"), tSettings)
		if err != nil {
			return err
		}

		blockPersisterStore, err := d.daemonStores.GetBlockPersisterStore(createLogger("bp"), tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "asset")
		if err != nil {
			return err
		}

		if err := sm.AddService("Asset", asset.NewServer(
			createLogger("asset"),
			tSettings,
			utxoStore,
			txStore,
			subtreeStore,
			blockPersisterStore,
			blockchainClient,
		)); err != nil {
			return err
		}
	}

	if startRPC {
		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "rpc")
		if err != nil {
			return err
		}

		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
		if err != nil {
			return err
		}

		rpcServer, err := rpc.NewServer(createLogger("rpc"), tSettings, blockchainClient, utxoStore)
		if err != nil {
			return err
		}

		if err := sm.AddService("Rpc", rpcServer); err != nil {
			return err
		}
	}

	if startAlert {
		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "alert")
		if err != nil {
			return err
		}

		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, createLogger("ba"), tSettings)
		if err != nil {
			return err
		}

		peerClient, err := peer.NewClient(ctx, createLogger("peer"), tSettings)
		if err != nil {
			return err
		}

		p2pClient, err := p2p.NewClient(ctx, createLogger("p2p"), tSettings)
		if err != nil {
			return err
		}

		if err = sm.AddService("Alert", alert.New(
			createLogger("alert"),
			tSettings,
			blockchainClient,
			utxoStore,
			blockassemblyClient,
			peerClient,
			p2pClient,
		)); err != nil {
			return err
		}
	}

	if startBlockPersister {
		blockStore, err := d.daemonStores.GetBlockStore(createLogger("bps"), tSettings)
		if err != nil {
			return err
		}

		subtreeStore, err := d.daemonStores.GetSubtreeStore(createLogger("subtrees"), tSettings)
		if err != nil {
			return err
		}

		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "blockpersister")
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockPersister", blockpersister.New(ctx,
			createLogger("bp"),
			tSettings,
			blockStore,
			subtreeStore,
			utxoStore,
			blockchainClient,
		)); err != nil {
			return err
		}
	}

	if startUTXOPersister {
		blockStore, err := d.daemonStores.GetBlockStore(createLogger("bps"), tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "utxopersister")
		if err != nil {
			return err
		}

		if err := sm.AddService("UTXOPersister", utxopersister.New(ctx,
			createLogger("utxop"),
			tSettings,
			blockStore,
			blockchainClient,
		)); err != nil {
			return err
		}
	}

	// blockAssembly
	if startBlockAssembly {
		if tSettings.BlockAssembly.GRPCListenAddress != "" {
			txStore, err := d.daemonStores.GetTxStore(createLogger("txs"), tSettings)
			if err != nil {
				return err
			}

			utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
			if err != nil {
				return err
			}

			subtreeStore, err := d.daemonStores.GetSubtreeStore(createLogger("subtrees"), tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "blockassembly")
			if err != nil {
				return err
			}

			if err = sm.AddService("BlockAssembly", blockassembly.New(
				createLogger("bass"),
				tSettings,
				txStore,
				utxoStore,
				subtreeStore,
				blockchainClient,
			)); err != nil {
				return err
			}
		}
	}

	// subtreeValidation
	if startSubtreeValidation {
		subtreeStore, err := d.daemonStores.GetSubtreeStore(createLogger("subtrees"), tSettings)
		if err != nil {
			return err
		}

		txStore, err := d.daemonStores.GetTxStore(createLogger("txs"), tSettings)
		if err != nil {
			return err
		}

		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := d.daemonStores.GetValidatorClient(ctx, createLogger("txval"), tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "subtreevalidation")
		if err != nil {
			return err
		}

		subtreeConsumerClient, err := getKafkaSubtreesConsumerGroup(createLogger("kcs"), tSettings, "subtreevalidation"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		txmetaConsumerClient, err := getKafkaTxmetaConsumerGroup(createLogger("kctm"), tSettings, "subtreevalidation"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		subtreeValidationService, err := subtreevalidation.New(ctx,
			createLogger("stval"),
			tSettings,
			subtreeStore,
			txStore,
			utxoStore,
			validatorClient,
			blockchainClient,
			subtreeConsumerClient,
			txmetaConsumerClient,
		)
		if err != nil {
			return err
		}

		if err = sm.AddService("Subtree Validation", subtreeValidationService); err != nil {
			return err
		}
	}

	// blockValidation
	if startBlockValidation {
		if tSettings.BlockValidation.GRPCListenAddress != "" {
			subtreeStore, err := d.daemonStores.GetSubtreeStore(createLogger("subtrees"), tSettings)
			if err != nil {
				return err
			}

			txStore, err := d.daemonStores.GetTxStore(createLogger("txs"), tSettings)
			if err != nil {
				return err
			}

			utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
			if err != nil {
				return err
			}

			validatorClient, err := d.daemonStores.GetValidatorClient(ctx, createLogger("txval"), tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "blockvalidation")
			if err != nil {
				return err
			}

			kafkaConsumerClient, err := getKafkaBlocksConsumerGroup(createLogger("kcb"), tSettings, "blockvalidation"+"."+tSettings.ClientName)
			if err != nil {
				return err
			}

			if err = sm.AddService("Block Validation", blockvalidation.New(
				createLogger("bval"),
				tSettings,
				subtreeStore,
				txStore,
				utxoStore,
				validatorClient,
				blockchainClient,
				kafkaConsumerClient,
			)); err != nil {
				return err
			}
		}
	}

	// validator
	if startValidator {
		if tSettings.Validator.GRPCListenAddress != "" {
			utxoStore, err := d.daemonStores.GetUtxoStore(ctx, createLogger("utxos"), tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "validator")
			if err != nil {
				return err
			}

			consumerClient, err := getKafkaTxConsumerGroup(createLogger("kctx"), tSettings, "validator"+"."+tSettings.ClientName)
			if err != nil {
				return err
			}

			txMetaKafkaProducerClient, err := getKafkaTxmetaAsyncProducer(ctx, createLogger("kctm"), tSettings)
			if err != nil {
				return err
			}

			rejectedTxKafkaProducerClient, err := getKafkaRejectedTxAsyncProducer(ctx, createLogger("kctr"), tSettings)
			if err != nil {
				return err
			}

			if err = sm.AddService("Validator", validator.NewServer(
				createLogger("validator"),
				tSettings,
				utxoStore,
				blockchainClient,
				consumerClient,
				txMetaKafkaProducerClient,
				rejectedTxKafkaProducerClient,
			)); err != nil {
				return err
			}
		}
	}

	// propagation
	if startPropagation {
		if tSettings.Propagation.GRPCListenAddress != "" {
			txStore, err := d.daemonStores.GetTxStore(createLogger("txs"), tSettings)
			if err != nil {
				return err
			}

			validatorClient, err := d.daemonStores.GetValidatorClient(ctx, createLogger("txval"), tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, createLogger("bchc"), tSettings, "propagation")
			if err != nil {
				return err
			}

			validatorKafkaProducerClient, err := getKafkaTxAsyncProducer(ctx, createLogger("kctx"))
			if err != nil {
				return err
			}

			if err = sm.AddService("PropagationServer", propagation.New(
				createLogger("prop"),
				tSettings,
				txStore,
				validatorClient,
				blockchainClient,
				validatorKafkaProducerClient,
			)); err != nil {
				return err
			}
		}
	}

	if startLegacy {
		// if tSettings.ChainCfgParams.Net == chaincfg.RegressionNetParams.Net {
		// 	logger.Warnf("legacy service not supported in regtest mode. Skipping legacy service...")
		// 	return nil
		// }
		subtreeStore, err := d.daemonStores.GetSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		tempStore, err := d.daemonStores.GetTempStore(logger, tSettings)
		if err != nil {
			return err
		}

		utxoStore, err := d.daemonStores.GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := d.daemonStores.GetValidatorClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := d.daemonStores.GetBlockchainClient(ctx, logger, tSettings, "legacy")
		if err != nil {
			return err
		}

		subtreeValidationClient, err := d.daemonStores.GetSubtreeValidationClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockValidationClient, err := d.daemonStores.GetBlockValidationClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		if err = sm.AddService("Legacy", legacy.New(
			createLogger("legacy"),
			tSettings,
			blockchainClient,
			validatorClient,
			subtreeStore,
			tempStore,
			utxoStore,
			subtreeValidationClient,
			blockValidationClient,
			blockassemblyClient,
		)); err != nil {
			return err
		}
	}

	for _, externalService := range d.externalServices {
		service, err := externalService.InitFunc()
		if err != nil {
			return err
		}

		if err := sm.AddService(externalService.Name, service); err != nil {
			return err
		}
	}

	if readyCh != nil {
		sm.WaitForServiceToBeReady()
		closeOnce.Do(func() { close(readyCh) })
	}

	return nil
}

func (d *Daemon) shouldStart(app string, args []string) bool {
	// See if the app is enabled in the command line
	cmdArg := fmt.Sprintf("-%s=1", strings.ToLower(app))
	for _, cmd := range args {
		if cmd == cmdArg {
			d.appCount++
			return true
		}
	}

	// See if the app is disabled in the command line
	cmdArg = fmt.Sprintf("-%s=0", strings.ToLower(app))
	for _, cmd := range args {
		if cmd == cmdArg {
			return false
		}
	}

	// Add option to stop all services from running if -all=0 is passed
	// except for the services that are explicitly enabled above
	for _, cmd := range args {
		if cmd == "-all=0" {
			return false
		}
	}

	// If the app was not specified on the command line, see if it is enabled in the config
	varArg := fmt.Sprintf("start%s", app)

	b := gocore.Config().GetBool(varArg)
	if b {
		d.appCount++
	}

	return b
}

func printUsage() {
	fmt.Println("usage: main [options]")
	fmt.Println("where options are:")
	fmt.Println("")
	fmt.Println("    -blockchain=<1|0>")
	fmt.Println("          whether to start the blockchain service")
	fmt.Println("")
	fmt.Println("    -blockassembly=<1|0>")
	fmt.Println("          whether to start the block assembly service")
	fmt.Println("")
	fmt.Println("    -subtreevalidation=<1|0>")
	fmt.Println("          whether to start the subtree validation service")
	fmt.Println("")
	fmt.Println("    -blockvalidation=<1|0>")
	fmt.Println("          whether to start the block validation service")
	fmt.Println("")
	fmt.Println("    -validator=<1|0>")
	fmt.Println("          whether to start the validator service")
	fmt.Println("")
	fmt.Println("    -propagation=<1|0>")
	fmt.Println("          whether to start the propagation service")
	fmt.Println("")
	fmt.Println("    -p2p=<1|0>")
	fmt.Println("          whether to start the P2P service")
	fmt.Println("")
	fmt.Println("    -asset=<1|0>")
	fmt.Println("          whether to start the asset service")
	fmt.Println("")
	fmt.Println("    -faucet=<1|0>")
	fmt.Println("          whether to start the faucet service")
	fmt.Println("")
	fmt.Println("    -blockpersister=<1|0>")
	fmt.Println("          whether to start the block persister service")
	fmt.Println("")
	fmt.Println("    -utxopersister=<1|0>")
	fmt.Println("          whether to start the UTXO persister service")
	fmt.Println("")
	fmt.Println("    -legacy=<1|0>")
	fmt.Println("          whether to start the legacy service")
	fmt.Println("")
	fmt.Println("    -rpc=<1|0>")
	fmt.Println("          whether to start the RPC service")
	fmt.Println("")
	fmt.Println("    -alert=<1|0>")
	fmt.Println("          whether to start the alert service")
	fmt.Println("")
	fmt.Println("    -all=0")
	fmt.Println("          disable all services unless explicitly overridden")
	fmt.Println("")
}

func waitForPostgresToStart(logger ulogger.Logger, address string) error {
	timeout := time.Minute // 1 minutes timeout

	logger.Infof("Waiting for PostgreSQL to be ready at %s\n", address)

	deadline := time.Now().Add(timeout)

	for {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			if time.Now().After(deadline) {
				return errors.NewStorageError("timed out waiting for PostgreSQL to start", err)
			}

			logger.Infof("PostgreSQL is not up yet - waiting")
			time.Sleep(time.Second)

			continue
		}

		_ = conn.Close()

		logger.Infof("PostgreSQL is up - ready to go!")

		return nil
	}
}
