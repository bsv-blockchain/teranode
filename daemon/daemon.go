// Package daemon provides the main entry point for the Teranode daemon.
// It is responsible for initializing and managing various services and components
// required for the operation of the Teranode system.
//
// Usage:
//
// This package is typically used to start the Teranode daemon, which orchestrates
// the interaction between blockchain, networking, and other subsystems.
//
// Functions:
//   - InitializeDaemon: Sets up the daemon and its dependencies.
//   - StartDaemon: Starts the daemon and its services.
//
// Side effects:
//
// Functions in this package may start background processes, open network connections,
// and interact with external systems such as databases and blockchain nodes.
package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/servicemanager"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
)

var (
	globalStoreMutex  sync.RWMutex
	healthRegistered  atomic.Bool
	metricsRegistered atomic.Bool
	pprofRegistered   atomic.Bool
)

const (
	// Logger names for various services
	loggerBlockAssembly            = "ba"
	loggerBlockPersisterStore      = "bps"
	loggerBlockValidation          = "bval"
	loggerBlockchain               = "bchn"
	loggerBlockchainClient         = "bchc"
	loggerBlockchainSQL            = "bcsql"
	loggerPruner                   = "pruner"
	loggerKafkaConsumerBlocks      = "kcb"
	loggerKafkaConsumerRejectedTx  = "kcrtx"
	loggerKafkaConsumerSubtree     = "kcs"
	loggerKafkaConsumerTransaction = "kctx"
	loggerKafkaConsumerTxMeta      = "kctm"
	loggerKafkaProducerBlockFinal  = "kpbf"
	loggerKafkaProducerBlocks      = "kpb"
	loggerKafkaProducerRejectedTx  = "kprtx"
	loggerKafkaProducerSubtree     = "kps"
	loggerP2P                      = "p2p"
	loggerPeerClient               = "peer"
	loggerPropagation              = "prop"
	loggerRPC                      = "rpc"
	loggerSubtreeValidation        = "stval"
	loggerSubtrees                 = "subtrees"
	loggerTemp                     = "temp"
	loggerTransactions             = "txs"
	loggerTxValidator              = "txval"
	loggerUtxos                    = "utxos"
	loggerAlert                    = "alert"

	// Service names
	serviceAlert                   = "alert"
	serviceAlertFormal             = "Alert"
	serviceAsset                   = "asset"
	serviceAssetFormal             = "Asset"
	serviceBlockAssembly           = "blockassembly"
	serviceBlockAssemblyFormal     = "BlockAssembly"
	serviceBlockPersister          = "blockpersister"
	serviceBlockPersisterFormal    = "BlockPersister"
	serviceBlockValidation         = "blockvalidation"
	servicePruner                  = "pruner"
	servicePrunerFormal            = "Pruner"
	serviceBlockValidationFormal   = "BlockValidation"
	serviceBlockchainFormal        = "Blockchain"
	serviceHelp                    = "help"
	serviceLegacy                  = "legacy"
	serviceLegacyFormal            = "Legacy"
	serviceNameDaemon              = "Daemon"
	serviceNameP2P                 = "p2p"
	serviceNameP2PFormal           = "P2P"
	servicePropagation             = "propagation"
	servicePropagationFormal       = "Propagation"
	serviceRPC                     = "rpc"
	serviceRPCFormal               = "RPC"
	serviceServiceManager          = "ServiceManager"
	serviceSubtreeValidation       = "subtreevalidation"
	serviceSubtreeValidationFormal = "SubtreeValidation"
	serviceUtxoPersister           = "utxopersister"
	serviceUtxoPersisterFormal     = "UTXOPersister"
	serviceValidator               = "validator"
	serviceValidatorFormal         = "Validator"
)

// externalService represents an external service that can be added to the Daemon.
type externalService struct {
	Name     string
	InitFunc func() (servicemanager.Service, error)
}

// Daemon is the main structure for managing the Teranode daemon.
type Daemon struct {
	Ctx                context.Context
	ServiceManager     *servicemanager.ServiceManager
	appCount           int
	blockValidationSrv *blockvalidation.Server
	closeDoneOnce      sync.Once
	closeStopOnce      sync.Once
	daemonStores       *Stores
	doneCh             chan struct{}
	externalServices   []*externalService
	loggerFactory      func(serviceName string) ulogger.Logger
	server             *http.Server
	serverMu           sync.Mutex
	stopCh             chan struct{}
}

// New creates a new Daemon instance with the provided options.
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
		daemonStores:     &Stores{},
	}

	// Apply functional options
	for _, opt := range opts {
		opt(d)
	}

	if d.loggerFactory == nil {
		d.loggerFactory = func(serviceName string) ulogger.Logger {
			return ulogger.New(serviceName)
		}
	}

	// Initialize ServiceManager with the configured logger factory
	d.ServiceManager = servicemanager.NewServiceManager(d.Ctx, d.loggerFactory(serviceServiceManager))

	return d
}

// AddExternalService adds an external service to the Daemon.
func (d *Daemon) AddExternalService(name string, initFunc func() (servicemanager.Service, error)) {
	d.externalServices = append(d.externalServices, &externalService{
		Name:     name,
		InitFunc: initFunc,
	})
}

// Stop gracefully stops the Daemon and all its services.
func (d *Daemon) Stop(skipTracerShutdown ...bool) error {
	logger := d.loggerFactory(serviceNameDaemon)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if len(skipTracerShutdown) == 0 || !skipTracerShutdown[0] {
		// Shutdown the tracer
		if err := tracing.ShutdownTracer(shutdownCtx); err != nil {
			logger.Errorf("Error shutting down tracer: %v", err)
		} else {
			logger.Infof("Tracer shutdown completed")
		}
	}

	d.serverMu.Lock()

	// Graceful shutdown the HTTP server if it exists
	if d.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// // Default timeout of 30 seconds if it wasn't provided
	shutdownTimeout := 30 * time.Second

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

// updateServiceStatuses logs the statuses of all services managed by the ServiceManager.
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

// Start initializes and starts the Daemon and its services.
func (d *Daemon) Start(logger ulogger.Logger, args []string, appSettings *settings.Settings, readyChannel ...chan struct{}) {
	if d.loggerFactory == nil {
		d.loggerFactory = func(serviceName string) ulogger.Logger {
			return ulogger.New(serviceName, ulogger.WithLevel(appSettings.LogLevel))
		}
	}

	// Before continuing, if the command line contains "-wait_for_postgres=1", wait for postgres to be ready
	if d.shouldStart("wait_for_postgres", args) {
		if err := waitForPostgresToStart(logger, appSettings.PostgresCheckAddress); err != nil {
			logger.Errorf("error waiting for postgres: %v", err)
			return
		}
	}

	sm := d.ServiceManager

	// Configure the per-service graceful Stop() timeout from settings so Kafka
	// flushes and batcher drains are not cut off at the old 5s. Falls back to
	// servicemanager.DefaultStopTimeout when unset.
	if appSettings.ServiceManagerStopTimeout > 0 {
		sm.StopTimeout = appSettings.ServiceManagerStopTimeout
	}

	var readyChInternal chan struct{}
	if len(readyChannel) > 0 {
		readyChInternal = readyChannel[0]
	}

	err := d.startServices(sm.Ctx, logger, appSettings, sm, args, readyChInternal)
	if err != nil {
		logger.Errorf("error starting services: %v", err)
		sm.ForceShutdown()
		d.closeDoneOnce.Do(func() { close(d.doneCh) })
	}

	util.RegisterPrometheusMetrics()

	const defaultHealthTimeout = 5 * time.Second

	mux := http.NewServeMux()
	healthFunc := func(liveness bool) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			timeout := defaultHealthTimeout
			if v := r.URL.Query().Get("timeout"); v != "" {
				if parsed, parseErr := time.ParseDuration(v); parseErr == nil && parsed > 0 {
					timeout = parsed
				}
			}

			ctx, cancel := context.WithTimeout(sm.Ctx, timeout)
			defer cancel()

			status, details, _ := sm.HealthHandler(ctx, liveness)

			w.WriteHeader(status)
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

	// Get listener using util.GetListener
	listener, listenAddress, _, err := util.GetListener(appSettings.Context, "health", "http://", appSettings.HealthCheckHTTPListenAddress)
	if err != nil {
		// Fatalf is expected to terminate the process in production, but test
		// loggers only record the message and return. Without an explicit return
		// here, execution would fall through to server.Serve(listener) with a nil
		// listener and panic. Return so a failed listener never reaches Serve.
		logger.Fatalf("Failed to get health check listener: %v", err)
		return
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 20 * time.Second,  // Prevent Slowloris attacks
		ReadTimeout:       60 * time.Second,  // Maximum duration for reading the entire request
		WriteTimeout:      60 * time.Second,  // Maximum duration before timing out writes of response
		IdleTimeout:       120 * time.Second, // Maximum amount of time to wait for the next request
	}

	d.serverMu.Lock()
	d.server = server // Store the reference
	d.serverMu.Unlock()

	go func() {
		if localErr := server.Serve(listener); localErr != nil && !errors.Is(localErr, http.ErrServerClosed) {
			logger.Fatalf("Error starting server: %v", localErr)
		}
		// Clean up the listener when server stops
		util.RemoveListener(appSettings.Context, "health", "http://")
	}()

	logger.Infof("Health check endpoint listening on %s", listenAddress)

	// Create a channel to receive the wait result
	waitErr := make(chan error, 1)
	go func() {
		waitErr <- sm.Wait()
	}()

	// closeStores must run on BOTH shutdown paths (OS-signal via waitErr,
	// daemon.Stop() via doneCh) and MUST run after every service has
	// finished using the stores. The defer is registered here, before the
	// select below, but runs when this function returns — i.e. after the
	// select has observed service exit on either path. This gives us a
	// single ordered teardown point that runs even if the body returns
	// through a signal-driven exit rather than an explicit Stop() call.
	//
	// The UTXO store in particular owns background batched-write workers
	// whose Close drains in-channel items before returning. Skipping this
	// drain corrupts UTXO state: callers will have received successful
	// acks for writes still queued in the batcher, the parent blocks get
	// committed elsewhere, and on restart subsequent blocks fail with
	// missing-parent errors.
	defer d.closeStores(logger)

	// closeClients must run AFTER all services have stopped (so borrowed
	// clients are no longer in use) and BEFORE closeStores (clients sit in
	// front of the stores in the dependency graph). Deferred AFTER the
	// closeStores defer above so LIFO ordering runs clients first, then stores.
	defer d.closeClients(logger)

	// Wait for either services to complete or doneCh to be closed
	select {
	case err = <-waitErr:
		if err != nil {
			logger.Errorf("services failed: %v", err)
		}
	case <-d.doneCh:
		logger.Infof("daemon shutdown requested")

		if shutErr := server.Shutdown(sm.Ctx); shutErr != nil {
			logger.Errorf("error shutting down server: %v", shutErr)
		}

		sm.ForceShutdown()

		logger.Infof("daemon shutdown waiting for services to finish")

		// Wait for services to complete shutdown using the existing waitCh
		if err = <-waitErr; err != nil {
			logger.Errorf("error during service shutdown: %v", err)
		}

		logger.Infof("daemon shutdown completed")
	}

	d.closeStopOnce.Do(func() { close(d.stopCh) })
}

// clientCloser is the optional close contract the daemon uses to tear down the
// gRPC clients it constructs. Production clients gain a Close() error method
// (Fix group A); the daemon's closeClients step (added later) type-asserts each
// retained client to this interface and closes it, so the client interfaces
// themselves do not need to declare Close. Mirrors the existing optional-stopper
// pattern in services/pruner/server.go.
type clientCloser interface {
	Close() error
}

// closeClients closes every gRPC client the daemon constructed, exactly once.
//
// The daemon is the sole owner of the clients it builds (both the daemonStores
// singletons and the fresh-per-construction clients retained via retainClient);
// receiving services borrow them and must not close them. This runs after
// ServiceManager.Wait() — so all service Stop()s have completed — and before
// closeStores, because clients sit in front of the stores in the dependency
// graph. Each client is closed via the Stage-E clientCloser contract.
func (d *Daemon) closeClients(logger ulogger.Logger) {
	d.daemonStores.constructedClientsMu.Lock()
	clients := d.daemonStores.constructedClients
	d.daemonStores.constructedClients = nil
	d.daemonStores.constructedClientsMu.Unlock()

	for _, client := range clients {
		if err := client.Close(); err != nil {
			logger.Errorf("error closing client: %v", err)
		}
	}
}

// closeStores safely closes the main stores used by the Daemon.
//
// Uses a fresh context with a 30 s deadline rather than the daemon's
// service-manager context: by the time this runs, the SM context has
// almost certainly been cancelled (that's what caused the services to
// exit), and we still want the underlying drains — especially the UTXO
// batcher drain — to run with real time available, not return
// immediately with context.Canceled.
func (d *Daemon) closeStores(logger ulogger.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	globalStoreMutex.RLock()
	txStoreToClose := d.daemonStores.mainTxStore
	subtreeStoreToClose := d.daemonStores.mainSubtreeStore
	tempStoreToClose := d.daemonStores.mainTempStore
	utxoStoreToClose := d.daemonStores.mainUtxoStore
	blockStoreToClose := d.daemonStores.mainBlockStore
	blockPersisterStoreToClose := d.daemonStores.mainBlockPersisterStore
	blockchainStoreToClose := d.daemonStores.mainBlockchainStore
	peerRegistryClientToClose := d.daemonStores.mainPeerRegistryClient
	globalStoreMutex.RUnlock()

	// Close the daemon-owned peer-registry client first (DC16). It is a gRPC
	// client to the blockchain service, not a store, and owns its conn
	// (ownsConn=true); Close() is a no-op for non-owned conns.
	if peerRegistryClientToClose != nil {
		logger.Debugf("closing peer registry client")

		if err := peerRegistryClientToClose.Close(); err != nil {
			logger.Errorf("error closing peer registry client: %v", err)
		}
	}

	// Drain the UTXO store first so any background-batched writes commit
	// before we tear down adjacent blob stores. UTXO writes can reference
	// subtree-data writes indirectly via height bookkeeping, so finishing
	// UTXO work before subtree teardown keeps the relationship
	// consistent.
	if utxoStoreToClose != nil {
		logger.Debugf("closing utxo store")

		if err := utxoStoreToClose.Close(ctx); err != nil {
			logger.Errorf("error closing utxo store: %v", err)
		}
	}

	if txStoreToClose != nil {
		logger.Debugf("closing tx store")

		_ = txStoreToClose.Close(ctx)
	}

	if subtreeStoreToClose != nil {
		logger.Debugf("closing subtree store")

		_ = subtreeStoreToClose.Close(ctx)
	}

	if tempStoreToClose != nil {
		logger.Debugf("closing temp store")

		_ = tempStoreToClose.Close(ctx)
	}

	// Block and block-persister blob stores (DC10) — alongside the other blob
	// stores; both have valid Close methods.
	if blockStoreToClose != nil {
		logger.Debugf("closing block store")

		_ = blockStoreToClose.Close(ctx)
	}

	if blockPersisterStoreToClose != nil {
		logger.Debugf("closing block persister store")

		_ = blockPersisterStoreToClose.Close(ctx)
	}

	// Close the blockchain state store last (DC9): it is the core chain-state
	// DB and its Close stops background goroutines and closes the DB handle
	// (orderly WAL checkpoint / connection drain on SQL/SQLite backends).
	if blockchainStoreToClose != nil {
		logger.Debugf("closing blockchain store")

		if err := blockchainStoreToClose.Close(ctx); err != nil {
			logger.Errorf("error closing blockchain store: %v", err)
		}
	}
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

	// Add an option to stop all services from running if -all=0 is passed
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

// printUsage prints the usage information for the Daemon command line options.
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
	fmt.Println("    -pruner=<1|0>")
	fmt.Println("          whether to start the pruner service")
	fmt.Println("")
	fmt.Println("    -all=0")
	fmt.Println("          disable all services unless explicitly overridden")
	fmt.Println("")
}

// waitForPostgresToStart waits for PostgreSQL to be ready at the specified address.
func waitForPostgresToStart(logger ulogger.Logger, address string) error {
	timeout := time.Minute // 1-minute timeout

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

func getBlockHeightTrackerCh(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI) (chan uint32, error) {
	blockchainSubscriptionCh, err := blockchainClient.Subscribe(ctx, "File BlockHeight")
	if err != nil {
		return nil, errors.NewServiceError("error subscribing to blockchain", err)
	}

	blockHeight, _, err := blockchainClient.GetBestHeightAndTime(ctx)
	if err != nil {
		return nil, errors.NewServiceError("error getting best height and time", err)
	}

	ch := make(chan uint32)

	go func() {
		ch <- blockHeight

		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscriptionCh:
				if notification.Type == model.NotificationType_Block {
					blockHeight, _, err = blockchainClient.GetBestHeightAndTime(ctx)
					if err != nil {
						// Don't log if context was cancelled (shutting down gracefully), sometimes causes panic in tests
						if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "client connection is closing") {
							logger.Errorf("[File] error getting best height and time: %v", err)
						}
					} else {
						ch <- blockHeight
					}
				}
			}
		}
	}()

	return ch, nil
}
