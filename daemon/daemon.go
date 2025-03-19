package daemon

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
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
	appCount          int
	pprofRegistered   atomic.Bool
	metricsRegistered atomic.Bool
	healthRegistered  atomic.Bool
	traceCloser       io.Closer
)

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
	server           *http.Server // Add this field
	ServiceManager   *servicemanager.ServiceManager
	externalServices []*externalService
}

func New() *Daemon {
	ctx := context.Background()

	return &Daemon{
		Ctx:              ctx,
		closeDoneOnce:    sync.Once{},
		closeStopOnce:    sync.Once{},
		doneCh:           make(chan struct{}),
		stopCh:           make(chan struct{}),
		server:           nil,
		ServiceManager:   servicemanager.NewServiceManager(ctx, ulogger.New("ServiceManager")),
		externalServices: make([]*externalService, 0),
	}
}

func (d *Daemon) AddExternalService(name string, initFunc func() (servicemanager.Service, error)) {
	d.externalServices = append(d.externalServices, &externalService{
		Name:     name,
		InitFunc: initFunc,
	})
}

func (d *Daemon) Stop(timeout ...time.Duration) error {
	if traceCloser != nil {
		_ = traceCloser.Close()
	}

	// Gracefully shutdown the HTTP server if it exists
	if d.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := d.server.Shutdown(ctx); err != nil {
			// Log error but continue with shutdown
			fmt.Printf("Error shutting down health check server: %v\n", err)
		}
	}

	d.closeDoneOnce.Do(func() { close(d.doneCh) })

	if appCount == 0 {
		d.closeStopOnce.Do(func() { close(d.stopCh) })
		return nil
	}

	if len(timeout) > 0 {
		select {
		case <-d.stopCh: // Wait for all services to complete
			return nil
		case <-time.After(timeout[0]):
			return errors.NewProcessingError("timeout waiting for services to stop after %v", timeout[0])
		}
	}

	<-d.stopCh // Wait for all services to complete without timeout

	return nil
}

func (d *Daemon) Start(logger ulogger.Logger, args []string, tSettings *settings.Settings, readyCh ...chan struct{}) {
	// Before continuing, if the command line contains "-wait_for_postgres=1", wait for postgres to be ready
	if shouldStart("wait_for_postgres", args) {
		if err := waitForPostgresToStart(logger, tSettings.PostgresCheckAddress); err != nil {
			logger.Errorf("error waiting for postgres: %v", err)
			return
		}
	}

	sm := d.ServiceManager

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		//
		// close all the stores
		//

		if mainTxstore != nil {
			logger.Debugf("closing tx store")

			_ = mainTxstore.Close(shutdownCtx)
		}

		if mainSubtreestore != nil {
			logger.Debugf("closing subtree store")

			_ = mainSubtreestore.Close(shutdownCtx)
		}
	}()

	var readyChInternal chan struct{}
	if len(readyCh) > 0 {
		readyChInternal = readyCh[0]
	}

	err := d.startServices(sm.Ctx, logger, tSettings, sm, args, readyChInternal)
	if err != nil {
		logger.Errorf("error starting services: %v", err)
		sm.ForceShutdown()
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
	d.server = server // Store the reference

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

		sm.ForceShutdown()

		// Wait for services to complete shutdown using the existing waitCh
		if err := <-waitErr; err != nil {
			logger.Errorf("error during service shutdown: %v", err)
		}
	}

	d.closeStopOnce.Do(func() { close(d.stopCh) })
}

// startServices starts the services based on the command line arguments and the config file
// nolint:gocognit
func (d *Daemon) startServices(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, sm *servicemanager.ServiceManager, args []string, readyCh chan<- struct{}) error {
	var closeOnce sync.Once

	if readyCh != nil {
		defer closeOnce.Do(func() { close(readyCh) })
	}

	help := shouldStart("help", args)
	startBlockchain := shouldStart("Blockchain", args)
	startBlockAssembly := shouldStart("BlockAssembly", args)
	startSubtreeValidation := shouldStart("SubtreeValidation", args)
	startBlockValidation := shouldStart("BlockValidation", args)
	startValidator := shouldStart("Validator", args)
	startPropagation := shouldStart("Propagation", args)
	startP2P := shouldStart("P2P", args)
	startAsset := shouldStart("Asset", args)
	startBlockPersister := shouldStart("BlockPersister", args)
	startUTXOPersister := shouldStart("UTXOPersister", args)
	startLegacy := shouldStart("Legacy", args)
	startRPC := shouldStart("RPC", args)
	startAlert := shouldStart("Alert", args)

	appCount += len(d.externalServices)

	if help || appCount == 0 {
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

		blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL, tSettings)
		if err != nil {
			return err
		}

		blocksFinalKafkaAsyncProducer, err := getKafkaBlocksFinalAsyncProducer(ctx, logger, tSettings)
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

		blockchainService, err = blockchain.New(ctx, logger.New("bchn"), tSettings, blockchainStore, blocksFinalKafkaAsyncProducer, localTestStartFromState)
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockChainService", blockchainService); err != nil {
			return err
		}
	}

	// p2p server
	if startP2P {
		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "p2p")
		if err != nil {
			return err
		}

		rejectedTxKafkaConsumerClient, err := getKafkaRejectedTxConsumerGroup(logger, tSettings, "p2p"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		subtreeKafkaProducerClient, err := getKafkaSubtreesAsyncProducer(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blocksKafkaProducerClient, err := getKafkaBlocksAsyncProducer(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		p2pService, err := p2p.NewServer(ctx,
			logger.New("P2P"),
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
		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		txStore, err := GetTxStore(logger)
		if err != nil {
			return err
		}

		subtreeStore, err := GetSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		blockPersisterStore, err := GetBlockPersisterStore(logger)
		if err != nil {
			return err
		}

		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "asset")
		if err != nil {
			return err
		}

		if err := sm.AddService("Asset", asset.NewServer(
			logger.New("asset"),
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
		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "rpc")
		if err != nil {
			return err
		}

		rpcServer, err := rpc.NewServer(logger.New("RPC"), tSettings, blockchainClient)
		if err != nil {
			return err
		}

		if err := sm.AddService("Rpc", rpcServer); err != nil {
			return err
		}
	}

	if startAlert {
		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "alert")
		if err != nil {
			return err
		}

		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		peerClient, err := peer.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		p2pClient, err := p2p.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		if err = sm.AddService("Alert", alert.New(
			logger.New("alert"),
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
		blockStore, err := GetBlockStore(logger)
		if err != nil {
			return err
		}

		subtreeStore, err := GetSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "blockpersister")
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockPersister", blockpersister.New(ctx,
			logger.New("bp"),
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
		blockStore, err := GetBlockStore(logger)
		if err != nil {
			return err
		}

		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "utxopersister")
		if err != nil {
			return err
		}

		if err := sm.AddService("UTXOPersister", utxopersister.New(ctx,
			logger.New("utxop"),
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
			txStore, err := GetTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			subtreeStore, err := GetSubtreeStore(logger, tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "blockassembly")
			if err != nil {
				return err
			}

			if err = sm.AddService("BlockAssembly", blockassembly.New(
				logger.New("bass"),
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
		subtreeStore, err := GetSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		txStore, err := GetTxStore(logger)
		if err != nil {
			return err
		}

		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := GetValidatorClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "subtreevalidation")
		if err != nil {
			return err
		}

		subtreeConsumerClient, err := getKafkaSubtreesConsumerGroup(logger, tSettings, "subtreevalidation"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		txmetaConsumerClient, err := getKafkaTxmetaConsumerGroup(logger, tSettings, "subtreevalidation"+"."+tSettings.ClientName)
		if err != nil {
			return err
		}

		subtreeValidationService, err := subtreevalidation.New(ctx,
			logger.New("stval"),
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
			subtreeStore, err := GetSubtreeStore(logger, tSettings)
			if err != nil {
				return err
			}

			txStore, err := GetTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			validatorClient, err := GetValidatorClient(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "blockvalidation")
			if err != nil {
				return err
			}

			kafkaConsumerClient, err := getKafkaBlocksConsumerGroup(logger, tSettings, "blockvalidation"+"."+tSettings.ClientName)
			if err != nil {
				return err
			}

			if err = sm.AddService("Block Validation", blockvalidation.New(
				logger.New("bval"),
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
			utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "validator")
			if err != nil {
				return err
			}

			consumerClient, err := getKafkaTxConsumerGroup(logger, "validator"+"."+tSettings.ClientName)
			if err != nil {
				return err
			}

			txMetaKafkaProducerClient, err := getKafkaTxmetaAsyncProducer(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			rejectedTxKafkaProducerClient, err := getKafkaRejectedTxAsyncProducer(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			if err = sm.AddService("Validator", validator.NewServer(
				logger.New("validator"),
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
			if tSettings.Propagation.UseDumb {
				if err := sm.AddService("PropagationServer", propagation.NewDumbPropagationServer(tSettings)); err != nil {
					return err
				}
			} else {
				txStore, err := GetTxStore(logger)
				if err != nil {
					return err
				}

				validatorClient, err := GetValidatorClient(ctx, logger, tSettings)
				if err != nil {
					return err
				}

				blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "propagation")
				if err != nil {
					return err
				}

				validatorKafkaProducerClient, err := getKafkaTxAsyncProducer(ctx, logger)
				if err != nil {
					return err
				}

				if err = sm.AddService("PropagationServer", propagation.New(
					logger.New("prop"),
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
	}

	if startLegacy {
		// if tSettings.ChainCfgParams.Net == chaincfg.RegressionNetParams.Net {
		// 	logger.Warnf("legacy service not supported in regtest mode. Skipping legacy service...")
		// 	return nil
		// }

		subtreeStore, err := GetSubtreeStore(logger, tSettings)
		if err != nil {
			return err
		}

		tempStore, err := GetTempStore(logger)
		if err != nil {
			return err
		}

		utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := GetValidatorClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := GetBlockchainClient(ctx, logger, tSettings, "legacy")
		if err != nil {
			return err
		}

		subtreeValidationClient, err := GetSubtreeValidationClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockValidationClient, err := GetBlockValidationClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		if err = sm.AddService("Legacy", legacy.New(
			logger.New("legacy"),
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

func shouldStart(app string, args []string) bool {
	// See if the app is enabled in the command line
	cmdArg := fmt.Sprintf("-%s=1", strings.ToLower(app))
	for _, cmd := range args {
		if cmd == cmdArg {
			appCount++
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
		appCount++
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
	fmt.Println("    -coinbase=<1|0>")
	fmt.Println("          whether to start the coinbase service")
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
	fmt.Println("    -all=<1|0>")
	fmt.Println("          enable/disable all services unless explicitly overridden")
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
