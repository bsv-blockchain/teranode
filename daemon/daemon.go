package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/alert"
	"github.com/bitcoin-sv/teranode/services/asset"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockpersister"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/coinbase"
	"github.com/bitcoin-sv/teranode/services/faucet"
	"github.com/bitcoin-sv/teranode/services/legacy"
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
	"github.com/felixge/fgprof"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var appCount int

type Daemon struct {
	doneCh chan struct{}
}

func New() *Daemon {
	return &Daemon{
		doneCh: make(chan struct{}),
	}
}

func (d *Daemon) Stop() {
	close(d.doneCh)
}

func (d *Daemon) Start(logger ulogger.Logger, args []string, tSettings *settings.Settings, readyCh ...chan struct{}) {
	// Before continuing, if the command line contains "-wait_for_postgres=1", wait for postgres to be ready
	if shouldStart("wait_for_postgres", args) {
		if err := waitForPostgresToStart(logger, tSettings.PostgresCheckAddress); err != nil {
			logger.Fatalf("error waiting for postgres: %v", err)
		}
	}

	sm := servicemanager.NewServiceManager(logger)

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

	err := startServices(sm.Ctx, logger, tSettings, sm, args)
	if err != nil {
		logger.Fatalf("error starting services: %v", err)
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

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("STOP USING THIS ENDPOINT - use port 8000/health/readiness or 8000/health/liveness"))
	})

	port := tSettings.HealthCheckPort

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 20 * time.Second,  // Prevent Slowloris attacks
		ReadTimeout:       60 * time.Second,  // Maximum duration for reading entire request
		WriteTimeout:      60 * time.Second,  // Maximum duration before timing out writes of response
		IdleTimeout:       120 * time.Second, // Maximum amount of time to wait for the next request
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Error starting server: %v", err)
		}
	}()

	logger.Infof("Health check endpoint listening on http://localhost:%d/health", port)

	// Create a channel to receive the wait result
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- sm.Wait()
	}()

	if len(readyCh) > 0 {
		close(readyCh[0])
	}

	// Wait for either services to complete or doneCh to be closed
	select {
	case err := <-waitCh:
		if err != nil {
			logger.Errorf("services failed: %v", err)
		}
	case <-d.doneCh:
		logger.Infof("daemon shutdown requested")
	}
}

// startServices starts the services based on the command line arguments and the config file
// nolint:gocognit
func startServices(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, sm *servicemanager.ServiceManager, args []string) error {
	help := shouldStart("help", args)
	startBlockchain := shouldStart("Blockchain", args)
	startBlockAssembly := shouldStart("BlockAssembly", args)
	startSubtreeValidation := shouldStart("SubtreeValidation", args)
	startBlockValidation := shouldStart("BlockValidation", args)
	startValidator := shouldStart("Validator", args)
	startPropagation := shouldStart("Propagation", args)
	startP2P := shouldStart("P2P", args)
	startAsset := shouldStart("Asset", args)
	startCoinbase := shouldStart("Coinbase", args)
	startFaucet := shouldStart("Faucet", args)
	startBlockPersister := shouldStart("BlockPersister", args)
	startUTXOPersister := shouldStart("UTXOPersister", args)
	startLegacy := shouldStart("Legacy", args)
	startRPC := shouldStart("RPC", args)
	startAlert := shouldStart("Alert", args)

	if help || appCount == 0 {
		printUsage()
		return nil
	}

	profilerAddr := tSettings.ProfilerAddr
	if profilerAddr != "" {
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

			http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
			logger.Fatalf("%v", server.ListenAndServe())
		}()
	}

	if tSettings.UseDatadogProfiler {
		deferFn := datadogProfiler()
		defer deferFn()
	}

	prometheusEndpoint := tSettings.PrometheusEndpoint
	if prometheusEndpoint != "" {
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

		closer, err := tracing.InitOpenTracer(serviceName, samplingRate)
		if err != nil {
			logger.Warnf("failed to initialize tracer: %v", err)
		}

		if closer != nil {
			defer closer.Close()
		}
	}

	var blockchainService *blockchain.Blockchain

	// blockchain service
	if startBlockchain {
		blockchainStoreURL := tSettings.BlockChain.StoreURL
		if blockchainStoreURL == nil {
			return errors.NewStorageError("blockchain store url not found")
		}

		blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL, tSettings.ChainCfgParams)
		if err != nil {
			return err
		}

		blocksFinalKafkaAsyncProducer, err := getKafkaBlocksFinalAsyncProducer(ctx, logger)
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
		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "p2p")
		if err != nil {
			return err
		}

		rejectedTxKafkaConsumerClient, err := getKafkaRejectedTxConsumerGroup(logger, "p2p")
		if err != nil {
			return err
		}

		subtreeKafkaProducerClient, err := getKafkaSubtreesAsyncProducer(ctx, logger)
		if err != nil {
			return err
		}

		blocksKafkaProducerClient, err := getKafkaBlocksAsyncProducer(ctx, logger)
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
		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		txStore, err := getTxStore(logger)
		if err != nil {
			return err
		}

		subtreeStore, err := getSubtreeStore(logger)
		if err != nil {
			return err
		}

		blockPersisterStore, err := getBlockPersisterStore(logger)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "asset")
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
		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "rpc")
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
		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "alert")
		if err != nil {
			return err
		}

		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		if err = sm.AddService("Alert", alert.New(
			logger.New("alert"),
			tSettings,
			blockchainClient,
			utxoStore,
			blockassemblyClient,
		)); err != nil {
			panic(err)
		}
	}

	if startBlockPersister {
		blockStore, err := getBlockStore(logger)
		if err != nil {
			return err
		}

		subtreeStore, err := getSubtreeStore(logger)
		if err != nil {
			return err
		}

		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "blockpersister")
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
		blockStore, err := getBlockStore(logger)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "utxopersister")
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
			txStore, err := getTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := getUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			subtreeStore, err := getSubtreeStore(logger)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "blockassembly")
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
		subtreeStore, err := getSubtreeStore(logger)
		if err != nil {
			return err
		}

		txStore, err := getTxStore(logger)
		if err != nil {
			return err
		}

		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := getValidatorClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "subtreevalidation")
		if err != nil {
			return err
		}

		subtreeConsumerClient, err := getKafkaSubtreesConsumerGroup(logger, "subtreevalidation")
		if err != nil {
			return err
		}

		txmetaConsumerClient, err := getKafkaTxmetaConsumerGroup(logger, "subtreevalidation")
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
			subtreeStore, err := getSubtreeStore(logger)
			if err != nil {
				return err
			}

			txStore, err := getTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := getUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			validatorClient, err := getValidatorClient(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "blockvalidation")
			if err != nil {
				return err
			}

			kafkaConsumerClient, err := getKafkaBlocksConsumerGroup(logger, "blockvalidation")
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
			utxoStore, err := getUtxoStore(ctx, logger, tSettings)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "validator")
			if err != nil {
				return err
			}

			consumerClient, err := getKafkaTxConsumerGroup(logger, "validator")
			if err != nil {
				return err
			}

			txMetaKafkaProducerClient, err := getKafkaTxmetaAsyncProducer(ctx, logger)
			if err != nil {
				return err
			}

			rejectedTxKafkaProducerClient, err := getKafkaRejectedTxAsyncProducer(ctx, logger)
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

	// coinbase tracker server
	if startCoinbase {
		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "coinbase")
		if err != nil {
			return err
		}

		if err = sm.AddService("Coinbase", coinbase.New(
			logger.New("coinB"),
			tSettings,
			blockchainClient,
		)); err != nil {
			return err
		}
	}

	if startFaucet {
		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "faucet")
		if err != nil {
			return err
		}

		if err = sm.AddService("Faucet", faucet.New(
			logger.New("faucet"),
			tSettings,
			blockchainClient,
		)); err != nil {
			return err
		}
	}

	// propagation
	if startPropagation {
		if tSettings.Propagation.GRPCListenAddress == "" {
			if tSettings.Propagation.UseDumb {
				if err := sm.AddService("PropagationServer", propagation.NewDumbPropagationServer(tSettings)); err != nil {
					return err
				}
			} else {
				txStore, err := getTxStore(logger)
				if err != nil {
					return err
				}

				validatorClient, err := getValidatorClient(ctx, logger, tSettings)
				if err != nil {
					return err
				}

				blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "propagation")
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
		subtreeStore, err := getSubtreeStore(logger)
		if err != nil {
			return err
		}

		tempStore, err := getTempStore(logger)
		if err != nil {
			return err
		}

		utxoStore, err := getUtxoStore(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		validatorClient, err := getValidatorClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, tSettings, "legacy")
		if err != nil {
			return err
		}

		subtreeValidationClient, err := getSubtreeValidationClient(ctx, logger, tSettings)
		if err != nil {
			return err
		}

		blockValidationClient, err := getBlockValidationClient(ctx, logger, tSettings)
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
	fmt.Println("          whether to start the blockassembly service")
	fmt.Println("")
	fmt.Println("    -blockvalidation=<1|0>")
	fmt.Println("          whether to start the blockvalidation service")
	fmt.Println("")
	fmt.Println("    -validator=<1|0>")
	fmt.Println("          whether to start the validator service")
	fmt.Println("")
	fmt.Println("    -utxostore=<1|0>")
	fmt.Println("          whether to start the utxo store service")
	fmt.Println("")
	fmt.Println("    -propagation=<1|0>")
	fmt.Println("          whether to start the propagation service")
	fmt.Println("")
	fmt.Println("    -seeder=<1|0>")
	fmt.Println("          whether to start the seeder service")
	fmt.Println("")
	fmt.Println("    -asset=<1|0>")
	fmt.Println("          whether to start the assert service")
	fmt.Println("")
	fmt.Println("    -coinbase=<1|0>")
	fmt.Println("          whether to start the coinbase server")
	fmt.Println("")
	fmt.Println("    -p2p=<1|0>")
	fmt.Println("          whether to start the p2p server")
	fmt.Println("")
	fmt.Println("    -tracer=<1|0>")
	fmt.Println("          whether to start the Jaeger tracer (default=false)")
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
