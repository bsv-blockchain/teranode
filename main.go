package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/cmd/aerospike_reader/aerospike_reader"
	"github.com/bitcoin-sv/ubsv/cmd/bare/bare"
	"github.com/bitcoin-sv/ubsv/cmd/bitcoin2utxoset/bitcoin2utxoset"
	"github.com/bitcoin-sv/ubsv/cmd/blockassembly_blaster/blockassembly_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/blockchainstatus/blockchainstatus"
	"github.com/bitcoin-sv/ubsv/cmd/chainintegrity/chainintegrity"
	"github.com/bitcoin-sv/ubsv/cmd/filereader/filereader"
	"github.com/bitcoin-sv/ubsv/cmd/propagation_blaster/propagation_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/s3_blaster/s3_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/s3inventoryintegrity/s3inventoryintegrity"
	"github.com/bitcoin-sv/ubsv/cmd/seeder/seeder"
	"github.com/bitcoin-sv/ubsv/cmd/settings/settings"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/txblaster"
	"github.com/bitcoin-sv/ubsv/cmd/txblockidcheck/txblockidcheck"
	utxopersister_cmd "github.com/bitcoin-sv/ubsv/cmd/utxopersister/utxopersister"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/alert"
	"github.com/bitcoin-sv/ubsv/services/asset"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockpersister"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/faucet"
	"github.com/bitcoin-sv/ubsv/services/legacy"
	"github.com/bitcoin-sv/ubsv/services/miner"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/rpc"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/services/validator"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/felixge/fgprof"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"golang.org/x/term"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "ubsv"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string
var appCount int

func init() {
	gocore.SetInfo(progname, version, commit)

	// Call the gocore.Log function to initialize the logger and start the Unix domain socket that allows us to configure settings at runtime.
	gocore.Log(progname)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})
}

func main() {
	switch path.Base(os.Args[0]) {
	case "bare.run":
		// bare.Init()
		bare.Start()
		return
	case "blockassemblyblaster.run":
		blockassembly_blaster.Init()
		blockassembly_blaster.Start()
		return
	case "chainintegrity.run":
		// chainintegrity.Init()
		chainintegrity.Start()
		return
	case "propagationblaster.run":
		propagation_blaster.Init()
		propagation_blaster.Start()
		return
	case "s3blaster.run":
		s3_blaster.Init()
		s3_blaster.Start()
		return
	case "blockchainstatus.run":
		blockchainstatus.Init()
		blockchainstatus.Start()
		return
	case "blaster.run":
		// txblaster.Init()
		txblaster.Start()
		return
	case "filereader.run":
		// filereader.Init()
		filereader.Start()
		return
	case "s3inventoryintegrity.run":
		s3inventoryintegrity.Start()
		return
	case "txblockidcheck.run":
		txblockidcheck.Start()
		return
	case "aerospike_reader.run":
		aerospike_reader.Start()
		return
	case "utxopersister.run":
		utxopersister_cmd.Start()
		return
	case "seeder.run":
		seeder.Start()
		return
	case "bitcoin2utxoset.run":
		bitcoin2utxoset.Start()
		return
	case "settings.run":
		settings.Start(version, commit)
		return
	}

	serviceName, _ := gocore.Config().Get("SERVICE_NAME", "ubsv")
	logger := initLogger(serviceName)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	// Before continuing, if the command line contains "-wait_for_postgres=1", wait for postgres to be ready
	if shouldStart("wait_for_postgres") {
		if err := waitForPostgresToStart(logger); err != nil {
			logger.Fatalf("error waiting for postgres: %v", err)
		}
	}

	sm, ctx := servicemanager.NewServiceManager(logger)

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

	err := startServices(ctx, logger, serviceName, sm)
	if err != nil {
		logger.Fatalf("error starting services: %v", err)
	}

	util.RegisterPrometheusMetrics()

	mux := http.NewServeMux()
	healthFunc := func(liveness bool) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			status, details, err := sm.HealthHandler(ctx, liveness)
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

	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		logger.Warnf("health_check_port not set in config, using default port 8000")
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Error starting server: %v", err)
		}
	}()

	logger.Infof("Health check endpoint listening on http://localhost:%d/health", port)

	if err := sm.Wait(); err != nil {
		logger.Errorf("services failed: %v", err)
	}

}

func startServices(ctx context.Context, logger ulogger.Logger, serviceName string, sm *servicemanager.ServiceManager) error {
	help := shouldStart("help")
	startBlockchain := shouldStart("Blockchain")
	startBlockAssembly := shouldStart("BlockAssembly")
	startSubtreeValidation := shouldStart("SubtreeValidation")
	startBlockValidation := shouldStart("BlockValidation")
	startValidator := shouldStart("Validator")
	startPropagation := shouldStart("Propagation")
	startMiner := shouldStart("Miner")
	startP2P := shouldStart("P2P")
	startAsset := shouldStart("Asset")
	startCoinbase := shouldStart("Coinbase")
	startFaucet := shouldStart("Faucet")
	startBlockPersister := shouldStart("BlockPersister")
	startUTXOPersister := shouldStart("UTXOPersister")
	startLegacy := shouldStart("Legacy")
	startRPC := shouldStart("RPC")
	startAlert := shouldStart("Alert")

	if help || appCount == 0 {
		printUsage()
		return nil
	}

	go func() {
		var profilerAddr string
		var ok bool
		profilerAddr, ok = gocore.Config().Get("profilerAddr")
		if ok {
			logger.Infof("Profiler listening on http://%s/debug/pprof", profilerAddr)

			gocore.RegisterStatsHandlers()
			prefix, _ := gocore.Config().Get("stats_prefix")
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
		}
	}()

	if gocore.Config().GetBool("use_datadog_profiler", false) {
		deferFn := datadogProfiler()
		defer deferFn()
	}

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	// tracingOn := gocore.Config().GetBool("tracing")
	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting tracer")
		// closeTracer := tracing.InitOtelTracer()
		// defer closeTracer()
		samplingRateStr, _ := gocore.Config().Get("tracing_SampleRate", "0.01")
		samplingRate, err := strconv.ParseFloat(samplingRateStr, 64)
		if err != nil {
			logger.Errorf("error parsing sampling rate: %v", err)
			samplingRate = 0.01
		}

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
		blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
		if err != nil || !found {
			return err
		}

		blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL)
		if err != nil {
			return err
		}

		blockchainService, err = blockchain.New(ctx, logger.New("bchn"), blockchainStore)
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockChainService", blockchainService); err != nil {
			return err
		}
	}

	// p2p server
	if startP2P {
		blockchainClient, err := getBlockchainClient(ctx, logger, "p2p")
		if err != nil {
			return err
		}

		p2pService, err := p2p.NewServer(ctx,
			logger.New("P2P"),
			blockchainClient,
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
		utxoStore, err := getUtxoStore(ctx, logger)
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

		blockchainClient, err := getBlockchainClient(ctx, logger, "asset")
		if err != nil {
			return err
		}

		if err := sm.AddService("Asset", asset.NewServer(
			logger.New("asset"),
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
		blockchainClient, err := getBlockchainClient(ctx, logger, "rpc")
		if err != nil {
			return err
		}

		rpcServer, err := rpc.NewServer(logger.New("RPC"), blockchainClient)
		if err != nil {
			return err

		}

		if err := sm.AddService("Rpc", rpcServer); err != nil {
			return err
		}
	}

	if startAlert {
		blockchainClient, err := getBlockchainClient(ctx, logger, "alert")
		if err != nil {
			return err
		}

		utxoStore, err := getUtxoStore(ctx, logger)
		if err != nil {
			return err
		}

		blockassemblyClient, err := blockassembly.NewClient(ctx, logger)
		if err != nil {
			return err
		}

		if err = sm.AddService("Alert", alert.New(
			logger.New("alert"),
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

		utxoStore, err := getUtxoStore(ctx, logger)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, "blockpersister")
		if err != nil {
			return err
		}

		if err = sm.AddService("BlockPersister", blockpersister.New(ctx,
			logger.New("bp"),
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

		blockchainClient, err := getBlockchainClient(ctx, logger, "utxopersister")
		if err != nil {
			return err
		}

		if err := sm.AddService("UTXOPersister", utxopersister.New(ctx,
			logger.New("utxop"),
			blockStore,
			blockchainClient,
		)); err != nil {
			return err

		}
	}

	// blockAssembly
	if startBlockAssembly {
		if _, found := gocore.Config().Get("blockassembly_grpcListenAddress"); found {
			txStore, err := getTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := getUtxoStore(ctx, logger)
			if err != nil {
				return err
			}

			subtreeStore, err := getSubtreeStore(logger)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, "blockassembly")
			if err != nil {
				return err
			}

			if err = sm.AddService("BlockAssembly", blockassembly.New(
				logger.New("bass"),
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

		utxoStore, err := getUtxoStore(ctx, logger)
		if err != nil {
			return err
		}

		validatorClient, err := getValidatorClient(ctx, logger)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, "subtreevalidation")
		if err != nil {
			return err
		}

		subtreeValidationService, err := subtreevalidation.New(ctx,
			logger.New("stval"),
			subtreeStore,
			txStore,
			utxoStore,
			validatorClient,
			blockchainClient,
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
		if _, found := gocore.Config().Get("blockvalidation_grpcListenAddress"); found {
			subtreeStore, err := getSubtreeStore(logger)
			if err != nil {
				return err
			}

			txStore, err := getTxStore(logger)
			if err != nil {
				return err
			}

			utxoStore, err := getUtxoStore(ctx, logger)
			if err != nil {
				return err
			}

			validatorClient, err := getValidatorClient(ctx, logger)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, "blockvalidation")
			if err != nil {
				return err
			}

			if err = sm.AddService("Block Validation", blockvalidation.New(
				logger.New("bval"),
				subtreeStore,
				txStore,
				utxoStore,
				validatorClient,
				blockchainClient,
			)); err != nil {
				return err
			}
		}
	}

	// validator
	if startValidator {
		if _, found := gocore.Config().Get("validator_grpcListenAddress"); found {
			utxoStore, err := getUtxoStore(ctx, logger)
			if err != nil {
				return err
			}

			blockchainClient, err := getBlockchainClient(ctx, logger, "validator")
			if err != nil {
				return err
			}

			if err = sm.AddService("Validator", validator.NewServer(
				logger.New("validator"),
				utxoStore,
				blockchainClient,
			)); err != nil {
				return err
			}
		}
	}

	// coinbase tracker server
	if startCoinbase {
		blockchainClient, err := getBlockchainClient(ctx, logger, "coinbase")
		if err != nil {
			return err
		}

		if err = sm.AddService("Coinbase", coinbase.New(
			logger.New("coinB"),
			blockchainClient,
		)); err != nil {
			return err

		}
	}

	if startFaucet {
		blockchainClient, err := getBlockchainClient(ctx, logger, "faucet")
		if err != nil {
			return err
		}

		if err = sm.AddService("Faucet", faucet.New(
			logger.New("faucet"),
			blockchainClient,
		)); err != nil {
			return err

		}
	}

	// propagation
	if startPropagation {
		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcListenAddress")
		if ok && propagationGrpcAddress != "" {
			if gocore.Config().GetBool("propagation_use_dumb", false) {
				if err := sm.AddService("PropagationServer", propagation.NewDumbPropagationServer()); err != nil {
					return err
				}
			} else {
				txStore, err := getTxStore(logger)
				if err != nil {
					return err
				}

				validatorClient, err := getValidatorClient(ctx, logger)
				if err != nil {
					return err
				}

				blockchainClient, err := getBlockchainClient(ctx, logger, "propagation")
				if err != nil {
					return err
				}

				if err = sm.AddService("PropagationServer", propagation.New(
					logger.New("prop"),
					txStore,
					validatorClient,
					blockchainClient,
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

		utxoStore, err := getUtxoStore(ctx, logger)
		if err != nil {
			return err
		}

		validatorClient, err := getValidatorClient(ctx, logger)
		if err != nil {
			return err
		}

		blockchainClient, err := getBlockchainClient(ctx, logger, "legacy")
		if err != nil {
			return err
		}

		subtreeValidationClient, err := getSubtreeValidationClient(ctx, logger)
		if err != nil {
			return err
		}

		blockValidationClient, err := getBlockValidationClient(ctx, logger)
		if err != nil {
			return err
		}

		if err = sm.AddService("Legacy", legacy.New(
			logger.New("legacy"),
			blockchainClient,
			validatorClient,
			subtreeStore,
			utxoStore,
			subtreeValidationClient,
			blockValidationClient,
		)); err != nil {
			return err
		}
	}

	if startMiner {
		blockchainClient, err := getBlockchainClient(ctx, logger, "miner")
		if err != nil {
			return err
		}

		minerService, err := miner.NewMiner(
			ctx,
			logger.New("miner"),
			blockchainClient,
		)
		if err != nil {
			return err
		}

		if err = sm.AddService("miner", minerService); err != nil {
			return err
		}
	}

	return nil
}

func initLogger(serviceName string) ulogger.Logger {
	logLevel, _ := gocore.Config().Get("logLevel", "info")
	logOptions := []ulogger.Option{
		ulogger.WithLevel(logLevel),
	}

	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isTerminal, // Disable color if output is not a terminal
	}

	logOptions = append(logOptions, ulogger.WithWriter(output))

	useLogger, ok := gocore.Config().Get("logger")
	if ok && useLogger != "" {
		logOptions = append(logOptions, ulogger.WithLoggerType(useLogger))
	}

	logger := ulogger.New(progname, logOptions...)

	return logger
}

func shouldStart(app string) bool {

	// See if the app is enabled in the command line
	cmdArg := fmt.Sprintf("-%s=1", strings.ToLower(app))
	for _, cmd := range os.Args[1:] {
		if cmd == cmdArg {
			appCount++
			return true
		}
	}

	// See if the app is disabled in the command line
	cmdArg = fmt.Sprintf("-%s=0", strings.ToLower(app))
	for _, cmd := range os.Args[1:] {
		if cmd == cmdArg {
			return false
		}
	}

	// Add option to stop all services from running if -all=0 is passed
	// except for the services that are explicitly enabled above
	for _, cmd := range os.Args[1:] {
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
	fmt.Println("    -miner=<1|0>")
	fmt.Println("          whether to start the miner service")
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

func waitForPostgresToStart(logger ulogger.Logger) error {
	address, _ := gocore.Config().Get("postgres_check_address", "localhost:5432")

	timeout := time.Minute // 1 minutes timeout

	logger.Infof("Waiting for PostgreSQL to be ready at %s\n", address)

	deadline := time.Now().Add(timeout)

	for {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			if time.Now().After(deadline) {
				return errors.NewStorageError("timed out waiting for PostgreSQL to start: %w", err)
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
