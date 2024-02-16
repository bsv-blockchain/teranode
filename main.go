package main

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/services/subtreeassembly"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	zlogsentry "github.com/archdx/zerolog-sentry"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/aerospiketest"
	"github.com/bitcoin-sv/ubsv/cmd/bare/bare"
	"github.com/bitcoin-sv/ubsv/cmd/blockassembly_blaster/blockassembly_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/blockchainstatus/blockchainstatus"
	"github.com/bitcoin-sv/ubsv/cmd/chainintegrity/chainintegrity"
	"github.com/bitcoin-sv/ubsv/cmd/propagation_blaster/propagation_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/s3_blaster/s3_blaster"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/txblaster"
	"github.com/bitcoin-sv/ubsv/cmd/utxostore_blaster/utxostore_blaster"
	"github.com/bitcoin-sv/ubsv/services/asset"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/bootstrap"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/faucet"
	"github.com/bitcoin-sv/ubsv/services/miner"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/getsentry/sentry-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "ubsv"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string
var appCount int

func init() {
	gocore.SetInfo(progname, version, commit)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})
}

func main() {
	// Flush buffered events before the program terminates.
	defer sentry.Flush(2 * time.Second)

	switch path.Base(os.Args[0]) {
	case "aerospiketest.run":
		// aerospiketest.Init()
		aerospiketest.Start()
		return
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
	case "utxostoreblaster.run":
		utxostore_blaster.Init()
		utxostore_blaster.Start()
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

	startBlockchain := shouldStart("Blockchain")
	startBlockAssembly := shouldStart("BlockAssembly")
	startBlockValidation := shouldStart("BlockValidation")
	startValidator := shouldStart("Validator")
	startPropagation := shouldStart("Propagation")
	startMiner := shouldStart("Miner")
	startAsset := shouldStart("Asset")
	startCoinbase := shouldStart("Coinbase")
	startFaucet := shouldStart("Faucet")
	startBootstrap := shouldStart("Bootstrap")
	startP2P := shouldStart("P2P")
	startSubtreeAssembly := shouldStart("SubtreeAssembly")
	help := shouldStart("help")

	if help || appCount == 0 {
		printUsage()
		return
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

			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}
	}()

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

		_, closer, err := util.InitGlobalTracer(serviceName, samplingRate)
		if err != nil {
			logger.Warnf("failed to initialize tracer: %v", err)
		}
		if closer != nil {
			defer closer.Close()
		}
	}

	sm, ctx := servicemanager.NewServiceManager(logger)

	var blockchainService *blockchain.Blockchain

	// blockchain service
	if startBlockchain {
		var err error
		blockchainService, err = blockchain.New(logger.New("bchn"))
		if err != nil {
			panic(err)
		}

		if err := sm.AddService("BlockChainService", blockchainService); err != nil {
			panic(err)
		}
	}

	// bootstrap server
	if startBootstrap {
		if err := sm.AddService("Bootstrap", bootstrap.NewServer(
			logger.New("bootS"),
		)); err != nil {
			panic(err)
		}
	}

	var err error

	// asset service
	if startAsset {
		if err := sm.AddService("Asset", asset.NewServer(
			logger.New("asset"),
			getUtxoStore(ctx, logger),
			getTxStore(logger),
			getTxMetaStore(logger),
			getSubtreeStore(logger),
		)); err != nil {
			panic(err)
		}
	}

	var blockValidationClient *blockvalidation.Client
	if startBlockAssembly || startPropagation || startValidator {
		blockValidationClient = blockvalidation.NewClient(ctx, logger)
	}

	if startSubtreeAssembly {
		blockchainClient, err := blockchain.NewClient(ctx, logger)
		if err != nil {
			panic(err)
		}

		if err = sm.AddService("SubtreeAssembly", subtreeassembly.New(
			logger,
			blockchainClient,
			getSubtreeStore(logger),
			getTxMetaStore(logger),
		)); err != nil {
			panic(err)
		}
	}

	// blockAssembly
	if startBlockAssembly {
		if _, found := gocore.Config().Get("blockassembly_grpcListenAddress"); found {
			// should this be done globally somewhere?
			blockchainClient, err := blockchain.NewClient(ctx, logger)
			if err != nil {
				panic(err)
			}

			assetAddr, ok := gocore.Config().Get("coinbase_assetGrpcAddress")
			if !ok {
				assetAddr, ok = gocore.Config().Get("asset_grpcAddress")
				if !ok {
					panic(err)
				}
			}

			assetClient, err := asset.NewClient(ctx, logger, assetAddr)
			if err != nil {
				panic(err)
			}

			if err = sm.AddService("BlockAssembly", blockassembly.New(
				logger.New("bass"),
				getTxStore(logger),
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
				getSubtreeStore(logger),
				blockchainClient,
				assetClient,
				blockValidationClient, // TODO replace with getSubtreeStore(logger) when running block assembly and block validation on the same node
			)); err != nil {
				panic(err)
			}
		}
	}

	// blockValidation
	if startBlockValidation {
		if _, found := gocore.Config().Get("blockvalidation_grpcListenAddress"); found {
			// create a local validator client
			validatorClient, err := validator.New(ctx,
				logger,
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
				nil,
			)
			if err != nil {
				logger.Fatalf("could not create validator [%v]", err)
			}

			if err := sm.AddService("Block Validation", blockvalidation.New(
				logger.New("bval"),
				getUtxoStore(ctx, logger),
				getSubtreeStore(logger),
				getTxStore(logger),
				getTxMetaStore(logger),
				validatorClient,
			)); err != nil {
				panic(err)
			}
		}
	}

	// validator
	if startValidator {
		if _, found := gocore.Config().Get("validator_grpcListenAddress"); found {
			if err := sm.AddService("Validator", validator.NewServer(
				logger.New("valid"),
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
				blockValidationClient,
			)); err != nil {
				panic(err)
			}
		}
	}

	// p2p server
	if startP2P {
		if err = sm.AddService("P2P", p2p.NewServer(
			logger.New("P2P"),
		)); err != nil {
			panic(err)
		}
	}

	// coinbase tracker server
	if startCoinbase {
		if err = sm.AddService("Coinbase", coinbase.New(
			logger.New("coinB"),
		)); err != nil {
			panic(err)
		}
	}

	if startFaucet {
		if err = sm.AddService("Faucet", faucet.New(
			logger.New("faucet"),
		)); err != nil {
			panic(err)
		}
	}

	// propagation
	if startPropagation {
		var validatorClient validator.Interface
		localValidator := gocore.Config().GetBool("useLocalValidator", false)
		if localValidator {
			logger.Infof("[Validator] Using local validator")
			validatorClient, err = validator.New(ctx,
				logger,
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
				blockValidationClient,
			)
			if err != nil {
				logger.Fatalf("could not create validator [%v]", err)
			}

		} else {
			validatorClient, err = validator.NewClient(ctx, logger)
			if err != nil {
				logger.Fatalf("error creating validator client: %v", err)
			}
		}

		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcListenAddress")
		if ok && propagationGrpcAddress != "" {
			if gocore.Config().GetBool("propagation_use_dumb", false) {
				if err := sm.AddService("PropagationServer", propagation.NewDumbPropagationServer()); err != nil {
					panic(err)
				}
			} else {
				if err = sm.AddService("PropagationServer", propagation.New(
					logger.New("prop"),
					getTxStore(logger),
					validatorClient,
				)); err != nil {
					panic(err)
				}
			}
		}
	}

	// miner
	if startMiner {
		if err = sm.AddService("miner", miner.NewMiner(ctx, logger.New("miner"))); err != nil {
			panic(err)
		}
	}

	// start prometheus metrics
	util.RegisterPrometheusMetrics()

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	if err = sm.Wait(); err != nil {
		logger.Errorf("services failed: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	//
	// close all the stores
	//

	if txStore != nil {
		logger.Debugf("closing tx store")
		_ = txStore.Close(shutdownCtx)
	}

	if subtreeStore != nil {
		logger.Debugf("closing subtree store")
		_ = subtreeStore.Close(shutdownCtx)
	}
}

func initLogger(serviceName string) ulogger.Logger {
	logLevel, _ := gocore.Config().Get("logLevel", "info")
	logOptions := []ulogger.Option{
		ulogger.WithLevel(logLevel),
	}

	// sentry
	if sentryDns, ok := gocore.Config().Get("sentry_dsn"); ok && sentryDns != "" {
		tracesSampleRateStr, _ := gocore.Config().Get("sentry_traces_sample_rate", "1.0")
		tracesSampleRate, err := strconv.ParseFloat(tracesSampleRateStr, 64)
		if err != nil {
			panic("failed to parse sentry_traces_sample_rate: " + err.Error())
		}

		w, err := zlogsentry.New(sentryDns,
			zlogsentry.WithEnvironment("dev"),
			zlogsentry.WithRelease("1.0.0"),
			zlogsentry.WithServerName(serviceName),
			zlogsentry.WithSampleRate(tracesSampleRate),
		)
		if err != nil {
			panic("sentry.Init: " + err.Error())
		}

		multi := zerolog.MultiLevelWriter(os.Stdout, w)
		logOptions = append(logOptions, ulogger.WithWriter(multi))
	}

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
	fmt.Println("    -txmeta=<1|0>")
	fmt.Println("          whether to start the tx meta store")
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
	fmt.Println("    -bootstrap=<1|0>")
	fmt.Println("          whether to start the bootstrap server")
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
				return fmt.Errorf("timed out waiting for PostgreSQL to start: %w", err)
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
