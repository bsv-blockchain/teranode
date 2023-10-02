package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/bootstrap"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/miner"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/seeder"
	"github.com/bitcoin-sv/ubsv/services/txmeta"
	"github.com/bitcoin-sv/ubsv/services/utxo"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/getsentry/sentry-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	logger := gocore.Log(progname)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	// sentry
	startSentry(logger)

	startBlockchain := shouldStart("Blockchain")
	startBlockAssembly := shouldStart("BlockAssembly")
	startBlockValidation := shouldStart("BlockValidation")
	startValidator := shouldStart("Validator")
	startUtxoStore := shouldStart("UtxoStore")
	startTxMetaStore := shouldStart("TxMetaStore")
	startPropagation := shouldStart("Propagation")
	startSeeder := shouldStart("Seeder")
	startMiner := shouldStart("Miner")
	startBlobServer := shouldStart("BlobServer")
	startCoinbase := shouldStart("Coinbase")
	startBootstrap := shouldStart("Bootstrap")
	startP2P := shouldStart("P2P")
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
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}
	}()

	go func() {
		statisticsServerAddr, found := gocore.Config().Get("gocore_stats_addr")
		if found {
			servicemanager.AddListenerInfo(fmt.Sprintf("StatsServer HTTP listening on %s", statisticsServerAddr))
			gocore.StartStatsServer(statisticsServerAddr)
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
		serviceName, _ := gocore.Config().Get("SERVICE_NAME", "ubsv")

		_, closer, err := util.InitGlobalTracer(serviceName)
		if err != nil {
			logger.Warnf("failed to initialize tracer: %v", err)
		}
		if closer != nil {
			defer closer.Close()
		}
	}

	sm, ctx := servicemanager.NewServiceManager()

	var blockchainService *blockchain.Blockchain

	// blockchain service needs to start first !
	if startBlockchain {
		var err error
		blockchainService, err = blockchain.New(gocore.Log("bchn"))
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
			gocore.Log("bootS"),
		)); err != nil {
			panic(err)
		}
	}

	var err error

	// txmeta store
	if startTxMetaStore {
		txMetaStoreURL, _, found := gocore.Config().GetURL("txmeta_store")
		if !found {
			panic("no txmeta_store setting found")
		}
		if txMetaStoreURL.Scheme != "memory" {
			panic("txmeta grpc server only supports memory store")
		}
		if err := sm.AddService("TxMetaStore", txmeta.New(
			gocore.Log("txsts"),
			txMetaStoreURL,
		)); err != nil {
			panic(err)
		}
	}

	// blockAssembly
	if startBlockAssembly {
		if _, found := gocore.Config().Get("blockassembly_grpcListenAddress"); found {
			// should this be done globally somewhere?
			blockchainClient, err := blockchain.NewClient(ctx)
			if err != nil {
				panic(err)
			}

			if err = sm.AddService("BlockAssembly", blockassembly.New(
				gocore.Log("bass"),
				getTxStore(),
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
				getSubtreeStore(),
				blockchainClient,
			)); err != nil {
				panic(err)
			}
		}
	}

	var validatorClient validator.Interface
	if startBlockValidation || startPropagation {
		localValidator := gocore.Config().GetBool("useLocalValidator", false)
		if localValidator {
			logger.Infof("[Validator] Using local validator")
			validatorClient, err = validator.New(ctx,
				logger,
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
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
	}

	// blockValidation
	if startBlockValidation {
		if _, found := gocore.Config().Get("blockvalidation_grpcListenAddress"); found {
			if err := sm.AddService("Block Validation", blockvalidation.New(
				gocore.Log("bval"),
				getUtxoStore(ctx, logger),
				getSubtreeStore(),
				getTxStore(),
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
				gocore.Log("valid"),
				getUtxoStore(ctx, logger),
				getTxMetaStore(logger),
			)); err != nil {
				panic(err)
			}
		}
	}

	// utxo store server
	if startUtxoStore {
		if err := sm.AddService("UTXOStoreServer", utxo.New(
			gocore.Log("utxo"),
			getUtxoMemoryStore(),
		)); err != nil {
			panic(err)
		}
	}

	// seeder
	if startSeeder {
		_, found := gocore.Config().Get("seeder_grpcListenAddress")
		if found {
			if err := sm.AddService("Seeder", seeder.NewServer(
				gocore.Log("seed"),
			)); err != nil {
				panic(err)
			}
		}
	}

	// blob server
	if startBlobServer {
		if err := sm.AddService("BlobServer", blobserver.NewServer(
			gocore.Log("blob"),
			getUtxoStore(ctx, logger),
			getTxStore(),
			getSubtreeStore(),
		)); err != nil {
			panic(err)
		}
	}

	// p2p server
	if startP2P {
		if err := sm.AddService("P2P", p2p.NewServer(
			gocore.Log("P2P"),
		)); err != nil {
			panic(err)
		}
	}

	// coinbase tracker server
	if startCoinbase {
		if err := sm.AddService("Coinbase", coinbase.New(
			gocore.Log("coinB"),
		)); err != nil {
			panic(err)
		}
	}

	// propagation
	if startPropagation {
		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcListenAddress")
		if ok && propagationGrpcAddress != "" {
			if gocore.Config().GetBool("propagation_use_dumb", false) {
				if err := sm.AddService("PropagationServer", propagation.NewDumbPropagationServer()); err != nil {
					panic(err)
				}
			} else {
				if err := sm.AddService("PropagationServer", propagation.New(
					gocore.Log("prop"),
					getTxStore(),
					validatorClient,
				)); err != nil {
					panic(err)
				}
			}
		}
	}

	// miner
	if startMiner {
		if err := sm.AddService("miner", miner.NewMiner(ctx)); err != nil {
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

func startSentry(logger *gocore.Logger) {
	if sentryDns, ok := gocore.Config().Get("sentry_dsn"); ok {
		tracesSampleRateStr, _ := gocore.Config().Get("sentry_traces_sample_rate", "1.0")
		tracesSampleRate, err := strconv.ParseFloat(tracesSampleRateStr, 64)
		if err != nil {
			logger.Fatalf("failed to parse sentry_traces_sample_rate: %v", err)
		}

		if err = sentry.Init(sentry.ClientOptions{
			Dsn: sentryDns,

			TracesSampleRate: tracesSampleRate,
		}); err != nil {
			logger.Fatalf("sentry.Init: %s", err)
		}
	}
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
	fmt.Println("    -blobserver=<1|0>")
	fmt.Println("          whether to start the blob server")
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
