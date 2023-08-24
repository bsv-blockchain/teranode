package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
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
	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/seeder"
	"github.com/bitcoin-sv/ubsv/services/txmeta"
	"github.com/bitcoin-sv/ubsv/services/txmeta/store"
	"github.com/bitcoin-sv/ubsv/services/utxo"
	"github.com/bitcoin-sv/ubsv/services/validator"
	validator_utxostore "github.com/bitcoin-sv/ubsv/services/validator/utxo"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/getsentry/sentry-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "ubsv"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

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
	if sentryDns, ok := gocore.Config().Get("sentry_dsn"); ok {
		tracesSampleRateStr, _ := gocore.Config().Get("sentry_traces_sample_rate", "1.0")
		tracesSampleRate, err := strconv.ParseFloat(tracesSampleRateStr, 64)
		if err != nil {
			logger.Fatalf("failed to parse sentry_traces_sample_rate: %v", err)
		}

		if err = sentry.Init(sentry.ClientOptions{
			Dsn: sentryDns,
			// Set TracesSampleRate to 1.0 to capture 100% of transactions for performance monitoring.
			// We recommend adjusting this value in production,
			TracesSampleRate: tracesSampleRate,
		}); err != nil {
			logger.Fatalf("sentry.Init: %s", err)
		}
	}

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
	help := shouldStart("help")

	if help ||
		(!startBlockchain &&
			!startBlockAssembly &&
			!startBlockValidation &&
			!startValidator &&
			!startUtxoStore &&
			!startTxMetaStore &&
			!startPropagation &&
			!startSeeder &&
			!startMiner &&
			!startBootstrap &&
			!startBlobServer &&
			!startCoinbase) {
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
		fmt.Println("    -tracer=<1|0>")
		fmt.Println("          whether to start the Jaeger tracer (default=false)")
		fmt.Println("")
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
		serviceName := os.Getenv("SERVICE_NAME")
		if serviceName == "" {
			serviceName = "ubsv" // default to ubsv in case the service is not passed
		}
		_, closer, err := utils.InitGlobalTracer(serviceName)
		if err != nil {
			logger.Fatalf("failed to initialize tracer: %v", err)
		}
		defer closer.Close()
	}

	sm, ctx := servicemanager.NewServiceManager()

	// bootstrap server
	if startBootstrap {
		sm.AddService("Bootstrap", bootstrap.NewServer(
			gocore.Log("bootS"),
		))
	}

	var blockchainService *blockchain.Blockchain
	// blockchain service needs to start first !
	if startBlockchain {
		var err error
		blockchainService, err = blockchain.New(gocore.Log("bchn"))
		if err != nil {
			panic(err)
		}

		// TODO - for a temporary period, we will start the blockchain service
		// outside of the service manager. This is because the blockchain service
		// needs to be running before the other services start.

		if err := blockchainService.Init(ctx); err != nil {
			panic(err)
		}

		go func() {
			if err := blockchainService.Start(ctx); err != nil {
				panic(err)
			}
		}()

		time.Sleep(3 * time.Second)

		//sm.AddService("BlockChainService", blockchainService)
	}

	//----------------------------------------------------------------
	// These are the main stores used in the system
	//
	var utxostoreURL *url.URL
	var utxoStore utxostore.Interface
	var err error
	var found bool

	if startBlockValidation || startValidator || startUtxoStore {
		utxostoreURL, err, found = gocore.Config().GetURL("utxostore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no utxostore setting found")
		}
		utxoStore, err = validator_utxostore.NewStore(ctx, logger, utxostoreURL, "main")
		if err != nil {
			panic(err)
		}
	}

	var txStore blob.Store

	var txStoreUrl *url.URL
	if startBlockAssembly || startBlobServer || startPropagation {
		txStoreUrl, err, found = gocore.Config().GetURL("txstore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("txstore config not found")
		}
		txStore, err = blob.NewStore(txStoreUrl)
		if err != nil {
			panic(err)
		}
	}

	var txMetaStoreURL *url.URL
	var txMetaStore txmetastore.Store

	if startTxMetaStore || startBlockValidation || startValidator {
		txMetaStoreURL, err, found = gocore.Config().GetURL("txmeta_store")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no txmeta_store setting found")
		}

		if txMetaStoreURL.Scheme == "memory" {
			// the memory store is reached through a grpc client
			txMetaStore, err = txmeta.NewClient(context.Background(), logger)
			if err != nil {
				panic(err)
			}
		} else {
			txMetaStore, err = store.New(logger, txMetaStoreURL)
			if err != nil {
				panic(err)
			}
		}
	}

	var subtreeStore blob.Store
	var subtreeStoreUrl *url.URL
	if startBlockAssembly || startBlockValidation || startBlobServer {
		subtreeStoreUrl, err, found = gocore.Config().GetURL("subtreestore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("subtreestore config not found")
		}
		subtreeStore, err = blob.NewStore(subtreeStoreUrl)
		if err != nil {
			panic(err)
		}
	}

	//
	//----------------------------------------------------------------
	var validatorClient *validator.Client

	if startBlockValidation || startPropagation {
		validatorClient, err = validator.NewClient(ctx, logger)
		if err != nil {
			logger.Fatalf("error creating validator client: %v", err)
		}
	}

	// txmeta store
	if startTxMetaStore {
		if txMetaStoreURL.Scheme != "memory" {
			panic("txmeta grpc server only supports memory store")
		}
		sm.AddService("TxMetaStore", txmeta.New(
			gocore.Log("txsts"),
			txMetaStoreURL,
		))
	}

	// blockAssembly
	if startBlockAssembly {
		if _, found = gocore.Config().Get("blockassembly_grpcListenAddress"); found {
			sm.AddService("BlockAssembly", blockassembly.New(
				gocore.Log("bass"),
				txStore,
				subtreeStore,
			))
		}
	}

	// blockValidation
	if startBlockValidation {
		if _, found = gocore.Config().Get("blockvalidation_grpcListenAddress"); found {
			sm.AddService("Block Validation", blockvalidation.New(
				gocore.Log("bval"),
				utxoStore,
				subtreeStore,
				txStore,
				txMetaStore,
				validatorClient,
			))
		}
	}

	// validator
	if startValidator {
		if _, found = gocore.Config().Get("validator_grpcListenAddress"); found {
			sm.AddService("Validator", validator.NewServer(
				gocore.Log("valid"),
				utxoStore,
				txMetaStore,
			))
		}
	}

	// utxo store server
	if startUtxoStore && utxostoreURL != nil {
		sm.AddService("UTXOStoreServer", utxo.New(
			gocore.Log("utxo"),
			utxoStore,
		))
	}

	// seeder
	if startSeeder {
		_, found = gocore.Config().Get("seeder_grpcListenAddress")
		if found {
			sm.AddService("Seeder", seeder.NewServer(
				gocore.Log("seed"),
			))
		}
	}

	// miner
	if startMiner {
		sm.AddService("miner", miner.NewMiner(ctx))
	}

	// blob server
	if startBlobServer {
		sm.AddService("BlobServer", blobserver.NewServer(
			gocore.Log("blob"),
			utxoStore,
			txStore,
			subtreeStore,
		))
	}

	// coinbase tracker server
	if startCoinbase {
		sm.AddService("Coinbase", coinbase.New(
			gocore.Log("coinB"),
		))
	}

	// propagation
	if startPropagation {
		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcListenAddress")
		if ok && propagationGrpcAddress != "" {
			sm.AddService("PropagationServer", propagation.New(
				gocore.Log("prop"),
				txStore,
				validatorClient,
			))
		}
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	if err = sm.StartAllAndWait(); err != nil {
		logger.Errorf("failed to start all services: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// TODO - As blockchain service is being started manually, we need to stop it manually
	if blockchainService != nil {
		logger.Infof("stopping blockchain service")
		if err := blockchainService.Stop(shutdownCtx); err != nil {
			logger.Errorf("failed to stop blockchain service: %v", err)
		}
	}

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

func shouldStart(app string) bool {

	// See if the app is enabled in the command line
	cmdArg := fmt.Sprintf("-%s=1", strings.ToLower(app))
	for _, cmd := range os.Args[1:] {
		if cmd == cmdArg {
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

	return gocore.Config().GetBool(varArg)
}
