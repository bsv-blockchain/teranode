package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blobserver"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/blockvalidation"
	"github.com/TAAL-GmbH/ubsv/services/bootstrap"
	"github.com/TAAL-GmbH/ubsv/services/coinbasetracker"
	"github.com/TAAL-GmbH/ubsv/services/miner"
	"github.com/TAAL-GmbH/ubsv/services/propagation"
	"github.com/TAAL-GmbH/ubsv/services/seeder"
	"github.com/TAAL-GmbH/ubsv/services/txmeta"
	"github.com/TAAL-GmbH/ubsv/services/txmeta/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	validator_utxostore "github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmetastore "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/utxo/memory"
	"github.com/getsentry/sentry-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/servicemanager"
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
}

func main() {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log(progname, gocore.NewLogLevelFromString(logLevel))

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	err := sentry.Init(sentry.ClientOptions{
		Dsn: "https://dcad1ec4c60a4a2e80a7f8599e86ec4b@o4505013263466496.ingest.sentry.io/4505013264449536",
		// Set TracesSampleRate to 1.0 to capture 100% of transactions for performance monitoring.
		// We recommend adjusting this value in production,
		TracesSampleRate: 1.0,
	})
	if err != nil {
		logger.Fatalf("sentry.Init: %s", err)
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
	startCoinbaseTracker := shouldStart("CoinbaseTracker")
	startBootstrapServer := shouldStart("BootstrapServer")
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
			!startBootstrapServer &&
			!startBlobServer &&
			!startCoinbaseTracker) {
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
		fmt.Println("    -coinbasetracker=<1|0>")
		fmt.Println("          whether to start the coinbase tracker server")
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

	sm := servicemanager.NewServiceManager()

	// blockchain service needs to start first !
	if startBlockchain {
		blockchainService, err := blockchain.New(logger)
		if err != nil {
			panic(err)
		}

		sm.AddService("BlockChainService", blockchainService)
	}

	//----------------------------------------------------------------
	// These are the main stores used in the system
	//
	var utxostoreURL *url.URL
	var utxoStore utxostore.Interface

	if startBlockValidation || startValidator || startUtxoStore {
		var err error
		var found bool

		utxostoreURL, err, found = gocore.Config().GetURL("utxostore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no utxostore setting found")
		}
		utxoStore, err = validator_utxostore.NewStore(logger, utxostoreURL)
		if err != nil {
			panic(err)
		}
	}

	var txStore blob.Store

	if startBlockAssembly || startBlobServer || startPropagation {
		var err error
		txStoreUrl, err, found := gocore.Config().GetURL("txstore")
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
		var err error
		var found bool

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

	if startBlockAssembly || startBlockValidation || startBlobServer {
		subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
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
		validatorClient, err = validator.NewClient(context.Background(), logger)
		if err != nil {
			logger.Fatalf("error creating validator client: %v", err)
		}
	}

	// txmeta store
	if startTxMetaStore {
		if txMetaStoreURL.Scheme != "memory" {
			panic("txmeta grpc server only supports memory store")
		}

		txMetaLogger := gocore.Log("txsts", gocore.NewLogLevelFromString(logLevel))
		txMetaStoreServer, err := txmeta.New(txMetaLogger, txMetaStoreURL)
		if err != nil {
			panic(err)
		}

		sm.AddService("TxMetaStore", txMetaStoreServer)
	}

	// blockAssembly
	if startBlockAssembly {
		if _, found := gocore.Config().Get("blockassembly_grpcAddress"); found {
			baLogger := gocore.Log("bchn", gocore.NewLogLevelFromString(logLevel))
			blockAssemblyService := blockassembly.New(context.TODO(), baLogger, txStore, subtreeStore)

			sm.AddService("BlockAssembly", blockAssemblyService)
		}
	}

	// blockValidation
	if startBlockValidation {
		if _, found := gocore.Config().Get("blockvalidation_grpcAddress"); found {
			bvLogger := gocore.Log("bval", gocore.NewLogLevelFromString(logLevel))
			blockValidationService, err := blockvalidation.New(bvLogger, utxoStore, subtreeStore, txMetaStore, validatorClient)
			if err != nil {
				logger.Errorf("blockvalidation errored: %v", err)
				panic(err)
			}

			sm.AddService("Block Validation", blockValidationService)
		}
	}

	// validator
	if startValidator {
		if validatorAddress, found := gocore.Config().Get("validator_grpcAddress"); found {
			logger.Infof("Starting Validator Server on: %s", validatorAddress)

			validatorLogger := gocore.Log("valid", gocore.NewLogLevelFromString(logLevel))
			validatorService := validator.NewServer(validatorLogger, utxoStore, txMetaStore)

			sm.AddService("Validator", validatorService)
		}
	}

	// utxostore
	if startUtxoStore && utxostoreURL != nil {
		logger.Infof("Starting UTXOStore on: %s", utxostoreURL.Host)

		var s utxostore.Interface
		switch utxostoreURL.Path {
		case "/splitbyhash":
			logger.Infof("[UTXOStore] using splitbyhash memory store")
			s = memory.NewSplitByHash(true)
		case "/swiss":
			logger.Infof("[UTXOStore] using swissmap memory store")
			s = memory.NewSwissMap(true)
		case "/xsyncmap":
			logger.Infof("[UTXOStore] using xsyncmap memory store")
			s = memory.NewXSyncMap(true)
		default:
			logger.Infof("[UTXOStore] using default memory store")
			s = memory.New(true)
		}

		utxoLogger := gocore.Log("utxo", gocore.NewLogLevelFromString(logLevel))
		utxoStoreServer, err := utxo.New(utxoLogger, s)
		if err != nil {
			logger.Errorf("utxo store failed: %v", err)
			panic(err)
		}

		sm.AddService("UTXOStoreServer", utxoStoreServer)
	}

	// seeder
	if startSeeder {
		seederURL, found := gocore.Config().Get("seeder_grpcAddress")
		if found {
			logger.Infof("Starting Seeder on: %s", seederURL)

			seederService := seeder.NewServer(gocore.Log("seed", gocore.NewLogLevelFromString(logLevel)))

			sm.AddService("Seeder", seederService)
		}
	}

	// miner
	if startMiner {
		minerServer := miner.NewMiner()

		sm.AddService("miner", minerServer)
	}

	// blob server
	if startBlobServer {
		blobServer, err := blobserver.NewServer(utxoStore, txStore, subtreeStore)
		if err != nil {
			panic(err)
		}

		sm.AddService("BlobServer", blobServer)
	}

	// coinbase tracker server
	if startCoinbaseTracker {
		coinbaseTrackerLogger := gocore.Log("con", gocore.NewLogLevelFromString(logLevel))
		coinbaseTrackerServer, err := coinbasetracker.New(coinbaseTrackerLogger)
		if err != nil {
			panic(err)
		}

		sm.AddService("CoinbaseTracker", coinbaseTrackerServer)
	}

	// bootstrap server
	if startBootstrapServer {
		bootstrapServer := bootstrap.NewServer()

		sm.AddService("BootstrapServer", bootstrapServer)
	}

	// propagation
	if startPropagation {
		// g.Go(func() error {
		// 	logger.Infof("Starting Propagation")

		// 	p2pLogger := gocore.Log("p2p", gocore.NewLogLevelFromString(logLevel))
		// 	propagationServer = propagation.NewServer(p2pLogger, txStore, subtreeStore, validatorClient)

		// 	return propagationServer.Start(ctx)
		// })

		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
		if ok && propagationGrpcAddress != "" {
			propagationGRPCServer, err := propagation.New(logger, txStore, validatorClient)
			if err != nil {
				logger.Errorf("propagation grpc server failed: %v", err)
				panic(err)
			}

			sm.AddService("PropagationServer", propagationGRPCServer)
		}
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	if err := sm.StartAllAndWait(context.Background()); err != nil {
		logger.Errorf("failed to start all services: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	//
	// close all the stores
	//

	if txStore != nil {
		_ = txStore.Close(shutdownCtx)
	}

	if subtreeStore != nil {
		_ = subtreeStore.Close(shutdownCtx)
	}

	logger.Info("\U0001f6d1 All services stopped.")
	// wait for clean shutdown for 5 seconds, otherwise force exit
	go func() {
		// Wait for 5 seconds and then force exit...
		<-time.NewTimer(time.Second * 5).C
		os.Exit(3)
	}()

}

func shouldStart(app string) bool {

	cmdArg := fmt.Sprintf("-%s=1", strings.ToLower(app))
	for _, cmd := range os.Args[1:] {
		if cmd == cmdArg {
			return true
		}
	}

	varArg := fmt.Sprintf("start%s", app)

	return gocore.Config().GetBool(varArg)
}
