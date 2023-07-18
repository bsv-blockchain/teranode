package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blobserver"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/blockvalidation"
	"github.com/TAAL-GmbH/ubsv/services/miner"
	"github.com/TAAL-GmbH/ubsv/services/propagation"
	"github.com/TAAL-GmbH/ubsv/services/seeder"
	"github.com/TAAL-GmbH/ubsv/services/txmeta"
	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	validator_utxostore "github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/utxo/memory"
	"github.com/getsentry/sentry-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
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

	startBlockchain := flag.Bool("blockchain", false, "start blockchain service")
	startBlockAssembly := flag.Bool("blockassembly", false, "start blockassembly service")
	startBlockValidation := flag.Bool("blockvalidation", false, "start blockvalidation service")
	startValidator := flag.Bool("validator", false, "start validator service")
	startUtxoStore := flag.Bool("utxostore", false, "start UTXO store")
	startTxMetaStore := flag.Bool("txmeta", false, "start txmeta store service")
	startPropagation := flag.Bool("propagation", false, "start propagation service")
	startSeeder := flag.Bool("seeder", false, "start seeder service")
	startMiner := flag.Bool("miner", false, "start miner service")
	startBlobServer := flag.Bool("blobserver", false, "start blob server")
	profileAddress := flag.String("profile", "", "use this profile port instead of the default")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if !*startBlockchain {
		*startBlockchain = gocore.Config().GetBool("startBlockchain", false)
	}

	if !*startBlockAssembly {
		*startBlockAssembly = gocore.Config().GetBool("startBlockAssembly", false)
	}

	if !*startBlockValidation {
		*startBlockValidation = gocore.Config().GetBool("startBlockValidation", false)
	}

	if !*startValidator {
		*startValidator = gocore.Config().GetBool("startValidator", false)
	}

	if !*startUtxoStore {
		*startUtxoStore = gocore.Config().GetBool("startUtxoStore", false)
	}

	if !*startTxMetaStore {
		*startTxMetaStore = gocore.Config().GetBool("startTxMetaStore", false)
	}

	if !*startPropagation {
		*startPropagation = gocore.Config().GetBool("startPropagation", false)
	}

	if !*startSeeder {
		*startSeeder = gocore.Config().GetBool("startSeeder", false)
	}

	if !*startMiner {
		*startMiner = gocore.Config().GetBool("startMiner", false)
	}

	if !*startBlobServer {
		*startBlobServer = gocore.Config().GetBool("startBlobServer", false)
	}

	if help != nil && *help ||
		(!*startBlockchain &&
			!*startBlockAssembly &&
			!*startBlockValidation &&
			!*startValidator &&
			!*startUtxoStore &&
			!*startTxMetaStore &&
			!*startPropagation &&
			!*startSeeder &&
			!*startMiner &&
			!*startBlobServer) {
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
		fmt.Println("    -tracer=<1|0>")
		fmt.Println("          whether to start the Jaeger tracer (default=false)")
		fmt.Println("")
		return
	}

	go func() {
		var profilerAddr string
		var ok bool
		if profileAddress != nil && *profileAddress != "" {
			profilerAddr, ok = *profileAddress, true
		} else {
			profilerAddr, ok = gocore.Config().Get("profilerAddr")
		}
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

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	g, ctx := errgroup.WithContext(ctx)

	var blockchainService *blockchain.Blockchain
	var validatorService *validator.Server
	var utxoStoreServer *utxo.UTXOStore
	var txMetaStore *txmeta.Server
	var propagationServer *propagation.Server
	var propagationGRPCServer *propagation.PropagationServer
	var blockAssemblyService *blockassembly.BlockAssembly
	var seederService *seeder.Server
	var minerServer *miner.Miner
	var blobServer *blobserver.Server

	//----------------------------------------------------------------
	// These are the main stores used in the system
	//
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}
	utxoStore, err := validator_utxostore.NewStore(logger, utxostoreURL)
	if err != nil {
		panic(err)
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStore, err := blob.NewStore(txStoreUrl)
	if err != nil {
		panic(err)
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err := blob.NewStore(subtreeStoreUrl)
	if err != nil {
		panic(err)
	}
	//
	//----------------------------------------------------------------

	// blockchain
	if *startBlockchain {
		blockchainService, err = blockchain.New(logger)
		if err != nil {
			panic(err)
		}

		g.Go(func() error {
			err := blockchainService.Start()
			if err != nil {
				logger.Errorf("blockchain service failed: %v", err)
				return err
			}
			return nil
		})
	}

	// txmeta store
	if *startTxMetaStore {
		txMetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
		if err != nil {
			panic(err)
		}

		if found {
			if txMetaStoreURL.Scheme != "memory" {
				panic("txmeta grpc server only supports memory store")
			}

			g.Go(func() (err error) {
				logger.Infof("Starting Tx Status Client on: %s", txMetaStoreURL.Host)

				txMetaLogger := gocore.Log("txsts", gocore.NewLogLevelFromString(logLevel))
				txMetaStore, err = txmeta.New(txMetaLogger, txMetaStoreURL)
				if err != nil {
					panic(err)
				}

				if err := txMetaStore.Start(); err != nil {
					logger.Errorf("txMetaStore errored: %v", err)
				}

				return nil
			})
		}
	}

	// blockAssembly
	if *startBlockAssembly {
		if _, found = gocore.Config().Get("blockassembly_grpcAddress"); found {
			g.Go(func() error {
				logger.Infof("Starting Block Assembly Server")

				baLogger := gocore.Log("bchn", gocore.NewLogLevelFromString(logLevel))
				blockAssemblyService = blockassembly.New(ctx, baLogger, subtreeStore)

				if err := blockAssemblyService.Start(); err != nil {
					logger.Errorf("blockassembly errored: %v", err)
					return err
				}

				return nil
			})
		}
	}

	// blockValidation
	if *startBlockValidation {
		if _, found = gocore.Config().Get("blockValidation_grpcAddress"); found {
			g.Go(func() error {
				logger.Infof("Starting Block Validation Server")

				bvLogger := gocore.Log("bval", gocore.NewLogLevelFromString(logLevel))
				blockValidationService, err := blockvalidation.New(bvLogger, utxoStore, subtreeStore)
				if err != nil {
					panic(err)
				}

				return blockValidationService.Start()
			})
		}
	}

	// validator
	if *startValidator {
		if validatorAddress, found := gocore.Config().Get("validator_grpcAddress"); found {
			g.Go(func() error {
				logger.Infof("Starting Validator Server on: %s", validatorAddress)

				validatorLogger := gocore.Log("valid", gocore.NewLogLevelFromString(logLevel))
				validatorService = validator.NewServer(validatorLogger, utxoStore)

				return validatorService.Start()
			})
		}
	}

	// utxostore
	if *startUtxoStore {
		utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
		if err != nil {
			panic(err)
		}

		if found {
			g.Go(func() (err error) {
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
				utxoStoreServer, err = utxo.New(utxoLogger, s)
				if err != nil {
					panic(err)
				}

				return utxoStoreServer.Start()
			})
		}
	}

	// seeder
	if *startSeeder {
		seederURL, found := gocore.Config().Get("seeder_grpcAddress")
		if found {
			g.Go(func() (err error) {
				logger.Infof("Starting Seeder on: %s", seederURL)

				seederService = seeder.NewServer(gocore.Log("seed", gocore.NewLogLevelFromString(logLevel)))

				return seederService.Start()
			})
		}
	}

	// miner
	if *startMiner {
		g.Go(func() (err error) {
			minerServer = miner.NewMiner()
			return minerServer.Start(ctx)
		})
	}

	// blob server
	if *startBlobServer {
		g.Go(func() (err error) {
			blobServer, err = blobserver.NewServer(utxoStore, txStore, subtreeStore)
			if err != nil {
				return err
			}

			if err := blobServer.Start(); err != nil {
				return err
			}

			return nil
		})
	}

	// propagation
	if *startPropagation {
		validatorClient, err := validator.NewClient(context.Background(), logger)
		if err != nil {
			logger.Fatalf("error creating validator client: %v", err)
		}

		// g.Go(func() error {
		// 	logger.Infof("Starting Propagation")

		// 	p2pLogger := gocore.Log("p2p", gocore.NewLogLevelFromString(logLevel))
		// 	propagationServer = propagation.NewServer(p2pLogger, txStore, subtreeStore, validatorClient)

		// 	return propagationServer.Start(ctx)
		// })

		propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
		if ok && propagationGrpcAddress != "" {
			g.Go(func() error {
				logger.Infof("Starting Propagation GRPC Server on: %s", propagationGrpcAddress)

				propagationGRPCServer, err = propagation.New(logger, txStore, validatorClient)
				if err != nil {
					panic(err)
				}

				return propagationGRPCServer.Start()
			})
		}
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	select {
	case <-interrupt:
		break
	case <-ctx.Done():
		logger.Errorf("context cancelled: %v", ctx.Err())
		break
	}

	logger.Infof("received shutdown signal")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if propagationServer != nil {
		propagationServer.Stop(shutdownCtx)
	}

	if propagationGRPCServer != nil {
		propagationGRPCServer.Stop(shutdownCtx)
	}

	if utxoStoreServer != nil {
		utxoStoreServer.Stop(shutdownCtx)
	}

	if validatorService != nil {
		validatorService.Stop(shutdownCtx)
	}

	if seederService != nil {
		seederService.Stop(shutdownCtx)
	}

	if blockAssemblyService != nil {
		blockAssemblyService.Stop(shutdownCtx)
	}

	if blockchainService != nil {
		blockchainService.Stop(shutdownCtx)
	}

	if minerServer != nil {
		minerServer.Stop(shutdownCtx)
	}

	if blobServer != nil {
		blobServer.Stop(shutdownCtx)
	}

	// wait for clean shutdown for 5 seconds, otherwise force exit
	go func() {
		// Wait for 5 seconds and then force exit...
		<-time.NewTimer(time.Second * 5).C
		os.Exit(3)
	}()

	if err = g.Wait(); err != nil {
		logger.Errorf("server returning an error: %v", err)
		os.Exit(2)
	}
}
