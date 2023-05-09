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

	"github.com/TAAL-GmbH/ubsv/services/blaster"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/services/propagation"
	"github.com/TAAL-GmbH/ubsv/services/seeder"
	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/validator"
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

	startBlockAssembly := flag.Bool("blockassembly", false, "start blockassembly service")
	startValidator := flag.Bool("validator", false, "start validator service")
	startUtxoStore := flag.Bool("utxostore", false, "start UTXO store")
	startPropagation := flag.Bool("propagation", false, "start propagation service")
	startSeeder := flag.Bool("seeder", false, "start seeder service")
	profilePort := flag.String("profile", "", "use this profile port instead of the default")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if !*startBlockAssembly {
		*startBlockAssembly = gocore.Config().GetBool("startBlockAssembly", false)
	}

	if !*startValidator {
		*startValidator = gocore.Config().GetBool("startValidator", false)
	}

	if !*startUtxoStore {
		*startUtxoStore = gocore.Config().GetBool("startUtxoStore", false)
	}

	if !*startPropagation {
		*startPropagation = gocore.Config().GetBool("startPropagation", false)
	}

	if !*startSeeder {
		*startSeeder = gocore.Config().GetBool("startSeeder", false)
	}

	if help != nil && *help || (!*startValidator && !*startUtxoStore && !*startPropagation && !*startBlockAssembly && !*startSeeder) {
		fmt.Println("usage: main [options]")
		fmt.Println("where options are:")
		fmt.Println("")
		fmt.Println("    -validator=<1|0>")
		fmt.Println("          whether to start the validator service")
		fmt.Println("")
		fmt.Println("    -propagation=<1|0>")
		fmt.Println("          whether to start the propagation service")
		fmt.Println("")
		fmt.Println("    -utxostore=<1|0>")
		fmt.Println("          whether to start the utxo store service")
		fmt.Println("")
		fmt.Println("    -blockassembly=<1|0>")
		fmt.Println("          whether to start the blockassembly service")
		fmt.Println("")
		fmt.Println("    -seeder=<1|0>")
		fmt.Println("          whether to start the seeder service")
		fmt.Println("")
		fmt.Println("    -tracer=<1|0>")
		fmt.Println("          whether to start the Jaeger tracer (default=false)")
		fmt.Println("")
		return
	}

	go func() {
		var profilerAddr string
		var ok bool
		if profilePort != nil && *profilePort != "" {
			profilerAddr, ok = ":"+*profilePort, true
		} else {
			profilerAddr, ok = gocore.Config().Get("profilerAddr")
		}
		if ok {
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
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

	var validatorService *validator.Server
	var utxoStore *utxo.UTXOStore
	var propagationServer *propagation.Server
	var propagationGRPCServer *propagation.PropagationServer
	var blockAssemblyService *blockassembly.BlockAssembly
	var seederService *seeder.Server

	// blockAssembly
	if *startBlockAssembly {
		if _, found := gocore.Config().Get("blockAssembly_grpcAddress"); found {
			g.Go(func() error {
				logger.Infof("Starting Server")

				blockAssemblyService = blockassembly.New(gocore.Log("block", gocore.NewLogLevelFromString(logLevel)))

				return blockAssemblyService.Start()
			})
		}
	}

	// validator
	if *startValidator {
		if _, found := gocore.Config().Get("validator_grpcAddress"); found {
			g.Go(func() error {
				logger.Infof("Starting Server")

				validatorService = validator.NewServer(gocore.Log("valid", gocore.NewLogLevelFromString(logLevel)))

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

				utxoStore, err = utxo.New(gocore.Log("utxo", gocore.NewLogLevelFromString(logLevel)))
				if err != nil {
					panic(err)
				}

				return utxoStore.Start()
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

	if blaster.Enabled() {
		b := blaster.New()

		if err := b.Start(); err != nil {
			panic(err)
		}
	}

	// propagation
	if *startPropagation {
		txStoreUrl, err, found := gocore.Config().GetURL("txstore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("txstore config not found")
		}
		txStore, err := propagation.NewStore(txStoreUrl)
		if err != nil {
			panic(err)
		}

		blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("blockstore config not found")
		}
		blockStore, err := propagation.NewStore(blockStoreUrl)
		if err != nil {
			panic(err)
		}

		validatorClient, err := validator.NewClient(context.Background())
		if err != nil {
			logger.Fatalf("error creating validator client: %v", err)
		}

		g.Go(func() error {
			logger.Infof("Starting Propagation")

			p2pLogger := gocore.Log("p2p", gocore.NewLogLevelFromString(logLevel))
			propagationServer = propagation.NewServer(p2pLogger, txStore, blockStore, validatorClient)

			return propagationServer.Start(ctx)
		})

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

	select {
	case <-interrupt:
		break
	case <-ctx.Done():
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

	if utxoStore != nil {
		utxoStore.Stop(shutdownCtx)
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

	go func() {
		// Wait for 5 seconds and then force exit...
		<-time.NewTimer(time.Second * 5).C
		os.Exit(3)
	}()

	if err := g.Wait(); err != nil {
		logger.Errorf("server returning an error: %v", err)
		os.Exit(2)
	}

}
