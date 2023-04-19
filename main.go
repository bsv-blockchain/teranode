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

	"github.com/TAAL-GmbH/ubsv/services/propagation"
	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/getsentry/sentry-go"
	"github.com/opentracing/opentracing-go"
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

	err := sentry.Init(sentry.ClientOptions{
		Dsn: "https://dcad1ec4c60a4a2e80a7f8599e86ec4b@o4505013263466496.ingest.sentry.io/4505013264449536",
		// Set TracesSampleRate to 1.0 to capture 100% of transactions for performance monitoring.
		// We recommend adjusting this value in production,
		TracesSampleRate: 1.0,
	})
	if err != nil {
		logger.Fatalf("sentry.Init: %s", err)
	}

	startValidator := flag.Bool("validator", false, "start validator service")
	startUtxoStore := flag.Bool("utxostore", false, "start UTXO store")
	startPropagation := flag.Bool("propagation", false, "start propagation service")
	useTracer := flag.Bool("tracer", false, "start tracer")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if help != nil && *help || (!*startValidator && !*startUtxoStore && !*startPropagation) {
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
		fmt.Println("    -tracer=<1|0>")
		fmt.Println("          whether to start the Jaeger tracer (default=false)")
		fmt.Println("")
		return
	}

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	go func() {
		profilerAddr, ok := gocore.Config().Get("profilerAddr")
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
	if *useTracer {
		logger.Infof("Starting tracer")
		// Start the tracer
		tracer, closer := tracing.InitTracer(logger, progname)
		defer closer.Close()

		if tracer != nil {
			// set the global tracer to use in all services
			opentracing.SetGlobalTracer(tracer)
		}
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
				logger.Infof("Starting PropagationServer on: %s", utxostoreURL.Host)

				utxoStore, err = utxo.New(gocore.Log("utxo", gocore.NewLogLevelFromString(logLevel)))
				if err != nil {
					panic(err)
				}

				return utxoStore.Start()
			})
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

		validatorClient, err := validator.NewClient()
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

	if err := g.Wait(); err != nil {
		logger.Errorf("server returning an error: %v", err)
		os.Exit(2)
	}

}
