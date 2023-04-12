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
	"github.com/TAAL-GmbH/ubsv/services/utxostore"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/tracing"
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
	var utxoStore *utxostore.UTXOStore
	var propagationServer *propagation.Server

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

				utxoStore, err = utxostore.New(gocore.Log("utxo", gocore.NewLogLevelFromString(logLevel)))
				if err != nil {
					panic(err)
				}

				return utxoStore.Start()
			})
		}
	}

	// propagation
	if *startPropagation {
		g.Go(func() error {
			logger.Infof("Starting Propagation")

			propagationServer = propagation.NewServer(gocore.Log("p2p", gocore.NewLogLevelFromString(logLevel)))

			return propagationServer.Start(ctx)
		})
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
