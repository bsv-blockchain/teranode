package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/TAAL-GmbH/ubs/tracing"
	"github.com/TAAL-GmbH/ubs/validator"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "ubs"

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

	tracingOn := gocore.Config().GetBool("tracing")
	if tracingOn {
		logger.Infof("Starting tracer")
		// Start the tracer
		tracer, closer := tracing.InitTracer(logger, progname)
		defer closer.Close()

		if tracer != nil {
			// set the global tracer to use in all services
			opentracing.SetGlobalTracer(tracer)
		}
	}

	shutdownFns := make([]func(), 0)

	if v, _ := gocore.Config().Get("validator_grpcAddress"); v == "" {
		logger.Infof("Starting Validator")

		var validatorLogger = gocore.Log("btx", gocore.NewLogLevelFromString(logLevel))

		validator := validator.NewServer(validatorLogger)

		if err := validator.StartGRPCServer(); err != nil {
			validatorLogger.Errorf("StartGRPCServer error: %v", err)
		}
	}

	statisticsServerAddr, found := gocore.Config().Get("statisticsServerAddress")
	if found {
		go func() {
			gocore.StartStatsServer(statisticsServerAddr)
		}()
	}

	// setup signal catching
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	<-signalChan
	appCleanup(logger, shutdownFns)
	os.Exit(1)
}

func appCleanup(logger utils.Logger, shutdownFns []func()) {
	logger.Infof("Shutting down...")

	var wg sync.WaitGroup
	for _, fn := range shutdownFns {
		// fire the shutdown functions off in the background
		// they might be relying on each other, and this allows them to gracefully stop
		wg.Add(1)
		go func(fn func()) {
			defer wg.Done()
			fn()
		}(fn)
	}
	wg.Wait()
}
