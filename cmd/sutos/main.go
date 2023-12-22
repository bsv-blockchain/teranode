package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "sutos"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func init() {
	gocore.SetInfo(progname, version, commit)
}

func main() {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := ulogger.New(progname, ulogger.WithLevel(logLevel))

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	profilerAddr, found := gocore.Config().Get("profilerAddr")
	if found {
		go func() {
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}()
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

	go func() {
		<-signalChan

		appCleanup(logger, nil)
		os.Exit(1)
	}()

	start()
}

func appCleanup(logger ulogger.Logger, shutdownFns []func()) {
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

func start() {

}
