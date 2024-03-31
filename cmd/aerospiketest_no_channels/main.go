package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest_no_channels/direct"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest_no_channels/nothing"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest_no_channels/simple"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest_no_channels/ubsv"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

type Strategy interface {
	Work(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, counterCh chan int)
}

var (
	logger        = ulogger.New("test")
	workers       int
	transactions  int
	timeoutStr    string
	aslLogger     bool
	strategyStr   string
	aerospikeHost string
	aerospikePort int
	wgStorers     = &sync.WaitGroup{}
	wgCounters    = &sync.WaitGroup{}
	workCounterCh = make(chan int)
	shutdownOnce  sync.Once
)

func main() {
	flag.IntVar(&transactions, "transactions", 100, "number of transactions to process")
	flag.IntVar(&workers, "workers", 10, "number of workers")
	flag.StringVar(&timeoutStr, "timeout", "", "timeout for aerospike")
	flag.BoolVar(&aslLogger, "asl_logger", false, "enable aerospike logger")
	flag.StringVar(&strategyStr, "strategy", "direct", "strategy to use [ubsv, direct, simple, nothing]")
	flag.StringVar(&aerospikeHost, "aerospike_host", "", "aerospike host")
	flag.IntVar(&aerospikePort, "aerospike_port", 3000, "aerospike port")

	flag.Parse()

	if aerospikeHost == "" && strategyStr != "nothing" {
		logger.Fatalf("aerospike_host is required")
	}

	if aslLogger {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	go func() {
		logger.Infof("Starting pprof on http://localhost:6060/debug/pprof")
		logger.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()

	var strategy Strategy

	split := transactions / workers
	remainder := transactions % workers

	switch strategyStr {
	case "direct":
		strategy = direct.New(logger, timeoutStr, aerospikeHost, aerospikePort)
	case "simple":
		strategy = simple.New(logger, timeoutStr, aerospikeHost, aerospikePort)
	case "nothing":
		strategy = nothing.New(logger)
	case "ubsv":
		strategy = ubsv.New(logger, timeoutStr, aerospikeHost, aerospikePort)
	default:
		logger.Fatalf("unknown strategy")
	}

	logger.Infof("Using %s strategy", strategyStr)
	logger.Infof("Using %d workers", workers)
	logger.Infof("Using %d transactions", transactions)
	if timeoutStr != "" {
		logger.Infof("Using timeout %s", timeoutStr)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		logger.Infof("Received signal, stopping...")
		cancelFunc() // cancel the contexts

		shutdown()

		os.Exit(1)
	}()

	counterWorker("Work  ", wgCounters, workCounterCh)

	for i := 0; i < workers; i++ {
		txCount := split
		if i == 0 {
			txCount += remainder
		}
		strategy.Work(ctx, i, txCount, wgStorers, workCounterCh)
	}

	shutdown()

	logger.Infof("Finished.")
	// os.Exit(0)
}

func shutdown() {
	shutdownOnce.Do(func() {
		// Wait for the storers to finish
		wgStorers.Wait()

		// Close the counters
		close(workCounterCh)

		// Close the finished channel
		wgCounters.Wait()
	})
}

func counterWorker(name string, wg *sync.WaitGroup, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		for count := range counterCh {
			counter += count
		}

		// Log the final count with the number formatted with comma separators

		logger.Infof("%s: %s", name, commaSeparatedInt(counter))

	}()
}

func commaSeparatedInt(iVal int) string {
	// Convert int to string
	s := strconv.Itoa(iVal)

	// Format the string with comma separators
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}

	return s
}
