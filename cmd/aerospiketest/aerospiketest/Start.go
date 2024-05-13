package aerospiketest

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/direct"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/nothing"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/simple"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/ubsv"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/ubsv_map"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type Strategy interface {
	Storer(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, spenderCh chan *bt.Tx, counterCh chan int)
	Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *bt.Tx, deleterCh chan *bt.Tx, counterCh chan int)
	Deleter(ctx context.Context, wg *sync.WaitGroup, deleteCh chan *bt.Tx, counterCh chan int)
}

var (
	logger             = ulogger.New("test")
	workers            int
	transactions       int
	repeat             int
	timeoutStr         string
	aslLogger          bool
	strategyStr        string
	aerospikeHost      string
	aerospikePort      int
	aerospikeNamespace string

	bufferSize int

	wgStorers        *sync.WaitGroup
	wgSpenders       *sync.WaitGroup
	wgDeleters       *sync.WaitGroup
	wgCounters       *sync.WaitGroup
	spenderCh        chan *bt.Tx
	deleterCh        chan *bt.Tx
	storerCounterCh  chan int
	spenderCounterCh chan int
	deleterCounterCh chan int
)

func Start() {

	flag.IntVar(&transactions, "transactions", 100, "number of transactions to process")
	flag.IntVar(&workers, "workers", 10, "number of workers")
	flag.IntVar(&repeat, "repeat", 1, "number of time to repeat the test [1]")
	flag.StringVar(&timeoutStr, "timeout", "", "timeout for aerospike")
	flag.BoolVar(&aslLogger, "asl_logger", false, "enable aerospike logger")
	flag.StringVar(&strategyStr, "strategy", "direct", "strategy to use [ubsv, ubsv_map, direct, simple, nothing]")
	flag.StringVar(&aerospikeHost, "aerospike_host", "", "aerospike host")
	flag.IntVar(&aerospikePort, "aerospike_port", 3000, "aerospike port")
	flag.StringVar(&aerospikeNamespace, "aerospike_namespace", "utxostore", "aerospike namespace [utxostore]")
	flag.IntVar(&bufferSize, "buffer_size", 1000, "buffer size")

	flag.Parse()

	if aerospikeHost == "" && strategyStr != "nothing" {
		logger.Fatalf("aerospike_host is required")
	}

	if aslLogger {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	server := &http.Server{
		Addr:         "localhost:6060",
		Handler:      nil,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Infof("Starting pprof on http://localhost:6060/debug/pprof")
		logger.Infof("%v", server.ListenAndServe())
	}()

	var strategy Strategy

	split := transactions / workers
	remainder := transactions % workers

	switch strategyStr {
	case "direct":
		strategy = direct.New(logger, timeoutStr, aerospikeHost, aerospikePort, aerospikeNamespace)
	case "simple":
		strategy = simple.New(logger, timeoutStr, aerospikeHost, aerospikePort, aerospikeNamespace)
	case "nothing":
		strategy = nothing.New(logger)
	case "ubsv":
		strategy = ubsv.New(logger, timeoutStr, aerospikeHost, aerospikePort, aerospikeNamespace)
	case "ubsv_map":
		strategy = ubsv_map.New(logger, timeoutStr, aerospikeHost, aerospikePort, aerospikeNamespace)
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

		closeChannels()

		os.Exit(1)
	}()

	for i := 0; i < repeat; i++ {
		runTest(ctx, split, remainder, strategy)
	}

	logger.Infof("Finished.")
	// os.Exit(0)
}

func runTest(ctx context.Context, split int, remainder int, strategy Strategy) {
	start := time.Now()

	createChannels()

	counterWorker("Stored ", wgCounters, storerCounterCh)
	counterWorker("Spent  ", wgCounters, spenderCounterCh)
	counterWorker("Deleted", wgCounters, deleterCounterCh)

	for i := 0; i < workers; i++ {
		txCount := split
		if i == 0 {
			txCount += remainder
		}
		strategy.Deleter(ctx, wgDeleters, deleterCh, deleterCounterCh)
		strategy.Spender(ctx, wgSpenders, spenderCh, deleterCh, spenderCounterCh)
		strategy.Storer(ctx, i, txCount, wgStorers, spenderCh, storerCounterCh)
	}

	closeChannels()

	// wait for everything to finish
	wgCounters.Wait()

	elapsed := time.Since(start)
	logger.Infof("Took %s", elapsed)
}

func createChannels() {
	wgStorers = &sync.WaitGroup{}
	wgSpenders = &sync.WaitGroup{}
	wgDeleters = &sync.WaitGroup{}
	wgCounters = &sync.WaitGroup{}
	spenderCh = make(chan *bt.Tx, bufferSize)
	deleterCh = make(chan *bt.Tx, bufferSize)
	storerCounterCh = make(chan int)
	spenderCounterCh = make(chan int)
	deleterCounterCh = make(chan int)
}

func closeChannels() {
	// Wait for the storers to finish
	wgStorers.Wait()

	// Close the spender channel
	logger.Infof("Closing spender channel")
	close(spenderCh)

	wgSpenders.Wait()

	// Close the deleter channel
	logger.Infof("Closing deleter channel")
	close(deleterCh)

	// Wait for the spender to finish
	wgDeleters.Wait()

	// Close the counters
	close(storerCounterCh)
	close(spenderCounterCh)
	close(deleterCounterCh)

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
