package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/direct"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/ubsv"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Strategy interface {
	Storer(ctx context.Context, id int, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, counterCh chan int)
	Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, deleterCh chan *chainhash.Hash, counterCh chan int)
	Deleter(ctx context.Context, wg *sync.WaitGroup, deleteCh chan *chainhash.Hash, counterCh chan int)
}

var (
	logger       = gocore.Log("test")
	workers      int
	transactions int
	timeoutStr   string
	aslLogger    bool
	strategyStr  string

	wgStorers        = &sync.WaitGroup{}
	wgSpenders       = &sync.WaitGroup{}
	wgDeleters       = &sync.WaitGroup{}
	wgCounters       = &sync.WaitGroup{}
	spenderCh        = make(chan *chainhash.Hash, 1000)
	deleterCh        = make(chan *chainhash.Hash, 1000)
	storerCounterCh  = make(chan int)
	spenderCounterCh = make(chan int)
	deleterCounterCh = make(chan int)
	shutdownOnce     sync.Once
)

func main() {

	flag.IntVar(&transactions, "transactions", 100, "number of transactions to process")
	flag.IntVar(&workers, "workers", 10, "number of workers")
	flag.StringVar(&timeoutStr, "timeout", "", "timeout for aerospike")
	flag.BoolVar(&aslLogger, "asl_logger", false, "enable aerospike logger")
	flag.StringVar(&strategyStr, "strategy", "direct", "strategy to use [ubsv, direct]")

	flag.Parse()

	if aslLogger {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	var strategy Strategy

	switch strategyStr {
	case "direct":
		strategy = direct.New(logger, transactions/workers, timeoutStr)
	case "ubsv":
		strategy = ubsv.New(logger, transactions/workers, timeoutStr)
	default:
		logger.Fatalf("unknown strategy")
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

	counterWorker("Stored ", wgCounters, storerCounterCh)
	counterWorker("Spent  ", wgCounters, spenderCounterCh)
	counterWorker("Deleted", wgCounters, deleterCounterCh)

	for i := 0; i < workers; i++ {
		strategy.Deleter(ctx, wgDeleters, deleterCh, deleterCounterCh)
		strategy.Spender(ctx, wgSpenders, spenderCh, deleterCh, spenderCounterCh)
		strategy.Storer(ctx, i, wgStorers, spenderCh, storerCounterCh)
	}

	shutdown()

	logger.Infof("Finished.")
	// os.Exit(0)
}

func shutdown() {
	shutdownOnce.Do(func() {
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

func commaSeparatedInt(i int) string {
	// Convert int to string
	s := strconv.Itoa(i)

	// Format the string with comma separators
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}

	return s
}
