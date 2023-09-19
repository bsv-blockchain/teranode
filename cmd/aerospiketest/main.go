package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/direct"
	"github.com/bitcoin-sv/ubsv/cmd/aerospiketest/ubsv"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	logger       = gocore.Log("test")
	workers      int
	transactions int
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
)

func main() {

	flag.IntVar(&transactions, "transactions", 100, "number of transactions to process")
	flag.IntVar(&workers, "workers", 10, "number of workers")
	flag.BoolVar(&aslLogger, "asl_logger", false, "enable aerospike logger")
	flag.StringVar(&strategyStr, "strategy", "direct1", "strategy to use [ubsv, direct]")
	flag.Parse()

	if aslLogger {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	var strategy Strategy

	switch strategyStr {
	case "direct":
		strategy = direct.New(logger, transactions/workers)
	case "ubsv":
		strategy = ubsv.New(logger, transactions/workers)
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

		wait()

		os.Exit(0)
	}()

	counterWorker("storer", wgCounters, storerCounterCh)
	counterWorker("spender", wgCounters, spenderCounterCh)
	counterWorker("deleter", wgCounters, deleterCounterCh)

	for i := 0; i < workers; i++ {
		strategy.Deleter(ctx, wgDeleters, deleterCh, deleterCounterCh)
		strategy.Spender(ctx, wgSpenders, spenderCh, deleterCh, spenderCounterCh)
		strategy.Storer(ctx, i, wgStorers, spenderCh, storerCounterCh)
	}

	wait()

	logger.Infof("Finished.")
}

func counterWorker(name string, wg *sync.WaitGroup, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		for count := range counterCh {
			counter += count
		}

		logger.Infof("%s count: %d", name, counter)
	}()
}

func wait() {
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
}
