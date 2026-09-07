// validator-loadtest drives sustained ProcessTransaction load against a
// real (testcontainers) or remote Aerospike instance. Measures sustained
// TPS + latency percentiles rather than batch wall time. Used to find
// the actual per-pod throughput ceiling and what binds it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		aerospikeURL       = flag.String("aerospike-url", "", "Aerospike URL (default: spawn testcontainer)")
		submitters         = flag.Int("submitters", 1024, "concurrent submitter goroutines")
		duration           = flag.Duration("duration", 60*time.Second, "total run length")
		targetTPS          = flag.Int("target-tps", 0, "cap submission rate; 0 = unbounded")
		warmUp             = flag.Duration("warm-up", 5*time.Second, "discarded window before metrics start")
		validateBatch      = flag.Bool("validate-batch", true, "turn on UseBatchValidation")
		batchMaxSize       = flag.Int("batch-max-size", 1024, "coalescer batch max size")
		batchMaxWait       = flag.Duration("batch-max-wait", 5*time.Millisecond, "coalescer batch max wait")
		batchMaxConcurrent = flag.Int("batch-max-concurrent", 64, "coalescer concurrent flushes")
		connQueueSize      = flag.Int("conn-queue-size", 128, "Aerospike client connection pool size")
		parentPoolSize     = flag.Int("parent-pool-size", 16384, "pre-seeded parent UTXOs")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("loadtest: spinning up fixture (this can take ~5s if using testcontainer)")
	fix := newFixture(ctx, fixtureConfig{
		aerospikeURL:       *aerospikeURL,
		useBatch:           *validateBatch,
		batchMaxSize:       *batchMaxSize,
		batchMaxWait:       *batchMaxWait,
		batchMaxConcurrent: *batchMaxConcurrent,
		connQueueSize:      *connQueueSize,
		parentPoolSize:     *parentPoolSize,
	})
	defer fix.cleanup()
	log.Printf("loadtest: fixture ready; seeded %d parents", len(fix.parents))

	h := newHarness(fix, harnessConfig{
		submitters: *submitters,
		duration:   *duration,
		warmUp:     *warmUp,
		targetTPS:  *targetTPS,
	})

	tel := startTelemetry(ctx, fix.validator, fix.containerName)
	defer tel.close()

	log.Printf("loadtest: starting %s warm-up + %s run with %d submitters (validate-batch=%v)",
		*warmUp, *duration, *submitters, *validateBatch)
	h.run(ctx)
	printSummary(h.summary(), *submitters, *validateBatch, *duration, *parentPoolSize)
	fmt.Println(tel.summary().format())
}

func printSummary(s summary, submitters int, useBatch bool, duration time.Duration, parentPoolSize int) {
	fmt.Println()
	fmt.Printf("Sustained TPS:     %.0f (after warm-up)\n", s.TPS)
	fmt.Printf("Total submitted:   %d / acked: %d / errors: %d\n", s.Submitted, s.Acked, s.Errored)
	fmt.Printf("Latency p50/p95/p99: %s / %s / %s\n", s.P50, s.P95, s.P99)
	fmt.Printf("Run: %s @ %d submitters, validate-batch=%v\n", s.Duration, submitters, useBatch)
	if s.Exhausted && s.Duration < duration {
		fmt.Printf("WARN: parent pool exhausted before duration elapsed; consider --parent-pool-size > %d for accurate sustained-TPS measurements\n",
			parentPoolSize)
	}
}
