package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
)

// harness drives N concurrent submitters for a fixed duration against
// a fixture, recording counts and latencies.
type harness struct {
	fix        *fixture
	cfg        harnessConfig
	latencies  *latencyHistogram
	submitted  atomic.Int64
	acked      atomic.Int64
	errored    atomic.Int64
	timerStart time.Time
	// parentIdx is an atomic counter consumed by submitters to claim unique
	// parent indices. Each parent is used by exactly one submitter call.
	parentIdx atomic.Int64
}

type harnessConfig struct {
	submitters int
	duration   time.Duration
	warmUp     time.Duration
	targetTPS  int // 0 = unbounded
}

func newHarness(fix *fixture, cfg harnessConfig) *harness {
	return &harness{
		fix:       fix,
		cfg:       cfg,
		latencies: newLatencyHistogram(),
	}
}

// run launches submitter goroutines and returns when warm-up + duration
// elapses or ctx cancels.
func (h *harness) run(ctx context.Context) {
	runCtx, runCancel := context.WithTimeout(ctx, h.cfg.warmUp+h.cfg.duration)
	defer runCancel()

	var afterWarmUp atomic.Bool
	go func() {
		select {
		case <-time.After(h.cfg.warmUp):
			h.timerStart = time.Now()
			afterWarmUp.Store(true)
		case <-runCtx.Done():
		}
	}()

	var rateLimiter <-chan time.Time
	if h.cfg.targetTPS > 0 {
		interval := time.Second / time.Duration(h.cfg.targetTPS)
		t := time.NewTicker(interval)
		defer t.Stop()
		rateLimiter = t.C
	}

	var wg sync.WaitGroup
	for i := 0; i < h.cfg.submitters; i++ {
		wg.Add(1)
		go func(submitterID int) {
			defer wg.Done()
			h.submitterLoop(runCtx, submitterID, &afterWarmUp, rateLimiter)
		}(i)
	}
	wg.Wait()
}

func (h *harness) submitterLoop(ctx context.Context, id int, afterWarmUp *atomic.Bool, rateLimiter <-chan time.Time) {
	parentLen := int64(len(h.fix.parents))
	if parentLen == 0 {
		log.Fatalf("submitter %d: empty parent pool", id)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if rateLimiter != nil {
			select {
			case <-rateLimiter:
			case <-ctx.Done():
				return
			}
		}

		// Claim a unique parent index. When the pool is exhausted,
		// this submitter is done.
		idx := h.parentIdx.Add(1) - 1
		if idx >= parentLen {
			return
		}
		parent := h.fix.parents[idx]
		child := buildChildSpending(parent)

		// Use a per-request context with a generous per-call timeout rather
		// than passing the measurement-window ctx (runCtx) directly. runCtx
		// has a deadline shared by all goroutines: when it fires every
		// in-flight call returns context.DeadlineExceeded immediately,
		// producing the 99% error rate seen with --validate-batch=true.
		// The loop-exit select above already gates when we stop submitting;
		// the per-call context only limits how long one call can block.
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		_, err := h.fix.ps.ProcessTransaction(reqCtx, &propagation_api.ProcessTransactionRequest{Tx: child.Bytes()})
		reqCancel()
		elapsed := time.Since(start)

		if afterWarmUp.Load() {
			h.submitted.Add(1)
			if err != nil {
				h.errored.Add(1)
			} else {
				h.acked.Add(1)
			}
			h.latencies.record(elapsed)
		}
	}
}

// summary captures end-of-run numbers for printing / JSON.
type summary struct {
	Submitted int64
	Acked     int64
	Errored   int64
	Duration  time.Duration
	TPS       float64
	P50       time.Duration
	P95       time.Duration
	P99       time.Duration
	// Exhausted is true when the parent pool ran out before the run duration
	// elapsed — results may understate sustained TPS.
	Exhausted bool
}

func (h *harness) summary() summary {
	dur := time.Since(h.timerStart)
	tps := 0.0
	if dur > 0 {
		tps = float64(h.submitted.Load()) / dur.Seconds()
	}
	return summary{
		Submitted: h.submitted.Load(),
		Acked:     h.acked.Load(),
		Errored:   h.errored.Load(),
		Duration:  dur,
		TPS:       tps,
		P50:       h.latencies.percentile(50),
		P95:       h.latencies.percentile(95),
		P99:       h.latencies.percentile(99),
		Exhausted: h.parentIdx.Load() >= int64(len(h.fix.parents)),
	}
}
