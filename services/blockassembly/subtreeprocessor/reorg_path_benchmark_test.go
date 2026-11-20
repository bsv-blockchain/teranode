package subtreeprocessor

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// BenchmarkReorgPath times a fork reorg of one block back and two forward through reorgBlocks, the
// production call site, on the scenario shape of issue 2269: the lost block about twice the pending
// set and almost entirely in the winners, the winners taking the oldest pending transactions. It is
// the baseline every reorg change of that issue is measured against.
//
// The name matches the subtreeprocessor-unit filter of the benchmark-compare workflow, which runs
// with -short: under -short only the 256K pending set runs, so the job stays inside its budget. The
// larger sets run without -short. The 125M and larger sizes are the reorgscale suite.
//
// Each iteration builds a fresh scenario outside the timer, because a reorg consumes its input.
func BenchmarkReorgPath(b *testing.B) {
	sizes := []int{256 << 10}
	if !testing.Short() {
		sizes = append(sizes, 1<<20)
	}

	for _, pending := range sizes {
		for _, hits := range []float64{0.3, 0.9} {
			for _, disk := range []bool{false, true} {
				cfg := reorgPathBenchConfig(pending, hits, disk)
				name := fmt.Sprintf("pending=%dK/hits=%.0f%%/map=%s", pending>>10, hits*100, map[bool]string{false: "memory", true: "disk"}[disk])

				b.Run(name, func(b *testing.B) {
					benchmarkReorgPath(b, cfg, slowReorgPath)
				})
			}
		}
	}
}

func reorgPathBenchConfig(pending int, hits float64, disk bool) reorgScenarioConfig {
	return reorgScenarioConfig{
		seed:             uint64(pending) ^ uint64(hits*100),
		pending:          pending,
		lost:             2 * pending,
		lostInWinners:    0.995,
		pendingInWinners: hits,
		hitsAtFront:      true,
		winnerOnly:       pending / 1000,
		forwardBlocks:    2,
		subtreeSize:      1 << 14,
		diskTxMap:        disk,
	}
}

func benchmarkReorgPath(b *testing.B, cfg reorgScenarioConfig, path reorgPath) {
	b.Helper()
	b.ReportAllocs()

	var total time.Duration

	phases := make(map[string]float64)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sc := newReorgScenario(b, cfg)
		before := reorgPhaseSeconds("reorgBlocks")
		b.StartTimer()

		start := time.Now()
		err := path(b, sc)
		total += time.Since(start)

		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}

		for phase, seconds := range reorgPhaseSeconds("reorgBlocks") {
			phases[phase] += seconds - before[phase]
		}
	}

	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(cfg.pending), "ns/pending_tx")

	for _, phase := range slices.Sorted(maps.Keys(phases)) {
		b.ReportMetric(phases[phase]*1000/float64(b.N), phase+"_ms")
	}
}

// reorgPhaseSeconds is the total time reorg_phase_seconds has recorded for each phase of op.
func reorgPhaseSeconds(op string) map[string]float64 {
	initPrometheusMetrics()

	ch := make(chan prometheus.Metric, 64)

	go func() {
		prometheusSubtreeProcessorReorgPhase.Collect(ch)
		close(ch)
	}()

	out := make(map[string]float64)

	for metric := range ch {
		m := &dto.Metric{}
		if err := metric.Write(m); err != nil {
			continue
		}

		var labelOp, phase string

		for _, l := range m.GetLabel() {
			switch l.GetName() {
			case "op":
				labelOp = l.GetValue()
			case "phase":
				phase = l.GetValue()
			}
		}

		if labelOp == op {
			out[phase] = m.GetHistogram().GetSampleSum()
		}
	}

	return out
}
