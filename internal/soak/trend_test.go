package soak

import (
	"context"
	"encoding/csv"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const mib = 1024 * 1024

// series builds n samples one second apart from per-index heap and goroutine functions.
func series(n int, heap func(i int) float64, goroutines func(i int) int) []Sample {
	out := make([]Sample, n)
	for i := range out {
		h := uint64(heap(i))
		out[i] = Sample{Elapsed: time.Duration(i) * time.Second, HeapInuse: h, HeapAlloc: h, NumGoroutine: goroutines(i)}
	}

	return out
}

func constGoroutines(int) int { return 300 }

func metric(t *testing.T, r Result, name string) MetricResult {
	t.Helper()

	for _, m := range r.Metrics {
		if m.Name == name {
			return m
		}
	}

	t.Fatalf("metric %s not in result", name)

	return MetricResult{}
}

func TestSlope(t *testing.T) {
	require.InDelta(t, 2.0, Slope([]float64{0, 1, 2, 3}, []float64{1, 3, 5, 7}), 1e-9)
	require.InDelta(t, 0.0, Slope([]float64{0, 1, 2, 3}, []float64{4, 4, 4, 4}), 1e-9)
	require.InDelta(t, 0.0, Slope([]float64{1, 1, 1}, []float64{1, 2, 3}), 1e-9, "no x spread must not divide by zero")
}

func TestAnalyzeFlatSeries(t *testing.T) {
	samples := series(100, func(int) float64 { return 200 * mib }, constGoroutines)

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, r.Flat(), r.String())
	require.Equal(t, 100, r.Samples)
}

func TestAnalyzeNoisyFlatSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic noise for a test

	samples := series(200,
		func(int) float64 { return 200*mib + (rng.Float64()-0.5)*20*mib },
		func(int) int { return 300 + rng.Intn(21) - 10 },
	)

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, r.Flat(), r.String())
}

func TestAnalyzeSawtoothIsFlat(t *testing.T) {
	// Heap climbs 30 MiB then drops back, the shape of a cache that fills and is flushed.
	samples := series(100, func(i int) float64 { return 200*mib + float64(i%10)*3*mib }, constGoroutines)

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, r.Flat(), r.String())
}

func TestAnalyzeLinearLeakFails(t *testing.T) {
	// 1 MiB and one goroutine per sample: 100 MiB and 100 goroutines over the run, well past the defaults.
	samples := series(100,
		func(i int) float64 { return 200*mib + float64(i)*mib },
		func(i int) int { return 300 + i },
	)

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.False(t, r.Flat())
	require.True(t, metric(t, r, "heap_inuse").Leaking, r.String())
	require.True(t, metric(t, r, "heap_alloc").Leaking, r.String())
	require.True(t, metric(t, r, "goroutines").Leaking, r.String())
}

func TestAnalyzeStepDuringWarmupIsFlat(t *testing.T) {
	// Caches filling at start-up: a large step inside the warm-up window, then a plateau.
	samples := series(120, func(i int) float64 {
		if i < 20 {
			return 50 * mib
		}

		return 250 * mib
	}, func(i int) int {
		if i < 20 {
			return 100
		}

		return 300
	})

	r, err := Analyze(samples, 20*time.Second, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, r.Flat(), r.String())

	// The same series analysed without a warm-up sees the step as growth.
	r, err = Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.False(t, r.Flat(), r.String())
}

func TestAnalyzeSingleSpikeIsFlat(t *testing.T) {
	// One large outlier on the final sample drags the least-squares slope past the tolerance, but the quarter means
	// barely move, so requiring both signals keeps a single spike from failing the run.
	samples := series(100, func(i int) float64 {
		if i == 99 {
			return 600 * mib
		}

		return 200 * mib
	}, constGoroutines)

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)

	heap := metric(t, r, "heap_inuse")
	require.Greater(t, heap.ProjectedGrowth, heap.Allowed, "the spike should push the slope alone past the tolerance")
	require.False(t, heap.Leaking, heap.String())
}

func TestAnalyzeToleranceUsesFirstQuarterBaseline(t *testing.T) {
	// A steady rise lifts the whole-window mean, so a tolerance taken from that mean would grow with the leak. With
	// a 50% relative tolerance this 85 MiB rise on a 100 MiB base slips under half the window mean but not under
	// half the first-quarter mean.
	tol := Tolerance{HeapRel: 0.5, GoroutineRel: 1, GoroutineAbs: 1e9}
	samples := series(100, func(i int) float64 { return 100*mib + float64(i)*85*mib/99 }, constGoroutines)

	r, err := Analyze(samples, 0, tol)
	require.NoError(t, err)

	heap := metric(t, r, "heap_inuse")
	require.Less(t, heap.QuarterDelta(), tol.HeapRel*heap.Mean, "a mean-based tolerance would have passed this leak")
	require.True(t, heap.Leaking, heap.String())
}

func TestAnalyzeMinDetectablePerHour(t *testing.T) {
	// 101 samples 36s apart: a one-hour window.
	hourly := func(ratePerHour float64) []Sample {
		out := make([]Sample, 101)
		for i := range out {
			h := uint64(200*mib + ratePerHour*float64(i)/100)
			out[i] = Sample{Elapsed: time.Duration(i) * 36 * time.Second, HeapInuse: h, HeapAlloc: h, NumGoroutine: 300}
		}

		return out
	}

	r, err := Analyze(hourly(0), 0, DefaultTolerance())
	require.NoError(t, err)

	floor := metric(t, r, "heap_inuse").MinDetectablePerHour
	require.InDelta(t, 20*mib/0.75, floor, mib, "10%% of 200 MiB over a one-hour window, divided by 0.75")

	r, err = Analyze(hourly(1.2*floor), 0, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, metric(t, r, "heap_inuse").Leaking, "a leak 20%% above the floor must fail: %s", r)

	r, err = Analyze(hourly(0.8*floor), 0, DefaultTolerance())
	require.NoError(t, err)
	require.False(t, metric(t, r, "heap_inuse").Leaking, "a leak 20%% below the floor must pass: %s", r)
}

func TestAnalyzeTooFewSamples(t *testing.T) {
	samples := series(20, func(int) float64 { return 200 * mib }, constGoroutines)

	_, err := Analyze(samples, 15*time.Second, DefaultTolerance())
	require.Error(t, err)
}

// leaked holds the deliberate leak for TestSamplerDetectsInProcessLeak.
var leaked [][]byte

// TestSamplerDetectsInProcessLeak drives a real per-iteration leak (a slice appended to a package-level variable and
// a goroutine that never exits) under the real sampler and requires the analysis to flag both.
func TestSamplerDetectsInProcessLeak(t *testing.T) {
	release := make(chan struct{})

	t.Cleanup(func() {
		close(release)

		leaked = nil
	})

	sampler := NewSampler(25 * time.Millisecond)
	sampler.Start(context.Background())

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		leaked = append(leaked, make([]byte, mib))

		go func() { <-release }()

		time.Sleep(10 * time.Millisecond)
	}

	samples := sampler.Stop()

	r, err := Analyze(samples, 0, DefaultTolerance())
	require.NoError(t, err)
	require.True(t, metric(t, r, "heap_inuse").Leaking, r.String())
	require.True(t, metric(t, r, "goroutines").Leaking, r.String())
}

func TestSamplerStopIsIdempotent(t *testing.T) {
	sampler := NewSampler(time.Hour)
	sampler.Start(context.Background())

	first := sampler.Stop()
	require.Len(t, first, 1, "Start takes an initial sample")
	require.Equal(t, first, sampler.Stop())
}

func TestWriteCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "soak.csv")
	samples := []Sample{
		{Elapsed: 0, HeapInuse: 10, HeapAlloc: 8, NumGoroutine: 3},
		{Elapsed: 1500 * time.Millisecond, HeapInuse: 11, HeapAlloc: 9, NumGoroutine: 4},
	}

	require.NoError(t, WriteCSV(path, samples))

	f, err := os.Open(path)
	require.NoError(t, err)

	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"elapsed_seconds", "heap_inuse_bytes", "heap_alloc_bytes", "goroutines"},
		{"0.000", "10", "8", "3"},
		{"1.500", "11", "9", "4"},
	}, rows)
}
