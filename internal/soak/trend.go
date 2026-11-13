package soak

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// MinAnalysisSamples is the fewest post-warm-up samples Analyze accepts; fewer cannot distinguish a trend from noise.
const MinAnalysisSamples = 8

// Tolerance bounds how much a metric may grow across the analysis window before it counts as leaking. For each
// metric the allowed growth is max(Rel * post-warm-up mean, Abs).
type Tolerance struct {
	HeapRel      float64
	HeapAbs      float64 // bytes
	GoroutineRel float64
	GoroutineAbs float64
}

// DefaultTolerance allows heap growth of 10% or 16 MiB, and goroutine growth of 5% or 20, whichever is larger.
func DefaultTolerance() Tolerance {
	return Tolerance{
		HeapRel:      0.10,
		HeapAbs:      16 * 1024 * 1024,
		GoroutineRel: 0.05,
		GoroutineAbs: 20,
	}
}

// MetricResult is the trend analysis of one metric over the post-warm-up window.
type MetricResult struct {
	Name             string
	Mean             float64
	SlopePerSecond   float64
	ProjectedGrowth  float64 // slope * window: growth the linear fit attributes to the whole window
	FirstQuarterMean float64
	LastQuarterMean  float64
	Allowed          float64
	Leaking          bool
}

// QuarterDelta is the difference between the last and first quarter means.
func (m MetricResult) QuarterDelta() float64 {
	return m.LastQuarterMean - m.FirstQuarterMean
}

func (m MetricResult) String() string {
	verdict := "flat"
	if m.Leaking {
		verdict = "LEAKING"
	}

	return fmt.Sprintf("%s: %s mean=%.0f slope=%.3f/s projected=%.0f quarters=%.0f->%.0f (delta %.0f) allowed=%.0f",
		m.Name, verdict, m.Mean, m.SlopePerSecond, m.ProjectedGrowth, m.FirstQuarterMean, m.LastQuarterMean,
		m.QuarterDelta(), m.Allowed)
}

// Result is the outcome of analysing a soak run.
type Result struct {
	Samples int
	Window  time.Duration
	Metrics []MetricResult
}

// Leaking returns the metrics judged to be growing.
func (r Result) Leaking() []MetricResult {
	var out []MetricResult

	for _, m := range r.Metrics {
		if m.Leaking {
			out = append(out, m)
		}
	}

	return out
}

// Flat reports whether no metric is growing.
func (r Result) Flat() bool {
	return len(r.Leaking()) == 0
}

func (r Result) String() string {
	parts := make([]string, 0, len(r.Metrics))
	for _, m := range r.Metrics {
		parts = append(parts, m.String())
	}

	return fmt.Sprintf("samples=%d window=%s; %s", r.Samples, r.Window.Round(time.Second), strings.Join(parts, "; "))
}

// Analyze discards samples taken before warmup and judges whether heap in-use, heap allocated and goroutine count
// are flat over the rest. A metric is leaking only when both the least-squares projected growth across the window
// and the last-quarter-minus-first-quarter mean exceed its allowed growth, so a single outlier or a GC sawtooth does
// not trip it on its own while a sustained rise does.
func Analyze(samples []Sample, warmup time.Duration, tol Tolerance) (Result, error) {
	var kept []Sample

	for _, s := range samples {
		if s.Elapsed >= warmup {
			kept = append(kept, s)
		}
	}

	if len(kept) < MinAnalysisSamples {
		return Result{}, errors.NewInvalidArgumentError("soak analysis needs at least %d post-warm-up samples, got %d of %d", MinAnalysisSamples, len(kept), len(samples))
	}

	xs := make([]float64, len(kept))
	inuse := make([]float64, len(kept))
	alloc := make([]float64, len(kept))
	goroutines := make([]float64, len(kept))

	for i, s := range kept {
		xs[i] = s.Elapsed.Seconds()
		inuse[i] = float64(s.HeapInuse)
		alloc[i] = float64(s.HeapAlloc)
		goroutines[i] = float64(s.NumGoroutine)
	}

	window := kept[len(kept)-1].Elapsed - kept[0].Elapsed

	return Result{
		Samples: len(kept),
		Window:  window,
		Metrics: []MetricResult{
			analyzeMetric("heap_inuse", xs, inuse, window, tol.HeapRel, tol.HeapAbs),
			analyzeMetric("heap_alloc", xs, alloc, window, tol.HeapRel, tol.HeapAbs),
			analyzeMetric("goroutines", xs, goroutines, window, tol.GoroutineRel, tol.GoroutineAbs),
		},
	}, nil
}

func analyzeMetric(name string, xs, ys []float64, window time.Duration, rel, abs float64) MetricResult {
	q := len(ys) / 4

	m := MetricResult{
		Name:             name,
		Mean:             mean(ys),
		SlopePerSecond:   Slope(xs, ys),
		FirstQuarterMean: mean(ys[:q]),
		LastQuarterMean:  mean(ys[len(ys)-q:]),
	}

	m.ProjectedGrowth = m.SlopePerSecond * window.Seconds()
	m.Allowed = math.Max(rel*m.Mean, abs)
	m.Leaking = m.ProjectedGrowth > m.Allowed && m.QuarterDelta() > m.Allowed

	return m
}

// Slope returns the ordinary least-squares slope of ys against xs, or 0 when xs has no spread.
func Slope(xs, ys []float64) float64 {
	mx, my := mean(xs), mean(ys)

	var num, den float64

	for i := range xs {
		dx := xs[i] - mx
		num += dx * (ys[i] - my)
		den += dx * dx
	}

	if den == 0 {
		return 0
	}

	return num / den
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}

	var sum float64
	for _, x := range v {
		sum += x
	}

	return sum / float64(len(v))
}
