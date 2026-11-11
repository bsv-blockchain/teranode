package alarm

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const (
	halfLife          = time.Hour
	minObservations   = 30
	toleranceSigmas   = 3.0
	toleranceFraction = 0.05
	toleranceMinimum  = 1e-9
	clampAfter        = 5
	clampSigmas       = 8.0
)

type MetricState struct {
	Mean     float64   `json:"mean"`
	Variance float64   `json:"variance"`
	Count    int       `json:"count"`
	First    time.Time `json:"first"`
	Last     time.Time `json:"last"`
}

type BaselineState struct {
	Metrics map[string]MetricState `json:"metrics"`
}

type Baseline struct {
	mu      sync.Mutex
	warmup  time.Duration
	metrics map[string]MetricState
}

func NewBaseline(warmup time.Duration) *Baseline {
	return &Baseline{warmup: warmup, metrics: map[string]MetricState{}}
}

func RestoreBaseline(state BaselineState, warmup time.Duration) *Baseline {
	b := NewBaseline(warmup)

	for id, m := range state.Metrics {
		b.metrics[id] = m
	}

	return b
}

func (b *Baseline) State() BaselineState {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := BaselineState{Metrics: make(map[string]MetricState, len(b.metrics))}

	for id, m := range b.metrics {
		out.Metrics[id] = m
	}

	return out
}

func (b *Baseline) Observe(metric string, v float64, t time.Time) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.metrics[metric]
	if !ok {
		b.metrics[metric] = MetricState{Mean: v, Count: 1, First: t, Last: t}
		return
	}

	if m.Count >= clampAfter {
		band := clampSigmas * toleranceOf(m)
		v = math.Max(m.Mean-band, math.Min(m.Mean+band, v))
	}

	dt := t.Sub(m.Last)
	if dt < 0 {
		dt = 0
	}

	alpha := 1 - math.Exp2(-float64(dt)/float64(halfLife))
	diff := v - m.Mean
	incr := alpha * diff
	m.Mean += incr
	m.Variance = (1 - alpha) * (m.Variance + diff*incr)
	m.Count++

	if t.After(m.Last) {
		m.Last = t
	}

	b.metrics[metric] = m
}

func (b *Baseline) Normal(metric string) (mean, tolerance float64, ready bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.metrics[metric]
	if !ok {
		return 0, 0, false
	}

	return m.Mean, toleranceOf(m), m.Count >= minObservations && m.Last.Sub(m.First) >= b.warmup
}

func toleranceOf(m MetricState) float64 {
	t := math.Max(toleranceSigmas*math.Sqrt(math.Max(m.Variance, 0)), toleranceFraction*math.Abs(m.Mean))

	return math.Max(t, toleranceMinimum)
}

func (b *Baseline) Calibration(now time.Time) snapshot.Calibration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.metrics) == 0 {
		return snapshot.Calibration{}
	}

	ready := true
	fraction := 1.0

	var until time.Time

	for _, m := range b.metrics {
		f := 1.0

		if b.warmup > 0 {
			f = math.Min(1, float64(now.Sub(m.First))/float64(b.warmup))
		}

		f = math.Max(f, 0)
		fraction = math.Min(fraction, f)

		if m.Count >= minObservations && now.Sub(m.First) >= b.warmup {
			continue
		}

		ready = false

		if end := m.First.Add(b.warmup); end.After(until) {
			until = end
		}
	}

	if ready {
		return snapshot.Calibration{Ready: true, Fraction: fraction}
	}

	return snapshot.Calibration{Until: until, Fraction: fraction}
}

func ParseFloors(entries []string) (map[string]float64, error) {
	floors := make(map[string]float64, len(entries))

	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}

		name, value, found := strings.Cut(entry, "=")
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)

		if !found || name == "" || value == "" {
			return nil, errors.NewInvalidArgumentError("invalid floor entry %s: want metric=value", strconv.Quote(entry))
		}

		f, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errors.NewInvalidArgumentError("invalid floor entry %s: value must be a finite number", strconv.Quote(entry))
		}

		if _, dup := floors[name]; dup {
			return nil, errors.NewInvalidArgumentError("invalid floor entry %s: duplicate metric", strconv.Quote(entry))
		}

		floors[name] = f
	}

	return floors, nil
}

func CheckFloorKeys(floors map[string]float64) error {
	known := make(map[string]bool, len(DefaultRules))

	for _, r := range DefaultRules {
		if r.FloorKey != "" {
			known[r.FloorKey] = true
		}
	}

	var unknown []string

	for key := range floors {
		if !known[key] {
			unknown = append(unknown, key)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	sort.Strings(unknown)

	return errors.NewInvalidArgumentError("unknown floor keys: %s", strings.Join(unknown, ", "))
}
