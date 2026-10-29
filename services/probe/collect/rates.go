package collect

import (
	"math"
	"sort"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type bucketSample struct {
	upper      float64
	cumulative float64
}

type seriesSample struct {
	value     float64
	histogram bool
	count     float64
	sum       float64
	buckets   []bucketSample
}

type observation struct {
	at     time.Time
	series map[string]seriesSample
}

const (
	historyRetention = 30 * time.Second
	historyMaxLen    = 64
)

type Rates struct {
	mu      sync.Mutex
	prev    *observation
	curr    *observation
	history []*observation
}

func NewRates() *Rates {
	return &Rates{}
}

func (r *Rates) Observe(now time.Time, m MetricSet) {
	snap := &observation{at: now, series: make(map[string]seriesSample, len(m))}

	for name, family := range m {
		if sample, ok := sampleOf(name, family); ok {
			snap.series[name] = sample
		}

		addStoreSeries(snap, name, family)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.prev = r.curr
	r.curr = snap
	r.history = trimHistory(append(r.history, snap), now)
}

func StoreSeries(name, store string) string {
	return name + `{store="` + store + `"}`
}

func sampleOf(name string, family *dto.MetricFamily) (seriesSample, bool) {
	switch family.GetType() {
	case dto.MetricType_COUNTER, dto.MetricType_GAUGE:
		value, ok := (MetricSet{name: family}).Sum(name)
		return seriesSample{value: value}, ok
	case dto.MetricType_HISTOGRAM:
		return histogramSample(family)
	default:
		return seriesSample{}, false
	}
}

func addStoreSeries(snap *observation, name string, family *dto.MetricFamily) {
	groups := make(map[string][]*dto.Metric)

	for _, metric := range family.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "store" {
				groups[label.GetValue()] = append(groups[label.GetValue()], metric)
			}
		}
	}

	for store, metrics := range groups {
		group := &dto.MetricFamily{Name: family.Name, Type: family.Type, Metric: metrics}

		if sample, ok := sampleOf(name, group); ok {
			snap.series[StoreSeries(name, store)] = sample
		}
	}
}

func trimHistory(history []*observation, now time.Time) []*observation {
	start := 0
	for start < len(history)-1 && (now.Sub(history[start].at) > historyRetention || len(history)-start > historyMaxLen) {
		start++
	}

	return append([]*observation(nil), history[start:]...)
}

func (r *Rates) frozen() *Rates {
	r.mu.Lock()
	defer r.mu.Unlock()

	return &Rates{prev: r.prev, curr: r.curr, history: append([]*observation(nil), r.history...)}
}

func (r *Rates) PerSecOver(name string, window time.Duration) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(r.history)
	if n < 2 || window <= 0 {
		return 0, false
	}

	newest := r.history[n-1]

	base := -1

	for i := n - 2; i >= 0; i-- {
		if newest.at.Sub(r.history[i].at) >= window {
			base = i
			break
		}
	}

	if base < 0 {
		return 0, false
	}

	prevValue, ok := seriesValue(r.history[base], name)
	if !ok {
		return 0, false
	}

	first := prevValue

	for _, obs := range r.history[base+1:] {
		value, ok := seriesValue(obs, name)
		if !ok || value < prevValue {
			return 0, false
		}

		prevValue = value
	}

	return (prevValue - first) / newest.at.Sub(r.history[base].at).Seconds(), true
}

func seriesValue(obs *observation, name string) (float64, bool) {
	sample, ok := obs.series[name]
	if !ok {
		return 0, false
	}

	if sample.histogram {
		return sample.count, true
	}

	return sample.value, true
}

func histogramSample(family *dto.MetricFamily) (seriesSample, bool) {
	if len(family.GetMetric()) == 0 {
		return seriesSample{}, false
	}

	sample := seriesSample{histogram: true}
	merged := make(map[float64]float64)

	for _, metric := range family.GetMetric() {
		h := metric.GetHistogram()
		sample.count += float64(h.GetSampleCount())
		sample.sum += h.GetSampleSum()

		for _, b := range h.GetBucket() {
			merged[b.GetUpperBound()] += float64(b.GetCumulativeCount())
		}
	}

	sample.buckets = make([]bucketSample, 0, len(merged))
	for upper, cumulative := range merged {
		sample.buckets = append(sample.buckets, bucketSample{upper: upper, cumulative: cumulative})
	}

	sort.Slice(sample.buckets, func(i, j int) bool { return sample.buckets[i].upper < sample.buckets[j].upper })

	return sample, true
}

func (r *Rates) pair(name string) (prev, curr seriesSample, elapsed time.Duration, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.prev == nil || r.curr == nil {
		return seriesSample{}, seriesSample{}, 0, false
	}

	prev, hasPrev := r.prev.series[name]
	curr, hasCurr := r.curr.series[name]

	if !hasPrev || !hasCurr {
		return seriesSample{}, seriesSample{}, 0, false
	}

	return prev, curr, r.curr.at.Sub(r.prev.at), true
}

func (r *Rates) Total(name string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.curr == nil {
		return 0, false
	}

	return seriesValue(r.curr, name)
}

func (r *Rates) AgeSec(name string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.curr == nil {
		return 0, false
	}

	stamp, ok := seriesValue(r.curr, name)
	if !ok || stamp <= 0 {
		return 0, false
	}

	return max(float64(r.curr.at.UnixNano())/float64(time.Second)-stamp, 0), true
}

func (r *Rates) PerSec(name string) (float64, bool) {
	prev, curr, elapsed, ok := r.pair(name)
	if !ok || elapsed <= 0 {
		return 0, false
	}

	before, after := prev.value, curr.value
	if curr.histogram {
		before, after = prev.count, curr.count
	}

	delta := after - before
	if delta < 0 {
		return 0, false
	}

	return delta / elapsed.Seconds(), true
}

func (r *Rates) Mean(histName string) (float64, bool) {
	prev, curr, _, ok := r.pair(histName)
	if !ok || !prev.histogram || !curr.histogram {
		return 0, false
	}

	count := curr.count - prev.count
	sum := curr.sum - prev.sum

	if count <= 0 || sum < 0 {
		return 0, false
	}

	return sum / count, true
}

func (r *Rates) Quantile(histName string, q float64) (float64, bool) {
	if q <= 0 || q >= 1 {
		return 0, false
	}

	prev, curr, _, ok := r.pair(histName)
	if !ok || !prev.histogram || !curr.histogram {
		return 0, false
	}

	total := curr.count - prev.count
	if total <= 0 {
		return 0, false
	}

	if len(prev.buckets) != len(curr.buckets) {
		return 0, false
	}

	deltas := make([]bucketSample, 0, len(curr.buckets))

	for i, b := range curr.buckets {
		if prev.buckets[i].upper != b.upper {
			return 0, false
		}

		delta := b.cumulative - prev.buckets[i].cumulative
		if delta < 0 {
			return 0, false
		}

		deltas = append(deltas, bucketSample{upper: b.upper, cumulative: delta})
	}

	rank := q * total
	lowerBound, lowerCum := 0.0, 0.0
	lastFinite, haveFinite := 0.0, false

	for _, b := range deltas {
		if math.IsInf(b.upper, 1) {
			break
		}

		lastFinite, haveFinite = b.upper, true

		if b.cumulative >= rank {
			if b.upper <= 0 && lowerBound == 0 {
				return b.upper, true
			}

			return lowerBound + (b.upper-lowerBound)*(rank-lowerCum)/(b.cumulative-lowerCum), true
		}

		lowerBound, lowerCum = b.upper, b.cumulative
	}

	if !haveFinite {
		return 0, false
	}

	return lastFinite, true
}
