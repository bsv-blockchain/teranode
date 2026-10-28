package collect

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func counterSet(name string, values ...float64) MetricSet {
	family := &dto.MetricFamily{Name: proto.String(name), Type: dto.MetricType_COUNTER.Enum()}

	for _, v := range values {
		family.Metric = append(family.Metric, &dto.Metric{Counter: &dto.Counter{Value: proto.Float64(v)}})
	}

	return MetricSet{name: family}
}

func histSet(name string, count uint64, sum float64, uppers []float64, cums []uint64) MetricSet {
	h := &dto.Histogram{SampleCount: proto.Uint64(count), SampleSum: proto.Float64(sum)}

	for i, u := range uppers {
		h.Bucket = append(h.Bucket, &dto.Bucket{UpperBound: proto.Float64(u), CumulativeCount: proto.Uint64(cums[i])})
	}

	family := &dto.MetricFamily{
		Name:   proto.String(name),
		Type:   dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{Histogram: h}},
	}

	return MetricSet{name: family}
}

func TestPerSecFirstSampleNotOK(t *testing.T) {
	r := NewRates()
	r.Observe(time.Unix(100, 0), counterSet("c_total", 100))

	_, ok := r.PerSec("c_total")
	require.False(t, ok)
}

func TestPerSecSteadyCounter(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, counterSet("c_total", 100))
	r.Observe(start.Add(2*time.Second), counterSet("c_total", 300))

	got, ok := r.PerSec("c_total")
	require.True(t, ok)
	require.InDelta(t, 100.0, got, 1e-9)
}

func TestPerSecSumsLabelSeries(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, counterSet("c_total", 10, 20))
	r.Observe(start.Add(2*time.Second), counterSet("c_total", 30, 40))

	got, ok := r.PerSec("c_total")
	require.True(t, ok)
	require.InDelta(t, 20.0, got, 1e-9)
}

func TestPerSecCounterResetNeverNegative(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, counterSet("c_total", 1000))
	r.Observe(start.Add(2*time.Second), counterSet("c_total", 5))

	got, ok := r.PerSec("c_total")
	require.False(t, ok)
	require.Zero(t, got)

	r.Observe(start.Add(4*time.Second), counterSet("c_total", 25))

	got, ok = r.PerSec("c_total")
	require.True(t, ok)
	require.InDelta(t, 10.0, got, 1e-9)
}

func TestPerSecAbsentFamilyAndNonPositiveElapsed(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, counterSet("c_total", 1))
	r.Observe(start, counterSet("c_total", 5))

	_, ok := r.PerSec("c_total")
	require.False(t, ok)

	r.Observe(start.Add(time.Second), MetricSet{})

	_, ok = r.PerSec("c_total")
	require.False(t, ok)

	_, ok = r.PerSec("never_seen")
	require.False(t, ok)
}

func TestPerSecHistogramSampleCount(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, histSet("h", 10, 1, []float64{1}, []uint64{10}))
	r.Observe(start.Add(5*time.Second), histSet("h", 60, 9, []float64{1}, []uint64{60}))

	got, ok := r.PerSec("h")
	require.True(t, ok)
	require.InDelta(t, 10.0, got, 1e-9)
}

func TestQuantileFromBuckets(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	uppers := []float64{0.1, 0.5, 1}
	r.Observe(start, histSet("h", 30, 5, uppers, []uint64{10, 20, 30}))
	r.Observe(start.Add(2*time.Second), histSet("h", 130, 50, uppers, []uint64{60, 110, 130}))

	got, ok := r.Quantile("h", 0.99)
	require.True(t, ok)
	require.InDelta(t, 0.95, got, 1e-9)

	got, ok = r.Quantile("h", 0.25)
	require.True(t, ok)
	require.InDelta(t, 0.05, got, 1e-9)
}

func TestQuantileInfBucketReturnsLastFiniteBound(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	uppers := []float64{0.1, 1}
	r.Observe(start, histSet("h", 0, 0, uppers, []uint64{0, 0}))
	r.Observe(start.Add(time.Second), histSet("h", 100, 500, uppers, []uint64{10, 20}))

	got, ok := r.Quantile("h", 0.99)
	require.True(t, ok)
	require.InDelta(t, 1.0, got, 1e-9)
}

func TestQuantileMergesLabelSeries(t *testing.T) {
	build := func(a, b uint64) MetricSet {
		set := histSet("h", a, 0, []float64{1, 2}, []uint64{a, a})
		other := histSet("h", b, 0, []float64{1, 2}, []uint64{0, b})
		set["h"].Metric = append(set["h"].Metric, other["h"].Metric...)

		return set
	}

	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, build(0, 0))
	r.Observe(start.Add(time.Second), build(50, 50))

	got, ok := r.Quantile("h", 0.75)
	require.True(t, ok)
	require.InDelta(t, 1.5, got, 1e-9)
}

func TestQuantileNoObservations(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	uppers := []float64{0.1, 1}

	got, ok := r.Quantile("h", 0.99)
	require.False(t, ok)
	require.Zero(t, got)

	r.Observe(start, histSet("h", 5, 1, uppers, []uint64{2, 5}))

	_, ok = r.Quantile("h", 0.99)
	require.False(t, ok)

	r.Observe(start.Add(time.Second), histSet("h", 5, 1, uppers, []uint64{2, 5}))

	got, ok = r.Quantile("h", 0.99)
	require.False(t, ok)
	require.Zero(t, got)
}

func TestQuantileResetNotOK(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	uppers := []float64{0.1, 1}
	r.Observe(start, histSet("h", 100, 1, uppers, []uint64{50, 100}))
	r.Observe(start.Add(time.Second), histSet("h", 10, 1, uppers, []uint64{5, 10}))

	_, ok := r.Quantile("h", 0.5)
	require.False(t, ok)
}

func TestQuantileBucketLayoutChangeNotOK(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, histSet("h", 30, 5, []float64{0.1, 0.5, 1}, []uint64{10, 20, 30}))

	newLayout := []float64{0.1, 0.25, 0.5, 1}
	r.Observe(start.Add(time.Second), histSet("h", 130, 50, newLayout, []uint64{60, 80, 110, 130}))

	got, ok := r.Quantile("h", 0.7)
	require.False(t, ok)
	require.Zero(t, got)

	r.Observe(start.Add(2*time.Second), histSet("h", 230, 90, newLayout, []uint64{110, 140, 190, 230}))

	got, ok = r.Quantile("h", 0.7)
	require.True(t, ok)
	require.InDelta(t, 0.375, got, 1e-9)
}
