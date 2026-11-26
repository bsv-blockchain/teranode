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

func gaugeSet(name string, values ...float64) MetricSet {
	family := &dto.MetricFamily{Name: proto.String(name), Type: dto.MetricType_GAUGE.Enum()}

	for _, v := range values {
		family.Metric = append(family.Metric, &dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(v)}})
	}

	return MetricSet{name: family}
}

func publishedEvery5s(at time.Duration, perSec float64) float64 {
	return perSec * float64(int(at.Seconds())/5*5)
}

func TestPerSecOverUsesWindow(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)

	for at := time.Duration(0); at <= 60*time.Second; at += 2 * time.Second {
		r.Observe(start.Add(at), counterSet("c_total", publishedEvery5s(at, 100)))

		if at < 10*time.Second {
			continue
		}

		got, ok := r.PerSecOver("c_total", 10*time.Second)
		require.True(t, ok, at)
		require.InDelta(t, 100.0, got, 1e-9, at)
	}
}

func TestPerSecOverNotOKUntilWindowFilled(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)

	for at := time.Duration(0); at < 10*time.Second; at += 2 * time.Second {
		r.Observe(start.Add(at), counterSet("c_total", 10*at.Seconds()))

		_, ok := r.PerSecOver("c_total", 10*time.Second)
		require.False(t, ok, at)
	}

	r.Observe(start.Add(10*time.Second), counterSet("c_total", 100))

	got, ok := r.PerSecOver("c_total", 10*time.Second)
	require.True(t, ok)
	require.InDelta(t, 10.0, got, 1e-9)

	_, ok = r.PerSecOver("never_seen", 10*time.Second)
	require.False(t, ok)
}

func TestPerSecOverResetNotOK(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	values := []float64{1000, 1020, 1040, 5, 25, 45, 65, 85, 105, 125}

	for i, v := range values {
		at := start.Add(time.Duration(i) * 2 * time.Second)
		r.Observe(at, counterSet("c_total", v))

		got, ok := r.PerSecOver("c_total", 10*time.Second)

		switch {
		case i < 5:
			require.False(t, ok, i)
		case i < 8:
			require.False(t, ok, i)
			require.Zero(t, got)
		default:
			require.True(t, ok, i)
			require.InDelta(t, 10.0, got, 1e-9, i)
		}
	}
}

func TestPerSecOverHistoryBounded(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)

	for i := 0; i < 200; i++ {
		r.Observe(start.Add(time.Duration(i)*2*time.Second), counterSet("c_total", float64(i)))
	}

	require.LessOrEqual(t, len(r.history), 16)

	_, ok := r.PerSecOver("c_total", 31*time.Second)
	require.False(t, ok)

	got, ok := r.PerSecOver("c_total", 30*time.Second)
	require.True(t, ok)
	require.InDelta(t, 0.5, got, 1e-9)

	for i := 0; i < 200; i++ {
		r.Observe(start.Add(400*time.Second+time.Duration(i)*time.Millisecond), counterSet("c_total", float64(i)))
	}

	require.LessOrEqual(t, len(r.history), historyMaxLen)
}

func TestPerSecCumulativeGauge(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, gaugeSet("g", 1000))
	r.Observe(start.Add(2*time.Second), gaugeSet("g", 1900))

	got, ok := r.PerSec("g")
	require.True(t, ok)
	require.InDelta(t, 450.0, got, 1e-9)

	r.Observe(start.Add(4*time.Second), gaugeSet("g", 10))

	_, ok = r.PerSec("g")
	require.False(t, ok)
}

func TestMeanFromSumAndCountDeltas(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, histSet("h", 10, 500, []float64{1000}, []uint64{10}))
	r.Observe(start.Add(2*time.Second), histSet("h", 20, 1280, []float64{1000}, []uint64{20}))

	got, ok := r.Mean("h")
	require.True(t, ok)
	require.InDelta(t, 78.0, got, 1e-9)
}

func TestMeanNotOK(t *testing.T) {
	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, histSet("h", 10, 500, []float64{1000}, []uint64{10}))

	_, ok := r.Mean("h")
	require.False(t, ok)

	r.Observe(start.Add(2*time.Second), histSet("h", 10, 500, []float64{1000}, []uint64{10}))

	_, ok = r.Mean("h")
	require.False(t, ok)

	r.Observe(start.Add(4*time.Second), histSet("h", 2, 30, []float64{1000}, []uint64{2}))

	_, ok = r.Mean("h")
	require.False(t, ok)

	r.Observe(start.Add(6*time.Second), counterSet("h", 5))
	r.Observe(start.Add(8*time.Second), counterSet("h", 9))

	_, ok = r.Mean("h")
	require.False(t, ok)
}

func TestAgeSecReadsTimestampGauge(t *testing.T) {
	r := NewRates()

	_, ok := r.AgeSec("ts")
	require.False(t, ok)

	r.Observe(time.Unix(100, 0), gaugeSet("ts", 96))

	age, ok := r.AgeSec("ts")
	require.True(t, ok)
	require.InDelta(t, 4, age, 1e-9)

	r.Observe(time.Unix(130, 0), gaugeSet("ts", 96))

	age, ok = r.AgeSec("ts")
	require.True(t, ok)
	require.InDelta(t, 34, age, 1e-9)

	r.Observe(time.Unix(140, 0), gaugeSet("ts", 150))

	age, ok = r.AgeSec("ts")
	require.True(t, ok)
	require.Zero(t, age)

	r.Observe(time.Unix(150, 0), gaugeSet("ts", 0))

	_, ok = r.AgeSec("ts")
	require.False(t, ok)

	_, ok = r.AgeSec("absent")
	require.False(t, ok)
}

func TestStoreSeriesSeparatesStoresAndKeepsTheFamilyTotal(t *testing.T) {
	build := func(subtree, block float64) MetricSet {
		return mergeStores(
			labelled(counterSet("blob_total", subtree), "subtree"),
			labelled(counterSet("blob_total", block), "block"),
		)
	}

	r := NewRates()
	start := time.Unix(100, 0)
	r.Observe(start, build(10, 100))
	r.Observe(start.Add(2*time.Second), build(30, 100))

	got, ok := r.PerSec(StoreSeries("blob_total", "subtree"))
	require.True(t, ok)
	require.InDelta(t, 10, got, 1e-9)

	got, ok = r.PerSec(StoreSeries("blob_total", "block"))
	require.True(t, ok)
	require.InDelta(t, 0, got, 1e-9)

	got, ok = r.PerSec("blob_total")
	require.True(t, ok)
	require.InDelta(t, 10, got, 1e-9)

	_, ok = r.PerSec(StoreSeries("blob_total", "tx"))
	require.False(t, ok)
}

func TestTotalReadsCounterGaugeAndHistogramCount(t *testing.T) {
	r := NewRates()

	_, ok := r.Total("c_total")
	require.False(t, ok)

	r.Observe(time.Unix(100, 0), join(counterSet("c_total", 7), histSet("h", 12, 3, []float64{1}, []uint64{12})))

	total, ok := r.Total("c_total")
	require.True(t, ok)
	require.InDelta(t, 7, total, 1e-9)

	total, ok = r.Total("h")
	require.True(t, ok)
	require.InDelta(t, 12, total, 1e-9)

	_, ok = r.Total("absent")
	require.False(t, ok)
}
