package util

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// fakeStatsSnapshot builds a stats map mimicking the shape returned by
// aerospike.Client.Stats(). Aerospike returns CUMULATIVE counts since
// process start, both for scalar counters and histogram buckets.
// clusterKey is the top-level key — tests use unique keys to avoid
// promauto duplicate-registration on the global Prometheus registry.
func fakeStatsSnapshot(clusterKey string, numHistograms, numBuckets int, bucketCount float64) map[string]interface{} {
	bucketSlice := make([]interface{}, numBuckets)
	for i := range bucketSlice {
		bucketSlice[i] = bucketCount
	}

	cluster := map[string]interface{}{}

	// One scalar counter we expect to grow per snapshot.
	cluster["transaction-retry-count"] = bucketCount * float64(numBuckets)

	for h := 0; h < numHistograms; h++ {
		histName := "operate-metrics"
		if h > 0 {
			histName = fmt.Sprintf("operate-metrics-%d", h)
		}

		cluster[histName] = map[string]interface{}{
			"count":   bucketCount * float64(numBuckets),
			"buckets": bucketSlice,
			"min":     uint64(0),
			"max":     uint64(0),
			"sum":     uint64(0),
		}
	}

	return map[string]interface{}{
		clusterKey: cluster,
	}
}

// clearAerospikeStatsLastState resets only the "last value" tracking maps.
// The Prometheus registry cannot be reset between tests, so tests use unique
// cluster keys to avoid name collisions.
func clearAerospikeStatsLastState() {
	for _, k := range aerospikeCounterLast.Keys() {
		aerospikeCounterLast.Delete(k)
	}

	for _, k := range aerospikeHistogramLastBuckets.Keys() {
		aerospikeHistogramLastBuckets.Delete(k)
	}
}

func latencyBuckets() []float64 {
	buckets := make([]float64, 24)
	base := 2.0

	for i := uint(1); i <= 24; i++ {
		shift := 1<<i - 1
		buckets[i-1] = base * float64(shift)
	}

	return buckets
}

// TestProcessAerospikeStatsCumulativeCounterDelta verifies that calling
// processAerospikeStats twice with growing cumulative scalar values only
// adds the delta to Prometheus counters (instead of the previous behaviour
// of re-adding the cumulative value).
func TestProcessAerospikeStatsCumulativeCounterDelta(t *testing.T) {
	clearAerospikeStatsLastState()

	logger := ulogger.New("test")
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	buckets := latencyBuckets()

	cluster := "test-counter-delta"

	snap1 := map[string]interface{}{
		cluster: map[string]interface{}{
			"transaction-retry-count": float64(100),
		},
	}
	processAerospikeStats(logger, snap1, buckets, regex)

	snap2 := map[string]interface{}{
		cluster: map[string]interface{}{
			"transaction-retry-count": float64(250),
		},
	}
	processAerospikeStats(logger, snap2, buckets, regex)

	counter, ok := aerospikePrometheusMetrics.Get("test_counter_delta_transaction_retry_count")
	require.True(t, ok, "counter not registered")

	got := dtoCounterValue(t, counter)
	require.InDelta(t, float64(250), got, 0.001, "counter must accumulate delta to current cumulative")
}

// TestProcessAerospikeStatsHistogramBucketDelta verifies the histogram path
// only observes the per-refresh delta, not the cumulative bucket count.
func TestProcessAerospikeStatsHistogramBucketDelta(t *testing.T) {
	clearAerospikeStatsLastState()

	logger := ulogger.New("test")
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	buckets := latencyBuckets()

	cluster := "test-hist-delta"
	histKey := "aerospike_client_histogram_test_hist_delta_operate_metrics"

	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 1, 24, 10), buckets, regex)
	require.Equal(t, uint64(24*10), histogramSampleCount(t, histKey), "first refresh observes full initial cumulative")

	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 1, 24, 11), buckets, regex)
	require.Equal(t, uint64(24*11), histogramSampleCount(t, histKey), "second refresh observes only delta (24 obs)")

	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 1, 24, 1000), buckets, regex)
	require.Equal(t, uint64(24*1000), histogramSampleCount(t, histKey), "total equals latest cumulative")
}

// TestProcessAerospikeStatsHistogramResetHandling verifies counter resets
// (e.g. client restart causing cumulative to drop) are tolerated.
func TestProcessAerospikeStatsHistogramResetHandling(t *testing.T) {
	clearAerospikeStatsLastState()

	logger := ulogger.New("test")
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	buckets := latencyBuckets()

	cluster := "test-hist-reset"
	histKey := "aerospike_client_histogram_test_hist_reset_operate_metrics"

	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 1, 24, 100), buckets, regex)
	c1 := histogramSampleCount(t, histKey)

	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 1, 24, 5), buckets, regex)
	c2 := histogramSampleCount(t, histKey)

	require.Equal(t, c1+24*5, c2, "after cumulative reset treat new value as the delta")
}

// BenchmarkProcessAerospikeStatsSteadyState reproduces the operational
// pattern: cumulative counts grow steadily. Under the old (replay) code path
// each refresh re-observed the entire cumulative count; under the new
// (delta) path each refresh observes only the new ops since the last refresh.
// Run with: go test -run x -bench BenchmarkProcessAerospikeStats -benchmem ./util/
func BenchmarkProcessAerospikeStatsSteadyState(b *testing.B) {
	clearAerospikeStatsLastState()

	logger := ulogger.New("test")
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	buckets := latencyBuckets()

	cluster := "bench-steady"

	// Warm: pretend 10M ops have already been observed before this benchmark
	// starts (typical for a long-running process).
	warm := fakeStatsSnapshot(cluster, 11, 24, 10_000_000)
	processAerospikeStats(logger, warm, buckets, regex)

	delta := float64(50_000) // ops per refresh per bucket

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cum := 10_000_000 + delta*float64(i+1)
		snap := fakeStatsSnapshot(cluster, 11, 24, cum)
		processAerospikeStats(logger, snap, buckets, regex)
	}
}

// BenchmarkProcessAerospikeStatsLegacyReplay reproduces the previous
// (pre-fix) behaviour of replaying the entire cumulative bucket count to
// each Prometheus histogram on every refresh. This is the side-by-side
// baseline for BenchmarkProcessAerospikeStatsSteadyState.
//
// NOTE: this is a benchmark-only re-implementation; the production code
// path has been replaced by processAerospikeStats with delta tracking.
func BenchmarkProcessAerospikeStatsLegacyReplay(b *testing.B) {
	logger := ulogger.New("test")
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	buckets := latencyBuckets()

	cluster := "bench-legacy"

	// Pre-register the histograms to exclude registration cost from the
	// inner loop, matching the new bench.
	processAerospikeStats(logger, fakeStatsSnapshot(cluster, 11, 24, 0), buckets, regex)

	delta := float64(50_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cum := 10_000_000 + delta*float64(i+1)
		snap := fakeStatsSnapshot(cluster, 11, 24, cum)
		legacyReplayProcessStats(logger, snap, buckets, regex)
	}
}

// legacyReplayProcessStats is a verbatim copy of the original pre-fix loop
// body in initStats.func2, kept only for the benchmark above to quantify
// the saving from the delta-based replacement.
func legacyReplayProcessStats(
	logger ulogger.Logger,
	stats map[string]interface{},
	latencyBuckets []float64,
	nonAlphanumericRegex *regexp.Regexp,
) {
	for key, stat := range stats {
		key := nonAlphanumericRegex.ReplaceAllString(key, "_")

		s, ok := stat.(map[string]interface{})
		if !ok {
			continue
		}

		for subKey, subStat := range s {
			subKey := nonAlphanumericRegex.ReplaceAllString(subKey, "_")

			sub, ok := subStat.(map[string]interface{})
			if !ok {
				continue
			}

			rawBuckets, ok := sub["buckets"].([]interface{})
			if !ok || len(rawBuckets) != len(latencyBuckets) {
				continue
			}

			histogramKey := "aerospike_client_histogram_" + key + "_" + subKey

			histogram, ok := aerospikePrometheusHistograms.Get(histogramKey)
			if !ok {
				logger.Warnf("Histogram %s not found", histogramKey)
				continue
			}

			for i, v := range rawBuckets {
				count, ok := v.(float64)
				if !ok || count == 0 {
					continue
				}

				var value float64
				if i < len(latencyBuckets)-1 {
					value = latencyBuckets[i]
				} else {
					value = latencyBuckets[len(latencyBuckets)-1]
				}

				for j := 0; j < int(count); j++ {
					histogram.Observe(value)
				}
			}
		}
	}
}

func dtoCounterValue(t *testing.T, c interface{}) float64 {
	t.Helper()

	w, ok := c.(interface {
		Write(*dto.Metric) error
	})
	require.True(t, ok, "expected Prometheus metric to implement Write")

	m := &dto.Metric{}
	require.NoError(t, w.Write(m))
	require.NotNil(t, m.Counter, "metric is not a counter")

	return m.Counter.GetValue()
}

func histogramSampleCount(t *testing.T, key string) uint64 {
	t.Helper()

	h, ok := aerospikePrometheusHistograms.Get(key)
	require.True(t, ok, "histogram %s not registered", key)

	w, ok := h.(interface {
		Write(*dto.Metric) error
	})
	require.True(t, ok, "histogram does not implement Write")

	m := &dto.Metric{}
	require.NoError(t, w.Write(m))
	require.NotNil(t, m.Histogram, "metric is not a histogram")

	return m.Histogram.GetSampleCount()
}
