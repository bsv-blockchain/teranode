package adaptivefetch

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// setMode is a test-only helper that sets the gauge for "test-service"
// without going through State. Production code uses State.emitMode which
// bakes in the serviceName the State was constructed with.
func (m *metrics) setMode(mode Mode) {
	val := 0.0
	if mode == ModeOptimistic {
		val = 1.0
	}
	m.modeGauge.WithLabelValues("test-service").Set(val)
}

// recordTransition is a test-only helper matching setMode — asserts the
// transitions counter is wired correctly from a test's point of view.
func (m *metrics) recordTransition(from, to Mode) {
	m.transitions.WithLabelValues("test-service", from.String(), to.String()).Inc()
}

func TestMetrics_ModeGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics("test-service", reg)

	m.setMode(ModePessimistic)
	require.InDelta(t, 0.0, testutil.ToFloat64(m.modeGauge.WithLabelValues("test-service")), 0.0001)

	m.setMode(ModeOptimistic)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.modeGauge.WithLabelValues("test-service")), 0.0001)
}

func TestMetrics_TransitionsCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics("test-service", reg)

	m.recordTransition(ModePessimistic, ModeOptimistic)
	m.recordTransition(ModePessimistic, ModeOptimistic)
	m.recordTransition(ModeOptimistic, ModePessimistic)

	require.InDelta(t, 2.0, testutil.ToFloat64(
		m.transitions.WithLabelValues("test-service", "pessimistic", "optimistic")), 0.0001)
	require.InDelta(t, 1.0, testutil.ToFloat64(
		m.transitions.WithLabelValues("test-service", "optimistic", "pessimistic")), 0.0001)
}

func TestMetrics_RegisteredNamesMatchSpec(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = newMetrics("test-service", reg)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	joined := strings.Join(names, ",")

	require.Contains(t, joined, "teranode_adaptive_fetch_mode")
	require.Contains(t, joined, "teranode_adaptive_fetch_mode_transitions_total")

	// hit_rate and missing_fetches_total are intentionally NOT exported: until
	// the validation hot paths plumb real LocalHits / MissingFetches counts,
	// they would publish a permanently-perfect (1.0 / 0) series. See metrics.go.
	require.NotContains(t, joined, "teranode_adaptive_fetch_hit_rate")
	require.NotContains(t, joined, "teranode_adaptive_fetch_missing_fetches_total")
}
