package blockvalidation

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestSetMinedMetricsAreLabelFree is the regression guard for the unbounded
// Prometheus cardinality issue: the setTxMined retry/drop counters previously
// carried a per-block-hash label, which adds a permanent time series for every
// distinct block that ever retries or is dropped. The offending block hash is
// recorded in the logs instead, so these counters must stay label-free.
func TestSetMinedMetricsAreLabelFree(t *testing.T) {
	initPrometheusMetrics()

	require.NotNil(t, prometheusBlockValidationSetMinedRetries)
	require.NotNil(t, prometheusBlockValidationSetMinedDrops)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	checked := map[string]bool{
		"teranode_blockvalidation_setmined_retry_total": false,
		"teranode_blockvalidation_setmined_drops_total": false,
	}

	for _, family := range families {
		if _, ok := checked[family.GetName()]; !ok {
			continue
		}

		checked[family.GetName()] = true

		// A label-free counter is a single series whose metric carries no
		// labels. A per-hash label would produce one series per block hash.
		require.Len(t, family.GetMetric(), 1, "%s must be a single series, not one per block hash", family.GetName())
		require.Empty(t, family.GetMetric()[0].GetLabel(), "%s must not carry any variable label", family.GetName())
	}

	for name, found := range checked {
		require.True(t, found, "metric %s should be registered", name)
	}
}

// TestSetMinedMetricsIncrement confirms the label-free counters increment as
// plain counters via Inc().
func TestSetMinedMetricsIncrement(t *testing.T) {
	initPrometheusMetrics()

	retriesBefore := testutil.ToFloat64(prometheusBlockValidationSetMinedRetries)
	dropsBefore := testutil.ToFloat64(prometheusBlockValidationSetMinedDrops)

	prometheusBlockValidationSetMinedRetries.Inc()
	prometheusBlockValidationSetMinedRetries.Inc()
	prometheusBlockValidationSetMinedDrops.Inc()

	require.Equal(t, retriesBefore+2, testutil.ToFloat64(prometheusBlockValidationSetMinedRetries))
	require.Equal(t, dropsBefore+1, testutil.ToFloat64(prometheusBlockValidationSetMinedDrops))
}
