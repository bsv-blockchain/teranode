package blockvalidation

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestQuickWindowMetricsAreRegistered(t *testing.T) {
	initPrometheusMetrics()

	missBefore := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)
	prometheusBlockValidationQuickWindowMissTotal.Inc()
	require.Equal(t, missBefore+1, testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal))

	abortsBefore := testutil.ToFloat64(prometheusBlockValidationQuickWindowAbortsTotal.WithLabelValues("head_failed"))
	prometheusBlockValidationQuickWindowAbortsTotal.WithLabelValues("head_failed").Inc()
	require.Equal(t, abortsBefore+1, testutil.ToFloat64(prometheusBlockValidationQuickWindowAbortsTotal.WithLabelValues("head_failed")))

	prometheusBlockValidationQuickCommitAddBlock.Observe(0.01)
	require.Equal(t, 1, testutil.CollectAndCount(prometheusBlockValidationQuickCommitAddBlock))

	prometheusBlockValidationQuickCommitUnlock.Observe(0.01)
	require.Equal(t, 1, testutil.CollectAndCount(prometheusBlockValidationQuickCommitUnlock))

	prometheusBlockValidationQuickCommitSubtreesSet.Observe(0.01)
	require.Equal(t, 1, testutil.CollectAndCount(prometheusBlockValidationQuickCommitSubtreesSet))

	prometheusBlockValidationQuickCommitBlockExists.Observe(0.01)
	require.Equal(t, 1, testutil.CollectAndCount(prometheusBlockValidationQuickCommitBlockExists))

	prometheusBlockValidationQuickWindowDepth.Set(3)
	require.Equal(t, float64(3), testutil.ToFloat64(prometheusBlockValidationQuickWindowDepth))

	prometheusBlockValidationQuickWindowGateWait.Observe(0.01)
	require.Equal(t, 1, testutil.CollectAndCount(prometheusBlockValidationQuickWindowGateWait))

	gateWaitsBefore := testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits)
	prometheusBlockValidationQuickWindowGateWaits.Inc()
	require.Equal(t, gateWaitsBefore+1, testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits))

	prometheusBlockValidationQuickWindowOldestAgeSeconds.Set(1.5)
	require.Equal(t, 1.5, testutil.ToFloat64(prometheusBlockValidationQuickWindowOldestAgeSeconds))
}
