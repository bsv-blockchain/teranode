// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInitPrometheusMetrics(t *testing.T) {
	// Call initPrometheusMetrics multiple times to ensure it's idempotent
	initPrometheusMetrics()
	initPrometheusMetrics()
	initPrometheusMetrics()

	// Verify that the metrics are initialized
	require.NotNil(t, prometheusHealth)

	// Verify the metric is registered with correct name and help text
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var foundHealthMetric bool
	for _, family := range metricFamilies {
		if family.GetName() == "teranode_alert_health" {
			foundHealthMetric = true
			require.Equal(t, "Number of calls to the Health endpoint", family.GetHelp())
			break
		}
	}

	require.True(t, foundHealthMetric, "Health metric should be registered")
}

func TestPrometheusHealthMetric(t *testing.T) {
	// Initialize metrics
	initPrometheusMetrics()

	// Reset the counter to ensure clean state
	prometheusHealth.Add(0)

	// Get initial value
	initialValue := testutil.ToFloat64(prometheusHealth)

	// Increment the counter
	prometheusHealth.Add(1)

	// Verify the value increased
	newValue := testutil.ToFloat64(prometheusHealth)
	require.Equal(t, initialValue+1, newValue)

	// Increment again
	prometheusHealth.Add(2)

	// Verify the value increased by 2
	finalValue := testutil.ToFloat64(prometheusHealth)
	require.Equal(t, newValue+2, finalValue)
}

func TestPrometheusMetricLabels(t *testing.T) {
	// Initialize metrics
	initPrometheusMetrics()

	// Verify the metric has correct labels structure
	metric := &prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "alert",
		Name:      "health",
		Help:      "Number of calls to the Health endpoint",
	}

	expectedName := prometheus.BuildFQName(metric.Namespace, metric.Subsystem, metric.Name)
	require.Equal(t, "teranode_alert_health", expectedName)

	// Verify the metric can be found in the registry
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var foundMetric bool
	for _, family := range metricFamilies {
		if family.GetName() == expectedName {
			foundMetric = true
			require.Equal(t, metric.Help, family.GetHelp())
			break
		}
	}

	require.True(t, foundMetric)
}

func TestPrometheusMetricOutput(t *testing.T) {
	// Initialize metrics
	initPrometheusMetrics()

	// Get current value and increment by 5
	before := testutil.ToFloat64(prometheusHealth)
	prometheusHealth.Add(5)
	after := testutil.ToFloat64(prometheusHealth)

	// Verify that we incremented by 5
	require.Equal(t, before+5, after)
}

func TestConcurrentMetricInitialization(t *testing.T) {
	// Test that concurrent calls to initPrometheusMetrics don't cause issues
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			initPrometheusMetrics()
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify the metrics are still properly initialized
	require.NotNil(t, prometheusHealth)

	// Verify the metric still works
	prometheusHealth.Add(1)
	value := testutil.ToFloat64(prometheusHealth)
	require.Greater(t, value, 0.0)
}

func TestMetricNamespacing(t *testing.T) {
	// Initialize metrics
	initPrometheusMetrics()

	// Verify all metrics follow the correct naming convention
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	alertMetrics := make([]string, 0)
	for _, family := range metricFamilies {
		if strings.HasPrefix(family.GetName(), "teranode_alert_") {
			alertMetrics = append(alertMetrics, family.GetName())
		}
	}

	require.Contains(t, alertMetrics, "teranode_alert_health")

	// Verify all alert metrics follow the naming convention
	for _, metricName := range alertMetrics {
		require.True(t, strings.HasPrefix(metricName, "teranode_alert_"))
	}
}
