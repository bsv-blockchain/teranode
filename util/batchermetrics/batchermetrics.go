// Package batchermetrics exposes a single shared go-batcher v2 metrics
// provider for all teranode services. The provider registers its histograms
// and counters with prometheus.DefaultRegisterer so it follows the same
// convention as the rest of the project (promauto.*).
package batchermetrics

import (
	"sync"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "teranode"
	subsystem = "batcher"
)

var (
	once     sync.Once
	provider batcher.Metrics
)

// Provider returns the shared batcher.Metrics instance, lazily constructed
// on first call. All callers receive the same provider, so the underlying
// Prometheus collectors are registered exactly once.
func Provider() batcher.Metrics {
	once.Do(func() {
		provider = batcher.NewPrometheusMetrics(prometheus.DefaultRegisterer, namespace, subsystem)
	})
	return provider
}
