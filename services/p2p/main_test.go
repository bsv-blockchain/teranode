package p2p

import (
	"os"
	"testing"
)

// TestMain registers the p2p Prometheus metrics once for the whole package
// before any test runs. Production registration happens lazily via
// initPrometheusMetrics(), called from NewServer; many tests in this package
// construct *Server directly (bypassing NewServer) and exercise ban/catchup/
// websocket code paths that record metrics, so tests need their own
// entry point into registration instead of relying on a package init().
func TestMain(m *testing.M) {
	initPrometheusMetrics()
	os.Exit(m.Run())
}
