package daemon

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestNewProfilerMuxMemoryAnalyzerLoopbackOnly guards the ASLR-defeat fix: the
// memory analyzer prints exact process address mappings, so it must be absent
// (404) from every profiler listener that is reachable from the network and
// present only on loopback-bound listeners (the .dev default).
//
// Also checks that Prometheus registration follows the caller's decision and
// is not derived from settings inside the builder.
func TestNewProfilerMuxMemoryAnalyzerLoopbackOnly(t *testing.T) {
	tests := []struct {
		name       string
		addr       string
		registered bool
	}{
		{name: "wildcard port only (non-dev default)", addr: ":9091", registered: false},
		{name: "ipv4 unspecified", addr: "0.0.0.0:9091", registered: false},
		{name: "ipv6 unspecified", addr: "[::]:9091", registered: false},
		{name: "routable interface", addr: "10.0.0.5:9091", registered: false},
		{name: "hostname", addr: "teranode1:9091", registered: false},
		{name: "localhost (dev default)", addr: "localhost:9091", registered: true},
		{name: "ipv4 loopback", addr: "127.0.0.1:9091", registered: true},
		{name: "ipv6 loopback", addr: "[::1]:9091", registered: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No Prometheus endpoint and no stats prefix: the builder is pure, so
			// this test touches neither the metricsRegistered atomic nor gocore's
			// global stats handlers that other daemon tests depend on.
			mux := newProfilerMux(ulogger.TestLogger{}, &settings.Settings{ProfilerAddr: tt.addr}, "")

			req := httptest.NewRequest(http.MethodGet, "/debug/memory", nil)

			// ServeMux.Handler reports the matched pattern; an empty pattern is
			// the built-in NotFound handler.
			_, pattern := mux.Handler(req)
			if tt.registered {
				require.Equal(t, "/debug/memory", pattern, "memory analyzer must be served on a loopback-bound profiler")
			} else {
				require.Empty(t, pattern, "memory analyzer must not be registered on a network-reachable profiler")
			}

			// End-to-end on the network-reachable listener: the route must
			// answer 404. The registered case is not served here because the
			// real handler reads /proc/self/smaps (500 on non-Linux).
			if !tt.registered {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
				require.Equal(t, http.StatusNotFound, rec.Code)
			}

			// The rest of the profiler surface is unaffected by the address.
			_, pprofPattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil))
			require.Equal(t, "/debug/pprof/", pprofPattern)
		})
	}
}

// TestNewProfilerMuxPrometheusEndpointFromCaller ensures the builder mounts the
// Prometheus handler exactly where the caller says, and nowhere when told not
// to, even if settings carry an endpoint.
func TestNewProfilerMuxPrometheusEndpointFromCaller(t *testing.T) {
	appSettings := &settings.Settings{ProfilerAddr: ":9091", PrometheusEndpoint: "/metrics"}

	_, pattern := newProfilerMux(ulogger.TestLogger{}, appSettings, "").Handler(httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Empty(t, pattern, "builder must not register /metrics when the caller did not claim it")

	_, pattern = newProfilerMux(ulogger.TestLogger{}, appSettings, "/metrics").Handler(httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, "/metrics", pattern)
}
