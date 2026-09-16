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
			mux := newProfilerMux(ulogger.TestLogger{}, &settings.Settings{ProfilerAddr: tt.addr})

			req := httptest.NewRequest(http.MethodGet, "/debug/memory", nil)

			// ServeMux.Handler reports the matched pattern; an empty pattern is
			// the built-in NotFound handler.
			_, pattern := mux.Handler(req)
			if tt.registered {
				require.Equal(t, "/debug/memory", pattern, "memory analyzer must be served on a loopback-bound profiler")
			} else {
				require.Empty(t, pattern, "memory analyzer must not be registered on a network-reachable profiler")
			}

			// End-to-end: the network-reachable listener must answer 404, not
			// the analyzer output and not an analyzer error.
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if tt.registered {
				require.NotEqual(t, http.StatusNotFound, rec.Code)
			} else {
				require.Equal(t, http.StatusNotFound, rec.Code)
			}

			// The rest of the profiler surface is unaffected by the address.
			_, pprofPattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil))
			require.Equal(t, "/debug/pprof/", pprofPattern)
		})
	}
}
