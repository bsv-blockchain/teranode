package util

import (
	"net/http"
	"path"
	"sync"

	"github.com/ordishs/gocore"
)

var (
	gocoreStatsOnce sync.Once
	gocoreStatsMux  *http.ServeMux

	defaultMuxStatsOnce sync.Once

	// statsMuxes records the non-default muxes the stats pages are already
	// mounted on. A mux is held for the life of the process, which is the
	// life of the listener that serves it.
	statsMuxesMu sync.Mutex
	statsMuxes   = map[*http.ServeMux]struct{}{}
)

// gocoreStatsHandler returns gocore's stats pages behind a guard that refuses
// the configuration page.
//
// gocore.RegisterStatsHandlers mounts <prefix>config next to the stats pages.
// That page renders every resolved setting and masks only values stored in
// *EHE* form, so a plaintext store URL such as postgres://user:password@host
// is served verbatim. The profiler listener that carries these handlers has
// no authentication and binds every interface on the default profilerAddr,
// so the page would hand the store passwords to anyone who can reach the port.
// teranode cannot re-render the page redacted, because gocore does not export
// the rows it builds, so the page is not served at all.
//
// gocore registers its handlers once per process, so they go onto one private
// mux that every listener shares.
func gocoreStatsHandler() http.Handler {
	gocoreStatsOnce.Do(func() {
		gocoreStatsMux = http.NewServeMux()
		gocore.RegisterStatsHandlers(gocoreStatsMux)
	})

	configPath := gocore.GetStatPrefix() + "config"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path.Clean(r.URL.Path) == configPath {
			http.NotFound(w, r)
			return
		}

		gocoreStatsMux.ServeHTTP(w, r)
	})
}

// RegisterGocoreStatsHandlers mounts gocore's stats pages on mux, leaving out
// the configuration page, which would serve store credentials to anyone who
// can reach the listener. A nil mux means http.DefaultServeMux. Each mux is
// mounted at most once however many times this is called, because
// http.ServeMux panics when the same pattern is registered twice.
//
// Call this in place of gocore.RegisterStatsHandlers.
func RegisterGocoreStatsHandlers(mux *http.ServeMux) {
	if mux == nil {
		defaultMuxStatsOnce.Do(func() {
			http.DefaultServeMux.Handle(gocore.GetStatPrefix(), gocoreStatsHandler())
		})

		return
	}

	statsMuxesMu.Lock()
	defer statsMuxesMu.Unlock()

	if _, mounted := statsMuxes[mux]; mounted {
		return
	}

	mux.Handle(gocore.GetStatPrefix(), gocoreStatsHandler())
	statsMuxes[mux] = struct{}{}
}
