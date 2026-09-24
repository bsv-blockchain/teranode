package util

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// gocoreConfigPassword is the canary planted in gocore's configuration.
const gocoreConfigPassword = "canary-gocore-config-password"

// TestGocoreStatsHandlersNeverServeConfig is the regression test for the
// profiler listener serving gocore's configuration page, which renders every
// resolved setting, store URL passwords included, to anyone who can reach the
// port. The listener has no authentication and binds every interface on the
// default profilerAddr.
func TestGocoreStatsHandlersNeverServeConfig(t *testing.T) {
	gocore.Config().Set("review_loop_canary_store", "postgres://teranode:"+gocoreConfigPassword+"@db:5432/teranode")

	prefix := gocore.GetStatPrefix()

	// gocore's own page carries the canary. Without this the assertions below
	// would pass just as well if gocore stopped rendering the setting.
	direct := httptest.NewRecorder()
	gocore.HandleConfig(direct, httptest.NewRequest(http.MethodGet, prefix+"config", nil))
	require.Contains(t, direct.Body.String(), gocoreConfigPassword, "gocore's config page no longer renders the canary, so this test checks nothing")

	mux := http.NewServeMux()
	RegisterGocoreStatsHandlers(mux)

	for _, p := range []string{prefix + "config", prefix + "config/", prefix + "./config", prefix + "stats/../config"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))

		require.NotContains(t, rec.Body.String(), gocoreConfigPassword, "%s served the configuration page", p)
		require.NotEqual(t, http.StatusOK, rec.Code, "%s answered 200", p)
	}

	// The stats page itself is still served.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, prefix+"stats", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), gocoreConfigPassword)
}

// TestNoDirectGocoreStatsRegistration fails when code outside this file
// mounts gocore's handlers itself, which would put the configuration page
// back on the listener. gocore registers once per process, so a direct call
// that runs first would also leave RegisterGocoreStatsHandlers serving an
// empty mux.
func TestNoDirectGocoreStatsRegistration(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	var offenders []string

	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", ".claude", ".worktrees":
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.HasSuffix(p, "gocore_stats.go") {
			return nil
		}

		b, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}

		if strings.Contains(string(b), "gocore.RegisterStatsHandlers(") || strings.Contains(string(b), "gocore.StartStatsServer(") {
			offenders = append(offenders, p)
		}

		return nil
	})
	require.NoError(t, err)
	require.Empty(t, offenders, "call util.RegisterGocoreStatsHandlers instead, which leaves out the configuration page")
}

// TestRegisterGocoreStatsHandlersDefaultMuxIsIdempotent pins that mounting on
// http.DefaultServeMux more than once, as a process running several daemons
// does, never registers the pattern twice. ServeMux panics on a duplicate.
func TestRegisterGocoreStatsHandlersDefaultMuxIsIdempotent(t *testing.T) {
	require.NotPanics(t, func() {
		RegisterGocoreStatsHandlers(nil)
		RegisterGocoreStatsHandlers(nil)
	})

	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, gocore.GetStatPrefix()+"config", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}
