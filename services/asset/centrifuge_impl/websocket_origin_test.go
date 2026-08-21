package centrifuge_impl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// The origin predicate itself is covered once, in
// util.TestWebsocketOriginChecker. These tests cover only what is specific to
// the Asset service: which origins it feeds that predicate.

// TestCentrifuge_wsAllowedOrigins_DevOriginsOnlyOnLoopback verifies the
// dev-server escape hatch is gated on the listen address rather than on
// dashboard_devServerPorts, whose default is non-empty in every settings
// context. Appending it unconditionally would leave
// http(s)://localhost:5173/:4173 permanently allowlisted on every production
// node, so any page an operator loads from one of those local ports could
// open websockets to any node their browser can reach.
func TestCentrifuge_wsAllowedOrigins_DevOriginsOnlyOnLoopback(t *testing.T) {
	newCentrifuge := func(listenAddress string) *Centrifuge {
		return &Centrifuge{
			settings: &settings.Settings{
				Asset: settings.AssetSettings{HTTPListenAddress: listenAddress},
				Dashboard: settings.DashboardSettings{
					DevServerPorts: []int{5173, 4173},
				},
			},
		}
	}

	allowsDevOrigin := func(c *Centrifuge) bool {
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8090/connection/websocket", nil)
		req.Host = "localhost:8090"
		req.Header.Set("Origin", "http://localhost:5173")

		return util.WebsocketOriginChecker(c.wsAllowedOrigins())(req)
	}

	require.True(t, allowsDevOrigin(newCentrifuge("127.0.0.1:8090")),
		"a loopback-bound asset server must still allow the dev server so `make dev` keeps working")

	require.False(t, allowsDevOrigin(newCentrifuge(":8090")),
		"a wildcard-bound asset server is network-reachable, so dev origins must not be allowlisted")

	require.False(t, allowsDevOrigin(newCentrifuge("10.0.0.5:8090")),
		"a network-bound asset server must not allowlist dev origins")
}

// TestCentrifuge_wsAllowedOrigins_IncludesConfiguredOrigins verifies the
// operator-configured asset_wsAllowedOrigins reach the checker regardless of
// the listen address.
func TestCentrifuge_wsAllowedOrigins_IncludesConfiguredOrigins(t *testing.T) {
	c := &Centrifuge{
		settings: &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPListenAddress: ":8090",
				WSAllowedOrigins:  []string{"https://dashboard.example.com"},
			},
		},
	}

	require.Equal(t, []string{"https://dashboard.example.com"}, c.wsAllowedOrigins())
}

// TestCentrifuge_wsAllowedOrigins_NilSettings verifies the nil-settings path
// is safe and yields no extra origins (same-host only).
func TestCentrifuge_wsAllowedOrigins_NilSettings(t *testing.T) {
	c := &Centrifuge{}
	require.Nil(t, c.wsAllowedOrigins())
}
