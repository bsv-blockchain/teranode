package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// TestGetSettings_DoesNotLeakDatastorePassword drives the real settings endpoint with a datastore
// password in the documented production syntax.
//
// The endpoint's own filter is name-based, and none of these keys look like a secret - nothing in
// `blockchain_store` matches `password`, `token` or `api_key` - so before the structural fix the
// password was serialised in full and any script running in the dashboard's origin could read it
// with one same-origin fetch (bitcoin-sv/teranode#4844).
func TestGetSettings_DoesNotLeakDatastorePassword(t *testing.T) {
	const marker = "audit-secret-marker"

	mustParse := func(raw string) *url.URL {
		u, err := url.Parse(raw)
		require.NoError(t, err)

		return u
	}

	tSettings := settings.NewSettings()
	tSettings.BlockChain.StoreURL = mustParse("postgres://audit-user:" + marker + "@db.internal:5432/chain")
	tSettings.Coinbase.Store = mustParse("postgres://audit-user:" + marker + "@db.internal:5432/coinbase")
	tSettings.Alert.StoreURL = mustParse("postgres://audit-user:" + marker + "@db.internal:5432/alert")
	tSettings.UtxoStore.UtxoStore = mustParse("aerospike://db.internal:3000/utxo?password=" + marker)

	handler := NewSettingsHandler(tSettings, ulogger.TestLogger{})

	for _, query := range []string{"", "search=audit"} {
		t.Run("query="+query, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/settings?"+query, nil)
			rec := httptest.NewRecorder()

			require.NoError(t, handler.GetSettings(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			// The search filter matches against CurrentValue, which is a distinct path into the
			// same data - a leak there would be reachable without knowing any key name.
			require.NotContains(t, rec.Body.String(), marker,
				"a datastore credential reached the settings endpoint")
		})
	}
}
