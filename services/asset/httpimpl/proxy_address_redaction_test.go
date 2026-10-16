package httpimpl

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestNewNeverLogsPropagationProxyAddressPassword pins the Errorf line New
// writes when asset_propagation_proxy_address does not parse. url.Parse embeds
// the whole address in its error, and its bare reason quotes part of it too: a
// space in the host is quoted as the invalid character, and a raw "/" in the
// password ends the authority there, so everything before it is quoted as an
// invalid port. The full password is never in that reason, so the assertion
// is on the part that used to be.
func TestNewNeverLogsPropagationProxyAddressPassword(t *testing.T) {
	tests := []struct {
		name    string
		address string
		leaked  string
	}{
		{"space in host", "http://teranode:canary-proxy-password@proxy host:8833", "canary-proxy"},
		{"raw slash in password", "http://teranode:canary-proxy/password@proxyhost:8833", "canary-proxy"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := &captureLogger{}

			s := &settings.Settings{
				Asset: settings.AssetSettings{
					APIPrefix:               "/api/v1",
					PropagationProxyEnabled: true,
					PropagationProxyAddress: tc.address,
				},
			}

			_, err := New(logger, s, &repository.Repository{}, nil)
			require.NoError(t, err)

			var proxyLine string

			for _, line := range logger.errors {
				require.NotContains(t, line, tc.leaked, "a log line carries part of the proxy address password")

				if strings.HasPrefix(line, "[Asset] failed to parse asset_propagation_proxy_address") {
					proxyLine = line
				}
			}

			require.NotEmpty(t, proxyLine, "New no longer logs the parse failure, so this test checks nothing")
		})
	}
}
