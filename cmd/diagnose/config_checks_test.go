package diagnose

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// findResult returns the ConfigResult for the given check label, or fails the test.
func findResult(t *testing.T, results []ConfigResult, label string) ConfigResult {
	t.Helper()

	for _, r := range results {
		if r.Check == label {
			return r
		}
	}

	require.FailNowf(t, "check not found", "no ConfigResult for %q", label)

	return ConfigResult{}
}

func TestCheckSecurityAdminAPIKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		severity Severity
		value    string
	}{
		{name: "placeholder is an error", key: "testkey", severity: SeverityERROR, value: "well-known placeholder"},
		{name: "placeholder is an error regardless of case", key: "ChangeMe", severity: SeverityERROR, value: "well-known placeholder"},
		{name: "weak key warns", key: "shortkey", severity: SeverityWARN},
		{name: "strong key is ok", key: "a-strong-random-admin-secret-value", severity: SeverityOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &settings.Settings{GRPCAdminAPIKey: tt.key}

			res := findResult(t, checkSecurity(s), labelGRPCAdminAPIKey)
			require.Equal(t, tt.severity, res.Severity)

			if tt.value != "" {
				require.Equal(t, tt.value, res.Value)
			}
		})
	}
}

// TestCheckP2PConfigListenAddresses pins that diagnose refuses the same listen
// addresses the P2P service refuses at startup. It used to report a narrowed,
// silently ignored value as OK.
func TestCheckP2PConfigListenAddresses(t *testing.T) {
	tests := []struct {
		name     string
		addrs    []string
		severity Severity
		value    string
	}{
		{name: "empty is an error", addrs: nil, severity: SeverityERROR, value: valueEmpty},
		{name: "wildcard is ok", addrs: []string{"0.0.0.0"}, severity: SeverityOK, value: "0.0.0.0"},
		{name: "dual-stack multiaddr is ok", addrs: []string{"/ip4/0.0.0.0/tcp/9905", "/ip6/::/tcp/9905"}, severity: SeverityOK},
		{name: "narrowed interface is an error", addrs: []string{"10.0.1.5"}, severity: SeverityERROR, value: "10.0.1.5"},
		{name: "port mismatch is an error", addrs: []string{"/ip4/0.0.0.0/tcp/9906"}, severity: SeverityERROR},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &settings.Settings{}
			s.P2P.Port = 9905
			s.P2P.ListenAddresses = tt.addrs

			res := findResult(t, checkP2PConfig(s), "P2P listen addresses")
			require.Equal(t, tt.severity, res.Severity)

			if tt.value != "" {
				require.Equal(t, tt.value, res.Value)
			}

			if tt.severity == SeverityERROR {
				require.Contains(t, res.Recommended, "p2p_listen_addresses")
			}
		})
	}

	t.Run("zero port is an error", func(t *testing.T) {
		s := &settings.Settings{}
		s.P2P.ListenAddresses = []string{"0.0.0.0"}

		res := findResult(t, checkP2PConfig(s), "P2P port")
		require.Equal(t, SeverityERROR, res.Severity)
	})
}
