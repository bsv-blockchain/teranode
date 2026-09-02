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
