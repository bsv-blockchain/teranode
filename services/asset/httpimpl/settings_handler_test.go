package httpimpl

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
)

func TestIsSensitiveKey(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		// Sensitive keys
		{"p2p_private_key", true},
		{"alert_p2p_private_key", true},
		{"coinbase_wallet_private_key", true},
		{"coinbaseDBUserPwd", true},
		{"database_password", true},
		{"slack_token", true},
		{"api_key_external", true},
		{"auth_key_setting", true},
		{"secret_mining_threshold", true},
		{"credential_store", true},

		// Non-sensitive keys (case variations)
		{"P2P_PRIVATE_KEY", true},
		{"Private_Key_Test", true},
		{"PASSWORD_FIELD", true},

		// Non-sensitive keys
		{"p2p_port", false},
		{"max_connections", false},
		{"kafka_brokers", false},
		{"block_size", false},
		{"log_level", false},
		{"node_name", false},
		{"public_key", false}, // public keys are fine to expose
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := isSensitiveKey(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRedactSensitiveSettings(t *testing.T) {
	input := []settings.SettingMetadata{
		{
			Key:          "p2p_private_key",
			Name:         "P2P Private Key",
			CurrentValue: "abc123secretkey",
			Category:     "P2P",
		},
		{
			Key:          "p2p_port",
			Name:         "P2P Port",
			CurrentValue: "9905",
			Category:     "P2P",
		},
		{
			Key:          "database_password",
			Name:         "Database Password",
			CurrentValue: "supersecretpassword",
			Category:     "Database",
		},
		{
			Key:          "slack_token",
			Name:         "Slack Token",
			CurrentValue: "xoxb-12345-67890",
			Category:     "Alerts",
		},
		{
			Key:          "empty_secret",
			Name:         "Empty Secret",
			CurrentValue: "",
			Category:     "Test",
		},
		{
			Key:          "max_connections",
			Name:         "Max Connections",
			CurrentValue: "100",
			Category:     "Global",
		},
	}

	result := redactSensitiveSettings(input)

	// Verify length is same
	assert.Len(t, result, len(input))

	// Verify sensitive values are redacted
	assert.Equal(t, "[REDACTED]", result[0].CurrentValue, "p2p_private_key should be redacted")
	assert.Equal(t, "[REDACTED]", result[2].CurrentValue, "database_password should be redacted")
	assert.Equal(t, "[REDACTED]", result[3].CurrentValue, "slack_token should be redacted")

	// Verify non-sensitive values are preserved
	assert.Equal(t, "9905", result[1].CurrentValue, "p2p_port should not be redacted")
	assert.Equal(t, "100", result[5].CurrentValue, "max_connections should not be redacted")

	// Verify empty sensitive values stay empty (not redacted to "[REDACTED]")
	assert.Equal(t, "", result[4].CurrentValue, "empty secrets should remain empty")

	// Verify original is not modified
	assert.Equal(t, "abc123secretkey", input[0].CurrentValue, "original should not be modified")
}
