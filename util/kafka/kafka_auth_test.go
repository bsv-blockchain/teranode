package kafka

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
)

func TestValidateKafkaAuthSettings(t *testing.T) {
	tests := []struct {
		name          string
		kafkaSettings *settings.KafkaSettings
		expectError   bool
	}{
		{
			name:          "No TLS enabled",
			kafkaSettings: &settings.KafkaSettings{},
			expectError:   false,
		},
		{
			name: "TLS enabled",
			kafkaSettings: &settings.KafkaSettings{
				EnableTLS:     true,
				TLSSkipVerify: false,
			},
			expectError: false,
		},
		{
			name: "TLS enabled with skip verify",
			kafkaSettings: &settings.KafkaSettings{
				EnableTLS:     true,
				TLSSkipVerify: true,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKafkaAuthSettings(tt.kafkaSettings)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
