package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureKafkaAuth(t *testing.T) {
	tests := []struct {
		name          string
		kafkaSettings *settings.KafkaSettings
		expectError   bool
		checkConfig   func(*testing.T, *sarama.Config)
	}{
		{
			name:          "No authentication settings",
			kafkaSettings: &settings.KafkaSettings{},
			expectError:   false,
			checkConfig: func(t *testing.T, config *sarama.Config) {
				assert.False(t, config.Net.TLS.Enable)
			},
		},
		{
			name: "TLS enabled",
			kafkaSettings: &settings.KafkaSettings{
				EnableTLS:     true,
				TLSSkipVerify: false,
			},
			expectError: false,
			checkConfig: func(t *testing.T, config *sarama.Config) {
				assert.True(t, config.Net.TLS.Enable)
				assert.NotNil(t, config.Net.TLS.Config)
				assert.False(t, config.Net.TLS.Config.InsecureSkipVerify)
			},
		},
		{
			name: "TLS with skip verify",
			kafkaSettings: &settings.KafkaSettings{
				EnableTLS:     true,
				TLSSkipVerify: true,
			},
			expectError: false,
			checkConfig: func(t *testing.T, config *sarama.Config) {
				assert.True(t, config.Net.TLS.Enable)
				assert.NotNil(t, config.Net.TLS.Config)
				assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := sarama.NewConfig()
			err := configureKafkaAuth(config, tt.kafkaSettings)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.checkConfig(t, config)
			}
		})
	}
}

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

func TestKafkaAuthIntegration(t *testing.T) {
	// Test that TLS settings are properly integrated
	// This test verifies that the authentication functions work together

	kafkaSettings := &settings.KafkaSettings{
		EnableTLS:     true,
		TLSSkipVerify: true,
	}

	// Test validation
	err := ValidateKafkaAuthSettings(kafkaSettings)
	require.NoError(t, err)

	// Test configuration
	config := sarama.NewConfig()
	err = configureKafkaAuth(config, kafkaSettings)
	require.NoError(t, err)

	// Verify configuration
	assert.True(t, config.Net.TLS.Enable)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
}
