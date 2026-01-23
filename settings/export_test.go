package settings

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

func TestExportMetadata(t *testing.T) {
	// Create a Settings instance with known values
	settings := &Settings{
		Version:             "1.0.0",
		Commit:              "abc123",
		TracingEnabled:      true,
		TracingSampleRate:   0.05,
		TracingCollectorURL: mustParseURL("http://collector:4318"),
		ClientName:          "test-client",
		DataFolder:          "/data",
		SecurityLevelHTTP:   1,
		LogLevel:            "DEBUG",
		PrettyLogs:          true,
		JSONLogging:         false,
		GRPCMaxRetries:      5,
		GRPCRetryBackoff:    2 * time.Second,
		ChainCfgParams:      &chaincfg.MainNetParams,
	}

	// Call ExportMetadata
	registry := settings.ExportMetadata()

	// Verify registry structure
	require.NotNil(t, registry)
	require.Equal(t, "1.0.0", registry.Version)
	require.Equal(t, "abc123", registry.Commit)
	require.NotEmpty(t, registry.Settings)
	require.NotEmpty(t, registry.Categories)

	// Create a map for easier lookup
	settingsMap := make(map[string]SettingMetadata)
	for _, setting := range registry.Settings {
		settingsMap[setting.Key] = setting
	}

	// Test tag-based settings with automatic field name (no conversion)
	t.Run("TracingEnabled", func(t *testing.T) {
		setting, ok := settingsMap["tracing_enabled"]
		require.True(t, ok, "tracing_enabled should be in the registry")
		// Name should be the field name as-is: "TracingEnabled"
		require.Equal(t, "TracingEnabled", setting.Name)
		require.Equal(t, "bool", setting.Type)
		require.Equal(t, "false", setting.DefaultValue)
		require.Equal(t, "true", setting.CurrentValue) // Runtime value
		require.Equal(t, "Enable OpenTelemetry distributed tracing", setting.Description)
		require.Equal(t, "Global", setting.Category)
		require.NotEmpty(t, setting.UsageHint)
	})

	t.Run("TracingSampleRate", func(t *testing.T) {
		setting, ok := settingsMap["tracing_SampleRate"]
		require.True(t, ok, "tracing_SampleRate should be in the registry")
		// Name should be the field name as-is: "TracingSampleRate"
		require.Equal(t, "TracingSampleRate", setting.Name)
		require.Equal(t, "float64", setting.Type)
		require.Equal(t, "0.01", setting.DefaultValue)
		require.Equal(t, "0.05", setting.CurrentValue) // Runtime value
		require.Equal(t, "Global", setting.Category)
	})

	t.Run("TracingCollectorURL", func(t *testing.T) {
		setting, ok := settingsMap["tracing_collector_url"]
		require.True(t, ok, "tracing_collector_url should be in the registry")
		// Name should be the field name as-is: "TracingCollectorURL"
		require.Equal(t, "TracingCollectorURL", setting.Name)
		require.Equal(t, "url", setting.Type)
		require.Equal(t, "http://collector:4318", setting.CurrentValue)
		require.Equal(t, "Global", setting.Category)
	})

	t.Run("LogLevel", func(t *testing.T) {
		setting, ok := settingsMap["logLevel"]
		require.True(t, ok, "logLevel should be in the registry")
		// Name should be the field name as-is: "LogLevel"
		require.Equal(t, "LogLevel", setting.Name)
		require.Equal(t, "string", setting.Type)
		require.Equal(t, "INFO", setting.DefaultValue)
		require.Equal(t, "DEBUG", setting.CurrentValue)
		require.Equal(t, "Global", setting.Category)
	})

	t.Run("GRPCRetryBackoff", func(t *testing.T) {
		setting, ok := settingsMap["grpc_retry_backoff"]
		require.True(t, ok, "grpc_retry_backoff should be in the registry")
		// Name should be the field name as-is: "GRPCRetryBackoff"
		require.Equal(t, "GRPCRetryBackoff", setting.Name)
		require.Equal(t, "duration", setting.Type)
		require.Equal(t, "2s", setting.CurrentValue)
		require.Equal(t, "Global", setting.Category)
	})

}

func TestExportMetadata_Caching(t *testing.T) {
	// Create two settings instances
	settings1 := &Settings{
		Version:        "1.0.0",
		Commit:         "abc123",
		LogLevel:       "DEBUG",
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	settings2 := &Settings{
		Version:        "1.0.1",
		Commit:         "def456",
		LogLevel:       "INFO",
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	// Call ExportMetadata on both
	registry1 := settings1.ExportMetadata()
	registry2 := settings2.ExportMetadata()

	// Both should have settings
	require.NotEmpty(t, registry1.Settings)
	require.NotEmpty(t, registry2.Settings)

	// Metadata structure should be the same (cached)
	require.Equal(t, len(registry1.Settings), len(registry2.Settings))

	// But current values should differ
	settingsMap1 := make(map[string]SettingMetadata)
	settingsMap2 := make(map[string]SettingMetadata)
	for _, s := range registry1.Settings {
		settingsMap1[s.Key] = s
	}
	for _, s := range registry2.Settings {
		settingsMap2[s.Key] = s
	}

	// LogLevel values should differ
	require.Equal(t, "DEBUG", settingsMap1["logLevel"].CurrentValue)
	require.Equal(t, "INFO", settingsMap2["logLevel"].CurrentValue)

	// Version and Commit should differ
	require.Equal(t, "1.0.0", registry1.Version)
	require.Equal(t, "1.0.1", registry2.Version)
	require.Equal(t, "abc123", registry1.Commit)
	require.Equal(t, "def456", registry2.Commit)
}

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}
