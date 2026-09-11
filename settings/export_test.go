package settings

import (
	"fmt"
	"net/url"
	"reflect"
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

func TestExportMetadata_SecretRedaction(t *testing.T) {
	settings := &Settings{
		Version:        "1.0.0",
		Commit:         "abc123",
		ChainCfgParams: &chaincfg.MainNetParams,
		RPC: RPCSettings{
			RPCPass:      "super-secret-password",
			RPCLimitPass: "limited-secret-password",
		},
		P2P: P2PSettings{
			PrivateKey: "deadbeef1234567890",
		},
	}

	registry := settings.ExportMetadata()
	require.NotNil(t, registry)

	settingsMap := make(map[string]SettingMetadata)
	for _, s := range registry.Settings {
		settingsMap[s.Key] = s
	}

	// Verify sensitive keys are redacted
	for key := range sensitiveKeys {
		if setting, ok := settingsMap[key]; ok {
			if setting.CurrentValue != "" {
				require.Equal(t, redactedValue, setting.CurrentValue,
					"sensitive key %q should be redacted", key)
			}
		}
	}

	// Verify specific keys we set are redacted
	require.Equal(t, redactedValue, settingsMap["rpc_pass"].CurrentValue)
	require.Equal(t, redactedValue, settingsMap["rpc_limit_pass"].CurrentValue)
	require.Equal(t, redactedValue, settingsMap["p2p_private_key"].CurrentValue)

	// Verify non-sensitive keys are NOT redacted
	logLevel := settingsMap["logLevel"]
	require.NotEqual(t, redactedValue, logLevel.CurrentValue)
}

// keyTagCollisionCategory distinguishes why a `key:` tag collision is
// exempted, so a future reader (and the test failure message) can tell
// instantly whether they're looking at a bug being tracked for removal or a
// deliberate design choice that is expected to persist.
type keyTagCollisionCategory string

const (
	// categoryAccidentalBug marks a collision where two fields have
	// different semantics/defaults and one masks the other in
	// ExportMetadata()'s flat key->default map - a genuine bug, exempted
	// here only until the underlying duplicate field is deleted.
	categoryAccidentalBug keyTagCollisionCategory = "accidental collision (bug, temporary exemption)"
	// categoryIntentionalSharing marks a collision where two fields are
	// deliberately wired to the same config key because two independent
	// services each need their own settings.<Service>.Field accessor for
	// the same value - they never diverge, so the masking in
	// ExportMetadata()'s flat map is harmless. This exemption is permanent
	// unless someone collapses the fields (see AdaptiveFetch precedent
	// below).
	categoryIntentionalSharing keyTagCollisionCategory = "intentional cross-service sharing (permanent exemption)"
)

// keyTagCollisionExemption documents one specific, named collision that the
// uniqueness check below is allowed to pass over. Each entry must name
// precisely one known collision and both colliding field paths - never a
// broad escape hatch.
type keyTagCollisionExemption struct {
	fieldPaths []string
	category   keyTagCollisionCategory
	reason     string
}

var keyTagCollisionExemptions = map[string]keyTagCollisionExemption{
	"blockmaxsize": {
		fieldPaths: []string{"PolicySettings.BlockMaxSize", "BlockSettings.MaxSize"},
		category:   categoryAccidentalBug,
		reason: "PolicySettings.BlockMaxSize (default 0, controls what this node mines) and " +
			"BlockSettings.MaxSize (default 4294967296, controls what it accepts) are different settings " +
			"that accidentally share key:\"blockmaxsize\" (see settings_doc_test.go's docDefaultsExemptions " +
			"entry for the same key, which documents the fallout in ExportMetadata()'s flat key->default map). " +
			"BlockSettings.MaxSize is removed by the pending fix/remove-dead-block-maxsize-setting change; " +
			"delete this exemption once that lands.",
	},
	"blockvalidation_processTxMetaUsingStore_BatchSize": {
		fieldPaths: []string{"BlockSettings.ProcessTxMetaUsingStoreBatchSize", "BlockValidationSettings.ProcessTxMetaUsingStoreBatchSize"},
		category:   categoryIntentionalSharing,
		reason: "BlockSettings.ProcessTxMetaUsingStoreBatchSize is read by blockpersister " +
			"(processTxMetaUsingStore.go:54, streaming_process_subtree.go:122); " +
			"BlockValidationSettings.ProcessTxMetaUsingStoreBatchSize is read by subtreevalidation and asset " +
			"(check_block_subtrees.go:1341, processTxMetaUsingStore.go:83, GetLegacyBlock.go:583). Both fields " +
			"are populated from the same getInt(\"blockvalidation_processTxMetaUsingStore_BatchSize\", 1024, ...) " +
			"call in settings.go, so they never diverge, and the sharing is already documented as **SHARED** at " +
			"docs/references/settings/services/blockpersister_settings.md:23. This is a deliberate choice, not a " +
			"bug: each service gets its own settings.<Service>.Field accessor for the same underlying value. The " +
			"AdaptiveFetch precedent (settings/interface.go, hoisted to the root of Settings for exactly this " +
			"cross-service-sharing reason) is the alternative if anyone later wants to collapse these two fields " +
			"into one - until then, this exemption stays.",
	},
}

// collectKeyTagFieldPaths walks typ recursively, recording the dotted
// TypeName.FieldName path of every field carrying a non-empty `key:` tag,
// keyed by that tag value.
func collectKeyTagFieldPaths(typ reflect.Type, paths map[string][]string) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldType := field.Type

		key := field.Tag.Get("key")
		if key == "" {
			if fieldType.Kind() == reflect.Pointer && fieldType.Elem().Kind() == reflect.Struct {
				collectKeyTagFieldPaths(fieldType.Elem(), paths)
			} else if fieldType.Kind() == reflect.Struct {
				collectKeyTagFieldPaths(fieldType, paths)
			}
			continue
		}

		fieldPath := fmt.Sprintf("%s.%s", typ.Name(), field.Name)
		paths[key] = append(paths[key], fieldPath)
	}
}

// TestSettingsKeyTagsAreUnique guards against the exact failure mode caused
// by the "blockmaxsize" collision: two struct fields sharing the same `key:`
// tag silently overwrite each other's entry in ExportMetadata()'s flat map,
// so the settings portal ends up publishing one of the two fields' metadata
// under both fields' identity. Every `key:` tag on Settings must therefore
// be unique, except for the narrowly-scoped, explicitly named exemptions
// above.
func TestSettingsKeyTagsAreUnique(t *testing.T) {
	paths := make(map[string][]string)
	collectKeyTagFieldPaths(reflect.TypeOf(Settings{}), paths)

	for key, fieldPaths := range paths {
		if len(fieldPaths) <= 1 {
			continue
		}

		if exempt, ok := keyTagCollisionExemptions[key]; ok {
			require.ElementsMatch(t, exempt.fieldPaths, fieldPaths,
				"exemption for key %q (%s) lists %v but the actual colliding fields are %v - update the exemption",
				key, exempt.category, exempt.fieldPaths, fieldPaths)
			continue
		}

		t.Errorf("duplicate key tag %q is used by %d struct fields: %v", key, len(fieldPaths), fieldPaths)
	}
}

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}
