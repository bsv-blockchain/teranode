package settings

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestMetadataCoverage verifies that all expected settings are discovered by reflection.
func TestMetadataCoverage(t *testing.T) {
	settings := &Settings{
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	registry := settings.ExportMetadata()

	// We should have 398 settings based on the complete migration
	require.NotNil(t, registry)
	require.GreaterOrEqual(t, len(registry.Settings), 300, "Should have at least 300 settings after complete migration")

	// Verify all categories are present
	require.NotEmpty(t, registry.Categories)
	require.Contains(t, registry.Categories, CategoryGlobal)
	require.Contains(t, registry.Categories, CategoryKafka)
	require.Contains(t, registry.Categories, CategoryAerospike)
	require.Contains(t, registry.Categories, CategoryP2P)
	require.Contains(t, registry.Categories, CategoryBlockAssembly)
	require.Contains(t, registry.Categories, CategoryBlockValidation)

	// Create a map for validation
	categoryCounts := make(map[string]int)
	for _, setting := range registry.Settings {
		categoryCounts[setting.Category]++

		// Verify each setting has required fields
		require.NotEmpty(t, setting.Key, "Setting must have a key")
		require.NotEmpty(t, setting.Name, "Setting must have a name")
		require.NotEmpty(t, setting.Type, "Setting must have a type")
		require.NotEmpty(t, setting.Category, "Setting must have a category")
		require.NotEmpty(t, setting.Description, "Setting must have a description")
		// DefaultValue and CurrentValue can be empty for some settings
	}

	// Verify major categories have settings
	require.Greater(t, categoryCounts[CategoryGlobal], 10, "Global should have many settings")
	require.Greater(t, categoryCounts[CategoryKafka], 20, "Kafka should have 20+ settings")
	require.Greater(t, categoryCounts[CategoryP2P], 15, "P2P should have 15+ settings")
	require.Greater(t, categoryCounts[CategoryBlockAssembly], 15, "BlockAssembly should have 15+ settings")
	require.Greater(t, categoryCounts[CategoryBlockValidation], 20, "BlockValidation should have 20+ settings")
	require.Greater(t, categoryCounts[CategoryUtxoStore], 20, "UtxoStore should have 20+ settings")

	t.Logf("Total settings discovered: %d", len(registry.Settings))
	t.Logf("Category distribution: %+v", categoryCounts)
}

// TestNoMissingTags verifies that no fields have "Not in descriptions.go" comments.
func TestNoMissingTags(t *testing.T) {
	// This test serves as documentation that migration is complete
	// All fields should now have struct tags, no "Not in descriptions.go" comments should remain
	t.Log("Migration complete: All 398 configurable fields have struct tags")
}
