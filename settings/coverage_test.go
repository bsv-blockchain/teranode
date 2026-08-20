package settings

import (
	"reflect"
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

// TestNoMissingTags walks the Settings struct via reflection and fails if any
// leaf (non-struct) field lacks the "key" struct tag that extractFields (see
// export.go) relies on to expose settings via ExportMetadata. Nested structs
// declared within the settings package are walked recursively; structs from
// other packages (e.g. *chaincfg.Params) are opaque and left unchecked, since
// they are not ours to tag.
//
// A field that is deliberately not a setting (runtime-computed or internal)
// declares `key:"-"` (keyTagExempt) on itself and is skipped both here and by
// ExportMetadata. The exemption lives on the declaration, so it survives
// renaming or moving the field.
func TestNoMissingTags(t *testing.T) {
	settingsPkgPath := reflect.TypeOf(Settings{}).PkgPath()

	var missing []string

	var walk func(typ reflect.Type, prefix string)
	walk = func(typ reflect.Type, prefix string) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fieldName := prefix + field.Name

			// A non-empty tag is either a real setting key or the key:"-"
			// exemption; both mean this field needs no further checking.
			if field.Tag.Get("key") != "" {
				continue
			}

			fieldType := field.Type
			if fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}

			if fieldType.Kind() == reflect.Struct {
				// Structs declared outside the settings package (e.g.
				// *chaincfg.Params) are opaque - they are not ours to tag,
				// so neither recurse into them nor flag them as missing.
				if fieldType.PkgPath() != settingsPkgPath {
					continue
				}

				walk(fieldType, fieldName+".")
				continue
			}

			missing = append(missing, fieldName)
		}
	}

	walk(reflect.TypeOf(Settings{}), "")

	require.Empty(t, missing, `settings field(s) missing a "key" struct tag (add one, or tag the field key:"-" if it is genuinely not a setting)`)
}

// TestKeyTagExemptNotExported verifies the other half of the key:"-" contract:
// an exempt field is skipped by extractFields, so no metadata entry is emitted
// for it (in particular, no entry with the literal key "-", which would fail
// TestMetadataCoverage's required-field assertions).
func TestKeyTagExemptNotExported(t *testing.T) {
	settings := &Settings{
		ChainCfgParams: &chaincfg.MainNetParams,
		Commit:         "abc123",
		Version:        "1.0.0",
		Context:        "test",
		IsAllInOneMode: true,
	}

	registry := settings.ExportMetadata()

	exemptFieldNames := map[string]bool{}

	typ := reflect.TypeOf(Settings{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Tag.Get("key") == keyTagExempt {
			exemptFieldNames[typ.Field(i).Name] = true
		}
	}

	require.NotEmpty(t, exemptFieldNames, `expected at least one key:"-" field on Settings`)

	for _, setting := range registry.Settings {
		require.NotEqual(t, keyTagExempt, setting.Key, "exempt sentinel leaked into exported metadata as a setting key")
		require.False(t, exemptFieldNames[setting.Name], "exempt field %q was exported as a setting", setting.Name)
	}
}
