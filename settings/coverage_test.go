package settings

import (
	"net/url"
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

// settingsWalk walks typ via reflection, invoking visit(fieldName, field,
// fieldType) for every leaf field - i.e. every field that is not itself a
// struct declared within the settings package. A field tagged `key:"-"`
// (keyTagExempt) is treated as fully exempt: it is neither visited nor (if it
// is a struct) descended into, mirroring extractFields (see export.go), which
// emits no metadata entry and does not recurse for such a field.
//
// A struct field declared within the settings package is always descended
// into, regardless of whether it also carries a real `key` tag: such a tag
// documents the struct as a single override setting (e.g.
// BlockChain.PostgresPool), but its children still need their own tags
// checked. A struct from another package (e.g. *url.URL, *chaincfg.Params) is
// opaque to extractFields' recursion in exactly the same way a `key:"-"`
// field is - it is not ours to walk into - so it is treated as a leaf here
// too, and must itself carry a tag.
func settingsWalk(settingsPkgPath string, typ reflect.Type, prefix string, visit func(fieldName string, field reflect.StructField, fieldType reflect.Type)) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldName := prefix + field.Name

		if field.Tag.Get("key") == keyTagExempt {
			continue
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct && fieldType.PkgPath() == settingsPkgPath {
			settingsWalk(settingsPkgPath, fieldType, fieldName+".", visit)
			continue
		}

		visit(fieldName, field, fieldType)
	}
}

// TestNoMissingTags walks the Settings struct via reflection and fails if any
// leaf field lacks the "key" struct tag that extractFields (see export.go)
// relies on to expose settings via ExportMetadata. Nested structs declared
// within the settings package are walked recursively, regardless of whether
// they also carry a real key tag; anything else - including a struct from
// another package such as *url.URL or *chaincfg.Params - is a leaf and must
// itself carry a tag.
//
// A field that is deliberately not a setting (runtime-computed or internal)
// declares `key:"-"` (keyTagExempt) on itself and is skipped both here and by
// ExportMetadata. The exemption lives on the declaration, so it survives
// renaming or moving the field.
func TestNoMissingTags(t *testing.T) {
	settingsPkgPath := reflect.TypeOf(Settings{}).PkgPath()

	var missing []string

	settingsWalk(settingsPkgPath, reflect.TypeOf(Settings{}), "", func(fieldName string, field reflect.StructField, _ reflect.Type) {
		if field.Tag.Get("key") == "" {
			missing = append(missing, fieldName)
		}
	})

	require.Empty(t, missing, `settings field(s) missing a "key" struct tag (add one, or tag the field key:"-" if it is genuinely not a setting)`)
}

// TestNoMissingTags_CatchesDroppedURLTag is a regression test for the walk
// above: it proves the guard actually fails when a tag is dropped from a
// *url.URL field, rather than silently treating the foreign url.URL struct as
// an opaque, unchecked container (which is what a PkgPath-based skip would
// do, since url.URL itself declares no "key" tags).
func TestNoMissingTags_CatchesDroppedURLTag(t *testing.T) {
	type structWithUntaggedURL struct {
		StoreURL *url.URL // deliberately no `key` tag
	}

	settingsPkgPath := reflect.TypeOf(Settings{}).PkgPath()

	var missing []string

	settingsWalk(settingsPkgPath, reflect.TypeOf(structWithUntaggedURL{}), "", func(fieldName string, field reflect.StructField, _ reflect.Type) {
		if field.Tag.Get("key") == "" {
			missing = append(missing, fieldName)
		}
	})

	require.Equal(t, []string{"StoreURL"}, missing,
		"an untagged *url.URL field must be reported as missing a key tag, not silently skipped as a foreign struct")
}

// TestKeyTagExemptNotExported verifies the other half of the key:"-" contract:
// an exempt field is skipped by extractFields, so no metadata entry is emitted
// for it (in particular, no entry with the literal key "-", which would fail
// TestMetadataCoverage's required-field assertions).
//
// Exempt fields are collected with the same recursive walk TestNoMissingTags
// uses (rather than a flat loop over Settings' top-level fields), so a nested
// key:"-" field is covered too, and are compared against extractFields' own
// dotted FieldName rather than the bare display name, so an unrelated setting
// that happens to share a leaf field name (e.g. a future nested "Version")
// cannot collide with the exempt set.
func TestKeyTagExemptNotExported(t *testing.T) {
	settings := &Settings{
		ChainCfgParams: &chaincfg.MainNetParams,
		Commit:         "abc123",
		Version:        "1.0.0",
		Context:        "test",
		IsAllInOneMode: true,
	}

	settingsPkgPath := reflect.TypeOf(Settings{}).PkgPath()

	exemptFieldPaths := map[string]bool{}
	settingsWalkExempt(settingsPkgPath, reflect.TypeOf(Settings{}), "", exemptFieldPaths)

	require.NotEmpty(t, exemptFieldPaths, `expected at least one key:"-" field on Settings`)

	registry := settings.ExportMetadata()
	for _, setting := range registry.Settings {
		require.NotEqual(t, keyTagExempt, setting.Key, "exempt sentinel leaked into exported metadata as a setting key")
	}

	for _, entry := range extractMetadataStructure() {
		require.False(t, exemptFieldPaths[entry.FieldName], "exempt field %q was exported as a setting", entry.FieldName)
	}
}

// settingsWalkExempt recursively collects the dotted field path of every
// key:"-" field in typ, descending into settings-package structs the same way
// extractFields does (a real key on a struct field stops extractFields from
// recursing, so it stops here too - only the keyTagExempt sentinel itself is
// what needs collecting, and it can appear at any depth).
func settingsWalkExempt(settingsPkgPath string, typ reflect.Type, prefix string, exempt map[string]bool) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldName := prefix + field.Name
		key := field.Tag.Get("key")

		if key == keyTagExempt {
			exempt[fieldName] = true
			continue
		}

		if key != "" {
			continue
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct && fieldType.PkgPath() == settingsPkgPath {
			settingsWalkExempt(settingsPkgPath, fieldType, fieldName+".", exempt)
		}
	}
}
