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

	// This is a regression floor, not an exact count: the real count as of
	// writing is 656 and grows as settings are added. It only needs to stay
	// comfortably below the true count so it catches a reflection walk that
	// stops discovering settings (e.g. a broken recursion), not every
	// individual addition or removal.
	require.NotNil(t, registry)
	require.GreaterOrEqual(t, len(registry.Settings), 600, "Should have at least 600 settings after complete migration")

	// PostgresPool fields document a *PostgresSettings struct as a single
	// opaque override setting: extractFields only recurses into a nested
	// struct field when its own key tag is empty, so dropping either tag
	// below silently stops emitting the struct-level setting and instead
	// flattens its children into keys that already exist elsewhere - both
	// changes TestNoMissingTags cannot see, since its walk always descends
	// into settings-package structs regardless of their own key tag.
	exportedKeys := make(map[string]bool, len(registry.Settings))
	for _, setting := range registry.Settings {
		exportedKeys[setting.Key] = true
	}
	require.True(t, exportedKeys["blockchain_postgres_pool"], "BlockChainSettings.PostgresPool must keep its key tag")
	require.True(t, exportedKeys["utxostore_postgres_pool"], "UtxoStoreSettings.PostgresPool must keep its key tag")

	// The two require.True calls above pin the two known struct-typed
	// override settings by name, which gives a precise, readable failure
	// message when one breaks. But that pair does not maintain itself: a
	// third *SomeSettings override field added later reopens the hole with
	// both calls staying green. The obvious generalization - "every
	// settings-package struct field carrying a real key tag must be in the
	// registry" - passes vacuously, since it is conditioned on the tag
	// existing: deleting the tag deletes the assertion along with it. This
	// census assertion instead enumerates every struct-typed override field
	// directly, independent of whether its tag survived, so it fails both on
	// a drop and on a future undocumented addition.
	settingsPkgPath := reflect.TypeOf(Settings{}).PkgPath()

	var structKeyed []string
	settingsWalkStructKeys(settingsPkgPath, reflect.TypeOf(Settings{}), "", &structKeyed)
	require.ElementsMatch(t,
		[]string{"BlockChain.PostgresPool", "UtxoStore.PostgresPool"}, structKeyed,
		`the set of struct-typed override settings changed; add the new key to the assertions above`)

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
// (keyTagExempt) is instead reported to visitExempt(fieldName, field), if
// non-nil, and is never descended into even if it is itself a struct,
// mirroring extractFields (see export.go), which emits no metadata entry and
// does not recurse for such a field. Most callers only care about
// non-exempt leaf fields and pass visitExempt as nil.
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
	settingsWalkExemptAware(settingsPkgPath, typ, prefix, visit, nil)
}

// settingsWalkExemptAware is settingsWalk plus a visitExempt callback for
// key:"-" fields; see settingsWalk's doc comment for the shared recursion
// rule. It exists so exempt-field collection (TestKeyTagExemptNotExported)
// and leaf-tag collection (TestNoMissingTags) share exactly one traversal
// rule instead of two independently written walks that can drift apart on
// what counts as "descend into this struct".
func settingsWalkExemptAware(settingsPkgPath string, typ reflect.Type, prefix string, visit func(fieldName string, field reflect.StructField, fieldType reflect.Type), visitExempt func(fieldName string, field reflect.StructField)) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldName := prefix + field.Name

		if field.Tag.Get("key") == keyTagExempt {
			if visitExempt != nil {
				visitExempt(fieldName, field)
			}
			continue
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct && fieldType.PkgPath() == settingsPkgPath {
			settingsWalkExemptAware(settingsPkgPath, fieldType, fieldName+".", visit, visitExempt)
			continue
		}

		visit(fieldName, field, fieldType)
	}
}

// settingsWalkStructKeys appends to out the dotted field path of every
// settings-package struct field that also carries a real (non-exempt) `key`
// tag - i.e. a struct documented as a single override setting, such as
// "BlockChain.PostgresPool" (see settingsWalk's doc comment above). It
// mirrors settingsWalk's own descent rule (skip key:"-", always recurse into
// a same-package struct regardless of its own tag) so the two cannot drift
// apart on what counts as "this struct is walked"; the only difference is
// that this walk records the struct field itself, rather than only visiting
// leaves, since settingsWalk never invokes visit for a field it recurses
// into.
func settingsWalkStructKeys(settingsPkgPath string, typ reflect.Type, prefix string, out *[]string) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldName := prefix + field.Name
		key := field.Tag.Get("key")

		if key == keyTagExempt {
			continue
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct && fieldType.PkgPath() == settingsPkgPath {
			if key != "" {
				*out = append(*out, fieldName)
			}
			settingsWalkStructKeys(settingsPkgPath, fieldType, fieldName+".", out)
		}
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
// Exempt fields are collected via settingsWalkExemptAware - the same
// recursive walk TestNoMissingTags uses via settingsWalk - rather than a
// separately written walk, so a nested key:"-" field (e.g. one added inside
// PostgresSettings, which settingsWalk always descends into regardless of
// its own key tag) is covered too. A previous, independently written walker
// here bailed out of recursing into any struct field carrying a non-empty
// key tag, so it would visit-and-skip such a struct (e.g.
// BlockChain.PostgresPool) at TestNoMissingTags' walk, yet never even
// descend into it here - silently failing to collect a nested key:"-" field
// and weakening this test rather than failing it. Collected paths are
// compared against extractFields' own dotted FieldName rather than the bare
// display name, so an unrelated setting that happens to share a leaf field
// name (e.g. a future nested "Version") cannot collide with the exempt set.
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
	settingsWalkExemptAware(settingsPkgPath, reflect.TypeOf(Settings{}), "", func(string, reflect.StructField, reflect.Type) {}, func(fieldName string, _ reflect.StructField) {
		exemptFieldPaths[fieldName] = true
	})

	require.NotEmpty(t, exemptFieldPaths, `expected at least one key:"-" field on Settings`)

	registry := settings.ExportMetadata()
	for _, setting := range registry.Settings {
		require.NotEqual(t, keyTagExempt, setting.Key, "exempt sentinel leaked into exported metadata as a setting key")
	}

	for _, entry := range extractMetadataStructure() {
		require.False(t, exemptFieldPaths[entry.FieldName], "exempt field %q was exported as a setting", entry.FieldName)
	}
}
