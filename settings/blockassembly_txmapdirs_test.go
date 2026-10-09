package settings

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// blockassembly_txMapDirs shipped as a dead key: the struct field and its
// documentation existed, but nothing in NewSettings ever read the config value
// into it. TxMapDirs was therefore always nil, so BlockAssembler's
// `len(tSettings.BlockAssembly.TxMapDirs) > 0` guard never fired and the
// disk-backed TxMap could not be enabled by any configuration. On dev-ovh-1
// that left SplitTxInpointsMap holding ~57-70% of block-assembly's live heap
// (~100 GB/node) with the intended replacement mounted but empty.
func TestBlockAssemblyTxMapDirs_EnvIsRead(t *testing.T) {
	t.Setenv("blockassembly_txMapDirs", "/data/txmap/d0|/data/txmap/d1")

	tSettings := NewSettings()

	require.Equal(t, []string{"/data/txmap/d0", "/data/txmap/d1"}, tSettings.BlockAssembly.TxMapDirs,
		"pipe-separated paths must reach the setting, or the disk-backed TxMap can never be enabled")
}

// Single path is the documented single-disk mode; it must still produce a
// non-empty slice so the enable guard fires.
func TestBlockAssemblyTxMapDirs_SinglePath(t *testing.T) {
	t.Setenv("blockassembly_txMapDirs", "/data/txmap/d0")

	tSettings := NewSettings()

	require.Equal(t, []string{"/data/txmap/d0"}, tSettings.BlockAssembly.TxMapDirs)
}

// Unset must stay empty — that is what selects the in-memory SplitTxInpointsMap.
func TestBlockAssemblyTxMapDirs_DefaultEmpty(t *testing.T) {
	tSettings := NewSettings()

	require.Empty(t, tSettings.BlockAssembly.TxMapDirs,
		"default must be empty so the in-memory map stays the default behaviour")
}

// blockassembly_subtreeMmapDir and blockvalidation_subtreeMmapDir were dead the
// same way blockassembly_txMapDirs was: both fields are declared with full
// documentation and both are consumed downstream -- BlockAssembler passes
// SubtreeMmapDir to subtreeprocessor.WithMmapDir, BlockValidation copies it to
// its own mmapDir and branches on it in subtreeFromBytesWithMmap -- but
// NewSettings never read either one. Both were therefore always "", so the
// `!= ""` guards never fired and NewTreeByLeafCountMmap / NewSubtreeFromReaderMmap
// were unreachable by any configuration. As with TxMapDirs there is no fallback
// log line to find it by, because the mmap path is never entered at all.
func TestBlockAssemblySubtreeMmapDir_EnvIsRead(t *testing.T) {
	t.Setenv("blockassembly_subtreeMmapDir", "/data/subtree-mmap")

	tSettings := NewSettings()

	require.Equal(t, "/data/subtree-mmap", tSettings.BlockAssembly.SubtreeMmapDir,
		"the path must reach the setting, or mmap-backed subtree Nodes can never be enabled")
}

func TestBlockValidationSubtreeMmapDir_EnvIsRead(t *testing.T) {
	t.Setenv("blockvalidation_subtreeMmapDir", "/data/subtree-mmap")

	tSettings := NewSettings()

	require.Equal(t, "/data/subtree-mmap", tSettings.BlockValidation.SubtreeMmapDir,
		"the path must reach the setting, or mmap-backed subtree loading can never be enabled")
}

// Unset must stay empty: that is what selects heap-allocated Nodes, which is the
// documented default and the behaviour every existing deployment has today.
func TestSubtreeMmapDir_DefaultEmpty(t *testing.T) {
	tSettings := NewSettings()

	require.Empty(t, tSettings.BlockAssembly.SubtreeMmapDir,
		"default must be empty so heap-allocated Nodes stay the default behaviour")
	require.Empty(t, tSettings.BlockValidation.SubtreeMmapDir,
		"default must be empty so heap-allocated Nodes stay the default behaviour")
}

// Guard against the whole class of bug rather than this one instance: every
// field carrying a `key:"..."` struct tag is advertised to operators as
// configurable, so every such key must actually be read somewhere in the
// settings package. A tag with no corresponding lookup is a setting that
// silently does nothing — which is strictly worse than not offering it, because
// operators configure it, observe no effect, and go hunting elsewhere.
//
// This check once parked unfixable keys in a knownDeadKeys allowlist (added
// with this test in #1803, emptied by #1811). The allowlist is gone: a scalar
// key is either wired up or its tag is deleted, and a struct-typed key is a
// settings group (see isSettingsGroup in export.go), which neither export nor
// this check treats as a setting.
func TestNoNewDeadSettingKeys(t *testing.T) {
	src, err := readSettingsPackageSource()
	require.NoError(t, err)

	var dead []string

	for _, key := range collectKeyTags(reflect.TypeOf(Settings{}), map[reflect.Type]bool{}) {
		// Must match a CALL argument -- getString("key", ...) -- not the
		// `key:"..."` struct tag that declared it. Searching for the bare
		// quoted key matches the tag itself and makes this test vacuous.
		if strings.Contains(src, `("`+key+`"`) {
			continue
		}

		dead = append(dead, key)
	}

	require.Empty(t, dead,
		"these keys are declared on Settings but never read by NewSettings, so configuring them does nothing: %v", dead)
}

// readSettingsPackageSource concatenates the non-test Go source of this package,
// which is where every config lookup lives.
func readSettingsPackageSource() (string, error) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		return "", err
	}

	var b strings.Builder

	for _, name := range entries {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		data, err := os.ReadFile(name)
		if err != nil {
			return "", err
		}

		b.Write(data)
	}

	return b.String(), nil
}

// collectKeyTags walks Settings (including nested and pointer-to-struct fields)
// and returns every `key:"..."` tag value that names a settable setting.
// Struct-typed keys are settings groups (isSettingsGroup): they are not
// exported as settings and cannot be read as scalars, so they are not
// collected here — their child fields still are.
func collectKeyTags(typ reflect.Type, seen map[reflect.Type]bool) []string {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct || seen[typ] {
		return nil
	}

	seen[typ] = true

	var keys []string

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if key, ok := field.Tag.Lookup("key"); ok && key != "" && !isSettingsGroup(field.Type) {
			keys = append(keys, key)
		}

		keys = append(keys, collectKeyTags(field.Type, seen)...)
	}

	return keys
}
