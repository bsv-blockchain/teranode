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

// knownDeadKeys are settings that are declared on Settings (and therefore
// advertised to operators by ExportMetadata) but never read by NewSettings, so
// configuring them does nothing at all. They are recorded here rather than
// fixed because each needs its own judgement — some may be deliberately
// retired, others are live bugs. blockassembly_txMapDirs was one of these until
// it cost ~100 GB of block-assembly heap per node on dev-ovh-1.
//
// Two of these are worth calling out as the same bug in the same feature:
// blockassembly_subtreeMmapDir and blockvalidation_subtreeMmapDir are the
// subtree half of the mmap work that blockassembly_txMapDirs was the TxMap half
// of. Any deployment that has mounted a volume for them is getting nothing.
//
// This list must only ever shrink. Delete an entry when you wire the key up.
var knownDeadKeys = map[string]struct{}{
	"postgres_circuitBreakerEnabled":               {},
	"postgres_circuitBreakerFailureThreshold":      {},
	"postgres_circuitBreakerHalfOpenMax":           {},
	"postgres_circuitBreakerCooldown":              {},
	"postgres_circuitBreakerFailureWindow":         {},
	"aerospike_enable_preserve_filter_expressions": {},
	"blockassembly_subtreeMmapDir":                 {},
	"blockchain_postgres_pool":                     {},
	"blockchain_raw_miner_tag":                     {},
	"blockchain_subscription_timeout":              {},
	"blockchain_peerRegistryStore":                 {},
	"blockchain_peerRegistrySaveInterval":          {},
	"blockvalidation_subtreeMmapDir":               {},
	"utxostore_postgres_pool":                      {},
	"p2p_peer_map_max_size":                        {},
	"p2p_peer_map_ttl":                             {},
	"p2p_peer_map_cleanup_interval":                {},
	"p2p_peer_registry_max_size":                   {},
	"p2p_peer_registry_ttl":                        {},
	"p2p_peer_registry_cleanup_interval":           {},
	"legacy_upnp":                                  {},
	"pruner_skipDuringCatchup":                     {},
	"pruner_force_ignore_block_persister_height":   {},
}

// Guard against the whole class of bug rather than this one instance: every
// field carrying a `key:"..."` struct tag is advertised to operators as
// configurable, so every such key must actually be read somewhere in the
// settings package. A tag with no corresponding lookup is a setting that
// silently does nothing — which is strictly worse than not offering it, because
// operators configure it, observe no effect, and go hunting elsewhere.
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

		if _, known := knownDeadKeys[key]; known {
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
// and returns every `key:"..."` tag value.
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

		if key, ok := field.Tag.Lookup("key"); ok && key != "" {
			keys = append(keys, key)
		}

		keys = append(keys, collectKeyTags(field.Type, seen)...)
	}

	return keys
}
