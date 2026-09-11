package settings

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// blockvalidationDocPath returns the path to the blockvalidation settings
// reference doc and to the settings package directory it is checked against.
func blockvalidationDocPath(t *testing.T) (docPath, settingsDir string) {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	settingsDir = filepath.Dir(thisFile)
	repoRoot := filepath.Dir(settingsDir)

	return filepath.Join(repoRoot, "docs", "references", "settings", "services", "blockvalidation_settings.md"), settingsDir
}

// docTableRowRe matches a settings table row of the reference doc:
// | FieldName | Type | Default | env_var_key | Usage |
var docTableRowRe = regexp.MustCompile(`(?m)^\|\s*([A-Za-z0-9_]+)\s*\|\s*([^|]+?)\s*\|\s*([^|]*?)\s*\|\s*([A-Za-z0-9_]+)\s*\|`)

// cpuHalfRe matches the ways the docs spell the max(4, NumCPU/2) default, which
// the struct tags spell as "auto".
var cpuHalfRe = regexp.MustCompile(`^max\(\s*4\s*,\s*CPU\s*/\s*2\s*\)$`)

// canonicalDefault normalises a documented or struct-tag default so the two can
// be compared: surrounding quotes are dropped, the CPU-derived default is folded
// onto "auto", durations are parsed, and integers are compared numerically.
func canonicalDefault(raw string) string {
	v := strings.TrimSpace(raw)
	v = strings.Trim(v, "`")
	v = strings.TrimSpace(v)

	if len(v) >= 2 && strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) {
		v = v[1 : len(v)-1]
	}

	if cpuHalfRe.MatchString(v) {
		return "auto"
	}

	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return strconv.FormatInt(n, 10)
	}

	if d, err := time.ParseDuration(v); err == nil {
		return d.String()
	}

	return v
}

// TestBlockvalidationSettingsDocDefaultsMatchCode walks every row of the
// blockvalidation settings reference table and cross-checks the documented
// default against the real default from the settings package (the struct tag
// default surfaced by ExportMetadata()). Two rows in this table were documented
// with each other's values for a long time while a guard existed for a single
// hand-picked setting - this covers the whole table instead.
func TestBlockvalidationSettingsDocDefaultsMatchCode(t *testing.T) {
	docPath, _ := blockvalidationDocPath(t)

	docBytes, err := os.ReadFile(docPath)
	require.NoError(t, err, "reading settings reference doc")

	s := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	codeDefaults := make(map[string]string)
	for _, entry := range s.ExportMetadata().Settings {
		codeDefaults[entry.Key] = entry.DefaultValue
	}

	rows := docTableRowRe.FindAllStringSubmatch(string(docBytes), -1)
	require.NotEmpty(t, rows, "no settings table rows found in %s", docPath)

	checked := 0

	for _, row := range rows {
		field, docDefault, key := row[1], row[3], row[4]

		// Only the settings table has an env-var key that the settings package
		// knows about; other tables in the doc (service dependencies, validation
		// rules) are skipped.
		codeDefault, ok := codeDefaults[key]
		if !ok {
			continue
		}

		checked++

		require.Equal(t, canonicalDefault(codeDefault), canonicalDefault(docDefault),
			"%s documents %s (%s) with default %q, but the settings package defaults it to %q",
			filepath.Base(docPath), field, key, docDefault, codeDefault)
	}

	// Sanity check that the row regex still matches the table; a doc reformat
	// that silently stops matching would otherwise turn this test into a no-op.
	require.Greater(t, checked, 50,
		"only %d blockvalidation settings rows were cross-checked - has the table format changed?", checked)
}

// exampleKeyRe matches a `blockvalidation_...=value` assignment in the doc's
// configuration examples.
var exampleKeyRe = regexp.MustCompile(`(?m)^(blockvalidation_[A-Za-z0-9_]+)=`)

// TestBlockvalidationSettingsDocExampleKeysExist checks that every setting key
// used in the doc's configuration examples is a real key the settings package
// reads. A misspelled key in an example is silently ignored at runtime, so the
// operator gets the default and no error.
func TestBlockvalidationSettingsDocExampleKeysExist(t *testing.T) {
	docPath, _ := blockvalidationDocPath(t)

	docBytes, err := os.ReadFile(docPath)
	require.NoError(t, err, "reading settings reference doc")

	s := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	knownKeys := make(map[string]struct{})
	for _, entry := range s.ExportMetadata().Settings {
		knownKeys[entry.Key] = struct{}{}
	}

	matches := exampleKeyRe.FindAllStringSubmatch(string(docBytes), -1)
	require.NotEmpty(t, matches, "no configuration example keys found in %s", docPath)

	for _, match := range matches {
		key := match[1]
		_, ok := knownKeys[key]
		require.True(t, ok,
			"%s uses %q in a configuration example, but no such setting key exists", filepath.Base(docPath), key)
	}
}

// TestOptimisticMiningDocMatchesCodeDefault guards against the settings reference
// doc drifting away from the code default for blockvalidation_optimistic_mining.
// It reads the documented default two ways - the settings table row in
// docs/references/settings/services/blockvalidation_settings.md, and the struct
// tag default surfaced via ExportMetadata() - and cross-checks both against the
// runtime default literal passed to getBool() in settings.go, so a change to
// either side without the other fails the test instead of silently drifting.
func TestOptimisticMiningDocMatchesCodeDefault(t *testing.T) {
	const settingKey = "blockvalidation_optimistic_mining"

	// 1. Documented default from the settings table.
	docPath, settingsDir := blockvalidationDocPath(t)
	docBytes, err := os.ReadFile(docPath)
	require.NoError(t, err, "reading settings reference doc")

	tableRowRe := regexp.MustCompile(`(?m)^\|\s*OptimisticMining\s*\|\s*bool\s*\|\s*(true|false)\s*\|\s*` + regexp.QuoteMeta(settingKey) + `\s*\|`)
	matches := tableRowRe.FindStringSubmatch(string(docBytes))
	require.Len(t, matches, 2, "could not find OptimisticMining settings table row in %s", docPath)
	docDefault := matches[1] == "true"

	// 2. Struct tag default, exposed via ExportMetadata() (settings/export.go).
	s := &Settings{ChainCfgParams: &chaincfg.MainNetParams}
	registry := s.ExportMetadata()

	var structTagDefault string
	found := false
	for _, entry := range registry.Settings {
		if entry.Key == settingKey {
			structTagDefault = entry.DefaultValue
			found = true
			break
		}
	}
	require.True(t, found, "setting %s not found in ExportMetadata()", settingKey)
	require.Contains(t, []string{"true", "false"}, structTagDefault,
		"struct tag default for %s is not a bool literal: %q", settingKey, structTagDefault)

	// 3. Runtime default literal passed to getBool() in settings.go.
	settingsGoPath := filepath.Join(settingsDir, "settings.go")
	settingsGoBytes, err := os.ReadFile(settingsGoPath)
	require.NoError(t, err, "reading settings.go")

	runtimeDefaultRe := regexp.MustCompile(`getBool\(\s*"` + regexp.QuoteMeta(settingKey) + `"\s*,\s*(true|false)\s*[,)]`)
	runtimeMatches := runtimeDefaultRe.FindStringSubmatch(string(settingsGoBytes))
	require.Len(t, runtimeMatches, 2, "could not find getBool default for %s in settings.go", settingKey)
	runtimeDefault := runtimeMatches[1] == "true"

	require.Equal(t, runtimeDefault, docDefault,
		"docs/references/settings/services/blockvalidation_settings.md documents OptimisticMining default as %v, "+
			"but settings.go defaults it to %v", docDefault, runtimeDefault)
	require.Equal(t, runtimeDefault, structTagDefault == "true",
		"struct tag default (%s) for %s disagrees with the settings.go runtime default (%v)",
		structTagDefault, settingKey, runtimeDefault)

	// 4. Stale-prose guard: these phrases described optimistic mining incorrectly (they named
	// script validation, rather than the block-level checks, as the deferred work) and were
	// corrected once already. If either the doc or the struct tag longdesc regresses back to one
	// of these phrases, fail even though the boolean default above still matches everywhere.
	stalePhrases := []string{
		"before full script validation completes",
		"Full script validation continues in parallel",
		"subtree validation runs in background",
	}

	blockvalidationSettingsGoPath := filepath.Join(settingsDir, "blockvalidation_settings.go")
	blockvalidationSettingsGoBytes, err := os.ReadFile(blockvalidationSettingsGoPath)
	require.NoError(t, err, "reading blockvalidation_settings.go")

	for _, phrase := range stalePhrases {
		require.NotContains(t, string(docBytes), phrase,
			"stale optimistic-mining prose is back in the settings doc: %q", phrase)
		require.NotContains(t, string(blockvalidationSettingsGoBytes), phrase,
			"stale optimistic-mining prose is back in the longdesc struct tag: %q", phrase)
	}
}
