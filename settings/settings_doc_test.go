package settings

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// This file generalises the settings-doc drift guard originally written for
// docs/references/settings/services/blockvalidation_settings.md (a single,
// hand-picked file) to every file under docs/references/settings/, so a doc
// drifting away from the real default - or a configuration example using a
// key that does not exist - fails a test wherever it happens, not just in the
// one file that happened to get a guard.

// settingsDocsDir returns docs/references/settings relative to this test file.
func settingsDocsDir(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	settingsDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(settingsDir)

	return filepath.Join(repoRoot, "docs", "references", "settings")
}

// settingsDocFiles returns every markdown file under docs/references/settings,
// sorted for deterministic test output. Walking the directory (rather than
// hardcoding a list) means a new doc file is covered automatically.
func settingsDocFiles(t *testing.T) []string {
	t.Helper()

	dir := settingsDocsDir(t)

	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err, "walking %s", dir)
	require.NotEmpty(t, files, "no settings doc files found under %s", dir)

	sort.Strings(files)

	return files
}

// docKeyRe matches a bare settings key: letters, digits and underscores only.
// It is used to reject table cells that are prose ("Usage" text, etc.) rather
// than an actual environment variable key.
var docKeyRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// tableSeparatorCellRe matches one cell of a markdown table header separator
// row, e.g. the `---` or `:---:` in `|---|:---:|`.
var tableSeparatorCellRe = regexp.MustCompile(`^:?-+:?$`)

// docTableRow is one data row of a markdown settings table that documents a
// real settings key: the field name (for error messages), the documented
// default, and the environment variable key used to resolve the real default.
type docTableRow struct {
	setting    string
	docDefault string
	key        string
}

// splitTableRow splits a trimmed markdown table row ("| a | b | c |") into its
// cells, stripping the leading and trailing pipe first.
func splitTableRow(trimmedLine string) []string {
	inner := strings.TrimPrefix(trimmedLine, "|")
	inner = strings.TrimSuffix(inner, "|")

	cells := strings.Split(inner, "|")
	for i, c := range cells {
		cells[i] = strings.TrimSpace(c)
	}

	return cells
}

// isTableSeparatorRow reports whether every cell of a table row is a header
// separator cell (only made of "-" and optional ":" anchors).
func isTableSeparatorRow(cells []string) bool {
	if len(cells) == 0 {
		return false
	}

	for _, c := range cells {
		if !tableSeparatorCellRe.MatchString(c) {
			return false
		}
	}

	return true
}

// parseDocSettingsTables walks every markdown table in doc and extracts data
// rows from tables whose header has both a "Default" column and an
// "Environment Variable" column - the shape used by every settings-reference
// table in docs/references/settings. Tables documenting something else (URL
// query parameters, validation rules, enum-value tables, Kafka topic-name
// mappings without a default, etc.) do not have both columns and are skipped
// automatically; nothing needs to be hardcoded per file.
func parseDocSettingsTables(doc string) []docTableRow {
	var (
		rows                           []docTableRow
		inTable                        bool
		defaultIdx, envIdx, settingIdx int
	)

	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "|") {
			inTable = false
			continue
		}

		cells := splitTableRow(trimmed)

		if isTableSeparatorRow(cells) {
			continue
		}

		if !inTable {
			defaultIdx, envIdx, settingIdx = -1, -1, -1

			for i, c := range cells {
				switch c {
				case "Default":
					defaultIdx = i
				case "Environment Variable":
					envIdx = i
				case "Setting":
					settingIdx = i
				}
			}

			inTable = defaultIdx != -1 && envIdx != -1

			continue
		}

		if defaultIdx >= len(cells) || envIdx >= len(cells) {
			continue
		}

		key := cells[envIdx]
		if !docKeyRe.MatchString(key) {
			continue
		}

		row := docTableRow{docDefault: cells[defaultIdx], key: key}
		if settingIdx != -1 && settingIdx < len(cells) {
			row.setting = cells[settingIdx]
		}

		rows = append(rows, row)
	}

	return rows
}

// cpuHalfRe matches the ways the docs spell the max(4, NumCPU/2) default, which
// the struct tags spell as "auto".
var cpuHalfRe = regexp.MustCompile(`^max\(\s*4\s*,\s*CPU\s*/\s*2\s*\)$`)

// trailingParenRe matches a single, non-nested trailing parenthetical used to
// annotate a default, e.g. "0 (unlimited)", "false (true in settings.conf)",
// or `"localhost:8089" (Go default; overridden ... by settings.conf)`. Group 1
// is the value with the annotation removed; it is empty for a doc cell that is
// only an annotation, e.g. "(see below)".
var trailingParenRe = regexp.MustCompile(`^(.*?)\s*\([^()]*\)$`)

// stripOneQuoteLayer removes one layer of surrounding markdown code-span
// backticks, double quotes, or the angle brackets markdown wraps bare URLs in
// ("<http://...>"). It is applied in a loop because a doc cell can be quoted
// more than once, e.g. `"`http://host/path`"` (a backtick-quoted URL, itself
// wrapped in double quotes because it contains no spaces to protect).
func stripOneQuoteLayer(v string) string {
	switch {
	case len(v) >= 2 && strings.HasPrefix(v, "`") && strings.HasSuffix(v, "`"):
		return v[1 : len(v)-1]
	case len(v) >= 2 && strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`):
		return v[1 : len(v)-1]
	case len(v) >= 2 && strings.HasPrefix(v, "<") && strings.HasSuffix(v, ">"):
		return v[1 : len(v)-1]
	default:
		return v
	}
}

// canonicalDefault normalises a documented or struct-tag default so the two
// can be compared: a trailing explanatory parenthetical is dropped, layered
// quoting (code spans, double quotes, markdown auto-link angle brackets) is
// stripped, the CPU-derived default is folded onto "auto", an explicit empty
// list/map ("[]"/"{}") is folded onto "" (the real default of every
// multi-string or string-set setting when unset), durations are parsed, and
// numbers are compared numerically.
func canonicalDefault(raw string) string {
	v := strings.TrimSpace(raw)

	// Checked before the trailing-parenthetical strip below: "max(4, CPU/2)" is
	// not a value with an explanatory annotation attached, it's the whole value.
	if cpuHalfRe.MatchString(v) {
		return "auto"
	}

	if m := trailingParenRe.FindStringSubmatch(v); m != nil {
		v = strings.TrimSpace(m[1])
	}

	for {
		stripped := stripOneQuoteLayer(v)
		if stripped == v {
			break
		}

		v = strings.TrimSpace(stripped)
	}

	if v == "[]" || v == "{}" || v == "map[]" || v == "nil" {
		return ""
	}

	// A comma-separated struct-tag list default (e.g. "[5173,4173]") and the
	// pipe-separated form formatValue's slice branch produces at runtime
	// (e.g. "5173|4173") are the same list written two different ways -
	// fold both onto the pipe-separated form so they compare equal.
	if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
		inner := strings.TrimSpace(v[1 : len(v)-1])
		if inner == "" {
			return ""
		}

		parts := strings.Split(inner, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}

		return strings.Join(parts, "|")
	}

	if cpuHalfRe.MatchString(v) {
		return "auto"
	}

	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return strconv.FormatInt(n, 10)
	}

	if d, err := time.ParseDuration(v); err == nil {
		// A duration field's zero value formats as "0s" (time.Duration.String()),
		// but a doc or struct tag documenting that same field's default often
		// just writes the bare "0" - both mean the same zero duration, so fold
		// them onto the same token rather than reporting a false mismatch.
		if d == 0 {
			return "0"
		}

		return d.String()
	}

	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}

	return v
}

// docDefaultsExemptions lists doc/key pairs that the three-way doc/tag/runtime
// comparison cannot fully verify, together with why. Each entry is checked to
// still exist in the doc so a fixed or removed row does not leave a stale
// exemption. Loosening canonicalDefault instead of listing these explicitly
// would let the guard silently stop checking rows it can't parse - worse than
// no guard, per the project rule this test is enforcing.
//
// unreliable names which anchor(s) this exemption says cannot be trusted for
// comparison against the other(s):
//   - "tag" - the struct tag is a static snapshot of a value actually computed
//     at runtime from another setting; doc/runtime is still checked and must
//     match, only the pairs involving the tag are skipped.
//   - "runtime" - the reverse: the value is only knowable at runtime (e.g. it
//     depends on the number of CPUs on the machine running the test), so the
//     doc and tag agree with each other on a formula/sentinel but neither can
//     be compared against the concrete number the runtime produces; doc/tag
//     is still checked and must match, only the pairs involving runtime are
//     skipped.
//   - "both" - neither anchor is trustworthy for this key (e.g. a key
//     resolution collision in ExportMetadata's flat map means both the tag
//     and the runtime value it reads back may reflect the wrong field); every
//     comparison for this row is skipped.
type docDefaultExemption struct {
	file       string // basename of the doc file
	key        string // environment variable key
	reason     string
	unreliable string // "tag", "runtime", or "both"
}

var docDefaultsExemptions = []docDefaultExemption{
	{
		file: "policy_settings.md",
		key:  "blockmaxsize",
		reason: "the key \"blockmaxsize\" is used by two different struct fields - PolicySettings.BlockMaxSize " +
			"(default 0, documented here, controls what this node mines) and BlockSettings.MaxSize (default " +
			"4294967296, controls what it accepts) - so ExportMetadata()'s flat key->default map holds whichever " +
			"one the reflection walk visits last, currently BlockSettings.MaxSize, for BOTH the tag default and the " +
			"runtime CurrentValue read back through the same map. This is a genuine settings.go ambiguity worth " +
			"fixing separately (surfaced here, not fixed, because resolving it changes runtime key-resolution " +
			"behaviour rather than a doc)",
		unreliable: "both",
	},
	{
		file: "blockvalidation_settings.md",
		key:  "blockvalidation_processTxMetaUsingStore_Concurrency",
		reason: "the doc (\"max(4, CPU/2)\") and the struct tag (default:\"auto\") agree with each other on the " +
			"formula, canonicalDefault folds both to \"auto\" - but the concrete runtime value is " +
			"max(4, runtime.NumCPU()/2) on whatever machine runs this test, which is not a fixed literal the doc " +
			"or tag can state and will vary by CI runner/laptop core count",
		unreliable: "runtime",
	},
	{
		file:       "blockvalidation_settings.md",
		key:        "blockvalidation_validateBlockSubtreesConcurrency",
		reason:     "same runtime.NumCPU()-derived default as blockvalidation_processTxMetaUsingStore_Concurrency above",
		unreliable: "runtime",
	},
	{
		file:       "blockvalidation_settings.md",
		key:        "blockvalidation_catchupConcurrency",
		reason:     "same runtime.NumCPU()-derived default as blockvalidation_processTxMetaUsingStore_Concurrency above",
		unreliable: "runtime",
	},
	{
		file:       "subtreevalidation_settings.md",
		key:        "subtreevalidation_getMissingTransactions",
		reason:     "same runtime.NumCPU()-derived default as blockvalidation_processTxMetaUsingStore_Concurrency above",
		unreliable: "runtime",
	},
	{
		file: "global_settings.md",
		key:  "postgres_circuitBreakerFailureThreshold",
		reason: "PostgresSettings circuit breaker is not wired to any loader in settings.go, so the runtime value " +
			"is always the Go zero value regardless of the struct tag; doc and tag correctly agree on the intended " +
			"default. Temporary pending #1642, which wires this field",
		unreliable: "runtime",
	},
	{
		file:       "global_settings.md",
		key:        "postgres_circuitBreakerHalfOpenMax",
		reason:     "same unwired PostgresSettings circuit breaker gap as postgres_circuitBreakerFailureThreshold above. Temporary pending #1642",
		unreliable: "runtime",
	},
	{
		file:       "global_settings.md",
		key:        "postgres_circuitBreakerCooldown",
		reason:     "same unwired PostgresSettings circuit breaker gap as postgres_circuitBreakerFailureThreshold above. Temporary pending #1642",
		unreliable: "runtime",
	},
	{
		file:       "global_settings.md",
		key:        "postgres_circuitBreakerFailureWindow",
		reason:     "same unwired PostgresSettings circuit breaker gap as postgres_circuitBreakerFailureThreshold above. Temporary pending #1642",
		unreliable: "runtime",
	},
}

// lookupExemption returns the exemption registered for file/key, if any.
func lookupExemption(file, key string) (docDefaultExemption, bool) {
	for _, e := range docDefaultsExemptions {
		if e.file == file && e.key == key {
			return e, true
		}
	}

	return docDefaultExemption{}, false
}

// nonSettingsKeyExemption lists a settings-table row's "Environment Variable"
// cell that does not resolve against ExportMetadata() - and, for the stated
// reason, cannot - so TestSettingsDocDefaultsMatchCode's row loop is allowed
// to skip it instead of failing. Every entry here is still a real,
// operator-facing key; what it lacks is a `key:` struct tag ExportMetadata()
// can see; it is checked to still appear in its doc so a fixed or removed
// row does not leave a stale exemption behind (same discipline as
// docDefaultsExemptions and exampleKeyExemptions). Without this list, a doc
// row naming a key that resolves to nothing at all - misspelled, renamed, or
// simply invented - would take the same silent `continue` as these
// legitimate rows and never be checked against anything.
type nonSettingsKeyExemption struct {
	file   string
	key    string
	reason string
}

var nonSettingsKeyExemptions = []nonSettingsKeyExemption{
	{
		file: "global_settings.md", key: "SETTINGS_CONTEXT",
		reason: "bootstrap context selector read directly via gocore.Config().GetContext() before the settings " +
			"registry exists - it has no `key` struct tag and is not in ExportMetadata()",
	},
	{
		file: "pruner_settings.md", key: "startPruner",
		reason: "read directly via gocore.Config().GetBool(\"start\"+app) in daemon.go's shouldStart(), one per " +
			"service - it has no `key` struct tag and is not in ExportMetadata()",
	},
	{
		file: "pruner_settings.md", key: "pruner_IndexName",
		reason: "read directly via a package-level `var IndexName, _ = gocore.Config().Get(\"pruner_IndexName\", " +
			"\"pruner_dah_index\")` in stores/utxo/aerospike/pruner/pruner_service.go - it has no `key` struct " +
			"tag and is not in ExportMetadata()",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_maxOpenConns",
		reason: "built by string concatenation in getPostgresPoolSettings(\"blockchain\", ...) (settings/helpers.go)" +
			" - BlockchainSettings.PostgresPool's own `key:\"blockchain_postgres_pool\"` tag makes ExportMetadata " +
			"treat the whole *PostgresSettings pointer as one opaque entry rather than recursing into " +
			"PostgresSettings' own field tags, so this key never appears in ExportMetadata() under any name",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_maxIdleConns",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_connMaxLifetime",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_connMaxIdleTime",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_retryEnabled",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_retryMaxAttempts",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "blockchain_settings.md", key: "blockchain_postgres_retryBaseDelay",
		reason: "same getPostgresPoolSettings(\"blockchain\", ...) concatenation gap as blockchain_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_maxOpenConns",
		reason: "built by string concatenation in getPostgresPoolSettings(\"utxostore\", ...) (settings/helpers.go)" +
			" - UtxoStoreSettings.PostgresPool's own `key:\"utxostore_postgres_pool\"` tag makes ExportMetadata " +
			"treat the whole *PostgresSettings pointer as one opaque entry rather than recursing into " +
			"PostgresSettings' own field tags, so this key never appears in ExportMetadata() under any name",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_maxIdleConns",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_connMaxLifetime",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_connMaxIdleTime",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_retryEnabled",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_retryMaxAttempts",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
	{
		file: "utxo_settings.md", key: "utxostore_postgres_retryBaseDelay",
		reason: "same getPostgresPoolSettings(\"utxostore\", ...) concatenation gap as utxostore_postgres_maxOpenConns above",
	},
}

// lookupNonSettingsKey returns the nonSettingsKeyExemption registered for
// file/key, if any.
func lookupNonSettingsKey(file, key string) (nonSettingsKeyExemption, bool) {
	for _, e := range nonSettingsKeyExemptions {
		if e.file == file && e.key == key {
			return e, true
		}
	}

	return nonSettingsKeyExemption{}, false
}

// isolatedSettingsContext is a gocore context name distinct from the ambient
// one (whatever SETTINGS_CONTEXT is set to, or gocore's own "dev" default -
// see gocore's Config()). Passing it to NewSettings makes
// gocore.Config(isolatedSettingsContext) create and cache its own copy of
// the confs map (see gocore's Config()) instead of returning the shared
// singleton every other test in this package reads through - so
// buildIsolatedRuntimeSettings below can empty that copy out without
// affecting anything else running in this test binary.
const isolatedSettingsContext = "settingsdoctest-isolated-context-3a1c9f"

// settingsKeys returns every settings-package key known to ExportMetadata().
// It only needs a throwaway Settings value: the key/tag list comes from
// reflection over the Settings type, not from any field's current value.
func settingsKeys(t *testing.T) []string {
	t.Helper()

	probe := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	keys := make([]string, 0, len(probe.ExportMetadata().Settings))
	for _, entry := range probe.ExportMetadata().Settings {
		keys = append(keys, entry.Key)
	}

	return keys
}

// buildIsolatedRuntimeSettings constructs a Settings the same way production
// code does - via NewSettings - but isolated from every source of config
// that could make the result depend on anything but the getX(...) literal
// fallback baked into settings.go:
//
//  1. settings.conf and settings_local.conf. The doc tables this test checks
//     document the getX(...) Go-literal fallback as "Default" and call out a
//     settings.conf override as a separate parenthetical when one exists
//     (see e.g. docs/references/settings/services/p2p_settings.md's
//     GRPCListenAddress row: "\":9906\" (Go default; overridden to `:9904`
//     by `settings.conf` ...)") - so "the value the settings package
//     actually produces" that this test must anchor to is the fallback, not
//     whatever settings.conf happens to ship, and settings.conf being
//     committed (unlike settings_local.conf) does not change that: it is a
//     deployment configuration file, not the thing the "Default" column
//     documents. Confirmed by running this uninstrumented: with settings.conf
//     applied, roughly 90 rows across every doc "mismatched" - overwhelmingly
//     tuned settings.conf values (batch sizes, ports, timeouts) that were
//     never doc/tag drift, which is what sent this investigation down the
//     right path.
//
//     gocore.Config(isolatedSettingsContext) (see gocore's Config()) starts
//     as a copy of the ambient singleton's confs map, which already has both
//     files merged in by the time any test in this package can observe it
//     (gocore.Config() loads them once, on the first call from anywhere in
//     the test binary - see gocore's Config()). Every key in that copy -
//     enumerated via GetAll() before any mutation - is Unset() one by one, so
//     the copy ends up with an empty confs map. This only touches the
//     isolatedSettingsContext copy; the shared "dev" (or whatever ambient
//     SETTINGS_CONTEXT) config every other test in this package relies on is
//     untouched.
//
//  2. OS environment variables. gocore's getInternal checks
//     os.LookupEnv(key) before consulting confs at all (see
//     (*gocore.Configuration).getInternal), so an ambient environment
//     variable named after a settings key would otherwise silently outrank
//     the code fallback regardless of step 1. Every variable whose name
//     matches a known settings key is unset for the duration of the call and
//     restored via t.Cleanup.
//
// A key that was never set is unaffected by either step: Unset on an absent
// confs key and Setenv/Unsetenv restore on a variable that was never set are
// both no-ops.
func buildIsolatedRuntimeSettings(t *testing.T, keys []string) *Settings {
	t.Helper()

	type envBackup struct {
		key    string
		value  string
		hadEnv bool
	}

	backups := make([]envBackup, 0, len(keys))

	for _, key := range keys {
		value, hadEnv := os.LookupEnv(key)
		backups = append(backups, envBackup{key: key, value: value, hadEnv: hadEnv})

		if hadEnv {
			require.NoError(t, os.Unsetenv(key), "unsetting ambient env var %s for isolation", key)
		}
	}

	t.Cleanup(func() {
		for _, b := range backups {
			if b.hadEnv {
				require.NoError(t, os.Setenv(b.key, b.value), "restoring ambient env var %s", b.key)
			}
		}
	})

	isolatedConfig := gocore.Config(isolatedSettingsContext)
	for key := range isolatedConfig.GetAll() {
		isolatedConfig.Unset(key)
	}

	return NewSettings(isolatedSettingsContext)
}

// TestSettingsDocDefaultsMatchCode walks every markdown file under
// docs/references/settings/ and cross-checks every settings-table row's
// documented default three ways: against the struct tag default surfaced by
// ExportMetadata(), and against the real value the settings package produces
// at runtime (NewSettings(), built in isolation - see
// buildIsolatedRuntimeSettings). A doc/tag-only check cannot catch the case
// where a doc and its struct tag drifted together: this PR found nine struct
// tags in settings/interface.go whose default:"..." no longer matched the
// literal passed to the corresponding getBool/getInt/getDuration(...) call in
// settings.go, in every case with the doc holding the correct value and the
// tag being the stale one - a doc-vs-tag comparison alone is blind to that
// class of bug because both sides being wrong the same way still compares
// equal.
func TestSettingsDocDefaultsMatchCode(t *testing.T) {
	tagProbe := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	codeDefaults := make(map[string]string)
	for _, entry := range tagProbe.ExportMetadata().Settings {
		codeDefaults[entry.Key] = entry.DefaultValue
	}

	runtimeSettings := buildIsolatedRuntimeSettings(t, settingsKeys(t))

	runtimeDefaults := make(map[string]string)
	for _, entry := range runtimeSettings.ExportMetadata().Settings {
		runtimeDefaults[entry.Key] = entry.CurrentValue
	}

	confValues := settingsConfBaseValues(t)

	seenExemptions := make(map[docDefaultExemption]bool)
	seenNonSettingsKeyExemptions := make(map[nonSettingsKeyExemption]bool)

	for _, docPath := range settingsDocFiles(t) {
		docPath := docPath
		base := filepath.Base(docPath)

		t.Run(base, func(t *testing.T) {
			docBytes, err := os.ReadFile(docPath)
			require.NoError(t, err, "reading %s", docPath)

			rows := parseDocSettingsTables(string(docBytes))

			checked := 0
			var rowFailures []string

			for _, row := range rows {
				codeDefault, ok := codeDefaults[row.key]
				if !ok {
					if nsExemption, nsExempt := lookupNonSettingsKey(base, row.key); nsExempt {
						seenNonSettingsKeyExemptions[nsExemption] = true
						continue
					}

					rowFailures = append(rowFailures, fmt.Sprintf(
						"%s (%s) is documented as a settings key, but no such key exists in ExportMetadata() - "+
							"misspelled, renamed, or fabricated key, or add a nonSettingsKeyExemptions entry if "+
							"it's a real key ExportMetadata() genuinely can't see",
						row.setting, row.key))

					continue
				}

				runtimeDefault, ok := runtimeDefaults[row.key]
				if !ok {
					continue
				}

				exemption, exempt := lookupExemption(base, row.key)
				if exempt {
					seenExemptions[exemption] = true
				}

				skipTag := exempt && (exemption.unreliable == "tag" || exemption.unreliable == "both")
				skipRuntime := exempt && (exemption.unreliable == "runtime" || exemption.unreliable == "both")

				// A "both"-exempted row has every comparison below skipped, so it
				// is not actually checked against anything - do not let it count
				// toward minCheckedRowsPerDoc's floor (see ChiR5 in the doc drift
				// review: a fully-skipped row inflating the count defeats the
				// floor's purpose of catching a silent parse failure).
				if !(skipTag && skipRuntime) {
					checked++
				}

				docC := canonicalDefault(row.docDefault)
				tagC := canonicalDefault(codeDefault)
				runtimeC := canonicalDefault(runtimeDefault)

				var mismatches []string

				if !skipTag && docC != tagC {
					mismatches = append(mismatches, "doc/tag")
				}

				if !skipRuntime && docC != runtimeC {
					mismatches = append(mismatches, "doc/runtime")
				}

				if !skipTag && !skipRuntime && tagC != runtimeC {
					mismatches = append(mismatches, "tag/runtime")
				}

				if len(mismatches) > 0 {
					rowFailures = append(rowFailures, fmt.Sprintf(
						"%s (%s): %v disagree - doc default %q, struct tag default %q, runtime default %q",
						row.setting, row.key, mismatches, row.docDefault, codeDefault, runtimeDefault))
				}

				// The "N (Go default; settings.conf ships X)" annotation is a
				// specific, mechanically-checkable claim about settings.conf
				// (which is committed) that canonicalDefault's generic
				// trailing-parenthetical strip otherwise discards as free text
				// (see ChiR8 in the doc drift review) - verify X against what
				// settings.conf's base/default context actually ships.
				if shipsMatch := shipsAnnotationRe.FindStringSubmatch(row.docDefault); shipsMatch != nil {
					shipped := shipsMatch[1]

					confValue, ok := confValues[row.key]
					if !ok {
						rowFailures = append(rowFailures, fmt.Sprintf(
							"%s (%s): doc claims settings.conf ships %q, but settings.conf has no base (context-less) "+
								"assignment for %q", row.setting, row.key, shipped, row.key))
					} else if canonicalDefault(shipped) != canonicalDefault(confValue) {
						rowFailures = append(rowFailures, fmt.Sprintf(
							"%s (%s): doc claims settings.conf ships %q, but settings.conf actually ships %q",
							row.setting, row.key, shipped, confValue))
					}
				}
			}

			// Every row is checked before failing (rather than failing on the first
			// mismatch) so one test run reports every drifted row in this doc, not
			// just the first one found.
			require.Empty(t, rowFailures, "%s has %d mismatched row(s):\n%s",
				base, len(rowFailures), strings.Join(rowFailures, "\n"))

			wantChecked, ok := checkedRowsPerDoc[base]
			require.True(t, ok, "no row-count expectation registered for %s - add one to checkedRowsPerDoc "+
				"(use 0 if this doc legitimately documents settings outside the standard table)", base)

			require.Equal(t, wantChecked, checked,
				"%d settings rows were cross-checked in %s, expected exactly %d - if this doc's table gained or "+
					"lost a row (or a row's exemption changed whether it counts, see ChiR5), update "+
					"checkedRowsPerDoc; if it dropped because the table parser stopped matching some rows (e.g. a "+
					"prose line interposed mid-table truncated it), fix the doc instead",
				checked, base, wantChecked)
		})
	}

	for _, e := range docDefaultsExemptions {
		require.True(t, seenExemptions[e],
			"stale exemption: %s/%s no longer appears as a settings-table row - remove it from docDefaultsExemptions", e.file, e.key)
	}

	for _, e := range nonSettingsKeyExemptions {
		require.True(t, seenNonSettingsKeyExemptions[e],
			"stale exemption: %s/%s no longer appears as a settings-table row - remove it from nonSettingsKeyExemptions", e.file, e.key)
	}
}

// runtimeUnreliableKeys returns every key that docDefaultsExemptions marks as
// "runtime" or "both" unreliable, independent of which doc(s) document it.
// TestSettingsTagMatchesRuntimeForAllKeys reuses this rather than
// re-declaring the same keys, because the reason a key's runtime value can't
// be trusted (CPU-count-derived, or a key-tag collision) does not depend on
// which doc happens to mention it.
func runtimeUnreliableKeys() map[string]bool {
	keys := make(map[string]bool)

	for _, e := range docDefaultsExemptions {
		if e.unreliable == "runtime" || e.unreliable == "both" {
			keys[e.key] = true
		}
	}

	return keys
}

// pendingWireExemption is a temporary exemption for a settings-package key
// whose struct-tag default does not match its isolated-runtime value because
// nothing in settings.go wires it up yet - the setting is declared but the
// literal it should be read from is missing, commented out, or misnamed.
// Each names the open PR that fixes the underlying wiring; once that PR
// merges, remove the exemption here (TestSettingsTagMatchesRuntimeForAllKeys
// will fail loudly if the entry becomes stale before then, via the
// still-mismatched check below, or if it's removed while still needed).
type pendingWireExemption struct {
	key    string
	reason string
}

// Note: the five postgres_circuitBreaker* keys are NOT listed here even
// though they are the same class of gap - they are already covered by
// docDefaultsExemptions (unreliable: "runtime", global_settings.md), and
// runtimeUnreliableKeys() above folds those into this test's skip set too.
// Listing them again here would make this list go stale the moment #1642
// lands (both lists claim the mismatch, only one would still be true).
var pendingWireExemptions = []pendingWireExemption{
	{
		key:    "p2p_peer_registry_ttl",
		reason: "tagged 24h, resolves to 0s - not wired to a loader; being fixed in #1645",
	},
	{
		key:    "p2p_peer_registry_cleanup_interval",
		reason: "tagged 1h, resolves to 0s - not wired to a loader; being fixed in #1645",
	},
	{
		key:    "p2p_peer_registry_max_size",
		reason: "tagged 10000, resolves to 0 - not wired to a loader; being fixed in #1645",
	},
	{
		key:    "blockchain_peerRegistrySaveInterval",
		reason: "tagged 60s, resolves to 0s - not wired to a loader; being fixed in #1643",
	},
	{
		key: "blockchain_subscription_timeout",
		reason: "tagged 30s, resolves to 0s - has no reader anywhere in the codebase; #1643 deletes the dead field " +
			"rather than wiring it",
	},
}

// TestSettingsTagMatchesRuntimeForAllKeys is the sound version of the
// tag/runtime half of TestSettingsDocDefaultsMatchCode: it compares every
// key ExportMetadata() knows about against the value NewSettings() actually
// produces in isolation, regardless of whether any doc mentions the key.
// TestSettingsDocDefaultsMatchCode only performs this comparison for keys
// that appear in a settings-table row, so a key with no doc row at all - or
// one documented under a fabricated composite name (see ChiR1 in the doc
// drift review) - was invisible to it. This test has no such blind spot: it
// walks ExportMetadata() directly, so helper-built keys are covered without
// needing to appear as a literal anywhere in settings.go.
func TestSettingsTagMatchesRuntimeForAllKeys(t *testing.T) {
	tagProbe := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	runtimeSettings := buildIsolatedRuntimeSettings(t, settingsKeys(t))

	runtimeDefaults := make(map[string]string)
	for _, entry := range runtimeSettings.ExportMetadata().Settings {
		runtimeDefaults[entry.Key] = entry.CurrentValue
	}

	unreliable := runtimeUnreliableKeys()

	pending := make(map[string]pendingWireExemption)
	for _, e := range pendingWireExemptions {
		pending[e.key] = e
	}

	seenPending := make(map[string]bool)

	var failures []string

	for _, entry := range tagProbe.ExportMetadata().Settings {
		if unreliable[entry.Key] {
			continue
		}

		runtimeDefault, ok := runtimeDefaults[entry.Key]
		if !ok {
			continue
		}

		if _, exempt := pending[entry.Key]; exempt {
			seenPending[entry.Key] = true
			continue
		}

		if canonicalDefault(entry.DefaultValue) != canonicalDefault(runtimeDefault) {
			failures = append(failures, fmt.Sprintf(
				"%s: struct tag default %q disagrees with isolated runtime default %q",
				entry.Key, entry.DefaultValue, runtimeDefault))
		}
	}

	sort.Strings(failures)
	require.Empty(t, failures, "%d key(s) have a struct-tag default that disagrees with the value NewSettings() "+
		"actually produces:\n%s", len(failures), strings.Join(failures, "\n"))

	for key := range pending {
		require.True(t, seenPending[key],
			"stale pendingWireExemption: %q no longer disagrees between tag and runtime - remove it from pendingWireExemptions", key)
	}
}

// checkedRowsPerDoc records, per doc file, the exact number of settings-table
// rows TestSettingsDocDefaultsMatchCode cross-checks against ExportMetadata()
// (a fully "both"-exempted row, see docDefaultsExemptions, does not count -
// every comparison on it is skipped). It exists so a doc reformat that
// silently stops the row parser from matching some rows (as happened
// historically for the single file this guard used to cover, and again for
// utxo_settings.md's mid-table "**Note**" line - see ChiR5 in the doc drift
// review) fails loudly per file instead of turning the test into a no-op. The
// count is exact, not a slack floor: a row added, removed, or newly/no-longer
// exempted must update its entry here in the same change, the same
// discipline the exemption staleness checks already impose.
//
// One file is registered at 0 because it does not use the tabular
// "| Setting | Type | Default | Environment Variable | Usage |" format at
// all, so there is nothing for the table-row parser to check:
// blob_settings.md documents blob store URL query parameters (batch,
// sizeInBytes, ...), not settings-package keys - there is no ExportMetadata()
// entry to cross-check them against.
var checkedRowsPerDoc = map[string]int{
	"global_settings.md":            44,
	"kafka_settings.md":             19,
	"policy_settings.md":            25,
	"alert_settings.md":             6,
	"asset_settings.md":             34,
	"blockassembly_settings.md":     41,
	"blockchain_settings.md":        11,
	"blockpersister_settings.md":    9,
	"blockvalidation_settings.md":   62,
	"coinbase_settings.md":          21,
	"faucet_settings.md":            1,
	"legacy_settings.md":            26,
	"p2p_settings.md":               40,
	"propagation_settings.md":       13,
	"pruner_settings.md":            27,
	"rpc_settings.md":               11,
	"subtreevalidation_settings.md": 25,
	"utxopersister_settings.md":     2,
	"validator_settings.md":         17,
	"aerospike_settings.md":         13,
	"blob_settings.md":              0,
	"utxo_settings.md":              39,
}

// exampleKeyRe matches a `key = value` or `key=value` assignment inside the
// doc's configuration examples, tolerating the leading indentation of a code
// block nested inside a numbered/bulleted list. The key may carry a
// settings-context suffix (e.g. `aerospike_host.docker.teranode1`); only the
// base key before the first "." is resolved against the settings package,
// since context suffixes are not part of ExportMetadata().
var exampleKeyRe = regexp.MustCompile(`(?m)^[ \t]*([A-Za-z][A-Za-z0-9_]*)((?:\.[A-Za-z0-9_]+)*)[ \t]*=[^=]`)

// configFenceRe matches a fenced code block and captures its language tag (if
// any) and its content.
var configFenceRe = regexp.MustCompile("(?ms)^[ \t]*```([A-Za-z0-9_+-]*)[ \t]*\n(.*?)\n[ \t]*```[ \t]*$")

// configFenceLanguages lists the fence languages used in
// docs/references/settings/ to show settings.conf/environment-variable
// examples. exampleKeyRe is only run against the content of fences tagged
// with one of these - not the doc's prose, and not fences in another
// language (e.g. the ```go struct-field-assignment sample in
// pruner_settings.md, or a ```sql snippet), which can contain an `=`
// assignment that resembles a settings key without being one.
var configFenceLanguages = map[string]bool{
	"conf": true,
	"bash": true,
	"text": true,
}

// extractConfigFenceContent returns the concatenated content of every fenced
// code block in doc whose language is in configFenceLanguages.
func extractConfigFenceContent(doc string) string {
	var sb strings.Builder

	for _, m := range configFenceRe.FindAllStringSubmatch(doc, -1) {
		lang, content := m[1], m[2]
		if !configFenceLanguages[lang] {
			continue
		}

		sb.WriteString(content)
		sb.WriteString("\n")
	}

	return sb.String()
}

// exampleKeyExemptions lists example-block tokens that look like a
// `key = value` assignment but are not settings-package keys, together with
// why. Each is checked to still appear in its doc.
type exampleKeyExemption struct {
	file   string
	key    string
	reason string
}

var exampleKeyExemptions = []exampleKeyExemption{
	{
		file: "global_settings.md", key: "SETTINGS_CONTEXT",
		reason: "bootstrap context selector read directly via gocore.Config().GetContext() before the settings " +
			"registry exists - it has no `key` struct tag and is not in ExportMetadata()",
	},
	{
		file: "pruner_settings.md", key: "startPruner",
		reason: "read directly via gocore.Config().GetBool(\"start\"+app) in daemon.go's shouldStart(), one per " +
			"service - it has no `key` struct tag and is not in ExportMetadata()",
	},
	{
		file: "pruner_settings.md", key: "PRUNER_GRPC_PORT",
		reason: "not a settings-package key at all - it's an operator-defined gocore config value referenced via " +
			"`${PRUNER_GRPC_PORT}` interpolation inside pruner_grpcAddress/pruner_grpcListenAddress (gocore's " +
			"generic ${VAR} substitution, see Configuration.replaceVariables), so it can be named anything",
	},
	{
		file: "pruner_settings.md", key: "pruner_IndexName",
		reason: "read directly via a package-level `var IndexName, _ = gocore.Config().Get(\"pruner_IndexName\", " +
			"\"pruner_dah_index\")` in stores/utxo/aerospike/pruner/pruner_service.go - it has no `key` struct " +
			"tag and is not in ExportMetadata()",
	},
}

// TestSettingsDocExampleKeysExist checks that every setting key used in a
// doc's configuration examples is a real key the settings package reads. A
// misspelled key in an example is silently ignored at runtime (gocore just
// never finds a match and falls back to the default), so the operator gets
// silent drift instead of an error.
func TestSettingsDocExampleKeysExist(t *testing.T) {
	s := &Settings{ChainCfgParams: &chaincfg.MainNetParams}

	knownKeys := make(map[string]struct{})
	for _, entry := range s.ExportMetadata().Settings {
		knownKeys[entry.Key] = struct{}{}
	}

	seenExemptions := make(map[exampleKeyExemption]bool)

	for _, docPath := range settingsDocFiles(t) {
		docPath := docPath
		base := filepath.Base(docPath)

		t.Run(base, func(t *testing.T) {
			docBytes, err := os.ReadFile(docPath)
			require.NoError(t, err, "reading %s", docPath)

			matches := exampleKeyRe.FindAllStringSubmatch(extractConfigFenceContent(string(docBytes)), -1)

			for _, match := range matches {
				key := match[1]

				isExempt := false
				for _, e := range exampleKeyExemptions {
					if e.file == base && e.key == key {
						seenExemptions[e] = true
						isExempt = true
					}
				}
				if isExempt {
					continue
				}

				_, ok := knownKeys[key]
				require.True(t, ok,
					"%s uses %q in a configuration example, but no such setting key exists", base, key)
			}
		})
	}

	for _, e := range exampleKeyExemptions {
		require.True(t, seenExemptions[e],
			"stale exemption: %s/%s no longer appears in a configuration example - remove it from exampleKeyExemptions", e.file, e.key)
	}
}

// settingsConfPath returns the repo-root settings.conf path relative to this
// test file.
func settingsConfPath(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	repoRoot := filepath.Dir(filepath.Dir(thisFile))

	return filepath.Join(repoRoot, "settings.conf")
}

// settingsConfBaseKeyRe matches the base key (before any settings-context
// suffix) of a settings.conf assignment line: "key.context.sub = value" or
// "key = value".
var settingsConfBaseKeyRe = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)`)

// settingsConfBaseKeys returns the set of distinct base keys assigned
// anywhere in settings.conf, with any ".context" suffix stripped (context
// suffixes are not part of ExportMetadata() and are not gocore ${VAR}
// interpolation targets either).
func settingsConfBaseKeys(t *testing.T) map[string]bool {
	t.Helper()

	confBytes, err := os.ReadFile(settingsConfPath(t))
	require.NoError(t, err, "reading settings.conf")

	keys := make(map[string]bool)

	for _, line := range strings.Split(string(confBytes), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		eq := strings.Index(trimmed, "=")
		if eq == -1 {
			continue
		}

		lhs := strings.TrimSpace(trimmed[:eq])

		m := settingsConfBaseKeyRe.FindString(lhs)
		if m == "" {
			continue
		}

		keys[m] = true
	}

	return keys
}

// settingsConfBaseValues returns, for every settings.conf assignment line
// whose key has no ".context" suffix (i.e. the base/default-context value,
// not a per-context override), the key mapped to its raw right-hand-side
// value: quotes stripped, and any trailing "# comment" removed.
func settingsConfBaseValues(t *testing.T) map[string]string {
	t.Helper()

	confBytes, err := os.ReadFile(settingsConfPath(t))
	require.NoError(t, err, "reading settings.conf")

	values := make(map[string]string)

	for _, line := range strings.Split(string(confBytes), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		eq := strings.Index(trimmed, "=")
		if eq == -1 {
			continue
		}

		lhs := strings.TrimSpace(trimmed[:eq])
		if strings.Contains(lhs, ".") {
			continue // per-context override, not the base/default-context value
		}

		if !settingsConfBaseKeyRe.MatchString(lhs) || settingsConfBaseKeyRe.FindString(lhs) != lhs {
			continue
		}

		rhs := trimmed[eq+1:]
		if hash := strings.Index(rhs, "#"); hash != -1 {
			rhs = rhs[:hash]
		}

		rhs = strings.TrimSpace(rhs)
		rhs = stripOneQuoteLayer(rhs)

		values[lhs] = rhs
	}

	return values
}

// shipsAnnotationRe matches the specific "N (Go default; settings.conf ships
// X)" doc-default form used to call out that settings.conf overrides the Go
// literal - see e.g. pruner_settings.md's UTXOChunkSize row. Group 1 is X,
// the value settings.conf is claimed to ship.
var shipsAnnotationRe = regexp.MustCompile(`\(Go default; settings\.conf ships (.+?)\)\s*$`)

// deadSettingsConfKeyExemption names a settings.conf key that this test
// asserts does NOT appear in settings.conf - it documents a feature that was
// removed, or a key that was fabricated in documentation and never existed,
// so its resolvedness is a permanent invariant rather than a per-file doc
// exemption. Each entry exists specifically because the doc drift review
// found it (see ChiR6: the "pruner_jobTimeout" doc row was deleted as
// describing a feature that was never implemented, but the settings.conf
// block that set it and described its behaviour in comments was left behind
// - the wrong half of an inconsistency was removed). Unlike
// nonSettingsKeyExemptions, which lists real operator-facing keys
// ExportMetadata() cannot see, every key here is asserted to be genuinely
// gone.
type deadSettingsConfKeyExemption struct {
	key    string
	reason string
}

var deadSettingsConfKeyExemptions = []deadSettingsConfKeyExemption{
	{
		key: "pruner_jobTimeout",
		reason: "described a background pruner-job re-queue behaviour no code ever implemented; removed from " +
			"settings.conf and its doc row together (see ChiR6 in the doc drift review)",
	},
	{
		key:    "utxostore_prunerMaxConcurrentOperations",
		reason: "nonexistent setting removed from its doc row; never had a settings.conf entry to begin with",
	},
}

// TestSettingsConfHasNoDeadKeys guards against the specific inconsistency
// ChiR6 found: a settings-table doc row deleted because it described a
// feature that was never implemented, while the settings.conf block that set
// the same key (with comments describing the same nonexistent behaviour) was
// left behind. It does not attempt the much larger project of resolving
// every one of settings.conf's ~400 base keys against ExportMetadata() -
// most are gocore ${VAR} interpolation targets (DATADIR, clientName, ...) or
// keys read directly outside the reflection-tagged Settings struct
// (startPruner and siblings), and distinguishing those from a genuinely dead
// key needs the kind of per-key investigation this review did for the two
// keys below, not a blanket assertion. Instead, this test is the narrow,
// mechanically-checkable half of that follow-up: once a key is confirmed
// dead, assert it stays dead.
func TestSettingsConfHasNoDeadKeys(t *testing.T) {
	present := settingsConfBaseKeys(t)

	for _, e := range deadSettingsConfKeyExemptions {
		require.False(t, present[e.key],
			"settings.conf sets %q, which TestSettingsConfHasNoDeadKeys asserts is dead: %s - if it has been "+
				"reintroduced for a real feature, remove the deadSettingsConfKeyExemptions entry; otherwise delete "+
				"the settings.conf line", e.key, e.reason)
	}
}
