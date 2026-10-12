package settings

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Committed settings files may only carry well-known public test fixtures as
// key-shaped values. Real per-node identity keys belong in an uncommitted
// settings_local.conf, env, or the generated p2p.key file. This test is the CI
// guard for bitcoin-sv/teranode issue 4739: a real Ed25519 identity key was
// committed to settings.conf and had to be treated as compromised.
//
// The check is value-shaped rather than key-name-based: gocore resolves
// ${VAR} indirection from same-file entries (including context-suffixed
// definitions), so a key hidden behind any variable name is caught where the
// literal is defined, whatever it is called. Lines are parsed with gocore's
// own rules (comment starts at the first '#' anywhere, key is everything left
// of the first '=') so the guard sees every literal gocore sees.

// allowedFixtures are the public throwaway values already published in the
// repo's test configs and tooling. Never add a real value here.
var allowedFixtures = map[string]bool{
	// teranode1 p2p fixture (test/test_settings.go Node1PrivateKey,
	// compose/settings_test.conf, compose/cmd/genpeerkeys/main.go).
	"c8a1b91ae120878d91a04c904e0d565aa44b2575c1bb30a729bd3e36e2a1d5e6067216fa92b1a1a7e30d0aaabe288e25f1efc0830f309152638b61d84be6b71d": true,
	// Coinbase teranode1 fixture (compose/settings_test.conf).
	"e76c77795b43d2aacd564648bffebde74a4c31540357dad4a3694a561b4c4f1fbb0ba060a3015f7f367742500ef8486707e58032af1b4dfdb1203c790bcf2526": true,
	// Coinbase dev fixture (settings.conf only; peer ID pinned alongside it).
	"44a5a189fbad1d7bc0c59b33fbd5e485f2f4d3d8bf293838c56ce72e53b557171444c0bb7d5cf75112717084cee9e9e98651421b3cd29d721e43c0a51d81aa54": true,
	// teranode2 / teranode3 p2p fixtures (compose/cmd/genpeerkeys/main.go).
	"89a2d8acf5b2e60fd969914c326c63cde50675a47897c0eaacc02eb6ff8665585d4d059f977910472bcb75040617632019cc0749443fdc66d331b61c8cfb4b0f": true,
	"d77a7cac7833f2c0263ed7b9aaeb8dda1effaf8af948d570ed8f7a93bd3c418d6efee7bdd82ddb80484be84ba0c78ea07251a3ba2b45b2b3367fd5e2f0284e7c": true,
	// Coinbase teranode2 / teranode3 fixtures (compose/settings_test.conf).
	"860616e0492a3050aa760440469acfe4f57cf5387a765f5227603c4f6aeac985bf6643d453a1d68a101e52766e9feb9721b95e34aa73e5ea6c69a44be43cab6d": true,
	"1d6a9c8963fdbb86eabc4d10cb1efdf418197cfc3f9779e3c8229663411ae5c8f1cee260eeeae89cb45aae6955230557eba5bf63ef38087ec6be91ab744326c7": true,
	// Long-committed dev wallet WIF fixtures (settings.conf PK1-PK6, also in
	// util/general_test.go and compose/docker-compose-3blasters.yml). Note:
	// miner_wallet_private_keys.operator.mainnet resolves to these unless the
	// deployment overrides PK1-PK3; flagged as a related finding on
	// bitcoin-sv/teranode issue 4739, tracked separately.
	"L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q": true,
	"KyAwSjuXZNgj78w3W7mR1fVMbPFu2heaCJJkWK5Yy58NZ4xafV6k": true,
	"L3NVjmwg3nC7ZPrwMVF6FXiG1a1RZ89nhizmJVctGztRKLYrhtFL": true,
	// Pre-existing committed dev libp2p PSK (settings.conf p2p_shared_key).
	// No Go consumer references the setting; candidate for separate removal.
	"285b49e6d910726a70f205086c39cbac6d8dcc47839053a21b1f614773bbc137": true,
}

// knownSettingsFiles must always be among the discovered scan targets; a
// discovery bug can therefore never silently reduce the guard to zero files.
var knownSettingsFiles = []string{
	"settings.conf",
	"compose/settings_test.conf",
	"deploy/docker/base/settings_local.conf",
	"deploy/docker/base/settings_local.conf.template",
	"util/servicemanager/example/settings_local.conf",
}

var (
	// hexKeyValue matches hex blobs long enough to be private key material
	// (32-byte seeds and up, including the 96-byte legacy libp2p priv+pub+pub
	// form that crypto.UnmarshalEd25519PrivateKey accepts as 192 hex chars).
	// It mirrors the .gitleaks.toml rule's {64,}.
	hexKeyValue = regexp.MustCompile(`^[0-9a-fA-F]{64,}$`)
	wifKeyValue = regexp.MustCompile(`^[5KL9c][1-9A-HJ-NP-Za-km-z]{50,51}$`)

	privateKeyName = regexp.MustCompile(`(?i)private_keys?$`)
	varRefAny      = regexp.MustCompile(`\$\{[^}]+\}`)

	// keyShapedToken pre-filters candidate tokens on the RAW line, before
	// comment stripping: a commented-out key is byte-for-byte as committed
	// and as exposed as a live one, so it gets the same fixture check.
	keyShapedToken = regexp.MustCompile(`[0-9a-zA-Z]{50,}`)
)

// isKeyShapedHex reports hex blobs long enough to be private key material.
// 66/130 chars are compressed/uncompressed public keys (alert_genesis_keys)
// and are deliberately excluded.
func isKeyShapedHex(v string) bool {
	return hexKeyValue.MatchString(v) && len(v) != 66 && len(v) != 130
}

// parseAssignment parses a settings line exactly as gocore's processFile does:
// everything from the first '#' is a comment, the key is everything left of
// the first '=', and the context is everything from the key's first '.'.
func parseAssignment(line string) (name, context, value string, ok bool) {
	line, _, _ = strings.Cut(line, "#")

	rawKey, rawValue, found := strings.Cut(line, "=")
	if !found {
		return "", "", "", false
	}

	key := strings.TrimSpace(rawKey)
	if key == "" {
		return "", "", "", false
	}

	name, context = key, ""
	if i := strings.Index(key, "."); i >= 0 {
		name, context = key[:i], key[i:]
	}

	return name, context, strings.TrimSpace(rawValue), true
}

// discoverSettingsFiles enumerates every COMMITTED settings-format file via
// git ls-files. Discovery must not walk the filesystem: a developer's local,
// gitignored settings_local.conf legitimately holds real keys and must never
// be scanned. Falls back to the known list when git is unavailable.
func discoverSettingsFiles(t *testing.T, root string) []string {
	out, err := exec.Command("git", "-C", root, "ls-files").Output()
	if err != nil {
		t.Logf("git ls-files unavailable (%v), falling back to the known settings file list", err)
		return knownSettingsFiles
	}

	var files []string

	for path := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		base := filepath.Base(path)

		isSettings := strings.HasPrefix(base, "settings") &&
			(strings.HasSuffix(base, ".conf") || strings.HasSuffix(base, ".conf.template") || strings.HasSuffix(base, ".conf.tmpl"))
		if isSettings || strings.HasSuffix(base, ".conf.tmpl") {
			files = append(files, path)
		}
	}

	for _, known := range knownSettingsFiles {
		require.Contains(t, files, known, "discovery must find every known committed settings file")
	}

	return files
}

func TestNoRealPrivateKeysCommitted(t *testing.T) {
	root, err := filepath.Abs("..")
	require.NoError(t, err)

	for _, rel := range discoverSettingsFiles(t, root) {
		content, err := os.ReadFile(filepath.Join(root, rel))
		require.NoError(t, err, "committed settings file must exist: %s", rel)

		lines := strings.Split(string(content), "\n")

		// ${VAR} resolution table so a key or fixture hidden behind variables
		// is still subject to both checks at the referencing line. Last
		// non-empty wins, matching gocore; context-suffixed definitions are
		// indexed by base name so they cannot dodge resolution.
		defs := map[string]string{}

		for _, raw := range lines {
			if name, _, value, ok := parseAssignment(raw); ok {
				if v := strings.Trim(value, `"`); v != "" {
					defs[name] = v
				}
			}
		}

		// Substring substitution iterated to a fixed point, like gocore's
		// replaceVariables - so fragments concatenated as ${A}${B}${C}${D}
		// assemble into the key the consumer would actually get. One
		// deliberate divergence: gocore rewrites an unresolvable ${X} to
		// {UNKNOWN}; a guard must not rewrite what it cannot see into
		// something benign, so unresolved tokens stay put and the fixed-point
		// check terminates the loop instead.
		resolve := func(v string) string {
			for i := 0; i < 8 && strings.Contains(v, "${"); i++ {
				next := varRefAny.ReplaceAllStringFunc(v, func(m string) string {
					if r, ok := defs[m[2:len(m)-1]]; ok {
						return r
					}
					return m
				})
				if next == v {
					break
				}
				v = next
			}
			return v
		}

		for i, raw := range lines {
			lineNo := i + 1

			// Raw-line pass: commented-out keys count - they are just as
			// committed. Fixture-check every key-shaped token before comment
			// stripping; the parsed pass below adds the structural checks.
			for _, tok := range keyShapedToken.FindAllString(raw, -1) {
				if !isKeyShapedHex(tok) && !wifKeyValue.MatchString(tok) {
					continue
				}

				require.True(t, allowedFixtures[tok],
					"%s:%d: committed private-key-shaped value that is not a known public test fixture (commented-out keys count - they are just as committed); if this leaked, rotate it",
					rel, lineNo)
			}

			name, context, value, ok := parseAssignment(raw)
			if !ok {
				continue
			}

			// Multi-key settings (miner_wallet_private_keys) are read via
			// getMultiString with '|' - check each element separately.
			for part := range strings.SplitSeq(resolve(strings.Trim(value, `"`)), "|") {
				part = strings.Trim(strings.TrimSpace(part), `"`)

				if !isKeyShapedHex(part) && !wifKeyValue.MatchString(part) {
					continue
				}

				require.True(t, allowedFixtures[part],
					"%s:%d: %s%s has a committed private-key-shaped value that is not a known public test fixture; real keys belong in an uncommitted settings_local.conf, env, or the generated p2p.key - if this leaked, rotate it",
					rel, lineNo, name, context)

				// The structural ban on context-less defaults covers hex
				// identity keys (this guard's subject). Context-less WIF
				// wallet defaults (coinbase_wallet_private_key = ${PK1}) are
				// pre-existing and tracked with the PK1-PK6 related finding
				// on bitcoin-sv/teranode issue 4739.
				if privateKeyName.MatchString(name) && isKeyShapedHex(part) {
					require.NotEmpty(t, context,
						"%s:%d: %s has a context-less committed key value; a bare default silently becomes every deployment's identity - leave it empty so the auto-generate path runs",
						rel, lineNo, name)
				}
			}
		}
	}
}
