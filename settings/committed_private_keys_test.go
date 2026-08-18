package settings

import (
	"os"
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
// literal is defined, whatever it is called.

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

// committedSettingsFiles are every settings-format file committed to the repo
// (the deploy/util settings_local.conf files are force-added past .gitignore).
var committedSettingsFiles = []string{
	"../settings.conf",
	"../compose/settings_test.conf",
	"../deploy/docker/base/settings_local.conf",
	"../deploy/docker/base/settings_local.conf.template",
	"../util/servicemanager/example/settings_local.conf",
}

var (
	assignmentLine = regexp.MustCompile(`^([A-Za-z0-9_]+)((?:\.[A-Za-z0-9_-]+)*)\s*=\s*(.*)$`)

	// Private key material shapes: 32- or 64-byte hex (Ed25519 seed or
	// priv+pub), and base58 WIF. Compressed/uncompressed public keys are 33/65
	// bytes (66/130 hex) and deliberately do not match.
	hexKeyValue = regexp.MustCompile(`^[0-9a-fA-F]{64}$|^[0-9a-fA-F]{128}$`)
	wifKeyValue = regexp.MustCompile(`^[5KL9c][1-9A-HJ-NP-Za-km-z]{50,51}$`)

	privateKeyName = regexp.MustCompile(`(?i)private_keys?$`)
)

func normalizeValue(raw string) string {
	value := strings.TrimSpace(raw)
	if i := strings.Index(value, " #"); i >= 0 { // trailing comment
		value = strings.TrimSpace(value[:i])
	}

	return strings.Trim(value, `"`)
}

func TestNoRealPrivateKeysCommitted(t *testing.T) {
	for _, rel := range committedSettingsFiles {
		path, err := filepath.Abs(rel)
		require.NoError(t, err)

		content, err := os.ReadFile(path)
		require.NoError(t, err, "committed settings file must exist: %s", rel)

		for i, raw := range strings.Split(string(content), "\n") {
			lineNo := i + 1

			line := strings.TrimSpace(raw)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			m := assignmentLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}

			name, context := m[1], m[2]

			value := normalizeValue(m[3])
			if !hexKeyValue.MatchString(value) && !wifKeyValue.MatchString(value) {
				continue
			}

			require.True(t, allowedFixtures[value],
				"%s:%d: %s%s has a committed private-key-shaped value that is not a known public test fixture; real keys belong in an uncommitted settings_local.conf, env, or the generated p2p.key - if this leaked, rotate it",
				rel, lineNo, name, context)

			if privateKeyName.MatchString(name) {
				require.NotEmpty(t, context,
					"%s:%d: %s has a context-less committed key value; a bare default silently becomes every deployment's identity - leave it empty so the auto-generate path runs",
					rel, lineNo, name)
			}
		}
	}
}
