package settings

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Committed settings files may only carry well-known public test fixtures under
// context-scoped *_private_key entries. Real per-node identity keys belong in
// settings_local.conf, env, or the generated p2p.key file. This test is the CI
// guard for bitcoin-sv/teranode issue 4739: a real Ed25519 identity key was
// committed to settings.conf and had to be treated as compromised.

// allowedFixtures are the public throwaway values already published in
// test/test_settings.go, compose/settings_test.conf and
// compose/cmd/genpeerkeys/main.go. Never add a real value here.
var allowedFixtures = map[string]bool{
	// Node1PrivateKey (test/test_settings.go), docker compose teranode1.
	"c8a1b91ae120878d91a04c904e0d565aa44b2575c1bb30a729bd3e36e2a1d5e6067216fa92b1a1a7e30d0aaabe288e25f1efc0830f309152638b61d84be6b71d": true,
	// Alert/coinbase fixture for the docker test rigs.
	"e76c77795b43d2aacd564648bffebde74a4c31540357dad4a3694a561b4c4f1fbb0ba060a3015f7f367742500ef8486707e58032af1b4dfdb1203c790bcf2526": true,
	// Coinbase dev fixture.
	"44a5a189fbad1d7bc0c59b33fbd5e485f2f4d3d8bf293838c56ce72e53b557171444c0bb7d5cf75112717084cee9e9e98651421b3cd29d721e43c0a51d81aa54": true,
	// Docker compose teranode2 / teranode3 fixtures (compose/cmd/genpeerkeys).
	"89a2d8acf5b2e60fd969914c326c63cde50675a47897c0eaacc02eb6ff8665585d4d059f977910472bcb75040617632019cc0749443fdc66d331b61c8cfb4b0f": true,
	"d77a7cac7833f2c0263ed7b9aaeb8dda1effaf8af948d570ed8f7a93bd3c418d6efee7bdd82ddb80484be84ba0c78ea07251a3ba2b45b2b3367fd5e2f0284e7c": true,
	// Coinbase docker compose teranode2 / teranode3 fixtures.
	"860616e0492a3050aa760440469acfe4f57cf5387a765f5227603c4f6aeac985bf6643d453a1d68a101e52766e9feb9721b95e34aa73e5ea6c69a44be43cab6d": true,
	"1d6a9c8963fdbb86eabc4d10cb1efdf418197cfc3f9779e3c8229663411ae5c8f1cee260eeeae89cb45aae6955230557eba5bf63ef38087ec6be91ab744326c7": true,
}

var privateKeyLine = regexp.MustCompile(`^([A-Za-z0-9_]*_private_keys?)((?:\.[A-Za-z0-9_-]+)*)\s*=\s*(.*)$`)

// placeholderOnly matches values built purely from ${VAR} placeholders and
// separators (e.g. "${PK1} | ${PK2}"), which are resolved at runtime.
var placeholderOnly = regexp.MustCompile(`^(\$\{[A-Za-z0-9_]+\}[\s|]*)+$`)

func TestNoRealPrivateKeysCommitted(t *testing.T) {
	for _, rel := range []string{"../settings.conf", "../compose/settings_test.conf"} {
		path, err := filepath.Abs(rel)
		require.NoError(t, err)

		f, err := os.Open(path)
		require.NoError(t, err, "committed settings file must exist: %s", rel)

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

		lineNo := 0
		for scanner.Scan() {
			lineNo++

			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			m := privateKeyLine.FindStringSubmatch(line)
			if m == nil {
				continue
			}

			name, context := m[1], m[2]

			value := strings.TrimSpace(m[3])
			if i := strings.Index(value, " #"); i >= 0 { // trailing comment
				value = strings.TrimSpace(value[:i])
			}
			value = strings.Trim(value, `"`)

			if value == "" || placeholderOnly.MatchString(value) {
				continue
			}

			require.NotEmpty(t, context,
				"%s:%d: %s has a context-less committed value; a bare default would silently become every deployment's identity - leave it empty so the auto-generate path runs",
				rel, lineNo, name)

			require.True(t, allowedFixtures[value],
				"%s:%d: %s%s has a committed value that is not a known public test fixture; real keys belong in settings_local.conf, env, or the generated p2p.key - if this leaked, rotate it",
				rel, lineNo, name, context)
		}

		require.NoError(t, scanner.Err())
		require.NoError(t, f.Close())
	}
}
