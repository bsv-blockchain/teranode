package settings

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactReplacesTaggedFields(t *testing.T) {
	in := &Settings{
		GRPCAdminAPIKey: "super-secret-grpc-key",
		Coinbase: CoinbaseSettings{
			UserPwd:          "coinbase-db-pwd",
			P2PPrivateKey:    "coinbase-p2p-key",
			WalletPrivateKey: "coinbase-wallet-key",
			SlackToken:       "xoxb-slack-token",
		},
		P2P: P2PSettings{
			PrivateKey: "p2p-priv-key",
		},
		Alert: AlertSettings{
			P2PPrivateKey: "alert-p2p-key",
		},
		RPC: RPCSettings{
			RPCPass:      "rpc-admin-pwd",
			RPCLimitPass: "rpc-limit-pwd",
		},
		BlockAssembly: BlockAssemblySettings{
			MinerWalletPrivateKeys: []string{"miner-key-1", "miner-key-2"},
		},
	}

	out, err := Redact(in)
	require.NoError(t, err)
	require.NotNil(t, out)

	data, err := json.Marshal(out)
	require.NoError(t, err)
	js := string(data)

	secrets := []string{
		"super-secret-grpc-key",
		"coinbase-db-pwd",
		"coinbase-p2p-key",
		"coinbase-wallet-key",
		"xoxb-slack-token",
		"p2p-priv-key",
		"alert-p2p-key",
		"rpc-admin-pwd",
		"rpc-limit-pwd",
		"miner-key-1",
		"miner-key-2",
	}
	for _, s := range secrets {
		require.NotContainsf(t, js, s, "secret %q leaked into redacted output", s)
	}

	require.Contains(t, js, redactedValue, "expected placeholder in output")
}

func TestRedactNilInputReturnsNil(t *testing.T) {
	out, err := Redact(nil)
	require.NoError(t, err)
	require.Nil(t, out)
}

// sensitiveNamePattern matches field names that look like they hold a secret.
// Any string field on Settings whose name matches this pattern MUST carry
// `redact:"true"` — or be added to the allowlist below as a documented
// false-match.
var sensitiveNamePattern = regexp.MustCompile(`(?i)(password|pwd|token|apikey|secret|privatekey)`)

// sensitiveNameAllowlist enumerates fields whose names match the sensitive
// pattern but are NOT secrets and therefore do not require the redact tag.
// Entries are full "Type.Field" paths so collisions on common field names
// (e.g. multiple Password fields) are disambiguated.
var sensitiveNameAllowlist = map[string]string{
	// SecretMiningThreshold is an integer threshold for detecting
	// secret-mining attacks; not a secret itself.
	"BlockValidationSettings.SecretMiningThreshold": "integer threshold, not a credential",
	// GenesisKeys are public keys for alert verification.
	"AlertSettings.GenesisKeys": "public keys, not private",
	// PrivateKeyID is the address version byte for WIF-encoded private keys
	// (chaincfg.Params); it's a single byte identifying the network, not a key.
	"Params.PrivateKeyID": "address version byte, not a key",
	// HDPrivateKeyID is the version bytes for BIP32 extended private keys
	// (chaincfg.Params); identifies the network for serialized extended keys.
	"Params.HDPrivateKeyID": "extended key version bytes, not a key",
}

// TestSensitiveFieldsHaveRedactTag enforces that every Settings field whose
// name matches sensitiveNamePattern carries `redact:"true"` (or is allowlisted
// with a documented rationale). Catches the case where a new sensitive field
// is added without the tag.
func TestSensitiveFieldsHaveRedactTag(t *testing.T) {
	missing := []string{}
	walkSensitiveCheck(reflect.TypeOf(Settings{}), &missing)

	if len(missing) > 0 {
		t.Fatalf("the following fields look sensitive by name but lack `redact:\"true\"` (add the tag or extend sensitiveNameAllowlist): %s",
			strings.Join(missing, ", "))
	}
}

func walkSensitiveCheck(t reflect.Type, missing *[]string) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		path := t.Name() + "." + f.Name

		if sensitiveNamePattern.MatchString(f.Name) {
			if f.Tag.Get("redact") != "true" {
				if _, allowed := sensitiveNameAllowlist[path]; !allowed {
					*missing = append(*missing, path)
				}
			}
		}

		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if ft.Kind() == reflect.Struct {
			walkSensitiveCheck(ft, missing)
		}
	}
}

// TestSensitiveKeysDerivedMatchesExpected confirms extractSensitiveKeys
// returns exactly the set of keys export.go used to hardcode. Regression
// guard against accidentally removing the tag from a sensitive field.
func TestSensitiveKeysDerivedMatchesExpected(t *testing.T) {
	expected := map[string]bool{
		"rpc_pass":                    true,
		"rpc_limit_pass":              true,
		"p2p_private_key":             true,
		"coinbase_p2p_private_key":    true,
		"alert_p2p_private_key":       true,
		"coinbase_wallet_private_key": true,
		"miner_wallet_private_keys":   true,
		"coinbaseDBUserPwd":           true,
		"slack_token":                 true,
		"grpc_admin_api_key":          true,
	}

	got := extractSensitiveKeys()
	require.Equal(t, expected, got)
}

func TestRedactPreservesNonSecretFields(t *testing.T) {
	// Sentinel values used to assert non-secret fields survive the
	// JSON-roundtrip + redaction pass intact.
	const (
		sentinelLogLevel      = "DEBUG_SENTINEL"
		sentinelProfilerAddr  = "localhost:6060-sentinel"
		sentinelArbitraryText = "miner-pool-sentinel"
		sentinelSecret        = "should-not-survive"
	)

	in := &Settings{
		LogLevel:     sentinelLogLevel,
		ProfilerAddr: sentinelProfilerAddr,
		Coinbase: CoinbaseSettings{
			ArbitraryText: sentinelArbitraryText,
			UserPwd:       sentinelSecret,
		},
	}

	out, err := Redact(in)
	require.NoError(t, err)
	require.NotNil(t, out)

	data, err := json.Marshal(out)
	require.NoError(t, err)
	js := string(data)

	// Placeholder is present (the secret was redacted).
	require.Contains(t, js, redactedValue, "expected redacted placeholder in output")

	// Non-secret fields survive the redact pipeline verbatim.
	require.Contains(t, js, sentinelLogLevel, "non-secret LogLevel must survive redaction")
	require.Contains(t, js, sentinelProfilerAddr, "non-secret ProfilerAddr must survive redaction")
	require.Contains(t, js, sentinelArbitraryText, "non-secret Coinbase.ArbitraryText must survive redaction")

	// Secret value does not survive.
	require.NotContains(t, js, sentinelSecret, "secret UserPwd value leaked through redaction")
}
