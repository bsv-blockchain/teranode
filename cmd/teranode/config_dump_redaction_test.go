package teranode

import (
	"net/url"
	"os"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// The canary is a value no real setting can contain, so "the password is not in
// the output" is an assertion about this test's own input rather than a guess.
const dumpCanary = "canary-boot-dump-password"

// taggedSecretKey is a redact-tagged setting whose value is not a URL.
// settings.conf leaves it commented out, so Unset restores the default state.
const (
	taggedSecretKey    = "blockpersister_httpAuthToken"
	taggedSecretCanary = "canary-tagged-auth-token"
)

// TestConfigStatsDumpNeverLogsStoreCredentials pins the boot-time dump that
// RunDaemon logs at cmd/teranode/daemon.go and that cmd/settings.PrintSettings
// logs again.
//
// It runs against gocore's real Stats() output rather than against the excerpt
// in docs/howto/bugReporting.md, because the shape of that output is what the
// redactor has to survive: a CMDLINE section of bare argv entries, then flat
// key=value and key[context]=value rows.
//
// The first assertion is the important one. gocore resolves each setting
// through the same decrypt path its getters use and masks only values still
// carrying its literal "*EHE*" prefix, so a plaintext store URL reaches the
// dump verbatim. If that ever stops being true the test should be deleted, not
// weakened: an assertion that the redactor removed something the dump never
// contained proves nothing.
func TestConfigStatsDumpNeverLogsStoreCredentials(t *testing.T) {
	cfg := gocore.Config()

	const storeKey = "blockchain_store_redaction_canary"

	cfg.Set(storeKey, "postgres://teranode:"+dumpCanary+"@db.example:5432/blockchain")
	t.Cleanup(func() { cfg.Unset(storeKey) })

	// A password holding a raw "/" after an all-digit segment makes url.Parse
	// succeed with the authority cut short: no userinfo, host "teranode:2024",
	// and the rest of the password in the path, where plain Redacted() prints
	// it.
	const misreadKey = "utxostore_misread_redaction_canary"

	misreadURL := "postgres://teranode:2024/" + dumpCanary + "@misread.example:5432/utxo"
	cfg.Set(misreadKey, misreadURL)
	t.Cleanup(func() { cfg.Unset(misreadKey) })

	require.Contains(t, cfg.Stats(), misreadURL,
		"gocore's own dump no longer carries the misread URL, so this case is not exercising a leak")

	// utxostore's externalStore parameter is a whole blob store URL, and an
	// http blob store sends its userinfo as a Basic header. Percent-encoded, as
	// a nested URL with options of its own has to be, its "@" is no longer raw.
	const nestedKey = "utxostore_nested_redaction_canary"

	nestedURL := "aerospike://nested.example:3000/utxo?set=utxo&externalStore=" +
		url.QueryEscape("http://blob:"+dumpCanary+"@blob.example:8080/x?batch=true&sizeInBytes=100")
	cfg.Set(nestedKey, nestedURL)
	t.Cleanup(func() { cfg.Unset(nestedKey) })

	require.Contains(t, cfg.Stats(), nestedURL,
		"gocore's own dump no longer carries the nested URL, so this case is not exercising a leak")

	// Stats() renders os.Args as its CMDLINE section, one bare entry per line
	// with no "key=" in front. A line-splitting redactor would miss the second
	// of these two, which is why RedactText works on tokens.
	originalArgs := os.Args
	os.Args = []string{
		"teranode",
		"--db-url=postgres://teranode:" + dumpCanary + "@flag.example:5432/blockchain",
		"--db-url",
		"postgres://teranode:" + dumpCanary + "@bare.example:5432/blockchain",
	}

	t.Cleanup(func() { os.Args = originalArgs })

	require.Contains(t, cfg.Stats(), dumpCanary,
		"gocore's own dump no longer carries the credential, so this test is not exercising a leak")

	// A redact-tagged setting that is not a URL. Only settings.RedactConfigStats
	// masks it, by key name; urlutil.RedactText sees no "scheme://" and leaves it
	// alone. Every other canary here is a URL, which RedactText alone catches, so
	// without this one dropping the settings pass from redactedConfigDump keeps
	// the test green.
	cfg.Set(taggedSecretKey, taggedSecretCanary)
	t.Cleanup(func() { cfg.Unset(taggedSecretKey) })

	require.Contains(t, cfg.Stats(), taggedSecretCanary,
		"gocore's own dump no longer carries the tagged secret, so this case is not exercising a leak")

	// redactedConfigDump, not urlutil.RedactText: the assertion has to fail if
	// RunDaemon stops redacting, not merely if the redactor stops working.
	redacted := redactedConfigDump()

	require.NotContains(t, redacted, dumpCanary, "the boot dump still carries a store password")
	require.NotContains(t, redacted, taggedSecretCanary, "the boot dump still carries a redact-tagged secret")
	require.Contains(t, redacted, taggedSecretKey, "the tagged secret's row lost its key")

	// The operator has to keep being able to read this dump, so redaction must
	// cost only the password.
	require.Contains(t, redacted, "db.example:5432", "the settings row lost its host")
	require.Contains(t, redacted, "flag.example:5432", "the key=value CMDLINE row lost its host")
	require.Contains(t, redacted, "bare.example:5432", "the bare argv CMDLINE row lost its host")
	require.Contains(t, redacted, "nested.example:3000", "the nested-store row lost its host")
	require.Contains(t, redacted, storeKey, "the settings row lost its key")
	require.Contains(t, redacted, "teranode:xxxxx@", "the username should survive redaction")
}

// TestConfigPayloadNeverAdvertisesStoreCredentials covers the second route to
// the same data: RunDaemon registers Config().GetAll() as gocore's "CONFIG"
// advertising payload, and gocore POSTs that map to advertisingURL on every
// advertising tick when that setting is non-empty. GetAll() returns the raw
// configuration map with no masking at all, so the payload has to be redacted
// on our side of the handover.
func TestConfigPayloadNeverAdvertisesStoreCredentials(t *testing.T) {
	cfg := gocore.Config()

	const storeKey = "utxostore_payload_redaction_canary"

	cfg.Set(storeKey, "aerospike://teranode:"+dumpCanary+"@aero.example:3000/utxo")
	t.Cleanup(func() { cfg.Unset(storeKey) })

	require.Contains(t, cfg.GetAll()[storeKey], dumpCanary,
		"gocore's GetAll no longer carries the credential, so this test is not exercising a leak")

	// The non-URL secret that only settings.RedactConfigMap masks; see the dump
	// test above.
	cfg.Set(taggedSecretKey, taggedSecretCanary)
	t.Cleanup(func() { cfg.Unset(taggedSecretKey) })

	require.Equal(t, taggedSecretCanary, cfg.GetAll()[taggedSecretKey],
		"gocore's GetAll no longer carries the tagged secret, so this case is not exercising a leak")

	// The exact function value RunDaemon registers with gocore.
	redacted, ok := configAdvertisingPayload().(map[string]string)
	require.True(t, ok, "the advertising payload is no longer a map[string]string")

	for key, value := range redacted {
		require.NotContains(t, value, dumpCanary, "advertised setting %q still carries a store password", key)
		require.NotContains(t, value, taggedSecretCanary, "advertised setting %q still carries a redact-tagged secret", key)
	}

	require.Contains(t, redacted, taggedSecretKey, "the tagged secret's key was dropped rather than masked")

	require.Contains(t, redacted[storeKey], "aero.example:3000", "the advertised value lost its host")
	require.Contains(t, redacted[storeKey], "teranode:xxxxx@", "the username should survive redaction")
}
