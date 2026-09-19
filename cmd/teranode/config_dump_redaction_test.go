package teranode

import (
	"os"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// The canary is a value no real setting can contain, so "the password is not in
// the output" is an assertion about this test's own input rather than a guess.
const dumpCanary = "canary-boot-dump-password"

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

	// redactedConfigDump, not urlutil.RedactText: the assertion has to fail if
	// RunDaemon stops redacting, not merely if the redactor stops working.
	redacted := redactedConfigDump()

	require.NotContains(t, redacted, dumpCanary, "the boot dump still carries a store password")

	// The operator has to keep being able to read this dump, so redaction must
	// cost only the password.
	require.Contains(t, redacted, "db.example:5432", "the settings row lost its host")
	require.Contains(t, redacted, "flag.example:5432", "the key=value CMDLINE row lost its host")
	require.Contains(t, redacted, "bare.example:5432", "the bare argv CMDLINE row lost its host")
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

	// The exact function value RunDaemon registers with gocore.
	redacted, ok := configAdvertisingPayload().(map[string]string)
	require.True(t, ok, "the advertising payload is no longer a map[string]string")

	for key, value := range redacted {
		require.NotContains(t, value, dumpCanary, "advertised setting %q still carries a store password", key)
	}

	require.Contains(t, redacted[storeKey], "aero.example:3000", "the advertised value lost its host")
	require.Contains(t, redacted[storeKey], "teranode:xxxxx@", "the username should survive redaction")
}
