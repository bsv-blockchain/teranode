package teranodecli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Both flags below are DSNs carrying a password in the userinfo, and both
// errors are printed to stdout by cli.go, which is container logs in practice.
// url.Parse embeds the whole string it was given in its error, so wrapping that
// error unmodified puts the password there.
const dsnCanaryPassword = "canary-cli-dsn-password"

func requireNoCredentialEcho(t *testing.T, err error, wantMessage string) {
	t.Helper()

	require.Error(t, err)
	require.NotContains(t, err.Error(), dsnCanaryPassword, "the DSN password reached the error message")
	require.NotContains(t, err.Error(), "teranode:", "the DSN userinfo reached the error message")
	require.NotContains(t, err.Error(), "db host", "the DSN host reached the error message")

	// The operator still learns which URL failed and why. Asserting the reason
	// and not only the prefix is what makes this catch a message left carrying
	// an unrendered format verb.
	require.Contains(t, err.Error(), wantMessage)
	require.Contains(t, strings.ToLower(err.Error()), "invalid character",
		"expected url.Parse's own reason to survive, got: %v", err)
	require.NotContains(t, err.Error(), "%", "the message renders a format verb literally")
}

// TestFixChainworkRejectsUnparseableDBURLWithoutEchoingIt covers --db-url.
func TestFixChainworkRejectsUnparseableDBURLWithoutEchoingIt(t *testing.T) {
	// A space in the host is what makes url.Parse refuse it.
	dbURL := "postgres://teranode:" + dsnCanaryPassword + "@db host:5432/blockchain"

	err := fixChainwork(dbURL, true, 1, 0, 1)
	requireNoCredentialEcho(t, err, "failed to parse database URL")
}

// TestLoadUnminedBenchRejectsUnparseableAerospikeURLWithoutEchoingIt covers
// --aerospike-url. A non-empty URL is passed so the benchmark never reaches
// the TestContainer path: the parse is the first thing it does with the flag.
func TestLoadUnminedBenchRejectsUnparseableAerospikeURLWithoutEchoingIt(t *testing.T) {
	aerospikeURL := "aerospike://teranode:" + dsnCanaryPassword + "@db host:3000/utxo"

	err := runLoadUnminedBenchmark(1, "", "", aerospikeURL)
	requireNoCredentialEcho(t, err, "failed to parse Aerospike URL")
}
