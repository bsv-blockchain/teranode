package main

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestRunRejectsUnparseableSeedURLWithoutEchoingIt covers the credential half
// of an invalid --seed-url. The flag can carry userinfo, and url.Parse embeds
// the string it was given verbatim in its error, so neither the value nor that
// error may reach the message.
func TestRunRejectsUnparseableSeedURLWithoutEchoingIt(t *testing.T) {
	const password = "canary-seed-url-password"

	// A space in the host is what makes url.Parse refuse it.
	seedURL := "http://teranode:" + password + "@blob server:8080/seed"

	// The secp256k1 generator point, compressed. Any valid pubkey will do: it
	// only has to get run() past the trusted-key check and as far as the URL.
	const authorityPubKey = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"

	err := run(context.Background(), ulogger.TestLogger{}, &settings.Settings{},
		"0000000000000000000000000000000000000000000000000000000000000001", seedURL, authorityPubKey)
	require.Error(t, err)

	require.NotContains(t, err.Error(), password, "the seed URL password reached the error message")
	require.NotContains(t, err.Error(), "teranode:", "the seed URL userinfo reached the error message")

	// The operator still learns why it was rejected. Asserting only the prefix
	// let this test pass on "invalid --seed-url: %v -> ...", where the verb
	// survived unrendered because errors.NewConfigurationError takes a trailing
	// error as the wrapped error and then never reaches fmt.Errorf. Pin the
	// reason and the absence of a verb, not the prefix.
	require.True(t, strings.Contains(err.Error(), "invalid --seed-url"),
		"expected the parse failure to be reported, got: %v", err)
	require.Contains(t, strings.ToLower(err.Error()), "invalid character",
		"expected url.Parse's own reason to survive, got: %v", err)
	require.NotContains(t, err.Error(), "%", "the message renders a format verb literally")
}
