package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestSkipTxBodyBelowCheckpoint_LoaderReadsKey guards the utxoset body-skip switch against the
// field-exists-but-loader-never-reads-it bug: a `key:` struct tag loads nothing on its own, so
// without a getBool in NewSettings the field stays false and the documented setting is
// silently unreadable.
//
// The default is false, which is also the Go zero value, so asserting the default alone would
// pass against a loader that never ran. The honest test sets the key and reads it back.
func TestSkipTxBodyBelowCheckpoint_LoaderReadsKey(t *testing.T) {
	const key = "utxostore_skipTxBodyBelowCheckpoint"

	require.False(t, NewSettings().UtxoStore.SkipTxBodyBelowCheckpoint,
		"the default must be off: skipping bodies is opt-in")

	// t.Setenv rather than gocore.Config().Set, for two reasons. An environment variable wins
	// over every settings.conf entry in gocore's lookup (config.go getInternal checks
	// os.LookupEnv first), so the test is hermetic whatever context the run carries and needs
	// no key.<context> dance. And t.Setenv RESTORES the previous state, where a Set has to be
	// undone by hand and the only undo available is setting the key to "", which is a value
	// rather than an absence and leaves the config dirty for the rest of the binary.
	t.Setenv(key, "true")

	require.True(t, NewSettings().UtxoStore.SkipTxBodyBelowCheckpoint,
		"loader must read %s under context %q", key, gocore.Config().GetContext())
}
