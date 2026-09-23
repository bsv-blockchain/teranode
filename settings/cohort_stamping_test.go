package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestCohortStamping_LoaderReadsKey guards the issue-556 cohort-stamping flag
// against the field-exists-but-loader-never-reads-it bug: the field carries a
// `key:` tag, but a struct tag is not wiring. If NewSettings() never calls
// getBool for it the field stays at the Go zero value, false, which is also the
// documented default — so a default-value assertion alone would pass even with
// the loader line missing.
//
// The honest test is therefore to set the key to true, call NewSettings(), and
// assert the field flipped.
func TestCohortStamping_LoaderReadsKey(t *testing.T) {
	const key = "utxostore_cohortStamping"

	// gocore resolves key.<context> first and strips suffixes down to the base
	// key, so a runtime Set on the base key is shadowed by any context-qualified
	// entry in settings.conf / settings_local.conf. Set at the precedence that
	// wins under the *ambient* context so the test is hermetic in dev, dev.<user>,
	// docker.m and so on.
	ctx := gocore.Config().GetContext()
	winKey := key
	if ctx != "" {
		winKey = key + "." + ctx
	}

	// Default must be off: with the flag off nothing is stamped at all.
	require.False(t, NewSettings().UtxoStore.CohortStamping,
		"cohort stamping must default to off")

	gocore.Config().Set(winKey, "true")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.True(t, NewSettings().UtxoStore.CohortStamping,
		"loader must read %s under context %q", key, ctx)
}
