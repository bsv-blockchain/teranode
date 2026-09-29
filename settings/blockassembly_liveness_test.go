package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLivenessStallTimeout_LoaderReadsKey guards blockassembly_livenessStallTimeout
// against the field-exists-but-loader-never-reads-it bug: the field carries a
// `key:` tag and a full longdesc, but if NewSettings() does not call getDuration
// for it the value stays at Go zero, the documented setting is silently
// unreadable, and the liveness watchdog it controls can never be turned on.
//
// This is not hypothetical — that is exactly how this setting was first written,
// and review caught it only because someone grepped settings.go by hand.
//
// The default is 0, which IS the Go zero value, so asserting the default alone
// would pass even with the loader line deleted. The honest test sets a non-zero
// override and asserts the field changed.
func TestLivenessStallTimeout_LoaderReadsKey(t *testing.T) {
	const key = "blockassembly_livenessStallTimeout"

	// Opt-in by default: until an operator chooses a value the probe must not be
	// able to restart anything.
	require.Zero(t, NewSettings().BlockAssembly.LivenessStallTimeout,
		"the watchdog must ship disabled so merging it changes no deployment's behaviour")

	gocore.Config().Set(key, "7m")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	require.Equal(t, 7*time.Minute, NewSettings().BlockAssembly.LivenessStallTimeout,
		"the loader must read %s, or the setting is dead and the watchdog cannot be enabled", key)
}
