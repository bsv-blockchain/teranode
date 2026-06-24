package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAerospikeSemaphoreMultiplier_Default exists to guard against a class of
// bug where a setting carries a struct-tag default but is missing from the
// imperative loader in NewSettings. The struct tag is documentation-only —
// only the explicit getFloat64 call in settings.go actually populates the
// runtime value. Without that loader entry SemaphoreMultiplier is the zero
// value (0.0), which disables the in-process uaerospike semaphore by
// default for every deployment without an explicit override — the opposite
// of the documented "1.0 preserves prior behavior".
func TestAerospikeSemaphoreMultiplier_Default(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.InDelta(t, 1.0, tSettings.Aerospike.SemaphoreMultiplier, 0,
		"default SemaphoreMultiplier must be 1.0; got %v. "+
			"If this fails the loader in settings.go is missing the "+
			`getFloat64("aerospike_semaphore_multiplier", 1.0, alternativeContext...)`+
			" entry and the in-process semaphore is silently disabled in prod.",
		tSettings.Aerospike.SemaphoreMultiplier)
}

// TestAerospikeOverloadRetry_Defaults guards the same loader-vs-struct-tag
// class of bug for the overload-retry settings. A missing loader entry would
// leave OverloadRetryMaxElapsed at 0, silently disabling the uaerospike
// overload-retry mitigation in every deployment.
func TestAerospikeOverloadRetry_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.Equal(t, 2*time.Minute, tSettings.Aerospike.OverloadRetryMaxElapsed,
		"default OverloadRetryMaxElapsed must be 2m; a zero value disables overload retry in prod")
	require.Equal(t, 50*time.Millisecond, tSettings.Aerospike.OverloadRetryBaseBackoff)
	require.Equal(t, 5*time.Second, tSettings.Aerospike.OverloadRetryMaxBackoff)
}
