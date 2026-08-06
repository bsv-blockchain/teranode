package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestValidatorKafkaBackpressureSettings_Defaults pins the shipped defaults: the
// controller is disabled, and the watermarks/timeouts match the documented
// values. These keys are new, so no settings.conf context overrides them.
func TestValidatorKafkaBackpressureSettings_Defaults(t *testing.T) {
	kbp := NewSettings().Validator.KafkaBackpressure

	require.False(t, kbp.Enabled, "controller must ship disabled")
	require.Equal(t, 500*time.Millisecond, kbp.PauseQueueAge)
	require.Equal(t, 100*time.Millisecond, kbp.ResumeQueueAge)
	require.Equal(t, 50*time.Millisecond, kbp.PollInterval)
	require.Equal(t, 100*time.Millisecond, kbp.ReadTimeout)
	require.Equal(t, 30*time.Second, kbp.MaxPause)
	require.Equal(t, 3, kbp.StaleErrorLimit)
}

// TestValidatorKafkaBackpressureSettings_LoaderReadsKeys guards against the
// field-exists-but-loader-never-reads-it bug by reading non-default overrides
// back through NewSettings.
func TestValidatorKafkaBackpressureSettings_LoaderReadsKeys(t *testing.T) {
	t.Setenv("validator_kafkaBackpressureEnabled", "true")
	t.Setenv("validator_kafkaBackpressurePauseQueueAge", "1s")
	t.Setenv("validator_kafkaBackpressureResumeQueueAge", "250ms")
	t.Setenv("validator_kafkaBackpressurePollInterval", "20ms")
	t.Setenv("validator_kafkaBackpressureReadTimeout", "80ms")
	t.Setenv("validator_kafkaBackpressureMaxPause", "45s")
	t.Setenv("validator_kafkaBackpressureStaleErrorLimit", "5")

	kbp := NewSettings().Validator.KafkaBackpressure

	require.True(t, kbp.Enabled)
	require.Equal(t, time.Second, kbp.PauseQueueAge)
	require.Equal(t, 250*time.Millisecond, kbp.ResumeQueueAge)
	require.Equal(t, 20*time.Millisecond, kbp.PollInterval)
	require.Equal(t, 80*time.Millisecond, kbp.ReadTimeout)
	require.Equal(t, 45*time.Second, kbp.MaxPause)
	require.Equal(t, 5, kbp.StaleErrorLimit)
}

// TestValidatorKafkaBackpressureSettings_ResumeClamp verifies a resume watermark
// that is not strictly below the pause watermark is clamped to half the pause
// watermark, while the controller stays enabled.
func TestValidatorKafkaBackpressureSettings_ResumeClamp(t *testing.T) {
	t.Setenv("validator_kafkaBackpressureEnabled", "true")
	t.Setenv("validator_kafkaBackpressurePauseQueueAge", "200ms")
	t.Setenv("validator_kafkaBackpressureResumeQueueAge", "300ms")

	kbp := NewSettings().Validator.KafkaBackpressure

	require.True(t, kbp.Enabled, "a clampable resume watermark keeps the controller enabled")
	require.Equal(t, 100*time.Millisecond, kbp.ResumeQueueAge, "resume clamped to half the pause watermark")
}

// TestValidatorKafkaBackpressureSettings_StaleErrorLimitFloor verifies a
// non-positive stale-error limit is floored at 1 so a single transient read
// error can never immediately fail open.
func TestValidatorKafkaBackpressureSettings_StaleErrorLimitFloor(t *testing.T) {
	t.Setenv("validator_kafkaBackpressureEnabled", "true")
	t.Setenv("validator_kafkaBackpressureStaleErrorLimit", "0")

	kbp := NewSettings().Validator.KafkaBackpressure

	require.True(t, kbp.Enabled)
	require.Equal(t, 1, kbp.StaleErrorLimit)
}

// TestValidatorKafkaBackpressureSettings_IncoherentForcesDisable verifies each
// non-positive timing knob forces the controller off even when explicitly
// enabled.
func TestValidatorKafkaBackpressureSettings_IncoherentForcesDisable(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"pauseQueueAge", "validator_kafkaBackpressurePauseQueueAge"},
		{"pollInterval", "validator_kafkaBackpressurePollInterval"},
		{"readTimeout", "validator_kafkaBackpressureReadTimeout"},
		{"maxPause", "validator_kafkaBackpressureMaxPause"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("validator_kafkaBackpressureEnabled", "true")
			t.Setenv(tc.key, "0s")

			kbp := NewSettings().Validator.KafkaBackpressure

			require.False(t, kbp.Enabled, "non-positive %s must force the controller off", tc.name)
		})
	}
}
