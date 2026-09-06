package settings

import (
	"strings"
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
	require.Equal(t, time.Second, kbp.MaxFailOpenCooldown)
	require.Equal(t, 3, kbp.StaleErrorLimit)
	require.Equal(t, 90, kbp.PauseQueueFillPercent)
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
	t.Setenv("validator_kafkaBackpressureMaxFailOpenCooldown", "2s")
	t.Setenv("validator_kafkaBackpressureStaleErrorLimit", "5")
	t.Setenv("validator_kafkaBackpressurePauseFillPercent", "75")

	kbp := NewSettings().Validator.KafkaBackpressure

	require.True(t, kbp.Enabled)
	require.Equal(t, time.Second, kbp.PauseQueueAge)
	require.Equal(t, 250*time.Millisecond, kbp.ResumeQueueAge)
	require.Equal(t, 20*time.Millisecond, kbp.PollInterval)
	require.Equal(t, 80*time.Millisecond, kbp.ReadTimeout)
	require.Equal(t, 45*time.Second, kbp.MaxPause)
	require.Equal(t, 2*time.Second, kbp.MaxFailOpenCooldown)
	require.Equal(t, 5, kbp.StaleErrorLimit)
	require.Equal(t, 75, kbp.PauseQueueFillPercent)
}

// A percentage outside 0..100 cannot express a fill fraction. Negative must become
// 0 — the predicate off, age-only behaviour — rather than being read as "always
// hot", and anything above 100 must become 100. Both report, because a silent
// clamp on a control input is how a controller ends up doing something the
// operator did not ask for.
func TestValidatorKafkaBackpressureSettings_PauseFillPercentClamps(t *testing.T) {
	base := func() ValidatorKafkaBackpressureSettings {
		return ValidatorKafkaBackpressureSettings{
			Enabled:             true,
			PauseQueueAge:       500 * time.Millisecond,
			ResumeQueueAge:      100 * time.Millisecond,
			PollInterval:        50 * time.Millisecond,
			ReadTimeout:         100 * time.Millisecond,
			MaxPause:            30 * time.Second,
			MaxFailOpenCooldown: time.Second,
			StaleErrorLimit:     3,
		}
	}

	t.Run("negative clamps to zero and reports", func(t *testing.T) {
		cfg := base()
		cfg.PauseQueueFillPercent = -5

		got, warnings := cfg.validated()

		require.Len(t, warnings, 1, "only the fill clamp fires: every other knob is coherent")
		require.Contains(t, warnings[0], "pauseFillPercent")
		require.Equal(t, 0, got.PauseQueueFillPercent)
		require.True(t, got.Enabled, "a clampable fill watermark keeps the controller enabled")
	})

	t.Run("above 100 clamps to 100 and reports", func(t *testing.T) {
		cfg := base()
		cfg.PauseQueueFillPercent = 140

		got, warnings := cfg.validated()

		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0], "pauseFillPercent")
		require.Equal(t, 100, got.PauseQueueFillPercent)
		require.True(t, got.Enabled)
	})

	t.Run("zero and 100 are both legal and silent", func(t *testing.T) {
		for _, pct := range []int{0, 100} {
			cfg := base()
			cfg.PauseQueueFillPercent = pct

			got, warnings := cfg.validated()

			require.Empty(t, warnings)
			require.Equal(t, pct, got.PauseQueueFillPercent)
		}
	})
}

// TestValidatorKafkaBackpressureSettings_MaxFailOpenCooldownFloor verifies the cap
// is floored at twice the poll interval.
//
// The floor is what stops a flapping signal busy-toggling pause/resume on
// consecutive ticks: a cap below one poll interval would leave the cooldown unable
// to span even a single tick, so the suppression would be no suppression at all. It
// is a floor on the CAP; the controller applies the same floor to each armed
// cooldown.
func TestValidatorKafkaBackpressureSettings_MaxFailOpenCooldownFloor(t *testing.T) {
	t.Setenv("validator_kafkaBackpressureEnabled", "true")
	t.Setenv("validator_kafkaBackpressurePollInterval", "50ms")
	t.Setenv("validator_kafkaBackpressureMaxFailOpenCooldown", "10ms")

	kbp := NewSettings().Validator.KafkaBackpressure

	require.True(t, kbp.Enabled, "a clampable cooldown cap keeps the controller enabled")
	require.Equal(t, 100*time.Millisecond, kbp.MaxFailOpenCooldown, "clamped up to 2 x pollInterval")
}

// TestValidatorSettings_BlockAssemblyShedRetryTimeout pins the default and the
// loader for the bound on the in-place block-assembly handoff retry. Without a
// bound, an ingest goroutine parks for the whole duration of a stall while still
// holding its Kafka record batch, so the growth the block-assembly queue cap removed
// reappears in the validator.
func TestValidatorSettings_BlockAssemblyShedRetryTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		require.Equal(t, 2*time.Second, NewSettings().Validator.BlockAssemblyShedRetryTimeout)
	})

	t.Run("loader reads the key", func(t *testing.T) {
		t.Setenv("validator_blockAssemblyShedRetryTimeout", "750ms")

		require.Equal(t, 750*time.Millisecond, NewSettings().Validator.BlockAssemblyShedRetryTimeout)
	})
}

// TestValidatorSettings_ShedUnwindTimeout pins the default and the loader for the
// bound on the shed unwind. It was a compiled-in constant, which left the operator
// remedy for a store too slow to finish the unwind inside the cap — raise the cap —
// unactionable without a rebuild.
func TestValidatorSettings_ShedUnwindTimeout(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		require.Equal(t, 2*time.Second, NewSettings().Validator.ShedUnwindTimeout)
	})

	t.Run("loader reads the key", func(t *testing.T) {
		t.Setenv("validator_shedUnwindTimeout", "5s")

		require.Equal(t, 5*time.Second, NewSettings().Validator.ShedUnwindTimeout)
	})
}

// TestValidatorSettings_HandoffRoundTripSlack pins the default and the loader for the
// per-attempt handoff margin. It is the only term that widens the handoff deadline, so
// it is the only effective remedy for the split-deployment settings skew the validator
// already warns about at runtime.
func TestValidatorSettings_HandoffRoundTripSlack(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		require.Equal(t, 500*time.Millisecond, NewSettings().Validator.HandoffRoundTripSlack)
	})

	t.Run("loader reads the key", func(t *testing.T) {
		t.Setenv("validator_handoffRoundTripSlack", "1500ms")

		require.Equal(t, 1500*time.Millisecond, NewSettings().Validator.HandoffRoundTripSlack)
	})
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

	// A negative resume watermark is a distinct violation from a resume that is not
	// strictly below the pause watermark: the "< pause" check passes for a negative
	// value against a positive pause, so the clamp below it is the only place this
	// shape can be reported. It used to clamp silently, which contradicted the
	// contract on validated() that every violation is reported.
	t.Run("negative resume is clamped and reported", func(t *testing.T) {
		cfg, warnings := ValidatorKafkaBackpressureSettings{
			Enabled:             true,
			PauseQueueAge:       500 * time.Millisecond,
			ResumeQueueAge:      -50 * time.Millisecond,
			PollInterval:        50 * time.Millisecond,
			ReadTimeout:         100 * time.Millisecond,
			MaxPause:            30 * time.Second,
			MaxFailOpenCooldown: time.Second,
			StaleErrorLimit:     3,
		}.validated()

		require.Len(t, warnings, 1, "only the negative clamp fires: every other knob is coherent")
		require.Contains(t, warnings[0], "resumeQueueAge")
		require.Equal(t, time.Duration(0), cfg.ResumeQueueAge)
		require.True(t, cfg.Enabled, "a clampable resume watermark keeps the controller enabled")
	})
}

// TestValidatorKafkaBackpressureSettings_StaleErrorLimitFloor verifies a
// non-positive stale-error limit is floored at 1 so the threshold is always a
// meaningful positive count.
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

// TestValidatorKafkaBackpressureSettings_AllBadKnobsReported pins that EVERY
// violation in a config is reported, not just the first.
//
// Each "disable the controller" check both warns and clears Enabled. Gating the
// warnings on the current value of Enabled therefore silenced every check after the
// first, so an operator with two bad knobs fixed one, restarted, and rediscovered the
// second. The gate is wasEnabled, captured once before any check can clear it.
//
// Asserted against validated() directly rather than through stderr: the warnings are
// returned to the caller, which is what makes this testable at all.
func TestValidatorKafkaBackpressureSettings_AllBadKnobsReported(t *testing.T) {
	t.Run("every bad knob is reported in one pass", func(t *testing.T) {
		cfg, warnings := ValidatorKafkaBackpressureSettings{
			Enabled: true,
			// All four disabling knobs are non-positive at once.
			PauseQueueAge: 0,
			PollInterval:  0,
			ReadTimeout:   0,
			MaxPause:      0,
			// Left coherent so they cannot add warnings of their own and inflate the
			// count: ResumeQueueAge < PauseQueueAge fails when both are 0, so the resume
			// clamp fires too — accounted for below.
			MaxFailOpenCooldown: time.Second,
			StaleErrorLimit:     3,
		}.validated()

		require.False(t, cfg.Enabled, "any non-positive timing knob disables the controller")

		joined := strings.Join(warnings, "\n")

		for _, knob := range []string{"pauseQueueAge", "pollInterval", "readTimeout", "maxPause"} {
			require.Contains(t, joined, knob, "every bad knob must be named, not just the first")
		}

		// Four disabling knobs plus the resume clamp, which is reachable here because
		// ResumeQueueAge (0) is not strictly below PauseQueueAge (0).
		require.Len(t, warnings, 5)
	})

	t.Run("a config that was never enabled stays silent", func(t *testing.T) {
		cfg, warnings := ValidatorKafkaBackpressureSettings{
			Enabled:       false,
			PauseQueueAge: 0,
			PollInterval:  0,
			ReadTimeout:   0,
			MaxPause:      0,
		}.validated()

		require.False(t, cfg.Enabled)
		require.Empty(t, warnings, "wasEnabled must not start warning operators who never enabled the controller")
	})
}
