package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyQueueSettings_Defaults pins the shipped defaults for the
// ingest-queue bound: the cap is 0 (disabled) so a freshly upgraded node keeps
// today's unbounded behaviour until an operator opts in, and the bounded wait is
// 100ms. These keys are new, so no settings.conf context overrides them.
func TestBlockAssemblyQueueSettings_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.Equal(t, int64(0), tSettings.BlockAssembly.MaxQueueItems,
		"default cap must be 0 (disabled) — enablement is a separate evidence-backed change")
	require.Equal(t, 100*time.Millisecond, tSettings.BlockAssembly.QueueFullWaitTimeout,
		"default bounded wait must be 100ms")
}

// TestBlockAssemblyQueueSettings_LoaderReadsKeys guards against the
// field-exists-but-loader-never-reads-it bug: MaxQueueItems defaults to the Go
// zero value, so only a non-zero override read back proves NewSettings wires the
// key.
func TestBlockAssemblyQueueSettings_LoaderReadsKeys(t *testing.T) {
	t.Setenv("blockassembly_maxQueueItems", "16777216")
	t.Setenv("blockassembly_queueFullWaitTimeout", "250ms")

	tSettings := NewSettings()

	require.Equal(t, int64(16_777_216), tSettings.BlockAssembly.MaxQueueItems,
		"loader must read blockassembly_maxQueueItems")
	require.Equal(t, 250*time.Millisecond, tSettings.BlockAssembly.QueueFullWaitTimeout,
		"loader must read blockassembly_queueFullWaitTimeout")
}
