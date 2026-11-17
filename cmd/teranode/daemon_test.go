package teranode

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/stretchr/testify/require"
)

// TestInitSemaphoresAppliesAndIsOnce covers InitSemaphores, which RunDaemon calls
// and nothing else does.
//
// It lives in cmd/teranode rather than beside the code it exercises, and that is
// deliberate. InitSemaphores replaces the package-level semaphores in
// stores/blob/file, and the tests in that package leave goroutines holding write
// permits after they return — TestSemaphoreExhaustion starts its writes in
// goroutines its WaitGroup does not track. A permit taken from one semaphore and
// released against a replacement panics with "released more than held", because
// the new semaphore has no record of it. Running here keeps the swap in a binary
// that performs no file operations at all, so the ordering can never arise.
// Coverage still lands on stores/blob/file/file.go: the Makefile builds the
// profile with -coverpkg=./..., which attributes lines to the file that owns
// them rather than to the test binary that ran them.
//
// useSystemLimits is off so the result does not depend on the open-file limit of
// whichever machine runs the test. fdlimit.Budget() is read before that flag is
// consulted, so the descriptor read is exercised either way.
func TestInitSemaphoresAppliesAndIsOnce(t *testing.T) {
	applied, err := file.InitSemaphores(768, 256, false)
	require.NoError(t, err)
	require.Equal(t, file.Limits{Read: 768, Write: 256, Clamped: false}, applied,
		"with useSystemLimits off the configured concurrency stands, whatever the open-file limit")

	// A sync.Once guards the body, so a later call must report what the first
	// call settled on rather than re-deciding. The arguments here are past the
	// accepted range, so a first call would have rejected them — passing them
	// proves the short-circuit rather than merely that the same numbers came
	// back from re-running the same decision.
	const pastTheMaximum = file.MaxSemaphoreLimit + 1

	repeat, err := file.InitSemaphores(pastTheMaximum, 1, true)
	require.NoError(t, err, "a repeat call must not re-run validation")
	require.Equal(t, applied, repeat, "a repeat call must report the limits already in force")
}
