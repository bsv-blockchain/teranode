package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestParkWriteConcurrency pins the two rules the limit follows, because getting
// either wrong turns a parallel loop back into a serial one or into a queue.
//
// The subtree write loop used to be serial, and on mainnet block 759245 that was
// 34.5 seconds writing 25 subtrees while 47 of 48 cores idled. The limit now
// derives from the blob store's own write concurrency, so the loop cannot
// over-subscribe it and convert parallelism into waiting for permits, and it is
// capped for the heap, which is the real constraint while a whole decoded block
// is resident against a soft memory limit.
func TestParkWriteConcurrency(t *testing.T) {
	set := func(permits int) *settings.Settings {
		s := &settings.Settings{}
		s.Block.FileStoreWriteConcurrency = permits

		return s
	}

	// Mainnet's value. Sixty-four permits over three artefacts is twenty-one
	// possible writers, so the heap cap is what binds, not the store.
	require.Equal(t, maxConcurrentSubtreeWrites, parkWriteConcurrency(set(64)),
		"with permits to spare the heap cap is the limit")

	// A store configured tighter than the cap must win, or the loop asks for
	// more permits than exist and the extra goroutines only queue.
	require.Equal(t, 2, parkWriteConcurrency(set(6)),
		"six permits over three artefacts is two writers, and the store's limit must win")

	// Never zero, never negative: a limit of zero on an errgroup would block
	// every goroutine forever, turning a slow write phase into a wedged one.
	require.Equal(t, 1, parkWriteConcurrency(set(3)))
	require.Equal(t, 1, parkWriteConcurrency(set(1)))
	require.Equal(t, 1, parkWriteConcurrency(set(0)))
	require.Equal(t, 1, parkWriteConcurrency(set(-5)))

	// Tests build SyncManager as a struct literal with no settings at all.
	require.Equal(t, 1, parkWriteConcurrency(nil))
}
