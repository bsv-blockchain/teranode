package util

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// captureLogger records the FORMATTED message of each Errorf call — i.e. the
// record as SignalBatchPanic built it, before any terminator a real logger
// would append. Asserting single-line-ness on that message keeps the test
// honest: a logger that appends its own trailing newline per record would
// otherwise fail the assertion for the wrong reason.
type captureLogger struct {
	ulogger.TestLogger

	mu   sync.Mutex
	msgs []string
}

func (l *captureLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.msgs = append(l.msgs, fmt.Sprintf(format, args...))
}

func (l *captureLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.msgs...)
}

// TestSignalBatchPanic verifies the panic safety net used by every batcher
// dispatch fn: on a recovered panic it must complete EVERY item exactly once,
// so go-batcher swallowing the panic can no longer orphan the waiting submitter
// goroutines. The test signal mirrors production's CAS-guarded it.complete.
func TestSignalBatchPanic(t *testing.T) {
	logger := ulogger.TestLogger{}

	// item + signal mirror the production completion contract: complete() is
	// CAS-guarded, so signalling an item an earlier stage already completed is a
	// no-op.
	type item struct {
		completed atomic.Bool
		result    error
	}

	signal := func(it *item, err error) {
		if it.completed.CompareAndSwap(false, true) {
			it.result = err
		}
	}

	t.Run("nil recovered is a no-op", func(t *testing.T) {
		batch := []*item{{}}
		handled := SignalBatchPanic(recover(), batch, "test", logger, signal)
		require.False(t, handled)
		require.False(t, batch[0].completed.Load(), "no item should be completed when nothing was recovered")
	})

	t.Run("panic completes every item exactly once", func(t *testing.T) {
		const n = 16
		batch := make([]*item, n)
		for i := range batch {
			batch[i] = &item{}
		}

		handled := SignalBatchPanic("boom", batch, "sendGetBatch", logger, signal)
		require.True(t, handled)

		for i, it := range batch {
			require.True(t, it.completed.Load(), "item %d must be completed after the panic sweep", i)
			require.Error(t, it.result, "item %d must carry an error", i)
			require.Contains(t, it.result.Error(), "panic in sendGetBatch")
		}
	})

	t.Run("already-completed item is not clobbered", func(t *testing.T) {
		// Mimics a dispatch fn that completed some items before panicking: the
		// panic fan-out must be a CAS-guarded no-op for those, preserving each
		// item's real result rather than overwriting it with the panic error.
		it := &item{}
		signal(it, errors.NewProcessingError("original result")) // already completed

		SignalBatchPanic("boom", []*item{it}, "test", logger, signal)

		require.Contains(t, it.result.Error(), "original result")
		require.NotContains(t, it.result.Error(), "panic in", "the panic sweep must not clobber an already-completed item")
	})
}

// TestSignalBatchPanic_LogIsSingleLine pins the %q decision behind the log
// format. The panic value is deliberately multi-line: a single-line panic value
// would pass even if only the stack were escaped.
func TestSignalBatchPanic_LogIsSingleLine(t *testing.T) {
	logger := &captureLogger{}

	type item struct{ result error }

	batch := []*item{{}, {}}

	handled := SignalBatchPanic("first\nsecond", batch, "sendGetBatch", logger, func(it *item, err error) {
		it.result = err
	})
	require.True(t, handled)

	msgs := logger.messages()
	require.Len(t, msgs, 1, "the sweep must emit exactly one record")

	msg := msgs[0]

	// Nothing was truncated — only escaped.
	require.Contains(t, msg, "goroutine", "the stack must still be in the record")
	require.Contains(t, msg, "first")
	require.Contains(t, msg, "second")

	require.NotContains(t, msg, "\n", "the record must stay on a single line")
}
