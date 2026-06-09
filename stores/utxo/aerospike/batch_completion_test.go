package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestSignalBatchPanic verifies the panic safety net used by every batcher
// dispatch fn: on a recovered panic it must surface an error on EVERY item's
// completion channel, so go-batcher swallowing the panic can no longer orphan
// the waiting submitter goroutines.
func TestSignalBatchPanic(t *testing.T) {
	InitPrometheusMetrics()

	logger := ulogger.TestLogger{}

	type item struct {
		done chan error
	}

	signal := func(it *item, err error) {
		util.SafeSend(it.done, err, batchSignalTimeout)
	}

	t.Run("nil recovered is a no-op", func(t *testing.T) {
		batch := []*item{{done: make(chan error, 1)}}
		handled := signalBatchPanic(recover(), batch, "test", logger, signal)
		require.False(t, handled)
		require.Len(t, batch[0].done, 0, "no signal should be sent when nothing was recovered")
	})

	t.Run("panic signals every waiter exactly once", func(t *testing.T) {
		const n = 16
		batch := make([]*item, n)
		for i := range batch {
			batch[i] = &item{done: make(chan error, 1)}
		}

		handled := signalBatchPanic("boom", batch, "sendGetBatch", logger, signal)
		require.True(t, handled)

		for i, it := range batch {
			select {
			case err := <-it.done:
				require.Error(t, err, "item %d must receive an error", i)
				require.Contains(t, err.Error(), "panic in sendGetBatch")
			case <-time.After(time.Second):
				t.Fatalf("item %d was orphaned: no completion signal after panic", i)
			}
		}
	})

	t.Run("already-signalled buffered-1 waiter does not block the worker", func(t *testing.T) {
		// Mimics a dispatch fn that signalled some items before panicking: the
		// channel buffer is already full, and the panic fan-out must not block.
		it := &item{done: make(chan error, 1)}
		it.done <- nil // pre-existing result

		done := make(chan struct{})
		go func() {
			signalBatchPanic("boom", []*item{it}, "test", logger, signal)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * batchSignalTimeout):
			t.Fatal("signalBatchPanic blocked on a full buffered channel")
		}
	})
}
