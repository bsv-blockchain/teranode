package teraslab

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// Close drains every in-flight batched write and releases the backing TeraSlab
// client (connection pools, cluster connections).
//
// The four batchers (store, get, spend, locked) each own a background worker
// goroutine. Their Close() drains all queued items through the flush fn and
// blocks until the worker has fully unwound, so callers that already received a
// successful response for a queued Create/Spend/SetLocked are guaranteed the
// write reached the server before Close returns.
//
// Unlike the Aerospike backend there is no inter-batcher drain dependency here:
// each send*Batch writes directly to the client and replies on per-item done
// channels — no flush callback enqueues into another batcher. We therefore drain
// the read-only get batcher first, then the state-mutating writers (store,
// spend, locked) so they have the best chance of committing before the deadline.
//
// The supplied context bounds the drain. The drain runs in a goroutine; if ctx
// expires first, Close returns ctx.Err() ("drain not confirmed complete") while
// the drain and client teardown continue best-effort in the background. The
// client is always closed inside the goroutine — even when ctx has already
// expired — so its connections are never leaked.
//
// After Close returns no further Store operations are valid; calling any batched
// op after Close will panic on send to a closed channel (go-batcher contract).
func (s *Store) Close(ctx context.Context) error {
	done := make(chan struct{})

	var closeErr error

	go func() {
		defer close(done)

		// Read-only batcher first.
		if s.getBatcher != nil {
			s.getBatcher.Close()
		}
		// State-mutating writers last so they drain closest to the deadline.
		if s.storeBatcher != nil {
			s.storeBatcher.Close()
		}
		if s.spendBatcher != nil {
			s.spendBatcher.Close()
		}
		if s.lockedBatcher != nil {
			s.lockedBatcher.Close()
		}

		// Always close the client after the batchers drain, even if ctx has
		// already expired, so the connection pool / cluster conns are not leaked.
		if s.client != nil {
			if err := s.client.Close(); err != nil {
				closeErr = errors.NewStorageError("teraslab client close", err)
			}
		}
	}()

	select {
	case <-done:
		return closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
