package blockpersister

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// The window tests run inside a synctest bubble so they never guess at scheduling. synctest.Wait
// returns only once every goroutine in the bubble is durably blocked, and a WriteBatch call held
// back by the window is durably blocked in sync.Cond.Wait, so after it every batch sent has either
// parked in pending or is waiting. The context and the storer, whose background reader blocks on
// a pipe, are created inside the bubble so that they count. The memory store is created outside
// it: its TTL cleaner runs until the store is closed, and the bubble would wait for it.
//
// Spawned goroutines record their errors rather than calling require, because FailNow is only
// valid on the goroutine running the test.

// newMemoryStore returns a memory blob store that is closed when the test ends.
func newMemoryStore(t *testing.T) *memory.Memory {
	t.Helper()

	store := memory.New()
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

// newBubbledWriter creates a SubtreeDataWriter whose storer streams to store under key. Call it
// inside the bubble, and defer writer.Abort(nil). A test that fails before it closes the writer
// leaves the storer's reader blocked on its pipe and batches blocked on the window, and the
// bubble then reports a deadlock on top of the failure. A deferred Abort runs when FailNow exits
// the goroutine and wakes them all; a t.Cleanup would run only after the bubble's goroutines have
// exited, which is too late. After a Close the Abort does nothing.
func newBubbledWriter(t *testing.T, ctx context.Context, tSettings *settings.Settings, store *memory.Memory, key []byte, window int) *SubtreeDataWriter {
	t.Helper()

	storer, err := filestorer.NewFileStorer(ctx, ulogger.TestLogger{}, tSettings, store, key, fileformat.FileTypeDat)
	require.NoError(t, err)

	return NewSubtreeDataWriter(ctx, storer, window)
}

// pendingLen is how many batches the writer has parked.
func pendingLen(w *SubtreeDataWriter) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.pending)
}

// TestSubtreeDataWriter_PendingNeverExceedsTheWindow holds batch 0 back while every later batch
// arrives, the order one slow fetch produces. Without a bound every later batch parks in pending,
// and on the fresh-create path pending grows to the whole subtree (issue 1916). Batch 3, the first
// past the opening window, is held back too, so the bound is checked again once the window has
// moved. pending is read only when every goroutine in the bubble is blocked. Between those points
// it can briefly hold window batches, when batch nextIndex is inserted before the drain, which no
// test can observe.
func TestSubtreeDataWriter_PendingNeverExceedsTheWindow(t *testing.T) {
	const (
		window     = 3
		batchCount = 10
	)

	tSettings := test.CreateBaseTestSettings(t)
	store := newMemoryStore(t)
	key := []byte("test-pending-window")

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		writer := newBubbledWriter(t, ctx, tSettings, store, key, window)
		defer writer.Abort(nil)

		var (
			wg       sync.WaitGroup
			returned atomic.Int32
			errs     [batchCount]error
		)

		for i := 1; i < batchCount; i++ {
			if i == window {
				continue
			}

			wg.Go(func() {
				errs[i] = writer.WriteBatch(i, [][]byte{{byte(i)}})

				returned.Add(1)
			})
		}

		synctest.Wait()

		// Batches 1 and 2 are inside the window and parked. Batches 4 to 9 wait for batch 0.
		require.Equal(t, window-1, pendingLen(writer))
		require.EqualValues(t, window-1, returned.Load())

		// Batch 0 drains 1 and 2, and the window moves to 3 to 5. Batches 4 and 5 park, and
		// 6 to 9 wait for batch 3.
		require.NoError(t, writer.WriteBatch(0, [][]byte{{0}}))
		synctest.Wait()

		require.Equal(t, window-1, pendingLen(writer))
		require.EqualValues(t, 2*(window-1), returned.Load())

		require.NoError(t, writer.WriteBatch(window, [][]byte{{window}}))
		waitOrFail(t, &wg, writer, "batches waiting on the window did not return after batches 0 and 3 were written")

		for i, err := range errs {
			require.NoError(t, err, "batch %d", i)
		}

		require.Zero(t, pendingLen(writer))
		require.NoError(t, writer.Close(ctx))
	})

	data, err := store.Get(context.Background(), key, fileformat.FileTypeDat)
	require.NoError(t, err)

	want := make([]byte, batchCount)
	for i := range want {
		want[i] = byte(i)
	}

	require.Equal(t, want, data)
}

// waitOrFail waits for wg, and fails the test if that takes a minute. Inside a bubble the minute
// is fake: once every goroutine is durably blocked the clock jumps to it, so a WriteBatch call
// that never wakes fails the test at once instead of hanging the suite. Before failing it aborts
// the writer and waits for the calls to return, so the bubble ends cleanly.
func waitOrFail(t *testing.T, wg *sync.WaitGroup, writer *SubtreeDataWriter, msg string) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Minute):
		writer.Abort(nil)
		<-done
		t.Fatal(msg)
	}
}

// TestSubtreeDataWriter_WaitingBatchesReturnWhenTheWriterStops covers the failure the window
// brings with it. A batch that fails before it reaches WriteBatch never arrives, so nextIndex
// stops at it, and the batches waiting behind it must still return, or g.Wait never does.
// The errgroup cancels its context when a batch fails; Abort and a failed write stop the writer
// from the inside.
func TestSubtreeDataWriter_WaitingBatchesReturnWhenTheWriterStops(t *testing.T) {
	const (
		window     = 2
		batchCount = 6
	)

	tests := []struct {
		name    string
		stop    func(t *testing.T, cancel context.CancelFunc, writer *SubtreeDataWriter)
		wantErr error
	}{
		{
			name: "the context is cancelled",
			stop: func(_ *testing.T, cancel context.CancelFunc, _ *SubtreeDataWriter) {
				// Batch 0 failed and never reached the writer.
				cancel()
			},
			wantErr: errors.ErrContextCanceled,
		},
		{
			name: "the writer is aborted",
			stop: func(_ *testing.T, _ context.CancelFunc, writer *SubtreeDataWriter) {
				writer.Abort(errors.NewProcessingError("aborted by the test"))
			},
			wantErr: errors.ErrProcessing,
		},
		{
			name: "batch 0 fails to write",
			stop: func(t *testing.T, _ context.CancelFunc, writer *SubtreeDataWriter) {
				// With its storer gone, writing batch 0 fails, and the batches behind it can
				// never be written.
				writer.storer.Abort(nil)
				require.Error(t, writer.WriteBatch(0, [][]byte{{0}}))
			},
			wantErr: errors.ErrStorageError,
		},
	}

	tSettings := test.CreateBaseTestSettings(t)
	store := newMemoryStore(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				writer := newBubbledWriter(t, ctx, tSettings, store, []byte(tt.name), window)
				defer writer.Abort(nil)

				var (
					wg   sync.WaitGroup
					errs [batchCount]error
				)

				for i := 1; i < batchCount; i++ {
					wg.Go(func() { errs[i] = writer.WriteBatch(i, [][]byte{{byte(i)}}) })
				}

				// Batch 1 is parked. Batches 2 to 5 wait for batch 0.
				synctest.Wait()
				require.Equal(t, 1, pendingLen(writer))

				tt.stop(t, cancel, writer)
				waitOrFail(t, &wg, writer, "batches waiting on the window did not return after the writer stopped")

				require.NoError(t, errs[1], "batch 1 was parked before the writer stopped")

				for i := window; i < batchCount; i++ {
					require.Error(t, errs[i], "batch %d", i)
					require.True(t, errors.Is(errs[i], tt.wantErr), "batch %d: %v", i, errs[i])
				}
			})
		})
	}
}

// TestSubtreeDataWriter_InOrderDispatchAlwaysCompletes drives the writer the way
// CreateSubtreeDataFileStreaming does: one loop dispatches the batches in index order through an
// errgroup, then waits. Each batch takes longer the lower its index, so they finish in reverse
// order and the later ones pile up against the window. Across windows and errgroup limits, 1
// included, every batch is written in order, and when batch 0 fails before it reaches the writer,
// Wait returns batch 0's error instead of hanging. That is the argument NewSubtreeDataWriter's
// comment makes.
func TestSubtreeDataWriter_InOrderDispatchAlwaysCompletes(t *testing.T) {
	const batchCount = 12

	tSettings := test.CreateBaseTestSettings(t)
	store := newMemoryStore(t)

	for _, window := range []int{0, 1, 2, 4, batchCount + 1} {
		for _, limit := range []int{1, 2, 4} {
			for _, batch0Fails := range []bool{false, true} {
				name := fmt.Sprintf("window %d limit %d batch 0 fails %t", window, limit, batch0Fails)

				t.Run(name, func(t *testing.T) {
					key := []byte(name)
					batch0Err := errors.NewServiceError("batch 0 failed to fetch")

					synctest.Test(t, func(t *testing.T) {
						g, gCtx := errgroup.WithContext(t.Context())
						g.SetLimit(limit)

						writer := newBubbledWriter(t, gCtx, tSettings, store, key, window)
						defer writer.Abort(nil)

						done := make(chan error, 1)

						go func() {
							for i := 0; i < batchCount; i++ {
								g.Go(func() error {
									time.Sleep(time.Duration(batchCount-i) * time.Millisecond)

									if i == 0 && batch0Fails {
										return batch0Err
									}

									return writer.WriteBatch(i, [][]byte{{byte(i)}})
								})
							}

							done <- g.Wait()
						}()

						var err error

						select {
						case err = <-done:
						case <-time.After(time.Hour):
							// Abort wakes the batches, so the bubble can end once they return.
							writer.Abort(nil)
							<-done
							t.Fatal("the batches never completed")
						}

						if batch0Fails {
							require.Same(t, batch0Err, err)

							return
						}

						require.NoError(t, err)
						require.NoError(t, writer.Close(t.Context()))
					})

					if batch0Fails {
						return
					}

					data, err := store.Get(context.Background(), key, fileformat.FileTypeDat)
					require.NoError(t, err)

					want := make([]byte, batchCount)
					for i := range want {
						want[i] = byte(i)
					}

					require.Equal(t, want, data)
				})
			}
		}
	}
}
