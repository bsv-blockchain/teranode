package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCommitBlock_ShutdownWaitsForInFlightUnlock covers the shutdown half of
// bitcoin-sv/teranode#4987. Server.Stop waits on BlockValidation.Wait, and that waits only for work
// registered with backgroundTasks. commitBlock's post-commit unlock runs on a context a shutdown
// does not cancel, so unless commitBlock is registered, Wait returns while the unlock is still in
// flight and the process can exit with a committed block's outputs locked.
//
// Here the unlock is held in flight while Wait runs. Wait must not return until the unlock has
// finished, and the block's outputs must end unlocked.
//
// Mutation proof: removing the trackBackgroundTask registration at the top of commitBlock makes
// Wait return at once, with the unlock still held, and the first assertion fails.
func TestCommitBlock_ShutdownWaitsForInFlightUnlock(t *testing.T) {
	var (
		once          sync.Once
		returnedEarly atomic.Bool
		lockedAtWait  atomic.Bool
		lockedOnHold  atomic.Bool
		lockedReadErr atomic.Value
		waitStarted   = make(chan struct{})
		waitReturned  = make(chan struct{})
		childHashRef  atomic.Pointer[chainhash.Hash]
	)

	h, _, _, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		beforeUnlock: func(bv *BlockValidation) {
			once.Do(func() {
				go func() {
					waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					close(waitStarted)
					bv.Wait(waitCtx)

					// Read the lock state at the moment Wait returns: that is the guarantee a
					// shutdown relies on.
					if child := childHashRef.Load(); child != nil {
						created, err := bv.utxoStore.Get(context.Background(), child, fields.Locked)
						if err != nil {
							lockedReadErr.Store(err)
						} else {
							lockedAtWait.Store(created.Locked)
						}
					}

					close(waitReturned)
				}()

				<-waitStarted

				// Precondition: the outputs are locked while the unlock is held, so the
				// unlocked state read after Wait is something the commit tail produced.
				if created, err := bv.utxoStore.Get(context.Background(), childHashRef.Load(), fields.Locked); err == nil {
					lockedOnHold.Store(created.Locked)
				}

				// Hold the unlock in flight. Wait has no reason to return while it is held.
				select {
				case <-waitReturned:
					returnedEarly.Store(true)
				case <-time.After(300 * time.Millisecond):
				}
			})
		},
		beforeRun: func(child *chainhash.Hash) { childHashRef.Store(child) },
	})

	require.True(t, lockedOnHold.Load(), "precondition: the outputs must be locked while the unlock is held")
	require.False(t, returnedEarly.Load(), "Wait returned while the post-commit unlock was still in flight")

	select {
	case <-waitReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after the commit finished")
	}

	require.Nil(t, lockedReadErr.Load())
	require.False(t, lockedAtWait.Load(), "the committed block's outputs must already be unlocked when Wait returns")

	require.NoError(t, quickErr)
	require.False(t, tryNormal)

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked)
}

// TestCommitBlock_ShutdownWaitIsBoundedByStopDeadline covers the other side: a commit tail that
// hangs must not hold shutdown past the deadline Server.Stop passes to Wait
// (service_manager_stopTimeout). Wait is called from inside the held unlock with a 100ms deadline.
// It must return on that deadline, not when the unlock ends, and must actually have waited.
//
// Without the registration Wait returns at once, so the lower bound fails.
func TestCommitBlock_ShutdownWaitIsBoundedByStopDeadline(t *testing.T) {
	const deadline = 100 * time.Millisecond

	var (
		once    sync.Once
		elapsed time.Duration
	)

	h, _, _, childHash, _, tryNormal, quickErr := gap056QuickValidateWith(t, gap056Config{
		beforeUnlock: func(bv *BlockValidation) {
			once.Do(func() {
				waitCtx, cancel := context.WithTimeout(context.Background(), deadline)
				defer cancel()

				// The commit cannot finish while this unlock is held, so only the deadline can
				// end Wait here.
				start := time.Now()
				bv.Wait(waitCtx)
				elapsed = time.Since(start)
			})
		},
	})

	require.GreaterOrEqual(t, elapsed, deadline, "Wait returned before its deadline, so it was not waiting for the commit")
	require.Less(t, elapsed, 10*time.Second, "Wait must return on its deadline while the commit is held")

	// The held unlock then completes, so the block still ends committed and unlocked.
	require.NoError(t, quickErr)
	require.False(t, tryNormal)

	created, err := h.utxoStore.Get(h.ctx, childHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked)
}

// TestTrackBackgroundTask_RefusesOnceWaitHasStarted pins the guard that keeps a commit from adding
// to backgroundTasks from zero while Wait is running, which sync.WaitGroup forbids.
func TestTrackBackgroundTask_RefusesOnceWaitHasStarted(t *testing.T) {
	bv := &BlockValidation{logger: ulogger.NewErrorTestLogger(t)}

	done, ok := bv.trackBackgroundTask()
	require.True(t, ok, "registration must be open before Wait")
	done()

	bv.Wait(context.Background())

	done, ok = bv.trackBackgroundTask()
	require.False(t, ok, "registration must be closed once Wait has started")
	require.Nil(t, done)
}
