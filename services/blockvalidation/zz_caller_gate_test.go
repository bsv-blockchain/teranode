package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The caller gate is the store-caller budget shared across every in-flight
// block, and it is the single largest source of blocking in the node: a block
// profile off mainnet put 350,582 of 524,066 seconds of cumulative blocking on
// it, with 182,276 of those on contention inside the primitive itself rather
// than on the budget.
//
// It changed from a weighted semaphore to a buffered channel of tokens, which is
// identical when the weight is always one and has no waiter list to walk.
// Benchmarked at 354 ns against 38.8 ns per acquire-release pair at 8
// goroutines, and 907 against 41.9 at 256. These tests pin the semantics the
// swap had to preserve, because the speed is worthless if the budget stops
// binding.

func newCallerGate(limit int) *quickWindow {
	return &quickWindow{
		logger:  ulogger.TestLogger{},
		callers: make(chan struct{}, limit),
	}
}

// TestCallerGateBindsAtItsLimit: the budget must actually bind, or a faster gate
// is just an unbounded one. A caller arriving at a spent budget waits.
func TestCallerGateBindsAtItsLimit(t *testing.T) {
	w := newCallerGate(2)
	ctx := context.Background()

	require.NoError(t, w.AcquireCaller(ctx))
	require.NoError(t, w.AcquireCaller(ctx))

	// The third caller must not get through while both slots are held.
	blocked := make(chan error, 1)

	go func() {
		waitCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		blocked <- w.AcquireCaller(waitCtx)
	}()

	select {
	case err := <-blocked:
		require.Error(t, err, "a third caller must wait at a limit of two, not proceed")
	case <-time.After(2 * time.Second):
		require.Fail(t, "the third caller neither proceeded nor timed out")
	}

	// Releasing one slot lets exactly one more caller through.
	w.ReleaseCaller()
	require.NoError(t, w.AcquireCaller(ctx))
}

// TestCallerGateRespectsACancelledContext: the call sites turn an error from
// this into a hard failure for the block, so the context arm is load-bearing.
// The semaphore returned the context's error; so must this.
func TestCallerGateRespectsACancelledContext(t *testing.T) {
	w := newCallerGate(1)

	require.NoError(t, w.AcquireCaller(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.AcquireCaller(ctx)
	require.ErrorIs(t, err, context.Canceled,
		"a cancelled caller must get the context's error, which the call site reports as a hard failure")
}

// TestCallerGateGivesNoSlotToACancelledCaller is why the context is checked
// before the select rather than only inside it.
//
// A select picks uniformly among ready cases. With a free slot and a cancelled
// context both ready, a bare select hands the cancelled caller a slot about half
// the time. It then returns nil, the caller proceeds, and the budget is briefly
// wrong for everybody else. Cheap to prevent and impossible to notice later.
func TestCallerGateGivesNoSlotToACancelledCaller(t *testing.T) {
	w := newCallerGate(4)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 200; i++ {
		require.ErrorIs(t, w.AcquireCaller(ctx), context.Canceled)
	}

	require.Empty(t, w.callers,
		"a cancelled caller must never consume a slot, however many times it asks")
}

// TestCallerGateUnderConcurrency is the property the whole change exists for,
// checked with the race detector: many goroutines acquiring and releasing must
// never exceed the budget and must never deadlock.
func TestCallerGateUnderConcurrency(t *testing.T) {
	const (
		limit    = 8
		workers  = 64
		eachDoes = 200
	)

	w := newCallerGate(limit)
	ctx := context.Background()

	var (
		mu      sync.Mutex
		inside  int
		highest int
	)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < eachDoes; j++ {
				require.NoError(t, w.AcquireCaller(ctx))

				mu.Lock()
				inside++
				if inside > highest {
					highest = inside
				}
				mu.Unlock()

				mu.Lock()
				inside--
				mu.Unlock()

				w.ReleaseCaller()
			}
		}()
	}

	wg.Wait()

	require.LessOrEqual(t, highest, limit,
		"the budget must never be exceeded, or it is not a budget")
	require.Empty(t, w.callers, "every slot taken must be given back")
}

// TestCallerGateSurvivesAnUnpairedRelease: a release without an acquire is a
// bug, and the question is which failure it causes. A bare channel receive would
// block for ever once the channel was empty, stalling a store apply. This warns
// and continues, which shows up as over-admission rather than a wedge.
func TestCallerGateSurvivesAnUnpairedRelease(t *testing.T) {
	w := newCallerGate(2)

	done := make(chan struct{})

	go func() {
		w.ReleaseCaller()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "an unpaired release blocked, which would stall a store apply for ever")
	}
}

// TestNewQuickWindowSizesTheGateFromItsLimit goes through the production
// constructor, which every other test in this file bypasses by building the
// gate directly.
//
// That was a real hole and a mutation found it: widening the constructor's
// capacity so the budget no longer binds passed every test above, because none
// of them ever called the constructor. A gate that is fast and unbounded is
// worse than the slow bounded one it replaced.
func TestNewQuickWindowSizesTheGateFromItsLimit(t *testing.T) {
	noCommit := func(context.Context, *windowEntry) error { return nil }

	for _, limit := range []int{1, 8, 64} {
		w := newQuickWindow(ulogger.TestLogger{}, 2, limit, noCommit)

		require.Equal(t, limit, cap(w.callers),
			"the gate's capacity is the configured caller budget, or the budget does not bind")

		// And it binds in practice at that capacity, not just in the type.
		ctx := context.Background()
		for i := 0; i < limit; i++ {
			require.NoError(t, w.AcquireCaller(ctx))
		}

		full, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		require.Error(t, w.AcquireCaller(full),
			"the caller after the last slot must wait")
		cancel()
	}

	// A limit below one is clamped up rather than producing a gate that admits
	// nobody, which would stall every store apply.
	w := newQuickWindow(ulogger.TestLogger{}, 2, 0, noCommit)
	require.Equal(t, 1, cap(w.callers), "a configured limit of zero must clamp to one, not to a gate nobody passes")

	require.NoError(t, w.AcquireCaller(context.Background()))
}
