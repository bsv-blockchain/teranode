package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3: the cut makes a stuck store visible in every template, so the processor
// prunes the set, sends a failed or stuck subtree again from memory, and exports the age of the
// oldest unstored subtree. All of it on a tick, on the processor goroutine, between handlers.

func oldestUnstoredSeconds() float64 {
	return promtestutil.ToFloat64(prometheusSubtreeProcessorStoredPrefixOldestUnstored)
}

// newWatchedProcessor is a processor with a chain of about 5 unstored subtrees.
func newWatchedProcessor(t *testing.T, mutate ...func(*SubtreeProcessor)) (*SubtreeProcessor, *StoredSet, *storeRequests) {
	t.Helper()

	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set, mutate...)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	// the sends of the build itself are not what the test looks at
	require.Eventually(t, func() bool { return requests.count() >= len(stp.chainedSubtrees) }, 5*time.Second, time.Millisecond)

	requests.reset()

	requests.mu.Lock()
	requests.results = nil
	requests.mu.Unlock()

	return stp, set, requests
}

func (r *storeRequests) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.roots = nil // the results stay: a test settles the sends of earlier rounds
}

// settleAll settles every send so far, as failed: the store ended and the watch may send again.
func (r *storeRequests) settleAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, result := range r.results {
		if result != nil {
			select {
			case result <- StoreResult{Err: errors.NewStorageError("settled")}:
			default:
			}
		}
	}
}

func (r *storeRequests) snapshot() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.roots...)
}

func (r *storeRequests) requireSent(t *testing.T, want ...chainhash.Hash) {
	t.Helper()

	require.Eventually(t, func() bool { return r.count() >= len(want) }, 5*time.Second, time.Millisecond, "the requests were sent")
	time.Sleep(20 * time.Millisecond) // no more than asked for
	require.Equal(t, want, r.snapshot())
	r.reset()
}

func (r *storeRequests) requireNoneSent(t *testing.T) {
	t.Helper()

	time.Sleep(30 * time.Millisecond)
	require.Empty(t, r.snapshot())
}

func TestStoredPrefixWatch_PrunesTheSetToTheChain(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)

	stranger := chainhash.HashH([]byte("a subtree that left the chain"))
	set.Add(stranger)
	set.Add(chainRoot(stp, 0))
	set.Finish(chainhash.HashH([]byte("another stranger")), StoreResult{Err: errors.NewStorageError("x")}, false)

	stp.maintainStoredPrefix(time.Now())

	require.False(t, set.Has(stranger), "a root that left the chain is dropped, so the set cannot outgrow it")
	require.True(t, set.Has(chainRoot(stp, 0)))
	require.False(t, set.HasFailed(chainhash.HashH([]byte("another stranger"))))
}

func TestStoredPrefixWatch_GaugeIsTheAgeOfTheOldestUnstoredSubtree(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)
	require.Zero(t, oldestUnstoredSeconds())

	stp.maintainStoredPrefix(t0.Add(40 * time.Second))
	require.InDelta(t, 40, oldestUnstoredSeconds(), 0.5)

	set.Add(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0.Add(50 * time.Second))
	require.InDelta(t, 50, oldestUnstoredSeconds(), 0.5, "the next unstored subtree has been waiting as long")

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	stp.maintainStoredPrefix(t0.Add(60 * time.Second))
	require.Zero(t, oldestUnstoredSeconds(), "everything stored")
}

func TestStoredPrefixWatch_AStuckPrefixIsSentAgainAndTheGaugeShowsIt(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(149 * time.Second))
	requests.requireNoneSent(t)

	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
	require.InDelta(t, 151, oldestUnstoredSeconds(), 0.5, "the alert: the prefix has not advanced")

	// not again at once
	stp.maintainStoredPrefix(t0.Add(152 * time.Second))
	requests.requireNoneSent(t)

	// stored since: the next stuck one is the second
	set.Add(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0.Add(153 * time.Second))
	stp.maintainStoredPrefix(t0.Add(310 * time.Second))
	requests.requireSent(t, chainRoot(stp, 1))
}

func TestStoredPrefixWatch_AdvancingPrefixIsLeftAlone(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)

	for i := range 3 {
		set.Add(chainRoot(stp, i))
		stp.maintainStoredPrefix(t0.Add(time.Duration(100*(i+1)) * time.Second))
	}

	requests.requireNoneSent(t)
}

func TestStoredPrefixWatch_NothingIsSentWhileARetryIsInFlight(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.RetryStarted(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(400 * time.Second))
	requests.requireNoneSent(t)

	set.Finish(chainRoot(stp, 0), StoreResult{Err: errors.NewStorageError("exhausted")}, true)
	stp.maintainStoredPrefix(t0.Add(401 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0)) // failed for good, and the retry is over
}

func TestStoredPrefixWatch_ASubtreeWhoseRetriesRanOutIsSentAgainAtOnceWhileItIsChained(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.Finish(chainRoot(stp, 2), StoreResult{Err: errors.NewStorageError("exhausted")}, true)

	stp.maintainStoredPrefix(t0)
	requests.requireSent(t, chainRoot(stp, 2))

	// a deterministic failure does not spin
	set.Finish(chainRoot(stp, 2), StoreResult{Err: errors.NewStorageError("exhausted")}, false)
	stp.maintainStoredPrefix(t0.Add(time.Second))
	requests.requireNoneSent(t)

	requests.settleAll()
	stp.maintainStoredPrefix(t0.Add(31 * time.Second))
	requests.requireSent(t, chainRoot(stp, 2))

	// one that left the chain is not sent
	set.Finish(chainhash.HashH([]byte("gone")), StoreResult{Err: errors.NewStorageError("exhausted")}, false)
	stp.maintainStoredPrefix(t0.Add(40 * time.Second))
	requests.requireNoneSent(t)
}

func TestStoredPrefixWatch_ARequestIsSentWithoutWaitingForTheStore(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)
	stp.newSubtreeChan = make(chan NewSubtreeRequest) // nobody receives: the store queue is full

	set.Finish(chainRoot(stp, 0), StoreResult{Err: errors.NewStorageError("exhausted")}, false)

	done := make(chan struct{})

	go func() {
		stp.maintainStoredPrefix(time.Now())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the processor goroutine waited for a full store queue")
	}
}

func TestStoredPrefixWatch_FullyStoredChainSendsNothing(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	t0 := time.Now()
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(time.Hour))
	requests.requireNoneSent(t)
}

// The tick runs in the processor's select loop, not as a message from a worker.
func TestStoredPrefixWatch_TheProcessorRunsItOnATick(t *testing.T) {
	stp, _, requests := newWatchedProcessor(t, func(stp *SubtreeProcessor) {
		stp.prefixWatch.tickEvery = 10 * time.Millisecond
		stp.prefixWatch.stallFloor = 20 * time.Millisecond
	})

	stp.Start(context.Background())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	require.Eventually(t, func() bool { return requests.has(chainRoot(stp, 0)) }, 5*time.Second, 5*time.Millisecond,
		"a stuck prefix is sent again without anything poking the processor")
}

// A hole at the front stays a hole while later subtrees store: the stall is the first unstored
// root's, not the set's.
func TestStoredPrefixWatch_AHoleAtTheFrontIsSentAgainWhileLaterRootsStore(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)

	set.Add(chainRoot(stp, 1))
	stp.maintainStoredPrefix(t0.Add(100 * time.Second))

	set.Add(chainRoot(stp, 2))
	stp.maintainStoredPrefix(t0.Add(149 * time.Second))
	requests.requireNoneSent(t)

	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
}

func TestStoredPrefixWatch_ARetryOfAnotherRootDoesNotHoldTheHoleBack(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.RetryStarted(chainRoot(stp, 3))
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
}

// In an outage every send holds bytes in the retry channel for about 100 s; a root is not sent
// again until its last send has settled.
func TestStoredPrefixWatch_ARootIsNotSentAgainWhileItsLastSendIsOutstanding(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	failed := StoreResult{Err: errors.NewStorageError("outage")}
	set.Finish(chainRoot(stp, 2), failed, false)

	for i := range 12 { // 6 minutes of ticks, the writes blocked: nothing settles
		stp.maintainStoredPrefix(t0.Add(time.Duration(i) * 31 * time.Second))
	}

	time.Sleep(30 * time.Millisecond)

	first := requests.snapshot()
	require.Equal(t, []chainhash.Hash{chainRoot(stp, 2), chainRoot(stp, 0)}, first)

	requests.mu.Lock()
	last := requests.results[0]
	requests.mu.Unlock()
	require.NotNil(t, last, "a re-send asks for its result, to know when it settles")
	requests.reset()

	// the send settles failed: now it may be sent again
	last <- failed
	set.Finish(chainRoot(stp, 2), failed, false)
	stp.maintainStoredPrefix(t0.Add(400 * time.Second))
	requests.requireSent(t, chainRoot(stp, 2))
}
