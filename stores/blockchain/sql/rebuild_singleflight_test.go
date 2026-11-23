package sql

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// rebuildProbe imitates rebuildOffChainSet closely enough to drive the ordering these tests
// are about: it reads the write epoch when it STARTS, can be blocked mid-read, and publishes
// through the same installOffChainSet the real rebuild uses.
//
// The concurrent tests run inside a synctest bubble so they never guess at scheduling. A
// second caller that is meant to join the in-flight rebuild must be inside rebuildGroup.Do
// and waiting on the leader before the leader is released, or it runs its own rebuild and
// the count is wrong for a reason the test is not about. synctest.Wait returns only once
// every goroutine in the bubble is durably blocked, and a joiner is durably blocked only
// once it waits on the leader's WaitGroup, so it is the exact synchronisation point a
// sleep was standing in for. The probe must be created inside the bubble for its channels
// to count.
//
// Spawned goroutines record their errors rather than calling require, because FailNow is
// only valid on the goroutine running the test.
type rebuildProbe struct {
	mu       sync.Mutex
	starts   int
	blockOn  int              // which start number blocks, 0 for none
	released chan struct{}    // closed to release the blocked start
	started  chan struct{}    // closed when the blocking start begins
	failOn   map[int]struct{} // start numbers that return an error
	s        *SQL
}

func newProbe(s *SQL, blockOn int) *rebuildProbe {
	return &rebuildProbe{
		blockOn:  blockOn,
		released: make(chan struct{}),
		started:  make(chan struct{}),
		failOn:   map[int]struct{}{},
		s:        s,
	}
}

func (p *rebuildProbe) work(context.Context) error {
	startEpoch := p.s.chainStateEpoch.Load()

	p.mu.Lock()
	p.starts++
	n := p.starts
	_, fails := p.failOn[n]
	p.mu.Unlock()

	if n == p.blockOn {
		close(p.started)
		<-p.released
	}

	if fails {
		return errors.NewStorageError("rebuild failed")
	}

	p.s.installOffChainSet(startEpoch, map[uint32]struct{}{})

	return nil
}

func (p *rebuildProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.starts
}

// TestAMutatorThatLedItsOwnRebuildDoesNotRunASecondOne pins why the epoch, not
// singleflight's shared flag, decides whether a mutator reruns. singleflight returns
// shared = c.dups > 0, which is true for the LEADER as well as soon as anyone joins it. So
// a mutator that led its own rebuild, and therefore provably read after its own write,
// still saw shared == true and paid a second full-chain recursive walk. The epoch says what
// `shared` cannot: whether the set now installed observed this write.
//
// The joiner is the whole point of the test. Without one the leader sees shared == false and
// both the old and the new code do the right thing, which is how the first version of this
// test passed against the bug it was written for.
//
// Keyed off shared, this ordering reads starts == 2: the full-chain recursive CTE run twice
// back to back under the guard. Restore the shared-flag condition in
// runRebuildObservingWrite and this test reports exactly that.
func TestAMutatorThatLedItsOwnRebuildDoesNotRunASecondOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &SQL{}
		p := newProbe(s, 1)

		// The mutator's write lands first, so its own rebuild reads after it.
		epoch := s.chainStateEpoch.Add(1)

		var (
			wg                  sync.WaitGroup
			mutatorErr, joinErr error
		)

		wg.Add(1)

		go func() {
			defer wg.Done()
			mutatorErr = s.runRebuildObservingWrite(context.Background(), epoch, p.work)
		}()

		<-p.started

		// Anyone else asking for a rebuild now joins the mutator's in-flight call, which is
		// what flips singleflight's shared flag on the LEADER.
		wg.Add(1)

		go func() {
			defer wg.Done()
			joinErr = s.runRebuild(context.Background(), p.work)
		}()

		synctest.Wait() // the joiner is now waiting on the leader, not about to run its own
		close(p.released)
		wg.Wait()

		require.NoError(t, mutatorErr)
		require.NoError(t, joinErr)
		require.Equal(t, 1, p.count(),
			"the mutator led a read that started after its own write, so nothing needed rerunning")
	})
}

// TestAJoinerWhoseSetPredatesItsWriteRebuildsAgain is the correctness half. The mutator
// joins a rebuild that began before its write committed, so the set it would otherwise
// install is missing the block it just moved.
func TestAJoinerWhoseSetPredatesItsWriteRebuildsAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &SQL{}
		p := newProbe(s, 1)

		var (
			wg                     sync.WaitGroup
			refreshErr, mutatorErr error
		)

		// A background refresh gets in first and is still reading.
		wg.Add(1)

		go func() {
			defer wg.Done()
			refreshErr = s.runRebuild(context.Background(), p.work)
		}()

		<-p.started

		// The mutator commits its on_main_chain change and asks for a rebuild.
		epoch := s.chainStateEpoch.Add(1)

		wg.Add(1)

		go func() {
			defer wg.Done()
			mutatorErr = s.runRebuildObservingWrite(context.Background(), epoch, p.work)
		}()

		synctest.Wait() // the mutator has joined the stale read rather than led its own
		close(p.released)
		wg.Wait()

		require.NoError(t, refreshErr)
		require.NoError(t, mutatorErr)
		require.Equal(t, 2, p.count(), "the mutator accepted a read that started before its own write")
		require.GreaterOrEqual(t, s.offChainSetEpoch.Load(), epoch, "the installed set still predates the mutator's write")
	})
}

// TestAStaleRebuildFinishingLastDoesNotOverwriteANewerSet covers the startup rebuild, and
// every other rebuild that does not go through the singleflight group. Ordering by
// completion is not ordering by what was read: a slow read that began before a write must
// not replace a fast one that began after it.
func TestAStaleRebuildFinishingLastDoesNotOverwriteANewerSet(t *testing.T) {
	s := &SQL{}

	stale := map[uint32]struct{}{7: {}} // what the old read saw
	fresh := map[uint32]struct{}{9: {}} // what the post-write read saw

	staleStartEpoch := s.chainStateEpoch.Load()

	writeEpoch := s.chainStateEpoch.Add(1)
	require.True(t, s.installOffChainSet(writeEpoch, fresh))

	require.False(t, s.installOffChainSet(staleStartEpoch, stale),
		"a read that began before the write overwrote a set that observed it")

	s.offChainBlockIDsMu.RLock()
	defer s.offChainBlockIDsMu.RUnlock()

	_, keptFresh := s.offChainBlockIDs[9]
	require.True(t, keptFresh, "the newer set was replaced by the older one")
}

// TestARetryThatFailsDeTrustsTheSet covers a retry that fails after a stale install. The
// leader's rebuild has already installed a stale set and already stamped
// lastSuccessfulRebuild inside rebuildOffChainSet, so a failed retry used to leave the route
// trusting a set that is missing the block the caller just moved. Nothing de-trusted it.
func TestARetryThatFailsDeTrustsTheSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &SQL{}
		p := newProbe(s, 1)
		p.failOn[2] = struct{}{} // the retry errors

		s.lastSuccessfulRebuild.Store(time.Now().Unix())

		var (
			wg                     sync.WaitGroup
			refreshErr, mutatorErr error
		)

		wg.Add(1)

		go func() {
			defer wg.Done()
			refreshErr = s.runRebuild(context.Background(), p.work)
		}()

		<-p.started

		epoch := s.chainStateEpoch.Add(1)

		wg.Add(1)

		go func() {
			defer wg.Done()
			mutatorErr = s.runRebuildObservingWrite(context.Background(), epoch, p.work)
		}()

		synctest.Wait() // the mutator has joined the stale read, so its retry is start 2
		close(p.released)
		wg.Wait()

		require.NoError(t, refreshErr)
		require.Equal(t, 2, p.count(), "precondition: the mutator joined the stale read and its retry is the one that failed")
		require.Error(t, mutatorErr, "a rebuild that never observed the caller's write must not report success")
		require.Zero(t, s.lastSuccessfulRebuild.Load(),
			"the set does not reflect the caller's write and could not be refreshed, so the route must fall back to SQL")
	})
}

// TestTheBackgroundRefreshStillDeduplicates guards the cost side. The retry must not turn
// concurrent background ticks into extra full-chain walks.
func TestTheBackgroundRefreshStillDeduplicates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &SQL{}
		p := newProbe(s, 1)

		var (
			wg                  sync.WaitGroup
			firstErr, secondErr error
		)

		wg.Add(1)

		go func() {
			defer wg.Done()
			firstErr = s.runRebuild(context.Background(), p.work)
		}()

		<-p.started

		wg.Add(1)

		go func() {
			defer wg.Done()
			secondErr = s.runRebuild(context.Background(), p.work)
		}()

		synctest.Wait() // the second refresh is waiting on the first, not about to run its own
		close(p.released)
		wg.Wait()

		require.NoError(t, firstErr)
		require.NoError(t, secondErr)
		require.Equal(t, 1, p.count(), "two background refreshes should collapse into one read")
	})
}
