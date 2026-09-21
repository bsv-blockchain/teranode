package blockvalidation

import (
	"sync"
	"testing"
	"time"
)

// TestDrainWriteJobs_ReleasesStrandedWaiter pins the leak fix (bitcoin-sv/teranode#4692):
// tryQuickValidation spawns `go func() { wg.Wait(); close(waitDone) }()` and then abandons it when
// the shared catch-up context is cancelled. In that case the block's own queued jobs are never
// received by any worker, so wg never reaches zero and that goroutine blocks for the life of the
// process. Draining the channel once the pool has torn down counts every stranded job down, which is
// what lets it finish.
//
// The assertion is behavioural — the exact barrier tryQuickValidation builds is reconstructed here
// and observed to close — rather than a goroutine count, which cannot distinguish this goroutine
// from any other and which require.Eventually cannot measure reliably because it runs its own
// condition in a spawned goroutine.
//
// Mutation proof: deleting the job.Done.Done() call leaves waitDone open and the test fails on its
// timeout.
func TestDrainWriteJobs_ReleasesStrandedWaiter(t *testing.T) {
	wg := &sync.WaitGroup{}
	ch := make(chan *SubtreeWriteJob, 4)

	wg.Add(2)
	ch <- &SubtreeWriteJob{Done: wg}
	ch <- &SubtreeWriteJob{Done: wg}
	ch <- &SubtreeWriteJob{} // a job with no barrier must not panic
	close(ch)

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	drainWriteJobs(ch)

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait-barrier goroutine was not released by the drain")
	}
}

// TestDrainWriteJobs_NilChannel covers the quick-validation-disabled wiring, where
// fetchAndValidateBlocks never creates the channel at all.
func TestDrainWriteJobs_NilChannel(t *testing.T) {
	drainWriteJobs(nil)
}
