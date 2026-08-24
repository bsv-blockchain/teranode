package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// delCountingStore wraps a real blob.Store and counts Del calls, so a test can assert
// removeCatchupSubtreeFiles was never invoked at all (bitcoin-sv/teranode#4692's
// context-cancelled skip-cleanup path), not merely that it found nothing to delete.
type delCountingStore struct {
	blob.Store

	mu       sync.Mutex
	delCalls int
}

func (d *delCountingStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	d.mu.Lock()
	d.delCalls++
	d.mu.Unlock()

	return d.Store.Del(ctx, key, fileType, opts...)
}

func (d *delCountingStore) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.delCalls
}

// gatedWriteJobRelay sits in front of a real write-job channel and holds every job it receives
// until release is closed, so a test can prove cleanup waits for an in-flight write rather than
// racing ahead of it (bitcoin-sv/teranode#4692). Jobs are forwarded in the order received.
func gatedWriteJobRelay(in <-chan *SubtreeWriteJob, out chan<- *SubtreeWriteJob, release <-chan struct{}) {
	for job := range in {
		<-release
		out <- job
	}

	close(out)
}

// TestTryQuickValidation_CorruptPath_WaitsForDelayedWriteBeforeCleanup pins the ordering fix
// (bitcoin-sv/teranode#4692): tryQuickValidation's corrupt branch must not run
// removeCatchupSubtreeFiles until every write job this block queued has actually been written (or
// skipped) by a worker — never race ahead of an in-flight write, which would let the worker's
// later Set silently resurrect a blob cleanup just deleted (reopening the exact stale-reuse bug
// this PR exists to fix, via an ordering race instead of an over-broad delete).
//
// Mutation proof: replacing the context-aware select{} in tryQuickValidation with a bare
// removeCatchupSubtreeFiles call (no wait) would let this test observe the corrupt branch return,
// and the FileTypeSubtree blob be absent, WHILE the write is still gated — reddening the
// "must not have returned yet" assertion.
func TestTryQuickValidation_CorruptPath_WaitsForDelayedWriteBeforeCleanup(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	block := buildOneSubtreeBlock(t, suite, 100)
	// Zero the header merkle root so quickValidateBlockAsync's final merkle check fails corrupt,
	// AFTER this block's single write job has already been built and queued.
	block.Header.HashMerkleRoot = &chainhash.Hash{}
	subtreeHash := block.Subtrees[0]

	// The job this attempt queues goes into relayChan; gatedWriteJobRelay holds it there until
	// release is closed, then forwards it to workerChan where the real subtreeWriteWorker
	// actually performs the write.
	relayChan := make(chan *SubtreeWriteJob, 16)
	workerChan := make(chan *SubtreeWriteJob, 16)
	release := make(chan struct{})

	go gatedWriteJobRelay(relayChan, workerChan, release)
	go func() { _ = suite.Server.blockValidation.subtreeWriteWorker(suite.Ctx, workerChan) }()

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	type result struct {
		tryNormal bool
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		tryNormal, err := suite.Server.tryQuickValidation(suite.Ctx, block, catchupCtx, "peer-corrupt", "http://peer", relayChan, nil)
		resultCh <- result{tryNormal, err}
	}()

	// The write is still gated behind release: tryQuickValidation must not have returned, and the
	// blob must not exist yet.
	select {
	case <-resultCh:
		t.Fatal("tryQuickValidation returned before the delayed write was released")
	case <-time.After(200 * time.Millisecond):
	}

	exists, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "the write is still gated behind release")

	close(release)

	var res result
	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("tryQuickValidation did not return after the delayed write was released")
	}

	require.Error(t, res.err)
	require.True(t, errors.IsBlockCorrupt(res.err), "the corrupt verdict must propagate, got: %v", res.err)
	require.False(t, res.tryNormal)

	// Cleanup ran AFTER the write landed (the <-waitDone case, not <-ctx.Done()): the
	// freshly-written FileTypeSubtree blob must be gone, not resurrected by a race.
	exists, err = suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "cleanup must have run after the delayed write landed, deleting it")
}

// TestTryQuickValidation_CorruptPath_SiblingWorkerErrorDoesNotHang pins the second half of the
// ordering fix (bitcoin-sv/teranode#4692): tryQuickValidation shares its ctx with the write-worker
// pool's own errgroup (the same gCtx created in fetchAndValidateBlocks), so a SIBLING worker's own
// failure elsewhere in the pool cancels that context too. A bare wg.Wait() would hang forever if
// this block's own queued job is stranded (no worker left to ever receive and Done() it); the
// context-aware select must instead return promptly via <-ctx.Done(), skip
// removeCatchupSubtreeFiles entirely, and still surface the original corrupt error unchanged.
//
// No consumer is ever attached to writeJobsChan in this test, modelling every real worker having
// already exited via its own ctx.Done() branch before draining this block's job.
//
// Mutation proof: replacing the select{} with a bare wg.Wait() call would hang this test until its
// own timeout fires, reddening the "hung" failure path.
func TestTryQuickValidation_CorruptPath_SiblingWorkerErrorDoesNotHang(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	setupQuickValidateMocks(suite)

	counting := &delCountingStore{Store: suite.Server.subtreeStore}
	suite.Server.subtreeStore = counting
	suite.Server.blockValidation.subtreeStore = counting

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	block := buildOneSubtreeBlock(t, suite, 100)
	block.Header.HashMerkleRoot = &chainhash.Hash{} // corrupt: merkle mismatch

	catchupCtx := &CatchupContext{
		blockUpTo:               block,
		baseURL:                 "http://peer",
		peerID:                  "peer-corrupt",
		startTime:               time.Now(),
		useQuickValidation:      true,
		highestCheckpointHeight: 1000,
	}

	// Shared errgroup context, mirroring fetchAndValidateBlocks' real wiring: the write workers
	// and validateBlocksOnChannel/tryQuickValidation all share one gCtx from one errgroup, so any
	// sibling worker's own error cancels it for the whole pool. Delayed so THIS block's own
	// pipeline (build, queue, then fail the final merkle check) completes and returns its corrupt
	// verdict — with its job still sitting unconsumed in writeJobsChan — before the sibling fails.
	errGroup, gCtx := errgroup.WithContext(suite.Ctx)
	errGroup.Go(func() error {
		time.Sleep(200 * time.Millisecond)
		return errors.NewProcessingError("sibling worker: simulated write failure elsewhere in the pool")
	})

	// Deliberately no consumer: this block's own write job is queued but never received by any
	// worker, exactly as if every real worker had already exited via ctx.Done().
	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	type result struct {
		tryNormal bool
		err       error
	}
	resultCh := make(chan result, 1)
	go func() {
		tryNormal, err := suite.Server.tryQuickValidation(gCtx, block, catchupCtx, "peer-corrupt", "http://peer", writeJobsChan, nil)
		resultCh <- result{tryNormal, err}
	}()

	select {
	case res := <-resultCh:
		require.Error(t, res.err)
		require.True(t, errors.IsBlockCorrupt(res.err), "the original corrupt error must be returned unchanged, got: %v", res.err)
		require.False(t, res.tryNormal)
	case <-time.After(5 * time.Second):
		t.Fatal("tryQuickValidation hung waiting on a stranded write job after a sibling worker's own error cancelled the shared context")
	}

	require.Equal(t, 0, counting.count(), "removeCatchupSubtreeFiles must be skipped entirely — no Del call — when the shared context is cancelled")
	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must still be struck even when cleanup is skipped")

	_ = errGroup.Wait() // drain the sibling goroutine so it doesn't leak past the test
}
