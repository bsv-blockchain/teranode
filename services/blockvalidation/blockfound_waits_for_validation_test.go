package blockvalidation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// These tests drive BlockFound with WaitToComplete through the real pipeline: a blockFoundCh
// worker, the priority queue and a block-processing worker, against the sqlitememory UTXO store
// of newProcessBlockFoundHarness. The caller must be answered with the block's final outcome:
// nil only once the block is stored (BV-INGRESS-005), and an error when it was handed to catchup
// instead (BV-INGRESS-017).

const waitTestPeer = "http://test-peer"

// startBlockFoundPipeline runs one blockFoundCh worker and the block-processing workers until ctx
// ends.
func startBlockFoundPipeline(ctx context.Context, s *Server) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case bf := <-s.blockFoundCh:
				s.processBlockFoundChItem(ctx, 0, bf)
			}
		}
	}()

	s.startBlockProcessingSystem(ctx)
}

// serveBlock answers the peer's /block/<hash> fetches with block. Fetch number holdFetch (counted
// from 1) waits until release is closed; 0 holds none. It returns the fetch counter.
func serveBlock(t *testing.T, block *model.Block, holdFetch int64, release <-chan struct{}) *atomic.Int64 {
	t.Helper()

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	var fetches atomic.Int64

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	httpmock.RegisterResponder(http.MethodGet, waitTestPeer+"/block/"+block.Hash().String(),
		func(*http.Request) (*http.Response, error) {
			if fetches.Add(1) == holdFetch {
				<-release
			}

			return httpmock.NewBytesResponse(http.StatusOK, blockBytes), nil
		})

	return &fetches
}

func blockFoundAndWait(ctx context.Context, s *Server, block *model.Block) <-chan error {
	done := make(chan error, 1)

	go func() {
		_, err := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:           block.Hash().CloneBytes(),
			BaseUrl:        waitTestPeer,
			WaitToComplete: true,
		})
		if err != nil {
			// BlockFound wraps its error for gRPC; unwrap it so the tests can check the code.
			err = errors.UnwrapGRPC(err)
		}
		done <- err
	}()

	return done
}

// TestBlockFoundWait_ReturnsOnlyOnceTheBlockIsStored holds the worker's fetch of the block, the
// first step of validation, after the block has been queued. Before the fix, the caller was
// answered nil as soon as the block was queued, while the worker had not yet fetched it.
func TestBlockFoundWait_ReturnsOnlyOnceTheBlockIsStored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	// Fetch 1 classifies the block before it is queued; fetch 2 is the worker's validation fetch.
	release := make(chan struct{})
	fetches := serveBlock(t, block, 2, release)

	startBlockFoundPipeline(ctx, s)

	done := blockFoundAndWait(ctx, s, block)

	require.Eventually(t, func() bool { return fetches.Load() == 2 }, 10*time.Second, 10*time.Millisecond,
		"setup: the worker never started validating the queued block")

	select {
	case err := <-done:
		t.Fatalf("BlockFound returned before the block was validated (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return after the block was validated")
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "a nil answer must mean the block is stored")
}

// TestBlockFoundWait_ParentMissingReturnsServiceError: a block whose parent is not stored goes to
// catchup, not validation. The caller must get a retry-later ERR_SERVICE_ERROR, not nil.
func TestBlockFoundWait_ParentMissingReturnsServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	serveBlock(t, block, 0, nil)
	startBlockFoundPipeline(ctx, s)

	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceError), "want ERR_SERVICE_ERROR, got %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return for a block handed to catchup")
	}

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}

// TestBlockFoundWait_ValidationFailureReachesCaller: the worker's verdict must reach the caller.
// The served block's header timestamp is before its parent's, which fails the median-time-past
// rule, so validation rejects it.
func TestBlockFoundWait_ValidationFailureReachesCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	parent := store.Blocks[*block.Header.HashPrevBlock]
	header := *block.Header
	header.Timestamp = parent.Header.Timestamp - 1
	header.Nonce = 0

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		require.Less(t, header.Nonce, uint32(10_000_000), "could not grind a nonce meeting the easy target")
		header.Nonce++
	}

	bad, err := model.NewBlock(&header, block.CoinbaseTx, block.Subtrees, block.TransactionCount, block.SizeInBytes, block.Height, 0)
	require.NoError(t, err)

	serveBlock(t, bad, 0, nil)
	startBlockFoundPipeline(ctx, s)

	select {
	case err := <-blockFoundAndWait(ctx, s, bad):
		require.Error(t, err, "a block that fails validation must not be answered nil")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "want the validation verdict, got %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("BlockFound did not return for a block that failed validation")
	}
}

// TestBlockFoundWait_CallerContextEndsTheWait: a caller that gives up gets its context error, and
// a late answer finds room in the reply, so the worker that sends it never blocks.
func TestBlockFoundWait_CallerContextEndsTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	callerCtx, callerCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer callerCancel()

	// No worker reads blockFoundCh, so nothing answers.
	select {
	case err := <-blockFoundAndWait(callerCtx, s, block):
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrContextCanceled), "want a context error, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("BlockFound ignored its caller's context")
	}

	bf := <-s.blockFoundCh
	require.NotNil(t, bf.reply)
	bf.reply.answer(nil) // must not block though the caller has gone
}

// TestBlockFoundWait_FetchFailureAnswersAtOnceAndRequeues: the worker's fetch fails with HTTP 500,
// a failure that is not a verdict on the block, so the block goes back on the queue for a retry.
// The caller must get that failure at once, not after the retries, because it can call BlockFound
// again itself (see answerQueuedBlock). The block must still be requeued, for the node's own sake.
func TestBlockFoundWait_FetchFailureAnswersAtOnceAndRequeues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)

	// Fetch 1 classifies the block before it is queued and succeeds; every later fetch fails.
	var fetches atomic.Int64

	httpmock.RegisterResponder(http.MethodGet, waitTestPeer+"/block/"+block.Hash().String(),
		func(*http.Request) (*http.Response, error) {
			if fetches.Add(1) == 1 {
				return httpmock.NewBytesResponse(http.StatusOK, blockBytes), nil
			}

			return httpmock.NewStringResponse(http.StatusInternalServerError, "peer failed"), nil
		})

	startBlockFoundPipeline(ctx, s)

	// Well under blockFetchRetryDelay, so an answer after the first retry would miss it.
	select {
	case err := <-blockFoundAndWait(ctx, s, block):
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a failed fetch is not a verdict on the block, got %v", err)
	case <-time.After(blockFetchRetryDelay / 2):
		t.Fatal("BlockFound waited for the retries instead of answering the first failure")
	}

	require.Eventually(t, func() bool { return s.blockPriorityQueue.Contains(*block.Hash()) }, 5*time.Second, 10*time.Millisecond,
		"the block must still be requeued for a retry after the caller was answered")

	exists, err := store.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists)
}
