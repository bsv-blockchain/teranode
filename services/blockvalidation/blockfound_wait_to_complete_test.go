//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_BlockFoundWithWaitToComplete_ReturnsOnlyAfterValidationWithItsOutcome checks
// that a waiting BlockFound call blocks until validation has finished and reports its result.
//
// Spec: BV-INGRESS-005 and AC-BV-INGRESS-005.1 (tracked as GAP-BV-001).
//
// Scenario: Given a server with its real Init workers and a peer that serves the announced block
// twice (the first GET is the classification fetch in addBlockToPriorityQueue, the second is the
// validation-stage fetch made by the priority-queue worker in processBlockFound), and the
// validation-stage fetch is held open so validation is demonstrably still running. When BlockFound
// is called with wait_to_complete = true. Then it has not returned while that fetch is held (500ms
// window), and once the fetch is released and fails with HTTP 500, BlockFound returns that error.
//
// Today: Server.go addBlockToPriorityQueue sends nil on errCh as soon as the block has been placed
// on the priority queue, so BlockFound returns success before the worker has even started
// validating.
func TestKnownDefect_BlockFoundWithWaitToComplete_ReturnsOnlyAfterValidationWithItsOutcome(t *testing.T) {
	const (
		peerBaseURL            = "http://test-peer"
		announcingPeerID       = "peerA"
		validationStartTimeout = 10 * time.Second
		stillWaitingWindow     = 500 * time.Millisecond
		returnAfterValidation  = 10 * time.Second
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Given: a server with real workers and a peer whose validation-stage fetch is held open.
	server, _, block := newProcessBlockFoundHarness(ctx, t)
	require.NoError(t, server.Init(ctx))

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	validationFetch := serveBlockThenHoldValidationFetch(t, peerBaseURL, block)

	// When: BlockFound is called and asked to wait for validation to complete.
	blockFoundReturned := make(chan blockFoundCallResult, 1)

	go func() {
		_, callErr := server.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:           block.Hash()[:],
			BaseUrl:        peerBaseURL,
			PeerId:         announcingPeerID,
			WaitToComplete: true,
		})
		blockFoundReturned <- blockFoundCallResult{err: callErr}
	}()

	select {
	case <-validationFetch.started:
	case <-time.After(validationStartTimeout):
		t.Fatal("setup: the validation stage never started (worker did not pick the block up)")
	}

	// Then: while validation is in flight and cannot finish, BlockFound is still waiting.
	select {
	case result := <-blockFoundReturned:
		close(validationFetch.release)
		t.Fatalf("BlockFound(wait_to_complete=true) returned (err=%v) while validation was still running; it returned once the block was queued", result.err)
	case <-time.After(stillWaitingWindow):
	}

	// And: once validation fails, BlockFound returns that failure.
	close(validationFetch.release)

	select {
	case result := <-blockFoundReturned:
		require.Error(t, result.err, "BlockFound must return the validation outcome (an error here), not success")
	case <-time.After(returnAfterValidation):
		t.Fatal("BlockFound did not return after validation finished")
	}
}

// blockFoundCallResult carries the error a BlockFound call returned out of its goroutine.
type blockFoundCallResult struct{ err error }

// heldValidationFetch lets a test see when the validation-stage block fetch has started and
// decide when it may finish.
type heldValidationFetch struct {
	started chan struct{} // closed when the validation-stage fetch arrives
	release chan struct{} // closed by the test to let the fetch finish (with HTTP 500)
}

// serveBlockThenHoldValidationFetch makes the peer at peerBaseURL serve the block on the first GET
// (the queue-stage classification fetch) and hold every later GET (the validation-stage fetch)
// until release is closed, then fail it with HTTP 500.
func serveBlockThenHoldValidationFetch(t *testing.T, peerBaseURL string, block *model.Block) heldValidationFetch {
	t.Helper()

	const (
		servedStatus      = 200
		failedFetchStatus = 500
	)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	fetch := heldValidationFetch{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	var getCount atomic.Int32

	httpmock.RegisterResponder("GET", peerBaseURL+"/block/"+block.Hash().String(),
		func(req *http.Request) (*http.Response, error) {
			if getCount.Add(1) == 1 {
				return httpmock.NewBytesResponse(servedStatus, blockBytes), nil // queue-stage fetch
			}

			// validation-stage fetch: hold it so validation is still running
			select {
			case <-fetch.started:
			default:
				close(fetch.started)
			}

			select {
			case <-fetch.release:
			case <-req.Context().Done():
			}

			return httpmock.NewStringResponse(failedFetchStatus, "validation-stage fetch failed"), nil
		})

	return fetch
}
