//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// TestGapBV001_BlockFoundWaitReturnsAfterValidation proves GAP-BV-001.
//
// Spec: BV-INGRESS-005 (AC-BV-INGRESS-005.1): BlockFound with wait_to_complete = true MUST NOT
// return until the block has been validated (committed or rejected), and MUST return the
// validation outcome.
//
// Scenario (real Init workers, real BlockFound): the peer serves the block twice. The first GET
// is the classification fetch in addBlockToPriorityQueue; the second GET is the validation-stage
// fetch made by the priority-queue worker in processBlockFound. The second GET is held open (so
// validation is demonstrably still running) and, when released, fails (HTTP 500), so the
// validation outcome is an error.
//
// Asserted: (a) while validation is in flight BlockFound has not returned; (b) once validation
// fails, BlockFound returns that error.
//
// Fails today because Server.go addBlockToPriorityQueue sends nil on errCh as soon as the block
// has been placed on the priority queue (~3048), so BlockFound returns success before the worker
// has even started validating.
func TestGapBV001_BlockFoundWaitReturnsAfterValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	require.NoError(t, s.Init(ctx))

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	var gets atomic.Int32

	validationFetchStarted := make(chan struct{})
	release := make(chan struct{})

	httpmock.RegisterResponder("GET", "http://test-peer/block/"+block.Hash().String(),
		func(req *http.Request) (*http.Response, error) {
			if gets.Add(1) == 1 {
				return httpmock.NewBytesResponse(200, blockBytes), nil // queue-stage fetch
			}

			// validation-stage fetch: hold it so validation is still running
			select {
			case <-validationFetchStarted:
			default:
				close(validationFetchStarted)
			}

			select {
			case <-release:
			case <-req.Context().Done():
			}

			return httpmock.NewStringResponse(500, "validation-stage fetch failed"), nil
		})

	type result struct{ err error }

	done := make(chan result, 1)

	go func() {
		_, callErr := s.BlockFound(ctx, &blockvalidation_api.BlockFoundRequest{
			Hash:           block.Hash()[:],
			BaseUrl:        "http://test-peer",
			PeerId:         "peerA",
			WaitToComplete: true,
		})
		done <- result{err: callErr}
	}()

	select {
	case <-validationFetchStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the validation stage never started (worker did not pick the block up)")
	}

	// Validation is now in flight and cannot finish: BlockFound must still be waiting.
	select {
	case r := <-done:
		close(release)
		t.Fatalf("BV-INGRESS-005: BlockFound(wait_to_complete=true) returned (err=%v) while validation was still running; it returned once the block was queued", r.err)
	case <-time.After(500 * time.Millisecond):
	}

	close(release)

	select {
	case r := <-done:
		require.Error(t, r.err, "BV-INGRESS-005: BlockFound must return the validation outcome (an error here), not success")
	case <-time.After(10 * time.Second):
		t.Fatal("BlockFound did not return after validation finished")
	}
}
