//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"regexp"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_Catchup_AbortsWhenOldBranchMinedSetClearFails checks that catchup aborts before
// any catchup block is fetched or validated when it cannot un-stamp the old branch.
//
// Spec: BV-CATCHUP-013 (tracked as GAP-BV-061).
//
// Scenario: Given a local tip at height 110, a stored common ancestor at height 100 (fork depth
// 10), and a peer offering a well-formed fork of 12 headers with more work than the local tip, which
// passes every header-chain check, When reading the old-branch headers fails, or clearing mined_set
// on one of them fails, Then catchup returns an error and requests no catchup block from the peer.
//
// Today: clearMinedSetOnForkedBlocks (catchup.go, step 9.6) logs the error and returns nothing, so
// the block phase starts and the first catchup block is requested from the peer.
func TestKnownDefect_Catchup_AbortsWhenOldBranchMinedSetClearFails(t *testing.T) {
	tests := []struct {
		name string
		// failStoreStep makes one old-branch store call fail.
		failStoreStep func(blockchainMock *mock.Mock)
		// clearIsAttempted is true when ClearBlockMinedSet must have been reached.
		clearIsAttempted bool
	}{
		{
			name:          "reading the old-branch headers fails",
			failStoreStep: failOldBranchHeaderRead,
		},
		{
			name:             "clearing mined_set fails",
			failStoreStep:    failOldBranchMinedSetClear,
			clearIsAttempted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				ancestorMerkleSeed = 0
				ancestorAgeSeconds = 3600
			)

			initPrometheusMetrics()

			// Given: a well-formed fork that out-works the local tip and reaches the mined_set clearing.
			ancestorHeader := newCatchupForkStoredHeader(t, ancestorMerkleSeed, ancestorAgeSeconds)
			forkHeaders := testhelpers.CreateSyntheticChainFrom(ancestorHeader, catchupForkHeaderCount)

			server, mockBlockchainClient, targetBlock, cleanup := newCatchupForkScenario(t,
				catchupForkAncestorHeight, catchupForkLocalTipHeight, ancestorHeader, forkHeaders)
			defer cleanup()

			tt.failStoreStep(&mockBlockchainClient.Mock)

			blockRequests := countCatchupBlockRequestsAfterHeaders()

			// When
			err := server.catchup(context.Background(), targetBlock, catchupForkPeerID, catchupForkPeerURL)

			// Then: sanity, the scenario reached step 5.5 and the injected call ran.
			mockBlockchainClient.AssertCalled(t, "GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything)
			if tt.clearIsAttempted {
				mockBlockchainClient.AssertCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
			}

			require.Error(t, err, "catchup must fail when the old branch cannot be un-stamped")
			require.Zero(t, blockRequests.Load(),
				"catchup went on to fetch catchup blocks after failing to clear mined_set on the old branch (err: %v)", err)
		})
	}
}

// failOldBranchHeaderRead makes reading the old-branch headers (GetBlockHeadersByHeight) fail.
func failOldBranchHeaderRead(blockchainMock *mock.Mock) {
	dropMockExpectationsFor(blockchainMock, "GetBlockHeadersByHeight")
	blockchainMock.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).
		Return(([]*model.BlockHeader)(nil), ([]*model.BlockHeaderMeta)(nil), errors.NewStorageError("simulated header read failure"))
}

// failOldBranchMinedSetClear makes clearing mined_set on an old-branch block (ClearBlockMinedSet)
// fail.
func failOldBranchMinedSetClear(blockchainMock *mock.Mock) {
	dropMockExpectationsFor(blockchainMock, "ClearBlockMinedSet")
	blockchainMock.On("ClearBlockMinedSet", mock.Anything, mock.Anything).
		Return(errors.NewStorageError("simulated mined_set clear failure"))
}

// dropMockExpectationsFor drops every registered expectation for method so the caller's
// replacement is the one testify matches (it matches the first registered expectation).
func dropMockExpectationsFor(blockchainMock *mock.Mock, method string) {
	kept := blockchainMock.ExpectedCalls[:0]
	for _, call := range blockchainMock.ExpectedCalls {
		if call.Method != method {
			kept = append(kept, call)
		}
	}

	blockchainMock.ExpectedCalls = kept
}

// countCatchupBlockRequestsAfterHeaders answers every peer request other than the header fetch with
// HTTP 500 and counts them. The header responder is registered first and httpmock tries regexp
// responders in registration order, so this one sees every other request to the peer: any request
// for a catchup block means the block phase started.
func countCatchupBlockRequestsAfterHeaders() *atomic.Int32 {
	var blockRequests atomic.Int32

	httpmock.RegisterRegexpResponder("GET", regexp.MustCompile(`^http://peer/`), func(*http.Request) (*http.Response, error) {
		blockRequests.Add(1)
		return httpmock.NewStringResponse(500, "block phase reached"), nil
	})

	return &blockRequests
}
