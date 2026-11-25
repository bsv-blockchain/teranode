package blockvalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_AbortsBeforeBlockFetchWhenFirstHeaderSkipsCommonAncestor checks that when the first
// header after the common ancestor does not link to the ancestor, catchup aborts before any block
// is fetched.
//
// Spec: BV-CATCHUP-008 and AC-BV-CATCHUP-008.1 (tracked as GAP-BV-020, resolved).
//
// Scenario: Given a local tip at height 110, a stored common ancestor A at height 100 and a
// different stored block B, and a peer offering [A, h1..h12] where h1's previous hash is B rather
// than A, When catchup runs, Then it returns an error and makes zero GET /blocks/... requests.
// Before bsv-blockchain/teranode#2034 the catchup went on to the block-body fetch with heights
// mis-assigned.
//
// Controls: the same scenario with h1 linked to A does reach the block fetch, so "zero /blocks
// requests" is not vacuous.
func TestCatchup_AbortsBeforeBlockFetchWhenFirstHeaderSkipsCommonAncestor(t *testing.T) {
	t.Run("control: a correctly linked chain reaches the block fetch", func(t *testing.T) {
		// Given / When
		_, blockFetchRequests := runCatchupWithFirstHeaderParent(t, catchupForkFirstParentIsCommonAncestor)

		// Then
		require.Positive(t, blockFetchRequests, "control: a correctly linked chain must reach the block fetch, else the test setup is vacuous")
	})

	t.Run("first header whose parent is another stored block aborts before any block fetch", func(t *testing.T) {
		// Given / When
		err, blockFetchRequests := runCatchupWithFirstHeaderParent(t, catchupForkFirstParentIsOtherStoredBlock)

		// Then
		require.Error(t, err, "catchup must abort when the first header does not link to the common ancestor")
		require.Zero(t, blockFetchRequests, "no block must be fetched before the linkage-to-ancestor check aborts the catchup")
	})
}

// catchupForkFirstParent selects which stored block the first offered fork header builds on.
type catchupForkFirstParent int

const (
	catchupForkFirstParentIsCommonAncestor catchupForkFirstParent = iota
	catchupForkFirstParentIsOtherStoredBlock
)

// runCatchupWithFirstHeaderParent runs catchup() against a peer that offers [ancestor A, <12
// headers built on the chosen parent>]. A is stored locally at height 100 (tip 110); B is a
// different stored block (a sibling of A with a distinct hash). Every block-body request is
// answered with HTTP 500. It returns catchup's error and how many GET /blocks/... requests the
// peer received.
func runCatchupWithFirstHeaderParent(t *testing.T, parent catchupForkFirstParent) (err error, blockFetchRequests int) {
	const (
		ancestorMerkleSeed    = 0
		ancestorAgeSeconds    = 3600
		otherBlockMerkleSeed  = 7
		otherBlockAgeSeconds  = 3500
		blockFetchErrorStatus = 500
	)

	initPrometheusMetrics()

	ancestorA := newCatchupForkStoredHeader(t, ancestorMerkleSeed, ancestorAgeSeconds)
	otherBlockB := newCatchupForkStoredHeader(t, otherBlockMerkleSeed, otherBlockAgeSeconds)
	require.NotEqual(t, ancestorA.Hash().String(), otherBlockB.Hash().String())

	firstParent := otherBlockB
	if parent == catchupForkFirstParentIsCommonAncestor {
		firstParent = ancestorA
	}

	forkHeaders := testhelpers.CreateSyntheticChainFrom(firstParent, catchupForkHeaderCount)

	server, mockBlockchainClient, targetBlock, cleanup := newCatchupForkScenario(t,
		catchupForkAncestorHeight, catchupForkLocalTipHeight, ancestorA, forkHeaders)
	defer cleanup()

	// B is stored locally: prepend so it wins over the scenario's catch-all "not stored" mock.
	existsB := mockBlockchainClient.On("GetBlockExists", mock.Anything, otherBlockB.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.ExpectedCalls = append([]*mock.Call{existsB}, withoutMockCall(mockBlockchainClient.ExpectedCalls, existsB)...)

	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(blockFetchErrorStatus, nil))

	err = server.catchup(context.Background(), targetBlock, catchupForkPeerID, catchupForkPeerURL)

	return err, countPeerBlockFetchRequests()
}

// countPeerBlockFetchRequests returns how many GET /blocks/... requests (the block-body fetch of
// step 10) the mocked peer received.
func countPeerBlockFetchRequests() int {
	count := 0
	for responderKey, calls := range httpmock.GetCallCountInfo() {
		if strings.Contains(responderKey, "http://peer/blocks/") {
			count += calls
		}
	}

	return count
}

// withoutMockCall returns calls with the given expectation removed.
func withoutMockCall(calls []*mock.Call, removed *mock.Call) []*mock.Call {
	out := make([]*mock.Call, 0, len(calls))
	for _, call := range calls {
		if call != removed {
			out = append(out, call)
		}
	}

	return out
}
