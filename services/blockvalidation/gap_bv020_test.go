//go:build bvgaps

package blockvalidation

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gap020BlocksRequests returns how many GET /blocks/... requests (the block-body fetch of step 10)
// the mocked peer received.
func gap020BlocksRequests() int {
	n := 0
	for k, v := range httpmock.GetCallCountInfo() {
		if strings.Contains(k, "http://peer/blocks/") {
			n += v
		}
	}
	return n
}

// gap020Run runs catchup() against a peer that offers [ancestorA, <chain built on firstParent>...].
// ancestorA is stored locally at height 100 (tip 110); otherBlockB is a different stored block.
// When firstParent is ancestorA the chain is correctly linked; when it is otherBlockB the first
// offered header's previous hash is a stored block that is NOT the common ancestor.
func gap020Run(t *testing.T, firstParentIsAncestor bool) (err error, blocksRequests int) {
	initPrometheusMetrics()

	nBits, e := model.NewNBitFromString("207fffff")
	require.NoError(t, e)

	ancestorA := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(0),
		Timestamp:      uint32(time.Now().Unix() - 3600),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(ancestorA)

	// A different locally stored block B (a sibling of A, distinct hash).
	otherBlockB := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: testhelpers.GenerateMerkleRoot(7),
		Timestamp:      uint32(time.Now().Unix() - 3500),
		Bits:           *nBits,
	}
	testhelpers.MineHeader(otherBlockB)
	require.NotEqual(t, ancestorA.Hash().String(), otherBlockB.Hash().String())

	firstParent := otherBlockB
	if firstParentIsAncestor {
		firstParent = ancestorA
	}
	forkHeaders := testhelpers.CreateSyntheticChainFrom(firstParent, 5)

	server, mockBlockchainClient, targetBlock, cleanup := gap021Scenario(t, 100, 110, ancestorA, forkHeaders)
	defer cleanup()

	// B is stored locally: prepend so it wins over the scenario's catch-all "not stored" mock.
	existsB := mockBlockchainClient.On("GetBlockExists", mock.Anything, otherBlockB.Hash()).Return(true, nil).Maybe()
	mockBlockchainClient.ExpectedCalls = append([]*mock.Call{existsB}, gap020RemoveCall(mockBlockchainClient.ExpectedCalls, existsB)...)

	httpmock.RegisterResponder("GET", `=~^http://peer/blocks/.*`, httpmock.NewBytesResponder(500, nil))

	err = server.catchup(context.Background(), targetBlock, "peer-gap020", "http://peer")

	return err, gap020BlocksRequests()
}

func gap020RemoveCall(calls []*mock.Call, c *mock.Call) []*mock.Call {
	out := make([]*mock.Call, 0, len(calls))
	for _, x := range calls {
		if x != c {
			out = append(out, x)
		}
	}
	return out
}

// TestGapBV020_FirstHeaderMustLinkToCommonAncestor asserts BV-CATCHUP-008 / AC-BV-CATCHUP-008.1:
// if the first header after the common ancestor does not link to the ancestor, the catchup MUST
// abort before any block is fetched.
//
// The peer offers [A, h1..h5] where A is the stored common ancestor but h1.HashPrevBlock is a
// DIFFERENT stored block B. Today buildHeaderCache only logs a warning, verifyChainContinuity only
// needs the parent to exist locally (B does), and the cache build checks linkage only among the new
// headers, so catchup proceeds to the block-body fetch (/blocks/...) with heights mis-assigned.
// The test fails today because catchup reaches step 10 (blocks requested) instead of aborting.
//
// A control subtest with a correctly linked chain proves the setup does reach the block fetch, so
// "zero /blocks requests" is not vacuous.
//
// Code: services/blockvalidation/catchup.go buildHeaderCache (~1132-1137 warning only),
// verifyChainContinuity (~1272-1294); catchup/header_chain_cache.go BuildFromHeaders (~115-121).
func TestGapBV020_FirstHeaderMustLinkToCommonAncestor(t *testing.T) {
	t.Run("control_linked_chain_reaches_block_fetch", func(t *testing.T) {
		_, blocks := gap020Run(t, true)
		require.Positive(t, blocks, "control: a correctly linked chain must reach the block fetch, else the test setup is vacuous")
	})

	t.Run("first_header_parent_is_other_stored_block", func(t *testing.T) {
		err, blocks := gap020Run(t, false)
		require.Error(t, err, "catchup must abort when the first header does not link to the common ancestor")
		require.Zero(t, blocks, "no block must be fetched before the linkage-to-ancestor check aborts the catchup")
	})
}
