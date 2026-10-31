//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gap061ReplaceExpectation drops every registered expectation for method so the caller's
// replacement is the one testify matches (it matches the first registered expectation).
func gap061ReplaceExpectation(m *mock.Mock, method string) {
	kept := m.ExpectedCalls[:0]
	for _, c := range m.ExpectedCalls {
		if c.Method != method {
			kept = append(kept, c)
		}
	}

	m.ExpectedCalls = kept
}

// TestGapBV061_CatchupAbortsWhenOldBranchMinedSetClearFails covers GAP-BV-061 (BV-CATCHUP-013).
//
// Spec-correct behaviour: when fork depth > 0 and reading the old-branch headers, or clearing
// mined_set on any of them, fails, the catchup MUST abort before any catchup block is fetched or
// validated. Here the peer offers a well-formed fork of depth 10 with more work than the local
// tip, which passes every header-chain check, and the local store fails the step. Expected to fail
// today: clearMinedSetOnForkedBlocks (catchup.go, step 9.6) logs the error and returns nothing, so
// the block phase starts and the first catchup block is requested from the peer.
func TestGapBV061_CatchupAbortsWhenOldBranchMinedSetClearFails(t *testing.T) {
	tests := []struct {
		name   string
		inject func(m *mock.Mock)
	}{
		{
			name: "reading the old-branch headers fails",
			inject: func(m *mock.Mock) {
				gap061ReplaceExpectation(m, "GetBlockHeadersByHeight")
				m.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).
					Return(([]*model.BlockHeader)(nil), ([]*model.BlockHeaderMeta)(nil), errors.NewStorageError("gap061: simulated header read failure"))
			},
		},
		{
			name: "clearing mined_set fails",
			inject: func(m *mock.Mock) {
				gap061ReplaceExpectation(m, "ClearBlockMinedSet")
				m.On("ClearBlockMinedSet", mock.Anything, mock.Anything).
					Return(errors.NewStorageError("gap061: simulated mined_set clear failure"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initPrometheusMetrics()

			ancestorHeader := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: testhelpers.GenerateMerkleRoot(0),
				Timestamp:      uint32(time.Now().Unix() - 3600),
			}
			nBits, err := model.NewNBitFromString("207fffff")
			require.NoError(t, err)
			ancestorHeader.Bits = *nBits
			testhelpers.MineHeader(ancestorHeader)

			// 12 headers on an ancestor at 100 out-work the local tip at 110, so the offered chain
			// passes the less-work refusal (catchup step 5.1) and reaches the mined_set clearing.
			forkHeaders := testhelpers.CreateSyntheticChainFrom(ancestorHeader, 12)

			server, mockBlockchainClient, targetBlock, cleanup := gap021Scenario(t, 100, 110, ancestorHeader, forkHeaders)
			defer cleanup()

			tt.inject(&mockBlockchainClient.Mock)

			// The header responder is registered first and httpmock tries regexp responders in
			// registration order, so this one sees every other request to the peer: any request
			// for a catchup block means the block phase started.
			var blockRequests atomic.Int32
			httpmock.RegisterRegexpResponder("GET", regexp.MustCompile(`^http://peer/`), func(*http.Request) (*http.Response, error) {
				blockRequests.Add(1)
				return httpmock.NewStringResponse(500, "gap061: block phase reached"), nil
			})

			err = server.catchup(context.Background(), targetBlock, "peer-gap061", "http://peer")

			// Sanity: the scenario reached step 5.5 and the injected call ran.
			mockBlockchainClient.AssertCalled(t, "GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything)
			if tt.name == "clearing mined_set fails" {
				mockBlockchainClient.AssertCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
			}

			require.Error(t, err, "catchup must fail when the old branch cannot be un-stamped")
			require.Zero(t, blockRequests.Load(),
				"GAP-BV-061: catchup went on to fetch catchup blocks after failing to clear mined_set on the old branch (err: %v)", err)
		})
	}
}
