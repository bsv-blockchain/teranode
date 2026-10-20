//go:build bvgaps

package blockvalidation

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gap050MaliciousRecorder is the existing no-op P2P client plus a record of every
// RecordCatchupMalicious call (the malicious report).
type gap050MaliciousRecorder struct {
	maliciousAbortP2PClient

	mu        sync.Mutex
	malicious []string
}

func (r *gap050MaliciousRecorder) RecordCatchupMalicious(_ context.Context, peerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.malicious = append(r.malicious, peerID)

	return nil
}

func (r *gap050MaliciousRecorder) reports() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.malicious...)
}

// TestGapBV050a_FutureDatedCatchupHeaderAbortsWithoutMaliciousReport asserts BV-CATCHUP-007 /
// AC-BV-CATCHUP-007.2 (and BV-HEADER-006).
//
// GAP-BV-050(a): a header timestamped more than 7200 s ahead of the local clock depends on the
// validating node's own clock, so bitcoin-sv attaches no penalty to it. The spec requires the
// catchup to ABORT but without a malicious report. Here a peer serves a PoW-valid, well-formed
// header stamped now+3h. Fails today because catchup/header_validation.go ValidateHeaderTimestamp
// returns NewNetworkInvalidResponseError (a "malicious response" class), and
// catchup_get_block_headers.go (validateBatchHeaders branch) calls reportCatchupMalicious ->
// P2PClient.RecordCatchupMalicious for any IsMaliciousResponseError.
func TestGapBV050a_FutureDatedCatchupHeaderAbortsWithoutMaliciousReport(t *testing.T) {
	ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
	defer cancel()

	server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	mockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

	recorder := &gap050MaliciousRecorder{}
	server.p2pClient = recorder

	mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
	bestBlockHeader := mainnetHeaders[0]
	targetBlock := &model.Block{Header: mainnetHeaders[0], Height: 1001}

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Well-formed in every respect (PoW against its own bits, non-zero merkle root, linked to the
	// tip) except that its timestamp is 3 hours ahead of the local clock.
	future := &model.BlockHeader{
		Version:        4,
		HashPrevBlock:  bestBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      uint32(time.Now().Add(3 * time.Hour).Unix()), //nolint:gosec
		Bits:           *nBits,
	}
	testhelpers.MineHeader(future)

	mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).Return(false, nil)
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)
	mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder("GET", `=~^http://peer-honest-clock/headers_from_common_ancestor/.*`,
		func(*http.Request) (*http.Response, error) {
			return httpmock.NewBytesResponse(200, append(bestBlockHeader.Bytes(), future.Bytes()...)), nil
		})

	result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-honest-clock", "http://peer-honest-clock")

	require.Positive(t, httpmock.GetTotalCallCount(), "fixture precondition: the header was actually fetched")
	require.Error(t, err, "the catchup must abort on a future-dated header")
	require.NotNil(t, result)
	require.False(t, result.Success)
	require.Contains(t, err.Error(), "future", "the abort must come from the timestamp rule, not another check")

	require.Empty(t, recorder.reports(),
		"BV-CATCHUP-007: a future-dated header is clock-dependent and must NOT be reported as a malicious response")
	require.False(t, errors.IsMaliciousResponseError(err),
		"the abort error must not carry the malicious-response class, which callers turn into a ban score: %v", err)
}
