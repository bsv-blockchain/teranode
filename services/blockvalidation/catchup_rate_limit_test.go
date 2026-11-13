package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// rateLimitedOnce answers the first request with the asset rate limiter's 429 and every later
// one with body, counting calls.
func rateLimitedOnce(calls *atomic.Int32, body []byte) httpmock.Responder {
	return func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			resp := httpmock.NewStringResponse(http.StatusTooManyRequests, `{"message":"rate limit exceeded"}`)
			resp.Header.Set("Retry-After", "1")

			return resp, nil
		}

		return httpmock.NewBytesResponse(http.StatusOK, body), nil
	}
}

// Issue 1174: a 429 from a peer's rate limiter failed the subtree fetch outright, so catch-up
// re-issued its whole fan-out straight back into the limiter and never progressed.
func TestFetchSubtreeFromPeer_BacksOffOn429(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t)}

	subtreeHash := createTestHash("rate-limited-subtree")
	expected := []byte("subtree-content-data")

	var calls atomic.Int32
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer:8080/subtree/%s", subtreeHash), rateLimitedOnce(&calls, expected))

	data, err := server.fetchSubtreeFromPeer(context.Background(), subtreeHash, "test-peer-id", "http://test-peer:8080", false)
	require.NoError(t, err)
	require.Equal(t, expected, data)
	require.Equal(t, int32(2), calls.Load())
}

func TestFetchBlocksBatch_BacksOffOn429(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	targetHash := blocks[1].Header.Hash()

	blockBytes, err := blocks[1].Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	var calls atomic.Int32
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=1", targetHash), rateLimitedOnce(&calls, blockBytes))

	ctx, cancel := context.WithTimeout(suite.Ctx, peerBlockFetchTimeout)
	defer cancel()

	fetched, err := suite.Server.fetchBlocksBatch(ctx, targetHash, 1, "test-peer-id", "http://test-peer")
	require.NoError(t, err)
	require.Len(t, fetched, 1)
	require.Equal(t, targetHash, fetched[0].Header.Hash())
	require.Equal(t, int32(2), calls.Load())
}
