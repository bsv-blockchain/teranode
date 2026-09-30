package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

func TestCatchupSubtreeHashes_DeadlineAttribution(t *testing.T) {
	for _, stage := range []string{"response headers", "body stream"} {
		for _, owner := range []string{"caller", "download"} {
			t.Run(stage+"/"+owner, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					store := memory.New()
					defer func() { require.NoError(t, store.Close(context.Background())) }()
					server := &Server{
						logger:           ulogger.TestLogger{},
						settings:         test.CreateBaseTestSettings(t),
						subtreeStore:     store,
						activeCatchupCtx: &CatchupContext{},
					}
					parentTimeout, downloadTimeout := time.Second, time.Minute
					if owner == "download" {
						parentTimeout, downloadTimeout = downloadTimeout, parentTimeout
					}
					server.settings.BlockValidation.SubtreeFetchTimeout = downloadTimeout
					subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
					require.NoError(t, err)
					require.NoError(t, subtree.AddCoinbaseNode())
					hash := subtree.RootHash()
					block := testhelpers.CreateTestBlockChain(t, 1)[0]
					block.Subtrees = append(block.Subtrees[:0], hash)

					httpmock.ActivateNonDefault(util.HTTPClient())
					defer httpmock.DeactivateAndReset()
					var body *stallingBlockResponseBody
					httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/subtree/%s", hash), func(req *http.Request) (*http.Response, error) {
						if stage == "response headers" {
							<-req.Context().Done()
							return nil, req.Context().Err()
						}
						body = &stallingBlockResponseBody{ctx: req.Context(), started: make(chan struct{})}
						return blockHTTPResponse(body), nil
					})

					// RevalidateBlock forwards its RPC context into this entry point.
					// Its caller's deadline must not charge a peer through a concurrently
					// active catchup's failure map before the download allowance expires.
					parent, cancel := context.WithTimeout(context.Background(), parentTimeout)
					defer cancel()
					_, _, err = server.fetchSubtreeDataForBlock(parent, block, "peer", "http://peer")
					require.Error(t, err)
					if stage == "body stream" {
						require.NotNil(t, body)
						require.True(t, body.closed.Load())
					}
					if owner == "caller" {
						require.ErrorIs(t, parent.Err(), context.DeadlineExceeded)
						require.Empty(t, server.activeCatchupCtx.failedPeers, "caller timeout must not charge an innocent peer")
						require.True(t, errors.IsLocalError(err), "caller timeout must remain local: %v", err)
						require.False(t, errors.Is(err, errors.ErrNetworkTimeout))
					} else {
						require.NoError(t, parent.Err())
						require.Contains(t, server.activeCatchupCtx.failedPeers, "peer", "the peer exhausted its own response allowance")
						require.False(t, errors.IsLocalError(err))
						require.ErrorIs(t, err, errors.ErrNetworkTimeout)
					}
				})
			})
		}
	}
}
