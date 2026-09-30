package blockvalidation

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

type blockFetchPolicyRecorder struct {
	failureCountingP2PClient
	errorReports int
}

func (r *blockFetchPolicyRecorder) UpdateCatchupError(context.Context, string, string) error {
	r.errorReports++
	return nil
}

// Exercise every announcement fetch caller: rejecting the declaration before
// decoding must retain both policy attribution and the per-peer retry cooldown.
func TestBlockFetchPolicyDecline_AnnouncementCallers(t *testing.T) {
	for _, route := range []string{"processing", "priority queue", "busy channel"} {
		t.Run(route, func(t *testing.T) {
			server, store, _, block, _ := newHeaderAttributionServer(t) // real sqlitememory store/client
			recorder := &blockFetchPolicyRecorder{}
			server.p2pClient = recorder
			server.settings.Policy.ExcessiveBlockSize = 1
			server.settings.BlockValidation.PerPeerFetchRate = 0
			server.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 3
			server.blockPolicyDeclineAttempts = ttlcache.New[blockAttemptKey, int](
				ttlcache.WithTTL[blockAttemptKey, int](time.Minute),
				ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
			)
			server.blockPriorityQueue = NewBlockPriorityQueue(server.logger)
			server.settings.BlockValidation.UseCatchupWhenBehind = true
			server.blockFoundCh = make(chan processBlockFound, 4)
			for i := 0; i < 4; i++ {
				server.blockFoundCh <- processBlockFound{}
			}
			block.TransactionCount = 1
			block.SizeInBytes = 80 + util.VarintSize(block.TransactionCount) + uint64(block.CoinbaseTx.Size())
			payload, err := block.Bytes()
			require.NoError(t, err)
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			requests := 0
			httpmock.RegisterResponder(http.MethodGet, "http://peer/block/"+block.Hash().String(), func(*http.Request) (*http.Response, error) {
				requests++
				return httpmock.NewBytesResponse(http.StatusOK, payload), nil
			})
			ctx := context.Background()
			deliver := func(peerID string) error {
				item := processBlockFound{hash: block.Hash(), baseURL: "http://peer", peerID: peerID, errCh: make(chan error, 1)}
				switch route {
				case "processing":
					return server.processBlockFound(ctx, block.Hash(), peerID, item.baseURL)
				case "priority queue":
					server.addBlockToPriorityQueue(ctx, item)
					return <-item.errCh
				default:
					channelErr := server.processBlockFoundChannel(ctx, item)
					require.ErrorIs(t, channelErr, errors.ErrBlockPolicyDeclined)
					return <-item.errCh
				}
			}
			for i := 0; i < 5; i++ {
				err := deliver("honest-peer")
				require.ErrorIs(t, err, errors.ErrBlockPolicyDeclined)
				require.NotErrorIs(t, err, errors.ErrBlockInvalid)
			}
			require.Zero(t, recorder.failures, "our policy must not penalize the serving peer")
			require.Zero(t, recorder.errorReports, "our policy must not set a peer fetch error")
			require.Equal(t, 3, requests, "same peer/hash must stop fetching after its decline budget")
			require.ErrorIs(t, deliver("other-peer"), errors.ErrBlockPolicyDeclined)
			require.Equal(t, 4, requests, "another identity retains an independent allowance")
			require.Zero(t, recorder.failures)
			exists, err := store.GetBlockExists(ctx, block.Hash())
			require.NoError(t, err)
			require.False(t, exists, "policy decline must not store or invalidate the block")
		})
	}
}
