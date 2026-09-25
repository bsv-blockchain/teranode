package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// Early declared-size rejection must retain the same local-policy verdict as
// validation; wrapping it in External instead charges an honest serving peer.
// The wire-limit control prevents accidentally exempting hostile transport excess.
func TestPeerBlockFetches_PolicyDeclineDoesNotChargePeer(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		for _, policyDecline := range []bool{true, false} {
			cause := "transport limit"
			if policyDecline {
				cause = "declared size policy"
			}
			t.Run(name+"/"+cause, func(t *testing.T) {
				server := &Server{settings: test.CreateBaseTestSettings(t), logger: ulogger.TestLogger{}}
				block := testhelpers.CreateTestBlockChain(t, 2)[1]
				block.TransactionCount = 1
				block.SizeInBytes = 80 + util.VarintSize(block.TransactionCount) + uint64(block.CoinbaseTx.Size())
				payload, err := block.Bytes()
				require.NoError(t, err)
				server.settings.BlockValidation.MaxIncomingBlockBytes = 1 << 20
				server.settings.BlockValidation.MaxIncomingBlockMessageBytes = 1 << 20
				server.settings.BlockValidation.PerPeerFetchRate = 0
				server.settings.Policy.ExcessiveBlockSize = 0
				if policyDecline {
					server.settings.Policy.ExcessiveBlockSize = int(block.SizeInBytes) - 1
				} else {
					server.settings.BlockValidation.MaxIncomingBlockBytes = int64(len(payload) / 2)
				}

				httpmock.ActivateNonDefault(util.HTTPClient())
				defer httpmock.DeactivateAndReset()
				resource := fmt.Sprintf("http://peer/block/%s", block.Hash())
				if batch {
					resource = fmt.Sprintf("http://peer/blocks/%s?n=1", block.Hash())
				}
				httpmock.RegisterResponder(http.MethodGet, resource, httpmock.NewBytesResponder(http.StatusOK, payload))
				ctx := context.Background()
				if batch {
					_, err = server.fetchBlocksBatch(ctx, block.Hash(), 1, "honest-peer", "http://peer")
				} else {
					_, err = server.fetchSingleBlock(ctx, block.Hash(), "honest-peer", "http://peer")
				}
				require.Error(t, err)
				client := &failureCountingP2PClient{}
				server.p2pClient = client
				server.reportCatchupFailureForError(ctx, "honest-peer", err)
				if policyDecline {
					// External must be absent, even if PolicyDeclined is also present:
					// processCatchupChItem examines External before local policy.
					require.Zero(t, client.failures, "our block-size policy must not charge the serving peer: %v", err)
					require.ErrorIs(t, err, errors.ErrBlockPolicyDeclined)
					require.NotErrorIs(t, err, errors.ErrExternal)
				} else {
					require.Equal(t, 1, client.failures, "wire-budget exhaustion remains a fetch failure")
					require.ErrorIs(t, err, errors.ErrExternal)
					require.NotErrorIs(t, err, errors.ErrBlockPolicyDeclined)
				}
				require.NotErrorIs(t, err, errors.ErrBlockInvalid)
			})
		}
	}
}
