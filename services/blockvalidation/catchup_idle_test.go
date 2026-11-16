package blockvalidation

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type idleCatchupClient struct {
	*promotionAuthorityClient
	peerFailures int
}

func (c *idleCatchupClient) ReportPeerFailure(context.Context, *chainhash.Hash, string, string, string) error {
	c.peerFailures++
	return nil
}

func TestCatchupEntry_OperatorIdleDoesNotStartValidation(t *testing.T) {
	server, client, store, catchup := newPromotionAuthority(t)
	ctx := context.Background()
	_, err := client.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	_, err = client.authority.Idle(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	// Exercise entry independently first: the old code accepts IDLE here.
	var size atomic.Int64
	err = server.setFSMCatchingBlocks(ctx, catchup, &size)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStateError))
	requirePromotionState(t, client, store, blockchain.FSMStateIDLE)

	// Both validation paths must return before starting workers or deferring RUN.
	for _, quick := range []bool{false, true} {
		catchup.useQuickValidation = quick
		err = server.fetchAndValidateBlocks(ctx, catchup)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStateError))
		require.Zero(t, client.runCalls)
		requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
	}
}

func TestProcessCatchupChItem_OperatorIdleIsNotPeerFailure(t *testing.T) {
	server, client, store, catchup := newPromotionAuthority(t)
	ctx := context.Background()
	_, err := client.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	_, err = client.authority.Idle(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	logs := &promotionLogRecorder{}
	server.logger = logs
	observed := &idleCatchupClient{promotionAuthorityClient: client}
	server.blockchainClient = observed
	server.processBlockNotify = ttlcache.New[chainhash.Hash, bool]()
	server.catchupAlternatives = ttlcache.New[chainhash.Hash, []processBlockCatchup]()
	server.blockCatchupAttempts = ttlcache.New[chainhash.Hash, int]()
	hash := *catchup.blockUpTo.Hash()
	server.processBlockNotify.Set(hash, true, ttlcache.DefaultTTL)
	server.catchupAlternatives.Set(hash, []processBlockCatchup{{block: catchup.blockUpTo}}, ttlcache.DefaultTTL)
	server.blockCatchupAttempts.Set(hash, 1, ttlcache.DefaultTTL)
	server.catchupFunc = func(ctx context.Context, _ *model.Block, _, _ string) error {
		return server.fetchAndValidateBlocks(ctx, catchup)
	}
	server.processCatchupChItem(ctx, processBlockCatchup{block: catchup.blockUpTo})

	require.Empty(t, logs.warnings, "operator IDLE is an expected refusal")
	require.Zero(t, observed.peerFailures)
	require.Contains(t, strings.Join(logs.infos, "\n"), "explicit operator resume")
	require.Equal(t, 1, server.blockCatchupAttempts.Get(hash).Value(), "refusal must not spend or reset the attempt budget")
	require.Nil(t, server.processBlockNotify.Get(hash), "later notifications must be able to retry")
	require.Nil(t, server.catchupAlternatives.Get(hash))
	require.Zero(t, client.runCalls)
	requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
}
