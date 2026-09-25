package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

// Retain the SQLite authority for successful promotion; inject a single RPC
// deadline outcome to exercise retry contexts without sleeping through a timeout.
type promotionDeadlineClient struct {
	blockchain.ClientI
	contexts  []context.Context
	deadlines []time.Time
	bounded   []bool
}

func (c *promotionDeadlineClient) Run(ctx context.Context, source string) error {
	deadline, bounded := ctx.Deadline()
	c.contexts = append(c.contexts, ctx)
	c.deadlines = append(c.deadlines, deadline)
	c.bounded = append(c.bounded, bounded)
	if len(c.contexts) == 1 {
		return context.DeadlineExceeded
	}
	return c.ClientI.Run(ctx, source)
}

func TestRestoreFSMState_RenewsDeadlineForEachAttempt(t *testing.T) {
	server, authority, store, catchupCtx := newPromotionAuthority(t)
	client := &promotionDeadlineClient{ClientI: authority}
	server.blockchainClient = client
	started := time.Now()
	server.restoreFSMState(context.Background(), catchupCtx)
	requirePromotionState(t, authority, store, blockchain.FSMStateRUNNING)
	require.Len(t, client.contexts, 2)
	require.Equal(t, []bool{true, true}, client.bounded, "every RUN attempt must have an RPC deadline")
	require.WithinDuration(t, started.Add(catchupAdmissionTimeout), client.deadlines[0], time.Second)
	require.True(t, client.deadlines[1].After(client.deadlines[0]), "retry must receive a fresh timeout, not reuse one context across all attempts")
	for _, ctx := range client.contexts {
		require.ErrorIs(t, ctx.Err(), context.Canceled, "each attempt must release its timer as soon as it returns")
	}
}
