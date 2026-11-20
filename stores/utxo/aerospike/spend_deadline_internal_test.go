package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// A create-first spender tells every other actor, with its spend lease, that it spends no later
// than the deadline of its context. Its batch is queued on the context of the store, so the store
// keeps that promise when it sends: these tests hold the two rules of splitSpendsByDeadline.
func TestSplitSpendsByDeadline(t *testing.T) {
	now := time.Now()
	group := completion.NewGroup(4)

	item := func(deadline time.Time) *batchSpend {
		return &batchSpend{spend: &utxo.Spend{TxID: &chainhash.Hash{}}, group: group, deadline: deadline}
	}

	unbounded := item(time.Time{})
	expired := item(now.Add(-time.Millisecond))
	atDeadline := item(now)
	early := item(now.Add(time.Second))
	late := item(now.Add(time.Minute))

	parts := splitSpendsByDeadline([]*batchSpend{late, expired, unbounded, nil, atDeadline, early}, now)

	require.Equal(t, [][]*batchSpend{{unbounded}, {late, early}}, parts,
		"items with a deadline are sent apart from the others, so the others keep the timeouts of the policy")

	for name, it := range map[string]*batchSpend{"expired": expired, "at its deadline": atDeadline} {
		require.True(t, it.completed.Load(), "%s: an item whose deadline has passed is completed and not sent", name)
		require.ErrorIs(t, it.spend.Err, errors.ErrServiceUnavailable, name)
	}

	for name, it := range map[string]*batchSpend{"unbounded": unbounded, "early": early, "late": late} {
		require.False(t, it.completed.Load(), "%s: an item that is sent is completed by its write", name)
	}

	require.Empty(t, splitSpendsByDeadline([]*batchSpend{item(now.Add(-time.Hour))}, now))
}

func TestSpendWriteContext(t *testing.T) {
	now := time.Now()

	early := &batchSpend{deadline: now.Add(time.Second)}
	late := &batchSpend{deadline: now.Add(time.Minute)}

	ctx, cancel := spendWriteContext(context.Background(), []*batchSpend{late, {}, nil, early})
	defer cancel()

	deadline, bounded := ctx.Deadline()
	require.True(t, bounded)
	require.Equal(t, early.deadline, deadline, "the write of a batch ends at the earliest deadline of its items")

	ctx, cancel = spendWriteContext(context.Background(), []*batchSpend{{}, nil})
	defer cancel()

	_, bounded = ctx.Deadline()
	require.False(t, bounded, "a batch with no deadline keeps the context it had")
}
