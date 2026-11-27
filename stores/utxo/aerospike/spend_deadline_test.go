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
// keeps that promise when it sends: these tests hold the rules of splitSpendsByDeadline.
func TestSplitSpendsByDeadline(t *testing.T) {
	now := time.Now().Truncate(time.Second).Add(100 * time.Millisecond)
	group := completion.NewGroup(8)

	item := func(deadline time.Time) *batchSpend {
		return &batchSpend{spend: &utxo.Spend{TxID: &chainhash.Hash{}}, group: group, deadline: deadline}
	}

	unbounded := item(time.Time{})
	expired := item(now.Add(-time.Millisecond))
	atDeadline := item(now)
	early := item(now.Add(5 * time.Second))
	sameSecond := item(now.Add(5*time.Second + 300*time.Millisecond))
	late := item(now.Add(time.Minute))

	parts := splitSpendsByDeadline([]*batchSpend{late, expired, unbounded, nil, atDeadline, sameSecond, early}, now)

	require.Equal(t, [][]*batchSpend{{unbounded}, {sameSecond, early}, {late}}, parts,
		"the items with no deadline go apart, and the others in groups of one second, the earliest first: no item is limited by a deadline more than a second shorter than its own")

	for name, it := range map[string]*batchSpend{"expired": expired, "at its deadline": atDeadline} {
		require.True(t, it.completed.Load(), "%s: an item whose deadline has passed is completed and not sent", name)
		require.ErrorIs(t, it.spend.Err, errors.ErrServiceUnavailable, name)
	}

	for name, it := range map[string]*batchSpend{"unbounded": unbounded, "early": early, "same second": sameSecond, "late": late} {
		require.False(t, it.completed.Load(), "%s: an item that is sent is completed by its write", name)
	}

	require.Empty(t, splitSpendsByDeadline([]*batchSpend{item(now.Add(-time.Hour))}, now))
}

// Only a spend that a lease bounds carries its deadline. The context of any other caller has a
// deadline that says when it stops waiting, and the store must not drop or reorder its spend for it.
func TestSpendDeadlineIsKeptOnlyForALeaseBoundContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.False(t, utxo.IsLeaseBound(ctx))
	require.True(t, utxo.IsLeaseBound(utxo.WithLeaseBound(ctx)))

	deadline, ok := utxo.WithLeaseBound(ctx).Deadline()
	require.True(t, ok)
	require.False(t, deadline.IsZero(), "marking a context does not change its deadline")
}

func TestFinalizeGroups(t *testing.T) {
	base := time.Now().Truncate(time.Second).Add(10 * time.Second)

	withDeadline := func(d time.Time) *batchFinalize {
		ctx, cancel := context.WithDeadline(context.Background(), d)
		t.Cleanup(cancel)

		return &batchFinalize{ctx: ctx}
	}

	a := withDeadline(base.Add(100 * time.Millisecond))
	b := withDeadline(base.Add(900 * time.Millisecond))
	c := withDeadline(base.Add(time.Minute))
	d := &batchFinalize{ctx: context.Background()}

	require.Equal(t, [][]int{{0, 1}, {2}, {3}}, finalizeGroups([]*batchFinalize{a, b, c, d}),
		"finalizes whose deadlines fall in the same second are sent together, the others apart, and one with no deadline apart from all")
	require.Empty(t, finalizeGroups(nil))
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
