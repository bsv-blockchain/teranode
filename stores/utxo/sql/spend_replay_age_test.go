package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// forEachSpendPath runs body on each spend implementation (SQLite per-row,
// Postgres per-row, Postgres bulk) at the default retention of 288, with the
// store's tip at 1000 and a fresh pruner service.
func forEachSpendPath(t *testing.T, body func(t *testing.T, ctx context.Context, store *Store)) {
	t.Helper()

	for _, backend := range heightBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.build(t)

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)

			store.settings.GlobalBlockHeightRetention = 288
			require.NoError(t, store.SetBlockHeight(1000))

			body(t, ctx, store)
		})
	}
}

func spenderRecordExists(t *testing.T, ctx context.Context, store *Store, tx interface{ TxIDChainHash() *chainhash.Hash }) bool {
	t.Helper()

	var exists bool

	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", tx.TxIDChainHash()[:]).Scan(&exists))

	return exists
}

// A transaction the pruner removed is resubmitted when its parent outputs were
// spent long ago. The output says "spent by C at 1200" and C has no record, so
// the replay is refused, and what the output records is left alone.
func TestSpendRefusesAReplayOfAPrunedTransaction(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p, q, child := twoParentPrunedChild(t, ctx, store)
		require.False(t, spenderRecordExists(t, ctx, store, child), "fixture: the pruner removed the child")

		recorded := spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes()
		require.Equal(t, recorded, outputSpendingData(t, ctx, store, p, 0), "fixture: P:0 records the child")

		spends, err := store.Spend(ctx, child, 2001)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
		require.NotErrorIs(t, err, errors.ErrSpent, "a pruned replay is not a double spend")

		for _, spend := range spends {
			require.ErrorIs(t, spend.Err, errors.ErrUtxoSpendingTxPruned, "both inputs are replays")
		}

		require.Equal(t, recorded, outputSpendingData(t, ctx, store, p, 0), "P:0 stays spent by the child")
		require.Equal(t, spendpkg.NewSpendingData(child.TxIDChainHash(), 1).Bytes(), outputSpendingData(t, ctx, store, q, 0), "Q:0 stays spent by the child")
		require.False(t, spenderRecordExists(t, ctx, store, child))
	})
}

// A transaction D takes P:1 at height 2001 and the node stops before D's record
// exists. D comes back two blocks later: the recorded spend is young, so the
// retry is the ordinary idempotent match and succeeds without writing again.
func TestSpendAcceptsAnHonestRetryTwoBlocksLater(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := newTwoOutputParent(t, 21)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		d := newSpenderOf(t, p, 21, 1)

		_, err = store.Spend(ctx, d, 2001)
		require.NoError(t, err)

		_, err = store.Spend(ctx, d, 2003)
		require.NoError(t, err, "a spend recorded two blocks ago is not history, though D has no record")
		require.Equal(t, int64(2001), recordedSpendHeight(t, ctx, store, p, 1).Int64, "the retry writes nothing")
	})
}

// A transaction that waited unmined for more than 144 blocks is resubmitted. Its
// record exists, so it gets no replay error: the spender check keeps today's
// answer for it.
func TestSpendOfAnOldTransactionThatIsStillStoredIsNotAReplay(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := newTwoOutputParent(t, 22)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		c := newSpenderOf(t, p, 22, 0)
		_, _, err = store.SpendAndCreate(ctx, c, 1000)
		require.NoError(t, err)

		_, err = store.Spend(ctx, c, 1400)
		require.NoError(t, err, "C is stored, so a spend of it 400 blocks later is no replay")
		require.Equal(t, int64(1000), recordedSpendHeight(t, ctx, store, p, 0).Int64)
	})
}

// A block that applies a transaction already in the store, at the height it
// was first applied at, is a young match.
func TestSpendAtTheRecordedHeightIsNotAReplay(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := newTwoOutputParent(t, 23)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		d := newSpenderOf(t, p, 23, 0)
		_, err = store.Spend(ctx, d, 1200)
		require.NoError(t, err)

		_, err = store.Spend(ctx, d, 1200)
		require.NoError(t, err)

		// Below the recorded height, as a re-applied older block does.
		_, err = store.Spend(ctx, d, 1100)
		require.NoError(t, err)
	})
}

// A replay whose second input is free is refused on the first, and the fresh
// spend it made of the second is reversed, while the historical match on the
// first is not.
func TestFailedReplayReversesItsFreshSpendAndKeepsTheHistoricalMatch(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p, q, child := twoParentPrunedChild(t, ctx, store)

		qHash, err := util.UTXOHashFromOutput(q.TxIDChainHash(), q.Outputs[0], 0)
		require.NoError(t, err)
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
			TxID: q.TxIDChainHash(), Vout: 0, UTXOHash: qHash,
			SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 1),
		}}))
		require.Nil(t, outputSpendingData(t, ctx, store, q, 0), "fixture: Q:0 is free, so the replay spends it fresh")

		_, err = store.Spend(ctx, child, 2001)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

		require.Nil(t, outputSpendingData(t, ctx, store, q, 0), "the replay's fresh spend of Q:0 is reversed")
		require.Equal(t, spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes(), outputSpendingData(t, ctx, store, p, 0),
			"the historical match on P:0 is a confirmed spend and is never reversed")
		require.False(t, recordedSpendHeight(t, ctx, store, q, 0).Valid, "Unspend and the rollback cleared Q:0's height")
		require.True(t, recordedSpendHeight(t, ctx, store, p, 0).Valid)
	})
}

// An output recorded with no height, by a build that stored none, counts as
// history. With no record for its spender it is refused.
func TestSpendRefusesAnUnrecordedHeightWhenTheSpenderHasNoRecord(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := newTwoOutputParent(t, 24)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		c := newSpenderOf(t, p, 24, 0)
		plantHistoricalSpend(t, ctx, store, p, c, 0)
		require.False(t, recordedSpendHeight(t, ctx, store, p, 0).Valid, "fixture: no height was recorded")

		_, err = store.Spend(ctx, c, 1001)
		require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "NULL height is history, and C has no record")
		require.NotEmpty(t, outputSpendingData(t, ctx, store, p, 0), "the refusal writes nothing and clears nothing")
	})
}

// The same unrecorded height with the spender stored is an ordinary match.
func TestSpendAcceptsAnUnrecordedHeightWhenTheSpenderIsStored(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		p := newTwoOutputParent(t, 25)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		c := newSpenderOf(t, p, 25, 0)
		_, _, err = store.SpendAndCreate(ctx, c, 1000)
		require.NoError(t, err)

		_, err = store.db.ExecContext(ctx, `UPDATE outputs SET spending_height = NULL WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)`, p.TxIDChainHash()[:])
		require.NoError(t, err)

		_, err = store.Spend(ctx, c, 1001)
		require.NoError(t, err)
	})
}

// With no retention nothing is ever pruned, so no match is history and an honest
// retry a block after its failed attempt is not refused.
func TestSpendAtZeroRetentionNeverTreatsAMatchAsAReplay(t *testing.T) {
	forEachSpendPath(t, func(t *testing.T, ctx context.Context, store *Store) {
		store.settings.GlobalBlockHeightRetention = 0
		store.settings.UtxoStore.BlockHeightRetentionAdjustment = 0

		p := newTwoOutputParent(t, 28)
		_, err := store.Create(ctx, p, 1000)
		require.NoError(t, err)

		d := newSpenderOf(t, p, 28, 0)
		_, err = store.Spend(ctx, d, 1200)
		require.NoError(t, err)

		_, err = store.Spend(ctx, d, 1201)
		require.NoError(t, err)

		// A spend recorded without a height and with no record for its spender is
		// refused at any other retention; with none it is an ordinary match.
		c := newSpenderOf(t, p, 29, 1)
		plantHistoricalSpend(t, ctx, store, p, c, 0)

		_, err = store.Spend(ctx, c, 1500)
		require.NoError(t, err)
	})
}
