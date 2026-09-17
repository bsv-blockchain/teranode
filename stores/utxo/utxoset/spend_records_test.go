package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendReturnsRecordsThisStoreCanRestore.
//
// Conflict resolution undoes its own spends when it fails part way, and it does that by handing
// the records this store returned straight back to this store's Unspend. Unspend refuses a
// record that cannot name the transaction that took the coin, deliberately, because restoring
// on the outpoint alone could resurrect a coin a different transaction now owns.
//
// So a record without a spender is not merely incomplete. It made every conflict-resolution
// failure escalate to the manual-intervention message, whatever had actually gone wrong,
// because the rollback itself could never succeed.
func TestSpendReturnsRecordsThisStoreCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	require.NotNil(t, spends[0].SpendingData,
		"the record must name the transaction that took the coin")
	require.Equal(t, child.TxIDChainHash().String(), spends[0].SpendingData.TxID.String())

	// The proof: hand them back unmodified, which is exactly what a rollback does.
	require.NoError(t, s.Unspend(ctx, spends, false),
		"a record this store produced must be one this store can restore")
}

// TestSpendAndCreateReturnsRecordsThisStoreCanRestore is the same rule on the combined call,
// which is the one conflict resolution actually uses.
func TestSpendAndCreateReturnsRecordsThisStoreCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	_, spends, err := s.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	require.NotNil(t, spends[0].SpendingData)
	require.Equal(t, child.TxIDChainHash().String(), spends[0].SpendingData.TxID.String())

	require.NoError(t, s.Unspend(ctx, spends, false))
}

// TestSpendNamesTheRightSpenderPerTransactionInOnePlan. One plan carries many transactions,
// and each record must name ITS OWN spender. Naming the plan's first, or last, would restore
// coins to the wrong owner on a rollback.
//
// This drives planSpends and runSpendPlan directly, because the store no longer exposes a way
// to put two transactions through one statement. It keeps the property under test because
// planSpends is still multi-item, which is the shape a batched SpendAndCreate would need.
func TestSpendNamesTheRightSpenderPerTransactionInOnePlan(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	first := spendOutput(t, parent, 0, 1)
	second := spendOutput(t, parent, 1, 2)

	require.NotEqual(t, first.TxIDChainHash().String(), second.TxIDChainHash().String())

	// The journal partition needs its own connection, so it is prepared before the
	// transaction opens rather than inside it.
	require.NoError(t, s.ensureSpendJournalPartition(ctx, 101))

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	plan := planSpends([]*spendItem{
		{tx: first, blockHeight: 101},
		{tx: second, blockHeight: 101},
	})

	require.NoError(t, s.runSpendPlan(ctx, dbTx, plan))
	require.NoError(t, dbTx.Commit(ctx))

	for i, want := range []string{first.TxIDChainHash().String(), second.TxIDChainHash().String()} {
		require.Len(t, plan.perItem[i], 1)
		require.NoError(t, plan.perItem[i][0].Err)
		require.NotNil(t, plan.perItem[i][0].SpendingData)
		require.Equal(t, want, plan.perItem[i][0].SpendingData.TxID.String(),
			"record %d must name its own spender, not another transaction's", i)
	}

	// And both are restorable together, which is what a rollback does.
	all := make([]*utxo.Spend, 0, 2)
	for _, spends := range plan.perItem {
		all = append(all, spends...)
	}

	require.NoError(t, s.Unspend(ctx, all, false))
}
