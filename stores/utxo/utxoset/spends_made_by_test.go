package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestSpendsMadeByAnswersWithoutTheTransaction is the whole point of the method on this store.
//
// Undoing a conflict has to put back the coins a transaction took, and it identifies each coin
// partly by the amount and the spending rules of the output being consumed. A transaction only
// records those when it is written in the longer form, and this store writes the short one, so
// the answer cannot come from the transaction here.
//
// It comes from two things this store keeps for far longer than it keeps transactions: the list
// of what each transaction spends, and the undo journal, which copied down each coin's amount
// and rules at the moment it was destroyed.
func TestSpendsMadeByAnswersWithoutTheTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	got, err := s.SpendsMadeBy(ctx, *child.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, got, 1, "it took exactly one coin")

	require.Equal(t, parent.TxIDChainHash().String(), got[0].TxID.String(), "from its parent")
	require.Equal(t, uint32(0), got[0].Vout, "output zero")
	require.NotNil(t, got[0].SpendingData, "and it names itself as the spender")
	require.Equal(t, child.TxIDChainHash().String(), got[0].SpendingData.TxID.String())

	// The proof that the records are usable: hand them straight to Unspend, which is what
	// undoing a conflict does with them.
	require.NoError(t, s.Unspend(ctx, got, false))
}

// TestSpendsMadeByStillAnswersOnceTheTransactionHasAgedOut is the reason this approach was
// chosen over reading the transaction and filling in its blanks.
//
// This store throws away transaction bytes after a couple of days but keeps a transaction that
// lost a double-spend indefinitely, because it may still need promoting. Anything that works by
// reading the transaction stops working at that horizon, silently. The two sources this uses do
// not age out on that schedule.
func TestSpendsMadeByStillAnswersOnceTheTransactionHasAgedOut(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// Drop the stored bytes, exactly as the reclaim does once they pass their horizon.
	dropped, err := s.dropTxBodyWindowsBelow(ctx, 100_000)
	require.NoError(t, err)
	require.Positive(t, dropped)

	ch := child.TxIDChainHash()

	meta, err := s.Get(ctx, ch)
	require.NoError(t, err)
	require.Nil(t, meta.Tx, "the transaction is gone, which is the state under test")

	got, err := s.SpendsMadeBy(ctx, *ch)
	require.NoError(t, err, "and the answer must still come back")
	require.Len(t, got, 1)
	require.Equal(t, parent.TxIDChainHash().String(), got[0].TxID.String())
}

// TestSpendsMadeByOmitsWhatItCannotRestore.
//
// A transaction that never actually took a coin has nothing to put back for that input. This
// store's Unspend refuses the whole batch if any record it is given cannot be restored, so
// reporting an input that was never spent would break the undo rather than pad it.
func TestSpendsMadeByOmitsWhatItCannotRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	// Created but never spent, so it took nothing.
	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	got, err := s.SpendsMadeBy(ctx, *child.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, got, "nothing was taken, so there is nothing to give back")

	require.NoError(t, s.Unspend(ctx, got, false), "and an empty restore is a clean no-op")
}

// TestSpendsMadeByReportsATransactionItDoesNotHold, so an undo aimed at something absent fails
// loudly rather than quietly restoring nothing.
func TestSpendsMadeByReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	missing := mkTx(t, 1, 9_999)

	_, err := s.SpendsMadeBy(ctx, *missing.TxIDChainHash())
	require.Error(t, err)
}

// TestSpendsMadeByMatchesWhatSetConflictingReturns. Both answer the same question from the same
// place, and undoing a conflict uses them one after the other, so a disagreement between them
// would restore a different set of coins than the one that was marked.
func TestSpendsMadeByMatchesWhatSetConflictingReturns(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	ch := child.TxIDChainHash()

	viaMethod, err := s.SpendsMadeBy(ctx, *ch)
	require.NoError(t, err)

	viaConflicting, _, err := s.SetConflicting(ctx, []chainhash.Hash{*ch}, true)
	require.NoError(t, err)

	require.Len(t, viaMethod, len(viaConflicting), "both must report the same number of coins")

	for i := range viaMethod {
		require.Equal(t, viaConflicting[i].TxID.String(), viaMethod[i].TxID.String(),
			"coin %d must come from the same transaction", i)
		require.Equal(t, viaConflicting[i].Vout, viaMethod[i].Vout,
			"and the same output", i)
	}
}
