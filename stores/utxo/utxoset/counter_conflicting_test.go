package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestCounterConflictingSurvivesABodyThatHasAgedOut is a crash test for shared code, driven
// through the one store that can actually produce the state.
//
// This store bounds serialized transaction bytes by a retention horizon and drops them by
// window, so a metadata read returns a record whose transaction is nil for anything older.
// That is the ordinary steady state rather than an error, and a transaction that lost a
// double-spend race is kept indefinitely while its bytes age out, so the combination is not
// exotic.
//
// The shared walk that finds who else spent a transaction's inputs read those inputs off the
// transaction body. With the body gone that is a nil pointer dereference, in a worker with
// nothing above it to recover, so it takes the whole process down. It is reachable from an
// incoming subtree, which means from the network.
//
// The inputs are on the identity record too, as stored inpoints, and those never age out.
func TestCounterConflictingSurvivesABodyThatHasAgedOut(t *testing.T) {
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

	// Age the bytes out, exactly as the pruner does once the horizon passes.
	dropped, err := s.dropTxBodyWindowsBelow(ctx, 100_000)
	require.NoError(t, err)
	require.Positive(t, dropped, "the body windows must actually have been dropped")

	ch := child.TxIDChainHash()

	got, err := s.Get(ctx, ch)
	require.NoError(t, err)
	require.Nil(t, got.Tx, "the body is gone, which is the state under test")
	require.NotEmpty(t, got.TxInpoints.ParentTxHashes, "but what it spends is still known")

	// The call that used to panic here, and then could not answer at all.
	//
	// It works now because the metadata read names who took each of a parent's outputs, read
	// from the journal since the coin row is gone. Both halves were needed: reading the
	// transaction's own inputs from the stored inpoints, which survive the body ageing out, and
	// reading its parents' spenders from the journal.
	hashes, err := utxo.GetCounterConflictingTxHashes(ctx, s, *ch, 1_000)
	require.NoError(t, err, "a body-less transaction must be answerable, not fatal")
	require.NotEmpty(t, hashes, "and the transaction itself is always in the answer")

	inSet := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		inSet[h.String()] = true
	}

	require.True(t, inSet[ch.String()], "the subject is always part of its own counter set")
}

// TestReverseConflictReachesATransactionWhoseBytesHaveGone.
//
// This store throws transaction bytes away after a couple of days, but keeps a transaction that
// lost a double-spend indefinitely, because it may still need promoting. So the two conditions
// meet routinely rather than rarely.
//
// Undoing a conflict used to skip such a transaction outright, and silently: it read the
// transaction to find out what it spends, found nothing, and moved on. The demotion did not
// happen and nobody was told. It now reads that from the permanent record instead, so the work
// still happens.
//
// What this does NOT fix is promoting the winner, which calls a method taking an actual
// transaction. That step still needs the winner's bytes and is a stated limit.
func TestReverseConflictReachesATransactionWhoseBytesHaveGone(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	loser := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, loser, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, loser, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	dropped, err := s.dropTxBodyWindowsBelow(ctx, 100_000)
	require.NoError(t, err)
	require.Positive(t, dropped, "the bytes must actually be gone for this to test anything")

	lh := loser.TxIDChainHash()

	got, err := s.Get(ctx, lh)
	require.NoError(t, err)
	require.Nil(t, got.Tx, "no transaction")
	require.NotEmpty(t, got.TxInpoints.ParentTxHashes, "but what it spent is still on record")

	// The step that used to be skipped: what does this transaction spend.
	made, err := s.SpendsMadeBy(ctx, *lh)
	require.NoError(t, err)
	require.Len(t, made, 1, "its one input is still findable without the transaction")
	require.Equal(t, parent.TxIDChainHash().String(), made[0].TxID.String())

	// And the coins can be put back, which is what the undo does with them.
	require.NoError(t, s.Unspend(ctx, made, false))

	resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status, "the coin is spendable again")
}
