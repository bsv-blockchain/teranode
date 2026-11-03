package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestParentOutputsForValidationAnswersAReassignedOutputAsStored: a reassigned output still
// holds the old owner's value and script, and it is answered with them, as aerospike answers
// from the record's outputs, both while it is unspent and after the new owner has spent it. The
// spender gets a verdict from that: the new owner's spend fails its script check, and the old
// owner's is refused by the spend's claim check. An error here made a block spending the
// output a store fault, which block validation retries for ever.
func TestParentOutputsForValidationAnswersAReassignedOutputAsStored(t *testing.T) {
	s, ctx := newTestStore(t)
	parent, newOutput, _, _, spendable := reassignedParent(t, s, ctx, 8_000)

	check := func(t *testing.T) {
		t.Helper()

		answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{
			{TxID: *parent.TxIDChainHash(), Vout: 0},
			{TxID: *parent.TxIDChainHash(), Vout: 1},
		})
		require.NoError(t, err)
		require.Len(t, answers, 2)

		for vout, answer := range answers {
			require.NoError(t, answer.Err, "output %d", vout)
			require.Contains(t, []utxo.ParentOutputStatus{utxo.ParentOutputMined, utxo.ParentOutputNotMined}, answer.Status, "output %d", vout)
			require.Equal(t, parent.Outputs[vout].Satoshis, answer.Satoshis, "output %d", vout)
			require.Equal(t, []byte(*parent.Outputs[vout].LockingScript), []byte(*answer.LockingScript), "output %d", vout)
		}
	}

	t.Run("unspent", check)

	// The old owner, presenting the output it was just answered with, is refused at the spend.
	_, spends, err := s.SpendAndCreate(ctx, spendParentTx(t, parent, nil, 500, 0), spendable)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrUtxoHashMismatch), "got %v", spends[0].Err)

	_, _, err = s.SpendAndCreate(ctx, spendParentTx(t, parent, newOutput, 1_000, 0), spendable)
	require.NoError(t, err)

	t.Run("spent by the new owner", check)
}

// TestFreezeOfAReassignedOutputNeedsItsNewHash: the alert system names a UTXO by the hash it
// computes from ParentOutputsForValidation, which for a reassigned UTXO is the old owner's. A
// freeze or unfreeze naming that hash must not act on the UTXO its new owner now holds, as
// aerospike refuses the mismatched hash. Naming the new hash still works.
func TestFreezeOfAReassignedOutputNeedsItsNewHash(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()
	parent, _, newHash, oldHash, _ := reassignedParent(t, s, ctx, 8_000)

	byOld := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: oldHash}}
	byNew := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: newHash}}

	require.Error(t, s.FreezeUTXOs(ctx, byOld, tSettings))
	require.Error(t, s.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 0}}, tSettings), "no hash")

	resp, err := s.GetSpend(ctx, byNew[0])
	require.NoError(t, err)
	require.NotEqual(t, int(utxo.Status_FROZEN), resp.Status, "the old owner's hash froze the new owner's UTXO")

	require.NoError(t, s.FreezeUTXOs(ctx, byNew, tSettings))
	require.Error(t, s.UnFreezeUTXOs(ctx, byOld, tSettings))
	require.NoError(t, s.UnFreezeUTXOs(ctx, byNew, tSettings))

	// A UTXO never reassigned is still frozen by its outpoint alone, as before.
	other := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 1}}
	require.NoError(t, s.FreezeUTXOs(ctx, other, tSettings))
}

// TestParentOutputsForValidationFailsOnlyTheUnreadableParent: a parent whose stored row does not
// decode is a storage fault on that parent alone. Its entries carry the error, and a parent
// that travelled in the same call is answered normally, so one corrupt row cannot reject every
// transaction validated beside it.
func TestParentOutputsForValidationFailsOnlyTheUnreadableParent(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	bad := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, bad, 1_000)
	require.NoError(t, err)

	good := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, good, 1_000)
	require.NoError(t, err)

	h := bad.TxIDChainHash()
	tag, err := s.pool.Exec(ctx, `UPDATE tx_ident SET tx_inpoints = '\xff'::bytea WHERE leaf = $1 AND txid = $2`, LeafFor(h[:]), h[:])
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())

	answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{
		{TxID: *h, Vout: 0},
		{TxID: *good.TxIDChainHash(), Vout: 0},
	})
	require.NoError(t, err)

	require.Error(t, answers[0].Err, "the unreadable parent's entry carries the fault")
	require.NoError(t, answers[1].Err, "the other parent is answered")
	require.Equal(t, good.Outputs[0].Satoshis, answers[1].Satoshis)
}
