package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestParentOutputsForValidationRefusesAReassignedOutput: an output an operator reassigned still
// holds the old owner's value and script, so handing them to the validator would validate a
// spend against the wrong terms. The entry carries an error instead, and the parent's other
// output is answered as usual.
func TestParentOutputsForValidationRefusesAReassignedOutput(t *testing.T) {
	s, ctx := newTestStore(t)
	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(),
		&bt.Output{Satoshis: 5_000, LockingScript: newOwnerScript(t)}, 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old, &utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{
		{TxID: *parent.TxIDChainHash(), Vout: 0},
		{TxID: *parent.TxIDChainHash(), Vout: 1},
	})
	require.NoError(t, err)

	require.Error(t, answers[0].Err, "a reassigned output must not be answered with the old owner's terms")
	require.Contains(t, answers[0].Err.Error(), "reassigned")

	require.NoError(t, answers[1].Err)
	require.Equal(t, parent.Outputs[1].Satoshis, answers[1].Satoshis)
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
