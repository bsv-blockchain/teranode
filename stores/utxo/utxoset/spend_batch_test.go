package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendGivesEachFailedInputAnErrorOfItsOwn is a transaction with one spent input and one
// frozen input. The validator joins its input errors into one chain, so if the store handed
// out the package-level error values, that join would chain ErrFrozen onto the global ErrSpent
// and every later double spend in the process would also read as frozen.
func TestSpendGivesEachFailedInputAnErrorOfItsOwn(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 3, 8_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	_, _, err = s.SpendAndCreate(ctx, spendParentTx(t, parent, nil, 1_000, 0), 100)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 1}}, settings.NewSettings()))

	_, spends, err := s.SpendAndCreate(ctx, spendParentTx(t, parent, nil, 2_000, 0, 1), 100)
	require.Error(t, err)
	require.Len(t, spends, 2)
	require.True(t, errors.Is(spends[0].Err, errors.ErrSpent), "got %v", spends[0].Err)
	require.True(t, errors.Is(spends[1].Err, errors.ErrFrozen), "got %v", spends[1].Err)
	require.NotSame(t, errors.ErrSpent, spends[0].Err)
	require.NotSame(t, errors.ErrFrozen, spends[1].Err)

	// What the validator does with them.
	_ = errors.JoinCapped(10, spends[0].Err, spends[1].Err)

	require.False(t, errors.Is(errors.ErrSpent, errors.ErrFrozen), "the join reached the global ErrSpent")

	_, spends, err = s.SpendAndCreate(ctx, spendParentTx(t, parent, nil, 3_000, 0), 100)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrSpent), "got %v", spends[0].Err)
	require.False(t, errors.Is(spends[0].Err, errors.ErrFrozen), "a later double spend reads as frozen: %v", spends[0].Err)
}

// TestSpendGivesAConflictingAndALockedInputEachAnErrorOfItsOwn is the same rule for the other two
// refusals the classifier hands out: a transaction with one input from a conflicting parent and
// one from a locked parent. Handing out the package-level ErrTxConflicting and ErrTxLocked would
// let the validator's join chain ErrTxLocked onto the global ErrTxConflicting, and every later
// conflicting spend in the process would also read as locked.
func TestSpendGivesAConflictingAndALockedInputEachAnErrorOfItsOwn(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	conflicting := mkTx(t, 2, 8_000)
	_, err := s.Create(ctx, conflicting, 100)
	require.NoError(t, err)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, true)
	require.NoError(t, err)

	locked := mkTx(t, 2, 9_000)
	_, err = s.Create(ctx, locked, 100, utxo.WithLocked(true))
	require.NoError(t, err)

	child := bt.NewTx()

	for _, parent := range []*bt.Tx{conflicting, locked} {
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          0,
			LockingScript: parent.Outputs[0].LockingScript,
			Satoshis:      parent.Outputs[0].Satoshis,
		}))
		child.Inputs[len(child.Inputs)-1].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	}

	child.AddOutput(&bt.Output{Satoshis: 1_000, LockingScript: conflicting.Outputs[1].LockingScript})

	_, spends, err := s.SpendAndCreate(ctx, child, 100)
	require.Error(t, err)
	require.Len(t, spends, 2)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxConflicting), "got %v", spends[0].Err)
	require.True(t, errors.Is(spends[1].Err, errors.ErrTxLocked), "got %v", spends[1].Err)
	require.NotSame(t, errors.ErrTxConflicting, spends[0].Err)
	require.NotSame(t, errors.ErrTxLocked, spends[1].Err)

	// What the validator does with them.
	_ = errors.JoinCapped(10, spends[0].Err, spends[1].Err)

	require.False(t, errors.Is(errors.ErrTxConflicting, errors.ErrTxLocked), "the join reached the global ErrTxConflicting")

	_, spends, err = s.SpendAndCreate(ctx, spendParentTx(t, conflicting, nil, 2_000, 1), 100)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxConflicting), "got %v", spends[0].Err)
	require.False(t, errors.Is(spends[0].Err, errors.ErrTxLocked), "a later conflicting spend reads as locked: %v", spends[0].Err)
}
