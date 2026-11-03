package utxoset

import (
	"testing"

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
