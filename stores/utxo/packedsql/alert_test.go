package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func TestFreezeBlocksSpend(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 2, 700_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	utxoHash1, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[1], 1)
	require.NoError(t, err)

	frozen := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash1}

	require.NoError(t, store.FreezeUTXOs(context.Background(), []*utxo.Spend{frozen}, store.settings))

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 1), 101)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrFrozen), "got %v", spends[0].Err)

	spends, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	err = store.FreezeUTXOs(context.Background(), []*utxo.Spend{frozen}, store.settings)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen))
}

func TestUnfreezeRestoresSpend(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 710_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	utxoHash0, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	sp := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}

	err = store.UnFreezeUTXOs(context.Background(), []*utxo.Spend{sp}, store.settings)
	require.Error(t, err)

	require.NoError(t, store.FreezeUTXOs(context.Background(), []*utxo.Spend{sp}, store.settings))
	require.NoError(t, store.UnFreezeUTXOs(context.Background(), []*utxo.Spend{sp}, store.settings))

	var flags int16
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&flags))
	require.Zero(t, flags&flagHasOverrides)

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

func TestReassign(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 720_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	utxoHash0, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	oldSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}

	other := newExtendedTx(t, 1, 721_000)
	newHash, err := util.UTXOHashFromOutput(other.TxIDChainHash(), other.Outputs[0], 0)
	require.NoError(t, err)

	newSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: newHash}

	err = store.ReAssignUTXO(context.Background(), oldSpend, newSpend, store.settings)
	require.Error(t, err)

	require.NoError(t, store.SetBlockHeight(100))
	require.NoError(t, store.FreezeUTXOs(context.Background(), []*utxo.Spend{oldSpend}, store.settings))
	require.NoError(t, store.ReAssignUTXO(context.Background(), oldSpend, newSpend, store.settings))

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.Error(t, err)
	require.Error(t, spends[0].Err)

	spendableAt := uint32(100 + utxo.ReAssignedUtxoSpendableAfterBlocks)

	spenderTx := newSpendingTx(t, parent, 0)

	spends, err = store.Spend(context.Background(), spenderTx, spendableAt, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}
