package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func TestGetRoundTrip(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 2, 80_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	md, err := store.Get(context.Background(), tx.TxIDChainHash())
	require.NoError(t, err)
	require.NotNil(t, md.Tx)
	require.Equal(t, tx.TxID(), md.Tx.TxID())
	require.Equal(t, tx.Size(), int(md.SizeInBytes))
	require.Equal(t, uint32(100), md.UnminedSince)
	require.False(t, md.IsCoinbase)
	require.Empty(t, md.BlockIDs)
	require.NotZero(t, md.CreatedAt)
	require.Equal(t, 1, len(md.TxInpoints.ParentTxHashes))
}

func TestGetFieldsSubset(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 90_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	md, err := store.Get(context.Background(), tx.TxIDChainHash(), fields.Fee, fields.LockTime)
	require.NoError(t, err)
	require.Nil(t, md.Tx)
	require.NotZero(t, md.Fee)
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 100_000)

	_, err := store.Get(context.Background(), tx.TxIDChainHash())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestGetMetaFillsData(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 105_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	var data meta.Data

	err = store.GetMeta(context.Background(), tx.TxIDChainHash(), &data)
	require.NoError(t, err)
	require.Nil(t, data.Tx)
	require.Equal(t, tx.Size(), int(data.SizeInBytes))
}

func TestGetSpendStates(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 2, 110_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
	require.NoError(t, err)

	sp := &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash}

	resp, err := store.GetSpend(context.Background(), sp)
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status)
	require.Nil(t, resp.SpendingData)

	spender := newExtendedTx(t, 1, 111_000)
	sd := spend.NewSpendingData(spender.TxIDChainHash(), 0)

	_, err = store.pool.Exec(context.Background(),
		`UPDATE packed_txs SET spends = overlay(spends PLACING $2::bytea FROM $3) WHERE hash = $1`,
		tx.TxIDChainHash()[:], packSpendingData(sd), 1*slotSpendSize+1)
	require.NoError(t, err)

	resp, err = store.GetSpend(context.Background(), sp)
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_SPENT), resp.Status)
	require.NotNil(t, resp.SpendingData)
	require.Equal(t, spender.TxIDChainHash().String(), resp.SpendingData.TxID.String())

	unknown := newExtendedTx(t, 1, 112_000)
	resp, err = store.GetSpend(context.Background(), &utxo.Spend{TxID: unknown.TxIDChainHash(), Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_NOT_FOUND), resp.Status)
}
