package model

import (
	"context"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

var (
	tx0 = newTx(0)
	tx1 = newTx(1)
	tx2 = newTx(2)
	tx3 = newTx(3)
	tx4 = newTx(4)
	tx5 = newTx(5)
	tx6 = newTx(6)
	tx7 = newTx(7)
)

func TestUpdateTxMinedStatus(t *testing.T) {
	t.Run("TestUpdateTxMinedStatus", func(t *testing.T) {
		txMetaStore := memory.New(ulogger.TestLogger{}, true)

		_, err := txMetaStore.Create(context.Background(), tx0)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx1)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx2)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx3)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx4)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx5)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx6)
		require.NoError(t, err)
		_, err = txMetaStore.Create(context.Background(), tx7)
		require.NoError(t, err)

		subtrees := []*util.Subtree{
			{
				Nodes: []util.SubtreeNode{
					{
						Hash: *tx0.TxIDChainHash(),
					},
					{
						Hash: *tx1.TxIDChainHash(),
					},
					{
						Hash: *tx2.TxIDChainHash(),
					},
					{
						Hash: *tx3.TxIDChainHash(),
					},
				},
			},
			{
				Nodes: []util.SubtreeNode{
					{
						Hash: *tx4.TxIDChainHash(),
					},
					{
						Hash: *tx5.TxIDChainHash(),
					},
					{
						Hash: *tx6.TxIDChainHash(),
					},
					{
						Hash: *tx7.TxIDChainHash(),
					},
				},
			},
		}

		err = UpdateTxMinedStatus(
			context.Background(),
			ulogger.TestLogger{},
			txMetaStore,
			subtrees,
			&chainhash.Hash{},
			1,
		)
		require.NoError(t, err)

		txMeta, err := txMetaStore.Get(context.Background(), tx1.TxIDChainHash())
		require.NoError(t, err)

		assert.Equal(t, uint32(1), txMeta.BlockIDs[0])
	})
}

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime
	return tx
}
