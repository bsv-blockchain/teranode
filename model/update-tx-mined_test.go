package model

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p"
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

	hashPrevBlock, _  = chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hashMerkleRoot, _ = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	nBits             = NewNBitFromString("1d00ffff")
	blockHeader       = &BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      1231469665,
		Bits:           nBits,
		Nonce:          2573394689,
	}
)

func TestUpdateTxMinedStatus(t *testing.T) {
	t.Run("TestUpdateTxMinedStatus", func(t *testing.T) {
		txMetaStore := memory.New(true)

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
			p2p.TestLogger{},
			txMetaStore,
			subtrees,
			blockHeader,
		)
		require.NoError(t, err)

		txMeta, err := txMetaStore.Get(context.Background(), tx0.TxIDChainHash())
		require.NoError(t, err)

		_ = txMeta
	})
}

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime
	return tx
}
