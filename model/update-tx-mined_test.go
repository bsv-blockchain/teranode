package model

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
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
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	tSettings.UtxoStore = settings.UtxoStoreSettings{
		UpdateTxMinedStatus: true,
		MaxMinedBatchSize:   1024,
		MaxMinedRoutines:    128,
		DBTimeout:           30 * time.Second, // Increase timeout for SQLite in-memory operations
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, err = utxoStore.Create(context.Background(), tx0, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx2, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx3, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx4, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx5, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx6, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), tx7, 0)
	require.NoError(t, err)

	block := &Block{}
	block.CoinbaseTx = tx0
	block.Subtrees = []*chainhash.Hash{
		tx1.TxIDChainHash(),
		tx2.TxIDChainHash(),
	}
	block.SubtreeSlices = []*subtree.Subtree{
		{
			Nodes: []subtree.SubtreeNode{
				{
					Hash: *subtree.CoinbasePlaceholderHash,
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
			Nodes: []subtree.SubtreeNode{
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
		ctx,
		logger,
		tSettings,
		utxoStore,
		block,
		1,
	)
	require.NoError(t, err)

	txMeta, err := utxoStore.Get(ctx, tx0.TxIDChainHash())
	require.NoError(t, err)
	assert.Empty(t, txMeta.BlockIDs) // tx0 is a coinbase tx, so it should not have any block IDs set by the SetMinedMulti process - its done in the block assembly process at the point of creating the coinbasetx

	txMeta, err = utxoStore.Get(ctx, tx1.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx2.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx3.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx4.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx5.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx6.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])

	txMeta, err = utxoStore.Get(ctx, tx7.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(1), txMeta.BlockIDs[0])
}

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime

	tx.Inputs = []*bt.Input{{
		UnlockingScript:    &bscript.Script{},
		PreviousTxOutIndex: 0,
	}}

	_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})

	return tx
}
