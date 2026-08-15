package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestSetMinedMulti(t *testing.T) {
	store := newTestStore(t)

	tx1 := newExtendedTx(t, 1, 400_000)
	tx2 := newExtendedTx(t, 1, 401_000)

	_, err := store.Create(context.Background(), tx1, 100)
	require.NoError(t, err)
	_, err = store.Create(context.Background(), tx2, 100)
	require.NoError(t, err)

	hashes := []*chainhash.Hash{tx1.TxIDChainHash(), tx2.TxIDChainHash()}

	result, err := store.SetMinedMulti(context.Background(), hashes,
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, SubtreeIdx: 1, OnLongestChain: true})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result[*tx1.TxIDChainHash()], uint32(7))
	require.Contains(t, result[*tx2.TxIDChainHash()], uint32(7))

	var unminedSince *int64

	var blockRefs []byte
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT unmined_since, block_refs FROM packed_txs WHERE hash = $1`, tx1.TxIDChainHash()[:]).
		Scan(&unminedSince, &blockRefs))
	require.Nil(t, unminedSince)

	ids, heights, subtrees := unpackBlockRefs(blockRefs)
	require.Equal(t, []uint32{7}, ids)
	require.Equal(t, []uint32{100}, heights)
	require.Equal(t, []int{1}, subtrees)
}

func TestSetMinedStampsDAHForFullySpent(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 410_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	_, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(context.Background(), []*chainhash.Hash{parent.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 105, OnLongestChain: true})
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&dah))

	retention := int64(store.settings.GetUtxoStoreBlockHeightRetention())
	require.NotNil(t, dah)
	require.Equal(t, 105+retention, *dah)
}

func TestUnsetMined(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 420_000)

	_, err := store.Create(context.Background(), tx, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true}))
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(110))

	_, err = store.SetMinedMulti(context.Background(), []*chainhash.Hash{tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, UnsetMined: true})
	require.NoError(t, err)

	var unminedSince *int64

	var dah *int64

	var blockRefs []byte
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT unmined_since, delete_at_height, block_refs FROM packed_txs WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&unminedSince, &dah, &blockRefs))
	require.NotNil(t, unminedSince)
	require.Equal(t, int64(111), *unminedSince)
	require.Nil(t, dah)
	require.Empty(t, blockRefs)

	missing := newExtendedTx(t, 1, 421_000)

	_, err = store.SetMinedMulti(context.Background(), []*chainhash.Hash{missing.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 5, UnsetMined: true})
	require.NoError(t, err)
}

func TestSetMinedMissingTxErrors(t *testing.T) {
	store := newTestStore(t)
	missing := newExtendedTx(t, 1, 430_000)

	_, err := store.SetMinedMulti(context.Background(), []*chainhash.Hash{missing.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 440_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	require.NoError(t, store.MarkTransactionsOnLongestChain(context.Background(),
		[]chainhash.Hash{*tx.TxIDChainHash()}, true))

	var unminedSince *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT unmined_since FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&unminedSince))
	require.Nil(t, unminedSince)

	require.NoError(t, store.SetBlockHeight(120))
	require.NoError(t, store.MarkTransactionsOnLongestChain(context.Background(),
		[]chainhash.Hash{*tx.TxIDChainHash()}, false))

	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT unmined_since FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&unminedSince))
	require.NotNil(t, unminedSince)
	require.Equal(t, int64(120), *unminedSince)

	missing := newExtendedTx(t, 1, 441_000)
	err = store.MarkTransactionsOnLongestChain(context.Background(),
		[]chainhash.Hash{*missing.TxIDChainHash()}, true)
	require.Error(t, err)
}
