package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func collectUnmined(t *testing.T, it utxo.UnminedTxIterator) []*utxo.UnminedTransaction {
	t.Helper()

	var all []*utxo.UnminedTransaction

	for {
		batch, err := it.Next(context.Background())
		require.NoError(t, err)

		if batch == nil {
			break
		}

		all = append(all, batch...)
	}

	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	return all
}

func TestUnminedIterator(t *testing.T) {
	store := newTestStore(t)

	unmined := make(map[chainhash.Hash]bool)

	for i := 0; i < 3; i++ {
		tx := newExtendedTx(t, 1, 500_000+uint64(i)*1000)
		_, err := store.Create(context.Background(), tx, 100+uint32(i))
		require.NoError(t, err)

		unmined[*tx.TxIDChainHash()] = true
	}

	mined := newExtendedTx(t, 1, 505_000)
	_, err := store.Create(context.Background(), mined, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	all := collectUnmined(t, it)
	require.Len(t, all, 3)

	for _, u := range all {
		require.False(t, u.Skip)
		require.True(t, unmined[u.Node.Hash])
		require.NotZero(t, u.Node.Fee)
		require.NotZero(t, u.Node.SizeInBytes)
		require.NotNil(t, u.TxInpoints)
		require.Len(t, u.TxInpoints.ParentTxHashes, 1)
		require.NotZero(t, u.UnminedSince)
		require.NotZero(t, u.CreatedAt)
	}
}

func TestPrunableIteratorFilters(t *testing.T) {
	store := newTestStore(t)

	oldTx := newExtendedTx(t, 1, 510_000)
	_, err := store.Create(context.Background(), oldTx, 50)
	require.NoError(t, err)

	newTx := newExtendedTx(t, 1, 511_000)
	_, err = store.Create(context.Background(), newTx, 200)
	require.NoError(t, err)

	it, err := store.GetPrunableUnminedTxIterator(100)
	require.NoError(t, err)

	all := collectUnmined(t, it)
	require.Len(t, all, 1)
	require.Equal(t, *oldTx.TxIDChainHash(), all[0].Node.Hash)
}

func TestConflictingIterator(t *testing.T) {
	store := newTestStore(t)

	conflicting := newExtendedTx(t, 1, 520_000)
	_, err := store.Create(context.Background(), conflicting, 100, utxo.WithConflicting(true))
	require.NoError(t, err)

	normal := newExtendedTx(t, 1, 521_000)
	_, err = store.Create(context.Background(), normal, 100)
	require.NoError(t, err)

	it, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	all := collectUnmined(t, it)
	require.Len(t, all, 1)
	require.Equal(t, *conflicting.TxIDChainHash(), all[0].Node.Hash)
}

func TestConsistencyScan(t *testing.T) {
	store := newTestStore(t)

	inconsistent := newExtendedTx(t, 1, 530_000)
	_, err := store.Create(context.Background(), inconsistent, 100)
	require.NoError(t, err)

	_, err = store.pool.Exec(context.Background(),
		`UPDATE packed_txs SET block_refs = $2 WHERE hash = $1`,
		inconsistent.TxIDChainHash()[:],
		packBlockRefs([]utxo.MinedBlockInfo{{BlockID: 9, BlockHeight: 100}}))
	require.NoError(t, err)

	clean := newExtendedTx(t, 1, 531_000)
	_, err = store.Create(context.Background(), clean, 100)
	require.NoError(t, err)

	it, err := store.ScanInconsistentUnminedTxs()
	require.NoError(t, err)

	var found []*utxo.InconsistentTxRecord

	for {
		batch, err := it.Next(context.Background())
		require.NoError(t, err)

		if batch == nil {
			break
		}

		found = append(found, batch...)
	}

	require.NoError(t, it.Close())
	require.GreaterOrEqual(t, it.TotalScanned(), int64(2))
	require.Len(t, found, 1)
	require.Equal(t, *inconsistent.TxIDChainHash(), found[0].Hash)
	require.Equal(t, []uint32{9}, found[0].BlockIDs)
}

func TestQueryOldUnmined(t *testing.T) {
	store := newTestStore(t)

	oldTx := newExtendedTx(t, 1, 540_000)
	_, err := store.Create(context.Background(), oldTx, 50)
	require.NoError(t, err)

	newTx := newExtendedTx(t, 1, 541_000)
	_, err = store.Create(context.Background(), newTx, 200)
	require.NoError(t, err)

	hashes, err := store.QueryOldUnminedTransactions(context.Background(), 100)
	require.NoError(t, err)
	require.Len(t, hashes, 1)
	require.Equal(t, *oldTx.TxIDChainHash(), hashes[0])
}

func TestPreserveAndExpire(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 550_000)

	_, err := store.Create(context.Background(), parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.NoError(t, err)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah)

	require.NoError(t, store.PreserveTransactions(context.Background(),
		[]chainhash.Hash{*parent.TxIDChainHash()}, 500))

	var preserveUntil *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height, preserve_until FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&dah, &preserveUntil))
	require.Nil(t, dah)
	require.NotNil(t, preserveUntil)
	require.Equal(t, int64(500), *preserveUntil)

	notEligible := newExtendedTx(t, 1, 551_000)
	_, err = store.Create(context.Background(), notEligible, 100)
	require.NoError(t, err)

	require.NoError(t, store.PreserveTransactions(context.Background(),
		[]chainhash.Hash{*notEligible.TxIDChainHash()}, 500))

	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT preserve_until FROM packed_txs WHERE hash = $1`,
		notEligible.TxIDChainHash()[:]).Scan(&preserveUntil))
	require.Nil(t, preserveUntil)

	require.NoError(t, store.ProcessExpiredPreservations(context.Background(), 501))

	retention := int64(store.settings.GetUtxoStoreBlockHeightRetention())
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height, preserve_until FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&dah, &preserveUntil))
	require.Nil(t, preserveUntil)
	require.NotNil(t, dah)
	require.Equal(t, 501+retention, *dah)
}
