package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func newExtendedTx(t testing.TB, outputs int, satoshisSeed uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	for i := 0; i < outputs; i++ {
		amount := uint64(1000 + i)
		if i == 0 {
			amount = satoshisSeed
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", amount))
	}

	return tx
}

func rowCounts(t *testing.T, store *Store, tx *bt.Tx) (totalCount, page0Count, spentCount, pagesTotal int, spendsLen, hashesLen int) {
	t.Helper()

	err := store.pool.QueryRow(context.Background(),
		`SELECT total_count, page0_count, spent_count, pages_total, octet_length(spends), octet_length(utxo_hashes)
		 FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).
		Scan(&totalCount, &page0Count, &spentCount, &pagesTotal, &spendsLen, &hashesLen)
	require.NoError(t, err)

	return
}

func TestCreateSimpleTx(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 2, 10_000)

	md, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, tx.Size(), int(md.SizeInBytes))
	require.Equal(t, 1, len(md.TxInpoints.ParentTxHashes))

	totalCount, page0Count, spentCount, pagesTotal, spendsLen, hashesLen := rowCounts(t, store, tx)
	require.Equal(t, 2, totalCount)
	require.Equal(t, 2, page0Count)
	require.Equal(t, 0, spentCount)
	require.Equal(t, 0, pagesTotal)
	require.Equal(t, 2*slotSpendSize, spendsLen)
	require.Equal(t, 2*slotHashSize, hashesLen)
}

func TestCreateDuplicateReturnsTxExists(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 20_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	_, err = store.Create(context.Background(), tx, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists))
}

func TestCreateMultiPage(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, int(store.pageSize)+3, 30_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	totalCount, page0Count, _, pagesTotal, spendsLen, _ := rowCounts(t, store, tx)
	require.Equal(t, int(store.pageSize)+3, totalCount)
	require.Equal(t, int(store.pageSize), page0Count)
	require.Equal(t, 1, pagesTotal)
	require.Equal(t, int(store.pageSize)*slotSpendSize, spendsLen)

	var page, spendableCount, pageSpendsLen int
	err = store.pool.QueryRow(context.Background(),
		`SELECT page, spendable_count, octet_length(spends) FROM packed_tx_pages WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&page, &spendableCount, &pageSpendsLen)
	require.NoError(t, err)
	require.Equal(t, 1, page)
	require.Equal(t, 3, spendableCount)
	require.Equal(t, 3*slotSpendSize, pageSpendsLen)
}

func TestCreateWithOpReturnOutput(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 40_000)

	opReturn := &bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x00, 0x6a}),
	}
	tx.Outputs = append(tx.Outputs, opReturn)

	require.False(t, utxo.ShouldStoreOutputAsUTXO(opReturn, 100, store.settings.ChainCfgParams.GenesisActivationHeight))

	_, err := store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	totalCount, page0Count, _, _, spendsLen, hashesLen := rowCounts(t, store, tx)
	require.Equal(t, 1, totalCount)
	require.Equal(t, 1, page0Count)
	require.Equal(t, 2*slotSpendSize, spendsLen)
	require.Equal(t, 2*slotHashSize, hashesLen)

	var slot1 []byte
	err = store.pool.QueryRow(context.Background(),
		`SELECT substring(utxo_hashes FROM $2 FOR 32) FROM packed_txs WHERE hash = $1`,
		tx.TxIDChainHash()[:], 1*slotHashSize+1).Scan(&slot1)
	require.NoError(t, err)
	require.Equal(t, make([]byte, slotHashSize), slot1)
}

func TestCreateMinedUnspendableGetsDAH(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 0, 50_000)

	opReturn := &bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{0x00, 0x6a}),
	}
	tx.Outputs = append(tx.Outputs, opReturn)

	_, err := store.Create(context.Background(), tx, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	var dah int64
	err = store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah)
	require.NoError(t, err)

	retention := int64(store.settings.GetUtxoStoreBlockHeightRetention())
	require.Equal(t, 100+retention, dah)
}

func TestCreateCoinbase(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 60_000)

	_, err := store.Create(context.Background(), tx, 200, utxo.WithSetCoinbase(true))
	require.NoError(t, err)

	var flags int16

	var csh int64
	err = store.pool.QueryRow(context.Background(),
		`SELECT flags, coinbase_spending_height FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).
		Scan(&flags, &csh)
	require.NoError(t, err)
	require.NotZero(t, flags&int16(1))
	require.Equal(t, int64(200)+int64(store.settings.ChainCfgParams.CoinbaseMaturity), csh)
}

func TestCreateWithConflictingAndLockedFlags(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 70_000)

	md, err := store.Create(context.Background(), tx, 100, utxo.WithConflicting(true), utxo.WithLocked(true))
	require.NoError(t, err)
	require.True(t, md.Conflicting)
	require.True(t, md.Locked)

	var flags int16
	err = store.pool.QueryRow(context.Background(),
		`SELECT flags FROM packed_txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&flags)
	require.NoError(t, err)
	require.NotZero(t, flags&flagConflicting)
	require.NotZero(t, flags&flagLocked)
}
