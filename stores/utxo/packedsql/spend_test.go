package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func newSpendingTx(t testing.TB, parent *bt.Tx, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for _, vout := range vouts {
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      parent.Outputs[vout].Satoshis,
		}))
	}

	for i := range tx.Inputs {
		tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

func setFlags(t *testing.T, store *Store, tx *bt.Tx, flags int16) {
	t.Helper()

	_, err := store.pool.Exec(context.Background(),
		`UPDATE packed_txs SET flags = flags | $2 WHERE hash = $1`, tx.TxIDChainHash()[:], flags)
	require.NoError(t, err)
}

func slotSpend(t *testing.T, store *Store, tx *bt.Tx, vout uint32) []byte {
	t.Helper()

	page := pageOfVout(vout, store.pageSize)
	slot := slotOfVout(vout, store.pageSize)

	var b []byte

	var err error
	if page == 0 {
		err = store.pool.QueryRow(context.Background(),
			`SELECT substring(spends FROM $2 FOR 36) FROM packed_txs WHERE hash = $1`,
			tx.TxIDChainHash()[:], int(slot)*slotSpendSize+1).Scan(&b)
	} else {
		err = store.pool.QueryRow(context.Background(),
			`SELECT substring(spends FROM $3 FOR 36) FROM packed_tx_pages WHERE hash = $1 AND page = $2`,
			tx.TxIDChainHash()[:], page, int(slot)*slotSpendSize+1).Scan(&b)
	}

	require.NoError(t, err)

	return b
}

func TestSpendTwoOutputsOneParent(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 2, 200_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	spender := newSpendingTx(t, parent, 0, 1)

	spends, err := store.Spend(context.Background(), spender, 101)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
	}

	require.NotEqual(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))
	require.NotEqual(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 1))

	_, _, spentCount, _, _, _ := rowCounts(t, store, parent)
	require.Equal(t, 2, spentCount)
}

func TestSpendSetsDAHWhenFullySpentAndMined(t *testing.T) {
	store := newTestStore(t)

	mined := newExtendedTx(t, 1, 210_000)
	_, err := store.Create(context.Background(), mined, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	unmined := newExtendedTx(t, 1, 211_000)
	_, err = store.Create(context.Background(), unmined, 100)
	require.NoError(t, err)

	_, err = store.Spend(context.Background(), newSpendingTx(t, mined, 0), 101)
	require.NoError(t, err)

	_, err = store.Spend(context.Background(), newSpendingTx(t, unmined, 0), 101)
	require.NoError(t, err)

	var minedDAH *int64

	var unminedDAH *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, mined.TxIDChainHash()[:]).Scan(&minedDAH))
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT delete_at_height FROM packed_txs WHERE hash = $1`, unmined.TxIDChainHash()[:]).Scan(&unminedDAH))

	retention := int64(store.settings.GetUtxoStoreBlockHeightRetention())
	require.NotNil(t, minedDAH)
	require.Equal(t, 101+retention, *minedDAH)
	require.Nil(t, unminedDAH)
}

func TestDoubleSpendReturnsUtxoSpent(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 220_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	spenderA := newSpendingTx(t, parent, 0)
	_, err = store.Spend(context.Background(), spenderA, 101)
	require.NoError(t, err)

	spenderB := bt.NewTx()
	require.NoError(t, spenderB.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	spenderB.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, spenderB.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 999))

	spends, err := store.Spend(context.Background(), spenderB, 101)
	require.Error(t, err)
	require.Len(t, spends, 1)
	require.Error(t, spends[0].Err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrSpent))
	require.NotNil(t, spends[0].ConflictingTxID)
	require.Equal(t, spenderA.TxID(), spends[0].ConflictingTxID.String())
}

func TestSpendIdempotent(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 230_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	spender := newSpendingTx(t, parent, 0)

	_, err = store.Spend(context.Background(), spender, 101)
	require.NoError(t, err)

	spends, err := store.Spend(context.Background(), spender, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

func TestSpendFlagGuards(t *testing.T) {
	cases := []struct {
		name    string
		flag    int16
		wantErr error
		ignore  utxo.IgnoreFlags
		ignored bool
	}{
		{"frozen", flagFrozen, errors.ErrFrozen, utxo.IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true}, false},
		{"conflicting", flagConflicting, errors.ErrTxConflicting, utxo.IgnoreFlags{IgnoreConflicting: true}, true},
		{"locked", flagLocked, errors.ErrTxLocked, utxo.IgnoreFlags{IgnoreLocked: true}, true},
	}

	store := newTestStore(t)

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := newExtendedTx(t, 1, 240_000+uint64(i)*1000)

			_, err := store.Create(context.Background(), parent, 100)
			require.NoError(t, err)

			setFlags(t, store, parent, tc.flag)

			spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
			require.Error(t, err)
			require.True(t, errors.Is(spends[0].Err, tc.wantErr), "got %v", spends[0].Err)

			spends, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101, tc.ignore)
			if tc.ignored {
				require.NoError(t, err)
				require.NoError(t, spends[0].Err)
			} else {
				require.Error(t, err)
				require.True(t, errors.Is(spends[0].Err, errors.ErrFrozen))
			}
		})
	}
}

func TestSpendCoinbaseImmature(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 250_000)

	_, err := store.Create(context.Background(), parent, 200, utxo.WithSetCoinbase(true))
	require.NoError(t, err)

	maturity := uint32(store.settings.ChainCfgParams.CoinbaseMaturity)

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 200+maturity-1)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxCoinbaseImmature), "got %v", spends[0].Err)

	spends, err = store.Spend(context.Background(), newSpendingTx(t, parent, 0), 200+maturity)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

func TestSpendUtxoHashMismatch(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 2, 260_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	spender := bt.NewTx()
	require.NoError(t, spender.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[1].LockingScript,
		Satoshis:      parent.Outputs[1].Satoshis + 42,
	}))
	spender.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 999))

	spends, err := store.Spend(context.Background(), spender, 101)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrUtxoHashMismatch), "got %v", spends[0].Err)

	spends, err = store.Spend(context.Background(), spender, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

func TestSpendMultiPage(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, int(store.pageSize)+2, 270_000)

	_, err := store.Create(context.Background(), parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0, store.pageSize), 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
	require.NoError(t, spends[1].Err)

	var pageSpent int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT spent_count FROM packed_tx_pages WHERE hash = $1 AND page = 1`,
		parent.TxIDChainHash()[:]).Scan(&pageSpent))
	require.Equal(t, 1, pageSpent)

	remaining := make([]uint32, 0, store.pageSize+1)

	for v := uint32(1); v < store.pageSize; v++ {
		remaining = append(remaining, v)
	}

	remaining = append(remaining, store.pageSize+1)

	_, err = store.Spend(context.Background(), newSpendingTx(t, parent, remaining...), 101)
	require.NoError(t, err)

	var pagesSpent int

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT pages_spent, delete_at_height FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&pagesSpent, &dah))
	require.Equal(t, 1, pagesSpent)
	require.NotNil(t, dah)
}

func TestSpendNotFoundParent(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 280_000)

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0), 101)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxNotFound), "got %v", spends[0].Err)
}

func TestUnspendOwnershipFence(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 290_000)

	_, err := store.Create(context.Background(), parent, 100,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spenderA := newSpendingTx(t, parent, 0)

	spendsA, err := store.Spend(context.Background(), spenderA, 101)
	require.NoError(t, err)

	other := newExtendedTx(t, 1, 291_000)
	wrong := spendsA[0].Clone()
	wrong.SpendingData.TxID = other.TxIDChainHash()

	require.NoError(t, store.Unspend(context.Background(), []*utxo.Spend{wrong}))
	require.NotEqual(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))

	require.NoError(t, store.Unspend(context.Background(), spendsA))
	require.Equal(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))

	var spentCount int

	var dah *int64
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT spent_count, delete_at_height FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&spentCount, &dah))
	require.Equal(t, 0, spentCount)
	require.Nil(t, dah)

	spendsB, err := store.Spend(context.Background(), spenderA, 101)
	require.NoError(t, err)

	require.NoError(t, store.Unspend(context.Background(), spendsB, true))

	var flags int16
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT flags FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&flags))
	require.NotZero(t, flags&flagLocked)
}
