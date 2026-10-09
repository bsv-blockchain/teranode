package pebble

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// openStoreAt opens (or reopens) a store rooted at the given data folder.
func openStoreAt(t testing.TB, folder string) *Store {
	t.Helper()

	tSettings := settings.NewSettings()
	tSettings.DataFolder = folder

	storeURL, err := url.Parse("pebble:///utxostore")
	require.NoError(t, err)

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// TestCloseDrainsInFlightOperations pins icellan finding 3. pebble panics on any
// use after Close, so closing the store while the validator is still inside a
// read or a commit used to take the process down. Close must drain instead, and
// later calls must return an error rather than panic.
func TestCloseDrainsInFlightOperations(t *testing.T) {
	store := openStoreAt(t, t.TempDir())
	require.NoError(t, store.SetBlockHeight(100))

	const workers = 16

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			<-start

			// Any of these panics on a closed pebble unless the guard holds.
			tx := newExtendedTx(t, 2, uint64(600_000+i*37))
			_, _ = store.Create(context.Background(), tx, 100)
			_, _ = store.Get(context.Background(), tx.TxIDChainHash(), fields.Utxos)
			_, _ = store.Spend(context.Background(), newSpendingTx(t, tx, 0), 101)
			_ = store.Delete(context.Background(), tx.TxIDChainHash())
		}(i)
	}

	close(start)

	// Close races the workers. It must not panic and must not return early.
	require.NoError(t, store.Close(context.Background()))

	wg.Wait()

	// Every path is now closed rather than panicking.
	tx := newExtendedTx(t, 1, 61_000)

	_, err := store.Create(context.Background(), tx, 100)
	require.Error(t, err)

	_, err = store.Get(context.Background(), tx.TxIDChainHash())
	require.Error(t, err)

	_, err = store.Spend(context.Background(), newSpendingTx(t, tx, 0), 101)
	require.Error(t, err)

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	_, err = it.Next(context.Background())
	require.Error(t, err)

	_, err = store.QueryOldUnminedTransactions(context.Background(), 1000)
	require.Error(t, err)

	// Close stays idempotent.
	require.NoError(t, store.Close(context.Background()))
}

// TestConcurrentStripedOperations drives the stripe locks from many goroutines
// at once. icellan noted nothing exercised the store's only isolation
// mechanism, so -race had nothing to observe.
func TestConcurrentStripedOperations(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	const workers = 24

	txs := make([]*bt2Tx, 0, workers)
	for i := 0; i < workers; i++ {
		txs = append(txs, &bt2Tx{tx: newExtendedTx(t, 3, uint64(700_000+i*101))})
	}

	var wg sync.WaitGroup

	start := make(chan struct{})

	for i := range txs {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			<-start

			tx := txs[i].tx

			_, err := store.Create(context.Background(), tx, 100)
			require.NoError(t, err)

			_, err = store.Spend(context.Background(), newSpendingTx(t, tx, 0), 101)
			require.NoError(t, err)

			_, err = store.Get(context.Background(), tx.TxIDChainHash(), fields.Utxos)
			require.NoError(t, err)
		}(i)
	}

	close(start)
	wg.Wait()

	// Every transaction must have exactly one spent slot and intact counters.
	for i := range txs {
		m, err := store.getMaster(txs[i].tx.TxIDChainHash())
		require.NoError(t, err)
		require.Equal(t, uint32(1), m.spentCount, "tx %d", i)
		require.Equal(t, uint32(3), m.page0Count, "tx %d", i)
	}
}

type bt2Tx struct{ tx *bt.Tx }

// TestCloseReopenPreservesState pins the durability gap icellan listed: pebble
// is the first backend whose state lives only on local disk between restarts,
// and the existing reopen test used an empty database.
func TestCloseReopenPreservesState(t *testing.T) {
	folder := t.TempDir()

	store := openStoreAt(t, folder)
	require.NoError(t, store.SetBlockHeight(100))

	// Real data: multi-page transaction, one spend on page 0 and one on a page
	// record, so the reopen has to read both back.
	parent := newExtendedTx(t, int(store.pageSize)+2, 81_000)
	mustCreate(t, store, parent, 100)

	pageVout := store.pageSize + 1

	spends, err := store.Spend(context.Background(), newSpendingTx(t, parent, 0, pageVout), 101)
	require.NoError(t, err)

	before, err := store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)

	require.NoError(t, store.Close(context.Background()))

	reopened := openStoreAt(t, folder)
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	md, err := reopened.Get(context.Background(), parent.TxIDChainHash(), fields.Tx, fields.Utxos)
	require.NoError(t, err)
	require.Equal(t, parent.TxID(), md.Tx.TxID())

	require.NotNil(t, md.SpendingDatas[0], "page-0 spend must survive the restart")
	require.NotNil(t, md.SpendingDatas[pageVout], "page-record spend must survive the restart")
	require.Nil(t, md.SpendingDatas[1], "an unspent output must stay unspent")

	after, err := reopened.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, before.spentCount, after.spentCount, "spentCount must survive the restart")
	require.Equal(t, before.pagesSpent, after.pagesSpent, "pagesSpent must survive the restart")
	require.Equal(t, before.page0Count, after.page0Count)

	// And the surviving spend must still be recognised as a double spend.
	_, err = reopened.Spend(context.Background(), newSpendingTx(t, parent, 0), 102)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent) || errors.Is(err, errors.ErrUtxoError), "got %v", err)

	require.NoError(t, reopened.Unspend(context.Background(), spends))
}

// TestSpendUnspendCounterDrift pins the counter-drift gap: spentCount and
// pagesSpent were never read back across an unspend / re-spend cycle.
func TestSpendUnspendCounterDrift(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	tx := newExtendedTx(t, int(store.pageSize)+2, 83_000)
	mustCreate(t, store, tx, 100)

	pageVout := store.pageSize + 1

	base, err := store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)

	for round := 0; round < 3; round++ {
		spends, err := store.Spend(context.Background(), newSpendingTx(t, tx, 0, pageVout), 101)
		require.NoError(t, err)

		require.NoError(t, store.Unspend(context.Background(), spends))

		m, err := store.getMaster(tx.TxIDChainHash())
		require.NoError(t, err)

		require.Equal(t, base.spentCount, m.spentCount, "spentCount drifted after round %d", round)
		require.Equal(t, base.pagesSpent, m.pagesSpent, "pagesSpent drifted after round %d", round)
	}

	// Also confirm the counters land where they should as a page completes. The
	// overflow page holds both remaining outputs, so it is complete only once
	// both are spent, not on the first of them.
	_, err = store.Spend(context.Background(), newSpendingTx(t, tx, pageVout), 101)
	require.NoError(t, err)

	m, err := store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.pagesSpent, "the overflow page still has an unspent output")

	_, err = store.Spend(context.Background(), newSpendingTx(t, tx, store.pageSize), 101)
	require.NoError(t, err)

	m, err = store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, m.pagesTotal, m.pagesSpent, "the overflow page is now complete")
	require.Equal(t, uint32(1), m.pagesSpent)
}
