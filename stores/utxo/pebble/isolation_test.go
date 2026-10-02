package pebble

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestTornDeleteReportsNotFound pins the second half of icellan finding 4. A
// master present with its payload gone is the shape an interleaving Delete or
// Prune leaves behind. Get used to answer STORAGE_ERROR, which is not
// ErrTxNotFound, so the validator's DAH-evicted-parent branch never fired and a
// valid transaction was rejected as a hard store failure.
func TestTornDeleteReportsNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	tx := newExtendedTx(t, 2, 91_000)
	mustCreate(t, store, tx, 100)

	// Remove only the payload, leaving the master behind.
	require.NoError(t, store.deleteDirect(payloadKey(tx.TxIDChainHash()[:])))

	_, err := store.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"a record whose payload is gone must read as not found, got %v", err)

	// The same must hold on the iterator path, which also reads the payload.
	_, err = store.Get(ctx, tx.TxIDChainHash(), fields.TxInpoints)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)
}

// TestReadsAreIsolatedFromConcurrentSpend pins the first half of finding 4. One
// Spend writes the page-0 slot (inside the master record) and the overflow page
// record in a single atomic batch. A reader assembling those from separate
// database reads could observe one without the other and return a spend state
// that never existed — the state GetCounterConflictingTxHashes builds its
// parent set from, so a double spend became invisible to the conflict
// machinery.
func TestReadsAreIsolatedFromConcurrentSpend(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	tx := newExtendedTx(t, int(store.pageSize)+2, 92_000)
	mustCreate(t, store, tx, 100)

	pageVout := store.pageSize + 1

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
	)

	// Writer: spend both slots in one batch, then undo, over and over.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			spends, err := store.Spend(ctx, newSpendingTx(t, tx, 0, pageVout), 101)
			if err != nil {
				continue
			}

			_ = store.Unspend(ctx, spends)
		}
	}()

	// Reader: the two slots were written by one batch, so a snapshot must show
	// both or neither. Anything else is a torn read.
	for i := 0; i < 3000; i++ {
		md, err := store.Get(ctx, tx.TxIDChainHash(), fields.Utxos)
		require.NoError(t, err)

		page0Spent := md.SpendingDatas[0] != nil
		pageNSpent := md.SpendingDatas[pageVout] != nil

		require.Equal(t, page0Spent, pageNSpent,
			"torn read at iteration %d: page-0 spent=%v, overflow page spent=%v", i, page0Spent, pageNSpent)
	}

	close(stop)
	wg.Wait()
}
