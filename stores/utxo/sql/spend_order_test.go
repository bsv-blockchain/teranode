package sql

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// A SQLite shared-cache lock cycle is retried like a Postgres deadlock. The
// message is the one the per-item spend got in Test_handleMultipleTx.
func TestIsDeadlockRecognisesSQLiteTableLocks(t *testing.T) {
	require.True(t, isDeadlock(errors.NewStorageError("database table is locked: database is deadlocked (6)")))
	require.True(t, isDeadlock(errors.NewStorageError("database is locked (5) (SQLITE_BUSY)")))
	require.False(t, isDeadlock(errors.NewStorageError("UNIQUE constraint failed: transactions.hash")))
	require.False(t, isDeadlock(nil))
}

// The bulk spend UPDATE must take its rows in (transaction_id, idx) order, so two
// batches over overlapping rows lock them in the same order. First arrival
// still wins a double spend: sorting happens after the dedup.
func TestSortSpendUpdateRows(t *testing.T) {
	rows := []spendUpdateRow{
		{batchIdx: 0, transactionID: 9, vout: 1},
		{batchIdx: 1, transactionID: 3, vout: 7},
		{batchIdx: 2, transactionID: 9, vout: 0},
		{batchIdx: 3, transactionID: 3, vout: 2},
		{batchIdx: 4, transactionID: 5, vout: 0},
	}

	sortSpendUpdateRows(rows)

	got := make([][2]int, len(rows))
	for i, r := range rows {
		got[i] = [2]int{r.transactionID, int(r.vout)}
	}

	require.Equal(t, [][2]int{{3, 2}, {3, 7}, {5, 0}, {9, 0}, {9, 1}}, got)
}

// Design test 14: two concurrent bulk spend batches over the same outputs, in
// opposite arrival order, must both complete; a deadlock, if Postgres still
// picks one, is absorbed by the retry. It proves completion only, not that the
// sort prevents deadlocks: it passes either way, and the sort's effect shows
// only in the "[Spend] deadlock detected" retry warnings.
func TestSpendBatchesInOppositeOrderBothComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true

	const outputs = 200

	for round := 0; round < 5; round++ {
		parent := bt.NewTx()
		require.NoError(t, parent.From(fmt.Sprintf("%064x", round+1), 0, "76a914000000000000000000000000000000000000000088ac", outputs*1000+1000))
		parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

		for i := 0; i < outputs; i++ {
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
		}

		_, err := store.Create(ctx, parent, 10)
		require.NoError(t, err)

		build := func(reverse bool) []*batchSpend {
			batch := make([]*batchSpend, outputs)

			for i := 0; i < outputs; i++ {
				vout := uint32(i) //nolint:gosec // test data
				if reverse {
					vout = uint32(outputs - 1 - i) //nolint:gosec // test data
				}

				h, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
				require.NoError(t, err)

				// Both batches carry the same spender, so both must succeed: the
				// second applies as an idempotent re-spend.
				batch[i] = &batchSpend{
					spend:        &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: vout, UTXOHash: h, SpendingData: spend.NewSpendingData(parent.TxIDChainHash(), int(vout))},
					blockHeight:  11,
					errCh:        make(chan error, 1),
					ignoreLocked: true,
				}
			}

			return batch
		}

		a, b := build(false), build(true)

		var wg sync.WaitGroup

		for _, batch := range [][]*batchSpend{a, b} {
			wg.Add(1)

			go func(batch []*batchSpend) {
				defer wg.Done()
				store.sendSpendBatch(batch)
			}(batch)
		}

		wg.Wait()

		for _, batch := range [][]*batchSpend{a, b} {
			for _, item := range batch {
				require.NoError(t, <-item.errCh)
			}
		}
	}
}
