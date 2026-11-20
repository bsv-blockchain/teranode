package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

func TestCanceledSpendBatchKeepsHistoricalMatch_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}
	store, ctx := setupPostgresStore(t)
	a := replayParent(t, ctx, store, strings.Repeat("11", 32))
	b := replayParent(t, ctx, store, strings.Repeat("22", 32))
	child := replaySpender(t, b, a)
	recorded := spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes()
	_, err := store.db.ExecContext(ctx, "UPDATE outputs SET spending_data = $2, spending_height = 1000 WHERE transaction_id IN (SELECT id FROM transactions WHERE hash = $1)", b.TxIDChainHash()[:], recorded)
	require.NoError(t, err)
	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.ExecContext(ctx, "SELECT id FROM transactions WHERE hash = $1 FOR NO KEY UPDATE", a.TxIDChainHash()[:])
	require.NoError(t, err)
	store.settings.UtxoStore.SpendWaitTimeout = time.Minute
	store.spendBatcher.Close()
	store.spendBatcher = batcher.NewWithPool(2, time.Minute, func(batch []*batchSpend) {
		// B is answered by an earlier transaction; A's later transaction is
		// canceled while waiting on pruning, before its final marker check.
		for _, parent := range []*bt.Tx{b, a} {
			for _, item := range batch {
				if item.spend.TxID.IsEqual(parent.TxIDChainHash()) {
					store.sendSpendBatch([]*batchSpend{item})
				}
			}
		}
	}, false)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	callDone := make(chan error, 1)
	go func() {
		_, err := store.Spend(callCtx, child, 1000)
		callDone <- err
	}()
	require.Eventually(t, func() bool {
		var waiting int
		err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'").Scan(&waiting)
		return err == nil && waiting > 0
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-callDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled spend did not return")
	}
	require.Equal(t, recorded, outputSpendingData(t, ctx, store, b, 0), "an interrupted marker check must not release an earlier historical match")
	require.Empty(t, outputSpendingData(t, ctx, store, a, 0))
}

// A pruning candidate holds the same parent row lock as holder below. A spend
// must not write its inputs after its caller has already abandoned the call.
func TestSpendDoesNotCommitAfterAbandoningRowLockWait_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}
	for _, bulk := range []bool{false, true} {
		for _, cancelCaller := range []bool{false, true} {
			t.Run(fmt.Sprintf("bulk_%t/cancel_%t", bulk, cancelCaller), func(t *testing.T) {
				store, ctx := setupPostgresStore(t)
				store.settings.UtxoStore.BatchSQLOperations = bulk
				store.settings.UtxoStore.SpendWaitTimeout = time.Second
				if cancelCaller {
					store.settings.UtxoStore.SpendWaitTimeout = time.Minute
				}
				parent := bt.NewTx()
				require.NoError(t, parent.From(strings.Repeat("11", 32), 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				for range 2 {
					require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
				}
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)
				child := bt.NewTx()
				for i, output := range parent.Outputs {
					require.NoError(t, child.From(parent.TxID(), uint32(i), output.LockingScript.String(), output.Satoshis))
					child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				}
				require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
				holder, err := store.db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = holder.Rollback() }()
				_, err = holder.ExecContext(ctx, "SELECT id FROM transactions WHERE hash = $1 FOR NO KEY UPDATE", parent.TxIDChainHash()[:])
				require.NoError(t, err)

				callbackDone := make(chan struct{})
				store.spendBatcher.Close()
				store.spendBatcher = batcher.NewWithPool(2, 10*time.Millisecond, func(batch []*batchSpend) {
					defer close(callbackDone)
					store.sendSpendBatch(batch)
				}, false)
				callCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				callDone := make(chan error, 1)
				go func() {
					_, _, err := store.SpendAndCreate(callCtx, child, 1000)
					callDone <- err
				}()
				require.Eventually(t, func() bool {
					var waiting int
					err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'").Scan(&waiting)
					return err == nil && waiting > 0
				}, 5*time.Second, 10*time.Millisecond, "the spend must reach the parent lock held by pruning")
				if cancelCaller {
					cancel()
				}
				select {
				case err := <-callDone:
					require.Error(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("an abandoned spend must finish while the pruning lock is still held")
				}
				require.NoError(t, holder.Rollback())
				select {
				case <-callbackDone:
				case <-time.After(5 * time.Second):
					t.Fatal("spend callback did not settle")
				}
				for i := range parent.Outputs {
					require.Empty(t, outputSpendingData(t, ctx, store, parent, uint32(i)), "no write may arrive after the failed caller returned")
				}
				var exists bool
				require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&exists))
				require.False(t, exists, "the failed spend must not create its child")
			})
		}
	}
}
