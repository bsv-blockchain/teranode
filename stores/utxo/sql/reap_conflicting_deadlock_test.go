package sql

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// The reap locks the conflicting record's row and then deletes its outputs, the
// opposite order to Spend and Unspend. When Postgres picks the reap as the
// deadlock victim it must try again, not leave the record for a later block.
// The test holds an output row of the loser, waits for the reap to block on it,
// then asks for the loser's row. Its own deadlock_timeout is long, so the reap's
// shorter one fires first and the reap is the victim.
func TestReapConflictingRetriesAfterBeingChosenAsADeadlockVictim(t *testing.T) {
	parallelPostgres(t)

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = true

	parent := newTwoOutputParent(t, 9)
	loser := newSpenderOf(t, parent, 9, 0)

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)
	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)
	_, err = store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	var before int
	require.NoError(t, store.db.QueryRowContext(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&before))

	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	_, err = holder.ExecContext(ctx, `SET LOCAL deadlock_timeout = '30s'`)
	require.NoError(t, err)
	_, err = holder.ExecContext(ctx, `UPDATE outputs SET satoshis = satoshis WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, loser.TxIDChainHash()[:])
	require.NoError(t, err)

	type result struct {
		reaped bool
		err    error
	}

	done := make(chan result, 1)

	go func() {
		reaped, reapErr := store.reapConflicting(ctx, *loser.TxIDChainHash(), store.GetBlockHeight()+1000, store.settings.GlobalBlockHeightRetention)
		done <- result{reaped, reapErr}
	}()

	// Wait until the reap is blocked on the loser's output row.
	require.Eventually(t, func() bool {
		var waiting int
		_ = store.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database() AND pid <> pg_backend_pid()`).Scan(&waiting)

		return waiting > 0
	}, 20*time.Second, 20*time.Millisecond)

	// Closing the cycle: the reap holds the loser's row, and this transaction now wants it.
	_, err = holder.ExecContext(ctx, `SELECT 1 FROM transactions WHERE hash = $1 FOR UPDATE`, loser.TxIDChainHash()[:])
	require.NoError(t, err, "the reap, not the holder, must be the deadlock victim")
	require.NoError(t, holder.Rollback())

	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.True(t, res.reaped, "the reap must try again after the deadlock")
	case <-time.After(30 * time.Second):
		t.Fatal("the reap did not finish")
	}

	var after int
	require.NoError(t, store.db.QueryRowContext(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&after))
	require.Greater(t, after, before, "the test must have caused a deadlock")

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.Error(t, err, "the loser is gone")
}
