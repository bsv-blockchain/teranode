package sql

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
	"github.com/stretchr/testify/require"
)

// TestPrunerRetriesSerializationFailureOnDelete_Postgres drives the SQL pruner
// through a real Postgres serialization failure raised by its own DELETE and
// asserts the store's end state: the tombstoned child is gone, its replay
// marker sits on the surviving parent, and the retry fired exactly once.
//
// The pruning transaction is SERIALIZABLE, so its snapshot is taken at its
// first statement, the marker INSERT. The interleaving is:
//
//  1. holder: BEGIN; INSERT the exact (parent_id, child_hash) marker the pruner
//     is about to write, and hold it uncommitted. The pruner's
//     INSERT ... ON CONFLICT has to wait on that row to learn whether it
//     conflicts, and it takes its snapshot before it starts waiting.
//  2. Prune starts and blocks there (observed through pg_stat_activity).
//  3. updater, autocommit: UPDATE the child row, committed after the pruner's
//     snapshot.
//  4. holder: ROLLBACK. The pruner's INSERT wakes, finds no conflict, writes the
//     marker, and its DELETE then reaches a child row with a newer committed
//     version than its snapshot. Under REPEATABLE READ or SERIALIZABLE that is
//     ERROR 40001 "could not serialize access due to concurrent update", raised
//     by the DELETE statement.
//  5. deleteTombstoned must see the *pgconn.PgError through the attempt's
//     error, wait, and re-run the whole transaction, which now succeeds.
//
// This is the regression guard for the wrapping inside one attempt: if any
// statement site goes back to errors.NewStorageError, the driver error is
// replaced by a bare *errors.Error carrying only its message, isPruneRetryable
// never sees the code, and Prune returns after one attempt with the child
// still in the store.
func TestPrunerRetriesSerializationFailureOnDelete_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)

	childHash := child.TxIDChainHash()[:]

	// Tombstone the child by hand, well below the prune height. The parent
	// still has an unspent output, so it is not a candidate.
	_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1100 WHERE hash = $1", childHash)
	require.NoError(t, err)

	var parentID int64
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", parent.TxIDChainHash()[:]).Scan(&parentID))

	// Step 1: hold the pruner's own marker row uncommitted.
	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	released := false
	t.Cleanup(func() {
		if !released {
			_ = holder.Rollback()
		}
	})
	_, err = holder.ExecContext(ctx, "INSERT INTO deleted_children (parent_id, child_hash) VALUES ($1, $2)", parentID, childHash)
	require.NoError(t, err)

	var (
		retries     atomic.Int32
		conflictErr atomic.Value // string: the error the retry warning carried
	)
	logger := &sqlpruner.MockLogger{
		WarnfFunc: func(format string, args ...interface{}) {
			if !strings.Contains(format, "retrying") {
				return
			}
			retries.Add(1)
			if len(args) > 0 {
				conflictErr.Store(fmt.Sprint(args[len(args)-1]))
			}
		},
	}
	svc, err := sqlpruner.NewService(store.settings, sqlpruner.Options{Logger: logger, DB: store.db, Engine: store.engine})
	require.NoError(t, err)

	type pruneResult struct {
		n   int64
		err error
	}
	done := make(chan pruneResult, 1)
	go func() {
		n, err := svc.Prune(ctx, 1300, "serialization-retry")
		done <- pruneResult{n: n, err: err}
	}()

	// Step 2: the pruner's marker INSERT has taken its snapshot and is waiting
	// on the holder's transaction.
	require.Eventually(t, func() bool {
		var waiting int
		err := store.db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO deleted_children%'`,
		).Scan(&waiting)
		return err == nil && waiting == 1
	}, 10*time.Second, 20*time.Millisecond, "the pruner's marker INSERT never blocked on the held marker row")

	// Step 3: a committed write to the child row, after the pruner's snapshot.
	_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1150 WHERE hash = $1", childHash)
	require.NoError(t, err)

	// Step 4: release the pruner; its DELETE now raises 40001.
	released = true
	require.NoError(t, holder.Rollback())

	var res pruneResult
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Prune did not return")
	}

	// End state in the store.
	var childExists bool
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", childHash).Scan(&childExists))
	require.False(t, childExists,
		"the tombstoned child must be pruned once the serialization failure on the DELETE is retried (Prune returned n=%d err=%v)", res.n, res.err)

	var markers int
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT count(*) FROM deleted_children WHERE parent_id = $1 AND child_hash = $2", parentID, childHash).Scan(&markers))
	require.Equal(t, 1, markers, "the replay marker must be on the parent")

	var parentExists bool
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE id = $1)", parentID).Scan(&parentExists))
	require.True(t, parentExists, "the parent is not tombstoned and must survive")

	require.NoError(t, res.err)
	require.Equal(t, int64(1), res.n)
	require.Equal(t, int32(1), retries.Load(), "exactly one serialization conflict must have been retried")

	// The conflict must have been the DELETE's 40001, seen through the attempt's
	// wrapper, not something raised earlier in the transaction.
	retried, _ := conflictErr.Load().(string)
	require.Contains(t, retried, "failed to delete transactions")
	require.Contains(t, retried, "40001")
}
