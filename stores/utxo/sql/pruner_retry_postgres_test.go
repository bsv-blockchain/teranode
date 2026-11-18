package sql

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
	"github.com/stretchr/testify/require"
)

// TestPrunerRetriesDeadlockOnMarkerInsert_Postgres drives the SQL pruner through a
// real Postgres deadlock that picks the pruning transaction as its victim, and
// asserts the store's end state: the tombstoned child is gone, its replay marker
// sits on the surviving parent, and the retry fired exactly once.
//
// The pruning transaction locks its candidate rows first, so the interleaving is:
//
//  1. holder: BEGIN; INSERT the exact (parent_id, child_hash) marker the pruner
//     is about to write, and hold it uncommitted. The pruner's
//     INSERT ... ON CONFLICT has to wait on that row to learn whether it
//     conflicts.
//  2. Prune starts, locks the child row, and blocks on the marker INSERT
//     (observed through pg_stat_activity).
//  3. holder: lock the child row. It waits for the pruner, which waits for the
//     holder: a lock cycle. The pruner has waited longer, so its deadlock check
//     fires first and it is the victim: ERROR 40P01 "deadlock detected", raised by
//     the marker INSERT.
//  4. deleteTombstoned must see the *pgconn.PgError through the attempt's
//     error, wait, and re-run the whole transaction. The holder then rolls back,
//     and the retry succeeds.
//
// This is the regression guard for the wrapping inside one attempt: if any
// statement site goes back to errors.NewStorageError, the driver error is
// replaced by a bare *errors.Error carrying only its message, isPruneRetryable
// never sees the code, and Prune returns after one attempt with the child
// still in the store.
func TestPrunerRetriesDeadlockOnMarkerInsert_Postgres(t *testing.T) {
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

	// Mined, because the pruner keeps an unmined record that is not conflicting.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
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
		n, err := svc.Prune(ctx, 1300, "deadlock-retry")
		done <- pruneResult{n: n, err: err}
	}()

	// Step 2: the pruner has locked its candidates and its marker INSERT is
	// waiting on the holder's transaction.
	require.Eventually(t, func() bool {
		var waiting int
		err := store.db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO deleted_children%'`,
		).Scan(&waiting)
		return err == nil && waiting == 1
	}, 10*time.Second, 20*time.Millisecond, "the pruner's marker INSERT never blocked on the held marker row")

	// Step 3: the holder asks for the child row the pruner holds. That closes the
	// cycle, and the pruner, which has waited longer, is the one Postgres aborts.
	holderLocked := make(chan error, 1)
	go func() {
		_, err := holder.ExecContext(ctx, "SELECT id FROM transactions WHERE hash = $1 FOR UPDATE", childHash)
		holderLocked <- err
	}()

	// Step 4: once the retry is logged the holder lets go, and the re-run succeeds.
	require.Eventually(t, func() bool { return retries.Load() >= 1 }, 30*time.Second, 20*time.Millisecond,
		"the pruner was never chosen as the deadlock victim")
	require.NoError(t, <-holderLocked, "the holder must not have been the victim")

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
		"the tombstoned child must be pruned once the deadlock is retried (Prune returned n=%d err=%v)", res.n, res.err)

	var markers int
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT count(*) FROM deleted_children WHERE parent_id = $1 AND child_hash = $2", parentID, childHash).Scan(&markers))
	require.Equal(t, 1, markers, "the replay marker must be on the parent")

	var parentExists bool
	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE id = $1)", parentID).Scan(&parentExists))
	require.True(t, parentExists, "the parent is not tombstoned and must survive")

	require.NoError(t, res.err)
	require.Equal(t, int64(1), res.n)
	require.Equal(t, int32(1), retries.Load(), "exactly one deadlock must have been retried")

	// The conflict must have been the marker INSERT's 40P01, seen through the
	// attempt's wrapper, not something raised earlier in the transaction.
	retried, _ := conflictErr.Load().(string)
	require.Contains(t, retried, "failed to mark pruned children")
	require.Contains(t, retried, "40P01")
}

// pruneConcurrencyFixture holds the records two candidates need:
//
//   - K spends Q:0 and is tombstoned. Q keeps an unspent output, so it survives and
//     K leaves a replay marker on it.
//   - P has one output, spent by C, and is tombstoned. C keeps an unspent output, so
//     it survives P, and a replay of C is an idempotent re-spend of P:0.
type pruneConcurrencyFixture struct {
	q, k, p, c *bt.Tx
	qID        int64
}

func seedPruneConcurrencyFixture(ctx context.Context, t *testing.T, store *Store) pruneConcurrencyFixture {
	t.Helper()

	const address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

	newParent := func(prev string, outputs int) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(prev, 0, "51", 30000))
		tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})

		for i := 0; i < outputs; i++ {
			require.NoError(t, tx.PayToAddress(address, 4000))
		}

		_, err := store.Create(ctx, tx, 1000)
		require.NoError(t, err)

		return tx
	}

	spendOf := func(parent *bt.Tx) *bt.Tx {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, child.PayToAddress(address, 3000))

		_, _, err := store.SpendAndCreate(ctx, child, 1000)
		require.NoError(t, err)

		return child
	}

	f := pruneConcurrencyFixture{}
	f.q = newParent("1111111111111111111111111111111111111111111111111111111111111111", 2)
	f.p = newParent("2222222222222222222222222222222222222222222222222222222222222222", 1)
	f.k = spendOf(f.q)
	f.c = spendOf(f.p)

	_, err := store.SetMinedMulti(ctx,
		[]*chainhash.Hash{f.q.TxIDChainHash(), f.p.TxIDChainHash(), f.k.TxIDChainHash(), f.c.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	// Tombstone both candidates well below the prune height.
	for _, h := range []*chainhash.Hash{f.k.TxIDChainHash(), f.p.TxIDChainHash()} {
		_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = 1100 WHERE hash = $1", h[:])
		require.NoError(t, err)
	}

	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", f.q.TxIDChainHash()[:]).Scan(&f.qID))

	return f
}

// pruneOutcome is the result of one background Prune.
type pruneOutcome struct {
	n   int64
	err error
}

func pruneInBackground(ctx context.Context, t *testing.T, store *Store, logger *sqlpruner.MockLogger) chan pruneOutcome {
	t.Helper()

	svc, err := sqlpruner.NewService(store.settings, sqlpruner.Options{Logger: logger, DB: store.db, Engine: store.engine})
	require.NoError(t, err)

	done := make(chan pruneOutcome, 1)

	go func() {
		n, err := svc.Prune(ctx, 1300, "prune-concurrency")
		done <- pruneOutcome{n: n, err: err}
	}()

	return done
}

func retryCounter(retries *atomic.Int32) *sqlpruner.MockLogger {
	return &sqlpruner.MockLogger{
		WarnfFunc: func(format string, _ ...interface{}) {
			if strings.Contains(format, "retrying") {
				retries.Add(1)
			}
		},
	}
}

// TestPruneIsNotAbortedByAReplayOfAChild_Postgres: any caller can resubmit a transaction
// that is already mined. The validator spends before it creates, so a replay of C, a
// child of the candidate P, is an idempotent re-spend of P:0 and runs setDAH on P, which
// moves P's stamp forward. Under SERIALIZABLE that committed update made the prune's
// DELETE fail with 40001 and rolled back the whole block's markers and deletions, so
// one replay per attempt kept the SQL pruner from completing a cycle.
//
// The prune now locks its candidates in the statement that selects them. The test
// holds the prune inside its marker INSERT (an uncommitted copy of the marker, as the
// retry test does), replays C, and requires that the replay waits on P's row instead of
// bumping it, that the prune completes with no retry, and that every candidate is
// deleted.
func TestPruneIsNotAbortedByAReplayOfAChild_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	f := seedPruneConcurrencyFixture(ctx, t, store)

	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	released := false

	t.Cleanup(func() {
		if !released {
			_ = holder.Rollback()
		}
	})

	_, err = holder.ExecContext(ctx, "INSERT INTO deleted_children (parent_id, child_hash) VALUES ($1, $2)", f.qID, f.k.TxIDChainHash()[:])
	require.NoError(t, err)

	var retries atomic.Int32

	pruned := pruneInBackground(ctx, t, store, retryCounter(&retries))

	require.Eventually(t, func() bool {
		var waiting int
		err := store.db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO deleted_children%'`,
		).Scan(&waiting)

		return err == nil && waiting == 1
	}, 10*time.Second, 20*time.Millisecond, "the pruner's marker INSERT never blocked on the held marker row")

	// The replay of C. Its setDAH on P must wait for the prune's lock on the row.
	replayed := make(chan error, 1)

	go func() {
		_, err := store.Spend(ctx, f.c, 1300)
		replayed <- err
	}()

	require.Eventually(t, func() bool {
		var waiting int
		err := store.db.QueryRowContext(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%FOR NO KEY UPDATE%'`,
		).Scan(&waiting)

		return err == nil && waiting == 1
	}, 10*time.Second, 20*time.Millisecond, "the replay's setDAH did not wait on the row the prune locked")

	released = true
	require.NoError(t, holder.Rollback())

	var res pruneOutcome

	select {
	case res = <-pruned:
	case <-time.After(30 * time.Second):
		t.Fatal("Prune did not return")
	}

	select {
	case replayErr := <-replayed:
		// The replay's parent was pruned under it, so it may return a storage error. What
		// matters is that it returns and did not abort the prune.
		t.Logf("the replay returned: %v", replayErr)
	case <-time.After(30 * time.Second):
		t.Fatal("the replay did not return")
	}

	require.NoError(t, res.err)
	require.Equal(t, int32(0), retries.Load(), "a replay must not abort the pruning transaction")
	require.Equal(t, int64(2), res.n, "both candidates must be deleted")
	require.False(t, rowExists(ctx, t, store, *f.k.TxIDChainHash()), "K must be pruned")
	require.False(t, rowExists(ctx, t, store, *f.p.TxIDChainHash()), "P must be pruned")
	require.True(t, rowExists(ctx, t, store, *f.c.TxIDChainHash()), "C is not a candidate")

	var markers int

	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM deleted_children WHERE parent_id = $1 AND child_hash = $2", f.qID, f.k.TxIDChainHash()[:]).Scan(&markers))
	require.Equal(t, 1, markers, "K's marker must sit on Q")
}

// TestPruneSkipsACandidateAnotherTransactionHolds_Postgres: a candidate row that another
// transaction holds is left for the next block. The prune neither waits for it nor fails,
// and the rows it did lock are deleted with their markers.
func TestPruneSkipsACandidateAnotherTransactionHolds_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	f := seedPruneConcurrencyFixture(ctx, t, store)

	holder, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	released := false

	t.Cleanup(func() {
		if !released {
			_ = holder.Rollback()
		}
	})

	_, err = holder.ExecContext(ctx, "SELECT id FROM transactions WHERE hash = $1 FOR NO KEY UPDATE", f.p.TxIDChainHash()[:])
	require.NoError(t, err)

	var retries atomic.Int32

	var res pruneOutcome

	select {
	case res = <-pruneInBackground(ctx, t, store, retryCounter(&retries)):
	case <-time.After(20 * time.Second):
		t.Fatal("the prune waited for a row it should have skipped")
	}

	require.NoError(t, res.err)
	require.Equal(t, int64(1), res.n, "only the unlocked candidate is deleted")
	require.Equal(t, int32(0), retries.Load())
	require.False(t, rowExists(ctx, t, store, *f.k.TxIDChainHash()), "K must be pruned")
	require.True(t, rowExists(ctx, t, store, *f.p.TxIDChainHash()), "P is held by another transaction and must be left alone")

	released = true
	require.NoError(t, holder.Rollback())

	select {
	case res = <-pruneInBackground(ctx, t, store, retryCounter(&retries)):
	case <-time.After(20 * time.Second):
		t.Fatal("the second prune did not return")
	}

	require.NoError(t, res.err)
	require.Equal(t, int64(1), res.n, "the skipped candidate is pruned once it is free")
	require.False(t, rowExists(ctx, t, store, *f.p.TxIDChainHash()), "P must be pruned the next block")
}

// TestPruneDropsItsCandidatesTableAtCommit_Postgres: the candidates table is dropped when
// the transaction commits, so no later statement on the connection finds it.
func TestPruneDropsItsCandidatesTableAtCommit_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	// One connection, so the check below runs on the connection the prune used.
	store.db.SetMaxOpenConns(1)

	seedPruneConcurrencyFixture(ctx, t, store)

	var retries atomic.Int32

	select {
	case res := <-pruneInBackground(ctx, t, store, retryCounter(&retries)):
		require.NoError(t, res.err)
		require.Equal(t, int64(2), res.n)
	case <-time.After(30 * time.Second):
		t.Fatal("Prune did not return")
	}

	var left bool

	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT to_regclass('pg_temp.utxo_prune_candidates') IS NOT NULL").Scan(&left))
	require.False(t, left, "the candidates table must not outlive the pruning transaction")
}
