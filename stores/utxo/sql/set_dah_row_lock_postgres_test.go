package sql

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const pgLockNotAvailable = "55P03"

// Delete and mining must lock the transaction before its block references.
// The short deadlock timeout on Delete makes its failure deterministic when
// the old opposite order forms a cycle; mining cannot hide it with a retry.
func TestDeleteAndMiningUseTheSameRowLockOrder_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}
	for _, unset := range []bool{false, true} {
		t.Run(fmt.Sprintf("unset_%t", unset), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			dbURL := startPostgresContainer(t, ctx)
			store, miner := newPostgresTestStore(t, ctx, dbURL), newPostgresTestStore(t, ctx, dbURL)
			t.Cleanup(func() { _ = store.Close(context.Background()); _ = miner.Close(context.Background()) })
			tx := replayParent(t, ctx, store, "1111111111111111111111111111111111111111111111111111111111111111")
			block := utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true}
			_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, block)
			require.NoError(t, err)
			// Each store now has one connection, so these session settings apply
			// to its actual Delete or SetMinedMulti transaction.
			store.db.SetMaxOpenConns(1)
			store.db.SetMaxIdleConns(1)
			miner.db.SetMaxOpenConns(1)
			miner.db.SetMaxIdleConns(1)
			_, err = store.db.ExecContext(ctx, "SET deadlock_timeout = '100ms'; SET application_name = 'delete-order-delete'")
			require.NoError(t, err)
			_, err = miner.db.ExecContext(ctx, "SET deadlock_timeout = '10s'; SET application_name = 'delete-order-miner'")
			require.NoError(t, err)
			control, err := pgx.Connect(ctx, dbURL.String())
			require.NoError(t, err)
			defer func() { _ = control.Close(context.Background()) }()
			holder, err := control.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = holder.Rollback(context.Background()) }()
			_, err = holder.Exec(ctx, "DELETE FROM block_ids WHERE transaction_id IN (SELECT id FROM transactions WHERE hash = $1)", tx.TxIDChainHash()[:])
			require.NoError(t, err)
			observer, err := pgx.Connect(ctx, dbURL.String())
			require.NoError(t, err)
			defer func() { _ = observer.Close(context.Background()) }()
			waitForLock := func(name string) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					var waiting bool
					err := observer.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock')", name).Scan(&waiting)
					require.NoError(t, err)
					if waiting {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatalf("%s must be waiting on the held row", name)
			}
			deleted, mined := make(chan error, 1), make(chan error, 1)
			go func() { deleted <- store.Delete(ctx, tx.TxIDChainHash()) }()
			waitForLock("delete-order-delete")
			block.UnsetMined = unset
			go func() { _, err := miner.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, block); mined <- err }()
			waitForLock("delete-order-miner")
			require.NoError(t, holder.Rollback(ctx))
			require.NoError(t, <-deleted, "Delete must not become the deadlock victim of mining")
			err = <-mined
			if unset {
				require.NoError(t, err, "unsetting a deleted transaction is a no-op")
			} else {
				require.ErrorIs(t, err, errors.ErrTxNotFound, "mining must see the row Delete removed")
			}
			require.False(t, rowExists(ctx, t, store, *tx.TxIDChainHash()))
		})
	}
}

// Delete must lock outputs before the transaction row, as Spend and Unspend do.
// The spend here has locked P's output and then asks setDAH for P's row while
// Delete runs. With Delete taking the row first, the two form a cycle and the
// short deadlock timeout makes Delete the victim; Delete does not retry.
func TestDeleteAndSpendUseTheSameRowLockOrder_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	dbURL := startPostgresContainer(t, ctx)
	store, spender := newPostgresTestStore(t, ctx, dbURL), newPostgresTestStore(t, ctx, dbURL)
	t.Cleanup(func() { _ = store.Close(context.Background()); _ = spender.Close(context.Background()) })
	floorRetention(t, spender)

	tx := replayParent(t, ctx, store, "2222222222222222222222222222222222222222222222222222222222222222")

	// One connection each, so the session settings apply to the real Delete.
	store.db.SetMaxOpenConns(1)
	store.db.SetMaxIdleConns(1)
	_, err := store.db.ExecContext(ctx, "SET deadlock_timeout = '100ms'; SET application_name = 'delete-spend-delete'")
	require.NoError(t, err)

	spend, err := spender.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = spend.Rollback() }()

	_, err = spend.ExecContext(ctx, "SET LOCAL deadlock_timeout = '10s'")
	require.NoError(t, err)

	var id int
	require.NoError(t, spend.QueryRowContext(ctx,
		"UPDATE outputs SET spending_data = $2 WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = 0 RETURNING transaction_id",
		tx.TxIDChainHash()[:], bytes.Repeat([]byte{0x42}, 36)).Scan(&id))

	observer, err := pgx.Connect(ctx, dbURL.String())
	require.NoError(t, err)

	defer func() { _ = observer.Close(context.Background()) }()

	deleted := make(chan error, 1)

	go func() { deleted <- store.Delete(ctx, tx.TxIDChainHash()) }()

	deadline := time.Now().Add(5 * time.Second)

	for {
		var waiting bool

		require.NoError(t, observer.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name = 'delete-spend-delete' AND wait_event_type = 'Lock')").Scan(&waiting))

		if waiting {
			break
		}

		require.True(t, time.Now().Before(deadline), "Delete must wait on the spend's output lock")
		time.Sleep(10 * time.Millisecond)
	}

	require.NoError(t, spender.setDAH(ctx, spend, id), "the spend must get the row lock Delete has not taken")
	require.NoError(t, spend.Commit())
	require.NoError(t, <-deleted, "Delete must not become the deadlock victim of a spend")
	require.False(t, rowExists(ctx, t, store, *tx.TxIDChainHash()))
}

// The pruner locks a candidate's row and then cascades into its outputs, the
// opposite of Delete's order. The holder here does the same on one row. Delete's
// short deadlock timeout makes it the victim, so it must retry and still delete.
func TestDeleteRetriesADeadlockWithThePrune_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	dbURL := startPostgresContainer(t, ctx)
	store := newPostgresTestStore(t, ctx, dbURL)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	tx := replayParent(t, ctx, store, "3333333333333333333333333333333333333333333333333333333333333333")

	store.db.SetMaxOpenConns(1)
	store.db.SetMaxIdleConns(1)
	_, err := store.db.ExecContext(ctx, "SET deadlock_timeout = '100ms'; SET application_name = 'delete-prune-delete'")
	require.NoError(t, err)

	control, err := pgx.Connect(ctx, dbURL.String())
	require.NoError(t, err)

	defer func() { _ = control.Close(context.Background()) }()

	prune, err := control.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = prune.Rollback(context.Background()) }()

	_, err = prune.Exec(ctx, "SET LOCAL deadlock_timeout = '10s'")
	require.NoError(t, err)

	var id int64
	require.NoError(t, prune.QueryRow(ctx, "SELECT id FROM transactions WHERE hash = $1 FOR NO KEY UPDATE", tx.TxIDChainHash()[:]).Scan(&id))

	observer, err := pgx.Connect(ctx, dbURL.String())
	require.NoError(t, err)

	defer func() { _ = observer.Close(context.Background()) }()

	deleted := make(chan error, 1)

	go func() { deleted <- store.Delete(ctx, tx.TxIDChainHash()) }()

	deadline := time.Now().Add(5 * time.Second)

	for {
		var waiting bool

		require.NoError(t, observer.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name = 'delete-prune-delete' AND wait_event_type = 'Lock')").Scan(&waiting))

		if waiting {
			break
		}

		require.True(t, time.Now().Before(deadline), "Delete must wait on the prune's row lock")
		time.Sleep(10 * time.Millisecond)
	}

	// The prune's cascade into outputs now waits on Delete: a cycle Postgres breaks
	// by aborting Delete, whose timeout is shorter.
	_, err = prune.Exec(ctx, "DELETE FROM outputs WHERE transaction_id = $1", id)
	require.NoError(t, err)
	require.NoError(t, prune.Rollback(ctx))

	require.NoError(t, <-deleted, "Delete must retry a deadlock with the prune")
	require.False(t, rowExists(ctx, t, store, *tx.TxIDChainHash()))
}

func pruneRowsAtHeight(ctx context.Context, t *testing.T, store *Store, height uint32) {
	t.Helper()

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	svc, err := provider.GetPrunerService()
	require.NoError(t, err)

	svc.Start(ctx)

	require.NoError(t, store.SetBlockHeight(height))

	_, err = svc.Prune(ctx, height, "<set-dah-row-lock>")
	require.NoError(t, err)
}

func rowExists(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) bool {
	t.Helper()

	var n int

	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE hash = $1", hash[:]).Scan(&n))

	return n > 0
}

// waitBlockedOrDone returns once a session waits on a lock or the writer has finished. A
// finished writer's result stays readable on done.
func waitBlockedOrDone(ctx context.Context, t *testing.T, store *Store, done chan error) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			done <- err

			return
		default:
		}

		var waiting int

		require.NoError(t, store.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting))

		if waiting > 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("writer neither blocked on a lock nor finished")
}

func pauseSetDAHOnce(t *testing.T) (reached chan struct{}, release func()) {
	t.Helper()

	reached = make(chan struct{})
	resume := make(chan struct{})

	var once, releaseOnce sync.Once

	setDAHBeforeWrite = func() {
		once.Do(func() {
			close(reached)
			<-resume
		})
	}

	release = func() { releaseOnce.Do(func() { close(resume) }) }

	t.Cleanup(func() {
		release()

		setDAHBeforeWrite = nil
	})

	return reached, release
}

// setDAH must hold the row lock from before its eligibility read to the end of the
// transaction, so SetConflicting(false) cannot commit a clear that a stale raise then
// overwrites on a row that is no longer conflicting.
func TestSetDAHHoldsRowLockWhileDeciding_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = $2 WHERE hash = $1",
		hash[:], int64(floorCachedTip)+1+retention)
	require.NoError(t, err)

	stale := readDAH(ctx, t, store, hash)
	require.NotNil(t, stale)
	require.Less(t, *stale, int64(floorMinedHeight)+retention, "setup: the stamp must sit below the mined floor")

	id := floorRowID(ctx, t, store, hash)

	txn, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = txn.Rollback() }()

	reached, release := pauseSetDAHOnce(t)

	setDAHDone := make(chan error, 1)

	go func() { setDAHDone <- store.setDAH(ctx, txn, id) }()

	select {
	case <-reached:
	case err := <-setDAHDone:
		t.Fatalf("setDAH returned before the write hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("setDAH did not reach the write hook")
	}

	_, err = store.db.ExecContext(ctx, "SELECT id FROM transactions WHERE id = $1 FOR UPDATE NOWAIT", id)
	require.Error(t, err, "the row must be locked while setDAH decides")

	var pgErr *pgconn.PgError

	require.True(t, errors.As(err, &pgErr), "unexpected error type: %v", err)
	require.Equal(t, pgLockNotAvailable, pgErr.Code)

	clearDone := make(chan error, 1)

	go func() {
		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, false)
		clearDone <- err
	}()

	waitBlockedOrDone(ctx, t, store, clearDone)

	release()

	require.NoError(t, <-setDAHDone)
	require.NoError(t, txn.Commit())
	require.NoError(t, <-clearDone)

	var (
		conflicting bool
		live        int
	)

	require.NoError(t, store.db.QueryRowContext(ctx, `
		SELECT conflicting, (SELECT count(*) FROM outputs WHERE transaction_id = transactions.id AND spending_data IS NULL)
		FROM transactions WHERE hash = $1`, hash[:]).Scan(&conflicting, &live))
	require.False(t, conflicting)
	require.Positive(t, live)
	require.Nil(t, readDAH(ctx, t, store, hash), "a row that is no longer conflicting must carry no stamp")

	pruneRowsAtHeight(ctx, t, store, floorMinedHeight+uint32(retention))
	require.True(t, rowExists(ctx, t, store, hash), "the pruner must not delete a live row")
}

// A spend that decides to stamp must see an Unspend that commits while it is paused:
// the Unspend's own setDAH waits on the row lock, then clears the stamp the spend wrote.
func TestSetDAHSeesConcurrentUnspend_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	require.GreaterOrEqual(t, len(tests.Tx.Outputs), 2)

	hash := createMinedAboveTip(ctx, t, store)

	spendOutput := func(vout int) []*utxo.Spend {
		out := tests.Tx.Outputs[vout]

		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(hash.String(), uint32(vout), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		spends, err := store.Spend(ctx, spendTx, floorCachedTip+1)
		require.NoError(t, err)

		return spends
	}

	for vout := 2; vout < len(tests.Tx.Outputs); vout++ {
		spendOutput(vout)
	}

	earlier := spendOutput(0)

	require.Nil(t, readDAH(ctx, t, store, hash), "setup: one output is still unspent")

	reached, release := pauseSetDAHOnce(t)

	id := floorRowID(ctx, t, store, hash)

	txn, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = txn.Rollback() }()

	_, err = txn.ExecContext(ctx, "UPDATE outputs SET spending_data = $3 WHERE transaction_id = $1 AND idx = $2",
		id, 1, bytes.Repeat([]byte{0x42}, 36))
	require.NoError(t, err)

	spendDone := make(chan error, 1)

	go func() {
		if err := store.setDAH(ctx, txn, id); err != nil {
			spendDone <- err
			return
		}

		spendDone <- txn.Commit()
	}()

	select {
	case <-reached:
	case err := <-spendDone:
		t.Fatalf("setDAH returned before the write hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("setDAH did not reach the write hook")
	}

	unspendDone := make(chan error, 1)

	go func() { unspendDone <- store.Unspend(ctx, earlier) }()

	waitBlockedOrDone(ctx, t, store, unspendDone)

	release()

	require.NoError(t, <-spendDone)
	require.NoError(t, <-unspendDone)

	var unspent int

	require.NoError(t, store.db.QueryRowContext(ctx, `
		SELECT count(*) FROM outputs o JOIN transactions t ON t.id = o.transaction_id
		WHERE t.hash = $1 AND o.idx = 0 AND o.spending_data IS NULL`, hash[:]).Scan(&unspent))
	require.Equal(t, 1, unspent, "the earlier spend must be undone")
	require.Nil(t, readDAH(ctx, t, store, hash), "a row with an unspent output must carry no stamp")

	pruneRowsAtHeight(ctx, t, store, floorMinedHeight+uint32(retention))
	require.True(t, rowExists(ctx, t, store, hash), "the pruner must not delete a row with a live output")
}

// SetMinedMulti must hold its row locks before it writes block_ids. The block_ids INSERT
// only takes FOR KEY SHARE on the parent through its foreign key, which setDAH's
// FOR NO KEY UPDATE does not conflict with. A spend of the parent's last output that ran
// setDAH inside SetMinedMulti's write window read the uncommitted block_ids as absent
// and wrote nothing, and SetMinedMulti's UPDATE then kept its own earlier view of the
// output as unspent, leaving a mined, fully spent parent with no stamp for good.
//
// The test runs the spend inside that window: SetMinedMulti pauses after its block_ids
// INSERT, the spend starts, and the spend is held after its eligibility read until
// SetMinedMulti's UPDATE has reached the row. With the lock the spend waits for
// SetMinedMulti to commit and then stamps; without it the stamp is lost.
func TestSetMinedMultiSerialisesWithSetDAHOnTheRow_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	_, err := store.Create(ctx, parent, floorCachedTip)
	require.NoError(t, err)

	hash := *parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	reached := make(chan struct{})
	resume := make(chan struct{})

	var once, releaseOnce sync.Once

	release := func() { releaseOnce.Do(func() { close(resume) }) }

	// Pause the spend after its eligibility read, which is where a stale read is taken.
	setDAHAfterRead = func() {
		once.Do(func() {
			close(reached)
			<-resume
		})
	}

	t.Cleanup(func() {
		release()

		setDAHAfterRead = nil
	})

	spendDone := make(chan error, 1)

	// The hook runs inside SetMinedMulti, after its block_ids INSERT. With the row lock
	// the spend blocks on it; without, the spend reaches its write hook.
	setMinedAfterBlockIDs = func() {
		go func() {
			_, err := store.Spend(ctx, child, floorCachedTip+1)
			spendDone <- err
		}()

		select {
		case <-reached:
		case <-time.After(3 * time.Second):
		}
	}

	t.Cleanup(func() { setMinedAfterBlockIDs = nil })

	minedDone := make(chan error, 1)

	go func() {
		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: floorMinedHeight, OnLongestChain: true,
		})
		minedDone <- err
	}()

	// Hold the spend at its write hook until SetMinedMulti's UPDATE waits on its row lock.
	go func() {
		<-reached

		deadline := time.Now().Add(3 * time.Second)

		for time.Now().Before(deadline) {
			var waiting int

			if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting); err == nil && waiting > 0 {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		release()
	}()

	select {
	case err := <-minedDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("SetMinedMulti did not finish")
	}

	select {
	case err := <-spendDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the spend did not finish")
	}

	var unspent int

	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM outputs o JOIN transactions t ON t.id = o.transaction_id WHERE t.hash = $1 AND o.spending_data IS NULL",
		hash[:]).Scan(&unspent))
	require.Zero(t, unspent, "setup: the spend must have taken the last output")

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah, "a mined, fully spent parent must carry a delete_at_height whichever writer ran first")
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
}

// TestSetMinedMultiTiming_Postgres times SetMinedMulti over n stored transactions, the
// cost the up-front row lock adds to it. It only measures: it runs when
// TERANODE_SQL_TIMING is set and logs the best of three runs, each over fresh rows.
func TestSetMinedMultiTiming_Postgres(t *testing.T) {
	if os.Getenv("TERANODE_SQL_TIMING") == "" {
		t.Skip("set TERANODE_SQL_TIMING=1 to run the SetMinedMulti timing")
	}

	store, ctx := setupPostgresStore(t)

	const n = 2000

	best := time.Duration(1<<63 - 1)

	for run := 0; run < 3; run++ {
		hashes := make([]*chainhash.Hash, 0, n)

		for i := 0; i < n; i++ {
			tx := bt.NewTx()
			prev := chainhash.HashH([]byte(fmt.Sprintf("setmined-timing-%d-%d", run, i)))
			require.NoError(t, tx.From(prev.String(), 0, "51", 10000))
			tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

			_, err := store.Create(ctx, tx, 1000)
			require.NoError(t, err)

			hashes = append(hashes, tx.TxIDChainHash())
		}

		start := time.Now()

		_, err := store.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{
			BlockID: uint32(100 + run), BlockHeight: 1001, OnLongestChain: true,
		})
		require.NoError(t, err)

		if d := time.Since(start); d < best {
			best = d
		}
	}

	t.Logf("SetMinedMulti n=%d best of 3: %s", n, best)
}

// TestSpendBatchSurvivesAParentThePruneDeletes_Postgres: the pruner locks every candidate
// parent until it commits, and a replay of a mined child is an idempotent match that sends
// setDAH to that parent's row. The batch waits for the lock, and when the prune commits the
// parent is gone. Only the spend of that parent is answered, as a parent that was already
// gone is answered, and the honest spend in the same batch still commits. It used to fail
// every spend of the batch with a storage error.
//
// The prune is a transaction the test holds open, so the wait is observed in pg_locks and
// ended by the test, with no sleeps in between. Both Postgres spend paths are run: the bulk one
// and the per-row one, which also sends an idempotent match to setDAH.
func TestSpendBatchSurvivesAParentThePruneDeletes_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, path := range []struct {
		name string
		bulk bool
	}{{"bulk", true}, {"per_row", false}} {
		t.Run(path.name, func(t *testing.T) {
			testSpendBatchSurvivesAParentThePruneDeletes(t, path.bulk)
		})
	}
}

func testSpendBatchSurvivesAParentThePruneDeletes(t *testing.T, bulk bool) {
	store, ctx := setupPostgresStore(t)
	floorRetention(t, store)

	store.settings.UtxoStore.BatchSQLOperations = bulk
	require.NoError(t, store.SetBlockHeight(1000))

	newParent := func(prev string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(prev, 0, "51", 30000))
		parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		return parent
	}

	spenderOf := func(parent *bt.Tx) *bt.Tx {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

		return child
	}

	itemOf := func(parent, child *bt.Tx) *batchSpend {
		utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
		require.NoError(t, err)

		return &batchSpend{
			spend: &utxo.Spend{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			},
			blockHeight: 1000,
			errCh:       make(chan error, 1),
		}
	}

	candidate := newParent("5555555555555555555555555555555555555555555555555555555555555555")
	bystander := newParent("6666666666666666666666666666666666666666666666666666666666666666")

	replayed := spenderOf(candidate)
	honest := spenderOf(bystander)

	// The replayed child's own spend, committed earlier, so the replay is an idempotent match.
	_, err := store.Spend(ctx, replayed, 1000)
	require.NoError(t, err)

	prune, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	t.Cleanup(func() { _ = prune.Rollback() })

	var lockedID int
	require.NoError(t, prune.QueryRowContext(ctx,
		"SELECT id FROM transactions WHERE hash = $1 FOR NO KEY UPDATE", candidate.TxIDChainHash()[:]).Scan(&lockedID))

	replayItem, honestItem := itemOf(candidate, replayed), itemOf(bystander, honest)

	done := make(chan error, 1)

	go func() {
		store.sendSpendBatch([]*batchSpend{replayItem, honestItem})
		done <- nil
	}()

	waitBlockedOrDone(ctx, t, store, done)

	select {
	case err = <-done:
		t.Fatalf("the batch finished while the prune held the parent's row: %v", err)
	default:
	}

	_, err = prune.ExecContext(ctx, "DELETE FROM transactions WHERE id = $1", lockedID)
	require.NoError(t, err)
	require.NoError(t, prune.Commit())

	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the batch did not finish once the prune committed")
	}

	require.NoError(t, <-honestItem.errCh, "an honest spend in the batch must not fail for another parent's deletion")
	require.ErrorIs(t, <-replayItem.errCh, errors.ErrTxNotFound, "the replay is answered as a parent that is gone")

	require.False(t, rowExists(ctx, t, store, *candidate.TxIDChainHash()), "the prune deleted the candidate")
	require.Equal(t, spendpkg.NewSpendingData(honest.TxIDChainHash(), 0).Bytes(), outputSpendingData(t, ctx, store, bystander, 0),
		"the honest spend committed")
}
