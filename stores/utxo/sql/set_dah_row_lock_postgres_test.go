package sql

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const pgLockNotAvailable = "55P03"

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
