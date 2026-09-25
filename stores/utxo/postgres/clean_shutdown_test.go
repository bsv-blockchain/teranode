package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// wipeCleanShutdownTestDB drops every table the store schema owns so each test
// in this file starts from a known-empty database. It mirrors the wipe list in
// setupTestStore (store_test.go) plus the new store_clean_shutdown marker table.
func wipeCleanShutdownTestDB(t *testing.T, ctx context.Context) {
	t.Helper()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw, dah_watermark, dah_part_watermark, dah_sweep_control, dah_reconcile_cursor,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
		DROP TABLE IF EXISTS pending_unmined CASCADE;
		DROP TABLE IF EXISTS pending_deletes CASCADE;
		DROP TABLE IF EXISTS store_clean_shutdown CASCADE;
		DROP INDEX IF EXISTS px_pu_backfill_marker;
	`)
}

// newCleanShutdownTestStore opens a new store against testDSN WITHOUT wiping the
// schema first (unlike setupTestStore's helper), so that a prior store's on-disk
// state — including the store_clean_shutdown marker row — survives into this
// store's startup. Callers are responsible for stopping the returned store.
func newCleanShutdownTestStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// queryCleanShutdownMarker opens its own connection (independent of any store's
// pool) and returns the store_clean_shutdown marker's clean value for id=1.
func queryCleanShutdownMarker(t *testing.T, ctx context.Context) bool {
	t.Helper()

	pool, err := pgxpool.New(ctx, testDSN)
	require.NoError(t, err)
	defer pool.Close()

	var clean bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT clean FROM store_clean_shutdown WHERE id = 1`).Scan(&clean))

	return clean
}

// insertTrapTx inserts a tx row DIRECTLY into txs (bypassing the write-behind
// projector entirely) with unmined_since set and conflicting=false, simulating
// data that only the seq-scan backfill would repair into pending_unmined.
func insertTrapTx(t *testing.T, ctx context.Context, hash []byte, unmindedSince int32) {
	t.Helper()

	pool, err := pgxpool.New(ctx, testDSN)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx,
		`INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since)
         VALUES ($1, 1, 0, 100, 50, false, $2)`,
		hash, unmindedSince)
	require.NoError(t, err)
}

// pendingUnminedHasHash opens its own connection and reports whether hash is
// present in pending_unmined.
func pendingUnminedHasHash(t *testing.T, ctx context.Context, hash []byte) bool {
	t.Helper()

	pool, err := pgxpool.New(ctx, testDSN)
	require.NoError(t, err)
	defer pool.Close()

	var exists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, hash).Scan(&exists))

	return exists
}

// TestCleanShutdownMarkerLifecycle verifies that the store_clean_shutdown marker
// is stamped clean=false while a store is running, and clean=true immediately
// after a clean Stop().
func TestCleanShutdownMarkerLifecycle(t *testing.T) {
	ctx := context.Background()
	wipeCleanShutdownTestDB(t, ctx)

	store := newCleanShutdownTestStore(t, ctx)

	require.False(t, queryCleanShutdownMarker(t, ctx), "marker must be clean=false while the store is running")

	store.Stop()

	require.True(t, queryCleanShutdownMarker(t, ctx), "marker must be clean=true immediately after a clean Stop()")
}

// TestProjectorDropForcesUncleanShutdown verifies Change 3: if the write-behind
// projector ever drops buffered entries at its hard cap (puDropped latched),
// then even a graceful Stop() must record the shutdown as UNCLEAN (clean=false)
// so the next startup runs the fail-safe backfill and re-projects any dropped
// genuinely-unmined tx. Without the fix, Stop() would stamp clean=true and the
// next startup would trust an incomplete pending_unmined.
func TestProjectorDropForcesUncleanShutdown(t *testing.T) {
	ctx := context.Background()
	wipeCleanShutdownTestDB(t, ctx)

	store := newCleanShutdownTestStore(t, ctx)

	// Simulate the projector having dropped entries at its hard cap this run.
	store.puDropped.Store(true)

	store.Stop()

	require.False(t, queryCleanShutdownMarker(t, ctx),
		"a projector drop must force the shutdown marker to clean=false, even on a graceful Stop()")
}

// TestBackfillSkippedAfterCleanShutdown verifies that after a clean Stop(), a
// second store startup SKIPS the pending_unmined seq-scan backfill: a trap row
// inserted directly into txs (bypassing the projector) must NOT appear in
// pending_unmined, proving the backfill never ran.
func TestBackfillSkippedAfterCleanShutdown(t *testing.T) {
	ctx := context.Background()
	wipeCleanShutdownTestDB(t, ctx)

	store1 := newCleanShutdownTestStore(t, ctx)
	store1.Stop()

	trapHash := make([]byte, 32)
	trapHash[0] = 0xAA
	insertTrapTx(t, ctx, trapHash, 123)

	store2 := newCleanShutdownTestStore(t, ctx)
	t.Cleanup(store2.Stop)

	require.False(t, pendingUnminedHasHash(t, ctx, trapHash),
		"backfill must have been skipped after a clean shutdown — trap row must NOT be in pending_unmined")
	require.False(t, queryCleanShutdownMarker(t, ctx), "marker must be clean=false while store2 is running")
}

// TestBackfillRunsAfterUncleanShutdown verifies that when the marker indicates
// an unclean prior shutdown, the next store startup RUNS the pending_unmined
// seq-scan backfill: a trap row inserted directly into txs must appear in
// pending_unmined after the second store starts.
func TestBackfillRunsAfterUncleanShutdown(t *testing.T) {
	ctx := context.Background()
	wipeCleanShutdownTestDB(t, ctx)

	store1 := newCleanShutdownTestStore(t, ctx)
	store1.Stop()

	trapHash := make([]byte, 32)
	trapHash[0] = 0xBB
	insertTrapTx(t, ctx, trapHash, 456)

	// Simulate an unclean/crashed prior run by forcing the marker back to false
	// after store1's clean Stop() already stamped it true.
	pool, err := pgxpool.New(ctx, testDSN)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE store_clean_shutdown SET clean = false WHERE id = 1`)
	require.NoError(t, err)
	pool.Close()

	store2 := newCleanShutdownTestStore(t, ctx)
	t.Cleanup(store2.Stop)

	require.True(t, pendingUnminedHasHash(t, ctx, trapHash),
		"backfill must have run after an unclean shutdown — trap row must be in pending_unmined")
}

// TestCleanShutdown_CreateBatcherDrainedBeforeMarkerStamped guards the
// false-clean teardown race: items still QUEUED in the create batcher's
// channel at the moment Stop() is called must be fully drained — and their
// enqueuePendingUnmined side effect flushed into pending_unmined — BEFORE the
// clean-shutdown marker is stamped true.
//
// Before the fix, Stop() called stopPendingUnminedProjector() (the write-behind
// projector's final drain, and permanent exit of its writer goroutine) BEFORE
// closing the create batcher. go-batcher's Close() blocks until every queued
// item has been handed to its callback (sendCreateBatch), which calls
// enqueuePendingUnmined — so any item still sitting in the batcher's channel
// at Stop() time was hashed into puBuf by sendCreateBatch AFTER the projector
// had already done its final flush and exited, with no consumer left to ever
// write it to pending_unmined. Meanwhile markCleanShutdown() had already
// stamped clean=true, so the next startup would (wrongly) skip the repair
// backfill — permanently losing visibility of that tx to the pruner/iterator.
//
// This test enqueues items directly into the batcher's channel via
// createBatcher.Put (bypassing the blocking Create() wrapper, so the test
// does not wait for them to be processed) with a batch size/duration large
// enough that nothing triggers a flush on its own — the items can only be
// processed by the drain Close() performs. It then calls Stop() immediately.
// This is deterministic (no sleep, no timing race): every item is guaranteed
// to still be queued, unprocessed, at the moment Stop() begins.
func TestCleanShutdown_CreateBatcherDrainedBeforeMarkerStamped(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Large size + long duration: with 500 queued items and a 10,000-item size
	// cap / 60s timer, nothing here can trigger a natural flush during the
	// test. The only thing that will ever process these items is the drain
	// Close() performs during Stop().
	st.settings.UtxoStore.StoreBatcherSize = 10_000
	st.settings.UtxoStore.StoreBatcherDurationMillis = 60_000
	st.Start(ctx)

	const n = 500

	hashes := make([]*chainhash.Hash, n)
	for i := 0; i < n; i++ {
		tx := makeBenchCreateTx()
		hashes[i] = tx.TxIDChainHash()

		// Put directly into the batcher's channel — fire-and-forget. Going
		// through Create() would block this goroutine on <-done until the
		// batch is dispatched, which never happens naturally here (that's
		// the point): we need the item QUEUED, not delivered, when Stop()
		// runs below.
		st.createBatcher.Put(&batchCreateItem{
			tx:          tx,
			blockHeight: 100,
			options:     &utxo.CreateOptions{},
			done:        make(chan batchCreateResult, 1),
		})
	}

	// Stop immediately: no waiting for the batch to flush naturally. Stop()
	// itself must force the create batcher to drain (handing every queued
	// item to sendCreateBatch) BEFORE the pending_unmined projector's final
	// drain runs and BEFORE the clean-shutdown marker is stamped.
	st.Stop()

	require.True(t, queryCleanShutdownMarker(t, ctx),
		"marker must be clean=true after Stop()")

	for i, h := range hashes {
		require.True(t, pendingUnminedHasHash(t, ctx, h[:]),
			"tx %d enqueued just before Stop() must be in pending_unmined — proves the "+
				"create batcher was drained (its queued item handed to sendCreateBatch, "+
				"which calls enqueuePendingUnmined) before the projector's final flush "+
				"and the clean-shutdown marker were stamped", i)
	}
}
