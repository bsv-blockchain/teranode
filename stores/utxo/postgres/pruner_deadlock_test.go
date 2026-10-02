// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPrune_RerunAfterFullDrainIsIdempotent demonstrates the safety property the
// pruner cascade-delete deadlock retry (deleteTombstonedBatchWithRetry in
// pruner_provider.go, wired through the shared retryOnPgDeadlock in deadlock.go)
// depends on: deleteTombstonedBatch is one pgx transaction that rolls back entirely
// on any error, so a retry after a deadlocked attempt persists nothing partial, and
// it re-selects its candidate set (`SELECT ... FOR UPDATE` against pending_deletes)
// from CURRENT ground truth rather than a stale list. This test simulates "the
// cascade batch ran once, then ran again" (what a deadlock retry does) at the
// Prune()-call level: running Prune a second time after it has already drained the
// whole backlog must delete nothing and error nothing, proving a retried batch
// cannot double-delete or trip over rows that no longer exist.
func TestPrune_RerunAfterFullDrainIsIdempotent(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	const (
		total       = 50
		dah         = int32(100)
		pruneHeight = uint32(200)
	)
	require.NoError(t, st.SetBlockHeight(pruneHeight))

	txsRows := make([][]any, 0, total)
	pdRows := make([][]any, 0, total)
	for i := 0; i < total; i++ {
		h := make([]byte, 32)
		binary.BigEndian.PutUint64(h[24:], uint64(i+1))
		txsRows = append(txsRows, []any{h, int64(1), int64(0), int64(0), int64(0), dah})
		pdRows = append(pdRows, []any{h, dah})
	}
	_, err := st.pool.CopyFrom(ctx, pgx.Identifier{"txs"},
		[]string{"hash", "version", "lock_time", "fee", "size_in_bytes", "delete_at_height"},
		pgx.CopyFromRows(txsRows))
	require.NoError(t, err)
	_, err = st.pool.CopyFrom(ctx, pgx.Identifier{"pending_deletes"},
		[]string{"hash", "delete_at_height"}, pgx.CopyFromRows(pdRows))
	require.NoError(t, err)

	prunerSvc, err := st.GetPrunerService()
	require.NoError(t, err)

	deleted1, err := prunerSvc.Prune(ctx, pruneHeight, "first-drain")
	require.NoError(t, err)
	require.Equal(t, int64(total), deleted1, "first Prune must delete the entire eligible backlog")

	// Re-run: simulates a retry landing after the (hypothetical) winning attempt
	// already committed. Nothing eligible remains, so it must be a clean no-op.
	deleted2, err := prunerSvc.Prune(ctx, pruneHeight, "second-drain")
	require.NoError(t, err)
	require.Zero(t, deleted2, "re-running Prune after a full drain must delete nothing and error nothing")

	var pdCount, txsCount int
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT count(*) FROM pending_deletes`).Scan(&pdCount))
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT count(*) FROM txs`).Scan(&txsCount))
	require.Zero(t, pdCount)
	require.Zero(t, txsCount)
}

// TestDeleteTombstonedBatchWithRetry_NonDeadlockErrorSurfacesImmediately exercises
// the pruner cascade-delete retry site's wiring with a REAL (non-deadlock) Postgres
// error: deleteTombstonedBatchWithRetry is called directly with deliberately
// malformed SQL standing in for the site's real doomedSQL/cascadeSQL, producing a
// genuine driver syntax error. It must be classified as NOT a deadlock (matching
// TestRetryOnPgDeadlock's generic "non-deadlock error returns immediately" case) and
// returned wrapped on the FIRST attempt -- proven by bounding the elapsed time well
// under what pgDeadlockMaxRetries retries with jittered backoff would take.
func TestDeleteTombstonedBatchWithRetry_NonDeadlockErrorSurfacesImmediately(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	svc := &postgresPrunerService{store: st, logger: st.logger}

	start := time.Now()
	_, err := svc.deleteTombstonedBatchWithRetry(ctx,
		`SELECT this is not valid SQL`, `DELETE FROM this_table_does_not_exist_xyz`,
		100, "txs_p00")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "cascade delete txs_p00")
	require.Less(t, elapsed, 500*time.Millisecond,
		"a non-deadlock error must surface on the first attempt, not after retry backoff")
}
