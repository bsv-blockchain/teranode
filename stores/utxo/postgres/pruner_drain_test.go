package postgres

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPendingDeletes_PruneDrainsBacklogBeyondOneCallCap is the regression gate for the
// block-triggered-pruner starvation bug seen live on teratestnet: a large eligible
// backlog (delete_at_height <= tip) was left half-pruned because one Prune call stopped
// after a bounded number of batches per partition and then waited for the NEXT block
// notification to run again. On a caught-up node with sparse blocks, the backlog barely
// drained (measured: ~9.5M eligible rows sitting, oldest overdue ~20,000 blocks).
//
// The contract: ONE Prune call must drain the ENTIRE eligible backlog — it loops each
// partition until a batch comes back short (fewer than pruneDeleteBatchSize rows), which
// is the drain signal. There is no per-call cap.
//
// The test shrinks pruneDeleteBatchSize so a modest, fast-to-insert backlog (800 rows)
// exceeds what the OLD capped code (pruneDeleteMaxBatchesPerCall * batchSize per
// partition = at most 24 * 8 = 192 rows/call) could ever delete in one call. Rows are
// inserted directly (COPY) as tombstoned txs + matching pending_deletes rows, so the
// test is fast and independent of the tx-creation path.
func TestPendingDeletes_PruneDrainsBacklogBeyondOneCallCap(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	// Shrink the per-statement batch so 800 rows span far more batches than any
	// former per-call cap, without inserting hundreds of thousands of rows.
	orig := pruneDeleteBatchSize
	pruneDeleteBatchSize = 3
	t.Cleanup(func() { pruneDeleteBatchSize = orig })

	const (
		total       = 800        // eligible tombstoned rows to insert (spread across 8 hash partitions)
		dah         = int32(100) // delete_at_height for every row — well below the prune height
		pruneHeight = uint32(200)
	)
	require.NoError(t, st.SetBlockHeight(pruneHeight))

	// Insert `total` tombstoned txs + matching pending_deletes rows directly. Unique,
	// monotonically increasing hashes spread the rows across all 8 hash partitions.
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

	// Precondition: all rows present and eligible.
	var before int
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT count(*) FROM pending_deletes`).Scan(&before))
	require.Equal(t, total, before, "setup: all rows must be inserted and eligible")

	// One Prune call must drain the ENTIRE eligible backlog.
	prunerSvc, err := st.GetPrunerService()
	require.NoError(t, err)
	deleted, err := prunerSvc.Prune(ctx, pruneHeight, "drain-backlog-test")
	require.NoError(t, err)
	require.Equal(t, int64(total), deleted, "one Prune must delete the entire eligible backlog in a single call")

	var pendAfter, txsAfter int
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT count(*) FROM pending_deletes`).Scan(&pendAfter))
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE delete_at_height <= $1`, int32(pruneHeight)).Scan(&txsAfter))
	require.Zero(t, pendAfter, "pending_deletes must be fully drained after one Prune")
	require.Zero(t, txsAfter, "no eligible tombstoned txs may remain after one Prune")
}
