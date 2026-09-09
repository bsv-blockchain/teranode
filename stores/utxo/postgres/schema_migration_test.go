package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchema_SpentBitsColumns asserts that:
//  1. Both v15 fold columns (spent_bits, last_spend_height) exist on txs, and the
//     pre-v15 spent_progress counter column does NOT (a fresh schema must never
//     recreate it — bootstrapDAHSweepProc refuses to arm when it is present).
//  2. Neither fold column is covered by any btree index on txs — HOT-update safety.
func TestSchema_SpentBitsColumns(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx := context.Background()

	// --- assertion 1: both bitmap columns present, the counter column gone ---
	var n int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_bits', 'last_spend_height')`).Scan(&n))
	require.Equal(t, 2, n, "txs must have both spent_bits and last_spend_height columns")

	var counter int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name = 'spent_progress'`).Scan(&counter))
	require.Zero(t, counter, "the pre-v15 spent_progress counter column must not exist on a fresh schema")

	// --- assertion 2: neither column may appear in any btree index on txs ---
	// Checks pg_index + pg_attribute across all partitions inheriting from txs.
	var idx int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_index i
		JOIN pg_class c  ON c.oid = i.indrelid
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_am am    ON am.oid = (SELECT relam FROM pg_class WHERE oid = i.indexrelid)
		WHERE (c.relname = 'txs' OR c.relname LIKE 'txs_p%')
		  AND am.amname = 'btree'
		  AND a.attname IN ('spent_bits', 'last_spend_height')`).Scan(&idx))
	require.Zero(t, idx, "spent_bits and last_spend_height must NOT be covered by any btree index on txs (HOT safety)")
}
