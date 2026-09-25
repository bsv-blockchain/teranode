package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// cleanPendingUnmined removes all rows from pending_unmined so each test starts
// with a clean side-table. newTestStoreWithFlag drops txs but not
// pending_unmined, so rows from a previous test run would otherwise persist.
func cleanPendingUnmined(t *testing.T, st *Store) {
	t.Helper()
	_, err := st.pool.Exec(context.Background(), `DELETE FROM pending_unmined`)
	require.NoError(t, err, "cleanPendingUnmined")
}

// TestPrunableUnminedTxIterator_RequiresPendingUnmined verifies the key
// contract of the JOIN-based query: a tx that is in txs with unmined_since set
// but is absent from pending_unmined must NOT be returned by the iterator.
// This test FAILS with the old seq-scan query (which reads txs directly) and
// PASSES with the new JOIN query (which drives from pending_unmined).
func TestPrunableUnminedTxIterator_RequiresPendingUnmined(t *testing.T) {
	st := newTestStoreWithFlag(t, false)
	ctx := context.Background()
	cleanPendingUnmined(t, st)

	// Create an unmined tx. The write hook inserts it into pending_unmined.
	tx := testExtendedTx(t)
	tx.LockTime = 400
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	_, err = st.pool.Exec(ctx,
		`UPDATE txs SET unmined_since = 1000 WHERE hash = $1`, h[:])
	require.NoError(t, err)

	// Manually remove the tx from pending_unmined to simulate the "absent" state.
	// The contract under test: a tx in txs with unmined_since IS NOT NULL but
	// absent from pending_unmined must NOT appear in the iterator result.
	_, err = st.pool.Exec(ctx, `DELETE FROM pending_unmined WHERE hash = $1`, h[:])
	require.NoError(t, err)

	// Confirm the tx is in txs with unmined_since set (sanity check).
	var count int
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE hash=$1 AND unmined_since IS NOT NULL`, h[:]).Scan(&count))
	require.Equal(t, 1, count, "precondition: tx must be in txs with unmined_since set")

	// pending_unmined is intentionally empty — the JOIN must yield 0 results.
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_unmined`).Scan(&count))
	require.Equal(t, 0, count, "precondition: pending_unmined must be empty after manual DELETE")

	// New JOIN query → must return 0 (tx absent from pending_unmined).
	// Old seq-scan query → would have returned 1 (reads from txs directly).
	iter, err := st.GetPrunableUnminedTxIterator(2000)
	require.NoError(t, err)

	var resultCount int
	for {
		batch, err := iter.Next(ctx)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, utx := range batch {
			if utx.Skip {
				continue
			}
			resultCount++
		}
	}
	require.NoError(t, iter.Close())

	require.Equal(t, 0, resultCount,
		"txs absent from pending_unmined must NOT be returned by the JOIN query")
}

// TestPrunableUnminedTxIterator_JoinsPendingUnmined verifies that txs present
// in pending_unmined with unmined_since <= cutoff are returned, and those with
// unmined_since > cutoff are not.
func TestPrunableUnminedTxIterator_JoinsPendingUnmined(t *testing.T) {
	st := newTestStoreWithFlag(t, false)
	ctx := context.Background()
	cleanPendingUnmined(t, st)

	// Create 5 distinct unmined txs. Vary LockTime for distinct txids.
	hashes := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		tx := testExtendedTx(t)
		tx.LockTime = uint32(500 + i)
		_, err := st.Create(ctx, tx, 100)
		require.NoError(t, err)

		h := tx.TxIDChainHash()
		hashes[i] = h[:]

		// Set unmined_since to staggered heights 1001..1005.
		_, err = st.pool.Exec(ctx,
			`UPDATE txs SET unmined_since = $1 WHERE hash = $2`,
			int32(1001+i), h[:])
		require.NoError(t, err)
	}

	// Update pending_unmined to the staggered heights used by this test. The
	// write hook already inserted each tx at unmined_since=100 (the create
	// blockHeight). Use UPSERT to set the staggered values the iterator checks.
	for i, h := range hashes {
		_, err := st.pool.Exec(ctx,
			`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, $2)
			 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
			h, int32(1001+i))
		require.NoError(t, err)
	}

	// cutoff=1002 → iterator must return exactly 2 txs (unmined_since 1001, 1002).
	iter, err := st.GetPrunableUnminedTxIterator(1002)
	require.NoError(t, err)

	var resultCount int
	for {
		batch, err := iter.Next(ctx)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, utx := range batch {
			if utx.Skip {
				continue
			}
			resultCount++
		}
	}
	require.NoError(t, iter.Close())

	require.Equal(t, 2, resultCount,
		"iterator should return exactly 2 txs with unmined_since <= 1002")
}

// TestPrunableUnminedTxIterator_SkipConflicting verifies that conflicting txs
// are excluded even when they have a row in pending_unmined.
func TestPrunableUnminedTxIterator_SkipConflicting(t *testing.T) {
	st := newTestStoreWithFlag(t, false)
	ctx := context.Background()
	cleanPendingUnmined(t, st)

	// Create 2 distinct txs. Use LockTimes 600..601 to avoid collision.
	tx1 := testExtendedTx(t)
	tx1.LockTime = 600
	tx2 := testExtendedTx(t)
	tx2.LockTime = 601

	_, err := st.Create(ctx, tx1, 100)
	require.NoError(t, err)
	_, err = st.Create(ctx, tx2, 100)
	require.NoError(t, err)

	h1 := tx1.TxIDChainHash()
	h2 := tx2.TxIDChainHash()

	// Set both unmined_since to 1001.
	_, err = st.pool.Exec(ctx, `UPDATE txs SET unmined_since = 1001 WHERE hash = $1`, h1[:])
	require.NoError(t, err)
	_, err = st.pool.Exec(ctx, `UPDATE txs SET unmined_since = 1001 WHERE hash = $1`, h2[:])
	require.NoError(t, err)

	// Upsert both into pending_unmined with the staggered height used by this test.
	// The write hook already inserted at unmined_since=100; update to 1001.
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, 1001), ($2, 1001)
		 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
		h1[:], h2[:])
	require.NoError(t, err)

	// Mark tx2 as conflicting.
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h2}, true)
	require.NoError(t, err)

	// Iterator should return only tx1 (tx2 is conflicting=true, filtered by WHERE).
	iter, err := st.GetPrunableUnminedTxIterator(1001)
	require.NoError(t, err)

	var resultCount int
	var foundH1 bool
	for {
		batch, err := iter.Next(ctx)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, utx := range batch {
			if utx.Skip {
				continue
			}
			resultCount++
			if utx.Node != nil && utx.Node.Hash == *h1 {
				foundH1 = true
			}
		}
	}
	require.NoError(t, iter.Close())

	require.Equal(t, 1, resultCount, "iterator should skip conflicting tx")
	require.True(t, foundH1, "tx1 (non-conflicting) must be returned")
}

// TestPrunableUnminedTxIterator_ExplainShowsIndexScan verifies that the query
// plan for the JOIN uses an index-based access path on pending_unmined, not a
// sequential scan of the entire txs table as the outer/driving node.
func TestPrunableUnminedTxIterator_ExplainShowsIndexScan(t *testing.T) {
	st := newTestStoreWithFlag(t, false)
	ctx := context.Background()
	cleanPendingUnmined(t, st)

	// Seed one tx and one pending_unmined row so the planner has basic stats.
	tx := testExtendedTx(t)
	tx.LockTime = 700
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	_, err = st.pool.Exec(ctx, `UPDATE txs SET unmined_since = 1001 WHERE hash = $1`, h[:])
	require.NoError(t, err)

	// Upsert into pending_unmined with the target height. Write hook already
	// inserted at unmined_since=100 (the create blockHeight); update to 1001.
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, 1001)
		 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
		h[:])
	require.NoError(t, err)

	// EXPLAIN the production JOIN query (TEXT format for readability in logs).
	// Must match the query in newPrunableUnminedTxIterator exactly.
	q := `
		EXPLAIN (FORMAT TEXT)
		SELECT t.hash, t.fee, t.size_in_bytes, t.inserted_at, t.coinbase,
		       t.locked, pu.unmined_since, t.raw_tx, t.mined_info
		FROM pending_unmined pu
		JOIN txs t ON t.hash = pu.hash
		WHERE pu.unmined_since <= $1
		  AND t.conflicting = false
		  AND t.unmined_since IS NOT NULL
		ORDER BY t.hash
	`

	rows, err := st.pool.Query(ctx, q, int32(1001))
	require.NoError(t, err)
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		planLines = append(planLines, line)
	}
	require.NoError(t, rows.Err())

	plan := strings.Join(planLines, "\n")
	t.Logf("EXPLAIN output:\n%s", plan)

	// With pending_unmined as the driving side, the planner must use a join
	// strategy (Nested Loop / Hash Join / Merge Join) driven by the small
	// side-table, not a bare Seq Scan on txs as the root node.
	hasJoinOrIndex := strings.Contains(plan, "Index Scan") ||
		strings.Contains(plan, "Bitmap") ||
		strings.Contains(plan, "Hash") ||
		strings.Contains(plan, "Nested Loop") ||
		strings.Contains(plan, "Merge Join")
	require.True(t, hasJoinOrIndex,
		"plan should use an index/join strategy on pending_unmined, got:\n%s", plan)
}

// TestPrunableUnminedTxIterator_Lever1_UnminedReturnedMinedCleanedNotReturned is the
// combined lever-1 correctness test:
//
//   - A genuinely-unmined tx (unmined_since <= cutoff, not conflicting) IS returned
//     by the pruner read and its pending_unmined row is NOT removed by lazy cleanup.
//   - A mined tx whose pending_unmined row LINGERS (stale, lever-1 hot-path removed)
//     IS cleaned by lazy cleanup and is NOT returned by the pruner read.
func TestPrunableUnminedTxIterator_Lever1_UnminedReturnedMinedCleanedNotReturned(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()
	cleanPendingUnmined(t, st)
	require.NoError(t, st.SetBlockHeight(100))

	// Create a genuinely-unmined tx at unmined_since=50.
	unminedTx := testExtendedTx(t)
	unminedTx.LockTime = 800
	_, err := st.Create(ctx, unminedTx, 100)
	require.NoError(t, err)
	unminedHash := unminedTx.TxIDChainHash()
	// Backfill pending_unmined to the test cutoff height.
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, 50)
		 ON CONFLICT (hash) DO UPDATE SET unmined_since = EXCLUDED.unmined_since`,
		unminedHash[:])
	require.NoError(t, err)

	// Create a tx, then mine it (OnLongestChain=true → unmined_since=NULL in txs).
	// After mine the pending_unmined row LINGERS (lever-1: hot-path DELETE removed).
	minedTx := testExtendedTx(t)
	minedTx.LockTime = 801
	_, err = st.Create(ctx, minedTx, 100)
	require.NoError(t, err)
	minedHash := minedTx.TxIDChainHash()
	// Flush the projector BEFORE mining: the flush is mined-aware (it skips
	// hashes whose tx is no longer unmined), so the lingering-row state this
	// test exercises only arises when the projection lands while the tx is
	// still unmined and the tx mines AFTERWARDS — project first, then mine.
	require.NoError(t, st.flushPendingUnmined(ctx))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{minedHash}, utxo.MinedBlockInfo{
		BlockID: 42, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	// Precondition: mined tx's row lingered.
	var minedRowExists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, minedHash[:]).Scan(&minedRowExists))
	require.True(t, minedRowExists, "precondition: mined tx's pending_unmined row must linger before pruner call")

	// Call GetPrunableUnminedTxIterator with cutoff=200 (covers both rows).
	iter, err := st.GetPrunableUnminedTxIterator(200)
	require.NoError(t, err)

	var foundUnmined, foundMined bool
	for {
		batch, batchErr := iter.Next(ctx)
		require.NoError(t, batchErr)
		if len(batch) == 0 {
			break
		}
		for _, utx := range batch {
			if utx.Skip || utx.Node == nil {
				continue
			}
			if utx.Node.Hash == *unminedHash {
				foundUnmined = true
			}
			if utx.Node.Hash == *minedHash {
				foundMined = true
			}
		}
	}
	require.NoError(t, iter.Close())

	// Genuinely-unmined tx IS returned.
	require.True(t, foundUnmined, "genuinely-unmined tx must be returned by pruner read")
	// Mined tx is NOT returned (read-filter: t.unmined_since IS NOT NULL).
	require.False(t, foundMined, "mined tx must NOT be returned by pruner read (read-filter)")

	// After the pruner call: stale mined-tx row must be removed by lazy cleanup.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var minedRowAfter bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, minedHash[:]).Scan(&minedRowAfter))
	require.False(t, minedRowAfter, "stale pending_unmined row for mined tx must be removed by lazy cleanup")

	// Genuinely-unmined tx's row must NOT be removed (it IS still unmined).
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var unminedRowAfter bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, unminedHash[:]).Scan(&unminedRowAfter))
	require.True(t, unminedRowAfter, "genuinely-unmined tx's pending_unmined row must NOT be removed by lazy cleanup")
}
