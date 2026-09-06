package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestSchema_PendingUnmined_CreationAndBackfill verifies that createSchemaWithPoolFlag
// always creates the pending_unmined parent table, 8 hash-partition leaves, 8 per-leaf
// btree indexes on unmined_since, and runs the one-time guarded backfill (leaving the
// marker index behind).
func TestSchema_PendingUnmined_CreationAndBackfill(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Clean slate: drop pending_unmined if a prior run left it.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	// Also drop the marker so the backfill can re-run on a fresh table.
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)

	// pending_unmined is ALWAYS-ON (no flag needed).
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: 8 pending_unmined leaves exist.
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'pending_unmined_p%' AND relkind='r'`).Scan(&n))
	require.Equal(t, 8, n, "8 pending_unmined leaves must be created")

	// Assert: 8 per-leaf btree indexes exist (px_pu_since_pNN).
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'px_pu_since_p%' AND relkind='i'`).Scan(&n))
	require.Equal(t, 8, n, "8 per-leaf btree indexes on unmined_since must be created")

	// The backfill now runs on EVERY startup (crash reconciliation for the
	// write-behind projector); the one-shot marker index is retired and dropped.
	var hasMarker bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_pu_backfill_marker')`).Scan(&hasMarker))
	require.False(t, hasMarker, "legacy one-shot backfill marker must be dropped (backfill runs every startup)")
}

// TestSchema_PendingUnmined_BackfillExistingUnminedTx verifies that an existing unmined
// (non-conflicting) tx row in txs is backfilled into pending_unmined when the schema is
// initialised with the table not yet present.
func TestSchema_PendingUnmined_BackfillExistingUnminedTx(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	// Full clean slate so we can create only txs first.
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS spends CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_part_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_sweep_control CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS conflict_intents CASCADE`)

	// Create only txs (no pending_unmined, no indexes yet) so we can seed a row before
	// the full schema init runs the backfill.
	_, err = pool.Exec(ctx, txsDDL)
	require.NoError(t, err, "create txs parent")
	_, err = pool.Exec(ctx, spendsDDL)
	require.NoError(t, err, "create spends parent")
	for i := 0; i < numPartitions; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS txs_p%02d PARTITION OF txs FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS spends_p%02d PARTITION OF spends FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
	}

	// Insert a non-conflicting tx row with unmined_since set.
	txHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	const unminedSinceVal = int32(100)
	_, err = pool.Exec(ctx,
		`INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since)
         VALUES ($1, 1, 0, 100, 50, false, $2)`,
		txHash, unminedSinceVal)
	require.NoError(t, err)

	// Verify the precondition: tx is in txs with unmined_since set.
	var unmined *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, txHash).Scan(&unmined))
	require.NotNil(t, unmined, "precondition: tx must have unmined_since in txs")
	require.Equal(t, unminedSinceVal, *unmined)

	// Now initialise the full schema (creates pending_unmined and runs the backfill).
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: the unmined tx was backfilled into pending_unmined with the correct unmined_since.
	var pu *int32
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, txHash).Scan(&pu))
	require.NotNil(t, pu, "unmined tx must be backfilled into pending_unmined")
	require.Equal(t, unminedSinceVal, *pu, "unmined_since in pending_unmined must match txs value")
}

// TestSchema_PendingUnmined_BackfillSkipsConflicting verifies that a conflicting unmined tx
// is NOT backfilled into pending_unmined (the INSERT … WHERE conflicting=false guard).
func TestSchema_PendingUnmined_BackfillSkipsConflicting(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS txs CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS spends CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_part_watermark CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS dah_sweep_control CASCADE`)
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS conflict_intents CASCADE`)

	_, err = pool.Exec(ctx, txsDDL)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, spendsDDL)
	require.NoError(t, err)
	for i := 0; i < numPartitions; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS txs_p%02d PARTITION OF txs FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS spends_p%02d PARTITION OF spends FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		))
		require.NoError(t, err)
	}

	// Insert a conflicting unmined tx (conflicting=true) — must NOT be backfilled.
	txHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33}
	const unminedSinceVal = int32(100)
	_, err = pool.Exec(ctx,
		`INSERT INTO txs (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since)
         VALUES ($1, 1, 0, 100, 50, true, $2)`,
		txHash, unminedSinceVal)
	require.NoError(t, err)

	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Assert: conflicting tx is NOT in pending_unmined.
	var exists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, txHash).Scan(&exists))
	require.False(t, exists, "conflicting tx must NOT be backfilled into pending_unmined")
}

// TestSchema_PendingUnmined_BackfillIdempotency verifies that calling createSchemaWithPoolFlag
// a second time is safe: the every-startup backfill is idempotent (ON CONFLICT DO NOTHING),
// so re-running creates no duplicates and no error.
func TestSchema_PendingUnmined_BackfillIdempotency(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skip("no postgres")
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS pending_unmined CASCADE`)
	_, _ = pool.Exec(ctx, `DROP INDEX IF EXISTS px_pu_backfill_marker`)

	// First init: backfill runs, marker created.
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// Second init: must succeed without error; the backfill re-runs idempotently.
	require.NoError(t, createSchemaWithPoolFlag(ctx, pool, false))

	// The retired one-shot marker must not exist.
	var markerCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_class WHERE relname='px_pu_backfill_marker'`).Scan(&markerCount))
	require.Equal(t, 0, markerCount, "legacy backfill marker must not be recreated")
}

// ---------------------------------------------------------------------------
// Task 4: Write hook tests — pending_unmined is populated on Create()
// ---------------------------------------------------------------------------

// TestCreateWriteHook_UnminedTx_SinglePath verifies that creating an unmined tx
// via the single/direct path (createDirect) inserts a row into pending_unmined
// with the correct unmined_since value.
func TestCreateWriteHook_UnminedTx_SinglePath(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	txHash := tx.TxIDChainHash()

	// Create unmined — no MinedBlockInfos → createDirect writes pending_unmined.
	md, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, uint32(100), md.UnminedSince, "metadata UnminedSince must equal blockHeight")

	// Drain the write-behind projector so the assertion is deterministic.
	require.NoError(t, st.flushPendingUnmined(ctx))

	// Assert the tx is in pending_unmined with matching unmined_since.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var storedUnminedSince int32
	err = st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`,
		txHash[:],
	).Scan(&storedUnminedSince)
	require.NoError(t, err, "unmined tx must be inserted into pending_unmined")
	require.Equal(t, int32(100), storedUnminedSince, "pending_unmined.unmined_since must equal blockHeight")
}

// TestCreateWriteHook_MinedTx_SinglePath verifies that creating a MINED tx via
// createDirect does NOT insert a row into pending_unmined.
func TestCreateWriteHook_MinedTx_SinglePath(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	txHash := tx.TxIDChainHash()

	// Create mined — MinedBlockInfos present → unmined_since = NULL in txs,
	// so no row should appear in pending_unmined.
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	md, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, uint32(0), md.UnminedSince, "mined tx must have zero UnminedSince")

	// Assert the tx is NOT in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`,
		txHash[:],
	).Scan(&exists))
	require.False(t, exists, "mined tx must NOT be inserted into pending_unmined")
}

// TestCreateWriteHook_ConflictingTx_NoPendingUnmined verifies that a conflicting
// unmined tx is NOT inserted into pending_unmined (conflicting txs are excluded
// from the projection invariant by the pruner).
func TestCreateWriteHook_ConflictingTx_NoPendingUnmined(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// First create a parent tx (non-conflicting) so it exists in the store.
	parent := testExtendedTx(t)
	_, err := st.Create(ctx, parent, 100)
	require.NoError(t, err)

	// Create a conflicting child tx (same inputs, different outputs).
	child := makeBenchCreateTx() // unique tx, no shared inputs with parent; mark conflicting via option.
	childHash := child.TxIDChainHash()
	_, err = st.Create(ctx, child, 100, utxo.WithConflicting(true))
	require.NoError(t, err)

	// Drain the write-behind projector so the assertion is deterministic.
	require.NoError(t, st.flushPendingUnmined(ctx))

	// Assert conflicting tx is NOT in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`,
		childHash[:],
	).Scan(&exists))
	require.False(t, exists, "conflicting tx must NOT be inserted into pending_unmined")
}

// TestCreateWriteHook_UnminedTx_BatchPath verifies that an unmined tx processed
// via the UNNEST bulk path (sendCreateBatchUNNEST, 2+ items) is also inserted
// into pending_unmined with the correct unmined_since.
func TestCreateWriteHook_UnminedTx_BatchPath(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(200))

	const blockHeight = uint32(200)

	// Build a 2-item batch: both unmined. 2 items forces sendCreateBatchUNNEST.
	items := []*batchCreateItem{
		{
			tx:          makeBenchCreateTx(),
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{},
			done:        make(chan batchCreateResult, 1),
		},
		{
			tx:          makeBenchCreateTx(),
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{},
			done:        make(chan batchCreateResult, 1),
		},
	}

	// Drive via sendCreateBatch (not sendCreateBatchUNNEST directly) so we exercise
	// the same dispatch path as production. With 2 non-conflicting, non-multi-block
	// items, sendCreateBatch routes both to sendCreateBatchUNNEST.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.sendCreateBatch(items)
	}()
	wg.Wait()

	// Drain the write-behind projector so the assertions are deterministic.
	require.NoError(t, st.flushPendingUnmined(ctx))

	for i, item := range items {
		res := <-item.done
		require.NoError(t, res.Err, "batch item %d create must succeed", i)
		require.NotNil(t, res.Data, "batch item %d data must not be nil", i)
		require.Equal(t, blockHeight, res.Data.UnminedSince, "batch item %d UnminedSince", i)

		txHash := item.tx.TxIDChainHash()
		var storedUnminedSince int32
		err := st.pool.QueryRow(ctx,
			`SELECT unmined_since FROM pending_unmined WHERE hash=$1`,
			txHash[:],
		).Scan(&storedUnminedSince)
		require.NoError(t, err, "batch item %d must be in pending_unmined", i)
		require.Equal(t, int32(blockHeight), storedUnminedSince, "batch item %d unmined_since mismatch", i)
	}
}

// TestCreateWriteHook_MinedTx_BatchPath verifies that mined txs processed via
// the UNNEST bulk path produce NO rows in pending_unmined.
func TestCreateWriteHook_MinedTx_BatchPath(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(200))

	const blockHeight = uint32(200)
	blockInfo := utxo.MinedBlockInfo{
		BlockID: 42, BlockHeight: blockHeight, SubtreeIdx: 0, OnLongestChain: true,
	}

	items := []*batchCreateItem{
		{
			tx:          makeBenchCreateTx(),
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{MinedBlockInfos: []utxo.MinedBlockInfo{blockInfo}},
			done:        make(chan batchCreateResult, 1),
		},
		{
			tx:          makeBenchCreateTx(),
			blockHeight: blockHeight,
			options:     &utxo.CreateOptions{MinedBlockInfos: []utxo.MinedBlockInfo{blockInfo}},
			done:        make(chan batchCreateResult, 1),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.sendCreateBatch(items)
	}()
	wg.Wait()

	// Drain the write-behind projector so the negative assertion is meaningful.
	require.NoError(t, st.flushPendingUnmined(ctx))

	for i, item := range items {
		res := <-item.done
		require.NoError(t, res.Err, "batch item %d create must succeed", i)

		txHash := item.tx.TxIDChainHash()
		var exists bool
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`,
			txHash[:],
		).Scan(&exists))
		require.False(t, exists, "mined batch item %d must NOT be in pending_unmined", i)
	}
}

// ---------------------------------------------------------------------------
// Task 6: Write hook tests — reorg + conflicting transitions
// ---------------------------------------------------------------------------

// TestPendingUnmined_ReorgOutInsertsHash verifies that when a tx is fully
// reorged out (unsetMinedMulti removes its only block_id, leaving block_ids
// empty), a row is inserted into pending_unmined with the correct unmined_since
// (blockHeight+1 from the store's current block height at the time of the reorg).
func TestPendingUnmined_ReorgOutInsertsHash(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create a tx, then mine it at blockID=100.
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Precondition: tx is mined, unmined_since must be NULL, and NOT in pending_unmined.
	var unminedSince *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, h[:]).Scan(&unminedSince))
	require.Nil(t, unminedSince, "precondition: mined tx must have unmined_since=NULL")

	var existsBefore bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&existsBefore))
	require.False(t, existsBefore, "precondition: mined tx must NOT be in pending_unmined")

	// Advance block height before unset-mined (matches unsetMinedMulti: currentBlockHeight = Load()+1).
	require.NoError(t, st.SetBlockHeight(150))

	// Reorg: unset mined (removes blockID=100 from block_ids, array becomes empty).
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:    100,
		UnsetMined: true,
	})
	require.NoError(t, err)

	// Assert hash is in pending_unmined with correct unmined_since.
	// unsetMinedMulti uses currentBlockHeight = s.blockHeight.Load() + 1 = 150 + 1 = 151.
	var us int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&us))
	require.Equal(t, int32(151), us, "reorged-out tx must be in pending_unmined with unmined_since=blockHeight+1")
}

// TestPendingUnmined_ReorgOutPartialDoesNotInsert verifies that when a reorg
// only removes ONE of multiple block_ids (partial reorg, block_ids non-empty),
// the tx is NOT inserted into pending_unmined (it is still mined on another chain).
func TestPendingUnmined_ReorgOutPartialDoesNotInsert(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create a tx and mine it on two blocks.
	tx := testExtendedTx(t)
	blockInfo1 := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo1))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Mine on a second block.
	blockInfo2 := utxo.MinedBlockInfo{
		BlockID:        101,
		BlockHeight:    101,
		SubtreeIdx:     0,
		OnLongestChain: false,
	}
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, blockInfo2)
	require.NoError(t, err)

	// Reorg only blockID=100 (blockID=101 remains → block_ids still non-empty).
	require.NoError(t, st.SetBlockHeight(150))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:    100,
		UnsetMined: true,
	})
	require.NoError(t, err)

	// tx still has block_ids=[101] — must NOT be in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "partially-reorged tx must NOT be in pending_unmined (still mined on blockID=101)")
}

// TestPendingUnmined_MarkOnLongestChainTrueDeletes verifies that
// MarkTransactionsOnLongestChain(true) removes a tx hash from pending_unmined
// atomically (U2 site). This simulates a reorg-out followed by re-mine on longest chain.
func TestPendingUnmined_MarkOnLongestChainTrueDeletes(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create an unmined tx so it's in pending_unmined.
	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Precondition: hash is in pending_unmined (Task 4 write hook).
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "precondition: hash must be in pending_unmined after Create (Task 4)")

	// MarkTransactionsOnLongestChain(true): tx is now mined on the longest chain.
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, true))

	// Assert hash is deleted from pending_unmined.
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "MarkTransactionsOnLongestChain(true) must delete from pending_unmined (U2)")

	// Assert unmined_since is NULL in txs.
	var us *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, h[:]).Scan(&us))
	require.Nil(t, us, "txs.unmined_since must be NULL after MarkTransactionsOnLongestChain(true)")
}

// TestPendingUnmined_MarkOnLongestChainFalseInsertsUnmined verifies that
// MarkTransactionsOnLongestChain(false) INSERTS a row into pending_unmined
// (U3 site, into-set direction) for an unmined tx.
// Per the == invariant: after this call txs.unmined_since is non-NULL and the tx
// is not conflicting, so it MUST be present in pending_unmined.
func TestPendingUnmined_MarkOnLongestChainFalseInsertsUnmined(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create + mine a tx (on longest chain, so NOT in pending_unmined initially).
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Precondition: mined tx is NOT in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "precondition: mined tx must NOT be in pending_unmined")

	// Advance block height and call MarkTransactionsOnLongestChain(false).
	// This sets txs.unmined_since = currentBlockHeight (non-NULL) and moves the
	// tx off the longest chain — it must now enter pending_unmined.
	require.NoError(t, st.SetBlockHeight(150))
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, false))

	// Assert hash is PRESENT in pending_unmined with the correct unmined_since (150).
	var storedUnminedSince int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&storedUnminedSince))
	require.Equal(t, int32(150), storedUnminedSince,
		"MarkTransactionsOnLongestChain(false) must INSERT into pending_unmined with correct unmined_since (U3)")

	// Assert txs.unmined_since was also updated.
	var txUnminedSince int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, h[:]).Scan(&txUnminedSince))
	require.Equal(t, int32(150), txUnminedSince, "txs.unmined_since must equal currentBlockHeight after MarkOnLongestChain(false)")
}

// TestPendingUnmined_MarkOnLongestChainFalseInsertsFromMined verifies that
// MarkTransactionsOnLongestChain(false) on a tx that was mined (not previously in
// pending_unmined) inserts it into pending_unmined — the common reorg-off-chain path
// where the tx was mined and is being moved off the longest chain for the first time.
// Per the == invariant: after the call txs.unmined_since is non-NULL and the tx is
// not conflicting, so it MUST be present in pending_unmined.
func TestPendingUnmined_MarkOnLongestChainFalseInsertsFromMined(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create + mine a tx (on longest chain) — no pending_unmined row.
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Precondition: NOT in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "precondition: mined tx must NOT be in pending_unmined")

	// Advance block height and call MarkTransactionsOnLongestChain(false).
	require.NoError(t, st.SetBlockHeight(150))
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, false),
		"MarkTransactionsOnLongestChain(false) must not error (U3 insert)")

	// Now the tx is unmined (unmined_since=150, not conflicting): must be in pending_unmined.
	var storedUnminedSince int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&storedUnminedSince))
	require.Equal(t, int32(150), storedUnminedSince,
		"MarkTransactionsOnLongestChain(false) from mined tx must INSERT into pending_unmined with correct unmined_since")
}

// TestPendingUnmined_SetConflictingTrueDeletes verifies that SetConflicting(true)
// removes a tx hash from pending_unmined. This covers the unmined→conflicting transition
// identified in the Task 4 findings: an unmined tx that is marked conflicting must be
// removed from pending_unmined (it no longer satisfies the invariant).
func TestPendingUnmined_SetConflictingTrueDeletes(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create an unmined tx — Task 4 hook inserts it into pending_unmined.
	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Precondition: hash is in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "precondition: unmined tx must be in pending_unmined (Task 4)")

	// Mark as conflicting (unmined→conflicting transition).
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, true)
	require.NoError(t, err)

	// Assert hash is deleted from pending_unmined (invariant: conflicting txs not in set).
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "SetConflicting(true) must delete from pending_unmined (unmined→conflicting transition)")
}

// TestPendingUnmined_SetConflictingFalseInsertsUnmined verifies that SetConflicting(false)
// on a still-unmined tx leaves/creates its row PRESENT in pending_unmined.
// Per the == invariant: after clearing conflicting on an unmined tx (unmined_since IS NOT NULL),
// it satisfies {unmined AND NOT conflicting} so it MUST be in pending_unmined.
func TestPendingUnmined_SetConflictingFalseInsertsUnmined(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create an unmined tx and DRAIN the projector before the conflicting
	// transition, so the U4-true DELETE sees the row (write-behind ordering:
	// an unflushed create re-inserted after the DELETE would linger — harmless
	// in production because the read filter excludes conflicting rows and lazy
	// cleanup reclaims them, but this test pins the synchronous U4 semantics).
	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()
	require.NoError(t, st.flushPendingUnmined(ctx))

	// First mark it conflicting — this removes it from pending_unmined (U4-true).
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, true)
	require.NoError(t, err)

	// Precondition: hash must NOT be in pending_unmined (was deleted by SetConflicting(true)).
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "precondition: SetConflicting(true) must have removed it from pending_unmined")

	// Precondition: tx is still unmined (unmined_since IS NOT NULL).
	var txUnminedSince *int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM txs WHERE hash=$1`, h[:]).Scan(&txUnminedSince))
	require.NotNil(t, txUnminedSince, "precondition: tx must still be unmined (unmined_since IS NOT NULL)")

	// SetConflicting(false): clears conflicting flag. Tx is still unmined, so it must
	// now be PRESENT in pending_unmined (into-set direction, U4-false).
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, false)
	require.NoError(t, err)

	// Assert hash is PRESENT in pending_unmined with the correct unmined_since.
	var storedUnminedSince int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&storedUnminedSince))
	require.Equal(t, *txUnminedSince, storedUnminedSince,
		"SetConflicting(false) on unmined tx must INSERT/UPSERT into pending_unmined (U4-false)")
}

// TestPendingUnmined_SetConflictingFalseMined_NoRow verifies that SetConflicting(false)
// on a MINED tx (unmined_since IS NULL) does NOT insert into pending_unmined, and
// removes any stale row that may exist.
func TestPendingUnmined_SetConflictingFalseMined_NoRow(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create a mined tx — no pending_unmined row.
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Manually simulate a stale pending_unmined row (e.g., from a prior reorg path
	// that didn't clean up properly).
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, $2)`,
		h[:], int32(50))
	require.NoError(t, err)

	// Precondition: stale row is present.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "precondition: stale pending_unmined row must exist")

	// SetConflicting(false) on the mined tx. unmined_since IS NULL → must delete stale row.
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, false)
	require.NoError(t, err)

	// Assert the stale row is removed (tx is mined, not in the set).
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "SetConflicting(false) on mined tx must delete any stale pending_unmined row")
}

// TestPendingUnmined_MultipleHooksCoexist verifies that pending_unmined is
// maintained consistently across multiple sequential state transitions on a
// single tx: create unmined → mine → reorg out → mark on longest chain again.
func TestPendingUnmined_MultipleHooksCoexist(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// After Create (unmined): must be in pending_unmined.
	require.NoError(t, st.flushPendingUnmined(ctx)) // drain write-behind projector
	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "step 0: unmined tx must be in pending_unmined after Create")

	// Mine at blockID=200.
	require.NoError(t, st.SetBlockHeight(200))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:        200,
		BlockHeight:    200,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// After mine (lever 1): row LINGERS in pending_unmined — the hot-path DELETE was
	// removed from SetMinedMulti. The lazy cleanup (GetPrunableUnminedTxIterator) will
	// remove it on the next pruner cycle. The invariant is maintained by the read-filter
	// (AND t.unmined_since IS NOT NULL) on the pruner iterator.
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "step 1: mined tx's pending_unmined row LINGERS (lever 1: hot-path DELETE removed)")

	// Reorg: unset mined (U1 inserts into pending_unmined).
	require.NoError(t, st.SetBlockHeight(250))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:    200,
		UnsetMined: true,
	})
	require.NoError(t, err)

	// After reorg: back in pending_unmined.
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.True(t, exists, "step 2: reorged-out tx must be back in pending_unmined (U1)")

	// MarkTransactionsOnLongestChain(true): re-mine on longest chain (U2 deletes).
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, true))

	// After MarkOnLongestChain(true): NOT in pending_unmined.
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&exists))
	require.False(t, exists, "step 3: MarkTransactionsOnLongestChain(true) must delete from pending_unmined (U2)")

	// M1: MarkTransactionsOnLongestChain(false) on an unmined tx (U3 inserts).
	// The tx is now mined (unmined_since=NULL from step 3). Calling MarkOnLongestChain(false)
	// sets unmined_since=300 and moves it off the longest chain → must be PRESENT in pending_unmined.
	require.NoError(t, st.SetBlockHeight(300))
	require.NoError(t, st.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, false))

	// After MarkOnLongestChain(false): PRESENT in pending_unmined with correct unmined_since.
	var storedUnminedSince int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&storedUnminedSince))
	require.Equal(t, int32(300), storedUnminedSince,
		"step 4 (M1): MarkTransactionsOnLongestChain(false) on unmined tx must INSERT into pending_unmined (U3)")
}

// ---------------------------------------------------------------------------
// Task 7 (U1 conflicting guard): reorg of a conflicting tx must NOT insert
// into pending_unmined; reorg of a non-conflicting tx still must.
// ---------------------------------------------------------------------------

// TestPendingUnmined_ReorgOutConflicting_NoRow asserts the U1 invariant guard:
// a tx that is CONFLICTING when fully reorged out (block_ids → empty) must NOT
// be inserted into pending_unmined.  The invariant is
//
//	pending_unmined == { (hash, unmined_since) : unmined_since IS NOT NULL AND NOT conflicting }
//
// Before this fix the U1 INSERT was unconditional, so conflicting reorged txs
// would silently diverge from the invariant.
func TestPendingUnmined_ReorgOutConflicting_NoRow(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create a tx and mine it at blockID=100 so unmined_since becomes NULL.
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Mark the tx as conflicting while it is still mined.
	_, _, err = st.SetConflicting(ctx, []chainhash.Hash{*h}, true)
	require.NoError(t, err)

	// Precondition: tx is conflicting AND mined (unmined_since IS NULL) — must NOT be in pending_unmined.
	var existsBefore bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&existsBefore))
	require.False(t, existsBefore, "precondition: conflicting mined tx must NOT be in pending_unmined")

	// Fully reorg the tx out (block_ids → empty).
	require.NoError(t, st.SetBlockHeight(150))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:    100,
		UnsetMined: true,
	})
	require.NoError(t, err)

	// U1 guard: conflicting tx must NOT appear in pending_unmined even though block_ids is now empty.
	var existsAfter bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pending_unmined WHERE hash=$1)`, h[:]).Scan(&existsAfter))
	require.False(t, existsAfter,
		"U1 guard: conflicting tx reorged out must NOT be inserted into pending_unmined")
}

// TestPendingUnmined_ReorgOutNonConflicting_StillInserts confirms that a
// NON-conflicting tx reorged out still gets its pending_unmined row (U1
// happy-path is unaffected by the guard).
func TestPendingUnmined_ReorgOutNonConflicting_StillInserts(t *testing.T) {
	st, ctx := setupTestStore(t)
	require.NoError(t, st.SetBlockHeight(100))

	// Create and mine a non-conflicting tx.
	tx := testExtendedTx(t)
	blockInfo := utxo.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	}
	_, err := st.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	// Fully reorg out.
	require.NoError(t, st.SetBlockHeight(150))
	_, err = st.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{
		BlockID:    100,
		UnsetMined: true,
	})
	require.NoError(t, err)

	// Non-conflicting reorged tx MUST be in pending_unmined with unmined_since=151.
	var us int32
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT unmined_since FROM pending_unmined WHERE hash=$1`, h[:]).Scan(&us))
	require.Equal(t, int32(151), us,
		"U1: non-conflicting reorged tx must be inserted into pending_unmined with unmined_since=blockHeight+1")
}
