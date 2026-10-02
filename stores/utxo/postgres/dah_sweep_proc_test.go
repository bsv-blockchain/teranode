package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// newUniqueUnminedTxK creates an unmined tx with exactly k spendable P2PKH
// outputs (spendable_count = k). Used by the fold-forward proc tests that need a
// k>2 output tx so that spends can be split across bands.
func newUniqueUnminedTxK(t *testing.T, store *Store, k int) *bt.Tx {
	t.Helper()
	ctx := context.Background()
	tx := bt.NewTx()
	for i := 0; i < k; i++ {
		//nolint:gosec // test-only value
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	}
	_, err := store.Create(ctx, tx, 10) // unmined — no WithMinedBlockInfo
	require.NoError(t, err)
	return tx
}

// spendVouts spends exactly the given output indexes of parentTx at spendHeight
// (creating and Spend-ing a single child that consumes those vouts). Unlike
// spendAllOutputs it does not self-assert full spend, so it can be called
// repeatedly to spend different vouts in different bands.
func spendVouts(t *testing.T, store *Store, parentTx *bt.Tx, spendHeight uint32, vouts ...uint32) {
	t.Helper()
	ctx := context.Background()
	child := getSpendingTx(t, parentTx, vouts...)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)
}

// dahOfTx reads txs.delete_at_height for tx (nil when unstamped).
func dahOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) *int64 {
	t.Helper()
	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
	return dah
}

// partitionOfTx returns the hash-partition leaf number (the NN of txs_pNN) the
// tx's row lives in. txs and spends are hash-partitioned on the same key value
// (the parent hash), so the parent's spends rows colocate in spends_pNN with
// the same NN — which is what lets a test drive sweepOnePartition for exactly
// the partition holding its fixture.
func partitionOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()
	var p int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT substring(tableoid::regclass::text from '(\d+)$')::int FROM txs WHERE hash = $1`,
		tx.TxIDChainHash()[:]).Scan(&p))
	return p
}

// watermarkOfPartition reads one partition's sweep watermark.
func watermarkOfPartition(t *testing.T, store *Store, ctx context.Context, partition int) int64 {
	t.Helper()
	var wm int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT last_swept_height FROM dah_part_watermark WHERE partition = $1`, partition).Scan(&wm))
	return wm
}

// minWatermark reads the lowest per-partition watermark.
func minWatermark(t *testing.T, store *Store, ctx context.Context) int64 {
	t.Helper()
	var wm int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT COALESCE(MIN(last_swept_height), 0) FROM dah_part_watermark`).Scan(&wm))
	return wm
}

// TestDAHProc_FoldStampsOnCompletion is the core fold-forward test: a k=3
// spendable mined tx with 2 outputs spent in band1 and the 3rd in band2. After a
// sweep whose band ends before band2's spend height, only 2 of 3 bits are
// folded (bit_count=2) so NO stamp fires. After a second sweep whose band
// covers band2, the 3rd fold completes (bit_count=3=spendable_count) and the
// stamp fires with delete_at_height = GREATEST(maxSpendHeight, minedHeight)+1+ret.
// The watermark must advance past both bands.
func TestDAHProc_FoldStampsOnCompletion(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Force a small, deterministic band so band1 (up to height 150) excludes the
	// band2 spend at height 200. band_heights=100 → first band is (0,100], but we
	// sweep incrementally by calling with a rising safe_tip below.
	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET band_heights = 50 WHERE id = 1`)
	require.NoError(t, err)

	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTxK(t, store, 3)
	mineTx(t, store, tx, 100)           // mined_at_height = 100
	spendVouts(t, store, tx, 150, 0, 1) // band1: 2 of 3 outputs spent at 150
	spendVouts(t, store, tx, 200, 2)    // band2: last output spent at 200

	// --- Sweep 1: safe_tip = 150, so only band1's spends are folded. ---
	require.NoError(t, store.SetBlockHeight(150))
	store.sweepAllPartitionsOnce(ctx, 150, int32(ret)) //nolint:gosec

	require.Equal(t, buildSpentBits(3, 0, 1), spentBitsOfTx(t, store, ctx, tx), "band1: 2 of 3 spendable outputs' bits folded")
	require.Nil(t, dahOfTx(t, store, ctx, tx), "partial fold (2/3) must NOT stamp")
	require.Equal(t, int64(150), minWatermark(t, store, ctx), "watermark advanced to safe_tip after band1")

	// --- Sweep 2: safe_tip = 200, band2's spend at 200 is now folded → complete. ---
	require.NoError(t, store.SetBlockHeight(200))
	store.sweepAllPartitionsOnce(ctx, 200, int32(ret)) //nolint:gosec

	require.Equal(t, buildSpentBits(3, 0, 1, 2), spentBitsOfTx(t, store, ctx, tx), "band2: all 3 spendable outputs' bits folded")
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "complete fold (3/3) of a mined tx must stamp")
	// GREATEST(maxSpendHeight=200, minedHeight=100) = 200
	require.Equal(t, int64(200)+1+ret, *dah, "DAH = GREATEST(lastSpendHeight, minedHeight)+1+retention")
	require.Equal(t, int64(200), minWatermark(t, store, ctx), "watermark advanced past both bands")

	// The stamp must also be mirrored into pending_deletes.
	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "stamp must be upserted into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestDAHProc_SpentBeforeMinedNotStampedByProc pins the proc's division of labour
// for the spent-before-mined ordering: outputs are all spent while the tx is
// UNMINED. The fold-forward proc folds the spent_bits bitmap but must NOT stamp
// delete_at_height while the tx is unmined (mined_at_height IS NULL / unmined_since
// set). Stamping the spent-before-mined tx is the mine path's job (Task 7 /
// SetMinedMulti), not the sweep's. This test asserts ONLY the proc's half: it
// leaves the unmined tx unstamped even after folding every spendable output.
func TestDAHProc_SpentBeforeMinedNotStampedByProc(t *testing.T) {
	store, ctx := setupTestStore(t)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined
	spendAllOutputs(t, store, tx, 50)      // all outputs spent while UNMINED

	require.NoError(t, store.SetBlockHeight(60))
	store.sweepAllPartitionsOnce(ctx, 60, ret)

	// The proc folds the spends (bit_count reaches spendable_count) but the mined
	// gate keeps it unstamped while unmined.
	require.Equal(t, 2, spentBitCountOfTx(t, store, ctx, tx), "fold completes the bitmap while unmined")
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"proc must NOT stamp a fully-spent-but-unmined tx (mine path owns that stamp)")

	// Now mine it: SetMinedMulti (the mine path) stamps the fully-spent tx.
	mineTx(t, store, tx, 60)
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "mine path must stamp the fully-spent tx on SetMinedMulti")
	require.Equal(t, int64(60)+1+int64(ret), *dah, "DAH = GREATEST(spend=50, mined=60)+1+retention")
}

// TestDAHProc_BoundedWorkAdvancesIncrementally pins that the proc advances the
// watermark in bounded bands rather than requiring the whole (0, safe_tip] range
// to drain in one CALL. With a tiny band and a max-bands-per-call budget smaller
// than the number of bands needed to reach safe_tip, a single CALL advances the
// watermark only by budget*band — proving per-call work is bounded and the
// watermark moves forward incrementally.
func TestDAHProc_BoundedWorkAdvancesIncrementally(t *testing.T) {
	store, ctx := setupTestStore(t)

	// band_heights=10, max 3 bands per CALL → one CALL advances at most 30 heights.
	_, err := store.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET band_heights = 10, max_windows_per_call = 3 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(1000)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	// One CALL per partition, single pass: watermark advances by exactly budget*band.
	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	wm := minWatermark(t, store, ctx)
	require.Equal(t, int64(30), wm,
		"one CALL advances at most max_bands*band = 3*10 = 30 heights (bounded per call)")
	require.Less(t, wm, safeTip, "the whole range must NOT drain in one CALL")

	// Repeated CALLs keep advancing forward until the safe_tip is reached — the
	// background driver loops CALLs while backlog>0, so a bounded number of CALLs
	// reaches the tip.
	for i := 0; i < 40 && minWatermark(t, store, ctx) < safeTip; i++ {
		store.sweepAllPartitionsOnce(ctx, safeTip, ret)
	}
	require.Equal(t, safeTip, minWatermark(t, store, ctx),
		"repeated bounded CALLs advance the watermark forward to safe_tip")
}

// TestDAHSweepProcBootstrap verifies the procedure is installed by store.New and
// that the control row records the version + seeded knobs, and that re-bootstrap
// is idempotent.
func TestDAHSweepProcBootstrap(t *testing.T) {
	store, ctx := setupTestStore(t) // store.New already bootstrapped the procedure

	var procCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_proc WHERE proname = 'dah_sweep_batch'`).Scan(&procCount))
	require.Equal(t, 1, procCount, "dah_sweep_batch procedure must be installed by store.New")

	var version, batchRows, maxWin, bandRows, workMemMB int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT proc_version, batch_rows, max_windows_per_call, band_rows, work_mem_mb FROM dah_sweep_control WHERE id = 1`,
	).Scan(&version, &batchRows, &maxWin, &bandRows, &workMemMB))
	require.Equal(t, dahSweepProcVersion, version)
	require.Positive(t, batchRows)
	require.Positive(t, maxWin)
	require.Positive(t, bandRows, "v15 band_rows knob must be seeded")
	require.Positive(t, workMemMB, "v15 work_mem_mb knob must be seeded")

	// Idempotent: a second bootstrap is a no-op (version already current).
	require.NoError(t, store.bootstrapDAHSweepProc(ctx))
}

// TestDAHSweepProcStampsExpectedDAH pins the procedure's DAH semantics against the
// spec directly (mined+fully-spent → stamp completion+1+retention; partially-spent
// → NULL; unmined → NULL) and that it advances the watermark. This is the proc's
// behavioural contract now that it is the only sweep mechanism.
func TestDAHSweepProcStampsExpectedDAH(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(105))

	const safeTip = int64(105)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	fullySpent := newUniqueUnminedTx(t, store)
	mineTx(t, store, fullySpent, 100)
	spendAllOutputs(t, store, fullySpent, 101) // mined@100, fully spent@101 → stamp

	partiallySpent := newUniqueUnminedTx(t, store)
	mineTx(t, store, partiallySpent, 100)
	spendOneOutput(t, store, partiallySpent, 0, 101) // partially spent → NULL

	unmined := newUniqueUnminedTx(t, store) // unmined → NULL (unmined_since guard)

	// Drive all 8 partition CALLs in parallel (the production path).
	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec // small positive

	dahOf := func(tx *bt.Tx) *int64 {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		return dah
	}

	// completion_height = GREATEST(max spent_at_height=101, mined_at_height=100) = 101
	fsDAH := dahOf(fullySpent)
	require.NotNil(t, fsDAH, "fully-spent mined parent must be stamped")
	require.Equal(t, int64(101)+1+ret, *fsDAH, "DAH must be completion+1+retention")

	require.Nil(t, dahOf(partiallySpent), "partially-spent parent must stay NULL")
	require.Nil(t, dahOf(unmined), "unmined tx must stay NULL")

	var watermark int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&watermark))
	require.Equal(t, safeTip, watermark, "every partition's watermark must reach the safe tip")
}

// TestDAHSweepProcDrainsWideRangeInOneCall pins that a single CALL drains the
// entire (watermark, safe_tip] range regardless of height span — the row-targeted
// proc has no per-CALL window cap. Red against the old window-capped proc.
func TestDAHSweepProcDrainsWideRangeInOneCall(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Force the old proc's worst case: at most one window per CALL.
	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET max_windows_per_call = 1, batch_rows = 1000 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(2000)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// Fully-spent mined parents spread across heights far wider than one 256-window.
	heights := []int64{100, 400, 800, 1200, 1600, 1900}
	txs := make([]*bt.Tx, 0, len(heights))
	for _, h := range heights {
		tx := newUniqueUnminedTx(t, store)
		mineTx(t, store, tx, uint32(h-1))        //nolint:gosec // small positive height
		spendAllOutputs(t, store, tx, uint32(h)) //nolint:gosec // small positive height
		txs = append(txs, tx)
	}

	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec

	for i, tx := range txs {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		require.NotNilf(t, dah, "parent at height %d must be stamped in one CALL", heights[i])
		require.Equal(t, heights[i]+1+ret, *dah)
	}

	var minWM int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&minWM))
	require.Equal(t, safeTip, minWM, "watermark must reach safe tip in one CALL across the wide range")
}

// TestDAHSweepProcDenseHeightMultiPass pins that a single height holding more
// fully-spent parents than batch_rows is fully drained by repeated bounded passes
// within one CALL, and the watermark advances to safe_tip.
func TestDAHSweepProcDenseHeightMultiPass(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx, `UPDATE dah_sweep_control SET batch_rows = 2 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(110)
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	// 7 fully-spent mined parents ALL at the same spend height (105) — well over batch_rows=2.
	txs := make([]*bt.Tx, 0, 7)
	for i := 0; i < 7; i++ {
		tx := newUniqueUnminedTx(t, store)
		mineTx(t, store, tx, 104)
		spendAllOutputs(t, store, tx, 105)
		txs = append(txs, tx)
	}

	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec

	for _, tx := range txs {
		var dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
		require.NotNil(t, dah, "every fully-spent parent at the dense height must be stamped")
		require.Equal(t, int64(105)+1+ret, *dah)
	}

	var minWM int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&minWM))
	require.Equal(t, safeTip, minWM, "watermark must reach safe tip after dense-height multipass")
}

// TestDAHSweepProcRewindRerunIsResultIdempotent verifies FULL fold idempotence
// under a watermark rewind (v15): rewinding the watermark and re-sweeping the
// same range must leave spent_bits BYTE-IDENTICAL (duplicate ORs are no-ops by
// construction — unlike the v13 counter, which double-counted and relied on a
// stamp-time recount), the stamp unchanged (no double-stamp: the
// delete_at_height IS NULL guard keeps the original value), and the
// pending_deletes feed unchanged (still exactly one row, same value).
func TestDAHSweepProcRewindRerunIsResultIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(uint32(110)))
	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTx(t, store)
	mineTx(t, store, tx, 104)
	spendAllOutputs(t, store, tx, 105)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	firstDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, firstDAH, "fully-spent mined parent stamped on first sweep")
	firstBits := spentBitsOfTx(t, store, ctx, tx)
	require.Equal(t, buildSpentBits(2, 0, 1), firstBits, "both bits folded on first sweep")

	readPD := func() (int, int64) {
		var n int
		var v int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT count(*), COALESCE(min(delete_at_height), 0) FROM pending_deletes WHERE hash = $1`,
			tx.TxIDChainHash()[:]).Scan(&n, &v))
		return n, v
	}
	pdCount, pdDAH := readPD()
	require.Equal(t, 1, pdCount, "stamp mirrored into pending_deletes once")
	require.Equal(t, *firstDAH, pdDAH)

	// Rewind the watermark so the same (surviving) range is re-swept, then sweep again.
	require.NoError(t, store.RewindDAHWatermark(ctx, 0))
	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	require.Equal(t, firstBits, spentBitsOfTx(t, store, ctx, tx),
		"re-folding the same spends must leave spent_bits byte-identical (idempotent OR)")

	secondDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, secondDAH, "genuinely fully-spent parent stays stamped after re-sweep")
	require.Equal(t, *firstDAH, *secondDAH, "rewind+re-sweep must yield the identical delete_at_height (no double-stamp)")

	pdCount2, pdDAH2 := readPD()
	require.Equal(t, 1, pdCount2, "pending_deletes must still hold exactly one row for the tx")
	require.Equal(t, pdDAH, pdDAH2, "pending_deletes value unchanged by the re-sweep")
}

// TestDAHSweepProcSkipsWatermarkOnLockContention verifies that when the per-partition
// advisory lock is held elsewhere, the CALL does NOT advance the watermark (which would
// silently skip the unswept range). It then releases the lock and confirms a normal sweep
// drains and advances.
func TestDAHSweepProcSkipsWatermarkOnLockContention(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	tx := newUniqueUnminedTx(t, store)
	mineTx(t, store, tx, 104)
	spendAllOutputs(t, store, tx, 105)

	// Hold all 8 partition advisory locks in a separate connection's open transaction
	// so every dah_sweep_batch CALL misses pg_try_advisory_xact_lock.
	holder, err := store.pool.Acquire(ctx)
	require.NoError(t, err)
	holderTx, err := holder.Begin(ctx)
	require.NoError(t, err)
	_, err = holderTx.Exec(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock(20240684 + g) FROM generate_series(0,%d) g`, numPartitions-1))
	require.NoError(t, err)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	var wmDuringContention int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MAX(last_swept_height) FROM dah_part_watermark`).Scan(&wmDuringContention))
	require.Equal(t, int64(0), wmDuringContention, "watermark must NOT advance while the lock is held elsewhere")

	// Release the locks, sweep again: now it drains and advances.
	require.NoError(t, holderTx.Rollback(ctx))
	holder.Release()

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	var dah *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&dah))
	require.NotNil(t, dah, "after lock release the fully-spent parent must be stamped")

	var wmAfter int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MIN(last_swept_height) FROM dah_part_watermark`).Scan(&wmAfter))
	require.Equal(t, safeTip, wmAfter, "after lock release the watermark must reach safe tip")
}

// TestBootstrapInstallsV17SpentBitmapProc verifies that the bootstrapped
// procedure is the v17 spent-bitmap fold: version recorded as 17, stamp gate on
// bit_count(spent_bits), no spent_progress counter reference anywhere, and (kept
// from v14) no in-proc lock_timeout. v17 adds the p_checkpoint boundary parameter
// (below-checkpoint immediate DAH) without touching the bitmap fold.
func TestBootstrapInstallsV17SpentBitmapProc(t *testing.T) {
	store, ctx := setupTestStore(t)

	var version int
	err := store.pool.QueryRow(ctx,
		`SELECT proc_version FROM dah_sweep_control WHERE id = 1`).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, dahSweepProcVersion, version)
	require.Equal(t, 17, version)

	var src string
	err = store.pool.QueryRow(ctx,
		`SELECT prosrc FROM pg_proc WHERE proname = 'dah_sweep_batch'`).Scan(&src)
	require.NoError(t, err)
	require.NotContains(t, src, "lock_timeout")
	require.Contains(t, src, "bit_count", "v15 stamp gate must be the bitmap bit_count")
	require.NotContains(t, src, "spent_progress", "the v13 counter must be gone from the proc")
}

func TestSweepOnePartitionRunsWithoutDeadline(t *testing.T) {
	store, ctx := setupTestStore(t)

	// safeTip=-1 is the documented no-op smoke value: the CALL returns immediately.
	// The contract under test: sweepOnePartition exists, takes NO timeout, and
	// surfaces the CALL error verbatim (nil here).
	err := store.sweepOnePartition(ctx, 0, -1, 0, 0)
	require.NoError(t, err)
}

func TestPgSQLStateExtractsCode(t *testing.T) {
	require.Equal(t, "", pgSQLState(nil))
	require.Equal(t, "", pgSQLState(errors.NewProcessingError("not a pg error")))

	pgErr := &pgconn.PgError{Code: "40P01"}
	require.Equal(t, "40P01", pgSQLState(pgErr))
}

// TestDAHProc_RowBoundedBandBoundary pins the v15 work-quantised band: when
// band_rows truncates the band MID-HEIGHT, the watermark must land at max_h - 1
// (the last FULLY covered height) and the next band must re-fold the boundary
// height to convergence — no drift (duplicate ORs are no-ops) and no missed
// spends.
//
// Fixture: one parent (k=5 spendable outputs, all its spends colocate in one
// spends_pNN leaf) with 3 spends at height 100 and 2 at height 101, band_rows=4:
// the first band takes 3@100 + 1@101 (row-truncated at max_h=101) → watermark
// 100; the second band re-folds height 101 (one duplicate OR + the missed
// spend) and completes the bitmap.
func TestDAHProc_RowBoundedBandBoundary(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET band_rows = 4, max_windows_per_call = 1, band_heights = 5000 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive

	parent := newUniqueUnminedTxK(t, store, 5)
	mineTx(t, store, parent, 90)
	spendVouts(t, store, parent, 100, 0, 1, 2) // 3 spends at height 100
	spendVouts(t, store, parent, 101, 3, 4)    // 2 spends at height 101
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))

	part := partitionOfTx(t, store, ctx, parent)

	// Band 1: 4 of the 5 spends (3@100 + one of the two @101) → row-truncated.
	// Watermark must land at max_h - 1 = 100, NOT at the band cap.
	require.NoError(t, store.sweepOnePartition(ctx, part, safeTip, ret, 0))
	require.Equal(t, int64(100), watermarkOfPartition(t, store, ctx, part),
		"row-truncated band must advance the watermark to max_h - 1 (boundary height re-folds next band)")
	require.Equal(t, 4, spentBitCountOfTx(t, store, ctx, parent), "4 of 5 bits folded in band 1")
	require.Nil(t, dahOfTx(t, store, ctx, parent), "partial bitmap must not stamp")

	// Band 2: re-folds the boundary height 101 (2 spends, one a duplicate OR) →
	// converges with no drift and no missed spend, and drains to the safe tip.
	require.NoError(t, store.sweepOnePartition(ctx, part, safeTip, ret, 0))
	require.Equal(t, safeTip, watermarkOfPartition(t, store, ctx, part),
		"boundary re-fold band must reach the safe tip")
	require.Equal(t, buildSpentBits(5, 0, 1, 2, 3, 4), spentBitsOfTx(t, store, ctx, parent),
		"boundary height re-fold must converge: every spend folded exactly once, no drift")

	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "completed bitmap on a mined tx must stamp")
	ret64 := int64(ret)
	require.Equal(t, int64(101)+1+ret64, *dah, "DAH = GREATEST(lastSpend=101, mined=90)+1+retention")
}

// TestDAHProc_SingleHeightFullDrain pins the dense-height overshoot rule: a
// single height holding MORE spends than band_rows is full-drained as ONE band
// (the no-row-cap re-run), with the watermark landing exactly on that height.
//
// Fixture: one parent (k=5) fully spent at height 100, band_rows=3, one band
// per CALL. CALL 1 row-truncates at max_h=100 with max_h-1 (99) > watermark (0),
// so it advances to 99 without covering the height. CALL 2 truncates again but
// now max_h-1 = 99 = watermark → the single dense height is re-run with NO row
// cap: all 5 spends fold in one band and the watermark lands at 100.
func TestDAHProc_SingleHeightFullDrain(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET band_rows = 3, max_windows_per_call = 1, band_heights = 5000 WHERE id = 1`)
	require.NoError(t, err)

	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive

	parent := newUniqueUnminedTxK(t, store, 5)
	mineTx(t, store, parent, 90)
	spendVouts(t, store, parent, 100, 0, 1, 2, 3, 4) // ALL 5 spends at ONE height > band_rows=3
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))

	part := partitionOfTx(t, store, ctx, parent)

	// CALL 1: row-truncated at max_h=100 → watermark advances to max_h-1 = 99.
	require.NoError(t, store.sweepOnePartition(ctx, part, safeTip, ret, 0))
	require.Equal(t, int64(99), watermarkOfPartition(t, store, ctx, part))
	require.Nil(t, dahOfTx(t, store, ctx, parent))

	// CALL 2: the dense height is now the boundary (max_h-1 = watermark) → full
	// drain of height 100 in ONE band, watermark = that height, bitmap complete,
	// stamp fires.
	require.NoError(t, store.sweepOnePartition(ctx, part, safeTip, ret, 0))
	require.Equal(t, int64(100), watermarkOfPartition(t, store, ctx, part),
		"single dense height must be full-drained as one band with the watermark landing on it")
	require.Equal(t, buildSpentBits(5, 0, 1, 2, 3, 4), spentBitsOfTx(t, store, ctx, parent),
		"the whole height's spends must fold in the one uncapped band")

	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "fully-spent mined parent must be stamped by the full-drain band")
	require.Equal(t, int64(100)+1+int64(ret), *dah)

	// CALL 3: the remaining empty range drains to the safe tip.
	require.NoError(t, store.sweepOnePartition(ctx, part, safeTip, ret, 0))
	require.Equal(t, safeTip, watermarkOfPartition(t, store, ctx, part))
}

// TestDAHProc_RewoundWatermarkRestartsFold is the pragmatic observable-behaviour
// test for the CAS watermark advance (WHERE last_swept_height = v_from): a true
// mid-CALL rewind cannot be orchestrated from outside a CALL, so this pins the
// property the CAS exists to guarantee — after RewindDAHWatermark the next CALL
// folds FROM THE REWOUND VALUE (it can never trust a previously-read watermark
// and skip the rewound range), and the re-fold converges with zero drift and no
// double-stamp.
func TestDAHProc_RewoundWatermarkRestartsFold(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(uint32(110)))
	const safeTip = int64(110)
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive

	tx := newUniqueUnminedTx(t, store)
	mineTx(t, store, tx, 104)
	spendAllOutputs(t, store, tx, 105)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)
	require.Equal(t, safeTip, minWatermark(t, store, ctx))
	firstDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, firstDAH)
	firstBits := spentBitsOfTx(t, store, ctx, tx)

	// Reorg-style rewind, then constrain the next CALL to a single small band so
	// the restart point is observable: after ONE band per partition the watermark
	// must sit at rewind + band_heights (folded from 0), not anywhere near the
	// pre-rewind 110.
	require.NoError(t, store.RewindDAHWatermark(ctx, 0))
	_, err := store.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET band_heights = 50, max_windows_per_call = 1 WHERE id = 1`)
	require.NoError(t, err)

	store.sweepAllPartitionsOnce(ctx, safeTip, ret)

	var maxWM int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT MAX(last_swept_height) FROM dah_part_watermark`).Scan(&maxWM))
	require.Equal(t, int64(50), maxWM,
		"the CALL must restart folding from the REWOUND watermark (one 50-height band from 0), not resume past it")

	// Re-fold to the tip: converges, byte-identical bits, same stamp.
	for i := 0; i < 20 && minWatermark(t, store, ctx) < safeTip; i++ {
		store.sweepAllPartitionsOnce(ctx, safeTip, ret)
	}
	require.Equal(t, safeTip, minWatermark(t, store, ctx))
	require.Equal(t, firstBits, spentBitsOfTx(t, store, ctx, tx), "re-fold of the rewound range must produce zero drift")
	secondDAH := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, secondDAH)
	require.Equal(t, *firstDAH, *secondDAH, "no double-stamp after the rewound re-fold")
}

func TestDAHPartitionBacklog(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.pool.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = 100 WHERE partition = 2`)
	require.NoError(t, err)

	backlog, err := store.dahPartitionBacklog(ctx, 2, 150)
	require.NoError(t, err)
	require.Equal(t, int64(50), backlog)

	backlog, err = store.dahPartitionBacklog(ctx, 2, 100)
	require.NoError(t, err)
	require.Zero(t, backlog)

	// Missing watermark row must be an ERROR (not silently "caught up") — the
	// old dahWatermarkBacklog returned 0 on any error, which idled the drain.
	_, err = store.dahPartitionBacklog(ctx, 99, 150)
	require.Error(t, err)
}
