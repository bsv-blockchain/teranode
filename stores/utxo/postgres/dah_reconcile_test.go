package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// setSpentBitsRaw forces txs.spent_bits to a wrong value directly, without
// touching the spends table — simulating bitmap corruption (stale-snapshot
// re-set bit, torn write, operator surgery) that the maintained bitmap can never
// self-correct on its own (the fold only ever ORs bits in; only the reconciler
// replaces the bitmap from ground truth).
func setSpentBitsRaw(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx, bits []byte) {
	t.Helper()
	_, err := store.pool.Exec(ctx,
		`UPDATE txs SET spent_bits = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], bits)
	require.NoError(t, err)
}

// setDAHRaw forces txs.delete_at_height directly (nil clears it).
func setDAHRaw(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx, val *int64) {
	t.Helper()
	_, err := store.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $2 WHERE hash = $1`, tx.TxIDChainHash()[:], val)
	require.NoError(t, err)
}

// reconcileAllPartitions runs the rotating-slice audit pass over EVERY partition
// once, using a slice large enough to cover the small test data. Returns the
// total number of drifted rows corrected across all partitions.
func reconcileAllPartitions(t *testing.T, store *Store, ctx context.Context, safeTip int64, slice int) int64 {
	t.Helper()
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive
	var total int64
	for p := 0; p < numPartitions; p++ {
		n, _, err := store.reconcileSpentBitsPartition(ctx, p, safeTip, ret, slice)
		require.NoError(t, err)
		total += n
	}
	return total
}

// TestReconcileHealsDriftUp pins the core self-heal: a maintained spent_bits
// bitmap with a WRONGLY-SET bit (e.g. the fold-vs-Unspend stale-snapshot race
// re-set a just-cleared bit) is corrected back to the true bitmap (recomputed
// from the spends table using the same spendable predicate the fold uses).
func TestReconcileHealsDriftUp(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100) // 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)    // one spendable output spent

	// Fold it: true bitmap = bit 0 only.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent))

	// Corrupt the bitmap UP: wrongly set bit 1 too (spends unchanged).
	setSpentBitsRaw(t, store, ctx, parent, buildSpentBits(2, 0, 1))
	require.Equal(t, 2, spentBitCountOfTx(t, store, ctx, parent), "precondition: wrongly-full bitmap")

	// Reconcile the slice: it must recompute the true bitmap and correct it.
	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Positive(t, corrected, "reconcile must report the drift it corrected")

	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent),
		"reconcile must clear the wrongly-set bit back to the true spendable-spend bitmap")
}

// TestReconcileHealsDriftDown pins the opposite drift: a bitmap with a
// WRONGLY-CLEARED bit (lost update / torn write) is corrected back up to the
// true bitmap.
func TestReconcileHealsDriftDown(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // both spendable outputs spent → true = full

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))

	setSpentBitsRaw(t, store, ctx, parent, buildSpentBits(2, 0)) // drift DOWN: bit 1 lost
	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Positive(t, corrected)

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"reconcile must restore the wrongly-cleared bit from the spends ground truth")
}

// TestReconcileCorrectsLastSpendHeight pins that reconcile also recomputes
// last_spend_height = max(spent_at_height) over the tx's spendable spends.
func TestReconcileCorrectsLastSpendHeight(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(210))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 150)
	_ = spendVoutOwned(t, store, parent, 1, 200) // max spendable spend height = 200

	_, err := procSweepUpTo(store, ctx, 210)
	require.NoError(t, err)

	// Corrupt both the bitmap and last_spend_height.
	setSpentBitsRaw(t, store, ctx, parent, buildSpentBits(2, 0))
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET last_spend_height = 999 WHERE hash = $1`, parent.TxIDChainHash()[:])
	require.NoError(t, err)

	reconcileAllPartitions(t, store, ctx, 210, 10000)

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))
	lsh := lastSpendHeightOfTx(t, store, ctx, parent)
	require.NotNil(t, lsh)
	require.Equal(t, int64(200), *lsh, "reconcile must recompute last_spend_height = max spendable spend height")
}

// TestReconcileCompletesAndStamps pins the drift-that-completes case: a
// fully-spent + mined tx whose spent_bits lost a bit (so it is un-stamped) must,
// after reconcile, be corrected to the full bitmap AND have delete_at_height
// stamped (mirroring the fold's stamp), catching a previously-missed completion.
// It must also be mirrored into pending_deletes.
func TestReconcileCompletesAndStamps(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // mined@100, 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent@101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))
	require.NotNil(t, dahOfTx(t, store, ctx, parent), "precondition: fold stamped it")

	// Corrupt: clear one bit AND clear the stamp, simulating a missed completion
	// (bitmap corruption left it below spendable_count and un-stamped).
	setSpentBitsRaw(t, store, ctx, parent, buildSpentBits(2, 0))
	setDAHRaw(t, store, ctx, parent, nil)
	require.Nil(t, dahOfTx(t, store, ctx, parent), "precondition: un-stamped")

	reconcileAllPartitions(t, store, ctx, 110, 10000)

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"reconcile must restore the full bitmap")
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "reconcile completing a fully-spent mined tx must stamp delete_at_height")
	require.Equal(t, int64(101)+1+ret, *dah,
		"stamped DAH = GREATEST(last_spend_height, mined)+1+retention")

	// Must be mirrored into pending_deletes (the only pruner path).
	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "reconcile stamp must be mirrored into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestReconcileDoesNotStampUnmined pins that reconcile respects the same mined
// gate as the fold: a fully-spent but UNMINED tx has its bitmap corrected to
// full but is NOT stamped (the mine path owns that stamp).
func TestReconcileDoesNotStampUnmined(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(60))

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined
	spendAllOutputs(t, store, tx, 50)

	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, tx))
	require.Nil(t, dahOfTx(t, store, ctx, tx))

	// Corrupt the bitmap to all-zero; reconcile must fix it but leave it unstamped.
	setSpentBitsRaw(t, store, ctx, tx, buildSpentBits(2))
	reconcileAllPartitions(t, store, ctx, 60, 10000)

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, tx),
		"reconcile corrects the bitmap even while unmined")
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"reconcile must NOT stamp an unmined tx (mine path owns that stamp)")
}

// TestReconcileNoDriftIsNoOp pins that a correct bitmap is left untouched and
// reports zero corrections (no spurious writes on the hot table).
func TestReconcileNoDriftIsNoOp(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent))

	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Zero(t, corrected, "a correct bitmap must not be reported as drift")
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent), "unchanged")
}

// TestReconcileBounded pins that a single reconcile call processes AT MOST the
// slice size rows — never O(all txs). With many drifted txs and a tiny slice, one
// pass over a partition corrects at most `slice` of that partition's rows.
func TestReconcileBounded(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec

	// Create 8 fully-spent mined parents and corrupt every bitmap. 0xFF is a
	// recognisable gross corruption (bits beyond the 2 spendable outputs set)
	// distinct from the healed value 0x03, so the drain-down loop below can count
	// still-drifted rows by exact byte value.
	const n = 8
	txs := make([]*bt.Tx, 0, n)
	for i := 0; i < n; i++ {
		tx := newUniqueUnminedTxK(t, store, 2)
		mineTx(t, store, tx, 100)
		spendAllOutputs(t, store, tx, 101)
		txs = append(txs, tx)
	}
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	for _, tx := range txs {
		setSpentBitsRaw(t, store, ctx, tx, []byte{0xFF}) // gross drift on all
	}

	// One reconcile pass with slice=1 per partition corrects at most numPartitions
	// rows total (1 per partition) — far fewer than the 8 drifted rows if they land
	// in fewer partitions. Assert it did NOT correct all of them in one pass.
	corrected := int64(0)
	for p := 0; p < numPartitions; p++ {
		c, _, cerr := store.reconcileSpentBitsPartition(ctx, p, 110, ret, 1)
		require.NoError(t, cerr)
		require.LessOrEqual(t, c, int64(1), "slice=1 must bound corrections to 1 per partition per pass")
		corrected += c
	}
	require.LessOrEqual(t, corrected, int64(numPartitions),
		"one bounded pass must correct at most slice*numPartitions rows, never all history")

	// Draining with repeated bounded passes eventually heals every row.
	for round := 0; round < 100; round++ {
		remaining := 0
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT count(*) FROM txs WHERE spent_bits = '\xff'::bytea`).Scan(&remaining))
		if remaining == 0 {
			break
		}
		for p := 0; p < numPartitions; p++ {
			_, _, cerr := store.reconcileSpentBitsPartition(ctx, p, 110, ret, 1)
			require.NoError(t, cerr)
		}
	}
	var stillDrifted int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM txs WHERE spent_bits = '\xff'::bytea`).Scan(&stillDrifted))
	require.Zero(t, stillDrifted, "repeated bounded passes eventually heal all drift")
}

// TestReconcileUnstampsStaleStampOnNotFullySpent proves the un-stamp defence: the
// reconcile backstop must CLEAR a delete_at_height stamp on a mined,
// non-conflicting, non-preserved tx that is NOT actually fully spent
// (bit_count(true_bits) < spendable_count), and remove it from pending_deletes.
// This is the defense-in-depth against a premature stamp left behind by any
// residual corruption source (a stamp taken from wrongly-full bits that a later
// heal corrected down): the bitmap can already be correct, so the stamp would
// otherwise survive and the pruner would delete a tx with a live UTXO. Without
// the defence the reconcile only ever SETS delete_at_height and never clears
// one, so the stale stamp persists.
func TestReconcileUnstampsStaleStampOnNotFullySpent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100) // mined, 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)    // PARTIAL: vout1 stays a live UTXO

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent), "bitmap is correct at bit 0 only")
	require.Nil(t, dahOfTx(t, store, ctx, parent))

	// Simulate a premature stamp left behind by residual corruption: the bitmap is
	// already the TRUE value, but delete_at_height + pending_deletes carry a stale stamp.
	bad := int64(999)
	setDAHRaw(t, store, ctx, parent, &bad)
	_, err = store.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, $2)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		parent.TxIDChainHash()[:], bad)
	require.NoError(t, err)

	reconcileAllPartitions(t, store, ctx, 110, 10000)

	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"reconcile must un-stamp a not-fully-spent tx (vout1 is a live UTXO)")
	var pd int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Zero(t, pd, "reconcile must remove the un-stamped tx from pending_deletes")
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent), "bitmap stays at the true value")
}

// TestReconcileKeepsConflictingStamp guards the un-stamp defence's scope: it must
// NOT clear a legitimate delete_at_height on a CONFLICTING (double-spend loser)
// tx, which is stamped for deletion regardless of spent-ness. Clearing it would
// resurrect a losing tx.
func TestReconcileKeepsConflictingStamp(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101) // partially spent

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	// Mark it conflicting and stamp it for deletion (the conflicting-loser path).
	bad := int64(999)
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET conflicting = true, delete_at_height = $2 WHERE hash = $1`,
		parent.TxIDChainHash()[:], bad)
	require.NoError(t, err)

	reconcileAllPartitions(t, store, ctx, 110, 10000)

	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "reconcile must NOT clear a conflicting tx's deletion stamp")
	require.Equal(t, bad, *dah)
}

// TestReconcileSkipsWhenPartitionAdvisoryLockHeld pins the deadlock-avoidance gate:
// when the sweep proc holds this partition's advisory xact lock
// (20240684 + partition, the same key the sweep takes per band), the reconcile
// audit statement must skip the partition entirely rather than block on it —
// no error, skipped=true, zero rows corrected, and (critically) the rotating
// cursor left UNTOUCHED so the next tick retries the same slice instead of
// mistaking the skip for "partition exhausted" and wrapping to the head.
func TestReconcileSkipsWhenPartitionAdvisoryLockHeld(t *testing.T) {
	store, ctx := setupTestStore(t)
	const partition = 3

	// Seed a distinct cursor value so we can assert it is untouched on skip.
	_, err := store.pool.Exec(ctx,
		`INSERT INTO dah_reconcile_cursor (partition, last_hash) VALUES ($1, $2)
		 ON CONFLICT (partition) DO UPDATE SET last_hash = EXCLUDED.last_hash`,
		partition, []byte{0xAA, 0xBB})
	require.NoError(t, err)

	// Hold the partition's advisory xact lock from a second session.
	holder, err := store.pool.Acquire(ctx)
	require.NoError(t, err)
	defer holder.Release()
	tx, err := holder.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(20240684 + $1)`, partition)
	require.NoError(t, err)

	// Reconcile must skip: no error, skipped=true, cursor untouched.
	corrected, skipped, err := store.reconcileSpentBitsPartition(ctx, partition, 1000, 1, 100)
	require.NoError(t, err)
	require.True(t, skipped)
	require.Zero(t, corrected)

	var cursor []byte
	err = store.pool.QueryRow(ctx,
		`SELECT last_hash FROM dah_reconcile_cursor WHERE partition = $1`, partition).Scan(&cursor)
	require.NoError(t, err)
	require.Equal(t, []byte{0xAA, 0xBB}, cursor)

	// Release the lock; reconcile must run (skipped=false) and manage the cursor normally.
	require.NoError(t, tx.Rollback(ctx))
	_, skipped, err = store.reconcileSpentBitsPartition(ctx, partition, 1000, 1, 100)
	require.NoError(t, err)
	require.False(t, skipped)
}

// TestDirtyDrainSkipsWhenPartitionAdvisoryLockHeld mirrors the skip gate for the
// dirty-parents drain: when the sweep holds the partition's advisory lock the
// drain must return skipped=true having consumed NOTHING — the queue rows stay
// put for the next tick (a drained-but-unhealed row would lose the heal).
func TestDirtyDrainSkipsWhenPartitionAdvisoryLockHeld(t *testing.T) {
	store, ctx := setupTestStore(t)
	const partition = 4

	// Seed a queue row in the partition directly (hash need not exist in txs for
	// the skip assertion — the gate is checked before anything is dequeued).
	_, err := store.pool.Exec(ctx,
		`INSERT INTO dah_dirty_parents (hash, partition) VALUES ($1, $2)`,
		[]byte{0x01, 0x02, 0x03}, partition)
	require.NoError(t, err)

	holder, err := store.pool.Acquire(ctx)
	require.NoError(t, err)
	defer holder.Release()
	tx, err := holder.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(20240684 + $1)`, partition)
	require.NoError(t, err)

	drained, corrected, skipped, err := store.drainDirtyParentsPartition(ctx, partition, 1000, 1, 100)
	require.NoError(t, err)
	require.True(t, skipped, "drain must skip while the sweep holds the partition")
	require.Zero(t, drained)
	require.Zero(t, corrected)

	var queued int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM dah_dirty_parents WHERE partition = $1`, partition).Scan(&queued))
	require.Equal(t, 1, queued, "the queue row must survive the skip untouched")
}

// TestWronglyFullBitsStampedThenHealedByReconcile is the v15 REPLACEMENT for the
// v13 test TestFullySpentStaysStampedDespiteCounterDrift / the recount-refusal
// premise: there is no stamp-time ground-truth recount anymore, so a bitmap that
// somehow goes WRONGLY FULL (here seeded directly via SQL — in production the
// fold-vs-Unspend stale-snapshot race) WILL be stamped by the bitmap-gated stamp
// sites (S6 at mine, the sweep, preservation expiry). The v15 safety story is
// that the reconciler recomputes the truth from spends and un-stamps + purges
// pending_deletes before the pruner (retention heights away) can act.
func TestWronglyFullBitsStampedThenHealedByReconcile(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(60))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newUniqueUnminedTxK(t, store, 2) // unmined, 2 spendable outputs
	spendVouts(t, store, parent, 50, 0)        // PARTIAL: vout1 stays a live UTXO

	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent))

	// Corrupt the bitmap to wrongly-full, then mine: the S6 bitmap gate trusts the
	// maintained bits (no recount exists in v15) and stamps the live-UTXO tx.
	setSpentBitsRaw(t, store, ctx, parent, buildSpentBits(2, 0, 1))
	mineTx(t, store, parent, 60)

	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "wrongly-full bitmap IS stamped by the bitmap-gated mine path (no recount in v15)")
	require.Equal(t, int64(60)+1+ret, *dah)
	var pd int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Equal(t, 1, pd, "the wrong stamp reaches pending_deletes")

	// The reconciler is the safety net: truth (bit 0 only) < spendable_count →
	// bits corrected, stamp cleared, pending_deletes purged.
	corrected := reconcileAllPartitions(t, store, ctx, 60, 10000)
	require.Positive(t, corrected)

	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent),
		"reconcile must correct the wrongly-full bitmap from spends ground truth")
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"reconcile must un-stamp the tx (vout1 is a live UTXO)")
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Zero(t, pd, "reconcile must purge the wrong stamp from pending_deletes")
}

// TestFullySpentStaysStampedAfterRewindReFold is the v15 translation of
// TestFullySpentStaysStampedDespiteCounterDrift: under v13 a reorg rewind +
// re-fold drifted the counter ABOVE spendable_count and the test proved the
// (correct) stamp survived anyway. Under v15 the drift itself is structurally
// gone — the re-fold re-ORs the same bits — so the assertions strengthen: the
// bitmap is byte-identical after the re-fold, the stamp is unchanged, and the
// reconciler finds NOTHING to correct.
func TestFullySpentStaysStampedAfterRewindReFold(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // mined@100, 2 spendable outputs
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent@101

	// Fold + stamp normally.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "genuinely fully-spent mined tx is stamped")
	require.Equal(t, int64(101)+1+ret, *dah)

	// Reorg rewind below both surviving spends + re-fold: the idempotent OR
	// re-sets the same bits; the delete_at_height IS NULL guard keeps the stamp.
	require.NoError(t, store.RewindDAHWatermark(ctx, 100))
	_, err = procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"rewind re-fold leaves the bitmap byte-identical (no drift class exists)")
	require.NotNil(t, dahOfTx(t, store, ctx, parent), "genuinely fully-spent tx stays stamped")
	require.Equal(t, int64(101)+1+ret, *dahOfTx(t, store, ctx, parent))

	// Nothing drifted, so the reconcile backstop must report zero corrections.
	corrected := reconcileAllPartitions(t, store, ctx, 110, 10000)
	require.Zero(t, corrected, "no drift exists after a rewind re-fold — reconcile has nothing to heal")
	require.NotNil(t, dahOfTx(t, store, ctx, parent), "correct stamp survives reconcile")
}
