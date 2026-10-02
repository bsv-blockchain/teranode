package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// spentBitsOfTx reads txs.spent_bits for tx (the v15 per-output spent bitmap).
func spentBitsOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) []byte {
	t.Helper()
	var b []byte
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spent_bits FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&b))
	return b
}

// spentBitCountOfTx reads bit_count(txs.spent_bits) — how many spendable
// outputs the fold has recorded as spent. This is the v15 replacement for the
// old spent_progress counter reads (PG14+, matching the proc's stamp gate).
func spentBitCountOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()
	var n int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT bit_count(spent_bits) FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&n))
	return n
}

// lastSpendHeightOfTx reads txs.last_spend_height for tx (nil when never folded).
func lastSpendHeightOfTx(t *testing.T, store *Store, ctx context.Context, tx *bt.Tx) *int64 {
	t.Helper()
	var h *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT last_spend_height FROM txs WHERE hash = $1`, tx.TxIDChainHash()[:]).Scan(&h))
	return h
}

// buildSpentBits builds a spent_bits value of (outCount+7)/8 bytes with the
// given vout bits set, using the store's LSB-first encoding
// (buf[i/8] |= 1 << (i%8), matching postgres get_bit / out_spendables).
func buildSpentBits(outCount int, vouts ...uint32) []byte {
	buf := make([]byte, (outCount+7)/8)
	for _, v := range vouts {
		buf[v/8] |= 1 << (v % 8)
	}
	return buf
}

// spendVoutOwned spends exactly one output (vout) of parentTx at spendHeight and
// returns the *utxo.Spend (carrying the stored spending_data ownership token) so a
// test can later Unspend that exact spend. Mirrors spendOneOutput but recovers the
// ownership token from the spends row it just created.
func spendVoutOwned(t *testing.T, store *Store, parentTx *bt.Tx, vout uint32, spendHeight uint32) *utxo.Spend {
	t.Helper()
	ctx := context.Background()

	child := getSpendingTx(t, parentTx, vout)
	_, err := store.Create(ctx, child, spendHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, spendHeight)
	require.NoError(t, err)

	parentHash := parentTx.TxIDChainHash()
	var sdBytes []byte
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spending_data FROM spends WHERE prev_tx_hash=$1 AND prev_output_idx=$2`,
		parentHash[:], int32(vout)).Scan(&sdBytes))
	sd, sdErr := spendpkg.NewSpendingDataFromBytes(sdBytes)
	require.NoError(t, sdErr)

	return &utxo.Spend{TxID: parentHash, Vout: vout, SpendingData: sd}
}

// TestUnspendClearsExactlyOwnedBits is the core reorg-bitmap test (translated
// from the v13 counter test TestUnspendDecrementsSpentProgressExactlyOne):
// a 2-spendable-output mined tx spent to completion (spent_bits folds to full and
// the sweep stamps delete_at_height). Unspending ONE spendable output must:
//   - clear EXACTLY that output's bit (vout0 cleared, vout1 still set), and
//   - clear delete_at_height (the tx is no longer fully spent → not prune-eligible).
//
// Re-spending that output at a NEW height must let the fold reconverge (bitmap back
// to full) and the sweep re-stamp with the new completion height. Unlike the v13
// counter, no watermark gymnastics are needed to avoid double-counting: the fold's
// OR is idempotent, so the surviving vout1 spend can be re-folded harmlessly.
func TestUnspendClearsExactlyOwnedBits(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100) // pre-mined, exactly 2 spendable outputs
	sp0 := spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // now fully spent at 101

	// Sweep folds both spends and stamps the fully-spent mined parent.
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"both spendable outputs' bits folded")
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before unspend")
	require.Equal(t, int64(101)+1+ret, *dah)

	// Unspend ONE spendable output.
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{sp0}))

	require.Equal(t, buildSpentBits(2, 1), spentBitsOfTx(t, store, ctx, parent),
		"Unspend of one owned spendable output must clear exactly that bit (vout0), leaving vout1 set")
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"Unspend must clear delete_at_height (no longer fully spent)")

	// pending_deletes must also be cleared for the revived parent.
	var pdCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pdCount))
	require.Zero(t, pdCount, "revived parent must be removed from pending_deletes")

	// Re-spend the same output at a NEW height; the fold must reconverge and
	// re-stamp. The watermark already sits at 110 from the first sweep, so only
	// the new spend at 200 is in the forward range — but even a full rewind
	// would be safe now (duplicate ORs are no-ops, unlike the v13 counter).
	require.NoError(t, store.SetBlockHeight(210))
	_ = spendVoutOwned(t, store, parent, 0, 200)

	_, err = procSweepUpTo(store, ctx, 210)
	require.NoError(t, err)

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"re-spend must let the fold reconverge to the full bitmap")
	dah2 := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah2, "reconverged fully-spent mined parent must be re-stamped")
	// GREATEST(lastSpendHeight, minedHeight)+1+ret. last_spend_height is the
	// running max of the folded spends: 101 (vout1, still present) and 200
	// (re-spent vout0) → 200. minedHeight is NULL for a pre-mined tx.
	require.Equal(t, int64(200)+1+ret, *dah2,
		"re-stamp uses the new completion height (max folded spend)")
}

// TestRewindReFoldProducesNoDrift is the v15 translation of the v13 test
// TestFoldStampRefusesDriftedCounter (the IBD data-loss wedge: Hetzner mainnet
// h63266 / testnet …5e5ea, 2026-07-02).
//
// Under v13's arithmetic counter, a reorg watermark rewind + re-fold DOUBLE-COUNTED
// the surviving spend (1 → 2 = spendable_count) and only a stamp-time ground-truth
// recount stopped the false stamp. Under v15's bitmap there is NO recount anymore —
// instead the drift class is structurally gone: re-folding the surviving spend re-ORs
// the SAME bit, so the bitmap is byte-identical after the re-fold, the stamp gate
// (bit_count = spendable_count) still correctly refuses, and nothing is queued for
// deletion. The companion safety test for bits that somehow DO go wrongly full is
// TestWronglyFullBitsStampedThenHealedByReconcile (dah_reconcile_test.go).
func TestRewindReFoldProducesNoDrift(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	// Unique tx (not the shared fixed-hash helper) so the pending_deletes assertion below
	// cannot collide with a stale row left by another test (pending_deletes is keyed by hash
	// and is not truncated between tests).
	parent := newUniqueUnminedTxK(t, store, 2) // unique hash, exactly 2 spendable outputs
	mineTx(t, store, parent, 100)              // mined (so the DAH stamp's mined gate can pass)
	spendVouts(t, store, parent, 101, 0)       // PARTIAL: only vout0 spent; vout1 stays a live UTXO

	// First fold: bit 0 set, NOT stamped (partial).
	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent), "one spendable output folded")
	require.Nil(t, dahOfTx(t, store, ctx, parent), "partially-spent tx must not be stamped")

	// Reorg: rewind the watermark BELOW the surviving vout0 spend (height 101) and re-fold.
	require.NoError(t, store.RewindDAHWatermark(ctx, 100))
	_, err = procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)

	// The idempotent OR leaves the bitmap byte-identical: no drift exists to mis-stamp from.
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent),
		"re-folding the surviving spend re-ORs the same bit — bitmap identical, no drift by construction")
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"stamp gate (bit_count=1 < spendable_count=2) must still refuse; vout1 is a live UTXO")
	var pd int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Zero(t, pd, "a not-fully-spent tx must never be queued for deletion")
}

// TestUnspendNonOwningDoesNotClearBits pins that a non-owning (mismatched
// SpendingData) Unspend deletes 0 spend rows and therefore clears NO spent_bits
// bits — the live spend row and its bit are untouched. Wrongly clearing here
// would resurrect a spent output's bit and defer pruning; wrongly deleting the
// live spend row would lose the true spender. However, the parent's
// delete_at_height MUST STILL BE CLEARED by the non-owning call, consistent
// with the DAH housekeeping that unconditionally clears the stamp for all
// affected parents (even if the spend deletion was a no-op).
func TestUnspendNonOwningDoesNotClearBits(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	parent := newMinedSingleOutputTx(t, store, 100)
	_ = spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent at 101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))
	dah := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, dah, "fully-spent mined parent must be stamped before non-owning Unspend")
	require.Equal(t, int64(101)+1+ret, *dah)

	// A non-owning Unspend: correct outpoint but WRONG spending_data token. It must
	// delete 0 rows and leave the bitmap fully set.
	bogus := spendpkg.NewSpendingData(parent.TxIDChainHash(), 999)
	nonOwning := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: bogus}
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{nonOwning}))

	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent),
		"non-owning no-op Unspend must NOT clear any spent_bits bit")

	// The owned spend row must still be present.
	var stillThere int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM spends WHERE prev_tx_hash=$1 AND prev_output_idx=0`,
		parent.TxIDChainHash()[:]).Scan(&stillThere))
	require.Equal(t, 1, stillThere, "the live owned spend row must survive a non-owning Unspend")

	// However, the parent's delete_at_height MUST be cleared by the non-owning call.
	// The DAH housekeeping in Unspend runs for EVERY affected parent, including
	// non-owning no-op deletes, so any stale stamp is cleared in the same
	// transaction as the spend deletion. A future regression (making the clear
	// conditional on owning rows) would leave a live tx carrying a prunable
	// stamp — live-UTXO loss — and should be caught here.
	require.Nil(t, dahOfTx(t, store, ctx, parent),
		"non-owning Unspend must still clear delete_at_height (DAH housekeeping is unconditional)")
}

// TestUnspendSingleSpendableClearsBitToZero asserts the single-output happy path:
// a single owned spendable Unspend returns the bitmap to all-zero. The clear
// filters on out_spendables so only spendable spend rows clear bits.
func TestUnspendSingleSpendableClearsBitToZero(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))

	parent := newMinedSingleOutputTx(t, store, 100)
	sp0 := spendVoutOwned(t, store, parent, 0, 101) // partial: only vout 0

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, parent), "one spendable output folded")

	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{sp0}))
	require.Equal(t, buildSpentBits(2), spentBitsOfTx(t, store, ctx, parent),
		"unspending the one owned spendable output returns the bitmap to all-zero")
	require.Equal(t, 0, spentBitCountOfTx(t, store, ctx, parent))
}

// TestSetMinedMultiStampsViaBitmap pins Site 1: the spent-before-mined ordering.
// A tx is fully spent while UNMINED. The sweep folds spent_bits to full but the
// mined gate keeps it unstamped. SetMinedMulti must then stamp delete_at_height
// DIRECTLY from the bitmap (bit_count(spent_bits) = spendable_count) with NO
// spends re-aggregation, using GREATEST(last_spend_height, mined)+1+ret.
func TestSetMinedMultiStampsViaBitmap(t *testing.T) {
	store, ctx := setupTestStore(t)
	ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTxK(t, store, 2) // stays unmined, 2 spendable outputs
	spendAllOutputs(t, store, tx, 50)      // fully spent while UNMINED at height 50

	// Fold the spends via the sweep so the bitmap reaches full while the tx is
	// still unmined (proc must NOT stamp — mined gate).
	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, tx), "sweep folds bitmap while unmined")
	require.Nil(t, dahOfTx(t, store, ctx, tx), "unmined tx must stay unstamped after fold")
	lsh := lastSpendHeightOfTx(t, store, ctx, tx)
	require.NotNil(t, lsh)
	require.Equal(t, int64(50), *lsh, "fold advanced last_spend_height to the spend height")

	// Now mine it: SetMinedMulti stamps via the bitmap (bit_count=spendable_count).
	mineTx(t, store, tx, 60)
	dah := dahOfTx(t, store, ctx, tx)
	require.NotNil(t, dah, "SetMinedMulti must stamp the fully-spent tx via the bitmap")
	// GREATEST(last_spend_height=50, mined=60)+1+ret = 61+ret
	require.Equal(t, int64(60)+1+ret, *dah, "DAH = GREATEST(last_spend_height, mined)+1+retention")

	var pdDAH *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&pdDAH))
	require.NotNil(t, pdDAH, "mine-time stamp must be mirrored into pending_deletes")
	require.Equal(t, *dah, *pdDAH)
}

// TestSetMinedMultiPartialBitmapDoesNotStamp pins that SetMinedMulti does NOT
// stamp when the bitmap has not reached spendable_count (partially spent), even
// though the tx is being mined. Under-stamping is safe (the sweep completes it
// later); over-stamping here would prune a tx with live UTXOs.
func TestSetMinedMultiPartialBitmapDoesNotStamp(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := newUniqueUnminedTxK(t, store, 2)
	spendVouts(t, store, tx, 50, 0) // only vout 0 spent while unmined

	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0), spentBitsOfTx(t, store, ctx, tx), "only one output's bit folded")

	mineTx(t, store, tx, 60)
	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"SetMinedMulti must NOT stamp a partially-spent tx (bit_count < spendable_count)")
}

// TestUnsetMinedClearsStampedDAH pins Site 3: unsetMinedMulti (reorg clearing
// mined) must clear a previously-stamped delete_at_height, consistent with Site 1.
func TestUnsetMinedClearsStampedDAH(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := newUniqueUnminedTxK(t, store, 2)
	spendAllOutputs(t, store, tx, 50)

	require.NoError(t, store.SetBlockHeight(60))
	_, err := procSweepUpTo(store, ctx, 60)
	require.NoError(t, err)

	// Mine it (stamps via the bitmap), confirm stamped.
	mineTx(t, store, tx, 60)
	require.NotNil(t, dahOfTx(t, store, ctx, tx), "must be stamped after mine")

	// Reorg it out: unsetMinedMulti (UnsetMined path) must clear the stamp.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:     60,
		BlockHeight: 60,
		UnsetMined:  true,
	})
	require.NoError(t, err)

	require.Nil(t, dahOfTx(t, store, ctx, tx),
		"unsetMinedMulti must clear the previously-stamped delete_at_height")

	var pdCount int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, tx.TxIDChainHash()[:]).Scan(&pdCount))
	require.Zero(t, pdCount, "unset-mined must remove the tx from pending_deletes")
}

// TestUnspendEnqueuesDirtyParentAndDrainHeals pins the v15 heal loop end-to-end:
//
//  1. Unspend must enqueue the affected parent into dah_dirty_parents IN THE SAME
//     transaction as the spend-row delete + bit clear (queue row present with the
//     parent's txs leaf partition number).
//  2. If a concurrent fold's stale band snapshot re-sets the just-cleared bit
//     (simulated here by writing the wrongly-full bitmap + stamp back via SQL),
//     one drainDirtyParentsPartition pass must heal it: recompute the bits from
//     spends (ground truth), un-stamp, purge pending_deletes, and consume the
//     queue row.
func TestUnspendEnqueuesDirtyParentAndDrainHeals(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(110))
	ret := int32(store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive

	parent := newUniqueUnminedTxK(t, store, 2)
	mineTx(t, store, parent, 100)
	sp0 := spendVoutOwned(t, store, parent, 0, 101)
	_ = spendVoutOwned(t, store, parent, 1, 101) // fully spent at 101

	_, err := procSweepUpTo(store, ctx, 110)
	require.NoError(t, err)
	require.Equal(t, buildSpentBits(2, 0, 1), spentBitsOfTx(t, store, ctx, parent))
	stampedDAH := dahOfTx(t, store, ctx, parent)
	require.NotNil(t, stampedDAH, "precondition: stamped while fully spent")

	// The partition the parent's txs row lives in (same NN as the queue row must carry).
	var parentPartition int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT substring(tableoid::regclass::text from '(\d+)$')::int FROM txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&parentPartition))

	// (1) Unspend enqueues the parent transactionally.
	require.NoError(t, store.Unspend(ctx, []*utxo.Spend{sp0}))
	require.Equal(t, buildSpentBits(2, 1), spentBitsOfTx(t, store, ctx, parent), "bit 0 cleared by Unspend")

	var queuedPartition int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT partition FROM dah_dirty_parents WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&queuedPartition))
	require.Equal(t, parentPartition, queuedPartition,
		"Unspend must enqueue the parent into dah_dirty_parents with its txs leaf partition")

	// (2) Simulate the fold-vs-Unspend stale-snapshot race outcome: a stale band
	// re-set the cleared bit and the wrongly-full bitmap got stamped again.
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET spent_bits = $2, delete_at_height = $3 WHERE hash = $1`,
		parent.TxIDChainHash()[:], buildSpentBits(2, 0, 1), *stampedDAH)
	require.NoError(t, err)
	_, err = store.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, $2)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		parent.TxIDChainHash()[:], *stampedDAH)
	require.NoError(t, err)

	// One dirty drain of that partition heals everything within one pass.
	drained, corrected, skipped, err := store.drainDirtyParentsPartition(ctx, parentPartition, 110, ret, 1000)
	require.NoError(t, err)
	require.False(t, skipped)
	require.GreaterOrEqual(t, drained, int64(1), "the enqueued parent must be drained")
	require.GreaterOrEqual(t, corrected, int64(1), "the wrongly-full row must be corrected")

	require.Equal(t, buildSpentBits(2, 1), spentBitsOfTx(t, store, ctx, parent),
		"drain must recompute spent_bits from spends (ground truth: only vout1 spent)")
	require.Nil(t, dahOfTx(t, store, ctx, parent), "drain must un-stamp the wrongly-full stamp")

	var pd int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&pd))
	require.Zero(t, pd, "drain must purge pending_deletes for the un-stamped parent")

	var queued int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM dah_dirty_parents WHERE hash=$1`, parent.TxIDChainHash()[:]).Scan(&queued))
	require.Zero(t, queued, "the queue row must be consumed by the drain")
}
