package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPruneDeleteMargin is the TDD gate for the delete-side crash-replay margin
// (2026-07-17 RAM-footprint design, Task 7). The pruner must delete a tombstoned
// row only once the TRIGGER height clears its delete_at_height (DAH) by at least
// PruneDeleteMarginBlocks, not merely reach it — a uniform delay applied to every
// row (retention-stamped and early-stamped alike) so that no pipeline replaying
// work near the trigger watermark (e.g. after a crash) can ever reference a
// parent this call just deleted.
//
// Modelled on TestPrunerService (store_test.go): a tx is created and its
// delete_at_height stamped directly on both txs and pending_deletes (the pruner's
// only candidate source), mirroring what the real stamp sites (SetMinedMulti S6,
// the DAH sweep proc, ProcessExpiredPreservations) do.
//
// With margin=32 (the production default — test.CreateBaseTestSettings loads it
// via settings.NewSettings(), and the assertion below pins the assumption) and a
// DAH-100 row:
//   - Prune(100): 100-32=68 <  100 -> row survives.
//   - Prune(131): 131-32=99 <  100 -> row survives.
//   - Prune(132): 132-32=100 >= 100 -> row deleted.
func TestPruneDeleteMargin(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	margin := st.settings.UtxoStore.PruneDeleteMarginBlocks
	require.Equal(t, int32(32), margin, "test assumes the production default PruneDeleteMarginBlocks")

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	const dah = int32(100)
	_, err = st.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $1 WHERE hash = $2`,
		dah, txHash[:])
	require.NoError(t, err)
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, $2)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		txHash[:], dah)
	require.NoError(t, err)

	svc, err := st.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	svc.Start(ctx)

	rowExists := func() bool {
		t.Helper()
		var exists bool
		require.NoError(t, st.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM txs WHERE hash = $1)`, txHash[:]).Scan(&exists))
		return exists
	}

	_, err = svc.Prune(ctx, 100, "margin-test-100")
	require.NoError(t, err)
	require.True(t, rowExists(),
		"Prune(100) must NOT delete a DAH-100 row under margin 32 (100-32=68 < 100)")

	_, err = svc.Prune(ctx, 131, "margin-test-131")
	require.NoError(t, err)
	require.True(t, rowExists(),
		"Prune(131) must NOT delete a DAH-100 row under margin 32 (131-32=99 < 100)")

	_, err = svc.Prune(ctx, 132, "margin-test-132")
	require.NoError(t, err)
	require.False(t, rowExists(),
		"Prune(132) MUST delete a DAH-100 row under margin 32 (132-32=100 >= 100)")
}

// TestPruneDeleteMargin_NegativeSettingClampsToZero guards the must-fix from
// review: PruneDeleteMarginBlocks is a signed int32 setting. Cast blindly to
// uint32, a negative misconfiguration wraps to a huge value, making
// blockHeight <= margin true on every call -- Prune would silently no-op
// forever, which is the same failure class ("pruner silently stops, disk
// fills") as a past production incident. A negative setting must instead be
// clamped to 0 (i.e. behave as if no margin were configured), so a bad config
// degrades to the OLD at-or-below-trigger semantics rather than disabling the
// pruner outright.
func TestPruneDeleteMargin_NegativeSettingClampsToZero(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	// Misconfigure the margin negative.
	st.settings.UtxoStore.PruneDeleteMarginBlocks = -5

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	const dah = int32(100)
	_, err = st.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $1 WHERE hash = $2`,
		dah, txHash[:])
	require.NoError(t, err)
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, $2)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		txHash[:], dah)
	require.NoError(t, err)

	svc, err := st.GetPrunerService()
	require.NoError(t, err)
	svc.Start(ctx)

	// Trigger == DAH, clamped margin == 0: with the clamp working, the pruner
	// falls back to the pre-margin "at or below trigger" rule and deletes the
	// row. If the clamp were missing/broken, margin would wrap to ~2^32-5 and
	// this row (and every row, forever) would survive.
	_, err = svc.Prune(ctx, 100, "margin-test-negative-clamp")
	require.NoError(t, err)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM txs WHERE hash = $1)`, txHash[:]).Scan(&exists))
	require.False(t, exists,
		"a negative PruneDeleteMarginBlocks must clamp to 0, not wrap to a huge margin that silently stops all deletes")
}

// TestPruneDeleteMargin_ExcessivePositiveSettingClamps guards the mirror-image
// failure mode from final review Important issue 2: an absurdly large
// positive PruneDeleteMarginBlocks makes blockHeight <= margin true for the
// entire foreseeable life of the chain, silently disabling every delete
// forever -- the identical "pruner silently stops, disk fills" incident class
// the negative-clamp test above guards against, just approached from the
// other sign. A misconfigured huge positive margin must clamp to a sane upper
// bound (maxPruneDeleteMarginBlocks) rather than pass through unbounded.
func TestPruneDeleteMargin_ExcessivePositiveSettingClamps(t *testing.T) {
	st := newTestStoreWithFlag(t, true)
	ctx := context.Background()

	// Misconfigure the margin absurdly high -- without the upper clamp this
	// would make Prune a permanent no-op until the chain reached height
	// 1,000,000.
	st.settings.UtxoStore.PruneDeleteMarginBlocks = 1_000_000

	tx := testExtendedTx(t)
	_, err := st.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	const dah = int32(1000)
	_, err = st.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = $1 WHERE hash = $2`,
		dah, txHash[:])
	require.NoError(t, err)
	_, err = st.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, $2)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		txHash[:], dah)
	require.NoError(t, err)

	svc, err := st.GetPrunerService()
	require.NoError(t, err)
	svc.Start(ctx)

	// Trigger height comfortably above the clamped margin (1000) but nowhere
	// near the misconfigured raw value (1,000,000): with the clamp working,
	// 2000 - 1000(clamped) = 1000 >= dah(1000), so the row is deleted. If the
	// clamp were missing, 2000 <= 1,000,000 would early-return 0 and the row
	// (and every row, until the chain reached height 1,000,000) would survive.
	_, err = svc.Prune(ctx, 2000, "margin-test-positive-clamp")
	require.NoError(t, err)

	var exists bool
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM txs WHERE hash = $1)`, txHash[:]).Scan(&exists))
	require.False(t, exists,
		"an excessive positive PruneDeleteMarginBlocks must clamp to a sane upper bound, not pass through and silently stop all deletes for the life of the chain")
}
