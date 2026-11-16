package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/chainancestry/chainancestrytest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// pagedPartitionZero stamps every page of partition 0 holding enough winners that the audit's
// random sampling cannot miss them all, and returns the drain, the ancestry and the tip, with the
// completion not yet written.
func pagedPartitionZero(t *testing.T) (*Store, context.Context, pruner.StampDrain, *chainancestry.Ancestry, uint32) {
	t.Helper()

	s, ctx := newUncheckpointedStore(t)

	for i := uint64(0); i < 40; i++ {
		mineAt(t, s, ctx, 5_000+i, 100, 7)
	}

	tip := stampDepthTip(s, 0)
	require.NoError(t, s.SetBlockHeight(tip))

	anc := chainancestrytest.Chain(t, 0, tip, map[uint32]uint32{100: 7})

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, drain.Close()) })

	state, err := drain.BeginWindow(ctx, 0, anc)
	require.NoError(t, err)
	require.Equal(t, pruner.StampWindowReady, state)

	for page := 0; page < drain.PagesPerWindow(); page++ {
		_, err := drain.StampPage(ctx, 0, anc, page)
		require.NoError(t, err)
	}

	return s, ctx, drain, anc, tip
}

// The audit is the detector for a UTXO the stamp missed. A UTXO of a sampled winner still at
// (0,0) with its identity row gone could never be stamped later, and dropping the partition would
// leave it with nothing to answer for it, so the partition must not complete.
func TestStampDoesNotCompleteAPartitionWithAUTXOLeftAtZeroZero(t *testing.T) {
	s, ctx, drain, anc, tip := pagedPartitionZero(t)

	_, err := s.pool.Exec(ctx, `UPDATE utxo SET mined_height = 0, block_id = 0`)
	require.NoError(t, err)

	before := testutil.ToFloat64(stampAuditViolations)

	err = drain.CompleteWindow(ctx, 0, anc, tip)
	require.ErrorContains(t, err, "still at (0,0)")
	require.Greater(t, testutil.ToFloat64(stampAuditViolations), before)

	_, ok := stampedAtOf(t, s, ctx, 0)
	require.False(t, ok, "no completion record")

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampCompleteFloor)
}

// A UTXO carrying some other real pair is counted and logged, but the partition still completes:
// the audit repairs nothing and only a (0,0) is unrecoverable.
func TestStampCountsADisagreeingPairAndStillCompletes(t *testing.T) {
	s, ctx, drain, anc, tip := pagedPartitionZero(t)

	_, err := s.pool.Exec(ctx, `UPDATE utxo SET mined_height = 55, block_id = 56`)
	require.NoError(t, err)

	before := testutil.ToFloat64(stampAuditDisagreements)

	require.NoError(t, drain.CompleteWindow(ctx, 0, anc, tip))
	require.Greater(t, testutil.ToFloat64(stampAuditDisagreements), before)

	_, ok := stampedAtOf(t, s, ctx, 0)
	require.True(t, ok)
}

// A winner whose identity row is still there after its page is the other thing the audit
// refuses to publish over.
func TestStampDoesNotCompleteAPartitionWithAnIdentityRowLeftOnAWinner(t *testing.T) {
	s, ctx, drain, anc, tip := pagedPartitionZero(t)

	_, err := s.pool.Exec(ctx, `
		INSERT INTO tx_ident (leaf, txid, created_height)
		SELECT (get_byte(txid, 0) & 7)::smallint, txid, 99 FROM tx_mined`)
	require.NoError(t, err)

	err = drain.CompleteWindow(ctx, 0, anc, tip)
	require.ErrorContains(t, err, "still have an identity row")

	_, ok := stampedAtOf(t, s, ctx, 0)
	require.False(t, ok)
}
