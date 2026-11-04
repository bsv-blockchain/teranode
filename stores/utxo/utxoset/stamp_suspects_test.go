package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/chainancestry/chainancestrytest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// markerSet reports whether a transaction's identity row carries the unmined marker.
func markerSet(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) bool {
	t.Helper()

	var set bool
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT off_chain_since IS NOT NULL FROM tx_ident WHERE txid = $1`, hashBytes(tx)).Scan(&set))

	return set
}

// suspectFixture is a store with partition 0 holding no winner for the suspect, and partition 1
// holding one other winner so that it exists and is stamped. The suspect is a transaction whose
// identity row says it is mined (marker NULL) and was first seen at height 99, below partition 1.
type suspectFixture struct {
	s       *Store
	ctx     context.Context
	suspect *bt.Tx
	tip     uint32
}

// newSuspectFixture builds it. confirmedAt, when non-zero, is the height of a chain-confirmed
// containment row the suspect has above partition 1; zero leaves the suspect with no containment
// row at all, as after a block whose containment was lost.
func newSuspectFixture(t *testing.T, confirmedAt uint32) suspectFixture {
	t.Helper()

	s, ctx := newUncheckpointedStore(t)

	if confirmedAt == 0 {
		suspect := mineAtSeenAt(t, s, ctx, 5_000, 99, 100, 7)
		_, err := s.pool.Exec(ctx, `DELETE FROM tx_mined WHERE txid = $1`, hashBytes(suspect))
		require.NoError(t, err)

		mineAt(t, s, ctx, 6_000, 300, 9)

		return suspectFixture{s: s, ctx: ctx, suspect: suspect, tip: stampDepthTip(s, 1)}
	}

	suspect := mineAtSeenAt(t, s, ctx, 5_000, 99, confirmedAt, 11)
	mineAt(t, s, ctx, 6_000, 300, 9)

	return suspectFixture{s: s, ctx: ctx, suspect: suspect, tip: stampDepthTip(s, 1)}
}

func (f suspectFixture) stamp(t *testing.T, anc *chainancestry.Ancestry) {
	t.Helper()

	require.NoError(t, f.s.SetBlockHeight(f.tip))
	drainAll(t, f.s, f.ctx, anc, f.tip)
}

// A transaction whose identity row believes it is mined but which has no containment row the
// chain confirms would leak for ever: the stamp never matches it and the unmined iterator never
// sees it. The page that finds it sets its marker so block assembly takes it up again.
func TestStampMarksATransactionBelievedMinedWithNoConfirmedContainment(t *testing.T) {
	f := newSuspectFixture(t, 0)
	require.False(t, markerSet(t, f.s, f.ctx, f.suspect), "harness check: the suspect believes it is mined")

	before := testutil.ToFloat64(stampNoWinner.WithLabelValues("null"))

	f.stamp(t, chainancestrytest.Chain(t, 0, f.tip, map[uint32]uint32{300: 9}))

	require.True(t, markerSet(t, f.s, f.ctx, f.suspect), "marker set so block assembly reconsiders it")
	require.True(t, identExists(t, f.s, f.ctx, f.suspect), "and the identity row stays")
	require.Equal(t, before+1, testutil.ToFloat64(stampNoWinner.WithLabelValues("null")))
}

// A suspect with a containment row above the partition that the ancestry confirms is mined, just in
// a later partition, and is left alone.
func TestStampLeavesATransactionConfirmedInALaterPartition(t *testing.T) {
	f := newSuspectFixture(t, 600)

	before := testutil.ToFloat64(stampNoWinner.WithLabelValues("null"))

	f.stamp(t, chainancestrytest.Chain(t, 0, f.tip, map[uint32]uint32{300: 9, 600: 11}))

	require.False(t, markerSet(t, f.s, f.ctx, f.suspect))
	require.Equal(t, before, testutil.ToFloat64(stampNoWinner.WithLabelValues("null")))
}

// The same row, when the ancestry names a different block at its height, is a loser's row: the
// transaction is not confirmed anywhere and is marked.
func TestStampMarksATransactionWhoseOnlyLaterRowIsOnAForkBlock(t *testing.T) {
	f := newSuspectFixture(t, 600)

	f.stamp(t, chainancestrytest.Chain(t, 0, f.tip, map[uint32]uint32{300: 9, 600: 12}))

	require.True(t, markerSet(t, f.s, f.ctx, f.suspect))
}

// Between the partition's upper bound and the anchor, a block whose mined_set is still false may
// have a record-mined call running, so its rows may not all be visible. Judging then could mark a
// transaction that is about to be confirmed; the step is deferred to a later drain and counted.
func TestStampDefersJudgingSuspectsWhileABlockAboveIsNotYetMined(t *testing.T) {
	f := newSuspectFixture(t, 0)

	deferredBefore := testutil.ToFloat64(stampSuspectsDeferred)
	markedBefore := testutil.ToFloat64(stampNoWinner.WithLabelValues("null"))

	f.stamp(t, chainancestrytest.ChainNotMined(t, 0, f.tip, map[uint32]uint32{300: 9}, []uint32{700}))

	require.False(t, markerSet(t, f.s, f.ctx, f.suspect), "not judged this drain")
	require.Equal(t, markedBefore, testutil.ToFloat64(stampNoWinner.WithLabelValues("null")))
	require.GreaterOrEqual(t, testutil.ToFloat64(stampSuspectsDeferred), deferredBefore+1)
}
