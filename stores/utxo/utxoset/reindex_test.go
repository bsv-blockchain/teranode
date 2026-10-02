package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUTXOIndexRebuildDecision pins the rule: a partition's packed-key index is rebuilt when
// it holds more than 55 bytes per entry, which is well above the 31.5-byte bulk floor and
// below the 63-byte churn plateau the store measured, and at most one partition per session.
// The fixtures sit above the size floor so it is the ratio being judged here.
func TestUTXOIndexRebuildDecision(t *testing.T) {
	require.False(t, utxoIndexNeedsRebuild(315_000_000, 10_000_000))
	require.False(t, utxoIndexNeedsRebuild(500_000_000, 10_000_000))
	require.True(t, utxoIndexNeedsRebuild(600_000_000, 10_000_000))
	require.False(t, utxoIndexNeedsRebuild(600_000_000, 0), "no rows, nothing to judge")
}

// TestUTXOIndexRebuildIgnoresASmallIndex pins the size floor. A btree carries a metapage and
// a root whatever it holds, so on a partition of a few thousand rows the fixed pages alone
// put it over 55 bytes per entry, and on the 2026-09-22 mainnet reset the pruner rebuilt the
// eight 98 KB indexes 110 times in 13 minutes, each back to 72 KB, reclaiming nothing worth a
// REINDEX CONCURRENTLY. Below the floor the ratio is not judged at all.
func TestUTXOIndexRebuildIgnoresASmallIndex(t *testing.T) {
	require.False(t, utxoIndexNeedsRebuild(98_304, 1_637), "60 bytes per entry, but 98 KB is fixed overhead")
	require.False(t, utxoIndexNeedsRebuild(utxoIndexRebuildMinBytes-1, (utxoIndexRebuildMinBytes-1)/100), "just under the floor")
	require.True(t, utxoIndexNeedsRebuild(utxoIndexRebuildMinBytes, utxoIndexRebuildMinBytes/100), "at the floor the ratio is judged again")
}

// TestRebuildUTXOIndexRunsConcurrentlyAndOnce exercises the statement against the test
// database: it must succeed on a live partition and touch only one partition per call.
func TestRebuildUTXOIndexRunsConcurrentlyAndOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	n, err := s.rebuildOneBloatedUTXOIndex(ctx, func(bytes, rows int64) bool { return true })
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
