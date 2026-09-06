package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCoinIndexRebuildDecision pins the rule: a partition's packed-key index is rebuilt when
// it holds more than 55 bytes per entry, which is well above the 31.5-byte bulk floor and
// below the 63-byte churn plateau the store measured, and at most one partition per session.
func TestCoinIndexRebuildDecision(t *testing.T) {
	require.False(t, coinIndexNeedsRebuild(31_500_000, 1_000_000))
	require.False(t, coinIndexNeedsRebuild(50_000_000, 1_000_000))
	require.True(t, coinIndexNeedsRebuild(60_000_000, 1_000_000))
	require.False(t, coinIndexNeedsRebuild(60_000_000, 0), "no rows, nothing to judge")
}

// TestRebuildCoinIndexRunsConcurrentlyAndOnce exercises the statement against the test
// database: it must succeed on a live partition and touch only one partition per call.
func TestRebuildCoinIndexRunsConcurrentlyAndOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	n, err := s.rebuildOneBloatedCoinIndex(ctx, func(bytes, rows int64) bool { return true })
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
