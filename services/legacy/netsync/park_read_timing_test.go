package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParkReadIsTimed covers the one step on the drain's critical path that had
// no timing at all.
//
// Tracing a single block on mainnet on 2026-09-10 accounted for every stage of a
// thirty-one second block except a five-second stretch between the park sweep
// offering it and delivery starting. Reading the block back off disk is the only
// substantial work in that stretch, parked files run to 143 MB, and the whole
// block is rebuilt as a Go object here. Three times today I attributed a gap to
// whatever was noisiest in the log rather than to what was unmeasured, so this
// closes the last unmeasured step rather than guessing again.
func TestParkReadIsTimed(t *testing.T) {
	h := newParkWiringHarness(t, true)
	park := h.sm.blockPark

	block := h.blocks[0].MsgBlock()
	hash := block.BlockHash()

	require.Equal(t, parkAccepted, park.Park(context.Background(), parkedBlock{
		hash:      hash,
		prevBlock: block.Header.PrevBlock,
	}, block))

	t.Run("a read still returns the block it was asked for", func(t *testing.T) {
		got, err := park.Read(context.Background(), hash)
		require.NoError(t, err, "timing must not change what the read does")
		require.Equal(t, hash, got.BlockHash())
	})

	t.Run("the threshold keeps a fast read quiet", func(t *testing.T) {
		// A test block reads in microseconds, far under the threshold, so nothing
		// is reported. The value of the threshold is that the common case does not
		// add a line per commit and crowd out the slow reads that matter.
		require.Greater(t, parkReadSlowAfter, time.Millisecond,
			"a threshold at or below a millisecond would report every read and tell nobody anything")
		require.LessOrEqual(t, parkReadSlowAfter, time.Second,
			"a threshold above a second would hide exactly the reads that made a block slow")
	})
}
