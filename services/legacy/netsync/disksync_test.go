package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/diskblocks"
	"github.com/stretchr/testify/require"
)

func refsAt(heights ...uint32) []*diskblocks.BlockRef {
	out := make([]*diskblocks.BlockRef, 0, len(heights))
	for _, h := range heights {
		out = append(out, &diskblocks.BlockRef{Height: h})
	}
	return out
}

func TestBlocksToFeedResume(t *testing.T) {
	chain := refsAt(0, 1, 2, 3, 4, 5)
	// best height already 2 -> feed 3,4,5
	got := blocksToFeed(chain, 2, 0)
	require.Len(t, got, 3)
	require.Equal(t, uint32(3), got[0].Height)
	require.Equal(t, uint32(5), got[len(got)-1].Height)
}

func TestBlocksToFeedStopHeight(t *testing.T) {
	chain := refsAt(0, 1, 2, 3, 4, 5)
	got := blocksToFeed(chain, 0, 3) // genesis already present (height 0) -> feed 1..3
	require.Equal(t, uint32(1), got[0].Height)
	require.Equal(t, uint32(3), got[len(got)-1].Height)
}

func TestBlocksToFeedNothingToDo(t *testing.T) {
	chain := refsAt(0, 1, 2)
	require.Empty(t, blocksToFeed(chain, 2, 0))
}
