package lane

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConversionSegmentCheck(t *testing.T) {
	require.NoError(t, ConversionSegment{ID: 1, First: 2, PrefixLast: 10, Last: 12}.Check())
	require.NoError(t, ConversionSegment{ID: 1, First: 2, PrefixLast: 1, Last: 5}.Check(), "empty prefix")
	require.NoError(t, ConversionSegment{ID: 1, First: 2, PrefixLast: 5, Last: 5}.Check(), "empty suffix")

	require.Error(t, ConversionSegment{ID: 0, First: 2, PrefixLast: 5, Last: 5}.Check())
	require.Error(t, ConversionSegment{ID: 1, First: BackfillSlotSentinel, PrefixLast: 5, Last: 5}.Check(), "position 1 is the slot sentinel")
	require.Error(t, ConversionSegment{ID: 1, First: 2, PrefixLast: 6, Last: 5}.Check())
	require.Error(t, ConversionSegment{ID: 1, First: 5, PrefixLast: 2, Last: 5}.Check())
	require.Error(t, ConversionSegment{ID: 1, First: 2, PrefixLast: 5, Last: MaxPosition + 1}.Check())
}

func TestCheckConversionBlocks(t *testing.T) {
	seg := ConversionSegment{ID: 1, First: 1_000_001, PrefixLast: 1_000_005, Last: 1_000_006}
	good := []ConversionBlock{
		{Seq: 0, BlockID: 7, Height: 700_000, First: 1_000_001, Count: 3},
		{Seq: 1, BlockID: 8, Height: 700_001, First: 1_000_004, Count: 0},
		{Seq: 2, BlockID: 9, Height: 700_002, First: 1_000_004, Count: 2},
	}
	require.NoError(t, CheckConversionBlocks(seg, good))

	mutate := func(f func(b []ConversionBlock) []ConversionBlock) []ConversionBlock {
		c := append([]ConversionBlock(nil), good...)
		return f(c)
	}

	for name, blocks := range map[string][]ConversionBlock{
		"short of the prefix": good[:2],
		"wrong seq":           mutate(func(b []ConversionBlock) []ConversionBlock { b[1].Seq = 5; return b }),
		"height not rising":   mutate(func(b []ConversionBlock) []ConversionBlock { b[2].Height = 700_001; return b }),
		"block twice":         mutate(func(b []ConversionBlock) []ConversionBlock { b[2].BlockID = 7; return b }),
		"gap":                 mutate(func(b []ConversionBlock) []ConversionBlock { b[2].First++; return b }),
		"past the prefix":     mutate(func(b []ConversionBlock) []ConversionBlock { b[2].Count = 3; return b }),
	} {
		require.Error(t, CheckConversionBlocks(seg, blocks), name)
	}

	require.NoError(t, CheckConversionBlocks(ConversionSegment{ID: 2, First: 2, PrefixLast: 1, Last: 3}, nil), "no prefix, no blocks")
}
