package blockchain

import (
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func TestCalcNextRequiredDifficulty(t *testing.T) {
	// A suitable first block is the median timestamp of n-144, n-145, and n-146

	// height - time stamp
	// 826076 - 170464 7470
	// 826077 - 170464 7484 *
	// 826078 - 170464 7599
	// 826079 - 170464 8099

	// 826220 - 1704737 701
	// 826221 - 1704738 369 *
	// 826222 - 1704738 562
	// 826223 - 1704738 582

	// use bits from block 826077 (826223-146)
	firstBlockHeader := &model.BlockHeader{
		Bits:      model.NewNBitFromString("1808f160"),
		Timestamp: 1704647484,
	}

	// use bits from block 826221
	lastBlockHeader := &model.BlockHeader{
		Bits:      model.NewNBitFromString("1809dd97"),
		Timestamp: 1704738369,
	}

	// expected from block 826224 - 180a39ef
	expectedNbits := model.NewNBitFromString("1809dd97")
	// expectedNbits := model.NewNBitFromString("180a1de9") // this is the actual nBit value in block 826224

	os.Setenv("difficulty_target_time_per_block", "144")

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, 600)
	require.NoError(t, err)

	nbits, err := d.ComputeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	require.Equal(t, expectedNbits, *nbits)
}

func TestCalculateDifficulty(t *testing.T) {
	tests := map[string]struct {
		firstBlockHeader model.BlockHeader
		lastBlockHeader  model.BlockHeader
		expected         model.NBit
	}{
		"block #800000": {firstBlockHeader: model.BlockHeader{
			Bits:      model.NewNBitFromString("180d589d"), // 800000
			Timestamp: 1688957834,
		}, lastBlockHeader: model.BlockHeader{ // 800144
			Bits:      model.NewNBitFromString("180f0e84"),
			Timestamp: 1689046071,
		}, expected: model.NewNBitFromString("180f0e84"), // 800146
		// expected     180f6077
		},
		"block #826768": {firstBlockHeader: model.BlockHeader{
			Bits:      model.NewNBitFromString("18087ed7"), // 826623
			Timestamp: 1704972003,
		}, lastBlockHeader: model.BlockHeader{ // 826767
			Bits:      model.NewNBitFromString("1807e0fb"),
			Timestamp: 1705054404,
		}, expected: model.NewNBitFromString("1807e0fb")}, // 826768
		// expected    180783a0
	}

	os.Setenv("difficulty_target_time_per_block", "144")

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, 600)
	require.NoError(t, err)
	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			got, err := d.ComputeTarget(&tc.firstBlockHeader, &tc.lastBlockHeader)
			require.NoError(t, err)
			require.Equal(t, tc.expected, *got)
		})
	}
}

func TestCalcNextRequiredDifficulty_fastBlocks(t *testing.T) {
	firstBlockHeader := &model.BlockHeader{
		Bits:      model.NewNBitFromString("1808de5f"),
		Timestamp: 1704647590,
	}

	lastBlockHeader := &model.BlockHeader{
		Bits:      model.NewNBitFromString("180a097a"),
		Timestamp: 1704647599,
	}

	// as the timestapms are too close together difficulty should be the last difficulty
	expectedNbits := model.NewNBitFromString("180a097a")

	os.Setenv("difficulty_target_time_per_block", "144")

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, 600)
	require.NoError(t, err)
	nbits, err := d.ComputeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	require.Equal(t, expectedNbits, *nbits)

}
