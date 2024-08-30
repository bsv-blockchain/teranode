package blockchain

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
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
	bits, _ := model.NewNBitFromString("1808f160")
	firstBlockHeader := &model.SuitableBlock{
		NBits: bits.CloneBytes(),
		Time:  1704647484, // 2024-01-07 17:11:24
	}

	// use bits from block 826221
	bits, _ = model.NewNBitFromString("1809dd97")
	lastBlockHeader := &model.SuitableBlock{
		NBits: bits.CloneBytes(),
		Time:  1704738369, // 2024-01-08 18:26:09
	}
	// duration 90885
	// expected from block 826224 - 180a39ef
	expectedNbits, _ := model.NewNBitFromString("1809dd97")
	// expectedNbits := model.NewNBitFromString("180a1de9") // this is the actual nBit value in block 826224

	// os.Setenv("difficulty_target_time_per_block", "144")
	params, err := chaincfg.GetChainParams("mainnet")
	if err != nil {
		t.Fatal("Unknown network: test")
	}

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, params)
	require.NoError(t, err)

	nbits, err := d.computeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	// expected: model.NBit{0x97, 0xdd, 0x9, 0x18}
	// actual  : model.NBit{0xb1, 0x60, 0xa, 0x18}
	require.Equal(t, *expectedNbits, *nbits)
}

func TestCalculateDifficulty(t *testing.T) {
	firstBits, _ := model.NewNBitFromString("180d589d")
	lastBits, _ := model.NewNBitFromString("180f0e84")
	expectedBits, _ := model.NewNBitFromString("180f0e84")
	fBits, _ := model.NewNBitFromString("18087ed7")
	lBits, _ := model.NewNBitFromString("1807e0fb")
	eBits, _ := model.NewNBitFromString("1807e0fb")
	tests := map[string]struct {
		firstBlockHeader model.SuitableBlock
		lastBlockHeader  model.SuitableBlock
		expected         model.NBit
	}{
		"block #800000": {firstBlockHeader: model.SuitableBlock{
			NBits: firstBits.CloneBytes(), // 800000
			Time:  1688957834,
		}, lastBlockHeader: model.SuitableBlock{ // 800144
			NBits: lastBits.CloneBytes(),
			Time:  1689046071,
		}, expected: *expectedBits, // 800146
		// expected     180f6077
		},
		"block #826768": {firstBlockHeader: model.SuitableBlock{
			NBits: fBits.CloneBytes(), // 826623
			Time:  1704972003,
		}, lastBlockHeader: model.SuitableBlock{ // 826767
			NBits: lBits.CloneBytes(),
			Time:  1705054404,
		}, expected: *eBits}, // 826768
		// expected    180783a0
	}

	// os.Setenv("difficulty_target_time_per_block", "144")

	params, err := chaincfg.GetChainParams("testnet")
	if err != nil {
		t.Fatal("Unknown network: testnet")
	}
	d, err := NewDifficulty(nil, ulogger.TestLogger{}, params)
	require.NoError(t, err)
	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			got, err := d.computeTarget(&tc.firstBlockHeader, &tc.lastBlockHeader)
			require.NoError(t, err)
			require.Equal(t, tc.expected, *got)
		})
	}
}

func TestCalcNextRequiredDifficulty_fastBlocks(t *testing.T) {
	firstBits, _ := model.NewNBitFromString("1808de5f")
	firstBlockHeader := &model.SuitableBlock{
		NBits: firstBits.CloneBytes(),
		Time:  1704647590,
	}

	lastBits, _ := model.NewNBitFromString("180a097a")
	lastBlockHeader := &model.SuitableBlock{
		NBits: lastBits.CloneBytes(),
		Time:  1704647599,
	}

	// as the timestapms are too close together difficulty should be the last difficulty
	expectedNbits, _ := model.NewNBitFromString("180a097a")

	// os.Setenv("difficulty_target_time_per_block", "144")

	params, err := chaincfg.GetChainParams("testnet")
	if err != nil {
		t.Fatal("Unknown network: test")
	}

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, params)
	require.NoError(t, err)
	nbits, err := d.computeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	require.Equal(t, *expectedNbits, *nbits)

}
