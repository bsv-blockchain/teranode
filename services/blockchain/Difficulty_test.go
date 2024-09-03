package blockchain

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/require"
)

func TestCalcNextRequiredDifficulty(t *testing.T) {
	// A suitable first block is the median timestamp of n-144, n-145, and n-146

	// expected bits from block 826224

	// height - time stamp
	// 826077 - 170464 7484
	// 826078 - 170464 7599 *
	// 826079 - 170464 8099

	// 826221 - 1704738 369
	// 826222 - 1704738 562 *
	// 826223 - 1704738 582

	// use bits from block 826078 (826223-146)
	firstChainwork, _ := hex.DecodeString("0000000000000000000000000000000000000000014fde9a5605193885731ee4")
	bits, _ := model.NewNBitFromString("1808de5f")
	firstBlockHeader := &model.SuitableBlock{
		NBits:     bits.CloneBytes(),
		Time:      1704647599,
		ChainWork: firstChainwork,
	}

	// use bits from block 826222
	lastChainwork, _ := hex.DecodeString("0000000000000000000000000000000000000000014fed8ff37cff135c70f4bb")
	bits, _ = model.NewNBitFromString("180a097a")
	lastBlockHeader := &model.SuitableBlock{
		NBits:     bits.CloneBytes(),
		Time:      1704738562,
		ChainWork: lastChainwork,
	}

	// expected from block 826224 - 180a39ef
	expectedNbits, _ := model.NewNBitFromString("180a2268")

	params, err := chaincfg.GetChainParams("mainnet")
	if err != nil {
		t.Fatal("Unknown network: mainnet")
	}

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, params)
	require.NoError(t, err)

	nbits, err := d.computeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	require.Equal(t, *expectedNbits, *nbits)
}

// 799999 1688956 277
// 800000 1688957 834 *
// 800001 1688957 991

// 800143 1689045 380
// 800144 1689046 071 *
// 800145 1689046 134
func TestCalculateDifficulty(t *testing.T) {
	firstBits, _ := model.NewNBitFromString("180d589d")                                                       // 800000
	firstChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001483b3cc42a76ae3dc13792") // 800000
	lastBits, _ := model.NewNBitFromString("180f0e84")                                                        // 800144
	lastChainwork, _ := hex.DecodeString("0000000000000000000000000000000000000000014844e5fe3b17bb6cc37242")  // 800144
	expectedBits, _ := model.NewNBitFromString("180f38dd")                                                    // 800146

	fBits, _ := model.NewNBitFromString("18088e92")                                                       // 826622
	fChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001501adda85a0136f3a3adf7") // 826622
	lBits, _ := model.NewNBitFromString("1807e840")                                                       // 826766
	lChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001502c65f8e18b3db26b2729") // 826766
	eBits, _ := model.NewNBitFromString("1807d4ed")                                                       // 826768

	tests := map[string]struct {
		firstBlockHeader model.SuitableBlock
		lastBlockHeader  model.SuitableBlock
		expected         model.NBit
	}{
		"block #800000": {firstBlockHeader: model.SuitableBlock{
			NBits:     firstBits.CloneBytes(), // 800000
			Time:      1688957834,
			ChainWork: firstChainwork,
		}, lastBlockHeader: model.SuitableBlock{ // 800144
			NBits:     lastBits.CloneBytes(),
			Time:      1689046071,
			ChainWork: lastChainwork,
		}, expected: *expectedBits, // 800146
		},
		"block #826768": {firstBlockHeader: model.SuitableBlock{
			NBits:     fBits.CloneBytes(), // 826622
			Time:      1704971891,
			ChainWork: fChainwork,
		}, lastBlockHeader: model.SuitableBlock{ // 826766
			NBits:     lBits.CloneBytes(),
			Time:      1705054277,
			ChainWork: lChainwork,
		}, expected: *eBits}, // 826768
	}

	params, err := chaincfg.GetChainParams("mainnet")
	if err != nil {
		t.Fatal("Unknown network: mainnet")
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

// func TestCalcNextRequiredDifficulty_fastBlocks(t *testing.T) {
// 	firstChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001483b3cc42a76ae3dc13792")
// 	lastChainwork, _ := hex.DecodeString("0000000000000000000000000000000000000000014fde9a5605193885731ee4")

// 	firstBits, _ := model.NewNBitFromString("1808de5f")
// 	firstBlockHeader := &model.SuitableBlock{
// 		NBits:     firstBits.CloneBytes(),
// 		Time:      1704647590,
// 		ChainWork: firstChainwork,
// 	}

// 	lastBits, _ := model.NewNBitFromString("1808de5f") // 826078
// 	lastBlockHeader := &model.SuitableBlock{
// 		NBits:     lastBits.CloneBytes(),
// 		Time:      1704647599,
// 		ChainWork: lastChainwork,
// 	}

// 	// 826080 bits 1808cf59
// 	// as the timestamps are too close together difficulty should be the last difficulty
// 	expectedNbits, _ := model.NewNBitFromString("1808de5f")

// 	params, err := chaincfg.GetChainParams("mainnet")
// 	if err != nil {
// 		t.Fatal("Unknown network: mainnet")
// 	}

// 	d, err := NewDifficulty(nil, ulogger.TestLogger{}, params)
// 	require.NoError(t, err)
// 	nbits, err := d.computeTarget(firstBlockHeader, lastBlockHeader)
// 	require.NoError(t, err)

// 	require.Equal(t, *expectedNbits, *nbits)

// }

// TestCalcWork ensures CalcWork calculates the expected work value from values
// in compact representation.
func TestCalcWork(t *testing.T) {
	tests := []struct {
		in  uint32
		out int64
	}{
		{10000000, 0},
	}

	for x, test := range tests {
		bits := test.in

		r := CalcWork(bits)
		if r.Int64() != test.out {
			t.Errorf("TestCalcWork test #%d failed: got %v want %d\n",
				x, r.Int64(), test.out)
			return
		}
	}
}

// TestBigToCompact ensures BigToCompact converts big integers to the expected
// compact representation.
func TestBigToCompact(t *testing.T) {
	tests := []struct {
		in  int64
		out uint32
	}{
		{0, 0},
		{-1, 25231360},
	}

	for x, test := range tests {
		n := big.NewInt(test.in)
		r := BigToCompact(n)
		if r != test.out {
			t.Errorf("TestBigToCompact test #%d failed: got %d want %d\n",
				x, r, test.out)
			return
		}
	}
}

// TestCompactToBig ensures CompactToBig converts numbers using the compact
// representation to the expected big intergers.
func TestCompactToBig(t *testing.T) {
	tests := []struct {
		in  uint32
		out int64
	}{
		{10000000, 0},
	}

	for x, test := range tests {
		n := CompactToBig(test.in)
		want := big.NewInt(test.out)
		if n.Cmp(want) != 0 {
			t.Errorf("TestCompactToBig test #%d failed: got %d want %d\n",
				x, n.Int64(), want.Int64())
			return
		}
	}
}
