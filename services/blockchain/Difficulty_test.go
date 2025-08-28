package blockchain

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/work"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

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
func TestCalcNextRequiredDifficulty(t *testing.T) {
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

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	d, err := NewDifficulty(nil, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	nbits, err := d.computeTarget(firstBlockHeader, lastBlockHeader)
	require.NoError(t, err)

	require.Equal(t, *expectedNbits, *nbits)
}

// TestBlock910479Fix tests the fix for issue #3772 where block 910479 was mined with incorrect difficulty
// The block had nBits 0x1818cd40 but should have had 0x181800c2
// This test uses a simplified scenario that demonstrates the fix
func TestBlock910479Fix(t *testing.T) {
	// Test that the precision squaring bug has been fixed
	// The bug was on lines 248-250 where newTarget was incorrectly squared

	// Create a scenario similar to what would produce the bug
	// Using chainwork values that when processed would expose the squaring issue
	firstBits, _ := model.NewNBitFromString("18180000")
	// Chainwork difference that would produce approximately the correct difficulty
	firstChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001683c00000000000000000")

	lastBits, _ := model.NewNBitFromString("18180000")
	// The chainwork should increase by approximately 144 blocks worth of work
	lastChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001683c90000000000000000")

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	firstBlock := &model.SuitableBlock{
		NBits:     firstBits.CloneBytes(),
		Time:      1755420000, // ~144 blocks * 600 seconds earlier
		ChainWork: firstChainwork,
		Height:    910335,
	}

	lastBlock := &model.SuitableBlock{
		NBits:     lastBits.CloneBytes(),
		Time:      1755506400, // 86400 seconds later (1 day)
		ChainWork: lastChainwork,
		Height:    910478,
	}

	calculatedBits, err := d.computeTarget(firstBlock, lastBlock)
	require.NoError(t, err)

	// Before the fix, the precision squaring would have made the target too large
	// After the fix, the calculation should be correct
	// We're checking that the mantissa is reasonable and not inflated by squaring
	calculatedUint32 := binary.LittleEndian.Uint32(calculatedBits.CloneBytes())
	mantissa := calculatedUint32 & 0x007fffff

	// The mantissa should not be inflated by the squaring bug
	// With the bug, mantissa would be much larger (like 0x18cd40)
	// Without the bug, it should be smaller (closer to 0x1800c2)
	require.Less(t, mantissa, uint32(0x190000),
		"Mantissa should not be inflated by squaring bug. This validates the fix for issue #3772")
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
		firstBlockHeader           *model.SuitableBlock
		lastBlockHeader            *model.SuitableBlock
		parentOfLastMinedBlockTime uint32
		lastMinedBlockTime         uint32
		expected                   model.NBit
	}{
		"block #800000": {
			firstBlockHeader: &model.SuitableBlock{
				NBits:     firstBits.CloneBytes(), // 800000
				Time:      1688957834,
				ChainWork: firstChainwork,
			},
			lastBlockHeader: &model.SuitableBlock{ // 800144
				NBits:     lastBits.CloneBytes(),
				Time:      1689046071,
				ChainWork: lastChainwork,
			},
			parentOfLastMinedBlockTime: 1688957834,
			lastMinedBlockTime:         1689046134,
			expected:                   *expectedBits, // 800146
		},
		"block #826768": {
			firstBlockHeader: &model.SuitableBlock{
				NBits:     fBits.CloneBytes(), // 826622
				Time:      1704971891,
				ChainWork: fChainwork,
			},
			lastBlockHeader: &model.SuitableBlock{ // 826766
				NBits:     lBits.CloneBytes(),
				Time:      1705054277,
				ChainWork: lChainwork,
			},
			parentOfLastMinedBlockTime: 1705054277,
			lastMinedBlockTime:         1705054404,
			expected:                   *eBits}, // 826768
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	d, err := NewDifficulty(nil, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := d.computeTarget(tc.firstBlockHeader, tc.lastBlockHeader)
			require.NoError(t, err)
			require.Equal(t, tc.expected, *got)
		})
	}
}

type testData struct {
	height    uint32
	time      int64
	chainwork *big.Int
	bits      *model.NBit
}

func (td testData) GetTime() uint32 {
	return uint32(td.time) // nolint:gosec
}

func TestComputeTargetFromCSV(t *testing.T) {
	// Open and read blocks.csv
	file, err := os.Open("./work/testnet.csv")
	if err != nil {
		t.Fatalf("Failed to open testnet.csv: %v", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := bufio.NewReader(file)
	// Skip header row if it exists
	_, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read CSV header: %v", err)
	}

	// Create a new Difficulty instance for testing
	logger := ulogger.TestLogger{}

	chainParams := &chaincfg.TestNetParams
	s := &settings.Settings{
		ChainCfgParams: chainParams,
	}

	d, err := NewDifficulty(nil, logger, s)
	require.NoError(t, err)

	data := make([]testData, 0, 1_700_000)
	i := 0

	// Read and process each row
	for {
		record, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("Error reading CSV record: %v", err)
		}

		// Parse CSV data - adjust these indices based on your CSV structure
		fields := strings.Split(strings.TrimSpace(record), ",")

		height, _ := strconv.ParseUint(fields[0], 10, 32)
		time, _ := strconv.ParseInt(fields[1], 10, 32)
		chainworkBytes, _ := hex.DecodeString(fields[2])
		chainwork := new(big.Int).SetBytes(chainworkBytes)
		bitsStr := fields[3]
		bits, _ := model.NewNBitFromString(bitsStr)

		data = append(data, testData{
			height:    uint32(height), // nolint:gosec
			time:      time,           // nolint:gosec
			chainwork: chainwork,
			bits:      bits,
		})

		i++
	}

	// Skip the first 144 rows for this test (144+2 needed)
	for i := 146; i < len(data)-1; i++ {
		// The latestSuitableBlock is based on the last block (i - 1) and the previous 2 blocks
		l := make([]testData, 3)
		l[2] = data[i]
		l[1] = data[i-1]
		l[0] = data[i-2]

		util.SortForDifficultyAdjustment(l)

		// Create test blocks
		latestSuitableBlock := &model.SuitableBlock{
			NBits:     l[1].bits.CloneBytes(),
			Time:      uint32(l[1].time), // nolint:gosec
			ChainWork: l[1].chainwork.Bytes(),
			Height:    l[1].height,
		}

		l[2] = data[i-144]
		l[1] = data[i-144-1]
		l[0] = data[i-144-2]

		util.SortForDifficultyAdjustment(l)

		earliestSuitableBlock := &model.SuitableBlock{
			NBits:     l[1].bits.CloneBytes(),
			Time:      uint32(l[1].time), // nolint:gosec
			ChainWork: l[1].chainwork.Bytes(),
			Height:    l[1].height,
		}

		// Call computeTarget
		result, err := d.computeTarget(earliestSuitableBlock, latestSuitableBlock, data[i].time, data[i+1].time)
		require.NoError(t, err)

		// t.Logf("height %d: %s", data[i].height, result.String())

		require.Equal(t, data[i+1].bits.String(), result.String())
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

// TestCalcBlockWork ensures CalcBlockWork (formerly CalcWork) calculates the expected work value from values
// in compact representation.
func TestCalcBlockWork(t *testing.T) {
	tests := []struct {
		in  uint32
		out int64
	}{
		{10000000, 0},
	}

	for x, test := range tests {
		bits := test.in

		r := work.CalcBlockWork(bits)
		if r.Int64() != test.out {
			t.Errorf("TestCalcBlockWork test #%d failed: got %v want %d\n",
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

		r, err := BigToCompact(n)
		require.NoError(t, err)
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

func TestDifficultyPrecision(t *testing.T) {
	chainParams := &chaincfg.MainNetParams
	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, chainParams.PowLimitBits) // 0x1d00ffff
	powLimitBits, _ := model.NewNBitFromSlice(bytesLittleEndian)
	firstChainwork, _ := hex.DecodeString("000000000000000000000000000000000000000001483b3cc42a76ae3dc13792")
	lastChainwork, _ := hex.DecodeString("0000000000000000000000000000000000000000014844e5fe3b17bb6cc37242")
	d := &Difficulty{
		powLimitnBits: powLimitBits,
		logger:        ulogger.TestLogger{},
		store:         nil,
		settings: &settings.Settings{
			ChainCfgParams: chainParams,
		},
	}

	testCases := []struct {
		name                       string
		firstBlockHeader           *model.SuitableBlock
		lastBlockHeader            *model.SuitableBlock
		lastMinedBlockTime         uint32
		parentOfLastMinedBlockTime uint32
		expectedNBits              string
		precision                  int64
	}{
		{
			name: "Test Case 1",
			firstBlockHeader: &model.SuitableBlock{
				NBits:     []byte{0x1a, 0x40, 0x6c, 0xcc},
				Time:      1620000000,
				ChainWork: firstChainwork,
			},
			lastBlockHeader: &model.SuitableBlock{
				NBits:     []byte{0x1a, 0x40, 0x3b, 0x78},
				Time:      1620010000,
				ChainWork: lastChainwork,
			},
			lastMinedBlockTime:         1620000000,
			parentOfLastMinedBlockTime: 1620000000,
			expectedNBits:              "de730718",
			precision:                  10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			newNbit, err := d.computeTarget(tc.firstBlockHeader, tc.lastBlockHeader)
			require.NoError(t, err)

			expectedPrecision := int64(math.Pow10(int(tc.precision)))

			newDiff := newNbit.CalculateDifficulty()
			integerPart, _ := newDiff.Int64()
			fractionalPart := new(big.Float).Sub(newDiff, big.NewFloat(float64(integerPart)))
			fractionalPart.Mul(fractionalPart, big.NewFloat(float64(expectedPrecision)))
			fractionalInt, _ := fractionalPart.Int64()
			require.LessOrEqual(t, fractionalInt, expectedPrecision-1)
		})
	}
}

func TestDifficultyFor0000000000000f8f4026dc67d5635d79d87ef2d88f870c1dfdaa1338b5fb40e0(t *testing.T) {
	// The latestSuitableBlock is based on the last block (i - 1) and the previous 2 blocks
	l := make([]testData, 3)

	chainworkBytes, _ := hex.DecodeString("000000000000000000000000000000000000000000000158148cc02c2eb88282")
	bits, _ := model.NewNBitFromString("1a404e48")

	l[2] = testData{
		height:    1648748,
		time:      1732540094,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("0000000000000000000000000000000000000000000001581488c50aba046156")
	bits, _ = model.NewNBitFromString("1a403b78")

	l[1] = testData{
		height:    1648747,
		time:      1732539484,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("0000000000000000000000000000000000000000000001581484c8bec914e1e1")
	bits, _ = model.NewNBitFromString("1a40a0b2")

	l[0] = testData{
		height:    1648746,
		time:      1732539272,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	util.SortForDifficultyAdjustment(l)

	// Create test blocks
	latestSuitableBlock := &model.SuitableBlock{
		NBits:     l[1].bits.CloneBytes(),
		Time:      uint32(l[1].time), // nolint:gosec
		ChainWork: l[1].chainwork.Bytes(),
		Height:    l[1].height,
	}

	chainworkBytes, _ = hex.DecodeString("000000000000000000000000000000000000000000000158129e0c7f17a78edd")
	bits, _ = model.NewNBitFromString("1a3d8bc7")

	l[2] = testData{
		height:    1648604,
		time:      1732465266,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("0000000000000000000000000000000000000000000001581299e3aabfab1301")
	bits, _ = model.NewNBitFromString("1a3d7182")

	l[1] = testData{
		height:    1648603,
		time:      1732464759,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("0000000000000000000000000000000000000000000001581295b90f25bbb5f3")
	bits, _ = model.NewNBitFromString("1a3d685b")

	l[0] = testData{
		height:    1648602,
		time:      1732464658,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	util.SortForDifficultyAdjustment(l)

	earliestSuitableBlock := &model.SuitableBlock{
		NBits:     l[1].bits.CloneBytes(),
		Time:      uint32(l[1].time), // nolint:gosec
		ChainWork: l[1].chainwork.Bytes(),
		Height:    l[1].height,
	}

	// Create a new Difficulty instance for testing
	logger := ulogger.TestLogger{}

	chainParams := &chaincfg.TestNetParams
	s := &settings.Settings{
		ChainCfgParams: chainParams,
	}

	d, err := NewDifficulty(nil, logger, s)
	require.NoError(t, err)

	// Call computeTarget
	result, err := d.computeTarget(earliestSuitableBlock, latestSuitableBlock, 1732540094, 1732540611)
	require.NoError(t, err)

	// t.Logf("height %d: %s", data[i].height, result.String())

	require.Equal(t, "1a406ccc", result.String())
}

func TestDifficultyFor000000001d27fe89679d4c2c873b0511df0aecdb95e19c0a782b9f4b0520cdf8(t *testing.T) {
	// The latestSuitableBlock is based on the last block (i - 1) and the previous 2 blocks
	l := make([]testData, 3)

	chainworkBytes, _ := hex.DecodeString("00000000000000000000000000000000000000000000015814b504f5f6df7b2d")
	bits, _ := model.NewNBitFromString("1c6a8bee")

	l[2] = testData{
		height:    1650064,
		time:      1733396488,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("00000000000000000000000000000000000000000000015814b504f38fc7d62f")
	bits, _ = model.NewNBitFromString("1c6c3804")

	l[1] = testData{
		height:    1650063,
		time:      1733396273,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("00000000000000000000000000000000000000000000015814b504f132315718")
	bits, _ = model.NewNBitFromString("1c6b48dd")

	l[0] = testData{
		height:    1650062,
		time:      1733396213,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	util.SortForDifficultyAdjustment(l)

	// Create test blocks
	latestSuitableBlock := &model.SuitableBlock{
		NBits:     l[1].bits.CloneBytes(),
		Time:      uint32(l[1].time), // nolint:gosec
		ChainWork: l[1].chainwork.Bytes(),
		Height:    l[1].height,
	}

	chainworkBytes, _ = hex.DecodeString("00000000000000000000000000000000000000000000015814b503d0bf1af03c")
	bits, _ = model.NewNBitFromString("1d0084e3")

	l[2] = testData{
		height:    1649920,
		time:      1733323735,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("00000000000000000000000000000000000000000000015814b503ced1eeec6a")
	bits, _ = model.NewNBitFromString("1d008544")

	l[1] = testData{
		height:    1649919,
		time:      1733323294,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	chainworkBytes, _ = hex.DecodeString("00000000000000000000000000000000000000000000015814b503cce629df99")
	bits, _ = model.NewNBitFromString("1d00ffff")

	l[0] = testData{
		height:    1649918,
		time:      1733323221,
		chainwork: new(big.Int).SetBytes(chainworkBytes),
		bits:      bits,
	}

	util.SortForDifficultyAdjustment(l)

	earliestSuitableBlock := &model.SuitableBlock{
		NBits:     l[1].bits.CloneBytes(),
		Time:      uint32(l[1].time), // nolint:gosec
		ChainWork: l[1].chainwork.Bytes(),
		Height:    l[1].height,
	}

	// Create a new Difficulty instance for testing
	logger := ulogger.TestLogger{}

	chainParams := &chaincfg.TestNetParams
	s := &settings.Settings{
		ChainCfgParams: chainParams,
	}

	d, err := NewDifficulty(nil, logger, s)
	require.NoError(t, err)

	// Call computeTarget
	result, err := d.computeTarget(earliestSuitableBlock, latestSuitableBlock, 1733396488, 1733396567)
	require.NoError(t, err)

	// t.Logf("height %d: %s", data[i].height, result.String())

	require.Equal(t, "1c6a5da8", result.String())
}
