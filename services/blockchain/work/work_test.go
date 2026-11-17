package work

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalcBlockWork(t *testing.T) {
	tests := []struct {
		name         string
		bits         uint32
		expectedWork string // hex string of expected work value
		expectsZero  bool
		description  string
	}{
		{
			name:         "Genesis block difficulty",
			bits:         0x1d00ffff,
			expectedWork: "0000000000000000000000000000000000000000000000000000000100010001",
			description:  "Bitcoin genesis block difficulty bits",
		},
		{
			name:         "Mainnet typical difficulty",
			bits:         0x1a05db8b,
			expectedWork: "000000000000000000000000000000000000000000000000002bb43836381c9c",
			description:  "Typical mainnet block difficulty",
		},
		{
			name:         "High difficulty",
			bits:         0x17053894,
			expectedWork: "0000000000000000000000000000000000000000000031085d594cb7e26e94b5",
			description:  "Higher difficulty requires more work",
		},
		{
			name:         "Low difficulty",
			bits:         0x207fffff,
			expectedWork: "0000000000000000000000000000000000000000000000000000000000000002",
			description:  "Maximum target (lowest difficulty)",
		},
		{
			name:        "Invalid negative target",
			bits:        0x01800000,
			expectsZero: true,
			description: "Negative target should return zero work",
		},
		{
			name:        "Zero target",
			bits:        0x00000000,
			expectsZero: true,
			description: "Zero target should return zero work",
		},
		{
			name:         "BSV regtest difficulty",
			bits:         0x207fffff,
			expectedWork: "0000000000000000000000000000000000000000000000000000000000000002",
			description:  "BSV regtest network difficulty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			work := CalcBlockWork(tt.bits)
			require.NotNil(t, work, "CalcBlockWork should never return nil")

			if tt.expectsZero {
				assert.Equal(t, big.NewInt(0), work, "Expected zero work for invalid target")
			} else {
				expectedBytes, err := hex.DecodeString(tt.expectedWork)
				require.NoError(t, err, "Failed to decode expected work hex")
				expected := new(big.Int).SetBytes(expectedBytes)

				assert.Equal(t, expected, work,
					"Work mismatch for %s: expected %s, got %s",
					tt.description, expected.Text(16), work.Text(16))
			}
		})
	}
}

func TestCalcBlockWork_Formula(t *testing.T) {
	// Test that the formula work = 2^256 / (target + 1) is correct
	bits := uint32(0x1d00ffff)

	// Calculate work using the function
	work := CalcBlockWork(bits)

	// Manually calculate expected work
	bytesLittleEndian := make([]byte, 4)
	bytesLittleEndian[0] = byte(bits)
	bytesLittleEndian[1] = byte(bits >> 8)
	bytesLittleEndian[2] = byte(bits >> 16)
	bytesLittleEndian[3] = byte(bits >> 24)

	nBit, err := model.NewNBitFromSlice(bytesLittleEndian)
	require.NoError(t, err)

	target := nBit.CalculateTarget()
	denominator := new(big.Int).Add(target, big.NewInt(1))
	expectedWork := new(big.Int).Div(new(big.Int).Lsh(big.NewInt(1), 256), denominator)

	assert.Equal(t, expectedWork, work, "Work calculation should match formula: 2^256 / (target + 1)")
}

func TestCalcBlockWork_InvalidNBit(t *testing.T) {
	// Test with bits that result in invalid NBit
	// This should be handled gracefully and return 0
	bits := uint32(0x01003456) // Invalid compact representation

	work := CalcBlockWork(bits)
	require.NotNil(t, work)

	// The function should handle this gracefully
	// Either returning 0 or some valid work value
	assert.True(t, work.Cmp(big.NewInt(0)) >= 0, "Work should never be negative")
}

func TestCalculateWork(t *testing.T) {
	tests := []struct {
		name         string
		prevWork     string // hex string
		nBits        string // NBit string
		expectedWork string // hex string
		description  string
	}{
		{
			name:         "Genesis block (zero previous work)",
			prevWork:     "0000000000000000000000000000000000000000000000000000000000000000",
			nBits:        "1d00ffff",
			expectedWork: "0100010001000000000000000000000000000000000000000000000000000000",
			description:  "First block with no previous work",
		},
		{
			name:         "Second block",
			prevWork:     "0100010001000000000000000000000000000000000000000000000000000000",
			nBits:        "1d00ffff",
			expectedWork: "0200020002000000000000000000000000000000000000000000000000000000",
			description:  "Adding work to existing chain",
		},
		{
			name:         "Increasing difficulty",
			prevWork:     "0000000000000000000000000000000000000000000000000001000000000000",
			nBits:        "1a05db8b",
			expectedWork: "9c1c383638b42b00000000000000000000000000000000000001000000000000",
			description:  "Higher difficulty adds more work",
		},
		{
			name:         "Large cumulative work",
			prevWork:     "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			nBits:        "207fffff",
			expectedWork: "0100000000000000000000000000000000000000000000000000000000000000",
			description:  "Work accumulation should handle overflow correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse previous work
			prevWorkBytes, err := hex.DecodeString(tt.prevWork)
			require.NoError(t, err, "Failed to decode previous work hex")
			prevWorkHash, err := chainhash.NewHash(prevWorkBytes)
			require.NoError(t, err, "Failed to create previous work hash")

			// Parse nBits
			nBit, err := model.NewNBitFromString(tt.nBits)
			require.NoError(t, err, "Failed to parse nBits")

			// Calculate work
			newWork, err := CalculateWork(prevWorkHash, *nBit)
			require.NoError(t, err, "CalculateWork should not return error")
			require.NotNil(t, newWork, "CalculateWork should not return nil")

			// Parse expected work
			expectedBytes, err := hex.DecodeString(tt.expectedWork)
			require.NoError(t, err, "Failed to decode expected work hex")
			expectedHash, err := chainhash.NewHash(expectedBytes)
			require.NoError(t, err, "Failed to create expected work hash")

			// Compare
			assert.Equal(t, expectedHash.String(), newWork.String(),
				"Work mismatch for %s", tt.description)
		})
	}
}

func TestCalculateWork_Accumulation(t *testing.T) {
	// Test that work accumulates correctly over multiple blocks
	prevWork := chainhash.Hash{}
	nBit, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	// Calculate work for 10 blocks
	for i := 0; i < 10; i++ {
		newWork, err := CalculateWork(&prevWork, *nBit)
		require.NoError(t, err, "Block %d: CalculateWork failed", i)
		require.NotNil(t, newWork, "Block %d: work is nil", i)

		// Verify that new work is greater than previous work
		prevWorkBig := new(big.Int).SetBytes(bt.ReverseBytes(prevWork.CloneBytes()))
		newWorkBig := new(big.Int).SetBytes(bt.ReverseBytes(newWork.CloneBytes()))

		assert.True(t, newWorkBig.Cmp(prevWorkBig) > 0,
			"Block %d: new work should be greater than previous work", i)

		prevWork = *newWork
	}
}

func TestCalculateWork_ByteOrdering(t *testing.T) {
	// Test that byte ordering is handled correctly (little-endian)
	prevWork := chainhash.Hash{}
	nBit, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	newWork, err := CalculateWork(&prevWork, *nBit)
	require.NoError(t, err)
	require.NotNil(t, newWork)

	// Verify that the result is properly reversed for hash representation
	// The internal calculation uses little-endian, but chainhash uses the correct ordering
	workBig := new(big.Int).SetBytes(bt.ReverseBytes(newWork.CloneBytes()))
	assert.True(t, workBig.Cmp(big.NewInt(0)) > 0, "Work should be positive")
}

func TestCalculateWork_ZeroDifficulty(t *testing.T) {
	// Test with zero/invalid difficulty
	prevWork := chainhash.Hash{}

	// Create an NBit with zero value
	nBit, err := model.NewNBitFromSlice([]byte{0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)

	newWork, err := CalculateWork(&prevWork, *nBit)
	require.NoError(t, err, "CalculateWork should handle zero difficulty gracefully")
	require.NotNil(t, newWork, "CalculateWork should not return nil")

	// With zero/invalid target, CalcBlockWork returns 0, so work should equal prevWork
	assert.Equal(t, prevWork.String(), newWork.String(),
		"With zero block work, cumulative work should remain unchanged")
}

func TestCalculateWork_NegativeTarget(t *testing.T) {
	// Test with a negative target (invalid)
	prevWork := chainhash.Hash{}

	// Create an NBit that would result in a negative target
	// 0x01800000 has the sign bit set in compact form
	bytesLittleEndian := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesLittleEndian, 0x01800000)
	nBit, err := model.NewNBitFromSlice(bytesLittleEndian)
	require.NoError(t, err)

	newWork, err := CalculateWork(&prevWork, *nBit)
	require.NoError(t, err, "CalculateWork should handle negative target gracefully")
	require.NotNil(t, newWork, "CalculateWork should not return nil")
}

func TestCalculateWork_RealWorldScenario(t *testing.T) {
	// Test with real Bitcoin block data
	// Block 1 (after genesis)
	prevWork := chainhash.Hash{} // Genesis has 0 previous work
	nBit, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	block1Work, err := CalculateWork(&prevWork, *nBit)
	require.NoError(t, err)
	require.NotNil(t, block1Work)

	// Block 2
	block2Work, err := CalculateWork(block1Work, *nBit)
	require.NoError(t, err)
	require.NotNil(t, block2Work)

	// Verify work is accumulating
	work1Big := new(big.Int).SetBytes(bt.ReverseBytes(block1Work.CloneBytes()))
	work2Big := new(big.Int).SetBytes(bt.ReverseBytes(block2Work.CloneBytes()))

	// Block 2 work should be approximately 2x block 1 work (same difficulty)
	expectedWork2 := new(big.Int).Mul(work1Big, big.NewInt(2))
	assert.Equal(t, expectedWork2, work2Big,
		"Block 2 work should be 2x block 1 work with same difficulty")
}

func TestCalculateWork_DifferentDifficulties(t *testing.T) {
	// Test that higher difficulty adds more work
	prevWork := chainhash.Hash{}

	lowDiff, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	highDiff, err := model.NewNBitFromString("1a05db8b")
	require.NoError(t, err)

	lowDiffWork, err := CalculateWork(&prevWork, *lowDiff)
	require.NoError(t, err)

	highDiffWork, err := CalculateWork(&prevWork, *highDiff)
	require.NoError(t, err)

	lowWorkBig := new(big.Int).SetBytes(bt.ReverseBytes(lowDiffWork.CloneBytes()))
	highWorkBig := new(big.Int).SetBytes(bt.ReverseBytes(highDiffWork.CloneBytes()))

	assert.True(t, highWorkBig.Cmp(lowWorkBig) > 0,
		"Higher difficulty should result in more work")
}

func TestTarget(t *testing.T) {
	// Original test - kept for compatibility
	bits, _ := model.NewNBitFromString("2000ffff")

	target := bits.CalculateTarget()

	s := fmt.Sprintf("%064x", target.Bytes())

	t.Logf("Target: %s, len: %d", s, len(s)/2)

	// Verify the target format
	assert.NotEmpty(t, s, "Target should not be empty")
	assert.Equal(t, 64, len(s), "Target should be 64 hex characters (32 bytes)")
}

func BenchmarkCalcBlockWork(b *testing.B) {
	bits := uint32(0x1d00ffff)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		CalcBlockWork(bits)
	}
}

func BenchmarkCalculateWork(b *testing.B) {
	prevWork := chainhash.Hash{}
	nBit, _ := model.NewNBitFromString("1d00ffff")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = CalculateWork(&prevWork, *nBit)
	}
}

func TestCompareChainWork(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    []byte
		want int
	}{
		{name: "greater", a: []byte{0x02}, b: []byte{0x01}, want: 1},
		{name: "less", a: []byte{0x01}, b: []byte{0x02}, want: -1},
		{name: "equal", a: []byte{0x05}, b: []byte{0x05}, want: 0},
		{name: "nonempty vs empty is greater", a: []byte{0x01}, b: []byte{}, want: 1},
		{name: "empty vs empty is equal", a: []byte{}, b: []byte{}, want: 0},
		{name: "empty vs nonempty is less", a: []byte{}, b: []byte{0x01}, want: -1},
		{name: "nil vs nil is equal", a: nil, b: nil, want: 0},
		{name: "nil vs nonempty is less", a: nil, b: []byte{0x01}, want: -1},
		{name: "nonempty vs nil is greater", a: []byte{0x01}, b: nil, want: 1},
		{name: "empty equals all-zero bytes", a: []byte{}, b: []byte{0x00, 0x00}, want: 0},
		{name: "leading zeros are insignificant", a: []byte{0x00, 0x00, 0x01}, b: []byte{0x01}, want: 0},
		{name: "length mismatch compares by value", a: []byte{0x01, 0x00}, b: []byte{0xff}, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, CompareChainWork(tt.a, tt.b))
		})
	}
}
