package model

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
1. The bits "1e0cbb05" is a hexadecimal value.
We extract the mantissa by performing a bitwise AND operation with 0x00FFFFFF:
	0x1e0cbb05 & 0x00FFFFFF = 0x00cbb05
3. We extract the exponent by performing a bitwise AND operation with 0xFF000000, right-shifting by 24 bits, and subtracting 3:
	(0x1e0cbb05 & 0xFF000000) >> 24 = 0x1e
	0x1e - 3 = 0x1b = 27 (decimal)
We calculate the mantissa part:
	0x00FFFFFF / 0x00cbb05 ≈ 3259.99291729 (decimal)
We calculate the exponent part:
	2^27 = 134217728 (decimal)
6. Finally, we multiply the mantissa and exponent parts:
	3259.99291729 134217728 ≈ 437590082.56 (decimal)
Therefore, the difficulty corresponding to the bits "1e0cbb05" is approximately 437590082.56.
*/
// The expected difficulty is "0.0003068360688", which is the reciprocal of the calculated difficulty (1 / 437590082.56 ≈ 0.0003068360688)
func TestNBit(t *testing.T) {
	bits, err := NewNBitFromString("1e0cbb05")
	require.NoError(t, err)
	require.Equal(t, "1e0cbb05", bits.String())
	difficulty := bits.CalculateDifficulty()
	// Standard Bitcoin difficulty calculation
	require.Equal(t, "0.0003068360688", difficulty.String())

	target := bits.CalculateTarget()
	require.Equal(t, "87862992749702277876753291758735394717545048148536728461472937357082624", target.String())
}
func TestCalculateTarget(t *testing.T) {
	bits, err := NewNBitFromString("180f7f7d") // block #869334
	require.NoError(t, err)

	difficulty, _ := bits.CalculateDifficulty().Float32()
	// Standard Bitcoin difficulty calculation
	expectedDifficulty, _ := big.NewFloat(70944300723.85233).Float32()
	// Use InDelta instead of Equal due to float32 precision
	require.InDelta(t, expectedDifficulty, difficulty, 1.0)

	target := bits.CalculateTarget()
	require.Equal(t, "380009881215830907712605183958726704270100120947772096512", target.String())
}

func TestCalculateTarget_NegativeNBitsRejected(t *testing.T) {
	t.Run("sign bit set returns zero target", func(t *testing.T) {
		// nBits 0x1d800001 has the mantissa sign bit (0x00800000) set.
		// Pre-fix this returned a small positive target (mantissa masked, then shifted).
		// Post-fix this is rejected with a zero target so PoW comparison can never succeed.
		bits, err := NewNBitFromString("1d800001")
		require.NoError(t, err)

		target := bits.CalculateTarget()
		require.Equal(t, 0, target.Sign(), "negative-encoded nBits must yield zero target")
		require.Equal(t, 0, target.Cmp(big.NewInt(0)))
	})

	t.Run("sign bit set with non-trivial mantissa returns zero target", func(t *testing.T) {
		// 0x1d8fffff — sign bit set, large mantissa, would be a tempting "easy" target if masked.
		bits, err := NewNBitFromString("1d8fffff")
		require.NoError(t, err)

		target := bits.CalculateTarget()
		require.Equal(t, 0, target.Sign())
	})

	t.Run("sign bit set with zero mantissa is just zero, not negative", func(t *testing.T) {
		// 0x1d800000 — sign bit set but mantissa is zero. The Bitcoin Protocol treats
		// this as the value zero (fNegative requires nWord != 0). The end-state
		// is a zero target either way, but we exercise the precondition here.
		bits, err := NewNBitFromString("1d800000")
		require.NoError(t, err)

		target := bits.CalculateTarget()
		require.Equal(t, 0, target.Sign())
	})

	t.Run("genesis nBits 0x1d00ffff returns expected positive target", func(t *testing.T) {
		// Bitcoin genesis nBits — sign bit clear, well-trodden happy path.
		bits, err := NewNBitFromString("1d00ffff")
		require.NoError(t, err)

		target := bits.CalculateTarget()
		require.Equal(t, 1, target.Sign())
		// 0x00ffff << (8 * (0x1d - 3)) = 0xffff0000000000000000000000000000000000000000000000000000
		expected, ok := new(big.Int).SetString("00000000FFFF0000000000000000000000000000000000000000000000000000", 16)
		require.True(t, ok)
		require.Equal(t, 0, target.Cmp(expected))
	})
}

func TestCalculateTarget_OverflowRejected(t *testing.T) {
	// Overflow rules per SVNode's arith_uint256::SetCompact:
	//   - exponent > 34            → any non-zero mantissa overflows 2^256
	//   - mantissa > 0xff   && exp > 33
	//   - mantissa > 0xffff && exp > 32
	cases := []struct {
		name  string
		nBits string
	}{
		{"exponent 35 overflows with smallest mantissa", "23000001"},
		{"exponent 36 overflows", "24000001"},
		{"exponent 34 with mantissa 0x000100 overflows", "22000100"},
		{"exponent 34 with max mantissa overflows", "227fffff"},
		{"exponent 33 with mantissa 0x010000 overflows", "21010000"},
		{"exponent 33 with max mantissa overflows", "217fffff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bits, err := NewNBitFromString(tc.nBits)
			require.NoError(t, err)
			require.Equal(t, 0, bits.CalculateTarget().Sign(),
				"overflowing nBits must yield zero target")
		})
	}
}

func TestCalculateTarget_OverflowBoundary(t *testing.T) {
	// Values exactly at the 256-bit boundary — must NOT be flagged as overflow.
	cases := []struct {
		name  string
		nBits string
	}{
		{"exponent 34 with mantissa 0x0000ff fits (8 + 248 = 256 bits)", "220000ff"},
		{"exponent 33 with mantissa 0x00ffff fits (16 + 240 = 256 bits)", "2100ffff"},
		{"exponent 32 with mantissa 0x7fffff fits (23 + 232 = 255 bits)", "207fffff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bits, err := NewNBitFromString(tc.nBits)
			require.NoError(t, err)
			require.Equal(t, 1, bits.CalculateTarget().Sign(),
				"boundary nBits must yield positive target")
		})
	}
}

func TestBlock911636Difficulty(t *testing.T) {
	// This test verifies the standard Bitcoin difficulty calculation
	// Block 911636 has nBits value 0x180f9ff5
	// Note: SVNode may report a different value (~35858832210.37) which appears
	// to be incorrect based on the SVNode C++ source code analysis

	bits, err := NewNBitFromString("180f9ff5")
	require.NoError(t, err)

	difficulty := bits.CalculateDifficulty()
	difficultyFloat, _ := difficulty.Float64()

	// Expected difficulty using standard Bitcoin algorithm
	// This matches what SVNode C++ code produces
	expectedDifficulty := 70368426346.669891357421875

	t.Logf("nBits: 0x180f9ff5")
	t.Logf("Calculated difficulty: %.10f", difficultyFloat)
	t.Logf("Expected difficulty: %.10f", expectedDifficulty)
	t.Logf("Difference: %.10f", difficultyFloat-expectedDifficulty)
	t.Logf("Percentage difference: %.6f%%", ((difficultyFloat-expectedDifficulty)/expectedDifficulty)*100)

	// The tolerance is set to 0.001 to allow for minor floating point differences
	require.InDelta(t, expectedDifficulty, difficultyFloat, 0.001,
		"Difficulty calculation for block 911636 (nBits: 0x180f9ff5)")
}
