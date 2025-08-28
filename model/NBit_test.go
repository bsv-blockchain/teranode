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

func TestBlock911636Difficulty(t *testing.T) {
	// This test verifies the standard Bitcoin difficulty calculation
	// Block 911636 has nBits value 0x180f9ff5
	// Note: SVNode may report a different value (~35858832210.37) which appears
	// to be incorrect based on the Bitcoin SV C++ source code analysis

	bits, err := NewNBitFromString("180f9ff5")
	require.NoError(t, err)

	difficulty := bits.CalculateDifficulty()
	difficultyFloat, _ := difficulty.Float64()

	// Expected difficulty using standard Bitcoin algorithm
	// This matches what Bitcoin Core and Bitcoin SV C++ code produces
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
