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
	require.Equal(t, "0.0003068360688", difficulty.String())

	target := bits.CalculateTarget()
	require.Equal(t, "87862992749702277876753291758735394717545048148536728461472937357082624", target.String())
}
func TestCalculateTarget(t *testing.T) {
	bits, err := NewNBitFromString("180f7f7d") // block #869334
	require.NoError(t, err)

	difficulty, _ := bits.CalculateDifficulty().Float32()
	expectedDifficulty, _ := big.NewFloat(70944300723.85233).Float32()
	require.Equal(t, expectedDifficulty, difficulty)

	target := bits.CalculateTarget()
	require.Equal(t, "380009881215830907712605183958726704270100120947772096512", target.String())
}
