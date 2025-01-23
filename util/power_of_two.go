package util

import (
	"math"
	"math/bits"
)

func CeilPowerOfTwo(num int) int {
	if num <= 0 {
		return 1
	}

	// Find the position of the most significant bit
	msbPos := uint(math.Ceil(math.Log2(float64(num))))

	// Calculate the power of 2 with the next higher position
	ceilValue := int(math.Pow(2, float64(msbPos)))

	return ceilValue
}

func IsPowerOfTwo(num int) bool {
	if num <= 0 {
		return false
	}

	return (num & (num - 1)) == 0
}

// NextPowerOfTwo returns the next highest power of two from a given number if
// it is not already a power of two.  This is a helper function used during the
// calculation of a merkle tree.
func NextPowerOfTwo(n int) int {
	// Return the number if it's already a power of 2.
	if n&(n-1) == 0 {
		return n
	}

	// Figure out and return the next power of two.
	exponent := uint(math.Log2(float64(n))) + 1

	return 1 << exponent // 2^exponent
}

// NextLowerPowerOfTwo finds the next power of 2 that is less than x.
func NextLowerPowerOfTwo(x uint) uint {
	if x == 0 {
		return 0
	}

	// bit length minus one gives the exponent of the next lower (or equal) power of 2
	return 1 << (bits.Len(x) - 1)
}
