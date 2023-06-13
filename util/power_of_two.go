package util

import "math"

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
