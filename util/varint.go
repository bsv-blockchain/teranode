package util

// VarintSize calculates the number of bytes required to store a value as a Bitcoin variable-length integer.
// Returns 1, 3, 5, or 9 bytes depending on the value size.
func VarintSize(x uint64) uint64 {
	if x < 0xfd {
		return 1
	}

	if x <= 0xffff {
		return 3
	}

	if x <= 0xffffffff {
		return 5
	}

	return 9
}
