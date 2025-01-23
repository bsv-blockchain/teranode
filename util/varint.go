package util

// Calculate the number of bytes required to store a value as a varint.
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
