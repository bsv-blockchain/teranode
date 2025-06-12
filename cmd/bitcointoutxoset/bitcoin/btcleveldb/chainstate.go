package btcleveldb

import "math"

// VarInt128Read reads a variable-length integer from a byte slice using Bitcoin's LevelDB varInt128 encoding.
//
// This function scans a byte slice starting from the given offset, collecting bytes that make up a varInt128-encoded integer.
// Each byte contains 7 bits of data, and the highest bit (0x80) indicates whether more bytes follow.
// The function returns the collected bytes representing the varInt128, and the number of bytes read.
//
// Parameters:
//   - bytes: The byte slice containing the varInt128-encoded integer.
//   - offset: The starting index in the byte slice to begin reading.
//
// Returns:
//   - A byte slice containing the varInt128-encoded integer.
//   - The number of bytes read from the input slice.
//
// Behavior:
//   - Iterates through the input slice starting at the given offset.
//   - Appends each byte to the result until a byte with the highest bit not set is found (end of varInt128).
//   - Returns the collected bytes and the count of bytes read.
//   - If the end of the slice is reached without finding a terminating byte, returns the collected bytes and 0.
//
// Example:
//
//	data := []byte{0x81, 0x01, 0xFF}
//	varInt, n := VarInt128Read(data, 0) // varInt = []byte{0x81, 0x01}, n = 2
func VarInt128Read(bytes []byte, offset int) ([]byte, int) { // take a byte array and return (byte array and number of bytes read)
	// Pre-allocate result slice with the maximum possible length
	result := make([]byte, 0, len(bytes)-offset)

	// loop through bytes
	for _, v := range bytes[offset:] { // start reading from an offset
		// store each byte as you go
		result = append(result, v)

		// Bitwise AND each of them with 128 (0b10000000) to check if the 8th bit has been set
		set := v & 128 // 0b10000000 is the same as 1 << 7

		// When you get to one without the 8th bit set, return that byte slice
		if set == 0 {
			// Also return the number of bytes read
			return result, len(result)
		}
	}

	// Return zero bytes read if we haven't managed to read bytes properly
	return result, 0
}

// VarInt128Decode decodes a variable-length integer from a byte slice using Bitcoin's LevelDB varInt128 encoding.
//
// This function interprets a byte slice as a varInt128-encoded integer, as used in Bitcoin's chainstate LevelDB.
// The encoding allows for compact storage of integers, where each byte contains 7 bits of data, and the highest bit
// indicates whether more bytes follow.
//
// Parameters:
//   - bytes: The byte slice containing the varInt128-encoded integer.
//
// Returns:
//   - The decoded integer value as int64.
//
// Behavior:
//   - Iterates through each byte, shifting and accumulating the lower 7 bits of each byte into the result.
//   - If the highest bit (0x80) of a byte is set, increments the result and continues to the next byte.
//   - Stops when a byte with the highest bit not set is encountered.
//   - Returns the final decoded integer value.
//
// Example:
//
//	encoded := []byte{0x81, 0x01}
//	value := VarInt128Decode(encoded) // value will be 129
func VarInt128Decode(bytes []byte) int64 { // takes a byte slice, returns an int64 (makes sure it works on 32-bit systems)
	// total
	var n int64 = 0

	for _, v := range bytes {
		// 1. shift n left 7 bits (add some extra bits to work with)
		//                             00000000
		n <<= 7

		// 2. set the last 7 bits of each byte in to the total value
		//    AND extracts 7 bits only 10111001  <- these are the bits of each byte
		//                              1111111
		//                              0111001  <- don't want the 8th bit (just indicated if there were more bytes in the varInt)
		//    OR sets the 7 bits
		//                             00000000  <- the result
		//                              0111001  <- the bits we want to set
		//                             00111001
		n |= int64(v & 127)

		// 3. add 1 each time (only for the ones where the 8th bit is set)
		if v&128 != 0 { // 0b10000000 <- AND to check if the 8th bit is set
			// 1 << 7 <- could always bit shift to get 128
			n++
		}
	}

	// 11101000000111110110
	return n
}

// DecompressValue decompresses a compressed integer value from Bitcoin's chainstate LevelDB.
//
// This function reverses the compression scheme used by Bitcoin Core to store UTXO values efficiently in LevelDB.
// The compression algorithm is designed to save space for common values (such as powers of ten and small values).
//
// Parameters:
//   - x: The compressed integer value (as stored in LevelDB).
//
// Returns:
//   - The decompressed integer value.
//
// Behavior:
//   - If x is zero, returns zero immediately (no decompression needed).
//   - Otherwise, applies the decompression algorithm:
//   - Decrements x by 1.
//   - Extracts the exponent (e) as x % 10, and reduces x by dividing by 10.
//   - If e < 9, further splits x to extract a digit (d) and computes the decompressed value as n = x*10 + d + 1.
//   - If e == 9, computes n = x + 1.
//   - The final result is n * 10^e.
//   - Returns the decompressed value as int64.
//
// Example:
//
//	compressed := int64(27)
//	value := DecompressValue(compressed) // value will be the original integer before compression
func DecompressValue(x int64) int64 {
	var n int64 // decompressed value

	// Return value if it is zero (nothing to decompress)
	if x == 0 {
		return 0
	}

	// Decompress...
	x--         // subtract 1 first
	e := x % 10 // remainder mod 10
	x /= 10     // quotient mod 10 (reduce x down by 10)

	// If the remainder is less than 9
	if e < 9 {
		d := x % 9       // remainder mod 9
		x /= 9           // (reduce x down by 9)
		n = x*10 + d + 1 // work out n
	} else {
		n = x + 1
	}

	// Multiply n by 10 to the power of the first remainder
	result := float64(n) * math.Pow(10, float64(e)) // math.Pow takes a float and returns a float

	// manual exponentiation
	// multiplier := 1
	// for i := 0; i < e; i++ {
	//     multiplier *= 10
	// }

	return int64(result)
}
