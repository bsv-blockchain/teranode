// Package keys provides utilities for working with Bitcoin public keys, including decompression of compressed secp256k1 public keys.
//
// Usage:
//
//	This package is typically used to decompress compressed public keys found in P2PK scripts and to handle related key operations
//	required for UTXO set extraction, analysis, or migration tools.
//
// Functions:
//   - DecompressPublicKey: Decompresses a compressed secp256k1 public key (33 bytes) to its uncompressed form (65 bytes).
//     Useful for reconstructing full public keys from their compressed representation in Bitcoin scripts.
//
// Side effects:
//
//	Functions in this package are pure and do not perform I/O or modify external state.
package keys

import (
	"math/big"
)

// DecompressPublicKey decompresses a compressed secp256k1 public key from a P2PK script to its uncompressed form.
//
// This function reconstructs the full (x, y) coordinates of a secp256k1 public key from its compressed representation.
// The compressed key consists of a prefix byte (0x02 or 0x03) indicating the parity of the y coordinate, followed by the x coordinate.
//
// Parameters:
//   - publickey: A byte slice containing the compressed public key (33 bytes: 1 prefix + 32 x-coordinate).
//
// Returns:
//   - A byte slice containing the uncompressed public key (65 bytes: 0x04 prefix + 32 x + 32 y).
//
// Behavior:
//   - Parses the prefix to determine the required parity of the y coordinate.
//   - Computes y^2 = x^3 + 7 mod p, where p is the secp256k1 field prime.
//   - Calculates the modular square root of y^2 to obtain y.
//   - Selects the correct y value based on the prefix (even/odd parity).
//   - Returns the uncompressed public key as [0x04 | x | y].
//
// Example:
//
//	compressed := []byte{0x02, ...32 bytes...}
//	uncompressed := DecompressPublicKey(compressed)
func DecompressPublicKey(publickey []byte) []byte { // decompressing public keys from P2PK scripts
	// first byte (indicates whether y is even or odd)
	prefix := publickey[0:1]

	// remaining bytes (x coordinate)
	x := publickey[1:]

	// y^2 = x^3 + 7 mod p
	p, _ := new(big.Int).SetString("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 0)
	xInt := new(big.Int).SetBytes(x)
	x3 := new(big.Int).Exp(xInt, big.NewInt(3), p)
	ySq := new(big.Int).Add(x3, big.NewInt(7))
	ySq = new(big.Int).Mod(ySq, p)

	// square root of y - secp256k1 is chosen so that the square root of y is y^((p+1)/4)
	y := new(big.Int).Exp(ySq, new(big.Int).Div(new(big.Int).Add(p, big.NewInt(1)), big.NewInt(4)), p)

	// determine if the y we have calculated is even or odd
	yMod2 := new(big.Int).Mod(y, big.NewInt(2))

	// if prefix is even (indicating an even y value) and y is odd, use other y value
	if (int(prefix[0])%2 == 0) && (yMod2.Cmp(big.NewInt(0)) != 0) { // Cmp returns 0 if equal
		y = new(big.Int).Mod(new(big.Int).Sub(p, y), p)
	}

	// if prefix is odd (indicating an odd y value) and y is even, use other y value
	if (int(prefix[0])%2 != 0) && (yMod2.Cmp(big.NewInt(0)) == 0) { // Cmp returns 0 if equal
		y = new(big.Int).Mod(new(big.Int).Sub(p, y), p)
	}

	// convert y to a byte array
	yBytes := y.Bytes()

	// make sure y value is 32 bytes in length
	if len(yBytes) < 32 {
		yBytes = make([]byte, 32)
		copy(yBytes[32-len(y.Bytes()):], y.Bytes())
	}

	// return full x and y coordinates (with 0x04 prefix) as a byte array
	uncompressed := []byte{0x04}
	uncompressed = append(uncompressed, x...)
	uncompressed = append(uncompressed, yBytes...)

	return uncompressed
}
