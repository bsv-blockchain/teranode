package util

import "crypto/sha256"

// Sha256d performs double SHA-256 hashing as used in Bitcoin protocol.
// It calculates SHA256(SHA256(b)) and returns the resulting 32-byte hash.
func Sha256d(b []byte) []byte {
	first := sha256.Sum256(b)
	second := sha256.Sum256(first[:])

	return second[:]
}
