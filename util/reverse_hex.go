package util

import (
	"encoding/hex"
	"slices"
)

// ReverseAndHexEncodeSlice returns the hex encoding of a reversed copy of b.
func ReverseAndHexEncodeSlice(b []byte) string {
	tmp := make([]byte, len(b))
	copy(tmp, b)
	slices.Reverse(tmp)
	return hex.EncodeToString(tmp)
}

// DecodeAndReverseHexString decodes the hex string and reverses the resulting bytes.
func DecodeAndReverseHexString(hexStr string) ([]byte, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}
	slices.Reverse(b)
	return b, nil
}

// DecodeAndReverseHashString decodes a 64-char hex hash string and returns
// the reversed 32-byte result.
func DecodeAndReverseHashString(hexStr string) ([32]byte, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return [32]byte{}, err
	}

	var b32 [32]byte
	copy(b32[:], b)

	// Reverse in place
	for i, j := 0, len(b32)-1; i < j; i, j = i+1, j-1 {
		b32[i], b32[j] = b32[j], b32[i]
	}

	return b32, nil
}

// ReverseSlice returns a reversed copy of the given slice.
func ReverseSlice[T any](a []T) []T {
	tmp := make([]T, len(a))
	copy(tmp, a)
	slices.Reverse(tmp)
	return tmp
}
