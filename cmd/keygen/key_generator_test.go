package main

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerateHexEncodedEd25519PrivateKey verifies that key generation succeeds
// and that a generated key round‑trips through decodeHexEd25519PrivateKey.
func TestGenerateHexEncodedEd25519PrivateKey(t *testing.T) {
	hexKey, err := generateHexEncodedEd25519PrivateKey()
	require.NoError(t, err, "key generation should not error")

	// An Ed25519 private key is 64 bytes; hex encoding doubles the length.
	require.Len(t, hexKey, 128, "hex‑encoded key should be 128 characters")

	// Round‑trip decode, then compare the raw bytes to the original hex string.
	privKey, err := decodeHexEd25519PrivateKey(hexKey)
	require.NoError(t, err, "decoding the just‑generated key should succeed")
	require.NotNil(t, privKey, "decoded key should not be nil")

	raw, err := privKey.Raw()
	require.NoError(t, err, "extracting raw bytes from decoded key should succeed")

	// Re‑encode and ensure it matches the original hex string.
	require.Equal(t, hexKey, hex.EncodeToString(raw), "round‑tripped key should match original")
}

// TestDecodeHexEd25519PrivateKey_InvalidHex ensures that non‑hex input is rejected.
func TestDecodeHexEd25519PrivateKey_InvalidHex(t *testing.T) {
	_, err := decodeHexEd25519PrivateKey("this‑is‑not‑hex")
	require.Error(t, err, "invalid hex should cause an error")
}

// TestDecodeHexEd25519PrivateKey_InvalidKeyBytes ensures that hex‑decoding succeeds,
// but the resulting byte slice is not a valid Ed25519 key.
func TestDecodeHexEd25519PrivateKey_InvalidKeyBytes(t *testing.T) {
	shortBytesHex := hex.EncodeToString(make([]byte, 10)) // only 10 bytes, not 64
	_, err := decodeHexEd25519PrivateKey(shortBytesHex)
	require.Error(t, err, "decoding an improperly sized key should fail")
}
