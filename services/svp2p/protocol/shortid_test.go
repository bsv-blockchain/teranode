package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// siphashRefKey0, siphashRefKey1 are the reference key from the SipHash
// paper's own test vectors and from bitcoin-sv src/test/hash_tests.cpp:87
// (k = 00 01 02 ... 0F, read as two little-endian uint64s).
const (
	siphashRefKey0 = 0x0706050403020100
	siphashRefKey1 = 0x0F0E0D0C0B0A0908
)

// TestShortID_ReferenceVector pins ShortID to bitcoin-sv
// src/test/hash_tests.cpp:111-115, the SipHashUint256 case:
//
//	SipHashUint256(0x0706050403020100ULL, 0x0F0E0D0C0B0A0908ULL,
//	               uint256S("1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")),
//	0x7127512f72f27cceull
//
// uint256S parses the hex string as a byte-reversed display string, so the
// internal (computation-order) bytes are 0x00, 0x01, ..., 0x1f, matching
// chainhash.Hash's own byte order (chainhash.Hash.String reverses for
// display, see go-bt/v2/chainhash/hash.go). GetShortID
// (blockencodings.cpp:78-81) masks the raw SipHash output to 48 bits.
func TestShortID_ReferenceVector(t *testing.T) {
	var h chainhash.Hash
	for i := range h {
		h[i] = byte(i)
	}

	const rawSipHash = uint64(0x7127512f72f27cce)
	want := rawSipHash & 0xffffffffffff

	got := ShortID(siphashRefKey0, siphashRefKey1, h)
	require.Equal(t, want, got)
}

// TestShortID_MasksTo48Bits pins the mask from blockencodings.cpp:80-81
// (SHORTTXIDS_LENGTH == 6, GetShortID masks with 0xffffffffffffL) using a
// hash designed to set every bit of the raw 64-bit SipHash output.
func TestShortID_MasksTo48Bits(t *testing.T) {
	var h chainhash.Hash
	for i := range h {
		h[i] = 0xff
	}

	got := ShortID(1, 2, h)
	require.Zero(t, got>>48, "ShortID must never set a bit above bit 47")
}

// TestShortIDKeys_MatchesFillShortTxIDSelector reimplements
// blockencodings.cpp:65-76 (FillShortTxIDSelector) independently — a single
// SHA256 over the 80-byte header serialization followed by the nonce as a
// little-endian uint64 — and checks ShortIDKeys against it byte for byte.
func TestShortIDKeys_MatchesFillShortTxIDSelector(t *testing.T) {
	header := &wire.BlockHeader{
		Version:    1,
		PrevBlock:  chainhash.Hash{0x01, 0x02, 0x03},
		MerkleRoot: chainhash.Hash{0xaa, 0xbb, 0xcc},
		Timestamp:  time.Unix(1231006505, 0),
		Bits:       0x1d00ffff,
		Nonce:      2083236893,
	}
	const nonce = uint64(0x0102030405060708)

	buf := make([]byte, 0, 88)
	headerBuf := &countingBuffer{}
	require.NoError(t, header.Serialize(headerBuf))
	require.Len(t, headerBuf.data, 80)
	buf = append(buf, headerBuf.data...)

	var nonceBytes [8]byte
	binary.LittleEndian.PutUint64(nonceBytes[:], nonce)
	buf = append(buf, nonceBytes[:]...)

	sum := sha256.Sum256(buf)
	wantK0 := binary.LittleEndian.Uint64(sum[0:8])
	wantK1 := binary.LittleEndian.Uint64(sum[8:16])

	gotK0, gotK1 := ShortIDKeys(header, nonce)
	require.Equal(t, wantK0, gotK0)
	require.Equal(t, wantK1, gotK1)
}

// TestShortIDKeys_DifferentNonceDifferentKeys guards against a selector that
// ignores the nonce (FillShortTxIDSelector hashes header || nonce together,
// blockencodings.cpp:67-68).
func TestShortIDKeys_DifferentNonceDifferentKeys(t *testing.T) {
	header := &wire.BlockHeader{Timestamp: time.Unix(0, 0)}

	k0a, k1a := ShortIDKeys(header, 1)
	k0b, k1b := ShortIDKeys(header, 2)

	require.False(t, k0a == k0b && k1a == k1b)
}

// countingBuffer is a minimal io.Writer that just appends to a slice; used
// instead of bytes.Buffer only so the test file has one fewer import to
// reason about.
type countingBuffer struct {
	data []byte
}

func (b *countingBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}
