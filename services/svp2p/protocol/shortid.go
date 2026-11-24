package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// shortIDMask keeps the low 48 bits of a SipHash output, per
// blockencodings.cpp:78-81 (GetShortID): SHORTTXIDS_LENGTH is 6 bytes, so
// the raw 64-bit SipHash-2-4 output is masked to 0xffffffffffffL.
const shortIDMask = 0xffffffffffff

// ShortIDKeys derives the (k0, k1) SipHash key for one compact block, the
// port of FillShortTxIDSelector (bitcoin-sv src/blockencodings.cpp:65-76):
// a single SHA256 over the 80-byte block header serialization followed by
// the nonce as a little-endian uint64, with k0/k1 read as the first two
// little-endian uint64 words of the digest (uint256::GetUint64,
// src/uint256.h:118-126).
func ShortIDKeys(header *wire.BlockHeader, nonce uint64) (k0, k1 uint64) {
	var buf bytes.Buffer
	buf.Grow(88)

	// header.Serialize never returns an error for the fixed-size header
	// encoding (block_header.go: 80 bytes, no variable-length fields).
	_ = header.Serialize(&buf)

	var nonceBytes [8]byte
	binary.LittleEndian.PutUint64(nonceBytes[:], nonce)
	buf.Write(nonceBytes[:])

	sum := sha256.Sum256(buf.Bytes())

	k0 = binary.LittleEndian.Uint64(sum[0:8])
	k1 = binary.LittleEndian.Uint64(sum[8:16])

	return k0, k1
}

// ShortID computes the BIP152 short transaction ID for hash, the port of
// GetShortID (bitcoin-sv src/blockencodings.cpp:78-81): SipHash-2-4 keyed
// with (k0, k1) over the transaction hash, masked to the low 48 bits.
func ShortID(k0, k1 uint64, hash chainhash.Hash) uint64 {
	return sipHash24(k0, k1, hash[:]) & shortIDMask
}
