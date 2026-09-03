package svp2ptest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// This is a SECOND implementation of the BIP152 short transaction ID, kept
// deliberately apart from the one in services/svp2p/protocol.
//
// svp2ptest stands in for a peer, so it must not share the code under test —
// the same reason ReadFrameHeader parses a wire header by hand instead of
// calling the transport. A scripted peer that announced compact blocks with
// the node's own derivation could not tell a correct derivation from a
// consistently wrong one. The two copies are held to each other by
// TestScriptedPeer_AnnounceCompactCarriesShortIDsAndPrefilled, which checks
// what this peer put on the wire against protocol.ShortIDKeys/ShortID.
//
// It cannot import that package in any case: protocol's tests are in package
// protocol and import svp2ptest, so an import the other way is a cycle.

// shortIDMask keeps the low 48 bits of a SipHash output
// (blockencodings.cpp:78-81, GetShortID: SHORTTXIDS_LENGTH is 6 bytes).
const shortIDMask = 0xffffffffffff

// shortIDKeys derives the (k0, k1) SipHash key for one compact block
// (blockencodings.cpp:65-76, FillShortTxIDSelector): SHA256 over the 80 byte
// header serialization followed by the little endian nonce, with k0 and k1
// read as the first two little endian uint64 words of the digest
// (uint256.h:118-126, uint256::GetUint64).
func shortIDKeys(header *wire.BlockHeader, nonce uint64) (k0, k1 uint64) {
	var buf bytes.Buffer
	buf.Grow(88)

	// Serialize never fails for the fixed size header encoding.
	_ = header.Serialize(&buf)

	var nonceBytes [8]byte

	binary.LittleEndian.PutUint64(nonceBytes[:], nonce)
	buf.Write(nonceBytes[:])

	sum := sha256.Sum256(buf.Bytes())

	return binary.LittleEndian.Uint64(sum[0:8]), binary.LittleEndian.Uint64(sum[8:16])
}

// shortID is the BIP152 short transaction ID of hash
// (blockencodings.cpp:78-81, GetShortID): SipHash-2-4 keyed with (k0, k1),
// masked to the low 48 bits.
func shortID(k0, k1 uint64, hash chainhash.Hash) uint64 {
	return sipHash24(k0, k1, hash[:]) & shortIDMask
}

// sipHash24 is SipHash-2-4 of data keyed with (k0, k1), ported from CSipHasher
// (hash.h:253-275, hash.cpp:124-193). The module carries no SipHash
// dependency, so the reference algorithm is written out here.
func sipHash24(k0, k1 uint64, data []byte) uint64 {
	// hash.cpp:124-129, CSipHasher::CSipHasher.
	v0 := 0x736f6d6570736575 ^ k0
	v1 := 0x646f72616e646f6d ^ k1
	v2 := 0x6c7967656e657261 ^ k0
	v3 := 0x7465646279746573 ^ k1

	// hash.cpp:150-176, CSipHasher::Write.
	n := len(data)
	end := n - n%8

	for i := 0; i < end; i += 8 {
		m := binary.LittleEndian.Uint64(data[i : i+8])

		v3 ^= m
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= m
	}

	// hash.cpp:178-193, CSipHasher::Finalize.
	var last uint64

	for i := end; i < n; i++ {
		last |= uint64(data[i]) << (8 * uint(i-end)) //nolint:gosec // i-end is below 8
	}

	last |= uint64(n) << 56

	v3 ^= last
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0 ^= last
	v2 ^= 0xff
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)

	return v0 ^ v1 ^ v2 ^ v3
}

// sipRound is the SIPROUND macro (hash.cpp:106-122).
func sipRound(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 += v1
	v1 = rotl64(v1, 13)
	v1 ^= v0
	v0 = rotl64(v0, 32)
	v2 += v3
	v3 = rotl64(v3, 16)
	v3 ^= v2
	v0 += v3
	v3 = rotl64(v3, 21)
	v3 ^= v0
	v2 += v1
	v1 = rotl64(v1, 17)
	v1 ^= v2
	v2 = rotl64(v2, 32)

	return v0, v1, v2, v3
}

// rotl64 is the ROTL macro (hash.cpp:104).
func rotl64(x uint64, b uint) uint64 {
	return (x << b) | (x >> (64 - b))
}
